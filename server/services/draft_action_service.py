"""
查询动作服务

统一处理确认、修订、换表等动作，并同步推进 query_sessions 状态。
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional, Tuple
from uuid import UUID, uuid4

import asyncpg
import structlog

from server.services.conversation_service import ActiveQueryRegistry
from server.services.learning_event_service import LearningEventService
from server.services.query_session_service import QuerySessionService
from server.services.stop_signal_service import StopSignalService
from server.utils.db_pool import get_metadata_pool
from server.utils.json_utils import sanitize_for_json

logger = structlog.get_logger()

ALLOWED_ACTIONS = {
    "question_intake": {
        "change_table",
        "revise",
        "request_explanation",
        "exit_current",
    },
    "draft_generation": {
        "change_table",
        "revise",
        "request_explanation",
        "exit_current",
    },
    "connection_resolution": {
        "change_table",
        "revise",
        "request_explanation",
        "exit_current",
    },
    "permission_resolution": {
        "change_table",
        "revise",
        "request_explanation",
        "exit_current",
    },
    "table_resolution": {
        "confirm",
        "revise",
        "change_table",
        "return_previous",
        "choose_option",
        "request_explanation",
        "exit_current",
    },
    "draft_confirmation": {
        "confirm",
        "revise",
        "change_table",
        "choose_option",
        "request_explanation",
        "exit_current",
    },
    "execution_guard": {
        "execution_decision",
        "revise",
        "change_table",
        "request_explanation",
        "exit_current",
    },
    "completed": {
        "change_table",
        "revise",
        "request_explanation",
    },
    "failed": {
        "change_table",
        "revise",
        "request_explanation",
    },
    "ir_ready": {
        "change_table",
        "revise",
        "request_explanation",
    },
    "table_resolved": {
        "change_table",
        "revise",
        "request_explanation",
        "exit_current",
    },
}

SEMANTIC_ACTION_ALIASES = {
    "choose_table": "confirm",
    "confirm_draft": "confirm",
    "manual_select_table": "change_table",
    "approve_execution": "execution_decision",
    "cancel_query": "exit_current",
}

SEMANTIC_ACTION_NODE_REQUIREMENTS = {
    "choose_table": {"table_resolution"},
    "confirm_draft": {"draft_confirmation"},
    "manual_select_table": {"table_resolution"},
    "approve_execution": {"execution_guard"},
}

LEGACY_CONFIRMATION_STATE_KEYS = [
    "candidate_snapshot",
    "draft_state",
    "draft_confirmation_card",
    "draft_confirmation_required",
    "draft_confirmation_approved",
]

RUNNING_RESULT_PENDING_ACTIONS = ["change_table", "revise", "request_explanation", "exit_current"]
RETURN_PREVIOUS_SNAPSHOT_KEY = "return_previous_snapshot"
RETURN_PREVIOUS_SOURCE_NODES = {"draft_confirmation", "execution_guard"}


class DraftActionService:
    def __init__(self, db_conn: Optional[asyncpg.Connection] = None):
        self.db = db_conn

    @asynccontextmanager
    async def _acquire(self) -> AsyncIterator[asyncpg.Connection]:
        if self.db is not None:
            yield self.db
            return

        pool = await get_metadata_pool()
        conn = await pool.acquire()
        try:
            yield conn
        finally:
            await pool.release(conn)

    async def get_action_by_idempotency(self, query_id: UUID, idempotency_key: str) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT action_id, query_id, draft_version, action_type, actor_type, actor_id,
                       payload_json, idempotency_key, created_at
                FROM draft_actions
                WHERE query_id = $1 AND idempotency_key = $2
                """,
                query_id,
                idempotency_key,
            )
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "action_id": str(row["action_id"]),
            "query_id": str(row["query_id"]),
            "draft_version": row["draft_version"],
            "action_type": row["action_type"],
            "actor_type": row["actor_type"],
            "actor_id": row["actor_id"],
            "payload_json": row["payload_json"] or {},
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _try_uuid(value: Optional[str]) -> Optional[UUID]:
        if not value:
            return None
        try:
            return UUID(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _looks_like_new_query(text: str) -> bool:
        normalized = (text or "").strip()
        if not normalized:
            return False

        query_prefixes = (
            "查",
            "查询",
            "统计",
            "看",
            "分析",
            "列出",
            "展示",
            "告诉我",
            "我想看",
            "帮我查",
            "帮我看",
            "帮我统计",
            "请查",
            "请帮我查",
            "那",
            "再看",
            "另外",
        )
        query_markers = (
            "？",
            "?",
            "多少",
            "哪些",
            "什么",
            "怎么",
            "趋势",
            "排名",
            "top",
            "同比",
            "环比",
            "分布",
            "明细",
            "情况",
        )
        lowered = normalized.lower()
        return normalized.startswith(query_prefixes) or any(marker in normalized for marker in query_markers) or "top" in lowered

    @staticmethod
    def resolve_pending_reply(current_node: str, text: str) -> Dict[str, Any]:
        normalized = (text or "").strip()
        if not normalized:
            return {
                "resolution": "need_clarification",
                "message": "请告诉我你是想确认当前方案、修改问题、重新选表，还是想看看系统是怎么理解的。",
            }

        if any(token in normalized for token in ["不是这张表", "换表", "表不对", "重新选表"]):
            return {
                "resolution": "resolved_to_action",
                "action_type": "change_table",
                "payload": {"reason": normalized},
            }

        if "手动选表" in normalized:
            return {
                "resolution": "resolved_to_action",
                "action_type": "change_table",
                "payload": {"mode": "manual_select", "reason": normalized},
            }

        if any(token in normalized for token in ["确认", "就按这个", "按这个查", "继续吧", "可以"]):
            if current_node == "execution_guard":
                return {
                    "resolution": "resolved_to_action",
                    "action_type": "execution_decision",
                    "payload": {"decision": "approve", "source_text": normalized},
                }
            return {
                "resolution": "resolved_to_action",
                "action_type": "confirm",
                "payload": {"source_text": normalized},
            }

        if any(token in normalized for token in ["取消", "算了", "先不查"]):
            return {
                "resolution": "resolved_to_action",
                "action_type": "exit_current",
                "payload": {"mode": "cancel", "source_text": normalized},
            }

        if any(token in normalized for token in ["为什么", "解释", "为啥", "系统理解", "怎么理解", "如何理解"]):
            return {
                "resolution": "resolved_to_action",
                "action_type": "request_explanation",
                "payload": {"question": normalized},
            }

        if any(token in normalized for token in ["改", "换成", "不要", "不是"]):
            return {
                "resolution": "resolved_to_action",
                "action_type": "revise",
                "payload": {"text": normalized},
            }

        if DraftActionService._looks_like_new_query(normalized):
            return {
                "resolution": "resolved_to_new_query",
                "new_query_text": normalized,
            }

        if current_node in {"table_resolution", "draft_confirmation", "execution_guard"}:
            return {
                "resolution": "need_clarification",
                "message": "我还不确定你是在确认当前方案、修改问题、查看系统理解，还是已经开始了一个新问题。请再明确一点。",
            }

        return {
            "resolution": "resolved_to_action",
            "action_type": "revise",
            "payload": {"text": normalized},
        }

    @staticmethod
    def resolve_natural_language_reply(current_node: str, text: str) -> Tuple[Optional[str], Dict[str, Any], str]:
        resolution = DraftActionService.resolve_pending_reply(current_node, text)
        if resolution["resolution"] != "resolved_to_action":
            return None, {}, resolution.get("message", "")
        return resolution["action_type"], resolution.get("payload", {}), ""

    @staticmethod
    def _ensure_action_allowed(current_node: str, action_type: str) -> None:
        allowed = ALLOWED_ACTIONS.get(current_node)
        if allowed is None:
            raise ValueError(f"当前节点 {current_node} 暂不支持动作提交")
        if action_type not in allowed:
            raise ValueError(f"当前节点 {current_node} 不允许动作 {action_type}")

    @staticmethod
    def _normalize_requested_action(
        current_node: str,
        action_type: str,
        payload: Optional[Dict[str, Any]],
    ) -> Tuple[str, Dict[str, Any]]:
        normalized_payload = dict(payload or {})
        required_nodes = SEMANTIC_ACTION_NODE_REQUIREMENTS.get(action_type)
        if required_nodes and current_node not in required_nodes:
            raise ValueError(f"当前节点 {current_node} 不允许语义动作 {action_type}")

        resolved_action = SEMANTIC_ACTION_ALIASES.get(action_type, action_type)
        if action_type == "manual_select_table":
            normalized_payload["mode"] = "manual_select"
        elif action_type == "approve_execution":
            normalized_payload["decision"] = "approve"
        elif action_type == "cancel_query":
            normalized_payload.setdefault("mode", "cancel")

        return resolved_action, normalized_payload

    @staticmethod
    def _build_resume_directive(
        *,
        updated_session: Optional[Dict[str, Any]],
        action_type: str,
    ) -> Optional[Dict[str, Any]]:
        if not updated_session:
            return None

        current_node = updated_session.get("current_node")
        state = dict(updated_session.get("state_json") or {})
        text = state.get("resolved_question_text") or state.get("question_text")
        selected_table_ids = list(state.get("selected_table_ids") or [])
        table_resolution_state = QuerySessionService._normalize_state(state.get("table_resolution_state"))
        multi_table_mode = (
            state.get("multi_table_mode")
            or table_resolution_state.get("multi_table_mode")
        )

        if current_node == "draft_generation" and action_type in {"confirm", "revise"}:
            progress_text = "正在根据修改意见重算确认稿..."
            resume_ir = None
            if action_type == "confirm":
                if QuerySessionService.has_confirmed_draft(state) and state.get("ir_snapshot"):
                    progress_text = "正在基于已确认草稿继续查询..."
                    resume_ir = state.get("ir_snapshot")
                else:
                    progress_text = "正在使用确认后的表继续查询..."
            return {
                "should_resume": True,
                "source_action": action_type,
                "target_node": current_node,
                "query_id": updated_session.get("query_id"),
                "text": text,
                "ir": resume_ir,
                "selected_table_ids": selected_table_ids,
                "multi_table_mode": multi_table_mode,
                "force_execute": False,
                "progress_text": progress_text,
            }

        if current_node == "execution_approved" and action_type == "execution_decision":
            if state.get("execution_decision") != "approve":
                return None
            return {
                "should_resume": True,
                "source_action": action_type,
                "target_node": current_node,
                "query_id": updated_session.get("query_id"),
                "text": text,
                "ir": state.get("ir_snapshot"),
                "selected_table_ids": selected_table_ids,
                "multi_table_mode": multi_table_mode,
                "force_execute": True,
                "progress_text": "正在继续执行查询...",
            }

        return None

    @staticmethod
    def _build_return_previous_snapshot(session: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        current_node = session.get("current_node")
        if current_node not in RETURN_PREVIOUS_SOURCE_NODES:
            return None

        state = QuerySessionService._normalize_state(session.get("state_json"))
        snapshot_state = {
            key: value
            for key, value in state.items()
            if key != RETURN_PREVIOUS_SNAPSHOT_KEY
        }
        return sanitize_for_json(
            {
                "status": session.get("status"),
                "current_node": current_node,
                "state_json": snapshot_state,
            }
        )

    @staticmethod
    def _restore_transition_from_snapshot(
        session: Dict[str, Any],
        snapshot: Optional[Dict[str, Any]],
    ) -> Tuple[str, str, Dict[str, Any]]:
        normalized_snapshot = QuerySessionService._normalize_state(snapshot)
        snapshot_status = str(normalized_snapshot.get("status") or "").strip()
        snapshot_node = str(normalized_snapshot.get("current_node") or "").strip()
        snapshot_state = QuerySessionService._normalize_state(normalized_snapshot.get("state_json"))
        if not snapshot_status or not snapshot_node or not snapshot_state:
            raise ValueError("当前没有可返回的上一页状态")

        current_state = QuerySessionService._normalize_state(session.get("state_json"))
        delete_keys = [
            key
            for key in current_state.keys()
            if key not in snapshot_state
        ]
        if RETURN_PREVIOUS_SNAPSHOT_KEY not in delete_keys:
            delete_keys.append(RETURN_PREVIOUS_SNAPSHOT_KEY)

        return (
            snapshot_status,
            snapshot_node,
            {
                **snapshot_state,
                "__delete_keys__": delete_keys,
            },
        )

    @staticmethod
    def _derive_session_transition(
        *,
        session: Dict[str, Any],
        action_type: str,
        payload: Dict[str, Any],
    ) -> Tuple[str, str, Dict[str, Any]]:
        state = dict(session.get("state_json") or {})
        draft_version = int(state.get("draft_version") or 1)
        current_node = session.get("current_node")

        if action_type == "confirm":
            selected_table_ids = payload.get("selected_table_ids") or state.get("selected_table_ids") or state.get("recommended_table_ids") or []
            if current_node == "draft_confirmation":
                draft_payload = QuerySessionService._normalize_state(state.get("provisional_draft"))
                confirmed_draft = QuerySessionService.build_confirmed_draft_state(
                    draft_payload,
                    draft_json=(
                        draft_payload.get("draft_json")
                        or QuerySessionService._normalize_state(state.get("ir_snapshot"))
                        or None
                    ),
                )
                return (
                    "running",
                    "draft_generation",
                    {
                        "__delete_keys__": LEGACY_CONFIRMATION_STATE_KEYS,
                        "pending_actions": RUNNING_RESULT_PENDING_ACTIONS,
                        "selected_table_ids": selected_table_ids,
                        "selected_table_id": selected_table_ids[0] if selected_table_ids else state.get("selected_table_id"),
                        "provisional_draft": None,
                        "confirmed_draft": confirmed_draft,
                        "revision_request": None,
                        "interruption_requested": False,
                        "last_action": "confirm",
                    },
                )
            return (
                "running",
                "draft_generation",
                {
                    "__delete_keys__": LEGACY_CONFIRMATION_STATE_KEYS,
                    "pending_actions": RUNNING_RESULT_PENDING_ACTIONS,
                    "selected_table_ids": selected_table_ids,
                    "selected_table_id": selected_table_ids[0] if selected_table_ids else state.get("selected_table_id"),
                    "draft_version": draft_version + 1,
                    "invalidated_artifacts": [],
                    "provisional_draft": QuerySessionService.build_provisional_draft_state(
                        status="pending_generation",
                        confirmation_required=True,
                    ),
                    "confirmed_draft": None,
                    "revision_request": None,
                    "manual_table_override": False,
                    "interruption_requested": False,
                    "last_action": "confirm",
                },
            )

        if action_type == "change_table":
            return_previous_snapshot = DraftActionService._build_return_previous_snapshot(session)
            rejected = list(state.get("rejected_table_ids") or [])
            for item in state.get("selected_table_ids") or state.get("recommended_table_ids") or []:
                if item and item not in rejected:
                    rejected.append(item)

            pending_actions = ["confirm", "change_table", "request_explanation", "exit_current"]
            if return_previous_snapshot:
                pending_actions.insert(1, "return_previous")

            return (
                "awaiting_user_action",
                "table_resolution",
                {
                    "__delete_keys__": LEGACY_CONFIRMATION_STATE_KEYS,
                    "pending_actions": pending_actions,
                    "rejected_table_ids": rejected,
                    "invalidated_artifacts": ["draft", "ir", "sql", "result"],
                    "manual_table_override": payload.get("mode") == "manual_select",
                    "selected_table_ids": [],
                    "selected_table_id": None,
                    "ir_ready": False,
                    "ir_snapshot": None,
                    "sql_preview": None,
                    "result_meta": None,
                    "provisional_draft": None,
                    "confirmed_draft": None,
                    "execution_guard_state": None,
                    "execution_guard": None,
                    "revision_request": None,
                    RETURN_PREVIOUS_SNAPSHOT_KEY: return_previous_snapshot,
                    "interruption_requested": session.get("status") == "running",
                    "interrupt_target_message_id": session.get("message_id"),
                    "interrupt_reason": "change_table",
                    "last_action": "change_table",
                },
            )

        if action_type == "return_previous":
            return DraftActionService._restore_transition_from_snapshot(
                session,
                state.get(RETURN_PREVIOUS_SNAPSHOT_KEY),
            )

        if action_type == "revise":
            selected_table_ids = (
                payload.get("selected_table_ids")
                or state.get("selected_table_ids")
                or state.get("recommended_table_ids")
                or []
            )
            return (
                "running",
                "draft_generation",
                {
                    "__delete_keys__": LEGACY_CONFIRMATION_STATE_KEYS,
                    "pending_actions": RUNNING_RESULT_PENDING_ACTIONS,
                    "selected_table_ids": selected_table_ids,
                    "selected_table_id": selected_table_ids[0] if selected_table_ids else state.get("selected_table_id"),
                    "revision_request": sanitize_for_json(payload),
                    "invalidated_artifacts": ["draft", "ir", "sql", "result"],
                    "ir_ready": False,
                    "ir_snapshot": None,
                    "sql_preview": None,
                    "result_meta": None,
                    "provisional_draft": QuerySessionService.build_provisional_draft_state(
                        status="pending_generation",
                        confirmation_required=True,
                    ),
                    "confirmed_draft": None,
                    "execution_guard_state": None,
                    "execution_guard": None,
                    "draft_version": draft_version + 1,
                    "interruption_requested": session.get("status") == "running",
                    "interrupt_target_message_id": session.get("message_id"),
                    "interrupt_reason": "revise",
                    "last_action": "revise",
                },
            )

        if action_type == "request_explanation":
            return (
                session["status"],
                session["current_node"],
                {
                    "last_action": "request_explanation",
                    "explanation_request": sanitize_for_json(payload),
                },
            )

        if action_type == "execution_decision":
            decision = payload.get("decision", "reject")
            if decision == "approve":
                confirmed_draft = QuerySessionService.build_downstream_confirmed_draft_state(
                    state,
                    draft_json=QuerySessionService._normalize_state(state.get("ir_snapshot")) or None,
                )
                return (
                    "running",
                    "execution_approved",
                    {
                        "__delete_keys__": LEGACY_CONFIRMATION_STATE_KEYS,
                        "pending_actions": [],
                        "last_action": "execution_decision",
                        "execution_decision": "approve",
                        "execution_guard_state": None,
                        "execution_guard": None,
                        "confirmed_draft": confirmed_draft,
                        "interruption_requested": False,
                    },
                )
            return (
                "awaiting_user_action",
                "execution_guard",
                {
                    "last_action": "execution_decision",
                    "execution_decision": "reject",
                },
            )

        if action_type == "exit_current":
            mode = payload.get("mode", "cancel")
            if mode == "new_query":
                return (
                    "completed",
                    "new_query_requested",
                    {
                        "pending_actions": [],
                        "last_action": "exit_current",
                        "new_query_text": payload.get("new_query_text"),
                    },
                )
            return (
                "cancelled",
                "cancelled",
                {
                    "pending_actions": [],
                    "last_action": "exit_current",
                    "cancel_reason": payload.get("source_text") or payload.get("reason") or "用户取消",
                    "interruption_requested": session.get("status") == "running",
                    "interrupt_target_message_id": session.get("message_id"),
                    "interrupt_reason": "exit_current",
                },
            )

        if action_type == "choose_option":
            return (
                "awaiting_user_action",
                session["current_node"],
                {
                    "last_action": "choose_option",
                    "selected_option": sanitize_for_json(payload),
                },
            )

        raise ValueError(f"未实现的动作类型: {action_type}")

    @staticmethod
    async def _interrupt_active_query_if_needed(
        conn: asyncpg.Connection,
        session: Dict[str, Any],
        action_type: str,
    ) -> Dict[str, Any]:
        if action_type not in {"change_table", "exit_current"}:
            return {"requested": False}

        if session.get("status") != "running":
            return {"requested": False}

        message_id = session.get("message_id")
        user_id = session.get("user_id")
        query_id = session.get("query_id")

        stop_signal_sent = False
        if message_id:
            stop_signal_sent = StopSignalService.set_stop_signal(message_id)

        registry_marked = False
        if query_id and user_id:
            try:
                registry = ActiveQueryRegistry(conn)
                registry_marked = await registry.mark_cancelling(UUID(query_id), UUID(user_id))
            except Exception as exc:
                logger.warning(
                    "标记活跃查询取消中失败",
                    query_id=query_id,
                    user_id=user_id,
                    error=str(exc),
                )

        return {
            "requested": True,
            "stop_signal_sent": stop_signal_sent,
            "registry_marked": registry_marked,
            "message_id": message_id,
        }

    async def apply_action(
        self,
        *,
        query_id: UUID,
        action_type: Optional[str],
        payload: Optional[Dict[str, Any]],
        natural_language_reply: Optional[str],
        draft_version: Optional[int],
        actor_type: str,
        actor_id: str,
        idempotency_key: Optional[str],
    ) -> Dict[str, Any]:
        async with self._acquire() as conn:
            session_service = QuerySessionService(conn)
            learning_event_service = LearningEventService(conn)
            session = await session_service.get_session(query_id)
            if not session:
                raise ValueError("查询会话不存在")

            current_node = session["current_node"]
            resolved_action = action_type
            resolved_payload = dict(payload or {})
            clarification = ""

            if natural_language_reply and not resolved_action:
                reply_resolution = self.resolve_pending_reply(
                    current_node,
                    natural_language_reply,
                )
                if reply_resolution["resolution"] == "resolved_to_new_query":
                    return {
                        "resolution": "resolved_to_new_query",
                        "new_query_text": reply_resolution["new_query_text"],
                        "session": session,
                    }
                if reply_resolution["resolution"] != "resolved_to_action":
                    return {
                        "resolution": "need_clarification",
                        "message": reply_resolution["message"],
                        "session": session,
                    }
                resolved_action = reply_resolution["action_type"]
                resolved_payload = dict(reply_resolution.get("payload") or {})
            elif not resolved_action:
                raise ValueError("必须提供 action_type 或 natural_language_reply")

            if resolved_action:
                resolved_action, resolved_payload = self._normalize_requested_action(
                    current_node,
                    resolved_action,
                    resolved_payload,
                )

            self._ensure_action_allowed(current_node, resolved_action)

            pending_actions = set(session.get("state_json", {}).get("pending_actions") or [])
            if pending_actions and resolved_action not in pending_actions and resolved_action not in {
                "request_explanation",
                "exit_current",
            }:
                raise ValueError(f"当前待处理动作不包含 {resolved_action}")

            if idempotency_key:
                existing = await conn.fetchrow(
                    """
                    SELECT action_id, query_id, draft_version, action_type, actor_type, actor_id,
                           payload_json, idempotency_key, created_at
                    FROM draft_actions
                    WHERE query_id = $1 AND idempotency_key = $2
                    """,
                    query_id,
                    idempotency_key,
                )
                if existing:
                    replay_action = self._row_to_dict(existing)
                    return {
                        "resolution": "resolved_to_action",
                        "action": replay_action,
                        "session": session,
                        "resume_directive": self._build_resume_directive(
                            updated_session=session,
                            action_type=replay_action["action_type"],
                        ),
                        "idempotent_replay": True,
                    }

            session_draft_version = int(session.get("state_json", {}).get("draft_version") or 1)
            if draft_version is not None and draft_version != session_draft_version:
                raise ValueError(f"草稿版本冲突，当前版本为 {session_draft_version}")

            next_status, next_node, state_updates = self._derive_session_transition(
                session=session,
                action_type=resolved_action,
                payload=resolved_payload,
            )

            payload_json = sanitize_for_json({
                **resolved_payload,
                "natural_language_reply": natural_language_reply,
            })

            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO draft_actions (
                        action_id, query_id, draft_version, action_type,
                        actor_type, actor_id, payload_json, idempotency_key
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                    RETURNING action_id, query_id, draft_version, action_type, actor_type, actor_id,
                              payload_json, idempotency_key, created_at
                    """,
                    uuid4(),
                    query_id,
                    session_draft_version,
                    resolved_action,
                    actor_type,
                    actor_id,
                    json.dumps(payload_json, ensure_ascii=False),
                    idempotency_key or str(uuid4()),
                )

                updated_session = await session_service.update_session(
                    query_id,
                    status=next_status,
                    current_node=next_node,
                    state_updates=state_updates,
                    last_error=None,
                )

                await learning_event_service.record_event(
                    event_key=f"draft_action:{row['action_id']}",
                    event_type="action_applied",
                    query_id=query_id,
                    conversation_id=self._try_uuid(session.get("conversation_id")),
                    user_id=self._try_uuid(session.get("user_id")),
                    source_component="draft_action_service",
                    payload_json={
                        "action_id": str(row["action_id"]),
                        "draft_version": session_draft_version,
                        "action_type": resolved_action,
                        "actor_type": actor_type,
                        "actor_id": actor_id,
                        "current_node": current_node,
                        "next_node": next_node,
                        "previous_status": session.get("status"),
                        "next_status": next_status,
                        "previous_selected_table_ids": list(session.get("state_json", {}).get("selected_table_ids") or []),
                        "previous_recommended_table_ids": list(session.get("state_json", {}).get("recommended_table_ids") or []),
                        "previous_rejected_table_ids": list(session.get("state_json", {}).get("rejected_table_ids") or []),
                        "next_selected_table_ids": list((updated_session or {}).get("state_json", {}).get("selected_table_ids") or []),
                        "next_rejected_table_ids": list((updated_session or {}).get("state_json", {}).get("rejected_table_ids") or []),
                        "manual_table_override": bool((updated_session or {}).get("state_json", {}).get("manual_table_override")),
                        "payload": payload_json,
                    },
                )

            interruption = await self._interrupt_active_query_if_needed(
                conn,
                session,
                resolved_action,
            )

        logger.debug(
            "查询动作已应用",
            query_id=str(query_id),
            action_type=resolved_action,
            current_node=current_node,
            next_node=next_node,
        )
        resume_directive = self._build_resume_directive(
            updated_session=updated_session,
            action_type=resolved_action,
        )
        return {
            "resolution": "resolved_to_action",
            "action": self._row_to_dict(row),
            "session": updated_session,
            "resume_directive": resume_directive,
            "idempotent_replay": False,
            "interruption": interruption,
        }
