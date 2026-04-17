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
from server.services.query_session_service import QuerySessionService
from server.services.stop_signal_service import StopSignalService
from server.utils.db_pool import get_metadata_pool
from server.utils.json_utils import sanitize_for_json

logger = structlog.get_logger()

ALLOWED_ACTIONS = {
    "table_resolution": {
        "confirm",
        "revise",
        "change_table",
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
        "exit_current",
    },
    "ir_ready": {
        "change_table",
        "revise",
        "request_explanation",
        "exit_current",
    },
    "table_resolved": {
        "change_table",
        "request_explanation",
        "exit_current",
    },
}


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
    def resolve_natural_language_reply(current_node: str, text: str) -> Tuple[Optional[str], Dict[str, Any], str]:
        normalized = (text or "").strip()
        lowered = normalized.lower()
        if not normalized:
            return None, {}, "请告诉我你是想确认当前理解、修改条件，还是重新选表。"

        if any(token in normalized for token in ["不是这张表", "换表", "表不对", "重新选表"]):
            return "change_table", {"reason": normalized}, ""

        if "手动选表" in normalized:
            return "change_table", {"mode": "manual_select", "reason": normalized}, ""

        if any(token in normalized for token in ["确认", "就按这个", "按这个查", "继续吧", "可以"]):
            if current_node == "execution_guard":
                return "execution_decision", {"decision": "approve", "source_text": normalized}, ""
            return "confirm", {"source_text": normalized}, ""

        if any(token in normalized for token in ["取消", "算了", "先不查"]):
            return "exit_current", {"mode": "cancel", "source_text": normalized}, ""

        if any(token in normalized for token in ["为什么", "解释", "为啥"]):
            return "request_explanation", {"question": normalized}, ""

        if any(token in normalized for token in ["改", "换成", "不要", "不是"]):
            return "revise", {"text": normalized}, ""

        if current_node in {"table_resolution", "draft_confirmation", "execution_guard"}:
            return None, {}, "我还不确定你是在确认当前方案、修改条件，还是准备换表。请再明确一点。"

        return "revise", {"text": normalized}, ""

    @staticmethod
    def _ensure_action_allowed(current_node: str, action_type: str) -> None:
        allowed = ALLOWED_ACTIONS.get(current_node)
        if allowed is None:
            raise ValueError(f"当前节点 {current_node} 暂不支持动作提交")
        if action_type not in allowed:
            raise ValueError(f"当前节点 {current_node} 不允许动作 {action_type}")

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
                return (
                    "running",
                    "draft_generation",
                    {
                        "pending_actions": [],
                        "selected_table_ids": selected_table_ids,
                        "selected_table_id": selected_table_ids[0] if selected_table_ids else state.get("selected_table_id"),
                        "draft_confirmation_approved": True,
                        "interruption_requested": False,
                        "last_action": "confirm",
                    },
                )
            return (
                "running",
                "draft_generation",
                {
                    "pending_actions": [],
                    "selected_table_ids": selected_table_ids,
                    "selected_table_id": selected_table_ids[0] if selected_table_ids else state.get("selected_table_id"),
                    "draft_version": draft_version + 1,
                    "invalidated_artifacts": [],
                    "draft_confirmation_required": True,
                    "draft_confirmation_approved": False,
                    "draft_confirmation_card": None,
                    "manual_table_override": False,
                    "interruption_requested": False,
                    "last_action": "confirm",
                },
            )

        if action_type == "change_table":
            rejected = list(state.get("rejected_table_ids") or [])
            for item in state.get("selected_table_ids") or state.get("recommended_table_ids") or []:
                if item and item not in rejected:
                    rejected.append(item)
            return (
                "awaiting_user_action",
                "table_resolution",
                {
                    "pending_actions": ["confirm", "change_table", "request_explanation", "exit_current"],
                    "rejected_table_ids": rejected,
                    "invalidated_artifacts": ["draft", "ir", "sql", "result"],
                    "manual_table_override": payload.get("mode") == "manual_select",
                    "selected_table_ids": [],
                    "selected_table_id": None,
                    "ir_ready": False,
                    "ir_snapshot": None,
                    "sql_preview": None,
                    "result_meta": None,
                    "execution_guard": None,
                    "draft_confirmation_card": None,
                    "draft_confirmation_required": False,
                    "draft_confirmation_approved": False,
                    "interruption_requested": session.get("status") == "running",
                    "interrupt_target_message_id": session.get("message_id"),
                    "interrupt_reason": "change_table",
                    "last_action": "change_table",
                },
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
                    "pending_actions": [],
                    "selected_table_ids": selected_table_ids,
                    "selected_table_id": selected_table_ids[0] if selected_table_ids else state.get("selected_table_id"),
                    "revision_request": sanitize_for_json(payload),
                    "invalidated_artifacts": ["draft", "ir", "sql", "result"],
                    "ir_ready": False,
                    "ir_snapshot": None,
                    "sql_preview": None,
                    "result_meta": None,
                    "execution_guard": None,
                    "draft_confirmation_card": None,
                    "draft_confirmation_required": True,
                    "draft_confirmation_approved": False,
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
                return (
                    "running",
                    "execution_approved",
                    {
                        "pending_actions": [],
                        "last_action": "execution_decision",
                        "execution_decision": "approve",
                        "execution_guard": None,
                        "draft_confirmation_required": False,
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
            session = await session_service.get_session(query_id)
            if not session:
                raise ValueError("查询会话不存在")

            current_node = session["current_node"]
            resolved_action = action_type
            resolved_payload = dict(payload or {})
            clarification = ""

            if natural_language_reply and not resolved_action:
                resolved_action, resolved_payload, clarification = self.resolve_natural_language_reply(
                    current_node,
                    natural_language_reply,
                )
                if not resolved_action:
                    return {
                        "resolution": "need_clarification",
                        "message": clarification,
                        "session": session,
                    }
            elif not resolved_action:
                raise ValueError("必须提供 action_type 或 natural_language_reply")

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
                    return {
                        "resolution": "resolved_to_action",
                        "action": self._row_to_dict(existing),
                        "session": session,
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
        return {
            "resolution": "resolved_to_action",
            "action": self._row_to_dict(row),
            "session": updated_session,
            "idempotent_replay": False,
            "interruption": interruption,
        }
