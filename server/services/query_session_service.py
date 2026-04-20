"""
查询会话状态服务

统一维护查询产品级状态，避免状态仅散落在消息、活跃查询和 trace 中。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional
from uuid import UUID

import asyncpg
import structlog

from server.utils.db_pool import get_metadata_pool
from server.utils.json_utils import sanitize_for_json

logger = structlog.get_logger()


class QuerySessionService:
    """查询会话状态服务"""

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

    @staticmethod
    def _dump_state(state: Optional[Dict[str, Any]]) -> str:
        return json.dumps(sanitize_for_json(QuerySessionService._normalize_state(state)), ensure_ascii=False)

    @staticmethod
    def _normalize_state(state: Optional[Any]) -> Dict[str, Any]:
        if state is None:
            return {}

        if isinstance(state, str):
            try:
                state = json.loads(state)
            except json.JSONDecodeError:
                logger.warning("query_session state_json 不是合法 JSON，已按空对象处理")
                return {}

        if isinstance(state, Mapping):
            return dict(state)

        logger.warning(
            "query_session state_json 类型异常，已按空对象处理",
            state_type=type(state).__name__,
        )
        return {}

    @staticmethod
    def _merge_state(current_state: Optional[Dict[str, Any]], updates: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        merged = QuerySessionService._normalize_state(current_state)
        if updates:
            merged.update(sanitize_for_json(QuerySessionService._normalize_state(updates)))
        return merged

    @staticmethod
    def _normalize_confirmation_state(payload: Optional[Any]) -> Dict[str, Any]:
        if payload is None:
            return {}
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump()
        return QuerySessionService._normalize_state(payload)

    @staticmethod
    def build_table_resolution_state(
        payload: Optional[Any] = None,
        *,
        question_text: Optional[str] = None,
        recommended_table_ids: Optional[list[str]] = None,
        selected_table_ids: Optional[list[str]] = None,
        rejected_table_ids: Optional[list[str]] = None,
        multi_table_mode: Optional[str] = None,
        manual_table_override: Optional[bool] = None,
    ) -> Dict[str, Any]:
        table_state = QuerySessionService._normalize_confirmation_state(payload)
        return sanitize_for_json(
            {
                "question": table_state.get("question") or question_text,
                "message": table_state.get("message"),
                "reason_summary": table_state.get("reason_summary") or table_state.get("confirmation_reason"),
                "candidates": list(table_state.get("candidates") or []),
                "recommended_table_ids": list(
                    recommended_table_ids
                    if recommended_table_ids is not None
                    else table_state.get("recommended_table_ids") or []
                ),
                "selected_table_ids": list(
                    selected_table_ids
                    if selected_table_ids is not None
                    else table_state.get("selected_table_ids") or []
                ),
                "rejected_table_ids": list(
                    rejected_table_ids
                    if rejected_table_ids is not None
                    else table_state.get("rejected_table_ids") or []
                ),
                "allow_multi_select": bool(table_state.get("allow_multi_select")),
                "multi_table_mode": multi_table_mode or table_state.get("multi_table_mode"),
                "manual_table_override": (
                    bool(manual_table_override)
                    if manual_table_override is not None
                    else bool(table_state.get("manual_table_override"))
                ),
            }
        )

    @staticmethod
    def build_draft_state(
        payload: Optional[Any] = None,
        *,
        status: Optional[str] = None,
        natural_language: Optional[str] = None,
        draft_json: Optional[Dict[str, Any]] = None,
        warnings: Optional[list[Any]] = None,
        suggestions: Optional[list[Any]] = None,
        confirmed: Optional[bool] = None,
        confirmation_required: Optional[bool] = None,
        table_dependent: Optional[bool] = None,
        invalidate_on_table_change: Optional[bool] = None,
    ) -> Dict[str, Any]:
        draft_state = QuerySessionService._normalize_confirmation_state(payload)
        resolved_table_dependent = True if table_dependent is None else bool(table_dependent)
        if table_dependent is None and draft_state.get("table_dependent") is not None:
            resolved_table_dependent = bool(draft_state.get("table_dependent"))

        resolved_invalidate_on_table_change = (
            True if invalidate_on_table_change is None else bool(invalidate_on_table_change)
        )
        if invalidate_on_table_change is None and draft_state.get("invalidate_on_table_change") is not None:
            resolved_invalidate_on_table_change = bool(draft_state.get("invalidate_on_table_change"))

        return sanitize_for_json(
            {
                "status": status or draft_state.get("status"),
                "natural_language": natural_language or draft_state.get("natural_language"),
                "draft_json": draft_json if draft_json is not None else draft_state.get("draft_json") or draft_state.get("ir"),
                "warnings": list(warnings if warnings is not None else draft_state.get("warnings") or []),
                "suggestions": list(suggestions if suggestions is not None else draft_state.get("suggestions") or []),
                "confirmed": bool(confirmed) if confirmed is not None else bool(draft_state.get("confirmed")),
                "confirmation_required": (
                    bool(confirmation_required)
                    if confirmation_required is not None
                    else bool(draft_state.get("confirmation_required"))
                ),
                "table_dependent": resolved_table_dependent,
                "invalidate_on_table_change": resolved_invalidate_on_table_change,
            }
        )

    @staticmethod
    def build_provisional_draft_state(
        payload: Optional[Any] = None,
        *,
        question_text: Optional[str] = None,
        selected_table_ids: Optional[list[str]] = None,
        recommended_table_ids: Optional[list[str]] = None,
        manual_table_override: Optional[bool] = None,
        allow_multi_select: Optional[bool] = None,
    ) -> Dict[str, Any]:
        draft_state = QuerySessionService._normalize_confirmation_state(payload)

        # 如果传入的已经是草稿形态，直接规整为 provisional draft。
        if any(
            draft_state.get(field) is not None
            for field in ("natural_language", "draft_json", "status", "warnings", "suggestions")
        ) and not draft_state.get("candidates"):
            return QuerySessionService.build_draft_state(
                draft_state,
                status=draft_state.get("status") or "provisional",
                natural_language=draft_state.get("natural_language"),
                draft_json=draft_state.get("draft_json"),
                warnings=list(draft_state.get("warnings") or []),
                suggestions=list(draft_state.get("suggestions") or []),
                confirmed=False,
                confirmation_required=False,
                table_dependent=True,
                invalidate_on_table_change=True,
            )

        candidates = list(draft_state.get("candidates") or [])
        preferred_table_ids = list(selected_table_ids or recommended_table_ids or [])

        candidate_names: list[str] = []
        seen_names: set[str] = set()
        for candidate in candidates:
            table_id = candidate.get("table_id")
            table_name = candidate.get("table_name")
            if not table_name or table_name in seen_names:
                continue
            if preferred_table_ids and table_id not in preferred_table_ids:
                continue
            seen_names.add(table_name)
            candidate_names.append(table_name)

        if not candidate_names:
            for candidate in candidates:
                table_name = candidate.get("table_name")
                if not table_name or table_name in seen_names:
                    continue
                seen_names.add(table_name)
                candidate_names.append(table_name)
                if len(candidate_names) >= 2:
                    break

        candidate_names = candidate_names[:2]
        table_hint = "、".join(candidate_names)
        resolved_manual_table_override = (
            bool(manual_table_override)
            if manual_table_override is not None
            else bool(draft_state.get("manual_table_override"))
        )

        if resolved_manual_table_override:
            natural_language = (
                f"当前问题是“{question_text}”。系统已切换为手动选表，确认数据表后会重新生成查询草稿。"
                if question_text
                else "系统已切换为手动选表，确认数据表后会重新生成查询草稿。"
            )
        elif table_hint and question_text:
            natural_language = (
                f"当前暂按候选表“{table_hint}”理解您的问题“{question_text}”，"
                "确认选表后系统会继续细化查询草稿。"
            )
        elif question_text:
            natural_language = f"系统已根据问题“{question_text}”生成暂定查询理解，确认选表后会继续细化查询草稿。"
        elif table_hint:
            natural_language = f"系统已基于候选表“{table_hint}”生成暂定查询理解，确认选表后会继续细化查询草稿。"
        else:
            natural_language = "系统已生成暂定查询理解，确认选表后会继续细化查询草稿。"

        resolved_allow_multi_select = (
            bool(allow_multi_select)
            if allow_multi_select is not None
            else bool(draft_state.get("allow_multi_select"))
        )
        warnings: list[str] = []
        if len(preferred_table_ids) > 1 or resolved_allow_multi_select:
            warnings.append("当前问题可能涉及多表查询，最终草稿会在确认选表后重新生成。")

        return QuerySessionService.build_draft_state(
            status="provisional",
            natural_language=natural_language,
            draft_json=None,
            warnings=warnings,
            suggestions=[],
            confirmed=False,
            confirmation_required=False,
            table_dependent=True,
            invalidate_on_table_change=True,
        )

    @staticmethod
    def build_execution_guard_state(
        payload: Optional[Any] = None,
        *,
        status: Optional[str] = None,
        natural_language: Optional[str] = None,
        warnings: Optional[list[Any]] = None,
        estimated_cost: Optional[Dict[str, Any]] = None,
        ir: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        guard_state = QuerySessionService._normalize_confirmation_state(payload)
        return sanitize_for_json(
            {
                "status": status or guard_state.get("status"),
                "natural_language": natural_language or guard_state.get("natural_language"),
                "warnings": list(warnings if warnings is not None else guard_state.get("warnings") or []),
                "estimated_cost": estimated_cost if estimated_cost is not None else guard_state.get("estimated_cost"),
                "ir": ir if ir is not None else guard_state.get("ir"),
            }
        )

    @staticmethod
    def _get_selected_table_ids(state: Dict[str, Any]) -> list[str]:
        table_resolution_state = QuerySessionService._normalize_confirmation_state(state.get("table_resolution_state"))
        selected_table_ids = list(state.get("selected_table_ids") or table_resolution_state.get("selected_table_ids") or [])
        selected_table_id = state.get("selected_table_id")
        if not selected_table_id and table_resolution_state.get("selected_table_ids"):
            selected_table_id = list(table_resolution_state.get("selected_table_ids") or [None])[0]
        if not selected_table_ids and selected_table_id:
            selected_table_ids = [selected_table_id]
        return selected_table_ids

    @staticmethod
    def _map_pending_action(current_node: Optional[str], action_type: str) -> str:
        if action_type == "confirm":
            if current_node == "table_resolution":
                return "choose_table"
            if current_node == "draft_confirmation":
                return "confirm_draft"
        if action_type == "execution_decision":
            return "approve_execution"
        if action_type == "exit_current":
            return "cancel_query"
        return action_type

    @staticmethod
    def _build_safe_summary(current_node: Optional[str], state: Dict[str, Any]) -> Dict[str, Any]:
        safe_summary = QuerySessionService._normalize_state(state.get("safe_summary"))
        open_points = list(safe_summary.get("open_points") or [])
        if not open_points:
            if current_node == "table_resolution":
                open_points = ["需确认应使用哪张数据表"]
            elif current_node == "draft_confirmation":
                open_points = ["需确认当前查询草稿是否符合预期"]
            elif current_node == "execution_guard":
                open_points = ["需确认是否执行当前查询"]

        return {
            "user_goal_summary": (
                safe_summary.get("user_goal_summary")
                or state.get("resolved_question_text")
                or state.get("question_text")
            ),
            "domain_hint": safe_summary.get("domain_hint") or state.get("domain_name") or state.get("domain_id"),
            "known_constraints": list(safe_summary.get("known_constraints") or []),
            "open_points": open_points,
        }

    @staticmethod
    def _build_table_resolution(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        table_resolution_state = QuerySessionService._normalize_confirmation_state(state.get("table_resolution_state"))
        candidate_snapshot = QuerySessionService._normalize_state(state.get("candidate_snapshot"))
        selected_table_ids = QuerySessionService._get_selected_table_ids(state)
        recommended_table_ids = list(
            state.get("recommended_table_ids")
            or table_resolution_state.get("recommended_table_ids")
            or candidate_snapshot.get("recommended_table_ids")
            or []
        )
        rejected_table_ids = list(state.get("rejected_table_ids") or table_resolution_state.get("rejected_table_ids") or [])
        candidates = list(table_resolution_state.get("candidates") or candidate_snapshot.get("candidates") or [])

        if not any([table_resolution_state, candidate_snapshot, selected_table_ids, recommended_table_ids, rejected_table_ids]):
            return None

        status = "idle"
        if current_node == "table_resolution":
            status = "awaiting_confirmation"
        elif selected_table_ids:
            status = "confirmed"

        return {
            "status": status,
            "question": table_resolution_state.get("question") or candidate_snapshot.get("question") or state.get("question_text"),
            "message": table_resolution_state.get("message") or candidate_snapshot.get("message"),
            "reason_summary": table_resolution_state.get("reason_summary") or candidate_snapshot.get("confirmation_reason"),
            "candidates": candidates,
            "recommended_table_ids": recommended_table_ids,
            "selected_table_ids": selected_table_ids,
            "rejected_table_ids": rejected_table_ids,
            "allow_multi_select": bool(
                table_resolution_state.get("allow_multi_select")
                if table_resolution_state.get("allow_multi_select") is not None
                else candidate_snapshot.get("allow_multi_select")
            ),
            "multi_table_mode": (
                state.get("multi_table_mode")
                or table_resolution_state.get("multi_table_mode")
                or candidate_snapshot.get("multi_table_mode")
            ),
            "manual_table_override": bool(
                state.get("manual_table_override")
                if state.get("manual_table_override") is not None
                else table_resolution_state.get("manual_table_override")
            ),
        }

    @staticmethod
    def _build_table_resolution_provisional_draft(
        current_node: Optional[str],
        state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if current_node != "table_resolution":
            return None

        table_resolution_state = QuerySessionService._normalize_confirmation_state(state.get("table_resolution_state"))
        candidate_snapshot = QuerySessionService._normalize_state(state.get("candidate_snapshot"))
        if not any([table_resolution_state, candidate_snapshot]):
            return None

        return QuerySessionService.build_provisional_draft_state(
            table_resolution_state or candidate_snapshot,
            question_text=(
                state.get("resolved_question_text")
                or state.get("question_text")
                or table_resolution_state.get("question")
                or candidate_snapshot.get("question")
            ),
            selected_table_ids=QuerySessionService._get_selected_table_ids(state),
            recommended_table_ids=list(
                state.get("recommended_table_ids")
                or table_resolution_state.get("recommended_table_ids")
                or candidate_snapshot.get("recommended_table_ids")
                or []
            ),
            manual_table_override=(
                state.get("manual_table_override")
                if state.get("manual_table_override") is not None
                else table_resolution_state.get("manual_table_override")
            ),
            allow_multi_select=table_resolution_state.get("allow_multi_select"),
        )

    @staticmethod
    def _build_draft(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        provisional_draft = QuerySessionService._normalize_state(state.get("provisional_draft"))
        confirmed_draft = QuerySessionService._normalize_state(state.get("confirmed_draft"))
        draft_state = QuerySessionService._normalize_confirmation_state(state.get("draft_state"))
        draft_confirmation_card = QuerySessionService._normalize_state(state.get("draft_confirmation_card"))
        ir_snapshot = QuerySessionService._normalize_state(state.get("ir_snapshot"))
        table_resolution_provisional_draft = QuerySessionService._build_table_resolution_provisional_draft(
            current_node,
            state,
        )

        if not any([
            provisional_draft,
            confirmed_draft,
            draft_state,
            draft_confirmation_card,
            ir_snapshot,
            table_resolution_provisional_draft,
        ]):
            return None

        derived_draft_state = draft_state or QuerySessionService.build_draft_state(
            draft_confirmation_card,
            draft_json=draft_confirmation_card.get("ir") or ir_snapshot or None,
        )
        if provisional_draft:
            status = provisional_draft.get("status") or "provisional"
            natural_language = provisional_draft.get("natural_language")
            draft_json = provisional_draft.get("draft_json")
            warnings = list(provisional_draft.get("warnings") or [])
            suggestions = list(provisional_draft.get("suggestions") or [])
        elif confirmed_draft:
            status = confirmed_draft.get("status") or "confirmed"
            natural_language = confirmed_draft.get("natural_language")
            draft_json = confirmed_draft.get("draft_json")
            warnings = list(confirmed_draft.get("warnings") or [])
            suggestions = list(confirmed_draft.get("suggestions") or [])
        elif table_resolution_provisional_draft:
            status = table_resolution_provisional_draft.get("status") or "provisional"
            natural_language = table_resolution_provisional_draft.get("natural_language")
            draft_json = table_resolution_provisional_draft.get("draft_json")
            warnings = list(table_resolution_provisional_draft.get("warnings") or [])
            suggestions = list(table_resolution_provisional_draft.get("suggestions") or [])
        else:
            if current_node == "draft_confirmation":
                status = "awaiting_confirmation"
            elif state.get("draft_confirmation_approved"):
                status = "confirmed"
            else:
                status = derived_draft_state.get("status") or "available"
            natural_language = derived_draft_state.get("natural_language")
            draft_json = derived_draft_state.get("draft_json") or derived_draft_state.get("ir") or ir_snapshot or None
            warnings = list(derived_draft_state.get("warnings") or [])
            suggestions = list(derived_draft_state.get("suggestions") or [])

        confirmed = state.get("draft_confirmation_approved")
        if confirmed is None:
            confirmed = (
                table_resolution_provisional_draft.get("confirmed")
                if table_resolution_provisional_draft
                else derived_draft_state.get("confirmed")
            )

        confirmation_required = state.get("draft_confirmation_required")
        if confirmation_required is None:
            confirmation_required = (
                table_resolution_provisional_draft.get("confirmation_required")
                if table_resolution_provisional_draft
                else derived_draft_state.get("confirmation_required")
            )

        base_draft_state = table_resolution_provisional_draft or derived_draft_state

        table_dependent = base_draft_state.get("table_dependent")
        if table_dependent is None:
            table_dependent = True

        invalidate_on_table_change = base_draft_state.get("invalidate_on_table_change")
        if invalidate_on_table_change is None:
            invalidate_on_table_change = True

        return {
            "status": status,
            "table_dependent": bool(table_dependent),
            "invalidate_on_table_change": bool(invalidate_on_table_change),
            "natural_language": natural_language,
            "draft_json": draft_json,
            "warnings": warnings,
            "suggestions": suggestions,
            "confirmed": bool(confirmed),
            "confirmation_required": bool(confirmation_required),
        }

    @staticmethod
    def _build_execution_guard(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        execution_guard_state = QuerySessionService._normalize_confirmation_state(state.get("execution_guard_state"))
        execution_guard = QuerySessionService._normalize_state(state.get("execution_guard"))
        derived_execution_guard = execution_guard_state or QuerySessionService.build_execution_guard_state(execution_guard)
        if not derived_execution_guard and current_node != "execution_guard":
            return None

        status = derived_execution_guard.get("status") or "available"
        if current_node == "execution_guard":
            status = "awaiting_confirmation"
        elif state.get("execution_decision") == "approve":
            status = "approved"

        return {
            "status": status,
            "natural_language": derived_execution_guard.get("natural_language"),
            "warnings": list(derived_execution_guard.get("warnings") or []),
            "estimated_cost": derived_execution_guard.get("estimated_cost"),
            "ir": derived_execution_guard.get("ir") or state.get("ir_snapshot"),
        }

    @staticmethod
    def build_confirmation_view(session: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not session:
            return None

        current_node = session.get("current_node")
        state = QuerySessionService._normalize_state(session.get("state_json"))
        raw_pending_actions = list(state.get("pending_actions") or [])
        pending_actions = []
        action_bindings: Dict[str, str] = {}
        for action_type in raw_pending_actions:
            semantic_action = QuerySessionService._map_pending_action(current_node, action_type)
            pending_actions.append(semantic_action)
            action_bindings.setdefault(semantic_action, action_type)

        selected_table_ids = QuerySessionService._get_selected_table_ids(state)
        draft = QuerySessionService._build_draft(current_node, state)
        return {
            "query_id": session.get("query_id"),
            "session": {
                "status": session.get("status"),
                "current_node": current_node,
            },
            "context": {
                "question_text": state.get("question_text") or state.get("resolved_question_text"),
                "safe_summary": QuerySessionService._build_safe_summary(current_node, state),
            },
            "table_resolution": QuerySessionService._build_table_resolution(current_node, state),
            "draft": draft,
            "execution_guard": QuerySessionService._build_execution_guard(current_node, state),
            "pending_actions": pending_actions,
            "dependency_meta": {
                "draft_version": state.get("draft_version"),
                "selected_table_ids": selected_table_ids,
                "invalidated_artifacts": list(state.get("invalidated_artifacts") or []),
                "raw_pending_actions": raw_pending_actions,
                "action_bindings": action_bindings,
                "analysis_context": state.get("analysis_context"),
                "invalidate_on_table_change": bool(
                    draft
                    or state.get("ir_snapshot")
                    or state.get("sql_preview")
                    or state.get("result_meta")
                ),
            },
        }

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        session = {
            "query_id": str(row["query_id"]),
            "conversation_id": str(row["conversation_id"]) if row["conversation_id"] else None,
            "message_id": str(row["message_id"]) if row["message_id"] else None,
            "user_id": str(row["user_id"]) if row["user_id"] else None,
            "status": row["status"],
            "current_node": row["current_node"],
            "state_json": QuerySessionService._normalize_state(row["state_json"]),
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        session["confirmation_view"] = QuerySessionService.build_confirmation_view(session)
        return session

    async def get_session(self, query_id: UUID) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT query_id, conversation_id, message_id, user_id, status, current_node,
                       state_json, last_error, created_at, updated_at
                FROM query_sessions
                WHERE query_id = $1
                """,
                query_id,
            )
        return self._row_to_dict(row)

    async def upsert_session(
        self,
        *,
        query_id: UUID,
        user_id: Optional[UUID],
        status: str,
        current_node: str,
        state_json: Optional[Dict[str, Any]] = None,
        conversation_id: Optional[UUID] = None,
        message_id: Optional[UUID] = None,
        last_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        async with self._acquire() as conn:
            existing = await conn.fetchrow(
                """
                SELECT query_id, conversation_id, message_id, user_id, status, current_node,
                       state_json, last_error, created_at, updated_at
                FROM query_sessions
                WHERE query_id = $1
                """,
                query_id,
            )

            merged_state = self._merge_state(existing["state_json"] if existing else None, state_json)
            row = await conn.fetchrow(
                """
                INSERT INTO query_sessions (
                    query_id, conversation_id, message_id, user_id,
                    status, current_node, state_json, last_error
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                ON CONFLICT (query_id) DO UPDATE SET
                    conversation_id = COALESCE(EXCLUDED.conversation_id, query_sessions.conversation_id),
                    message_id = COALESCE(EXCLUDED.message_id, query_sessions.message_id),
                    user_id = COALESCE(EXCLUDED.user_id, query_sessions.user_id),
                    status = EXCLUDED.status,
                    current_node = EXCLUDED.current_node,
                    state_json = EXCLUDED.state_json,
                    last_error = EXCLUDED.last_error,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING query_id, conversation_id, message_id, user_id, status, current_node,
                          state_json, last_error, created_at, updated_at
                """,
                query_id,
                conversation_id,
                message_id,
                user_id,
                status,
                current_node,
                self._dump_state(merged_state),
                last_error,
            )

        logger.debug("查询会话已写入", query_id=str(query_id), status=status, current_node=current_node)
        return self._row_to_dict(row) or {}

    async def update_session(
        self,
        query_id: UUID,
        *,
        status: Optional[str] = None,
        current_node: Optional[str] = None,
        state_updates: Optional[Dict[str, Any]] = None,
        conversation_id: Optional[UUID] = None,
        message_id: Optional[UUID] = None,
        last_error: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            existing = await conn.fetchrow(
                """
                SELECT query_id, conversation_id, message_id, user_id, status, current_node,
                       state_json, last_error, created_at, updated_at
                FROM query_sessions
                WHERE query_id = $1
                """,
                query_id,
            )
            if not existing:
                return None

            row = await conn.fetchrow(
                """
                UPDATE query_sessions
                SET conversation_id = COALESCE($2, conversation_id),
                    message_id = COALESCE($3, message_id),
                    status = COALESCE($4, status),
                    current_node = COALESCE($5, current_node),
                    state_json = $6::jsonb,
                    last_error = $7,
                    updated_at = CURRENT_TIMESTAMP
                WHERE query_id = $1
                RETURNING query_id, conversation_id, message_id, user_id, status, current_node,
                          state_json, last_error, created_at, updated_at
                """,
                query_id,
                conversation_id,
                message_id,
                status,
                current_node,
                self._dump_state(self._merge_state(existing["state_json"], state_updates)),
                last_error,
            )

        logger.debug(
            "查询会话已更新",
            query_id=str(query_id),
            status=status or existing["status"],
            current_node=current_node or existing["current_node"],
        )
        return self._row_to_dict(row)
