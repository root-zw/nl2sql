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
    def _get_selected_table_ids(state: Dict[str, Any]) -> list[str]:
        selected_table_ids = list(state.get("selected_table_ids") or [])
        selected_table_id = state.get("selected_table_id")
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
        candidate_snapshot = QuerySessionService._normalize_state(state.get("candidate_snapshot"))
        selected_table_ids = QuerySessionService._get_selected_table_ids(state)
        recommended_table_ids = list(
            state.get("recommended_table_ids")
            or candidate_snapshot.get("recommended_table_ids")
            or []
        )
        rejected_table_ids = list(state.get("rejected_table_ids") or [])
        candidates = list(candidate_snapshot.get("candidates") or [])

        if not any([candidate_snapshot, selected_table_ids, recommended_table_ids, rejected_table_ids]):
            return None

        status = "idle"
        if current_node == "table_resolution":
            status = "awaiting_confirmation"
        elif selected_table_ids:
            status = "confirmed"

        return {
            "status": status,
            "question": candidate_snapshot.get("question") or state.get("question_text"),
            "message": candidate_snapshot.get("message"),
            "reason_summary": candidate_snapshot.get("confirmation_reason"),
            "candidates": candidates,
            "recommended_table_ids": recommended_table_ids,
            "selected_table_ids": selected_table_ids,
            "rejected_table_ids": rejected_table_ids,
            "allow_multi_select": bool(candidate_snapshot.get("allow_multi_select")),
            "multi_table_mode": state.get("multi_table_mode") or candidate_snapshot.get("multi_table_mode"),
            "manual_table_override": bool(state.get("manual_table_override")),
        }

    @staticmethod
    def _build_draft(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        provisional_draft = QuerySessionService._normalize_state(state.get("provisional_draft"))
        confirmed_draft = QuerySessionService._normalize_state(state.get("confirmed_draft"))
        draft_confirmation_card = QuerySessionService._normalize_state(state.get("draft_confirmation_card"))
        ir_snapshot = QuerySessionService._normalize_state(state.get("ir_snapshot"))

        if not any([provisional_draft, confirmed_draft, draft_confirmation_card, ir_snapshot]):
            return None

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
        else:
            if current_node == "draft_confirmation":
                status = "awaiting_confirmation"
            elif state.get("draft_confirmation_approved"):
                status = "confirmed"
            else:
                status = "available"
            natural_language = draft_confirmation_card.get("natural_language")
            draft_json = draft_confirmation_card.get("ir") or ir_snapshot or None
            warnings = list(draft_confirmation_card.get("warnings") or [])
            suggestions = list(draft_confirmation_card.get("suggestions") or [])

        return {
            "status": status,
            "table_dependent": True,
            "invalidate_on_table_change": True,
            "natural_language": natural_language,
            "draft_json": draft_json,
            "warnings": warnings,
            "suggestions": suggestions,
            "confirmed": bool(state.get("draft_confirmation_approved")),
            "confirmation_required": bool(state.get("draft_confirmation_required")),
        }

    @staticmethod
    def _build_execution_guard(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        execution_guard = QuerySessionService._normalize_state(state.get("execution_guard"))
        if not execution_guard and current_node != "execution_guard":
            return None

        status = "available"
        if current_node == "execution_guard":
            status = "awaiting_confirmation"
        elif state.get("execution_decision") == "approve":
            status = "approved"

        return {
            "status": status,
            "natural_language": execution_guard.get("natural_language"),
            "warnings": list(execution_guard.get("warnings") or []),
            "estimated_cost": execution_guard.get("estimated_cost"),
            "ir": execution_guard.get("ir") or state.get("ir_snapshot"),
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
