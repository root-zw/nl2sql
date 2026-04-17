"""
查询会话状态服务

统一维护查询产品级状态，避免状态仅散落在消息、活跃查询和 trace 中。
"""

from __future__ import annotations

import json
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
        return json.dumps(sanitize_for_json(state or {}), ensure_ascii=False)

    @staticmethod
    def _merge_state(current_state: Optional[Dict[str, Any]], updates: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        merged = dict(current_state or {})
        if updates:
            merged.update(sanitize_for_json(updates))
        return merged

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "query_id": str(row["query_id"]),
            "conversation_id": str(row["conversation_id"]) if row["conversation_id"] else None,
            "message_id": str(row["message_id"]) if row["message_id"] else None,
            "user_id": str(row["user_id"]) if row["user_id"] else None,
            "status": row["status"],
            "current_node": row["current_node"],
            "state_json": row["state_json"] or {},
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

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
