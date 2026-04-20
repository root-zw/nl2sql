"""
学习事件服务

统一记录可复用的交互事实，作为后续治理和记忆层的事实来源。
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


class LearningEventService:
    """学习事件写入服务"""

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
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "event_id": str(row["event_id"]),
            "event_key": row["event_key"],
            "query_id": str(row["query_id"]) if row["query_id"] else None,
            "conversation_id": str(row["conversation_id"]) if row["conversation_id"] else None,
            "user_id": str(row["user_id"]) if row["user_id"] else None,
            "event_type": row["event_type"],
            "event_version": row["event_version"],
            "payload_json": row["payload_json"] or {},
            "source_component": row["source_component"],
            "created_at": row["created_at"],
        }

    async def get_by_event_key(self, event_key: str) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT event_id, event_key, query_id, conversation_id, user_id,
                       event_type, event_version, payload_json, source_component, created_at
                FROM learning_events
                WHERE event_key = $1
                """,
                event_key,
            )
        return self._row_to_dict(row)

    async def record_event(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_json: Optional[Dict[str, Any]],
        source_component: str,
        query_id: Optional[UUID] = None,
        conversation_id: Optional[UUID] = None,
        user_id: Optional[UUID] = None,
        event_version: int = 1,
    ) -> Dict[str, Any]:
        normalized_payload = sanitize_for_json(payload_json or {})

        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO learning_events (
                    event_key, query_id, conversation_id, user_id,
                    event_type, event_version, payload_json, source_component
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                ON CONFLICT (event_key) DO NOTHING
                RETURNING event_id, event_key, query_id, conversation_id, user_id,
                          event_type, event_version, payload_json, source_component, created_at
                """,
                event_key,
                query_id,
                conversation_id,
                user_id,
                event_type,
                event_version,
                json.dumps(normalized_payload, ensure_ascii=False),
                source_component,
            )
            if row:
                logger.debug("学习事件已写入", event_key=event_key, event_type=event_type)
                return self._row_to_dict(row) or {}

            existing = await conn.fetchrow(
                """
                SELECT event_id, event_key, query_id, conversation_id, user_id,
                       event_type, event_version, payload_json, source_component, created_at
                FROM learning_events
                WHERE event_key = $1
                """,
                event_key,
            )
            logger.debug("学习事件命中去重", event_key=event_key, event_type=event_type)
            return self._row_to_dict(existing) or {}
