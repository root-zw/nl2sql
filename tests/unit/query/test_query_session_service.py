from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from server.services.query_session_service import QuerySessionService


class FakeQuerySessionDB:
    def __init__(self):
        self.sessions = {}

    async def fetchrow(self, query, *params):
        if "FROM query_sessions" in query and query.lstrip().startswith("SELECT"):
            return self.sessions.get(params[0])

        if "INSERT INTO query_sessions" in query:
            query_id, conversation_id, message_id, user_id, status, current_node, state_json, last_error = params
            now = datetime.now(timezone.utc)
            existing = self.sessions.get(query_id)
            created_at = existing["created_at"] if existing else now
            row = {
                "query_id": query_id,
                "conversation_id": conversation_id or (existing["conversation_id"] if existing else None),
                "message_id": message_id or (existing["message_id"] if existing else None),
                "user_id": user_id or (existing["user_id"] if existing else None),
                "status": status,
                "current_node": current_node,
                "state_json": json.loads(state_json),
                "last_error": last_error,
                "created_at": created_at,
                "updated_at": now,
            }
            self.sessions[query_id] = row
            return row

        if "UPDATE query_sessions" in query:
            query_id, conversation_id, message_id, status, current_node, state_json, last_error = params
            existing = self.sessions[query_id]
            now = datetime.now(timezone.utc)
            updated = {
                **existing,
                "conversation_id": conversation_id or existing["conversation_id"],
                "message_id": message_id or existing["message_id"],
                "status": status or existing["status"],
                "current_node": current_node or existing["current_node"],
                "state_json": json.loads(state_json),
                "last_error": last_error,
                "updated_at": now,
            }
            self.sessions[query_id] = updated
            return updated

        raise AssertionError(f"unexpected query: {query}")


@pytest.mark.asyncio
async def test_upsert_session_creates_query_session_with_state():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()
    user_id = uuid4()

    result = await service.upsert_session(
        query_id=query_id,
        user_id=user_id,
        status="running",
        current_node="question_intake",
        state_json={"question_text": "查一下武汉土地成交均价", "pending_actions": []},
    )

    assert result["query_id"] == str(query_id)
    assert result["user_id"] == str(user_id)
    assert result["status"] == "running"
    assert result["current_node"] == "question_intake"
    assert result["state_json"]["question_text"] == "查一下武汉土地成交均价"


@pytest.mark.asyncio
async def test_update_session_merges_state_without_losing_existing_fields():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="running",
        current_node="question_intake",
        state_json={
            "question_text": "查一下滨江地产拿地均价",
            "pending_actions": [],
        },
    )

    updated = await service.update_session(
        query_id,
        status="awaiting_user_action",
        current_node="table_resolution",
        state_updates={
            "pending_actions": ["choose_table", "manual_select_table"],
            "recommended_table_ids": ["table_1"],
        },
    )

    assert updated is not None
    assert updated["status"] == "awaiting_user_action"
    assert updated["current_node"] == "table_resolution"
    assert updated["state_json"]["question_text"] == "查一下滨江地产拿地均价"
    assert updated["state_json"]["pending_actions"] == ["choose_table", "manual_select_table"]
    assert updated["state_json"]["recommended_table_ids"] == ["table_1"]


@pytest.mark.asyncio
async def test_get_session_returns_none_for_unknown_query():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)

    result = await service.get_session(uuid4())

    assert result is None
