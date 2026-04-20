from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from server.services.learning_event_service import LearningEventService


class FakeLearningEventDB:
    def __init__(self):
        self.events = {}

    async def fetchrow(self, query, *params):
        if "INSERT INTO learning_events" in query:
            (
                event_key,
                query_id,
                conversation_id,
                user_id,
                event_type,
                event_version,
                payload_json,
                source_component,
            ) = params
            existing = self.events.get(event_key)
            if existing:
                return None

            row = {
                "event_id": uuid4(),
                "event_key": event_key,
                "query_id": query_id,
                "conversation_id": conversation_id,
                "user_id": user_id,
                "event_type": event_type,
                "event_version": event_version,
                "payload_json": json.loads(payload_json),
                "source_component": source_component,
                "created_at": datetime.now(timezone.utc),
            }
            self.events[event_key] = row
            return row

        if "FROM learning_events" in query and "WHERE event_key = $1" in query:
            return self.events.get(params[0])

        raise AssertionError(f"unexpected query: {query}")


@pytest.mark.asyncio
async def test_record_event_deduplicates_by_event_key():
    db = FakeLearningEventDB()
    service = LearningEventService(db)
    query_id = uuid4()

    first = await service.record_event(
        event_key="draft_action:1",
        query_id=query_id,
        conversation_id=None,
        user_id=None,
        event_type="action_applied",
        event_version=1,
        payload_json={"action_type": "confirm"},
        source_component="draft_action_service",
    )
    second = await service.record_event(
        event_key="draft_action:1",
        query_id=query_id,
        conversation_id=None,
        user_id=None,
        event_type="action_applied",
        event_version=1,
        payload_json={"action_type": "confirm"},
        source_component="draft_action_service",
    )

    assert first["event_key"] == "draft_action:1"
    assert second["event_id"] == first["event_id"]
    assert len(db.events) == 1
    assert second["payload_json"]["action_type"] == "confirm"
