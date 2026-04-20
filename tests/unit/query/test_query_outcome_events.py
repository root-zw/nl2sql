from uuid import uuid4

import pytest

from server.api.query.routes import (
    _build_query_outcome_event_key,
    _emit_query_outcome_event,
    _map_query_outcome_event_type,
)


def test_map_query_outcome_event_type_returns_expected_event_names():
    assert _map_query_outcome_event_type("completed") == "execution_completed"
    assert _map_query_outcome_event_type("failed") == "execution_failed"
    assert _map_query_outcome_event_type("cancelled") == "execution_cancelled"
    assert _map_query_outcome_event_type("running") is None


def test_build_query_outcome_event_key_uses_message_id_and_status():
    message_id = uuid4()

    assert _build_query_outcome_event_key("q1", message_id, "completed") == f"query_outcome:q1:{message_id}:completed"
    assert _build_query_outcome_event_key("q1", None, "failed") == "query_outcome:q1:none:failed"


@pytest.mark.asyncio
async def test_emit_query_outcome_event_records_learning_event(monkeypatch):
    calls = []

    async def fake_record_event(self, **kwargs):
        calls.append(kwargs)
        return {"event_key": kwargs["event_key"]}

    monkeypatch.setattr("server.services.learning_event_service.LearningEventService.record_event", fake_record_event)

    query_id = str(uuid4())
    message_id = uuid4()
    conversation_id = uuid4()
    user_id = uuid4()

    await _emit_query_outcome_event(
        query_id=query_id,
        message_id=message_id,
        conversation_id=conversation_id,
        user_id=user_id,
        status="failed",
        current_node="failed",
        question_text="查询武汉土地成交总价",
        selected_table_ids=["table_land_deal"],
        result_row_count=None,
        error={"message": "权限不足"},
        request_context={"force_execute": False},
        last_error="权限不足",
    )

    assert len(calls) == 1
    payload = calls[0]
    assert payload["event_type"] == "execution_failed"
    assert payload["event_key"] == f"query_outcome:{query_id}:{message_id}:failed"
    assert payload["payload_json"]["query_status"] == "failed"
    assert payload["payload_json"]["selected_table_ids"] == ["table_land_deal"]
    assert payload["payload_json"]["error"]["message"] == "权限不足"


@pytest.mark.asyncio
async def test_emit_query_outcome_event_ignores_non_terminal_status(monkeypatch):
    called = False

    async def fake_record_event(self, **kwargs):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr("server.services.learning_event_service.LearningEventService.record_event", fake_record_event)

    await _emit_query_outcome_event(
        query_id=str(uuid4()),
        message_id=None,
        conversation_id=None,
        user_id=None,
        status="running",
        current_node="draft_generation",
        question_text="查询武汉土地成交总价",
    )

    assert called is False
