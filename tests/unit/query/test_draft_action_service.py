from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from server.services.draft_action_service import DraftActionService
from server.services.query_session_service import QuerySessionService


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeActionDB:
    def __init__(self):
        self.sessions = {}
        self.actions = []

    def transaction(self):
        return FakeTransaction()

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
            updated = {
                **existing,
                "conversation_id": conversation_id or existing["conversation_id"],
                "message_id": message_id or existing["message_id"],
                "status": status or existing["status"],
                "current_node": current_node or existing["current_node"],
                "state_json": json.loads(state_json),
                "last_error": last_error,
                "updated_at": datetime.now(timezone.utc),
            }
            self.sessions[query_id] = updated
            return updated

        if "FROM draft_actions" in query and "idempotency_key" in query:
            query_id, idempotency_key = params
            for action in self.actions:
                if action["query_id"] == query_id and action["idempotency_key"] == idempotency_key:
                    return action
            return None

        if "INSERT INTO draft_actions" in query:
            action_id, query_id, draft_version, action_type, actor_type, actor_id, payload_json, idempotency_key = params
            row = {
                "action_id": action_id,
                "query_id": query_id,
                "draft_version": draft_version,
                "action_type": action_type,
                "actor_type": actor_type,
                "actor_id": actor_id,
                "payload_json": json.loads(payload_json),
                "idempotency_key": idempotency_key,
                "created_at": datetime.now(timezone.utc),
            }
            self.actions.append(row)
            return row

        raise AssertionError(f"unexpected query: {query}")


@pytest.mark.asyncio
async def test_confirm_action_advances_table_resolution_to_draft_generation():
    db = FakeActionDB()
    query_id = uuid4()
    session_service = QuerySessionService(db)
    await session_service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="table_resolution",
        state_json={
            "draft_version": 1,
            "question_text": "查询土地成交情况",
            "pending_actions": ["confirm", "change_table", "request_explanation", "exit_current"],
            "recommended_table_ids": ["table_land_deal"],
        },
    )

    service = DraftActionService(db)
    result = await service.apply_action(
        query_id=query_id,
        action_type="confirm",
        payload={},
        natural_language_reply=None,
        draft_version=1,
        actor_type="user",
        actor_id="u1",
        idempotency_key="confirm-1",
    )

    assert result["resolution"] == "resolved_to_action"
    assert result["action"]["action_type"] == "confirm"
    assert result["session"]["status"] == "running"
    assert result["session"]["current_node"] == "draft_generation"
    assert result["session"]["state_json"]["selected_table_ids"] == ["table_land_deal"]
    assert result["session"]["state_json"]["draft_confirmation_required"] is True
    assert result["session"]["state_json"]["draft_confirmation_approved"] is False
    assert result["resume_directive"]["should_resume"] is True
    assert result["resume_directive"]["text"] == "查询土地成交情况"
    assert result["resume_directive"]["ir"] is None
    assert result["resume_directive"]["progress_text"] == "正在使用确认后的表继续查询..."


@pytest.mark.asyncio
async def test_revise_action_advances_to_draft_generation_and_keeps_revision_request():
    db = FakeActionDB()
    query_id = uuid4()
    session_service = QuerySessionService(db)
    await session_service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="execution_guard",
        state_json={
            "draft_version": 2,
            "question_text": "查询土地成交情况",
            "pending_actions": ["execution_decision", "revise", "change_table", "request_explanation", "exit_current"],
            "selected_table_ids": ["table_land_deal"],
            "ir_snapshot": {"query_type": "aggregation"},
        },
    )

    service = DraftActionService(db)
    result = await service.apply_action(
        query_id=query_id,
        action_type="revise",
        payload={"text": "改成查询武汉市"},
        natural_language_reply=None,
        draft_version=2,
        actor_type="user",
        actor_id="u1",
        idempotency_key="revise-1",
    )

    assert result["action"]["action_type"] == "revise"
    assert result["session"]["status"] == "running"
    assert result["session"]["current_node"] == "draft_generation"
    assert result["session"]["state_json"]["revision_request"]["text"] == "改成查询武汉市"
    assert result["session"]["state_json"]["draft_confirmation_required"] is True
    assert result["session"]["state_json"]["draft_confirmation_approved"] is False
    assert result["session"]["state_json"]["ir_snapshot"] is None
    assert result["resume_directive"]["should_resume"] is True
    assert result["resume_directive"]["text"] == "查询土地成交情况"
    assert result["resume_directive"]["progress_text"] == "正在根据修改意见重算确认稿..."


@pytest.mark.asyncio
async def test_confirm_from_draft_confirmation_marks_draft_as_approved():
    db = FakeActionDB()
    query_id = uuid4()
    session_service = QuerySessionService(db)
    await session_service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="draft_confirmation",
        state_json={
            "draft_version": 3,
            "question_text": "查询土地成交均价",
            "pending_actions": ["confirm", "revise", "change_table", "request_explanation", "exit_current"],
            "selected_table_ids": ["table_land_deal"],
            "ir_snapshot": {"query_type": "aggregation"},
        },
    )

    service = DraftActionService(db)
    result = await service.apply_action(
        query_id=query_id,
        action_type="confirm",
        payload={},
        natural_language_reply=None,
        draft_version=3,
        actor_type="user",
        actor_id="u1",
        idempotency_key="draft-confirm-1",
    )

    assert result["action"]["action_type"] == "confirm"
    assert result["session"]["current_node"] == "draft_generation"
    assert result["session"]["state_json"]["draft_confirmation_approved"] is True
    assert result["session"]["state_json"]["selected_table_ids"] == ["table_land_deal"]
    assert result["resume_directive"]["should_resume"] is True
    assert result["resume_directive"]["text"] == "查询土地成交均价"
    assert result["resume_directive"]["ir"] == {"query_type": "aggregation"}
    assert result["resume_directive"]["progress_text"] == "正在基于已确认草稿继续查询..."


@pytest.mark.asyncio
async def test_execution_approve_returns_resume_directive():
    db = FakeActionDB()
    query_id = uuid4()
    session_service = QuerySessionService(db)
    await session_service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="execution_guard",
        state_json={
            "draft_version": 4,
            "question_text": "查询土地成交总价",
            "pending_actions": ["execution_decision", "revise", "change_table", "request_explanation", "exit_current"],
            "selected_table_ids": ["table_land_deal"],
            "ir_snapshot": {"query_type": "aggregation"},
        },
    )

    service = DraftActionService(db)
    result = await service.apply_action(
        query_id=query_id,
        action_type="execution_decision",
        payload={"decision": "approve"},
        natural_language_reply=None,
        draft_version=4,
        actor_type="user",
        actor_id="u1",
        idempotency_key="execution-approve-1",
    )

    assert result["session"]["current_node"] == "execution_approved"
    assert result["resume_directive"]["should_resume"] is True
    assert result["resume_directive"]["force_execute"] is True
    assert result["resume_directive"]["ir"] == {"query_type": "aggregation"}
    assert result["resume_directive"]["progress_text"] == "正在继续执行查询..."


@pytest.mark.asyncio
async def test_natural_language_change_table_invalidates_ir_artifacts():
    db = FakeActionDB()
    query_id = uuid4()
    session_service = QuerySessionService(db)
    await session_service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="completed",
        current_node="completed",
        state_json={
            "draft_version": 2,
            "pending_actions": ["change_table", "revise", "request_explanation", "exit_current"],
            "selected_table_ids": ["table_land_deal"],
        },
    )

    service = DraftActionService(db)
    result = await service.apply_action(
        query_id=query_id,
        action_type=None,
        payload={},
        natural_language_reply="不是这张表，换表",
        draft_version=2,
        actor_type="user",
        actor_id="u1",
        idempotency_key="change-table-1",
    )

    assert result["action"]["action_type"] == "change_table"
    assert result["session"]["current_node"] == "table_resolution"
    assert result["session"]["state_json"]["invalidated_artifacts"] == ["draft", "ir", "sql", "result"]
    assert result["session"]["state_json"]["ir_ready"] is False
    assert result["session"]["state_json"]["ir_snapshot"] is None
    assert result["session"]["state_json"]["sql_preview"] is None
    assert "table_land_deal" in result["session"]["state_json"]["rejected_table_ids"]
    assert result["interruption"]["requested"] is False


@pytest.mark.asyncio
async def test_change_table_requests_stop_signal_for_running_query(monkeypatch):
    db = FakeActionDB()
    query_id = uuid4()
    user_id = uuid4()
    message_id = uuid4()
    session_service = QuerySessionService(db)
    await session_service.upsert_session(
        query_id=query_id,
        user_id=user_id,
        status="running",
        current_node="table_resolved",
        state_json={
            "draft_version": 3,
            "selected_table_ids": ["table_land_deal"],
        },
        message_id=message_id,
    )

    stop_calls = []
    cancel_calls = []

    def fake_set_stop_signal(requested_message_id):
        stop_calls.append(requested_message_id)
        return True

    async def fake_mark_cancelling(self, requested_query_id, requested_user_id):
        cancel_calls.append((requested_query_id, requested_user_id))
        return True

    monkeypatch.setattr(
        "server.services.stop_signal_service.StopSignalService.set_stop_signal",
        fake_set_stop_signal,
    )
    monkeypatch.setattr(
        "server.services.conversation_service.ActiveQueryRegistry.mark_cancelling",
        fake_mark_cancelling,
    )

    service = DraftActionService(db)
    result = await service.apply_action(
        query_id=query_id,
        action_type="change_table",
        payload={"reason": "用户判断表不对"},
        natural_language_reply=None,
        draft_version=3,
        actor_type="user",
        actor_id="u1",
        idempotency_key="change-table-running-1",
    )

    assert result["session"]["current_node"] == "table_resolution"
    assert result["session"]["state_json"]["interruption_requested"] is True
    assert result["session"]["state_json"]["interrupt_target_message_id"] == str(message_id)
    assert result["interruption"]["requested"] is True
    assert result["interruption"]["stop_signal_sent"] is True
    assert result["interruption"]["registry_marked"] is True
    assert stop_calls == [str(message_id)]
    assert cancel_calls == [(query_id, user_id)]


@pytest.mark.asyncio
async def test_action_validation_rejects_disallowed_transition():
    db = FakeActionDB()
    query_id = uuid4()
    session_service = QuerySessionService(db)
    await session_service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="execution_guard",
        state_json={
            "draft_version": 1,
            "pending_actions": ["execution_decision", "request_explanation", "exit_current"],
        },
    )

    service = DraftActionService(db)
    with pytest.raises(ValueError, match="不允许动作 choose_option"):
        await service.apply_action(
            query_id=query_id,
            action_type="choose_option",
            payload={},
            natural_language_reply=None,
            draft_version=1,
            actor_type="user",
            actor_id="u1",
            idempotency_key="bad-action",
        )
