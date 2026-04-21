from uuid import uuid4
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.query.routes import router


def build_test_client():
    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app)


ROOT_DIR = Path(__file__).resolve().parents[3]


def test_get_query_session_snapshot_route(monkeypatch):
    query_id = uuid4()

    async def fake_get_session(self, requested_query_id):
        assert requested_query_id == query_id
        return {
            "query_id": str(query_id),
            "status": "awaiting_user_action",
            "current_node": "table_resolution",
            "state_json": {
                "question_text": "查询土地利用现状",
                "pending_actions": ["confirm", "change_table"],
                "table_resolution_state": {
                    "reason_summary": "需要确认目标表",
                    "candidates": [{"table_id": "table_land_status", "table_name": "土地利用现状表"}],
                },
            },
        }

    monkeypatch.setattr("server.services.query_session_service.QuerySessionService.get_session", fake_get_session)
    client = build_test_client()

    response = client.get(f"/api/query-sessions/{query_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["query_id"] == str(query_id)
    assert "pending_actions" not in payload
    assert payload["confirmation_view"]["pending_actions"] == ["choose_table", "change_table"]
    assert payload["confirmation_view"]["table_resolution"]["reason_summary"] == "需要确认目标表"


def test_submit_query_session_action_route(monkeypatch):
    query_id = uuid4()

    async def fake_apply_action(self, **kwargs):
        assert kwargs["query_id"] == query_id
        assert kwargs["action_type"] == "confirm"
        return {
            "resolution": "resolved_to_action",
            "action": {"action_type": "confirm"},
            "resume_directive": {"should_resume": True, "query_id": str(query_id)},
            "session": {"status": "running", "current_node": "draft_generation"},
        }

    monkeypatch.setattr("server.services.draft_action_service.DraftActionService.apply_action", fake_apply_action)
    client = build_test_client()

    response = client.post(
        f"/api/query-sessions/{query_id}/actions",
        json={
            "action_type": "confirm",
            "payload": {},
            "draft_version": 1,
            "actor_type": "user",
            "actor_id": "u1",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolution"] == "resolved_to_action"
    assert payload["session"]["current_node"] == "draft_generation"
    assert payload["resume_directive"]["should_resume"] is True


def test_submit_query_session_action_route_accepts_semantic_action_alias(monkeypatch):
    query_id = uuid4()

    async def fake_apply_action(self, **kwargs):
        assert kwargs["query_id"] == query_id
        assert kwargs["action_type"] == "confirm_draft"
        return {
            "resolution": "resolved_to_action",
            "action": {"action_type": "confirm"},
            "session": {"status": "running", "current_node": "draft_generation"},
        }

    monkeypatch.setattr("server.services.draft_action_service.DraftActionService.apply_action", fake_apply_action)
    client = build_test_client()

    response = client.post(
        f"/api/query-sessions/{query_id}/actions",
        json={
            "action_type": "confirm_draft",
            "payload": {},
            "draft_version": 1,
            "actor_type": "user",
            "actor_id": "u1",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolution"] == "resolved_to_action"
    assert payload["action"]["action_type"] == "confirm"


def test_get_query_session_snapshot_route_includes_result_actions(monkeypatch):
    query_id = uuid4()

    async def fake_get_session(self, requested_query_id):
        assert requested_query_id == query_id
        return {
            "query_id": str(query_id),
            "status": "completed",
            "current_node": "completed",
            "state_json": {
                "question_text": "查询土地成交总价",
                "pending_actions": ["change_table", "revise", "request_explanation"],
                "selected_table_ids": ["table_land_deal"],
                "ir_snapshot": {"query_type": "aggregation"},
                "result_meta": {"row_count": 12},
            },
        }

    monkeypatch.setattr("server.services.query_session_service.QuerySessionService.get_session", fake_get_session)
    client = build_test_client()

    response = client.get(f"/api/query-sessions/{query_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["confirmation_view"]["result_actions"]["source_node"] == "completed"
    assert payload["confirmation_view"]["result_actions"]["available_actions"] == [
        "change_table",
        "revise",
        "request_explanation",
    ]


def test_submit_query_session_action_route_persists_user_reply_message(monkeypatch):
    query_id = uuid4()
    conversation_id = uuid4()
    captured = {}

    async def fake_apply_action(self, **kwargs):
        return {
            "resolution": "resolved_to_action",
            "action": {"action_type": "revise"},
            "session": {
                "query_id": str(query_id),
                "conversation_id": str(conversation_id),
                "status": "awaiting_user_action",
                "current_node": "draft_confirmation",
            },
            "resume_directive": {"should_resume": False},
            "idempotent_replay": False,
        }

    class FakePool:
        async def acquire(self):
            return object()

        async def release(self, conn):
            return None

    async def fake_get_metadata_pool():
        return FakePool()

    async def fake_add_message(self, **kwargs):
        captured.update(kwargs)
        return {"message_id": str(uuid4())}

    monkeypatch.setattr("server.services.draft_action_service.DraftActionService.apply_action", fake_apply_action)
    monkeypatch.setattr("server.api.query.routes.get_metadata_pool", fake_get_metadata_pool)
    monkeypatch.setattr("server.services.conversation_service.ConversationService.add_message", fake_add_message)
    client = build_test_client()

    response = client.post(
        f"/api/query-sessions/{query_id}/actions",
        json={
            "action_type": "revise",
            "payload": {"text": "请改成按区域统计"},
            "draft_version": 1,
            "actor_type": "user",
            "actor_id": "u1",
        },
    )

    assert response.status_code == 200
    assert captured["conversation_id"] == conversation_id
    assert captured["role"] == "user"
    assert captured["content"] == "请改成按区域统计"
    assert captured["metadata"]["query_session_reply"] is True


def test_submit_query_session_action_route_skips_persist_for_new_query_resolution(monkeypatch):
    query_id = uuid4()
    conversation_id = uuid4()
    captured = {"called": False}

    async def fake_apply_action(self, **kwargs):
        return {
            "resolution": "resolved_to_new_query",
            "new_query_text": "那武汉今年成交总价排名前十的是哪些地块？",
            "session": {
                "query_id": str(query_id),
                "conversation_id": str(conversation_id),
                "status": "awaiting_user_action",
                "current_node": "draft_confirmation",
            },
            "idempotent_replay": False,
        }

    class FakePool:
        async def acquire(self):
            return object()

        async def release(self, conn):
            return None

    async def fake_get_metadata_pool():
        return FakePool()

    async def fake_add_message(self, **kwargs):
        captured["called"] = True
        return {"message_id": str(uuid4())}

    monkeypatch.setattr("server.services.draft_action_service.DraftActionService.apply_action", fake_apply_action)
    monkeypatch.setattr("server.api.query.routes.get_metadata_pool", fake_get_metadata_pool)
    monkeypatch.setattr("server.services.conversation_service.ConversationService.add_message", fake_add_message)
    client = build_test_client()

    response = client.post(
        f"/api/query-sessions/{query_id}/actions",
        json={
            "natural_language_reply": "那武汉今年成交总价排名前十的是哪些地块？",
            "draft_version": 1,
            "actor_type": "user",
            "actor_id": "u1",
        },
    )

    assert response.status_code == 200
    assert response.json()["resolution"] == "resolved_to_new_query"
    assert captured["called"] is False


def test_query_route_prefers_context_query_id_and_persists_before_streaming():
    routes_file = ROOT_DIR / "server" / "api" / "query" / "routes.py"
    content = routes_file.read_text(encoding="utf-8")
    api_models_file = ROOT_DIR / "server" / "models" / "api.py"
    api_models_content = api_models_file.read_text(encoding="utf-8")

    assert "query_id = _query_id_ctx.get() or request.original_query_id or str(uuid.uuid4())" in content
    assert "resume_as_new_turn: bool = False" in api_models_content
    assert "and not request.resume_as_new_turn" in content
    assert "elif request.resume_as_new_turn:" in content
    assert "async def _build_ir_display_dict_with_units(" in content
    assert "return f\"{normalized_name}（{normalized_unit}）\"" in content
    assert "async def _return_streamed_query_response(" in content
    assert "query_sessions 记录缺失，改为补建" in content
    assert "query_sessions 持久化失败，准备降级重试" in content
    assert "stream_event={\"type\": \"table_selection\", \"payload\": pending_card}" in content
    assert "stream_event={\"type\": \"table_selection\", \"payload\": fallback_card}" in content
    assert "stream_event={\"type\": \"confirmation\", \"payload\": confirm_card}" in content
