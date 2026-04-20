from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.query.routes import router


def build_test_client():
    app = FastAPI()
    app.include_router(router, prefix="/api")
    return TestClient(app)


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
                "candidate_snapshot": {
                    "confirmation_reason": "需要确认目标表",
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
    assert payload["pending_actions"] == ["confirm", "change_table"]
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
