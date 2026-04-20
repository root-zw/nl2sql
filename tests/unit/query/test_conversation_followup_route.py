from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.conversations import router, get_db_connection
from server.middleware.auth import get_current_active_user


def build_test_client():
    app = FastAPI()
    app.include_router(router, prefix="/api")

    async def fake_current_user():
        return SimpleNamespace(user_id=uuid4(), role="user")

    async def fake_db_connection():
        yield object()

    app.dependency_overrides[get_current_active_user] = fake_current_user
    app.dependency_overrides[get_db_connection] = fake_db_connection
    return TestClient(app)


def test_resolve_followup_context_route(monkeypatch):
    conversation_id = uuid4()
    target_conversation_id = conversation_id

    async def fake_get_conversation(self, conversation_id, user_id):
        assert conversation_id == target_conversation_id
        return {"conversation_id": str(target_conversation_id)}

    async def fake_get_recent_context(self, conversation_id, depth=None):
        return [
            {
                "message_id": "m1",
                "role": "assistant",
                "status": "completed",
                "query_id": "q1",
                "result_summary": "上一结果按区域展示了武汉土地成交总价。",
                "result_data": {"meta": {"selected_table_ids": ["table_land_deal"]}},
            }
        ]

    monkeypatch.setattr("server.services.conversation_service.ConversationService.get_conversation", fake_get_conversation)
    monkeypatch.setattr("server.services.conversation_service.ConversationService.get_recent_context", fake_get_recent_context)
    client = build_test_client()

    response = client.post(
        f"/api/conversations/{conversation_id}/followup-context-resolution",
        json={"text": "那按区域展开看一下呢？"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolution"] == "continue_on_result"
    assert payload["analysis_context"]["inherit_from_query_id"] == "q1"
