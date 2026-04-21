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


def test_get_conversation_route_returns_active_query_session(monkeypatch):
    conversation_id = uuid4()
    user_id = uuid4()

    async def fake_current_user():
        return SimpleNamespace(user_id=user_id, role="user")

    async def fake_get_conversation(self, conversation_id, user_id):
        return {
            "conversation_id": str(conversation_id),
            "user_id": str(user_id),
            "title": "测试会话",
            "connection_id": None,
            "domain_id": None,
            "connection_name": None,
            "domain_name": None,
            "is_active": True,
            "is_pinned": False,
            "message_count": 2,
            "created_at": "2026-04-21T10:00:00+08:00",
            "updated_at": "2026-04-21T10:00:00+08:00",
            "last_message_at": "2026-04-21T10:00:00+08:00",
        }

    async def fake_get_messages(self, conversation_id, include_result_data=True):
        return [
            {
                "message_id": str(uuid4()),
                "conversation_id": str(conversation_id),
                "role": "user",
                "content": "查一下武汉土地成交均价",
                "query_id": None,
                "sql_text": None,
                "result_summary": None,
                "result_data": None,
                "status": "completed",
                "error_message": None,
                "query_params": None,
                "context_message_ids": None,
                "metadata": None,
                "created_at": "2026-04-21T10:00:00+08:00",
                "updated_at": "2026-04-21T10:00:00+08:00",
            }
        ]

    async def fake_get_latest_pending_session(self, requested_conversation_id):
        assert requested_conversation_id == conversation_id
        return {
            "query_id": str(uuid4()),
            "message_id": str(uuid4()),
            "status": "awaiting_user_action",
            "current_node": "table_resolution",
            "state_json": {"question_text": "查一下武汉土地成交均价"},
            "confirmation_view": {
                "context": {"question_text": "查一下武汉土地成交均价"},
                "pending_actions": ["choose_table"],
            },
        }

    monkeypatch.setattr("server.services.conversation_service.ConversationService.get_conversation", fake_get_conversation)
    monkeypatch.setattr("server.services.conversation_service.ConversationService.get_messages", fake_get_messages)
    monkeypatch.setattr(
        "server.services.query_session_service.QuerySessionService.get_latest_pending_session_for_conversation",
        fake_get_latest_pending_session,
    )

    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.dependency_overrides[get_current_active_user] = fake_current_user

    async def fake_db_connection():
        yield object()

    app.dependency_overrides[get_db_connection] = fake_db_connection
    client = TestClient(app)

    response = client.get(f"/api/conversations/{conversation_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["conversation"]["conversation_id"] == str(conversation_id)
    assert payload["active_query_session"]["status"] == "awaiting_user_action"
    assert payload["active_query_session"]["current_node"] == "table_resolution"
