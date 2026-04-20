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
async def test_update_session_accepts_json_string_state_from_database():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="running",
        current_node="question_intake",
        state_json={
            "question_text": "查一下武汉土地成交均价",
            "pending_actions": [],
        },
    )
    db.sessions[query_id]["state_json"] = json.dumps(db.sessions[query_id]["state_json"], ensure_ascii=False)

    updated = await service.update_session(
        query_id,
        status="awaiting_user_action",
        current_node="draft_confirmation",
        state_updates={
            "pending_actions": ["confirm", "revise"],
            "draft_confirmation_required": True,
        },
    )

    assert updated is not None
    assert updated["current_node"] == "draft_confirmation"
    assert updated["state_json"]["question_text"] == "查一下武汉土地成交均价"
    assert updated["state_json"]["pending_actions"] == ["confirm", "revise"]
    assert updated["state_json"]["draft_confirmation_required"] is True


@pytest.mark.asyncio
async def test_get_session_parses_json_string_state():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="table_resolution",
        state_json={
            "pending_actions": ["confirm", "change_table"],
            "question_text": "查询土地利用现状",
        },
    )
    db.sessions[query_id]["state_json"] = json.dumps(db.sessions[query_id]["state_json"], ensure_ascii=False)

    result = await service.get_session(query_id)

    assert result is not None
    assert result["current_node"] == "table_resolution"
    assert result["state_json"]["pending_actions"] == ["confirm", "change_table"]
    assert result["state_json"]["question_text"] == "查询土地利用现状"


@pytest.mark.asyncio
async def test_get_session_derives_confirmation_view_for_table_resolution():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="table_resolution",
        state_json={
            "question_text": "查询土地利用现状",
            "pending_actions": ["confirm", "change_table", "exit_current"],
            "recommended_table_ids": ["table_land_status"],
            "candidate_snapshot": {
                "question": "查询土地利用现状",
                "confirmation_reason": "存在多个相近业务表，请确认",
                "candidates": [
                    {"table_id": "table_land_status", "table_name": "土地利用现状表", "confidence": 0.82},
                ],
            },
        },
    )

    result = await service.get_session(query_id)

    assert result is not None
    confirmation_view = result["confirmation_view"]
    assert confirmation_view["context"]["safe_summary"]["user_goal_summary"] == "查询土地利用现状"
    assert confirmation_view["context"]["safe_summary"]["open_points"] == ["需确认应使用哪张数据表"]
    assert confirmation_view["table_resolution"]["status"] == "awaiting_confirmation"
    assert confirmation_view["table_resolution"]["reason_summary"] == "存在多个相近业务表，请确认"
    assert confirmation_view["pending_actions"] == ["choose_table", "change_table", "cancel_query"]
    assert confirmation_view["dependency_meta"]["action_bindings"]["choose_table"] == "confirm"
    assert confirmation_view["dependency_meta"]["raw_pending_actions"] == ["confirm", "change_table", "exit_current"]


@pytest.mark.asyncio
async def test_get_session_derives_confirmation_view_from_unified_table_resolution_state():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="table_resolution",
        state_json={
            "question_text": "查询土地利用现状",
            "pending_actions": ["confirm", "change_table"],
            "table_resolution_state": {
                "question": "查询土地利用现状",
                "reason_summary": "存在多个相近业务表，请确认",
                "candidates": [
                    {"table_id": "table_land_status", "table_name": "土地利用现状表", "confidence": 0.82},
                ],
                "recommended_table_ids": ["table_land_status"],
                "selected_table_ids": ["table_land_status"],
                "rejected_table_ids": ["table_land_archive"],
                "allow_multi_select": False,
                "multi_table_mode": "union",
                "manual_table_override": True,
            },
        },
    )

    result = await service.get_session(query_id)

    assert result is not None
    confirmation_view = result["confirmation_view"]
    assert confirmation_view["table_resolution"]["reason_summary"] == "存在多个相近业务表，请确认"
    assert confirmation_view["table_resolution"]["selected_table_ids"] == ["table_land_status"]
    assert confirmation_view["table_resolution"]["rejected_table_ids"] == ["table_land_archive"]
    assert confirmation_view["table_resolution"]["multi_table_mode"] == "union"
    assert confirmation_view["table_resolution"]["manual_table_override"] is True


@pytest.mark.asyncio
async def test_get_session_derives_provisional_draft_for_table_resolution():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="table_resolution",
        state_json={
            "question_text": "查询土地利用现状",
            "pending_actions": ["confirm", "change_table"],
            "recommended_table_ids": ["table_land_status"],
            "table_resolution_state": {
                "question": "查询土地利用现状",
                "reason_summary": "存在多个相近业务表，请确认",
                "candidates": [
                    {"table_id": "table_land_status", "table_name": "土地利用现状表", "confidence": 0.82},
                    {"table_id": "table_land_plan", "table_name": "土地利用规划表", "confidence": 0.71},
                ],
                "recommended_table_ids": ["table_land_status"],
                "selected_table_ids": ["table_land_status"],
                "allow_multi_select": False,
            },
        },
    )

    result = await service.get_session(query_id)

    assert result is not None
    draft = result["confirmation_view"]["draft"]
    assert draft["status"] == "provisional"
    assert "土地利用现状表" in draft["natural_language"]
    assert "查询土地利用现状" in draft["natural_language"]
    assert draft["draft_json"] is None
    assert draft["warnings"] == []
    assert draft["confirmed"] is False
    assert draft["confirmation_required"] is False


@pytest.mark.asyncio
async def test_update_session_derives_confirmation_view_for_draft_confirmation():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="running",
        current_node="draft_generation",
        state_json={
            "question_text": "查一下武汉土地成交均价",
            "selected_table_ids": ["table_land_deal"],
        },
    )

    updated = await service.update_session(
        query_id,
        status="awaiting_user_action",
        current_node="draft_confirmation",
        state_updates={
            "pending_actions": ["confirm", "revise", "change_table", "exit_current"],
            "draft_confirmation_required": True,
            "draft_confirmation_card": {
                "natural_language": "按年份统计武汉土地成交均价",
                "warnings": ["统计口径依赖成交公告时间"],
                "ir": {"query_type": "aggregation"},
            },
        },
    )

    assert updated is not None
    confirmation_view = updated["confirmation_view"]
    assert confirmation_view["draft"]["status"] == "awaiting_confirmation"
    assert confirmation_view["draft"]["natural_language"] == "按年份统计武汉土地成交均价"
    assert confirmation_view["draft"]["warnings"] == ["统计口径依赖成交公告时间"]
    assert confirmation_view["pending_actions"] == ["confirm_draft", "revise", "change_table", "cancel_query"]
    assert confirmation_view["dependency_meta"]["selected_table_ids"] == ["table_land_deal"]


@pytest.mark.asyncio
async def test_update_session_derives_confirmation_view_from_unified_draft_state():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="running",
        current_node="draft_generation",
        state_json={
            "question_text": "查一下武汉土地成交均价",
            "selected_table_ids": ["table_land_deal"],
        },
    )

    updated = await service.update_session(
        query_id,
        status="awaiting_user_action",
        current_node="draft_confirmation",
        state_updates={
            "pending_actions": ["confirm", "revise", "change_table", "exit_current"],
            "draft_state": {
                "status": "awaiting_confirmation",
                "natural_language": "按年份统计武汉土地成交均价",
                "draft_json": {"query_type": "aggregation"},
                "warnings": ["统计口径依赖成交公告时间"],
                "suggestions": [{"label": "改成按区域统计"}],
                "confirmed": False,
                "confirmation_required": False,
                "table_dependent": True,
                "invalidate_on_table_change": True,
            },
        },
    )

    assert updated is not None
    confirmation_view = updated["confirmation_view"]
    assert confirmation_view["draft"]["status"] == "awaiting_confirmation"
    assert confirmation_view["draft"]["natural_language"] == "按年份统计武汉土地成交均价"
    assert confirmation_view["draft"]["draft_json"] == {"query_type": "aggregation"}
    assert confirmation_view["draft"]["warnings"] == ["统计口径依赖成交公告时间"]
    assert confirmation_view["draft"]["suggestions"] == [{"label": "改成按区域统计"}]


@pytest.mark.asyncio
async def test_get_session_derives_confirmation_view_from_unified_execution_guard_state():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="awaiting_user_action",
        current_node="execution_guard",
        state_json={
            "question_text": "查询土地成交总价",
            "pending_actions": ["execution_decision", "revise", "exit_current"],
            "execution_guard_state": {
                "status": "awaiting_confirmation",
                "natural_language": "此查询将扫描约 100 万行数据",
                "warnings": ["查询范围较大，耗时可能较长"],
                "estimated_cost": {"rows": 1000000, "cost": 42.5},
                "ir": {"query_type": "aggregation"},
            },
            "ir_snapshot": {"query_type": "aggregation"},
        },
    )

    result = await service.get_session(query_id)

    assert result is not None
    confirmation_view = result["confirmation_view"]
    assert confirmation_view["execution_guard"]["status"] == "awaiting_confirmation"
    assert confirmation_view["execution_guard"]["natural_language"] == "此查询将扫描约 100 万行数据"
    assert confirmation_view["execution_guard"]["warnings"] == ["查询范围较大，耗时可能较长"]
    assert confirmation_view["execution_guard"]["estimated_cost"] == {"rows": 1000000, "cost": 42.5}
    assert confirmation_view["execution_guard"]["ir"] == {"query_type": "aggregation"}


@pytest.mark.asyncio
async def test_get_session_keeps_analysis_context_in_confirmation_view_dependency_meta():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)
    query_id = uuid4()

    await service.upsert_session(
        query_id=query_id,
        user_id=None,
        status="running",
        current_node="question_intake",
        state_json={
            "question_text": "那按区域展开看一下呢？",
            "analysis_context": {
                "context_mode": "followup",
                "inherit_from_query_id": "q-last",
            },
        },
    )

    result = await service.get_session(query_id)

    assert result is not None
    assert result["confirmation_view"]["dependency_meta"]["analysis_context"]["context_mode"] == "followup"
    assert result["confirmation_view"]["dependency_meta"]["analysis_context"]["inherit_from_query_id"] == "q-last"


@pytest.mark.asyncio
async def test_get_session_returns_none_for_unknown_query():
    db = FakeQuerySessionDB()
    service = QuerySessionService(db)

    result = await service.get_session(uuid4())

    assert result is None
