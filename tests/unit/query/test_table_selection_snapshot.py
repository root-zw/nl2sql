from __future__ import annotations

import pytest

from server.api.query.table_selection import llm_select_table
from server.models.api import TableCandidate
from server.nl2ir.llm_table_selector import TableMeta, TableSelectionResult


class DummyStep:
    def set_input(self, *_args, **_kwargs):
        return None

    def add_metadata(self, *_args, **_kwargs):
        return None

    def set_output(self, *_args, **_kwargs):
        return None


class DummyTracer:
    def start_step(self, *_args, **_kwargs):
        return DummyStep()

    def end_step(self):
        return None


@pytest.mark.asyncio
async def test_llm_select_table_success_includes_candidate_snapshot(monkeypatch):
    async def fake_get_metadata_pool():
        return object()

    async def fake_load_all_tables_meta(**_kwargs):
        return [
            TableMeta(
                table_id="table_land_deal",
                display_name="土地成交表",
                description="土地成交明细",
                domain_name="土地",
                tags=["成交"],
                connection_id="conn-1",
            ),
            TableMeta(
                table_id="table_land_plan",
                display_name="土地规划表",
                description="土地规划信息",
                domain_name="土地",
                tags=["规划"],
                connection_id="conn-1",
            ),
        ]

    class DummySelector:
        def __init__(self, *_args, **_kwargs):
            self.last_system_prompt = None
            self.last_user_prompt = None
            self.last_result_json = None

        async def select_tables(self, **_kwargs):
            return TableSelectionResult(
                candidates=[
                    TableCandidate(
                        table_id="table_land_deal",
                        table_name="土地成交表",
                        description="土地成交明细",
                        confidence=0.92,
                        reason="问题关注成交信息",
                    ),
                    TableCandidate(
                        table_id="table_land_plan",
                        table_name="土地规划表",
                        description="土地规划信息",
                        confidence=0.41,
                        reason="名称接近但语义次要",
                    ),
                ],
                primary_table_id="table_land_deal",
                selection_summary="优先使用成交表",
                needs_confirmation=False,
                action="execute",
                is_multi_table_query=False,
                multi_table_mode="single",
                recommended_table_ids=["table_land_deal"],
            )

    async def noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr("server.utils.db_pool.get_metadata_pool", fake_get_metadata_pool)
    monkeypatch.setattr("server.api.query.table_selection.stream_progress", noop)
    monkeypatch.setattr("server.api.query.table_selection.stream_thinking", noop)
    monkeypatch.setattr("server.dependencies.get_table_selection_llm_client", lambda: object())
    monkeypatch.setattr("server.nl2ir.llm_table_selector.load_all_tables_meta", fake_load_all_tables_meta)
    monkeypatch.setattr("server.nl2ir.llm_table_selector.LLMTableSelector", DummySelector)

    result = await llm_select_table(
        question="查询武汉土地成交情况",
        user_id="anonymous",
        user_role="viewer",
        connection_id="conn-1",
        domain_id=None,
        selected_table_id=None,
        tracer=DummyTracer(),
        stream=None,
        query_id="q1",
        timestamp="2026-04-17T00:00:00+08:00",
    )

    assert result["status"] == "success"
    assert result["selected_table_id"] == "table_land_deal"
    assert result["candidate_snapshot"]["recommended_table_ids"] == ["table_land_deal"]
    assert result["candidate_snapshot"]["candidates"][0]["table_id"] == "table_land_deal"
    assert "自动选表" in result["candidate_snapshot"]["confirmation_reason"]
