from __future__ import annotations

from types import SimpleNamespace

from server.models.ir import IntermediateRepresentation
from server.services.system_understanding_service import (
    build_permission_scope_items,
    build_system_understanding,
    preview_default_filter_items,
    summarize_understanding_items,
)


def _build_semantic_model():
    return SimpleNamespace(
        metrics={
            "land_area": SimpleNamespace(
                default_filters=["approve_state = '已审核'"],
            ),
        },
        fields={
            "approve_state": SimpleNamespace(display_name="审核状态"),
            "record_status": SimpleNamespace(display_name="数据状态"),
            "city": SimpleNamespace(display_name="城市"),
            "trade_type": SimpleNamespace(display_name="出让方式"),
            "district": SimpleNamespace(display_name="区县"),
        },
        rules=SimpleNamespace(
            data_quality_rules={
                "enabled": True,
                "default_record_filter": {
                    "field_name": "record_status",
                    "operator": "=",
                    "value": "有效",
                    "apply_to": {"detail_queries": True},
                    "apply_to_tables": ["public_deal"],
                },
            }
        ),
        sources={
            "public_deal": SimpleNamespace(
                detail_view=SimpleNamespace(
                    default_filters=["city = '杭州'"],
                )
            )
        },
    )


def test_preview_default_filter_items_includes_actual_dynamic_scope_sources():
    semantic_model = _build_semantic_model()
    ir = IntermediateRepresentation(
        query_type="detail",
        metrics=["land_area"],
        primary_table_id="public_deal",
        selected_table_ids=["public_deal"],
        original_question="查一下公开成交明细",
    )

    items = preview_default_filter_items(
        ir,
        semantic_model,
        global_rules=[
            {
                "rule_type": "default_filter",
                "rule_definition": {
                    "table_id": "public_deal",
                    "filter_field": "trade_type",
                    "filter_operator": "=",
                    "filter_value": "招拍挂",
                },
            }
        ],
        selected_table_ids=["public_deal"],
    )

    assert items == [
        "系统默认会按指标口径只统计审核状态为“已审核”的记录。",
        "系统默认只统计数据状态为“有效”的记录。",
        "系统默认只统计城市为“杭州”的记录。",
        "系统默认只统计出让方式为“招拍挂”的记录。",
    ]


def test_build_permission_scope_items_renders_concrete_allowed_values():
    semantic_model = _build_semantic_model()

    items = build_permission_scope_items(
        {
            "applied": True,
            "restricted_fields": {
                "city": ["杭州", "宁波"],
                "district": ["滨江区", "西湖区", "余杭区", "上城区"],
            },
        },
        semantic_model=semantic_model,
    )

    assert items == [
        "当前结果仅包含你有权限访问的城市：杭州、宁波。",
        "当前结果仅包含你有权限访问的区县：滨江区、西湖区、余杭区 等4个值。",
    ]


def test_build_system_understanding_merges_model_summary_with_dynamic_scope_items():
    items = build_system_understanding(
        ir_display={
            "metrics": ["出让面积（平方米）"],
            "dimensions": ["成交年份"],
        },
        selected_table_names=["公开成交"],
        model_understanding=[
            {
                "text": "我会基于公开成交统计出让面积，并按成交年份展开",
                "anchors": ["table", "metric", "dimension"],
            },
            {
                "text": "我会继续计算工业用地面积占比，并按同比展示增长率",
                "anchors": ["ratio", "comparison"],
            },
            {
                "text": "我会继续计算工业用地面积占比，并按同比展示增长率",
                "anchors": ["ratio", "comparison"],
            },
        ],
        revision_request={"text": "只看招拍挂"},
        default_filter_items=["系统默认只统计审核状态为“已审核”的记录。"],
        permission_scope_items=["当前结果仅包含你有权限访问的城市：杭州、宁波。"],
    )

    assert [item.text for item in items] == [
        "已吸收本轮修改：只看招拍挂。",
        "我会基于公开成交统计出让面积，并按成交年份展开",
        "我会继续计算工业用地面积占比，并按同比展示增长率",
        "系统默认只统计审核状态为“已审核”的记录。",
        "当前结果仅包含你有权限访问的城市：杭州、宁波。",
    ]
    assert items[0].source == "system"
    assert items[0].material is False
    assert items[1].source == "model"
    assert items[3].anchors == ["default_filters"]
    assert items[4].anchors == ["permission_scope"]
    assert summarize_understanding_items(items).endswith("当前结果仅包含你有权限访问的城市：杭州、宁波。")


def test_build_system_understanding_omits_scope_lines_when_none_exist():
    items = build_system_understanding(
        ir_display={
            "metrics": ["出让面积（平方米）"],
            "dimensions": ["成交年份"],
        },
        selected_table_names=["公开成交"],
        model_understanding=[],
        default_filter_items=[],
        permission_scope_items=[],
    )

    assert items
    texts = [item.text for item in items]
    assert all("系统默认" not in text for text in texts)
    assert all("权限访问" not in text for text in texts)

