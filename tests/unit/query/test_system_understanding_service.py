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


def _build_ratio_trend_ir_display():
    return {
        "query_type": "aggregation",
        "metrics": ["出让面积（平方米）"],
        "dimensions": ["成交年份"],
        "ratio_metrics": [{"alias": "工业用地面积占比"}],
        "conditional_metrics": [{"alias": "工业用地面积(平方米)"}],
        "filters": [
            {"field": "成交年份", "op": ">=", "value": 2016},
            {"field": "成交年份", "op": "<=", "value": 2025},
            {"field": "出让方式", "op": "LIKE", "value": "%招拍挂%"},
        ],
        "comparison_type": "yoy",
        "show_growth_rate": True,
    }


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
        "基于公开成交统计出让面积，并按成交年份展开",
        "计算工业用地面积占比，并按同比展示增长率",
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
    assert all("当前数据表：" not in text for text in texts)
    assert all("我要基于【" not in text for text in texts)


def test_build_system_understanding_filters_structured_template_bullets_for_ratio_trend_query():
    items = build_system_understanding(
        ir_display=_build_ratio_trend_ir_display(),
        selected_table_names=["公开成交"],
        model_understanding=[
            {"text": "我要基于【公开成交】进行查询"},
            {"text": "统计【出让面积（平方米）】"},
            {"text": "按【成交年份】展开"},
            {"text": "计算占比指标【工业用地面积占比】"},
            {"text": "补充条件指标【工业用地面积(平方米)】"},
            {"text": "筛选条件为 【成交年份】>=2016、【成交年份】<=2025、【出让方式】LIKE%招拍挂%"},
            {"text": "分析方式为【同比】，并显示增长率"},
            {"text": "当前数据表：公开成交"},
            {"text": "统计指标：出让面积（平方米）"},
            {"text": "占比指标：工业用地面积占比"},
            {"text": "条件指标：工业用地面积(平方米)"},
            {"text": "分析维度：成交年份"},
            {"text": "分析方式：同比 + 增长率"},
        ],
        default_filter_items=[],
        permission_scope_items=[],
    )

    texts = [item.text for item in items]

    assert any("基于公开成交查询" in text for text in texts)
    assert any("按成交年份展开，统计出让面积（平方米）" in text for text in texts)
    assert any("工业用地面积占比" in text for text in texts)
    assert any("仅包含" in text and "出让方式包含“招拍挂”" in text for text in texts)
    assert any("进行同比分析并显示增长率" in text for text in texts)
    assert all("当前数据表：" not in text for text in texts)
    assert all("统计指标：" not in text for text in texts)
    assert all("我要基于【" not in text for text in texts)


def test_build_system_understanding_keeps_natural_model_bullets_and_drops_structured_tail():
    items = build_system_understanding(
        ir_display=_build_ratio_trend_ir_display(),
        selected_table_names=["公开成交"],
        model_understanding=[
            {"text": "我会先在公开成交里统计近10年的出让面积，并按成交年份展开。"},
            {"text": "结果会计算工业用地面积占比，并按同比展示增长率。"},
            {"text": "当前数据表：公开成交"},
            {"text": "统计指标：出让面积（平方米）"},
        ],
        default_filter_items=[],
        permission_scope_items=[],
    )

    texts = [item.text for item in items]

    assert texts == [
        "在公开成交里统计近10年的出让面积，并按成交年份展开。",
        "计算工业用地面积占比，并按同比展示增长率。",
    ]


def test_build_system_understanding_rewrites_first_person_model_bullets_to_direct_style():
    items = build_system_understanding(
        ir_display={
            "metrics": ["成交宗数"],
            "dimensions": ["行政区"],
        },
        selected_table_names=["公开成交"],
        model_understanding=[
            {"text": "我会查询2025年武汉市所有行政区的土地成交数据"},
            {"text": "结果会按行政区进行分组统计"},
            {"text": "筛选范围会限制在2025年，并且只看出让方式包含“招拍挂”的记录。"},
        ],
        default_filter_items=[],
        permission_scope_items=[],
    )

    assert [item.text for item in items] == [
        "查询2025年武汉市所有行政区的土地成交数据",
        "按行政区进行分组统计",
        "范围限定在2025年，只包含出让方式包含“招拍挂”的记录。",
    ]
