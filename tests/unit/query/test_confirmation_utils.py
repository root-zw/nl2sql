from server.api.query.confirmation_utils import (
    apply_analysis_context_to_ir,
    build_draft_confirmation_open_points,
    build_draft_confirmation_summary,
    build_safe_summary,
    compose_question_with_revision,
    resolve_confirmation_mode,
    should_pause_for_draft_confirmation,
)
from server.api.query.ir_utils import ir_to_display_dict
from server.models.ir import IntermediateRepresentation


def test_compose_question_with_revision_appends_revision_text():
    result = compose_question_with_revision(
        "查询武汉土地成交均价",
        {"text": "只看滨江地产"},
    )

    assert "查询武汉土地成交均价" in result
    assert "补充修改要求：只看滨江地产" in result


def test_build_draft_confirmation_summary_includes_core_ir_facts():
    summary = build_draft_confirmation_summary(
        {
            "metrics": ["成交总价"],
            "dimensions": ["城市"],
            "filters": [{"field": "开发商", "op": "=", "value": "滨江地产"}],
            "time": {"type": "relative", "last_n": 1, "unit": "year"},
            "order_by": [{"field": "成交总价", "desc": True}],
            "limit": 10,
        },
        selected_table_names=["土地成交表"],
        revision_request={"text": "只看住宅用地"},
    )

    assert "土地成交表" in summary
    assert "成交总价" in summary
    assert "开发商" in summary
    assert "最近1年" in summary
    assert "只看住宅用地" in summary


def test_build_draft_confirmation_summary_does_not_append_cta_suffix():
    summary = build_draft_confirmation_summary(
        {
            "metrics": ["成交总价"],
            "dimensions": ["城市"],
        },
        selected_table_names=["土地成交表"],
    )

    assert summary
    assert "请确认是否继续" not in summary
    assert summary.endswith("。")


def test_build_draft_confirmation_summary_includes_advanced_ir_semantics():
    summary = build_draft_confirmation_summary(
        {
            "query_type": "aggregation",
            "metrics": ["成交总价"],
            "dimensions": ["区域"],
            "ratio_metrics": [{"alias": "工业用地面积占比"}],
            "conditional_metrics": [{"alias": "住宅用地成交宗数"}],
            "calculated_fields": [{"alias": "溢价率"}],
            "comparison_type": "yoy",
            "comparison_periods": 1,
            "show_growth_rate": True,
            "cumulative_metrics": ["成交总价"],
            "moving_average_window": 3,
            "moving_average_metrics": ["成交均价"],
            "cross_partition_query": True,
            "cross_partition_mode": "compare",
        },
        selected_table_names=["土地成交表", "土地供应表"],
    )

    assert "工业用地面积占比" in summary
    assert "住宅用地成交宗数" in summary
    assert "溢价率" in summary
    assert "同比" in summary
    assert "增长率" in summary
    assert "累计值" in summary
    assert "移动平均" in summary
    assert "跨表对比" in summary


def test_ir_to_display_dict_hides_internal_metric_prefixes():
    ir = IntermediateRepresentation(
        query_type="aggregation",
        metrics=["derived:每亩单价", "__row_count__"],
        original_question="查一下土地成交情况",
    )

    display = ir_to_display_dict(ir, semantic_model=None)

    assert display["metrics"] == ["每亩单价", "记录数"]


def test_resolve_confirmation_mode_prefers_request_override():
    result = resolve_confirmation_mode("adaptive", "always_confirm", "always_confirm")

    assert result == "adaptive"


def test_resolve_confirmation_mode_falls_back_to_existing_state_and_default():
    assert resolve_confirmation_mode(None, "adaptive", "always_confirm") == "adaptive"
    assert resolve_confirmation_mode(None, None, "always_confirm") == "always_confirm"


def test_resolve_confirmation_mode_uses_safe_fallback_for_invalid_values():
    result = resolve_confirmation_mode("invalid", "broken", "weird")

    assert result == "always_confirm"


def test_build_draft_confirmation_open_points_includes_confidence_ambiguity_and_revision():
    result = build_draft_confirmation_open_points(
        confidence=0.62,
        confidence_threshold=0.7,
        ambiguities=["不确定“成交总价”是否要按行政区展开"],
        revision_request={"text": "改成只看住宅用地"},
    )

    assert any("62%" in item for item in result)
    assert any("行政区" in item for item in result)
    assert any("修改意见" in item for item in result)


def test_build_safe_summary_includes_result_context_table_and_ir_constraints():
    result = build_safe_summary(
        question_text="那按区域展开看一下呢？",
        analysis_context={"scope_summary": "上一结果按区域展示了武汉土地成交总价。"},
        selected_table_names=["土地成交表"],
        ir_display={
            "metrics": ["成交总价"],
            "dimensions": ["区域"],
            "time": {"type": "relative", "last_n": 1, "unit": "year"},
        },
        open_points=["需确认当前查询草稿是否符合预期"],
    )

    assert result["user_goal_summary"] == "那按区域展开看一下呢？"
    assert "当前数据表：土地成交表" in result["known_constraints"]
    assert any("承接上一结果" in item for item in result["known_constraints"])
    assert any("统计指标：成交总价" in item for item in result["known_constraints"])
    assert result["open_points"] == ["需确认当前查询草稿是否符合预期"]


def test_build_safe_summary_filters_uuid_table_names_and_preserves_metric_units():
    leaked_uuid = "ac9c3e49-8c62-471f-b954-0b397b4f614a"

    result = build_safe_summary(
        question_text="查一下每亩单价",
        selected_table_names=[leaked_uuid, "土地成交表"],
        ir_display={
            "metrics": ["每亩单价（元/亩）"],
        },
        known_constraints=[
            leaked_uuid,
            f"当前数据表：{leaked_uuid}、土地成交表",
        ],
        open_points=[leaked_uuid, "请确认统计口径"],
    )

    assert all(leaked_uuid not in item for item in result["known_constraints"])
    assert "当前数据表：土地成交表" in result["known_constraints"]
    assert "统计指标：每亩单价（元/亩）" in result["known_constraints"]
    assert result["open_points"] == ["请确认统计口径"]


def test_build_safe_summary_includes_advanced_ir_constraints():
    result = build_safe_summary(
        question_text="查一下工业用地面积占比和溢价率",
        selected_table_names=["土地成交表"],
        ir_display={
            "ratio_metrics": [{"alias": "工业用地面积占比"}],
            "conditional_metrics": [{"alias": "住宅用地成交宗数"}],
            "calculated_fields": [{"alias": "溢价率"}],
            "comparison_type": "yoy",
            "show_growth_rate": True,
            "moving_average_window": 6,
            "moving_average_metrics": ["成交均价"],
        },
        open_points=["请确认统计口径"],
    )

    assert "占比指标：工业用地面积占比" in result["known_constraints"]
    assert "条件指标：住宅用地成交宗数" in result["known_constraints"]
    assert "计算字段：溢价率" in result["known_constraints"]
    assert "分析方式：同比 + 增长率" in result["known_constraints"]
    assert "移动平均：成交均价（6期）" in result["known_constraints"]


def test_should_pause_for_draft_confirmation_supports_real_adaptive_rules():
    assert should_pause_for_draft_confirmation(
        confirmation_mode="adaptive",
        request_has_ir=False,
        existing_requires_draft_confirmation=False,
        existing_has_confirmed_draft=False,
        confidence=0.61,
        ambiguities=[],
        confidence_threshold=0.7,
    ) is True
    assert should_pause_for_draft_confirmation(
        confirmation_mode="adaptive",
        request_has_ir=False,
        existing_requires_draft_confirmation=False,
        existing_has_confirmed_draft=False,
        confidence=0.92,
        ambiguities=["指标口径存在歧义"],
        confidence_threshold=0.7,
    ) is True
    assert should_pause_for_draft_confirmation(
        confirmation_mode="adaptive",
        request_has_ir=False,
        existing_requires_draft_confirmation=False,
        existing_has_confirmed_draft=False,
        confidence=0.92,
        ambiguities=[],
        confidence_threshold=0.7,
    ) is False


def test_apply_analysis_context_to_ir_inherits_missing_structure_from_previous_result():
    ir = IntermediateRepresentation(
        query_type="aggregation",
        metrics=[],
        dimensions=[],
        original_question="那按区域展开看一下呢？",
    )

    updated = apply_analysis_context_to_ir(
        ir,
        {
            "carry_over_flags": {
                "table": True,
                "metrics": True,
                "dimensions": True,
            },
            "base_result_refs": [
                {
                    "table_ids": ["table_land_deal"],
                    "metric_ids": ["deal_amount"],
                    "dimension_ids": ["district"],
                }
            ],
        },
    )

    assert updated.metrics == ["deal_amount"]
    assert updated.dimensions == ["district"]
    assert updated.selected_table_ids == ["table_land_deal"]
    assert updated.primary_table_id == "table_land_deal"
