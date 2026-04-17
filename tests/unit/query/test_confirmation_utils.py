from server.api.query.confirmation_utils import (
    build_draft_confirmation_summary,
    compose_question_with_revision,
    resolve_confirmation_mode,
)


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


def test_resolve_confirmation_mode_prefers_request_override():
    result = resolve_confirmation_mode("adaptive", "always_confirm", "always_confirm")

    assert result == "adaptive"


def test_resolve_confirmation_mode_falls_back_to_existing_state_and_default():
    assert resolve_confirmation_mode(None, "adaptive", "always_confirm") == "adaptive"
    assert resolve_confirmation_mode(None, None, "always_confirm") == "always_confirm"


def test_resolve_confirmation_mode_uses_safe_fallback_for_invalid_values():
    result = resolve_confirmation_mode("invalid", "broken", "weird")

    assert result == "always_confirm"
