from server.api.query.confirmation_utils import (
    build_draft_confirmation_summary,
    compose_question_with_revision,
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
