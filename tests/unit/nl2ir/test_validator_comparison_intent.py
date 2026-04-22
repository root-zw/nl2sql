from server.models.ir import IntermediateRepresentation
from server.nl2ir.validator import IRValidator


def test_validator_clears_default_comparison_for_multi_period_trend_question():
    validator = IRValidator()
    ir = IntermediateRepresentation(
        query_type="aggregation",
        metrics=["land_area"],
        dimensions=["成交年份"],
        filters=[
            {"field": "成交年份", "op": ">=", "value": 2016},
            {"field": "成交年份", "op": "<=", "value": 2025},
        ],
        comparison_type="yoy",
        show_growth_rate=True,
        original_question="近10年工业用地面积占比变化情况",
    )

    validated = validator.validate_and_fix(ir)

    assert validated.comparison_type is None
    assert validated.show_growth_rate is False
    assert validated.show_previous_period_value is False


def test_validator_keeps_explicit_comparison_keywords_for_multi_period_query():
    validator = IRValidator()
    ir = IntermediateRepresentation(
        query_type="aggregation",
        metrics=["land_area"],
        dimensions=["成交年份"],
        filters=[
            {"field": "成交年份", "op": ">=", "value": 2016},
            {"field": "成交年份", "op": "<=", "value": 2025},
        ],
        comparison_type="yoy",
        show_growth_rate=True,
        original_question="近10年工业用地面积占比同比增长率变化情况",
    )

    validated = validator.validate_and_fix(ir)

    assert validated.comparison_type == "yoy"
    assert validated.show_growth_rate is True
