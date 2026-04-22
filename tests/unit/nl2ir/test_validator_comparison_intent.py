from types import SimpleNamespace

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


def test_validator_clears_default_comparison_for_uuid_year_field_by_display_name():
    semantic_model = SimpleNamespace(
        fields={
            "de675dd5-14c8-40fa-ace7-801546b4762f": SimpleNamespace(
                display_name="成交年份",
                field_name="ssnf",
                physical_column_name="ssnf",
                description=None,
                synonyms=[],
                field_category="dimension",
                data_type="nvarchar",
                timestamp_props=None,
                dimension_props=SimpleNamespace(dimension_type="categorical"),
            )
        },
        dimensions={},
        field_enums={},
    )
    validator = IRValidator(semantic_model)
    ir = IntermediateRepresentation(
        query_type="aggregation",
        metrics=["land_area"],
        dimensions=["de675dd5-14c8-40fa-ace7-801546b4762f"],
        filters=[
            {"field": "de675dd5-14c8-40fa-ace7-801546b4762f", "op": ">=", "value": 2016},
        ],
        comparison_type="yoy",
        show_growth_rate=True,
        original_question="近10年工业用地面积占比变化情况",
    )

    validated = validator.validate_and_fix(ir)

    assert validated.comparison_type is None
    assert validated.show_growth_rate is False
    assert validated.show_previous_period_value is False
