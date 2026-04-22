from types import SimpleNamespace

from sqlglot import exp, select

from server.compiler.ast_builder import ASTBuilder


def _build_model():
    return SimpleNamespace(
        metrics={},
        measures={
            "land_area": SimpleNamespace(display_name="出让面积"),
        },
        dimensions={
            "deal_year": SimpleNamespace(display_name="成交年份"),
        },
        fields={},
        sources={},
        datasources={},
        field_enums={},
    )


def _build_inner_query():
    return (
        select(
            exp.column("成交年份").as_("成交年份"),
            exp.column("出让面积").as_("出让面积"),
            exp.column("工业用地面积占比").as_("工业用地面积占比"),
        )
        .from_("sales")
    )


def test_vertical_comparison_defaults_to_compact_columns_for_multi_period_query():
    builder = ASTBuilder(_build_model(), dialect="tsql", global_rules=[])
    ir = SimpleNamespace(
        metrics=["land_area"],
        dimensions=["deal_year"],
        conditional_metrics=[],
        ratio_metrics=[SimpleNamespace(alias="工业用地面积占比")],
        comparison_type="yoy",
        comparison_periods=1,
        show_growth_rate=True,
        show_previous_period_value=False,
        limit=None,
    )

    query = builder._build_vertical_comparison_query(
        _build_inner_query(),
        ir,
        "成交年份",
        [],
        1,
    )

    sql = query.sql(dialect=builder.dialect)

    assert "上年出让面积" not in sql
    assert "上年工业用地面积占比" not in sql
    assert "出让面积_同比增长率" in sql
    assert "工业用地面积占比_同比增长率" in sql


def test_vertical_comparison_can_include_previous_period_columns_when_requested():
    builder = ASTBuilder(_build_model(), dialect="tsql", global_rules=[])
    ir = SimpleNamespace(
        metrics=["land_area"],
        dimensions=["deal_year"],
        conditional_metrics=[],
        ratio_metrics=[SimpleNamespace(alias="工业用地面积占比")],
        comparison_type="yoy",
        comparison_periods=1,
        show_growth_rate=True,
        show_previous_period_value=True,
        limit=None,
    )

    query = builder._build_vertical_comparison_query(
        _build_inner_query(),
        ir,
        "成交年份",
        [],
        1,
    )

    sql = query.sql(dialect=builder.dialect)

    assert "上年出让面积" in sql
    assert "上年工业用地面积占比" in sql
