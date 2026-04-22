from datetime import datetime
from types import SimpleNamespace

from server.compiler import ast_builder as ast_builder_module
from server.compiler.ast_builder import ASTBuilder
from server.models.ir import TimeRange


def _build_model(*, time_field=None):
    source = SimpleNamespace(table_name="sales", time_field=time_field)
    return SimpleNamespace(
        sources={"sales_table": source},
        fields={
            "dim_year": SimpleNamespace(
                datasource_id="sales_table",
                display_name="成交年份",
                field_name="deal_year",
                data_type="int",
            )
        },
        measures={},
        dimensions={},
        datasources={},
        field_enums={},
    )


def test_relative_year_without_time_field_does_not_fallback_to_year_dimension():
    builder = ASTBuilder(_build_model(), dialect="tsql", global_rules=[])
    ir = SimpleNamespace(
        filters=[],
        time=TimeRange(type="relative", last_n=10, unit="year"),
        dimensions=["dim_year"],
        join_strategy="matched",
    )

    conditions = builder._build_where_clause(ir, "sales_table")

    assert conditions == []


def test_relative_year_uses_time_field_when_source_has_datetime_column(monkeypatch):
    builder = ASTBuilder(_build_model(time_field="deal_date"), dialect="tsql", global_rules=[])
    ir = SimpleNamespace(
        filters=[],
        time=TimeRange(type="relative", last_n=2, unit="year"),
        dimensions=["dim_year"],
        join_strategy="matched",
    )

    monkeypatch.setattr(
        ast_builder_module,
        "now_with_tz",
        lambda: datetime(2026, 4, 22, 10, 0, 0),
    )

    conditions = builder._build_where_clause(ir, "sales_table")

    assert len(conditions) == 1
    sql = conditions[0].sql(dialect=builder.dialect)
    assert "deal_date" in sql
    assert "2024-04-22" in sql
