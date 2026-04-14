from types import SimpleNamespace

from sqlglot import exp, select

from server.compiler.ast_builder import ASTBuilder


def _build_minimal_model():
    return SimpleNamespace(
        dimensions={
            "dim_region": SimpleNamespace(display_name="地区"),
        },
        fields={},
        field_enums={},
    )


def test_total_row_uses_union_all_strategy_for_mysql_family():
    builder = ASTBuilder(
        _build_minimal_model(),
        dialect="mysql",
        global_rules=[],
        db_type="mariadb",
    )
    ir = SimpleNamespace(dimensions=["dim_region"])

    detail_query = (
        select(
            exp.column("region").as_("地区"),
            exp.func("SUM", exp.column("amount")).as_("金额"),
        )
        .from_("sales")
        .group_by(exp.column("region"))
    )

    sql = builder._add_total_row(detail_query, ir, "sales", [], []).sql(dialect="mysql")

    assert "UNION ALL" in sql
    assert "合计" in sql
