from server.compiler.sql_post_processor import SQLPostProcessor


def test_mariadb_rejects_full_outer_join():
    processor = SQLPostProcessor(dialect="mariadb")

    result = processor.process(
        "SELECT * FROM sales_current FULL OUTER JOIN sales_base ON sales_current.id = sales_base.id"
    )

    assert result.is_valid is False
    assert any("FULL OUTER JOIN" in error for error in result.errors)


def test_mysql_rollup_is_rewritten_to_with_rollup():
    processor = SQLPostProcessor(dialect="mysql")

    result = processor.process(
        "SELECT region, SUM(amount) AS total_amount FROM sales GROUP BY ROLLUP (region)"
    )

    assert result.is_valid is True
    assert "WITH ROLLUP" in result.sql
