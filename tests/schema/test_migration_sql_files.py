from pathlib import Path

from migrations.sql_files import load_sql_script, split_sql_statements


ROOT_DIR = Path(__file__).resolve().parents[2]


def test_split_sql_statements_handles_do_blocks_and_strings():
    sql = """
    DO $$
    BEGIN
        RAISE NOTICE 'hello;world';
    END $$;
    CREATE TABLE demo(id INT);
    """

    statements = split_sql_statements(sql)

    assert len(statements) == 2
    assert statements[0].startswith("DO $$")
    assert statements[1] == "CREATE TABLE demo(id INT)"


def test_split_sql_statements_ignores_semicolons_in_comments():
    sql = """
    -- semicolon ; in line comment
    CREATE TABLE first_table(id INT);
    /* block ; comment */
    CREATE TABLE second_table(id INT);
    """

    statements = split_sql_statements(sql)

    assert len(statements) == 2
    assert "CREATE TABLE first_table" in statements[0]
    assert "CREATE TABLE second_table" in statements[1]


def test_split_init_sql_returns_large_non_empty_statement_set():
    sql = load_sql_script("docker/init-scripts/init_database_complete.sql")

    statements = split_sql_statements(sql)

    assert len(statements) > 100
    assert statements[0].startswith("--")
    assert "DO $$" in statements[-1]
    assert statements[-1].endswith("END $$")
