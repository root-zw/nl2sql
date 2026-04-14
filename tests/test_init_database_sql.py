from pathlib import Path


SQL_FILE = Path(__file__).resolve().parents[1] / "docker" / "init-scripts" / "init_database_complete.sql"


def test_init_sql_contains_runtime_tables_used_by_current_code():
    content = SQL_FILE.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS model_providers" in content
    assert "CREATE TABLE IF NOT EXISTS prompt_templates" in content
    assert "CREATE TABLE IF NOT EXISTS conversations" in content
    assert "CREATE TABLE IF NOT EXISTS conversation_messages" in content
    assert "CREATE TABLE IF NOT EXISTS active_queries" in content


def test_init_sql_creates_update_trigger_after_trigger_function():
    content = SQL_FILE.read_text(encoding="utf-8")

    function_pos = content.index("CREATE OR REPLACE FUNCTION update_updated_at_column()")
    trigger_pos = content.index("CREATE TRIGGER update_few_shot_samples_updated_at")

    assert function_pos < trigger_pos


def test_init_sql_creates_sync_views_after_milvus_tables():
    content = SQL_FILE.read_text(encoding="utf-8")

    pending_table_pos = content.index("CREATE TABLE IF NOT EXISTS milvus_pending_changes")
    history_table_pos = content.index("CREATE TABLE IF NOT EXISTS milvus_sync_history")
    pending_view_pos = content.rindex("CREATE OR REPLACE VIEW v_pending_changes_stats")
    history_view_pos = content.rindex("CREATE OR REPLACE VIEW v_sync_history_stats")

    assert pending_table_pos < pending_view_pos
    assert history_table_pos < history_view_pos
