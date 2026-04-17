from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SQL_FILE = ROOT_DIR / "docker" / "init-scripts" / "init_database_complete.sql"
REVISION_FILE = ROOT_DIR / "migrations" / "versions" / "20260417_0002_query_sessions.py"


def load_revision_module():
    spec = spec_from_file_location("query_sessions_revision", REVISION_FILE)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_init_sql_contains_query_sessions_table_and_indexes():
    content = SQL_FILE.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS query_sessions" in content
    assert "CREATE INDEX IF NOT EXISTS idx_query_sessions_user_status" in content
    assert "CREATE INDEX IF NOT EXISTS idx_query_sessions_conversation" in content
    assert "CREATE TRIGGER update_query_sessions_updated_at BEFORE UPDATE ON query_sessions" in content


def test_query_sessions_revision_metadata_is_consistent():
    module = load_revision_module()

    assert module.revision == "20260417_0002"
    assert module.down_revision == "20260414_0001"
