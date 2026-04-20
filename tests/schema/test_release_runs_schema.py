from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
SQL_FILE = ROOT_DIR / "docker" / "init-scripts" / "init_database_complete.sql"
REVISION_FILE = ROOT_DIR / "migrations" / "versions" / "20260420_0006_release_runs.py"


def load_revision_module():
    spec = spec_from_file_location("release_runs_revision", REVISION_FILE)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_init_sql_contains_release_runs_table_and_indexes():
    content = SQL_FILE.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS release_runs" in content
    assert "CREATE INDEX IF NOT EXISTS idx_release_runs_status_created" in content
    assert "CREATE INDEX IF NOT EXISTS idx_release_runs_source_created" in content


def test_release_runs_revision_metadata_is_consistent():
    module = load_revision_module()

    assert module.revision == "20260420_0006"
    assert module.down_revision == "20260420_0005"
