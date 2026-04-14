from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from migrations.sql_files import iter_sql_file


ROOT_DIR = Path(__file__).resolve().parents[2]
REVISION_FILE = ROOT_DIR / "migrations" / "versions" / "20260414_0001_metadata_baseline.py"


def load_revision_module():
    spec = spec_from_file_location("baseline_revision", REVISION_FILE)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_baseline_revision_metadata_is_consistent():
    module = load_revision_module()

    assert module.revision == "20260414_0001"
    assert module.down_revision is None
    assert module.BASELINE_SQL == "docker/init-scripts/init_database_complete.sql"


def test_baseline_sql_contains_expected_bootstrap_objects():
    statements = iter_sql_file("docker/init-scripts/init_database_complete.sql")

    assert any('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"' in stmt for stmt in statements)
    assert any("CREATE TABLE users" in stmt for stmt in statements)
    assert any("CREATE TABLE organizations" in stmt for stmt in statements)
    assert any("COMMENT ON VIEW v_organization_stats" in stmt for stmt in statements)
