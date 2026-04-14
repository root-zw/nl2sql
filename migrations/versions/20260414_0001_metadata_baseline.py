"""metadata baseline from init sql"""

from alembic import op

from migrations.sql_files import iter_sql_file


revision = "20260414_0001"
down_revision = None
branch_labels = None
depends_on = None

BASELINE_SQL = "docker/init-scripts/init_database_complete.sql"


def upgrade() -> None:
    for statement in iter_sql_file(BASELINE_SQL):
        op.execute(statement)


def downgrade() -> None:
    raise NotImplementedError("Baseline migration does not support automatic downgrade.")
