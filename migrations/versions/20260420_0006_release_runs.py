"""add release_runs table"""

from alembic import op


revision = "20260420_0006"
down_revision = "20260420_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS release_runs (
            release_run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            release_type VARCHAR(64) NOT NULL,
            source_type VARCHAR(64) NOT NULL,
            source_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            policy_snapshot_id UUID,
            status VARCHAR(32) NOT NULL,
            plan_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            triggered_by UUID REFERENCES users(user_id) ON DELETE SET NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP WITH TIME ZONE,
            completed_at TIMESTAMP WITH TIME ZONE
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_release_runs_status_created
            ON release_runs(status, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_release_runs_source_created
            ON release_runs(source_type, created_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS release_runs")
