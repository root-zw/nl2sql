"""add governance_candidates table"""

from alembic import op


revision = "20260420_0005"
down_revision = "20260420_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS governance_candidates (
            candidate_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            candidate_type VARCHAR(64) NOT NULL,
            target_object_type VARCHAR(64) NOT NULL,
            target_object_id VARCHAR(128) NOT NULL,
            scope_type VARCHAR(64) NOT NULL,
            scope_id UUID,
            suggested_change_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            evidence_summary TEXT,
            evidence_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            support_count INT NOT NULL DEFAULT 1,
            confidence_score NUMERIC(5,4),
            status VARCHAR(32) NOT NULL DEFAULT 'observed',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP WITH TIME ZONE,
            reviewed_by UUID REFERENCES users(user_id) ON DELETE SET NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_governance_candidates_status_created
            ON governance_candidates(status, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_governance_candidates_target
            ON governance_candidates(target_object_type, target_object_id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_governance_candidates_scope
            ON governance_candidates(scope_type, scope_id)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS governance_candidates")
