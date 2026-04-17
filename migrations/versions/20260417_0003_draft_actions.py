"""add draft_actions table"""

from alembic import op


revision = "20260417_0003"
down_revision = "20260417_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_actions (
            action_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            query_id UUID NOT NULL REFERENCES query_sessions(query_id) ON DELETE CASCADE,
            draft_id UUID,
            draft_version INT,
            action_type VARCHAR(64) NOT NULL,
            actor_type VARCHAR(32) NOT NULL,
            actor_id VARCHAR(128) NOT NULL,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            idempotency_key VARCHAR(128) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_draft_actions_query_idempotency
            ON draft_actions(query_id, idempotency_key)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_draft_actions_query_created
            ON draft_actions(query_id, created_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS draft_actions")
