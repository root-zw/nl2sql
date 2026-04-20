"""add learning_events table"""

from alembic import op


revision = "20260420_0004"
down_revision = "20260417_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS learning_events (
            event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            event_key VARCHAR(128) NOT NULL,
            query_id UUID REFERENCES query_sessions(query_id) ON DELETE SET NULL,
            conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
            user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
            event_type VARCHAR(64) NOT NULL,
            event_version INT NOT NULL DEFAULT 1,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            source_component VARCHAR(128) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_learning_events_event_key
            ON learning_events(event_key)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_learning_events_query_created
            ON learning_events(query_id, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_learning_events_type_created
            ON learning_events(event_type, created_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS learning_events")
