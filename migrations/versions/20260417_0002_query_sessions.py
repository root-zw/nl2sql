"""add query_sessions table"""

from alembic import op


revision = "20260417_0002"
down_revision = "20260414_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS query_sessions (
            query_id UUID PRIMARY KEY,
            conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
            message_id UUID REFERENCES conversation_messages(message_id) ON DELETE SET NULL,
            user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
            status VARCHAR(32) NOT NULL,
            current_node VARCHAR(64) NOT NULL,
            state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            last_error TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_query_sessions_user_status
            ON query_sessions(user_id, status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_query_sessions_conversation
            ON query_sessions(conversation_id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_query_sessions_updated
            ON query_sessions(updated_at DESC)
        """
    )
    op.execute("DROP TRIGGER IF EXISTS update_query_sessions_updated_at ON query_sessions")
    op.execute(
        """
        CREATE TRIGGER update_query_sessions_updated_at BEFORE UPDATE ON query_sessions
         FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS update_query_sessions_updated_at ON query_sessions")
    op.execute("DROP TABLE IF EXISTS query_sessions")
