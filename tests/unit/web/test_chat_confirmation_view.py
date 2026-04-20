from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]


def test_chat_view_prefers_confirmation_view_for_pending_session_rendering():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "const pendingConfirmationView = computed(() =>" in content
    assert "confirmation_view: payload.confirmation_view || payload.session?.confirmation_view || null" in content
    assert "buildPendingTableSelectionCard(snapshot, options.fallbackTableSelection)" in content
    assert "buildPendingConfirmCard(snapshot, 'execution_guard', options.fallbackConfirmation)" in content
    assert "snapshot.confirmation_view?.context?.question_text" in content
