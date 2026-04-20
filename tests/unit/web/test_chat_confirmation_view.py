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
    assert "const pendingSessionDomainHint = computed(() =>" in content
    assert "const pendingSessionKnownConstraints = computed(() =>" in content
    assert "v-if=\"pendingSessionDomainHint || pendingSessionKnownConstraints.length\"" in content
    assert "const pendingTableResolutionDraftPreview = computed(() =>" in content
    assert "v-if=\"pendingTableResolutionDraftPreview\"" in content
    assert "这是基于当前候选表生成的暂定理解，确认选表后系统会继续重算。" in content
    assert "const hasLegacyPendingConfirmFallback = computed(() =>" in content
    assert "const hasLegacyPendingTableSelectionFallback = computed(() =>" in content
    assert "v-if=\"hasLegacyPendingConfirmFallback\"" in content
    assert "v-if=\"hasLegacyPendingTableSelectionFallback\"" in content


def test_chat_view_uses_resume_directive_to_continue_pending_query():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "async function continueQueryFromResumeDirective" in content
    assert "if (result?.resume_directive?.should_resume)" in content
    assert "await continueQueryFromResumeDirective(result.resume_directive)" in content


def test_chat_view_starts_fresh_query_when_pending_reply_becomes_new_query():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "async function startFreshQueryFromPendingReply" in content
    assert "if (result?.resolution === 'resolved_to_new_query')" in content
    assert "await startFreshQueryFromPendingReply(nextQueryText)" in content


def test_chat_view_resolves_followup_context_before_query_execution():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "async function resolveFollowupContextResolution" in content
    assert "conversationAPI.resolveFollowupContext" in content
    assert "if (followupResolutionResult?.resolution === 'need_clarification')" in content
    assert "analysis_context: options.analysisContext || null" in content
    assert "followup_resolution: options.followupResolution || null" in content
