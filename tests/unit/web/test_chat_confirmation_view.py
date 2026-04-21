from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]


def test_chat_view_prefers_confirmation_view_for_pending_session_rendering():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "import { useQueryActionControls } from '@/composables/useQueryActionControls'" in content
    assert "import { usePendingSessionPresentation } from '@/composables/usePendingSessionPresentation'" in content
    assert "import { usePendingSessionViewModels } from '@/composables/usePendingSessionViewModels'" in content
    assert "import { useQuerySessionSnapshots } from '@/composables/useQuerySessionSnapshots'" in content
    assert "v-if=\"pendingSessionNode === 'table_resolution' && pendingSessionChallengeItem\"" in content
    assert "请在下方选择数据表，可单选，也可多选。" in content
    assert "请从全部数据表中选择，可单选，也可多选。" in content
    assert "const hasLegacyPendingConfirmFallback = computed(() =>" not in content
    assert "const hasLegacyPendingTableSelectionFallback = computed(() =>" not in content
    assert "const {\n  normalizeSessionSnapshot,\n  cacheResultSessionSnapshot," in content
    assert "const {\n  applyPendingSessionSnapshot,\n  loadQuerySessionSnapshot,\n  buildPendingExplanation,\n  isManualTableOverride," in content
    assert "const {\n  pendingSessionTitle,\n  pendingSessionNodeLabel,\n  pendingSessionIcon,\n  pendingSessionSummaryItems," in content
    assert "} = usePendingSessionPresentation({" in content
    assert "const { pendingSessionActionButtons, getVisibleResultActions, hasVisibleResultActions } = useQueryActionControls({" in content
    assert "v-for=\"action in pendingSessionActionButtons\"" in content
    assert "v-if=\"hasLegacyPendingConfirmFallback\"" not in content
    assert "v-if=\"hasLegacyPendingTableSelectionFallback\"" not in content
    assert "pendingSessionSnapshot.value?.confirmation_view?.pending_actions" in content
    assert "pendingSessionSnapshot.value?.pending_actions" not in content
    assert "请选择接下来要使用的数据表" not in content
    assert "请选择要继续生成 IR 的数据表" not in content
    assert "res.data?.active_query_session || null" in content
    assert "const snapshot = applyPendingSessionSnapshot(activeQuerySession, { preserveSelection: true })" in content
    assert "messages.value = rawMessages.map(hydrateConversationMessage)" in content
    assert "v-if=\"pendingSessionSummaryItems.length\"" in content
    assert "class=\"understanding-list\"" in content
    assert "🤖 系统理解：" in content
    assert "🤖 系统确认：" not in content


def test_chat_view_uses_resume_directive_to_continue_pending_query():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "async function continueQueryFromResumeDirective" in content
    assert "if (result?.resume_directive?.should_resume)" in content
    assert "await continueQueryFromResumeDirective(result.resume_directive)" in content


def test_chat_view_simplifies_confirmation_mode_to_two_visible_options():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "const SYSTEM_DEFAULT_CONFIRMATION_MODE = 'always_confirm'" in content
    assert "function isConfirmationModeActive(mode)" in content
    assert "label: '智能确认'" in content
    assert "label: '始终确认'" in content
    assert "label: '跟随系统'" not in content
    assert "value: 'system'" not in content
    assert "本次确认：" in content
    assert "function getMessageConfirmationModeLabel(msg)" in content


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
    assert "{ silentStatuses: [404] }" in content
    assert "if (followupResolutionResult?.resolution === 'need_clarification')" in content
    assert "analysis_context: options.analysisContext || null" in content
    assert "followup_resolution: options.followupResolution || null" in content


def test_chat_view_uses_result_action_contract_for_result_stage_actions():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "const resultActionLoadingIds = reactive({})" in content
    assert "const resultReplyContext = ref(null)" in content
    assert "loadResultSessionSnapshot," in content
    assert "hydrateResultActionContracts," in content
    assert "canUseResultAction," in content
    assert "v-for=\"action in getVisibleResultActions(msg)\"" in content
    assert "async function submitResultSessionAction" in content
    assert "function startResultRevision(msg)" in content
    assert "async function submitResultRevisionReply(text)" in content
    assert "function getPendingActionBinding(semanticAction)" in content
    assert "const resolvedActionType = semanticAction ? getPendingActionBinding(semanticAction) : actionType" in content
    assert "semanticAction: 'choose_table'" in content
    assert "semanticAction: 'approve_execution'" in content
    assert "semanticAction: 'confirm_draft'" in content
    assert "semanticAction: 'cancel_query'" in content
    assert "await loadResultSessionSnapshot(msg.query_id)" in content
    assert "msg.status === 'error' && hasVisibleResultActions(msg)" in content
    assert "当前将作为上一条结果的修改意见提交" in content
    assert "async function requestManualTableSelection()" in content
    assert "resetAllTablesFilter()" in content
    assert "await expandAllTables()" in content
    assert "semanticAction: 'manual_select_table'" not in content
    assert "const restartingQueryIds = reactive({})" not in content
    assert "function canRetryTableSelection(msg)" not in content
    assert "不是这张表，重新选表" not in content
    assert "pendingConfirm.value = payload.confirmation" not in content
    assert "pendingTableSelection.value = payload.table_selection" not in content


def test_query_action_controls_composable_holds_action_mapping_contract():
    composable = ROOT_DIR / "frontend" / "src" / "composables" / "useQueryActionControls.js"
    content = composable.read_text(encoding="utf-8")

    assert "export function useQueryActionControls({" in content
    assert "const RESULT_ACTION_ORDER = ['change_table', 'revise']" in content
    assert "const pendingSessionActionButtons = computed(() =>" in content
    assert "function getVisibleResultActions(msg)" in content
    assert "buildPendingActionButton('choose_table'" in content
    assert "buildPendingActionButton('cancel_query'" in content
    assert "pendingTableSelection," in content
    assert "label: '继续执行'" in content
    assert "label: '查看所有表'" in content
    assert "确认所选（${selectedTableIds.value.length}）" in content
    assert "'确认所选'" in content
    assert "label: '改问题'" in content
    assert "requestPendingExplanation" not in content
    assert "label: '不是这张表'" not in content
    assert "hasModelRecommendedTables" not in content
    assert "revise: '修改问题'" in content
    assert "request_explanation: '查看系统理解'" not in content
    assert "继续修改" not in content
    assert "解释一下" not in content


def test_query_session_snapshots_composable_holds_snapshot_cache_contract():
    composable = ROOT_DIR / "frontend" / "src" / "composables" / "useQuerySessionSnapshots.js"
    content = composable.read_text(encoding="utf-8")

    assert "export function useQuerySessionSnapshots({ messages })" in content
    assert "const resultSessionSnapshots = reactive({})" in content
    assert "const QUERY_SESSION_SNAPSHOT_RETRY_COUNT = 2" in content
    assert "function normalizeSessionSnapshot(payload)" in content
    assert "function cacheResultSessionSnapshot(payload)" in content
    assert "async function fetchQuerySessionSnapshotWithRetry(" in content
    assert "querySessionAPI.get(queryId, { silentStatuses: [404] })" in content
    assert "async function loadResultSessionSnapshot(queryId, { force = false } = {})" in content
    assert "async function hydrateResultActionContracts(targetMessages = messages?.value || [])" in content
    assert "message_id: payload.message_id || payload.session?.message_id || null" in content
    assert "!msg?.hidden &&" in content
    assert "pending_actions: state.pending_actions || []" not in content


def test_pending_session_view_models_composable_holds_pending_card_contract():
    composable = ROOT_DIR / "frontend" / "src" / "composables" / "usePendingSessionViewModels.js"
    content = composable.read_text(encoding="utf-8")

    assert "export function usePendingSessionViewModels({" in content
    assert "function buildPendingTableSelectionCard(snapshot)" in content
    assert "function buildPendingConfirmCard(snapshot, currentNode)" in content
    assert "function applyPendingSessionSnapshot(payload, options = {})" in content
    assert "pendingQueryText.value = snapshot.confirmation_view?.context?.question_text ||" in content
    assert "async function fetchPendingQuerySessionSnapshotWithRetry(" in content
    assert "querySessionAPI.get(queryId, { silentStatuses: [404] })" in content
    assert "async function loadQuerySessionSnapshot(" in content
    assert "function buildPendingExplanation(snapshot = pendingSessionSnapshot.value)" in content
    assert "function getSnapshotSelectedTableNames(snapshot = pendingSessionSnapshot.value)" in content
    assert "allow_multi_select: true" in content
    assert "当前涉及数据表：${selectedTableNames.join('、')}" in content
    assert "clearPendingSessionState({ keepQueryText: true })" in content


def test_request_interceptor_supports_silent_statuses():
    request_file = ROOT_DIR / "frontend" / "src" / "utils" / "request.js"
    content = request_file.read_text(encoding="utf-8")

    assert "const silentStatuses = Array.isArray(error.config?.silentStatuses) ? error.config.silentStatuses : []" in content
    assert "const suppressErrorMessage = Boolean(error.config?.suppressErrorMessage) || silentStatuses.includes(status)" in content


def test_pending_session_presentation_composable_holds_node_display_contract():
    composable = ROOT_DIR / "frontend" / "src" / "composables" / "usePendingSessionPresentation.js"
    content = composable.read_text(encoding="utf-8")

    assert "export function usePendingSessionPresentation({" in content
    assert "const PENDING_SESSION_NODE_META = {" in content
    assert "table_resolution:" in content
    assert "execution_guard:" in content
    assert "draft_confirmation:" in content
    assert "const pendingSessionMeta = computed(() =>" in content
    assert "const pendingSessionTitle = computed(() => pendingSessionMeta.value.title)" in content
    assert "const pendingSessionNodeLabel = computed(() => pendingSessionMeta.value.label)" in content
    assert "const pendingSessionIcon = computed(() => pendingSessionMeta.value.icon)" in content
    assert "const pendingSessionSummaryItems = computed(() =>" in content
    assert "const pendingSessionSummaryText = computed(() =>" in content
    assert "const pendingSessionChallengeItem = computed(() =>" in content
    assert "const pendingSessionDomainHint = computed(() =>" in content
    assert "const pendingSessionKnownConstraints = computed(() =>" in content
    assert "const pendingTableResolutionDraftPreview = computed(() =>" in content
    assert "function looksLikeUuid(value)" in content
    assert "function normalizeTableSummaryItem(text, prefix)" in content
    assert "function normalizeSummaryItem(item)" in content
    assert "function splitDraftUnderstandingItems(text)" in content
    assert "for (const prefix of ['当前数据表：', '当前涉及数据表：'])" in content
    assert ".split('；')" in content
    assert "pendingConfirmationView.value?.draft?.natural_language || ''" in content
    assert "pendingTableSelection.value?.confirmation_reason" in content
    assert "return looksLikeUuid(domainHint) ? '' : domainHint" in content
    assert "请从全部数据表中选择要查询的数据表，可单选，也可多选。" in content
    assert "需要您确认后再继续生成 IR" not in content


def test_chat_view_renders_structured_draft_confirmation_details():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "class=\"understanding-list\"" in content
    assert "pendingSessionNode === 'table_resolution' && pendingSessionChallengeItem" in content
    assert "class=\"pending-challenge-text\"" in content
    assert "class=\"candidate-topline\"" in content
    assert "class=\"candidate-primary\"" in content
    assert "class=\"candidate-meta candidate-meta-inline\"" in content
    assert "class=\"candidate-meta-item\"" in content
    assert "pendingConfirm?.selected_table_names?.length" not in content
    assert "pendingConfirm?.confidence !== null" not in content
    assert "pendingConfirm?.open_points?.length" not in content


def test_chat_view_table_selection_is_always_multi_selectable():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "selectedTableIds.value.push(tableId)" in content
    assert "selectedTableIds.value = [...selectedTableIds.value, tableId]" in content
    assert "if (pendingTableSelection.value?.allow_multi_select)" not in content


def test_chat_view_restores_thinking_steps_and_reuses_existing_assistant_message():
    chat_view = ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue"
    content = chat_view.read_text(encoding="utf-8")

    assert "function hydrateConversationMessage(msg)" in content
    assert "nextMessage.metadata?.thinking_steps || nextMessage.thinking_steps" in content
    assert "hidden: Boolean(nextMessage.metadata?.hidden) || isLegacyBlankAssistantMessage(nextMessage)" in content
    assert "function reuseOrCreateAssistantMessage(queryId)" in content
    assert "const assistantMessageId = reuseOrCreateAssistantMessage(queryId)" in content
