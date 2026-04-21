import { querySessionAPI } from '@/api'

const PENDING_QUERY_SESSION_RETRY_COUNT = 2
const PENDING_QUERY_SESSION_RETRY_DELAY_MS = 200

function looksLikeUuid(value) {
  return typeof value === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.trim())
}

export function usePendingSessionViewModels({
  pendingSessionSnapshot,
  pendingConfirm,
  pendingTableSelection,
  pendingQueryText,
  originalQueryId,
  selectedTableIds,
  selectedTableId,
  tableBatchIndex,
  pendingRevisionNote,
  normalizeSessionSnapshot,
  cacheResultSessionSnapshot,
  clearPendingSessionState,
  resetPendingTableUi,
  formatEstimatedRows,
  expandAllTables,
}) {
  function getDefaultSelectedTableIds(card, state = {}, options = {}) {
    const preserveSelection = options.preserveSelection === true
    const rejectedTableIds = new Set(state.rejected_table_ids || [])

    if (preserveSelection && selectedTableIds.value.length > 0) {
      const preserved = selectedTableIds.value.filter(id => !rejectedTableIds.has(id))
      if (preserved.length > 0) return preserved
    }

    const stateSelected = (state.selected_table_ids || []).filter(id => !rejectedTableIds.has(id))
    if (stateSelected.length > 0) return stateSelected

    const recommendedRaw = card?.recommended_table_ids || state.recommended_table_ids || []
    const recommended = recommendedRaw.filter(id => !rejectedTableIds.has(id))
    if (recommended.length > 0) {
      return recommended
    }

    const candidates = (card?.candidates || []).filter(candidate => !rejectedTableIds.has(candidate.table_id))
    return candidates[0] ? [candidates[0].table_id] : []
  }

  function getSnapshotSelectedTableIds(snapshot = pendingSessionSnapshot.value) {
    const normalized = normalizeSessionSnapshot(snapshot)
    if (!normalized) return []

    const confirmationView = normalized.confirmation_view || {}
    const selectedFromMeta = confirmationView.dependency_meta?.selected_table_ids
    if (Array.isArray(selectedFromMeta) && selectedFromMeta.length > 0) {
      return selectedFromMeta
    }

    const selectedFromTable = confirmationView.table_resolution?.selected_table_ids
    if (Array.isArray(selectedFromTable) && selectedFromTable.length > 0) {
      return selectedFromTable
    }

    return normalized.state?.selected_table_ids || []
  }

  function isManualTableOverride(snapshot = pendingSessionSnapshot.value) {
    const normalized = normalizeSessionSnapshot(snapshot)
    if (!normalized) return false

    const fromView = normalized.confirmation_view?.table_resolution?.manual_table_override
    if (fromView !== undefined && fromView !== null) {
      return Boolean(fromView)
    }
    return Boolean(normalized.state?.manual_table_override)
  }

  function getSnapshotSelectedTableNames(snapshot = pendingSessionSnapshot.value) {
    const normalized = normalizeSessionSnapshot(snapshot)
    if (!normalized) return []

    const draftNames = normalized.confirmation_view?.draft?.selected_table_names
    if (Array.isArray(draftNames) && draftNames.length > 0) {
      return draftNames.filter(name => name && !looksLikeUuid(name))
    }

    const safeConstraints = normalized.confirmation_view?.context?.safe_summary?.known_constraints || []
    const tableConstraint = safeConstraints.find(item => typeof item === 'string' && item.startsWith('当前数据表：'))
    if (tableConstraint) {
      return tableConstraint
        .slice('当前数据表：'.length)
        .split('、')
        .map(name => name.trim())
        .filter(name => name && !looksLikeUuid(name))
    }

    const tableResolution = normalized.confirmation_view?.table_resolution || null
    const selectedTableIds = getSnapshotSelectedTableIds(normalized)
    if (tableResolution?.candidates?.length && selectedTableIds.length > 0) {
      return tableResolution.candidates
        .filter(candidate => selectedTableIds.includes(candidate.table_id))
        .map(candidate => candidate.table_name)
        .filter(name => name && !looksLikeUuid(name))
    }

    return []
  }

  function buildPendingTableSelectionCard(snapshot) {
    const normalized = normalizeSessionSnapshot(snapshot)
    if (!normalized) return null

    const tableResolution = normalized.confirmation_view?.table_resolution || null
    if (!tableResolution) return null

    return {
      question: tableResolution.question || normalized.state?.question_text || '',
      message: tableResolution.message || '',
      confirmation_reason: tableResolution.reason_summary || '',
      candidates: tableResolution.candidates || [],
      recommended_table_ids: tableResolution.recommended_table_ids || [],
      selected_table_ids: tableResolution.selected_table_ids || getSnapshotSelectedTableIds(normalized),
      rejected_table_ids: tableResolution.rejected_table_ids || normalized.state?.rejected_table_ids || [],
      allow_multi_select: true,
      multi_table_mode: tableResolution.multi_table_mode || normalized.state?.multi_table_mode || null,
      manual_table_override: isManualTableOverride(normalized)
    }
  }

  function buildPendingConfirmCard(snapshot, currentNode) {
    const normalized = normalizeSessionSnapshot(snapshot)
    if (!normalized) return null

    const viewCard = currentNode === 'execution_guard'
      ? normalized.confirmation_view?.execution_guard
      : normalized.confirmation_view?.draft

    if (!viewCard) return null

    return {
      natural_language: viewCard.natural_language || '',
      warnings: viewCard.warnings || [],
      suggestions: viewCard.suggestions || [],
      confidence: viewCard.confidence ?? null,
      ambiguities: viewCard.ambiguities || [],
      open_points: viewCard.open_points || [],
      selected_table_names: viewCard.selected_table_names || [],
      estimated_cost: viewCard.estimated_cost || null,
      ir: viewCard.draft_json || viewCard.ir || null
    }
  }

  function applyPendingSessionSnapshot(payload, options = {}) {
    const snapshot = normalizeSessionSnapshot(payload)
    if (!snapshot) {
      clearPendingSessionState()
      return null
    }

    pendingSessionSnapshot.value = snapshot
    cacheResultSessionSnapshot(snapshot)
    pendingQueryText.value = snapshot.confirmation_view?.context?.question_text ||
      snapshot.state?.question_text ||
      pendingQueryText.value
    originalQueryId.value = snapshot.query_id || originalQueryId.value

    if (snapshot.current_node === 'table_resolution') {
      const card = buildPendingTableSelectionCard(snapshot)
      pendingTableSelection.value = card
      pendingConfirm.value = null
      const nextSelectedTableIds = getDefaultSelectedTableIds(card, snapshot.state || {}, options)
      selectedTableIds.value = nextSelectedTableIds
      selectedTableId.value = nextSelectedTableIds[0] || null
      if (!options.preserveBatch) {
        tableBatchIndex.value = 0
      }
    } else if (snapshot.current_node === 'execution_guard') {
      pendingConfirm.value = buildPendingConfirmCard(snapshot, 'execution_guard')
      pendingTableSelection.value = null
      const stateSelected = getSnapshotSelectedTableIds(snapshot)
      selectedTableIds.value = stateSelected
      selectedTableId.value = stateSelected[0] || null
      resetPendingTableUi()
    } else if (snapshot.current_node === 'draft_confirmation') {
      pendingConfirm.value = buildPendingConfirmCard(snapshot, 'draft_confirmation')
      pendingTableSelection.value = buildPendingTableSelectionCard(snapshot)
      const nextSelectedTableIds = getDefaultSelectedTableIds(pendingTableSelection.value, snapshot.state || {}, options)
      selectedTableIds.value = nextSelectedTableIds
      selectedTableId.value = nextSelectedTableIds[0] || null
    } else {
      pendingConfirm.value = null
      pendingTableSelection.value = null
      resetPendingTableUi()
    }

    return snapshot
  }

  function sleep(ms) {
    return new Promise(resolve => window.setTimeout(resolve, ms))
  }

  async function fetchPendingQuerySessionSnapshotWithRetry(
    queryId,
    {
      retryCount = PENDING_QUERY_SESSION_RETRY_COUNT,
      retryDelayMs = PENDING_QUERY_SESSION_RETRY_DELAY_MS,
    } = {}
  ) {
    let attempt = 0

    while (true) {
      try {
        return await querySessionAPI.get(queryId, { silentStatuses: [404] })
      } catch (e) {
        if (e?.response?.status !== 404 || attempt >= retryCount) {
          throw e
        }
        attempt += 1
        await sleep(retryDelayMs * attempt)
      }
    }
  }

  async function loadQuerySessionSnapshot(
    queryId,
    { preserveSelection = false, preserveExistingOnError = false } = {}
  ) {
    if (!queryId) return null

    try {
      const res = await fetchPendingQuerySessionSnapshotWithRetry(queryId)
      const snapshot = applyPendingSessionSnapshot(res.data, { preserveSelection })

      if (snapshot?.current_node === 'table_resolution' && isManualTableOverride(snapshot)) {
        await expandAllTables()
      }
      return snapshot
    } catch (e) {
      console.warn('加载查询会话失败', e)
      const shouldPreserveExistingState = preserveExistingOnError &&
        pendingSessionSnapshot.value?.query_id === queryId

      if (!shouldPreserveExistingState) {
        clearPendingSessionState({ keepQueryText: true })
      }
      originalQueryId.value = queryId
      return null
    }
  }

  function buildPendingExplanation(snapshot = pendingSessionSnapshot.value) {
    const currentSnapshot = normalizeSessionSnapshot(snapshot)
    if (!currentSnapshot) return '当前没有可解释的确认步骤。'

    if (currentSnapshot.current_node === 'table_resolution') {
      const tableResolution = currentSnapshot.confirmation_view?.table_resolution || null
      const reason = tableResolution?.reason_summary || '当前命中了多个候选表，需要先确认目标表。'
      const candidateReasons = (tableResolution?.candidates || [])
        .slice(0, 3)
        .map(candidate => `- ${candidate.table_name}: ${candidate.reason || '语义相近候选表'}`)
        .join('\n')

      return [
        '当前处于选表确认阶段。',
        `原因：${reason}`,
        candidateReasons ? `候选表依据：\n${candidateReasons}` : ''
      ].filter(Boolean).join('\n\n')
    }

    if (currentSnapshot.current_node === 'execution_guard') {
      const guard = currentSnapshot.confirmation_view?.execution_guard || null
      const warnings = (guard?.warnings || []).map(item => `- ${item}`).join('\n')
      const estimatedRows = guard?.estimated_cost?.rows

      return [
        '当前处于执行确认阶段。',
        estimatedRows !== undefined ? `预计扫描行数：${formatEstimatedRows(estimatedRows)}` : '',
        warnings ? `风险提示：\n${warnings}` : ''
      ].filter(Boolean).join('\n\n')
    }

    if (currentSnapshot.current_node === 'draft_confirmation') {
      const draft = currentSnapshot.confirmation_view?.draft || null
      return [
        draft?.natural_language || '',
        pendingRevisionNote.value ? `当前已记录修改意见：${pendingRevisionNote.value}` : ''
      ].filter(Boolean).join('\n\n') || '当前处于草稿确认阶段，请确认是否继续。'
    }

    if (['completed', 'failed', 'ir_ready', 'table_resolved'].includes(currentSnapshot.current_node)) {
      const draft = currentSnapshot.confirmation_view?.draft || null
      const selectedTableNames = getSnapshotSelectedTableNames(currentSnapshot)
      const lastError = currentSnapshot.session?.last_error || currentSnapshot.state?.last_error || ''
      const resultMeta = currentSnapshot.state?.result_meta || {}
      const statusLabelMap = {
        completed: '结果已生成阶段',
        failed: '执行失败阶段',
        ir_ready: 'SQL 已生成阶段',
        table_resolved: '结果后重选表阶段'
      }

      return [
        `当前处于${statusLabelMap[currentSnapshot.current_node] || '结果态'}。`,
        draft?.natural_language ? `最近一次确认的查询理解：${draft.natural_language}` : '',
        selectedTableNames.length ? `当前涉及数据表：${selectedTableNames.join('、')}` : '',
        resultMeta.row_count !== undefined ? `结果记录数：${resultMeta.row_count}` : '',
        currentSnapshot.current_node === 'failed' && lastError ? `失败原因：${lastError}` : ''
      ].filter(Boolean).join('\n\n')
    }

    return '当前确认原因已记录。'
  }

  return {
    buildPendingTableSelectionCard,
    buildPendingConfirmCard,
    applyPendingSessionSnapshot,
    loadQuerySessionSnapshot,
    buildPendingExplanation,
    isManualTableOverride,
  }
}
