import { computed } from 'vue'

function looksLikeUuid(value) {
  return typeof value === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.trim())
}

function normalizeTableSummaryItem(text, prefix) {
  if (!text.startsWith(prefix)) return null
  const tableNames = text
    .slice(prefix.length)
    .split('、')
    .map(name => name.trim())
    .filter(name => name && !looksLikeUuid(name))
  if (tableNames.length === 0) return ''
  return `${prefix}${tableNames.join('、')}`
}

function normalizeSummaryItem(item) {
  const text = typeof item === 'string' ? item.trim() : ''
  if (!text) return ''
  if (looksLikeUuid(text)) return ''
  for (const prefix of ['当前数据表：', '当前涉及数据表：']) {
    const normalizedTableSummary = normalizeTableSummaryItem(text, prefix)
    if (normalizedTableSummary !== null) {
      return normalizedTableSummary
    }
  }
  return text
}

const PENDING_SESSION_NODE_META = {
  table_resolution: {
    title: '请确认要使用的数据表',
    label: '选表确认',
    icon: '📊',
  },
  execution_guard: {
    title: '请确认是否执行查询',
    label: '执行确认',
    icon: '⚠️',
  },
  draft_confirmation: {
    title: '请确认当前查询草稿',
    label: '草稿确认',
    icon: '📝',
  },
}

export function usePendingSessionPresentation({
  pendingSessionSnapshot,
  pendingSessionState,
  pendingSessionNode,
  pendingTableSelection,
  pendingQueryText,
}) {
  const pendingConfirmationView = computed(() => {
    return pendingSessionSnapshot.value?.confirmation_view || null
  })

  const pendingSessionMeta = computed(() => {
    return PENDING_SESSION_NODE_META[pendingSessionNode.value] || {
      title: '请确认当前操作',
      label: '确认中',
      icon: '🤖',
    }
  })

  const pendingSessionTitle = computed(() => pendingSessionMeta.value.title)

  const pendingSessionNodeLabel = computed(() => pendingSessionMeta.value.label)

  const pendingSessionIcon = computed(() => pendingSessionMeta.value.icon)

  const pendingSessionSummaryItems = computed(() => {
    const safeSummary = pendingConfirmationView.value?.context?.safe_summary || {}
    const safeConstraints = Array.isArray(safeSummary.known_constraints) ? safeSummary.known_constraints : []
    const revisionRequest = pendingSessionState.value?.revision_request || {}
    const revisionText = revisionRequest.text || revisionRequest.source_text || revisionRequest.natural_language_reply || ''

    if (pendingSessionNode.value === 'table_resolution') {
      if (pendingTableSelection.value?.manual_table_override || pendingSessionState.value.manual_table_override) {
        return ['已切换为手动选表，请重新确认要查询的数据表。']
      }

      const goalSummary = safeSummary.user_goal_summary && safeSummary.user_goal_summary !== pendingQueryText.value
        ? `当前理解：${safeSummary.user_goal_summary}`
        : ''
      const reasonText = pendingTableSelection.value?.message ||
        pendingTableSelection.value?.confirmation_reason ||
        safeSummary.open_points?.[0] ||
        '系统识别到多个候选表，需要您确认后再继续生成查询草稿。'

      return [goalSummary, reasonText].map(normalizeSummaryItem).filter(Boolean)
    }

    if (pendingSessionNode.value === 'execution_guard') {
      return [
        pendingConfirmationView.value?.execution_guard?.natural_language || '该查询可能扫描较大数据量，请确认是否继续执行。'
      ].map(normalizeSummaryItem).filter(Boolean)
    }

    if (pendingSessionNode.value === 'draft_confirmation') {
      const draftSummaryItems = []
      if (revisionText) {
        draftSummaryItems.push(`已吸收修改：${revisionText}`)
      }
      draftSummaryItems.push(...safeConstraints)
      if (draftSummaryItems.length === 0) {
        draftSummaryItems.push(
          pendingConfirmationView.value?.draft?.natural_language || '系统已生成新的查询草稿，请确认是否继续。'
        )
      }
      return draftSummaryItems.map(normalizeSummaryItem).filter(Boolean)
    }

    return []
  })

  const pendingSessionSummaryText = computed(() => pendingSessionSummaryItems.value.join('\n'))

  const pendingSessionDomainHint = computed(() => {
    const domainHint = pendingConfirmationView.value?.context?.safe_summary?.domain_hint || ''
    return looksLikeUuid(domainHint) ? '' : domainHint
  })

  const pendingSessionKnownConstraints = computed(() => {
    const constraints = pendingConfirmationView.value?.context?.safe_summary?.known_constraints
    return Array.isArray(constraints) ? constraints.map(normalizeSummaryItem).filter(Boolean) : []
  })

  const pendingTableResolutionDraftPreview = computed(() => {
    if (pendingSessionNode.value !== 'table_resolution') return null

    const draft = pendingConfirmationView.value?.draft || null
    if (!draft) return null

    const naturalLanguage = draft.natural_language || ''
    const warnings = Array.isArray(draft.warnings) ? draft.warnings.filter(Boolean) : []
    if (!naturalLanguage && warnings.length === 0 && !draft.draft_json) {
      return null
    }

    return {
      natural_language: naturalLanguage || '系统已生成暂定草稿，确认选表后会继续细化。',
      warnings,
    }
  })

  return {
    pendingConfirmationView,
    pendingSessionTitle,
    pendingSessionNodeLabel,
    pendingSessionIcon,
    pendingSessionSummaryItems,
    pendingSessionSummaryText,
    pendingSessionDomainHint,
    pendingSessionKnownConstraints,
    pendingTableResolutionDraftPreview,
  }
}
