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

function splitDraftUnderstandingItems(text) {
  const normalizedText = typeof text === 'string' ? text.trim() : ''
  if (!normalizedText) return []

  const strippedText = normalizedText
    .replace(/。?请确认是否继续。?$/, '')
    .trim()

  if (!strippedText) return []

  return strippedText
    .split('；')
    .flatMap(item => item.split('\n'))
    .map(item => item.trim().replace(/^[；。]+|[；。]+$/g, ''))
    .map(normalizeSummaryItem)
    .filter(Boolean)
}

function extractUnderstandingTexts(items) {
  return (Array.isArray(items) ? items : [])
    .map(item => {
      if (typeof item === 'string') return item
      if (item && typeof item === 'object') return item.text || ''
      return ''
    })
    .map(normalizeSummaryItem)
    .filter(Boolean)
}

function mergeSummaryItems(...groups) {
  const merged = []
  const seen = new Set()

  for (const group of groups) {
    for (const rawItem of group || []) {
      const item = normalizeSummaryItem(rawItem)
      if (!item || seen.has(item)) continue
      seen.add(item)
      merged.push(item)
    }
  }

  return merged
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
        return ['请从全部数据表中选择要查询的数据表，可单选，也可多选。']
      }

      const draftUnderstandingItems = splitDraftUnderstandingItems(
        pendingConfirmationView.value?.draft?.natural_language || ''
      )
      if (draftUnderstandingItems.length > 0) {
        return mergeSummaryItems(draftUnderstandingItems, safeConstraints)
      }

      const goalSummary = safeSummary.user_goal_summary
        ? `当前理解：${safeSummary.user_goal_summary}`
        : '系统已识别到可能相关的数据表，请确认后继续。'

      return mergeSummaryItems([goalSummary], safeConstraints)
    }

    if (pendingSessionNode.value === 'execution_guard') {
      return [
        pendingConfirmationView.value?.execution_guard?.natural_language || '该查询可能扫描较大数据量，请确认是否继续执行。'
      ].map(normalizeSummaryItem).filter(Boolean)
    }

    if (pendingSessionNode.value === 'draft_confirmation') {
      const draftUnderstandingItems = extractUnderstandingTexts(
        pendingConfirmationView.value?.draft?.system_understanding
      )
      const fallbackDraftUnderstandingItems = splitDraftUnderstandingItems(
        pendingConfirmationView.value?.draft?.natural_language || ''
      )
      const revisionItems = revisionText ? [`已吸收修改：${revisionText}`] : []

      if (draftUnderstandingItems.length > 0) {
        return mergeSummaryItems(revisionItems, draftUnderstandingItems)
      }
      if (fallbackDraftUnderstandingItems.length > 0) {
        return mergeSummaryItems(revisionItems, fallbackDraftUnderstandingItems)
      }

      const fallback = pendingConfirmationView.value?.draft?.natural_language
        || '系统已生成新的查询草稿，请确认是否继续。'
      return mergeSummaryItems(revisionItems, [fallback])
    }

    return []
  })

  const pendingSessionSummaryText = computed(() => pendingSessionSummaryItems.value.join('\n'))

  const pendingSessionChallengeItem = computed(() => {
    if (pendingSessionNode.value === 'table_resolution') {
      if (pendingTableSelection.value?.manual_table_override || pendingSessionState.value.manual_table_override) {
        return ''
      }
      return normalizeSummaryItem(
        pendingTableSelection.value?.confirmation_reason ||
        pendingConfirmationView.value?.context?.safe_summary?.open_points?.[0] ||
        ''
      )
    }

    return ''
  })

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
    pendingSessionChallengeItem,
    pendingSessionDomainHint,
    pendingSessionKnownConstraints,
    pendingTableResolutionDraftPreview,
  }
}
