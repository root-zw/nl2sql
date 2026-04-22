import { computed } from 'vue'

const RESULT_ACTION_ORDER = ['change_table', 'revise']
const RESULT_ACTION_LABELS = {
  change_table: '重新选表',
  revise: '修改问题'
}

function buildPendingActionButton(key, {
  label,
  className = 'btn-secondary',
  disabled = false,
  onClick,
  visible = true
} = {}) {
  return {
    key,
    label,
    className,
    disabled,
    onClick,
    visible
  }
}

function filterVisibleActionButtons(buttons) {
  return (buttons || []).filter(button => button && button.visible !== false)
}

export function useQueryActionControls({
  pendingSessionNode,
  pendingSessionActionLoading,
  selectedTableIds,
  pendingTableSelection,
  showAllAccessibleTables,
  canUsePendingAction,
  confirmTableSelection,
  requestTableReselection,
  backToRecommendTables,
  requestManualTableSelection,
  focusPendingReplyInput,
  cancelPendingSession,
  approveExecution,
  confirmDraftRevision,
  canUseResultAction,
  isResultRevisionActive,
  isResultActionBusy,
  reopenTableSelectionForMessage,
  startResultRevision,
}) {
  const pendingSessionActionButtons = computed(() => {
    const commonDisabled = pendingSessionActionLoading.value

    if (pendingSessionNode.value === 'table_resolution') {
      return filterVisibleActionButtons([
        buildPendingActionButton('choose_table', {
          label: selectedTableIds.value.length > 1 ? `确认所选（${selectedTableIds.value.length}）` : '确认所选',
          className: 'btn-confirm',
          disabled: selectedTableIds.value.length === 0 || commonDisabled,
          onClick: () => confirmTableSelection()
        }),
        buildPendingActionButton('back_to_recommend', {
          label: '返回推荐',
          className: 'btn-back-to-recommend',
          disabled: commonDisabled,
          onClick: () => backToRecommendTables(),
          visible: showAllAccessibleTables.value
        }),
        buildPendingActionButton('manual_select_table', {
          label: '查看所有表',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => requestManualTableSelection(),
          visible: !showAllAccessibleTables.value
        }),
        buildPendingActionButton('revise', {
          label: '改问题',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => focusPendingReplyInput('请修改为：'),
          visible: canUsePendingAction('revise')
        }),
        buildPendingActionButton('cancel_query', {
          label: '取消',
          className: 'btn-cancel',
          disabled: commonDisabled,
          onClick: () => cancelPendingSession()
        })
      ])
    }

    if (pendingSessionNode.value === 'execution_guard') {
      return filterVisibleActionButtons([
        buildPendingActionButton('approve_execution', {
          label: '继续执行',
          className: 'btn-confirm',
          disabled: commonDisabled,
          onClick: () => approveExecution()
        }),
        buildPendingActionButton('change_table', {
          label: '不是这张表',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => requestTableReselection(),
          visible: canUsePendingAction('change_table')
        }),
        buildPendingActionButton('revise', {
          label: '改问题',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => focusPendingReplyInput('请修改为：'),
          visible: canUsePendingAction('revise')
        }),
        buildPendingActionButton('cancel_query', {
          label: '取消查询',
          className: 'btn-cancel',
          disabled: commonDisabled,
          onClick: () => cancelPendingSession()
        })
      ])
    }

    if (pendingSessionNode.value === 'draft_confirmation') {
      return filterVisibleActionButtons([
        buildPendingActionButton('confirm_draft', {
          label: '继续',
          className: 'btn-confirm',
          disabled: commonDisabled,
          onClick: () => confirmDraftRevision()
        }),
        buildPendingActionButton('revise', {
          label: '改问题',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => focusPendingReplyInput('请修改为：'),
          visible: canUsePendingAction('revise')
        }),
        buildPendingActionButton('change_table', {
          label: '不是这张表',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => requestTableReselection(),
          visible: canUsePendingAction('change_table')
        }),
        buildPendingActionButton('cancel_query', {
          label: '取消',
          className: 'btn-cancel',
          disabled: commonDisabled,
          onClick: () => cancelPendingSession()
        })
      ])
    }

    return []
  })

  function getResultActionLabel(msg, actionType) {
    if (actionType === 'revise') {
      return isResultRevisionActive(msg) ? '请输入修改意见...' : RESULT_ACTION_LABELS[actionType]
    }

    if (isResultActionBusy(msg)) {
      return '处理中...'
    }

    return RESULT_ACTION_LABELS[actionType] || actionType
  }

  function createResultActionHandler(msg, actionType) {
    if (actionType === 'change_table') {
      return () => reopenTableSelectionForMessage(msg)
    }
    if (actionType === 'revise') {
      return () => startResultRevision(msg)
    }
    return () => {}
  }

  function getVisibleResultActions(msg) {
    const actionKey = msg?.message_id || msg?.query_id || 'result'
    return RESULT_ACTION_ORDER
      .filter(actionType => canUseResultAction(msg, actionType))
      .map(actionType => ({
        key: `${actionKey}-${actionType}`,
        actionType,
        label: getResultActionLabel(msg, actionType),
        disabled: isResultActionBusy(msg),
        onClick: createResultActionHandler(msg, actionType)
      }))
  }

  function hasVisibleResultActions(msg) {
    return getVisibleResultActions(msg).length > 0
  }

  return {
    pendingSessionActionButtons,
    getVisibleResultActions,
    hasVisibleResultActions,
  }
}
