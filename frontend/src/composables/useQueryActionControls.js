import { computed } from 'vue'

const RESULT_ACTION_ORDER = ['change_table', 'revise', 'request_explanation']
const RESULT_ACTION_LABELS = {
  change_table: '重新选表',
  revise: '修改问题',
  request_explanation: '查看系统理解'
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
  requestResultExplanation,
}) {
  const pendingSessionActionButtons = computed(() => {
    const commonDisabled = pendingSessionActionLoading.value

    if (pendingSessionNode.value === 'table_resolution') {
      return filterVisibleActionButtons([
        buildPendingActionButton('choose_table', {
          label: `✓ 确认选择 (${selectedTableIds.value.length}个表)`,
          className: 'btn-confirm',
          disabled: selectedTableIds.value.length === 0 || commonDisabled,
          onClick: () => confirmTableSelection()
        }),
        buildPendingActionButton('change_table', {
          label: '不是这张表',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => requestTableReselection(),
          visible: !showAllAccessibleTables.value
        }),
        buildPendingActionButton('back_to_recommend', {
          label: '← 返回推荐',
          className: 'btn-back-to-recommend',
          disabled: commonDisabled,
          onClick: () => backToRecommendTables(),
          visible: showAllAccessibleTables.value
        }),
        buildPendingActionButton('manual_select_table', {
          label: '手动选表',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => requestManualTableSelection()
        }),
        buildPendingActionButton('revise', {
          label: '修改问题',
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
          label: '✓ 确认执行',
          className: 'btn-confirm',
          disabled: commonDisabled,
          onClick: () => approveExecution()
        }),
        buildPendingActionButton('change_table', {
          label: '重新选表',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => requestTableReselection(),
          visible: canUsePendingAction('change_table')
        }),
        buildPendingActionButton('revise', {
          label: '修改问题',
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
          label: '✓ 确认当前草稿',
          className: 'btn-confirm',
          disabled: commonDisabled,
          onClick: () => confirmDraftRevision()
        }),
        buildPendingActionButton('revise', {
          label: '修改问题',
          className: 'btn-secondary',
          disabled: commonDisabled,
          onClick: () => focusPendingReplyInput('请修改为：'),
          visible: canUsePendingAction('revise')
        }),
        buildPendingActionButton('change_table', {
          label: '重新选表',
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
    if (actionType === 'request_explanation') {
      return () => requestResultExplanation(msg)
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
