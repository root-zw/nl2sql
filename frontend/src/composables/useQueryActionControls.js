import { computed } from 'vue'

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
  canUsePendingAction,
  confirmTableSelection,
  requestTableReselection,
  focusPendingReplyInput,
  cancelPendingSession,
  approveExecution,
  confirmDraftRevision,
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
          label: '重新选表',
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

  return {
    pendingSessionActionButtons,
  }
}
