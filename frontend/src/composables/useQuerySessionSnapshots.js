import { reactive } from 'vue'
import { querySessionAPI } from '@/api'

const QUERY_SESSION_SNAPSHOT_RETRY_COUNT = 2
const QUERY_SESSION_SNAPSHOT_RETRY_DELAY_MS = 200

export function useQuerySessionSnapshots({ messages }) {
  const resultSessionSnapshots = reactive({})
  const resultSessionSnapshotLoading = reactive({})
  const resultSessionSnapshotPromises = {}

  function sleep(ms) {
    return new Promise(resolve => window.setTimeout(resolve, ms))
  }

  async function fetchQuerySessionSnapshotWithRetry(
    queryId,
    {
      retryCount = QUERY_SESSION_SNAPSHOT_RETRY_COUNT,
      retryDelayMs = QUERY_SESSION_SNAPSHOT_RETRY_DELAY_MS,
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

  function normalizeSessionSnapshot(payload) {
    if (!payload) return null
    const confirmationView = payload.confirmation_view || payload.session?.confirmation_view || null

    if (payload.state) {
      return {
        ...payload,
        message_id: payload.message_id || payload.session?.message_id || null,
        confirmation_view: confirmationView,
      }
    }

    const state = payload.state_json || {}
    return {
      query_id: payload.query_id,
      message_id: payload.message_id || payload.session?.message_id || null,
      status: payload.status,
      current_node: payload.current_node,
      confirmation_view: confirmationView,
      state,
      session: payload.session || payload
    }
  }

  function cacheResultSessionSnapshot(payload) {
    const snapshot = normalizeSessionSnapshot(payload)
    if (snapshot?.query_id) {
      resultSessionSnapshots[snapshot.query_id] = snapshot
    }
    return snapshot
  }

  function getResultSessionSnapshot(queryId) {
    if (!queryId) return null
    return resultSessionSnapshots[queryId] || null
  }

  function getResultActionContract(msg) {
    if (!msg?.query_id) return null
    return getResultSessionSnapshot(msg.query_id)?.confirmation_view?.result_actions || null
  }

  function canUseResultAction(msg, actionType) {
    return Boolean(getResultActionContract(msg)?.available_actions?.includes(actionType))
  }

  function getResultActionBinding(msg, actionType) {
    return getResultActionContract(msg)?.action_bindings?.[actionType] || null
  }

  async function loadResultSessionSnapshot(queryId, { force = false } = {}) {
    if (!queryId) return null
    if (!force && resultSessionSnapshots[queryId]) {
      return resultSessionSnapshots[queryId]
    }
    if (resultSessionSnapshotPromises[queryId]) {
      return await resultSessionSnapshotPromises[queryId]
    }

    resultSessionSnapshotLoading[queryId] = true
    resultSessionSnapshotPromises[queryId] = fetchQuerySessionSnapshotWithRetry(queryId)
      .then(res => cacheResultSessionSnapshot(res.data))
      .catch(e => {
        console.warn('加载结果态动作契约失败', e)
        return null
      })
      .finally(() => {
        delete resultSessionSnapshotLoading[queryId]
        delete resultSessionSnapshotPromises[queryId]
      })

    return await resultSessionSnapshotPromises[queryId]
  }

  async function hydrateResultActionContracts(targetMessages = messages?.value || []) {
    const queryIds = [...new Set(
      (targetMessages || [])
        .filter(msg =>
          msg?.role === 'assistant' &&
          msg?.query_id &&
          !msg?.hidden &&
          msg?.status !== 'pending' &&
          msg?.status !== 'running'
        )
        .map(msg => msg.query_id)
    )]

    if (queryIds.length === 0) return
    await Promise.allSettled(queryIds.map(queryId => loadResultSessionSnapshot(queryId)))
  }

  return {
    normalizeSessionSnapshot,
    cacheResultSessionSnapshot,
    getResultSessionSnapshot,
    canUseResultAction,
    getResultActionBinding,
    loadResultSessionSnapshot,
    hydrateResultActionContracts,
  }
}
