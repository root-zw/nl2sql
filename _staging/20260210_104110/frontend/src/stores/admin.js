/**
 * 管理后台共享状态
 */
import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from '@/utils/request'

export const useAdminStore = defineStore('admin', () => {
  // 数据库连接列表
  const connections = ref([])
  const connectionsLoading = ref(false)
  const connectionsLoaded = ref(false)

  // 加载数据库连接列表（单例）
  async function loadConnections(force = false) {
    // 如果已加载且非强制刷新，直接返回
    if (connectionsLoaded.value && !force) {
      return connections.value
    }

    connectionsLoading.value = true
    try {
      const { data } = await axios.get('/admin/connections')
      connections.value = data.items || []
      connectionsLoaded.value = true
      return connections.value
    } catch (error) {
      console.error('加载数据库连接列表失败', error)
      throw error
    } finally {
      connectionsLoading.value = false
    }
  }

  // 获取活跃的数据库连接列表（用于下拉框）
  async function getActiveConnections() {
    const allConnections = await loadConnections()
    return allConnections.filter(conn => conn.is_active)
  }

  // 重置状态
  function reset() {
    connections.value = []
    connectionsLoaded.value = false
  }

  return {
    connections,
    connectionsLoading,
    connectionsLoaded,
    loadConnections,
    getActiveConnections,
    reset
  }
})

