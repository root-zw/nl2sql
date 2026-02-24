<template>
  <div class="database-connections">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>💾 数据库连接管理</span>
          <el-button type="primary" @click="showAddDialog">
            <el-icon><Plus /></el-icon>
            添加连接
          </el-button>
        </div>
      </template>

      <!-- 工具栏 -->
      <div class="toolbar">
        <el-input
          v-model="searchText"
          placeholder="搜索连接名称..."
          style="width: 100%; max-width: 300px"
          clearable
        >
          <template #prefix>
            <el-icon><Search /></el-icon>
          </template>
        </el-input>

        <div class="toolbar-right">
          <el-button @click="loadConnections">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </div>
      </div>

      <!-- 连接列表 -->
      <el-table
        v-loading="loading"
        :data="filteredConnections"
        stripe
        style="width: 100%"
        table-layout="auto"
      >
        <el-table-column type="expand">
          <template #default="{ row }">
            <div class="connection-detail">
              <el-descriptions :column="2" border size="small">
                <el-descriptions-item label="连接地址">
                  {{ row.host }}:{{ row.port }}
                </el-descriptions-item>
                <el-descriptions-item label="数据库类型">
                  <el-tag>{{ getDbTypeLabel(row.db_type) }}</el-tag>
                </el-descriptions-item>
                <el-descriptions-item label="数据库名称">
                  {{ row.database_name }}
                </el-descriptions-item>
                <el-descriptions-item label="用户名">
                  {{ row.username }}
                </el-descriptions-item>
                <el-descriptions-item label="描述" :span="2">
                  {{ row.description || '-' }}
                </el-descriptions-item>
                <el-descriptions-item label="最大连接数">
                  {{ row.max_connections }}
                </el-descriptions-item>
                <el-descriptions-item label="连接超时">
                  {{ row.connection_timeout }}秒
                </el-descriptions-item>
                <el-descriptions-item label="创建时间" :span="2">
                  {{ formatDate(row.created_at) }}
                </el-descriptions-item>
                <el-descriptions-item label="最后同步时间" :span="2">
                  {{ formatDate(row.last_sync_at) || '从未同步' }}
                </el-descriptions-item>
              </el-descriptions>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="连接名称" min-width="180">
          <template #default="{ row }">
            <div class="connection-name">
              <span>{{ getDbIcon(row.db_type) }}</span>
              <span style="margin-left: 8px">{{ row.connection_name }}</span>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="数据库类型" min-width="120">
          <template #default="{ row }">
            <el-tag :type="getDbTypeColor(row.db_type)">
              {{ getDbTypeLabel(row.db_type) }}
            </el-tag>
          </template>
        </el-table-column>

        <el-table-column label="主机地址" min-width="140">
          <template #default="{ row }">
            <el-text>{{ row.host }}</el-text>
          </template>
        </el-table-column>

        <el-table-column label="端口" min-width="80">
          <template #default="{ row }">
            <el-text>{{ row.port }}</el-text>
          </template>
        </el-table-column>

        <el-table-column label="数据库名" min-width="120">
          <template #default="{ row }">
            <el-text>{{ row.database_name }}</el-text>
          </template>
        </el-table-column>

        <el-table-column label="用户名" min-width="100">
          <template #default="{ row }">
            <el-text>{{ row.username }}</el-text>
          </template>
        </el-table-column>

        <el-table-column label="同步状态" min-width="100">
          <template #default="{ row }">
            <el-tag
              v-if="row.sync_status === 'success'"
              type="success"
              size="small"
            >
              已同步
            </el-tag>
            <el-tag
              v-else-if="row.sync_status === 'failed'"
              type="danger"
              size="small"
            >
              失败
            </el-tag>
            <el-tag v-else type="info" size="small">
              未同步
            </el-tag>
          </template>
        </el-table-column>

        <el-table-column label="启用" min-width="70" align="center">
          <template #default="{ row }">
            <el-switch
              v-model="row.is_active"
              @change="toggleConnectionStatus(row)"
              :loading="row.updating"
            />
          </template>
        </el-table-column>

        <el-table-column label="操作" min-width="360" fixed="right">
          <template #default="{ row }">
            <div class="action-buttons">
              <el-button
                size="small"
                type="success"
                link
                @click="testConnection(row)"
                :loading="row.testing"
              >
                测试连接
              </el-button>
              <el-button
                size="small"
                type="primary"
                link
                @click="syncSchema(row)"
                :loading="row.syncing"
              >
                同步Schema
              </el-button>
              <el-button
                size="small"
                type="primary"
                link
                @click="showEditDialog(row)"
              >
                编辑
              </el-button>
              <el-button
                size="small"
                type="danger"
                link
                @click="deleteConnection(row)"
              >
                删除
              </el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 添加/编辑对话框 -->
    <ConnectionDialog
      v-model="dialogVisible"
      :connection="currentConnection"
      @success="handleSuccess"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, defineAsyncComponent } from 'vue'
import { Plus, Search, Refresh } from '@element-plus/icons-vue'
import axios from '@/utils/request'
import { ElMessage, ElMessageBox } from 'element-plus'
import { formatDateTime } from '@/utils/date'
import { useAdminStore } from '@/stores/admin'

const ConnectionDialog = defineAsyncComponent(() => import('./ConnectionDialog.vue'))

// 状态
const adminStore = useAdminStore()
const loading = ref(false)
const searchText = ref('')
const connections = ref([])
const dialogVisible = ref(false)
const currentConnection = ref(null)

// 过滤后的连接列表
const filteredConnections = computed(() => {
  if (!searchText.value) return connections.value

  const search = searchText.value.toLowerCase()
  return connections.value.filter(conn =>
    conn.connection_name.toLowerCase().includes(search) ||
    conn.host.toLowerCase().includes(search) ||
    conn.database_name.toLowerCase().includes(search)
  )
})

function syncAdminStoreConnections(list) {
  adminStore.connections = list
  adminStore.connectionsLoaded = true
}

// 加载连接列表
async function loadConnections() {
  loading.value = true
  try {
    const { data } = await axios.get('/admin/connections/all', {
      params: { is_active: null }
    })
    const list = Array.isArray(data) ? data : data?.items || []
    syncAdminStoreConnections(list)
    connections.value = list.map(c => ({
      ...c,
      updating: false,
      testing: false,
      syncing: false
    }))
  } catch (error) {
    ElMessage.error('加载连接列表失败')
    console.error(error)
  } finally {
    loading.value = false
  }
}

// 显示添加对话框
function showAddDialog() {
  currentConnection.value = null
  dialogVisible.value = true
}

// 显示编辑对话框
function showEditDialog(connection) {
  currentConnection.value = connection
  dialogVisible.value = true
}

// 测试连接
async function testConnection(connection) {
  connection.testing = true
  try {
    // 如果连接已存在（有 connection_id），使用 connection_id 测试（从数据库读取密码）
    // 否则使用提供的连接信息测试（用于创建新连接时）
    const payload = connection.connection_id
      ? { connection_id: connection.connection_id }
      : {
          db_type: connection.db_type,
          host: connection.host,
          port: connection.port,
          database_name: connection.database_name,
          username: connection.username,
          password: connection.password || ''
        }
    
    const response = await axios.post('/admin/connections/test', payload)
    if (response.data.success) {
      ElMessage.success('连接测试成功！')
    } else {
      ElMessage.error('连接测试失败: ' + (response.data.message || '未知错误'))
    }
  } catch (error) {
    ElMessage.error('连接测试失败: ' + (error.response?.data?.detail || error.response?.data?.message || error.message))
  } finally {
    connection.testing = false
  }
}

// 同步Schema
async function syncSchema(connection) {
  connection.syncing = true
  try {
    await axios.post(`/admin/connections/${connection.connection_id}/sync`)
    ElMessage.success('Schema同步成功！')
    await loadConnections()
  } catch (error) {
    ElMessage.error('同步失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    connection.syncing = false
  }
}

// 切换连接状态
async function toggleConnectionStatus(connection) {
  connection.updating = true
  try {
    await axios.put(`/admin/connections/${connection.connection_id}`, {
      is_active: connection.is_active
    })
    ElMessage.success(connection.is_active ? '已启用' : '已禁用')
  } catch (error) {
    connection.is_active = !connection.is_active
    ElMessage.error('更新失败')
  } finally {
    connection.updating = false
  }
}

// 删除连接
async function deleteConnection(connection) {
  try {
    await ElMessageBox.confirm(
      `确定要删除连接 "${connection.connection_name}" 吗？`,
      '确认删除',
      { type: 'warning' }
    )

    await axios.delete(`/admin/connections/${connection.connection_id}`)
    ElMessage.success('连接已删除')
    await loadConnections()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败: ' + (error.response?.data?.detail || error.message))
    }
  }
}

// 操作成功回调
function handleSuccess() {
  dialogVisible.value = false
  loadConnections()
}

// 获取数据库图标
function getDbIcon(type) {
  const icons = {
    sqlserver: '🗄️',
    mysql: '🐬',
    postgresql: '🐘'
  }
  return icons[type] || '💾'
}

// 获取数据库类型标签
function getDbTypeLabel(type) {
  const labels = {
    sqlserver: 'SQL Server',
    mysql: 'MySQL',
    postgresql: 'PostgreSQL'
  }
  return labels[type] || type
}

// 获取数据库类型颜色
function getDbTypeColor(type) {
  const colors = {
    sqlserver: 'primary',
    mysql: 'warning',
    postgresql: 'success'
  }
  return colors[type] || ''
}

// 格式化日期 - 使用统一的工具函数
function formatDate(date) {
  return formatDateTime(date)
}

// 初始化
onMounted(() => {
  loadConnections()
})
</script>

<style scoped>
.database-connections {
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 16px;
  font-weight: 600;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.toolbar-right {
  display: flex;
  gap: 12px;
}

.connection-name {
  display: flex;
  align-items: center;
  font-weight: 500;
}

.connection-detail {
  padding: 16px 48px;
}

.action-buttons {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: nowrap;
  white-space: nowrap;
  overflow: visible;
}

.action-buttons .el-button {
  flex-shrink: 0;
}

/* 确保容器填满 */
.database-connections {
  width: 100%;
}

.database-connections .el-card {
  width: 100%;
}

::deep(.el-table) {
  width: 100%;
}

::deep(.el-table__body-wrapper) {
  width: 100%;
}

/* 响应式设计 */
@media screen and (max-width: 1400px) {
  .action-buttons {
    flex-wrap: wrap;
  }
  
  .action-buttons .el-button {
    margin-bottom: 4px;
  }
}

@media screen and (max-width: 768px) {
  .card-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
    font-size: 15px;
  }
  
  .card-header .el-button {
    width: 100%;
  }
  
  .toolbar {
    flex-direction: column;
    align-items: stretch;
    gap: 10px;
  }
  
  .toolbar .el-input {
    width: 100% !important;
    max-width: none !important;
  }
  
  .toolbar-right {
    justify-content: flex-end;
  }
  
  .connection-detail {
    padding: 12px 16px;
  }
  
  /* 表格容器横向滚动 */
  :deep(.el-table) {
    font-size: 13px;
  }
  
  :deep(.el-descriptions) {
    font-size: 12px;
  }
  
  :deep(.el-descriptions__label) {
    width: 90px !important;
    min-width: 90px !important;
  }
  
  .action-buttons {
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }
  
  .action-buttons .el-button {
    margin-left: 0 !important;
    margin-bottom: 0;
  }
}

@media screen and (max-width: 480px) {
  .card-header {
    font-size: 14px;
  }
  
  .connection-detail {
    padding: 10px 12px;
  }
  
  :deep(.el-descriptions__label) {
    width: 70px !important;
    min-width: 70px !important;
    font-size: 11px;
  }
  
  :deep(.el-descriptions__content) {
    font-size: 11px;
  }
}
</style>

