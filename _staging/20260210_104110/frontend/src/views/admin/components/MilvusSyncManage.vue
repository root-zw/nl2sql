<template>
  <div class="milvus-sync">
    <!-- 全局同步配置区域 -->
    <el-card v-if="globalSettingsLoaded" class="global-settings-card">
      <div class="global-settings-header">
        <div>
          <div class="title">全局同步配置</div>
          <el-text size="small" type="info">
            {{ globalSettingsSourceLabel }}
          </el-text>
        </div>
        <div class="global-settings-actions">
          <el-tag :type="effectiveGlobalSettings.auto_sync_enabled ? 'success' : 'warning'" effect="dark">
            {{ effectiveGlobalSettings.auto_sync_enabled ? '自动同步开启' : '自动同步关闭' }}
          </el-tag>
          <el-tag type="info">
            {{ effectiveGlobalSettings.auto_sync_mode === 'auto' ? 'Auto 模式' : 'Manual 模式' }}
          </el-tag>
          <el-button text size="small" @click="loadGlobalSettings">
            刷新
          </el-button>
          <el-button type="primary" @click="openGlobalSettingsDialog()">
            编辑全局配置
          </el-button>
          <el-button type="success" plain @click="openGlobalSettingsDialog('all')">
            应用到所有连接
          </el-button>
        </div>
      </div>
      <el-descriptions :column="3" size="small" border class="global-settings-descriptions">
        <el-descriptions-item label="业务域">
          <el-tag :type="effectiveGlobalSettings.auto_sync_domains ? 'success' : 'info'">
            {{ effectiveGlobalSettings.auto_sync_domains ? '开启' : '关闭' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="数据表">
          <el-tag :type="effectiveGlobalSettings.auto_sync_tables ? 'success' : 'info'">
            {{ effectiveGlobalSettings.auto_sync_tables ? '开启' : '关闭' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="字段">
          <el-tag :type="effectiveGlobalSettings.auto_sync_fields ? 'success' : 'info'">
            {{ effectiveGlobalSettings.auto_sync_fields ? '开启' : '关闭' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="枚举">
          <el-tag :type="effectiveGlobalSettings.auto_sync_enums ? 'success' : 'info'">
            {{ effectiveGlobalSettings.auto_sync_enums ? '开启' : '关闭' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="Few-Shot">
          <el-tag :type="effectiveGlobalSettings.auto_sync_few_shot ? 'success' : 'info'">
            {{ effectiveGlobalSettings.auto_sync_few_shot ? '开启' : '关闭' }}
          </el-tag>
        </el-descriptions-item>
      </el-descriptions>
      <div class="global-settings-foot">
        <el-text size="small">
          最近一次批量应用：{{ formatShanghaiTime(globalSettings && globalSettings.created_at) || '尚未执行' }}
        </el-text>
        <el-text v-if="globalSettings && globalSettings.apply_scope === 'connections'" size="small" type="info">
          已覆盖 {{ globalSettings.applied_connection_count || ((globalSettings.applied_connection_ids && globalSettings.applied_connection_ids.length) || 0) }} 个连接
        </el-text>
      </div>
    </el-card>

    <!-- 同步管理内容 -->
    <el-card>
      <template #header>
        <div class="card-header">
          <span>🚀 Milvus向量库同步管理</span>
          <div class="header-right">
            <!-- 当前同步状态 -->
            <el-tag v-if="currentSyncStatus" :type="getSyncStatusTagType(currentSyncStatus.status)">
              {{ getSyncStatusText(currentSyncStatus.status) }}
            </el-tag>
          </div>
        </div>
      </template>

      <!-- 健康状态卡片 -->
      <el-row :gutter="16" class="health-cards">
        <el-col :xs="24" :sm="12" :md="12" :lg="12" :xl="12">
          <el-card class="health-card">
            <div class="health-item">
              <div class="health-label">Milvus状态</div>
              <el-tag :type="milvusHealthy ? 'success' : 'danger'" effect="dark">
                <el-icon style="margin-right: 4px;">
                  <component :is="milvusHealthy ? 'CircleCheck' : 'CircleClose'" />
                </el-icon>
                {{ healthMessage }}
              </el-tag>
            </div>
          </el-card>
        </el-col>

        <el-col :xs="24" :sm="12" :md="12" :lg="12" :xl="12">
          <el-card class="health-card">
            <div class="health-item">
              <div class="health-item-header">
                <div class="health-label">最后同步</div>
              </div>
              <div class="last-sync">
                <el-text size="small" :type="lastSyncTime ? 'success' : 'info'">
                  {{ lastSyncTime || '从未同步' }}
                </el-text>
              </div>
            </div>
          </el-card>
        </el-col>
      </el-row>

      <!-- 统计信息 -->
      <el-descriptions :column="4" border class="stats-descriptions">
        <el-descriptions-item label="业务域">
          <el-tag type="success">{{ stats.domain_count ?? 0 }} 个</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="数据表">
          <el-tag type="success">{{ stats.table_count ?? 0 }} 张</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="字段">
          <el-tag type="info">{{ stats.field_count ?? 0 }} 个</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="向量总数">
          <el-tag type="primary">{{ totalVectorCount }} 条</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="枚举值">
          <el-tag type="info">{{ stats.enum_count ?? 0 }} 条</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="Few-Shot样本">
          <el-tag type="info">{{ stats.few_shot_count ?? 0 }} 条</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="度量字段">
          <el-tag type="warning">{{ stats.measure_count ?? 0 }} 个</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="维度字段">
          <el-tag type="warning">{{ stats.dimension_count ?? 0 }} 个</el-tag>
        </el-descriptions-item>
      </el-descriptions>

      <el-divider />

      <!-- 同步操作区域 -->
      <div class="sync-actions">
        <el-row :gutter="16">
          <!-- 全量同步 -->
          <el-col :xs="24" :sm="12" :md="8" :lg="8" :xl="8">
            <el-card shadow="hover" class="action-card">
              <div class="action-content">
                <div class="action-info">
                  <h4>🔄 全量同步</h4>
                  <el-text size="small" type="info">
                    同步所有业务域、表、字段、枚举及Few-Shot数据到Milvus
                  </el-text>
                </div>
                <el-button
                  type="primary"
                  @click="triggerFullSync"
                  :loading="syncing"
                  style="width: 100%; margin-top: 12px;"
                >
                  {{ syncing ? '同步中...' : '执行全量同步' }}
                </el-button>
              </div>
            </el-card>
          </el-col>

          <!-- 刷新统计 -->
          <el-col :xs="24" :sm="12" :md="8" :lg="8" :xl="8">
            <el-card shadow="hover" class="action-card">
              <div class="action-content">
                <div class="action-info">
                  <h4>📊 刷新统计</h4>
                  <el-text size="small" type="info">
                    重新加载元数据统计信息
                  </el-text>
                </div>
                <el-button
                  type="info"
                  @click="loadStats"
                  style="width: 100%; margin-top: 12px;"
                >
                  刷新统计
                </el-button>
              </div>
            </el-card>
          </el-col>

          <!-- 健康检查 -->
          <el-col :xs="24" :sm="12" :md="8" :lg="8" :xl="8">
            <el-card shadow="hover" class="action-card">
              <div class="action-content">
                <div class="action-info">
                  <h4>🩺 健康检查</h4>
                  <el-text size="small" type="info">
                    检查 Milvus 服务连接状态
                  </el-text>
                </div>
                <el-button
                  type="warning"
                  @click="checkMilvusHealth"
                  style="width: 100%; margin-top: 12px;"
                >
                  检查健康状态
                </el-button>
              </div>
            </el-card>
          </el-col>
        </el-row>
      </div>

      <!-- 当前同步进度 -->
      <div v-if="currentSyncStatus && currentSyncStatus.status === 'running'" class="sync-progress">
        <el-divider>同步进度</el-divider>
        <div class="progress-content">
          <el-progress
            :percentage="currentSyncStatus.progress_percentage || 0"
            :status="currentSyncStatus.status === 'failed' ? 'exception' : 'success'"
            :stroke-width="12"
          />
          <div class="progress-info">
            <el-text type="primary">{{ currentSyncStatus.current_step || '准备中...' }}</el-text>
            <el-button
              v-if="currentSyncStatus.sync_id"
              type="danger"
              size="small"
              @click="cancelCurrentSync"
              :loading="cancelling"
            >
              取消同步
            </el-button>
          </div>
        </div>
      </div>

      <!-- 同步日志 -->
      <el-divider>同步日志</el-divider>

      <div class="sync-logs">
        <el-timeline v-if="syncLogs.length > 0">
          <el-timeline-item
            v-for="(log, index) in syncLogs"
            :key="index"
            :timestamp="log.timestamp"
            :type="log.type"
          >
            {{ log.message }}
          </el-timeline-item>
        </el-timeline>
        <el-empty v-else description="暂无同步日志" :image-size="80" />
      </div>
    </el-card>

    <!-- 全局同步配置对话框 -->
    <el-dialog
      v-model="globalSettingsDialogVisible"
      title="全局同步配置"
      width="90%"
      :style="{ maxWidth: '520px' }"
      :close-on-click-modal="false"
    >
      <el-form :model="globalSettingsForm" label-width="140px">
        <el-form-item label="自动同步总开关">
          <el-switch v-model="globalSettingsForm.auto_sync_enabled" />
        </el-form-item>
        <el-form-item label="自动同步模式">
          <el-radio-group v-model="globalSettingsForm.auto_sync_mode">
            <el-radio-button label="auto">自动</el-radio-button>
            <el-radio-button label="manual">手动</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="实体开关">
          <div class="global-entity-switches">
            <el-checkbox v-model="globalSettingsForm.auto_sync_domains">业务域</el-checkbox>
            <el-checkbox v-model="globalSettingsForm.auto_sync_tables">数据表</el-checkbox>
            <el-checkbox v-model="globalSettingsForm.auto_sync_fields">字段</el-checkbox>
            <el-checkbox v-model="globalSettingsForm.auto_sync_enums">枚举</el-checkbox>
            <el-checkbox v-model="globalSettingsForm.auto_sync_few_shot">Few-Shot</el-checkbox>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="globalSettingsDialogVisible = false">取消</el-button>
          <el-button
            type="primary"
            :loading="globalSettingsSaving"
            @click="submitGlobalSettings"
          >
            保存并应用
          </el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, reactive, onMounted, onUnmounted, watch } from 'vue'
import { milvusAPI, autoSyncAPI } from '@/api'
import { ElMessage, ElMessageBox } from 'element-plus'
import { CircleCheck, CircleClose, InfoFilled, CirclePlus, Edit, Delete } from '@element-plus/icons-vue'

// 状态
const milvusHealthy = ref(true)
const healthMessage = ref('正常运行')
const healthDetails = ref('')
const syncStatus = ref('unknown')
const lastSyncTime = ref(null)
const stats = ref({})
const syncing = ref(false)
const cancelling = ref(false)
const syncLogs = ref([])

// 全局同步配置常量（必须在使用前定义）
const DEFAULT_GLOBAL_SETTINGS = {
  auto_sync_enabled: true,
  auto_sync_mode: 'auto',
  auto_sync_domains: true,
  auto_sync_tables: true,
  auto_sync_fields: true,
  auto_sync_enums: true,
  auto_sync_few_shot: true
}

function buildGlobalSettingsSnapshot(source = {}) {
  return {
    ...DEFAULT_GLOBAL_SETTINGS,
    ...(source || {})
  }
}

// 全局同步配置状态
const globalSettingsLoaded = ref(false)
const globalDefaults = ref(null)
const globalSettings = ref(null)
const globalSettingsDialogVisible = ref(false)
const globalSettingsSaving = ref(false)
const globalApplyScope = ref('all')
const globalSelectedConnections = ref([])
const globalSettingsForm = reactive(buildGlobalSettingsSnapshot())

// 同步相关状态
const currentSyncStatus = ref(null)

// 对话框状态
const activeTab = ref('domain')

// 计算向量总数
const totalVectorCount = computed(() => {
  if (typeof stats.value.vector_total === 'number') {
    return stats.value.vector_total
  }
  const domain = stats.value.domain_count || 0
  const table = stats.value.table_count || 0
  return domain + table
})

const effectiveGlobalSettings = computed(() => {
  const defaults = buildGlobalSettingsSnapshot(globalDefaults.value || {})
  if (globalSettings.value) {
    return {
      ...defaults,
      ...globalSettings.value
    }
  }
  return defaults
})

const globalSettingsSourceLabel = computed(() => {
  if (globalSettings.value) {
    if (globalSettings.value.apply_scope === 'all') {
      return '已批量覆盖所有连接'
    }
    const count = globalSettings.value.applied_connection_count ?? (globalSettings.value.applied_connection_ids?.length ?? 0)
    return `已指定 ${count} 个连接`
  }
  return '使用 .env 默认值'
})



async function loadGlobalSettings() {
  try {
    const { data } = await milvusAPI.getGlobalSettings()
    globalDefaults.value = buildGlobalSettingsSnapshot(data?.defaults || {})
    globalSettings.value = data?.active || null
  } catch (error) {
    console.error('加载全局同步配置失败', error)
    globalDefaults.value = buildGlobalSettingsSnapshot()
    globalSettings.value = null
  } finally {
    globalSettingsLoaded.value = true
  }
}

// 加载同步统计（全量）
async function loadStats() {
  try {
    const { data } = await milvusAPI.getStatsAll()
    stats.value = data.stats || {}
    syncStatus.value = data.success ? 'success' : 'unknown'

    const savedTime = localStorage.getItem('milvusLastSyncTime')
    if (savedTime) {
      lastSyncTime.value = savedTime
    } else {
      lastSyncTime.value = null
    }
  } catch (error) {
    console.error('加载同步统计失败', error)
    stats.value = {}
  }
}

// 检查Milvus健康状态
async function checkMilvusHealth() {
  try {
    const { data } = await milvusAPI.checkHealth()
    milvusHealthy.value = data.healthy
    healthMessage.value = data.message || (data.healthy ? '正常运行' : '连接失败')

    if (!data.healthy && data.details) {
      const details = []
      if (!data.details.milvus_connected) details.push('Milvus未连接')
      if (!data.details.embedding_available) details.push('Embedding服务不可用')
      if (data.details.failure_count > 0) details.push(`失败${data.details.failure_count}次`)
      healthDetails.value = details.join('，')
    } else {
      healthDetails.value = ''
    }
  } catch (error) {
    milvusHealthy.value = false
    healthMessage.value = '检查失败'
    healthDetails.value = '无法连接到Milvus服务'
    console.error('检查Milvus健康状态失败', error)
  }
}

// 触发全量同步（所有元数据）
async function triggerFullSync() {
  try {
    await ElMessageBox.confirm(
      '此操作将把所有业务域、表、字段、枚举及Few-Shot数据全量写入 Milvus。确定继续？',
      '全量同步确认',
      { type: 'info', confirmButtonText: '确定同步' }
    )

    syncing.value = true
    addLog('开始全量同步所有元数据...', 'primary')

    const { data } = await milvusAPI.syncAll({
      sync_domains: true,
      sync_tables: true,
      sync_fields: true,
      sync_enums: true,
      sync_few_shot: true,
      recreate_collections: false
    })

    if (data.success) {
      ElMessage.success('全量同步完成')
      addLog(`同步完成！业务域: ${data.stats.domains}，表: ${data.stats.tables}，字段: ${data.stats.fields}，枚举: ${data.stats.enums}，Few-Shot: ${data.stats.few_shot}`, 'success')
      updateLastSyncTime()
      await loadStats()
    } else {
      ElMessage.error(data.message || '同步失败')
      addLog(`同步失败: ${data.message}`, 'danger')
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('同步失败: ' + (error.response?.data?.detail || error.message))
      addLog(`同步失败: ${error.message}`, 'danger')
    }
  } finally {
    syncing.value = false
  }
}

// 取消当前同步
async function cancelCurrentSync() {
  if (!currentSyncStatus.value || !currentSyncStatus.value.sync_id) {
    return
  }

  try {
    await ElMessageBox.confirm(
      '确定要取消当前同步任务吗？',
      '取消同步',
      { type: 'warning', confirmButtonText: '确定取消' }
    )

    cancelling.value = true

    const { data } = await autoSyncAPI.cancelSync(currentSyncStatus.value.sync_id)

    if (data.success) {
      ElMessage.success('同步已取消')
      addLog('同步任务已取消', 'warning')
      currentSyncStatus.value = null
    } else {
      ElMessage.error(data.message || '取消失败')
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('取消失败: ' + (error.response?.data?.detail || error.message))
    }
  } finally {
    cancelling.value = false
  }
}

function openGlobalSettingsDialog(scope = null) {
  const defaults = buildGlobalSettingsSnapshot(globalDefaults.value || {})
  const active = globalSettings.value ? buildGlobalSettingsSnapshot(globalSettings.value) : {}
  Object.assign(globalSettingsForm, defaults, active)
  const targetScope = scope || (globalSettings.value?.apply_scope || 'all')
  globalApplyScope.value = targetScope
  globalSelectedConnections.value = []
  globalSettingsDialogVisible.value = true
}

async function submitGlobalSettings() {
  const scope = globalApplyScope.value

  try {
    globalSettingsSaving.value = true
    const payload = {
      settings: { ...globalSettingsForm },
      apply_scope: scope
    }

    const { data } = await milvusAPI.updateGlobalSettings(payload)
    if (data.success) {
      ElMessage.success(`全局配置已应用（更新 ${data.updated_connections} 个连接）`)
      globalSettingsDialogVisible.value = false
      await loadGlobalSettings()
    } else {
      ElMessage.error(data.message || '全局配置更新失败')
    }
  } catch (error) {
    ElMessage.error('全局配置更新失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    globalSettingsSaving.value = false
  }
}

// 更新最后同步时间
function updateLastSyncTime() {
  const now = new Date().toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
  localStorage.setItem('milvusLastSyncTime', now)
  lastSyncTime.value = now
}

// 添加日志
function addLog(message, type = 'info') {
  // 将Element Plus不支持的type映射为支持的type
  const typeMapping = {
    'error': 'danger',
    'log': 'info',
    'debug': 'info'
  }

  const normalizedType = typeMapping[type] || type

  const entry = {
    timestamp: new Date().toLocaleString('zh-CN'),
    message,
    type: normalizedType
  }

  syncLogs.value.unshift(entry)

  if (syncLogs.value.length > 20) {
    syncLogs.value = syncLogs.value.slice(0, 20)
  }

  localStorage.setItem('milvusSyncLogs', JSON.stringify(syncLogs.value))
}

// 工具函数
function getDbIcon(type) {
  const icons = {
    sqlserver: '🗄️',
    mysql: '🐬',
    postgresql: '🐘'
  }
  return icons[type] || '💾'
}

function getEntityTypeLabel(type) {
  const labels = {
    domain: '业务域',
    table: '数据表',
    field: '字段',
    enum: '枚举值'
  }
  return labels[type] || type
}

function getOperationTagType(operation) {
  const types = {
    'INSERT': 'success',
    'UPDATE': 'warning',
    'DELETE': 'danger'
  }
  return types[operation] || 'info'
}

function getSyncTypeLabel(type) {
  const labels = {
    full: '全量同步',
    incremental: '增量同步',
    enums: '枚举值同步'
  }
  return labels[type] || type
}

function getSyncStatusTagType(status) {
  const types = {
    'pending': 'info',
    'running': 'warning',
    'completed': 'success',
    'failed': 'danger',
    'cancelled': 'info'
  }
  return types[status] || 'info'
}

function getSyncStatusText(status) {
  const texts = {
    'pending': '待执行',
    'running': '执行中',
    'completed': '已完成',
    'failed': '失败',
    'cancelled': '已取消'
  }
  return texts[status] || status
}

function getHealthStatus(score) {
  if (score >= 0.8) return 'success'
  if (score >= 0.6) return 'warning'
  return 'exception'
}


// 获取操作类型图标
function getOperationIcon(operation) {
  const icons = {
    'INSERT': CirclePlus,
    'UPDATE': Edit,
    'DELETE': Delete
  }
  return icons[operation] || InfoFilled
}

// 获取操作类型文本
function getOperationText(operation) {
  const texts = {
    'INSERT': '新增',
    'UPDATE': '更新',
    'DELETE': '删除'
  }
  return texts[operation] || operation
}

// 获取实体类型标签样式
function getEntityTypeTagType(entityType) {
  const types = {
    'domain': 'primary',
    'table': 'success',
    'field': 'warning',
    'enum': 'info'
  }
  return types[entityType] || 'info'
}

// 获取实体类型文本
function getEntityTypeText(entityType) {
  const texts = {
    'domain': '业务域',
    'table': '数据表',
    'field': '字段',
    'enum': '枚举值'
  }
  return texts[entityType] || entityType
}

// 获取变更摘要
function getChangeSummary(row) {
  try {
    const { operation, entity_type } = row
    const newData = normalizeChangeData(row.new_data)
    const oldData = normalizeChangeData(row.old_data)

    if (operation === 'DELETE') {
      const name = getDataDisplayName(oldData, entity_type)
      const description = oldData?.description || oldData?.business_description || ''
      return `删除了${getEntityTypeText(entity_type)}「${name}」${description ? `（${description}）` : ''}`
    }

    if (operation === 'INSERT') {
      const name = getDataDisplayName(newData, entity_type)
      const description = newData?.description || newData?.business_description || ''
      return `新增了${getEntityTypeText(entity_type)}「${name}」${description ? `（${description}）` : ''}`
    }

    if (operation === 'UPDATE') {
      const oldName = getDataDisplayName(oldData, entity_type)
      const newName = getDataDisplayName(newData, entity_type)

      if (oldName !== newName) {
        return `重命名${getEntityTypeText(entity_type)}: 「${oldName}」→「${newName}」`
      } else {
        // 分析具体变更了哪些字段
        const changedFields = getChangedFields(oldData, newData, entity_type)
        if (changedFields.length > 0) {
          return `更新了${getEntityTypeText(entity_type)}「${newName}」的${changedFields.join('、')}`
        } else {
          return `更新了${getEntityTypeText(entity_type)}「${newName}」`
        }
      }
    }

    return `${getOperationText(operation)}了${getEntityTypeText(entity_type)}`
  } catch (error) {
    return '变更信息解析失败'
  }
}

// 获取变更的字段
function getChangedFields(oldData, newData, entityType) {
  oldData = normalizeChangeData(oldData)
  newData = normalizeChangeData(newData)
  if (!oldData || !newData) return []

  const changes = []
  const fieldNames = {
    'domain': {
      'domain_name': '名称',
      'description': '描述',
      'keywords': '关键词'
    },
    'table': {
      'table_name': '表名',
      'table_display_name': '显示名称',
      'description': '描述',
      'business_description': '业务描述'
    },
    'field': {
      'field_name': '字段名',
      'field_display_name': '显示名称',
      'description': '描述',
      'data_type': '数据类型'
    },
    'enum': {
      'enum_value': '枚举值',
      'value': '值',
      'description': '描述',
      'meaning': '含义'
    }
  }

  const relevantFields = fieldNames[entityType] || { 'name': '名称', 'description': '描述' }

  Object.keys(relevantFields).forEach(field => {
    if (oldData[field] !== newData[field]) {
      changes.push(relevantFields[field])
    }
  })

  return changes
}

// 获取数据显示名称
function getDataDisplayName(data, entityType) {
  data = normalizeChangeData(data)
  if (!data) return '未知'

  switch (entityType) {
    case 'domain':
      return data.domain_name || data.name || '未知业务域'
    case 'table':
      return data.table_name || data.table_display_name || data.name || '未知数据表'
    case 'field':
      return data.field_name || data.field_display_name || data.name || '未知字段'
    case 'enum':
      return data.enum_value || data.value || data.name || '未知枚举值'
    default:
      return data.name || data.id || '未知项目'
  }
}

// 格式化变更数据
function formatChangeData(data, entityType) {
  data = normalizeChangeData(data)
  if (!data) return '无数据'

  // 根据实体类型选择重要的字段来显示
  const importantFields = {
    'domain': ['domain_name', 'name', 'description', 'keywords'],
    'table': ['table_name', 'table_display_name', 'description', 'business_description'],
    'field': ['field_name', 'field_display_name', 'description', 'data_type'],
    'enum': ['enum_value', 'value', 'description', 'meaning']
  }

  const fields = importantFields[entityType] || ['name', 'description']
  const result = {}

  fields.forEach(field => {
    if (data[field] !== undefined && data[field] !== null) {
      result[field] = data[field]
    }
  })

  if (Object.keys(result).length === 0) {
    const entries = Object.entries(data).slice(0, 5)
    entries.forEach(([key, value]) => {
      result[key] = value
    })
  }

  return Object.entries(result)
    .map(([key, value]) => `${key}: ${Array.isArray(value) ? value.join(', ') : value}`)
    .join('；')
}

function normalizeChangeData(data) {
  if (!data) return null
  if (typeof data === 'string') {
    try {
      return JSON.parse(data)
    } catch (error) {
      return { value: data }
    }
  }
  return data
}

// 获取实体显示名称
function getEntityDisplayName(row) {
  try {
    const { entity_type, new_data, old_data } = row
    const data = new_data || old_data || {}

    switch (entity_type) {
      case 'domain':
        return data.domain_name || data.name || '未知业务域'
      case 'table':
        return data.table_name || data.table_display_name || data.name || '未知数据表'
      case 'field':
        return data.field_name || data.field_display_name || data.name || '未知字段'
      case 'enum':
        return data.enum_value || data.value || data.name || '未知枚举值'
      default:
        return data.name || data.id || '未知项目'
    }
  } catch (error) {
    return '获取名称失败'
  }
}

// 格式化上海时区时间
function formatShanghaiTime(timeStr) {
  if (!timeStr) return '-'

  try {
    const date = new Date(timeStr)
    // 使用上海时区 (UTC+8)
    const shanghaiTime = new Date(date.getTime() + (8 * 60 * 60 * 1000) + (date.getTimezoneOffset() * 60 * 1000))

    return shanghaiTime.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    })
  } catch (error) {
    return timeStr
  }
}

// 获取相对时间
function getRelativeTime(timeStr) {
  if (!timeStr) return '-'

  try {
    const date = new Date(timeStr)
    const now = new Date()
    const diff = now - date

    const minutes = Math.floor(diff / (1000 * 60))
    const hours = Math.floor(diff / (1000 * 60 * 60))
    const days = Math.floor(diff / (1000 * 60 * 60 * 24))

    if (minutes < 1) return '刚刚'
    if (minutes < 60) return `${minutes}分钟前`
    if (hours < 24) return `${hours}小时前`
    if (days < 7) return `${days}天前`

    return formatShanghaiTime(timeStr)
  } catch (error) {
    return formatShanghaiTime(timeStr)
  }
}

// 检查是否有详细变更
function hasDetailedChanges(row) {
  return row.new_data || row.old_data
}

// 获取变更行的样式类名
function getChangeRowClass({ row }) {
  const operationClass = `change-row-${row.operation.toLowerCase()}`
  const typeClass = `change-row-${row.entity_type}`
  return `${operationClass} ${typeClass}`
}

watch(globalApplyScope, (value) => {
  if (value === 'all') {
    globalSelectedConnections.value = []
  }
})

// 生命周期
onMounted(() => {
  // 加载全局配置和统计
  loadGlobalSettings()
  loadStats()
  checkMilvusHealth()

  // 恢复日志
  const savedLogs = JSON.parse(localStorage.getItem('milvusSyncLogs') || '[]')
  syncLogs.value = savedLogs.map(log => ({
    ...log,
    type: log.type === 'error' ? 'danger' : log.type
  }))
})

onUnmounted(() => {
  // 清理资源
})
</script>

<style scoped>
.milvus-sync {
  padding: 0;
  width: 100%;
}

.milvus-sync .el-card {
  width: 100%;
}

/* 选择器卡片 */
.selector-card {
  margin-bottom: 20px;
  border-radius: 8px;
}

.selector-content {
  display: flex;
  gap: 16px;
  align-items: center;
  justify-content: space-between;
}

.connection-select {
  flex: 1;
  max-width: 400px;
}

.info-icon {
  color: var(--el-color-info);
  cursor: help;
}

.global-settings-card {
  margin-bottom: 20px;
  border-left: 4px solid var(--el-color-primary);
}

.global-settings-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
}

.global-settings-header .title {
  font-weight: 600;
  font-size: 16px;
  margin-bottom: 4px;
}

.global-settings-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  align-items: center;
  justify-content: flex-end;
}

.global-settings-descriptions {
  margin-bottom: 10px;
}

.global-settings-foot {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.global-entity-switches {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
}

.connection-config-alert {
  margin: 12px 0 20px 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 16px;
  font-weight: 600;
}

.header-right {
  display: flex;
  gap: 12px;
  align-items: center;
}

/* 健康状态卡片 */
.health-cards {
  margin-bottom: 20px;
}

.health-card {
  height: 80px;
}

.health-item {
  display: flex;
  flex-direction: column;
  justify-content: center;
  height: 100%;
}

.health-item-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.health-label {
  font-size: 12px;
  color: var(--el-text-color-regular);
}

.health-score {
  font-weight: bold;
  margin-left: 8px;
}

.last-sync {
  font-size: 12px;
}

.polling-indicator {
  display: flex;
  align-items: center;
  gap: 4px;
}

/* 统计信息 */
.stats-descriptions {
  margin-bottom: 20px;
}

/* 同步操作区域 */
.sync-actions {
  margin-top: 20px;
}

.action-card {
  height: 100%;
  min-height: 140px;
}

.action-content {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.action-info {
  flex: 1;
}

.action-info h4 {
  margin: 0 0 8px 0;
  font-size: 16px;
}

.no-pending-hint {
  margin-top: 8px;
  text-align: center;
  font-style: italic;
}

/* 同步进度 */
.sync-progress {
  margin: 20px 0;
}

.progress-content {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.progress-info {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

/* 同步日志 */
.sync-logs {
  margin-top: 16px;
  max-height: 400px;
  overflow-y: auto;
}

/* 对话框内容 */
.pending-changes-content {
  max-height: 60vh;
  overflow-y: auto;
  padding: 0;
}

.change-detail {
  font-size: 13px;
  line-height: 1.5;
}

.change-detail pre {
  background: #f5f5f5;
  padding: 8px;
  border-radius: 4px;
  margin: 4px 0;
  max-height: 100px;
  overflow-y: auto;
}

/* 实体信息样式 */
.entity-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.entity-name {
  font-weight: 600;
  color: var(--el-text-color-primary);
  font-size: 13px;
}

.entity-id {
  font-size: 11px;
  color: var(--el-text-color-placeholder);
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
}

/* 时间信息样式 */
.time-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.relative-time {
  font-weight: 600;
  color: var(--el-text-color-primary);
  font-size: 13px;
}

.absolute-time {
  font-size: 11px;
  color: var(--el-text-color-placeholder);
}

/* 表格行样式优化 */
:deep(.el-table .change-row-insert) {
  background-color: var(--el-color-success-light-9);
}

:deep(.el-table .change-row-update) {
  background-color: var(--el-color-warning-light-9);
}

:deep(.el-table .change-row-delete) {
  background-color: var(--el-color-danger-light-9);
}

:deep(.el-table .change-row-insert:hover) {
  background-color: var(--el-color-success-light-7);
}

:deep(.el-table .change-row-update:hover) {
  background-color: var(--el-color-warning-light-7);
}

:deep(.el-table .change-row-delete:hover) {
  background-color: var(--el-color-danger-light-7);
}

/* 优化表格单元格内边距 */
:deep(.el-table td) {
  padding: 12px 0;
}

/* 优化标签样式 */
:deep(.el-tag) {
  border-radius: 12px;
  font-weight: 500;
}

/* 优化折叠面板样式 */
:deep(.el-collapse) {
  border: none;
}

:deep(.el-collapse-item__header) {
  font-size: 12px;
  padding: 8px 12px;
  background-color: var(--el-fill-color-lighter);
  border-radius: 4px;
}

:deep(.el-collapse-item__content) {
  padding: 12px;
}

/* 优化数据展示区域 */
.data-display {
  background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
  border: 1px solid #dee2e6;
  border-radius: 8px;
  padding: 12px;
  margin-top: 8px;
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  font-size: 12px;
  max-height: 200px;
  overflow-y: auto;
  box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
}

.sync-history-content {
  max-height: 70vh;
  overflow-y: auto;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

/* 变更详情样式 */
.change-detail {
  max-width: 400px;
}

.change-type-badge {
  display: flex;
  align-items: center;
  margin-bottom: 4px;
}

.change-summary {
  line-height: 1.4;
}

.data-display {
  background: #f8f9fa;
  border: 1px solid #e9ecef;
  border-radius: 4px;
  padding: 8px;
  margin-top: 4px;
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  font-size: 12px;
  max-height: 200px;
  overflow-y: auto;
}

.detailed-changes {
  border-left: 3px solid #409eff;
  padding-left: 12px;
}

.new-data {
  border-left: 3px solid #67c23a;
  padding-left: 8px;
}

.old-data {
  border-left: 3px solid #e6a23c;
  padding-left: 8px;
}


::deep(.el-table) {
  width: 100%;
}

::deep(.el-table__body-wrapper) {
  width: 100%;
}

/* 响应式设计 */
@media screen and (max-width: 1400px) {
  .entity-info {
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }
}

@media screen and (max-width: 768px) {
  .card-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
    font-size: 15px;
  }
  
  .header-actions {
    width: 100%;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  
  .header-actions .el-button {
    flex: 1;
    min-width: 100px;
  }
  
  .toolbar {
    flex-direction: column;
    gap: 10px;
  }
  
  .toolbar .el-input,
  .toolbar .el-select {
    width: 100% !important;
    margin-bottom: 0;
  }
  
  :deep(.el-table) {
    font-size: 13px;
  }
  
  .change-detail {
    font-size: 12px;
  }
  
  /* 对话框 */
  :deep(.el-dialog) {
    width: 95% !important;
  }
}

@media screen and (max-width: 480px) {
  .card-header {
    font-size: 14px;
  }
  
  :deep(.el-table) {
    font-size: 12px;
  }
}
</style>
