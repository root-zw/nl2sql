<template>
  <div class="system-monitor">
    <el-row :gutter="20">
      <!-- 系统状态 -->
      <el-col :span="24">
        <el-card>
          <template #header>
            <div class="card-header">
              <span>📈 系统状态</span>
              <el-button
                text
                type="primary"
                size="small"
                :icon="Refresh"
                @click="refreshAll"
              >
                刷新
              </el-button>
            </div>
          </template>

          <el-row :gutter="16">
            <el-col :xs="12" :sm="6" :md="6" :lg="6" :xl="6">
              <el-statistic title="在线用户" :value="stats.online_users || 0">
                <template #suffix>
                  <el-text type="success">人</el-text>
                </template>
              </el-statistic>
            </el-col>

            <el-col :xs="12" :sm="6" :md="6" :lg="6" :xl="6">
              <el-statistic title="今日查询" :value="stats.today_queries || 0">
                <template #suffix>
                  <el-text type="primary">次</el-text>
                </template>
              </el-statistic>
            </el-col>

            <el-col :xs="12" :sm="6" :md="6" :lg="6" :xl="6">
              <el-statistic title="平均响应时间" :value="stats.avg_response_time || 0">
                <template #suffix>
                  <el-text>ms</el-text>
                </template>
              </el-statistic>
            </el-col>

            <el-col :xs="12" :sm="6" :md="6" :lg="6" :xl="6">
              <el-statistic title="成功率" :value="stats.success_rate || 0">
                <template #suffix>
                  <el-text type="success">%</el-text>
                </template>
              </el-statistic>
            </el-col>
          </el-row>
        </el-card>
      </el-col>

      <!-- 数据库连接状态 -->
      <el-col :xs="24" :sm="24" :md="12" :lg="12" :xl="12" class="monitor-card">
        <el-card>
          <template #header>
            <span>💾 数据库状态</span>
          </template>

          <el-table :data="dbStatus" stripe style="width: 100%" table-layout="auto">
            <el-table-column label="数据库" prop="name" min-width="120" />
            <el-table-column label="状态" min-width="100">
              <template #default="{ row }">
                <el-tag :type="row.status === 'healthy' ? 'success' : (row.status === 'disabled' ? 'info' : 'danger')">
                  {{ row.status === 'healthy' ? '✓ 正常' : (row.status === 'disabled' ? '○ 未启用' : '✗ 异常') }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="连接数" prop="connections" min-width="100" />
          </el-table>
        </el-card>
      </el-col>

      <!-- 模型信息 -->
      <el-col :xs="24" :sm="24" :md="12" :lg="12" :xl="12" class="monitor-card">
        <el-card>
          <template #header>
            <span>🤖 模型信息</span>
          </template>

          <el-descriptions :column="1" size="small" border>
            <el-descriptions-item label="LLM 提供商">
              {{ modelInfo.llm_provider || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="LLM 模型">
              {{ modelInfo.llm_model || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="LLM Base URL">
              {{ modelInfo.llm_base_url || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Embedding 模型">
              {{ modelInfo.embedding_model || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Embedding Base URL">
              {{ modelInfo.embedding_base_url || '-' }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>

    </el-row>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import axios from '@/utils/request'

const REFRESH_INTERVAL = Number(import.meta.env.VITE_MONITOR_REFRESH_INTERVAL ?? 30000)

const stats = ref({})
const dbStatus = ref([])
const modelInfo = computed(() => stats.value?.model_info || {})

// 加载系统统计
async function loadStats() {
  try {
    const { data } = await axios.get('/admin/monitor/stats')
    stats.value = data
  } catch (error) {
    console.error('加载系统统计失败', error)
    // 使用默认数据
    stats.value = {
      online_users: 1,
      today_queries: 0,
      avg_response_time: 0,
      success_rate: 100.0
    }
  }
}

// 加载数据库状态
async function loadDbStatus() {
  try {
    const { data } = await axios.get('/admin/monitor/health')
    const services = data.services || {}
    
    dbStatus.value = [
      {
        name: 'PostgreSQL',
        status: services.PostgreSQL?.status === 'healthy' ? 'healthy' : 'error',
        connections: services.PostgreSQL?.connections || 0
      },
      {
        name: 'Milvus',
        status: services.Milvus?.status === 'healthy' ? 'healthy' : (services.Milvus?.status === 'disabled' ? 'disabled' : 'error'),
        connections: services.Milvus?.connections || 0
      },
      {
        name: 'Redis',
        status: services.Redis?.status === 'healthy' ? 'healthy' : (services.Redis?.status === 'disabled' ? 'disabled' : 'error'),
        connections: services.Redis?.connections || 0
      }
    ]
  } catch (error) {
    console.error('加载数据库状态失败', error)
    // 使用默认数据
    dbStatus.value = [
      { name: 'PostgreSQL', status: 'error', connections: 0 },
      { name: 'Milvus', status: 'error', connections: 0 },
      { name: 'Redis', status: 'error', connections: 0 }
    ]
  }
}

function refreshAll() {
  loadStats()
  loadDbStatus()
}

let statsInterval = null

// 初始化
onMounted(() => {
  refreshAll()
  statsInterval = setInterval(refreshAll, REFRESH_INTERVAL)
})

onUnmounted(() => {
  if (statsInterval) {
    clearInterval(statsInterval)
    statsInterval = null
  }
})
</script>

<style scoped>
.system-monitor {
  padding: 0;
}

.monitor-card {
  margin-top: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

/* 统一卡片内边距 */
:deep(.el-card__body) {
  padding: 20px;
}

/* 表格紧凑显示 */
:deep(.el-table) {
  font-size: 14px;
}

:deep(.el-table th),
:deep(.el-table td) {
  padding: 12px 0;
}

/* 确保容器填满 */
.system-monitor {
  width: 100%;
}

.system-monitor .el-card {
  width: 100%;
}

::deep(.el-table) {
  width: 100%;
}

::deep(.el-table__body-wrapper) {
  width: 100%;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  .card-header {
    font-size: 15px;
  }
  
  .monitor-card {
    margin-top: 12px;
  }
  
  :deep(.el-descriptions) {
    font-size: 13px;
  }
  
  :deep(.el-descriptions__label) {
    width: 100px !important;
    min-width: 100px !important;
  }
  
  :deep(.el-table) {
    font-size: 13px;
  }
}

@media screen and (max-width: 480px) {
  .card-header {
    font-size: 14px;
  }
  
  :deep(.el-descriptions) {
    font-size: 12px;
  }
  
  :deep(.el-descriptions__label) {
    width: 80px !important;
    min-width: 80px !important;
  }
}
</style>

