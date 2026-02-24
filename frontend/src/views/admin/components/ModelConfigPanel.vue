<template>
  <div class="model-config">
    <div class="config-header">
      <div>
        <h3>环境变量配置</h3>
        <p>以下是 .env 文件中的模型配置，作为模型供应商未配置时的兜底</p>
      </div>
      <el-button :icon="Refresh" @click="loadConfig" :loading="loading" text type="primary">
        刷新
      </el-button>
    </div>

    <!-- 默认模型配置 -->
    <el-row :gutter="20">
      <!-- 默认 LLM -->
      <el-col :xs="24" :sm="24" :md="8" :lg="8" :xl="8">
        <el-card class="config-card">
          <template #header>
            <div class="card-title">
              <span>🤖 默认 LLM</span>
              <el-tag type="success" size="small">{{ config?.llm?.provider || '-' }}</el-tag>
            </div>
          </template>

          <el-descriptions :column="1" border size="small">
            <el-descriptions-item label="模型">
              {{ config?.llm?.model || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Base URL">
              <span class="url-text">{{ config?.llm?.base_url || '-' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="API Key">
              {{ config?.llm?.api_key || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="超时 (s)">
              {{ config?.llm?.timeout ?? '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="重试次数">
              {{ config?.llm?.max_retries ?? '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Temperature">
              {{ config?.llm?.temperature ?? '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Max Tokens">
              {{ config?.llm?.max_tokens ?? '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Function Call">
              <el-tag :type="config?.llm?.use_tools ? 'success' : 'info'" size="small">
                {{ config?.llm?.use_tools ? '启用' : '关闭' }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="JSON Mode">
              <el-tag :type="config?.llm?.use_json_mode ? 'success' : 'info'" size="small">
                {{ config?.llm?.use_json_mode ? '启用' : '关闭' }}
              </el-tag>
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>

      <!-- Embedding -->
      <el-col :xs="24" :sm="24" :md="8" :lg="8" :xl="8">
        <el-card class="config-card">
          <template #header>
            <div class="card-title">
              <span>📊 Embedding</span>
            </div>
          </template>

          <el-descriptions :column="1" border size="small">
            <el-descriptions-item label="模型">
              {{ config?.embedding?.model || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Base URL">
              <span class="url-text">{{ config?.embedding?.base_url || '-' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="API Key">
              {{ config?.embedding?.api_key || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="超时 (s)">
              {{ config?.embedding?.timeout ?? '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="向量维度">
              {{ config?.embedding?.dimension ?? '-' }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>

      <!-- Reranker -->
      <el-col :xs="24" :sm="24" :md="8" :lg="8" :xl="8">
        <el-card class="config-card">
          <template #header>
            <div class="card-title">
              <span>🔄 Reranker</span>
              <el-tag v-if="config?.reranker?.endpoint && config?.reranker?.endpoint !== '(未配置)'" type="success" size="small">已配置</el-tag>
              <el-tag v-else type="warning" size="small">未配置</el-tag>
            </div>
          </template>

          <el-descriptions :column="1" border size="small">
            <el-descriptions-item label="模型">
              {{ config?.reranker?.model || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="Endpoint">
              <span class="url-text">{{ config?.reranker?.endpoint || '-' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="API Key">
              {{ config?.reranker?.api_key || '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="超时 (s)">
              {{ config?.reranker?.timeout ?? '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="权重">
              {{ config?.reranker?.weight ?? '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="最大并发">
              {{ config?.reranker?.max_concurrent ?? '-' }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
    </el-row>

    <!-- LLM 场景特定配置 -->
    <div class="scenario-section" v-if="config?.llm_scenarios">
      <h4 class="section-title">LLM 场景配置</h4>
      <p class="section-desc">不同业务场景可配置独立的模型，未配置的参数会使用默认 LLM 配置</p>
      
      <el-row :gutter="20">
        <el-col 
          v-for="(scenario, key) in config.llm_scenarios" 
          :key="key"
          :xs="24" :sm="12" :md="8" :lg="8" :xl="6"
        >
          <el-card class="scenario-card">
            <template #header>
              <div class="card-title">
                <span>{{ scenario.label }}</span>
                <el-tag v-if="scenario.enabled === false" type="danger" size="small">已禁用</el-tag>
                <el-tag v-else-if="isScenarioConfigured(scenario)" type="success" size="small">已配置</el-tag>
                <el-tag v-else type="info" size="small">使用默认</el-tag>
              </div>
            </template>

            <p class="scenario-desc">{{ scenario.description }}</p>

            <el-descriptions :column="1" border size="small">
              <el-descriptions-item label="模型">
                <span :class="{ 'default-value': scenario.model === '(使用默认)' }">
                  {{ scenario.model }}
                </span>
              </el-descriptions-item>
              <el-descriptions-item label="Base URL">
                <span class="url-text" :class="{ 'default-value': scenario.base_url === '(使用默认)' }">
                  {{ scenario.base_url }}
                </span>
              </el-descriptions-item>
              <el-descriptions-item label="API Key">
                <span :class="{ 'default-value': scenario.api_key === '(使用默认)' }">
                  {{ scenario.api_key }}
                </span>
              </el-descriptions-item>
              <el-descriptions-item label="Temperature">
                <span :class="{ 'default-value': scenario.temperature === '(使用默认)' }">
                  {{ scenario.temperature }}
                </span>
              </el-descriptions-item>
              <el-descriptions-item label="Max Tokens">
                <span :class="{ 'default-value': scenario.max_tokens === '(使用默认)' }">
                  {{ scenario.max_tokens }}
                </span>
              </el-descriptions-item>
              <el-descriptions-item label="超时 (s)">
                <span :class="{ 'default-value': scenario.timeout === '(使用默认)' }">
                  {{ scenario.timeout }}
                </span>
              </el-descriptions-item>
            </el-descriptions>
          </el-card>
        </el-col>
      </el-row>
    </div>

    <el-alert
      title="配置优先级说明"
      type="info"
      show-icon
      class="tip-alert"
      :closable="false"
    >
      <template #default>
        <div class="priority-info">
          <p><strong>1. 模型供应商配置（数据库）</strong> - 优先级最高，通过管理界面动态配置</p>
          <p><strong>2. 场景特定环境变量</strong> - 如 LLM_TABLE_SELECTION_MODEL、LLM_NL2IR_MODEL 等</p>
          <p><strong>3. 默认环境变量配置</strong> - 兜底配置，如 LLM_MODEL、EMBEDDING_MODEL 等</p>
          <p class="tip-note">如需修改环境变量配置，请在服务器的 .env 文件中更新后重启服务</p>
        </div>
      </template>
    </el-alert>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import axios from '@/utils/request'

const loading = ref(false)
const config = ref(null)

function isScenarioConfigured(scenario) {
  // 检查是否有任何非默认配置
  return (scenario.model && scenario.model !== '(使用默认)') ||
         (scenario.base_url && scenario.base_url !== '(使用默认)') ||
         (scenario.api_key && scenario.api_key !== '(使用默认)')
}

async function loadConfig() {
  loading.value = true
  try {
    const { data } = await axios.get('/admin/system-config/model')
    config.value = data
  } catch (error) {
    ElMessage.error('加载模型配置失败')
  } finally {
    loading.value = false
  }
}

onMounted(loadConfig)
</script>

<style scoped>
.model-config {
  padding: 10px 0 0;
  width: 100%;
}

.config-card,
.scenario-card {
  width: 100%;
  margin-bottom: 16px;
  height: calc(100% - 16px);
}

.config-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.config-header h3 {
  margin: 0;
  font-size: 18px;
}

.config-header p {
  margin: 4px 0 0;
  color: #909399;
  font-size: 13px;
}

.card-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  font-size: 14px;
}

.section-title {
  margin: 24px 0 8px;
  font-size: 16px;
  font-weight: 600;
  color: #303133;
}

.section-desc {
  margin: 0 0 16px;
  color: #909399;
  font-size: 13px;
}

.scenario-desc {
  margin: 0 0 12px;
  color: #606266;
  font-size: 12px;
  line-height: 1.5;
}

.url-text {
  word-break: break-all;
  font-size: 12px;
}

.default-value {
  color: #909399;
  font-style: italic;
}

.tip-alert {
  margin-top: 16px;
}

.priority-info p {
  margin: 4px 0;
  font-size: 13px;
  color: #606266;
}

.priority-info .tip-note {
  margin-top: 12px;
  color: #909399;
  font-style: italic;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  .config-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }
  
  .config-header h3 {
    font-size: 16px;
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
