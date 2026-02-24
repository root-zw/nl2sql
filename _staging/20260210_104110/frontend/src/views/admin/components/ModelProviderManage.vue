<template>
  <div class="model-provider-manage">
    <el-card>
      <template #header>
        <div class="card-header">
          <div class="header-left">
            <span>🤖 模型供应商管理</span>
            <el-tag type="info" size="small" class="header-tag">
              支持多供应商、多凭证配置
            </el-tag>
          </div>
          <div class="header-actions">
            <el-button type="primary" :icon="Plus" @click="showAddProviderDialog">
              添加供应商
            </el-button>
            <el-button :icon="Refresh" @click="loadProviders" :loading="loading">
              刷新
            </el-button>
          </div>
        </div>
      </template>

      <!-- 已配置的供应商 -->
      <div class="section" v-if="configuredProviders.length > 0">
        <h4 class="section-title">已配置</h4>
        <div class="provider-grid">
          <div 
            v-for="provider in configuredProviders" 
            :key="provider.provider_id"
            class="provider-card configured"
          >
            <div class="provider-icon">
              <span class="icon-text">{{ getProviderIcon(provider.provider_name) }}</span>
            </div>
            <div class="provider-info">
              <div class="provider-name">{{ provider.display_name }}</div>
              <div class="provider-meta">
                <el-tag size="small" :type="provider.is_enabled ? 'success' : 'info'">
                  {{ provider.is_enabled ? '已启用' : '已禁用' }}
                </el-tag>
                <span class="meta-text">
                  {{ provider.credential_count }} 个凭证 · {{ provider.model_count }} 个模型
                </span>
              </div>
            </div>
            <div class="provider-actions">
              <el-button-group>
                <el-button size="small" @click="showCredentialDialog(provider)">
                  凭证管理
                </el-button>
                <el-button size="small" @click="showModelDialog(provider)">
                  模型管理
                </el-button>
                <el-dropdown trigger="click" @command="handleProviderCommand($event, provider)">
                  <el-button size="small" :icon="More" />
                  <template #dropdown>
                    <el-dropdown-menu>
                      <el-dropdown-item command="toggle">
                        {{ provider.is_enabled ? '禁用' : '启用' }}
                      </el-dropdown-item>
                      <el-dropdown-item command="delete" divided>
                        <span class="danger-text">删除</span>
                      </el-dropdown-item>
                    </el-dropdown-menu>
                  </template>
                </el-dropdown>
              </el-button-group>
            </div>
          </div>
        </div>
      </div>

      <!-- 空状态 -->
      <el-empty 
        v-if="configuredProviders.length === 0 && !loading"
        description="暂未配置任何模型供应商"
      >
        <el-button type="primary" @click="showAddProviderDialog">
          添加第一个供应商
        </el-button>
      </el-empty>

      <!-- 加载状态 -->
      <div v-if="loading" class="loading-container">
        <el-skeleton :rows="3" animated />
      </div>
    </el-card>

    <!-- 模型配置卡片 -->
    <el-card class="scenario-card">
      <template #header>
        <div class="card-header">
          <span>🎯 模型配置</span>
          <el-button text type="primary" @click="loadScenarioConfigs">
            <el-icon><Refresh /></el-icon>
          </el-button>
        </div>
      </template>
      
      <el-table :data="scenarioConfigs" stripe v-loading="scenarioLoading">
        <el-table-column prop="scenario" label="场景" width="150">
          <template #default="{ row }">
            <div class="scenario-info">
              <span class="scenario-name">{{ getScenarioLabel(row.scenario) }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="模型配置" min-width="300">
          <template #default="{ row }">
            <div v-if="row.model_id" class="model-config">
              <el-tag>{{ row.provider_display_name || row.provider_name }}</el-tag>
              <span class="model-name">{{ row.model_display_name || row.model_name }}</span>
            </div>
            <span v-else class="not-configured">未配置（使用默认）</span>
          </template>
        </el-table-column>
        <el-table-column label="参数" width="200">
          <template #default="{ row }">
            <div class="params-info" v-if="row.model_id">
              <span>T: {{ row.temperature ?? '-' }}</span>
              <span>Tokens: {{ row.max_tokens ?? '-' }}</span>
            </div>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{ row }">
            <el-button size="small" type="primary" link @click="showScenarioConfigDialog(row)">
              配置
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 添加供应商对话框 -->
    <el-dialog 
      v-model="addProviderVisible" 
      title="添加模型供应商"
      width="600px"
      destroy-on-close
    >
      <div class="preset-list">
        <div 
          v-for="preset in presets" 
          :key="preset.name"
          class="preset-item"
          :class="{ selected: selectedPreset?.name === preset.name }"
          @click="selectPreset(preset)"
        >
          <div class="preset-icon">{{ getProviderIcon(preset.name) }}</div>
          <div class="preset-info">
            <div class="preset-name">{{ preset.display_name }}</div>
            <div class="preset-desc">{{ preset.description }}</div>
          </div>
          <el-icon v-if="selectedPreset?.name === preset.name" class="check-icon">
            <Check />
          </el-icon>
        </div>
      </div>
      
      <el-form 
        v-if="selectedPreset" 
        :model="addProviderForm" 
        ref="addProviderFormRef"
        label-width="100px"
        class="add-provider-form"
      >
        <el-form-item label="API Key" prop="api_key" required>
          <el-input 
            v-model="addProviderForm.api_key" 
            type="password" 
            show-password
            placeholder="请输入 API Key"
          />
        </el-form-item>
        <el-form-item label="凭证名称" prop="credential_name">
          <el-input 
            v-model="addProviderForm.credential_name" 
            placeholder="默认凭证"
          />
        </el-form-item>
        <el-form-item label="API 地址" prop="base_url" v-if="!selectedPreset.default_base_url">
          <el-input 
            v-model="addProviderForm.base_url" 
            placeholder="请输入 API 基础地址"
          />
        </el-form-item>
      </el-form>
      
      <template #footer>
        <el-button @click="addProviderVisible = false">取消</el-button>
        <el-button type="primary" @click="addProvider" :loading="addLoading">
          添加
        </el-button>
      </template>
    </el-dialog>

    <!-- 凭证管理对话框 -->
    <el-dialog 
      v-model="credentialDialogVisible" 
      :title="`凭证管理 - ${currentProvider?.display_name}`"
      width="700px"
      destroy-on-close
    >
      <div class="credential-actions">
        <el-button type="primary" size="small" @click="showAddCredentialForm">
          添加凭证
        </el-button>
      </div>
      
      <el-table :data="credentials" stripe v-loading="credentialLoading">
        <el-table-column prop="credential_name" label="凭证名称" width="150" />
        <el-table-column prop="api_key_masked" label="API Key" width="180" />
        <el-table-column label="状态" width="150">
          <template #default="{ row }">
            <el-tag :type="row.is_default ? 'success' : 'info'" size="small">
              {{ row.is_default ? '默认' : '备用' }}
            </el-tag>
            <el-tag v-if="!row.is_active" type="danger" size="small" class="ml-2">
              已禁用
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="使用统计" width="150">
          <template #default="{ row }">
            <span>{{ row.total_requests }} 次</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150">
          <template #default="{ row }">
            <el-button 
              v-if="!row.is_default" 
              size="small" 
              link 
              @click="setDefaultCredential(row)"
            >
              设为默认
            </el-button>
            <el-button size="small" type="danger" link @click="deleteCredential(row)">
              删除
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <!-- 添加凭证表单 -->
      <el-form 
        v-if="showCredentialForm"
        :model="credentialForm"
        label-width="100px"
        class="credential-form"
      >
        <el-divider>添加新凭证</el-divider>
        <el-form-item label="凭证名称" required>
          <el-input v-model="credentialForm.credential_name" placeholder="请输入凭证名称" />
        </el-form-item>
        <el-form-item label="API Key" required>
          <el-input 
            v-model="credentialForm.api_key" 
            type="password" 
            show-password
            placeholder="请输入 API Key"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="addCredential" :loading="credentialLoading">
            添加
          </el-button>
          <el-button @click="showCredentialForm = false">取消</el-button>
        </el-form-item>
      </el-form>
    </el-dialog>

    <!-- 模型管理对话框 -->
    <el-dialog
      v-model="modelDialogVisible"
      :title="`模型管理 - ${currentProvider?.display_name}`"
      width="950px"
      destroy-on-close
      :close-on-click-modal="false"
    >
      <div class="model-actions">
        <el-input
          v-model="modelSearchKeyword"
          placeholder="搜索模型标识或名称"
          clearable
          style="width: 220px; margin-right: 12px;"
          :prefix-icon="Search"
        />
        <el-button
          type="success"
          size="small"
          @click="syncModelsFromAPI"
          :loading="syncModelLoading"
          v-if="currentProvider && currentProvider.provider_type === 'openai_compatible'"
        >
          <el-icon><Refresh /></el-icon>
          从API同步模型
        </el-button>
        <el-button type="primary" size="small" @click="showAddModelForm">
          手动添加模型
        </el-button>
      </div>

      <div class="model-dialog-content">
        <el-table :data="filteredProviderModels" stripe v-loading="modelLoading" max-height="400">
          <el-table-column prop="model_name" label="模型标识" width="200" />
          <el-table-column prop="display_name" label="显示名称" width="150" />
          <el-table-column prop="model_type" label="类型" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="getModelTypeTag(row.model_type)">
              {{ getModelTypeLabel(row.model_type) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="特性" width="200">
          <template #default="{ row }">
            <div class="model-features">
              <el-tag v-if="row.supports_function_calling" size="small" type="success">FC</el-tag>
              <el-tag v-if="row.supports_json_mode" size="small" type="info">JSON</el-tag>
              <el-tag v-if="row.supports_vision" size="small" type="warning">Vision</el-tag>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="80">
          <template #default="{ row }">
            <el-tag :type="row.is_enabled ? 'success' : 'info'" size="small">
              {{ row.is_enabled ? '启用' : '禁用' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150">
          <template #default="{ row }">
            <el-button size="small" link @click="toggleModelStatus(row)">
              {{ row.is_enabled ? '禁用' : '启用' }}
            </el-button>
            <el-button size="small" type="danger" link @click="deleteModel(row)">
              删除
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <!-- 添加模型表单 -->
      <el-form
        v-if="showModelForm"
        :model="modelForm"
        label-width="100px"
        class="model-form"
      >
        <el-divider>添加新模型</el-divider>
        <el-form-item label="模型标识" prop="model_name" required>
          <el-input v-model="modelForm.model_name" name="model_name" placeholder="如 qwen-plus、gpt-4o" />
        </el-form-item>
        <el-form-item label="显示名称" prop="display_name">
          <el-input v-model="modelForm.display_name" name="display_name" placeholder="可选，如 通义千问 Plus" />
        </el-form-item>
        <el-form-item label="模型类型" prop="model_type" required>
          <el-select v-model="modelForm.model_type" name="model_type" style="width: 100%">
            <el-option label="LLM (大语言模型)" value="llm" />
            <el-option label="Embedding (嵌入模型)" value="embedding" />
            <el-option label="Rerank (重排序模型)" value="rerank" />
          </el-select>
        </el-form-item>
        <el-form-item label="特性">
          <el-checkbox v-model="modelForm.supports_function_calling" name="supports_function_calling">Function Calling</el-checkbox>
          <el-checkbox v-model="modelForm.supports_json_mode" name="supports_json_mode">JSON Mode</el-checkbox>
          <el-checkbox v-model="modelForm.supports_vision" name="supports_vision">Vision</el-checkbox>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="addModel" :loading="modelLoading">
            添加
          </el-button>
          <el-button @click="showModelForm = false">取消</el-button>
        </el-form-item>
      </el-form>
      </div>
    </el-dialog>

    <!-- 模型配置对话框 -->
    <el-dialog
      v-model="scenarioConfigDialogVisible"
      :title="`配置 - ${getScenarioLabel(currentScenario?.scenario)}`"
      width="500px"
      destroy-on-close
    >
      <el-form :model="scenarioForm" label-width="100px">
        <el-form-item label="模型">
          <el-select
            v-model="scenarioForm.model_id"
            placeholder="选择模型（留空使用系统默认）"
            clearable
            filterable
            style="width: 100%"
          >
            <el-option-group 
              v-for="group in groupedAvailableModels" 
              :key="group.provider"
              :label="group.providerDisplayName"
            >
              <el-option 
                v-for="model in group.models" 
                :key="model.model_id"
                :label="model.display_name || model.model_name"
                :value="model.model_id"
              >
                <div class="model-option">
                  <span>{{ model.display_name || model.model_name }}</span>
                  <div class="model-features">
                    <el-tag v-if="model.features.includes('function_calling')" size="small" type="success">
                      FC
                    </el-tag>
                    <el-tag v-if="model.features.includes('json_mode')" size="small" type="info">
                      JSON
                    </el-tag>
                  </div>
                </div>
              </el-option>
            </el-option-group>
          </el-select>
        </el-form-item>
        <el-form-item label="Temperature">
          <el-input-number 
            v-model="scenarioForm.temperature" 
            :min="0" 
            :max="2" 
            :step="0.1"
            :precision="1"
          />
        </el-form-item>
        <el-form-item label="Max Tokens">
          <el-input-number 
            v-model="scenarioForm.max_tokens" 
            :min="1" 
            :max="128000"
            :step="256"
          />
        </el-form-item>
        <el-form-item label="超时(秒)">
          <el-input-number 
            v-model="scenarioForm.timeout_seconds" 
            :min="1" 
            :max="600"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="scenarioConfigDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveScenarioConfig" :loading="scenarioLoading">
          保存
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Refresh, More, Check, Search } from '@element-plus/icons-vue'
import { modelProviderAPI, scenarioConfigAPI } from '@/api'

// 状态
const loading = ref(false)
const addLoading = ref(false)
const providers = ref([])
const presets = ref([])
const credentials = ref([])
const scenarioConfigs = ref([])
const availableModels = ref([])

// 对话框
const addProviderVisible = ref(false)
const credentialDialogVisible = ref(false)
const modelDialogVisible = ref(false)
const scenarioConfigDialogVisible = ref(false)
const showCredentialForm = ref(false)
const showModelForm = ref(false)
const credentialLoading = ref(false)
const modelLoading = ref(false)
const syncModelLoading = ref(false)
const scenarioLoading = ref(false)
const providerModels = ref([])
const modelSearchKeyword = ref('')

// 当前选中
const selectedPreset = ref(null)
const currentProvider = ref(null)
const currentScenario = ref(null)

// 表单
const addProviderForm = ref({
  api_key: '',
  credential_name: '默认凭证',
  base_url: ''
})

const credentialForm = ref({
  credential_name: '',
  api_key: ''
})

const scenarioForm = ref({
  model_id: null,
  temperature: 0.0,
  max_tokens: 2048,
  timeout_seconds: 60
})

const modelForm = ref({
  model_name: '',
  display_name: '',
  model_type: 'llm',
  supports_function_calling: false,
  supports_json_mode: false,
  supports_vision: false
})

// 计算属性
const configuredProviders = computed(() => {
  return providers.value.filter(p => p.is_enabled || p.credential_count > 0)
})

const groupedAvailableModels = computed(() => {
  const groups = {}
  availableModels.value.forEach(model => {
    if (!groups[model.provider_name]) {
      groups[model.provider_name] = {
        provider: model.provider_name,
        providerDisplayName: model.provider_display_name,
        models: []
      }
    }
    groups[model.provider_name].models.push(model)
  })
  return Object.values(groups)
})

const filteredProviderModels = computed(() => {
  if (!modelSearchKeyword.value) {
    return providerModels.value
  }
  const keyword = modelSearchKeyword.value.toLowerCase()
  return providerModels.value.filter(model =>
    model.model_name?.toLowerCase().includes(keyword) ||
    model.display_name?.toLowerCase().includes(keyword)
  )
})

// 方法
const getProviderIcon = (name) => {
  const icons = {
    'dashscope': '🌐',
    'openai': '🤖',
    'deepseek': '🔍',
    'siliconflow': '💎',
    'ollama': '🦙',
    'azure': '☁️',
    'vllm': '⚡',
    'custom': '⚙️'
  }
  return icons[name] || '🔧'
}

const getScenarioLabel = (scenario) => {
  const labels = {
    'default': '默认 LLM',
    'table_selection': '表选择',
    'nl2ir': 'NL2IR 解析',
    'direct_sql': '直接 SQL',
    'narrative': '叙述生成',
    'vector_selector': '向量表选择',
    'embedding': '嵌入模型',
    'rerank': '重排模型'
  }
  return labels[scenario] || scenario
}

const loadProviders = async () => {
  loading.value = true
  try {
    const { data } = await modelProviderAPI.list(true)
    providers.value = data
  } catch (error) {
    ElMessage.error('加载供应商列表失败')
  } finally {
    loading.value = false
  }
}

const loadPresets = async () => {
  try {
    const { data } = await modelProviderAPI.getPresets()
    presets.value = data
  } catch (error) {
    console.error('加载预置供应商失败', error)
  }
}

const loadScenarioConfigs = async () => {
  scenarioLoading.value = true
  try {
    const { data } = await scenarioConfigAPI.list()
    scenarioConfigs.value = data
  } catch (error) {
    console.error('加载场景配置失败', error)
  } finally {
    scenarioLoading.value = false
  }
}

const loadAvailableModels = async () => {
  try {
    // 加载所有类型的模型（不传类型参数）
    const { data } = await modelProviderAPI.getAvailableModels()
    availableModels.value = data.models
  } catch (error) {
    console.error('加载可用模型失败', error)
  }
}

const showAddProviderDialog = () => {
  selectedPreset.value = null
  addProviderForm.value = { api_key: '', credential_name: '默认凭证', base_url: '' }
  addProviderVisible.value = true
  loadPresets()
}

const selectPreset = (preset) => {
  selectedPreset.value = preset
  addProviderForm.value.base_url = preset.default_base_url || ''
}

const addProvider = async () => {
  if (!selectedPreset.value) {
    ElMessage.warning('请选择一个供应商')
    return
  }
  if (!addProviderForm.value.api_key) {
    ElMessage.warning('请输入 API Key')
    return
  }
  
  addLoading.value = true
  try {
    await modelProviderAPI.addFromPreset(selectedPreset.value.name, {
      api_key: addProviderForm.value.api_key,
      credential_name: addProviderForm.value.credential_name || '默认凭证',
      base_url: addProviderForm.value.base_url || null
    })
    ElMessage.success('添加成功')
    addProviderVisible.value = false
    loadProviders()
    loadAvailableModels()
  } catch (error) {
    ElMessage.error(error.response?.data?.detail || '添加失败')
  } finally {
    addLoading.value = false
  }
}

const handleProviderCommand = async (command, provider) => {
  if (command === 'toggle') {
    try {
      await modelProviderAPI.update(provider.provider_id, { 
        is_enabled: !provider.is_enabled 
      })
      ElMessage.success(provider.is_enabled ? '已禁用' : '已启用')
      loadProviders()
    } catch (error) {
      ElMessage.error('操作失败')
    }
  } else if (command === 'delete') {
    try {
      await ElMessageBox.confirm(
        `确定要删除供应商 "${provider.display_name}" 吗？关联的凭证和模型也会被删除。`,
        '确认删除',
        { type: 'warning' }
      )
      await modelProviderAPI.delete(provider.provider_id)
      ElMessage.success('删除成功')
      loadProviders()
      loadAvailableModels()
    } catch (error) {
      if (error !== 'cancel') {
        ElMessage.error('删除失败')
      }
    }
  }
}

const showCredentialDialog = async (provider) => {
  currentProvider.value = provider
  credentialDialogVisible.value = true
  showCredentialForm.value = false
  await loadCredentials(provider.provider_id)
}

const loadCredentials = async (providerId) => {
  credentialLoading.value = true
  try {
    const { data } = await modelProviderAPI.listCredentials(providerId, true)
    credentials.value = data
  } catch (error) {
    ElMessage.error('加载凭证列表失败')
  } finally {
    credentialLoading.value = false
  }
}

const showAddCredentialForm = () => {
  credentialForm.value = { credential_name: '', api_key: '' }
  showCredentialForm.value = true
}

const addCredential = async () => {
  if (!credentialForm.value.credential_name || !credentialForm.value.api_key) {
    ElMessage.warning('请填写完整信息')
    return
  }
  
  credentialLoading.value = true
  try {
    await modelProviderAPI.createCredential(currentProvider.value.provider_id, credentialForm.value)
    ElMessage.success('添加成功')
    showCredentialForm.value = false
    await loadCredentials(currentProvider.value.provider_id)
    loadProviders()
  } catch (error) {
    ElMessage.error('添加失败')
  } finally {
    credentialLoading.value = false
  }
}

const setDefaultCredential = async (credential) => {
  try {
    await modelProviderAPI.setDefaultCredential(
      currentProvider.value.provider_id, 
      credential.credential_id
    )
    ElMessage.success('设置成功')
    await loadCredentials(currentProvider.value.provider_id)
  } catch (error) {
    ElMessage.error('设置失败')
  }
}

const deleteCredential = async (credential) => {
  try {
    await ElMessageBox.confirm('确定要删除此凭证吗？', '确认删除', { type: 'warning' })
    await modelProviderAPI.deleteCredential(
      currentProvider.value.provider_id, 
      credential.credential_id
    )
    ElMessage.success('删除成功')
    await loadCredentials(currentProvider.value.provider_id)
    loadProviders()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

const showModelDialog = async (provider) => {
  currentProvider.value = provider
  modelDialogVisible.value = true
  showModelForm.value = false
  modelSearchKeyword.value = ''
  await loadProviderModels(provider.provider_id)
}

const syncModelsFromAPI = async () => {
  if (!currentProvider.value) return
  
  try {
    await ElMessageBox.confirm(
      '将从供应商API获取可用模型列表并同步到数据库，是否继续？',
      '确认同步',
      { type: 'info' }
    )
  } catch {
    return
  }
  
  syncModelLoading.value = true
  try {
    const { data } = await modelProviderAPI.syncModels(
      currentProvider.value.provider_id,
      null, // 使用默认凭证
      null  // 同步所有类型
    )
    ElMessage.success(`成功同步 ${data.length} 个模型`)
    await loadProviderModels(currentProvider.value.provider_id)
    loadAvailableModels()
  } catch (error) {
    ElMessage.error(error.response?.data?.detail || '同步失败，请检查供应商配置和凭证')
  } finally {
    syncModelLoading.value = false
  }
}

const loadProviderModels = async (providerId) => {
  modelLoading.value = true
  try {
    const { data } = await modelProviderAPI.listModels(providerId, null, true)
    providerModels.value = data
  } catch (error) {
    ElMessage.error('加载模型列表失败')
  } finally {
    modelLoading.value = false
  }
}

const showAddModelForm = () => {
  modelForm.value = {
    model_name: '',
    display_name: '',
    model_type: 'llm',
    supports_function_calling: false,
    supports_json_mode: false,
    supports_vision: false
  }
  showModelForm.value = true
}

const addModel = async () => {
  if (!modelForm.value.model_name) {
    ElMessage.warning('请输入模型标识')
    return
  }
  
  modelLoading.value = true
  try {
    await modelProviderAPI.createModel(currentProvider.value.provider_id, modelForm.value)
    ElMessage.success('添加成功')
    showModelForm.value = false
    await loadProviderModels(currentProvider.value.provider_id)
    loadProviders()
    loadAvailableModels()
  } catch (error) {
    ElMessage.error(error.response?.data?.detail || '添加失败')
  } finally {
    modelLoading.value = false
  }
}

const toggleModelStatus = async (model) => {
  try {
    await modelProviderAPI.updateModel(currentProvider.value.provider_id, model.model_id, {
      is_enabled: !model.is_enabled
    })
    ElMessage.success(model.is_enabled ? '已禁用' : '已启用')
    await loadProviderModels(currentProvider.value.provider_id)
    loadAvailableModels()
  } catch (error) {
    ElMessage.error('操作失败')
  }
}

const deleteModel = async (model) => {
  try {
    await ElMessageBox.confirm(`确定要删除模型 "${model.display_name || model.model_name}" 吗？`, '确认删除', { type: 'warning' })
    await modelProviderAPI.deleteModel(currentProvider.value.provider_id, model.model_id)
    ElMessage.success('删除成功')
    await loadProviderModels(currentProvider.value.provider_id)
    loadProviders()
    loadAvailableModels()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

const getModelTypeLabel = (type) => {
  const labels = { 'llm': 'LLM', 'embedding': 'Embedding', 'rerank': 'Rerank' }
  return labels[type] || type
}

const getModelTypeTag = (type) => {
  const tags = { 'llm': 'primary', 'embedding': 'success', 'rerank': 'warning' }
  return tags[type] || 'info'
}

const showScenarioConfigDialog = (scenario) => {
  currentScenario.value = scenario
  scenarioForm.value = {
    model_id: scenario.model_id || null,
    temperature: scenario.temperature ?? 0.0,
    max_tokens: scenario.max_tokens ?? 2048,
    timeout_seconds: scenario.timeout_seconds ?? 60
  }
  scenarioConfigDialogVisible.value = true
  loadAvailableModels()
}

const saveScenarioConfig = async () => {
  scenarioLoading.value = true
  try {
    await scenarioConfigAPI.update(currentScenario.value.scenario, scenarioForm.value)
    ElMessage.success('保存成功')
    scenarioConfigDialogVisible.value = false
    loadScenarioConfigs()
  } catch (error) {
    ElMessage.error('保存失败')
  } finally {
    scenarioLoading.value = false
  }
}

// 初始化
onMounted(() => {
  loadProviders()
  loadScenarioConfigs()
  loadAvailableModels()
})
</script>

<style scoped>
.model-provider-manage {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 16px;
  font-weight: 600;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 8px;
}

.header-tag {
  font-weight: normal;
}

.header-actions {
  display: flex;
  gap: 8px;
}

.section-title {
  font-size: 14px;
  color: #606266;
  margin: 0 0 12px;
}

.provider-grid {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.provider-card {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 16px;
  border: 1px solid #e4e7ed;
  border-radius: 8px;
  background: #fff;
  transition: all 0.3s;
}

.provider-card:hover {
  border-color: #409eff;
  box-shadow: 0 2px 12px rgba(64, 158, 255, 0.1);
}

.provider-icon {
  width: 48px;
  height: 48px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-radius: 12px;
  flex-shrink: 0;
}

.icon-text {
  font-size: 24px;
}

.provider-info {
  flex: 1;
  min-width: 0;
}

.provider-name {
  font-size: 16px;
  font-weight: 600;
  color: #303133;
  margin-bottom: 4px;
}

.provider-meta {
  display: flex;
  align-items: center;
  gap: 8px;
}

.meta-text {
  font-size: 12px;
  color: #909399;
}

.provider-actions {
  flex-shrink: 0;
}

.loading-container {
  padding: 20px;
}

.scenario-card {
  margin-top: 0;
}

.section-desc {
  margin: 0 0 16px;
  color: #909399;
  font-size: 13px;
}

.scenario-info {
  display: flex;
  flex-direction: column;
}

.scenario-name {
  font-weight: 500;
}

.model-config {
  display: flex;
  align-items: center;
  gap: 8px;
}

.model-name {
  font-weight: 500;
}

.not-configured {
  color: #909399;
  font-style: italic;
}

.params-info {
  display: flex;
  gap: 12px;
  font-size: 12px;
  color: #606266;
}

/* 添加供应商对话框 */
.preset-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 300px;
  overflow-y: auto;
  margin-bottom: 16px;
}

.preset-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px;
  border: 1px solid #e4e7ed;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
}

.preset-item:hover {
  border-color: #409eff;
  background: #f5f7fa;
}

.preset-item.selected {
  border-color: #409eff;
  background: #ecf5ff;
}

.preset-icon {
  font-size: 24px;
  width: 40px;
  text-align: center;
}

.preset-info {
  flex: 1;
}

.preset-name {
  font-weight: 600;
  margin-bottom: 2px;
}

.preset-desc {
  font-size: 12px;
  color: #909399;
}

.check-icon {
  color: #409eff;
  font-size: 20px;
}

.add-provider-form {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid #e4e7ed;
}

/* 凭证管理 */
.credential-actions {
  margin-bottom: 16px;
}

.credential-form {
  margin-top: 16px;
}

/* 模型管理 */
.model-actions {
  margin-bottom: 16px;
  display: flex;
  align-items: center;
}

.model-dialog-content {
  max-height: 60vh;
  overflow-y: auto;
}

.model-form {
  margin-top: 16px;
}

.ml-2 {
  margin-left: 8px;
}

.danger-text {
  color: #f56c6c;
}

/* 模型选择器 */
.model-option {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.model-features {
  display: flex;
  gap: 4px;
}

@media screen and (max-width: 768px) {
  .card-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }

  .provider-card {
    flex-direction: column;
    align-items: flex-start;
  }

  .provider-actions {
    width: 100%;
  }

  .provider-actions .el-button-group {
    display: flex;
    width: 100%;
  }

  .provider-actions .el-button-group .el-button {
    flex: 1;
  }

  /* 模型管理对话框适配 */
  .model-actions {
    flex-wrap: wrap;
    gap: 8px;
  }

  .model-actions .el-input {
    width: 100% !important;
    margin-right: 0 !important;
    margin-bottom: 8px;
  }

  .model-dialog-content {
    max-height: 50vh;
  }

  /* 凭证管理对话框适配 */
  .credential-actions {
    margin-bottom: 12px;
  }

  /* 场景配置表格适配 */
  .scenario-card :deep(.el-table) {
    font-size: 12px;
  }

  .params-info {
    flex-direction: column;
    gap: 4px;
  }
}

/* 对话框宽度适配 */
@media screen and (max-width: 992px) {
  :deep(.el-dialog) {
    width: 90% !important;
    max-width: 90vw !important;
  }
}
</style>

