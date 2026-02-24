<template>
  <div class="prompt-manage">
    <!-- 简介说明 -->
    <div class="intro-bar">
      <span class="intro-text">管理各场景的LLM提示词模板，支持在线编辑、版本控制和热更新</span>
    </div>

    <!-- 场景选择卡片 -->
    <el-row :gutter="16" class="scenario-cards">
      <el-col :xs="24" :sm="12" :md="8" :lg="6" v-for="scenario in scenarios" :key="scenario.scenario">
        <div 
          class="scenario-card" 
          :class="{ active: activeScenario === scenario.scenario }"
          @click="selectScenario(scenario)"
        >
          <div class="card-header">
            <span class="scenario-icon">{{ getScenarioIcon(scenario.scenario) }}</span>
            <span class="scenario-label">{{ scenario.label }}</span>
          </div>
          <p class="scenario-desc">{{ scenario.description }}</p>
          <div class="prompt-badges">
            <el-tag 
              v-for="prompt in scenario.prompts" 
              :key="prompt.prompt_type"
              :type="getPromptTagType(prompt)"
              size="small"
              class="prompt-badge"
            >
              {{ prompt.type_label }}
              <el-icon v-if="prompt.is_active" class="active-icon"><Check /></el-icon>
            </el-tag>
          </div>
        </div>
      </el-col>
    </el-row>

    <!-- 提示词编辑区域 -->
    <el-card v-if="activeScenario" class="editor-card">
      <template #header>
        <div class="editor-header">
          <div class="header-left">
            <span class="scenario-name">{{ currentScenarioLabel }}</span>
            <el-select 
              v-model="activePromptType" 
              size="small" 
              class="prompt-type-select"
              @change="loadPrompt"
            >
              <el-option 
                v-for="prompt in currentScenarioPrompts" 
                :key="prompt.prompt_type"
                :label="prompt.type_label"
                :value="prompt.prompt_type"
              />
            </el-select>
          </div>
          <div class="header-actions">
            <el-tooltip content="从文件恢复（将文件内容导入数据库）" placement="top">
              <el-button size="small" :icon="Download" @click="syncFromFile" :loading="syncing">
                从文件恢复
              </el-button>
            </el-tooltip>
            <el-button 
              type="primary" 
              size="small" 
              :icon="Check" 
              @click="savePrompt" 
              :loading="saving"
              :disabled="!hasChanges"
            >
              保存并生效
            </el-button>
          </div>
        </div>
      </template>

      <!-- 状态栏 -->
      <div class="status-bar" v-if="currentPrompt">
        <div class="status-item">
          <span class="label">来源：</span>
          <el-tag :type="currentPrompt.template_id ? 'success' : 'info'" size="small">
            {{ currentPrompt.template_id ? '数据库' : '文件' }}
          </el-tag>
        </div>
        <div class="status-item" v-if="currentPrompt.template_id">
          <span class="label">版本：</span>
          <span>v{{ currentPrompt.version }}</span>
        </div>
        <div class="status-item" v-if="currentPrompt.template_id">
          <span class="label">状态：</span>
          <el-switch 
            v-model="promptActive" 
            active-text="已激活" 
            inactive-text="未激活"
            @change="toggleActive"
            :loading="toggling"
          />
        </div>
        <div class="status-item" v-if="currentPrompt.updated_at">
          <span class="label">更新时间：</span>
          <span>{{ formatTime(currentPrompt.updated_at) }}</span>
        </div>
        <el-button 
          v-if="currentPrompt.template_id" 
          type="text" 
          size="small" 
          @click="showHistory = true"
        >
          查看历史
        </el-button>
      </div>

      <!-- 编辑器 -->
      <div class="editor-container">
        <div class="editor-tabs" v-if="currentPrompt?.file_content && currentPrompt?.template_id">
          <el-radio-group v-model="editorMode" size="small">
            <el-radio-button label="edit">编辑</el-radio-button>
            <el-radio-button label="compare">对比文件</el-radio-button>
          </el-radio-group>
        </div>
        
        <div v-if="editorMode === 'edit'" class="single-editor">
          <el-input
            v-model="editContent"
            type="textarea"
            :rows="25"
            placeholder="请输入提示词内容..."
            class="prompt-editor"
            @input="checkChanges"
          />
        </div>
        
        <div v-else class="compare-editor">
          <div class="compare-pane">
            <div class="pane-header">数据库版本</div>
            <el-input
              v-model="editContent"
              type="textarea"
              :rows="22"
              class="prompt-editor"
              @input="checkChanges"
            />
          </div>
          <div class="compare-pane file-pane">
            <div class="pane-header">文件版本</div>
            <el-input
              :model-value="currentPrompt?.file_content || ''"
              type="textarea"
              :rows="22"
              readonly
              class="prompt-editor readonly"
            />
          </div>
        </div>
      </div>
    </el-card>

    <!-- 历史记录对话框 -->
    <el-dialog 
      v-model="showHistory" 
      title="版本历史" 
      width="800px"
      :close-on-click-modal="false"
    >
      <el-table :data="historyList" v-loading="loadingHistory" max-height="400">
        <el-table-column prop="version" label="版本" width="80" />
        <el-table-column prop="change_reason" label="变更原因" show-overflow-tooltip />
        <el-table-column prop="changed_by_name" label="修改人" width="120" />
        <el-table-column label="时间" width="180">
          <template #default="{ row }">
            {{ formatTime(row.changed_at) }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150">
          <template #default="{ row }">
            <el-button size="small" type="text" @click="previewHistory(row)">查看</el-button>
            <el-button size="small" type="text" @click="rollbackTo(row)">回滚</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>

    <!-- 历史内容预览对话框 -->
    <el-dialog 
      v-model="showPreview" 
      :title="`版本 v${previewVersion} 内容`"
      width="900px"
    >
      <el-input
        :model-value="previewContent"
        type="textarea"
        :rows="20"
        readonly
        class="prompt-editor readonly"
      />
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Check, Download } from '@element-plus/icons-vue'
import request from '@/utils/request'

// 状态
const scenarios = ref([])
const activeScenario = ref('')
const activePromptType = ref('')
const currentPrompt = ref(null)
const editContent = ref('')
const originalContent = ref('')
const promptActive = ref(false)
const editorMode = ref('edit')

// 加载状态
const loading = ref(false)
const saving = ref(false)
const syncing = ref(false)
const toggling = ref(false)
const loadingHistory = ref(false)

// 历史相关
const showHistory = ref(false)
const historyList = ref([])
const showPreview = ref(false)
const previewContent = ref('')
const previewVersion = ref(0)

// 计算属性
const currentScenarioLabel = computed(() => {
  const s = scenarios.value.find(s => s.scenario === activeScenario.value)
  return s?.label || ''
})

const currentScenarioPrompts = computed(() => {
  const s = scenarios.value.find(s => s.scenario === activeScenario.value)
  return s?.prompts || []
})

const hasChanges = computed(() => {
  return editContent.value !== originalContent.value
})

// 场景图标
function getScenarioIcon(scenario) {
  const icons = {
    table_selector: '📊',
    nl2ir: '🔄',
    direct_sql: '📝',
    narrative: '💬',
    cot_planner: '🧠',
    vector_table_selector: '🔍'
  }
  return icons[scenario] || '📄'
}

// 提示词标签类型
function getPromptTagType(prompt) {
  if (prompt.is_active) return 'success'
  if (prompt.has_db_version) return 'warning'
  return 'info'
}

// 格式化时间
function formatTime(timeStr) {
  if (!timeStr) return ''
  const date = new Date(timeStr)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  })
}

// 加载场景列表
async function loadScenarios() {
  loading.value = true
  try {
    const res = await request.get('/admin/prompts/scenarios')
    scenarios.value = res.data || res
    
    // 默认选中第一个场景
    if (scenarios.value.length > 0 && !activeScenario.value) {
      selectScenario(scenarios.value[0])
    }
  } catch (error) {
    ElMessage.error('加载场景列表失败: ' + (error.message || '未知错误'))
  } finally {
    loading.value = false
  }
}

// 选择场景
function selectScenario(scenario) {
  activeScenario.value = scenario.scenario
  // 默认选中第一个提示词类型
  if (scenario.prompts && scenario.prompts.length > 0) {
    activePromptType.value = scenario.prompts[0].prompt_type
    loadPrompt()
  }
}

// 加载提示词内容
async function loadPrompt() {
  if (!activeScenario.value || !activePromptType.value) return
  
  loading.value = true
  try {
    const res = await request.get(`/admin/prompts/${activeScenario.value}/${activePromptType.value}`, {
      params: { include_file: true }
    })
    currentPrompt.value = res.data || res
    editContent.value = currentPrompt.value.content || ''
    originalContent.value = editContent.value
    promptActive.value = currentPrompt.value.is_active || false
    editorMode.value = 'edit'
  } catch (error) {
    ElMessage.error('加载提示词失败: ' + (error.message || '未知错误'))
    currentPrompt.value = null
    editContent.value = ''
  } finally {
    loading.value = false
  }
}

// 检查变更
function checkChanges() {
  // 空操作，hasChanges 会自动计算
}

// 保存提示词
async function savePrompt() {
  if (!hasChanges.value) return
  
  try {
    const reason = await ElMessageBox.prompt('请输入变更原因（可选）', '保存提示词', {
      confirmButtonText: '保存',
      cancelButtonText: '取消',
      inputPlaceholder: '如：优化XX逻辑',
      inputValue: ''
    }).catch(() => ({ value: '' }))
    
    saving.value = true
    const res = await request.put(`/admin/prompts/${activeScenario.value}/${activePromptType.value}`, {
      content: editContent.value,
      is_active: promptActive.value,
      change_reason: reason.value || null,
      sync_to_file: true  // 默认同步到文件
    })
    
    currentPrompt.value = res.data || res
    originalContent.value = editContent.value
    ElMessage.success('保存成功，已同步到文件并立即生效')
    
    // 刷新场景列表
    loadScenarios()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('保存失败: ' + (error.message || '未知错误'))
    }
  } finally {
    saving.value = false
  }
}

// 从文件同步
async function syncFromFile() {
  try {
    await ElMessageBox.confirm('将从文件同步提示词到数据库，是否继续？', '同步确认', {
      confirmButtonText: '同步',
      cancelButtonText: '取消',
      type: 'info'
    })
    
    syncing.value = true
    await request.post(`/admin/prompts/${activeScenario.value}/${activePromptType.value}/sync-from-file`)
    ElMessage.success('同步成功')
    loadPrompt()
    loadScenarios()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('同步失败: ' + (error.message || '未知错误'))
    }
  } finally {
    syncing.value = false
  }
}

// 切换激活状态
async function toggleActive(value) {
  toggling.value = true
  try {
    await request.patch(`/admin/prompts/${activeScenario.value}/${activePromptType.value}/toggle`, {
      is_active: value
    })
    ElMessage.success(value ? '已激活数据库版本' : '已禁用，将使用文件版本')
    loadScenarios()
  } catch (error) {
    promptActive.value = !value // 回滚
    ElMessage.error('切换状态失败: ' + (error.message || '未知错误'))
  } finally {
    toggling.value = false
  }
}

// 加载历史记录
async function loadHistory() {
  loadingHistory.value = true
  try {
    const res = await request.get(`/admin/prompts/${activeScenario.value}/${activePromptType.value}/history`)
    historyList.value = res.data || res
  } catch (error) {
    ElMessage.error('加载历史失败: ' + (error.message || '未知错误'))
  } finally {
    loadingHistory.value = false
  }
}

// 预览历史版本
function previewHistory(row) {
  previewContent.value = row.content
  previewVersion.value = row.version
  showPreview.value = true
}

// 回滚到指定版本
async function rollbackTo(row) {
  try {
    await ElMessageBox.confirm(`确定要回滚到版本 v${row.version} 吗？`, '回滚确认', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    })
    
    await request.post(`/admin/prompts/${activeScenario.value}/${activePromptType.value}/rollback`, {
      version: row.version
    })
    
    ElMessage.success('回滚成功')
    showHistory.value = false
    loadPrompt()
    loadScenarios()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('回滚失败: ' + (error.message || '未知错误'))
    }
  }
}

// 监听历史对话框
watch(showHistory, (val) => {
  if (val) {
    loadHistory()
  }
})

// 初始化
onMounted(() => {
  loadScenarios()
})
</script>

<style scoped>
.prompt-manage {
  padding: 0;
}

.intro-bar {
  margin-bottom: 16px;
  padding: 10px 14px;
  background: linear-gradient(135deg, #f0f7ff 0%, #f5f5f5 100%);
  border-radius: 8px;
  border-left: 3px solid #409eff;
}

.intro-text {
  color: #606266;
  font-size: 13px;
}

/* 场景卡片 */
.scenario-cards {
  margin-bottom: 24px;
}

.scenario-card {
  background: white;
  border-radius: 12px;
  padding: 16px;
  cursor: pointer;
  transition: all 0.3s;
  border: 2px solid transparent;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
  margin-bottom: 16px;
}

.scenario-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.scenario-card.active {
  border-color: #409eff;
  background: linear-gradient(135deg, #f0f7ff 0%, #fff 100%);
}

.card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.scenario-icon {
  font-size: 20px;
}

.scenario-label {
  font-size: 16px;
  font-weight: 600;
  color: #303133;
}

.scenario-desc {
  margin: 0 0 12px 0;
  font-size: 13px;
  color: #909399;
  line-height: 1.4;
}

.prompt-badges {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.prompt-badge {
  font-size: 11px;
}

.active-icon {
  margin-left: 4px;
  font-size: 12px;
}

/* 编辑器卡片 */
.editor-card {
  border-radius: 12px;
}

.editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 12px;
}

.scenario-name {
  font-size: 16px;
  font-weight: 600;
  color: #303133;
}

.prompt-type-select {
  width: 150px;
}

.header-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

/* 状态栏 */
.status-bar {
  display: flex;
  align-items: center;
  gap: 20px;
  padding: 12px 16px;
  background: #f5f7fa;
  border-radius: 8px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}

.status-item {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
}

.status-item .label {
  color: #909399;
}

/* 编辑器容器 */
.editor-container {
  min-height: 400px;
}

.editor-tabs {
  margin-bottom: 12px;
}

.single-editor,
.compare-editor {
  width: 100%;
}

.compare-editor {
  display: flex;
  gap: 16px;
}

.compare-pane {
  flex: 1;
  min-width: 0;
}

.pane-header {
  font-size: 13px;
  font-weight: 500;
  color: #606266;
  margin-bottom: 8px;
  padding: 4px 8px;
  background: #f0f2f5;
  border-radius: 4px;
}

.prompt-editor :deep(textarea) {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  font-size: 13px;
  line-height: 1.6;
}

.prompt-editor.readonly :deep(textarea) {
  background: #fafafa;
  color: #606266;
}

/* 响应式 */
@media (max-width: 768px) {
  .editor-header {
    flex-direction: column;
    align-items: flex-start;
  }
  
  .header-actions {
    width: 100%;
    justify-content: flex-end;
  }
  
  .compare-editor {
    flex-direction: column;
  }
  
  .status-bar {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
  }
}
</style>

