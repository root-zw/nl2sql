<template>
  <div class="rule-list-container">
    <!-- 顶部操作栏 -->
    <div class="toolbar">
      <div class="toolbar-left">
        <el-button type="primary" :icon="Plus" @click="handleCreate">
          创建规则
        </el-button>
        <el-button :icon="Refresh" @click="loadRules">刷新</el-button>
      </div>
      
      <div class="toolbar-right">
        <el-input
          v-model="searchKeyword"
          placeholder="搜索规则名称"
          :prefix-icon="Search"
          clearable
          style="width: 100%; max-width: 200px; margin-right: 10px;"
          @input="handleSearch"
        />
        
        <el-select
          v-model="filterRuleType"
          placeholder="规则类型"
          clearable
          style="width: 100%; max-width: 150px; margin-right: 10px;"
          @change="loadRules"
        >
          <el-option label="全部类型" value="" />
          <el-option label="派生指标" value="derived_metric" />
          <el-option label="默认过滤" value="default_filter" />
          <el-option label="自定义规则" value="custom_instruction" />
        </el-select>
        
        <el-select
          v-model="filterStatus"
          placeholder="状态"
          clearable
          style="width: 100%; max-width: 120px;"
          @change="loadRules"
        >
          <el-option label="全部状态" value="" />
          <el-option label="启用" :value="true" />
          <el-option label="禁用" :value="false" />
        </el-select>
      </div>
    </div>

    <!-- 规则列表 -->
    <el-table
      v-loading="loading"
      :data="filteredRules"
      style="width: 100%"
      :height="tableHeight"
      table-layout="auto"
    >
      <el-table-column prop="rule_name" label="规则名称" min-width="180">
        <template #default="{ row }">
          <div class="rule-name-cell">
            <el-text type="primary" style="font-weight: 500;">{{ row.rule_name }}</el-text>
            <el-tag
              v-if="row.scope === 'domain'"
              size="small"
              type="warning"
              style="margin-left: 8px;"
            >
              域级
            </el-tag>
          </div>
        </template>
      </el-table-column>
      
      <el-table-column prop="rule_type" label="规则类型" min-width="140">
        <template #default="{ row }">
          <el-tag :type="getRuleTypeColor(row.rule_type)" size="small">
            {{ getRuleTypeLabel(row.rule_type) }}
          </el-tag>
        </template>
      </el-table-column>
      
      
      <el-table-column prop="description" label="描述" min-width="150" show-overflow-tooltip />
      
      <el-table-column label="应用业务域" min-width="150">
        <template #default="{ row }">
          <template v-if="row.scope === 'domain' && row.domain_ids && row.domain_ids.length > 0">
            <el-tag
              v-for="domainId in row.domain_ids.slice(0, 2)"
              :key="domainId"
              size="small"
              type="info"
              style="margin: 2px;"
            >
              {{ getDomainName(domainId) }}
            </el-tag>
            <el-tag
              v-if="row.domain_ids.length > 2"
              size="small"
              type="info"
              style="margin: 2px;"
            >
              +{{ row.domain_ids.length - 2 }}
            </el-tag>
          </template>
          <el-text v-else-if="row.scope === 'global'" type="info" size="small">
            全局
          </el-text>
          <el-text v-else type="info" size="small">
            -
          </el-text>
        </template>
      </el-table-column>
      
      <el-table-column prop="priority" label="优先级" min-width="80" align="center" />
      
      <el-table-column prop="is_active" label="状态" min-width="80" align="center">
        <template #default="{ row }">
          <el-switch
            v-model="row.is_active"
            @change="handleToggleStatus(row)"
          />
        </template>
      </el-table-column>
      
      <el-table-column prop="created_at" label="创建时间" min-width="160">
        <template #default="{ row }">
          {{ formatDateTime(row.created_at) }}
        </template>
      </el-table-column>
      
      <el-table-column label="操作" min-width="160" fixed="right" align="center">
        <template #default="{ row }">
          <div class="action-buttons">
            <el-button type="primary" size="small" link @click="handleView(row)">查看</el-button>
            <el-button type="primary" size="small" link @click="handleEdit(row)">编辑</el-button>
            <el-button type="danger" size="small" link @click="handleDelete(row)">删除</el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>

    <!-- 规则表单对话框 -->
    <el-dialog
      v-model="dialogVisible"
      :title="dialogTitle"
      width="90%"
      :style="{ maxWidth: '800px' }"
      :close-on-click-modal="false"
      @close="handleDialogClose"
    >
      <!-- 表单主体：添加可滚动容器，避免内容过长无法查看 -->
      <div class="rule-dialog-body">
        <el-form
          ref="formRef"
          :model="formData"
          :rules="formRules"
          label-width="120px"
        >
        <el-form-item label="规则类型" prop="rule_type">
          <el-select
            v-model="formData.rule_type"
            placeholder="请选择规则类型"
            :disabled="dialogMode === 'edit' || dialogMode === 'view'"
            style="width: 100%"
          >
            <el-option label="派生指标" value="derived_metric" />
            <el-option label="默认过滤" value="default_filter" />
            <el-option label="自定义规则" value="custom_instruction" />
          </el-select>
        </el-form-item>
        
        <el-form-item label="规则名称" prop="rule_name">
          <el-input
            v-model="formData.rule_name"
            placeholder="请输入规则名称"
            :disabled="dialogMode === 'view'"
          />
        </el-form-item>
        
        <el-form-item label="描述">
          <el-input
            v-model="formData.description"
            type="textarea"
            :rows="2"
            placeholder="请输入规则描述"
            :disabled="dialogMode === 'view'"
          />
        </el-form-item>
        
        <el-form-item label="作用范围" prop="scope">
          <el-radio-group v-model="formData.scope" :disabled="dialogMode === 'view'">
            <el-radio value="global">全局</el-radio>
            <el-radio value="domain">业务域</el-radio>
          </el-radio-group>
        </el-form-item>
        
        <el-form-item v-if="formData.scope === 'domain'" label="业务域" prop="domain_ids">
          <el-select
            v-model="formData.domain_ids"
            placeholder="请选择一个或多个业务域"
            :disabled="dialogMode === 'view'"
            style="width: 100%"
            multiple
            filterable
            collapse-tags
            collapse-tags-tooltip
            :max-collapse-tags="2"
          >
            <el-option
              v-for="domain in domains"
              :key="domain.domain_id"
              :label="domain.domain_name || domain.name || '未命名业务域'"
              :value="domain.domain_id"
            >
              <span>{{ domain.domain_name || domain.name || '未命名业务域' }}</span>
              <span v-if="domain.table_count" style="color: #999; font-size: 12px; margin-left: 8px;">
                ({{ domain.table_count }}张表)
              </span>
            </el-option>
            <template #empty>
              <div style="padding: 20px; text-align: center; color: #999;">
                暂无业务域，请先创建业务域
              </div>
            </template>
          </el-select>
          <el-text type="info" size="small" style="display: block; margin-top: 5px;">
            可选择多个业务域，规则将应用于所选的所有业务域
          </el-text>
          <el-text v-if="domains.length === 0" type="warning" size="small" style="display: block; margin-top: 5px;">
            当前没有可用的业务域，请先在业务域管理中创建
          </el-text>
        </el-form-item>
        
        <el-form-item label="优先级">
          <el-input-number
            v-model="formData.priority"
            :min="0"
            :max="100"
            :disabled="dialogMode === 'view'"
          />
          <el-text type="info" size="small" style="margin-left: 10px;">
            数值越大优先级越高
          </el-text>
        </el-form-item>
        
        <!-- 规则定义（根据类型动态显示） -->
        <el-divider content-position="left">规则配置</el-divider>
        
        <!-- 派生指标配置 -->
        <template v-if="formData.rule_type === 'derived_metric'">
          <DerivedMetricForm
            v-model="formData.rule_definition"
            :disabled="dialogMode === 'view'"
            :fields="fields"
            :tables="tables"
          />
        </template>
        
        <!-- 默认过滤配置 -->
        <template v-else-if="formData.rule_type === 'default_filter'">
          <DefaultFilterForm
            v-model="formData.rule_definition"
            :disabled="dialogMode === 'view'"
            :tables="tables"
            :fields="fields"
          />
        </template>
        
        <!-- 自定义规则配置 -->
        <template v-else-if="formData.rule_type === 'custom_instruction'">
          <CustomInstructionForm
            v-model="formData.rule_definition"
            :disabled="dialogMode === 'view'"
          />
        </template>
        
          <el-alert
            v-else
            type="info"
            :closable="false"
            show-icon
          >
            请先选择规则类型
          </el-alert>
        </el-form>
      </div>
      
      <template #footer v-if="dialogMode !== 'view'">
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="handleSave">
          保存
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Refresh, Search, View, Edit, Delete } from '@element-plus/icons-vue'
import { ruleAPI, domainAPI, tableAPI, fieldAPI } from '@/api'
import { useMetadataLoader } from '@/composables/useMetadataLoader'
import DerivedMetricForm from './rule-forms/DerivedMetricForm.vue'
import DefaultFilterForm from './rule-forms/DefaultFilterForm.vue'
import CustomInstructionForm from './rule-forms/CustomInstructionForm.vue'

// 规则不再绑定数据源，通过 scope + domain_ids 控制作用范围
const props = defineProps({
  // 保留 connectionId 仅为向后兼容，实际不再使用
})

// 使用统一的元数据加载器
const {
  domains,
  tables,
  fields,
  loading: metadataLoading,
  loadAll: loadAllMetadata,
  getDomainDisplayName,
  getTableDisplayName,
  getFieldDisplayName
} = useMetadataLoader()

// 数据
const loading = ref(false)
const saving = ref(false)
const rules = ref([])

// 筛选
const searchKeyword = ref('')
const filterRuleType = ref('')
const filterStatus = ref('')

// 对话框
const dialogVisible = ref(false)
const dialogMode = ref('create') // 'create' | 'edit' | 'view'
const formRef = ref(null)
const formData = ref({
  rule_type: '',
  rule_name: '',
  description: '',
  scope: 'global',
  domain_id: null,  // 保留用于兼容
  domain_ids: [],   // 支持多选
  priority: 50,
  rule_definition: {}
})

const formRules = {
  rule_type: [{ required: true, message: '请选择规则类型', trigger: 'change' }],
  rule_name: [{ required: true, message: '请输入规则名称', trigger: 'blur' }],
  scope: [{ required: true, message: '请选择作用范围', trigger: 'change' }],
  domain_ids: [
    { 
      required: true, 
      message: '请至少选择一个业务域', 
      trigger: 'change',
      validator: (rule, value, callback) => {
        if (!value || value.length === 0) {
          callback(new Error('请至少选择一个业务域'))
        } else {
          callback()
        }
      }
    }
  ]
}

// 计算属性
const dialogTitle = computed(() => {
  const titles = {
    create: '创建规则',
    edit: '编辑规则',
    view: '查看规则'
  }
  return titles[dialogMode.value] || '规则管理'
})

const filteredRules = computed(() => {
  let result = rules.value

  // 搜索过滤
  if (searchKeyword.value) {
    const keyword = searchKeyword.value.toLowerCase()
    result = result.filter(rule => 
      rule.rule_name.toLowerCase().includes(keyword) ||
      (rule.description && rule.description.toLowerCase().includes(keyword))
    )
  }

  return result
})

const tableHeight = computed(() => {
  // 对话框高度约 80vh，最大 800px，减去头部、工具栏、padding等，约 220px
  const dialogHeight = Math.min(window.innerHeight * 0.8, 800)
  return dialogHeight - 220
})

// 方法
const loadRules = async () => {
  try {
    loading.value = true
    const params = {}
    
    // 加载所有规则（规则不再绑定数据源）
    if (filterRuleType.value) {
      params.rule_type = filterRuleType.value
    }
    
    if (filterStatus.value !== '') {
      params.is_active = filterStatus.value
    }
    
    const response = await ruleAPI.list(params)
    // 后端返回的是数组，直接使用
    rules.value = Array.isArray(response.data) ? response.data : []
  } catch (error) {
    console.error('加载规则列表失败:', error)
    ElMessage.error(error.response?.data?.detail || '加载规则列表失败')
  } finally {
    loading.value = false
  }
}

// 加载所有元数据（规则不再绑定数据源，需要显示所有业务域、表、字段供选择）
const loadMetadata = async () => {
  try {
    // 并行加载所有业务域、表、字段（不按数据源筛选）
    const [domainsRes, tablesRes, fieldsRes] = await Promise.all([
      domainAPI.list({ is_active: true }),
      tableAPI.list({ is_included: true }),
      fieldAPI.list({ is_active: true })
    ])

    // 业务域
    const allDomains = domainsRes?.data?.data || domainsRes?.data || []
    domains.value = Array.isArray(allDomains) ? allDomains : []

    // 表（用于派生指标、默认过滤的表选择）
    const allTables = tablesRes?.data?.data || tablesRes?.data || []
    tables.value = Array.isArray(allTables) ? allTables : []

    // 字段（用于派生指标的字段依赖选择）
    const allFields = fieldsRes?.data?.data || fieldsRes?.data || []
    fields.value = Array.isArray(allFields) ? allFields : []

    console.log(`[RuleList] 元数据加载完成: ${domains.value.length} 个业务域, ${tables.value.length} 张表, ${fields.value.length} 个字段`)
  } catch (error) {
    console.error('[RuleList] 加载元数据失败:', error)
    domains.value = []
    tables.value = []
    fields.value = []
  }
}

const handleSearch = () => {
  // 搜索在computed中实现，这里只是触发
}

const handleCreate = () => {
  dialogMode.value = 'create'
  formData.value = {
    rule_type: '',
    rule_name: '',
    description: '',
    scope: 'global',
    domain_id: null,
    domain_ids: [],
    priority: 50,
    rule_definition: {}
  }
  dialogVisible.value = true
}

const handleView = (row) => {
  dialogMode.value = 'view'
  // 🔧 使用深拷贝，避免引用共享
  formData.value = JSON.parse(JSON.stringify({
    ...row,
    // 确保 domain_ids 是数组
    domain_ids: row.domain_ids || (row.domain_id ? [row.domain_id] : [])
  }))
  dialogVisible.value = true
}

const handleEdit = (row) => {
  dialogMode.value = 'edit'
  // 🔧 使用深拷贝，避免引用共享
  formData.value = JSON.parse(JSON.stringify({
    ...row,
    // 确保 domain_ids 是数组
    domain_ids: row.domain_ids || (row.domain_id ? [row.domain_id] : [])
  }))
  dialogVisible.value = true
}

const handleDelete = async (row) => {
  try {
    await ElMessageBox.confirm(
      `确定要删除规则"${row.rule_name}"吗？此操作不可恢复。`,
      '删除确认',
      {
        type: 'warning',
        confirmButtonText: '确定删除',
        cancelButtonText: '取消'
      }
    )
    
    await ruleAPI.delete(row.rule_id)
    ElMessage.success('删除成功')
    await loadRules()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('删除规则失败:', error)
      ElMessage.error(error.response?.data?.detail || '删除规则失败')
    }
  }
}

const handleToggleStatus = async (row) => {
  try {
    await ruleAPI.update(row.rule_id, {
      is_active: row.is_active
    })
    ElMessage.success(row.is_active ? '规则已启用' : '规则已禁用')
  } catch (error) {
    console.error('更新规则状态失败:', error)
    ElMessage.error(error.response?.data?.detail || '更新规则状态失败')
    // 恢复原状态
    row.is_active = !row.is_active
  }
}

// 清理 rule_definition，确保所有 ID 字段都是字符串而非对象
const cleanRuleDefinition = (ruleType, ruleDef) => {
  if (!ruleDef || typeof ruleDef !== 'object') return {}
  
  const cleaned = { ...ruleDef }
  
  // 根据规则类型清理特定字段
  if (ruleType === 'default_filter') {
    // 默认过滤：清理 table_id
    if (cleaned.table_id && typeof cleaned.table_id === 'object') {
      cleaned.table_id = cleaned.table_id.table_id || ''
    }
  } else if (ruleType === 'derived_metric') {
    // 派生指标：清理 field_dependencies 中的 field_id
    if (Array.isArray(cleaned.field_dependencies)) {
      cleaned.field_dependencies = cleaned.field_dependencies.map(dep => ({
        ...dep,
        field_id: typeof dep.field_id === 'object' ? (dep.field_id?.field_id || '') : dep.field_id
      }))
    }
  }
  
  return cleaned
}

const handleSave = async () => {
  try {
    await formRef.value?.validate()
    
    saving.value = true
    
    console.log('🔍 [保存前] formData.rule_definition:', JSON.stringify(formData.value.rule_definition, null, 2))
    
    // 准备提交数据
    const cleanedRuleDef = cleanRuleDefinition(formData.value.rule_type, formData.value.rule_definition)
    console.log('✅ [清理后] cleanedRuleDef:', JSON.stringify(cleanedRuleDef, null, 2))
    
    const data = {
      // 规则不再绑定数据源，通过 scope + domain_ids 控制作用范围
      rule_type: formData.value.rule_type,
      rule_name: formData.value.rule_name,
      description: formData.value.description || '',
      scope: formData.value.scope,
      priority: formData.value.priority,
      rule_definition: cleanedRuleDef
    }
    
    console.log('📤 [最终提交] data:', JSON.stringify(data, null, 2))
    
    // 处理业务域
    if (formData.value.scope === 'domain') {
      // 使用新的 domain_ids 字段（数组）
      data.domain_ids = formData.value.domain_ids || []
      // 同时设置第一个作为 domain_id（向后兼容）
      data.domain_id = data.domain_ids.length > 0 ? data.domain_ids[0] : null
    } else {
      data.domain_id = null
      data.domain_ids = null
    }
    
    if (dialogMode.value === 'create') {
      await ruleAPI.create(data)
      ElMessage.success('规则创建成功')
    } else {
      await ruleAPI.update(formData.value.rule_id, data)
      ElMessage.success('规则更新成功')
    }
    
    dialogVisible.value = false
    await loadRules()
  } catch (error) {
    console.error('保存规则失败:', error)
    if (error.response?.data?.detail) {
      ElMessage.error(error.response.data.detail)
    } else if (typeof error === 'object' && !error.response) {
      // 表单验证失败
      ElMessage.warning('请检查表单填写是否完整')
    } else {
      ElMessage.error('保存规则失败')
    }
  } finally {
    saving.value = false
  }
}

const handleDialogClose = () => {
  formRef.value?.resetFields()
}

const getRuleTypeLabel = (type) => {
  const labels = {
    derived_metric: '派生指标',
    default_filter: '默认过滤',
    custom_instruction: '自定义规则'
  }
  return labels[type] || type
}

const getRuleTypeColor = (type) => {
  const colors = {
    derived_metric: 'success',
    default_filter: 'warning',
    custom_instruction: 'info'
  }
  return colors[type] || ''
}

const formatDateTime = (dateStr) => {
  if (!dateStr) return '-'
  const date = new Date(dateStr)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  })
}

const getDomainName = (domainId) => {
  const domain = domains.value.find(d => d.domain_id === domainId)
  return domain?.domain_name || domain?.name || '未知域'
}

// connectionId 不再使用，规则通过 scope + domain_ids 控制作用范围

// 监听rule_type变化，重置rule_definition
watch(() => formData.value.rule_type, (newVal, oldVal) => {
  if (newVal !== oldVal && dialogMode.value === 'create') {
    // 仅在创建时按类型初始化必要字段，避免无关字段选择后被清空
    if (newVal === 'default_filter') {
      formData.value.rule_definition = {
        table_id: '',
        filter_field: '',
        filter_operator: '=',
        filter_value: ''
      }
    } else if (newVal === 'derived_metric') {
      formData.value.rule_definition = {
        metric_id: '',
        display_name: '',
        formula: '',
        field_dependencies: [],
        unit: ''
      }
    } else {
      formData.value.rule_definition = {}
    }
  }
})

// 生命周期
onMounted(() => {
  // 加载所有规则
  loadRules()
  // 加载所有业务域（供创建/编辑规则时选择）
  loadMetadata()
})
</script>

<style scoped>
.rule-list-container {
  padding: 0;
  background: #fff;
  border-radius: 4px;
  height: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-height: 0;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  flex-shrink: 0;
}

.toolbar-left,
.toolbar-right {
  display: flex;
  align-items: center;
  gap: 10px;
}

.rule-name-cell {
  display: flex;
  align-items: center;
}

:deep(.el-table) {
  flex: 1;
  overflow: hidden;
}

.action-buttons {
  display: flex;
  justify-content: center;
  gap: 4px;
  white-space: nowrap;
}

.rule-dialog-body {
  max-height: 70vh;
  overflow-y: auto;
  padding-right: 12px;
}

/* 确保容器填满 */
.rule-list-container {
  width: 100%;
}

.rule-list-container .el-card {
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
  
  .action-buttons {
    flex-direction: column;
    gap: 4px;
  }
  
  .action-buttons .el-button {
    margin-left: 0 !important;
  }
  
  :deep(.el-dialog) {
    width: 95% !important;
  }
  
  :deep(.el-dialog__body) {
    padding: 16px;
    max-height: 65vh;
    overflow-y: auto;
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

