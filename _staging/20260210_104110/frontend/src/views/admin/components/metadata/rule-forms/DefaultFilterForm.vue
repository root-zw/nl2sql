<template>
  <div class="default-filter-form">
    <el-form-item label="应用表" prop="rule_definition.table_id">
      <el-select
        v-model="localValue.table_id"
        placeholder="选择表"
        filterable
        style="width: 100%;"
        :disabled="disabled"
        @change="handleTableChange"
      >
        <el-option
          v-for="table in tables"
          :key="table.table_id"
          :label="getTableLabel(table)"
          :value="table.table_id"
        >
          <span>{{ getTableLabel(table) }}</span>
        </el-option>
        <template #empty>
          <div style="padding: 20px; text-align: center; color: #999;">
            暂无可用的表
          </div>
        </template>
      </el-select>
      <el-text type="info" size="small">
        规则将应用于此表的所有查询
      </el-text>
      <el-text v-if="tables.length === 0" type="warning" size="small" style="display: block; margin-top: 5px;">
        暂无可用的表，请先同步数据库Schema
      </el-text>
    </el-form-item>
    
    <el-form-item label="过滤字段" prop="rule_definition.filter_field">
      <el-select
        v-model="localValue.filter_field"
        placeholder="选择字段"
        filterable
        style="width: 100%;"
        :disabled="disabled || !localValue.table_id"
      >
        <el-option
          v-for="col in tableColumns"
          :key="col.column_name"
          :label="col.display_name || col.column_name || '未命名字段'"
          :value="col.column_name"
        />
        <template #empty>
          <div style="padding: 20px; text-align: center; color: #999;">
            {{ !localValue.table_id ? '请先选择表' : '该表暂无字段' }}
          </div>
        </template>
      </el-select>
      <el-text v-if="localValue.table_id && tableColumns.length === 0" type="warning" size="small">
        该表暂无可用字段
      </el-text>
    </el-form-item>
    
    <el-form-item label="操作符" prop="rule_definition.filter_operator">
      <el-select
        v-model="localValue.filter_operator"
        placeholder="选择操作符"
        style="width: 100%;"
        :disabled="disabled"
      >
        <el-option label="等于 (=)" value="=" />
        <el-option label="不等于 (!=)" value="!=" />
        <el-option label="大于 (>)" value=">" />
        <el-option label="小于 (<)" value="<" />
        <el-option label="大于等于 (>=)" value=">=" />
        <el-option label="小于等于 (<=)" value="<=" />
        <el-option label="包含 (IN)" value="IN" />
        <el-option label="不包含 (NOT IN)" value="NOT IN" />
        <el-option label="模糊匹配 (LIKE)" value="LIKE" />
        <el-option label="为空 (IS NULL)" value="IS NULL" />
        <el-option label="不为空 (IS NOT NULL)" value="IS NOT NULL" />
      </el-select>
    </el-form-item>
    
    <el-form-item 
      v-if="needsValue" 
      label="过滤值" 
      prop="rule_definition.filter_value"
    >
      <!-- IN / NOT IN 操作符：多值输入 -->
      <template v-if="isMultiValue">
        <el-select
          v-model="filterValueArray"
          multiple
          filterable
          allow-create
          default-first-option
          :reserve-keyword="false"
          placeholder="输入值后按回车添加（可添加多个）"
          style="width: 100%;"
          :disabled="disabled"
        >
        </el-select>
        <el-text type="info" size="small">
          可输入多个值
        </el-text>
      </template>
      
      <!-- 其他操作符：单值输入 -->
      <template v-else>
        <el-input
          v-model="localValue.filter_value"
          placeholder="输入过滤值"
          :disabled="disabled"
        />
        <el-text type="info" size="small">
          如：已审核、已完成等
        </el-text>
      </template>
    </el-form-item>
    
    <el-alert
      type="info"
      :closable="false"
      show-icon
      style="margin-top: 16px;"
    >
      <template #title>
        <strong>规则说明</strong>
      </template>
      <p style="margin: 8px 0;">
        此规则将在SQL编译时自动注入WHERE条件，用户无需在查询中明确指定。
      </p>
      <p style="margin: 8px 0;">
        <strong>示例</strong>：配置"订单表.approvestate = 已审核"后，
        所有涉及订单表的查询都会自动添加此过滤条件。
      </p>
    </el-alert>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({})
  },
  disabled: {
    type: Boolean,
    default: false
  },
  tables: {
    type: Array,
    default: () => []
  },
  fields: {
    type: Array,
    default: () => []
  }
})

const emit = defineEmits(['update:modelValue'])

const localValue = ref({
  table_id: '',
  filter_field: '',
  filter_operator: '=',
  filter_value: '',
  ...props.modelValue
})

const filterValueArray = ref([])

// 当前选中表的字段列表
const tableColumns = computed(() => {
  if (!localValue.value.table_id) return []
  
  // 首先尝试从表对象的columns属性获取
  const table = props.tables.find(t => t.table_id === localValue.value.table_id)
  if (table?.columns && table.columns.length > 0) {
    // 统一规范为 { column_name, display_name }
    return table.columns
      .filter(col => !!col)
      .map((col, idx) => {
        const columnName = col.column_name || col.columnName || col.name || ''
        const displayName = col.display_name || col.column_comment || col.comment || columnName
        return columnName
          ? { column_name: columnName, display_name: displayName }
          : null
      })
      .filter(Boolean)
  }
  
  // 如果没有，从字段列表中筛选该表的字段
  if (props.fields && props.fields.length > 0 && table) {
    const tableName = table.table_name
    return props.fields
      .filter(f => f.table_name === tableName)
      .map((f, idx) => {
        const columnName = f.column_name || f.field_name || f.display_name || ''
        const displayName = f.display_name || f.column_name || f.field_name || `字段${idx + 1}`
        return columnName ? { column_name: columnName, display_name: displayName } : null
      })
      .filter(Boolean)
  }
  
  return []
})

// 是否需要输入值（IS NULL/IS NOT NULL不需要）
const needsValue = computed(() => {
  return localValue.value.filter_operator !== 'IS NULL' && 
         localValue.value.filter_operator !== 'IS NOT NULL'
})

// 是否为多值操作符（IN/NOT IN）
const isMultiValue = computed(() => {
  return localValue.value.filter_operator === 'IN' || 
         localValue.value.filter_operator === 'NOT IN'
})

// 获取表显示标签
const getTableLabel = (table) => {
  if (!table) return '未知表'
  // 优先使用 display_name，否则使用 table_name，最后使用 name
  return table.display_name || table.table_name || table.name || '未命名表'
}

const handleTableChange = () => {
  // 表变更时，清空字段
  localValue.value.filter_field = ''
  localValue.value.filter_value = ''
  filterValueArray.value = []
}

// 同步多值数组到filter_value
watch(filterValueArray, (newVal) => {
  if (isMultiValue.value && JSON.stringify(newVal) !== JSON.stringify(localValue.value.filter_value)) {
    localValue.value.filter_value = newVal
  }
}, { deep: true })

// 初始化多值数组
watch(() => localValue.value.filter_value, (newVal) => {
  if (isMultiValue.value && Array.isArray(newVal) && JSON.stringify(newVal) !== JSON.stringify(filterValueArray.value)) {
    filterValueArray.value = newVal
  }
})

// 同步 localValue 到父组件（避免递归）
let isUpdating = false
watch(localValue, (newVal) => {
  if (!isUpdating) {
    isUpdating = true
    // 确保只传递纯值，避免传递对象引用
    const cleanValue = {
      table_id: typeof newVal.table_id === 'object' ? (newVal.table_id?.table_id || '') : (newVal.table_id || ''),
      filter_field: typeof newVal.filter_field === 'string' ? newVal.filter_field : '',
      filter_operator: newVal.filter_operator || '=',
      filter_value: newVal.filter_value || ''
    }
    emit('update:modelValue', cleanValue)
    setTimeout(() => { isUpdating = false }, 0)
  }
}, { deep: true })

// 监听父组件传入的值
watch(() => props.modelValue, (newVal) => {
  if (!isUpdating && JSON.stringify(newVal) !== JSON.stringify(localValue.value)) {
    isUpdating = true
    // 清理可能的对象引用，确保 table_id 是字符串
    const cleanedTableId = typeof newVal?.table_id === 'object' 
      ? (newVal.table_id?.table_id || '') 
      : (newVal?.table_id || '')
    
    localValue.value = {
      table_id: cleanedTableId,
      filter_field: newVal?.filter_field || '',
      filter_operator: newVal?.filter_operator || '=',
      filter_value: newVal?.filter_value || ''
    }
    
    // 如果是多值操作符且已有值，初始化数组
    if (isMultiValue.value && Array.isArray(newVal.filter_value)) {
      filterValueArray.value = [...newVal.filter_value]
    }
    setTimeout(() => { isUpdating = false }, 0)
  }
}, { deep: true, immediate: true })
</script>

<style scoped>
.default-filter-form {
  padding: 0;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  :deep(.el-form-item__label) {
    font-size: 13px;
  }
  
  :deep(.el-input__inner),
  :deep(.el-textarea__inner) {
    font-size: 16px;
  }
}
</style>

