<template>
  <div class="derived-metric-form">
    <el-form-item label="指标ID" prop="rule_definition.metric_id">
      <el-input
        v-model="localValue.metric_id"
        placeholder="如：derived_profit"
        :disabled="disabled"
      />
      <el-text type="info" size="small">
        用于系统内部标识，建议使用英文
      </el-text>
    </el-form-item>
    
    <el-form-item label="显示名称" prop="rule_definition.display_name">
      <el-input
        v-model="localValue.display_name"
        placeholder="如：利润"
        :disabled="disabled"
      />
    </el-form-item>
    
    <el-form-item label="计算公式" prop="rule_definition.formula">
      <el-input
        v-model="localValue.formula"
        type="textarea"
        :rows="3"
        placeholder="如：SUM({revenue}) - SUM({cost})"
        :disabled="disabled"
      />
      <el-text type="info" size="small">
        使用 {placeholder} 引用字段，如 SUM({收入}) - SUM({成本})
      </el-text>
    </el-form-item>
    
    <el-form-item label="字段依赖">
      <el-button
        type="primary"
        size="small"
        :icon="Plus"
        :disabled="disabled"
        @click="addFieldDependency"
      >
        添加字段
      </el-button>
      
      <div v-if="localValue.field_dependencies && localValue.field_dependencies.length > 0" style="margin-top: 12px;">
        <el-card
          v-for="(dep, index) in localValue.field_dependencies"
          :key="index"
          shadow="never"
          style="margin-bottom: 10px;"
        >
          <div style="display: flex; flex-direction: column; gap: 10px;">
            <div style="display: flex; align-items: center; gap: 10px;">
              <el-input
                v-model="dep.placeholder"
                placeholder="占位符（如：revenue）"
                style="flex: 1; min-width: 120px; max-width: 150px;"
                :disabled="disabled"
              />
              
              <el-select
                v-model="dep.table_name"
                placeholder="先选择表"
                filterable
                clearable
                style="flex: 1;"
                :disabled="disabled"
                @change="() => handleTableChange(index)"
              >
                <el-option
                  v-for="table in uniqueTables"
                  :key="table.table_id || table.table_name"
                  :label="getTableDisplayName(table)"
                  :value="table.table_name"
                >
                  <span>{{ getTableDisplayName(table) }}</span>
                </el-option>
                <template #empty>
                  <div style="padding: 20px; text-align: center; color: #999;">
                    暂无可用的表
                  </div>
                </template>
              </el-select>
              
              <el-button
                type="danger"
                :icon="Delete"
                circle
                size="small"
                :disabled="disabled"
                @click="removeFieldDependency(index)"
              />
            </div>
            
            <div style="display: flex; align-items: center; gap: 10px; padding-left: 160px;">
              <el-select
                v-model="dep.field_id"
                placeholder="再选择字段"
                filterable
                style="flex: 1;"
                :disabled="disabled || !dep.table_name"
              >
                <el-option
                  v-for="field in getFieldsByTable(dep.table_name)"
                  :key="field.field_id"
                  :label="getFieldLabel(field)"
                  :value="field.field_id"
                >
                  <span>{{ getFieldLabel(field) }}</span>
                </el-option>
                <template #empty>
                  <div style="padding: 20px; text-align: center; color: #999;">
                    {{ !dep.table_name ? '请先选择表' : '该表暂无字段' }}
                  </div>
                </template>
              </el-select>
              
              <el-select
                v-model="dep.aggregation"
                placeholder="聚合函数"
                style="flex: 0 0 auto; width: 120px;"
                :disabled="disabled"
              >
                <el-option label="SUM" value="SUM" />
                <el-option label="AVG" value="AVG" />
                <el-option label="COUNT" value="COUNT" />
                <el-option label="MAX" value="MAX" />
                <el-option label="MIN" value="MIN" />
              </el-select>
            </div>
          </div>
        </el-card>
      </div>
      
      <el-empty
        v-else
        description="暂无字段依赖，请添加"
        :image-size="80"
      />
    </el-form-item>
    
    <el-form-item label="单位">
      <el-input
        v-model="localValue.unit"
        placeholder="如：元、万元、%"
        :disabled="disabled"
      />
    </el-form-item>
    
    <el-form-item label="小数位数">
      <el-input-number
        v-model="localValue.decimal_places"
        :min="0"
        :max="10"
        :disabled="disabled"
        placeholder="如：2"
        style="width: 100%;"
      />
      <el-text type="info" size="small">
        结果显示的小数位数，0表示整数（如：宗数）
      </el-text>
    </el-form-item>
    
    <el-form-item label="同义词">
      <el-select
        v-model="localValue.synonyms"
        multiple
        filterable
        allow-create
        default-first-option
        :reserve-keyword="false"
        placeholder="输入同义词后按回车添加"
        style="width: 100%"
        :disabled="disabled"
      >
      </el-select>
      <el-text type="info" size="small">
        帮助LLM识别该指标的不同表述方式（如："宗数"、"地块数量"）
      </el-text>
    </el-form-item>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { Plus, Delete } from '@element-plus/icons-vue'

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({})
  },
  disabled: {
    type: Boolean,
    default: false
  },
  fields: {
    type: Array,
    default: () => []
  },
  tables: {
    type: Array,
    default: () => []
  }
})

const emit = defineEmits(['update:modelValue'])

// 🔧 初始化时使用深拷贝
const localValue = ref(JSON.parse(JSON.stringify({
  metric_id: '',
  display_name: '',
  formula: '',
  field_dependencies: [],
  unit: '',
  decimal_places: 2,  // 默认保留2位小数
  synonyms: [],  // 同义词列表
  ...props.modelValue
})))

// 获取所有唯一的表（优先使用 tables，否则从 fields 提取）
const uniqueTables = computed(() => {
  if (props.tables && props.tables.length > 0) {
    // 使用传入的表对象
    return props.tables
  }
  
  // 降级：从字段中提取表名（只返回表名字符串）
  const tableNames = new Set()
  props.fields.forEach(field => {
    if (field.table_name) {
      tableNames.add(field.table_name)
    }
  })
  // 将表名转换为简单的表对象
  return Array.from(tableNames).sort().map(name => ({ table_name: name }))
})

// 根据表名获取字段列表
const getFieldsByTable = (tableName) => {
  if (!tableName) return []
  return props.fields.filter(field => field.table_name === tableName)
}

// 获取表显示名称
const getTableDisplayName = (table) => {
  if (!table) return '未知表'
  // 如果是完整的表对象
  if (table.display_name !== undefined || table.table_id) {
    return table.display_name || table.table_name || '未命名表'
  }
  // 如果只是表名字符串对象
  return table.table_name || table || '未命名表'
}

// 获取字段显示标签
const getFieldLabel = (field) => {
  if (!field) return '未知字段'
  // 优先使用 display_name，否则使用 column_name，最后使用 field_name
  const name = field.display_name || field.column_name || field.field_name || '未命名字段'
  // 不再附加表名，因为已经在表选择器中显示了
  return name
}

// 表改变时清空字段选择
const handleTableChange = (index) => {
  if (localValue.value.field_dependencies[index]) {
    localValue.value.field_dependencies[index].field_id = ''
  }
}

const addFieldDependency = () => {
  if (!localValue.value.field_dependencies) {
    localValue.value.field_dependencies = []
  }
  localValue.value.field_dependencies.push({
    placeholder: '',
    table_name: '',
    field_id: '',
    aggregation: 'SUM'
  })
}

const removeFieldDependency = (index) => {
  localValue.value.field_dependencies.splice(index, 1)
}

// 防止递归更新
let isUpdating = false

watch(localValue, (newVal) => {
  if (!isUpdating) {
    isUpdating = true
    // 确保只传递纯值，清理对象引用
    const cleanValue = {
      metric_id: newVal.metric_id || '',
      display_name: newVal.display_name || '',
      formula: newVal.formula || '',
      field_dependencies: (newVal.field_dependencies || []).map(dep => ({
        placeholder: dep.placeholder || '',
        table_name: typeof dep.table_name === 'string' ? dep.table_name : '',
        field_id: typeof dep.field_id === 'object' ? (dep.field_id?.field_id || '') : (dep.field_id || ''),
        aggregation: dep.aggregation || 'SUM'
      })),
      unit: newVal.unit || '',
      decimal_places: newVal.decimal_places !== undefined ? newVal.decimal_places : 2,
      synonyms: Array.isArray(newVal.synonyms) ? newVal.synonyms : []
    }
    emit('update:modelValue', cleanValue)
    setTimeout(() => { isUpdating = false }, 0)
  }
}, { deep: true })

watch(() => props.modelValue, (newVal) => {
  if (!isUpdating && JSON.stringify(newVal) !== JSON.stringify(localValue.value)) {
    isUpdating = true
    // 🔧 使用深拷贝，避免修改原始对象
    localValue.value = JSON.parse(JSON.stringify({
      metric_id: '',
      display_name: '',
      formula: '',
      field_dependencies: [],
      unit: '',
      decimal_places: 2,  // 默认保留2位小数
      synonyms: [],  // 同义词列表
      ...newVal
    }))
    setTimeout(() => { isUpdating = false }, 0)
  }
}, { deep: true, immediate: true })
</script>

<style scoped>
.derived-metric-form {
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

