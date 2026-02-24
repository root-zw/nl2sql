<template>
  <el-dialog
    :model-value="modelValue"
    @update:model-value="$emit('update:modelValue', $event)"
    :title="isEdit ? '编辑数据库连接' : '添加数据库连接'"
    width="90%"
    :style="{ maxWidth: '650px' }"
    :close-on-click-modal="false"
  >
    <el-form 
      ref="formRef" 
      :model="form" 
      :rules="rules" 
      label-width="120px"
    >
      <el-form-item label="连接名称" prop="connection_name">
        <el-input 
          v-model="form.connection_name" 
          placeholder="如：生产数据库"
        />
      </el-form-item>

      <el-form-item label="数据库类型" prop="db_type">
        <el-select v-model="form.db_type" style="width: 100%">
          <el-option label="SQL Server" value="sqlserver" />
          <el-option label="MySQL" value="mysql" />
          <el-option label="PostgreSQL" value="postgresql" />
        </el-select>
      </el-form-item>

      <el-form-item label="主机地址" prop="host">
        <el-input v-model="form.host" placeholder="127.0.0.1" />
      </el-form-item>

      <el-form-item label="端口" prop="port">
        <el-input-number 
          v-model="form.port" 
          :min="1" 
          :max="65535"
          style="width: 100%"
        />
      </el-form-item>

      <el-form-item label="数据库名" prop="database_name">
        <el-input v-model="form.database_name" />
      </el-form-item>

      <el-form-item label="用户名" prop="username">
        <el-input v-model="form.username" />
      </el-form-item>

      <el-form-item label="密码" prop="password">
        <el-input 
          v-model="form.password" 
          type="password" 
          show-password
        />
      </el-form-item>

      <el-form-item label="描述">
        <el-input 
          v-model="form.description" 
          type="textarea" 
          :rows="2"
        />
      </el-form-item>

      <el-form-item label="最大连接数">
        <el-input-number 
          v-model="form.max_connections" 
          :min="1" 
          :max="100"
          style="width: 100%"
        />
      </el-form-item>
    </el-form>

    <template #footer>
      <el-button @click="$emit('update:modelValue', false)">
        取消
      </el-button>
      <el-button @click="testConnection" :loading="testing">
        <el-icon><Connection /></el-icon>
        测试连接
      </el-button>
      <el-button type="primary" @click="save" :loading="saving">
        保存
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup>
import { ref, reactive, computed, watch } from 'vue'
import { Connection } from '@element-plus/icons-vue'
import axios from '@/utils/request'
import { ElMessage } from 'element-plus'

const DEFAULT_DB_HOST = import.meta.env.VITE_DEFAULT_DB_HOST || '127.0.0.1'
const DEFAULT_DB_PORT = Number(import.meta.env.VITE_DEFAULT_DB_PORT ?? 1433)

// 数据库类型对应的默认端口
const DEFAULT_PORTS = {
  sqlserver: 1433,
  mysql: 3306,
  postgresql: 5432
}

const props = defineProps({
  modelValue: Boolean,
  // 当前编辑的连接；为空时表示"新增"
  connection: {
    type: Object,
    default: null
  }
})

const emit = defineEmits(['update:modelValue', 'success'])

const formRef = ref()
const testing = ref(false)
const saving = ref(false)

// 是否为编辑模式
const isEdit = computed(() => !!props.connection && !!props.connection.connection_id)

const form = reactive({
  connection_name: '',
  db_type: 'sqlserver',
  host: DEFAULT_DB_HOST,
  port: DEFAULT_DB_PORT,
  database_name: '',
  username: '',
  password: '',
  description: '',
  max_connections: 10
})

// 根据模式初始化 / 填充表单
function fillFormFromConnection(conn) {
  if (!conn) {
    // 新增时使用默认值
    form.connection_name = ''
    form.db_type = 'sqlserver'
    form.host = DEFAULT_DB_HOST
    form.port = DEFAULT_DB_PORT
    form.database_name = ''
    form.username = ''
    form.password = ''
    form.description = ''
    form.max_connections = 10
    return
  }

  // 编辑模式：填充已有数据，密码不回显
  form.connection_name = conn.connection_name || ''
  form.db_type = conn.db_type || 'sqlserver'
  form.host = conn.host || DEFAULT_DB_HOST
  form.port = conn.port || DEFAULT_DB_PORT
  form.database_name = conn.database_name || ''
  form.username = conn.username || ''
  form.password = ''
  form.description = conn.description || ''
  form.max_connections = conn.max_connections ?? 10
}

// 监听传入的 connection 变化（点击“新增/编辑”时）
watch(
  () => props.connection,
  (conn) => {
    fillFormFromConnection(conn)
    // 切换记录时重置校验状态
    formRef.value && formRef.value.clearValidate()
  },
  { immediate: true }
)

// 监听弹窗显隐，关闭时重置"新增"表单
watch(
  () => props.modelValue,
  (val) => {
    if (!val && !isEdit.value) {
      // 只在新增模式下完全重置
      formRef.value && formRef.value.resetFields()
      fillFormFromConnection(null)
    }
  }
)

// 监听数据库类型变化，自动更新端口
watch(
  () => form.db_type,
  (newType) => {
    if (newType && DEFAULT_PORTS[newType]) {
      form.port = DEFAULT_PORTS[newType]
    }
  }
)

const rules = {
  connection_name: [
    { required: true, message: '请输入连接名称', trigger: 'blur' }
  ],
  db_type: [
    { required: true, message: '请选择数据库类型', trigger: 'change' }
  ],
  host: [
    { required: true, message: '请输入主机地址', trigger: 'blur' }
  ],
  port: [
    { required: true, message: '请输入端口', trigger: 'blur' }
  ],
  database_name: [
    { required: true, message: '请输入数据库名', trigger: 'blur' }
  ],
  username: [
    { required: true, message: '请输入用户名', trigger: 'blur' }
  ],
  // 新增时必须输入密码；编辑时可以不输（不改密码）
  password: [
    {
      validator: (rule, value, callback) => {
        if (!isEdit.value && !value) {
          callback(new Error('请输入密码'))
        } else {
          callback()
        }
      },
      trigger: 'blur'
    }
  ]
}

// 测试连接
async function testConnection() {
  const valid = await formRef.value.validate().catch(() => false)
  if (!valid) return

  // 编辑模式且未修改密码时，无法在弹窗内使用新的主机/端口测试，
  // 建议用户先保存再在列表中使用“测试连接”按钮。
  if (isEdit.value && !form.password) {
    ElMessage.info('编辑模式下如未修改密码，请先保存后在列表中使用“测试连接”按钮进行测试')
    return
  }

  testing.value = true
  try {
    const { data } = await axios.post('/admin/connections/test', form)
    if (data.success) {
      ElMessage.success('连接测试成功！')
    } else {
      ElMessage.warning(data.message || '连接失败')
    }
  } catch (error) {
    ElMessage.error('测试失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    testing.value = false
  }
}

// 保存
async function save() {
  const valid = await formRef.value.validate().catch(() => false)
  if (!valid) return

  saving.value = true
  try {
    if (isEdit.value) {
      // 编辑：调用更新接口，保持 connection_id 不变
      const payload = {
        connection_name: form.connection_name,
        description: form.description,
        host: form.host,
        port: form.port,
        database_name: form.database_name,
        username: form.username,
        max_connections: form.max_connections
      }
      // 仅在用户输入新密码时才更新密码
      if (form.password) {
        payload.password = form.password
      }
      await axios.put(`/admin/connections/${props.connection.connection_id}`, payload)
    } else {
      // 新增
      await axios.post('/admin/connections', form)
      // 新增成功后重置表单
      formRef.value.resetFields()
      fillFormFromConnection(null)
    }

    ElMessage.success('保存成功')
    emit('success')
  } catch (error) {
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
/* 响应式设计 */
@media screen and (max-width: 768px) {
  :deep(.el-form-item__label) {
    font-size: 13px;
  }
  
  :deep(.el-input__inner) {
    font-size: 16px; /* 防止iOS缩放 */
  }
  
  :deep(.el-dialog__footer) {
    display: flex;
    flex-direction: column-reverse;
    gap: 8px;
  }
  
  :deep(.el-dialog__footer .el-button) {
    width: 100%;
    margin-left: 0 !important;
  }
}

@media screen and (max-width: 480px) {
  :deep(.el-form) {
    --el-form-label-width: 100px !important;
  }
}
</style>

