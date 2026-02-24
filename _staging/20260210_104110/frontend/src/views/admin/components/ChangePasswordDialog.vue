<template>
  <div class="change-password-dialog">
    <el-form
      ref="formRef"
      :model="form"
      :rules="rules"
      label-width="100px"
      @submit.prevent="handleSubmit"
    >
      <el-form-item label="当前密码" prop="oldPassword">
        <el-input
          v-model="form.oldPassword"
          type="password"
          placeholder="请输入当前密码"
          show-password
          clearable
        />
      </el-form-item>

      <el-form-item label="新密码" prop="newPassword">
        <el-input
          v-model="form.newPassword"
          type="password"
          placeholder="请输入新密码（至少8位，包含大写、小写、数字、特殊字符中的至少三种）"
          show-password
          clearable
        />
      </el-form-item>

      <el-form-item label="确认密码" prop="confirmPassword">
        <el-input
          v-model="form.confirmPassword"
          type="password"
          placeholder="请再次输入新密码"
          show-password
          clearable
        />
      </el-form-item>
    </el-form>

    <div class="dialog-footer">
      <el-button @click="handleCancel">取消</el-button>
      <el-button type="primary" @click="handleSubmit" :loading="loading">
        确认修改
      </el-button>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive } from 'vue'
import { ElMessage } from 'element-plus'
import { authAPI } from '@/api'
import { validatePasswordStrength } from '@/utils/common'

const emit = defineEmits(['success', 'cancel'])

const formRef = ref(null)
const loading = ref(false)

const form = reactive({
  oldPassword: '',
  newPassword: '',
  confirmPassword: ''
})

// 验证新密码强度
const validateNewPassword = (rule, value, callback) => {
  if (!value) {
    callback(new Error('请输入新密码'))
    return
  }
  const result = validatePasswordStrength(value)
  if (!result.valid) {
    callback(new Error(result.message))
  } else {
    callback()
  }
}

// 验证确认密码
const validateConfirmPassword = (rule, value, callback) => {
  if (value !== form.newPassword) {
    callback(new Error('两次输入的密码不一致'))
  } else {
    callback()
  }
}

const rules = {
  oldPassword: [
    { required: true, message: '请输入当前密码', trigger: 'blur' }
  ],
  newPassword: [
    { required: true, message: '请输入新密码', trigger: 'blur' },
    { validator: validateNewPassword, trigger: 'blur' }
  ],
  confirmPassword: [
    { required: true, message: '请确认新密码', trigger: 'blur' },
    { validator: validateConfirmPassword, trigger: 'blur' }
  ]
}

// 提交表单
async function handleSubmit() {
  if (!formRef.value) return

  try {
    await formRef.value.validate()
    
    loading.value = true
    
    await authAPI.changePassword({
      old_password: form.oldPassword,
      new_password: form.newPassword
    })
    
    // 重置表单
    form.oldPassword = ''
    form.newPassword = ''
    form.confirmPassword = ''
    formRef.value.resetFields()
    
    emit('success')
  } catch (error) {
    // 表单验证错误不需要额外处理，已经在表单中显示
    if (error.message && (error.message.includes('请输入') || error.message.includes('至少') || error.message.includes('不一致'))) {
      return
    }
    // 其他错误（如网络错误、服务器错误）由全局拦截器处理，这里不需要重复提示
  } finally {
    loading.value = false
  }
}

// 取消
function handleCancel() {
  form.oldPassword = ''
  form.newPassword = ''
  form.confirmPassword = ''
  formRef.value?.resetFields()
  emit('cancel')
}
</script>

<style scoped>
.change-password-dialog {
  padding: 20px 0;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  margin-top: 20px;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  .dialog-footer {
    flex-direction: column-reverse;
    gap: 8px;
  }
  
  .dialog-footer .el-button {
    width: 100%;
    margin-left: 0 !important;
  }
  
  :deep(.el-form-item__label) {
    font-size: 13px;
  }
  
  :deep(.el-input__inner) {
    font-size: 16px; /* 防止iOS缩放 */
  }
}
</style>

