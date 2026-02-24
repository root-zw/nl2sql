<template>
  <div class="card">
    <div class="card-header">
      <div>第三方用户同步</div>
      <el-button type="primary" size="small" @click="submit">提交同步</el-button>
    </div>
    <el-alert
      title="输入外部用户列表，字段：external_idp, external_uid, username(可选), email(可选), full_name(可选), role(默认user), is_active(默认true), profile(可选JSON)"
      type="info"
      show-icon
      class="mb-12"
    />
    <el-input
      v-model="payload"
      type="textarea"
      :rows="12"
      placeholder='[{"external_idp":"oidc","external_uid":"sub-123","username":"alice","email":"a@x.com"}]'
    />
    <div class="mt-12">
      <div>结果：</div>
      <pre class="result">{{ resultText }}</pre>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import { userSyncAPI } from '@/api'

const payload = ref('[\n  {\n    "external_idp": "oidc",\n    "external_uid": "demo-user-1",\n    "username": "demo1",\n    "email": "demo1@example.com"\n  }\n]')
const resultText = ref('')

const submit = async () => {
  try {
    const list = JSON.parse(payload.value)
    const res = await userSyncAPI.sync(list)
    resultText.value = JSON.stringify(res, null, 2)
    ElMessage.success('同步完成')
  } catch (e) {
    ElMessage.error('提交失败: ' + e)
  }
}
</script>

<style scoped>
.card {
  padding: 16px;
  background: #fff;
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
  font-weight: 600;
}
.mb-12 {
  margin-bottom: 12px;
}
.mt-12 {
  margin-top: 12px;
}
.result {
  white-space: pre-wrap;
  word-break: break-all;
  background: #f7f7f7;
  padding: 8px;
  border-radius: 4px;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  :deep(.el-card) {
    margin-bottom: 12px;
  }
  
  :deep(.el-form-item__label) {
    font-size: 13px;
  }
  
  .result {
    font-size: 12px;
    padding: 6px;
  }
}
</style>

