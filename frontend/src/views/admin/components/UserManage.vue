<template>
  <div class="user-manage">
    <el-tabs v-model="active" type="card" @tab-change="onTabChange">
      <el-tab-pane label="用户管理" name="users">
        <UserManagePanel />
      </el-tab-pane>
      <el-tab-pane label="组织架构" name="organizations">
        <OrganizationManage />
      </el-tab-pane>
      <el-tab-pane label="认证配置" name="auth-providers">
        <AuthProviderManage />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import UserManagePanel from './UserManagePanel.vue'
import OrganizationManage from './OrganizationManage.vue'
import AuthProviderManage from './AuthProviderManage.vue'

// 有效的 Tab 名称
const validTabs = ['users', 'organizations', 'auth-providers']

// 从 URL hash 或 localStorage 恢复 Tab 状态
const getInitialTab = () => {
  // 优先从 URL hash 获取
  const hash = window.location.hash.slice(1)
  if (validTabs.includes(hash)) {
    return hash
  }
  // 其次从 localStorage 获取
  const saved = localStorage.getItem('userManageTab')
  if (validTabs.includes(saved)) {
    return saved
  }
  return 'users'
}

const active = ref(getInitialTab())

// Tab 切换时保存状态
const onTabChange = (tabName) => {
  localStorage.setItem('userManageTab', tabName)
  // 更新 URL hash（可选，便于分享链接）
  window.location.hash = tabName
}

// 初始化时设置 hash
onMounted(() => {
  if (!window.location.hash) {
    window.location.hash = active.value
  }
})
</script>

<style scoped>
.user-manage {
  padding: 0;
  width: 100%;
}
</style>
