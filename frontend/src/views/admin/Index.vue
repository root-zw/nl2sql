<template>
  <div class="admin-index">
    <!-- 移动端遮罩层 -->
    <div 
      class="mobile-overlay" 
      :class="{ visible: mobileMenuOpen }"
      @click="mobileMenuOpen = false"
    ></div>
    
    <el-container class="admin-container">
      <!-- 左侧功能导航 -->
      <el-aside width="220px" class="nav-aside" :class="{ 'mobile-open': mobileMenuOpen }">
        <div class="aside-header">
          <h3>智能问数元数据管理</h3>
        </div>

        <el-menu
          :default-active="currentMenuPath"
          @select="handleMenuSelect"
          class="nav-menu"
          router
        >
          <!-- 数据管理员可见：数据库连接、元数据、Milvus同步、数据权限管理 -->
          <el-menu-item index="/admin/connections">
            <el-icon><Connection /></el-icon>
            <span>数据库连接</span>
          </el-menu-item>

          <el-menu-item index="/admin/metadata">
            <el-icon><DataAnalysis /></el-icon>
            <span>元数据管理</span>
          </el-menu-item>

          <el-menu-item index="/admin/milvus">
            <el-icon><Refresh /></el-icon>
            <span>Milvus同步</span>
          </el-menu-item>

          <el-menu-item index="/admin/data-roles">
            <el-icon><Lock /></el-icon>
            <span>数据权限管理</span>
          </el-menu-item>

          <el-menu-item index="/admin/governance-candidates">
            <el-icon><Document /></el-icon>
            <span>治理候选</span>
          </el-menu-item>

          <!-- 系统管理员专属 -->
          <template v-if="isSystemAdmin">
            <el-divider />

            <el-menu-item index="/admin/monitor">
              <el-icon><Monitor /></el-icon>
              <span>系统监控</span>
            </el-menu-item>

            <el-menu-item index="/admin/query-logs">
              <el-icon><Document /></el-icon>
              <span>查询日志</span>
            </el-menu-item>

            <el-menu-item index="/admin/model-config">
              <el-icon><Setting /></el-icon>
              <span>模型配置</span>
            </el-menu-item>

            <el-menu-item index="/admin/users">
              <el-icon><User /></el-icon>
              <span>用户管理</span>
            </el-menu-item>

          </template>
        </el-menu>
      </el-aside>

      <!-- 右侧内容区 -->
      <el-container>
        <!-- 顶部Header -->
        <el-header class="content-header">
          <div class="header-content">
            <!-- 移动端菜单按钮 -->
            <button class="mobile-menu-btn" @click="mobileMenuOpen = true">
              <el-icon size="20"><Menu /></el-icon>
            </button>
            <div class="breadcrumb-bar">
              <el-breadcrumb separator="/">
                <el-breadcrumb-item>管理系统</el-breadcrumb-item>
                <el-breadcrumb-item>{{ currentMenuName }}</el-breadcrumb-item>
              </el-breadcrumb>
            </div>
            <div class="user-section">
              <el-button 
                type="primary" 
                :icon="ChatDotRound" 
                @click="goToQuery"
                class="query-btn"
              >
                智能问答
              </el-button>
              <el-dropdown @command="handleCommand">
                <span class="user-info">
                  <el-icon><User /></el-icon>
                  {{ user.username || 'admin' }}
                  <el-icon class="el-icon--right"><ArrowDown /></el-icon>
                </span>
                <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item command="changePassword">修改密码</el-dropdown-item>
                    <el-dropdown-item command="logout">退出登录</el-dropdown-item>
                  </el-dropdown-menu>
                </template>
              </el-dropdown>
            </div>
          </div>
        </el-header>

        <!-- 主内容区 -->
        <el-main class="content-main">
          <!-- 动态内容区 -->
          <div class="content-body">
            <router-view />
          </div>
        </el-main>
      </el-container>
    </el-container>

    <!-- 修改密码对话框 -->
    <el-dialog
      v-model="showChangePasswordDialog"
      title="修改密码"
      width="90%"
      :style="{ maxWidth: '500px' }"
      :close-on-click-modal="false"
    >
      <ChangePasswordDialog 
        @success="handlePasswordChangeSuccess" 
        @cancel="showChangePasswordDialog = false"
      />
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import {
  Connection,
  DataAnalysis,
  Refresh,
  User,
  Monitor,
  Document,
  Setting,
  ChatDotRound,
  ArrowDown,
  Lock,
  Menu
} from '@element-plus/icons-vue'
import ChangePasswordDialog from './components/ChangePasswordDialog.vue'

const router = useRouter()
const route = useRoute()

// 允许登录后台的角色（直接使用数据库值）
const BACKEND_ALLOWED_ROLES = ['admin', 'data_admin']

// 状态
const user = ref(JSON.parse(localStorage.getItem('user') || '{}'))
const mobileMenuOpen = ref(false)

// 检查用户权限
onMounted(() => {
  const userRole = user.value?.role
  if (!userRole || !BACKEND_ALLOWED_ROLES.includes(userRole)) {
    ElMessage.warning('您没有访问管理后台的权限')
    router.replace('/admin/login')
  }
})

// 系统角色判断（直接使用数据库值）
// admin -> 系统管理员，可访问所有功能
// data_admin -> 数据管理员，只能访问数据相关功能
const isSystemAdmin = computed(() => {
  return user.value?.role === 'admin'
})

const isDataAdmin = computed(() => {
  const role = user.value?.role
  return role === 'admin' || role === 'data_admin'
})

const menuNames = {
  '/admin/connections': '数据库连接',
  '/admin/metadata': '元数据管理',
  '/admin/milvus': 'Milvus同步',
  '/admin/data-roles': '数据权限管理',
  '/admin/governance-candidates': '治理候选',
  '/admin/monitor': '系统监控',
  '/admin/query-logs': '查询日志',
  '/admin/model-config': '模型配置',
  '/admin/users': '用户管理'
}

// 当前激活的菜单路径
const currentMenuPath = computed(() => route.path)

// 当前菜单名称
const currentMenuName = computed(() => menuNames[route.path] || '')

// 菜单选择（由于使用了 router 模式，el-menu 会自动导航，这里只需处理特殊情况）
function handleMenuSelect(index) {
  // 移动端选择菜单后自动关闭侧边栏
  mobileMenuOpen.value = false
  
  if (index === '/query') {
    // query 页面单独处理
    return
  }
}

// 跳转到智能问答
function goToQuery() {
  window.open('/query', '_blank')
}

// 修改密码对话框显示状态
const showChangePasswordDialog = ref(false)

// 用户菜单操作
function handleCommand(command) {
  if (command === 'logout') {
    localStorage.removeItem('token')
    localStorage.removeItem('user')
    ElMessage.success('已退出登录')
    router.push('/login')
  } else if (command === 'changePassword') {
    showChangePasswordDialog.value = true
  }
}

// 密码修改成功回调
function handlePasswordChangeSuccess() {
  showChangePasswordDialog.value = false
  ElMessage.success('密码修改成功')
}
</script>

<style scoped>
.admin-index {
  height: 100%;
  background: #f5f7fa;
}

.admin-container {
  height: 100%;
}

/* 左侧导航 */
.nav-aside {
  background: linear-gradient(180deg, #1e3a5f 0%, #2d4a6d 100%);
  box-shadow: 2px 0 8px rgba(0, 0, 0, 0.1);
}

.aside-header {
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  background: rgba(0, 0, 0, 0.1);
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.aside-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: white;
  letter-spacing: 0.5px;
}

.nav-menu {
  border: none;
  background: transparent;
}

.nav-menu :deep(.el-menu-item) {
  color: rgba(255, 255, 255, 0.85);
  transition: all 0.3s;
}

.nav-menu :deep(.el-menu-item:hover) {
  background: rgba(255, 255, 255, 0.1) !important;
  color: #fff;
}

.nav-menu :deep(.el-menu-item.is-active) {
  background: linear-gradient(90deg, rgba(64, 158, 255, 0.2), transparent) !important;
  border-right: 3px solid #409eff;
  color: #409eff;
  font-weight: 600;
}

/* 右侧内容区 */
.content-header {
  background: white;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
  height: 64px;
  padding: 0 24px;
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 100%;
}

.breadcrumb-bar {
  flex: 1;
}

.user-section {
  display: flex;
  align-items: center;
  gap: 16px;
}

.query-btn {
  margin-right: 0;
}

.user-info {
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  border-radius: 6px;
  transition: all 0.3s;
  color: #606266;
}

.user-info:hover {
  background: #f5f7fa;
  color: #409eff;
}

.content-main {
  background: #f5f7fa;
  padding: 24px;
  overflow-y: auto;
}

.content-body {
  min-height: calc(100vh - 112px);
}

/* 移动端菜单按钮 - 默认隐藏 */
.mobile-menu-btn {
  display: none;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  background: none;
  border: none;
  border-radius: 8px;
  color: #606266;
  cursor: pointer;
  transition: all 0.2s;
  margin-right: 12px;
}

.mobile-menu-btn:hover {
  background: #f5f7fa;
  color: #409eff;
}

/* 移动端遮罩层 - 默认隐藏 */
.mobile-overlay {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
  z-index: 999;
}

.mobile-overlay.visible {
  display: block;
}

/* ==================== 响应式 - 平板 ==================== */
@media (max-width: 1024px) {
  .content-main {
    padding: 16px;
  }
  
  .aside-header h3 {
    font-size: 15px;
  }
}

/* ==================== 响应式 - 移动端 ==================== */
@media (max-width: 768px) {
  /* 显示移动端菜单按钮 */
  .mobile-menu-btn {
    display: flex;
  }
  
  /* 侧边栏抽屉式 */
  .nav-aside {
    position: fixed;
    left: 0;
    top: 0;
    bottom: 0;
    z-index: 1000;
    width: 260px !important;
    transform: translateX(-100%);
    transition: transform 0.3s ease;
  }
  
  .nav-aside.mobile-open {
    transform: translateX(0);
  }
  
  .aside-header {
    height: 56px;
  }
  
  .aside-header h3 {
    font-size: 14px;
  }
  
  /* 顶部Header */
  .content-header {
    height: 56px;
    padding: 0 12px;
  }
  
  .header-content {
    gap: 8px;
  }
  
  .breadcrumb-bar {
    flex: 1;
    min-width: 0;
    overflow: hidden;
  }
  
  .breadcrumb-bar :deep(.el-breadcrumb) {
    font-size: 13px;
  }
  
  .breadcrumb-bar :deep(.el-breadcrumb__item:first-child) {
    display: none;
  }
  
  .user-section {
    gap: 8px;
  }
  
  .query-btn {
    padding: 8px 12px;
    font-size: 13px;
  }
  
  .query-btn :deep(.el-icon) {
    margin-right: 4px;
  }
  
  .query-btn :deep(span:last-child) {
    display: none;
  }
  
  .user-info {
    padding: 6px 10px;
    font-size: 13px;
  }
  
  .user-info :deep(.el-icon--right) {
    display: none;
  }
  
  /* 主内容区 */
  .content-main {
    padding: 12px;
  }
  
  .content-body {
    min-height: calc(100vh - 80px);
  }
  
  /* 菜单项 */
  .nav-menu :deep(.el-menu-item) {
    height: 48px;
    line-height: 48px;
    font-size: 14px;
  }
}

/* ==================== 响应式 - 小手机 ==================== */
@media (max-width: 400px) {
  .nav-aside {
    width: 240px !important;
  }
  
  .query-btn {
    padding: 6px 10px;
  }
  
  .user-info {
    padding: 4px 8px;
  }
  
  .user-info span:not(.el-icon) {
    display: none;
  }
}
</style>
