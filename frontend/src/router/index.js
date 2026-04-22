import { createRouter, createWebHistory } from 'vue-router'
import { ElMessage } from 'element-plus'

const env = import.meta.env
const BRAND_NAME = env.VITE_BRAND_NAME || '智能问数'
const QUERY_ROUTE_TITLE = env.VITE_QUERY_ROUTE_TITLE || BRAND_NAME
const ADMIN_PAGE_TITLE = env.VITE_ADMIN_PAGE_TITLE || `${BRAND_NAME} 管理系统`
const DOC_TITLE_SUFFIX = env.VITE_DOC_TITLE_SUFFIX || BRAND_NAME

const BACKEND_ALLOWED_ROLES = ['admin', 'data_admin']

function isLoggedIn() {
  const token = localStorage.getItem('token')
  const userStr = localStorage.getItem('user')
  return !!token && !!userStr
}

function hasBackendAccess() {
  const userStr = localStorage.getItem('user')
  if (!userStr) {
    return false
  }
  try {
    const user = JSON.parse(userStr)
    return !!user?.role && BACKEND_ALLOWED_ROLES.includes(user.role)
  } catch (error) {
    console.warn('解析用户信息失败:', error)
    return false
  }
}

const routes = [
  {
    path: '/',
    redirect: '/login'
  },
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/Login.vue'),
    meta: { title: '登录', public: true }
  },
  {
    path: '/chat',
    name: 'Chat',
    component: () => import('@/views/Chat.vue'),
    meta: { title: QUERY_ROUTE_TITLE, requiresAuth: true }
  },
  {
    path: '/query',
    redirect: '/chat'
  },
  {
    path: '/admin/login',
    name: 'AdminLogin',
    redirect: '/login?admin=true'
  },
  {
    path: '/admin',
    name: 'Admin',
    component: () => import('@/views/admin/Index.vue'),
    meta: { requiresAuth: true, title: ADMIN_PAGE_TITLE },
    redirect: '/admin/connections',
    children: [
      {
        path: 'connections',
        name: 'AdminConnections',
        component: () => import('@/views/admin/components/DatabaseConnections.vue'),
        meta: { requiresAuth: true, title: '数据库连接' }
      },
      {
        path: 'metadata',
        name: 'AdminMetadata',
        component: () => import('@/views/admin/components/MetadataManage.vue'),
        meta: { requiresAuth: true, title: '元数据管理' }
      },
      {
        path: 'milvus',
        name: 'AdminMilvus',
        component: () => import('@/views/admin/components/MilvusSyncManage.vue'),
        meta: { requiresAuth: true, title: 'Milvus同步' }
      },
      {
        path: 'monitor',
        name: 'AdminMonitor',
        component: () => import('@/views/admin/components/SystemMonitor.vue'),
        meta: { requiresAuth: true, title: '系统监控' }
      },
      {
        path: 'query-logs',
        name: 'AdminQueryLogs',
        component: () => import('@/views/admin/components/QueryLogs.vue'),
        meta: { requiresAuth: true, title: '查询日志' }
      },
      {
        path: 'model-config',
        name: 'AdminModelConfig',
        component: () => import('@/views/admin/components/ModelConfigManage.vue'),
        meta: { requiresAuth: true, title: '模型配置' }
      },
      {
        path: 'users',
        name: 'AdminUsers',
        component: () => import('@/views/admin/components/UserManage.vue'),
        meta: { requiresAuth: true, title: '用户管理' }
      },
      {
        path: 'data-roles',
        name: 'AdminDataRoles',
        component: () => import('@/views/admin/components/DataRoleManage.vue'),
        meta: { requiresAuth: true, title: '数据权限管理' }
      },
      {
        path: 'governance-candidates',
        name: 'AdminGovernanceCandidates',
        component: () => import('@/views/admin/components/GovernanceCandidates.vue'),
        meta: { requiresAuth: true, title: '治理候选' }
      },
      {
        path: 'auth-providers',
        name: 'AdminAuthProviders',
        component: () => import('@/views/admin/components/AuthProviderManage.vue'),
        meta: { requiresAuth: true, title: '认证配置' }
      },
      {
        path: 'user-sync',
        name: 'AdminUserSync',
        component: () => import('@/views/admin/components/UserSync.vue'),
        meta: { requiresAuth: true, title: '用户同步' }
      }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

// 路由守卫
router.beforeEach((to, from, next) => {
  const loggedIn = isLoggedIn()
  const canAccessBackend = hasBackendAccess()

  // 设置页面标题
  if (to.meta.title) {
    document.title = `${to.meta.title} - ${DOC_TITLE_SUFFIX}`
  }

  // 公开页面（登录页）直接放行
  if (to.meta.public) {
    // 如果已登录且访问登录页，根据情况重定向
    if (loggedIn) {
      if (to.path === '/login') {
        next('/chat')
        return
      }
      if (to.path === '/admin/login' && canAccessBackend) {
        next('/admin/connections')
        return
      }
    }
    next()
    return
  }

  // 需要登录的页面
  if (to.meta.requiresAuth || to.path.startsWith('/admin')) {
    if (!loggedIn) {
      // 未登录，重定向到登录页
      if (to.path.startsWith('/admin')) {
        next('/admin/login')
      } else {
        next('/login')
      }
      return
    }

    // 管理后台需要额外的权限检查
    if (to.path.startsWith('/admin') && to.path !== '/admin/login') {
      if (!canAccessBackend) {
        ElMessage.warning('您没有访问管理后台的权限')
        next('/chat')
        return
      }
    }
  }

  next()
})

export default router
