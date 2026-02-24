<template>
  <div class="login-page">
    <div class="login-content">
      <section class="intro-panel">
        <p class="system-badge">NL2SQL Enterprise</p>
        <h1>智能问数系统</h1>
        <p class="intro-desc">
          用自然语言提问，AI 自动生成并执行 SQL。保存历史记录，
          让数据洞察更快速、安全地抵达决策。
        </p>
        <ul class="feature-list">
          <li v-for="item in highlights" :key="item.title">
            <h3>{{ item.title }}</h3>
            <p>{{ item.desc }}</p>
          </li>
        </ul>
      </section>

      <div class="login-panels">
        <!-- 本地登录面板 -->
        <el-card class="login-card local-login-card" shadow="hover">
          <div class="card-badge">
            <span class="badge-icon">👤</span>
            <span>账号密码登录</span>
          </div>
        <el-form
          class="login-form"
          ref="loginFormRef"
          :model="loginForm"
          :rules="rules"
          label-position="top"
          :hide-required-asterisk="true"
          @submit.prevent="handleLogin"
        >
          <el-form-item label="用户名" prop="username">
            <el-input
              v-model="loginForm.username"
              placeholder="请输入用户名"
              clearable
            />
          </el-form-item>
          
          <el-form-item label="密码" prop="password">
            <el-input
              v-model="loginForm.password"
              type="password"
              placeholder="请输入密码"
              show-password
              @keyup.enter="handleLogin"
            />
          </el-form-item>
          
          <el-form-item label="验证码" prop="captcha_code" class="captcha-item">
            <div class="captcha-wrapper">
              <el-input
                v-model="loginForm.captcha_code"
                placeholder="请输入验证码"
                maxlength="4"
                @keyup.enter="handleLogin"
              />
              <div
                class="captcha-image"
                :class="{ loading: captchaLoading }"
                @click="loadCaptcha"
              >
                <img v-if="captchaImage" :src="captchaImage" alt="captcha" />
                <span v-else>{{ captchaLoading ? '加载中' : '点击刷新' }}</span>
              </div>
            </div>
          </el-form-item>
          
          <el-form-item>
            <el-button
              type="primary"
              :loading="loading"
              class="login-btn"
              @click="handleLogin"
            >
              登录
            </el-button>
          </el-form-item>
        </el-form>
      </el-card>

        <!-- 其他登录方式面板 -->
        <el-card v-if="ssoProviders.length > 0" class="login-card sso-login-card" shadow="hover">
          <div class="card-badge">
            <span class="badge-icon">🔐</span>
            <span>其他登录方式</span>
          </div>
          <div class="sso-content">
            <div class="sso-buttons">
              <el-button
                v-for="provider in ssoProviders"
                :key="provider.provider_key"
                class="sso-btn"
                :loading="ssoLoading"
                @click="handleSsoLogin(provider)"
              >
                <span class="sso-icon">{{ provider.type === 'oauth2' ? '🔗' : '🔑' }}</span>
                {{ provider.provider_key }} 登录
              </el-button>
            </div>
          </div>
        </el-card>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import axios from '@/utils/request'
import { tokenManager } from '@/utils/tokenManager'

const router = useRouter()
const route = useRoute()
const loginFormRef = ref()
const loading = ref(false)
const captchaImage = ref('')
const captchaLoading = ref(false)
const captchaId = ref('')

// SSO 登录相关
const ssoProviders = ref([])
const ssoLoading = ref(false)

const loginForm = reactive({
  username: '',
  password: '',
  captcha_code: ''
})

const rules = {
  username: [{ required: true, message: '请输入用户名', trigger: 'blur' }],
  password: [{ required: true, message: '请输入密码', trigger: 'blur' }],
  captcha_code: [
    { required: true, message: '请输入验证码', trigger: 'blur' },
    { pattern: /^[A-Za-z0-9]{4}$/, message: '验证码为4位字母或数字', trigger: ['blur', 'change'] }
  ]
}

const highlights = [
  {
    title: '智能问数',
    desc: '用自然语言提问，AI 自动生成并执行 SQL，像聊天一样查询数据。'
  },
  {
    title: '统一权限体系',
    desc: '系统管理员 / 数据管理员分职清晰，字段级与行级权限一体化管控。'
  }
]

// 允许登录后台的角色（直接使用数据库值）
const BACKEND_ALLOWED_ROLES = ['admin', 'data_admin']

// 检查是否是管理后台入口（通过 URL 参数或来源判断）
const isAdminEntry = computed(() => {
  return route.query.admin === 'true' || route.path === '/admin/login'
})

const loadCaptcha = async () => {
  captchaLoading.value = true
  try {
    const { data } = await axios.get('/admin/captcha')
    captchaId.value = data.captcha_id
    captchaImage.value = data.image_base64?.startsWith('data:')
      ? data.image_base64
      : `data:image/png;base64,${data.image_base64}`
    loginForm.captcha_code = ''
  } catch (error) {
    console.error('加载验证码失败', error)
    ElMessage.error('验证码加载失败，请稍后重试')
  } finally {
    captchaLoading.value = false
  }
}

// 加载可用的 SSO 提供者（OIDC + OAuth 2.0）
const loadSsoProviders = async () => {
  try {
    // 并行加载 OIDC 和 OAuth 2.0 提供者
    const [oidcRes, oauth2Res] = await Promise.all([
      axios.get('/admin/oidc/providers').catch(() => ({ data: { providers: [] } })),
      axios.get('/admin/oauth2/providers').catch(() => ({ data: { providers: [] } }))
    ])
    
    const oidcProviders = (oidcRes.data?.providers || []).map(p => ({ ...p, type: 'oidc' }))
    const oauth2Providers = (oauth2Res.data?.providers || []).map(p => ({ ...p, type: 'oauth2' }))
    
    ssoProviders.value = [...oidcProviders, ...oauth2Providers]
  } catch (error) {
    console.error('加载SSO提供者失败', error)
    // 静默失败，不影响本地登录
  }
}

// SSO 登录（支持 OIDC 和 OAuth 2.0）
const handleSsoLogin = async (provider) => {
  ssoLoading.value = true
  try {
    // 根据提供者类型调用不同的接口
    const endpoint = provider.type === 'oauth2' ? '/admin/oauth2/login' : '/admin/oidc/login'
    const { data } = await axios.get(endpoint, {
      params: { provider_key: provider.provider_key }
    })
    if (data.redirect_url) {
      // 跳转到认证提供者登录页面
      window.location.href = data.redirect_url
    } else {
      ElMessage.error('获取登录地址失败')
    }
  } catch (error) {
    console.error('SSO登录失败', error)
    ElMessage.error(error.response?.data?.detail || 'SSO登录失败')
  } finally {
    ssoLoading.value = false
  }
}

// 处理 OIDC 回调（从 URL hash 中获取 token）
const handleOidcCallback = () => {
  // 从 hash 中解析参数（格式: #token=xxx&refresh_token=xxx&expires_in=xxx&user=xxx）
  const hash = window.location.hash.substring(1)
  if (!hash) return
  
  const params = new URLSearchParams(hash)
  const token = params.get('token')
  const refreshToken = params.get('refresh_token')
  const expiresIn = params.get('expires_in')
  const userStr = params.get('user')
  
  if (token) {
    // 使用 tokenManager 保存 Token 对
    tokenManager.saveTokens(
      token,
      refreshToken,
      expiresIn ? parseInt(expiresIn) : null,
      null  // SSO 场景暂不传递 refresh_expires_in
    )
    
    let userRole = null
    if (userStr) {
      try {
        const user = JSON.parse(decodeURIComponent(userStr))
        userRole = user.role
        localStorage.setItem('user', JSON.stringify(user))
      } catch (e) {
        console.error('解析用户信息失败', e)
      }
    }
    axios.defaults.headers.common['Authorization'] = `Bearer ${token}`
    
    // 清除 URL hash，避免敏感信息留在地址栏
    window.history.replaceState(null, '', window.location.pathname)
    
    ElMessage.success('登录成功')
    
    // 根据入口和角色决定跳转目标
    if (isAdminEntry.value && BACKEND_ALLOWED_ROLES.includes(userRole)) {
      router.push('/admin')
    } else {
      router.push('/chat')
    }
  }
}

const handleLogin = async () => {
  if (!loginFormRef.value) return
  
  await loginFormRef.value.validate(async (valid) => {
    if (!valid) return
    if (!captchaId.value) {
      ElMessage.error('验证码加载中，请稍后重试')
      await loadCaptcha()
      return
    }
    
    loading.value = true
    try {
      const payload = {
        username: loginForm.username.trim(),
        password: loginForm.password,
        captcha_code: loginForm.captcha_code.trim(),
        captcha_id: captchaId.value
      }
      const { data } = await axios.post('/admin/login', payload)
      
      // 使用 tokenManager 保存 Token 对
      tokenManager.saveTokens(
        data.access_token,
        data.refresh_token,
        data.expires_in,
        data.refresh_expires_in
      )
      localStorage.setItem('user', JSON.stringify(data.user))
      
      // 设置axios默认header
      axios.defaults.headers.common['Authorization'] = `Bearer ${data.access_token}`
      
      ElMessage.success('登录成功')
      
      // 根据入口和角色决定跳转目标
      const userRole = data.user?.role
      if (isAdminEntry.value && BACKEND_ALLOWED_ROLES.includes(userRole)) {
        // 从管理后台入口登录，且有权限，跳转到管理后台
        router.push('/admin')
      } else {
        // 普通入口或普通用户，跳转到会话页面
        router.push('/chat')
      }
    } catch (error) {
      ElMessage.error(error.response?.data?.detail || '登录失败')
      await loadCaptcha()
    } finally {
      loading.value = false
    }
  })
}

onMounted(() => {
  handleOidcCallback()  // 处理可能的 OIDC 回调
  loadCaptcha()
  loadSsoProviders()    // 加载 SSO 提供者
})
</script>

<style scoped>
.login-page {
  min-height: 100vh;
  padding: 48px 24px;
  background: url('/images/admin-login-bg.png') center/cover no-repeat fixed;
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
}

.login-page::before {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(120deg, rgba(15, 23, 42, 0.92), rgba(30, 41, 59, 0.65));
  backdrop-filter: blur(2px);
}

.login-content {
  position: relative;
  z-index: 1;
  width: 100%;
  max-width: 1100px;
  display: flex;
  gap: 32px;
  align-items: stretch;
}

.intro-panel {
  flex: 1;
  padding: 40px;
  border-radius: 24px;
  background: rgba(15, 23, 42, 0.6);
  border: 1px solid rgba(148, 163, 184, 0.2);
  color: #f8fafc;
  backdrop-filter: blur(8px);
  display: flex;
  flex-direction: column;
}

.system-badge {
  display: inline-flex;
  padding: 6px 14px;
  border-radius: 999px;
  border: 1px solid rgba(248, 250, 252, 0.4);
  font-size: 13px;
  letter-spacing: 0.05em;
  margin-bottom: 18px;
}

.intro-panel h1 {
  margin: 0 0 12px;
  font-size: 36px;
  font-weight: 600;
}

.intro-desc {
  margin: 0 0 28px;
  color: rgba(248, 250, 252, 0.85);
  line-height: 1.6;
}

.feature-list {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.feature-list li {
  padding: 16px;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.06);
  border: 1px solid rgba(255, 255, 255, 0.08);
}

.feature-list h3 {
  margin: 0 0 8px;
  font-size: 16px;
  font-weight: 600;
}

.feature-list p {
  margin: 0;
  font-size: 13px;
  color: rgba(248, 250, 252, 0.8);
  line-height: 1.5;
}

/* 登录面板容器 */
.login-panels {
  display: flex;
  gap: 24px;
  flex: 1.2;
}

.login-card {
  flex: 1;
  border-radius: 24px;
  background: rgba(15, 23, 42, 0.62);
  border: 1px solid rgba(148, 163, 184, 0.25);
  box-shadow: 0 25px 50px rgba(2, 6, 23, 0.55);
  color: #e2e8f0;
  overflow: hidden;
  backdrop-filter: blur(14px);
}

.local-login-card {
  flex: 1.8;
  min-width: 340px;
}

.sso-login-card {
  flex: 1;
  min-width: 220px;
}

:deep(.login-card .el-card__header) {
  display: none;
}

:deep(.login-card .el-card__body) {
  padding: 28px 24px 32px;
  display: flex;
  flex-direction: column;
}

.card-badge {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  border-radius: 999px;
  border: 1px solid rgba(248, 250, 252, 0.4);
  font-size: 14px;
  font-weight: 500;
  color: #f8fafc;
  letter-spacing: 0.03em;
  margin-bottom: 24px;
  width: fit-content;
}

.badge-icon {
  font-size: 16px;
}

.login-form {
  width: 100%;
  max-width: 330px;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  gap: 18px;
}

:deep(.login-form .el-form-item) {
  margin-bottom: 0;
}

:deep(.login-form .el-form-item__label) {
  padding-bottom: 6px;
  color: rgba(226, 232, 240, 0.9);
  font-weight: 500;
  font-size: 13px;
}

.login-btn {
  width: 100%;
  height: 48px;
  font-size: 15px;
  font-weight: 600;
  letter-spacing: 0.05em;
  border-radius: 16px;
  border: none;
  background: rgba(248, 250, 252, 0.95);
  color: #0f172a;
  box-shadow: none;
  transition: background 0.2s ease, transform 0.2s ease;
}

.login-btn:hover:not(.is-disabled) {
  background: rgba(248, 250, 252, 1);
  transform: translateY(-1px);
}

:deep(.login-card .el-input__wrapper) {
  background: rgba(15, 23, 42, 0.65);
  border: 1px solid rgba(148, 163, 184, 0.35);
  box-shadow: none;
  border-radius: 16px;
  min-height: 48px;
}

:deep(.login-card .el-input__inner) {
  color: #e2e8f0;
}

:deep(.login-card .el-input__suffix i),
:deep(.login-card .el-input__prefix i) {
  color: rgba(226, 232, 240, 0.7);
}

.captcha-wrapper {
  display: flex;
  align-items: center;
  gap: 12px;
}

.captcha-wrapper :deep(.el-input__wrapper) {
  flex: 1;
  min-width: 0; /* 允许输入框在小屏幕下收缩 */
}

.captcha-image {
  flex-shrink: 0; /* 优先保持验证码完整显示 */
  width: 130px;
  height: 48px;
  min-width: 100px; /* 最小宽度 */
  border-radius: 14px;
  border: none;
  background: transparent;
  padding: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  user-select: none;
  color: rgba(15, 23, 42, 0.8);
  font-size: 12px;
  font-weight: 600;
  transition: opacity 0.2s ease;
  overflow: hidden; /* 防止图片溢出 */
}

.captcha-image:hover {
  opacity: 0.85;
}

.captcha-image.loading {
  opacity: 0.6;
  cursor: wait;
}

.captcha-image img {
  width: 100%;
  height: 100%;
  border-radius: 10px;
  background: #fff;
  object-fit: contain; /* 保持比例，完整显示 */
}

/* SSO 登录内容 */
.sso-content {
  display: flex;
  flex-direction: column;
  justify-content: center;
  height: 100%;
  min-height: 280px;
}

.sso-buttons {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.sso-btn {
  width: 100%;
  height: 52px;
  font-size: 15px;
  font-weight: 600;
  border-radius: 16px;
  border: 1px solid rgba(99, 102, 241, 0.4);
  background: linear-gradient(135deg, rgba(99, 102, 241, 0.15), rgba(139, 92, 246, 0.15));
  color: #e2e8f0;
  transition: all 0.2s ease;
}

.sso-btn:hover:not(.is-disabled) {
  background: linear-gradient(135deg, rgba(99, 102, 241, 0.25), rgba(139, 92, 246, 0.25));
  border-color: rgba(99, 102, 241, 0.6);
  color: #f8fafc;
  transform: translateY(-2px);
  box-shadow: 0 4px 16px rgba(99, 102, 241, 0.2);
}

.sso-icon {
  margin-right: 10px;
  font-size: 18px;
}

/* 响应式：小屏幕下调整验证码大小 */
@media (max-width: 480px) {
  .captcha-image {
    width: 110px;
    height: 40px;
    min-width: 90px;
  }
}

@media (max-width: 1200px) {
  .login-content {
    flex-direction: column;
    max-width: 800px;
  }
  
  .login-panels {
    flex-direction: row;
  }
  
  .local-login-card,
  .sso-login-card {
    flex: 1;
  }
}

@media (max-width: 900px) {
  .login-panels {
    flex-direction: column;
  }
  
  .sso-login-card {
    min-width: auto;
  }
}

@media (max-width: 768px) {
  .login-page {
    padding: 32px 16px;
  }
  
  .intro-panel {
    padding: 24px;
  }
  
  .login-panels {
    gap: 16px;
  }
  
  :deep(.login-card .el-card__body) {
    padding: 20px 16px 24px;
  }
}
</style>

