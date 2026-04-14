/**
 * Axios 请求配置和拦截器
 * 智能问数
 * 
 * 特性：
 * - 自动 Token 刷新（Access Token 过期前自动刷新）
 * - 401 错误自动处理
 * - 并发请求时的 Token 刷新锁
 */
import axios from 'axios'
import { ElMessage } from 'element-plus'
import router from '@/router'
import { tokenManager } from './tokenManager'

const REQUEST_TIMEOUT = Number(import.meta.env.VITE_REQUEST_TIMEOUT ?? 30000)

// 创建 axios 实例
const request = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
  timeout: Number.isFinite(REQUEST_TIMEOUT) ? REQUEST_TIMEOUT : 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// 请求拦截器
request.interceptors.request.use(
  async config => {
    // 跳过刷新 Token 请求本身
    if (config.url?.includes('/admin/refresh')) {
      return config
    }
    
    // 确保 Token 有效（如果即将过期会自动刷新）
    const token = await tokenManager.ensureValidToken()
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
    return config
  },
  error => {
    console.error('请求错误:', error)
    return Promise.reject(error)
  }
)

// 响应拦截器
request.interceptors.response.use(
  response => {
    return response
  },
  error => {
    console.error('响应错误:', error)
    
    // 处理不同的错误状态码
    if (error.response) {
      const { status, data } = error.response
      
      switch (status) {
        case 401: {
          // 处理 detail 可能是对象或字符串的情况
          const rawDetail = data?.detail
          let detailMessage = ''
          let errorCode = null
          
          if (typeof rawDetail === 'object' && rawDetail !== null) {
            // 如果是对象，提取 message 和 error_code
            detailMessage = rawDetail.message || rawDetail.detail || ''
            errorCode = rawDetail.error_code
          } else {
            detailMessage = rawDetail || ''
          }
          
          const lowerDetail = detailMessage.toLowerCase()
          
          // 需要强制退出登录的错误码
          const forceLogoutErrorCodes = [
            10201,  // JWT_DECODE_FAILED
            10202,  // JWT_EXPIRED
            10105,  // AES_TOKEN_EXPIRED
            10101,  // AES_DECODE_FAILED
            10102,  // AES_DECRYPT_FAILED
            10002,  // NO_PROVIDER (所有认证方式都失败)
            10003,  // ALL_PROVIDERS_FAILED
          ]
          const isAuthErrorByCode = errorCode && forceLogoutErrorCodes.includes(errorCode)
          
          // 关键词匹配
          const shouldForceLogoutByKeyword = [
            '无效',
            '过期',
            'not valid',
            'expired',
            '不存在',
            '被禁用',
            '认证失败',
            '解析失败'
          ].some(keyword => lowerDetail.includes(keyword))
          
          // 任何 401 + 有 token 的情况都应该退出登录（token 无效了）
          const hasToken = !!tokenManager.getAccessToken()
          const shouldForceLogout = hasToken && (isAuthErrorByCode || shouldForceLogoutByKeyword || !detailMessage)

          if (shouldForceLogout) {
            ElMessage.error(detailMessage || '登录已过期，请重新登录')
            // 使用 tokenManager 清除所有 Token
            tokenManager.clearTokens()
            // 根据当前路径决定跳转到哪个登录页
            const currentPath = window.location.pathname
            if (currentPath.startsWith('/admin')) {
              router.push('/admin/login')
            } else {
              router.push('/login')
            }
          } else {
            ElMessage.warning(detailMessage || '当前操作需要登录')
          }
          break
        }
          
        case 403:
          ElMessage.error('没有权限访问该资源')
          break
          
        case 404:
          ElMessage.error('请求的资源不存在')
          break
          
        case 500:
          ElMessage.error(data?.detail || '服务器内部错误')
          break
          
        default:
          ElMessage.error(data?.detail || error.message || '请求失败')
      }
    } else if (error.request) {
      // 请求已发出但没有收到响应
      ElMessage.error('网络连接失败，请检查网络')
    } else {
      // 其他错误
      ElMessage.error(error.message || '请求失败')
    }
    
    return Promise.reject(error)
  }
)

// 导出 axios 实例
export default request

// 导出便捷方法
export const get = (url, params, config) => {
  return request.get(url, { params, ...config })
}

export const post = (url, data, config) => {
  return request.post(url, data, config)
}

export const put = (url, data, config) => {
  return request.put(url, data, config)
}

export const del = (url, config) => {
  return request.delete(url, config)
}
