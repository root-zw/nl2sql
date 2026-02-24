/**
 * Token 管理器
 * 
 * 功能：
 * 1. 存储和获取 Access Token / Refresh Token
 * 2. 自动检测 Token 过期
 * 3. 静默刷新 Token
 * 4. 处理并发请求时的 Token 刷新
 */

import axios from 'axios'

const TOKEN_KEY = 'token'
const REFRESH_TOKEN_KEY = 'refresh_token'
const TOKEN_EXPIRES_AT_KEY = 'token_expires_at'
const REFRESH_EXPIRES_AT_KEY = 'refresh_expires_at'

// 刷新 Token 的锁，防止并发刷新
let isRefreshing = false
// 等待刷新完成的请求队列
let refreshSubscribers = []

/**
 * Token 管理器
 */
class TokenManager {
  /**
   * 保存 Token 对
   */
  saveTokens(accessToken, refreshToken, expiresIn, refreshExpiresIn) {
    localStorage.setItem(TOKEN_KEY, accessToken)
    if (refreshToken) {
      localStorage.setItem(REFRESH_TOKEN_KEY, refreshToken)
    }
    
    // 计算过期时间戳
    if (expiresIn) {
      const expiresAt = Date.now() + expiresIn * 1000
      localStorage.setItem(TOKEN_EXPIRES_AT_KEY, expiresAt.toString())
    }
    if (refreshExpiresIn) {
      const refreshExpiresAt = Date.now() + refreshExpiresIn * 1000
      localStorage.setItem(REFRESH_EXPIRES_AT_KEY, refreshExpiresAt.toString())
    }
  }

  /**
   * 获取 Access Token
   */
  getAccessToken() {
    return localStorage.getItem(TOKEN_KEY)
  }

  /**
   * 获取 Refresh Token
   */
  getRefreshToken() {
    return localStorage.getItem(REFRESH_TOKEN_KEY)
  }

  /**
   * 清除所有 Token
   */
  clearTokens() {
    localStorage.removeItem(TOKEN_KEY)
    localStorage.removeItem(REFRESH_TOKEN_KEY)
    localStorage.removeItem(TOKEN_EXPIRES_AT_KEY)
    localStorage.removeItem(REFRESH_EXPIRES_AT_KEY)
    localStorage.removeItem('user')
  }

  /**
   * 检查 Access Token 是否即将过期（提前 2 分钟）
   */
  isAccessTokenExpiringSoon() {
    const expiresAt = localStorage.getItem(TOKEN_EXPIRES_AT_KEY)
    if (!expiresAt) {
      return false
    }
    // 提前 2 分钟刷新
    const bufferTime = 2 * 60 * 1000
    return Date.now() > (parseInt(expiresAt) - bufferTime)
  }

  /**
   * 检查 Refresh Token 是否已过期
   */
  isRefreshTokenExpired() {
    const expiresAt = localStorage.getItem(REFRESH_EXPIRES_AT_KEY)
    if (!expiresAt) {
      return true
    }
    return Date.now() > parseInt(expiresAt)
  }

  /**
   * 刷新 Token
   * @returns {Promise<boolean>} 刷新是否成功
   */
  async refreshToken() {
    const refreshToken = this.getRefreshToken()
    if (!refreshToken) {
      console.warn('No refresh token available')
      return false
    }

    // 如果 Refresh Token 已过期，直接返回失败
    if (this.isRefreshTokenExpired()) {
      console.warn('Refresh token has expired')
      return false
    }

    try {
      // 使用独立的 axios 实例，避免触发拦截器
      const baseURL = import.meta.env.VITE_API_BASE_URL || '/api'
      const response = await axios.post(`${baseURL}/admin/refresh`, {
        refresh_token: refreshToken
      }, {
        headers: {
          'Content-Type': 'application/json'
        }
      })

      const { access_token, refresh_token, expires_in, refresh_expires_in } = response.data
      
      // 保存新的 Token
      this.saveTokens(access_token, refresh_token, expires_in, refresh_expires_in)
      
      console.log('Token refreshed successfully')
      return true
    } catch (error) {
      console.error('Token refresh failed:', error)
      return false
    }
  }

  /**
   * 带锁的刷新 Token（防止并发）
   * @returns {Promise<boolean>}
   */
  async refreshTokenWithLock() {
    if (isRefreshing) {
      // 已经在刷新中，等待刷新完成
      return new Promise((resolve) => {
        refreshSubscribers.push((success) => {
          resolve(success)
        })
      })
    }

    isRefreshing = true

    try {
      const success = await this.refreshToken()
      
      // 通知所有等待的请求
      refreshSubscribers.forEach(callback => callback(success))
      refreshSubscribers = []
      
      return success
    } finally {
      isRefreshing = false
    }
  }

  /**
   * 确保 Token 有效
   * 如果 Token 即将过期，自动刷新
   * @returns {Promise<string|null>} 有效的 Access Token 或 null
   */
  async ensureValidToken() {
    const accessToken = this.getAccessToken()
    
    if (!accessToken) {
      return null
    }

    // 如果 Token 即将过期，尝试刷新
    if (this.isAccessTokenExpiringSoon()) {
      const success = await this.refreshTokenWithLock()
      if (!success) {
        // 刷新失败，清除 Token
        this.clearTokens()
        return null
      }
      // 返回新的 Token
      return this.getAccessToken()
    }

    return accessToken
  }
}

// 导出单例
export const tokenManager = new TokenManager()

export default tokenManager

