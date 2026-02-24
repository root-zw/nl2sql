/**
 * 本地存储工具类
 * NL2SQL
 */

const PREFIX = (import.meta.env.VITE_STORAGE_PREFIX && import.meta.env.VITE_STORAGE_PREFIX.trim()) || 'NL2SQL_'

/**
 * localStorage 工具类
 */
export const local = {
  /**
   * 设置
   * @param {String} key - 键
   * @param {*} value - 值
   */
  set(key, value) {
    try {
      const data = JSON.stringify(value)
      localStorage.setItem(PREFIX + key, data)
    } catch (error) {
      console.error('localStorage set error:', error)
    }
  },
  
  /**
   * 获取
   * @param {String} key - 键
   * @param {*} defaultValue - 默认值
   * @returns {*}
   */
  get(key, defaultValue = null) {
    try {
      const data = localStorage.getItem(PREFIX + key)
      return data ? JSON.parse(data) : defaultValue
    } catch (error) {
      console.error('localStorage get error:', error)
      return defaultValue
    }
  },
  
  /**
   * 删除
   * @param {String} key - 键
   */
  remove(key) {
    try {
      localStorage.removeItem(PREFIX + key)
    } catch (error) {
      console.error('localStorage remove error:', error)
    }
  },
  
  /**
   * 清空
   */
  clear() {
    try {
      // 只清空带前缀的项
      Object.keys(localStorage).forEach(key => {
        if (key.startsWith(PREFIX)) {
          localStorage.removeItem(key)
        }
      })
    } catch (error) {
      console.error('localStorage clear error:', error)
    }
  }
}

/**
 * sessionStorage 工具类
 */
export const session = {
  /**
   * 设置
   * @param {String} key - 键
   * @param {*} value - 值
   */
  set(key, value) {
    try {
      const data = JSON.stringify(value)
      sessionStorage.setItem(PREFIX + key, data)
    } catch (error) {
      console.error('sessionStorage set error:', error)
    }
  },
  
  /**
   * 获取
   * @param {String} key - 键
   * @param {*} defaultValue - 默认值
   * @returns {*}
   */
  get(key, defaultValue = null) {
    try {
      const data = sessionStorage.getItem(PREFIX + key)
      return data ? JSON.parse(data) : defaultValue
    } catch (error) {
      console.error('sessionStorage get error:', error)
      return defaultValue
    }
  },
  
  /**
   * 删除
   * @param {String} key - 键
   */
  remove(key) {
    try {
      sessionStorage.removeItem(PREFIX + key)
    } catch (error) {
      console.error('sessionStorage remove error:', error)
    }
  },
  
  /**
   * 清空
   */
  clear() {
    try {
      // 只清空带前缀的项
      Object.keys(sessionStorage).forEach(key => {
        if (key.startsWith(PREFIX)) {
          sessionStorage.removeItem(key)
        }
      })
    } catch (error) {
      console.error('sessionStorage clear error:', error)
    }
  }
}

/**
 * Cookie 工具类
 */
export const cookie = {
  /**
   * 设置
   * @param {String} key - 键
   * @param {String} value - 值
   * @param {Number} days - 过期天数
   */
  set(key, value, days = 7) {
    try {
      const expires = new Date()
      expires.setTime(expires.getTime() + days * 24 * 60 * 60 * 1000)
      document.cookie = `${PREFIX}${key}=${value};expires=${expires.toUTCString()};path=/`
    } catch (error) {
      console.error('cookie set error:', error)
    }
  },
  
  /**
   * 获取
   * @param {String} key - 键
   * @returns {String|null}
   */
  get(key) {
    try {
      const name = PREFIX + key + '='
      const cookies = document.cookie.split(';')
      for (let cookie of cookies) {
        cookie = cookie.trim()
        if (cookie.indexOf(name) === 0) {
          return cookie.substring(name.length)
        }
      }
      return null
    } catch (error) {
      console.error('cookie get error:', error)
      return null
    }
  },
  
  /**
   * 删除
   * @param {String} key - 键
   */
  remove(key) {
    this.set(key, '', -1)
  },
  
  /**
   * 清空（清空所有带前缀的cookie）
   */
  clear() {
    try {
      const cookies = document.cookie.split(';')
      cookies.forEach(cookie => {
        const name = cookie.split('=')[0].trim()
        if (name.startsWith(PREFIX)) {
          const key = name.substring(PREFIX.length)
          this.remove(key)
        }
      })
    } catch (error) {
      console.error('cookie clear error:', error)
    }
  }
}

/**
 * Token 管理
 */
export const token = {
  /**
   * 设置 Token
   * @param {String} value - Token值
   */
  set(value) {
    local.set('token', value)
  },
  
  /**
   * 获取 Token
   * @returns {String|null}
   */
  get() {
    return local.get('token')
  },
  
  /**
   * 删除 Token
   */
  remove() {
    local.remove('token')
  }
}

/**
 * 用户信息管理
 */
export const user = {
  /**
   * 设置用户信息
   * @param {Object} value - 用户信息
   */
  set(value) {
    local.set('user', value)
  },
  
  /**
   * 获取用户信息
   * @returns {Object|null}
   */
  get() {
    return local.get('user')
  },
  
  /**
   * 删除用户信息
   */
  remove() {
    local.remove('user')
  }
}

export default {
  local,
  session,
  cookie,
  token,
  user
}

