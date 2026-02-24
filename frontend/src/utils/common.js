/**
 * 通用工具函数
 * NL2SQL
 */

const DEFAULT_DELAY = Number.isFinite(Number(import.meta.env.VITE_DEBOUNCE_DELAY))
  ? Number(import.meta.env.VITE_DEBOUNCE_DELAY)
  : 300

/**
 * 格式化日期时间
 * @param {String|Date} date - 日期
 * @param {String} format - 格式，默认 'YYYY-MM-DD HH:mm:ss'
 * @returns {String}
 */
export function formatDate(date, format = 'YYYY-MM-DD HH:mm:ss') {
  if (!date) return '-'
  
  const d = new Date(date)
  if (isNaN(d.getTime())) return '-'
  
  const year = d.getFullYear()
  const month = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  const hour = String(d.getHours()).padStart(2, '0')
  const minute = String(d.getMinutes()).padStart(2, '0')
  const second = String(d.getSeconds()).padStart(2, '0')
  
  return format
    .replace('YYYY', year)
    .replace('MM', month)
    .replace('DD', day)
    .replace('HH', hour)
    .replace('mm', minute)
    .replace('ss', second)
}

/**
 * 格式化文件大小
 * @param {Number} bytes - 字节数
 * @returns {String}
 */
export function formatFileSize(bytes) {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return (bytes / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i]
}

/**
 * 格式化耗时（智能转换：毫秒、秒、分钟）
 * @param {Number} ms - 毫秒数
 * @returns {String}
 */
export function formatDuration(ms) {
  if (ms === null || ms === undefined || isNaN(ms)) return '-'
  
  const milliseconds = Number(ms)
  
  // 小于1秒，显示毫秒
  if (milliseconds < 1000) {
    return `${Math.round(milliseconds)}ms`
  }
  
  // 小于1分钟，显示秒（保留1位小数）
  if (milliseconds < 60000) {
    const seconds = milliseconds / 1000
    return `${seconds.toFixed(1)}s`
  }
  
  // 大于等于1分钟，显示分钟和秒
  const minutes = Math.floor(milliseconds / 60000)
  const remainingSeconds = Math.floor((milliseconds % 60000) / 1000)
  
  if (remainingSeconds === 0) {
    return `${minutes}分钟`
  }
  return `${minutes}分钟${remainingSeconds}秒`
}

/**
 * 格式化数字（千分位）
 * @param {Number} num - 数字
 * @param {Number} precision - 小数位数
 * @returns {String}
 */
export function formatNumber(num, precision = 2) {
  if (num === null || num === undefined) return '-'
  return Number(num).toLocaleString('zh-CN', {
    minimumFractionDigits: 0,
    maximumFractionDigits: precision
  })
}

/**
 * 防抖函数
 * @param {Function} fn - 要防抖的函数
 * @param {Number} delay - 延迟时间（毫秒）
 * @returns {Function}
 */
export function debounce(fn, delay = DEFAULT_DELAY) {
  let timer = null
  return function (...args) {
    if (timer) clearTimeout(timer)
    timer = setTimeout(() => {
      fn.apply(this, args)
    }, delay)
  }
}

/**
 * 节流函数
 * @param {Function} fn - 要节流的函数
 * @param {Number} delay - 延迟时间（毫秒）
 * @returns {Function}
 */
export function throttle(fn, delay = DEFAULT_DELAY) {
  let lastTime = 0
  return function (...args) {
    const now = Date.now()
    if (now - lastTime >= delay) {
      lastTime = now
      fn.apply(this, args)
    }
  }
}

/**
 * 深拷贝
 * @param {*} obj - 要拷贝的对象
 * @returns {*}
 */
export function deepClone(obj) {
  if (obj === null || typeof obj !== 'object') return obj
  if (obj instanceof Date) return new Date(obj)
  if (obj instanceof Array) return obj.map(item => deepClone(item))
  
  const clonedObj = {}
  for (const key in obj) {
    if (obj.hasOwnProperty(key)) {
      clonedObj[key] = deepClone(obj[key])
    }
  }
  return clonedObj
}

/**
 * 生成唯一ID
 * @returns {String}
 */
export function generateId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2)
}

/**
 * 下载文件
 * @param {Blob} blob - 文件Blob
 * @param {String} filename - 文件名
 */
export function downloadFile(blob, filename) {
  const url = window.URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  window.URL.revokeObjectURL(url)
}

/**
 * 复制到剪贴板
 * @param {String} text - 要复制的文本
 * @returns {Promise}
 */
export function copyToClipboard(text) {
  if (navigator.clipboard) {
    return navigator.clipboard.writeText(text)
  } else {
    // 兼容旧浏览器
    const textarea = document.createElement('textarea')
    textarea.value = text
    textarea.style.position = 'fixed'
    textarea.style.opacity = '0'
    document.body.appendChild(textarea)
    textarea.select()
    document.execCommand('copy')
    document.body.removeChild(textarea)
    return Promise.resolve()
  }
}

/**
 * 获取URL参数
 * @param {String} name - 参数名
 * @returns {String|null}
 */
export function getUrlParam(name) {
  const reg = new RegExp('(^|&)' + name + '=([^&]*)(&|$)')
  const r = window.location.search.substr(1).match(reg)
  if (r != null) return decodeURIComponent(r[2])
  return null
}

/**
 * 高亮文本中的关键词
 * @param {String} text - 原文本
 * @param {String} keyword - 关键词
 * @returns {String}
 */
export function highlightKeyword(text, keyword) {
  if (!keyword) return text
  const reg = new RegExp(keyword, 'gi')
  return text.replace(reg, match => `<span class="highlight">${match}</span>`)
}

/**
 * 验证邮箱
 * @param {String} email - 邮箱地址
 * @returns {Boolean}
 */
export function isValidEmail(email) {
  const reg = /^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/
  return reg.test(email)
}

/**
 * 验证手机号
 * @param {String} phone - 手机号
 * @returns {Boolean}
 */
export function isValidPhone(phone) {
  const reg = /^1[3-9]\d{9}$/
  return reg.test(phone)
}

/**
 * 验证URL
 * @param {String} url - URL地址
 * @returns {Boolean}
 */
export function isValidUrl(url) {
  const reg = /^(https?:\/\/)?([\da-z.-]+)\.([a-z.]{2,6})([/\w .-]*)*\/?$/
  return reg.test(url)
}

/**
 * 验证密码强度
 * 要求：最少8位，需要大写字母、小写字母、数字、特殊字符至少三种组合
 * @param {String} password - 密码
 * @returns {Object} { valid: boolean, message: string }
 */
export function validatePasswordStrength(password) {
  if (!password) {
    return { valid: false, message: '密码不能为空' }
  }
  
  // 检查长度
  if (password.length < 8) {
    return { valid: false, message: '密码长度至少为8位' }
  }
  
  // 检查字符类型
  const hasUpper = /[A-Z]/.test(password)
  const hasLower = /[a-z]/.test(password)
  const hasDigit = /\d/.test(password)
  const hasSpecial = /[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/.test(password)
  
  // 统计包含的字符类型数量
  const typeCount = [hasUpper, hasLower, hasDigit, hasSpecial].filter(Boolean).length
  
  if (typeCount < 3) {
    const missingTypes = []
    if (!hasUpper) missingTypes.push('大写字母')
    if (!hasLower) missingTypes.push('小写字母')
    if (!hasDigit) missingTypes.push('数字')
    if (!hasSpecial) missingTypes.push('特殊字符')
    
    return { 
      valid: false, 
      message: `密码必须包含大写字母、小写字母、数字、特殊字符中的至少三种（当前缺少：${missingTypes.join('、')}）` 
    }
  }
  
  return { valid: true, message: '' }
}

/**
 * 获取文件扩展名
 * @param {String} filename - 文件名
 * @returns {String}
 */
export function getFileExtension(filename) {
  return filename.slice((filename.lastIndexOf('.') - 1 >>> 0) + 2)
}

/**
 * 首字母大写
 * @param {String} str - 字符串
 * @returns {String}
 */
export function capitalize(str) {
  if (!str) return ''
  return str.charAt(0).toUpperCase() + str.slice(1)
}

/**
 * 驼峰转下划线
 * @param {String} str - 驼峰字符串
 * @returns {String}
 */
export function camelToSnake(str) {
  return str.replace(/([A-Z])/g, '_$1').toLowerCase()
}

/**
 * 下划线转驼峰
 * @param {String} str - 下划线字符串
 * @returns {String}
 */
export function snakeToCamel(str) {
  return str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase())
}

/**
 * 计算百分比
 * @param {Number} value - 当前值
 * @param {Number} total - 总值
 * @param {Number} precision - 小数位数
 * @returns {String}
 */
export function calculatePercentage(value, total, precision = 2) {
  if (!total || total === 0) return '0%'
  return ((value / total) * 100).toFixed(precision) + '%'
}

/**
 * 数组去重
 * @param {Array} arr - 数组
 * @param {String} key - 对象数组的去重键
 * @returns {Array}
 */
export function uniqueArray(arr, key) {
  if (!key) {
    return [...new Set(arr)]
  }
  const map = new Map()
  return arr.filter(item => !map.has(item[key]) && map.set(item[key], 1))
}

/**
 * 树形数据扁平化
 * @param {Array} tree - 树形数据
 * @param {String} childrenKey - 子节点键名
 * @returns {Array}
 */
export function flattenTree(tree, childrenKey = 'children') {
  const result = []
  const flatten = (nodes) => {
    nodes.forEach(node => {
      result.push(node)
      if (node[childrenKey] && node[childrenKey].length) {
        flatten(node[childrenKey])
      }
    })
  }
  flatten(tree)
  return result
}

/**
 * 数组转树形结构
 * @param {Array} arr - 扁平数组
 * @param {String} idKey - ID键名
 * @param {String} pidKey - 父ID键名
 * @param {String} childrenKey - 子节点键名
 * @returns {Array}
 */
/**
 * 检查 JWT token 是否过期
 * @param {String} token - JWT token
 * @returns {Object} { expired: boolean, expiresAt: Date|null, message: string }
 */
export function checkTokenExpiration(token) {
  if (!token) {
    return { expired: true, expiresAt: null, message: 'Token 不存在' }
  }

  try {
    // JWT token 格式：header.payload.signature
    const parts = token.split('.')
    if (parts.length !== 3) {
      return { expired: true, expiresAt: null, message: 'Token 格式无效' }
    }

    // 解码 payload（Base64 URL 解码）
    const payload = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')))
    
    // 检查是否有过期时间
    if (!payload.exp) {
      return { expired: false, expiresAt: null, message: 'Token 无过期时间' }
    }

    // exp 是 Unix 时间戳（秒），转换为 Date 对象
    const expiresAt = new Date(payload.exp * 1000)
    const now = new Date()
    const expired = now >= expiresAt

    return {
      expired,
      expiresAt,
      message: expired 
        ? `Token 已于 ${expiresAt.toLocaleString('zh-CN')} 过期` 
        : `Token 将在 ${expiresAt.toLocaleString('zh-CN')} 过期`
    }
  } catch (error) {
    console.error('检查 token 过期时间失败:', error)
    return { expired: true, expiresAt: null, message: 'Token 解析失败' }
  }
}

export function arrayToTree(arr, idKey = 'id', pidKey = 'pid', childrenKey = 'children') {
  const map = {}
  const tree = []
  
  arr.forEach(item => {
    map[item[idKey]] = { ...item, [childrenKey]: [] }
  })
  
  arr.forEach(item => {
    const node = map[item[idKey]]
    if (item[pidKey] && map[item[pidKey]]) {
      map[item[pidKey]][childrenKey].push(node)
    } else {
      tree.push(node)
    }
  })
  
  return tree
}

export default {
  formatDate,
  formatFileSize,
  formatNumber,
  formatDuration,
  debounce,
  throttle,
  deepClone,
  generateId,
  downloadFile,
  copyToClipboard,
  getUrlParam,
  highlightKeyword,
  isValidEmail,
  isValidPhone,
  isValidUrl,
  getFileExtension,
  capitalize,
  camelToSnake,
  snakeToCamel,
  calculatePercentage,
  uniqueArray,
  flattenTree,
  arrayToTree,
  validatePasswordStrength
}

