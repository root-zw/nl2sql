/**
 * 日期时间格式化工具
 * 统一使用中国时区 Asia/Shanghai
 */

/**
 * 格式化日期时间（完整格式）
 * @param {string|Date} date - 日期对象或ISO字符串
 * @returns {string} 格式：2024-10-27 16:30:45
 */
export function formatDateTime(date) {
  if (!date) return '-'
  
  try {
    return new Date(date).toLocaleString('zh-CN', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    })
  } catch (error) {
    console.error('日期格式化失败:', error)
    return '-'
  }
}

/**
 * 格式化日期（仅日期）
 * @param {string|Date} date - 日期对象或ISO字符串
 * @returns {string} 格式：2024-10-27
 */
export function formatDate(date) {
  if (!date) return '-'
  
  try {
    return new Date(date).toLocaleDateString('zh-CN', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    })
  } catch (error) {
    console.error('日期格式化失败:', error)
    return '-'
  }
}

/**
 * 格式化时间（仅时间）
 * @param {string|Date} date - 日期对象或ISO字符串
 * @returns {string} 格式：16:30:45
 */
export function formatTime(date) {
  if (!date) return '-'
  
  try {
    return new Date(date).toLocaleTimeString('zh-CN', {
      timeZone: 'Asia/Shanghai',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    })
  } catch (error) {
    console.error('时间格式化失败:', error)
    return '-'
  }
}

/**
 * 相对时间（如：3分钟前）
 * @param {string|Date} date - 日期对象或ISO字符串
 * @returns {string}
 */
export function formatRelativeTime(date) {
  if (!date) return '-'
  
  try {
    const now = new Date()
    const target = new Date(date)
    const diff = Math.floor((now - target) / 1000) // 秒
    
    if (diff < 60) return '刚刚'
    if (diff < 3600) return `${Math.floor(diff / 60)}分钟前`
    if (diff < 86400) return `${Math.floor(diff / 3600)}小时前`
    if (diff < 604800) return `${Math.floor(diff / 86400)}天前`
    
    return formatDateTime(date)
  } catch (error) {
    console.error('相对时间格式化失败:', error)
    return '-'
  }
}

export default {
  formatDateTime,
  formatDate,
  formatTime,
  formatRelativeTime
}

