/**
 * 时间日期工具函数
 * 统一前端时间处理和格式化
 * 
 * 系统时区：Asia/Shanghai (UTC+8)
 */

/**
 * 格式化日期时间
 * @param {string|Date} datetime - 时间对象或ISO字符串
 * @param {string} format - 格式化模板，默认 'YYYY-MM-DD HH:mm:ss'
 * @returns {string} 格式化后的字符串
 */
export function formatDateTime(datetime, format = 'YYYY-MM-DD HH:mm:ss') {
  if (!datetime) return '-'
  
  const date = typeof datetime === 'string' ? new Date(datetime) : datetime
  
  if (isNaN(date.getTime())) {
    return '-'
  }
  
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  const seconds = String(date.getSeconds()).padStart(2, '0')
  
  return format
    .replace('YYYY', year)
    .replace('MM', month)
    .replace('DD', day)
    .replace('HH', hours)
    .replace('mm', minutes)
    .replace('ss', seconds)
}

/**
 * 格式化日期（不含时间）
 * @param {string|Date} datetime - 时间对象或ISO字符串
 * @returns {string} 格式化后的日期字符串
 */
export function formatDate(datetime) {
  return formatDateTime(datetime, 'YYYY-MM-DD')
}

/**
 * 格式化时间（不含日期）
 * @param {string|Date} datetime - 时间对象或ISO字符串
 * @returns {string} 格式化后的时间字符串
 */
export function formatTime(datetime) {
  return formatDateTime(datetime, 'HH:mm:ss')
}

/**
 * 格式化为相对时间（如：3分钟前、2小时前）
 * @param {string|Date} datetime - 时间对象或ISO字符串
 * @returns {string} 相对时间字符串
 */
export function formatRelativeTime(datetime) {
  if (!datetime) return '-'
  
  const date = typeof datetime === 'string' ? new Date(datetime) : datetime
  
  if (isNaN(date.getTime())) {
    return '-'
  }
  
  const now = new Date()
  const diff = Math.floor((now - date) / 1000) // 秒
  
  if (diff < 60) {
    return '刚刚'
  } else if (diff < 3600) {
    return `${Math.floor(diff / 60)}分钟前`
  } else if (diff < 86400) {
    return `${Math.floor(diff / 3600)}小时前`
  } else if (diff < 2592000) {
    return `${Math.floor(diff / 86400)}天前`
  } else if (diff < 31536000) {
    return `${Math.floor(diff / 2592000)}个月前`
  } else {
    return `${Math.floor(diff / 31536000)}年前`
  }
}

/**
 * 解析ISO字符串为Date对象
 * @param {string} isoString - ISO格式的时间字符串
 * @returns {Date|null} Date对象或null
 */
export function parseDateTime(isoString) {
  if (!isoString) return null
  
  const date = new Date(isoString)
  return isNaN(date.getTime()) ? null : date
}

/**
 * 获取当前时间的ISO字符串
 * @returns {string} ISO格式的时间字符串
 */
export function nowISO() {
  return new Date().toISOString()
}

/**
 * 获取今天的开始时间（00:00:00）
 * @returns {Date} 今天开始时间
 */
export function getTodayStart() {
  const now = new Date()
  return new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0)
}

/**
 * 获取今天的结束时间（23:59:59）
 * @returns {Date} 今天结束时间
 */
export function getTodayEnd() {
  const now = new Date()
  return new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59, 999)
}

/**
 * 获取N天前的日期
 * @param {number} days - 天数
 * @returns {Date} N天前的日期
 */
export function getDaysAgo(days) {
  const now = new Date()
  return new Date(now.getTime() - days * 24 * 60 * 60 * 1000)
}

/**
 * 获取N天后的日期
 * @param {number} days - 天数
 * @returns {Date} N天后的日期
 */
export function getDaysLater(days) {
  const now = new Date()
  return new Date(now.getTime() + days * 24 * 60 * 60 * 1000)
}

/**
 * 检查是否为有效的日期
 * @param {any} date - 待检查的值
 * @returns {boolean} 是否为有效日期
 */
export function isValidDate(date) {
  if (!date) return false
  const d = date instanceof Date ? date : new Date(date)
  return !isNaN(d.getTime())
}

/**
 * 比较两个日期（忽略时间）
 * @param {Date|string} date1 - 日期1
 * @param {Date|string} date2 - 日期2
 * @returns {number} 1: date1 > date2, 0: 相等, -1: date1 < date2
 */
export function compareDates(date1, date2) {
  const d1 = typeof date1 === 'string' ? new Date(date1) : date1
  const d2 = typeof date2 === 'string' ? new Date(date2) : date2
  
  const t1 = new Date(d1.getFullYear(), d1.getMonth(), d1.getDate()).getTime()
  const t2 = new Date(d2.getFullYear(), d2.getMonth(), d2.getDate()).getTime()
  
  if (t1 > t2) return 1
  if (t1 < t2) return -1
  return 0
}

/**
 * 格式化时长（秒数转为可读格式）
 * @param {number} seconds - 秒数
 * @returns {string} 格式化后的时长字符串
 */
export function formatDuration(seconds) {
  if (seconds < 60) {
    return `${seconds}秒`
  } else if (seconds < 3600) {
    const mins = Math.floor(seconds / 60)
    const secs = seconds % 60
    return secs > 0 ? `${mins}分${secs}秒` : `${mins}分钟`
  } else if (seconds < 86400) {
    const hours = Math.floor(seconds / 3600)
    const mins = Math.floor((seconds % 3600) / 60)
    return mins > 0 ? `${hours}小时${mins}分钟` : `${hours}小时`
  } else {
    const days = Math.floor(seconds / 86400)
    const hours = Math.floor((seconds % 86400) / 3600)
    return hours > 0 ? `${days}天${hours}小时` : `${days}天`
  }
}

/**
 * Element Plus 日期选择器配置
 */
export const datePickerShortcuts = [
  {
    text: '今天',
    value: () => new Date()
  },
  {
    text: '昨天',
    value: () => getDaysAgo(1)
  },
  {
    text: '最近7天',
    value: () => [getDaysAgo(7), new Date()]
  },
  {
    text: '最近30天',
    value: () => [getDaysAgo(30), new Date()]
  },
  {
    text: '本月',
    value: () => {
      const now = new Date()
      return [
        new Date(now.getFullYear(), now.getMonth(), 1),
        new Date(now.getFullYear(), now.getMonth() + 1, 0)
      ]
    }
  },
  {
    text: '上个月',
    value: () => {
      const now = new Date()
      return [
        new Date(now.getFullYear(), now.getMonth() - 1, 1),
        new Date(now.getFullYear(), now.getMonth(), 0)
      ]
    }
  }
]

export default {
  formatDateTime,
  formatDate,
  formatTime,
  formatRelativeTime,
  parseDateTime,
  nowISO,
  getTodayStart,
  getTodayEnd,
  getDaysAgo,
  getDaysLater,
  isValidDate,
  compareDates,
  formatDuration,
  datePickerShortcuts
}

