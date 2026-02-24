/**
 * 全局常量配置
 * NL2SQL
 */

const env = import.meta.env
const parseEnvNumber = (value, fallback) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

// 数据库类型
export const DB_TYPES = [
  { label: 'SQL Server', value: 'sqlserver', icon: '🗄️', color: '#CC2927' },
  { label: 'MySQL', value: 'mysql', icon: '🐬', color: '#00758F' },
  { label: 'PostgreSQL', value: 'postgresql', icon: '🐘', color: '#336791' }
]

// 字段类型
export const FIELD_TYPES = [
  { label: '维度', value: 'dimension', icon: '📊', color: '#409EFF', tag: 'primary' },
  { label: '度量', value: 'measure', icon: '📈', color: '#67C23A', tag: 'success' },
  { label: '时间戳', value: 'timestamp', icon: '⏰', color: '#E6A23C', tag: 'warning' },
  { label: '标识', value: 'identifier', icon: '🔑', color: '#909399', tag: 'info' }
]

// 聚合函数
export const AGGREGATION_TYPES = [
  { label: 'SUM', value: 'SUM', description: '求和' },
  { label: 'AVG', value: 'AVG', description: '平均值' },
  { label: 'COUNT', value: 'COUNT', description: '计数' },
  { label: 'MAX', value: 'MAX', description: '最大值' },
  { label: 'MIN', value: 'MIN', description: '最小值' },
  { label: 'DISTINCT', value: 'DISTINCT', description: '去重计数' }
]

// 表关系类型
export const RELATIONSHIP_TYPES = [
  { label: '一对多', value: 'one_to_many', icon: '1→∞' },
  { label: '多对一', value: 'many_to_one', icon: '∞→1' },
  { label: '一对一', value: 'one_to_one', icon: '1→1' },
  { label: '多对多', value: 'many_to_many', icon: '∞→∞' }
]

// 规则类型
export const RULE_TYPES = [
  { 
    label: '派生指标', 
    value: 'derived_metric', 
    icon: '📐',
    description: '通过公式计算得出的指标',
    example: 'SUM(收入) - SUM(成本)'
  },
  { 
    label: '单位转换', 
    value: 'unit_conversion', 
    icon: '🔄',
    description: '不同单位之间的换算',
    example: '元 → 万元 (系数: 0.0001)'
  },
  { 
    label: '校验规则', 
    value: 'validation', 
    icon: '✅',
    description: '数据有效性验证',
    example: 'value >= 0'
  },
  { 
    label: '枚举映射', 
    value: 'enum_mapping', 
    icon: '🔗',
    description: '枚举值的同义词映射',
    example: '已完成 → [完成, 已完成, done]'
  }
]

// 同步状态
export const SYNC_STATUS = [
  { label: '未同步', value: 'pending', color: '#909399', tag: 'info' },
  { label: '同步中', value: 'syncing', color: '#409EFF', tag: '' },
  { label: '已同步', value: 'success', color: '#67C23A', tag: 'success' },
  { label: '失败', value: 'failed', color: '#F56C6C', tag: 'danger' }
]

// 识别方式
export const DETECTION_METHODS = [
  { label: '自动识别', value: true, tag: 'info' },
  { label: '手动配置', value: false, tag: 'success' }
]

// 确认状态
export const CONFIRM_STATUS = [
  { label: '待确认', value: false, color: '#E6A23C', tag: 'warning' },
  { label: '已确认', value: true, color: '#67C23A', tag: 'success' }
]

// 业务域图标
export const DOMAIN_ICONS = [
  '💰', '📊', '📈', '🏢', '👥', '🛒', '📦', '🚚',
  '💳', '📱', '🔧', '⚙️', '📝', '📅', '🎯', '🏆',
  '💼', '📋', '🗂️', '📁', '🔍', '📊', '💡', '🌟'
]

// 常用单位
export const COMMON_UNITS = [
  { label: '元', value: '元' },
  { label: '万元', value: '万元' },
  { label: '亿元', value: '亿元' },
  { label: '个', value: '个' },
  { label: '件', value: '件' },
  { label: '%', value: '%' },
  { label: '人', value: '人' },
  { label: '天', value: '天' },
  { label: '小时', value: '小时' },
  { label: '分钟', value: '分钟' },
  { label: 'kg', value: 'kg' },
  { label: 'm', value: 'm' },
  { label: 'm²', value: 'm²' },
  { label: 'm³', value: 'm³' }
]

// 时间格式
export const DATE_FORMATS = [
  { label: 'YYYY-MM-DD', value: 'YYYY-MM-DD' },
  { label: 'YYYY-MM-DD HH:mm:ss', value: 'YYYY-MM-DD HH:mm:ss' },
  { label: 'YYYY/MM/DD', value: 'YYYY/MM/DD' },
  { label: 'MM/DD/YYYY', value: 'MM/DD/YYYY' }
]

// 页面大小选项
export const PAGE_SIZES = [10, 20, 50, 100]

// 默认分页
export const DEFAULT_PAGINATION = {
  page: 1,
  pageSize: 20,
  total: 0
}

// 表格空数据文本
export const EMPTY_TEXT = '暂无数据'

// 成功消息持续时间（毫秒）
export const SUCCESS_DURATION = parseEnvNumber(env.VITE_SUCCESS_MESSAGE_DURATION, 3000)

// 错误消息持续时间（毫秒）
export const ERROR_DURATION = parseEnvNumber(env.VITE_ERROR_MESSAGE_DURATION, 5000)

// 请求超时时间（毫秒）
export const REQUEST_TIMEOUT = parseEnvNumber(env.VITE_REQUEST_TIMEOUT, 30000)

// Token 存储键
export const TOKEN_KEY = 'token'

// 用户信息存储键
export const USER_KEY = 'user'

// 主题存储键
export const THEME_KEY = 'theme'

// 语言存储键
export const LANG_KEY = 'lang'

// 默认主题
export const DEFAULT_THEME = 'light'

// 默认语言
export const DEFAULT_LANG = 'zh-CN'

// 支持的语言
export const LANGUAGES = [
  { label: '简体中文', value: 'zh-CN' },
  { label: 'English', value: 'en-US' }
]

// 用户角色
export const USER_ROLES = [
  { label: '超级管理员', value: 'super_admin', color: '#F56C6C' },
  { label: '管理员', value: 'admin', color: '#E6A23C' },
  { label: '普通用户', value: 'user', color: '#409EFF' },
  { label: '访客', value: 'viewer', color: '#909399' }
]

// 导出所有常量
export default {
  DB_TYPES,
  FIELD_TYPES,
  AGGREGATION_TYPES,
  RELATIONSHIP_TYPES,
  RULE_TYPES,
  SYNC_STATUS,
  DETECTION_METHODS,
  CONFIRM_STATUS,
  DOMAIN_ICONS,
  COMMON_UNITS,
  DATE_FORMATS,
  PAGE_SIZES,
  DEFAULT_PAGINATION,
  EMPTY_TEXT,
  SUCCESS_DURATION,
  ERROR_DURATION,
  REQUEST_TIMEOUT,
  TOKEN_KEY,
  USER_KEY,
  THEME_KEY,
  LANG_KEY,
  DEFAULT_THEME,
  DEFAULT_LANG,
  LANGUAGES,
  USER_ROLES
}

