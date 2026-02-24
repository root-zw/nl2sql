/**
 * 元数据加载器 Composable
 * 统一管理业务域、表、字段的加载，提供一致的数据过滤和错误处理
 */

import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import { domainAPI, tableAPI, fieldAPI } from '@/api'

export function useMetadataLoader() {
  // 响应式数据
  const domains = ref([])
  const tables = ref([])
  const fields = ref([])
  
  // 加载状态
  const loading = ref({
    domains: false,
    tables: false,
    fields: false
  })

  /**
   * 加载业务域列表
   * @param {string} connectionId - 数据库连接ID
   * @param {object} options - 加载选项
   * @param {boolean} options.onlyActive - 是否只加载激活的业务域（默认true）
   * @param {boolean} options.silent - 是否静默加载（不显示错误消息，默认false）
   */
  const loadDomains = async (connectionId, options = {}) => {
    const { onlyActive = true, silent = false } = options
    
    if (!connectionId) {
      console.warn('[useMetadataLoader] loadDomains: connectionId为空')
      return []
    }

    loading.value.domains = true
    try {
      const params = { connection_id: connectionId }
      if (onlyActive) {
        params.is_active = true
      }

      const response = await domainAPI.list(params)
      const data = response.data?.data || response.data || []
      domains.value = Array.isArray(data) ? data : []
      
      console.log(`[useMetadataLoader] 加载业务域: ${domains.value.length} 个`)
      return domains.value
    } catch (error) {
      console.error('[useMetadataLoader] 加载业务域失败:', error)
      if (!silent) {
        ElMessage.error('加载业务域列表失败')
      }
      domains.value = []
      return []
    } finally {
      loading.value.domains = false
    }
  }

  /**
   * 加载表列表
   * @param {string} connectionId - 数据库连接ID
   * @param {object} options - 加载选项
   * @param {boolean} options.onlyIncluded - 是否只加载已启用的表（默认true）
   * @param {string} options.domainId - 按业务域过滤（可选）
   * @param {boolean} options.silent - 是否静默加载（默认false）
   */
  const loadTables = async (connectionId, options = {}) => {
    const { onlyIncluded = true, domainId = null, silent = false } = options
    
    if (!connectionId) {
      console.warn('[useMetadataLoader] loadTables: connectionId为空')
      return []
    }

    loading.value.tables = true
    try {
      const response = await tableAPI.list({ connection_id: connectionId })
      const data = response.data?.data || response.data || []
      let tableList = Array.isArray(data) ? data : []
      
      // 过滤掉未启用的表（is_included=false）
      if (onlyIncluded) {
        tableList = tableList.filter(table => table.is_included !== false)
      }
      
      // 按业务域过滤
      if (domainId) {
        tableList = tableList.filter(table => table.domain_id === domainId)
      }
      
      tables.value = tableList
      
      console.log(`[useMetadataLoader] 加载表: ${tables.value.length} 张` + 
                  (onlyIncluded ? ' (仅启用)' : ' (全部)'))
      
      if (onlyIncluded && data.length > tableList.length) {
        console.log(`[useMetadataLoader] 过滤掉 ${data.length - tableList.length} 张禁用的表`)
      }
      
      return tables.value
    } catch (error) {
      console.error('[useMetadataLoader] 加载表列表失败:', error)
      if (!silent) {
        ElMessage.error('加载表列表失败')
      }
      tables.value = []
      return []
    } finally {
      loading.value.tables = false
    }
  }

  /**
   * 加载字段列表
   * @param {string} connectionId - 数据库连接ID
   * @param {object} options - 加载选项
   * @param {boolean} options.onlyActive - 是否只加载激活的字段（默认true）
   * @param {string} options.fieldType - 按字段类型过滤（可选: 'dimension', 'measure', 'identifier'等）
   * @param {string} options.tableName - 按表名过滤（可选）
   * @param {boolean} options.silent - 是否静默加载（默认false）
   */
  const loadFields = async (connectionId, options = {}) => {
    const { onlyActive = true, fieldType = null, tableName = null, silent = false } = options
    
    if (!connectionId) {
      console.warn('[useMetadataLoader] loadFields: connectionId为空')
      return []
    }

    loading.value.fields = true
    try {
      const params = { connection_id: connectionId }
      if (onlyActive) {
        params.is_active = true
      }
      if (fieldType) {
        params.field_type = fieldType
      }

      const response = await fieldAPI.list(params)
      const data = response.data?.data || response.data || []
      let fieldList = Array.isArray(data) ? data : []
      
      // 按表名过滤（如果指定）
      if (tableName) {
        fieldList = fieldList.filter(field => field.table_name === tableName)
      }
      
      fields.value = fieldList
      
      console.log(`[useMetadataLoader] 加载字段: ${fields.value.length} 个` +
                  (fieldType ? ` (类型: ${fieldType})` : '') +
                  (onlyActive ? ' (仅激活)' : ' (全部)'))
      
      return fields.value
    } catch (error) {
      console.error('[useMetadataLoader] 加载字段列表失败:', error)
      if (!silent) {
        ElMessage.error('加载字段列表失败')
      }
      fields.value = []
      return []
    } finally {
      loading.value.fields = false
    }
  }

  /**
   * 一次性加载所有元数据
   * @param {string} connectionId - 数据库连接ID
   * @param {object} options - 加载选项
   */
  const loadAll = async (connectionId, options = {}) => {
    if (!connectionId) {
      console.warn('[useMetadataLoader] loadAll: connectionId为空')
      return { domains: [], tables: [], fields: [] }
    }

    console.log(`[useMetadataLoader] 开始加载所有元数据 (connectionId: ${connectionId})`)
    
    // 并行加载
    const [domainsData, tablesData, fieldsData] = await Promise.all([
      loadDomains(connectionId, options),
      loadTables(connectionId, options),
      loadFields(connectionId, options)
    ])

    console.log(`[useMetadataLoader] 所有元数据加载完成`)
    
    return {
      domains: domainsData,
      tables: tablesData,
      fields: fieldsData
    }
  }

  /**
   * 重置所有数据
   */
  const reset = () => {
    domains.value = []
    tables.value = []
    fields.value = []
    loading.value = {
      domains: false,
      tables: false,
      fields: false
    }
  }

  /**
   * 获取表的显示名称（带回退逻辑）
   */
  const getTableDisplayName = (table) => {
    if (!table) return '未知表'
    return table.display_name || table.table_name || table.name || '未命名表'
  }

  /**
   * 获取字段的显示名称（带回退逻辑）
   */
  const getFieldDisplayName = (field) => {
    if (!field) return '未知字段'
    return field.display_name || field.column_name || field.field_name || '未命名字段'
  }

  /**
   * 获取业务域的显示名称（带回退逻辑）
   */
  const getDomainDisplayName = (domain) => {
    if (!domain) return '未知业务域'
    return domain.domain_name || domain.name || '未命名业务域'
  }

  /**
   * 根据表名获取表对象
   */
  const getTableByName = (tableName) => {
    return tables.value.find(t => t.table_name === tableName)
  }

  /**
   * 根据表ID获取表对象
   */
  const getTableById = (tableId) => {
    return tables.value.find(t => t.table_id === tableId)
  }

  /**
   * 获取指定表的字段列表
   */
  const getFieldsByTable = (tableName) => {
    return fields.value.filter(f => f.table_name === tableName)
  }

  /**
   * 获取指定类型的字段列表
   */
  const getFieldsByType = (fieldType) => {
    return fields.value.filter(f => f.field_type === fieldType)
  }

  return {
    // 响应式数据
    domains,
    tables,
    fields,
    loading,
    
    // 加载方法
    loadDomains,
    loadTables,
    loadFields,
    loadAll,
    reset,
    
    // 工具方法
    getTableDisplayName,
    getFieldDisplayName,
    getDomainDisplayName,
    getTableByName,
    getTableById,
    getFieldsByTable,
    getFieldsByType
  }
}

