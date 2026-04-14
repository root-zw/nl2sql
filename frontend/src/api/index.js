/**
 * API 服务层
 * 智能问数
 */
import request from '@/utils/request'

// ============================================================================
// 用户认证
// ============================================================================
export const authAPI = {
  // 登录
  login: (data) => request.post('/admin/login', data),
  
  // OIDC 相关
  // 获取可用的OIDC提供者列表
  getOidcProviders: () => request.get('/admin/oidc/providers'),
  // OIDC 登录跳转（可选指定provider_key）
  oidcLogin: (providerKey) => request.get('/admin/oidc/login', { 
    params: providerKey ? { provider_key: providerKey } : {} 
  }),
  // OIDC 回调（前端可在后端完成，这里预留）
  oidcCallback: (params) => request.get('/admin/oidc/callback', { params }),
  
  // OAuth 2.0 相关（钉钉、企业微信、飞书等）
  // 获取可用的 OAuth 2.0 提供者列表
  getOauth2Providers: () => request.get('/admin/oauth2/providers'),
  // OAuth 2.0 登录跳转（可选指定provider_key）
  oauth2Login: (providerKey) => request.get('/admin/oauth2/login', { 
    params: providerKey ? { provider_key: providerKey } : {} 
  }),
  
  // 获取所有 SSO 提供者（OIDC + OAuth 2.0）
  getAllSsoProviders: async () => {
    const [oidcRes, oauth2Res] = await Promise.all([
      request.get('/admin/oidc/providers').catch(() => ({ data: { providers: [] } })),
      request.get('/admin/oauth2/providers').catch(() => ({ data: { providers: [] } }))
    ])
    const oidcProviders = (oidcRes.data?.providers || []).map(p => ({ ...p, type: 'oidc' }))
    const oauth2Providers = (oauth2Res.data?.providers || []).map(p => ({ ...p, type: 'oauth2' }))
    return { providers: [...oidcProviders, ...oauth2Providers] }
  },
  
  // 获取当前用户信息
  getUserInfo: () => request.get('/admin/user/info'),
  
  // 退出登录
  logout: () => request.post('/admin/logout'),
  
  // 修改密码
  changePassword: (data) => request.post('/admin/change-password', data)
}

// ============================================================================
// 认证提供者配置
// ============================================================================
export const authProviderAPI = {
  // 获取所有提供者配置
  list: () => request.get('/admin/auth-providers'),
  // 获取当前运行时已加载的提供者
  listActive: () => request.get('/admin/auth-providers/active'),
  // 获取支持的提供者类型及配置字段说明
  getTypes: () => request.get('/admin/auth-providers/types'),
  // 创建提供者
  create: (data) => request.post('/admin/auth-providers', data),
  // 更新提供者
  update: (id, data) => request.put(`/admin/auth-providers/${id}`, data),
  // 删除提供者
  delete: (id) => request.delete(`/admin/auth-providers/${id}`),
  // 热重载配置
  reload: () => request.post('/admin/auth-providers/reload'),
  // 测试连接
  test: (id) => request.post(`/admin/auth-providers/${id}/test`),
  
  // 用户同步相关
  // 获取提供者的已同步用户
  getUsers: (id) => request.get(`/admin/auth-providers/${id}/users`),
  // 同步用户到提供者
  syncUsers: (id, users) => request.post(`/admin/auth-providers/${id}/sync-users`, { users }),
  // 获取同步统计
  getSyncStats: (id) => request.get(`/admin/auth-providers/${id}/sync-stats`),
  // 移除用户
  removeUser: (providerId, userId) => request.delete(`/admin/auth-providers/${providerId}/users/${userId}`),
  // 从外部系统获取用户列表（一键获取）
  fetchUsers: (id) => request.post(`/admin/auth-providers/${id}/fetch-users`),
  // 一键同步：自动获取并同步用户
  autoSync: (id) => request.post(`/admin/auth-providers/${id}/auto-sync`)
}

// ============================================================================
// 用户同步
// ============================================================================
export const userSyncAPI = {
  sync: (users) => request.post('/admin/users/sync', users)
}

// ============================================================================
// 数据库连接
// ============================================================================
export const connectionAPI = {
  // 获取连接列表
  list: (params) => request.get('/admin/connections', { params }),
  
  // 创建连接
  create: (data) => request.post('/admin/connections', data),
  
  // 更新连接
  update: (id, data) => request.put(`/admin/connections/${id}`, data),
  
  // 删除连接
  delete: (id) => request.delete(`/admin/connections/${id}`),
  
  // 测试连接
  test: (data) => request.post('/admin/connections/test', data),
  
  // 同步Schema
  sync: (id) => request.post(`/admin/connections/${id}/sync`),
  
  // 获取连接详情
  detail: (id) => request.get(`/admin/connections/${id}`),
  
  // 获取连接统计
  getStats: (id) => request.get(`/admin/connections/${id}/stats`),
  
  // 获取连接下的表列表
  getTables: (id, params) => request.get(`/admin/connections/${id}/tables`, { params }),
  
  // 获取表的列
  getTableColumns: (connectionId, tableId) => 
    request.get(`/admin/connections/${connectionId}/tables/${tableId}/columns`)
}

// ============================================================================
// 表管理
// ============================================================================
export const tableAPI = {
  // 获取表列表
  list: (params) => request.get('/admin/tables', { params }),
  
  // 更新表配置
  update: (id, data) => request.put(`/admin/tables/${id}`, data),
  
  // 删除表
  delete: (id) => request.delete(`/admin/tables/${id}`)
}

// ============================================================================
// 业务域
// ============================================================================
export const domainAPI = {
  // 获取业务域列表
  list: (params) => request.get('/admin/domains', { params }),
  
  // 创建业务域
  create: (data) => request.post('/admin/domains', data),
  
  // 更新业务域
  update: (id, data) => request.put(`/admin/domains/${id}`, data),
  
  // 删除业务域
  delete: (id) => request.delete(`/admin/domains/${id}`),
  
  // 自动识别业务域功能已移除
  // autoDetect: (connectionId) => request.post(`/admin/domains/auto-detect/${connectionId}`),
  
  // 同步到Milvus（新版统一入口，按连接）
  syncToMilvus: (connectionId, options = {}) => 
    request.post(`/admin/milvus/sync/${connectionId}`, options)
}

// ============================================================================
// 字段配置
// ============================================================================
export const fieldAPI = {
  // 获取字段列表
  list: (params) => request.get('/admin/fields', { params }),
  
  // 更新字段（精调）
  update: (id, data) => request.put(`/admin/fields/${id}`, data),
  
  // 删除字段
  delete: (id) => request.delete(`/admin/fields/${id}`),
  
  // 批量自动识别字段类型功能已移除
  // autoDetect: (connectionId) => request.post(`/admin/fields/auto-detect/${connectionId}`),
  
  // 自动识别字段（新接口）
  autoIdentify: (data) => request.post('/admin/fields/auto-identify', data),
  
  // 批量更新字段
  batchUpdate: (data) => request.post('/admin/fields/batch-update', data),
  
  // 获取字段枚举值
  getEnumValues: (fieldId) => request.get(`/admin/fields/${fieldId}/enum-values`),
  
  // 从样本数据生成枚举值
  sampleEnumValues: (fieldId, data) => request.post(`/admin/fields/${fieldId}/enum-values/sample`, data),
  
  // 更新枚举值
  updateEnumValue: (fieldId, enumValueId, data) => request.put(`/admin/fields/${fieldId}/enum-values/${enumValueId}`, data),
  
  // 创建枚举值
  createEnumValue: (fieldId, data) => request.post(`/admin/fields/${fieldId}/enum-values`, data),
  
  // 删除枚举值
  deleteEnumValue: (fieldId, enumValueId) => request.delete(`/admin/fields/${fieldId}/enum-values/${enumValueId}`)
}

// ============================================================================
// 表关系
// ============================================================================
export const relationshipAPI = {
  // 获取表关系列表
  list: (params) => request.get('/admin/relationships', { params }),
  
  // 创建表关系
  create: (data) => request.post('/admin/relationships', data),
  
  // 更新表关系
  update: (id, data) => request.put(`/admin/relationships/${id}`, data),
  
  // 删除表关系
  delete: (id) => request.delete(`/admin/relationships/${id}`),
  
  // 自动识别表关系
  autoDetect: (connectionId) => request.post(`/admin/relationships/auto-detect/${connectionId}`),
  
  // 确认表关系
  confirm: (id) => request.put(`/admin/relationships/${id}/confirm`),
  
  // 预览SQL
  previewSQL: (id) => request.get(`/admin/relationships/${id}/preview-sql`)
}

// ============================================================================
// 全局规则
// ============================================================================
export const ruleAPI = {
  // 获取规则列表
  list: (params) => request.get('/admin/rules', { params }),
  
  // 创建规则
  create: (data) => request.post('/admin/rules', data),
  
  // 更新规则
  update: (id, data) => request.put(`/admin/rules/${id}`, data),
  
  // 删除规则
  delete: (id) => request.delete(`/admin/rules/${id}`),
  
  // 获取规则模板
  getTemplates: () => request.get('/admin/rules/templates'),
  
  // 测试规则
  test: (data) => request.post('/admin/rules/test', data)
}

// ============================================================================
// Milvus同步
// ============================================================================
export const milvusAPI = {
  // 检查Milvus健康状态
  checkHealth: () => request.get('/admin/milvus/health'),
  
  // 同步到Milvus（全量/增量）- 按连接
  sync: (connectionId, data) => request.post(`/admin/milvus/sync/${connectionId}`, data),
  
  // 同步所有元数据到Milvus（不区分连接）
  syncAll: (data = {}) => request.post('/admin/milvus/sync-all', data),
  
  // 清空Milvus数据
  clear: (connectionId) => request.delete(`/admin/milvus/clear/${connectionId}`),
  
  // 获取Milvus统计（按连接）
  getStats: (connectionId) => request.get(`/admin/milvus/stats/${connectionId}`),
  
  // 获取所有元数据的Milvus统计（不区分连接）
  getStatsAll: () => request.get('/admin/milvus/stats-all'),
  
  // 获取枚举值同步统计
  getEnumSyncStats: (connectionId) => request.get(`/admin/milvus/enum-sync-stats/${connectionId}`),

  // 全局同步配置
  getGlobalSettings: () => request.get('/admin/milvus/global-settings'),
  updateGlobalSettings: (data) => request.post('/admin/milvus/global-settings', data)
}

// ============================================================================
// 智能问答
// ============================================================================
export const queryAPI = {
  // 提交查询
  query: (data) => request.post('/query', data),
  
  // 获取查询历史
  getHistory: (params) => request.get('/admin/history', { params }),
  
  // 删除历史记录
  deleteHistory: (id) => request.delete(`/admin/history/${id}`)
}

// ============================================================================
// 会话管理（多轮对话）
// ============================================================================
export const conversationAPI = {
  // 创建会话
  create: (data) => request.post('/conversations', data),
  
  // 获取会话列表
  list: (params) => request.get('/conversations', { params }),
  
  // 获取会话详情（包含消息历史）
  get: (conversationId, params) => request.get(`/conversations/${conversationId}`, { params }),
  
  // 更新会话
  update: (conversationId, data) => request.patch(`/conversations/${conversationId}`, data),
  
  // 删除会话
  delete: (conversationId, hardDelete = false) => 
    request.delete(`/conversations/${conversationId}`, { params: { hard_delete: hardDelete } }),
  
  // 获取会话上下文
  getContext: (conversationId, depth) =>
    request.get(`/conversations/${conversationId}/context`, { params: { depth } }),

  // 停止消息生成
  stopMessage: (messageId) => request.post('/conversations/messages/stop', { message_id: messageId }),
  
  // 获取正在执行的查询
  getRunningQueries: () => request.get('/conversations/queries/running')
}

// ============================================================================
// 系统管理
// ============================================================================
export const systemAPI = {
  // 健康检查
  health: () => request.get('/health'),

  // 获取检索统计
  getRetrievalStats: () => request.get('/admin/retrieval-stats'),

  // 重置检索统计
  resetRetrievalStats: () => request.post('/admin/retrieval-stats/reset'),

  // 获取系统信息
  getSystemInfo: () => request.get('/')
}

// ============================================================================
// 元数据导入导出
// ============================================================================
export const metadataIOAPI = {
  // 导出元数据模板
  // connectionIds: 可选，为空时导出所有数据源；支持逗号分隔的多个ID
  // options.table_name: 指定表名，只导出该表的配置（支持逗号分隔多个表）
  exportTemplate: (connectionIds, options = {}) => {
    const params = {}
    if (connectionIds) params.connection_ids = connectionIds
    if (options.table_name) params.table_name = options.table_name
    if (options.include_domains !== undefined) params.include_domains = options.include_domains
    if (options.include_tables !== undefined) params.include_tables = options.include_tables
    if (options.include_fields !== undefined) params.include_fields = options.include_fields
    if (options.include_enums !== undefined) params.include_enums = options.include_enums
    if (options.include_relationships !== undefined) params.include_relationships = options.include_relationships
    if (options.include_rules !== undefined) params.include_rules = options.include_rules

    // 通过 axios 发起请求以便自动附带认证头
    return request.get('/admin/metadata/export', {
      params,
      responseType: 'blob'
    })
  },

  // 导入元数据（预览模式）- 单数据源
  importPreview: (connectionId, file, mode = 'update') => {
    const formData = new FormData()
    formData.append('file', file)
    return request.post(`/admin/metadata/import/${connectionId}?mode=${mode}&dry_run=true`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
  },

  // 导入元数据（实际执行）- 单数据源
  importExecute: (connectionId, file, mode = 'update') => {
    const formData = new FormData()
    formData.append('file', file)
    return request.post(`/admin/metadata/import/${connectionId}?mode=${mode}&dry_run=false`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
  },

  // 统一导入元数据（预览模式）- 支持多数据源自动分发
  // connectionIds: 可选，为空时根据Excel中的数据源列自动分发
  importUnifiedPreview: (connectionIds, file, mode = 'update') => {
    const formData = new FormData()
    formData.append('file', file)
    const params = new URLSearchParams()
    if (connectionIds) params.append('connection_ids', connectionIds)
    params.append('mode', mode)
    params.append('dry_run', 'true')
    return request.post(`/admin/metadata/import?${params.toString()}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
  },

  // 统一导入元数据（实际执行）- 支持多数据源自动分发
  importUnifiedExecute: (connectionIds, file, mode = 'update') => {
    const formData = new FormData()
    formData.append('file', file)
    const params = new URLSearchParams()
    if (connectionIds) params.append('connection_ids', connectionIds)
    params.append('mode', mode)
    params.append('dry_run', 'false')
    return request.post(`/admin/metadata/import?${params.toString()}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
  }
}

// ============================================================================
// 统一元数据管理（跨数据源）
// ============================================================================
export const unifiedMetadataAPI = {
  // 获取统一元数据树
  getTree: (options = {}) => {
    const params = new URLSearchParams()
    if (options.include_inactive) params.append('include_inactive', 'true')
    if (options.search) params.append('search', options.search)
    const queryString = params.toString()
    return request.get(`/admin/unified-metadata/tree${queryString ? '?' + queryString : ''}`)
  },

  // 获取表详情（包含字段）
  getTableDetail: (tableId) => 
    request.get(`/admin/unified-metadata/tables/${tableId}`),

  // 更新表的业务域关联
  updateTableDomain: (tableId, domainId) => 
    request.put(`/admin/unified-metadata/tables/${tableId}/domain?domain_id=${domainId || ''}`),

  // 批量将表分配到业务域
  batchAssignTables: (domainId, tableIds) => 
    request.post(`/admin/unified-metadata/tables/batch-assign-domain?domain_id=${domainId}`, tableIds),

  // 获取连接摘要
  getConnectionsSummary: () => 
    request.get('/admin/unified-metadata/connections/summary')
}

// ============================================================================
// 组织架构管理
// ============================================================================
export const organizationAPI = {
  // 获取组织列表
  list: (params) => request.get('/admin/organizations', { params }),
  // 获取组织树
  getTree: (params) => request.get('/admin/organizations/tree', { params }),
  // 获取根组织
  getRoots: () => request.get('/admin/organizations/roots'),
  // 创建组织
  create: (data) => request.post('/admin/organizations', data),
  // 获取组织详情
  get: (id) => request.get(`/admin/organizations/${id}`),
  // 更新组织
  update: (id, data) => request.put(`/admin/organizations/${id}`, data),
  // 删除组织
  delete: (id) => request.delete(`/admin/organizations/${id}`),
  // 获取子组织
  getChildren: (id) => request.get(`/admin/organizations/${id}/children`),
  // 获取组织成员
  getMembers: (id, params) => request.get(`/admin/organizations/${id}/members`, { params }),
  // 获取未分配用户
  getUnassignedUsers: (params) => request.get('/admin/organizations/users/unassigned', { params }),
  // 分配用户到组织
  assignUser: (userId, orgId, position) => request.put(`/admin/organizations/users/${userId}/org`, null, { 
    params: { org_id: orgId, position } 
  }),
  // 批量分配用户
  batchAssignUsers: (data) => request.put('/admin/organizations/users/batch-assign', data),
  // 获取用户组织信息
  getUserOrg: (userId) => request.get(`/admin/organizations/users/${userId}`),
  // 获取组织的数据角色
  getOrgRoles: (orgId) => request.get(`/admin/organizations/${orgId}/data-roles`),
  // 为组织分配数据角色
  assignOrgRole: (orgId, data) => request.post(`/admin/organizations/${orgId}/data-roles`, data),
  // 移除组织的数据角色
  removeOrgRole: (orgId, roleId) => request.delete(`/admin/organizations/${orgId}/data-roles/${roleId}`),
  // 获取拥有某角色的组织
  getRoleOrganizations: (roleId) => request.get(`/admin/organizations/data-roles/${roleId}/organizations`),
  // 获取用户有效权限（含组织继承）
  getUserEffectiveRoles: (userId) => request.get(`/admin/organizations/users/${userId}/effective-roles`),
  // 同步外部组织
  syncExternal: (data) => request.post('/admin/organizations/sync', data),
  // 按来源获取组织
  getBySource: (sourceIdp) => request.get(`/admin/organizations/by-source/${sourceIdp}`)
}

// ============================================================================
// 自动同步管理
// ============================================================================
export const autoSyncAPI = {
  // 触发自动同步
  triggerAutoSync: (connectionId, entityChanges) =>
    request.post(`/admin/auto-sync/trigger`, {
      connection_id: connectionId,
      entity_changes: entityChanges
    }),

  // 触发手动同步
  triggerManualSync: (connectionId, options = {}) =>
    request.post(`/admin/manual-sync/${connectionId}`, options),

  // 获取同步状态
  getSyncStatus: (syncId) => request.get(`/admin/sync-status/${syncId}`),

  // 获取待同步变更
  getPendingChanges: (connectionId, options = {}) =>
    request.get(`/admin/pending-changes/${connectionId}`, { params: options }),

  // 获取同步健康状态
  getSyncHealth: (connectionId) => request.get(`/admin/sync-health/${connectionId}`),

  // 取消同步任务
  cancelSync: (syncId) => request.delete(`/admin/sync/${syncId}`),

  // 获取同步历史
  getSyncHistory: (connectionId, options = {}) =>
    request.get(`/admin/sync-history/${connectionId}`, { params: options }),

  // 获取队列状态
  getQueueStatus: () => request.get('/admin/queue-status'),

  // 更新同步配置
  updateSyncConfig: (connectionId, config) =>
    request.post(`/admin/sync-config/${connectionId}`, config),

  // 获取同步配置
  getSyncConfig: (connectionId) => request.get(`/admin/sync-config/${connectionId}`)
}

// ============================================================================
// 模型供应商管理
// ============================================================================
export const modelProviderAPI = {
  // 获取预置供应商列表
  getPresets: () => request.get('/admin/model-providers/presets'),
  
  // 从预置模板添加供应商
  addFromPreset: (presetName, data) => 
    request.post(`/admin/model-providers/presets/${presetName}/add`, null, { params: data }),
  
  // 获取供应商列表
  list: (includeDisabled = false) => 
    request.get('/admin/model-providers', { params: { include_disabled: includeDisabled } }),
  
  // 获取供应商详情
  get: (providerId) => request.get(`/admin/model-providers/${providerId}`),
  
  // 创建供应商
  create: (data) => request.post('/admin/model-providers', data),
  
  // 更新供应商
  update: (providerId, data) => request.put(`/admin/model-providers/${providerId}`, data),
  
  // 删除供应商
  delete: (providerId) => request.delete(`/admin/model-providers/${providerId}`),
  
  // --- 凭证管理 ---
  // 获取凭证列表
  listCredentials: (providerId, includeInactive = false) =>
    request.get(`/admin/model-providers/${providerId}/credentials`, { 
      params: { include_inactive: includeInactive } 
    }),
  
  // 创建凭证
  createCredential: (providerId, data) => 
    request.post(`/admin/model-providers/${providerId}/credentials`, data),
  
  // 更新凭证
  updateCredential: (providerId, credentialId, data) =>
    request.put(`/admin/model-providers/${providerId}/credentials/${credentialId}`, data),
  
  // 删除凭证
  deleteCredential: (providerId, credentialId) =>
    request.delete(`/admin/model-providers/${providerId}/credentials/${credentialId}`),
  
  // 设置默认凭证
  setDefaultCredential: (providerId, credentialId) =>
    request.post(`/admin/model-providers/${providerId}/credentials/${credentialId}/set-default`),
  
  // --- 模型管理 ---
  // 获取供应商的模型列表
  listModels: (providerId, modelType = null, includeDisabled = false) =>
    request.get(`/admin/model-providers/${providerId}/models`, { 
      params: { model_type: modelType, include_disabled: includeDisabled } 
    }),
  
  // 从供应商API获取可用模型列表（不保存到数据库）
  fetchModels: (providerId, credentialId = null) =>
    request.get(`/admin/model-providers/${providerId}/models/fetch`, {
      params: { credential_id: credentialId }
    }),
  
  // 从供应商API同步模型列表到数据库
  syncModels: (providerId, credentialId = null, modelType = null) => {
    const params = {}
    if (credentialId) params.credential_id = credentialId
    if (modelType) params.model_type = modelType
    return request.post(`/admin/model-providers/${providerId}/models/sync`, null, { params })
  },
  
  // 创建模型
  createModel: (providerId, data) => 
    request.post(`/admin/model-providers/${providerId}/models`, data),
  
  // 更新模型
  updateModel: (providerId, modelId, data) =>
    request.put(`/admin/model-providers/${providerId}/models/${modelId}`, data),
  
  // 删除模型
  deleteModel: (providerId, modelId) =>
    request.delete(`/admin/model-providers/${providerId}/models/${modelId}`),
  
  // --- 可用模型（选择器用）---
  getAvailableModels: (modelType = null, scenario = null) =>
    request.get('/admin/model-providers/available-models', { 
      params: { model_type: modelType, scenario } 
    })
}

// ============================================================================
// 场景模型配置
// ============================================================================
export const scenarioConfigAPI = {
  // 获取所有场景配置
  list: () => request.get('/admin/scenario-configs'),
  
  // 获取指定场景配置
  get: (scenario) => request.get(`/admin/scenario-configs/${scenario}`),
  
  // 更新场景配置
  update: (scenario, data) => request.put(`/admin/scenario-configs/${scenario}`, data)
}

// 注意：默认模型配置已合并到场景配置中，使用 scenario=default

// ============================================================================
// 提示词模板管理
// ============================================================================
export const promptAPI = {
  // 获取场景列表
  getScenarios: () => request.get('/admin/prompts/scenarios'),
  
  // 获取提示词类型枚举
  getTypes: () => request.get('/admin/prompts/types'),
  
  // 获取提示词详情
  get: (scenario, promptType, includeFile = true) => 
    request.get(`/admin/prompts/${scenario}/${promptType}`, { 
      params: { include_file: includeFile } 
    }),
  
  // 保存提示词
  save: (scenario, promptType, data) => 
    request.put(`/admin/prompts/${scenario}/${promptType}`, data),
  
  // 切换激活状态
  toggle: (scenario, promptType, isActive) => 
    request.patch(`/admin/prompts/${scenario}/${promptType}/toggle`, { is_active: isActive }),
  
  // 从文件同步
  syncFromFile: (scenario, promptType) => 
    request.post(`/admin/prompts/${scenario}/${promptType}/sync-from-file`),
  
  // 导出到文件
  exportToFile: (scenario, promptType) => 
    request.post(`/admin/prompts/${scenario}/${promptType}/export-to-file`),
  
  // 获取历史记录
  getHistory: (scenario, promptType, limit = 10) => 
    request.get(`/admin/prompts/${scenario}/${promptType}/history`, { params: { limit } }),
  
  // 回滚到指定版本
  rollback: (scenario, promptType, version) => 
    request.post(`/admin/prompts/${scenario}/${promptType}/rollback`, { version })
}

// 导出所有API
export default {
  auth: authAPI,
  connection: connectionAPI,
  table: tableAPI,
  domain: domainAPI,
  field: fieldAPI,
  relationship: relationshipAPI,
  rule: ruleAPI,
  milvus: milvusAPI,
  metadataIO: metadataIOAPI,
  autoSync: autoSyncAPI,
  organization: organizationAPI,
  query: queryAPI,
  conversation: conversationAPI,
  system: systemAPI,
  modelProvider: modelProviderAPI,
  scenarioConfig: scenarioConfigAPI,
  prompt: promptAPI
}
