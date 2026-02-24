<template>
  <div class="auth-provider-manage">
    <!-- 头部操作栏 -->
    <div class="card-header">
      <div class="header-left">
        <span class="title">🔐 认证提供者配置</span>
        <el-tag type="info" size="small" class="ml-8">
          运行中: {{ activeProviders.length }} 个
        </el-tag>
      </div>
      <div class="header-right">
        <el-button type="success" size="small" @click="reloadProviders" :loading="reloading">
          <el-icon><Refresh /></el-icon>
          重载配置
        </el-button>
        <el-button type="primary" size="small" @click="openCreate">
          <el-icon><Plus /></el-icon>
          新增提供者
        </el-button>
      </div>
    </div>

    <!-- 运行时状态提示 -->
    <el-alert
      v-if="activeProviders.length > 0"
      type="success"
      :closable="false"
      class="mb-12"
    >
      <template #title>
        <span>（添加或修改认证提供者后请点击重载配置）当前运行中的认证提供者：</span>
        <el-tag 
          v-for="p in activeProviders" 
          :key="p.name" 
          :type="p.enabled ? 'success' : 'info'"
          size="small"
          class="ml-8"
        >
          {{ p.name }} (优先级: {{ p.priority }})
        </el-tag>
      </template>
    </el-alert>

    <!-- 提供者列表 -->
    <el-table :data="providers" size="small" style="width: 100%" v-loading="loading">
      <el-table-column prop="provider_key" label="标识" width="150" />
      <el-table-column prop="provider_type" label="类型" width="120">
        <template #default="{ row }">
          <el-tag :type="getTypeColor(row.provider_type)">
            {{ getTypeName(row.provider_type) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="priority" label="优先级" width="80" />
      <el-table-column prop="enabled" label="启用" width="70">
        <template #default="{ row }">
          <el-switch 
            v-model="row.enabled" 
            @change="toggleEnabled(row)"
            size="small"
          />
        </template>
      </el-table-column>
      <el-table-column label="已同步用户" width="100">
        <template #default="{ row }">
          <el-tag v-if="row.syncStats" type="info" size="small">
            {{ row.syncStats.total_users || 0 }}
          </el-tag>
          <span v-else class="text-muted">-</span>
        </template>
      </el-table-column>
      <el-table-column label="配置摘要">
        <template #default="{ row }">
          <div class="config-summary">
            <template v-if="row.provider_type === 'oidc'">
              <span v-if="row.config_json?.issuer_url">
                Issuer: {{ truncate(row.config_json.issuer_url, 35) }}
              </span>
            </template>
            <template v-else-if="row.provider_type === 'ldap'">
              <span v-if="row.config_json?.server">
                Server: {{ truncate(row.config_json.server, 35) }}
              </span>
            </template>
            <template v-else-if="row.provider_type === 'api_gateway'">
              <span v-if="row.config_json?.trusted_ips">
                可信IP: {{ truncate(row.config_json.trusted_ips, 35) }}
              </span>
              <span v-else>已配置签名验证</span>
            </template>
            <template v-else-if="row.provider_type === 'oauth2'">
              <span v-if="row.config_json?.authorization_endpoint">
                {{ truncate(row.config_json.authorization_endpoint, 35) }}
              </span>
            </template>
            <template v-else-if="row.provider_type === 'external_aes'">
              <span>
                {{ row.config_json?.algorithm || 'AES-128-CBC' }} | 
                {{ row.config_json?.token_format === 'simple' ? '简单模式' : 
                   row.config_json?.token_format === 'with_user' ? '完整用户模式' : 
                   row.config_json?.token_format === 'with_username' ? '用户名模式' : 
                   row.config_json?.token_format || '简单模式' }} |
                有效期: {{ row.config_json?.validity_minutes || 5 }}分钟
              </span>
            </template>
            <template v-else>
              <span class="text-muted">本地JWT认证</span>
            </template>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="280" fixed="right">
        <template #default="{ row }">
          <div class="action-btns">
            <el-button type="primary" link size="small" @click="openEdit(row)">
              编辑
            </el-button>
            <el-button 
              type="success" 
              link 
              size="small" 
              @click="testConnection(row)"
              :loading="row.testing"
            >
              测试
            </el-button>
            <el-button 
              v-if="row.provider_type !== 'local'"
              type="warning" 
              link 
              size="small" 
              @click="openSyncDialog(row)"
            >
              同步用户
            </el-button>
            <el-button 
              v-if="row.provider_type !== 'local' && row.provider_key !== 'local'"
              type="danger" 
              link 
              size="small" 
              @click="openDeleteDialog(row)"
            >
              删除
            </el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>

    <!-- 新增/编辑对话框 -->
    <el-dialog 
      v-model="dialogVisible" 
      :title="dialogMode === 'create' ? '新增认证提供者' : '编辑认证提供者'" 
      width="70%"
      destroy-on-close
      class="provider-dialog"
    >
      <el-form :model="form" :rules="rules" ref="formRef" label-width="120px" class="provider-form">
        <!-- 基本信息 -->
        <div class="form-grid basic-grid">
          <el-form-item label="标识" prop="provider_key">
            <el-input 
              v-model="form.provider_key" 
              :disabled="dialogMode === 'edit'" 
              placeholder="唯一标识，如 oidc_main"
            />
          </el-form-item>
          
          <el-form-item label="类型" prop="provider_type">
            <el-select 
              v-model="form.provider_type" 
              placeholder="选择认证类型"
              :disabled="dialogMode === 'edit'"
              @change="onTypeChange"
              style="width: 100%"
            >
              <el-option 
                v-for="t in providerTypes" 
                :key="t.type" 
                :label="t.name" 
                :value="t.type"
              >
                <span>{{ t.name }}</span>
                <span class="option-desc">{{ t.description }}</span>
              </el-option>
            </el-select>
          </el-form-item>
          
          <el-form-item label="优先级" prop="priority">
            <el-input-number v-model="form.priority" :min="1" :max="1000" />
            <span class="form-tip">数值越大优先级越高</span>
          </el-form-item>
          
          <el-form-item label="启用" class="switch-row">
            <el-switch v-model="form.enabled" />
          </el-form-item>
        </div>

        <el-divider>配置详情</el-divider>

        <!-- 动态配置字段 -->
        <template v-if="currentTypeConfig">
          <!-- 开关配置：集中排列，放在最上面 -->
          <div class="form-grid switch-grid" v-if="booleanConfigFields.length">
            <el-form-item 
              v-for="field in booleanConfigFields" 
              :key="field.name"
              :label="field.label"
              :prop="`config.${field.name}`"
              :required="field.required"
              class="config-item"
            >
              <el-switch v-model="form.config[field.name]" />
              <div class="form-tip" v-if="field.description">
                {{ field.description }}
              </div>
            </el-form-item>
          </div>

          <!-- 非开关配置 -->
          <div class="form-grid config-grid">
            <el-form-item 
              v-for="field in nonBooleanConfigFields" 
              :key="field.name"
              :label="field.label"
              :prop="`config.${field.name}`"
              :required="field.required"
              :class="['config-item', { 'full-row': field.type === 'json' || field.name === 'redirect_uri' }]"
            >
              <el-input 
                v-if="field.type === 'string'" 
                v-model="form.config[field.name]"
                :placeholder="field.placeholder || ''"
              />
              <el-input 
                v-else-if="field.type === 'password'" 
                v-model="form.config[field.name]"
                type="password"
                show-password
                :placeholder="field.placeholder || ''"
              />
              <template v-else-if="field.type === 'json'">
                <el-input 
                  v-model="form.configJsonStrings[field.name]"
                  type="textarea"
                  :rows="3"
                  :placeholder="JSON.stringify(field.default || {}, null, 2)"
                />
                <div class="form-tip" v-if="field.description">{{ field.description }}</div>
              </template>
              <el-input 
                v-else 
                v-model="form.config[field.name]"
                :placeholder="field.placeholder || ''"
              />
              <div class="form-tip" v-if="field.description && field.type !== 'json'">
                {{ field.description }}
              </div>
            </el-form-item>
          </div>
        </template>

        <el-alert 
          v-if="form.provider_type === 'local'"
          title="本地认证使用系统内置的用户名密码 + JWT 机制，无需额外配置"
          type="info"
          :closable="false"
        />

        <!-- 同步配置（非本地认证时显示） -->
        <template v-if="form.provider_type !== 'local'">
          <el-divider>同步配置</el-divider>
          
          <el-collapse v-model="syncConfigExpanded">
            <el-collapse-item title="用户同步配置" name="user_sync">
              <div class="sync-config-section">
                <el-form-item label="启用用户同步" class="switch-item">
                  <el-switch v-model="form.syncConfig.sync_users_enabled" />
                  <span class="form-tip">开启后可从外部系统同步用户到本地</span>
                </el-form-item>
                
                <template v-if="form.syncConfig.sync_users_enabled">
                  <!-- 认证类型特定的同步配置字段 -->
                  <div class="form-grid config-grid" v-if="syncConfigFields.length > 0">
                    <el-form-item 
                      v-for="field in syncConfigFields" 
                      :key="field.name"
                      :label="field.label"
                      :class="{ 'full-row': field.type === 'json' }"
                    >
                      <el-input 
                        v-if="field.type === 'string'" 
                        v-model="form.config[field.name]"
                        :placeholder="field.placeholder || ''"
                      />
                      <el-input 
                        v-else-if="field.type === 'password'" 
                        v-model="form.config[field.name]"
                        type="password"
                        show-password
                        :placeholder="field.placeholder || ''"
                      />
                      <template v-else-if="field.type === 'json'">
                        <el-input 
                          v-model="form.configJsonStrings[field.name]"
                          type="textarea"
                          :rows="3"
                          :placeholder="JSON.stringify(field.default || {}, null, 2)"
                        />
                      </template>
                      <div class="form-tip" v-if="field.description">{{ field.description }}</div>
                    </el-form-item>
                  </div>

                  <el-divider content-position="left" v-if="syncConfigFields.length > 0">字段映射</el-divider>

                  <div class="form-grid config-grid">
                    <el-form-item label="用户名字段">
                      <el-input 
                        v-model="form.syncConfig.user_mapping.username_field" 
                        placeholder="preferred_username"
                      />
                    </el-form-item>
                    <el-form-item label="邮箱字段">
                      <el-input 
                        v-model="form.syncConfig.user_mapping.email_field" 
                        placeholder="email"
                      />
                    </el-form-item>
                    <el-form-item label="姓名字段">
                      <el-input 
                        v-model="form.syncConfig.user_mapping.full_name_field" 
                        placeholder="name"
                      />
                    </el-form-item>
                    <el-form-item label="组织字段">
                      <el-input 
                        v-model="form.syncConfig.user_mapping.org_field" 
                        placeholder="department（可选）"
                      />
                      <div class="form-tip">用户所属组织的字段名，用于自动分配组织</div>
                    </el-form-item>
                  </div>
                </template>
              </div>
            </el-collapse-item>

            <el-collapse-item title="组织同步配置" name="org_sync">
              <div class="sync-config-section">
                <el-form-item label="启用组织同步" class="switch-item">
                  <el-switch v-model="form.syncConfig.sync_orgs_enabled" />
                  <span class="form-tip">开启后可从外部系统同步组织架构</span>
                </el-form-item>
                
                <template v-if="form.syncConfig.sync_orgs_enabled">
                  <div class="form-grid config-grid">
                    <el-form-item label="组织ID字段">
                      <el-input 
                        v-model="form.syncConfig.org_mapping.id_field" 
                        placeholder="org_id"
                      />
                    </el-form-item>
                    <el-form-item label="组织编码字段">
                      <el-input 
                        v-model="form.syncConfig.org_mapping.code_field" 
                        placeholder="org_code（可选）"
                      />
                    </el-form-item>
                    <el-form-item label="组织名称字段">
                      <el-input 
                        v-model="form.syncConfig.org_mapping.name_field" 
                        placeholder="org_name"
                      />
                    </el-form-item>
                    <el-form-item label="父组织字段">
                      <el-input 
                        v-model="form.syncConfig.org_mapping.parent_field" 
                        placeholder="parent_org_id（可选）"
                      />
                    </el-form-item>
                  </div>
                  
                  <el-form-item label="组织列表接口" class="full-row">
                    <el-input 
                      v-model="form.syncConfig.orgs_endpoint" 
                      placeholder="/api/organizations（可选，用于获取组织列表）"
                    />
                  </el-form-item>
                </template>
              </div>
            </el-collapse-item>
          </el-collapse>
        </template>
      </el-form>

      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submit" :loading="submitting">保存</el-button>
      </template>
    </el-dialog>

    <!-- 用户同步对话框 -->
    <el-dialog 
      v-model="syncDialogVisible" 
      :title="`用户同步 - ${currentProvider?.provider_key || ''}`"
      width="800px"
      destroy-on-close
    >
      <el-tabs v-model="syncTab">
        <!-- 已同步用户列表 -->
        <el-tab-pane label="已同步用户" name="users">
          <div class="sync-stats" v-if="syncStats">
            <el-descriptions :column="4" border size="small">
              <el-descriptions-item label="总用户数">{{ syncStats.total_users }}</el-descriptions-item>
              <el-descriptions-item label="活跃用户">{{ syncStats.active_users }}</el-descriptions-item>
              <el-descriptions-item label="已登录">{{ syncStats.logged_in_users }}</el-descriptions-item>
              <el-descriptions-item label="最后同步">{{ syncStats.last_sync_at || '-' }}</el-descriptions-item>
            </el-descriptions>
          </div>
          
          <el-table :data="syncedUsers" size="small" max-height="400" v-loading="loadingUsers" class="mt-12">
            <el-table-column prop="username" label="用户名" width="120" />
            <el-table-column prop="email" label="邮箱" width="180" show-overflow-tooltip />
            <el-table-column prop="full_name" label="姓名" width="100" />
            <el-table-column prop="role" label="角色" width="100">
              <template #default="{ row }">
                <el-tag :type="getRoleColor(row.role)" size="small">{{ getRoleLabel(row.role) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="is_active" label="状态" width="80">
              <template #default="{ row }">
                <el-tag :type="row.is_active ? 'success' : 'info'" size="small">
                  {{ row.is_active ? '启用' : '禁用' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="last_login_at" label="最后登录" width="160">
              <template #default="{ row }">
                {{ row.last_login_at || '-' }}
              </template>
            </el-table-column>
            <el-table-column label="操作" width="80">
              <template #default="{ row }">
                <el-popconfirm title="确认禁用此用户？" @confirm="removeUser(row.user_id)">
                  <template #reference>
                    <el-button type="danger" link size="small" :disabled="!row.is_active">禁用</el-button>
                  </template>
                </el-popconfirm>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- 同步新用户 -->
        <el-tab-pane label="同步新用户" name="sync">
          <!-- 一键同步区域 -->
          <el-card shadow="never" class="mb-12">
            <template #header>
              <div class="card-header-mini">
                <span>🚀 一键同步</span>
                <el-tag type="success" size="small">推荐</el-tag>
              </div>
            </template>
            <p class="sync-desc">自动从外部认证系统获取用户列表并同步到本地，无需手动配置。</p>
            <div class="sync-actions">
              <el-button type="success" @click="autoSyncUsers" :loading="autoSyncing">
                <el-icon><Refresh /></el-icon>
                一键同步用户
              </el-button>
              <el-button type="primary" plain @click="fetchExternalUsers" :loading="fetching">
                <el-icon><Download /></el-icon>
                仅获取用户列表
              </el-button>
            </div>
            <div v-if="fetchResult" class="fetch-result mt-12">
              <el-alert 
                :type="fetchResult.success ? 'success' : 'error'" 
                :closable="false"
              >
                <template #title>{{ fetchResult.message }}</template>
              </el-alert>
            </div>
          </el-card>

          <el-divider>或手动输入用户数据</el-divider>
          
          <el-alert 
            type="info" 
            :closable="false"
            class="mb-12"
          >
            <template #title>
              输入要同步的用户列表（JSON格式），同步后用户默认角色为"普通用户"
            </template>
          </el-alert>
          
          <el-input
            v-model="syncPayload"
            type="textarea"
            :rows="10"
            placeholder='[
  {
    "external_uid": "user-001",
    "username": "zhangsan",
    "email": "zhangsan@example.com",
    "full_name": "张三"
  }
]'
          />
          
          <div class="sync-actions mt-12">
            <el-button type="primary" @click="submitSync" :loading="syncing">
              <el-icon><Upload /></el-icon>
              提交同步
            </el-button>
          </div>
          
          <div v-if="syncResult" class="sync-result mt-12">
            <el-alert :type="syncResult.skipped > 0 ? 'warning' : 'success'" :closable="false">
              <template #title>
                同步完成：创建 {{ syncResult.created }} 个，更新 {{ syncResult.updated }} 个，跳过 {{ syncResult.skipped }} 个
              </template>
            </el-alert>
          </div>
        </el-tab-pane>
      </el-tabs>
    </el-dialog>

    <!-- 删除确认弹框 -->
    <el-dialog 
      v-model="deleteDialogVisible" 
      title="确认删除" 
      width="420px"
      destroy-on-close
    >
      <div>
        确认删除认证提供者 
        <strong>{{ deleteTarget?.provider_key || '' }}</strong>
        吗？删除后会同时清理该提供者同步的用户。
      </div>
      <template #footer>
        <el-button @click="deleteDialogVisible = false">取消</el-button>
        <el-button type="danger" :loading="deleting" @click="confirmDelete">删除</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { Plus, Refresh, Upload, Download } from '@element-plus/icons-vue'
import { authProviderAPI } from '@/api'

// 状态
const providers = ref([])
const activeProviders = ref([])
const providerTypes = ref([])
const loading = ref(false)
const reloading = ref(false)
const submitting = ref(false)
const dialogVisible = ref(false)
const dialogMode = ref('create')
const formRef = ref()

// 同步相关状态
const syncDialogVisible = ref(false)
const syncTab = ref('users')
const currentProvider = ref(null)
const syncedUsers = ref([])
const syncStats = ref(null)
const loadingUsers = ref(false)
const syncPayload = ref('')
const syncing = ref(false)
const syncResult = ref(null)
const autoSyncing = ref(false)
const fetching = ref(false)
const fetchResult = ref(null)
const deleteDialogVisible = ref(false)
const deleteTarget = ref(null)
const deleting = ref(false)

// 同步配置展开状态
const syncConfigExpanded = ref([])

// 默认同步配置
const defaultSyncConfig = () => ({
  sync_users_enabled: false,
  user_mapping: {
    username_field: 'preferred_username',
    email_field: 'email',
    full_name_field: 'name',
    org_field: ''
  },
  sync_orgs_enabled: false,
  org_mapping: {
    id_field: 'org_id',
    code_field: 'org_code',
    name_field: 'org_name',
    parent_field: 'parent_org_id'
  },
  orgs_endpoint: ''
})

// 表单
const form = ref({
  provider_key: '',
  provider_type: 'local',
  priority: 100,
  enabled: true,
  config: {},
  configJsonStrings: {},
  syncConfig: defaultSyncConfig()
})

const rules = ref({
  provider_key: [
    { required: true, message: '请输入标识', trigger: 'blur' },
    { min: 2, max: 100, message: '长度在 2 到 100 个字符', trigger: 'blur' }
  ],
  provider_type: [
    { required: true, message: '请选择类型', trigger: 'change' }
  ]
})

const buildConfigRules = () => {
  const baseRules = {
  provider_key: [
    { required: true, message: '请输入标识', trigger: 'blur' },
    { min: 2, max: 100, message: '长度在 2 到 100 个字符', trigger: 'blur' }
  ],
  provider_type: [
    { required: true, message: '请选择类型', trigger: 'change' }
  ]
  }

  const typeConfig = currentTypeConfig.value
  if (typeConfig) {
    for (const field of typeConfig.config_fields) {
      if (field.required) {
        baseRules[`config.${field.name}`] = [
          { required: true, message: `${field.label}为必填项`, trigger: 'blur' }
        ]
      }
    }
  }

  rules.value = baseRules
}

const currentTypeConfig = computed(() => {
  return providerTypes.value.find(t => t.type === form.value.provider_type)
})

const nonBooleanConfigFields = computed(() => {
  const cfg = currentTypeConfig.value
  return cfg ? cfg.config_fields.filter(f => f.type !== 'boolean') : []
})

const booleanConfigFields = computed(() => {
  const cfg = currentTypeConfig.value
  return cfg ? cfg.config_fields.filter(f => f.type === 'boolean') : []
})

// 同步配置字段（根据认证类型动态获取）
const syncConfigFields = computed(() => {
  const cfg = currentTypeConfig.value
  return cfg?.sync_config_fields || []
})

watch(
  currentTypeConfig,
  () => {
    buildConfigRules()
  },
  { immediate: true }
)

// 加载提供者列表
const loadProviders = async () => {
  loading.value = true
  try {
    const [res, activeRes] = await Promise.all([
      authProviderAPI.list(),
      authProviderAPI.listActive()
    ])
    // 处理响应数据（兼容axios响应对象和直接数据）
    const listData = res?.data ?? res
    const activeData = activeRes?.data ?? activeRes
    const list = Array.isArray(listData) ? listData : []
    
    // 获取每个提供者的同步统计
    for (const p of list) {
      p.testing = false
      if (p.provider_type !== 'local') {
        try {
          const statsRes = await authProviderAPI.getSyncStats(p.provider_id)
          p.syncStats = statsRes?.data ?? statsRes
        } catch {
          p.syncStats = null
        }
      }
    }
    
    providers.value = list
    activeProviders.value = Array.isArray(activeData) ? activeData : []
  } catch (e) {
    console.error('加载提供者列表失败:', e)
    providers.value = []
    activeProviders.value = []
    ElMessage.error('加载提供者列表失败')
  } finally {
    loading.value = false
  }
}

// 加载类型配置
const loadTypes = async () => {
  try {
    const res = await authProviderAPI.getTypes()
    // 处理响应数据（兼容axios响应对象和直接数据）
    const data = res?.data ?? res
    console.log('Auth provider types response:', data)
    providerTypes.value = data?.types || []
    console.log('Loaded provider types:', providerTypes.value)
  } catch (e) {
    console.error('加载类型配置失败:', e)
    // 使用默认类型作为fallback
    providerTypes.value = [
      { type: 'local', name: '本地认证', config_fields: [] },
      { type: 'oidc', name: 'OIDC/SSO', config_fields: [] },
      { type: 'api_gateway', name: 'API网关', config_fields: [] },
      { type: 'ldap', name: 'LDAP/AD', config_fields: [] }
    ]
  }
}

// 重载配置
const reloadProviders = async () => {
  reloading.value = true
  try {
    const res = await authProviderAPI.reload()
    const data = res?.data ?? res
    ElMessage.success(data.message || '配置已重载')
    activeProviders.value = data.providers || []
  } catch (e) {
    ElMessage.error('重载失败: ' + (e.response?.data?.detail || e.message))
  } finally {
    reloading.value = false
  }
}

// 打开新增对话框
const openCreate = () => {
  dialogMode.value = 'create'
  form.value = {
    provider_key: '',
    provider_type: 'local',
    priority: 100,
    enabled: true,
    config: {},
    configJsonStrings: {},
    syncConfig: defaultSyncConfig()
  }
  syncConfigExpanded.value = []
  initConfigDefaults()
  dialogVisible.value = true
}

// 打开编辑对话框
const openEdit = (row) => {
  dialogMode.value = 'edit'
  const configJsonStrings = {}
  const typeConfig = providerTypes.value.find(t => t.type === row.provider_type)
  if (typeConfig) {
    // 处理 config_fields 中的 JSON 字段
    for (const field of typeConfig.config_fields) {
      if (field.type === 'json' && row.config_json?.[field.name]) {
        configJsonStrings[field.name] = JSON.stringify(row.config_json[field.name], null, 2)
      }
    }
    // 处理 sync_config_fields 中的 JSON 字段
    for (const field of (typeConfig.sync_config_fields || [])) {
      if (field.type === 'json' && row.config_json?.[field.name]) {
        configJsonStrings[field.name] = JSON.stringify(row.config_json[field.name], null, 2)
      }
    }
  }
  
  // 解析现有的同步配置
  const existingSyncConfig = row.config_json?.sync_config || {}
  const syncConfig = {
    sync_users_enabled: existingSyncConfig.sync_users_enabled || false,
    user_mapping: {
      username_field: existingSyncConfig.user_mapping?.username_field || 'preferred_username',
      email_field: existingSyncConfig.user_mapping?.email_field || 'email',
      full_name_field: existingSyncConfig.user_mapping?.full_name_field || 'name',
      org_field: existingSyncConfig.user_mapping?.org_field || ''
    },
    sync_orgs_enabled: existingSyncConfig.sync_orgs_enabled || false,
    org_mapping: {
      id_field: existingSyncConfig.org_mapping?.id_field || 'org_id',
      code_field: existingSyncConfig.org_mapping?.code_field || 'org_code',
      name_field: existingSyncConfig.org_mapping?.name_field || 'org_name',
      parent_field: existingSyncConfig.org_mapping?.parent_field || 'parent_org_id'
    },
    orgs_endpoint: existingSyncConfig.orgs_endpoint || ''
  }
  
  // 展开已启用的同步配置面板
  const expanded = []
  if (syncConfig.sync_users_enabled) expanded.push('user_sync')
  if (syncConfig.sync_orgs_enabled) expanded.push('org_sync')
  syncConfigExpanded.value = expanded
  
  form.value = {
    provider_key: row.provider_key,
    provider_type: row.provider_type,
    priority: row.priority,
    enabled: row.enabled,
    provider_id: row.provider_id,
    config: { ...(row.config_json || {}) },
    configJsonStrings,
    syncConfig
  }
  dialogVisible.value = true
}

const onTypeChange = () => {
  form.value.config = {}
  form.value.configJsonStrings = {}
  form.value.syncConfig = defaultSyncConfig()
  syncConfigExpanded.value = []
  initConfigDefaults()
  buildConfigRules()
  formRef.value?.clearValidate()
}

const initConfigDefaults = () => {
  const typeConfig = currentTypeConfig.value
  if (!typeConfig) return
  // 处理 config_fields 默认值
  for (const field of typeConfig.config_fields) {
    if (field.default !== undefined && form.value.config[field.name] === undefined) {
      if (field.type === 'json') {
        form.value.configJsonStrings[field.name] = JSON.stringify(field.default, null, 2)
      } else {
        form.value.config[field.name] = field.default
      }
    }
  }
  // 处理 sync_config_fields 默认值
  for (const field of (typeConfig.sync_config_fields || [])) {
    if (field.default !== undefined && form.value.config[field.name] === undefined) {
      if (field.type === 'json') {
        form.value.configJsonStrings[field.name] = JSON.stringify(field.default, null, 2)
      } else {
        form.value.config[field.name] = field.default
      }
    }
  }
}

// 提交表单
const submit = async () => {
  try {
    await formRef.value.validate()
  } catch {
    return
  }
  
  const configJson = { ...form.value.config }
  const typeConfig = currentTypeConfig.value
  if (typeConfig) {
    // 处理 config_fields 中的 JSON 字段
    for (const field of typeConfig.config_fields) {
      if (field.type === 'json' && form.value.configJsonStrings[field.name]) {
        try {
          configJson[field.name] = JSON.parse(form.value.configJsonStrings[field.name])
        } catch (err) {
          ElMessage.error(`${field.label} JSON格式错误`)
          return
        }
      }
    }
    // 处理 sync_config_fields 中的 JSON 字段
    for (const field of (typeConfig.sync_config_fields || [])) {
      if (field.type === 'json' && form.value.configJsonStrings[field.name]) {
        try {
          configJson[field.name] = JSON.parse(form.value.configJsonStrings[field.name])
        } catch (err) {
          ElMessage.error(`${field.label} JSON格式错误`)
          return
        }
      }
    }
  }
  
  // 添加同步配置（非本地认证时）
  if (form.value.provider_type !== 'local') {
    const sc = form.value.syncConfig
    // 只保存启用的同步配置
    configJson.sync_config = {
      sync_users_enabled: sc.sync_users_enabled,
      sync_orgs_enabled: sc.sync_orgs_enabled
    }
    if (sc.sync_users_enabled) {
      configJson.sync_config.user_mapping = sc.user_mapping
    }
    if (sc.sync_orgs_enabled) {
      configJson.sync_config.org_mapping = sc.org_mapping
      if (sc.orgs_endpoint) {
        configJson.sync_config.orgs_endpoint = sc.orgs_endpoint
      }
    }
  }
  
  const payload = {
    provider_key: form.value.provider_key,
    provider_type: form.value.provider_type,
    priority: form.value.priority,
    enabled: form.value.enabled,
    config_json: configJson
  }
  
  submitting.value = true
  try {
    if (dialogMode.value === 'create') {
      await authProviderAPI.create(payload)
      ElMessage.success('创建成功')
    } else {
      await authProviderAPI.update(form.value.provider_id, {
        priority: payload.priority,
        enabled: payload.enabled,
        config_json: payload.config_json
      })
      ElMessage.success('更新成功')
    }
    dialogVisible.value = false
    await loadProviders()
  } catch (e) {
    ElMessage.error('保存失败: ' + (e.response?.data?.detail || e.message))
  } finally {
    submitting.value = false
  }
}

// 切换启用状态
const toggleEnabled = async (row) => {
  try {
    await authProviderAPI.update(row.provider_id, { enabled: row.enabled })
    ElMessage.success(row.enabled ? '已启用' : '已禁用')
  } catch (e) {
    row.enabled = !row.enabled
    ElMessage.error('更新失败')
  }
}

// 测试连接
const testConnection = async (row) => {
  row.testing = true
  try {
    const res = await authProviderAPI.test(row.provider_id)
    const data = res?.data ?? res
    if (data.success) {
      ElMessage.success(data.message || '连接成功')
    } else {
      ElMessage.warning(data.message || '连接失败')
    }
  } catch (e) {
    ElMessage.error('测试失败: ' + (e.response?.data?.detail || e.message))
  } finally {
    row.testing = false
  }
}

// 删除
const remove = async (id) => {
  try {
    await authProviderAPI.delete(id)
    ElMessage.success('已删除')
    await loadProviders()
  } catch (e) {
    ElMessage.error('删除失败')
  }
}

const openDeleteDialog = (row) => {
  deleteTarget.value = row
  deleteDialogVisible.value = true
}

const confirmDelete = async () => {
  if (!deleteTarget.value) return
  deleting.value = true
  try {
    await remove(deleteTarget.value.provider_id)
    deleteDialogVisible.value = false
    deleteTarget.value = null
  } finally {
    deleting.value = false
  }
}

// ============ 用户同步相关 ============

// 打开同步对话框
const openSyncDialog = async (row) => {
  currentProvider.value = row
  syncTab.value = 'users'
  syncResult.value = null
  fetchResult.value = null
  syncPayload.value = ''
  syncDialogVisible.value = true
  
  await loadSyncData()
}

// 加载同步数据
const loadSyncData = async () => {
  if (!currentProvider.value) return
  
  loadingUsers.value = true
  try {
    const [usersRes, statsRes] = await Promise.all([
      authProviderAPI.getUsers(currentProvider.value.provider_id),
      authProviderAPI.getSyncStats(currentProvider.value.provider_id)
    ])
    const usersData = usersRes?.data ?? usersRes
    const statsData = statsRes?.data ?? statsRes
    syncedUsers.value = usersData?.users || []
    syncStats.value = statsData
  } catch (e) {
    console.error('加载同步数据失败:', e)
    syncedUsers.value = []
    syncStats.value = null
  } finally {
    loadingUsers.value = false
  }
}

// 提交同步
const submitSync = async () => {
  if (!currentProvider.value) return
  
  let users
  try {
    users = JSON.parse(syncPayload.value)
    if (!Array.isArray(users)) {
      ElMessage.error('请输入用户数组')
      return
    }
  } catch (e) {
    ElMessage.error('JSON 格式错误: ' + e.message)
    return
  }
  
  syncing.value = true
  try {
    const res = await authProviderAPI.syncUsers(currentProvider.value.provider_id, users)
    const data = res?.data ?? res
    syncResult.value = data
    ElMessage.success('同步完成')
    await loadSyncData()
    // 通知用户列表刷新
    window.dispatchEvent(new CustomEvent('user-list-reload'))
  } catch (e) {
    ElMessage.error('同步失败: ' + (e.response?.data?.detail || e.message))
  } finally {
    syncing.value = false
  }
}

// 移除用户
const removeUser = async (userId) => {
  if (!currentProvider.value) return
  
  try {
    await authProviderAPI.removeUser(currentProvider.value.provider_id, userId)
    ElMessage.success('用户已禁用')
    await loadSyncData()
  } catch (e) {
    ElMessage.error('操作失败')
  }
}

// 一键同步用户（自动获取并同步）
const autoSyncUsers = async () => {
  if (!currentProvider.value) return
  
  autoSyncing.value = true
  fetchResult.value = null
  syncResult.value = null
  
  try {
    const res = await authProviderAPI.autoSync(currentProvider.value.provider_id)
    const data = res?.data ?? res
    
    if (data.success) {
      syncResult.value = data
      ElMessage.success(data.message || '一键同步完成')
      await loadSyncData()
      // 通知用户列表刷新
      window.dispatchEvent(new CustomEvent('user-list-reload'))
    } else {
      fetchResult.value = { success: false, message: data.message || '同步失败' }
      ElMessage.warning(data.message || '同步失败')
    }
  } catch (e) {
    fetchResult.value = { success: false, message: e.response?.data?.detail || e.message }
    ElMessage.error('一键同步失败: ' + (e.response?.data?.detail || e.message))
  } finally {
    autoSyncing.value = false
  }
}

// 仅获取外部用户列表（不同步）
const fetchExternalUsers = async () => {
  if (!currentProvider.value) return
  
  fetching.value = true
  fetchResult.value = null
  
  try {
    const res = await authProviderAPI.fetchUsers(currentProvider.value.provider_id)
    const data = res?.data ?? res
    
    if (data.success) {
      fetchResult.value = { success: true, message: data.message }
      // 将获取的用户列表填充到手动输入框
      if (data.users && data.users.length > 0) {
        syncPayload.value = JSON.stringify(data.users, null, 2)
        ElMessage.success(`成功获取 ${data.users.length} 个用户，已填充到下方输入框`)
      } else {
        ElMessage.info('外部系统没有用户')
      }
    } else {
      fetchResult.value = { success: false, message: data.message }
      ElMessage.warning(data.message || '获取用户列表失败')
    }
  } catch (e) {
    fetchResult.value = { success: false, message: e.response?.data?.detail || e.message }
    ElMessage.error('获取用户列表失败: ' + (e.response?.data?.detail || e.message))
  } finally {
    fetching.value = false
  }
}

// 工具函数
const getTypeColor = (type) => {
  const colors = { local: 'info', oidc: 'success', api_gateway: 'warning', ldap: 'primary', oauth2: 'success', external_aes: 'danger' }
  return colors[type] || ''
}

const getTypeName = (type) => {
  const names = { local: '本地认证', oidc: 'OIDC/SSO', api_gateway: 'API网关', ldap: 'LDAP/AD', oauth2: 'OAuth2.0', external_aes: '外部AES' }
  return names[type] || type
}

const getRoleColor = (role) => {
  const colors = { admin: 'danger', data_admin: 'warning', user: 'info' }
  return colors[role] || ''
}

const getRoleLabel = (role) => {
  const labels = { admin: '管理员', data_admin: '数据管理员', user: '普通用户' }
  return labels[role] || role
}

const truncate = (str, len) => {
  if (!str) return ''
  return str.length > len ? str.slice(0, len) + '...' : str
}

// 初始化
onMounted(async () => {
  await loadTypes()
  await loadProviders()
})
</script>

<style scoped>
.auth-provider-manage {
  padding: 16px;
  background: #fff;
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}

.header-left {
  display: flex;
  align-items: center;
}

.header-right {
  display: flex;
  gap: 8px;
}

.title {
  font-weight: 600;
  font-size: 16px;
}

.ml-8 {
  margin-left: 8px;
}

.mb-12 {
  margin-bottom: 12px;
}

.mt-12 {
  margin-top: 12px;
}

.config-summary {
  font-size: 12px;
  color: #666;
}

.text-muted {
  color: #999;
}

.option-desc {
  float: right;
  color: #999;
  font-size: 12px;
}

.form-tip {
  font-size: 12px;
  color: #909399;
  margin-left: 8px;
  line-height: 18px;
  margin-top: 2px;
}

.sync-stats {
  margin-bottom: 12px;
}

.sync-actions {
  display: flex;
  justify-content: flex-end;
}

.sync-result {
  margin-top: 12px;
}

.card-header-mini {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 500;
}

.sync-desc {
  font-size: 13px;
  color: #666;
  margin-bottom: 12px;
}

.fetch-result {
  margin-top: 12px;
}

.provider-form {
  margin-top: 4px;
}

.provider-dialog {
  max-width: 960px;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr)); /* 每行 3 个输入框 */
  gap: 12px 16px;
}

.basic-grid .el-form-item {
  margin-bottom: 0;
}

.config-grid .el-form-item {
  margin-bottom: 0;
}

.switch-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px 16px;
}

.config-item.full-row {
  grid-column: 1 / -1;
}

.switch-row {
  grid-column: 1 / -1;
}

.switch-row :deep(.el-form-item__content) {
  display: flex;
  align-items: center;
}

.provider-form :deep(.el-form-item__content) {
  width: 100%;
  line-height: 22px;
  align-items: flex-start;
}

.provider-form :deep(.el-form-item__label) {
  line-height: 22px;
}

.action-btns {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  flex-wrap: nowrap;
}

.action-btns :deep(.el-button) {
  padding-left: 6px;
  padding-right: 6px;
}

:deep(.el-form-item__content) {
  flex-wrap: wrap;
}

:deep(.el-divider__text) {
  font-size: 14px;
  color: #606266;
}

.sync-config-section {
  padding: 8px 0;
}

.switch-item {
  margin-bottom: 12px;
}

.switch-item :deep(.el-form-item__content) {
  display: flex;
  align-items: center;
  gap: 8px;
}

:deep(.el-collapse-item__header) {
  font-weight: 500;
  color: #409eff;
}

:deep(.el-collapse-item__content) {
  padding-bottom: 12px;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  .card-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
    font-size: 15px;
  }
  
  .header-actions {
    width: 100%;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  
  .header-actions .el-button {
    flex: 1;
    min-width: 100px;
  }
  
  :deep(.el-table) {
    font-size: 13px;
  }
  
  :deep(.el-dialog) {
    width: 95% !important;
  }
  
  :deep(.el-dialog__body) {
    padding: 16px;
    max-height: 65vh;
    overflow-y: auto;
  }
  
  :deep(.el-form-item__label) {
    font-size: 13px;
  }
}

@media screen and (max-width: 480px) {
  .card-header {
    font-size: 14px;
  }
  
  :deep(.el-table) {
    font-size: 12px;
  }
}
</style>
