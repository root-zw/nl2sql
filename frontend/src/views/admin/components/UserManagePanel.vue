<template>
  <div class="user-manage">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>👥 用户管理</span>
          <el-button type="primary" size="small" @click="showAddDialog">
            <el-icon><Plus /></el-icon>
            添加用户
          </el-button>
        </div>
      </template>

      <div class="toolbar">
        <div class="toolbar-left">
          <el-input
            v-model="searchText"
            placeholder="搜索用户名..."
            style="width: 200px"
            clearable
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
          
          <el-select 
            v-model="filterOrgId" 
            placeholder="筛选组织" 
            clearable 
            style="width: 180px"
            @change="loadUsers"
          >
            <el-option label="未分配" value="unassigned" />
            <el-option 
              v-for="org in organizations" 
              :key="org.org_id" 
              :label="org.org_name" 
              :value="org.org_id"
            />
          </el-select>
          
          <el-select 
            v-model="filterSource" 
            placeholder="筛选来源" 
            clearable 
            style="width: 140px"
            @change="loadUsers"
          >
            <el-option label="本地用户" value="local" />
            <el-option 
              v-for="src in authSources" 
              :key="src" 
              :label="src" 
              :value="src"
            />
          </el-select>
        </div>

        <el-button @click="loadUsers">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>

      <el-table
        v-loading="loading"
        :data="filteredUsers"
        stripe
        style="width: 100%"
        table-layout="auto"
      >
        <el-table-column label="用户名" prop="username" min-width="120" />
        <el-table-column label="姓名" prop="full_name" min-width="100">
          <template #default="{ row }">
            {{ row.full_name || '-' }}
          </template>
        </el-table-column>
        <el-table-column label="邮箱" prop="email" min-width="180" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.email || '-' }}
          </template>
        </el-table-column>
        <el-table-column label="系统角色" min-width="100">
          <template #default="{ row }">
            <el-tag :type="getRoleColor(row.role)" size="small">
              {{ getRoleLabel(row.role) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="所属组织" min-width="120">
          <template #default="{ row }">
            <el-tag v-if="row.org_name" size="small">{{ row.org_name }}</el-tag>
            <span v-else class="text-muted">未分配</span>
          </template>
        </el-table-column>
        <el-table-column label="认证来源" min-width="100">
          <template #default="{ row }">
            <el-tag v-if="row.external_idp" size="small" type="success">{{ row.external_idp }}</el-tag>
            <el-tag v-else size="small" type="info">本地</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="创建时间" min-width="160">
          <template #default="{ row }">
            {{ formatDate(row.created_at) }}
          </template>
        </el-table-column>
        <el-table-column label="最后登录" min-width="180">
          <template #default="{ row }">
            {{ formatDate(row.last_login_at) }}
          </template>
        </el-table-column>
        <el-table-column label="状态" min-width="80" align="center">
          <template #default="{ row }">
            <el-switch
              v-model="row.is_active"
              @change="toggleUserStatus(row)"
              :loading="row.updating"
              :disabled="row.username === 'admin'"
            />
          </template>
        </el-table-column>
        <el-table-column label="操作" min-width="320" fixed="right">
          <template #default="{ row }">
            <div class="action-buttons">
              <el-button
                size="small"
                type="primary"
                text
                @click="showEditDialog(row)"
              >
                编辑
              </el-button>
              <el-button
                size="small"
                type="success"
                text
                @click="showDataRolesDialog(row)"
              >
                数据角色
              </el-button>
              <el-button
                size="small"
                type="warning"
                text
                @click="resetPassword(row)"
              >
                重置密码
              </el-button>
              <el-button
                size="small"
                type="danger"
                text
                @click="deleteUser(row)"
                :disabled="row.username === 'admin'"
              >
                删除
              </el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog
      v-model="dialogVisible"
      :title="isEdit ? '编辑用户' : '添加用户'"
      width="90%"
      :style="{ maxWidth: '500px' }"
    >
      <el-form
        ref="formRef"
        :model="form"
        :rules="rules"
        label-width="100px"
      >
        <el-form-item label="用户名" prop="username">
          <el-input
            v-model="form.username"
            :disabled="isEdit"
            placeholder="请输入用户名"
          />
        </el-form-item>

        <el-form-item label="密码" prop="password" v-if="!isEdit">
          <el-input
            v-model="form.password"
            type="password"
            placeholder="请输入密码（至少8位，包含大写、小写、数字、特殊字符中的至少三种）"
            show-password
          />
        </el-form-item>

        <el-form-item label="姓名" prop="full_name">
          <el-input
            v-model="form.full_name"
            placeholder="请输入姓名（可选）"
          />
        </el-form-item>

        <el-form-item label="邮箱" prop="email">
          <el-input
            v-model="form.email"
            placeholder="请输入邮箱（可选）"
          />
        </el-form-item>

        <el-form-item label="系统角色" prop="role">
          <el-select v-model="form.role" style="width: 100%">
            <el-option label="系统管理员" value="admin">
              <span>🔑 系统管理员</span>
              <span style="color: #999; font-size: 12px; margin-left: 8px">所有管理功能</span>
            </el-option>
            <el-option label="数据管理员" value="data_admin">
              <span>📊 数据管理员</span>
              <span style="color: #999; font-size: 12px; margin-left: 8px">数据库连接/元数据/同步/数据权限</span>
            </el-option>
            <el-option label="普通用户" value="user">
              <span>👤 普通用户</span>
              <span style="color: #999; font-size: 12px; margin-left: 8px">仅查询，不能登录后台</span>
            </el-option>
          </el-select>
          <div style="font-size: 12px; color: #909399; margin-top: 4px;">
            <div>• <b>系统管理员</b>：可访问所有后台功能</div>
            <div>• <b>数据管理员</b>：可管理数据库连接、元数据、Milvus同步、数据权限</div>
            <div>• <b>普通用户</b>：仅能进行数据查询，不能登录管理后台</div>
          </div>
        </el-form-item>
      </el-form>

      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveUser" :loading="saving">
          保存
        </el-button>
      </template>
    </el-dialog>

    <!-- 数据角色配置弹窗 -->
    <el-dialog
      v-model="dataRolesDialogVisible"
      :title="`配置数据角色 - ${currentUserForRoles?.username}`"
      width="90%"
      :style="{ maxWidth: '700px' }"
    >
      <div v-loading="loadingRoles">
        <div class="data-roles-header">
          <span class="section-title">已分配的数据角色</span>
          <el-button type="primary" size="small" @click="showAssignRoleDialog">
            <el-icon><Plus /></el-icon>分配角色
          </el-button>
        </div>
        
        <el-table :data="userDataRoles" stripe max-height="300" style="width: 100%" table-layout="auto">
          <el-table-column prop="role_name" label="角色名称" min-width="150" />
          <el-table-column prop="role_code" label="角色编码" min-width="140" />
          <el-table-column prop="connection_name" label="数据库" min-width="150" />
          <el-table-column label="操作" min-width="100">
            <template #default="{ row }">
              <el-button
                size="small"
                type="danger"
                text
                @click="removeDataRole(row)"
              >
                移除
              </el-button>
            </template>
          </el-table-column>
        </el-table>

        <el-divider />

        <div class="section-title">用户属性（供行级过滤使用）</div>
        <div class="user-attributes">
          <div v-for="(attr, index) in userAttributes" :key="index" class="attr-row">
            <el-input v-model="attr.attribute_name" placeholder="属性名" style="flex: 1; min-width: 120px; max-width: 150px" />
            <el-input v-model="attr.attribute_value" placeholder="属性值" style="flex: 1; margin: 0 8px" />
            <el-button :icon="Delete" circle @click="removeAttribute(index)" />
          </div>
          <el-button size="small" @click="addAttribute">
            <el-icon><Plus /></el-icon>添加属性
          </el-button>
        </div>
      </div>
      <template #footer>
        <el-button @click="dataRolesDialogVisible = false">关闭</el-button>
        <el-button type="primary" @click="saveUserAttributes" :loading="savingAttrs">
          保存属性
        </el-button>
      </template>
    </el-dialog>

    <!-- 分配角色弹窗 -->
    <el-dialog v-model="assignRoleDialogVisible" title="分配数据角色" width="90%" :style="{ maxWidth: '500px' }">
      <el-form label-width="100px">
        <el-form-item label="数据库连接">
          <el-select v-model="selectedConnection" @change="loadAvailableRoles" style="width: 100%">
            <el-option v-for="conn in connections" :key="conn.connection_id" :label="conn.connection_name" :value="conn.connection_id" />
          </el-select>
        </el-form-item>
        <el-form-item label="选择角色">
          <el-select v-model="selectedRoleId" style="width: 100%">
            <el-option v-for="role in availableRoles" :key="role.role_id" :label="role.role_name" :value="role.role_id">
              <span>{{ role.role_name }}</span>
              <span style="color: #999; margin-left: 8px">({{ role.role_code }})</span>
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="assignRoleDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="assignDataRole" :loading="assigningRole">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onActivated } from 'vue'
import { Plus, Search, Refresh, Delete } from '@element-plus/icons-vue'
import axios from '@/utils/request'
import { ElMessage, ElMessageBox } from 'element-plus'
import { validatePasswordStrength } from '@/utils/common'
import { onUnmounted } from 'vue'
import { organizationAPI } from '@/api'

const loading = ref(false)
const saving = ref(false)
const searchText = ref('')
const users = ref([])
const dialogVisible = ref(false)
const isEdit = ref(false)
const formRef = ref()

// 组织相关
const organizations = ref([])
const filterOrgId = ref('')
const filterSource = ref('')
const authSources = ref([])

// ========== 数据角色相关状态 ==========
const dataRolesDialogVisible = ref(false)
const currentUserForRoles = ref(null)
const userDataRoles = ref([])
const userAttributes = ref([])
const loadingRoles = ref(false)
const savingAttrs = ref(false)
const assignRoleDialogVisible = ref(false)
const connections = ref([])
const availableRoles = ref([])
const selectedConnection = ref('')
const selectedRoleId = ref('')
const assigningRole = ref(false)

const form = ref({
  user_id: null,
  username: '',
  password: '',
  full_name: '',
  email: '',
  role: 'user'
})

// 验证密码强度
const validatePassword = (rule, value, callback) => {
  if (!value) {
    callback(new Error('请输入密码'))
    return
  }
  const result = validatePasswordStrength(value)
  if (!result.valid) {
    callback(new Error(result.message))
  } else {
    callback()
  }
}

const rules = {
  username: [
    { required: true, message: '请输入用户名', trigger: 'blur' },
    { min: 3, max: 50, message: '用户名长度需在 3-50 个字符之间', trigger: 'blur' }
  ],
  password: [
    { required: true, message: '请输入密码', trigger: 'blur' },
    { validator: validatePassword, trigger: 'blur' }
  ],
  email: [
    { type: 'email', message: '请输入有效的邮箱地址', trigger: 'blur' }
  ],
  role: [
    { required: true, message: '请选择角色', trigger: 'change' }
  ]
}

function extractErrorMessage(error, fallback = '请求失败') {
  const detail = error?.response?.data?.detail
  if (Array.isArray(detail)) {
    return detail
      .map(item => item?.msg || JSON.stringify(item))
      .join('；')
  }
  if (typeof detail === 'string') return detail
  if (detail && typeof detail === 'object' && detail.message) return detail.message
  return error?.message || fallback
}

const filteredUsers = computed(() => {
  let result = users.value
  
  // 文本搜索
  if (searchText.value) {
    const search = searchText.value.toLowerCase()
    result = result.filter(user =>
      user.username.toLowerCase().includes(search) ||
      (user.full_name && user.full_name.toLowerCase().includes(search)) ||
      (user.email && user.email.toLowerCase().includes(search))
    )
  }
  
  // 组织筛选
  if (filterOrgId.value) {
    if (filterOrgId.value === 'unassigned') {
      result = result.filter(user => !user.org_id)
    } else {
      result = result.filter(user => user.org_id === filterOrgId.value)
    }
  }
  
  // 来源筛选
  if (filterSource.value) {
    if (filterSource.value === 'local') {
      result = result.filter(user => !user.external_idp)
    } else {
      result = result.filter(user => user.external_idp === filterSource.value)
    }
  }
  
  return result
})

async function loadUsers() {
  loading.value = true
  try {
    const { data } = await axios.get('/admin/users')
    users.value = data.map(u => ({ ...u, updating: false }))
    
    // 收集认证来源
    const sources = new Set()
    data.forEach(u => {
      if (u.external_idp) sources.add(u.external_idp)
    })
    authSources.value = Array.from(sources)
  } catch (error) {
    ElMessage.error('加载用户列表失败')
    console.error(error)
  } finally {
    loading.value = false
  }
}

async function loadOrganizations() {
  try {
    const res = await organizationAPI.list({ is_active: true })
    organizations.value = res.data || res || []
  } catch (error) {
    console.error('加载组织列表失败:', error)
  }
}

function showAddDialog() {
  isEdit.value = false
  form.value = {
    user_id: null,
    username: '',
    password: '',
    full_name: '',
    email: '',
    role: 'user'
  }
  dialogVisible.value = true
}

function showEditDialog(user) {
  isEdit.value = true
  form.value = {
    user_id: user.user_id,
    username: user.username,
    password: '',
    full_name: user.full_name || '',
    email: user.email || '',
    role: user.role
  }
  dialogVisible.value = true
}

async function saveUser() {
  const valid = await formRef.value.validate().catch(() => false)
  if (!valid) return

  saving.value = true
  try {
    if (isEdit.value) {
      // 编辑时更新角色、姓名、邮箱
      const updateData = {
        role: form.value.role,
        full_name: form.value.full_name || null,
        email: form.value.email || null
      }
      await axios.put(`/admin/users/${form.value.user_id}`, updateData)
      ElMessage.success('用户已更新')
    } else {
      // 创建时传入所有字段
      const createData = {
        username: form.value.username,
        password: form.value.password,
        role: form.value.role,
        full_name: form.value.full_name || null,
        email: form.value.email || null
      }
      await axios.post('/admin/users', createData)
      ElMessage.success('用户已添加')
    }

    dialogVisible.value = false
    await loadUsers()
  } catch (error) {
    ElMessage.error(`保存失败: ${extractErrorMessage(error, '请检查输入信息')}`)
  } finally {
    saving.value = false
  }
}

async function toggleUserStatus(user) {
  user.updating = true
  try {
    await axios.put(`/admin/users/${user.user_id}`, {
      is_active: user.is_active
    })
    ElMessage.success(user.is_active ? '已启用' : '已禁用')
  } catch (error) {
    user.is_active = !user.is_active
    ElMessage.error('更新失败')
  } finally {
    user.updating = false
  }
}

async function resetPassword(user) {
  try {
    const { value } = await ElMessageBox.prompt(
      `请输入 ${user.username} 的新密码`,
      '重置密码',
      {
        inputType: 'password',
        inputPlaceholder: '至少8位，包含大写、小写、数字、特殊字符中的至少三种',
        inputValidator: (value) => {
          if (!value) {
            return '密码不能为空'
          }
          const result = validatePasswordStrength(value)
          return result.valid || result.message
        }
      }
    )

    await axios.post(`/admin/users/${user.user_id}/reset-password`, {
      new_password: value
    })
    ElMessage.success('密码已重置')
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error(`重置失败: ${extractErrorMessage(error, '请重试')}`)
    }
  }
}

async function deleteUser(user) {
  try {
    await ElMessageBox.confirm(
      `确定要删除用户 "${user.username}" 吗？`,
      '确认删除',
      { type: 'warning' }
    )

    await axios.delete(`/admin/users/${user.user_id}`)
    ElMessage.success('用户已删除')
    await loadUsers()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error(`删除失败: ${extractErrorMessage(error, '请重试')}`)
    }
  }
}

function getRoleColor(role) {
  const colors = {
    admin: 'danger',
    data_admin: 'warning',
    user: 'info',
    // 兼容旧数据
    viewer: 'info'
  }
  return colors[role] || ''
}

function getRoleLabel(role) {
  const labels = {
    admin: '系统管理员',
    data_admin: '数据管理员',
    user: '普通用户',
    // 兼容旧数据
    viewer: '只读用户'
  }
  return labels[role] || role
}

function formatDate(date) {
  if (!date) return '-'
  return new Date(date).toLocaleString('zh-CN')
}

// ========== 数据角色相关方法 ==========
async function showDataRolesDialog(user) {
  currentUserForRoles.value = user
  dataRolesDialogVisible.value = true
  loadingRoles.value = true
  
  try {
    // 加载用户的数据角色
    const rolesRes = await axios.get(`/admin/permissions/users/${user.user_id}/data-roles`)
    userDataRoles.value = rolesRes.data
    
    // 加载用户属性
    const attrsRes = await axios.get(`/admin/permissions/users/${user.user_id}/attributes`)
    userAttributes.value = attrsRes.data.map(a => ({
      attribute_name: a.attribute_name,
      attribute_value: a.attribute_value
    }))
    
    // 加载数据库连接
    const connRes = await axios.get('/admin/connections/all')
    connections.value = connRes.data
  } catch (error) {
    ElMessage.error('加载数据失败')
    console.error(error)
  } finally {
    loadingRoles.value = false
  }
}

async function showAssignRoleDialog() {
  selectedConnection.value = ''
  selectedRoleId.value = ''
  availableRoles.value = []
  assignRoleDialogVisible.value = true
}

async function loadAvailableRoles() {
  if (!selectedConnection.value) {
    availableRoles.value = []
    return
  }
  try {
    const res = await axios.get('/admin/permissions/data-roles', {
      params: { connection_id: selectedConnection.value, is_active: true }
    })
    availableRoles.value = res.data
  } catch (error) {
    console.error('加载角色失败:', error)
  }
}

async function assignDataRole() {
  if (!selectedRoleId.value) {
    ElMessage.warning('请选择角色')
    return
  }
  assigningRole.value = true
  try {
    await axios.post(`/admin/permissions/users/${currentUserForRoles.value.user_id}/data-roles`, {
      role_id: selectedRoleId.value
    })
    ElMessage.success('分配成功')
    assignRoleDialogVisible.value = false
    // 刷新用户角色列表
    const rolesRes = await axios.get(`/admin/permissions/users/${currentUserForRoles.value.user_id}/data-roles`)
    userDataRoles.value = rolesRes.data
  } catch (error) {
    ElMessage.error('分配失败')
    console.error(error)
  } finally {
    assigningRole.value = false
  }
}

async function removeDataRole(role) {
  try {
    await ElMessageBox.confirm(`确定要移除角色 "${role.role_name}" 吗？`, '确认', { type: 'warning' })
    await axios.delete(`/admin/permissions/users/${currentUserForRoles.value.user_id}/data-roles/${role.role_id}`)
    ElMessage.success('移除成功')
    userDataRoles.value = userDataRoles.value.filter(r => r.role_id !== role.role_id)
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('移除失败')
      console.error(error)
    }
  }
}

function addAttribute() {
  userAttributes.value.push({ attribute_name: '', attribute_value: '' })
}

function removeAttribute(index) {
  userAttributes.value.splice(index, 1)
}

async function saveUserAttributes() {
  const validAttrs = userAttributes.value.filter(a => a.attribute_name && a.attribute_value)
  if (validAttrs.length === 0) {
    ElMessage.warning('请添加有效属性')
    return
  }
  
  savingAttrs.value = true
  try {
    const attrsObj = {}
    validAttrs.forEach(a => {
      attrsObj[a.attribute_name] = a.attribute_value
    })
    await axios.put(`/admin/permissions/users/${currentUserForRoles.value.user_id}/attributes`, {
      attributes: attrsObj
    })
    ElMessage.success('属性保存成功')
  } catch (error) {
    ElMessage.error('保存失败')
    console.error(error)
  } finally {
    savingAttrs.value = false
  }
}

onMounted(() => {
  loadUsers()
  loadOrganizations()
  // 监听外部同步完成后刷新
  window.addEventListener('user-list-reload', loadUsers)
})

onActivated(() => {
  loadUsers()
  loadOrganizations()
})

onUnmounted(() => {
  window.removeEventListener('user-list-reload', loadUsers)
})
</script>

<style scoped>
.user-manage {
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 16px;
  font-weight: 600;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.toolbar-left {
  display: flex;
  gap: 12px;
  align-items: center;
}

.text-muted {
  color: #909399;
  font-size: 12px;
}

.action-buttons {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: nowrap;
}

.action-buttons .el-button {
  white-space: nowrap;
  padding: 5px 8px;
}

::deep(.el-card__body) {
  padding: 20px;
}

::deep(.el-table) {
  font-size: 14px;
  width: 100%;
}

::deep(.el-table th),
::deep(.el-table td) {
  padding: 12px 0;
}

::deep(.el-table__body-wrapper) {
  width: 100%;
}

/* 确保表格填满容器 */
.user-manage {
  width: 100%;
}

.user-manage .el-card {
  width: 100%;
}

/* 响应式设计 */
@media screen and (max-width: 1400px) {
  .action-buttons {
    flex-wrap: wrap;
  }
  
  .action-buttons .el-button {
    margin-bottom: 4px;
  }
}

@media screen and (max-width: 768px) {
  .card-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
    font-size: 15px;
  }
  
  .card-header .el-button {
    width: 100%;
  }
  
  .toolbar {
    flex-direction: column;
    gap: 10px;
  }
  
  .toolbar .el-input,
  .toolbar .el-select {
    width: 100% !important;
    margin-bottom: 0;
  }
  
  .toolbar-right {
    width: 100%;
    justify-content: flex-end;
  }
  
  :deep(.el-table) {
    font-size: 13px;
  }
  
  .action-buttons {
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }
  
  .action-buttons .el-button {
    margin-left: 0 !important;
    margin-bottom: 0;
  }
  
  /* 对话框 */
  :deep(.el-dialog) {
    width: 95% !important;
  }
  
  :deep(.el-dialog__body) {
    padding: 16px;
    max-height: 65vh;
    overflow-y: auto;
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

/* 数据角色配置样式 */
.data-roles-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.section-title {
  font-weight: 600;
  font-size: 14px;
  color: #333;
}

.user-attributes {
  margin-top: 12px;
}

.attr-row {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
}
</style>

