<template>
  <div class="organization-manage">
    <div class="org-layout">
      <!-- 左侧：组织树 -->
      <div class="org-tree-panel">
        <div class="panel-header">
          <span class="title">🏢 组织架构</span>
          <el-button type="primary" size="small" @click="showCreateDialog(null)">
            <el-icon><Plus /></el-icon>
            新建
          </el-button>
        </div>
        
        <div class="tree-toolbar">
          <el-input
            v-model="treeSearch"
            placeholder="搜索组织..."
            clearable
            size="small"
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
        </div>

        <div class="tree-container" v-loading="loadingTree">
          <!-- 未分配用户入口 -->
          <div 
            class="tree-node unassigned-node"
            :class="{ active: selectedOrgId === 'unassigned' }"
            @click="selectUnassigned"
          >
            <el-icon><User /></el-icon>
            <span>未分配用户</span>
            <el-tag size="small" type="warning">{{ unassignedCount }}</el-tag>
          </div>
          
          <el-divider style="margin: 8px 0;" />
          
          <el-tree
            ref="treeRef"
            :data="filteredTreeData"
            :props="treeProps"
            node-key="org_id"
            highlight-current
            default-expand-all
            :expand-on-click-node="false"
            @node-click="onNodeClick"
          >
            <template #default="{ node, data }">
              <div 
                class="tree-node-content"
                @mouseenter="hoveredOrgId = data.org_id"
                @mouseleave="hoveredOrgId = null"
              >
                <span class="node-icon">{{ getOrgIcon(data.org_type) }}</span>
                <span class="node-label">{{ data.org_name }}</span>
                <el-tag v-if="data.direct_user_count > 0" size="small" type="info">
                  {{ data.direct_user_count }}
                </el-tag>
                <span v-if="hoveredOrgId === data.org_id" class="node-actions">
                  <el-tooltip content="添加子组织" placement="top">
                    <el-icon class="action-icon" @click.stop="showCreateDialog(data)">
                      <Plus />
                    </el-icon>
                  </el-tooltip>
                  <el-tooltip content="编辑" placement="top">
                    <el-icon class="action-icon" @click.stop="showEditDialog(data)">
                      <Edit />
                    </el-icon>
                  </el-tooltip>
                  <el-tooltip content="删除" placement="top">
                    <el-icon class="action-icon danger" @click.stop="deleteOrg(data)">
                      <Delete />
                    </el-icon>
                  </el-tooltip>
                </span>
              </div>
            </template>
          </el-tree>
        </div>
      </div>

      <!-- 右侧：组织详情/成员列表 -->
      <div class="org-detail-panel">
        <!-- 未分配用户列表 -->
        <template v-if="selectedOrgId === 'unassigned'">
          <div class="panel-header">
            <span class="title">👤 未分配用户</span>
            <el-button 
              type="primary" 
              size="small" 
              :disabled="selectedUserIds.length === 0"
              @click="showBatchAssignDialog"
            >
              <el-icon><FolderAdd /></el-icon>
              批量分配
            </el-button>
          </div>
          
          <el-table 
            v-loading="loadingMembers"
            :data="unassignedUsers"
            size="small"
            @selection-change="onSelectionChange"
          >
            <el-table-column type="selection" width="40" />
            <el-table-column prop="username" label="用户名" width="120" />
            <el-table-column prop="full_name" label="姓名" width="100">
              <template #default="{ row }">{{ row.full_name || '-' }}</template>
            </el-table-column>
            <el-table-column prop="email" label="邮箱" show-overflow-tooltip />
            <el-table-column prop="external_idp" label="来源" width="100">
              <template #default="{ row }">
                <el-tag v-if="row.external_idp" size="small">{{ row.external_idp }}</el-tag>
                <el-tag v-else size="small" type="info">本地</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="100">
              <template #default="{ row }">
                <el-button type="primary" link size="small" @click="showAssignDialog(row)">
                  分配
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </template>

        <!-- 组织详情 -->
        <template v-else-if="selectedOrg">
          <div class="panel-header">
            <div class="org-info">
              <span class="org-icon">{{ getOrgIcon(selectedOrg.org_type) }}</span>
              <span class="org-name">{{ selectedOrg.org_name }}</span>
              <el-tag v-if="selectedOrg.source_idp" size="small">
                来源: {{ selectedOrg.source_idp }}
              </el-tag>
            </div>
            <el-button type="primary" size="small" @click="showAssignDialog(null)">
              <el-icon><Plus /></el-icon>
              添加成员
            </el-button>
          </div>

          <el-tabs v-model="detailTab">
            <!-- 成员列表 -->
            <el-tab-pane label="成员列表" name="members">
              <div class="members-toolbar">
                <el-checkbox v-model="includeChildren" @change="loadOrgMembers">
                  包含子组织成员
                </el-checkbox>
                <el-input
                  v-model="memberSearch"
                  placeholder="搜索成员..."
                  clearable
                  size="small"
                  style="width: 200px"
                  @clear="loadOrgMembers"
                  @keyup.enter="loadOrgMembers"
                />
              </div>
              
              <el-table 
                v-loading="loadingMembers"
                :data="orgMembers"
                size="small"
                @selection-change="onSelectionChange"
              >
                <el-table-column type="selection" width="40" />
                <el-table-column prop="username" label="用户名" width="120" />
                <el-table-column prop="full_name" label="姓名" width="100">
                  <template #default="{ row }">{{ row.full_name || '-' }}</template>
                </el-table-column>
                <el-table-column prop="position" label="职位" width="100">
                  <template #default="{ row }">{{ row.position || '-' }}</template>
                </el-table-column>
                <el-table-column prop="role" label="系统角色" width="100">
                  <template #default="{ row }">
                    <el-tag :type="getRoleColor(row.role)" size="small">
                      {{ getRoleLabel(row.role) }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="external_idp" label="来源" width="80">
                  <template #default="{ row }">
                    <el-tag v-if="row.external_idp" size="small">{{ row.external_idp }}</el-tag>
                    <el-tag v-else size="small" type="info">本地</el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="操作" width="120">
                  <template #default="{ row }">
                    <el-button type="warning" link size="small" @click="showAssignDialog(row)">
                      调整
                    </el-button>
                    <el-button type="danger" link size="small" @click="removeFromOrg(row)">
                      移除
                    </el-button>
                  </template>
                </el-table-column>
              </el-table>
              
              <el-pagination
                v-if="membersTotal > membersPageSize"
                :current-page="membersPage"
                :page-size="membersPageSize"
                :total="membersTotal"
                layout="total, prev, pager, next"
                @current-change="onPageChange"
                style="margin-top: 12px; justify-content: flex-end;"
              />
            </el-tab-pane>

            <!-- 数据角色 -->
            <el-tab-pane label="数据角色" name="roles">
              <div class="roles-header">
                <span class="section-desc">
                  为组织分配数据角色，组织成员将自动继承这些角色
                </span>
                <el-button type="primary" size="small" @click="showAssignRoleDialog">
                  <el-icon><Plus /></el-icon>
                  分配角色
                </el-button>
              </div>
              
              <el-table :data="orgRoles" size="small" v-loading="loadingRoles">
                <el-table-column prop="role_name" label="角色名称" />
                <el-table-column prop="role_code" label="角色编码" width="140" />
                <el-table-column prop="scope_type" label="范围" width="100">
                  <template #default="{ row }">
                    <el-tag :type="row.scope_type === 'all' ? 'success' : 'info'" size="small">
                      {{ row.scope_type === 'all' ? '全量' : '受限' }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="inherit_to_children" label="继承子组织" width="100">
                  <template #default="{ row }">
                    <el-tag :type="row.inherit_to_children ? 'success' : 'warning'" size="small">
                      {{ row.inherit_to_children ? '是' : '否' }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="操作" width="80">
                  <template #default="{ row }">
                    <el-button type="danger" link size="small" @click="removeOrgRole(row)">
                      移除
                    </el-button>
                  </template>
                </el-table-column>
              </el-table>
            </el-tab-pane>

            <!-- 基本信息 -->
            <el-tab-pane label="基本信息" name="info">
              <el-descriptions :column="2" border>
                <el-descriptions-item label="组织编码">{{ selectedOrg.org_code }}</el-descriptions-item>
                <el-descriptions-item label="组织名称">{{ selectedOrg.org_name }}</el-descriptions-item>
                <el-descriptions-item label="组织类型">{{ getOrgTypeName(selectedOrg.org_type) }}</el-descriptions-item>
                <el-descriptions-item label="层级">{{ selectedOrg.level }}</el-descriptions-item>
                <el-descriptions-item label="父组织">{{ selectedOrg.parent_org_name || '无（根组织）' }}</el-descriptions-item>
                <el-descriptions-item label="完整路径">{{ selectedOrg.org_path || '/' }}</el-descriptions-item>
                <el-descriptions-item label="直接成员">{{ selectedOrg.direct_user_count }}</el-descriptions-item>
                <el-descriptions-item label="子组织数">{{ selectedOrg.child_org_count }}</el-descriptions-item>
                <el-descriptions-item label="外部来源" v-if="selectedOrg.source_idp">
                  {{ selectedOrg.source_idp }}
                </el-descriptions-item>
                <el-descriptions-item label="描述" :span="2">
                  {{ selectedOrg.description || '-' }}
                </el-descriptions-item>
              </el-descriptions>
            </el-tab-pane>
          </el-tabs>
        </template>

        <!-- 空状态 -->
        <template v-else>
          <el-empty description="请从左侧选择组织或查看未分配用户" />
        </template>
      </div>
    </div>

    <!-- 创建/编辑组织对话框 -->
    <el-dialog
      v-model="orgDialogVisible"
      :title="isEditOrg ? '编辑组织' : '新建组织'"
      width="500px"
      destroy-on-close
    >
      <el-form :model="orgForm" :rules="orgRules" ref="orgFormRef" label-width="100px">
        <el-form-item label="组织编码" prop="org_code">
          <el-input 
            v-model="orgForm.org_code" 
            :disabled="isEditOrg"
            placeholder="唯一标识，如 tech_dept"
          />
        </el-form-item>
        <el-form-item label="组织名称" prop="org_name">
          <el-input v-model="orgForm.org_name" placeholder="如：技术部" />
        </el-form-item>
        <el-form-item label="组织类型" prop="org_type">
          <el-select v-model="orgForm.org_type" style="width: 100%">
            <el-option label="🏢 公司" value="company" />
            <el-option label="📁 部门" value="department" />
            <el-option label="👥 团队" value="team" />
            <el-option label="👤 小组" value="group" />
          </el-select>
        </el-form-item>
        <el-form-item label="上级组织">
          <el-tree-select
            v-model="orgForm.parent_id"
            :data="treeDataForSelect"
            :props="{ label: 'org_name', value: 'org_id', children: 'children' }"
            clearable
            check-strictly
            placeholder="选择上级组织（不选则为根组织）"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="排序" prop="sort_order">
          <el-input-number v-model="orgForm.sort_order" :min="0" :max="1000" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="orgForm.description" type="textarea" :rows="2" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="orgDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveOrg" :loading="savingOrg">保存</el-button>
      </template>
    </el-dialog>

    <!-- 分配用户到组织对话框 -->
    <el-dialog
      v-model="assignDialogVisible"
      :title="assignUser ? `分配用户: ${assignUser.username}` : '添加成员'"
      width="500px"
      destroy-on-close
    >
      <el-form label-width="100px">
        <el-form-item label="选择用户" v-if="!assignUser">
          <el-select 
            v-model="assignForm.user_id" 
            filterable
            remote
            :remote-method="searchUnassigned"
            placeholder="搜索用户名..."
            style="width: 100%"
          >
            <el-option 
              v-for="u in searchedUsers" 
              :key="u.user_id" 
              :label="`${u.username} (${u.full_name || '-'})`"
              :value="u.user_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="目标组织">
          <el-tree-select
            v-model="assignForm.org_id"
            :data="treeDataForSelect"
            :props="{ label: 'org_name', value: 'org_id', children: 'children' }"
            clearable
            check-strictly
            placeholder="选择目标组织"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="职位">
          <el-input v-model="assignForm.position" placeholder="可选，如：工程师" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="assignDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submitAssign" :loading="assigning">确定</el-button>
      </template>
    </el-dialog>

    <!-- 批量分配对话框 -->
    <el-dialog
      v-model="batchAssignDialogVisible"
      title="批量分配用户"
      width="500px"
      destroy-on-close
    >
      <el-form label-width="100px">
        <el-form-item label="已选用户">
          <el-tag v-for="id in selectedUserIds" :key="id" class="mr-4 mb-4">
            {{ getUsernameById(id) }}
          </el-tag>
        </el-form-item>
        <el-form-item label="目标组织">
          <el-tree-select
            v-model="batchAssignForm.org_id"
            :data="treeDataForSelect"
            :props="{ label: 'org_name', value: 'org_id', children: 'children' }"
            check-strictly
            placeholder="选择目标组织"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="职位">
          <el-input v-model="batchAssignForm.position" placeholder="可选" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="batchAssignDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submitBatchAssign" :loading="batchAssigning">确定</el-button>
      </template>
    </el-dialog>

    <!-- 分配数据角色对话框 -->
    <el-dialog
      v-model="assignRoleDialogVisible"
      title="分配数据角色"
      width="500px"
      destroy-on-close
    >
      <el-form label-width="100px">
        <el-form-item label="选择角色">
          <el-select v-model="assignRoleForm.role_id" style="width: 100%">
            <el-option 
              v-for="role in availableRoles" 
              :key="role.role_id" 
              :label="role.role_name"
              :value="role.role_id"
            >
              <span>{{ role.role_name }}</span>
              <span style="color: #999; margin-left: 8px">({{ role.role_code }})</span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="继承子组织">
          <el-switch v-model="assignRoleForm.inherit_to_children" />
          <span class="form-tip">开启后，子组织成员也将拥有此角色</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="assignRoleDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submitAssignRole" :loading="assigningRole">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Search, User, Edit, Delete, FolderAdd } from '@element-plus/icons-vue'
import { organizationAPI } from '@/api'
import axios from '@/utils/request'

// ========== 状态 ==========
const treeRef = ref()
const treeData = ref([])
const treeSearch = ref('')
const loadingTree = ref(false)
const hoveredOrgId = ref(null)
const selectedOrgId = ref(null)
const selectedOrg = ref(null)
const detailTab = ref('members')

// 成员相关
const orgMembers = ref([])
const unassignedUsers = ref([])
const unassignedCount = ref(0)
const loadingMembers = ref(false)
const includeChildren = ref(false)
const memberSearch = ref('')
const membersPage = ref(1)
const membersPageSize = ref(20)
const membersTotal = ref(0)
const selectedUserIds = ref([])

// 组织角色
const orgRoles = ref([])
const loadingRoles = ref(false)
const availableRoles = ref([])

// 对话框状态
const orgDialogVisible = ref(false)
const isEditOrg = ref(false)
const orgFormRef = ref()
const savingOrg = ref(false)
const orgForm = ref({
  org_code: '',
  org_name: '',
  org_type: 'department',
  parent_id: null,
  sort_order: 0,
  description: ''
})

const assignDialogVisible = ref(false)
const assignUser = ref(null)
const assigning = ref(false)
const assignForm = ref({ user_id: null, org_id: null, position: '' })
const searchedUsers = ref([])

const batchAssignDialogVisible = ref(false)
const batchAssigning = ref(false)
const batchAssignForm = ref({ org_id: null, position: '' })

const assignRoleDialogVisible = ref(false)
const assigningRole = ref(false)
const assignRoleForm = ref({ role_id: null, inherit_to_children: true })

// ========== 配置 ==========
const treeProps = { children: 'children', label: 'org_name' }

const orgRules = {
  org_code: [
    { required: true, message: '请输入组织编码', trigger: 'blur' },
    { min: 2, max: 50, message: '长度2-50字符', trigger: 'blur' }
  ],
  org_name: [
    { required: true, message: '请输入组织名称', trigger: 'blur' }
  ],
  org_type: [
    { required: true, message: '请选择组织类型', trigger: 'change' }
  ]
}

// ========== 计算属性 ==========
const filteredTreeData = computed(() => {
  if (!treeSearch.value) return treeData.value
  const search = treeSearch.value.toLowerCase()
  const filter = (nodes) => {
    return nodes.filter(node => {
      const match = node.org_name.toLowerCase().includes(search) ||
                    node.org_code.toLowerCase().includes(search)
      if (match) return true
      if (node.children && node.children.length) {
        node.children = filter(node.children)
        return node.children.length > 0
      }
      return false
    })
  }
  return filter(JSON.parse(JSON.stringify(treeData.value)))
})

const treeDataForSelect = computed(() => {
  const addDisabled = (nodes, disabledId) => {
    return nodes.map(node => ({
      ...node,
      disabled: node.org_id === disabledId,
      children: node.children ? addDisabled(node.children, disabledId) : []
    }))
  }
  // 编辑时禁止选择自己作为父组织
  const disabledId = isEditOrg.value ? orgForm.value.org_id : null
  return addDisabled(treeData.value, disabledId)
})

// ========== 方法 ==========
const loadTree = async () => {
  loadingTree.value = true
  try {
    const res = await organizationAPI.getTree()
    treeData.value = res.data || res || []
  } catch (e) {
    console.error('加载组织树失败:', e)
    treeData.value = []
  } finally {
    loadingTree.value = false
  }
}

const loadUnassignedUsers = async () => {
  loadingMembers.value = true
  try {
    const res = await organizationAPI.getUnassignedUsers()
    unassignedUsers.value = res.data || res || []
    unassignedCount.value = unassignedUsers.value.length
  } catch (e) {
    console.error('加载未分配用户失败:', e)
  } finally {
    loadingMembers.value = false
  }
}

const loadOrgMembers = async () => {
  if (!selectedOrg.value) return
  loadingMembers.value = true
  try {
    const res = await organizationAPI.getMembers(selectedOrg.value.org_id, {
      include_children: includeChildren.value,
      search: memberSearch.value || undefined,
      page: membersPage.value,
      page_size: membersPageSize.value
    })
    const data = res.data || res
    orgMembers.value = data.members || []
    membersTotal.value = data.total || 0
  } catch (e) {
    console.error('加载组织成员失败:', e)
  } finally {
    loadingMembers.value = false
  }
}

const loadOrgRoles = async () => {
  if (!selectedOrg.value) return
  loadingRoles.value = true
  try {
    const res = await organizationAPI.getOrgRoles(selectedOrg.value.org_id)
    orgRoles.value = res.data || res || []
  } catch (e) {
    console.error('加载组织角色失败:', e)
  } finally {
    loadingRoles.value = false
  }
}

const loadAvailableRoles = async () => {
  try {
    const res = await axios.get('/admin/permissions/data-roles', { params: { is_active: true } })
    availableRoles.value = res.data || []
  } catch (e) {
    console.error('加载可用角色失败:', e)
  }
}

const onNodeClick = async (data) => {
  selectedOrgId.value = data.org_id
  selectedOrg.value = null
  detailTab.value = 'members'
  membersPage.value = 1
  
  try {
    const res = await organizationAPI.get(data.org_id)
    selectedOrg.value = res.data || res
    await loadOrgMembers()
    await loadOrgRoles()
  } catch (e) {
    ElMessage.error('加载组织详情失败')
  }
}

const selectUnassigned = () => {
  selectedOrgId.value = 'unassigned'
  selectedOrg.value = null
  loadUnassignedUsers()
}

const onPageChange = (page) => {
  membersPage.value = page
  loadOrgMembers()
}

const onSelectionChange = (selection) => {
  selectedUserIds.value = selection.map(u => u.user_id)
}

// 组织CRUD
const showCreateDialog = (parentNode) => {
  isEditOrg.value = false
  orgForm.value = {
    org_code: '',
    org_name: '',
    org_type: 'department',
    parent_id: parentNode?.org_id || null,
    sort_order: 0,
    description: ''
  }
  orgDialogVisible.value = true
}

const showEditDialog = (data) => {
  isEditOrg.value = true
  orgForm.value = {
    org_id: data.org_id,
    org_code: data.org_code,
    org_name: data.org_name,
    org_type: data.org_type,
    parent_id: data.parent_id,
    sort_order: data.sort_order || 0,
    description: data.description || ''
  }
  orgDialogVisible.value = true
}

const saveOrg = async () => {
  try {
    await orgFormRef.value.validate()
  } catch {
    return
  }
  
  savingOrg.value = true
  try {
    if (isEditOrg.value) {
      await organizationAPI.update(orgForm.value.org_id, {
        org_name: orgForm.value.org_name,
        org_type: orgForm.value.org_type,
        parent_id: orgForm.value.parent_id,
        sort_order: orgForm.value.sort_order,
        description: orgForm.value.description
      })
      ElMessage.success('组织已更新')
    } else {
      await organizationAPI.create(orgForm.value)
      ElMessage.success('组织已创建')
    }
    orgDialogVisible.value = false
    await loadTree()
    if (selectedOrg.value) {
      const res = await organizationAPI.get(selectedOrg.value.org_id)
      selectedOrg.value = res.data || res
    }
  } catch (e) {
    const msg = e.response?.data?.detail || e.message
    ElMessage.error('保存失败: ' + msg)
  } finally {
    savingOrg.value = false
  }
}

const deleteOrg = async (data) => {
  try {
    await ElMessageBox.confirm(
      `确定要删除组织"${data.org_name}"吗？`,
      '确认删除',
      { type: 'warning' }
    )
    await organizationAPI.delete(data.org_id)
    ElMessage.success('组织已删除')
    await loadTree()
    if (selectedOrg.value?.org_id === data.org_id) {
      selectedOrg.value = null
      selectedOrgId.value = null
    }
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error('删除失败: ' + (e.response?.data?.detail || e.message))
    }
  }
}

// 用户分配
const showAssignDialog = (user) => {
  assignUser.value = user
  assignForm.value = {
    user_id: user?.user_id || null,
    org_id: selectedOrg.value?.org_id || null,
    position: user?.position || ''
  }
  if (!user) {
    searchedUsers.value = []
  }
  assignDialogVisible.value = true
}

const searchUnassigned = async (query) => {
  if (!query) {
    searchedUsers.value = []
    return
  }
  try {
    const res = await organizationAPI.getUnassignedUsers({ search: query })
    searchedUsers.value = res.data || res || []
  } catch (e) {
    console.error('搜索用户失败:', e)
  }
}

const submitAssign = async () => {
  const userId = assignUser.value?.user_id || assignForm.value.user_id
  if (!userId) {
    ElMessage.warning('请选择用户')
    return
  }
  
  assigning.value = true
  try {
    await organizationAPI.assignUser(userId, assignForm.value.org_id, assignForm.value.position || undefined)
    ElMessage.success('分配成功')
    assignDialogVisible.value = false
    
    // 刷新数据
    await loadTree()
    if (selectedOrgId.value === 'unassigned') {
      await loadUnassignedUsers()
    } else if (selectedOrg.value) {
      await loadOrgMembers()
    }
  } catch (e) {
    ElMessage.error('分配失败: ' + (e.response?.data?.detail || e.message))
  } finally {
    assigning.value = false
  }
}

const showBatchAssignDialog = () => {
  batchAssignForm.value = { org_id: null, position: '' }
  batchAssignDialogVisible.value = true
}

const submitBatchAssign = async () => {
  if (!batchAssignForm.value.org_id) {
    ElMessage.warning('请选择目标组织')
    return
  }
  
  batchAssigning.value = true
  try {
    await organizationAPI.batchAssignUsers({
      user_ids: selectedUserIds.value,
      org_id: batchAssignForm.value.org_id,
      position: batchAssignForm.value.position || undefined
    })
    ElMessage.success('批量分配成功')
    batchAssignDialogVisible.value = false
    selectedUserIds.value = []
    
    await loadTree()
    if (selectedOrgId.value === 'unassigned') {
      await loadUnassignedUsers()
    }
  } catch (e) {
    ElMessage.error('批量分配失败')
  } finally {
    batchAssigning.value = false
  }
}

const removeFromOrg = async (user) => {
  try {
    await ElMessageBox.confirm(
      `确定要将"${user.username}"从组织中移除吗？`,
      '确认',
      { type: 'warning' }
    )
    await organizationAPI.assignUser(user.user_id, null, null)
    ElMessage.success('已移除')
    await loadTree()
    await loadOrgMembers()
    await loadUnassignedUsers()
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error('操作失败')
    }
  }
}

const getUsernameById = (userId) => {
  const user = unassignedUsers.value.find(u => u.user_id === userId) ||
               orgMembers.value.find(u => u.user_id === userId)
  return user?.username || userId
}

// 数据角色
const showAssignRoleDialog = () => {
  assignRoleForm.value = { role_id: null, inherit_to_children: true }
  loadAvailableRoles()
  assignRoleDialogVisible.value = true
}

const submitAssignRole = async () => {
  if (!assignRoleForm.value.role_id) {
    ElMessage.warning('请选择角色')
    return
  }
  
  assigningRole.value = true
  try {
    await organizationAPI.assignOrgRole(selectedOrg.value.org_id, assignRoleForm.value)
    ElMessage.success('分配成功')
    assignRoleDialogVisible.value = false
    await loadOrgRoles()
  } catch (e) {
    ElMessage.error('分配失败')
  } finally {
    assigningRole.value = false
  }
}

const removeOrgRole = async (role) => {
  try {
    await ElMessageBox.confirm(`确定要移除角色"${role.role_name}"吗？`, '确认', { type: 'warning' })
    await organizationAPI.removeOrgRole(selectedOrg.value.org_id, role.role_id)
    ElMessage.success('已移除')
    await loadOrgRoles()
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error('操作失败')
    }
  }
}

// 工具函数
const getOrgIcon = (type) => {
  const icons = { company: '🏢', department: '📁', team: '👥', group: '👤' }
  return icons[type] || '📁'
}

const getOrgTypeName = (type) => {
  const names = { company: '公司', department: '部门', team: '团队', group: '小组' }
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

// ========== 生命周期 ==========
onMounted(async () => {
  await loadTree()
  await loadUnassignedUsers()
})
</script>

<style scoped>
.organization-manage {
  height: 100%;
  background: #fff;
  border-radius: 8px;
}

.org-layout {
  display: flex;
  height: calc(100vh - 200px);
  min-height: 500px;
}

.org-tree-panel {
  width: 320px;
  border-right: 1px solid #ebeef5;
  display: flex;
  flex-direction: column;
}

.org-detail-panel {
  flex: 1;
  overflow: auto;
  padding: 16px;
}

.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  border-bottom: 1px solid #ebeef5;
}

.title {
  font-weight: 600;
  font-size: 15px;
}

.tree-toolbar {
  padding: 8px 12px;
}

.tree-container {
  flex: 1;
  overflow: auto;
  padding: 8px 12px;
}

.tree-node {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-radius: 4px;
  cursor: pointer;
  transition: background 0.2s;
}

.tree-node:hover {
  background: #f5f7fa;
}

.tree-node.active {
  background: #ecf5ff;
}

.unassigned-node {
  color: #909399;
}

.tree-node-content {
  display: flex;
  align-items: center;
  gap: 6px;
  flex: 1;
}

.node-icon {
  font-size: 14px;
}

.node-label {
  flex: 1;
}

.node-actions {
  display: inline-flex;
  gap: 8px;
  margin-left: 6px;
}

.action-icon {
  width: 24px;
  height: 24px;
  font-size: 16px;
  cursor: pointer;
  padding: 0;
  border-radius: 8px;
  color: #606266;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  transition: all 0.15s ease;
}

.action-icon:hover {
  color: #409eff;
  background: #ecf5ff;
}

.action-icon.danger:hover {
  color: #f56c6c;
  background: #fef0f0;
}

.org-info {
  display: flex;
  align-items: center;
  gap: 8px;
}

.org-icon {
  font-size: 20px;
}

.org-name {
  font-size: 16px;
  font-weight: 600;
}

.members-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.roles-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.section-desc {
  font-size: 13px;
  color: #909399;
}

.form-tip {
  font-size: 12px;
  color: #909399;
  margin-left: 8px;
}

.mr-4 {
  margin-right: 4px;
}

.mb-4 {
  margin-bottom: 4px;
}

:deep(.el-tree-node__content) {
  height: 36px;
}

:deep(.el-tabs__content) {
  padding: 12px 0;
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
  
  .toolbar {
    flex-direction: column;
    gap: 10px;
  }
  
  .toolbar .el-input,
  .toolbar .el-select {
    width: 100% !important;
  }
  
  :deep(.el-tree-node__content) {
    height: 40px;
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
}
</style>

