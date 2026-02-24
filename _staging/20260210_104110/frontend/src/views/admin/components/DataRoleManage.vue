<template>
  <div class="data-role-manage">
    <!-- 页面头部 -->
    <div class="page-header">
      <h2>数据权限管理</h2>
      <p class="description">管理数据角色（全局）、表权限和行级过滤规则</p>
    </div>

    <!-- 工具栏 -->
    <div class="toolbar">
      <div class="left">
        <el-input v-model="searchKeyword" placeholder="搜索角色" clearable style="width: 100%; max-width: 300px" @input="handleSearch">
          <template #prefix><el-icon><Search /></el-icon></template>
        </el-input>
        <el-select v-model="filterScopeType" placeholder="范围类型" clearable style="width: 100%; max-width: 150px; margin-left: 12px" @change="loadRoles">
          <el-option label="全部" value="" />
          <el-option label="全量访问" value="all" />
          <el-option label="受限访问" value="limited" />
        </el-select>
      </div>
      <div class="right">
        <el-button @click="exportPermissions">
          <el-icon><Download /></el-icon>导出配置
        </el-button>
        <el-button @click="showImportDialog">
          <el-icon><Upload /></el-icon>导入配置
        </el-button>
        <el-button type="primary" @click="showCreateDialog">
          <el-icon><Plus /></el-icon>新建角色
        </el-button>
      </div>
    </div>

    <!-- 角色列表 -->
    <el-table :data="roles" v-loading="loading" stripe style="width: 100%" table-layout="auto">
      <el-table-column prop="role_name" label="角色名称" min-width="120">
        <template #default="{ row }">
          <span class="name">{{ row.role_name }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="role_code" label="角色编码" min-width="120" />
      <el-table-column prop="scope_type" label="范围类型" width="110">
        <template #default="{ row }">
          <el-tag :type="row.scope_type === 'all' ? 'warning' : undefined" size="small">
            {{ row.scope_type === 'all' ? '全量访问' : '受限访问' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="description" label="描述" min-width="180" show-overflow-tooltip />
      <el-table-column label="状态" width="80" align="center">
        <template #default="{ row }">
          <el-tag :type="row.is_active ? 'success' : 'info'" size="small">
            {{ row.is_active ? '启用' : '禁用' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="统计" min-width="200">
        <template #default="{ row }">
          <div class="stats">
            <span><el-icon><User /></el-icon> {{ row.user_count }}人</span>
            <span><el-icon><Grid /></el-icon> {{ row.table_permission_count }}表</span>
            <span><el-icon><Filter /></el-icon> {{ row.row_filter_count }}规则</span>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="340" fixed="right">
        <template #default="{ row }">
          <div class="action-buttons">
            <el-button size="small" type="primary" @click="showPermissionConfig(row)">
              <el-icon><Setting /></el-icon>权限配置
            </el-button>
            <el-button size="small" @click="showEditDialog(row)">编辑</el-button>
            <el-button size="small" @click="showCloneDialog(row)">克隆</el-button>
            <el-button size="small" type="danger" @click="handleDelete(row)">删除</el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>

    <!-- 创建/编辑角色对话框 -->
    <el-dialog v-model="dialogVisible" :title="isEdit ? '编辑角色' : '新建角色'" width="90%" :style="{ maxWidth: '500px' }">
      <el-form :model="roleForm" :rules="roleRules" ref="roleFormRef" label-width="100px">
        <el-form-item label="角色名称" prop="role_name">
          <el-input v-model="roleForm.role_name" placeholder="如：销售部经理" />
        </el-form-item>
        <el-form-item label="角色编码" prop="role_code" v-if="!isEdit">
          <el-input v-model="roleForm.role_code" placeholder="如：sales_manager" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="roleForm.description" type="textarea" :rows="3" />
        </el-form-item>
        <el-form-item label="范围类型" prop="scope_type">
          <el-radio-group v-model="roleForm.scope_type">
            <el-radio label="limited">
              <span style="font-weight: 500;">受限访问</span>
              <span style="margin-left: 8px; color: #999; font-size: 12px;">需配置具体表权限</span>
            </el-radio>
            <el-radio label="all">
              <span style="font-weight: 500;">全量访问</span>
              <span style="margin-left: 8px; color: #999; font-size: 12px;">可访问所有数据源</span>
            </el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="默认角色">
          <el-switch v-model="roleForm.is_default" />
          <span class="form-tip">新用户自动分配此角色</span>
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="roleForm.is_active" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submitRole" :loading="submitting">保存</el-button>
      </template>
    </el-dialog>

    <!-- 权限配置抽屉 -->
    <el-drawer v-model="permDrawerVisible" :title="`权限配置 - ${currentRole?.role_name}`" size="75%">
      <el-tabs v-model="permActiveTab">
        <!-- 表权限 -->
        <el-tab-pane label="表权限" name="tables">
          <div class="perm-section">
            <div class="section-header">
              <span>已授权的表</span>
              <div class="section-actions">
                <el-button 
                  size="small" 
                  :disabled="selectedGrantedTables.length === 0" 
                  @click="batchRemoveTablePermissions"
                >
                  <el-icon><Delete /></el-icon>批量移除 ({{ selectedGrantedTables.length }})
                </el-button>
                <el-button type="primary" size="small" @click="showAddTableDialog" :disabled="!canAddTable">
                  <el-icon><Plus /></el-icon>添加表权限
                </el-button>
              </div>
            </div>
            <el-table 
              ref="grantedTablesTableRef"
              :data="grantedTables" 
              v-loading="tablesLoading" 
              max-height="400" 
              style="width: 100%" 
              table-layout="auto"
              @selection-change="handleGrantedTableSelectionChange"
            >
              <el-table-column type="selection" width="50" />
              <el-table-column prop="connection_name" label="连接" min-width="180">
                <template #default="{ row }">
                  {{ row.connection_name || '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="table_name" label="表名" min-width="200" />
              <el-table-column prop="display_name" label="显示名" min-width="150" />
              <el-table-column label="权限" min-width="150">
                <template #default="{ row }">
                  <el-tag size="small" type="success" v-if="row.can_query">可查询</el-tag>
                  <el-tag size="small" type="warning" v-if="row.can_export" style="margin-left: 4px">可导出</el-tag>
                </template>
              </el-table-column>
              <el-table-column label="列控制" min-width="120">
                <template #default="{ row }">
                  <el-tag size="small">{{ row.column_access_mode === 'whitelist' ? '白名单' : '黑名单' }}</el-tag>
                </template>
              </el-table-column>
              <el-table-column label="操作" min-width="150">
                <template #default="{ row }">
                  <el-button size="small" @click="showColumnConfig(row)">列配置</el-button>
                  <el-button size="small" type="danger" @click="removeTablePermission(row)">移除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </el-tab-pane>

        <!-- 行过滤规则 -->
        <el-tab-pane label="行级过滤" name="filters">
          <div class="perm-section">
            <div class="section-header">
              <span>行级过滤规则（OR合并）</span>
              <div class="section-actions">
                <el-button 
                  size="small" 
                  :disabled="selectedRowFilters.length === 0" 
                  @click="batchDeleteRowFilters"
                >
                  <el-icon><Delete /></el-icon>批量移除 ({{ selectedRowFilters.length }})
                </el-button>
                <el-button type="primary" size="small" @click="showAddFilterDialog">
                  <el-icon><Plus /></el-icon>添加规则
                </el-button>
              </div>
            </div>
            <el-table 
              ref="rowFiltersTableRef"
              :data="rowFilters" 
              v-loading="filtersLoading" 
              max-height="400" 
              style="width: 100%" 
              table-layout="auto"
              @selection-change="handleRowFilterSelectionChange"
            >
              <el-table-column type="selection" width="50" />
              <el-table-column prop="filter_name" label="规则名称" min-width="150" />
              <el-table-column prop="table_name" label="作用表" min-width="150">
                <template #default="{ row }">{{ row.table_name || '所有表' }}</template>
              </el-table-column>
            <el-table-column label="条件预览" min-width="200" show-overflow-tooltip>
              <template #default="{ row }">
                <code>{{ formatFilterPreview(row) }}</code>
              </template>
            </el-table-column>
              <el-table-column prop="is_active" label="状态" min-width="80">
                <template #default="{ row }">
                  <el-tag :type="row.is_active ? 'success' : 'info'" size="small">
                    {{ row.is_active ? '启用' : '禁用' }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="操作" min-width="120">
                <template #default="{ row }">
                  <el-button size="small" @click="editFilter(row)">编辑</el-button>
                  <el-button size="small" type="danger" @click="deleteFilter(row)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </el-tab-pane>

        <!-- 用户分配 -->
        <el-tab-pane label="用户分配" name="users">
          <div class="perm-section">
            <div class="section-header">
              <span>已分配用户</span>
              <div class="section-actions">
                <el-button 
                  size="small" 
                  :disabled="selectedRoleUsers.length === 0" 
                  @click="batchRemoveUsers"
                >
                  <el-icon><Delete /></el-icon>批量移除 ({{ selectedRoleUsers.length }})
                </el-button>
                <el-button type="primary" size="small" @click="showAssignUserDialog">
                  <el-icon><Plus /></el-icon>分配用户
                </el-button>
              </div>
            </div>
            <el-table 
              ref="roleUsersTableRef"
              :data="roleUsers" 
              v-loading="usersLoading" 
              max-height="400" 
              style="width: 100%" 
              table-layout="auto"
              @selection-change="handleRoleUserSelectionChange"
            >
              <el-table-column type="selection" width="50" />
              <el-table-column prop="username" label="用户名" min-width="150" />
              <el-table-column prop="full_name" label="姓名" min-width="120" />
              <el-table-column prop="email" label="邮箱" min-width="180" show-overflow-tooltip />
              <el-table-column prop="granted_at" label="授权时间" min-width="180">
                <template #default="{ row }">{{ formatDate(row.granted_at) }}</template>
              </el-table-column>
              <el-table-column label="操作" min-width="100">
                <template #default="{ row }">
                  <el-button size="small" type="danger" @click="removeUser(row)">移除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </el-tab-pane>
      </el-tabs>
    </el-drawer>

    <!-- 添加过滤规则对话框 -->
    <el-dialog v-model="filterDialogVisible" :title="editingFilter ? '编辑规则' : '添加规则'" width="90%" :style="{ maxWidth: '900px' }">
      <el-form :model="filterForm" :rules="filterRules" ref="filterFormRef" label-width="100px">
        <el-form-item label="规则名称" prop="filter_name">
          <el-input v-model="filterForm.filter_name" />
        </el-form-item>
        <el-form-item label="作用表">
          <el-select v-model="filterForm.table_id" placeholder="选择表（空=所有表）" clearable style="width: 100%" @change="handleFilterTableChange">
            <el-option v-for="t in availableTables" :key="t.table_id" :label="t.display_name || t.table_name" :value="t.table_id" />
          </el-select>
        </el-form-item>
        <el-form-item label="使用模板">
          <el-select v-model="selectedTemplate" placeholder="选择模板快速创建" clearable @change="applyTemplate" style="width: 100%">
            <el-option v-for="tpl in templates" :key="tpl.template_code" :label="tpl.template_name" :value="tpl.template_code">
              <span>{{ tpl.template_name }}</span>
              <span style="color: #999; margin-left: 8px">{{ tpl.description }}</span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="过滤条件">
          <div class="filter-builder">
            <div v-for="(cond, idx) in filterForm.conditions" :key="idx" class="condition-row">
              <el-select 
                v-model="cond.field_name" 
                placeholder="选择字段" 
                filterable
                style="flex: 1; min-width: 150px; max-width: 200px"
                :disabled="!filterForm.table_id"
              >
                <el-option 
                  v-for="field in tableFields" 
                  :key="field.field_id || field.column_name" 
                  :label="field.display_name || field.column_name" 
                  :value="field.column_name || field.field_id"
                />
              </el-select>
              <el-select v-model="cond.operator" style="flex: 0 0 auto; width: 120px">
                <el-option label="=" value="=" />
                <el-option label="!=" value="!=" />
                <el-option label="IN" value="IN" />
                <el-option label="NOT IN" value="NOT IN" />
                <el-option label=">" value=">" />
                <el-option label=">=" value=">=" />
                <el-option label="<" value="<" />
                <el-option label="<=" value="<=" />
                <el-option label="LIKE" value="LIKE" />
              </el-select>
              <el-select v-model="cond.value_type" style="flex: 0 0 auto; width: 120px">
                <el-option label="静态值" value="static" />
                <el-option label="用户属性" value="user_attr" />
              </el-select>
              <el-input v-model="cond.value" placeholder="值" style="flex: 1; min-width: 150px; max-width: 250px" />
              <el-button :icon="Delete" circle @click="removeCondition(idx)" />
            </div>
            <el-button size="small" @click="addCondition">
              <el-icon><Plus /></el-icon>添加条件
            </el-button>
          </div>
        </el-form-item>
        <el-form-item label="条件逻辑">
          <el-radio-group v-model="filterForm.logic">
            <el-radio label="AND">AND（全部满足）</el-radio>
            <el-radio label="OR">OR（满足任一）</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="filterDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submitFilter" :loading="submitting">保存</el-button>
      </template>
    </el-dialog>

    <!-- 添加表权限对话框 -->
    <el-dialog 
      v-model="addTableDialogVisible" 
      title="添加表权限" 
      width="90%"
      :style="{ maxWidth: '900px' }"
      :close-on-click-modal="false"
    >
      <div class="add-table-toolbar">
        <el-select
          v-model="addTableConnectionFilter"
          placeholder="筛选连接"
          clearable
          style="width: 100%; max-width: 220px;"
        >
          <el-option
            v-for="conn in allowedConnectionsForTables"
            :key="conn.connection_id"
            :label="conn.connection_name"
            :value="conn.connection_id"
          />
        </el-select>
        <el-input
          v-model="addTableSearchKeyword"
          placeholder="搜索表名、显示名或描述"
          clearable
          style="width: 100%; max-width: 350px;"
        >
          <template #prefix>
            <el-icon><Search /></el-icon>
          </template>
        </el-input>
      </div>
      <el-table 
        ref="addTableTableRef"
        :data="filteredUnauthorizedTables" 
        v-loading="availableTablesLoading" 
        max-height="450"
        style="width: 100%"
        table-layout="auto"
        @selection-change="handleTableSelectionChange"
      >
        <el-table-column type="selection" min-width="50" />
        <el-table-column prop="connection_name" label="连接" min-width="180">
          <template #default="{ row }">
            {{ row.connection_name || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="table_name" label="表名" min-width="200" />
        <el-table-column prop="display_name" label="显示名" min-width="150" />
        <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
      </el-table>
      <div style="margin-top: 12px; color: #909399; font-size: 12px;">
        已选择 {{ selectedTables.length }} 个表，授权后默认开启查询权限（黑名单模式）
      </div>
      <template #footer>
        <el-button @click="closeAddTableDialog">取消</el-button>
        <el-button 
          type="primary" 
          @click="saveTablePermissions" 
          :loading="submitting" 
          :disabled="selectedTables.length === 0"
        >
          确定添加
        </el-button>
      </template>
    </el-dialog>

    <!-- 分配用户对话框 -->
    <el-dialog 
      v-model="assignUserDialogVisible" 
      title="分配用户" 
      width="90%"
      :style="{ maxWidth: '900px' }"
      :close-on-click-modal="false"
    >
      <div style="margin-bottom: 12px;">
        <el-input
          v-model="addUserSearchKeyword"
          placeholder="搜索用户名、姓名或邮箱"
          clearable
          style="width: 100%; max-width: 350px;"
        >
          <template #prefix>
            <el-icon><Search /></el-icon>
          </template>
        </el-input>
      </div>
      <el-table 
        ref="addUserTableRef"
        :data="filteredUnassignedUsers" 
        v-loading="usersLoading" 
        max-height="450"
        style="width: 100%"
        table-layout="auto"
        @selection-change="handleUserSelectionChange"
      >
        <el-table-column type="selection" min-width="50" />
        <el-table-column prop="username" label="用户名" min-width="150" />
        <el-table-column prop="full_name" label="姓名" min-width="120" />
        <el-table-column prop="email" label="邮箱" min-width="180" show-overflow-tooltip />
        <el-table-column prop="role" label="系统角色" min-width="120">
          <template #default="{ row }">
            <el-tag 
              size="small" 
              :type="getUserRoleColor(row.role)"
            >
              {{ getUserRoleLabel(row.role) }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
      <div style="margin-top: 12px; color: #909399; font-size: 12px;">
        已选择 {{ selectedUsers.length }} 个用户
      </div>
      <template #footer>
        <el-button @click="closeAssignUserDialog">取消</el-button>
        <el-button 
          type="primary" 
          @click="assignUser" 
          :loading="submitting" 
          :disabled="selectedUsers.length === 0"
        >
          确定分配
        </el-button>
      </template>
    </el-dialog>

    <!-- 导入配置对话框 -->
    <el-dialog v-model="importDialogVisible" title="导入权限配置" width="90%" :style="{ maxWidth: '600px' }">
      <el-alert type="info" :closable="false" style="margin-bottom: 16px">
        <template #title>
          <div>导入将会：</div>
          <ul style="margin: 8px 0 0 20px; padding: 0">
            <li>创建新的数据角色（如果角色编码不存在）</li>
            <li>更新现有角色的配置（如果角色编码已存在且选择合并模式）</li>
            <li>导入表权限和行级过滤规则</li>
          </ul>
        </template>
      </el-alert>
      <el-form label-width="100px">
        <el-form-item label="导入模式">
          <el-radio-group v-model="importMergeMode">
            <el-radio :label="true">合并模式（保留现有角色，更新配置）</el-radio>
            <el-radio :label="false">覆盖模式（删除同名角色后重建）</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="配置文件">
          <el-upload
            ref="uploadRef"
            :auto-upload="false"
            :limit="1"
            accept=".json"
            :on-change="handleFileChange"
          >
            <template #trigger>
              <el-button type="primary">选择JSON文件</el-button>
            </template>
            <template #tip>
              <div class="el-upload__tip">仅支持导出的JSON格式文件</div>
            </template>
          </el-upload>
        </el-form-item>
        <el-form-item label="预览" v-if="importPreview">
          <div class="import-preview">
            <p><strong>角色数量：</strong>{{ importPreview.data_roles?.length || 0 }}</p>
            <p><strong>导出时间：</strong>{{ importPreview.exported_at }}</p>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="importDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="executeImport" :loading="importing" :disabled="!importPreview">
          确认导入
        </el-button>
      </template>
    </el-dialog>

    <!-- 克隆角色对话框 -->
    <el-dialog v-model="cloneDialogVisible" title="克隆角色" width="90%" :style="{ maxWidth: '450px' }">
      <el-form :model="cloneForm" label-width="100px">
        <el-form-item label="新角色名称">
          <el-input v-model="cloneForm.role_name" placeholder="如：销售部经理(副本)" />
        </el-form-item>
        <el-form-item label="新角色编码">
          <el-input v-model="cloneForm.role_code" placeholder="如：sales_manager_copy" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="cloneDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="executeClone" :loading="cloning">确认克隆</el-button>
      </template>
    </el-dialog>

    <!-- 列配置对话框 -->
    <el-dialog 
      v-model="columnConfigDialogVisible" 
      :title="`列配置 - ${currentTable?.display_name || currentTable?.table_name}`" 
      width="95%"
      :style="{ maxWidth: '1200px' }"
      :close-on-click-modal="false"
    >
      <el-tabs v-model="columnConfigTab">
        <!-- 基础配置 -->
        <el-tab-pane label="基础权限" name="basic">
          <el-form label-width="120px">
            <el-form-item label="查询权限">
              <el-switch v-model="columnConfig.can_query" />
              <span style="margin-left: 12px; color: #999; font-size: 13px;">允许在查询中使用此表</span>
            </el-form-item>
            <el-form-item label="导出权限">
              <el-switch v-model="columnConfig.can_export" />
              <span style="margin-left: 12px; color: #999; font-size: 13px;">允许导出此表的查询结果</span>
            </el-form-item>
          </el-form>
        </el-tab-pane>

        <!-- 列可见性 -->
        <el-tab-pane label="列可见性" name="visibility">
          <div style="margin-bottom: 16px;">
            <el-radio-group v-model="columnConfig.column_access_mode">
              <el-radio label="blacklist">
                <span style="font-weight: 500;">黑名单模式</span>
                <span style="margin-left: 8px; color: #999; font-size: 13px;">默认全部可见，明确禁止部分字段</span>
              </el-radio>
              <el-radio label="whitelist" style="margin-left: 24px;">
                <span style="font-weight: 500;">白名单模式</span>
                <span style="margin-left: 8px; color: #999; font-size: 13px;">默认全部不可见，明确允许部分字段</span>
              </el-radio>
            </el-radio-group>
          </div>

          <div style="margin-bottom: 12px;">
            <el-input
              v-model="columnFieldSearchKeyword"
              placeholder="搜索字段名、显示名或描述"
              clearable
              style="width: 100%; max-width: 350px;"
            >
              <template #prefix>
                <el-icon><Search /></el-icon>
              </template>
            </el-input>
          </div>

          <div v-if="columnConfig.column_access_mode === 'blacklist'" style="margin-top: 16px;">
            <el-alert type="info" :closable="false" style="margin-bottom: 12px;">
              未选中的字段默认可见，选中的字段将被隐藏
            </el-alert>
            <el-table 
              :data="filteredTableFields" 
              v-loading="fieldsLoading"
              @selection-change="handleExcludedFieldsChange"
              ref="excludedFieldsTableRef"
              max-height="400"
              style="width: 100%"
              table-layout="auto"
            >
              <el-table-column type="selection" min-width="50" />
              <el-table-column prop="column_name" label="字段名" min-width="200" />
              <el-table-column prop="display_name" label="显示名" min-width="150" />
              <el-table-column prop="data_type" label="数据类型" min-width="120" />
              <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
            </el-table>
          </div>

          <div v-else style="margin-top: 16px;">
            <el-alert type="warning" :closable="false" style="margin-bottom: 12px;">
              只有选中的字段可见，未选中的字段将被隐藏（更安全）
            </el-alert>
            <el-table 
              :data="filteredTableFields" 
              v-loading="fieldsLoading"
              @selection-change="handleIncludedFieldsChange"
              ref="includedFieldsTableRef"
              max-height="400"
              style="width: 100%"
              table-layout="auto"
            >
              <el-table-column type="selection" min-width="50" />
              <el-table-column prop="column_name" label="字段名" min-width="200" />
              <el-table-column prop="display_name" label="显示名" min-width="150" />
              <el-table-column prop="data_type" label="数据类型" min-width="120" />
              <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
            </el-table>
          </div>
        </el-tab-pane>

        <!-- 列脱敏 -->
        <el-tab-pane label="数据脱敏" name="masking">
          <el-alert type="info" :closable="false" style="margin-bottom: 16px;">
            脱敏字段在查询结果中会显示，但内容会被部分隐藏（如手机号：138****5678）
          </el-alert>
          <el-table 
            :data="filteredTableFields" 
            v-loading="fieldsLoading"
            @selection-change="handleMaskedFieldsChange"
            ref="maskedFieldsTableRef"
            max-height="400"
            style="width: 100%"
            table-layout="auto"
          >
            <el-table-column type="selection" min-width="50" />
            <el-table-column prop="column_name" label="字段名" min-width="200" />
            <el-table-column prop="display_name" label="显示名" min-width="150" />
            <el-table-column prop="data_type" label="数据类型" min-width="120" />
            <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
          </el-table>
        </el-tab-pane>
      </el-tabs>

      <template #footer>
        <el-button @click="closeColumnConfigDialog">取消</el-button>
        <el-button type="primary" @click="saveColumnConfig" :loading="submitting">保存配置</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed, watch, nextTick } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Search, Plus, User, Grid, Filter, Setting, Delete, Download, Upload } from '@element-plus/icons-vue'
import request from '@/utils/request'

// 状态
const loading = ref(false)
const submitting = ref(false)
const roles = ref([])
const searchKeyword = ref('')
const filterScopeType = ref('')
const dialogVisible = ref(false)
const isEdit = ref(false)
const roleFormRef = ref(null)

// 所有连接列表
const allConnections = ref([])

// 角色表单
const roleForm = reactive({
  role_name: '',
  role_code: '',
  description: '',
  scope_type: 'limited',
  is_default: false,
  is_active: true
})

const roleRules = {
  role_name: [{ required: true, message: '请输入角色名称', trigger: 'blur' }],
  role_code: [{ required: true, message: '请输入角色编码', trigger: 'blur' }],
  scope_type: [{ required: true, message: '请选择范围类型', trigger: 'change' }]
}

// 权限配置相关
const permDrawerVisible = ref(false)
const permActiveTab = ref('tables')
const currentRole = ref(null)

// 表权限相关
const tablesLoading = ref(false)
const availableTablesLoading = ref(false)
const availableTables = ref([])
const grantedTables = ref([])
const grantedTablesTableRef = ref(null)
const addTableDialogVisible = ref(false)
const addTableSearchKeyword = ref('')
const addTableConnectionFilter = ref('')
const addTableTableRef = ref(null)
const selectedTables = ref([])
const selectedGrantedTables = ref([])

// 行过滤相关
const filtersLoading = ref(false)
const rowFilters = ref([])
const rowFiltersTableRef = ref(null)
const selectedRowFilters = ref([])
const tableFieldLabelMap = ref({})
const templates = ref([])
const tableFields = ref([])
const fieldsLoading = ref(false)
const filterDialogVisible = ref(false)
const editingFilter = ref(null)
const filterFormRef = ref(null)
const selectedTemplate = ref('')
const filterForm = reactive({
  filter_name: '',
  table_id: null,
  conditions: [{ field_name: '', operator: '=', value_type: 'static', value: '' }],
  logic: 'AND'
})

const filterRules = {
  filter_name: [{ required: true, message: '请输入规则名称', trigger: 'blur' }]
}

// 用户分配相关
const usersLoading = ref(false)
const roleUsers = ref([])
const roleUsersTableRef = ref(null)
const assignUserDialogVisible = ref(false)
const allUsers = ref([])
const addUserSearchKeyword = ref('')
const addUserTableRef = ref(null)
const selectedUsers = ref([])
const selectedRoleUsers = ref([])

// 导入导出相关
const importDialogVisible = ref(false)
const importMergeMode = ref(true)
const importPreview = ref(null)
const importFile = ref(null)
const importing = ref(false)
const uploadRef = ref(null)

// 克隆相关
const cloneDialogVisible = ref(false)
const cloneForm = ref({ role_name: '', role_code: '' })
const cloneSourceRoleId = ref('')
const cloning = ref(false)

// 列配置相关
const columnConfigDialogVisible = ref(false)
const columnConfigTab = ref('basic')
const currentTable = ref(null)
const currentTableFields = ref([])
const columnFieldSearchKeyword = ref('')
const isSettingSelection = ref(false)
const columnConfig = reactive({
  can_query: true,
  can_export: false,
  column_access_mode: 'blacklist',
  included_column_ids: [],
  excluded_column_ids: [],
  masked_column_ids: [],
  restricted_filter_column_ids: [],
  restricted_aggregate_column_ids: [],
  restricted_group_by_column_ids: [],
  restricted_order_by_column_ids: []
})

const excludedFieldsTableRef = ref(null)
const includedFieldsTableRef = ref(null)
const maskedFieldsTableRef = ref(null)

// 计算属性
const filteredUnauthorizedTables = computed(() => {
  const grantedTableIds = new Set(grantedTables.value.map(t => t.table_id))
  let tables = availableTables.value.filter(t => !grantedTableIds.has(t.table_id))
  
  if (addTableConnectionFilter.value) {
    tables = tables.filter(table => table.connection_id === addTableConnectionFilter.value)
  }

  if (addTableSearchKeyword.value) {
    const keyword = addTableSearchKeyword.value.toLowerCase()
    tables = tables.filter(table => 
      (table.table_name && table.table_name.toLowerCase().includes(keyword)) ||
      (table.display_name && table.display_name.toLowerCase().includes(keyword)) ||
      (table.description && table.description.toLowerCase().includes(keyword)) ||
      (table.connection_name && table.connection_name.toLowerCase().includes(keyword))
    )
  }
  return tables
})

const unassignedUsers = computed(() => {
  const assignedUserIds = new Set(roleUsers.value.map(u => u.user_id))
  return allUsers.value.filter(u => !assignedUserIds.has(u.user_id))
})

const filteredUnassignedUsers = computed(() => {
  if (!addUserSearchKeyword.value) return unassignedUsers.value
  const keyword = addUserSearchKeyword.value.toLowerCase()
  return unassignedUsers.value.filter(user => 
    (user.username && user.username.toLowerCase().includes(keyword)) ||
    (user.full_name && user.full_name.toLowerCase().includes(keyword)) ||
    (user.email && user.email.toLowerCase().includes(keyword))
  )
})

const filteredTableFields = computed(() => {
  if (!columnFieldSearchKeyword.value) return currentTableFields.value
  const keyword = columnFieldSearchKeyword.value.toLowerCase()
  return currentTableFields.value.filter(field => 
    (field.column_name && field.column_name.toLowerCase().includes(keyword)) ||
    (field.display_name && field.display_name.toLowerCase().includes(keyword)) ||
    (field.description && field.description.toLowerCase().includes(keyword))
  )
})

const allowedConnectionsForTables = computed(() => allConnections.value || [])

const canAddTable = computed(() => allowedConnectionsForTables.value.length > 0)

// 加载所有连接
const loadAllConnections = async () => {
  try {
    const res = await request.get('/admin/connections/all')
    const raw = res.data
    allConnections.value = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
  } catch (error) {
    console.error('加载连接失败:', error)
  }
}

// 加载角色列表
const loadRoles = async () => {
  loading.value = true
  try {
    const params = {}
    if (searchKeyword.value) params.search = searchKeyword.value
    if (filterScopeType.value) params.scope_type = filterScopeType.value
    const res = await request.get('/admin/permissions/data-roles', { params })
    const raw = res.data
    roles.value = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
  } catch (error) {
    console.error('加载角色失败:', error)
    ElMessage.error('加载角色失败')
  } finally {
    loading.value = false
  }
}

// 搜索处理
let searchTimer = null
const handleSearch = () => {
  clearTimeout(searchTimer)
  searchTimer = setTimeout(() => loadRoles(), 300)
}

// 显示创建对话框
const showCreateDialog = () => {
  isEdit.value = false
  Object.assign(roleForm, {
    role_name: '',
    role_code: '',
    description: '',
    scope_type: 'limited',
    is_default: false,
    is_active: true
  })
  dialogVisible.value = true
}

// 显示编辑对话框
const showEditDialog = (row) => {
  isEdit.value = true
  Object.assign(roleForm, {
    role_id: row.role_id,
    role_name: row.role_name,
    description: row.description,
    scope_type: row.scope_type,
    is_default: row.is_default,
    is_active: row.is_active
  })
  dialogVisible.value = true
}

// 提交角色
const submitRole = async () => {
  if (!roleFormRef.value) return
  await roleFormRef.value.validate()
  
  submitting.value = true
  try {
    if (isEdit.value) {
      await request.put(`/admin/permissions/data-roles/${roleForm.role_id}`, {
        role_name: roleForm.role_name,
        description: roleForm.description,
        scope_type: roleForm.scope_type,
        is_default: roleForm.is_default,
        is_active: roleForm.is_active
      })
      ElMessage.success('更新成功')
    } else {
      await request.post('/admin/permissions/data-roles', roleForm)
      ElMessage.success('创建成功')
    }
    dialogVisible.value = false
    loadRoles()
  } catch (error) {
    console.error('保存失败:', error)
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    submitting.value = false
  }
}

// 删除角色
const handleDelete = async (row) => {
  try {
    await ElMessageBox.confirm(`确定要删除角色 "${row.role_name}" 吗？`, '确认删除', { type: 'warning' })
    await request.delete(`/admin/permissions/data-roles/${row.role_id}`)
    ElMessage.success('删除成功')
    loadRoles()
  } catch (error) {
    if (error !== 'cancel') console.error('删除失败:', error)
  }
}

// 显示权限配置
const showPermissionConfig = async (row) => {
  currentRole.value = row
  permDrawerVisible.value = true
  permActiveTab.value = 'tables'
  grantedTables.value = []
  
  await Promise.all([
    loadRowFilters(),
    loadRoleUsers(),
    loadTemplates(),
    loadGrantedTables()
  ])
}

// 加载已授权的表
const loadGrantedTables = async () => {
  if (!currentRole.value) {
    grantedTables.value = []
    return
  }
  
  tablesLoading.value = true
  try {
    const permRes = await request.get(`/admin/permissions/data-roles/${currentRole.value.role_id}/tables`)
    const permRaw = permRes.data
    grantedTables.value = Array.isArray(permRaw) ? permRaw : (permRaw && Array.isArray(permRaw.data) ? permRaw.data : [])
    nextTick(() => {
      selectedGrantedTables.value = []
      grantedTablesTableRef.value?.clearSelection()
    })
  } catch (error) {
    console.error('加载表失败:', error)
  } finally {
    tablesLoading.value = false
  }
}

// 加载可授权/可选的表（按角色可访问的连接聚合）
const loadAccessibleTables = async () => {
  if (!currentRole.value) return
  if (allConnections.value.length === 0) {
    await loadAllConnections()
  }
  const connections = allowedConnectionsForTables.value
  if (!connections.length) {
    availableTables.value = []
    return
  }

  availableTablesLoading.value = true
  try {
    const results = await Promise.all(
      connections.map(async (conn) => {
        try {
          const res = await request.get('/admin/tables', {
            params: { connection_id: conn.connection_id }
          })
          const raw = res.data
          const tables = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
          return tables.map(table => ({
            ...table,
            connection_name: conn.connection_name || table.connection_name
          }))
        } catch (error) {
          console.error(`加载连接 ${conn.connection_name} 的表失败:`, error)
          return []
        }
      })
    )
    availableTables.value = results.flat().filter(table => Boolean(table.table_id))
  } catch (error) {
    console.error('加载可授权表失败:', error)
    availableTables.value = []
  } finally {
    availableTablesLoading.value = false
  }
}

// 显示添加表权限对话框
const showAddTableDialog = async () => {
  if (!canAddTable.value) {
    ElMessage.warning('暂无可用连接，请先创建数据源')
    return
  }
  selectedTables.value = []
  addTableSearchKeyword.value = ''
  addTableConnectionFilter.value = ''
  addTableDialogVisible.value = true
  await loadAccessibleTables()
}

// 关闭添加表对话框
const closeAddTableDialog = () => {
  addTableDialogVisible.value = false
  selectedTables.value = []
  addTableSearchKeyword.value = ''
  addTableConnectionFilter.value = ''
}

// 处理表选择
const handleTableSelectionChange = (selection) => {
  selectedTables.value = selection
}

const handleGrantedTableSelectionChange = (selection) => {
  selectedGrantedTables.value = selection
}

// 保存表权限
const saveTablePermissions = async () => {
  if (selectedTables.value.length === 0) return
  
  submitting.value = true
  try {
    const permissions = selectedTables.value.map(table => ({
      role_id: currentRole.value.role_id,
      table_id: table.table_id,
      can_query: true,
      can_export: false,
      column_access_mode: 'blacklist'
    }))
    
    await request.put(`/admin/permissions/data-roles/${currentRole.value.role_id}/tables`, permissions)
    ElMessage.success(`成功添加 ${selectedTables.value.length} 个表权限`)
    closeAddTableDialog()
    await loadGrantedTables()
    loadRoles()
  } catch (error) {
    console.error('添加表权限失败:', error)
    ElMessage.error('添加失败')
  } finally {
    submitting.value = false
  }
}

// 移除表权限
const removeTablePermission = async (row) => {
  try {
    await ElMessageBox.confirm(`确定要移除表 "${row.display_name || row.table_name}" 的权限吗？`, '确认移除', { type: 'warning' })
    await request.delete(`/admin/permissions/data-roles/${currentRole.value.role_id}/tables/${row.table_id}`)
    ElMessage.success('移除成功')
    await loadGrantedTables()
    loadRoles()
  } catch (error) {
    if (error !== 'cancel') console.error('移除失败:', error)
  }
}

const batchRemoveTablePermissions = async () => {
  if (!selectedGrantedTables.value.length || !currentRole.value) return
  try {
    await ElMessageBox.confirm(
      `确定要移除选中的 ${selectedGrantedTables.value.length} 个表权限吗？`,
      '批量移除',
      { type: 'warning' }
    )
    submitting.value = true
    await Promise.all(
      selectedGrantedTables.value.map(table =>
        request.delete(`/admin/permissions/data-roles/${currentRole.value.role_id}/tables/${table.table_id}`)
      )
    )
    ElMessage.success(`已移除 ${selectedGrantedTables.value.length} 个表`)
    selectedGrantedTables.value = []
    await loadGrantedTables()
    loadRoles()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('批量移除表失败:', error)
      ElMessage.error('批量移除失败')
    }
  } finally {
    submitting.value = false
  }
}

// 加载行过滤规则
const loadRowFilters = async () => {
  if (!currentRole.value) return
  filtersLoading.value = true
  try {
    const res = await request.get(`/admin/permissions/data-roles/${currentRole.value.role_id}/row-filters`)
    const raw = res.data
    rowFilters.value = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
    await preloadRowFilterFieldLabels(rowFilters.value)
    nextTick(() => {
      selectedRowFilters.value = []
      rowFiltersTableRef.value?.clearSelection()
    })
  } catch (error) {
    console.error('加载规则失败:', error)
  } finally {
    filtersLoading.value = false
  }
}

// 加载角色用户
const loadRoleUsers = async () => {
  if (!currentRole.value) return
  usersLoading.value = true
  try {
    const res = await request.get(`/admin/permissions/data-roles/${currentRole.value.role_id}/users`)
    const raw = res.data
    roleUsers.value = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
    nextTick(() => {
      selectedRoleUsers.value = []
      roleUsersTableRef.value?.clearSelection()
    })
  } catch (error) {
    console.error('加载用户失败:', error)
  } finally {
    usersLoading.value = false
  }
}

// 加载模板
const loadTemplates = async () => {
  try {
    const res = await request.get('/admin/permissions/rls-templates')
    const raw = res.data
    templates.value = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
  } catch (error) {
    console.error('加载模板失败:', error)
  }
}

const ensureTableFieldLabels = async (tableId) => {
  if (!tableId) return
  if (tableFieldLabelMap.value[tableId]) return
  try {
    const res = await request.get('/admin/fields', {
      params: { table_id: tableId, is_active: true }
    })
    const raw = res.data
    const fields = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
    const labelMap = {}
    fields.forEach(field => {
      const label = field.display_name || field.column_name || field.field_id
      if (field.field_id) labelMap[field.field_id] = label
      if (field.column_name) labelMap[field.column_name] = label
    })
    tableFieldLabelMap.value = {
      ...tableFieldLabelMap.value,
      [tableId]: labelMap
    }
  } catch (error) {
    console.error('加载字段名称失败:', error)
  }
}

const preloadRowFilterFieldLabels = async (filters = []) => {
  const tableIds = Array.from(new Set(filters.map(f => f.table_id).filter(Boolean)))
  if (!tableIds.length) return
  await Promise.all(tableIds.map(id => ensureTableFieldLabels(id)))
}

const getFieldLabel = (tableId, fieldName) => {
  if (!fieldName) return '-'
  if (!tableId) return fieldName
  const map = tableFieldLabelMap.value[tableId] || {}
  return map[fieldName] || fieldName
}

const formatFilterPreview = (row) => {
  const def = row?.filter_definition
  if (!def || !Array.isArray(def.conditions) || def.conditions.length === 0) return '-'
  const logic = def.logic || 'AND'
  return def.conditions
    .map(cond => {
      const fieldLabel = getFieldLabel(row?.table_id, cond.field_name)
      const rawValue = cond.value
      let valueText = ''
      if (cond.value_type === 'user_attr') {
        valueText = `@${rawValue}`
      } else if (Array.isArray(rawValue)) {
        valueText = `(${rawValue.join(', ')})`
      } else {
        valueText = rawValue ?? ''
      }
      return `${fieldLabel} ${cond.operator} ${valueText}`
    })
    .join(` ${logic} `)
}

// 显示添加过滤规则对话框
const showAddFilterDialog = async () => {
  editingFilter.value = null
  selectedTemplate.value = ''
  tableFields.value = []
  Object.assign(filterForm, {
    filter_name: '',
    table_id: null,
    conditions: [{ field_name: '', operator: '=', value_type: 'static', value: '' }],
    logic: 'AND'
  })
  await loadAccessibleTables()
  filterDialogVisible.value = true
}

// 处理过滤规则表变更
const handleFilterTableChange = async (tableId) => {
  tableFields.value = []
  if (!tableId) return
  
  fieldsLoading.value = true
  try {
    const res = await request.get('/admin/fields', {
      params: { table_id: tableId, is_active: true }
    })
    const raw = res.data
    const fields = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
    tableFields.value = fields.map(f => ({
      column_name: f.column_name,
      display_name: f.display_name || f.column_name || f.field_id,
      field_id: f.field_id,
      data_type: f.data_type
    }))
  } catch (error) {
    console.error('加载字段失败:', error)
  } finally {
    fieldsLoading.value = false
  }
}

// 编辑过滤规则
const editFilter = async (row) => {
  editingFilter.value = row
  Object.assign(filterForm, {
    filter_name: row.filter_name,
    table_id: row.table_id,
    conditions: row.filter_definition?.conditions || [],
    logic: row.filter_definition?.logic || 'AND'
  })
  await loadAccessibleTables()
  filterDialogVisible.value = true
  if (row.table_id) {
    await handleFilterTableChange(row.table_id)
  }
}

// 应用模板
const applyTemplate = () => {
  const tpl = templates.value.find(t => t.template_code === selectedTemplate.value)
  if (tpl && tpl.template_definition) {
    filterForm.conditions = tpl.template_definition.conditions || []
    filterForm.logic = tpl.template_definition.logic || 'AND'
  }
}

// 添加条件
const addCondition = () => {
  filterForm.conditions.push({ field_name: '', operator: '=', value_type: 'static', value: '' })
}

// 删除条件
const removeCondition = (idx) => {
  filterForm.conditions.splice(idx, 1)
}

// 提交过滤规则
const submitFilter = async () => {
  if (!filterFormRef.value) return
  await filterFormRef.value.validate()

  if (!currentRole.value?.role_id) {
    ElMessage.error('当前角色信息缺失，请重新打开权限配置')
    return
  }
  
  submitting.value = true
  try {
    const data = {
      filter_name: filterForm.filter_name,
      table_id: filterForm.table_id,
      filter_definition: {
        conditions: filterForm.conditions,
        logic: filterForm.logic
      },
      is_active: true
    }

    if (!editingFilter.value) {
      data.role_id = currentRole.value.role_id
    }
    
    if (editingFilter.value) {
      await request.put(`/admin/permissions/row-filters/${editingFilter.value.filter_id}`, data)
      ElMessage.success('更新成功')
    } else {
      await request.post(`/admin/permissions/data-roles/${currentRole.value.role_id}/row-filters`, data)
      ElMessage.success('创建成功')
    }
    filterDialogVisible.value = false
    loadRowFilters()
  } catch (error) {
    console.error('保存失败:', error)
  } finally {
    submitting.value = false
  }
}

// 删除过滤规则
const deleteFilter = async (row) => {
  try {
    await ElMessageBox.confirm('确定要删除此规则吗？', '确认删除', { type: 'warning' })
    await request.delete(`/admin/permissions/row-filters/${row.filter_id}`)
    ElMessage.success('删除成功')
    loadRowFilters()
  } catch (error) {
    if (error !== 'cancel') console.error('删除失败:', error)
  }
}

const handleRowFilterSelectionChange = (selection) => {
  selectedRowFilters.value = selection
}

const batchDeleteRowFilters = async () => {
  if (!selectedRowFilters.value.length) return
  try {
    await ElMessageBox.confirm(
      `确定删除选中的 ${selectedRowFilters.value.length} 条规则吗？`,
      '批量删除',
      { type: 'warning' }
    )
    submitting.value = true
    await Promise.all(
      selectedRowFilters.value.map(filter => 
        request.delete(`/admin/permissions/row-filters/${filter.filter_id}`)
      )
    )
    ElMessage.success(`已删除 ${selectedRowFilters.value.length} 条规则`)
    selectedRowFilters.value = []
    loadRowFilters()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('批量删除规则失败:', error)
      ElMessage.error('批量删除失败')
    }
  } finally {
    submitting.value = false
  }
}

// 显示分配用户对话框
const showAssignUserDialog = async () => {
  try {
    usersLoading.value = true
    // 确保 roleUsers 已加载（如果还没有加载）
    if (!roleUsers.value || roleUsers.value.length === 0) {
      await loadRoleUsers()
    }
    // 重新加载所有用户数据，确保数据是最新的
    const res = await request.get('/admin/users')
    const raw = res.data
    allUsers.value = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
    selectedUsers.value = []
    addUserSearchKeyword.value = ''
    assignUserDialogVisible.value = true
  } catch (error) {
    console.error('加载用户失败:', error)
    ElMessage.error('加载用户列表失败')
  } finally {
    usersLoading.value = false
  }
}

// 关闭分配用户对话框
const closeAssignUserDialog = () => {
  assignUserDialogVisible.value = false
  selectedUsers.value = []
  addUserSearchKeyword.value = ''
}

// 处理用户选择
const handleUserSelectionChange = (selection) => {
  selectedUsers.value = selection
}

// 分配用户
const assignUser = async () => {
  if (selectedUsers.value.length === 0) return
  
  submitting.value = true
  try {
    const promises = selectedUsers.value.map(user => 
      request.post(`/admin/permissions/users/${user.user_id}/data-roles`, {
        role_id: currentRole.value.role_id
      })
    )
    await Promise.all(promises)
    ElMessage.success(`成功分配 ${selectedUsers.value.length} 个用户`)
    closeAssignUserDialog()
    loadRoleUsers()
    loadRoles()
  } catch (error) {
    console.error('分配失败:', error)
    ElMessage.error('分配失败')
  } finally {
    submitting.value = false
  }
}

// 移除用户
const removeUser = async (row) => {
  try {
    await ElMessageBox.confirm(`确定要移除用户 "${row.username}" 吗？`, '确认移除', { type: 'warning' })
    await request.delete(`/admin/permissions/users/${row.user_id}/data-roles/${currentRole.value.role_id}`)
    ElMessage.success('移除成功')
    loadRoleUsers()
    loadRoles()
  } catch (error) {
    if (error !== 'cancel') console.error('移除失败:', error)
  }
}

const handleRoleUserSelectionChange = (selection) => {
  selectedRoleUsers.value = selection
}

const batchRemoveUsers = async () => {
  if (!selectedRoleUsers.value.length || !currentRole.value) return
  try {
    await ElMessageBox.confirm(
      `确定要移除选中的 ${selectedRoleUsers.value.length} 个用户吗？`,
      '批量移除',
      { type: 'warning' }
    )
    submitting.value = true
    await Promise.all(
      selectedRoleUsers.value.map(user => 
        request.delete(`/admin/permissions/users/${user.user_id}/data-roles/${currentRole.value.role_id}`)
      )
    )
    ElMessage.success(`已移除 ${selectedRoleUsers.value.length} 个用户`)
    selectedRoleUsers.value = []
    await loadRoleUsers()
    loadRoles()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('批量移除用户失败:', error)
      ElMessage.error('批量移除失败')
    }
  } finally {
    submitting.value = false
  }
}

// 导出权限
const exportPermissions = async () => {
  try {
    const response = await request.get('/admin/permissions/export', { responseType: 'blob' })
    const blob = new Blob([response.data], { type: 'application/json' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = 'permissions_all.json'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
    ElMessage.success('导出成功')
  } catch (error) {
    ElMessage.error('导出失败')
  }
}

// 显示导入对话框
const showImportDialog = () => {
  importPreview.value = null
  importFile.value = null
  importMergeMode.value = true
  importDialogVisible.value = true
}

// 处理文件选择
const handleFileChange = (file) => {
  const reader = new FileReader()
  reader.onload = (e) => {
    try {
      const data = JSON.parse(e.target.result)
      importPreview.value = data
      importFile.value = data
    } catch (err) {
      ElMessage.error('文件格式错误')
      importPreview.value = null
    }
  }
  reader.readAsText(file.raw)
}

// 执行导入
const executeImport = async () => {
  if (!importFile.value) return
  
  importing.value = true
  try {
    const res = await request.post(`/admin/permissions/import?merge_mode=${importMergeMode.value}`, importFile.value)
    if (res.data.success) {
      ElMessage.success(`导入成功：${res.data.roles_imported}个角色`)
      importDialogVisible.value = false
      loadRoles()
    } else {
      ElMessage.error('导入失败：' + (res.data.errors?.join(', ') || '未知错误'))
    }
  } catch (error) {
    ElMessage.error('导入失败')
  } finally {
    importing.value = false
  }
}

// 显示克隆对话框
const showCloneDialog = (row) => {
  cloneSourceRoleId.value = row.role_id
  cloneForm.value = {
    role_name: `${row.role_name}(副本)`,
    role_code: `${row.role_code}_copy`
  }
  cloneDialogVisible.value = true
}

// 执行克隆
const executeClone = async () => {
  if (!cloneForm.value.role_name || !cloneForm.value.role_code) {
    ElMessage.warning('请填写新角色名称和编码')
    return
  }
  
  cloning.value = true
  try {
    await request.post(
      `/admin/permissions/clone/${cloneSourceRoleId.value}?new_role_code=${encodeURIComponent(cloneForm.value.role_code)}&new_role_name=${encodeURIComponent(cloneForm.value.role_name)}`
    )
    ElMessage.success('克隆成功')
    cloneDialogVisible.value = false
    loadRoles()
  } catch (error) {
    ElMessage.error('克隆失败')
  } finally {
    cloning.value = false
  }
}

// 列配置相关
const showColumnConfig = async (table) => {
  currentTable.value = table
  columnConfigTab.value = 'basic'
  
  Object.assign(columnConfig, {
    can_query: table.can_query ?? true,
    can_export: table.can_export ?? false,
    column_access_mode: table.column_access_mode || 'blacklist',
    included_column_ids: table.included_column_ids || [],
    excluded_column_ids: table.excluded_column_ids || [],
    masked_column_ids: table.masked_column_ids || [],
    restricted_filter_column_ids: table.restricted_filter_column_ids || [],
    restricted_aggregate_column_ids: table.restricted_aggregate_column_ids || [],
    restricted_group_by_column_ids: table.restricted_group_by_column_ids || [],
    restricted_order_by_column_ids: table.restricted_order_by_column_ids || []
  })
  
  columnConfigDialogVisible.value = true
  await loadTableFields(table.table_id)
  
  setTimeout(() => setFieldSelections(), 100)
}

// 加载表字段
const loadTableFields = async (tableId) => {
  fieldsLoading.value = true
  currentTableFields.value = []
  columnFieldSearchKeyword.value = ''
  
  try {
    const res = await request.get('/admin/fields', {
      params: { table_id: tableId, is_active: true }
    })
    const raw = res.data
    const fieldsData = Array.isArray(raw) ? raw : (raw && Array.isArray(raw.data) ? raw.data : [])
    
    currentTableFields.value = fieldsData
      .filter(f => f.is_active === true && f.field_id)
      .map(f => ({
        field_id: f.field_id,
        column_name: f.column_name,
        display_name: f.display_name || f.column_name,
        data_type: f.data_type,
        description: f.description
      }))
  } catch (error) {
    console.error('加载字段失败:', error)
  } finally {
    fieldsLoading.value = false
  }
}

// 设置字段选中状态
const setFieldSelections = () => {
  isSettingSelection.value = true
  try {
    if (excludedFieldsTableRef.value && columnConfig.excluded_column_ids.length > 0) {
      const fields = currentTableFields.value.filter(f => columnConfig.excluded_column_ids.includes(f.field_id))
      fields.forEach(field => excludedFieldsTableRef.value.toggleRowSelection(field, true))
    }
    if (includedFieldsTableRef.value && columnConfig.included_column_ids.length > 0) {
      const fields = currentTableFields.value.filter(f => columnConfig.included_column_ids.includes(f.field_id))
      fields.forEach(field => includedFieldsTableRef.value.toggleRowSelection(field, true))
    }
    if (maskedFieldsTableRef.value && columnConfig.masked_column_ids.length > 0) {
      const fields = currentTableFields.value.filter(f => columnConfig.masked_column_ids.includes(f.field_id))
      fields.forEach(field => maskedFieldsTableRef.value.toggleRowSelection(field, true))
    }
  } finally {
    isSettingSelection.value = false
  }
}

// 字段选择变更处理
const handleExcludedFieldsChange = (selection) => {
  if (isSettingSelection.value) return
  const visibleFieldIds = new Set(filteredTableFields.value.map(f => f.field_id))
  const hiddenSelected = columnConfig.excluded_column_ids.filter(id => !visibleFieldIds.has(id))
  const currentSelected = selection.map(f => f.field_id)
  columnConfig.excluded_column_ids = Array.from(new Set([...hiddenSelected, ...currentSelected]))
}

const handleIncludedFieldsChange = (selection) => {
  if (isSettingSelection.value) return
  const visibleFieldIds = new Set(filteredTableFields.value.map(f => f.field_id))
  const hiddenSelected = columnConfig.included_column_ids.filter(id => !visibleFieldIds.has(id))
  const currentSelected = selection.map(f => f.field_id)
  columnConfig.included_column_ids = Array.from(new Set([...hiddenSelected, ...currentSelected]))
}

const handleMaskedFieldsChange = (selection) => {
  if (isSettingSelection.value) return
  const visibleFieldIds = new Set(filteredTableFields.value.map(f => f.field_id))
  const hiddenSelected = columnConfig.masked_column_ids.filter(id => !visibleFieldIds.has(id))
  const currentSelected = selection.map(f => f.field_id)
  columnConfig.masked_column_ids = Array.from(new Set([...hiddenSelected, ...currentSelected]))
}

// 关闭列配置对话框
const closeColumnConfigDialog = () => {
  columnConfigDialogVisible.value = false
  currentTable.value = null
  currentTableFields.value = []
}

// 保存列配置
const saveColumnConfig = async () => {
  submitting.value = true
  try {
    await request.put(
      `/admin/permissions/data-roles/${currentRole.value.role_id}/tables/${currentTable.value.table_id}`,
      columnConfig
    )
    ElMessage.success('列配置保存成功')
    closeColumnConfigDialog()
    await loadGrantedTables()
  } catch (error) {
    console.error('保存列配置失败:', error)
    ElMessage.error('保存失败')
  } finally {
    submitting.value = false
  }
}

// 格式化日期
// 获取用户角色颜色
const getUserRoleColor = (role) => {
  const colors = {
    admin: 'danger',
    data_admin: 'warning',
    user: 'info',
    viewer: 'info'
  }
  return colors[role] || ''
}

// 获取用户角色标签
const getUserRoleLabel = (role) => {
  const labels = {
    admin: '系统管理员',
    data_admin: '数据管理员',
    user: '普通用户',
    viewer: '只读用户'
  }
  return labels[role] || role
}

const formatDate = (date) => {
  if (!date) return '-'
  return new Date(date).toLocaleString('zh-CN')
}

// 监听搜索和tab切换
watch(columnFieldSearchKeyword, () => {
  nextTick(() => {
    isSettingSelection.value = true
    try {
      const updateSelection = (tableRef, selectedIds) => {
        if (!tableRef) return
        tableRef.clearSelection()
        const selectedSet = new Set(selectedIds)
        filteredTableFields.value.forEach(field => {
          if (selectedSet.has(field.field_id)) {
            tableRef.toggleRowSelection(field, true)
          }
        })
      }
      
      if (columnConfig.column_access_mode === 'blacklist') {
        updateSelection(excludedFieldsTableRef.value, columnConfig.excluded_column_ids)
      } else {
        updateSelection(includedFieldsTableRef.value, columnConfig.included_column_ids)
      }
      updateSelection(maskedFieldsTableRef.value, columnConfig.masked_column_ids)
    } finally {
      isSettingSelection.value = false
    }
  })
})

onMounted(() => {
  loadAllConnections()
  loadRoles()
})
</script>

<style scoped>
.data-role-manage {
  padding: 20px;
}

.page-header {
  margin-bottom: 24px;
}

.page-header h2 {
  margin: 0 0 8px 0;
  font-size: 20px;
}

.page-header .description {
  margin: 0;
  color: #666;
  font-size: 14px;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.toolbar .left {
  display: flex;
  align-items: center;
}

.add-table-toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-bottom: 12px;
}

.role-name {
  display: flex;
  align-items: center;
  gap: 8px;
}

.stats {
  display: flex;
  gap: 12px;
  font-size: 12px;
  color: #666;
}

.stats span {
  display: flex;
  align-items: center;
  gap: 4px;
}

.action-buttons {
  display: flex;
  gap: 8px;
  flex-wrap: nowrap;
  align-items: center;
  justify-content: flex-start;
}

/* 减少操作列单元格的 padding */
::deep(.el-table__body-wrapper .el-table__body tr td:last-child) {
  padding-left: 12px;
  padding-right: 12px;
}

.form-tip {
  margin-left: 8px;
  font-size: 12px;
  color: #999;
}

.perm-section {
  padding: 16px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  font-weight: 500;
}

.section-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.add-dialog-toolbar {
  display: flex;
  flex-wrap: wrap;
  margin-bottom: 12px;
}

.filter-builder {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.condition-row {
  display: flex;
  gap: 8px;
  align-items: center;
}

code {
  background: #f5f5f5;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 12px;
}

.import-preview {
  padding: 12px;
  background: #f5f7fa;
  border-radius: 6px;
  font-size: 14px;
}

.import-preview p {
  margin: 4px 0;
}

/* 确保表格填满容器 */
.data-role-manage {
  width: 100%;
}

.data-role-manage .el-card {
  width: 100%;
}

::deep(.el-table) {
  width: 100%;
}

::deep(.el-table__body-wrapper) {
  width: 100%;
}

/* 响应式设计 */
@media screen and (max-width: 1400px) {
  .stats {
    flex-direction: column;
    gap: 4px;
  }
  
  .role-name {
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }
}

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
  
  .stats {
    font-size: 12px;
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
  
  .stats {
    font-size: 11px;
  }
}
</style>

