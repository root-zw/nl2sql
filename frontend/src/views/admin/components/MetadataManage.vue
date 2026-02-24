<template>
  <div class="metadata-manage">
    <!-- 工具栏区域（统一元数据架构，无需选择数据源） -->
    <el-card class="selector-card">
      <div class="selector-content">
        <!-- 左侧：操作按钮 -->
        <div class="selector-left">
          <!-- 数据源过滤器（可选） -->
          <span class="selector-label">筛选数据源</span>
          <el-select
            v-model="currentConnectionId"
            placeholder="全部数据源"
            @change="handleConnectionChange"
            class="connection-select"
            filterable
            clearable
          >
            <el-option
              v-for="conn in connections"
              :key="conn.connection_id"
              :label="conn.connection_name"
              :value="conn.connection_id"
            >
              <span class="conn-option">
                <span>{{ getDbIcon(conn.db_type) }} {{ conn.connection_name }}</span>
              </span>
            </el-option>
          </el-select>
          
          <!-- 表关系和规则按钮（全局操作） -->
          <el-button-group class="action-group">
            <el-tooltip content="管理表之间的关联关系" placement="bottom">
              <el-button @click="showRelationshipsDialog">
                <el-icon><Link /></el-icon>
                表关系
              </el-button>
            </el-tooltip>
            <el-tooltip content="配置派生指标、枚举展开等规则" placement="bottom">
              <el-button @click="showRulesDialog">
                <el-icon><Setting /></el-icon>
                全局规则
              </el-button>
            </el-tooltip>
          </el-button-group>
        </div>
        
        <!-- 右侧：操作按钮组 -->
        <div class="selector-right">
          <!-- 导入导出 -->
          <el-dropdown trigger="click" @command="handleIOCommand">
            <el-button type="success" plain>
              <el-icon><DocumentCopy /></el-icon>
              导入导出
              <el-icon class="el-icon--right"><ArrowDown /></el-icon>
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="export">
                  <el-icon><Download /></el-icon>
                  导出配置
                </el-dropdown-item>
                <el-dropdown-item command="import" divided>
                  <el-icon><Upload /></el-icon>
                  导入配置
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
      </div>
    </el-card>

    <!-- 主内容区域（始终显示，无需选择数据源） -->
    <!-- 左右布局：树形导航 + 配置面板 -->
    <el-card class="content-card">
      <el-row :gutter="20" class="tree-layout">
        <!-- 左侧：树形结构 -->
        <el-col :xs="24" :sm="24" :md="8" :lg="6" :xl="6">
            <div class="tree-panel">
              <div class="tree-header">
                <span class="tree-title">元数据结构</span>
                <el-space>
                  <el-button 
                    type="primary" 
                    :icon="Plus" 
                    size="small"
                    @click="handleAddDomain"
                  >
                    创建业务域
                  </el-button>
                  <el-button 
                    :icon="Refresh" 
                    size="small" 
                    text 
                    @click="loadTreeData"
                    :loading="treeLoading"
                  >
                    刷新
                  </el-button>
                </el-space>
              </div>
              <el-divider style="margin: 12px 0;" />
              
              <!-- 状态过滤器 -->
              <div style="margin-bottom: 12px;">
                <el-radio-group v-model="statusFilter" size="small">
                  <el-radio-button label="enabled">启用</el-radio-button>
                  <el-radio-button label="disabled">禁用</el-radio-button>
                  <el-radio-button label="all">全部</el-radio-button>
                </el-radio-group>
              </div>

              <!-- 树形视图 -->
              <el-tree
                ref="treeRef"
                v-loading="treeLoading"
                :data="filteredTreeData"
                :props="treeProps"
                node-key="id"
                :default-expand-all="false"
                :expand-on-click-node="false"
                highlight-current
                @node-click="handleNodeClick"
                @node-contextmenu="handleNodeContextMenu"
                @node-expand="handleNodeExpand"
                @node-collapse="handleNodeCollapse"
              >
                <template #default="{ node, data }">
          <span 
            class="tree-node-wrapper" 
            @mouseenter="handleNodeHover(data, true)"
            @mouseleave="handleNodeHover(data, false)"
          >
                    <span class="tree-node">
                      <el-icon class="tree-icon">
                        <component :is="getNodeIcon(data.type)" />
                      </el-icon>
                      <span class="tree-label" :class="{ 'label-disabled': isNodeDisabled(data) }">{{ node.label }}</span>
                      <el-tag v-if="getNodeCount(data) !== undefined" size="small" type="info">
                        {{ getNodeCount(data) }}
                      </el-tag>
                      <!-- 状态标识 - 业务域 -->
                      <el-tag 
                        v-if="data.type === 'domain' && asBool(data.raw?.is_active, true) === false"
                        size="small" 
                        type="info"
                        style="margin-left: 4px;"
                      >
                        禁用
                      </el-tag>
                      <!-- 状态标识 - 表 -->
                      <el-tag 
                        v-if="data.type === 'table' && isNodeDisabled(data)"
                        size="small" 
                        type="info"
                        style="margin-left: 4px;"
                      >
                        {{ data.parentDomainDisabled && asBool(data.raw?.is_included, true) !== false ? '禁用（父业务域）' : '禁用' }}
                      </el-tag>
                      <!-- 状态标识 - 字段 -->
                      <el-tag 
                        v-if="data.type === 'field' && isNodeDisabled(data)"
                        size="small" 
                        type="info"
                        style="margin-left: 4px;"
                      >
                        {{ getFieldDisabledReason(data) }}
                      </el-tag>
                    </span>
                    <!-- 悬停操作按钮 -->
                    <span v-if="hoveredNodeId === data.id" class="tree-node-actions">
                      <el-tooltip content="添加" placement="top" v-if="data.type !== 'field'">
                        <el-icon class="action-icon" @click.stop="handleAddChild(data)">
                          <Plus />
                        </el-icon>
                      </el-tooltip>
                      <el-tooltip content="编辑" placement="top">
                        <el-icon class="action-icon" @click.stop="handleEdit(data)">
                          <Edit />
                        </el-icon>
                      </el-tooltip>
                      <el-tooltip content="删除" placement="top">
                        <el-icon class="action-icon danger" @click.stop="handleDelete(data)">
                          <Delete />
                        </el-icon>
                      </el-tooltip>
                    </span>
                  </span>
                </template>
              </el-tree>
            </div>
          </el-col>

          <!-- 右侧：配置面板 -->
          <el-col :xs="24" :sm="24" :md="16" :lg="18" :xl="18">
            <div class="config-panel">
              <!-- 未选择节点 -->
              <el-empty 
                v-if="!selectedNode" 
                description="请从左侧树形结构中选择要配置的项"
                :image-size="120"
              />

              <!-- 业务域配置 -->
              <div v-else-if="selectedNode.type === 'domain'" class="config-content">
                <div class="config-header">
                  <h3>📁 {{ selectedNode.label }}</h3>
                  <el-space>
                    <el-button size="small" @click="handleEdit(selectedNode)">
                      <el-icon><Edit /></el-icon>
                      编辑
                    </el-button>
                    <el-button type="primary" size="small" @click="handleAddChild(selectedNode)">
                      <el-icon><Plus /></el-icon>
                      添加表
                    </el-button>
                  </el-space>
                </div>
                
                <!-- 业务域信息卡片 -->
                <el-collapse v-model="activeCollapse" class="domain-collapse">
                  <el-collapse-item title="业务域信息" name="domainInfo">
                    <el-descriptions :column="2" border size="small">
                      <el-descriptions-item label="业务域代码">
                        {{ selectedNode.raw.domain_code }}
                      </el-descriptions-item>
                      <el-descriptions-item label="业务域名称">
                        {{ selectedNode.raw.domain_name }}
                      </el-descriptions-item>
                      <el-descriptions-item label="描述" :span="2">
                        {{ selectedNode.raw.description || '暂无描述' }}
                      </el-descriptions-item>
                      <el-descriptions-item label="关键词" :span="2">
                        <el-tag 
                          v-for="(kw, idx) in (selectedNode.raw.keywords || [])" 
                          :key="idx"
                          style="margin-right: 4px;"
                          size="small"
                        >
                          {{ kw }}
                        </el-tag>
                        <span v-if="!selectedNode.raw.keywords || selectedNode.raw.keywords.length === 0">
                          暂无关键词
                        </span>
                      </el-descriptions-item>
                      <el-descriptions-item label="状态">
                        <el-tag :type="selectedNode.raw.is_active ? 'success' : 'info'">
                          {{ selectedNode.raw.is_active ? '启用' : '禁用' }}
                        </el-tag>
                      </el-descriptions-item>
                    </el-descriptions>
                  </el-collapse-item>
                </el-collapse>

                <!-- 包含的表列表 -->
                <div class="table-cards-section">
                  <div class="section-header">
                    <h4>包含的数据表 ({{ selectedNode.children?.length || 0 }})</h4>
                  </div>
                  <el-row :gutter="16" v-if="selectedNode.children && selectedNode.children.length > 0">
                    <el-col :xs="24" :sm="12" :md="8" :lg="8" :xl="8" v-for="table in selectedNode.children" :key="table.id">
                      <el-card 
                        class="table-mini-card" 
                        shadow="hover"
                        @click="handleNodeClick(table)"
                      >
                        <div class="table-card-content">
                          <div class="table-card-header">
                            <el-icon size="20"><Grid /></el-icon>
                            <span class="table-card-name">{{ table.label }}</span>
                          </div>
                          <div class="table-card-info">
                            <el-tag size="small">{{ table.count || 0 }} 字段</el-tag>
                          </div>
                          <div class="table-card-actions">
                            <el-button link type="primary" size="small" @click.stop="handleNodeClick(table)">
                              查看 <el-icon><ArrowRight /></el-icon>
                            </el-button>
              </div>
            </div>
          </el-card>
        </el-col>
                  </el-row>
                  <el-empty v-else description="暂无数据表" :image-size="80" />
                </div>
              </div>

              <!-- 数据表配置 -->
              <div v-else-if="selectedNode.type === 'table'" class="config-content">
                <div class="config-header">
                  <h3>
                    📊 {{ selectedNode.label }}
                    <el-tag 
                      v-if="getTableConnectionName(selectedNode)"
                      size="small"
                      type="warning"
                      style="margin-left: 8px;"
                    >
                      {{ getTableConnectionName(selectedNode) }}
                    </el-tag>
                  </h3>
                  <el-space>
                    <el-button size="small" @click="handleEdit(selectedNode)">
                      <el-icon><Edit /></el-icon>
                      编辑
                    </el-button>
                  </el-space>
                </div>
                
                <!-- 表信息卡片 -->
                <el-collapse v-model="activeCollapse" class="domain-collapse">
                  <el-collapse-item title="表基本信息" name="tableInfo">
                    <el-descriptions :column="2" border size="small">
                      <el-descriptions-item label="表名">
                        {{ selectedNode.raw.table_name }}
                      </el-descriptions-item>
                      <el-descriptions-item label="显示名称">
                        {{ selectedNode.raw.display_name || '未设置' }}
                      </el-descriptions-item>
                      <el-descriptions-item label="Schema">
                        {{ selectedNode.raw.schema_name || 'dbo' }}
                      </el-descriptions-item>
                      <el-descriptions-item label="所属业务域">
                        <el-tag v-if="selectedNode.raw.domain_name" type="success" size="small">
                          {{ selectedNode.raw.domain_name }}
                        </el-tag>
                        <span v-else>未分配</span>
                      </el-descriptions-item>
                      <el-descriptions-item label="年份">
                        {{ selectedNode.raw.data_year || '未设置' }}
                      </el-descriptions-item>
                      <el-descriptions-item label="描述" :span="2">
                        {{ selectedNode.raw.description || '暂无描述' }}
                      </el-descriptions-item>
                      <el-descriptions-item label="标签/同义词" :span="2">
                        <el-tag 
                          v-for="(tag, idx) in (selectedNode.raw.tags || [])" 
                          :key="idx"
                          style="margin-right: 4px;"
                          size="small"
                        >
                          {{ tag }}
                        </el-tag>
                        <span v-if="!selectedNode.raw.tags || selectedNode.raw.tags.length === 0">
                          暂无标签
                        </span>
                      </el-descriptions-item>
                      <el-descriptions-item label="行数">
                        {{ selectedNode.raw.row_count || '-' }}
                      </el-descriptions-item>
                      <el-descriptions-item label="列数">
                        {{ selectedNode.raw.column_count || 0 }}
                      </el-descriptions-item>
                      <el-descriptions-item label="状态">
                        <div style="display: flex; align-items: center; gap: 8px;">
                          <el-tag :type="isNodeDisabled(selectedNode) ? 'info' : 'success'" size="small">
                            {{ isNodeDisabled(selectedNode) ? '禁用' : '启用' }}
                          </el-tag>
                          <el-text v-if="selectedNode.parentDomainDisabled && asBool(selectedNode.raw?.is_included, true) !== false" type="warning" size="small">
                            （父业务域已禁用）
                          </el-text>
                          <el-text v-else-if="asBool(selectedNode.raw?.is_included, true) === false" type="info" size="small">
                            （表自身禁用）
                          </el-text>
                        </div>
                      </el-descriptions-item>
                    </el-descriptions>
                  </el-collapse-item>
                </el-collapse>

                <!-- 字段列表 -->
                <div class="field-list-section">
                  <div class="section-header">
                    <h4>字段列表 ({{ selectedNode.children?.length || 0 }})</h4>
                  </div>
                  <el-table 
                    v-if="selectedNode.children && selectedNode.children.length > 0"
                    :data="selectedNode.children" 
                    size="small" 
                    stripe
                    max-height="400"
                    style="width: 100%"
                    table-layout="auto"
                  >
                    <el-table-column label="字段名" prop="label" min-width="180" />
                    <el-table-column label="数据类型" min-width="120">
                      <template #default="{ row }">
                        {{ row.raw.data_type || '-' }}
                      </template>
                    </el-table-column>
                    <el-table-column label="主键" min-width="60" align="center">
                      <template #default="{ row }">
                        <el-icon v-if="row.raw.is_primary_key" color="#67c23a"><CircleCheck /></el-icon>
                        <span v-else>-</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="外键" min-width="60" align="center">
                      <template #default="{ row }">
                        <el-icon v-if="row.raw.is_foreign_key" color="#409eff"><Link /></el-icon>
                        <span v-else>-</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="字段类型" min-width="100">
                      <template #default="{ row }">
                        <el-tag v-if="row.raw.field_type" :type="getFieldTypeColor(row.raw.field_type)" size="small">
                          {{ getFieldTypeLabel(row.raw.field_type) }}
                        </el-tag>
                        <span v-else>-</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="操作" min-width="100" fixed="right">
                      <template #default="{ row }">
                        <el-button link type="primary" size="small" @click="handleNodeClick(row)">
                          查看
                        </el-button>
                      </template>
                    </el-table-column>
                  </el-table>
                  <el-empty v-else description="暂无字段数据" :image-size="80" />
                </div>
              </div>

              <!-- 字段配置 -->
              <div v-else-if="selectedNode.type === 'field'" class="config-content">
                <div class="config-header">
                  <h3>📝 字段配置</h3>
                  <el-button type="primary" size="small" @click="editField(selectedNode)">
                    编辑
                  </el-button>
            </div>
                 <el-descriptions :column="2" border size="small">
                  <el-descriptions-item label="字段名">
                    <el-text type="primary">{{ selectedNode.raw.column_name }}</el-text>
                  </el-descriptions-item>
                  <el-descriptions-item label="显示名称">
                    {{ selectedNode.raw.display_name || '未设置' }}
                  </el-descriptions-item>
                  <el-descriptions-item label="描述" :span="2">
                    {{ selectedNode.raw.description || '暂无描述' }}
                  </el-descriptions-item>
                  <el-descriptions-item label="数据类型">
                    <el-tag size="small">{{ selectedNode.raw.data_type }}</el-tag>
                  </el-descriptions-item>
                  <el-descriptions-item label="字段类型">
                    <el-tag v-if="selectedNode.raw.field_type" :type="getFieldTypeColor(selectedNode.raw.field_type)" size="small">
                      {{ getFieldTypeLabel(selectedNode.raw.field_type) }}
                    </el-tag>
                    <span v-else>未识别</span>
                  </el-descriptions-item>
                  <el-descriptions-item label="状态">
                    <div style="display: flex; align-items: center; gap: 8px;">
                      <el-tag :type="isNodeDisabled(selectedNode) ? 'info' : 'success'" size="small">
                        {{ isNodeDisabled(selectedNode) ? '禁用' : '启用' }}
                      </el-tag>
                      <el-text v-if="selectedNode.parentDomainDisabled && !selectedNode.parentTableDisabled && selectedNode.raw?.is_active !== false" type="warning" size="small">
                        （父业务域已禁用）
                      </el-text>
                      <el-text v-else-if="selectedNode.parentTableDisabled && selectedNode.raw?.is_active !== false" type="warning" size="small">
                        （父表已禁用）
                      </el-text>
                      <el-text v-else-if="selectedNode.raw?.is_active === false" type="info" size="small">
                        （字段自身禁用）
                      </el-text>
                    </div>
                  </el-descriptions-item>
                  <el-descriptions-item label="明细查询显示" v-if="selectedNode.raw?.is_active !== false">
                    <el-tag :type="selectedNode.raw?.show_in_detail !== false ? 'success' : 'info'" size="small">
                      {{ selectedNode.raw?.show_in_detail !== false ? '默认显示' : '默认隐藏' }}
                    </el-tag>
                  </el-descriptions-item>
                  <el-descriptions-item label="显示优先级" v-if="selectedNode.raw?.is_active !== false && selectedNode.raw?.show_in_detail !== false">
                    <el-tag size="small" type="primary">
                      {{ selectedNode.raw?.priority || 50 }}
                    </el-tag>
                  </el-descriptions-item>
                  <el-descriptions-item label="序号">
                    {{ selectedNode.raw.ordinal_position || '-' }}
                  </el-descriptions-item>
                  
                  <!-- 度量字段专属 -->
                  <template v-if="selectedNode.raw.field_type === 'measure'">
                    <el-descriptions-item label="单位">
                      <el-tag v-if="selectedNode.raw.unit" size="small" type="success">
                        {{ selectedNode.raw.unit }}
                      </el-tag>
                      <span v-else>未设置</span>
                    </el-descriptions-item>
                    <el-descriptions-item label="默认聚合">
                      <el-tag v-if="selectedNode.raw.default_aggregation" size="small" type="warning">
                        {{ selectedNode.raw.default_aggregation.toUpperCase() }}
                      </el-tag>
                      <span v-else>未设置</span>
                    </el-descriptions-item>
                    
                    <!-- 🆕 单位转换配置 -->
                    <el-descriptions-item label="单位转换" :span="2" v-if="selectedNode.raw.unit_conversion && selectedNode.raw.unit_conversion.enabled">
                      <el-tag size="small" type="primary">已启用</el-tag>
                      <el-text size="small" style="margin-left: 8px;">
                        {{ selectedNode.raw.unit }} → {{ selectedNode.raw.unit_conversion.display_unit }} | 
                        {{ selectedNode.raw.unit_conversion.conversion?.method === 'divide' ? '除以' : '乘以' }} 
                        {{ selectedNode.raw.unit_conversion.conversion?.factor || 1 }}，
                        保留 {{ selectedNode.raw.unit_conversion.conversion?.precision || 2 }} 位小数
                      </el-text>
                    </el-descriptions-item>
                  </template>
                  
                  <!-- 同义词 -->
                  <el-descriptions-item label="同义词" :span="2" v-if="selectedNode.raw.synonyms && selectedNode.raw.synonyms.length > 0">
                    <el-space wrap>
                      <el-tag
                        v-for="(syn, idx) in selectedNode.raw.synonyms"
                        :key="idx"
                        size="small"
                        type="info"
                      >
                        {{ syn }}
                      </el-tag>
                    </el-space>
                  </el-descriptions-item>
                  
                  <!-- 标签 -->
                  <el-descriptions-item label="标签" :span="2" v-if="selectedNode.raw.tags && selectedNode.raw.tags.length > 0">
                    <el-space wrap>
                      <el-tag
                        v-for="(tag, idx) in selectedNode.raw.tags"
                        :key="idx"
                        size="small"
                        effect="plain"
                      >
                        {{ tag }}
                      </el-tag>
                    </el-space>
                  </el-descriptions-item>
                  
                  <!-- 数据库属性 -->
                  <el-descriptions-item label="主键">
                    <el-icon :color="selectedNode.raw.is_primary_key ? '#67c23a' : '#909399'">
                      <component :is="selectedNode.raw.is_primary_key ? 'CircleCheck' : 'CircleClose'" />
                    </el-icon>
                    <span style="margin-left: 4px;">{{ selectedNode.raw.is_primary_key ? '是' : '否' }}</span>
                  </el-descriptions-item>
                  <el-descriptions-item label="外键">
                    <el-icon :color="selectedNode.raw.is_foreign_key ? '#67c23a' : '#909399'">
                      <component :is="selectedNode.raw.is_foreign_key ? 'CircleCheck' : 'CircleClose'" />
                    </el-icon>
                    <span style="margin-left: 4px;">{{ selectedNode.raw.is_foreign_key ? '是' : '否' }}</span>
                  </el-descriptions-item>
                  <el-descriptions-item label="可为空">
                    <el-icon :color="selectedNode.raw.is_nullable ? '#67c23a' : '#909399'">
                      <component :is="selectedNode.raw.is_nullable ? 'CircleCheck' : 'CircleClose'" />
                    </el-icon>
                    <span style="margin-left: 4px;">{{ selectedNode.raw.is_nullable ? '是' : '否' }}</span>
                  </el-descriptions-item>
                  <el-descriptions-item label="唯一值数">
                    {{ selectedNode.raw.distinct_count || '-' }}
                  </el-descriptions-item>
                  
                  <!-- 维度字段-枚举值 -->
                  <el-descriptions-item label="枚举值" :span="2" v-if="selectedNode.raw.field_type === 'dimension'">
                    <el-space>
                      <el-text size="small">共 {{ getFieldEnumCount(selectedNode) }} 个</el-text>
                      <el-button
                        size="small"
                        type="primary"
                        link
                        @click="viewFieldEnumValues(selectedNode)"
                      >
                        查看详情
                      </el-button>
                    </el-space>
                  </el-descriptions-item>
                </el-descriptions>
              </div>
            </div>
        </el-col>
      </el-row>
    </el-card>

    <!-- 表关系管理对话框 -->
    <el-dialog
      v-model="relationshipsDialogVisible"
      title="表关系管理"
      width="85%"
      :close-on-click-modal="false"
      class="relationships-dialog"
    >
      <div class="relationships-content">
        <div class="relationships-header" style="margin-bottom: 16px;">
          <el-space wrap>
            <!-- 数据源选择器 -->
            <el-select
              v-model="relationshipsConnectionId"
              placeholder="选择数据源"
              filterable
              clearable
              style="width: 200px"
              @change="loadRelationships"
            >
              <el-option
                v-for="conn in connections"
                :key="conn.connection_id"
                :label="conn.connection_name"
                :value="conn.connection_id"
              >
                <span>{{ getDbIcon(conn.db_type) }} {{ conn.connection_name }}</span>
              </el-option>
            </el-select>
            <el-button
              type="primary"
              @click="autoDetectRelationships"
              :loading="detectingRelationships"
              :disabled="!relationshipsConnectionId"
            >
              自动识别表关系
            </el-button>
            <el-button @click="loadRelationships" :loading="loadingRelationships">
              刷新
            </el-button>
          </el-space>
        </div>
        
        <div class="relationships-table-wrapper" v-if="!loadingRelationships && relationships.length > 0">
          <el-table
            :data="relationships"
            style="width: 100%"
            :height="relationshipsTableHeight"
            table-layout="auto"
          >
        <el-table-column prop="relationship_name" label="关系名称" min-width="200" />
        <el-table-column label="数据源" min-width="120">
          <template #default="{ row }">
            <el-tag v-if="row.connection_name" type="info" size="small">
              {{ row.connection_name }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="关系类型" min-width="120" align="center">
          <template #default="{ row }">
            <el-tag :type="getRelationshipTypeColor(row.relationship_type)" size="small">
              {{ getRelationshipTypeLabel(row.relationship_type) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="JOIN类型" min-width="100" align="center">
          <template #default="{ row }">
            <el-tag size="small">{{ row.join_type }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="识别方法" min-width="120">
          <template #default="{ row }">
            <el-tag
              :type="row.detection_method === 'foreign_key' ? 'success' : 'info'"
              size="small"
            >
              {{ getDetectionMethodLabel(row.detection_method) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="置信度" min-width="100" align="center">
          <template #default="{ row }">
            <el-progress
              v-if="row.confidence_score"
              :percentage="Math.round(row.confidence_score * 100)"
              :color="getConfidenceColor(row.confidence_score)"
              :stroke-width="6"
              :show-text="false"
            />
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="状态" min-width="80" align="center">
          <template #default="{ row }">
            <el-tag :type="row.is_confirmed ? 'success' : 'warning'" size="small">
              {{ row.is_confirmed ? '已确认' : '待确认' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" min-width="150" fixed="right">
          <template #default="{ row }">
            <el-button
              v-if="!row.is_confirmed"
              link
              type="success"
              size="small"
              @click="confirmRelationship(row.relationship_id)"
            >
              确认
            </el-button>
            <el-button
              link
              type="primary"
              size="small"
              @click="previewRelationshipSQL(row.relationship_id)"
            >
              预览SQL
            </el-button>
            <el-button
              link
              type="danger"
              size="small"
              @click="deleteRelationship(row.relationship_id)"
            >
              删除
            </el-button>
          </template>
        </el-table-column>
          </el-table>
        </div>
        
        <div class="relationships-empty-wrapper" v-if="!loadingRelationships && relationships.length === 0">
          <el-empty
            description="暂无表关系，请先同步Schema或自动识别"
            :image-size="100"
          />
        </div>
        
        <div v-if="loadingRelationships" style="flex: 1; display: flex; align-items: center; justify-content: center;">
          <el-icon class="is-loading" style="font-size: 24px;"><Loading /></el-icon>
        </div>
      </div>
    </el-dialog>

    <!-- SQL预览对话框 -->
    <el-dialog
      v-model="sqlPreviewDialogVisible"
      title="SQL JOIN预览"
      width="90%"
      :style="{ maxWidth: '600px' }"
    >
      <el-input
        v-model="previewSQL"
        type="textarea"
        :rows="10"
        readonly
      />
    </el-dialog>

    <!-- 全局规则管理对话框 -->
    <el-dialog
      v-model="rulesDialogVisible"
      title="全局规则管理"
      width="85%"
      :close-on-click-modal="false"
      destroy-on-close
      class="rules-dialog"
    >
      <!-- 规则通过 scope + domain_ids 控制作用范围，不再需要数据源筛选 -->
      <RuleList v-if="rulesDialogVisible" />
    </el-dialog>

    <!-- 创建/编辑规则对话框 -->
    <el-dialog
      v-model="ruleEditDialogVisible"
      :title="editingRule ? '编辑规则' : '新建规则'"
      width="90%"
      :style="{ maxWidth: '700px' }"
      :close-on-click-modal="false"
    >
      <el-form :model="ruleForm" label-width="100px">
        <el-form-item label="规则名称" required>
          <el-input v-model="ruleForm.rule_name" placeholder="请输入规则名称" />
        </el-form-item>
        
        <el-form-item label="规则类型" required>
          <el-select
            v-model="ruleForm.rule_type"
            placeholder="请选择规则类型"
            style="width: 100%"
            :disabled="!!editingRule"
            @change="handleRuleTypeChange"
          >
            <el-option label="派生指标" value="derived_metric" />
            <el-option label="单位转换" value="unit_conversion" />
            <el-option label="校验规则" value="validation" />
            <el-option label="同义词映射" value="synonym_mapping" />
          </el-select>
        </el-form-item>
        
        <el-form-item label="描述">
          <el-input
            v-model="ruleForm.description"
            type="textarea"
            :rows="2"
            placeholder="请输入规则描述"
          />
        </el-form-item>
        
        <el-form-item label="优先级">
          <el-input-number v-model="ruleForm.priority" :min="0" :max="100" />
        </el-form-item>
        
        <el-divider content-position="left">规则定义</el-divider>
        
        <!-- 派生指标 -->
        <template v-if="ruleForm.rule_type === 'derived_metric'">
          <el-form-item label="公式" required>
            <el-input
              v-model="ruleForm.rule_definition.formula"
              placeholder="例如: SUM(收入) - SUM(成本)"
            />
          </el-form-item>
          <el-form-item label="显示名称" required>
            <el-input v-model="ruleForm.rule_definition.display_name" placeholder="例如: 利润" />
          </el-form-item>
          <el-form-item label="单位">
            <el-input v-model="ruleForm.rule_definition.unit" placeholder="例如: 元" />
          </el-form-item>
        </template>
        
        <!-- 单位转换 -->
        <template v-if="ruleForm.rule_type === 'unit_conversion'">
          <el-form-item label="源单位" required>
            <el-input v-model="ruleForm.rule_definition.from_unit" placeholder="例如: 元" />
          </el-form-item>
          <el-form-item label="目标单位" required>
            <el-input v-model="ruleForm.rule_definition.to_unit" placeholder="例如: 万元" />
          </el-form-item>
          <el-form-item label="转换系数" required>
            <el-input-number
              v-model="ruleForm.rule_definition.conversion_factor"
              :precision="6"
              :step="0.0001"
              placeholder="例如: 0.0001"
            />
          </el-form-item>
        </template>
        
        <!-- 校验规则 -->
        <template v-if="ruleForm.rule_type === 'validation'">
          <el-form-item label="字段ID">
            <el-input v-model="ruleForm.rule_definition.field_id" placeholder="字段ID" />
          </el-form-item>
          <el-form-item label="校验表达式" required>
            <el-input
              v-model="ruleForm.rule_definition.rule_expression"
              placeholder="例如: value >= 0"
            />
          </el-form-item>
          <el-form-item label="错误提示" required>
            <el-input
              v-model="ruleForm.rule_definition.error_message"
              placeholder="例如: 金额不能为负"
            />
          </el-form-item>
          <el-form-item label="严重级别">
            <el-select v-model="ruleForm.rule_definition.severity" style="width: 100%">
              <el-option label="错误" value="error" />
              <el-option label="警告" value="warning" />
              <el-option label="信息" value="info" />
            </el-select>
          </el-form-item>
        </template>
        
        <!-- 同义词映射 -->
        <template v-if="ruleForm.rule_type === 'synonym_mapping'">
          <el-form-item label="字段ID">
            <el-input v-model="ruleForm.rule_definition.field_id" placeholder="字段ID（可选）" />
          </el-form-item>
          <el-form-item label="同义词映射">
            <el-input
              v-model="synonymMappingText"
              type="textarea"
              :rows="6"
              placeholder="格式：每行一组，用逗号分隔，第一个为主词，后续为同义词&#10;例如：&#10;收入,营收,销售额,revenue&#10;成本,花费,开支,cost"
              @blur="parseSynonymMapping"
            />
            <el-text type="info" size="small" style="margin-top: 4px;">
              每行一组同义词，用逗号分隔，第一个为主词
            </el-text>
          </el-form-item>
        </template>
        
        <el-form-item label="状态">
          <el-switch v-model="ruleForm.is_active" active-text="启用" inactive-text="禁用" />
        </el-form-item>
      </el-form>
      
      <template #footer>
        <el-button @click="ruleEditDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveRule" :loading="savingRule">保存</el-button>
      </template>
    </el-dialog>

    <!-- 自动识别业务域功能已移除 -->

    <!-- 编辑对话框 -->
    <el-dialog
      v-model="editDialogVisible"
      :title="getDialogTitle()"
      width="90%"
      :style="{ maxWidth: '600px' }"
      :close-on-click-modal="false"
    >
      <el-form :model="editForm" label-width="100px">
        <!-- 业务域编辑 -->
        <template v-if="editDialogType === 'domain'">
          <el-form-item label="业务域名称" required>
            <el-input v-model="editForm.domain_name" placeholder="请输入业务域名称" />
          </el-form-item>
          <el-form-item label="业务域代码">
            <el-input v-model="editForm.domain_code" placeholder="请输入业务域代码" />
          </el-form-item>
          <el-form-item label="描述">
            <el-input 
              v-model="editForm.description" 
              type="textarea" 
              :rows="3"
              placeholder="请输入描述"
            />
          </el-form-item>
          <el-form-item label="关键词">
            <el-select
              v-model="editForm.keywords"
              multiple
              filterable
              allow-create
              default-first-option
              :reserve-keyword="false"
              placeholder="输入关键词后按回车添加"
              style="width: 100%"
            >
            </el-select>
          </el-form-item>
          <el-form-item label="状态">
            <el-switch v-model="editForm.is_active" active-text="启用" inactive-text="禁用" />
          </el-form-item>
        </template>

        <!-- 表编辑 -->
        <template v-else-if="editDialogType === 'table'">
          <el-form-item label="表名" required>
            <!-- 编辑时显示禁用的输入框 -->
            <el-input 
              v-if="editingNode" 
              v-model="editForm.table_name" 
              disabled 
            />
            <!-- 新增时显示下拉选择 -->
            <el-select
              v-else
              v-model="editForm.table_id"
              placeholder="选择要添加的表"
              filterable
              style="width: 100%"
              :loading="loadingAvailableTables"
              @change="handleSelectTableChange"
            >
              <el-option
                v-for="table in availableTables"
                :key="table.table_id"
                :label="table.connection_name ? `[${table.connection_name}] ${table.table_name}` : table.table_name"
                :value="table.table_id"
              >
                <div style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                  <span>
                    <span v-if="table.connection_name" style="color: #409eff; font-size: 12px; margin-right: 6px;">[{{ table.connection_name }}]</span>
                    <span>{{ table.table_name }}</span>
                  </span>
                  <span v-if="table.display_name" style="color: #999; font-size: 12px;">{{ table.display_name }}</span>
                </div>
              </el-option>
              <template #empty>
                <div style="padding: 16px; text-align: center; color: #999;">
                  {{ connections.length === 0 ? '请先添加数据库连接' : '暂无可添加的表，请先同步数据库Schema' }}
                </div>
              </template>
            </el-select>
          </el-form-item>
          <el-form-item label="显示名称">
            <el-input v-model="editForm.display_name" placeholder="请输入显示名称" />
          </el-form-item>
          <el-form-item label="所属业务域">
            <el-select
              v-model="editForm.domain_id"
              placeholder="选择业务域"
              clearable
              style="width: 100%"
            >
              <el-option
                v-for="d in domainOptions"
                :key="d.domain_id"
                :label="d.domain_name"
                :value="d.domain_id"
              />
              <template #empty>
                <div style="padding: 16px; text-align: center; color: #999;">
                  暂无业务域，请先创建业务域
                </div>
              </template>
            </el-select>
            <el-text type="info" size="small" style="display: block; margin-top: 4px;">
              用于两步意图识别，提高检索准确性
            </el-text>
          </el-form-item>
          <el-form-item label="年份">
            <el-input v-model="editForm.data_year" placeholder="例如: 2023或2023年" />
          </el-form-item>
          <el-form-item label="描述">
            <el-input 
              v-model="editForm.description" 
              type="textarea" 
              :rows="3"
              placeholder="请输入描述（表的作用以及包含的关键信息）"
            />
          </el-form-item>
          <el-form-item label="标签/同义词">
            <el-select
              v-model="editForm.tags"
              multiple
              filterable
              allow-create
              default-first-option
              :reserve-keyword="false"
              placeholder="输入标签后按回车添加"
              style="width: 100%"
            >
            </el-select>
          </el-form-item>
          <el-form-item label="状态">
            <el-switch v-model="editForm.is_included" active-text="启用" inactive-text="禁用" />
            <el-text type="info" size="small" style="margin-left: 8px;">
              禁用后该表及其所有字段不会出现在NL2SQL查询中
            </el-text>
          </el-form-item>
        </template>

        <!-- 字段编辑 -->
        <template v-else-if="editDialogType === 'field'">
          <el-form-item label="字段名">
            <el-input v-model="editForm.column_name" disabled />
          </el-form-item>
          <el-form-item label="显示名称" required>
            <el-input 
              v-model="editForm.display_name" 
              :placeholder="`请输入显示名称（默认: ${editForm.column_name}）`" 
            />
            <el-text type="info" size="small" style="margin-top: 4px;">
              为空时将使用字段名作为显示名称
            </el-text>
          </el-form-item>
          <el-form-item label="字段类型">
            <div style="display: flex; gap: 8px; align-items: center; width: 100%; flex-wrap: wrap;">
              <el-select
                v-model="editForm.field_type"
                placeholder="请选择字段类型"
                style="flex: 1; min-width: 150px; max-width: 200px;"
                @change="handleFieldTypeChange"
              >
                <el-option label="度量" value="measure" />
                <el-option label="维度" value="dimension" />
                <el-option label="时间戳" value="timestamp" />
                <el-option label="标识符" value="identifier" />
                <el-option label="空间" value="spatial" />
              </el-select>
              <el-button
                v-if="getSuggestedFieldType()"
                size="small"
                @click="applySuggestedFieldType"
              >
                <el-icon><MagicStick /></el-icon>
                智能推荐
              </el-button>
              <el-text v-if="editForm.data_type" type="info" size="small">
                数据类型：{{ editForm.data_type }}
              </el-text>
            </div>
          </el-form-item>
          
          <!-- 度量字段专属配置 -->
          <template v-if="editForm.field_type === 'measure'">
            <el-form-item label="单位">
              <el-input v-model="editForm.unit" placeholder="例如: 元、个、次、m²" />
            </el-form-item>
            
            <!-- 🆕 单位转换配置 -->
            <el-divider content-position="left">
              <el-text size="small">单位转换配置（可选）</el-text>
            </el-divider>
            
            <el-form-item>
              <template #label>
                <el-space :size="4">
                  <span>启用转换</span>
                  <el-tooltip content="启用后，查询结果将自动转换为目标单位显示" placement="top">
                    <el-icon><QuestionFilled /></el-icon>
                  </el-tooltip>
                </el-space>
              </template>
              <el-switch 
                v-model="editForm.unit_conversion_enabled"
                @change="handleUnitConversionToggle"
              />
            </el-form-item>
            
            <template v-if="editForm.unit_conversion_enabled">
              <el-form-item label="显示单位" required>
                <el-input 
                  v-model="editForm.unit_conversion_display_unit" 
                  placeholder="例如: 公顷、亿元"
                />
                <el-text type="info" size="small" style="margin-top: 4px;">
                  转换后在查询结果中显示的单位
                </el-text>
              </el-form-item>
              
              <el-form-item label="转换方法" required>
                <el-select v-model="editForm.unit_conversion_method" style="width: 100%">
                  <el-option label="除以（÷）" value="divide" />
                  <el-option label="乘以（×）" value="multiply" />
                </el-select>
              </el-form-item>
              
              <el-form-item label="转换因子" required>
                <el-input-number
                  v-model="editForm.unit_conversion_factor"
                  :min="0.0000000001"
                  :step="1"
                  style="width: 100%"
                  placeholder="例如: 10000"
                />
                <el-text type="info" size="small" style="margin-top: 4px;">
                  示例：m² → 公顷，选择"除以"，因子填 10000
                </el-text>
              </el-form-item>
              
              <el-form-item label="小数位数">
                <el-input-number
                  v-model="editForm.unit_conversion_precision"
                  :min="0"
                  :max="6"
                  style="width: 100%"
                  placeholder="默认: 2"
                />
              </el-form-item>
              
              
            </template>
            
            <el-form-item label="默认聚合">
              <el-select v-model="editForm.default_aggregation" placeholder="选择默认聚合方式" style="width: 100%">
                <el-option label="求和(SUM)" value="sum" />
                <el-option label="平均(AVG)" value="avg" />
                <el-option label="计数(COUNT)" value="count" />
                <el-option label="最大值(MAX)" value="max" />
                <el-option label="最小值(MIN)" value="min" />
              </el-select>
            </el-form-item>
          </template>
          
          <el-form-item label="同义词/标签">
            <el-select
              v-model="editForm.synonyms"
              multiple
              filterable
              allow-create
              default-first-option
              :reserve-keyword="false"
              placeholder="输入同义词后按回车添加"
              style="width: 100%"
            >
            </el-select>
          </el-form-item>
          
          <el-form-item label="描述">
            <el-input 
              v-model="editForm.description" 
              type="textarea" 
              :rows="3"
              placeholder="请输入描述"
            />
          </el-form-item>
          
          <el-form-item label="字段可用性">
            <el-switch v-model="editForm.is_active" active-text="启用" inactive-text="禁用" />
            <el-text type="info" size="small" style="margin-left: 8px;">
              控制字段是否可被NL2IR识别（禁用后完全不参与查询）
            </el-text>
          </el-form-item>
          
          <el-form-item v-if="editForm.is_active">
            <template #label>
              <el-space :size="4">
                <span>明细查询显示</span>
                <el-tooltip placement="top">
                  <template #content>
                    <div style="max-width: 350px;">
                      <p><strong>层级关系：</strong></p>
                      <p style="margin-top: 4px;">1️⃣ is_active (字段可用性) - 能否被问到</p>
                      <p style="margin-top: 4px;">2️⃣ show_in_detail (明细显示) - 默认是否显示</p>
                      <p style="margin-top: 4px;">3️⃣ priority (显示顺序) - 显示的先后顺序</p>
                      <p style="margin-top: 8px;"><strong>示例：</strong></p>
                      <p style="margin-top: 4px;">• 竞得人: is_active=✓, show_in_detail=✓ → 可被问到且默认显示</p>
                      <p style="margin-top: 4px;">• 备注: is_active=✓, show_in_detail=✗ → 可被问到（"显示备注"）但默认不显示</p>
                      <p style="margin-top: 4px;">• Shape: is_active=✗ → 完全禁用</p>
                    </div>
                  </template>
                  <el-icon><QuestionFilled /></el-icon>
                </el-tooltip>
              </el-space>
            </template>
            <el-switch v-model="editForm.show_in_detail" active-text="默认显示" inactive-text="默认隐藏" />
            <el-text type="info" size="small" style="margin-left: 8px;">
              当用户未明确指定字段时，是否在明细查询结果中显示
            </el-text>
          </el-form-item>
          
          <el-form-item>
            <template #label>
              <el-space :size="4">
                <span>显示优先级</span>
                <el-tooltip placement="top">
                  <template #content>
                    <div style="max-width: 300px;">
                      <p>控制明细查询时字段的显示顺序</p>
                      <p style="margin-top: 4px;">• 范围：1-100（数值越大越靠前）</p>
                      <p style="margin-top: 4px;">• 默认：50</p>
                      <p style="margin-top: 4px;">• 仅当用户未明确指定字段时生效</p>
                    </div>
                  </template>
                  <el-icon><QuestionFilled /></el-icon>
                </el-tooltip>
              </el-space>
            </template>
            <el-input-number
              v-model="editForm.priority"
              :min="1"
              :max="100"
              :step="5"
              style="width: 100%; max-width: 150px;"
              placeholder="默认: 50"
            />
            <el-text type="info" size="small" style="margin-left: 8px;">
              数值越大，在明细查询结果中越靠前
            </el-text>
          </el-form-item>
          
          <!-- 枚举值管理（仅维度字段） -->
          <el-divider v-if="editForm.field_type === 'dimension'" content-position="left">
            枚举值管理
          </el-divider>
          
          <div v-if="editForm.field_type === 'dimension'" style="margin-top: 16px;">
            <!-- 枚举值状态和操作 -->
            <div style="margin-bottom: 12px; display: flex; align-items: center; justify-content: space-between;">
              <div>
                <el-text type="info" size="small">
                  <template v-if="loadingEnumValues">正在加载...</template>
                  <template v-else-if="fieldEnumValues.length > 0">
                    共 {{ fieldEnumValues.length }} 个枚举值
                    <template v-if="editForm.synced_enum_count !== undefined">
                      <el-divider direction="vertical" />
                      已同步: {{ editForm.synced_enum_count }}
                      <template v-if="editForm.last_synced_at">
                        ({{ new Date(editForm.last_synced_at).toLocaleString('zh-CN') }})
                      </template>
                    </template>
                  </template>
                  <template v-else>暂无枚举值，点击右侧按钮从数据库采样</template>
                </el-text>
              </div>
              <el-space>
                <el-button
                  type="primary"
                  size="small"
                  @click="sampleFieldEnumValues"
                  :loading="samplingEnumValues"
                >
                  <el-icon><Refresh /></el-icon>
                  从数据库采样
                </el-button>
                <el-button
                  type="danger"
                  size="small"
                  :disabled="selectedEnumRows.length === 0"
                  @click="batchDeleteEnumValues"
                >
                  <el-icon><Delete /></el-icon>
                  批量删除
                </el-button>
              </el-space>
            </div>
            
            <el-table
              v-if="fieldEnumValues.length > 0"
              :data="fieldEnumValues"
              size="small"
              max-height="300"
              style="width: 100%"
              table-layout="auto"
              ref="enumTableRef"
              @selection-change="handleEnumSelectionChange"
            >
              <el-table-column type="selection" min-width="48" fixed="left" />
              <el-table-column prop="original_value" label="原始值" min-width="120" />
              <el-table-column prop="display_value" label="显示值" min-width="120" />
              <el-table-column label="同义词" min-width="150">
                <template #default="{ row }">
                  <el-tag
                    v-for="(syn, idx) in (row.synonyms || []).slice(0, 2)"
                    :key="idx"
                    size="small"
                    style="margin-right: 4px;"
                  >
                    {{ syn }}
                  </el-tag>
                  <span v-if="(row.synonyms || []).length > 2">...</span>
                </template>
              </el-table-column>
              <el-table-column prop="frequency" label="频次" min-width="80" align="center" />
              <el-table-column label="操作" min-width="140" fixed="right">
                <template #default="{ row }">
                  <el-button link type="primary" size="small" @click="editEnumValue(row)">
                    编辑
                  </el-button>
                  <el-button link type="danger" size="small" @click="deleteEnumValue(row)">
                    删除
                  </el-button>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </template>
      </el-form>

      <template #footer>
        <el-button @click="editDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveEdit">保存</el-button>
      </template>
    </el-dialog>

    <!-- 枚举值编辑对话框 -->
    <el-dialog
      v-model="enumEditDialogVisible"
      title="编辑枚举值"
      width="90%"
      :style="{ maxWidth: '500px' }"
    >
      <el-form label-width="100px">
        <el-form-item label="原始值">
          <el-input v-model="enumEditForm.original_value" disabled />
        </el-form-item>
        
        <el-form-item label="显示值">
          <el-input
            v-model="enumEditForm.display_value"
            placeholder="用户友好的显示名称"
          />
          <el-text type="info" size="small" style="margin-top: 4px;">
            用于前端显示的友好名称
          </el-text>
        </el-form-item>
        
        <el-form-item label="同义词">
          <el-select
            v-model="enumEditForm.synonyms"
            multiple
            filterable
            allow-create
            default-first-option
            :reserve-keyword="false"
            placeholder="输入同义词后按回车添加"
            style="width: 100%"
          >
          </el-select>
          <el-text type="info" size="small" style="margin-top: 4px;">
            用于NL2SQL时的多种表达识别
          </el-text>
        </el-form-item>
        
        <el-form-item label="包含值">
          <el-select
            v-model="enumEditForm.includes_values"
            multiple
            filterable
            allow-create
            default-first-option
            :reserve-keyword="false"
            placeholder="输入包含的其他标准值后按回车添加"
            style="width: 100%"
          >
            <el-option
              v-for="val in fieldEnumValues.filter(v => v.original_value !== enumEditForm.original_value)"
              :key="val.original_value"
              :label="val.original_value"
              :value="val.original_value"
            />
          </el-select>
          <el-text type="info" size="small" style="margin-top: 4px;">
            该枚举值包含哪些其他标准值（用于查询展开，如"住宅、商服用地"包含"住宅用地"和"商服用地"）
          </el-text>
        </el-form-item>
        
        <el-form-item label="频次">
          <el-input-number
            v-model="enumEditForm.frequency"
            :min="0"
            disabled
          />
          <el-text type="info" size="small" style="margin-left: 8px;">
            数据库中的出现次数（只读）
          </el-text>
        </el-form-item>
        
        <el-form-item label="状态">
          <el-switch
            v-model="enumEditForm.is_active"
            active-text="启用"
            inactive-text="禁用"
          />
          <el-text type="info" size="small" style="margin-left: 8px;">
            禁用后该枚举值不会在NL2SQL中使用
          </el-text>
        </el-form-item>
      </el-form>
      
      <template #footer>
        <el-button @click="enumEditDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveEnumValue" :loading="savingEnumValue">
          保存
        </el-button>
      </template>
    </el-dialog>

    <!-- 元数据导出对话框 -->
    <el-dialog
      v-model="exportDialogVisible"
      title="导出元数据配置"
      width="90%"
      :style="{ maxWidth: '700px' }"
      :close-on-click-modal="false"
    >
      <el-alert 
        title="导出说明" 
        type="info" 
        :closable="false"
        style="margin-bottom: 16px;"
      >
        <template #default>
          <div style="line-height: 1.8;">
            • <b>全局配置</b>（业务域、全局规则）：始终导出全部，不受表选择影响<br/>
            • <b>表级配置</b>（表、字段、枚举值、表关系）：可指定导出特定表的配置
          </div>
        </template>
      </el-alert>
      
      <el-form label-width="100px">
        <!-- 选择数据源（支持多选） -->
        <el-form-item label="数据源">
          <el-select
            v-model="exportConnectionIds"
            placeholder="全部数据源"
            filterable
            clearable
            multiple
            collapse-tags
            collapse-tags-tooltip
            :max-collapse-tags="2"
            style="width: 100%"
          >
            <el-option
              v-for="conn in connections"
              :key="conn.connection_id"
              :label="conn.connection_name"
              :value="conn.connection_id"
            >
              <span>{{ getDbIcon(conn.db_type) }} {{ conn.connection_name }}</span>
            </el-option>
          </el-select>
          <el-text type="info" size="small" style="display: block; margin-top: 5px;">
            留空导出所有数据源，可多选指定数据源
          </el-text>
        </el-form-item>
        <!-- 全局配置 -->
        <el-divider content-position="left">
          <el-text type="info" size="small">全局配置（导出全部）</el-text>
        </el-divider>
        <el-form-item label="配置项">
          <el-checkbox v-model="exportOptions.include_domains">业务域配置</el-checkbox>
          <el-checkbox v-model="exportOptions.include_rules" style="margin-left: 20px;">全局规则配置</el-checkbox>
        </el-form-item>
        
        <!-- 表级配置 -->
        <el-divider content-position="left">
          <el-text type="info" size="small">表级配置（可指定表）</el-text>
        </el-divider>
        <el-form-item label="配置项">
          <el-checkbox v-model="exportOptions.include_tables">表配置</el-checkbox>
          <el-checkbox v-model="exportOptions.include_fields" style="margin-left: 20px;">字段配置</el-checkbox>
          <el-checkbox v-model="exportOptions.include_enums" style="margin-left: 20px;">枚举值配置</el-checkbox>
          <el-checkbox v-model="exportOptions.include_relationships" style="margin-left: 20px;">表关系配置</el-checkbox>
        </el-form-item>
        
        <el-form-item label="指定表">
          <el-select
            v-model="selectedExportTables"
            multiple
            filterable
            clearable
            collapse-tags
            collapse-tags-tooltip
            :max-collapse-tags="3"
            placeholder="留空 = 导出全部表；可多选指定表"
            style="width: 100%"
          >
            <el-option-group
              v-for="group in groupedExportTables"
              :key="group.connectionName"
              :label="group.connectionName"
            >
              <el-option
                v-for="table in group.tables"
                :key="table.table_id"
                :label="table.display_name ? `${table.table_name} (${table.display_name})` : table.table_name"
                :value="table.table_name"
              >
                <span>{{ table.table_name }}</span>
                <span v-if="table.display_name" style="color: #909399; margin-left: 8px; font-size: 12px;">
                  {{ table.display_name }}
                </span>
              </el-option>
            </el-option-group>
          </el-select>
          <el-text type="info" size="small" style="display: block; margin-top: 5px;">
            支持多选，按数据源分组显示
          </el-text>
        </el-form-item>
        
        <!-- 导出预览 -->
        <el-form-item label="导出内容">
          <el-tag v-if="exportOptions.include_domains" type="primary" size="small" style="margin: 2px;">业务域</el-tag>
          <el-tag v-if="exportOptions.include_rules" type="primary" size="small" style="margin: 2px;">全局规则</el-tag>
          <el-tag v-if="exportOptions.include_tables" :type="selectedExportTables.length ? 'warning' : 'success'" size="small" style="margin: 2px;">
            表配置{{ selectedExportTables.length ? `(${selectedExportTables.length}个表)` : '(全部)' }}
          </el-tag>
          <el-tag v-if="exportOptions.include_fields" :type="selectedExportTables.length ? 'warning' : 'success'" size="small" style="margin: 2px;">
            字段配置{{ selectedExportTables.length ? `(${selectedExportTables.length}个表)` : '(全部)' }}
          </el-tag>
          <el-tag v-if="exportOptions.include_enums" :type="selectedExportTables.length ? 'warning' : 'success'" size="small" style="margin: 2px;">
            枚举值{{ selectedExportTables.length ? `(${selectedExportTables.length}个表)` : '(全部)' }}
          </el-tag>
          <el-tag v-if="exportOptions.include_relationships" :type="selectedExportTables.length ? 'warning' : 'success'" size="small" style="margin: 2px;">
            表关系{{ selectedExportTables.length ? '(相关)' : '(全部)' }}
          </el-tag>
        </el-form-item>
      </el-form>
      
      <template #footer>
        <el-button @click="exportDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="executeExport" :loading="exportLoading">
          <el-icon><Download /></el-icon>
          导出模板
        </el-button>
      </template>
    </el-dialog>

    <!-- 元数据导入对话框 -->
    <el-dialog
      v-model="importDialogVisible"
      title="导入元数据配置"
      width="90%"
      :style="{ maxWidth: '800px' }"
      :close-on-click-modal="false"
    >
      <el-form label-width="100px">
        <!-- 导入模式选择 -->
        <el-form-item label="导入方式">
          <el-radio-group v-model="importTargetMode" @change="handleImportTargetModeChange">
            <el-radio value="auto">
              <span>自动识别</span>
              <el-text type="info" size="small" style="margin-left: 4px;">根据Excel中的数据源列自动分发</el-text>
            </el-radio>
            <el-radio value="single">
              <span>指定数据源</span>
              <el-text type="info" size="small" style="margin-left: 4px;">导入到选定的单个数据源</el-text>
            </el-radio>
          </el-radio-group>
        </el-form-item>
        
        <!-- 选择目标数据源（仅单数据源模式显示） -->
        <el-form-item label="目标数据源" v-if="importTargetMode === 'single'" required>
          <el-select
            v-model="importConnectionId"
            placeholder="请选择导入的目标数据源"
            filterable
            style="width: 100%"
          >
            <el-option
              v-for="conn in connections"
              :key="conn.connection_id"
              :label="conn.connection_name"
              :value="conn.connection_id"
            >
              <span>{{ getDbIcon(conn.db_type) }} {{ conn.connection_name }}</span>
            </el-option>
          </el-select>
          <div class="el-form-item__tip" style="color: #909399; font-size: 12px; margin-top: 4px;">
            元数据将导入到选择的数据源中
          </div>
        </el-form-item>
        
        <!-- 自动识别模式提示 -->
        <el-form-item v-if="importTargetMode === 'auto'">
          <el-alert
            title="自动识别模式"
            type="info"
            :closable="false"
            show-icon
          >
            <template #default>
              <div style="font-size: 12px; line-height: 1.6;">
                适用于从"导出全部/多数据源"下载的Excel文件。<br/>
                系统会根据Excel中的"数据源(只读)"列，自动将数据分发到对应的数据源。
              </div>
            </template>
          </el-alert>
        </el-form-item>
        
        <!-- 步骤1：选择文件 -->
        <el-form-item label="选择文件">
          <el-upload
            class="upload-demo"
            drag
            :auto-upload="false"
            :limit="1"
            accept=".xlsx,.xls"
            :on-change="handleFileChange"
            :on-remove="() => { importFile = null; importPreviewResult = null }"
            :file-list="importFile ? [{ name: importFile.name }] : []"
          >
            <el-icon class="el-icon--upload"><Upload /></el-icon>
            <div class="el-upload__text">
              将文件拖到此处，或<em>点击上传</em>
            </div>
            <template #tip>
              <div class="el-upload__tip">
                支持从"导出配置"下载的 Excel 文件
              </div>
            </template>
          </el-upload>
        </el-form-item>
        
        <!-- 步骤2：选择导入模式（卡片式） -->
        <el-form-item label="导入模式">
          <div class="import-mode-cards">
            <div 
              :class="['mode-card', { active: importMode === 'update' }]"
              @click="importMode = 'update'"
            >
              <div class="mode-icon">📝</div>
              <div class="mode-title">更新模式</div>
              <div class="mode-desc">
                <div>• 已存在 → <el-text type="warning">覆盖更新</el-text></div>
                <div>• 不存在 → <el-text type="success">新增</el-text></div>
              </div>
              <el-tag v-if="importMode === 'update'" type="primary" size="small" class="mode-tag">推荐</el-tag>
            </div>
            <div 
              :class="['mode-card', { active: importMode === 'merge' }]"
              @click="importMode = 'merge'"
            >
              <div class="mode-icon">🔀</div>
              <div class="mode-title">合并模式</div>
              <div class="mode-desc">
                <div>• 已存在 → <el-text type="info">保持不变</el-text></div>
                <div>• 不存在 → <el-text type="success">新增</el-text></div>
              </div>
              <el-tag v-if="importMode === 'merge'" type="success" size="small" class="mode-tag">安全</el-tag>
            </div>
          </div>
        </el-form-item>
        
        <!-- 预览结果 -->
        <el-form-item label="预览结果" v-if="importPreviewResult">
          <div class="import-preview">
            <!-- 多数据源模式：按数据源显示 -->
            <template v-if="importPreviewResult.by_connection">
              <el-collapse v-model="previewExpandedConnections">
                <el-collapse-item 
                  v-for="(summary, connName) in importPreviewResult.by_connection" 
                  :key="connName"
                  :name="connName"
                >
                  <template #title>
                    <span style="font-weight: 500;">📊 {{ connName }}</span>
                    <el-tag size="small" type="info" style="margin-left: 8px;">
                      {{ getConnectionStatsText(summary) }}
                    </el-tag>
                  </template>
                  <div class="preview-summary" style="padding: 8px 0;">
                    <div class="summary-item" v-for="(stats, key) in summary" :key="key">
                      <div class="summary-label">{{ getStatLabel(key) }}</div>
                      <div class="summary-stats">
                        <span v-if="stats.new > 0" class="stat-new">+{{ stats.new }}</span>
                        <span v-if="stats.update > 0" class="stat-update">~{{ stats.update }}</span>
                        <span v-if="stats.skip > 0" class="stat-skip">○{{ stats.skip }}</span>
                        <span v-if="stats.error > 0" class="stat-error">✕{{ stats.error }}</span>
                      </div>
                    </div>
                  </div>
                </el-collapse-item>
              </el-collapse>
            </template>
            
            <!-- 单数据源模式：原有显示 -->
            <template v-else-if="importPreviewResult.summary">
              <div class="preview-summary">
                <div class="summary-item" v-for="(stats, key) in importPreviewResult.summary" :key="key">
                  <div class="summary-label">{{ getStatLabel(key) }}</div>
                  <div class="summary-stats">
                    <span v-if="stats.new > 0" class="stat-new">+{{ stats.new }}</span>
                    <span v-if="stats.update > 0" class="stat-update">~{{ stats.update }}</span>
                    <span v-if="stats.skip > 0" class="stat-skip">○{{ stats.skip }}</span>
                    <span v-if="stats.error > 0" class="stat-error">✕{{ stats.error }}</span>
                  </div>
                </div>
              </div>
            </template>
            
            <!-- 图例说明 -->
            <div class="preview-legend">
              <span><span class="stat-new">+</span> 新增</span>
              <span><span class="stat-update">~</span> 更新</span>
              <span><span class="stat-skip">○</span> 跳过</span>
              <span><span class="stat-error">✕</span> 错误</span>
            </div>
            
            <!-- 错误列表 -->
            <el-collapse v-if="importPreviewResult.errors?.length > 0" style="margin-top: 12px;">
              <el-collapse-item>
                <template #title>
                  <el-text type="danger">
                    <el-icon><CircleClose /></el-icon>
                    {{ importPreviewResult.errors.length }} 个错误（必须修复）
                  </el-text>
                </template>
                <el-table :data="importPreviewResult.errors" size="small" max-height="180">
                  <el-table-column prop="sheet" label="Sheet" min-width="100" />
                  <el-table-column prop="row" label="行" min-width="50" />
                  <el-table-column prop="message" label="错误信息" show-overflow-tooltip />
                </el-table>
              </el-collapse-item>
            </el-collapse>
            
            <!-- 警告列表 -->
            <el-collapse v-if="importPreviewResult.warnings?.length > 0" style="margin-top: 8px;">
              <el-collapse-item>
                <template #title>
                  <el-text type="warning">
                    <el-icon><InfoFilled /></el-icon>
                    {{ importPreviewResult.warnings.length }} 个警告（可忽略）
                  </el-text>
                </template>
                <el-table :data="importPreviewResult.warnings" size="small" max-height="150">
                  <el-table-column prop="sheet" label="Sheet" min-width="100" />
                  <el-table-column prop="row" label="行" min-width="50" />
                  <el-table-column prop="message" label="警告信息" show-overflow-tooltip />
                </el-table>
              </el-collapse-item>
            </el-collapse>
            
            <!-- 无变更提示 -->
            <el-empty 
              v-if="isPreviewEmpty" 
              description="没有检测到需要导入的变更" 
              :image-size="60"
            />
          </div>
        </el-form-item>
      </el-form>
      
      <template #footer>
        <div class="import-footer">
          <el-button @click="importDialogVisible = false">取消</el-button>
          <el-button 
            type="primary" 
            @click="handleImportPreview" 
            :loading="importLoading"
            :disabled="!importFile"
          >
            <el-icon><Refresh /></el-icon>
            预览变更
          </el-button>
          <el-button 
            type="success" 
            @click="handleImportExecute" 
            :loading="importLoading"
            :disabled="!importPreviewResult || importPreviewResult.errors?.length > 0 || isPreviewEmpty"
          >
            <el-icon><Upload /></el-icon>
            执行导入
          </el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch, nextTick } from 'vue'
import { Refresh, Grid, List, FolderOpened, Document, CircleCheck, CircleClose, Plus, Edit, Delete, ArrowRight, Link, Setting, MagicStick, InfoFilled, DocumentCopy, ArrowDown, Download, Upload, Loading } from '@element-plus/icons-vue'
import { useAdminStore } from '@/stores/admin'
import { ElMessage, ElMessageBox } from 'element-plus'
import { connectionAPI, domainAPI, tableAPI, fieldAPI, relationshipAPI, ruleAPI, milvusAPI, metadataIOAPI } from '@/api'
import RuleList from './metadata/RuleList.vue'

const adminStore = useAdminStore()

// 状态
const currentConnectionId = ref(null)
const syncing = ref(false)
const connections = ref([])
const domainOptions = ref([]) // 业务域选项列表（用于下拉选择）

// 树形结构相关状态
const treeData = ref([])
const treeLoading = ref(false)
const treeRef = ref(null) // 树组件引用
const selectedNode = ref(null)
const hoveredNodeId = ref(null)
const expandedNodeIds = ref([]) // 记录手动展开的节点，刷新后保持展开状态
const statusFilter = ref('enabled') // 状态过滤器：all, enabled, disabled（默认启用）
const activeCollapse = ref(['domainInfo', 'tableInfo'])
const treeProps = {
  children: 'children',
  label: 'label'
}

// 统一将后端返回的真假值规范为布尔值
// 支持 true/false、1/0、'1'/'0'，并可指定默认值（用于 null/undefined）
function asBool(value, defaultValue = true) {
  if (value === null || value === undefined) return defaultValue
  if (value === true || value === false) return value
  if (value === 1 || value === '1') return true
  if (value === 0 || value === '0') return false
  return !!value
}

// 提取业务域ID/表的业务域ID（兼容不同字段名）
const getDomainId = (d) => d?.domain_id || d?.id || d?.domainId
const getTableDomainId = (t) => t?.domain_id || t?.domainId

// 过滤后的树形数据
const filteredTreeData = computed(() => {
  if (statusFilter.value === 'all') {
    return treeData.value
  }
  
  // 递归过滤函数，传入父节点状态（业务域禁用状态、表禁用状态）
  const filterNode = (node, parentDomainDisabled = false, parentTableDisabled = false) => {
    // 复制节点
    const newNode = { ...node }
    
    // 判断当前节点的禁用状态（考虑层级关系）
    let currentNodeDisabled = false
    let currentDomainDisabled = parentDomainDisabled
    let currentTableDisabled = parentTableDisabled
    
    if (node.type === 'domain') {
      // 业务域的禁用状态：自身 is_active 为 false
      currentDomainDisabled = asBool(node.raw?.is_active, true) === false
      currentNodeDisabled = currentDomainDisabled
    } else if (node.type === 'table') {
      // 表的禁用状态：父业务域禁用 或 表自身禁用
      const tableSelfDisabled = asBool(node.raw?.is_included, true) === false
      currentTableDisabled = parentDomainDisabled || tableSelfDisabled
      currentNodeDisabled = currentTableDisabled
    } else if (node.type === 'field') {
      // 字段的禁用状态：父表禁用（包含业务域级联）或 字段自身禁用
      currentNodeDisabled = parentTableDisabled || asBool(node.raw?.is_active, true) === false
    }
    
    // 过滤子节点（传递当前层级的禁用状态给子节点）
    if (newNode.children) {
      newNode.children = newNode.children
        .map(child => filterNode(child, currentDomainDisabled, currentTableDisabled))
        .filter(child => child !== null)
    }
    
    // 判断当前节点是否应该显示
    const shouldShow = () => {
      if (node.type === 'domain') {
        if (statusFilter.value === 'enabled') {
          // 启用过滤：
          // 1. 业务域自身启用，且至少有一个启用的子节点 或
          // 2. 业务域自身启用（即使没有子节点也显示，便于添加表）
          if (currentNodeDisabled) {
            return false
          }
          // 业务域启用时，如果有子节点且全部被过滤掉则不显示
          // 但如果是空业务域（没有表）则显示，便于后续添加表
          return !newNode.children || newNode.children.length === 0 || newNode.children.length > 0
        } else { // disabled
          // 禁用过滤：业务域自身禁用 或 有禁用的子节点
          if (currentNodeDisabled) {
            return true
          }
          // 业务域启用但有禁用的子节点
          return newNode.children && newNode.children.length > 0
        }
      }
      
      if (node.type === 'table') {
        if (statusFilter.value === 'enabled') {
          // 启用过滤：父业务域未禁用 且 表自身未禁用
          return !currentNodeDisabled
        } else { // disabled
          // 禁用过滤：表被禁用（包括父业务域禁用导致的禁用）或 有禁用的子字段
          if (currentNodeDisabled) {
            return true
          }
          // 表启用，但有禁用的子字段，也要显示
          return newNode.children && newNode.children.length > 0
        }
      }
      
      if (node.type === 'field') {
        if (statusFilter.value === 'enabled') {
          // 启用过滤：父表未禁用 且 字段自身未禁用
          return !currentNodeDisabled
        } else { // disabled
          // 禁用过滤：字段被禁用（包括父表/业务域禁用导致的禁用）
          return currentNodeDisabled
        }
      }
      
      return true
    }
    
    return shouldShow() ? newNode : null
  }
  
  return treeData.value
    .map(node => filterNode(node, false, false))
    .filter(node => node !== null)
})

// 编辑对话框状态
const editDialogVisible = ref(false)
const editDialogType = ref('') // 'domain', 'table', 'field'
const editingNode = ref(null)
const editForm = ref({
  domain_name: '',
  domain_code: '',
  description: '',
  keywords: [],
  display_name: '',
  table_name: '',
  data_year: '',
  tags: [],
  column_name: '',
  data_type: '',
  field_type: '',
  unit: '',
  default_aggregation: '',
  synonyms: [],
  is_active: true,  // 字段可用性，默认启用
  show_in_detail: true,  // 🆕 明细查询显示，默认显示
  priority: 50,  // 🆕 字段显示优先级，默认50
  // 🆕 单位转换配置
  unit_conversion_enabled: false,
  unit_conversion_display_unit: '',
  unit_conversion_method: 'divide',
  unit_conversion_factor: 10000,
  unit_conversion_precision: 2,
  // 枚举值统计
  enum_count: 0,
  synced_enum_count: 0,
  last_synced_at: null
})

// 自动识别业务域功能已移除

// 表关系管理状态
const relationshipsDialogVisible = ref(false)
const loadingRelationships = ref(false)
const detectingRelationships = ref(false)
const relationships = ref([])
const relationshipsConnectionId = ref('')  // 表关系数据源筛选
const sqlPreviewDialogVisible = ref(false)
const previewSQL = ref('')

// 表关系表格高度计算（基于对话框高度，留出头部和操作栏空间）
const relationshipsTableHeight = computed(() => {
  // 对话框高度约 80vh，减去头部、操作栏、padding等，约 200px
  const dialogHeight = Math.min(window.innerHeight * 0.8, 800)
  return dialogHeight - 200
})

// 字段枚举值状态
const loadingEnumValues = ref(false)
const samplingEnumValues = ref(false)
const fieldEnumValues = ref([])
const enumTableRef = ref(null)
const selectedEnumRows = ref([])
const sampleTopN = ref(1000)  // 采样数量

// 全局规则管理状态
const rulesDialogVisible = ref(false)
const ruleEditDialogVisible = ref(false)
const editingRule = ref(null)
const savingRule = ref(false)
const synonymMappingText = ref('')
const ruleForm = ref({
  rule_name: '',
  rule_type: '',
  description: '',
  priority: 50,
  rule_definition: {},
  is_active: true
})

// 枚举值编辑状态
const enumEditDialogVisible = ref(false)
const savingEnumValue = ref(false)
const enumEditForm = ref({
  enum_value_id: '',
  field_id: '',
  original_value: '',
  display_value: '',
  synonyms: [],
  includes_values: [],
  frequency: 0,
  is_active: true
})

// 可用表列表（用于新增表到业务域）
const availableTables = ref([])
const loadingAvailableTables = ref(false)

// 加载可用表列表（排除已分配到当前业务域的表）
// 修复：当没有选中特定连接时，加载所有连接下的表（业务域是全局的，不绑定特定连接）
async function loadAvailableTables(targetDomainId = null) {
  loadingAvailableTables.value = true
  try {
    let allTables = []
    
    if (currentConnectionId.value) {
      // 按当前选中的连接过滤
      const { data: tablesData } = await connectionAPI.getTables(currentConnectionId.value, { include_columns: false })
      const tables = tablesData.tables || tablesData || []
      // 为表添加连接名称信息
      const conn = connections.value.find(c => c.connection_id === currentConnectionId.value)
      tables.forEach(t => {
        t.connection_name = conn?.connection_name || ''
      })
      allTables = tables
    } else {
      // 没有选中连接时，加载所有连接下的表（支持全局业务域）
      for (const conn of connections.value) {
        try {
          const { data: tablesData } = await connectionAPI.getTables(conn.connection_id, { include_columns: false })
          const tables = tablesData.tables || tablesData || []
          // 为每个表添加连接名称，便于用户识别
          tables.forEach(t => {
            t.connection_name = conn.connection_name
          })
          allTables.push(...tables)
        } catch (error) {
          console.error(`加载连接 ${conn.connection_name} 的表失败`, error)
        }
      }
    }
    
    // 过滤出未分配业务域的表，或者不属于目标业务域的表（允许从其他业务域移动）
    availableTables.value = allTables.filter(t => !t.domain_id || t.domain_id !== targetDomainId)
  } catch (error) {
    console.error('加载可用表列表失败', error)
    availableTables.value = []
  } finally {
    loadingAvailableTables.value = false
  }
}

// 处理选择表变化
function handleSelectTableChange(tableId) {
  if (!tableId) {
    editForm.value.table_name = ''
    editForm.value.display_name = ''
    editForm.value.description = ''
    editForm.value.tags = []
    editForm.value.data_year = ''
    return
  }
  
  const selectedTable = availableTables.value.find(t => t.table_id === tableId)
  if (selectedTable) {
    editForm.value.table_name = selectedTable.table_name
    editForm.value.display_name = selectedTable.display_name || ''
    editForm.value.description = selectedTable.description || ''
    editForm.value.tags = selectedTable.tags || []
    editForm.value.data_year = selectedTable.data_year || ''
  }
}

// 加载数据库连接
async function loadConnections() {
  try {
    await adminStore.loadConnections()
    connections.value = adminStore.connections
    
    // 不再自动选择连接，保持空以显示所有数据
    // 用户可以选择某个连接来过滤
    
    // 立即加载树形数据（显示所有数据源）
    await loadTreeData()
  } catch (error) {
    console.error('加载数据库连接失败', error)
  }
}

// 加载树形数据
async function loadTreeData() {
  // 新架构：不再需要选择 connection_id，可以加载所有数据
  
  treeLoading.value = true
  try {
    // 1. 加载业务域列表（可选过滤条件）
    const domainParams = currentConnectionId.value ? { connection_id: currentConnectionId.value } : {}
    const domainsResp = await domainAPI.list(domainParams)
    const domains = domainsResp.data?.data || domainsResp.data || []
    domainOptions.value = Array.isArray(domains) ? domains : []
    // 2. 加载所有表（包含列信息）- 支持全部数据源或按连接过滤
    let allTables = []
    if (currentConnectionId.value) {
      // 按连接过滤
      const { data: tablesData } = await connectionAPI.getTables(currentConnectionId.value, { include_columns: true })
      allTables = tablesData.tables || tablesData || []
    } else {
      // 加载所有连接的表
      for (const conn of connections.value) {
        try {
          const { data: tablesData } = await connectionAPI.getTables(conn.connection_id, { include_columns: true })
          const tables = (tablesData.tables || tablesData || []).map(t => ({
            ...t,
            _connection_name: conn.connection_name  // 添加连接名称标记
          }))
          allTables = allTables.concat(tables)
        } catch (err) {
          console.warn('加载连接表失败:', conn.connection_name, err)
        }
      }
    }
    // 3. 构建树形结构
    const tree = []
    const assignedTableIds = new Set()
    
    // 添加业务域节点
    for (const domain of domains) {
      const domainId = getDomainId(domain)
      const domainTables = allTables.filter(t => {
        const tDomainId = getTableDomainId(t)
        return domainId && tDomainId && tDomainId === domainId
      })
      
      // 业务域禁用状态
      const domainDisabled = asBool(domain.is_active, true) === false
      
      const domainNode = {
        id: `domain-${domainId || domain.domain_name || domain.domain_code || crypto.randomUUID?.() || Math.random().toString(36).slice(2)}`,
        label: domain.domain_name || domain.domain_code || '未命名业务域',
        type: 'domain',
        count: domainTables.length,
        raw: domain,
        children: []
      }
      
      // 添加该业务域下的表节点
      for (const table of domainTables) {
        assignedTableIds.add(table.table_id)
        const tableDisabled = asBool(table.is_included, true) === false
        // 表的有效禁用状态：父业务域禁用 或 表自身禁用
        const effectiveTableDisabled = domainDisabled || tableDisabled
        const tableNode = {
          id: `table-${table.table_id}`,
          label: table.display_name || table.table_name,
          type: 'table',
          count: table.columns?.length || table.column_count,
          raw: table,
          parentDomainDisabled: domainDisabled, // 记录父业务域禁用状态
          children: []
        }
        
        // 添加表的字段节点
        if (table.columns && Array.isArray(table.columns)) {
          for (const column of table.columns) {
            tableNode.children.push({
              id: `field-${column.column_id || column.id || Math.random().toString(36).slice(2)}`,
              label: column.column_name || column.name,
              type: 'field',
              raw: column,
              parentDomainDisabled: domainDisabled, // 记录祖父业务域禁用状态
              parentTableDisabled: effectiveTableDisabled // 记录父表禁用状态（包含业务域级联）
            })
          }
        }
        
        domainNode.children.push(tableNode)
      }
      
      tree.push(domainNode)
    }
    
    // 添加未分配业务域的表
    const unassignedTables = allTables.filter(t => !assignedTableIds.has(t.table_id))
    if (unassignedTables.length > 0) {
      const unassignedNode = {
        id: 'domain-unassigned',
        label: '未分配业务域',
        type: 'domain',
        count: unassignedTables.length,
        raw: { domain_name: '未分配业务域', is_active: true },
        children: []
      }
      
      for (const table of unassignedTables) {
        const tableDisabled = asBool(table.is_included, true) === false
        const tableNode = {
          id: `table-${table.table_id}`,
          label: table.display_name || table.table_name,
          type: 'table',
          count: table.columns?.length || table.column_count,
          raw: table,
          parentDomainDisabled: false, // 未分配业务域不禁用
          children: []
        }
        
        // 添加表的字段节点
        if (table.columns && Array.isArray(table.columns)) {
          for (const column of table.columns) {
            tableNode.children.push({
              id: `field-${column.column_id || column.id || Math.random().toString(36).slice(2)}`,
              label: column.column_name || column.name,
              type: 'field',
              raw: column,
              parentDomainDisabled: false, // 未分配业务域不禁用
              parentTableDisabled: tableDisabled // 记录父表禁用状态
            })
          }
        }
        
        unassignedNode.children.push(tableNode)
      }
      
      tree.push(unassignedNode)
    }
    
    treeData.value = tree
    
    ensureDefaultExpandedNodes()
    // 刷新后恢复业务域与用户自定义的展开状态
    await restoreTreeExpansion()
  } catch (error) {
    console.error('加载树形数据失败', error)
    ElMessage.error('加载元数据结构失败')
  } finally {
    treeLoading.value = false
  }
}

// 切换数据库
function handleConnectionChange() {
  // 保存当前选择的连接到 localStorage
  if (currentConnectionId.value) {
    localStorage.setItem('metadataSelectedConnectionId', currentConnectionId.value)
  }
  selectedNode.value = null
  // watch会自动触发loadTreeData，不需要手动调用
}

// 同步Schema
async function syncSchema() {
  syncing.value = true
  try {
    await connectionAPI.sync(currentConnectionId.value)
    ElMessage.success('Schema同步成功！')
    await loadTreeData()
  } catch (error) {
    ElMessage.error('同步失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    syncing.value = false
  }
}

// 获取数据库图标
function getDbIcon(type) {
  const icons = {
    sqlserver: '🗄️',
    mysql: '🐬',
    postgresql: '🐘'
  }
  return icons[type] || '💾'
}

// 获取节点图标
function getNodeIcon(type) {
  const icons = {
    domain: FolderOpened,
    table: Grid,
    field: List
  }
  return icons[type] || Document
}

// 判断节点是否禁用（考虑层级关系）
function isNodeDisabled(data) {
  if (data.type === 'domain') {
    // 业务域禁用：自身 is_active 为 false
    return asBool(data.raw?.is_active, true) === false
  }
  if (data.type === 'table') {
    // 表禁用：父业务域禁用 或 表自身禁用
    return data.parentDomainDisabled || asBool(data.raw?.is_included, true) === false
  }
  if (data.type === 'field') {
    // 字段禁用：父表禁用（包含业务域级联）或 字段自身禁用
    return data.parentTableDisabled || asBool(data.raw?.is_active, true) === false
  }
  return false
}

// 判断节点自身是否禁用（不考虑父级继承）
function isNodeSelfDisabled(data) {
  if (data.type === 'domain') {
    return asBool(data.raw?.is_active, true) === false
  }
  if (data.type === 'table') {
    return asBool(data.raw?.is_included, true) === false
  }
  if (data.type === 'field') {
    return asBool(data.raw?.is_active, true) === false
  }
  return false
}

// 获取字段禁用原因描述
function getFieldDisabledReason(data) {
  if (data.type !== 'field') return '禁用'
  
  const fieldSelfDisabled = asBool(data.raw?.is_active, true) === false
  const parentTableDisabled = data.parentTableDisabled  // 包含业务域级联影响
  const parentDomainDisabled = data.parentDomainDisabled  // 仅业务域状态
  
  // 优先显示自身禁用状态
  if (fieldSelfDisabled) {
    return '禁用'
  }
  // 父业务域禁用导致（parentTableDisabled 已经包含了 domainDisabled 的影响）
  // 需要判断是业务域禁用还是表自身禁用
  if (parentDomainDisabled) {
    return '禁用（父业务域）'
  }
  // 父表自身禁用（非业务域导致）
  if (parentTableDisabled) {
    return '禁用（父表）'
  }
  return '禁用'
}

// 动态计算节点统计数（根据过滤器状态）
function getNodeCount(data) {
  if (data.type === 'field') {
    return undefined // 字段节点不显示统计数
  }
  
  if (data.type === 'domain') {
    // 业务域：统计表数量
    if (!data.children) return 0
    
    const domainDisabled = asBool(data.raw?.is_active, true) === false
    
    if (statusFilter.value === 'all') {
      return data.children.length
    } else if (statusFilter.value === 'enabled') {
      // 启用的表：业务域启用 且 表自身启用
      if (domainDisabled) return 0 // 业务域禁用，所有表都禁用
      return data.children.filter(table => asBool(table.raw?.is_included, true) === true).length
    } else { // disabled
      // 禁用的表：业务域禁用导致的 或 表自身禁用 或 有禁用字段的表
      if (domainDisabled) {
        return data.children.length // 业务域禁用，所有表都禁用
      }
      return data.children.filter(table => {
        const tableDisabled = asBool(table.raw?.is_included, true) === false
        if (tableDisabled) return true
        
        // 检查是否有禁用字段
        if (table.children) {
          return table.children.some(field => asBool(field.raw?.is_active, true) === false)
        }
        return false
      }).length
    }
  }
  
  if (data.type === 'table') {
    // 表：统计字段数量
    if (!data.children) return 0
    
    // 表的禁用状态：父业务域禁用 或 表自身禁用
    const tableDisabled = data.parentDomainDisabled || asBool(data.raw?.is_included, true) === false
    
    if (statusFilter.value === 'all') {
      return data.children.length
    } else if (statusFilter.value === 'enabled') {
      // 启用的字段（父表启用且字段自身启用）
      if (tableDisabled) return 0
      return data.children.filter(field => asBool(field.raw?.is_active, true) === true).length
    } else { // disabled
      // 禁用的字段（父表禁用或字段自身禁用）
      if (tableDisabled) {
        return data.children.length // 父表禁用，所有字段都禁用
      }
      return data.children.filter(field => asBool(field.raw?.is_active, true) === false).length
    }
  }
  
  return undefined
}

// 获取表的连接名称（用于详情展示）
const getTableConnectionName = (tableNode) => {
  const raw = tableNode?.raw || {}
  return raw._connection_name || raw.connection_name_cache || raw.connection_name || raw.connection?.connection_name || ''
}

// 递归查找节点
function findNodeById(nodes, targetId) {
  for (const node of nodes) {
    if (node.id === targetId) {
      return node
    }
    if (node.children && node.children.length > 0) {
      const found = findNodeById(node.children, targetId)
      if (found) {
        return found
      }
    }
  }
  return null
}

// 处理树节点点击
function handleNodeClick(data) {
  selectedNode.value = data
}

// 处理节点悬停
function handleNodeHover(data, isHover) {
  hoveredNodeId.value = isHover ? data.id : null
}

// 处理节点右键菜单（暂时禁用，使用悬停按钮代替）
function handleNodeContextMenu(event, data) {
  event.preventDefault()
  // 右键点击时选中节点
  selectedNode.value = data.data
}

function handleNodeExpand(data) {
  if (!data?.id) return
  if (expandedNodeIds.value.includes(data.id)) return
  expandedNodeIds.value = [...expandedNodeIds.value, data.id]
}

function handleNodeCollapse(data) {
  if (!data?.id) return
  expandedNodeIds.value = expandedNodeIds.value.filter(id => id !== data.id)
}

function getDefaultExpandedKeys() {
  return filteredTreeData.value
    .filter(node => node.type === 'domain')
    .map(node => node.id)
}

async function restoreTreeExpansion() {
  await nextTick()
  const tree = treeRef.value
  const nodesMap = tree?.store?.nodesMap
  if (!nodesMap) return

  const keysToExpand = new Set(expandedNodeIds.value)
  Object.entries(nodesMap).forEach(([key, node]) => {
    if (keysToExpand.has(key)) {
      node.expand()
    } else if (node.expanded) {
      node.collapse()
    }
  })
}

function ensureDefaultExpandedNodes() {
  if (expandedNodeIds.value.length === 0) {
    expandedNodeIds.value = getDefaultExpandedKeys()
  }
}

// 添加业务域
function handleAddDomain() {
  editDialogType.value = 'domain'
  editingNode.value = null
  editForm.value = {
    domain_name: '',
    domain_code: '',
    description: '',
    keywords: [],
    is_active: true
  }
  editDialogVisible.value = true
}

// 自动识别业务域功能已移除

// 批量自动识别字段类型功能已移除

// 添加子项
async function handleAddChild(node) {
  if (node.type === 'domain') {
    // 检查是否是"未分配业务域"节点
    if (node.id === 'domain-unassigned') {
      ElMessage.warning('无法向"未分配业务域"添加表，请先创建业务域')
      return
    }
    
    // 加载可用表列表
    await loadAvailableTables(node.raw.domain_id)
    
    // 准备添加表的对话框
    editDialogType.value = 'table'
    editingNode.value = null // null表示新增
    editForm.value = {
      table_id: '', // 用于选择表
      table_name: '',
      display_name: '',
      data_year: '',
      description: '',
      tags: [],
      domain_id: node.raw.domain_id, // 关联到当前业务域
      is_included: true
    }
    editDialogVisible.value = true
  } else if (node.type === 'table') {
    ElMessage.info('添加字段功能：请从数据库同步Schema获取')
  }
}

// 编辑节点
function handleEdit(node) {
  // 检查是否是"未分配业务域"节点
  if (node.id === 'domain-unassigned') {
    ElMessage.warning('无法编辑"未分配业务域"节点')
    return
  }
  
  editingNode.value = node
  editDialogType.value = node.type
  
  if (node.type === 'domain') {
    editForm.value = {
      domain_id: node.raw.domain_id,
      domain_name: node.raw.domain_name,
      domain_code: node.raw.domain_code || '',
      description: node.raw.description || '',
      keywords: node.raw.keywords || [],
      is_active: node.raw.is_active
    }
  } else if (node.type === 'table') {
    editForm.value = {
      table_id: node.raw.table_id,
      table_name: node.raw.table_name,
      display_name: node.raw.display_name || '',
      data_year: node.raw.data_year || '',
      description: node.raw.description || '',
      tags: node.raw.tags || [],
      domain_id: node.raw.domain_id || null,
      is_included: node.raw.is_included !== undefined && node.raw.is_included !== null ? !!node.raw.is_included : true // 默认为true，正确处理null/undefined，并确保转换为布尔值
    }
  } else if (node.type === 'field') {
    // 🆕 读取单位转换配置
    const unitConversion = node.raw.unit_conversion || {}
    const conversionConfig = unitConversion.conversion || {}
    
    editForm.value = {
      field_id: node.raw.field_id, // 字段ID（用于枚举值等操作）
      column_id: node.raw.column_id, // 列ID（用于更新字段）
      column_name: node.raw.column_name,
      data_type: node.raw.data_type || '',
      display_name: node.raw.display_name || node.raw.column_name, // 如果没有显示名称，使用字段名
      field_type: node.raw.field_type || '',
      unit: node.raw.unit || '',
      default_aggregation: node.raw.default_aggregation || '',
      synonyms: node.raw.synonyms || [],
      description: node.raw.description || '',
      // 统一处理 null/undefined/0/1 → 布尔，默认启用
      is_active: asBool(node.raw.is_active, true),
      show_in_detail: node.raw.show_in_detail !== undefined ? node.raw.show_in_detail : true,  // 🆕 明细查询显示
      priority: node.raw.priority !== undefined ? node.raw.priority : 50,  // 🆕 字段显示优先级
      // 🆕 单位转换配置
      unit_conversion_enabled: unitConversion.enabled || false,
      unit_conversion_display_unit: unitConversion.display_unit || '',
      unit_conversion_method: conversionConfig.method || 'divide',
      unit_conversion_factor: conversionConfig.factor ?? 10000,
      unit_conversion_precision: conversionConfig.precision !== undefined ? conversionConfig.precision : 2,
      // 枚举值统计
      enum_count: node.raw.enum_count || 0,
      synced_enum_count: node.raw.synced_enum_count || 0,
      last_synced_at: node.raw.last_synced_at || null
    }
  }
  
  editDialogVisible.value = true
  
  // 如果是维度字段，自动加载枚举值（静默模式）
  if (node.type === 'field' && editForm.value.field_type === 'dimension') {
    // 使用 nextTick 确保对话框已经打开
    nextTick(() => {
      loadFieldEnumValues(true) // true = 静默模式，不显示提示
    })
  }
}

// 编辑字段（从字段详情页调用）
function editField(node) {
  handleEdit(node)
}

// 查看字段枚举值
function viewFieldEnumValues(node) {
  // 直接进入编辑模式并显示枚举值
  handleEdit(node)
  // 在编辑对话框中会自动加载枚举值
}

// 获取字段枚举值数量
function getFieldEnumCount(node) {
  // 优先使用后端返回的枚举值数量
  if (node.raw && node.raw.enum_value_count !== undefined) {
    return node.raw.enum_value_count
  }
  // 兜底：如果有枚举值数组，返回数组长度
  if (node.raw && node.raw.enum_values && Array.isArray(node.raw.enum_values)) {
    return node.raw.enum_values.length
  }
  return 0
}

// 删除节点
async function handleDelete(node) {
  // 检查是否是"未分配业务域"节点
  if (node.id === 'domain-unassigned') {
    ElMessage.warning('无法删除"未分配业务域"节点')
    return
  }
  
  try {
    await ElMessageBox.confirm(
      `确定要删除 ${node.label} 吗？此操作不可恢复。`,
      '删除确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )
    
    // 执行删除
    if (node.type === 'domain') {
      await domainAPI.delete(node.raw.domain_id)
      ElMessage.success('业务域删除成功')
    } else if (node.type === 'table') {
      ElMessage.info('表删除功能：需要从数据库级别操作')
      return
    } else if (node.type === 'field') {
      ElMessage.info('字段删除功能：需要从数据库级别操作')
      return
    }
    
    // 刷新树形数据
    await loadTreeData()
    selectedNode.value = null
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败: ' + (error.response?.data?.detail || error.message))
    }
  }
}

// 保存编辑
async function saveEdit() {
  let targetNodeId = null
  let targetNodeType = null
  try {
    if (editDialogType.value === 'domain') {
      if (editingNode.value) {
        // 更新业务域
        await domainAPI.update(editForm.value.domain_id, editForm.value)
        ElMessage.success('业务域更新成功')
      } else {
        // 创建业务域
        const { data: created } = await domainAPI.create({
          ...editForm.value,
          connection_id: currentConnectionId.value
        })
        ElMessage.success('业务域创建成功')
        if (created && created.domain_id) {
          targetNodeId = `domain-${created.domain_id}`
          targetNodeType = 'domain'
        }
      }
    } else if (editDialogType.value === 'table') {
      if (editingNode.value) {
        // 更新表配置
        const tableId = editForm.value.table_id
        const updateData = {
          display_name: editForm.value.display_name || '',
          data_year: editForm.value.data_year || '',
          description: editForm.value.description || '',
          tags: editForm.value.tags || [],
          domain_id: editForm.value.domain_id || null,
          is_included: editForm.value.is_included !== undefined && editForm.value.is_included !== null ? !!editForm.value.is_included : true // 默认为true，正确处理null/undefined，并确保转换为布尔值
        }
        await tableAPI.update(tableId, updateData)
        ElMessage.success('表配置更新成功')
      } else {
        // 新增表到业务域（将选中的表关联到业务域）
        if (!editForm.value.table_id) {
          ElMessage.warning('请选择要添加的表')
          return
        }
        const tableId = editForm.value.table_id
        const updateData = {
          display_name: editForm.value.display_name || '',
          data_year: editForm.value.data_year || '',
          description: editForm.value.description || '',
          tags: editForm.value.tags || [],
          domain_id: editForm.value.domain_id, // 关联到目标业务域
          is_included: editForm.value.is_included !== undefined && editForm.value.is_included !== null ? !!editForm.value.is_included : true
        }
        await tableAPI.update(tableId, updateData)
        ElMessage.success('表添加成功')
        
        // 设置目标节点以便刷新后定位
        targetNodeId = `table-${tableId}`
        targetNodeType = 'table'
      }
    } else if (editDialogType.value === 'field') {
      // 更新字段配置
      const fieldId = editForm.value.column_id
      
      // 构建更新数据，只发送有值的字段
      const updateData = {}
      
      // 必填字段 - display_name至少需要1个字符，如果为空则使用字段名
      if (editForm.value.display_name !== undefined) {
        updateData.display_name = editForm.value.display_name?.trim() || editForm.value.column_name
      }
      
      // 可选字段 - 只有非空时才发送
      if (editForm.value.field_type) {
        updateData.field_type = editForm.value.field_type
      }
      
      if (editForm.value.unit) {
        updateData.unit = editForm.value.unit
      }
      
      if (editForm.value.default_aggregation) {
        updateData.default_aggregation = editForm.value.default_aggregation
      }
      
      if (editForm.value.description !== undefined) {
        updateData.description = editForm.value.description || ''
      }

      // 数组字段 - 确保是数组
      if (editForm.value.synonyms !== undefined) {
        updateData.synonyms = Array.isArray(editForm.value.synonyms) ? editForm.value.synonyms : []
      }

      // 布尔字段 - 始终发送（默认true）
      updateData.is_active = editForm.value.is_active ?? true

      // 🆕 明细查询显示（布尔字段，默认true）
      if (editForm.value.show_in_detail !== undefined) {
        updateData.show_in_detail = editForm.value.show_in_detail ?? true
      }
      
      // 🆕 字段显示优先级（数值字段）
      if (editForm.value.priority !== undefined && editForm.value.priority !== null) {
        updateData.priority = editForm.value.priority
      }
      
      // 🆕 单位转换配置
      if (editForm.value.unit_conversion_enabled) {
        updateData.unit_conversion = {
          enabled: true,
          display_unit: editForm.value.unit_conversion_display_unit || '',
          conversion: {
            factor: editForm.value.unit_conversion_factor ?? 10000,
            method: editForm.value.unit_conversion_method || 'divide',
            precision: editForm.value.unit_conversion_precision !== undefined ? editForm.value.unit_conversion_precision : 2
          }
        }
      } else {
        // 如果禁用转换，发送null以清除配置
        updateData.unit_conversion = null
      }
      
      await fieldAPI.update(fieldId, updateData)
      ElMessage.success('字段配置更新成功')
    }
    
    editDialogVisible.value = false
    
    // 保存当前选中节点的信息以便重新加载后恢复
    const currentNodeId = targetNodeId || selectedNode.value?.id
    const currentNodeType = targetNodeType || selectedNode.value?.type
    
    await loadTreeData()
    
    // 重新选中之前选中的节点，刷新详情面板
    if (currentNodeId && currentNodeType) {
      await nextTick(() => {
        const node = findNodeById(treeData.value, currentNodeId)
        if (node) {
          selectedNode.value = node
          treeRef.value?.setCurrentKey(currentNodeId)
        }
      })
    }
  } catch (error) {
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  }
}

// 同步业务域到 Milvus
function handleSyncDomain(node) {
  ElMessage.info('同步到Milvus功能：请在Milvus同步页面操作')
}

// 获取对话框标题
function getDialogTitle() {
  const titles = {
    domain: editingNode.value ? '编辑业务域' : '新增业务域',
    table: editingNode.value ? '编辑表配置' : '新增表',
    field: '编辑字段配置'
  }
  return titles[editDialogType.value] || '编辑'
}

// 获取字段类型颜色
function getFieldTypeColor(fieldType) {
  const colors = {
    measure: 'success',
    dimension: 'primary',
    timestamp: 'warning',
    identifier: 'info',
    spatial: 'danger'
  }
  return colors[fieldType] || ''
}

// 获取字段类型标签
function getFieldTypeLabel(fieldType) {
  const labels = {
    measure: '度量',
    dimension: '维度',
    timestamp: '时间戳',
    identifier: '标识符',
    spatial: '空间'
  }
  return labels[fieldType] || fieldType
}

// ============================================================================
// 表关系管理
// ============================================================================

// 显示表关系管理对话框
function showRelationshipsDialog() {
  // 初始化数据源（使用当前筛选的数据源或第一个）
  relationshipsConnectionId.value = currentConnectionId.value || (connections.value.length > 0 ? connections.value[0].connection_id : '')
  relationshipsDialogVisible.value = true
  loadRelationships()
}

// 加载表关系列表
async function loadRelationships() {
  loadingRelationships.value = true
  try {
    // 如果选择了数据源则按数据源筛选，否则加载所有
    const params = relationshipsConnectionId.value ? { connection_id: relationshipsConnectionId.value } : {}
    const { data } = await relationshipAPI.list(params)
    relationships.value = data || []
  } catch (error) {
    console.error('加载表关系失败', error)
    ElMessage.error('加载表关系失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    loadingRelationships.value = false
  }
}

// 自动识别表关系
async function autoDetectRelationships() {
  if (!relationshipsConnectionId.value) {
    ElMessage.warning('请先选择数据源')
    return
  }
  
  detectingRelationships.value = true
  try {
    const { data } = await relationshipAPI.autoDetect(relationshipsConnectionId.value)
    
    if (data.success) {
      ElMessage.success(data.message || '表关系识别完成')
      await loadRelationships()
    } else {
      ElMessage.error(data.message || '识别失败')
    }
  } catch (error) {
    console.error('自动识别表关系失败', error)
    ElMessage.error('识别失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    detectingRelationships.value = false
  }
}

// 确认表关系
async function confirmRelationship(relationshipId) {
  try {
    const { data } = await relationshipAPI.confirm(relationshipId)
    
    if (data.success) {
      ElMessage.success('确认成功')
      await loadRelationships()
    } else {
      ElMessage.error('确认失败')
    }
  } catch (error) {
    console.error('确认表关系失败', error)
    ElMessage.error('确认失败: ' + (error.response?.data?.detail || error.message))
  }
}

// 预览表关系SQL
async function previewRelationshipSQL(relationshipId) {
  try {
    const { data } = await relationshipAPI.previewSQL(relationshipId)
    
    if (data.success && data.sql) {
      previewSQL.value = data.sql
      sqlPreviewDialogVisible.value = true
    } else {
      ElMessage.error('预览失败')
    }
  } catch (error) {
    console.error('预览SQL失败', error)
    ElMessage.error('预览失败: ' + (error.response?.data?.detail || error.message))
  }
}

// 删除表关系
async function deleteRelationship(relationshipId) {
  try {
    await ElMessageBox.confirm(
      '确定要删除此表关系吗？',
      '删除确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )
    
    await relationshipAPI.delete(relationshipId)
    ElMessage.success('删除成功')
    await loadRelationships()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('删除表关系失败', error)
      ElMessage.error('删除失败: ' + (error.response?.data?.detail || error.message))
    }
  }
}

// 获取关系类型标签
function getRelationshipTypeLabel(type) {
  const labels = {
    one_to_one: '一对一',
    one_to_many: '一对多',
    many_to_many: '多对多'
  }
  return labels[type] || type
}

// 获取关系类型颜色
function getRelationshipTypeColor(type) {
  const colors = {
    one_to_one: 'success',
    one_to_many: 'primary',
    many_to_many: 'warning'
  }
  return colors[type] || ''
}

// 获取识别方法标签
function getDetectionMethodLabel(method) {
  const labels = {
    foreign_key: '外键',
    name_similarity: '名称相似',
    data_analysis: '数据分析',
    manual: '手动'
  }
  return labels[method] || method
}

// 获取置信度颜色
function getConfidenceColor(score) {
  if (score >= 0.8) return '#67c23a'
  if (score >= 0.6) return '#e6a23c'
  return '#f56c6c'
}

// ============================================================================
// 全局规则管理
// ============================================================================

// 显示规则管理对话框
function showRulesDialog() {
  rulesDialogVisible.value = true
  // 规则列表由 RuleList 组件内部加载
}

// 显示创建规则对话框
function showCreateRuleDialog() {
  editingRule.value = null
  ruleForm.value = {
    rule_name: '',
    rule_type: '',
    description: '',
    priority: 50,
    rule_definition: {},
    is_active: true
  }
  synonymMappingText.value = ''
  ruleEditDialogVisible.value = true
}

// 编辑规则
function editRule(rule) {
  editingRule.value = rule
  ruleForm.value = {
    rule_id: rule.rule_id,
    rule_name: rule.rule_name,
    rule_type: rule.rule_type,
    description: rule.description || '',
    priority: rule.priority,
    rule_definition: { ...rule.rule_definition },
    is_active: rule.is_active
  }
  
  // 如果是同义词映射，转换为文本格式
  if (rule.rule_type === 'synonym_mapping' && rule.rule_definition.synonyms) {
    const lines = []
    for (const [key, values] of Object.entries(rule.rule_definition.synonyms)) {
      lines.push([key, ...values].join(','))
    }
    synonymMappingText.value = lines.join('\n')
  }
  
  ruleEditDialogVisible.value = true
}

// 处理规则类型变化
function handleRuleTypeChange(newType) {
  // 重置规则定义
  if (newType === 'derived_metric') {
    ruleForm.value.rule_definition = {
      formula: '',
      display_name: '',
      dependencies: [],
      unit: '',
      description: ''
    }
  } else if (newType === 'unit_conversion') {
    ruleForm.value.rule_definition = {
      from_unit: '',
      to_unit: '',
      conversion_factor: 1,
      description: ''
    }
  } else if (newType === 'validation') {
    ruleForm.value.rule_definition = {
      field_id: '',
      rule_expression: '',
      error_message: '',
      severity: 'error'
    }
  } else if (newType === 'synonym_mapping') {
    ruleForm.value.rule_definition = {
      field_id: '',
      synonyms: {}
    }
    synonymMappingText.value = ''
  }
}

// 解析同义词映射文本
function parseSynonymMapping() {
  if (!synonymMappingText.value.trim()) {
    ruleForm.value.rule_definition.synonyms = {}
    return
  }
  
  const synonyms = {}
  const lines = synonymMappingText.value.split('\n')
  
  for (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed) continue
    
    const parts = trimmed.split(',').map(s => s.trim()).filter(s => s)
    if (parts.length >= 2) {
      const key = parts[0]
      const values = parts.slice(1)
      synonyms[key] = values
    }
  }
  
  ruleForm.value.rule_definition.synonyms = synonyms
}

// 保存规则
async function saveRule() {
  // 验证必填字段
  if (!ruleForm.value.rule_name) {
    ElMessage.warning('请输入规则名称')
    return
  }
  
  if (!ruleForm.value.rule_type) {
    ElMessage.warning('请选择规则类型')
    return
  }
  
  // 同义词映射需要先解析
  if (ruleForm.value.rule_type === 'synonym_mapping') {
    parseSynonymMapping()
  }
  
  savingRule.value = true
  try {
    const payload = {
      connection_id: currentConnectionId.value,
      rule_name: ruleForm.value.rule_name,
      rule_type: ruleForm.value.rule_type,
      description: ruleForm.value.description,
      priority: ruleForm.value.priority,
      rule_definition: ruleForm.value.rule_definition,
      is_active: ruleForm.value.is_active
    }
    
    if (editingRule.value) {
      // 更新规则
      await ruleAPI.update(ruleForm.value.rule_id, payload)
      ElMessage.success('规则更新成功')
    } else {
      // 创建规则
      await ruleAPI.create(payload)
      ElMessage.success('规则创建成功')
    }
    
    ruleEditDialogVisible.value = false
    await loadRules()
  } catch (error) {
    console.error('保存规则失败', error)
    ElMessage.error('保存失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    savingRule.value = false
  }
}

// 删除规则
async function deleteRule(ruleId) {
  try {
    await ElMessageBox.confirm(
      '确定要删除此规则吗？',
      '删除确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )
    
    await ruleAPI.delete(ruleId)
    ElMessage.success('删除成功')
    await loadRules()
  } catch (error) {
    if (error !== 'cancel') {
      console.error('删除规则失败', error)
      ElMessage.error('删除失败: ' + (error.response?.data?.detail || error.message))
    }
  }
}

// 获取规则类型标签
function getRuleTypeLabel(type) {
  const labels = {
    derived_metric: '派生指标',
    unit_conversion: '单位转换',
    validation: '校验规则',
    synonym_mapping: '同义词映射'
  }
  return labels[type] || type
}

// 获取规则类型颜色
function getRuleTypeColor(type) {
  const colors = {
    derived_metric: 'success',
    unit_conversion: 'primary',
    validation: 'warning',
    synonym_mapping: 'info'
  }
  return colors[type] || ''
}

// 格式化规则定义预览
function formatRuleDefinition(definition, type) {
  if (!definition) return '-'
  
  try {
    if (type === 'derived_metric') {
      return definition.formula || '-'
    } else if (type === 'unit_conversion') {
      return `${definition.from_unit} → ${definition.to_unit} (×${definition.conversion_factor})`
    } else if (type === 'validation') {
      return definition.rule_expression || '-'
    } else if (type === 'synonym_mapping') {
      const synonyms = definition.synonyms || {}
      const count = Object.keys(synonyms).length
      return `${count} 组同义词`
    }
    return JSON.stringify(definition)
  } catch (e) {
    return '-'
  }
}

// ============================================================================
// 字段枚举值管理
// ============================================================================

// 处理字段类型变化
function handleFieldTypeChange(newType) {
  // 如果切换到非维度类型，清空枚举值
  if (newType !== 'dimension') {
    fieldEnumValues.value = []
  }
  
  // 如果切换到非度量类型，清空度量相关字段
  if (newType !== 'measure') {
    editForm.value.unit = ''
    editForm.value.default_aggregation = ''
    editForm.value.unit_conversion_enabled = false
  }
}

// 🆕 处理单位转换开关切换
function handleUnitConversionToggle(enabled) {
  if (enabled) {
    // 启用时初始化默认值（如果为空）
    if (!editForm.value.unit_conversion_method) {
      editForm.value.unit_conversion_method = 'divide'
    }
    if (editForm.value.unit_conversion_factor === undefined || editForm.value.unit_conversion_factor === null) {
      editForm.value.unit_conversion_factor = 10000
    }
    if (editForm.value.unit_conversion_precision === undefined || editForm.value.unit_conversion_precision === null) {
      editForm.value.unit_conversion_precision = 2
    }
    // 阈值逻辑已取消，无需设置
  }
}

// 根据数据类型获取建议的字段类型
function getSuggestedFieldType() {
  const dataType = (editForm.value.data_type || '').toLowerCase()
  const columnName = (editForm.value.column_name || '').toLowerCase()
  const displayName = (editForm.value.display_name || '').toLowerCase()
  
  if (!dataType) return null
  
  // 空间类型（GIS数据）
  const spatialTypes = ['geometry', 'geography', 'point', 'linestring', 'polygon', 
                        'multipoint', 'multilinestring', 'multipolygon', 
                        'geometrycollection', 'geom', 'shape']
  if (spatialTypes.some(type => dataType.includes(type))) {
    return 'spatial'
  }
  
  // 检查字段名是否为标识符类型（英文+中文关键词）
  const isIdentifier = (name) => {
    const idPatterns = ['_id', 'id_', 'key', 'code', '_no', 'no_', '编号', '代码', '编码', '序号', '主键']
    return idPatterns.some(pattern => name.includes(pattern))
  }
  
  // 检查字段名是否为度量类型（英文+中文关键词）
  const isMeasure = (name) => {
    const measurePatterns = [
      'amount', 'price', 'cost', 'fee', 'money', 'salary', 'income', 'revenue', 'total',
      'count', 'qty', 'quantity', 'num', 'number', 'sum', 'avg', 'average',
      'rate', 'percent', 'ratio', 'score', 'value', 'weight', 'height', 'width', 'length',
      '金额', '价格', '费用', '成本', '工资', '收入', '营收', '销售额', '总额', '总价',
      '数量', '数目', '个数', '件数', '笔数', '次数', '人数',
      '比率', '比例', '百分比', '占比', '得分', '分数', '评分',
      '重量', '高度', '宽度', '长度', '面积', '体积', '容量'
    ]
    return measurePatterns.some(pattern => name.includes(pattern))
  }
  
  // 数值类型
  const numericTypes = ['int', 'integer', 'bigint', 'smallint', 'tinyint', 
                        'decimal', 'numeric', 'float', 'double', 'real', 'money']
  if (numericTypes.some(type => dataType.includes(type))) {
    // 优先检查是否为标识符
    if (isIdentifier(columnName) || isIdentifier(displayName)) {
      return 'identifier'
    }
    // 默认数值类型为度量
    return 'measure'
  }
  
  // 时间类型
  const dateTypes = ['date', 'datetime', 'timestamp', 'time', 'year']
  if (dateTypes.some(type => dataType.includes(type))) {
    return 'timestamp'
  }
  
  // 字符串类型
  const stringTypes = ['char', 'varchar', 'nchar', 'nvarchar', 'text', 'string']
  if (stringTypes.some(type => dataType.includes(type))) {
    // 优先检查是否为标识符
    if (isIdentifier(columnName) || isIdentifier(displayName)) {
      return 'identifier'
    }
    // 检查是否为度量（某些字符串存储的数值）
    if (isMeasure(columnName) || isMeasure(displayName)) {
      return 'measure'
    }
    return 'dimension'
  }
  
  return null
}

// 获取建议字段类型的标签
function getSuggestedFieldTypeLabel() {
  const suggested = getSuggestedFieldType()
  const labels = {
    measure: '度量',
    dimension: '维度',
    timestamp: '时间戳',
    identifier: '标识符',
    spatial: '空间'
  }
  return labels[suggested] || ''
}

// 应用建议的字段类型
function applySuggestedFieldType() {
  const suggested = getSuggestedFieldType()
  if (suggested) {
    editForm.value.field_type = suggested
    handleFieldTypeChange(suggested)
    
    // 如果是度量类型，根据字段名自动设置单位和默认聚合
    if (suggested === 'measure') {
      const columnName = (editForm.value.column_name || '').toLowerCase()
      const displayName = (editForm.value.display_name || '').toLowerCase()
      const fullName = columnName + displayName
      
      // 自动设置单位（支持中英文字段名）
      if (fullName.match(/amount|price|cost|fee|money|salary|income|revenue|金额|价格|费用|成本|工资|收入|营收|总价|单价/)) {
        editForm.value.unit = '元'
      } else if (fullName.match(/count|qty|quantity|num|number|数量|数目|个数|件数|笔数|次数|人数/)) {
        editForm.value.unit = '个'
      } else if (fullName.match(/percent|rate|ratio|百分比|比率|比例|占比/)) {
        editForm.value.unit = '%'
      } else if (fullName.match(/weight|重量/)) {
        editForm.value.unit = 'kg'
      } else if (fullName.match(/height|width|length|高度|宽度|长度/)) {
        editForm.value.unit = 'm'
      } else if (fullName.match(/area|面积/)) {
        editForm.value.unit = 'm²'
      } else if (fullName.match(/volume|体积|容量/)) {
        editForm.value.unit = 'm³'
      } else if (fullName.match(/score|得分|分数|评分/)) {
        editForm.value.unit = '分'
      }
      
      // 自动设置默认聚合方式
      if (fullName.match(/count|num|quantity|数量|个数|件数|笔数|次数|人数/)) {
        editForm.value.default_aggregation = 'sum'
      } else if (fullName.match(/avg|average|平均/)) {
        editForm.value.default_aggregation = 'avg'
      } else if (fullName.match(/max|最大|最高/)) {
        editForm.value.default_aggregation = 'max'
      } else if (fullName.match(/min|最小|最低/)) {
        editForm.value.default_aggregation = 'min'
      } else {
        // 默认使用求和
        editForm.value.default_aggregation = 'sum'
      }
    }
    
    ElMessage.success(`已应用推荐：${getSuggestedFieldTypeLabel()}`)
  }
}

// 加载字段枚举值
async function loadFieldEnumValues(silent = false) {
  // 优先使用field_id，如果没有则使用column_id作为兜底
  const fieldId = editForm.value.field_id || editForm.value.column_id
  if (!fieldId) {
    if (!silent) {
      ElMessage.warning('字段ID不存在')
    }
    return
  }
  
  loadingEnumValues.value = true
  try {
    const { data } = await fieldAPI.getEnumValues(fieldId)
    fieldEnumValues.value = data || []
    
    // 只在非静默模式下显示提示
    if (!silent) {
      if (fieldEnumValues.value.length === 0) {
        ElMessage.info('该字段暂无枚举值数据，请点击"从数据库采样"按钮获取')
      } else {
        ElMessage.success(`加载成功，共 ${fieldEnumValues.value.length} 个枚举值`)
      }
    }
  } catch (error) {
    console.error('加载枚举值失败', error)
    if (!silent) {
      ElMessage.error('加载枚举值失败: ' + (error.response?.data?.detail || error.message))
    }
  } finally {
    loadingEnumValues.value = false
  }
}

// 从数据库采样枚举值
async function sampleFieldEnumValues() {
  // 优先使用field_id，如果没有则使用column_id作为兜底
  const fieldId = editForm.value.field_id || editForm.value.column_id
  if (!fieldId) {
    ElMessage.warning('字段ID不存在')
    return
  }
  
  try {
    await ElMessageBox.prompt(
      `将从业务数据库中采样该字段的枚举值（按频次降序）。\n\n建议：\n• 低基数字段（如行政区）：100-500\n• 中基数字段（如街道）：1000-3000\n• 高基数字段（如公司名）：5000-10000+`,
      '采样枚举值',
      {
        confirmButtonText: '开始采样',
        cancelButtonText: '取消',
        inputPattern: /^[1-9]\d*$/,
        inputErrorMessage: '请输入有效的正整数',
        inputValue: sampleTopN.value.toString(),
        inputPlaceholder: '采样数量'
      }
    ).then(({ value }) => {
      sampleTopN.value = parseInt(value)
    })
    
    samplingEnumValues.value = true
    try {
      const { data } = await fieldAPI.sampleEnumValues(fieldId, { top_n: sampleTopN.value })
      
      if (data.success) {
        ElMessage.success(data.message || `采样成功！已采样 ${data.sampled_count || sampleTopN.value} 个枚举值`)
        // 重新加载枚举值
        await loadFieldEnumValues()
      } else {
        ElMessage.warning(data.message || '采样功能暂未完全实现，请联系管理员')
      }
    } catch (error) {
      console.error('采样枚举值失败', error)
      ElMessage.error('采样失败: ' + (error.response?.data?.detail || error.message))
    } finally {
      samplingEnumValues.value = false
    }
  } catch (error) {
    if (error !== 'cancel') {
      console.error(error)
    }
  }
}

// 监听连接变化（包括清空）
watch(currentConnectionId, () => {
  // 无论是选择连接还是清空，都重新加载数据
  loadTreeData()
})

// 编辑枚举值
function editEnumValue(row) {
  enumEditForm.value = {
    enum_value_id: row.enum_value_id,
    field_id: row.field_id,
    original_value: row.original_value,
    display_value: row.display_value || row.original_value,
    synonyms: row.synonyms || [],
    includes_values: row.includes_values || [],
    frequency: row.frequency || 0,
    is_active: row.is_active !== false
  }
  enumEditDialogVisible.value = true
}

function handleEnumSelectionChange(val) {
  selectedEnumRows.value = val || []
}


// 删除枚举值
async function deleteEnumValue(row) {
  try {
    await ElMessageBox.confirm(
      `确定要删除枚举值 "${row.original_value}" 吗？此操作不可恢复。`,
      '删除确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )
    await fieldAPI.deleteEnumValue(row.field_id, row.enum_value_id)
    ElMessage.success('删除成功')
    await loadFieldEnumValues(true)
  } catch (error) {
    if (error !== 'cancel') {
      console.error('删除枚举值失败', error)
      ElMessage.error('删除失败: ' + (error.response?.data?.detail || error.message))
    }
  }
}

// 批量删除枚举值
async function batchDeleteEnumValues() {
  if (!selectedEnumRows.value || selectedEnumRows.value.length === 0) return
  const total = selectedEnumRows.value.length
  try {
    await ElMessageBox.confirm(
      `确定要删除选中的 ${total} 个枚举值吗？此操作不可恢复。`,
      '批量删除确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }
    )

    const fieldIdFallback = editForm.value.field_id || editForm.value.column_id
    const results = await Promise.allSettled(
      selectedEnumRows.value.map(row =>
        fieldAPI.deleteEnumValue(row.field_id || fieldIdFallback, row.enum_value_id)
      )
    )
    const success = results.filter(r => r.status === 'fulfilled').length
    const failed = results.length - success
    if (success > 0) {
      ElMessage.success(`删除完成：成功 ${success}，失败 ${failed}`)
    } else {
      ElMessage.error('删除失败')
    }
    await loadFieldEnumValues(true)
    selectedEnumRows.value = []
    if (enumTableRef.value) {
      enumTableRef.value.clearSelection()
    }
  } catch (error) {
    if (error !== 'cancel') {
      console.error('批量删除枚举值失败', error)
      ElMessage.error('删除失败: ' + (error.response?.data?.detail || error.message))
    }
  }
}

// 保存枚举值
async function saveEnumValue() {
  try {
    savingEnumValue.value = true
    
    const updateData = {
      display_value: enumEditForm.value.display_value,
      synonyms: enumEditForm.value.synonyms,
      includes_values: enumEditForm.value.includes_values && enumEditForm.value.includes_values.length > 0 
        ? enumEditForm.value.includes_values 
        : null,
      is_active: enumEditForm.value.is_active
    }
    
    await fieldAPI.updateEnumValue(
      enumEditForm.value.field_id,
      enumEditForm.value.enum_value_id,
      updateData
    )
    
    ElMessage.success('枚举值更新成功')
    enumEditDialogVisible.value = false
    
    // 重新加载枚举值列表
    await loadFieldEnumValues()
  } catch (error) {
    console.error('更新枚举值失败', error)
    ElMessage.error('更新失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    savingEnumValue.value = false
  }
}

// 监听状态过滤器变化，保持列表展开状态
watch(statusFilter, () => {
  restoreTreeExpansion()
})

// ============================================================================
// 元数据导入导出
// ============================================================================

// 导入导出状态
const importDialogVisible = ref(false)
const importFile = ref(null)
const importMode = ref('update')
const importLoading = ref(false)
const importPreviewResult = ref(null)
const importConnectionId = ref('')  // 导入目标数据源
const importTargetMode = ref('auto')  // 导入方式: 'auto'(自动识别) 或 'single'(指定数据源)
const previewExpandedConnections = ref([])  // 多数据源预览展开的连接
const exportDialogVisible = ref(false)
const exportLoading = ref(false)
const exportTableList = ref([])  // 可选的表列表
const selectedExportTables = ref([])  // 用户选中的表
const exportConnectionIds = ref([])  // 导出源数据源（支持多选）

// 监听导出数据源变化，重新加载表列表
watch(exportConnectionIds, async (newVal) => {
  if (exportDialogVisible.value) {
    try {
      let allTables = []
      if (newVal && newVal.length > 0) {
        // 加载选中数据源的表
        for (const connId of newVal) {
          const response = await tableAPI.list({ connection_id: connId })
          const tables = response?.data?.data || response?.data || []
          if (Array.isArray(tables)) {
            // 添加数据源信息以便区分
            const conn = connections.value.find(c => c.connection_id === connId)
            allTables.push(...tables.map(t => ({
              ...t,
              _connectionName: conn?.connection_name || ''
            })))
          }
        }
      } else {
        // 加载所有表
        const response = await tableAPI.list({})
        const tables = response?.data?.data || response?.data || []
        if (Array.isArray(tables)) {
          allTables = tables.map(t => {
            const conn = connections.value.find(c => c.connection_id === t.connection_id)
            return { ...t, _connectionName: conn?.connection_name || '' }
          })
        }
      }
      exportTableList.value = allTables
      selectedExportTables.value = []  // 清空已选表
    } catch (error) {
      console.error('加载表列表失败', error)
      exportTableList.value = []
    }
  }
}, { deep: true })

// 按数据源分组的导出表列表
const groupedExportTables = computed(() => {
  const groups = {}
  for (const table of exportTableList.value) {
    const connName = table._connectionName || '未知数据源'
    if (!groups[connName]) {
      groups[connName] = []
    }
    groups[connName].push(table)
  }
  return Object.entries(groups).map(([connectionName, tables]) => ({
    connectionName,
    tables
  }))
})

// 导入导出选项
const exportOptions = ref({
  include_domains: true,
  include_tables: true,
  include_fields: true,
  include_enums: true,
  include_relationships: true,
  include_rules: true,
  table_name: ''  // 指定表名
})


// 处理导入导出命令
function handleIOCommand(command) {
  if (command === 'export') {
    showExportDialog()
  } else if (command === 'import') {
    showImportDialog()
  }
}

// 显示导出对话框
async function showExportDialog() {
  // 初始化导出数据源（使用当前筛选的数据源，留空表示导出全部）
  exportConnectionIds.value = currentConnectionId.value ? [currentConnectionId.value] : []
  
  // 重置选项
  selectedExportTables.value = []
  exportOptions.value.include_domains = true
  exportOptions.value.include_tables = true
  exportOptions.value.include_fields = true
  exportOptions.value.include_enums = true
  exportOptions.value.include_relationships = true
  exportOptions.value.include_rules = true
  exportOptions.value.table_name = ''
  
  // 加载表列表
  try {
    let allTables = []
    if (exportConnectionIds.value.length > 0) {
      for (const connId of exportConnectionIds.value) {
        const response = await tableAPI.list({ connection_id: connId })
        const tables = response?.data?.data || response?.data || []
        if (Array.isArray(tables)) {
          const conn = connections.value.find(c => c.connection_id === connId)
          allTables.push(...tables.map(t => ({
            ...t,
            _connectionName: conn?.connection_name || ''
          })))
        }
      }
    } else {
      const response = await tableAPI.list({})
      const tables = response?.data?.data || response?.data || []
      if (Array.isArray(tables)) {
        allTables = tables.map(t => {
          const conn = connections.value.find(c => c.connection_id === t.connection_id)
          return { ...t, _connectionName: conn?.connection_name || '' }
        })
      }
    }
    exportTableList.value = allTables
  } catch (error) {
    console.error('加载表列表失败', error)
    exportTableList.value = []
  }
  
  exportDialogVisible.value = true
}

// 执行导出
async function executeExport() {
  exportLoading.value = true
  try {
    // 构建导出选项
    const options = { ...exportOptions.value }
    
    // 如果选择了特定的表
    if (selectedExportTables.value.length > 0) {
      options.table_name = selectedExportTables.value.join(',')
    }
    
    // 构建下载URL
    // 多选数据源时用逗号分隔，空数组导出所有
    const connectionIdParam = exportConnectionIds.value.length > 0 
      ? exportConnectionIds.value.join(',') 
      : null

    // 请求导出文件（自动带上认证头）
    const response = await metadataIOAPI.exportTemplate(connectionIdParam, options)
    const blob = response?.data
    if (!blob) {
      throw new Error('未获取到导出文件数据')
    }

    // 解析文件名（兼容 filename* 和 filename）
    const disposition = response.headers?.['content-disposition'] || ''
    let filename = 'metadata_export.xlsx'
    const match = disposition.match(/filename\*?=([^;]+)/i)
    if (match && match[1]) {
      const raw = match[1].trim().replace(/(^["']|["']$)/g, '')
      filename = raw.startsWith("UTF-8''")
        ? decodeURIComponent(raw.replace("UTF-8''", ''))
        : raw
    }

    // 触发下载
    const blobUrl = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = blobUrl
    link.download = filename
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(blobUrl)

    ElMessage.success('正在下载配置模板...')
    exportDialogVisible.value = false
  } catch (error) {
    console.error('导出失败', error)
    const detail = error?.response?.data?.detail || error.message || '导出失败'
    ElMessage.error('导出失败: ' + detail)
  } finally {
    exportLoading.value = false
  }
}

// 显示导入对话框
function showImportDialog() {
  // 初始化导入数据源（使用当前筛选的数据源或第一个）
  importConnectionId.value = currentConnectionId.value || (connections.value.length > 0 ? connections.value[0].connection_id : '')
  
  if (!importConnectionId.value && connections.value.length === 0) {
    ElMessage.warning('没有可用的数据库连接')
    return
  }
  
  importDialogVisible.value = true
  importFile.value = null
  importPreviewResult.value = null
  importMode.value = 'update'
  importTargetMode.value = 'auto'  // 默认自动识别
  previewExpandedConnections.value = []
}

// 处理文件选择
function handleFileChange(file) {
  importFile.value = file.raw
  importPreviewResult.value = null
}

// 处理导入方式切换
function handleImportTargetModeChange() {
  importPreviewResult.value = null
  previewExpandedConnections.value = []
}

// 预览导入
async function handleImportPreview() {
  if (importTargetMode.value === 'single' && !importConnectionId.value) {
    ElMessage.warning('请选择目标数据源')
    return
  }
  if (!importFile.value) {
    ElMessage.warning('请先选择文件')
    return
  }
  
  importLoading.value = true
  try {
    let result
    if (importTargetMode.value === 'auto') {
      // 使用统一导入API（自动识别）
      result = await metadataIOAPI.importUnifiedPreview(
        null,  // 不指定数据源，自动识别
        importFile.value,
        importMode.value
      )
    } else {
      // 使用单数据源导入API
      result = await metadataIOAPI.importPreview(
        importConnectionId.value,
        importFile.value,
        importMode.value
      )
    }
    importPreviewResult.value = result.data || result
    
    // 多数据源模式下默认展开所有连接
    if (importPreviewResult.value.by_connection) {
      previewExpandedConnections.value = Object.keys(importPreviewResult.value.by_connection)
    }
    
    if (importPreviewResult.value.errors?.length > 0) {
      ElMessage.warning('预览完成，发现一些错误，请查看详情')
    } else {
      ElMessage.success('预览完成，可以执行导入')
    }
  } catch (error) {
    console.error('预览失败', error)
    ElMessage.error('预览失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    importLoading.value = false
  }
}

// 执行导入
async function handleImportExecute() {
  if (importTargetMode.value === 'single' && !importConnectionId.value) {
    ElMessage.warning('请选择目标数据源')
    return
  }
  if (!importFile.value) {
    ElMessage.warning('请先选择文件')
    return
  }
  
  const confirmMsg = importTargetMode.value === 'auto' 
    ? '确定要执行导入吗？数据将自动分发到对应的数据源。'
    : '确定要执行导入吗？这将更新现有配置。'
  
  try {
    await ElMessageBox.confirm(confirmMsg, '确认导入', { type: 'warning' })
  } catch {
    return
  }
  
  importLoading.value = true
  try {
    let result
    if (importTargetMode.value === 'auto') {
      result = await metadataIOAPI.importUnifiedExecute(
        null,
        importFile.value,
        importMode.value
      )
    } else {
      result = await metadataIOAPI.importExecute(
        importConnectionId.value,
        importFile.value,
        importMode.value
      )
    }
    
    const data = result.data || result
    if (data.success) {
      ElMessage.success('导入成功！')
      importDialogVisible.value = false
      // 刷新树形数据
      loadTreeData()
    } else {
      ElMessage.error('导入失败：' + (data.errors?.[0]?.message || '未知错误'))
    }
  } catch (error) {
    console.error('导入失败', error)
    ElMessage.error('导入失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    importLoading.value = false
  }
}

// 获取数据源统计文本（用于多数据源预览）
function getConnectionStatsText(summary) {
  let newCount = 0, updateCount = 0
  for (const key in summary) {
    newCount += summary[key].new || 0
    updateCount += summary[key].update || 0
  }
  const parts = []
  if (newCount > 0) parts.push(`+${newCount}`)
  if (updateCount > 0) parts.push(`~${updateCount}`)
  return parts.length > 0 ? parts.join(' ') : '无变更'
}

// 检查预览结果是否为空（没有任何变更）
const isPreviewEmpty = computed(() => {
  const result = importPreviewResult.value
  if (!result) return true
  
  // 多数据源模式
  if (result.by_connection) {
    for (const connName in result.by_connection) {
      const summary = result.by_connection[connName]
      for (const key in summary) {
        const stats = summary[key]
        if (stats.new > 0 || stats.update > 0) return false
      }
    }
    return true
  }
  
  // 单数据源模式
  if (result.summary) {
    for (const key in result.summary) {
      const stats = result.summary[key]
      if (stats.new > 0 || stats.update > 0) return false
    }
  }
  return true
})

// 获取统计项的标签
function getStatLabel(key) {
  const labels = {
    domains: '业务域',
    tables: '表配置',
    fields: '字段配置',
    enums: '枚举值',
    relationships: '表关系',
    rules: '全局规则'
  }
  return labels[key] || key
}

// 初始化
onMounted(() => {
  loadConnections()
})
</script>

<style scoped>
.metadata-manage {
  min-height: 100%;
  width: 100%;
}

.metadata-manage .el-card {
  width: 100%;
}

/* 选择器卡片 */
.selector-card {
  margin-bottom: 20px;
  border-radius: 8px;
}

.selector-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
}

.selector-left {
  display: flex;
  align-items: center;
  gap: 12px;
}

.selector-label {
  font-weight: 500;
  color: #606266;
  white-space: nowrap;
}

.selector-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.action-group {
  display: flex;
}

.selector-right .el-divider--vertical {
  height: 24px;
  margin: 0 4px;
}

.connection-select {
  width: 100%;
  max-width: 240px;
}

.conn-option {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

/* 空状态 */
.empty-state {
  background: white;
  border-radius: 8px;
  padding: 60px 20px;
  margin-top: 20px;
}

/* 内容卡片 */
.content-card {
  border-radius: 8px;
}

/* 树形布局 */
.tree-layout {
  min-height: 600px;
  width: 100%;
}

/* 树形面板 */
.tree-panel {
  border-right: 1px solid #e4e7ed;
  padding: 16px;
  height: 600px;
  overflow-y: auto;
  width: 100%;
}

.tree-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.tree-title {
  font-size: 14px;
  font-weight: 600;
  color: #303133;
}


.tree-node-wrapper {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding-right: 8px;
}

.tree-node {
  display: flex;
  align-items: center;
  gap: 8px;
  flex: 1;
  min-width: 0;
}

.tree-icon {
  font-size: 16px;
  color: #606266;
  flex-shrink: 0;
}

.tree-label {
  flex: 1;
  font-size: 14px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.label-disabled {
  color: #909399;
  text-decoration: line-through;
  opacity: 0.6;
}

.tree-node-actions {
  display: flex;
  gap: 4px;
  align-items: center;
  flex-shrink: 0;
}

.action-icon {
  font-size: 14px;
  color: #606266;
  cursor: pointer;
  padding: 2px;
  border-radius: 3px;
  transition: all 0.2s;
}

.action-icon:hover {
  background-color: #f0f0f0;
  color: #409eff;
}

.action-icon.danger:hover {
  color: #f56c6c;
}

/* 配置面板 */
.config-panel {
  padding: 16px;
  height: 600px;
  overflow-y: auto;
  width: 100%;
}

.config-content {
  animation: fadeIn 0.3s;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.config-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  padding-bottom: 12px;
  border-bottom: 2px solid #f0f2f5;
}

.config-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: #303133;
}

/* 折叠面板 */
.domain-collapse {
  margin-bottom: 20px;
}

/* 表卡片区域 */
.table-cards-section,
.field-list-section {
  margin-top: 20px;
}

.section-header {
  margin-bottom: 16px;
  padding-bottom: 8px;
  border-bottom: 2px solid #f0f2f5;
}

.section-header h4 {
  margin: 0;
  font-size: 16px;
  font-weight: 600;
  color: #303133;
}

/* 表卡片 */
.table-mini-card {
  cursor: pointer;
  margin-bottom: 16px;
  transition: all 0.3s;
}

.table-mini-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 4px 12px rgba(0,0,0,0.15);
}

.table-card-content {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.table-card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  color: #303133;
}

.table-card-name {
  font-size: 14px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.table-card-info {
  margin: 4px 0;
}

.table-card-actions {
  display: flex;
  justify-content: flex-end;
}

/* 滚动条美化 */
.tree-panel::-webkit-scrollbar,
.config-panel::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

.tree-panel::-webkit-scrollbar-thumb,
.config-panel::-webkit-scrollbar-thumb {
  background: #dcdfe6;
  border-radius: 3px;
}

.tree-panel::-webkit-scrollbar-thumb:hover,
.config-panel::-webkit-scrollbar-thumb:hover {
  background: #c0c4cc;
}

.tree-panel::-webkit-scrollbar-track,
.config-panel::-webkit-scrollbar-track {
  background: #f5f7fa;
}

/* 规则定义预览 */
.rule-definition-preview {
  font-size: 12px;
  color: #606266;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 280px;
}

/* 导入对话框样式 */
.import-mode-cards {
  display: flex;
  gap: 16px;
}

.mode-card {
  flex: 1;
  padding: 16px;
  border: 2px solid var(--el-border-color);
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
  position: relative;
  background: #fff;
}

.mode-card:hover {
  border-color: var(--el-color-primary-light-3);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
}

.mode-card.active {
  border-color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
}

.mode-icon {
  font-size: 24px;
  margin-bottom: 8px;
}

.mode-title {
  font-weight: 600;
  font-size: 15px;
  margin-bottom: 8px;
}

.mode-desc {
  font-size: 13px;
  color: #606266;
  line-height: 1.8;
}

.mode-tag {
  position: absolute;
  top: 8px;
  right: 8px;
}

.import-preview {
  background: var(--el-fill-color-light);
  padding: 16px;
  border-radius: 8px;
}

.preview-summary {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
  margin-bottom: 12px;
}

.summary-item {
  background: #fff;
  padding: 12px;
  border-radius: 6px;
  text-align: center;
}

.summary-label {
  font-size: 12px;
  color: #909399;
  margin-bottom: 6px;
}

.summary-stats {
  display: flex;
  justify-content: center;
  gap: 8px;
  font-size: 14px;
  font-weight: 500;
}

.stat-new { color: #67c23a; }
.stat-update { color: #e6a23c; }
.stat-skip { color: #909399; }
.stat-error { color: #f56c6c; }

.preview-legend {
  display: flex;
  justify-content: center;
  gap: 16px;
  font-size: 12px;
  color: #909399;
  padding: 8px 0;
  border-top: 1px dashed var(--el-border-color);
}

.preview-legend span {
  display: flex;
  align-items: center;
  gap: 4px;
}

.import-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.import-errors,
.import-warnings {
  background: #fff;
  padding: 12px;
  border-radius: 4px;
}

.upload-demo {
  width: 100%;
}

.upload-demo :deep(.el-upload-dragger) {
  width: 100%;
}

/* 对话框最大宽度和高度限制 */
:deep(.relationships-dialog .el-dialog),
:deep(.rules-dialog .el-dialog) {
  max-width: 1400px;
  height: 80vh;
  max-height: 800px;
  display: flex;
  flex-direction: column;
  margin-top: 5vh !important;
  margin-bottom: 5vh !important;
}

:deep(.relationships-dialog .el-dialog__body),
:deep(.rules-dialog .el-dialog__body) {
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  padding: 20px;
  min-height: 0;
}

/* 表关系对话框内容区域 */
.relationships-content {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.relationships-table-wrapper {
  flex: 1;
  overflow: hidden;
  min-height: 0;
}

.relationships-empty-wrapper {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 0;
}

/* 响应式布局 */
@media screen and (max-width: 1200px) {
  .tree-panel,
  .config-panel {
    height: 500px;
  }
}

@media screen and (max-width: 768px) {
  /* 选择器卡片 */
  .selector-content {
    flex-direction: column;
    gap: 12px;
  }
  
  .selector-left,
  .selector-right {
    width: 100%;
    flex-wrap: wrap;
    gap: 10px;
  }
  
  .selector-label {
    display: none;
  }
  
  .connection-select {
    flex: 1;
    min-width: 150px;
  }
  
  .action-group {
    width: 100%;
    display: flex;
  }
  
  .action-group .el-button {
    flex: 1;
  }
  
  .selector-right .el-button,
  .selector-right .el-dropdown {
    flex: 1;
  }
  
  /* 树形布局 */
  .tree-layout {
    min-height: auto;
  }
  
  .tree-panel,
  .config-panel {
    height: auto;
    min-height: 350px;
    max-height: 450px;
    border-right: none;
    border-bottom: 1px solid #e4e7ed;
    margin-bottom: 16px;
  }
  
  .tree-panel {
    padding-bottom: 16px;
  }
  
  .tree-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
  }
  
  .config-panel {
    padding-top: 16px;
    max-height: none;
  }
  
  /* 表单 */
  :deep(.el-form) {
    padding: 0;
  }
  
  :deep(.el-form-item__label) {
    font-size: 13px;
  }
  
  /* 对话框 */
  :deep(.el-dialog) {
    width: 95% !important;
    margin: 3vh auto !important;
  }
  
  :deep(.el-dialog__body) {
    max-height: 60vh;
    overflow-y: auto;
  }
}

@media screen and (max-width: 480px) {
  .tree-panel,
  .config-panel {
    min-height: 300px;
    max-height: 400px;
  }
  
  .tree-node .tree-label {
    font-size: 13px;
  }
  
  :deep(.el-tree-node__content) {
    padding: 4px 0;
  }
}
</style>
