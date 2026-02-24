<template>
  <div class="query-page">
    <!-- 顶部导航栏 -->
    <div class="top-bar">
      <div class="top-bar-content">
        <div class="logo-section">
          <div class="logo-icon">🤖</div>
          <div class="logo-text">
            <h1>{{ brandName }}</h1>
            <p>{{ brandTagline }}</p>
          </div>
        </div>
        <div class="top-bar-right">
          <!-- 用户登录状态 -->
          <div v-if="isLoggedIn" class="user-section">
            <span class="user-avatar">👤</span>
            <span class="user-name">{{ currentUser?.username }}</span>
            <button class="logout-btn" @click="handleLogout" title="退出登录">退出</button>
          </div>
          <button v-else class="login-btn" @click="openLoginDialog">
            <span>🔐</span> 登录
          </button>
          <a href="/admin" target="_blank" class="admin-btn">
            <span class="admin-icon">⚙️</span>
            {{ adminEntryLabel }}
          </a>
        </div>
      </div>
    </div>

    <!-- 主内容区 -->
    <div class="main-container">
      <!-- 介绍区域 -->
      <div class="hero-section">
        <h2 class="hero-title">{{ heroTitle }}</h2>
        <p class="hero-subtitle">{{ heroSubtitle }}</p>
      </div>

      <!-- 查询卡片 -->
      <div class="query-card">
        <div class="query-input-section">
          <div class="input-wrapper">
            <span class="input-icon">💬</span>
          <input
            type="text"
            class="query-input"
            v-model.trim="queryText"
              placeholder="例如：2025年武昌区的成交宗数"
            @keyup.enter="executeQuery"
          />
            <button 
              class="query-btn" 
              :class="{ 'loading': loading }" 
              :disabled="loading || !queryText" 
              @click="executeQuery"
            >
              <span v-if="loading" class="btn-spinner"></span>
              <span v-else class="btn-icon">🔍</span>
            {{ loading ? '查询中...' : '查询' }}
          </button>
        </div>

          <div class="options-row">
            <!-- 数据库连接选择器（可选，不选时自动检测） -->
            <div class="connection-selector">
              <span class="selector-label">💾 数据库：</span>
              <select v-model="selectedConnection" class="selector-input">
                <option value="">自动检测</option>
                <option v-for="conn in availableConnections" :key="conn.id" :value="conn.id">
                  {{ conn.name }}
                </option>
              </select>
            </div>
            
            <!-- 业务域选择器 -->
            <div class="domain-selector" v-if="availableDomains.length > 0">
              <span class="domain-label">🏢 业务域：</span>
              <select v-model="selectedDomain" class="domain-select">
                <option value="">自动检测</option>
                <option v-for="domain in availableDomains" :key="domain.id" :value="domain.id">
                  {{ domain.name }}
                </option>
              </select>
            </div>
            
            <label class="option-item">
            <input type="checkbox" v-model="explainOnly" />
              <span class="option-text">只生成 SQL</span>
          </label>
            <label class="option-item">
            <input type="checkbox" v-model="forceExecute" />
              <span class="option-text">强制执行</span>
          </label>
          </div>
        </div>

        <!-- 结果区域 -->
        <div class="result-area" v-if="error || loading || tableSelection || result">
          <!-- 错误提示 -->
          <div v-if="error" class="error-box">
            <div class="error-icon">❌</div>
            <div class="error-content">
              <h4>查询失败 [{{ error.code }}]</h4>
              <p>{{ error.message }}</p>
            </div>
      </div>

          <!-- 动态进度提示 -->
          <div v-else-if="showProgressFlow" class="progress-flow-wrapper">
            <div class="progress-timeline">
              <div
                v-for="(node, index) in visibleProgressSteps"
                :key="node.id || index"
                :class="['progress-item', node.status]"
                :style="{ animationDelay: `${index * 0.08}s` }"
              >
                <div class="progress-marker">
                  <span class="progress-item-dot" :class="node.status"></span>
                  <span
                    v-if="index !== visibleProgressSteps.length - 1"
                    class="progress-item-line"
                    :style="{ animationDelay: `${index * 0.08 + 0.12}s` }"
                  ></span>
                </div>
                <div
                  class="progress-item-content"
                  :style="{ animationDelay: `${index * 0.08 + 0.16}s` }"
                >
                  <div class="progress-item-title">
                    {{ node.step }}
                  </div>
                  <div class="progress-item-status">
                    {{ describeProgressStatus(node.status) }}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- 加载状态（兜底） -->
          <div v-else-if="loading" class="loading-box">
            <div class="loading-animation">
              <div class="dot"></div>
              <div class="dot"></div>
              <div class="dot"></div>
            </div>
            <p class="loading-text">AI 正在理解您的问题并生成 SQL...</p>
        </div>

          <!-- 旧的"请确认 AI 的理解"界面已移除，现在统一使用表选择确认卡 -->

          <!-- 表选择确认卡 -->
          <div v-if="!error && !loading && tableSelection" class="table-selection-box">
            <div class="table-selection-header">
              <span class="table-selection-icon">📊</span>
              <h3>请选择数据表</h3>
              <!-- 换一批 + 展开全部按钮 -->
              <div class="table-selection-header-actions">
                <!-- 上一批按钮 -->
                <button 
                  v-if="hasPrevBatch && !showAllAccessibleTables" 
                  class="btn-prev-batch" 
                  @click="prevTableBatch" 
                  title="上一批候选表"
                >
                  ← 上一批
                </button>
                <!-- 换一批/下一批按钮 -->
                <button 
                  v-if="hasNextBatch && !showAllAccessibleTables" 
                  class="btn-refresh-batch" 
                  @click="refreshTableBatch" 
                  title="下一批候选表"
                >
                  🔄 换一批 ({{ tableBatchIndex + 1 }}/{{ totalBatches }})
                </button>
                <!-- 批次信息（当只有一批或已到最后一批时） -->
                <span 
                  v-if="totalBatches > 1 && !hasNextBatch && !showAllAccessibleTables" 
                  class="batch-info"
                >
                  第 {{ tableBatchIndex + 1 }}/{{ totalBatches }} 批
                </span>
                <button 
                  v-if="!showAllAccessibleTables"
                  class="btn-expand-all" 
                  @click="openAllTablesModal" 
                  title="查看所有可用数据表"
                  :disabled="loadingAllTables"
                >
                  {{ loadingAllTables ? '加载中...' : '📋 展开全部' }}
                </button>
              </div>
            </div>
            <div class="table-selection-content">
              <p class="table-selection-message">{{ showAllAccessibleTables ? '以下是您可访问的所有数据表，请选择要查询的表：' : tableSelection.message }}</p>
              
              <!-- 确认原因提示 -->
              <div v-if="!showAllAccessibleTables && tableSelection.confirmation_reason" class="confirmation-reason-hint">
                <span class="hint-icon">💡</span>
                <span class="hint-text">{{ tableSelection.confirmation_reason }}</span>
              </div>
              
              <!-- 跨年查询提示 -->
              <div v-if="!showAllAccessibleTables && tableSelection.is_cross_year_query && tableSelection.cross_year_hint" class="cross-year-hint">
                <span class="hint-icon">📅</span>
                <span class="hint-text">{{ tableSelection.cross_year_hint }}</span>
              </div>
              
              <!-- 展开全部模式：搜索框 -->
              <div v-if="showAllAccessibleTables" class="all-tables-search">
                <input 
                  v-model="allTablesSearchQuery" 
                  type="text" 
                  placeholder="搜索表名或描述..." 
                  class="search-input"
                  @input="filterAllTables"
                />
                <span class="search-count">共 {{ filteredAllTables.length }} 张表</span>
              </div>
              
              <!-- LLM推荐模式 -->
              <div v-if="!showAllAccessibleTables" class="table-candidates">
                <div 
                  v-for="candidate in visibleTableCandidates" 
                  :key="candidate.table_id"
                  :class="['table-candidate', { 'selected': isTableSelected(candidate.table_id) }]"
                  @click="toggleTableSelection(candidate.table_id)"
                >
                  <div class="candidate-checkbox">
                    <span :class="['checkbox-box', { 'checked': isTableSelected(candidate.table_id) }]">
                      <span v-if="isTableSelected(candidate.table_id)" class="checkbox-tick">✓</span>
                    </span>
                  </div>
                  <div class="candidate-content">
                    <div class="candidate-header">
                      <span class="candidate-name">{{ candidate.table_name }}</span>
                      <span v-if="candidate.data_year" class="candidate-year">{{ candidate.data_year }}年</span>
                      <span class="candidate-confidence">置信度: {{ (candidate.confidence * 100).toFixed(0) }}%</span>
                    </div>
                    <p v-if="candidate.description" class="candidate-desc">{{ candidate.description }}</p>
                    <div class="candidate-fields" v-if="candidate.key_dimensions?.length || candidate.key_measures?.length">
                      <span class="field-label">关键字段：</span>
                      <span class="field-list">
                        {{ [...(candidate.key_dimensions || []), ...(candidate.key_measures || [])].slice(0, 6).join(', ') }}
                      </span>
                    </div>
                    <p v-if="candidate.reason" class="candidate-reason">💡 {{ candidate.reason }}</p>
                  </div>
                </div>
              </div>
              
              <!-- 展开全部模式：所有可访问表 -->
              <div v-else class="table-candidates all-tables-mode">
                <div 
                  v-for="table in filteredAllTables" 
                  :key="table.table_id"
                  :class="['table-candidate', { 'selected': isTableSelected(table.table_id) }]"
                  @click="toggleTableSelection(table.table_id)"
                >
                  <div class="candidate-checkbox">
                    <span :class="['checkbox-box', { 'checked': isTableSelected(table.table_id) }]">
                      <span v-if="isTableSelected(table.table_id)" class="checkbox-tick">✓</span>
                    </span>
                  </div>
                  <div class="candidate-content">
                    <div class="candidate-header">
                      <span class="candidate-name">{{ table.table_name }}</span>
                      <span v-if="table.data_year" class="candidate-year">{{ table.data_year }}年</span>
                      <span class="candidate-connection">{{ table.connection_name }}</span>
                    </div>
                    <p v-if="table.description" class="candidate-desc">{{ table.description }}</p>
                    <div class="candidate-fields" v-if="table.key_dimensions?.length || table.key_measures?.length">
                      <span class="field-label">关键字段：</span>
                      <span class="field-list">
                        {{ [...(table.key_dimensions || []), ...(table.key_measures || [])].slice(0, 6).join(', ') }}
                      </span>
                    </div>
                    <p v-if="table.domain_name" class="candidate-domain">📁 {{ table.domain_name }}</p>
                  </div>
                </div>
                <div v-if="filteredAllTables.length === 0" class="no-tables-found">
                  <span>未找到匹配的数据表</span>
                </div>
              </div>
            </div>
            <div class="table-selection-actions">
              <button class="btn-confirm" @click="confirmTableSelection" :disabled="selectedTableIds.length === 0">
                ✓ 确认选择 {{ selectedTableIds.length > 1 ? `(${selectedTableIds.length}张表)` : '' }}
              </button>
              <button v-if="showAllAccessibleTables" class="btn-back-to-recommend" @click="backToRecommendTables">
                ← 返回推荐
              </button>
              <button class="btn-cancel" @click="cancelTableSelection">
                ✕ 取消
              </button>
            </div>
          </div>

          <!-- 查询结果 -->
          <div v-if="!error && !tableSelection && result" class="result-box">
            <!-- 自然语言总结（支持 Markdown 渲染） -->
            <div v-if="summaryDisplay" class="summary-section">
              <div class="summary-icon">💡</div>
              <div class="summary-text markdown-body">
                <div v-html="renderMarkdown(summaryDisplay)"></div>
                <span v-if="narrativeStreaming" class="summary-streaming-dot">...</span>
              </div>
            </div>
            <div v-else-if="narrativeStreaming" class="summary-section">
              <div class="summary-icon">💡</div>
              <div class="summary-text">叙述生成中...</div>
            </div>

            <template v-if="canDisplayResult">
              <!-- 查询信息卡片 -->
              <div v-if="(result.meta?.process_explanation && result.meta.process_explanation.length > 0) || (result.meta?.derived_calculations && result.meta.derived_calculations.length > 0)" class="query-info-card">
                <!-- 查询过程说明 -->
                <div v-if="result.meta?.process_explanation && result.meta.process_explanation.length > 0" class="info-row">
                  <span class="info-label">查询过程：</span>
                  <span class="info-value">{{ result.meta.process_explanation.join(' → ') }}</span>
                </div>

                <!-- 指标计算说明 -->
                <div v-if="result.meta?.derived_calculations && result.meta.derived_calculations.length > 0" class="info-section">
                  <div class="info-label">计算说明：</div>
                  <div class="calc-list">
                    <div v-for="(calc, idx) in result.meta.derived_calculations" :key="idx" class="calc-item">
                      <strong class="calc-name">{{ calc.display_name }}</strong>：{{ calc.formula_detailed || calc.formula }}
                    </div>
                  </div>
                </div>
              </div>

              <!-- 数据表格 -->
              <div v-if="hasTable" class="data-section">
                <div class="data-header">
                  <h3>📋 查询结果</h3>
                  <div class="data-header-right">
                    <!-- 切换按钮 -->
                    <div v-if="canShowChart" class="view-toggle">
                      <button 
                        class="toggle-btn" 
                        :class="{ active: viewMode === 'table' }"
                        @click="viewMode = 'table'"
                      >
                        📊 表格
                      </button>
                      <button 
                        class="toggle-btn" 
                        :class="{ active: viewMode === 'chart' }"
                        @click="viewMode = 'chart'"
                      >
                        📈 图表
                      </button>
                    </div>
                    <span class="data-count">
                      共 {{ result.rows.length }} 条记录
                      <span v-if="result.meta?.total_time_ms || result.meta?.latency_ms" class="latency-info">
                        · 耗时 {{ formatDuration(result.meta?.total_time_ms || result.meta?.latency_ms) }}
                      </span>
                    </span>
                  </div>
                </div>
                
                <!-- 表格视图 -->
                <div v-show="viewMode === 'table'" class="table-container">
                  <table class="data-table">
                    <thead>
                      <tr>
                        <th v-for="(c, i) in result.columns" :key="i">{{ c.name }}</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="(row, ri) in result.rows" :key="ri">
                        <td 
                          v-for="(cell, ci) in row" 
                          :key="ci" 
                          v-html="formatCell(cell)"
                          :title="getCellTooltip(cell)"
                        ></td>
                      </tr>
                    </tbody>
                  </table>
                </div>
                
                <!-- 图表视图 -->
                <div v-show="viewMode === 'chart' && canShowChart" class="chart-view">
                  <!-- 图表类型选择器 -->
                  <div class="chart-type-selector">
                    <span class="selector-label">图表类型：</span>
                    <div class="chart-type-buttons">
                      <button
                        v-for="type in chartTypes"
                        :key="type.value"
                        class="chart-type-btn"
                        :class="{ active: currentChartType === type.value }"
                        @click="currentChartType = type.value"
                        :title="type.label"
                      >
                        <span class="type-icon">{{ type.icon }}</span>
                        <span class="type-label">{{ type.label.substring(2) }}</span>
                      </button>
                    </div>
                  </div>
                  <!-- 图表容器 -->
                  <div class="chart-container">
                    <v-chart :option="chartOption" autoresize style="height: 450px;" />
                  </div>
                </div>
              </div>

              <!-- SQL 代码 -->
              <div v-if="sqlStatements.length" class="sql-section">
                <div class="sql-header">
                  <span class="sql-label">📝 生成的 SQL</span>
                </div>
                <div
                  v-for="(sqlItem, idx) in sqlStatements"
                  :key="sqlItem.key || idx"
                  class="sql-block"
                >
                  <div class="sql-subheader">
                    <div class="sql-title">
                      <span class="sql-label-secondary">{{ sqlItem.title || `SQL 步骤 ${idx + 1}` }}</span>
                      <span v-if="sqlItem.isPrimary" class="sql-tag primary">主</span>
                      <span v-if="sqlItem.nodeId" class="sql-tag">节点 {{ sqlItem.nodeId }}</span>
                    </div>
                    <button class="copy-btn" @click="copySQL(sqlItem.sql)">复制</button>
                  </div>
                  <pre class="sql-code">{{ sqlItem.sql }}</pre>
                </div>
              </div>

              <!-- 空状态 -->
              <div v-else class="empty-result">
                <div class="empty-icon">📭</div>
                  <p v-if="result.meta?.explain_only">SQL 已生成，未执行（explain_only 模式）</p>
                  <p v-else>查询成功，但没有返回数据</p>
              </div>
            </template>
            <div v-else class="result-skeleton">
              <div class="skeleton-line large shimmer"></div>
              <div class="skeleton-line medium shimmer"></div>
              <div class="skeleton-line short shimmer"></div>
              <div class="skeleton-table">
                <div class="skeleton-row" v-for="n in 3" :key="`row-${n}`">
                  <span
                    v-for="m in 4"
                    :key="`cell-${n}-${m}`"
                    class="skeleton-cell shimmer"
                  ></span>
                </div>
              </div>
            </div>
              </div>
            </div>

        <!-- 初始空状态 -->
        <div v-if="!error && !loading && !tableSelection && !result" class="initial-state">
          <div class="initial-icon">📊</div>
          <h3>开始您的数据探索之旅</h3>
          <p>在上方输入框中输入您的问题，按回车或点击查询按钮</p>
          <div class="example-queries">
            <p class="example-label">💡 试试这些问题：</p>
            <div class="example-item" @click="queryText = '武汉市各行政区2025年成交地块总宗数、出让面积、总价、每亩单价和楼面地价分别是多少?'">
              "武汉市各行政区2025年成交地块总宗数、出让面积、总价、每亩单价和楼面地价分别是多少?"
            </div>
            <div class="example-item" @click="queryText = '2025年江夏区出让总价最高的工业用地是哪一宗?成交总价、每亩单价、楼面地价和竞得人分别是什么?'">
              "2025年江夏区出让总价最高的工业用地是哪一宗?成交总价、每亩单价、楼面地价和竞得人分别是什么?"
            </div>
            <div class="example-item" @click="queryText = '新增建设用地批复数据中，武汉市各行政区已批复用地的宗数及总用地、农用地、耕地的用地面积分别是多少?'">
              "新增建设用地批复数据中，武汉市各行政区已批复用地的宗数及总用地、农用地、耕地的用地面积分别是多少?"
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onBeforeUnmount } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import MarkdownIt from 'markdown-it'
import { tokenManager } from '@/utils/tokenManager'

// 初始化 Markdown 渲染器
const md = new MarkdownIt({
  html: false,
  breaks: true,
  linkify: true
})

// 渲染 Markdown 内容
function renderMarkdown(content) {
  if (!content) return ''
  return md.render(content)
}

const router = useRouter()
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, LineChart, PieChart } from 'echarts/charts'
import { formatDuration } from '@/utils/common'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent
} from 'echarts/components'
import VChart from 'vue-echarts'

// 注册 ECharts 组件
use([
  CanvasRenderer,
  BarChart,
  LineChart,
  PieChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent
])

const API_BASE = ''
const brandName = import.meta.env.VITE_BRAND_NAME || 'NL2SQL'
const brandTagline = import.meta.env.VITE_BRAND_TAGLINE || '智能问数系统'
const heroTitle = import.meta.env.VITE_HERO_TITLE || '用自然语言提问，AI 自动生成并执行 SQL'
const heroSubtitle = import.meta.env.VITE_HERO_SUBTITLE || '无需编写代码，像聊天一样查询数据'
const adminEntryLabel = import.meta.env.VITE_ADMIN_ENTRY_LABEL || '管理后台'
const wsUrl = new URL(`${API_BASE || ''}/api/query/stream`, window.location.origin)
wsUrl.protocol = wsUrl.protocol === 'https:' ? 'wss:' : 'ws:'
const QUERY_STREAM_URL = wsUrl.toString()

// ==================== 登录状态管理 ====================
const currentUser = ref(null)
const isLoggedIn = computed(() => !!currentUser.value)

// 初始化登录状态
const initLoginState = () => {
  // 首先检查 URL hash 中是否有 OIDC 回调的 token
  const hash = window.location.hash.substring(1)
  if (hash) {
    const params = new URLSearchParams(hash)
    const hashToken = params.get('token')
    const hashUserStr = params.get('user')
    
    if (hashToken) {
      localStorage.setItem('token', hashToken)
      if (hashUserStr) {
        try {
          const user = JSON.parse(decodeURIComponent(hashUserStr))
          localStorage.setItem('user', JSON.stringify(user))
          currentUser.value = user
        } catch (e) {
          console.error('解析用户信息失败:', e)
        }
      }
      // 清除 URL hash，避免敏感信息留在地址栏
      window.history.replaceState(null, '', window.location.pathname + window.location.search)
      ElMessage.success('登录成功')
      return
    }
  }
  
  // 从 localStorage 恢复登录状态
  const token = localStorage.getItem('token')
  const userStr = localStorage.getItem('user')
  if (token && userStr) {
    try {
      currentUser.value = JSON.parse(userStr)
    } catch (e) {
      console.error('解析用户信息失败:', e)
      localStorage.removeItem('token')
      localStorage.removeItem('user')
    }
  }
}

// 退出登录
const handleLogout = async () => {
  localStorage.removeItem('token')
  localStorage.removeItem('user')
  currentUser.value = null
  ElMessage.info('已退出登录')
  router.push('/login')
}

// 跳转到统一登录页面
const openLoginDialog = () => {
  router.push('/login')
}

// 页面加载时初始化登录状态
initLoginState()
// ==================== 登录状态管理 END ====================

const queryText = ref('')
const explainOnly = ref(false)
const forceExecute = ref(false)
const loading = ref(false)
const result = ref(null)
// confirm ref 已移除 - 旧的"请确认AI理解"功能已废弃
const tableSelection = ref(null)  // 表选择确认卡
const selectedTableId = ref(null)  // 🆕: 用户选中的表ID（向后兼容）
const selectedTableIds = ref([])  // 🆕: 多表选择的表ID列表
const showAllCandidates = ref(false)  // 🆕: 是否展开所有候选表（已弃用，保留向后兼容）
const tableBatchIndex = ref(0)  // 🆕: 当前换一批的索引
const originalQueryId = ref(null)  // 🆕: LLM表选择时的查询ID（用于追踪关联）
// 展开全部功能相关
const showAllAccessibleTables = ref(false)  // 是否显示所有可访问表模式
const allAccessibleTables = ref([])  // 所有可访问的表列表
const filteredAllTables = ref([])  // 过滤后的表列表
const allTablesSearchQuery = ref('')  // 搜索关键词
const loadingAllTables = ref(false)  // 加载状态
const error = ref(null)
const selectedDomain = ref('')  // 选中的业务域
const availableDomains = ref([])  // 可用的业务域列表
const selectedConnection = ref('')  // 🆕: 选中的数据库连接
const availableConnections = ref([])  // 🆕: 可用的数据库连接列表
const viewMode = ref('table')  // 'table' 或 'chart'
const currentChartType = ref('bar')  // 当前选择的图表类型
const wsRef = ref(null)
const narrativeBuffer = ref('')
const narrativeStreaming = ref(false)
const narrativePending = ref(false)
const progressSteps = ref([])

// 后端实际发送的步骤（按执行顺序）
// 注意：不是所有步骤都会执行，取决于查询流程
// - LLM表选择：仅在需要表选择时
// - 使用已选表：用户确认表选择后
// - 直接SQL生成：复杂查询跳过IR流程时
// - 权限过滤：有权限配置时
const KNOWN_PROGRESS_STEPS = [
  'LLM表选择',        // table_selection.py
  '使用已选表',       // routes.py - 用户确认表选择后
  'NL2IR解析',        // routes.py - 自然语言转IR
  '使用直接IR',       // routes.py - 用户直接提供IR
  '权限过滤',         // routes.py - 行级权限注入
  '表/列权限校验',    // permission_checker.py
  '直接SQL生成',      // routes.py - 复杂查询直接生成SQL
  'CoT规划与执行',    // routes.py - 复杂查询自动编排
  '确认检查',         // routes.py
  '缓存检查',         // routes.py
  'IR2SQL编译',       // routes.py
  '成本守护',         // routes.py
  'SQL执行',          // routes.py
  '结果格式化',       // routes.py
  '生成说明',         // routes.py
  '生成叙述'          // routes.py
]

// 只显示后端实际发送过的步骤
const visibleProgressSteps = computed(() =>
  progressSteps.value.filter((node) => node.status !== 'pending')
)

// 可用的图表类型
const chartTypes = [
  { value: 'bar', label: '📊 柱状图', icon: '📊' },
  { value: 'bar-stack', label: '📚 堆叠柱状图', icon: '📚' },
  { value: 'bar-horizontal', label: '📉 横向柱状图', icon: '📉' },
  { value: 'line', label: '📈 折线图', icon: '📈' },
  { value: 'area', label: '🌊 面积图', icon: '🌊' },
  { value: 'pie', label: '🥧 饼图', icon: '🥧' }
]

// ==================== 表选择相关 ====================
// 当前可见的候选表（分批展示/换一批逻辑）
const visibleTableCandidates = computed(() => {
  if (!tableSelection.value?.candidates) return []
  
  const candidates = tableSelection.value.candidates
  const pageSize = tableSelection.value.page_size || 5
  
  // 如果展开全部，显示所有
  if (showAllCandidates.value) {
    return candidates
  }
  
  // 分批展示：根据 tableBatchIndex 计算当前批次（不循环）
  const startIdx = tableBatchIndex.value * pageSize
  const endIdx = Math.min(startIdx + pageSize, candidates.length)
  
  // 如果 startIdx 超出范围，返回空（不应该发生，但安全起见）
  if (startIdx >= candidates.length) {
    return []
  }
  
  return candidates.slice(startIdx, endIdx)
})

// 计算总批次数
const totalBatches = computed(() => {
  if (!tableSelection.value?.candidates) return 1
  const candidates = tableSelection.value.candidates
  const pageSize = tableSelection.value.page_size || 5
  return Math.ceil(candidates.length / pageSize)
})

// 是否还有下一批（用于控制换一批按钮显示）
const hasNextBatch = computed(() => {
  return tableBatchIndex.value < totalBatches.value - 1
})

// 是否有上一批
const hasPrevBatch = computed(() => {
  return tableBatchIndex.value > 0
})

// 检查表是否被选中
function isTableSelected(tableId) {
  return selectedTableIds.value.includes(tableId)
}

// 切换表选择状态
function toggleTableSelection(tableId) {
  const idx = selectedTableIds.value.indexOf(tableId)
  if (idx === -1) {
    // 如果是多选模式，添加到列表
    if (tableSelection.value?.allow_multi_select) {
      selectedTableIds.value = [...selectedTableIds.value, tableId]
    } else {
      // 单选模式，替换
      selectedTableIds.value = [tableId]
    }
  } else {
    // 取消选择
    selectedTableIds.value = selectedTableIds.value.filter(id => id !== tableId)
  }
  // 同步旧的单选变量（向后兼容）
  selectedTableId.value = selectedTableIds.value[0] || null
}

// 换一批候选表（切换到下一批，不循环）
function refreshTableBatch() {
  if (!tableSelection.value?.candidates) return
  if (hasNextBatch.value) {
    tableBatchIndex.value++
  }
}

// 上一批候选表
function prevTableBatch() {
  if (hasPrevBatch.value) {
    tableBatchIndex.value--
  }
}

// 打开所有可访问表的展开模式
async function openAllTablesModal() {
  if (loadingAllTables.value) return
  
  loadingAllTables.value = true
  try {
    const token = localStorage.getItem('token')
    if (!token) {
      ElMessage.warning('请先登录')
      return
    }
    
    const resp = await fetch(`${API_BASE}/api/query/accessible-tables`, {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    })
    
    if (resp.status === 401) {
      console.warn('Token已过期，清理登录状态')
      localStorage.removeItem('token')
      localStorage.removeItem('user')
      currentUser.value = null
      ElMessage.warning('登录已过期，请重新登录')
      openLoginDialog()
      return
    }
    
    if (!resp.ok) {
      const errData = await resp.json().catch(() => ({}))
      throw new Error(errData.detail || '获取表列表失败')
    }
    
    const data = await resp.json()
    allAccessibleTables.value = data.tables || []
    filteredAllTables.value = allAccessibleTables.value
    allTablesSearchQuery.value = ''
    showAllAccessibleTables.value = true
    
  } catch (e) {
    console.error('获取所有可访问表失败:', e)
    ElMessage.error(e.message || '获取表列表失败')
  } finally {
    loadingAllTables.value = false
  }
}

// 过滤所有表
function filterAllTables() {
  const query = allTablesSearchQuery.value.trim().toLowerCase()
  if (!query) {
    filteredAllTables.value = allAccessibleTables.value
    return
  }
  
  filteredAllTables.value = allAccessibleTables.value.filter(table => {
    const nameMatch = (table.table_name || '').toLowerCase().includes(query)
    const descMatch = (table.description || '').toLowerCase().includes(query)
    const connMatch = (table.connection_name || '').toLowerCase().includes(query)
    const domainMatch = (table.domain_name || '').toLowerCase().includes(query)
    return nameMatch || descMatch || connMatch || domainMatch
  })
}

// 返回推荐模式
function backToRecommendTables() {
  showAllAccessibleTables.value = false
  allTablesSearchQuery.value = ''
}

const hasTable = computed(() => {
  return !!(result.value && result.value.columns && result.value.rows && result.value.rows.length > 0)
})

const canShowChart = computed(() => {
  if (!hasTable.value) {
    return false
  }
  const hint = result.value?.visualization_hint
  if (!hint) {
    return true
  }
  return hint !== 'table'
})

const sqlStatements = computed(() => {
  const statements = []
  const meta = result.value?.meta
  if (!meta) {
    return statements
  }

  if (Array.isArray(meta.dag_node_traces) && meta.dag_node_traces.length > 0) {
    meta.dag_node_traces.forEach((node, idx) => {
      if (!node?.sql) return
      statements.push({
        key: node.node_id || `dag-node-${idx}`,
        title: node.description || `SQL 步骤 ${idx + 1}`,
        sql: node.sql,
        nodeId: node.node_id,
        isPrimary: node.node_id && node.node_id === meta.dag_primary_node_id
      })
    })
    return statements
  }

  if (meta.sql) {
    statements.push({
      key: meta.dag_plan_id || 'primary-sql',
      title: meta.is_complex_dag ? '主查询 SQL' : '生成的 SQL',
      sql: meta.sql,
      nodeId: meta.dag_plan_id ? 'primary' : null,
      isPrimary: true
    })
  }

  if (Array.isArray(meta.dag_additional_outputs)) {
    meta.dag_additional_outputs.forEach((node, idx) => {
      if (node?.sql) {
        statements.push({
          key: node.node_id || `additional-${idx}`,
          title: node.description || `附加 SQL ${idx + 1}`,
          sql: node.sql,
          nodeId: node.node_id,
          isPrimary: false
        })
      }
    })
  }

  return statements
})

const summaryDisplay = computed(() => {
  if (narrativeBuffer.value) {
    return narrativeBuffer.value
  }
  return result.value?.summary || ''
})

const shouldDelayResultDisplay = computed(() => {
  if (!result.value) return false
  if (narrativeStreaming.value) return true
  return narrativePending.value
})

const canDisplayResult = computed(() => !!result.value && !shouldDelayResultDisplay.value)

const showProgressFlow = computed(
  () => loading.value && visibleProgressSteps.value.length > 0
)

function describeProgressStatus(status) {
  if (status === 'success') return '完成'
  if (status === 'error') return '异常'
  if (status === 'started') return '进行中'
  return '待执行'
}

function closeStream() {
  if (wsRef.value) {
    try {
      wsRef.value.close()
    } catch (err) {
      console.warn('关闭 WebSocket 失败', err)
    }
    wsRef.value = null
  }
}

function resetStreamState() {
  narrativeBuffer.value = ''
  narrativeStreaming.value = false
  narrativePending.value = false
  progressSteps.value = []
}

function initProgressSteps() {
  // 完全由后端驱动，初始化为空数组
  // 后端发送 progress 事件时会动态添加步骤
  progressSteps.value = []
}

function updateProgressNode(stepName, status) {
  const idx = progressSteps.value.findIndex((item) => item.step === stepName)
  if (idx === -1) {
    // 新步骤：根据 KNOWN_PROGRESS_STEPS 顺序插入到正确位置
    const knownIdx = KNOWN_PROGRESS_STEPS.indexOf(stepName)
    let insertIdx = progressSteps.value.length // 默认插入到末尾

    if (knownIdx !== -1) {
      // 找到第一个在当前步骤之后的已存在步骤
      for (let i = 0; i < progressSteps.value.length; i++) {
        const existingKnownIdx = KNOWN_PROGRESS_STEPS.indexOf(progressSteps.value[i].step)
        if (existingKnownIdx > knownIdx) {
          insertIdx = i
          break
        }
      }
    }

    const newStep = { id: `${stepName}-${Date.now()}`, step: stepName, status }
    const updated = [...progressSteps.value]
    updated.splice(insertIdx, 0, newStep)
    progressSteps.value = updated
    return
  }

  const updated = [...progressSteps.value]
  updated[idx] = { ...updated[idx], status }
  progressSteps.value = updated
}

function handleStreamMessage(message = {}) {
  const type = message.event
  const payload = message.payload || {}

  switch (type) {
    case 'progress':
      const stepName = payload.step || payload.description || '执行中'
      const status = payload.status || 'started'
      updateProgressNode(stepName, status)
      break
    case 'result':
      if (payload.result) {
        result.value = payload.result
        loading.value = false
        const hasSummary =
          typeof payload.result.summary === 'string' && payload.result.summary.trim().length > 0
        narrativePending.value = !hasSummary
      }
      progressSteps.value = []
      break
    case 'narrative':
      narrativePending.value = true
      if (payload.chunk) {
        narrativeBuffer.value += payload.chunk
      }
      narrativeStreaming.value = !payload.done
      if (payload.done && result.value) {
        narrativePending.value = false
        result.value.summary = narrativeBuffer.value
      }
      break
    case 'confirm':
      // 旧的"请确认AI理解"功能已废弃，这个消息类型不再使用
      // 保留 case 以避免未知消息警告，但不做处理
      console.warn('收到已废弃的 confirm 消息类型，已忽略')
      loading.value = false
      break
    case 'table_selection':
      // 🆕: 表选择确认
      tableSelection.value = payload.table_selection
      // 初始化选中状态：使用 LLM 推荐的表进行预选
      const firstCandidate = payload.table_selection?.candidates?.[0]
      const recommendedIds = payload.table_selection?.recommended_table_ids || []
      if (recommendedIds.length > 0) {
        // 使用 LLM 推荐的表进行预选
        selectedTableIds.value = recommendedIds
      } else if (firstCandidate) {
        // 降级：如果没有推荐表，选中第一个
        selectedTableIds.value = [firstCandidate.table_id]
      } else {
        selectedTableIds.value = []
      }
      selectedTableId.value = selectedTableIds.value[0] || null
      originalQueryId.value = payload.query_id || null
      showAllCandidates.value = false
      tableBatchIndex.value = 0
      loading.value = false
      progressSteps.value = []
      narrativePending.value = false
      break
    case 'auth_error':
      // 认证错误（如 JWT 过期），触发退出登录
      console.warn('WebSocket 认证错误', payload.error)
      localStorage.removeItem('token')
      localStorage.removeItem('user')
      currentUser.value = null
      ElMessage.error(payload.error?.message || '登录已过期，请重新登录')
      openLoginDialog()
      loading.value = false
      progressSteps.value = []
      break
    case 'error':
      error.value = payload.error || { code: 'STREAM_ERROR', message: '查询失败' }
      loading.value = false
      progressSteps.value = []
      narrativePending.value = false
      break
    case 'completed':
      if (payload.result) {
        if (result.value) {
          // 合并completed事件中的最新元信息（包含total_time_ms等）
          result.value = {
            ...result.value,
            meta: {
              ...(result.value.meta || {}),
              ...(payload.result.meta || {})
            },
            summary: result.value.summary || payload.result.summary,
            visualization_hint: result.value.visualization_hint ?? payload.result.visualization_hint
          }
        } else {
          result.value = payload.result
        }
      }
      loading.value = false
      progressSteps.value = []
      narrativePending.value = false
      break
    default:
      break
  }
}

const chartOption = computed(() => {
  if (!canShowChart.value) return null
  
  const vizType = currentChartType.value  // 使用用户选择的图表类型
  const columns = result.value.columns
  const allRows = result.value.rows
  
  // 🆕 过滤掉合计行（第一列包含"合计"的行）
  const rows = allRows.filter(row => {
    const firstCell = String(row[0] || '').trim()
    return !firstCell.includes('合计') && !firstCell.includes('**合计**')
  })
  
  if (rows.length === 0) return null
  
  // 找到第一个文本列作为维度，其他数值列作为指标
  const dimIndex = columns.findIndex(c => typeof rows[0]?.[columns.indexOf(c)] === 'string')
  const dimName = dimIndex >= 0 ? columns[dimIndex].name : '类别'
  const categories = rows.map(row => row[dimIndex] || '')
  
  // 数值列
  const valueColumns = columns.filter((c, idx) => {
    const val = rows[0]?.[idx]
    return typeof val === 'number' || !isNaN(parseFloat(val))
  })
  
  if (vizType === 'pie') {
    // 饼图：只显示第一个数值列
    const valueIdx = columns.indexOf(valueColumns[0])
    return {
      title: { text: '', left: 'center' },
      tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      legend: { orient: 'vertical', right: 10, top: 'center' },
      series: [{
        type: 'pie',
        radius: '60%',
        data: rows.map(row => ({
          name: row[dimIndex] || '',
          value: parseFloat(row[valueIdx]) || 0
        })),
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        }
      }]
    }
  } else if (vizType === 'bar' || vizType === 'bar-stack') {
    // 柱状图：支持多个指标
    return {
      title: { text: '', left: 'center' },
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      legend: { data: valueColumns.map(c => c.name), top: 30 },
      grid: { left: '3%', right: '4%', bottom: '3%', top: 80, containLabel: true },
      xAxis: {
        type: 'category',
        data: categories,
        axisLabel: { interval: 0, rotate: categories.length > 5 ? 30 : 0 }
      },
      yAxis: { type: 'value' },
      series: valueColumns.map(col => {
        const valueIdx = columns.indexOf(col)
        return {
          name: col.name,
          type: 'bar',
          stack: vizType === 'bar-stack' ? 'total' : undefined,  // 堆叠柱状图
          data: rows.map(row => parseFloat(row[valueIdx]) || 0)
        }
      })
    }
  } else if (vizType === 'line' || vizType === 'area') {
    // 折线图/面积图：支持多个指标
    return {
      title: { text: '', left: 'center' },
      tooltip: { trigger: 'axis' },
      legend: { data: valueColumns.map(c => c.name), top: 30 },
      grid: { left: '3%', right: '4%', bottom: '3%', top: 80, containLabel: true },
      xAxis: {
        type: 'category',
        data: categories,
        boundaryGap: false
      },
      yAxis: { type: 'value' },
      series: valueColumns.map(col => {
        const valueIdx = columns.indexOf(col)
        return {
          name: col.name,
          type: 'line',
          smooth: true,
          areaStyle: vizType === 'area' ? {} : undefined,  // 面积图
          data: rows.map(row => parseFloat(row[valueIdx]) || 0)
        }
      })
    }
  } else if (vizType === 'bar-horizontal') {
    // 横向柱状图
    return {
      title: { text: '', left: 'center' },
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      legend: { data: valueColumns.map(c => c.name), top: 30 },
      grid: { left: '15%', right: '4%', bottom: '3%', top: 80, containLabel: true },
      yAxis: {
        type: 'category',
        data: categories
      },
      xAxis: { type: 'value' },
      series: valueColumns.map(col => {
        const valueIdx = columns.indexOf(col)
        return {
          name: col.name,
          type: 'bar',
          data: rows.map(row => parseFloat(row[valueIdx]) || 0)
        }
      })
    }
  }
  
  return null
})

// cancelQuery 函数已废弃，原用于旧的"请确认AI理解"功能
// function cancelQuery() { ... }

// 🆕: 取消表选择
function cancelTableSelection() {
  tableSelection.value = null
  selectedTableId.value = null
  selectedTableIds.value = []
  showAllCandidates.value = false
  tableBatchIndex.value = 0
  // 重置展开全部状态
  showAllAccessibleTables.value = false
  allAccessibleTables.value = []
  filteredAllTables.value = []
  allTablesSearchQuery.value = ''
}

// 🆕: 确认表选择并继续查询
async function confirmTableSelection() {
  if (selectedTableIds.value.length === 0) {
    ElMessage.warning('请至少选择一张表')
    return
  }
  
  // 保存选中的表ID列表，清除确认卡
  const tableIds = [...selectedTableIds.value]
  tableSelection.value = null
  showAllCandidates.value = false
  tableBatchIndex.value = 0
  // 重置展开全部状态
  showAllAccessibleTables.value = false
  allAccessibleTables.value = []
  filteredAllTables.value = []
  allTablesSearchQuery.value = ''
  
  // 使用选中的表ID重新发起查询
  await executeQueryWithTables(tableIds)
}

// 🆕: 使用指定表ID执行查询（向后兼容单表）
async function executeQueryWithTable(tableId) {
  await executeQueryWithTables([tableId])
}

// 🆕: 使用指定表ID列表执行查询（支持多表）
async function executeQueryWithTables(tableIds) {
  closeStream()
  resetStreamState()
  initProgressSteps()
  
  error.value = null
  result.value = null
  tableSelection.value = null
  selectedTableIds.value = []
  
  const text = queryText.value.trim()
  if (!text) {
    error.value = { code: 'EMPTY', message: '请输入查询问题' }
    return
  }
  
  loading.value = true
  viewMode.value = 'table'
  
  // 确保 Token 有效（如果即将过期会自动刷新）
  const token = await tokenManager.ensureValidToken()
  const payload = {
    type: 'query',
    text,
    connection_id: selectedConnection.value || null,
    user_id: currentUser.value?.user_id || 'anonymous',
    role: currentUser.value?.role || 'viewer',
    explain_only: explainOnly.value,
    force_execute: forceExecute.value,
    domain_id: selectedDomain.value || null,
    // 支持单表和多表选择
    selected_table_id: tableIds.length === 1 ? tableIds[0] : null,
    selected_table_ids: tableIds.length > 0 ? tableIds : null,
    original_query_id: originalQueryId.value || null,
    token: token || null
  }
  
  try {
    const socket = new WebSocket(QUERY_STREAM_URL)
    wsRef.value = socket

    socket.onopen = () => {
      socket.send(JSON.stringify(payload))
    }

    socket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data)
        handleStreamMessage(message)
      } catch (err) {
        console.error('解析查询流事件失败', err)
      }
    }

    socket.onerror = () => {
      loading.value = false
      error.value = { code: 'WS_ERROR', message: '实时连接异常，请稍后重试' }
    }

    socket.onclose = () => {
      wsRef.value = null
      narrativeStreaming.value = false
    }
  } catch (e) {
    loading.value = false
    error.value = { code: 'WS_INIT_ERROR', message: `无法建立实时连接：${e.message}` }
  }
}

// applySuggestion 和 confirmAndExecute 函数已废弃
// 原用于旧的"请确认AI理解"功能，该功能已移除
// 现在统一使用表选择确认卡（confirmTableSelection）进行用户确认

function copySQL(sql) {
  if (!sql) return

  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(sql)
      .then(() => {
        ElMessage.success('SQL 已复制到剪贴板')
      })
      .catch(() => {
        ElMessage.error('复制失败，请手动复制')
      })
  } else {
    // Fallback for insecure contexts (like HTTP)
    const textArea = document.createElement('textarea')
    textArea.value = sql
    textArea.style.position = 'absolute'
    textArea.style.left = '-9999px'
    document.body.appendChild(textArea)
    textArea.select()
    try {
      document.execCommand('copy')
      ElMessage.success('SQL 已复制到剪贴板')
    } catch (err) {
      ElMessage.error('复制失败，请手动复制')
    }
    document.body.removeChild(textArea)
  }
}

function formatCell(value) {
  if (value === null || value === undefined) {
    return '<span style="color: #999;">NULL</span>'
  }
  if (typeof value === 'number') {
    return String(value)
  }
  const div = document.createElement('div')
  div.textContent = String(value)
  let escaped = div.innerHTML
  escaped = escaped.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
  return escaped
}

function getCellTooltip(value) {
  // 为长文本或复杂内容提供工具提示
  if (value === null || value === undefined) {
    return ''
  }
  const strValue = String(value)
  // 只有长度超过30个字符的文本才显示tooltip
  if (strValue.length > 30) {
    return strValue
  }
  return ''
}

async function executeQuery() {
  closeStream()
  resetStreamState()
  initProgressSteps()

  error.value = null
  result.value = null
  tableSelection.value = null  // 清空表选择

  const text = queryText.value.trim()
  if (!text) {
    error.value = { code: 'EMPTY', message: '请输入查询问题' }
    return
  }

  if (!isLoggedIn.value) {
    error.value = { code: 'UNAUTHENTICATED', message: '请先登录后再进行查询' }
    ElMessage.warning('请先登录后再进行查询')
    openLoginDialog()
    return
  }

  // 数据库连接现在是可选的，不选时后端会自动检测
  // if (!selectedConnection.value) {
  //   error.value = { code: 'NO_CONNECTION', message: '请先选择数据库连接' }
  //   ElMessage.warning('请先选择数据库连接')
  //   return
  // }

  loading.value = true

  // 确保 Token 有效（如果即将过期会自动刷新）
  const token = await tokenManager.ensureValidToken()
  const payload = {
    text,
    connection_id: selectedConnection.value || null,  // 可选，为空时后端自动检测
    user_id: currentUser.value?.user_id || 'anonymous',
    role: currentUser.value?.role || 'viewer',
    explain_only: explainOnly.value,
    force_execute: forceExecute.value,
    domain_id: selectedDomain.value || null,
    // 传递token用于权限验证
    token: token || null
  }

  try {
    const socket = new WebSocket(QUERY_STREAM_URL)
    wsRef.value = socket

    socket.onopen = () => {
      socket.send(JSON.stringify(payload))
    }

    socket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data)
        handleStreamMessage(message)
      } catch (err) {
        console.error('解析查询流事件失败', err)
      }
    }

    socket.onerror = () => {
      loading.value = false
      error.value = { code: 'WS_ERROR', message: '实时连接异常，请稍后重试' }
    }

    socket.onclose = () => {
      wsRef.value = null
      narrativeStreaming.value = false
    }
  } catch (e) {
    loading.value = false
    error.value = { code: 'WS_INIT_ERROR', message: `无法建立实时连接：${e.message}` }
  }
}

// 🆕: 加载数据库连接列表（根据用户权限过滤）
async function loadConnections(options = {}) {
  const { showLoginNotice = false } = options
  try {
    console.log('开始加载数据库连接列表...')
    
    const token = localStorage.getItem('token')
    
    if (!token || !isLoggedIn.value) {
      console.warn('当前未登录，无法加载数据库连接')
      availableConnections.value = []
      selectedConnection.value = ''
      if (showLoginNotice) {
        ElMessage.warning('请先登录后再选择数据库连接')
        openLoginDialog()  // 自动弹出登录框
      }
      return
    }
    
      console.log('用户已登录，获取权限过滤后的连接列表')
    const resp = await fetch(`${API_BASE}/api/query/connections`, {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      })
      
    if (resp.status === 401) {
      console.warn('Token已过期，清理登录状态')
        localStorage.removeItem('token')
      localStorage.removeItem('user')
        currentUser.value = null
      availableConnections.value = []
      selectedConnection.value = ''
      ElMessage.warning('登录已过期，请重新登录')
      openLoginDialog()
      return
    }

    const raw = await resp.text()
    if (!resp.ok) {
      let errMsg = '加载数据库连接失败'
      if (raw) {
        try {
          const parsed = JSON.parse(raw)
          errMsg = parsed.detail || parsed.message || errMsg
        } catch {
          errMsg = raw
        }
      }
      throw new Error(errMsg)
    }

    const data = raw ? JSON.parse(raw) : {}
    const connections = (data.connections && Array.isArray(data.connections))
      ? data.connections.map(conn => ({
          id: conn.connection_id,
          name: conn.connection_name || conn.db_type || '未命名连接',
          can_query: conn.can_query,
          can_export: conn.can_export
        }))
      : []
    
    availableConnections.value = connections
    console.log('映射后的数据库连接:', availableConnections.value)
    
    if (selectedConnection.value && !availableConnections.value.some(conn => conn.id === selectedConnection.value)) {
      selectedConnection.value = ''
    }
    
    // 如果只有一个连接，自动选择
    if (availableConnections.value.length === 1) {
      selectedConnection.value = availableConnections.value[0].id
      console.log('自动选择唯一的数据库连接:', selectedConnection.value)
    } else if (availableConnections.value.length === 0) {
      console.warn('⚠️ 无可用的数据库连接')
      ElMessage.warning({
        message: '您没有任何数据源的访问权限，请联系管理员',
        duration: 5000,
        showClose: true
      })
    }
  } catch (e) {
    console.error('加载数据库连接列表失败', e)
    ElMessage.error(e.message || '加载数据库连接失败，请刷新页面重试')
  }
}

// 加载业务域列表（按所选连接过滤）
async function loadDomains(options = {}) {
  const { showLoginNotice = false } = options
  try {
    if (!isLoggedIn.value) {
      availableDomains.value = []
      selectedDomain.value = ''
      if (showLoginNotice) {
        ElMessage.warning('请先登录后再选择业务域')
        openLoginDialog()  // 自动弹出登录框
      }
      return
    }

    if (!selectedConnection.value) {
      console.warn('当前未选择数据库连接，业务域列表保持为空')
      availableDomains.value = []
      selectedDomain.value = ''
      return
    }

    console.log('开始加载业务域列表...')
    const token = localStorage.getItem('token')
    const params = `?connection_id=${selectedConnection.value}`
    const resp = await fetch(`${API_BASE}/api/domains${params}`, {
      headers: token ? { 'Authorization': `Bearer ${token}` } : {}
    })
    console.log('API响应状态:', resp.status)

    if (resp.status === 401) {
      console.warn('Token已过期，清理登录状态')
      localStorage.removeItem('token')
      localStorage.removeItem('user')
      currentUser.value = null
      ElMessage.warning('登录已过期，请重新登录')
      openLoginDialog()
      return
    }

    if (!resp.ok) {
      const errBody = await resp.text()
      throw new Error(errBody || '加载业务域失败')
    }

    const data = await resp.json()
    console.log('获取到的业务域:', data)
    const domains = data.items || (Array.isArray(data) ? data : [])
    availableDomains.value = domains.map(d => ({
      id: d.domain_id,
      name: d.domain_name || d.display_name
    }))
    selectedDomain.value = ''
    console.log('映射后的业务域:', availableDomains.value)
  } catch (e) {
    console.error('加载业务域列表失败', e)
    ElMessage.error(e.message || '加载业务域列表失败，请重试')
  }
}

// 页面加载时获取数据库连接列表
console.log('Query.vue 组件加载，准备加载数据...')
loadConnections({ showLoginNotice: true })  // 🆕: 优先加载数据库连接

// 当选择的数据库连接变化时，刷新业务域列表并重置选中域
watch(selectedConnection, () => {
  selectedDomain.value = ''
  if (selectedConnection.value) {
    loadDomains()
  } else {
    availableDomains.value = []
  }
})

// 监听结果变化，每次有新结果时重置为表格视图
watch(result, (newResult) => {
  if (newResult) {
    viewMode.value = 'table'
    // 根据后端推荐设置默认图表类型
    if (newResult.visualization_hint) {
      currentChartType.value = newResult.visualization_hint === 'table' ? 'bar' : newResult.visualization_hint
    } else {
      currentChartType.value = 'bar'
    }
  }
})

onBeforeUnmount(() => {
  closeStream()
})
</script>

<style scoped>
/* ===== 全局样式 ===== */
.query-page {
  min-height: 100vh;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  padding-bottom: 60px;
}

/* ===== 顶部导航栏 ===== */
.top-bar {
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid rgba(255, 255, 255, 0.2);
  padding: 16px 0;
}

.top-bar-content {
  max-width: 95%;
  margin: 0 auto;
  padding: 0 20px;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.logo-section {
  display: flex;
  align-items: center;
  gap: 12px;
}

.logo-icon {
  font-size: 32px;
  animation: float 3s ease-in-out infinite;
}

@keyframes float {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(-8px); }
}

.logo-text h1 {
  margin: 0;
  font-size: 24px;
  font-weight: 700;
  color: white;
  letter-spacing: 1px;
}

.logo-text p {
  margin: 0;
  font-size: 12px;
  color: rgba(255, 255, 255, 0.8);
  margin-top: 2px;
}

.admin-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 20px;
  background: rgba(255, 255, 255, 0.2);
  border: 1px solid rgba(255, 255, 255, 0.3);
  border-radius: 20px;
  color: white;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.3s;
  backdrop-filter: blur(10px);
  text-decoration: none;
}

.admin-btn:hover {
  background: rgba(255, 255, 255, 0.3);
  transform: translateY(-2px);
}

.admin-icon {
  font-size: 16px;
}

/* ===== 顶栏右侧区域 ===== */
.top-bar-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.user-section {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  background: rgba(255, 255, 255, 0.15);
  border-radius: 20px;
  color: white;
}

.user-avatar {
  font-size: 16px;
}

.user-name {
  font-size: 14px;
  font-weight: 500;
}

.logout-btn {
  margin-left: 8px;
  padding: 4px 10px;
  background: rgba(255, 255, 255, 0.2);
  border: none;
  border-radius: 12px;
  color: white;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.3s;
}

.logout-btn:hover {
  background: rgba(255, 100, 100, 0.4);
}

.login-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: rgba(255, 255, 255, 0.2);
  border: 1px solid rgba(255, 255, 255, 0.3);
  border-radius: 20px;
  color: white;
  font-size: 14px;
  cursor: pointer;
  transition: all 0.3s;
}

.login-btn:hover {
  background: rgba(255, 255, 255, 0.3);
}

/* ===== 主容器 ===== */
.main-container {
  max-width: 95%;
  margin: 0 auto;
  padding: 40px 20px;
}

/* ===== 介绍区域 ===== */
.hero-section {
  text-align: center;
  margin-bottom: 40px;
  color: white;
}

.hero-title {
  font-size: 32px;
  font-weight: 700;
  margin: 0 0 12px 0;
  text-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
}

.hero-subtitle {
  font-size: 18px;
  margin: 0;
  color: rgba(255, 255, 255, 0.9);
}

/* ===== 查询卡片 ===== */
.query-card {
  background: white;
  border-radius: 16px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
  overflow: hidden;
}

.query-input-section {
  padding: 32px 40px;
  background: linear-gradient(to bottom, #f8f9fa, #ffffff);
  border-bottom: 1px solid #e5e7eb;
}

.input-wrapper {
  display: flex;
  align-items: center;
  gap: 12px;
  background: white;
  border: 2px solid #e1e4e8;
  border-radius: 16px;
  padding: 6px;
  transition: all 0.3s;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
}

.input-wrapper:focus-within {
  border-color: #667eea;
  box-shadow: 0 4px 16px rgba(102, 126, 234, 0.2);
}

.input-icon {
  font-size: 24px;
  margin-left: 12px;
}

.query-input {
  flex: 1;
  border: none;
  outline: none;
  font-size: 17px;
  padding: 14px 8px;
  background: transparent;
}

.query-input::placeholder {
  color: #a0a0a0;
}

.query-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 14px 36px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  border: none;
  border-radius: 12px;
  font-size: 17px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.3s;
  white-space: nowrap;
}

.query-btn:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4);
}

.query-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.btn-icon {
  font-size: 18px;
}

.btn-spinner {
  width: 16px;
  height: 16px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-top-color: white;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.options-row {
  display: flex;
  gap: 24px;
  margin-top: 16px;
}

.option-item {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  user-select: none;
}

.option-item input[type="checkbox"] {
  width: 18px;
  height: 18px;
  cursor: pointer;
}

.option-text {
  font-size: 14px;
  color: #586069;
}

/* ===== 数据库连接选择器 ===== */
.connection-selector {
  display: flex;
  align-items: center;
  gap: 8px;
}

.selector-label {
  font-size: 14px;
  color: #586069;
  font-weight: 600;
}

.selector-input {
  padding: 6px 12px;
  border: 2px solid #667eea;
  border-radius: 8px;
  font-size: 14px;
  color: #24292e;
  background: white;
  cursor: pointer;
  transition: all 0.3s;
  min-width: 200px;
}

.selector-input:hover {
  border-color: #764ba2;
  box-shadow: 0 2px 8px rgba(102, 126, 234, 0.2);
}

.selector-input:focus {
  outline: none;
  border-color: #764ba2;
  box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.15);
}

/* ===== 业务域选择器 ===== */
.domain-selector {
  display: flex;
  align-items: center;
  gap: 8px;
}

.domain-label {
  font-size: 14px;
  color: #586069;
  font-weight: 500;
}

.domain-select {
  padding: 6px 12px;
  border: 2px solid #e1e4e8;
  border-radius: 8px;
  font-size: 14px;
  color: #24292e;
  background: white;
  cursor: pointer;
  transition: all 0.3s;
}

.domain-select:hover {
  border-color: #667eea;
}

.domain-select:focus {
  outline: none;
  border-color: #667eea;
  box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
}

/* ===== 结果区域 ===== */
.result-area {
  padding: 32px;
}

.progress-flow-wrapper {
  margin: 20px 0 24px;
  padding: 18px 24px;
  border-radius: 14px;
  background: #f8fafc;
  border: 1px solid #e2e8f0;
}

.progress-timeline {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.progress-item {
  display: flex;
  gap: 14px;
  position: relative;
  opacity: 0;
  transform: translateY(8px);
  animation: progressFadeIn 0.45s ease forwards;
}

.progress-marker {
  position: relative;
  width: 16px;
  display: flex;
  justify-content: center;
}

.progress-item-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: #cbd5f5;
  margin-top: 4px;
  transition: background 0.3s, box-shadow 0.3s;
}

.progress-item-dot.started {
  background: #6366f1;
  box-shadow: 0 0 0 4px rgba(99, 102, 241, 0.15);
  animation: markerPulse 1.6s ease-in-out infinite;
}

.progress-item-dot.success {
  background: #10b981;
  box-shadow: 0 0 0 4px rgba(16, 185, 129, 0.15);
}

.progress-item-dot.error {
  background: #ef4444;
  box-shadow: 0 0 0 4px rgba(239, 68, 68, 0.15);
}

.progress-item-line {
  position: absolute;
  top: 20px;
  bottom: -14px;
  width: 2px;
  background: #e2e8f0;
  transform-origin: top;
  transform: scaleY(0);
  animation: lineGrow 0.35s ease forwards;
}

.progress-item-content {
  flex: 1;
  padding: 12px 16px;
  border-radius: 12px;
  border: 1px dashed rgba(148, 163, 184, 0.4);
  background: rgba(255, 255, 255, 0.85);
  box-shadow: 0 6px 20px rgba(15, 23, 42, 0.03);
  position: relative;
  overflow: hidden;
  transition: border-color 0.3s, background 0.3s, box-shadow 0.3s;
  opacity: 0;
  transform: translateY(6px);
  animation: textReveal 0.4s ease forwards;
}

.progress-item:last-child .progress-item-content {
  margin-bottom: 0;
}

.progress-item-content::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(
    110deg,
    rgba(255, 255, 255, 0) 0%,
    rgba(255, 255, 255, 0.7) 45%,
    rgba(255, 255, 255, 0) 90%
  );
  transform: translateX(-120%);
  opacity: 0;
}

.progress-item-title {
  font-size: 14px;
  font-weight: 600;
  color: #1f2937;
}

.progress-item.started .progress-item-title {
  color: #4338ca;
  animation: textGlowPrimary 1.8s ease-in-out infinite;
}

.progress-item.started .progress-item-content {
  border-color: rgba(99, 102, 241, 0.45);
  background: rgba(224, 231, 255, 0.65);
  box-shadow: 0 10px 28px rgba(99, 102, 241, 0.18);
}

.progress-item.started .progress-item-content::after {
  animation: highlightSweep 1.4s linear infinite;
  opacity: 1;
}

.progress-item.success .progress-item-title {
  color: #047857;
}

.progress-item.success .progress-item-content {
  border-color: rgba(16, 185, 129, 0.4);
  background: rgba(236, 253, 245, 0.85);
  box-shadow: 0 10px 26px rgba(16, 185, 129, 0.18);
}

.progress-item.success .progress-item-content::after {
  animation: none;
  opacity: 0;
}

.progress-item.error .progress-item-title {
  color: #b91c1c;
  animation: textGlowError 1.8s ease-in-out infinite;
}

.progress-item.error .progress-item-content {
  border-color: rgba(239, 68, 68, 0.45);
  background: rgba(254, 242, 242, 0.9);
  box-shadow: 0 10px 26px rgba(239, 68, 68, 0.16);
}

.progress-item.error .progress-item-content::after {
  animation: highlightSweep 1.6s linear infinite;
  opacity: 1;
}

.progress-item-status {
  font-size: 12px;
  margin-top: 2px;
  color: #64748b;
}

.progress-item.started .progress-item-status {
  color: #6366f1;
  animation: textGlowPrimary 1.8s ease-in-out infinite;
}

.progress-item.success .progress-item-status {
  color: #059669;
}

.progress-item.error .progress-item-status {
  color: #dc2626;
  animation: textGlowError 1.8s ease-in-out infinite;
}

.progress-item.started .progress-item-title,
.progress-item.started .progress-item-status,
.progress-item.success .progress-item-title,
.progress-item.success .progress-item-status,
.progress-item.error .progress-item-title,
.progress-item.error .progress-item-status {
  transition: color 0.3s;
}

.summary-streaming-dot {
  margin-left: 8px;
  animation: blink 1s infinite;
  color: #a855f7;
}

@keyframes blink {
  0%, 100% { opacity: 0.3; }
  50% { opacity: 1; }
}

@keyframes textReveal {
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@keyframes textGlowPrimary {
  0%, 100% {
    text-shadow: 0 0 0 rgba(99, 102, 241, 0);
  }
  50% {
    text-shadow: 0 0 10px rgba(99, 102, 241, 0.35);
  }
}

@keyframes textGlowSuccess {
  0%, 100% {
    text-shadow: 0 0 0 rgba(34, 197, 94, 0);
  }
  50% {
    text-shadow: 0 0 10px rgba(34, 197, 94, 0.35);
  }
}

@keyframes textGlowError {
  0%, 100% {
    text-shadow: 0 0 0 rgba(239, 68, 68, 0);
  }
  50% {
    text-shadow: 0 0 10px rgba(239, 68, 68, 0.35);
  }
}

@keyframes highlightSweep {
  0% {
    transform: translateX(-120%);
    opacity: 0;
  }
  20% {
    opacity: 0.9;
  }
  100% {
    transform: translateX(120%);
    opacity: 0;
  }
}

@keyframes progressFadeIn {
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@keyframes markerPulse {
  0% {
    box-shadow: 0 0 0 0 rgba(99, 102, 241, 0.25);
  }
  70% {
    box-shadow: 0 0 0 8px rgba(99, 102, 241, 0);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(99, 102, 241, 0);
  }
}

@keyframes lineGrow {
  to {
    transform: scaleY(1);
  }
}

.result-skeleton {
  margin-top: 20px;
  padding: 28px 24px;
  border-radius: 16px;
  background: #ffffff;
  border: 1px solid rgba(148, 163, 184, 0.3);
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.6);
}

.skeleton-line {
  height: 14px;
  border-radius: 999px;
  background: #e5e7eb;
  margin-bottom: 12px;
}

.skeleton-line.large {
  width: 45%;
}

.skeleton-line.medium {
  width: 60%;
}

.skeleton-line.short {
  width: 25%;
}

.skeleton-table {
  margin-top: 20px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.skeleton-row {
  display: flex;
  gap: 12px;
}

.skeleton-cell {
  flex: 1;
  height: 18px;
  border-radius: 10px;
  background: #e5e7eb;
}

.shimmer {
  position: relative;
  overflow: hidden;
}

.shimmer::after {
  content: '';
  position: absolute;
  top: 0;
  left: -150%;
  width: 50%;
  height: 100%;
  background: linear-gradient(
    120deg,
    rgba(255, 255, 255, 0) 0%,
    rgba(255, 255, 255, 0.7) 50%,
    rgba(255, 255, 255, 0) 100%
  );
  animation: shimmer 1.5s infinite;
}

@keyframes shimmer {
  100% {
    transform: translateX(300%);
  }
}

/* ===== 错误提示 ===== */
.error-box {
  display: flex;
  gap: 16px;
  padding: 20px;
  background: #fef2f2;
  border: 2px solid #fecaca;
  border-radius: 12px;
  color: #dc2626;
}

.error-icon {
  font-size: 32px;
  flex-shrink: 0;
}

.error-content h4 {
  margin: 0 0 8px 0;
  font-size: 16px;
  font-weight: 600;
}

.error-content p {
  margin: 0;
  font-size: 14px;
  line-height: 1.6;
}

/* ===== 加载状态 ===== */
.loading-box {
  text-align: center;
  padding: 60px 20px;
}

.loading-animation {
  display: flex;
  justify-content: center;
  gap: 8px;
  margin-bottom: 20px;
}

.dot {
  width: 12px;
  height: 12px;
  background: #667eea;
  border-radius: 50%;
  animation: bounce 1.4s infinite ease-in-out both;
}

.dot:nth-child(1) {
  animation-delay: -0.32s;
}

.dot:nth-child(2) {
  animation-delay: -0.16s;
}

@keyframes bounce {
  0%, 80%, 100% {
    transform: scale(0);
  }
  40% {
    transform: scale(1);
  }
}

.loading-text {
  color: #586069;
  font-size: 16px;
  margin: 0;
}

/* 旧的"请确认AI理解"确认提示样式已移除 */
/* 现在统一使用表选择确认卡样式 */

.btn-confirm,
.btn-cancel {
  flex: 1;
  padding: 12px 24px;
  border: none;
  border-radius: 10px;
  font-size: 15px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.3s;
}

.btn-confirm {
  background: linear-gradient(135deg, #10b981 0%, #059669 100%);
  color: white;
}

.btn-confirm:hover {
  transform: translateY(-2px);
  box-shadow: 0 6px 16px rgba(16, 185, 129, 0.3);
}

.btn-cancel {
  background: white;
  color: #6b7280;
  border: 2px solid #e5e7eb;
}

.btn-cancel:hover {
  background: #f9fafb;
}

/* ===== 表选择确认卡 ===== */
.table-selection-box {
  background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
  border: 2px solid #0ea5e9;
  border-radius: 16px;
  padding: 24px;
  animation: fadeIn 0.3s ease;
}

.table-selection-header {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
}

.table-selection-icon {
  font-size: 28px;
}

.table-selection-header h3 {
  margin: 0;
  font-size: 20px;
  font-weight: 600;
  color: #0c4a6e;
}

.table-selection-message {
  color: #0369a1;
  font-size: 14px;
  margin-bottom: 16px;
}

.table-candidates {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.table-candidate {
  display: flex;
  gap: 12px;
  padding: 16px;
  background: white;
  border: 2px solid #e0e7ff;
  border-radius: 12px;
  cursor: pointer;
  transition: all 0.2s;
}

.table-candidate:hover {
  border-color: #6366f1;
  box-shadow: 0 4px 12px rgba(99, 102, 241, 0.15);
}

.table-candidate.selected {
  border-color: #0284c7;
  border-width: 3px;
  background: #e0f2fe;
  box-shadow: 0 4px 16px rgba(2, 132, 199, 0.3);
}

.candidate-radio {
  display: flex;
  align-items: flex-start;
  padding-top: 4px;
}

.radio-dot {
  width: 20px;
  height: 20px;
  border-radius: 50%;
  border: 2px solid #cbd5e1;
  background: white;
  transition: all 0.2s;
}

.radio-dot.checked {
  border-color: #0ea5e9;
  background: #0ea5e9;
  box-shadow: inset 0 0 0 4px white;
}

.candidate-content {
  flex: 1;
}

.candidate-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}

.candidate-name {
  font-size: 16px;
  font-weight: 600;
  color: #1e293b;
}

.candidate-confidence {
  font-size: 13px;
  color: #64748b;
  background: #f1f5f9;
  padding: 4px 8px;
  border-radius: 6px;
}

.candidate-desc {
  font-size: 14px;
  color: #475569;
  margin: 0 0 8px 0;
}

.candidate-fields {
  font-size: 13px;
  color: #64748b;
  margin-bottom: 8px;
}

.field-label {
  font-weight: 500;
}

.field-list {
  color: #0369a1;
}

.candidate-reason {
  font-size: 13px;
  color: #059669;
  margin: 0;
  padding: 8px 12px;
  background: #ecfdf5;
  border-radius: 8px;
}

.table-selection-actions {
  display: flex;
  gap: 12px;
  margin-top: 20px;
}

.table-selection-content {
  margin-bottom: 16px;
}

/* 表选择头部操作区 */
.table-selection-header-actions {
  margin-left: auto;
  display: flex;
  gap: 8px;
  align-items: center;
}

.btn-refresh-batch {
  padding: 6px 14px;
  font-size: 13px;
  font-weight: 500;
  color: #0369a1;
  background: white;
  border: 1px solid #bae6fd;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-refresh-batch:hover {
  background: #e0f2fe;
  border-color: #0ea5e9;
}

/* 上一批按钮 */
.btn-prev-batch {
  padding: 6px 14px;
  font-size: 13px;
  font-weight: 500;
  color: #64748b;
  background: white;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-prev-batch:hover {
  background: #f1f5f9;
  border-color: #94a3b8;
}

/* 批次信息 */
.batch-info {
  font-size: 12px;
  color: #64748b;
  padding: 6px 10px;
  background: #f8fafc;
  border-radius: 6px;
}

/* 展开全部按钮 */
.btn-expand-all {
  padding: 6px 14px;
  font-size: 13px;
  font-weight: 500;
  color: #7c3aed;
  background: white;
  border: 1px solid #ddd6fe;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-expand-all:hover:not(:disabled) {
  background: #ede9fe;
  border-color: #8b5cf6;
}

.btn-expand-all:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* 展开全部模式搜索框 */
.all-tables-search {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
  padding: 12px 16px;
  background: #f8fafc;
  border-radius: 10px;
  border: 1px solid #e2e8f0;
}

.all-tables-search .search-input {
  flex: 1;
  padding: 8px 14px;
  font-size: 14px;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  outline: none;
  transition: border-color 0.2s;
}

.all-tables-search .search-input:focus {
  border-color: #0ea5e9;
  box-shadow: 0 0 0 3px rgba(14, 165, 233, 0.1);
}

.all-tables-search .search-count {
  font-size: 13px;
  color: #64748b;
  white-space: nowrap;
}

/* 所有表模式的列表 */
.table-candidates.all-tables-mode {
  max-height: 400px;
  overflow-y: auto;
  padding-right: 8px;
}

.table-candidates.all-tables-mode::-webkit-scrollbar {
  width: 6px;
}

.table-candidates.all-tables-mode::-webkit-scrollbar-track {
  background: #f1f5f9;
  border-radius: 3px;
}

.table-candidates.all-tables-mode::-webkit-scrollbar-thumb {
  background: #cbd5e1;
  border-radius: 3px;
}

.table-candidates.all-tables-mode::-webkit-scrollbar-thumb:hover {
  background: #94a3b8;
}

/* 连接名标签 */
.candidate-connection {
  font-size: 11px;
  padding: 2px 8px;
  background: #e0f2fe;
  color: #0369a1;
  border-radius: 4px;
  margin-left: 8px;
}

/* 业务域标签 */
.candidate-domain {
  font-size: 12px;
  color: #7c3aed;
  margin: 6px 0 0;
}

/* 返回推荐按钮 */
.btn-back-to-recommend {
  padding: 10px 20px;
  font-size: 14px;
  color: #64748b;
  background: white;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-back-to-recommend:hover {
  background: #f1f5f9;
  border-color: #94a3b8;
}

/* 无匹配结果 */
.no-tables-found {
  padding: 40px 20px;
  text-align: center;
  color: #94a3b8;
  font-size: 14px;
}

/* 跨年查询提示 */
/* 确认原因提示 */
.confirmation-reason-hint {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 14px;
  background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
  border: 1px solid #3b82f6;
  border-radius: 8px;
  margin-bottom: 12px;
}

.confirmation-reason-hint .hint-icon {
  font-size: 16px;
}

.confirmation-reason-hint .hint-text {
  font-size: 13px;
  color: #1e40af;
  line-height: 1.4;
}

.cross-year-hint {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
  border: 1px solid #f59e0b;
  border-radius: 10px;
  margin-bottom: 16px;
}

.cross-year-hint .hint-icon {
  font-size: 18px;
}

.cross-year-hint .hint-text {
  font-size: 13px;
  color: #92400e;
  line-height: 1.5;
}

/* 多选复选框样式 */
.candidate-checkbox {
  display: flex;
  align-items: flex-start;
  padding-top: 4px;
}

.checkbox-box {
  width: 20px;
  height: 20px;
  border-radius: 4px;
  border: 2px solid #cbd5e1;
  background: white;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s;
}

.checkbox-box.checked {
  border-color: #0284c7;
  background: #0284c7;
  box-shadow: 0 2px 8px rgba(2, 132, 199, 0.4);
}

.checkbox-tick {
  color: #fef3c7;
  font-size: 12px;
  font-weight: bold;
}

/* 年份标签 */
.candidate-year {
  font-size: 12px;
  padding: 2px 8px;
  background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
  color: #92400e;
  border-radius: 6px;
  font-weight: 500;
  margin-left: 8px;
}

/* 查看更多/收起 */
.table-candidates-more {
  text-align: center;
  margin-top: 12px;
}

.btn-show-more,
.btn-show-less {
  padding: 8px 20px;
  font-size: 13px;
  font-weight: 500;
  color: #0369a1;
  background: transparent;
  border: 1px dashed #bae6fd;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-show-more:hover,
.btn-show-less:hover {
  background: #e0f2fe;
  border-style: solid;
}

/* 确认按钮禁用状态 */
.table-selection-actions .btn-confirm:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* ===== 查询结果 ===== */
.result-box {
  animation: fadeIn 0.5s ease;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* ===== 自然语言总结区 ===== */
.summary-section {
  display: flex;
  gap: 16px;
  padding: 20px 24px;
  background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
  border: 2px solid #bae6fd;
  border-radius: 12px;
  margin-bottom: 24px;
  animation: slideIn 0.5s ease;
}

@keyframes slideIn {
  from {
    opacity: 0;
    transform: translateX(-20px);
  }
  to {
    opacity: 1;
    transform: translateX(0);
  }
}

.summary-icon {
  font-size: 28px;
  flex-shrink: 0;
}

.summary-text {
  font-size: 16px;
  line-height: 1.6;
  color: #0c4a6e;
  font-weight: 500;
}

/* Markdown 渲染样式 */
.summary-text.markdown-body {
  font-weight: normal;
}

.summary-text.markdown-body h3 {
  font-size: 15px;
  font-weight: 600;
  color: #0369a1;
  margin: 16px 0 8px 0;
  padding-bottom: 0;
  border-bottom: none;
}

.summary-text.markdown-body h3:first-child {
  margin-top: 0;
}

.summary-text.markdown-body p {
  margin: 0 0 12px 0;
  line-height: 1.7;
}

.summary-text.markdown-body p:last-child {
  margin-bottom: 0;
}

.summary-text.markdown-body strong {
  font-weight: 600;
  color: #0c4a6e;
}

.summary-text.markdown-body ul,
.summary-text.markdown-body ol {
  margin: 8px 0;
  padding-left: 20px;
}

.summary-text.markdown-body li {
  margin: 4px 0;
  line-height: 1.6;
}

.summary-text.markdown-body table {
  width: 100%;
  border-collapse: collapse;
  margin: 12px 0;
  font-size: 14px;
}

.summary-text.markdown-body th,
.summary-text.markdown-body td {
  padding: 8px 12px;
  border: 1px solid #e5e7eb;
  text-align: left;
}

.summary-text.markdown-body th {
  background: #f0f9ff;
  font-weight: 600;
}

.summary-text.markdown-body code {
  background: #f1f5f9;
  padding: 2px 6px;
  border-radius: 4px;
  font-family: monospace;
  font-size: 13px;
}

/* ===== 查询信息卡片 ===== */
.query-info-card {
  background: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 16px 20px;
  margin-bottom: 20px;
  font-size: 13px;
  line-height: 1.8;
}

.info-row {
  margin-bottom: 12px;
  color: #6b7280;
}

.info-row:last-child {
  margin-bottom: 0;
}

.info-section {
  color: #6b7280;
}

.info-label {
  font-weight: 600;
  color: #374151;
  margin-right: 6px;
}

.info-value {
  color: #6b7280;
}

.calc-list {
  margin-top: 8px;
  margin-left: 16px;
}

.calc-item {
  margin-bottom: 6px;
  color: #6b7280;
  line-height: 1.6;
}

.calc-item:last-child {
  margin-bottom: 0;
}

.calc-name {
  color: #1f2937;
  font-weight: 500;
}

/* ===== SQL 代码区 ===== */
.sql-section {
  margin-bottom: 24px;
  border-radius: 12px;
  overflow: hidden;
  border: 1px solid #e1e4e8;
}

.sql-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 20px;
  background: #282c34;
  color: white;
}

.sql-label {
  font-size: 14px;
  font-weight: 600;
}

.sql-block:not(:last-child) {
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
}

.sql-subheader {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 20px;
  background: #1f232a;
  color: #f9fafb;
  border-top: 1px solid rgba(255, 255, 255, 0.05);
}

.sql-title {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.sql-label-secondary {
  font-weight: 600;
  font-size: 13px;
}

.sql-tag {
  font-size: 12px;
  color: #fcd34d;
  background: rgba(252, 211, 77, 0.15);
  border: 1px solid rgba(252, 211, 77, 0.4);
  padding: 2px 8px;
  border-radius: 999px;
}

.sql-tag.primary {
  color: #10b981;
  background: rgba(16, 185, 129, 0.15);
  border: 1px solid rgba(16, 185, 129, 0.4);
}

.copy-btn {
  padding: 6px 16px;
  background: rgba(255, 255, 255, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
  border-radius: 6px;
  color: white;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.3s;
}

.copy-btn:hover {
  background: rgba(255, 255, 255, 0.2);
}

.sql-code {
  margin: 0;
  padding: 20px;
  background: #282c34;
  color: #abb2bf;
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
  font-size: 14px;
  line-height: 1.6;
  overflow-x: auto;
}

/* ===== 数据表格区 ===== */
.data-section {
  margin-bottom: 24px;
}

.data-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.data-header h3 {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary, #1f2937);
}

.data-header-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

/* 视图切换按钮 */
.view-toggle {
  display: flex;
  gap: 4px;
  background: #f3f4f6;
  padding: 4px;
  border-radius: 10px;
}

.toggle-btn {
  padding: 6px 16px;
  border: none;
  background: transparent;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 500;
  color: #6b7280;
  cursor: pointer;
  transition: all 0.3s;
  white-space: nowrap;
}

.toggle-btn:hover {
  background: rgba(102, 126, 234, 0.1);
  color: #667eea;
}

.toggle-btn.active {
  background: white;
  color: #667eea;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.data-count {
  font-size: 14px;
  color: #6b7280;
  background: #f3f4f6;
  padding: 6px 12px;
  border-radius: 20px;
  white-space: nowrap;
}

.latency-info {
  color: #10b981;
  font-weight: 500;
}

.table-container {
  border: 1px solid #e1e4e8;
  border-radius: 12px;
  overflow: auto;
  max-height: 600px;
  position: relative;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
}

/* 图表视图 */
.chart-view {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

/* 图表类型选择器 */
.chart-type-selector {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 16px 20px;
  background: linear-gradient(to right, #f8f9fa, #ffffff);
  border: 1px solid #e1e4e8;
  border-radius: 12px;
}

.selector-label {
  font-size: 14px;
  font-weight: 600;
  color: #374151;
  white-space: nowrap;
}

.chart-type-buttons {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.chart-type-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 14px;
  background: white;
  border: 2px solid #e1e4e8;
  border-radius: 8px;
  font-size: 13px;
  color: #6b7280;
  cursor: pointer;
  transition: all 0.3s;
  white-space: nowrap;
}

.chart-type-btn:hover {
  border-color: #667eea;
  color: #667eea;
  transform: translateY(-2px);
  box-shadow: 0 4px 8px rgba(102, 126, 234, 0.2);
}

.chart-type-btn.active {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-color: #667eea;
  color: white;
  font-weight: 600;
  box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
}

.chart-type-btn.active .type-icon {
  transform: scale(1.1);
}

.type-icon {
  font-size: 16px;
  transition: transform 0.3s;
}

.type-label {
  font-size: 13px;
}

/* 图表容器 */
.chart-container {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 16px;
  min-height: 450px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
}

.data-table {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  font-size: 14px;
  background: white;
}

.data-table thead {
  background: linear-gradient(to bottom, #f8f9fa, #f1f3f5);
}

.data-table thead tr {
  position: sticky;
  top: 0;
  z-index: 10;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

.data-table th {
  padding: 14px 16px;
  text-align: left;
  font-weight: 600;
  color: #1f2937;
  background: linear-gradient(to bottom, #f8f9fa, #f1f3f5);
  border-bottom: 2px solid #d1d5db;
  white-space: nowrap;
  font-size: 13px;
  letter-spacing: 0.3px;
  text-transform: uppercase;
  font-size: 12px;
  position: relative;
}

.data-table th::after {
  content: '';
  position: absolute;
  right: 0;
  top: 25%;
  height: 50%;
  width: 1px;
  background: #e5e7eb;
}

.data-table th:last-child::after {
  display: none;
}

.data-table td {
  padding: 12px 16px;
  border-bottom: 1px solid #f3f4f6;
  color: #374151;
  font-size: 13px;
  line-height: 1.5;
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  background: white;
  transition: background-color 0.2s ease;
}

/* 数字列右对齐 */
.data-table td:has(> span[style*="color"]) {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

/* 斑马纹 */
.data-table tbody tr:nth-child(even) td {
  background: #fafbfc;
}

/* 悬停效果 */
.data-table tbody tr:hover td {
  background: #f0f7ff !important;
  cursor: pointer;
}

/* 行选中效果 */
.data-table tbody tr:active td {
  background: #e6f2ff !important;
}

.data-table tbody tr:last-child td {
  border-bottom: none;
}

/* 单元格悬停工具提示效果 */
.data-table td[title] {
  position: relative;
}

.data-table td[title]:hover::after {
  content: attr(title);
  position: absolute;
  left: 0;
  top: 100%;
  z-index: 100;
  padding: 6px 10px;
  background: #1f2937;
  color: white;
  font-size: 12px;
  border-radius: 6px;
  white-space: normal;
  max-width: 300px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  margin-top: 4px;
  word-wrap: break-word;
}

/* ===== 空状态 ===== */
.empty-result {
  text-align: center;
  padding: 60px 20px;
  color: #6b7280;
}

.empty-icon {
  font-size: 64px;
  margin-bottom: 16px;
  opacity: 0.6;
}

.empty-result p {
  margin: 0;
  font-size: 15px;
}

/* ===== 初始空状态 ===== */
.initial-state {
  text-align: center;
  padding: 100px 60px;
}

.initial-icon {
  font-size: 72px;
  margin-bottom: 24px;
  opacity: 0.7;
  animation: float 3s ease-in-out infinite;
}

.initial-state h3 {
  margin: 0 0 12px 0;
  font-size: 24px;
  font-weight: 600;
  color: #1f2937;
}

.initial-state > p {
  margin: 0 0 32px 0;
  font-size: 15px;
  color: #6b7280;
}

.example-queries {
  max-width: 1200px;
  margin: 0 auto;
}

.example-label {
  font-size: 14px;
  font-weight: 600;
  color: #6b7280;
  margin: 0 0 16px 0;
  text-align: left;
}

.example-item {
  padding: 16px 20px;
  background: #f9fafb;
  border: 2px solid #e5e7eb;
  border-radius: 10px;
  margin-bottom: 14px;
  font-size: 15px;
  color: #4b5563;
  cursor: pointer;
  transition: all 0.3s;
  text-align: left;
  line-height: 1.6;
}

.example-item:hover {
  background: white;
  border-color: #667eea;
  color: #667eea;
  transform: translateX(4px);
}

/* ===== 响应式设计 ===== */

/* 平板适配 */
@media (max-width: 1024px) {
  .main-container {
    max-width: 100%;
    padding: 30px 16px;
  }
  
  .top-bar-content {
    max-width: 100%;
    padding: 0 16px;
  }
  
  .options-row {
    flex-wrap: wrap;
    gap: 12px;
  }
  
  .selector-input,
  .domain-select {
    min-width: 160px;
  }
}

/* 移动端适配 */
@media (max-width: 768px) {
  .query-page {
    padding-bottom: 40px;
  }
  
  /* 顶部导航栏 */
  .top-bar {
    padding: 12px 0;
  }
  
  .top-bar-content {
    flex-wrap: wrap;
    gap: 12px;
    padding: 0 12px;
  }
  
  .logo-section {
    gap: 8px;
  }
  
  .logo-icon {
    font-size: 24px;
  }
  
  .logo-text h1 {
    font-size: 18px;
  }
  
  .logo-text p {
    font-size: 10px;
  }
  
  .top-bar-right {
    gap: 8px;
  }
  
  .admin-btn {
    padding: 8px 14px;
    font-size: 12px;
    border-radius: 16px;
  }
  
  .admin-icon {
    font-size: 14px;
  }
  
  .user-section {
    padding: 4px 10px;
  }
  
  .user-name {
    display: none;
  }
  
  .logout-btn {
    padding: 4px 8px;
    font-size: 11px;
  }
  
  .login-btn {
    padding: 6px 12px;
    font-size: 12px;
  }
  
  /* 主容器 */
  .main-container {
    padding: 20px 12px;
  }
  
  /* 介绍区域 */
  .hero-section {
    margin-bottom: 24px;
  }

  .hero-title {
    font-size: 20px;
    line-height: 1.4;
  }

  .hero-subtitle {
    font-size: 14px;
  }
  
  /* 查询卡片 */
  .query-card {
    border-radius: 12px;
  }

  .query-input-section {
    padding: 16px;
  }

  .input-wrapper {
    flex-direction: column;
    align-items: stretch;
    padding: 4px;
    border-radius: 12px;
  }
  
  .input-icon {
    display: none;
  }
  
  .query-input {
    font-size: 16px; /* 防止iOS缩放 */
    padding: 12px;
  }

  .query-btn {
    width: 100%;
    justify-content: center;
    padding: 14px 20px;
    font-size: 15px;
    border-radius: 10px;
  }
  
  .options-row {
    flex-direction: column;
    gap: 10px;
    margin-top: 12px;
  }
  
  .connection-selector,
  .domain-selector {
    width: 100%;
  }
  
  .selector-input,
  .domain-select {
    flex: 1;
    min-width: 0;
    font-size: 14px;
  }
  
  .option-item {
    justify-content: flex-start;
  }
  
  /* 结果区域 */
  .result-area {
    padding: 16px;
  }
  
  /* 进度流程 */
  .progress-flow-wrapper {
    padding: 14px 16px;
    margin: 16px 0 20px;
  }
  
  .progress-item-content {
    padding: 10px 12px;
  }
  
  .progress-item-title {
    font-size: 13px;
  }
  
  .progress-item-status {
    font-size: 11px;
  }
  
  /* 加载状态 */
  .loading-box {
    padding: 40px 16px;
  }
  
  .loading-text {
    font-size: 14px;
  }
  
  /* 错误提示 */
  .error-box {
    padding: 16px;
    gap: 12px;
  }
  
  .error-icon {
    font-size: 24px;
  }
  
  .error-content h4 {
    font-size: 14px;
  }
  
  .error-content p {
    font-size: 13px;
  }
  
  /* 表选择确认卡 */
  .table-selection-box {
    padding: 16px;
    border-radius: 12px;
  }
  
  .table-selection-header {
    flex-wrap: wrap;
    gap: 10px;
  }
  
  .table-selection-header h3 {
    font-size: 16px;
    flex: 1;
    min-width: 100%;
  }
  
  .table-selection-header-actions {
    margin-left: 0;
    width: 100%;
    flex-wrap: wrap;
  }
  
  .btn-prev-batch,
  .btn-refresh-batch,
  .btn-expand-all {
    padding: 8px 12px;
    font-size: 12px;
  }
  
  .table-selection-message {
    font-size: 13px;
  }
  
  .table-candidate {
    padding: 12px;
    border-radius: 10px;
  }
  
  .candidate-name {
    font-size: 14px;
  }
  
  .candidate-desc {
    font-size: 13px;
  }
  
  .candidate-confidence,
  .candidate-year,
  .candidate-connection {
    font-size: 11px;
  }
  
  .candidate-fields {
    font-size: 12px;
  }
  
  .candidate-reason {
    font-size: 12px;
    padding: 6px 10px;
  }
  
  .table-selection-actions {
    flex-direction: column;
    gap: 10px;
  }
  
  .btn-confirm,
  .btn-cancel,
  .btn-back-to-recommend {
    width: 100%;
    padding: 14px 20px;
  }
  
  /* 跨年提示 */
  .cross-year-hint {
    padding: 10px 12px;
    font-size: 12px;
  }
  
  /* 搜索框 */
  .all-tables-search {
    flex-direction: column;
    gap: 8px;
    padding: 10px 12px;
  }
  
  .all-tables-search .search-input {
    width: 100%;
  }
  
  /* 结果框 */
  .summary-section {
    padding: 14px 16px;
    gap: 12px;
    margin-bottom: 16px;
  }
  
  .summary-icon {
    font-size: 22px;
  }
  
  .summary-text {
    font-size: 14px;
    line-height: 1.5;
  }
  
  /* 查询信息卡片 */
  .query-info-card {
    padding: 12px 14px;
    font-size: 12px;
  }
  
  .calc-list {
    margin-left: 8px;
  }
  
  /* 数据区域 */
  .data-section {
    margin-bottom: 16px;
  }
  
  .data-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }
  
  .data-header h3 {
    font-size: 15px;
  }
  
  .data-header-right {
    width: 100%;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px;
  }
  
  .view-toggle {
    order: -1;
    width: 100%;
    justify-content: center;
  }
  
  .toggle-btn {
    flex: 1;
    text-align: center;
    padding: 10px 12px;
  }
  
  .data-count {
    font-size: 12px;
    padding: 4px 10px;
  }
  
  /* 表格 - 横向滚动 */
  .table-container {
    max-height: 350px;
    margin: 0 -16px;
    border-radius: 0;
    border-left: none;
    border-right: none;
  }

  .data-table {
    font-size: 12px;
    min-width: 500px;
  }

  .data-table th,
  .data-table td {
    padding: 10px 12px;
    max-width: 150px;
  }

  .data-table th {
    font-size: 11px;
  }
  
  /* 图表视图 */
  .chart-view {
    gap: 12px;
  }
  
  .chart-type-selector {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
    padding: 12px 14px;
  }
  
  .chart-type-buttons {
    width: 100%;
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 6px;
  }
  
  .chart-type-btn {
    padding: 8px 10px;
    flex-direction: column;
    gap: 2px;
  }
  
  .type-icon {
    font-size: 18px;
  }
  
  .type-label {
    font-size: 10px;
  }
  
  .chart-container {
    min-height: 300px;
    padding: 12px;
  }
  
  .chart-container > div {
    height: 300px !important;
  }
  
  /* SQL区域 */
  .sql-section {
    margin-bottom: 16px;
    border-radius: 10px;
    margin-left: -16px;
    margin-right: -16px;
    border-radius: 0;
    border-left: none;
    border-right: none;
  }
  
  .sql-header {
    padding: 10px 14px;
  }
  
  .sql-label {
    font-size: 13px;
  }
  
  .sql-subheader {
    padding: 10px 14px;
  }
  
  .sql-label-secondary {
    font-size: 12px;
  }
  
  .copy-btn {
    padding: 5px 12px;
    font-size: 12px;
  }
  
  .sql-code {
    padding: 14px;
    font-size: 12px;
    line-height: 1.5;
  }
  
  /* 空状态 */
  .empty-result {
    padding: 40px 16px;
  }
  
  .empty-icon {
    font-size: 48px;
  }
  
  .empty-result p {
    font-size: 14px;
  }
  
  /* 初始空状态 */
  .initial-state {
    padding: 60px 20px;
  }
  
  .initial-icon {
    font-size: 56px;
  }
  
  .initial-state h3 {
    font-size: 20px;
  }
  
  .initial-state > p {
    font-size: 14px;
    margin-bottom: 24px;
  }
  
  .example-label {
    font-size: 13px;
  }
  
  .example-item {
    padding: 12px 14px;
    font-size: 13px;
    border-radius: 8px;
  }
}

/* 小手机适配 */
@media (max-width: 400px) {
  .hero-title {
    font-size: 18px;
  }
  
  .hero-subtitle {
    font-size: 13px;
  }
  
  .query-input-section {
    padding: 12px;
  }
  
  .query-btn {
    padding: 12px 16px;
    font-size: 14px;
  }
  
  .table-selection-header h3 {
    font-size: 15px;
  }
  
  .chart-type-buttons {
    grid-template-columns: repeat(2, 1fr);
  }
  
  .chart-container > div {
    height: 250px !important;
  }
  
  .initial-state {
    padding: 40px 16px;
  }
  
  .initial-icon {
    font-size: 44px;
  }
  
  .initial-state h3 {
    font-size: 18px;
  }
}

/* 横屏模式 */
@media (max-height: 500px) and (orientation: landscape) {
  .hero-section {
    margin-bottom: 16px;
  }
  
  .hero-title {
    font-size: 18px;
    margin-bottom: 4px;
  }
  
  .hero-subtitle {
    font-size: 12px;
  }
  
  .initial-state {
    padding: 30px 20px;
  }
  
  .initial-icon {
    font-size: 40px;
    margin-bottom: 12px;
  }
  
  .example-queries {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 10px;
  }
  
  .example-label {
    grid-column: 1 / -1;
  }
  
  .chart-container > div {
    height: 200px !important;
  }
}

/* 滚动条美化 */
.table-container::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.table-container::-webkit-scrollbar-track {
  background: #f1f3f5;
  border-radius: 4px;
}

.table-container::-webkit-scrollbar-thumb {
  background: #c1c8cd;
  border-radius: 4px;
  transition: background 0.3s;
}

.table-container::-webkit-scrollbar-thumb:hover {
  background: #a8b1b8;
}

.table-container::-webkit-scrollbar-corner {
  background: #f1f3f5;
}
</style>
