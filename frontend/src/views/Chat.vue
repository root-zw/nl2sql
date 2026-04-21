<template>
  <div class="chat-page">
    <!-- 移动端遮罩层 -->
    <div 
      class="sidebar-overlay" 
      :class="{ visible: mobileMenuOpen }"
      @click="mobileMenuOpen = false"
    ></div>
    
    <!-- 会话列表侧边栏 -->
    <aside class="sidebar" :class="{ collapsed: sidebarCollapsed, 'mobile-open': mobileMenuOpen }">
      <div class="sidebar-header">
        <button class="new-chat-btn" @click="createNewConversation" v-if="!sidebarCollapsed">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 5v14M5 12h14"/>
          </svg>
          <span>新对话</span>
        </button>
        <button class="toggle-btn" @click="sidebarCollapsed = !sidebarCollapsed">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2v10z"/>
          </svg>
        </button>
      </div>

      <div class="sidebar-content" v-if="!sidebarCollapsed">
        <div class="conversations-list">
          <div v-if="conversationsLoading" class="loading-placeholder">
            <div class="spinner"></div>
          </div>
          <div v-else-if="conversations.length === 0" class="empty-placeholder">
            <p>开始您的第一次对话</p>
          </div>
          <div
            v-else
            v-for="conv in conversations"
            :key="conv.conversation_id"
            class="conversation-item"
            :class="{ active: currentConversationId === conv.conversation_id }"
            @click="selectConversation(conv.conversation_id)"
          >
            <svg class="conv-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2v10z"/>
            </svg>
            <span class="conv-title">{{ conv.title || '新对话' }}</span>
            <div class="conv-actions">
              <button class="conv-action-btn" @click.stop="togglePin(conv)" :title="conv.is_pinned ? '取消置顶' : '置顶'">
                <svg v-if="conv.is_pinned" width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M16 12V4h1V2H7v2h1v8l-2 2v2h5v6l1 1 1-1v-6h5v-2l-2-2z"/></svg>
                <svg v-else width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 12V4h1V2H7v2h1v8l-2 2v2h5v6l1 1 1-1v-6h5v-2l-2-2z"/></svg>
              </button>
              <button class="conv-action-btn delete" @click.stop="deleteConversation(conv.conversation_id)" title="删除">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
              </button>
            </div>
          </div>
        </div>
      </div>

      <div class="sidebar-footer" v-if="!sidebarCollapsed">
        <a v-if="isAdmin" href="/admin" target="_blank" class="footer-link">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-2 2 2 2 0 01-2-2v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06a1.65 1.65 0 00.33-1.82 1.65 1.65 0 00-1.51-1H3a2 2 0 01-2-2 2 2 0 012-2h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06a1.65 1.65 0 001.82.33H9a1.65 1.65 0 001-1.51V3a2 2 0 012-2 2 2 0 012 2v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06a1.65 1.65 0 00-.33 1.82V9a1.65 1.65 0 001.51 1H21a2 2 0 012 2 2 2 0 01-2 2h-.09a1.65 1.65 0 00-1.51 1z"/></svg>
          <span>{{ adminEntryLabel }}</span>
        </a>
        <div class="user-section" v-if="isLoggedIn">
          <div class="user-info">
            <div class="user-avatar">{{ currentUser?.username?.charAt(0)?.toUpperCase() || 'U' }}</div>
            <span class="user-name">{{ currentUser?.username }}</span>
          </div>
          <button class="logout-btn" @click="handleLogout" title="退出登录">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9"/></svg>
          </button>
        </div>
      </div>
    </aside>

    <!-- 主聊天区域 -->
    <main class="chat-main">
      <!-- 移动端顶部栏 -->
      <div class="mobile-header">
        <button class="mobile-menu-btn" @click="mobileMenuOpen = true">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M3 12h18M3 6h18M3 18h18"/>
          </svg>
        </button>
        <span class="mobile-title">{{ currentConversation?.title || '新对话' }}</span>
        <button class="mobile-new-btn" @click="createNewConversation">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 5v14M5 12h14"/>
          </svg>
        </button>
      </div>
      
      <!-- 消息列表 -->
      <div class="messages-container" ref="messagesContainer">
        <!-- 欢迎页面 -->
        <div v-if="!currentConversationId" class="welcome-screen">
          <div class="welcome-content">
            <div class="welcome-logo">
              <svg width="48" height="48" viewBox="0 0 24 24" fill="none">
                <rect x="2" y="2" width="20" height="20" rx="4" fill="url(#gradient1)"/>
                <path d="M7 8h10M7 12h6M7 16h8" stroke="white" stroke-width="2" stroke-linecap="round"/>
                <defs>
                  <linearGradient id="gradient1" x1="2" y1="2" x2="22" y2="22">
                    <stop offset="0%" stop-color="#6366f1"/>
                    <stop offset="100%" stop-color="#8b5cf6"/>
                  </linearGradient>
                </defs>
              </svg>
            </div>
            <h1 class="welcome-title">{{ heroTitle }}</h1>
            <p class="welcome-subtitle">{{ heroSubtitle }}</p>
            <div class="quick-examples">
              <button 
                class="example-card" 
                v-for="example in quickExamples" 
                :key="example" 
                @click="setQueryText(example)"
              >
                <span class="example-text">{{ example }}</span>
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
              </button>
            </div>
          </div>
        </div>

        <div v-else class="messages-list">
          <div v-if="messagesLoading" class="loading-messages">
            <span class="spinner"></span> 加载消息中...
          </div>
          <template v-else>
            <div
              v-for="msg in renderableMessages"
              :key="msg.message_id"
              class="message-item"
              :class="[msg.role, msg.status]"
            >
              <div class="message-avatar">
                {{ msg.role === 'user' ? '👤' : '🤖' }}
              </div>
              <div class="message-content">
                <!-- 用户消息 -->
                <div v-if="msg.role === 'user'" class="user-message">
                  <div>{{ msg.content }}</div>
                  <div v-if="getMessageConfirmationModeLabel(msg)" class="user-message-meta">
                    本次确认：{{ getMessageConfirmationModeLabel(msg) }}
                  </div>
                </div>

                <!-- AI 回答 -->
                <div v-else class="assistant-message">
                  <!-- 取消状态提示（显示在内容上方，不阻止已有内容显示） -->
                  <div v-if="msg.status === 'cancelled'" class="message-cancelled">
                    <span class="cancelled-icon">⚠️</span>
                    <span>查询已取消</span>
                  </div>

                  <!-- 错误状态（只有错误时才只显示错误信息） -->
                  <div v-if="msg.status === 'error'" class="message-error">
                    <span class="error-icon">❌</span>
                    <span>{{ msg.error_message || '查询失败' }}</span>
                  </div>
                  <div
                    v-if="msg.status === 'error' && hasVisibleResultActions(msg)"
                    class="result-action-bar result-action-bar-error"
                  >
                    <button
                      v-for="action in getVisibleResultActions(msg)"
                      :key="action.key"
                      class="inline-session-btn"
                      @click="action.onClick()"
                      :disabled="action.disabled"
                    >
                      {{ action.label }}
                    </button>
                  </div>
                  <div
                    v-if="msg.status === 'error' && isResultRevisionActive(msg)"
                    class="result-action-note"
                  >
                    已进入修改模式，请在下方输入修改意见。
                  </div>

                  <!-- 消息内容（包括 cancelled/pending/running/completed，只要有内容就显示） -->
                  <div v-if="msg.status !== 'error' && (hasAnyContent(msg) || msg.status === 'pending' || msg.status === 'running')" class="message-result">
                    <!-- 0. 思考过程（类似 Deep Research 效果） -->
                    <div v-if="hasThinkingSteps(msg) || (msg.status === 'running' && getThinkingSteps(msg.message_id).length > 0)" 
                         class="thinking-section"
                         :class="{ 'is-collapsed': !isThinkingExpanded(msg.message_id) && msg.status !== 'running' }">
                      <div 
                        class="thinking-header" 
                        @click="toggleThinkingExpand(msg.message_id)"
                        :class="{ collapsed: !isThinkingExpanded(msg.message_id) && msg.status !== 'running' }"
                      >
                        <span class="thinking-icon">{{ msg.status === 'running' && isThinkingExpanded(msg.message_id) ? '⏳' : '💭' }}</span>
                        <span class="thinking-title">
                          {{ msg.status === 'running' && isThinkingExpanded(msg.message_id) ? '正在思考...' : '思考过程' }}
                        </span>
                        <span class="thinking-toggle">
                          {{ isThinkingExpanded(msg.message_id) ? '▼' : '▶' }}
                        </span>
                      </div>
                      <transition name="thinking-slide">
                        <div 
                          v-show="isThinkingExpanded(msg.message_id)" 
                          class="thinking-content"
                        >
                          <div 
                            v-for="(step, idx) in getThinkingSteps(msg.message_id)" 
                            :key="idx" 
                            class="thinking-step"
                            :class="[step.status, { active: !step.done }]"
                            :style="{ animationDelay: `${idx * 0.1}s` }"
                          >
                            <div class="step-header">
                              <span class="step-indicator" :class="step.status">
                                {{ step.done ? '✓' : '●' }}
                              </span>
                              <span class="step-name">{{ getStepLabel(step.step) }}</span>
                            </div>
                            <div class="step-content markdown-body" v-html="renderMarkdown(step.content)"></div>
                          </div>
                        </div>
                      </transition>
                    </div>

                    <!-- 1. 叙述摘要（流式输出时显示，支持 Markdown） -->
                    <div v-if="msg.result_summary" class="summary-section">
                      <div 
                        class="summary-content markdown-body" 
                        v-html="renderMarkdown(msg.result_summary)"
                      ></div>
                    </div>
                    <!-- 叙述生成中的占位 -->
                    <div v-else-if="isNarrativeStreaming(msg)" class="summary-section loading">
                      <span class="loading-text">正在生成分析...</span>
                    </div>

                    <!-- 以下内容在叙述完成后才显示 -->
                    <template v-if="canShowResultDetails(msg)">
                      <!-- 数据结果区域 -->
                      <div v-if="hasTableData(msg)" class="data-section">
                        <div class="data-header">
                          <div v-if="extractQueryResultTitle(msg.result_summary)" 
                               class="query-result-title markdown-body" 
                               v-html="renderMarkdown(extractQueryResultTitle(msg.result_summary))">
                          </div>
                          <h3 v-else>查询结果</h3>
                          <div class="data-header-right">
                            <!-- 表格/图表切换 -->
                            <div v-if="canShowChart(msg)" class="view-toggle">
                              <button 
                                class="toggle-btn" 
                                :class="{ active: getViewMode(msg) === 'table' }"
                                @click="setViewMode(msg.message_id, 'table')"
                              >表格</button>
                              <button 
                                class="toggle-btn" 
                                :class="{ active: getViewMode(msg) === 'chart' }"
                                @click="setViewMode(msg.message_id, 'chart')"
                              >图表</button>
                            </div>
                            <span class="data-count">
                              共 {{ msg.result_data.rows.length }} 条记录
                              <span v-if="msg.result_data.meta?.total_time_ms || msg.result_data.meta?.latency_ms" class="latency-info">
                                · 耗时 {{ formatDuration(msg.result_data.meta?.total_time_ms || msg.result_data.meta?.latency_ms) }}
                              </span>
                            </span>
                            <button class="export-csv-btn" @click="exportToCSV(msg)" title="导出CSV">
                              导出
                            </button>
                          </div>
                        </div>

                        <!-- 表格视图 -->
                        <div v-show="getViewMode(msg) === 'table'" class="table-wrapper">
                          <table class="result-table">
                            <thead>
                              <tr>
                                <th v-for="col in msg.result_data.columns" :key="col.name">{{ col.label || col.name }}</th>
                              </tr>
                            </thead>
                            <tbody>
                              <tr v-for="(row, rowIdx) in getDisplayRows(msg)" :key="rowIdx">
                                <td v-for="col in msg.result_data.columns" :key="col.name" v-html="formatCellValue(row[col.name])"></td>
                              </tr>
                            </tbody>
                          </table>
                          <div v-if="msg.result_data.rows.length > getMaxDisplayRows(msg)" class="table-more">
                            <span>还有 {{ msg.result_data.rows.length - getMaxDisplayRows(msg) }} 行数据...</span>
                            <button class="show-more-btn" @click="toggleShowAllRows(msg.message_id)">
                              {{ expandedRows[msg.message_id] ? '收起' : '显示全部' }}
                            </button>
                          </div>
                        </div>

                        <!-- 图表视图 -->
                        <div v-if="getViewMode(msg) === 'chart' && canShowChart(msg)" class="chart-view">
                          <div class="chart-type-selector">
                            <span class="selector-label">图表类型：</span>
                            <div class="chart-type-buttons">
                              <button
                                v-for="type in ['bar', 'line', 'pie']"
                                :key="type"
                                class="chart-type-btn"
                                :class="{ active: getChartType(msg) === type }"
                                @click="setChartType(msg.message_id, type)"
                              >{{ chartTypeLabels[type] }}</button>
                            </div>
                          </div>
                          <div class="chart-container">
                            <v-chart 
                              :option="getChartOption(msg)" 
                              autoresize 
                              style="height: 400px;" 
                              @legendselectchanged="(params) => handleLegendSelect(msg.message_id, params)"
                            />
                          </div>
                        </div>
                      </div>

                      <!-- 4. SQL 代码展示 -->
                      <div v-if="msg.sql_text" class="sql-section">
                        <div class="sql-header" @click="toggleSqlExpand(msg.message_id)">
                          <span>SQL</span>
                          <span class="expand-icon">{{ expandedSql[msg.message_id] ? '▼' : '▶' }}</span>
                          <button class="copy-sql-btn-inline" @click.stop="copySQL(msg.sql_text)" title="复制SQL">复制</button>
                        </div>
                        <div v-if="expandedSql[msg.message_id]" class="sql-code-wrapper">
                          <pre class="sql-code"><code>{{ msg.sql_text }}</code></pre>
                        </div>
                      </div>

                      <!-- 无数据时的提示：仅在有SQL但查询结果为空时显示 -->
                      <div v-if="msg.sql_text && !hasTableData(msg) && msg.status === 'completed'" class="empty-result">
                        <p v-if="msg.result_data?.meta?.explain_only">SQL 已生成，未执行（仅生成SQL模式）</p>
                        <p v-else>查询结果为空</p>
                      </div>
                    </template>

                    <div v-if="hasVisibleResultActions(msg)" class="result-action-bar">
                      <button
                        v-for="action in getVisibleResultActions(msg)"
                        :key="action.key"
                        class="inline-session-btn"
                        @click="action.onClick()"
                        :disabled="action.disabled"
                      >
                        {{ action.label }}
                      </button>
                    </div>
                    <div
                      v-if="isResultRevisionActive(msg)"
                      class="result-action-note"
                    >
                      已进入修改模式，请在下方输入修改意见。
                    </div>

                    <!-- 加载中状态（无思考步骤和叙述内容时才显示） -->
                    <div v-if="(msg.status === 'pending' || (msg.status === 'running' && !msg.result_summary && !isNarrativeStreaming(msg))) && !hasThinkingSteps(msg) && getThinkingSteps(msg.message_id).length === 0" class="loading-indicator">
                      <div class="typing-indicator">
                        <span></span><span></span><span></span>
                      </div>
                      <span class="loading-text">{{ getStatusText(msg.status) }}</span>
                    </div>
                  </div>
                </div>

                <div class="message-time">{{ formatTime(msg.created_at) }}</div>
              </div>
            </div>
          </template>

          <!-- 统一确认卡 -->
          <div v-if="hasActivePendingSession" class="message-item assistant">
            <div class="message-avatar">🤖</div>
            <div class="message-content">
              <div class="confirm-box session-review-box">
                <div class="confirm-header">
                  <span class="confirm-icon">{{ pendingSessionIcon }}</span>
                  <h3>{{ pendingSessionTitle }}</h3>
                  <span class="session-node-badge">{{ pendingSessionNodeLabel }}</span>
                </div>
                <div class="confirm-content">
                  <div class="user-question-section">
                    <p class="section-label">💬 您的问题：</p>
                    <div class="user-question-text">{{ pendingQueryText }}</div>
                  </div>

                  <div v-if="pendingSessionSummaryItems.length" class="ai-understanding-section">
                    <p class="section-label">🤖 系统理解：</p>
                    <div class="understanding-content">
                      <ul class="understanding-list">
                        <li
                          v-for="(item, index) in pendingSessionSummaryItems"
                          :key="`pending-summary-${index}`"
                          class="understanding-item"
                        >
                          {{ item }}
                        </li>
                      </ul>
                    </div>
                  </div>

                  <div v-if="pendingSessionNode === 'table_resolution' && pendingSessionChallengeItem" class="pending-challenge-section">
                    <p class="section-label">❓ 需要确认：</p>
                    <div class="pending-challenge-text">{{ pendingSessionChallengeItem }}</div>
                  </div>

                  <template v-if="pendingSessionNode === 'table_resolution'">
                    <div class="table-selection-tip unified-table-tip">
                      <span v-if="showAllAccessibleTables">请从全部数据表中选择，可单选，也可多选。</span>
                      <span v-else>请在下方选择数据表，可单选，也可多选。</span>
                    </div>

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

                    <div v-if="!showAllAccessibleTables" class="table-candidates">
                      <div
                        v-for="candidate in visibleCandidates"
                        :key="candidate.table_id"
                        class="table-candidate"
                        :class="{ selected: isTableSelected(candidate.table_id), 'is-year-table': candidate.data_year }"
                        @click="toggleTableSelection(candidate)"
                      >
                        <div class="candidate-checkbox">
                          <span v-if="isTableSelected(candidate.table_id)" class="checkbox-tick">✓</span>
                        </div>
                        <div class="candidate-info">
                          <div class="candidate-topline">
                            <div class="candidate-primary">
                              <div class="candidate-name">{{ candidate.table_name }}</div>
                              <div class="candidate-meta candidate-meta-inline">
                                <span v-if="candidate.description" class="candidate-meta-item">{{ candidate.description }}</span>
                                <span v-if="candidate.data_year" class="candidate-meta-item">{{ candidate.data_year }}年度</span>
                                <span v-if="candidate.domain_name" class="candidate-meta-item">{{ candidate.domain_name }}</span>
                              </div>
                            </div>
                            <div class="candidate-score">{{ Math.round((candidate.confidence || 0) * 100) }}%</div>
                          </div>
                        </div>
                      </div>
                      <div v-if="visibleCandidates.length === 0" class="no-tables-found">
                        <span>当前推荐表为空，请查看所有表。</span>
                      </div>
                    </div>

                    <div v-else class="table-candidates all-tables-mode">
                      <div
                        v-for="table in filteredAllTables"
                        :key="table.table_id"
                        class="table-candidate"
                        :class="{ selected: isTableSelected(table.table_id) }"
                        @click="toggleTableSelectionById(table.table_id)"
                      >
                        <div class="candidate-checkbox">
                          <span v-if="isTableSelected(table.table_id)">✓</span>
                        </div>
                        <div class="candidate-info">
                          <div class="candidate-topline">
                            <div class="candidate-primary">
                              <div class="candidate-name">{{ table.table_name }}</div>
                              <div class="candidate-meta candidate-meta-inline">
                                <span v-if="table.description" class="candidate-meta-item">{{ table.description }}</span>
                                <span v-if="table.data_year" class="candidate-meta-item">{{ table.data_year }}年度</span>
                                <span v-if="table.domain_name" class="candidate-meta-item">{{ table.domain_name }}</span>
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                      <div v-if="filteredAllTables.length === 0" class="no-tables-found">
                        <span>未找到匹配的数据表</span>
                      </div>
                    </div>

                    <div class="table-selection-actions">
                      <button
                        v-for="action in pendingSessionActionButtons"
                        :key="action.key"
                        :class="action.className"
                        @click="action.onClick()"
                        :disabled="action.disabled"
                      >
                        {{ action.label }}
                      </button>
                    </div>
                  </template>

                  <template v-else-if="pendingSessionNode === 'execution_guard'">
                    <div v-if="pendingConfirm?.warnings?.length" class="warnings-section">
                      <p class="warnings-label">⚡ 风险提示：</p>
                      <div v-for="(w, i) in pendingConfirm.warnings" :key="i" class="warning-tag">{{ w }}</div>
                    </div>
                    <div v-if="pendingConfirm?.estimated_cost" class="estimated-cost-box">
                      <div class="estimated-cost-item">
                        <span>预计扫描行数</span>
                        <strong>{{ formatEstimatedRows(pendingConfirm.estimated_cost.rows) }}</strong>
                      </div>
                      <div class="estimated-cost-item">
                        <span>估算成本</span>
                        <strong>{{ pendingConfirm.estimated_cost.cost ?? '-' }}</strong>
                      </div>
                    </div>
                    <div class="confirm-actions session-actions">
                      <button
                        v-for="action in pendingSessionActionButtons"
                        :key="action.key"
                        :class="action.className"
                        @click="action.onClick()"
                        :disabled="action.disabled"
                      >
                        {{ action.label }}
                      </button>
                    </div>
                  </template>

                  <template v-else-if="pendingSessionNode === 'draft_confirmation'">
                    <div class="confirm-actions session-actions">
                      <button
                        v-for="action in pendingSessionActionButtons"
                        :key="action.key"
                        :class="action.className"
                        @click="action.onClick()"
                        :disabled="action.disabled"
                      >
                        {{ action.label }}
                      </button>
                    </div>
                  </template>

                  <div v-if="pendingSessionActionLoading" class="session-inline-status">
                    正在提交确认动作...
                  </div>
                </div>
              </div>
            </div>
          </div>

        </div>
      </div>

      <!-- 输入区域 -->
      <footer class="chat-input-area">
        <div class="input-container">
          <!-- 配置选项栏 -->
          <div class="input-options">
            <select v-model="selectedConnection" class="option-select">
              <option value="">🔌 自动检测数据库</option>
              <option v-for="conn in availableConnections" :key="conn.id" :value="conn.id">
                {{ conn.name }}
              </option>
            </select>
            <select v-model="selectedDomain" class="option-select" v-if="availableDomains.length > 0">
              <option value="">📁 自动检测业务域</option>
              <option v-for="domain in availableDomains" :key="domain.id" :value="domain.id">
                {{ domain.name }}
              </option>
            </select>
            <div class="confirmation-mode-group" :class="{ disabled: preQueryConfirmationLocked }">
              <span class="confirmation-mode-label">确认策略</span>
              <button
                v-for="option in confirmationModeOptions"
                :key="option.value"
                type="button"
                class="confirmation-mode-btn"
                :class="{ active: isConfirmationModeActive(option.value) }"
                :disabled="preQueryConfirmationLocked"
                :title="option.description"
                @click="setQueryConfirmationMode(option.value)"
              >
                {{ option.label }}
              </button>
            </div>
            <label class="option-checkbox">
              <input type="checkbox" v-model="explainOnly" />
              <span>仅生成SQL</span>
            </label>
          </div>
          <div class="input-wrapper">
            <textarea
              ref="inputTextarea"
              v-model="queryText"
              class="chat-input"
              :placeholder="inputPlaceholder"
              @keydown.enter.exact.prevent="handleSendMessage"
              @keydown.enter.shift.exact="null"
              :disabled="interactionLocked"
              rows="1"
              @input="autoResizeTextarea"
            ></textarea>
            <div class="input-actions">
              <button
                v-if="loading || narrativeStreaming"
                class="stop-btn"
                @click="stopQuery"
                title="停止生成"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>
              </button>
              <button
                v-else
                class="send-btn"
                :disabled="!queryText.trim() || !canSendMessage || interactionLocked"
                @click="handleSendMessage"
                title="发送"
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg>
              </button>
            </div>
          </div>
          <p class="input-hint">{{ inputHintText }}</p>
        </div>
      </footer>
    </main>

  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onBeforeUnmount, watch, nextTick } from 'vue'
import { useRouter } from 'vue-router'
import { conversationAPI, querySessionAPI } from '@/api'
import { useQueryActionControls } from '@/composables/useQueryActionControls'
import { usePendingSessionPresentation } from '@/composables/usePendingSessionPresentation'
import { usePendingSessionViewModels } from '@/composables/usePendingSessionViewModels'
import { useQuerySessionSnapshots } from '@/composables/useQuerySessionSnapshots'
import request from '@/utils/request'
import { tokenManager } from '@/utils/tokenManager'
import MarkdownIt from 'markdown-it'
import markdownItKatex from '@traptitech/markdown-it-katex'
import 'katex/dist/katex.min.css'
import { ElMessage, ElMessageBox } from 'element-plus'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, LineChart, PieChart } from 'echarts/charts'
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

// 初始化 Markdown 渲染器（支持数学公式）
const md = new MarkdownIt({
  html: false,
  linkify: true,
  typographer: true,
  breaks: true
})
md.use(markdownItKatex, {
  throwOnError: false,
  errorColor: '#cc0000'
})

// 预处理 LaTeX 公式格式，修复常见格式问题
function preprocessLatex(content) {
  if (!content) return content
  
  // 1. 修复行内公式 $ 和内容之间的空格: $ \text{...} $ → $\text{...}$
  // 匹配 $ 开头有空格或结尾有空格的情况
  content = content.replace(/\$\s+([^$]+?)\s+\$/g, (match, inner) => {
    return '$' + inner.trim() + '$'
  })
  
  // 2. 修复只有开头空格的情况: $ \text{...}$ → $\text{...}$
  content = content.replace(/\$\s+([^$]+?)\$/g, (match, inner) => {
    return '$' + inner.trimStart() + '$'
  })
  
  // 3. 修复只有结尾空格的情况: $\text{...} $ → $\text{...}$
  content = content.replace(/\$([^$]+?)\s+\$/g, (match, inner) => {
    return '$' + inner.trimEnd() + '$'
  })
  
  // 4. 在 LaTeX 公式中转义 % 符号（% 在 LaTeX 中是注释符）
  // 只处理公式内部的 %，不是已转义的 \%
  content = content.replace(/\$([^$]+)\$/g, (match, inner) => {
    // 将未转义的 % 替换为 \%
    const escaped = inner.replace(/(?<!\\)%/g, '\\%')
    return '$' + escaped + '$'
  })
  
  return content
}

// 渲染 Markdown 内容
function renderMarkdown(content) {
  if (!content) return ''
  // 预处理 LaTeX 公式格式
  const preprocessed = preprocessLatex(content)
  return md.render(preprocessed)
}

// 从markdown内容中提取"查询结果"标题
function extractQueryResultTitle(summary) {
  if (!summary) return null
  // 查找 ### 查询结果 标题（支持多种格式）
  const patterns = [
    /###\s*查询结果\s*\n/,
    /###\s*查询结果\s*$/m,
    /###\s*查询结果/
  ]
  for (const pattern of patterns) {
    if (pattern.test(summary)) {
      // 返回标题的markdown格式
      return '### 查询结果'
    }
  }
  return null
}

const router = useRouter()

// ==================== 环境变量配置 ====================
const env = import.meta.env
const brandName = env.VITE_BRAND_NAME || '智能问数'
const brandTagline = env.VITE_BRAND_TAGLINE || '智能问数'
const heroTitle = env.VITE_HERO_TITLE || '智能问数'
const heroSubtitle = env.VITE_HERO_SUBTITLE || '无需编写代码，像聊天一样查询数据'
const adminEntryLabel = env.VITE_ADMIN_ENTRY_LABEL || '管理后台'
const backendPort = env.VITE_BACKEND_PORT || '8000'
const wsHost = env.VITE_WS_HOST || ''
const API_BASE = ''  // API 基础路径（相对路径）

// ==================== 状态定义 ====================
// 侧边栏
const sidebarCollapsed = ref(false)
const mobileMenuOpen = ref(false)

// 会话相关
const conversations = ref([])
const conversationsLoading = ref(false)
const currentConversationId = ref(null)
const currentConversation = ref(null)
const messages = ref([])
const messagesLoading = ref(false)

// 查询相关
const queryText = ref('')
const loading = ref(false)
const currentQueryId = ref(null)
const currentProgressText = ref('思考中...')
const currentQueryRunning = computed(() => loading.value)

// 进度流程相关
const progressSteps = ref([])
// 叙述流式输出相关
const narrativeStreaming = ref(false)  // 是否正在流式输出叙述
const narrativePending = ref(false)    // 叙述是否待完成
const narrativeBuffer = ref('')        // 叙述内容缓冲区
const currentStreamingMessageId = ref(null)  // 当前正在流式输出的消息ID

// 思考过程相关（类似 Deep Research 效果）
const thinkingSteps = reactive({})  // { messageId: [{ step, content, done, status }] }
const thinkingExpanded = reactive({})  // { messageId: boolean } 是否展开思考过程

// 确认卡相关
const pendingConfirm = ref(null)
const pendingQueryText = ref('')
const pendingSessionSnapshot = ref(null)
const pendingSessionActionLoading = ref(false)

// 表选择相关
const pendingTableSelection = ref(null)
const selectedTableIds = ref([])
const selectedTableId = ref(null)
const originalQueryId = ref(null)
const tableBatchIndex = ref(0)
const loadingAllTables = ref(false)
const allAccessibleTables = ref([])
// 展开全部功能相关
const showAllAccessibleTables = ref(false)  // 是否显示所有可访问表模式
const filteredAllTables = ref([])  // 过滤后的表列表
const allTablesSearchQuery = ref('')  // 搜索关键词

// 计算每批显示的数量，使用后端返回的 page_size，如果没有则默认为 5
const tableBatchSize = computed(() => {
  return pendingTableSelection.value?.page_size || 5
})

// 图表相关
const chartTypeMap = {
  bar: 'bar',
  line: 'line',
  pie: 'pie'
}

// 用户状态
const isLoggedIn = ref(false)
const currentUser = ref(null)

// 选项
const selectedConnection = ref('')
const selectedDomain = ref('')
const availableConnections = ref([])
const availableDomains = ref([])
const explainOnly = ref(false)
const SYSTEM_DEFAULT_CONFIRMATION_MODE = 'always_confirm'
const confirmationModeOptions = [
  {
    value: 'adaptive',
    label: '智能确认',
    description: '仅在歧义、高风险或低置信度时进入确认'
  },
  {
    value: 'always_confirm',
    label: '始终确认',
    description: '本次提问先进入确认，再继续生成或执行'
  }
]
const queryConfirmationMode = ref(null)
const confirmationModeLabelMap = {
  adaptive: '智能确认',
  always_confirm: '始终确认'
}

// WebSocket
const wsRef = ref(null)

// UI 状态
const expandedSql = reactive({})
const expandedRows = reactive({})
const chartTypes = reactive({})  // message_id -> chart type
const legendFirstClick = reactive({})  // message_id -> boolean (是否已首次点击图例)
const legendSelected = reactive({})  // message_id -> { seriesName: boolean } (图例选中状态)
const resultActionLoadingIds = reactive({})
const resultReplyContext = ref(null)
const messagesContainer = ref(null)
const inputTextarea = ref(null)

// 图表类型标签
const chartTypeLabels = {
  bar: '柱状图',
  line: '折线图',
  pie: '饼图'
}

// 快速示例
const quickExamples = ref([
  '2023年和2024年成交总价最高的土地分别是？',
  '2025年武汉市各行政区成交地块总宗数、出让面积、总价、每亩单价和楼面地价分别是多少?',
  '今年江夏区出让总价最高的工业用地是哪一宗?成交总价、每亩单价、楼面地价和竞得人分别是什么?'
])

// ==================== 计算属性 ====================
const canSendMessage = computed(() => {
  return isLoggedIn.value && queryText.value.trim()
})

const interactionLocked = computed(() => {
  return loading.value || pendingSessionActionLoading.value || Object.keys(resultActionLoadingIds).length > 0
})

const renderableMessages = computed(() => {
  return messages.value.filter(msg => !msg.hidden)
})

const hasActivePendingSession = computed(() => {
  const snapshot = pendingSessionSnapshot.value
  if (!snapshot) return false
  return snapshot.status === 'awaiting_user_action' &&
    ['table_resolution', 'execution_guard', 'draft_confirmation'].includes(snapshot.current_node)
})

const pendingSessionState = computed(() => {
  return pendingSessionSnapshot.value?.state || {}
})

const pendingSessionNode = computed(() => {
  return pendingSessionSnapshot.value?.current_node || ''
})

const pendingRevisionNote = computed(() => {
  const revisionRequest = pendingSessionState.value.revision_request || {}
  return revisionRequest.text || revisionRequest.source_text || revisionRequest.natural_language_reply || ''
})

const isAdmin = computed(() => {
  const role = currentUser.value?.role
  return role === 'admin' || role === 'data_admin'
})

const inputPlaceholder = computed(() => {
  if (!isLoggedIn.value) return '请先登录后再提问...'
  if (hasActivePendingSession.value) return '当前处于确认阶段，可直接回复“确认”“改成…”“为什么这样理解”等...'
  if (resultReplyContext.value?.queryId) return '请输入你希望修改的内容，按 Enter 提交到当前结果...'
  if (!currentConversationId.value) return '输入问题开始新对话，例如：2025年武昌区的成交宗数'
  return '输入追问内容，按 Enter 发送...'
})

const preQueryConfirmationLocked = computed(() => {
  return hasActivePendingSession.value || interactionLocked.value
})

const effectiveQueryConfirmationMode = computed(() => {
  return queryConfirmationMode.value || SYSTEM_DEFAULT_CONFIRMATION_MODE
})

const inputHintText = computed(() => {
  const base = '按 Enter 发送，Shift + Enter 换行'
  if (hasActivePendingSession.value) {
    return `${base}；当前正在确认上一条查询`
  }
  if (resultReplyContext.value?.queryId) {
    return `${base}；当前将作为上一条结果的修改意见提交`
  }
  if (queryConfirmationMode.value === 'adaptive') {
    return `${base}；本次使用智能确认`
  }
  if (queryConfirmationMode.value === 'always_confirm') {
    return `${base}；本次提问将先进入确认`
  }
  return base
})

// ==================== 生命周期 ====================
onMounted(async () => {
  document.documentElement.classList.add('no-page-scroll')
  document.body.classList.add('no-page-scroll')
  checkLoginStatus()
  await loadInitialData()
})

onBeforeUnmount(() => {
  document.documentElement.classList.remove('no-page-scroll')
  document.body.classList.remove('no-page-scroll')
  closeWebSocket()
})

// ==================== 方法定义 ====================

// 检查登录状态
function checkLoginStatus() {
  const token = localStorage.getItem('token')
  const userStr = localStorage.getItem('user')
  if (token && userStr) {
    try {
      currentUser.value = JSON.parse(userStr)
      isLoggedIn.value = true
    } catch {
      localStorage.removeItem('token')
      localStorage.removeItem('user')
    }
  }
}

// 加载初始数据
async function loadInitialData() {
  await Promise.all([
    loadConnections(),
    loadDomains(),
    loadConversations()
  ])
}

function hasResolvableResultContext() {
  return messages.value.some(msg =>
    msg.role === 'assistant' &&
    msg.status === 'completed' &&
    msg.query_id &&
    (msg.result_data || msg.result_summary || msg.content)
  )
}

async function resolveFollowupContextResolution(text) {
  if (!currentConversationId.value || !hasResolvableResultContext()) {
    return { resolution: 'resolved_to_new_query' }
  }

  try {
    const res = await conversationAPI.resolveFollowupContext(
      currentConversationId.value,
      { text },
      { silentStatuses: [404] }
    )
    return res.data || { resolution: 'resolved_to_new_query' }
  } catch (e) {
    console.warn('解析结果后追问上下文失败，已回退为独立新问题', e)
    return { resolution: 'resolved_to_new_query' }
  }
}

function buildIdempotencyKey(prefix = 'session-action') {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

function setQueryConfirmationMode(mode) {
  if (preQueryConfirmationLocked.value) return
  queryConfirmationMode.value = mode
}

function isConfirmationModeActive(mode) {
  return effectiveQueryConfirmationMode.value === mode
}

function consumeQueryConfirmationMode() {
  const mode = queryConfirmationMode.value
  queryConfirmationMode.value = null
  return mode
}

function getConfirmationModeLabel(mode) {
  return confirmationModeLabelMap[mode] || ''
}

function getMessageConfirmationModeLabel(msg) {
  return msg?.metadata?.confirmation_mode_label ||
    getConfirmationModeLabel(msg?.metadata?.confirmation_mode)
}

function normalizeThinkingSteps(steps) {
  if (!Array.isArray(steps)) return []
  return steps
    .filter(step => step?.step)
    .map(step => ({
      step: step.step,
      content: step.content || '',
      done: Boolean(step.done),
      status: step.status || 'started'
    }))
}

function hasPersistedThinkingSteps(msg) {
  return normalizeThinkingSteps(msg?.metadata?.thinking_steps || msg?.thinking_steps).length > 0
}

function isLegacyBlankAssistantMessage(msg) {
  return msg?.role === 'assistant' &&
    ['completed', 'cancelled'].includes(msg?.status) &&
    !msg?.content &&
    !msg?.result_summary &&
    !msg?.sql_text &&
    !(msg?.result_data?.rows?.length > 0) &&
    !msg?.error_message &&
    !hasPersistedThinkingSteps(msg)
}

function hydrateConversationMessage(msg) {
  let nextMessage = { ...msg }

  if (nextMessage.result_data?.rows && nextMessage.result_data?.columns) {
    const columns = nextMessage.result_data.columns
    const rawRows = nextMessage.result_data.rows
    if (rawRows.length > 0 && Array.isArray(rawRows[0])) {
      const objectRows = rawRows.map(row => {
        const obj = {}
        columns.forEach((col, idx) => {
          obj[col.name] = row[idx]
        })
        return obj
      })
      nextMessage = {
        ...nextMessage,
        result_data: {
          ...nextMessage.result_data,
          rows: objectRows
        }
      }
    }
  }

  const restoredThinkingSteps = normalizeThinkingSteps(
    nextMessage.metadata?.thinking_steps || nextMessage.thinking_steps
  )
  if (restoredThinkingSteps.length > 0) {
    thinkingSteps[nextMessage.message_id] = restoredThinkingSteps
    thinkingExpanded[nextMessage.message_id] = false
  }

  return {
    ...nextMessage,
    hidden: Boolean(nextMessage.metadata?.hidden) || isLegacyBlankAssistantMessage(nextMessage)
  }
}

function clearResultReplyContext() {
  resultReplyContext.value = null
}

function resetPendingTableUi() {
  showAllAccessibleTables.value = false
  allAccessibleTables.value = []
  filteredAllTables.value = []
  allTablesSearchQuery.value = ''
  tableBatchIndex.value = 0
}

function clearPendingSessionState({ keepQueryText = false } = {}) {
  pendingSessionSnapshot.value = null
  pendingConfirm.value = null
  pendingTableSelection.value = null
  clearResultReplyContext()
  selectedTableIds.value = []
  selectedTableId.value = null
  resetPendingTableUi()
  if (!keepQueryText) {
    pendingQueryText.value = ''
    originalQueryId.value = null
  }
}

function hideAssistantPlaceholder(messageId) {
  const msg = findMessage(messageId)
  if (!msg) return
  msg.hidden = true
  msg.status = 'completed'
}

function createAssistantPlaceholder(messageId = null) {
  const assistantMessageId = messageId || ('temp-assistant-' + Date.now())
  const assistantMessage = {
    message_id: assistantMessageId,
    conversation_id: currentConversationId.value,
    role: 'assistant',
    content: '',
    status: 'pending',
    created_at: new Date().toISOString()
  }
  messages.value.push(assistantMessage)
  return assistantMessageId
}

function reuseOrCreateAssistantMessage(queryId) {
  const snapshotMessageId = pendingSessionSnapshot.value?.message_id ||
    pendingSessionSnapshot.value?.session?.message_id ||
    getResultSessionSnapshot(queryId)?.message_id ||
    null

  if (snapshotMessageId) {
    const existingMsg = findMessage(snapshotMessageId)
    if (existingMsg) {
      existingMsg.hidden = false
      existingMsg.status = 'pending'
      existingMsg.content = ''
      existingMsg.sql_text = null
      existingMsg.result_summary = ''
      existingMsg.result_data = null
      existingMsg.error_message = null
      existingMsg.is_stopping = false
      return snapshotMessageId
    }
    return createAssistantPlaceholder(snapshotMessageId)
  }

  return createAssistantPlaceholder()
}

function prepareAssistantPlaceholder(assistantMessageId, progressText = '思考中...') {
  loading.value = true
  currentProgressText.value = progressText
  narrativeStreaming.value = false
  narrativePending.value = false
  narrativeBuffer.value = ''
  currentStreamingMessageId.value = assistantMessageId
  progressSteps.value = []
  scrollToBottom()
}

function appendAssistantInfoMessage(text) {
  if (!text) return
  messages.value.push({
    message_id: 'local-assistant-' + Date.now(),
    conversation_id: currentConversationId.value,
    role: 'assistant',
    content: '',
    status: 'completed',
    result_summary: text,
    created_at: new Date().toISOString()
  })
  scrollToBottom()
}

function formatEstimatedRows(rows) {
  if (rows === null || rows === undefined || rows === '') return '-'
  return new Intl.NumberFormat('zh-CN').format(rows)
}

function canUsePendingAction(actionType) {
  return (pendingSessionSnapshot.value?.confirmation_view?.pending_actions || []).includes(actionType)
}

function getPendingActionBinding(semanticAction) {
  return pendingSessionSnapshot.value?.confirmation_view?.dependency_meta?.action_bindings?.[semanticAction] || semanticAction
}

function isResultActionBusy(msg) {
  return Boolean(msg?.query_id && resultActionLoadingIds[msg.query_id])
}

function isResultRevisionActive(msg) {
  return Boolean(msg?.query_id && resultReplyContext.value?.queryId === msg.query_id)
}

const {
  normalizeSessionSnapshot,
  cacheResultSessionSnapshot,
  getResultSessionSnapshot,
  canUseResultAction,
  getResultActionBinding,
  loadResultSessionSnapshot,
  hydrateResultActionContracts,
} = useQuerySessionSnapshots({ messages })

const {
  applyPendingSessionSnapshot,
  loadQuerySessionSnapshot,
  buildPendingExplanation,
  isManualTableOverride,
} = usePendingSessionViewModels({
  pendingSessionSnapshot,
  pendingConfirm,
  pendingTableSelection,
  pendingQueryText,
  originalQueryId,
  selectedTableIds,
  selectedTableId,
  tableBatchIndex,
  pendingRevisionNote,
  normalizeSessionSnapshot,
  cacheResultSessionSnapshot,
  clearPendingSessionState,
  resetPendingTableUi,
  formatEstimatedRows,
  expandAllTables,
})

const {
  pendingSessionTitle,
  pendingSessionNodeLabel,
  pendingSessionIcon,
  pendingSessionSummaryItems,
  pendingSessionSummaryText,
  pendingSessionChallengeItem,
} = usePendingSessionPresentation({
  pendingSessionSnapshot,
  pendingSessionState,
  pendingSessionNode,
  pendingTableSelection,
  pendingQueryText,
})

const { pendingSessionActionButtons, getVisibleResultActions, hasVisibleResultActions } = useQueryActionControls({
  pendingSessionNode,
  pendingSessionActionLoading,
  selectedTableIds,
  pendingTableSelection,
  showAllAccessibleTables,
  canUsePendingAction,
  confirmTableSelection,
  requestTableReselection,
  backToRecommendTables,
  requestManualTableSelection,
  focusPendingReplyInput,
  cancelPendingSession,
  approveExecution,
  confirmDraftRevision,
  canUseResultAction,
  isResultRevisionActive,
  isResultActionBusy,
  reopenTableSelectionForMessage,
  startResultRevision,
})

// 加载数据库连接列表
async function loadConnections() {
  try {
    const res = await request.get('/query/connections')
    availableConnections.value = res.data?.connections?.map(c => ({
      id: c.connection_id,
      name: c.connection_name
    })) || []
  } catch (e) {
    console.warn('加载连接列表失败', e)
  }
}

// 加载业务域列表
async function loadDomains() {
  try {
    const res = await request.get('/domains')
    availableDomains.value = res.data?.map(d => ({
      id: d.domain_id,
      name: d.domain_name
    })) || []
  } catch (e) {
    console.warn('加载业务域失败', e)
  }
}

// 加载会话列表
async function loadConversations() {
  if (!isLoggedIn.value) {
    conversations.value = []
    return
  }
  
  conversationsLoading.value = true
  try {
    const res = await conversationAPI.list({ limit: 50 })
    conversations.value = res.data?.items || []
  } catch (e) {
    console.warn('加载会话列表失败', e)
  } finally {
    conversationsLoading.value = false
  }
}

// 创建新会话
async function createNewConversation() {
  if (!isLoggedIn.value) {
    openLoginDialog()
    return
  }
  
  clearPendingSessionState()
  currentConversationId.value = null
  currentConversation.value = null
  messages.value = []
  queryText.value = ''
}

// 选择会话
async function selectConversation(conversationId) {
  if (currentConversationId.value === conversationId) return
  
  clearPendingSessionState()
  Object.keys(thinkingSteps).forEach(key => delete thinkingSteps[key])
  Object.keys(thinkingExpanded).forEach(key => delete thinkingExpanded[key])
  currentConversationId.value = conversationId
  messagesLoading.value = true
  
  try {
    const res = await conversationAPI.get(conversationId, { include_result_data: true })
    currentConversation.value = res.data?.conversation || null

    const rawMessages = res.data?.messages || []
    messages.value = rawMessages.map(hydrateConversationMessage)
    void hydrateResultActionContracts(messages.value)

    const activeQuerySession = res.data?.active_query_session || null
    if (activeQuerySession) {
      const snapshot = applyPendingSessionSnapshot(activeQuerySession, { preserveSelection: true })
      if (snapshot?.current_node === 'table_resolution' && isManualTableOverride(snapshot)) {
        await expandAllTables()
      }
    }
    
    // 如果会话有指定连接，自动选择
    if (currentConversation.value?.connection_id) {
      selectedConnection.value = currentConversation.value.connection_id
    }
    if (currentConversation.value?.domain_id) {
      selectedDomain.value = currentConversation.value.domain_id
    }
    
    scrollToBottom()
  } catch (e) {
    console.error('加载会话详情失败', e)
  } finally {
    messagesLoading.value = false
  }
}

// 删除会话
async function deleteConversation(conversationId) {
  try {
    await ElMessageBox.confirm(
      '确定要删除这个对话吗？删除后无法恢复。',
      '确认删除',
      {
        type: 'warning',
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        confirmButtonClass: 'el-button--danger'
      }
    )
    
    await conversationAPI.delete(conversationId)
    conversations.value = conversations.value.filter(c => c.conversation_id !== conversationId)
    
    if (currentConversationId.value === conversationId) {
      currentConversationId.value = null
      currentConversation.value = null
      messages.value = []
    }
    
    ElMessage.success('对话已删除')
  } catch (e) {
    if (e !== 'cancel') {
      console.error('删除会话失败', e)
      ElMessage.error('删除失败: ' + (e.response?.data?.detail || e.message || '未知错误'))
    }
  }
}

// 切换置顶
async function togglePin(conv) {
  try {
    await conversationAPI.update(conv.conversation_id, { is_pinned: !conv.is_pinned })
    conv.is_pinned = !conv.is_pinned
    // 重新排序会话列表
    conversations.value.sort((a, b) => {
      if (a.is_pinned !== b.is_pinned) return b.is_pinned ? 1 : -1
      return new Date(b.last_message_at || 0) - new Date(a.last_message_at || 0)
    })
  } catch (e) {
    console.error('更新置顶状态失败', e)
  }
}

// 发送消息
async function handleSendMessage() {
  if (!canSendMessage.value || interactionLocked.value) return
  
  const text = queryText.value.trim()
  if (!text) return
  const isFreshQuery = !hasActivePendingSession.value && !resultReplyContext.value?.queryId
  const confirmationModeForDisplay = isFreshQuery ? effectiveQueryConfirmationMode.value : null
  
  // 如果没有当前会话，先创建一个
  if (!currentConversationId.value) {
    try {
      const res = await conversationAPI.create({
        title: text.length > 50 ? text.substring(0, 47) + '...' : text,
        connection_id: selectedConnection.value || null,
        domain_id: selectedDomain.value || null
      })
      currentConversationId.value = res.data.conversation_id
      currentConversation.value = res.data
      conversations.value.unshift(res.data)
    } catch (e) {
      console.error('创建会话失败', e)
      return
    }
  }
  
  // 添加用户消息到列表
  const userMessage = {
    message_id: 'temp-user-' + Date.now(),
    conversation_id: currentConversationId.value,
    role: 'user',
    content: text,
    status: 'completed',
    metadata: confirmationModeForDisplay ? {
      confirmation_mode: confirmationModeForDisplay,
      confirmation_mode_label: getConfirmationModeLabel(confirmationModeForDisplay)
    } : null,
    created_at: new Date().toISOString()
  }
  messages.value.push(userMessage)

  queryText.value = ''
  // 重置输入框高度
  if (inputTextarea.value) {
    inputTextarea.value.style.height = '24px'
  }
  scrollToBottom()

  if (hasActivePendingSession.value) {
    await handlePendingSessionReply(text)
    return
  }

  if (resultReplyContext.value?.queryId) {
    await submitResultRevisionReply(text)
    return
  }

  const followupResolutionResult = await resolveFollowupContextResolution(text)
  if (followupResolutionResult?.resolution === 'need_clarification') {
    appendAssistantInfoMessage(
      followupResolutionResult.message || '请先说明你是要继续分析上一结果，还是要发起一个新问题。'
    )
    return
  }

  const confirmationModeForSend = consumeQueryConfirmationMode()
  clearPendingSessionState()
  const assistantMessageId = createAssistantPlaceholder()
  prepareAssistantPlaceholder(assistantMessageId)
  await executeQueryViaWebSocket(text, assistantMessageId, {
    confirmationMode: confirmationModeForSend,
    analysisContext: followupResolutionResult?.analysis_context || null,
    followupResolution: ['continue_on_result', 'compare_with_result'].includes(followupResolutionResult?.resolution)
      ? followupResolutionResult.resolution
      : null
  })
}

// 通过 WebSocket 执行查询
async function executeQueryViaWebSocket(text, assistantMessageId, options = {}) {
  closeWebSocket()
  
  // 确保 Token 有效（如果即将过期会自动刷新）
  const token = await tokenManager.ensureValidToken()
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  // 生产环境通常通过 Nginx 反代（HTTPS 443），不要直连后端端口（8890），否则会出现 wss://host:8890 连接被重置
  // 开发环境（如 Vite 3000）则仍可通过 VITE_BACKEND_PORT 直连后端
  const isStandardPort = !window.location.port || window.location.port === '80' || window.location.port === '443'
  const host = wsHost || (isStandardPort ? window.location.host : `${window.location.hostname}:${backendPort}`)
  const wsUrl = `${protocol}//${host}/api/query/stream`
  
  const ws = new WebSocket(wsUrl)
  wsRef.value = ws
  currentQueryId.value = options.originalQueryId || null
  
  ws.onopen = () => {
    const payload = {
      text: text || null,
      ir: options.ir || null,
      user_id: currentUser.value?.user_id || 'anonymous',
      role: currentUser.value?.role || 'viewer',
      connection_id: selectedConnection.value || null,
      domain_id: selectedDomain.value || null,
      conversation_id: currentConversationId.value,
      message_id: assistantMessageId,
      selected_table_ids: options.selectedTableIds || null,
      multi_table_mode: options.multiTableMode || null,
      original_query_id: options.originalQueryId || null,
      resume_as_new_turn: Boolean(options.resumeAsNewTurn),
      confirmation_mode: options.confirmationMode || null,
      analysis_context: options.analysisContext || null,
      followup_resolution: options.followupResolution || null,
      force_execute: Boolean(options.forceExecute),
      explain_only: explainOnly.value,
      token: token ? `Bearer ${token}` : null
    }
    ws.send(JSON.stringify(payload))
  }
  
  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data)
      void handleWebSocketMessage(data, assistantMessageId)
    } catch (e) {
      console.error('解析WebSocket消息失败', e)
    }
  }
  
  ws.onerror = (error) => {
    console.error('WebSocket 错误', error)
    updateAssistantMessage(assistantMessageId, {
      status: 'error',
      error_message: '连接错误'
    })
    loading.value = false
    progressSteps.value = []
  }
  
  ws.onclose = () => {
    // 如果连接关闭时消息还在 running 状态，说明发生了异常
    const msg = findMessage(assistantMessageId)
    if (msg && (msg.status === 'running' || msg.status === 'pending')) {
      updateAssistantMessage(assistantMessageId, {
        status: 'error',
        error_message: '连接意外关闭，请重试'
      })
    }
    loading.value = false
    progressSteps.value = []
  }
}

// 处理 WebSocket 消息
async function handleWebSocketMessage(data, assistantMessageId) {
  const event = data.event
  const payload = data.payload
  
  if (data.query_id) {
    currentQueryId.value = data.query_id
    const currentMsg = findMessage(assistantMessageId)
    if (currentMsg) {
      currentMsg.query_id = data.query_id
    }
  }
  
  // 如果响应中包含 message_id，使用真实的 message_id（后端创建的占位消息ID）
  // 这样前端就能正确调用停止接口
  if (payload?.message_id) {
    const realMessageId = payload.message_id
    // 更新消息ID映射
    const tempMsg = findMessage(assistantMessageId)
    if (tempMsg) {
      tempMsg.message_id = realMessageId
      tempMsg.query_id = data.query_id || tempMsg.query_id
      // 更新 currentStreamingMessageId
      if (currentStreamingMessageId.value === assistantMessageId) {
        currentStreamingMessageId.value = realMessageId
      }
    }
    assistantMessageId = realMessageId
  }
  
  switch (event) {
    case 'progress':
      currentProgressText.value = payload.description || getProgressStepText(payload.step)
      updateAssistantMessage(assistantMessageId, { status: 'running' })
      // 更新进度步骤
      updateProgressStep(payload.step, payload.status || 'running', payload.description)
      if (payload.step === 'table_selection') {
        const currentMsg = findMessage(assistantMessageId)
        if (currentMsg?.query_id) {
          void loadResultSessionSnapshot(currentMsg.query_id, { force: true })
        }
      }
      break
    
    case 'thinking':
      // 思考过程流式输出（类似 Deep Research 效果）
      updateThinkingStep(assistantMessageId, payload.step, payload.content, payload.done, payload.step_status)
      updateAssistantMessage(assistantMessageId, { status: 'running' })
      if (payload.step === 'table_selection') {
        const currentMsg = findMessage(assistantMessageId)
        if (currentMsg?.query_id) {
          void loadResultSessionSnapshot(currentMsg.query_id, { force: true })
        }
      }
      break
      
    case 'confirm':
      // 需要确认
      hideAssistantPlaceholder(assistantMessageId)
      pendingQueryText.value = queryText.value || payload.query_text || ''
      originalQueryId.value = data.query_id || currentQueryId.value || null
      if (!(await loadQuerySessionSnapshot(originalQueryId.value))) {
        appendAssistantInfoMessage('确认阶段状态加载失败，请重试。')
      }
      loading.value = false
      progressSteps.value = []
      break
      
    case 'table_selection':
      // 表选择
      hideAssistantPlaceholder(assistantMessageId)
      originalQueryId.value = data.query_id || null
      // 保存原始查询文本，确认时需要用
      pendingQueryText.value = payload.query_text || queryText.value || ''
      if (!(await loadQuerySessionSnapshot(originalQueryId.value))) {
        appendAssistantInfoMessage('选表阶段状态加载失败，请重试。')
      }
      loading.value = false
      progressSteps.value = []
      break
      
    case 'result':
      // 接收到结果，清除进度流程
      clearPendingSessionState()
      progressSteps.value = []
      
      if (payload.result) {
        // 将二维数组 rows 转换为对象数组，方便模板访问
        const columns = payload.result.columns || []
        const rawRows = payload.result.rows || []
        const objectRows = rawRows.map(row => {
          const obj = {}
          columns.forEach((col, idx) => {
            obj[col.name] = row[idx]
          })
          return obj
        })
        
        // 检查是否有摘要，决定是否等待叙述
        const hasSummary = typeof payload.result.summary === 'string' && payload.result.summary.trim().length > 0
        narrativePending.value = !hasSummary
        
        // 如果已有摘要，立即折叠思考过程
        if (hasSummary) {
          thinkingExpanded[assistantMessageId] = false
        }
        
        updateAssistantMessage(assistantMessageId, {
          status: 'running',  // 保持 running 状态，等待叙述完成
          sql_text: payload.result.meta?.sql || payload.result.sql,
          result_summary: hasSummary ? payload.result.summary : undefined,
          result_data: {
            columns: columns,
            rows: objectRows,
            visualization_hint: payload.result.visualization_hint,
            meta: payload.result.meta
          }
        })
        
        // 重置叙述缓冲
        narrativeBuffer.value = hasSummary ? payload.result.summary : ''
        currentStreamingMessageId.value = assistantMessageId
        
        loading.value = false
      }
      break
      
    case 'narrative':
      // 叙述流式生成
      currentStreamingMessageId.value = assistantMessageId
      
      // 叙述开始时立即折叠思考过程（无论是否有chunk）
      if (!narrativeStreaming.value && !payload.done) {
        // 首次收到叙述事件，立即折叠思考过程
        thinkingExpanded[assistantMessageId] = false
      }
      
      narrativeStreaming.value = !payload.done
      
      if (payload.chunk) {
        narrativeBuffer.value += payload.chunk
        const msg = findMessage(assistantMessageId)
        if (msg) {
          msg.result_summary = narrativeBuffer.value
        }
      }
      
      // 叙述完成
      if (payload.done) {
        narrativeStreaming.value = false
        narrativePending.value = false
        const msg = findMessage(assistantMessageId)
        if (msg) {
          msg.result_summary = narrativeBuffer.value || msg.result_summary
        }
      }
      
      scrollToBottom()
      break
      
    case 'completed':
      // 查询完成，重置所有状态
      clearPendingSessionState()
      loading.value = false
      progressSteps.value = []
      narrativeStreaming.value = false
      narrativePending.value = false
      
      // 检查是否有错误状态（SQL 执行失败等情况）
      if (payload.status === 'error' || payload.error) {
        updateAssistantMessage(assistantMessageId, {
          status: 'error',
          error_message: payload.error?.message || payload.error?.details || '查询执行失败'
        })
        const currentMsg = findMessage(assistantMessageId)
        if (currentMsg?.query_id) {
          void loadResultSessionSnapshot(currentMsg.query_id, { force: true })
        }
        // 保存思考步骤到消息
        saveThinkingToMessage(assistantMessageId)
        scrollToBottom()
        break
      }
      
      if (payload.result) {
        // 将二维数组 rows 转换为对象数组
        const columns = payload.result.columns || []
        const rawRows = payload.result.rows || []
        const objectRows = rawRows.map(row => {
          const obj = {}
          columns.forEach((col, idx) => {
            obj[col.name] = row[idx]
          })
          return obj
        })
        
        // 获取当前消息，合并数据而不是覆盖
        const currentMsg = findMessage(assistantMessageId)
        const existingSummary = currentMsg?.result_summary || narrativeBuffer.value
        
        updateAssistantMessage(assistantMessageId, {
          status: 'completed',
          sql_text: payload.result.meta?.sql || payload.result.sql || currentMsg?.sql_text,
          result_summary: existingSummary || payload.result.summary || payload.result.explanation,
          result_data: objectRows.length > 0 ? {
            columns: columns,
            rows: objectRows,
            visualization_hint: payload.result.visualization_hint,
            meta: payload.result.meta
          } : currentMsg?.result_data
        })
        const completedMsg = findMessage(assistantMessageId)
        if (completedMsg?.query_id) {
          void loadResultSessionSnapshot(completedMsg.query_id, { force: true })
        }
      } else {
        // 没有新结果，只更新状态
        const currentMsg = findMessage(assistantMessageId)
        if (currentMsg) {
          currentMsg.status = 'completed'
          // 如果有叙述缓冲，更新摘要
          if (narrativeBuffer.value && !currentMsg.result_summary) {
            currentMsg.result_summary = narrativeBuffer.value
          }
          if (currentMsg.query_id) {
            void loadResultSessionSnapshot(currentMsg.query_id, { force: true })
          }
        }
      }
      
      // 重置叙述缓冲
      narrativeBuffer.value = ''
      currentStreamingMessageId.value = null
      
      // 保存思考步骤到消息，并收起
      saveThinkingToMessage(assistantMessageId)
      
      scrollToBottom()
      loadConversations() // 刷新会话列表
      break
      
    case 'auth_error':
      // 认证错误（如 JWT 过期），触发退出登录
      clearPendingSessionState()
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
      clearPendingSessionState()
      updateAssistantMessage(assistantMessageId, {
        status: 'error',
        error_message: payload.error?.message || '查询失败'
      })
      const errorMsg = findMessage(assistantMessageId)
      if (errorMsg?.query_id) {
        void loadResultSessionSnapshot(errorMsg.query_id, { force: true })
      }
      loading.value = false
      narrativeStreaming.value = false
      progressSteps.value = []
      break

    case 'cancelled':
      // 后端确认取消，更新状态（保留已输出的内容）
      clearPendingSessionState()
      const cancelledMsg = findMessage(assistantMessageId)
      if (cancelledMsg) {
        cancelledMsg.status = 'cancelled'
        cancelledMsg.is_stopping = false
      }
      loading.value = false
      narrativeStreaming.value = false
      progressSteps.value = []
      closeWebSocket()
      break
  }
  
  scrollToBottom()
}

// 更新助手消息
function updateAssistantMessage(messageId, updates) {
  const msg = findMessage(messageId)
  if (msg) {
    Object.assign(msg, updates)
    // 如果有 SQL，默认展开显示
    if (updates.sql_text && !expandedSql[messageId]) {
      expandedSql[messageId] = true
    }
  }
}

// 查找消息
function findMessage(messageId) {
  return messages.value.find(m => m.message_id === messageId)
}

async function continueQueryFromPendingState({
  text,
  ir = null,
  queryId,
  selectedTableIds: continuationTableIds = [],
  multiTableMode = null,
  forceExecute = false,
  progressText = '思考中...',
  resumeAsNewTurn = false
}) {
  if ((!text && !ir) || !queryId) {
    ElMessage.error('缺少原始查询上下文，无法继续执行。')
    return
  }

  const assistantMessageId = resumeAsNewTurn
    ? createAssistantPlaceholder()
    : reuseOrCreateAssistantMessage(queryId)
  prepareAssistantPlaceholder(assistantMessageId, progressText)
  clearPendingSessionState({ keepQueryText: true })
  await executeQueryViaWebSocket(text, assistantMessageId, {
    ir,
    selectedTableIds: continuationTableIds,
    multiTableMode,
    originalQueryId: queryId,
    forceExecute,
    resumeAsNewTurn
  })
}

async function continueQueryFromResumeDirective(resumeDirective = {}, { resumeAsNewTurn = false } = {}) {
  if (!resumeDirective?.should_resume) return

  await continueQueryFromPendingState({
    text: resumeDirective.text || pendingQueryText.value,
    ir: resumeDirective.ir || null,
    queryId: resumeDirective.query_id || pendingSessionSnapshot.value?.query_id || originalQueryId.value,
    selectedTableIds: resumeDirective.selected_table_ids || selectedTableIds.value,
    multiTableMode: resumeDirective.multi_table_mode || pendingTableSelection.value?.multi_table_mode || null,
    forceExecute: Boolean(resumeDirective.force_execute),
    progressText: resumeDirective.progress_text || '思考中...',
    resumeAsNewTurn
  })
}

async function startFreshQueryFromPendingReply(text) {
  const assistantMessageId = createAssistantPlaceholder()
  prepareAssistantPlaceholder(assistantMessageId)
  clearPendingSessionState()
  await executeQueryViaWebSocket(text, assistantMessageId, {})
}

async function submitPendingSessionAction({
  semanticAction = null,
  actionType = null,
  payload = {},
  naturalLanguageReply = null,
  preserveSelection = false
} = {}) {
  const queryId = pendingSessionSnapshot.value?.query_id || originalQueryId.value
  if (!queryId) {
    ElMessage.error('未找到待确认的查询会话。')
    return null
  }

  pendingSessionActionLoading.value = true
  try {
    const resolvedActionType = semanticAction ? getPendingActionBinding(semanticAction) : actionType
    const res = await querySessionAPI.submitAction(queryId, {
      action_type: resolvedActionType,
      payload,
      natural_language_reply: naturalLanguageReply,
      draft_version: pendingSessionState.value.draft_version || 1,
      actor_type: 'user',
      actor_id: currentUser.value?.user_id || 'anonymous',
      idempotency_key: buildIdempotencyKey(resolvedActionType || semanticAction || 'reply')
    })

    return await handleSessionActionResult(res.data, {
      preserveSelection,
      naturalLanguageReply
    })
  } catch (e) {
    console.error('提交确认动作失败', e)
    ElMessage.error(e.response?.data?.detail || e.message || '提交确认动作失败')
    return null
  } finally {
    pendingSessionActionLoading.value = false
  }
}

async function handlePendingSessionReply(text) {
  await submitPendingSessionAction({
    naturalLanguageReply: text,
    preserveSelection: true
  })
}

async function handleSessionActionResult(result, {
  preserveSelection = false,
  naturalLanguageReply = null
} = {}) {
  if (result?.resolution === 'need_clarification') {
    appendAssistantInfoMessage(result.message)
    return result
  }

  if (result?.resolution === 'resolved_to_new_query') {
    const nextQueryText = result.new_query_text || naturalLanguageReply
    if (nextQueryText) {
      await startFreshQueryFromPendingReply(nextQueryText)
    }
    return result
  }

  if (result?.action?.action_type) {
    clearResultReplyContext()
  }

  const sessionSnapshot = cacheResultSessionSnapshot(result?.session)
  const snapshot = applyPendingSessionSnapshot(sessionSnapshot || result?.session, { preserveSelection })

  if (result?.resume_directive?.should_resume) {
    await continueQueryFromResumeDirective(result.resume_directive, {
      resumeAsNewTurn: Boolean(naturalLanguageReply)
    })
    return result
  }

  if (result?.action?.action_type === 'change_table') {
    if (isManualTableOverride(snapshot)) {
      await expandAllTables()
    } else if (visibleCandidates.value.length === 0) {
      await expandAllTables()
    }
    return result
  }

  if (result?.action?.action_type === 'request_explanation') {
    appendAssistantInfoMessage(buildPendingExplanation(snapshot))
    return result
  }

  if (result?.action?.action_type === 'revise') {
    appendAssistantInfoMessage('修改意见已记录，请继续确认。')
    return result
  }

  if (result?.action?.action_type === 'exit_current') {
    const cancelReason = snapshot?.state?.cancel_reason || '已取消当前查询。'
    clearPendingSessionState()
    appendAssistantInfoMessage(cancelReason)
    return result
  }

  return result
}

async function submitResultSessionAction(msg, {
  semanticAction,
  payload = {},
  preserveSelection = false,
  idempotencyPrefix = 'result-action'
} = {}) {
  if (!msg?.query_id) {
    ElMessage.error('未找到结果对应的查询会话。')
    return null
  }

  await loadResultSessionSnapshot(msg.query_id)
  if (!canUseResultAction(msg, semanticAction)) {
    ElMessage.error('当前结果不支持该动作。')
    return null
  }

  resultActionLoadingIds[msg.query_id] = true
  try {
    const actionType = getResultActionBinding(msg, semanticAction) || semanticAction
    const res = await querySessionAPI.submitAction(msg.query_id, {
      action_type: actionType,
      payload,
      draft_version: getResultSessionSnapshot(msg.query_id)?.state?.draft_version || 1,
      actor_type: 'user',
      actor_id: currentUser.value?.user_id || 'anonymous',
      idempotency_key: buildIdempotencyKey(idempotencyPrefix)
    })

    return await handleSessionActionResult(res.data, { preserveSelection })
  } catch (e) {
    console.error('提交结果态动作失败', e)
    ElMessage.error(e.response?.data?.detail || e.message || '提交结果态动作失败')
    return null
  } finally {
    delete resultActionLoadingIds[msg.query_id]
  }
}

function startResultRevision(msg) {
  if (!msg?.query_id || !canUseResultAction(msg, 'revise')) return
  resultReplyContext.value = {
    queryId: msg.query_id,
    messageId: msg.message_id,
  }
  focusPendingReplyInput('请修改为：')
}

async function submitResultRevisionReply(text) {
  const queryId = resultReplyContext.value?.queryId
  if (!queryId) return null

  const result = await submitResultSessionAction(
    { query_id: queryId },
    {
      semanticAction: 'revise',
      payload: { text },
      preserveSelection: true,
      idempotencyPrefix: 'result-revise'
    }
  )

  if (result) {
    clearResultReplyContext()
  }
  return result
}

async function requestTableReselection() {
  await submitPendingSessionAction({
    actionType: 'change_table',
    payload: { reason: '用户点击不是这张表' },
    preserveSelection: false
  })
}

async function requestManualTableSelection() {
  if (allAccessibleTables.value.length > 0) {
    resetAllTablesFilter()
    showAllAccessibleTables.value = true
    return
  }

  await expandAllTables()
}

async function approveExecution() {
  await submitPendingSessionAction({
    semanticAction: 'approve_execution',
    payload: { decision: 'approve' },
    preserveSelection: true
  })
}

async function confirmDraftRevision() {
  await submitPendingSessionAction({
    semanticAction: 'confirm_draft',
    payload: {
      selected_table_ids: selectedTableIds.value,
      multi_table_mode: pendingTableSelection.value?.multi_table_mode || null
    },
    preserveSelection: true
  })
}

async function cancelPendingSession() {
  if (hasActivePendingSession.value) {
    await submitPendingSessionAction({
      semanticAction: 'cancel_query',
      payload: { mode: 'cancel', source_text: '用户取消当前查询' },
      preserveSelection: true
    })
    return
  }

  clearPendingSessionState()
}

// 停止查询（使用 Redis 停止信号）
async function stopQuery() {
  // 优先使用 currentStreamingMessageId 精确定位，否则查找第一个 running/pending 的消息
  const messageId = currentStreamingMessageId.value ||
    messages.value.find(m =>
      m.role === 'assistant' && (m.status === 'running' || m.status === 'pending')
    )?.message_id

  if (!messageId) {
    // 没有消息 ID，直接关闭 WebSocket
    closeWebSocket()
    loading.value = false
    narrativeStreaming.value = false
    return
  }

  // 标记为"正在停止"状态（不立即标记为 cancelled，保留已输出的内容）
  const msg = findMessage(messageId)
  if (msg) {
    msg.is_stopping = true  // 添加停止中标记，不改变 status
  }

  // 调用停止接口（使用 Redis 停止信号）
  try {
    await conversationAPI.stopMessage(messageId)
  } catch (e) {
    // 404 表示消息不存在或已完成，这是正常情况
    if (e.response?.status !== 404) {
      console.warn('停止消息失败', e)
    }
  }

  // 设置超时：如果 3 秒内没有收到后端确认，强制标记为已取消
  setTimeout(() => {
    const msg = findMessage(messageId)
    if (msg && msg.is_stopping && msg.status !== 'cancelled') {
      msg.status = 'cancelled'
      msg.is_stopping = false
      loading.value = false
      narrativeStreaming.value = false
      progressSteps.value = []
      closeWebSocket()
    }
  }, 3000)
}

// 关闭 WebSocket
function closeWebSocket() {
  if (wsRef.value) {
    try {
      wsRef.value.close()
    } catch {}
    wsRef.value = null
  }
}

// 设置查询文本
function setQueryText(text) {
  queryText.value = text
  inputTextarea.value?.focus()
}

function focusPendingReplyInput(seedText = '') {
  if (seedText && !queryText.value.trim()) {
    queryText.value = seedText
  }
  nextTick(() => {
    inputTextarea.value?.focus()
  })
}

// 自动调整文本框高度
function autoResizeTextarea() {
  const textarea = inputTextarea.value
  if (textarea) {
    // 重置高度以获取正确的 scrollHeight
    textarea.style.height = 'auto'
    // 最小 24px，最大 200px
    const newHeight = Math.max(24, Math.min(textarea.scrollHeight, 200))
    textarea.style.height = newHeight + 'px'
  }
}

// 滚动到底部
function scrollToBottom() {
  nextTick(() => {
    if (messagesContainer.value) {
      messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight
    }
  })
}

// 切换 SQL 展开
function toggleSqlExpand(messageId) {
  expandedSql[messageId] = !expandedSql[messageId]
}

// 获取进度步骤文本
function getProgressStepText(step) {
  const stepTexts = {
    'table_selection': '正在选择数据表...',
    'nl2ir': '正在解析问题...',
    'compile': '正在生成SQL...',
    'execute': '正在执行查询...',
    'narrative': '正在生成分析...'
  }
  return stepTexts[step] || '处理中...'
}

// 更新进度步骤
function updateProgressStep(stepName, status, description) {
  const stepLabels = {
    'table_selection': '选择数据表',
    'nl2ir': '解析问题',
    'compile': '生成SQL',
    'execute': '执行查询',
    'narrative': '生成分析'
  }
  
  const existingIndex = progressSteps.value.findIndex(s => s.id === stepName)
  const stepData = {
    id: stepName,
    step: stepLabels[stepName] || stepName,
    status: status,
    description: description
  }
  
  if (existingIndex >= 0) {
    progressSteps.value[existingIndex] = stepData
  } else {
    progressSteps.value.push(stepData)
  }
}

// ==================== 思考过程相关方法 ====================
// 获取步骤标签
function getStepLabel(step) {
  const labels = {
    'table_selection': '数据表选择',
    'nl2ir': '问题解析',
    'compile': 'SQL生成',
    'execute': '执行查询',
    'narrative': '生成分析'
  }
  return labels[step] || step
}

// 更新思考步骤
function updateThinkingStep(messageId, stepName, content, done, stepStatus) {
  if (!thinkingSteps[messageId]) {
    thinkingSteps[messageId] = []
  }
  
  // 查找是否已有该步骤
  const existingIndex = thinkingSteps[messageId].findIndex(s => s.step === stepName)
  
  const stepData = {
    step: stepName,
    content: content,
    done: done,
    status: stepStatus || 'started'
  }
  
  if (existingIndex >= 0) {
    // 如果已有内容，追加新内容
    if (!done && thinkingSteps[messageId][existingIndex].content !== content) {
      thinkingSteps[messageId][existingIndex].content = content
    } else if (done) {
      thinkingSteps[messageId][existingIndex] = stepData
    }
  } else {
    thinkingSteps[messageId].push(stepData)
  }
  
  // 查询进行中时自动展开
  thinkingExpanded[messageId] = true
  
  scrollToBottom()
}

// 获取消息的思考步骤
function getThinkingSteps(messageId) {
  return thinkingSteps[messageId] || []
}

// 检查消息是否有思考步骤
function hasThinkingSteps(msg) {
  return (thinkingSteps[msg.message_id] || []).length > 0
}

// 检查消息是否有任何内容（用于 cancelled 状态保留已输出内容）
function hasAnyContent(msg) {
  return hasThinkingSteps(msg) ||
         msg.result_summary ||
         msg.sql_text ||
         msg.result_data?.rows?.length > 0
}

// 切换思考过程展开/收起
function toggleThinkingExpand(messageId) {
  thinkingExpanded[messageId] = !thinkingExpanded[messageId]
}

// 检查思考过程是否展开
function isThinkingExpanded(messageId) {
  return thinkingExpanded[messageId] !== false  // 默认展开
}

// 保存思考步骤到消息（查询完成时调用）
function saveThinkingToMessage(messageId) {
  const msg = findMessage(messageId)
  if (msg && thinkingSteps[messageId]?.length > 0) {
    msg.thinking_steps = [...thinkingSteps[messageId]]
    // 查询完成后默认收起思考过程
    thinkingExpanded[messageId] = false
  }
}

// 描述进度状态
function describeProgressStatus(status) {
  const statusMap = {
    'pending': '等待中',
    'running': '进行中...',
    'completed': '已完成',
    'error': '失败'
  }
  return statusMap[status] || status
}

// ==================== 表选择相关方法 ====================
const visibleCandidates = computed(() => {
  if (!pendingTableSelection.value?.candidates) return []
  const rejectedTableIds = new Set(pendingSessionState.value.rejected_table_ids || [])
  const candidates = pendingTableSelection.value.candidates.filter(
    candidate => !rejectedTableIds.has(candidate.table_id)
  )
  const start = tableBatchIndex.value * tableBatchSize.value
  return candidates.slice(start, start + tableBatchSize.value)
})

const totalBatches = computed(() => {
  if (!pendingTableSelection.value?.candidates) return 0
  const rejectedTableIds = new Set(pendingSessionState.value.rejected_table_ids || [])
  const availableCandidates = pendingTableSelection.value.candidates.filter(
    candidate => !rejectedTableIds.has(candidate.table_id)
  )
  return Math.ceil(availableCandidates.length / tableBatchSize.value)
})

const hasPrevBatch = computed(() => tableBatchIndex.value > 0)
const hasNextBatch = computed(() => tableBatchIndex.value < totalBatches.value - 1)

function prevTableBatch() {
  if (hasPrevBatch.value) tableBatchIndex.value--
}

function nextTableBatch() {
  if (hasNextBatch.value) tableBatchIndex.value++
}

function isTableSelected(tableId) {
  return selectedTableIds.value.includes(tableId)
}

function toggleTableSelection(candidate) {
  const tableId = candidate.table_id
  const index = selectedTableIds.value.indexOf(tableId)
  if (index >= 0) {
    selectedTableIds.value.splice(index, 1)
  } else {
    selectedTableIds.value.push(tableId)
  }
  selectedTableId.value = selectedTableIds.value[0] || null
}

async function confirmTableSelection() {
  if (selectedTableIds.value.length === 0) return
  await submitPendingSessionAction({
    semanticAction: 'choose_table',
    payload: {
      selected_table_ids: selectedTableIds.value,
      multi_table_mode: pendingTableSelection.value?.multi_table_mode || null
    },
    preserveSelection: true
  })
}

  // 展开全部：调用 API 获取所有可访问表
async function expandAllTables() {
  if (loadingAllTables.value) return

  loadingAllTables.value = true
  try {
    const token = localStorage.getItem('token')
    if (!token) {
      console.warn('用户未登录，无法获取所有可访问表')
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
    resetAllTablesFilter()
    showAllAccessibleTables.value = true

  } catch (e) {
    console.error('获取所有可访问表失败:', e)
  } finally {
    loadingAllTables.value = false
  }
}

function resetAllTablesFilter() {
  filteredAllTables.value = [...allAccessibleTables.value]
  allTablesSearchQuery.value = ''
}

// 过滤所有表
function filterAllTables() {
  const query = allTablesSearchQuery.value.trim().toLowerCase()
  if (!query) {
    filteredAllTables.value = [...allAccessibleTables.value]
    return
  }

  filteredAllTables.value = allAccessibleTables.value.filter(table => {
    const nameMatch = (table.table_name || '').toLowerCase().includes(query)
    const descMatch = (table.description || '').toLowerCase().includes(query)
    const domainMatch = (table.domain_name || '').toLowerCase().includes(query)
    return nameMatch || descMatch || domainMatch
  })
}

// 返回推荐模式
function backToRecommendTables() {
  showAllAccessibleTables.value = false
  resetAllTablesFilter()
}

// 通过 table_id 切换选择（用于展开全部模式）
function toggleTableSelectionById(tableId) {
  const isSelected = selectedTableIds.value.includes(tableId)
  if (isSelected) {
    selectedTableIds.value = selectedTableIds.value.filter(id => id !== tableId)
  } else {
    selectedTableIds.value = [...selectedTableIds.value, tableId]
  }
  selectedTableId.value = selectedTableIds.value[0] || null
}

// ==================== 消息显示辅助方法 ====================
// 判断是否正在流式输出叙述
function isNarrativeStreaming(msg) {
  return narrativeStreaming.value && currentStreamingMessageId.value === msg.message_id
}

// 判断是否可以显示结果详情（叙述完成后）
function canShowResultDetails(msg) {
  // 如果消息已完成，显示所有内容
  if (msg.status === 'completed') return true
  return Boolean(
    msg?.result_summary ||
    msg?.sql_text ||
    msg?.result_data?.columns?.length ||
    msg?.result_data?.rows?.length ||
    msg?.result_data?.meta?.explain_only
  )
}

async function reopenTableSelectionForMessage(msg) {
  if (!msg?.query_id) return

  try {
    await submitResultSessionAction(msg, {
      semanticAction: 'change_table',
      payload: { reason: '用户在结果态点击重新选表' },
      preserveSelection: false,
      idempotencyPrefix: 'result-change-table'
    })
    scrollToBottom()
  } catch (e) {
    console.error('重新进入选表阶段失败', e)
    ElMessage.error(e.response?.data?.detail || e.message || '重新进入选表阶段失败')
  }
}

// 判断是否有表格数据
function hasTableData(msg) {
  return msg.result_data?.rows?.length > 0
}

// 视图模式管理
const viewModes = reactive({})  // message_id -> 'table' | 'chart'
function getViewMode(msg) {
  return viewModes[msg.message_id] || 'table'
}
function setViewMode(messageId, mode) {
  viewModes[messageId] = mode
}

// 格式化时间
function formatDuration(ms) {
  if (!ms) return ''
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}min`
}

// ==================== 图表相关方法 ====================
function canShowChart(msg) {
  if (!msg.result_data?.rows?.length || !msg.result_data?.columns?.length) return false
  if (msg.result_data.rows.length < 2) return false
  
  // 检查 visualization_hint
  const hint = msg.result_data?.visualization_hint
  if (hint === 'table') return false
  
  return true
}

function getChartType(msg) {
  if (chartTypes[msg.message_id]) return chartTypes[msg.message_id]
  // 根据 visualization_hint 推断
  const hint = msg.result_data?.visualization_hint
  if (hint === 'line' || hint === 'trend') return 'line'
  if (hint === 'pie' || hint === 'proportion') return 'pie'
  return 'bar'
}

function setChartType(messageId, type) {
  chartTypes[messageId] = type
  // 切换图表类型时重置图例状态
  legendFirstClick[messageId] = false
  delete legendSelected[messageId]
}

// 处理图例点击事件
function handleLegendSelect(messageId, params) {
  const { name, selected } = params
  
  // 首次点击：只保留当前点击的系列
  if (!legendFirstClick[messageId]) {
    legendFirstClick[messageId] = true
    // 设置只选中当前点击的系列
    const newSelected = {}
    Object.keys(selected).forEach(key => {
      newSelected[key] = (key === name)
    })
    legendSelected[messageId] = newSelected
  } else {
    // 后续点击：正常切换
    legendSelected[messageId] = { ...selected }
  }
}

function getChartOption(msg) {
  if (!canShowChart(msg)) return null
  
  const vizType = getChartType(msg)
  const columns = msg.result_data.columns
  const allRows = msg.result_data.rows
  
  // 过滤掉合计行
  const rows = allRows.filter(row => {
    const firstCell = String(row[columns[0]?.name] || '').trim()
    return !firstCell.includes('合计') && !firstCell.includes('**合计**')
  })
  
  if (rows.length === 0) return null
  
  // 找到第一个文本列作为维度
  const dimCol = columns.find(c => {
    const val = rows[0]?.[c.name]
    return typeof val === 'string' && isNaN(parseFloat(val))
  }) || columns[0]
  
  const categories = rows.map(row => String(row[dimCol.name] || ''))
  
  // 数值列（跳过维度列）
  const valueColumns = columns.filter(c => {
    if (c.name === dimCol.name) return false
    const val = rows[0]?.[c.name]
    return typeof val === 'number' || !isNaN(parseFloat(val))
  })
  
  if (valueColumns.length === 0) return null
  
  if (vizType === 'pie') {
    // 饼图：只显示第一个数值列
    const valueCol = valueColumns[0]
    return {
      tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      legend: { orient: 'vertical', right: 10, top: 'center' },
      series: [{
        type: 'pie',
        radius: '60%',
        data: rows.map(row => ({
          name: String(row[dimCol.name] || ''),
          value: parseFloat(row[valueCol.name]) || 0
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
  } else if (vizType === 'bar') {
    // 柱状图
    const legendNames = valueColumns.map(c => c.label || c.name)
    return {
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      legend: { 
        data: legendNames, 
        top: 10,
        selected: legendSelected[msg.message_id] || undefined
      },
      grid: { left: '3%', right: '4%', bottom: '15%', top: 60, containLabel: true },
      xAxis: {
        type: 'category',
        data: categories,
        axisLabel: { interval: 0, rotate: categories.length > 5 ? 30 : 0 }
      },
      yAxis: { type: 'value' },
      series: valueColumns.map(col => ({
        name: col.label || col.name,
        type: 'bar',
        data: rows.map(row => parseFloat(row[col.name]) || 0)
      }))
    }
  } else if (vizType === 'line') {
    // 折线图
    const legendNames = valueColumns.map(c => c.label || c.name)
    return {
      tooltip: { trigger: 'axis' },
      legend: { 
        data: legendNames, 
        top: 10,
        selected: legendSelected[msg.message_id] || undefined
      },
      grid: { left: '3%', right: '4%', bottom: '15%', top: 60, containLabel: true },
      xAxis: {
        type: 'category',
        data: categories,
        boundaryGap: false
      },
      yAxis: { type: 'value' },
      series: valueColumns.map(col => ({
        name: col.label || col.name,
        type: 'line',
        smooth: true,
        data: rows.map(row => parseFloat(row[col.name]) || 0)
      }))
    }
  }
  
  return null
}

// ==================== 导出和复制方法 ====================
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

// 清理 Markdown 格式标记（如 **加粗**、*斜体*、`代码` 等）
function stripMarkdown(text) {
  if (typeof text !== 'string') return text
  return text
    .replace(/\*\*(.+?)\*\*/g, '$1')  // **加粗** -> 加粗
    .replace(/\*(.+?)\*/g, '$1')       // *斜体* -> 斜体
    .replace(/__(.+?)__/g, '$1')       // __加粗__ -> 加粗
    .replace(/_(.+?)_/g, '$1')         // _斜体_ -> 斜体
    .replace(/`(.+?)`/g, '$1')         // `代码` -> 代码
    .replace(/~~(.+?)~~/g, '$1')       // ~~删除线~~ -> 删除线
}

function exportToCSV(msg) {
  if (!msg.result_data?.columns || !msg.result_data?.rows) return
  
  const columns = msg.result_data.columns
  const rows = msg.result_data.rows
  
  // 构建CSV内容，清理 Markdown 标记
  const header = columns.map(c => `"${stripMarkdown(c.label || c.name).replace(/"/g, '""')}"`).join(',')
  const dataRows = rows.map(row => 
    columns.map(c => {
      const val = row[c.name]
      if (val === null || val === undefined) return ''
      if (typeof val === 'string') {
        const cleanVal = stripMarkdown(val)
        return `"${cleanVal.replace(/"/g, '""')}"`
      }
      return val
    }).join(',')
  )
  
  const csvContent = [header, ...dataRows].join('\n')
  const BOM = '\uFEFF'
  const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' })
  
  const link = document.createElement('a')
  link.href = URL.createObjectURL(blob)
  link.download = `query_result_${new Date().toISOString().slice(0, 10)}.csv`
  link.click()
  URL.revokeObjectURL(link.href)
}

// ==================== 表格显示方法 ====================
function getDisplayRows(msg) {
  const rows = msg.result_data?.rows || []
  const maxRows = expandedRows[msg.message_id] ? rows.length : 10
  return rows.slice(0, maxRows)
}

function getMaxDisplayRows(msg) {
  return expandedRows[msg.message_id] ? (msg.result_data?.rows?.length || 0) : 10
}

function toggleShowAllRows(messageId) {
  expandedRows[messageId] = !expandedRows[messageId]
}

function formatCellValue(val) {
  if (val === null || val === undefined) return '-'
  if (typeof val === 'number') {
    // 数字格式化（保留原值，不做大数字缩写）
    if (!Number.isInteger(val)) {
      return val.toFixed(2)
    }
    return val.toString()
  }
  // 字符串：支持简单 Markdown 渲染
  let str = String(val)
  // 加粗：**text** 或 __text__
  str = str.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
  str = str.replace(/__(.+?)__/g, '<strong>$1</strong>')
  // 斜体：*text* 或 _text_（避免与加粗冲突，仅处理单个）
  str = str.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>')
  // 代码：`code`
  str = str.replace(/`(.+?)`/g, '<code>$1</code>')
  return str
}

// 获取状态文本
function getStatusText(status) {
  const statusTexts = {
    'pending': '等待处理...',
    'running': '正在执行...'
  }
  return statusTexts[status] || '处理中...'
}

// 格式化时间
function formatTime(isoString) {
  if (!isoString) return ''
  const date = new Date(isoString)
  const now = new Date()
  const diff = now - date
  
  if (diff < 60000) return '刚刚'
  if (diff < 3600000) return Math.floor(diff / 60000) + '分钟前'
  if (diff < 86400000) return Math.floor(diff / 3600000) + '小时前'
  if (diff < 604800000) return Math.floor(diff / 86400000) + '天前'
  
  return date.toLocaleDateString()
}

// ==================== 登录相关 ====================
function openLoginDialog() {
  router.push('/login')
}

function handleLogout() {
  localStorage.removeItem('token')
  localStorage.removeItem('user')
  clearPendingSessionState()
  currentUser.value = null
  isLoggedIn.value = false
  conversations.value = []
  currentConversationId.value = null
  currentConversation.value = null
  messages.value = []
  router.push('/login')
}

// 监听登录状态变化
watch(isLoggedIn, (val) => {
  if (val) {
    loadConversations()
  }
})
</script>

<style scoped>
/* ==================== 现代简洁主题 ==================== */
.chat-page {
  --bg-main: #ffffff;
  --bg-sidebar: #f9fafb;
  --bg-hover: #f3f4f6;
  --bg-active: #e5e7eb;
  --bg-input: #ffffff;
  --text-primary: #111827;
  --text-secondary: #6b7280;
  --text-muted: #9ca3af;
  --border-color: #e5e7eb;
  --accent-color: #6366f1;
  --accent-hover: #4f46e5;
  --success-color: #10b981;
  --error-color: #ef4444;
  --warning-color: #f59e0b;
  --shadow-sm: 0 1px 2px rgba(0,0,0,0.05);
  --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.1);
  --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.1);
  --radius-sm: 8px;
  --radius-md: 12px;
  --radius-lg: 16px;
  --radius-full: 9999px;
}

.chat-page {
  display: flex;
  height: 100%;
  overflow: hidden;
  background: var(--bg-main);
  color: var(--text-primary);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
}

/* ==================== 侧边栏 ==================== */
.sidebar {
  width: 260px;
  background: var(--bg-sidebar);
  border-right: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
  transition: width 0.2s ease;
}

.sidebar.collapsed {
  width: 56px;
}

.sidebar-header {
  padding: 12px;
  display: flex;
  align-items: center;
  gap: 8px;
}

.new-chat-btn {
  flex: 1;
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 14px;
  background: var(--bg-main);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.15s;
}

.new-chat-btn:hover {
  background: var(--bg-hover);
  border-color: var(--text-muted);
}

.new-chat-btn svg {
  color: var(--text-secondary);
}

.toggle-btn {
  padding: 10px;
  background: none;
  border: none;
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.15s;
}

.toggle-btn:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
}

.sidebar-content {
  flex: 1;
  overflow-y: auto;
  padding: 8px 12px;
}

.conversations-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.loading-placeholder,
.empty-placeholder {
  padding: 40px 16px;
  text-align: center;
  color: var(--text-muted);
  font-size: 13px;
}

.spinner {
  width: 24px;
  height: 24px;
  border: 2px solid var(--border-color);
  border-top-color: var(--accent-color);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  margin: 0 auto;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.conversation-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: background 0.15s;
}

.conversation-item:hover {
  background: var(--bg-hover);
}

.conversation-item.active {
  background: var(--bg-active);
}

.conv-icon {
  flex-shrink: 0;
  color: var(--text-muted);
}

.conv-title {
  flex: 1;
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.conv-actions {
  display: none;
  gap: 2px;
}

.conversation-item:hover .conv-actions {
  display: flex;
}

.conv-action-btn {
  padding: 4px;
  background: none;
  border: none;
  border-radius: 4px;
  color: var(--text-muted);
  cursor: pointer;
  transition: all 0.15s;
}

.conv-action-btn:hover {
  background: var(--bg-active);
  color: var(--text-primary);
}

.conv-action-btn.delete:hover {
  color: var(--error-color);
}

.sidebar-footer {
  padding: 12px;
  border-top: 1px solid var(--border-color);
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.footer-link {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  color: var(--text-secondary);
  text-decoration: none;
  font-size: 14px;
  border-radius: var(--radius-sm);
  transition: all 0.15s;
}

.footer-link:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
}

.user-section {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  margin-top: 8px;
  border-top: 1px solid var(--border-color);
  padding-top: 16px;
}

.user-info {
  flex: 1;
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 0;
}

.user-avatar {
  width: 32px;
  height: 32px;
  background: linear-gradient(135deg, var(--accent-color), #8b5cf6);
  border-radius: var(--radius-full);
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  font-size: 13px;
  font-weight: 600;
  flex-shrink: 0;
}

.user-name {
  font-size: 14px;
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.logout-btn {
  padding: 6px;
  background: none;
  border: none;
  border-radius: 6px;
  color: var(--text-muted);
  cursor: pointer;
  transition: all 0.15s;
}

.logout-btn:hover {
  background: var(--bg-hover);
  color: var(--error-color);
}

/* ==================== 主聊天区域 ==================== */
.chat-main {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
  background: var(--bg-main);
}

/* 移动端顶部栏 */
.mobile-header {
  display: none;
  align-items: center;
  gap: 12px;
  padding: 12px 16px;
  background: var(--bg-main);
  border-bottom: 1px solid var(--border-color);
  position: sticky;
  top: 0;
  z-index: 50;
}

.mobile-menu-btn,
.mobile-new-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  background: none;
  border: none;
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.15s;
}

.mobile-menu-btn:hover,
.mobile-new-btn:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
}

.mobile-title {
  flex: 1;
  font-size: 15px;
  font-weight: 500;
  color: var(--text-primary);
  text-align: center;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

@media (max-width: 768px) {
  .mobile-header {
    display: flex;
  }
}


/* ==================== 消息区域 ==================== */
.messages-container {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
}

/* ==================== 欢迎页面 ==================== */
.welcome-screen {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 40px 24px;
}

.welcome-content {
  max-width: 640px;
  text-align: center;
}

.welcome-logo {
  margin-bottom: 32px;
}

.welcome-logo svg {
  width: 56px;
  height: 56px;
}

.welcome-title {
  font-size: 28px;
  font-weight: 600;
  color: var(--text-primary);
  margin: 0 0 12px;
  line-height: 1.3;
}

.welcome-subtitle {
  font-size: 16px;
  color: var(--text-secondary);
  margin: 0 0 48px;
  line-height: 1.5;
}

.quick-examples {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.example-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  background: var(--bg-input);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  cursor: pointer;
  transition: all 0.2s;
  text-align: left;
}

.example-card:hover {
  border-color: var(--accent-color);
  box-shadow: var(--shadow-md);
}

.example-card:hover svg {
  transform: translateX(4px);
}

.example-text {
  font-size: 14px;
  color: var(--text-primary);
}

.example-card svg {
  color: var(--text-muted);
  transition: transform 0.2s;
  flex-shrink: 0;
}

/* ==================== 消息列表 ==================== */
.messages-list {
  max-width: 960px;
  width: 100%;
  margin: 0 auto;
  padding: 24px;
  display: flex;
  flex-direction: column;
  gap: 32px;
}

.loading-messages {
  text-align: center;
  color: var(--text-muted);
  padding: 60px 24px;
}

.message-item {
  display: flex;
  gap: 16px;
}

.message-item.user {
  flex-direction: row-reverse;
}

.message-item.user .message-content {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
}

.message-avatar {
  width: 36px;
  height: 36px;
  border-radius: var(--radius-full);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 16px;
  flex-shrink: 0;
  background: var(--bg-hover);
}

.message-item.user .message-avatar {
  background: linear-gradient(135deg, var(--accent-color), #8b5cf6);
  color: white;
}

.message-content {
  flex: 1;
  min-width: 0;
  max-width: 860px;
}

.user-message {
  background: var(--accent-color);
  color: white;
  padding: 12px 18px;
  border-radius: var(--radius-lg) var(--radius-lg) 4px var(--radius-lg);
  font-size: 15px;
  line-height: 1.6;
  /* 气泡宽度根据文字内容自适应 */
  width: fit-content;
  max-width: 100%;
  word-break: break-word;
}

.user-message-meta {
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px solid rgba(255, 255, 255, 0.18);
  font-size: 12px;
  opacity: 0.88;
}

.assistant-message {
  font-size: 15px;
  line-height: 1.7;
}

.message-loading {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 0;
}

.typing-indicator {
  display: flex;
  gap: 4px;
}

.typing-indicator span {
  width: 6px;
  height: 6px;
  background: var(--accent-color);
  border-radius: 50%;
  animation: bounce 1.4s infinite ease-in-out both;
}

.typing-indicator span:nth-child(1) { animation-delay: -0.32s; }
.typing-indicator span:nth-child(2) { animation-delay: -0.16s; }

@keyframes bounce {
  0%, 80%, 100% { transform: scale(0); opacity: 0.4; }
  40% { transform: scale(1); opacity: 1; }
}

.loading-text {
  color: var(--text-muted);
  font-size: 14px;
}

.message-cancelled,
.message-error {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 16px;
  border-radius: var(--radius-sm);
  font-size: 14px;
}

.message-cancelled {
  background: rgba(245, 158, 11, 0.1);
  color: var(--warning-color);
}

.message-error {
  background: rgba(239, 68, 68, 0.1);
  color: var(--error-color);
}

/* ==================== 思考过程样式（类似 Deep Research） ==================== */
.thinking-section {
  margin-bottom: 16px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-md);
  overflow: hidden;
  transition: all 0.3s ease;
}

.thinking-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  background: var(--bg-hover);
  cursor: pointer;
  user-select: none;
  transition: background 0.2s;
}

.thinking-header:hover {
  background: var(--border-color);
}

.thinking-header.collapsed {
  border-bottom: none;
}

.thinking-icon {
  font-size: 14px;
}

/* 思考中的动态图标动画 */
.thinking-header:not(.collapsed) .thinking-icon {
  animation: thinking-bounce 1s ease-in-out infinite;
}

@keyframes thinking-bounce {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(-2px); }
}

.thinking-title {
  flex: 1;
  font-size: 13px;
  font-weight: 500;
  color: var(--text-secondary);
}

.thinking-toggle {
  font-size: 12px;
  color: var(--text-secondary);
  opacity: 0.7;
  transition: transform 0.2s;
}

.thinking-header.collapsed .thinking-toggle {
  transform: rotate(-90deg);
}

.thinking-content {
  padding: 12px 16px;
  background: var(--bg-main);
  max-height: 400px;
  overflow-y: auto;
  /* 折叠过渡动画 */
  animation: content-expand 0.3s ease;
}

@keyframes content-expand {
  from {
    opacity: 0;
    max-height: 0;
    padding: 0 14px;
  }
  to {
    opacity: 1;
    max-height: 400px;
    padding: 12px 14px;
  }
}

/* 折叠状态的思考区域样式 */
.thinking-section.collapsed .thinking-content {
  animation: content-collapse 0.3s ease forwards;
}

@keyframes content-collapse {
  from {
    opacity: 1;
    max-height: 400px;
  }
  to {
    opacity: 0;
    max-height: 0;
    padding: 0 14px;
  }
}

/* Vue Transition - 思考过程滑动效果 */
.thinking-slide-enter-active,
.thinking-slide-leave-active {
  transition: all 0.3s ease;
  overflow: hidden;
}

.thinking-slide-enter-from,
.thinking-slide-leave-to {
  opacity: 0;
  max-height: 0;
  padding-top: 0;
  padding-bottom: 0;
}

.thinking-slide-enter-to,
.thinking-slide-leave-from {
  opacity: 1;
  max-height: 400px;
}

/* 折叠状态的视觉提示 */
.thinking-section.is-collapsed {
  border-color: var(--text-muted);
  opacity: 0.8;
}

.thinking-section.is-collapsed:hover {
  opacity: 1;
}

/* 思考步骤 - 循序渐进动画 */
.thinking-step {
  position: relative;
  padding-left: 32px;
  margin-bottom: 10px;
  opacity: 0;
  transform: translateY(8px);
  animation: step-fade-in 0.3s ease forwards;
}

/* 每个步骤延迟出现，形成循序渐进效果 */
.thinking-step:nth-child(1) { animation-delay: 0s; }
.thinking-step:nth-child(2) { animation-delay: 0.1s; }
.thinking-step:nth-child(3) { animation-delay: 0.2s; }
.thinking-step:nth-child(4) { animation-delay: 0.3s; }
.thinking-step:nth-child(5) { animation-delay: 0.4s; }
.thinking-step:nth-child(6) { animation-delay: 0.5s; }

@keyframes step-fade-in {
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.thinking-step:last-child {
  margin-bottom: 0;
}

/* 连接线动画 */
.thinking-step::before {
  content: '';
  position: absolute;
  left: 10px;
  top: 20px;
  bottom: -10px;
  width: 2px;
  background: linear-gradient(to bottom, var(--border-color), transparent);
  transform-origin: top;
  animation: line-grow 0.3s ease forwards;
}

@keyframes line-grow {
  from {
    transform: scaleY(0);
  }
  to {
    transform: scaleY(1);
  }
}

.thinking-step:last-child::before {
  display: none;
}

.step-header {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 4px;
}

/* 步骤指示器 - 增强动态效果 */
.step-indicator {
  position: absolute;
  left: 2px;
  width: 18px;
  height: 18px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 10px;
  border-radius: 50%;
  background: var(--bg-hover);
  color: var(--text-secondary);
  transition: all 0.3s ease;
  z-index: 1;
}

/* 完成状态 - 勾选动画 */
.step-indicator.success {
  background: rgba(34, 197, 94, 0.15);
  color: #22c55e;
  animation: check-pop 0.3s ease;
}

@keyframes check-pop {
  0% { transform: scale(0.8); }
  50% { transform: scale(1.2); }
  100% { transform: scale(1); }
}

/* 进行中状态 - 脉冲+旋转动画 */
.step-indicator.started {
  background: rgba(99, 102, 241, 0.15);
  color: var(--accent-color);
  box-shadow: 0 0 0 0 rgba(99, 102, 241, 0.4);
  animation: indicator-pulse 1.5s ease-in-out infinite, indicator-glow 2s ease-in-out infinite;
}

@keyframes indicator-pulse {
  0%, 100% { 
    transform: scale(1);
  }
  50% { 
    transform: scale(1.1);
  }
}

@keyframes indicator-glow {
  0%, 100% {
    box-shadow: 0 0 0 0 rgba(99, 102, 241, 0.4);
  }
  50% {
    box-shadow: 0 0 0 6px rgba(99, 102, 241, 0);
  }
}

.step-name {
  font-size: 13px;
  font-weight: 500;
  color: var(--text-primary);
  transition: color 0.2s;
}

/* 活跃步骤的标题高亮 */
.thinking-step.active .step-name {
  color: var(--accent-color);
}

/* 步骤内容 - 渐入动画 */
.step-content {
  font-size: 12px;
  color: var(--text-secondary);
  line-height: 1.3;
  white-space: pre-line;
  opacity: 0;
  animation: content-fade-in 0.3s ease 0.2s forwards;
}

.step-content p {
  margin: 0 0 2px 0;
  line-height: 1.3;
}

.step-content p:last-child {
  margin-bottom: 0;
}

/* 思考内容中的 Markdown 样式覆盖 - 使用 :deep() */
.step-content.markdown-body {
  font-size: 12px;
  line-height: 1.3;
}

.step-content.markdown-body :deep(p) {
  margin: 0 0 2px 0;
}

.step-content.markdown-body :deep(ul),
.step-content.markdown-body :deep(ol) {
  margin: 2px 0;
  padding-left: 16px;
  list-style-position: outside;
}

.step-content.markdown-body :deep(li) {
  margin: 1px 0;
}

@keyframes content-fade-in {
  to {
    opacity: 1;
  }
}

.step-content code {
  font-family: 'Fira Code', 'Monaco', monospace;
  font-size: 11px;
  background: var(--bg-hover);
  padding: 2px 6px;
  border-radius: 3px;
}

.step-content pre {
  margin: 8px 0;
  padding: 10px;
  background: var(--bg-hover);
  border-radius: var(--radius-sm);
  overflow-x: auto;
}

.step-content pre code {
  background: none;
  padding: 0;
}

.thinking-step.active .step-content {
  color: var(--text-primary);
}

/* 活跃步骤的内容区域背景 */
.thinking-step.active {
  background: linear-gradient(90deg, rgba(99, 102, 241, 0.05) 0%, transparent 100%);
  margin-left: -16px;
  padding-left: 48px;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-right: 8px;
  border-radius: var(--radius-sm);
}

/* 活跃步骤的指示器位置补偿 */
.thinking-step.active .step-indicator {
  left: 18px;
}

/* 活跃步骤的连接线位置补偿 */
.thinking-step.active::before {
  left: 26px;
}

/* ==================== 叙述摘要样式 ==================== */
.summary-section {
  display: flex;
  gap: 12px;
  margin-bottom: 16px;
}

.summary-section.loading {
  opacity: 0.8;
}

.summary-icon {
  flex-shrink: 0;
  font-size: 20px;
}

.summary-content {
  flex: 1;
  color: var(--text-primary);
  line-height: 1.8;
}

.result-action-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 14px;
}

.result-action-bar-error {
  margin-top: 12px;
  margin-left: 28px;
}

.result-action-note {
  margin-top: 8px;
  color: var(--text-secondary);
  font-size: 13px;
}

.inline-session-btn {
  padding: 8px 14px;
  border-radius: 999px;
  border: 1px solid var(--border-color);
  background: var(--bg-secondary);
  color: var(--text-primary);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
}

.inline-session-btn:hover:not(:disabled) {
  background: var(--bg-hover);
}

.inline-session-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* Markdown 渲染样式 - 使用 :deep() 穿透 scoped CSS 应用到 v-html 内容 */
.markdown-body {
  font-size: 14px;
  line-height: 1.8;
}

.markdown-body :deep(h3) {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
  margin: 16px 0 8px 0;
  padding-bottom: 0;
  border-bottom: none;
}

.markdown-body :deep(h3:first-child) {
  margin-top: 0;
}

.markdown-body :deep(p) {
  margin: 0 0 12px 0;
}

.markdown-body :deep(p:last-child) {
  margin-bottom: 0;
}

.markdown-body :deep(ul),
.markdown-body :deep(ol) {
  margin: 8px 0;
  padding-left: 24px;
  list-style-position: outside;
}

.markdown-body :deep(li) {
  margin: 4px 0;
  padding-left: 4px;
}

.markdown-body :deep(code) {
  background: var(--bg-hover);
  padding: 2px 6px;
  border-radius: 4px;
  font-family: 'SF Mono', 'Monaco', 'Inconsolata', monospace;
  font-size: 13px;
}

.markdown-body :deep(pre) {
  background: var(--bg-sidebar);
  padding: 16px;
  border-radius: var(--radius-sm);
  overflow-x: auto;
  margin: 12px 0;
}

.markdown-body :deep(pre code) {
  background: none;
  padding: 0;
}

.markdown-body :deep(strong) {
  font-weight: 600;
  color: var(--text-primary);
}

.markdown-body :deep(table) {
  width: 100%;
  border-collapse: collapse;
  margin: 12px 0;
}

.markdown-body :deep(th),
.markdown-body :deep(td) {
  padding: 8px 12px;
  border: 1px solid var(--border-color);
  text-align: left;
}

.markdown-body :deep(th) {
  background: var(--bg-hover);
  font-weight: 600;
}

/* KaTeX 公式样式 */
.markdown-body :deep(.katex) {
  font-size: 1em;
}

.markdown-body :deep(.katex-display) {
  margin: 12px 0;
  overflow-x: auto;
  overflow-y: hidden;
}

.markdown-body :deep(.katex-html) {
  white-space: nowrap;
}

/* ==================== 数据区域样式 ==================== */
.data-section {
  margin-bottom: 20px;
}

.data-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  flex-wrap: wrap;
  gap: 12px;
}

.data-header h3 {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.data-header .query-result-title {
  margin: 0;
}

.data-header .query-result-title :deep(h3) {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
  display: inline;
}

.data-header-right {
  display: flex;
  align-items: center;
  gap: 16px;
}

.view-toggle {
  display: flex;
  background: var(--bg-hover);
  border-radius: var(--radius-sm);
  padding: 3px;
}

.toggle-btn {
  padding: 6px 12px;
  border: none;
  background: none;
  color: var(--text-secondary);
  font-size: 13px;
  cursor: pointer;
  border-radius: var(--radius-sm);
  transition: all 0.2s;
}

.toggle-btn.active {
  background: var(--bg-main);
  color: var(--text-primary);
  box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

.data-count {
  font-size: 13px;
  color: var(--text-secondary);
}

.latency-info {
  color: var(--success-color);
}

/* ==================== 图表视图样式 ==================== */
.chart-view {
  background: var(--bg-main);
  border-radius: var(--radius-md);
  padding: 16px;
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.chart-type-selector {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
}

.selector-label {
  font-size: 13px;
  color: var(--text-secondary);
}

.chart-type-buttons {
  display: flex;
  gap: 8px;
}

/* ==================== 加载指示器样式 ==================== */
.loading-indicator {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 16px 0;
}

@keyframes blink {
  0%, 50% { opacity: 1; }
  51%, 100% { opacity: 0; }
}

.streaming-content {
  margin-bottom: 12px;
}

.loading-indicator {
  display: flex;
  align-items: center;
  gap: 12px;
}

.loading-more {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px dashed var(--border-color);
}

.typing-indicator.small {
  transform: scale(0.7);
}

.loading-text-small {
  font-size: 12px;
  color: var(--text-secondary);
}

.sql-section.streaming {
  margin-top: 12px;
  margin-bottom: 12px;
}

.sql-section {
  margin-top: 20px;
  background: var(--bg-sidebar);
  border-radius: var(--radius-md);
  overflow: hidden;
}

.sql-header {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--text-secondary);
  font-size: 13px;
  cursor: pointer;
  padding: 12px 16px;
  background: var(--bg-hover);
  transition: all 0.15s;
}

.sql-header:hover {
  color: var(--text-primary);
  background: var(--bg-main);
}

.sql-icon {
  font-size: 14px;
}

.expand-icon {
  font-size: 10px;
}

.copy-sql-btn-inline {
  margin-left: auto;
  padding: 4px 10px;
  background: var(--bg-main);
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
}

.copy-sql-btn-inline:hover {
  color: var(--text-primary);
  border-color: var(--accent-color);
}

.sql-code-wrapper {
  position: relative;
}

.sql-code {
  background: #1a1a2e;
  color: #e4e4e7;
  padding: 16px 20px;
  margin: 0;
  font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 13px;
  line-height: 1.7;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
}

.sql-code code {
  font-family: inherit;
  color: inherit;
}

.empty-result {
  text-align: center;
  padding: 20px 16px;
  color: var(--text-secondary);
  font-size: 13px;
  background: var(--bg-hover);
  border-radius: var(--radius-sm);
}

.empty-result p {
  margin: 0;
}

/* 旧的复制按钮样式（兼容） */
.copy-sql-btn {
  position: absolute;
  top: 8px;
  right: 8px;
  padding: 6px 10px;
  background: rgba(255, 255, 255, 0.1);
  border: none;
  border-radius: 4px;
  color: #a0a0a0;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
}

.copy-sql-btn:hover {
  background: rgba(255, 255, 255, 0.2);
  color: white;
}

.result-table-section {
  margin-top: 20px;
}

.table-info {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
  font-size: 13px;
  color: var(--text-secondary);
}

.row-count {
  font-weight: 500;
}

.truncated-warning {
  color: var(--warning-color);
}

.table-wrapper {
  overflow-x: auto;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border-color);
}

.result-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.result-table th,
.result-table td {
  padding: 12px 16px;
  text-align: left;
  border-bottom: 1px solid var(--border-color);
}

.result-table th {
  background: var(--bg-hover);
  font-weight: 600;
  color: var(--text-secondary);
  white-space: nowrap;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.result-table td {
  color: var(--text-primary);
}

.result-table tr:last-child td {
  border-bottom: none;
}

.result-table tbody tr:hover {
  background: var(--bg-hover);
}

.table-more {
  padding: 12px 16px;
  text-align: center;
  color: var(--text-muted);
  font-size: 13px;
  background: var(--bg-hover);
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  border-top: 1px solid var(--border-color);
}

.show-more-btn {
  background: var(--bg-main);
  border: 1px solid var(--border-color);
  color: var(--accent-color);
  padding: 6px 14px;
  border-radius: var(--radius-sm);
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.15s;
}

.show-more-btn:hover {
  background: var(--accent-color);
  color: white;
  border-color: var(--accent-color);
}

.export-csv-btn {
  background: var(--bg-main);
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  padding: 6px 14px;
  border-radius: var(--radius-sm);
  font-size: 12px;
  cursor: pointer;
  margin-left: auto;
  transition: all 0.15s;
}

.export-csv-btn:hover {
  border-color: var(--text-muted);
  color: var(--text-primary);
}

/* ==================== 进度流程图 ==================== */
.progress-flow-wrapper {
  padding: 16px;
}

.progress-timeline {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.progress-item {
  display: flex;
  align-items: flex-start;
  gap: 12px;
}

.progress-marker {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 20px;
}

.progress-item-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: var(--border-color);
  border: 2px solid var(--bg-secondary);
}

.progress-item-dot.running {
  background: var(--primary-color);
  animation: pulse 1.5s infinite;
}

.progress-item-dot.completed {
  background: var(--success-color);
}

.progress-item-dot.error {
  background: var(--error-color);
}

@keyframes pulse {
  0%, 100% { transform: scale(1); opacity: 1; }
  50% { transform: scale(1.2); opacity: 0.7; }
}

.progress-item-line {
  width: 2px;
  height: 20px;
  background: var(--border-color);
  margin-top: 4px;
}

.progress-item-content {
  flex: 1;
}

.progress-item-title {
  font-size: 14px;
  font-weight: 500;
  color: var(--text-primary);
}

.progress-item-status {
  font-size: 12px;
  color: var(--text-muted);
  margin-top: 2px;
}

/* ==================== 确认卡 ==================== */
.confirm-box {
  background: var(--bg-hover);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.session-review-box {
  box-shadow: var(--shadow-sm);
}

.confirm-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 14px 16px;
  background: var(--bg-main);
  border-bottom: 1px solid var(--border-color);
}

.confirm-header h3 {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.confirm-icon {
  font-size: 18px;
}

.session-node-badge {
  margin-left: auto;
  padding: 4px 10px;
  border-radius: var(--radius-full);
  background: var(--bg-secondary);
  color: var(--text-secondary);
  font-size: 12px;
  font-weight: 500;
}

.confirm-content {
  padding: 16px;
}

.section-label,
.warnings-label,
.suggestions-label {
  font-size: 13px;
  color: var(--text-muted);
  margin: 0 0 8px;
}

.user-question-section,
.ai-understanding-section {
  margin-bottom: 16px;
}

.session-context-section {
  margin-bottom: 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.session-context-row {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.session-context-chip-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.session-context-chip {
  display: inline-flex;
  align-items: center;
  padding: 6px 12px;
  background: rgba(99, 102, 241, 0.08);
  color: var(--text-primary);
  border: 1px solid rgba(99, 102, 241, 0.18);
  border-radius: 999px;
  font-size: 13px;
  line-height: 1.4;
}

.draft-preview-section {
  margin-bottom: 16px;
}

.draft-preview-box {
  background: rgba(16, 185, 129, 0.08);
  border: 1px solid rgba(16, 185, 129, 0.16);
  border-radius: 8px;
  overflow: hidden;
}

.draft-preview-tip {
  margin-top: 8px;
  font-size: 12px;
  color: var(--text-muted);
  line-height: 1.5;
}

.user-question-text {
  background: var(--bg-secondary);
  padding: 12px;
  border-radius: 8px;
  font-size: 14px;
}

.understanding-content {
  background: var(--bg-secondary);
  border-radius: 8px;
  overflow: hidden;
}

.understanding-list {
  margin: 0;
  padding: 12px 12px 12px 32px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.understanding-item {
  color: var(--text-primary);
  font-size: 14px;
  line-height: 1.6;
}

.understanding-text {
  margin: 0;
  padding: 12px;
  font-size: 14px;
  line-height: 1.6;
  white-space: pre-wrap;
  font-family: inherit;
}

.pending-challenge-section {
  margin-bottom: 16px;
}

.pending-challenge-text {
  padding: 10px 12px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.18);
  border-radius: 8px;
  font-size: 13px;
  line-height: 1.5;
  color: var(--text-primary);
}

.warnings-section {
  margin-bottom: 16px;
}

.warning-tag {
  display: inline-block;
  background: rgba(245, 158, 11, 0.15);
  color: var(--warning-color);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 13px;
  margin: 4px 4px 4px 0;
}

.suggestions-section {
  margin-bottom: 8px;
}

.suggestions-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.suggestion-btn {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  padding: 8px 14px;
  border-radius: 8px;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
}

.suggestion-btn:hover {
  background: var(--primary-color);
  color: white;
  border-color: var(--primary-color);
}

.confirm-actions {
  display: flex;
  gap: 12px;
  padding: 16px;
  border-top: 1px solid var(--border-color);
}

.btn-confirm,
.btn-cancel,
.btn-secondary {
  flex: 1;
  padding: 12px 20px;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-confirm {
  background: var(--success-color);
  border: none;
  color: white;
}

.btn-confirm:hover:not(:disabled) {
  background: #16a34a;
}

.btn-confirm:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-secondary {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  color: var(--text-primary);
}

.btn-secondary:hover:not(:disabled) {
  background: var(--bg-tertiary);
}

.btn-cancel {
  background: none;
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
}

.btn-cancel:hover {
  background: var(--bg-tertiary);
  color: var(--text-primary);
}

.session-actions {
  flex-wrap: wrap;
}

.session-inline-status {
  margin-top: 12px;
  font-size: 13px;
  color: var(--text-muted);
}

.estimated-cost-box {
  display: grid;
  gap: 10px;
  margin-bottom: 16px;
}

.estimated-cost-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 14px;
  background: var(--bg-secondary);
  border-radius: 10px;
  font-size: 14px;
}

.estimated-cost-item strong {
  color: var(--text-primary);
}

.revision-note {
  margin-bottom: 16px;
}

/* ==================== 表选择卡 ==================== */
.table-selection-box {
  background: var(--bg-hover);
  border-radius: var(--radius-md);
  border: 1px solid var(--border-color);
  overflow: hidden;
}

.table-selection-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 14px 16px;
  background: var(--bg-main);
  border-bottom: 1px solid var(--border-color);
  flex-wrap: wrap;
}

.table-selection-header h3 {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
  color: var(--text-primary);
}

.table-selection-icon {
  font-size: 18px;
}

.table-selection-header-actions {
  margin-left: auto;
  display: flex;
  gap: 8px;
}

.btn-prev-batch,
.btn-refresh-batch,
.btn-expand-all,
.btn-collapse-all {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 12px;
  cursor: pointer;
}

.btn-prev-batch:hover,
.btn-refresh-batch:hover,
.btn-expand-all:hover,
.btn-collapse-all:hover {
  background: var(--bg-tertiary);
  color: var(--text-primary);
}

.table-selection-tip {
  padding: 8px 0 12px;
  font-size: 12px;
  color: var(--text-muted);
}

.unified-table-tip {
  margin: 0 0 8px;
}

.cross-year-tip {
  color: var(--warning-color);
}

.table-candidates {
  padding: 0;
  display: grid;
  gap: 8px;
}

.table-candidate {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  background: var(--bg-secondary);
  border-radius: 10px;
  border: 1px solid var(--border-color);
  cursor: pointer;
  transition: all 0.2s;
}

.table-candidate:hover {
  background: var(--bg-hover);
}

.table-candidate.selected {
  border-color: var(--accent-color);
  background: rgba(2, 132, 199, 0.08);
  box-shadow: none;
}

.table-candidate.is-year-table {
  border-left: 2px solid var(--text-secondary);
}

.candidate-checkbox {
  width: 18px;
  height: 18px;
  border-radius: 5px;
  border: 1.5px solid var(--border-color);
  background: var(--bg-main);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  font-size: 12px;
  color: var(--primary-color);
  transition: all 0.2s;
}

.table-candidate.selected .candidate-checkbox {
  background: var(--primary-color);
  border-color: var(--primary-color);
  color: white !important;
}

.candidate-info {
  flex: 1;
  min-width: 0;
}

.candidate-topline {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  justify-content: space-between;
}

.candidate-primary {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 4px 8px;
}

.candidate-name {
  font-size: 14px;
  font-weight: 600;
  min-width: 0;
  line-height: 1.35;
}

.candidate-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 8px;
  min-width: 0;
}

.candidate-meta-inline {
  flex: 1;
  align-items: baseline;
}

.candidate-meta-item {
  font-size: 11px;
  color: var(--text-muted);
  line-height: 1.35;
  min-width: 0;
  word-break: break-word;
}

.candidate-score {
  flex-shrink: 0;
  font-size: 11px;
  font-weight: 500;
  color: var(--text-secondary);
  padding: 2px 8px;
  background: var(--bg-main);
  border-radius: 12px;
}

/* 展开全部模式搜索框 */
.all-tables-search {
  display: flex;
  align-items: center;
  gap: 12px;
  margin: 8px 0 12px;
  padding: 10px 12px;
  background: var(--bg-secondary);
  border-radius: 10px;
  border: 1px solid var(--border-color);
}

.all-tables-search .search-input {
  flex: 1;
  padding: 8px 14px;
  font-size: 14px;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  outline: none;
  background: var(--bg-primary);
  color: var(--text-primary);
  transition: border-color 0.2s;
}

.all-tables-search .search-input:focus {
  border-color: var(--accent-color);
}

.all-tables-search .search-count {
  font-size: 13px;
  color: var(--text-muted);
  white-space: nowrap;
}

/* 展开全部模式的表列表 */
.table-candidates.all-tables-mode {
  max-height: 400px;
  overflow-y: auto;
}

/* 未找到提示 */
.no-tables-found {
  padding: 16px 12px;
  text-align: center;
  color: var(--text-muted);
  font-size: 12px;
}

/* 返回推荐按钮 */
.btn-back-to-recommend {
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  padding: 8px 16px;
  border-radius: 8px;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-back-to-recommend:hover {
  background: var(--bg-tertiary);
  color: var(--text-primary);
}

.table-selection-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  padding: 14px 0 0;
}

/* ==================== 图表区域 ==================== */
.chart-section {
  margin-top: 16px;
  border-top: 1px solid var(--border-color);
  padding-top: 16px;
}

.chart-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.chart-icon {
  font-size: 16px;
}

.chart-type-selector {
  margin-left: auto;
  display: flex;
  gap: 4px;
}

.chart-type-btn {
  background: var(--bg-hover);
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.15s;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.2s;
}

.chart-type-btn:hover {
  background: var(--bg-active);
  color: var(--text-primary);
  border-color: var(--text-muted);
}

.chart-type-btn.active {
  background: var(--accent-color);
  border-color: var(--accent-color);
  color: white;
  font-weight: 500;
}

.chart-container {
  height: 400px;
  background: var(--bg-main);
  border-radius: 8px;
  overflow: hidden;
}

/* ==================== SQL 代码样式增强 ==================== */
.sql-code-wrapper {
  position: relative;
  margin-top: 8px;
}

.copy-sql-btn {
  position: absolute;
  top: 8px;
  right: 8px;
  background: var(--bg-tertiary);
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.2s;
}

.copy-sql-btn:hover {
  background: var(--primary-color);
  color: white;
}

.message-time {
  font-size: 11px;
  color: var(--text-muted);
  margin-top: 8px;
}

.message-item.user .message-time {
  text-align: right;
}

/* ==================== 输入区域 ==================== */
.chat-input-area {
  padding: 16px 24px 24px;
  background: linear-gradient(180deg, transparent 0%, var(--bg-main) 20%);
}

.input-container {
  max-width: 960px;
  margin: 0 auto;
}

/* 配置选项栏 */
.input-options {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.option-select {
  background: var(--bg-input);
  border: 1px solid var(--border-color);
  color: var(--text-secondary);
  padding: 6px 12px;
  border-radius: var(--radius-sm);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.15s;
}

.option-select:hover {
  border-color: var(--text-muted);
  color: var(--text-primary);
}

.option-select:focus {
  outline: none;
  border-color: var(--accent-color);
}

.option-checkbox {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--text-secondary);
  cursor: pointer;
  padding: 6px 12px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-input);
  transition: all 0.15s;
}

.option-checkbox:hover {
  border-color: var(--text-muted);
  color: var(--text-primary);
}

.option-checkbox input {
  accent-color: var(--accent-color);
  width: 14px;
  height: 14px;
  margin: 0;
}

.confirmation-mode-group {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 4px;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  background: var(--bg-input);
}

.confirmation-mode-group.disabled {
  opacity: 0.6;
}

.confirmation-mode-label {
  font-size: 12px;
  color: var(--text-muted);
  padding: 0 4px;
  white-space: nowrap;
}

.confirmation-mode-btn {
  border: 0;
  background: transparent;
  color: var(--text-secondary);
  font-size: 12px;
  padding: 6px 10px;
  border-radius: 999px;
  cursor: pointer;
  transition: all 0.15s ease;
}

.confirmation-mode-btn:hover:not(:disabled) {
  background: var(--bg-hover);
  color: var(--text-primary);
}

.confirmation-mode-btn.active {
  background: rgba(99, 102, 241, 0.12);
  color: var(--accent-color);
  font-weight: 600;
}

.confirmation-mode-btn:disabled {
  cursor: not-allowed;
}

.input-wrapper {
  display: flex;
  align-items: flex-end;
  gap: 12px;
  background: var(--bg-input);
  border-radius: var(--radius-lg);
  padding: 12px 16px;
  border: 1px solid var(--border-color);
  box-shadow: var(--shadow-md);
  transition: all 0.2s;
}

.input-wrapper:focus-within {
  border-color: var(--accent-color);
  box-shadow: var(--shadow-lg), 0 0 0 3px rgba(99, 102, 241, 0.1);
}

.chat-input {
  flex: 1;
  background: none;
  border: none;
  color: var(--text-primary);
  font-size: 15px;
  line-height: 1.5;
  resize: none;
  min-height: 24px;
  max-height: 200px;
  overflow-y: auto;
}

.chat-input:focus {
  outline: none;
}

.chat-input::placeholder {
  color: var(--text-muted);
}

.input-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.send-btn,
.stop-btn {
  width: 36px;
  height: 36px;
  border: none;
  border-radius: var(--radius-sm);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.15s;
}

.send-btn {
  background: var(--accent-color);
  color: white;
}

.send-btn:hover:not(:disabled) {
  background: var(--accent-hover);
  transform: scale(1.05);
}

.send-btn:disabled {
  background: var(--bg-hover);
  color: var(--text-muted);
  cursor: not-allowed;
}

.send-btn:disabled svg {
  opacity: 0.5;
}

.stop-btn {
  background: var(--error-color);
  color: white;
}

.stop-btn:hover {
  background: #dc2626;
  transform: scale(1.05);
}

.input-hint {
  text-align: center;
  font-size: 12px;
  color: var(--text-muted);
  margin-top: 10px;
}

.message-time {
  font-size: 11px;
  color: var(--text-muted);
  margin-top: 8px;
}

.message-item.user .message-time {
  text-align: right;
}

/* ==================== 响应式 - 平板 (768px - 1024px) ==================== */
@media (max-width: 1024px) {
  .messages-list {
    max-width: 100%;
    padding: 20px;
  }
  
  .message-content {
    max-width: 100%;
  }
  
  .input-container {
    max-width: 100%;
  }
  
  .welcome-content {
    max-width: 90%;
  }
}

/* ==================== 响应式 - 移动端 (< 768px) ==================== */
@media (max-width: 768px) {
  /* 侧边栏 - 抽屉式 */
  .sidebar {
    position: fixed;
    left: 0;
    top: 0;
    bottom: 0;
    z-index: 1000;
    width: 280px;
    transform: translateX(-100%);
    transition: transform 0.3s ease;
    background: var(--bg-sidebar);
  }
  
  .sidebar.mobile-open {
    transform: translateX(0);
    box-shadow: 0 0 40px rgba(0, 0, 0, 0.3);
  }
  
  .sidebar.collapsed {
    transform: translateX(-100%);
  }
  
  /* 遮罩层 */
  .sidebar-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.5);
    z-index: 999;
  }
  
  .sidebar-overlay.visible {
    display: block;
  }
  
  /* 主聊天区域 */
  .chat-main {
    width: 100%;
  }
  
  /* 移动端菜单按钮 */
  .mobile-menu-btn {
    display: flex !important;
  }
  
  /* 消息区域 */
  .messages-container {
    padding: 0;
  }
  
  .messages-list {
    max-width: 100%;
    padding: 12px;
    gap: 20px;
  }
  
  /* 消息项 */
  .message-item {
    gap: 10px;
  }
  
  .message-avatar {
    width: 32px;
    height: 32px;
    font-size: 14px;
    flex-shrink: 0;
  }
  
  .message-content {
    max-width: calc(100% - 44px);
  }
  
  .user-message {
    padding: 10px 14px;
    font-size: 14px;
  }
  
  .assistant-message {
    font-size: 14px;
  }
  
  /* 欢迎页面 */
  .welcome-screen {
    padding: 20px 16px;
  }
  
  .welcome-content {
    max-width: 100%;
  }
  
  .welcome-logo svg {
    width: 44px;
    height: 44px;
  }
  
  .welcome-title {
    font-size: 20px;
    margin-bottom: 8px;
  }
  
  .welcome-subtitle {
    font-size: 14px;
    margin-bottom: 32px;
  }
  
  .quick-examples {
    gap: 10px;
  }
  
  .example-card {
    padding: 12px 14px;
  }
  
  .example-text {
    font-size: 13px;
  }
  
  /* 思考过程 */
  .thinking-section {
    margin-bottom: 12px;
  }
  
  .thinking-header {
    padding: 8px 12px;
  }
  
  .thinking-content {
    padding: 10px 14px;
  }
  
  .thinking-step {
    padding-left: 28px;
    margin-bottom: 8px;
  }
  
  .thinking-step::before {
    left: 8px;
  }
  
  .step-indicator {
    left: 0;
  }
  
  .thinking-step.active {
    margin-left: -14px;
    padding-left: 42px;
  }
  
  .thinking-step.active .step-indicator {
    left: 14px;
  }
  
  .thinking-step.active::before {
    left: 22px;
  }
  
  .step-name {
    font-size: 12px;
  }
  
  .step-content {
    font-size: 11px;
  }
  
  /* 叙述摘要 */
  .summary-content {
    font-size: 14px;
    line-height: 1.7;
  }

  /* 数据区域 */
  .data-section {
    margin-bottom: 16px;
  }
  
  .data-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
  }
  
  .data-header h4 {
    font-size: 14px;
  }
  
  .data-header-right {
    width: 100%;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 8px;
  }
  
  .view-toggle {
    order: -1;
  }
  
  .data-count {
    font-size: 12px;
  }
  
  /* 表格 - 横向滚动 */
  .table-wrapper {
    margin: 0 -12px;
    padding: 0 12px;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
  }
  
  .result-table {
    min-width: 500px;
    font-size: 12px;
  }
  
  .result-table th,
  .result-table td {
    padding: 10px 12px;
    white-space: nowrap;
  }
  
  .table-more {
    padding: 10px 12px;
    font-size: 12px;
    flex-direction: column;
    gap: 8px;
  }
  
  /* 图表 */
  .chart-view {
    padding: 12px;
    margin: 0 -12px;
    border-radius: 0;
    border-left: none;
    border-right: none;
  }
  
  .chart-type-selector {
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
  }
  
  .chart-type-buttons {
    width: 100%;
    display: flex;
    gap: 6px;
  }
  
  .chart-type-btn {
    flex: 1;
    text-align: center;
    padding: 8px 10px;
  }
  
  .chart-container {
    height: 280px;
  }
  
  /* SQL区域 */
  .sql-section {
    margin-top: 16px;
  }
  
  .sql-header {
    padding: 10px 12px;
    font-size: 12px;
  }
  
  .sql-code {
    padding: 12px;
    font-size: 11px;
    line-height: 1.5;
  }
  
  /* 确认卡 */
  .confirm-box {
    margin: 0 -12px;
    border-radius: 0;
    border-left: none;
    border-right: none;
  }
  
  .confirm-header {
    padding: 12px;
  }
  
  .confirm-header h3 {
    font-size: 14px;
  }
  
  .confirm-content {
    padding: 12px;
  }
  
  .user-question-text,
  .understanding-text {
    font-size: 13px;
    padding: 10px;
  }
  
  .confirm-actions {
    padding: 12px;
    flex-direction: column;
  }
  
  .btn-confirm,
  .btn-cancel,
  .btn-secondary {
    width: 100%;
    padding: 14px 20px;
  }
  
  /* 表选择卡 */
  .table-selection-box {
    margin: 0 -12px;
    border-radius: 0;
    border-left: none;
    border-right: none;
  }
  
  .table-selection-header {
    padding: 12px;
    flex-wrap: wrap;
  }
  
  .table-selection-header h3 {
    font-size: 14px;
    width: 100%;
    margin-bottom: 8px;
  }
  
  .table-selection-header-actions {
    margin-left: 0;
    width: 100%;
    justify-content: flex-start;
  }
  
  .table-selection-tip {
    padding: 10px 12px;
    font-size: 12px;
  }
  
  .table-candidates {
    padding: 12px;
    gap: 10px;
  }
  
  .table-candidate {
    padding: 12px;
  }
  
  .candidate-name {
    font-size: 13px;
  }
  
  .candidate-desc {
    font-size: 11px;
  }
  
  .table-selection-actions {
    padding: 12px;
    flex-direction: column;
  }
  
  /* 输入区域 */
  .chat-input-area {
    padding: 12px 12px calc(12px + env(safe-area-inset-bottom, 0px));
  }
  
  .input-container {
    max-width: 100%;
  }
  
  .input-options {
    gap: 6px;
    margin-bottom: 10px;
  }
  
  .option-select {
    flex: 1;
    min-width: 0;
    padding: 8px 10px;
    font-size: 12px;
  }
  
  .option-checkbox {
    padding: 8px 10px;
    font-size: 12px;
  }

  .confirmation-mode-group {
    width: 100%;
    justify-content: space-between;
  }

  .confirmation-mode-label {
    padding-left: 2px;
  }

  .confirmation-mode-btn {
    flex: 1;
    min-width: 0;
    padding: 8px 6px;
  }
  
  .input-wrapper {
    padding: 10px 12px;
    border-radius: var(--radius-md);
  }
  
  .chat-input {
    font-size: 16px; /* 防止iOS缩放 */
  }
  
  .send-btn,
  .stop-btn {
    width: 40px;
    height: 40px;
  }
  
  .input-hint {
    font-size: 11px;
    margin-top: 8px;
  }
  
  /* 侧边栏内容 */
  .sidebar-header {
    padding: 12px;
  }
  
  .new-chat-btn {
    padding: 12px 14px;
  }
  
  .sidebar-content {
    padding: 8px 12px;
  }
  
  .conversation-item {
    padding: 12px;
  }
  
  .sidebar-footer {
    padding: 12px;
  }
}

/* ==================== 响应式 - 小手机 (< 400px) ==================== */
@media (max-width: 400px) {
  .welcome-title {
    font-size: 18px;
  }
  
  .welcome-subtitle {
    font-size: 13px;
  }
  
  .example-card {
    padding: 10px 12px;
  }
  
  .example-text {
    font-size: 12px;
  }
  
  .message-avatar {
    width: 28px;
    height: 28px;
    font-size: 12px;
  }
  
  .user-message {
    padding: 8px 12px;
    font-size: 13px;
  }
  
  .chart-container {
    height: 220px;
  }
  
  .input-options {
    flex-direction: column;
  }
  
  .option-select {
    width: 100%;
  }

  .confirmation-mode-group {
    width: 100%;
    flex-wrap: wrap;
  }

  .confirmation-mode-label {
    width: 100%;
    padding: 0 0 2px 2px;
  }
}

/* ==================== 横屏模式 ==================== */
@media (max-height: 500px) and (orientation: landscape) {
  .welcome-screen {
    padding: 16px;
  }
  
  .welcome-logo {
    margin-bottom: 16px;
  }
  
  .welcome-subtitle {
    margin-bottom: 20px;
  }
  
  .quick-examples {
    flex-direction: row;
    flex-wrap: wrap;
  }
  
  .example-card {
    flex: 1;
    min-width: 200px;
  }
  
  .chart-container {
    height: 200px;
  }
}
</style>
