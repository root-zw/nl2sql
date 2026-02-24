<template>
  <div class="query-logs">
    <el-card>
      <template #header>
        <div class="card-header">
          <div class="title">
            <span>📜 查询日志</span>
            <el-tag type="info" size="small">
              共 {{ total }} 条
            </el-tag>
          </div>
          <div class="header-actions">
            <el-button
              text
              type="primary"
              :icon="Download"
              @click="openExportDialog"
            >
              导出
            </el-button>
            <el-button text type="primary" :icon="Refresh" @click="loadLogs">
              刷新
            </el-button>
          </div>
        </div>
      </template>

      <div class="toolbar">
        <el-input
          v-model="filters.keyword"
          placeholder="搜索问题或 SQL"
          clearable
          @keyup.enter="handleSearch"
          @clear="handleSearch"
        >
          <template #prefix>
            <el-icon><Search /></el-icon>
          </template>
        </el-input>

        <el-select
          v-model="filters.user_id"
          placeholder="执行用户"
          clearable
          filterable
        >
          <el-option
            v-for="user in userOptions"
            :key="user.user_id"
            :label="user.username"
            :value="user.user_id"
          />
        </el-select>

        <el-select
          v-model="filters.connection_id"
          placeholder="数据库"
          clearable
          filterable
        >
          <el-option
            v-for="conn in connectionOptions"
            :key="conn.connection_id"
            :label="conn.connection_name"
            :value="conn.connection_id"
          />
        </el-select>

        <el-date-picker
          v-model="filters.dateRange"
          type="datetimerange"
          range-separator="至"
          start-placeholder="开始时间"
          end-placeholder="结束时间"
          :disabled-date="disabledFuture"
        />

        <div class="toolbar-actions">
          <el-button type="primary" @click="handleSearch">
            查询
          </el-button>
          <el-button @click="resetFilters">
            重置
          </el-button>
        </div>
      </div>

      <el-table
        v-loading="loading"
        :data="logs"
        stripe
        height="520px"
        style="width: 100%"
        table-layout="auto"
        @row-click="handleRowClick"
        empty-text="暂无查询记录"
      >
        <el-table-column label="查询时间" min-width="180">
          <template #default="{ row }">
            {{ formatDate(row.created_at) }}
          </template>
        </el-table-column>

        <el-table-column label="用户" min-width="140">
          <template #default="{ row }">
            {{ row.user?.username || '-' }}
          </template>
        </el-table-column>

        <el-table-column label="数据库" min-width="160">
          <template #default="{ row }">
            {{ row.connection?.connection_name || '-' }}
          </template>
        </el-table-column>

        <el-table-column label="用户问题" min-width="220" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.original_question || '-' }}
          </template>
        </el-table-column>

        <el-table-column label="耗时" min-width="120">
          <template #default="{ row }">
            {{ formatDuration(row.execution_time_ms) }}
          </template>
        </el-table-column>

        <el-table-column label="状态" min-width="110">
          <template #default="{ row }">
            <el-tag :type="getStatusTag(row.execution_status)">
              {{ getStatusLabel(row.execution_status) }}
            </el-tag>
          </template>
        </el-table-column>

        <el-table-column label="操作" min-width="120" fixed="right">
          <template #default="{ row }">
            <el-button
              size="small"
              type="primary"
              text
              @click.stop="handleRowClick(row)"
            >
              查看详情
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="table-footer">
        <el-pagination
          background
          layout="total, sizes, prev, pager, next, jumper"
          :current-page="pager.page"
          :page-size="pager.pageSize"
          :page-sizes="[10, 20, 50, 100]"
          :total="total"
          @current-change="handlePageChange"
          @size-change="handleSizeChange"
        />
      </div>
    </el-card>

    <el-drawer
      v-model="detailVisible"
      title="查询详情"
      size="60%"
      :with-header="true"
      destroy-on-close
      @close="handleDrawerClose"
    >
      <div v-if="detailLoading" class="drawer-loading">
        <el-skeleton :rows="8" animated />
      </div>

      <div v-else-if="detailData" class="detail-content">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="执行用户">
            {{ detailData.user?.username || '-' }}
          </el-descriptions-item>
          <el-descriptions-item label="数据库">
            {{ detailData.connection?.connection_name || '-' }}
          </el-descriptions-item>
          <el-descriptions-item label="提交时间">
            {{ formatDate(detailData.created_at) }}
          </el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusTag(detailData.execution_status)">
              {{ getStatusLabel(detailData.execution_status) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="耗时">
            {{ formatDuration(detailData.execution_time_ms) }}
          </el-descriptions-item>
          <el-descriptions-item label="结果行数">
            {{ detailData.result_row_count ?? '-' }}
          </el-descriptions-item>
        </el-descriptions>

        <el-card class="detail-block">
          <template #header>用户问题</template>
          <p class="question-text">{{ detailData.original_question || '-' }}</p>
        </el-card>

        <el-card class="detail-block">
          <template #header>
            <div class="sql-header">
              <span>生成的 SQL</span>
              <el-button
                type="primary"
                text
                size="small"
                @click="copySQL(detailData.generated_sql)"
              >
                复制 SQL
              </el-button>
            </div>
          </template>
          <el-input
            v-model="detailData.generated_sql"
            type="textarea"
            :autosize="{ minRows: 4, maxRows: 10 }"
            readonly
          />
        </el-card>

        <el-alert
          v-if="detailData.error_message"
          title="错误信息"
          type="error"
          :closable="false"
          class="detail-block"
        >
          <template #default>
            <pre class="error-text">{{ detailData.error_message }}</pre>
          </template>
        </el-alert>

        <el-card class="detail-block" v-if="detailData.result_preview">
          <template #header>
            <div class="sql-header">
              <span>SQL 执行结果</span>
              <small v-if="detailData.result_preview.meta">
                行数：{{ detailData.result_preview.meta.row_count ?? '-' }}，
                {{ detailData.result_preview.meta.truncated ? '已截断显示' : '完整显示' }}
              </small>
            </div>
          </template>

          <el-alert
            v-if="detailData.result_preview.error"
            type="error"
            :closable="false"
            :title="detailData.result_preview.error"
          />

          <el-alert
            v-else-if="detailData.result_preview.warning"
            type="warning"
            :closable="false"
            :title="detailData.result_preview.warning"
          />

          <el-table
            v-else
            :data="detailData.result_preview.rows || []"
            :height="320"
            size="small"
            border
            empty-text="暂无结果"
          >
            <el-table-column
              v-for="(col, colIdx) in detailData.result_preview.columns || []"
              :key="col.name || colIdx"
              :label="col.name || `列 ${colIdx + 1}`"
              :min-width="120"
            >
              <template #default="{ row }">
                {{ row[colIdx] ?? '' }}
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </div>

      <el-empty
        v-else
        description="暂无详情数据"
      />
    </el-drawer>

    <el-dialog
      v-model="exportDialogVisible"
      title="导出查询日志"
      width="90%"
      :style="{ maxWidth: '520px' }"
      :close-on-click-modal="false"
      @closed="resetExportForm"
    >
      <p class="export-tip">可选择条件导出；留空时将导出全部记录</p>
      <el-form label-width="90px">
        <el-form-item label="执行用户">
          <el-select
            v-model="exportForm.user_id"
            placeholder="全部用户"
            clearable
            filterable
          >
            <el-option
              v-for="user in userOptions"
              :key="user.user_id"
              :label="user.username"
              :value="user.user_id"
            />
          </el-select>
        </el-form-item>

        <el-form-item label="数据库">
          <el-select
            v-model="exportForm.connection_id"
            placeholder="全部数据库"
            clearable
            filterable
          >
            <el-option
              v-for="conn in connectionOptions"
              :key="conn.connection_id"
              :label="conn.connection_name"
              :value="conn.connection_id"
            />
          </el-select>
        </el-form-item>

        <el-form-item label="时间范围">
          <el-date-picker
            v-model="exportForm.dateRange"
            type="datetimerange"
            range-separator="至"
            start-placeholder="开始时间"
            end-placeholder="结束时间"
            :disabled-date="disabledFuture"
          />
        </el-form-item>
      </el-form>

      <template #footer>
        <el-button @click="exportDialogVisible = false" :disabled="exporting">取消</el-button>
        <el-button type="primary" :loading="exporting" @click="handleExport">
          导出
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh, Search, Download } from '@element-plus/icons-vue'
import axios from '@/utils/request'
import { formatDate, formatDuration, copyToClipboard, downloadFile } from '@/utils/common'

const logs = ref([])
const total = ref(0)
const loading = ref(false)
const exporting = ref(false)
const exportDialogVisible = ref(false)

const pager = reactive({
  page: 1,
  pageSize: 20
})

const filters = reactive({
  keyword: '',
  user_id: '',
  connection_id: '',
  dateRange: []
})

const userOptions = ref([])
const connectionOptions = ref([])

const exportForm = reactive({
  user_id: '',
  connection_id: '',
  dateRange: []
})

const detailVisible = ref(false)
const detailLoading = ref(false)
const detailData = ref(null)

function getStatusTag(status) {
  if (status === 'success' || status === 'completed') return 'success'
  if (status === 'failed') return 'danger'
  return 'info'
}

function getStatusLabel(status) {
  const map = {
    success: '成功',
    failed: '失败',
    completed: '完成',
    cancelled: '已取消'
  }
  return map[status] || status || '-'
}

function disabledFuture(time) {
  return time.getTime() > Date.now()
}

function buildParams(options = { includePaging: true }) {
  const params = {}

  if (options.includePaging) {
    params.page = pager.page
    params.page_size = pager.pageSize
  }

  if (filters.keyword?.trim()) {
    params.keyword = filters.keyword.trim()
  }
  if (filters.user_id) {
    params.user_id = filters.user_id
  }
  if (filters.connection_id) {
    params.connection_id = filters.connection_id
  }
  if (filters.dateRange?.length === 2) {
    const [start, end] = filters.dateRange
    const startISO = toISO(start)
    const endISO = toISO(end)
    if (startISO) params.start_time = startISO
    if (endISO) params.end_time = endISO
  }
  return params
}

function toISO(value) {
  if (!value) return null
  const date = value instanceof Date ? value : new Date(value)
  if (isNaN(date.getTime())) return null
  return date.toISOString()
}

async function loadReferenceData() {
  try {
    const [userRes, connRes] = await Promise.all([
      axios.get('/admin/users'),
      axios.get('/admin/connections', {
        params: { page: 1, page_size: 200 }
      })
    ])
    userOptions.value = userRes.data || []
    const connItems = connRes.data?.items || []
    connectionOptions.value = connItems
  } catch (error) {
    console.error('加载筛选数据失败', error)
  }
}

async function loadLogs() {
  loading.value = true
  try {
    const { data } = await axios.get('/admin/monitor/query-logs', {
      params: buildParams({ includePaging: true })
    })
    logs.value = data.items || []
    total.value = data.total || 0
  } catch (error) {
    ElMessage.error('加载查询日志失败')
  } finally {
    loading.value = false
  }
}

function handleSearch() {
  pager.page = 1
  loadLogs()
}

function resetFilters() {
  filters.keyword = ''
  filters.user_id = ''
  filters.connection_id = ''
  filters.dateRange = []
  handleSearch()
}

function handlePageChange(page) {
  pager.page = page
  loadLogs()
}

function handleSizeChange(size) {
  pager.pageSize = size
  pager.page = 1
  loadLogs()
}

function handleRowClick(row) {
  if (!row?.query_id) return
  detailVisible.value = true
  fetchDetail(row.query_id)
}

async function fetchDetail(queryId) {
  detailLoading.value = true
  try {
    const { data } = await axios.get(`/admin/monitor/query-logs/${queryId}`)
    detailData.value = data
  } catch (error) {
    ElMessage.error('获取日志详情失败')
  } finally {
    detailLoading.value = false
  }
}

function handleDrawerClose() {
  detailData.value = null
  detailLoading.value = false
}

async function copySQL(sql) {
  if (!sql) {
    ElMessage.warning('没有可复制的 SQL')
    return
  }
  try {
    await copyToClipboard(sql)
    ElMessage.success('SQL 已复制到剪贴板')
  } catch (error) {
    ElMessage.error('复制 SQL 失败')
  }
}

function openExportDialog() {
  exportDialogVisible.value = true
}

function resetExportForm() {
  exportForm.user_id = ''
  exportForm.connection_id = ''
  exportForm.dateRange = []
}

function buildExportParams() {
  const params = {}
  if (exportForm.user_id) {
    params.user_id = exportForm.user_id
  }
  if (exportForm.connection_id) {
    params.connection_id = exportForm.connection_id
  }
  if (exportForm.dateRange?.length === 2) {
    const [start, end] = exportForm.dateRange
    const startISO = toISO(start)
    const endISO = toISO(end)
    if (startISO) params.start_time = startISO
    if (endISO) params.end_time = endISO
  }
  return params
}

async function handleExport() {
  exporting.value = true
  try {
    const { data } = await axios.get('/admin/monitor/query-logs/export', {
      params: buildExportParams(),
      responseType: 'blob'
    })
    const blob = new Blob([data], { type: 'text/csv' })
    downloadFile(blob, `query_logs_${new Date().toISOString().slice(0, 10)}.csv`)
    ElMessage.success('导出成功')
    exportDialogVisible.value = false
  } catch (error) {
    ElMessage.error('导出失败')
  } finally {
    exporting.value = false
  }
}

onMounted(async () => {
  await loadReferenceData()
  loadLogs()
})
</script>

<style scoped>
.query-logs {
  padding: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card-header .title {
  display: flex;
  align-items: center;
  gap: 12px;
  font-weight: 600;
}

.toolbar {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 12px;
  margin-bottom: 16px;
  align-items: center;
}

.toolbar-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.table-footer {
  margin-top: 16px;
  display: flex;
  justify-content: flex-end;
}

.detail-block {
  margin-top: 16px;
}

.question-text {
  margin: 0;
  font-size: 14px;
  line-height: 1.6;
  color: #333;
}

.sql-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.error-text {
  margin: 0;
  white-space: pre-wrap;
  font-family: Consolas, Monaco, 'Courier New', monospace;
}

.drawer-loading {
  padding: 24px;
}

.detail-content {
  max-height: 80vh;
  overflow-y: auto;
  padding-right: 8px;
}

.export-tip {
  margin: 0 0 16px;
  color: #909399;
  font-size: 13px;
}

/* 确保容器填满 */
.query-logs {
  width: 100%;
}

.query-logs .el-card {
  width: 100%;
}

::deep(.el-table) {
  width: 100%;
}

::deep(.el-table__body-wrapper) {
  width: 100%;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  .card-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 10px;
  }
  
  .title {
    font-size: 15px;
  }
  
  .header-actions {
    width: 100%;
    display: flex;
    justify-content: flex-end;
  }
  
  .toolbar {
    flex-direction: column;
    gap: 10px;
  }
  
  .toolbar .el-input,
  .toolbar .el-select,
  .toolbar .el-date-picker {
    width: 100% !important;
    margin-bottom: 0;
  }
  
  .toolbar-actions {
    display: flex;
    gap: 8px;
    width: 100%;
  }
  
  .toolbar-actions .el-button {
    flex: 1;
  }
  
  :deep(.el-table) {
    font-size: 13px;
  }
  
  .detail-dialog :deep(.el-dialog) {
    width: 95% !important;
  }
}

@media screen and (max-width: 480px) {
  .title span {
    font-size: 14px;
  }
  
  :deep(.el-table) {
    font-size: 12px;
  }
}
</style>

