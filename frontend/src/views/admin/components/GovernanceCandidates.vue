<template>
  <div class="governance-candidates">
    <el-card>
      <template #header>
        <div class="card-header">
          <div class="title-group">
            <span>治理候选</span>
            <el-tag type="info" size="small">共 {{ totalCount }} 条</el-tag>
          </div>
          <div class="header-actions">
            <el-select v-model="observeLimit" size="small" class="observe-limit">
              <el-option :value="50" label="最近 50 条事件" />
              <el-option :value="100" label="最近 100 条事件" />
              <el-option :value="200" label="最近 200 条事件" />
            </el-select>
            <el-button
              type="primary"
              plain
              size="small"
              :loading="observing"
              @click="observeLearningEvents"
            >
              扫描学习事件
            </el-button>
            <el-button
              text
              type="primary"
              size="small"
              :icon="Refresh"
              @click="loadCandidates"
            >
              刷新
            </el-button>
          </div>
        </div>
      </template>

      <div class="toolbar">
        <el-select
          v-model="statusFilter"
          clearable
          placeholder="按状态筛选"
          class="status-filter"
          @change="loadCandidates"
        >
          <el-option label="已观察" value="observed" />
          <el-option label="已批准" value="approved" />
          <el-option label="已拒绝" value="rejected" />
        </el-select>
      </div>

      <el-alert
        v-if="observeSummary"
        class="observe-summary"
        type="success"
        :closable="true"
        @close="observeSummary = null"
      >
        <template #title>
          本次扫描了 {{ observeSummary.scanned_events }} 条事件，
          新增 {{ observeSummary.created_candidates }} 条候选，
          更新 {{ observeSummary.updated_candidates }} 条候选，
          去重 {{ observeSummary.deduplicated_events }} 条事件。
        </template>
      </el-alert>

      <el-table
        v-loading="loading"
        :data="candidates"
        stripe
        style="width: 100%"
        table-layout="auto"
        empty-text="暂无治理候选"
      >
        <el-table-column label="候选类型" min-width="150">
          <template #default="{ row }">
            <el-tag size="small" type="info">
              {{ formatCandidateType(row.candidate_type) }}
            </el-tag>
          </template>
        </el-table-column>

        <el-table-column label="目标对象" min-width="220">
          <template #default="{ row }">
            <div class="target-object">
              <div class="target-main">{{ row.target_object_id }}</div>
              <div class="target-meta">{{ row.target_object_type }} / {{ row.scope_type }}</div>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="证据摘要" min-width="320" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.evidence_summary || '-' }}
          </template>
        </el-table-column>

        <el-table-column label="支持次数" min-width="100">
          <template #default="{ row }">
            {{ row.support_count ?? 0 }}
          </template>
        </el-table-column>

        <el-table-column label="置信度" min-width="110">
          <template #default="{ row }">
            {{ formatConfidence(row.confidence_score) }}
          </template>
        </el-table-column>

        <el-table-column label="状态" min-width="110">
          <template #default="{ row }">
            <el-tag :type="getStatusTagType(row.status)">
              {{ formatStatus(row.status) }}
            </el-tag>
          </template>
        </el-table-column>

        <el-table-column label="审核时间" min-width="180">
          <template #default="{ row }">
            {{ formatDateTime(row.reviewed_at) }}
          </template>
        </el-table-column>

        <el-table-column label="操作" min-width="220" fixed="right">
          <template #default="{ row }">
            <div class="action-group">
              <el-button
                size="small"
                text
                type="primary"
                @click="openEvidenceDialog(row)"
              >
                查看详情
              </el-button>
              <el-button
                v-if="row.status === 'observed'"
                size="small"
                text
                type="success"
                :loading="Boolean(actionLoadingMap[row.candidate_id])"
                @click="reviewCandidate(row, 'approve')"
              >
                批准
              </el-button>
              <el-button
                v-if="row.status === 'observed'"
                size="small"
                text
                type="danger"
                :loading="Boolean(actionLoadingMap[row.candidate_id])"
                @click="reviewCandidate(row, 'reject')"
              >
                拒绝
              </el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog
      v-model="evidenceDialogVisible"
      title="治理候选详情"
      width="90%"
      :style="{ maxWidth: '960px' }"
      destroy-on-close
    >
      <div v-if="selectedCandidate" class="evidence-dialog">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="候选类型">
            {{ formatCandidateType(selectedCandidate.candidate_type) }}
          </el-descriptions-item>
          <el-descriptions-item label="状态">
            {{ formatStatus(selectedCandidate.status) }}
          </el-descriptions-item>
          <el-descriptions-item label="目标对象">
            {{ selectedCandidate.target_object_id }}
          </el-descriptions-item>
          <el-descriptions-item label="支持次数">
            {{ selectedCandidate.support_count ?? 0 }}
          </el-descriptions-item>
          <el-descriptions-item label="置信度">
            {{ formatConfidence(selectedCandidate.confidence_score) }}
          </el-descriptions-item>
          <el-descriptions-item label="审核人">
            {{ selectedCandidate.reviewed_by || '-' }}
          </el-descriptions-item>
        </el-descriptions>

        <el-card class="detail-card">
          <template #header>证据摘要</template>
          <div class="detail-text">{{ selectedCandidate.evidence_summary || '-' }}</div>
        </el-card>

        <el-card class="detail-card">
          <template #header>建议变更</template>
          <pre class="json-preview">{{ formatJson(selectedCandidate.suggested_change_json) }}</pre>
        </el-card>

        <el-card class="detail-card">
          <template #header>证据明细</template>
          <pre class="json-preview">{{ formatJson(selectedCandidate.evidence_payload_json) }}</pre>
        </el-card>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import { governanceCandidateAPI } from '@/api'

const loading = ref(false)
const observing = ref(false)
const candidates = ref([])
const totalCount = ref(0)
const statusFilter = ref('observed')
const observeLimit = ref(100)
const observeSummary = ref(null)
const evidenceDialogVisible = ref(false)
const selectedCandidate = ref(null)
const actionLoadingMap = reactive({})

const candidateTypeLabelMap = {
  table_selection_rejection: '选表拒绝',
}

const statusLabelMap = {
  observed: '已观察',
  approved: '已批准',
  rejected: '已拒绝',
}

async function loadCandidates() {
  loading.value = true
  try {
    const { data } = await governanceCandidateAPI.list({
      status: statusFilter.value || undefined,
      limit: 100,
    })
    candidates.value = data.items || []
    totalCount.value = data.total_count || 0
  } catch (error) {
    console.error('加载治理候选失败', error)
    ElMessage.error(error.response?.data?.detail || error.message || '加载治理候选失败')
  } finally {
    loading.value = false
  }
}

async function observeLearningEvents() {
  observing.value = true
  try {
    const { data } = await governanceCandidateAPI.observeLearningEvents({
      limit: observeLimit.value,
    })
    observeSummary.value = data
    ElMessage.success('治理候选扫描完成')
    await loadCandidates()
  } catch (error) {
    console.error('扫描治理候选失败', error)
    ElMessage.error(error.response?.data?.detail || error.message || '扫描治理候选失败')
  } finally {
    observing.value = false
  }
}

async function reviewCandidate(row, action) {
  const actionLabel = action === 'approve' ? '批准' : '拒绝'
  try {
    await ElMessageBox.confirm(
      `确定要${actionLabel}候选 ${row.target_object_id} 吗？`,
      `${actionLabel}治理候选`,
      {
        type: action === 'approve' ? 'success' : 'warning',
        confirmButtonText: '确定',
        cancelButtonText: '取消',
      }
    )
  } catch {
    return
  }

  actionLoadingMap[row.candidate_id] = true
  try {
    await governanceCandidateAPI.review(row.candidate_id, { action })
    ElMessage.success(`治理候选已${actionLabel}`)
    await loadCandidates()
  } catch (error) {
    console.error(`${actionLabel}治理候选失败`, error)
    ElMessage.error(error.response?.data?.detail || error.message || `${actionLabel}治理候选失败`)
  } finally {
    delete actionLoadingMap[row.candidate_id]
  }
}

function openEvidenceDialog(row) {
  selectedCandidate.value = row
  evidenceDialogVisible.value = true
}

function formatCandidateType(candidateType) {
  return candidateTypeLabelMap[candidateType] || candidateType || '-'
}

function formatStatus(status) {
  return statusLabelMap[status] || status || '-'
}

function getStatusTagType(status) {
  if (status === 'approved') return 'success'
  if (status === 'rejected') return 'danger'
  return 'warning'
}

function formatConfidence(confidenceScore) {
  if (confidenceScore === null || confidenceScore === undefined || confidenceScore === '') return '-'
  return `${Math.round(Number(confidenceScore) * 100)}%`
}

function formatDateTime(value) {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '-'
  return date.toLocaleString('zh-CN', { hour12: false })
}

function formatJson(value) {
  return JSON.stringify(value || {}, null, 2)
}

onMounted(() => {
  loadCandidates()
})
</script>

<style scoped>
.governance-candidates {
  width: 100%;
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.title-group {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
}

.observe-limit,
.status-filter {
  width: 180px;
}

.toolbar {
  margin-bottom: 16px;
  display: flex;
  justify-content: space-between;
  gap: 12px;
}

.observe-summary {
  margin-bottom: 16px;
}

.target-object {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.target-main {
  font-weight: 600;
  color: #1f2937;
}

.target-meta {
  font-size: 12px;
  color: #6b7280;
}

.action-group {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.evidence-dialog {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.detail-card {
  margin-top: 16px;
}

.detail-text {
  white-space: pre-wrap;
  line-height: 1.6;
}

.json-preview {
  margin: 0;
  padding: 12px;
  background: #f8fafc;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  overflow: auto;
  line-height: 1.5;
  font-size: 12px;
}

@media (max-width: 768px) {
  .card-header,
  .header-actions,
  .toolbar {
    flex-direction: column;
    align-items: stretch;
  }

  .observe-limit,
  .status-filter {
    width: 100%;
  }
}
</style>
