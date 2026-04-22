from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]


def test_governance_candidate_api_contract_exists():
    api_file = ROOT_DIR / "frontend" / "src" / "api" / "index.js"
    content = api_file.read_text(encoding="utf-8")

    assert "export const governanceCandidateAPI = {" in content
    assert "list: (params) => request.get('/admin/governance-candidates', { params })" in content
    assert "observeLearningEvents: (data) => request.post('/admin/governance-candidates/observe-learning-events', data)" in content
    assert "review: (candidateId, data) => request.post(`/admin/governance-candidates/${candidateId}/review`, data)" in content


def test_admin_router_registers_governance_candidates_page():
    router_file = ROOT_DIR / "frontend" / "src" / "router" / "index.js"
    content = router_file.read_text(encoding="utf-8")

    assert "path: 'governance-candidates'" in content
    assert "name: 'AdminGovernanceCandidates'" in content
    assert "component: () => import('@/views/admin/components/GovernanceCandidates.vue')" in content


def test_admin_index_exposes_governance_candidates_menu():
    admin_index = ROOT_DIR / "frontend" / "src" / "views" / "admin" / "Index.vue"
    content = admin_index.read_text(encoding="utf-8")

    assert "<el-menu-item index=\"/admin/governance-candidates\">" in content
    assert "<span>治理候选</span>" in content
    assert "'/admin/governance-candidates': '治理候选'" in content


def test_governance_candidates_component_supports_scan_and_review_actions():
    component = ROOT_DIR / "frontend" / "src" / "views" / "admin" / "components" / "GovernanceCandidates.vue"
    content = component.read_text(encoding="utf-8")

    assert "governanceCandidateAPI.list" in content
    assert "governanceCandidateAPI.observeLearningEvents" in content
    assert "governanceCandidateAPI.review" in content
    assert "async function observeLearningEvents()" in content
    assert "async function reviewCandidate(row, action)" in content
    assert "扫描学习事件" in content
    assert "批准" in content
    assert "拒绝" in content
