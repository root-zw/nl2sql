from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api.admin.governance_candidates import (
    get_db_pool,
    router,
)
from server.middleware.auth import require_data_admin


def build_test_client():
    app = FastAPI()
    app.include_router(router, prefix="/api/admin")

    async def fake_current_user():
        return SimpleNamespace(user_id=uuid4(), role="data_admin")

    async def fake_db_pool():
        yield object()

    app.dependency_overrides[require_data_admin] = fake_current_user
    app.dependency_overrides[get_db_pool] = fake_db_pool
    return TestClient(app)


def test_list_governance_candidates_route(monkeypatch):
    async def fake_list_candidates(self, status=None, limit=50):
        assert status == "observed"
        assert limit == 20
        return [
            {
                "candidate_id": str(uuid4()),
                "candidate_type": "table_selection_rejection",
                "target_object_type": "table",
                "target_object_id": "table_land_deal",
                "scope_type": "global",
                "scope_id": None,
                "suggested_change_json": {"action": "review_table_metadata"},
                "evidence_summary": "表 table_land_deal 在统一确认阶段被重复换表",
                "evidence_payload_json": {"event_keys": ["draft_action:1"]},
                "support_count": 2,
                "confidence_score": 0.55,
                "status": "observed",
                "created_at": None,
                "reviewed_at": None,
                "reviewed_by": None,
            }
        ]

    monkeypatch.setattr(
        "server.services.governance_candidate_service.GovernanceCandidateService.list_candidates",
        fake_list_candidates,
    )
    client = build_test_client()

    response = client.get("/api/admin/governance-candidates", params={"status": "observed", "limit": 20})

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_count"] == 1
    assert payload["items"][0]["target_object_id"] == "table_land_deal"


def test_observe_learning_events_route(monkeypatch):
    async def fake_observe_recent_learning_events(self, limit=100):
        assert limit == 50
        return {
            "scanned_events": 3,
            "created_candidates": 1,
            "updated_candidates": 1,
            "deduplicated_events": 0,
            "ignored_events": 1,
            "candidates": [
                {
                    "candidate_id": str(uuid4()),
                    "candidate_type": "table_selection_rejection",
                    "target_object_type": "table",
                    "target_object_id": "table_land_deal",
                    "scope_type": "global",
                    "scope_id": None,
                    "suggested_change_json": {"action": "review_table_metadata"},
                    "evidence_summary": "表 table_land_deal 在统一确认阶段已发生 2 次换表",
                    "evidence_payload_json": {"event_keys": ["draft_action:1", "draft_action:2"]},
                    "support_count": 2,
                    "confidence_score": 0.55,
                    "status": "observed",
                    "created_at": None,
                    "reviewed_at": None,
                    "reviewed_by": None,
                }
            ],
        }

    monkeypatch.setattr(
        "server.services.governance_candidate_service.GovernanceCandidateService.observe_recent_learning_events",
        fake_observe_recent_learning_events,
    )
    client = build_test_client()

    response = client.post("/api/admin/governance-candidates/observe-learning-events", json={"limit": 50})

    assert response.status_code == 200
    payload = response.json()
    assert payload["scanned_events"] == 3
    assert payload["created_candidates"] == 1
    assert payload["candidates"][0]["support_count"] == 2


def test_review_governance_candidate_route(monkeypatch):
    candidate_id = uuid4()
    reviewer_id_value = uuid4()

    async def fake_current_user():
        return SimpleNamespace(user_id=reviewer_id_value, role="data_admin")

    async def fake_review_candidate(self, requested_candidate_id, *, action, reviewer_id):
        assert requested_candidate_id == candidate_id
        assert action == "approve"
        assert reviewer_id == reviewer_id_value
        return {
            "candidate_id": str(candidate_id),
            "candidate_type": "table_selection_rejection",
            "target_object_type": "table",
            "target_object_id": "table_land_deal",
            "scope_type": "global",
            "scope_id": None,
            "suggested_change_json": {"action": "review_table_metadata"},
            "evidence_summary": "表 table_land_deal 在统一确认阶段已发生 2 次换表",
            "evidence_payload_json": {"event_keys": ["draft_action:1", "draft_action:2"]},
            "support_count": 2,
            "confidence_score": 0.55,
            "status": "approved",
            "created_at": None,
            "reviewed_at": None,
            "reviewed_by": str(reviewer_id_value),
        }

    monkeypatch.setattr(
        "server.services.governance_candidate_service.GovernanceCandidateService.review_candidate",
        fake_review_candidate,
    )
    client = build_test_client()
    client.app.dependency_overrides[require_data_admin] = fake_current_user

    response = client.post(
        f"/api/admin/governance-candidates/{candidate_id}/review",
        json={"action": "approve"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == str(candidate_id)
    assert payload["status"] == "approved"


def test_list_governance_candidate_release_runs_route(monkeypatch):
    candidate_id = uuid4()

    async def fake_list_release_runs(self, *, source_type=None, source_id=None, limit=50):
        assert source_type == "governance_candidate"
        assert source_id == candidate_id
        assert limit == 10
        return [
            {
                "release_run_id": str(uuid4()),
                "release_type": "governance_candidate_publish",
                "source_type": "governance_candidate",
                "source_ids_json": [str(candidate_id)],
                "policy_snapshot_id": None,
                "status": "blocked",
                "plan_json": {"publishable": False},
                "result_json": {"outcome": "blocked"},
                "triggered_by": str(uuid4()),
                "created_at": None,
                "started_at": None,
                "completed_at": None,
            }
        ]

    monkeypatch.setattr(
        "server.services.publish_adapter_service.PublishAdapterService.list_release_runs",
        fake_list_release_runs,
    )
    client = build_test_client()

    response = client.get(f"/api/admin/governance-candidates/{candidate_id}/release-runs", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert payload["total_count"] == 1
    assert payload["items"][0]["status"] == "blocked"


def test_publish_governance_candidate_route(monkeypatch):
    candidate_id = uuid4()
    publisher_id_value = uuid4()

    async def fake_current_user():
        return SimpleNamespace(user_id=publisher_id_value, role="data_admin")

    async def fake_publish_governance_candidate(self, requested_candidate_id, *, triggered_by):
        assert requested_candidate_id == candidate_id
        assert triggered_by == publisher_id_value
        return {
            "candidate": {
                "candidate_id": str(candidate_id),
                "candidate_type": "table_selection_rejection",
                "target_object_type": "table",
                "target_object_id": "table_land_deal",
                "scope_type": "global",
                "scope_id": None,
                "suggested_change_json": {"action": "review_table_metadata"},
                "evidence_summary": "表 table_land_deal 在统一确认阶段已发生 2 次换表",
                "evidence_payload_json": {"event_keys": ["draft_action:1", "draft_action:2"]},
                "support_count": 2,
                "confidence_score": 0.55,
                "status": "approved",
                "created_at": None,
                "reviewed_at": None,
                "reviewed_by": str(publisher_id_value),
            },
            "release_run": {
                "release_run_id": str(uuid4()),
                "release_type": "governance_candidate_publish",
                "source_type": "governance_candidate",
                "source_ids_json": [str(candidate_id)],
                "policy_snapshot_id": None,
                "status": "blocked",
                "plan_json": {"publishable": False},
                "result_json": {"outcome": "blocked"},
                "triggered_by": str(publisher_id_value),
                "created_at": None,
                "started_at": None,
                "completed_at": None,
            },
        }

    monkeypatch.setattr(
        "server.services.publish_adapter_service.PublishAdapterService.publish_governance_candidate",
        fake_publish_governance_candidate,
    )
    client = build_test_client()
    client.app.dependency_overrides[require_data_admin] = fake_current_user

    response = client.post(f"/api/admin/governance-candidates/{candidate_id}/publish")

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate"]["candidate_id"] == str(candidate_id)
    assert payload["release_run"]["status"] == "blocked"
