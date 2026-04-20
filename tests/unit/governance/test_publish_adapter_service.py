from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from server.services.publish_adapter_service import (
    PublishAdapterService,
    SOURCE_TYPE_GOVERNANCE_CANDIDATE,
)


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePublishDB:
    def __init__(self):
        self.candidates = {}
        self.release_runs = {}

    def transaction(self):
        return FakeTransaction()

    async def fetchrow(self, query, *params):
        if "FROM governance_candidates" in query and "WHERE candidate_id = $1" in query:
            return self.candidates.get(params[0])

        if "INSERT INTO release_runs" in query:
            (
                release_run_id,
                release_type,
                source_type,
                source_ids_json,
                status,
                plan_json,
                result_json,
                triggered_by,
            ) = params
            now = datetime.now(timezone.utc)
            row = {
                "release_run_id": release_run_id,
                "release_type": release_type,
                "source_type": source_type,
                "source_ids_json": json.loads(source_ids_json),
                "policy_snapshot_id": None,
                "status": status,
                "plan_json": json.loads(plan_json),
                "result_json": json.loads(result_json),
                "triggered_by": triggered_by,
                "created_at": now,
                "started_at": now,
                "completed_at": now,
            }
            self.release_runs[release_run_id] = row
            return row

        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query, *params):
        if "FROM release_runs" in query:
            rows = list(self.release_runs.values())
            if "source_ids_json @> $2::jsonb" in query:
                source_type, source_ids_json, limit = params
                source_ids = set(json.loads(source_ids_json))
                rows = [
                    row for row in rows
                    if row["source_type"] == source_type
                    and source_ids.issubset(set(row["source_ids_json"]))
                ]
            elif "WHERE source_type = $1" in query:
                source_type, limit = params
                rows = [row for row in rows if row["source_type"] == source_type]
            else:
                limit = params[0]
            rows.sort(key=lambda item: item["created_at"], reverse=True)
            return rows[:limit]

        raise AssertionError(f"unexpected fetch query: {query}")


def build_candidate_row(*, candidate_id: UUID, status: str = "approved"):
    return {
        "candidate_id": candidate_id,
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
        "status": status,
        "created_at": datetime.now(timezone.utc),
        "reviewed_at": datetime.now(timezone.utc),
        "reviewed_by": uuid4(),
    }


@pytest.mark.asyncio
async def test_publish_governance_candidate_creates_blocked_release_run_for_manual_review_candidate():
    db = FakePublishDB()
    service = PublishAdapterService(db)
    candidate_id = uuid4()
    publisher_id = uuid4()
    db.candidates[candidate_id] = build_candidate_row(candidate_id=candidate_id, status="approved")

    result = await service.publish_governance_candidate(candidate_id, triggered_by=publisher_id)

    assert result is not None
    assert result["candidate"]["candidate_id"] == str(candidate_id)
    assert result["candidate"]["status"] == "approved"
    assert result["release_run"]["status"] == "blocked"
    assert result["release_run"]["source_type"] == SOURCE_TYPE_GOVERNANCE_CANDIDATE
    assert result["release_run"]["source_ids_json"] == [str(candidate_id)]
    assert result["release_run"]["plan_json"]["publishable"] is False
    assert result["release_run"]["plan_json"]["blocking_reasons"][0]["code"] == "manual_review_required"
    assert result["release_run"]["result_json"]["outcome"] == "blocked"


@pytest.mark.asyncio
async def test_publish_governance_candidate_rejects_non_approved_candidate():
    db = FakePublishDB()
    service = PublishAdapterService(db)
    candidate_id = uuid4()
    db.candidates[candidate_id] = build_candidate_row(candidate_id=candidate_id, status="observed")

    with pytest.raises(ValueError, match="不允许发布"):
        await service.publish_governance_candidate(candidate_id, triggered_by=uuid4())

    assert db.release_runs == {}


@pytest.mark.asyncio
async def test_list_release_runs_filters_by_candidate_source_id():
    db = FakePublishDB()
    service = PublishAdapterService(db)
    first_candidate_id = uuid4()
    second_candidate_id = uuid4()
    db.candidates[first_candidate_id] = build_candidate_row(candidate_id=first_candidate_id, status="approved")
    db.candidates[second_candidate_id] = build_candidate_row(candidate_id=second_candidate_id, status="approved")

    await service.publish_governance_candidate(first_candidate_id, triggered_by=uuid4())
    await service.publish_governance_candidate(second_candidate_id, triggered_by=uuid4())

    items = await service.list_release_runs(
        source_type=SOURCE_TYPE_GOVERNANCE_CANDIDATE,
        source_id=UUID(str(first_candidate_id)),
        limit=10,
    )

    assert len(items) == 1
    assert items[0]["source_ids_json"] == [str(first_candidate_id)]
