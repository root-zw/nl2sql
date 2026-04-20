from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from server.services.governance_candidate_service import GovernanceCandidateService


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeGovernanceDB:
    def __init__(self):
        self.candidates = {}
        self.learning_events = {}

    def transaction(self):
        return FakeTransaction()

    async def fetchrow(self, query, *params):
        if "FROM governance_candidates" in query and "WHERE candidate_type = $1" in query:
            candidate_type, target_object_type, target_object_id, scope_type, scope_id = params
            for row in self.candidates.values():
                if (
                    row["candidate_type"] == candidate_type
                    and row["target_object_type"] == target_object_type
                    and row["target_object_id"] == target_object_id
                    and row["scope_type"] == scope_type
                    and row["scope_id"] == scope_id
                    and row["status"] == "observed"
                ):
                    return row
            return None

        if "INSERT INTO governance_candidates" in query:
            (
                candidate_id,
                candidate_type,
                target_object_type,
                target_object_id,
                scope_type,
                scope_id,
                suggested_change_json,
                evidence_summary,
                evidence_payload_json,
                support_count,
                confidence_score,
            ) = params
            row = {
                "candidate_id": candidate_id,
                "candidate_type": candidate_type,
                "target_object_type": target_object_type,
                "target_object_id": target_object_id,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "suggested_change_json": json.loads(suggested_change_json),
                "evidence_summary": evidence_summary,
                "evidence_payload_json": json.loads(evidence_payload_json),
                "support_count": support_count,
                "confidence_score": Decimal(str(confidence_score)),
                "status": "observed",
                "created_at": datetime.now(timezone.utc),
                "reviewed_at": None,
                "reviewed_by": None,
            }
            self.candidates[candidate_id] = row
            return row

        if "UPDATE governance_candidates" in query:
            candidate_id, evidence_summary, evidence_payload_json, support_count, confidence_score = params
            row = self.candidates[candidate_id]
            row["evidence_summary"] = evidence_summary
            row["evidence_payload_json"] = json.loads(evidence_payload_json)
            row["support_count"] = support_count
            row["confidence_score"] = Decimal(str(confidence_score))
            return row

        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query, *params):
        if "FROM learning_events" in query:
            limit = params[0]
            rows = [event for event in self.learning_events.values() if event["event_type"] == "action_applied"]
            rows.sort(key=lambda item: item["created_at"])
            return rows[:limit]

        if "FROM governance_candidates" in query:
            rows = list(self.candidates.values())
            if "WHERE status = $1" in query:
                status, limit = params
                rows = [row for row in rows if row["status"] == status][:limit]
            else:
                limit = params[0]
                rows = rows[:limit]
            rows.sort(key=lambda item: (-item["support_count"], item["created_at"]), reverse=False)
            return rows

        raise AssertionError(f"unexpected fetch query: {query}")


def build_change_table_event(event_key: str, *, table_id: str, created_offset: int = 0, mode: str | None = None):
    created_at = datetime.now(timezone.utc) + timedelta(seconds=created_offset)
    payload = {
        "action_type": "change_table",
        "current_node": "table_resolution",
        "next_node": "table_resolution",
        "previous_selected_table_ids": [table_id],
        "previous_recommended_table_ids": [table_id],
        "next_rejected_table_ids": [table_id],
        "payload": {
            "reason": "这张表不对",
        },
    }
    if mode:
        payload["payload"]["mode"] = mode
    return {
        "event_id": str(uuid4()),
        "event_key": event_key,
        "query_id": str(uuid4()),
        "conversation_id": str(uuid4()),
        "user_id": str(uuid4()),
        "event_type": "action_applied",
        "event_version": 1,
        "payload_json": payload,
        "source_component": "draft_action_service",
        "created_at": created_at,
    }


@pytest.mark.asyncio
async def test_observe_learning_event_creates_candidate_for_table_rejection():
    db = FakeGovernanceDB()
    service = GovernanceCandidateService(db)
    event = build_change_table_event("draft_action:1", table_id="table_land_deal")

    result = await service.observe_learning_event(event)

    assert result["observed"] is True
    assert result["created"] is True
    assert result["candidate"]["candidate_type"] == "table_selection_rejection"
    assert result["candidate"]["target_object_id"] == "table_land_deal"
    assert result["candidate"]["support_count"] == 1
    assert result["candidate"]["evidence_payload_json"]["event_keys"] == ["draft_action:1"]


@pytest.mark.asyncio
async def test_observe_learning_event_updates_support_count_and_deduplicates():
    db = FakeGovernanceDB()
    service = GovernanceCandidateService(db)
    first = build_change_table_event("draft_action:1", table_id="table_land_deal")
    second = build_change_table_event("draft_action:2", table_id="table_land_deal", created_offset=1)

    first_result = await service.observe_learning_event(first)
    second_result = await service.observe_learning_event(second)
    replay_result = await service.observe_learning_event(second)

    assert first_result["candidate"]["support_count"] == 1
    assert second_result["updated"] is True
    assert second_result["candidate"]["support_count"] == 2
    assert replay_result["deduplicated"] is True
    assert replay_result["candidate"]["support_count"] == 2


@pytest.mark.asyncio
async def test_observe_recent_learning_events_ignores_unsupported_events_and_lists_candidates():
    db = FakeGovernanceDB()
    service = GovernanceCandidateService(db)
    first = build_change_table_event("draft_action:1", table_id="table_land_deal")
    second = build_change_table_event("draft_action:2", table_id="table_land_deal", created_offset=1, mode="manual_select")
    ignored = {
        **build_change_table_event("draft_action:3", table_id="table_land_use", created_offset=2),
        "payload_json": {"action_type": "confirm"},
    }
    db.learning_events[first["event_key"]] = first
    db.learning_events[second["event_key"]] = second
    db.learning_events[ignored["event_key"]] = ignored

    summary = await service.observe_recent_learning_events(limit=10)
    items = await service.list_candidates(limit=10)

    assert summary["scanned_events"] == 3
    assert summary["created_candidates"] == 1
    assert summary["updated_candidates"] == 1
    assert summary["ignored_events"] == 1
    assert len(items) == 1
    assert items[0]["target_object_id"] == "table_land_deal"
    assert items[0]["support_count"] == 2
