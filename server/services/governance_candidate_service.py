"""
治理候选服务

基于已落地 learning_events 观察稳定模式，并生成最小治理候选。
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import UUID, uuid4

import asyncpg
import structlog

from server.utils.db_pool import get_metadata_pool
from server.utils.json_utils import sanitize_for_json

logger = structlog.get_logger()

CANDIDATE_REVIEW_STATUS_MAP = {
    "approve": "approved",
    "reject": "rejected",
}


class GovernanceCandidateService:
    """治理候选观察服务"""

    def __init__(self, db_conn: Optional[asyncpg.Connection] = None):
        self.db = db_conn

    @asynccontextmanager
    async def _acquire(self) -> AsyncIterator[asyncpg.Connection]:
        if self.db is not None:
            yield self.db
            return

        pool = await get_metadata_pool()
        conn = await pool.acquire()
        try:
            yield conn
        finally:
            await pool.release(conn)

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        confidence_score = row["confidence_score"]
        if isinstance(confidence_score, Decimal):
            confidence_score = float(confidence_score)
        return {
            "candidate_id": str(row["candidate_id"]),
            "candidate_type": row["candidate_type"],
            "target_object_type": row["target_object_type"],
            "target_object_id": row["target_object_id"],
            "scope_type": row["scope_type"],
            "scope_id": str(row["scope_id"]) if row["scope_id"] else None,
            "suggested_change_json": row["suggested_change_json"] or {},
            "evidence_summary": row["evidence_summary"],
            "evidence_payload_json": row["evidence_payload_json"] or {},
            "support_count": row["support_count"],
            "confidence_score": confidence_score,
            "status": row["status"],
            "created_at": row["created_at"],
            "reviewed_at": row["reviewed_at"],
            "reviewed_by": str(row["reviewed_by"]) if row["reviewed_by"] else None,
        }

    @staticmethod
    def _build_table_rejection_candidate(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if event.get("event_type") != "action_applied":
            return None

        payload = dict(event.get("payload_json") or {})
        if payload.get("action_type") != "change_table":
            return None

        previous_selected_table_ids = list(payload.get("previous_selected_table_ids") or [])
        previous_recommended_table_ids = list(payload.get("previous_recommended_table_ids") or [])
        rejected_targets = previous_selected_table_ids or previous_recommended_table_ids
        if not rejected_targets:
            return None

        target_table_id = str(rejected_targets[0])
        action_payload = dict(payload.get("payload") or {})
        candidate_type = "table_selection_rejection"

        reason = action_payload.get("reason") or action_payload.get("source_text")
        evidence_summary = f"表 {target_table_id} 在统一确认阶段被重复换表"
        if reason:
            evidence_summary = f"{evidence_summary}，最新原因：{reason}"

        return {
            "candidate_type": candidate_type,
            "target_object_type": "table",
            "target_object_id": target_table_id,
            "scope_type": "global",
            "scope_id": None,
            "suggested_change_json": {
                "action": "review_table_metadata",
                "reason_type": candidate_type,
                "target_table_id": target_table_id,
                "latest_reason": reason,
                "latest_current_node": payload.get("current_node"),
                "latest_selection_mode": action_payload.get("mode"),
            },
            "evidence_summary": evidence_summary,
            "evidence_payload_json": {
                "event_keys": [event["event_key"]],
                "sample_events": [
                    {
                        "event_key": event["event_key"],
                        "query_id": event.get("query_id"),
                        "conversation_id": event.get("conversation_id"),
                        "current_node": payload.get("current_node"),
                        "next_node": payload.get("next_node"),
                        "reason": reason,
                        "previous_selected_table_ids": previous_selected_table_ids,
                        "previous_recommended_table_ids": previous_recommended_table_ids,
                        "next_rejected_table_ids": list(payload.get("next_rejected_table_ids") or []),
                    }
                ],
            },
            "support_count": 1,
            "confidence_score": 0.35,
        }

    @staticmethod
    def _build_candidate_from_learning_event(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return GovernanceCandidateService._build_table_rejection_candidate(event)

    @staticmethod
    def _merge_candidate_evidence(
        existing: Dict[str, Any],
        event: Dict[str, Any],
    ) -> Dict[str, Any]:
        payload = dict(event.get("payload_json") or {})
        existing_evidence = dict(existing.get("evidence_payload_json") or {})
        event_keys = list(existing_evidence.get("event_keys") or [])
        if event["event_key"] in event_keys:
            return {
                "deduplicated": True,
                "support_count": existing.get("support_count") or len(event_keys) or 1,
                "confidence_score": existing.get("confidence_score") or 0.35,
                "evidence_summary": existing.get("evidence_summary"),
                "evidence_payload_json": existing_evidence,
            }

        event_keys.append(event["event_key"])
        sample_events = list(existing_evidence.get("sample_events") or [])
        action_payload = dict(payload.get("payload") or {})
        sample_events.append(
            {
                "event_key": event["event_key"],
                "query_id": event.get("query_id"),
                "conversation_id": event.get("conversation_id"),
                "current_node": payload.get("current_node"),
                "next_node": payload.get("next_node"),
                "reason": action_payload.get("reason") or action_payload.get("source_text"),
                "previous_selected_table_ids": list(payload.get("previous_selected_table_ids") or []),
                "previous_recommended_table_ids": list(payload.get("previous_recommended_table_ids") or []),
                "next_rejected_table_ids": list(payload.get("next_rejected_table_ids") or []),
            }
        )
        sample_events = sample_events[-20:]

        support_count = len(event_keys)
        confidence_score = min(0.95, round(0.25 + support_count * 0.15, 4))
        reason = action_payload.get("reason") or action_payload.get("source_text")
        evidence_summary = f"表 {existing['target_object_id']} 在统一确认阶段已发生 {support_count} 次换表"
        if reason:
            evidence_summary = f"{evidence_summary}，最新原因：{reason}"

        return {
            "deduplicated": False,
            "support_count": support_count,
            "confidence_score": confidence_score,
            "evidence_summary": evidence_summary,
            "evidence_payload_json": {
                **existing_evidence,
                "event_keys": event_keys,
                "sample_events": sample_events,
            },
        }

    async def list_candidates(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        async with self._acquire() as conn:
            if status:
                rows = await conn.fetch(
                    """
                    SELECT candidate_id, candidate_type, target_object_type, target_object_id,
                           scope_type, scope_id, suggested_change_json, evidence_summary,
                           evidence_payload_json, support_count, confidence_score, status,
                           created_at, reviewed_at, reviewed_by
                    FROM governance_candidates
                    WHERE status = $1
                    ORDER BY support_count DESC, created_at DESC
                    LIMIT $2
                    """,
                    status,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT candidate_id, candidate_type, target_object_type, target_object_id,
                           scope_type, scope_id, suggested_change_json, evidence_summary,
                           evidence_payload_json, support_count, confidence_score, status,
                           created_at, reviewed_at, reviewed_by
                    FROM governance_candidates
                    ORDER BY support_count DESC, created_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
        return [item for item in (self._row_to_dict(row) for row in rows) if item]

    async def count_candidates(
        self,
        *,
        status: Optional[str] = None,
    ) -> int:
        async with self._acquire() as conn:
            if status:
                count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM governance_candidates
                    WHERE status = $1
                    """,
                    status,
                )
            else:
                count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM governance_candidates
                    """
                )
        return int(count or 0)

    async def get_candidate(self, candidate_id: UUID) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT candidate_id, candidate_type, target_object_type, target_object_id,
                       scope_type, scope_id, suggested_change_json, evidence_summary,
                       evidence_payload_json, support_count, confidence_score, status,
                       created_at, reviewed_at, reviewed_by
                FROM governance_candidates
                WHERE candidate_id = $1
                """,
                candidate_id,
            )
        return self._row_to_dict(row)

    async def review_candidate(
        self,
        candidate_id: UUID,
        *,
        action: str,
        reviewer_id: Optional[UUID],
    ) -> Optional[Dict[str, Any]]:
        target_status = CANDIDATE_REVIEW_STATUS_MAP.get(action)
        if not target_status:
            raise ValueError(f"不支持的治理候选审核动作: {action}")

        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE governance_candidates
                SET status = $2,
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = $3
                WHERE candidate_id = $1
                  AND status = 'observed'
                RETURNING candidate_id, candidate_type, target_object_type, target_object_id,
                          scope_type, scope_id, suggested_change_json, evidence_summary,
                          evidence_payload_json, support_count, confidence_score, status,
                          created_at, reviewed_at, reviewed_by
                """,
                candidate_id,
                target_status,
                reviewer_id,
            )
            if not row:
                existing = await conn.fetchrow(
                    """
                    SELECT candidate_id, candidate_type, target_object_type, target_object_id,
                           scope_type, scope_id, suggested_change_json, evidence_summary,
                           evidence_payload_json, support_count, confidence_score, status,
                           created_at, reviewed_at, reviewed_by
                    FROM governance_candidates
                    WHERE candidate_id = $1
                    """,
                    candidate_id,
                )
                existing_candidate = self._row_to_dict(existing)
                if not existing_candidate:
                    return None
                raise ValueError(f"当前治理候选状态 {existing_candidate['status']} 不允许再次审核")

        candidate = self._row_to_dict(row)
        if candidate:
            logger.info(
                "治理候选已审核",
                candidate_id=str(candidate_id),
                action=action,
                status=target_status,
                reviewer_id=str(reviewer_id) if reviewer_id else None,
            )
        return candidate

    async def _find_existing_candidate(
        self,
        conn: asyncpg.Connection,
        *,
        candidate_type: str,
        target_object_type: str,
        target_object_id: str,
        scope_type: str,
        scope_id: Optional[UUID],
    ) -> Optional[Dict[str, Any]]:
        row = await conn.fetchrow(
            """
            SELECT candidate_id, candidate_type, target_object_type, target_object_id,
                   scope_type, scope_id, suggested_change_json, evidence_summary,
                   evidence_payload_json, support_count, confidence_score, status,
                   created_at, reviewed_at, reviewed_by
            FROM governance_candidates
            WHERE candidate_type = $1
              AND target_object_type = $2
              AND target_object_id = $3
              AND scope_type = $4
              AND scope_id IS NOT DISTINCT FROM $5
              AND status = 'observed'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            candidate_type,
            target_object_type,
            target_object_id,
            scope_type,
            scope_id,
        )
        return self._row_to_dict(row)

    async def observe_learning_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        proposal = self._build_candidate_from_learning_event(event)
        if not proposal:
            return {
                "observed": False,
                "reason": "unsupported_event",
                "event_key": event.get("event_key"),
            }

        async with self._acquire() as conn:
            async with conn.transaction():
                existing = await self._find_existing_candidate(
                    conn,
                    candidate_type=proposal["candidate_type"],
                    target_object_type=proposal["target_object_type"],
                    target_object_id=proposal["target_object_id"],
                    scope_type=proposal["scope_type"],
                    scope_id=proposal["scope_id"],
                )

                if not existing:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO governance_candidates (
                            candidate_id, candidate_type, target_object_type, target_object_id,
                            scope_type, scope_id, suggested_change_json, evidence_summary,
                            evidence_payload_json, support_count, confidence_score, status
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, $10, $11, 'observed')
                        RETURNING candidate_id, candidate_type, target_object_type, target_object_id,
                                  scope_type, scope_id, suggested_change_json, evidence_summary,
                                  evidence_payload_json, support_count, confidence_score, status,
                                  created_at, reviewed_at, reviewed_by
                        """,
                        uuid4(),
                        proposal["candidate_type"],
                        proposal["target_object_type"],
                        proposal["target_object_id"],
                        proposal["scope_type"],
                        proposal["scope_id"],
                        json.dumps(sanitize_for_json(proposal["suggested_change_json"]), ensure_ascii=False),
                        proposal["evidence_summary"],
                        json.dumps(sanitize_for_json(proposal["evidence_payload_json"]), ensure_ascii=False),
                        proposal["support_count"],
                        proposal["confidence_score"],
                    )
                    candidate = self._row_to_dict(row) or {}
                    logger.info(
                        "治理候选已创建",
                        candidate_type=proposal["candidate_type"],
                        target_object_id=proposal["target_object_id"],
                        source_event_key=event.get("event_key"),
                    )
                    return {
                        "observed": True,
                        "created": True,
                        "updated": False,
                        "deduplicated": False,
                        "candidate": candidate,
                    }

                merged = self._merge_candidate_evidence(existing, event)
                if merged["deduplicated"]:
                    return {
                        "observed": True,
                        "created": False,
                        "updated": False,
                        "deduplicated": True,
                        "candidate": existing,
                    }

                row = await conn.fetchrow(
                    """
                    UPDATE governance_candidates
                    SET evidence_summary = $2,
                        evidence_payload_json = $3::jsonb,
                        support_count = $4,
                        confidence_score = $5
                    WHERE candidate_id = $1
                    RETURNING candidate_id, candidate_type, target_object_type, target_object_id,
                              scope_type, scope_id, suggested_change_json, evidence_summary,
                              evidence_payload_json, support_count, confidence_score, status,
                              created_at, reviewed_at, reviewed_by
                    """,
                    UUID(existing["candidate_id"]),
                    merged["evidence_summary"],
                    json.dumps(sanitize_for_json(merged["evidence_payload_json"]), ensure_ascii=False),
                    merged["support_count"],
                    merged["confidence_score"],
                )
                candidate = self._row_to_dict(row) or {}
                logger.info(
                    "治理候选已更新",
                    candidate_id=existing["candidate_id"],
                    target_object_id=existing["target_object_id"],
                    support_count=merged["support_count"],
                    source_event_key=event.get("event_key"),
                )
                return {
                    "observed": True,
                    "created": False,
                    "updated": True,
                    "deduplicated": False,
                    "candidate": candidate,
                }

    async def observe_recent_learning_events(self, *, limit: int = 100) -> Dict[str, Any]:
        async with self._acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_id, event_key, query_id, conversation_id, user_id,
                       event_type, event_version, payload_json, source_component, created_at
                FROM learning_events
                WHERE event_type = 'action_applied'
                ORDER BY created_at ASC
                LIMIT $1
                """,
                limit,
            )

        events = [
            {
                "event_id": str(row["event_id"]),
                "event_key": row["event_key"],
                "query_id": str(row["query_id"]) if row["query_id"] else None,
                "conversation_id": str(row["conversation_id"]) if row["conversation_id"] else None,
                "user_id": str(row["user_id"]) if row["user_id"] else None,
                "event_type": row["event_type"],
                "event_version": row["event_version"],
                "payload_json": row["payload_json"] or {},
                "source_component": row["source_component"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

        created = 0
        updated = 0
        deduplicated = 0
        ignored = 0
        observed_candidates: List[Dict[str, Any]] = []

        for event in events:
            result = await self.observe_learning_event(event)
            if not result["observed"]:
                ignored += 1
                continue
            if result["created"]:
                created += 1
            elif result["updated"]:
                updated += 1
            elif result["deduplicated"]:
                deduplicated += 1
            observed_candidates.append(result["candidate"])

        return {
            "scanned_events": len(events),
            "created_candidates": created,
            "updated_candidates": updated,
            "deduplicated_events": deduplicated,
            "ignored_events": ignored,
            "candidates": observed_candidates,
        }
