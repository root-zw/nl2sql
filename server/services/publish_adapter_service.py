"""
发布适配服务

当前阶段先补最小发布审计层：
- 已批准治理候选可以尝试进入发布接口
- 即使当前无法自动回写正式层，也必须留下 release_runs 审计
- 对于仅表示“需要人工复核”的候选，明确阻断发布并返回阻断原因
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import UUID, uuid4

import asyncpg
import structlog

from server.services.governance_candidate_service import GovernanceCandidateService
from server.utils.db_pool import get_metadata_pool
from server.utils.json_utils import sanitize_for_json

logger = structlog.get_logger()

RELEASE_TYPE_GOVERNANCE_CANDIDATE_PUBLISH = "governance_candidate_publish"
SOURCE_TYPE_GOVERNANCE_CANDIDATE = "governance_candidate"


class PublishAdapterService:
    """治理候选发布适配服务"""

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
        return {
            "release_run_id": str(row["release_run_id"]),
            "release_type": row["release_type"],
            "source_type": row["source_type"],
            "source_ids_json": row["source_ids_json"] or [],
            "policy_snapshot_id": str(row["policy_snapshot_id"]) if row["policy_snapshot_id"] else None,
            "status": row["status"],
            "plan_json": row["plan_json"] or {},
            "result_json": row["result_json"] or {},
            "triggered_by": str(row["triggered_by"]) if row["triggered_by"] else None,
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
        }

    async def list_release_runs(
        self,
        *,
        source_type: Optional[str] = None,
        source_id: Optional[UUID] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        async with self._acquire() as conn:
            if source_type and source_id:
                rows = await conn.fetch(
                    """
                    SELECT release_run_id, release_type, source_type, source_ids_json,
                           policy_snapshot_id, status, plan_json, result_json,
                           triggered_by, created_at, started_at, completed_at
                    FROM release_runs
                    WHERE source_type = $1
                      AND source_ids_json @> $2::jsonb
                    ORDER BY created_at DESC
                    LIMIT $3
                    """,
                    source_type,
                    json.dumps([str(source_id)], ensure_ascii=False),
                    limit,
                )
            elif source_type:
                rows = await conn.fetch(
                    """
                    SELECT release_run_id, release_type, source_type, source_ids_json,
                           policy_snapshot_id, status, plan_json, result_json,
                           triggered_by, created_at, started_at, completed_at
                    FROM release_runs
                    WHERE source_type = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    source_type,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT release_run_id, release_type, source_type, source_ids_json,
                           policy_snapshot_id, status, plan_json, result_json,
                           triggered_by, created_at, started_at, completed_at
                    FROM release_runs
                    ORDER BY created_at DESC
                    LIMIT $1
                    """,
                    limit,
                )

        return [item for item in (self._row_to_dict(row) for row in rows) if item]

    @staticmethod
    def _build_candidate_publish_plan(candidate: Dict[str, Any]) -> Dict[str, Any]:
        suggested_change = dict(candidate.get("suggested_change_json") or {})
        target_object_type = candidate.get("target_object_type")
        target_object_id = candidate.get("target_object_id")
        target_entry = {
            "target_object_type": target_object_type,
            "target_object_id": target_object_id,
        }

        formal_targets: List[Dict[str, Any]] = []
        if target_object_type == "table" and target_object_id:
            formal_targets.append(
                {
                    **target_entry,
                    "formal_table": "db_tables",
                }
            )

        action = suggested_change.get("action")
        block_code = "unsupported_candidate_action"
        block_message = "当前治理候选尚未形成可自动回写的正式层变更载荷"
        recommended_entry = None

        if action == "review_table_metadata":
            block_code = "manual_review_required"
            block_message = (
                "当前治理候选只表达表级元数据需要人工复核，尚未形成可自动回写的正式层补丁"
            )
            recommended_entry = {
                "entry_type": "admin_page",
                "route": "/admin/unified-metadata",
                **target_entry,
            }

        return {
            "publishable": False,
            "candidate_type": candidate.get("candidate_type"),
            "suggested_action": action,
            "formal_targets": formal_targets,
            "blocking_reasons": [
                {
                    "code": block_code,
                    "message": block_message,
                }
            ],
            "recommended_entry": recommended_entry,
        }

    async def _create_release_run(
        self,
        conn: asyncpg.Connection,
        *,
        release_type: str,
        source_type: str,
        source_ids_json: List[str],
        status: str,
        plan_json: Dict[str, Any],
        result_json: Dict[str, Any],
        triggered_by: Optional[UUID],
    ) -> Dict[str, Any]:
        row = await conn.fetchrow(
            """
            INSERT INTO release_runs (
                release_run_id, release_type, source_type, source_ids_json,
                policy_snapshot_id, status, plan_json, result_json,
                triggered_by, started_at, completed_at
            )
            VALUES ($1, $2, $3, $4::jsonb, NULL, $5, $6::jsonb, $7::jsonb, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING release_run_id, release_type, source_type, source_ids_json,
                      policy_snapshot_id, status, plan_json, result_json,
                      triggered_by, created_at, started_at, completed_at
            """,
            uuid4(),
            release_type,
            source_type,
            json.dumps(sanitize_for_json(source_ids_json), ensure_ascii=False),
            status,
            json.dumps(sanitize_for_json(plan_json), ensure_ascii=False),
            json.dumps(sanitize_for_json(result_json), ensure_ascii=False),
            triggered_by,
        )
        return self._row_to_dict(row) or {}

    async def publish_governance_candidate(
        self,
        candidate_id: UUID,
        *,
        triggered_by: Optional[UUID],
    ) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            candidate = await GovernanceCandidateService(conn).get_candidate(candidate_id)
            if not candidate:
                return None

            if candidate["status"] != "approved":
                raise ValueError(f"当前治理候选状态 {candidate['status']} 不允许发布")

            plan_json = self._build_candidate_publish_plan(candidate)
            if plan_json["publishable"]:
                raise ValueError("当前版本尚未接通自动发布处理器")

            result_json = {
                "outcome": "blocked",
                "published": False,
                "message": "当前治理候选未进入正式层，已记录最小发布审计并返回阻断原因",
                "blocking_reasons": plan_json.get("blocking_reasons") or [],
            }
            release_run = await self._create_release_run(
                conn,
                release_type=RELEASE_TYPE_GOVERNANCE_CANDIDATE_PUBLISH,
                source_type=SOURCE_TYPE_GOVERNANCE_CANDIDATE,
                source_ids_json=[candidate["candidate_id"]],
                status="blocked",
                plan_json=plan_json,
                result_json=result_json,
                triggered_by=triggered_by,
            )

        logger.info(
            "治理候选发布已阻断并留痕",
            candidate_id=str(candidate_id),
            candidate_type=candidate.get("candidate_type"),
            triggered_by=str(triggered_by) if triggered_by else None,
            release_run_id=release_run.get("release_run_id"),
        )
        return {
            "candidate": candidate,
            "release_run": release_run,
        }
