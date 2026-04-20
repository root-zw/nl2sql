"""
治理候选管理 API

当前提供最小观察层能力：
- 查看治理候选列表
- 手动触发基于 learning_events 的候选观察
- 对已观察到的治理候选执行最小审核动作
- 对已批准候选尝试发布，并记录最小发布审计
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser
from server.services.governance_candidate_service import GovernanceCandidateService
from server.services.publish_adapter_service import (
    PublishAdapterService,
    SOURCE_TYPE_GOVERNANCE_CANDIDATE,
)
from server.utils.db_pool import get_metadata_pool


router = APIRouter(prefix="/governance-candidates", tags=["治理候选管理"])


async def get_db_pool():
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


class GovernanceCandidateItem(BaseModel):
    candidate_id: str
    candidate_type: str
    target_object_type: str
    target_object_id: str
    scope_type: str
    scope_id: Optional[str] = None
    suggested_change_json: Dict[str, Any] = Field(default_factory=dict)
    evidence_summary: Optional[str] = None
    evidence_payload_json: Dict[str, Any] = Field(default_factory=dict)
    support_count: int
    confidence_score: Optional[float] = None
    status: str
    created_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = None
    reviewed_by: Optional[str] = None


class GovernanceCandidateListResponse(BaseModel):
    items: List[GovernanceCandidateItem]
    total_count: int


class ObserveLearningEventsRequest(BaseModel):
    limit: int = Field(100, ge=1, le=500, description="本次最多扫描的事件数量")


class ObserveLearningEventsResponse(BaseModel):
    scanned_events: int
    created_candidates: int
    updated_candidates: int
    deduplicated_events: int
    ignored_events: int
    candidates: List[GovernanceCandidateItem]


class ReviewGovernanceCandidateRequest(BaseModel):
    action: Literal["approve", "reject"] = Field(..., description="治理候选审核动作")


class ReleaseRunItem(BaseModel):
    release_run_id: str
    release_type: str
    source_type: str
    source_ids_json: List[str] = Field(default_factory=list)
    policy_snapshot_id: Optional[str] = None
    status: str
    plan_json: Dict[str, Any] = Field(default_factory=dict)
    result_json: Dict[str, Any] = Field(default_factory=dict)
    triggered_by: Optional[str] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class GovernanceCandidateReleaseRunListResponse(BaseModel):
    items: List[ReleaseRunItem]
    total_count: int


class PublishGovernanceCandidateResponse(BaseModel):
    candidate: GovernanceCandidateItem
    release_run: ReleaseRunItem


@router.get("", response_model=GovernanceCandidateListResponse)
async def list_governance_candidates(
    status: Optional[str] = Query(None, description="按候选状态筛选"),
    limit: int = Query(50, ge=1, le=200, description="返回条数"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool),
):
    service = GovernanceCandidateService(db)
    items = await service.list_candidates(status=status, limit=limit)
    return GovernanceCandidateListResponse(
        items=[GovernanceCandidateItem(**item) for item in items],
        total_count=len(items),
    )


@router.post("/observe-learning-events", response_model=ObserveLearningEventsResponse)
async def observe_learning_events(
    request: ObserveLearningEventsRequest,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool),
):
    service = GovernanceCandidateService(db)
    result = await service.observe_recent_learning_events(limit=request.limit)
    return ObserveLearningEventsResponse(
        scanned_events=result["scanned_events"],
        created_candidates=result["created_candidates"],
        updated_candidates=result["updated_candidates"],
        deduplicated_events=result["deduplicated_events"],
        ignored_events=result["ignored_events"],
        candidates=[GovernanceCandidateItem(**item) for item in result["candidates"]],
    )


@router.post("/{candidate_id}/review", response_model=GovernanceCandidateItem)
async def review_governance_candidate(
    candidate_id: str,
    request: ReviewGovernanceCandidateRequest,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool),
):
    service = GovernanceCandidateService(db)
    try:
        item = await service.review_candidate(
            UUID(candidate_id),
            action=request.action,
            reviewer_id=current_user.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if not item:
        raise HTTPException(status_code=404, detail="治理候选不存在")

    return GovernanceCandidateItem(**item)


@router.get("/{candidate_id}/release-runs", response_model=GovernanceCandidateReleaseRunListResponse)
async def list_governance_candidate_release_runs(
    candidate_id: str,
    limit: int = Query(20, ge=1, le=100, description="返回条数"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool),
):
    service = PublishAdapterService(db)
    items = await service.list_release_runs(
        source_type=SOURCE_TYPE_GOVERNANCE_CANDIDATE,
        source_id=UUID(candidate_id),
        limit=limit,
    )
    return GovernanceCandidateReleaseRunListResponse(
        items=[ReleaseRunItem(**item) for item in items],
        total_count=len(items),
    )


@router.post("/{candidate_id}/publish", response_model=PublishGovernanceCandidateResponse)
async def publish_governance_candidate(
    candidate_id: str,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool),
):
    service = PublishAdapterService(db)
    try:
        result = await service.publish_governance_candidate(
            UUID(candidate_id),
            triggered_by=current_user.user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if not result:
        raise HTTPException(status_code=404, detail="治理候选不存在")

    return PublishGovernanceCandidateResponse(
        candidate=GovernanceCandidateItem(**result["candidate"]),
        release_run=ReleaseRunItem(**result["release_run"]),
    )
