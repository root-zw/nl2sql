"""
调试API - 提供查询追踪和诊断功能
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel

from server.trace.tracer import get_tracer, get_all_tracers, clear_tracers

router = APIRouter()


class TraceResponse(BaseModel):
    """追踪响应"""
    success: bool
    data: Optional[dict] = None
    markdown: Optional[str] = None


@router.get("/debug/traces")
async def list_traces(
    limit: int = Query(default=10, ge=1, le=100)
):
    """
    获取最近的查询追踪列表
    
    Args:
        limit: 返回数量限制
    """
    tracers = get_all_tracers()
    
    # 按时间倒序排序
    tracers_sorted = sorted(
        tracers,
        key=lambda t: t.start_time,
        reverse=True
    )[:limit]
    
    return {
        "success": True,
        "total": len(tracers),
        "traces": [
            {
                "query_id": t.query_id,
                "question": t.question,
                "connection_id": t.connection_id,
                "start_time": t.start_time.isoformat(),
                "total_duration_ms": round(t.total_duration_ms, 2) if t.total_duration_ms else None,
                "total_steps": len(t.steps),
                "has_error": any(s.error for s in t.steps)
            }
            for t in tracers_sorted
        ]
    }


@router.get("/debug/traces/{query_id}")
async def get_trace_detail(
    query_id: str,
    format: str = Query(default="json", regex="^(json|markdown)$")
):
    """
    获取指定查询的详细追踪信息
    
    Args:
        query_id: 查询ID
        format: 返回格式（json或markdown）
    """
    tracer = get_tracer(query_id)
    
    if not tracer:
        raise HTTPException(
            status_code=404,
            detail=f"查询追踪不存在: {query_id}"
        )
    
    if format == "markdown":
        return {
            "success": True,
            "query_id": query_id,
            "markdown": tracer.to_markdown()
        }
    else:
        return {
            "success": True,
            "data": tracer.to_dict()
        }


@router.delete("/debug/traces")
async def clear_all_traces():
    """
    清空所有查询追踪
    """
    count = len(get_all_tracers())
    clear_tracers()
    
    return {
        "success": True,
        "message": f"已清空 {count} 条追踪记录"
    }


@router.get("/debug/traces/{query_id}/steps/{step_index}")
async def get_trace_step_detail(
    query_id: str,
    step_index: int
):
    """
    获取指定步骤的详细信息
    
    Args:
        query_id: 查询ID
        step_index: 步骤索引（从0开始）
    """
    tracer = get_tracer(query_id)
    
    if not tracer:
        raise HTTPException(
            status_code=404,
            detail=f"查询追踪不存在: {query_id}"
        )
    
    if step_index < 0 or step_index >= len(tracer.steps):
        raise HTTPException(
            status_code=404,
            detail=f"步骤索引超出范围: {step_index}"
        )
    
    step = tracer.steps[step_index]
    
    return {
        "success": True,
        "data": step.to_dict()
    }

