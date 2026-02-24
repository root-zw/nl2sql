"""缓存管理 API"""

from typing import Dict, Any, List
import structlog
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi import status as http_status
from uuid import UUID

from server.dependencies import get_metadata_manager
from server.models.database import UserRole
from server.api.admin.auth import require_role

logger = structlog.get_logger()
router = APIRouter()

# 简化：管理员权限检查
require_admin = require_role(UserRole.ADMIN)


@router.post("/cache/invalidate/{connection_id}")
async def invalidate_connection_cache(
    connection_id: UUID,
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """
    使指定连接的缓存失效
    
    Args:
        connection_id: 数据库连接ID
    """
    try:
        manager = get_metadata_manager()
        if not manager:
            return {
                "success": False,
                "message": "元数据管理器未初始化"
            }
        
        cache_key = f"conn_{connection_id}"
        
        if cache_key in manager._cache:
            del manager._cache[cache_key]
            logger.info("连接缓存已失效", connection_id=str(connection_id))
            invalidated = True
        else:
            logger.info("缓存不存在，无需失效", connection_id=str(connection_id))
            invalidated = False
        
        return {
            "success": True,
            "message": "缓存失效成功" if invalidated else "缓存不存在",
            "connection_id": str(connection_id),
            "invalidated": invalidated
        }
        
    except Exception as e:
        logger.exception("缓存失效失败", connection_id=str(connection_id), error=str(e))
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"缓存失效失败: {str(e)}"
        )


@router.post("/cache/invalidate-all")
async def invalidate_all_cache(
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """
    清空所有缓存
    """
    try:
        manager = get_metadata_manager()
        if not manager:
            return {
                "success": False,
                "message": "元数据管理器未初始化"
            }
        
        cache_count = len(manager._cache)
        manager._cache.clear()
        
        logger.info("所有缓存已清空", count=cache_count)
        
        return {
            "success": True,
            "message": f"已清空 {cache_count} 个缓存项",
            "count": cache_count
        }
        
    except Exception as e:
        logger.exception("清空缓存失败", error=str(e))
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清空缓存失败: {str(e)}"
        )


@router.get("/cache/stats")
async def get_cache_stats(
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """
    获取缓存统计信息
    """
    try:
        manager = get_metadata_manager()
        if not manager:
            return {
                "success": False,
                "message": "元数据管理器未初始化"
            }
        
        cache_items = []
        for key, (model, expire_time) in manager._cache.items():
            from server.utils.timezone_helper import now_with_tz
            remaining_seconds = int((expire_time - now_with_tz()).total_seconds())
            
            cache_items.append({
                "cache_key": key,
                "domains": len(model.domains) if hasattr(model, 'domains') else 0,
                "datasources": len(model.datasources) if hasattr(model, 'datasources') else 0,
                "fields": len(model.fields) if hasattr(model, 'fields') else 0,
                "metrics": len(model.metrics) if hasattr(model, 'metrics') else 0,
                "ttl_seconds": remaining_seconds,
                "is_expired": remaining_seconds <= 0
            })
        
        return {
            "success": True,
            "total_items": len(cache_items),
            "cache_ttl": manager.cache_ttl,
            "items": cache_items
        }
        
    except Exception as e:
        logger.exception("获取缓存统计失败", error=str(e))
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取缓存统计失败: {str(e)}"
        )


@router.post("/cache/reload/{connection_id}")
async def reload_connection_cache(
    connection_id: UUID,
    background_tasks: BackgroundTasks,
    sync_milvus: bool = True,
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """
    重新加载指定连接的缓存，并可选同步到Milvus
    
    Args:
        connection_id: 数据库连接ID
        sync_milvus: 是否同步到Milvus（默认True）
    """
    try:
        manager = get_metadata_manager()
        if not manager:
            raise HTTPException(
                status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="元数据管理器未初始化"
            )
        
        # 1. 强制重新加载
        logger.info("开始重新加载缓存", connection_id=str(connection_id))
        model = await manager.get_connection_model(
            str(connection_id),
            force_reload=True
        )
        
        stats = {
            "domains": len(model.domains) if hasattr(model, 'domains') else 0,
            "datasources": len(model.datasources) if hasattr(model, 'datasources') else 0,
            "fields": len(model.fields) if hasattr(model, 'fields') else 0,
            "metrics": len(model.metrics) if hasattr(model, 'metrics') else 0
        }
        
        # 2. 可选：同步到Milvus（后台任务）
        if sync_milvus:
            from server.api.admin.milvus import sync_to_milvus_task
            background_tasks.add_task(
                sync_to_milvus_task,
                connection_id
            )
            logger.info("已添加Milvus同步任务", connection_id=str(connection_id))
        
        return {
            "success": True,
            "message": "缓存重新加载成功",
            "connection_id": str(connection_id),
            "stats": stats,
            "milvus_sync_scheduled": sync_milvus
        }
        
    except Exception as e:
        logger.exception("重新加载缓存失败", connection_id=str(connection_id), error=str(e))
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"重新加载缓存失败: {str(e)}"
        )

