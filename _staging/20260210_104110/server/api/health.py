"""健康检查 API"""

from fastapi import APIRouter, Depends
from typing import Dict, Any
import structlog

from server.dependencies import get_query_cache

logger = structlog.get_logger()
router = APIRouter()


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    健康检查
    
    Returns:
        {
            "status": "healthy" | "unhealthy",
            "components": {
                "database": "ok" | "error",
                "redis": "ok" | "error"
            }
        }
    """
    components = {}
    overall_status = "healthy"
    
    # 检查数据库
    try:
        from server.exec.connection import get_connection_manager
        conn_mgr = get_connection_manager()
        from sqlalchemy import text

        conn = await conn_mgr.get_connection()
        try:
            await conn.execute(text("SELECT 1"))
            components["database"] = "ok"
        finally:
            await conn.close()
    except Exception as e:
        logger.error("数据库健康检查失败", error=str(e))
        components["database"] = f"error: {str(e)}"
        overall_status = "unhealthy"
    
    # 检查 Redis（如果启用）
    from server.config import settings
    if settings.cache_enabled:
        try:
            from server.exec.cache import QueryCache
            cache = QueryCache()
            await cache.initialize()
            if cache.client:
                await cache.client.ping()
                components["redis"] = "ok"
                await cache.close()
            else:
                components["redis"] = "disabled"
        except Exception as e:
            logger.error("Redis 健康检查失败", error=str(e))
            components["redis"] = f"error: {str(e)}"
            # Redis 失败不影响整体健康状态（降级可用）
    else:
        components["redis"] = "disabled"
    
    return {
        "status": overall_status,
        "components": components
    }


@router.get("/ready")
async def readiness_check() -> Dict[str, Any]:
    """
    就绪检查 - 用于 K8s readiness probe
    """
    # 简化版：只要服务启动就认为就绪
    return {"status": "ready"}

