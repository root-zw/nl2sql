"""
自动同步API路由
提供自动同步的管理接口
"""

import json
from datetime import datetime
from typing import Dict, List, Optional
from server.utils.timezone_helper import now_with_tz
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Body, Query
import asyncpg
from asyncpg import Pool
import structlog
from pydantic import BaseModel

from server.models.sync import (
    SyncType, TriggeredBy, SyncStatus, EntityType,
    ManualSyncRequest, SyncHistory, SyncHealthStatus,
    PendingChangesStats, AutoSyncRequest, AutoSyncResponse
)
from server.sync.unified_sync_service import UnifiedSyncService
from server.sync.auto_sync_trigger import AutoSyncTrigger
from server.dependencies import get_redis_client, get_milvus_client, get_embedding_client, get_db_pool as get_system_db_pool
from server.config import settings
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser

logger = structlog.get_logger()
router = APIRouter()


class ManualSyncPayload(BaseModel):
    sync_domains: bool = True
    sync_tables: bool = True
    sync_fields: bool = True  # 同步字段选项
    sync_enums: bool = True
    sync_few_shot: bool = True
    force_full_sync: bool = False
    domain_ids: Optional[List[UUID]] = None
    table_ids: Optional[List[UUID]] = None
    field_ids: Optional[List[UUID]] = None
    dry_run: bool = False


async def get_sync_db_pool():
    """获取同步服务专用数据库连接（使用 yield 确保连接自动关闭）"""
    from server.config import settings
    conn = await asyncpg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        database=settings.postgres_db
    )
    try:
        yield conn
    finally:
        await conn.close()

# 全局同步服务实例
_sync_service: Optional[UnifiedSyncService] = None


def get_sync_service() -> UnifiedSyncService:
    """获取统一同步服务实例"""
    global _sync_service
    if _sync_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="同步服务未初始化"
        )
    return _sync_service


async def init_sync_service():
    """初始化同步服务"""
    global _sync_service
    try:
        # 获取数据库连接池
        db_pool = await get_system_db_pool()

        # 获取Redis客户端
        redis_client = None
        if hasattr(settings, 'redis_url') and settings.redis_url:
            try:
                import redis
                redis_client = redis.from_url(settings.redis_url, decode_responses=True)
            except Exception as e:
                logger.warning("Redis连接失败，将使用本地锁机制", error=str(e))

        # 获取Milvus和Embedding客户端
        from server.api.admin.milvus import get_milvus_client, get_embedding_client
        milvus_client = await get_milvus_client()
        embedding_client = await get_embedding_client()

        if _sync_service is None:
            _sync_service = UnifiedSyncService(
                db_pool=db_pool,
                redis_client=redis_client,
                milvus_client=milvus_client,
                embedding_client=embedding_client
            )
            await _sync_service.start()

        logger.debug("自动同步服务初始化成功")

    except Exception as e:
        logger.error("初始化自动同步服务失败", error=str(e))
        _sync_service = None


async def cleanup_sync_service():
    """清理同步服务"""
    global _sync_service
    if _sync_service:
        await _sync_service.stop()
        _sync_service = None
        logger.debug("自动同步服务已清理")


# 初始化事件已移至main.py，避免重复初始化


@router.post("/auto-sync/trigger")
async def trigger_auto_sync(
    request: AutoSyncRequest = Body(...),
    current_user: AdminUser = Depends(require_data_admin),
    sync_service: UnifiedSyncService = Depends(get_sync_service)
) -> AutoSyncResponse:
    """
    触发自动同步

    Args:
        request: 自动同步请求

    Returns:
        AutoSyncResponse: 同步响应
    """
    try:
        logger.debug(
            "收到自动同步请求",
            connection_id=str(request.connection_id),
            changes_count=len(request.entity_changes)
        )

        # 触发自动同步
        sync_id = await sync_service.trigger_auto_sync(
            request.connection_id,
            request.entity_changes
        )

        if sync_id:
            return AutoSyncResponse(
                success=True,
                sync_id=sync_id,
                message="自动同步已触发",
                stats={"sync_id": str(sync_id), "changes_count": len(request.entity_changes)}
            )
        else:
            return AutoSyncResponse(
                success=False,
                sync_id=None,
                message="触发自动同步失败",
                stats={}
            )

    except Exception as e:
        logger.exception("触发自动同步失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"触发自动同步失败: {str(e)}"
        )


@router.post("/manual-sync/{connection_id}")
async def trigger_manual_sync(
    connection_id: UUID,
    payload: ManualSyncPayload = Body(default_factory=ManualSyncPayload),
    current_user: AdminUser = Depends(require_data_admin),
    sync_service: UnifiedSyncService = Depends(get_sync_service),
    db = Depends(get_sync_db_pool)
) -> dict:
    """
    触发手动同步

    Args:
        connection_id: 连接ID
        sync_domains: 是否同步业务域
        sync_tables: 是否同步表
        sync_enums: 是否同步枚举值
        force_full_sync: 是否强制全量同步
        domain_ids: 指定的业务域ID列表
        table_ids: 指定的表ID列表
        field_ids: 指定的字段ID列表
        dry_run: 是否仅检查不执行
        sync_service: 统一同步服务
        db: 数据库连接

    Returns:
        dict: 同步响应
    """
    try:
        logger.debug(
            "收到手动同步请求",
            connection_id=str(connection_id),
            sync_domains=payload.sync_domains,
            sync_tables=payload.sync_tables,
            sync_enums=payload.sync_enums,
            force_full_sync=payload.force_full_sync,
            dry_run=payload.dry_run
        )

        # 验证连接是否存在
        connection_exists = await db.fetchval("""
            SELECT EXISTS(
                SELECT 1 FROM database_connections 
                WHERE connection_id = $1 AND is_active = TRUE
            )
        """, connection_id)
        
        if not connection_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库连接 {connection_id} 不存在或未激活"
            )

        # 构建手动同步请求
        manual_request = ManualSyncRequest(
            connection_id=connection_id,
            **payload.model_dump()
        )

        if payload.dry_run:
            # 仅检查，不执行
            pending_changes = await sync_service.get_pending_changes(connection_id)
            return {
                "success": True,
                "message": "检查完成（模拟运行）",
                "dry_run": True,
                "pending_changes": len(pending_changes),
                "sync_id": None
            }

        # 触发手动同步
        sync_id = await sync_service.trigger_manual_sync(manual_request)

        if sync_id:
            return {
                "success": True,
                "message": "手动同步已触发",
                "sync_id": str(sync_id),
                "dry_run": False
            }
        else:
            return {
                "success": False,
                "message": "触发手动同步失败：连接不存在或未激活",
                "sync_id": None,
                "dry_run": False
            }

    except Exception as e:
        logger.exception("触发手动同步失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"触发手动同步失败: {str(e)}"
        )


@router.get("/sync-status/{sync_id}")
async def get_sync_status(
    sync_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    sync_service: UnifiedSyncService = Depends(get_sync_service)
) -> dict:
    """
    获取同步状态

    Args:
        sync_id: 同步任务ID
        sync_service: 统一同步服务

    Returns:
        dict: 同步状态信息
    """
    try:
        sync_history = await sync_service.get_sync_status(sync_id)

        if not sync_history:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"同步任务 {sync_id} 不存在"
            )

        return {
            "success": True,
            "sync_id": str(sync_history.sync_id),
            "connection_id": str(sync_history.connection_id),
            "sync_type": sync_history.sync_type.value,
            "triggered_by": sync_history.triggered_by.value,
            "status": sync_history.status.value,
            "started_at": sync_history.started_at.isoformat() if sync_history.started_at else None,
            "completed_at": sync_history.completed_at.isoformat() if sync_history.completed_at else None,
            "duration_seconds": sync_history.duration_seconds,
            "total_entities": sync_history.total_entities,
            "synced_entities": sync_history.synced_entities,
            "failed_entities": sync_history.failed_entities,
            "current_step": sync_history.current_step,
            "progress_percentage": sync_history.progress_percentage,
            "error_message": sync_history.error_message,
            "error_details": sync_history.error_details
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取同步状态失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取同步状态失败: {str(e)}"
        )


@router.get("/pending-changes/{connection_id}")
async def get_pending_changes(
    connection_id: UUID,
    entity_types: Optional[List[str]] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    current_user: AdminUser = Depends(require_data_admin),
    sync_service: UnifiedSyncService = Depends(get_sync_service)
) -> dict:
    """
    获取待同步变更

    Args:
        connection_id: 连接ID
        entity_types: 实体类型过滤
        limit: 返回数量限制
        sync_service: 统一同步服务

    Returns:
        dict: 待同步变更信息
    """
    try:
        payload = await build_pending_changes_payload(
            connection_id,
            sync_service,
            limit=limit,
            entity_types=entity_types
        )

        return {"success": True, **payload}

    except Exception as e:
        logger.exception("获取待同步变更失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取待同步变更失败: {str(e)}"
        )


@router.get("/sync-health/{connection_id}")
async def get_sync_health_status(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    sync_service: UnifiedSyncService = Depends(get_sync_service)
) -> dict:
    """
    获取同步健康状态

    Args:
        connection_id: 连接ID
        sync_service: 统一同步服务

    Returns:
        dict: 健康状态信息
    """
    try:
        payload = await build_sync_health_payload(connection_id, sync_service)

        return {"success": True, **payload}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取同步健康状态失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取同步健康状态失败: {str(e)}"
        )


@router.delete("/sync/{sync_id}")
async def cancel_sync(
    sync_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    sync_service: UnifiedSyncService = Depends(get_sync_service)
) -> dict:
    """
    取消同步任务

    Args:
        sync_id: 同步任务ID
        sync_service: 统一同步服务

    Returns:
        dict: 取消结果
    """
    try:
        success = await sync_service.cancel_sync(sync_id)

        if success:
            return {
                "success": True,
                "message": f"同步任务 {sync_id} 已取消",
                "sync_id": str(sync_id)
            }
        else:
            return {
                "success": False,
                "message": f"无法取消同步任务 {sync_id}（可能已完成或不存在）",
                "sync_id": str(sync_id)
            }

    except Exception as e:
        logger.exception("取消同步任务失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"取消同步任务失败: {str(e)}"
        )


@router.get("/sync-history/{connection_id}")
async def get_sync_history(
    connection_id: UUID,
    limit: int = Query(20, ge=1, le=100),
    status_filter: Optional[str] = Query(None),
    current_user: AdminUser = Depends(require_data_admin),
    db_pool: Pool = Depends(get_sync_db_pool)
) -> dict:
    """
    获取同步历史

    Args:
        connection_id: 连接ID
        limit: 返回数量限制
        status_filter: 状态过滤
        db_pool: 数据库连接池

    Returns:
        dict: 同步历史列表
    """
    try:
        # 构建查询条件
        status_condition = ""
        params = [connection_id, limit]

        if status_filter:
            status_condition = "AND status = $3"
            params.append(status_filter)

        query = f"""
            SELECT
                sync_id, sync_type, triggered_by, status,
                started_at, completed_at, duration_seconds,
                total_entities, synced_entities, failed_entities,
                error_message, current_step, progress_percentage
            FROM milvus_sync_history
            WHERE connection_id = $1 {status_condition}
            ORDER BY started_at DESC
            LIMIT $2
        """

        rows = await db_pool.fetch(query, *params)

        history_list = []
        for row in rows:
            history_list.append({
                "sync_id": str(row['sync_id']),
                "sync_type": row['sync_type'],
                "triggered_by": row['triggered_by'],
                "status": row['status'],
                "started_at": row['started_at'].isoformat() if row['started_at'] else None,
                "completed_at": row['completed_at'].isoformat() if row['completed_at'] else None,
                "duration_seconds": row['duration_seconds'],
                "total_entities": row['total_entities'],
                "synced_entities": row['synced_entities'],
                "failed_entities": row['failed_entities'],
                "error_message": row['error_message'],
                "current_step": row['current_step'],
                "progress_percentage": row['progress_percentage']
            })

        return {
            "success": True,
            "connection_id": str(connection_id),
            "history": history_list,
            "total_count": len(history_list),
            "limit": limit
        }

    except Exception as e:
        logger.exception("获取同步历史失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取同步历史失败: {str(e)}"
        )


@router.get("/queue-status")
async def get_queue_status(
    current_user: AdminUser = Depends(require_data_admin),
    sync_service: UnifiedSyncService = Depends(get_sync_service)
) -> dict:
    """
    获取同步队列状态

    Args:
        sync_service: 统一同步服务

    Returns:
        dict: 队列状态信息
    """
    try:
        queue_status = await sync_service.concurrency.get_queue_status()
        active_resources = await sync_service.resource_manager.get_active_resources()

        return {
            "success": True,
            "queue_status": queue_status,
            "active_resources": active_resources,
            "timestamp": now_with_tz().isoformat()
        }

    except Exception as e:
        logger.exception("获取队列状态失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取队列状态失败: {str(e)}"
        )


@router.post("/sync-config/{connection_id}")
async def update_sync_config(
    connection_id: UUID,
    config_data: dict = Body(...),
    current_user: AdminUser = Depends(require_data_admin),
    db_pool: Pool = Depends(get_sync_db_pool)
) -> dict:
    """
    更新同步配置

    Args:
        connection_id: 连接ID
        config_data: 配置数据
        db_pool: 数据库连接池

    Returns:
        dict: 更新结果
    """
    try:
        # 更新配置
        await db_pool.execute("""
            INSERT INTO milvus_sync_config (
                connection_id, auto_sync_enabled, auto_sync_mode,
                auto_sync_domains, auto_sync_tables, auto_sync_fields,
                auto_sync_enums, auto_sync_few_shot,
                batch_window_seconds,
                max_batch_size, sync_timeout_seconds, domain_priority,
                table_priority, field_priority, enum_priority,
                min_sync_interval_seconds, max_retry_attempts, retry_delay_seconds,
                inherits_global, global_setting_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, FALSE, NULL)
            ON CONFLICT (connection_id) DO UPDATE SET
                auto_sync_enabled = EXCLUDED.auto_sync_enabled,
                auto_sync_mode = EXCLUDED.auto_sync_mode,
                auto_sync_domains = EXCLUDED.auto_sync_domains,
                auto_sync_tables = EXCLUDED.auto_sync_tables,
                auto_sync_fields = EXCLUDED.auto_sync_fields,
                auto_sync_enums = EXCLUDED.auto_sync_enums,
                auto_sync_few_shot = EXCLUDED.auto_sync_few_shot,
                batch_window_seconds = EXCLUDED.batch_window_seconds,
                max_batch_size = EXCLUDED.max_batch_size,
                sync_timeout_seconds = EXCLUDED.sync_timeout_seconds,
                domain_priority = EXCLUDED.domain_priority,
                table_priority = EXCLUDED.table_priority,
                field_priority = EXCLUDED.field_priority,
                enum_priority = EXCLUDED.enum_priority,
                min_sync_interval_seconds = EXCLUDED.min_sync_interval_seconds,
                max_retry_attempts = EXCLUDED.max_retry_attempts,
                retry_delay_seconds = EXCLUDED.retry_delay_seconds,
                inherits_global = FALSE,
                global_setting_id = NULL,
                updated_at = $19
        """,
            connection_id,
            config_data.get('auto_sync_enabled', settings.auto_sync_enabled),
            config_data.get('auto_sync_mode', settings.auto_sync_mode),
            config_data.get('auto_sync_domains', settings.auto_sync_domains),
            config_data.get('auto_sync_tables', settings.auto_sync_tables),
            config_data.get('auto_sync_fields', settings.auto_sync_fields),
            config_data.get('auto_sync_enums', settings.auto_sync_enums),
            config_data.get('auto_sync_few_shot', settings.auto_sync_few_shot),
            config_data.get('batch_window_seconds', settings.sync_batch_window_seconds),
            config_data.get('max_batch_size', 100),
            config_data.get('sync_timeout_seconds', 300),
            config_data.get('domain_priority', 1),
            config_data.get('table_priority', 2),
            config_data.get('field_priority', 3),
            config_data.get('enum_priority', 4),
            config_data.get('min_sync_interval_seconds', 60),
            config_data.get('max_retry_attempts', 3),
            config_data.get('retry_delay_seconds', 10),
            now_with_tz()
        )

        return {
            "success": True,
            "message": "同步配置已更新",
            "connection_id": str(connection_id)
        }

    except Exception as e:
        logger.exception("更新同步配置失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新同步配置失败: {str(e)}"
        )


@router.get("/sync-config/{connection_id}")
async def get_sync_config(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db_pool: Pool = Depends(get_sync_db_pool)
) -> dict:
    """
    获取同步配置

    Args:
        connection_id: 连接ID
        db_pool: 数据库连接池

    Returns:
        dict: 配置信息
    """
    try:
        row = await db_pool.fetchrow("""
            SELECT
                config_id, connection_id, auto_sync_enabled,
                auto_sync_mode, auto_sync_domains, auto_sync_tables,
                auto_sync_fields, auto_sync_enums, auto_sync_few_shot,
                batch_window_seconds, max_batch_size, sync_timeout_seconds,
                domain_priority, table_priority, field_priority, enum_priority,
                min_sync_interval_seconds, max_retry_attempts, retry_delay_seconds,
                inherits_global, global_setting_id,
                created_at, updated_at, created_by
            FROM milvus_sync_config
            WHERE connection_id = $1
        """, connection_id)

        if not row:
            # 返回默认配置
            return {
                "success": True,
                "connection_id": str(connection_id),
                "config": {
                    "auto_sync_enabled": settings.auto_sync_enabled,
                    "auto_sync_mode": settings.auto_sync_mode,
                    "auto_sync_domains": settings.auto_sync_domains,
                    "auto_sync_tables": settings.auto_sync_tables,
                    "auto_sync_fields": settings.auto_sync_fields,
                    "auto_sync_enums": settings.auto_sync_enums,
                    "auto_sync_few_shot": settings.auto_sync_few_shot,
                    "batch_window_seconds": settings.sync_batch_window_seconds,
                    "max_batch_size": 100,
                    "sync_timeout_seconds": 300,
                    "domain_priority": 1,
                    "table_priority": 2,
                    "field_priority": 3,
                    "enum_priority": 4,
                    "min_sync_interval_seconds": 60,
                    "max_retry_attempts": 3,
                    "retry_delay_seconds": 10,
                    "inherits_global": False,
                    "global_setting_id": None,
                    "settings_source": "env"
                }
            }

        config = {
            "config_id": str(row['config_id']),
            "auto_sync_enabled": row['auto_sync_enabled'],
                "auto_sync_mode": row['auto_sync_mode'],
                "auto_sync_domains": row['auto_sync_domains'],
                "auto_sync_tables": row['auto_sync_tables'],
                "auto_sync_fields": row['auto_sync_fields'],
                "auto_sync_enums": row['auto_sync_enums'],
                "auto_sync_few_shot": row['auto_sync_few_shot'],
            "batch_window_seconds": row['batch_window_seconds'],
            "max_batch_size": row['max_batch_size'],
            "sync_timeout_seconds": row['sync_timeout_seconds'],
            "domain_priority": row['domain_priority'],
            "table_priority": row['table_priority'],
            "field_priority": row['field_priority'],
            "enum_priority": row['enum_priority'],
            "min_sync_interval_seconds": row['min_sync_interval_seconds'],
            "max_retry_attempts": row['max_retry_attempts'],
            "retry_delay_seconds": row['retry_delay_seconds'],
            "inherits_global": row['inherits_global'],
            "global_setting_id": str(row['global_setting_id']) if row['global_setting_id'] else None,
            "settings_source": "global" if row['inherits_global'] else "custom",
            "created_at": row['created_at'].isoformat() if row['created_at'] else None,
            "updated_at": row['updated_at'].isoformat() if row['updated_at'] else None,
            "created_by": str(row['created_by']) if row['created_by'] else None
        }

        return {
            "success": True,
            "connection_id": str(connection_id),
            "config": config
        }

    except Exception as e:
        logger.exception("获取同步配置失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取同步配置失败: {str(e)}"
        )


# ============================================================================
# 共享工具函数（供HTTP API、WebSocket及监控模块复用）
# ============================================================================


def serialize_sync_health_status(health_status: SyncHealthStatus) -> dict:
    """将 SyncHealthStatus 数据类转换为统一的API结构"""
    return {
        "connection_id": str(health_status.connection_id),
        "auto_sync_enabled": health_status.auto_sync_enabled,
        "milvus_connected": health_status.milvus_connected,
        "embedding_available": health_status.embedding_available,
         "collection_ready": health_status.collection_ready,
        "last_sync_status": health_status.last_sync_status.value if health_status.last_sync_status else None,
        "last_sync_time": health_status.last_sync_time.isoformat() if health_status.last_sync_time else None,
        "pending_changes_count": health_status.pending_changes_count,
        "oldest_pending_change": health_status.oldest_pending_change.isoformat() if health_status.oldest_pending_change else None,
        "is_syncing": health_status.is_syncing,
        "current_sync_id": str(health_status.current_sync_id) if health_status.current_sync_id else None,
        "health_score": health_status.health_score,
        "health_message": health_status.health_message,
        "alert_level": health_status.alert_level
    }


def _parse_entity_types(entity_types: Optional[List[str]]) -> Optional[List[EntityType]]:
    if not entity_types:
        return None

    result: List[EntityType] = []
    for t in entity_types:
        try:
            result.append(EntityType(t))
        except ValueError as e:
            logger.warning("无效的实体类型，已跳过", entity_type=t, error=str(e))
    return result or None


async def build_sync_health_payload(connection_id: UUID, sync_service: UnifiedSyncService) -> dict:
    """获取并序列化同步健康状态"""
    health_status = await sync_service.auto_trigger.get_sync_health_status(connection_id)

    if not health_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"连接 {connection_id} 不存在或未配置同步"
        )

    return serialize_sync_health_status(health_status)


async def build_pending_changes_payload(
    connection_id: UUID,
    sync_service: UnifiedSyncService,
    *,
    limit: int,
    entity_types: Optional[List[str]] = None
) -> dict:
    """构建待同步变更的统一结构"""
    entity_type_enums = _parse_entity_types(entity_types)

    pending_changes = await sync_service.get_pending_changes(
        connection_id, entity_type_enums, limit
    )

    changes_list = []
    stats: Dict[str, int] = {}

    for change in pending_changes:
        entity_type_value = change.entity_type.value
        stats[entity_type_value] = stats.get(entity_type_value, 0) + 1

        changes_list.append({
            "change_id": str(change.change_id),
            "entity_type": entity_type_value,
            "entity_id": str(change.entity_id),
            "operation": change.operation.value,
            "created_at": change.created_at.isoformat(),
            "priority": change.priority,
            "old_data": change.old_data,
            "new_data": change.new_data
        })

    overall_stats = {}
    overall_total = len(pending_changes)
    try:
        if sync_service.auto_trigger:
            full_stats = await sync_service.auto_trigger.get_pending_changes_count(connection_id)
            if full_stats:
                overall_stats = {k: int(v) for k, v in full_stats.items()}
                overall_total = sum(overall_stats.values())
    except Exception as exc:
        logger.warning(
            "获取待同步变更总体统计失败，使用当前批次数据",
            connection_id=str(connection_id),
            error=str(exc)
        )

    return {
        "connection_id": str(connection_id),
        "pending_changes": changes_list,
        "total_count": len(pending_changes),
        "stats": stats,
        "limit": limit,
        "overall_stats": overall_stats or stats,
        "overall_total": overall_total
    }


async def build_auto_sync_status_payload(
    connection_id: UUID,
    sync_service: UnifiedSyncService
) -> dict:
    """组合健康状态与待同步变更的概要，供WebSocket等实时场景使用"""
    health_info = await build_sync_health_payload(connection_id, sync_service)

    pending_summary = await build_pending_changes_payload(
        connection_id,
        sync_service,
        limit=50
    )

    return {
        "connection_id": str(connection_id),
        "health_info": health_info,
        "pending_changes": {
            "total_count": pending_summary["total_count"],
            "stats": pending_summary["stats"],
            "overall_stats": pending_summary.get("overall_stats", pending_summary["stats"]),
            "overall_total": pending_summary.get("overall_total", pending_summary["total_count"]),
            # 仅携带部分数据预览，避免在实时消息中推送过多内容
            "preview": pending_summary["pending_changes"][:5]
        }
    }
