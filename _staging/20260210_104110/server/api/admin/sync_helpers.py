"""
Admin 同步相关公共方法
"""

from __future__ import annotations

from typing import Iterable, List, Optional
from uuid import UUID

import structlog
from fastapi import HTTPException, Response

from server.models.sync import EntityType, TriggeredBy, SyncConfig
from server.sync.auto_sync_policy import manual_sync_allowed

logger = structlog.get_logger()


async def trigger_entity_sync_now(
    response: Response,
    connection_id: UUID,
    entity_types: Iterable[EntityType],
    source: str,
    sync_now: bool,
    db=None,
) -> None:
    """
    在元数据保存后按需触发增量同步。

    Args:
        response: FastAPI Response 对象（用于写入头信息）
        connection_id: 数据库连接ID
        entity_types: 需要同步的实体类型列表
        source: 日志来源
        sync_now: 是否触发同步
        db: 可选数据库连接，用于读取连接级策略
    """
    if not sync_now:
        return

    config = await _load_connection_sync_config(db, connection_id) if db else None

    if config and not config.auto_sync_enabled:
        response.headers["X-Sync-Triggered"] = "false"
        response.headers["X-Sync-Reason"] = "disabled"
        return

    allowed_types: List[EntityType] = []
    for entity_type in entity_types:
        if manual_sync_allowed(entity_type, config):
            allowed_types.append(entity_type)

    if not allowed_types:
        response.headers["X-Sync-Triggered"] = "false"
        response.headers["X-Sync-Reason"] = "disabled"
        return

    try:
        from server.api.admin.auto_sync import get_sync_service

        sync_service = get_sync_service()
    except HTTPException as exc:
        logger.warning(
            "同步服务不可用，无法触发即时同步",
            connection_id=str(connection_id),
            source=source,
            error=exc.detail
        )
        response.headers["X-Sync-Triggered"] = "false"
        response.headers["X-Sync-Reason"] = "service_unavailable"
        return

    try:
        sync_id = await sync_service.trigger_incremental_sync(
            connection_id,
            allowed_types,
            triggered_by=TriggeredBy.MANUAL,
            priority=1
        )
    except Exception as exc:  # pragma: no cover
        logger.warning(
            "即时同步失败",
            connection_id=str(connection_id),
            source=source,
            error=str(exc)
        )
        response.headers["X-Sync-Triggered"] = "false"
        response.headers["X-Sync-Reason"] = "sync_failed"
        response.headers["X-Sync-Error"] = str(exc)
        return

    if sync_id:
        response.headers["X-Sync-Triggered"] = "true"
        response.headers["X-Sync-Task"] = str(sync_id)
    else:
        response.headers["X-Sync-Triggered"] = "false"
        response.headers["X-Sync-Reason"] = "no_pending_changes"


async def _load_connection_sync_config(db, connection_id: UUID) -> Optional[SyncConfig]:
    if not db:
        return None
    try:
        row = await db.fetchrow("""
            SELECT
                config_id, auto_sync_enabled, auto_sync_mode,
                auto_sync_domains, auto_sync_tables, auto_sync_fields,
                auto_sync_enums, auto_sync_few_shot,
                inherits_global, global_setting_id
            FROM milvus_sync_config
            WHERE connection_id = $1
        """, connection_id)
        if not row:
            return None
        return SyncConfig(
            config_id=row['config_id'],
            connection_id=connection_id,
            auto_sync_enabled=row['auto_sync_enabled'],
            auto_sync_mode=row['auto_sync_mode'],
            auto_sync_domains=row['auto_sync_domains'],
            auto_sync_tables=row['auto_sync_tables'],
            auto_sync_fields=row['auto_sync_fields'],
            auto_sync_enums=row['auto_sync_enums'],
            auto_sync_few_shot=row['auto_sync_few_shot'],
                inherits_global=row.get('inherits_global', False),
                global_setting_id=row.get('global_setting_id'),
        )
    except Exception as exc:  # pragma: no cover
        logger.debug(
            "加载连接同步配置失败，使用全局配置",
            connection_id=str(connection_id),
            error=str(exc)
        )
        return None


