"""
自动同步策略辅助方法
"""

from __future__ import annotations

from typing import Iterable, List, Optional
from uuid import UUID

import structlog

from server.config import settings
from server.models.sync import EntityType, SyncConfig

logger = structlog.get_logger()

_ENTITY_FLAG_ATTR = {
    EntityType.DOMAIN: "auto_sync_domains",
    EntityType.TABLE: "auto_sync_tables",
    EntityType.FIELD: "auto_sync_fields",
    EntityType.ENUM: "auto_sync_enums",
    EntityType.FEW_SHOT: "auto_sync_few_shot",
}


def _normalize_mode(mode: Optional[str]) -> str:
    candidate = (mode or settings.auto_sync_mode or "auto").strip().lower()
    if candidate not in {"auto", "manual"}:
        return "auto"
    return candidate


def _flag_from_settings(entity_type: EntityType) -> bool:
    attr = _ENTITY_FLAG_ATTR.get(entity_type)
    if not attr:
        return True
    return getattr(settings, attr, True)


def _flag_from_config(entity_type: EntityType, config: Optional[SyncConfig]) -> Optional[bool]:
    if not config:
        return None
    attr = _ENTITY_FLAG_ATTR.get(entity_type)
    if not attr:
        return None
    return getattr(config, attr, None)


def is_auto_mode(config: Optional[SyncConfig]) -> bool:
    if config and config.auto_sync_mode:
        return _normalize_mode(config.auto_sync_mode) == "auto"
    return _normalize_mode(settings.auto_sync_mode) == "auto"


def is_entity_enabled(entity_type: EntityType, config: Optional[SyncConfig]) -> bool:
    override = _flag_from_config(entity_type, config)
    if override is not None:
        return override
    return _flag_from_settings(entity_type)


def should_auto_process(entity_type: EntityType, config: Optional[SyncConfig]) -> bool:
    if not is_auto_mode(config):
        return False
    return is_entity_enabled(entity_type, config)


def filter_auto_enabled_changes(
    changes: Iterable,
    config: Optional[SyncConfig],
) -> List:
    """
    根据策略过滤允许自动同步的变更。

    Args:
        changes: PendingChange 列表
        config: 数据库级同步配置
    """
    allowed = []
    for change in changes:
        entity_type = getattr(change, "entity_type", None)
        if not isinstance(entity_type, EntityType):
            logger.warning("未知实体类型，自动同步策略跳过", change=getattr(change, "change_id", None))
            continue
        if should_auto_process(entity_type, config):
            allowed.append(change)
    return allowed


def manual_sync_allowed(entity_type: EntityType, config: Optional[SyncConfig] = None) -> bool:
    return is_entity_enabled(entity_type, config)


def describe_policy(connection_id: Optional[UUID], config: Optional[SyncConfig]) -> dict:
    """
    将策略信息转换为可返回前端的结构。
    """
    inherits_global = bool(getattr(config, "inherits_global", False))
    return {
        "connection_id": str(connection_id) if connection_id else None,
        "auto_sync_enabled": config.auto_sync_enabled if config else settings.auto_sync_enabled,
        "auto_sync_mode": _normalize_mode(
            config.auto_sync_mode if config else settings.auto_sync_mode
        ),
        "auto_sync_domains": is_entity_enabled(EntityType.DOMAIN, config),
        "auto_sync_tables": is_entity_enabled(EntityType.TABLE, config),
        "auto_sync_fields": is_entity_enabled(EntityType.FIELD, config),
        "auto_sync_enums": is_entity_enabled(EntityType.ENUM, config),
        "auto_sync_few_shot": is_entity_enabled(EntityType.FEW_SHOT, config),
        "is_inherited_from_global": inherits_global,
        "global_setting_id": str(getattr(config, "global_setting_id")) if getattr(config, "global_setting_id", None) else None,
        "settings_source": "global" if inherits_global else "custom",
    }


