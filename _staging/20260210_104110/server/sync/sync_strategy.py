"""
同步策略基础定义
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional
from uuid import UUID

import asyncpg

from server.models.sync import ManualSyncRequest, PendingChange, SyncHistory, SyncType

ProgressHook = Callable[[str, int], Awaitable[None]]


@dataclass
class SyncContext:
    """同步上下文"""

    connection_id: UUID
    db: asyncpg.pool.Pool
    milvus_client: Any
    embedding_client: Any
    sync_type: SyncType
    manual_request: Optional[ManualSyncRequest] = None
    sync_record: Optional[SyncHistory] = None
    recreate_collections: bool = False
    pending_changes: List[PendingChange] = field(default_factory=list)
    progress_hook: Optional[ProgressHook] = None
    service: Optional[Any] = None  # 避免循环依赖


@dataclass
class SyncResult:
    """同步结果"""

    success: bool
    stats: Dict[str, Any] = field(default_factory=dict)
    total_entities: int = 0
    synced_entities: int = 0
    message: Optional[str] = None
    synced_change_ids: Optional[List[UUID]] = None


class SyncStrategy:
    """同步策略接口"""

    async def execute(self, context: SyncContext) -> SyncResult:
        raise NotImplementedError




















