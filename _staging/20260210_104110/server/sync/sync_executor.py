"""
同步策略执行器
"""

from __future__ import annotations

from typing import Dict, Type

from server.models.sync import SyncType
from server.sync.sync_strategy import SyncContext, SyncResult, SyncStrategy


class SyncExecutor:
    """根据同步类型选择策略并执行"""

    def __init__(self) -> None:
        self._strategies: Dict[SyncType, Type[SyncStrategy]] = {}

    def register(self, sync_type: SyncType, strategy_cls: Type[SyncStrategy]) -> None:
        self._strategies[sync_type] = strategy_cls

    async def execute(self, context: SyncContext) -> SyncResult:
        strategy_cls = self._strategies.get(context.sync_type)
        if not strategy_cls:
            raise ValueError(f"未注册的同步策略: {context.sync_type}")

        strategy = strategy_cls()
        return await strategy.execute(context)
















