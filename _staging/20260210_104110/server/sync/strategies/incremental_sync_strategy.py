"""
增量同步策略
"""

from __future__ import annotations

import structlog

from server.models.sync import EntityType
from server.sync.sync_strategy import SyncContext, SyncResult, SyncStrategy

logger = structlog.get_logger()


class IncrementalSyncStrategy(SyncStrategy):
    async def execute(self, context: SyncContext) -> SyncResult:
        pending_changes = context.pending_changes or []
        connection_id = context.connection_id

        async def report(step: str, percentage: int) -> None:
            if context.progress_hook:
                await context.progress_hook(step, percentage)

        if not pending_changes:
            await report("无变更需要同步", 100)
            return SyncResult(
                success=True,
                stats={},
                total_entities=0,
                synced_entities=0,
                message="无待处理变更",
                synced_change_ids=[],
            )

        service = context.service
        if not service:
            raise RuntimeError("缺少同步服务实例")

        domain_changes = [c for c in pending_changes if c.entity_type == EntityType.DOMAIN]
        table_changes = [c for c in pending_changes if c.entity_type == EntityType.TABLE]
        field_changes = [c for c in pending_changes if c.entity_type == EntityType.FIELD]
        enum_changes = [c for c in pending_changes if c.entity_type == EntityType.ENUM]
        few_shot_changes = [c for c in pending_changes if c.entity_type == EntityType.FEW_SHOT]

        stats = {
            "domains": len(domain_changes),
            "tables": len(table_changes),
            "fields": len(field_changes),
            "enums": len(enum_changes),
            "few_shot": len(few_shot_changes),
        }

        success = True
        current_step = 0
        total_steps = sum(1 for group in [domain_changes, table_changes, field_changes, enum_changes, few_shot_changes] if group)

        async def step_progress(title: str) -> None:
            nonlocal current_step, total_steps
            if total_steps == 0:
                await report(title, 50)
                return
            current_step += 1
            percentage = int(current_step * 100 / total_steps)
            await report(title, max(percentage, 10))

        if domain_changes:
            await step_progress(f"同步业务域变更({len(domain_changes)}个)")
            success &= await service._sync_domain_changes(connection_id, domain_changes)

        if table_changes and success:
            await step_progress(f"同步表变更({len(table_changes)}个)")
            success &= await service._sync_table_changes(connection_id, table_changes)

        if field_changes and success:
            await step_progress(f"同步字段变更({len(field_changes)}个)")
            success &= await service._sync_field_changes(connection_id, field_changes)

        if enum_changes and success:
            await step_progress(f"同步枚举值变更({len(enum_changes)}个)")
            success &= await service._sync_enum_changes(connection_id, enum_changes)

        if few_shot_changes and success:
            await step_progress(f"同步Few-Shot变更({len(few_shot_changes)}个)")
            success &= await service._sync_few_shot_changes(connection_id, few_shot_changes)

        if success:
            await report("增量同步完成", 100)
            synced_ids = [change.change_id for change in pending_changes]
            return SyncResult(
                success=True,
                stats=stats,
                total_entities=len(pending_changes),
                synced_entities=len(pending_changes),
                message="增量同步完成",
                synced_change_ids=synced_ids,
            )

        logger.error("增量同步失败，已中止后续步骤", connection_id=str(connection_id))
        return SyncResult(
            success=False,
            stats=stats,
            total_entities=len(pending_changes),
            synced_entities=0,
            message="增量同步失败",
        )



