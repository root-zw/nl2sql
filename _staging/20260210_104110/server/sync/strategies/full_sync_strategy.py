"""
全量同步策略
"""

from __future__ import annotations

from typing import Dict

import structlog

from server.config import settings, RetrievalConfig
from server.api.admin.milvus import (
    ensure_collection_exists,
    ensure_enum_collection_exists,
    ensure_few_shot_collection_exists,
)
from server.sync.sync_entities import (
    build_domain_entities,
    build_enum_entities,
    build_field_entities,
    build_few_shot_entities,
    build_table_entities,
)
from server.sync.sync_milvus import upsert_to_milvus
from server.sync.sync_queries import (
    fetch_domains_for_sync,
    fetch_enums_for_sync,
    fetch_few_shots_for_sync,
    fetch_fields_for_sync,
    fetch_tables_for_sync,
)
from server.sync.sync_strategy import SyncContext, SyncResult, SyncStrategy

logger = structlog.get_logger()


class FullSyncStrategy(SyncStrategy):
    async def execute(self, context: SyncContext) -> SyncResult:
        connection_id = context.connection_id
        db = context.db
        milvus_client = context.milvus_client
        embedding_client = context.embedding_client
        recreate = context.recreate_collections
        manual_request = context.manual_request

        if not milvus_client or not embedding_client:
            raise RuntimeError("Milvus或Embedding客户端未配置")

        stats: Dict[str, int] = {"domains": 0, "tables": 0, "fields": 0, "enums": 0, "few_shot": 0}
        total_expected = 0
        total_synced = 0

        async def report(step: str, percentage: int) -> None:
            if context.progress_hook:
                await context.progress_hook(step, percentage)

        await report("同步业务域和表", 30)

        sync_domains = manual_request.sync_domains if manual_request else True
        sync_tables = manual_request.sync_tables if manual_request else True
        sync_fields = (
            manual_request.sync_fields if manual_request else settings.auto_sync_fields
        )
        sync_enums = manual_request.sync_enums if manual_request else True
        sync_few_shot = (
            manual_request.sync_few_shot
            if manual_request
            else RetrievalConfig.few_shot_sync_in_full_sync()
        )

        domain_entities = []
        if sync_domains:
            domains = await fetch_domains_for_sync(db, connection_id)
            domain_entities = await build_domain_entities(domains, embedding_client, connection_id)
            stats["domains"] = len(domain_entities)

        table_entities = []
        if sync_tables:
            tables = await fetch_tables_for_sync(db, connection_id)
            table_entities = await build_table_entities(tables, embedding_client, connection_id)
            stats["tables"] = len(table_entities)

        field_entities = []
        if sync_fields:
            fields = await fetch_fields_for_sync(db, connection_id)
            field_entities = await build_field_entities(fields, embedding_client, connection_id)
            stats["fields"] = len(field_entities)

        semantic_total = 0
        if domain_entities or table_entities or field_entities:
            collection_name = await ensure_collection_exists(milvus_client, recreate=recreate)

            if domain_entities:
                upsert_to_milvus(
                    milvus_client,
                    collection_name,
                    domain_entities,
                    connection_id,
                    delete_before_insert=True,
                    delete_filter=f'connection_id == "{str(connection_id)}" and entity_type == "domain"',
                )
                semantic_total += len(domain_entities)

            if table_entities:
                upsert_to_milvus(
                    milvus_client,
                    collection_name,
                    table_entities,
                    connection_id,
                    delete_before_insert=True,
                    delete_filter=f'connection_id == "{str(connection_id)}" and entity_type == "table"',
                )
                semantic_total += len(table_entities)

            if field_entities:
                upsert_to_milvus(
                    milvus_client,
                    collection_name,
                    field_entities,
                    connection_id,
                    delete_before_insert=True,
                    delete_filter=f'connection_id == "{str(connection_id)}" and entity_type == "field"',
                )
                semantic_total += len(field_entities)

            total_expected += semantic_total
            total_synced += semantic_total
        else:
            logger.warning("没有业务域/表/字段需要同步", connection_id=str(connection_id))

        enum_total = 0
        if sync_enums:
            await report("同步枚举值", 70)
            try:
                enum_rows = await fetch_enums_for_sync(db, connection_id, only_pending=False)
                enum_entities = await build_enum_entities(enum_rows, embedding_client, connection_id)
                if enum_entities:
                    collection = ensure_enum_collection_exists(milvus_client, recreate=recreate)
                    upsert_to_milvus(
                        milvus_client,
                        collection,
                        enum_entities,
                        connection_id,
                        delete_before_insert=True,
                    )
                    enum_total = len(enum_entities)
                    stats["enums"] = enum_total
                    total_expected += enum_total
                    total_synced += enum_total

                    unique_field_ids = {row["field_id"] for row in enum_rows}
                    if unique_field_ids:
                        await db.execute(
                            """
                            UPDATE field_enum_values
                            SET is_synced_to_milvus = TRUE,
                                last_synced_at = NOW()
                            WHERE field_id = ANY($1::uuid[])
                              AND is_active = TRUE
                            """,
                            list(unique_field_ids),
                        )
                else:
                    logger.debug("没有可同步的枚举值", connection_id=str(connection_id))
            except Exception as exc:
                logger.warning("枚举值同步失败（已忽略）", connection_id=str(connection_id), error=str(exc))

        few_shot_total = 0
        if sync_few_shot:
            await report("同步Few-Shot样本", 85)
            try:
                rows = await fetch_few_shots_for_sync(
                    db,
                    connection_id,
                    min_quality_score=RetrievalConfig.few_shot_min_quality_score(),
                    include_inactive=False,
                    only_verified=False,
                )
                few_shot_entities = await build_few_shot_entities(rows, embedding_client, connection_id)
                if few_shot_entities:
                    collection = ensure_few_shot_collection_exists(milvus_client, recreate=recreate)
                    upsert_to_milvus(
                        milvus_client,
                        collection,
                        few_shot_entities,
                        connection_id,
                        delete_before_insert=True,
                    )
                    few_shot_total = len(few_shot_entities)
                    stats["few_shot"] = few_shot_total
                    total_expected += few_shot_total
                    total_synced += few_shot_total
            except Exception as exc:
                logger.warning("Few-Shot同步失败（已忽略）", connection_id=str(connection_id), error=str(exc))

        await report("同步完成", 100)

        return SyncResult(
            success=True,
            stats=stats,
            total_entities=total_expected,
            synced_entities=total_synced,
            message="全量同步完成",
        )



