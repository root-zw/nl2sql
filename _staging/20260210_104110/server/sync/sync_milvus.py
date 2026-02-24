"""
Milvus写入操作封装
"""

from __future__ import annotations

from typing import Iterable, Sequence, Optional
from uuid import UUID

import structlog

logger = structlog.get_logger()


def upsert_to_milvus(
    milvus_client,
    collection_name: str,
    entities: Sequence[dict],
    connection_id: UUID,
    *,
    delete_before_insert: bool = True,
    delete_filter: Optional[str] = None,
    partition_name: Optional[str] = None,
) -> int:
    """删除旧数据后批量插入新实体
    
    注意：当集合使用分区键模式（partition key mode）时，partition_name 参数会被忽略。
    Milvus 会根据数据中的分区键字段值自动分配分区。
    """
    if not entities:
        return 0

    if delete_before_insert:
        filter_expr = delete_filter or f'connection_id == "{str(connection_id)}"'
        _safe_delete(
            milvus_client,
            collection_name,
            filter_expr,
        )

    # 使用分区键模式时，不传递 partition_name 参数
    # Milvus 会根据数据中的 entity_type 字段值自动分配到正确的分区
    insert_params = {
        "collection_name": collection_name,
        "data": list(entities),
    }
    # 只有在非分区键模式下才传递 partition_name
    # 由于当前集合使用分区键模式，这里不传递 partition_name
    # 如果未来需要支持非分区键模式的集合，可以添加判断逻辑
    
    milvus_client.insert(**insert_params)
    logger.debug(
        "Milvus批量写入完成",
        collection=collection_name,
        connection_id=str(connection_id),
        count=len(entities),
    )
    return len(entities)


def incremental_upsert_to_milvus(
    milvus_client,
    collection_name: str,
    entities: Sequence[dict],
    entity_ids: Iterable[UUID],
    connection_id: UUID,
    *,
    id_field: str = "item_id",
    partition_name: Optional[str] = None,
) -> int:
    """增量更新指定实体
    
    注意：当集合使用分区键模式（partition key mode）时，partition_name 参数会被忽略。
    Milvus 会根据数据中的分区键字段值自动分配分区。
    """
    ids = [str(eid) for eid in entity_ids]
    if ids:
        quoted = ",".join(f'"{eid}"' for eid in ids)
        filter_expr = f'connection_id == "{str(connection_id)}" and {id_field} in [{quoted}]'
        _safe_delete(milvus_client, collection_name, filter_expr)

    if not entities:
        return 0

    # 使用分区键模式时，不传递 partition_name 参数
    # Milvus 会根据数据中的 entity_type 字段值自动分配到正确的分区
    insert_params = {
        "collection_name": collection_name,
        "data": list(entities),
    }
    # 只有在非分区键模式下才传递 partition_name
    # 由于当前集合使用分区键模式，这里不传递 partition_name
    
    milvus_client.insert(**insert_params)
    logger.debug(
        "Milvus增量写入完成",
        collection=collection_name,
        connection_id=str(connection_id),
        count=len(entities),
    )
    return len(entities)


def delete_from_milvus(
    milvus_client,
    collection_name: str,
    connection_id: UUID,
    entity_ids: Iterable[UUID],
    *,
    id_field: str = "item_id",
) -> int:
    """删除指定实体"""
    ids = [str(eid) for eid in entity_ids]
    if not ids:
        return 0

    quoted = ",".join(f'"{eid}"' for eid in ids)
    filter_expr = f'connection_id == "{str(connection_id)}" and {id_field} in [{quoted}]'
    deleted = _safe_delete(milvus_client, collection_name, filter_expr)
    logger.debug(
        "Milvus删除完成",
        collection=collection_name,
        connection_id=str(connection_id),
        count=deleted,
    )
    return deleted


def _safe_delete(milvus_client, collection_name: str, filter_expr: str) -> int:
    """执行Milvus删除操作，忽略异常"""
    try:
        result = milvus_client.delete(collection_name=collection_name, filter=filter_expr)
        deleted_count = getattr(result, "delete_count", 0)
        return deleted_count or 0
    except Exception as exc:
        logger.debug(
            "删除Milvus数据失败（忽略）",
            collection=collection_name,
            filter=filter_expr,
            error=str(exc),
        )
        return 0



