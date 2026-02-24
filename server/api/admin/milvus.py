"""
Milvus同步API
同步业务域、表、字段到Milvus向量数据库
"""

import json
import time

from fastapi import APIRouter, Depends, HTTPException, status, Body
from pydantic import BaseModel, field_validator, model_validator
from server.utils.timezone_helper import now_with_tz
from typing import Any, Dict, List, Optional, Literal, Sequence
from uuid import UUID, uuid4
import asyncpg
import structlog
logger = structlog.get_logger()

from server.models.admin import MilvusSyncRequest, MilvusSyncEnumsRequest, MilvusFewShotSyncRequest
from server.models.sync import SyncConfig
from server.sync.sync_entities import (
    build_domain_entities,
    build_enum_entities,
    build_field_entities,
    build_few_shot_entities,
    build_table_entities,
    build_index_text as _build_index_text,
    build_rich_table_index_text as _build_rich_table_index_text,
    normalize_tags as _normalize_tags,
)
from server.sync.sync_milvus import upsert_to_milvus, incremental_upsert_to_milvus
from server.sync.sync_queries import (
    fetch_domains_for_sync,
    fetch_enums_for_sync,
    fetch_fields_for_sync,
    fetch_few_shots_for_sync,
    fetch_tables_for_sync,
)
from server.sync.auto_sync_policy import describe_policy
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser

router = APIRouter()


def _build_chinese_analyzer_params() -> Dict[str, Any]:
    """
    构建中文 analyzer 参数配置
    
    根据配置决定使用内置的 "chinese" analyzer 还是自定义 analyzer。
    自定义 analyzer 支持：
    - jieba 分词器
    - 停用词过滤
    - cnalphanumonly 过滤（保留中文和字母数字，移除标点）
    
    Returns:
        analyzer_params 字典，可直接用于 FieldSchema 的 analyzer_params 参数
    """
    from server.config import settings
    
    # 如果使用内置的 chinese analyzer（不支持自定义参数）
    if not settings.milvus_use_custom_chinese_analyzer:
        return {"type": "chinese"}
    
    # 使用自定义 analyzer（支持停用词和过滤器配置）
    filters = []
    
    # 添加 cnalphanumonly 过滤器（保留中文和字母数字，移除标点符号）
    # 注意：Milvus 不支持 removepunct，使用 cnalphanumonly 替代
    if settings.milvus_analyzer_remove_punct:
        filters.append("cnalphanumonly")
    
    # 添加停用词过滤器
    if settings.milvus_chinese_stopwords:
        stopwords = [
            word.strip() 
            for word in settings.milvus_chinese_stopwords.split(",") 
            if word.strip()
        ]
        if stopwords:
            filters.append({
                "type": "stop",
                "stop_words": stopwords
            })
    
    # 构建 analyzer_params
    analyzer_params = {
        "tokenizer": "jieba"
    }
    
    if filters:
        analyzer_params["filter"] = filters
    
    return analyzer_params


def _normalize_enum_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(entity)
    normalized.pop("id", None)

    str_fields = [
        "value_id",
        "field_id",
        "table_id",
        "domain_id",
        "field_name",
        "table_name",
        "connection_id",
        "value",
        "display_name",
        "value_index_text",
        "context_index_text",
        "bm25_text",
    ]
    for field in str_fields:
        if field in normalized:
            value = normalized[field]
            normalized[field] = "" if value is None else str(value)

    normalized["frequency"] = int(normalized.get("frequency") or 0)
    if "json_meta" in normalized and normalized["json_meta"] is None:
        normalized["json_meta"] = {}
    normalized["is_active"] = bool(normalized.get("is_active", True))

    return normalized


class MilvusAutoSyncSettings(BaseModel):
    auto_sync_enabled: bool = True
    auto_sync_mode: str = "auto"
    auto_sync_domains: bool = True
    auto_sync_tables: bool = True
    auto_sync_fields: bool = True
    auto_sync_enums: bool = True
    auto_sync_few_shot: bool = True

    @field_validator("auto_sync_mode")
    @classmethod
    def validate_mode(cls, value: str) -> str:
        normalized = (value or "auto").strip().lower()
        if normalized not in {"auto", "manual"}:
            raise ValueError("auto_sync_mode 必须是 auto 或 manual")
        return normalized


class MilvusGlobalSettingsRequest(BaseModel):
    settings: MilvusAutoSyncSettings
    apply_scope: Literal["all", "connections"] = "all"
    connection_ids: Optional[List[UUID]] = None

    @model_validator(mode="after")
    def validate_scope(self):
        if self.apply_scope == "connections":
            ids = self.connection_ids or []
            unique_ids = list(dict.fromkeys(ids))
            if not unique_ids:
                raise ValueError("指定连接范围时必须提供 connection_ids")
            self.connection_ids = unique_ids
        else:
            self.connection_ids = None
        return self


def _build_env_sync_defaults() -> dict:
    from server.config import settings
    return {
        "auto_sync_enabled": getattr(settings, "auto_sync_enabled", True),
        "auto_sync_mode": (settings.auto_sync_mode or "auto").lower(),
        "auto_sync_domains": settings.auto_sync_domains,
        "auto_sync_tables": settings.auto_sync_tables,
        "auto_sync_fields": settings.auto_sync_fields,
        "auto_sync_enums": settings.auto_sync_enums,
        "auto_sync_few_shot": settings.auto_sync_few_shot,
    }


async def get_db_pool():
    """获取数据库连接池"""
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


async def get_milvus_client():
    """获取Milvus客户端（使用连接池）"""
    from server.utils.client_pool import get_milvus_client as get_pooled_client
    return await get_pooled_client()


async def get_embedding_client():
    """获取Embedding客户端（使用连接池）"""
    from server.utils.client_pool import get_embedding_client as get_pooled_client
    return await get_pooled_client()


@router.get("/milvus/global-settings")
async def get_global_sync_settings(
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    返回 .env 默认值与最近一次全局同步配置（基于 milvus_sync_config 记录）
    """
    try:
        defaults = _build_env_sync_defaults()
        total_connections = await db.fetchval("SELECT COUNT(*) FROM database_connections")

        latest_record = await db.fetchrow("""
            SELECT 
                config_id,
                connection_id,
                global_setting_id,
                auto_sync_enabled,
                auto_sync_mode,
                auto_sync_domains,
                auto_sync_tables,
                auto_sync_fields,
                auto_sync_enums,
                auto_sync_few_shot,
                updated_at
            FROM milvus_sync_config
            WHERE inherits_global = TRUE
              AND global_setting_id IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
        """)

        active_payload = None
        if latest_record and latest_record["global_setting_id"]:
            setting_id = latest_record["global_setting_id"]
            applied_rows = await db.fetch("""
                SELECT connection_id
                FROM milvus_sync_config
                WHERE inherits_global = TRUE
                  AND global_setting_id = $1
            """, setting_id)
            applied_ids = [str(row["connection_id"]) for row in applied_rows]
            applied_count = len(applied_ids)
            scope = "all" if total_connections and applied_count == int(total_connections) else "connections"

            active_payload = {
                "setting_id": str(setting_id),
                "auto_sync_enabled": latest_record["auto_sync_enabled"],
                "auto_sync_mode": latest_record["auto_sync_mode"],
                "auto_sync_domains": latest_record["auto_sync_domains"],
                "auto_sync_tables": latest_record["auto_sync_tables"],
                "auto_sync_fields": latest_record["auto_sync_fields"],
                "auto_sync_enums": latest_record["auto_sync_enums"],
                "auto_sync_few_shot": latest_record["auto_sync_few_shot"],
                "apply_scope": scope,
                "applied_connection_ids": applied_ids,
                "applied_connection_count": applied_count,
                "created_at": latest_record["updated_at"].isoformat() if latest_record["updated_at"] else None,
            }

        return {
            "success": True,
            "defaults": defaults,
            "active": active_payload,
            "connection_total": int(total_connections or 0)
        }
    except Exception as exc:
        logger.exception("获取全局同步配置失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取全局同步配置失败: {str(exc)}"
        )


@router.post("/milvus/global-settings")
async def apply_global_sync_settings(
    payload: MilvusGlobalSettingsRequest,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    将给定同步配置批量应用到全部或指定连接
    """
    try:
        settings_payload = payload.settings.model_dump()
        now = now_with_tz()
        global_setting_id = uuid4()

        if payload.apply_scope == "all":
            target_rows = await db.fetch("SELECT connection_id FROM database_connections")
        else:
            target_rows = await db.fetch(
                """
                SELECT connection_id
                FROM database_connections
                WHERE connection_id = ANY($1::uuid[])
                """,
                payload.connection_ids
            )

        target_ids = [row["connection_id"] for row in target_rows]
        requested_ids = set(payload.connection_ids or [])
        found_ids = set(target_ids)
        missing_ids = []
        if payload.apply_scope == "connections":
            missing_ids = [str(cid) for cid in requested_ids - found_ids]

        updated_connections: List[UUID] = []
        if target_ids:
            rows = await db.fetch("""
                WITH target_connections AS (
                    SELECT UNNEST($2::uuid[]) AS connection_id
                )
                INSERT INTO milvus_sync_config (
                    connection_id, auto_sync_enabled, auto_sync_mode,
                    auto_sync_domains, auto_sync_tables, auto_sync_fields,
                    auto_sync_enums, auto_sync_few_shot,
                    inherits_global, global_setting_id
                )
                SELECT
                    tc.connection_id,
                    $3, $4, $5, $6, $7, $8, $9,
                    TRUE, $1
                FROM target_connections tc
                ON CONFLICT (connection_id) DO UPDATE SET
                    auto_sync_enabled = EXCLUDED.auto_sync_enabled,
                    auto_sync_mode = EXCLUDED.auto_sync_mode,
                    auto_sync_domains = EXCLUDED.auto_sync_domains,
                    auto_sync_tables = EXCLUDED.auto_sync_tables,
                    auto_sync_fields = EXCLUDED.auto_sync_fields,
                    auto_sync_enums = EXCLUDED.auto_sync_enums,
                    auto_sync_few_shot = EXCLUDED.auto_sync_few_shot,
                    inherits_global = TRUE,
                    global_setting_id = EXCLUDED.global_setting_id,
                    updated_at = $10
                RETURNING connection_id
            """,
                global_setting_id,
                target_ids,
                settings_payload["auto_sync_enabled"],
                settings_payload["auto_sync_mode"],
                settings_payload["auto_sync_domains"],
                settings_payload["auto_sync_tables"],
                settings_payload["auto_sync_fields"],
                settings_payload["auto_sync_enums"],
                settings_payload["auto_sync_few_shot"],
                now
            )
            updated_connections = [row["connection_id"] for row in rows]

        return {
            "success": True,
            "global_setting_id": str(global_setting_id),
            "updated_connections": len(updated_connections),
            "target_connection_ids": [str(cid) for cid in target_ids],
            "missing_connections": missing_ids,
            "settings": settings_payload,
            "applied_scope": payload.apply_scope
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("批量更新同步配置失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"批量更新同步配置失败: {str(exc)}"
        )


async def ensure_collection_exists(milvus_client, recreate: bool = False) -> str:
    """确保 semantic_metadata 集合存在（Hybrid Schema）。"""
    from server.config import settings

    collection_name = getattr(
        milvus_client, "collection_name", settings.milvus_collection
    )
    from pymilvus import DataType, Function, FunctionType

    collections = milvus_client.list_collections()
    collection_exists = collection_name in collections

    def _drop():
        nonlocal collection_exists
        try:
            milvus_client.drop_collection(collection_name=collection_name)
            collection_exists = False
            logger.info("已删除Milvus集合以重建", collection_name=collection_name)
        except Exception as exc:
            logger.warning(
                "删除Milvus集合失败",
                collection_name=collection_name,
                error=str(exc),
            )

    if recreate and collection_exists:
        _drop()

    required_fields = {
        "id",
        "item_id",
        "connection_id",
        "domain_id",
        "table_id",
        "entity_type",
        "semantic_type",
        "schema_name",
        "table_name",
        "column_name",
        "display_name",
        "description",
        "dense_vector",
        "sparse_vector",
        "bm25_text",
        "json_meta",
        "graph_text",
        "is_active",
    }

    if collection_exists:
        try:
            collection_info = milvus_client.describe_collection(
                collection_name=collection_name
            )
            existing_fields = {field["name"] for field in collection_info["fields"]}
            missing_fields = required_fields - existing_fields
            if missing_fields:
                logger.warning(
                    "Milvus集合缺少字段，重建以匹配最新schema",
                    collection=collection_name,
                    missing=list(missing_fields),
                )
                _drop()
        except Exception as exc:
            logger.warning("检查Milvus集合schema失败，将重新创建", error=str(exc))
            _drop()

    if not collection_exists:
        schema = milvus_client.create_schema(
            auto_id=True,
            enable_dynamic_field=False,
            description="Hybrid semantic metadata (domain/table/field)",
        )
        schema.add_field(
            field_name="id", datatype=DataType.INT64, is_primary=True, auto_id=True
        )
        schema.add_field(field_name="item_id", datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(
            field_name="connection_id", datatype=DataType.VARCHAR, max_length=64
        )
        schema.add_field(
            field_name="domain_id", datatype=DataType.VARCHAR, max_length=64
        )
        schema.add_field(
            field_name="table_id", datatype=DataType.VARCHAR, max_length=64
        )
        schema.add_field(
            field_name="entity_type",
            datatype=DataType.VARCHAR,
            max_length=32,
            is_partition_key=True,
        )
        schema.add_field(
            field_name="semantic_type", datatype=DataType.VARCHAR, max_length=32
        )
        schema.add_field(
            field_name="schema_name", datatype=DataType.VARCHAR, max_length=128
        )
        schema.add_field(
            field_name="table_name", datatype=DataType.VARCHAR, max_length=128
        )
        schema.add_field(
            field_name="column_name", datatype=DataType.VARCHAR, max_length=128
        )
        schema.add_field(
            field_name="display_name", datatype=DataType.VARCHAR, max_length=256
        )
        schema.add_field(
            field_name="description", datatype=DataType.VARCHAR, max_length=2048
        )
        schema.add_field(
            field_name="graph_text", datatype=DataType.VARCHAR, max_length=2048
        )
        schema.add_field(field_name="field_id", datatype=DataType.VARCHAR, max_length=64)
        schema.add_field(
            field_name="dense_vector",
            datatype=DataType.FLOAT_VECTOR,
            dim=settings.embedding_dim,
        )
        schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(
            field_name="bm25_text",
            datatype=DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,  # 启用文本分析器
            analyzer_params=_build_chinese_analyzer_params(),  # 使用优化的中文分析器
        )
        schema.add_field(field_name="json_meta", datatype=DataType.JSON)
        schema.add_field(field_name="is_active", datatype=DataType.BOOL)

        # 添加 BM25 Function：自动将 bm25_text 转换为 sparse_vector
        bm25_function = Function(
            name="bm25_text_to_sparse",
            function_type=FunctionType.BM25,
            input_field_names=["bm25_text"],
            output_field_names=["sparse_vector"],
            params={}
        )
        schema.add_function(bm25_function)

        index_params = milvus_client.prepare_index_params()
        index_params.add_index(
            field_name="dense_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )
        index_params.add_index(
            field_name="sparse_vector",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="BM25",
            params={
                "inverted_index_algo": "DAAT_MAXSCORE",  # 适合高 k 值查询
                "bm25_k1": 1.2,  # 词频饱和度参数
                "bm25_b": 0.75,  # 文档长度归一化参数
                "drop_ratio_build": 0.2,  # 构建时丢弃低频词比例
            },
        )

        milvus_client.create_collection(
            collection_name=collection_name, schema=schema, index_params=index_params
        )

        # 注意：使用分区键模式（entity_type 为分区键）时，分区会自动创建和管理
        # 不需要也不能手动创建分区或指定分区名称

        milvus_client.load_collection(collection_name=collection_name)
        logger.info("已创建Hybrid语义集合并加载", collection_name=collection_name)
    else:
        logger.debug("Milvus集合已存在且schema匹配", collection_name=collection_name)

    return collection_name


@router.get("/milvus/health")
async def check_milvus_health(
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """
    检查Milvus服务健康状态

    Returns:
        {
            "healthy": bool,
            "message": str,
            "details": {
                "milvus_connected": bool,
                "embedding_available": bool,
                "collection_exists": bool,
                "strategy": str
            }
        }
    """
    try:
        from server.config import settings

        milvus_client = await get_milvus_client()
        embedding_client = await get_embedding_client()

        milvus_connected = milvus_client is not None
        embedding_available = embedding_client is not None
        collection_exists = False
        failure_count = 0

        # 检查Milvus集合是否存在
        if milvus_connected:
            try:
                collection_name = settings.milvus_collection
                collections = milvus_client.list_collections()
                collection_exists = collection_name in collections

                if not collection_exists:
                    logger.warning(f"Milvus集合不存在: {collection_name}")
            except Exception as e:
                logger.warning(f"检查Milvus集合失败: {e}")

        # 统计近24小时失败次数（用于前端详情展示）
        try:
            failure_count = await db.fetchval(
                """
                SELECT COUNT(*)
                FROM milvus_sync_history
                WHERE status = 'failed'
                  AND started_at >= NOW() - INTERVAL '24 hours'
                """
            )
        except Exception as e:
            logger.debug("统计同步失败次数出错", error=str(e))
            failure_count = 0

        # 确定健康状态
        if not milvus_connected and not embedding_available:
            message = "Milvus和Embedding均未配置"
            healthy = False
        elif not milvus_connected:
            message = "Milvus未配置或连接失败"
            healthy = False
        elif not embedding_available:
            message = "Embedding未配置"
            healthy = False
        elif not collection_exists:
            message = f"Milvus已连接，但集合'{settings.milvus_collection}'不存在（首次同步会自动创建）"
            healthy = True  # 首次使用时集合不存在是正常的
        else:
            message = "Milvus服务运行正常"
            healthy = True

        return {
            "healthy": healthy,
            "message": message,
            "details": {
                "milvus_connected": milvus_connected,
                "embedding_available": embedding_available,
                "collection_exists": collection_exists,
                "collection_name": settings.milvus_collection if milvus_connected else None,
                "failure_count": int(failure_count or 0),
                "strategy": "两层向量存储（业务域+表）"
            }
        }

    except Exception as e:
        logger.exception("检查Milvus健康状态失败")
        return {
            "healthy": False,
            "message": f"健康检查失败: {str(e)}",
            "details": {
                "milvus_connected": False,
                "embedding_available": False,
                "collection_exists": False,
                "failure_count": 0,
                "strategy": "两层向量存储（业务域+表）"
            }
        }


@router.post("/milvus/sync/{connection_id}")
async def sync_to_milvus(
    connection_id: UUID,
    payload: MilvusSyncRequest = Body(default_factory=MilvusSyncRequest),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool),
    recreate_collections: bool = False
):
    """同步元数据到Milvus（业务域/表/字段 + 枚举/Few-Shot）"""
    try:
        request = payload or MilvusSyncRequest()

        try:
            milvus_client = await get_milvus_client()
            embedding_client = await get_embedding_client()
        except Exception as client_error:
            logger.error("获取Milvus或Embedding客户端失败", error=str(client_error))
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"服务不可用: {str(client_error)}"
            )

        if not milvus_client or not embedding_client:
            return {
                "success": False,
                "message": "Milvus或Embedding客户端未配置，跳过同步"
            }

        if request.incremental:
            logger.debug(
                "收到增量同步请求，将执行当前策略定义的同步流程",
                connection_id=str(connection_id)
            )

        # 检查连接是否存在
        conn = await db.fetchrow("""
            SELECT connection_id, connection_name
            FROM database_connections
            WHERE connection_id = $1
        """, connection_id)

        if not conn:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库连接 {connection_id} 不存在"
            )

        domain_entities: List[dict] = []
        table_entities: List[dict] = []
        field_entities: List[dict] = []
        stats = {"domains": 0, "tables": 0, "fields": 0}

        if request.sync_domains:
            domains = await fetch_domains_for_sync(db, connection_id)
            domain_entities = await build_domain_entities(domains, embedding_client, connection_id)
            stats["domains"] = len(domain_entities)
            logger.debug("业务域同步准备完成", count=stats["domains"])

        if request.sync_tables:
            tables = await fetch_tables_for_sync(db, connection_id)
            table_entities = await build_table_entities(tables, embedding_client, connection_id)
            stats["tables"] = len(table_entities)
            logger.debug("表同步准备完成", count=stats["tables"])

        if request.sync_fields:
            fields = await fetch_fields_for_sync(db, connection_id)
            field_entities = await build_field_entities(fields, embedding_client, connection_id)
            stats["fields"] = len(field_entities)

        semantic_entities = domain_entities + table_entities + field_entities
        total_entities = len(semantic_entities)

        if semantic_entities:
            collection_name = await ensure_collection_exists(
                milvus_client,
                recreate=recreate_collections
            )

            try:
                if domain_entities:
                    upsert_to_milvus(
                        milvus_client,
                        collection_name,
                        domain_entities,
                        connection_id,
                        delete_before_insert=True,
                        delete_filter=f'connection_id == "{str(connection_id)}" and entity_type == "domain"',
                    )

                if table_entities:
                    upsert_to_milvus(
                        milvus_client,
                        collection_name,
                        table_entities,
                        connection_id,
                        delete_before_insert=True,
                        delete_filter=f'connection_id == "{str(connection_id)}" and entity_type == "table"',
                    )

                if field_entities:
                    upsert_to_milvus(
                        milvus_client,
                        collection_name,
                        field_entities,
                        connection_id,
                        delete_before_insert=True,
                        delete_filter=f'connection_id == "{str(connection_id)}" and entity_type == "field"',
                    )

                logger.info(
                    "Milvus语义集合同步完成",
                    connection_id=str(connection_id),
                    totals=stats,
                )
            except Exception as insert_error:
                logger.error("Milvus数据写入失败", error=str(insert_error))
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Milvus数据写入失败: {str(insert_error)}"
                )

        result = {
            "success": True,
            "message": "同步成功",
            "stats": stats,
            "total_entities": total_entities
        }

        await _clear_pending_changes_and_broadcast(connection_id, db)
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Milvus同步失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Milvus同步失败: {str(e)}"
        )


async def sync_to_milvus_task(
    connection_id: UUID,
    recreate_collections: bool = False,
    db_pool: Optional[asyncpg.Pool] = None
):
    """
    Milvus同步后台任务（供其他模块调用）

    Args:
        connection_id: 数据库连接ID
    """
    from server.config import settings

    try:
        db = db_pool
        should_close_pool = False

        if db is None:
            # 连接数据库
            db = await asyncpg.create_pool(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db,
                min_size=settings.milvus_pool_min_size,
                max_size=settings.milvus_pool_max_size,
                command_timeout=settings.metadata_db_command_timeout
            )
            should_close_pool = True

        try:
            milvus_client = await get_milvus_client()
            embedding_client = await get_embedding_client()

            if not milvus_client or not embedding_client:
                logger.warning("Milvus或Embedding客户端未配置，跳过同步",
                             connection_id=str(connection_id))
                return

            # 检查连接是否存在
            conn_info = await db.fetchrow("""
                SELECT connection_id, connection_name
                FROM database_connections
                WHERE connection_id = $1
            """, connection_id)

            if not conn_info:
                logger.error("数据库连接不存在", connection_id=str(connection_id))
                return

            entities = []
            stats = {"domains": 0, "tables": 0, "fields": 0}

            domains = await fetch_domains_for_sync(db, connection_id)
            domain_entities = await build_domain_entities(domains, embedding_client, connection_id)
            stats["domains"] = len(domain_entities)

            tables = await fetch_tables_for_sync(db, connection_id)
            table_entities = await build_table_entities(tables, embedding_client, connection_id)
            stats["tables"] = len(table_entities)

            entities.extend(domain_entities)
            entities.extend(table_entities)

            if entities:
                collection_name = await ensure_collection_exists(
                    milvus_client,
                    recreate=recreate_collections
                )
                upsert_to_milvus(
                    milvus_client,
                    collection_name,
                    entities,
                    connection_id,
                    delete_before_insert=True
                )
                logger.info(
                    "Milvus后台同步完成",
                    connection_id=str(connection_id),
                    total_entities=len(entities)
                )
            else:
                logger.warning("没有数据需要同步到Milvus",
                               connection_id=str(connection_id))

            result = {
                "success": True,
                "stats": stats,
                "total_entities": len(entities)
            }

            await _clear_pending_changes_and_broadcast(connection_id, db)
            return result

        finally:
            if should_close_pool and db:
                await db.close()

    except Exception as e:
        logger.exception("Milvus后台同步失败",
                       connection_id=str(connection_id),
                       error=str(e))
        raise


async def _clear_pending_changes_and_broadcast(connection_id: UUID, db):
    """将pending/syncing状态标记为synced，并通知前端刷新"""

    async def _run(conn):
        try:
            current_time = now_with_tz()
            await conn.execute(
                """
                UPDATE milvus_pending_changes
                SET sync_status = 'synced', synced_at = $1
                WHERE connection_id = $2 AND sync_status IN ('pending', 'syncing')
                """,
                current_time,
                connection_id
            )

            stats_rows = await conn.fetch(
                """
                SELECT entity_type, COUNT(*) AS count
                FROM milvus_pending_changes
                WHERE connection_id = $1 AND sync_status = 'pending'
                GROUP BY entity_type
                """,
                connection_id
            )

            preview_rows = await conn.fetch(
                """
                SELECT change_id, entity_type, entity_id, operation, created_at
                FROM milvus_pending_changes
                WHERE connection_id = $1 AND sync_status = 'pending'
                ORDER BY created_at ASC
                LIMIT 5
                """,
                connection_id
            )

            stats_dict = {row['entity_type']: row['count'] for row in stats_rows}
            total_count = sum(stats_dict.values())

            payload = {
                "total_count": total_count,
                "stats": stats_dict,
                "preview": [
                    {
                        "change_id": str(row['change_id']),
                        "entity_type": row['entity_type'],
                        "entity_id": str(row['entity_id']),
                        "operation": row['operation'],
                        "created_at": row['created_at'].isoformat() if row['created_at'] else None
                    }
                    for row in preview_rows
                ],
                "overall_stats": stats_dict,
                "overall_total": total_count
            }

            from server.websocket_manager import sync_event_broadcaster
            await sync_event_broadcaster.broadcast_pending_changes_update(
                str(connection_id),
                payload
            )

        except Exception as e:
            logger.warning("更新待同步状态失败", connection_id=str(connection_id), error=str(e))

    if hasattr(db, 'acquire'):
        async with db.acquire() as conn:
            await _run(conn)
    else:
        await _run(db)


def ensure_enum_collection_exists(milvus_client, recreate: bool = False):
    """确保枚举值集合存在；recreate=True 时强制重建"""
    from server.config import settings
    from pymilvus import DataType, Function, FunctionType

    collection_name = settings.milvus_enum_collection
    collections = milvus_client.list_collections()

    def _drop():
        nonlocal collections
        try:
            milvus_client.drop_collection(collection_name=collection_name)
            collections = milvus_client.list_collections()
            logger.info("已删除枚举值集合以重建", collection_name=collection_name)
        except Exception as exc:
            logger.warning(
                "删除枚举值集合失败", collection_name=collection_name, error=str(exc)
            )

    if recreate and collection_name in collections:
        _drop()

    required_fields = {
        "id",
        "value_id",
        "field_id",
        "table_id",
        "domain_id",
        "value",
        "display_name",
        "value_index_text",
        "context_index_text",
        "bm25_text",
        "value_vector",
        "context_vector",
        "sparse_vector",
        "connection_id",
        "is_active",
    }

    if collection_name in collections and not recreate:
        try:
            collection_info = milvus_client.describe_collection(
                collection_name=collection_name
            )
            existing_fields = {field["name"] for field in collection_info["fields"]}
            missing_fields = required_fields - existing_fields
            if not missing_fields:
                logger.debug("枚举值集合已存在", collection_name=collection_name)
                return collection_name
            logger.warning(
                "枚举集合缺少字段，将重建以匹配Hybrid Schema",
                collection=collection_name,
                missing=list(missing_fields),
            )
            _drop()
        except Exception as exc:
            logger.warning(
                "检查枚举集合字段失败，重新创建",
                collection=collection_name,
                error=str(exc),
            )
            _drop()

    schema = milvus_client.create_schema(
        auto_id=True,
        enable_dynamic_field=False,
        description="Enum values with dense+sparse vectors",
    )
    schema.add_field(
        field_name="id", datatype=DataType.INT64, is_primary=True, auto_id=True
    )
    schema.add_field(field_name="value_id", datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(field_name="field_id", datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(field_name="table_id", datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(field_name="domain_id", datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(field_name="field_name", datatype=DataType.VARCHAR, max_length=256)
    schema.add_field(field_name="table_name", datatype=DataType.VARCHAR, max_length=256)
    schema.add_field(
        field_name="connection_id",
        datatype=DataType.VARCHAR,
        max_length=64,
        is_partition_key=True,  # 按 connection_id 分区，便于多连接隔离
    )
    schema.add_field(field_name="value", datatype=DataType.VARCHAR, max_length=2048)
    schema.add_field(
        field_name="display_name", datatype=DataType.VARCHAR, max_length=256
    )
    schema.add_field(
        field_name="synonyms", datatype=DataType.VARCHAR, max_length=2048
    )
    schema.add_field(
        field_name="value_index_text", datatype=DataType.VARCHAR, max_length=65535
    )
    schema.add_field(
        field_name="context_index_text", datatype=DataType.VARCHAR, max_length=65535
    )
    schema.add_field(
        field_name="bm25_text",
        datatype=DataType.VARCHAR,
        max_length=65535,
        enable_analyzer=True,  # 启用文本分析器
        analyzer_params=_build_chinese_analyzer_params(),  # 使用优化的中文分析器
    )
    schema.add_field(field_name="json_meta", datatype=DataType.JSON)
    schema.add_field(field_name="frequency", datatype=DataType.INT64)
    schema.add_field(field_name="value_vector", datatype=DataType.FLOAT_VECTOR, dim=settings.embedding_dim)
    schema.add_field(field_name="context_vector", datatype=DataType.FLOAT_VECTOR, dim=settings.embedding_dim)
    schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
    schema.add_field(field_name="is_active", datatype=DataType.BOOL)

    # 添加 BM25 Function：自动将 bm25_text 转换为 sparse_vector
    bm25_function = Function(
        name="bm25_text_to_sparse",
        function_type=FunctionType.BM25,
        input_field_names=["bm25_text"],
        output_field_names=["sparse_vector"],
        params={}
    )
    schema.add_function(bm25_function)

    index_params = milvus_client.prepare_index_params()
    index_params.add_index(
        field_name="value_vector",
        index_type="HNSW",
        metric_type="COSINE",
        params={"M": 16, "efConstruction": 200},
    )
    index_params.add_index(
        field_name="context_vector",
        index_type="HNSW",
        metric_type="COSINE",
        params={"M": 16, "efConstruction": 200},
    )
    index_params.add_index(
        field_name="sparse_vector",
        index_type="SPARSE_INVERTED_INDEX",
        metric_type="BM25",
        params={
            "inverted_index_algo": "DAAT_MAXSCORE",  # 适合高 k 值查询
            "bm25_k1": 1.2,  # 词频饱和度参数
            "bm25_b": 0.75,  # 文档长度归一化参数
            "drop_ratio_build": 0.2,  # 构建时丢弃低频词比例
        },
    )

    milvus_client.create_collection(
        collection_name=collection_name, schema=schema, index_params=index_params
    )
    milvus_client.load_collection(collection_name=collection_name)
    logger.info("已创建Hybrid枚举集合并加载", collection_name=collection_name)
    return collection_name


def ensure_few_shot_collection_exists(milvus_client, recreate: bool = False):
    """确保Few-Shot集合存在；recreate=True 时重建"""
    from server.config import settings
    from pymilvus import DataType, Function, FunctionType

    collection_name = settings.milvus_few_shot_collection
    collections = milvus_client.list_collections()

    def _drop():
        nonlocal collections
        try:
            milvus_client.drop_collection(collection_name=collection_name)
            collections = milvus_client.list_collections()
            logger.info("已删除Few-Shot集合以重建", collection_name=collection_name)
        except Exception as exc:
            logger.warning(
                "删除Few-Shot集合失败", collection_name=collection_name, error=str(exc)
            )

    if recreate and collection_name in collections:
        _drop()

    required_fields = {
        "id",
        "sample_id",
        "connection_id",
        "domain_id",
        "sample_type",
        "question",
        "ir_json",
        "sql_context",
        "error_msg",
        "quality_score",
        "bm25_text",
        "dense_vector",
        "sparse_vector",
        "json_meta",
        "is_active",
    }

    if collection_name in collections and not recreate:
        try:
            collection_info = milvus_client.describe_collection(
                collection_name=collection_name
            )
            existing_fields = {field["name"] for field in collection_info["fields"]}
            if required_fields.issubset(existing_fields):
                logger.debug("Few-Shot集合已存在", collection_name=collection_name)
                return collection_name
            logger.warning(
                "Few-Shot集合缺少字段，重建集合以匹配Hybrid Schema",
                collection=collection_name,
                missing=list(required_fields - existing_fields),
            )
            _drop()
        except Exception as exc:
            logger.warning(
                "检查Few-Shot集合字段失败，重新创建",
                collection=collection_name,
                error=str(exc),
            )
            _drop()

    schema = milvus_client.create_schema(
        auto_id=True,
        enable_dynamic_field=False,
        description="Few-shot question to IR pairs (dense+sparse)",
    )
    schema.add_field(
        field_name="id", datatype=DataType.INT64, is_primary=True, auto_id=True
    )
    schema.add_field(field_name="sample_id", datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(
        field_name="connection_id", datatype=DataType.VARCHAR, max_length=64
    )
    schema.add_field(field_name="domain_id", datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(field_name="sample_type", datatype=DataType.VARCHAR, max_length=32)
    schema.add_field(field_name="question", datatype=DataType.VARCHAR, max_length=2048)
    schema.add_field(field_name="ir_json", datatype=DataType.VARCHAR, max_length=65535)
    schema.add_field(
        field_name="sql_context", datatype=DataType.VARCHAR, max_length=65535
    )
    schema.add_field(field_name="error_msg", datatype=DataType.VARCHAR, max_length=2048)
    schema.add_field(field_name="quality_score", datatype=DataType.FLOAT)
    schema.add_field(
        field_name="bm25_text",
        datatype=DataType.VARCHAR,
        max_length=65535,
        enable_analyzer=True,  # 启用文本分析器
        analyzer_params=_build_chinese_analyzer_params(),  # 使用优化的中文分析器
    )
    schema.add_field(field_name="json_meta", datatype=DataType.JSON)
    schema.add_field(
        field_name="dense_vector",
        datatype=DataType.FLOAT_VECTOR,
        dim=settings.embedding_dim,
    )
    schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
    schema.add_field(field_name="is_active", datatype=DataType.BOOL)

    # 添加 BM25 Function：自动将 bm25_text 转换为 sparse_vector
    bm25_function = Function(
        name="bm25_text_to_sparse",
        function_type=FunctionType.BM25,
        input_field_names=["bm25_text"],
        output_field_names=["sparse_vector"],
        params={}
    )
    schema.add_function(bm25_function)

    index_params = milvus_client.prepare_index_params()
    index_params.add_index(
        field_name="dense_vector",
        index_type="HNSW",
        metric_type="COSINE",
        params={"M": 16, "efConstruction": 200},
    )
    index_params.add_index(
        field_name="sparse_vector",
        index_type="SPARSE_INVERTED_INDEX",
        metric_type="BM25",
        params={
            "inverted_index_algo": "DAAT_MAXSCORE",  # 适合高 k 值查询
            "bm25_k1": 1.2,  # 词频饱和度参数
            "bm25_b": 0.75,  # 文档长度归一化参数
            "drop_ratio_build": 0.2,  # 构建时丢弃低频词比例
        },
    )

    milvus_client.create_collection(
        collection_name=collection_name, schema=schema, index_params=index_params
    )
    milvus_client.load_collection(collection_name=collection_name)
    logger.info("已创建Few-Shot Milvus集合并加载", collection_name=collection_name)
    return collection_name


def normalize_tags(tags) -> List[str]:
    """兼容旧逻辑的包装函数"""
    return _normalize_tags(tags)



def build_rich_table_index_text(
    table_name: str,
    description: Optional[str],
    tags: Optional[List[str]],
    field_names: Optional[List[str]],
    field_count: int,
    data_year: Optional[str] = None
) -> str:
    """兼容旧逻辑的包装函数"""
    return _build_rich_table_index_text(
        table_name,
        description,
        tags,
        field_names,
        field_count,
        data_year,
    )



async def clear_milvus_data(
    connection_id: UUID,
    db = Depends(get_db_pool)
):
    """
    清除Milvus中某个连接的数据
    """
    try:
        milvus_client = await get_milvus_client()

        if not milvus_client:
            return {
                "success": False,
                "message": "Milvus客户端未配置"
            }

        # 删除数据
        collection_name = getattr(milvus_client, 'collection_name', 'semantic_metadata')
        try:
            milvus_client.delete(
                collection_name=collection_name,
                filter=f'connection_id == "{str(connection_id)}"'
            )
        except Exception as e:
            # 如果集合不存在，忽略错误
            logger.debug("删除数据失败（集合可能不存在）", error=str(e))
            if "collection not found" not in str(e).lower():
                raise

        logger.info(f"清除Milvus数据成功: {connection_id}")

        return {
            "success": True,
            "message": "清除成功"
        }

    except Exception as e:
        logger.exception("清除Milvus数据失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清除Milvus数据失败: {str(e)}"
        )


@router.get("/milvus/stats/{connection_id}")
async def get_milvus_stats(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取Milvus同步统计

    从元数据库查询统计信息（不依赖Milvus）
    """
    try:
        # 查询业务域数量（仅统计启用的，包含全局业务域）
        # 统计：绑定到该连接的业务域 + 全局业务域（有该连接下表的）
        domain_count = await db.fetchval(
            """
            SELECT COUNT(DISTINCT bd.domain_id)
            FROM business_domains bd
            LEFT JOIN db_tables t ON t.domain_id = bd.domain_id 
                AND t.connection_id = $1 
                AND t.is_included = TRUE
            WHERE bd.is_active = TRUE
              AND (
                  bd.connection_id = $1
                  OR (bd.connection_id IS NULL AND t.table_id IS NOT NULL)
              )
            """,
            connection_id
        )

        # 查询表数量（仅统计已纳入的）
        table_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM db_tables t
            WHERE t.connection_id = $1 AND t.is_included = TRUE
            """,
            connection_id
        )

        # 查询字段数量（仅统计启用字段，且所属表已纳入）
        field_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE t.connection_id = $1 AND f.is_active = TRUE AND t.is_included = TRUE
            """,
            connection_id
        )

        # 查询枚举值数量（正确的表名：field_enum_values）
        enum_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM field_enum_values ev
            JOIN fields f ON ev.field_id = f.field_id
            WHERE f.connection_id = $1
            """,
            connection_id
        )

        # 分类统计（度量/维度）
        measure_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE t.connection_id = $1 AND f.is_active = TRUE AND t.is_included = TRUE AND f.field_type = 'measure'
            """,
            connection_id
        )

        dimension_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE t.connection_id = $1 AND f.is_active = TRUE AND t.is_included = TRUE AND f.field_type = 'dimension'
            """,
            connection_id
        )

        domain_total = domain_count or 0
        table_total = table_count or 0

        stats = {
            "domain_count": domain_total,
            "table_count": table_total,
            "vector_total": domain_total + table_total,
            "field_count": field_count or 0,
            "measure_count": measure_count or 0,
            "dimension_count": dimension_count or 0,
            "enum_count": enum_count or 0,
            "note": "字段不同步到Milvus，表确定后从PostgreSQL动态加载"
        }

        return {
            "success": True,
            "connection_id": str(connection_id),
            "stats": stats
        }

    except Exception as e:
        logger.exception("获取Milvus统计失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取统计失败: {str(e)}"
        )


@router.get("/milvus/settings/{connection_id}")
async def get_milvus_settings(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取自动同步模式及实体级开关
    """
    try:
        row = await db.fetchrow("""
            SELECT
                config_id, connection_id, auto_sync_enabled,
                auto_sync_mode, auto_sync_domains, auto_sync_tables,
                auto_sync_fields, auto_sync_enums, auto_sync_few_shot,
                inherits_global, global_setting_id
            FROM milvus_sync_config
            WHERE connection_id = $1
        """, connection_id)

        config = None
        if row:
            config = SyncConfig(
                config_id=row['config_id'],
                connection_id=row['connection_id'],
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

        payload = describe_policy(connection_id, config)
        return {
            "success": True,
            "settings": payload
        }

    except Exception as e:
        logger.exception("获取自动同步设置失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取自动同步设置失败: {str(e)}"
        )


@router.put("/milvus/settings/{connection_id}")
async def update_milvus_settings(
    connection_id: UUID,
    payload: MilvusAutoSyncSettings,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    更新自动同步模式及实体级开关
    """
    try:
        now = now_with_tz()
        await db.execute("""
            INSERT INTO milvus_sync_config (
                connection_id, auto_sync_enabled, auto_sync_mode,
                auto_sync_domains, auto_sync_tables, auto_sync_fields,
                auto_sync_enums, auto_sync_few_shot,
                inherits_global, global_setting_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8, FALSE, NULL)
            ON CONFLICT (connection_id) DO UPDATE SET
                auto_sync_enabled = EXCLUDED.auto_sync_enabled,
                auto_sync_mode = EXCLUDED.auto_sync_mode,
                auto_sync_domains = EXCLUDED.auto_sync_domains,
                auto_sync_tables = EXCLUDED.auto_sync_tables,
                auto_sync_fields = EXCLUDED.auto_sync_fields,
                auto_sync_enums = EXCLUDED.auto_sync_enums,
                auto_sync_few_shot = EXCLUDED.auto_sync_few_shot,
                inherits_global = FALSE,
                global_setting_id = NULL,
                updated_at = $9
        """,
            connection_id,
            payload.auto_sync_enabled,
            payload.auto_sync_mode,
            payload.auto_sync_domains,
            payload.auto_sync_tables,
            payload.auto_sync_fields,
            payload.auto_sync_enums,
            payload.auto_sync_few_shot,
            now
        )

        settings_payload = {
            **payload.model_dump(),
            "connection_id": str(connection_id)
        }

        return {
            "success": True,
            "settings": settings_payload
        }

    except Exception as e:
        logger.exception("更新自动同步设置失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新自动同步设置失败: {str(e)}"
        )


# ============================================================================
# 双向量枚举值同步
# ============================================================================

@router.post("/milvus/sync-enums/{connection_id}")
async def sync_enum_values_dual_vector(
    connection_id: UUID,
    payload: MilvusSyncEnumsRequest = Body(default_factory=MilvusSyncEnumsRequest),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    同步枚举值到Milvus（双向量版本）
    
    为每个枚举值生成两个向量：
    1. value_vector: embed(枚举值 + 同义词) - 用于跨字段召回
    2. context_vector: embed(字段名×3 + 字段描述 + 枚举值 + 同义词) - 用于字段内精排
    
    Args:
        connection_id: 数据库连接ID
        payload: 请求体，可指定要同步的字段ID列表（为空则同步所有维度字段）
    
    Returns:
        {
            "success": true,
            "stats": {
                "fields": 10,
                "enums": 500,
                "vectors": 1000
            }
        }
    """
    try:
        from server.config import settings
        import asyncio

        request = payload or MilvusSyncEnumsRequest()
        field_ids = request.field_ids
        force_full = bool(request.force_full_sync)

        try:
            milvus_client = await get_milvus_client()
            embedding_client = await get_embedding_client()
        except Exception as client_error:
            logger.error("获取Milvus或Embedding客户端失败", error=str(client_error))
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"服务不可用: {str(client_error)}"
            )

        if not milvus_client or not embedding_client:
            return {
                "success": False,
                "message": "Milvus或Embedding服务未配置",
                "stats": {"fields": 0, "enums": 0, "vectors": 0}
            }
        
        result = await sync_enum_values_to_milvus(
            connection_id,
            milvus_client,
            embedding_client,
            db,
            field_ids=field_ids,
            only_pending=not force_full,
            recreate_collection=force_full,
        )

        stats = result["stats"]
        return {
            "success": True,
            "message": f"同步完成：{stats['fields']}个字段，{stats['enums']}个枚举值，{stats['vectors']}个向量",
            "stats": stats,
            "total_entities": result["total_entities"],
            "started_at": result["started_at"],
            "completed_at": result["completed_at"],
            "duration_ms": result["duration_ms"],
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("枚举值同步失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"枚举值同步失败: {str(e)}"
        )


@router.get("/milvus/enum-sync-stats/{connection_id}")
async def get_enum_sync_stats(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取枚举值同步状态统计
    
    Returns:
        {
            "fields_total": 20,
            "fields_enabled": 18,
            "enums_total": 500,
            "enums_synced": 450,
            "last_sync_time": "2025-11-04T12:00:00"
        }
    """
    try:
        milvus_client = await get_milvus_client()
        
        # 统计字段
        field_stats = await db.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (
                    WHERE enum_sync_config->>'enabled' = 'true'
                    OR enum_sync_config IS NULL
                ) as enabled
            FROM fields
            WHERE connection_id = $1
              AND field_type = 'dimension'
              AND is_active = true
        """, connection_id)
        
        # 统计枚举值
        enum_stats = await db.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE is_synced_to_milvus = true) as synced,
                MAX(last_synced_at) as last_sync_time
            FROM field_enum_values e
            JOIN fields f ON e.field_id = f.field_id
            WHERE f.connection_id = $1
              AND e.is_active = true
        """, connection_id)
        
        # 统计Milvus中的向量数
        milvus_count = 0
        if milvus_client:
            try:
                results = milvus_client.query(
                    collection_name="enum_values_dual",
                    filter=f'connection_id == "{str(connection_id)}"',
                    output_fields=["id"]
                )
                milvus_count = len(results) if results else 0
            except Exception as e:
                logger.warning(f"查询Milvus失败: {e}")
        
        return {
            "success": True,
            "stats": {
                "fields_total": field_stats['total'],
                "fields_enabled": field_stats['enabled'],
                "enums_total": enum_stats['total'],
                "enums_synced": enum_stats['synced'],
                "vectors_in_milvus": milvus_count,
                "last_sync_time": enum_stats['last_sync_time'].isoformat() if enum_stats['last_sync_time'] else None
            }
        }
  
    except Exception as e:
        logger.exception("获取枚举值统计失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取统计失败: {str(e)}"
        )


@router.post("/milvus/sync-few-shot/{connection_id}")
async def sync_few_shot_samples(
    connection_id: UUID,
    payload: MilvusFewShotSyncRequest = Body(default_factory=MilvusFewShotSyncRequest),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool),
    recreate_collection: bool = False
):
    """同步Few-Shot问答样本到Milvus"""
    try:
        from server.config import settings, RetrievalConfig

        request = payload or MilvusFewShotSyncRequest()
        min_quality_score = (
            request.min_quality_score
            if request.min_quality_score is not None
            else RetrievalConfig.few_shot_min_quality_score()
        )

        try:
            milvus_client = await get_milvus_client()
            embedding_client = await get_embedding_client()
        except Exception as client_error:
            logger.error("获取Milvus或Embedding客户端失败", error=str(client_error))
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"服务不可用: {str(client_error)}"
            )

        if not milvus_client or not embedding_client:
            return {
                "success": False,
                "message": "Milvus或Embedding服务未配置",
                "stats": {"samples": 0}
            }

        exists = await db.fetchval(
            """
            SELECT 1 FROM database_connections WHERE connection_id = $1
            """,
            connection_id
        )
        if not exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库连接 {connection_id} 不存在"
            )

        rows = await fetch_few_shots_for_sync(
            db,
            connection_id,
            min_quality_score=min_quality_score,
            include_inactive=request.include_inactive,
            only_verified=request.only_verified,
            domain_ids=request.domain_ids,
            limit=request.limit,
        )
        if not rows:
            return {
                "success": True,
                "message": "没有符合条件的Few-Shot样本",
                "stats": {"samples": 0}
            }

        entities = await build_few_shot_entities(rows, embedding_client, connection_id)

        if not entities:
            return {
                "success": True,
                "message": "所有样本在Embedding阶段被过滤",
                "stats": {"samples": 0}
            }

        collection_name = ensure_few_shot_collection_exists(
            milvus_client, recreate=recreate_collection
        )

        upsert_to_milvus(
            milvus_client,
            collection_name,
            entities,
            connection_id,
            delete_before_insert=True
        )

        logger.info("Few-Shot样本同步完成",
                    connection_id=str(connection_id),
                    samples=len(entities))

        return {
            "success": True,
            "message": "Few-Shot样本同步完成",
            "stats": {"samples": len(entities)}
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Few-Shot样本同步失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Few-Shot样本同步失败: {str(e)}"
        )


async def sync_enum_values_to_milvus(
    connection_id: UUID,
    milvus_client,
    embedding_client,
    db,
    *,
    field_ids: Optional[Sequence[UUID]] = None,
    only_pending: bool = True,
    recreate_collection: bool = False,
):
    """
    同步枚举值到Milvus（供同步任务调用）
    这是sync_enum_values_dual_vector函数的简化版本，专门用于后台同步任务
    """
    try:
        mode = "incremental" if only_pending else "full"
        logger.info("开始同步枚举值到Milvus", connection_id=str(connection_id), mode=mode)

        started_at = now_with_tz()
        perf_start = time.perf_counter()

        field_filter = ""
        params: List[Any] = [connection_id]
        if field_ids:
            field_filter = "AND field_id = ANY($2::uuid[])"
            params.append(list(field_ids))

        all_fields = await db.fetch(f"""
            SELECT field_id
            FROM fields
            WHERE connection_id = $1
              AND field_type = 'dimension'
              AND is_active = true
              {field_filter}
        """, *params)
        expected_field_ids = {row["field_id"] for row in all_fields}

        enum_rows = await fetch_enums_for_sync(
            db,
            connection_id,
            field_ids=field_ids,
            only_pending=only_pending,
        )

        if not enum_rows:
            completed_at = now_with_tz()
            duration_ms = int((time.perf_counter() - perf_start) * 1000)
            return {
                "stats": {
                    "fields": 0,
                    "enums": 0,
                    "vectors": 0,
                    "skipped_fields": len(expected_field_ids),
                },
                "total_entities": 0,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_ms": duration_ms,
            }

        entities = await build_enum_entities(enum_rows, embedding_client, connection_id)
        entities = [_normalize_enum_entity(item) for item in entities]

        unique_field_ids = {row["field_id"] for row in enum_rows}
        processed_enum_ids = {row["enum_value_id"] for row in enum_rows}
        skipped_fields = len(expected_field_ids - unique_field_ids)
        total_entities = len(entities)

        collection_name = ensure_enum_collection_exists(
            milvus_client,
            recreate=recreate_collection and not only_pending,
        )

        if entities:
            value_ids = [str(row["enum_value_id"]) for row in enum_rows]
            if only_pending and value_ids:
                incremental_upsert_to_milvus(
                    milvus_client,
                    collection_name,
                    entities,
                    value_ids,
                    connection_id,
                    id_field="value_id",
                )
            else:
                upsert_to_milvus(
                    milvus_client,
                    collection_name,
                    entities,
                    connection_id,
                    delete_before_insert=True,
                )

            logger.info(
                "枚举值同步完成",
                connection_id=str(connection_id),
                fields=len(unique_field_ids),
                enums=total_entities,
                mode=mode,
            )

        current_time = now_with_tz()
        if processed_enum_ids:
            await db.execute(
                """
                UPDATE field_enum_values
                SET is_synced_to_milvus = true,
                    last_synced_at = $1
                WHERE enum_value_id = ANY($2::uuid[])
                """,
                current_time,
                list(processed_enum_ids),
            )

        completed_at = now_with_tz()
        duration_ms = int((time.perf_counter() - perf_start) * 1000)

        stats = {
            "fields": len(unique_field_ids),
            "enums": total_entities,
            "vectors": total_entities * 2,
            "skipped_fields": skipped_fields,
        }

        return {
            "stats": stats,
            "total_entities": total_entities,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_ms": duration_ms,
        }
    except Exception as e:
        logger.exception("枚举值同步失败", connection_id=str(connection_id))
        raise


# ============================================================================
# 全量同步所有元数据（不区分连接）
# ============================================================================

class MilvusSyncAllRequest(BaseModel):
    """全量同步请求"""
    sync_domains: bool = True
    sync_tables: bool = True
    sync_fields: bool = True
    sync_enums: bool = True
    sync_few_shot: bool = True
    recreate_collections: bool = False


@router.post("/milvus/sync-all")
async def sync_all_metadata_to_milvus(
    payload: MilvusSyncAllRequest = Body(default_factory=MilvusSyncAllRequest),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool),
):
    """
    同步所有元数据到Milvus（不区分数据库连接）
    
    将所有业务域、表、字段、枚举值和Few-Shot样本同步到Milvus向量数据库
    """
    try:
        request = payload or MilvusSyncAllRequest()
        started_at = now_with_tz()
        perf_start = time.perf_counter()

        try:
            milvus_client = await get_milvus_client()
            embedding_client = await get_embedding_client()
        except Exception as client_error:
            logger.error("获取Milvus或Embedding客户端失败", error=str(client_error))
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"服务不可用: {str(client_error)}"
            )

        if not milvus_client or not embedding_client:
            return {
                "success": False,
                "message": "Milvus或Embedding客户端未配置，跳过同步"
            }

        domain_entities: List[dict] = []
        table_entities: List[dict] = []
        field_entities: List[dict] = []
        stats = {"domains": 0, "tables": 0, "fields": 0, "enums": 0, "few_shot": 0}

        # 同步业务域（不指定connection_id，同步所有）
        if request.sync_domains:
            domains = await fetch_domains_for_sync(db, None)
            domain_entities = await build_domain_entities(domains, embedding_client, None)
            stats["domains"] = len(domain_entities)
            logger.debug("业务域同步准备完成", count=stats["domains"])

        # 同步表（不指定connection_id，同步所有）
        if request.sync_tables:
            tables = await fetch_tables_for_sync(db, None)
            table_entities = await build_table_entities(tables, embedding_client, None)
            stats["tables"] = len(table_entities)
            logger.debug("表同步准备完成", count=stats["tables"])

        # 同步字段（不指定connection_id，同步所有）
        if request.sync_fields:
            fields = await fetch_fields_for_sync(db, None)
            field_entities = await build_field_entities(fields, embedding_client, None)
            stats["fields"] = len(field_entities)
            logger.debug("字段同步准备完成", count=stats["fields"])

        semantic_entities = domain_entities + table_entities + field_entities
        total_entities = len(semantic_entities)

        # 写入语义元数据集合
        if semantic_entities:
            collection_name = await ensure_collection_exists(
                milvus_client,
                recreate=request.recreate_collections
            )

            try:
                if domain_entities:
                    upsert_to_milvus(
                        milvus_client,
                        collection_name,
                        domain_entities,
                        None,  # 不指定connection_id
                        delete_before_insert=True,
                        delete_filter='entity_type == "domain"',
                    )

                if table_entities:
                    upsert_to_milvus(
                        milvus_client,
                        collection_name,
                        table_entities,
                        None,
                        delete_before_insert=True,
                        delete_filter='entity_type == "table"',
                    )

                if field_entities:
                    upsert_to_milvus(
                        milvus_client,
                        collection_name,
                        field_entities,
                        None,
                        delete_before_insert=True,
                        delete_filter='entity_type == "field"',
                    )

                logger.info(
                    "Milvus语义集合同步完成",
                    totals=stats,
                )
            except Exception as insert_error:
                logger.error("Milvus数据写入失败", error=str(insert_error))
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Milvus数据写入失败: {str(insert_error)}"
                )

        # 同步枚举值
        enum_result = {"stats": {"enums": 0, "vectors": 0}}
        if request.sync_enums:
            try:
                enum_result = await sync_all_enum_values_to_milvus(
                    milvus_client,
                    embedding_client,
                    db,
                    recreate_collection=request.recreate_collections,
                )
                stats["enums"] = enum_result["stats"]["enums"]
            except Exception as e:
                logger.warning("枚举值同步失败", error=str(e))

        # 同步Few-Shot样本
        few_shot_count = 0
        if request.sync_few_shot:
            try:
                from server.config import RetrievalConfig
                few_shot_rows = await fetch_few_shots_for_sync(
                    db,
                    None,  # 不指定connection_id
                    min_quality_score=RetrievalConfig.few_shot_min_quality_score(),
                )
                if few_shot_rows:
                    few_shot_entities = await build_few_shot_entities(
                        few_shot_rows, embedding_client, None
                    )
                    if few_shot_entities:
                        fs_collection = ensure_few_shot_collection_exists(
                            milvus_client, recreate=request.recreate_collections
                        )
                        upsert_to_milvus(
                            milvus_client,
                            fs_collection,
                            few_shot_entities,
                            None,
                            delete_before_insert=True,
                        )
                        few_shot_count = len(few_shot_entities)
                stats["few_shot"] = few_shot_count
            except Exception as e:
                logger.warning("Few-Shot样本同步失败", error=str(e))

        completed_at = now_with_tz()
        duration_ms = int((time.perf_counter() - perf_start) * 1000)

        return {
            "success": True,
            "message": "全量同步完成",
            "stats": stats,
            "total_entities": total_entities + stats["enums"] + few_shot_count,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_ms": duration_ms,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("全量Milvus同步失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"全量Milvus同步失败: {str(e)}"
        )


async def sync_all_enum_values_to_milvus(
    milvus_client,
    embedding_client,
    db,
    *,
    recreate_collection: bool = False,
):
    """
    同步所有枚举值到Milvus（不区分连接）
    """
    try:
        logger.info("开始同步所有枚举值到Milvus")
        started_at = now_with_tz()
        perf_start = time.perf_counter()

        # 查询所有枚举值
        enum_rows = await fetch_enums_for_sync(
            db,
            None,  # 不指定connection_id
            only_pending=False,
        )

        if not enum_rows:
            completed_at = now_with_tz()
            duration_ms = int((time.perf_counter() - perf_start) * 1000)
            return {
                "stats": {"fields": 0, "enums": 0, "vectors": 0},
                "total_entities": 0,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_ms": duration_ms,
            }

        entities = await build_enum_entities(enum_rows, embedding_client, None)
        entities = [_normalize_enum_entity(item) for item in entities]

        unique_field_ids = {row["field_id"] for row in enum_rows}
        processed_enum_ids = {row["enum_value_id"] for row in enum_rows}
        total_entities = len(entities)

        collection_name = ensure_enum_collection_exists(
            milvus_client,
            recreate=recreate_collection,
        )

        if entities:
            upsert_to_milvus(
                milvus_client,
                collection_name,
                entities,
                None,  # 不指定connection_id
                delete_before_insert=True,
            )

            logger.info(
                "所有枚举值同步完成",
                fields=len(unique_field_ids),
                enums=total_entities,
            )

        # 更新同步状态
        current_time = now_with_tz()
        if processed_enum_ids:
            await db.execute(
                """
                UPDATE field_enum_values
                SET is_synced_to_milvus = true,
                    last_synced_at = $1
                WHERE enum_value_id = ANY($2::uuid[])
                """,
                current_time,
                list(processed_enum_ids),
            )

        completed_at = now_with_tz()
        duration_ms = int((time.perf_counter() - perf_start) * 1000)

        return {
            "stats": {
                "fields": len(unique_field_ids),
                "enums": total_entities,
                "vectors": total_entities * 2,
            },
            "total_entities": total_entities,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_ms": duration_ms,
        }
    except Exception as e:
        logger.exception("所有枚举值同步失败")
        raise


@router.get("/milvus/stats-all")
async def get_all_milvus_stats(
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取所有元数据的Milvus同步统计（不区分连接）
    """
    try:
        # 查询业务域数量（所有启用的）
        domain_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM business_domains
            WHERE is_active = TRUE
            """
        )

        # 查询表数量（所有已纳入的）
        table_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM db_tables t
            WHERE t.is_included = TRUE
            """
        )

        # 查询字段数量（所有启用字段，且所属表已纳入）
        field_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE f.is_active = TRUE AND t.is_included = TRUE
            """
        )

        # 查询枚举值数量
        enum_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM field_enum_values ev
            JOIN fields f ON ev.field_id = f.field_id
            WHERE ev.is_active = TRUE
            """
        )

        # 分类统计（度量/维度）
        measure_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE f.is_active = TRUE AND t.is_included = TRUE AND f.field_type = 'measure'
            """
        )

        dimension_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE f.is_active = TRUE AND t.is_included = TRUE AND f.field_type = 'dimension'
            """
        )

        # Few-Shot样本数量
        few_shot_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM qa_few_shot_samples
            WHERE is_active = TRUE
            """
        )

        domain_total = domain_count or 0
        table_total = table_count or 0

        stats = {
            "domain_count": domain_total,
            "table_count": table_total,
            "vector_total": domain_total + table_total,
            "field_count": field_count or 0,
            "measure_count": measure_count or 0,
            "dimension_count": dimension_count or 0,
            "enum_count": enum_count or 0,
            "few_shot_count": few_shot_count or 0,
            "note": "统计所有元数据（不区分数据库连接）"
        }

        return {
            "success": True,
            "stats": stats
        }

    except Exception as e:
        logger.exception("获取全量Milvus统计失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取统计失败: {str(e)}"
        )
