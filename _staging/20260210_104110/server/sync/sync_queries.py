"""
同步流程的数据查询层
统一封装各类实体的查询逻辑，避免在业务代码中重复编写SQL
"""

from typing import Any, Dict, Iterable, List, Optional, Sequence
from uuid import UUID

import asyncpg

from server.models.sync import EntityType

DbExecutor = asyncpg.Connection | asyncpg.pool.Pool


async def fetch_domains_for_sync(db: DbExecutor, connection_id: Optional[UUID] = None) -> List[asyncpg.Record]:
    """查询需要同步的业务域
    
    Args:
        db: 数据库执行器
        connection_id: 可选的连接ID，为None时查询所有连接的业务域
        
    Returns:
        业务域记录列表，包含动态统计的table_count和关联表名列表table_names
        
    Note:
        当指定connection_id时，会同步以下业务域：
        1. 绑定到该连接的业务域（bd.connection_id = $1）
        2. 全局业务域（bd.connection_id IS NULL）且有该连接下表的业务域
    """
    if connection_id:
        return await db.fetch(
            """
            WITH domain_stats AS (
                SELECT
                    t.domain_id,
                    COUNT(*) AS table_count,
                    ARRAY_AGG(t.display_name ORDER BY t.display_name) FILTER (WHERE t.display_name IS NOT NULL) AS table_names
                FROM db_tables t
                WHERE t.connection_id = $1
                  AND t.is_included = TRUE
                  AND t.domain_id IS NOT NULL
                GROUP BY t.domain_id
            ),
            domain_fields AS (
                -- 获取业务域下的代表性字段名（用于BM25增强）
                SELECT 
                    t.domain_id,
                    ARRAY_AGG(DISTINCT f.display_name ORDER BY f.display_name) FILTER (WHERE f.display_name IS NOT NULL) AS field_names
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE t.connection_id = $1
                  AND t.is_included = TRUE
                  AND f.is_active = TRUE
                  AND t.domain_id IS NOT NULL
                GROUP BY t.domain_id
            )
            SELECT
                bd.domain_id,
                bd.connection_id,
                bd.domain_name,
                bd.domain_code,
                bd.description,
                bd.keywords,
                -- 优先使用动态统计值，确保table_count准确
                COALESCE(ds.table_count, 0) AS table_count,
                COALESCE(ds.table_names, ARRAY[]::varchar[]) AS table_names,
                COALESCE(df.field_names, ARRAY[]::varchar[]) AS representative_fields,
                bd.is_active
            FROM business_domains bd
            LEFT JOIN domain_stats ds ON ds.domain_id = bd.domain_id
            LEFT JOIN domain_fields df ON df.domain_id = bd.domain_id
            WHERE bd.is_active = TRUE
              AND (
                  -- 绑定到该连接的业务域
                  bd.connection_id = $1
                  -- 或者：全局业务域（connection_id为空）且有该连接下的表
                  OR (bd.connection_id IS NULL AND ds.table_count > 0)
              )
            """,
            connection_id,
        )
    else:
        return await db.fetch(
            """
            WITH domain_stats AS (
                SELECT
                    t.domain_id,
                    COUNT(*) AS table_count,
                    ARRAY_AGG(t.display_name ORDER BY t.display_name) FILTER (WHERE t.display_name IS NOT NULL) AS table_names
                FROM db_tables t
                WHERE t.is_included = TRUE
                  AND t.domain_id IS NOT NULL
                GROUP BY t.domain_id
            ),
            domain_fields AS (
                -- 获取业务域下的代表性字段名（用于BM25增强）
                SELECT 
                    t.domain_id,
                    ARRAY_AGG(DISTINCT f.display_name ORDER BY f.display_name) FILTER (WHERE f.display_name IS NOT NULL) AS field_names
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE t.is_included = TRUE
                  AND f.is_active = TRUE
                  AND t.domain_id IS NOT NULL
                GROUP BY t.domain_id
            )
            SELECT
                bd.domain_id,
                bd.connection_id,
                bd.domain_name,
                bd.domain_code,
                bd.description,
                bd.keywords,
                -- 优先使用动态统计值，确保table_count准确
                COALESCE(ds.table_count, 0) AS table_count,
                COALESCE(ds.table_names, ARRAY[]::varchar[]) AS table_names,
                COALESCE(df.field_names, ARRAY[]::varchar[]) AS representative_fields,
                bd.is_active
            FROM business_domains bd
            LEFT JOIN domain_stats ds ON ds.domain_id = bd.domain_id
            LEFT JOIN domain_fields df ON df.domain_id = bd.domain_id
            WHERE bd.is_active = TRUE
            """
        )


async def fetch_tables_for_sync(db: DbExecutor, connection_id: Optional[UUID] = None) -> List[asyncpg.Record]:
    """查询需要同步的表（包含字段聚合信息、主键、外键等）
    
    Args:
        db: 数据库执行器
        connection_id: 可选的连接ID，为None时查询所有连接的表
    """
    # 构建关系子查询的连接过滤条件
    conn_filter = "tr.connection_id = $1" if connection_id else "TRUE"
    table_conn_filter = "t.connection_id = $1" if connection_id else "TRUE"
    
    query = f"""
        WITH rel AS (
            SELECT
                rel_base.table_id,
                jsonb_agg(
                    jsonb_build_object(
                        'target_table_id', rel_base.target_table_id,
                        'target_table_name', rel_base.target_table_name,
                        'relationship_type', rel_base.relationship_type,
                        'join_type', rel_base.join_type
                    )
                ) AS relations
            FROM (
                SELECT
                    tr.left_table_id AS table_id,
                    tr.right_table_id AS target_table_id,
                    rt.display_name AS target_table_name,
                    tr.relationship_type,
                    tr.join_type
                FROM table_relationships tr
                JOIN db_tables rt ON tr.right_table_id = rt.table_id
                WHERE {conn_filter}
                  AND tr.is_active = TRUE
                UNION ALL
                SELECT
                    tr.right_table_id AS table_id,
                    tr.left_table_id AS target_table_id,
                    lt.display_name AS target_table_name,
                    tr.relationship_type,
                    tr.join_type
                FROM table_relationships tr
                JOIN db_tables lt ON tr.left_table_id = lt.table_id
                WHERE {conn_filter}
                  AND tr.is_active = TRUE
            ) rel_base
            GROUP BY rel_base.table_id
        ),
        field_details AS (
            SELECT
                c.table_id,
                jsonb_agg(
                    jsonb_build_object(
                        'field_id', f.field_id,
                        'display_name', f.display_name,
                        'field_type', f.field_type,
                        'data_type', c.data_type,
                        'description', f.description
                    ) ORDER BY 
                        CASE f.field_type WHEN 'measure' THEN 0 ELSE 1 END,
                        f.display_name
                ) AS details
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            WHERE f.is_active = TRUE
            GROUP BY c.table_id
        ),
        pk_info AS (
            SELECT
                c.table_id,
                ARRAY_AGG(c.column_name ORDER BY c.ordinal_position) AS primary_keys
            FROM db_columns c
            WHERE c.is_primary_key = TRUE
            GROUP BY c.table_id
        )
        SELECT
            t.table_id,
            t.connection_id,
            t.table_name,
            t.schema_name,
            t.display_name,
            t.description,
            t.tags,
            t.domain_id,
            t.data_year,
            t.row_count,
            d.domain_name,
            COUNT(DISTINCT f.field_id) AS field_count,
            ARRAY_AGG(DISTINCT f.display_name)
                FILTER (WHERE f.field_id IS NOT NULL) AS field_names,
            rel.relations,
            fd.details AS field_details,
            COALESCE(pk.primary_keys, ARRAY[]::varchar[]) AS primary_keys
        FROM db_tables t
        LEFT JOIN business_domains d ON t.domain_id = d.domain_id
        LEFT JOIN db_columns c ON t.table_id = c.table_id
        LEFT JOIN fields f
            ON c.column_id = f.source_column_id
           AND f.is_active = TRUE
        LEFT JOIN rel ON rel.table_id = t.table_id
        LEFT JOIN field_details fd ON fd.table_id = t.table_id
        LEFT JOIN pk_info pk ON pk.table_id = t.table_id
        WHERE {table_conn_filter}
          AND t.is_included = TRUE
        GROUP BY
            t.table_id,
            t.connection_id,
            t.table_name,
            t.schema_name,
            t.display_name,
            t.description,
            t.tags,
            t.domain_id,
            t.data_year,
            t.row_count,
            d.domain_name,
            rel.relations,
            fd.details,
            pk.primary_keys
        """
    
    if connection_id:
        return await db.fetch(query, connection_id)
    else:
        return await db.fetch(query)


async def fetch_fields_for_sync(db: DbExecutor, connection_id: Optional[UUID] = None) -> List[asyncpg.Record]:
    """查询需要同步的字段
    
    Args:
        db: 数据库执行器
        connection_id: 可选的连接ID，为None时查询所有连接的字段
    """
    conn_filter = "t.connection_id = $1" if connection_id else "TRUE"
    
    query = f"""
        SELECT
            f.field_id,
            t.connection_id,
            f.display_name,
            f.description,
            f.field_type,
            f.synonyms,
            f.unit,
            f.format_pattern,
            t.domain_id,  -- domain_id 来自 db_tables 表，字段属于表，表属于业务域
            t.table_id,
            t.display_name AS table_display_name,
            t.table_name,
            t.schema_name,
            d.domain_name,
            c.column_name,
            c.data_type,
            c.distinct_count
        FROM fields f
        JOIN db_columns c ON f.source_column_id = c.column_id
        JOIN db_tables t ON c.table_id = t.table_id
        LEFT JOIN business_domains d ON t.domain_id = d.domain_id
        WHERE {conn_filter}
          AND t.is_included = TRUE
          AND f.is_active = TRUE
        """
    
    if connection_id:
        return await db.fetch(query, connection_id)
    else:
        return await db.fetch(query)


async def fetch_enums_for_sync(
    db: DbExecutor,
    connection_id: Optional[UUID] = None,
    field_ids: Optional[Sequence[UUID]] = None,
    *,
    only_pending: bool = True,
) -> List[asyncpg.Record]:
    """查询需要同步的枚举值（包含增强的表/业务域信息）
    
    Args:
        db: 数据库执行器
        connection_id: 可选的连接ID，为None时查询所有连接的枚举值
        field_ids: 可选的字段ID列表
        only_pending: 是否只查询待同步的枚举值
    """
    params: List[Any] = []
    param_idx = 1
    
    # 构建连接过滤条件
    if connection_id:
        conn_filter = f"t.connection_id = ${param_idx}"
        params.append(connection_id)
        param_idx += 1
    else:
        conn_filter = "TRUE"
    
    query = f"""
        SELECT
            f.field_id,
            t.connection_id,
            f.display_name AS field_name,
            f.display_name AS field_display_name,
            f.description AS field_description,
            f.field_type,
            f.enum_sync_config,
            ev.enum_value_id,
            ev.original_value,
            ev.display_value,
            ev.synonyms,
            ev.frequency,
            ev.is_active,
            t.table_id,
            t.display_name AS table_display_name,
            t.table_name,
            t.schema_name,
            d.domain_id,
            d.domain_name,
            c.column_name,
            c.distinct_count
        FROM fields f
        JOIN field_enum_values ev ON f.field_id = ev.field_id
        JOIN db_columns c ON f.source_column_id = c.column_id
        JOIN db_tables t ON c.table_id = t.table_id
        LEFT JOIN business_domains d ON t.domain_id = d.domain_id
        WHERE {conn_filter}
          AND t.is_included = TRUE
          AND f.field_type = 'dimension'
          AND f.is_active = TRUE
          AND ev.is_active = TRUE
    """

    if field_ids:
        query += f" AND f.field_id = ANY(${param_idx}::uuid[])"
        params.append(list(field_ids))
        param_idx += 1

    if only_pending:
        query += """
          AND (
                ev.is_synced_to_milvus = FALSE
             OR ev.is_synced_to_milvus IS NULL
             OR ev.last_synced_at IS NULL
             OR ev.updated_at > COALESCE(ev.last_synced_at, ev.created_at)
          )
        """

    query += " ORDER BY f.field_id, ev.frequency DESC"
    return await db.fetch(query, *params)


async def fetch_few_shots_for_sync(
    db: DbExecutor,
    connection_id: Optional[UUID] = None,
    *,
    min_quality_score: float,
    include_inactive: bool = False,
    only_verified: bool = False,
    domain_ids: Optional[Sequence[UUID]] = None,
    limit: Optional[int] = None,
) -> List[asyncpg.Record]:
    """查询Few-Shot样本（包含业务域名称）
    
    Args:
        db: 数据库执行器
        connection_id: 可选的连接ID，为None时查询所有连接的Few-Shot样本
        min_quality_score: 最小质量分数
        include_inactive: 是否包含未激活的样本
        only_verified: 是否只包含已验证的样本
        domain_ids: 可选的业务域ID列表
        limit: 返回数量限制
    """
    params: List[Any] = []
    param_idx = 1
    
    # 构建连接过滤条件
    if connection_id:
        conn_filter = f"qs.connection_id = ${param_idx}"
        params.append(connection_id)
        param_idx += 1
    else:
        conn_filter = "TRUE"
    
    # 质量分数过滤
    quality_filter = f"qs.quality_score >= ${param_idx}"
    params.append(min_quality_score)
    param_idx += 1
    
    query = f"""
        SELECT
            qs.sample_id,
            qs.connection_id,
            qs.question,
            qs.sql_text,
            qs.ir_json,
            qs.tables_json,
            qs.tables,
            qs.domain_id,
            d.domain_name,
            qs.quality_score,
            qs.source_tag,
            COALESCE(qs.metadata->>'sample_type', 'standard') AS sample_type,
            COALESCE(qs.metadata->>'sql_context', qs.sql_text) AS sql_context,
            qs.metadata->>'error_msg' AS error_msg,
            qs.last_verified_at,
            qs.is_active,
            qs.is_verified,
            qs.updated_at,
            qs.metadata
        FROM qa_few_shot_samples qs
        LEFT JOIN business_domains d ON qs.domain_id = d.domain_id
        WHERE {conn_filter}
          AND {quality_filter}
    """

    if not include_inactive:
        query += " AND qs.is_active = TRUE"

    if only_verified:
        query += " AND qs.last_verified_at IS NOT NULL"

    if domain_ids:
        query += f" AND qs.domain_id = ANY(${param_idx})"
        params.append(list(domain_ids))
        param_idx += 1

    query += " ORDER BY COALESCE(qs.last_verified_at, qs.updated_at) DESC, qs.quality_score DESC"

    if limit:
        query += f" LIMIT ${param_idx}"
        params.append(limit)

    return await db.fetch(query, *params)


async def fetch_entities_by_ids(
    db: DbExecutor,
    entity_type: EntityType,
    connection_id: Optional[UUID],
    entity_ids: Iterable[UUID],
) -> List[asyncpg.Record]:
    """
    根据实体类型与ID列表查询最新数据
    供增量同步使用
    """
    ids = list(entity_ids)
    if not ids:
        return []

    table_map: Dict[str, str] = {
        EntityType.DOMAIN.value: "business_domains",
        EntityType.TABLE.value: "db_tables",
        EntityType.FIELD.value: "fields",
        EntityType.ENUM.value: "field_enum_values",
        EntityType.FEW_SHOT.value: "qa_few_shot_samples",
    }

    table_name = table_map.get(entity_type.value)
    if not table_name:
        raise ValueError(f"不支持的实体类型: {entity_type}")

    pk_column = get_primary_key(table_name)
    params: List[Any] = [ids]
    query = f"SELECT * FROM {table_name} WHERE {pk_column} = ANY($1::uuid[])"

    if connection_id and table_name not in {"field_enum_values"}:
        query += " AND connection_id = $2"
        params.append(connection_id)

    return await db.fetch(query, *params)


def get_primary_key(table_name: str) -> str:
    """根据表名返回主键列名称"""
    mapping = {
        "business_domains": "domain_id",
        "db_tables": "table_id",
        "fields": "field_id",
        "field_enum_values": "enum_value_id",
        "qa_few_shot_samples": "sample_id",
    }

    if table_name not in mapping:
        raise ValueError(f"未知的表名: {table_name}")

    return mapping[table_name]
