"""
字段配置管理API
支持自动识别 + 手动精调
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from typing import List, Optional
from uuid import UUID
import asyncpg
import structlog
from server.config import settings

logger = structlog.get_logger()

from server.models.database import (
    FieldCreate,
    FieldUpdate,
    FieldResponse,
    EnumValueCreate,
    EnumValueUpdate,
    EnumValueResponse
)
from server.models.sync import EntityType
from server.api.admin.sync_helpers import trigger_entity_sync_now
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser
# 字段类型自动识别
from server.utils.field_analyzer import FieldAnalyzer

router = APIRouter()


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


# ============================================================================
# 字段CRUD
# ============================================================================

@router.get("/fields", response_model=List[FieldResponse])
async def list_fields(
    connection_id: Optional[UUID] = None,
    table_id: Optional[UUID] = None,
    field_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """获取字段列表"""
    try:
        where_clause = "WHERE 1=1"
        params = []

        if connection_id:
            where_clause += f" AND f.connection_id = ${len(params) + 1}"
            params.append(connection_id)

        if table_id:
            where_clause += f" AND t.table_id = ${len(params) + 1}"
            params.append(table_id)

        if field_type:
            where_clause += f" AND f.field_type = ${len(params) + 1}"
            params.append(field_type)

        if is_active is not None:
            where_clause += f" AND f.is_active = ${len(params) + 1}"
            params.append(is_active)

        query = f"""
            SELECT
                f.field_id, f.connection_id, f.source_type, f.source_column_id,
                f.source_expression, f.field_type, f.display_name, f.description,
                f.synonyms, f.default_aggregation, f.allowed_aggregations,
                f.unit, f.unit_conversion, f.format_pattern, f.dimension_type, f.hierarchy_level,
                f.is_additive, f.is_unique, f.tags, f.business_category,
                f.auto_detected, f.confidence_score, f.is_active, f.created_at,
                f.show_in_detail,
                f.enum_sync_config,
                t.table_name, t.schema_name, t.table_id, c.column_name, c.data_type,
                -- 枚举值统计（仅维度字段）
                COUNT(e.enum_value_id) as enum_count,
                COUNT(*) FILTER (WHERE e.is_synced_to_milvus = true) as synced_enum_count,
                MAX(e.last_synced_at) as last_synced_at
            FROM fields f
            LEFT JOIN db_columns c ON f.source_column_id = c.column_id
            LEFT JOIN db_tables t ON c.table_id = t.table_id
            LEFT JOIN field_enum_values e ON f.field_id = e.field_id AND e.is_active = true
            {where_clause}
            GROUP BY f.field_id, t.table_name, t.schema_name, t.table_id, c.column_name, c.data_type
            ORDER BY f.created_at DESC
        """

        rows = await db.fetch(query, *params)

        #  处理JSONB字段（从字符串转换为字典）
        import json
        result = []
        for row in rows:
            row_dict = {
                **dict(row),
                'synonyms': row['synonyms'] or [],
                'allowed_aggregations': row['allowed_aggregations'] or [],
                'tags': row['tags'] or []
            }
            # 解析unit_conversion（JSONB字段）
            if row_dict.get('unit_conversion'):
                try:
                    if isinstance(row_dict['unit_conversion'], str):
                        row_dict['unit_conversion'] = json.loads(row_dict['unit_conversion'])
                except (json.JSONDecodeError, TypeError):
                    row_dict['unit_conversion'] = None

            # 解析enum_sync_config（JSONB字段）
            if row_dict.get('enum_sync_config'):
                try:
                    if isinstance(row_dict['enum_sync_config'], str):
                        row_dict['enum_sync_config'] = json.loads(row_dict['enum_sync_config'])
                except (json.JSONDecodeError, TypeError):
                    row_dict['enum_sync_config'] = None

            result.append(row_dict)

        return result

    except Exception as e:
        logger.exception("获取字段列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取字段列表失败: {str(e)}"
        )

@router.put("/fields/{field_id}", response_model=FieldResponse)
async def update_field(
    field_id: UUID,
    field: FieldUpdate,
    response: Response,
    sync_now: bool = Query(False, description="保存后立即触发增量同步"),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    更新字段（手动精调）
    注意：field_id参数可以是field_id或column_id

    更新后会自动：
    1. 清空对应连接的缓存
    2. 异步同步到Milvus
    """
    try:
        # 检查字段是否存在（先尝试field_id）
        existing = await db.fetchrow(
            "SELECT field_id, auto_detected FROM fields WHERE field_id = $1",
            field_id
        )

        # 如果找不到，尝试通过column_id查找
        if not existing:
            existing = await db.fetchrow(
                "SELECT field_id, auto_detected FROM fields WHERE source_column_id = $1",
                field_id
            )

        # 如果fields表中没有记录，自动创建（基于db_columns）
        if not existing:
            # 获取column信息（包含主键信息用于类型识别）
            column = await db.fetchrow("""
                SELECT c.column_id, c.column_name, c.data_type, c.is_primary_key,
                       c.is_foreign_key, t.connection_id
                FROM db_columns c
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE c.column_id = $1
            """, field_id)

            if not column:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"字段不存在，请先同步Schema"
                )

            # 自动识别字段类型
            analysis = FieldAnalyzer.analyze_field(
                column_name=column['column_name'],
                data_type=column['data_type'] or '',
                is_primary_key=column['is_primary_key'] or False,
                is_foreign_key=column['is_foreign_key'] or False
            )

            # 创建新的field记录
            new_field_id = await db.fetchval("""
                INSERT INTO fields (
                    connection_id, source_type, source_column_id,
                    field_type, display_name, default_aggregation, unit,
                    auto_detected, is_active, show_in_detail
                )
                VALUES ($1, 'column', $2, $3, $4, $5, $6, TRUE, TRUE, FALSE)
                RETURNING field_id
            """, column['connection_id'], column['column_id'],
                analysis.field_type, analysis.display_name,
                analysis.default_aggregation, analysis.unit)

            logger.debug(f"自动创建field记录: {new_field_id} for column {field_id}, 类型: {analysis.field_type}")

            existing = {'field_id': new_field_id, 'auto_detected': True}

        # 使用实际的field_id
        field_id = existing['field_id']

        # 构建更新语句
        updates = []
        params = []
        param_index = 1

        update_fields = field.dict(exclude_unset=True)

        # 如果 display_name 为空字符串，从 column 获取
        if 'display_name' in update_fields and not update_fields['display_name']:
            column_info = await db.fetchrow("""
                SELECT c.column_name
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                WHERE f.field_id = $1
            """, field_id)
            if column_info:
                update_fields['display_name'] = column_info['column_name']

        # 如果是手动修改，标记为非自动检测
        if existing['auto_detected'] and update_fields:
            updates.append(f"auto_detected = FALSE")

        for field_name, value in update_fields.items():
            # 处理数组类型字段
            if field_name in ['synonyms', 'allowed_aggregations', 'tags']:
                if isinstance(value, list):
                    updates.append(f"{field_name} = ${param_index}::text[]")
                    params.append(value)
                    param_index += 1
                else:
                    # 如果不是列表，设为空数组
                    updates.append(f"{field_name} = ARRAY[]::text[]")
            #  处理JSONB类型字段
            elif field_name in ['unit_conversion', 'enum_sync_config']:
                if value is None:
                    updates.append(f"{field_name} = NULL")
                else:
                    import json
                    updates.append(f"{field_name} = ${param_index}::jsonb")
                    params.append(json.dumps(value))
                    param_index += 1
            else:
                updates.append(f"{field_name} = ${param_index}")
                params.append(value)
                param_index += 1

        if not updates:
            # 没有任何更新，返回当前数据
            return await db.fetchrow("SELECT * FROM fields WHERE field_id = $1", field_id)

        params.append(field_id)

        query = f"""
            UPDATE fields
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE field_id = ${param_index}
            RETURNING field_id, connection_id, source_type, source_column_id,
                      source_expression, field_type, display_name, description,
                      synonyms, default_aggregation, allowed_aggregations,
                      unit, unit_conversion, format_pattern, dimension_type, hierarchy_level,
                      is_additive, is_unique, tags, business_category,
                      auto_detected, confidence_score, is_active, created_at, show_in_detail,
                      enum_sync_config
        """

        row = await db.fetchrow(query, *params)

        logger.debug(f"更新字段成功: {field_id}")

        #  解析JSONB字段（unit_conversion 和 enum_sync_config）
        import json
        unit_conversion_parsed = row['unit_conversion']
        if unit_conversion_parsed and isinstance(unit_conversion_parsed, str):
            try:
                unit_conversion_parsed = json.loads(unit_conversion_parsed)
            except (json.JSONDecodeError, TypeError):
                unit_conversion_parsed = None
        
        enum_sync_config_parsed = row.get('enum_sync_config')
        if enum_sync_config_parsed and isinstance(enum_sync_config_parsed, str):
            try:
                enum_sync_config_parsed = json.loads(enum_sync_config_parsed)
            except (json.JSONDecodeError, TypeError):
                enum_sync_config_parsed = None

        #  自动清空缓存并依赖触发器执行增量同步
        connection_id = row['connection_id']
        try:
            # 1. 清空缓存
            from server.dependencies import get_metadata_manager
            manager = get_metadata_manager()
            if manager:
                cache_key = f"conn_{connection_id}"
                if cache_key in manager._cache:
                    del manager._cache[cache_key]
                    logger.debug(
                        "字段更新：缓存已清空",
                        field_id=str(field_id),
                        connection_id=str(connection_id)
                    )
            logger.debug(
                "字段更新：依赖触发器执行增量同步",
                field_id=str(field_id),
                connection_id=str(connection_id)
            )
        except Exception as e:
            logger.warning(
                "字段更新后的自动同步失败（不影响主流程）",
                error=str(e)
            )

        result = FieldResponse(
            field_id=row['field_id'],
            connection_id=row['connection_id'],
            source_type=row['source_type'],
            source_column_id=row['source_column_id'],
            source_expression=row['source_expression'],
            field_type=row['field_type'],
            display_name=row['display_name'],
            description=row['description'],
            synonyms=row['synonyms'] or [],
            default_aggregation=row['default_aggregation'],
            allowed_aggregations=row['allowed_aggregations'] or [],
            unit=row['unit'],
            unit_conversion=unit_conversion_parsed,  #  使用解析后的字典
            format_pattern=row['format_pattern'],
            dimension_type=row['dimension_type'],
            hierarchy_level=row['hierarchy_level'],
            is_additive=row['is_additive'],
            is_unique=row['is_unique'],
            tags=row['tags'] or [],
            business_category=row['business_category'],
            auto_detected=row['auto_detected'],
            confidence_score=row['confidence_score'],
            is_active=row['is_active'],
            show_in_detail=row.get('show_in_detail', False),  # 添加 show_in_detail 字段
            enum_sync_config=enum_sync_config_parsed,  # 添加 enum_sync_config 字段
            created_at=row['created_at']
        )

        await trigger_entity_sync_now(
            response,
            row['connection_id'],
            [EntityType.FIELD],
            source="fields.update",
            sync_now=sync_now,
            db=db
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新字段失败",
                        field_id=str(field_id),
                        update_fields=field.dict(exclude_unset=True),
                        error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新字段失败: {str(e)}"
        )


# ============================================================================
# 枚举值管理（维度字段）
# ============================================================================

@router.post("/fields/{field_id}/enum-values/sample")
async def sample_enum_values(
    field_id: UUID,
    top_n: int = 1000,  # 默认1000个，用户可自行调整
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    采样枚举值（维度字段）

    从实际数据中采样Top N个不同值（按频次降序）
    
    Args:
        field_id: 字段ID（也可以是column_id）
        top_n: 采样数量，默认1000
               - 对于低基数字段（如行政区），100-500足够
               - 对于中基数字段（如街道），1000-3000合适
               - 对于高基数字段（如公司名），可设置5000-10000或更多
    
    注意：
    - 采样后的枚举值存储在PostgreSQL中
    - 根据字段的enum_sync_config决定同步多少到Milvus
    - 查询时通过Milvus向量检索返回最相关的Top-K
    - 建议先采样少量（如1000）测试，确认无误后再增加
    """
    try:
        # 1. 获取字段信息（尝试通过field_id查找）
        field = await db.fetchrow("""
            SELECT f.field_id, f.field_type, f.source_column_id, f.connection_id,
                   c.column_name, c.table_id,
                   t.schema_name, t.table_name
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE f.field_id = $1
        """, field_id)

        # 如果通过field_id找不到，尝试通过column_id查找
        if not field:
            field = await db.fetchrow("""
                SELECT f.field_id, f.field_type, f.source_column_id, f.connection_id,
                       c.column_name, c.table_id,
                       t.schema_name, t.table_name
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE c.column_id = $1
            """, field_id)

        # 如果还是找不到，尝试自动创建field记录
        if not field:
            # 获取column信息（包含主键信息用于类型识别）
            column = await db.fetchrow("""
                SELECT c.column_id, c.column_name, c.data_type, c.is_primary_key,
                       c.is_foreign_key, t.connection_id,
                       t.schema_name, t.table_name, t.table_id
                FROM db_columns c
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE c.column_id = $1
            """, field_id)

            if not column:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"字段不存在，请先同步Schema"
                )

            # 自动识别字段类型
            analysis = FieldAnalyzer.analyze_field(
                column_name=column['column_name'],
                data_type=column['data_type'] or '',
                is_primary_key=column['is_primary_key'] or False,
                is_foreign_key=column['is_foreign_key'] or False
            )

            # 创建新的field记录
            new_field_id = await db.fetchval("""
                INSERT INTO fields (
                    connection_id, source_type, source_column_id,
                    field_type, display_name, default_aggregation, unit,
                    auto_detected, is_active, show_in_detail
                )
                VALUES ($1, 'column', $2, $3, $4, $5, $6, TRUE, TRUE, FALSE)
                RETURNING field_id
            """, column['connection_id'], column['column_id'],
                analysis.field_type, analysis.display_name,
                analysis.default_aggregation, analysis.unit)

            logger.debug(f"自动创建field记录: {new_field_id} for column {field_id}, 类型: {analysis.field_type}")

            # 重新查询field信息
            field = {
                'field_id': new_field_id,
                'field_type': analysis.field_type,
                'source_column_id': column['column_id'],
                'connection_id': column['connection_id'],
                'column_name': column['column_name'],
                'table_id': column['table_id'],
                'schema_name': column['schema_name'],
                'table_name': column['table_name']
            }

        if field['field_type'] != 'dimension':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="只能对维度字段采样枚举值"
            )

        # 2. 获取数据库连接信息
        conn_info = await db.fetchrow("""
            SELECT host, port, database_name, username, password_encrypted, db_type
            FROM database_connections
            WHERE connection_id = $1
        """, field['connection_id'])

        if not conn_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="数据库连接不存在"
            )

        # 3. 使用统一的DatabaseInspector连接业务数据库，采样数据
        from server.utils.db_inspector import get_inspector
        from server.api.admin.datasources import decrypt_password

        # 解密密码
        password = decrypt_password(conn_info['password_encrypted'])
        db_type = conn_info['db_type']

        # 获取Inspector
        inspector = get_inspector(
            db_type=db_type,
            host=conn_info['host'],
            port=conn_info['port'],
            database=conn_info['database_name'],
            username=conn_info['username'],
            password=password
        )

        try:
            # 调用Inspector的采样方法
            rows_list = await inspector.sample_enum_values(
                schema_name=field['schema_name'],
                table_name=field['table_name'],
                column_name=field['column_name'],
                top_n=top_n
            )

            # 4. 保存枚举值到元数据库
            sampled_count = 0
            for row in rows_list:
                value = str(row['value']) if row['value'] is not None else None
                if not value:
                    continue

                # 检查是否已存在
                existing = await db.fetchval("""
                    SELECT enum_value_id FROM field_enum_values
                    WHERE field_id = $1 AND original_value = $2
                """, field['field_id'], value)

                if not existing:
                    # 插入新枚举值
                    await db.execute("""
                        INSERT INTO field_enum_values (
                            field_id, original_value, display_value, frequency
                        )
                        VALUES ($1, $2, $2, $3)
                    """, field['field_id'], value, row['frequency'])
                    sampled_count += 1
                else:
                    # 更新频率
                    await db.execute("""
                        UPDATE field_enum_values
                        SET frequency = $1, updated_at = NOW()
                        WHERE enum_value_id = $2
                    """, row['frequency'], existing)

            logger.debug(f"采样成功: field_id={field['field_id']}, db_type={db_type}, 新增={sampled_count}, 总计={len(rows_list)}")

            return {
                "success": True,
                "message": f"采样成功，新增 {sampled_count} 个枚举值，更新 {len(rows_list) - sampled_count} 个",
                "field_id": str(field['field_id']),
                "sampled_count": sampled_count,
                "total_count": len(rows_list)
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"连接业务数据库失败: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"连接业务数据库失败: {str(e)}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("采样枚举值失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"采样枚举值失败: {str(e)}"
        )


@router.get("/fields/{field_id}/enum-values", response_model=List[EnumValueResponse])
async def list_enum_values(
    field_id: UUID,
    is_active: Optional[bool] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取字段的枚举值列表
    注意：field_id参数可以是field_id或column_id
    """
    try:
        # 先尝试通过field_id查找
        actual_field_id = await db.fetchval(
            "SELECT field_id FROM fields WHERE field_id = $1",
            field_id
        )

        # 如果找不到，尝试通过column_id查找
        if not actual_field_id:
            actual_field_id = await db.fetchval(
                "SELECT field_id FROM fields WHERE source_column_id = $1",
                field_id
            )

        # 如果还是找不到，返回空列表
        if not actual_field_id:
            return []

        where_clause = "WHERE field_id = $1"
        params = [actual_field_id]

        if is_active is not None:
            where_clause += f" AND is_active = ${len(params) + 1}"
            params.append(is_active)

        query = f"""
            SELECT
                enum_value_id, field_id, original_value, display_value,
                synonyms, frequency, is_active, includes_values
            FROM field_enum_values
            {where_clause}
            ORDER BY frequency DESC, original_value
        """

        rows = await db.fetch(query, *params)

        return [
            EnumValueResponse(
                enum_value_id=row['enum_value_id'],
                field_id=row['field_id'],
                original_value=row['original_value'],
                display_value=row['display_value'],
                synonyms=row['synonyms'] or [],
                frequency=row['frequency'],
                is_active=row['is_active'],
                includes_values=list(row['includes_values']) if row['includes_values'] else None
            )
            for row in rows
        ]

    except Exception as e:
        logger.exception("获取枚举值列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取枚举值列表失败: {str(e)}"
        )


@router.post("/fields/{field_id}/enum-values")
async def create_enum_value(
    field_id: UUID,
    data: EnumValueCreate,
    response: Response,
    sync_now: bool = Query(False, description="保存后立即触发枚举同步"),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """创建枚举值"""
    try:
        query = """
            INSERT INTO field_enum_values (
                field_id, original_value, display_value, synonyms, frequency, is_active, includes_values
            )
            VALUES ($1, $2, $3, $4, $5, TRUE, $6)
            RETURNING enum_value_id
        """

        enum_value_id = await db.fetchval(
            query,
            field_id,
            data.original_value,
            data.display_value,
            data.synonyms or [],
            data.frequency or 0,
            data.includes_values or None
        )

        logger.debug("创建枚举值成功", enum_value_id=str(enum_value_id))

        await _trigger_enum_sync_if_needed(response, field_id, sync_now, db, source="fields.create_enum")
        return {
            "success": True,
            "enum_value_id": str(enum_value_id)
        }

    except Exception as e:
        logger.exception("创建枚举值失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建枚举值失败: {str(e)}"
        )


@router.put("/fields/{field_id}/enum-values/{enum_value_id}")
async def update_enum_value(
    field_id: UUID,
    enum_value_id: UUID,
    data: EnumValueUpdate,
    response: Response,
    sync_now: bool = Query(False, description="保存后立即触发枚举同步"),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """更新枚举值"""
    try:
        # 构建更新字段
        updates = []
        params = []
        param_count = 1

        if data.display_value is not None:
            updates.append(f"display_value = ${param_count}")
            params.append(data.display_value)
            param_count += 1

        if data.synonyms is not None:
            updates.append(f"synonyms = ${param_count}")
            params.append(data.synonyms)
            param_count += 1

        if data.includes_values is not None:
            updates.append(f"includes_values = ${param_count}")
            params.append(data.includes_values)
            param_count += 1

        if data.is_active is not None:
            updates.append(f"is_active = ${param_count}")
            params.append(data.is_active)
            param_count += 1

        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="没有提供更新字段"
            )

        # 添加updated_at
        updates.append("updated_at = NOW()")

        # 添加条件参数
        params.extend([enum_value_id, field_id])

        query = f"""
            UPDATE field_enum_values
            SET {", ".join(updates)}
            WHERE enum_value_id = ${param_count} AND field_id = ${param_count + 1}
            RETURNING enum_value_id
        """

        result = await db.fetchval(query, *params)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="枚举值不存在"
            )

        logger.debug("更新枚举值成功", enum_value_id=str(enum_value_id))

        await db.execute(
            """
            UPDATE field_enum_values
            SET is_synced_to_milvus = FALSE,
                last_synced_at = NULL
            WHERE enum_value_id = $1
            """,
            enum_value_id,
        )

        await _trigger_enum_sync_if_needed(response, field_id, sync_now, db, source="fields.update_enum")
        return {
            "success": True,
            "enum_value_id": str(enum_value_id)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新枚举值失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新枚举值失败: {str(e)}"
        )


@router.delete("/fields/{field_id}/enum-values/{enum_value_id}")
async def delete_enum_value(
    field_id: UUID,
    enum_value_id: UUID,
    response: Response,
    sync_now: bool = Query(False, description="保存后立即触发枚举同步"),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """删除枚举值"""
    try:
        query = """
            DELETE FROM field_enum_values
            WHERE enum_value_id = $1 AND field_id = $2
            RETURNING enum_value_id
        """

        result = await db.fetchval(query, enum_value_id, field_id)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="枚举值不存在"
            )

        logger.debug("删除枚举值成功", enum_value_id=str(enum_value_id))

        await _trigger_enum_sync_if_needed(response, field_id, sync_now, db, source="fields.delete_enum")
        return {
            "success": True,
            "message": "枚举值已删除"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除枚举值失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除枚举值失败: {str(e)}"
        )


async def _trigger_enum_sync_if_needed(
    response: Response,
    field_id: UUID,
    sync_now: bool,
    db,
    *,
    source: str
) -> None:
    if not sync_now:
        return
    connection_id = await _get_connection_id_by_field(db, field_id)
    if connection_id:
        await trigger_entity_sync_now(
            response,
            connection_id,
            [EntityType.ENUM],
            source=source,
            sync_now=True,
            db=db
        )


async def _get_connection_id_by_field(db, field_id: UUID) -> Optional[UUID]:
    return await db.fetchval(
        "SELECT connection_id FROM fields WHERE field_id = $1",
        field_id
    )

