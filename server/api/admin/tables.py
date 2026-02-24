"""
数据表配置API
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Response, Query
from typing import Optional
from uuid import UUID
import asyncpg
import structlog

from server.config import settings
from server.api.admin.sync_helpers import trigger_entity_sync_now
from server.models.sync import EntityType
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser

logger = structlog.get_logger()
router = APIRouter()


async def get_db_pool():
    """获取数据库连接池"""
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


@router.get("/tables")
async def list_tables(
    connection_id: Optional[UUID] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取表列表
    """
    try:
        where_clause = ""
        params = []
        
        if connection_id:
            where_clause = "WHERE connection_id = $1"
            params.append(connection_id)
        
        query = f"""
            SELECT 
                table_id, connection_id, schema_name, table_name,
                display_name, description, domain_id, tags, data_year,
                is_included, discovered_at, updated_at
            FROM db_tables
            {where_clause}
            ORDER BY table_name
        """
        
        rows = await db.fetch(query, *params)
        
        return {
            "success": True,
            "data": [dict(row) for row in rows]
        }
    
    except Exception as e:
        logger.exception("获取表列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取表列表失败: {str(e)}"
        )


@router.put("/tables/{table_id}")
async def update_table(
    table_id: UUID,
    update_data: dict,
    background_tasks: BackgroundTasks,
    response: Response,
    sync_now: bool = Query(False, description="保存后立即触发增量同步"),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    更新数据表配置
    """
    try:
        # 获取表所属连接
        table_row = await db.fetchrow(
            "SELECT connection_id FROM db_tables WHERE table_id = $1",
            table_id
        )

        if not table_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="表不存在"
            )

        connection_id = table_row["connection_id"]

        # 构建更新字段
        updates = []
        params = []
        param_count = 1
        
        # 从请求体中提取字段
        display_name = update_data.get('display_name')
        description = update_data.get('description')
        domain_id = update_data.get('domain_id')
        tags = update_data.get('tags')
        data_year = update_data.get('data_year')
        is_included = update_data.get('is_included')
        
        if display_name is not None:
            updates.append(f"display_name = ${param_count}")
            params.append(display_name)
            param_count += 1
        
        if description is not None:
            updates.append(f"description = ${param_count}")
            params.append(description)
            param_count += 1
        
        if domain_id is not None:
            # 处理空字符串的情况
            if domain_id == '' or domain_id == 'null':
                updates.append(f"domain_id = NULL")
            else:
                updates.append(f"domain_id = ${param_count}")
                params.append(domain_id)
                param_count += 1
        
        if tags is not None:
            # 处理tags字段 - 数据库类型是 TEXT[]
            if isinstance(tags, list):
                updates.append(f"tags = ${param_count}::text[]")
                params.append(tags)
                param_count += 1
            else:
                # 如果不是列表，设为空数组
                updates.append(f"tags = ARRAY[]::text[]")
        
        if data_year is not None:
            updates.append(f"data_year = ${param_count}")
            params.append(data_year)
            param_count += 1
        
        if is_included is not None:
            updates.append(f"is_included = ${param_count}")
            params.append(is_included)
            param_count += 1
        
        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="没有提供更新字段"
            )
        
        # 添加updated_at
        updates.append("updated_at = NOW()")
        
        # 添加table_id
        params.append(table_id)
        
        query = f"""
            UPDATE db_tables
            SET {", ".join(updates)}
            WHERE table_id = ${param_count}
            RETURNING table_id
        """
        
        result = await db.fetchval(query, *params)
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="表不存在"
            )

        # 清理缓存
        try:
            from server.dependencies import get_metadata_manager
            manager = get_metadata_manager()
            if manager:
                cache_key = f"conn_{connection_id}"
                if cache_key in manager._cache:
                    del manager._cache[cache_key]
                    logger.debug("表更新：缓存已清空",
                                 table_id=str(table_id),
                                 connection_id=str(connection_id))
        except Exception as cache_error:
            logger.debug("表更新：清理缓存失败", error=str(cache_error))

        # 根据配置触发同步
        if settings.immediate_sync_on_table_update:
            try:
                from server.api.admin.milvus import sync_to_milvus_task
                background_tasks.add_task(sync_to_milvus_task, connection_id)
                logger.debug("表更新：已触发Milvus全量同步",
                             table_id=str(table_id),
                             connection_id=str(connection_id))
            except Exception as sync_error:
                logger.warning("表更新：触发同步失败（已忽略）", error=str(sync_error))

        logger.info("更新表配置成功", table_id=str(table_id))

        await trigger_entity_sync_now(
            response,
            connection_id,
            [EntityType.TABLE],
            source="tables.update",
            sync_now=sync_now,
            db=db
        )
        
        return {
            "success": True,
            "message": "表配置已更新",
            "table_id": str(table_id)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新表配置失败", 
                        table_id=str(table_id), 
                        update_data=update_data,
                        error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新表配置失败: {str(e)}"
        )


@router.get("/connections/{connection_id}/stats")
async def get_connection_stats(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取数据库连接的统计信息
    """
    try:
        query = """
            SELECT 
                COUNT(DISTINCT t.table_id) AS table_count,
                COUNT(DISTINCT CASE WHEN t.is_included THEN t.table_id END) AS active_table_count,
                COUNT(DISTINCT CASE WHEN t.display_name IS NOT NULL THEN t.table_id END) AS configured_table_count,
                COUNT(DISTINCT f.field_id) AS field_count,
                COUNT(DISTINCT CASE WHEN f.auto_detected = FALSE THEN f.field_id END) AS configured_field_count,
                COUNT(DISTINCT d.domain_id) AS domain_count,
                COUNT(DISTINCT r.relationship_id) AS join_count,
                COUNT(DISTINCT gr.rule_id) AS rule_count
            FROM database_connections c
            LEFT JOIN db_tables t ON c.connection_id = t.connection_id
            LEFT JOIN db_columns col ON t.table_id = col.table_id
            LEFT JOIN fields f ON col.column_id = f.source_column_id
            LEFT JOIN business_domains d ON c.connection_id = d.connection_id AND d.is_active = TRUE
            LEFT JOIN table_relationships r ON c.connection_id = r.connection_id AND r.is_active = TRUE
            LEFT JOIN global_rules gr ON c.connection_id = gr.connection_id AND gr.is_active = TRUE
            WHERE c.connection_id = $1
            GROUP BY c.connection_id
        """
        
        row = await db.fetchrow(query, connection_id)
        
        if not row:
            # 返回默认值
            return {
                "table_count": 0,
                "active_table_count": 0,
                "configured_table_count": 0,
                "field_count": 0,
                "configured_field_count": 0,
                "domain_count": 0,
                "join_count": 0,
                "rule_count": 0
            }
        
        return dict(row)
    
    except Exception as e:
        logger.exception("获取统计信息失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取统计信息失败: {str(e)}"
        )

