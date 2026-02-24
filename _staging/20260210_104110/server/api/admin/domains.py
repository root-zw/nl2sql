"""
业务域管理API
用于两步意图识别的第一步：业务域识别
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from typing import List, Optional
from uuid import UUID
import asyncpg
import structlog
logger = structlog.get_logger()

from server.models.database import (
    BusinessDomainCreate,
    BusinessDomainUpdate,
    BusinessDomainResponse
)
from server.models.sync import EntityType
from server.api.admin.sync_helpers import trigger_entity_sync_now
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser

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
# 业务域CRUD
# ============================================================================

@router.get("/domains", response_model=List[BusinessDomainResponse])
async def list_domains(
    connection_id: Optional[UUID] = None,
    is_active: Optional[bool] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取业务域列表
    """
    try:
        where_clause = "WHERE 1=1"
        params = []
        
        if connection_id:
            where_clause += f" AND connection_id = ${len(params) + 1}"
            params.append(connection_id)
        
        if is_active is not None:
            where_clause += f" AND is_active = ${len(params) + 1}"
            params.append(is_active)
        
        query = f"""
            SELECT 
                domain_id, connection_id, domain_code, domain_name,
                description, keywords, typical_queries, icon, color,
                sort_order, table_count, is_active, created_at
            FROM business_domains
            {where_clause}
            ORDER BY connection_id, sort_order, domain_name
        """
        
        rows = await db.fetch(query, *params)
        
        return [
            BusinessDomainResponse(
                domain_id=row['domain_id'],
                connection_id=row['connection_id'],
                domain_code=row['domain_code'],
                domain_name=row['domain_name'],
                description=row['description'],
                keywords=row['keywords'] or [],
                typical_queries=row['typical_queries'] or [],
                icon=row['icon'],
                color=row['color'],
                sort_order=row['sort_order'],
                table_count=row['table_count'],
                is_active=row['is_active'],
                created_at=row['created_at']
            )
            for row in rows
        ]
    
    except Exception as e:
        logger.exception("获取业务域列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取业务域列表失败: {str(e)}"
        )


@router.get("/domains/{domain_id}", response_model=BusinessDomainResponse)
async def get_domain(
    domain_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取单个业务域详情
    """
    try:
        query = """
            SELECT 
                domain_id, connection_id, domain_code, domain_name,
                description, keywords, typical_queries, icon, color,
                sort_order, table_count, is_active, created_at
            FROM business_domains
            WHERE domain_id = $1
        """
        
        row = await db.fetchrow(query, domain_id)
        
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"业务域 {domain_id} 不存在"
            )
        
        return BusinessDomainResponse(
            domain_id=row['domain_id'],
            connection_id=row['connection_id'],
            domain_code=row['domain_code'],
            domain_name=row['domain_name'],
            description=row['description'],
            keywords=row['keywords'] or [],
            typical_queries=row['typical_queries'] or [],
            icon=row['icon'],
            color=row['color'],
            sort_order=row['sort_order'],
            table_count=row['table_count'],
            is_active=row['is_active'],
            created_at=row['created_at']
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取业务域详情失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取业务域详情失败: {str(e)}"
        )


@router.post("/domains", response_model=BusinessDomainResponse, status_code=status.HTTP_201_CREATED)
async def create_domain(
    domain: BusinessDomainCreate,
    response: Response,
    sync_now: bool = Query(False, description="保存后立即触发增量同步"),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    创建业务域（connection_id 可选，为空时创建全局业务域）
    """
    try:
        # 检查连接是否存在（如果指定了连接）
        if domain.connection_id:
            conn_exists = await db.fetchval(
                "SELECT 1 FROM database_connections WHERE connection_id = $1",
                domain.connection_id
            )
            
            if not conn_exists:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"数据库连接 {domain.connection_id} 不存在"
                )
        
        # 插入业务域
        query = """
            INSERT INTO business_domains (
                connection_id, domain_code, domain_name, description,
                keywords, typical_queries, icon, color, sort_order, is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE)
            RETURNING domain_id, connection_id, domain_code, domain_name,
                      description, keywords, typical_queries, icon, color,
                      sort_order, table_count, is_active, created_at
        """
        
        row = await db.fetchrow(
            query,
            domain.connection_id,
            domain.domain_code,
            domain.domain_name,
            domain.description,
            domain.keywords,
            domain.typical_queries,
            domain.icon,
            domain.color,
            domain.sort_order
        )
        
        logger.info(f"创建业务域成功: {domain.domain_name}")

        await trigger_entity_sync_now(
            response,
            row['connection_id'],
            [EntityType.DOMAIN],
            source="domains.create",
            sync_now=sync_now,
            db=db
        )
        
        return BusinessDomainResponse(
            domain_id=row['domain_id'],
            connection_id=row['connection_id'],
            domain_code=row['domain_code'],
            domain_name=row['domain_name'],
            description=row['description'],
            keywords=row['keywords'] or [],
            typical_queries=row['typical_queries'] or [],
            icon=row['icon'],
            color=row['color'],
            sort_order=row['sort_order'],
            table_count=row['table_count'],
            is_active=row['is_active'],
            created_at=row['created_at']
        )
    
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"业务域代码 '{domain.domain_code}' 在该数据库连接下已存在"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("创建业务域失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建业务域失败: {str(e)}"
        )


@router.put("/domains/{domain_id}", response_model=BusinessDomainResponse)
async def update_domain(
    domain_id: UUID,
    domain: BusinessDomainUpdate,
    response: Response,
    sync_now: bool = Query(False, description="保存后立即触发增量同步"),
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    更新业务域
    """
    try:
        # 检查业务域是否存在
        existing = await db.fetchrow(
            "SELECT 1 FROM business_domains WHERE domain_id = $1",
            domain_id
        )
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"业务域 {domain_id} 不存在"
            )
        
        # 构建更新语句
        updates = []
        params = []
        param_index = 1
        
        update_fields = domain.dict(exclude_unset=True)
        
        for field, value in update_fields.items():
            updates.append(f"{field} = ${param_index}")
            params.append(value)
            param_index += 1
        
        if not updates:
            # 没有任何更新，直接返回当前数据
            return await get_domain(domain_id, db)
        
        params.append(domain_id)
        
        query = f"""
            UPDATE business_domains
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE domain_id = ${param_index}
            RETURNING domain_id, connection_id, domain_code, domain_name,
                      description, keywords, typical_queries, icon, color,
                      sort_order, table_count, is_active, created_at
        """
        
        row = await db.fetchrow(query, *params)
        
        logger.info(f"更新业务域成功: {domain_id}")

        await trigger_entity_sync_now(
            response,
            row['connection_id'],
            [EntityType.DOMAIN],
            source="domains.update",
            sync_now=sync_now,
            db=db
        )
        
        return BusinessDomainResponse(
            domain_id=row['domain_id'],
            connection_id=row['connection_id'],
            domain_code=row['domain_code'],
            domain_name=row['domain_name'],
            description=row['description'],
            keywords=row['keywords'] or [],
            typical_queries=row['typical_queries'] or [],
            icon=row['icon'],
            color=row['color'],
            sort_order=row['sort_order'],
            table_count=row['table_count'],
            is_active=row['is_active'],
            created_at=row['created_at']
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新业务域失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新业务域失败: {str(e)}"
        )


@router.delete("/domains/{domain_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_domain(
    domain_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    删除业务域（将关联表的domain_id设为NULL）
    """
    try:
        # 先读取业务域，避免触发器因 connection_id 为空写入 milvus_pending_changes 失败
        domain_row = await db.fetchrow(
            "SELECT domain_id, connection_id FROM business_domains WHERE domain_id = $1",
            domain_id
        )
        if not domain_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"业务域 {domain_id} 不存在"
            )
        
        # 如果业务域的 connection_id 为 NULL，尝试从关联表继承一个连接以满足触发器的 NOT NULL 约束
        connection_id = domain_row["connection_id"]
        if connection_id is None:
            connection_id = await db.fetchval(
                "SELECT connection_id FROM db_tables WHERE domain_id = $1 LIMIT 1",
                domain_id
            )
            # 如果业务域下没有表，再兜底选一个任意连接，确保触发器能落库
            if connection_id is None:
                connection_id = await db.fetchval(
                    "SELECT connection_id FROM database_connections LIMIT 1"
                )
            if connection_id is not None:
                await db.execute(
                    "UPDATE business_domains SET connection_id = $1 WHERE domain_id = $2",
                    connection_id,
                    domain_id
                )
        
        if connection_id is None:
            # 系统没有可用连接，关闭触发器执行删除（不会写入 milvus_pending_changes）
            async with db.transaction():
                await db.execute("SET LOCAL session_replication_role = replica")
                result = await db.execute(
                    "DELETE FROM business_domains WHERE domain_id = $1",
                    domain_id
                )
            logger.warning("删除业务域时未找到可用连接，已跳过同步触发器: %s", domain_id)
        else:
            result = await db.execute(
                "DELETE FROM business_domains WHERE domain_id = $1",
                domain_id
            )
        
        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"业务域 {domain_id} 不存在"
            )
        
        logger.info(f"删除业务域成功: {domain_id}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除业务域失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除业务域失败: {str(e)}"
        )


# ============================================================================
# 业务域自动识别（基于表名、列名的聚类分析）
# ============================================================================

# 自动识别业务域功能已移除
# @router.post("/domains/auto-detect/{connection_id}")


@router.post("/domains/{domain_id}/assign-tables")
async def assign_tables_to_domain(
    domain_id: UUID,
    table_ids: List[UUID],
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    将表分配到业务域
    """
    try:
        # 检查业务域是否存在
        domain = await db.fetchrow(
            "SELECT domain_id FROM business_domains WHERE domain_id = $1",
            domain_id
        )
        
        if not domain:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"业务域 {domain_id} 不存在"
            )
        
        # 批量更新表的domain_id
        await db.execute("""
            UPDATE db_tables
            SET domain_id = $1, updated_at = NOW()
            WHERE table_id = ANY($2::uuid[])
        """, domain_id, table_ids)
        
        # 更新业务域的table_count
        table_count = await db.fetchval("""
            SELECT COUNT(*)
            FROM db_tables
            WHERE domain_id = $1
        """, domain_id)
        
        await db.execute("""
            UPDATE business_domains
            SET table_count = $1, updated_at = NOW()
            WHERE domain_id = $2
        """, table_count, domain_id)
        
        logger.info(f"分配 {len(table_ids)} 张表到业务域 {domain_id}")
        
        return {
            "success": True,
            "message": f"成功分配 {len(table_ids)} 张表",
            "domain_id": str(domain_id),
            "table_count": table_count
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("分配表到业务域失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"分配表到业务域失败: {str(e)}"
        )


@router.get("/domains/{domain_id}/tables")
async def get_domain_tables(
    domain_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取业务域下的所有表
    """
    try:
        rows = await db.fetch("""
            SELECT 
                table_id, schema_name, table_name, display_name,
                description, tags, row_count, column_count,
                is_included, discovered_at
            FROM db_tables
            WHERE domain_id = $1
            ORDER BY schema_name, table_name
        """, domain_id)
        
        return {
            "total": len(rows),
            "tables": [dict(r) for r in rows]
        }
    
    except Exception as e:
        logger.exception("获取业务域表列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取业务域表列表失败: {str(e)}"
        )

