"""
统一元数据管理API
提供跨数据源的统一元数据视图

新架构特点：
- 业务域不再绑定特定数据库连接
- 提供统一的元数据树视图
- 表节点显示所属的数据库连接
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field
from datetime import datetime
import asyncpg
import structlog

from server.config import settings
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser

logger = structlog.get_logger()
router = APIRouter(prefix="/unified-metadata", tags=["统一元数据管理"])


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


# ============================================================================
# 响应模型
# ============================================================================

class ConnectionNode(BaseModel):
    """数据库连接节点"""
    connection_id: UUID
    connection_name: str
    db_type: str
    is_active: bool
    table_count: int = 0


class TableNode(BaseModel):
    """表节点"""
    table_id: UUID
    table_name: str
    schema_name: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    connection_id: UUID
    connection_name: Optional[str] = None  # 缓存的连接名称
    domain_id: Optional[UUID] = None
    domain_name: Optional[str] = None
    tags: Optional[List[str]] = None
    data_year: Optional[str] = None
    is_included: bool = True
    field_count: int = 0


class DomainNode(BaseModel):
    """业务域节点"""
    domain_id: UUID
    domain_code: str
    domain_name: str
    description: Optional[str] = None
    icon: str = "📊"
    color: str = "#409eff"
    is_active: bool = True
    table_count: int = 0
    tables: List[TableNode] = []


class UnifiedMetadataTree(BaseModel):
    """统一元数据树"""
    connections: List[ConnectionNode]
    domains: List[DomainNode]
    unassigned_tables: List[TableNode]  # 未分配业务域的表
    total_tables: int
    total_fields: int


class FieldNode(BaseModel):
    """字段节点"""
    field_id: UUID
    field_name: str
    display_name: Optional[str] = None
    data_type: Optional[str] = None
    field_type: str = "dimension"
    is_active: bool = True
    has_enum: bool = False
    enum_count: int = 0


class TableDetail(BaseModel):
    """表详情"""
    table_id: UUID
    table_name: str
    schema_name: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    connection_id: UUID
    connection_name: Optional[str] = None
    domain_id: Optional[UUID] = None
    domain_name: Optional[str] = None
    tags: Optional[List[str]] = None
    data_year: Optional[str] = None
    is_included: bool = True
    fields: List[FieldNode] = []


# ============================================================================
# API 接口
# ============================================================================

@router.get("/tree", response_model=UnifiedMetadataTree)
async def get_unified_metadata_tree(
    include_inactive: bool = Query(False, description="是否包含已禁用的内容"),
    search: Optional[str] = Query(None, description="搜索关键词（表名、业务域名）"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """
    获取统一元数据树
    
    返回跨所有数据源的统一元数据视图：
    - 所有数据库连接
    - 所有业务域及其关联的表
    - 未分配业务域的表列表
    """
    try:
        # 1. 获取所有数据库连接
        conn_query = """
            SELECT 
                dc.connection_id, dc.connection_name, dc.db_type, dc.is_active,
                (SELECT COUNT(*) FROM db_tables t WHERE t.connection_id = dc.connection_id) as table_count
            FROM database_connections dc
            WHERE ($1 OR dc.is_active = TRUE)
            ORDER BY dc.connection_name
        """
        conn_rows = await db.fetch(conn_query, include_inactive)
        connections = [
            ConnectionNode(
                connection_id=row['connection_id'],
                connection_name=row['connection_name'],
                db_type=row['db_type'],
                is_active=row['is_active'],
                table_count=row['table_count']
            )
            for row in conn_rows
        ]
        
        # 2. 获取所有业务域
        domain_query = """
            SELECT 
                bd.domain_id, bd.domain_code, bd.domain_name, bd.description,
                bd.icon, bd.color, bd.is_active,
                (SELECT COUNT(*) FROM db_tables t WHERE t.domain_id = bd.domain_id) as table_count
            FROM business_domains bd
            WHERE ($1 OR bd.is_active = TRUE)
        """
        if search:
            domain_query += " AND (bd.domain_name ILIKE $2 OR bd.domain_code ILIKE $2)"
            domain_rows = await db.fetch(domain_query, include_inactive, f"%{search}%")
        else:
            domain_rows = await db.fetch(domain_query, include_inactive)
        
        # 3. 获取所有表（按业务域分组）
        table_query = """
            SELECT 
                t.table_id, t.table_name, t.schema_name, t.display_name, t.description,
                t.connection_id, t.domain_id, t.tags, t.data_year, t.is_included,
                dc.connection_name,
                bd.domain_name,
                (SELECT COUNT(*) FROM fields f WHERE f.source_column_id IN 
                    (SELECT column_id FROM db_columns WHERE table_id = t.table_id)) as field_count
            FROM db_tables t
            JOIN database_connections dc ON t.connection_id = dc.connection_id
            LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
            WHERE ($1 OR dc.is_active = TRUE)
        """
        if search:
            table_query += " AND (t.table_name ILIKE $2 OR t.display_name ILIKE $2)"
            table_rows = await db.fetch(table_query, include_inactive, f"%{search}%")
        else:
            table_rows = await db.fetch(table_query, include_inactive)
        
        # 4. 构建业务域树
        domain_tables: Dict[UUID, List[TableNode]] = {row['domain_id']: [] for row in domain_rows}
        unassigned_tables: List[TableNode] = []
        
        for row in table_rows:
            table_node = TableNode(
                table_id=row['table_id'],
                table_name=row['table_name'],
                schema_name=row['schema_name'],
                display_name=row['display_name'],
                description=row['description'],
                connection_id=row['connection_id'],
                connection_name=row['connection_name'],
                domain_id=row['domain_id'],
                domain_name=row['domain_name'],
                tags=row['tags'],
                data_year=row['data_year'],
                is_included=row['is_included'],
                field_count=row['field_count'] or 0
            )
            
            if row['domain_id'] and row['domain_id'] in domain_tables:
                domain_tables[row['domain_id']].append(table_node)
            else:
                unassigned_tables.append(table_node)
        
        # 5. 构建业务域节点
        domains = [
            DomainNode(
                domain_id=row['domain_id'],
                domain_code=row['domain_code'],
                domain_name=row['domain_name'],
                description=row['description'],
                icon=row['icon'] or "📊",
                color=row['color'] or "#409eff",
                is_active=row['is_active'],
                table_count=row['table_count'] or 0,
                tables=domain_tables.get(row['domain_id'], [])
            )
            for row in domain_rows
        ]
        
        # 按表数量降序排序
        domains.sort(key=lambda d: d.table_count, reverse=True)
        unassigned_tables.sort(key=lambda t: t.table_name)
        
        # 6. 统计总数
        total_tables = len(table_rows)
        
        # 获取字段总数
        field_count_row = await db.fetchrow("""
            SELECT COUNT(*) as cnt FROM fields WHERE is_active = TRUE
        """)
        total_fields = field_count_row['cnt'] if field_count_row else 0
        
        return UnifiedMetadataTree(
            connections=connections,
            domains=domains,
            unassigned_tables=unassigned_tables,
            total_tables=total_tables,
            total_fields=total_fields
        )
        
    except Exception as e:
        logger.exception("获取统一元数据树失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取统一元数据树失败: {str(e)}"
        )


@router.get("/tables/{table_id}", response_model=TableDetail)
async def get_table_detail(
    table_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取表详情，包含字段列表"""
    try:
        # 获取表信息
        table_row = await db.fetchrow("""
            SELECT 
                t.table_id, t.table_name, t.schema_name, t.display_name, t.description,
                t.connection_id, t.domain_id, t.tags, t.data_year, t.is_included,
                dc.connection_name,
                bd.domain_name
            FROM db_tables t
            JOIN database_connections dc ON t.connection_id = dc.connection_id
            LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
            WHERE t.table_id = $1
        """, table_id)
        
        if not table_row:
            raise HTTPException(status_code=404, detail="表不存在")
        
        # 获取字段列表
        field_rows = await db.fetch("""
            SELECT 
                f.field_id, f.field_name, f.display_name, f.data_type, f.field_type,
                f.is_active,
                (SELECT COUNT(*) FROM field_enum_values fev WHERE fev.field_id = f.field_id) as enum_count
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            WHERE c.table_id = $1
            ORDER BY c.ordinal_position, f.field_name
        """, table_id)
        
        fields = [
            FieldNode(
                field_id=row['field_id'],
                field_name=row['field_name'],
                display_name=row['display_name'],
                data_type=row['data_type'],
                field_type=row['field_type'],
                is_active=row['is_active'],
                has_enum=(row['enum_count'] or 0) > 0,
                enum_count=row['enum_count'] or 0
            )
            for row in field_rows
        ]
        
        return TableDetail(
            table_id=table_row['table_id'],
            table_name=table_row['table_name'],
            schema_name=table_row['schema_name'],
            display_name=table_row['display_name'],
            description=table_row['description'],
            connection_id=table_row['connection_id'],
            connection_name=table_row['connection_name'],
            domain_id=table_row['domain_id'],
            domain_name=table_row['domain_name'],
            tags=table_row['tags'],
            data_year=table_row['data_year'],
            is_included=table_row['is_included'],
            fields=fields
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取表详情失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取表详情失败: {str(e)}"
        )


@router.put("/tables/{table_id}/domain")
async def update_table_domain(
    table_id: UUID,
    domain_id: Optional[UUID] = Query(None, description="业务域ID，NULL表示取消关联"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """更新表的业务域关联"""
    try:
        # 验证表存在
        table_row = await db.fetchrow(
            "SELECT table_id FROM db_tables WHERE table_id = $1", 
            table_id
        )
        if not table_row:
            raise HTTPException(status_code=404, detail="表不存在")
        
        # 验证业务域存在（如果指定了）
        if domain_id:
            domain_row = await db.fetchrow(
                "SELECT domain_id FROM business_domains WHERE domain_id = $1",
                domain_id
            )
            if not domain_row:
                raise HTTPException(status_code=404, detail="业务域不存在")
        
        # 更新关联
        await db.execute(
            "UPDATE db_tables SET domain_id = $1, updated_at = CURRENT_TIMESTAMP WHERE table_id = $2",
            domain_id, table_id
        )
        
        # 更新业务域的表计数
        await db.execute("""
            UPDATE business_domains bd
            SET table_count = (SELECT COUNT(*) FROM db_tables t WHERE t.domain_id = bd.domain_id)
            WHERE bd.domain_id = $1 OR bd.domain_id = (
                SELECT domain_id FROM db_tables WHERE table_id = $2
            )
        """, domain_id, table_id)
        
        return {"success": True, "message": "表业务域关联已更新"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新表业务域关联失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新表业务域关联失败: {str(e)}"
        )


@router.post("/tables/batch-assign-domain")
async def batch_assign_tables_to_domain(
    domain_id: UUID = Query(..., description="目标业务域ID"),
    table_ids: List[UUID] = Query(..., description="要分配的表ID列表"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """批量将表分配到业务域"""
    try:
        # 验证业务域存在
        domain_row = await db.fetchrow(
            "SELECT domain_id FROM business_domains WHERE domain_id = $1",
            domain_id
        )
        if not domain_row:
            raise HTTPException(status_code=404, detail="业务域不存在")
        
        # 批量更新
        await db.execute("""
            UPDATE db_tables 
            SET domain_id = $1, updated_at = CURRENT_TIMESTAMP 
            WHERE table_id = ANY($2::uuid[])
        """, domain_id, table_ids)
        
        # 更新业务域的表计数
        await db.execute("""
            UPDATE business_domains bd
            SET table_count = (SELECT COUNT(*) FROM db_tables t WHERE t.domain_id = bd.domain_id)
        """)
        
        return {"success": True, "updated_count": len(table_ids)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("批量分配表到业务域失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"批量分配表到业务域失败: {str(e)}"
        )


@router.get("/connections/summary")
async def get_connections_summary(
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取所有数据库连接的摘要信息"""
    try:
        rows = await db.fetch("""
            SELECT 
                dc.connection_id, dc.connection_name, dc.db_type, dc.description,
                dc.is_active,
                (SELECT COUNT(*) FROM db_tables t WHERE t.connection_id = dc.connection_id) as table_count,
                (SELECT COUNT(*) FROM db_tables t 
                 WHERE t.connection_id = dc.connection_id AND t.domain_id IS NOT NULL) as assigned_table_count,
                (SELECT COUNT(DISTINCT bd.domain_id) FROM db_tables t 
                 JOIN business_domains bd ON t.domain_id = bd.domain_id
                 WHERE t.connection_id = dc.connection_id) as domain_count
            FROM database_connections dc
            WHERE dc.is_active = TRUE
            ORDER BY dc.connection_name
        """)
        
        return {
            "success": True,
            "data": [dict(row) for row in rows]
        }
        
    except Exception as e:
        logger.exception("获取连接摘要失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取连接摘要失败: {str(e)}"
        )

