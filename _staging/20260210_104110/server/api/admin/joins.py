"""
表关系管理API
支持自动识别 + 手动确认
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional
from uuid import UUID
import asyncpg
import structlog
logger = structlog.get_logger()

from server.models.database import (
    TableRelationshipCreate,
    TableRelationshipUpdate,
    TableRelationshipResponse
)
from server.utils.relationship_detector import RelationshipDetector
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
# 表关系CRUD
# ============================================================================

@router.get("/relationships", response_model=List[TableRelationshipResponse])
async def list_relationships(
    connection_id: Optional[UUID] = None,
    is_confirmed: Optional[bool] = None,
    is_active: Optional[bool] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """获取表关系列表"""
    try:
        where_clause = "WHERE 1=1"
        params = []
        
        if connection_id:
            where_clause += f" AND connection_id = ${len(params) + 1}"
            params.append(connection_id)
        
        if is_confirmed is not None:
            where_clause += f" AND is_confirmed = ${len(params) + 1}"
            params.append(is_confirmed)
        
        if is_active is not None:
            where_clause += f" AND is_active = ${len(params) + 1}"
            params.append(is_active)
        
        query = f"""
            SELECT 
                tr.relationship_id, tr.connection_id, tr.left_table_id, tr.right_table_id,
                tr.left_column_id, tr.right_column_id, tr.relationship_type, tr.join_type,
                tr.detection_method, tr.confidence_score, tr.is_confirmed, tr.is_active,
                tr.relationship_name, tr.description, tr.detected_at,
                dc.connection_name
            FROM table_relationships tr
            LEFT JOIN database_connections dc ON tr.connection_id = dc.connection_id
            {where_clause.replace('connection_id', 'tr.connection_id').replace('is_confirmed', 'tr.is_confirmed').replace('is_active', 'tr.is_active')}
            ORDER BY tr.confidence_score DESC, tr.detected_at DESC
        """
        
        rows = await db.fetch(query, *params)
        
        return [
            TableRelationshipResponse(
                relationship_id=row['relationship_id'],
                connection_id=row['connection_id'],
                connection_name=row['connection_name'],  # 数据源名称
                left_table_id=row['left_table_id'],
                right_table_id=row['right_table_id'],
                left_column_id=row['left_column_id'],
                right_column_id=row['right_column_id'],
                relationship_type=row['relationship_type'],
                join_type=row['join_type'],
                detection_method=row['detection_method'],
                confidence_score=row['confidence_score'],
                is_confirmed=row['is_confirmed'],
                relationship_name=row['relationship_name'],
                description=row['description'],
                detected_at=row['detected_at']
            )
            for row in rows
        ]
    
    except Exception as e:
        logger.exception("获取表关系列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取表关系列表失败: {str(e)}"
        )


@router.post("/relationships/auto-detect/{connection_id}")
async def auto_detect_relationships(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    自动识别表关系
    
    方法：
    1. 外键约束检测
    2. 名称相似度检测
    3. 数据分析检测（TODO）
    """
    try:
        # 1. 获取所有表
        tables = await db.fetch("""
            SELECT table_id, table_name, schema_name
            FROM db_tables
            WHERE connection_id = $1 AND is_included = TRUE
        """, connection_id)
        
        if not tables:
            return {
                "success": True,
                "message": "没有可用的表",
                "detected_count": 0
            }
        
        # 2. 获取所有列
        columns = await db.fetch("""
            SELECT 
                c.column_id, c.table_id, c.column_name, c.is_primary_key,
                c.is_foreign_key, c.referenced_table_id, c.referenced_column_id
            FROM db_columns c
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE t.connection_id = $1 AND t.is_included = TRUE
        """, connection_id)
        
        # 3. 调用检测器
        detector = RelationshipDetector()
        suggestions = detector.detect_all(
            [dict(t) for t in tables],
            [dict(c) for c in columns]
        )
        
        # 4. 插入数据库（跳过已存在的）
        detected_count = 0
        
        for suggestion in suggestions:
            # 检查是否已存在
            existing = await db.fetchval("""
                SELECT relationship_id
                FROM table_relationships
                WHERE connection_id = $1
                  AND left_table_id = $2
                  AND left_column_id = $3
                  AND right_table_id = $4
                  AND right_column_id = $5
            """, 
                connection_id,
                UUID(suggestion.left_table_id),
                UUID(suggestion.left_column_id),
                UUID(suggestion.right_table_id),
                UUID(suggestion.right_column_id)
            )
            
            if existing:
                continue
            
            # 生成关系名称
            relationship_name = (
                f"{suggestion.left_table_name}.{suggestion.left_column_name} → "
                f"{suggestion.right_table_name}.{suggestion.right_column_name}"
            )
            
            # 插入
            await db.execute("""
                INSERT INTO table_relationships (
                    connection_id, left_table_id, right_table_id,
                    left_column_id, right_column_id, relationship_type,
                    join_type, detection_method, confidence_score,
                    is_confirmed, is_active, relationship_name
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE, $11)
            """,
                connection_id,
                UUID(suggestion.left_table_id),
                UUID(suggestion.right_table_id),
                UUID(suggestion.left_column_id),
                UUID(suggestion.right_column_id),
                suggestion.relationship_type,
                suggestion.join_type,
                suggestion.detection_method,
                suggestion.confidence_score,
                suggestion.detection_method == 'foreign_key',  # 外键自动确认
                relationship_name
            )
            
            detected_count += 1
        
        logger.info(
            f"表关系自动识别完成",
            connection_id=str(connection_id),
            detected=detected_count
        )
        
        return {
            "success": True,
            "message": f"成功识别 {detected_count} 个表关系",
            "detected_count": detected_count,
            "total_suggestions": len(suggestions)
        }
    
    except Exception as e:
        logger.exception("表关系自动识别失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"表关系自动识别失败: {str(e)}"
        )


@router.post("/relationships", response_model=TableRelationshipResponse, status_code=status.HTTP_201_CREATED)
async def create_relationship(
    relationship: TableRelationshipCreate,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """手动创建表关系"""
    try:
        # 插入关系
        row = await db.fetchrow("""
            INSERT INTO table_relationships (
                connection_id, left_table_id, right_table_id,
                left_column_id, right_column_id, relationship_type,
                join_type, detection_method, relationship_name,
                description, is_confirmed, is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE, TRUE)
            RETURNING relationship_id, connection_id, left_table_id, right_table_id,
                      left_column_id, right_column_id, relationship_type, join_type,
                      detection_method, confidence_score, is_confirmed,
                      relationship_name, description, detected_at
        """,
            relationship.connection_id,
            relationship.left_table_id,
            relationship.right_table_id,
            relationship.left_column_id,
            relationship.right_column_id,
            relationship.relationship_type,
            relationship.join_type,
            relationship.detection_method,
            relationship.relationship_name,
            relationship.description
        )
        
        logger.info(f"创建表关系成功: {relationship.relationship_name}")
        
        return TableRelationshipResponse(
            relationship_id=row['relationship_id'],
            connection_id=row['connection_id'],
            left_table_id=row['left_table_id'],
            right_table_id=row['right_table_id'],
            left_column_id=row['left_column_id'],
            right_column_id=row['right_column_id'],
            relationship_type=row['relationship_type'],
            join_type=row['join_type'],
            detection_method=row['detection_method'],
            confidence_score=row['confidence_score'],
            is_confirmed=row['is_confirmed'],
            relationship_name=row['relationship_name'],
            description=row['description'],
            detected_at=row['detected_at']
        )
    
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="该表关系已存在"
        )
    except Exception as e:
        logger.exception("创建表关系失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建表关系失败: {str(e)}"
        )


@router.put("/relationships/{relationship_id}/confirm")
async def confirm_relationship(
    relationship_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """确认表关系（人工审核）"""
    try:
        result = await db.execute("""
            UPDATE table_relationships
            SET is_confirmed = TRUE,
                confirmed_at = NOW(),
                updated_at = NOW()
            WHERE relationship_id = $1
        """, relationship_id)
        
        if result == "UPDATE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"表关系 {relationship_id} 不存在"
            )
        
        logger.info(f"确认表关系成功: {relationship_id}")
        
        return {
            "success": True,
            "message": "确认成功"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("确认表关系失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"确认表关系失败: {str(e)}"
        )


@router.put("/relationships/{relationship_id}", response_model=TableRelationshipResponse)
async def update_relationship(
    relationship_id: UUID,
    relationship: TableRelationshipUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """更新表关系"""
    try:
        # 检查是否存在
        existing = await db.fetchrow(
            "SELECT 1 FROM table_relationships WHERE relationship_id = $1",
            relationship_id
        )
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"表关系 {relationship_id} 不存在"
            )
        
        # 构建更新语句
        updates = []
        params = []
        param_index = 1
        
        update_fields = relationship.dict(exclude_unset=True)
        
        for field, value in update_fields.items():
            updates.append(f"{field} = ${param_index}")
            params.append(value)
            param_index += 1
        
        if not updates:
            # 没有更新，返回当前数据
            return await db.fetchrow(
                "SELECT * FROM table_relationships WHERE relationship_id = $1",
                relationship_id
            )
        
        params.append(relationship_id)
        
        query = f"""
            UPDATE table_relationships
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE relationship_id = ${param_index}
            RETURNING relationship_id, connection_id, left_table_id, right_table_id,
                      left_column_id, right_column_id, relationship_type, join_type,
                      detection_method, confidence_score, is_confirmed,
                      relationship_name, description, detected_at
        """
        
        row = await db.fetchrow(query, *params)
        
        logger.info(f"更新表关系成功: {relationship_id}")
        
        return TableRelationshipResponse(
            relationship_id=row['relationship_id'],
            connection_id=row['connection_id'],
            left_table_id=row['left_table_id'],
            right_table_id=row['right_table_id'],
            left_column_id=row['left_column_id'],
            right_column_id=row['right_column_id'],
            relationship_type=row['relationship_type'],
            join_type=row['join_type'],
            detection_method=row['detection_method'],
            confidence_score=row['confidence_score'],
            is_confirmed=row['is_confirmed'],
            relationship_name=row['relationship_name'],
            description=row['description'],
            detected_at=row['detected_at']
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新表关系失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新表关系失败: {str(e)}"
        )


@router.delete("/relationships/{relationship_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_relationship(
    relationship_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """删除表关系"""
    try:
        result = await db.execute(
            "DELETE FROM table_relationships WHERE relationship_id = $1",
            relationship_id
        )
        
        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"表关系 {relationship_id} 不存在"
            )
        
        logger.info(f"删除表关系成功: {relationship_id}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除表关系失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除表关系失败: {str(e)}"
        )


@router.get("/relationships/{relationship_id}/preview-sql")
async def preview_relationship_sql(
    relationship_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    预览表关系的SQL JOIN语句
    """
    try:
        # 获取关系信息
        rel = await db.fetchrow("""
            SELECT 
                r.relationship_id, r.join_type,
                lt.schema_name as left_schema, lt.table_name as left_table,
                lc.column_name as left_column,
                rt.schema_name as right_schema, rt.table_name as right_table,
                rc.column_name as right_column
            FROM table_relationships r
            JOIN db_tables lt ON r.left_table_id = lt.table_id
            JOIN db_tables rt ON r.right_table_id = rt.table_id
            JOIN db_columns lc ON r.left_column_id = lc.column_id
            JOIN db_columns rc ON r.right_column_id = rc.column_id
            WHERE r.relationship_id = $1
        """, relationship_id)
        
        if not rel:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"表关系 {relationship_id} 不存在"
            )
        
        # 生成SQL
        left_full = f"{rel['left_schema']}.{rel['left_table']}"
        right_full = f"{rel['right_schema']}.{rel['right_table']}"
        
        sql = f"""
SELECT *
FROM {left_full} AS t1
{rel['join_type']} JOIN {right_full} AS t2
  ON t1.{rel['left_column']} = t2.{rel['right_column']}
        """.strip()
        
        return {
            "success": True,
            "sql": sql,
            "left_table": left_full,
            "right_table": right_full,
            "join_condition": f"t1.{rel['left_column']} = t2.{rel['right_column']}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("预览关系SQL失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"预览关系SQL失败: {str(e)}"
        )

