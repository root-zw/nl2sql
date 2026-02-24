"""配置历史查询API"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
import structlog
import json

from server.models.admin import User
from server.middleware.auth import require_data_admin
from server.config import settings
import asyncpg

logger = structlog.get_logger()
router = APIRouter(prefix="/history", tags=["配置历史"])


@router.get("")
async def list_history(
    table_name: Optional[str] = Query(None, description="表名筛选"),
    record_id: Optional[str] = Query(None, description="记录ID筛选"),
    operation: Optional[str] = Query(None, description="操作类型筛选: INSERT, UPDATE, DELETE"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: User = Depends(require_data_admin)
):
    """获取配置变更历史
    
    支持分页和多种筛选条件
    """
    try:
        conn = await asyncpg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )

        try:
            # 构建查询
            query = """
                SELECT change_id, table_name, record_id, operation,
                       old_snapshot, new_snapshot, changed_by, change_reason,
                       changed_at, diff
                FROM metadata_change_log
                WHERE 1=1
            """
            params = []

            # 添加表名筛选
            if table_name:
                query += " AND table_name = $%d" % (len(params) + 1)
                params.append(table_name)

            # 添加记录ID筛选
            if record_id:
                query += " AND record_id = $%d" % (len(params) + 1)
                params.append(record_id)

            # 添加操作类型筛选
            if operation:
                query += " AND operation = $%d" % (len(params) + 1)
                params.append(operation.upper())

            # 添加排序和分页
            query += " ORDER BY changed_at DESC LIMIT $%d OFFSET $%d" % (
                len(params) + 1, len(params) + 2
            )
            params.extend([page_size, (page - 1) * page_size])

            rows = await conn.fetch(query, *params)

            results = []
            for row in rows:
                item = dict(row)
                # 解析JSONB字段
                for field in ['old_snapshot', 'new_snapshot', 'diff']:
                    if item.get(field) and isinstance(item[field], str):
                        try:
                            item[field] = json.loads(item[field])
                        except:
                            pass
                results.append(item)

            return results
        finally:
            await conn.close()

    except Exception as e:
        logger.error("获取配置历史失败", error=str(e))
        raise HTTPException(500, f"获取配置历史失败: {str(e)}")


@router.get("/{history_id}")
async def get_history_detail(
    history_id: str,
    current_user: User = Depends(require_data_admin)
):
    """获取配置历史详情"""
    try:
        conn = await asyncpg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )

        try:
            row = await conn.fetchrow(
                """
                SELECT * FROM metadata_change_log
                WHERE change_id = $1
                """,
                history_id
            )

            if not row:
                raise HTTPException(404, f"历史记录不存在: {history_id}")

            # 解析JSONB字段
            history_dict = dict(row)
            for field in ['old_snapshot', 'new_snapshot', 'diff']:
                if history_dict.get(field) and isinstance(history_dict[field], str):
                    try:
                        history_dict[field] = json.loads(history_dict[field])
                    except:
                        pass

            return history_dict
        finally:
            await conn.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取配置历史详情失败", error=str(e))
        raise HTTPException(500, f"获取配置历史详情失败: {str(e)}")


@router.get("/compare/{table_name}/{record_id}")
async def compare_versions(
    table_name: str,
    record_id: str,
    version1: int = Query(..., description="版本1"),
    version2: int = Query(..., description="版本2"),
    current_user: User = Depends(require_data_admin)
):
    """比较两个版本的差异"""
    try:
        conn = await asyncpg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )

        try:
            # 获取两个版本（简化实现：获取最近的两条变更记录）
            rows = await conn.fetch(
                """
                SELECT change_id, new_snapshot, changed_at, changed_by
                FROM metadata_change_log
                WHERE table_name = $1 AND record_id = $2
                ORDER BY changed_at DESC
                LIMIT 2
                """,
                table_name, record_id
            )

            if len(rows) < 2:
                raise HTTPException(404, "指定的版本不存在")

            result = {
                "table_name": table_name,
                "record_id": record_id,
                "version1": {
                    "change_id": str(rows[1]['change_id']),
                    "data": json.loads(rows[1]['new_snapshot']) if isinstance(rows[1]['new_snapshot'], str) else rows[1]['new_snapshot'],
                    "changed_at": rows[1]['changed_at'],
                    "changed_by": rows[1]['changed_by']
                },
                "version2": {
                    "change_id": str(rows[0]['change_id']),
                    "data": json.loads(rows[0]['new_snapshot']) if isinstance(rows[0]['new_snapshot'], str) else rows[0]['new_snapshot'],
                    "changed_at": rows[0]['changed_at'],
                    "changed_by": rows[0]['changed_by']
                }
            }

            return result
        finally:
            await conn.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("版本比较失败", error=str(e))
        raise HTTPException(500, f"版本比较失败: {str(e)}")

