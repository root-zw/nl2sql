"""系统监控API"""

import csv
import io
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
import structlog
import asyncpg

from server.models.admin import User
from server.models.database import UserRole
from server.api.admin.auth import require_role
from server.dependencies import get_query_cache, create_query_executor
from server.config import settings
from server.exceptions import ExecutionError, SecurityError
from server.utils.db_pool import get_metadata_pool

logger = structlog.get_logger()
router = APIRouter(prefix="/monitor", tags=["系统监控"])
require_admin = require_role(UserRole.ADMIN)


def _format_timestamp(value: Optional[datetime]) -> Optional[str]:
    """格式化时间戳"""
    if not value:
        return None
    return value.isoformat()


def _serialize_query_log_row(row: asyncpg.Record) -> Dict[str, Any]:
    """将查询日志记录序列化为前端可用结构"""
    data = dict(row)
    return {
        "query_id": str(data["query_id"]),
        "original_question": data.get("original_question") or "",
        "generated_sql": data.get("generated_sql") or "",
        "execution_status": data.get("execution_status"),
        "execution_time_ms": data.get("execution_time_ms"),
        "result_row_count": data.get("result_row_count"),
        "error_message": data.get("error_message"),
        "created_at": _format_timestamp(data.get("created_at")),
        "connection_id": str(data["connection_id"]) if data.get("connection_id") else None,
        "user_id": str(data["user_id"]) if data.get("user_id") else None,
        "user": {
            "user_id": str(data["user_id"]) if data.get("user_id") else None,
            "username": data.get("username"),
            "full_name": data.get("full_name")
        },
        "connection": {
            "connection_id": str(data["connection_id"]) if data.get("connection_id") else None,
            "connection_name": data.get("connection_name"),
            "db_type": data.get("db_type")
        }
    }


async def _build_result_preview(log_row: Dict[str, Any], preview_limit: int) -> Optional[Dict[str, Any]]:
    """为查询日志构建结果预览"""
    sql = (log_row.get("generated_sql") or "").strip()
    connection_id = log_row.get("connection_id")

    if not sql or not connection_id:
        return None

    normalized_sql = sql.lower()
    if not (normalized_sql.startswith("select") or normalized_sql.startswith("with")):
        return {"warning": "该SQL不是查询语句，无法预览"}

    try:
        executor = await create_query_executor(str(connection_id))
        result = await executor.execute_async(sql)
        rows = result.rows or []
        truncated_rows = rows[:preview_limit]
        meta = result.meta or {}

        preview_meta = {
            "latency_ms": meta.get("latency_ms"),
            "dialect": meta.get("dialect"),
            "row_count": len(rows),
            "limit": preview_limit,
            "truncated": len(rows) > preview_limit
        }

        return {
            "columns": result.columns,
            "rows": truncated_rows,
            "meta": preview_meta
        }
    except (ExecutionError, SecurityError) as e:
        return {"error": str(e)}
    except Exception as e:
        logger.warning("查询结果预览失败", error=str(e), query_id=str(log_row.get("query_id")))
        return {"error": f"获取结果预览失败: {str(e)}"}


def _build_query_log_filters(
    status: Optional[str],
    user_id: Optional[UUID],
    connection_id: Optional[UUID],
    keyword: Optional[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime]
) -> Tuple[str, List[Any]]:
    conditions: List[str] = ["1=1"]
    params: List[Any] = []

    if status:
        params.append(status)
        conditions.append(f"q.execution_status = ${len(params)}")

    if user_id:
        params.append(user_id)
        conditions.append(f"q.user_id = ${len(params)}")

    if connection_id:
        params.append(connection_id)
        conditions.append(f"q.connection_id = ${len(params)}")

    if keyword:
        params.append(f"%{keyword}%")
        conditions.append(
            f"(q.original_question ILIKE ${len(params)} OR q.generated_sql ILIKE ${len(params)})"
        )

    if start_time:
        params.append(start_time)
        conditions.append(f"q.created_at >= ${len(params)}")

    if end_time:
        params.append(end_time)
        conditions.append(f"q.created_at <= ${len(params)}")

    return " AND ".join(conditions), params


@router.get("/stats")
async def get_system_stats(
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """获取系统统计信息"""
    try:
        # 初始化统计数据
        stats = {
            "online_users": 1,  # 当前用户
            "today_queries": 0,
            "avg_response_time": 0,
            "success_rate": 100.0,
            "cache_hit_rate": 0.0,
            "active_connections": 0,
            "system_uptime": "0天 0小时",
            "last_updated": datetime.now().isoformat()
        }

        # 获取今日查询统计
        try:
            pool = await get_metadata_pool()
            
            async with pool.acquire() as conn:
                today = datetime.now().date()
                row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) as total_queries,
                        COUNT(CASE WHEN execution_status IN ('success', 'completed', 'ok') THEN 1 END) as success_queries,
                        AVG(CASE WHEN execution_time_ms IS NOT NULL THEN execution_time_ms END) as avg_response_time
                    FROM query_history
                    WHERE DATE(created_at) = $1
                    """,
                    today
                )

                if row:
                    stats["today_queries"] = row["total_queries"] or 0
                    stats["avg_response_time"] = int(row["avg_response_time"] or 0)

                    total = row["total_queries"] or 0
                    success = row["success_queries"] or 0
                    stats["success_rate"] = round((success / total * 100), 1) if total > 0 else 100.0

                # 统计最近 X 分钟内提交过查询的用户数量
                online_window = max(settings.monitor_online_window_minutes, 1)
                online_users = await conn.fetchval(
                    """
                    SELECT COALESCE(COUNT(DISTINCT user_id), 0)
                    FROM query_history
                    WHERE user_id IS NOT NULL
                      AND created_at >= NOW() - make_interval(mins => $1)
                    """,
                    online_window
                )
                stats["online_users"] = int(online_users or 0)

        except Exception as e:
            logger.warning("获取查询统计失败", error=str(e))

        # 获取缓存命中率（如果启用缓存）
        try:
            cache = get_query_cache()
            if cache:
                # 这里简化处理，实际应该从缓存系统获取统计
                stats["cache_hit_rate"] = 85.0  # 模拟数据
        except Exception as e:
            logger.warning("获取缓存统计失败", error=str(e))

        # 获取活跃连接数（简化处理）
        try:
            from server.exec.connection import get_connection_manager
            conn_mgr = get_connection_manager()
            pool_metrics = conn_mgr.get_pool_metrics() or {}
            stats["active_connections"] = pool_metrics.get("checked_out", 0)
        except Exception as e:
            logger.warning("获取连接统计失败", error=str(e))

        # 计算系统运行时间
        try:
            import time
            start_time = getattr(get_system_stats, '_start_time', None)
            if start_time is None:
                get_system_stats._start_time = time.time()
                start_time = get_system_stats._start_time

            uptime_seconds = int(time.time() - start_time)
            days = uptime_seconds // 86400
            hours = (uptime_seconds % 86400) // 3600
            stats["system_uptime"] = f"{days}天 {hours}小时"
        except Exception as e:
            logger.warning("计算运行时间失败", error=str(e))

        try:
            stats["model_info"] = {
                "llm_provider": settings.llm_provider,
                "llm_model": settings.llm_model,
                "llm_base_url": settings.nl2sql_base_url,
                "embedding_model": settings.embedding_model,
                "embedding_base_url": settings.embedding_base_url
            }
        except Exception as e:
            logger.warning("获取模型信息失败", error=str(e))

        return stats

    except Exception as e:
        logger.error("获取系统统计失败", error=str(e))
        # 返回默认统计数据
        return {
            "online_users": 1,
            "today_queries": 0,
            "avg_response_time": 0,
            "success_rate": 100.0,
            "cache_hit_rate": 0.0,
            "active_connections": 0,
            "system_uptime": "0天 0小时",
            "last_updated": datetime.now().isoformat()
        }


@router.get("/recent-queries")
async def get_recent_queries(
    limit: int = 10,
    current_user: dict = Depends(require_admin)
) -> List[Dict[str, Any]]:
    """获取最近查询记录"""
    try:
        from server.utils.db_pool import get_metadata_pool
        pool = await get_metadata_pool()
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    query_id,
                    original_question,
                    generated_sql,
                    execution_status,
                    execution_time_ms,
                    error_message,
                    created_at
                FROM query_history
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit
            )

            results = []
            for row in rows:
                results.append({
                    "query_id": str(row["query_id"]),
                    "original_question": row["original_question"] or "",
                    "nl_query": row["original_question"] or "",  # 兼容前端字段名
                    "generated_sql": row["generated_sql"] or "",
                    "execution_status": row["execution_status"],
                    "status": row["execution_status"],  # 兼容前端字段名
                    "execution_time_ms": row["execution_time_ms"] or 0,
                    "error_message": row["error_message"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None
                })

            return results

    except Exception as e:
        logger.error("获取最近查询失败", error=str(e))
        return []


@router.get("/query-logs")
async def list_query_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None, description="执行状态筛选"),
    user_id: Optional[UUID] = Query(None, description="用户筛选"),
    connection_id: Optional[UUID] = Query(None, description="数据库筛选"),
    keyword: Optional[str] = Query(None, description="模糊搜索问题或SQL"),
    start_time: Optional[datetime] = Query(None, description="开始时间"),
    end_time: Optional[datetime] = Query(None, description="结束时间"),
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """查询日志列表"""
    try:
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            where_clause, params = _build_query_log_filters(
                status, user_id, connection_id, keyword, start_time, end_time
            )

            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM query_history q WHERE {where_clause}",
                *params
            ) or 0

            offset = (page - 1) * page_size
            params_with_pagination = params + [page_size, offset]

            query = f"""
                SELECT
                    q.query_id,
                    q.original_question,
                    q.generated_sql,
                    q.execution_status,
                    q.execution_time_ms,
                    q.result_row_count,
                    q.error_message,
                    q.created_at,
                    q.connection_id,
                    q.user_id,
                    u.username,
                    u.full_name,
                    dc.connection_name,
                    dc.db_type
                FROM query_history q
                LEFT JOIN users u ON q.user_id = u.user_id
                LEFT JOIN database_connections dc ON q.connection_id = dc.connection_id
                WHERE {where_clause}
                ORDER BY q.created_at DESC
                LIMIT ${len(params_with_pagination) - 1}
                OFFSET ${len(params_with_pagination)}
            """

            rows = await conn.fetch(query, *params_with_pagination)
            items = [_serialize_query_log_row(row) for row in rows]

            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    except Exception as e:
        logger.error("获取查询日志失败", error=str(e))
        raise HTTPException(status_code=500, detail=f"获取查询日志失败: {str(e)}")


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


@router.get("/query-logs/export")
async def export_query_logs(
    request: Request,
    current_user: dict = Depends(require_admin)
):
    """导出查询日志 CSV"""
    try:
        params = request.query_params
        status = params.get("status")
        user_id = params.get("user_id")
        connection_id = params.get("connection_id")
        keyword = params.get("keyword")
        start_time = _parse_datetime(params.get("start_time"))
        end_time = _parse_datetime(params.get("end_time"))

        try:
            user_uuid = UUID(user_id) if user_id else None
            connection_uuid = UUID(connection_id) if connection_id else None
        except ValueError:
            raise HTTPException(status_code=400, detail="无效的筛选参数")

        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            where_clause, params = _build_query_log_filters(
                status, user_uuid, connection_uuid, keyword, start_time, end_time
            )

            query = f"""
                SELECT
                    q.query_id,
                    q.original_question,
                    q.generated_sql,
                    q.execution_status,
                    q.execution_time_ms,
                    q.result_row_count,
                    q.error_message,
                    q.created_at,
                    q.connection_id,
                    q.user_id,
                    u.username,
                    u.full_name,
                    dc.connection_name,
                    dc.db_type
                FROM query_history q
                LEFT JOIN users u ON q.user_id = u.user_id
                LEFT JOIN database_connections dc ON q.connection_id = dc.connection_id
                WHERE {where_clause}
                ORDER BY q.created_at DESC
            """

            rows = await conn.fetch(query, *params)

            output = io.StringIO()
            output.write('\ufeff')  # BOM for Excel-friendly UTF-8
            writer = csv.writer(output)
            writer.writerow([
                "查询时间",
                "用户",
                "数据库",
                "问题",
                "SQL",
                "状态",
                "耗时(ms)"
            ])

            for row in rows:
                serialized = _serialize_query_log_row(row)
                writer.writerow([
                    serialized.get("created_at") or "-",
                    serialized.get("user", {}).get("username") or "-",
                    serialized.get("connection", {}).get("connection_name") or "-",
                    serialized.get("original_question") or "-",
                    serialized.get("generated_sql") or "-",
                    serialized.get("execution_status") or "-",
                    serialized.get("execution_time_ms") or "-"
                ])

            output.seek(0)
            filename = f"query_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            headers = {
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers=headers
            )
    except Exception as e:
        logger.error("导出查询日志失败", error=str(e))
        raise HTTPException(status_code=500, detail=f"导出查询日志失败: {str(e)}")


@router.get("/query-logs/{query_id}")
async def get_query_log_detail(
    query_id: UUID,
    include_result: bool = Query(True, description="是否返回结果预览"),
    preview_limit: Optional[int] = Query(None, ge=1, le=500, description="结果预览行数"),
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """查询日志详情"""
    try:
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    q.*,
                    u.username,
                    u.full_name,
                    u.email,
                    dc.connection_name,
                    dc.db_type
                FROM query_history q
                LEFT JOIN users u ON q.user_id = u.user_id
                LEFT JOIN database_connections dc ON q.connection_id = dc.connection_id
                WHERE q.query_id = $1
                """,
                query_id
            )

            if not row:
                raise HTTPException(status_code=404, detail="查询记录不存在")

            row_dict = dict(row)
            serialized = _serialize_query_log_row(row)
            serialized["intent_detection_result"] = row_dict.get("intent_detection_result")
            serialized["error_message"] = row_dict.get("error_message")
            serialized["generated_sql"] = row_dict.get("generated_sql") or ""

            intent_data = serialized["intent_detection_result"]
            if isinstance(intent_data, str):
                try:
                    serialized["intent_detection_result"] = json.loads(intent_data)
                except Exception:
                    pass

            if include_result:
                limit = preview_limit or settings.monitor_query_preview_limit
                preview = await _build_result_preview(row_dict, limit)
                serialized["result_preview"] = preview
            else:
                serialized["result_preview"] = None

            return serialized
    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取查询日志详情失败", error=str(e), query_id=str(query_id))
        raise HTTPException(status_code=500, detail=f"获取查询日志详情失败: {str(e)}")


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """系统健康检查"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {}
        }

        # 检查数据库连接
        try:
            from server.utils.db_pool import get_metadata_pool
            pool = await get_metadata_pool()
            
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                # 获取PostgreSQL连接数
                pg_conn_count = await conn.fetchval("""
                    SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
                """)
                health_status["services"]["PostgreSQL"] = {
                    "status": "healthy",
                    "connections": pg_conn_count or 0
                }
        except Exception as e:
            health_status["services"]["PostgreSQL"] = {
                "status": "error",
                "connections": 0,
                "error": str(e)
            }
            health_status["status"] = "unhealthy"

        # 检查Milvus连接
        try:
            from server.api.admin.milvus import get_milvus_client
            milvus_client = await get_milvus_client()
            if milvus_client:
                # 尝试获取Milvus连接信息
                health_status["services"]["Milvus"] = {
                    "status": "healthy",
                    "connections": 2  # 简化处理，实际应该从Milvus获取
                }
            else:
                health_status["services"]["Milvus"] = {
                    "status": "disabled",
                    "connections": 0
                }
        except Exception as e:
            health_status["services"]["Milvus"] = {
                "status": "error",
                "connections": 0,
                "error": str(e)
            }

        # 检查Redis连接
        try:
            cache = get_query_cache()
            if cache and hasattr(cache, 'client') and cache.client:
                # 尝试ping Redis
                try:
                    await cache.client.ping()
                    health_status["services"]["Redis"] = {
                        "status": "healthy",
                        "connections": 3  # 简化处理
                    }
                except:
                    health_status["services"]["Redis"] = {
                        "status": "error",
                        "connections": 0
                    }
            else:
                health_status["services"]["Redis"] = {
                    "status": "disabled",
                    "connections": 0
                }
        except Exception as e:
            health_status["services"]["Redis"] = {
                "status": "error",
                "connections": 0,
                "error": str(e)
            }

        return health_status

    except Exception as e:
        logger.error("健康检查失败", error=str(e))
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }