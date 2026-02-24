"""
查询历史保存
"""

import json
from typing import Dict, Any, Optional
from uuid import UUID
import structlog

logger = structlog.get_logger()


async def save_query_history(
    query_id: str,
    connection_id: Optional[str],
    user_id: Optional[str],
    original_question: str,
    generated_sql: Optional[str],
    execution_status: str,
    execution_time_ms: Optional[int],
    result_row_count: Optional[int],
    error_message: Optional[str],
    intent_detection_result: Optional[Dict[str, Any]]
):
    """保存查询历史到数据库（使用连接池）"""
    try:
        from server.utils.db_pool import get_metadata_pool
        pool = await get_metadata_pool()
        
        # 安全地转换UUID，处理空字符串和无效格式
        def safe_uuid(value: Optional[str]) -> Optional[UUID]:
            if not value:
                return None
            trimmed = value.strip()
            if not trimmed:
                return None
            if trimmed in {"public_user", "anonymous_user"}:
                return None
            try:
                return UUID(trimmed)
            except (ValueError, AttributeError, TypeError):
                # 放弃记录警告，避免前端匿名场景刷屏
                return None
        
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO query_history (
                    query_id, connection_id, user_id, original_question, generated_sql,
                    execution_status, execution_time_ms, result_row_count, error_message,
                    intent_detection_result, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
            """,
                safe_uuid(query_id),
                safe_uuid(connection_id),
                safe_uuid(user_id),
                original_question,
                generated_sql,
                execution_status,
                execution_time_ms,
                result_row_count,
                error_message,
                json.dumps(intent_detection_result) if intent_detection_result else None
            )
            logger.debug("查询历史已保存", query_id=query_id)
    except Exception as e:
        logger.warning("保存查询历史失败", error=str(e), query_id=query_id)
        # 不抛出异常，避免影响主流程

