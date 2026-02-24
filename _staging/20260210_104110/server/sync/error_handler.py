"""
自动同步系统错误处理和重试机制
"""

import asyncio
import json
import structlog
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
from dataclasses import dataclass, asdict
import traceback

import asyncpg

from server.models.sync import SyncStatus, SyncHistory, PendingChange
from server.config import settings

logger = structlog.get_logger()


class ErrorType(str, Enum):
    """错误类型"""
    NETWORK_ERROR = "network_error"           # 网络连接错误
    DATABASE_ERROR = "database_error"         # 数据库操作错误
    MILVUS_ERROR = "milvus_error"             # Milvus操作错误
    EMBEDDING_ERROR = "embedding_error"       # 向量化错误
    VALIDATION_ERROR = "validation_error"     # 数据验证错误
    CONCURRENCY_ERROR = "concurrency_error"   # 并发冲突错误
    RESOURCE_ERROR = "resource_error"         # 资源不足错误
    TIMEOUT_ERROR = "timeout_error"           # 超时错误
    UNKNOWN_ERROR = "unknown_error"           # 未知错误


class ErrorSeverity(str, Enum):
    """错误严重程度"""
    LOW = "low"           # 低：轻微错误，可以重试
    MEDIUM = "medium"     # 中：影响部分功能，需要关注
    HIGH = "high"         # 高：严重影响，需要立即处理
    CRITICAL = "critical" # 严重：系统故障，需要紧急处理


class RetryStrategy(str, Enum):
    """重试策略"""
    IMMEDIATE = "immediate"     # 立即重试
    EXPONENTIAL_BACKOFF = "exponential_backoff"  # 指数退避
    FIXED_DELAY = "fixed_delay" # 固定延迟
    LINEAR_BACKOFF = "linear_backoff"  # 线性退避


@dataclass
class ErrorInfo:
    """错误信息"""
    error_id: str
    error_type: ErrorType
    severity: ErrorSeverity
    error_message: str
    error_details: Dict[str, Any]
    stack_trace: Optional[str]
    timestamp: datetime
    context: Dict[str, Any]  # 上下文信息（sync_id, connection_id等）
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: Optional[datetime] = None
    resolved: bool = False


@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay: float = 1.0  # 基础延迟秒数
    max_delay: float = 300.0  # 最大延迟秒数
    backoff_multiplier: float = 2.0  # 退避倍数
    jitter: bool = True  # 是否添加随机抖动


class SyncErrorHandler:
    """同步错误处理器"""

    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        self.retry_queue = asyncio.Queue()
        self.active_retries: Dict[str, asyncio.Task] = {}
        self.error_handlers: Dict[ErrorType, Callable] = {
            ErrorType.NETWORK_ERROR: self._handle_network_error,
            ErrorType.DATABASE_ERROR: self._handle_database_error,
            ErrorType.MILVUS_ERROR: self._handle_milvus_error,
            ErrorType.EMBEDDING_ERROR: self._handle_embedding_error,
            ErrorType.VALIDATION_ERROR: self._handle_validation_error,
            ErrorType.CONCURRENCY_ERROR: self._handle_concurrency_error,
            ErrorType.RESOURCE_ERROR: self._handle_resource_error,
            ErrorType.TIMEOUT_ERROR: self._handle_timeout_error,
            ErrorType.UNKNOWN_ERROR: self._handle_unknown_error,
        }

    async def handle_sync_error(self, error: Exception, context: Dict[str, Any],
                              retry_config: Optional[RetryConfig] = None) -> ErrorInfo:
        """
        处理同步错误

        Args:
            error: 异常对象
            context: 上下文信息（sync_id, connection_id等）
            retry_config: 重试配置

        Returns:
            ErrorInfo: 错误信息对象
        """
        # 分析错误类型和严重程度
        error_type, severity = self._classify_error(error)

        # 创建错误信息
        error_info = ErrorInfo(
            error_id=self._generate_error_id(),
            error_type=error_type,
            severity=severity,
            error_message=str(error),
            error_details=self._extract_error_details(error),
            stack_trace=traceback.format_exc(),
            timestamp=datetime.utcnow(),
            context=context,
            max_retries=retry_config.max_retries if retry_config else 3
        )

        # 记录错误到数据库
        await self._record_error(error_info)

        # 根据错误类型和严重程度决定是否重试
        if self._should_retry(error_info) and retry_config:
            await self._schedule_retry(error_info, retry_config)

        # 发送错误通知
        await self._notify_error(error_info)

        logger.error(
            "同步错误处理完成",
            error_id=error_info.error_id,
            error_type=error_type.value,
            severity=severity.value,
            sync_id=context.get('sync_id'),
            connection_id=context.get('connection_id')
        )

        return error_info

    def _classify_error(self, error: Exception) -> tuple[ErrorType, ErrorSeverity]:
        """分析错误类型和严重程度"""
        error_message = str(error).lower()
        error_type_name = error.__class__.__name__.lower()

        # 网络相关错误
        if any(keyword in error_message for keyword in ['connection', 'network', 'timeout', 'socket']):
            if 'timeout' in error_message:
                return ErrorType.TIMEOUT_ERROR, ErrorSeverity.MEDIUM
            return ErrorType.NETWORK_ERROR, ErrorSeverity.MEDIUM

        # 数据库相关错误
        if any(keyword in error_message or error_type_name for keyword in ['database', 'sql', 'postgresql', 'asyncpg']):
            return ErrorType.DATABASE_ERROR, ErrorSeverity.HIGH

        # Milvus相关错误
        if any(keyword in error_message or error_type_name for keyword in ['milvus', 'vector', 'collection']):
            return ErrorType.MILVUS_ERROR, ErrorSeverity.HIGH

        # 向量化相关错误
        if any(keyword in error_message or error_type_name for keyword in ['embedding', 'vectorize', 'openai']):
            return ErrorType.EMBEDDING_ERROR, ErrorSeverity.MEDIUM

        # 并发相关错误
        if any(keyword in error_message for keyword in ['concurrency', 'lock', 'conflict']):
            return ErrorType.CONCURRENCY_ERROR, ErrorSeverity.MEDIUM

        # 资源相关错误
        if any(keyword in error_message for keyword in ['memory', 'disk', 'resource', 'limit']):
            return ErrorType.RESOURCE_ERROR, ErrorSeverity.HIGH

        # 验证相关错误
        if any(keyword in error_message for keyword in ['validation', 'invalid', 'format']):
            return ErrorType.VALIDATION_ERROR, ErrorSeverity.LOW

        # 未知错误
        return ErrorType.UNKNOWN_ERROR, ErrorSeverity.MEDIUM

    def _extract_error_details(self, error: Exception) -> Dict[str, Any]:
        """提取错误详细信息"""
        return {
            "error_class": error.__class__.__name__,
            "error_module": error.__class__.__module__,
            "error_args": list(error.args) if hasattr(error, 'args') else [],
            "has_cause": hasattr(error, '__cause__') and error.__cause__ is not None,
        }

    def _should_retry(self, error_info: ErrorInfo) -> bool:
        """判断是否应该重试"""
        # 检查重试次数
        if error_info.retry_count >= error_info.max_retries:
            return False

        # 检查错误类型
        non_retryable_types = {
            ErrorType.VALIDATION_ERROR,  # 数据验证错误不会自动修复
        }

        if error_info.error_type in non_retryable_types:
            return False

        # 检查严重程度
        if error_info.severity == ErrorSeverity.CRITICAL:
            return False

        return True

    async def _schedule_retry(self, error_info: ErrorInfo, retry_config: RetryConfig):
        """安排重试"""
        # 计算下次重试时间
        delay = self._calculate_retry_delay(error_info.retry_count, retry_config)
        next_retry_at = datetime.utcnow() + timedelta(seconds=delay)

        error_info.next_retry_at = next_retry_at
        error_info.retry_count += 1

        # 添加到重试队列
        await self.retry_queue.put((error_info, retry_config))

        logger.debug(
            "安排错误重试",
            error_id=error_info.error_id,
            retry_count=error_info.retry_count,
            next_retry_at=next_retry_at.isoformat(),
            delay_seconds=delay
        )

    def _calculate_retry_delay(self, retry_count: int, config: RetryConfig) -> float:
        """计算重试延迟"""
        if config.strategy == RetryStrategy.IMMEDIATE:
            return 0.1  # 立即重试，但给一点时间让系统恢复

        elif config.strategy == RetryStrategy.FIXED_DELAY:
            delay = config.base_delay

        elif config.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = config.base_delay * (retry_count + 1)

        elif config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = config.base_delay * (config.backoff_multiplier ** retry_count)

        else:
            delay = config.base_delay

        # 限制最大延迟
        delay = min(delay, config.max_delay)

        # 添加随机抖动以避免雷群效应
        if config.jitter:
            import random
            jitter_range = delay * 0.1
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    async def _record_error(self, error_info: ErrorInfo):
        """记录错误到数据库"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO sync_errors (
                        error_id, error_type, severity, error_message, error_details,
                        stack_trace, timestamp, context, retry_count, max_retries,
                        next_retry_at, resolved
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                    error_info.error_id,
                    error_info.error_type.value,
                    error_info.severity.value,
                    error_info.error_message,
                    json.dumps(error_info.error_details),
                    error_info.stack_trace,
                    error_info.timestamp,
                    json.dumps(error_info.context),
                    error_info.retry_count,
                    error_info.max_retries,
                    error_info.next_retry_at,
                    error_info.resolved
                )
        except Exception as e:
            logger.error("记录错误信息失败", error=str(e), error_id=error_info.error_id)

    async def _notify_error(self, error_info: ErrorInfo):
        """发送错误通知"""
        try:
            # WebSocket通知
            from server.websocket_manager import sync_event_broadcaster
            await sync_event_broadcaster.broadcast_error(
                error_info.context.get('connection_id', ''),
                {
                    "error_id": error_info.error_id,
                    "error_type": error_info.error_type.value,
                    "severity": error_info.severity.value,
                    "message": error_info.error_message,
                    "retry_count": error_info.retry_count,
                    "max_retries": error_info.max_retries
                }
            )
        except Exception as e:
            logger.warning("发送错误通知失败", error=str(e), error_id=error_info.error_id)

    def _generate_error_id(self) -> str:
        """生成错误ID"""
        import uuid
        return str(uuid.uuid4())

    async def start_retry_processor(self):
        """启动重试处理器"""
        asyncio.create_task(self._process_retry_queue())
        logger.info("重试处理器已启动")

    async def _process_retry_queue(self):
        """处理重试队列"""
        while True:
            try:
                # 等待重试任务
                error_info, retry_config = await self.retry_queue.get()

                # 等待到重试时间
                now = datetime.utcnow()
                if error_info.next_retry_at > now:
                    delay = (error_info.next_retry_at - now).total_seconds()
                    await asyncio.sleep(delay)

                # 执行重试
                retry_task = asyncio.create_task(
                    self._execute_retry(error_info, retry_config)
                )
                self.active_retries[error_info.error_id] = retry_task

                await retry_task

                # 清理完成的任务
                if error_info.error_id in self.active_retries:
                    del self.active_retries[error_info.error_id]

            except Exception as e:
                logger.error("重试处理器异常", error=str(e))
                await asyncio.sleep(5)  # 短暂休息后继续

    async def _execute_retry(self, error_info: ErrorInfo, retry_config: RetryConfig):
        """执行重试"""
        try:
            logger.debug(
                "开始执行重试",
                error_id=error_info.error_id,
                retry_count=error_info.retry_count
            )

            # 根据错误类型调用相应的处理函数
            handler = self.error_handlers.get(error_info.error_type, self._handle_unknown_error)
            success = await handler(error_info)

            if success:
                # 重试成功，标记错误为已解决
                await self._mark_error_resolved(error_info.error_id)
                logger.debug(
                    "重试成功",
                    error_id=error_info.error_id,
                    retry_count=error_info.retry_count
                )
            else:
                # 重试失败，安排下次重试
                if self._should_retry(error_info):
                    await self._schedule_retry(error_info, retry_config)
                else:
                    # 不再重试，标记为失败
                    await self._mark_error_failed(error_info.error_id)
                    logger.error(
                        "重试失败，不再重试",
                        error_id=error_info.error_id,
                        retry_count=error_info.retry_count
                    )

        except Exception as e:
            logger.error(
                "重试执行异常",
                error=str(e),
                error_id=error_info.error_id
            )
            # 异常也安排重试
            if self._should_retry(error_info):
                await self._schedule_retry(error_info, retry_config)

    # 具体错误处理函数
    async def _handle_network_error(self, error_info: ErrorInfo) -> bool:
        """处理网络错误"""
        # 尝试重新建立连接
        connection_id = error_info.context.get('connection_id')
        if connection_id:
            try:
                # 这里可以实现连接重建逻辑
                logger.debug("尝试重新建立网络连接", connection_id=connection_id)
                await asyncio.sleep(2)  # 等待连接重建
                return True
            except Exception as e:
                logger.error("重新建立连接失败", error=str(e))
        return False

    async def _handle_database_error(self, error_info: ErrorInfo) -> bool:
        """处理数据库错误"""
        # 检查连接池状态
        try:
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error("数据库连接检查失败", error=str(e))
            return False

    async def _handle_milvus_error(self, error_info: ErrorInfo) -> bool:
        """处理Milvus错误"""
        # 检查Milvus连接状态
        try:
            from server.api.admin.milvus import get_milvus_client
            milvus_client = await get_milvus_client()
            if milvus_client:
                # 这里可以实现Milvus健康检查
                return True
        except Exception as e:
            logger.error("Milvus连接检查失败", error=str(e))
        return False

    async def _handle_embedding_error(self, error_info: ErrorInfo) -> bool:
        """处理向量化错误"""
        # 向量化错误通常是暂时的，直接返回True重试
        await asyncio.sleep(1)
        return True

    async def _handle_validation_error(self, error_info: ErrorInfo) -> bool:
        """处理验证错误"""
        # 验证错误通常不会自动修复，返回False
        return False

    async def _handle_concurrency_error(self, error_info: ErrorInfo) -> bool:
        """处理并发错误"""
        # 等待一段时间后重试
        await asyncio.sleep(5)
        return True

    async def _handle_resource_error(self, error_info: ErrorInfo) -> bool:
        """处理资源错误"""
        # 等待资源释放
        await asyncio.sleep(10)
        return True

    async def _handle_timeout_error(self, error_info: ErrorInfo) -> bool:
        """处理超时错误"""
        # 增加超时时间后重试
        await asyncio.sleep(5)
        return True

    async def _handle_unknown_error(self, error_info: ErrorInfo) -> bool:
        """处理未知错误"""
        # 未知错误采用保守策略，等待后重试
        await asyncio.sleep(3)
        return True

    async def _mark_error_resolved(self, error_id: str):
        """标记错误为已解决"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE sync_errors
                    SET resolved = true, resolved_at = CURRENT_TIMESTAMP
                    WHERE error_id = $1
                """, error_id)
        except Exception as e:
            logger.error("标记错误为已解决失败", error=str(e), error_id=error_id)

    async def _mark_error_failed(self, error_id: str):
        """标记错误为失败"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE sync_errors
                    SET resolved = false, failed_at = CURRENT_TIMESTAMP
                    WHERE error_id = $1
                """, error_id)
        except Exception as e:
            logger.error("标记错误为失败失败", error=str(e), error_id=error_id)

    async def get_error_statistics(self, connection_id: Optional[str] = None,
                                  days: int = 7) -> Dict[str, Any]:
        """获取错误统计信息"""
        try:
            async with self.db_pool.acquire() as conn:
                since_date = datetime.utcnow() - timedelta(days=days)

                query = """
                    SELECT
                        error_type,
                        severity,
                        COUNT(*) as count,
                        AVG(retry_count) as avg_retries,
                        COUNT(CASE WHEN resolved = true THEN 1 END) as resolved_count
                    FROM sync_errors
                    WHERE timestamp >= $1
                    AND ($2::uuid IS NULL OR context->>'connection_id' = $2::text)
                    GROUP BY error_type, severity
                    ORDER BY count DESC
                """

                results = await conn.fetch(query, since_date, connection_id)

                stats = {
                    "total_errors": sum(row['count'] for row in results),
                    "resolved_errors": sum(row['resolved_count'] for row in results),
                    "error_breakdown": []
                }

                for row in results:
                    stats["error_breakdown"].append({
                        "error_type": row['error_type'],
                        "severity": row['severity'],
                        "count": row['count'],
                        "resolved_count": row['resolved_count'],
                        "avg_retries": float(row['avg_retries']) if row['avg_retries'] else 0,
                        "resolution_rate": row['resolved_count'] / row['count'] if row['count'] > 0 else 0
                    })

                return stats

        except Exception as e:
            logger.error("获取错误统计失败", error=str(e))
            return {"error": str(e)}


# 全局错误处理器实例
error_handler: Optional[SyncErrorHandler] = None


def get_error_handler() -> Optional[SyncErrorHandler]:
    """获取全局错误处理器实例"""
    return error_handler


def set_error_handler(handler: SyncErrorHandler):
    """设置全局错误处理器实例"""
    global error_handler
    error_handler = handler