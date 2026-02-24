"""
自动同步系统监控指标收集器
"""

import asyncio
import time
import structlog
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from uuid import UUID
from dataclasses import dataclass, asdict
import json

import asyncpg

from server.config import settings

logger = structlog.get_logger()


@dataclass
class SyncMetrics:
    """同步指标数据"""
    timestamp: datetime
    connection_id: str

    # 同步计数指标
    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    running_syncs: int = 0

    # 性能指标
    avg_sync_duration: float = 0.0  # 平均同步时间（秒）
    max_sync_duration: float = 0.0  # 最大同步时间（秒）
    min_sync_duration: float = 0.0  # 最小同步时间（秒）

    # 数据量指标
    total_entities_synced: int = 0
    avg_entities_per_sync: float = 0.0

    # 健康指标
    health_score: float = 1.0  # 0-1
    error_rate: float = 0.0    # 错误率

    # 待同步变更指标
    pending_changes_count: int = 0
    oldest_pending_change_age: float = 0.0  # 最老的待同步变更年龄（小时）

    # 系统资源指标
    cpu_usage: float = 0.0     # CPU使用率
    memory_usage: float = 0.0   # 内存使用率
    redis_connections: int = 0  # Redis连接数


@dataclass
class SystemMetrics:
    """系统级指标"""
    timestamp: datetime

    # 连接统计
    total_connections: int = 0
    active_connections: int = 0

    # 同步统计
    total_syncs_last_hour: int = 0
    total_syncs_last_day: int = 0
    successful_syncs_last_hour: int = 0
    failed_syncs_last_hour: int = 0

    # 错误统计
    total_errors_last_hour: int = 0
    critical_errors_last_hour: int = 0
    error_rate_last_hour: float = 0.0

    # 性能统计
    avg_response_time: float = 0.0
    p95_response_time: float = 0.0
    p99_response_time: float = 0.0

    # 队列统计
    pending_sync_queue_size: int = 0
    retry_queue_size: int = 0


class MetricsCollector:
    """监控指标收集器"""

    def __init__(self, db_pool: asyncpg.Pool, redis_client=None):
        self.db_pool = db_pool
        self.redis_client = redis_client
        self.metrics_history: List[SyncMetrics] = []
        self.system_metrics_history: List[SystemMetrics] = []
        self.collection_interval = 60  # 60秒收集一次
        self.max_history_size = 1440   # 保留24小时的数据（60秒间隔）

    async def start_collection(self):
        """开始指标收集"""
        logger.info("启动指标收集器", interval=self.collection_interval)
        asyncio.create_task(self._collection_loop())

    async def _collection_loop(self):
        """指标收集循环"""
        while True:
            try:
                await self._collect_metrics()
                await asyncio.sleep(self.collection_interval)
            except Exception as e:
                logger.error("指标收集异常", error=str(e))
                await asyncio.sleep(10)  # 出错后短暂休息

    async def _collect_metrics(self):
        """收集所有指标"""
        current_time = datetime.utcnow()

        # 收集连接级指标
        connection_metrics = await self._collect_connection_metrics(current_time)

        # 收集系统级指标
        system_metrics = await self._collect_system_metrics(current_time, connection_metrics)

        # 存储到历史记录
        self.metrics_history.extend(connection_metrics)
        self.system_metrics_history.append(system_metrics)

        # 限制历史记录大小
        if len(self.metrics_history) > self.max_history_size:
            self.metrics_history = self.metrics_history[-self.max_history_size:]
        if len(self.system_metrics_history) > self.max_history_size:
            self.system_metrics_history = self.system_metrics_history[-self.max_history_size:]

        # 发送到监控系统（这里可以实现Prometheus等集成）
        await self._send_to_monitoring_system(system_metrics, connection_metrics)

    async def _collect_connection_metrics(self, timestamp: datetime) -> List[SyncMetrics]:
        """收集连接级指标"""
        try:
            # 获取所有活跃连接
            async with self.db_pool.acquire() as conn:
                connections = await conn.fetch("""
                    SELECT DISTINCT connection_id
                    FROM database_connections
                    WHERE is_active = true
                """)

            connection_metrics = []

            for conn_record in connections:
                connection_id = str(conn_record['connection_id'])
                metrics = await self._collect_single_connection_metrics(timestamp, connection_id)
                connection_metrics.append(metrics)

            return connection_metrics

        except Exception as e:
            logger.error("收集连接指标失败", error=str(e))
            return []

    async def _collect_single_connection_metrics(self, timestamp: datetime, connection_id: str) -> SyncMetrics:
        """收集单个连接的指标"""
        try:
            async with self.db_pool.acquire() as conn:
                # 同步统计
                sync_stats = await conn.fetchrow("""
                    SELECT
                        COUNT(*) as total_syncs,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_syncs,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_syncs,
                        COUNT(CASE WHEN status = 'running' THEN 1 END) as running_syncs,
                        AVG(duration_seconds) as avg_duration,
                        MAX(duration_seconds) as max_duration,
                        MIN(duration_seconds) as min_duration,
                        SUM(synced_entities) as total_entities
                    FROM milvus_sync_history
                    WHERE connection_id = $1
                    AND started_at >= $2
                """, connection_id, timestamp - timedelta(hours=24))

                # 待同步变更统计
                pending_stats = await conn.fetchrow("""
                    SELECT
                        COUNT(*) as pending_count,
                        EXTRACT(EPOCH FROM ($1 - MIN(created_at))) / 3600 as oldest_age_hours
                    FROM milvus_pending_changes
                    WHERE connection_id = $2
                    AND sync_status = 'pending'
                """, timestamp, connection_id)

                # 错误统计
                error_stats = await conn.fetchrow("""
                    SELECT
                        COUNT(*) as total_errors,
                        COUNT(CASE WHEN severity = 'critical' THEN 1 END) as critical_errors
                    FROM sync_errors
                    WHERE context->>'connection_id' = $1
                    AND timestamp >= $2
                    AND resolved = false
                """, connection_id, timestamp - timedelta(hours=24))

                # 最近一次同步状态
                last_sync = await conn.fetchrow("""
                    SELECT status, started_at, completed_at, duration_seconds
                    FROM milvus_sync_history
                    WHERE connection_id = $1
                    ORDER BY started_at DESC
                    LIMIT 1
                """, connection_id)

            # 计算指标
            total_syncs = sync_stats['total_syncs'] or 0
            successful_syncs = sync_stats['successful_syncs'] or 0
            failed_syncs = sync_stats['failed_syncs'] or 0
            running_syncs = sync_stats['running_syncs'] or 0

            # 计算健康分数
            if total_syncs > 0:
                success_rate = successful_syncs / total_syncs
                error_penalty = min(failed_syncs / max(total_syncs, 1), 0.5)
                health_score = max(0.0, success_rate - error_penalty)
            else:
                health_score = 1.0  # 没有同步记录时给满分

            # 计算错误率
            error_rate = failed_syncs / max(total_syncs, 1)

            # 平均每个同步的实体数
            avg_entities = sync_stats['total_entities'] / max(successful_syncs, 1) if successful_syncs > 0 else 0

            return SyncMetrics(
                timestamp=timestamp,
                connection_id=connection_id,
                total_syncs=total_syncs,
                successful_syncs=successful_syncs,
                failed_syncs=failed_syncs,
                running_syncs=running_syncs,
                avg_sync_duration=float(sync_stats['avg_duration'] or 0),
                max_sync_duration=float(sync_stats['max_duration'] or 0),
                min_sync_duration=float(sync_stats['min_duration'] or 0),
                total_entities_synced=int(sync_stats['total_entities'] or 0),
                avg_entities_per_sync=avg_entities,
                health_score=health_score,
                error_rate=error_rate,
                pending_changes_count=int(pending_stats['pending_count'] or 0),
                oldest_pending_change_age=float(pending_stats['oldest_age_hours'] or 0),
                cpu_usage=0.0,  # TODO: 实现系统资源监控
                memory_usage=0.0,
                redis_connections=0  # TODO: 实现Redis连接监控
            )

        except Exception as e:
            logger.error(f"收集连接 {connection_id} 指标失败", error=str(e))
            return SyncMetrics(timestamp=timestamp, connection_id=connection_id)

    async def _collect_system_metrics(self, timestamp: datetime, connection_metrics: List[SyncMetrics]) -> SystemMetrics:
        """收集系统级指标"""
        try:
            # 聚合连接指标
            total_connections = len(connection_metrics)
            active_connections = len([m for m in connection_metrics if m.total_syncs > 0 or m.running_syncs > 0])

            # 汇总同步统计
            total_syncs_last_hour = sum(m.total_syncs for m in connection_metrics)
            successful_syncs_last_hour = sum(m.successful_syncs for m in connection_metrics)
            failed_syncs_last_hour = sum(m.failed_syncs for m in connection_metrics)

            # 错误统计
            total_errors = sum(m.failed_syncs for m in connection_metrics)
            critical_errors = 0  # TODO: 从错误表统计严重错误

            error_rate = failed_syncs_last_hour / max(total_syncs_last_hour, 1)

            # 性能统计
            response_times = [m.avg_sync_duration for m in connection_metrics if m.avg_sync_duration > 0]
            if response_times:
                avg_response_time = sum(response_times) / len(response_times)
                sorted_times = sorted(response_times)
                p95_response_time = sorted_times[int(len(sorted_times) * 0.95)]
                p99_response_time = sorted_times[int(len(sorted_times) * 0.99)]
            else:
                avg_response_time = p95_response_time = p99_response_time = 0.0

            # 队列统计
            pending_sync_queue_size = sum(m.pending_changes_count for m in connection_metrics)
            retry_queue_size = await self._get_retry_queue_size()

            return SystemMetrics(
                timestamp=timestamp,
                total_connections=total_connections,
                active_connections=active_connections,
                total_syncs_last_hour=total_syncs_last_hour,
                total_syncs_last_day=sum(m.total_syncs for m in self.metrics_history if
                                        m.timestamp >= timestamp - timedelta(days=1)),
                successful_syncs_last_hour=successful_syncs_last_hour,
                failed_syncs_last_hour=failed_syncs_last_hour,
                total_errors_last_hour=total_errors,
                critical_errors_last_hour=critical_errors,
                error_rate_last_hour=error_rate,
                avg_response_time=avg_response_time,
                p95_response_time=p95_response_time,
                p99_response_time=p99_response_time,
                pending_sync_queue_size=pending_sync_queue_size,
                retry_queue_size=retry_queue_size
            )

        except Exception as e:
            logger.error("收集系统指标失败", error=str(e))
            return SystemMetrics(timestamp=timestamp)

    async def _get_retry_queue_size(self) -> int:
        """获取重试队列大小"""
        try:
            if self.redis_client:
                # TODO: 实现Redis队列大小检查
                pass

            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM sync_errors
                    WHERE resolved = false
                    AND next_retry_at <= CURRENT_TIMESTAMP
                """)
                return result or 0
        except Exception as e:
            logger.error("获取重试队列大小失败", error=str(e))
            return 0

    async def _send_to_monitoring_system(self, system_metrics: SystemMetrics, connection_metrics: List[SyncMetrics]):
        """发送指标到监控系统"""
        try:
            # 这里可以实现Prometheus、InfluxDB等监控系统的集成
            # 目前只记录日志

            # 发送WebSocket通知
            from server.websocket_manager import sync_event_broadcaster

            # 尝试广播真实健康状态（与HTTP接口保持一致的数据结构）
            try:
                from server.api.admin.auto_sync import (
                    get_sync_service,
                    build_sync_health_payload
                )

                sync_service = get_sync_service()

                for metrics in connection_metrics:
                    try:
                        payload = await build_sync_health_payload(
                            UUID(metrics.connection_id),
                            sync_service
                        )
                        await sync_event_broadcaster.broadcast_health_update(
                            metrics.connection_id,
                            payload
                        )
                    except Exception as broadcast_error:
                        logger.debug(
                            "广播健康状态失败",
                            connection_id=metrics.connection_id,
                            error=str(broadcast_error)
                        )
            except Exception as sync_service_error:
                logger.debug(
                    "自动同步服务不可用，跳过健康状态广播",
                    error=str(sync_service_error)
                )

        except Exception as e:
            logger.warning("发送监控指标失败", error=str(e))

    def _calculate_overall_health(self, system_metrics: SystemMetrics, connection_metrics: List[SyncMetrics]) -> float:
        """计算整体健康分数"""
        try:
            # 基于多个因素计算健康分数
            if not connection_metrics:
                return 1.0

            # 连接健康分数平均
            connection_health = sum(m.health_score for m in connection_metrics) / len(connection_metrics)

            # 系统负载因素（基于队列大小）
            queue_factor = max(0, 1.0 - system_metrics.pending_sync_queue_size / 1000)

            # 错误率因素
            error_factor = max(0, 1.0 - system_metrics.error_rate_last_hour * 2)

            # 综合计算
            overall_health = (connection_health * 0.6 + queue_factor * 0.2 + error_factor * 0.2)

            return round(min(1.0, max(0.0, overall_health)), 3)

        except Exception as e:
            logger.error("计算整体健康分数失败", error=str(e))
            return 0.5

    async def get_metrics_summary(self, hours: int = 24) -> Dict[str, Any]:
        """获取指标摘要"""
        try:
            since = datetime.utcnow() - timedelta(hours=hours)

            # 过滤历史数据
            recent_system_metrics = [m for m in self.system_metrics_history if m.timestamp >= since]
            recent_connection_metrics = [m for m in self.metrics_history if m.timestamp >= since]

            if not recent_system_metrics:
                return {"error": "没有可用数据"}

            # 计算汇总统计
            summary = {
                "time_range_hours": hours,
                "data_points": len(recent_system_metrics),
                "system": {
                    "avg_total_syncs": sum(m.total_syncs_last_hour for m in recent_system_metrics) / len(recent_system_metrics),
                    "avg_success_rate": 0,  # 需要计算
                    "avg_error_rate": sum(m.error_rate_last_hour for m in recent_system_metrics) / len(recent_system_metrics),
                    "avg_response_time": sum(m.avg_response_time for m in recent_system_metrics) / len(recent_system_metrics),
                    "avg_queue_size": sum(m.pending_sync_queue_size for m in recent_system_metrics) / len(recent_system_metrics)
                },
                "connections": {}
            }

            # 按连接分组统计
            connection_groups = {}
            for metrics in recent_connection_metrics:
                if metrics.connection_id not in connection_groups:
                    connection_groups[metrics.connection_id] = []
                connection_groups[metrics.connection_id].append(metrics)

            for conn_id, conn_metrics in connection_groups.items():
                summary["connections"][conn_id] = {
                    "avg_health": sum(m.health_score for m in conn_metrics) / len(conn_metrics),
                    "total_syncs": sum(m.total_syncs for m in conn_metrics),
                    "successful_syncs": sum(m.successful_syncs for m in conn_metrics),
                    "avg_duration": sum(m.avg_sync_duration for m in conn_metrics) / len(conn_metrics),
                    "total_entities": sum(m.total_entities_synced for m in conn_metrics)
                }

            return summary

        except Exception as e:
            logger.error("获取指标摘要失败", error=str(e))
            return {"error": str(e)}


# 全局指标收集器实例
metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> Optional[MetricsCollector]:
    """获取全局指标收集器实例"""
    return metrics_collector


def set_metrics_collector(collector: MetricsCollector):
    """设置全局指标收集器实例"""
    global metrics_collector
    metrics_collector = collector
