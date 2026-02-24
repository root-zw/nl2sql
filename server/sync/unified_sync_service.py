"""
统一Milvus同步管理服务
整合业务域+表同步和枚举值同步，支持自动和手动触发
"""

import asyncio
import json
import structlog
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple, Set
from uuid import UUID
from dataclasses import asdict
from server.utils.timezone_helper import now_with_tz, local_to_utc

import asyncpg

from server.models.sync import (
    SyncType, TriggeredBy, SyncStatus, EntitySyncStatus,
    EntityType, OperationType, SyncHistory, SyncConfig,
    PendingChange, EntityChange, SyncProgress, ManualSyncRequest
)
from server.models.admin import MilvusSyncEnumsRequest
from server.sync.concurrency_control import ConcurrencyController, SyncResourceManager
from server.sync.auto_sync_trigger import AutoSyncTrigger
from server.sync.error_handler import SyncErrorHandler, RetryConfig, set_error_handler
from server.config import settings, RetrievalConfig
from server.websocket_manager import sync_event_broadcaster
from server.api.admin.milvus import ensure_collection_exists, ensure_enum_collection_exists
from server.sync.sync_entities import (
    build_domain_entities,
    build_table_entities,
    build_field_entities,
    build_enum_entities,
    build_few_shot_entities,
    build_index_text,
    build_rich_table_index_text,
    normalize_tags,
)
from server.sync.sync_milvus import upsert_to_milvus, incremental_upsert_to_milvus
from server.sync.sync_queries import fetch_enums_for_sync, fetch_few_shots_for_sync
from server.sync.sync_executor import SyncExecutor
from server.sync.sync_strategy import SyncContext
from server.sync.strategies.full_sync_strategy import FullSyncStrategy
from server.sync.strategies.incremental_sync_strategy import IncrementalSyncStrategy

logger = structlog.get_logger()


def _json_default(value: Any) -> str:
    """将 UUID/时间等对象序列化为 JSON 字符串。"""
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class UnifiedSyncService:
    """统一Milvus同步管理服务

    功能：
    1. 统一管理所有类型的同步
    2. 支持自动和手动触发
    3. 实现真正的增量同步
    4. 并发控制和资源管理
    5. 错误处理和重试
    6. 进度跟踪和状态通知
    """

    def __init__(self, db_pool: asyncpg.Pool, redis_client=None,
                 milvus_client=None, embedding_client=None):
        """
        初始化统一同步服务

        Args:
            db_pool: 数据库连接池
            redis_client: Redis客户端
            milvus_client: Milvus客户端
            embedding_client: Embedding客户端
        """
        self.db = db_pool
        self.milvus = milvus_client
        self.embedding = embedding_client

        # 初始化并发控制和资源管理
        self.concurrency = ConcurrencyController(redis_client)
        self.resource_manager = SyncResourceManager(self.concurrency)

        # 初始化自动同步触发器
        self.auto_trigger = AutoSyncTrigger(db_pool, redis_client)
        
        # 注入依赖到AutoSyncTrigger
        self.auto_trigger.set_sync_service(self, self.concurrency)

        # 初始化错误处理器
        self.error_handler = SyncErrorHandler(db_pool)
        set_error_handler(self.error_handler)

        # 启动错误重试处理器
        asyncio.create_task(self.error_handler.start_retry_processor())

        # 同步任务管理
        self.running_tasks = {}  # 正在运行的任务

        # 配置参数
        self.default_timeout = settings.sync_default_timeout_seconds
        self.max_retry_attempts = settings.sync_max_retry_attempts
        self.retry_delay_base = settings.sync_retry_delay_base  # 重试延迟基数（秒）

        # 同步策略执行器
        self.sync_executor = SyncExecutor()
        self.sync_executor.register(SyncType.FULL, FullSyncStrategy)
        self.sync_executor.register(SyncType.INCREMENTAL, IncrementalSyncStrategy)

    def _map_table_name_to_entity_type(self, table_name: str) -> EntityType:
        """
        将表名映射到实体类型

        Args:
            table_name: 数据库表名

        Returns:
            EntityType: 对应的实体类型枚举
        """
        entity_type_mapping = {
            'business_domains': 'domain',
            'db_tables': 'table',
            'fields': 'field',
            'field_enum_values': 'enum',
            'qa_few_shot_samples': 'few_shot',
            'few_shot': 'few_shot'
        }

        mapped_type = entity_type_mapping.get(table_name.lower(), table_name.lower())

        try:
            return EntityType(mapped_type)
        except ValueError:
            logger.warning(f"无法映射表名到实体类型: {table_name} -> {mapped_type}")
            raise ValueError(f"不支持的表名: {table_name}")

    async def start(self):
        """启动同步服务"""
        try:
            # 启动自动同步触发器
            await self.auto_trigger.start()

            # 启动队列处理器
            asyncio.create_task(self._queue_processor())

            # 启动状态监控器
            asyncio.create_task(self._status_monitor())

            logger.info("统一同步服务已启动")

        except Exception as e:
            logger.error("启动统一同步服务失败", error=str(e))
            raise

    async def stop(self):
        """停止同步服务"""
        try:
            # 停止自动同步触发器
            await self.auto_trigger.stop()

            # 取消所有运行中的任务
            for sync_id, task in list(self.running_tasks.items()):
                if not task.done():
                    task.cancel()
                    logger.debug("取消同步任务", sync_id=str(sync_id))

            # 等待任务完成
            if self.running_tasks:
                await asyncio.gather(
                    *[task for task in self.running_tasks.values() if not task.done()],
                    return_exceptions=True
                )

            self.running_tasks.clear()

            logger.info("统一同步服务已停止")

        except Exception as e:
            logger.error("停止统一同步服务失败", error=str(e))

    async def trigger_auto_sync(self, connection_id: UUID,
                               entity_changes: List[EntityChange]) -> Optional[UUID]:
        """
        触发自动同步

        Args:
            connection_id: 连接ID
            entity_changes: 实体变更列表

        Returns:
            UUID: 同步任务ID，失败时返回None
        """
        try:
            logger.debug(
                "触发自动同步",
                connection_id=str(connection_id),
                changes_count=len(entity_changes)
            )

            # 创建同步历史记录
            sync_id = await self._create_sync_record(
                connection_id,
                SyncType.INCREMENTAL,
                TriggeredBy.AUTO,
                entity_changes
            )

            if sync_id:
                # 将任务加入队列
                await self.concurrency.enqueue_sync_task(
                    connection_id, sync_id, priority=3  # 自动同步优先级较低
                )

            return sync_id

        except Exception as e:
            logger.error(
                "触发自动同步失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return None

    async def trigger_manual_sync(self, request: ManualSyncRequest) -> Optional[UUID]:
        """
        触发手动同步

        Args:
            request: 手动同步请求

        Returns:
            UUID: 同步任务ID，失败时返回None
        """
        try:
            logger.debug(
                "触发手动同步",
                connection_id=str(request.connection_id),
                sync_domains=request.sync_domains,
                sync_tables=request.sync_tables,
                sync_enums=request.sync_enums,
                force_full_sync=request.force_full_sync
            )

            # 确定同步类型
            sync_type = SyncType.FULL if request.force_full_sync else SyncType.INCREMENTAL
            logger.debug(
                "确定同步类型",
                connection_id=str(request.connection_id),
                force_full_sync=request.force_full_sync,
                sync_type=sync_type.value
            )

            # 创建同步历史记录
            sync_id = await self._create_sync_record(
                request.connection_id,
                sync_type,
                TriggeredBy.MANUAL,
                manual_request=request
            )

            if sync_id:
                # 将任务加入队列（手动同步优先级较高）
                await self.concurrency.enqueue_sync_task(
                    request.connection_id, sync_id, priority=1
                )

            return sync_id

        except Exception as e:
            logger.error(
                "触发手动同步失败",
                connection_id=str(request.connection_id),
                error=str(e)
            )
            return None

    async def trigger_incremental_sync(
        self,
        connection_id: UUID,
        entity_types: Optional[List[EntityType]] = None,
        *,
        triggered_by: TriggeredBy = TriggeredBy.MANUAL,
        priority: int = 1
    ) -> Optional[UUID]:
        """
        触发部分实体的增量同步（用于实体编辑后的 sync_now）
        """
        try:
            pending_changes = await self.get_pending_changes(
                connection_id,
                entity_types,
                limit=None
            )
            if not pending_changes:
                logger.debug(
                    "sync_now：没有待同步变更",
                    connection_id=str(connection_id),
                    entity_types=[et.value for et in entity_types] if entity_types else "all"
                )
                return None

            entity_changes = [
                EntityChange(
                    entity_type=change.entity_type,
                    entity_id=change.entity_id,
                    operation=change.operation,
                    old_data=change.old_data,
                    new_data=change.new_data,
                    changed_at=change.created_at
                )
                for change in pending_changes
            ]

            sync_type = self._infer_sync_type(pending_changes)
            sync_id = await self._create_sync_record(
                connection_id,
                sync_type,
                triggered_by,
                entity_changes=entity_changes
            )
            if not sync_id:
                return None

            await self._mark_change_ids_syncing(
                [change.change_id for change in pending_changes],
                sync_id
            )

            await self.concurrency.enqueue_sync_task(
                connection_id,
                sync_id,
                priority=priority
            )
            return sync_id

        except Exception as e:
            logger.error(
                "触发增量同步失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return None

    @staticmethod
    def _infer_sync_type(pending_changes: List[PendingChange]) -> SyncType:
        if not pending_changes:
            return SyncType.INCREMENTAL
        entity_types = {change.entity_type for change in pending_changes}
        if entity_types == {EntityType.ENUM}:
            return SyncType.ENUMS
        if EntityType.ENUM in entity_types and len(entity_types) == 1:
            return SyncType.ENUMS
        return SyncType.INCREMENTAL

    async def get_sync_status(self, sync_id: UUID) -> Optional[SyncHistory]:
        """
        获取同步状态

        Args:
            sync_id: 同步任务ID

        Returns:
            SyncHistory: 同步历史记录
        """
        try:
            row = await self.db.fetchrow("""
                SELECT
                    sync_id, connection_id, sync_type, triggered_by,
                    status, started_at, completed_at, duration_seconds,
                    total_entities, synced_entities, failed_entities,
                    entity_changes, sync_config, error_message, error_details,
                    current_step, progress_percentage, created_by
                FROM milvus_sync_history
                WHERE sync_id = $1
            """, sync_id)

            if not row:
                return None

            return SyncHistory(
                sync_id=row['sync_id'],
                connection_id=row['connection_id'],
                sync_type=SyncType(row['sync_type']),
                triggered_by=TriggeredBy(row['triggered_by']),
                status=SyncStatus(row['status']),
                started_at=row['started_at'],
                completed_at=row['completed_at'],
                duration_seconds=row['duration_seconds'],
                total_entities=row['total_entities'],
                synced_entities=row['synced_entities'],
                failed_entities=row['failed_entities'],
                entity_changes=row['entity_changes'],
                sync_config=row['sync_config'],
                error_message=row['error_message'],
                error_details=row['error_details'],
                current_step=row['current_step'],
                progress_percentage=row['progress_percentage'],
                created_by=row['created_by']
            )

        except Exception as e:
            logger.error(
                "获取同步状态失败",
                sync_id=str(sync_id),
                error=str(e)
            )
            return None

    def _map_entity_type_to_table_name(self, entity_type: EntityType) -> str:
        """
        将实体类型枚举映射回数据库表名

        Args:
            entity_type: 实体类型枚举

        Returns:
            str: 数据库表名
        """
        reverse_mapping = {
            'domain': 'business_domains',
            'table': 'db_tables',
            'field': 'fields',
            'enum': 'field_enum_values',
            'few_shot': 'qa_few_shot_samples'
        }
        return reverse_mapping.get(entity_type.value, entity_type.value)

    async def get_pending_changes(self, connection_id: UUID,
                                 entity_types: Optional[List[EntityType]] = None,
                                 limit: Optional[int] = 100,
                                 *,
                                 statuses: Optional[List[EntitySyncStatus]] = None,
                                 sync_id: Optional[UUID] = None) -> List[PendingChange]:
        """
        获取待同步变更

        Args:
            connection_id: 连接ID
            entity_types: 实体类型过滤
            limit: 返回数量限制

        Returns:
            List[PendingChange]: 待同步变更列表
        """
        try:
            params = [connection_id]
            param_index = 2

            status_values = [
                status.value if isinstance(status, EntitySyncStatus) else str(status)
                for status in (statuses or [EntitySyncStatus.PENDING])
            ]

            query = """
                SELECT
                    change_id, connection_id, entity_type, entity_id,
                    operation, old_data, new_data, created_at,
                    updated_at, sync_status, sync_id, synced_at, priority
                FROM milvus_pending_changes
                WHERE connection_id = $1
                  AND sync_status = ANY($2)
            """
            params.append(status_values)
            param_index += 1

            if sync_id:
                query += f" AND sync_id = ${param_index}"
                params.append(sync_id)
                param_index += 1

            if entity_types:
                # 将EntityType枚举值映射回数据库表名
                table_names = [self._map_entity_type_to_table_name(t) for t in entity_types]
                query += f" AND entity_type = ANY(${param_index})"
                params.append(table_names)
                param_index += 1

            query += """
                ORDER BY
                    CASE entity_type
                        WHEN 'business_domains' THEN 1
                        WHEN 'domain' THEN 1
                        WHEN 'db_tables' THEN 2
                        WHEN 'table' THEN 2
                        WHEN 'fields' THEN 3
                        WHEN 'field_enum_values' THEN 4
                        WHEN 'enum' THEN 4
                        WHEN 'qa_few_shot_samples' THEN 5
                        WHEN 'few_shot' THEN 5
                        ELSE 99
                    END,
                    priority ASC,
                    created_at ASC
            """

            if limit is not None:
                query += f" LIMIT ${param_index}"
                params.append(limit)

            rows = await self.db.fetch(query, *params)

            changes = []
            for row in rows:
                change = PendingChange(
                    change_id=row['change_id'],
                    connection_id=row['connection_id'],
                    entity_type=self._map_table_name_to_entity_type(row['entity_type']),
                    entity_id=row['entity_id'],
                    operation=OperationType(row['operation']),
                    old_data=row['old_data'],
                    new_data=row['new_data'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    sync_status=EntitySyncStatus(row['sync_status']),
                    sync_id=row['sync_id'],
                    synced_at=row['synced_at'],
                    priority=row['priority']
                )
                changes.append(change)

            return changes

        except Exception as e:
            logger.error(
                "获取待同步变更失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return []

    async def cancel_sync(self, sync_id: UUID) -> bool:
        """
        取消同步任务

        Args:
            sync_id: 同步任务ID

        Returns:
            bool: 是否成功取消
        """
        try:
            # 获取同步记录
            sync_record = await self.get_sync_status(sync_id)
            if not sync_record:
                return False

            # 只有待执行和运行中的任务可以取消
            if sync_record.status not in [SyncStatus.PENDING, SyncStatus.RUNNING]:
                logger.warning(
                    "同步任务状态不允许取消",
                    sync_id=str(sync_id),
                    status=sync_record.status.value
                )
                return False

            # 更新状态为已取消
            current_time = now_with_tz()
            await self.db.execute("""
                UPDATE milvus_sync_history
                SET status = 'cancelled', completed_at = $1
                WHERE sync_id = $2
            """, current_time, sync_id)

            # 如果任务正在运行，取消协程
            if sync_id in self.running_tasks:
                task = self.running_tasks[sync_id]
                if not task.done():
                    task.cancel()
                    del self.running_tasks[sync_id]

            # 释放资源
            await self.resource_manager.release_resources(
                sync_record.connection_id,
                SyncStatus.CANCELLED
            )

            logger.debug("同步任务已取消", sync_id=str(sync_id))
            return True

        except Exception as e:
            logger.error(
                "取消同步任务失败",
                sync_id=str(sync_id),
                error=str(e)
            )
            return False

    async def _create_sync_record(self, connection_id: UUID,
                                 sync_type: SyncType,
                                 triggered_by: TriggeredBy,
                                 entity_changes: Optional[List[EntityChange]] = None,
                                 manual_request: Optional[ManualSyncRequest] = None) -> Optional[UUID]:
        """创建同步历史记录"""
        try:
            # 首先验证连接是否存在
            connection_exists = await self.db.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM database_connections 
                    WHERE connection_id = $1 AND is_active = TRUE
                )
            """, connection_id)
            
            if not connection_exists:
                logger.error(
                    "连接不存在或未激活，无法创建同步记录",
                    connection_id=str(connection_id)
                )
                return None
            
            # 序列化变更信息
            changes_json = None
            if entity_changes:
                changes_payload = [asdict(change) for change in entity_changes]
                changes_json = json.dumps(changes_payload, default=_json_default)

            # 构建同步配置
            sync_config = None
            if manual_request:
                sync_config = manual_request.model_dump_json()

            # 创建记录
            sync_id = await self.db.fetchval("""
                INSERT INTO milvus_sync_history (
                    connection_id, sync_type, triggered_by, status,
                    entity_changes, sync_config
                ) VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING sync_id
            """,
                connection_id,
                sync_type.value,
                triggered_by.value,
                SyncStatus.PENDING.value,
                changes_json,
                sync_config
            )

            logger.debug(
                "创建同步记录",
                sync_id=str(sync_id),
                connection_id=str(connection_id),
                sync_type=sync_type.value
            )
            return sync_id

        except Exception as e:
            # 检查是否是外键约束错误
            error_str = str(e)
            if "foreign key constraint" in error_str.lower() or "violates foreign key" in error_str.lower():
                logger.error(
                    "连接不存在，无法创建同步记录",
                    connection_id=str(connection_id),
                    error="连接ID在 database_connections 表中不存在"
                )
            else:
                logger.error(
                    "创建同步记录失败",
                    connection_id=str(connection_id),
                    error=error_str
                )
            return None

    async def _queue_processor(self):
        """队列处理器 - 处理队列中的同步任务"""
        logger.info("启动同步队列处理器")

        while True:
            try:
                # 从队列取出任务
                task_data = await self.concurrency.dequeue_sync_task()
                if task_data:
                    sync_id = UUID(task_data["sync_id"])
                    connection_id = UUID(task_data["connection_id"])

                    # 检查任务是否已被取消
                    sync_status = await self.get_sync_status(sync_id)
                    if sync_status and sync_status.status == SyncStatus.CANCELLED:
                        continue

                    # 执行同步任务
                    task = asyncio.create_task(
                        self._perform_sync(sync_id, connection_id)
                    )
                    self.running_tasks[sync_id] = task

                else:
                    # 没有任务时短暂休眠
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("队列处理器被取消")
                break
            except Exception as e:
                logger.error("队列处理器异常", error=str(e))
                await asyncio.sleep(5)

    async def _perform_sync(self, sync_id: UUID, connection_id: UUID):
        """
        执行同步任务

        Args:
            sync_id: 同步任务ID
            connection_id: 连接ID
        """
        try:
            logger.info("开始执行同步任务", sync_id=str(sync_id))

            # 1. 获取同步资源
            if not await self.resource_manager.acquire_resources(connection_id):
                await self._mark_sync_failed(sync_id, "无法获取同步资源")
                return

            # 2. 获取同步配置
            sync_record = await self.get_sync_status(sync_id)
            if not sync_record:
                await self._mark_sync_failed(sync_id, "找不到同步记录")
                return

            # 3. 更新状态为运行中
            await self._update_sync_progress(sync_id, "开始同步", 0)

            # 4. 广播同步开始事件
            try:
                await sync_event_broadcaster.broadcast_sync_started(
                    str(connection_id), str(sync_id), sync_record.sync_type.value
                )
            except Exception as e:
                logger.warning("广播同步开始事件失败", error=str(e))

            # 5. 根据同步类型执行相应的同步
            logger.debug(
                "准备执行同步",
                sync_id=str(sync_id),
                sync_type=sync_record.sync_type.value,
                connection_id=str(connection_id)
            )
            timeout_seconds = self.default_timeout
            if sync_record.sync_type == SyncType.FULL:
                timeout_seconds = getattr(settings, "sync_full_timeout_seconds", self.default_timeout)

            if sync_record.sync_type == SyncType.FULL:
                logger.info("执行全量同步", sync_id=str(sync_id))
                run_coro = self._perform_full_sync(sync_id, connection_id, sync_record)
            elif sync_record.sync_type == SyncType.INCREMENTAL:
                logger.info("执行增量同步", sync_id=str(sync_id))
                run_coro = self._perform_incremental_sync(sync_id, connection_id, sync_record)
            elif sync_record.sync_type == SyncType.ENUMS:
                logger.info("执行枚举值同步", sync_id=str(sync_id))
                run_coro = self._perform_enum_sync(sync_id, connection_id, sync_record)
            else:
                run_coro = None
                success = await self._mark_sync_failed(sync_id, f"不支持的同步类型: {sync_record.sync_type}")

            if run_coro is not None:
                try:
                    success = await asyncio.wait_for(run_coro, timeout=timeout_seconds)
                except asyncio.TimeoutError:
                    message = f"全量同步超时（>{timeout_seconds}s）" if sync_record.sync_type == SyncType.FULL else f"同步超时（>{timeout_seconds}s）"
                    logger.warning(message, sync_id=str(sync_id))
                    await self._mark_sync_failed(sync_id, message)
                    success = False

            # 6. 清理资源
            final_status = SyncStatus.COMPLETED if success else SyncStatus.FAILED
            await self.resource_manager.release_resources(connection_id, final_status)

            # 7. 广播同步完成/失败事件
            try:
                if success:
                    # 获取最终结果
                    final_record = await self.get_sync_status(sync_id)
                    result = {
                        "sync_type": final_record.sync_type.value if final_record else "unknown",
                        "total_entities": final_record.total_entities if final_record else 0,
                        "synced_entities": final_record.synced_entities if final_record else 0,
                        "failed_entities": final_record.failed_entities if final_record else 0,
                        "duration_seconds": final_record.duration_seconds if final_record else 0
                    }
                    await sync_event_broadcaster.broadcast_sync_completed(
                        str(connection_id), str(sync_id), result
                    )
                else:
                    # 获取错误信息
                    failed_record = await self.get_sync_status(sync_id)
                    error_message = failed_record.error_message if failed_record else "同步失败"
                    await sync_event_broadcaster.broadcast_sync_failed(
                        str(connection_id), str(sync_id), error_message
                    )
            except Exception as e:
                logger.warning("广播同步完成事件失败", error=str(e))

            if sync_id in self.running_tasks:
                del self.running_tasks[sync_id]

            logger.debug(
                "同步任务执行完成",
                sync_id=str(sync_id),
                success=success,
                final_status=final_status.value
            )

        except asyncio.CancelledError:
            logger.info("同步任务被取消", sync_id=str(sync_id))
            await self.resource_manager.release_resources(connection_id, SyncStatus.CANCELLED)
        except Exception as e:
            logger.error(
                "同步任务执行异常",
                sync_id=str(sync_id),
                error=str(e)
            )

            # 使用错误处理器处理异常
            context = {
                'sync_id': str(sync_id),
                'connection_id': str(connection_id),
                'sync_type': 'unknown'
            }

            retry_config = RetryConfig(
                max_retries=self.max_retry_attempts,
                strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
                base_delay=self.retry_delay_base
            )

            error_info = await self.error_handler.handle_sync_error(e, context, retry_config)

            await self._mark_sync_failed(sync_id, str(e))
            await self.resource_manager.release_resources(connection_id, SyncStatus.FAILED)

    async def _perform_full_sync(self, sync_id: UUID, connection_id: UUID,
                                 sync_record: SyncHistory) -> bool:
        """执行全量同步（策略模式）"""
        try:
            logger.info("开始全量同步", sync_id=str(sync_id))

            pending_change_ids = await self._get_pending_change_ids(connection_id)

            manual_request = None
            sync_config = sync_record.sync_config or {}
            if sync_config:
                if isinstance(sync_config, str):
                    try:
                        sync_config = json.loads(sync_config)
                    except json.JSONDecodeError as e:
                        logger.warning("解析sync_config失败，使用默认配置", error=str(e))
                        sync_config = {}
                manual_request = ManualSyncRequest(**sync_config)

            context = SyncContext(
                connection_id=connection_id,
                db=self.db,
                milvus_client=self.milvus,
                embedding_client=self.embedding,
                sync_type=SyncType.FULL,
                manual_request=manual_request,
                sync_record=sync_record,
                # 保留集合，仅按 connection_id 删除再写入，避免影响其他连接的数据
                recreate_collections=False,
                progress_hook=lambda step, pct: self._update_sync_progress(sync_id, step, pct),
                service=self,
            )

            result = await self.sync_executor.execute(context)

            if not result.success:
                raise RuntimeError(result.message or "全量同步失败")

            await self._update_sync_completion(
                sync_id,
                True,
                total_entities_override=result.total_entities,
                synced_entities_override=result.synced_entities,
            )

            if pending_change_ids:
                await self._mark_change_ids_synced(pending_change_ids, connection_id)

            return True

        except Exception as e:
            logger.error("全量同步失败", sync_id=str(sync_id), error=str(e))
            await self._mark_sync_failed(sync_id, str(e))
            return False

    async def _perform_incremental_sync(self, sync_id: UUID, connection_id: UUID,
                                        sync_record: SyncHistory) -> bool:
        """执行增量同步（策略模式）"""
        try:
            logger.info("开始增量同步", sync_id=str(sync_id))

            # 获取所有待处理的变更（不使用 sync_id 过滤，因为变更记录不会预先关联到同步任务）
            pending_changes = await self.get_pending_changes(
                connection_id,
                limit=None,
                statuses=[EntitySyncStatus.PENDING, EntitySyncStatus.SYNCING],
                sync_id=None  # 修复：不使用 sync_id 过滤，获取所有待处理变更
            )

            context = SyncContext(
                connection_id=connection_id,
                db=self.db,
                milvus_client=self.milvus,
                embedding_client=self.embedding,
                sync_type=SyncType.INCREMENTAL,
                sync_record=sync_record,
                pending_changes=pending_changes,
                progress_hook=lambda step, pct: self._update_sync_progress(sync_id, step, pct),
                service=self,
            )

            result = await self.sync_executor.execute(context)

            if not result.success:
                raise RuntimeError(result.message or "增量同步失败")

            if result.synced_change_ids:
                await self._mark_change_ids_synced(result.synced_change_ids, connection_id)
            elif pending_changes:
                await self._mark_changes_synced(pending_changes)

            await self._update_sync_completion(
                sync_id,
                True,
                total_entities_override=result.total_entities,
                synced_entities_override=result.synced_entities,
            )

            return True

        except Exception as e:
            logger.error("增量同步失败", sync_id=str(sync_id), error=str(e))
            await self._mark_sync_failed(sync_id, str(e))
            return False

    async def _perform_enum_sync(self, sync_id: UUID, connection_id: UUID,
                               sync_record: SyncHistory) -> bool:
        """执行枚举值同步"""
        try:
            logger.info("开始枚举值同步", sync_id=str(sync_id))

            # 获取所有需要同步的枚举值变更
            enum_changes = await self.get_pending_changes(
                connection_id, [EntityType.ENUM]
            )

            if not enum_changes:
                logger.debug("没有枚举值变更需要同步", sync_id=str(sync_id))
                await self._update_sync_progress(sync_id, "无枚举值变更", 100)
                await self._update_sync_completion(sync_id, True)
                return True

            await self._update_sync_progress(sync_id, f"同步{len(enum_changes)}个枚举值变更", 50)

            # 调用枚举值同步逻辑
            success = await self._sync_enum_changes(connection_id, enum_changes)

            if success:
                # 标记变更已同步
                await self._mark_changes_synced(enum_changes)
                await self._update_sync_progress(sync_id, "枚举值同步完成", 100)

            await self._update_sync_completion(sync_id, success)
            return success

        except Exception as e:
            logger.error(
                "枚举值同步失败",
                sync_id=str(sync_id),
                error=str(e)
            )
            await self._mark_sync_failed(sync_id, str(e))
            return False

    async def _sync_domain_changes(self, connection_id: UUID,
                                   changes: List[PendingChange]) -> bool:
        """同步业务域变更（增量）"""
        if not changes:
            return True

        if not self.milvus or not self.embedding:
            logger.warning("Milvus或Embedding客户端未配置，无法同步业务域", connection_id=str(connection_id))
            return False

        try:
            latest_changes = self._latest_change_by_entity(changes)
            collection_name = await ensure_collection_exists(self.milvus, recreate=False)

            upsert_ids = [entity_id for entity_id, change in latest_changes.items()
                          if change.operation != OperationType.DELETE]
            domain_rows = {}
            if upsert_ids:
                # 查询字段需要匹配 build_domain_entities 的期望格式
                rows = await self.db.fetch("""
                    SELECT domain_id, domain_name, domain_code, description, keywords,
                           table_count, is_active
                    FROM business_domains
                    WHERE connection_id = $1 AND domain_id = ANY($2::uuid[])
                """, connection_id, upsert_ids)
                domain_rows = {row['domain_id']: row for row in rows}

            entities = []
            delete_ids: Set[UUID] = set()

            for entity_id, change in latest_changes.items():
                if change.operation == OperationType.DELETE:
                    delete_ids.add(entity_id)
                    continue

                row = domain_rows.get(entity_id)
                if not row or not row['is_active']:
                    delete_ids.add(entity_id)
                    continue

                # 使用 build_domain_entities 来正确构建实体，确保字段完整
                domain_entities = await build_domain_entities(
                    [row],
                    self.embedding,
                    connection_id
                )
                entities.extend(domain_entities)

            # 先删除旧数据（包括要更新的实体）
            ids_to_remove: Set[str] = {str(item_id) for item_id in delete_ids}
            ids_to_remove.update(entity["item_id"] for entity in entities)
            if ids_to_remove:
                await self._delete_milvus_entities(collection_name, connection_id, ids_to_remove)

            # 然后插入新数据
            if entities:
                await asyncio.to_thread(
                    self.milvus.insert,
                    collection_name=collection_name,
                    data=entities,
                )
                logger.debug(
                    "增量同步业务域完成",
                    connection_id=str(connection_id),
                    upsert=len(entities),
                    deleted=len(delete_ids)
                )

            if delete_ids and not entities:
                logger.debug(
                    "业务域增量同步仅执行删除",
                    connection_id=str(connection_id),
                    deleted=len(delete_ids)
                )

            return True

        except Exception as e:
            logger.error("同步业务域变更失败", connection_id=str(connection_id), error=str(e))
            return False

    async def _sync_table_changes(self, connection_id: UUID,
                                  changes: List[PendingChange]) -> bool:
        """同步数据表变更（增量）"""
        if not changes:
            return True

        if not self.milvus or not self.embedding:
            logger.warning("Milvus或Embedding客户端未配置，无法同步数据表", connection_id=str(connection_id))
            return False

        try:
            latest_changes = self._latest_change_by_entity(changes)
            collection_name = await ensure_collection_exists(self.milvus, recreate=False)

            upsert_ids = [entity_id for entity_id, change in latest_changes.items()
                          if change.operation != OperationType.DELETE]
            table_rows = {}
            if upsert_ids:
                # 查询字段需要匹配 build_table_entities 的期望格式，包括 relations 和 domain_name
                rows = await self.db.fetch("""
                    WITH rel AS (
                        SELECT
                            rel_base.table_id,
                            jsonb_agg(
                                jsonb_build_object(
                                    'target_table_id', rel_base.target_table_id,
                                    'target_table_name', rel_base.target_table_name,
                                    'relationship_type', rel_base.relationship_type,
                                    'join_type', rel_base.join_type
                                )
                            ) AS relations
                        FROM (
                            SELECT
                                tr.left_table_id AS table_id,
                                tr.right_table_id AS target_table_id,
                                rt.display_name AS target_table_name,
                                tr.relationship_type,
                                tr.join_type
                            FROM table_relationships tr
                            JOIN db_tables rt ON tr.right_table_id = rt.table_id
                            WHERE tr.connection_id = $1
                              AND tr.is_active = TRUE
                            UNION ALL
                            SELECT
                                tr.right_table_id AS table_id,
                                tr.left_table_id AS target_table_id,
                                lt.display_name AS target_table_name,
                                tr.relationship_type,
                                tr.join_type
                            FROM table_relationships tr
                            JOIN db_tables lt ON tr.left_table_id = lt.table_id
                            WHERE tr.connection_id = $1
                              AND tr.is_active = TRUE
                        ) rel_base
                        GROUP BY rel_base.table_id
                    )
                    SELECT
                        t.table_id,
                        t.table_name,
                        t.schema_name,
                        t.display_name,
                        t.description,
                        t.tags,
                        t.domain_id,
                        t.data_year,
                        t.is_included,
                        d.domain_name,
                        COUNT(DISTINCT f.field_id) AS field_count,
                        ARRAY_AGG(DISTINCT f.display_name)
                            FILTER (WHERE f.field_id IS NOT NULL) AS field_names,
                        rel.relations
                    FROM db_tables t
                    LEFT JOIN business_domains d ON t.domain_id = d.domain_id
                    LEFT JOIN db_columns c ON t.table_id = c.table_id
                    LEFT JOIN fields f
                        ON c.column_id = f.source_column_id
                       AND f.is_active = TRUE
                    LEFT JOIN rel ON rel.table_id = t.table_id
                    WHERE t.connection_id = $1
                      AND t.table_id = ANY($2::uuid[])
                      AND t.is_included = TRUE
                    GROUP BY
                        t.table_id,
                        t.table_name,
                        t.schema_name,
                        t.display_name,
                        t.description,
                        t.tags,
                        t.domain_id,
                        t.data_year,
                        t.is_included,
                        d.domain_name,
                        rel.relations
                """, connection_id, upsert_ids)
                table_rows = {row['table_id']: row for row in rows}

            entities = []
            delete_ids: Set[UUID] = set()

            for entity_id, change in latest_changes.items():
                if change.operation == OperationType.DELETE:
                    delete_ids.add(entity_id)
                    continue

                row = table_rows.get(entity_id)
                if not row or not row['is_included']:
                    delete_ids.add(entity_id)
                    continue

                # 使用 build_table_entities 来正确构建实体，确保字段完整
                table_entities = await build_table_entities(
                    [row],
                    self.embedding,
                    connection_id
                )
                entities.extend(table_entities)

            # 先删除旧数据（包括要更新的实体）
            ids_to_remove: Set[str] = {str(item_id) for item_id in delete_ids}
            ids_to_remove.update(entity["item_id"] for entity in entities)
            if ids_to_remove:
                await self._delete_milvus_entities(collection_name, connection_id, ids_to_remove)

            # 然后插入新数据
            if entities:
                await asyncio.to_thread(
                    self.milvus.insert,
                    collection_name=collection_name,
                    data=entities,
                )
                logger.debug(
                    "增量同步数据表完成",
                    connection_id=str(connection_id),
                    upsert=len(entities),
                    deleted=len(delete_ids)
                )

            if delete_ids and not entities:
                logger.debug(
                    "数据表增量同步仅执行删除",
                    connection_id=str(connection_id),
                    deleted=len(delete_ids)
                )

            return True

        except Exception as e:
            logger.error("同步表变更失败", connection_id=str(connection_id), error=str(e))
            return False

    async def _sync_field_changes(self, connection_id: UUID,
                                  changes: List[PendingChange]) -> bool:
        """同步字段变更（增量）"""
        if not changes:
            return True

        if not self.milvus or not self.embedding:
            logger.warning("Milvus或Embedding客户端未配置，无法同步字段", connection_id=str(connection_id))
            return False

        try:
            latest_changes = self._latest_change_by_entity(changes)
            collection_name = await ensure_collection_exists(self.milvus, recreate=False)

            upsert_ids = [entity_id for entity_id, change in latest_changes.items()
                          if change.operation != OperationType.DELETE]
            field_rows = {}
            if upsert_ids:
                # 查询字段需要匹配 build_field_entities 的期望格式
                rows = await self.db.fetch("""
                    SELECT
                        f.field_id,
                        f.display_name,
                        f.description,
                        f.field_type,
                        f.synonyms,
                        f.unit,
                        f.format_pattern,
                        t.domain_id,
                        t.table_id,
                        t.display_name AS table_display_name,
                        t.table_name,
                        t.schema_name,
                        d.domain_name,
                        c.column_name,
                        c.data_type,
                        c.distinct_count
                    FROM fields f
                    JOIN db_columns c ON f.source_column_id = c.column_id
                    JOIN db_tables t ON c.table_id = t.table_id
                    LEFT JOIN business_domains d ON t.domain_id = d.domain_id
                    WHERE t.connection_id = $1
                      AND f.field_id = ANY($2::uuid[])
                      AND t.is_included = TRUE
                      AND f.is_active = TRUE
                """, connection_id, upsert_ids)
                field_rows = {row['field_id']: row for row in rows}

            entities = []
            delete_ids: Set[UUID] = set()

            for entity_id, change in latest_changes.items():
                if change.operation == OperationType.DELETE:
                    delete_ids.add(entity_id)
                    continue

                row = field_rows.get(entity_id)
                if not row:
                    delete_ids.add(entity_id)
                    continue

                # 使用 build_field_entities 来正确构建实体，确保字段完整
                field_entities = await build_field_entities(
                    [row],
                    self.embedding,
                    connection_id
                )
                entities.extend(field_entities)

            # 先删除旧数据（包括要更新的实体）
            ids_to_remove: Set[str] = {str(item_id) for item_id in delete_ids}
            ids_to_remove.update(entity["item_id"] for entity in entities)
            if ids_to_remove:
                await self._delete_milvus_entities(collection_name, connection_id, ids_to_remove)

            # 然后插入新数据
            if entities:
                await asyncio.to_thread(
                    self.milvus.insert,
                    collection_name=collection_name,
                    data=entities,
                )
                logger.debug(
                    "增量同步字段完成",
                    connection_id=str(connection_id),
                    upsert=len(entities),
                    deleted=len(delete_ids)
                )

            if delete_ids and not entities:
                logger.debug(
                    "字段增量同步仅执行删除",
                    connection_id=str(connection_id),
                    deleted=len(delete_ids)
                )

            return True

        except Exception as e:
            logger.error("同步字段变更失败", connection_id=str(connection_id), error=str(e))
            return False

    async def _sync_enum_changes(self, connection_id: UUID,
                               changes: List[PendingChange]) -> bool:
        """同步枚举值变更（增量）"""
        if not changes:
            return True

        if not self.milvus or not self.embedding:
            logger.warning("Milvus或Embedding服务未配置，无法同步枚举值", connection_id=str(connection_id))
            return False

        try:
            # 获取最新的变更（按实体分组，保留最新的一条）
            latest_changes = self._latest_change_by_entity(changes)
            
            # 提取枚举值ID
            enum_value_ids = [entity_id for entity_id, change in latest_changes.items()
                             if change.operation != OperationType.DELETE]
            delete_ids: Set[UUID] = {entity_id for entity_id, change in latest_changes.items()
                                    if change.operation == OperationType.DELETE}

            # 确保枚举值集合存在
            from server.api.admin.milvus import ensure_enum_collection_exists
            collection_name = ensure_enum_collection_exists(self.milvus, recreate=False)

            # 1. 处理删除操作
            if delete_ids:
                for enum_value_id in delete_ids:
                    try:
                        filter_expr = f'value_id == "{str(enum_value_id)}"'
                        self.milvus.delete(
                            collection_name=collection_name,
                            filter=filter_expr,
                        )
                    except Exception as exc:
                        logger.debug(
                            "删除Milvus枚举值失败",
                            connection_id=str(connection_id),
                            enum_value_id=str(enum_value_id),
                            error=str(exc),
                        )

            # 2. 处理插入/更新操作：只同步变更的枚举值（真正的增量同步）
            if enum_value_ids:
                # 查询变更的枚举值及其字段信息
                enum_rows = await self.db.fetch(
                    """
                    SELECT
                        ev.enum_value_id,
                        ev.original_value,
                        ev.display_value,
                        ev.synonyms,
                        ev.frequency,
                        ev.is_active,
                        f.field_id,
                        f.display_name AS field_name,
                        f.description AS field_description,
                        t.domain_id,  -- domain_id 来自 db_tables 表，字段属于表，表属于业务域
                        t.table_id,
                        t.display_name AS table_display_name,
                        t.table_name,
                        t.schema_name,
                        d.domain_id AS domain_id_ref,
                        c.column_name,
                        c.distinct_count
                    FROM field_enum_values ev
                    JOIN fields f ON ev.field_id = f.field_id
                    JOIN db_columns c ON f.source_column_id = c.column_id
                    JOIN db_tables t ON c.table_id = t.table_id
                    LEFT JOIN business_domains d ON t.domain_id = d.domain_id
                    WHERE ev.enum_value_id = ANY($1::uuid[])
                      AND f.connection_id = $2
                      AND t.is_included = TRUE
                      AND f.field_type = 'dimension'
                      AND f.is_active = TRUE
                """,
                    enum_value_ids,
                    connection_id,
                )

                if not enum_rows:
                    logger.warning("未找到需要同步的枚举值", enum_value_ids=enum_value_ids)
                    return True

                value_ids = [row["enum_value_id"] for row in enum_rows]
                entities = await build_enum_entities(enum_rows, self.embedding, connection_id)

                if entities:
                    incremental_upsert_to_milvus(
                        self.milvus,
                        collection_name,
                        entities,
                        value_ids,
                        connection_id,
                        id_field="value_id",
                    )
                    logger.debug(
                        "增量同步枚举值完成",
                        connection_id=str(connection_id),
                        enums=len(entities),
                        deleted=len(delete_ids),
                    )

            return True

        except Exception as e:
            logger.error("同步枚举值变更失败", connection_id=str(connection_id), error=str(e))
            return False

    async def _sync_few_shot_changes(self, connection_id: UUID,
                                     changes: List[PendingChange]) -> bool:
        """同步Few-Shot问答样本变更（增量）"""
        if not changes:
            return True

        if not self.milvus or not self.embedding:
            logger.warning("Milvus或Embedding服务未配置，无法同步Few-Shot样本", connection_id=str(connection_id))
            return False

        try:
            latest_changes = self._latest_change_by_entity(changes)
            upsert_ids = [entity_id for entity_id, change in latest_changes.items()
                          if change.operation != OperationType.DELETE]
            delete_ids: Set[UUID] = {entity_id for entity_id, change in latest_changes.items()
                                     if change.operation == OperationType.DELETE}

            from server.api.admin.milvus import ensure_few_shot_collection_exists
            collection_name = ensure_few_shot_collection_exists(self.milvus, recreate=False)

            entities = []

            if upsert_ids:
                rows = await self.db.fetch(
                    """
                    SELECT
                        sample_id,
                        question,
                        sql_text,
                        ir_json,
                        tables_json,
                        tables,
                        domain_id,
                        quality_score,
                        source_tag,
                        COALESCE(metadata->>'sample_type', 'standard') AS sample_type,
                        COALESCE(metadata->>'sql_context', sql_text) AS sql_context,
                        metadata->>'error_msg' AS error_msg,
                        metadata,
                        last_verified_at,
                        is_active
                    FROM qa_few_shot_samples
                    WHERE sample_id = ANY($1::uuid[])
                      AND connection_id = $2
                    """,
                    upsert_ids,
                    connection_id,
                )

                if rows:
                    inactive_ids = {row['sample_id'] for row in rows if not row['is_active']}
                    delete_ids.update(inactive_ids)

                    active_rows = [row for row in rows if row['is_active']]
                    if active_rows:
                        entities = await build_few_shot_entities(
                            active_rows,
                            self.embedding,
                            connection_id,
                        )
                        if entities:
                            incremental_upsert_to_milvus(
                                self.milvus,
                                collection_name,
                                entities,
                                [row["sample_id"] for row in active_rows],
                                connection_id,
                                id_field="sample_id",
                            )

            if delete_ids:
                for sample_id in delete_ids:
                    try:
                        filter_expr = f'sample_id == "{str(sample_id)}"'
                        await asyncio.to_thread(
                            self.milvus.delete,
                            collection_name=collection_name,
                            filter=filter_expr,
                        )
                    except Exception as exc:
                        logger.debug(
                            "删除Few-Shot样本失败",
                            connection_id=str(connection_id),
                            sample_id=str(sample_id),
                            error=str(exc)
                        )

            return True

        except Exception as e:
            logger.error("同步Few-Shot样本变更失败", connection_id=str(connection_id), error=str(e))
            return False

    @staticmethod
    def _latest_change_by_entity(changes: List[PendingChange]) -> Dict[UUID, PendingChange]:
        """按实体保留最新的一条变更记录"""
        latest: Dict[UUID, PendingChange] = {}
        for change in changes:
            latest[change.entity_id] = change
        return latest

    async def _delete_milvus_entities(self, collection_name: str, connection_id: UUID,
                                      item_ids: Set[str]):
        """从Milvus中删除指定实体"""
        if not item_ids or not self.milvus:
            return

        for item_id in item_ids:
            try:
                if not item_id:
                    continue
                filter_expr = f'connection_id == "{str(connection_id)}" and item_id == "{item_id}"'
                await asyncio.to_thread(
                    self.milvus.delete,
                    collection_name=collection_name,
                    filter=filter_expr,
                )
            except Exception as exc:
                logger.debug(
                    "删除Milvus实体失败",
                    connection_id=str(connection_id),
                    item_id=item_id,
                    error=str(exc)
                )

    async def _mark_changes_synced(self, changes: List[PendingChange]):
        """标记变更为已同步"""
        change_ids = [c.change_id for c in changes]
        connection_id = changes[0].connection_id if changes else None
        await self._mark_change_ids_synced(change_ids, connection_id)

    async def _mark_change_ids_synced(self, change_ids: List[UUID],
                                      connection_id: Optional[UUID] = None):
        if not change_ids:
            return

        try:
            current_time = now_with_tz()
            await self.db.execute("""
                UPDATE milvus_pending_changes
                SET sync_status = 'synced', synced_at = $1
                WHERE change_id = ANY($2)
            """, current_time, change_ids)

            logger.debug("标记变更已同步", count=len(change_ids))

            if connection_id:
                await self._broadcast_pending_changes(connection_id)

        except Exception as e:
            logger.error("标记变更状态失败", error=str(e))

    async def _mark_change_ids_syncing(self, change_ids: List[UUID], sync_id: UUID):
        if not change_ids:
            return
        try:
            await self.db.execute("""
                UPDATE milvus_pending_changes
                SET sync_status = 'syncing', sync_id = $1, updated_at = NOW()
                WHERE change_id = ANY($2)
            """, sync_id, change_ids)
        except Exception as e:
            logger.error("标记变更为同步中失败", sync_id=str(sync_id), error=str(e))

    async def _get_pending_change_ids(self, connection_id: UUID) -> List[UUID]:
        """获取指定连接当前处于待同步/同步中的变更ID（用于全量同步后清空待办）"""
        try:
            rows = await self.db.fetch("""
                SELECT change_id
                FROM milvus_pending_changes
                WHERE connection_id = $1
                  AND sync_status IN ('pending', 'syncing')
            """, connection_id)
            return [row['change_id'] for row in rows]
        except Exception as e:
            logger.error("获取待同步变更ID失败", connection_id=str(connection_id), error=str(e))
            return []

    async def _broadcast_pending_changes(self, connection_id: UUID):
        try:
            stats_rows = await self.db.fetch("""
                SELECT entity_type, COUNT(*) AS count
                FROM milvus_pending_changes
                WHERE connection_id = $1 AND sync_status = 'pending'
                GROUP BY entity_type
            """, connection_id)

            total_count = sum(row['count'] for row in stats_rows) if stats_rows else 0
            stats = {row['entity_type']: row['count'] for row in stats_rows}

            preview_rows = await self.db.fetch("""
                SELECT change_id, entity_type, entity_id, operation, created_at
                FROM milvus_pending_changes
                WHERE connection_id = $1 AND sync_status = 'pending'
                ORDER BY created_at ASC
                LIMIT 5
            """, connection_id)

            preview = [
                {
                    "change_id": str(row['change_id']),
                    "entity_type": row['entity_type'],
                    "entity_id": str(row['entity_id']),
                    "operation": row['operation'],
                    "created_at": row['created_at'].isoformat() if row['created_at'] else None
                }
                for row in preview_rows
            ]

            overall_stats = stats
            overall_total = total_count
            try:
                if self.auto_trigger:
                    full_stats = await self.auto_trigger.get_pending_changes_count(connection_id)
                    if full_stats:
                        overall_stats = {k: int(v) for k, v in full_stats.items()}
                        overall_total = sum(overall_stats.values())
            except Exception as exc:
                logger.debug("获取总体待同步统计失败，使用当前快照",
                             connection_id=str(connection_id),
                             error=str(exc))

            payload = {
                "total_count": total_count,
                "stats": stats,
                "preview": preview,
                "overall_stats": overall_stats,
                "overall_total": overall_total
            }

            await sync_event_broadcaster.broadcast_pending_changes_update(
                str(connection_id),
                payload
            )

        except Exception as e:
            logger.debug("广播待同步变更失败", connection_id=str(connection_id), error=str(e))

    async def _update_sync_progress(self, sync_id: UUID, step: str, percentage: int):
        """更新同步进度"""
        try:
            await self.db.execute("""
                UPDATE milvus_sync_history
                SET current_step = $1, progress_percentage = $2
                WHERE sync_id = $3
            """, step, percentage, sync_id)

            logger.debug(
                "更新同步进度",
                sync_id=str(sync_id),
                step=step,
                percentage=percentage
            )

            # 广播进度更新
            try:
                # 获取连接ID
                sync_record = await self.db.fetchrow("""
                    SELECT connection_id FROM milvus_sync_history WHERE sync_id = $1
                """, sync_id)

                if sync_record:
                    connection_id = str(sync_record['connection_id'])
                    progress_data = {
                        "step": step,
                        "percentage": percentage
                    }
                    await sync_event_broadcaster.broadcast_sync_progress(
                        connection_id, str(sync_id), progress_data
                    )
            except Exception as e:
                logger.warning("广播进度更新失败", error=str(e))

        except Exception as e:
            logger.error("更新同步进度失败", error=str(e))

    async def _update_sync_completion(self, sync_id: UUID, success: bool,
                                      total_entities_override: Optional[int] = None,
                                      synced_entities_override: Optional[int] = None):
        """更新同步完成状态，可传入覆盖的实体统计"""
        try:
            if total_entities_override is not None:
                total_entities = total_entities_override
                synced_entities = synced_entities_override if synced_entities_override is not None else total_entities_override
            else:
                # 统计同步的实体数量（基于待同步队列）
                total_entities = await self.db.fetchval("""
                    SELECT COUNT(*) FROM milvus_pending_changes
                    WHERE sync_id = $1
                """, sync_id)

                synced_entities = await self.db.fetchval("""
                    SELECT COUNT(*) FROM milvus_pending_changes
                    WHERE sync_id = $1 AND sync_status = 'synced'
                """, sync_id)

            failed_entities = max(0, total_entities - synced_entities)

            # 使用当前时间计算持续时间
            current_time = now_with_tz()
            await self.db.execute("""
                UPDATE milvus_sync_history
                SET status = $1,
                    completed_at = $2,
                    duration_seconds = EXTRACT(EPOCH FROM ($2 - started_at))::INTEGER,
                    total_entities = $3,
                    synced_entities = $4,
                    failed_entities = $5,
                    progress_percentage = 100
                WHERE sync_id = $6
            """,
                SyncStatus.COMPLETED.value if success else SyncStatus.FAILED.value,
                current_time,
                total_entities,
                synced_entities,
                failed_entities,
                sync_id
            )

        except Exception as e:
            logger.error("更新同步完成状态失败", error=str(e))

    async def _mark_sync_failed(self, sync_id: UUID, error_message: str):
        """标记同步失败"""
        try:
            current_time = now_with_tz()
            await self.db.execute("""
                UPDATE milvus_sync_history
                SET status = 'failed', error_message = $1, completed_at = $2
                WHERE sync_id = $3
            """, error_message, current_time, sync_id)

        except Exception as e:
            logger.error("标记同步失败状态失败", error=str(e))

    async def _status_monitor(self):
        """状态监控器 - 监控运行中的任务"""
        while True:
            try:
                # 检查超时的任务
                await self._check_timeout_tasks()

                # 清理过期的锁
                await self.concurrency.cleanup_expired_locks()

                await asyncio.sleep(30)  # 每30秒检查一次

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("状态监控异常", error=str(e))
                await asyncio.sleep(30)

    async def _check_timeout_tasks(self):
        """检查超时的任务"""
        try:
            # 查找运行时间过长的任务
            timeout_threshold = now_with_tz() - timedelta(seconds=self.default_timeout)

            timeout_tasks = await self.db.fetch("""
                SELECT sync_id, connection_id, started_at
                FROM milvus_sync_history
                WHERE status = 'running' AND started_at < $1
            """, timeout_threshold)

            for task in timeout_tasks:
                logger.warning(
                    "同步任务超时",
                    sync_id=str(task['sync_id']),
                    connection_id=str(task['connection_id']),
                    started_at=task['started_at']
                )

                # 标记为失败
                await self._mark_sync_failed(
                    task['sync_id'],
                    "同步任务超时"
                )

                # 释放资源
                await self.resource_manager.release_resources(
                    task['connection_id'],
                    SyncStatus.FAILED,
                    {"reason": "timeout"}
                )

                # 取消运行中的协程
                if task['sync_id'] in self.running_tasks:
                    task_coro = self.running_tasks[task['sync_id']]
                    if not task_coro.done():
                        task_coro.cancel()
                    del self.running_tasks[task['sync_id']]

        except Exception as e:
            logger.error("检查超时任务失败", error=str(e))

    async def _check_need_full_sync(self, connection_id: UUID) -> bool:
        """检查是否需要全量同步（首次同步或集合重建后）"""
        try:
            # 1. 检查是否从未同步过
            last_sync = await self.db.fetchval("""
                SELECT started_at
                FROM milvus_sync_history
                WHERE connection_id = $1 AND status IN ('running', 'completed')
                ORDER BY started_at DESC
                LIMIT 1
            """, connection_id)

            if not last_sync:
                logger.debug("检测到首次同步，需要全量同步", connection_id=str(connection_id))
                return True

            # 2. 检查Milvus集合是否存在数据
            if self.milvus:
                try:
                    # 检查主集合
                    collection_name = getattr(self.milvus, 'collection_name', 'semantic_metadata')
                    collections = self.milvus.list_collections()
                    if collection_name in collections:
                        result = self.milvus.query(
                            collection_name=collection_name,
                            filter=f'connection_id == "{str(connection_id)}"',
                            output_fields=["id"],
                            limit=1
                        )
                        if not result or len(result) == 0:
                            logger.debug("主集合为空，需要全量同步", connection_id=str(connection_id))
                            return True

                    # 检查枚举值集合
                    enum_collection = "enum_values_dual"
                    if enum_collection in collections:
                        result = self.milvus.query(
                            collection_name=enum_collection,
                            filter=f'connection_id == "{str(connection_id)}"',
                            output_fields=["id"],
                            limit=1
                        )
                        if not result or len(result) == 0:
                            logger.debug("枚举值集合为空，需要全量同步", connection_id=str(connection_id))
                            return True

                except Exception as e:
                    logger.warning("检查Milvus集合数据失败，倾向于全量同步",
                                   connection_id=str(connection_id), error=str(e))
                    return True

            # 3. 检查元数据库是否有数据但未同步
            domain_count = await self.db.fetchval("""
                SELECT COUNT(*) FROM business_domains
                WHERE connection_id = $1 AND is_active = TRUE
            """, connection_id)

            table_count = await self.db.fetchval("""
                SELECT COUNT(*) FROM db_tables
                WHERE connection_id = $1 AND is_included = TRUE
            """, connection_id)

            enum_count = await self.db.fetchval("""
                SELECT COUNT(*) FROM field_enum_values ev
                JOIN fields f ON ev.field_id = f.field_id
                WHERE f.connection_id = $1 AND ev.is_active = TRUE
            """, connection_id)

            if domain_count > 0 or table_count > 0 or enum_count > 0:
                logger.debug(
                    "元数据库有数据但Milvus为空，需要全量同步",
                    connection_id=str(connection_id),
                    domains=domain_count,
                    tables=table_count,
                    enums=enum_count
                )
                return True

            return False

        except Exception as e:
            logger.error("检查是否需要全量同步失败", connection_id=str(connection_id), error=str(e))
            # 出错时倾向于执行全量同步以确保数据一致性
            return True

    async def _sync_enum_values_direct(self, connection_id: UUID,
                                       *, recreate_collection: bool = False) -> Dict[str, Any]:
        """直接执行枚举值同步（内部调用版本）"""
        try:
            if not self.milvus:
                return {"success": False, "message": "Milvus客户端未配置"}

            if not self.embedding:
                return {"success": False, "message": "Embedding客户端未配置"}

            enum_rows = await fetch_enums_for_sync(self.db, connection_id)
            if not enum_rows:
                return {
                    "success": True,
                    "message": "没有需要同步的字段",
                    "stats": {"fields": 0, "enums": 0, "vectors": 0},
                }

            entities = await build_enum_entities(enum_rows, self.embedding, connection_id)
            stats = {
                "fields": len({row["field_id"] for row in enum_rows}),
                "enums": len(entities),
                "vectors": len(entities) * 2,
            }

            if entities:
                collection_name = ensure_enum_collection_exists(self.milvus, recreate=recreate_collection)
                upsert_to_milvus(
                    self.milvus,
                    collection_name,
                    entities,
                    connection_id,
                    delete_before_insert=True,
                )

            field_ids = {row["field_id"] for row in enum_rows}
            if field_ids:
                await self.db.execute(
                    """
                    UPDATE field_enum_values
                    SET is_synced_to_milvus = TRUE,
                        last_synced_at = NOW()
                    WHERE field_id = ANY($1::uuid[])
                      AND is_active = TRUE
                    """,
                    list(field_ids),
                )

            return {
                "success": True,
                "message": "枚举值同步完成",
                "stats": stats,
                "total_entities": len(entities),
            }

        except Exception as e:
            logger.exception("枚举值同步失败")
            return {
                "success": False,
                "message": f"枚举值同步失败: {str(e)}"
            }

    async def _sync_few_shot_full(
        self,
        connection_id: UUID,
        *,
        recreate_collection: bool = False,
        min_quality_score: Optional[float] = None,
        limit: Optional[int] = None,
        domain_ids: Optional[List[UUID]] = None,
        only_verified: bool = False,
        include_inactive: bool = False
    ) -> Dict[str, Any]:
        """直接执行Few-Shot样本的全量同步"""
        if not self.milvus:
            return {"success": False, "message": "Milvus客户端未配置"}
        if not self.embedding:
            return {"success": False, "message": "Embedding客户端未配置"}

        try:
            threshold = (
                min_quality_score
                if min_quality_score is not None
                else RetrievalConfig.few_shot_min_quality_score()
            )

            rows = await fetch_few_shots_for_sync(
                self.db,
                connection_id,
                min_quality_score=threshold,
                include_inactive=include_inactive,
                only_verified=only_verified,
                domain_ids=domain_ids,
                limit=limit,
            )
            if not rows:
                return {
                    "success": True,
                    "message": "没有符合条件的Few-Shot样本",
                    "stats": {"samples": 0}
                }

            entities = await build_few_shot_entities(rows, self.embedding, connection_id)

            if not entities:
                return {
                    "success": True,
                    "message": "所有Few-Shot样本在Embedding阶段被过滤",
                    "stats": {"samples": 0}
                }

            from server.api.admin.milvus import ensure_few_shot_collection_exists

            collection_name = ensure_few_shot_collection_exists(
                self.milvus,
                recreate=recreate_collection
            )

            upsert_to_milvus(
                self.milvus,
                collection_name,
                entities,
                connection_id,
                delete_before_insert=True
            )

            logger.info(
                "Few-Shot样本同步完成",
                connection_id=str(connection_id),
                samples=len(entities)
            )

            return {
                "success": True,
                "message": "Few-Shot样本同步完成",
                "stats": {"samples": len(entities)}
            }

        except Exception as e:
            logger.exception("Few-Shot样本同步失败", connection_id=str(connection_id))
            return {
                "success": False,
                "message": f"Few-Shot样本同步失败: {str(e)}"
            }
