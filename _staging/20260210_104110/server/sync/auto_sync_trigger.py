"""
自动同步触发服务 - 基于PostgreSQL LISTEN/NOTIFY
实时监听元数据库变更，触发Milvus同步
"""

import asyncio
import json
import structlog
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Set
from uuid import UUID
from dataclasses import asdict

import asyncpg

from server.models.sync import (
    SyncType, TriggeredBy, SyncStatus, EntitySyncStatus,
    EntityType, OperationType, PendingChange, SyncHistory,
    SyncConfig, EntityChange, SyncHealthStatus
)
from server.config import settings
from server.sync.auto_sync_policy import filter_auto_enabled_changes, is_auto_mode
from server.api.admin.milvus import (
    get_milvus_client as admin_get_milvus_client,
    get_embedding_client as admin_get_embedding_client
)

logger = structlog.get_logger()


class AutoSyncTrigger:
    """自动同步触发器 - 基于PostgreSQL LISTEN/NOTIFY

    功能：
    1. 实时监听数据库变更通知
    2. 批量合并变更
    3. 触发同步任务
    4. 防重复触发
    5. 自动重连机制
    """

    def __init__(self, db_pool: asyncpg.Pool, redis_client=None):
        """
        初始化自动同步触发器

        Args:
            db_pool: 数据库连接池
            redis_client: Redis客户端（用于分布式锁）
        """
        self.db = db_pool
        self.redis = redis_client
        self.running = False
        self._listen_task = None
        self._reconnect_task = None
        self._listen_conn = None  # LISTEN专用连接

        # 配置参数
        self.batch_window = settings.sync_batch_window_seconds
        self.max_batch_size = settings.sync_milvus_batch_size  # 最大批量大小

        # 通知队列和批量处理
        self._notification_queue = asyncio.Queue()
        self._pending_changes: Dict[UUID, List[EntityChange]] = {}  # connection_id -> changes
        self._batch_timers: Dict[UUID, asyncio.Task] = {}  # connection_id -> timer task

        # 连接管理
        self._reconnect_interval = 5  # 重连间隔（秒）
        self._max_reconnect_attempts = 10  # 最大重连次数

        # 监听回调函数
        self._listen_callback = None

        # 数据库连接配置（用于LISTEN专用连接）
        self._db_config = None
        
        # 同步服务引用
        self._sync_service = None
        self._concurrency_controller = None

    def set_sync_service(self, sync_service, concurrency_controller):
        """
        设置同步服务引用（依赖注入）
        
        Args:
            sync_service: UnifiedSyncService实例
            concurrency_controller: 并发控制器实例
        """
        self._sync_service = sync_service
        self._concurrency_controller = concurrency_controller
        logger.debug("已注入同步服务依赖")

    async def start(self):
        """启动自动同步触发器"""
        if self.running:
            logger.warning("自动同步触发器已在运行中")
            return

        # 获取数据库连接配置（用于创建专用LISTEN连接）
        await self._get_db_config()

        self.running = True

        # 启动LISTEN任务
        self._listen_task = asyncio.create_task(self._listen_with_reconnect())

        # 启动通知处理任务
        asyncio.create_task(self._process_notifications())

        logger.info("自动同步触发器已启动（LISTEN/NOTIFY模式）")

    async def _get_db_config(self):
        """获取数据库连接配置"""
        try:
            # 直接使用PostgreSQL环境变量构建连接配置
            import os
            from server.config import settings

            # 使用PostgreSQL配置而不是旧的SQL Server配置
            pg_host = getattr(settings, 'postgres_host', 'localhost')
            pg_port = getattr(settings, 'postgres_port', '5432')
            pg_db = getattr(settings, 'postgres_db', 'NL2SQL_metadata')
            pg_user = getattr(settings, 'postgres_user', 'NL2SQL_user')
            pg_password = getattr(settings, 'postgres_password', None)
            if not pg_password:
                raise ValueError("缺少 PostgreSQL 密码配置（POSTGRES_PASSWORD）")

            # 构建PostgreSQL连接参数字典
            self._db_config = {
                'host': pg_host,
                'port': int(pg_port) if isinstance(pg_port, str) else pg_port,
                'database': pg_db,
                'user': pg_user,
                'password': pg_password
            }

            # 安全地记录连接信息（隐藏密码）
            logger.debug("已获取PostgreSQL连接配置", host=pg_host, port=pg_port, database=pg_db)

        except Exception as e:
            logger.error("获取数据库连接配置失败", error=str(e))
            # 使用默认PostgreSQL配置
            import os
            self._db_config = {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'port': int(os.getenv('POSTGRES_PORT', '5432')),
                'database': os.getenv('POSTGRES_DB', 'NL2SQL_metadata'),
                'user': os.getenv('POSTGRES_USER', 'NL2SQL_user'),
                'password': os.getenv('POSTGRES_PASSWORD')
            }
            if not self._db_config.get('password'):
                raise ValueError("缺少 PostgreSQL 密码配置（POSTGRES_PASSWORD）")

    async def stop(self):
        """停止自动同步触发器"""
        if not self.running:
            return

        self.running = False

        # 停止所有任务
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass

        # 停止所有批量定时器
        for timer_task in self._batch_timers.values():
            timer_task.cancel()
            try:
                await timer_task
            except asyncio.CancelledError:
                pass

        # 关闭LISTEN连接
        if self._listen_conn:
            await self._listen_conn.close()

        logger.info("自动同步触发器已停止")

    async def _listen_with_reconnect(self):
        """带重连机制的LISTEN任务（优化版）"""
        reconnect_attempts = 0
        last_successful_connection = None

        while self.running and reconnect_attempts < self._max_reconnect_attempts:
            try:
                # 创建专用的数据库连接用于LISTEN（不从连接池获取）
                self._listen_conn = await asyncpg.connect(
                    **self._db_config,
                    timeout=settings.db_connect_timeout,
                    command_timeout=settings.db_command_timeout
                )

                # 添加通知监听器
                await self._listen_conn.add_listener('milvus_sync_changes', self._notification_callback)

                # 设置LISTEN
                await self._listen_conn.execute("LISTEN milvus_sync_changes")
                logger.debug("已设置数据库LISTEN监听", channel="milvus_sync_changes")

                # 重置重连计数
                reconnect_attempts = 0
                last_successful_connection = asyncio.get_event_loop().time()

                # 保持连接活跃并等待通知
                consecutive_errors = 0
                while self.running:
                    try:
                        # 定期发送心跳查询以保持连接活跃
                        await self._listen_conn.execute("SELECT 1")
                        consecutive_errors = 0  # 重置错误计数
                        
                        # 动态心跳间隔：短时间内成功则延长间隔
                        current_time = asyncio.get_event_loop().time()
                        if last_successful_connection and (current_time - last_successful_connection) > 300:
                            # 连接稳定超过5分钟，心跳间隔延长到60秒
                            await asyncio.sleep(60)
                        else:
                            # 新连接或刚恢复，保持30秒心跳
                            await asyncio.sleep(30)

                    except asyncio.CancelledError:
                        logger.debug("LISTEN任务被取消")
                        break
                    except Exception as e:
                        consecutive_errors += 1
                        logger.warning(
                            "心跳查询失败",
                            error=str(e),
                            consecutive_errors=consecutive_errors
                        )
                        
                        # 连续失败3次，主动断开重连
                        if consecutive_errors >= 3:
                            logger.warning("连续心跳失败，触发重连")
                            break
                        
                        # 短暂等待后重试
                        await asyncio.sleep(5)

            except asyncpg.PostgresConnectionError as e:
                logger.error(
                    "数据库LISTEN连接失败",
                    attempt=f"{reconnect_attempts + 1}/{self._max_reconnect_attempts}",
                    error=str(e)
                )
                reconnect_attempts += 1

            except Exception as e:
                logger.error(
                    "LISTEN任务异常",
                    attempt=f"{reconnect_attempts + 1}/{self._max_reconnect_attempts}",
                    error=str(e),
                    error_type=type(e).__name__
                )
                reconnect_attempts += 1

            finally:
                if self.running:
                    # 清理连接
                    await self._cleanup_listen_connection()

                    # 指数退避重连策略
                    if reconnect_attempts < self._max_reconnect_attempts:
                        backoff = min(self._reconnect_interval * (2 ** (reconnect_attempts - 1)), 60)
                        logger.debug(f"等待 {backoff}秒 后重连...")
                        await asyncio.sleep(backoff)

        if reconnect_attempts >= self._max_reconnect_attempts:
            logger.error("达到最大重连次数，停止LISTEN任务")

    async def _cleanup_listen_connection(self):
        """清理LISTEN连接"""
        if self._listen_conn:
            try:
                if not self._listen_conn.is_closed():
                    # 移除监听器
                    try:
                        await self._listen_conn.remove_listener('milvus_sync_changes', self._notification_callback)
                    except:
                        pass
                    # 关闭连接
                    await self._listen_conn.close()
            except Exception as e:
                logger.error("清理LISTEN连接失败", error=str(e))
            finally:
                self._listen_conn = None

    def _notification_callback(self, connection, pid, channel, payload):
        """数据库通知回调函数"""
        try:
            logger.debug(
                "收到数据库通知",
                channel=channel,
                payload=payload,
                pid=pid
            )

            # 将通知添加到队列进行异步处理
            asyncio.create_task(self._queue_notification(payload))

        except Exception as e:
            logger.error("处理数据库通知回调失败", error=str(e), payload=payload)

    async def _queue_notification(self, payload: str):
        """将通知添加到队列"""
        try:
            # 解析通知内容
            notification_data = json.loads(payload)
            connection_id = UUID(notification_data['connection_id'])
            entity_type = notification_data['entity_type']
            entity_id = UUID(notification_data['entity_id'])
            operation = notification_data['operation']
            timestamp = float(notification_data['timestamp'])

            # 映射表名到实体类型
            entity_type_mapping = {
                'business_domains': 'domain',
                'db_tables': 'table',
                'fields': 'field',
                'field_enum_values': 'enum',
                'qa_few_shot_samples': 'few_shot',
                'few_shot': 'few_shot'
            }

            mapped_entity_type = entity_type_mapping.get(entity_type.lower(), entity_type.lower())

            # 验证实体类型是否有效
            try:
                entity_type_enum = EntityType(mapped_entity_type)
            except ValueError as e:
                logger.error(f"无效的实体类型: {mapped_entity_type}, 原始类型: {entity_type}")
                # 如果映射失败，尝试直接使用原始类型
                try:
                    entity_type_enum = EntityType(entity_type.lower())
                except ValueError:
                    logger.error(f"原始实体类型也无效: {entity_type}")
                    return

            # 创建实体变更对象
            entity_change = EntityChange(
                entity_type=entity_type_enum,
                entity_id=entity_id,
                operation=OperationType(operation),
                old_data=None,  # 通知中不包含详细数据
                new_data=None,  # 可以从数据库获取
                changed_at=datetime.fromtimestamp(timestamp)
            )

            # 添加到队列
            await self._notification_queue.put((connection_id, entity_change))

            logger.debug(
                "已添加通知到处理队列",
                connection_id=str(connection_id),
                entity_type=entity_type,
                entity_id=str(entity_id),
                operation=operation
            )

        except Exception as e:
            logger.error("处理通知队列失败", error=str(e), payload=payload)

    
    async def _process_notifications(self):
        """处理通知队列"""
        while self.running:
            try:
                # 获取通知
                connection_id, entity_change = await asyncio.wait_for(
                    self._notification_queue.get(),
                    timeout=settings.sync_notification_queue_timeout
                )

                # 添加到待处理变更
                if connection_id not in self._pending_changes:
                    self._pending_changes[connection_id] = []

                self._pending_changes[connection_id].append(entity_change)

                # 启动或重置批量处理定时器
                await self._schedule_batch_processing(connection_id)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("处理通知队列失败", error=str(e))

    async def _schedule_batch_processing(self, connection_id: UUID):
        """调度批量处理"""
        # 如果已有定时器，取消它
        if connection_id in self._batch_timers:
            self._batch_timers[connection_id].cancel()

        # 创建新的定时器
        timer_task = asyncio.create_task(
            self._batch_delayed_process(connection_id)
        )
        self._batch_timers[connection_id] = timer_task

    async def _batch_delayed_process(self, connection_id: UUID):
        """延迟批量处理"""
        try:
            # 等待批量窗口
            await asyncio.sleep(self.batch_window)

            # 处理该连接的所有待处理变更
            await self._process_pending_changes(connection_id)

        except asyncio.CancelledError:
            # 被取消是正常的（有新的变更到达）
            pass
        except Exception as e:
            logger.error("批量处理失败", connection_id=str(connection_id), error=str(e))
        finally:
            # 清理定时器
            if connection_id in self._batch_timers:
                del self._batch_timers[connection_id]

    async def _process_pending_changes(self, connection_id: UUID):
        """处理待同步变更"""
        try:
            # 获取待处理变更
            changes = self._pending_changes.pop(connection_id, [])
            if not changes:
                return

            logger.debug(
                "开始处理批量变更",
                connection_id=str(connection_id),
                changes_count=len(changes)
            )

            # 1. 检查是否有锁（防止重复同步）
            if await self._is_sync_locked(connection_id):
                logger.debug("连接正在同步中，跳过", connection_id=str(connection_id))
                # 将变更重新放回队列稍后处理
                self._pending_changes[connection_id] = changes
                await asyncio.sleep(5)  # 等待5秒后重新调度
                await self._schedule_batch_processing(connection_id)
                return

            # 2. 检查是否启用自动同步
            config = await self._get_sync_config(connection_id)
            if not config or not config.auto_sync_enabled:
                logger.debug("自动同步未启用", connection_id=str(connection_id))
                return
            if not is_auto_mode(config):
                logger.debug(
                    "连接处于手动同步模式，自动触发跳过",
                    connection_id=str(connection_id)
                )
                return

            # 3. 检查最小同步间隔
            if await self._is_too_frequent(connection_id, config.min_sync_interval_seconds):
                logger.debug("同步过于频繁，跳过", connection_id=str(connection_id))
                return

            # 4. 从数据库获取完整的待同步变更
            pending_changes = await self._get_pending_changes_from_db(connection_id)
            if not pending_changes:
                logger.debug("没有待同步变更", connection_id=str(connection_id))
                return

            auto_changes = filter_auto_enabled_changes(pending_changes, config)
            if not auto_changes:
                logger.debug(
                    "无符合策略的变更，保持待同步状态",
                    connection_id=str(connection_id)
                )
                return

            # 5. 创建同步任务
            sync_id = await self._create_sync_task(connection_id, auto_changes)
            if sync_id:
                # 6. 标记变更为同步中
                await self._mark_changes_syncing(connection_id, auto_changes, sync_id)

                # 7. 触发同步执行
                await self._trigger_sync_execution(sync_id)

        except Exception as e:
            logger.error(
                "处理待同步变更失败",
                connection_id=str(connection_id),
                error=str(e)
            )

    async def _get_pending_changes_from_db(self, connection_id: UUID) -> List[PendingChange]:
        """从数据库获取待同步变更"""
        try:
            async with self.db.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT
                        change_id, connection_id, entity_type, entity_id,
                        operation, old_data, new_data, created_at,
                        updated_at, sync_status, sync_id, synced_at, priority
                    FROM milvus_pending_changes
                    WHERE connection_id = $1 AND sync_status = 'pending'
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
                    LIMIT $2
                """, connection_id, self.max_batch_size)

                changes = []
                for row in rows:
                    # 将数据库表名映射到实体类型枚举
                    entity_type_str = row['entity_type']
                    entity_type_mapping = {
                        'business_domains': 'domain',
                        'db_tables': 'table',
                        'fields': 'field',
                        'field_enum_values': 'enum',
                        'qa_few_shot_samples': 'few_shot',
                        'few_shot': 'few_shot'
                    }
                    mapped_entity_type = entity_type_mapping.get(entity_type_str.lower(), entity_type_str.lower())
                    try:
                        entity_type_enum = EntityType(mapped_entity_type)
                    except ValueError:
                        logger.error(f"无效的实体类型: {mapped_entity_type}, 原始类型: {entity_type_str}")
                        continue
                    
                    change = PendingChange(
                        change_id=row['change_id'],
                        connection_id=row['connection_id'],
                        entity_type=entity_type_enum,
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

                logger.debug(
                    "从数据库获取待同步变更",
                    connection_id=str(connection_id),
                    count=len(changes)
                )
                return changes

        except Exception as e:
            logger.error(
                "从数据库获取待同步变更失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return []

    # 以下方法保持与原来一致
    async def _is_sync_locked(self, connection_id: UUID) -> bool:
        """检查连接是否被锁定（正在同步中）"""
        if not self.redis:
            # 如果没有Redis，检查数据库中的运行状态
            async with self.db.acquire() as conn:
                running_sync = await conn.fetchval("""
                    SELECT sync_id
                    FROM milvus_sync_history
                    WHERE connection_id = $1 AND status = 'running'
                    LIMIT 1
                """, connection_id)
                return running_sync is not None

        # 使用Redis分布式锁
        lock_key = f"milvus_sync_lock:{connection_id}"
        return self.redis.exists(lock_key)

    async def _get_sync_config(self, connection_id: UUID) -> Optional[SyncConfig]:
        """获取同步配置"""
        try:
            async with self.db.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT
                        config_id, connection_id, auto_sync_enabled,
                        auto_sync_mode, auto_sync_domains, auto_sync_tables,
                        auto_sync_fields, auto_sync_enums, auto_sync_few_shot,
                        batch_window_seconds, max_batch_size, sync_timeout_seconds,
                        domain_priority, table_priority, field_priority, enum_priority,
                        min_sync_interval_seconds, max_retry_attempts, retry_delay_seconds,
                        created_at, updated_at, created_by
                    FROM milvus_sync_config
                    WHERE connection_id = $1
                """, connection_id)

                if not row:
                    return None

                return SyncConfig(
                    config_id=row['config_id'],
                    connection_id=row['connection_id'],
                    auto_sync_enabled=row['auto_sync_enabled'],
                    auto_sync_mode=row['auto_sync_mode'],
                    auto_sync_domains=row['auto_sync_domains'],
                    auto_sync_tables=row['auto_sync_tables'],
                    auto_sync_fields=row['auto_sync_fields'],
                    auto_sync_enums=row['auto_sync_enums'],
                    auto_sync_few_shot=row['auto_sync_few_shot'],
                    batch_window_seconds=row['batch_window_seconds'],
                    max_batch_size=row['max_batch_size'],
                    sync_timeout_seconds=row['sync_timeout_seconds'],
                    domain_priority=row['domain_priority'],
                    table_priority=row['table_priority'],
                    field_priority=row['field_priority'],
                    enum_priority=row['enum_priority'],
                    min_sync_interval_seconds=row['min_sync_interval_seconds'],
                    max_retry_attempts=row['max_retry_attempts'],
                    retry_delay_seconds=row['retry_delay_seconds'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    created_by=row['created_by']
                )
        except Exception as e:
            logger.error(
                "获取同步配置失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return None

    async def _get_vector_service_status(self) -> Dict[str, bool]:
        """检测Milvus与Embedding服务状态"""
        status = {
            "milvus_connected": False,
            "embedding_available": False,
            "collection_exists": False
        }

        try:
            milvus_client = await admin_get_milvus_client()
            if milvus_client:
                status["milvus_connected"] = True
                try:
                    collection_name = getattr(milvus_client, 'collection_name', settings.milvus_collection)
                    collections = milvus_client.list_collections()
                    status["collection_exists"] = collection_name in collections
                except Exception as e:
                    logger.debug("检查Milvus集合失败", error=str(e))
        except Exception as e:
            logger.debug("检测Milvus连接失败", error=str(e))

        try:
            embedding_client = await admin_get_embedding_client()
            status["embedding_available"] = embedding_client is not None
        except Exception as e:
            logger.debug("检测Embedding服务失败", error=str(e))

        return status

    async def _is_too_frequent(self, connection_id: UUID, min_interval: int) -> bool:
        """检查同步是否过于频繁"""
        try:
            async with self.db.acquire() as conn:
                last_sync = await conn.fetchval("""
                    SELECT started_at
                    FROM milvus_sync_history
                    WHERE connection_id = $1 AND status IN ('running', 'completed')
                    ORDER BY started_at DESC
                    LIMIT 1
                """, connection_id)

                if not last_sync:
                    return False

                now = datetime.now(timezone.utc)
                last = last_sync
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                time_since_last_sync = now - last
                return time_since_last_sync.total_seconds() < min_interval

        except Exception as e:
            logger.error(
                "检查同步频率失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return False

    async def _create_sync_task(self, connection_id: UUID, changes: List[PendingChange]) -> Optional[UUID]:
        """创建同步任务"""
        try:
            # 首先验证连接是否存在
            async with self.db.acquire() as conn:
                connection_exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM database_connections 
                        WHERE connection_id = $1 AND is_active = TRUE
                    )
                """, connection_id)
                
                if not connection_exists:
                    logger.error(
                        "连接不存在或未激活，无法创建同步任务",
                        connection_id=str(connection_id)
                    )
                    return None
            
            # 分析变更类型，决定同步策略
            sync_type = self._determine_sync_type(changes)

            # 构建实体变更信息
            entity_changes = []
            for change in changes:
                entity_change = EntityChange(
                    entity_type=change.entity_type,
                    entity_id=change.entity_id,
                    operation=change.operation,
                    old_data=change.old_data,
                    new_data=change.new_data,
                    changed_at=change.created_at
                )
                entity_changes.append(self._serialize_entity_change(entity_change))

            # 创建同步历史记录
            async with self.db.acquire() as conn:
                sync_id = await conn.fetchval("""
                    INSERT INTO milvus_sync_history (
                        connection_id, sync_type, triggered_by, status,
                        total_entities, entity_changes
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING sync_id
                """,
                    connection_id,
                    sync_type.value,
                    TriggeredBy.AUTO.value,
                    SyncStatus.PENDING.value,
                    len(changes),
                    json.dumps(entity_changes)
                )

            logger.debug(
                "创建自动同步任务",
                sync_id=str(sync_id),
                connection_id=str(connection_id),
                sync_type=sync_type.value,
                changes_count=len(changes)
            )
            return sync_id

        except Exception as e:
            # 检查是否是外键约束错误
            error_str = str(e)
            if "foreign key constraint" in error_str.lower() or "violates foreign key" in error_str.lower():
                logger.error(
                    "连接不存在，无法创建同步任务",
                    connection_id=str(connection_id),
                    error="连接ID在 database_connections 表中不存在"
                )
            else:
                logger.error(
                    "创建同步任务失败",
                    connection_id=str(connection_id),
                    error=error_str
                )
            return None

    def _determine_sync_type(self, changes: List[PendingChange]) -> SyncType:
        """根据变更确定同步类型"""
        entity_types = set(c.entity_type for c in changes)

        # 如果有枚举值变更，需要枚举值同步
        if EntityType.ENUM in entity_types:
            return SyncType.ENUMS

        # 如果有业务域或表变更，需要增量同步
        if EntityType.DOMAIN in entity_types or EntityType.TABLE in entity_types:
            return SyncType.INCREMENTAL

        # 否则为增量同步
        return SyncType.INCREMENTAL

    @staticmethod
    def _serialize_entity_change(change: EntityChange) -> dict:
        """将实体变更序列化为可JSON编码的结构"""
        payload = asdict(change)
        for key, value in list(payload.items()):
            if isinstance(value, UUID):
                payload[key] = str(value)
            elif isinstance(value, datetime):
                payload[key] = value.isoformat()
        payload["entity_type"] = change.entity_type.value
        payload["operation"] = change.operation.value
        return payload

    async def _mark_changes_syncing(self, connection_id: UUID, changes: List[PendingChange], sync_id: UUID):
        """标记变更为同步中状态"""
        try:
            change_ids = [c.change_id for c in changes]
            async with self.db.acquire() as conn:
                await conn.execute("""
                    UPDATE milvus_pending_changes
                    SET sync_status = 'syncing', sync_id = $1, updated_at = NOW()
                    WHERE change_id = ANY($2)
                """, sync_id, change_ids)

            logger.debug(
                "标记变更为同步中",
                sync_id=str(sync_id),
                changes_count=len(change_ids)
            )
        except Exception as e:
            logger.error(
                "标记变更状态失败",
                sync_id=str(sync_id),
                error=str(e)
            )

    async def _trigger_sync_execution(self, sync_id: UUID):
        """触发同步执行（将任务加入UnifiedSyncService的队列）"""
        try:
            # 获取连接ID
            async with self.db.acquire() as conn:
                connection_id = await conn.fetchval("""
                    SELECT connection_id FROM milvus_sync_history WHERE sync_id = $1
                """, sync_id)

            if not connection_id:
                logger.error("无法获取连接ID", sync_id=str(sync_id))
                return

            # 将任务加入UnifiedSyncService的队列
            if self._concurrency_controller:
                # 使用并发控制器的队列
                await self._concurrency_controller.enqueue_sync_task(
                    connection_id, sync_id, priority=3  # 自动同步优先级较低
                )
                logger.debug(
                    "自动同步任务已加入队列",
                    sync_id=str(sync_id),
                    connection_id=str(connection_id)
                )
            else:
                # 降级方案：直接更新状态为pending，由队列处理器轮询
                logger.warning(
                    "并发控制器未注入，任务保持pending状态",
                    sync_id=str(sync_id)
                )
                # 任务已经在_create_sync_task中创建为pending状态
                # UnifiedSyncService的_queue_processor会从数据库轮询pending任务

        except Exception as e:
            logger.error(
                "触发同步执行失败",
                sync_id=str(sync_id),
                error=str(e)
            )
            # 更新状态为失败
            try:
                async with self.db.acquire() as conn:
                    await conn.execute("""
                        UPDATE milvus_sync_history
                        SET status = 'failed', error_message = $1, completed_at = NOW()
                        WHERE sync_id = $2
                    """, str(e), sync_id)
            except Exception as update_error:
                logger.error("更新任务失败状态失败", error=str(update_error))

    # 手动触发接口保持不变
    async def trigger_manual_sync(self, connection_id: UUID, force: bool = False) -> Optional[UUID]:
        """手动触发同步"""
        try:
            if not force and await self._is_sync_locked(connection_id):
                logger.warning("连接正在同步中，无法触发手动同步", connection_id=str(connection_id))
                return None

            # 获取所有待同步变更
            pending_changes = await self._get_pending_changes_from_db(connection_id)
            if not pending_changes:
                logger.debug("没有待同步变更", connection_id=str(connection_id))
                return None

            # 强制触发同步（忽略频率限制）
            sync_id = await self._create_sync_task(connection_id, pending_changes)
            if sync_id:
                await self._mark_changes_syncing(connection_id, pending_changes, sync_id)
                await self._trigger_sync_execution(sync_id)

            return sync_id

        except Exception as e:
            logger.error(
                "手动触发同步失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return None

    async def get_pending_changes_count(self, connection_id: UUID) -> Dict[str, int]:
        """获取待同步变更数量"""
        try:
            async with self.db.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT entity_type, COUNT(*) as count
                    FROM milvus_pending_changes
                    WHERE connection_id = $1 AND sync_status = 'pending'
                    GROUP BY entity_type
                """, connection_id)

                result = {}
                for row in rows:
                    result[row['entity_type']] = row['count']

                return result

        except Exception as e:
            logger.error(
                "获取待同步变更数量失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return {}

    async def get_sync_health_status(self, connection_id: UUID) -> Optional[SyncHealthStatus]:
        """获取同步健康状态"""
        try:
            # 获取同步配置
            config = await self._get_sync_config(connection_id)
            if not config:
                return None

            # 获取最近的同步记录
            async with self.db.acquire() as conn:
                last_sync = await conn.fetchrow("""
                    SELECT sync_id, status, started_at, completed_at, error_message
                    FROM milvus_sync_history
                    WHERE connection_id = $1
                    ORDER BY started_at DESC
                    LIMIT 1
                """, connection_id)

            # 获取待同步变更统计
            async with self.db.acquire() as conn:
                pending_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM milvus_pending_changes
                    WHERE connection_id = $1 AND sync_status = 'pending'
                """, connection_id)

            # 获取最老的待同步变更
            async with self.db.acquire() as conn:
                oldest_change = await conn.fetchval("""
                    SELECT MIN(created_at) FROM milvus_pending_changes
                    WHERE connection_id = $1 AND sync_status = 'pending'
                """, connection_id)

            # 检查是否正在同步
            async with self.db.acquire() as conn:
                is_syncing = await conn.fetchval("""
                    SELECT sync_id FROM milvus_sync_history
                    WHERE connection_id = $1 AND status = 'running'
                    LIMIT 1
                """, connection_id)

            # 检查向量服务状态
            vector_status = await self._get_vector_service_status()

            # 计算健康评分
            health_score = self._calculate_health_score(
                last_sync, pending_count, is_syncing
            )

            if not vector_status["milvus_connected"]:
                health_score = min(health_score, 0.2)
            elif not vector_status["collection_exists"]:
                health_score = min(health_score, 0.5)

            if not vector_status["embedding_available"]:
                health_score = min(health_score, 0.6)

            # 确定健康消息和告警级别
            health_message, alert_level = self._determine_health_status(
                health_score, last_sync, pending_count, is_syncing, vector_status
            )

            return SyncHealthStatus(
                connection_id=connection_id,
                auto_sync_enabled=config.auto_sync_enabled,
                milvus_connected=vector_status["milvus_connected"],
                embedding_available=vector_status["embedding_available"],
                collection_ready=vector_status["collection_exists"],
                last_sync_status=SyncStatus(last_sync['status']) if last_sync else None,
                last_sync_time=last_sync['started_at'] if last_sync else None,
                pending_changes_count=pending_count,
                oldest_pending_change=oldest_change,
                is_syncing=is_syncing is not None,
                current_sync_id=is_syncing,
                health_score=health_score,
                health_message=health_message,
                alert_level=alert_level
            )

        except Exception as e:
            logger.error(
                "获取同步健康状态失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return None

    def _calculate_health_score(self, last_sync: dict, pending_count: int, is_syncing: bool) -> float:
        """计算健康评分"""
        score = 1.0

        # 检查最近同步状态
        if last_sync:
            if last_sync['status'] == 'failed':
                score -= 0.3
            elif last_sync['status'] == 'running':
                score -= 0.1
        else:
            score -= 0.2  # 从未同步

        # 检查待同步变更数量
        # 只要有待同步变更，就应该降低健康度
        if pending_count > 100:
            score -= 0.3
        elif pending_count > 50:
            score -= 0.2
        elif pending_count > 10:
            score -= 0.1
        elif pending_count > 0:
            # 即使只有1个待同步变更，也应该降低健康度（至少降低5%）
            score -= 0.05

        # 检查是否正在同步
        if is_syncing:
            score -= 0.05

        return max(0.0, score)

    def _determine_health_status(self, health_score: float, last_sync: dict,
                                pending_count: int, is_syncing: bool,
                                service_status: Optional[Dict[str, bool]] = None) -> tuple:
        """确定健康状态和告警级别"""
        if service_status:
            if not service_status.get("milvus_connected", False):
                return "Milvus未连接", "error"
            if not service_status.get("embedding_available", False):
                return "Embedding服务不可用", "warning"
            if not service_status.get("collection_exists", True):
                return "Milvus集合未准备好", "warning"

        if health_score >= 0.9:
            return "系统运行正常", "normal"
        elif health_score >= 0.7:
            return "系统基本正常", "normal"
        elif health_score >= 0.5:
            return "存在少量待同步变更", "warning"
        elif health_score >= 0.3:
            return "同步存在异常", "warning"
        else:
            return "同步系统异常", "error"
