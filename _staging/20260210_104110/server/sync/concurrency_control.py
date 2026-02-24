"""
同步并发控制机制
基于Redis实现分布式锁和队列管理
"""

import asyncio
import json
import time
import structlog
from typing import Optional, Dict, Any, List
from uuid import UUID
from dataclasses import asdict
from enum import Enum

from server.models.sync import SyncStatus, SyncConfig
from server.config import settings

logger = structlog.get_logger()


class LockType(str, Enum):
    """锁类型"""
    SYNC = "sync"           # 同步锁
    CONFIG = "config"       # 配置锁
    MAINTENANCE = "maintenance"  # 维护锁


class ConcurrencyController:
    """并发控制器

    功能：
    1. 分布式锁管理
    2. 同步任务队列
    3. 资源限制控制
    4. 优先级调度
    """

    def __init__(self, redis_client=None):
        """
        初始化并发控制器

        Args:
            redis_client: Redis客户端
        """
        self.redis = redis_client
        self.local_locks = {}  # 本地锁缓存

        # 配置参数
        self.default_lock_timeout = settings.sync_lock_timeout_seconds  # 默认锁超时时间（秒）
        self.max_concurrent_syncs = settings.sync_max_concurrent    # 最大并发同步数
        self.queue_key_prefix = settings.sync_queue_key_prefix
        self.lock_key_prefix = settings.sync_lock_key_prefix
        self.status_key_prefix = settings.sync_status_key_prefix

    async def acquire_sync_lock(self, connection_id: UUID,
                               timeout: Optional[int] = None,
                               wait: bool = True) -> bool:
        """
        获取同步锁

        Args:
            connection_id: 连接ID
            timeout: 锁超时时间（秒）
            wait: 是否等待锁释放

        Returns:
            bool: 是否成功获取锁
        """
        if not self.redis:
            # 降级到本地锁
            return await self._acquire_local_lock(connection_id, timeout or self.default_lock_timeout)

        lock_key = f"{self.lock_key_prefix}:{LockType.SYNC.value}:{connection_id}"
        timeout = timeout or self.default_lock_timeout

        try:
            # 尝试获取锁
            if wait:
                # 等待锁释放
                start_time = time.time()
                while time.time() - start_time < timeout:
                    if self.redis.set(lock_key, "locked", ex=timeout, nx=True):
                        logger.debug("获取同步锁成功", connection_id=str(connection_id))
                        return True
                    await asyncio.sleep(0.1)
                logger.warning("等待同步锁超时", connection_id=str(connection_id))
                return False
            else:
                # 不等待，直接尝试获取
                if self.redis.set(lock_key, "locked", ex=timeout, nx=True):
                    logger.debug("获取同步锁成功", connection_id=str(connection_id))
                    return True
                else:
                    logger.debug("同步锁已被占用", connection_id=str(connection_id))
                    return False

        except Exception as e:
            logger.error(
                "获取同步锁失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return False

    async def release_sync_lock(self, connection_id: UUID) -> bool:
        """
        释放同步锁

        Args:
            connection_id: 连接ID

        Returns:
            bool: 是否成功释放锁
        """
        if not self.redis:
            # 释放本地锁
            return self._release_local_lock(connection_id)

        lock_key = f"{self.lock_key_prefix}:{LockType.SYNC.value}:{connection_id}"

        try:
            result = self.redis.delete(lock_key)
            if result:
                logger.debug("释放同步锁成功", connection_id=str(connection_id))
                return True
            else:
                logger.warning("同步锁不存在或已过期", connection_id=str(connection_id))
                return False

        except Exception as e:
            logger.error(
                "释放同步锁失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return False

    async def is_sync_locked(self, connection_id: UUID) -> bool:
        """
        检查同步锁状态

        Args:
            connection_id: 连接ID

        Returns:
            bool: 是否被锁定
        """
        if not self.redis:
            return connection_id in self.local_locks

        lock_key = f"{self.lock_key_prefix}:{LockType.SYNC.value}:{connection_id}"
        return self.redis.exists(lock_key)

    async def get_lock_ttl(self, connection_id: UUID) -> int:
        """
        获取锁剩余时间

        Args:
            connection_id: 连接ID

        Returns:
            int: 剩余时间（秒），-1表示永久锁，0表示不存在
        """
        if not self.redis:
            return 0

        lock_key = f"{self.lock_key_prefix}:{LockType.SYNC.value}:{connection_id}"
        return self.redis.ttl(lock_key)

    async def enqueue_sync_task(self, connection_id: UUID, sync_id: UUID,
                               priority: int = 5, delay: int = 0) -> bool:
        """
        将同步任务加入队列

        Args:
            connection_id: 连接ID
            sync_id: 同步任务ID
            priority: 优先级（1-10，数字越小优先级越高）
            delay: 延迟执行时间（秒）

        Returns:
            bool: 是否成功入队
        """
        if not self.redis:
            logger.warning("Redis未配置，无法使用队列功能")
            return False

        try:
            # 构建任务数据
            task_data = {
                "sync_id": str(sync_id),
                "connection_id": str(connection_id),
                "priority": priority,
                "created_at": time.time(),
                "delay": delay
            }

            # 如果有延迟，使用延迟队列
            if delay > 0:
                delayed_key = f"{self.queue_key_prefix}:delayed"
                score = time.time() + delay
                self.redis.zadd(delayed_key, {json.dumps(task_data): score})
            else:
                # 使用优先级队列
                queue_key = f"{self.queue_key_prefix}:priority"
                score = priority * 1000000 + time.time()  # 优先级 + 时间戳
                self.redis.zadd(queue_key, {json.dumps(task_data): score})

            logger.debug(
                "同步任务入队成功",
                sync_id=str(sync_id),
                connection_id=str(connection_id),
                priority=priority,
                delay=delay
            )
            return True

        except Exception as e:
            logger.error(
                "同步任务入队失败",
                sync_id=str(sync_id),
                connection_id=str(connection_id),
                error=str(e)
            )
            return False

    async def dequeue_sync_task(self) -> Optional[Dict[str, Any]]:
        """
        从队列中取出一个同步任务

        Returns:
            dict: 任务数据，如果没有任务则返回None
        """
        if not self.redis:
            return None

        try:
            # 首先处理延迟队列
            delayed_key = f"{self.queue_key_prefix}:delayed"
            now = time.time()

            # 获取到期的延迟任务
            expired_tasks = self.redis.zrangebyscore(delayed_key, 0, now)
            if expired_tasks:
                for task_json in expired_tasks:
                    # 移动到优先级队列
                    task_data = json.loads(task_json)
                    priority_queue_key = f"{self.queue_key_prefix}:priority"
                    score = task_data["priority"] * 1000000 + time.time()
                    self.redis.zadd(priority_queue_key, {task_json: score})
                    self.redis.zrem(delayed_key, task_json)

            # 从优先级队列取出最高优先级任务
            priority_queue_key = f"{self.queue_key_prefix}:priority"
            result = self.redis.zrange(priority_queue_key, 0, 0, withscores=True)

            if result:
                task_json, score = result[0]
                if self.redis.zrem(priority_queue_key, task_json):
                    task_data = json.loads(task_json)
                    logger.debug(
                        "取出同步任务",
                        sync_id=task_data["sync_id"],
                        connection_id=task_data["connection_id"]
                    )
                    return task_data

            return None

        except Exception as e:
            logger.error("取出同步任务失败", error=str(e))
            return None

    async def get_queue_status(self) -> Dict[str, Any]:
        """
        获取队列状态

        Returns:
            dict: 队列状态信息
        """
        if not self.redis:
            return {"enabled": False}

        try:
            delayed_key = f"{self.queue_key_prefix}:delayed"
            priority_queue_key = f"{self.queue_key_prefix}:priority"

            delayed_count = self.redis.zcard(delayed_key)
            priority_count = self.redis.zcard(priority_queue_key)

            # 获取前几个任务的信息
            top_tasks = self.redis.zrange(priority_queue_key, 0, 4)
            task_info = []
            for task_json in top_tasks:
                task_data = json.loads(task_json)
                task_info.append({
                    "sync_id": task_data["sync_id"],
                    "connection_id": task_data["connection_id"],
                    "priority": task_data["priority"],
                    "created_at": task_data["created_at"]
                })

            return {
                "enabled": True,
                "delayed_count": delayed_count,
                "priority_count": priority_count,
                "total_count": delayed_count + priority_count,
                "top_tasks": task_info
            }

        except Exception as e:
            logger.error("获取队列状态失败", error=str(e))
            return {"enabled": False, "error": str(e)}

    async def update_sync_status(self, connection_id: UUID, status: SyncStatus,
                                metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        更新同步状态

        Args:
            connection_id: 连接ID
            status: 同步状态
            metadata: 额外的元数据

        Returns:
            bool: 是否成功更新
        """
        if not self.redis:
            return False

        try:
            status_key = f"{self.status_key_prefix}:{connection_id}"

            status_data = {
                "status": status.value,
                "updated_at": time.time(),
                "metadata": metadata or {}
            }

            self.redis.setex(status_key, 3600, json.dumps(status_data))  # 1小时过期

            logger.debug(
                "更新同步状态",
                connection_id=str(connection_id),
                status=status.value
            )
            return True

        except Exception as e:
            logger.error(
                "更新同步状态失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return False

    async def get_sync_status(self, connection_id: UUID) -> Optional[Dict[str, Any]]:
        """
        获取同步状态

        Args:
            connection_id: 连接ID

        Returns:
            dict: 状态信息
        """
        if not self.redis:
            return None

        try:
            status_key = f"{self.status_key_prefix}:{connection_id}"
            status_json = self.redis.get(status_key)

            if status_json:
                status_data = json.loads(status_json)
                return status_data

            return None

        except Exception as e:
            logger.error(
                "获取同步状态失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return None

    async def check_concurrency_limit(self) -> bool:
        """
        检查并发数限制

        Returns:
            bool: 是否可以启动新的同步
        """
        if not self.redis:
            return True  # Redis未配置时不限制并发

        try:
            # 统计当前运行中的同步数量
            pattern = f"{self.status_key_prefix}:*"
            keys = self.redis.keys(pattern)

            running_count = 0
            for key in keys:
                status_json = self.redis.get(key)
                if status_json:
                    status_data = json.loads(status_json)
                    if status_data.get("status") == SyncStatus.RUNNING.value:
                        running_count += 1

            can_start = running_count < self.max_concurrent_syncs

            logger.debug(
                "并发数检查",
                running_count=running_count,
                max_allowed=self.max_concurrent_syncs,
                can_start=can_start
            )

            return can_start

        except Exception as e:
            logger.error("检查并发数限制失败", error=str(e))
            return True  # 出错时允许启动

    async def cleanup_expired_locks(self) -> int:
        """
        清理过期的锁

        Returns:
            int: 清理的锁数量
        """
        if not self.redis:
            return 0

        try:
            pattern = f"{self.lock_key_prefix}:*"
            keys = self.redis.keys(pattern)

            cleaned_count = 0
            for key in keys:
                ttl = self.redis.ttl(key)
                if ttl == -1:  # 永久锁，设置过期时间
                    self.redis.expire(key, self.default_lock_timeout)
                    cleaned_count += 1
                elif ttl == -2:  # 已过期但未删除
                    self.redis.delete(key)
                    cleaned_count += 1

            if cleaned_count > 0:
                logger.debug("清理过期锁", count=cleaned_count)

            return cleaned_count

        except Exception as e:
            logger.error("清理过期锁失败", error=str(e))
            return 0

    # 本地锁方法（Redis不可用时的降级方案）
    async def _acquire_local_lock(self, connection_id: UUID, timeout: int) -> bool:
        """获取本地锁"""
        lock_key = str(connection_id)

        if lock_key in self.local_locks:
            return False

        self.local_locks[lock_key] = {
            "locked_at": time.time(),
            "timeout": timeout
        }

        # 设置定时器自动释放锁
        asyncio.create_task(self._auto_release_local_lock(lock_key, timeout))

        return True

    def _release_local_lock(self, connection_id: UUID) -> bool:
        """释放本地锁"""
        lock_key = str(connection_id)

        if lock_key in self.local_locks:
            del self.local_locks[lock_key]
            return True

        return False

    async def _auto_release_local_lock(self, lock_key: str, timeout: int):
        """自动释放本地锁"""
        await asyncio.sleep(timeout)
        if lock_key in self.local_locks:
            del self.local_locks[lock_key]
            logger.debug("本地锁自动释放", lock_key=lock_key)


class SyncResourceManager:
    """同步资源管理器

    管理同步过程中的各种资源，包括：
    - 数据库连接
    - Milvus客户端
    - Embedding客户端
    """

    def __init__(self, concurrency_controller: ConcurrencyController):
        self.controller = concurrency_controller
        self.active_resources = {}  # 活跃资源

    async def acquire_resources(self, connection_id: UUID) -> bool:
        """
        获取同步所需资源

        Args:
            connection_id: 连接ID

        Returns:
            bool: 是否成功获取资源
        """
        try:
            # 1. 检查并发限制
            if not await self.controller.check_concurrency_limit():
                logger.warning("并发数已达上限", connection_id=str(connection_id))
                return False

            # 2. 获取同步锁
            if not await self.controller.acquire_sync_lock(connection_id):
                logger.warning("无法获取同步锁", connection_id=str(connection_id))
                return False

            # 3. 更新状态为运行中
            await self.controller.update_sync_status(
                connection_id,
                SyncStatus.RUNNING,
                {"started_at": time.time()}
            )

            # 4. 记录资源获取
            self.active_resources[str(connection_id)] = {
                "acquired_at": time.time(),
                "type": "sync"
            }

            logger.debug("同步资源获取成功", connection_id=str(connection_id))
            return True

        except Exception as e:
            logger.error(
                "获取同步资源失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return False

    async def release_resources(self, connection_id: UUID,
                               final_status: SyncStatus = SyncStatus.COMPLETED,
                               metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        释放同步资源

        Args:
            connection_id: 连接ID
            final_status: 最终状态
            metadata: 额外元数据

        Returns:
            bool: 是否成功释放资源
        """
        try:
            # 1. 更新最终状态
            await self.controller.update_sync_status(
                connection_id,
                final_status,
                {
                    "completed_at": time.time(),
                    **(metadata or {})
                }
            )

            # 2. 释放同步锁
            await self.controller.release_sync_lock(connection_id)

            # 3. 清理资源记录
            connection_key = str(connection_id)
            if connection_key in self.active_resources:
                del self.active_resources[connection_key]

            logger.debug(
                "同步资源释放完成",
                connection_id=str(connection_id),
                final_status=final_status.value
            )
            return True

        except Exception as e:
            logger.error(
                "释放同步资源失败",
                connection_id=str(connection_id),
                error=str(e)
            )
            return False

    async def get_active_resources(self) -> Dict[str, Any]:
        """
        获取活跃资源统计

        Returns:
            dict: 资源统计信息
        """
        active_count = len(self.active_resources)
        queue_status = await self.controller.get_queue_status()

        return {
            "active_syncs": active_count,
            "active_connections": list(self.active_resources.keys()),
            "queue_status": queue_status,
            "max_concurrent": self.controller.max_concurrent_syncs
        }