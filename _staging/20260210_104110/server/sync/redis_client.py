"""
Redis客户端获取工具
为自动同步系统提供Redis客户端
"""

import structlog
from typing import Optional

from server.dependencies import get_redis_client

logger = structlog.get_logger()


def get_sync_redis_client():
    """
    获取自动同步系统专用的Redis客户端

    Returns:
        Redis客户端实例，如果Redis未配置或连接失败则返回None
    """
    try:
        redis_client = get_redis_client()
        if redis_client:
            logger.debug("自动同步系统Redis客户端获取成功")
            return redis_client
        else:
            logger.warning("Redis客户端不可用，自动同步将使用本地锁机制")
            return None
    except Exception as e:
        logger.error(f"获取Redis客户端失败: {e}")
        return None


def test_redis_connection() -> bool:
    """
    测试Redis连接是否正常

    Returns:
        bool: 连接是否正常
    """
    try:
        redis_client = get_sync_redis_client()
        if redis_client:
            # 测试ping
            result = redis_client.ping()
            return result is True
        return False
    except Exception as e:
        logger.error(f"Redis连接测试失败: {e}")
        return False


def set_redis_value(key: str, value: str, expire: Optional[int] = None) -> bool:
    """
    设置Redis键值对

    Args:
        key: 键名
        value: 值
        expire: 过期时间（秒）

    Returns:
        bool: 是否设置成功
    """
    try:
        redis_client = get_sync_redis_client()
        if redis_client:
            if expire:
                return redis_client.setex(key, expire, value)
            else:
                return redis_client.set(key, value)
        return False
    except Exception as e:
        logger.error(f"设置Redis值失败: {e}")
        return False


def get_redis_value(key: str) -> Optional[str]:
    """
    获取Redis值

    Args:
        key: 键名

    Returns:
        str: 值，不存在或失败时返回None
    """
    try:
        redis_client = get_sync_redis_client()
        if redis_client:
            return redis_client.get(key)
        return None
    except Exception as e:
        logger.error(f"获取Redis值失败: {e}")
        return None


def delete_redis_key(key: str) -> bool:
    """
    删除Redis键

    Args:
        key: 键名

    Returns:
        bool: 是否删除成功
    """
    try:
        redis_client = get_sync_redis_client()
        if redis_client:
            result = redis_client.delete(key)
            return result > 0
        return False
    except Exception as e:
        logger.error(f"删除Redis键失败: {e}")
        return False


def redis_exists(key: str) -> bool:
    """
    检查Redis键是否存在

    Args:
        key: 键名

    Returns:
        bool: 键是否存在
    """
    try:
        redis_client = get_sync_redis_client()
        if redis_client:
            return redis_client.exists(key)
        return False
    except Exception as e:
        logger.error(f"检查Redis键存在性失败: {e}")
        return False


def acquire_redis_lock(key: str, expire: int = 300) -> bool:
    """
    获取Redis分布式锁

    Args:
        key: 锁键名
        expire: 过期时间（秒）

    Returns:
        bool: 是否成功获取锁
    """
    try:
        redis_client = get_sync_redis_client()
        if redis_client:
            # 使用SET NX EX原子操作
            return redis_client.set(key, "locked", ex=expire, nx=True)
        return False
    except Exception as e:
        logger.error(f"获取Redis锁失败: {e}")
        return False


def release_redis_lock(key: str) -> bool:
    """
    释放Redis分布式锁

    Args:
        key: 锁键名

    Returns:
        bool: 是否成功释放锁
    """
    try:
        redis_client = get_sync_redis_client()
        if redis_client:
            result = redis_client.delete(key)
            return result > 0
        return False
    except Exception as e:
        logger.error(f"释放Redis锁失败: {e}")
        return False