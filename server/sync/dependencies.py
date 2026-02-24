"""
同步服务依赖注入
"""

import asyncpg
import redis
from typing import Optional

from server.config import settings


# 同步的依赖获取函数（用于同步上下文）
def get_db_pool_sync() -> asyncpg.Pool:
    """获取数据库连接池（同步版本）"""
    # 这里应该返回已初始化的连接池
    # 在实际应用中，连接池应该在应用启动时初始化并存储在全局变量中
    from server.utils.db_pool import get_metadata_pool
    return get_metadata_pool()


def get_redis_client_sync() -> Optional[redis.Redis]:
    """获取Redis客户端（同步版本）"""
    if not settings.redis_url:
        return None

    try:
        import redis
        return redis.from_url(settings.redis_url, decode_responses=True)
    except Exception:
        return None