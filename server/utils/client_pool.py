"""
客户端连接池管理器
提供Milvus和Embedding客户端的单例模式和连接池管理
"""

import asyncio
from typing import Optional, Any, List
import structlog
from datetime import datetime, timedelta
from types import SimpleNamespace

logger = structlog.get_logger()


class _HitAdapter(SimpleNamespace):
    """适配 MilvusClient 返回的 dict，使其具备 .entity/.distance 属性，兼容 ORM Hit 接口。"""

    def __init__(self, data: Any):
        if isinstance(data, dict):
            entity = data.get("entity") or data
            distance = data.get("distance")
        else:
            entity = getattr(data, "entity", data)
            distance = getattr(data, "distance", None)
        super().__init__(entity=entity, distance=distance)

    def get(self, key, default=None):
        return getattr(self, key, default)


class _MilvusOrmCompatClient:
    """
    将 MilvusClient 的返回值适配为 ORM Hit 风格，减少上层改动。

    - search：把 dict hit 转成带 .entity/.distance 的对象列表。
    - 其他方法/属性透传到底层 MilvusClient。
    """

    def __init__(self, client):
        self._client = client
        self.collection_name = getattr(client, "collection_name", None)

    def __getattr__(self, item):
        return getattr(self._client, item)

    def search(self, *args, **kwargs) -> List[List[_HitAdapter]]:
        raw = self._client.search(*args, **kwargs)
        if not raw:
            return raw
        adapted: List[List[_HitAdapter]] = []
        for hits in raw:
            if not hits:
                adapted.append([])
                continue
            adapted.append([_HitAdapter(h) for h in hits])
        return adapted

class MilvusClientPool:
    """Milvus客户端池（单例模式）"""
    
    _instance: Optional['MilvusClientPool'] = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        self._client = None
        self._last_health_check = None
        self._health_check_interval = timedelta(minutes=5)
        self._is_healthy = True
        
    @classmethod
    async def get_instance(cls) -> 'MilvusClientPool':
        """获取单例实例"""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    async def get_client(self, max_retries: int = 3, retry_delay: float = 2.0):
        """获取Milvus客户端（带健康检查和重试机制）"""
        from server.config import settings
        
        if not settings.milvus_enabled:
            return None
        
        # 首次创建或需要健康检查
        now = datetime.now()
        should_check = (
            self._client is None or 
            not self._is_healthy or
            self._last_health_check is None or
            (now - self._last_health_check) > self._health_check_interval
        )
        
        if should_check:
            last_error = None
            for attempt in range(max_retries):
                try:
                    is_new_client = self._client is None
                    if is_new_client:
                        from pymilvus import MilvusClient
                        self._client = _MilvusOrmCompatClient(
                            MilvusClient(
                                uri=settings.milvus_uri,
                                token=settings.milvus_token if settings.milvus_token else None
                            )
                        )
                        self._client.collection_name = settings.milvus_collection

                    # 健康检查
                    self._client.list_collections()
                    self._is_healthy = True
                    self._last_health_check = now
                    if is_new_client:
                        logger.info("Milvus客户端初始化成功", attempt=attempt + 1 if attempt > 0 else None)
                    else:
                        logger.debug("Milvus客户端健康检查通过")
                    break
                    
                except Exception as e:
                    last_error = e
                    error_msg = str(e)
                    # 如果是"not ready yet"错误，进行重试
                    if "not ready yet" in error_msg.lower() or "service unavailable" in error_msg.lower():
                        if attempt < max_retries - 1:
                            logger.warning(
                                "Milvus服务尚未就绪，等待后重试",
                                attempt=attempt + 1,
                                max_retries=max_retries,
                                delay=retry_delay,
                                error=error_msg
                            )
                            await asyncio.sleep(retry_delay)
                            # 清理失败的客户端
                            if self._client:
                                try:
                                    self._client.close()
                                except:
                                    pass
                                self._client = None
                            continue
                    
                    # 其他错误或重试次数用完
                    logger.error("Milvus客户端不可用", error=str(e), attempt=attempt + 1)
                    self._is_healthy = False
                    self._client = None
                    if attempt == max_retries - 1:
                        return None
        
        return self._client
    
    async def close(self):
        """关闭客户端连接"""
        if self._client:
            try:
                self._client.close()
                logger.debug("Milvus客户端已关闭")
            except Exception as e:
                logger.warning("关闭Milvus客户端失败", error=str(e))
            finally:
                self._client = None
                self._is_healthy = False


# ============================================================================
# Embedding/Reranker 客户端统一管理
# 使用 server.utils.model_clients 模块
# ============================================================================

# 便捷函数
async def get_milvus_client():
    """获取Milvus客户端（推荐使用）"""
    pool = await MilvusClientPool.get_instance()
    return await pool.get_client()


async def get_embedding_client():
    """
    获取Embedding客户端（推荐使用）
    
    使用统一的模型客户端管理器
    """
    from server.utils.model_clients import get_embedding_client as _get_embedding_client
    return await _get_embedding_client()


async def get_reranker_client():
    """
    获取Reranker客户端（推荐使用）
    
    使用统一的模型客户端管理器
    """
    from server.utils.model_clients import get_reranker_client as _get_reranker_client
    return await _get_reranker_client()


async def close_all_clients():
    """关闭所有客户端连接（应用关闭时调用）"""
    from server.utils.model_clients import close_model_clients
    
    tasks = []
    
    if MilvusClientPool._instance:
        tasks.append(MilvusClientPool._instance.close())
    
    # 关闭模型客户端
    tasks.append(close_model_clients())
    
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

