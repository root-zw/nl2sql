"""查询缓存"""

from typing import Optional
import hashlib
import json
import structlog
import redis.asyncio as redis

from server.models.ir import IntermediateRepresentation
from server.models.api import QueryResult
from server.config import settings

logger = structlog.get_logger()


class QueryCache:
    """查询缓存 - 使用 Redis"""
    
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or settings.redis_url
        self.ttl = settings.cache_ttl_seconds
        self.prefix = settings.cache_key_prefix
        self.enabled = settings.cache_enabled
        self.client: Optional[redis.Redis] = None
    
    async def initialize(self):
        """初始化 Redis 连接"""
        if not self.enabled:
            logger.debug("查询缓存未启用")
            return
        
        try:
            self.client = redis.from_url(
                self.redis_url,
                decode_responses=True,
                max_connections=settings.redis_max_connections
            )
            # 测试连接
            await self.client.ping()
            logger.info("Redis 缓存初始化完成", url=self.redis_url)
        except Exception as e:
            logger.error("Redis 连接失败", error=str(e))
            self.enabled = False
    
    def get_cache_key(
        self,
        ir: IntermediateRepresentation,
        user_context: dict
    ) -> str:
        """
        生成缓存 Key
        
        考虑：IR + 用户角色（影响 RLS）
        
        Args:
            ir: 中间表示
            user_context: 用户上下文
        
        Returns:
            缓存 Key
        """
        # 排除不影响结果的字段
        ir_dict = ir.model_dump(exclude={"original_question", "ambiguities", "confidence"})
        ir_json = json.dumps(ir_dict, sort_keys=True)
        
        # 添加角色信息（RLS 相关）
        role = user_context.get("role", "viewer")
        tenant_id = user_context.get("tenant_id", "")
        
        key_material = f"{ir_json}:{role}:{tenant_id}"
        key_hash = hashlib.sha256(key_material.encode()).hexdigest()[:16]
        
        return f"{self.prefix}query:{key_hash}"
    
    async def get(self, key: str) -> Optional[QueryResult]:
        """
        从缓存获取结果
        
        Args:
            key: 缓存 Key
        
        Returns:
            QueryResult 或 None
        """
        if not self.enabled or not self.client:
            return None
        
        try:
            data = await self.client.get(key)
            if data:
                logger.debug("缓存命中", key=key)
                result_dict = json.loads(data)
                result = QueryResult(**result_dict)
                # 标记为缓存命中
                result.meta["cache_hit"] = True
                return result
            
            logger.debug("缓存未命中", key=key)
            return None
            
        except Exception as e:
            logger.error("从缓存读取失败", error=str(e))
            return None
    
    async def set(self, key: str, result: QueryResult):
        """
        设置缓存
        
        Args:
            key: 缓存 Key
            result: 查询结果
        """
        if not self.enabled or not self.client:
            return
        
        try:
            # 标记为非缓存命中（原始结果）
            result.meta["cache_hit"] = False
            
            data = result.model_dump_json()
            await self.client.setex(key, self.ttl, data)
            logger.debug("结果已缓存", key=key, ttl=self.ttl)
            
        except Exception as e:
            logger.error("写入缓存失败", error=str(e))
    
    async def invalidate(self, key: str):
        """使缓存失效"""
        if not self.enabled or not self.client:
            return
        
        try:
            await self.client.delete(key)
            logger.debug("缓存已失效", key=key)
        except Exception as e:
            logger.error("删除缓存失败", error=str(e))
    
    async def close(self):
        """关闭 Redis 连接"""
        if self.client:
            await self.client.close()
            logger.debug("Redis 连接已关闭")

