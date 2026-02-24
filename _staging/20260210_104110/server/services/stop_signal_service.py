"""
停止信号服务
使用 Redis 存储停止标记，支持实时中断流式生成
"""

from typing import Optional
import structlog
from uuid import UUID

from server.dependencies import get_redis_client

logger = structlog.get_logger()


class QueryStoppedException(Exception):
    """查询被用户停止的异常"""

    def __init__(self, message_id: str, reason: str = "用户取消"):
        self.message_id = message_id
        self.reason = reason
        super().__init__(f"查询已取消: {message_id} ({reason})")


class StopSignalService:
    """停止信号服务 - 管理查询/消息的停止标记"""
    
    # Redis Key 前缀
    STOP_SIGNAL_PREFIX = "stop:message:"
    STOP_SIGNAL_TTL = 3600  # 1小时过期
    
    @staticmethod
    def _get_stop_key(message_id: str) -> str:
        """生成停止标记的 Redis Key"""
        return f"{StopSignalService.STOP_SIGNAL_PREFIX}{message_id}"
    
    @staticmethod
    def set_stop_signal(message_id: str) -> bool:
        """
        设置停止信号
        
        Args:
            message_id: 消息ID
            
        Returns:
            bool: 是否成功设置
        """
        try:
            redis_client = get_redis_client()
            if not redis_client:
                logger.warning("Redis未启用，无法设置停止信号", message_id=message_id)
                return False
            
            key = StopSignalService._get_stop_key(message_id)
            redis_client.setex(key, StopSignalService.STOP_SIGNAL_TTL, "1")
            logger.info("停止信号已设置", message_id=message_id)
            return True
        except Exception as e:
            logger.error("设置停止信号失败", message_id=message_id, error=str(e))
            return False
    
    @staticmethod
    def check_stop_signal(message_id: str) -> bool:
        """
        检查是否有停止信号
        
        Args:
            message_id: 消息ID
            
        Returns:
            bool: 是否有停止信号
        """
        try:
            redis_client = get_redis_client()
            if not redis_client:
                return False
            
            key = StopSignalService._get_stop_key(message_id)
            value = redis_client.get(key)
            return value == "1"
        except Exception as e:
            logger.warning("检查停止信号失败", message_id=message_id, error=str(e))
            return False
    
    @staticmethod
    def clear_stop_signal(message_id: str) -> bool:
        """
        清除停止信号
        
        Args:
            message_id: 消息ID
            
        Returns:
            bool: 是否成功清除
        """
        try:
            redis_client = get_redis_client()
            if not redis_client:
                return False
            
            key = StopSignalService._get_stop_key(message_id)
            redis_client.delete(key)
            logger.debug("停止信号已清除", message_id=message_id)
            return True
        except Exception as e:
            logger.warning("清除停止信号失败", message_id=message_id, error=str(e))
            return False
    
    @staticmethod
    def check_and_raise_if_stopped(message_id: str):
        """
        检查停止信号，如果已停止则抛出异常

        Args:
            message_id: 消息ID

        Raises:
            QueryStoppedException: 如果检测到停止信号
        """
        if StopSignalService.check_stop_signal(message_id):
            logger.info("检测到停止信号，中断生成", message_id=message_id)
            raise QueryStoppedException(message_id, "用户取消")

