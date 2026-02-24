"""数据库连接管理 - 支持动态多数据库连接"""

import structlog
from typing import Optional
from urllib.parse import quote_plus
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from cryptography.fernet import Fernet

from server.config import settings

logger = structlog.get_logger()


def escape_odbc_value(value: str) -> str:
    """
    转义 ODBC 连接字符串中的值
    如果值包含特殊字符（分号、等号、大括号），需要用大括号包围
    """
    if not value:
        return value
    
    # 如果值包含特殊字符，需要用大括号包围
    if any(char in value for char in [';', '=', '{', '}', ']']):
        # 如果已经有大括号，需要转义内部的大括号
        escaped = value.replace('}', '}}').replace('{', '{{')
        return f"{{{escaped}}}"
    return value


#  密码解密
try:
    if hasattr(settings, 'encryption_key'):
        cipher_suite = Fernet(settings.encryption_key.encode())
    else:
        cipher_suite = None
        logger.warning("未配置ENCRYPTION_KEY，无法解密数据库密码")
except Exception as e:
    cipher_suite = None
    logger.error(f"加载ENCRYPTION_KEY失败: {e}")


def decrypt_password(encrypted: str) -> str:
    """解密密码"""
    if cipher_suite is None:
        logger.warning("密码加密未启用，直接返回原文")
        return encrypted
    try:
        return cipher_suite.decrypt(encrypted.encode()).decode()
    except Exception as e:
        logger.error(f"密码解密失败: {e}")
        raise ValueError("密码解密失败")


class DatabaseConnectionManager:
    """数据库连接管理器（用于系统健康检查，已完全异步化）"""

    def __init__(self):
        self.engine: Optional[AsyncEngine] = None
        self._dsn: Optional[str] = None
        self._initialize_engine()

    def _ensure_async_driver(self, dsn: str) -> str:
        """将同步驱动DSN转换为异步驱动"""
        mapping = [
            ("mssql+pyodbc://", "mssql+aioodbc://"),
            ("postgresql+psycopg2://", "postgresql+asyncpg://"),
            ("postgresql://", "postgresql+asyncpg://"),
            ("mysql+pymysql://", "mysql+aiomysql://"),
            ("mysql://", "mysql+aiomysql://"),
            ("sqlite+pysqlite://", "sqlite+aiosqlite://"),
            ("sqlite://", "sqlite+aiosqlite://"),
        ]
        for prefix, replacement in mapping:
            if dsn.startswith(prefix):
                return replacement + dsn[len(prefix):]
        return dsn

    def _ensure_sqlserver_autocommit(self, dsn: str) -> str:
        """确保 SQL Server ODBC 连接启用 AUTOCOMMIT"""
        if "mssql+aioodbc" not in dsn or "autocommit=True" in dsn:
            return dsn

        if "?odbc_connect=" in dsn:
            import urllib.parse

            parsed = urllib.parse.urlparse(dsn)
            odbc_connect = urllib.parse.parse_qs(parsed.query)
            if "odbc_connect" in odbc_connect:
                existing_connect = urllib.parse.unquote(odbc_connect["odbc_connect"][0])
                if "autocommit=True" not in existing_connect:
                    updated_connect = existing_connect + ";autocommit=True;"
                    quoted_connect = urllib.parse.quote_plus(updated_connect)
                    return f"mssql+aioodbc:///?odbc_connect={quoted_connect}"

        return dsn

    def _initialize_engine(self) -> None:
        """初始化异步数据库引擎"""
        dsn = settings.get_db_dsn()
        async_dsn = self._ensure_async_driver(dsn)
        async_dsn = self._ensure_sqlserver_autocommit(async_dsn)
        self._dsn = async_dsn

        safe_dsn = (
            async_dsn.split("@")[0] + "@***"
            if "@" in async_dsn
            else async_dsn.split("/")[0] + "/***"
        )
        logger.debug("初始化默认异步数据库连接（用于健康检查）", dsn=safe_dsn)

        engine_kwargs = dict(
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_pool_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            echo=settings.db_echo,
            connect_args={"timeout": 30},
            pool_pre_ping=True,
        )

        if async_dsn.startswith("mssql+aioodbc://"):
            engine_kwargs["pool_reset_on_return"] = None

        self.engine = create_async_engine(async_dsn, **engine_kwargs)
        logger.debug("默认异步数据库连接初始化完成")

    async def get_connection(self) -> AsyncConnection:
        """获取一个异步连接（调用方需负责关闭）"""
        if self.engine is None:
            self._initialize_engine()
        return await self.engine.connect()

    async def close(self):
        """关闭异步连接池"""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            logger.debug("数据库连接池已关闭")

    def get_pool_metrics(self) -> dict:
        """获取连接池指标（用于监控展示）"""
        if not self.engine:
            return {}
        pool = self.engine.sync_engine.pool
        metrics = {
            "size": pool.size(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
        }
        return metrics


# 全局连接管理器实例
_connection_manager: Optional[DatabaseConnectionManager] = None


def get_connection_manager() -> DatabaseConnectionManager:
    """获取全局连接管理器（用于健康检查等场景）"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = DatabaseConnectionManager()
    return _connection_manager

