"""
数据库连接池管理
统一管理元数据库和目标数据库的连接池
"""

from typing import Dict, Optional
import asyncpg
from sqlalchemy import create_engine, pool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
import structlog
from urllib.parse import quote, quote_plus

from server.config import settings
from server.compiler.dialect_profiles import get_dialect_profile
from server.exec.connection import decrypt_password

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


class MetadataDBPool:
    """元数据库连接池（PostgreSQL）"""
    
    _instance: Optional['MetadataDBPool'] = None
    _pool: Optional[asyncpg.Pool] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def initialize(self):
        """初始化连接池"""
        if self._pool is not None:
            logger.warning("元数据库连接池已初始化")
            return
        
        try:
            self._pool = await asyncpg.create_pool(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db,
                min_size=settings.metadata_db_pool_min_size,
                max_size=settings.metadata_db_pool_max_size,
                command_timeout=settings.metadata_db_command_timeout,
                max_queries=settings.metadata_db_max_queries,
                max_inactive_connection_lifetime=settings.metadata_db_max_inactive_lifetime
            )
            logger.info("元数据库连接池初始化成功",
                       host=settings.postgres_host,
                       db=settings.postgres_db,
                       min_size=settings.metadata_db_pool_min_size,
                       max_size=settings.metadata_db_pool_max_size)
        except Exception as e:
            logger.exception("元数据库连接池初始化失败", error=str(e))
            raise
    
    async def close(self):
        """关闭连接池"""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("元数据库连接池已关闭")
    
    def get_pool(self) -> asyncpg.Pool:
        """获取连接池"""
        if self._pool is None:
            raise RuntimeError("元数据库连接池未初始化，请先调用 initialize()")
        return self._pool
    
    async def acquire(self):
        """获取一个连接（上下文管理器）"""
        return self._pool.acquire()


class TargetDBPoolManager:
    """目标数据库连接池管理器（支持多数据库连接）"""
    
    _instance: Optional['TargetDBPoolManager'] = None
    _pools: Dict[str, AsyncEngine] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._pools = {}
        return cls._instance
    
    async def get_or_create_pool(
        self,
        connection_id: str,
        db_type: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str
    ) -> AsyncEngine:
        """
        获取或创建目标数据库连接池
        
        Args:
            connection_id: 连接ID
            db_type: 数据库类型 (sqlserver/mysql/postgresql)
            host: 主机地址
            port: 端口
            database: 数据库名
            username: 用户名
            password: 密码
            
        Returns:
            AsyncEngine: SQLAlchemy异步引擎
        """
        # 检查是否已存在
        if connection_id in self._pools:
            logger.debug("使用已有连接池", connection_id=connection_id)
            return self._pools[connection_id]
        
        # 先解密密码（若启用加密）
        try:
            if password:
                password = decrypt_password(password)
        except Exception as e:
            # 解密失败时记录错误但不抛出异常，稍后连接可能会失败并记录日志
            logger.warning("密码解密失败，使用原始值", 
                         connection_id=connection_id,
                         error=str(e))

        profile = get_dialect_profile(db_type)
        db_type = profile.db_type

        # 构建连接字符串
        if db_type == "sqlserver":
            # 使用 FreeTDS 驱动（libtdsodbc.so）避免 SSL 兼容性问题
            # 使用 odbc_connect 参数传递完整的 ODBC 连接字符串
            # 转义用户名和密码中的特殊字符
            escaped_username = escape_odbc_value(username)
            escaped_password = escape_odbc_value(password or '')
            escaped_database = escape_odbc_value(database)
            
            # 记录连接信息（不记录密码）
            logger.debug("构建 SQL Server 连接字符串",
                        connection_id=connection_id,
                        host=host,
                        port=port,
                        database=database,
                        username=username,
                        password_length=len(password) if password else 0)
            
            odbc_connect = (
                f"Driver={settings.freetds_driver_path};"
                f"Server={host},{port};"
                f"Database={escaped_database};"
                f"UID={escaped_username};"
                f"PWD={escaped_password};"
                f"TDS_Version={settings.freetds_version};"
                f"Encrypt=no;"
                f"ClientCharset=UTF-8;"
            )
            quoted_odbc_connect = quote_plus(odbc_connect)
            connection_string = (
                f"mssql+aioodbc:///?odbc_connect={quoted_odbc_connect}"
            )
        elif profile.is_mysql_family:
            # MySQL/MariaDB: 使用 aiomysql
            quoted_username = quote(username, safe="")
            quoted_password = quote(password or "", safe="")
            quoted_database = quote(database, safe="")
            connection_string = (
                f"mysql+aiomysql://{quoted_username}:{quoted_password}@{host}:{port}/{quoted_database}"
                "?charset=utf8mb4"
            )
        elif db_type == "postgresql":
            # PostgreSQL: 使用 asyncpg
            connection_string = (
                f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}"
            )
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")
        
        try:
            # 创建异步引擎
            # SQL Server使用AUTOCOMMIT模式，配合禁用连接池自动事务管理
            if db_type == "sqlserver":
                # 为SQL Server添加autocommit参数到连接字符串
                if "autocommit=True" not in connection_string:
                    # 在现有连接字符串中添加autocommit
                    if "?odbc_connect=" in connection_string:
                        # 对于ODBC连接，需要在odbc_connect参数中添加
                        import urllib.parse
                        parsed = urllib.parse.urlparse(connection_string)
                        odbc_connect = urllib.parse.parse_qs(parsed.query)
                        if "odbc_connect" in odbc_connect:
                            # 解码现有的odbc_connect
                            existing_connect = urllib.parse.unquote(odbc_connect["odbc_connect"][0])
                            if "autocommit=True" not in existing_connect:
                                # 添加autocommit参数
                                updated_connect = existing_connect + ";autocommit=True;"
                                quoted_connect = urllib.parse.quote_plus(updated_connect)
                                connection_string = f"mssql+aioodbc:///?odbc_connect={quoted_connect}"

                engine = create_async_engine(
                    connection_string,
                    pool_size=settings.target_db_pool_size,
                    max_overflow=settings.target_db_pool_max_overflow,
                    pool_timeout=settings.target_db_pool_timeout,
                    pool_recycle=settings.target_db_pool_recycle,
                    pool_pre_ping=True,
                    echo=False,
                    # 禁用连接池的自动事务管理，因为已启用AUTOCOMMIT
                    pool_reset_on_return=None
                )
            else:
                engine = create_async_engine(
                    connection_string,
                    pool_size=settings.target_db_pool_size,
                    max_overflow=settings.target_db_pool_max_overflow,
                    pool_timeout=settings.target_db_pool_timeout,
                    pool_recycle=settings.target_db_pool_recycle,
                    pool_pre_ping=True,
                    echo=False
                )
            
            self._pools[connection_id] = engine
            
            logger.info(
                "目标数据库连接池创建成功",
                connection_id=connection_id,
                db_type=db_type,
                host=host
            )
            
            return engine
            
        except Exception as e:
            logger.exception("创建目标数据库连接池失败",
                           connection_id=connection_id,
                           db_type=db_type,
                           error=str(e))
            raise
    
    async def get_pool(self, connection_id: str) -> Optional[AsyncEngine]:
        """获取已存在的连接池"""
        return self._pools.get(connection_id)
    
    async def remove_pool(self, connection_id: str):
        """移除并关闭指定的连接池"""
        if connection_id in self._pools:
            engine = self._pools.pop(connection_id)
            await engine.dispose()
            logger.debug("连接池已移除", connection_id=connection_id)
    
    async def close_all(self):
        """关闭所有连接池"""
        for connection_id, engine in self._pools.items():
            await engine.dispose()
            logger.debug("连接池已关闭", connection_id=connection_id)
        self._pools.clear()
        logger.info("所有目标数据库连接池已关闭")


# 全局单例
_metadata_pool: Optional[MetadataDBPool] = None
_target_pool_manager: Optional[TargetDBPoolManager] = None


async def get_metadata_pool() -> asyncpg.Pool:
    """
    获取元数据库连接池
    
    Returns:
        asyncpg.Pool
    """
    global _metadata_pool
    
    if _metadata_pool is None:
        _metadata_pool = MetadataDBPool()
        await _metadata_pool.initialize()
    
    return _metadata_pool.get_pool()


def get_target_pool_manager() -> TargetDBPoolManager:
    """
    获取目标数据库连接池管理器
    
    Returns:
        TargetDBPoolManager
    """
    global _target_pool_manager
    
    if _target_pool_manager is None:
        _target_pool_manager = TargetDBPoolManager()
    
    return _target_pool_manager


async def initialize_pools():
    """初始化所有连接池（应用启动时调用）"""
    global _metadata_pool
    
    _metadata_pool = MetadataDBPool()
    await _metadata_pool.initialize()
    
    logger.info("数据库连接池初始化完成")


async def close_all_pools():
    """关闭所有连接池（应用关闭时调用）"""
    global _metadata_pool, _target_pool_manager
    
    if _metadata_pool:
        await _metadata_pool.close()
    
    if _target_pool_manager:
        await _target_pool_manager.close_all()
    
    logger.info("所有数据库连接池已关闭")

