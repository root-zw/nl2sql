"""
数据库会话管理器
负责管理只读事务会话，保证复杂查询执行期间的数据一致性（Snapshot Isolation）
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
import structlog
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncTransaction
from sqlalchemy import text

from server.utils.db_pool import get_target_pool_manager

logger = structlog.get_logger()

class DatabaseSessionManager:
    """
    数据库会话管理器
    
    核心功能：提供 snapshot_scope 上下文管理器，
    确保在 Scope 内的所有查询基于同一个数据快照。
    """
    
    def __init__(self):
        self.pool_manager = get_target_pool_manager()

    async def _get_engine(self, connection_id: str) -> AsyncEngine:
        pool = await self.pool_manager.get_pool(connection_id)
        if not pool:
            raise RuntimeError(f"未找到连接池: {connection_id}")
        return pool

    async def _setup_snapshot(self, conn: AsyncConnection, db_type: str) -> Optional[AsyncTransaction]:
        """设置快照隔离级别并返回事务对象"""
        transaction: Optional[AsyncTransaction] = None
        try:
            if db_type == "postgresql":
                transaction = await conn.begin()
                await conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            
            elif db_type == "mysql":
                await conn.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                await conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
                transaction = await conn.begin()
                
            elif db_type == "sqlserver":
                try:
                    await conn.execute(text("SET TRANSACTION ISOLATION LEVEL SNAPSHOT"))
                    transaction = await conn.begin()
                    await conn.execute(text("SELECT 1"))
                except Exception as e:
                    logger.warning(f"SQL Server启用快照隔离失败({str(e)})，回退到 READ UNCOMMITTED")
                    if transaction:
                        try:
                            await transaction.rollback()
                        except Exception:
                            pass
                        transaction = None
                    
                    await conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
                    transaction = await conn.begin()
            else:
                transaction = await conn.begin()
                
        except Exception as e:
            logger.error(f"设置事务隔离级别失败: {str(e)}", db_type=db_type)
            if transaction:
                try:
                    await transaction.rollback()
                except Exception:
                    pass
            raise
        
        return transaction

    @asynccontextmanager
    async def snapshot_scope(self, connection_id: str, db_type: str) -> AsyncGenerator[AsyncConnection, None]:
        """
        获取一个保证快照隔离级别的只读连接/会话
        
        Args:
            connection_id: 数据库连接ID
            db_type: 数据库类型 (postgresql, mysql, sqlserver)
            
        Yields:
            AsyncConnection: 已开启事务的连接对象
        """
        engine = await self._get_engine(connection_id)
        
        # 获取连接
        conn = await engine.connect()
        transaction: Optional[AsyncTransaction] = None
        try:
            # 设置快照事务
            transaction = await self._setup_snapshot(conn, db_type)
            
            logger.debug("已开启只读快照事务", connection_id=connection_id)
            yield conn
            
        except Exception as e:
            logger.error("会话异常", error=str(e), connection_id=connection_id)
            raise
        finally:
            # 无论成功失败，最后都回滚（因为是只读，不需要 commit）
            try:
                if transaction:
                    await transaction.rollback()
            except Exception as e:
                logger.debug("[SessionManager] 回滚失败已忽略", error=str(e))
            finally:
                await conn.close()
            logger.debug("已关闭只读快照事务", connection_id=connection_id)

# 全局实例
_session_manager = DatabaseSessionManager()

def get_session_manager() -> DatabaseSessionManager:
    return _session_manager
