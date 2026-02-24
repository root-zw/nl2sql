"""查询执行器"""

from typing import Dict, Any, List, Optional
import time
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from server.models.api import QueryResult
from server.config import settings
from server.exceptions import ExecutionError, SecurityError
from server.security.sql_validator import validate_sql_security

logger = structlog.get_logger()


class QueryExecutor:
    """查询执行器（使用连接池）"""
    
    def __init__(
        self,
        connection_id: str,
        db_type: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str
    ):
        """
        初始化查询执行器
        
        Args:
            connection_id: 连接ID
            db_type: 数据库类型 (sqlserver/mysql/postgresql)
            host: 主机地址
            port: 端口
            database: 数据库名
            username: 用户名
            password: 密码
        """
        self.connection_id = connection_id
        self.db_type = db_type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self._engine: Optional[AsyncEngine] = None
    
    async def _get_engine(self) -> AsyncEngine:
        """获取或创建数据库引擎（使用连接池）"""
        if self._engine is None:
            from server.utils.db_pool import get_target_pool_manager
            pool_manager = get_target_pool_manager()
            
            self._engine = await pool_manager.get_or_create_pool(
                connection_id=self.connection_id,
                db_type=self.db_type,
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password
            )
        
        return self._engine
    
    async def ensure_engine(self) -> AsyncEngine:
        """
        显式初始化并返回底层引擎。
        DAG 等需要共享会话的流程会提前调用，确保连接池已经建立。
        """
        return await self._get_engine()
    
    async def execute_async(
        self,
        sql: str,
        timeout: int = None
    ) -> QueryResult:
        """
        异步执行 SQL 查询（自动管理连接）
        """
        engine = await self._get_engine()
        async with engine.connect() as conn:
            return await self.execute_with_connection(conn, sql, timeout)

    async def execute_with_connection(
        self,
        conn,
        sql: str,
        timeout: int = None
    ) -> QueryResult:
        """
        使用指定连接执行 SQL 查询
        
        Args:
            conn: 已建立的数据库连接
            sql: SQL 语句
            timeout: 超时时间（秒）
        """
        # 1. SQL安全验证
        try:
            logger.debug("开始SQL安全验证", sql_length=len(sql))
            is_safe, error_msg = validate_sql_security(sql, self.db_type)
            if not is_safe:
                raise SecurityError(error_msg or "SQL安全验证失败")
            logger.debug("SQL安全验证通过")
        except SecurityError:
            raise
        except Exception as e:
            logger.error("SQL安全验证异常", error=str(e))
            raise SecurityError(f"SQL安全验证失败: {str(e)}")

        timeout = timeout or settings.query_timeout_seconds

        logger.debug("开始执行查询", sql_length=len(sql), timeout=timeout, db_type=self.db_type)
        start_time = time.time()

        try:
            # 设置查询超时（根据数据库类型）
            if self.db_type == "sqlserver":
                await conn.execute(text(f"SET LOCK_TIMEOUT {timeout * 1000}"))
            elif self.db_type == "postgresql":
                await conn.execute(text(f"SET statement_timeout = {timeout * 1000}"))
            elif self.db_type == "mysql":
                await conn.execute(text(f"SET SESSION max_execution_time = {timeout * 1000}"))

            # 执行查询
            result = await conn.execute(text(sql))
            
            # 获取列信息
            columns = []
            if result.cursor and result.cursor.description:
                columns = [
                    {"name": col[0], "type": self._map_column_type(col[1])}
                    for col in result.cursor.description
                ]
            
            # 获取行数据
            rows = result.fetchall()
            rows_data = [list(row) for row in rows]
            
            latency_ms = (time.time() - start_time) * 1000
            
            logger.debug(
                "查询执行完成",
                rows=len(rows_data),
                columns=len(columns),
                latency_ms=int(latency_ms),
                db_type=self.db_type
            )
            
            return QueryResult(
                columns=columns,
                rows=rows_data,
                meta={
                    "sql": sql,
                    "latency_ms": int(latency_ms),
                    "cache_hit": False,
                    "dialect": self.db_type,
                    "row_count": len(rows_data)
                }
            )

        except SecurityError:
            # SecurityError 重新抛出，不需要包装
            raise
        except Exception as e:
            logger.error("查询执行失败", error=str(e), db_type=self.db_type)
            logger.debug("失败SQL详情", sql=sql)
            raise ExecutionError(
                f"查询执行失败: {str(e)}",
                details={"sql": sql[:500], "db_type": self.db_type}
            )
    
    def execute(
        self,
        sql: str,
        timeout: int = None
    ) -> QueryResult:
        """
        同步执行 SQL 查询（内部调用异步方法）
        """
        import asyncio
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果在异步上下文中，直接调用异步方法
                raise RuntimeError("请使用 execute_async() 而不是 execute()")
            else:
                # 在同步上下文中，创建新的事件循环
                return asyncio.run(self.execute_async(sql, timeout))
        except RuntimeError as e:
            if "execute_async()" in str(e):
                raise
            # 其他RuntimeError，尝试使用新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self.execute_async(sql, timeout))
            finally:
                loop.close()
    
    def _map_column_type(self, db_type) -> str:
        """
        映射数据库类型到前端类型
        
        Args:
            db_type: 数据库列类型对象
        
        Returns:
            类型字符串: "string", "number", "boolean", "date"
        """
        type_name = str(db_type).upper()
        
        if "INT" in type_name or "DECIMAL" in type_name or "FLOAT" in type_name or "NUMERIC" in type_name:
            return "number"
        elif "DATE" in type_name or "TIME" in type_name:
            return "date"
        elif "BIT" in type_name or "BOOL" in type_name:
            return "boolean"
        else:
            return "string"
