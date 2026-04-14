"""
数据库检查器
支持自动提取数据库Schema（表、列、外键关系等）
支持 SQL Server、MySQL、PostgreSQL
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import structlog
logger = structlog.get_logger()
import aioodbc
import aiomysql
import asyncpg

from server.config import settings

MYSQL_UNBOUNDED_LENGTH_TYPES = {
    "tinytext",
    "text",
    "mediumtext",
    "longtext",
    "tinyblob",
    "blob",
    "mediumblob",
    "longblob",
    "json",
}


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


def normalize_mysql_max_length(
    data_type: Optional[str],
    max_length: Optional[int]
) -> Optional[int]:
    """
    归一化 MySQL/MariaDB 的 CHARACTER_MAXIMUM_LENGTH。

    对 TEXT/BLOB/JSON 这类“理论超大长度”字段，返回 None，
    避免把 4294967295 之类的哨兵值写入元数据库。
    """
    if max_length is None:
        return None

    normalized_type = (data_type or "").strip().lower()
    if normalized_type in MYSQL_UNBOUNDED_LENGTH_TYPES:
        return None

    try:
        normalized_length = int(max_length)
    except (TypeError, ValueError):
        return None

    return normalized_length if normalized_length >= 0 else None


@dataclass
class ColumnInfo:
    """列信息"""
    column_name: str
    data_type: str
    max_length: Optional[int]
    is_nullable: bool
    is_primary_key: bool
    is_foreign_key: bool
    referenced_table: Optional[str] = None
    referenced_column: Optional[str] = None
    default_value: Optional[str] = None
    ordinal_position: int = 0


@dataclass
class TableInfo:
    """表信息"""
    schema_name: str
    table_name: str
    row_count: Optional[int] = None
    columns: List[ColumnInfo] = None
    
    def __post_init__(self):
        if self.columns is None:
            self.columns = []


@dataclass
class DatabaseSchema:
    """数据库Schema"""
    database_name: str
    database_type: str
    tables: List[TableInfo]
    total_tables: int
    total_columns: int


class DatabaseInspector:
    """数据库检查器基类"""
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
    
    async def test_connection(self) -> Tuple[bool, str]:
        """测试连接"""
        raise NotImplementedError
    
    async def get_schema(self) -> DatabaseSchema:
        """获取数据库Schema"""
        raise NotImplementedError
    
    async def sample_enum_values(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        top_n: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        从数据库采样枚举值（按频次降序）
        
        Args:
            top_n: 采样数量上限，默认1000（用户可自行调整）
        
        Returns:
            List[Dict]: [{'value': ..., 'frequency': ...}, ...]
        """
        raise NotImplementedError


class SQLServerInspector(DatabaseInspector):
    """SQL Server检查器"""
    
    async def test_connection(self) -> Tuple[bool, str]:
        """测试连接"""
        try:
            # 使用 FreeTDS 驱动（libtdsodbc.so）避免 SSL 兼容性问题
            # 转义用户名和密码中的特殊字符
            escaped_username = escape_odbc_value(self.username)
            escaped_password = escape_odbc_value(self.password)
            escaped_database = escape_odbc_value(self.database)
            
            connection_string = (
                f"Driver={settings.freetds_driver_path};"
                f"Server={self.host},{self.port};"
                f"Database={escaped_database};"
                f"UID={escaped_username};"
                f"PWD={escaped_password};"
                f"TDS_Version={settings.freetds_version};"
                f"Encrypt=no;"
                f"ClientCharset=UTF-8;"
                f"Timeout={settings.db_inspector_timeout};"
            )
            
            conn = await aioodbc.connect(dsn=connection_string, timeout=settings.db_inspector_timeout)
            cursor = await conn.cursor()
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
            await cursor.close()
            await conn.close()
            
            return True, "连接成功"
        except Exception as e:
            logger.error(f"SQL Server连接失败: {e}")
            return False, f"连接失败: {str(e)}"
    
    async def get_schema(self) -> DatabaseSchema:
        """获取SQL Server Schema"""
        try:
            # 使用 FreeTDS 驱动（libtdsodbc.so）避免 SSL 兼容性问题
            # 转义用户名和密码中的特殊字符
            escaped_username = escape_odbc_value(self.username)
            escaped_password = escape_odbc_value(self.password)
            escaped_database = escape_odbc_value(self.database)
            
            connection_string = (
                f"Driver={settings.freetds_driver_path};"
                f"Server={self.host},{self.port};"
                f"Database={escaped_database};"
                f"UID={escaped_username};"
                f"PWD={escaped_password};"
                f"TDS_Version={settings.freetds_version};"
                f"Encrypt=no;"
                f"ClientCharset=UTF-8;"
            )
            
            conn = await aioodbc.connect(dsn=connection_string)
            cursor = await conn.cursor()
            
            # 1. 获取所有表
            await cursor.execute("""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            
            tables = []
            table_rows = await cursor.fetchall()
            
            for schema_name, table_name in table_rows:
                table_info = TableInfo(
                    schema_name=schema_name,
                    table_name=table_name
                )
                
                # 2. 获取列信息
                await cursor.execute("""
                    SELECT 
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.CHARACTER_MAXIMUM_LENGTH,
                        c.IS_NULLABLE,
                        c.ORDINAL_POSITION,
                        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    LEFT JOIN (
                        SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                            ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
                        AND c.TABLE_NAME = pk.TABLE_NAME 
                        AND c.COLUMN_NAME = pk.COLUMN_NAME
                    WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
                    ORDER BY c.ORDINAL_POSITION
                """, schema_name, table_name)
                
                columns = []
                column_rows = await cursor.fetchall()
                
                for col in column_rows:
                    columns.append(ColumnInfo(
                        column_name=col[0],
                        data_type=col[1],
                        max_length=col[2],
                        is_nullable=(col[3] == 'YES'),
                        ordinal_position=col[4],
                        is_primary_key=bool(col[5]),
                        is_foreign_key=False  # 外键需要单独查询
                    ))
                
                # 3. 获取外键信息
                await cursor.execute("""
                    SELECT 
                        fk.name AS FK_NAME,
                        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS FK_COLUMN,
                        OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS REFERENCED_SCHEMA,
                        OBJECT_NAME(fk.referenced_object_id) AS REFERENCED_TABLE,
                        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS REFERENCED_COLUMN
                    FROM sys.foreign_keys AS fk
                    INNER JOIN sys.foreign_key_columns AS fkc 
                        ON fk.object_id = fkc.constraint_object_id
                    WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = ? 
                        AND OBJECT_NAME(fk.parent_object_id) = ?
                """, schema_name, table_name)
                
                fk_rows = await cursor.fetchall()
                for fk in fk_rows:
                    fk_column = fk[1]
                    # 更新对应列的外键信息
                    for col in columns:
                        if col.column_name == fk_column:
                            col.is_foreign_key = True
                            col.referenced_table = f"{fk[2]}.{fk[3]}"
                            col.referenced_column = fk[4]
                            break
                
                # 4. 获取行数（可选，可能慢）
                try:
                    await cursor.execute(f"""
                        SELECT COUNT_BIG(*) 
                        FROM [{schema_name}].[{table_name}]
                    """)
                    row = await cursor.fetchone()
                    table_info.row_count = row[0] if row else None
                except:
                    table_info.row_count = None
                
                table_info.columns = columns
                tables.append(table_info)
            
            await cursor.close()
            await conn.close()
            
            total_columns = sum(len(t.columns) for t in tables)
            
            return DatabaseSchema(
                database_name=self.database,
                database_type="sqlserver",
                tables=tables,
                total_tables=len(tables),
                total_columns=total_columns
            )
        
        except Exception as e:
            logger.exception("获取SQL Server Schema失败")
            raise
    
    async def sample_enum_values(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        top_n: int = 1000
    ) -> List[Dict[str, Any]]:
        """采样SQL Server枚举值"""
        try:
            # 使用 FreeTDS 驱动（libtdsodbc.so）避免 SSL 兼容性问题
            # 转义用户名和密码中的特殊字符
            escaped_username = escape_odbc_value(self.username)
            escaped_password = escape_odbc_value(self.password)
            escaped_database = escape_odbc_value(self.database)
            
            connection_string = (
                    f"Driver={settings.freetds_driver_path};"
                f"Server={self.host},{self.port};"
                f"Database={escaped_database};"
                f"UID={escaped_username};"
                f"PWD={escaped_password};"
                    f"TDS_Version={settings.freetds_version};"
                f"Encrypt=no;"
                f"ClientCharset=UTF-8;"
            )
            
            conn = await aioodbc.connect(dsn=connection_string)
            cursor = await conn.cursor()
            
            full_table_name = f"[{schema_name}].[{table_name}]" if schema_name else f"[{table_name}]"
            query = f"""
                SELECT TOP (?) [{column_name}] as value, COUNT(*) as frequency
                FROM {full_table_name}
                WHERE [{column_name}] IS NOT NULL
                GROUP BY [{column_name}]
                ORDER BY frequency DESC
            """
            
            await cursor.execute(query, top_n)
            rows = await cursor.fetchall()
            
            result = [{'value': row[0], 'frequency': row[1]} for row in rows]
            
            await cursor.close()
            await conn.close()
            
            return result
            
        except Exception as e:
            logger.exception(f"SQL Server采样枚举值失败")
            raise


class MySQLInspector(DatabaseInspector):
    """MySQL检查器"""

    database_type = "mysql"

    async def _get_version_string(self, conn) -> str:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT VERSION()")
            row = await cursor.fetchone()
            return row[0] if row else ""
    
    async def test_connection(self) -> Tuple[bool, str]:
        """测试连接"""
        try:
            conn = await aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.database,
                connect_timeout=settings.db_inspector_timeout
            )
            version = await self._get_version_string(conn)
            conn.close()
            
            product_name = "MariaDB" if "MariaDB" in version else self.database_type.upper()
            version_suffix = f" ({version})" if version else ""
            return True, f"连接成功: {product_name}{version_suffix}"
        except Exception as e:
            logger.error(f"MySQL连接失败: {e}")
            return False, f"连接失败: {str(e)}"
    
    async def get_schema(self) -> DatabaseSchema:
        """获取MySQL Schema"""
        try:
            conn = await aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.database,
                connect_timeout=settings.db_inspector_timeout
            )
            
            async with conn.cursor() as cursor:
                # 1. 获取所有表
                await cursor.execute("""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """, (self.database,))
                
                tables = []
                table_rows = await cursor.fetchall()
                
                for (table_name,) in table_rows:
                    table_info = TableInfo(
                        schema_name=self.database,
                        table_name=table_name
                    )
                    
                    # 2. 获取列信息
                    await cursor.execute("""
                        SELECT 
                            COLUMN_NAME,
                            DATA_TYPE,
                            CHARACTER_MAXIMUM_LENGTH,
                            IS_NULLABLE,
                            ORDINAL_POSITION,
                            COLUMN_KEY
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        ORDER BY ORDINAL_POSITION
                    """, (self.database, table_name))
                    
                    columns = []
                    column_rows = await cursor.fetchall()
                    
                    for col in column_rows:
                        columns.append(ColumnInfo(
                            column_name=col[0],
                            data_type=col[1],
                            max_length=normalize_mysql_max_length(col[1], col[2]),
                            is_nullable=(col[3] == 'YES'),
                            ordinal_position=col[4],
                            is_primary_key=(col[5] == 'PRI'),
                            is_foreign_key=(col[5] == 'MUL')
                        ))
                    
                    # 3. 获取外键详细信息
                    await cursor.execute("""
                        SELECT 
                            COLUMN_NAME,
                            REFERENCED_TABLE_NAME,
                            REFERENCED_COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = %s 
                            AND TABLE_NAME = %s 
                            AND REFERENCED_TABLE_NAME IS NOT NULL
                    """, (self.database, table_name))
                    
                    fk_rows = await cursor.fetchall()
                    for fk in fk_rows:
                        for col in columns:
                            if col.column_name == fk[0]:
                                col.referenced_table = fk[1]
                                col.referenced_column = fk[2]
                                break
                    
                    # 4. 获取行数
                    try:
                        await cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                        row = await cursor.fetchone()
                        table_info.row_count = row[0] if row else None
                    except:
                        table_info.row_count = None
                    
                    table_info.columns = columns
                    tables.append(table_info)
            
            conn.close()
            
            total_columns = sum(len(t.columns) for t in tables)
            
            return DatabaseSchema(
                database_name=self.database,
                database_type=self.database_type,
                tables=tables,
                total_tables=len(tables),
                total_columns=total_columns
            )
        
        except Exception as e:
            logger.exception("获取MySQL Schema失败")
            raise
    
    async def sample_enum_values(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        top_n: int = 1000
    ) -> List[Dict[str, Any]]:
        """采样MySQL枚举值"""
        try:
            conn = await aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.database
            )
            
            async with conn.cursor() as cursor:
                full_table_name = f"`{schema_name}`.`{table_name}`" if schema_name and schema_name != self.database else f"`{table_name}`"
                query = f"""
                    SELECT `{column_name}` as value, COUNT(*) as frequency
                    FROM {full_table_name}
                    WHERE `{column_name}` IS NOT NULL
                    GROUP BY `{column_name}`
                    ORDER BY frequency DESC
                    LIMIT %s
                """
                
                await cursor.execute(query, (top_n,))
                rows = await cursor.fetchall()
                
                result = [{'value': row[0], 'frequency': row[1]} for row in rows]
            
            conn.close()
            return result
            
        except Exception as e:
            logger.exception(f"MySQL采样枚举值失败")
            raise


class MariaDBInspector(MySQLInspector):
    """MariaDB检查器"""

    database_type = "mariadb"


class PostgreSQLInspector(DatabaseInspector):
    """PostgreSQL检查器"""
    
    async def test_connection(self) -> Tuple[bool, str]:
        """测试连接"""
        try:
            conn = await asyncpg.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=settings.db_inspector_timeout
            )
            await conn.fetchval("SELECT 1")
            await conn.close()
            
            return True, "连接成功"
        except Exception as e:
            logger.error(f"PostgreSQL连接失败: {e}")
            return False, f"连接失败: {str(e)}"
    
    async def get_schema(self) -> DatabaseSchema:
        """获取PostgreSQL Schema"""
        try:
            conn = await asyncpg.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=settings.db_inspector_timeout
            )
            
            # 1. 获取所有表
            tables_query = """
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schemaname, tablename
            """
            table_rows = await conn.fetch(tables_query)
            
            tables = []
            
            for row in table_rows:
                schema_name = row['schemaname']
                table_name = row['tablename']
                
                table_info = TableInfo(
                    schema_name=schema_name,
                    table_name=table_name
                )
                
                # 2. 获取列信息
                columns_query = """
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.character_maximum_length,
                        c.is_nullable,
                        c.ordinal_position,
                        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT ku.table_schema, ku.table_name, ku.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage ku
                            ON tc.constraint_name = ku.constraint_name
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                    ) pk ON c.table_schema = pk.table_schema 
                        AND c.table_name = pk.table_name 
                        AND c.column_name = pk.column_name
                    WHERE c.table_schema = $1 AND c.table_name = $2
                    ORDER BY c.ordinal_position
                """
                column_rows = await conn.fetch(columns_query, schema_name, table_name)
                
                columns = []
                for col in column_rows:
                    columns.append(ColumnInfo(
                        column_name=col['column_name'],
                        data_type=col['data_type'],
                        max_length=col['character_maximum_length'],
                        is_nullable=(col['is_nullable'] == 'YES'),
                        ordinal_position=col['ordinal_position'],
                        is_primary_key=col['is_primary_key'],
                        is_foreign_key=False
                    ))
                
                # 3. 获取外键信息
                fk_query = """
                    SELECT
                        kcu.column_name,
                        ccu.table_schema AS foreign_table_schema,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = $1
                        AND tc.table_name = $2
                """
                fk_rows = await conn.fetch(fk_query, schema_name, table_name)
                
                for fk in fk_rows:
                    for col in columns:
                        if col.column_name == fk['column_name']:
                            col.is_foreign_key = True
                            col.referenced_table = f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}"
                            col.referenced_column = fk['foreign_column_name']
                            break
                
                # 4. 获取行数
                try:
                    count = await conn.fetchval(
                        f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
                    )
                    table_info.row_count = count
                except:
                    table_info.row_count = None
                
                table_info.columns = columns
                tables.append(table_info)
            
            await conn.close()
            
            total_columns = sum(len(t.columns) for t in tables)
            
            return DatabaseSchema(
                database_name=self.database,
                database_type="postgresql",
                tables=tables,
                total_tables=len(tables),
                total_columns=total_columns
            )
        
        except Exception as e:
            logger.exception("获取PostgreSQL Schema失败")
            raise
    
    async def sample_enum_values(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        top_n: int = 1000
    ) -> List[Dict[str, Any]]:
        """采样PostgreSQL枚举值"""
        try:
            conn = await asyncpg.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                timeout=settings.db_inspector_timeout
            )
            
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
            query = f"""
                SELECT "{column_name}" as value, COUNT(*) as frequency
                FROM {full_table_name}
                WHERE "{column_name}" IS NOT NULL
                GROUP BY "{column_name}"
                ORDER BY frequency DESC
                LIMIT $1
            """
            
            rows = await conn.fetch(query, top_n)
            result = [{'value': row['value'], 'frequency': row['frequency']} for row in rows]
            
            await conn.close()
            return result
            
        except Exception as e:
            logger.exception(f"PostgreSQL采样枚举值失败")
            raise


def get_inspector(
    db_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str
) -> DatabaseInspector:
    """获取数据库检查器"""
    if db_type.lower() == "sqlserver":
        return SQLServerInspector(host, port, database, username, password)
    elif db_type.lower() == "mysql":
        return MySQLInspector(host, port, database, username, password)
    elif db_type.lower() == "mariadb":
        return MariaDBInspector(host, port, database, username, password)
    elif db_type.lower() == "postgresql":
        return PostgreSQLInspector(host, port, database, username, password)
    else:
        raise ValueError(f"不支持的数据库类型: {db_type}")
