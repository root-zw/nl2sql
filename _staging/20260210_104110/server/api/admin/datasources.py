"""
数据库连接管理API
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional
from uuid import UUID
import asyncpg
from cryptography.fernet import Fernet
import base64
import structlog
logger = structlog.get_logger()

from server.models.database import (
    DatabaseConnectionCreate,
    DatabaseConnectionUpdate,
    DatabaseConnectionResponse,
    TestConnectionRequest,
    TestConnectionResponse,
    PaginationParams,
    PaginatedResponse
)
from server.utils.db_inspector import get_inspector, DatabaseSchema
from server.utils.field_analyzer import FieldAnalyzer  # 智能推荐
from server.config import settings
from server.api.admin.milvus import get_milvus_client
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser

router = APIRouter()

#  密码加密（使用Fernet对称加密）
# 如果配置的密钥无效，自动生成一个（开发环境）
try:
    if hasattr(settings, 'encryption_key'):
        # 尝试使用配置的密钥
        test_key = settings.encryption_key.encode()
        cipher_suite = Fernet(test_key)
    else:
        # 没有配置，生成临时密钥（仅开发环境）
        cipher_suite = Fernet(Fernet.generate_key())
        logger.warning("未配置ENCRYPTION_KEY，使用临时密钥（仅开发环境）")
except Exception as e:
    # 密钥格式无效，生成新密钥
    temp_key = Fernet.generate_key()
    cipher_suite = Fernet(temp_key)
    logger.warning(
        f"配置的ENCRYPTION_KEY无效，使用临时密钥（仅开发环境）",
        error=str(e),
        temp_key=temp_key.decode()
    )


def encrypt_password(password: str) -> str:
    """加密密码"""
    return cipher_suite.encrypt(password.encode()).decode()


def decrypt_password(encrypted: str) -> str:
    """解密密码"""
    return cipher_suite.decrypt(encrypted.encode()).decode()


async def ensure_default_sync_config(db_conn, connection_id: UUID) -> None:
    """
    为新建连接写入默认的Milvus同步配置，避免后续健康检查404。
    """
    await db_conn.execute(
        """
        INSERT INTO milvus_sync_config (connection_id)
        VALUES ($1)
        ON CONFLICT (connection_id) DO NOTHING
        """,
        connection_id,
    )


async def get_db_pool():
    """获取元数据库连接（使用连接池）"""
    from server.utils.db_pool import get_metadata_pool

    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


# ============================================================================
# 数据库连接CRUD
# ============================================================================

@router.get("/connections/all", response_model=List[DatabaseConnectionResponse])
async def list_all_connections(
    is_active: Optional[bool] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取所有数据库连接（不分页，用于下拉框等场景）
    """
    try:
        # 构建查询
        where_clause = "WHERE 1=1"
        params = []

        if is_active is not None:
            where_clause += f" AND is_active = ${len(params) + 1}"
            params.append(is_active)

        query = f"""
            SELECT
                connection_id, connection_name, description, db_type,
                host, port, database_name, username,
                max_connections, connection_timeout,
                is_active, last_sync_at, sync_status, sync_message,
                table_count, field_count, created_at, updated_at
            FROM database_connections
            {where_clause}
            ORDER BY created_at DESC
        """

        rows = await db.fetch(query, *params)

        connections = [
            DatabaseConnectionResponse(
                connection_id=row['connection_id'],
                connection_name=row['connection_name'],
                description=row['description'],
                db_type=row['db_type'],
                host=row['host'],
                port=row['port'],
                database_name=row['database_name'],
                username=row['username'],
                max_connections=row['max_connections'],
                connection_timeout=row['connection_timeout'],
                is_active=row['is_active'],
                last_sync_at=row['last_sync_at'],
                sync_status=row['sync_status'],
                sync_message=row['sync_message'],
                table_count=row['table_count'],
                field_count=row['field_count'],
                created_at=row['created_at']
            )
            for row in rows
        ]

        return connections

    except Exception as e:
        logger.exception("获取所有数据库连接失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据库连接失败: {str(e)}"
        )


@router.get("/connections", response_model=PaginatedResponse)
async def list_connections(
    page: int = 1,
    page_size: int = 20,
    is_active: Optional[bool] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取数据库连接列表（分页，用于管理界面）
    """
    try:
        # 构建查询
        where_clause = "WHERE 1=1"
        params = []

        if is_active is not None:
            where_clause += f" AND is_active = ${len(params) + 1}"
            params.append(is_active)

        # 获取总数
        count_query = f"SELECT COUNT(*) FROM database_connections {where_clause}"
        total = await db.fetchval(count_query, *params)

        # 获取分页数据
        offset = (page - 1) * page_size
        params.extend([page_size, offset])

        query = f"""
            SELECT
                connection_id, connection_name, description, db_type,
                host, port, database_name, username, max_connections,
                connection_timeout, is_active, last_sync_at, sync_status,
                sync_message, table_count, field_count, created_at
            FROM database_connections
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${len(params) - 1} OFFSET ${len(params)}
        """

        rows = await db.fetch(query, *params)

        items = [
            DatabaseConnectionResponse(
                connection_id=row['connection_id'],
                connection_name=row['connection_name'],
                description=row['description'],
                db_type=row['db_type'],
                host=row['host'],
                port=row['port'],
                database_name=row['database_name'],
                username=row['username'],
                max_connections=row['max_connections'],
                connection_timeout=row['connection_timeout'],
                is_active=row['is_active'],
                last_sync_at=row['last_sync_at'],
                sync_status=row['sync_status'],
                sync_message=row['sync_message'],
                table_count=row['table_count'],
                field_count=row['field_count'],
                created_at=row['created_at']
            )
            for row in rows
        ]

        return PaginatedResponse(
            total=total,
            page=page,
            page_size=page_size,
            items=items
        )

    except Exception as e:
        logger.exception("获取数据库连接列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据库连接列表失败: {str(e)}"
        )


@router.get("/connections/{connection_id}", response_model=DatabaseConnectionResponse)
async def get_connection(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取单个数据库连接详情
    """
    try:
        query = """
            SELECT
                connection_id, connection_name, description, db_type,
                host, port, database_name, username, max_connections,
                connection_timeout, is_active, last_sync_at, sync_status,
                sync_message, table_count, field_count, created_at
            FROM database_connections
            WHERE connection_id = $1
        """

        row = await db.fetchrow(query, connection_id)

        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库连接 {connection_id} 不存在"
            )

        return DatabaseConnectionResponse(
            connection_id=row['connection_id'],
            connection_name=row['connection_name'],
            description=row['description'],
            db_type=row['db_type'],
            host=row['host'],
            port=row['port'],
            database_name=row['database_name'],
            username=row['username'],
            max_connections=row['max_connections'],
            connection_timeout=row['connection_timeout'],
            is_active=row['is_active'],
            last_sync_at=row['last_sync_at'],
            sync_status=row['sync_status'],
            sync_message=row['sync_message'],
            table_count=row['table_count'],
            field_count=row['field_count'],
            created_at=row['created_at']
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取数据库连接详情失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取数据库连接详情失败: {str(e)}"
        )


@router.post("/connections", response_model=DatabaseConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    connection: DatabaseConnectionCreate,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    创建数据库连接
    """
    try:
        # 加密密码
        encrypted_password = encrypt_password(connection.password)

        # 插入数据库并初始化默认同步配置
        query = """
            INSERT INTO database_connections (
                connection_name, description, db_type, host, port,
                database_name, username, password_encrypted,
                max_connections, connection_timeout, is_active
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE)
            RETURNING connection_id, connection_name, description, db_type,
                      host, port, database_name, username, max_connections,
                      connection_timeout, is_active, last_sync_at, sync_status,
                      sync_message, table_count, field_count, created_at
        """

        async with db.transaction():
            row = await db.fetchrow(
                query,
                connection.connection_name,
                connection.description,
                connection.db_type,
                connection.host,
                connection.port,
                connection.database_name,
                connection.username,
                encrypted_password,
                connection.max_connections,
                connection.connection_timeout
            )
            await ensure_default_sync_config(db, row['connection_id'])

        logger.debug(f"创建数据库连接成功: {connection.connection_name}")

        return DatabaseConnectionResponse(
            connection_id=row['connection_id'],
            connection_name=row['connection_name'],
            description=row['description'],
            db_type=row['db_type'],
            host=row['host'],
            port=row['port'],
            database_name=row['database_name'],
            username=row['username'],
            max_connections=row['max_connections'],
            connection_timeout=row['connection_timeout'],
            is_active=row['is_active'],
            last_sync_at=row['last_sync_at'],
            sync_status=row['sync_status'],
            sync_message=row['sync_message'],
            table_count=row['table_count'],
            field_count=row['field_count'],
            created_at=row['created_at']
        )

    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"数据库连接名称 '{connection.connection_name}' 已存在"
        )
    except Exception as e:
        logger.exception("创建数据库连接失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建数据库连接失败: {str(e)}"
        )


@router.put("/connections/{connection_id}", response_model=DatabaseConnectionResponse)
async def update_connection(
    connection_id: UUID,
    connection: DatabaseConnectionUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    更新数据库连接
    """
    try:
        # 检查连接是否存在
        existing = await db.fetchrow(
            "SELECT 1 FROM database_connections WHERE connection_id = $1",
            connection_id
        )

        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库连接 {connection_id} 不存在"
            )

        # 构建更新语句
        updates = []
        params = []
        param_index = 1

        update_fields = connection.dict(exclude_unset=True)

        # 处理密码加密
        if 'password' in update_fields:
            update_fields['password_encrypted'] = encrypt_password(update_fields.pop('password'))

        for field, value in update_fields.items():
            updates.append(f"{field} = ${param_index}")
            params.append(value)
            param_index += 1

        if not updates:
            # 没有任何更新，直接返回当前数据
            return await get_connection(connection_id, db)

        params.append(connection_id)

        query = f"""
            UPDATE database_connections
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE connection_id = ${param_index}
            RETURNING connection_id, connection_name, description, db_type,
                      host, port, database_name, username, max_connections,
                      connection_timeout, is_active, last_sync_at, sync_status,
                      sync_message, table_count, field_count, created_at
        """

        row = await db.fetchrow(query, *params)

        logger.debug(f"更新数据库连接成功: {connection_id}")

        return DatabaseConnectionResponse(
            connection_id=row['connection_id'],
            connection_name=row['connection_name'],
            description=row['description'],
            db_type=row['db_type'],
            host=row['host'],
            port=row['port'],
            database_name=row['database_name'],
            username=row['username'],
            max_connections=row['max_connections'],
            connection_timeout=row['connection_timeout'],
            is_active=row['is_active'],
            last_sync_at=row['last_sync_at'],
            sync_status=row['sync_status'],
            sync_message=row['sync_message'],
            table_count=row['table_count'],
            field_count=row['field_count'],
            created_at=row['created_at']
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新数据库连接失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新数据库连接失败: {str(e)}"
        )


@router.delete("/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    删除数据库连接（级联删除所有相关数据）
    """
    try:
        # 先检查连接是否存在
        connection_exists = await db.fetchval(
            "SELECT 1 FROM database_connections WHERE connection_id = $1",
            connection_id
        )
        
        if not connection_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库连接 {connection_id} 不存在"
            )

        # 0. 先删除 Milvus 中的实际数据（包括主集合、枚举值集合、Few-Shot集合）
        try:
            milvus_client = await get_milvus_client()
            if milvus_client:
                # 需要清理的集合列表
                collections_to_clean = [
                    getattr(milvus_client, 'collection_name', 'semantic_metadata'),  # 主集合（业务域+表）
                    settings.milvus_enum_collection,  # 枚举值集合
                    settings.milvus_few_shot_collection,  # Few-Shot样本集合
                ]

                for collection_name in collections_to_clean:
                    try:
                        # 检查集合是否存在
                        if collection_name in milvus_client.list_collections():
                            milvus_client.delete(
                                collection_name=collection_name,
                                filter=f'connection_id == "{str(connection_id)}"'
                            )
                            logger.debug(f"已清理Milvus集合 {collection_name} 中的数据: {connection_id}")
                    except Exception as e:
                        # 如果集合不存在或删除失败，记录日志但不阻止删除连接
                        logger.debug(f"清理Milvus集合 {collection_name} 数据失败（可能集合不存在）: {connection_id}", error=str(e))

                logger.debug(f"删除数据库连接前，已完成Milvus数据清理: {connection_id}")
            else:
                logger.debug("Milvus客户端未配置，跳过Milvus数据清理")
        except Exception as e:
            # Milvus清理失败不应该阻止删除连接
            logger.warning(f"清理Milvus数据时出错，继续删除连接: {connection_id}", error=str(e))

        # 删除相关的 milvus 记录（因为外键约束）
        # 注意删除顺序：先删除 milvus_pending_changes，因为它引用了 milvus_sync_history
        # 1. 删除 milvus_pending_changes 记录
        pending_changes_result = await db.execute(
            "DELETE FROM milvus_pending_changes WHERE connection_id = $1",
            connection_id
        )
        if pending_changes_result and pending_changes_result != "DELETE 0":
            logger.debug(f"删除数据库连接前，已清理待同步变更记录: {pending_changes_result}")

        # 2. 删除 milvus_sync_history 记录
        # 必须在 milvus_pending_changes 之后删除，因为 pending_changes 的 sync_id 引用了 sync_history
        sync_history_result = await db.execute(
            "DELETE FROM milvus_sync_history WHERE connection_id = $1",
            connection_id
        )
        if sync_history_result and sync_history_result != "DELETE 0":
            logger.debug(f"删除数据库连接前，已清理同步历史记录: {sync_history_result}")

        # 3. 删除 milvus_sync_config 记录
        sync_config_result = await db.execute(
            "DELETE FROM milvus_sync_config WHERE connection_id = $1",
            connection_id
        )
        if sync_config_result and sync_config_result != "DELETE 0":
            logger.debug(f"删除数据库连接前，已清理同步配置: {sync_config_result}")

        # 在删除 database_connections 之前，临时禁用相关触发器
        # 因为级联删除会触发 db_tables、fields 等的删除触发器，
        # 这些触发器会尝试向 milvus_pending_changes 插入记录，但此时 connection_id 已不存在
        # 
        # 注意：DISABLE TRIGGER 只影响当前数据库连接，不会影响其他连接
        # 由于我们使用连接池，每个请求使用独立连接，所以是安全的
        trigger_tables = [
            "business_domains",
            "db_tables",
            "fields",
            "field_enum_values",
            "qa_few_shot_samples",
        ]
        
        try:
            # 禁用相关触发器（全部用户级触发器，避免遗漏新增触发器）
            for table_name in trigger_tables:
                await db.execute(
                    f"ALTER TABLE {table_name} DISABLE TRIGGER USER"
                )
            logger.debug(f"已临时禁用 {len(trigger_tables)} 张表的触发器")
            
            # 删除数据库连接（会级联删除相关表）
            result = await db.execute(
                "DELETE FROM database_connections WHERE connection_id = $1",
                connection_id
            )
            
            if result == "DELETE 0":
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"数据库连接 {connection_id} 不存在"
                )
        finally:
            # 恢复触发器（即使删除失败也要恢复）
            # 这是关键：确保触发器一定会被恢复，不需要手动操作
            for table_name in trigger_tables:
                try:
                    await db.execute(
                        f"ALTER TABLE {table_name} ENABLE TRIGGER USER"
                    )
                except Exception as restore_error:
                    logger.error(
                        f"恢复触发器失败: {table_name}",
                        error=str(restore_error)
                    )
                    # 即使恢复失败，也继续恢复其他触发器
            logger.debug(f"已恢复 {len(trigger_tables)} 张表的触发器")

        logger.debug(f"删除数据库连接成功: {connection_id}")

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除数据库连接失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除数据库连接失败: {str(e)}"
        )


# ============================================================================
# 测试连接与Schema提取
# ============================================================================

@router.post("/connections/test", response_model=TestConnectionResponse)
async def test_connection(
    request: TestConnectionRequest,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    测试数据库连接（不保存）
    
    支持两种模式：
    1. 如果提供了 connection_id，从数据库读取连接信息（包括加密的密码）进行测试
    2. 如果没有提供 connection_id，使用请求中提供的连接信息进行测试
    """
    try:
        # 如果提供了 connection_id，从数据库读取连接信息
        if request.connection_id:
            conn_row = await db.fetchrow("""
                SELECT connection_id, db_type, host, port, database_name,
                       username, password_encrypted
                FROM database_connections
                WHERE connection_id = $1
            """, request.connection_id)
            
            if not conn_row:
                return TestConnectionResponse(
                    success=False,
                    message=f"数据库连接 {request.connection_id} 不存在"
                )
            
            # 解密密码
            password = decrypt_password(conn_row['password_encrypted'])
            
            # 使用数据库中的连接信息
            db_type = conn_row['db_type']
            host = conn_row['host']
            port = conn_row['port']
            database = conn_row['database_name']
            username = conn_row['username']
        else:
            # 使用请求中提供的连接信息
            if not all([request.db_type, request.host, request.port, 
                       request.database_name, request.username, request.password]):
                return TestConnectionResponse(
                    success=False,
                    message="缺少必需的连接参数（当未提供 connection_id 时，需要提供所有连接信息）"
                )
            
            db_type = request.db_type
            host = request.host
            port = request.port
            database = request.database_name
            username = request.username
            password = request.password
        
        inspector = get_inspector(
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password
        )

        success, message = await inspector.test_connection()

        return TestConnectionResponse(
            success=success,
            message=message
        )

    except Exception as e:
        logger.exception("测试数据库连接失败")
        return TestConnectionResponse(
            success=False,
            message=f"测试连接失败: {str(e)}"
        )


@router.post("/connections/{connection_id}/sync")
async def sync_connection_schema(
    connection_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    同步数据库Schema（表、列、外键等）
    """
    try:
        # 1. 获取连接信息
        conn_row = await db.fetchrow("""
            SELECT connection_id, db_type, host, port, database_name,
                   username, password_encrypted
            FROM database_connections
            WHERE connection_id = $1
        """, connection_id)

        if not conn_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"数据库连接 {connection_id} 不存在"
            )

        # 更新状态为同步中
        await db.execute("""
            UPDATE database_connections
            SET sync_status = 'syncing', updated_at = NOW()
            WHERE connection_id = $1
        """, connection_id)

        # 2. 解密密码
        password = decrypt_password(conn_row['password_encrypted'])

        # 3. 获取Schema
        inspector = get_inspector(
            db_type=conn_row['db_type'],
            host=conn_row['host'],
            port=conn_row['port'],
            database=conn_row['database_name'],
            username=conn_row['username'],
            password=password
        )

        schema = await inspector.get_schema()

        logger.debug(
            f"获取Schema成功",
            connection_id=str(connection_id),
            total_tables=schema.total_tables,
            total_columns=schema.total_columns
        )

        # 4. 保存到数据库（事务） - 增量更新，保留用户编辑
        async with db.transaction():
            # 4.1 获取当前已存在的表
            existing_tables = await db.fetch("""
                SELECT table_id, schema_name, table_name
                FROM db_tables
                WHERE connection_id = $1
            """, connection_id)

            existing_table_map = {
                (row['schema_name'], row['table_name']): row['table_id']
                for row in existing_tables
            }

            synced_table_ids = set()

            # 4.2 插入或更新表和列
            for table in schema.tables:
                table_key = (table.schema_name, table.table_name)

                if table_key in existing_table_map:
                    # 表已存在，更新物理信息（保留用户编辑的 display_name, description, domain_id, tags, is_included）
                    table_id = existing_table_map[table_key]
                    await db.execute("""
                        UPDATE db_tables
                        SET row_count = $1,
                            column_count = $2,
                            discovered_at = NOW()
                        WHERE table_id = $3
                    """, table.row_count, len(table.columns), table_id)
                else:
                    # 新表，插入
                    table_id = await db.fetchval("""
                        INSERT INTO db_tables (
                            connection_id, schema_name, table_name, row_count,
                            column_count, is_included
                        )
                        VALUES ($1, $2, $3, $4, $5, TRUE)
                        RETURNING table_id
                    """, connection_id, table.schema_name, table.table_name,
                         table.row_count, len(table.columns))

                synced_table_ids.add(table_id)

                # 获取该表已存在的列
                existing_columns = await db.fetch("""
                    SELECT column_id, column_name
                    FROM db_columns
                    WHERE table_id = $1
                """, table_id)

                existing_column_map = {
                    row['column_name']: row['column_id']
                    for row in existing_columns
                }

                synced_column_ids = set()

                # 插入或更新列
                for idx, col in enumerate(table.columns, start=1):
                    if col.column_name in existing_column_map:
                        # 列已存在，更新物理信息（保留 fields 表中的用户编辑）
                        column_id = existing_column_map[col.column_name]
                        await db.execute("""
                            UPDATE db_columns
                            SET data_type = $1,
                                max_length = $2,
                                is_nullable = $3,
                                is_primary_key = $4,
                                is_foreign_key = $5,
                                ordinal_position = $6
                            WHERE column_id = $7
                        """, col.data_type, col.max_length, col.is_nullable,
                             col.is_primary_key, col.is_foreign_key, idx, column_id)
                        synced_column_ids.add(column_id)
                    else:
                        # 新列，插入
                        column_id = await db.fetchval("""
                            INSERT INTO db_columns (
                                table_id, column_name, data_type, max_length,
                                is_nullable, is_primary_key, is_foreign_key,
                                sample_values, ordinal_position
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            RETURNING column_id
                        """, table_id, col.column_name, col.data_type, col.max_length,
                             col.is_nullable, col.is_primary_key, col.is_foreign_key,
                             [], idx)
                        synced_column_ids.add(column_id)

                        # 🆕 自动创建 fields 记录并应用智能推荐
                        # 使用 FieldAnalyzer 智能推荐字段类型
                        analysis = FieldAnalyzer.analyze_field(
                            column_name=col.column_name,
                            data_type=col.data_type,
                            is_primary_key=col.is_primary_key,
                            is_foreign_key=col.is_foreign_key
                        )

                        # 直接使用列名作为显示名称（用户需求）
                        display_name = col.column_name

                        # 创建 fields 记录
                        await db.execute("""
                            INSERT INTO fields (
                                connection_id, source_type, source_column_id,
                                field_type, display_name,
                                default_aggregation, allowed_aggregations,
                                unit, is_additive, is_unique,
                                auto_detected, confidence_score, is_active, show_in_detail
                            )
                            VALUES ($1, 'column', $2, $3, $4, $5, $6, $7, $8, $9, TRUE, $10, TRUE, FALSE)
                        """,
                            connection_id,
                            column_id,
                            analysis.field_type,
                            display_name,
                            analysis.default_aggregation,
                            analysis.allowed_aggregations or [],
                            analysis.unit,
                            analysis.is_additive,
                            analysis.is_unique,
                            analysis.confidence_score
                        )

                        logger.debug(
                            f"自动创建字段: {col.column_name} -> {analysis.field_type}",
                            column_name=col.column_name,
                            field_type=analysis.field_type,
                            confidence=analysis.confidence_score
                        )

                # 删除不再存在的列（可选，或者标记为删除）
                # 注意：这会级联删除关联的 fields 和 field_enum_values
                # 如果要保留历史数据，可以添加一个 is_deleted 标记
                columns_to_delete = set(existing_column_map.values()) - synced_column_ids
                if columns_to_delete:
                    await db.execute("""
                        DELETE FROM db_columns
                        WHERE column_id = ANY($1::uuid[])
                    """, list(columns_to_delete))
                    logger.debug(f"删除了 {len(columns_to_delete)} 个不存在的列")

            # 删除不再存在的表（可选，或者标记为删除）
            tables_to_delete = set(existing_table_map.values()) - synced_table_ids
            if tables_to_delete:
                await db.execute("""
                    DELETE FROM db_tables
                    WHERE table_id = ANY($1::uuid[])
                """, list(tables_to_delete))
                logger.debug(f"删除了 {len(tables_to_delete)} 个不存在的表")

            # 4.3 更新连接统计信息
            await db.execute("""
                UPDATE database_connections
                SET table_count = $1,
                    field_count = $2,
                    last_sync_at = NOW(),
                    sync_status = 'success',
                    sync_message = '同步成功',
                    updated_at = NOW()
                WHERE connection_id = $3
            """, schema.total_tables, schema.total_columns, connection_id)

        return {
            "success": True,
            "message": "同步成功",
            "total_tables": schema.total_tables,
            "total_columns": schema.total_columns
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("同步数据库Schema失败")

        # 更新失败状态
        try:
            await db.execute("""
                UPDATE database_connections
                SET sync_status = 'failed',
                    sync_message = $1,
                    updated_at = NOW()
                WHERE connection_id = $2
            """, str(e), connection_id)
        except:
            pass

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"同步数据库Schema失败: {str(e)}"
        )


@router.get("/connections/{connection_id}/tables")
async def get_connection_tables(
    connection_id: UUID,
    include_columns: bool = False,
    domain_id: Optional[UUID] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取数据库连接下的所有表
    """
    try:
        where_clause = "WHERE t.connection_id = $1"
        params = [connection_id]

        if domain_id:
            where_clause += f" AND t.domain_id = ${len(params) + 1}"
            params.append(domain_id)

        query = f"""
            SELECT
                t.table_id, t.schema_name, t.table_name, t.display_name,
                t.description, t.tags, t.domain_id, t.row_count,
                t.column_count, t.is_included, t.discovered_at, t.data_year,
                d.domain_name
            FROM db_tables t
            LEFT JOIN business_domains d ON t.domain_id = d.domain_id
            {where_clause}
            ORDER BY t.schema_name, t.table_name
        """

        rows = await db.fetch(query, *params)

        tables = []
        for row in rows:
            table = {
                "table_id": row['table_id'],
                "schema_name": row['schema_name'],
                "table_name": row['table_name'],
                "display_name": row['display_name'],
                "description": row['description'],
                "tags": row['tags'],
                "domain_id": row['domain_id'],
                "domain_name": row['domain_name'],
                "row_count": row['row_count'],
                "column_count": row['column_count'],
                "is_included": row['is_included'],
                "discovered_at": row['discovered_at'],
                "data_year": row['data_year']
            }

            # 如果需要包含列信息
            if include_columns:
                columns = await db.fetch("""
                    SELECT
                        c.column_id, c.column_name, c.data_type, c.max_length,
                        c.is_nullable, c.is_primary_key, c.is_foreign_key,
                        c.referenced_table_id, c.referenced_column_id,
                        c.distinct_count, c.sample_values, c.ordinal_position,
                        f.field_id, f.field_type, f.display_name, f.description,
                        f.unit, f.unit_conversion, f.default_aggregation, f.allowed_aggregations,
                        f.synonyms, f.tags, f.is_active, f.show_in_detail, f.priority,
                        f.enum_sync_config,
                        (SELECT COUNT(*) FROM field_enum_values WHERE field_id = f.field_id) as enum_value_count
                    FROM db_columns c
                    LEFT JOIN fields f ON c.column_id = f.source_column_id
                    WHERE c.table_id = $1
                    ORDER BY c.ordinal_position NULLS LAST
                """, row['table_id'])

                #  将JSONB字段（字符串）转换为字典
                import json
                parsed_columns = []
                for c in columns:
                    col_dict = dict(c)
                    # 解析unit_conversion（JSONB字段）
                    if col_dict.get('unit_conversion'):
                        try:
                            if isinstance(col_dict['unit_conversion'], str):
                                col_dict['unit_conversion'] = json.loads(col_dict['unit_conversion'])
                        except (json.JSONDecodeError, TypeError):
                            col_dict['unit_conversion'] = None
                    # 解析enum_sync_config（JSONB字段）
                    if col_dict.get('enum_sync_config'):
                        try:
                            if isinstance(col_dict['enum_sync_config'], str):
                                col_dict['enum_sync_config'] = json.loads(col_dict['enum_sync_config'])
                        except (json.JSONDecodeError, TypeError):
                            col_dict['enum_sync_config'] = None
                    parsed_columns.append(col_dict)

                table['columns'] = parsed_columns

            tables.append(table)

        return {
            "total": len(tables),
            "tables": tables
        }

    except Exception as e:
        logger.exception("获取表列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取表列表失败: {str(e)}"
        )

