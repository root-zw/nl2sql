"""
依赖注入
集中管理全局单例和工厂函数
"""

import structlog
from functools import lru_cache
from pathlib import Path
from typing import Optional

from server.config import settings
from server.nl2ir.llm_client import LLMClient
from server.metadata.db_manager import MetadataManager
from server.compiler.compiler import SQLCompiler
from server.compiler.dialect_profiles import get_dialect_profile
from server.nl2ir.domain_detector import DomainDetector
from server.utils.global_rules_loader import GlobalRulesLoader

logger = structlog.get_logger()

# ════════════════════════════════════════════════════════════════════════
# 全局单例对象（延迟初始化）
# ════════════════════════════════════════════════════════════════════════
_llm_client = None
_metadata_manager = None
_redis_client = None


# 多场景 LLM 客户端缓存
_llm_clients = {}


def get_llm_client(scenario: str = "default") -> LLMClient:
    """
    获取 LLM 客户端（按场景缓存）
    
    Args:
        scenario: 使用场景
            - "default": 默认配置
            - "table_selection": 表选择（可独立配置模型）
            - "nl2ir": NL2IR 解析（可独立配置模型）
            - "direct_sql": 直接 SQL 生成（可独立配置模型）
            - "narrative": 叙述生成（可独立配置模型）
    
    Returns:
        LLMClient 实例
    """
    global _llm_clients
    
    if scenario not in _llm_clients:
        logger.debug(f"初始化 LLM 客户端: scenario={scenario}")
        _llm_clients[scenario] = LLMClient(scenario=scenario)
    
    return _llm_clients[scenario]


def get_table_selection_llm_client() -> LLMClient:
    """获取表选择场景的 LLM 客户端"""
    return get_llm_client("table_selection")


def get_nl2ir_llm_client() -> LLMClient:
    """获取 NL2IR 解析场景的 LLM 客户端"""
    return get_llm_client("nl2ir")


def get_direct_sql_llm_client() -> LLMClient:
    """获取直接 SQL 生成场景的 LLM 客户端"""
    return get_llm_client("direct_sql")


def get_narrative_llm_client() -> LLMClient:
    """获取叙述生成场景的 LLM 客户端"""
    return get_llm_client("narrative")


def get_vector_selector_llm_client() -> LLMClient:
    """获取向量表选择场景的 LLM 客户端（LLM3）"""
    return get_llm_client("vector_selector")






@lru_cache(maxsize=1)
def get_metadata_manager():
    """获取元数据管理器（单例）"""
    global _metadata_manager

    if _metadata_manager is None:
        logger.debug("初始化元数据管理器")

        # 构建数据库URL
        db_url = f"postgresql://{settings.postgres_user}:{settings.postgres_password}@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"

        # 初始化MetadataManager
        _metadata_manager = MetadataManager(db_url)

        logger.debug("元数据管理器初始化成功")

    return _metadata_manager


def get_global_rules_loader(connection_id: str = None):
    """
    获取全局规则加载器

    Args:
        connection_id: 连接ID（如果提供则按连接过滤）

    Returns:
        GlobalRulesLoader实例，初始化失败时返回None
    """
    try:
        from server.utils.global_rules_loader import GlobalRulesLoader
        from uuid import UUID

        if connection_id:
            # 按连接ID过滤的规则加载器
            return GlobalRulesLoader(UUID(connection_id))
        else:
            logger.warning("未提供connection_id，无法初始化全局规则加载器")
            return None
    except Exception as e:
        logger.warning(f"初始化全局规则加载器失败: {e}")
        return None


async def get_enum_retriever():
    """获取枚举值检索器（双向量）"""
    from server.nl2ir.enum_value_retriever import DualVectorEnumRetriever

    try:
        from server.utils.db_pool import get_metadata_pool
        from server.api.admin.milvus import get_milvus_client, get_embedding_client
        db_pool = await get_metadata_pool()
        milvus_client = get_milvus_client()
        embedding_client = get_embedding_client()

        # 如果Milvus或Embedding未配置，返回None
        if not milvus_client or not embedding_client:
            logger.warning("Milvus或Embedding服务未配置，枚举值检索器不可用")
            return None

        return DualVectorEnumRetriever(
            db_pool=db_pool,
            milvus_client=milvus_client,
            embedding_client=embedding_client,
            collection_name=settings.milvus_enum_collection
        )
    except Exception as e:
        logger.error("枚举值检索器初始化失败", error=str(e))
        return None


async def create_nl2ir_parser(connection_id: str):
    """
    基于指定 connection_id 动态创建 NL→IR 解析器（元数据库模式）。
    不依赖全局语义模型，直接从元数据库加载对应连接的模型。
    """
    from server.nl2ir.parser import NL2IRParser
    # 使用 NL2IR 场景的 LLM 客户端（支持独立配置模型）
    llm_client = get_nl2ir_llm_client()

    # 加载该连接的语义模型
    manager = get_metadata_manager()
    semantic_model = await manager.get_connection_model(connection_id)

    # 创建业务域检测器并初始化（加载业务域关键词等，用于自动判定 domain_id）
    from server.nl2ir.domain_detector import DomainDetector
    domain_detector = DomainDetector(semantic_model, connection_id=str(connection_id))
    try:
        # 确保元数据连接池已就绪（get_connection_model 内部已 connect）
        await domain_detector.initialize(manager, str(connection_id))
        logger.debug("业务域检测器已初始化，启用关键词检测")
    except Exception as e:
        # 初始化失败不阻塞查询，仅记录告警
        logger.warning("业务域检测器初始化失败，将不进行自动域识别", error=str(e))

    #  创建层次化检索器（业务域 → 表 → 字段）
    hierarchical_retriever = None
    try:
        from server.nl2ir.hierarchical_retriever import HierarchicalRetriever
        from pymilvus import MilvusClient
        from server.utils.model_clients import EmbeddingClient
        from server.utils.db_pool import get_metadata_pool

        # 获取Milvus和Embedding客户端
        milvus_client = None
        embedding_client = None
        db_pool = None

        # 元数据库连接池（用于度量/枚举等增强检索）
        try:
            db_pool = await get_metadata_pool()
        except Exception as e:
            logger.warning("获取元数据库连接池失败，将禁用PG增强检索", error=str(e))

        if settings.milvus_enabled:
            # 初始化Milvus客户端
            milvus_client = MilvusClient(
                uri=settings.milvus_uri,
                token=settings.milvus_token
            )

            # 初始化Embedding客户端
            if settings.embedding_base_url and settings.embedding_api_key:
                embedding_client = EmbeddingClient(
                    base_url=settings.embedding_base_url,
                    api_key=settings.embedding_api_key,
                    model=settings.embedding_model,
                    timeout=settings.embedding_timeout
                )

        hierarchical_retriever = HierarchicalRetriever(
            semantic_model=semantic_model,
            domain_detector=domain_detector,
            milvus_client=milvus_client,
            embedding_client=embedding_client,
            connection_id=str(connection_id),
            db_pool=db_pool,
        )
        logger.debug(f"层次化检索器初始化成功，连接ID: {connection_id}")
    except Exception as e:
        logger.error("层次化检索器初始化失败", error=str(e))
        raise

    # 全局规则加载器（按连接）
    global_rules_loader = get_global_rules_loader(connection_id)

    # 枚举值检索器（双向量）
    enum_retriever = None
    try:
        from server.nl2ir.enum_value_retriever import DualVectorEnumRetriever
        
        if milvus_client and embedding_client and db_pool:
            enum_retriever = DualVectorEnumRetriever(
                db_pool=db_pool,
                milvus_client=milvus_client,
                embedding_client=embedding_client,
                collection_name=settings.milvus_enum_collection,
                connection_id=str(connection_id),
            )
            logger.debug("枚举值检索器初始化成功")
        else:
            logger.warning("Milvus或Embedding未配置，枚举值检索器不可用")
    except Exception as e:
        logger.warning("枚举值检索器初始化失败，将不使用枚举值增强功能", error=str(e))

    return NL2IRParser(
        llm_client=llm_client,
        semantic_model=semantic_model,
        domain_detector=domain_detector,
        global_rules_loader=global_rules_loader,
        hierarchical_retriever=hierarchical_retriever,
        enum_retriever=enum_retriever
    )


async def create_sql_compiler(connection_id: str) -> SQLCompiler:
    """
    动态创建 SQL 编译器（ - 支持多数据库方言）

    Args:
        connection_id: 数据库连接ID

    Returns:
        SQLCompiler 实例
    """
    # 加载语义模型和图
    manager = get_metadata_manager()
    model = await manager.get_connection_model(connection_id)

    from server.metadata.semantic_graph import SemanticGraph
    graph = SemanticGraph(model.joins)

    # 获取数据库方言
    from server.utils.db_pool import get_metadata_pool
    db_pool = await get_metadata_pool()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT db_type FROM database_connections WHERE connection_id = $1",
            connection_id
        )

    profile = get_dialect_profile(row["db_type"] if row else "postgresql")

    logger.debug(
        "创建SQL编译器",
        connection_id=connection_id,
        db_type=profile.db_type,
        dialect=profile.compiler_dialect,
        dialect_display=profile.display_name,
    )

    # 全局规则加载器（用于派生指标等）
    global_rules_loader = get_global_rules_loader(connection_id)

    return SQLCompiler(
        model,
        graph,
        dialect=profile.compiler_dialect,
        db_type=profile.db_type,
        global_rules_loader=global_rules_loader,
    )


_query_cache = None

def get_query_cache():
    """获取查询缓存（单例）"""
    global _query_cache
    if _query_cache is None:
        from server.exec.cache import QueryCache
        _query_cache = QueryCache()
        logger.debug("查询缓存实例已创建")
    return _query_cache

async def initialize_query_cache():
    """初始化查询缓存（异步）"""
    global _query_cache
    if _query_cache is None:
        _query_cache = get_query_cache()
    if _query_cache and settings.cache_enabled:
        await _query_cache.initialize()
        logger.debug("查询缓存已初始化")


async def create_query_executor(connection_id: str):
    """
    动态创建查询执行器

    Args:
        connection_id: 数据库连接ID

    Returns:
        QueryExecutor 实例
    """
    from server.exec.executor import QueryExecutor
    from server.utils.db_pool import get_metadata_pool

    db_pool = await get_metadata_pool()

    # 获取数据库连接信息
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT connection_id, connection_name, db_type,
                   host, port, database_name, username, password_encrypted,
                   is_active
            FROM database_connections
            WHERE connection_id = $1
        """, connection_id)

    if not row:
        raise ValueError(f"数据库连接不存在: {connection_id}")

    if not row['is_active']:
        raise ValueError(f"数据库连接未激活: {connection_id}")

    # 创建executor实例
    executor = QueryExecutor(
        connection_id=str(row['connection_id']),
        db_type=row['db_type'],
        host=row['host'],
        port=row['port'],
        database=row['database_name'],
        username=row['username'],
        password=row['password_encrypted']  #  使用正确的列名
    )

    logger.debug(f"查询执行器创建成功: connection_id={connection_id}, db_type={row['db_type']}")

    return executor


@lru_cache(maxsize=1)
def get_redis_client():
    """获取Redis客户端（单例）"""
    global _redis_client

    if _redis_client is None:
        if not settings.redis_enabled:
            logger.debug("Redis未启用，返回None")
            return None

        try:
            logger.debug("初始化Redis客户端")

            # 优先使用redis_url配置
            if hasattr(settings, 'redis_url') and settings.redis_url:
                import redis
                _redis_client = redis.from_url(
                    settings.redis_url,
                    password=settings.redis_password,
                    decode_responses=True
                )
            else:
                # 使用host/port配置
                import redis
                _redis_client = redis.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    db=settings.redis_db,
                    password=settings.redis_password,
                    decode_responses=True
                )

            # 测试连接
            _redis_client.ping()
            logger.debug("Redis客户端初始化成功")

        except Exception as e:
            logger.error(f"Redis客户端初始化失败: {e}")
            _redis_client = None

    return _redis_client




@lru_cache(maxsize=1)
async def get_db_pool():
    """获取元数据库连接池"""
    from server.utils.db_pool import get_metadata_pool
    return await get_metadata_pool()


def get_redis_client_sync():
    """获取Redis客户端（同步版本）"""
    return get_redis_client()


def get_milvus_client():
    """获取Milvus客户端"""
    try:
        from server.api.admin.milvus import get_milvus_client
        return get_milvus_client()
    except Exception as e:
        logger.warning(f"获取Milvus客户端失败: {e}")
        return None


def get_embedding_client():
    """获取Embedding客户端"""
    try:
        from server.api.admin.milvus import get_embedding_client
        return get_embedding_client()
    except Exception as e:
        logger.warning(f"获取Embedding客户端失败: {e}")
        return None
