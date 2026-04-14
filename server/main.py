"""FastAPI 应用入口"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import date

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server.config import settings
from server.frontend_static import register_frontend_routes

# 根据配置设置系统时区
os.environ.setdefault('TZ', settings.timezone)
from server.api import query, health
from server.api import dify_adapter
from server.api import conversations
from server.trace import api as trace_api

# 🔵 NL2SQL核心查询系统API
# query和health模块提供核心查询功能

# 🟢 管理系统 API
from server.api.admin import (
    auth,            # 用户认证
    auth_providers,  # 认证配置
    auto_sync,       # 自动同步管理
    websocket,       # WebSocket实时通信
    metrics,         # 监控指标
    datasources,     # 数据库连接管理
    tables,          # 数据表配置
    domains,         # 业务域管理
    fields,          # 字段配置（已整合枚举值同步配置）
    joins,           # 表关系管理
    rules,           # 全局规则管理
    milvus,          # Milvus同步
    history,         # 查询历史
    cache,           # 缓存管理
    monitor,         # 系统监控
    metadata_io,     # 元数据批量导入导出
    system_config,   # 系统配置
    permissions,     # 数据权限管理
    user_sync,       # 用户同步
    unified_metadata, # 统一元数据管理
    organizations,   # 组织架构管理
    tokenizer,       # 分词器管理
    model_providers, # 模型供应商管理
    prompts,         # 提示词模板管理
)

from server.dependencies import (
    get_query_cache,
    get_metadata_manager
)

# 配置日志
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=False),  # 使用本地时区
        structlog.processors.JSONRenderer(ensure_ascii=False)  # 显示中文而不转义
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    logger.info("应用启动中", env=settings.app_env, version=settings.app_version)
    
    try:
        # 初始化数据库连接池
        logger.info("正在初始化数据库连接池...")
        from server.utils.db_pool import initialize_pools, get_metadata_pool
        await initialize_pools()
        logger.info("数据库连接池初始化完成")
        
        # 初始化认证管理器（从数据库加载配置）
        try:
            logger.info("正在初始化认证管理器...")
            from server.auth import initialize_auth_manager
            metadata_pool = await get_metadata_pool()
            await initialize_auth_manager(metadata_pool)
            logger.info("认证管理器初始化完成")
        except Exception as e:
            logger.warning("认证管理器初始化失败，使用环境变量配置", error=str(e))
        
        # 多数据库管理工具，不再需要预加载全局语义模型
        # 语义模型按数据库连接动态加载
        logger.info("NL2SQL 启动中...")
        logger.info("多数据库管理模式：语义模型按连接动态加载")
        
        # 初始化缓存
        if settings.cache_enabled:
            logger.info("初始化查询缓存...")
            from server.dependencies import initialize_query_cache
            await initialize_query_cache()

        # 初始化自动同步服务
        try:
            logger.info("初始化自动同步服务...")
            from server.api.admin.auto_sync import init_sync_service
            await init_sync_service()
            logger.info("自动同步服务初始化完成")
        except Exception as e:
            logger.warning("自动同步服务初始化失败", error=str(e))

        # 初始化WebSocket服务
        try:
            logger.info("初始化WebSocket服务...")
            from server.websocket_manager import start_websocket_background_tasks
            await start_websocket_background_tasks()
            logger.info("WebSocket服务初始化完成")
        except Exception as e:
            logger.warning("WebSocket服务初始化失败", error=str(e))

        # 初始化分词器（预热，避免冷启动延迟）
        try:
            logger.info("初始化分词器...")
            from server.nl2ir.tokenizer import Tokenizer
            tokenizer = Tokenizer.get_instance()
            logger.info("分词器初始化完成", stats=tokenizer.get_stats())
        except Exception as e:
            logger.warning("分词器初始化失败，将在首次使用时初始化", error=str(e))

        # 初始化监控指标收集器
        try:
            logger.info("初始化监控指标收集器...")
            from server.monitoring.metrics_collector import MetricsCollector, set_metrics_collector
            from server.utils.db_pool import _metadata_pool
            from server.dependencies import get_redis_client

            # 使用已经初始化的元数据库连接池（不是业务数据库）
            if _metadata_pool is None:
                raise RuntimeError("元数据库连接池未初始化")

            metadata_db_pool = _metadata_pool.get_pool()
            redis_client = get_redis_client()

            metrics_collector = MetricsCollector(metadata_db_pool, redis_client)
            set_metrics_collector(metrics_collector)

            # 启动指标收集
            await metrics_collector.start_collection()
            logger.info("监控指标收集器初始化完成")
        except Exception as e:
            logger.warning("监控指标收集器初始化失败", error=str(e))

        logger.info("应用启动完成", mode="metadata_db")
        
    except Exception as e:
        logger.error("应用启动失败", error=str(e))
        raise
    
    yield
    
    # 关闭时清理连接池
    logger.info("正在关闭数据库连接池...")
    from server.utils.db_pool import close_all_pools
    await close_all_pools()
    logger.info("数据库连接池已关闭")
    
    # 关闭客户端连接池
    try:
        logger.info("关闭Milvus和Embedding客户端连接池...")
        from server.utils.client_pool import close_all_clients
        await close_all_clients()
        logger.info("客户端连接池已关闭")
    except Exception as e:
        logger.warning("关闭客户端连接池失败", error=str(e))
    
    # 关闭时
    logger.info("应用关闭中...")
    
    try:
        # 关闭元数据库连接
        if settings.use_metadata_db:
            manager = get_metadata_manager()
            if manager:
                await manager.close()
                logger.info("元数据库连接已关闭")
        
        # 关闭缓存连接
        if settings.cache_enabled:
            from server.dependencies import _query_cache
            if _query_cache:
                await _query_cache.close()

        # 关闭自动同步服务
        try:
            from server.api.admin.auto_sync import cleanup_sync_service
            await cleanup_sync_service()
            logger.info("自动同步服务已关闭")
        except Exception as e:
            logger.warning("关闭自动同步服务失败", error=str(e))

        # 关闭数据库连接
        from server.exec.connection import get_connection_manager
        conn_mgr = get_connection_manager()
        await conn_mgr.close()

        logger.info("应用已关闭")
        
    except Exception as e:
        logger.error("应用关闭异常", error=str(e))


# 自定义 JSON 序列化，支持 date 类型
def custom_json_serializer(obj):
    """自定义 JSON 序列化器"""
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# 创建 FastAPI 应用
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# 🔵 NL2SQL核心查询系统路由
# ============================================================================
app.include_router(query.router, prefix="/api", tags=["查询"])
app.include_router(health.router, tags=["健康检查"])
app.include_router(trace_api.router, prefix="/api", tags=["查询追踪"])
app.include_router(dify_adapter.router, prefix="/api", tags=["Dify集成"])
app.include_router(conversations.router, prefix="/api", tags=["会话管理"])

# ============================================================================
# 🟢 管理系统路由
# ============================================================================
app.include_router(auth.router, prefix="/api/admin", tags=["管理-用户认证"])
app.include_router(auth_providers.router, prefix="/api/admin", tags=["管理-认证配置"])
app.include_router(auto_sync.router, prefix="/api/admin", tags=["管理-自动同步"])
app.include_router(user_sync.router, prefix="/api/admin", tags=["管理-用户同步"])
app.include_router(websocket.router, prefix="/api/admin", tags=["管理-WebSocket通信"])
app.include_router(metrics.router, prefix="/api/admin", tags=["管理-监控指标"])
app.include_router(datasources.router, prefix="/api/admin", tags=["管理-数据库连接"])
app.include_router(tables.router, prefix="/api/admin", tags=["管理-数据表配置"])
app.include_router(domains.router, prefix="/api/admin", tags=["管理-业务域"])
app.include_router(fields.router, prefix="/api/admin", tags=["管理-字段配置"])
app.include_router(joins.router, prefix="/api/admin", tags=["管理-表关系"])
app.include_router(rules.router, prefix="/api/admin", tags=["管理-全局规则"])
app.include_router(milvus.router, prefix="/api/admin", tags=["管理-Milvus同步"])
app.include_router(cache.router, prefix="/api/admin", tags=["管理-缓存管理"])
app.include_router(history.router, prefix="/api/admin", tags=["管理-查询历史"])
app.include_router(monitor.router, prefix="/api/admin", tags=["管理-系统监控"])
app.include_router(metadata_io.router, prefix="/api/admin", tags=["管理-元数据导入导出"])
app.include_router(system_config.router, prefix="/api/admin", tags=["管理-系统配置"])
app.include_router(permissions.router, prefix="/api/admin", tags=["管理-数据权限"])
app.include_router(organizations.router, prefix="/api/admin", tags=["管理-组织架构"])
app.include_router(unified_metadata.router, prefix="/api/admin", tags=["管理-统一元数据"])
app.include_router(tokenizer.router, prefix="/api/admin", tags=["管理-分词器"])
app.include_router(model_providers.router, prefix="/api/admin", tags=["管理-模型供应商"])
app.include_router(model_providers.scenario_router, prefix="/api/admin", tags=["管理-模型配置"])
app.include_router(prompts.router, prefix="/api/admin", tags=["管理-提示词模板"])
# 注意：默认模型配置已合并到模型配置中，使用 scenario=default

def build_root_payload() -> dict:
    """构建前端未打包时的根路径回退信息。"""
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "env": settings.app_env,
        "description": "NL2SQL - 自然语言转SQL查询系统 + 管理系统",
        "endpoints": {
            "核心查询": {
                "query": "/api/query - 自然语言查询",
                "health": "/api/health - 健康检查"
            },
            "管理系统": {
                "auth": "/api/admin/login - 用户登录",
                "connections": "/api/admin/connections - 数据库连接管理",
                "domains": "/api/admin/domains - 业务域管理",
                "fields": "/api/admin/fields - 字段配置",
                "relationships": "/api/admin/relationships - 表关系管理",
                "rules": "/api/admin/rules - 全局规则管理",
                "milvus": "/api/admin/milvus/sync - Milvus同步"
            },
            "文档": {
                "swagger": "/docs - Swagger UI",
                "redoc": "/redoc - ReDoc文档"
            }
        }
    }


register_frontend_routes(app, fallback_payload=build_root_payload())


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "server.main:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=settings.app_env == "development",
        log_level=settings.log_level.lower()
    )
