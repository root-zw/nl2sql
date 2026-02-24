"""
认证管理器构建与配置加载。
支持从环境变量和数据库两种方式加载配置，数据库配置优先。
"""

from __future__ import annotations

import json
from typing import List, Optional, Dict, Any

import structlog

from server.auth.manager import AuthManager
from server.auth.providers.local import LocalJWTProvider
from server.auth.providers.oidc import OIDCProvider, OIDCProviderConfig
from server.auth.providers.oauth2 import OAuth2Provider, OAuth2ProviderConfig
from server.auth.providers.api_gateway import APIGatewayProvider
from server.auth.providers.external_aes import ExternalAESProvider
from server.auth.base import AuthProvider
from server.config import settings

logger = structlog.get_logger()


def _parse_providers(raw: str) -> List[str]:
    if not raw:
        return ["local"]
    return [item.strip() for item in raw.split(",") if item.strip()]


def _build_oidc_provider_from_config(config: Dict[str, Any], name: str, priority: int, enabled: bool) -> OIDCProvider:
    """从配置字典构建 OIDC Provider"""
    # 获取用户属性声明列表
    user_attribute_claims = config.get("user_attribute_claims")
    if user_attribute_claims is None:
        user_attribute_claims = settings.oidc_user_attribute_claims_list
    elif isinstance(user_attribute_claims, str):
        user_attribute_claims = [a.strip() for a in user_attribute_claims.split(",") if a.strip()]
    
    cfg = OIDCProviderConfig(
        issuer_url=config.get("issuer_url", ""),
        client_id=config.get("client_id", ""),
        client_secret=config.get("client_secret", ""),
        redirect_uri=config.get("redirect_uri", ""),
        scope=config.get("scope", "openid profile email"),
        role_mapping=config.get("role_mapping", {}),
        audiences=config.get("audiences"),
        leeway_seconds=config.get("leeway_seconds", 60),
        name=name,
        # 前端重定向配置
        frontend_redirect_url=config.get("frontend_redirect_url"),
        # 数据角色同步配置
        data_role_claim=config.get("data_role_claim", settings.oidc_data_role_claim),
        auto_create_data_role=config.get("auto_create_data_role", settings.oidc_auto_create_data_role),
        user_attribute_claims=user_attribute_claims,
        # 用户同步 API 配置
        users_api_url=config.get("users_api_url"),
        users_api_token_url=config.get("users_api_token_url"),
        users_api_client_id=config.get("users_api_client_id"),
        users_api_client_secret=config.get("users_api_client_secret"),
        users_api_scope=config.get("users_api_scope"),
        users_field_mapping=config.get("users_field_mapping"),
    )
    return OIDCProvider(config=cfg, priority=priority, enabled=enabled)


def _build_oauth2_provider_from_config(config: Dict[str, Any], name: str, priority: int, enabled: bool) -> OAuth2Provider:
    """从配置字典构建 OAuth 2.0 Provider"""
    # 获取用户属性声明列表
    user_attribute_claims = config.get("user_attribute_claims")
    if user_attribute_claims is None:
        user_attribute_claims = ["department", "region"]
    elif isinstance(user_attribute_claims, str):
        user_attribute_claims = [a.strip() for a in user_attribute_claims.split(",") if a.strip()]
    
    cfg = OAuth2ProviderConfig(
        name=name,
        client_id=config.get("client_id", ""),
        client_secret=config.get("client_secret", ""),
        authorization_endpoint=config.get("authorization_endpoint", ""),
        token_endpoint=config.get("token_endpoint", ""),
        userinfo_endpoint=config.get("userinfo_endpoint", ""),
        redirect_uri=config.get("redirect_uri", ""),
        scope=config.get("scope", ""),
        response_type=config.get("response_type", "code"),
        # Token 请求配置
        token_auth_method=config.get("token_auth_method", "body"),
        token_content_type=config.get("token_content_type", "form"),  # 钉钉等需要 "json"
        token_param_style=config.get("token_param_style", "snake_case"),  # 钉钉需要 "camel_case"
        token_extra_params=config.get("token_extra_params", {}),
        # Userinfo 请求配置
        userinfo_auth_method=config.get("userinfo_auth_method", "header"),
        userinfo_http_method=config.get("userinfo_http_method", "GET"),
        userinfo_token_param=config.get("userinfo_token_param", "access_token"),
        userinfo_token_header=config.get("userinfo_token_header", ""),  # 钉钉需要 x-acs-dingtalk-access-token
        userinfo_extra_params=config.get("userinfo_extra_params", {}),
        userinfo_data_path=config.get("userinfo_data_path", ""),
        # 字段映射
        token_field_mapping=config.get("token_field_mapping", {
            "access_token": "access_token",
            "refresh_token": "refresh_token",
            "expires_in": "expires_in",
        }),
        userinfo_field_mapping=config.get("userinfo_field_mapping", {
            "user_id": "id",
            "username": "username",
            "email": "email",
            "name": "name",
            "phone": "phone",
        }),
        # 角色映射
        role_mapping=config.get("role_mapping", {}),
        roles_field=config.get("roles_field", "roles"),
        # 数据角色配置
        data_role_claim=config.get("data_role_claim", "roles"),
        auto_create_data_role=config.get("auto_create_data_role", True),
        user_attribute_claims=user_attribute_claims,
        # 前端重定向
        frontend_redirect_url=config.get("frontend_redirect_url"),
        # 用户同步配置
        users_api_url=config.get("users_api_url"),
        users_api_token_url=config.get("users_api_token_url"),
        users_field_mapping=config.get("users_field_mapping"),
    )
    return OAuth2Provider(config=cfg, priority=priority, enabled=enabled)


def _build_api_gateway_provider_from_config(config: Dict[str, Any], priority: int, enabled: bool) -> APIGatewayProvider:
    """从配置字典构建 API Gateway Provider"""
    provider = APIGatewayProvider(priority=priority, enabled=enabled, config=config)
    # 覆盖配置
    if config.get("signature_secret"):
        provider.secret = config["signature_secret"]
    if config.get("trusted_ips"):
        trusted_ips = config["trusted_ips"]
        if isinstance(trusted_ips, str):
            provider.trusted_ips = [ip.strip() for ip in trusted_ips.split(",") if ip.strip()]
        elif isinstance(trusted_ips, list):
            provider.trusted_ips = trusted_ips
    return provider


def build_auth_manager_from_env() -> AuthManager:
    """从环境变量构建 AuthManager（向后兼容）"""
    manager = AuthManager()

    # 解析 providers 列表
    providers = _parse_providers(getattr(settings, "auth_providers", ""))
    mode = getattr(settings, "auth_mode", "local").lower()

    # 始终注册本地 JWT（可根据开关禁用）
    local_enabled = "local" in providers or mode in {"local", "chain"}
    manager.register(LocalJWTProvider(priority=100, enabled=local_enabled))

    # OIDC / SSO
    if "oidc" in providers or mode in {"oidc", "chain"}:
        cfg = OIDCProviderConfig(
            issuer_url=settings.oidc_issuer_url or "",
            client_id=settings.oidc_client_id or "",
            client_secret=settings.oidc_client_secret or "",
            redirect_uri=settings.oidc_redirect_uri or "",
            scope=settings.oidc_scope or "openid profile email",
            role_mapping=json.loads(settings.oidc_role_mapping or "{}"),
            audiences=None,
            leeway_seconds=60,
            name="oidc",
            # 数据角色同步配置
            data_role_claim=settings.oidc_data_role_claim,
            auto_create_data_role=settings.oidc_auto_create_data_role,
            user_attribute_claims=settings.oidc_user_attribute_claims_list,
        )
        manager.register(
            OIDCProvider(
                config=cfg,
                priority=80,
                enabled=settings.oidc_enabled,
            )
        )

    # API Gateway
    if "api_gateway" in providers or mode in {"api_gateway", "chain"}:
        manager.register(
            APIGatewayProvider(
                priority=90,
                enabled=settings.api_gateway_enabled,
            )
        )

    # LDAP 预留
    if "ldap" in providers or mode in {"ldap", "chain"}:
        # 动态导入避免循环依赖
        try:
            from server.auth.providers.ldap import LDAPProvider
            ldap_config = {
                "server": settings.ldap_server or "",
                "base_dn": settings.ldap_base_dn or "",
                "bind_dn": settings.ldap_bind_dn or "",
                "bind_password": settings.ldap_bind_password or "",
            }
            manager.register(
                LDAPProvider(
                    config=ldap_config,
                    priority=70,
                    enabled=settings.ldap_enabled,
                )
            )
        except ImportError:
            logger.info("LDAP provider requested but module not found; skip registering")

    return manager


async def _ensure_default_local_provider(db_pool):
    """确保数据库中有默认的本地认证提供者"""
    try:
        row = await db_pool.fetchrow(
            "SELECT provider_id FROM auth_providers WHERE provider_key = 'local'"
        )
        if not row:
            await db_pool.execute(
                """
                INSERT INTO auth_providers (provider_key, provider_type, config_json, enabled, priority)
                VALUES ('local', 'local', '{}'::jsonb, TRUE, 100)
                """
            )
            logger.info("已创建默认本地认证提供者")
    except Exception as e:
        logger.warning("创建默认本地认证提供者失败", error=str(e))


async def build_auth_manager_from_db(db_pool) -> AuthManager:
    """从数据库加载配置构建 AuthManager"""
    manager = AuthManager()
    
    try:
        # 确保有默认的本地认证提供者
        await _ensure_default_local_provider(db_pool)
        
        # 查询所有已启用的认证提供者，按优先级排序
        rows = await db_pool.fetch(
            """
            SELECT provider_key, provider_type, config_json, enabled, priority
            FROM auth_providers
            ORDER BY priority DESC, created_at ASC
            """
        )
        
        if not rows:
            logger.info("数据库中无认证提供者配置，使用环境变量配置")
            return build_auth_manager_from_env()
        
        has_local = False
        
        for row in rows:
            provider_key = row["provider_key"]
            provider_type = row["provider_type"]
            raw_config = row["config_json"]
            # asyncpg 可能返回 JSONB 为字符串，需要解析
            if isinstance(raw_config, str):
                config_json = json.loads(raw_config) if raw_config else {}
            else:
                config_json = raw_config or {}
            enabled = row["enabled"]
            priority = row["priority"]
            
            try:
                if provider_type == "local":
                    has_local = True
                    manager.register(LocalJWTProvider(priority=priority, enabled=enabled))
                    
                elif provider_type == "oidc":
                    provider = _build_oidc_provider_from_config(
                        config_json, 
                        name=provider_key,
                        priority=priority, 
                        enabled=enabled
                    )
                    manager.register(provider)
                    
                elif provider_type == "api_gateway":
                    provider = _build_api_gateway_provider_from_config(
                        config_json,
                        priority=priority,
                        enabled=enabled
                    )
                    manager.register(provider)
                
                elif provider_type == "oauth2":
                    provider = _build_oauth2_provider_from_config(
                        config_json,
                        name=provider_key,
                        priority=priority,
                        enabled=enabled
                    )
                    manager.register(provider)
                    
                elif provider_type == "ldap":
                    try:
                        from server.auth.providers.ldap import LDAPProvider
                        provider = LDAPProvider(
                            config=config_json,
                            priority=priority,
                            enabled=enabled,
                        )
                        manager.register(provider)
                    except ImportError:
                        logger.warning("LDAP provider requested but module not found", provider_key=provider_key)
                
                elif provider_type == "external_aes":
                    provider = ExternalAESProvider(
                        config=config_json,
                        priority=priority,
                        enabled=enabled,
                    )
                    manager.register(provider)
                        
                else:
                    logger.warning("未知的认证提供者类型", provider_type=provider_type, provider_key=provider_key)
                    
            except Exception as e:
                logger.error("加载认证提供者失败", provider_key=provider_key, error=str(e))
                continue
        
        # 如果数据库中没有 local provider，默认添加一个（始终可用作 fallback）
        if not has_local:
            manager.register(LocalJWTProvider(priority=100, enabled=True))
            logger.info("数据库配置中无 local provider，已添加默认 local provider")
        
        logger.info("从数据库加载认证提供者完成", count=len(manager.list_providers()))
        return manager
        
    except Exception as e:
        logger.error("从数据库加载认证配置失败，回退到环境变量配置", error=str(e))
        return build_auth_manager_from_env()


class AuthManagerHolder:
    """
    认证管理器持有者，支持异步初始化和热重载。
    """
    
    _instance: Optional['AuthManagerHolder'] = None
    _manager: Optional[AuthManager] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @property
    def manager(self) -> AuthManager:
        """获取当前的认证管理器"""
        if self._manager is None:
            # 如果还未初始化，使用环境变量配置作为默认
            self._manager = build_auth_manager_from_env()
            logger.warning("AuthManager 未异步初始化，使用环境变量配置")
        return self._manager
    
    async def initialize(self, db_pool=None):
        """
        异步初始化认证管理器。
        
        Args:
            db_pool: 数据库连接池，如果提供则从数据库加载配置
        """
        if self._initialized:
            logger.debug("AuthManager 已初始化，跳过")
            return
        
        if db_pool is not None:
            self._manager = await build_auth_manager_from_db(db_pool)
        else:
            self._manager = build_auth_manager_from_env()
        
        self._initialized = True
        logger.info("AuthManager 初始化完成", providers=[p.name for p in self._manager.list_providers()])
    
    async def reload(self, db_pool=None):
        """
        热重载认证管理器配置。
        
        Args:
            db_pool: 数据库连接池
        """
        if db_pool is not None:
            self._manager = await build_auth_manager_from_db(db_pool)
        else:
            self._manager = build_auth_manager_from_env()
        
        logger.info("AuthManager 已重载", providers=[p.name for p in self._manager.list_providers()])
    
    def get_manager(self) -> AuthManager:
        """获取认证管理器"""
        return self.manager


# 全局单例持有者
_holder = AuthManagerHolder()


def get_auth_manager() -> AuthManager:
    """获取当前的认证管理器"""
    return _holder.manager


async def initialize_auth_manager(db_pool=None):
    """初始化认证管理器（应在应用启动时调用）"""
    await _holder.initialize(db_pool)


async def reload_auth_manager(db_pool=None):
    """重载认证管理器配置"""
    await _holder.reload(db_pool)


# 向后兼容：保持原有的 build_auth_manager 和 auth_manager
def build_auth_manager() -> AuthManager:
    """向后兼容的同步构建函数"""
    return build_auth_manager_from_env()


# 全局单例（向后兼容，但建议使用 get_auth_manager()）
auth_manager = build_auth_manager_from_env()
