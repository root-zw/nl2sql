"""
认证提供者配置 API
整合用户同步功能
"""

from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
import json

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, EmailStr
import structlog

from server.models.auth_provider import (
    AuthProviderCreate,
    AuthProviderUpdate,
    AuthProviderInDB,
)
from server.services.auth_provider_service import AuthProviderService
from server.middleware.auth import require_admin
from server.utils.db_pool import get_metadata_pool
from server.auth import reload_auth_manager, get_auth_manager

logger = structlog.get_logger()

router = APIRouter()


# ============================================================================
# 用户同步相关模型
# ============================================================================

class SyncUserItem(BaseModel):
    """同步用户项"""
    external_uid: str = Field(..., min_length=1, max_length=200)
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    role: str = Field(default="user", description="系统角色，默认 user")
    is_active: bool = True
    profile: Optional[dict] = None


class ProviderSyncRequest(BaseModel):
    """提供者用户同步请求"""
    users: List[SyncUserItem]


class SyncResult(BaseModel):
    """同步结果"""
    provider_key: str
    created: int
    updated: int
    skipped: int


async def get_service():
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield AuthProviderService(conn)


async def get_db_conn():
    """获取数据库连接"""
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


@router.get("/auth-providers", response_model=List[AuthProviderInDB])
async def list_providers(
    svc: AuthProviderService = Depends(get_service),
    current_user=Depends(require_admin),
):
    """获取所有认证提供者配置"""
    return await svc.list()


@router.get("/auth-providers/active")
async def list_active_providers(
    current_user=Depends(require_admin),
):
    """获取当前已加载的认证提供者（运行时状态）"""
    manager = get_auth_manager()
    providers = manager.list_providers()
    return [
        {
            "name": p.name,
            "priority": p.priority,
            "enabled": p.enabled,
        }
        for p in providers
    ]


@router.get("/auth-providers/types")
async def get_provider_types(
    current_user=Depends(require_admin),
):
    """获取支持的认证提供者类型及其配置字段说明"""
    return {
        "types": [
            {
                "type": "local",
                "name": "本地认证",
                "description": "使用本地用户名密码 + JWT Token 认证",
                "config_fields": []
            },
            {
                "type": "oidc",
                "name": "OIDC/SSO",
                "description": "OpenID Connect 单点登录，支持企业统一认证",
                "config_fields": [
                    {"name": "issuer_url", "label": "Issuer URL", "type": "string", "required": True, "placeholder": "https://idp.example.com"},
                    {"name": "client_id", "label": "Client ID", "type": "string", "required": True},
                    {"name": "client_secret", "label": "Client Secret", "type": "password", "required": True},
                    {"name": "redirect_uri", "label": "回调地址", "type": "string", "required": True, "placeholder": "https://your-app.com/api/admin/oidc/callback"},
                    {"name": "frontend_redirect_url", "label": "前端重定向地址", "type": "string", "required": False, "placeholder": "http://localhost:3000", "description": "登录成功后重定向的前端页面地址，留空则使用回调地址同域"},
                    {"name": "scope", "label": "Scope", "type": "string", "required": False, "default": "openid profile email"},
                    {"name": "role_mapping", "label": "系统角色映射", "type": "json", "required": False, "default": {"*": "user"}, "description": "外部角色到系统角色(admin/data_admin/user)的映射"},
                    {"name": "audiences", "label": "允许的 audience 列表", "type": "json", "required": False, "description": "如需支持多受众，可配置数组"},
                    {"name": "leeway_seconds", "label": "时钟偏移容忍秒数", "type": "number", "required": False, "default": 60}
                ],
                # 同步配置字段（显示在同步配置区域）
                "sync_config_fields": [
                    {"name": "users_api_url", "label": "用户列表接口", "type": "string", "required": False, "placeholder": "https://keycloak.example.com/admin/realms/master/users", "description": "用于一键同步的用户列表 API 地址"},
                    {"name": "users_api_token_url", "label": "Token 接口", "type": "string", "required": False, "placeholder": "https://keycloak.example.com/realms/master/protocol/openid-connect/token", "description": "获取访问令牌的接口，留空使用 OIDC discovery"},
                    {"name": "users_api_client_id", "label": "API Client ID", "type": "string", "required": False, "description": "具有用户查询权限的 Client ID，留空使用上面的 Client ID"},
                    {"name": "users_api_client_secret", "label": "API Client Secret", "type": "password", "required": False, "description": "API Client 的密钥，留空使用上面的密钥"},
                    {"name": "users_field_mapping", "label": "用户字段映射", "type": "json", "required": False, "default": {"list_path": "", "id_field": "id", "username_field": "username", "email_field": "email", "first_name_field": "firstName", "last_name_field": "lastName", "enabled_field": "enabled", "roles_field": "roles", "phone_field": "phone", "department_field": "department"}, "description": "响应字段映射，list_path为空表示根数组"},
                    {"name": "data_role_claim", "label": "数据角色 Claim", "type": "string", "required": False, "default": "roles", "description": "从 Token 的哪个 claim 读取数据角色列表（如 roles、groups）"},
                    {"name": "auto_create_data_role", "label": "自动创建数据角色", "type": "boolean", "required": False, "default": True, "description": "Token 中的数据角色不存在时是否自动创建"},
                    {"name": "user_attribute_claims", "label": "用户属性字段", "type": "string", "required": False, "default": "department,region", "description": "需要从 Token 同步到用户属性的字段（逗号分隔）"}
                ]
            },
            {
                "type": "api_gateway",
                "name": "API 网关",
                "description": "从上游网关透传的 Header 获取用户信息",
                "config_fields": [
                    {"name": "signature_secret", "label": "签名密钥", "type": "password", "required": False, "description": "用于校验网关签名"},
                    {"name": "trusted_ips", "label": "可信 IP 列表", "type": "string", "required": False, "placeholder": "192.168.1.1,192.168.1.2", "description": "逗号分隔的可信网关 IP"},
                    {"name": "role_mapping", "label": "系统角色映射", "type": "json", "required": False, "default": {"admin": "admin", "data_admin": "data_admin", "*": "user"}, "description": "将上游角色映射到本地系统角色(admin/data_admin/user)"}
                ],
                # 同步配置字段（显示在同步配置区域）
                "sync_config_fields": [
                    {"name": "users_api_url", "label": "用户列表接口", "type": "string", "required": False, "placeholder": "https://gateway.example.com/api/users", "description": "上游系统用户列表接口地址，用于一键同步"},
                    {"name": "users_api_token", "label": "接口令牌", "type": "password", "required": False, "description": "用户列表接口的认证令牌"},
                    {"name": "user_field_mapping", "label": "用户字段映射", "type": "json", "required": False, "default": {"list_path": "users", "id_field": "id", "username_field": "username", "email_field": "email", "name_field": "name"}, "description": "响应字段映射配置"},
                    {"name": "auto_create_data_role", "label": "自动创建数据角色", "type": "boolean", "required": False, "default": True, "description": "Header 中的数据角色不存在时是否自动创建"}
                ]
            },
            {
                "type": "oauth2",
                "name": "OAuth 2.0",
                "description": "通用 OAuth 2.0 认证，支持钉钉、企业微信、飞书等",
                "config_fields": [
                    {"name": "client_id", "label": "Client ID / AppKey", "type": "string", "required": True, "description": "客户端ID（钉钉：AppKey，企业微信：CorpID，飞书：App ID）"},
                    {"name": "client_secret", "label": "Client Secret / AppSecret", "type": "password", "required": True, "description": "客户端密钥"},
                    {"name": "authorization_endpoint", "label": "授权端点", "type": "string", "required": True, "placeholder": "https://login.dingtalk.com/oauth2/auth", "description": "OAuth 授权页面地址"},
                    {"name": "token_endpoint", "label": "Token 端点", "type": "string", "required": True, "placeholder": "https://api.dingtalk.com/v1.0/oauth2/userAccessToken", "description": "获取 access_token 的接口"},
                    {"name": "userinfo_endpoint", "label": "用户信息端点", "type": "string", "required": True, "placeholder": "https://api.dingtalk.com/v1.0/contact/users/me", "description": "获取用户信息的接口"},
                    {"name": "redirect_uri", "label": "回调地址", "type": "string", "required": True, "placeholder": "https://your-app.com/api/admin/oauth2/callback"},
                    {"name": "frontend_redirect_url", "label": "前端重定向地址", "type": "string", "required": False, "placeholder": "http://localhost:3000", "description": "登录成功后重定向的前端页面地址"},
                    {"name": "scope", "label": "Scope", "type": "string", "required": False, "default": "openid", "description": "授权范围"},
                    {"name": "token_auth_method", "label": "Token 认证方式", "type": "select", "required": False, "default": "body", "options": ["body", "basic_auth", "query"], "description": "获取 Token 时的认证方式"},
                    {"name": "token_content_type", "label": "Token 请求格式", "type": "select", "required": False, "default": "form", "options": ["form", "json"], "description": "Token 请求体格式（钉钉等需要 JSON）"},
                    {"name": "token_param_style", "label": "Token 参数风格", "type": "select", "required": False, "default": "snake_case", "options": ["snake_case", "camel_case"], "description": "Token 请求参数命名风格（钉钉需要 camel_case）"},
                    {"name": "token_extra_params", "label": "Token 额外参数", "type": "json", "required": False, "default": {}, "description": "Token 请求的额外参数"},
                    {"name": "token_field_mapping", "label": "Token 响应字段映射", "type": "json", "required": False, "default": {"access_token": "access_token", "refresh_token": "refresh_token", "expires_in": "expires_in"}, "description": "Token 响应字段映射（钉钉需要 accessToken/refreshToken/expireIn）"},
                    {"name": "userinfo_auth_method", "label": "Userinfo 认证方式", "type": "select", "required": False, "default": "header", "options": ["header", "query", "body"], "description": "获取用户信息时的认证方式"},
                    {"name": "userinfo_http_method", "label": "Userinfo HTTP 方法", "type": "select", "required": False, "default": "GET", "options": ["GET", "POST"]},
                    {"name": "userinfo_token_header", "label": "Token Header 名称", "type": "string", "required": False, "placeholder": "x-acs-dingtalk-access-token", "description": "自定义 Token Header（钉钉需要 x-acs-dingtalk-access-token，留空使用标准 Authorization: Bearer）"},
                    {"name": "userinfo_data_path", "label": "用户信息数据路径", "type": "string", "required": False, "placeholder": "result", "description": "用户信息在响应中的嵌套路径（如钉钉的 result）"},
                    {"name": "userinfo_field_mapping", "label": "用户字段映射", "type": "json", "required": False, "default": {"user_id": "id", "username": "username", "email": "email", "name": "name", "phone": "phone"}, "description": "用户信息字段映射"},
                    {"name": "role_mapping", "label": "系统角色映射", "type": "json", "required": False, "default": {"*": "user"}, "description": "外部角色到系统角色(admin/data_admin/user)的映射"},
                ],
                "sync_config_fields": [
                    {"name": "users_api_url", "label": "用户列表接口", "type": "string", "required": False, "description": "用于一键同步的用户列表 API 地址"},
                    {"name": "users_api_token_url", "label": "Token 接口", "type": "string", "required": False, "description": "获取 API 访问令牌的接口"},
                    {"name": "users_field_mapping", "label": "用户字段映射", "type": "json", "required": False, "default": {"list_path": "", "id_field": "id", "username_field": "username", "email_field": "email", "name_field": "name", "phone_field": "phone", "department_field": "department"}, "description": "响应字段映射"},
                    {"name": "data_role_claim", "label": "数据角色字段", "type": "string", "required": False, "default": "roles", "description": "用户信息中的角色字段名"},
                    {"name": "auto_create_data_role", "label": "自动创建数据角色", "type": "boolean", "required": False, "default": True},
                    {"name": "user_attribute_claims", "label": "用户属性字段", "type": "string", "required": False, "default": "department,region", "description": "需要同步的用户属性字段（逗号分隔）"}
                ],
                "presets": [
                    {
                        "name": "dingtalk",
                        "label": "钉钉",
                        "description": "钉钉企业应用 OAuth 2.0",
                        "config": {
                            "authorization_endpoint": "https://login.dingtalk.com/oauth2/auth",
                            "token_endpoint": "https://api.dingtalk.com/v1.0/oauth2/userAccessToken",
                            "userinfo_endpoint": "https://api.dingtalk.com/v1.0/contact/users/me",
                            "scope": "openid",
                            "token_auth_method": "body",
                            "token_content_type": "json",
                            "token_param_style": "camel_case",
                            "token_field_mapping": {"access_token": "accessToken", "refresh_token": "refreshToken", "expires_in": "expireIn", "token_type": "token_type"},
                            "userinfo_auth_method": "header",
                            "userinfo_http_method": "GET",
                            "userinfo_token_header": "x-acs-dingtalk-access-token",
                            "userinfo_field_mapping": {"user_id": "unionId", "username": "nick", "email": "email", "name": "nick", "phone": "mobile", "avatar": "avatarUrl"}
                        }
                    },
                    {
                        "name": "feishu",
                        "label": "飞书",
                        "description": "飞书企业应用 OAuth 2.0",
                        "config": {
                            "authorization_endpoint": "https://open.feishu.cn/open-apis/authen/v1/authorize",
                            "token_endpoint": "https://open.feishu.cn/open-apis/authen/v1/oidc/access_token",
                            "userinfo_endpoint": "https://open.feishu.cn/open-apis/authen/v1/user_info",
                            "scope": "",
                            "token_auth_method": "body",
                            "token_extra_params": {"grant_type": "authorization_code"},
                            "userinfo_auth_method": "header",
                            "userinfo_http_method": "GET",
                            "userinfo_data_path": "data",
                            "userinfo_field_mapping": {"user_id": "union_id", "username": "name", "email": "email", "name": "name", "phone": "mobile", "avatar": "avatar_url"}
                        }
                    }
                ]
            },
            {
                "type": "ldap",
                "name": "LDAP/AD",
                "description": "LDAP 或 Active Directory 认证",
                "config_fields": [
                    {"name": "server", "label": "服务器地址", "type": "string", "required": True, "placeholder": "ldap://ldap.example.com:389"},
                    {"name": "base_dn", "label": "Base DN", "type": "string", "required": True, "placeholder": "dc=example,dc=com"},
                    {"name": "bind_dn", "label": "绑定 DN", "type": "string", "required": True, "placeholder": "cn=admin,dc=example,dc=com"},
                    {"name": "bind_password", "label": "绑定密码", "type": "password", "required": True},
                    {"name": "user_search_filter", "label": "用户搜索过滤器", "type": "string", "required": False, "default": "(sAMAccountName={username})"},
                    {"name": "user_search_base", "label": "用户搜索 Base", "type": "string", "required": False, "placeholder": ""},
                    {"name": "attr_username", "label": "用户名属性", "type": "string", "required": False, "default": "sAMAccountName", "placeholder": "AD 推荐 sAMAccountName"},
                    {"name": "attr_email", "label": "邮箱属性", "type": "string", "required": False, "default": "mail"},
                    {"name": "attr_full_name", "label": "姓名属性", "type": "string", "required": False, "default": "cn"},
                    {"name": "attr_member_of", "label": "成员属性", "type": "string", "required": False, "default": "memberOf", "description": "用于角色映射的组/成员属性"},
                    {"name": "use_ssl", "label": "使用 SSL", "type": "boolean", "required": False, "default": False},
                    {"name": "start_tls", "label": "使用 StartTLS", "type": "boolean", "required": False, "default": False},
                    {"name": "role_mapping", "label": "系统角色映射", "type": "json", "required": False, "default": {"*": "user"}, "description": "LDAP 组到系统角色(admin/data_admin/user)的映射"}
                ],
                # 同步配置字段（显示在同步配置区域）
                "sync_config_fields": [
                    {"name": "user_list_filter", "label": "用户列表过滤器", "type": "string", "required": False, "default": "(&(objectClass=user)(!(objectClass=computer)))", "description": "一键同步时获取所有用户的 LDAP 过滤器"}
                ]
            },
            {
                "type": "external_aes",
                "name": "外部 AES Token",
                "description": "通过 AES 加密 Token 与外部系统对接认证，适用于跨系统单点登录",
                "config_fields": [
                    # ============ Token 格式配置 ============
                    {"name": "token_format", "label": "Token 明文格式", "type": "select", "required": True, "default": "simple", "options": [
                        {"value": "simple", "label": "简单模式: 角色|时间戳|ServiceKey"},
                        {"value": "with_user", "label": "完整用户模式: 用户ID|用户名|角色|时间戳|ServiceKey"},
                        {"value": "with_username", "label": "用户名模式: 用户名|角色|时间戳|ServiceKey"},
                        {"value": "role_only", "label": "仅角色模式: 角色|时间戳|ServiceKey"},
                        {"value": "custom", "label": "自定义格式"}
                    ], "description": "Token 明文的字段组成格式"},
                    {"name": "custom_format", "label": "自定义格式模板", "type": "string", "required": False, "placeholder": "{user_id}|{username}|{role}|{timestamp}|{service_key}", "description": "仅当选择自定义格式时生效，支持占位符：{user_id}, {username}, {role}, {timestamp}, {service_key}"},
                    {"name": "field_separator", "label": "字段分隔符", "type": "string", "required": False, "default": "|", "description": "Token 各字段之间的分隔符"},
                    {"name": "timestamp_format", "label": "时间戳格式", "type": "string", "required": False, "default": "%Y-%m-%d %H:%M:%S", "placeholder": "%Y-%m-%d %H:%M:%S", "description": "Python strptime 格式"},
                    
                    # ============ 加密配置 ============
                    {"name": "algorithm", "label": "加密算法", "type": "select", "required": True, "default": "AES-128-CBC", "options": [
                        {"value": "AES-128-CBC", "label": "AES-128-CBC（密钥16字节）"},
                        {"value": "AES-192-CBC", "label": "AES-192-CBC（密钥24字节）"},
                        {"value": "AES-256-CBC", "label": "AES-256-CBC（密钥32字节）"},
                        {"value": "AES-128-GCM", "label": "AES-128-GCM（认证加密，密钥16字节）"},
                        {"value": "AES-192-GCM", "label": "AES-192-GCM（认证加密，密钥24字节）"},
                        {"value": "AES-256-GCM", "label": "AES-256-GCM（认证加密，密钥32字节）"},
                        {"value": "AES-128-CTR", "label": "AES-128-CTR（流模式，密钥16字节）"},
                        {"value": "AES-256-CTR", "label": "AES-256-CTR（流模式，密钥32字节）"},
                        {"value": "AES-128-CFB", "label": "AES-128-CFB（密钥16字节）"},
                        {"value": "AES-256-CFB", "label": "AES-256-CFB（密钥32字节）"},
                        {"value": "AES-128-OFB", "label": "AES-128-OFB（密钥16字节）"},
                        {"value": "AES-256-OFB", "label": "AES-256-OFB（密钥32字节）"},
                        {"value": "AES-128-ECB", "label": "AES-128-ECB（不推荐，无IV）"},
                        {"value": "AES-256-ECB", "label": "AES-256-ECB（不推荐，无IV）"},
                        {"value": "SM4-CBC", "label": "SM4-CBC（国密，密钥16字节）"},
                        {"value": "SM4-GCM", "label": "SM4-GCM（国密认证加密，密钥16字节）"},
                        {"value": "SM4-CTR", "label": "SM4-CTR（国密流模式，密钥16字节）"}
                    ], "description": "AES/SM4 加密算法和模式"},
                    {"name": "aes_key", "label": "加密密钥", "type": "password", "required": True, "placeholder": "16/24/32字节字符串", "description": "AES 加密密钥，长度需匹配算法要求"},
                    {"name": "aes_iv", "label": "初始化向量 (IV)", "type": "password", "required": False, "placeholder": "16字节字符串", "description": "固定 IV 模式下需要配置，16字节；ECB/随机IV模式无需配置"},
                    {"name": "iv_mode", "label": "IV 模式", "type": "select", "required": False, "default": "fixed", "options": [
                        {"value": "fixed", "label": "固定 IV（需配置上方 IV）"},
                        {"value": "prepend", "label": "随机 IV（IV 附加在密文前）"},
                        {"value": "append", "label": "随机 IV（IV 附加在密文后）"}
                    ], "description": "ECB模式无需IV；固定IV简单但安全性略低，随机IV更安全"},
                    {"name": "encoding", "label": "编码方式", "type": "select", "required": False, "default": "base64", "options": [
                        {"value": "base64", "label": "Base64（标准）"},
                        {"value": "base64url", "label": "Base64 URL Safe（URL安全）"},
                        {"value": "hex", "label": "十六进制"}
                    ], "description": "加密后的二进制数据编码方式"},
                    {"name": "padding", "label": "填充方式", "type": "select", "required": False, "default": "pkcs7", "options": [
                        {"value": "pkcs7", "label": "PKCS7（推荐）"},
                        {"value": "zero", "label": "零填充"},
                        {"value": "none", "label": "无填充（流模式）"}
                    ], "description": "块加密填充方式，流模式(CTR/CFB/OFB/GCM)无需填充"},
                    
                    # ============ 验证配置 ============
                    {"name": "service_keys", "label": "ServiceKey 列表", "type": "textarea", "required": True, "placeholder": "每行一个 ServiceKey\n8DE9B8B467DDE857CC96A7B3027CBBCD\nOLD_KEY_FOR_MIGRATION", "description": "允许的 ServiceKey，每行一个或逗号分隔。支持多个用于平滑轮换"},
                    {"name": "validity_minutes", "label": "Token 有效期（分钟）", "type": "number", "required": False, "default": 5, "description": "Token 时间戳的有效期，超过则拒绝访问"},
                    {"name": "allow_clock_skew", "label": "允许时钟偏差", "type": "boolean", "required": False, "default": True, "description": "考虑服务器时间同步差异，验证时增加容差"},
                    {"name": "clock_skew_seconds", "label": "时钟偏差容忍（秒）", "type": "number", "required": False, "default": 30, "description": "允许的最大时钟偏差秒数"},
                    
                    # ============ 角色配置 ============
                    {"name": "role_field_is_data_role", "label": "角色字段作为数据角色", "type": "boolean", "required": False, "default": True, "description": "开启后，Token 中的角色作为数据角色（用于数据权限）。关闭后下方角色验证配置无效"},
                    {"name": "strict_role_validation", "label": "严格角色验证（推荐）", "type": "boolean", "required": False, "default": True, "description": "【默认开启】数据角色必须在本地数据库中存在才能认证通过。关闭后可启用下方自动创建"},
                    {"name": "auto_create_data_role", "label": "自动创建数据角色", "type": "boolean", "required": False, "default": False, "description": "【仅严格验证关闭时生效】角色不存在时自动创建。严格模式下此选项被忽略"},
                    {"name": "role_mapping", "label": "系统角色映射", "type": "json", "required": False, "default": {"*": "user"}, "description": "外部角色到系统角色(admin/data_admin/user)的映射，* 为默认"},
                    {"name": "default_system_role", "label": "默认系统角色", "type": "select", "required": False, "default": "user", "options": ["admin", "data_admin", "user"], "description": "未匹配到映射规则时使用的系统角色"},
                    
                    # ============ 前端对接配置 ============
                    {"name": "token_location", "label": "Token 传递位置", "type": "select", "required": False, "default": "both", "options": [
                        {"value": "header", "label": "仅 Header（Authorization: Bearer xxx）"},
                        {"value": "query", "label": "仅 URL 参数（?token=xxx）"},
                        {"value": "both", "label": "两者都支持（优先 Header）"}
                    ], "description": "前端如何传递 Token"},
                    {"name": "query_param_name", "label": "URL 参数名", "type": "string", "required": False, "default": "token", "description": "当 Token 通过 URL 参数传递时的参数名"},
                    {"name": "header_name", "label": "自定义 Header 名称", "type": "string", "required": False, "placeholder": "留空使用标准 Authorization", "description": "自定义 Header 名称，如 X-External-Token"},
                    {"name": "frontend_redirect_url", "label": "前端重定向地址", "type": "string", "required": False, "placeholder": "http://localhost:3000", "description": "外部系统登录成功后重定向的前端页面地址"},
                    
                    # ============ 用户创建配置 ============
                    {"name": "auto_create_user", "label": "自动创建用户", "type": "boolean", "required": False, "default": True, "description": "Token 验证通过后，用户不存在时是否自动创建"},
                    {"name": "user_id_field", "label": "用户ID来源字段", "type": "select", "required": False, "default": "role", "options": [
                        {"value": "user_id", "label": "使用 Token 中的 user_id"},
                        {"value": "username", "label": "使用 Token 中的 username"},
                        {"value": "role", "label": "使用 Token 中的 role（简单模式）"}
                    ], "description": "作为用户唯一标识的字段"}
                ],
                "sync_config_fields": []
            }
        ]
    }


@router.post("/auth-providers/reload")
async def reload_providers(
    current_user=Depends(require_admin),
):
    """
    热重载认证提供者配置。
    从数据库重新加载所有认证提供者配置，无需重启服务。
    """
    try:
        pool = await get_metadata_pool()
        await reload_auth_manager(pool)
        
        # 返回重载后的提供者列表
        manager = get_auth_manager()
        providers = manager.list_providers()
        
        logger.info("认证提供者已重载", count=len(providers))
        
        return {
            "success": True,
            "message": "认证提供者配置已重载",
            "providers": [
                {
                    "name": p.name,
                    "priority": p.priority,
                    "enabled": p.enabled,
                }
                for p in providers
            ]
        }
    except Exception as e:
        logger.error("重载认证提供者失败", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"重载失败: {str(e)}"
        )


@router.post("/auth-providers", response_model=AuthProviderInDB, status_code=status.HTTP_201_CREATED)
async def create_provider(
    data: AuthProviderCreate,
    svc: AuthProviderService = Depends(get_service),
    current_user=Depends(require_admin),
):
    """创建认证提供者配置"""
    existing = await svc.get_by_key(data.provider_key)
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="provider_key 已存在")
    result = await svc.create(data)
    logger.info("认证提供者已创建", provider_key=data.provider_key, provider_type=data.provider_type)
    return result


@router.put("/auth-providers/{provider_id}", response_model=AuthProviderInDB)
async def update_provider(
    provider_id: UUID,
    data: AuthProviderUpdate,
    svc: AuthProviderService = Depends(get_service),
    current_user=Depends(require_admin),
):
    """更新认证提供者配置"""
    updated = await svc.update(provider_id, data)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    logger.info("认证提供者已更新", provider_id=str(provider_id))
    return updated


@router.delete("/auth-providers/{provider_id}")
async def delete_provider(
    provider_id: UUID,
    svc: AuthProviderService = Depends(get_service),
    conn=Depends(get_db_conn),
    current_user=Depends(require_admin),
):
    """
    删除认证提供者配置。
    - local 提供者禁止删除，可禁用。
    - 删除前自动清理该提供者同步的用户（按 external_idp 匹配），以免留下孤儿数据。
    """
    provider = await svc.get(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    if provider.provider_key == "local" or provider.provider_type == "local":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="本地认证不允许删除，可选择禁用")
    
    # 清理同步用户
    deleted_users = 0
    try:
        res = await conn.execute(
            "DELETE FROM users WHERE external_idp = $1",
            provider.provider_key,
        )
        # execute 返回类似 'DELETE 3'
        try:
            deleted_users = int(res.split(" ")[1])
        except Exception:
            deleted_users = 0
    except Exception as e:
        logger.error("删除认证提供者前清理用户失败", provider_key=provider.provider_key, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清理同步用户失败: {str(e)}",
        )
    
    ok = await svc.delete(provider_id)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    logger.info("认证提供者已删除", provider_id=str(provider_id), provider_key=provider.provider_key, deleted_users=deleted_users)
    return {"success": True, "deleted_users": deleted_users}


@router.post("/auth-providers/{provider_id}/test")
async def test_provider(
    provider_id: UUID,
    svc: AuthProviderService = Depends(get_service),
    current_user=Depends(require_admin),
):
    """
    测试认证提供者连接。
    对于 OIDC，测试 discovery 端点；对于 LDAP，测试绑定连接。
    """
    provider_config = await svc.get(provider_id)
    if not provider_config:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    provider_type = provider_config.provider_type
    config = provider_config.config_json or {}
    
    try:
        if provider_type == "local":
            return {"success": True, "message": "本地认证无需测试连接"}
        
        elif provider_type == "oidc":
            # 测试 OIDC discovery
            from server.auth.providers.oidc import OIDCProvider, OIDCProviderConfig
            import httpx
            
            issuer_url = config.get("issuer_url", "")
            if not issuer_url:
                return {"success": False, "message": "未配置 issuer_url"}
            
            discovery_url = f"{issuer_url.rstrip('/')}/.well-known/openid-configuration"
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(discovery_url)
                resp.raise_for_status()
                discovery = resp.json()
            
            return {
                "success": True,
                "message": "OIDC Discovery 成功",
                "details": {
                    "issuer": discovery.get("issuer"),
                    "authorization_endpoint": discovery.get("authorization_endpoint"),
                    "token_endpoint": discovery.get("token_endpoint"),
                    "jwks_uri": discovery.get("jwks_uri"),
                }
            }
        
        elif provider_type == "ldap":
            # 测试 LDAP 连接
            try:
                from server.auth.providers.ldap import LDAPProvider
                ldap_provider = LDAPProvider(config=config, priority=0, enabled=True)
                result = await ldap_provider.test_connection()
                return result
            except ImportError:
                return {"success": False, "message": "ldap3 库未安装"}
        
        elif provider_type == "api_gateway":
            return {"success": True, "message": "API 网关认证无需测试连接，配置将在请求时生效"}
        
        elif provider_type == "oauth2":
            # 测试 OAuth 2.0 端点可达性
            import httpx
            
            auth_endpoint = config.get("authorization_endpoint", "")
            token_endpoint = config.get("token_endpoint", "")
            userinfo_endpoint = config.get("userinfo_endpoint", "")
            
            if not auth_endpoint or not token_endpoint:
                return {"success": False, "message": "未配置授权端点或 Token 端点"}
            
            # 简单测试端点可达性（HEAD 请求）
            async with httpx.AsyncClient(timeout=10) as client:
                try:
                    resp = await client.head(auth_endpoint, follow_redirects=True)
                    # OAuth 2.0 授权端点通常返回 200 或 302
                    auth_ok = resp.status_code in (200, 302, 400, 401, 405)
                except Exception as e:
                    return {"success": False, "message": f"授权端点不可达: {str(e)}"}
            
            return {
                "success": True,
                "message": "OAuth 2.0 端点可达",
                "details": {
                    "authorization_endpoint": auth_endpoint,
                    "token_endpoint": token_endpoint,
                    "userinfo_endpoint": userinfo_endpoint,
                }
            }
        
        elif provider_type == "external_aes":
            # 测试 AES 配置是否有效
            try:
                from server.auth.providers.external_aes import ExternalAESProvider, create_test_token
                
                # 验证配置
                aes_key = config.get("aes_key", "")
                service_keys = config.get("service_keys", "")
                algorithm = config.get("algorithm", "AES-128-CBC")
                
                if not aes_key:
                    return {"success": False, "message": "未配置加密密钥 (aes_key)"}
                if not service_keys:
                    return {"success": False, "message": "未配置 ServiceKey 列表"}
                
                # 尝试创建 Provider 实例验证配置
                provider = ExternalAESProvider(config=config, priority=0, enabled=True)
                
                # 生成测试 Token 并验证解密
                test_service_key = provider.service_keys[0] if provider.service_keys else ""
                aes_iv = config.get("aes_iv", "")
                
                if provider.iv_mode == "fixed" and not aes_iv and algorithm not in ("AES-128-ECB", "AES-192-ECB", "AES-256-ECB"):
                    return {"success": False, "message": "固定 IV 模式需要配置初始化向量 (aes_iv)"}
                
                # 尝试生成并解密测试 Token
                try:
                    test_token = create_test_token(
                        role="test_role",
                        aes_key=aes_key,
                        aes_iv=aes_iv or "0" * 16,
                        service_key=test_service_key,
                        algorithm=algorithm,
                        token_format=config.get("token_format", "simple"),
                        encoding=config.get("encoding", "base64"),
                    )
                    
                    # 验证能否解密
                    from server.auth.providers.external_aes import ExternalAESProvider
                    encrypted_data = provider._decode_token(test_token)
                    if encrypted_data:
                        plaintext = provider._decrypt_token(encrypted_data)
                        if plaintext:
                            return {
                                "success": True,
                                "message": "AES 加密配置验证成功",
                                "details": {
                                    "algorithm": algorithm,
                                    "token_format": config.get("token_format", "simple"),
                                    "encoding": config.get("encoding", "base64"),
                                    "iv_mode": config.get("iv_mode", "fixed"),
                                    "service_keys_count": len(provider.service_keys),
                                    "validity_minutes": config.get("validity_minutes", 5),
                                    "test_plaintext_preview": plaintext[:50] + "..." if len(plaintext) > 50 else plaintext,
                                }
                            }
                    return {"success": False, "message": "Token 解密验证失败，请检查密钥和 IV 配置"}
                except Exception as e:
                    return {"success": False, "message": f"Token 生成/解密测试失败: {str(e)}"}
                    
            except ValueError as e:
                return {"success": False, "message": str(e)}
            except ImportError as e:
                return {"success": False, "message": f"pycryptodome 库未安装: {str(e)}"}
        
        else:
            return {"success": False, "message": f"未知的提供者类型: {provider_type}"}
            
    except httpx.HTTPStatusError as e:
        return {"success": False, "message": f"HTTP 请求失败: {e.response.status_code}"}
    except Exception as e:
        logger.warning("测试认证提供者失败", provider_id=str(provider_id), error=str(e))
        return {"success": False, "message": f"测试失败: {str(e)}"}


def _hash_password(password: str) -> str:
    """哈希密码"""
    import bcrypt
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


async def _bind_default_data_roles(conn, user_id: UUID):
    """绑定默认数据角色"""
    from server.api.admin.auth import _bind_default_data_roles as bind_roles
    await bind_roles(conn, user_id)


@router.get("/auth-providers/{provider_id}/users")
async def get_provider_users(
    provider_id: UUID,
    svc: AuthProviderService = Depends(get_service),
    conn = Depends(get_db_conn),
    current_user = Depends(require_admin),
):
    """获取指定认证提供者的已同步用户列表"""
    provider = await svc.get(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    provider_key = provider.provider_key
    
    rows = await conn.fetch(
        """
        SELECT user_id, username, email, full_name, role, is_active, 
               external_uid, profile_json, created_at, last_login_at
        FROM users
        WHERE external_idp = $1
        ORDER BY created_at DESC
        """,
        provider_key,
    )
    
    return {
        "provider_id": str(provider_id),
        "provider_key": provider_key,
        "total": len(rows),
        "users": [
            {
                "user_id": str(r["user_id"]),
                "username": r["username"],
                "email": r["email"],
                "full_name": r["full_name"],
                "role": r["role"],
                "is_active": r["is_active"],
                "external_uid": r["external_uid"],
                "profile": r["profile_json"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "last_login_at": r["last_login_at"].isoformat() if r["last_login_at"] else None,
            }
            for r in rows
        ]
    }


@router.post("/auth-providers/{provider_id}/sync-users", response_model=SyncResult)
async def sync_provider_users(
    provider_id: UUID,
    request: ProviderSyncRequest,
    svc: AuthProviderService = Depends(get_service),
    conn = Depends(get_db_conn),
    current_user = Depends(require_admin),
):
    """为指定认证提供者同步用户"""
    provider = await svc.get(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    provider_key = provider.provider_key
    created = 0
    updated = 0
    skipped = 0
    
    for user in request.users:
        try:
            # 检查用户是否已存在
            row = await conn.fetchrow(
                "SELECT user_id FROM users WHERE external_idp = $1 AND external_uid = $2",
                provider_key,
                user.external_uid,
            )
            
            profile_json = json.dumps(user.profile or {}, ensure_ascii=False)
            
            if not row:
                # 创建新用户
                password_hash = _hash_password(uuid4().hex)
                username = user.username or f"{provider_key}_{user.external_uid[:8]}"
                
                new_user = await conn.fetchrow(
                    """
                    INSERT INTO users (username, password_hash, email, full_name, role, is_active, 
                                       external_idp, external_uid, profile_json)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING user_id
                    """,
                    username,
                    password_hash,
                    user.email,
                    user.full_name,
                    user.role or "user",
                    user.is_active,
                    provider_key,
                    user.external_uid,
                    profile_json,
                )
                
                # 绑定默认数据角色
                await _bind_default_data_roles(conn, new_user["user_id"])
                created += 1
            else:
                # 更新现有用户
                # 注意：不更新 role 字段，保留管理员手动修改的角色
                await conn.execute(
                    """
                    UPDATE users
                    SET email = $1, full_name = $2, is_active = $3, 
                        profile_json = $4, updated_at = NOW()
                    WHERE external_idp = $5 AND external_uid = $6
                    """,
                    user.email,
                    user.full_name,
                    user.is_active,
                    profile_json,
                    provider_key,
                    user.external_uid,
                )
                updated += 1
                
        except Exception as e:
            logger.warning("同步用户失败", provider_key=provider_key, external_uid=user.external_uid, error=str(e))
            skipped += 1
    
    logger.info("用户同步完成", provider_key=provider_key, created=created, updated=updated, skipped=skipped)
    
    return SyncResult(
        provider_key=provider_key,
        created=created,
        updated=updated,
        skipped=skipped,
    )


@router.get("/auth-providers/{provider_id}/sync-stats")
async def get_provider_sync_stats(
    provider_id: UUID,
    svc: AuthProviderService = Depends(get_service),
    conn = Depends(get_db_conn),
    current_user = Depends(require_admin),
):
    """获取指定认证提供者的用户同步统计"""
    provider = await svc.get(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    provider_key = provider.provider_key
    
    row = await conn.fetchrow(
        """
        SELECT 
            COUNT(*) as total_users,
            COUNT(*) FILTER (WHERE is_active = TRUE) as active_users,
            COUNT(*) FILTER (WHERE last_login_at IS NOT NULL) as logged_in_users,
            MAX(created_at) as last_sync_at,
            MAX(last_login_at) as last_login_at
        FROM users
        WHERE external_idp = $1
        """,
        provider_key,
    )
    
    return {
        "provider_id": str(provider_id),
        "provider_key": provider_key,
        "total_users": row["total_users"] or 0,
        "active_users": row["active_users"] or 0,
        "logged_in_users": row["logged_in_users"] or 0,
        "last_sync_at": row["last_sync_at"].isoformat() if row["last_sync_at"] else None,
        "last_login_at": row["last_login_at"].isoformat() if row["last_login_at"] else None,
    }


@router.delete("/auth-providers/{provider_id}/users/{user_id}")
async def remove_provider_user(
    provider_id: UUID,
    user_id: UUID,
    svc: AuthProviderService = Depends(get_service),
    conn = Depends(get_db_conn),
    current_user = Depends(require_admin),
):
    """从指定认证提供者移除用户（仅标记为非活动，不删除）"""
    provider = await svc.get(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    provider_key = provider.provider_key
    
    result = await conn.execute(
        """
        UPDATE users SET is_active = FALSE, updated_at = NOW()
        WHERE user_id = $1 AND external_idp = $2
        """,
        user_id,
        provider_key,
    )
    
    if result == "UPDATE 0":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到用户")
    
    return {"success": True, "message": "用户已禁用"}


@router.post("/auth-providers/{provider_id}/fetch-users")
async def fetch_provider_users(
    provider_id: UUID,
    svc: AuthProviderService = Depends(get_service),
    current_user = Depends(require_admin),
):
    """
    从外部认证系统自动获取用户列表（一键获取）。
    
    根据提供者类型：
    - LDAP: 从 LDAP 服务器搜索获取所有用户
    - OIDC: 从 Admin API（如 Keycloak）获取用户列表
    - API 网关: 调用上游系统的用户列表接口
    
    返回获取到的用户列表，可直接用于同步。
    """
    provider = await svc.get(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    provider_type = provider.provider_type
    config = provider.config_json or {}
    
    try:
        if provider_type == "ldap":
            from server.auth.providers.ldap import LDAPProvider
            ldap_provider = LDAPProvider(config=config)
            result = await ldap_provider.list_users()
            
        elif provider_type == "oidc":
            from server.auth.providers.oidc import OIDCProvider, OIDCProviderConfig
            oidc_config = OIDCProviderConfig(
                issuer_url=config.get("issuer_url", ""),
                client_id=config.get("client_id", ""),
                client_secret=config.get("client_secret", ""),
                redirect_uri=config.get("redirect_uri", ""),
                scope=config.get("scope", "openid profile email"),
                role_mapping=config.get("role_mapping", {}),
                users_api_url=config.get("users_api_url"),
                users_api_token_url=config.get("users_api_token_url"),
                users_api_client_id=config.get("users_api_client_id"),
                users_api_client_secret=config.get("users_api_client_secret"),
                users_field_mapping=config.get("users_field_mapping"),
            )
            oidc_provider = OIDCProvider(config=oidc_config)
            result = await oidc_provider.list_users()
            
        elif provider_type == "api_gateway":
            from server.auth.providers.api_gateway import APIGatewayProvider
            gateway_provider = APIGatewayProvider(config=config)
            result = await gateway_provider.list_users()
        
        elif provider_type == "oauth2":
            from server.auth.providers.oauth2 import OAuth2Provider, OAuth2ProviderConfig
            oauth2_config = OAuth2ProviderConfig(
                name=provider.provider_key,
                client_id=config.get("client_id", ""),
                client_secret=config.get("client_secret", ""),
                authorization_endpoint=config.get("authorization_endpoint", ""),
                token_endpoint=config.get("token_endpoint", ""),
                userinfo_endpoint=config.get("userinfo_endpoint", ""),
                redirect_uri=config.get("redirect_uri", ""),
                scope=config.get("scope", ""),
                role_mapping=config.get("role_mapping", {}),
                users_api_url=config.get("users_api_url"),
                users_api_token_url=config.get("users_api_token_url"),
                users_field_mapping=config.get("users_field_mapping"),
                user_attribute_claims=config.get("user_attribute_claims", ["department", "region"]),
            )
            oauth2_provider = OAuth2Provider(config=oauth2_config)
            result = await oauth2_provider.list_users()
            
        elif provider_type == "local":
            return {
                "success": False,
                "message": "本地认证不支持从外部获取用户列表",
                "users": []
            }
        else:
            return {
                "success": False,
                "message": f"未知的提供者类型: {provider_type}",
                "users": []
            }
        
        return result
        
    except Exception as e:
        logger.error("获取用户列表失败", provider_id=str(provider_id), error=str(e))
        return {
            "success": False,
            "message": f"获取用户列表失败: {str(e)}",
            "users": []
        }


@router.post("/auth-providers/{provider_id}/auto-sync")
async def auto_sync_provider_users(
    provider_id: UUID,
    svc: AuthProviderService = Depends(get_service),
    conn = Depends(get_db_conn),
    current_user = Depends(require_admin),
):
    """
    一键同步：自动从外部系统获取用户列表并同步到本地。
    
    这是 fetch-users + sync-users 的组合操作。
    """
    provider = await svc.get(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到提供者")
    
    provider_type = provider.provider_type
    provider_key = provider.provider_key
    config = provider.config_json or {}
    
    # 第一步：获取用户列表
    try:
        if provider_type == "ldap":
            from server.auth.providers.ldap import LDAPProvider
            auth_provider = LDAPProvider(config=config)
            fetch_result = await auth_provider.list_users()
            
        elif provider_type == "oidc":
            from server.auth.providers.oidc import OIDCProvider, OIDCProviderConfig
            oidc_config = OIDCProviderConfig(
                issuer_url=config.get("issuer_url", ""),
                client_id=config.get("client_id", ""),
                client_secret=config.get("client_secret", ""),
                redirect_uri=config.get("redirect_uri", ""),
                scope=config.get("scope", "openid profile email"),
                role_mapping=config.get("role_mapping", {}),
                users_api_url=config.get("users_api_url"),
                users_api_token_url=config.get("users_api_token_url"),
                users_api_client_id=config.get("users_api_client_id"),
                users_api_client_secret=config.get("users_api_client_secret"),
                users_field_mapping=config.get("users_field_mapping"),
            )
            auth_provider = OIDCProvider(config=oidc_config)
            fetch_result = await auth_provider.list_users()
            
        elif provider_type == "api_gateway":
            from server.auth.providers.api_gateway import APIGatewayProvider
            auth_provider = APIGatewayProvider(config=config)
            fetch_result = await auth_provider.list_users()
        
        elif provider_type == "oauth2":
            from server.auth.providers.oauth2 import OAuth2Provider, OAuth2ProviderConfig
            oauth2_config = OAuth2ProviderConfig(
                name=provider_key,
                client_id=config.get("client_id", ""),
                client_secret=config.get("client_secret", ""),
                authorization_endpoint=config.get("authorization_endpoint", ""),
                token_endpoint=config.get("token_endpoint", ""),
                userinfo_endpoint=config.get("userinfo_endpoint", ""),
                redirect_uri=config.get("redirect_uri", ""),
                scope=config.get("scope", ""),
                role_mapping=config.get("role_mapping", {}),
                users_api_url=config.get("users_api_url"),
                users_api_token_url=config.get("users_api_token_url"),
                users_field_mapping=config.get("users_field_mapping"),
                user_attribute_claims=config.get("user_attribute_claims", ["department", "region"]),
            )
            auth_provider = OAuth2Provider(config=oauth2_config)
            fetch_result = await auth_provider.list_users()
            
        elif provider_type == "local":
            return {
                "success": False,
                "message": "本地认证不支持一键同步",
                "created": 0,
                "updated": 0,
                "skipped": 0,
            }
        else:
            return {
                "success": False,
                "message": f"未知的提供者类型: {provider_type}",
                "created": 0,
                "updated": 0,
                "skipped": 0,
            }
        
        if not fetch_result.get("success"):
            return {
                "success": False,
                "message": fetch_result.get("message", "获取用户列表失败"),
                "created": 0,
                "updated": 0,
                "skipped": 0,
            }
        
        users = fetch_result.get("users", [])
        if not users:
            return {
                "success": True,
                "message": "外部系统没有用户需要同步",
                "created": 0,
                "updated": 0,
                "skipped": 0,
            }
        
    except Exception as e:
        logger.error("获取用户列表失败", provider_id=str(provider_id), error=str(e))
        return {
            "success": False,
            "message": f"获取用户列表失败: {str(e)}",
            "created": 0,
            "updated": 0,
            "skipped": 0,
        }
    
    # 第二步：同步用户
    created = 0
    updated = 0
    skipped = 0
    
    for user in users:
        try:
            external_uid = user.get("external_uid", "")
            if not external_uid:
                skipped += 1
                continue
            
            # 检查用户是否已存在
            row = await conn.fetchrow(
                "SELECT user_id FROM users WHERE external_idp = $1 AND external_uid = $2",
                provider_key,
                external_uid,
            )
            
            profile_json = json.dumps(user.get("profile") or {}, ensure_ascii=False)
            
            if not row:
                # 创建新用户
                password_hash = _hash_password(uuid4().hex)
                username = user.get("username") or f"{provider_key}_{external_uid[:8]}"
                
                new_user = await conn.fetchrow(
                    """
                    INSERT INTO users (username, password_hash, email, full_name, role, is_active, 
                                       external_idp, external_uid, profile_json)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING user_id
                    """,
                    username,
                    password_hash,
                    user.get("email"),
                    user.get("full_name"),
                    user.get("role") or "user",
                    user.get("is_active", True),
                    provider_key,
                    external_uid,
                    profile_json,
                )
                
                # 绑定默认数据角色
                await _bind_default_data_roles(conn, new_user["user_id"])
                created += 1
            else:
                # 更新现有用户
                # 注意：不更新 role 字段，保留管理员手动修改的角色
                await conn.execute(
                    """
                    UPDATE users
                    SET email = $1, full_name = $2, is_active = $3, 
                        profile_json = $4, updated_at = NOW()
                    WHERE external_idp = $5 AND external_uid = $6
                    """,
                    user.get("email"),
                    user.get("full_name"),
                    user.get("is_active", True),
                    profile_json,
                    provider_key,
                    external_uid,
                )
                updated += 1
                
        except Exception as e:
            logger.warning("同步用户失败", provider_key=provider_key, error=str(e))
            skipped += 1
    
    logger.info("一键同步完成", provider_key=provider_key, created=created, updated=updated, skipped=skipped)
    
    return {
        "success": True,
        "message": f"同步完成：创建 {created} 个，更新 {updated} 个，跳过 {skipped} 个",
        "created": created,
        "updated": updated,
        "skipped": skipped,
    }
