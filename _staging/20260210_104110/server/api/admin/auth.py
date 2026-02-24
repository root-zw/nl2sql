"""
用户认证API
简化的登录、权限验证
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import timedelta
from typing import Optional
import asyncpg
import jwt
import httpx
import json
from uuid import UUID, uuid4
import bcrypt
import structlog
logger = structlog.get_logger()
from urllib.parse import urlencode

from server.models.database import (
    LoginRequest,
    LoginResponse,
    UserResponse,
    UserCreate,
    UserUpdate,
    UserRole,
    CaptchaResponse,
)
from pydantic import BaseModel, Field, validator
from server.services.auth_service import AuthService
from server.middleware.auth import get_current_active_user, require_data_admin
from server.models.admin import User as AdminUser
from server.utils.timezone_helper import now_utc, now_with_tz, get_datetime_with_delta
from server.dependencies import get_redis_client_sync
from server.utils.captcha import (
    generate_captcha,
    validate_captcha,
    CAPTCHA_TTL_SECONDS,
)
from server.utils.password_validator import validate_password_strength
from server.utils.pinyin import name_to_username, is_chinese_name
from server.auth.providers.oidc import OIDCProvider
from server.auth.providers.oauth2 import OAuth2Provider
from server.auth import get_auth_manager
from server.config import settings
from server.services.auth_service import auth_service

router = APIRouter()
security = HTTPBearer()



async def get_db_pool():
    """获取数据库连接池（使用连接池，避免连接泄漏）"""
    from server.utils.db_pool import get_metadata_pool
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


def hash_password(password: str) -> str:
    """哈希密码"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


class OIDCLoginResponse(BaseModel):
    """OIDC 登录跳转响应"""
    redirect_url: str
    state: str


def _get_oidc_provider(provider_key: Optional[str] = None) -> Optional[OIDCProvider]:
    """
    获取 OIDC 提供者。
    
    Args:
        provider_key: 可选，指定提供者标识。如果不指定，返回第一个启用的 OIDC 提供者。
    """
    auth_manager = get_auth_manager()
    for p in auth_manager.list_providers():
        if isinstance(p, OIDCProvider) and p.enabled:
            if provider_key is None or p.name == provider_key:
                return p
    return None


def _get_oauth2_provider(provider_key: Optional[str] = None) -> Optional[OAuth2Provider]:
    """
    获取 OAuth 2.0 提供者。
    
    Args:
        provider_key: 可选，指定提供者标识。如果不指定，返回第一个启用的 OAuth 2.0 提供者。
    """
    auth_manager = get_auth_manager()
    for p in auth_manager.list_providers():
        if isinstance(p, OAuth2Provider) and p.enabled:
            if provider_key is None or p.name == provider_key:
                return p
    return None

def create_access_token(user_id: str, username: str, role: str) -> str:
    """创建访问令牌"""
    from datetime import timedelta
    from server.utils.timezone_helper import now_utc, get_datetime_with_delta

    # 设置过期时间
    expire = get_datetime_with_delta(base_time=now_utc(), minutes=1440)  # 24小时

    # 构建JWT payload
    payload = {
        "user_id": user_id,
        "username": username,
        "role": role,
        "exp": expire,
        "iat": now_utc(),
    }

    # 生成JWT token
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    """解码令牌"""
    token_data = AuthService.decode_access_token(token)
    if token_data is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的令牌"
        )
    return {
        "user_id": str(token_data.user_id),
        "username": token_data.username,
        "role": token_data.role
    }


async def _bind_default_data_roles(conn, user_id: UUID):
    """为新用户绑定默认数据角色"""
    default_roles = await conn.fetch(
        "SELECT role_id FROM data_roles WHERE is_default = TRUE AND is_active = TRUE"
    )
    for r in default_roles:
        await conn.execute(
            """
            INSERT INTO user_data_roles (user_id, role_id, granted_by, granted_at, is_active)
            VALUES ($1, $2, NULL, NOW(), TRUE)
            ON CONFLICT (user_id, role_id) DO NOTHING
            """,
            user_id,
            r["role_id"],
        )


async def _sync_data_roles_and_attributes(conn, user_id: UUID, claims: dict, provider: OIDCProvider):
    """
    从 OIDC claims 同步数据角色和用户属性
    
    Args:
        conn: 数据库连接
        user_id: 用户ID
        claims: OIDC claims
        provider: OIDC Provider（包含配置信息）
    """
    from server.services.auth_sync_service import sync_auth_context_to_db
    
    # 统一提取并标准化数据角色
    data_roles = provider._extract_data_roles(claims)
    
    # 从 claims 提取用户属性
    user_attributes = {}
    for attr_name in provider.config.user_attribute_claims:
        if attr_name in claims and claims[attr_name] is not None:
            user_attributes[attr_name] = claims[attr_name]
    
    # 同步到数据库
    if data_roles or user_attributes:
        try:
            sync_result = await sync_auth_context_to_db(
                conn=conn,
                user_id=user_id,
                data_roles=data_roles,
                user_attributes=user_attributes,
                attribute_claims=provider.config.user_attribute_claims,
                auto_create_role=provider.config.auto_create_data_role,
                source=provider.name
            )
            logger.info(
                "OIDC 登录同步数据角色和属性完成",
                user_id=str(user_id),
                data_roles=data_roles,
                attributes=list(user_attributes.keys()),
                sync_result=sync_result
            )
        except Exception as e:
            logger.warning("OIDC 同步数据角色/属性失败", user_id=str(user_id), error=str(e))


async def _upsert_oidc_user(claims: dict, provider: OIDCProvider, conn) -> dict:
    """根据 OIDC claims 创建或更新用户"""
    # 使用 provider.name 作为 external_idp，便于识别和与同步时的 source 保持一致
    external_idp = provider.name or "oidc"
    # 旧格式使用 issuer_url（向后兼容）
    legacy_external_idp = provider.config.issuer_url or "oidc"
    external_uid = claims.get("sub")
    if not external_uid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="sub 缺失")

    # 获取原始名称
    raw_name = claims.get("name") or claims.get("displayName")
    raw_username = claims.get("preferred_username")
    
    # 用户名优先级：
    # 1. 如果有 preferred_username 且非中文，直接使用
    # 2. 如果姓名是中文，转换为拼音
    # 3. 使用 email 或兜底值
    if raw_username and not is_chinese_name(raw_username):
        username = raw_username
    elif raw_name and is_chinese_name(raw_name):
        # 中文姓名转拼音
        username = name_to_username(raw_name, fallback=f"oidc_{external_uid[:8]}")
        logger.info("OIDC 中文姓名转拼音", raw_name=raw_name, username=username)
    else:
        username = raw_username or raw_name or claims.get("email") or f"oidc_{external_uid[:8]}"
    
    email = claims.get("email")
    # 全名优先级：displayName（Casdoor） > name > 空（保留中文用于显示）
    full_name = claims.get("displayName") or claims.get("name")
    
    # 统一提取并标准化数据角色
    external_roles = provider._extract_data_roles(claims)
    
    # 映射系统角色
    mapped_role = provider.map_role(external_roles)
    profile_json = json.dumps(claims, ensure_ascii=False)

    # 先用新格式查找用户
    row = await conn.fetchrow(
        """
        SELECT user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at, external_idp
        FROM users
        WHERE external_idp = $1 AND external_uid = $2
        """,
        external_idp,
        external_uid,
    )

    # 如果找不到，尝试用旧格式（issuer_url）查找，并迁移到新格式
    if not row and legacy_external_idp != external_idp:
        row = await conn.fetchrow(
            """
            SELECT user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at, external_idp
            FROM users
            WHERE external_idp = $1 AND external_uid = $2
            """,
            legacy_external_idp,
            external_uid,
        )
        if row:
            # 迁移：更新 external_idp 为新格式
            await conn.execute(
                "UPDATE users SET external_idp = $1 WHERE user_id = $2",
                external_idp,
                row["user_id"],
            )
            logger.info("迁移用户 external_idp 格式", user_id=str(row["user_id"]), old_idp=legacy_external_idp, new_idp=external_idp)

    if not row:
        password_hash = hash_password(uuid4().hex)
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO users (username, password_hash, email, full_name, role, is_active, external_idp, external_uid, profile_json)
                VALUES ($1, $2, $3, $4, $5, TRUE, $6, $7, $8)
                RETURNING user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at
                """,
                username,
                password_hash,
                email,
                full_name,
                mapped_role,
                external_idp,
                external_uid,
                profile_json,
            )
            logger.info("OIDC 创建新用户", username=username, external_uid=external_uid)
        except asyncpg.UniqueViolationError:
            # 用户名冲突时追加随机后缀再试
            username = f"{username}_{uuid4().hex[:6]}"
            row = await conn.fetchrow(
                """
                INSERT INTO users (username, password_hash, email, full_name, role, is_active, external_idp, external_uid, profile_json)
                VALUES ($1, $2, $3, $4, $5, TRUE, $6, $7, $8)
                RETURNING user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at
                """,
                username,
                password_hash,
                email,
                full_name,
                mapped_role,
                external_idp,
                external_uid,
                profile_json,
            )
            logger.info("OIDC 创建新用户（名称冲突后重试）", username=username, external_uid=external_uid)
    else:
        # 注意：不更新 role 字段，保留管理员手动修改的角色
        # 只有创建新用户时才使用外部映射的角色作为默认值
        row = await conn.fetchrow(
            """
            UPDATE users
            SET email = $2,
                full_name = $3,
                profile_json = $4,
                updated_at = NOW()
            WHERE user_id = $1
            RETURNING user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at
            """,
            row["user_id"],
            email,
            full_name,
            profile_json,
        )

    if not row["is_active"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="用户已被禁用")

    return dict(row)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db = Depends(get_db_pool)
) -> dict:
    """获取当前用户"""
    token = credentials.credentials
    payload = decode_token(token)
    
    user_id = payload.get("user_id")
    
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的令牌"
        )
    
    # 从数据库查询用户
    user = await db.fetchrow("""
        SELECT user_id, username, email, full_name, role, is_active
        FROM users
        WHERE user_id = $1
    """, user_id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户不存在"
        )
    
    if not user['is_active']:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="用户已被禁用"
        )
    
    return dict(user)


def require_role(required_role: UserRole):
    """权限检查装饰器
    
    角色权限层级：
    - ADMIN (admin): 系统管理员 - 所有功能
    - DATA_ADMIN (data_admin): 数据管理员 - 数据相关功能
    - USER (user): 普通用户 - 仅查询
    """
    async def role_checker(current_user: dict = Depends(get_current_user)):
        user_role = current_user['role']
        
        # admin可以访问所有
        if user_role == UserRole.ADMIN or user_role == 'admin':
            return current_user
        
        # 检查权限层级
        role_hierarchy = {
            UserRole.ADMIN: 3,
            'admin': 3,
            UserRole.DATA_ADMIN: 2,
            'data_admin': 2,
            UserRole.USER: 1,
            'user': 1,
            # 兼容旧角色
            'viewer': 1
        }
        
        required_level = role_hierarchy.get(required_role, role_hierarchy.get(required_role.value if hasattr(required_role, 'value') else required_role, 0))
        user_level = role_hierarchy.get(user_role, 0)
        
        if user_level < required_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"需要更高权限"
            )
        
        return current_user
    
    return role_checker


# ============================================================================
# 认证接口
# ============================================================================


@router.get("/oidc/providers")
async def list_oidc_providers():
    """获取可用的 OIDC 提供者列表"""
    auth_manager = get_auth_manager()
    providers = []
    for p in auth_manager.list_providers():
        if isinstance(p, OIDCProvider) and p.enabled:
            providers.append({
                "provider_key": p.name,
                "issuer_url": p.config.issuer_url,
                "enabled": p.enabled,
            })
    return {"providers": providers}


@router.get("/oidc/login", response_model=OIDCLoginResponse)
async def oidc_login(provider_key: Optional[str] = None):
    """
    获取 OIDC 登录跳转地址。
    
    Args:
        provider_key: 可选，指定 OIDC 提供者标识。如果不指定，使用第一个启用的 OIDC 提供者。
    """
    provider = _get_oidc_provider(provider_key)
    if not provider:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="OIDC 未启用或指定的提供者不存在")
    
    discovery = await provider.get_discovery()
    auth_endpoint = discovery.get("authorization_endpoint")
    if not auth_endpoint:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="未获取到授权端点")
    
    # 在 state 中嵌入 provider_key，以便回调时识别
    state_data = f"{provider.name}:{uuid4()}"
    params = {
        "response_type": "code",
        "client_id": provider.config.client_id,
        "redirect_uri": provider.config.redirect_uri,
        "scope": provider.config.scope,
        "state": state_data,
    }
    redirect_url = f"{auth_endpoint}?{urlencode(params)}"
    return OIDCLoginResponse(redirect_url=redirect_url, state=state_data)


@router.get("/oidc/callback")
async def oidc_callback(
    request: Request,
    code: str,
    state: Optional[str] = None,
    redirect_to: Optional[str] = None,
    db = Depends(get_db_pool)
):
    """
    OIDC 回调，交换 code 并颁发本地 Token。
    
    state 格式: provider_key:uuid，用于识别是哪个 OIDC 提供者的回调。
    
    响应方式：
    - 浏览器直接访问：重定向到前端页面并携带 token
    - AJAX 调用：返回 JSON 数据
    """
    from fastapi.responses import RedirectResponse
    import urllib.parse
    
    # 从 state 中解析 provider_key
    provider_key = None
    if state and ":" in state:
        provider_key = state.split(":")[0]
    
    provider = _get_oidc_provider(provider_key)
    if not provider:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="OIDC 未启用或指定的提供者不存在")

    discovery = await provider.get_discovery()
    token_endpoint = discovery.get("token_endpoint")
    if not token_endpoint:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="未获取到 token 端点")

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": provider.config.redirect_uri,
    }

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(token_endpoint, data=data, auth=(provider.config.client_id, provider.config.client_secret))
        if resp.status_code != 200:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"OIDC 交换 code 失败: {resp.text}")
        token_payload = resp.json()

    id_token = token_payload.get("id_token")
    if not id_token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="id_token 缺失")

    claims = await provider.validate_id_token(id_token)

    user_row = await _upsert_oidc_user(claims, provider, db)
    
    # 同步数据角色和用户属性（核心功能）
    await _sync_data_roles_and_attributes(db, user_row["user_id"], claims, provider)
    
    # 绑定默认数据角色（补充，不会覆盖已同步的角色）
    await _bind_default_data_roles(db, user_row["user_id"])
    
    # 更新最后登录时间
    await db.execute("UPDATE users SET last_login_at = NOW() WHERE user_id = $1", user_row["user_id"])

    user_model = AdminUser(
        user_id=user_row["user_id"],
        username=user_row["username"],
        email=user_row["email"],
        full_name=user_row["full_name"],
        role=user_row["role"],
        is_active=user_row["is_active"],
        created_at=user_row["created_at"],
        updated_at=user_row["updated_at"],
        last_login=user_row.get("last_login_at"),
    )
    
    # 生成 Access Token 和 Refresh Token
    token_pair = AuthService.create_token_pair(user_model)

    # 构建用户信息 JSON（用于前端存储）
    user_info = {
        "user_id": str(user_row["user_id"]),
        "username": user_row["username"],
        "email": user_row["email"],
        "full_name": user_row["full_name"],
        "role": user_row["role"],
        "is_active": user_row["is_active"],
    }
    
    # 判断是浏览器直接访问还是 AJAX 调用
    accept_header = request.headers.get("Accept", "")
    is_browser_request = "text/html" in accept_header and "application/json" not in accept_header
    
    if is_browser_request:
        # 浏览器直接访问：重定向到前端页面
        # 优先使用认证提供者配置中的 frontend_redirect_url，否则使用环境变量 FRONTEND_URL
        provider_frontend_url = getattr(provider.config, 'frontend_redirect_url', None)
        frontend_base = (provider_frontend_url or settings.frontend_url or "").rstrip("/")
        
        # 根据用户角色决定重定向目标路径
        # 注意：使用 /login?admin=true 而非 /admin/login，因为后者是前端 redirect 规则
        # 直接跳转到 /admin/login 会导致 URL hash 在 redirect 过程中丢失
        user_role = user_row["role"]
        if user_role in ("admin", "data_admin"):
            # 管理员重定向到登录页（带 admin 标记）
            frontend_path = redirect_to or "/login?admin=true"
        else:
            # 普通用户重定向到问答页面
            frontend_path = redirect_to or "/"
        
        # 构建完整的重定向 URL
        frontend_url = f"{frontend_base}{frontend_path}" if frontend_base else frontend_path
        
        # 使用 fragment (#) 传递 token，避免服务器日志记录敏感信息
        user_json = urllib.parse.quote(json.dumps(user_info, ensure_ascii=False))
        redirect_url = f"{frontend_url}#token={token_pair['access_token']}&refresh_token={token_pair['refresh_token']}&expires_in={token_pair['expires_in']}&user={user_json}"
        
        return RedirectResponse(url=redirect_url, status_code=302)
    else:
        # AJAX 调用：返回 JSON
        return LoginResponse(
            access_token=token_pair["access_token"],
            refresh_token=token_pair["refresh_token"],
            token_type="bearer",
            expires_in=token_pair["expires_in"],
            refresh_expires_in=token_pair["refresh_expires_in"],
            user=UserResponse(
                user_id=user_row["user_id"],
                username=user_row["username"],
                email=user_row["email"],
                full_name=user_row["full_name"],
                role=user_row["role"],
                is_active=user_row["is_active"],
                last_login_at=user_row.get("last_login_at"),
                created_at=user_row["created_at"],
            ),
        )


# ============================================================================
# OAuth 2.0 认证接口（支持钉钉、企业微信、飞书等）
# ============================================================================


class OAuth2LoginResponse(BaseModel):
    """OAuth 2.0 登录跳转响应"""
    redirect_url: str
    state: str
    provider_key: str


@router.get("/oauth2/providers")
async def list_oauth2_providers():
    """获取可用的 OAuth 2.0 提供者列表"""
    auth_manager = get_auth_manager()
    providers = []
    for p in auth_manager.list_providers():
        if isinstance(p, OAuth2Provider) and p.enabled:
            providers.append({
                "provider_key": p.name,
                "authorization_endpoint": p.config.authorization_endpoint,
                "enabled": p.enabled,
            })
    return {"providers": providers}


@router.get("/oauth2/login", response_model=OAuth2LoginResponse)
async def oauth2_login(provider_key: Optional[str] = None):
    """
    获取 OAuth 2.0 登录跳转地址。
    
    Args:
        provider_key: 可选，指定 OAuth 2.0 提供者标识。如果不指定，使用第一个启用的提供者。
    """
    provider = _get_oauth2_provider(provider_key)
    if not provider:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="OAuth 2.0 未启用或指定的提供者不存在")
    
    # 在 state 中嵌入 provider_key，以便回调时识别
    state_data = f"{provider.name}:{uuid4()}"
    
    redirect_url = provider.get_authorization_url(state=state_data)
    
    return OAuth2LoginResponse(
        redirect_url=redirect_url,
        state=state_data,
        provider_key=provider.name
    )


async def _upsert_oauth2_user(userinfo: dict, provider: OAuth2Provider, conn) -> dict:
    """根据 OAuth 2.0 userinfo 创建或更新用户"""
    external_idp = provider.name
    external_uid = str(userinfo.get("user_id"))
    if not external_uid:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="user_id 缺失")

    # 获取原始名称
    raw_name = userinfo.get("name") or userinfo.get("username")
    raw_username = userinfo.get("username")
    
    # 用户名优先级：
    # 1. 如果有 username 且非中文，直接使用
    # 2. 如果姓名是中文，转换为拼音
    # 3. 使用 email 或兜底值
    if raw_username and not is_chinese_name(raw_username):
        username = raw_username
    elif raw_name and is_chinese_name(raw_name):
        # 中文姓名转拼音
        username = name_to_username(raw_name, fallback=f"oauth2_{external_uid[:8]}")
        logger.info("中文姓名转拼音", raw_name=raw_name, username=username)
    else:
        username = raw_username or raw_name or userinfo.get("email") or f"oauth2_{external_uid[:8]}"
    
    email = userinfo.get("email")
    full_name = userinfo.get("name")  # full_name 保留中文姓名用于显示
    
    # 提取角色
    external_roles = provider._extract_data_roles(userinfo)
    mapped_role = provider.map_role(external_roles)
    
    profile_json = json.dumps(userinfo.get("_raw", userinfo), ensure_ascii=False)

    # 查找用户
    row = await conn.fetchrow(
        """
        SELECT user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at, external_idp
        FROM users
        WHERE external_idp = $1 AND external_uid = $2
        """,
        external_idp,
        external_uid,
    )

    if not row:
        password_hash = hash_password(uuid4().hex)
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO users (username, password_hash, email, full_name, role, is_active, external_idp, external_uid, profile_json)
                VALUES ($1, $2, $3, $4, $5, TRUE, $6, $7, $8)
                RETURNING user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at
                """,
                username,
                password_hash,
                email,
                full_name,
                mapped_role,
                external_idp,
                external_uid,
                profile_json,
            )
            logger.info("OAuth2 创建新用户", username=username, external_uid=external_uid, provider=external_idp)
        except asyncpg.UniqueViolationError:
            username = f"{username}_{uuid4().hex[:6]}"
            row = await conn.fetchrow(
                """
                INSERT INTO users (username, password_hash, email, full_name, role, is_active, external_idp, external_uid, profile_json)
                VALUES ($1, $2, $3, $4, $5, TRUE, $6, $7, $8)
                RETURNING user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at
                """,
                username,
                password_hash,
                email,
                full_name,
                mapped_role,
                external_idp,
                external_uid,
                profile_json,
            )
            logger.info("OAuth2 创建新用户（名称冲突后重试）", username=username, external_uid=external_uid)
    else:
        # 注意：不更新 role 字段，保留管理员手动修改的角色
        # 只有创建新用户时才使用外部映射的角色作为默认值
        row = await conn.fetchrow(
            """
            UPDATE users
            SET email = $2,
                full_name = $3,
                profile_json = $4,
                updated_at = NOW()
            WHERE user_id = $1
            RETURNING user_id, username, email, full_name, role, is_active, created_at, updated_at, last_login_at
            """,
            row["user_id"],
            email,
            full_name,
            profile_json,
        )

    if not row["is_active"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="用户已被禁用")

    return dict(row)


async def _sync_oauth2_data_roles_and_attributes(conn, user_id: UUID, userinfo: dict, provider: OAuth2Provider):
    """从 OAuth 2.0 userinfo 同步数据角色和用户属性"""
    from server.services.auth_sync_service import sync_auth_context_to_db
    
    data_roles = provider._extract_data_roles(userinfo)
    user_attributes = provider._extract_user_attributes(userinfo)
    
    if data_roles or user_attributes:
        try:
            sync_result = await sync_auth_context_to_db(
                conn=conn,
                user_id=user_id,
                data_roles=data_roles,
                user_attributes=user_attributes,
                attribute_claims=provider.config.user_attribute_claims,
                auto_create_role=provider.config.auto_create_data_role,
                source=provider.name
            )
            logger.info(
                "OAuth2 登录同步数据角色和属性完成",
                user_id=str(user_id),
                data_roles=data_roles,
                attributes=list(user_attributes.keys()),
                sync_result=sync_result
            )
        except Exception as e:
            logger.warning("OAuth2 同步数据角色/属性失败", user_id=str(user_id), error=str(e))


@router.get("/oauth2/callback")
async def oauth2_callback(
    request: Request,
    code: str,
    state: Optional[str] = None,
    redirect_to: Optional[str] = None,
    db = Depends(get_db_pool)
):
    """
    OAuth 2.0 回调，交换 code 并颁发本地 Token。
    
    state 格式: provider_key:uuid，用于识别是哪个 OAuth 2.0 提供者的回调。
    """
    from fastapi.responses import RedirectResponse
    import urllib.parse
    
    # 从 state 中解析 provider_key
    provider_key = None
    if state and ":" in state:
        provider_key = state.split(":")[0]
    
    provider = _get_oauth2_provider(provider_key)
    if not provider:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="OAuth 2.0 未启用或指定的提供者不存在")

    # 用 code 换取 access_token
    try:
        token_response = await provider.exchange_code_for_token(code)
        access_token = token_response.get("access_token")
        if not access_token:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="未获取到 access_token")
    except Exception as e:
        logger.error("OAuth2 token exchange failed", error=str(e))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Token 交换失败: {str(e)}")

    # 获取用户信息
    try:
        userinfo = await provider.get_userinfo(access_token, token_response)
    except Exception as e:
        logger.error("OAuth2 get userinfo failed", error=str(e))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"获取用户信息失败: {str(e)}")

    # 创建或更新用户
    user_row = await _upsert_oauth2_user(userinfo, provider, db)
    
    # 同步数据角色和用户属性
    await _sync_oauth2_data_roles_and_attributes(db, user_row["user_id"], userinfo, provider)
    
    # 绑定默认数据角色
    await _bind_default_data_roles(db, user_row["user_id"])
    
    # 更新最后登录时间
    await db.execute("UPDATE users SET last_login_at = NOW() WHERE user_id = $1", user_row["user_id"])

    # 创建本地 JWT Token
    user_model = AdminUser(
        user_id=user_row["user_id"],
        username=user_row["username"],
        email=user_row["email"],
        full_name=user_row["full_name"],
        role=user_row["role"],
        is_active=user_row["is_active"],
        created_at=user_row["created_at"],
        updated_at=user_row["updated_at"],
        last_login=user_row.get("last_login_at"),
    )
    
    # 生成 Access Token 和 Refresh Token
    token_pair = AuthService.create_token_pair(user_model)

    # 构建用户信息 JSON
    user_info = {
        "user_id": str(user_row["user_id"]),
        "username": user_row["username"],
        "email": user_row["email"],
        "full_name": user_row["full_name"],
        "role": user_row["role"],
        "is_active": user_row["is_active"],
    }
    
    # 判断是浏览器直接访问还是 AJAX 调用
    accept_header = request.headers.get("Accept", "")
    is_browser_request = "text/html" in accept_header and "application/json" not in accept_header
    
    if is_browser_request:
        # 浏览器直接访问：重定向到前端页面
        frontend_base = (provider.config.frontend_redirect_url or settings.frontend_url or "").rstrip("/")
        
        # 根据用户角色决定重定向目标路径
        # 注意：使用 /login?admin=true 而非 /admin/login，因为后者是前端 redirect 规则
        # 直接跳转到 /admin/login 会导致 URL hash 在 redirect 过程中丢失
        user_role = user_row["role"]
        if user_role in ("admin", "data_admin"):
            # 管理员重定向到登录页（带 admin 标记）
            frontend_path = redirect_to or "/login?admin=true"
        else:
            # 普通用户重定向到问答页面
            frontend_path = redirect_to or "/"
        
        frontend_url = f"{frontend_base}{frontend_path}" if frontend_base else frontend_path
        
        user_json = urllib.parse.quote(json.dumps(user_info, ensure_ascii=False))
        redirect_url = f"{frontend_url}#token={token_pair['access_token']}&refresh_token={token_pair['refresh_token']}&expires_in={token_pair['expires_in']}&user={user_json}"
        
        return RedirectResponse(url=redirect_url, status_code=302)
    else:
        # AJAX 调用：返回 JSON
        return LoginResponse(
            access_token=token_pair["access_token"],
            refresh_token=token_pair["refresh_token"],
            token_type="bearer",
            expires_in=token_pair["expires_in"],
            refresh_expires_in=token_pair["refresh_expires_in"],
            user=UserResponse(
                user_id=user_row["user_id"],
                username=user_row["username"],
                email=user_row["email"],
                full_name=user_row["full_name"],
                role=user_row["role"],
                is_active=user_row["is_active"],
                last_login_at=user_row.get("last_login_at"),
                created_at=user_row["created_at"],
            ),
        )


@router.post("/login", response_model=LoginResponse)
async def login(
    request: LoginRequest,
    db = Depends(get_db_pool)
):
    """
    用户登录
    """
    try:
        redis_client = get_redis_client_sync()

        # 自动化/开发支持：允许通过环境变量关闭验证码校验（默认开启，生产建议保持开启）
        if settings.auth_captcha_enabled:
            if not validate_captcha(request.captcha_id, request.captcha_code, redis_client):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="验证码错误或已过期"
                )

        # 查询用户
        user = await db.fetchrow("""
            SELECT user_id, username, password_hash, email, full_name, role, is_active, external_idp
            FROM users
            WHERE username = $1
        """, request.username)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户名或密码错误"
            )
        
        # 检查是否为外部认证用户，禁止本地登录
        if user.get('external_idp'):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"该用户来自外部认证（{user['external_idp']}），请使用单点登录"
            )
        
        if not user['is_active']:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="用户已被禁用"
            )
        
        # 验证密码
        if not verify_password(request.password, user['password_hash']):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户名或密码错误"
            )
        
        # 更新最后登录时间
        await db.execute("""
            UPDATE users
            SET last_login_at = NOW()
            WHERE user_id = $1
        """, user['user_id'])
        
        # 直接使用数据库角色值：admin, data_admin, user
        mapped_role = user['role']

        # 绑定默认数据角色（如有）
        await _bind_default_data_roles(db, user['user_id'])

        # 生成令牌（统一走 AuthService）
        user_model = AdminUser(
            user_id=user['user_id'],
            username=user['username'],
            email=user['email'],
            full_name=user['full_name'],
            role=mapped_role,
            is_active=user['is_active'],
            created_at=user['created_at'] if 'created_at' in user else now_with_tz(),
            updated_at=now_with_tz(),
            last_login=user['last_login_at'] if 'last_login_at' in user else None,
        )
        
        # 生成 Access Token 和 Refresh Token
        token_pair = AuthService.create_token_pair(user_model)
        
        logger.info(f"用户登录成功: {user['username']}")
        
        return LoginResponse(
            access_token=token_pair["access_token"],
            refresh_token=token_pair["refresh_token"],
            token_type="bearer",
            expires_in=token_pair["expires_in"],
            refresh_expires_in=token_pair["refresh_expires_in"],
            user=UserResponse(
                user_id=user['user_id'],
                username=user['username'],
                email=user['email'],
                full_name=user['full_name'],
                role=user['role'],
                is_active=user['is_active'],
                last_login_at=user['last_login_at'] if 'last_login_at' in user else None,
                created_at=user['created_at'] if 'created_at' in user else now_with_tz()
            )
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("登录失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"登录失败: {str(e)}"
        )


@router.get("/captcha", response_model=CaptchaResponse)
async def get_captcha():
    """获取登录验证码"""
    redis_client = get_redis_client_sync()
    captcha_id, image_base64 = generate_captcha(redis_client)

    return CaptchaResponse(
        captcha_id=captcha_id,
        image_base64=f"data:image/png;base64,{image_base64}",
        expires_in=CAPTCHA_TTL_SECONDS
    )


class RefreshTokenRequest(BaseModel):
    """刷新 Token 请求"""
    refresh_token: str = Field(..., description="Refresh Token")


class RefreshTokenResponse(BaseModel):
    """刷新 Token 响应"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # Access Token 过期时间（秒）
    refresh_expires_in: int  # Refresh Token 过期时间（秒）


@router.post("/refresh", response_model=RefreshTokenResponse)
async def refresh_token(
    request: RefreshTokenRequest,
    db = Depends(get_db_pool)
):
    """
    刷新访问令牌
    
    使用 Refresh Token 获取新的 Access Token 和 Refresh Token。
    每次刷新都会生成新的 Refresh Token（Refresh Token Rotation），提高安全性。
    """
    try:
        # 解码 Refresh Token
        refresh_data = AuthService.decode_refresh_token(request.refresh_token)
        if not refresh_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"error_code": 10202, "message": "Refresh Token 无效或已过期"}
            )
        
        # 查询用户信息
        user = await db.fetchrow("""
            SELECT user_id, username, email, full_name, role, is_active,
                   created_at, updated_at, last_login_at
            FROM users
            WHERE user_id = $1
        """, refresh_data.user_id)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"error_code": 10202, "message": "用户不存在"}
            )
        
        if not user['is_active']:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"error_code": 10202, "message": "用户已被禁用"}
            )
        
        # 构建用户模型
        user_model = AdminUser(
            user_id=user['user_id'],
            username=user['username'],
            email=user['email'],
            full_name=user['full_name'],
            role=user['role'],
            is_active=user['is_active'],
            created_at=user['created_at'],
            updated_at=user['updated_at'] or now_with_tz(),
            last_login=user['last_login_at'],
        )
        
        # 生成新的 Token 对（Refresh Token Rotation）
        token_pair = AuthService.create_token_pair(user_model)
        
        logger.debug(f"Token 刷新成功: {user['username']}")
        
        return RefreshTokenResponse(
            access_token=token_pair["access_token"],
            refresh_token=token_pair["refresh_token"],
            token_type="bearer",
            expires_in=token_pair["expires_in"],
            refresh_expires_in=token_pair["refresh_expires_in"],
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Token 刷新失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Token 刷新失败: {str(e)}"
        )


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: dict = Depends(get_current_user)
):
    """
    获取当前用户信息
    """
    return UserResponse(
        user_id=current_user['user_id'],
        username=current_user['username'],
        email=current_user['email'],
        full_name=current_user['full_name'],
        role=current_user['role'],
        is_active=current_user['is_active'],
        last_login_at=None,
        created_at=now_with_tz()
    )


@router.post("/logout")
async def logout(
    current_user: dict = Depends(get_current_user)
):
    """
    用户登出（客户端需删除token）
    """
    logger.info(f"用户登出: {current_user['username']}")
    
    return {
        "success": True,
        "message": "登出成功"
    }


class ChangePasswordRequest(BaseModel):
    """修改密码请求"""
    old_password: str = Field(..., min_length=1, description="当前密码")
    new_password: str = Field(..., min_length=8, description="新密码")
    
    @validator('new_password')
    def validate_new_password(cls, v):
        is_valid, error_msg = validate_password_strength(v)
        if not is_valid:
            raise ValueError(error_msg)
        return v


@router.post("/change-password")
async def change_password(
    request: ChangePasswordRequest,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db_pool)
):
    """
    修改当前用户密码
    """
    try:
        user_id = current_user['user_id']
        
        # 查询用户当前密码和外部认证信息
        user = await db.fetchrow("""
            SELECT password_hash, external_idp
            FROM users
            WHERE user_id = $1
        """, user_id)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="用户不存在"
            )
        
        # 检查是否为外部认证用户，禁止修改密码
        if user.get('external_idp'):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"您的账号来自外部认证（{user['external_idp']}），请通过身份提供商修改密码"
            )
        
        # 验证当前密码
        if not verify_password(request.old_password, user['password_hash']):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="当前密码错误"
            )
        
        # 检查新密码是否与旧密码相同
        if verify_password(request.new_password, user['password_hash']):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="新密码不能与当前密码相同"
            )
        
        # 更新密码
        new_password_hash = hash_password(request.new_password)
        await db.execute("""
            UPDATE users
            SET password_hash = $1
            WHERE user_id = $2
        """, new_password_hash, user_id)
        
        logger.info(f"用户修改密码成功: {current_user['username']}")
        
        return {
            "success": True,
            "message": "密码修改成功"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("修改密码失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"修改密码失败: {str(e)}"
        )


# ============================================================================
# 用户管理（仅管理员）
# ============================================================================

@router.post("/users", response_model=UserResponse)
async def create_user(
    user: UserCreate,
    current_user: dict = Depends(require_role(UserRole.ADMIN)),
    db = Depends(get_db_pool)
):
    """
    创建用户（仅管理员）
    """
    try:
        # 哈希密码
        password_hash = hash_password(user.password)
        
        # 插入用户
        row = await db.fetchrow("""
            INSERT INTO users (username, password_hash, email, full_name, role, is_active)
            VALUES ($1, $2, $3, $4, $5, TRUE)
            RETURNING user_id, username, email, full_name, role, is_active, created_at
        """, user.username, password_hash, user.email, user.full_name, user.role)
        
        logger.info(f"创建用户成功: {user.username}")
        
        return UserResponse(
            user_id=row['user_id'],
            username=row['username'],
            email=row['email'],
            full_name=row['full_name'],
            role=row['role'],
            is_active=row['is_active'],
            last_login_at=None,
            created_at=row['created_at']
        )
    
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"用户名 '{user.username}' 已存在"
        )
    except Exception as e:
        logger.exception("创建用户失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建用户失败: {str(e)}"
        )


@router.get("/users")
async def list_users(
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取用户列表（系统管理员和数据管理员可访问）
    
    数据管理员需要此接口来分配数据权限
    """
    try:
        rows = await db.fetch("""
            SELECT u.user_id, u.username, u.email, u.full_name, u.role, u.is_active, 
                   u.last_login_at, u.created_at, u.updated_at, u.external_idp,
                   u.org_id, o.org_name
            FROM users u
            LEFT JOIN organizations o ON u.org_id = o.org_id
            ORDER BY
                CASE u.role
                    WHEN 'admin' THEN 1     -- 系统管理员
                    WHEN 'data_admin' THEN 2 -- 数据管理员
                    WHEN 'user' THEN 3      -- 普通用户
                    ELSE 4                  -- 兼容未来角色
                END,
                u.created_at DESC
        """)
        
        return [
            UserResponse(
                user_id=row['user_id'],
                username=row['username'],
                email=row['email'],
                full_name=row['full_name'],
                role=row['role'],
                is_active=row['is_active'],
                last_login_at=row['last_login_at'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                external_idp=row['external_idp'],
                org_id=row['org_id'],
                org_name=row['org_name']
            )
            for row in rows
        ]
    
    except Exception as e:
        logger.exception("获取用户列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取用户列表失败: {str(e)}"
        )


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: UUID,
    user_update: UserUpdate,
    current_user: dict = Depends(require_role(UserRole.ADMIN)),
    db = Depends(get_db_pool)
):
    """
    更新用户信息（仅管理员）
    """
    try:
        # 检查用户是否存在
        existing = await db.fetchrow("""
            SELECT user_id, username, email, full_name, role, is_active, 
                   last_login_at, created_at
            FROM users
            WHERE user_id = $1
        """, user_id)
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"用户 {user_id} 不存在"
            )
        
        # 构建更新语句
        updates = []
        params = []
        param_index = 1
        
        update_fields = user_update.dict(exclude_unset=True)
        
        # 处理密码更新
        if 'password' in update_fields:
            update_fields['password_hash'] = hash_password(update_fields.pop('password'))
        
        for field, value in update_fields.items():
            updates.append(f"{field} = ${param_index}")
            params.append(value)
            param_index += 1
        
        if not updates:
            # 没有任何更新，直接返回当前数据
            return UserResponse(
                user_id=existing['user_id'],
                username=existing['username'],
                email=existing['email'],
                full_name=existing['full_name'],
                role=existing['role'],
                is_active=existing['is_active'],
                last_login_at=existing['last_login_at'],
                created_at=existing['created_at']
            )
        
        params.append(user_id)
        
        # 执行更新
        row = await db.fetchrow(f"""
            UPDATE users
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE user_id = ${param_index}
            RETURNING user_id, username, email, full_name, role, is_active, 
                      last_login_at, created_at
        """, *params)
        
        logger.info(f"更新用户成功: {row['username']}")
        
        return UserResponse(
            user_id=row['user_id'],
            username=row['username'],
            email=row['email'],
            full_name=row['full_name'],
            role=row['role'],
            is_active=row['is_active'],
            last_login_at=row['last_login_at'],
            created_at=row['created_at']
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新用户失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新用户失败: {str(e)}"
        )


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: UUID,
    current_user: dict = Depends(require_role(UserRole.ADMIN)),
    db = Depends(get_db_pool)
):
    """
    删除用户（仅管理员）
    """
    try:
        # 检查用户是否存在
        existing = await db.fetchrow("""
            SELECT user_id, username FROM users WHERE user_id = $1
        """, user_id)
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"用户 {user_id} 不存在"
            )
        
        # 不能删除自己
        if str(existing['user_id']) == str(current_user['user_id']):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="不能删除自己的账户"
            )
        
        # 删除用户
        await db.execute("""
            DELETE FROM users WHERE user_id = $1
        """, user_id)
        
        logger.info(f"删除用户成功: {existing['username']}")
        
        return {
            "success": True,
            "message": "用户已删除"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除用户失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除用户失败: {str(e)}"
        )


@router.post("/users/{user_id}/reset-password")
async def reset_user_password(
    user_id: UUID,
    request: dict,
    current_user: dict = Depends(require_role(UserRole.ADMIN)),
    db = Depends(get_db_pool)
):
    """
    重置用户密码（仅管理员）
    """
    try:
        new_password = request.get('new_password')
        if not new_password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="密码不能为空"
            )
        
        # 验证密码强度
        is_valid, error_msg = validate_password_strength(new_password)
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_msg
            )
        
        # 检查用户是否存在
        existing = await db.fetchrow("""
            SELECT user_id, username, external_idp FROM users WHERE user_id = $1
        """, user_id)
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"用户 {user_id} 不存在"
            )
        
        # 检查是否为外部认证用户，禁止重置密码
        if existing.get('external_idp'):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"该用户来自外部认证（{existing['external_idp']}），不允许重置密码，请通过身份提供商管理"
            )
        
        # 更新密码
        password_hash = hash_password(new_password)
        await db.execute("""
            UPDATE users
            SET password_hash = $1, updated_at = NOW()
            WHERE user_id = $2
        """, password_hash, user_id)
        
        logger.info(f"重置用户密码成功: {existing['username']}")
        
        return {
            "success": True,
            "message": "密码已重置"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("重置密码失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"重置密码失败: {str(e)}"
        )

