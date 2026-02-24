"""认证中间件"""

from typing import Optional
from fastapi import Depends, HTTPException, Header, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import asyncpg
import structlog

from server.auth import get_auth_manager
from server.auth.base import AuthContext
from server.models.admin import User
from server.config import settings
from server.services.auth_sync_service import sync_auth_context_to_db
import bcrypt
import json
from uuid import uuid4, UUID

logger = structlog.get_logger()

# HTTP Bearer认证方案
security = HTTPBearer(auto_error=False)


async def _hash_password(raw: str) -> str:
    return bcrypt.hashpw(raw.encode(), bcrypt.gensalt()).decode()


async def _get_or_create_local_user(conn, ctx: AuthContext, sync_roles_and_attrs: bool = True):
    """
    确保外部认证用户在本地 users 表中有映射。
    
    - 优先按 user_id 直接匹配
    - 不存在时按 external_idp/external_uid 回退匹配
    - 仍不存在则创建，占位随机密码
    - 自动同步数据角色和用户属性（可选）
    
    Args:
        conn: 数据库连接
        ctx: 认证上下文
        sync_roles_and_attrs: 是否同步数据角色和用户属性
    
    Returns:
        用户记录行
    """
    is_new_user = False
    
    # 1) 直接按 user_id 查找（仅当 user_id 是有效 UUID 时）
    row = None
    try:
        user_uuid = UUID(ctx.user_id) if isinstance(ctx.user_id, str) else ctx.user_id
        row = await conn.fetchrow(
            """
            SELECT user_id, username, email, full_name, role,
                   is_active, created_at, updated_at, last_login_at
            FROM users
            WHERE user_id = $1
            """,
            user_uuid,
        )
    except (ValueError, AttributeError):
        # user_id 不是有效 UUID，跳过
        pass

    # 2) 回退 external_idp/external_uid
    if not row:
        row = await conn.fetchrow(
            """
            SELECT user_id, username, email, full_name, role,
                   is_active, created_at, updated_at, last_login_at
            FROM users
            WHERE external_idp = $1 AND external_uid = $2
            """,
            ctx.source,
            str(ctx.user_id),
        )

    # 3) 创建新用户
    if not row:
        is_new_user = True
        username = ctx.username or f"{ctx.source}_{str(ctx.user_id)[:8]}"
        password_hash = await _hash_password(uuid4().hex)
        role = ctx.role or "user"
        email = None
        full_name = None
        if ctx.attributes:
            email = ctx.attributes.get("email")
            full_name = ctx.attributes.get("name") or ctx.attributes.get("full_name")
        profile_json = json.dumps(ctx.extra or {}, ensure_ascii=False) if ctx.extra else "{}"
        
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO users (username, password_hash, email, full_name, role, is_active,
                                   external_idp, external_uid, profile_json)
                VALUES ($1, $2, $3, $4, $5, TRUE, $6, $7, $8)
                RETURNING user_id, username, email, full_name, role,
                          is_active, created_at, updated_at, last_login_at
                """,
                username,
                password_hash,
                email,
                full_name,
                role,
                ctx.source,
                str(ctx.user_id),
                profile_json,
            )
            logger.info("创建外部用户", username=username, source=ctx.source)
        except asyncpg.UniqueViolationError:
            # 用户名冲突，追加随机后缀
            username = f"{username}_{uuid4().hex[:6]}"
            row = await conn.fetchrow(
                """
                INSERT INTO users (username, password_hash, email, full_name, role, is_active,
                                   external_idp, external_uid, profile_json)
                VALUES ($1, $2, $3, $4, $5, TRUE, $6, $7, $8)
                RETURNING user_id, username, email, full_name, role,
                          is_active, created_at, updated_at, last_login_at
                """,
                username,
                password_hash,
                email,
                full_name,
                role,
                ctx.source,
                str(ctx.user_id),
                profile_json,
            )
            logger.info("创建外部用户（名称冲突后重试）", username=username, source=ctx.source)
    else:
        # 更新已有用户的基本信息
        email = ctx.attributes.get("email") if ctx.attributes else None
        full_name = (ctx.attributes.get("name") or ctx.attributes.get("full_name")) if ctx.attributes else None
        profile_json = json.dumps(ctx.extra or {}, ensure_ascii=False) if ctx.extra else None
        
        if email or full_name or profile_json:
            row = await conn.fetchrow(
                """
                UPDATE users SET
                    email = COALESCE($2, email),
                    full_name = COALESCE($3, full_name),
                    profile_json = COALESCE($4, profile_json),
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = $1
                RETURNING user_id, username, email, full_name, role,
                          is_active, created_at, updated_at, last_login_at
                """,
                row['user_id'],
                email,
                full_name,
                profile_json,
            )
    
    # 4) 同步数据角色和用户属性
    if row and sync_roles_and_attrs:
        user_id = row['user_id']
        
        # 获取同步配置
        auto_create_role = settings.oidc_auto_create_data_role
        attribute_claims = settings.oidc_user_attribute_claims_list
        
        # 从 AuthContext 获取数据角色和用户属性
        data_roles = getattr(ctx, 'data_roles', []) or []
        user_attributes = getattr(ctx, 'user_attributes', {}) or {}
        
        if data_roles or user_attributes:
            try:
                sync_result = await sync_auth_context_to_db(
                    conn=conn,
                    user_id=user_id,
                    data_roles=data_roles,
                    user_attributes=user_attributes,
                    attribute_claims=attribute_claims,
                    auto_create_role=auto_create_role,
                    source=ctx.source
                )
                
                if is_new_user or data_roles:
                    logger.debug(
                        "同步认证数据完成",
                        user_id=str(user_id),
                        data_roles=data_roles,
                        sync_result=sync_result
                    )
            except Exception as e:
                # 同步失败不影响登录，仅记录警告
                logger.warning("同步数据角色/属性失败", user_id=str(user_id), error=str(e))
    
    return row


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    request: Request = None,
) -> User:
    """获取当前登录用户（兼容外部ID回落并自动落库）"""
    # 统一通过认证管理器（支持多 provider）
    provided_token = credentials.credentials if credentials else None
    auth_manager = get_auth_manager()
    auth_result = await auth_manager.authenticate_with_result(request, provided_token)

    if not auth_result.success:
        # 返回错误代码和友好的错误消息
        from server.auth.base import get_error_message
        error_message = get_error_message(auth_result.error_code) if auth_result.error_code else "认证失败"
        detail = {
            "error_code": auth_result.error_code,
            "provider": auth_result.provider,
            "message": error_message  # 添加友好的错误消息
        }
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    ctx = auth_result.context
    
    # 从数据库获取/创建用户信息
    try:
        conn = await asyncpg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )

        try:
            row = await _get_or_create_local_user(conn, ctx)

            if not row:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="用户不存在"
                )

            # 检查用户是否激活
            if not row['is_active']:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="用户已被禁用"
                )

            # 系统角色直接使用数据库值
            return User(
                user_id=row['user_id'],
                username=row['username'],
                email=row['email'],
                full_name=row['full_name'],
                role=row['role'],
                is_active=row['is_active'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                last_login=row['last_login_at']
            )
        finally:
            await conn.close()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取用户信息失败: {str(e)}"
        )


async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """获取当前激活的用户
    
    Args:
        current_user: 当前用户
        
    Returns:
        User: 当前激活的用户
        
    Raises:
        HTTPException: 400 如果用户未激活
    """
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="用户未激活"
        )
    return current_user


def require_role(required_role: str):
    """要求特定角色权限（工厂函数）
    
    用法: Depends(require_role(UserRole.ADMIN))
    
    Args:
        required_role: 需要的角色
        
    Returns:
        依赖函数
    """
    async def role_checker(
        current_user: User = Depends(get_current_active_user)
    ) -> User:
        if not auth_service.check_permission(current_user, required_role):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"需要 {required_role} 或更高权限"
            )
        return current_user
    return role_checker


# ============================================================
# 系统角色定义（直接使用数据库值）
# ============================================================
# admin: 系统管理员 - 所有功能
# data_admin: 数据管理员 - 数据库连接/元数据/Milvus同步/数据权限
# user: 普通用户 - 仅查询，不能登录后台

# 允许登录后台的角色
BACKEND_ALLOWED_ROLES = ["admin", "data_admin"]

# 数据管理相关功能允许的角色
DATA_MANAGEMENT_ROLES = ["admin", "data_admin"]


# ============================================================
# 常用的权限依赖
# ============================================================

async def require_backend_access(
    current_user: User = Depends(get_current_active_user)
) -> User:
    """要求后台访问权限（系统管理员或数据管理员）
    
    普通用户(user)不能登录后台管理系统
    """
    if current_user.role not in BACKEND_ALLOWED_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="您没有访问管理后台的权限"
        )
    return current_user


async def require_admin(
    current_user: User = Depends(get_current_active_user)
) -> User:
    """要求系统管理员权限"""
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="需要系统管理员权限"
        )
    return current_user


async def require_super_admin(
    current_user: User = Depends(get_current_active_user)
) -> User:
    """要求系统管理员权限（别名，兼容旧代码）"""
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="需要系统管理员权限"
        )
    return current_user


async def require_data_admin(
    current_user: User = Depends(get_current_active_user)
) -> User:
    """要求数据管理权限（系统管理员或数据管理员）
    
    可访问：数据库连接、元数据管理、Milvus同步、数据权限
    """
    if current_user.role not in DATA_MANAGEMENT_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="需要数据管理权限"
        )
    return current_user


async def require_editor(
    current_user: User = Depends(get_current_active_user)
) -> User:
    """要求编辑权限（兼容旧接口，等同于数据管理权限）"""
    if current_user.role not in DATA_MANAGEMENT_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="需要编辑权限"
        )
    return current_user


def check_tenant_access(user: User, tenant_id: str):
    """检查用户是否有权访问指定租户
    
    Args:
        user: 用户对象
        tenant_id: 租户ID
        
    Raises:
        HTTPException: 403 如果无权访问
    """
    if not auth_service.can_access_tenant(user, tenant_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"无权访问租户: {tenant_id}"
        )


# ============================================================
# 可选认证（用于查询接口）
# ============================================================

async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    request: Request = None,
) -> Optional[User]:
    """获取当前用户（可选）
    
    与 get_current_user 不同，此函数不会在未提供token时抛出异常，
    而是返回 None，允许匿名访问。
    
    用于查询接口，支持：
    1. 已认证用户：应用数据权限
    2. 匿名用户：使用默认/受限权限
    
    认证失败原因会存储在 request.state.auth_failure_reason 中，
    供 API 响应使用。
    
    Returns:
        Optional[User]: 当前用户对象，或 None（如果未认证）
    """
    auth_manager = get_auth_manager()
    token = credentials.credentials if credentials else None
    
    # 使用带结果的认证方法
    auth_result = await auth_manager.authenticate_with_result(request, token)
    
    # 存储认证状态到 request.state
    if request:
        request.state.auth_attempted = bool(token)
        request.state.auth_success = auth_result.success
        request.state.auth_error_code = auth_result.error_code if not auth_result.success else None
        request.state.auth_provider = auth_result.provider
    
    if not auth_result.success:
        return None
    
    ctx = auth_result.context
    
    # 从数据库获取用户信息
    try:
        conn = await asyncpg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )

        try:
            row = await _get_or_create_local_user(conn, ctx)
            if row and not row["is_active"]:
                if request:
                    request.state.auth_failure_reason = "用户已被禁用"
                return None

            if not row:
                if request:
                    request.state.auth_failure_reason = "用户创建失败"
                return None

            return User(
                user_id=row['user_id'],
                username=row['username'],
                email=row['email'],
                full_name=row['full_name'],
                role=row['role'],
                is_active=row['is_active'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                last_login=row['last_login_at']
            )
        finally:
            await conn.close()

    except Exception as e:
        # 认证失败不抛异常，返回None允许匿名访问
        import structlog
        logger = structlog.get_logger()
        logger.warning(f"可选认证失败: {e}")
        if request:
            request.state.auth_failure_reason = f"用户信息获取失败: {str(e)}"
        return None

