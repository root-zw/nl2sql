"""认证服务模块"""

from datetime import datetime, timedelta
from typing import Optional, Tuple
from uuid import UUID
import hashlib
import secrets
import structlog

from jose import JWTError, jwt
from passlib.context import CryptContext

from server.config import settings
from server.models.admin import User, UserInDB, TokenData
from server.utils.timezone_helper import now_utc, now_with_tz, get_datetime_with_delta


# 密码加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

logger = structlog.get_logger()


class RefreshTokenData:
    """Refresh Token 解码后的数据"""
    def __init__(self, user_id: UUID, token_id: str):
        self.user_id = user_id
        self.token_id = token_id  # 用于撤销特定的 refresh token


class AuthService:
    """认证服务类"""
    
    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """验证密码
        
        Args:
            plain_password: 明文密码
            hashed_password: 哈希后的密码
            
        Returns:
            bool: 密码是否匹配
        """
        return pwd_context.verify(plain_password, hashed_password)
    
    @staticmethod
    def get_password_hash(password: str) -> str:
        """生成密码哈希
        
        Args:
            password: 明文密码
            
        Returns:
            str: 哈希后的密码
        """
        return pwd_context.hash(password)
    
    @staticmethod
    def create_access_token(
        user: User,
        expires_delta: Optional[timedelta] = None
    ) -> tuple[str, datetime]:
        """创建JWT访问令牌
        
        Args:
            user: 用户对象
            expires_delta: 过期时间增量，默认使用配置中的值
            
        Returns:
            tuple: (token字符串, 过期时间)
        """
        # 设置过期时间
        if expires_delta:
            expire = now_utc() + expires_delta
        else:
            expire = get_datetime_with_delta(base_time=now_utc(), minutes=settings.jwt_expire_minutes)
        
        # 构建JWT payload
        payload = {
            "sub": str(user.user_id),  # subject: 用户ID
            "username": user.username,
            "role": user.role,
            "exp": expire,  # expiration time
            "iat": now_utc(),  # issued at
        }
        
        # 生成JWT token
        encoded_jwt = jwt.encode(
            payload,
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm
        )
        
        return encoded_jwt, expire
    
    @staticmethod
    def create_refresh_token(
        user: User,
        expires_delta: Optional[timedelta] = None
    ) -> Tuple[str, datetime, str]:
        """创建 Refresh Token
        
        Args:
            user: 用户对象
            expires_delta: 过期时间增量，默认使用配置中的值
            
        Returns:
            tuple: (token字符串, 过期时间, token_id)
        """
        # 生成唯一的 token ID（用于撤销）
        token_id = secrets.token_urlsafe(16)
        
        # 设置过期时间
        if expires_delta:
            expire = now_utc() + expires_delta
        else:
            expire = now_utc() + timedelta(days=settings.jwt_refresh_expire_days)
        
        # 构建 Refresh Token payload
        payload = {
            "sub": str(user.user_id),
            "type": "refresh",  # 标识这是 refresh token
            "jti": token_id,    # JWT ID，用于撤销
            "exp": expire,
            "iat": now_utc(),
        }
        
        # 生成 JWT token
        encoded_jwt = jwt.encode(
            payload,
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm
        )
        
        return encoded_jwt, expire, token_id
    
    @staticmethod
    def decode_refresh_token(token: str) -> Optional[RefreshTokenData]:
        """解码 Refresh Token
        
        Args:
            token: Refresh Token 字符串
            
        Returns:
            RefreshTokenData: 解码后的数据，如果失败则返回 None
        """
        try:
            payload = jwt.decode(
                token,
                settings.jwt_secret,
                algorithms=[settings.jwt_algorithm]
            )
            
            # 验证是 refresh token
            if payload.get("type") != "refresh":
                logger.warning("Token is not a refresh token")
                return None
            
            user_id_raw = payload.get("sub")
            token_id = payload.get("jti")
            
            if not user_id_raw or not token_id:
                logger.warning("Refresh token missing required fields")
                return None
            
            try:
                user_id = UUID(user_id_raw)
            except (ValueError, TypeError):
                logger.warning("Invalid user_id in refresh token")
                return None
            
            return RefreshTokenData(user_id=user_id, token_id=token_id)
            
        except JWTError as e:
            logger.warning("Refresh token decode failed", error=str(e))
            return None
        except Exception as e:
            logger.error("Unexpected error during refresh token decode", error=str(e))
            return None
    
    @staticmethod
    def create_token_pair(user: User) -> dict:
        """创建 Access Token 和 Refresh Token 对
        
        Args:
            user: 用户对象
            
        Returns:
            dict: 包含 access_token, refresh_token 和相关信息
        """
        access_token, access_expire = AuthService.create_access_token(user)
        refresh_token, refresh_expire, token_id = AuthService.create_refresh_token(user)
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": settings.jwt_expire_minutes * 60,  # 秒
            "refresh_expires_in": settings.jwt_refresh_expire_days * 24 * 60 * 60,  # 秒
            "access_token_expires_at": access_expire.isoformat(),
            "refresh_token_expires_at": refresh_expire.isoformat(),
        }
    
    @staticmethod
    def decode_access_token(token: str) -> Optional[TokenData]:
        """解码JWT访问令牌
        
        Args:
            token: JWT token字符串
            
        Returns:
            TokenData: 解码后的token数据，如果失败则返回None
        """
        try:
            payload = jwt.decode(
                token,
                settings.jwt_secret,
                algorithms=[settings.jwt_algorithm]
            )

            # 获取用户ID，支持多种可能的字段名
            user_id_raw = payload.get("sub") or payload.get("user_id") or payload.get("id")
            username: str = payload.get("username")
            role: str = payload.get("role", "user")  # 默认角色为user

            # 验证必需字段
            if user_id_raw is None:
                logger.warning("JWT token missing user identification", payload=payload)
                return None

            if username is None:
                username = payload.get("name") or payload.get("email") or "unknown"

            try:
                # 转换为UUID对象
                from uuid import UUID
                user_id: UUID = UUID(user_id_raw)
            except (ValueError, TypeError) as e:
                logger.warning("Invalid user_id format in JWT token", user_id=user_id_raw, error=str(e))
                return None

            return TokenData(
                user_id=user_id,
                username=username,
                role=role
            )
        except JWTError as e:
            logger.warning("JWT decode failed", error=str(e))
            return None
        except Exception as e:
            logger.error("Unexpected error during JWT decode", error=str(e))
            return None
    
    @staticmethod
    def hash_token_for_storage(token: str) -> str:
        """为存储生成token的哈希值
        
        用于在数据库中安全存储token（不存储原始token）
        
        Args:
            token: 原始token
            
        Returns:
            str: token的SHA256哈希值
        """
        return hashlib.sha256(token.encode()).hexdigest()
    
    @staticmethod
    def check_permission(user: User, required_role: str) -> bool:
        """检查用户权限
        
        Args:
            user: 用户对象
            required_role: 需要的角色
            
        Returns:
            bool: 是否有权限
        """
        # 角色层级（从高到低，直接使用数据库值）
        role_hierarchy = {
            "admin": 3,       # 系统管理员
            "data_admin": 2,  # 数据管理员
            "user": 1         # 普通用户
        }
        
        user_level = role_hierarchy.get(user.role, 0)
        required_level = role_hierarchy.get(required_role, 0)
        
        return user_level >= required_level
    
    @staticmethod
    def can_access_tenant(user: User, tenant_id: str) -> bool:
        """检查用户是否可以访问指定租户
        
        Args:
            user: 用户对象
            tenant_id: 租户ID
            
        Returns:
            bool: 是否可以访问
        """
        # admin可以访问所有租户
        if user.role == "admin":
            return True
        
        # 其他角色只能访问自己的租户
        return user.tenant_id == tenant_id


# 创建全局认证服务实例
auth_service = AuthService()


# ============================================================
# 辅助函数
# ============================================================

def authenticate_user(user_db: UserInDB, password: str) -> bool:
    """验证用户凭据
    
    Args:
        user_db: 数据库中的用户对象（包含密码哈希）
        password: 用户输入的明文密码
        
    Returns:
        bool: 认证是否成功
    """
    if not user_db:
        return False
    if not auth_service.verify_password(password, user_db.password_hash):
        return False
    if not user_db.is_active:
        return False
    return True


def create_default_admin_user() -> UserInDB:
    """创建默认管理员用户对象（用于初始化）
    
    Returns:
        UserInDB: 默认管理员用户对象
    """
    return UserInDB(
        user_id=1,
        username="admin",
        email="admin@NL2SQL.local",
        password_hash=auth_service.get_password_hash(settings.default_admin_password),
        full_name="系统管理员",
        role="admin",  # 直接使用数据库角色值
        tenant_id=None,
        is_active=True,
        created_at=now_with_tz(),
        updated_at=now_with_tz()
    )

