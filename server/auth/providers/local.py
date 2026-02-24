"""
本地 JWT 提供者：复用现有 auth_service.decode_access_token。
"""

from __future__ import annotations

from typing import Optional

from fastapi import Request
from jose import jwt, JWTError, ExpiredSignatureError

from server.auth.base import AuthProvider, AuthContext, AuthenticationError, AuthErrorCode
from server.services.auth_service import auth_service
from server.config import settings


class LocalJWTProvider(AuthProvider):
    """本地 JWT 认证"""

    def __init__(self, priority: int = 100, enabled: bool = True):
        super().__init__(name="local", priority=priority, enabled=enabled)

    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        # 优先使用传入 token，否则从 Header 读取
        if token is None:
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return None
            token = auth_header[7:]

        # 先尝试解码以获取详细错误信息
        try:
            jwt.decode(
                token,
                settings.jwt_secret,
                algorithms=[settings.jwt_algorithm]
            )
        except ExpiredSignatureError:
            # JWT 已过期，抛出明确的错误
            raise AuthenticationError(AuthErrorCode.JWT_EXPIRED, provider=self.name)
        except JWTError:
            # 其他 JWT 错误（签名无效等）
            raise AuthenticationError(AuthErrorCode.JWT_DECODE_FAILED, provider=self.name)
        except Exception:
            # 未知错误
            raise AuthenticationError(AuthErrorCode.JWT_DECODE_FAILED, provider=self.name)

        # 使用 auth_service 完成完整的解码和验证
        token_data = auth_service.decode_access_token(token)
        if not token_data:
            raise AuthenticationError(AuthErrorCode.JWT_DECODE_FAILED, provider=self.name)

        return AuthContext(
            user_id=str(token_data.user_id),
            username=token_data.username,
            role=token_data.role,
            source="local_jwt",
            attributes=None,
        )

