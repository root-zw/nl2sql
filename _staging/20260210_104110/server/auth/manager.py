"""
认证管理器：注册多个 Provider，按优先级依次尝试。
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import Request
import structlog

from server.auth.base import AuthProvider, AuthContext, AuthResult, AuthenticationError, AuthErrorCode

logger = structlog.get_logger()


class AuthManager:
    """认证管理器"""

    def __init__(self):
        self._providers: List[AuthProvider] = []

    def register(self, provider: AuthProvider):
        """注册认证提供者"""
        self._providers.append(provider)
        # 按优先级从高到低排序
        self._providers.sort(key=lambda p: p.priority, reverse=True)
        logger.info("auth provider registered", provider=provider.name, priority=provider.priority, enabled=provider.enabled)

    def list_providers(self) -> List[AuthProvider]:
        return list(self._providers)

    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        """按优先级尝试认证，返回第一个成功的结果"""
        result = await self.authenticate_with_result(request, token)
        return result.context if result.success else None
    
    async def authenticate_with_result(self, request: Request, token: Optional[str] = None) -> AuthResult:
        """
        按优先级尝试认证，返回包含错误代码和失败原因的结果
        
        Returns:
            AuthResult: 认证结果，包含成功/失败信息、错误代码和失败原因
        """
        if not token:
            return AuthResult.fail(AuthErrorCode.NO_TOKEN, provider=None)
        
        # 记录最后一个明确的认证错误（优先返回）
        last_auth_error: AuthenticationError = None
        other_failures = []
        
        for provider in self._providers:
            if not provider.enabled:
                continue
            try:
                ctx = await provider.authenticate(request, token)
                if ctx:
                    logger.debug("auth success", provider=provider.name, user_id=ctx.user_id, source=ctx.source)
                    return AuthResult.ok(ctx)
            except AuthenticationError as auth_exc:
                # 明确的认证失败，记录错误代码
                last_auth_error = auth_exc
                logger.warning(
                    "auth provider rejected",
                    provider=provider.name,
                    error_code=auth_exc.code,
                    message=auth_exc.message
                )
                continue
            except Exception as exc:  # noqa: BLE001
                reason = str(exc)
                other_failures.append(f"{provider.name}: {reason}")
                logger.warning("auth provider failed", provider=provider.name, error=reason)
                continue
        
        # 返回最后一个明确的认证错误（带错误代码）
        if last_auth_error:
            return AuthResult.fail(
                AuthErrorCode(last_auth_error.code),
                detail=last_auth_error.detail,
                provider=last_auth_error.provider or "unknown"
            )
        
        # 没有明确的认证错误，返回通用失败
        if other_failures:
            return AuthResult.fail_with_message(
                code=int(AuthErrorCode.ALL_PROVIDERS_FAILED),
                message="所有认证提供者均失败",
                detail="; ".join(other_failures),
                provider="all"
            )
        
        return AuthResult.fail(AuthErrorCode.NO_PROVIDER, provider=None)

