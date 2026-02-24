"""
认证提供者基类与通用类型。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional

from fastapi import Request


class AuthErrorCode(IntEnum):
    """认证错误代码"""
    # 通用错误 (10000-10099)
    NO_TOKEN = 10001              # 未提供认证令牌
    NO_PROVIDER = 10002           # 没有可用的认证提供者
    ALL_PROVIDERS_FAILED = 10003  # 所有认证提供者均失败
    
    # AES Token 错误 (10100-10199)
    AES_DECODE_FAILED = 10101     # Token Base64 解码失败
    AES_DECRYPT_FAILED = 10102    # Token 解密失败
    AES_PARSE_FAILED = 10103      # Token 字段解析失败
    AES_SERVICE_KEY_INVALID = 10104  # ServiceKey 验证失败
    AES_TOKEN_EXPIRED = 10105     # Token 已过期或时间戳无效
    AES_ROLE_NOT_FOUND = 10106    # 数据角色不存在
    
    # JWT 错误 (10200-10299)
    JWT_DECODE_FAILED = 10201     # JWT 解析失败
    JWT_EXPIRED = 10202           # JWT 已过期
    JWT_INVALID_SIGNATURE = 10203 # JWT 签名无效
    
    # OIDC 错误 (10300-10399)
    OIDC_FAILED = 10301           # OIDC 认证失败
    OIDC_TOKEN_INVALID = 10302    # OIDC Token 无效


# 错误代码到错误信息的映射
AUTH_ERROR_MESSAGES: Dict[int, str] = {
    AuthErrorCode.NO_TOKEN: "未提供认证令牌",
    AuthErrorCode.NO_PROVIDER: "没有可用的认证提供者",
    AuthErrorCode.ALL_PROVIDERS_FAILED: "所有认证提供者均失败",
    
    AuthErrorCode.AES_DECODE_FAILED: "Token 解码失败",
    AuthErrorCode.AES_DECRYPT_FAILED: "Token 解密失败",
    AuthErrorCode.AES_PARSE_FAILED: "Token 字段解析失败",
    AuthErrorCode.AES_SERVICE_KEY_INVALID: "ServiceKey 验证失败",
    AuthErrorCode.AES_TOKEN_EXPIRED: "Token 已过期或时间戳无效",
    AuthErrorCode.AES_ROLE_NOT_FOUND: "数据角色不存在",
    
    AuthErrorCode.JWT_DECODE_FAILED: "JWT 解析失败",
    AuthErrorCode.JWT_EXPIRED: "JWT 已过期",
    AuthErrorCode.JWT_INVALID_SIGNATURE: "JWT 签名无效",
    
    AuthErrorCode.OIDC_FAILED: "OIDC 认证失败",
    AuthErrorCode.OIDC_TOKEN_INVALID: "OIDC Token 无效",
}


def get_error_message(code: int, detail: str = None) -> str:
    """根据错误代码获取错误信息"""
    base_msg = AUTH_ERROR_MESSAGES.get(code, "未知错误")
    if detail:
        return f"{base_msg}: {detail}"
    return base_msg


class AuthenticationError(Exception):
    """认证失败异常，携带错误代码和失败原因"""
    def __init__(self, code: AuthErrorCode, detail: str = None, provider: str = None):
        self.code = int(code)
        self.detail = detail
        self.provider = provider
        self.message = get_error_message(code, detail)
        super().__init__(self.message)


@dataclass
class AuthContext:
    """
    认证结果上下文
    
    Attributes:
        user_id: 用户唯一标识（外部系统的 sub 或 user_id）
        username: 用户名
        role: 系统角色（admin/data_admin/user），通过角色映射确定
        source: 认证来源标识（如 oidc_main, api_gateway）
        attributes: 用户属性字典（如 email, name）
        extra: 额外信息（如完整的 claims）
        data_roles: 数据角色列表（来自认证服务器的角色，如 sales_manager）
        user_attributes: 用户业务属性（用于行级权限，如 department, region）
    """
    user_id: str
    username: Optional[str]
    role: Optional[str]
    source: str
    attributes: Optional[Dict[str, Any]] = None
    extra: Optional[Dict[str, Any]] = None
    # 新增：数据角色列表（来自认证服务器）
    data_roles: List[str] = field(default_factory=list)
    # 新增：用户业务属性（用于行级权限过滤）
    user_attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthResult:
    """
    认证结果（包含成功/失败信息）
    
    Attributes:
        success: 是否认证成功
        context: 认证成功时的上下文
        error_code: 错误代码（数字）
        error_message: 错误信息
        error_detail: 错误详情（如具体的角色名等）
        provider: 尝试认证的提供者名称
    """
    success: bool
    context: Optional[AuthContext] = None
    error_code: Optional[int] = None
    error_message: Optional[str] = None
    error_detail: Optional[str] = None
    provider: Optional[str] = None
    
    @classmethod
    def ok(cls, context: AuthContext) -> "AuthResult":
        """创建成功结果"""
        return cls(success=True, context=context, provider=context.source)
    
    @classmethod
    def fail(cls, code: AuthErrorCode, detail: str = None, provider: Optional[str] = None) -> "AuthResult":
        """创建失败结果"""
        return cls(
            success=False,
            error_code=int(code),
            error_message=get_error_message(code),
            error_detail=detail,
            provider=provider
        )
    
    @classmethod
    def fail_with_message(cls, code: int, message: str, detail: str = None, provider: Optional[str] = None) -> "AuthResult":
        """使用自定义消息创建失败结果"""
        return cls(
            success=False,
            error_code=code,
            error_message=message,
            error_detail=detail,
            provider=provider
        )


class AuthProvider(ABC):
    """认证提供者接口"""

    def __init__(self, name: str, priority: int = 100, enabled: bool = True):
        self._name = name
        self.priority = priority
        self.enabled = enabled

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        """尝试认证，返回 AuthContext 或 None"""
        raise NotImplementedError

