"""
OIDC 认证提供者：校验上游 id_token，映射到本地用户身份。
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

import httpx
from fastapi import Request
from jose import jwt, jwk
from jose.utils import base64url_decode
import structlog

from server.auth.base import AuthProvider, AuthContext, AuthenticationError, AuthErrorCode

logger = structlog.get_logger()


class OIDCProviderConfig:
    def __init__(
        self,
        issuer_url: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        scope: str = "openid profile email",
        role_mapping: Optional[Dict[str, str]] = None,
        name: str = "oidc",
        audiences: Optional[list[str]] = None,
        leeway_seconds: int = 60,
        # === 前端重定向配置 ===
        frontend_redirect_url: Optional[str] = None,  # 登录成功后重定向的前端地址
        # === 数据角色同步配置 ===
        data_role_claim: str = "roles",  # 从哪个 claim 读取数据角色
        auto_create_data_role: bool = True,  # 是否自动创建不存在的数据角色
        user_attribute_claims: Optional[list[str]] = None,  # 需要同步的用户属性字段
        # 用户同步 API 配置（用于获取用户列表）
        users_api_url: Optional[str] = None,  # 用户列表接口地址
        users_api_token_url: Optional[str] = None,  # Token 获取地址（留空使用 OIDC discovery）
        users_api_client_id: Optional[str] = None,  # 留空使用上面的 client_id
        users_api_client_secret: Optional[str] = None,  # 留空使用上面的 client_secret
        users_api_scope: Optional[str] = None,  # Token 请求的 scope
        users_field_mapping: Optional[Dict[str, str]] = None,  # 字段映射
    ):
        self.issuer_url = issuer_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scope = scope
        self.role_mapping = role_mapping or {}
        self.name = name
        self.audiences = audiences
        self.leeway_seconds = leeway_seconds
        # 前端重定向配置
        self.frontend_redirect_url = frontend_redirect_url.rstrip("/") if frontend_redirect_url else None
        # 数据角色同步配置
        self.data_role_claim = data_role_claim
        self.auto_create_data_role = auto_create_data_role
        self.user_attribute_claims = user_attribute_claims or ["department", "region"]
        # 用户同步 API 配置
        self.users_api_url = users_api_url.rstrip("/") if users_api_url else None
        self.users_api_token_url = users_api_token_url
        self.users_api_client_id = users_api_client_id or client_id
        self.users_api_client_secret = users_api_client_secret or client_secret
        self.users_api_scope = users_api_scope
        # 字段映射（默认适配常见格式）
        self.users_field_mapping = users_field_mapping or {
            "list_path": "",  # 用户列表在响应中的路径，空表示根数组
            "id_field": "id",
            "username_field": "username",
            "email_field": "email",
            "first_name_field": "firstName",
            "last_name_field": "lastName",
            "name_field": "name",
            "enabled_field": "enabled",
            "roles_field": "roles",
            "phone_field": "phone",
            "department_field": "department",
        }


class OIDCProvider(AuthProvider):
    """OIDC Provider，支持直接使用 id_token 作为 Bearer 认证。"""

    def __init__(self, config: OIDCProviderConfig, priority: int = 80, enabled: bool = True):
        super().__init__(name=config.name, priority=priority, enabled=enabled)
        self.config = config
        self._discovery_cache: Optional[Dict[str, Any]] = None
        self._jwks_cache: Optional[Dict[str, Any]] = None
        self._jwks_lock = asyncio.Lock()

    async def _fetch_discovery(self) -> Dict[str, Any]:
        if self._discovery_cache:
            return self._discovery_cache
        url = f"{self.config.issuer_url}/.well-known/openid-configuration"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            self._discovery_cache = resp.json()
            return self._discovery_cache

    async def _fetch_jwks(self) -> Dict[str, Any]:
        if self._jwks_cache:
            return self._jwks_cache
        async with self._jwks_lock:
            if self._jwks_cache:
                return self._jwks_cache
            discovery = await self._fetch_discovery()
            jwks_uri = discovery.get("jwks_uri")
            if not jwks_uri:
                raise ValueError("jwks_uri not provided by discovery")
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(jwks_uri)
                resp.raise_for_status()
                self._jwks_cache = resp.json()
                return self._jwks_cache

    @staticmethod
    def _normalize_role_value(value: Any) -> Optional[str]:
        """将外部角色值标准化为字符串，兼容字典结构。"""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            for key in ("name", "value", "role", "code", "id", "key"):
                if value.get(key):
                    return str(value[key])
            # 兜底：序列化整个对象，避免不可哈希错误
            return json.dumps(value, ensure_ascii=False)
        try:
            return str(value)
        except Exception:
            return None

    def map_role(self, external_roles: Optional[list[Any]]) -> str:
        """将外部角色映射到系统角色，容错处理非字符串角色值。"""
        normalized_roles = []
        for r in external_roles or []:
            normalized = self._normalize_role_value(r)
            if normalized:
                normalized_roles.append(normalized)

        if not normalized_roles:
            return self.config.role_mapping.get("*", "user")

        for r in normalized_roles:
            mapped = self.config.role_mapping.get(r)
            if mapped:
                return mapped
        return self.config.role_mapping.get("*", "user")

    def _validate_id_token(self, token: str, jwks: Dict[str, Any]) -> Dict[str, Any]:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        alg = header.get("alg")
        if not kid:
            raise ValueError("missing kid in id_token header")
        key_data = None
        for k in jwks.get("keys", []):
            if k.get("kid") == kid:
                key_data = k
                break
        if not key_data:
            raise ValueError("kid not found in jwks")

        public_key = jwk.construct(key_data)

        # 可选：手动验签（python-jose decode 会再次验签，这里先快速检查）
        message, encoded_sig = token.rsplit(".", 1)
        decoded_sig = base64url_decode(encoded_sig.encode())
        if not public_key.verify(message.encode(), decoded_sig):
            raise ValueError("id_token signature verification failed")

        claims = jwt.decode(
            token,
            public_key.to_pem().decode("utf-8"),
            algorithms=[alg],
            audience=self.config.audiences or self.config.client_id,
            issuer=self.config.issuer_url,
            options={"leeway": self.config.leeway_seconds},
        )
        return claims

    async def validate_id_token(self, token: str) -> Dict[str, Any]:
        """对外暴露的 id_token 校验"""
        jwks = await self._fetch_jwks()
        return self._validate_id_token(token, jwks)

    def _extract_data_roles(self, claims: Dict[str, Any]) -> list[str]:
        """从 claims 中提取数据角色列表"""
        # 尝试从配置的 claim 字段获取
        roles = claims.get(self.config.data_role_claim)
        
        # 回退到常见的角色字段
        if not roles:
            roles = claims.get("roles") or claims.get("role") or claims.get("groups")
        
        # 统一转换为列表并标准化
        if roles is None:
            return []
        if isinstance(roles, (str, dict)):
            roles = [roles]
        if isinstance(roles, list):
            normalized = []
            for r in roles:
                nr = self._normalize_role_value(r)
                if nr:
                    normalized.append(nr)
            return normalized
        return []
    
    def _extract_user_attributes(self, claims: Dict[str, Any]) -> Dict[str, Any]:
        """从 claims 中提取用户属性"""
        attributes = {}
        for attr_name in self.config.user_attribute_claims:
            if attr_name in claims and claims[attr_name] is not None:
                attributes[attr_name] = claims[attr_name]
        return attributes

    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        # 若未启用，直接跳过
        if not self.enabled:
            return None

        if token is None:
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return None
            token = auth_header[7:]

        try:
            jwks = await self._fetch_jwks()
            claims = self._validate_id_token(token, jwks)
            sub = claims.get("sub")
            if not sub:
                return None

            # 提取数据角色列表
            data_roles = self._extract_data_roles(claims)
            
            # 映射系统角色（基于数据角色）
            role = self.map_role(data_roles)
            
            # 提取用户属性（用于行级权限）
            user_attributes = self._extract_user_attributes(claims)

            return AuthContext(
                user_id=sub,
                # 用户名优先级：preferred_username > name（Casdoor等使用） > email
                username=claims.get("preferred_username") or claims.get("name") or claims.get("email"),
                role=role,
                source=self.name,
                # full_name 优先使用 displayName（Casdoor）
                attributes={"email": claims.get("email"), "name": claims.get("displayName") or claims.get("name")},
                extra={"claims": claims},
                # 新增：数据角色和用户属性
                data_roles=data_roles,
                user_attributes=user_attributes,
            )
        except AuthenticationError:
            # 已经是认证错误，直接抛出
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("oidc authenticate failed", error=str(exc))
            raise AuthenticationError(AuthErrorCode.OIDC_FAILED, detail=str(exc), provider=self.name)

    async def get_discovery(self) -> Dict[str, Any]:
        """对外暴露，供路由获取授权/令牌端点。"""
        return await self._fetch_discovery()

    async def _get_users_api_token(self) -> Optional[str]:
        """
        获取用户列表 API 的访问令牌。
        使用 client_credentials 流程获取服务账号令牌。
        """
        # 确定 token endpoint
        token_url = self.config.users_api_token_url
        if not token_url:
            # 尝试从 discovery 获取
            try:
                discovery = await self._fetch_discovery()
                token_url = discovery.get("token_endpoint")
            except Exception:
                token_url = f"{self.config.issuer_url}/protocol/openid-connect/token"
        
        if not token_url:
            return None
        
        try:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.config.users_api_client_id,
                "client_secret": self.config.users_api_client_secret,
            }
            if self.config.users_api_scope:
                data["scope"] = self.config.users_api_scope
            
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(token_url, data=data)
                resp.raise_for_status()
                result = resp.json()
                return result.get("access_token")
        except Exception as e:
            logger.warning("获取用户列表 API 令牌失败", error=str(e))
            return None

    async def list_users(self, max_results: int = 1000) -> Dict[str, Any]:
        """
        从配置的用户列表 API 获取用户。
        
        用户需要配置:
        - users_api_url: 用户列表接口地址
        - users_api_token_url: Token 获取地址（可选，默认使用 OIDC discovery）
        - users_field_mapping: 字段映射配置
        
        Args:
            max_results: 最大返回用户数
            
        Returns:
            包含用户列表和状态的字典
        """
        if not self.config.users_api_url:
            return {
                "success": False, 
                "message": "未配置用户列表接口地址 (users_api_url)，无法获取用户列表", 
                "users": []
            }
        
        try:
            # 获取 API 令牌
            token = await self._get_users_api_token()
            if not token:
                return {
                    "success": False,
                    "message": "无法获取 API 访问令牌，请检查 Client 配置是否启用了 Service Account",
                    "users": []
                }
            
            # 调用用户列表接口
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.get(
                    self.config.users_api_url,
                    headers={"Authorization": f"Bearer {token}"},
                    params={"max": max_results},
                )
                resp.raise_for_status()
                data = resp.json()
            
            # 解析用户列表
            mapping = self.config.users_field_mapping
            list_path = mapping.get("list_path", "")
            
            # 支持嵌套路径，如 "data.users"
            raw_users = data
            if list_path:
                for key in list_path.split("."):
                    if key and isinstance(raw_users, dict):
                        raw_users = raw_users.get(key, [])
            
            if not isinstance(raw_users, list):
                raw_users = [raw_users] if raw_users else []
            
            # 转换为统一格式
            users = []
            for u in raw_users:
                if not isinstance(u, dict):
                    continue
                
                user_id = str(u.get(mapping.get("id_field", "id"), ""))
                username = str(u.get(mapping.get("username_field", "username"), ""))
                
                if not user_id and not username:
                    continue
                
                email = u.get(mapping.get("email_field", "email"))
                
                # 处理姓名字段
                first_name = u.get(mapping.get("first_name_field", "firstName"), "") or ""
                last_name = u.get(mapping.get("last_name_field", "lastName"), "") or ""
                full_name = u.get(mapping.get("name_field", "name"))
                if not full_name and (first_name or last_name):
                    full_name = f"{first_name} {last_name}".strip()
                phone = u.get(mapping.get("phone_field", "phone"))
                department = u.get(mapping.get("department_field", "department"))
                
                # 处理启用状态
                enabled_field = mapping.get("enabled_field", "enabled")
                enabled = u.get(enabled_field, True)
                if isinstance(enabled, str):
                    enabled = enabled.lower() in ("true", "1", "yes", "active")
                
                # 处理角色
                roles_field = mapping.get("roles_field", "roles")
                roles = u.get(roles_field, [])
                if isinstance(roles, str):
                    roles = [roles]
                if roles is None:
                    roles = []
                role = self.map_role(roles)
                
                # 提取用户属性
                user_attrs = {}
                for attr_name in self.config.user_attribute_claims:
                    if attr_name in u and u[attr_name] is not None:
                        user_attrs[attr_name] = u[attr_name]
                
                users.append({
                    "external_uid": user_id or username,
                    "username": username or user_id,
                    "email": email,
                    "full_name": full_name,
                    "role": role,
                    "is_active": bool(enabled),
                    "data_roles": roles,  # 新增：数据角色列表
                    "user_attributes": user_attrs,  # 新增：用户属性
                    "profile": {"source": "oidc", "raw_data": u, "phone": phone, "department": department}
                })
            
            logger.info("OIDC 用户列表获取成功", count=len(users))
            return {
                "success": True,
                "message": f"成功获取 {len(users)} 个用户",
                "users": users
            }
            
        except httpx.HTTPStatusError as e:
            error_msg = f"用户列表 API 请求失败: HTTP {e.response.status_code}"
            if e.response.status_code == 401:
                error_msg = "API 认证失败，请检查 Client ID/Secret 配置"
            elif e.response.status_code == 403:
                error_msg = "API 权限不足，请确保 Client 具有相应权限"
            logger.error(error_msg, status_code=e.response.status_code)
            return {"success": False, "message": error_msg, "users": []}
        except Exception as e:
            logger.error("获取 OIDC 用户列表失败", error=str(e))
            return {"success": False, "message": f"获取用户列表失败: {str(e)}", "users": []}

