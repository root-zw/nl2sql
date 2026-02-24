"""
通用 OAuth 2.0 认证提供者

支持钉钉、企业微信、飞书等非标准 OIDC 的 OAuth 2.0 认证服务。
与 OIDC Provider 的区别：
- 不依赖 /.well-known/openid-configuration 发现端点
- 使用 access_token 而非 id_token
- 需要调用 userinfo 接口获取用户信息
- 支持灵活的字段映射
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

import httpx
from fastapi import Request
import structlog

from server.auth.base import AuthProvider, AuthContext, AuthenticationError, AuthErrorCode

logger = structlog.get_logger()


@dataclass
class OAuth2ProviderConfig:
    """OAuth 2.0 提供者配置"""
    
    # === 基础配置 ===
    name: str = "oauth2"  # 提供者标识
    
    # === 凭证配置 ===
    client_id: str = ""  # 也可以是 AppKey、CorpID 等
    client_secret: str = ""  # 也可以是 AppSecret、Secret 等
    
    # === 端点配置（必须手动配置，不支持自动发现）===
    authorization_endpoint: str = ""  # 授权端点
    token_endpoint: str = ""  # Token 端点
    userinfo_endpoint: str = ""  # 用户信息端点
    
    # === 请求配置 ===
    redirect_uri: str = ""
    scope: str = ""  # 默认 scope
    response_type: str = "code"  # 默认 authorization_code 流程
    
    # === Token 请求配置 ===
    # Token 请求方式: "body" (form data) | "basic_auth" (HTTP Basic Auth) | "query" (URL params)
    token_auth_method: str = "body"
    # Token 请求内容类型: "form" (application/x-www-form-urlencoded) | "json" (application/json)
    # 钉钉 v1.0 API 需要 JSON 格式
    token_content_type: str = "form"
    # Token 请求参数风格: "snake_case" (标准 OAuth2) | "camel_case" (钉钉等)
    token_param_style: str = "snake_case"
    # 额外的 Token 请求参数（如钉钉需要 grant_type=authorization_code）
    token_extra_params: Dict[str, str] = field(default_factory=dict)
    
    # === Userinfo 请求配置 ===
    # Userinfo 请求方式: "header" (Bearer token) | "query" (URL param) | "body" (POST body)
    userinfo_auth_method: str = "header"
    # Userinfo 请求 HTTP 方法: "GET" | "POST"
    userinfo_http_method: str = "GET"
    # access_token 参数名（用于 query/body 方式）
    userinfo_token_param: str = "access_token"
    # 自定义 Token Header 名称（钉钉需要 x-acs-dingtalk-access-token）
    userinfo_token_header: str = ""  # 空表示使用标准 Authorization: Bearer
    # 额外的 Userinfo 请求参数
    userinfo_extra_params: Dict[str, str] = field(default_factory=dict)
    
    # === 字段映射配置 ===
    # Token 响应字段映射
    token_field_mapping: Dict[str, str] = field(default_factory=lambda: {
        "access_token": "access_token",
        "refresh_token": "refresh_token",
        "expires_in": "expires_in",
        "token_type": "token_type",
    })
    
    # Userinfo 响应字段映射
    userinfo_field_mapping: Dict[str, str] = field(default_factory=lambda: {
        "user_id": "id",  # 用户唯一标识
        "username": "username",  # 用户名
        "email": "email",
        "name": "name",  # 显示名称
        "phone": "phone",
        "avatar": "avatar",
        "department": "department",
    })
    
    # Userinfo 嵌套路径（如钉钉的用户信息在 result 字段下）
    userinfo_data_path: str = ""  # 空表示根对象，如 "result" 或 "data.user"
    
    # === 角色映射 ===
    role_mapping: Dict[str, str] = field(default_factory=dict)
    roles_field: str = "roles"  # 从 userinfo 的哪个字段读取角色
    
    # === 数据角色配置 ===
    data_role_claim: str = "roles"
    auto_create_data_role: bool = True
    user_attribute_claims: List[str] = field(default_factory=lambda: ["department", "region"])
    
    # === 前端重定向 ===
    frontend_redirect_url: Optional[str] = None
    
    # === 用户同步 API 配置 ===
    users_api_url: Optional[str] = None
    users_api_token_url: Optional[str] = None
    users_api_token_method: str = "client_credentials"  # 获取 API token 的方式
    users_api_extra_params: Dict[str, str] = field(default_factory=dict)
    users_field_mapping: Dict[str, str] = field(default_factory=lambda: {
        "list_path": "",
        "id_field": "id",
        "username_field": "username",
        "email_field": "email",
        "name_field": "name",
        "phone_field": "phone",
        "department_field": "department",
        "enabled_field": "enabled",
        "roles_field": "roles",
    })
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于序列化）"""
        return {
            "name": self.name,
            "client_id": self.client_id,
            "client_secret": "***",  # 隐藏敏感信息
            "authorization_endpoint": self.authorization_endpoint,
            "token_endpoint": self.token_endpoint,
            "userinfo_endpoint": self.userinfo_endpoint,
            "redirect_uri": self.redirect_uri,
            "scope": self.scope,
        }


class OAuth2Provider(AuthProvider):
    """
    通用 OAuth 2.0 认证提供者。
    
    支持 authorization_code 流程，可适配：
    - 钉钉 OAuth 2.0
    - 企业微信 OAuth 2.0
    - 飞书 OAuth 2.0
    - 其他非标准 OIDC 的 OAuth 2.0 服务
    """
    
    def __init__(self, config: OAuth2ProviderConfig, priority: int = 75, enabled: bool = True):
        super().__init__(name=config.name, priority=priority, enabled=enabled)
        self.config = config
        self._access_token_cache: Optional[Dict[str, Any]] = None
    
    @staticmethod
    def _normalize_role_value(value: Any) -> Optional[str]:
        """将外部角色值标准化为字符串"""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            for key in ("name", "value", "role", "code", "id", "key"):
                if value.get(key):
                    return str(value[key])
            return json.dumps(value, ensure_ascii=False)
        try:
            return str(value)
        except Exception:
            return None
    
    def map_role(self, external_roles: Optional[List[Any]]) -> str:
        """将外部角色映射到系统角色"""
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
    
    def _extract_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """从嵌套字典中提取值"""
        if not path:
            return data
        
        current = data
        for key in path.split("."):
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current
    
    def _extract_data_roles(self, userinfo: Dict[str, Any]) -> List[str]:
        """从用户信息中提取数据角色列表"""
        roles = userinfo.get(self.config.data_role_claim) or userinfo.get(self.config.roles_field)
        
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
    
    def _extract_user_attributes(self, userinfo: Dict[str, Any]) -> Dict[str, Any]:
        """从用户信息中提取用户属性"""
        attributes = {}
        for attr_name in self.config.user_attribute_claims:
            if attr_name in userinfo and userinfo[attr_name] is not None:
                attributes[attr_name] = userinfo[attr_name]
        return attributes
    
    async def exchange_code_for_token(self, code: str) -> Dict[str, Any]:
        """
        用 authorization code 换取 access_token。
        
        Args:
            code: 授权码
            
        Returns:
            Token 响应（已经过字段映射）
        """
        if not self.config.token_endpoint:
            raise ValueError("token_endpoint 未配置")
        
        # 根据参数风格选择字段名（去除可能的空格）
        param_style = (self.config.token_param_style or "snake_case").strip()
        if param_style == "camel_case":
            # 钉钉等使用 camelCase 风格
            grant_type_key = "grantType"
            client_id_key = "clientId"
            client_secret_key = "clientSecret"
            redirect_uri_key = "redirectUri"
        else:
            # 标准 OAuth2 使用 snake_case 风格
            grant_type_key = "grant_type"
            client_id_key = "client_id"
            client_secret_key = "client_secret"
            redirect_uri_key = "redirect_uri"
        
        # 构建请求参数
        params = {
            grant_type_key: "authorization_code",
            "code": code,
            **self.config.token_extra_params,
        }
        
        # 只有非 camelCase 风格才添加 redirect_uri（钉钉不需要）
        if param_style != "camel_case":
            params[redirect_uri_key] = self.config.redirect_uri
        
        # 根据认证方式构建请求
        auth = None
        data = None
        url_params = None
        
        if self.config.token_auth_method == "basic_auth":
            auth = (self.config.client_id, self.config.client_secret)
            data = params
        elif self.config.token_auth_method == "query":
            params[client_id_key] = self.config.client_id
            params[client_secret_key] = self.config.client_secret
            url_params = params
            data = None
        else:  # body (默认)
            params[client_id_key] = self.config.client_id
            params[client_secret_key] = self.config.client_secret
            data = params
        
        logger.info("OAuth2 token exchange request", 
                    endpoint=self.config.token_endpoint,
                    content_type=self.config.token_content_type,
                    param_style=self.config.token_param_style,
                    data_keys=list(data.keys()) if data else None)
        
        async with httpx.AsyncClient(timeout=30) as client:
            if url_params:
                resp = await client.post(self.config.token_endpoint, params=url_params, auth=auth)
            elif self.config.token_content_type == "json":
                # 钉钉等 API 需要 JSON 格式请求体
                resp = await client.post(self.config.token_endpoint, json=data, auth=auth)
            else:
                # 默认使用表单编码
                resp = await client.post(self.config.token_endpoint, data=data, auth=auth)
            
            logger.info("OAuth2 token exchange response", status=resp.status_code, body=resp.text[:500] if resp.text else None)
            
            if resp.status_code != 200:
                logger.error("OAuth2 token exchange failed", status=resp.status_code, body=resp.text)
                raise ValueError(f"Token 交换失败: HTTP {resp.status_code}")
            
            token_data = resp.json()
        
        # 应用字段映射
        mapping = dict(self.config.token_field_mapping)
        
        # 如果使用 camelCase 风格，检查是否需要自动转换响应字段
        param_style = (self.config.token_param_style or "snake_case").strip()
        
        # 检查是否使用默认的 snake_case 映射（即没有自定义为 camelCase）
        is_default_mapping = (
            mapping.get("access_token") == "access_token" or
            mapping.get("access_token") is None
        )
        
        if param_style == "camel_case" and is_default_mapping:
            # 钉钉等使用 camelCase 响应字段，自动转换
            mapping = {
                "access_token": "accessToken",
                "refresh_token": "refreshToken",
                "expires_in": "expireIn",
                "token_type": "token_type",
            }
            logger.debug("OAuth2 使用 camelCase 响应字段映射", mapping=mapping)
        
        logger.debug("OAuth2 token 响应", token_data=token_data, mapping=mapping)
        
        result = {}
        for local_key, remote_key in mapping.items():
            if remote_key in token_data:
                result[local_key] = token_data[remote_key]
        
        # 保留原始数据
        result["_raw"] = token_data
        
        return result
    
    async def get_userinfo(self, access_token: str, token_response: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        获取用户信息。
        
        Args:
            access_token: 访问令牌
            token_response: Token 响应（可选，用于提取 unionId 等信息）
            
        Returns:
            用户信息（已经过字段映射）
        """
        if not self.config.userinfo_endpoint:
            raise ValueError("userinfo_endpoint 未配置")
        
        # 构建请求
        headers = {}
        params = {}
        data = {}
        
        # 添加额外参数
        extra_params = dict(self.config.userinfo_extra_params)
        
        if self.config.userinfo_auth_method == "header":
            # 检查是否使用自定义 header（如钉钉的 x-acs-dingtalk-access-token）
            custom_header = self.config.userinfo_token_header
            if custom_header:
                headers[custom_header] = access_token
                logger.info("OAuth2 userinfo 使用自定义 header", header_name=custom_header, token_preview=access_token[:10] + "...")
            else:
                headers["Authorization"] = f"Bearer {access_token}"
            params = extra_params
        elif self.config.userinfo_auth_method == "query":
            params = {self.config.userinfo_token_param: access_token, **extra_params}
        else:  # body
            data = {self.config.userinfo_token_param: access_token, **extra_params}
        
        raw_userinfo = None
        userinfo_error = None
        
        async with httpx.AsyncClient(timeout=30) as client:
            if self.config.userinfo_http_method == "POST":
                resp = await client.post(
                    self.config.userinfo_endpoint,
                    headers=headers,
                    params=params if self.config.userinfo_auth_method != "query" else None,
                    data=data if data else None,
                )
            else:  # GET
                resp = await client.get(
                    self.config.userinfo_endpoint,
                    headers=headers,
                    params=params,
                )
            
            if resp.status_code != 200:
                userinfo_error = f"HTTP {resp.status_code}: {resp.text}"
                logger.warning("OAuth2 userinfo 主接口失败，尝试回退方案", status=resp.status_code, body=resp.text[:200])
                
                # 钉钉回退方案：使用企业内部 API
                if self._is_dingtalk_provider():
                    raw_userinfo = await self._dingtalk_fallback_userinfo(token_response)
                    if raw_userinfo:
                        logger.info("OAuth2 钉钉回退方案成功")
                
                if not raw_userinfo:
                    logger.error("OAuth2 userinfo failed", status=resp.status_code, body=resp.text)
                    raise ValueError(f"获取用户信息失败: {userinfo_error}")
            else:
                raw_userinfo = resp.json()
        
        # 从嵌套路径提取数据
        userinfo = self._extract_nested_value(raw_userinfo, self.config.userinfo_data_path)
        if userinfo is None:
            userinfo = raw_userinfo
        
        # 应用字段映射
        mapping = self.config.userinfo_field_mapping
        result = {}
        for local_key, remote_key in mapping.items():
            # 支持嵌套路径
            value = self._extract_nested_value(userinfo, remote_key)
            if value is not None:
                result[local_key] = value
        
        # 保留原始数据
        result["_raw"] = raw_userinfo
        
        return result
    
    def _is_dingtalk_provider(self) -> bool:
        """判断是否是钉钉提供者"""
        return (
            "dingtalk" in self.config.name.lower() or
            "钉钉" in self.config.name or
            "dingtalk.com" in (self.config.token_endpoint or "")
        )
    
    async def _dingtalk_fallback_userinfo(self, token_response: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        钉钉回退方案：使用企业内部 API 获取用户信息。
        
        当个人授权 API (/v1.0/contact/users/me) 权限不足时，
        通过企业内部应用的 access_token 和 unionId 获取用户信息。
        """
        try:
            # 从 token 响应中获取 unionId
            union_id = None
            if token_response:
                raw = token_response.get("_raw", {})
                union_id = raw.get("unionId") or raw.get("openId")
            
            if not union_id:
                logger.warning("钉钉回退方案：无法获取 unionId")
                return None
            
            logger.info("钉钉回退方案：尝试使用企业 API", union_id=union_id[:10] + "...")
            
            # 获取企业内部应用的 access_token
            enterprise_token = await self._get_dingtalk_enterprise_token()
            if not enterprise_token:
                logger.warning("钉钉回退方案：无法获取企业 access_token")
                return None
            
            # 通过 unionId 获取 userId
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    "https://oapi.dingtalk.com/user/getUseridByUnionid",
                    params={"access_token": enterprise_token, "unionid": union_id}
                )
                if resp.status_code != 200:
                    logger.warning("钉钉回退方案：获取 userId 失败", status=resp.status_code)
                    return None
                
                result = resp.json()
                if result.get("errcode") != 0:
                    logger.warning("钉钉回退方案：获取 userId 失败", error=result.get("errmsg"))
                    return None
                
                user_id = result.get("userid")
                if not user_id:
                    return None
                
                # 通过 userId 获取用户详细信息
                resp = await client.get(
                    "https://oapi.dingtalk.com/user/get",
                    params={"access_token": enterprise_token, "userid": user_id}
                )
                if resp.status_code != 200:
                    return None
                
                user_detail = resp.json()
                if user_detail.get("errcode") != 0:
                    return None
                
                logger.info("钉钉回退方案：成功获取用户信息", name=user_detail.get("name"))
                
                # 转换为统一格式
                return {
                    "unionId": user_detail.get("unionid") or union_id,
                    "nick": user_detail.get("name"),
                    "name": user_detail.get("name"),
                    "mobile": user_detail.get("mobile"),
                    "email": user_detail.get("email"),
                    "avatarUrl": user_detail.get("avatar"),
                    "userId": user_id,
                    "department": user_detail.get("department", []),
                    "position": user_detail.get("position"),
                }
                
        except Exception as e:
            logger.warning("钉钉回退方案异常", error=str(e))
            return None
    
    async def _get_dingtalk_enterprise_token(self) -> Optional[str]:
        """获取钉钉企业内部应用的 access_token"""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    "https://oapi.dingtalk.com/gettoken",
                    params={
                        "appkey": self.config.client_id,
                        "appsecret": self.config.client_secret
                    }
                )
                if resp.status_code != 200:
                    return None
                
                result = resp.json()
                if result.get("errcode") != 0:
                    logger.warning("获取钉钉企业 token 失败", error=result.get("errmsg"))
                    return None
                
                return result.get("access_token")
        except Exception as e:
            logger.warning("获取钉钉企业 token 异常", error=str(e))
            return None
    
    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        """
        认证请求。
        
        注意：OAuth 2.0 的 access_token 不是自包含的，无法直接验证。
        此方法主要用于透传场景，实际的 OAuth 流程应通过 callback 接口完成。
        
        对于 Bearer token 认证，需要调用 userinfo 接口验证。
        """
        if not self.enabled:
            return None
        
        if token is None:
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return None
            token = auth_header[7:]
        
        try:
            # 调用 userinfo 接口验证 token 并获取用户信息
            userinfo = await self.get_userinfo(token)
            
            user_id = userinfo.get("user_id")
            if not user_id:
                logger.warning("OAuth2 userinfo missing user_id")
                return None
            
            # 提取角色
            data_roles = self._extract_data_roles(userinfo)
            role = self.map_role(data_roles)
            
            # 提取用户属性
            user_attributes = self._extract_user_attributes(userinfo)
            
            return AuthContext(
                user_id=str(user_id),
                username=userinfo.get("username") or userinfo.get("name") or userinfo.get("email"),
                role=role,
                source=self.name,
                attributes={
                    "email": userinfo.get("email"),
                    "name": userinfo.get("name"),
                    "phone": userinfo.get("phone"),
                    "avatar": userinfo.get("avatar"),
                },
                extra={"userinfo": userinfo},
                data_roles=data_roles,
                user_attributes=user_attributes,
            )
        except AuthenticationError:
            # 已经是认证错误，直接抛出
            raise
        except Exception as exc:
            logger.warning("OAuth2 authenticate failed", error=str(exc))
            raise AuthenticationError(AuthErrorCode.OIDC_FAILED, detail=str(exc), provider=self.name)
    
    def get_authorization_url(self, state: str, extra_params: Optional[Dict[str, str]] = None) -> str:
        """
        构建授权 URL。
        
        Args:
            state: CSRF 防护 state 参数
            extra_params: 额外的 URL 参数
            
        Returns:
            完整的授权 URL
        """
        if not self.config.authorization_endpoint:
            raise ValueError("authorization_endpoint 未配置")
        
        from urllib.parse import urlencode
        
        params = {
            "response_type": self.config.response_type,
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "state": state,
        }
        
        if self.config.scope:
            params["scope"] = self.config.scope
        
        if extra_params:
            params.update(extra_params)
        
        return f"{self.config.authorization_endpoint}?{urlencode(params)}"
    
    async def list_users(self, max_results: int = 1000) -> Dict[str, Any]:
        """
        从用户列表 API 获取用户。
        
        Returns:
            包含用户列表和状态的字典
        """
        if not self.config.users_api_url:
            return {
                "success": False,
                "message": "未配置用户列表接口地址 (users_api_url)",
                "users": []
            }
        
        try:
            # 获取 API token
            token = await self._get_api_token()
            if not token:
                return {
                    "success": False,
                    "message": "无法获取 API 访问令牌",
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
            
            raw_users = self._extract_nested_value(data, list_path)
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
                full_name = u.get(mapping.get("name_field", "name"))
                phone = u.get(mapping.get("phone_field", "phone"))
                department = u.get(mapping.get("department_field", "department"))
                
                enabled_field = mapping.get("enabled_field", "enabled")
                enabled = u.get(enabled_field, True)
                if isinstance(enabled, str):
                    enabled = enabled.lower() in ("true", "1", "yes", "active")
                
                roles_field = mapping.get("roles_field", "roles")
                roles = u.get(roles_field, [])
                if isinstance(roles, str):
                    roles = [roles]
                if roles is None:
                    roles = []
                role = self.map_role(roles)
                
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
                    "data_roles": roles,
                    "user_attributes": user_attrs,
                    "profile": {"source": self.name, "raw_data": u, "phone": phone, "department": department}
                })
            
            logger.info("OAuth2 用户列表获取成功", count=len(users), provider=self.name)
            return {
                "success": True,
                "message": f"成功获取 {len(users)} 个用户",
                "users": users
            }
            
        except httpx.HTTPStatusError as e:
            error_msg = f"用户列表 API 请求失败: HTTP {e.response.status_code}"
            logger.error(error_msg, status_code=e.response.status_code)
            return {"success": False, "message": error_msg, "users": []}
        except Exception as e:
            logger.error("获取 OAuth2 用户列表失败", error=str(e))
            return {"success": False, "message": f"获取用户列表失败: {str(e)}", "users": []}
    
    async def _get_api_token(self) -> Optional[str]:
        """获取用户列表 API 的访问令牌"""
        token_url = self.config.users_api_token_url or self.config.token_endpoint
        
        if not token_url:
            return None
        
        try:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                **self.config.users_api_extra_params,
            }
            
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(token_url, data=data)
                resp.raise_for_status()
                result = resp.json()
                
                # 应用字段映射
                token_field = self.config.token_field_mapping.get("access_token", "access_token")
                return result.get(token_field)
        except Exception as e:
            logger.warning("获取 API 令牌失败", error=str(e))
            return None


# ============================================================================
# 预置配置工厂
# ============================================================================

def create_dingtalk_config(
    app_key: str,
    app_secret: str,
    redirect_uri: str,
    corp_id: Optional[str] = None,
    **kwargs
) -> OAuth2ProviderConfig:
    """
    创建钉钉 OAuth 2.0 配置。
    
    Args:
        app_key: 钉钉应用的 AppKey
        app_secret: 钉钉应用的 AppSecret
        redirect_uri: 回调地址
        corp_id: 企业 ID（可选，用于企业内部应用）
        **kwargs: 其他配置参数
        
    Returns:
        配置好的 OAuth2ProviderConfig
    """
    return OAuth2ProviderConfig(
        name=kwargs.get("name", "dingtalk"),
        client_id=app_key,
        client_secret=app_secret,
        redirect_uri=redirect_uri,
        
        # 钉钉端点
        authorization_endpoint="https://login.dingtalk.com/oauth2/auth",
        token_endpoint="https://api.dingtalk.com/v1.0/oauth2/userAccessToken",
        userinfo_endpoint="https://api.dingtalk.com/v1.0/contact/users/me",
        
        # 钉钉特定配置
        scope="openid",
        response_type="code",
        token_auth_method="body",
        token_content_type="json",  # 钉钉 v1.0 API 需要 JSON 格式请求体
        token_param_style="camel_case",  # 钉钉使用 camelCase 字段名
        
        # 钉钉 Token 响应字段映射（钉钉返回 camelCase 格式）
        token_field_mapping={
            "access_token": "accessToken",
            "refresh_token": "refreshToken",
            "expires_in": "expireIn",
            "token_type": "token_type",
        },
        
        userinfo_auth_method="header",
        userinfo_http_method="GET",
        userinfo_token_header="x-acs-dingtalk-access-token",  # 钉钉特殊 header
        
        # 钉钉用户信息字段映射
        userinfo_field_mapping={
            "user_id": "unionId",  # 或 openId
            "username": "nick",
            "email": "email",
            "name": "nick",
            "phone": "mobile",
            "avatar": "avatarUrl",
        },
        
        role_mapping=kwargs.get("role_mapping", {"*": "user"}),
        frontend_redirect_url=kwargs.get("frontend_redirect_url"),
        
        # 用户同步配置
        users_api_url=kwargs.get("users_api_url"),
        users_field_mapping=kwargs.get("users_field_mapping", {
            "list_path": "result.list",
            "id_field": "userid",
            "username_field": "name",
            "email_field": "email",
            "name_field": "name",
            "phone_field": "mobile",
            "department_field": "dept_id_list",
        }),
    )


def create_wecom_config(
    corp_id: str,
    agent_id: str,
    secret: str,
    redirect_uri: str,
    **kwargs
) -> OAuth2ProviderConfig:
    """
    创建企业微信 OAuth 2.0 配置。
    
    Args:
        corp_id: 企业 ID
        agent_id: 应用 AgentId
        secret: 应用 Secret
        redirect_uri: 回调地址
        **kwargs: 其他配置参数
        
    Returns:
        配置好的 OAuth2ProviderConfig
    """
    return OAuth2ProviderConfig(
        name=kwargs.get("name", "wecom"),
        client_id=corp_id,
        client_secret=secret,
        redirect_uri=redirect_uri,
        
        # 企业微信端点
        authorization_endpoint="https://open.weixin.qq.com/connect/oauth2/authorize",
        token_endpoint="https://qyapi.weixin.qq.com/cgi-bin/gettoken",
        userinfo_endpoint="https://qyapi.weixin.qq.com/cgi-bin/user/getuserinfo",
        
        # 企业微信特定配置
        scope="snsapi_base",
        response_type="code",
        
        # 企业微信 token 获取方式特殊：通过 query params
        token_auth_method="query",
        token_extra_params={"corpid": corp_id, "corpsecret": secret},
        
        # 企业微信 userinfo 通过 query params 传递 token
        userinfo_auth_method="query",
        userinfo_token_param="access_token",
        userinfo_http_method="GET",
        userinfo_extra_params={"code": ""},  # code 需要在运行时填充
        
        # 企业微信用户信息字段映射
        userinfo_field_mapping={
            "user_id": "UserId",
            "username": "UserId",
            "name": "UserId",
            "department": "department",
        },
        
        role_mapping=kwargs.get("role_mapping", {"*": "user"}),
        frontend_redirect_url=kwargs.get("frontend_redirect_url"),
    )


def create_feishu_config(
    app_id: str,
    app_secret: str,
    redirect_uri: str,
    **kwargs
) -> OAuth2ProviderConfig:
    """
    创建飞书 OAuth 2.0 配置。
    
    Args:
        app_id: 飞书应用的 App ID
        app_secret: 飞书应用的 App Secret
        redirect_uri: 回调地址
        **kwargs: 其他配置参数
        
    Returns:
        配置好的 OAuth2ProviderConfig
    """
    return OAuth2ProviderConfig(
        name=kwargs.get("name", "feishu"),
        client_id=app_id,
        client_secret=app_secret,
        redirect_uri=redirect_uri,
        
        # 飞书端点
        authorization_endpoint="https://open.feishu.cn/open-apis/authen/v1/authorize",
        token_endpoint="https://open.feishu.cn/open-apis/authen/v1/oidc/access_token",
        userinfo_endpoint="https://open.feishu.cn/open-apis/authen/v1/user_info",
        
        # 飞书特定配置
        scope="",
        response_type="code",
        token_auth_method="body",
        token_extra_params={"grant_type": "authorization_code"},
        
        userinfo_auth_method="header",
        userinfo_http_method="GET",
        userinfo_data_path="data",  # 飞书用户信息在 data 字段下
        
        # 飞书用户信息字段映射
        userinfo_field_mapping={
            "user_id": "union_id",  # 或 open_id
            "username": "name",
            "email": "email",
            "name": "name",
            "phone": "mobile",
            "avatar": "avatar_url",
            "department": "department_ids",
        },
        
        role_mapping=kwargs.get("role_mapping", {"*": "user"}),
        frontend_redirect_url=kwargs.get("frontend_redirect_url"),
    )

