"""
API 网关认证提供者：从网关透传的 Header 读取用户信息，并校验签名/IP。

支持的 Header：
- X-User-Id: 用户唯一标识
- X-User-Name: 用户名
- X-User-Roles: 数据角色列表（JSON 数组）
- X-User-Role: 单个角色（兼容旧格式）
- X-User-Email: 邮箱
- X-User-FullName: 显示名称
- X-User-Department: 部门
- X-User-Region: 区域（JSON 数组或字符串）
- X-User-Signature: HMAC 签名
"""

from __future__ import annotations

import hmac
import hashlib
import json
from typing import Any, Dict, List, Optional
from fastapi import Request
import httpx
import structlog

from server.auth.base import AuthProvider, AuthContext
from server.config import settings

logger = structlog.get_logger()


class APIGatewayProvider(AuthProvider):
    def __init__(
        self, 
        priority: int = 90, 
        enabled: bool = True,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name="api_gateway", priority=priority, enabled=enabled)
        config = config or {}
        self.secret = config.get("signature_secret") or settings.gateway_signature_secret or ""
        trusted_ips_str = config.get("trusted_ips") or settings.trusted_gateway_ips or ""
        self.trusted_ips = [ip.strip() for ip in trusted_ips_str.split(",") if ip.strip()]
        self.role_mapping = config.get("role_mapping") or {"admin": "admin", "data_admin": "data_admin", "*": "user"}
        
        # 数据角色同步配置
        self.auto_create_data_role = config.get("auto_create_data_role", settings.api_gateway_auto_create_role)
        # 需要同步的用户属性 Header（映射：Header名 -> 属性名）
        self.user_attribute_headers = config.get("user_attribute_headers", {
            "X-User-Department": "department",
            "X-User-Region": "region",
        })
        
        # 用户列表接口配置
        self.users_api_url = config.get("users_api_url", "")
        self.users_api_token = config.get("users_api_token", "")
        self.users_api_method = config.get("users_api_method", "GET")
        self.users_api_headers = config.get("users_api_headers", {})
        # 响应字段映射
        self.user_field_mapping = config.get("user_field_mapping", {
            "list_path": "users",  # 用户列表在响应中的路径
            "id_field": "id",
            "username_field": "username",
            "email_field": "email",
            "name_field": "name",
            "roles_field": "roles",  # 角色列表字段
            "role_field": "role",    # 单角色字段（兼容）
            "active_field": "is_active",
        })

    def _map_system_role(self, data_roles: List[str]) -> str:
        """从数据角色列表映射系统角色"""
        if not data_roles:
            return self.role_mapping.get("*", "user")
        for r in data_roles:
            mapped = self.role_mapping.get(r)
            if mapped:
                return mapped
        return self.role_mapping.get("*", "user")

    def _check_ip(self, request: Request) -> bool:
        if not self.trusted_ips:
            return True
        client_ip = request.client.host if request.client else ""
        # 同时检查 X-Forwarded-For
        forwarded_for = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        return client_ip in self.trusted_ips or forwarded_for in self.trusted_ips

    def _verify_signature(self, user_id: str, username: str, roles_json: str, signature: str) -> bool:
        """
        验证签名
        
        签名算法：HMAC-SHA256(user_id:username:roles_json)
        其中 roles_json 是角色列表的 JSON 字符串
        """
        if not self.secret:
            # 未配置密钥时跳过签名验证（仅依赖 IP 白名单）
            logger.warning("API 网关未配置签名密钥，跳过签名验证")
            return True
        if not signature:
            return False
        msg = f"{user_id}:{username}:{roles_json}".encode()
        mac = hmac.new(self.secret.encode(), msg, hashlib.sha256).hexdigest()
        return hmac.compare_digest(mac, signature)

    def _parse_roles_header(self, request: Request) -> List[str]:
        """解析角色 Header，支持 JSON 数组和单值"""
        # 优先使用 X-User-Roles（JSON 数组）
        roles_header = request.headers.get("X-User-Roles", "")
        if roles_header:
            try:
                roles = json.loads(roles_header)
                if isinstance(roles, list):
                    return [str(r) for r in roles if r]
                elif isinstance(roles, str):
                    return [roles]
            except json.JSONDecodeError:
                # 非 JSON，当作逗号分隔字符串
                return [r.strip() for r in roles_header.split(",") if r.strip()]
        
        # 回退到 X-User-Role（单值）
        role = request.headers.get("X-User-Role", "")
        if role:
            return [role]
        
        return []

    def _parse_user_attributes(self, request: Request) -> Dict[str, Any]:
        """从 Header 解析用户属性"""
        attributes = {}
        for header_name, attr_name in self.user_attribute_headers.items():
            value = request.headers.get(header_name)
            if value:
                # 尝试解析 JSON（支持数组）
                try:
                    parsed = json.loads(value)
                    attributes[attr_name] = parsed
                except json.JSONDecodeError:
                    attributes[attr_name] = value
        return attributes

    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        if not self.enabled:
            return None

        if not self._check_ip(request):
            logger.warning("gateway ip not trusted", client_ip=request.client.host if request.client else "unknown")
            return None

        user_id = request.headers.get("X-User-Id")
        username = request.headers.get("X-User-Name") or request.headers.get("X-User-Username")
        signature = request.headers.get("X-User-Signature", "")

        if not user_id or not username:
            return None

        # 解析数据角色列表
        data_roles = self._parse_roles_header(request)
        
        # 用于签名验证的 roles JSON
        roles_json = request.headers.get("X-User-Roles", json.dumps(data_roles, ensure_ascii=False))
        
        # 验证签名
        if not self._verify_signature(user_id, username, roles_json, signature):
            logger.warning("gateway signature invalid", user_id=user_id)
            return None

        # 映射系统角色
        system_role = self._map_system_role(data_roles)
        
        # 解析用户属性
        user_attributes = self._parse_user_attributes(request)
        
        # 基础属性
        email = request.headers.get("X-User-Email")
        full_name = request.headers.get("X-User-FullName")

        return AuthContext(
            user_id=user_id,
            username=username,
            role=system_role,
            source=self.name,
            attributes={"email": email, "name": full_name},
            extra={"raw_headers": dict(request.headers)},
            # 新增：数据角色和用户属性
            data_roles=data_roles,
            user_attributes=user_attributes,
        )

    async def list_users(self, max_results: int = 1000) -> Dict[str, Any]:
        """
        从上游系统 API 获取用户列表。
        
        需要在配置中设置:
        - users_api_url: 用户列表接口地址
        - users_api_token: 接口认证令牌（可选）
        - users_api_headers: 额外请求头（可选）
        - user_field_mapping: 字段映射配置
        
        Returns:
            包含用户列表和状态的字典
        """
        if not self.users_api_url:
            return {
                "success": False,
                "message": "未配置用户列表接口地址 (users_api_url)，无法获取用户列表",
                "users": []
            }
        
        try:
            # 构建请求头
            headers = dict(self.users_api_headers) if self.users_api_headers else {}
            if self.users_api_token:
                headers["Authorization"] = f"Bearer {self.users_api_token}"
            
            async with httpx.AsyncClient(timeout=60) as client:
                if self.users_api_method.upper() == "POST":
                    resp = await client.post(
                        self.users_api_url,
                        headers=headers,
                        json={"max": max_results},
                    )
                else:
                    resp = await client.get(
                        self.users_api_url,
                        headers=headers,
                        params={"max": max_results},
                    )
                resp.raise_for_status()
                data = resp.json()
            
            # 解析用户列表
            mapping = self.user_field_mapping
            list_path = mapping.get("list_path", "users")
            
            # 支持嵌套路径，如 "data.users"
            raw_users = data
            for key in list_path.split("."):
                if key and isinstance(raw_users, dict):
                    raw_users = raw_users.get(key, [])
            
            if not isinstance(raw_users, list):
                raw_users = []
            
            # 转换为统一格式
            users = []
            for u in raw_users:
                if not isinstance(u, dict):
                    continue
                    
                user_id = str(u.get(mapping.get("id_field", "id"), ""))
                username = str(u.get(mapping.get("username_field", "username"), ""))
                
                if not user_id or not username:
                    continue
                
                email = u.get(mapping.get("email_field", "email"))
                full_name = u.get(mapping.get("name_field", "name"))
                
                # 解析数据角色列表
                roles = u.get(mapping.get("roles_field", "roles"), [])
                if isinstance(roles, str):
                    roles = [roles]
                if roles is None:
                    # 回退到单角色字段
                    single_role = u.get(mapping.get("role_field", "role"))
                    roles = [single_role] if single_role else []
                
                # 映射系统角色
                system_role = self._map_system_role(roles)
                
                is_active = u.get(mapping.get("active_field", "is_active"), True)
                
                # 提取用户属性
                user_attrs = {}
                for header_name, attr_name in self.user_attribute_headers.items():
                    # 从原始数据中尝试获取对应字段
                    if attr_name in u and u[attr_name] is not None:
                        user_attrs[attr_name] = u[attr_name]
                
                users.append({
                    "external_uid": user_id,
                    "username": username,
                    "email": email,
                    "full_name": full_name,
                    "role": system_role if system_role in ("admin", "data_admin", "user") else "user",
                    "is_active": bool(is_active),
                    "data_roles": roles,  # 新增：数据角色列表
                    "user_attributes": user_attrs,  # 新增：用户属性
                    "profile": {"source": "api_gateway", "raw_data": u}
                })
            
            logger.info("API 网关用户列表获取成功", count=len(users))
            return {
                "success": True,
                "message": f"成功获取 {len(users)} 个用户",
                "users": users
            }
            
        except httpx.HTTPStatusError as e:
            error_msg = f"用户列表接口请求失败: HTTP {e.response.status_code}"
            logger.error(error_msg)
            return {"success": False, "message": error_msg, "users": []}
        except Exception as e:
            logger.error("获取 API 网关用户列表失败", error=str(e))
            return {"success": False, "message": f"获取用户列表失败: {str(e)}", "users": []}

