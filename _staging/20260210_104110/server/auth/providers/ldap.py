"""
LDAP 认证提供者：支持通过 LDAP/AD 进行用户认证。
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import Request
import structlog

from server.auth.base import AuthProvider, AuthContext

logger = structlog.get_logger()


class LDAPProvider(AuthProvider):
    """
    LDAP 认证提供者。
    
    支持两种认证方式：
    1. 直接绑定：使用用户提供的凭据直接绑定到 LDAP
    2. 搜索绑定：先用服务账号搜索用户，再用用户凭据绑定
    
    配置示例：
    {
        "server": "ldap://ldap.example.com:389",
        "base_dn": "dc=example,dc=com",
        "bind_dn": "cn=admin,dc=example,dc=com",
        "bind_password": "admin_password",
        "user_search_filter": "(uid={username})",
        "user_search_base": "ou=users,dc=example,dc=com",
        "group_search_filter": "(member={user_dn})",
        "group_search_base": "ou=groups,dc=example,dc=com",
        "role_mapping": {
            "cn=admins,ou=groups,dc=example,dc=com": "admin",
            "cn=managers,ou=groups,dc=example,dc=com": "data_admin",
            "*": "user"
        },
        "use_ssl": false,
        "start_tls": false,
        "connection_timeout": 10
    }
    """

    def __init__(
        self,
        config: Dict[str, Any],
        priority: int = 70,
        enabled: bool = True,
    ):
        super().__init__(name="ldap", priority=priority, enabled=enabled)
        self.config = config
        self.server = config.get("server", "")
        self.base_dn = config.get("base_dn", "")
        self.bind_dn = config.get("bind_dn", "")
        self.bind_password = config.get("bind_password", "")
        self.user_search_filter = config.get("user_search_filter", "(uid={username})")
        self.user_search_base = config.get("user_search_base", "")
        self.group_search_filter = config.get("group_search_filter", "(member={user_dn})")
        self.group_search_base = config.get("group_search_base", "")
        self.role_mapping = config.get("role_mapping", {"*": "user"})
        self.use_ssl = config.get("use_ssl", False)
        self.start_tls = config.get("start_tls", False)
        self.connection_timeout = config.get("connection_timeout", 10)
        
        # 属性映射
        self.attr_username = config.get("attr_username", "uid")
        self.attr_email = config.get("attr_email", "mail")
        self.attr_full_name = config.get("attr_full_name", "cn")
        self.attr_member_of = config.get("attr_member_of", "memberOf")
        self.username_fallback_prefix = config.get("username_fallback_prefix", "ldap_")
        
        self._ldap = None

    def _get_ldap_connection(self):
        """获取 LDAP 连接"""
        try:
            import ldap3
            from ldap3 import Server, Connection, ALL, SUBTREE, Tls
            import ssl
        except ImportError:
            raise ImportError("ldap3 库未安装，请运行: pip install ldap3")
        
        # 配置 TLS
        tls_config = None
        if self.use_ssl or self.start_tls:
            tls_config = Tls(validate=ssl.CERT_NONE)  # 生产环境应配置证书验证
        
        # 创建服务器连接
        server = Server(
            self.server,
            get_info=ALL,
            use_ssl=self.use_ssl,
            tls=tls_config,
            connect_timeout=self.connection_timeout,
        )
        
        return server, Connection, SUBTREE

    def _bind_as_service(self):
        """使用服务账号绑定"""
        server, Connection, _ = self._get_ldap_connection()
        
        conn = Connection(
            server,
            user=self.bind_dn,
            password=self.bind_password,
            auto_bind=True,
            raise_exceptions=True,
        )
        
        if self.start_tls and not self.use_ssl:
            conn.start_tls()
        
        return conn

    @staticmethod
    def _first_attr(entry, attr_name: str, default=None):
        """
        安全获取条目属性的首个值，避免空列表导致越界。
        """
        try:
            if not hasattr(entry, attr_name):
                return default
            values = getattr(entry, attr_name)
            # ldap3 属性可能是 list，也可能是单值
            if values is None:
                return default
            if isinstance(values, (list, tuple)):
                return values[0] if values else default
            return values
        except Exception:
            return default

    def _search_user(self, conn, username: str) -> Optional[Dict[str, Any]]:
        """搜索用户"""
        _, _, SUBTREE = self._get_ldap_connection()
        
        search_base = self.user_search_base or self.base_dn
        search_filter = self.user_search_filter.format(username=username)
        
        conn.search(
            search_base=search_base,
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=[
                self.attr_username,
                self.attr_email,
                self.attr_full_name,
                self.attr_member_of,
            ],
        )
        
        if not conn.entries:
            return None
        
        entry = conn.entries[0]
        member_of = getattr(entry, self.attr_member_of, [])
        groups = []
        if isinstance(member_of, (list, tuple)):
            groups = list(member_of)
        else:
            groups = [member_of] if member_of else []

        return {
            "dn": str(entry.entry_dn),
            "username": str(self._first_attr(entry, self.attr_username, username) or username),
            "email": str(self._first_attr(entry, self.attr_email, "")) or None,
            "full_name": str(self._first_attr(entry, self.attr_full_name, "")) or None,
            "groups": groups,
        }

    def _verify_password(self, user_dn: str, password: str) -> bool:
        """验证用户密码"""
        try:
            server, Connection, _ = self._get_ldap_connection()
            
            conn = Connection(
                server,
                user=user_dn,
                password=password,
                auto_bind=True,
                raise_exceptions=True,
            )
            
            if self.start_tls and not self.use_ssl:
                conn.start_tls()
            
            # 绑定成功即密码正确
            conn.unbind()
            return True
            
        except Exception as e:
            logger.debug("LDAP password verification failed", user_dn=user_dn, error=str(e))
            return False

    def _map_role(self, groups: list) -> str:
        """根据组成员关系映射角色"""
        for group in groups:
            group_lower = group.lower()
            for pattern, role in self.role_mapping.items():
                if pattern == "*":
                    continue
                if pattern.lower() in group_lower or group_lower.endswith(pattern.lower()):
                    return role
        
        return self.role_mapping.get("*", "user")

    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        """
        LDAP 认证。
        
        支持两种方式：
        1. 从请求体获取 username/password（用于登录）
        2. 从 Authorization Header 获取 Basic 认证信息
        """
        if not self.enabled:
            return None
        
        if not self.server or not self.base_dn:
            logger.warning("LDAP 配置不完整，跳过认证")
            return None
        
        username = None
        password = None
        
        # 尝试从 Basic Auth 获取凭据
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Basic "):
            try:
                import base64
                credentials = base64.b64decode(auth_header[6:]).decode("utf-8")
                username, password = credentials.split(":", 1)
            except Exception:
                pass
        
        # 如果没有 Basic Auth，尝试从请求体获取（用于登录表单）
        if not username or not password:
            try:
                body = await request.json()
                username = body.get("username")
                password = body.get("password")
            except Exception:
                pass
        
        if not username or not password:
            return None
        
        try:
            # 使用服务账号搜索用户
            conn = self._bind_as_service()
            user_info = self._search_user(conn, username)
            conn.unbind()
            
            if not user_info:
                logger.debug("LDAP user not found", username=username)
                return None
            
            # 用户名兜底
            if not user_info.get("username"):
                user_info["username"] = f"{self.username_fallback_prefix}{uuid4().hex[:8]}"
            
            # 验证用户密码
            if not self._verify_password(user_info["dn"], password):
                logger.debug("LDAP password incorrect", username=username)
                return None
            
            # 映射角色
            role = self._map_role(user_info.get("groups", []))
            
            return AuthContext(
                user_id=user_info["dn"],  # 使用 DN 作为用户 ID
                username=user_info["username"],
                role=role,
                source=self.name,
                attributes={
                    "email": user_info.get("email"),
                    "full_name": user_info.get("full_name"),
                },
                extra={
                    "ldap_dn": user_info["dn"],
                    "ldap_groups": user_info.get("groups", []),
                },
            )
            
        except ImportError as e:
            logger.error("LDAP 库未安装", error=str(e))
            return None
        except Exception as e:
            logger.warning("LDAP 认证失败", username=username, error=str(e))
            return None

    async def test_connection(self) -> Dict[str, Any]:
        """测试 LDAP 连接"""
        try:
            conn = self._bind_as_service()
            conn.unbind()
            return {"success": True, "message": "LDAP 连接成功"}
        except ImportError as e:
            return {"success": False, "message": f"ldap3 库未安装: {str(e)}"}
        except Exception as e:
            return {"success": False, "message": f"LDAP 连接失败: {str(e)}"}

    async def list_users(self, max_results: int = 1000) -> Dict[str, Any]:
        """
        从 LDAP 获取用户列表。
        
        Args:
            max_results: 最大返回用户数，默认 1000
            
        Returns:
            包含用户列表和状态的字典
        """
        if not self.server or not self.base_dn:
            return {"success": False, "message": "LDAP 配置不完整", "users": []}
        
        try:
            from ldap3 import SUBTREE
            
            conn = self._bind_as_service()
            
            # 搜索所有用户
            search_base = self.user_search_base or self.base_dn
            # 使用通用的用户对象过滤器
            search_filter = self.config.get("user_list_filter", "(objectClass=person)")
            
            conn.search(
                search_base=search_base,
                search_filter=search_filter,
                search_scope=SUBTREE,
                attributes=[
                    self.attr_username,
                    self.attr_email,
                    self.attr_full_name,
                    self.attr_member_of,
                ],
                size_limit=max_results,
            )
            
            users = []
            for entry in conn.entries:
                user_dn = str(entry.entry_dn)
                username = str(self._first_attr(entry, self.attr_username, "") or "")
                email = str(self._first_attr(entry, self.attr_email, "")) or None
                full_name = str(self._first_attr(entry, self.attr_full_name, "")) or None
                groups = list(getattr(entry, self.attr_member_of, [])) if hasattr(entry, self.attr_member_of) else []
                
                if not username:
                    continue
                
                # 映射角色
                role = self._map_role(groups)
                
                users.append({
                    "external_uid": user_dn,
                    "username": username,
                    "email": email if email and email != "[]" else None,
                    "full_name": full_name if full_name and full_name != "[]" else None,
                    "role": role,
                    "is_active": True,
                    "profile": {
                        "ldap_dn": user_dn,
                        "ldap_groups": groups,
                    }
                })
            
            conn.unbind()
            
            logger.info("LDAP 用户列表获取成功", count=len(users))
            return {
                "success": True,
                "message": f"成功获取 {len(users)} 个用户",
                "users": users
            }
            
        except ImportError as e:
            return {"success": False, "message": f"ldap3 库未安装: {str(e)}", "users": []}
        except Exception as e:
            logger.error("获取 LDAP 用户列表失败", error=str(e))
            return {"success": False, "message": f"获取用户列表失败: {str(e)}", "users": []}

