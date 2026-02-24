"""
外部 AES Token 认证提供者

通过 AES 加密的 Token 与外部系统对接认证。
Token 明文格式可配置，支持多种加密算法和模式。

典型应用场景：
- 与其他系统的单点登录对接
- 前端部署在另一个系统，后端 API 服务
- 简单的跨系统认证集成
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import re

from fastapi import Request
import structlog

from server.auth.base import AuthProvider, AuthContext, AuthenticationError, AuthErrorCode

logger = structlog.get_logger()


class ExternalAESProvider(AuthProvider):
    """
    外部 AES Token 认证提供者
    
    支持的加密算法：
    - AES-128-CBC, AES-192-CBC, AES-256-CBC
    - AES-128-GCM, AES-192-GCM, AES-256-GCM
    - AES-128-CTR, AES-192-CTR, AES-256-CTR
    - AES-128-CFB, AES-192-CFB, AES-256-CFB
    - AES-128-OFB, AES-192-OFB, AES-256-OFB
    - AES-128-ECB, AES-192-ECB, AES-256-ECB (不推荐)
    - SM4-CBC, SM4-ECB (国密算法)
    
    Token 格式：
    - simple: {role}|{timestamp}|{service_key}
    - with_user: {user_id}|{username}|{role}|{timestamp}|{service_key}
    - custom: 自定义模板
    """
    
    # 支持的加密算法
    SUPPORTED_ALGORITHMS = {
        # AES-CBC 模式
        "AES-128-CBC": {"key_size": 16, "mode": "CBC", "cipher": "AES"},
        "AES-192-CBC": {"key_size": 24, "mode": "CBC", "cipher": "AES"},
        "AES-256-CBC": {"key_size": 32, "mode": "CBC", "cipher": "AES"},
        # AES-GCM 模式（认证加密）
        "AES-128-GCM": {"key_size": 16, "mode": "GCM", "cipher": "AES"},
        "AES-192-GCM": {"key_size": 24, "mode": "GCM", "cipher": "AES"},
        "AES-256-GCM": {"key_size": 32, "mode": "GCM", "cipher": "AES"},
        # AES-CTR 模式
        "AES-128-CTR": {"key_size": 16, "mode": "CTR", "cipher": "AES"},
        "AES-192-CTR": {"key_size": 24, "mode": "CTR", "cipher": "AES"},
        "AES-256-CTR": {"key_size": 32, "mode": "CTR", "cipher": "AES"},
        # AES-CFB 模式
        "AES-128-CFB": {"key_size": 16, "mode": "CFB", "cipher": "AES"},
        "AES-192-CFB": {"key_size": 24, "mode": "CFB", "cipher": "AES"},
        "AES-256-CFB": {"key_size": 32, "mode": "CFB", "cipher": "AES"},
        # AES-OFB 模式
        "AES-128-OFB": {"key_size": 16, "mode": "OFB", "cipher": "AES"},
        "AES-192-OFB": {"key_size": 24, "mode": "OFB", "cipher": "AES"},
        "AES-256-OFB": {"key_size": 32, "mode": "OFB", "cipher": "AES"},
        # AES-ECB 模式（不推荐，无 IV）
        "AES-128-ECB": {"key_size": 16, "mode": "ECB", "cipher": "AES"},
        "AES-192-ECB": {"key_size": 24, "mode": "ECB", "cipher": "AES"},
        "AES-256-ECB": {"key_size": 32, "mode": "ECB", "cipher": "AES"},
        # 国密 SM4
        "SM4-CBC": {"key_size": 16, "mode": "CBC", "cipher": "SM4"},
        "SM4-ECB": {"key_size": 16, "mode": "ECB", "cipher": "SM4"},
        "SM4-CTR": {"key_size": 16, "mode": "CTR", "cipher": "SM4"},
        "SM4-CFB": {"key_size": 16, "mode": "CFB", "cipher": "SM4"},
        "SM4-OFB": {"key_size": 16, "mode": "OFB", "cipher": "SM4"},
        "SM4-GCM": {"key_size": 16, "mode": "GCM", "cipher": "SM4"},
    }
    
    # Token 格式模板
    TOKEN_FORMATS = {
        "simple": [("role", True), ("timestamp", True), ("service_key", True)],
        "with_user": [("user_id", True), ("username", True), ("role", True), ("timestamp", True), ("service_key", True)],
        "with_username": [("username", True), ("role", True), ("timestamp", True), ("service_key", True)],
        "role_only": [("role", True), ("timestamp", True), ("service_key", True)],
    }
    
    def __init__(
        self,
        config: Dict[str, Any],
        priority: int = 85,
        enabled: bool = True,
    ):
        super().__init__(name="external_aes", priority=priority, enabled=enabled)
        
        # Token 格式配置
        self.token_format = config.get("token_format", "simple")
        self.custom_format = config.get("custom_format", "")
        self.field_separator = config.get("field_separator", "|")
        self.timestamp_format = config.get("timestamp_format", "%Y-%m-%d %H:%M:%S")
        
        # 加密配置
        self.algorithm = config.get("algorithm", "AES-128-CBC")
        self.aes_key = config.get("aes_key", "")
        self.aes_iv = config.get("aes_iv", "")
        self.iv_mode = config.get("iv_mode", "fixed")  # fixed, prepend, append
        self.encoding = config.get("encoding", "base64")  # base64, base64url, hex
        self.padding = config.get("padding", "pkcs7")  # pkcs7, zero, none
        
        # 验证配置
        service_keys = config.get("service_keys", "")
        if isinstance(service_keys, str):
            # 支持多行或逗号分隔
            self.service_keys = [k.strip() for k in re.split(r'[\n,]', service_keys) if k.strip()]
        elif isinstance(service_keys, list):
            self.service_keys = [str(k).strip() for k in service_keys if k]
        else:
            self.service_keys = []
        
        self.validity_minutes = int(config.get("validity_minutes", 5))
        self.allow_clock_skew = config.get("allow_clock_skew", True)
        self.clock_skew_seconds = int(config.get("clock_skew_seconds", 30))
        
        # 角色配置
        self.role_field_is_data_role = config.get("role_field_is_data_role", True)
        self.role_mapping = config.get("role_mapping", {"*": "user"})
        self.default_system_role = config.get("default_system_role", "user")
        self.strict_role_validation = config.get("strict_role_validation", True)  # 默认严格模式
        # 自动创建数据角色：仅在非严格模式下生效
        self.auto_create_data_role = config.get("auto_create_data_role", False) if not self.strict_role_validation else False
        
        # 前端对接配置
        self.token_location = config.get("token_location", "both")  # header, query, both
        self.query_param_name = config.get("query_param_name", "token")
        self.header_name = config.get("header_name", "")  # 空则使用 Authorization
        self.frontend_redirect_url = config.get("frontend_redirect_url", "")
        
        # 用户创建配置
        self.auto_create_user = config.get("auto_create_user", True)
        self.user_id_field = config.get("user_id_field", "role")
        self.username_field = config.get("username_field", "username")
        
        # 验证配置
        self._validate_config()
    
    def _validate_config(self):
        """验证配置有效性"""
        if self.algorithm not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(f"不支持的加密算法: {self.algorithm}")
        
        algo_info = self.SUPPORTED_ALGORITHMS[self.algorithm]
        key_size = algo_info["key_size"]
        
        if not self.aes_key:
            raise ValueError("未配置加密密钥 (aes_key)")
        
        # 检查密钥长度
        key_bytes = self.aes_key.encode('utf-8')
        if len(key_bytes) != key_size:
            logger.warning(
                f"密钥长度不匹配: 期望 {key_size} 字节, 实际 {len(key_bytes)} 字节, 将自动调整",
                algorithm=self.algorithm
            )
        
        # 检查 IV（非 ECB 模式需要）
        mode = algo_info["mode"]
        if mode != "ECB" and self.iv_mode == "fixed":
            if not self.aes_iv:
                raise ValueError(f"{mode} 模式需要配置初始化向量 (aes_iv)")
            iv_bytes = self.aes_iv.encode('utf-8')
            if len(iv_bytes) != 16:
                logger.warning(
                    f"IV 长度不匹配: 期望 16 字节, 实际 {len(iv_bytes)} 字节, 将自动调整"
                )
        
        if not self.service_keys:
            raise ValueError("未配置 ServiceKey 列表")
    
    def _get_cipher(self, iv: Optional[bytes] = None):
        """获取加密器实例"""
        try:
            from Crypto.Cipher import AES
            from Crypto.Util.Padding import unpad
        except ImportError:
            try:
                from Cryptodome.Cipher import AES
                from Cryptodome.Util.Padding import unpad
            except ImportError:
                raise ImportError("需要安装 pycryptodome: pip install pycryptodome")
        
        algo_info = self.SUPPORTED_ALGORITHMS[self.algorithm]
        mode_name = algo_info["mode"]
        cipher_name = algo_info["cipher"]
        key_size = algo_info["key_size"]
        
        # 调整密钥长度
        key = self._adjust_key_length(self.aes_key.encode('utf-8'), key_size)
        
        # 获取加密模式
        if cipher_name == "AES":
            cipher_module = AES
        elif cipher_name == "SM4":
            try:
                from Crypto.Cipher import SM4
                cipher_module = SM4
            except ImportError:
                raise ImportError("SM4 算法需要 pycryptodome >= 3.15")
        else:
            raise ValueError(f"不支持的加密算法: {cipher_name}")
        
        # 创建加密器
        mode_map = {
            "CBC": cipher_module.MODE_CBC,
            "GCM": cipher_module.MODE_GCM,
            "CTR": cipher_module.MODE_CTR,
            "CFB": cipher_module.MODE_CFB,
            "OFB": cipher_module.MODE_OFB,
            "ECB": cipher_module.MODE_ECB,
        }
        
        mode = mode_map.get(mode_name)
        if mode is None:
            raise ValueError(f"不支持的加密模式: {mode_name}")
        
        if mode_name == "ECB":
            return cipher_module.new(key, mode), mode_name
        elif mode_name == "GCM":
            # GCM 模式使用 nonce
            if iv is None:
                iv = self._get_iv()
            return cipher_module.new(key, mode, nonce=iv), mode_name
        elif mode_name == "CTR":
            # CTR 模式使用 nonce
            if iv is None:
                iv = self._get_iv()
            return cipher_module.new(key, mode, nonce=iv[:8]), mode_name
        else:
            if iv is None:
                iv = self._get_iv()
            return cipher_module.new(key, mode, iv), mode_name
    
    def _adjust_key_length(self, key: bytes, target_size: int) -> bytes:
        """调整密钥长度到目标大小"""
        if len(key) == target_size:
            return key
        elif len(key) > target_size:
            return key[:target_size]
        else:
            # 使用 SHA256 哈希扩展
            expanded = hashlib.sha256(key).digest()
            return expanded[:target_size]
    
    def _get_iv(self) -> bytes:
        """获取初始化向量"""
        iv = self.aes_iv.encode('utf-8')
        if len(iv) < 16:
            iv = iv + b'\x00' * (16 - len(iv))
        elif len(iv) > 16:
            iv = iv[:16]
        return iv
    
    def _decode_token(self, token: str) -> Optional[bytes]:
        """解码 Token"""
        try:
            if self.encoding == "base64":
                # 标准 Base64，添加填充
                padding = 4 - len(token) % 4
                if padding != 4:
                    token += '=' * padding
                return base64.b64decode(token)
            elif self.encoding == "base64url":
                # URL 安全的 Base64
                padding = 4 - len(token) % 4
                if padding != 4:
                    token += '=' * padding
                return base64.urlsafe_b64decode(token)
            elif self.encoding == "hex":
                return bytes.fromhex(token)
            else:
                return base64.b64decode(token)
        except Exception as e:
            logger.debug(f"Token 解码失败: {e}")
            return None
    
    def _decrypt_token(self, encrypted_data: bytes) -> Optional[str]:
        """解密 Token"""
        try:
            algo_info = self.SUPPORTED_ALGORITHMS[self.algorithm]
            mode_name = algo_info["mode"]
            
            # 处理 IV
            iv = None
            data = encrypted_data
            
            if mode_name != "ECB":
                if self.iv_mode == "prepend":
                    # IV 在密文前面
                    if mode_name == "GCM":
                        iv = data[:12]  # GCM 使用 12 字节 nonce
                        data = data[12:]
                    elif mode_name == "CTR":
                        iv = data[:8]  # CTR 使用 8 字节 nonce
                        data = data[8:]
                    else:
                        iv = data[:16]
                        data = data[16:]
                elif self.iv_mode == "append":
                    # IV 在密文后面
                    if mode_name == "GCM":
                        iv = data[-12:]
                        data = data[:-12]
                    elif mode_name == "CTR":
                        iv = data[-8:]
                        data = data[:-8]
                    else:
                        iv = data[-16:]
                        data = data[:-16]
                else:
                    # 固定 IV
                    iv = self._get_iv()
                    if mode_name == "GCM":
                        iv = iv[:12]
                    elif mode_name == "CTR":
                        iv = iv[:8]
            
            cipher, mode = self._get_cipher(iv)
            
            if mode == "GCM":
                # GCM 模式需要处理认证标签
                # 假设标签在密文最后 16 字节
                if len(data) < 16:
                    return None
                ciphertext = data[:-16]
                tag = data[-16:]
                decrypted = cipher.decrypt_and_verify(ciphertext, tag)
            else:
                decrypted = cipher.decrypt(data)
            
            # 去除填充
            if self.padding == "pkcs7" and mode not in ("CTR", "CFB", "OFB", "GCM"):
                try:
                    from Crypto.Util.Padding import unpad
                except ImportError:
                    from Cryptodome.Util.Padding import unpad
                decrypted = unpad(decrypted, 16)
            elif self.padding == "zero":
                decrypted = decrypted.rstrip(b'\x00')
            
            return decrypted.decode('utf-8')
            
        except Exception as e:
            logger.debug(f"Token 解密失败: {e}")
            return None
    
    def _parse_token_fields(self, plaintext: str) -> Optional[Dict[str, str]]:
        """解析 Token 字段"""
        parts = plaintext.split(self.field_separator)
        
        if self.token_format == "custom" and self.custom_format:
            # 解析自定义格式
            return self._parse_custom_format(plaintext)
        
        format_fields = self.TOKEN_FORMATS.get(self.token_format)
        if not format_fields:
            format_fields = self.TOKEN_FORMATS["simple"]
        
        if len(parts) != len(format_fields):
            logger.debug(f"Token 字段数量不匹配: 期望 {len(format_fields)}, 实际 {len(parts)}")
            return None
        
        result = {}
        for i, (field_name, required) in enumerate(format_fields):
            value = parts[i].strip()
            if required and not value:
                logger.debug(f"必填字段 {field_name} 为空")
                return None
            result[field_name] = value
        
        return result
    
    def _parse_custom_format(self, plaintext: str) -> Optional[Dict[str, str]]:
        """解析自定义格式"""
        # 从模板提取字段名
        field_pattern = re.compile(r'\{(\w+)\}')
        field_names = field_pattern.findall(self.custom_format)
        
        if not field_names:
            return None
        
        # 构建正则表达式匹配明文
        regex_pattern = self.custom_format
        for field in field_names:
            regex_pattern = regex_pattern.replace(f'{{{field}}}', f'(?P<{field}>.+?)')
        
        # 转义分隔符
        regex_pattern = regex_pattern.replace('|', r'\|')
        
        try:
            match = re.match(f'^{regex_pattern}$', plaintext)
            if match:
                return match.groupdict()
        except Exception as e:
            logger.debug(f"自定义格式解析失败: {e}")
        
        return None
    
    def _verify_service_key(self, received_key: str) -> bool:
        """验证 ServiceKey"""
        return received_key in self.service_keys
    
    def _verify_timestamp(self, timestamp_str: str) -> bool:
        """验证时间戳"""
        try:
            token_time = datetime.strptime(timestamp_str, self.timestamp_format)
            now = datetime.now()
            
            # 计算时间差
            diff = abs((now - token_time).total_seconds())
            
            # 允许的最大时间差（分钟转秒）
            max_diff = self.validity_minutes * 60
            
            # 如果允许时钟偏差，增加容差
            if self.allow_clock_skew:
                max_diff += self.clock_skew_seconds
            
            if diff > max_diff:
                logger.debug(f"Token 已过期: 时间差 {diff} 秒, 允许 {max_diff} 秒")
                return False
            
            return True
            
        except ValueError as e:
            logger.debug(f"时间戳解析失败: {e}")
            return False
    
    def _map_system_role(self, external_role: str) -> str:
        """映射系统角色"""
        if not self.role_mapping:
            return self.default_system_role
        
        # 精确匹配
        if external_role in self.role_mapping:
            return self.role_mapping[external_role]
        
        # 通配符匹配
        if "*" in self.role_mapping:
            return self.role_mapping["*"]
        
        return self.default_system_role
    
    def _get_token_from_request(self, request: Request) -> Optional[str]:
        """从请求中获取 Token"""
        token = None
        
        # 从 Header 获取
        if self.token_location in ("header", "both"):
            if self.header_name:
                # 自定义 Header
                token = request.headers.get(self.header_name)
            else:
                # 标准 Authorization Header
                auth_header = request.headers.get("Authorization")
                if auth_header and auth_header.startswith("Bearer "):
                    token = auth_header[7:]
        
        # 从 URL 参数获取
        if not token and self.token_location in ("query", "both"):
            token = request.query_params.get(self.query_param_name)
        
        return token
    
    async def _verify_data_role_exists(self, role_name: str) -> bool:
        """
        验证数据角色是否存在于数据库中（根据角色名称验证）
        
        Args:
            role_name: 角色名称（如"保利中心"）
        
        Returns:
            True 如果角色存在，False 否则
        """
        try:
            from server.utils.db_pool import get_metadata_pool
            
            pool = await get_metadata_pool()
            async with pool.acquire() as conn:
                # 根据角色名称（role_name）验证，同时也兼容角色编码（role_code）
                row = await conn.fetchrow(
                    """
                    SELECT role_id FROM data_roles 
                    WHERE (role_name = $1 OR role_code = $1) AND is_active = TRUE
                    """,
                    role_name
                )
                return row is not None
        except Exception as e:
            logger.error(f"验证数据角色时出错: {e}", role_name=role_name)
            # 出错时根据配置决定是否允许通过
            # 严格模式下，出错也拒绝
            return False
    
    async def authenticate(self, request: Request, token: Optional[str] = None) -> Optional[AuthContext]:
        """认证"""
        if not self.enabled:
            return None
        
        # 获取 Token
        if token is None:
            token = self._get_token_from_request(request)
        
        if not token:
            return None
        
        # 快速检测：如果是 JWT 格式，直接跳过让其他 provider 处理
        # JWT 格式特征：以 "eyJ" 开头（Base64 编码的 {"alg":...}）
        if token.startswith("eyJ"):
            logger.debug("Token 是 JWT 格式，跳过 AES 认证")
            return None
        
        # 解码
        encrypted_data = self._decode_token(token)
        if not encrypted_data:
            logger.debug("Token 解码失败")
            raise AuthenticationError(AuthErrorCode.AES_DECODE_FAILED, provider=self.name)
        
        # 解密
        plaintext = self._decrypt_token(encrypted_data)
        if not plaintext:
            logger.debug("Token 解密失败")
            raise AuthenticationError(AuthErrorCode.AES_DECRYPT_FAILED, provider=self.name)
        
        # 解析字段
        fields = self._parse_token_fields(plaintext)
        if not fields:
            logger.debug("Token 字段解析失败")
            raise AuthenticationError(AuthErrorCode.AES_PARSE_FAILED, provider=self.name)
        
        # 验证 ServiceKey
        service_key = fields.get("service_key", "")
        if not self._verify_service_key(service_key):
            logger.warning("ServiceKey 验证失败")
            raise AuthenticationError(AuthErrorCode.AES_SERVICE_KEY_INVALID, provider=self.name)
        
        # 验证时间戳
        timestamp = fields.get("timestamp", "")
        if not self._verify_timestamp(timestamp):
            logger.warning("Token 时间戳验证失败")
            raise AuthenticationError(AuthErrorCode.AES_TOKEN_EXPIRED, provider=self.name)
        
        # 提取用户信息
        role = fields.get("role", "")
        user_id = fields.get("user_id", "")
        username = fields.get("username", "")
        
        # 确定用户标识
        if self.user_id_field == "user_id" and user_id:
            external_uid = user_id
        elif self.user_id_field == "username" and username:
            external_uid = username
        else:
            external_uid = role
        
        # 确定用户名
        if not username:
            username = external_uid
        
        # 映射系统角色
        system_role = self._map_system_role(role)
        
        # 构建数据角色
        data_roles = []
        if self.role_field_is_data_role and role:
            data_roles = [role]
        
        # 严格角色验证：检查数据角色是否存在于数据库
        if self.strict_role_validation and data_roles:
            role_exists = await self._verify_data_role_exists(data_roles[0])
            if not role_exists:
                logger.warning(
                    "数据角色验证失败：角色不存在",
                    role=data_roles[0],
                    user_id=external_uid
                )
                raise AuthenticationError(AuthErrorCode.AES_ROLE_NOT_FOUND, detail=data_roles[0], provider=self.name)
        
        logger.debug(
            "External AES 认证成功",
            user_id=external_uid,
            username=username,
            role=role,
            system_role=system_role,
            data_roles=data_roles
        )
        
        return AuthContext(
            user_id=external_uid,
            username=username,
            role=system_role,
            source=self.name,
            attributes={"original_role": role},
            extra={"token_format": self.token_format, "algorithm": self.algorithm},
            data_roles=data_roles,
            user_attributes={},
        )
    
    @classmethod
    def get_supported_algorithms(cls) -> List[Dict[str, Any]]:
        """获取支持的加密算法列表"""
        result = []
        for algo, info in cls.SUPPORTED_ALGORITHMS.items():
            result.append({
                "value": algo,
                "label": f"{algo}（密钥{info['key_size']}字节）",
                "key_size": info["key_size"],
                "mode": info["mode"],
                "cipher": info["cipher"],
            })
        return result
    
    @classmethod
    def get_token_formats(cls) -> List[Dict[str, str]]:
        """获取支持的 Token 格式列表"""
        return [
            {"value": "simple", "label": "简单模式: 角色|时间戳|ServiceKey"},
            {"value": "with_user", "label": "完整用户模式: 用户ID|用户名|角色|时间戳|ServiceKey"},
            {"value": "with_username", "label": "用户名模式: 用户名|角色|时间戳|ServiceKey"},
            {"value": "role_only", "label": "仅角色模式: 角色|时间戳|ServiceKey"},
            {"value": "custom", "label": "自定义格式"},
        ]


def create_test_token(
    role: str,
    aes_key: str,
    aes_iv: str,
    service_key: str,
    algorithm: str = "AES-128-CBC",
    token_format: str = "simple",
    user_id: str = "",
    username: str = "",
    timestamp: Optional[datetime] = None,
    encoding: str = "base64",
) -> str:
    """
    创建测试 Token（用于开发调试）
    
    Args:
        role: 角色名称
        aes_key: 加密密钥
        aes_iv: 初始化向量
        service_key: ServiceKey
        algorithm: 加密算法
        token_format: Token 格式
        user_id: 用户ID（with_user 格式需要）
        username: 用户名（with_user 格式需要）
        timestamp: 时间戳（默认当前时间）
        encoding: 编码方式
    
    Returns:
        加密后的 Token 字符串
    """
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import pad
    except ImportError:
        from Cryptodome.Cipher import AES
        from Cryptodome.Util.Padding import pad
    
    # 生成时间戳
    if timestamp is None:
        timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    
    # 构建明文
    if token_format == "with_user":
        plaintext = f"{user_id}|{username}|{role}|{timestamp_str}|{service_key}"
    elif token_format == "with_username":
        plaintext = f"{username}|{role}|{timestamp_str}|{service_key}"
    else:
        plaintext = f"{role}|{timestamp_str}|{service_key}"
    
    # 调整密钥和 IV
    algo_info = ExternalAESProvider.SUPPORTED_ALGORITHMS.get(algorithm, {"key_size": 16, "mode": "CBC"})
    key_size = algo_info["key_size"]
    
    key = aes_key.encode('utf-8')
    if len(key) < key_size:
        key = hashlib.sha256(key).digest()[:key_size]
    elif len(key) > key_size:
        key = key[:key_size]
    
    iv = aes_iv.encode('utf-8')
    if len(iv) < 16:
        iv = iv + b'\x00' * (16 - len(iv))
    elif len(iv) > 16:
        iv = iv[:16]
    
    # 加密
    mode = algo_info["mode"]
    if mode == "CBC":
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded = pad(plaintext.encode('utf-8'), 16)
        encrypted = cipher.encrypt(padded)
    elif mode == "GCM":
        cipher = AES.new(key, AES.MODE_GCM, nonce=iv[:12])
        encrypted, tag = cipher.encrypt_and_digest(plaintext.encode('utf-8'))
        encrypted = encrypted + tag
    else:
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded = pad(plaintext.encode('utf-8'), 16)
        encrypted = cipher.encrypt(padded)
    
    # 编码
    if encoding == "base64url":
        return base64.urlsafe_b64encode(encrypted).decode('utf-8').rstrip('=')
    elif encoding == "hex":
        return encrypted.hex()
    else:
        return base64.b64encode(encrypted).decode('utf-8')

