from typing import Optional, Dict, Any, List
from uuid import UUID
from pydantic import BaseModel, Field


# ============================================================================
# 同步配置模型（用于config_json.sync_config）
# ============================================================================

class UserMappingConfig(BaseModel):
    """用户字段映射配置"""
    username_field: str = Field("preferred_username", description="用户名字段")
    email_field: str = Field("email", description="邮箱字段")
    full_name_field: str = Field("name", description="全名字段")
    org_field: Optional[str] = Field(None, description="用户所属组织字段")


class OrgMappingConfig(BaseModel):
    """组织字段映射配置"""
    id_field: str = Field("org_id", description="外部组织ID字段")
    code_field: Optional[str] = Field("org_code", description="组织编码字段")
    name_field: str = Field("org_name", description="组织名称字段")
    parent_field: Optional[str] = Field("parent_org_id", description="父组织ID字段")
    type_field: Optional[str] = Field("org_type", description="组织类型字段")


class SyncConfig(BaseModel):
    """同步配置（存储在config_json.sync_config中）"""
    sync_users_enabled: bool = Field(False, description="是否启用用户同步")
    user_mapping: Optional[UserMappingConfig] = Field(None, description="用户字段映射")
    sync_orgs_enabled: bool = Field(False, description="是否启用组织同步")
    org_mapping: Optional[OrgMappingConfig] = Field(None, description="组织字段映射")
    orgs_endpoint: Optional[str] = Field(None, description="组织列表API端点")
    users_endpoint: Optional[str] = Field(None, description="用户列表API端点")


# ============================================================================
# 认证提供者模型
# ============================================================================

class AuthProviderBase(BaseModel):
    provider_key: str = Field(..., min_length=1, max_length=100)
    provider_type: str = Field(..., description="local/oidc/oauth2/api_gateway/ldap")
    config_json: Dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    priority: int = 100


class AuthProviderCreate(AuthProviderBase):
    pass


class AuthProviderUpdate(BaseModel):
    config_json: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None
    priority: Optional[int] = None


class AuthProviderInDB(AuthProviderBase):
    provider_id: UUID

    class Config:
        from_attributes = True

    def get_sync_config(self) -> Optional[SyncConfig]:
        """获取同步配置"""
        sync_data = self.config_json.get("sync_config")
        if sync_data:
            return SyncConfig(**sync_data)
        return None


class AuthProviderResponse(AuthProviderInDB):
    """认证提供者响应（含同步配置解析）"""
    sync_users_enabled: bool = False
    sync_orgs_enabled: bool = False
    
    @classmethod
    def from_db(cls, db_model: AuthProviderInDB) -> "AuthProviderResponse":
        sync_config = db_model.get_sync_config()
        return cls(
            **db_model.model_dump(),
            sync_users_enabled=sync_config.sync_users_enabled if sync_config else False,
            sync_orgs_enabled=sync_config.sync_orgs_enabled if sync_config else False
        )

