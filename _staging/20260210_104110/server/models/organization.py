"""
NL2SQL 组织架构模型
组织架构、用户-组织关联、组织-数据角色关联相关的Pydantic模型
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field
from enum import Enum


# ============================================================================
# 枚举类型
# ============================================================================

class OrgType(str, Enum):
    """组织类型"""
    COMPANY = "company"        # 公司
    DEPARTMENT = "department"  # 部门
    TEAM = "team"              # 团队
    GROUP = "group"            # 小组


# ============================================================================
# 组织架构模型
# ============================================================================

class OrganizationBase(BaseModel):
    """组织架构基础模型"""
    org_code: str = Field(..., max_length=50, description="组织编码，全局唯一")
    org_name: str = Field(..., max_length=100, description="组织名称")
    parent_id: Optional[UUID] = Field(None, description="父组织ID，NULL表示根节点")
    org_type: OrgType = Field(OrgType.DEPARTMENT, description="组织类型")
    description: Optional[str] = Field(None, description="组织描述")
    sort_order: int = Field(0, description="同级排序")
    is_active: bool = Field(True, description="是否启用")


class OrganizationCreate(OrganizationBase):
    """创建组织"""
    pass


class OrganizationUpdate(BaseModel):
    """更新组织"""
    org_name: Optional[str] = Field(None, max_length=100)
    parent_id: Optional[UUID] = None
    org_type: Optional[OrgType] = None
    description: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None


class OrganizationInDB(OrganizationBase):
    """数据库中的组织"""
    org_id: UUID
    source_idp: Optional[str] = None       # 外部来源
    external_org_id: Optional[str] = None  # 外部组织ID
    org_path: Optional[str] = None         # 完整路径
    level: int = 0                         # 层级深度
    created_at: datetime
    created_by: Optional[UUID] = None
    updated_at: datetime

    class Config:
        from_attributes = True


class OrganizationResponse(OrganizationInDB):
    """组织响应（含统计信息）"""
    parent_org_name: Optional[str] = None   # 父组织名称
    direct_user_count: int = 0              # 直接成员数
    child_org_count: int = 0                # 子组织数
    data_role_count: int = 0                # 关联的数据角色数


class OrganizationTreeNode(BaseModel):
    """组织树节点（用于前端树形展示）"""
    org_id: UUID
    org_code: str
    org_name: str
    org_type: OrgType
    level: int
    parent_id: Optional[UUID] = None
    is_active: bool = True
    direct_user_count: int = 0
    children: List["OrganizationTreeNode"] = Field(default_factory=list)

    class Config:
        from_attributes = True


# 解决循环引用
OrganizationTreeNode.model_rebuild()


# ============================================================================
# 组织同步模型（从外部系统同步）
# ============================================================================

class ExternalOrgInfo(BaseModel):
    """外部组织信息（用于同步）"""
    external_org_id: str = Field(..., description="外部系统中的组织ID")
    org_code: Optional[str] = Field(None, description="组织编码")
    org_name: str = Field(..., description="组织名称")
    parent_external_id: Optional[str] = Field(None, description="父组织的外部ID")
    org_type: Optional[str] = Field(None, description="组织类型")
    description: Optional[str] = Field(None, description="组织描述")


class OrgSyncRequest(BaseModel):
    """组织同步请求"""
    provider_key: str = Field(..., description="认证提供者key")
    organizations: List[ExternalOrgInfo] = Field(..., description="组织列表")
    clear_existing: bool = Field(False, description="是否清除该来源的现有组织")


class OrgSyncResult(BaseModel):
    """组织同步结果"""
    success: bool = True
    total: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: List[str] = Field(default_factory=list)


# ============================================================================
# 用户-组织关联模型
# ============================================================================

class UserOrgAssignment(BaseModel):
    """用户组织分配"""
    user_id: UUID = Field(..., description="用户ID")
    org_id: Optional[UUID] = Field(None, description="组织ID，NULL表示取消分配")
    position: Optional[str] = Field(None, max_length=100, description="职位")


class UserOrgBatchAssignment(BaseModel):
    """批量用户组织分配"""
    user_ids: List[UUID] = Field(..., description="用户ID列表")
    org_id: Optional[UUID] = Field(None, description="组织ID")
    position: Optional[str] = Field(None, description="职位")


class UserWithOrg(BaseModel):
    """带组织信息的用户"""
    user_id: UUID
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    role: str                              # 系统角色
    is_active: bool
    org_id: Optional[UUID] = None
    org_name: Optional[str] = None
    org_code: Optional[str] = None
    position: Optional[str] = None
    external_idp: Optional[str] = None     # 认证来源

    class Config:
        from_attributes = True


# ============================================================================
# 组织-数据角色关联模型
# ============================================================================

class OrgDataRoleBase(BaseModel):
    """组织-数据角色关联基础模型"""
    inherit_to_children: bool = Field(True, description="是否继承给子组织")
    is_active: bool = Field(True, description="是否启用")


class OrgDataRoleAssign(OrgDataRoleBase):
    """为组织分配数据角色的请求"""
    role_id: UUID = Field(..., description="角色ID")


class OrgDataRoleCreate(OrgDataRoleBase):
    """创建组织-数据角色关联"""
    org_id: UUID = Field(..., description="组织ID")
    role_id: UUID = Field(..., description="角色ID")


class OrgDataRoleInDB(OrgDataRoleBase):
    """数据库中的组织-数据角色关联"""
    org_id: UUID
    role_id: UUID
    granted_by: Optional[UUID] = None
    granted_at: datetime

    class Config:
        from_attributes = True


class OrgDataRoleResponse(OrgDataRoleInDB):
    """组织-数据角色关联响应"""
    role_name: Optional[str] = None
    role_code: Optional[str] = None
    scope_type: Optional[str] = None
    org_name: Optional[str] = None


# ============================================================================
# 用户有效权限模型（包含组织继承）
# ============================================================================

class UserEffectiveRole(BaseModel):
    """用户有效数据角色（包含来源信息）"""
    user_id: UUID
    username: str
    user_org_id: Optional[UUID] = None
    user_org_name: Optional[str] = None
    role_id: UUID
    role_code: str
    role_name: str
    scope_type: str
    grant_source: str                      # 'direct', 'org_direct', 'org_inherited'
    source_org_id: Optional[UUID] = None   # 权限来源组织ID
    source_org_name: Optional[str] = None  # 权限来源组织名称

    class Config:
        from_attributes = True


class UserEffectiveRolesResponse(BaseModel):
    """用户有效权限响应"""
    user_id: UUID
    username: str
    org_id: Optional[UUID] = None
    org_name: Optional[str] = None
    roles: List[UserEffectiveRole] = Field(default_factory=list)
    direct_roles_count: int = 0            # 直接分配的角色数
    inherited_roles_count: int = 0         # 继承的角色数


# ============================================================================
# 认证提供者同步配置模型
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
    """同步配置"""
    sync_users_enabled: bool = Field(False, description="是否启用用户同步")
    user_mapping: Optional[UserMappingConfig] = Field(None, description="用户字段映射")
    sync_orgs_enabled: bool = Field(False, description="是否启用组织同步")
    org_mapping: Optional[OrgMappingConfig] = Field(None, description="组织字段映射")
    orgs_endpoint: Optional[str] = Field(None, description="组织列表API端点")
    users_endpoint: Optional[str] = Field(None, description="用户列表API端点")


# ============================================================================
# 组织成员查询模型
# ============================================================================

class OrgMembersQuery(BaseModel):
    """组织成员查询参数"""
    include_children: bool = Field(False, description="是否包含子组织成员")
    is_active: Optional[bool] = Field(None, description="用户状态筛选")
    search: Optional[str] = Field(None, description="搜索关键词")
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页数量")


class OrgMembersResponse(BaseModel):
    """组织成员列表响应"""
    org_id: UUID
    org_name: str
    include_children: bool
    total: int
    page: int
    page_size: int
    members: List[UserWithOrg] = Field(default_factory=list)

