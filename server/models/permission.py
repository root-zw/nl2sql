"""
NL2SQL 数据权限模型
数据角色、表权限、行级过滤、用户属性相关的Pydantic模型
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field
from enum import Enum


# ============================================================================
# 枚举类型
# ============================================================================

class ColumnAccessMode(str, Enum):
    """列访问模式"""
    WHITELIST = "whitelist"  # 白名单模式：默认不可见，仅included可见
    BLACKLIST = "blacklist"  # 黑名单模式：默认可见，excluded不可见


class FilterOperator(str, Enum):
    """过滤操作符"""
    EQ = "="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    BETWEEN = "BETWEEN"


class FilterValueType(str, Enum):
    """过滤值类型"""
    STATIC = "static"          # 静态值
    USER_ATTR = "user_attr"    # 用户属性
    EXPRESSION = "expression"  # SQL表达式


class FilterLogic(str, Enum):
    """过滤条件组合逻辑"""
    AND = "AND"
    OR = "OR"


class RoleScopeType(str, Enum):
    """角色范围类型"""
    ALL = "all"        # 全量访问所有连接
    LIMITED = "limited"  # 受限访问（需配置具体连接）


# ============================================================================
# 数据角色模型（全局角色）
# ============================================================================

class DataRoleBase(BaseModel):
    """数据角色基础模型"""
    role_name: str = Field(..., max_length=100, description="角色名称")
    role_code: str = Field(..., max_length=50, description="角色编码")
    description: Optional[str] = Field(None, description="角色描述")
    scope_type: RoleScopeType = Field(RoleScopeType.LIMITED, description="范围类型：all=全量访问，limited=受限访问")
    is_default: bool = Field(False, description="是否为默认角色")
    is_active: bool = Field(True, description="是否启用")


class DataRoleCreate(DataRoleBase):
    """创建数据角色（全局，无需绑定connection_id）"""
    pass


class DataRoleUpdate(BaseModel):
    """更新数据角色"""
    role_name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    scope_type: Optional[RoleScopeType] = None
    is_default: Optional[bool] = None
    is_active: Optional[bool] = None


class DataRoleInDB(DataRoleBase):
    """数据库中的数据角色"""
    role_id: UUID
    created_at: datetime
    created_by: Optional[UUID] = None
    updated_at: datetime

    class Config:
        from_attributes = True


class DataRoleResponse(DataRoleInDB):
    """数据角色响应（含关联信息）"""
    user_count: int = 0  # 关联用户数
    connection_count: int = 0  # 可访问的连接数
    table_permission_count: int = 0  # 表权限数
    row_filter_count: int = 0  # 行过滤规则数


# ============================================================================
# [已删除] 角色连接权限模型
# 新架构中移除了连接权限层，权限直接在表级别控制
# ============================================================================


# ============================================================================
# 表权限模型
# ============================================================================

class TablePermissionBase(BaseModel):
    """表权限基础模型"""
    can_query: bool = Field(True, description="是否可查询")
    can_export: bool = Field(False, description="是否可导出")
    
    # 列可见性控制
    column_access_mode: ColumnAccessMode = Field(
        ColumnAccessMode.BLACKLIST, 
        description="列访问模式"
    )
    included_column_ids: Optional[List[UUID]] = Field(
        None, 
        description="白名单模式下允许的字段ID"
    )
    excluded_column_ids: Optional[List[UUID]] = Field(
        None, 
        description="黑名单模式下禁止的字段ID"
    )
    
    # 列脱敏
    masked_column_ids: Optional[List[UUID]] = Field(
        None, 
        description="需要脱敏的字段ID"
    )
    
    # 列使用限制（防推断攻击）
    restricted_filter_column_ids: Optional[List[UUID]] = Field(
        None, 
        description="禁止用于WHERE的字段ID"
    )
    restricted_aggregate_column_ids: Optional[List[UUID]] = Field(
        None, 
        description="禁止用于聚合的字段ID"
    )
    restricted_group_by_column_ids: Optional[List[UUID]] = Field(
        None, 
        description="禁止用于GROUP BY的字段ID"
    )
    restricted_order_by_column_ids: Optional[List[UUID]] = Field(
        None, 
        description="禁止用于ORDER BY的字段ID"
    )


class TablePermissionCreate(TablePermissionBase):
    """创建表权限"""
    role_id: UUID = Field(..., description="角色ID")
    table_id: UUID = Field(..., description="表ID")


class TablePermissionUpdate(BaseModel):
    """更新表权限"""
    can_query: Optional[bool] = None
    can_export: Optional[bool] = None
    column_access_mode: Optional[ColumnAccessMode] = None
    included_column_ids: Optional[List[UUID]] = None
    excluded_column_ids: Optional[List[UUID]] = None
    masked_column_ids: Optional[List[UUID]] = None
    restricted_filter_column_ids: Optional[List[UUID]] = None
    restricted_aggregate_column_ids: Optional[List[UUID]] = None
    restricted_group_by_column_ids: Optional[List[UUID]] = None
    restricted_order_by_column_ids: Optional[List[UUID]] = None


class TablePermissionInDB(TablePermissionBase):
    """数据库中的表权限"""
    permission_id: UUID
    role_id: UUID
    table_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TablePermissionResponse(TablePermissionInDB):
    """表权限响应（含表信息）"""
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    display_name: Optional[str] = None
    connection_id: Optional[UUID] = None
    connection_name: Optional[str] = None


# ============================================================================
# 行级过滤规则模型
# ============================================================================

class FilterCondition(BaseModel):
    """单个过滤条件"""
    field_name: str = Field(..., description="字段名")
    operator: str = Field(..., description="操作符")
    value_type: FilterValueType = Field(..., description="值类型")
    value: Any = Field(..., description="值")


class FilterDefinition(BaseModel):
    """过滤条件定义"""
    conditions: List[FilterCondition] = Field(..., description="条件列表")
    logic: FilterLogic = Field(FilterLogic.AND, description="组合逻辑")


class RowFilterBase(BaseModel):
    """行级过滤规则基础模型"""
    filter_name: str = Field(..., max_length=100, description="规则名称")
    description: Optional[str] = Field(None, description="规则描述")
    table_id: Optional[UUID] = Field(None, description="作用表ID，NULL表示所有表")
    filter_definition: Dict[str, Any] = Field(..., description="过滤条件定义")
    priority: int = Field(0, description="优先级")
    is_active: bool = Field(True, description="是否启用")


class RowFilterCreate(RowFilterBase):
    """创建行级过滤规则"""
    role_id: UUID = Field(..., description="角色ID")


class RowFilterUpdate(BaseModel):
    """更新行级过滤规则"""
    filter_name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    table_id: Optional[UUID] = None
    filter_definition: Optional[Dict[str, Any]] = None
    priority: Optional[int] = None
    is_active: Optional[bool] = None


class RowFilterInDB(RowFilterBase):
    """数据库中的行级过滤规则"""
    filter_id: UUID
    role_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class RowFilterResponse(RowFilterInDB):
    """行级过滤规则响应"""
    table_name: Optional[str] = None
    parsed_sql: Optional[str] = None  # 解析后的SQL条件（预览用）


class RowFilterFromTemplate(BaseModel):
    """从模板创建行级过滤规则"""
    template_code: str = Field(..., description="模板编码")
    params: Dict[str, Any] = Field(..., description="模板参数")
    filter_name: str = Field(..., description="规则名称")
    table_id: Optional[UUID] = Field(None, description="作用表ID")


# ============================================================================
# RLS规则模板模型
# ============================================================================

class RLSTemplateBase(BaseModel):
    """RLS规则模板基础模型"""
    template_name: str = Field(..., max_length=100, description="模板名称")
    template_code: str = Field(..., max_length=50, description="模板编码")
    description: Optional[str] = Field(None, description="模板描述")
    category: Optional[str] = Field(None, max_length=50, description="分类")
    template_definition: Dict[str, Any] = Field(..., description="模板定义")
    required_params: Optional[Dict[str, Any]] = Field(None, description="必需参数")
    optional_params: Optional[Dict[str, Any]] = Field(None, description="可选参数")
    example_params: Optional[Dict[str, Any]] = Field(None, description="示例参数")
    example_sql: Optional[str] = Field(None, description="示例SQL")


class RLSTemplateCreate(RLSTemplateBase):
    """创建RLS规则模板"""
    is_system: bool = Field(False, description="是否系统模板")


class RLSTemplateInDB(RLSTemplateBase):
    """数据库中的RLS规则模板"""
    template_id: UUID
    is_system: bool
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class RLSTemplateResponse(RLSTemplateInDB):
    """RLS规则模板响应"""
    pass


# ============================================================================
# 用户-数据角色关联模型
# ============================================================================

class UserDataRoleBase(BaseModel):
    """用户-数据角色关联基础模型"""
    expires_at: Optional[datetime] = Field(None, description="过期时间")
    is_active: bool = Field(True, description="是否启用")


class UserDataRoleAssignRequest(UserDataRoleBase):
    """为用户分配数据角色的请求"""
    role_id: UUID = Field(..., description="角色ID")


class UserDataRoleCreate(UserDataRoleBase):
    """创建用户-数据角色关联（包含用户ID）"""
    user_id: UUID = Field(..., description="用户ID")
    role_id: UUID = Field(..., description="角色ID")


class UserDataRoleInDB(UserDataRoleBase):
    """数据库中的用户-数据角色关联"""
    user_id: UUID
    role_id: UUID
    granted_by: Optional[UUID] = None
    granted_at: datetime

    class Config:
        from_attributes = True


class UserDataRoleResponse(UserDataRoleInDB):
    """用户-数据角色关联响应"""
    role_name: Optional[str] = None
    role_code: Optional[str] = None
    scope_type: Optional[str] = None  # 'all' 或 'limited'
    connection_count: int = 0  # 可访问的连接数（scope_type=limited时有效）


# ============================================================================
# 用户属性模型
# ============================================================================

class UserAttributeBase(BaseModel):
    """用户属性基础模型"""
    attribute_name: str = Field(..., max_length=100, description="属性名")
    attribute_value: str = Field(..., description="属性值")


class UserAttributeCreate(UserAttributeBase):
    """创建用户属性"""
    user_id: UUID = Field(..., description="用户ID")


class UserAttributeUpdate(BaseModel):
    """更新用户属性"""
    attribute_value: str = Field(..., description="属性值")


class UserAttributeInDB(UserAttributeBase):
    """数据库中的用户属性"""
    attribute_id: UUID
    user_id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserAttributeBatchUpdate(BaseModel):
    """批量更新用户属性"""
    attributes: Dict[str, str] = Field(..., description="属性字典 {name: value}")


# ============================================================================
# 权限审计日志模型
# ============================================================================

class PermissionAuditLog(BaseModel):
    """权限审计日志"""
    log_id: UUID
    user_id: Optional[UUID] = None
    username: Optional[str] = None
    query_id: Optional[UUID] = None
    connection_id: Optional[UUID] = None
    original_question: Optional[str] = None
    applied_roles: Optional[List[UUID]] = None
    applied_table_filters: Optional[Dict[str, Any]] = None
    applied_row_filters: Optional[Dict[str, Any]] = None
    applied_column_masks: Optional[Dict[str, Any]] = None
    generated_sql: Optional[str] = None
    result_row_count: Optional[int] = None
    execution_time_ms: Optional[int] = None
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# 权限检查相关模型
# ============================================================================

class PermissionCheckRequest(BaseModel):
    """权限检查请求"""
    user_id: UUID = Field(..., description="用户ID")
    connection_id: UUID = Field(..., description="数据库连接ID")
    table_ids: Optional[List[UUID]] = Field(None, description="要检查的表ID列表")


class PermissionCheckResult(BaseModel):
    """权限检查结果"""
    user_id: UUID
    connection_id: UUID
    accessible_tables: List[UUID] = Field(default_factory=list, description="可访问的表")
    row_filters: Dict[str, str] = Field(default_factory=dict, description="行过滤条件")
    column_permissions: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, 
        description="列权限配置"
    )
    has_permission: bool = Field(True, description="是否有权限")
    denied_tables: List[UUID] = Field(default_factory=list, description="无权限的表")


class RLSPreviewRequest(BaseModel):
    """RLS规则预览请求"""
    filter_definition: Dict[str, Any] = Field(..., description="过滤条件定义")
    user_id: UUID = Field(..., description="测试用户ID")


class RLSPreviewResponse(BaseModel):
    """RLS规则预览响应"""
    parsed_condition: str = Field(..., description="解析后的SQL条件")
    user_attributes_used: Dict[str, str] = Field(
        default_factory=dict, 
        description="使用的用户属性"
    )
    warnings: List[str] = Field(default_factory=list, description="警告信息")


# ============================================================================
# 用户完整权限视图模型
# ============================================================================

class UserPermissionView(BaseModel):
    """用户完整权限视图"""
    user_id: UUID
    username: str
    system_role: str  # admin/user/viewer
    data_roles: List[UserDataRoleResponse] = Field(
        default_factory=list, 
        description="数据角色列表"
    )
    attributes: Dict[str, str] = Field(
        default_factory=dict, 
        description="用户属性"
    )


class UserPermissionSummary(BaseModel):
    """用户权限摘要（用于前端显示）"""
    user_id: UUID
    username: str
    system_role: str
    data_role_count: int = 0
    connections: List[Dict[str, Any]] = Field(
        default_factory=list, 
        description="可访问的连接列表"
    )


# ============================================================================
# 用户可访问连接模型（用于前端数据源选择）
# ============================================================================

class UserAccessibleConnection(BaseModel):
    """用户可访问的数据库连接"""
    connection_id: UUID
    connection_name: str
    description: Optional[str] = None
    db_type: str
    can_query: bool = True
    can_export: bool = False
    
    
class UserAccessibleConnectionsResponse(BaseModel):
    """用户可访问连接列表响应"""
    user_id: UUID
    is_admin: bool = False  # 系统管理员可访问所有连接
    has_all_access: bool = False  # 是否拥有scope_type=all的角色
    connections: List[UserAccessibleConnection] = Field(
        default_factory=list,
        description="可访问的连接列表"
    )

