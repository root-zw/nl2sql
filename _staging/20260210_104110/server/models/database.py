"""
NL2SQL 数据模型
重构后的数据库模型：用户、数据库连接、业务域、表、字段、全局规则
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field, validator
from enum import Enum
from server.utils.password_validator import validate_password_strength


# ============================================================================
# 枚举类型
# ============================================================================

class UserRole(str, Enum):
    """系统角色"""
    ADMIN = "admin"        # 系统管理员：所有管理功能
    DATA_ADMIN = "data_admin"  # 数据管理员：数据库连接/元数据/同步/数据权限
    USER = "user"          # 普通用户：仅查询，不能登录后台


class DatabaseType(str, Enum):
    """数据库类型"""
    SQLSERVER = "sqlserver"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"


class SyncStatus(str, Enum):
    """同步状态"""
    SUCCESS = "success"
    FAILED = "failed"
    SYNCING = "syncing"


class FieldType(str, Enum):
    """字段类型"""
    DIMENSION = "dimension"  # 维度
    MEASURE = "measure"      # 度量
    TIMESTAMP = "timestamp"  # 时间戳
    IDENTIFIER = "identifier"  # 标识
    SPATIAL = "spatial"  # 空间


class DimensionType(str, Enum):
    """维度类型"""
    CATEGORICAL = "categorical"  # 分类
    HIERARCHICAL = "hierarchical"  # 层级
    TEMPORAL = "temporal"  # 时间


class RelationshipType(str, Enum):
    """表关系类型"""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_MANY = "many_to_many"


class JoinType(str, Enum):
    """JOIN类型"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class DetectionMethod(str, Enum):
    """关系识别方法"""
    FOREIGN_KEY = "foreign_key"  # 外键约束
    NAME_SIMILARITY = "name_similarity"  # 名称相似度
    DATA_ANALYSIS = "data_analysis"  # 数据分析
    MANUAL = "manual"  # 手动创建


class RuleType(str, Enum):
    """全局规则类型"""
    DERIVED_METRIC = "derived_metric"  # 派生指标
    DEFAULT_FILTER = "default_filter"  # 默认过滤
    CUSTOM_INSTRUCTION = "custom_instruction"  # 自定义规则


class ExecutionStatus(str, Enum):
    """执行状态"""
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ============================================================================
# 第一层：用户模型
# ============================================================================

class UserBase(BaseModel):
    """用户基础信息"""
    # 兼容外部目录中长度较短的账号（如 2 位），放宽到 1
    username: str = Field(..., min_length=1, max_length=50)
    email: Optional[str] = Field(None, max_length=100)
    full_name: Optional[str] = Field(None, max_length=100)
    role: UserRole = UserRole.USER  # 默认普通用户


class UserCreate(UserBase):
    """创建用户"""
    password: str = Field(..., min_length=8)
    
    @validator('password')
    def validate_password(cls, v):
        is_valid, error_msg = validate_password_strength(v)
        if not is_valid:
            raise ValueError(error_msg)
        return v


class UserUpdate(BaseModel):
    """更新用户"""
    email: Optional[str] = None
    full_name: Optional[str] = None
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None


class UserInDB(UserBase):
    """数据库中的用户"""
    user_id: UUID
    password_hash: str
    is_active: bool
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserResponse(UserBase):
    """用户响应（不包含密码）"""
    user_id: UUID
    is_active: bool
    last_login_at: Optional[datetime]
    created_at: datetime
    updated_at: Optional[datetime] = None
    external_idp: Optional[str] = None  # 外部身份提供方标识
    org_id: Optional[UUID] = None  # 所属组织ID
    org_name: Optional[str] = None  # 所属组织名称（关联查询）

    class Config:
        from_attributes = True


class LoginRequest(BaseModel):
    """登录请求"""
    username: str
    password: str
    captcha_code: str = Field(..., min_length=4, max_length=4, pattern=r"^[A-Za-z0-9]{4}$")
    captcha_id: str = Field(..., min_length=8)


class LoginResponse(BaseModel):
    """登录响应"""
    access_token: str
    refresh_token: Optional[str] = None  # Refresh Token
    token_type: str = "bearer"
    expires_in: Optional[int] = None  # Access Token 过期时间（秒）
    refresh_expires_in: Optional[int] = None  # Refresh Token 过期时间（秒）
    user: UserResponse


class CaptchaResponse(BaseModel):
    """验证码响应"""
    captcha_id: str
    image_base64: str
    expires_in: int = Field(default=300, description="验证码有效期（秒）")


# ============================================================================
# 第二层：数据库连接模型
# ============================================================================

class DatabaseConnectionBase(BaseModel):
    """数据库连接基础信息"""
    connection_name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    db_type: DatabaseType = DatabaseType.SQLSERVER
    host: str = Field(..., min_length=1)
    port: int = Field(..., gt=0, lt=65536)
    database_name: str = Field(..., min_length=1)
    username: str = Field(..., min_length=1)
    max_connections: int = Field(10, gt=0, le=100)
    connection_timeout: int = Field(30, gt=0, le=300)


class DatabaseConnectionCreate(DatabaseConnectionBase):
    """创建数据库连接"""
    password: str = Field(..., min_length=1)  # 明文密码，服务端加密


class DatabaseConnectionUpdate(BaseModel):
    """更新数据库连接"""
    connection_name: Optional[str] = None
    description: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None  # 如果提供，则更新
    max_connections: Optional[int] = None
    connection_timeout: Optional[int] = None
    is_active: Optional[bool] = None


class DatabaseConnectionInDB(DatabaseConnectionBase):
    """数据库中的连接"""
    connection_id: UUID
    password_encrypted: str
    is_active: bool
    last_sync_at: Optional[datetime]
    sync_status: Optional[SyncStatus]
    sync_message: Optional[str]
    table_count: int
    field_count: int
    created_at: datetime
    created_by: Optional[UUID]
    updated_at: datetime

    class Config:
        from_attributes = True


class DatabaseConnectionResponse(DatabaseConnectionBase):
    """数据库连接响应（不包含密码）"""
    connection_id: UUID
    is_active: bool
    last_sync_at: Optional[datetime]
    sync_status: Optional[SyncStatus]
    sync_message: Optional[str]
    table_count: int
    field_count: int
    created_at: datetime

    class Config:
        from_attributes = True


class TestConnectionRequest(BaseModel):
    """测试数据库连接"""
    connection_id: Optional[UUID] = None  # 如果提供，从数据库读取密码；否则使用提供的密码
    db_type: Optional[DatabaseType] = None  # connection_id 存在时可选
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None  # connection_id 存在时忽略


class TestConnectionResponse(BaseModel):
    """测试连接响应"""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None


# ============================================================================
# 第三层：业务域模型
# ============================================================================

class BusinessDomainBase(BaseModel):
    """业务域基础信息"""
    domain_code: str = Field(..., min_length=1, max_length=50)
    domain_name: str = Field(..., min_length=1, max_length=100)
    description: str = Field(default="", max_length=1000)  # 可选描述
    keywords: List[str] = Field(default_factory=list)
    typical_queries: List[str] = Field(default_factory=list)
    icon: str = Field("", max_length=50)
    color: str = Field("#409eff", max_length=20)
    sort_order: int = 0


class BusinessDomainCreate(BusinessDomainBase):
    """创建业务域（connection_id 可选，为空时创建全局业务域）"""
    connection_id: Optional[UUID] = None


class BusinessDomainUpdate(BaseModel):
    """更新业务域"""
    domain_name: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    typical_queries: Optional[List[str]] = None
    icon: Optional[str] = None
    color: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None


class BusinessDomainInDB(BusinessDomainBase):
    """数据库中的业务域"""
    domain_id: UUID
    connection_id: Optional[UUID] = None  # 全局业务域时为空
    table_count: int
    is_active: bool
    created_at: datetime
    created_by: Optional[UUID]
    updated_at: datetime

    class Config:
        from_attributes = True


class BusinessDomainResponse(BusinessDomainBase):
    """业务域响应"""
    domain_id: UUID
    connection_id: Optional[UUID] = None  # 全局业务域时为空
    table_count: int
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# 第四层：数据表与列模型
# ============================================================================

class DBTableBase(BaseModel):
    """数据表基础信息"""
    schema_name: Optional[str] = None
    table_name: str = Field(..., min_length=1)
    display_name: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    is_included: bool = True


class DBTableCreate(DBTableBase):
    """创建数据表"""
    connection_id: UUID
    domain_id: Optional[UUID] = None


class DBTableUpdate(BaseModel):
    """更新数据表"""
    display_name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    domain_id: Optional[UUID] = None
    is_included: Optional[bool] = None


class DBTableInDB(DBTableBase):
    """数据库中的表"""
    table_id: UUID
    connection_id: UUID
    domain_id: Optional[UUID]
    row_count: Optional[int]
    column_count: Optional[int]
    data_size_mb: Optional[float]
    last_updated_at: Optional[datetime]
    discovered_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class DBTableResponse(DBTableBase):
    """数据表响应"""
    table_id: UUID
    connection_id: UUID
    domain_id: Optional[UUID]
    row_count: Optional[int]
    column_count: Optional[int]
    discovered_at: datetime

    class Config:
        from_attributes = True


class DBColumnBase(BaseModel):
    """数据列基础信息"""
    column_name: str = Field(..., min_length=1)
    data_type: str
    max_length: Optional[int] = None
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False


class DBColumnCreate(DBColumnBase):
    """创建数据列"""
    table_id: UUID
    referenced_table_id: Optional[UUID] = None
    referenced_column_id: Optional[UUID] = None


class DBColumnInDB(DBColumnBase):
    """数据库中的列"""
    column_id: UUID
    table_id: UUID
    referenced_table_id: Optional[UUID]
    referenced_column_id: Optional[UUID]
    distinct_count: Optional[int]
    null_count: Optional[int]
    sample_values: List[str] = Field(default_factory=list)
    discovered_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class DBColumnResponse(DBColumnBase):
    """数据列响应"""
    column_id: UUID
    table_id: UUID
    distinct_count: Optional[int]
    sample_values: List[str]

    class Config:
        from_attributes = True


# ============================================================================
# 第五层：字段配置模型
# ============================================================================

class FieldBase(BaseModel):
    """字段基础信息"""
    source_type: str = Field(..., pattern="^(column|expression)$")
    field_type: FieldType
    display_name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    synonyms: List[str] = Field(default_factory=list)

    #  基础指标配置（融合到字段）
    default_aggregation: Optional[str] = None  # SUM, AVG, COUNT, MAX, MIN
    allowed_aggregations: List[str] = Field(default_factory=list)
    unit: Optional[str] = None
    unit_conversion: Optional[Dict[str, Any]] = None  #  单位转换配置
    format_pattern: Optional[str] = None

    # 明细显示控制（默认关闭）
    show_in_detail: Optional[bool] = False

    # 维度配置
    dimension_type: Optional[DimensionType] = None
    hierarchy_level: Optional[int] = None

    # 度量配置
    is_additive: bool = True

    # 标识配置
    is_unique: bool = False

    tags: List[str] = Field(default_factory=list)
    business_category: Optional[str] = None


class FieldCreate(FieldBase):
    """创建字段"""
    connection_id: UUID
    source_column_id: Optional[UUID] = None
    source_expression: Optional[str] = None
    parent_field_id: Optional[UUID] = None


class FieldUpdate(BaseModel):
    """更新字段"""
    field_type: Optional[FieldType] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    synonyms: Optional[List[str]] = None
    default_aggregation: Optional[str] = None
    allowed_aggregations: Optional[List[str]] = None
    unit: Optional[str] = None
    unit_conversion: Optional[Dict[str, Any]] = None  #  单位转换配置
    format_pattern: Optional[str] = None
    dimension_type: Optional[DimensionType] = None
    hierarchy_level: Optional[int] = None
    is_additive: Optional[bool] = None
    is_unique: Optional[bool] = None
    tags: Optional[List[str]] = None
    business_category: Optional[str] = None
    is_active: Optional[bool] = None
    show_in_detail: Optional[bool] = None
    enum_sync_config: Optional[Dict[str, Any]] = None  # 枚举值同步配置


class FieldInDB(FieldBase):
    """数据库中的字段"""
    field_id: UUID
    connection_id: UUID
    source_column_id: Optional[UUID]
    source_expression: Optional[str]
    parent_field_id: Optional[UUID]
    auto_detected: bool
    confidence_score: Optional[float]
    is_active: bool
    created_at: datetime
    created_by: Optional[UUID]
    updated_at: datetime

    class Config:
        from_attributes = True


class FieldResponse(FieldBase):
    """字段响应"""
    field_id: UUID
    connection_id: UUID
    source_column_id: Optional[UUID]
    source_expression: Optional[str]
    auto_detected: bool
    confidence_score: Optional[float]
    is_active: bool
    show_in_detail: Optional[bool] = False
    created_at: datetime
    table_name: Optional[str] = None  # 字段所属表名（来自db_tables）
    schema_name: Optional[str] = None  # 字段所属schema（来自db_tables）
    
    # 枚举值同步配置和统计（仅维度字段）
    enum_sync_config: Optional[Dict[str, Any]] = None  # 同步配置
    enum_count: Optional[int] = None  # 枚举值总数
    synced_enum_count: Optional[int] = None  # 已同步数量
    last_synced_at: Optional[datetime] = None  # 最后同步时间

    class Config:
        from_attributes = True


# ============================================================================
# 第六层：枚举值模型
# ============================================================================

class EnumValueBase(BaseModel):
    """枚举值基础信息"""
    original_value: str = Field(..., max_length=200)
    display_value: Optional[str] = None
    synonyms: List[str] = Field(default_factory=list)
    includes_values: Optional[List[str]] = Field(default=None, description="该枚举值包含的其他标准值列表，用于查询展开")


class EnumValueCreate(EnumValueBase):
    """创建枚举值"""
    frequency: int = 0


class EnumValueUpdate(BaseModel):
    """更新枚举值"""
    display_value: Optional[str] = None
    synonyms: Optional[List[str]] = None
    includes_values: Optional[List[str]] = Field(default=None, description="该枚举值包含的其他标准值列表")
    is_active: Optional[bool] = None


class EnumValueInDB(EnumValueBase):
    """数据库中的枚举值"""
    enum_value_id: UUID
    field_id: UUID
    frequency: int
    is_active: bool
    sampled_at: datetime
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class EnumValueResponse(EnumValueBase):
    """枚举值响应"""
    enum_value_id: UUID
    field_id: UUID
    frequency: int
    is_active: bool

    class Config:
        from_attributes = True


# ============================================================================
# 第七层：表关系模型
# ============================================================================

class TableRelationshipBase(BaseModel):
    """表关系基础信息"""
    relationship_type: RelationshipType = RelationshipType.ONE_TO_MANY
    join_type: JoinType = JoinType.INNER
    relationship_name: Optional[str] = None
    description: Optional[str] = None


class TableRelationshipCreate(TableRelationshipBase):
    """创建表关系"""
    connection_id: UUID
    left_table_id: UUID
    right_table_id: UUID
    left_column_id: UUID
    right_column_id: UUID
    detection_method: DetectionMethod = DetectionMethod.MANUAL


class TableRelationshipUpdate(BaseModel):
    """更新表关系"""
    relationship_type: Optional[RelationshipType] = None
    join_type: Optional[JoinType] = None
    relationship_name: Optional[str] = None
    description: Optional[str] = None
    is_confirmed: Optional[bool] = None
    is_active: Optional[bool] = None


class TableRelationshipInDB(TableRelationshipBase):
    """数据库中的表关系"""
    relationship_id: UUID
    connection_id: UUID
    left_table_id: UUID
    right_table_id: UUID
    left_column_id: UUID
    right_column_id: UUID
    detection_method: DetectionMethod
    confidence_score: Optional[float]
    is_confirmed: bool
    is_active: bool
    detected_at: datetime
    confirmed_at: Optional[datetime]
    confirmed_by: Optional[UUID]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TableRelationshipResponse(TableRelationshipBase):
    """表关系响应"""
    relationship_id: UUID
    connection_id: UUID
    connection_name: Optional[str] = None  # 数据源名称
    left_table_id: UUID
    right_table_id: UUID
    left_column_id: UUID
    right_column_id: UUID
    detection_method: DetectionMethod
    confidence_score: Optional[float]
    is_confirmed: bool
    detected_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# 第八层：全局规则模型
# ============================================================================

class GlobalRuleBase(BaseModel):
    """全局规则基础信息"""
    rule_type: RuleType
    rule_name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    rule_definition: Dict[str, Any]  # JSON格式
    scope: str = Field(default="global")  # 'global' 或 'domain'
    domain_id: Optional[UUID] = None  # 兼容旧版，保留单个域ID
    domain_ids: Optional[List[UUID]] = None  # scope='domain'时有值，支持多业务域
    priority: int = 0


class GlobalRuleCreate(GlobalRuleBase):
    """创建全局规则（connection_id 可选，为空时创建全局规则）"""
    connection_id: Optional[UUID] = None


class GlobalRuleUpdate(BaseModel):
    """更新全局规则"""
    rule_name: Optional[str] = None
    description: Optional[str] = None
    rule_definition: Optional[Dict[str, Any]] = None
    scope: Optional[str] = None
    domain_id: Optional[UUID] = None  # 兼容旧版
    domain_ids: Optional[List[UUID]] = None  # 支持多业务域
    priority: Optional[int] = None
    is_active: Optional[bool] = None


class GlobalRuleInDB(GlobalRuleBase):
    """数据库中的全局规则"""
    rule_id: UUID
    connection_id: Optional[UUID] = None  # 全局规则时为空
    is_active: bool
    created_at: datetime
    created_by: Optional[UUID]
    updated_at: datetime

    class Config:
        from_attributes = True


class GlobalRuleResponse(GlobalRuleBase):
    """全局规则响应"""
    rule_id: UUID
    connection_id: Optional[UUID] = None  # 全局规则时为空
    connection_name: Optional[str] = None  # 数据源名称（全局规则时为空）
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# 第九层：查询历史模型
# ============================================================================

class QueryHistoryCreate(BaseModel):
    """创建查询历史"""
    connection_id: UUID
    original_question: str
    generated_sql: Optional[str] = None
    intent_detection_result: Optional[Dict[str, Any]] = None


class QueryHistoryUpdate(BaseModel):
    """更新查询历史"""
    execution_status: Optional[ExecutionStatus] = None
    execution_time_ms: Optional[int] = None
    result_row_count: Optional[int] = None
    error_message: Optional[str] = None
    quality_score: Optional[float] = None
    user_feedback: Optional[str] = None


class QueryHistoryInDB(BaseModel):
    """数据库中的查询历史"""
    query_id: UUID
    connection_id: Optional[UUID]
    user_id: Optional[UUID]
    original_question: str
    generated_sql: Optional[str]
    intent_detection_result: Optional[Dict[str, Any]]
    execution_status: Optional[ExecutionStatus]
    execution_time_ms: Optional[int]
    result_row_count: Optional[int]
    error_message: Optional[str]
    quality_score: Optional[float]
    user_feedback: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class QueryHistoryResponse(BaseModel):
    """查询历史响应"""
    query_id: UUID
    connection_id: Optional[UUID]
    original_question: str
    generated_sql: Optional[str]
    execution_status: Optional[ExecutionStatus]
    execution_time_ms: Optional[int]
    result_row_count: Optional[int]
    quality_score: Optional[float]
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================================
# 辅助模型
# ============================================================================

class PaginationParams(BaseModel):
    """分页参数"""
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)


class PaginatedResponse(BaseModel):
    """分页响应"""
    total: int
    page: int
    page_size: int
    items: List[Any]

    @property
    def total_pages(self) -> int:
        return (self.total + self.page_size - 1) // self.page_size
