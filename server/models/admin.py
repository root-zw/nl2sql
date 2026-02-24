"""管理系统的Pydantic模型定义"""

from datetime import datetime
from typing import Optional, List
from uuid import UUID
from pydantic import BaseModel, EmailStr, Field, validator
from server.utils.password_validator import validate_password_strength


# ============================================================
# 用户相关模型
# ============================================================

class UserRole:
    """系统角色常量（直接使用数据库值）"""
    ADMIN = "admin"              # 系统管理员 - 所有功能
    DATA_ADMIN = "data_admin"    # 数据管理员 - 数据库连接/元数据/同步/数据权限
    USER = "user"                # 普通用户 - 仅查询，不能登录后台


class UserBase(BaseModel):
    """用户基础信息"""
    username: str = Field(..., min_length=1, max_length=50)  # 中文姓名可能只有 2 个字符
    email: Optional[EmailStr] = None  # 邮箱可选
    full_name: Optional[str] = Field(None, max_length=100)
    role: str = Field(default=UserRole.USER)
    is_active: bool = True
    
    @validator('role')
    def validate_role(cls, v):
        # 直接使用数据库角色值
        valid_roles = [UserRole.ADMIN, UserRole.DATA_ADMIN, UserRole.USER]
        if v not in valid_roles:
            raise ValueError(f'角色必须是: admin, data_admin, user')
        return v


class UserCreate(UserBase):
    """创建用户请求"""
    password: str = Field(..., min_length=8)
    
    @validator('password')
    def validate_password(cls, v):
        is_valid, error_msg = validate_password_strength(v)
        if not is_valid:
            raise ValueError(error_msg)
        return v


class UserUpdate(BaseModel):
    """更新用户请求"""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    role: Optional[str] = None
    tenant_id: Optional[str] = None
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=8)
    
    @validator('password')
    def validate_password(cls, v):
        if v is not None:
            is_valid, error_msg = validate_password_strength(v)
            if not is_valid:
                raise ValueError(error_msg)
        return v


class User(UserBase):
    """用户完整信息（响应）"""
    user_id: UUID
    created_at: datetime
    updated_at: datetime
    last_login: Optional[datetime] = None

    class Config:
        from_attributes = True


class UserInDB(User):
    """数据库中的用户（包含密码哈希）"""
    password_hash: str


# ============================================================
# 认证相关模型
# ============================================================

class LoginRequest(BaseModel):
    """登录请求"""
    username: str
    password: str


class Token(BaseModel):
    """JWT Token响应"""
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # 秒


class TokenData(BaseModel):
    """Token解析后的数据"""
    user_id: UUID
    username: str
    role: str


class LoginResponse(BaseModel):
    """登录响应"""
    token: Token
    user: User


# ============================================================
# 会话相关模型
# ============================================================

class Session(BaseModel):
    """用户会话"""
    session_id: str
    user_id: int
    expires_at: datetime
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    created_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================
# 同步任务相关模型
# ============================================================

class SyncTarget:
    """同步目标常量"""
    MILVUS = "milvus"


class SyncMode:
    """同步模式常量"""
    FULL = "full"  # 全量
    INCREMENTAL = "incremental"  # 增量


class SyncStatus:
    """同步状态常量"""
    PENDING = "pending"  # 待执行
    RUNNING = "running"  # 执行中
    SUCCESS = "success"  # 成功
    FAILED = "failed"  # 失败


class SyncRequest(BaseModel):
    """触发同步请求"""
    tenant_id: str
    target: str = Field(..., description="同步目标: milvus")
    mode: str = Field(default=SyncMode.FULL, description="同步模式: full 或 incremental")
    
    @validator('target')
    def validate_target(cls, v):
        if v != SyncTarget.MILVUS:
            raise ValueError(f'同步目标必须是: {SyncTarget.MILVUS}')
        return v

    @validator('mode')
    def validate_mode(cls, v):
        if v not in [SyncMode.FULL, SyncMode.INCREMENTAL]:
            raise ValueError(f'同步模式必须是: {SyncMode.FULL} 或 {SyncMode.INCREMENTAL}')
        return v


class MilvusSyncRequest(BaseModel):
    """Milvus同步请求体"""
    sync_domains: bool = True
    sync_tables: bool = True
    sync_fields: bool = True
    incremental: bool = False


class MilvusSyncEnumsRequest(BaseModel):
    """Milvus枚举同步请求体"""
    field_ids: Optional[List[UUID]] = None
    force_full_sync: bool = False


class MilvusFewShotSyncRequest(BaseModel):
    """Few-Shot 样本同步请求体"""
    min_quality_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    limit: Optional[int] = Field(default=None, ge=1, le=1000)
    domain_ids: Optional[List[UUID]] = None
    only_verified: bool = False
    include_inactive: bool = False


class SyncStatusResponse(BaseModel):
    """同步状态响应"""
    sync_id: int
    tenant_id: str
    sync_target: str
    sync_mode: str
    status: str
    total_items: int
    synced_items: int
    failed_items: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    triggered_by: Optional[int] = None
    triggered_at: datetime
    
    class Config:
        from_attributes = True


class SyncHistoryItem(BaseModel):
    """同步历史记录项"""
    sync_id: int
    sync_target: str
    sync_mode: str
    status: str
    triggered_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    total_items: int
    synced_items: int
    failed_items: int
    
    class Config:
        from_attributes = True


# ============================================================
# 配置草稿相关模型
# ============================================================

class DraftStatus:
    """草稿状态常量"""
    DRAFT = "draft"  # 草稿
    PENDING_REVIEW = "pending_review"  # 待审核
    APPROVED = "approved"  # 已批准
    REJECTED = "rejected"  # 已拒绝


class DraftCreate(BaseModel):
    """创建草稿请求"""
    tenant_id: str
    config_type: str  # sources, dimensions, measures, metrics, join_paths
    record_id: Optional[str] = None  # 如果是新建则为None
    draft_data: dict  # 草稿数据（JSON）


class DraftUpdate(BaseModel):
    """更新草稿请求"""
    draft_data: dict
    status: Optional[str] = None


class Draft(BaseModel):
    """草稿完整信息"""
    draft_id: int
    tenant_id: str
    config_type: str
    record_id: Optional[str] = None
    draft_data: dict
    status: str
    created_by: Optional[int] = None
    created_at: datetime
    updated_by: Optional[int] = None
    updated_at: datetime
    reviewed_by: Optional[int] = None
    reviewed_at: Optional[datetime] = None
    review_comment: Optional[str] = None
    
    class Config:
        from_attributes = True


# ============================================================
# 数据源管理相关模型（扩展）
# ============================================================

class SourceListItem(BaseModel):
    """数据源列表项（简化版）"""
    source_id: str
    display_name: str
    schema_name: str
    table_name: str
    description: Optional[str] = None
    column_count: Optional[int] = None
    domain_id: Optional[str] = None  # 所属业务域ID
    updated_at: datetime
    
    class Config:
        from_attributes = True


class SourceDetail(BaseModel):
    """数据源详情（完整版）"""
    source_id: str
    schema_name: str
    table_name: str
    display_name: str
    description: Optional[str] = None
    columns: List[dict]
    primary_key: Optional[List[str]] = None
    detail_view: Optional[dict] = None
    table_identity: Optional[dict] = None
    time_field: Optional[str] = None
    partition_by: Optional[str] = None
    freshness_sla_minutes: Optional[int] = None
    owner: Optional[str] = None
    sensitive: bool = False
    tags: Optional[List[str]] = []
    version: int
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    class Config:
        from_attributes = True


# ============================================================
# 数据源管理 - 新建/更新请求体
# ============================================================

class SourceUpsert(BaseModel):
    """新建/更新数据源的请求体"""
    source_id: str
    schema_name: str
    table_name: str
    display_name: str
    description: Optional[str] = None
    columns: List[dict] = []
    primary_key: Optional[List[str]] = None
    detail_view: Optional[dict] = None
    table_identity: Optional[dict] = None
    time_field: Optional[str] = None
    partition_by: Optional[str] = None
    freshness_sla_minutes: Optional[int] = None
    owner: Optional[str] = None
    sensitive: bool = False
    tags: Optional[List[str]] = []

# ============================================================
# 指标管理相关模型（扩展）
# ============================================================

class MetricListItem(BaseModel):
    """指标列表项（简化版）"""
    metric_id: str
    display_name: str
    description: Optional[str] = None
    category: Optional[str] = None
    unit: Optional[str] = None
    updated_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================
# 通用响应模型
# ============================================================

class MessageResponse(BaseModel):
    """通用消息响应"""
    message: str
    detail: Optional[str] = None


class ErrorResponse(BaseModel):
    """错误响应"""
    error: str
    detail: Optional[str] = None
    code: Optional[str] = None


class PaginatedResponse(BaseModel):
    """分页响应"""
    items: List[BaseModel]
    total: int
    page: int
    page_size: int
    total_pages: int


# ============================================================
# 统计信息模型
# ============================================================

class TenantStats(BaseModel):
    """租户统计信息"""
    tenant_id: str
    tenant_name: str
    sources_count: int
    dimensions_count: int
    measures_count: int
    metrics_count: int
    joins_count: int
    last_sync_at: Optional[datetime] = None
    has_unsynced_changes: bool = False


class DashboardStats(BaseModel):
    """仪表板统计信息"""
    total_users: int
    active_users: int
    total_tenants: int
    total_sources: int
    total_metrics: int
    pending_syncs: int
    recent_activities: List[dict] = []
