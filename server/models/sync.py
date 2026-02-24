"""
Milvus自动同步相关数据模型
"""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from uuid import UUID
from pydantic import BaseModel


class SyncType(str, Enum):
    """同步类型"""
    FULL = "full"           # 全量同步
    INCREMENTAL = "incremental"  # 增量同步
    ENUMS = "enums"         # 枚举值同步


class TriggeredBy(str, Enum):
    """触发方式"""
    AUTO = "auto"           # 自动触发
    MANUAL = "manual"       # 手动触发


class SyncStatus(str, Enum):
    """同步状态"""
    PENDING = "pending"     # 待执行
    RUNNING = "running"     # 执行中
    COMPLETED = "completed" # 已完成
    FAILED = "failed"       # 失败
    CANCELLED = "cancelled" # 已取消


class EntitySyncStatus(str, Enum):
    """实体同步状态"""
    PENDING = "pending"     # 待同步
    SYNCING = "syncing"     # 同步中
    SYNCED = "synced"       # 已同步
    FAILED = "failed"       # 同步失败


class EntityType(str, Enum):
    """实体类型"""
    DOMAIN = "domain"       # 业务域
    TABLE = "table"         # 数据表
    FIELD = "field"         # 字段
    ENUM = "enum"           # 枚举值
    FEW_SHOT = "few_shot"   # Few-Shot SQL 问答样本


class OperationType(str, Enum):
    """操作类型"""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass
class PendingChange:
    """待同步变更"""
    change_id: UUID
    connection_id: UUID
    entity_type: EntityType
    entity_id: UUID
    operation: OperationType

    old_data: Optional[Dict[str, Any]] = None
    new_data: Optional[Dict[str, Any]] = None

    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    sync_status: EntitySyncStatus = EntitySyncStatus.PENDING
    sync_id: Optional[UUID] = None
    synced_at: Optional[datetime] = None

    priority: int = 5  # 1-10, 数字越小优先级越高


@dataclass
class SyncHistory:
    """同步历史记录"""
    sync_id: UUID
    connection_id: UUID
    sync_type: SyncType
    triggered_by: TriggeredBy

    status: SyncStatus = SyncStatus.PENDING
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None

    # 统计信息
    total_entities: int = 0
    synced_entities: int = 0
    failed_entities: int = 0

    # 同步内容
    entity_changes: Optional[List[Dict[str, Any]]] = field(default_factory=list)
    sync_config: Optional[Dict[str, Any]] = field(default_factory=dict)

    # 错误信息
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

    # 进度跟踪
    current_step: Optional[str] = None
    progress_percentage: int = 0

    created_by: Optional[UUID] = None


@dataclass
class SyncConfig:
    """同步配置"""
    config_id: UUID
    connection_id: UUID

    # 自动同步配置
    auto_sync_enabled: bool = True
    auto_sync_mode: str = "auto"
    auto_sync_domains: bool = True
    auto_sync_tables: bool = True
    auto_sync_fields: bool = True
    auto_sync_enums: bool = True
    auto_sync_few_shot: bool = True
    inherits_global: bool = False  # 是否沿用最近一次全局模板
    global_setting_id: Optional[UUID] = None  # 最近一次应用的全局模板标识

    # 同步策略配置
    batch_window_seconds: int = 5      # 批量变更合并窗口（秒）
    max_batch_size: int = 100         # 最大批量大小
    sync_timeout_seconds: int = 300   # 同步超时时间

    # 优先级配置
    domain_priority: int = 1          # 业务域变更优先级
    table_priority: int = 2           # 表变更优先级
    field_priority: int = 3           # 字段变更优先级
    enum_priority: int = 4            # 枚举值变更优先级

    # 同步频率限制
    min_sync_interval_seconds: int = 60  # 最小同步间隔

    # 错误处理配置
    max_retry_attempts: int = 3
    retry_delay_seconds: int = 10

    # 元数据
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[UUID] = None


@dataclass
class EntityChange:
    """实体变更信息"""
    entity_type: EntityType
    entity_id: UUID
    operation: OperationType
    old_data: Optional[Dict[str, Any]] = None
    new_data: Optional[Dict[str, Any]] = None

    # 变更时间戳
    changed_at: datetime = field(default_factory=datetime.utcnow)

    # 是否影响Milvus向量
    affects_vector: bool = True


class ManualSyncRequest(BaseModel):
    """手动同步请求"""
    connection_id: UUID

    # 同步选项
    sync_domains: bool = True
    sync_tables: bool = True
    sync_fields: bool = True  # 同步字段选项
    sync_enums: bool = True
    sync_few_shot: bool = True

    # 是否强制全量同步
    force_full_sync: bool = False

    # 指定同步的实体（可选）
    domain_ids: Optional[List[UUID]] = None
    table_ids: Optional[List[UUID]] = None
    field_ids: Optional[List[UUID]] = None

    # 同步配置
    dry_run: bool = False  # 仅检查，不执行同步


@dataclass
class SyncProgress:
    """同步进度"""
    sync_id: UUID
    connection_id: UUID
    started_at: datetime

    # 进度信息
    current_step: str
    progress_percentage: int = 0

    # 详细进度
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0

    # 时间信息
    estimated_completion: Optional[datetime] = None

    # 状态信息
    status: SyncStatus = SyncStatus.RUNNING
    message: Optional[str] = None


@dataclass
class PendingChangesStats:
    """待同步变更统计"""
    connection_id: UUID
    entity_type: EntityType
    earliest_change: datetime
    latest_change: datetime
    pending_count: int = 0


@dataclass
class SyncStats:
    """同步统计"""
    # 基础统计
    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0

    # 时间统计
    avg_duration_seconds: float = 0.0
    last_sync_time: Optional[datetime] = None

    # 实体统计
    total_entities: int = 0
    synced_entities: int = 0

    # 待同步统计
    pending_changes: List[PendingChangesStats] = field(default_factory=list)

    # 健康状态
    health_score: float = 1.0  # 0-1, 基于成功率和延迟时间计算


@dataclass
class SyncHealthStatus:
    """同步健康状态"""
    connection_id: UUID

    # 配置状态
    auto_sync_enabled: bool
    milvus_connected: bool
    embedding_available: bool
    collection_ready: bool = True

    # 同步状态
    last_sync_status: Optional[SyncStatus] = None
    last_sync_time: Optional[datetime] = None

    # 待同步统计
    pending_changes_count: int = 0
    oldest_pending_change: Optional[datetime] = None

    # 系统状态
    is_syncing: bool = False
    current_sync_id: Optional[UUID] = None

    # 健康评分
    health_score: float = 1.0  # 0-1
    health_message: str = "健康"

    # 告警级别
    alert_level: str = "normal"  # normal, warning, error, critical


# API请求/响应模型
class AutoSyncRequest(BaseModel):
    """自动同步请求"""
    connection_id: UUID
    entity_changes: List[EntityChange]


class AutoSyncResponse(BaseModel):
    """自动同步响应"""
    success: bool
    sync_id: UUID
    message: str
    stats: Dict[str, Any]


class GetPendingChangesRequest(BaseModel):
    """获取待同步变更请求"""
    connection_id: UUID
    entity_types: Optional[List[EntityType]] = None
    limit: int = 100
