"""模型供应商相关的Pydantic模型"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class ProviderType(str, Enum):
    """供应商类型"""
    OPENAI_COMPATIBLE = "openai_compatible"
    AZURE = "azure"
    OLLAMA = "ollama"
    CUSTOM = "custom"


class ModelType(str, Enum):
    """模型类型"""
    LLM = "llm"
    EMBEDDING = "embedding"
    RERANK = "rerank"


class LLMScenario(str, Enum):
    """模型使用场景"""
    # LLM 场景
    DEFAULT = "default"
    TABLE_SELECTION = "table_selection"
    NL2IR = "nl2ir"
    DIRECT_SQL = "direct_sql"
    NARRATIVE = "narrative"
    VECTOR_SELECTOR = "vector_selector"
    # Embedding 场景
    EMBEDDING = "embedding"
    # Rerank 场景
    RERANK = "rerank"


# ============================================================================
# 供应商相关模型
# ============================================================================

class ProviderBase(BaseModel):
    """供应商基础模型"""
    provider_name: str = Field(..., max_length=100, description="供应商唯一标识")
    display_name: str = Field(..., max_length=100, description="显示名称")
    provider_type: ProviderType = Field(..., description="供应商类型")
    base_url: Optional[str] = Field(None, max_length=500, description="API基础URL")
    icon: Optional[str] = Field(None, max_length=100, description="图标标识")
    description: Optional[str] = Field(None, description="供应商描述")


class ProviderCreate(ProviderBase):
    """创建供应商"""
    pass


class ProviderUpdate(BaseModel):
    """更新供应商"""
    display_name: Optional[str] = Field(None, max_length=100)
    base_url: Optional[str] = Field(None, max_length=500)
    icon: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None


class ProviderInfo(ProviderBase):
    """供应商信息（响应）"""
    provider_id: UUID
    is_enabled: bool = True
    is_valid: bool = False
    last_validated_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    # 统计信息
    credential_count: int = 0
    model_count: int = 0
    
    class Config:
        from_attributes = True


class ProviderDetail(ProviderInfo):
    """供应商详情（包含凭证和模型）"""
    credentials: List["CredentialInfo"] = []
    models: List["ModelInfo"] = []


# ============================================================================
# 凭证相关模型
# ============================================================================

class CredentialBase(BaseModel):
    """凭证基础模型"""
    credential_name: str = Field(..., max_length=100, description="凭证名称")
    extra_config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="额外配置")


class CredentialCreate(CredentialBase):
    """创建凭证"""
    api_key: str = Field(..., min_length=1, description="API Key")


class CredentialUpdate(BaseModel):
    """更新凭证"""
    credential_name: Optional[str] = Field(None, max_length=100)
    api_key: Optional[str] = Field(None, min_length=1, description="API Key（不传则不更新）")
    extra_config: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class CredentialInfo(CredentialBase):
    """凭证信息（响应）"""
    credential_id: UUID
    provider_id: UUID
    is_active: bool = True
    is_default: bool = False
    total_requests: int = 0
    total_tokens: int = 0
    last_used_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    # API Key 脱敏显示
    api_key_masked: str = ""
    
    class Config:
        from_attributes = True


# ============================================================================
# 模型相关
# ============================================================================

class ModelBase(BaseModel):
    """模型基础信息"""
    model_name: str = Field(..., max_length=100, description="模型名称")
    display_name: Optional[str] = Field(None, max_length=100, description="显示名称")
    model_type: ModelType = Field(..., description="模型类型")
    
    # 模型特性
    supports_function_calling: bool = Field(default=False, description="是否支持函数调用")
    supports_json_mode: bool = Field(default=False, description="是否支持JSON模式")
    supports_streaming: bool = Field(default=True, description="是否支持流式输出")
    supports_vision: bool = Field(default=False, description="是否支持视觉")
    context_window: Optional[int] = Field(None, description="上下文窗口大小")
    max_output_tokens: Optional[int] = Field(None, description="最大输出Token数")
    
    # 默认参数
    default_temperature: float = Field(default=0.0, ge=0, le=2)
    default_top_p: float = Field(default=1.0, ge=0, le=1)
    default_max_tokens: int = Field(default=2048, ge=1)


class ModelCreate(ModelBase):
    """创建模型"""
    pass


class ModelUpdate(BaseModel):
    """更新模型"""
    display_name: Optional[str] = Field(None, max_length=100)
    supports_function_calling: Optional[bool] = None
    supports_json_mode: Optional[bool] = None
    supports_streaming: Optional[bool] = None
    supports_vision: Optional[bool] = None
    context_window: Optional[int] = None
    max_output_tokens: Optional[int] = None
    default_temperature: Optional[float] = Field(None, ge=0, le=2)
    default_top_p: Optional[float] = Field(None, ge=0, le=1)
    default_max_tokens: Optional[int] = Field(None, ge=1)
    is_enabled: Optional[bool] = None


class ModelInfo(ModelBase):
    """模型信息（响应）"""
    model_id: UUID
    provider_id: UUID
    is_enabled: bool = True
    is_custom: bool = False
    created_at: datetime
    updated_at: datetime
    
    # 关联的供应商信息（可选）
    provider_name: Optional[str] = None
    provider_display_name: Optional[str] = None
    
    class Config:
        from_attributes = True


# ============================================================================
# 场景配置相关
# ============================================================================

class ScenarioConfigBase(BaseModel):
    """场景配置基础模型"""
    scenario: LLMScenario = Field(..., description="场景名称")
    model_id: Optional[UUID] = Field(None, description="模型ID")
    credential_id: Optional[UUID] = Field(None, description="凭证ID")
    
    # 参数配置
    temperature: Optional[float] = Field(None, ge=0, le=2)
    top_p: Optional[float] = Field(None, ge=0, le=1)
    max_tokens: Optional[int] = Field(None, ge=1)
    timeout_seconds: int = Field(default=60, ge=1)
    max_retries: int = Field(default=2, ge=0)
    
    # 额外配置
    extra_params: Optional[Dict[str, Any]] = Field(default_factory=dict)


class ScenarioConfigCreate(ScenarioConfigBase):
    """创建场景配置"""
    priority: int = Field(default=0, ge=0)


class ScenarioConfigUpdate(BaseModel):
    """更新场景配置"""
    model_id: Optional[UUID] = None
    credential_id: Optional[UUID] = None
    temperature: Optional[float] = Field(None, ge=0, le=2)
    top_p: Optional[float] = Field(None, ge=0, le=1)
    max_tokens: Optional[int] = Field(None, ge=1)
    timeout_seconds: Optional[int] = Field(None, ge=1)
    max_retries: Optional[int] = Field(None, ge=0)
    extra_params: Optional[Dict[str, Any]] = None
    is_enabled: Optional[bool] = None


class ScenarioConfigInfo(ScenarioConfigBase):
    """场景配置信息（响应）"""
    config_id: Optional[UUID] = None  # 未配置时为 None
    priority: int = 0
    is_enabled: bool = True
    created_at: Optional[datetime] = None  # 未配置时为 None
    updated_at: Optional[datetime] = None  # 未配置时为 None
    
    # 关联模型信息
    model_name: Optional[str] = None
    model_display_name: Optional[str] = None
    provider_name: Optional[str] = None
    provider_display_name: Optional[str] = None
    
    class Config:
        from_attributes = True


# ============================================================================
# 可用模型列表（给前端选择器使用）
# ============================================================================

class AvailableModel(BaseModel):
    """可用模型（用于前端选择器）"""
    model_id: UUID
    model_name: str
    display_name: Optional[str] = None
    model_type: ModelType
    
    # 供应商信息
    provider_id: UUID
    provider_name: str
    provider_display_name: str
    
    # 特性标签
    features: List[str] = []  # e.g., ["function_calling", "json_mode", "vision"]
    
    # 是否有有效凭证
    has_valid_credential: bool = False


class AvailableModelsResponse(BaseModel):
    """可用模型列表响应"""
    models: List[AvailableModel]
    total: int


# ============================================================================
# 预置供应商模板
# ============================================================================

class PresetProviderModel(BaseModel):
    """预置模型定义"""
    name: str
    display_name: Optional[str] = None
    type: ModelType
    supports_function_calling: bool = False
    supports_json_mode: bool = False
    supports_vision: bool = False
    context_window: Optional[int] = None


class PresetProvider(BaseModel):
    """预置供应商定义"""
    name: str
    display_name: str
    type: ProviderType
    icon: str
    default_base_url: Optional[str] = None
    description: Optional[str] = None
    models: List[PresetProviderModel] = []


# Forward references
ProviderDetail.model_rebuild()

