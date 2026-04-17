"""API 请求响应模型"""

from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field
from .ir import IntermediateRepresentation


class QueryRequest(BaseModel):
    """查询请求"""
    text: Optional[str] = None  # 自然语言问题
    ir: Optional[IntermediateRepresentation] = None  # 或直接传 IR
    
    #: 数据库连接（可选，为空时自动检测）
    connection_id: Optional[str] = None  # 数据库连接ID，为空时根据检索结果自动确定
    
    # 上下文
    user_id: str
    role: str = "viewer"
    session_id: Optional[str] = None
    
    # 业务域（可选）
    domain_id: Optional[str] = None  # 用户指定的业务域ID
    
    # 表选择确认（用户从确认卡中选择的表）
    # 支持单表选择（向后兼容）和多表选择
    selected_table_id: Optional[str] = None  # 单表选择（向后兼容）
    selected_table_ids: Optional[List[str]] = None  # 多表选择
    multi_table_mode: Optional[str] = None  # 多表查询模式（compare/union/multi_join）
    
    # 关联上一次请求（用于追踪LLM表选择→用户确认的完整流程）
    original_query_id: Optional[str] = None
    
    # 选项
    skip_cache: bool = False
    force_execute: bool = False  # 跳过成本检查
    explain_only: bool = False   # 只返回 SQL 不执行
    
    # 叙述生成控制（供调用方覆盖全局配置，例如 Dify 工具调用时关闭内部叙述）
    disable_narrative: bool = False
    
    # 多轮对话会话相关
    conversation_id: Optional[str] = None  # 会话ID
    message_id: Optional[str] = None  # 消息ID（由前端生成，用于追踪）
    context_depth: Optional[int] = None  # 上下文深度（覆盖默认值）
    
    def get_selected_table_ids(self) -> List[str]:
        """获取所有选中的表ID（兼容单表和多表模式）"""
        if self.selected_table_ids:
            return self.selected_table_ids
        if self.selected_table_id:
            return [self.selected_table_id]
        return []


class TableCandidate(BaseModel):
    """表候选项（LLM表选择结果）"""
    table_id: str
    table_name: str  # display_name
    description: Optional[str] = None
    confidence: float = Field(ge=0, le=1, description="LLM给出的置信度 0-1")
    reason: str = ""  # LLM给出的选择理由
    tags: List[str] = []  # 语义标签
    key_dimensions: List[str] = []  # 关键维度字段预览
    key_measures: List[str] = []  # 关键度量字段预览
    domain_name: Optional[str] = None  # 所属业务域
    domain_id: Optional[str] = None  # 业务域ID
    data_year: Optional[str] = None  # 数据年份（用于跨年查询识别）


class TableSelectionCard(BaseModel):
    """表选择确认卡"""
    candidates: List[TableCandidate]  # 候选表列表（按置信度降序）
    question: str  # 原始问题
    message: str = "系统找到了多个可能相关的表，请确认您要查询的是哪张表："
    
    # 确认原因说明（告诉用户为什么需要确认）
    confirmation_reason: Optional[str] = None  # 如 "存在多个相似的表"、"置信度不够高"
    
    # 控制选项
    allow_multi_select: bool = False  # 是否允许多选（多表联合查询）
    multi_table_mode: Optional[str] = None  # 多表查询模式（compare/union/multi_join）
    allow_cancel: bool = True
    
    # 分批展示控制（实际值由后端根据配置填充）
    page_size: int = 0  # 每页展示数量（由后端根据 LLM_TABLE_SELECTION_PAGE_SIZE 填充）
    total_candidates: int = 0  # 总候选数量（用于前端判断是否有更多）
    
    # 跨年查询提示
    is_cross_year_query: bool = False  # 是否是跨年对比查询
    cross_year_hint: Optional[str] = None  # 跨年查询提示信息

    # LLM 推荐的表ID列表（用于前端预选，而不是全选）
    recommended_table_ids: List[str] = []


class ConfirmationCard(BaseModel):
    """确认卡（IR生成后的意图确认）"""
    ir: IntermediateRepresentation
    natural_language: str  # 意图复述
    
    # 可调整的 Chips
    suggestions: List[Dict[str, Any]] = []  # 如 [{"label": "改为最近7天", "modify": {"time.last_n": 7}}]
    
    # 风险提示
    warnings: List[str] = []  # 如 ["查询跨度较大，可能耗时较长"]
    estimated_cost: Optional[Dict[str, Any]] = None  # {"rows": 1000000, "seconds": 15}


class QueryResult(BaseModel):
    """查询结果"""
    columns: List[Dict[str, str]]  # [{"name": "category", "type": "string"}, ...]
    rows: List[List[Any]]
    
    # 元数据
    meta: Dict[str, Any]  # sql, ir, latency_ms, cache_hit, dialect, cost
    
    # 可选：自然语言总结
    summary: Optional[str] = None
    visualization_hint: Optional[Literal["table", "bar", "line", "pie"]] = None


class AuthStatus(BaseModel):
    """认证状态信息"""
    authenticated: bool = False
    auth_attempted: bool = False  # 是否尝试了认证（传入了token）
    error_code: Optional[int] = None  # 错误代码（如 10104）
    provider: Optional[str] = None  # 使用的认证提供者


class QueryResponse(BaseModel):
    """查询接口响应"""
    status: Literal["success", "confirm_needed", "table_selection_needed", "error"]
    
    # 成功时
    result: Optional[QueryResult] = None
    
    # 需要确认IR时
    confirmation: Optional[ConfirmationCard] = None
    
    # 需要确认表选择时
    table_selection: Optional[TableSelectionCard] = None
    
    # 错误时
    error: Optional[Dict[str, Any]] = None  # {"code": "PARSE_ERROR", "message": "..."}
    
    # 认证状态
    auth_status: Optional[AuthStatus] = None
    
    # 通用
    query_id: str
    timestamp: str


class QuerySessionActionRequest(BaseModel):
    """查询会话动作请求"""
    action_type: Optional[Literal[
        "confirm",
        "revise",
        "change_table",
        "choose_option",
        "request_explanation",
        "execution_decision",
        "exit_current",
    ]] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    natural_language_reply: Optional[str] = None
    draft_version: Optional[int] = None
    actor_type: str = "user"
    actor_id: str = "anonymous"
    idempotency_key: Optional[str] = None


class AccessibleTableItem(BaseModel):
    """用户可访问的表信息（用于展开全部功能）"""
    table_id: str
    table_name: str  # display_name
    schema_name: Optional[str] = None
    description: Optional[str] = None
    connection_id: str
    connection_name: str
    domain_id: Optional[str] = None
    domain_name: Optional[str] = None
    data_year: Optional[str] = None
    tags: List[str] = []
    key_dimensions: List[str] = []  # 关键维度字段预览
    key_measures: List[str] = []  # 关键度量字段预览


class AccessibleTablesResponse(BaseModel):
    """用户可访问表列表响应"""
    tables: List[AccessibleTableItem]
    total: int
    is_admin: bool = False
    has_all_access: bool = False
