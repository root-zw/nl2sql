"""
查询复杂度路由器 - 混合架构核心组件

根据LLM在NL2IR阶段的判断智能选择处理流程：
1. 标准 IR 流程 - 普通查询，使用增强的 IR
2. 复杂拆分流程 - is_too_complex=true，使用 CoT + DAG 执行
3. 直接 SQL 流程 - requires_direct_sql=true，LLM 直接生成 SQL + 后处理

## 路由策略（重要）
完全依赖 LLM 在 NL2IR 解析阶段的判断，不使用关键字匹配：
1. requires_direct_sql=true → 直接SQL流程（CTE、递归、PIVOT等IR无法表达的场景）
2. is_too_complex=true → 复杂拆分流程（多步骤、顺序依赖等可拆解场景）
3. 其他 → 标准IR流程（包括增强IR功能如条件聚合、HAVING等）
"""

from typing import Optional, Literal
from dataclasses import dataclass, field
from typing import List
import structlog

from server.models.ir import IntermediateRepresentation

logger = structlog.get_logger()


@dataclass
class RoutingDecision:
    """路由决策结果"""
    route: Literal["standard_ir", "complex_split", "direct_sql"]
    confidence: float
    reason: str
    # 以下字段用于追踪和调试
    detected_features: List[str] = field(default_factory=list)
    fallback_route: Optional[str] = None


class QueryRouter:
    """
    查询复杂度路由器
    
    完全依赖 LLM 在 NL2IR 解析阶段输出的 IR 标记来决定路由。
    不使用程序化的关键字匹配或规则推断。
    """
    
    def __init__(
        self,
        enable_complex_split: bool = True,
        enable_direct_sql: bool = True,
    ):
        """
        初始化路由器
        
        Args:
            enable_complex_split: 是否启用复杂拆分流程（CoT + DAG）
            enable_direct_sql: 是否启用直接 SQL 流程
        """
        self.enable_complex_split = enable_complex_split
        self.enable_direct_sql = enable_direct_sql
    
    def route(
        self,
        question: str,
        ir: Optional[IntermediateRepresentation] = None
    ) -> RoutingDecision:
        """
        决定查询应该使用哪种处理流程
        
        路由逻辑完全基于 LLM 在 IR 中的标记：
        1. requires_direct_sql=true → direct_sql
        2. is_too_complex=true → complex_split
        3. 其他 → standard_ir（包括增强IR功能）
        
        Args:
            question: 用户问题文本（用于日志记录）
            ir: 已解析的 IR（必须提供以进行路由决策）
        
        Returns:
            RoutingDecision 对象
        """
        detected_features = []
        
        # 如果没有 IR，默认使用标准 IR 流程
        if ir is None:
            logger.debug("未提供IR，使用标准IR流程")
            return RoutingDecision(
                route="standard_ir",
                confidence=0.8,
                reason="未提供IR，使用默认标准流程",
                detected_features=[],
                fallback_route=None
            )
        
        # ======== 第1优先级：检查 requires_direct_sql 标记 ========
        # 适用于 IR 无法表达的查询（CTE、递归、PIVOT等）
        if getattr(ir, 'requires_direct_sql', False) and self.enable_direct_sql:
            direct_sql_reason = getattr(ir, 'direct_sql_reason', '未说明原因')
            detected_features.append("requires_direct_sql")
            
            logger.info(
                "路由决策：直接SQL流程",
                reason=direct_sql_reason,
                question_preview=question[:50] if question else ""
            )
            
            return RoutingDecision(
                route="direct_sql",
                confidence=0.95,
                reason=f"LLM标记需要直接SQL: {direct_sql_reason}",
                detected_features=detected_features,
                fallback_route="standard_ir"
            )
        
        # ======== 第2优先级：检查 is_too_complex 标记 ========
        # 适用于可拆解为多个子查询的复杂问题
        if getattr(ir, 'is_too_complex', False) and self.enable_complex_split:
            complexity_reason = getattr(ir, 'complexity_reason', '未说明原因')
            suggested_subquestions = getattr(ir, 'suggested_subquestions', [])
            detected_features.append("is_too_complex")
            
            logger.info(
                "路由决策：复杂拆分流程",
                reason=complexity_reason,
                subquestions_count=len(suggested_subquestions),
                question_preview=question[:50] if question else ""
            )
            
            return RoutingDecision(
                route="complex_split",
                confidence=0.9,
                reason=f"LLM标记问题复杂需拆解: {complexity_reason}",
                detected_features=detected_features,
                fallback_route="standard_ir"
            )
        
        # ======== 第3优先级：检测使用的增强IR功能（仅用于追踪） ========
        # 这些功能由编译器统一处理，不需要单独路由
        if getattr(ir, 'conditional_metrics', None):
            detected_features.append("conditional_metrics")
        if getattr(ir, 'having_filters', None):
            detected_features.append("having_filters")
        if getattr(ir, 'ratio_metrics', None):
            detected_features.append("ratio_metrics")
        if getattr(ir, 'calculated_fields', None):
            detected_features.append("calculated_fields")
        if getattr(ir, 'comparison_type', None):
            detected_features.append(f"comparison:{ir.comparison_type}")
        if getattr(ir, 'cumulative_metrics', None):
            detected_features.append("cumulative_metrics")
        if getattr(ir, 'moving_average_metrics', None):
            detected_features.append("moving_average")
        if getattr(ir, 'cross_partition_query', False):
            detected_features.append("cross_partition")
        
        # ======== 默认：标准 IR 流程 ========
        # 包括基础查询和所有增强IR功能
        feature_desc = ""
        if detected_features:
            feature_desc = f"（使用增强功能: {', '.join(detected_features)}）"
        
        logger.debug(
            "路由决策：标准IR流程",
            detected_features=detected_features,
            question_preview=question[:50] if question else ""
        )
        
        return RoutingDecision(
            route="standard_ir",
            confidence=0.9,
            reason=f"使用标准IR流程{feature_desc}",
            detected_features=detected_features,
            fallback_route=None
        )
    
    def should_use_direct_sql(
        self,
        ir: Optional[IntermediateRepresentation]
    ) -> tuple:
        """
        快速判断是否应该使用直接 SQL 生成
        
        Args:
            ir: 已解析的 IR
        
        Returns:
            (should_use: bool, reason: str) 元组
        """
        if not self.enable_direct_sql:
            return False, "直接SQL生成未启用"
        
        if ir is None:
            return False, "未提供IR"
        
        if getattr(ir, 'requires_direct_sql', False):
            reason = getattr(ir, 'direct_sql_reason', 'LLM标记需要直接SQL')
            return True, reason
        
        return False, "不需要直接SQL"
    
    def should_use_complex_split(
        self,
        ir: Optional[IntermediateRepresentation]
    ) -> tuple:
        """
        快速判断是否应该使用复杂拆分流程（CoT + DAG）
        
        Args:
            ir: 已解析的 IR
        
        Returns:
            (should_use: bool, reason: str, suggested_subquestions: list) 元组
        """
        if not self.enable_complex_split:
            return False, "复杂拆分流程未启用", []
        
        if ir is None:
            return False, "未提供IR", []
        
        if getattr(ir, 'is_too_complex', False):
            reason = getattr(ir, 'complexity_reason', 'LLM标记问题过于复杂')
            subquestions = getattr(ir, 'suggested_subquestions', [])
            return True, reason, subquestions
        
        return False, "不需要拆分", []


def get_query_router(
    enable_complex_split: bool = True,
    enable_direct_sql: bool = True
) -> QueryRouter:
    """获取查询路由器实例"""
    return QueryRouter(
        enable_complex_split=enable_complex_split,
        enable_direct_sql=enable_direct_sql
    )
