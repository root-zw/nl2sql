"""查询复杂度分析模块

基于 LLM 在 IR 中的标记进行路由决策，不使用程序化的关键字匹配。

路由策略：
1. requires_direct_sql=true → direct_sql（直接SQL生成）
2. is_too_complex=true → complex_split（CoT + DAG 执行）
3. 其他 → standard_ir（包括增强IR功能）
"""

from .evaluator import HybridComplexityEvaluator, get_complexity_evaluator
from .router import QueryRouter, RoutingDecision, get_query_router

__all__ = [
    "HybridComplexityEvaluator",
    "get_complexity_evaluator",
    "QueryRouter",
    "RoutingDecision",
    "get_query_router"
]
