"""
混合复杂度评估器
基于 LLM 在 IR 中的标记评估查询复杂度

评估逻辑完全依赖 LLM 的判断，不使用程序化规则：
- is_too_complex=true → 复杂（complex）
- requires_direct_sql=true → 复杂（complex）
- 使用增强IR功能 → 中等（medium）
- 其他 → 简单（simple）
"""

from typing import Tuple, List
import structlog
from server.models.ir import IntermediateRepresentation

logger = structlog.get_logger()


class HybridComplexityEvaluator:
    """
    混合复杂度评估器
    完全基于 LLM 在 IR 中的标记评估查询复杂度
    
    评分范围: 0.0 - 1.0
    - 0.0 - 0.3: 简单 (simple) → 标准IR流程
    - 0.3 - 0.7: 中等 (medium) → 增强IR流程（编译器统一处理）
    - 0.7 - 1.0: 复杂 (complex) → CoT + DAG 或 直接SQL
    """

    def evaluate(
        self,
        ir: IntermediateRepresentation,
        question: str = ""
    ) -> Tuple[float, str, str]:
        """
        评估复杂度
        
        Args:
            ir: 中间表示
            question: 原始问题（用于日志）
        
        Returns:
            (score, level, reason)
            score: 0.0-1.0
            level: "simple" | "medium" | "complex"
            reason: 复杂度原因说明
        """
        reasons: List[str] = []
        
        # ======== 第1优先级：检查 LLM 的复杂性标记 ========
        
        # requires_direct_sql → 直接标记为复杂
        if getattr(ir, 'requires_direct_sql', False):
            direct_sql_reason = getattr(ir, 'direct_sql_reason', 'IR无法表达')
            reasons.append(f"需要直接SQL: {direct_sql_reason}")
            
            logger.debug(
                "复杂度评估：复杂（直接SQL）",
                reason=direct_sql_reason,
                question_preview=question[:30] if question else ""
            )
            
            return 0.9, "complex", " | ".join(reasons)
        
        # is_too_complex → 直接标记为复杂
        if getattr(ir, 'is_too_complex', False):
            complexity_reason = getattr(ir, 'complexity_reason', '问题过于复杂')
            reasons.append(f"LLM标记复杂: {complexity_reason}")
            
            logger.debug(
                "复杂度评估：复杂（需拆分）",
                reason=complexity_reason,
                question_preview=question[:30] if question else ""
            )
            
            return 0.85, "complex", " | ".join(reasons)
        
        # ======== 第2优先级：检测增强IR功能使用情况 ========
        # 这些不影响路由，但用于评估和追踪
        
        score = 0.0
        
        # 条件聚合
        if getattr(ir, 'conditional_metrics', None):
            score += 0.2
            reasons.append("条件聚合")
        
        # HAVING过滤
        if getattr(ir, 'having_filters', None):
            score += 0.15
            reasons.append("HAVING过滤")
        
        # 占比指标
        if getattr(ir, 'ratio_metrics', None):
            score += 0.2
            reasons.append("占比计算")
        
        # 计算字段
        if getattr(ir, 'calculated_fields', None):
            score += 0.15
            reasons.append("计算字段")
        
        # 同比环比
        if getattr(ir, 'comparison_type', None):
            score += 0.25
            reasons.append(f"对比分析({ir.comparison_type})")
        
        # 累计统计
        if getattr(ir, 'cumulative_metrics', None):
            score += 0.2
            reasons.append("累计统计")
        
        # 移动平均
        if getattr(ir, 'moving_average_metrics', None):
            score += 0.2
            reasons.append("移动平均")
        
        # 窗口函数查询
        if ir.query_type == "window_detail":
            score += 0.25
            reasons.append("窗口函数")
        
        # 跨分区查询
        if getattr(ir, 'cross_partition_query', False):
            score += 0.3
            reasons.append("跨分区查询")
        
        # ======== 第3优先级：基础结构评估（轻微加分） ========
        
        # 多维度分组
        dim_count = len(ir.dimensions) if ir.dimensions else 0
        if dim_count > 3:
            score += 0.1
            reasons.append(f"多维分组({dim_count})")
        
        # 多过滤条件
        filter_count = len(ir.filters) if ir.filters else 0
        if filter_count > 4:
            score += 0.1
            reasons.append(f"多重筛选({filter_count})")
        
        # ======== 计算最终评级 ========
        score = min(score, 1.0)
        
        if score < 0.3:
            level = "simple"
        elif score < 0.7:
            level = "medium"
        else:
            level = "complex"
        
        reason_str = " | ".join(reasons) if reasons else "结构简单"
        
        logger.debug(
            "复杂度评估完成",
            score=round(score, 2),
            level=level,
            reasons=reason_str,
            question_preview=question[:30] if question else ""
        )
        
        return score, level, reason_str
    
    def should_use_dag_execution(
        self,
        ir: IntermediateRepresentation
    ) -> Tuple[bool, str]:
        """
        判断是否应该使用 DAG 执行
        
        仅当 LLM 明确标记 is_too_complex=true 时使用 DAG 执行。
        requires_direct_sql=true 使用直接SQL流程，不走 DAG。
        
        Args:
            ir: 中间表示
        
        Returns:
            (should_use: bool, reason: str)
        """
        # requires_direct_sql 使用直接SQL，不走DAG
        if getattr(ir, 'requires_direct_sql', False):
            return False, "使用直接SQL流程"
        
        # is_too_complex 使用 DAG 执行
        if getattr(ir, 'is_too_complex', False):
            reason = getattr(ir, 'complexity_reason', '问题需要拆分执行')
            return True, reason
        
        return False, "不需要DAG执行"


def get_complexity_evaluator() -> HybridComplexityEvaluator:
    """获取复杂度评估器实例"""
    return HybridComplexityEvaluator()
