"""
向量表选择置信度评估器

综合多个维度评估LLM3选表+IR生成的置信度：
1. LLM输出的自报置信度
2. 向量检索分数（TOP1绝对分、TOP1-TOP2分差）
3. IR结构完整性
4. 字段覆盖率
5. 枚举匹配度

用于分流决策：高置信度直接执行 vs 中置信度确认 vs 低置信度降级
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING
import re
import structlog

from server.config import RetrievalConfig, get_retrieval_param

if TYPE_CHECKING:
    from server.nl2ir.vector_table_selector import TableWithFields

logger = structlog.get_logger()


@dataclass
class ConfidenceFactors:
    """置信度各维度分值"""
    # LLM自报置信度 [0,1]
    llm_confidence: float = 0.0
    
    # 向量检索分数相关
    top1_absolute_score: float = 0.0  # TOP1绝对分 [0,1]
    top1_top2_gap: float = 0.0        # TOP1-TOP2分差，归一化到[0,1]
    selected_is_top1: bool = False     # 选中的表是否是TOP1
    
    # IR结构完整性
    ir_completeness: float = 0.0      # IR结构完整性 [0,1]
    
    # 字段覆盖率（问题中提及的字段是否在IR中）
    field_coverage: float = 0.0       # [0,1]
    
    # 枚举匹配度（如果有枚举匹配信息）
    enum_match_score: float = 0.0     # [0,1]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "llm_confidence": round(self.llm_confidence, 4),
            "top1_absolute_score": round(self.top1_absolute_score, 4),
            "top1_top2_gap": round(self.top1_top2_gap, 4),
            "selected_is_top1": self.selected_is_top1,
            "ir_completeness": round(self.ir_completeness, 4),
            "field_coverage": round(self.field_coverage, 4),
            "enum_match_score": round(self.enum_match_score, 4),
        }


@dataclass
class ConfidenceEvaluationResult:
    """置信度评估结果"""
    # 综合置信度 [0,1]
    final_confidence: float = 0.0
    
    # 各维度分值
    factors: ConfidenceFactors = field(default_factory=ConfidenceFactors)
    
    # 各维度加权贡献
    contributions: Dict[str, float] = field(default_factory=dict)
    
    # 评估说明
    explanation: str = ""
    
    # 警告信息
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "final_confidence": round(self.final_confidence, 4),
            "factors": self.factors.to_dict(),
            "contributions": {k: round(v, 4) for k, v in self.contributions.items()},
            "explanation": self.explanation,
            "warnings": self.warnings,
        }


class VectorSelectionConfidenceCalculator:
    """
    向量表选择置信度计算器
    
    综合多个维度计算最终置信度，用于分流决策
    """
    
    # 默认权重配置
    DEFAULT_WEIGHTS = {
        "llm_confidence": 0.35,       # LLM自报置信度
        "top1_absolute": 0.15,        # TOP1绝对分
        "top1_top2_gap": 0.15,        # TOP1-TOP2分差
        "selected_is_top1": 0.10,     # 选中的是否是TOP1
        "ir_completeness": 0.15,      # IR结构完整性
        "field_coverage": 0.10,       # 字段覆盖率
    }
    
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        """
        初始化计算器
        
        Args:
            weights: 各维度权重，为None时从配置读取
        """
        self.weights = weights or self._load_weights()
    
    def _load_weights(self) -> Dict[str, float]:
        """从配置加载权重"""
        config_weights = get_retrieval_param(
            "vector_table_selection.confidence_weights",
            None
        )
        if config_weights and isinstance(config_weights, dict):
            # 合并配置权重和默认权重
            return {**self.DEFAULT_WEIGHTS, **config_weights}
        return self.DEFAULT_WEIGHTS.copy()
    
    def evaluate(
        self,
        llm_confidence: float,
        tables: List["TableWithFields"],
        selected_table_id: str,
        ir: Optional[Dict[str, Any]],
        question: str,
        enum_matches: Optional[List[Dict[str, Any]]] = None,
    ) -> ConfidenceEvaluationResult:
        """
        评估置信度
        
        Args:
            llm_confidence: LLM输出的置信度
            tables: 候选表列表（含检索分数）
            selected_table_id: LLM选中的表ID
            ir: LLM生成的IR
            question: 用户问题
            enum_matches: 枚举匹配信息（可选）
        
        Returns:
            ConfidenceEvaluationResult
        """
        factors = ConfidenceFactors()
        warnings = []
        
        # 1. LLM自报置信度
        factors.llm_confidence = max(0.0, min(1.0, llm_confidence))
        
        # 2. 向量检索分数分析
        if tables:
            sorted_tables = sorted(tables, key=lambda t: t.retrieval_score, reverse=True)
            top1_score = sorted_tables[0].retrieval_score
            top2_score = sorted_tables[1].retrieval_score if len(sorted_tables) > 1 else 0
            
            # TOP1绝对分（归一化，假设0.6为高分）
            factors.top1_absolute_score = min(1.0, top1_score / 0.6)
            
            # TOP1-TOP2分差（归一化，假设0.15为显著分差）
            gap = top1_score - top2_score
            factors.top1_top2_gap = min(1.0, gap / 0.15)
            
            # 选中的是否是TOP1
            factors.selected_is_top1 = (selected_table_id == sorted_tables[0].table_id)
            
            # 如果选中的不是TOP1，降低警告
            if not factors.selected_is_top1:
                # 找到选中表的排名
                selected_rank = next(
                    (i for i, t in enumerate(sorted_tables) if t.table_id == selected_table_id),
                    -1
                )
                if selected_rank >= 0:
                    warnings.append(f"LLM选择了排名第{selected_rank + 1}的表，而非TOP1")
                else:
                    warnings.append("LLM选择的表ID不在候选列表中")
        
        # 3. IR结构完整性
        factors.ir_completeness = self._evaluate_ir_completeness(ir)
        if factors.ir_completeness < 0.5:
            warnings.append("IR结构不完整")
        
        # 4. 字段覆盖率
        if ir and tables:
            selected_table = next((t for t in tables if t.table_id == selected_table_id), None)
            if selected_table:
                factors.field_coverage = self._evaluate_field_coverage(ir, selected_table, question)
        
        # 5. 枚举匹配度（如果有）
        if enum_matches:
            factors.enum_match_score = self._evaluate_enum_matches(enum_matches, selected_table_id)
        
        # 计算加权贡献和最终置信度
        contributions = {}
        total_confidence = 0.0
        
        # LLM置信度
        weight = self.weights.get("llm_confidence", 0.35)
        contribution = factors.llm_confidence * weight
        contributions["llm_confidence"] = contribution
        total_confidence += contribution
        
        # TOP1绝对分
        weight = self.weights.get("top1_absolute", 0.15)
        contribution = factors.top1_absolute_score * weight
        contributions["top1_absolute"] = contribution
        total_confidence += contribution
        
        # TOP1-TOP2分差
        weight = self.weights.get("top1_top2_gap", 0.15)
        contribution = factors.top1_top2_gap * weight
        contributions["top1_top2_gap"] = contribution
        total_confidence += contribution
        
        # 选中的是否是TOP1
        weight = self.weights.get("selected_is_top1", 0.10)
        contribution = (1.0 if factors.selected_is_top1 else 0.3) * weight
        contributions["selected_is_top1"] = contribution
        total_confidence += contribution
        
        # IR完整性
        weight = self.weights.get("ir_completeness", 0.15)
        contribution = factors.ir_completeness * weight
        contributions["ir_completeness"] = contribution
        total_confidence += contribution
        
        # 字段覆盖率
        weight = self.weights.get("field_coverage", 0.10)
        contribution = factors.field_coverage * weight
        contributions["field_coverage"] = contribution
        total_confidence += contribution
        
        # 生成评估说明
        explanation = self._generate_explanation(factors, total_confidence)
        
        return ConfidenceEvaluationResult(
            final_confidence=total_confidence,
            factors=factors,
            contributions=contributions,
            explanation=explanation,
            warnings=warnings
        )
    
    def _evaluate_ir_completeness(self, ir: Optional[Dict[str, Any]]) -> float:
        """评估IR结构完整性"""
        if not ir:
            return 0.0
        
        score = 0.0
        
        # 必须有query_type
        if ir.get("query_type"):
            score += 0.3
        
        # 必须有metrics或dimensions
        has_metrics = bool(ir.get("metrics"))
        has_dimensions = bool(ir.get("dimensions"))
        
        if has_metrics or has_dimensions:
            score += 0.3
        
        if has_metrics and has_dimensions:
            score += 0.1
        
        # 有filters是加分项
        if ir.get("filters"):
            score += 0.15
        
        # 有排序/限制是加分项
        if ir.get("sort_by") or ir.get("limit"):
            score += 0.1
        
        # 没有歧义是加分项
        ambiguities = ir.get("ambiguities", [])
        if not ambiguities:
            score += 0.05
        
        return min(1.0, score)
    
    def _evaluate_field_coverage(
        self,
        ir: Dict[str, Any],
        selected_table: "TableWithFields",
        question: str
    ) -> float:
        """评估字段覆盖率"""
        # 从IR中提取所有引用的字段
        ir_fields = set()
        
        for field_id in ir.get("metrics", []):
            if field_id and field_id != "__row_count__":
                ir_fields.add(field_id)
        
        for field_id in ir.get("dimensions", []):
            if field_id:
                ir_fields.add(field_id)
        
        for filter_item in ir.get("filters", []):
            field_id = filter_item.get("field")
            if field_id:
                ir_fields.add(field_id)
        
        if not ir_fields:
            # 没有引用字段，可能是简单计数查询
            if ir.get("metrics") == ["__row_count__"]:
                return 0.8  # 计数查询给予较高分
            return 0.3
        
        # 获取表的所有字段ID
        table_field_ids = set()
        for f in selected_table.dimensions + selected_table.measures + selected_table.identifiers:
            table_field_ids.add(f.field_id)
        
        # 检查IR中的字段是否都在表中
        valid_fields = ir_fields & table_field_ids
        if ir_fields:
            coverage = len(valid_fields) / len(ir_fields)
        else:
            coverage = 0.5
        
        return coverage
    
    def _evaluate_enum_matches(
        self,
        enum_matches: List[Dict[str, Any]],
        selected_table_id: str
    ) -> float:
        """评估枚举匹配度"""
        if not enum_matches:
            return 0.5  # 无枚举匹配信息时给中等分
        
        # 统计与选中表相关的枚举匹配
        relevant_matches = [
            m for m in enum_matches
            if m.get("table_id") == selected_table_id
        ]
        
        if not relevant_matches:
            return 0.3  # 选中表没有枚举匹配
        
        # 计算平均匹配分数
        scores = [m.get("score", 0) for m in relevant_matches if m.get("score")]
        if scores:
            avg_score = sum(scores) / len(scores)
            return min(1.0, avg_score)
        
        return 0.5
    
    def _generate_explanation(
        self,
        factors: ConfidenceFactors,
        total_confidence: float
    ) -> str:
        """生成评估说明"""
        parts = []
        
        if total_confidence >= 0.85:
            parts.append("高置信度")
        elif total_confidence >= 0.5:
            parts.append("中等置信度")
        else:
            parts.append("低置信度")
        
        # 分析主要贡献和问题
        if factors.llm_confidence >= 0.8:
            parts.append("LLM高度确信")
        elif factors.llm_confidence < 0.5:
            parts.append("LLM不太确定")
        
        if factors.top1_top2_gap >= 0.8:
            parts.append("TOP1优势明显")
        elif factors.top1_top2_gap < 0.3:
            parts.append("候选表分数接近")
        
        if not factors.selected_is_top1:
            parts.append("选择非TOP1表")
        
        if factors.ir_completeness < 0.5:
            parts.append("IR结构欠完整")
        
        return "，".join(parts)


def quick_evaluate_confidence(
    llm_confidence: float,
    top1_score: float,
    top2_score: float,
    selected_is_top1: bool,
    ir: Optional[Dict[str, Any]] = None
) -> float:
    """
    快速评估置信度（简化版，用于无法获取完整信息时）
    
    Args:
        llm_confidence: LLM输出的置信度
        top1_score: TOP1表的检索分数
        top2_score: TOP2表的检索分数
        selected_is_top1: 选中的是否是TOP1
        ir: IR结构（可选）
    
    Returns:
        综合置信度 [0, 1]
    """
    # 简化的权重
    weights = {
        "llm": 0.40,
        "top1_absolute": 0.20,
        "gap": 0.20,
        "is_top1": 0.10,
        "ir": 0.10
    }
    
    # LLM置信度
    score = llm_confidence * weights["llm"]
    
    # TOP1绝对分（归一化到0.6）
    score += min(1.0, top1_score / 0.6) * weights["top1_absolute"]
    
    # TOP1-TOP2分差（归一化到0.15）
    gap = top1_score - top2_score
    score += min(1.0, gap / 0.15) * weights["gap"]
    
    # 选中TOP1
    score += (1.0 if selected_is_top1 else 0.3) * weights["is_top1"]
    
    # IR完整性（简化检查）
    if ir:
        ir_score = 0.0
        if ir.get("query_type"):
            ir_score += 0.5
        if ir.get("metrics") or ir.get("dimensions"):
            ir_score += 0.5
        score += ir_score * weights["ir"]
    else:
        score += 0.5 * weights["ir"]  # 无IR时给中等分
    
    return min(1.0, score)

