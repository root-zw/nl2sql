"""
综合置信度计算器

基于多个检索因素计算综合置信度，用于：
- 动态Prompt策略选择
- 过滤条件决策
- 上下文内容裁剪
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import structlog

from server.config import RetrievalConfig

if TYPE_CHECKING:
    from server.nl2ir.parallel_retriever import RetrievalBundle

logger = structlog.get_logger()


@dataclass
class ConfidenceFactors:
    """置信度因素分值"""
    # A1-A7 核心因素
    A1_domain_dense: float = 0.0       # 业务域Dense得分
    A2_table_score: float = 0.0        # 表检索得分
    A3_enum_exact_count: float = 0.0   # 精确枚举匹配数（归一化）
    A4_measure_match: float = 0.0      # 度量匹配得分
    A5_triplet_count: float = 0.0      # 维度三元组数量（归一化）
    A6_rrf_top: float = 0.0            # RRF Top得分（归一化）
    A7_reranker: float = 0.0           # Reranker得分
    
    # B1-B2 辅助因素
    B1_fewshot_exact: float = 0.0      # Few-Shot精确匹配
    B2_keyword_hit_ratio: float = 0.0  # 关键词命中率
    
    def to_dict(self) -> Dict[str, float]:
        return {
            "A1_domain_dense": self.A1_domain_dense,
            "A2_table_score": self.A2_table_score,
            "A3_enum_exact_count": self.A3_enum_exact_count,
            "A4_measure_match": self.A4_measure_match,
            "A5_triplet_count": self.A5_triplet_count,
            "A6_rrf_top": self.A6_rrf_top,
            "A7_reranker": self.A7_reranker,
            "B1_fewshot_exact": self.B1_fewshot_exact,
            "B2_keyword_hit_ratio": self.B2_keyword_hit_ratio,
        }


@dataclass
class ConfidenceResult:
    """置信度计算结果"""
    # 综合置信度 [0, 1]
    confidence: float = 0.0
    
    # 置信度等级: high, medium, low
    level: str = "low"
    
    # 各因素分值
    factors: ConfidenceFactors = field(default_factory=ConfidenceFactors)
    
    # 加权贡献明细
    contributions: Dict[str, float] = field(default_factory=dict)
    
    # 决策建议
    recommendation: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "confidence": round(self.confidence, 4),
            "level": self.level,
            "factors": self.factors.to_dict(),
            "contributions": {k: round(v, 4) for k, v in self.contributions.items()},
            "recommendation": self.recommendation,
        }


class ConfidenceCalculator:
    """
    综合置信度计算器
    
    基于多个检索因素的加权融合计算整体置信度。
    """
    
    # 默认权重配置
    DEFAULT_WEIGHTS: Dict[str, float] = {
        "A1_domain_dense": 0.12,
        "A2_table_score": 0.20,
        "A3_enum_exact_count": 0.12,
        "A4_measure_match": 0.08,
        "A5_triplet_count": 0.08,
        "A6_rrf_top": 0.15,
        "A7_reranker": 0.10,
        "B1_fewshot_exact": 0.10,
        "B2_keyword_hit_ratio": 0.05,
    }
    
    # 默认阈值配置
    DEFAULT_THRESHOLDS = {
        "high": 0.75,
        "medium": 0.45,
    }
    
    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        thresholds: Optional[Dict[str, float]] = None,
    ):
        """
        初始化计算器
        
        Args:
            weights: 各因素权重，为None时从配置读取
            thresholds: 置信度分级阈值
        """
        self.weights = weights or self._load_weights()
        self.thresholds = thresholds or self._load_thresholds()
    
    @classmethod
    def from_config(cls) -> "ConfidenceCalculator":
        """从配置创建实例"""
        return cls()
    
    def _load_weights(self) -> Dict[str, float]:
        """从配置加载权重"""
        return RetrievalConfig.confidence_weights() or self.DEFAULT_WEIGHTS
    
    def _load_thresholds(self) -> Dict[str, float]:
        """从配置加载阈值"""
        return {
            "high": RetrievalConfig.confidence_high_threshold(),
            "medium": RetrievalConfig.confidence_medium_threshold(),
        }
    
    def compute(self, bundle: "RetrievalBundle") -> ConfidenceResult:
        """
        计算综合置信度
        
        Args:
            bundle: 检索结果束
        
        Returns:
            ConfidenceResult 包含置信度和详细因素
        """
        factors = self._extract_factors(bundle)
        
        # 计算加权贡献
        contributions: Dict[str, float] = {}
        total_confidence = 0.0
        
        factors_dict = factors.to_dict()
        for factor_name, factor_value in factors_dict.items():
            weight = self.weights.get(factor_name, 0.0)
            contribution = factor_value * weight
            contributions[factor_name] = contribution
            total_confidence += contribution
        
        # 确定置信度等级
        level = self._determine_level(total_confidence)
        
        # 生成建议
        recommendation = self._generate_recommendation(level, factors)
        
        result = ConfidenceResult(
            confidence=total_confidence,
            level=level,
            factors=factors,
            contributions=contributions,
            recommendation=recommendation,
        )
        
        logger.debug(
            "置信度计算完成",
            confidence=round(total_confidence, 4),
            level=level,
        )
        
        return result
    
    def _extract_factors(self, bundle: "RetrievalBundle") -> ConfidenceFactors:
        """
        从检索结果束提取各因素分值
        
        Args:
            bundle: 检索结果束
        
        Returns:
            ConfidenceFactors
        """
        factors = ConfidenceFactors()
        
        # A1: 业务域Dense得分
        if bundle.domain_result and bundle.domain_result.candidates:
            top_candidate = bundle.domain_result.candidates[0]
            factors.A1_domain_dense = min(1.0, top_candidate.dense_score or 0.0)
        
        # A2: 表检索得分
        if bundle.table_candidates:
            top_table = bundle.table_candidates[0]
            if hasattr(top_table, "score"):
                factors.A2_table_score = min(1.0, top_table.score or 0.0)
            elif isinstance(top_table, dict):
                factors.A2_table_score = min(1.0, top_table.get("score", 0.0))
        
        # A3: 精确枚举匹配数（归一化：每个精确匹配+0.2，最多1.0）
        exact_enum_count = sum(
            1 for e in bundle.enum_matches
            if e.get("match_type") == "exact"
        )
        factors.A3_enum_exact_count = min(1.0, exact_enum_count * 0.2)
        
        # A4: 度量匹配得分
        if bundle.measure_matches:
            top_measure = bundle.measure_matches[0]
            if hasattr(top_measure, "score"):
                factors.A4_measure_match = min(1.0, top_measure.score or 0.0)
            elif isinstance(top_measure, dict):
                factors.A4_measure_match = min(1.0, top_measure.get("score", 0.0))
        
        # A5: 维度三元组数量（归一化：每个三元组+0.1，最多1.0）
        triplet_count = len([
            e for e in bundle.enum_matches
            if e.get("triplet")
        ])
        factors.A5_triplet_count = min(1.0, triplet_count * 0.1)
        
        # A6: RRF Top得分（归一化：RRF通常很小，乘以15归一化）
        if bundle.table_candidates:
            top_table = bundle.table_candidates[0]
            rrf_score = 0.0
            if hasattr(top_table, "rrf_score"):
                rrf_score = top_table.rrf_score or 0.0
            elif isinstance(top_table, dict):
                rrf_score = top_table.get("rrf_score", 0.0)
            factors.A6_rrf_top = min(1.0, rrf_score * 15)
        
        # A7: Reranker得分
        if bundle.table_candidates:
            top_table = bundle.table_candidates[0]
            reranker_score = 0.0
            if hasattr(top_table, "reranker_score"):
                reranker_score = top_table.reranker_score or 0.0
            elif isinstance(top_table, dict):
                reranker_score = top_table.get("reranker_score", 0.0)
            # Reranker已归一化到[0,1]，直接使用
            factors.A7_reranker = max(0.0, min(1.0, reranker_score))
        
        # B1: Few-Shot精确匹配
        for sample in bundle.few_shot_samples:
            similarity = sample.get("similarity", 0) or sample.get("raw_similarity", 0)
            if similarity >= 0.95:
                factors.B1_fewshot_exact = 1.0
                break
        
        # B2: 关键词命中率（需要外部传入，这里简单估算）
        # 基于枚举匹配数和表匹配数估算
        total_matches = len(bundle.enum_matches) + len(bundle.table_candidates)
        if total_matches > 0:
            # 简单估算：匹配数越多，关键词命中率越高
            factors.B2_keyword_hit_ratio = min(1.0, total_matches * 0.1)
        
        return factors
    
    def _determine_level(self, confidence: float) -> str:
        """确定置信度等级"""
        if confidence >= self.thresholds["high"]:
            return "high"
        elif confidence >= self.thresholds["medium"]:
            return "medium"
        else:
            return "low"
    
    def _generate_recommendation(
        self,
        level: str,
        factors: ConfidenceFactors,
    ) -> str:
        """生成决策建议"""
        if level == "high":
            return "高置信度：可使用精确模式，仅提供匹配字段"
        elif level == "medium":
            return "中等置信度：使用标准模式，提供完整表结构"
        else:
            # 低置信度时，分析原因
            reasons = []
            if factors.A1_domain_dense < 0.3:
                reasons.append("业务域匹配不确定")
            if factors.A2_table_score < 0.3:
                reasons.append("表匹配置信度低")
            if factors.A3_enum_exact_count == 0:
                reasons.append("无精确枚举匹配")
            
            reason_text = "、".join(reasons) if reasons else "整体匹配分数较低"
            return f"低置信度（{reason_text}）：建议用户澄清问题"


def compute_confidence(bundle: "RetrievalBundle") -> ConfidenceResult:
    """
    便捷函数：计算检索结果置信度
    
    Args:
        bundle: 检索结果束
    
    Returns:
        ConfidenceResult
    """
    calculator = ConfidenceCalculator.from_config()
    return calculator.compute(bundle)

