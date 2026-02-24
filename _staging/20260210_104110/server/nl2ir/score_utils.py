"""
统一分数归一化工具

提供各种检索分数的归一化处理函数，确保不同来源的分数可比较。
"""

from __future__ import annotations

import math
from typing import Optional, List, Dict, Any


def normalize_dense_score(
    distance: Optional[float], 
    metric: Optional[str] = None
) -> float:
    """
    归一化Dense向量距离/相似度到[0,1]区间
    
    支持的度量类型：
    - COSINE: 余弦相似度，范围[-1,1] -> [0,1]
    - IP (Inner Product): 内积，范围不定，使用sigmoid
    - L2: 欧氏距离，范围[0,+∞) -> [0,1] (距离越小越相似)
    
    Args:
        distance: 原始距离/相似度值
        metric: 度量类型，可选 COSINE, IP, L2
    
    Returns:
        归一化后的相似度 [0, 1]
    """
    if distance is None:
        return 0.0
    
    metric_upper = (metric or "COSINE").upper()
    
    if metric_upper == "COSINE":
        # COSINE相似度范围 [-1, 1] -> [0, 1]
        return max(0.0, min(1.0, (distance + 1.0) / 2.0))
    
    elif metric_upper == "IP":
        # IP (Inner Product) 范围不定，使用sigmoid映射
        return sigmoid(distance)
    
    elif metric_upper == "L2":
        # L2距离范围 [0, +∞)，距离越小越相似
        # 使用 1 / (1 + distance) 映射到 (0, 1]
        return 1.0 / (1.0 + distance)
    
    else:
        # 默认假设是相似度，截断到[0,1]
        return max(0.0, min(1.0, distance))


def normalize_sparse_score(
    score: Optional[float],
    max_score: Optional[float] = None
) -> float:
    """
    归一化Sparse/BM25分数到[0,1]区间
    
    BM25分数通常在 [0, 20+] 范围，需要归一化处理
    
    Args:
        score: BM25原始分数
        max_score: 当前批次最大分数（用于min-max归一化），为None时使用sigmoid
    
    Returns:
        归一化后的分数 [0, 1]
    """
    if score is None:
        return 0.0
    
    if max_score is not None and max_score > 0:
        # Min-Max归一化
        return min(1.0, score / max_score)
    else:
        # 使用调整后的sigmoid，使典型BM25分数（5-15）映射到合理区间
        # 调整参数使 score=10 约等于 0.73
        adjusted = (score - 5.0) / 5.0
        return sigmoid(adjusted)


def sigmoid(x: float) -> float:
    """
    Sigmoid函数
    
    将任意实数映射到 (0, 1) 区间
    
    Args:
        x: 输入值
    
    Returns:
        sigmoid(x) ∈ (0, 1)
    """
    # 防止数值溢出
    if x >= 700:
        return 1.0
    if x <= -700:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def normalize_reranker_score(
    score: Optional[float],
    use_sigmoid: bool = True
) -> float:
    """
    归一化Reranker分数到[0,1]区间
    
    Reranker模型输出可能是任意实数（包括负数），需要归一化
    
    Args:
        score: Reranker原始分数
        use_sigmoid: 是否使用sigmoid归一化
    
    Returns:
        归一化后的分数 [0, 1]
    """
    if score is None:
        return 0.0
    
    if use_sigmoid:
        return sigmoid(score)
    else:
        # 简单截断到[0,1]
        return max(0.0, min(1.0, score))


def normalize_rrf_score(
    score: Optional[float],
    k: int = 60
) -> float:
    """
    归一化RRF分数到[0,1]区间
    
    RRF分数范围取决于参与融合的通道数，通常很小（<0.1）
    
    Args:
        score: RRF原始分数
        k: RRF k参数
    
    Returns:
        归一化后的分数 [0, 1]
    """
    if score is None or score <= 0:
        return 0.0
    
    # RRF理论最大值（当只有一个候选且排名第一时）= 1/(k+1)
    # 两通道融合时最大值 = 2/(k+1)
    # 归一化：乘以(k+1)/2使双通道第一名归一化为1.0
    normalized = score * (k + 1) / 2.0
    return min(1.0, normalized)


def compute_weighted_score(
    scores: Dict[str, float],
    weights: Dict[str, float],
    normalize_weights: bool = True
) -> float:
    """
    计算加权总分
    
    Args:
        scores: 各通道分数字典，如 {"dense": 0.8, "sparse": 0.6}
        weights: 各通道权重字典，如 {"dense": 0.6, "sparse": 0.4}
        normalize_weights: 是否归一化权重使其和为1
    
    Returns:
        加权总分
    """
    if not scores or not weights:
        return 0.0
    
    # 计算权重总和
    if normalize_weights:
        total_weight = sum(weights.get(k, 0) for k in scores.keys())
        if total_weight <= 0:
            return 0.0
    else:
        total_weight = 1.0
    
    # 计算加权和
    weighted_sum = 0.0
    for key, score in scores.items():
        weight = weights.get(key, 0)
        weighted_sum += score * weight
    
    if normalize_weights and total_weight > 0:
        return weighted_sum / total_weight
    return weighted_sum


def blend_scores(
    primary_score: float,
    secondary_score: float,
    primary_weight: float = 0.7
) -> float:
    """
    融合两个分数
    
    Args:
        primary_score: 主要分数
        secondary_score: 次要分数
        primary_weight: 主要分数权重 (0-1)
    
    Returns:
        融合后的分数
    """
    secondary_weight = 1.0 - primary_weight
    return primary_score * primary_weight + secondary_score * secondary_weight


def batch_normalize_scores(
    items: List[Dict[str, Any]],
    score_key: str,
    method: str = "min_max",
    output_key: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    批量归一化分数
    
    Args:
        items: 包含分数的字典列表
        score_key: 分数字段名
        method: 归一化方法，可选 "min_max", "sigmoid", "rank"
        output_key: 输出字段名，为None时覆盖原字段
    
    Returns:
        归一化后的字典列表
    """
    if not items:
        return items
    
    output_key = output_key or score_key
    scores = [item.get(score_key, 0) for item in items]
    
    if method == "min_max":
        min_score = min(scores)
        max_score = max(scores)
        score_range = max_score - min_score
        if score_range > 0:
            for item, score in zip(items, scores):
                item[output_key] = (score - min_score) / score_range
        else:
            for item in items:
                item[output_key] = 0.5
    
    elif method == "sigmoid":
        for item, score in zip(items, scores):
            item[output_key] = sigmoid(score)
    
    elif method == "rank":
        # 基于排名的归一化
        n = len(items)
        sorted_indices = sorted(range(n), key=lambda i: scores[i], reverse=True)
        for rank, idx in enumerate(sorted_indices):
            items[idx][output_key] = 1.0 - (rank / n)
    
    return items


class ScoreNormalizer:
    """分数归一化器（带状态）"""
    
    def __init__(
        self,
        dense_metric: str = "COSINE",
        reranker_use_sigmoid: bool = True,
        rrf_k: int = 60
    ):
        """
        初始化归一化器
        
        Args:
            dense_metric: Dense向量度量类型
            reranker_use_sigmoid: Reranker是否使用sigmoid
            rrf_k: RRF k参数
        """
        self.dense_metric = dense_metric
        self.reranker_use_sigmoid = reranker_use_sigmoid
        self.rrf_k = rrf_k
    
    def normalize_dense(self, score: Optional[float]) -> float:
        """归一化Dense分数"""
        return normalize_dense_score(score, self.dense_metric)
    
    def normalize_sparse(
        self, 
        score: Optional[float], 
        max_score: Optional[float] = None
    ) -> float:
        """归一化Sparse分数"""
        return normalize_sparse_score(score, max_score)
    
    def normalize_reranker(self, score: Optional[float]) -> float:
        """归一化Reranker分数"""
        return normalize_reranker_score(score, self.reranker_use_sigmoid)
    
    def normalize_rrf(self, score: Optional[float]) -> float:
        """归一化RRF分数"""
        return normalize_rrf_score(score, self.rrf_k)

