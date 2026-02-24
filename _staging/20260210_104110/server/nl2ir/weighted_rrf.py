"""
加权RRF（Reciprocal Rank Fusion）融合模块

提供Python端手动融合策略，完全控制Dense和Sparse的权重分配。
支持多通道检索结果融合，可配置化权重。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Callable
import structlog

from server.config import RetrievalConfig

logger = structlog.get_logger()


@dataclass
class RRFHit:
    """RRF融合后的单个结果"""
    identity: str                     # 唯一标识（如 table_id, field_id）
    rrf_score: float = 0.0            # RRF融合分数
    dense_score: Optional[float] = None   # Dense通道原始分数
    sparse_score: Optional[float] = None  # Sparse通道原始分数
    dense_rank: Optional[int] = None      # Dense通道排名
    sparse_rank: Optional[int] = None     # Sparse通道排名
    reranker_score: Optional[float] = None  # Reranker分数
    final_score: float = 0.0          # 最终融合分数
    payload: Dict[str, Any] = field(default_factory=dict)  # 原始数据


class WeightedRRFMerger:
    """
    加权RRF融合器
    
    支持:
    - 可配置的k参数（控制排名敏感度）
    - Dense/Sparse独立权重
    - 多来源融合
    - Reranker二次融合
    """
    
    def __init__(
        self,
        k: int = 60,
        dense_weight: float = 0.6,
        sparse_weight: float = 0.4,
        reranker_weight: float = 0.3,
    ):
        """
        初始化融合器
        
        Args:
            k: RRF k参数，越小分数区分度越大，默认60
            dense_weight: Dense通道权重
            sparse_weight: Sparse通道权重
            reranker_weight: Reranker融合权重（0表示不使用）
        """
        self.k = k
        self.dense_weight = dense_weight
        self.sparse_weight = sparse_weight
        self.reranker_weight = reranker_weight
    
    @classmethod
    def from_config(cls) -> "WeightedRRFMerger":
        """从配置创建实例"""
        return cls(
            k=RetrievalConfig.rrf_k(),
            dense_weight=RetrievalConfig.rrf_dense_weight(),
            sparse_weight=RetrievalConfig.rrf_sparse_weight(),
            reranker_weight=RetrievalConfig.reranker_weight(),
        )
    
    def merge(
        self,
        dense_hits: List[Dict[str, Any]],
        sparse_hits: List[Dict[str, Any]],
        identity_key: str = "identity",
    ) -> List[RRFHit]:
        """
        融合Dense和Sparse检索结果
        
        Args:
            dense_hits: Dense检索结果列表，每个元素需包含 identity_key 和 score
            sparse_hits: Sparse检索结果列表
            identity_key: 用于标识唯一性的字段名
        
        Returns:
            融合后的结果列表，按RRF分数降序排列
        """
        aggregated: Dict[str, RRFHit] = {}
        
        # 处理Dense通道
        for rank, hit in enumerate(dense_hits, start=1):
            identity = str(hit.get(identity_key, ""))
            if not identity:
                continue
            
            contribution = self.dense_weight * (1.0 / (self.k + rank))
            
            if identity not in aggregated:
                aggregated[identity] = RRFHit(
                    identity=identity,
                    payload=hit,
                )
            
            entry = aggregated[identity]
            entry.rrf_score += contribution
            entry.dense_score = hit.get("score") or hit.get("distance")
            entry.dense_rank = rank
        
        # 处理Sparse通道
        for rank, hit in enumerate(sparse_hits, start=1):
            identity = str(hit.get(identity_key, ""))
            if not identity:
                continue
            
            contribution = self.sparse_weight * (1.0 / (self.k + rank))
            
            if identity not in aggregated:
                aggregated[identity] = RRFHit(
                    identity=identity,
                    payload=hit,
                )
            
            entry = aggregated[identity]
            entry.rrf_score += contribution
            entry.sparse_score = hit.get("score") or hit.get("distance")
            entry.sparse_rank = rank
            
            # 合并payload（优先保留Dense的payload，Sparse补充）
            if entry.dense_rank is None:
                entry.payload = hit
        
        # 设置final_score为rrf_score
        for entry in aggregated.values():
            entry.final_score = entry.rrf_score
        
        # 按RRF分数降序排列
        result = sorted(aggregated.values(), key=lambda x: x.rrf_score, reverse=True)
        
        logger.debug(
            "RRF融合完成",
            dense_count=len(dense_hits),
            sparse_count=len(sparse_hits),
            merged_count=len(result),
            k=self.k,
            dense_weight=self.dense_weight,
            sparse_weight=self.sparse_weight,
        )
        
        return result
    
    def merge_with_reranker(
        self,
        rrf_hits: List[RRFHit],
        reranker_scores: List[float],
    ) -> List[RRFHit]:
        """
        将RRF结果与Reranker分数融合
        
        Args:
            rrf_hits: RRF融合后的结果
            reranker_scores: 对应的Reranker分数（已归一化到[0,1]）
        
        Returns:
            融合Reranker后的结果列表
        """
        if not rrf_hits or not reranker_scores:
            return rrf_hits
        
        if len(rrf_hits) != len(reranker_scores):
            logger.warning(
                "RRF hits和Reranker scores数量不匹配",
                rrf_count=len(rrf_hits),
                reranker_count=len(reranker_scores),
            )
            return rrf_hits
        
        for hit, reranker_score in zip(rrf_hits, reranker_scores):
            hit.reranker_score = reranker_score
            # 融合公式: final = (1 - reranker_weight) * rrf + reranker_weight * reranker
            hit.final_score = (
                (1 - self.reranker_weight) * hit.rrf_score + 
                self.reranker_weight * reranker_score
            )
        
        # 按最终分数重新排序
        rrf_hits.sort(key=lambda x: x.final_score, reverse=True)
        
        return rrf_hits
    
    def merge_multi_source(
        self,
        sources: Dict[str, Tuple[List[Dict[str, Any]], float]],
        identity_key: str = "identity",
    ) -> List[RRFHit]:
        """
        多来源RRF融合
        
        Args:
            sources: 来源字典，格式为 {source_name: (hits_list, weight)}
            identity_key: 唯一标识字段名
        
        Returns:
            融合后的结果列表
        """
        aggregated: Dict[str, RRFHit] = {}
        
        for source_name, (hits, weight) in sources.items():
            for rank, hit in enumerate(hits, start=1):
                identity = str(hit.get(identity_key, ""))
                if not identity:
                    continue
                
                contribution = weight * (1.0 / (self.k + rank))
                
                if identity not in aggregated:
                    aggregated[identity] = RRFHit(
                        identity=identity,
                        payload=hit,
                    )
                
                entry = aggregated[identity]
                entry.rrf_score += contribution
                
                # 记录来源信息
                if "sources" not in entry.payload:
                    entry.payload["sources"] = {}
                entry.payload["sources"][source_name] = {
                    "rank": rank,
                    "score": hit.get("score"),
                    "weight": weight,
                }
        
        for entry in aggregated.values():
            entry.final_score = entry.rrf_score
        
        return sorted(aggregated.values(), key=lambda x: x.rrf_score, reverse=True)


def rrf_merge_simple(
    dense_hits: List[Dict[str, Any]],
    sparse_hits: List[Dict[str, Any]],
    identity_key: str = "identity",
    k: int = 60,
    dense_weight: float = 0.6,
    sparse_weight: float = 0.4,
) -> List[Dict[str, Any]]:
    """
    简化版RRF融合函数（兼容旧接口）
    
    Args:
        dense_hits: Dense检索结果
        sparse_hits: Sparse检索结果
        identity_key: 唯一标识字段
        k: RRF k参数
        dense_weight: Dense权重
        sparse_weight: Sparse权重
    
    Returns:
        融合后的字典列表
    """
    merger = WeightedRRFMerger(
        k=k,
        dense_weight=dense_weight,
        sparse_weight=sparse_weight,
    )
    
    rrf_hits = merger.merge(dense_hits, sparse_hits, identity_key)
    
    # 转换为字典格式
    results = []
    for hit in rrf_hits:
        result = dict(hit.payload)
        result["rrf_score"] = hit.rrf_score
        result["dense_score"] = hit.dense_score
        result["sparse_score"] = hit.sparse_score
        result["dense_rank"] = hit.dense_rank
        result["sparse_rank"] = hit.sparse_rank
        result["final_score"] = hit.final_score
        results.append(result)
    
    return results


def compute_rrf_contribution(rank: int, k: int = 60, weight: float = 1.0) -> float:
    """
    计算单个排名的RRF贡献值
    
    Args:
        rank: 排名（从1开始）
        k: RRF k参数
        weight: 权重
    
    Returns:
        RRF贡献值
    """
    if rank <= 0:
        return 0.0
    return weight * (1.0 / (k + rank))

