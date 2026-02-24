"""混合检索通用工具（稠密 + 稀疏 + RRF）。"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Union

from server.config import settings, RetrievalConfig
from server.nl2ir.sparse_utils import (
    build_sparse_vector,
    prepare_bm25_text,
    tokenize_for_bm25,
)

try:
    # Milvus 2.6 可能提供 SparseVector 类（不同发行包名称有差异）
    from pymilvus import SparseVector as _SparseVector  # type: ignore
except Exception:
    _SparseVector = None


# =============================================================================
# HitExtractor: 统一的 Milvus Hit 处理工具类
# =============================================================================
# 设计目的：
#   1. 兼容 ORM Hit 对象 (.entity 返回 Entity 类) 和 MilvusClient dict hit
#   2. 提供统一 API，所有检索器调用此类，无需关心底层格式差异
#   3. 后期 Milvus SDK 升级或格式变化只需修改此类
# =============================================================================


class HitExtractor:
    """
    统一的 Milvus Hit 提取器。
    
    兼容两种 hit 格式:
    - ORM Hit: hit.entity 返回 Entity 对象，需调用 .to_dict() 或 .get()
    - MilvusClient dict: {'id': ..., 'distance': ..., 'entity': {...}}
    
    使用示例:
    ```python
    for hit in results[0]:
        extractor = HitExtractor(hit)
        payload = extractor.to_payload()  # 获取字段字典
        distance = extractor.distance     # 获取距离值
        item_id = extractor.get("item_id")  # 获取单个字段
    ```
    """
    
    __slots__ = ("_hit", "_entity", "_distance")
    
    def __init__(self, hit: Any):
        """
        初始化提取器。
        
        Args:
            hit: Milvus search 返回的单条结果（ORM Hit 或 dict）
        """
        self._hit = hit
        self._entity: Optional[Dict[str, Any]] = None
        self._distance: Optional[float] = None
        self._parse_hit()
    
    def _parse_hit(self) -> None:
        """解析 hit，提取 entity 和 distance。"""
        hit = self._hit
        
        # 提取 distance
        if hasattr(hit, "distance"):
            self._distance = hit.distance
        elif isinstance(hit, dict):
            self._distance = hit.get("distance")
        
        # 提取 entity
        raw_entity = None
        if hasattr(hit, "entity"):
            raw_entity = hit.entity
        elif isinstance(hit, dict):
            raw_entity = hit.get("entity") or hit
        else:
            raw_entity = hit
        
        # 转换为 dict
        if raw_entity is None:
            self._entity = {}
        elif isinstance(raw_entity, dict):
            self._entity = raw_entity
        elif hasattr(raw_entity, "to_dict"):
            try:
                self._entity = raw_entity.to_dict()
            except Exception:
                self._entity = dict(raw_entity) if hasattr(raw_entity, "__iter__") else {}
        elif hasattr(raw_entity, "__dict__"):
            self._entity = dict(raw_entity.__dict__)
        else:
            self._entity = {}
    
    @property
    def distance(self) -> float:
        """获取距离/相似度值。"""
        return float(self._distance) if self._distance is not None else 0.0
    
    @property
    def entity(self) -> Dict[str, Any]:
        """获取实体字段字典（只读）。"""
        return self._entity or {}
    
    def get(self, field: str, default: Any = None) -> Any:
        """
        获取单个字段值。
        
        Args:
            field: 字段名
            default: 默认值
            
        Returns:
            字段值或默认值
        """
        return self._entity.get(field, default) if self._entity else default
    
    def to_payload(self, fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        构建 payload 字典。
        
        Args:
            fields: 可选的字段列表，若提供则只返回这些字段；否则返回全部
            
        Returns:
            payload 字典
        """
        if not self._entity:
            return {}
        if fields:
            return {f: self._entity.get(f) for f in fields}
        return dict(self._entity)
    
    def to_hit_dict(
        self,
        identity_field: str = "item_id",
        metric_type: str = "COSINE",
        score_type: str = "dense",
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        构建标准化的 hit 字典，可直接用于 RRF 合并。
        
        Args:
            identity_field: 用作 identity 的字段名
            metric_type: 距离度量类型 (COSINE/IP/L2/BM25)
            score_type: 得分类型 ("dense" 或 "sparse")
            fields: 可选的字段列表
            
        Returns:
            标准化的 hit 字典：
            {
                "identity": ...,
                "score": ...,
                "payload": {...},
                "{score_type}_score": ...,
                "raw_score": ...,        # 原始分数（用于调试）
                "raw_similarity": ...    # 仅 dense
            }
        """
        payload = self.to_payload(fields)
        identity = payload.get(identity_field)
        raw_distance = self.distance
        
        if score_type == "sparse":
            # sparse (BM25) 需要归一化
            score = normalize_sparse_score(raw_distance)
            return {
                "identity": identity,
                "score": score,
                "payload": payload,
                "sparse_score": score,
                "raw_score": raw_distance,  # 保留原始分数用于调试
            }
        else:
            # dense 需要归一化
            score = normalize_dense_score(raw_distance, metric_type)
            return {
                "identity": identity,
                "score": score,
                "payload": payload,
                "dense_score": score,
                "raw_similarity": raw_distance,
            }
    
    @classmethod
    def extract_all(
        cls,
        results: Any,
        identity_field: str = "item_id",
        metric_type: str = "COSINE",
        score_type: str = "dense",
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        批量提取搜索结果，返回标准化的 hit 列表。
        
        Args:
            results: Milvus search 返回的结果 (List[List[Hit]])
            identity_field: 用作 identity 的字段名
            metric_type: 距离度量类型
            score_type: 得分类型
            fields: 可选的字段列表
            
        Returns:
            标准化的 hit 字典列表
        """
        hits: List[Dict[str, Any]] = []
        if not results or not results[0]:
            return hits
        
        for hit in results[0]:
            extractor = cls(hit)
            hits.append(
                extractor.to_hit_dict(
                    identity_field=identity_field,
                    metric_type=metric_type,
                    score_type=score_type,
                    fields=fields,
                )
            )
        return hits


def build_sparse_query(text: str) -> Dict[str, List[float] | str]:
    """
    根据用户输入构建 BM25 查询文本（Milvus 侧可直接用文本 + BM25 Function）。

    - 先做分词/规范化，确保中文被拆成空格分隔的 token，从而兼容默认 analyzer
      （默认 analyzer 不对中文做分词，直接传原句会全部当作单 token）。
    - 同时保留稀疏向量（indices/values），便于需要手工向量的调用场景。
    """
    bm25_text = prepare_bm25_text([text], RetrievalConfig.bm25_text_limit())
    tokens = tokenize_for_bm25(bm25_text)
    # 将 tokens 以空格拼回，保证 BM25 Function 能够正确分词匹配
    tokenized_text = " ".join(tokens) if tokens else bm25_text
    sparse_vector = build_sparse_vector(tokenized_text)
    payload = None
    if _SparseVector and sparse_vector["indices"]:
        try:
            payload = _SparseVector(
                dict(zip(sparse_vector["indices"], sparse_vector["values"]))
            )
        except Exception:
            payload = None
    return {
        "text": tokenized_text,
        "raw_text": bm25_text,
        "indices": sparse_vector["indices"],
        "values": sparse_vector["values"],
        "token_count": len(tokens),
        "payload": payload,  # 若 SDK 暴露 SparseVector，返回可直接用于 search 的对象
    }


def normalize_sparse_score(
    score: float,
    method: Optional[str] = None,
    scale: Optional[float] = None,
) -> float:
    """
    BM25 分数归一化到 [0, 1] 区间。
    
    BM25 原始分数范围不固定（可能 0~50+），需要归一化以便与其他分数融合。
    
    Args:
        score: BM25 原始分数
        method: 归一化方法，支持 "sigmoid" / "minmax"，默认从配置读取
        scale: 缩放因子，默认从配置读取
        
    Returns:
        归一化后的分数 [0, 1]
    """
    from server.config import RetrievalConfig
    
    if score is None:
        return 0.0
    if score < 0:
        return 0.0
    
    # 从配置读取参数
    if method is None:
        method = RetrievalConfig.score_normalization_method()
    if scale is None:
        scale = RetrievalConfig.score_normalization_sparse_scale()
    
    if method == "sigmoid":
        # Sigmoid 归一化：将分数映射到 (0, 1)
        # scale 控制曲线陡峭程度，scale 越大曲线越平缓
        return 1.0 / (1.0 + math.exp(-score / scale))
    elif method == "minmax":
        # MinMax 归一化：线性映射，超过 scale 的截断为 1
        return min(1.0, score / scale)
    else:
        # 默认 sigmoid
        return 1.0 / (1.0 + math.exp(-score / scale))


def normalize_dense_score(
    distance: Optional[float],
    metric: Optional[str] = None,
) -> float:
    """
    根据 Milvus metric 类型将原始 distance 转为可比对的得分。

    - COSINE/IP：Milvus 已返回相似度（distance 越大越相似或本身即余弦值），直接透传
    - L2/L2_SQUARED：距离越小越相似，转换为 [0,1] 区间
    - 其他未识别 metric：回退到历史逻辑（1 - distance，截断到 [0,1]）
    """
    if distance is None:
        return 0.0

    metric_name = (metric or "").upper()
    value = float(distance)

    if metric_name in {"COSINE", "IP", "INNER_PRODUCT"}:
        return value

    if metric_name in {"L2", "L2_SQUARED", "EUCLIDEAN"}:
        return 1.0 / (1.0 + max(value, 0.0))

    return max(0.0, min(1.0, 1.0 - value))


def rrf_merge_hits(
    hits_per_source: Dict[str, List[Dict]],
    k: Optional[int] = None,
    weights: Optional[Dict[str, float]] = None,
) -> List[Dict]:
    """
    将多个来源的候选结果按照加权RRF规则合并。

    每个 hit 需包含：
        - identity: 唯一标识
        - score: 该来源得分
        - payload: 原始实体（Milvus返回的字段集合）
    
    Args:
        hits_per_source: 各来源的检索结果，格式为 {source_name: [hit, ...]}
        k: RRF k参数，越小分数区分度越大
        weights: 各来源权重，格式为 {source_name: weight}，为None时从配置读取
    
    Returns:
        融合后的结果列表，按RRF分数降序排列
    """
    from server.config import RetrievalConfig

    if k is None:
        k = RetrievalConfig.rrf_k()
    
    # 获取权重配置
    if weights is None:
        weights = {
            "dense": RetrievalConfig.rrf_dense_weight(),
            "sparse": RetrievalConfig.rrf_sparse_weight(),
        }
    
    aggregated: Dict[str, Dict] = {}

    for source_name, hits in hits_per_source.items():
        if not hits:
            continue
        
        # 获取该来源的权重，默认为1.0
        source_weight = weights.get(source_name, 1.0)
        
        for rank, hit in enumerate(hits, start=1):
            identity = hit.get("identity")
            if not identity:
                continue

            entry = aggregated.setdefault(
                identity,
                {
                    "identity": identity,
                    "payload": hit.get("payload"),
                    "rrf_score": 0.0,
                    "source_scores": {},
                },
            )
            # 加权RRF公式
            contribution = source_weight * (1.0 / (k + rank))
            entry["rrf_score"] += contribution
            entry["source_scores"][source_name] = {
                "rank": rank,
                "score": hit.get("score", 0.0),
                "raw_score": hit.get("raw_score"),  # 保留原始分数
                "weight": source_weight,
                "contribution": contribution,
            }
            # 方便调试：保留各路召回原始得分
            entry[f"{source_name}_score"] = hit.get("score")
            entry[f"{source_name}_rank"] = rank
            raw_similarity = hit.get("raw_similarity")
            if raw_similarity is not None:
                entry[f"{source_name}_raw_similarity"] = raw_similarity
                if source_name == "dense":
                    entry["raw_similarity"] = raw_similarity
            # 保留 sparse 原始分数
            raw_score = hit.get("raw_score")
            if raw_score is not None:
                entry[f"{source_name}_raw_score"] = raw_score

    merged = list(aggregated.values())
    merged.sort(key=lambda item: item["rrf_score"], reverse=True)
    return merged


def rrf_merge_dual_channel(
    dense_hits: List[Dict],
    sparse_hits: List[Dict],
    k: Optional[int] = None,
    dense_weight: Optional[float] = None,
    sparse_weight: Optional[float] = None,
) -> List[Dict]:
    """
    双通道（Dense + Sparse）RRF融合快捷函数。
    
    支持通过 feature_switches 控制各通道的启用/禁用：
    - dense_enabled: 控制Dense通道
    - sparse_enabled: 控制Sparse通道
    - rrf_enabled: 控制RRF融合（禁用时仅返回Dense或Sparse结果）
    
    Args:
        dense_hits: Dense检索结果
        sparse_hits: Sparse检索结果
        k: RRF k参数
        dense_weight: Dense权重，为None时从配置读取
        sparse_weight: Sparse权重，为None时从配置读取
    
    Returns:
        融合后的结果列表
    """
    from server.config import RetrievalConfig

    if k is None:
        k = RetrievalConfig.rrf_k()
    
    # 检查开关状态
    dense_enabled = RetrievalConfig.dense_enabled()
    sparse_enabled = RetrievalConfig.sparse_enabled()
    rrf_enabled = RetrievalConfig.rrf_enabled()
    
    # 根据开关过滤通道
    effective_dense = dense_hits if dense_enabled else []
    effective_sparse = sparse_hits if sparse_enabled else []
    
    # 如果RRF禁用，返回单通道结果
    if not rrf_enabled:
        if dense_enabled and effective_dense:
            # 返回Dense结果，添加rrf_score字段以保持接口一致
            for hit in effective_dense:
                hit["rrf_score"] = hit.get("score", 0.0)
            return effective_dense
        elif sparse_enabled and effective_sparse:
            for hit in effective_sparse:
                hit["rrf_score"] = hit.get("score", 0.0)
            return effective_sparse
        return []
    
    # 如果只有一个通道启用，直接返回该通道结果
    if not dense_enabled and sparse_enabled:
        for hit in effective_sparse:
            hit["rrf_score"] = hit.get("score", 0.0)
        return effective_sparse
    elif dense_enabled and not sparse_enabled:
        for hit in effective_dense:
            hit["rrf_score"] = hit.get("score", 0.0)
        return effective_dense
    
    # 正常RRF融合
    weights = {
        "dense": dense_weight if dense_weight is not None else RetrievalConfig.rrf_dense_weight(),
        "sparse": sparse_weight if sparse_weight is not None else RetrievalConfig.rrf_sparse_weight(),
    }
    
    return rrf_merge_hits(
        {"dense": effective_dense, "sparse": effective_sparse},
        k=k,
        weights=weights,
    )
