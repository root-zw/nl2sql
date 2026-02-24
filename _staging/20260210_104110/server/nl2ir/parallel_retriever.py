"""
并行检索协调器

移除串行依赖，实现业务域、表、枚举、Few-Shot的并行检索。
后置软过滤替代前置硬过滤。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from uuid import UUID

import structlog

from server.config import settings, RetrievalConfig

if TYPE_CHECKING:
    from server.nl2ir.domain_detector import DomainDetector, DomainResult
    from server.nl2ir.table_retriever import TableRetriever, TableCandidate
    from server.nl2ir.enum_value_retriever import EnumValueRetriever
    from server.nl2ir.few_shot_retriever import FewShotRetriever

logger = structlog.get_logger()


@dataclass
class RetrievalBundle:
    """
    检索结果束
    
    包含所有并行检索通道的结果，供后续融合和过滤使用。
    """
    # 业务域检索结果
    domain_result: Optional["DomainResult"] = None
    domain_candidates: List[Dict[str, Any]] = field(default_factory=list)
    
    # 表检索结果
    table_candidates: List[Any] = field(default_factory=list)
    
    # 枚举值检索结果
    enum_matches: List[Dict[str, Any]] = field(default_factory=list)
    
    # Few-Shot检索结果
    few_shot_samples: List[Dict[str, Any]] = field(default_factory=list)
    
    # 度量字段检索结果
    measure_matches: List[Dict[str, Any]] = field(default_factory=list)
    
    # 元信息
    question: str = ""
    connection_id: Optional[str] = None
    query_vector: Optional[List[float]] = None
    
    # 检索耗时（毫秒）
    timing: Dict[str, float] = field(default_factory=dict)
    
    # 各通道原始结果数量
    raw_counts: Dict[str, int] = field(default_factory=dict)
    
    def get_primary_domain_id(self) -> Optional[str]:
        """获取主业务域ID"""
        if self.domain_result and self.domain_result.candidates:
            return str(self.domain_result.primary_domain_id)
        return None
    
    def get_top_table_ids(self, limit: int = 3) -> List[str]:
        """获取排名靠前的表ID"""
        return [
            str(t.table_id) if hasattr(t, "table_id") else str(t.get("table_id"))
            for t in self.table_candidates[:limit]
        ]
    
    def to_trace_dict(self) -> Dict[str, Any]:
        """转换为可序列化的trace字典"""
        return {
            "question": self.question,
            "connection_id": self.connection_id,
            "domain_id": self.get_primary_domain_id(),
            "table_count": len(self.table_candidates),
            "enum_count": len(self.enum_matches),
            "few_shot_count": len(self.few_shot_samples),
            "measure_count": len(self.measure_matches),
            "timing": self.timing,
            "raw_counts": self.raw_counts,
        }


class ParallelRetriever:
    """
    并行检索协调器
    
    特点：
    - 所有检索通道并行执行（无前置硬过滤）
    - 共享query_vector避免重复embedding
    - 后置软过滤替代前置硬过滤
    """
    
    def __init__(
        self,
        domain_detector: Optional["DomainDetector"] = None,
        table_retriever: Optional["TableRetriever"] = None,
        enum_retriever: Optional["EnumValueRetriever"] = None,
        few_shot_retriever: Optional["FewShotRetriever"] = None,
        embedding_client: Optional[Any] = None,
    ):
        self.domain_detector = domain_detector
        self.table_retriever = table_retriever
        self.enum_retriever = enum_retriever
        self.few_shot_retriever = few_shot_retriever
        self.embedding_client = embedding_client
    
    async def retrieve_all(
        self,
        question: str,
        connection_id: str,
        query_vector: Optional[List[float]] = None,
    ) -> RetrievalBundle:
        """
        并行执行所有检索
        
        Args:
            question: 用户问题
            connection_id: 数据库连接ID
            query_vector: 预计算的query向量，为None时自动生成
        
        Returns:
            RetrievalBundle 包含所有检索结果
        """
        import time
        
        bundle = RetrievalBundle(
            question=question,
            connection_id=connection_id,
        )
        
        # 生成query向量（如果未提供）
        if query_vector is None and self.embedding_client:
            try:
                query_vector = await self.embedding_client.embed_single(question)
            except Exception as e:
                logger.warning("生成query向量失败", error=str(e))
        
        bundle.query_vector = query_vector
        
        # 构建并行任务
        tasks = {}
        
        if self.domain_detector:
            tasks["domain"] = self._detect_domain(question, query_vector)
        
        if self.table_retriever:
            # 注意：不传domain_id，实现无前置过滤
            tasks["table"] = self._retrieve_tables(question, connection_id, query_vector)
        
        if self.enum_retriever:
            tasks["enum"] = self._match_enums(question, connection_id)
        
        if self.few_shot_retriever:
            tasks["few_shot"] = self._retrieve_few_shots(question, connection_id, query_vector)
        
        # 并行执行所有任务
        if tasks:
            start_time = time.perf_counter()
            
            # 使用gather并行执行
            results = await asyncio.gather(
                *tasks.values(),
                return_exceptions=True,
            )
            
            total_time = (time.perf_counter() - start_time) * 1000
            bundle.timing["total"] = round(total_time, 2)
            
            # 处理结果
            for task_name, result in zip(tasks.keys(), results):
                if isinstance(result, Exception):
                    logger.warning(f"检索任务失败: {task_name}", error=str(result))
                    continue
                
                if task_name == "domain" and result:
                    bundle.domain_result = result.get("result")
                    bundle.domain_candidates = result.get("candidates", [])
                    bundle.timing["domain"] = result.get("timing", 0)
                    bundle.raw_counts["domain"] = len(bundle.domain_candidates)
                
                elif task_name == "table" and result:
                    bundle.table_candidates = result.get("candidates", [])
                    bundle.timing["table"] = result.get("timing", 0)
                    bundle.raw_counts["table"] = len(bundle.table_candidates)
                
                elif task_name == "enum" and result:
                    bundle.enum_matches = result.get("matches", [])
                    bundle.timing["enum"] = result.get("timing", 0)
                    bundle.raw_counts["enum"] = len(bundle.enum_matches)
                
                elif task_name == "few_shot" and result:
                    bundle.few_shot_samples = result.get("samples", [])
                    bundle.timing["few_shot"] = result.get("timing", 0)
                    bundle.raw_counts["few_shot"] = len(bundle.few_shot_samples)
        
        logger.debug(
            "并行检索完成",
            question=question[:50],
            timing=bundle.timing,
            raw_counts=bundle.raw_counts,
        )
        
        return bundle
    
    async def _detect_domain(
        self,
        question: str,
        query_vector: Optional[List[float]],
    ) -> Dict[str, Any]:
        """业务域检测任务"""
        import time
        start = time.perf_counter()
        
        try:
            result = await self.domain_detector.detect(
                question,
                query_vector=query_vector,
            )
            
            candidates = []
            if result and result.candidates:
                candidates = [
                    {
                        "domain_id": str(c.domain_id),
                        "domain_name": c.domain_name,
                        "dense_score": c.dense_score,
                        "sparse_score": c.sparse_score,
                        "rrf_score": c.rrf_score,
                    }
                    for c in result.candidates
                ]
            
            return {
                "result": result,
                "candidates": candidates,
                "timing": round((time.perf_counter() - start) * 1000, 2),
            }
        except Exception as e:
            logger.error("业务域检测失败", error=str(e))
            return {"result": None, "candidates": [], "timing": 0}
    
    async def _retrieve_tables(
        self,
        question: str,
        connection_id: str,
        query_vector: Optional[List[float]],
    ) -> Dict[str, Any]:
        """表检索任务（无domain过滤）"""
        import time
        start = time.perf_counter()
        
        try:
            # 注意：domain_id=None 实现无前置过滤
            candidates = await self.table_retriever.retrieve_relevant_tables(
                question=question,
                connection_id=connection_id,
                domain_id=None,
                query_vector=query_vector,
            )
            
            return {
                "candidates": candidates,
                "timing": round((time.perf_counter() - start) * 1000, 2),
            }
        except Exception as e:
            logger.error("表检索失败", error=str(e))
            return {"candidates": [], "timing": 0}
    
    async def _match_enums(
        self,
        question: str,
        connection_id: str,
    ) -> Dict[str, Any]:
        """枚举值匹配任务"""
        import time
        start = time.perf_counter()
        
        try:
            matches = await self.enum_retriever.match(
                question=question,
                connection_id=connection_id,
            )
            
            return {
                "matches": matches,
                "timing": round((time.perf_counter() - start) * 1000, 2),
            }
        except Exception as e:
            logger.error("枚举匹配失败", error=str(e))
            return {"matches": [], "timing": 0}
    
    async def _retrieve_few_shots(
        self,
        question: str,
        connection_id: str,
        query_vector: Optional[List[float]],
    ) -> Dict[str, Any]:
        """Few-Shot检索任务"""
        import time
        start = time.perf_counter()
        
        try:
            samples = await self.few_shot_retriever.retrieve(
                question=question,
                connection_id=connection_id,
                query_vector=query_vector,
            )
            
            return {
                "samples": samples,
                "timing": round((time.perf_counter() - start) * 1000, 2),
            }
        except Exception as e:
            logger.error("Few-Shot检索失败", error=str(e))
            return {"samples": [], "timing": 0}


class SoftDomainFilter:
    """
    后置软过滤器
    
    基于业务域置信度进行软过滤/加权，而非硬过滤。
    """
    
    def __init__(
        self,
        high_confidence_threshold: float = 0.6,
        match_boost_factor: float = 1.2,
    ):
        """
        初始化软过滤器
        
        Args:
            high_confidence_threshold: 高置信度阈值
            match_boost_factor: 匹配域加权因子
        """
        self.high_confidence_threshold = high_confidence_threshold
        self.match_boost_factor = match_boost_factor
    
    @classmethod
    def from_config(cls) -> "SoftDomainFilter":
        """从配置创建实例"""
        from server.config import get_retrieval_param
        return cls(
            high_confidence_threshold=get_retrieval_param(
                "soft_filter.domain.high_confidence_threshold", 0.6
            ),
            match_boost_factor=get_retrieval_param(
                "soft_filter.domain.match_boost_factor", 1.2
            ),
        )
    
    def apply(self, bundle: RetrievalBundle) -> RetrievalBundle:
        """
        应用软过滤
        
        策略：
        - 高置信度：对匹配域的表加权
        - 低置信度：保留所有候选，不过滤
        
        Args:
            bundle: 检索结果束
        
        Returns:
            处理后的检索结果束
        """
        # 获取主业务域信息
        primary_domain_id = bundle.get_primary_domain_id()
        domain_confidence = 0.0
        
        if bundle.domain_result and bundle.domain_result.candidates:
            top_candidate = bundle.domain_result.candidates[0]
            domain_confidence = top_candidate.dense_score or 0.0
        
        # 根据置信度决定处理策略
        if domain_confidence >= self.high_confidence_threshold and primary_domain_id:
            # 高置信度：对匹配域的表加权
            for table in bundle.table_candidates:
                table_domain_id = None
                
                if hasattr(table, "evidence") and table.evidence:
                    table_domain_id = table.evidence.get("domain_id")
                elif hasattr(table, "domain_id"):
                    table_domain_id = str(table.domain_id)
                elif isinstance(table, dict):
                    table_domain_id = table.get("domain_id")
                
                if table_domain_id and str(table_domain_id) == primary_domain_id:
                    # 加权匹配域的表
                    if hasattr(table, "score"):
                        table.score *= self.match_boost_factor
                    elif isinstance(table, dict) and "score" in table:
                        table["score"] *= self.match_boost_factor
            
            # 重新排序
            bundle.table_candidates.sort(
                key=lambda t: (
                    t.score if hasattr(t, "score") 
                    else t.get("score", 0) if isinstance(t, dict) 
                    else 0
                ),
                reverse=True,
            )
            
            logger.debug(
                "应用域加权",
                domain_id=primary_domain_id,
                confidence=domain_confidence,
                boost_factor=self.match_boost_factor,
            )
        else:
            # 低置信度：保留所有候选，不过滤
            logger.debug(
                "域置信度不足，保留所有候选",
                confidence=domain_confidence,
                threshold=self.high_confidence_threshold,
            )
        
        return bundle


class FewShotTableBooster:
    """
    Few-Shot表增强器
    
    从高相似度Few-Shot样本中提取推荐表，并对这些表加权。
    """
    
    def __init__(
        self,
        min_similarity: float = 0.8,
        boost_factor: float = 1.1,
    ):
        self.min_similarity = min_similarity
        self.boost_factor = boost_factor
    
    def apply(self, bundle: RetrievalBundle) -> RetrievalBundle:
        """
        从Few-Shot提取推荐表并加权
        
        Args:
            bundle: 检索结果束
        
        Returns:
            处理后的检索结果束
        """
        if not bundle.few_shot_samples or not bundle.table_candidates:
            return bundle
        
        # 提取高相似度Few-Shot推荐的表
        recommended_tables = set()
        for sample in bundle.few_shot_samples:
            similarity = sample.get("similarity", 0)
            if similarity >= self.min_similarity:
                tables = sample.get("tables") or sample.get("json_meta", {}).get("tables", [])
                if isinstance(tables, list):
                    for t in tables:
                        if isinstance(t, dict):
                            table_id = t.get("table_id")
                            if table_id:
                                recommended_tables.add(str(table_id))
                        elif isinstance(t, str):
                            recommended_tables.add(t)
        
        if not recommended_tables:
            return bundle
        
        # 对推荐表加权
        for table in bundle.table_candidates:
            table_id = None
            if hasattr(table, "table_id"):
                table_id = str(table.table_id)
            elif isinstance(table, dict):
                table_id = str(table.get("table_id"))
            
            if table_id and table_id in recommended_tables:
                if hasattr(table, "score"):
                    table.score *= self.boost_factor
                elif isinstance(table, dict) and "score" in table:
                    table["score"] *= self.boost_factor
        
        # 重新排序
        bundle.table_candidates.sort(
            key=lambda t: (
                t.score if hasattr(t, "score") 
                else t.get("score", 0) if isinstance(t, dict) 
                else 0
            ),
            reverse=True,
        )
        
        logger.debug(
            "Few-Shot表加权完成",
            recommended_tables=len(recommended_tables),
            boost_factor=self.boost_factor,
        )
        
        return bundle

