"""业务域智能检测器"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import asyncio
import structlog

from server.config import settings, RetrievalConfig, get_retrieval_param
from server.models.semantic import SemanticModel
from server.nl2ir.hybrid_utils import (
    build_sparse_query,
    normalize_dense_score,
    rrf_merge_dual_channel,
    HitExtractor,
)

logger = structlog.get_logger()

# 从配置读取业务域候选数量，默认值为3（向后兼容）
def get_max_domain_candidates() -> int:
    """获取业务域最大候选数量（从配置文件读取）"""
    return RetrievalConfig.domain_top_k()


def get_domain_threshold_margin() -> float:
    """获取业务域阈值边界余量（从配置文件读取）"""
    from server.config import get_retrieval_param
    return get_retrieval_param("domain_retrieval.threshold_margin", 0.05)


def get_domain_score_gap() -> float:
    """获取业务域分差阈值（从配置文件读取）"""
    from server.config import get_retrieval_param
    return get_retrieval_param("domain_retrieval.score_gap_threshold", 0.05)


@dataclass
class DomainCandidate:
    """单个业务域候选"""

    domain_id: Optional[str]
    domain_name: Optional[str]
    dense_score: float = 0.0
    sparse_score: float = 0.0
    rrf_score: float = 0.0
    rank: int = 0
    source_scores: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DomainDetectionResult:
    """业务域检测结果（含多候选）"""

    primary_domain_id: Optional[str]
    primary_domain_name: Optional[str]
    detection_method: Optional[str]
    candidates: List[DomainCandidate] = field(default_factory=list)
    fallback_reason: Optional[str] = None

    def candidate_ids(self) -> List[str]:
        return [
            candidate.domain_id
            for candidate in self.candidates
            if candidate.domain_id
        ]


class DomainDetector:
    """业务域智能检测器
    
    支持两种检测方式：
    1. 向量检索（主要）：Milvus语义搜索，理解复杂问题
    2. 关键词匹配（Fallback）：简单快速，适合简单问题或向量检索失败时
    """
    
    def __init__(
        self,
        semantic_model: SemanticModel,
        milvus_client=None,
        embedding_client=None,
        use_vector_search: bool = True,
        connection_id: Optional[str] = None,
    ):
        """
        初始化业务域检测器
        
        Args:
            semantic_model: 语义模型
            milvus_client: Milvus客户端（可选）
            embedding_client: Embedding客户端（可选）
            use_vector_search: 是否启用向量检索（默认True）
        """
        self.model = semantic_model
        self.milvus_client = milvus_client
        self.embedding_client = embedding_client
        self.use_vector_search = use_vector_search
        self._domain_cache: Dict[str, Dict] = {}  # {domain_id: {keywords, priority}}
        self._initialized = False
        self.connection_id: Optional[str] = None
        self.last_detection_method: Optional[str] = None
        self.connection_id = connection_id
        self.collection_name = getattr(
            milvus_client, "collection_name", settings.milvus_collection
        )
        self.last_domain_result: Optional[DomainDetectionResult] = None
    def set_clients(self, milvus_client, embedding_client, connection_id: Optional[str]):
        """在运行时补充 Milvus / Embedding 客户端。"""
        if milvus_client:
            self.milvus_client = milvus_client
            self.collection_name = getattr(
                milvus_client, "collection_name", settings.milvus_collection
            )
        if embedding_client:
            self.embedding_client = embedding_client
        if connection_id:
            self.connection_id = connection_id
    
    async def initialize(self, db_manager, connection_id: Optional[str] = None):
        """从数据库加载业务域信息（按连接缓存）"""
        if self._initialized and connection_id == self.connection_id:
            return
        
        try:
            async with db_manager.pool.acquire() as conn:
                query = """
                    SELECT domain_id, domain_code, domain_name, keywords, sort_order
                    FROM business_domains
                    WHERE is_active = TRUE
                """
                params: List = []
                if connection_id:
                    query += " AND connection_id = $1"
                    params.append(connection_id)
                query += " ORDER BY sort_order"
                
                rows = await conn.fetch(query, *params)
                
                self._domain_cache.clear()
                for row in rows:
                    keywords = row['keywords']
                    if isinstance(keywords, str):
                        import json
                        keywords = json.loads(keywords) if keywords else []
                    
                    self._domain_cache[str(row['domain_id'])] = {
                        'code': row['domain_code'],
                        'name': row['domain_name'],
                        'keywords': keywords or [],
                        'priority': 10 - row['sort_order']  # sort_order越小，优先级越高
                    }
                logger.info(
                    "业务域检测器初始化完成",
                    domain_count=len(self._domain_cache),
                    connection_id=connection_id
                )
                self.connection_id = connection_id
                self._initialized = True
        except Exception as e:
            logger.error(
                "业务域检测器初始化失败",
                error=str(e),
                connection_id=connection_id
            )
            self._initialized = False
    
    async def detect(
        self,
        question: str,
        user_specified_domain: Optional[str] = None,
        query_vector: Optional[List[float]] = None
    ) -> DomainDetectionResult:
        """检测问题所属的业务域（支持向量检索+关键词Fallback）"""
        # 尽量保证域缓存可用：若未走 DB initialize，则从 SemanticModel 兜底构建
        self._ensure_domain_cache_initialized()
        # 1. 用户指定优先
        if user_specified_domain:
            logger.debug("使用用户指定的业务域", domain=user_specified_domain)
            candidate = DomainCandidate(
                domain_id=user_specified_domain,
                domain_name=self.get_domain_name(user_specified_domain),
                dense_score=1.0,
                sparse_score=0.0,
                rrf_score=1.0,
                rank=1,
                source_scores={"manual": {"score": 1.0, "rank": 1}},
            )
            result = DomainDetectionResult(
                primary_domain_id=user_specified_domain,
                primary_domain_name=candidate.domain_name,
                detection_method="user_specified",
                candidates=[candidate],
            )
            self._set_last_result(result)
            return result
        
        # 2. 如果没有加载业务域数据，返回空结果
        if not self._initialized or not self._domain_cache:
            logger.warning("业务域缓存未初始化")
            result = DomainDetectionResult(
                primary_domain_id=None,
                primary_domain_name=None,
                detection_method="unavailable",
                candidates=[],
                fallback_reason="domain_cache_uninitialized",
            )
            self._set_last_result(result)
            return result
        
        domain_count = len(self._domain_cache)
        logger.debug("开始业务域检测", domain_count=domain_count, question_preview=question[:50])
        
        candidates: List[DomainCandidate] = []
        detection_method: Optional[str] = None
        fallback_reason: Optional[str] = None
        
        # 3. 如果启用向量检索且配置了Milvus，优先使用向量检索
        if self.use_vector_search and self.milvus_client and self.embedding_client:
            try:
                logger.debug("尝试向量检索业务域")
                vector_candidates = await self._vector_detect(question, query_vector)
                if vector_candidates:
                    candidates = self._select_domain_candidates(vector_candidates)
                    detection_method = "vector"
                else:
                    logger.debug("向量检索无候选，准备回退")
                    detection_method = "vector_failed"
                    fallback_reason = "vector_empty"
            except Exception as e:
                logger.warning("向量检索失败，降级到关键词匹配", error=str(e))
                detection_method = "vector_exception"
                fallback_reason = "vector_exception"
        else:
            logger.debug(
                "向量检索未启用，使用关键词匹配",
                use_vector=self.use_vector_search,
                has_milvus=bool(self.milvus_client),
                has_embedding=bool(self.embedding_client)
            )
            detection_method = "vector_disabled"
            fallback_reason = "vector_disabled"
        
        # 4. Fallback：使用关键词匹配
        if not candidates:
            keyword_candidates = self._keyword_detect(question)
            if keyword_candidates:
                candidates = keyword_candidates
                detection_method = "keyword"
            else:
                logger.debug("关键词匹配也未找到业务域")
        
        primary_candidate = candidates[0] if candidates else None
        result = DomainDetectionResult(
            primary_domain_id=primary_candidate.domain_id if primary_candidate else None,
            primary_domain_name=primary_candidate.domain_name if primary_candidate else None,
            detection_method=detection_method,
            candidates=candidates,
            fallback_reason=fallback_reason,
        )
        self._set_last_result(result)
        return result

    def _ensure_domain_cache_initialized(self) -> None:
        """
        确保域缓存已初始化。

        说明：
        - 线上主流程不一定会显式调用 initialize(db_manager)，导致 _domain_cache 为空，
          进而域识别常为 None，domain_match 等后置特征失效。
        - 此处提供“语义模型兜底初始化”，不依赖 DB。
        """
        if self._initialized and self._domain_cache:
            return
        domains = getattr(self.model, "domains", None) if self.model else None
        if not isinstance(domains, dict) or not domains:
            return
        try:
            self._domain_cache.clear()
            # sort_order 越小越优先（沿用 DB initialize 的 priority 逻辑）
            for domain_id, domain in domains.items():
                if not domain_id or not domain:
                    continue
                keywords = getattr(domain, "keywords", None) or []
                sort_order = int(getattr(domain, "sort_order", 0) or 0)
                priority = max(1, min(10, 10 - sort_order))
                self._domain_cache[str(domain_id)] = {
                    "code": getattr(domain, "domain_code", "") or "",
                    "name": getattr(domain, "domain_name", "") or "",
                    "keywords": list(keywords) if isinstance(keywords, list) else [],
                    "priority": priority,
                }
            if self._domain_cache:
                self._initialized = True
                logger.debug("业务域缓存使用 SemanticModel 兜底初始化", domain_count=len(self._domain_cache))
        except Exception as exc:
            logger.warning("SemanticModel 兜底初始化业务域缓存失败", error=str(exc))

    async def _vector_detect(
        self,
        question: str,
        query_vector: Optional[List[float]] = None
    ) -> List[DomainCandidate]:
        """
        向量检索业务域（语义理解）
        """
        try:
            dense_enabled = RetrievalConfig.dense_enabled()
            sparse_enabled = RetrievalConfig.sparse_enabled()

            # 1. 生成问题向量（如果未提供）
            if dense_enabled and query_vector is None:
                query_vector = await self.embedding_client.embed_single(question)
                logger.debug("问题向量生成完成", vector_dim=len(query_vector))
            else:
                if query_vector is not None:
                    logger.debug("复用已有问题向量", vector_dim=len(query_vector))
            
            # 2. 在Milvus中搜索业务域
            filter_expr = self._build_filter_expr()

            dense_task = (
                asyncio.create_task(self._search_dense(query_vector, filter_expr))
                if dense_enabled
                else None
            )
            sparse_task = (
                asyncio.create_task(self._search_sparse(question, filter_expr))
                if sparse_enabled
                else None
            )
            dense_hits = await dense_task if dense_task else []
            sparse_hits = await sparse_task if sparse_task else []
            merged = rrf_merge_dual_channel(
                dense_hits,
                sparse_hits,
                k=RetrievalConfig.rrf_k(),
                dense_weight=RetrievalConfig.domain_dense_weight(),
                sparse_weight=RetrievalConfig.domain_sparse_weight(),
            )

            if not merged:
                logger.debug("业务域混合检索无结果")
                return []

            candidates: List[DomainCandidate] = []
            for idx, entry in enumerate(merged):
                payload = entry.get("payload") or {}
                domain_id = payload.get("item_id")
                domain_name = payload.get("display_name")
                dense_score = entry.get("dense_score")
                if dense_score is None:
                    dense_score = (
                        (entry.get("source_scores") or {})
                        .get("dense", {})
                        .get("score")
                    )
                sparse_score = entry.get("sparse_score")
                if sparse_score is None:
                    sparse_score = (
                        (entry.get("source_scores") or {})
                        .get("sparse", {})
                        .get("score")
                    )
                candidates.append(
                    DomainCandidate(
                        domain_id=domain_id,
                        domain_name=domain_name,
                        dense_score=float(dense_score or 0.0),
                        sparse_score=float(sparse_score or 0.0),
                        rrf_score=float(entry.get("rrf_score", 0.0)),
                        rank=idx + 1,
                        source_scores=entry.get("source_scores", {}),
                    )
                )
            return candidates
            
        except Exception as e:
            logger.exception("向量检索异常", error=str(e))
            raise

    def _build_filter_expr(self) -> str:
        filters = ['entity_type == "domain"', "is_active == true"]
        if self.connection_id:
            filters.append(f'connection_id == "{self.connection_id}"')
        return " and ".join(filters)

    async def _search_dense(self, query_vector, filter_expr: str) -> List[Dict]:
        """Dense 向量检索（使用 HitExtractor 统一处理）。"""
        output_fields = ["item_id", "display_name", "description"]
        # 确保 limit 至少等于配置的 top_k，以便有足够候选进行后续筛选
        search_limit = max(5, get_max_domain_candidates())
        nprobe = int(get_retrieval_param("domain_retrieval.milvus_search_params.dense.nprobe", 10) or 10)
        results = await asyncio.to_thread(
            self.milvus_client.search,
            collection_name=self.collection_name,
            data=[query_vector],
            anns_field="dense_vector",
            search_params={"metric_type": "COSINE", "params": {"nprobe": nprobe}},
            limit=search_limit,
            filter=filter_expr,
            output_fields=output_fields,
        )
        return HitExtractor.extract_all(
            results,
            identity_field="item_id",
            metric_type="COSINE",
            score_type="dense",
            fields=output_fields,
        )

    async def _search_sparse(self, question: str, filter_expr: str) -> List[Dict]:
        """Sparse BM25 检索（使用 HitExtractor 统一处理）。"""
        sparse_query = build_sparse_query(question)
        if not sparse_query["text"]:
            return []

        output_fields = ["item_id", "display_name", "description"]

        # 确保 limit 至少等于配置的 top_k，以便有足够候选进行后续筛选
        search_limit = max(5, get_max_domain_candidates())
        drop_ratio_search = float(get_retrieval_param("domain_retrieval.milvus_search_params.sparse.drop_ratio_search", 0.2) or 0.2)

        async def _do_search(data):
            return await asyncio.to_thread(
                self.milvus_client.search,
                collection_name=self.collection_name,
                data=[data],
                anns_field="sparse_vector",
                search_params={
                    "metric_type": "BM25",
                    "params": {"drop_ratio_search": drop_ratio_search},
                },
                limit=search_limit,
                filter=filter_expr,
                output_fields=output_fields,
            )

        results = await _do_search(sparse_query["text"])
        # 若文本召回为空且有 SparseVector payload，使用向量再次尝试（兼容 2.6 SDK）
        if (not results or len(results[0]) == 0) and sparse_query.get("payload"):
            try:
                results = await _do_search(sparse_query["payload"])
            except Exception:
                pass

        return HitExtractor.extract_all(
            results,
            identity_field="item_id",
            metric_type="BM25",
            score_type="sparse",
            fields=output_fields,
        )
    
    def _keyword_detect(self, question: str) -> List[DomainCandidate]:
        """
        关键词匹配业务域（Fallback）
        
        Args:
            question: 用户问题
            
        Returns:
            候选列表，可能为空
        """
        domain_scores = {}
        
        for domain_id, domain_info in self._domain_cache.items():
            score = 0.0
            keywords = domain_info.get('keywords', [])
            
            # 计算匹配得分
            for keyword in keywords:
                if keyword in question:
                    # 关键词长度越长，权重越高（避免短词误匹配）
                    weight = len(keyword) / 5.0
                    score += weight
            
            # 加入优先级因子
            priority = domain_info.get('priority', 5)
            score = score * (priority / 10.0)
            
            if score > 0:
                domain_scores[domain_id] = score
        
        # 返回得分最高的域
        if domain_scores:
            sorted_domains = sorted(
                domain_scores.items(),
                key=lambda x: x[1],
                reverse=True,
            )
            candidates: List[DomainCandidate] = []
            max_candidates = get_max_domain_candidates()
            for idx, (domain_id, score) in enumerate(sorted_domains[:max_candidates]):
                info = self._domain_cache.get(domain_id, {})
                candidates.append(
                    DomainCandidate(
                        domain_id=domain_id,
                        domain_name=info.get("name"),
                        dense_score=float(score),
                        sparse_score=0.0,
                        rrf_score=float(score),
                        rank=idx + 1,
                        source_scores={"keyword": {"score": score, "rank": idx + 1}},
                    )
                )
            logger.debug(
                "业务域关键词匹配成功",
                best_domain=candidates[0].domain_name if candidates else None,
                score=candidates[0].dense_score if candidates else None,
            )
            return candidates
        
        logger.debug("未能检测到明确的业务域")
        return []
    
    def get_domain_name(self, domain_id: str) -> Optional[str]:
        """获取业务域名称"""
        if domain_id in self._domain_cache:
            return self._domain_cache[domain_id]['name']
        return None
    
    def get_all_domains(self) -> List[Dict]:
        """获取所有活跃的业务域（供前端使用）"""
        return [
            {
                'id': domain_id,
                'code': info['code'],
                'name': info['name'],
                'keywords': info['keywords']
            }
            for domain_id, info in self._domain_cache.items()
        ]

    def _select_domain_candidates(self, candidates: List[DomainCandidate]) -> List[DomainCandidate]:
        """根据阈值与分差筛选需要传递的域候选。"""
        if not candidates:
            return []
        selected: List[DomainCandidate] = [candidates[0]]
        threshold = RetrievalConfig.domain_threshold()
        top_dense = candidates[0].dense_score or 0.0
        # 从配置读取阈值参数（消除硬编码）
        threshold_margin = get_domain_threshold_margin()
        score_gap = get_domain_score_gap()
        allow_extra = top_dense < (threshold + threshold_margin)
        max_candidates = get_max_domain_candidates()

        for candidate in candidates[1:]:
            if len(selected) >= max_candidates:
                break
            dense_score = candidate.dense_score or 0.0
            dense_gap = top_dense - dense_score
            sparse_boost = candidate.sparse_score > 0
            if (
                allow_extra
                or dense_gap <= score_gap
                or sparse_boost
            ):
                selected.append(candidate)

        return selected

    def _set_last_result(self, result: DomainDetectionResult) -> None:
        """缓存最近一次检测结果，供 Trace/调试使用。"""
        self.last_domain_result = result
        self.last_domain_candidates = result.candidates
        self.last_detection_method = result.detection_method
