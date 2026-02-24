"""
双向量枚举值检索器

核心功能：
1. 双向量检索（value_vector召回 + context_vector精排）
2. 多源融合（精确匹配、同义词匹配、向量检索）
3. RRF排序融合
"""

from typing import List, Dict, Any, Optional, Tuple, Set, TYPE_CHECKING, FrozenSet
from dataclasses import dataclass, field
import asyncio
import json
import re
import structlog
import numpy as np

from server.config import settings, RetrievalConfig
from server.config import get_retrieval_param
from server.nl2ir.hybrid_utils import (
    build_sparse_query,
    normalize_dense_score,
    rrf_merge_hits,
    HitExtractor,
)
from server.utils.model_clients import RerankerClient
from server.nl2ir.sparse_utils import tokenize_for_dense
from server.nl2ir.field_precision import get_field_precision_inferencer

if TYPE_CHECKING:
    from server.nl2ir.keyword_pipeline import KeywordExtractionResult

logger = structlog.get_logger()


@dataclass
class EnumMatch:
    """枚举值匹配结果"""
    enum_id: str
    field_id: str
    field_name: str
    value: str
    display_name: str
    similarity: float  # 原始相似度
    match_type: str    # "exact" | "synonym" | "value_vector" | "context_vector"
    
    # 双向量特有字段
    value_similarity: Optional[float] = None     # value_vector相似度
    context_similarity: Optional[float] = None   # context_vector相似度
    final_score: float = 0.0                     # 综合分数
    dense_score: Optional[float] = None
    sparse_score: Optional[float] = None
    reranker_score: Optional[float] = None
    payload: Optional[Dict[str, Any]] = None
    table_name: Optional[str] = None
    table_id: Optional[str] = None
    domain_id: Optional[str] = None
    domain_name: Optional[str] = None
    
    # RRF融合信息
    rrf_sources: List[Dict[str, Any]] = field(default_factory=list)
    
    # 调试信息
    context_vector: Optional[List[float]] = None  # 用于精排的上下文向量
    trace: Dict[str, Any] = field(default_factory=dict)


class DualVectorEnumRetriever:
    """双向量枚举值检索器"""
    
    def __init__(
        self,
        db_pool,
        milvus_client,
        embedding_client,
        collection_name: str = "enum_values_dual",
        connection_id: Optional[str] = None,
        reranker: Optional[RerankerClient] = None,
    ):
        from server.config import RetrievalConfig
        
        # 基础属性
        self.db = db_pool
        self.milvus = milvus_client
        self.embedding = embedding_client
        self.collection_name = collection_name
        self.connection_id = connection_id
        self.reranker = reranker
        self.min_final_score = getattr(settings, "enum_min_final_score", 0.0)
        
        # 从配置加载参数
        self.HIGH_CONFIDENCE_THRESHOLD = RetrievalConfig.enum_high_confidence_threshold()
        self.HIGH_NOISE_THRESHOLD = RetrievalConfig.enum_high_noise_threshold()
        self.PER_FIELD_LIMIT = RetrievalConfig.enum_per_field_limit()
        self.HIGH_NOISE_FIELDS = frozenset(RetrievalConfig.enum_high_noise_fields())
        self.NEGATIVE_SIGNAL_SCORE_GAP = RetrievalConfig.enum_negative_signal_score_gap()
        self.NEGATIVE_SIGNAL_MAX_PENALTY = RetrievalConfig.enum_negative_signal_max_penalty()
    def set_context(
        self,
        connection_id: Optional[str] = None,
        reranker: Optional[RerankerClient] = None,
    ):
        if connection_id:
            self.connection_id = connection_id
        if reranker:
            self.reranker = reranker
        self.connection_id = connection_id
        self.reranker = reranker
    
    async def match_enum_values(
        self,
        user_input: str,
        candidate_fields: List[Any],  # List[Field]
        top_k: int = 5,
        use_dual_vector: bool = True,
        keyword_profile: Optional["KeywordExtractionResult"] = None,
        per_field_limit: Optional[int] = None,  # 每字段限制，None时使用配置默认值
    ) -> List[EnumMatch]:
        """
        枚举值匹配主入口
        
        Args:
            user_input: 用户输入文本
            candidate_fields: 候选字段列表
            top_k: 返回Top-K结果
            use_dual_vector: 是否使用双向量策略（否则只用value_vector）
            
        Returns:
            匹配结果列表（按final_score降序）
        """
        if not candidate_fields:
            logger.warning("候选字段列表为空")
            return []
        
        try:
            normalized_question = self._normalize_text(user_input)
            query_keywords = self._extract_query_keywords(user_input)
            candidate_fields, keyword_filtered, field_keyword_hits = self._select_relevant_fields(
                user_input,
                candidate_fields,
            )

            logger.debug(
                "开始枚举值匹配",
                user_input=user_input,
                candidate_fields=[f.display_name for f in candidate_fields],
                use_dual_vector=use_dual_vector,
                keyword_filtered=keyword_filtered,
            )
            
            # ========== 阶段1：多源并行检索 ==========
            
            results = await asyncio.gather(
                self._exact_match_all_fields(user_input, candidate_fields),
                self._synonym_match_all_fields(user_input, candidate_fields),
                self._hybrid_value_recall(
                    user_input,
                    candidate_fields,
                    top_k * 3,
                ),
                return_exceptions=True
            )
            
            exact_matches, synonym_matches, value_recalls = results
            
            # 处理异常
            if isinstance(exact_matches, Exception):
                logger.error("精确匹配失败", error=str(exact_matches))
                exact_matches = []
            if isinstance(synonym_matches, Exception):
                logger.error("同义词匹配失败", error=str(synonym_matches))
                synonym_matches = []
            if isinstance(value_recalls, Exception):
                logger.error("向量召回失败", error=str(value_recalls))
                value_recalls = []
            
            # ========== 阶段2：上下文向量精排（可选） ==========
            
            if use_dual_vector and value_recalls:
                context_reranked = await self._context_vector_rerank(
                    user_input,
                    candidate_fields,
                    value_recalls
                )
            else:
                context_reranked = value_recalls
            
            # ========== 阶段3：RRF融合 ==========
            
            all_sources = []
            
            if exact_matches:
                all_sources.append(("exact", exact_matches))
            
            if synonym_matches:
                all_sources.append(("synonym", synonym_matches))
            
            if context_reranked:
                all_sources.append(("vector", context_reranked))
            
            if not all_sources:
                logger.warning("所有检索方式均无结果")
                return []
            
            final = self._rrf_merge(all_sources, top_k * 2)

            final = self._apply_keyword_boosts(
                final,
                field_keyword_hits=field_keyword_hits,
                keyword_profile=keyword_profile,
            )

            if self.reranker and self.reranker.is_enabled():
                rerank_docs = [
                    f"Field: {m.field_name}\nValue: {m.display_name or m.value}"
                    for m in final
                ]
                try:
                    reranker_scores = await self.reranker.rerank(user_input, rerank_docs)
                    for match, score in zip(final, reranker_scores):
                        match.reranker_score = score
                        match.final_score = self._blend_reranker_score(match.final_score, score)
                except Exception as rerank_error:
                    logger.warning("枚举值Reranker失败，保留RRF结果", error=str(rerank_error))

            # ========== 新放行规则：收紧条件 ==========
            # 提取激活字段ID集合（关键词命中的字段）
            activated_field_ids = set(field_keyword_hits.keys())
            
            filtered_results = []
            for match in final:
                trace = match.trace or {}
                
                # exact/synonym 匹配直接放行
                if match.match_type in {"exact", "synonym"}:
                    trace["pass_reason"] = "exact_or_synonym"
                    match.trace = trace
                    filtered_results.append(match)
                    continue
                
                # 最低分数门槛
                if match.final_score < self.min_final_score:
                    trace["reject_reason"] = f"below_min_score({self.min_final_score})"
                    match.trace = trace
                    continue
                
                # 检查是否为高噪声字段
                is_high_noise = self._is_high_noise_field(match.field_name)
                
                # 检查字段名是否在问题中显式提及
                field_name_mentioned = self._field_name_mentioned(
                    match.field_name, normalized_question
                )
                
                # 检查值是否被提及
                value_mentioned = self._value_mentioned(match, normalized_question, query_keywords)
                
                # 检查关键词命中
                keyword_hit = match.field_id in activated_field_ids
                
                # ========== 高噪声字段特殊处理 ==========
                if is_high_noise:
                    # 高噪声字段需满足：字段名显式提及 或 分数超过高噪声阈值
                    if field_name_mentioned:
                        trace["pass_reason"] = "high_noise_field_name_mentioned"
                        match.trace = trace
                        filtered_results.append(match)
                        continue
                    if match.final_score >= self.HIGH_NOISE_THRESHOLD:
                        trace["pass_reason"] = f"high_noise_above_threshold({self.HIGH_NOISE_THRESHOLD})"
                        match.trace = trace
                        filtered_results.append(match)
                        continue
                    # 否则过滤掉
                    trace["reject_reason"] = "high_noise_field_not_qualified"
                    match.trace = trace
                    continue
                
                # ========== 普通字段放行条件（至少满足一条） ==========
                pass_conditions = []
                
                if value_mentioned:
                    pass_conditions.append("value_mentioned")
                if field_name_mentioned:
                    pass_conditions.append("field_name_mentioned")
                if keyword_hit:
                    pass_conditions.append("keyword_hit")
                if match.final_score >= self.HIGH_CONFIDENCE_THRESHOLD:
                    pass_conditions.append(f"above_threshold({self.HIGH_CONFIDENCE_THRESHOLD})")
                
                if pass_conditions:
                    trace["pass_reason"] = ",".join(pass_conditions)
                    match.trace = trace
                    filtered_results.append(match)
                else:
                    trace["reject_reason"] = "no_pass_condition_met"
                    match.trace = trace
            
            # ========== 负向信号处理：同字段多值降权 ==========
            filtered_results = self._apply_negative_signal_penalty(filtered_results)
            
            # ========== 去重和限制 ==========
            filtered_results = self._deduplicate_results(filtered_results)
            effective_per_field_limit = per_field_limit if per_field_limit is not None else self.PER_FIELD_LIMIT
            filtered_results = self._limit_per_field(
                filtered_results,
                effective_per_field_limit,
            )

            filtered_results = filtered_results[:top_k]
            
            logger.debug(
                "枚举值匹配完成",
                total_matches=len(filtered_results),
                top3=[
                    f"{m.field_name}.{m.value}({m.final_score:.3f})"
                    for m in filtered_results[:3]
                ]
            )
            
            return filtered_results
            
        except Exception as e:
            logger.exception("枚举值匹配失败", error=str(e))
            return []
    
    async def _exact_match_all_fields(
        self,
        user_input: str,
        candidate_fields: List[Any]
    ) -> List[EnumMatch]:
        """
        精确匹配（PostgreSQL）
        
        支持两种匹配模式：
        1. 枚举值完全等于用户输入
        2. 用户输入包含枚举值（适用于较短的枚举值如"长江新区"）
        
        V2改进：对纯数字枚举值添加语义过滤，避免年份子串被错误匹配
        """
        field_ids = [f.field_id for f in candidate_fields]
        if not field_ids:
            return []

        # 规范化用户输入用于包含匹配
        normalized_input = self._normalize_text(user_input)
        
        clauses = []
        params: List[Any] = []
        idx = 1
        if self.connection_id:
            clauses.append(f"t.connection_id = ${idx}")
            params.append(self.connection_id)
            idx += 1
        clauses.append(f"e.field_id = ANY(${idx}::uuid[])")
        params.append(field_ids)
        idx += 1
        # 两种匹配：完全相等 OR 用户输入包含枚举值（至少2个字符的值）
        # V2改进：排除纯数字枚举值的包含匹配（避免"2024"匹配到"202"、"20"、"02"）
        clauses.append(f"""(
            LOWER(e.original_value) = LOWER(${idx})
            OR (
                LENGTH(e.original_value) >= 2 
                AND POSITION(LOWER(e.original_value) IN LOWER(${idx})) > 0
                AND e.original_value !~ '^[0-9]+$'
            )
            OR (
                LENGTH(COALESCE(e.display_value, '')) >= 2 
                AND POSITION(LOWER(e.display_value) IN LOWER(${idx})) > 0
                AND COALESCE(e.display_value, '') !~ '^[0-9]+$'
            )
        )""")
        params.append(user_input)

        where_sql = " AND ".join(clauses)
        query = f"""
            SELECT 
                e.enum_value_id,
                e.field_id,
                f.display_name AS field_name,
                e.original_value AS value,
                e.display_value,
                t.table_id,
                t.display_name AS table_name,
                t.domain_id,
                CASE 
                    WHEN LOWER(e.original_value) = LOWER(${idx}) THEN 1.0
                    ELSE 0.95
                END AS match_score
            FROM field_enum_values e
            JOIN fields f ON e.field_id = f.field_id
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE {where_sql}
              AND e.is_active = true
              AND f.is_active = true
              AND t.is_included = true
            ORDER BY match_score DESC, e.frequency DESC
            LIMIT 5
        """
        results = await self.db.fetch(query, *params)
        
        matches = []
        for result in results:
            match_score = float(result.get('match_score', 0.95))
            matches.append(EnumMatch(
                enum_id=str(result['enum_value_id']),
                field_id=str(result['field_id']),
                field_name=result['field_name'],
                value=result['value'],
                display_name=result['display_value'] or result['value'],
                similarity=match_score,
                match_type="exact",
                final_score=match_score,
                context_similarity=0.0,  # 精确匹配无context，显式设0
                table_name=result.get("table_name"),
                table_id=str(result.get("table_id")) if result.get("table_id") else None,
                domain_id=str(result.get("domain_id")) if result.get("domain_id") else None,
                trace={"exact_match": True, "contains_match": match_score < 1.0},
            ))
        
        if matches:
            logger.debug("精确匹配命中", count=len(matches), values=[m.value for m in matches[:3]])
        
        return matches
    
    async def _synonym_match_all_fields(
        self,
        user_input: str,
        candidate_fields: List[Any]
    ) -> List[EnumMatch]:
        """
        同义词匹配（PostgreSQL）
        
        支持两种匹配模式：
        1. 同义词完全等于用户输入
        2. 用户输入包含同义词（适用于较短的同义词）
        
        V2改进：对纯数字同义词添加语义过滤
        """
        field_ids = [f.field_id for f in candidate_fields]
        if not field_ids:
            return []

        clauses = []
        params: List[Any] = []
        idx = 1
        if self.connection_id:
            clauses.append(f"t.connection_id = ${idx}")
            params.append(self.connection_id)
            idx += 1
        clauses.append(f"e.field_id = ANY(${idx}::uuid[])")
        params.append(field_ids)
        idx += 1
        
        # 同义词匹配：精确匹配 OR 用户输入包含同义词
        # V2改进：排除纯数字同义词的包含匹配
        clauses.append(f"""(
            ${idx} = ANY(e.synonyms)
            OR EXISTS (
                SELECT 1 FROM unnest(e.synonyms) AS syn
                WHERE LENGTH(syn) >= 2 
                    AND POSITION(LOWER(syn) IN LOWER(${idx})) > 0
                    AND syn !~ '^[0-9]+$'
            )
        )""")
        params.append(user_input)

        where_sql = " AND ".join(clauses)
        query = f"""
            SELECT 
                e.enum_value_id,
                e.field_id,
                f.display_name AS field_name,
                e.original_value AS value,
                e.display_value,
                e.synonyms,
                t.table_id,
                t.display_name AS table_name,
                t.domain_id,
                CASE 
                    WHEN ${idx} = ANY(e.synonyms) THEN 0.98
                    ELSE 0.90
                END AS match_score
            FROM field_enum_values e
            JOIN fields f ON e.field_id = f.field_id
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE {where_sql}
              AND e.is_active = true
              AND f.is_active = true
              AND t.is_included = true
            ORDER BY match_score DESC, e.frequency DESC
            LIMIT 10
        """
        results = await self.db.fetch(query, *params)
        
        matches = []
        for r in results:
            match_score = float(r.get('match_score', 0.90))
            # 找到匹配的同义词用于调试
            matched_synonym = None
            synonyms = r.get('synonyms') or []
            normalized_input = self._normalize_text(user_input)
            for syn in synonyms:
                normalized_syn = self._normalize_text(syn)
                if normalized_syn and (
                    normalized_syn == normalized_input or 
                    normalized_syn in normalized_input
                ):
                    matched_synonym = syn
                    break
            
            matches.append(EnumMatch(
                enum_id=str(r['enum_value_id']),
                field_id=str(r['field_id']),
                field_name=r['field_name'],
                value=r['value'],
                display_name=r['display_value'] or r['value'],
                similarity=match_score,
                match_type="synonym",
                final_score=match_score,
                context_similarity=0.0,  # 同义词匹配无context，显式设0
                table_name=r.get("table_name"),
                table_id=str(r.get("table_id")) if r.get("table_id") else None,
                domain_id=str(r.get("domain_id")) if r.get("domain_id") else None,
                trace={
                    "synonym_match": True, 
                    "matched_synonym": matched_synonym,
                    "contains_match": match_score < 0.98,
                },
            ))
        
        if matches:
            logger.debug("同义词匹配命中", count=len(matches), values=[m.value for m in matches[:3]])
        
        return matches
    
    async def _hybrid_value_recall(
        self,
        user_input: str,
        candidate_fields: List[Any],
        top_k: int,
    ) -> List[EnumMatch]:
        if not self.milvus or not self.embedding:
            logger.warning("Milvus或Embedding服务未配置")
            return []

        try:
            query_vector = await self.embedding.embed_single(user_input)
            field_ids = [str(f.field_id) for f in candidate_fields]
            filter_expr = self._build_filter(field_ids)

            dense_task = asyncio.create_task(self._search_value_dense(query_vector, filter_expr, top_k))
            sparse_task = asyncio.create_task(self._search_value_sparse(user_input, filter_expr, top_k))
            dense_hits, sparse_hits = await asyncio.gather(dense_task, sparse_task)
            merged = rrf_merge_hits({"dense": dense_hits, "sparse": sparse_hits})

            matches: List[EnumMatch] = []
            for entry in merged:
                payload = entry.get("payload") or {}
                enum_id = payload.get("value_id") or payload.get("id")
                match = EnumMatch(
                    enum_id=str(enum_id),
                    field_id=str(payload.get("field_id")),
                    field_name=payload.get("field_name"),
                    value=payload.get("value"),
                    display_name=payload.get("display_name") or payload.get("value"),
                    similarity=entry.get("dense_score") or entry.get("sparse_score") or 0.0,
                    match_type="value_vector",
                    value_similarity=entry.get("dense_score"),
                    context_similarity=0.0,  # 初始化为0，后续 context_vector_rerank 会更新
                    dense_score=entry.get("dense_score"),
                    sparse_score=entry.get("sparse_score"),
                    context_vector=payload.get("context_vector"),
                    payload=payload,
                    table_name=payload.get("table_name"),
                    table_id=payload.get("table_id"),
                    domain_id=str(payload.get("domain_id")) if payload.get("domain_id") else None,
                )
                matches.append(match)

            logger.debug("混合枚举召回完成", count=len(matches))
            return matches
        except Exception as exc:
            logger.exception("hybrid枚举召回失败", error=str(exc))
            return []
    
    def _extract_field_relevant_keywords(
        self,
        user_input: str,
        field_name: str,
        field_type: str = "dimension"
    ) -> str:
        """
        从用户问题中提取与字段相关的关键词
        
        策略：
        1. 地理字段（行政区、街道等）：提取地名实体
        2. 机构字段（征收单位、建设单位等）：提取机构名称
        3. 时间字段（年份、月份等）：提取时间表达式
        4. 其他：使用jieba分词+关键词权重
        
        Args:
            user_input: 用户问题
            field_name: 字段显示名称
            field_type: 字段类型
            
        Returns:
            优化后的context_query字符串
        """
        import jieba.posseg as pseg
        
        # 获取字段的关键词提取配置
        inferencer = get_field_precision_inferencer()
        extraction_config = inferencer.get_keyword_extraction_config(field_name)
        
        keywords = []
        
        if extraction_config:
            # 使用配置的提取规则
            extraction_rules = extraction_config.get('extraction_rules', [])
            for rule in extraction_rules:
                pattern = rule.get('pattern', '')
                try:
                    matches = re.findall(pattern, user_input)
                    keywords.extend(matches)
                except re.error as e:
                    logger.warning(f"正则表达式错误: {pattern}", error=str(e))
                    continue
        
        # 如果没有提取到关键词，使用jieba分词
        if not keywords:
            words = pseg.cut(user_input)
            # 提取名词和专有名词
            keywords = [w.word for w in words 
                       if w.flag.startswith('n') and len(w.word) >= 2]
        
        # 去重并限制长度
        general_config = inferencer.config.get('keyword_extraction', {}).get('general', {})
        max_keywords = general_config.get('max_keywords', 5)
        min_length = general_config.get('min_keyword_length', 2)
        
        keywords = [k for k in keywords if len(k) >= min_length]
        keywords = list(dict.fromkeys(keywords))[:max_keywords]
        
        if keywords:
            # 构造更精确的查询
            context_query = f"{field_name} {' '.join(keywords)}"
            logger.debug(
                "关键词提取成功",
                field=field_name,
                original_length=len(user_input),
                keywords=keywords,
                context_query_length=len(context_query)
            )
        else:
            # 降级：使用原问题的前N个字符
            fallback_chars = general_config.get('fallback_chars', 30)
            context_query = f"{field_name} {user_input[:fallback_chars]}"
            logger.debug(
                "关键词提取失败，使用降级策略",
                field=field_name,
                fallback_chars=fallback_chars
            )
        
        return context_query
    
    async def _context_vector_rerank(
        self,
        user_input: str,
        candidate_fields: List[Any],
        recall_results: List[EnumMatch]
    ) -> List[EnumMatch]:
        """
        阶段2：使用context_vector字段内精排
        
        目标：提升准确率，根据字段上下文重新打分
        
        改进：使用关键词提取优化context_query构造
        """
        if not recall_results:
            return []
        
        try:
            # 按字段分组
            results_by_field = {}
            for match in recall_results:
                if match.field_id not in results_by_field:
                    results_by_field[match.field_id] = []
                results_by_field[match.field_id].append(match)
            
            reranked = []
            
            # 为每个字段单独精排
            for field in candidate_fields:
                field_id = str(field.field_id)
                
                if field_id not in results_by_field:
                    continue
                
                field_results = results_by_field[field_id]
                
                # 🔥 改进：提取字段相关关键词
                context_query = self._extract_field_relevant_keywords(
                    user_input,
                    field.display_name,
                    getattr(field, 'field_type', 'dimension')
                )
                
                context_query_vector = await self.embedding.embed_single(context_query)
                
                # 使用context_vector重新打分
                for match in field_results:
                    if match.context_vector:
                        # 计算上下文相似度
                        context_similarity = self._cosine_similarity(
                            context_query_vector,
                            match.context_vector
                        )
                        
                        match.context_similarity = context_similarity
                        
                        # 综合分数：召回分数 × 0.4 + 上下文分数 × 0.6
                        match.final_score = (
                            match.similarity * 0.4 +
                            context_similarity * 0.6
                        )
                    else:
                        # 无 context_vector 时，显式设0（避免聚合时被忽略）
                        match.context_similarity = 0.0
                        match.final_score = match.similarity
                    
                    reranked.append(match)
            
            # 按final_score排序
            reranked.sort(key=lambda x: x.final_score, reverse=True)
            
            logger.info(
                "context_vector精排完成",
                count=len(reranked),
                fields_processed=len(results_by_field),
                top5=[(m.field_name, m.value, round(m.final_score, 3), round(m.context_similarity or 0, 3)) 
                      for m in reranked[:5]]
            )
            
            return reranked
            
        except Exception as e:
            logger.exception("context_vector精排失败", error=str(e))
            # 降级：返回原始结果
            return recall_results
    
    def _rrf_merge(
        self,
        sources: List[Tuple[str, List[EnumMatch]]],
        top_k: int,
        k: int = 60
    ) -> List[EnumMatch]:
        """
        RRF（Reciprocal Rank Fusion）融合多个来源的结果
        
        改进：保留原始匹配分数，RRF 仅用于相同分数时的排序
        - exact/synonym 匹配保持原始高分（0.9+）
        - vector 匹配使用 RRF 计算
        """
        rrf_scores = {}
        match_objects = {}
        original_scores = {}  # 保留原始匹配分数
        source_boosts = {
            "exact": RetrievalConfig.enum_exact_rrf_boost(),
            "synonym": RetrievalConfig.enum_synonym_rrf_boost(),
        }
        
        for source_name, matches in sources:
            for rank, match in enumerate(matches, start=1):
                key = (match.field_id, match.value)
                
                if key not in rrf_scores:
                    rrf_scores[key] = 0.0
                    match_objects[key] = match
                    match_objects[key].rrf_sources = []
                    # 保留原始匹配分数（exact/synonym 的原始分数较高）
                    original_scores[key] = match.similarity or 0.0
                else:
                    # 多源命中时取最高的原始分数
                    original_scores[key] = max(original_scores[key], match.similarity or 0.0)
                
                contribution = 1.0 / (k + rank) + source_boosts.get(source_name, 0.0)
                rrf_scores[key] += contribution
                
                match_objects[key].rrf_sources.append({
                    "source": source_name,
                    "rank": rank,
                    "contribution": contribution,
                    "original_score": match.similarity
                })
        
        # 生成最终结果：使用原始分数和 RRF 分数的加权组合
        final = []
        for key, rrf_score in rrf_scores.items():
            match = match_objects[key]
            orig_score = original_scores.get(key, 0.0)
            
            # 对于 exact/synonym 匹配，保持高分；对于 vector 匹配，使用 RRF
            if match.match_type in {"exact", "synonym"}:
                # exact/synonym 保留原始高分，RRF 只做微调
                match.final_score = orig_score * 0.8 + min(rrf_score, 0.3) * 0.2
            else:
                # vector 匹配：如果已有 context_vector 精排的 final_score（> 0），保留它
                # 否则使用 RRF + 原始相似度的加权
                existing_final = getattr(match, 'final_score', 0.0) or 0.0
                context_sim = getattr(match, 'context_similarity', 0.0) or 0.0
                if existing_final > 0 and context_sim > 0:
                    # 已有 context_vector 精排分数，仅做微调（RRF 贡献 10%）
                    match.final_score = existing_final * 0.9 + min(rrf_score, 0.2) * 0.1
                else:
                    # 无精排分数，使用 RRF 分数 + 原始相似度的加权
                    match.final_score = rrf_score * 0.6 + orig_score * 0.4
            
            final.append(match)
        
        # 排序
        final.sort(key=lambda x: x.final_score, reverse=True)
        
        return final[:top_k]
    
    def _cosine_similarity(
        self,
        vec1: List[float],
        vec2: List[float]
    ) -> float:
        """计算余弦相似度"""
        try:
            vec1 = np.array(vec1)
            vec2 = np.array(vec2)
            return float(np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2)))
        except Exception as e:
            logger.error(f"余弦相似度计算失败: {e}")
            return 0.0

    def _select_relevant_fields(
        self,
        user_input: str,
        candidate_fields: List[Any],
    ) -> Tuple[List[Any], bool, Dict[str, List[str]]]:
        normalized_question = self._normalize_text(user_input)
        activated: List[Any] = []
        field_hits: Dict[str, List[str]] = {}

        for field in candidate_fields:
            keywords = self._field_keywords(field)
            matched = [
                keyword for keyword in keywords if keyword and keyword in normalized_question
            ]
            if matched:
                activated.append(field)
                field_id = str(getattr(field, "field_id", ""))
                if field_id:
                    field_hits[field_id] = matched

        if activated:
            logger.debug(
                "枚举字段关键词命中",
                fields=[f.display_name for f in activated],
            )
            activated_ids = {str(getattr(f, "field_id", "")) for f in activated}
            ordered = activated + [
                f for f in candidate_fields if str(getattr(f, "field_id", "")) not in activated_ids
            ]
            return ordered, True, field_hits

        return candidate_fields, False, field_hits

    @staticmethod
    def _field_keywords(field: Any) -> Set[str]:
        keywords: Set[str] = set()

        def _add(token: Optional[str]):
            if not token:
                return
            normalized = DualVectorEnumRetriever._normalize_text(token)
            if normalized:
                keywords.add(normalized)

        _add(getattr(field, "display_name", None))
        _add(getattr(field, "field_name", None))
        for synonym in getattr(field, "synonyms", []) or []:
            _add(synonym)
        return keywords

    @staticmethod
    def _extract_query_keywords(text: Optional[str]) -> Set[str]:
        if not text:
            return set()
        cleaned = re.sub(r"[“”\"'‘’]", " ", text)
        cleaned = re.sub(r"[^\w\u4e00-\u9fff]+", " ", cleaned)
        keywords: Set[str] = set()
        for token in cleaned.split():
            normalized = DualVectorEnumRetriever._normalize_text(token)
            if normalized:
                keywords.add(normalized)
        return keywords

    @staticmethod
    def _normalize_text(value: Optional[str]) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", "", str(value).lower())

    def _value_mentioned(
        self,
        match: EnumMatch,
        normalized_question: str,
        query_keywords: Optional[Set[str]] = None,
    ) -> bool:
        """
        检查枚举值是否在用户问题中被提及
        
        严格匹配策略（避免批次名称等误输出）：
        1. 完整子串匹配（值或同义词完整出现在问题中）
        2. 字段名显式匹配（字段名如"批次名称"出现在问题中）
        3. 年份匹配（仅当问题中明确提及年份时）
        4. 关键词匹配（仅匹配有意义的关键词，不做部分重叠）
        """
        tokens = []
        tokens.append(self._normalize_text(match.value))
        tokens.append(self._normalize_text(match.display_name))

        payload_synonyms = []
        if match.payload:
            raw_synonyms = match.payload.get("synonyms")
            if isinstance(raw_synonyms, str):
                try:
                    payload_synonyms = json.loads(raw_synonyms)
                except json.JSONDecodeError:
                    payload_synonyms = []
            elif isinstance(raw_synonyms, list):
                payload_synonyms = raw_synonyms
        for synonym in payload_synonyms or []:
            tokens.append(self._normalize_text(synonym))

        normalized_question = normalized_question or ""
        
        # 1. 完整子串匹配：值完整出现在问题中
        for token in tokens:
            if token and len(token) >= 2 and token in normalized_question:
                return True

        # 2. 年份匹配：仅当问题中包含年份，且值中的年份与问题年份匹配
        question_years = set(re.findall(r'(20\d{2}|19\d{2})', normalized_question))
        if question_years:
            value_text = match.value or match.display_name or ""
            value_years = set(re.findall(r'(20\d{2}|19\d{2})', value_text))
            if question_years & value_years:  # 有交集
                return True

        if not query_keywords:
            return False

        # 3. 关键词完整匹配（不做部分重叠匹配，避免误判）
        # 排除通用词：年、月、日、区、市、第、批次、城市等
        stop_keywords = set(get_retrieval_param(
            "enum_retrieval.value_mention.stop_keywords",
            # 默认给“通用停用词”，不包含具体城市/项目名，避免跨项目硬编码依赖
            ["年", "月", "日", "区", "市", "第", "批次", "城市"],
        ))
        min_keyword_len = int(get_retrieval_param("enum_retrieval.value_mention.min_keyword_len", 2))
        meaningful_keywords = {kw for kw in query_keywords if len(kw) >= min_keyword_len and kw not in stop_keywords}
        
        for token in tokens:
            if not token or len(token) < 2:
                continue
            for keyword in meaningful_keywords:
                if not keyword:
                    continue
                # 严格匹配：关键词完整包含在token中，或token完整包含在关键词中
                # 且匹配长度至少 min_keyword_len（由配置控制，默认2）
                if len(keyword) >= min_keyword_len and keyword in token:
                    return True
                if len(token) >= min_keyword_len and token in keyword:
                    return True
        
        return False

    def _is_high_noise_field(self, field_name: Optional[str]) -> bool:
        """
        检查是否为高噪声字段
        
        高噪声字段（如批次名、项目名称等长文本字段）容易产生误召回，
        需要更高的阈值或显式字段名提及才能放行。
        """
        if not field_name:
            return False
        normalized = self._normalize_text(field_name)
        
        # 检查完整匹配
        if normalized in self.HIGH_NOISE_FIELDS:
            return True
        
        # 检查部分匹配（如"批次名称"包含"批次名"）
        for noise_field in self.HIGH_NOISE_FIELDS:
            if noise_field in normalized or normalized in noise_field:
                return True
        
        return False

    def _field_name_mentioned(
        self,
        field_name: Optional[str],
        normalized_question: str
    ) -> bool:
        """
        检查字段名是否在问题中显式提及
        
        支持完整字段名匹配和关键部分匹配
        """
        if not field_name or not normalized_question:
            return False
        
        field_name_normalized = self._normalize_text(field_name)
        if not field_name_normalized:
            return False
        
        # 完整字段名匹配
        if field_name_normalized in normalized_question:
            return True
        
        # 关键部分匹配（处理"所属行政区"→"行政区"等情况）
        field_keywords = get_retrieval_param(
            "enum_retrieval.field_name_mention.keywords",
            ["年份", "行政区", "类型", "状态"],
        )
        for kw in field_keywords:
            if kw in field_name_normalized and kw in normalized_question:
                return True
        
        return False

    def _apply_negative_signal_penalty(
        self,
        matches: List[EnumMatch]
    ) -> List[EnumMatch]:
        """
        负向信号处理：同字段出现多条互斥值时，对低分项降权
        
        场景：同一个字段（如"行政区"）召回了多个值（"武昌区"、"洪山区"），
        当分数差距较大时，低分项很可能是误召回，需要降权或剔除。
        """
        if not matches:
            return matches
        
        # 按字段分组
        field_groups: Dict[str, List[EnumMatch]] = {}
        for match in matches:
            field_id = match.field_id
            if field_id not in field_groups:
                field_groups[field_id] = []
            field_groups[field_id].append(match)
        
        result = []
        for field_id, group in field_groups.items():
            if len(group) <= 1:
                result.extend(group)
                continue
            
            # 按分数排序
            group.sort(key=lambda x: x.final_score, reverse=True)
            top_score = group[0].final_score
            
            for match in group:
                trace = match.trace or {}
                
                # 如果和最高分差距超过阈值，且不是精确/同义词匹配，降权
                score_gap = top_score - match.final_score
                if (score_gap > self.NEGATIVE_SIGNAL_SCORE_GAP and 
                    match.match_type not in {"exact", "synonym"}):
                    # 降权处理
                    penalty = min(self.NEGATIVE_SIGNAL_MAX_PENALTY, score_gap * 0.3)
                    match.final_score = max(0, match.final_score - penalty)
                    trace["negative_signal_penalty"] = penalty
                    trace["score_gap_from_top"] = score_gap
                
                match.trace = trace
                result.append(match)
        
        # 重新排序
        result.sort(key=lambda x: x.final_score, reverse=True)
        return result

    def _deduplicate_results(
        self,
        matches: List[EnumMatch]
    ) -> List[EnumMatch]:
        """
        按 (field_id, value) 去重
        
        跨表同名字段可能产生重复枚举值，保留分数最高的
        """
        if not matches:
            return matches
        
        # 按分数排序（确保高分在前）
        sorted_matches = sorted(matches, key=lambda x: x.final_score, reverse=True)
        
        seen: Set[Tuple[str, str]] = set()
        deduped: List[EnumMatch] = []
        
        for match in sorted_matches:
            key = (match.field_id, match.value)
            if key not in seen:
                seen.add(key)
                deduped.append(match)
            else:
                # 记录被去重
                trace = match.trace or {}
                trace["deduplicated"] = True
                match.trace = trace
        
        return deduped

    def _limit_per_field(self, matches: List[EnumMatch], per_field_limit: int) -> List[EnumMatch]:
        """
        限制每个字段的枚举数量
        
        新增：高噪声字段使用更低的限制
        """
        from server.config import RetrievalConfig
        
        # 高噪声字段的特殊限制
        high_noise_limit = RetrievalConfig.enum_per_high_noise_field_limit()
        
        limited: List[EnumMatch] = []
        counts: Dict[str, int] = {}
        
        for match in matches:
            field_id = match.field_id
            counts.setdefault(field_id, 0)
            
            # 判断是否为高噪声字段
            is_high_noise = self._is_high_noise_field(match.field_name)
            effective_limit = high_noise_limit if is_high_noise else per_field_limit
            
            if counts[field_id] >= effective_limit:
                # 记录被限制的原因
                trace = match.trace or {}
                if is_high_noise:
                    trace["limited_reason"] = f"high_noise_field_limit({high_noise_limit})"
                else:
                    trace["limited_reason"] = f"per_field_limit({per_field_limit})"
                match.trace = trace
                continue
            
            counts[field_id] += 1
            limited.append(match)
        
        return limited

    def _apply_keyword_boosts(
        self,
        matches: List[EnumMatch],
        field_keyword_hits: Dict[str, List[str]],
        keyword_profile: Optional["KeywordExtractionResult"],
    ) -> List[EnumMatch]:
        if not matches:
            return matches
        
        # 从配置读取关键词增益参数（消除硬编码）
        from server.config import get_retrieval_param
        keyword_gain_per_hit = get_retrieval_param("enum_retrieval.keyword_gain", 0.05)

        for match in matches:
            trace = match.trace or {}
            keyword_gain = 0.0

            if field_keyword_hits.get(match.field_id):
                trace["field_keyword_hit"] = field_keyword_hits[match.field_id]
                keyword_gain += keyword_gain_per_hit  # 从配置读取

            if keyword_profile:
                field_tokens = keyword_profile.field_hits.get(match.field_id)
                if field_tokens:
                    trace["global_field_tokens"] = field_tokens
                    keyword_gain += min(0.1, 0.03 * len(field_tokens))

                normalized_value = self._normalize_text(match.value) or self._normalize_text(
                    match.display_name
                )
                if (
                    normalized_value
                    and normalized_value in keyword_profile.normalized_enum_tokens
                ):
                    trace["enum_keyword_hit"] = True
                    keyword_gain += keyword_gain_per_hit  # 从配置读取

            if match.match_type in {"exact", "synonym"}:
                trace["high_confidence_match"] = True

            match.final_score = (match.final_score or match.similarity or 0.0) + keyword_gain
            match.trace = trace

        matches.sort(key=lambda m: m.final_score, reverse=True)
        return matches

    def _build_filter(
        self,
        field_ids: List[str],
    ) -> str:
        formatted_ids = ", ".join(f'"{fid}"' for fid in field_ids)
        filters = [f"field_id in [{formatted_ids}]", "is_active == true"]
        if self.connection_id:
            filters.append(f'connection_id == "{self.connection_id}"')
        return " and ".join(filters)

    async def _search_value_dense(self, query_vector, filter_expr: str, limit: int) -> List[Dict]:
        """Dense 向量检索（使用 HitExtractor 统一处理）。"""
        from server.config import get_retrieval_param
        output_fields = [
            "value_id",
            "field_id",
            "field_name",
            "value",
            "display_name",
            "context_vector",
            "table_id",
            "table_name",
            "domain_id",
            "json_meta",
        ]
        nprobe = int(get_retrieval_param("enum_retrieval.milvus_search_params.dense.nprobe", 10) or 10)
        results = await asyncio.to_thread(
            self.milvus.search,
            collection_name=self.collection_name,
            data=[query_vector],
            anns_field="value_vector",
            search_params={"metric_type": "COSINE", "params": {"nprobe": nprobe}},
            filter=filter_expr,
            limit=limit,
            output_fields=output_fields,
        )
        hits = HitExtractor.extract_all(
            results,
            identity_field="value_id",
            metric_type="COSINE",
            score_type="dense",
            fields=output_fields,
        )
        # 后处理：json_meta 安全解析
        for hit in hits:
            payload = hit.get("payload", {})
            payload["json_meta"] = self._safe_json(payload.get("json_meta"))
        return hits

    async def _search_value_sparse(self, user_input: str, filter_expr: str, limit: int) -> List[Dict]:
        """Sparse BM25 检索（使用 HitExtractor 统一处理）。"""
        from server.config import get_retrieval_param
        sparse_query = build_sparse_query(user_input)
        if not sparse_query["text"]:
            return []
        output_fields = [
            "value_id",
            "field_id",
            "field_name",
            "value",
            "display_name",
            "context_vector",
            "table_id",
            "table_name",
            "domain_id",
            "json_meta",
        ]
        drop_ratio_search = float(get_retrieval_param("enum_retrieval.milvus_search_params.sparse.drop_ratio_search", 0.2) or 0.2)
        results = await asyncio.to_thread(
            self.milvus.search,
            collection_name=self.collection_name,
            data=[sparse_query["text"]],
            anns_field="sparse_vector",
            search_params={"metric_type": "BM25", "params": {"drop_ratio_search": drop_ratio_search}},
            filter=filter_expr,
            limit=limit,
            output_fields=output_fields,
        )
        hits = HitExtractor.extract_all(
            results,
            identity_field="value_id",
            metric_type="BM25",
            score_type="sparse",
            fields=output_fields,
        )
        # 后处理：json_meta 安全解析
        for hit in hits:
            payload = hit.get("payload", {})
            payload["json_meta"] = self._safe_json(payload.get("json_meta"))
        return hits

    @staticmethod
    def _blend_reranker_score(
        base_score: Optional[float],
        rerank_score: Optional[float],
    ) -> Optional[float]:
        if rerank_score is None:
            return base_score
        weight = RetrievalConfig.reranker_weight()
        return (1 - weight) * (base_score or 0.0) + weight * rerank_score

    @staticmethod
    def _safe_json(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return {}
        return {}
