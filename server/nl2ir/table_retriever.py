"""
表级检索器
基于向量相似度检索相关的数据表
"""

import asyncio
import json
import math
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass
import structlog

from server.config import settings, RetrievalConfig
from server.models.semantic import SemanticModel, Datasource
from server.nl2ir.hybrid_utils import (
    build_sparse_query,
    normalize_dense_score,
    rrf_merge_dual_channel,
    HitExtractor,
)
# 使用统一的模型客户端
from server.utils.model_clients import EmbeddingClient, RerankerClient

logger = structlog.get_logger()

if TYPE_CHECKING:
    from server.nl2ir.keyword_pipeline import KeywordExtractionResult


@dataclass
class TableRetrievalResult:
    """表检索结果"""
    table_id: str
    datasource: Datasource
    score: float
    field_count: int = 0
    dense_score: Optional[float] = None
    sparse_score: Optional[float] = None
    reranker_score: Optional[float] = None
    rrf_score: Optional[float] = None
    evidence: Optional[Dict[str, Any]] = None
    connection_id: Optional[str] = None  # 多连接支持：表所属连接


class TableRetriever:
    """
    表级检索器：支持 Dense + Sparse 双路召回 + Reranker 精排。
    """

    def __init__(
        self,
        milvus_client=None,
        embedding_client=None,
        semantic_model: SemanticModel = None,
        connection_id: Optional[str] = None,
        reranker: Optional["RerankerClient"] = None,
    ):
        self.milvus_client = milvus_client
        self.embedding_client = embedding_client
        self.model = semantic_model
        self.connection_id = connection_id
        self.collection_name = getattr(
            milvus_client, "collection_name", settings.milvus_collection
        ) if milvus_client else settings.milvus_collection
        self.reranker = reranker
        self.last_retrieval_info: Dict[str, Any] = {}
    
    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    
    def _estimate_field_count_from_model(self, table_id: Optional[str]) -> int:
        """从语义模型估算字段数量，作为field_count缺失时的兜底。"""
        if not table_id or not self.model or not getattr(self.model, "fields", None):
            return 0
        total = 0
        for field in self.model.fields.values():
            if field.datasource_id != table_id:
                continue
            if hasattr(field, "is_active") and field.is_active is False:
                continue
            total += 1
        return total
    
    def _compute_field_count(
        self,
        table_id: Optional[str],
        json_meta: Optional[Dict[str, Any]],
    ) -> int:
        """
        统一计算字段数量，优先读取json_meta的顶层字段，其次读取stats字段，
        最后回退到语义模型。
        """
        count = 0
        if isinstance(json_meta, dict):
            count = self._safe_int(json_meta.get("field_count"))
            if not count:
                stats = json_meta.get("stats")
                if isinstance(stats, dict):
                    count = self._safe_int(stats.get("field_count"))
        if not count:
            count = self._estimate_field_count_from_model(table_id)
        return count

    async def retrieve_relevant_tables(
        self,
        question: str,
        domain_id: Optional[str] = None,
        connection_id: Optional[str] = None,
        top_k: int = 3,
        threshold: float = 0.3,
        query_vector: Optional[List[float]] = None,
        candidate_domain_ids: Optional[List[str]] = None,
        keyword_profile: Optional["KeywordExtractionResult"] = None,
        allow_cross_domain_retry: bool = True,
    ) -> List[TableRetrievalResult]:
        """
        表检索主入口（全量召回 + 后置打分）。

        重要：V3 架构下 **不再使用 domain_id 做 Milvus 前置过滤**，domain_id 仅作为元信息返回（evidence.domain_id）
        供后续排序算子使用，避免“误杀”导致 Recall 下降。
        """
        if not self.milvus_client or not self.embedding_client:
            logger.warning("Milvus或Embedding客户端未配置，无法进行表级检索")
            self.last_retrieval_info = {
                "method": "unavailable",
                "reason": "missing_clients",
                "candidate_domain_ids": candidate_domain_ids or [],
            }
            return []

        # V3：domain/candidate_domain_ids 不做硬过滤，保留参数以兼容旧调用方
        candidate_domain_ids = candidate_domain_ids or []
        # connection_id 优先取入参，其次取实例默认
        if connection_id:
            self.connection_id = connection_id

        try:
            dense_enabled = RetrievalConfig.dense_enabled()
            sparse_enabled = RetrievalConfig.sparse_enabled()

            if dense_enabled and query_vector is None:
                logger.debug("生成问题向量", question=question)
                query_vector = await self.embedding_client.embed_single(question)
            elif query_vector is not None:
                logger.debug("复用已有问题向量", vector_dim=len(query_vector))

            # V3：全局过滤（entity_type + connection），不加 domain 条件
            filter_expr = self._build_filter()
            logger.debug(
                "开始表级混合检索（全量召回）",
                filter=filter_expr,
                top_k=top_k,
                threshold=threshold,
            )

            expansion_factor = RetrievalConfig.table_expansion_factor()
            dense_task = (
                asyncio.create_task(
                    self._search_dense(query_vector, filter_expr, top_k * expansion_factor)
                )
                if dense_enabled
                else None
            )
            sparse_task = (
                asyncio.create_task(
                    self._search_sparse(question, filter_expr, top_k * expansion_factor)
                )
                if sparse_enabled
                else None
            )
            dense_hits = await dense_task if dense_task else []
            sparse_hits = await sparse_task if sparse_task else []
            merged = rrf_merge_dual_channel(
                dense_hits,
                sparse_hits,
                k=RetrievalConfig.rrf_k(),
                dense_weight=RetrievalConfig.table_dense_weight(),
                sparse_weight=RetrievalConfig.table_sparse_weight(),
            )

            if not merged:
                self.last_retrieval_info = {
                    "method": "hybrid_global",
                    "filter": filter_expr,
                    "dense_candidates": len(dense_hits),
                    "sparse_candidates": len(sparse_hits),
                    "found_tables": 0,
                    "candidate_domain_ids": candidate_domain_ids,
                    "note": "empty_recall",
                }
                return []

            limited = merged[: max(1, top_k) * 3]
            reranker_scores = await self._apply_reranker(
                question,
                [entry["payload"] for entry in limited],
            )

            table_results: List[TableRetrievalResult] = []
            for idx, entry in enumerate(limited):
                payload = entry.get("payload") or {}
                table_id = payload.get("item_id") or payload.get("table_id")
                if not table_id:
                    continue
                datasource = self._resolve_datasource(table_id, payload)
                json_meta = payload.get("json_meta") or {}
                if isinstance(json_meta, str):
                    try:
                        json_meta = json.loads(json_meta)
                    except json.JSONDecodeError:
                        json_meta = {}

                dense_score = entry.get("dense_score")
                sparse_score = entry.get("sparse_score")
                rrf_score = entry.get("rrf_score")
                rerank_score = reranker_scores[idx] if idx < len(reranker_scores) else None
                final_score = self._blend_reranker_score(rrf_score, rerank_score)
                field_count = self._compute_field_count(table_id, json_meta)

                table_results.append(
                    TableRetrievalResult(
                        table_id=table_id,
                        datasource=datasource,
                        score=final_score or 0.0,
                        field_count=field_count,
                        dense_score=dense_score,
                        sparse_score=sparse_score,
                        reranker_score=rerank_score,
                        rrf_score=rrf_score,
                        connection_id=datasource.connection_id,  # 填充连接ID
                        evidence={
                            "graph_text": payload.get("graph_text"),
                            # 保留 domain_id 作为元信息供后置排序使用
                            "domain_id": payload.get("domain_id"),
                        },
                    )
                )

            table_results.sort(key=lambda item: item.score, reverse=True)
            table_results = table_results[: max(1, top_k)]

            table_results = self._apply_keyword_boost(table_results, keyword_profile, question)

            reranker_used = bool(self.reranker and self.reranker.is_enabled())
            # 阈值过滤（仅当未使用Reranker时）
            if not reranker_used:
                filtered = [
                    result for result in table_results if (result.dense_score or 0.0) >= threshold
                ]
                if filtered:
                    table_results = filtered
                else:
                    logger.debug("Dense得分全部低于阈值，保留原始结果以保留多样性")

            # 统计域分布（只做观测，不过滤）
            domain_distribution: Dict[str, int] = {}
            for t in table_results:
                did = (t.evidence or {}).get("domain_id")
                if did:
                    domain_distribution[str(did)] = domain_distribution.get(str(did), 0) + 1

            self.last_retrieval_info = {
                "method": "hybrid_global",
                "filter": filter_expr,
                "dense_candidates": len(dense_hits),
                "sparse_candidates": len(sparse_hits),
                "found_tables": len(table_results),
                "top_rrf": merged[0]["rrf_score"] if merged else 0,
                "reranker_used": reranker_used,
                "candidate_domain_ids": candidate_domain_ids,
                "domain_distribution": domain_distribution,
            }

            logger.debug(
                "表级混合检索完成（全量召回）",
                found_tables=len(table_results),
                top_score=table_results[0].score if table_results else 0,
            )
            return table_results

        except Exception as exc:
            logger.exception("表级检索失败", error=str(exc))
            self.last_retrieval_info = {
                "method": "hybrid_global",
                "error": str(exc),
                "fallback_used": False,
                "candidate_domain_ids": candidate_domain_ids,
            }
            return []

    async def retrieve_tables_for_multi_domain(
        self,
        question: str,
        candidate_domain_ids: List[str],
        query_vector: Optional[List[float]] = None,
        top_k_per_domain: int = 5,
        global_top_k: int = 25,
        keyword_profile: Optional["KeywordExtractionResult"] = None,
    ) -> Dict[str, List[TableRetrievalResult]]:
        """
        全局统一表检索（方案A实现）
        
        核心改进：
        1. 不使用域过滤，进行全局Dense+Sparse检索
        2. 使用全局排名计算RRF分数（确保跨域可比）
        3. 对全局TOP表统一应用Reranker
        4. 按候选域分组，每个域返回top_k_per_domain个表
        
        Args:
            question: 用户问题
            candidate_domain_ids: 候选业务域ID列表
            query_vector: 问题向量（可选，未提供则自动生成）
            top_k_per_domain: 每个域返回的表数量
            global_top_k: 全局检索的候选数量
            keyword_profile: 关键词配置
            
        Returns:
            Dict[domain_id, List[TableRetrievalResult]] 按域分组的检索结果
        """
        if not self.milvus_client or not self.embedding_client:
            logger.warning("Milvus或Embedding客户端未配置")
            self.last_retrieval_info = {"method": "global_multi_domain", "error": "missing_clients"}
            return {}
        
        if not candidate_domain_ids:
            logger.warning("候选域列表为空")
            return {}

        try:
            # 1. 生成问题向量
            dense_enabled = RetrievalConfig.dense_enabled()
            sparse_enabled = RetrievalConfig.sparse_enabled()

            if dense_enabled and query_vector is None:
                query_vector = await self.embedding_client.embed_single(question)
            
            # 2. 构建全局过滤条件（仅限制 entity_type/connection，不限制 domain）
            filter_expr = self._build_filter()
            
            logger.debug(
                "全局多域表检索开始",
                candidate_domains=len(candidate_domain_ids),
                global_top_k=global_top_k,
                filter=filter_expr[:100],
            )
            
            # 3. 全局Dense检索
            expansion_factor = RetrievalConfig.table_expansion_factor()
            dense_limit = global_top_k * expansion_factor
            dense_task = (
                asyncio.create_task(self._search_dense(query_vector, filter_expr, dense_limit))
                if dense_enabled
                else None
            )
            
            # 4. 全局Sparse检索
            sparse_task = (
                asyncio.create_task(self._search_sparse(question, filter_expr, dense_limit))
                if sparse_enabled
                else None
            )
            dense_hits = await dense_task if dense_task else []
            sparse_hits = await sparse_task if sparse_task else []
            
            # 5. 全局RRF融合（使用全局排名）
            merged = rrf_merge_dual_channel(
                dense_hits,
                sparse_hits,
                k=RetrievalConfig.rrf_k(),
                dense_weight=RetrievalConfig.table_dense_weight(),
                sparse_weight=RetrievalConfig.table_sparse_weight(),
            )
            
            if not merged:
                logger.debug("全局多域检索无结果")
                self.last_retrieval_info = {"method": "global_multi_domain", "found": 0}
                return {}
            
            # 6. 取全局TOP进行Reranker
            reranker_limit = min(len(merged), global_top_k * 3)
            limited = merged[:reranker_limit]
            
            reranker_scores = await self._apply_reranker(
                question,
                [entry["payload"] for entry in limited],
            )
            
            # 7. 构建检索结果，保留全局分数
            all_results: List[TableRetrievalResult] = []
            for idx, entry in enumerate(limited):
                payload = entry.get("payload") or {}
                table_id = payload.get("item_id") or payload.get("table_id")
                domain_id = payload.get("domain_id")

                if not table_id:
                    continue
                
                datasource = self._resolve_datasource(table_id, payload)
                json_meta = payload.get("json_meta") or {}
                if isinstance(json_meta, str):
                    try:
                        json_meta = json.loads(json_meta)
                    except json.JSONDecodeError:
                        json_meta = {}
                
                dense_score = entry.get("dense_score")
                sparse_score = entry.get("sparse_score")
                rrf_score = entry.get("rrf_score")
                rerank_score = reranker_scores[idx] if idx < len(reranker_scores) else None
                final_score = self._blend_reranker_score(rrf_score, rerank_score)
                field_count = self._compute_field_count(table_id, json_meta)
                
                result = TableRetrievalResult(
                    table_id=table_id,
                    datasource=datasource,
                    score=final_score or 0.0,
                    field_count=field_count,
                    dense_score=dense_score,
                    sparse_score=sparse_score,
                    reranker_score=rerank_score,
                    rrf_score=rrf_score,
                    connection_id=datasource.connection_id,
                    evidence={
                        "graph_text": payload.get("graph_text"),
                        "domain_id": domain_id,
                        "global_rank": idx + 1,  # 记录全局排名
                    },
                )
                all_results.append(result)
            
            # 8. 应用关键词加分
            all_results = self._apply_keyword_boost(all_results, keyword_profile, question)
            
            # 9. 按域分组，每个域取top_k_per_domain个
            domain_results: Dict[str, List[TableRetrievalResult]] = {d: [] for d in candidate_domain_ids}
            
            # 按分数排序
            all_results.sort(key=lambda x: x.score, reverse=True)
            
            for result in all_results:
                domain_id = result.evidence.get("domain_id") if result.evidence else None
                if domain_id and domain_id in domain_results:
                    if len(domain_results[domain_id]) < top_k_per_domain:
                        domain_results[domain_id].append(result)
            
            # 10. 记录检索信息
            total_tables = sum(len(tables) for tables in domain_results.values())
            self.last_retrieval_info = {
                "method": "global_multi_domain",
                "candidate_domains": len(candidate_domain_ids),
                "global_candidates": len(merged),
                "reranker_processed": reranker_limit,
                "total_tables": total_tables,
                "domain_distribution": {d: len(t) for d, t in domain_results.items()},
                "top_global_score": all_results[0].score if all_results else 0,
            }
            
            logger.info(
                "全局多域表检索完成",
                total_tables=total_tables,
                domain_distribution=self.last_retrieval_info["domain_distribution"],
            )
            
            return domain_results
            
        except Exception as exc:
            logger.exception("全局多域表检索失败", error=str(exc))
            self.last_retrieval_info = {"method": "global_multi_domain", "error": str(exc)}
            return {}

    def _apply_keyword_boost(
        self,
        table_results: List[TableRetrievalResult],
        keyword_profile: Optional["KeywordExtractionResult"],
        question: Optional[str] = None,
    ) -> List[TableRetrievalResult]:
        """根据关键词命中情况对表得分进行微调
        
        包含四种加分机制（按优先级排序）：
        0. 年份敏感匹配加分：问题中明确提到年份时，匹配对应年份的表【核心修复】
        1. 表名精确匹配加分：问题中明确提到表名（如"xxx中"、"xxx数据"）
        2. 表名关键词匹配加分：问题中的关键词与表名部分匹配
        3. keyword_profile 中的表级加分：字段/枚举匹配
        
        配置参数来自 retrieval_config.yaml:
        - keyword_boost_enabled: 是否启用关键词加分
        - keyword_boost_max: 关键词加分最大值
        - keyword_boost_per_hit: 每命中一个关键词的加分
        - name_keyword_boost_enabled: 是否启用表名关键词匹配
        - keyword_name_boost_per_match: 每匹配一个表名关键词的加分
        
        配置路径（retrieval_config.yaml）:
        - table_retrieval.boost_mechanisms.keyword_boost.*
        - table_retrieval.boost_mechanisms.name_keyword_match.*
        - table_retrieval.boost_mechanisms.exact_table_name_match.*
        - table_retrieval.boost_mechanisms.year_sensitive_match.*
        """
        from server.config import get_retrieval_param, RetrievalConfig
        import re
        
        if not table_results:
            return table_results
        
        # ========== 读取配置参数（开关从feature_switches读取，参数从各自位置读取）==========
        # 1. 关键词加分配置（开关来自feature_switches）
        keyword_boost_enabled = RetrievalConfig.keyword_boost_enabled()
        keyword_boost_per_hit = get_retrieval_param(
            "table_retrieval.boost_mechanisms.keyword_boost.per_hit", 0.04)
        keyword_boost_max = get_retrieval_param(
            "table_retrieval.boost_mechanisms.keyword_boost.max", 0.10)
        
        # 2. 表名关键词匹配配置（开关来自feature_switches）
        name_boost_enabled = RetrievalConfig.name_keyword_match_enabled()
        name_boost_per_match = get_retrieval_param(
            "table_retrieval.boost_mechanisms.name_keyword_match.per_match", 0.03)
        name_boost_max = get_retrieval_param(
            "table_retrieval.boost_mechanisms.name_keyword_match.max", 0.10)
        name_min_length = get_retrieval_param(
            "table_retrieval.boost_mechanisms.name_keyword_match.min_keyword_length", 2)
        # 核心业务词配置（P0新增）
        core_business_keywords = set(get_retrieval_param(
            "table_retrieval.boost_mechanisms.name_keyword_match.core_business_keywords",
            ["成交", "批复", "批准", "审批", "征收", "现状", "利用"]))
        core_keyword_boost = get_retrieval_param(
            "table_retrieval.boost_mechanisms.name_keyword_match.core_keyword_boost", 0.12)
        
        # 3. 表名精确匹配配置（开关来自feature_switches）
        exact_match_enabled = RetrievalConfig.exact_table_name_match_enabled()
        exact_match_boost = get_retrieval_param(
            "table_retrieval.boost_mechanisms.exact_table_name_match.boost", 0.08)
        exact_suffix_patterns = get_retrieval_param(
            "table_retrieval.boost_mechanisms.exact_table_name_match.suffix_patterns",
            ["中", "内", "里", "数据", "表", "记录", "的"])
        exact_prefix_patterns = get_retrieval_param(
            "table_retrieval.boost_mechanisms.exact_table_name_match.prefix_patterns",
            ["在", "从", "查", "取得", "获得"])
        
        # 4. 年份敏感匹配配置【新增】
        year_match_enabled = get_retrieval_param(
            "table_retrieval.boost_mechanisms.year_sensitive_match.enabled", True)
        year_match_boost = get_retrieval_param(
            "table_retrieval.boost_mechanisms.year_sensitive_match.boost", 0.15)
        year_mismatch_penalty = get_retrieval_param(
            "table_retrieval.boost_mechanisms.year_sensitive_match.mismatch_penalty", 0.08)
        # P0新增：最新年份语义词配置
        latest_year_keywords = get_retrieval_param(
            "table_retrieval.boost_mechanisms.year_sensitive_match.latest_year_keywords",
            ["最新", "最新年份", "最新的", "今年", "本年", "当前"])
        # 使用系统时间获取当前年份，避免硬编码
        from datetime import datetime
        current_year = str(datetime.now().year)
        latest_year_boost = get_retrieval_param(
            "table_retrieval.boost_mechanisms.year_sensitive_match.latest_year_boost", 0.18)

        # === 0. 年份敏感匹配【核心修复 + P0增强】===
        # 当用户问题中明确提到年份时，优先匹配对应年份的表
        # 例如："2023年现状水浇地" 应该优先匹配 "2023年土地利用现状"
        # P0增强：支持"最新年份"、"最新"等语义词映射
        year_matched_tables = set()
        year_mismatched_tables = set()
        question_years = []
        is_latest_year_query = False  # P0：标记是否为"最新年份"查询
        
        if year_match_enabled and question:
            # P0新增：检测"最新年份"语义词
            for latest_kw in latest_year_keywords:
                if latest_kw in question:
                    is_latest_year_query = True
                    break
            
            # 提取问题中的年份（支持 2023、2023年、二〇二三年 等格式）
            # 仅在“明确年份语义”或“边界分隔”下识别年份，避免把编号/代码中的 4 位数字误识别为年份
            year_patterns = [
                r'(\d{4})年',            # 2023年
                r'(\d{4})年度',           # 2023年度
                r'(\d{4})年份',           # 2023年份
                # 边界分隔（空白/标点/括号等），例如："2023 土地利用现状" / "（2023）"
                r'(?:^|[\\s,，。.!?；;：:（）()\\[\\]{}\"\\\'/\\\\-])(\\d{4})(?:$|[\\s,，。.!?；;：:（）()\\[\\]{}\"\\\'/\\\\-])',
            ]
            
            for pattern in year_patterns:
                matches = re.findall(pattern, question)
                for match in matches:
                    year = match if isinstance(match, str) else match[0]
                    if 1990 <= int(year) <= 2100:  # 合理年份范围
                        question_years.append(year)
            
            # 去重
            question_years = list(dict.fromkeys(question_years))
            
            # P0：如果是"最新年份"查询，动态确定最新年份
            # 优先从候选表中找到实际的最新年份
            detected_latest_year = None
            if is_latest_year_query:
                # 收集所有候选表的年份
                all_table_years = set()
                for table_result in table_results:
                    display_name = table_result.datasource.display_name or ""
                    table_year_matches = re.findall(r'(\d{4})', display_name)
                    for y in table_year_matches:
                        if 2000 <= int(y) <= 2100:  # 合理的数据年份范围
                            all_table_years.add(y)
                
                if all_table_years:
                    # 使用候选表中的最新年份
                    detected_latest_year = max(all_table_years)
                else:
                    # 使用系统当前年份
                    detected_latest_year = current_year
                
                # 将最新年份加入查询年份列表
                if detected_latest_year and detected_latest_year not in question_years:
                    question_years.append(detected_latest_year)
                
                logger.info(
                    "P0：检测到最新年份查询",
                    question=question[:50],
                    detected_latest_year=detected_latest_year,
                    question_years=question_years
                )
            
            if question_years or is_latest_year_query:
                logger.debug(
                    "年份敏感匹配：检测到年份约束",
                    question=question[:50],
                    detected_years=question_years,
                    is_latest_year_query=is_latest_year_query
                )
                
                for table_result in table_results:
                    display_name = table_result.datasource.display_name or ""
                    if not display_name:
                        continue
                    
                    # 检查表名中是否包含问题中指定的年份
                    table_year_match = False
                    table_has_year = False
                    matched_year = None
                    is_latest_match = False
                    
                    # 提取表名中的年份
                    table_year_matches = re.findall(r'(\d{4})年?', display_name)
                    if table_year_matches:
                        table_has_year = True
                        for table_year in table_year_matches:
                            if table_year in question_years:
                                table_year_match = True
                                matched_year = table_year
                                # P0：检查是否为最新年份匹配
                                if is_latest_year_query and table_year == detected_latest_year:
                                    is_latest_match = True
                                break
                    
                    if table_year_match:
                        # 表名年份与问题年份匹配：给予加分
                        year_matched_tables.add(table_result.table_id)
                        
                        # P0：最新年份匹配给予更高加分
                        applied_boost = latest_year_boost if is_latest_match else year_match_boost
                        table_result.score = (table_result.score or 0.0) + applied_boost
                        
                        if table_result.evidence is None:
                            table_result.evidence = {}
                        table_result.evidence["year_match"] = True
                        table_result.evidence["matched_year"] = matched_year
                        table_result.evidence["year_match_boost"] = applied_boost
                        table_result.evidence["is_latest_match"] = is_latest_match
                        
                        logger.debug(
                            "年份匹配加分" + (" (最新年份)" if is_latest_match else ""),
                            table=display_name,
                            matched_year=matched_year,
                            boost=applied_boost
                        )
                    elif table_has_year and question_years:
                        # 表名有年份但与问题年份不匹配：给予惩罚
                        # 这避免了 "2024年土地利用现状" 在查询 "2023年" 时排在前面
                        year_mismatched_tables.add(table_result.table_id)
                        table_result.score = (table_result.score or 0.0) - year_mismatch_penalty
                        if table_result.evidence is None:
                            table_result.evidence = {}
                        table_result.evidence["year_mismatch"] = True
                        table_result.evidence["table_years"] = table_year_matches
                        table_result.evidence["question_years"] = question_years
                        table_result.evidence["year_mismatch_penalty"] = -year_mismatch_penalty
                        
                        logger.debug(
                            "年份不匹配惩罚",
                            table=display_name,
                            table_years=table_year_matches,
                            question_years=question_years,
                            penalty=-year_mismatch_penalty
                        )

        # === 1. 表名精确匹配检测 ===
        # 检测问题中是否明确提到某个表名（如"xxx中"、"xxx数据"、"在xxx中"等）
        exact_matched_tables = set()
        if exact_match_enabled and question:
            for table_result in table_results:
                display_name = table_result.datasource.display_name or ""
                if not display_name or len(display_name) < 3:
                    continue
                
                # 从配置构建匹配模式
                exact_patterns = []
                for suffix in exact_suffix_patterns:
                    exact_patterns.append(f"{display_name}{suffix}")
                for prefix in exact_prefix_patterns:
                    exact_patterns.append(f"{prefix}{display_name}")
                
                for pattern in exact_patterns:
                    if pattern in question:
                        exact_matched_tables.add(table_result.table_id)
                        # 给予精确匹配加分
                        table_result.score = (table_result.score or 0.0) + exact_match_boost
                        if table_result.evidence is None:
                            table_result.evidence = {}
                        table_result.evidence["exact_table_name_match"] = True
                        table_result.evidence["exact_match_pattern"] = pattern
                        break  # 每个表只加一次

        # === 2. 表名关键词匹配加分（含核心业务词加分）===
        if name_boost_enabled and question:
            # 提取问题中的关键词（长度>=min_length的连续中文/英文词）
            chinese_words = re.findall(r'[\u4e00-\u9fa5]{2,}', question)
            english_words = re.findall(r'[a-zA-Z]{2,}', question)
            # 过滤太短的词
            question_keywords = [w for w in chinese_words + english_words if len(w) >= name_min_length]
            
            # 【优化】过滤掉已匹配的年份，避免重复加分
            question_keywords = [
                kw for kw in question_keywords 
                if not (kw.isdigit() and len(kw) == 4 and 1990 <= int(kw) <= 2100)
            ]
            
            # 【P0新增】提取问题中的核心业务词
            question_core_keywords = [kw for kw in core_business_keywords if kw in question]
            
            for table_result in table_results:
                # 如果已经获得精确匹配加分，跳过关键词匹配
                if table_result.table_id in exact_matched_tables:
                    continue
                    
                display_name = table_result.datasource.display_name or ""
                if not display_name:
                    continue
                
                # 统计匹配的关键词数量
                name_match_count = 0
                matched_keywords = []
                for keyword in question_keywords:
                    if keyword in display_name:
                        name_match_count += 1
                        matched_keywords.append(keyword)
                
                # 【P0新增】核心业务词匹配：给予额外高加分
                # 例如：问题中有"成交"，表名是"公开成交"，给予 core_keyword_boost 加分
                core_matched = []
                for core_kw in question_core_keywords:
                    if core_kw in display_name:
                        core_matched.append(core_kw)
                
                core_boost_applied = 0.0
                if core_matched:
                    # 核心业务词匹配，给予高额加分（不受普通max限制）
                    core_boost_applied = core_keyword_boost
                    if table_result.evidence is None:
                        table_result.evidence = {}
                    table_result.evidence["core_keyword_match"] = True
                    table_result.evidence["core_matched_keywords"] = core_matched
                    table_result.evidence["core_keyword_boost"] = core_boost_applied
                    
                    logger.debug(
                        "核心业务词匹配加分",
                        table=display_name,
                        core_matched=core_matched,
                        boost=core_boost_applied
                    )
                
                # 普通关键词加分（至少匹配1个才加分）
                name_boost_applied = 0.0
                if name_match_count >= 1:
                    name_boost_applied = min(name_boost_max, name_boost_per_match * name_match_count)
                    if table_result.evidence is None:
                        table_result.evidence = {}
                    table_result.evidence["name_keyword_boost"] = name_boost_applied
                    table_result.evidence["name_matched_keywords"] = matched_keywords
                
                # 总加分 = 核心业务词加分 + 普通关键词加分
                total_name_boost = core_boost_applied + name_boost_applied
                if total_name_boost > 0:
                    table_result.score = (table_result.score or 0.0) + total_name_boost

        # === 3. keyword_profile 中的表级加分 ===
        if keyword_boost_enabled and keyword_profile:
            # 噪声守卫：当 keyword_profile 同时“命中太多表”时，说明 token 很可能过泛（如滑窗爆炸/通用词）
            # 为避免大范围小幅加分导致排序抖动，按配置对 keyword_boost 做降权（不禁用，保持鲁棒）
            noise_guard = get_retrieval_param(
                "table_retrieval.boost_mechanisms.keyword_boost.noise_guard", {}
            ) or {}
            guard_enabled = bool(noise_guard.get("enabled", True))
            max_ratio = float(noise_guard.get("max_table_coverage_ratio", 0.6) or 0.6)
            downscale = float(noise_guard.get("downscale", 0.5) or 0.5)

            table_boost_count = len(keyword_profile.table_boosts or {})
            table_result_count = len(table_results) if table_results else 0
            coverage_ratio = (table_boost_count / table_result_count) if table_result_count else 0.0
            guard_factor = 1.0
            if guard_enabled and coverage_ratio > max_ratio:
                guard_factor = max(0.0, min(1.0, downscale))

            for table_result in table_results:
                boost_weight = keyword_profile.table_boosts.get(table_result.table_id)
                if not boost_weight:
                    continue
                boost = min(keyword_boost_max, keyword_boost_per_hit * boost_weight) * guard_factor
                table_result.score = (table_result.score or 0.0) + boost
                if table_result.evidence is None:
                    table_result.evidence = {}
                table_result.evidence.setdefault("keyword_boost", 0.0)
                table_result.evidence["keyword_boost"] += boost
                if guard_enabled and guard_factor != 1.0:
                    table_result.evidence["keyword_boost_noise_guard"] = {
                        "coverage_ratio": round(coverage_ratio, 3),
                        "max_ratio": max_ratio,
                        "factor": guard_factor,
                        "table_boost_count": table_boost_count,
                        "candidate_table_count": table_result_count,
                    }

        table_results.sort(key=lambda item: item.score, reverse=True)
        return table_results

    def _build_filter(self, domain_id: Optional[str] = None) -> str:
        # NOTE: domain_id 参数保留以兼容旧调用，但 V3 中 **不再用于过滤**
        filters = ['entity_type == "table"', "is_active == true"]
        if self.connection_id:
            filters.append(f'connection_id == "{self.connection_id}"')
        return " and ".join(filters)

    def _build_domain_attempts(
        self,
        primary_domain_id: Optional[str],
        candidate_domain_ids: List[str],
        allow_cross_domain_retry: bool = True,
    ) -> List[Dict[str, Optional[str]]]:
        attempts: List[Dict[str, Optional[str]]] = []
        seen: set = set()

        def add_attempt(domain_value: Optional[str], reason: str):
            key = domain_value or "__none__"
            if key in seen:
                return
            seen.add(key)
            attempts.append({"domain_id": domain_value, "reason": reason})

        add_attempt(primary_domain_id, "primary")
        if allow_cross_domain_retry:
            max_candidates = RetrievalConfig.cross_domain_max_candidates()
            limited_candidates = (candidate_domain_ids or [])[:max_candidates]
            for idx, candidate in enumerate(limited_candidates):
                add_attempt(candidate, f"candidate_{idx + 1}")

            if RetrievalConfig.cross_domain_allow_no_domain():
                add_attempt(None, "global_retry")
        return attempts

    @staticmethod
    def _is_result_quality_sufficient(
        table_results: List[TableRetrievalResult],
        threshold: float,
        min_results: int,
        reranker_used: bool,
    ) -> bool:
        if not table_results:
            return False
        top_score = table_results[0].score if reranker_used else (table_results[0].dense_score or 0.0)
        if top_score < threshold:
            return False
        if min_results > 1 and len(table_results) < min_results:
            return False
        return True

    @staticmethod
    def _get_quality_issue(
        table_results: List[TableRetrievalResult],
        threshold: float,
        min_results: int,
        reranker_used: bool,
    ) -> str:
        if not table_results:
            return "no_result"
        top_score = table_results[0].score if reranker_used else (table_results[0].dense_score or 0.0)
        if top_score < threshold:
            return "low_score"
        if min_results > 1 and len(table_results) < min_results:
            return "low_count"
        return "unknown"

    def _expand_query_for_sparse(self, question: str) -> str:
        """
        查询扩展：提取业务实体关键词并增强权重
        
        例如：
        输入: "2025年武汉市长江新区新批复的建设用地有多少宗"
        输出: "建设用地批复 新增建设用地 批复 建设用地 2025年武汉市长江新区新批复的建设用地有多少宗"
        
        Args:
            question: 原始用户问题
            
        Returns:
            扩展后的查询文本
        """
        import re
        
        from server.config import get_retrieval_param

        # 业务实体模式（按优先级排序，长词优先）
        # 这些模式会被提取出来并添加到查询前面以增强 BM25 匹配
        business_patterns = get_retrieval_param("table_retrieval.sparse_query_expansion.patterns", []) or []
        
        # 提取匹配的业务实体
        boosted_terms = []
        matched_patterns = set()
        
        for pattern in business_patterns:
            if re.search(pattern, question):
                # 避免重复添加被包含的短词
                is_substring = any(
                    pattern != p and pattern in p 
                    for p in matched_patterns
                )
                if not is_substring:
                    boosted_terms.append(pattern)
                    matched_patterns.add(pattern)
        
        # 组合增强词 + 原始问题
        if boosted_terms:
            # 增强词放在最前面，提高 BM25 权重
            expanded = f"{' '.join(boosted_terms)} {question}"
            logger.debug(
                "查询扩展完成",
                original_len=len(question),
                boosted_terms=boosted_terms,
                expanded_preview=expanded[:100],
            )
            return expanded
        
        return question


    async def _search_dense(self, query_vector, filter_expr: str, limit: int) -> List[Dict]:
        """Dense 向量检索（使用 HitExtractor 统一处理）。"""
        if query_vector is None:
            return []
        from server.config import get_retrieval_param
        output_fields = [
            "item_id",
            "table_id",
            "display_name",
            "description",
            "table_name",
            "schema_name",
            "domain_id",
            "graph_text",
            "json_meta",
        ]
        nprobe = int(get_retrieval_param("table_retrieval.milvus_search_params.dense.nprobe", 10) or 10)
        results = await asyncio.to_thread(
            self.milvus_client.search,
            collection_name=self.collection_name,
            data=[query_vector],
            anns_field="dense_vector",
            search_params={"metric_type": "COSINE", "params": {"nprobe": nprobe}},
            filter=filter_expr,
            limit=limit,
            output_fields=output_fields,
        )
        hits = HitExtractor.extract_all(
            results,
            identity_field="table_id",  # 优先用 table_id
            metric_type="COSINE",
            score_type="dense",
            fields=output_fields,
        )
        # 兼容处理：若 table_id 为空则用 item_id
        for hit in hits:
            if not hit.get("identity"):
                hit["identity"] = hit.get("payload", {}).get("item_id")
        return hits

    async def _search_sparse(self, question: str, filter_expr: str, limit: int) -> List[Dict]:
        """Sparse BM25 检索（使用 HitExtractor 统一处理）。"""
        # 查询扩展：提取业务实体关键词并增强权重
        from server.config import get_retrieval_param
        expanded_question = self._expand_query_for_sparse(question)
        sparse_query = build_sparse_query(expanded_question)
        if not sparse_query["text"]:
            return []
        output_fields = [
            "item_id",
            "table_id",
            "display_name",
            "description",
            "table_name",
            "schema_name",
            "domain_id",
            "graph_text",
            "json_meta",
        ]

        drop_ratio_search = float(get_retrieval_param("table_retrieval.milvus_search_params.sparse.drop_ratio_search", 0.2) or 0.2)

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
                filter=filter_expr,
                limit=limit,
                output_fields=output_fields,
            )

        results = await _do_search(sparse_query["text"])
        # 若文本召回为空且有 SparseVector payload，使用向量再次尝试（兼容 2.6 SDK）
        if (not results or len(results[0]) == 0) and sparse_query.get("payload"):
            try:
                results = await _do_search(sparse_query["payload"])
            except Exception:
                pass

        hits = HitExtractor.extract_all(
            results,
            identity_field="table_id",  # 优先用 table_id
            metric_type="BM25",
            score_type="sparse",
            fields=output_fields,
        )
        # 兼容处理：若 table_id 为空则用 item_id
        for hit in hits:
            if not hit.get("identity"):
                hit["identity"] = hit.get("payload", {}).get("item_id")
        return hits

    async def _apply_reranker(
        self,
        question: str,
        payloads: List[Dict[str, Any]],
    ) -> List[Optional[float]]:
        """
        应用 Reranker 精排
        
        V2 增强：输入文本包含表名、描述、语义标签(tags)、度量字段
        """
        if not payloads:
            return []
        if not self.reranker or not self.reranker.is_enabled():
            return [None for _ in payloads]
        
        from server.config import RetrievalConfig
        
        docs = []
        for payload in payloads:
            parts = []
            
            # 1. 表名（必须）
            display_name = payload.get("display_name", "")
            if display_name:
                parts.append(f"表名：{display_name}")
            
            # 2. 描述（如果配置启用且存在）
            if RetrievalConfig.reranker_include_description():
                description = payload.get("description")
                if description:
                    parts.append(f"描述：{description}")
            
            # 3. 语义标签（如果配置启用）
            if RetrievalConfig.reranker_include_tags():
                table_id = payload.get("item_id") or payload.get("table_id")
                tags = self._get_table_tags(table_id, payload)
                if tags:
                    parts.append(f"语义标签：{', '.join(tags)}")
            
            # 4. 度量字段（如果配置启用）
            if RetrievalConfig.reranker_include_measures():
                table_id = payload.get("item_id") or payload.get("table_id")
                measures = self._get_table_measures(table_id, payload)
                if measures:
                    parts.append(f"度量字段：{', '.join(measures)}")
            
            doc = "\n".join(parts) if parts else payload.get("display_name", "")
            docs.append(doc)
        
        logger.debug(
            "Reranker 增强输入",
            sample_doc=docs[0] if docs else None,
            doc_count=len(docs)
        )
        
        return await self.reranker.rerank(question, docs)
    
    def _get_table_tags(self, table_id: Optional[str], payload: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        获取表的语义标签
        
        优先从 SemanticModel 获取，fallback 到 payload
        """
        tags = []
        
        # 1. 尝试从 SemanticModel 获取
        if table_id and self.model:
            datasource = self.model.datasources.get(table_id)
            if datasource and hasattr(datasource, 'tags'):
                tags = datasource.tags or []
        
        # 2. Fallback: 从 payload 获取
        if not tags and payload:
            # 尝试从 json_meta 获取
            json_meta = payload.get("json_meta") or {}
            if isinstance(json_meta, str):
                try:
                    import json
                    json_meta = json.loads(json_meta)
                except:
                    json_meta = {}
            tags = json_meta.get("tags") or []
        
        return tags
    
    def _get_table_measures(self, table_id: Optional[str], payload: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        获取表的度量字段名称
        
        优先从 SemanticModel 获取，fallback 到 payload
        """
        measures = []
        
        # 1. 尝试从 SemanticModel 获取
        if table_id and self.model:
            for field_id, field in self.model.fields.items():
                # 检查字段是否属于该表且是度量类型
                if (field.datasource_id == table_id and 
                    field.field_category == 'measure' and
                    field.is_active):
                    measures.append(field.display_name)
        
        return measures

    def _resolve_datasource(self, table_id: str, payload: Dict[str, Any]) -> Datasource:
        if self.model and table_id in self.model.datasources:
            return self.model.datasources[table_id]

        return Datasource(
            datasource_id=table_id,
            datasource_name=payload.get("table_name", ""),
            display_name=payload.get("display_name", ""),
            description=payload.get("description"),
            schema_name=payload.get("schema_name"),
            table_type="table",
        )

    @staticmethod
    def _blend_reranker_score(
        rrf_score: Optional[float],
        rerank_score: Optional[float],
    ) -> Optional[float]:
        """
        融合RRF分数和Reranker分数
        
        当 reranker 分数为 None（或 NaN）时，直接返回 RRF 分数（不受 reranker 影响）。
        注意：0.0 可能是有效的 reranker 分数（尤其当服务返回 [0,1] 或做过归一化时），
        不能作为“无效”的判定条件，否则会丢失低相关性的负向信号，影响 TOP1 稳定性。
        """
        if rerank_score is None:
            return rrf_score
        if isinstance(rerank_score, float) and math.isnan(rerank_score):
            return rrf_score
        
        base = rrf_score or 0.0
        weight = RetrievalConfig.reranker_weight()
        return (1 - weight) * base + weight * rerank_score

    
    def retrieve_tables_by_keywords(
        self,
        keywords: List[str],
        domain_id: Optional[str] = None,
        limit: int = 5
    ) -> List[TableRetrievalResult]:
        """
        基于关键词检索表（Fallback机制，改进版）
        
        当向量检索失败或结果不理想时使用
        
        改进点：
        1. 使用jieba对表名、描述进行分词
        2. 精确词匹配 + 模糊子串匹配
        3. 不同字段给予不同权重
        
        Args:
            keywords: 关键词列表
            domain_id: 业务域ID
            limit: 返回数量
            
        Returns:
            表检索结果列表
        """
        if not self.model or not keywords:
            self.last_retrieval_info = {
                "method": "keyword",
                "keyword_count": len(keywords or []),
                "domain_id": domain_id,
                "reason": "no_model_or_keywords"
            }
            return []
        
        try:
            import jieba
            use_jieba = True
        except ImportError:
            logger.warning("jieba未安装，使用简单匹配")
            use_jieba = False
        
        results = []
        
        for table_id, datasource in self.model.datasources.items():
            # V3：不基于 domain_id 做硬过滤，避免误杀（domain 仅用于排序算子）
            
            # 获取表的各个字段
            table_name = datasource.display_name or ""
            table_desc = datasource.description or ""
            db_name = datasource.datasource_name or ""
            
            # 分词（如果有jieba）
            if use_jieba:
                name_words = set(jieba.lcut(table_name.lower()))
                desc_words = set(jieba.lcut(table_desc.lower()))
                db_words = set(jieba.lcut(db_name.lower()))
            else:
                # 简单split作为fallback
                name_words = set(table_name.lower().split())
                desc_words = set(table_desc.lower().split())
                db_words = set(db_name.lower().split())
            
            # 计算匹配得分
            score = 0.0
            matched_count = 0
            match_details = []
            
            for keyword in keywords:
                kw_lower = keyword.lower()
                matched = False
                
                # 策略1：表名精确匹配（权重最高）
                if kw_lower in name_words:
                    score += 3.0
                    matched = True
                    match_details.append(f"{keyword}→表名")
                
                # 策略2：描述精确匹配
                elif kw_lower in desc_words:
                    score += 2.0
                    matched = True
                    match_details.append(f"{keyword}→描述")
                
                # 策略3：数据库表名精确匹配
                elif kw_lower in db_words:
                    score += 1.5
                    matched = True
                    match_details.append(f"{keyword}→表名")
                
                # 策略4：模糊子串匹配（权重较低）
                elif kw_lower in table_name.lower():
                    score += 1.0
                    matched = True
                    match_details.append(f"{keyword}→表名(模糊)")
                
                elif kw_lower in table_desc.lower():
                    score += 0.5
                    matched = True
                    match_details.append(f"{keyword}→描述(模糊)")
                
                if matched:
                    matched_count += 1
            
            if score > 0:
                # 综合得分计算：
                # 1. 匹配比例：匹配的关键词数 / 总关键词数
                # 2. 加权得分：实际得分 / 最大可能得分
                match_ratio = matched_count / len(keywords)
                max_possible_score = len(keywords) * 3.0
                weighted_score = score / max_possible_score
                
                # 最终得分：匹配比例60% + 加权得分40%
                normalized_score = min(match_ratio * 0.6 + weighted_score * 0.4, 1.0)
                
                results.append(TableRetrievalResult(
                    table_id=table_id,
                    datasource=datasource,
                    score=normalized_score,
                    field_count=self._estimate_field_count_from_model(table_id),
                    connection_id=datasource.connection_id  # 填充连接ID
                ))
                
                logger.debug("关键词匹配",
                           table=table_name,
                           matched=matched_count,
                           total=len(keywords),
                           score=round(normalized_score, 3),
                           details=match_details)
        
        # 按得分排序
        results.sort(key=lambda x: x.score, reverse=True)
        
        logger.debug(
            "关键词表检索完成",
            keywords=keywords,
            found_tables=len(results),
            top3=[(r.datasource.display_name, round(r.score, 3)) for r in results[:3]]
        )
        self.last_retrieval_info = {
            "method": "keyword",
            "keyword_count": len(keywords),
            "domain_id": domain_id,
            "found_tables": len(results),
            "top3": [
                (r.datasource.display_name, round(r.score, 3))
                for r in results[:3]
            ]
        }
        
        return results[:limit]
