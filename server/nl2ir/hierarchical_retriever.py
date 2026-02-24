"""
层次化检索器
业务域 → 表 → 字段的三层检索架构

支持两种表选择模式：
1. 向量检索模式（默认）：使用 Milvus 进行 Dense + Sparse 检索
2. LLM 表选择模式（新增）：使用 LLM 智能选择表，支持置信度判断和用户确认
"""

import asyncio
from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
import structlog
import re

from server.models.semantic import SemanticModel
from server.models.api import TableCandidate, TableSelectionCard
from server.nl2ir.domain_detector import (
    DomainDetector,
    DomainCandidate,
    DomainDetectionResult,
)
from server.nl2ir.table_retriever import TableRetriever, TableRetrievalResult
from server.nl2ir.few_shot_retriever import FewShotRetriever, FewShotSample
from server.nl2ir.table_structure_loader import TableStructureLoader, TableStructure
from server.nl2ir.hierarchical_prompt_builder import HierarchicalPromptBuilder
from server.utils.model_clients import RerankerClient
from server.nl2ir.keyword_pipeline import KeywordExtractor, KeywordExtractionResult
from server.config import settings, RetrievalConfig, get_retrieval_param
from server.nl2ir.field_precision import get_field_precision_inferencer

if TYPE_CHECKING:
    from server.nl2ir.llm_table_selector import LLMTableSelector, TableSelectionResult

logger = structlog.get_logger()


@dataclass
class HierarchicalRetrievalResult:
    """层次化检索结果"""
    domain_id: Optional[str]
    domain_name: Optional[str]
    table_results: List[TableRetrievalResult]  # 表检索结果
    table_structures: List[TableStructure]      # 表结构（包含字段）
    prompt_context: str                          # 格式化的Prompt
    domain_detection_method: Optional[str] = None
    table_retrieval_method: Optional[str] = None
    table_retrieval_info: Optional[Dict[str, Any]] = None
    keyword_fallback_used: bool = False
    query_vector_dim: Optional[int] = None
    few_shot_examples: List[FewShotSample] = field(default_factory=list)
    few_shot_direct_candidates: List[FewShotSample] = field(default_factory=list)
    domain_candidates: List[Dict[str, Any]] = field(default_factory=list)
    domain_fallback_reason: Optional[str] = None
    keyword_profile: Optional[KeywordExtractionResult] = None
    question_vector: Optional[List[float]] = None
    
    # LLM 表选择相关字段
    table_selection_needed: bool = False  # 是否需要用户确认表选择
    table_selection_card: Optional[TableSelectionCard] = None  # 表选择确认卡
    table_selection_action: Optional[str] = None  # "execute" | "confirm" | "clarify"
    
    # 多连接支持：连接检测结果
    detected_connection_id: Optional[str] = None  # 检测到的主连接
    involved_connections: Dict[str, int] = field(default_factory=dict)  # {connection_id: table_count}
    cross_connection_warning: Optional[str] = None  # 跨连接警告信息


class HierarchicalRetriever:
    """
    层次化检索器

    实现三层检索架构：
    1. 业务域识别
    2. 表级向量检索
    3. 字段结构加载
    """

    def __init__(
        self,
        semantic_model: SemanticModel,
        domain_detector: Optional[DomainDetector] = None,
        table_retriever: Optional[TableRetriever] = None,
        milvus_client=None,
        embedding_client=None,
        connection_id: Optional[str] = None,
        db_pool: Optional[Any] = None,
        entity_recognizer: Optional[Any] = None,
    ):
        """
        初始化层次化检索器

        Args:
            semantic_model: 语义模型
            domain_detector: 业务域检测器（可选）
            table_retriever: 表检索器（可选）
            milvus_client: Milvus客户端
            embedding_client: Embedding客户端
        """
        self.model = semantic_model
        self.embedding_client = embedding_client  #  保存用于向量复用
        self.milvus_client = milvus_client
        self.db_pool = db_pool
        self.reranker = RerankerClient()
        self.entity_recognizer = entity_recognizer

        # 如果没有提供domain_detector，创建一个（支持向量检索）
        if domain_detector is None and milvus_client and embedding_client:
            self.domain_detector = DomainDetector(
                semantic_model=semantic_model,
                milvus_client=milvus_client,
                embedding_client=embedding_client,
                use_vector_search=True,
                connection_id=connection_id,
            )
        else:
            self.domain_detector = domain_detector

        if self.domain_detector:
            self.domain_detector.set_clients(milvus_client, embedding_client, connection_id)

        # 如果没有提供table_retriever，创建一个
        if table_retriever is None:
            self.table_retriever = TableRetriever(
                milvus_client=milvus_client,
                embedding_client=embedding_client,
                semantic_model=semantic_model,
                connection_id=connection_id,
                reranker=self.reranker,
            )
        else:
            self.table_retriever = table_retriever
            if hasattr(self.table_retriever, "reranker"):
                self.table_retriever.reranker = self.reranker

        # 创建表结构加载器和Prompt构建器
        self.structure_loader = TableStructureLoader(semantic_model)
        self.prompt_builder = HierarchicalPromptBuilder(semantic_model)
        self.connection_id = connection_id
        self.keyword_extractor = KeywordExtractor(semantic_model)

        # 从配置加载表打分 V2 参数
        # 枚举加成全局开关（从 retrieval_config.yaml 读取）
        self._enum_boost_enabled = RetrievalConfig.enum_boost_enabled()
        # 枚举门控参数（从 enum_field_precision.yaml 读取）
        self._enum_gate_context_threshold = RetrievalConfig.enum_gate_context_threshold()
        self._enum_gate_exact_boost = RetrievalConfig.enum_gate_exact_boost()
        self._enum_gate_vector_boost = RetrievalConfig.enum_gate_vector_boost()
        self._enum_gate_max_boost = RetrievalConfig.enum_gate_max_boost()
        self._measure_coverage_enabled = RetrievalConfig.measure_coverage_enabled()
        self._measure_coverage_partial_min = RetrievalConfig.measure_coverage_partial_min()
        self._measure_coverage_keywords = RetrievalConfig.measure_coverage_keywords()

        # 度量表级信号（PG/Milvus）与救援（默认开启，可配置关闭；并受“度量意图门控”约束）
        self._measure_pg_enabled = RetrievalConfig.measure_pg_enabled()
        self._measure_pg_weight = RetrievalConfig.measure_pg_weight()
        self._measure_pg_top_k_fields = RetrievalConfig.measure_pg_top_k_fields()
        self._measure_pg_table_agg = RetrievalConfig.measure_pg_table_agg()
        self._measure_pg_apply_when_measure_intent = RetrievalConfig.measure_pg_apply_when_measure_intent()

        self._measure_field_milvus_enabled = RetrievalConfig.measure_field_milvus_enabled()
        self._measure_field_milvus_weight = RetrievalConfig.measure_field_milvus_weight()
        self._measure_field_milvus_top_k_fields = RetrievalConfig.measure_field_milvus_top_k_fields()
        self._measure_field_milvus_min_field_score = RetrievalConfig.measure_field_milvus_min_field_score()
        self._measure_field_milvus_min_field_score_ratio = RetrievalConfig.measure_field_milvus_min_field_score_ratio()
        self._measure_field_milvus_use_measure_query_vector = RetrievalConfig.measure_field_milvus_use_measure_query_vector()
        self._measure_field_milvus_apply_when_measure_intent = RetrievalConfig.measure_field_milvus_apply_when_measure_intent()

        self._measure_rescue_enabled = RetrievalConfig.measure_rescue_enabled()
        self._measure_rescue_threshold = RetrievalConfig.measure_rescue_threshold()

        self._measure_retriever = None
        self._measure_field_retriever = None
        self._rescue_enabled = RetrievalConfig.table_rescue_enabled()
        self._rescue_min_score = RetrievalConfig.table_rescue_min_score()
        self._high_noise_fields = set(RetrievalConfig.enum_high_noise_fields())

        if milvus_client and embedding_client:
            self.few_shot_retriever = FewShotRetriever(
                milvus_client=milvus_client,
                embedding_client=embedding_client,
                reranker=self.reranker,
                semantic_model=semantic_model,
            )
        else:
            self.few_shot_retriever = None
        
        # LLM 表选择器（当启用时延迟初始化）
        self.llm_table_selector: Optional["LLMTableSelector"] = None
        self._llm_table_selection_enabled = settings.llm_table_selection_enabled

    async def _init_llm_table_selector(self, llm_client) -> None:
        """延迟初始化 LLM 表选择器"""
        if self.llm_table_selector is None and self._llm_table_selection_enabled:
            from server.nl2ir.llm_table_selector import LLMTableSelector
            self.llm_table_selector = LLMTableSelector(
                llm_client=llm_client,
                structure_loader=self.structure_loader
            )

    async def retrieve_with_llm_selection(
        self,
        question: str,
        llm_client,
        user_domain_id: Optional[str] = None,
        global_rules: Optional[List[dict]] = None,
        selected_table_id: Optional[str] = None,
        user_id: Optional[str] = None,
        user_role: Optional[str] = None
    ) -> HierarchicalRetrievalResult:
        """
        使用 LLM 进行表选择的检索
        
        Args:
            question: 用户问题
            llm_client: LLM 客户端
            user_domain_id: 用户指定的业务域
            global_rules: 全局规则列表
            selected_table_id: 用户已确认的表ID（如果有）
            user_id: 用户ID（用于权限过滤）
            user_role: 用户角色（用于权限过滤）
        
        Returns:
            HierarchicalRetrievalResult
        """
        from server.nl2ir.llm_table_selector import (
            LLMTableSelector, 
            TableMeta, 
            load_all_tables_meta
        )
        
        logger.debug("开始 LLM 表选择检索", question=question, selected_table_id=selected_table_id)
        
        # 1. 初始化 LLM 表选择器
        await self._init_llm_table_selector(llm_client)
        
        # 2. 如果用户已确认表选择，直接使用该表
        if selected_table_id:
            logger.debug("使用用户确认的表", table_id=selected_table_id)
            return await self._build_result_from_selected_table(
                question=question,
                selected_table_id=selected_table_id,
                global_rules=global_rules
            )
        
        # 3. 加载所有表的精简元数据（带用户权限过滤）
        all_tables_meta = await load_all_tables_meta(
            connection_id=self.connection_id,
            structure_loader=self.structure_loader,
            user_id=user_id,
            user_role=user_role
        )
        
        if not all_tables_meta:
            logger.warning("LLM表选择：无可用表")
            return HierarchicalRetrievalResult(
                domain_id=None,
                domain_name=None,
                table_results=[],
                table_structures=[],
                prompt_context="",
                table_retrieval_method="llm_selection",
                table_selection_needed=False,
                table_selection_action="clarify"
            )
        
        # 4. 调用 LLM 选表
        selection_result = await self.llm_table_selector.select_tables(
            question=question,
            all_tables_meta=all_tables_meta
        )
        
        # 5. 根据选择结果决定下一步
        if selection_result.action == "confirm":
            # 需要用户确认
            logger.debug(
                "LLM表选择需要确认",
                candidates=[c.table_name for c in selection_result.candidates]
            )
            return HierarchicalRetrievalResult(
                domain_id=None,
                domain_name=None,
                table_results=[],
                table_structures=[],
                prompt_context="",
                table_retrieval_method="llm_selection",
                table_retrieval_info={
                    "selection_summary": selection_result.selection_summary,
                    "candidates": [c.model_dump() for c in selection_result.candidates]
                },
                table_selection_needed=True,
                table_selection_card=TableSelectionCard(
                    candidates=selection_result.candidates,
                    question=question,
                    message="系统找到了多个可能相关的表，请确认您要查询的是哪张表：",
                    confirmation_reason="请选择最符合您查询意图的数据表"
                ),
                table_selection_action="confirm"
            )
        
        if selection_result.action == "clarify":
            # 置信度过低，需要澄清
            logger.warning("LLM表选择：置信度过低")
            return HierarchicalRetrievalResult(
                domain_id=None,
                domain_name=None,
                table_results=[],
                table_structures=[],
                prompt_context="",
                table_retrieval_method="llm_selection",
                table_selection_needed=False,
                table_selection_action="clarify"
            )
        
        # 6. action == "execute"：直接使用选中的表
        primary_table_id = selection_result.primary_table_id
        if not primary_table_id:
            primary_table_id = selection_result.candidates[0].table_id if selection_result.candidates else None
        
        if not primary_table_id:
            logger.warning("LLM表选择：未选中任何表")
            return HierarchicalRetrievalResult(
                domain_id=None,
                domain_name=None,
                table_results=[],
                table_structures=[],
                prompt_context="",
                table_retrieval_method="llm_selection",
                table_selection_action="clarify"
            )
        
        return await self._build_result_from_selected_table(
            question=question,
            selected_table_id=primary_table_id,
            global_rules=global_rules,
            selection_result=selection_result
        )
    
    async def _build_result_from_selected_table(
        self,
        question: str,
        selected_table_id: str,
        global_rules: Optional[List[dict]] = None,
        selection_result: Optional["TableSelectionResult"] = None
    ) -> HierarchicalRetrievalResult:
        """
        根据选中的表构建检索结果
        """
        # 1. 加载表结构
        try:
            table_structure = self.structure_loader.load_table_structure(selected_table_id)
        except ValueError as e:
            logger.error("加载表结构失败", table_id=selected_table_id, error=str(e))
            return HierarchicalRetrievalResult(
                domain_id=None,
                domain_name=None,
                table_results=[],
                table_structures=[],
                prompt_context="",
                table_retrieval_method="llm_selection",
                table_selection_action="clarify"
            )
        
        # 2. 构建 TableRetrievalResult
        datasource = self.model.datasources.get(selected_table_id)
        confidence = 1.0
        if selection_result and selection_result.candidates:
            for c in selection_result.candidates:
                if c.table_id == selected_table_id:
                    confidence = c.confidence
                    break
        
        table_result = TableRetrievalResult(
            table_id=selected_table_id,
            datasource=datasource,
            score=confidence,
            field_count=table_structure.total_fields
        )
        
        # 3. 获取域信息
        domain_id = table_structure.domain_id
        domain_name = table_structure.domain_name
        
        # 4. 构建 Prompt
        table_score_map = {selected_table_id: table_result}
        prompt_context = self.prompt_builder.build_context(
            table_structures=[table_structure],
            question=question,
            domain_name=domain_name,
            global_rules=global_rules,
            table_scores=table_score_map
        )
        
        # 5. 构建检索信息
        retrieval_info = {
            "method": "llm_selection",
            "selected_table_id": selected_table_id,
            "confidence": confidence
        }
        if selection_result:
            retrieval_info["selection_summary"] = selection_result.selection_summary
            retrieval_info["candidates"] = [
                {"table_id": c.table_id, "table_name": c.table_name, "confidence": c.confidence}
                for c in selection_result.candidates[:3]
            ]
        
        logger.debug(
            "LLM表选择完成",
            selected_table=table_structure.display_name,
            confidence=confidence
        )
        
        return HierarchicalRetrievalResult(
            domain_id=domain_id,
            domain_name=domain_name,
            table_results=[table_result],
            table_structures=[table_structure],
            prompt_context=prompt_context,
            table_retrieval_method="llm_selection",
            table_retrieval_info=retrieval_info,
            table_selection_needed=False,
            table_selection_action="execute"
        )

    async def retrieve(
        self,
        question: str,
        user_domain_id: Optional[str] = None,
        top_k_tables: Optional[int] = None,
        global_rules: Optional[List[dict]] = None,
        selected_table_id: Optional[str] = None,
        selected_table_ids: Optional[List[str]] = None,
        user_id: Optional[str] = None,
        user_role: Optional[str] = None
    ) -> HierarchicalRetrievalResult:
        """
        执行层次化检索

        Args:
            question: 用户问题
            user_domain_id: 用户指定的业务域（优先级最高）
            top_k_tables: 返回前K个表（通过环境变量可配置）
            global_rules: 全局规则列表
            selected_table_id: 已选择的表ID（兼容单表模式）
            selected_table_ids: 已选择的多个表ID（跨年查询等多表场景）
            user_id: 用户ID（用于权限过滤）
            user_role: 用户角色（用于权限过滤）

        Returns:
            HierarchicalRetrievalResult: 包含域、表、字段的完整检索结果
        """
        logger.debug("开始层次化检索", question=question, selected_table_id=selected_table_id, selected_table_ids=selected_table_ids)
        
        # 提前提取通用度量词典（用于表检索权重计算）
        self._universal_measures = self._extract_universal_measures_from_rules(global_rules)
        if self._universal_measures:
            logger.info(
                "提取通用度量词典完成",
                measure_count=len(self._universal_measures),
                measures=list(self._universal_measures.keys())
            )

        # 如果未指定top_k_tables，使用YAML配置的默认值
        if top_k_tables is None:
            top_k_tables = RetrievalConfig.table_top_k()

        # ═══════════════════════════════════════════════════════════
        # 快速路径：如果已选择多个表（跨年查询等），直接加载这些表
        # ═══════════════════════════════════════════════════════════
        if selected_table_ids and len(selected_table_ids) > 1:
            logger.debug("使用已选择的多个表ID（跨年/多表查询）", selected_table_ids=selected_table_ids)
            return await self._retrieve_with_selected_tables(
                question=question,
                selected_table_ids=selected_table_ids,
                user_domain_id=user_domain_id,
                global_rules=global_rules
            )
        
        # ═══════════════════════════════════════════════════════════
        # 快速路径：如果已选择单个表ID，跳过向量检索，直接加载该表
        # ═══════════════════════════════════════════════════════════
        if selected_table_id:
            logger.debug("使用已选择的表ID，跳过表检索", selected_table_id=selected_table_id)
            return await self._retrieve_with_selected_table(
                question=question,
                selected_table_id=selected_table_id,
                user_domain_id=user_domain_id,
                global_rules=global_rules
            )

        # ═══════════════════════════════════════════════════════════
        # 向量复用优化：问题向量只生成一次
        # ═══════════════════════════════════════════════════════════
        query_vector = None
        query_vector_dim = None
        if self.embedding_client:
            try:
                logger.debug("生成问题向量（全局复用）")
                query_vector = await self.embedding_client.embed_single(question)
                query_vector_dim = len(query_vector)
                logger.debug("问题向量生成完成", vector_dim=query_vector_dim)
            except Exception as e:
                logger.warning("问题向量生成失败，将使用关键词检索", error=str(e))

        keyword_profile = None
        try:
            keyword_profile = self.keyword_extractor.extract(question)
        except Exception as keyword_error:
            logger.warning("关键词提取失败，将跳过 Boost", error=str(keyword_error))

        # ═══════════════════════════════════════════════════════════
        # V3 Pipeline：并行执行业务域识别 + 全量表召回（取消 1->2 硬依赖）
        # 目标：提升 Recall，避免 domain 前置过滤误杀
        # ═══════════════════════════════════════════════════════════
        domain_id: Optional[str] = user_domain_id
        domain_name: Optional[str] = None
        domain_candidates_payload: List[Dict[str, Any]] = []
        domain_fallback_reason: Optional[str] = None
        candidate_domain_ids: List[str] = []
        source_domain_id_for_fewshot = user_domain_id

        detection_method: Optional[str] = None
        detection_result: Optional[DomainDetectionResult] = None

        # 任务1：业务域识别（可选）
        domain_task = None
        if user_domain_id:
            detection_method = "user_specified"
            domain_name = self._resolve_domain_name(user_domain_id)
            manual_candidate = DomainCandidate(
                domain_id=user_domain_id,
                domain_name=domain_name,
                dense_score=1.0,
                sparse_score=0.0,
                rrf_score=1.0,
                rank=1,
                source_scores={"manual": {"score": 1.0, "rank": 1}},
            )
            detection_result = DomainDetectionResult(
                primary_domain_id=user_domain_id,
                primary_domain_name=domain_name,
                detection_method=detection_method,
                candidates=[manual_candidate],
            )
            domain_candidates_payload = self._serialize_domain_candidates(
                [manual_candidate],
                source=detection_method,
            )
            source_domain_id_for_fewshot = user_domain_id
        elif self.domain_detector:
            domain_task = asyncio.create_task(
                self.domain_detector.detect(question, query_vector=query_vector)
            )

        # 任务2：表召回（全量 Milvus 表索引；domain_id 不参与过滤）
        actual_top_k = top_k_tables
        actual_threshold = RetrievalConfig.table_threshold()
        table_task = asyncio.create_task(
            self.table_retriever.retrieve_relevant_tables(
                question=question,
                domain_id=None,  # 取消前置 domain filter
                connection_id=self.connection_id,
                top_k=actual_top_k,
                threshold=actual_threshold,
                query_vector=query_vector,  # 复用向量
                candidate_domain_ids=[],  # 仅用于 trace/诊断，不影响过滤
                keyword_profile=keyword_profile,
            )
        )

        logger.debug(
            "使用配置的检索参数（V3全量召回）",
            top_k=actual_top_k,
            threshold=actual_threshold,
        )

        # 等待并行任务
        table_results: List[TableRetrievalResult] = []
        try:
            table_results = await table_task
        except Exception as e:
            logger.warning("表检索任务失败", error=str(e))
            table_results = []

        # 表检索诊断信息（无论是否成功都尽量保留）
        table_retrieval_info: Dict[str, Any] = getattr(self.table_retriever, "last_retrieval_info", {}) or {}
        table_retrieval_method = table_retrieval_info.get("method", "vector")

        # 等待 domain 任务（如果有）
        if domain_task is not None:
            try:
                detection_result = await domain_task
            except Exception as e:
                logger.warning("业务域识别任务失败", error=str(e))
                detection_result = None

        if detection_result and not user_domain_id:
            domain_id = detection_result.primary_domain_id
            domain_name = detection_result.primary_domain_name or self._resolve_domain_name(domain_id)
            detection_method = detection_result.detection_method
            domain_fallback_reason = detection_result.fallback_reason
            domain_candidates_payload = self._serialize_domain_candidates(
                detection_result.candidates,
                source=detection_method,
            )
            candidate_domain_ids = [
                candidate["domain_id"]
                for candidate in domain_candidates_payload
                if candidate.get("domain_id") and candidate.get("domain_id") != domain_id
            ]
            source_domain_id_for_fewshot = domain_id
            if domain_id:
                logger.debug("业务域识别完成（V3并行）", domain_id=domain_id, domain_name=domain_name)
            # 将候选域信息写入表检索info（仅用于trace/诊断）
            try:
                table_retrieval_info.setdefault("domain_candidates", domain_candidates_payload)
            except Exception:
                pass
        keyword_fallback_used = False

        if not table_results:
            logger.warning("未找到相关表，尝试关键词检索")
            # Fallback：使用关键词检索
            keywords = (
                keyword_profile.raw_tokens if keyword_profile else self._extract_keywords(question)
            )
            table_results = self.table_retriever.retrieve_tables_by_keywords(
                keywords=keywords,
                domain_id=None,  # V3：不做 domain 过滤
                limit=top_k_tables
            )
            table_retrieval_method = "keyword"
            keyword_fallback_used = True
            fallback_info = getattr(self.table_retriever, "last_retrieval_info", None)
            if fallback_info:
                table_retrieval_info = fallback_info
            else:
                table_retrieval_info = {
                    "method": "keyword",
                    "keyword_count": len(keywords)
                }

        logger.debug(
            "表级检索完成（权限过滤前）",
            found_tables=len(table_results),
            top_score=table_results[0].score if table_results else 0
        )
        
        # ═══════════════════════════════════════════════════════════
        # 用户权限过滤：过滤用户无权访问的表
        # ═══════════════════════════════════════════════════════════
        if user_id and user_role and user_role != 'admin':
            table_results = await self._filter_tables_by_permission(
                table_results, user_id, user_role
            )
            logger.debug(
                "表级检索完成（权限过滤后）",
                found_tables=len(table_results),
                top_score=table_results[0].score if table_results else 0
            )
        
        table_retrieval_info["found_tables"] = len(table_results)
        table_retrieval_info["top_score"] = table_results[0].score if table_results else 0
        # V3：domain 不再由表检索阶段"反推/覆盖"，仅作为元信息与排序算子输入

        self.last_retrieval_info = table_retrieval_info

        # ═══════════════════════════════════════════════════════════
        # 第3层：加载表结构（字段）
        # ═══════════════════════════════════════════════════════════
        table_structures = self._load_table_structures(table_results)

        few_shot_examples: List[FewShotSample] = []
        few_shot_direct_candidates: List[FewShotSample] = []
        few_shot_info: Optional[Dict[str, Any]] = None
        if self.few_shot_retriever and self.connection_id:
            few_shot_examples = await self.few_shot_retriever.retrieve(
                question=question,
                connection_id=self.connection_id,
                query_vector=query_vector,
                source_domain_id=source_domain_id_for_fewshot,
                resolved_domain_id=domain_id,
            )
            few_shot_info = getattr(self.few_shot_retriever, "last_retrieval_info", None) or {}
            few_shot_direct_candidates = list(
                getattr(self.few_shot_retriever, "last_direct_candidates", few_shot_examples)
            )
            logger.debug(
                "Few-Shot召回完成",
                connection_id=self.connection_id,
                samples=len(few_shot_examples),
                retrieval_info=few_shot_info,
            )
        else:
            logger.debug(
                "Few-Shot检索未执行",
                has_retriever=bool(self.few_shot_retriever),
                connection_id=self.connection_id,
            )

        # ═══════════════════════════════════════════════════════════
        # 构建Prompt
        # ═══════════════════════════════════════════════════════════
        table_score_map = {result.table_id: result for result in table_results}

        prompt_context = self.prompt_builder.build_context(
            table_structures=table_structures,
            question=question,
            domain_name=domain_name,
            global_rules=global_rules,
            few_shot_examples=few_shot_examples,
            table_scores=table_score_map,
        )

        logger.debug("Prompt构建完成",
                    length=len(prompt_context),
                    tables=len(table_structures))
        
        # ═══════════════════════════════════════════════════════════
        # 连接检测：统计涉及的表所属连接
        # ═══════════════════════════════════════════════════════════
        detected_connection_id = None
        involved_connections = {}
        cross_connection_warning = None
        
        for result in table_results:
            if result.connection_id:
                involved_connections[result.connection_id] = \
                    involved_connections.get(result.connection_id, 0) + 1
        
        if len(involved_connections) == 1:
            # 单连接
            detected_connection_id = list(involved_connections.keys())[0]
            logger.debug("检测到单连接", connection_id=detected_connection_id)
        elif len(involved_connections) > 1:
            # 多连接
            # 选择表数量最多的连接作为主连接
            detected_connection_id = max(
                involved_connections.items(),
                key=lambda x: x[1]
            )[0]
            cross_connection_warning = (
                f"查询涉及{len(involved_connections)}个数据库连接，"
                f"已自动选择主连接（{detected_connection_id}）"
            )
            logger.warning(
                "检测到跨连接查询",
                connections=list(involved_connections.keys()),
                distribution=involved_connections,
                primary=detected_connection_id
            )

        # 返回完整结果
        return HierarchicalRetrievalResult(
            domain_id=domain_id,
            domain_name=domain_name,
            table_results=table_results,
            table_structures=table_structures,
            prompt_context=prompt_context,
            domain_detection_method=detection_method,
            table_retrieval_method=table_retrieval_method,
            table_retrieval_info=(
                {**table_retrieval_info, "few_shot": few_shot_info}
                if few_shot_info
                else table_retrieval_info
            ),
            keyword_fallback_used=keyword_fallback_used,
            query_vector_dim=query_vector_dim,
            few_shot_examples=few_shot_examples,
            few_shot_direct_candidates=few_shot_direct_candidates,
            domain_candidates=domain_candidates_payload,
            domain_fallback_reason=domain_fallback_reason,
            keyword_profile=keyword_profile,
            question_vector=query_vector,
            # 多连接支持
            detected_connection_id=detected_connection_id,
            involved_connections=involved_connections,
            cross_connection_warning=cross_connection_warning,
        )

    async def _retrieve_tables_with_multi_domain_strategy(
        self,
        question: str,
        detection_result: Optional[DomainDetectionResult],
        top_k_tables: int,
        threshold: float,
        query_vector: Optional[List[float]],
        keyword_profile: Optional[KeywordExtractionResult],
    ) -> Optional[Dict[str, Any]]:
        """
        多域全局统一检索（方案A实现）
        
        核心改进：
        1. 不再对每个域分别检索，改为全局统一检索
        2. Dense/Sparse使用全局排名计算RRF分数（确保跨域可比）
        3. Reranker对全局TOP表统一打分
        4. 按域分组后，综合域分数和表分数进行最终排序
        """
        if not detection_result or not detection_result.candidates:
            return None

        candidate_limit = max(1, RetrievalConfig.multi_domain_candidate_count())
        seen_domains: set = set()
        domain_candidates: List[DomainCandidate] = []
        for candidate in detection_result.candidates:
            if not candidate.domain_id or candidate.domain_id in seen_domains:
                continue
            seen_domains.add(candidate.domain_id)
            domain_candidates.append(candidate)
            if len(domain_candidates) >= candidate_limit:
                break

        if not domain_candidates:
            return None

        candidate_domain_ids = [c.domain_id for c in domain_candidates if c.domain_id]
        
        logger.info(
            "执行全局多域表检索策略",
            domain_count=len(domain_candidates),
            domains=candidate_domain_ids,
        )

        # 构建域ID到候选对象的映射
        domain_candidate_map: Dict[str, DomainCandidate] = {
            c.domain_id: c for c in domain_candidates if c.domain_id
        }
        
        # 全局统一表检索（优先）；若 Milvus/Embedding 不可用，则降级为“逐域独立检索”
        domain_table_map: Dict[str, List[TableRetrievalResult]] = {}
        global_top_k = RetrievalConfig.global_table_retrieval_limit()
        can_use_global = bool(
            getattr(self, "table_retriever", None)
            and getattr(self.table_retriever, "milvus_client", None)
            and getattr(self.table_retriever, "embedding_client", None)
        )

        if can_use_global:
            domain_table_map = await self.table_retriever.retrieve_tables_for_multi_domain(
                question=question,
                candidate_domain_ids=candidate_domain_ids,
                query_vector=query_vector,
                top_k_per_domain=top_k_tables,
                global_top_k=global_top_k,
                keyword_profile=keyword_profile,
            )
        else:
            logger.warning("Milvus或Embedding客户端未配置，多域检索降级为逐域独立检索")
            for candidate in domain_candidates:
                try:
                    payload = await self._run_multi_domain_retrieval_for_candidate(
                        candidate=candidate,
                        question=question,
                        top_k_tables=top_k_tables,
                        threshold=threshold,
                        query_vector=query_vector,
                        keyword_profile=keyword_profile,
                    )
                    domain_table_map[str(candidate.domain_id)] = payload.get("tables", []) or []
                except Exception:
                    domain_table_map[str(candidate.domain_id)] = []
        
        if not domain_table_map or all(len(t) == 0 for t in domain_table_map.values()):
            logger.debug("全局多域检索未返回任何表结果")
            return None

        # 获取质量评估权重
        weights = RetrievalConfig.multi_domain_quality_weights()
        table_weight = float(weights.get("table_score", 0.9))
        domain_weight = float(weights.get("domain_score", 0.1))
        total_weight = max(table_weight + domain_weight, 1e-6)
        table_weight /= total_weight
        domain_weight /= total_weight

        # 收集所有表和域信息
        candidate_tables: List[Dict[str, Any]] = []
        domain_details: List[Dict[str, Any]] = []
        domain_score_map: Dict[str, float] = {}
        # 兼容：旧字段名 min_table_field_count（历史配置），新字段名 min_field_count（与其他配置命名一致）
        min_field_count = int(
            get_retrieval_param(
                "table_retrieval.multi_domain_retrieval.min_field_count",
                get_retrieval_param("table_retrieval.multi_domain_retrieval.min_table_field_count", 1),
            )
            or 1
        )

        for domain_id, tables in domain_table_map.items():
            candidate = domain_candidate_map.get(domain_id)
            if not candidate:
                continue

            raw_tables = list(tables or [])
            usable_tables = [
                t for t in raw_tables if max(0, int(getattr(t, "field_count", 0) or 0)) >= min_field_count
            ]
            
            domain_score = self._extract_domain_candidate_score(candidate)
            domain_score_map[domain_id] = domain_score
            
            domain_details.append({
                "domain_id": domain_id,
                "domain_name": candidate.domain_name,
                "domain_score": domain_score,
                "raw_table_count": len(raw_tables),
                "usable_table_count": len(usable_tables),
                "dropped_table_count": max(0, len(raw_tables) - len(usable_tables)),
                "top_table_score": usable_tables[0].score if usable_tables else (raw_tables[0].score if raw_tables else 0.0),
                "top_field_count": usable_tables[0].field_count if usable_tables else (raw_tables[0].field_count if raw_tables else 0),
                "retrieval_top_score": usable_tables[0].score if usable_tables else (raw_tables[0].score if raw_tables else None),
                "retrieval_found_tables": len(raw_tables),
            })
            
            for table in usable_tables:
                candidate_tables.append({
                    "domain": candidate,
                    "domain_score": domain_score,
                    "table": table,
                    "field_count": max(0, table.field_count or 0),
                })

        if not candidate_tables:
            logger.info("全局多域检索未找到可用表", domains=candidate_domain_ids)
            return None

        # ========== V2.2 改进：在多域选表前应用 measure_factor ==========
        # 目的：让业务语义匹配度不高的表在选表阶段就被降权，避免选错域
        # 例如：用户问 "新批复的建设用地"，"新增建设用地批复" 应该胜出，
        # 即使 "2024年土地利用现状" 的向量语义分数更高
        if self._measure_coverage_enabled:
            # 1. 加载所有候选表的度量字段缓存
            all_table_results = [entry["table"] for entry in candidate_tables]
            table_measures_cache = self._load_table_measures_cache(all_table_results)
            
            # 2. 收集所有度量（用于动态关键词提取）
            all_measures = []
            for measures in table_measures_cache.values():
                all_measures.extend(measures)
            
            # 3. 计算每个表的 measure_factor 并调整分数
            for entry in candidate_tables:
                table = entry["table"]
                table_id = table.table_id
                table_measures = table_measures_cache.get(table_id, [])
                
                measure_factor, measure_detail = self._calculate_measure_coverage_factor(
                    question, table_id, table_measures, all_measures
                )

                # 强门槛：仅在“聚合词/数字+单位”等强证据存在时才允许降权（多域阶段无 PG/Milvus 证据）
                measure_factor, measure_gate = self._apply_measure_factor_strong_gate(
                    table_id=table_id,
                    measure_factor=measure_factor,
                    measure_detail=measure_detail,
                    apply_measure_signals=bool((measure_detail or {}).get("required") or []),
                )
                entry["measure_gate"] = measure_gate
                
                # 调整分数（乘法因子）
                original_score = table.score or 0.0
                adjusted_score = original_score * measure_factor
                
                # 更新分数和记录
                entry["original_score"] = original_score
                entry["measure_factor"] = measure_factor
                entry["adjusted_score"] = adjusted_score
                entry["measure_detail"] = measure_detail
                
                # 使用调整后的分数进行后续计算
                entry["effective_score"] = adjusted_score
            
            logger.debug(
                "多域选表 measure_factor 预处理完成",
                table_count=len(candidate_tables),
                adjustments=[{
                    "table_id": e["table"].table_id[:12],
                    "original": round(e.get("original_score", 0), 4),
                    "factor": round(e.get("measure_factor", 1), 4),
                    "adjusted": round(e.get("adjusted_score", 0), 4),
                } for e in candidate_tables[:5]]
            )
        else:
            # 未启用时使用原始分数
            for entry in candidate_tables:
                entry["effective_score"] = entry["table"].score or 0.0
                entry["measure_factor"] = 1.0

        # 计算归一化分数和质量分数（使用 effective_score）
        # 【优化】使用软归一化，保留分数差异，避免归一化完全抹平差距
        max_table_score = max(entry["effective_score"] for entry in candidate_tables)
        min_table_score = min(entry["effective_score"] for entry in candidate_tables)
        max_domain_score = max(domain_score_map.values()) if domain_score_map else 0.0
        
        # 【优化】软归一化：保留相对差距，避免最高分直接变成 1.0
        # 使用 score_range 来区分分数，而不是简单的 max 归一化
        score_range = max_table_score - min_table_score
        use_soft_normalize = score_range > 0.01  # 分数差距足够大时使用软归一化

        for entry in candidate_tables:
            table_score_raw = entry["effective_score"]
            
            if use_soft_normalize:
                # 软归一化：(score - min) / range，保留相对差距
                # 再加上一个基准值，避免最低分变成 0
                base_normalized = (table_score_raw - min_table_score) / score_range if score_range > 0 else 0.5
                # 加权：50% 基于相对排名，50% 基于绝对分数
                absolute_normalized = table_score_raw / max_table_score if max_table_score > 0 else 0.0
                normalized_table = 0.5 * base_normalized + 0.5 * absolute_normalized
            else:
                # 分数差距太小，使用原始归一化
                normalized_table = (
                    table_score_raw / max_table_score if max_table_score > 0 else 0.0
                )
            
            normalized_domain = (
                entry["domain_score"] / max_domain_score
                if max_domain_score > 0
                else 0.0
            )

            quality_score = (
                table_weight * normalized_table
                + domain_weight * normalized_domain
            )
            entry["quality_score"] = quality_score
            entry["components"] = {
                "table_score": round(normalized_table, 4),
                "domain_score": round(normalized_domain, 4),
                "raw_effective_score": round(table_score_raw, 4),  # 【新增】记录原始分数
            }
            # 记录调整信息供 trace 使用
            if self._measure_coverage_enabled:
                entry["components"]["original_table_score"] = round(entry.get("original_score", 0), 4)
                entry["components"]["measure_factor"] = round(entry.get("measure_factor", 1), 4)

        # 按质量分数排序
        candidate_tables.sort(key=lambda item: item["quality_score"], reverse=True)

        # ========== V2.3 改进：返回全局TOP表而非单一域的表 ==========
        # 核心修复：将跨域高分表纳入最终候选，避免因域选择错误导致正确表被排除
        # 
        # 原问题：用户问"2024年武汉市东湖高新区已征收土地"
        # - "建设用地批准书"（土地管理审批域）是正确答案，全局排名第2
        # - 但因为"国土变更调查"域的"2024年土地利用现状"分数最高
        # - 旧逻辑只返回国土变更调查域的表，导致正确表被排除
        #
        # 解决方案：
        # 1. 返回全局TOP表（按quality_score排序），不限于单一域
        # 2. 主域由最高分表所属域决定（用于few-shot等）
        # 3. llm_top_k 从全局TOP表中截取
        
        winning_entry = candidate_tables[0]
        selected_domain = winning_entry["domain"]
        selected_domain_id = selected_domain.domain_id
        selected_domain_name = selected_domain.domain_name
        
        # 从全局排序的 candidate_tables 中提取 TableRetrievalResult
        # 限制数量为 top_k_tables（通常是10），保持与单域检索一致
        global_top_tables: List[TableRetrievalResult] = []
        seen_table_ids: set = set()
        
        for entry in candidate_tables:
            if len(global_top_tables) >= top_k_tables:
                break
            table = entry["table"]
            if table.table_id in seen_table_ids:
                continue
            seen_table_ids.add(table.table_id)
            global_top_tables.append(table)
        
        # 使用全局TOP表替代原来的单域表
        selected_tables = global_top_tables
        
        logger.info(
            "全局多域表选择完成（V2.3）",
            primary_domain=selected_domain_id,
            total_global_tables=len(global_top_tables),
            table_domain_distribution={
                entry["domain"].domain_id: 1 
                for entry in candidate_tables[:len(global_top_tables)]
            },
        )

        # 构建排名详情
        ranking_details = [
            {
                "domain_id": entry["domain"].domain_id,
                "domain_name": entry["domain"].domain_name,
                "table_id": entry["table"].table_id,
                "table_score": round(entry["table"].score or 0.0, 4),
                "field_count": entry["field_count"],
                "quality_score": round(entry["quality_score"], 4),
                "components": entry["components"],
                "global_rank": entry["table"].evidence.get("global_rank") if entry["table"].evidence else None,
            }
            for entry in candidate_tables[:15]
        ]

        # 统计全局TOP表的域分布
        global_domain_distribution: Dict[str, int] = {}
        for table in selected_tables:
            domain_id_in_table = table.evidence.get("domain_id") if table.evidence else None
            if domain_id_in_table:
                global_domain_distribution[domain_id_in_table] = \
                    global_domain_distribution.get(domain_id_in_table, 0) + 1
        
        multi_domain_info = {
            "enabled": True,
            "method": "global_unified_v2.3",  # 标识使用全局统一检索V2.3（跨域TOP表）
            "candidate_count": len(domain_candidates),
            "quality_weights": {
                "table_score": table_weight,
                "domain_score": domain_weight,
            },
            "candidate_domains": domain_details,
            "ranking": ranking_details,
            "selected_table": ranking_details[0] if ranking_details else None,
            "global_top_domain_distribution": global_domain_distribution,  # 新增：全局TOP表的域分布
        }

        retrieval_info = {
            # 兼容：对外方法名保持稳定，具体实现细节见 multi_domain.method
            "method": "multi_domain_hybrid",
            "effective_domain_id": selected_domain_id,
            "effective_domain_name": selected_domain_name,
            "found_tables": len(selected_tables),
            "top_score": selected_tables[0].score if selected_tables else 0.0,
            "candidate_domain_ids": candidate_domain_ids,
            "multi_domain": multi_domain_info,
            "cross_domain_tables_included": len(global_domain_distribution) > 1,  # 新增：是否包含跨域表
        }
        self.table_retriever.last_retrieval_info = retrieval_info

        logger.info(
            "全局多域表检索完成（V2.3）",
            primary_domain=selected_domain_id,
            selected_table=multi_domain_info["selected_table"],
            found_tables=len(selected_tables),
            global_top_k=global_top_k,
            cross_domain_included=retrieval_info.get("cross_domain_tables_included", False),
            domain_distribution=global_domain_distribution,
        )

        return {
            "table_results": selected_tables,
            "retrieval_info": retrieval_info,
            "domain_id": selected_domain_id,
            "domain_name": selected_domain_name,
        }

    async def _run_multi_domain_retrieval_for_candidate(
        self,
        candidate: DomainCandidate,
        question: str,
        top_k_tables: int,
        threshold: float,
        query_vector: Optional[List[float]],
        keyword_profile: Optional[KeywordExtractionResult],
    ) -> Dict[str, Any]:
        """对单个候选域执行表检索，禁用跨域兜底，便于独立评估。"""
        clone_retriever = TableRetriever(
            milvus_client=self.table_retriever.milvus_client,
            embedding_client=self.table_retriever.embedding_client or self.embedding_client,
            semantic_model=self.model,
            connection_id=self.connection_id,
            reranker=self.reranker,
        )
        tables = await clone_retriever.retrieve_relevant_tables(
            question=question,
            domain_id=candidate.domain_id,
            connection_id=self.connection_id,
            top_k=top_k_tables,
            threshold=threshold,
            query_vector=query_vector,
            candidate_domain_ids=[],
            keyword_profile=keyword_profile,
            allow_cross_domain_retry=False,
        )
        info_snapshot = getattr(clone_retriever, "last_retrieval_info", {}) or {}
        return {
            "candidate": candidate,
            "tables": tables,
            "info": info_snapshot,
        }

    @staticmethod
    def _extract_domain_candidate_score(candidate: DomainCandidate) -> float:
        """从候选中抽取一个可用于排序的得分。"""
        for attr in ("rrf_score", "dense_score", "sparse_score"):
            value = getattr(candidate, attr, None)
            if value:
                return float(value)
        return 0.0

    def _load_table_structures(
        self,
        table_results: List[TableRetrievalResult]
    ) -> List[TableStructure]:
        """加载表结构，便于Prompt与Few-Shot过滤使用。"""
        table_structures: List[TableStructure] = []
        for table_result in table_results:
            try:
                structure = self.structure_loader.load_table_structure(table_result.table_id)
                table_structures.append(structure)
                logger.debug(
                    "表结构加载成功",
                    table=structure.display_name,
                    fields=structure.total_fields
                )
            except ValueError as exc:
                logger.warning("跳过无效的表", table_id=table_result.table_id, error=str(exc))
                continue

        logger.debug(
            "表结构加载完成",
            tables=len(table_structures),
            total_fields=sum(t.total_fields for t in table_structures)
        )
        return table_structures

    async def _filter_tables_by_permission(
        self,
        table_results: List[TableRetrievalResult],
        user_id: str,
        user_role: str
    ) -> List[TableRetrievalResult]:
        """
        根据用户权限过滤表检索结果
        
        Args:
            table_results: 向量检索的表结果
            user_id: 用户ID
            user_role: 用户角色
        
        Returns:
            过滤后的表结果列表
        """
        if not table_results:
            return table_results
        
        from uuid import UUID
        from server.utils.db_pool import get_metadata_pool
        
        try:
            user_uuid = UUID(user_id)
            pool = await get_metadata_pool()
            
            async with pool.acquire() as conn:
                # 检查用户是否有 scope_type='all' 的角色
                has_all_access = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM user_data_roles udr
                        JOIN data_roles dr ON udr.role_id = dr.role_id
                        WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                        AND dr.is_active = TRUE AND dr.scope_type = 'all'
                    )
                """, user_uuid)
                
                if has_all_access:
                    # 全量访问，不需要过滤
                    return table_results
                
                # 获取用户可访问的表ID列表
                accessible_table_rows = await conn.fetch("""
                    SELECT DISTINCT rtp.table_id
                    FROM user_data_roles udr
                    JOIN data_roles dr ON udr.role_id = dr.role_id
                    JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                    WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                    AND dr.is_active = TRUE AND dr.scope_type = 'limited'
                    AND rtp.can_query = TRUE
                """, user_uuid)
                
                accessible_table_ids = {str(row['table_id']) for row in accessible_table_rows}
                
                if not accessible_table_ids:
                    logger.warning("用户没有任何表的查询权限", user_id=user_id)
                    return []
                
                # 过滤表结果
                filtered_results = [
                    tr for tr in table_results 
                    if tr.table_id in accessible_table_ids
                ]
                
                logger.debug(
                    "权限过滤表结果",
                    before=len(table_results),
                    after=len(filtered_results),
                    accessible_tables=len(accessible_table_ids)
                )
                
                return filtered_results
                
        except Exception as e:
            logger.error("表权限过滤失败，返回空结果", error=str(e))
            # 权限检查失败时，安全起见返回空结果
            return []

    def _extract_keywords(self, question: str) -> List[str]:
        """
        从问题中提取关键词（使用jieba分词）

        Args:
            question: 用户问题

        Returns:
            关键词列表
        """
        import re
        
        try:
            import jieba
        except ImportError:
            logger.warning("jieba未安装，使用简单分词")
            # Fallback到简单分词
            text = re.sub(r'[^\w\s]', ' ', question)
            words = text.split()
        else:
            # 使用jieba分词
            # 去除标点符号但保留数字
            text = re.sub(r'[^\w\s]', '', question)
            words = jieba.lcut(text)
            logger.debug("jieba分词结果", words=words)

        # 过滤停用词（从配置加载）
        from server.utils.text_templates import get_stopwords
        stopwords = set(get_stopwords())
        
        # 过滤条件：
        # 1. 不在停用词中
        # 2. 长度>1 或 是纯数字（保留年份、数量等）
        # 3. 不是纯空格
        keywords = []
        for w in words:
            w = w.strip()
            if w and (len(w) > 1 or w.isdigit()) and w not in stopwords:
                keywords.append(w)

        logger.debug("提取关键词", question=question, keywords=keywords)
        return keywords[:15]  # 增加到15个关键词

    def _serialize_domain_candidates(
        self,
        candidates: List[DomainCandidate],
        source: Optional[str],
    ) -> List[Dict[str, Any]]:
        serialized: List[Dict[str, Any]] = []
        for idx, candidate in enumerate(candidates):
            rank = candidate.rank or (idx + 1)
            serialized.append(
                {
                    "rank": rank,
                    "domain_id": candidate.domain_id,
                    "domain_name": candidate.domain_name,
                    "dense_score": round(candidate.dense_score, 4),
                    "sparse_score": round(candidate.sparse_score, 4),
                    "rrf_score": round(candidate.rrf_score, 4),
                    "selected": idx == 0,
                    "source": source,
                    "source_scores": candidate.source_scores,
                }
            )
        return serialized

    async def _retrieve_with_selected_table(
        self,
        question: str,
        selected_table_id: str,
        user_domain_id: Optional[str] = None,
        global_rules: Optional[List[dict]] = None
    ) -> HierarchicalRetrievalResult:
        """
        使用已选择的表ID直接构建检索结果，跳过表检索步骤
        
        Args:
            question: 用户问题
            selected_table_id: 已选择的表ID
            user_domain_id: 用户指定的业务域
            global_rules: 全局规则列表
        
        Returns:
            HierarchicalRetrievalResult
        """
        # 获取表的数据源信息
        datasource = self.model.datasources.get(selected_table_id)
        if not datasource:
            logger.warning(f"未找到表: {selected_table_id}")
            # 尝试从数据库加载
            from server.utils.db_pool import get_metadata_pool
            from uuid import UUID
            pool = await get_metadata_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT t.table_id, t.display_name, t.domain_id, bd.domain_name
                    FROM db_tables t
                    LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                    WHERE t.table_id = $1
                """, UUID(selected_table_id))
                if row:
                    domain_id = str(row["domain_id"]) if row["domain_id"] else user_domain_id
                    domain_name = row["domain_name"] or self._resolve_domain_name(domain_id)
                else:
                    domain_id = user_domain_id
                    domain_name = self._resolve_domain_name(domain_id)
        else:
            domain_id = str(datasource.domain_id) if datasource.domain_id else user_domain_id
            domain_name = self._resolve_domain_name(domain_id)
        
        # 加载表结构
        table_structure = self.structure_loader.load_table_structure(selected_table_id)
        table_structures = [table_structure] if table_structure else []
        
        if not table_structures:
            logger.warning(f"无法加载表结构: {selected_table_id}")
        
        # 构建 Prompt
        table_score_map = {selected_table_id: TableRetrievalResult(
            table_id=selected_table_id,
            datasource=datasource,
            score=1.0,
            reranker_score=1.0,
            rrf_score=1.0,
            evidence={"source": "llm_selection"}
        )}
        prompt_context = self.prompt_builder.build_context(
            table_structures=table_structures,
            question=question,
            domain_name=domain_name,
            global_rules=global_rules,
            table_scores=table_score_map
        )
        
        # 复用 table_score_map 中的 table_results
        table_results = list(table_score_map.values())
        
        logger.debug(
            "使用已选择表构建检索结果",
            selected_table_id=selected_table_id,
            domain_id=domain_id,
            table_structures=len(table_structures)
        )
        
        return HierarchicalRetrievalResult(
            domain_id=domain_id,
            domain_name=domain_name,
            table_results=table_results,
            table_structures=table_structures,
            prompt_context=prompt_context,
            domain_detection_method="llm_selection",
            table_retrieval_method="llm_selection",
            table_retrieval_info={
                "method": "llm_selection",
                "selected_table_id": selected_table_id
            }
        )

    async def _retrieve_with_selected_tables(
        self,
        question: str,
        selected_table_ids: List[str],
        user_domain_id: Optional[str] = None,
        global_rules: Optional[List[dict]] = None
    ) -> HierarchicalRetrievalResult:
        """
        使用多个已选择的表ID直接构建检索结果（用于跨年查询等多表场景）
        
        Args:
            question: 用户问题
            selected_table_ids: 已选择的多个表ID列表
            user_domain_id: 用户指定的业务域
            global_rules: 全局规则列表
        
        Returns:
            HierarchicalRetrievalResult
        """
        from server.utils.db_pool import get_metadata_pool
        from uuid import UUID
        
        table_structures = []
        table_results = []
        table_score_map = {}
        domain_id = user_domain_id
        domain_name = None
        
        # 批量加载表信息
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 查询所有选中表的基本信息
            table_rows = await conn.fetch("""
                SELECT t.table_id, t.display_name, t.domain_id, t.data_year, bd.domain_name
                FROM db_tables t
                LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                WHERE t.table_id = ANY($1::uuid[])
            """, [UUID(tid) for tid in selected_table_ids])
            
            table_info_map = {str(row["table_id"]): row for row in table_rows}
        
        # 加载每个表的结构
        for idx, table_id in enumerate(selected_table_ids):
            # 获取表信息
            row = table_info_map.get(table_id)
            if row and not domain_id:
                domain_id = str(row["domain_id"]) if row["domain_id"] else None
                domain_name = row["domain_name"]
            
            # 加载表结构（优先从 semantic_model，失败时从数据库加载）
            table_structure = None
            try:
                table_structure = self.structure_loader.load_table_structure(table_id)
            except ValueError:
                # 表不在当前 semantic_model 中（可能是跨域表），从数据库直接加载
                logger.debug(f"表 {table_id} 不在当前语义模型中，尝试从数据库加载")
                table_structure = await self._load_table_structure_from_db(table_id, row)
            
            if table_structure:
                table_structures.append(table_structure)
                
                # 构建 TableRetrievalResult
                datasource = self.model.datasources.get(table_id)
                table_result = TableRetrievalResult(
                    table_id=table_id,
                    datasource=datasource,
                    score=1.0 - idx * 0.01,  # 按顺序略微降分，保持排序
                    reranker_score=1.0,
                    rrf_score=1.0,
                    evidence={"source": "multi_table_selection", "index": idx}
                )
                table_results.append(table_result)
                table_score_map[table_id] = table_result
            else:
                logger.warning(f"无法加载表结构: {table_id}")
        
        if not domain_name:
            domain_name = self._resolve_domain_name(domain_id)
        
        # 构建合并的 Prompt（包含所有表的字段）
        prompt_context = self.prompt_builder.build_context(
            table_structures=table_structures,
            question=question,
            domain_name=domain_name,
            global_rules=global_rules,
            table_scores=table_score_map
        )
        
        logger.debug(
            "使用多表构建检索结果（跨年/多表查询）",
            selected_table_ids=selected_table_ids,
            domain_id=domain_id,
            table_count=len(table_structures)
        )
        
        return HierarchicalRetrievalResult(
            domain_id=domain_id,
            domain_name=domain_name,
            table_results=table_results,
            table_structures=table_structures,
            prompt_context=prompt_context,
            domain_detection_method="multi_table_selection",
            table_retrieval_method="multi_table_selection",
            table_retrieval_info={
                "method": "multi_table_selection",
                "selected_table_ids": selected_table_ids,
                "table_count": len(selected_table_ids)
            }
        )

    async def _load_table_structure_from_db(
        self,
        table_id: str,
        table_info: Optional[dict] = None
    ) -> Optional[TableStructure]:
        """
        从数据库直接加载表结构（用于跨域表的加载）
        
        Args:
            table_id: 表ID
            table_info: 表的基本信息（如果已经查询过）
            
        Returns:
            TableStructure 对象，加载失败返回 None
        """
        from server.utils.db_pool import get_metadata_pool
        from uuid import UUID
        from server.nl2ir.llm_table_selector import FieldInfo
        
        try:
            pool = await get_metadata_pool()
            async with pool.acquire() as conn:
                # 如果没有表基本信息，先查询
                if not table_info:
                    table_info = await conn.fetchrow("""
                        SELECT t.table_id, t.display_name, t.table_name, t.schema_name,
                               t.physical_table_name, t.description, t.domain_id, 
                               t.data_year, t.tags, bd.domain_name
                        FROM db_tables t
                        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                        WHERE t.table_id = $1
                    """, UUID(table_id))
                    
                    if not table_info:
                        logger.warning(f"表不存在: {table_id}")
                        return None
                
                # 查询表的完整信息
                table_row = await conn.fetchrow("""
                    SELECT t.table_id, t.display_name, t.table_name, t.schema_name,
                           t.physical_table_name, t.description, t.domain_id, 
                           t.data_year, t.tags, bd.domain_name
                    FROM db_tables t
                    LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                    WHERE t.table_id = $1
                """, UUID(table_id))
                
                if not table_row:
                    return None
                
                # 查询字段信息（只获取 is_active=true 的字段）
                # 注意：fields 表通过 source_column_id 关联 db_columns，需要 JOIN 获取 column_name
                field_rows = await conn.fetch("""
                    SELECT f.field_id, f.display_name, dc.column_name, dc.data_type, 
                           f.field_type, f.unit, f.description, f.synonyms
                    FROM fields f
                    JOIN db_columns dc ON f.source_column_id = dc.column_id
                    WHERE dc.table_id = $1 AND f.is_active = true
                    ORDER BY f.field_type, f.display_name
                """, UUID(table_id))
                
                # 按字段类型分类
                dimensions = []
                measures = []
                identifiers = []
                timestamps = []
                geometries = []
                
                for field in field_rows:
                    field_info = FieldInfo(
                        field_id=str(field["field_id"]),
                        display_name=field["display_name"],
                        column_name=field["column_name"],
                        data_type=field["data_type"],
                        unit=field["unit"],
                        description=field["description"],
                        synonyms=field["synonyms"] or [],
                        enum_values=None  # 枚举值暂不加载
                    )
                    
                    field_type = field["field_type"]
                    if field_type == "dimension":
                        dimensions.append(field_info)
                    elif field_type == "measure":
                        measures.append(field_info)
                    elif field_type == "identifier":
                        identifiers.append(field_info)
                    elif field_type == "timestamp":
                        timestamps.append(field_info)
                    elif field_type == "geometry":
                        geometries.append(field_info)
                    else:
                        dimensions.append(field_info)  # 默认为维度
                
                # 构建 TableStructure
                structure = TableStructure(
                    table_id=table_id,
                    table_name=table_row["table_name"],
                    display_name=table_row["display_name"],
                    description=table_row["description"] or "",
                    domain_id=str(table_row["domain_id"]) if table_row["domain_id"] else None,
                    domain_name=table_row["domain_name"],
                    schema_name=table_row["schema_name"],
                    physical_table_name=table_row["physical_table_name"],
                    dimensions=dimensions,
                    measures=measures,
                    identifiers=identifiers,
                    timestamps=timestamps,
                    geometries=geometries,
                    tags=table_row["tags"] or [],
                    aliases=table_row["tags"] or [],
                    data_year=str(table_row["data_year"]) if table_row["data_year"] else None
                )
                
                logger.debug(
                    "从数据库加载表结构成功",
                    table_id=table_id,
                    display_name=table_row["display_name"],
                    domain_name=table_row["domain_name"],
                    field_count=len(dimensions) + len(measures) + len(identifiers)
                )
                
                return structure
                
        except Exception as e:
            logger.exception(f"从数据库加载表结构失败: {table_id}", error=str(e))
            return None

    def _resolve_domain_name(self, domain_id: Optional[str]) -> Optional[str]:
        if not domain_id:
            return None
        if self.domain_detector:
            return self.domain_detector.get_domain_name(domain_id)
        return None

    def get_table_structure(self, table_id: str) -> TableStructure:
        """
        获取指定表的结构

        Args:
            table_id: 表ID

        Returns:
            TableStructure对象
        """
        return self.structure_loader.load_table_structure(table_id)

    def apply_llm_top_k_truncation(
        self,
        result: HierarchicalRetrievalResult,
        question: str,
        global_rules: Optional[List[dict]] = None,
        enum_matches: Optional[List[Any]] = None,
    ) -> HierarchicalRetrievalResult:
        """
        应用 llm_top_k 截断逻辑（无论是否有枚举匹配都会执行）
        
        这是二阶段截断：保留完整的 table_results（供后续fallback使用），
        但对 table_structures 和 prompt_context 截断到 llm_top_k 指定的数量。
        
        设计说明：
        - table_results: 保持完整（用于 fallback_candidate_count 构建确认卡）
        - table_structures: 截断到 llm_top_k（用于传统 LLM 路径）
        - prompt_context: 只包含前 llm_top_k 个表（传递给 LLM）
        
        向量表选择路径需要在 routes.py 中自行处理 llm_top_k 截断。
        
        Args:
            result: 检索结果
            question: 用户问题
            global_rules: 全局规则
            enum_matches: 枚举匹配结果
            
        Returns:
            更新后的检索结果（table_structures 和 prompt_context 已截断）
        """
        if not result.table_results:
            return result
        
        # 多表选择场景（跨年/跨分区/multi_join）不进行截断
        # 这些场景下，用户已经明确选择了多个表，必须保留所有表的结构信息
        if result.table_retrieval_method == "multi_table_selection":
            logger.debug(
                "多表选择场景，跳过 llm_top_k 截断",
                table_count=len(result.table_results),
                structure_count=len(result.table_structures) if result.table_structures else 0
            )
            return result
        
        # 获取 llm_top_k 配置
        llm_top_k = get_retrieval_param("table_retrieval.llm_top_k", None)
        
        if llm_top_k is None:
            # 如果未配置，不进行截断
            logger.debug("llm_top_k 未配置，不进行截断")
            return result
        
        # 记录原始数量用于日志
        original_table_count = len(result.table_results)
        original_structure_count = len(result.table_structures) if result.table_structures else 0
        
        # 如果表数量已经小于等于 llm_top_k，无需截断
        if original_table_count <= llm_top_k:
            logger.debug(
                "表数量已满足 llm_top_k 限制",
                table_count=original_table_count,
                llm_top_k=llm_top_k
            )
            return result
        
        # 计算截断后的表ID集合（用于 prompt_context 和 table_structures）
        llm_table_results = result.table_results[:llm_top_k]
        llm_table_ids = {t.table_id for t in llm_table_results}
        
        # 截断 table_structures（仅保留 llm_top_k 个表）
        if result.table_structures:
            truncated_structures = [s for s in result.table_structures if s.table_id in llm_table_ids]
            result.table_structures = truncated_structures
        else:
            truncated_structures = []
        
        # 重新构建 Prompt（使用截断后的表集合）
        if truncated_structures:
            table_score_map = {r.table_id: r for r in llm_table_results}
            result.prompt_context = self.prompt_builder.build_context(
                table_structures=truncated_structures,
                question=question,
                domain_name=result.domain_name,
                global_rules=global_rules,
                few_shot_examples=result.few_shot_examples,
                table_scores=table_score_map,
                enum_matches=enum_matches,
            )
        
        # 统计 prompt_context 中的表数量用于验证
        prompt_table_count = 0
        if result.prompt_context:
            table_pattern = r"### 表\d+:"
            prompt_table_count = len(re.findall(table_pattern, result.prompt_context))
        
        logger.info(
            "llm_top_k 截断完成",
            original_table_count=original_table_count,
            original_structure_count=original_structure_count,
            llm_structure_count=len(truncated_structures),
            prompt_context_table_count=prompt_table_count,
            llm_top_k=llm_top_k,
            table_results_kept=original_table_count,  # table_results 保持完整
            llm_table_ids=[t.table_id[:8] for t in llm_table_results],
        )
        
        return result

    def format_prompt_for_llm(
        self,
        result: HierarchicalRetrievalResult,
        question: str
    ) -> str:
        """
        为LLM格式化最终的Prompt

        Args:
            result: 检索结果
            question: 用户问题

        Returns:
            格式化的Prompt文本
        """
        # 调试：统计 prompt_context 中的表数量
        import re
        table_pattern = r"### 表\d+:"
        prompt_table_count = len(re.findall(table_pattern, result.prompt_context))
        logger.debug(
            "format_prompt_for_llm 调用",
            prompt_context_table_count=prompt_table_count,
            prompt_context_len=len(result.prompt_context),
        )
        section_label = str(get_retrieval_param("llm_prompt.question_section.label", "IR") or "IR").strip() or "IR"
        
        return (
            f"{result.prompt_context}\n\n"
            "---\n\n"
            f"## 用户问题\n{question}"
        )

    async def apply_enum_boost_and_rerank(
        self,
        result: HierarchicalRetrievalResult,
        enum_matches: List[Any],
        question: str,
        global_rules: Optional[List[dict]] = None,
    ) -> HierarchicalRetrievalResult:
        """
        应用表打分并重排表（V3 优先，V2 兼容）
        
        V3 评分公式（表检索 V3 综合评分）：
        S_final = (S_base * f_measure) + w_domain * B(d_user, d_table) + S_enum + S_tag (+ S_rescue)
        
        其中：
        - S_base: 基础检索分数（Dense + Sparse RRF 融合 + 可选 Reranker）
        - f_measure: 度量覆盖率因子（0.3~1.0，降权不淘汰）
        - B(d_user, d_table): 业务域匹配增益（0/1）
        - w_domain: 业务域权重（随域识别置信度自适应降权）
        - S_enum: 枚举门控加成（_calculate_gated_enum_boost）
        - S_tag: 语义标签匹配加成
        - S_rescue: 表救援修正项（触发时给高置信加成进入精排）
        
        Args:
            result: 原始检索结果
            enum_matches: 枚举值匹配结果
            question: 用户问题
            global_rules: 全局规则
            
        Returns:
            更新后的检索结果
        """
        if not result.table_results:
            return result

        # ========== Ranker 版本选择（单一权威：table_scoring.ranker_version）==========
        cfg_version = str(get_retrieval_param("table_scoring.ranker_version", "") or "").strip().lower()
        if cfg_version in {"v2", "v3", "v4"}:
            ranker_version = cfg_version
        else:
            ranker_version = "v2"
            if cfg_version:
                logger.warning("无效的 table_scoring.ranker_version，回退到 v2", ranker_version=cfg_version)
            else:
                logger.warning("未设置 table_scoring.ranker_version，回退到 v2")

        # 若仍保留 v3_ranker/v4_ranker.enabled（历史配置），仅用于诊断提示，不参与版本选择
        cfg_v3_enabled = bool(get_retrieval_param("table_scoring.v3_ranker.enabled", False))
        cfg_v4_enabled = bool(get_retrieval_param("table_scoring.v4_ranker.enabled", False))
        if (ranker_version == "v3" and not cfg_v3_enabled) or (ranker_version == "v4" and not cfg_v4_enabled):
            logger.warning(
                "ranker_version 与 enabled 开关不一致（以 ranker_version 为准）",
                ranker_version=ranker_version,
                v3_enabled=cfg_v3_enabled,
                v4_enabled=cfg_v4_enabled,
            )

        use_v4 = ranker_version == "v4"
        use_v3 = ranker_version == "v3"

        # ========== 0. 业务域权重（随置信度自适应降权）==========
        primary_domain_id = getattr(result, "domain_id", None)
        detection_method = getattr(result, "domain_detection_method", None) or ""
        domain_cfg_enabled = bool(get_retrieval_param("table_scoring.domain_match.enabled", True))
        base_domain_weight = float(get_retrieval_param("table_scoring.domain_match.weight", 0.2) or 0.2)
        low_conf_th = float(get_retrieval_param("table_scoring.domain_match.low_confidence_threshold", 0.35) or 0.35)
        score_gap_ref = float(get_retrieval_param("table_scoring.domain_match.score_gap_ref", 0.1) or 0.1)
        low_extra_downscale = float(get_retrieval_param("table_scoring.domain_match.low_confidence_extra_downscale", 0.5) or 0.5)
        keyword_method_downscale = float(get_retrieval_param("table_scoring.domain_match.keyword_method_downscale", 0.7) or 0.7)
        weak_method_downscale = float(get_retrieval_param("table_scoring.domain_match.weak_method_downscale", 0.5) or 0.5)

        def _clamp01(x: float) -> float:
            try:
                return max(0.0, min(1.0, float(x)))
            except Exception:
                return 0.0

        def _estimate_domain_confidence() -> float:
            if not domain_cfg_enabled or not primary_domain_id:
                return 0.0
            if detection_method == "user_specified":
                return 1.0
            candidates = getattr(result, "domain_candidates", None) or []
            if not isinstance(candidates, list) or not candidates:
                return 0.0
            # 取 top1 / top2 的 rrf_score（优先）或 dense_score
            def _score(item: Dict[str, Any]) -> float:
                if not isinstance(item, dict):
                    return 0.0
                for k in ("rrf_score", "dense_score"):
                    v = item.get(k)
                    if v is not None:
                        try:
                            return float(v)
                        except Exception:
                            continue
                return 0.0
            top = _score(candidates[0])
            second = _score(candidates[1]) if len(candidates) > 1 else 0.0
            gap = max(0.0, top - second)
            # gap 越大，置信度越高；score_gap_ref 用于尺度归一
            gap_factor = 0.5 + min(0.5, gap / max(score_gap_ref, 1e-6))
            return _clamp01(top) * _clamp01(gap_factor)

        domain_confidence = _estimate_domain_confidence()
        domain_weight = 0.0
        if domain_cfg_enabled and primary_domain_id:
            scale = _clamp01(domain_confidence)
            if detection_method == "keyword":
                scale *= keyword_method_downscale
            elif detection_method in {"vector_failed", "vector_exception", "unavailable", "vector_disabled"}:
                scale *= weak_method_downscale
            if domain_confidence < low_conf_th:
                scale *= low_extra_downscale
            domain_weight = base_domain_weight * _clamp01(scale)
        
        # ========== 2.1 表救援机制（保留）==========
        rescued_tables = self._rescue_tables_by_enum(
            result.table_results,
            enum_matches,
            result.domain_id
        )
        
        # 合并救援的表
        if rescued_tables:
            existing_ids = {t.table_id for t in result.table_results}
            for rescued in rescued_tables:
                if rescued.table_id not in existing_ids:
                    result.table_results.append(rescued)
                    logger.debug(
                        "表救援成功",
                        table_id=rescued.table_id,
                        rescue_score=rescued.score
                    )
            # 触发后赋予“高置信基础分”进入精排池（不硬插入排序，由最终公式决定）
            try:
                top_base_score_snapshot = max((t.score or 0.0) for t in result.table_results)
                for tr in result.table_results:
                    if getattr(tr, "rescue_reason", None):
                        tr.score = max(tr.score or 0.0, top_base_score_snapshot)
            except Exception:
                pass
        
        # ========== 2.2 预加载表的度量字段 ==========
        # 优先从 table_structures 获取（已加载完整字段信息）
        table_measures_cache = self._load_table_measures_from_structures(result.table_structures)
        
        # 合并所有候选表的度量字段（用于动态构建关键词集合）
        all_measures = []
        for measures in table_measures_cache.values():
            all_measures.extend(measures)

        # ========== 2.2.1 抽取度量意图（统一归一化 + 最长子串回退 + 度量族映射）==========
        measure_intent = None
        measure_tokens: List[str] = []
        try:
            from server.nl2ir.tokenizer import Tokenizer
            measure_tokens = Tokenizer.get_instance().cut(question)
        except Exception:
            measure_tokens = []

        try:
            from server.nl2ir.measure_intent import extract_measure_intent

            measure_intent = extract_measure_intent(
                question=question,
                tokens=measure_tokens,
                all_measures=all_measures,
                keywords=RetrievalConfig.measure_coverage_keywords(),
                universal_keywords=RetrievalConfig.measure_universal_keywords(),
                compound_keywords=RetrievalConfig.measure_compound_keywords(),
                measure_families=RetrievalConfig.measure_families(),
                generic_terms=RetrievalConfig.measure_extraction_generic_terms(),
                suffix_keywords=RetrievalConfig.measure_extraction_suffix_keywords(),
                normalization_cfg=RetrievalConfig.measure_extraction_normalization(),
                min_phrase_len=RetrievalConfig.measure_extraction_min_phrase_len(),
                aggregation_hints=RetrievalConfig.aggregation_hints(),
                unit_hints=RetrievalConfig.measure_extraction_unit_hints(),
                unit_require_number=RetrievalConfig.measure_extraction_unit_require_number(),
            )
        except Exception as exc:
            logger.debug("度量意图抽取失败（忽略）", error=str(exc))
            measure_intent = None

        has_specific_measure = bool(getattr(measure_intent, "required_concepts", []) or [])
        is_universal_only = bool(getattr(measure_intent, "is_universal_only", False))
        apply_measure_signals = bool(has_specific_measure and not is_universal_only)

        # 组合“度量检索 query”（用于 Milvus field sparse / 可选 dense）
        measure_query = ""
        if measure_intent is not None:
            parts: List[str] = []
            parts.extend(list(getattr(measure_intent, "required_concepts", []) or []))
            parts.extend(list(getattr(measure_intent, "matched_phrases", []) or []))
            # 去重保持顺序
            dedup = []
            for p in parts:
                if p and p not in dedup:
                    dedup.append(p)
            if dedup:
                measure_query = " ".join(dedup) + " " + question

        # ========== 2.2.2 生成表级度量信号（PG / Milvus field）==========
        measure_pg_norm: Dict[str, float] = {}
        measure_pg_debug: Dict[str, Any] = {"executed": False, "skipped_reason": None}
        measure_milvus_norm: Dict[str, float] = {}
        measure_milvus_debug: Dict[str, Any] = {"executed": False, "skipped_reason": None}

        table_ids_for_scoring = [t.table_id for t in result.table_results if getattr(t, "table_id", None)]

        # PG：启发式字段打分（仅在明确度量且非通用统计意图时启用）
        if (
            apply_measure_signals
            and self._measure_pg_enabled
            and self._measure_pg_apply_when_measure_intent
            and self.db_pool
            and table_ids_for_scoring
        ):
            try:
                measure_pg_debug["executed"] = True
                if self._measure_retriever is None:
                    from server.nl2ir.measure_retriever import MeasureRetriever

                    self._measure_retriever = MeasureRetriever(
                        db_pool=self.db_pool,
                        aggregation_hints=RetrievalConfig.aggregation_hints(),
                        measure_keywords=RetrievalConfig.measure_retrieval_keywords(),
                    )

                per_table = max(1, int(self._measure_pg_top_k_fields or 1))
                top_k_total = max(20, per_table * max(1, len(table_ids_for_scoring)))
                matches = await self._measure_retriever.retrieve(
                    question=question,
                    table_ids=table_ids_for_scoring,
                    connection_id=self.connection_id,
                    top_k=top_k_total,
                )

                table_raw: Dict[str, List[float]] = {}
                table_top: Dict[str, Any] = {}
                for m in matches or []:
                    tid = getattr(m, "table_id", None)
                    if not tid:
                        continue
                    score = float(getattr(m, "score", 0.0) or 0.0)
                    table_raw.setdefault(tid, []).append(score)
                    prev = table_top.get(tid)
                    if prev is None or score > float(prev.get("score", 0.0)):
                        table_top[tid] = {
                            "field_id": getattr(m, "field_id", None),
                            "display_name": getattr(m, "display_name", None),
                            "score": score,
                            "match_type": getattr(m, "match_type", None),
                            "aggregation_type": getattr(m, "aggregation_type", None),
                            "evidence": getattr(m, "evidence", None),
                        }

                table_score_raw: Dict[str, float] = {}
                agg_mode = (self._measure_pg_table_agg or "max").strip().lower()
                for tid, scores in table_raw.items():
                    if not scores:
                        continue
                    sorted_scores = sorted(scores, reverse=True)
                    if agg_mode == "sum_top2":
                        table_score_raw[tid] = float(sum(sorted_scores[:2]))
                    else:
                        table_score_raw[tid] = float(sorted_scores[0])

                max_raw = max(table_score_raw.values(), default=0.0) or 0.0
                if max_raw > 0:
                    for tid, raw in table_score_raw.items():
                        measure_pg_norm[tid] = float(raw / max_raw)
                measure_pg_debug = {
                    "executed": True,
                    "top_hit": table_top,
                    "agg_mode": agg_mode,
                    "max_raw": max_raw,
                }
            except Exception as exc:
                measure_pg_debug["executed"] = True
                measure_pg_debug["error"] = str(exc)
                logger.debug("PG度量信号计算失败（忽略）", error=str(exc))
        else:
            if not apply_measure_signals:
                measure_pg_debug["skipped_reason"] = "no_measure_intent"
            elif not self._measure_pg_enabled:
                measure_pg_debug["skipped_reason"] = "disabled"
            elif not self._measure_pg_apply_when_measure_intent:
                measure_pg_debug["skipped_reason"] = "apply_when_measure_intent=false"
            elif not self.db_pool:
                measure_pg_debug["skipped_reason"] = "no_db_pool"
            elif not table_ids_for_scoring:
                measure_pg_debug["skipped_reason"] = "no_candidate_tables"

        # Milvus：field 级混合检索（仅在明确度量且非通用统计意图时启用）
        if (
            apply_measure_signals
            and self._measure_field_milvus_enabled
            and self._measure_field_milvus_apply_when_measure_intent
            and self.milvus_client
            and self.embedding_client
            and table_ids_for_scoring
        ):
            try:
                measure_milvus_debug["executed"] = True
                if self._measure_field_retriever is None:
                    from server.nl2ir.measure_field_milvus_retriever import MeasureFieldMilvusRetriever

                    self._measure_field_retriever = MeasureFieldMilvusRetriever(
                        milvus_client=self.milvus_client,
                        embedding_client=self.embedding_client,
                        collection_name=settings.milvus_collection,
                        connection_id=self.connection_id,
                    )

                from server.nl2ir.measure_field_milvus_retriever import aggregate_table_scores

                hits = await self._measure_field_retriever.retrieve(
                    question=question,
                    measure_query=measure_query or question,
                    query_vector=getattr(result, "question_vector", None),
                    top_k_fields=int(self._measure_field_milvus_top_k_fields or 50),
                    min_field_score=float(self._measure_field_milvus_min_field_score or 0.0),
                    min_field_score_ratio=float(self._measure_field_milvus_min_field_score_ratio or 0.0),
                    use_measure_query_vector=bool(self._measure_field_milvus_use_measure_query_vector),
                )
                table_score_raw, debug = aggregate_table_scores(hits)
                max_raw = max(table_score_raw.values(), default=0.0) or 0.0
                if max_raw > 0:
                    for tid, raw in table_score_raw.items():
                        measure_milvus_norm[tid] = float(raw / max_raw)
                measure_milvus_debug = {"executed": True, **(debug or {}), "max_raw": max_raw}
            except Exception as exc:
                measure_milvus_debug["executed"] = True
                measure_milvus_debug["error"] = str(exc)
                logger.debug("Milvus度量信号计算失败（忽略）", error=str(exc))
        else:
            if not apply_measure_signals:
                measure_milvus_debug["skipped_reason"] = "no_measure_intent"
            elif not self._measure_field_milvus_enabled:
                measure_milvus_debug["skipped_reason"] = "disabled"
            elif not self._measure_field_milvus_apply_when_measure_intent:
                measure_milvus_debug["skipped_reason"] = "apply_when_measure_intent=false"
            elif not self.milvus_client:
                measure_milvus_debug["skipped_reason"] = "no_milvus_client"
            elif not self.embedding_client:
                measure_milvus_debug["skipped_reason"] = "no_embedding_client"
            elif not table_ids_for_scoring:
                measure_milvus_debug["skipped_reason"] = "no_candidate_tables"

        # ========== 2.2.3 度量救援（基于 Milvus 字段强命中）==========
        if apply_measure_signals and self._measure_rescue_enabled and measure_milvus_norm:
            rescued_by_measure = self._rescue_tables_by_measure_milvus(
                table_results=result.table_results,
                measure_table_scores=measure_milvus_norm,
                threshold=float(self._measure_rescue_threshold or 0.85),
            )
            if rescued_by_measure:
                existing_ids = {t.table_id for t in result.table_results}
                for rescued in rescued_by_measure:
                    if rescued.table_id not in existing_ids:
                        result.table_results.append(rescued)
                        logger.debug(
                            "度量救援成功",
                            table_id=rescued.table_id,
                            rescue_score=rescued.score,
                        )
                # 触发后赋予“高置信基础分”进入精排池（不硬插入排序，由最终公式决定）
                try:
                    top_base_score_snapshot = max((t.score or 0.0) for t in result.table_results)
                    for tr in result.table_results:
                        if getattr(tr, "rescue_reason", None):
                            tr.score = max(tr.score or 0.0, top_base_score_snapshot)
                except Exception:
                    pass

        # 补齐 rescued 表的度量字段缓存（避免后续 measure_factor 计算缺失）
        if result.table_results:
            missing = [t for t in result.table_results if t.table_id not in table_measures_cache]
            if missing:
                try:
                    extra_cache = self._load_table_measures_cache(missing)
                    table_measures_cache.update(extra_cache or {})
                    for measures in (extra_cache or {}).values():
                        all_measures.extend(measures or [])
                except Exception:
                    pass
        
        # ========== 2.3 应用 V2 评分公式 ==========
        scoring_details = {}
        # V3：在改写 table_result.score 之前，先保存一次 base 分数的 top 值
        top_base_score_for_v3 = max((t.score or 0.0) for t in result.table_results) if result.table_results else 0.0
        
        for table_result in result.table_results:
            table_id = table_result.table_id
            base_score = table_result.score or 0.0
            
            # 1. 度量覆盖率因子（乘法）
            measure_factor, measure_detail = self._calculate_measure_coverage_factor(
                question, table_id, table_measures_cache.get(table_id, []), all_measures, measure_intent
            )
            measure_factor, measure_gate = self._apply_measure_factor_strong_gate(
                table_id=table_id,
                measure_factor=measure_factor,
                measure_detail=measure_detail,
                apply_measure_signals=bool(apply_measure_signals),
                measure_pg_norm=measure_pg_norm,
                measure_milvus_norm=measure_milvus_norm,
            )
            
            # 2. 枚举门控加成（加法）- 受全局开关控制
            # LLM 选表模式下，跳过复杂门控，直接按相似度放行枚举值（让 LLM 自己判断）
            # 包括单表选择（llm_selection）和多表选择（multi_table_selection）
            is_llm_selection_mode = result.table_retrieval_method in ("llm_selection", "multi_table_selection")
            
            if self._enum_boost_enabled:
                if is_llm_selection_mode:
                    # LLM 选表模式：简化处理，直接按相似度取主表的枚举值
                    gated_enum_boost, enum_detail = self._pass_enum_for_llm_mode(
                        enum_matches, table_id, question
                    )
                else:
                    # 向量检索模式：使用严格的门控逻辑
                    gated_enum_boost, enum_detail = self._calculate_gated_enum_boost(
                        enum_matches, table_id, question
                    )
            else:
                # 枚举加成已禁用，跳过计算
                gated_enum_boost = 0.0
                enum_detail = {
                    "passed": 0,
                    "blocked": 0,
                    "passed_enums": [],
                    "blocked_enums": [],
                    "total_boost": 0.0,
                    "disabled": True,
                    "precision_stats": {}
                }
            
            # 3. 语义标签匹配加成（V2.1 新增）
            # 从 table_structures 获取表的 tags
            table_tags = []
            for struct in result.table_structures:
                if struct.table_id == table_id:
                    table_tags = struct.tags or []
                    break
            if not table_tags:
                try:
                    if getattr(table_result, "datasource", None) and hasattr(table_result.datasource, "tags"):
                        table_tags = table_result.datasource.tags or []
                except Exception:
                    pass
            
            tag_match_boost, tag_detail = self._calculate_tag_match_boost(
                question, table_tags, table_id
            )

            # 3.5 业务域匹配增益（V3 新增，后置排序算子）
            table_domain_id = None
            try:
                if getattr(table_result, "evidence", None):
                    table_domain_id = (table_result.evidence or {}).get("domain_id")
                if not table_domain_id and getattr(table_result, "datasource", None):
                    table_domain_id = getattr(table_result.datasource, "domain_id", None)
            except Exception:
                table_domain_id = None
            is_domain_match = 1.0 if (primary_domain_id and table_domain_id and str(table_domain_id) == str(primary_domain_id)) else 0.0

            # 3.6 表救援修正项（V3：作为分数修正项，而非硬插入）
            rescue_boost = float(get_retrieval_param("table_scoring.table_rescue.boost", 0.15) or 0.15) if getattr(table_result, "rescue_reason", None) else 0.0
            
            # 4. 最终公式：
            # - V3（启用）：(S_base * f_measure) + w_domain * is_domain_match + S_enum + S_tag (+ S_rescue)
            # - V2（默认）：S_base * f_measure + S_enum + S_tag
            if use_v4:
                from server.nl2ir.table_ranker_v4 import TableRankerV4

                # V4：先算“基础惩罚分”与“全局封顶后的boost”，再由 anti-flip/low-confidence 在批量阶段处理
                # 注意：measure_*_boost 在此处暂不叠加，按 V4 将其作为 B_raw 的组成部分放入 ranker 内
                v4_components = TableRankerV4.compute(
                    base_score=base_score,
                    top_base_score=top_base_score_for_v3,
                    measure_factor=measure_factor,
                    domain_bonus=float(domain_weight) * float(is_domain_match or 0.0),
                    enum_boost=gated_enum_boost,
                    tag_boost=tag_match_boost,
                    measure_pg_boost=0.0,
                    measure_milvus_boost=0.0,
                    rescue_boost=rescue_boost,
                    evidence=getattr(table_result, "evidence", None),
                )
                final_score = float(v4_components.s_final)
                # 在 scoring_details 中补齐 V4 中间项，便于诊断与 anti-flip/low-confidence 使用
                scoring_details.setdefault(table_id, {})
                scoring_details[table_id].update(
                    {
                        "v4_enabled": True,
                        "v4_stable_base": round(v4_components.stable_base, 6),
                        "v4_s_penalty": round(v4_components.s_penalty, 6),
                        "v4_gate": round(v4_components.gate, 6),
                        "v4_b_raw_pre_measure_boost": round(v4_components.b_raw, 6),
                        "v4_b_clamped_pre_measure_boost": round(v4_components.b_clamped, 6),
                        "v4_lambda": round(v4_components.lambda_cap, 6),
                        "v4_year_multiplier": round(v4_components.year_multiplier, 6),
                        "v4_structure_multiplier": round(v4_components.structure_multiplier, 6),
                    }
                )
            elif use_v3:
                from server.nl2ir.table_ranker_v3 import rank_score_v3

                final_score = rank_score_v3(
                    question=question,
                    base_score=base_score,
                    top_base_score=top_base_score_for_v3,
                    measure_factor=measure_factor,
                    measure_intent=apply_measure_signals,
                    domain_is_match=bool(is_domain_match),
                    domain_weight=float(domain_weight),
                    gated_enum_boost=gated_enum_boost,
                    tag_match_boost=tag_match_boost,
                    rescue_boost=rescue_boost,
                    evidence=getattr(table_result, "evidence", None),
                )
            else:
                # V2（默认）：保持现有行为
                final_score = base_score * measure_factor + gated_enum_boost + tag_match_boost

            # 4.1 度量表级信号加分（PG / Milvus field）
            measure_pg_boost = 0.0
            if measure_pg_norm and table_id in measure_pg_norm:
                measure_pg_boost = float(self._measure_pg_weight or 0.0) * float(measure_pg_norm.get(table_id) or 0.0)

            measure_milvus_boost = 0.0
            if measure_milvus_norm and table_id in measure_milvus_norm:
                measure_milvus_boost = float(self._measure_field_milvus_weight or 0.0) * float(measure_milvus_norm.get(table_id) or 0.0)

            # V4：measure_* 作为 B_raw 的组成部分，需要再次通过全局 cap 与 gate
            if use_v4:
                from server.nl2ir.table_ranker_v4 import TableRankerV4

                v4_components_with_measure = TableRankerV4.compute(
                    base_score=base_score,
                    top_base_score=top_base_score_for_v3,
                    measure_factor=measure_factor,
                    domain_bonus=float(domain_weight) * float(is_domain_match or 0.0),
                    enum_boost=gated_enum_boost,
                    tag_boost=tag_match_boost,
                    measure_pg_boost=measure_pg_boost,
                    measure_milvus_boost=measure_milvus_boost,
                    rescue_boost=rescue_boost,
                    evidence={
                        **(getattr(table_result, "evidence", None) or {}),
                        # 用于可选结构惩罚
                        "field_count": getattr(table_result, "field_count", None),
                    },
                )
                final_score = float(v4_components_with_measure.s_final)
                scoring_details.setdefault(table_id, {})
                scoring_details[table_id].update(
                    {
                        "measure_pg_boost": round(measure_pg_boost, 4),
                        "measure_milvus_boost": round(measure_milvus_boost, 4),
                        # 供 low-confidence / anti-flip 使用的关键中间项
                        "v4_stable_base": round(v4_components_with_measure.stable_base, 6),
                        "v4_s_penalty": round(v4_components_with_measure.s_penalty, 6),
                        "v4_gate": round(v4_components_with_measure.gate, 6),
                        "v4_lambda_cap": round(v4_components_with_measure.lambda_cap, 6),
                        "v4_b_raw": round(v4_components_with_measure.b_raw, 6),
                        "v4_b_clamped": round(v4_components_with_measure.b_clamped, 6),
                        "v4_s_final": round(v4_components_with_measure.s_final, 6),
                    }
                )
            else:
                final_score = float(final_score) + float(measure_pg_boost) + float(measure_milvus_boost)
            
            # 5. 更新结果
            table_result.score = final_score
            
            # 6. 记录评分详情（用于 trace）
            existing_detail = scoring_details.get(table_id, {}) or {}
            existing_detail.update({
                "base_score": round(base_score, 4),
                "measure_factor": round(measure_factor, 4),
                "measure_intent": bool(apply_measure_signals),
                "measure_gate_passed": bool((measure_gate or {}).get("passed")) if "measure_gate" in locals() else None,
                "measure_gate_reason": (measure_gate or {}).get("reason") if "measure_gate" in locals() else None,
                "measure_pg_boost": round(measure_pg_boost, 4),
                "measure_milvus_boost": round(measure_milvus_boost, 4),
                "domain_weight": round(domain_weight, 4),
                "is_domain_match": int(is_domain_match),
                "gated_enum_boost": round(gated_enum_boost, 4),
                "tag_match_boost": round(tag_match_boost, 4),
                "rescue_boost": round(rescue_boost, 4),
                "final_score": round(final_score, 4),
                "measure_detail": measure_detail,
                "measure_pg": measure_pg_debug,
                "measure_milvus": measure_milvus_debug,
                "enum_detail": enum_detail,
                "tag_detail": tag_detail
            })
            scoring_details[table_id] = existing_detail
            
            # 兼容旧的 enum_boost_trace 属性
            if not hasattr(table_result, 'enum_boost_trace'):
                table_result.enum_boost_trace = {}
            table_result.enum_boost_trace = scoring_details[table_id]
        
        # 按新分数降序排序
        result.table_results.sort(key=lambda t: t.score or 0.0, reverse=True)

        # ========== V4：Low-confidence Mode（全体低语义降级）==========
        if use_v4:
            low_cfg = get_retrieval_param("table_scoring.v4_ranker.low_confidence", {}) or {}
            low_enabled = bool(low_cfg.get("enabled", True))
            tau_low_fallback = float(low_cfg.get("tau_low_fallback", 0.2) or 0.2)
            strategy = str(low_cfg.get("rerank_strategy", "sparse_only") or "sparse_only").strip()

            # max(S_penalty) 来自 scoring_details[v4_s_penalty]；缺失则回退到 base_score
            max_penalty = 0.0
            for tr in result.table_results:
                try:
                    d = scoring_details.get(tr.table_id, {}) or {}
                    s_pen = float(d.get("v4_s_penalty", d.get("base_score", 0.0)) or 0.0)
                    max_penalty = max(max_penalty, s_pen)
                except Exception:
                    continue

            if low_enabled and max_penalty < tau_low_fallback:
                # 进入保守模式：禁用所有 boost，按 sparse 主导重排（不改变召回池）
                for tr in result.table_results:
                    d = scoring_details.get(tr.table_id, {}) or {}
                    sparse = getattr(tr, "sparse_score", None)
                    dense = getattr(tr, "dense_score", None)
                    rrf = getattr(tr, "rrf_score", None)
                    base = getattr(tr, "score", None)  # 当前 score 已是 V4 的 s_final

                    if strategy == "sparse_heavy_blend":
                        # 0.2*dense + 0.8*sparse（缺失则降级）
                        s = 0.0
                        if dense is not None and sparse is not None:
                            s = 0.2 * float(dense or 0.0) + 0.8 * float(sparse or 0.0)
                        elif sparse is not None:
                            s = float(sparse or 0.0)
                        elif rrf is not None:
                            s = float(rrf or 0.0)
                        else:
                            s = float(base or 0.0)
                        tr.score = float(s)
                    else:
                        # sparse_only（默认）：优先 sparse_score
                        if sparse is not None:
                            tr.score = float(sparse or 0.0)
                        elif rrf is not None:
                            tr.score = float(rrf or 0.0)
                        else:
                            tr.score = float(base or 0.0)

                    d["v4_low_confidence_mode"] = True
                    d["v4_low_confidence_strategy"] = strategy
                    scoring_details[tr.table_id] = d

                result.table_results.sort(key=lambda t: t.score or 0.0, reverse=True)

        # ========== V4：Anti-flip（排序稳定性约束）==========
        if use_v4:
            anti_cfg = get_retrieval_param("table_scoring.v4_ranker.anti_flip", {}) or {}
            anti_enabled = bool(anti_cfg.get("enabled", True))
            epsilon = float(anti_cfg.get("epsilon", 0.02) or 0.02)
            if anti_enabled and epsilon > 0 and len(result.table_results) >= 2:
                # 候选数量很小（top_k<=10），用相邻冒泡修正即可
                swapped = True
                # 最多迭代 N 次，避免异常数据导致死循环
                for _ in range(len(result.table_results)):
                    if not swapped:
                        break
                    swapped = False
                    for i in range(len(result.table_results) - 1):
                        a = result.table_results[i]
                        b = result.table_results[i + 1]
                        sa = float(a.score or 0.0)
                        sb = float(b.score or 0.0)
                        if abs(sa - sb) >= epsilon:
                            continue
                        da = scoring_details.get(a.table_id, {}) or {}
                        db = scoring_details.get(b.table_id, {}) or {}
                        pa = float(da.get("v4_s_penalty", da.get("base_score", 0.0)) or 0.0)
                        pb = float(db.get("v4_s_penalty", db.get("base_score", 0.0)) or 0.0)
                        # tie-break：S_penalty 更高者靠前
                        if pb > pa:
                            result.table_results[i], result.table_results[i + 1] = b, a
                            swapped = True
                            da["v4_anti_flip_swapped_with_next"] = True
                            db["v4_anti_flip_swapped_with_prev"] = True
                            scoring_details[a.table_id] = da
                            scoring_details[b.table_id] = db
        
        # 更新table_structures顺序，并应用字段枚举反馈排序
        table_id_order = [t.table_id for t in result.table_results]
        new_structures = []
        for tid in table_id_order:
            try:
                # 使用带枚举反馈的表结构加载
                structure = self.structure_loader.load_table_structure_with_enum_feedback(
                    tid, enum_matches
                )
                new_structures.append(structure)
            except ValueError:
                # 如果表不存在，尝试使用原有结构
                for s in result.table_structures:
                    if s.table_id == tid:
                        new_structures.append(s)
                        break
        result.table_structures = new_structures

        # 二阶段截断：只收敛在一个位置（避免重复重建 prompt 导致行为漂移）
        result = self.apply_llm_top_k_truncation(
            result=result,
            question=question,
            global_rules=global_rules,
            enum_matches=enum_matches,
        )
        
        logger.debug(
            "表打分 V2 完成",
            top_table=result.table_results[0].table_id if result.table_results else None,
            top_score=result.table_results[0].score if result.table_results else 0,
            scoring_details=scoring_details
        )
        
        return result
    
    def _load_table_measures_from_structures(
        self,
        table_structures: List["TableStructure"]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        从表结构中预加载度量字段
        
        Args:
            table_structures: 表结构列表（已包含完整字段信息）
            
        Returns:
            {table_id: [{display_name, synonyms}, ...]}
        """
        from server.nl2ir.table_structure_loader import TableStructure
        
        cache = {}
        for structure in table_structures:
            table_id = structure.table_id
            measures = []
            for measure_field in structure.measures:
                measures.append({
                    "field_id": getattr(measure_field, "field_id", None),
                    "display_name": measure_field.display_name,
                    "synonyms": measure_field.synonyms or []
                })
            cache[table_id] = measures
            
            if measures:
                logger.debug(
                    "度量字段加载",
                    table_id=table_id,
                    table_name=structure.display_name,
                    measure_count=len(measures),
                    measure_names=[m["display_name"] for m in measures]
                )
        
        return cache
    
    def _load_table_measures_cache(
        self,
        table_results: List[TableRetrievalResult]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        从 self.model.fields 预加载度量字段（备用方法）
        
        Returns:
            {table_id: [{display_name, synonyms}, ...]}
        """
        cache = {}
        for table_result in table_results:
            table_id = table_result.table_id
            measures = []
            for field_id, field in self.model.fields.items():
                if (field.datasource_id == table_id and 
                    field.field_category == 'measure' and
                    field.is_active):
                    measures.append({
                        "field_id": getattr(field, "field_id", field_id),
                        "display_name": field.display_name,
                        "synonyms": field.synonyms or []
                    })
            cache[table_id] = measures
        return cache
    
    def _calculate_measure_coverage_factor(
        self,
        question: str,
        table_id: str,
        table_measures: List[Dict[str, Any]],
        all_measures: Optional[List[Dict[str, Any]]] = None,
        measure_intent: Optional[Any] = None,
    ) -> tuple:
        """
        计算度量覆盖率因子（乘法因子）
        
        Args:
            question: 用户问题
            table_id: 表ID
            table_measures: 当前表的度量字段列表 [{display_name, synonyms}, ...]
            all_measures: 所有候选表的度量字段列表（用于动态提取关键词）
            
        Returns:
            (factor, detail)
            factor: 0.3 ~ 1.0（降权不淘汰）
            detail: 调试信息
        """
        # 如果度量覆盖因子未启用，直接返回 1.0
        if not self._measure_coverage_enabled:
            return 1.0, {"enabled": False, "note": "度量覆盖因子已禁用"}
        
        from server.nl2ir.measure_intent import extract_measure_intent, normalize_text

        # 1. 抽取度量意图（优先复用上游预计算）
        if measure_intent is None:
            tokens = []
            try:
                from server.nl2ir.tokenizer import Tokenizer
                tokens = Tokenizer.get_instance().cut(question)
            except Exception:
                tokens = []
            measure_intent = extract_measure_intent(
                question=question,
                tokens=tokens,
                all_measures=all_measures,
                keywords=RetrievalConfig.measure_coverage_keywords(),
                universal_keywords=RetrievalConfig.measure_universal_keywords(),
                compound_keywords=RetrievalConfig.measure_compound_keywords(),
                measure_families=RetrievalConfig.measure_families(),
                generic_terms=RetrievalConfig.measure_extraction_generic_terms(),
                suffix_keywords=RetrievalConfig.measure_extraction_suffix_keywords(),
                normalization_cfg=RetrievalConfig.measure_extraction_normalization(),
                min_phrase_len=RetrievalConfig.measure_extraction_min_phrase_len(),
                aggregation_hints=RetrievalConfig.aggregation_hints(),
                unit_hints=RetrievalConfig.measure_extraction_unit_hints(),
                unit_require_number=RetrievalConfig.measure_extraction_unit_require_number(),
            )

        required = list(getattr(measure_intent, "required_concepts", []) or [])
        if not required:
            return 1.0, {
                "required": [],
                "available": [],
                "coverage": 1.0,
                "note": "无明确度量需求或仅通用统计意图",
                "intent": getattr(measure_intent, "evidence", {}),
            }
        
        # 2. 构建当前表的“可用概念集合”（把字段名/同义词映射到 concept key）
        normalization_cfg = RetrievalConfig.measure_extraction_normalization()
        compound_map_norm = {
            normalize_text(k, normalization_cfg): v
            for k, v in (RetrievalConfig.measure_compound_keywords() or {}).items()
            if k
        }
        families_map_norm = {
            normalize_text(k, normalization_cfg): v
            for k, v in (RetrievalConfig.measure_families() or {}).items()
            if k
        }

        def _to_concept(p: str) -> str:
            pn = normalize_text(p, normalization_cfg)
            if not pn:
                return ""
            # families 优先
            fam = families_map_norm.get(pn)
            if fam:
                return normalize_text(fam, normalization_cfg)
            comp = compound_map_norm.get(pn)
            if comp:
                return normalize_text(comp, normalization_cfg)
            return pn

        available_concepts = set()
        available_raw = set()
        for measure in table_measures:
            dn = measure.get("display_name") or ""
            if dn:
                available_raw.add(dn)
                c = _to_concept(dn)
                if c:
                    available_concepts.add(c)
            for syn in (measure.get("synonyms") or []):
                if not syn:
                    continue
                available_raw.add(syn)
                c = _to_concept(syn)
                if c:
                    available_concepts.add(c)
        
        # 3. 添加通用度量到可用集合（所有表都支持）
        if hasattr(self, '_universal_measures') and self._universal_measures:
            for keywords_list in self._universal_measures.values():
                for kw in keywords_list:
                    c = _to_concept(kw)
                    if c:
                        available_concepts.add(c)
        
        # 4. 计算覆盖
        covered = [m for m in required if m in available_concepts]
        coverage = len(covered) / len(required) if required else 1.0
        
        # 5. 识别通用度量匹配
        universal_matched = []
        if hasattr(self, '_universal_measures'):
            for measure_name, keywords in self._universal_measures.items():
                if any(keyword in covered for keyword in keywords):
                    universal_matched.append(measure_name)
        
        # 6. 返回因子（降权不淘汰）
        factor = max(self._measure_coverage_partial_min, coverage)
        
        detail = {
            "required": required,
            "available": list(dict.fromkeys([normalize_text(x, normalization_cfg) for x in list(available_raw)[:50] if x]))[:10],
            "available_concepts": list(available_concepts)[:20],
            "covered": covered,
            "coverage": round(coverage, 4),
            "universal_matched": universal_matched,  # 新增
            "has_universal": bool(universal_matched),  # 新增
            "intent": getattr(measure_intent, "evidence", {}),
        }
        
        # 增强日志
        logger.info(
            "表度量覆盖率计算",
            table_id=table_id[:8] + "...",
            required_count=len(required),
            covered_count=len(covered),
            coverage_ratio=round(coverage, 3),
            final_factor=round(factor, 3),
            universal_measures=universal_matched,
            has_universal_measures=bool(hasattr(self, '_universal_measures'))
        )
        
        return factor, detail

    def _apply_measure_factor_strong_gate(
        self,
        *,
        table_id: str,
        measure_factor: float,
        measure_detail: Dict[str, Any],
        apply_measure_signals: bool,
        measure_pg_norm: Optional[Dict[str, float]] = None,
        measure_milvus_norm: Optional[Dict[str, float]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """
        强门槛：避免 measure_factor 在“度量意图不够明确”时误降权。

        通过条件（默认）：
        - 必须命中聚合词或“数字+单位”（来自 measure_intent.evidence）
        - 或（可选）该表至少命中一条 PG/Milvus 度量 evidence（按 norm 分数阈值）
        """
        strong_cfg = get_retrieval_param("table_scoring.measure_coverage.strong_gate", {}) or {}
        enabled = bool(strong_cfg.get("enabled", True))
        require_agg_or_unit = bool(strong_cfg.get("require_agg_or_unit", True))
        allow_signal_evidence = bool(strong_cfg.get("allow_signal_evidence", True))
        signal_min_norm = float(strong_cfg.get("signal_min_norm", 0.05) or 0.05)

        required = list((measure_detail or {}).get("required") or [])
        intent = (measure_detail or {}).get("intent") or {}
        agg_hit = bool(intent.get("agg_keywords_hit"))
        unit_hit = bool(intent.get("unit_hits"))

        basic_pass = True if not require_agg_or_unit else bool(agg_hit or unit_hit)

        pg_v = float(((measure_pg_norm or {}).get(table_id) or 0.0))
        milvus_v = float(((measure_milvus_norm or {}).get(table_id) or 0.0))
        signal_pass = False
        if allow_signal_evidence and apply_measure_signals:
            signal_pass = (pg_v >= signal_min_norm) or (milvus_v >= signal_min_norm)

        passed = (not enabled) or (not required) or bool(basic_pass or signal_pass)
        if not enabled:
            reason = "disabled"
        elif not required:
            reason = "no_required_concepts"
        elif basic_pass:
            reason = "agg_or_unit"
        elif signal_pass:
            reason = "signal_evidence"
        else:
            reason = "weak_intent_no_signal"

        final_factor = float(measure_factor or 1.0)
        if enabled and required and final_factor < 0.999999 and not (basic_pass or signal_pass):
            final_factor = 1.0

        gate_trace = {
            "enabled": enabled,
            "passed": bool(passed),
            "reason": reason,
            "require_agg_or_unit": require_agg_or_unit,
            "agg_hit": agg_hit,
            "unit_hit": unit_hit,
            "allow_signal_evidence": allow_signal_evidence,
            "signal_min_norm": signal_min_norm,
            "pg_norm": round(pg_v, 6),
            "milvus_norm": round(milvus_v, 6),
        }
        try:
            (measure_detail or {}).setdefault("strong_gate", gate_trace)
        except Exception:
            pass
        return final_factor, gate_trace
    
    def _extract_universal_measures_from_rules(
        self,
        global_rules: Optional[List[dict]]
    ) -> Dict[str, List[str]]:
        """
        从派生指标提取通用度量词典
        
        通用度量定义：无字段依赖、适用所有表的度量（如COUNT(*)）
        
        Args:
            global_rules: 全局规则列表
            
        Returns:
            {
                "宗数": ["宗数", "笔数", "件数", "条数", "数量", "记录数"],
                ...
            }
        """
        universal_measures = {}
        
        if not global_rules:
            return universal_measures
        
        for rule in global_rules:
            if rule.get('rule_type') != 'derived_metric':
                continue
            
            rule_def = rule.get('rule_definition', {})
            display_name = rule_def.get('display_name')
            formula = rule_def.get('formula', '')
            field_deps = rule_def.get('field_dependencies', [])
            synonyms = rule_def.get('synonyms', [])
            
            # 判断是否为通用度量：无字段依赖 + 公式包含COUNT/EXISTS等
            is_universal = (
                not field_deps and 
                any(keyword in formula.upper() 
                    for keyword in ['COUNT(*)', 'COUNT', 'EXISTS'])
            )
            
            if is_universal and display_name:
                # 合并显示名和同义词
                all_keywords = [display_name] + (synonyms if synonyms else [])
                universal_measures[display_name] = all_keywords
                
                logger.info(
                    "识别到通用度量",
                    display_name=display_name,
                    keywords=all_keywords[:5],  # 只显示前5个
                    formula=formula
                )
        
        return universal_measures
    
    def _extract_measure_keywords(
        self,
        question: str,
        all_measures: Optional[List[Dict[str, Any]]] = None
    ) -> List[str]:
        """
        从问题中提取度量关键词
        
        优先级：
        1. 通用度量词典（派生指标，最高优先级）
        2. 配置的基础关键词
        3. 候选表的度量字段（动态）
        
        核心改进（P0）：
        - 使用复合词映射，将"耕地"、"农用地"等复合词映射到基础度量词"面积"
        - 这样表只需要有"面积"字段就能满足"耕地面积"的需求
        - 通用度量关键词（如"宗数"、"数量"）不参与覆盖率惩罚
        
        Args:
            question: 用户问题
            all_measures: 所有候选表的度量字段列表 [{display_name, synonyms}, ...]
            
        Returns:
            度量关键词列表（已转换为基础度量词，排除通用度量）
        """
        # 兼容旧调用方：返回“参与覆盖率惩罚的概念 key 列表”（不含通用统计词）
        tokens: List[str] = []
        try:
            from server.nl2ir.tokenizer import Tokenizer
            tokens = Tokenizer.get_instance().cut(question)
        except Exception:
            tokens = []

        from server.nl2ir.measure_intent import extract_measure_intent

        intent = extract_measure_intent(
            question=question,
            tokens=tokens,
            all_measures=all_measures,
            keywords=RetrievalConfig.measure_coverage_keywords(),
            universal_keywords=RetrievalConfig.measure_universal_keywords(),
            compound_keywords=RetrievalConfig.measure_compound_keywords(),
            measure_families=RetrievalConfig.measure_families(),
            generic_terms=RetrievalConfig.measure_extraction_generic_terms(),
            suffix_keywords=RetrievalConfig.measure_extraction_suffix_keywords(),
            normalization_cfg=RetrievalConfig.measure_extraction_normalization(),
            min_phrase_len=RetrievalConfig.measure_extraction_min_phrase_len(),
            aggregation_hints=RetrievalConfig.aggregation_hints(),
            unit_hints=RetrievalConfig.measure_extraction_unit_hints(),
            unit_require_number=RetrievalConfig.measure_extraction_unit_require_number(),
        )
        return list(getattr(intent, "required_concepts", []) or [])
    
    def _calculate_tag_match_boost(
        self,
        question: str,
        table_tags: List[str],
        table_id: Optional[str] = None
    ) -> tuple:
        """
        计算语义标签匹配加成（V2.1 新增）
        
        从表的 tags（已存储在 PostgreSQL/Milvus）中检查是否与问题匹配。
        支持直接匹配和同义词匹配（同义词从数据库获取）。
        
        Args:
            question: 用户问题
            table_tags: 表的语义标签列表（来自 db_tables.tags）
            table_id: 表ID（用于获取同义词）
            
        Returns:
            (boost, detail)
        """
        # 开关检查（从feature_switches读取）
        if not RetrievalConfig.tag_match_enabled():
            return 0.0, {"matched_tags": [], "boost": 0.0, "note": "语义标签匹配已禁用"}
        
        if not table_tags:
            return 0.0, {"matched_tags": [], "boost": 0.0, "note": "无语义标签"}
        
        matched_tags = []
        
        # 获取表的同义词（从 SemanticModel 中获取，如果有的话）
        tag_synonyms = self._get_tag_synonyms_from_model(table_id)
        
        question_lower = question.lower()
        for tag in table_tags:
            if not tag:
                continue
            tag_lower = tag.lower()

            # 1. 直接匹配：tag 在问题中
            if tag_lower in question_lower:
                matched_tags.append({"tag": tag, "match_type": "direct"})
                continue
            
            # 2. 同义词匹配：使用数据库中的同义词
            synonyms = tag_synonyms.get(tag, [])
            synonym_hit = None
            for syn in synonyms:
                if syn and syn.lower() in question_lower:
                    synonym_hit = syn
                    matched_tags.append({"tag": tag, "match_type": "synonym", "matched_by": syn})
                    break
            
            if synonym_hit:
                continue
            
            # 注：已移除模糊匹配（substring），避免误匹配影响准确度
        
        # 从配置获取加成参数
        # 注：已移除累加机制，只要有匹配就给固定加分（max_boost），避免多标签累加导致不准确
        max_tag_boost = get_retrieval_param("table_scoring.tag_match.max_boost", 0.06)
        total_boost = max_tag_boost if matched_tags else 0.0
        
        return total_boost, {
            "table_tags": table_tags,
            "matched_tags": matched_tags,
            "boost": round(total_boost, 4)
        }
    
    def _get_tag_synonyms_from_model(self, table_id: Optional[str]) -> Dict[str, List[str]]:
        """
        从 SemanticModel 获取表的同义词配置
        
        同义词存储在 datasource.identity.unique_terms 或 datasource.identity.core_phrases
        
        Returns:
            {tag: [synonyms]}
        """
        if not table_id or not self.model:
            return {}
        
        datasource = self.model.datasources.get(table_id)
        if not datasource:
            return {}
        
        # 构建同义词映射
        synonyms_map = {}
        
        # 从 identity 配置获取
        identity = getattr(datasource, 'identity', None)
        if identity:
            unique_terms = getattr(identity, 'unique_terms', []) or []
            core_phrases = getattr(identity, 'core_phrases', []) or []
            
            # 将 unique_terms 作为所有 tags 的通用同义词
            tags = datasource.tags or []
            for tag in tags:
                synonyms_map[tag] = list(unique_terms)
                # 添加 core_phrases 的文本
                for phrase in core_phrases:
                    text = getattr(phrase, 'text', None)
                    if text and text != tag:
                        synonyms_map[tag].append(text)
        
        return synonyms_map
    
    def _pass_enum_for_llm_mode(
        self,
        enum_matches: List[Any],
        table_id: str,
        question: str
    ) -> tuple:
        """
        LLM 选表模式下的简化枚举处理
        
        策略：直接按相似度排序，取当前表的所有枚举值传给 LLM，让 LLM 自己判断用哪个。
        不做复杂的门控过滤，因为 LLM 能够理解语义。
        
        Args:
            enum_matches: 枚举匹配结果
            table_id: 表ID
            question: 用户问题（暂未使用，保留接口一致性）
            
        Returns:
            (boost, detail)
        """
        passed_enums = []
        
        # 筛选当前表的枚举值，按 final_score 排序
        table_enums = []
        for enum in enum_matches:
            enum_table_id = getattr(enum, 'table_id', None)
            if enum_table_id != table_id:
                continue
            table_enums.append(enum)
        
        # 先按字段分组
        field_groups: Dict[str, List[Any]] = {}
        for enum in table_enums:
            field_id = getattr(enum, 'field_id', None)
            if field_id not in field_groups:
                field_groups[field_id] = []
            field_groups[field_id].append(enum)
        
        # 每个字段最多取 top N 个枚举值（可配置，避免过多干扰 LLM）
        # 优先使用 .env 配置，回退到 YAML 配置
        max_per_field = settings.llm_table_selection_enum_per_field
        
        # 每个字段内部按分数排序，取前 N 个
        for field_id, enums in field_groups.items():
            # 字段内部按分数排序
            sorted_field_enums = sorted(
                enums,
                key=lambda x: getattr(x, 'final_score', 0) or 0,
                reverse=True
            )
            # 取前 N 个
            top_enums = sorted_field_enums[:max_per_field]
            
            for enum in top_enums:
                field_name = getattr(enum, 'field_name', '')
                value = getattr(enum, 'value', '')
                final_score = getattr(enum, 'final_score', 0.0) or 0.0
                match_type = getattr(enum, 'match_type', 'value_vector')
                context_sim = getattr(enum, 'context_similarity', None) or 0.0
                
                passed_enums.append({
                    "field": field_name,
                    "value": value,
                    "match_type": match_type,
                    "final_score": round(final_score, 3),
                    "context_sim": round(context_sim, 3),
                    "llm_mode": True  # 标记为 LLM 模式放行
                })
        
        # LLM 模式下不计算 boost（表已经被选中了，无需再调整分数）
        total_boost = 0.0
        
        logger.info(
            "LLM模式枚举放行",
            table_id=table_id[:8] + "...",
            passed_count=len(passed_enums),
            fields=len(field_groups)
        )
        
        return total_boost, {
            "passed": len(passed_enums),
            "blocked": 0,
            "passed_enums": passed_enums,
            "blocked_enums": [],
            "total_boost": 0.0,
            "llm_mode": True,
            "precision_stats": {}
        }
    
    def _calculate_gated_enum_boost(
        self,
        enum_matches: List[Any],
        table_id: str,
        question: str
    ) -> tuple:
        """
        计算带门控的枚举加成（加法加成）
        
        V3.0 改进（P0方案）：
        - exact/synonym 匹配直接通过门控（它们本身就是高质量匹配）
        - value_vector 匹配使用动态阈值（根据字段精度推断）
        - 高精度字段（行政区、征收单位）：threshold=0.85
        - 中精度字段（用途、地类）：threshold=0.70
        - 低精度字段（备注、说明）：threshold=0.60
        
        Args:
            enum_matches: 枚举匹配结果
            table_id: 表ID
            question: 用户问题
            
        Returns:
            (boost, detail)
        """
        total_boost = 0.0
        passed_enums = []
        blocked_enums = []
        
        question_lower = question.lower()
        
        # 获取字段精度推断器
        inferencer = get_field_precision_inferencer()
        
        for enum in enum_matches:
            enum_table_id = getattr(enum, 'table_id', None)
            if enum_table_id != table_id:
                continue
            
            context_sim = getattr(enum, 'context_similarity', None) or 0.0
            field_id = getattr(enum, "field_id", None)
            field_name = getattr(enum, 'field_name', '')
            value = getattr(enum, 'value', '')
            match_type = getattr(enum, 'match_type', 'value_vector')
            final_score = getattr(enum, 'final_score', 0.0) or getattr(enum, 'score', 0.0) or 0.0
            
            # exact/synonym 匹配直接通过门控
            if match_type in ('exact', 'synonym'):
                # exact/synonym 匹配本身就是高质量的，直接通过
                boost = self._enum_gate_exact_boost
                total_boost += boost
                passed_enums.append({
                    "field": field_name,
                    "value": value,
                    "match_type": match_type,
                    "final_score": round(final_score, 3),
                    "boost": boost,
                    "gate_bypass": "exact/synonym 匹配直接通过"
                })
            else:
                # 🔥 P0改进：动态推断字段精度
                # 优先使用语义模型中的字段类型/枚举基数，提升跨数据源鲁棒性（避免只依赖中文字段名规则）
                field_type = None
                enum_count = None
                try:
                    if field_id and self.model and getattr(self.model, "fields", None) and field_id in self.model.fields:
                        fobj = self.model.fields[field_id]
                        field_type = getattr(fobj, "field_category", None)
                        if getattr(self.model, "field_enums", None) and field_id in self.model.field_enums:
                            enum_count = len(self.model.field_enums.get(field_id) or [])
                except Exception:
                    field_type = None
                    enum_count = None

                precision_info = inferencer.infer_precision(
                    field_name=field_name,
                    field_type=field_type,
                    enum_count=enum_count
                )
                
                dynamic_threshold = precision_info['threshold']
                dynamic_boost = precision_info['boost']
                precision_level = precision_info['precision_level']
                
                field_in_question = self._field_name_in_question(field_name, question_lower, table_id)
                is_high_noise = field_name in self._high_noise_fields

                # 高噪声字段需要问题中出现相关描述
                if is_high_noise and not field_in_question:
                    blocked_enums.append({
                        "field": field_name,
                        "value": value,
                        "match_type": match_type,
                        "context_sim": round(context_sim, 3),
                        "precision_level": precision_level,
                        "threshold": dynamic_threshold,
                        "reason": "high_noise_field_without_question_hint"
                    })
                    continue

                # 🔥 P0改进：使用动态阈值和动态boost
                if context_sim >= dynamic_threshold and field_in_question:
                    total_boost += dynamic_boost
                    passed_enums.append({
                        "field": field_name,
                        "value": value,
                        "match_type": match_type,
                        "context_sim": round(context_sim, 3),
                        "threshold": dynamic_threshold,
                        "boost": dynamic_boost,
                        "precision_level": precision_level
                    })
                else:
                    # 记录未通过原因
                    reason_parts = []
                    if context_sim < dynamic_threshold:
                        reason_parts.append(f"context_sim {context_sim:.3f} < threshold {dynamic_threshold}")
                    if not field_in_question:
                        reason_parts.append("field_not_in_question")
                    
                    blocked_enums.append({
                        "field": field_name,
                        "value": value,
                        "match_type": match_type,
                        "context_sim": round(context_sim, 3),
                        "threshold": dynamic_threshold,
                        "precision_level": precision_level,
                        "reason": " & ".join(reason_parts) if reason_parts else "unknown"
                    })
        
        # 应用上限
        final_boost = min(total_boost, self._enum_gate_max_boost)
        
        # 统计精度等级分布
        precision_stats = {}
        for enum_info in passed_enums + blocked_enums:
            level = enum_info.get('precision_level', 'unknown')
            if level not in precision_stats:
                precision_stats[level] = {"passed": 0, "blocked": 0}
        
        for enum_info in passed_enums:
            level = enum_info.get('precision_level', 'unknown')
            precision_stats[level]["passed"] += 1
        
        for enum_info in blocked_enums:
            level = enum_info.get('precision_level', 'unknown')
            precision_stats[level]["blocked"] += 1
        
        # 增强日志
        logger.info(
            "枚举门控完成",
            table_id=table_id[:8] + "...",
            passed_count=len(passed_enums),
            blocked_count=len(blocked_enums),
            final_boost=round(final_boost, 4),
            precision_stats=precision_stats
        )
        
        return final_boost, {
            "passed": len(passed_enums),
            "blocked": len(blocked_enums),
            "passed_enums": passed_enums,
            "blocked_enums": blocked_enums[:5],  # 增加输出长度以便调试
            "total_boost": round(final_boost, 4),
            "precision_stats": precision_stats  # 新增精度统计
        }

    def _field_name_in_question(self, field_name: str, question_lower: str, table_id: Optional[str]) -> bool:
        if not field_name:
            return False

        normalized = field_name.lower()
        if normalized in question_lower:
            return True

        if not self.model or not table_id:
            return False

        for field in self.model.fields.values():
            if field.datasource_id != table_id:
                continue
            if field.display_name == field_name:
                for synonym in field.synonyms or []:
                    if synonym and synonym.lower() in question_lower:
                        return True
                break
        return False

    @staticmethod
    def _fuzzy_contains(text_lower: str, pattern_lower: str, min_overlap: int = 2) -> bool:
        if not pattern_lower:
            return False
        if pattern_lower in text_lower:
            return True

        length = len(pattern_lower)
        for window in range(length, min_overlap - 1, -1):
            for start in range(0, length - window + 1):
                snippet = pattern_lower[start:start + window]
                if len(snippet) >= min_overlap and snippet in text_lower:
                    return True
        return False

    def _rescue_tables_by_enum(
        self,
        table_results: List[TableRetrievalResult],
        enum_matches: List[Any],
        domain_id: Optional[str]
    ) -> List[TableRetrievalResult]:
        """
        表救援机制：高置信枚举命中但表不在Top-K时，救回该表
        
        触发条件：
        - 表不在 Top-K
        - 但有高置信枚举（exact/synonym）命中该表
        
        救援策略：
        - 以中性分（当前 Top-K 最低分）插入候选尾部
        
        Args:
            table_results: 当前表检索结果
            enum_matches: 枚举匹配结果
            domain_id: 当前业务域ID
            
        Returns:
            被救援的表列表
        """
        if not self._rescue_enabled:
            return []
        
        # 当前Top-K的表ID集合
        existing_table_ids = {t.table_id for t in table_results}
        
        # 获取最低分作为救援分数
        min_score = min((t.score or 0.0 for t in table_results), default=0.3)
        rescue_score = max(min_score, self._rescue_min_score)
        
        # 找出需要救援的表
        rescue_table_ids: Dict[str, str] = {}  # table_id -> rescue_reason
        
        for enum in enum_matches:
            table_id = getattr(enum, 'table_id', None)
            if not table_id:
                continue
            
            # 只有exact/synonym匹配才触发救援
            match_type = getattr(enum, 'match_type', 'value_vector')
            if match_type not in ('exact', 'synonym'):
                continue
            
            # 检查表是否不在当前结果中
            if table_id not in existing_table_ids:
                rescue_reason = (
                    f"enum_{match_type}: {getattr(enum, 'field_name', '')}="
                    f"{getattr(enum, 'value', '')}"
                )
                rescue_table_ids[table_id] = rescue_reason
        
        # 创建救援的TableRetrievalResult
        rescued: List[TableRetrievalResult] = []
        for table_id, reason in rescue_table_ids.items():
            try:
                # 从语义模型获取表信息
                datasource = self.model.datasources.get(table_id)
                if not datasource:
                    continue
                
                # V3：不再因 domain 不一致跳过救援（domain 仅作为排序算子，避免 Recall 误杀）
                
                rescued_result = TableRetrievalResult(
                    table_id=table_id,
                    datasource=datasource,
                    score=rescue_score,
                    field_count=self.table_retriever._estimate_field_count_from_model(
                        table_id
                    ),
                    dense_score=None,
                    sparse_score=None,
                    rrf_score=rescue_score,
                    reranker_score=None,
                )
                # 记录救援原因
                rescued_result.rescue_reason = reason
                try:
                    if rescued_result.evidence is None:
                        rescued_result.evidence = {}
                    rescued_result.evidence["rescued"] = True
                    rescued_result.evidence["rescue_reason"] = reason
                    # 保留表所属 domain_id 供 V3 打分判断
                    if getattr(datasource, "domain_id", None):
                        rescued_result.evidence.setdefault("domain_id", getattr(datasource, "domain_id"))
                except Exception:
                    pass
                rescued.append(rescued_result)
                
                logger.debug(
                    "表被救援",
                    table_id=table_id,
                    rescue_reason=reason,
                    rescue_score=rescue_score
                )
                
            except Exception as e:
                logger.warning(
                    "表救援失败",
                    table_id=table_id,
                    error=str(e)
                )
        
        return rescued

    def _rescue_tables_by_measure_milvus(
        self,
        *,
        table_results: List[TableRetrievalResult],
        measure_table_scores: Dict[str, float],
        threshold: float,
    ) -> List[TableRetrievalResult]:
        """
        度量救援（Milvus field 强命中）：
        - 仅将表“救回进池”，不强行置顶；最终排序仍由统一公式决定。
        - 触发条件：table_id 不在当前候选集，且 measure_table_scores[table_id] >= threshold
        """
        if not measure_table_scores:
            return []

        existing_table_ids = {t.table_id for t in (table_results or [])}
        min_score = min((t.score or 0.0 for t in table_results), default=0.3)
        rescue_score = max(min_score, self._rescue_min_score)

        rescued: List[TableRetrievalResult] = []
        for table_id, score_norm in (measure_table_scores or {}).items():
            try:
                if table_id in existing_table_ids:
                    continue
                if float(score_norm or 0.0) < float(threshold or 0.0):
                    continue

                datasource = self.model.datasources.get(table_id) if self.model else None
                if not datasource:
                    continue

                rescued_result = TableRetrievalResult(
                    table_id=table_id,
                    datasource=datasource,
                    score=rescue_score,
                    field_count=self.table_retriever._estimate_field_count_from_model(table_id),
                    dense_score=None,
                    sparse_score=None,
                    rrf_score=rescue_score,
                    reranker_score=None,
                )
                rescued_result.rescue_reason = f"measure_milvus>= {round(float(score_norm), 3)}"
                try:
                    rescued_result.evidence = rescued_result.evidence or {}
                    rescued_result.evidence["rescued"] = True
                    rescued_result.evidence["rescue_reason"] = rescued_result.rescue_reason
                    rescued_result.evidence["measure_milvus_norm"] = float(score_norm)
                    if getattr(datasource, "domain_id", None):
                        rescued_result.evidence.setdefault("domain_id", getattr(datasource, "domain_id"))
                except Exception:
                    pass

                rescued.append(rescued_result)
            except Exception:
                continue

        return rescued

    def _check_main_table_consistency(
        self,
        result: HierarchicalRetrievalResult,
        enum_matches: List[Any],
        required_measures: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        检查主表一致性（V2 版本已通过 measure_coverage_factor 和 gated_enum_boost 处理）
        
        保留此方法以兼容旧代码，但默认返回一致性通过。
        表的评分现在由 apply_enum_boost_and_rerank 中的 V2 公式处理。
        
        Args:
            result: 层次化检索结果
            enum_matches: 枚举匹配结果
            required_measures: 所需的度量字段列表
            
        Returns:
            {
                "consistent": bool,
                "main_table_id": str,
                "action": str
            }
        """
        # V2 版本：主表一致性检查已通过评分公式处理
        # 直接返回一致性通过
        if not result.table_results:
            return {"consistent": True, "action": "keep"}
        
        main_table_id = result.table_results[0].table_id
        return {
            "consistent": True,
            "main_table_id": main_table_id,
            "action": "keep",
            "note": "V2 版本通过 measure_coverage_factor 和 gated_enum_boost 处理表评分"
        }
    
    def apply_main_table_switch(
        self,
        result: HierarchicalRetrievalResult,
        switch_to_table_id: str,
    ) -> HierarchicalRetrievalResult:
        """
        执行主表切换
        
        Args:
            result: 原始检索结果
            switch_to_table_id: 切换目标表ID
            
        Returns:
            更新后的检索结果
        """
        # 找到目标表在结果中的位置
        target_index = None
        for i, tr in enumerate(result.table_results):
            if tr.table_id == switch_to_table_id:
                target_index = i
                break
        
        if target_index is None or target_index == 0:
            # 目标表不存在或已经是主表
            return result
        
        # 交换位置：将目标表移到第一位
        target_table = result.table_results.pop(target_index)
        result.table_results.insert(0, target_table)
        
        # 同步更新 table_structures 顺序
        target_struct_index = None
        for i, struct in enumerate(result.table_structures):
            if struct.table_id == switch_to_table_id:
                target_struct_index = i
                break
        
        if target_struct_index is not None and target_struct_index != 0:
            target_struct = result.table_structures.pop(target_struct_index)
            result.table_structures.insert(0, target_struct)
        
        logger.info(
            "主表切换完成",
            new_main_table=switch_to_table_id
        )
        
        return result
