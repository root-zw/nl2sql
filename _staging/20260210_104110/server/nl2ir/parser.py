"""NL → IR 解析器"""

from typing import Tuple, Dict, Any, List, Optional
import json
import copy
import re
from pathlib import Path
import structlog
from pydantic import ValidationError

from server.models.ir import IntermediateRepresentation
from server.nl2ir.llm_client import LLMClient
from server.nl2ir.validator import IRValidator
from server.nl2ir.domain_detector import DomainDetector
from server.nl2ir.cross_domain_validator import CrossDomainValidator
from server.nl2ir.few_shot_direct import (
    FewShotDirectDecision,
    select_direct_execution_candidate,
)
from server.nl2ir.post_fusion import PostFusionScorer
from server.exceptions import ParseError
from server.config import settings, RetrievalConfig, get_retrieval_param
from server.utils.prompt_loader import resolve_path

logger = structlog.get_logger()

# 提示词文件路径
_DEFAULT_PROMPTS_DIR = Path(__file__).parent.parent.parent / "prompts" / "nl2ir"
PROMPTS_DIR = resolve_path(settings.nl2ir_prompts_dir, _DEFAULT_PROMPTS_DIR)
SYSTEM_PROMPT_FILE = resolve_path(settings.nl2ir_system_prompt_file, PROMPTS_DIR / "system.txt")
FUNCTION_SCHEMA_FILE = resolve_path(settings.nl2ir_function_schema_file, PROMPTS_DIR / "function_schema.json")


def load_system_prompt() -> str:
    """加载系统提示词"""
    try:
        with open(SYSTEM_PROMPT_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
        logger.debug("系统提示词加载成功", file=str(SYSTEM_PROMPT_FILE))
        return content
    except FileNotFoundError:
        logger.warning(f"提示词文件不存在，使用默认配置: {SYSTEM_PROMPT_FILE}")
        return _get_default_system_prompt()
    except Exception as e:
        logger.error(f"加载提示词失败: {e}，使用默认配置")
        return _get_default_system_prompt()


def load_function_schema() -> Dict[str, Any]:
    """加载 Function Schema"""
    try:
        with open(FUNCTION_SCHEMA_FILE, "r", encoding="utf-8") as f:
            schema = json.load(f)
        logger.debug("Function Schema 加载成功", file=str(FUNCTION_SCHEMA_FILE))
        return schema
    except FileNotFoundError:
        logger.warning(f"Schema 文件不存在，使用默认配置: {FUNCTION_SCHEMA_FILE}")
        return _get_default_function_schema()
    except Exception as e:
        logger.error(f"加载 Schema 失败: {e}，使用默认配置")
        return _get_default_function_schema()


def _get_default_system_prompt() -> str:
    """默认系统提示词（作为后备）"""
    return """你是一个专业的数据分析助手。你的任务是将用户的自然语言问题转换为结构化的查询指令。

核心原则：
1. 严格基于候选项选择：只能从提供的候选指标和维度中选择，不能自己编造。
2. 精确理解时间：具体日期使用 type=absolute，日期必须使用 ISO 格式字符串（YYYY-MM-DD）。
3. 明确表达不确定性：如果有多种理解方式，请在 ambiguities 字段中说明。
4. 保持简洁：不要过度解读，用户问什么就转换什么。

请严格调用 produce_ir 函数生成查询指令。"""


def _get_default_function_schema() -> Dict[str, Any]:
    """默认 Function Schema（作为后备）"""
    return {
        "type": "function",
        "function": {
            "name": "produce_ir",
            "description": "将用户的自然语言查询转换为结构化的中间表示(IR)",
            "parameters": {
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {
                            "oneOf": [
                                {"type": "string", "description": "指标字段ID（默认SUM聚合）"},
                                {
                                    "type": "object",
                                    "properties": {
                                        "field": {"type": "string", "description": "指标字段ID"},
                                        "aggregation": {
                                            "type": "string",
                                            "enum": ["SUM", "AVG", "MIN", "MAX", "COUNT"],
                                            "description": "聚合函数类型"
                                        },
                                        "alias": {"type": "string", "description": "结果列别名"}
                                    },
                                    "required": ["field", "aggregation", "alias"],
                                    "description": "指定聚合函数的指标"
                                }
                            ]
                        },
                        "description": "指标列表，支持字符串（默认SUM）或对象格式（指定聚合函数）"
                    },
                    "dimensions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "维度ID列表"
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "解析置信度"
                    }
                },
                "required": ["metrics"]
            }
        }
    }


# 加载提示词和 Schema
SYSTEM_PROMPT = load_system_prompt()
PRODUCE_IR_TOOL = load_function_schema()




class NL2IRParser:
    """NL → IR 解析器"""

    def __init__(
        self,
        llm_client: LLMClient,
        semantic_model,
        domain_detector,
        global_rules_loader,
        hierarchical_retriever,
        enum_retriever=None  # 新增：枚举值检索器
    ):
        self.llm_client = llm_client
        self.semantic_model = semantic_model
        self.validator = IRValidator(semantic_model)
        self.domain_detector = domain_detector
        self.cross_domain_validator = CrossDomainValidator(semantic_model) if semantic_model else None
        self.global_rules_loader = global_rules_loader
        self.enum_retriever = enum_retriever  # 新增
        self.post_fusion = PostFusionScorer()

        # 保存最后一次解析的调试信息
        self.last_system_prompt = None
        self.last_user_prompt = None
        self.last_messages = None
        self.hierarchical_retriever = hierarchical_retriever
        self.last_validation_notes = None
        self.last_raw_ir_json: Dict[str, Any] | None = None
        self.last_retrieval_summary: Optional[Dict[str, Any]] = None

    async def parse(
        self,
        question: str,
        retry_count: int = None,
        user_specified_domain: str = None,  # 用户指定的业务域
        pre_retrieved_result=None,  # 预先检索的结果（LLM表选择模式使用）
        selected_table_id: str = None,  # 已选择的表ID（LLM表选择后传入，兼容单表）
        selected_table_ids: List[str] = None,  # 已选择的多个表ID（跨年查询等多表场景）
        user_id: str = None,  # 用户ID（用于权限过滤）
        user_role: str = None,  # 用户角色（用于权限过滤）
        is_cross_partition_query: bool = False,  # 是否为跨分区查询
        cross_partition_mode: str = None  # 跨分区模式：compare, union, multi_join（兼容旧版cross_year_*格式）
    ) -> Tuple[IntermediateRepresentation, float]:
        """
        将自然语言问题解析为 IR

        Args:
            question: 用户问题
            retry_count: 重试次数（默认从配置读取）
            user_specified_domain: 用户手动指定的业务域ID（优先级最高）
            pre_retrieved_result: 预先检索的 HierarchicalRetrievalResult（LLM表选择模式使用）
            selected_table_id: 已选择的表ID（兼容单表模式）
            selected_table_ids: 已选择的多个表ID（跨年查询等多表场景）
            user_id: 用户ID（用于权限过滤）
            user_role: 用户角色（用于权限过滤）
            is_cross_partition_query: 是否为跨分区查询（由表选择阶段确定）
            cross_partition_mode: 跨分区模式（compare/union/multi_join）

        Returns:
            (IR对象, 置信度)

        Raises:
            ParseError: 解析失败
        """
        self.last_retrieval_summary = None

        if retry_count is None:
            retry_count = settings.llm_max_retries

        logger.debug("开始解析自然语言（层次化检索）", question=question, user_domain=user_specified_domain)

        # 加载全局规则（派生指标、自定义规则）
        global_rules = []
        if self.global_rules_loader:
            try:
                global_rules = await self.global_rules_loader.load_active_rules(
                    rule_types=['derived_metric', 'custom_instruction'],
                    domain_id=user_specified_domain
                )
                logger.debug(f"加载全局规则成功，共{len(global_rules)}条")
            except Exception as e:
                logger.exception("加载全局规则失败，将继续使用基础Prompt", error=str(e))

        # 统一处理表ID：优先使用多表列表，兼容单表模式
        effective_table_ids = selected_table_ids or ([selected_table_id] if selected_table_id else None)
        
        # 如果提供了预检索结果（LLM表选择模式），直接使用
        if pre_retrieved_result is not None:
            hierarchical_result = pre_retrieved_result
            logger.debug("使用预检索结果（LLM表选择模式）")
        else:
            # 层次化检索（业务域 → 表 → 字段）
            # 如果已有选定的表，传递给检索器以跳过表选择
            hierarchical_result = await self.hierarchical_retriever.retrieve(
                question=question,
                user_domain_id=user_specified_domain,
                top_k_tables=RetrievalConfig.table_top_k(),
                global_rules=global_rules,
                selected_table_id=effective_table_ids[0] if effective_table_ids else None,
                selected_table_ids=effective_table_ids,  # 传递多表列表
                user_id=user_id,
                user_role=user_role
            )

        detected_domain_id = hierarchical_result.domain_id
        domain_name = hierarchical_result.domain_name

        # 若未检索到任何表结构，直接返回明确错误，避免无效的 LLM 调用
        if not hierarchical_result.table_structures:
            logger.warning(
                "层次化检索未返回任何表",
                question=question,
                domain=domain_name,
            )
            raise ParseError("未找到相关数据表，请检查问题描述或先同步元数据。")

        # ========== 注意：llm_top_k 截断移到枚举加成之后 ==========
        # 原因：枚举加成可能改变表的排序，必须先执行枚举加成再截断
        # 否则跨域高分表（如"建设用地批准书"）在枚举加成前就被排除了
        
        logger.debug("层次化检索完成（截断前）",
                   domain=domain_name,
                   tables=len(hierarchical_result.table_structures),
                   total_fields=sum(t.total_fields for t in hierarchical_result.table_structures))

        # ========== 新增：枚举值智能匹配 ==========
        enum_matches = []
        enum_triples: List[Dict[str, Any]] = []
        enum_suggestions = {}
        enum_prompt_metadata: Dict[str, List[Dict[str, Any]]] = {}
        candidate_fields = []
        enhanced_system_prompt = SYSTEM_PROMPT
        
        # 判断是否为LLM选表模式（表已确定）
        # 包括单表选择（llm_selection）和多表选择（multi_table_selection）
        table_retrieval_method = getattr(hierarchical_result, 'table_retrieval_method', None)
        is_llm_selection_mode = table_retrieval_method in ('llm_selection', 'multi_table_selection')

        if self.enum_retriever and hierarchical_result.table_structures:
            try:
                # 收集候选字段（仅维度/枚举型字段）
                # 度量字段（数值型、金额、面积等）不进枚举候选

                if is_llm_selection_mode and effective_table_ids:
                    # LLM选表模式：表已确定，只在已选定表的字段内检索
                    selected_table_set = set(effective_table_ids)
                    for table in hierarchical_result.table_structures:
                        if table.table_id in selected_table_set:
                            candidate_fields.extend(table.dimensions)
                            candidate_fields.extend(table.identifiers)
                    logger.debug(
                        "LLM选表模式，仅在已选表字段内检索枚举",
                        selected_tables=list(selected_table_set),
                        candidate_fields=len(candidate_fields)
                    )
                else:
                    # 向量选表模式：在所有候选表的字段内检索（用于辅助确定表）
                    for table in hierarchical_result.table_structures:
                        candidate_fields.extend(table.dimensions)
                        candidate_fields.extend(table.identifiers)

                if candidate_fields:
                    # 根据模式选择 top_k 策略
                    if is_llm_selection_mode:
                        # LLM选表模式：字段级 top_k（表已确定，确保每个字段都有枚举覆盖）
                        max_per_field = settings.llm_table_selection_enum_per_field
                        calculated_top_k = len(candidate_fields) * max_per_field
                        enum_top_k = min(calculated_top_k, 100)  # 上限100
                        enum_per_field_limit = max_per_field  # LLM模式使用环境变量配置
                        logger.debug(
                            "LLM选表模式，字段级top_k",
                            candidate_fields=len(candidate_fields),
                            max_per_field=max_per_field,
                            enum_top_k=enum_top_k
                        )
                    else:
                        # 向量选表模式：全局 top_k（跨表比较，辅助确定表）
                        enum_top_k = RetrievalConfig.enum_top_k()
                        enum_per_field_limit = None  # 向量模式使用配置文件默认值
                        logger.debug(f"向量选表模式，全局top_k: {enum_top_k}, 候选字段: {len(candidate_fields)}个")

                    # 执行枚举值检索
                    enum_matches = await self.enum_retriever.match_enum_values(
                        user_input=question,
                        candidate_fields=candidate_fields,
                        top_k=enum_top_k,
                        keyword_profile=getattr(hierarchical_result, "keyword_profile", None),
                        per_field_limit=enum_per_field_limit,  # LLM模式传入5，向量模式使用默认3
                    )
                    
                    if enum_matches:
                        logger.debug(
                            f"枚举值匹配成功: {len(enum_matches)}个结果",
                            top3=[f"{m.field_name}.{m.value}" for m in enum_matches[:3]]
                        )

                    enum_triples = self.post_fusion.combine(
                        tables=hierarchical_result.table_results,
                        enums=enum_matches,
                        question_vector=getattr(hierarchical_result, "question_vector", None),
                    )
                else:
                    enum_prompt_metadata = {}
                    logger.debug("无候选字段，跳过枚举值检索")
                    
            except Exception as e:
                logger.exception("枚举值检索失败，继续使用原始Prompt", error=str(e))
                enum_prompt_metadata = {}
                enum_triples = []
        else:
            if not self.enum_retriever:
                logger.debug("枚举值检索器未配置")
            enum_prompt_metadata = {}
            enum_triples = []

        # ========== V3 Pipeline：动态打分（仅向量选表模式需要，LLM选表模式表已确定）==========
        if hierarchical_result.table_results and not is_llm_selection_mode:
            hierarchical_result = await self.hierarchical_retriever.apply_enum_boost_and_rerank(
                result=hierarchical_result,
                enum_matches=enum_matches or [],
                question=question,
                global_rules=global_rules,
            )
        elif is_llm_selection_mode:
            logger.debug("LLM选表模式，跳过枚举加权重排（表已确定）")

        # ========== 生成最终 user_prompt（先定表顺序，再做枚举增强）==========
        user_prompt = self.hierarchical_retriever.format_prompt_for_llm(
            hierarchical_result,
            question,
        )

        # 应用枚举混合传递策略（在最终 user_prompt 上增强）
        # - LLM选表模式：使用内联格式【可选值: xxx】，每字段最多显示 top N 相似值
        # - 向量选表模式：使用阈值分类模式（prefill/suggest/hint）
        if enum_matches:
            from server.nl2ir.enum_prompt_strategy import EnumPromptStrategy

            main_table_id = None
            try:
                if hierarchical_result.table_results:
                    main_table_id = hierarchical_result.table_results[0].table_id
            except Exception:
                main_table_id = None

            strategy = EnumPromptStrategy(
                main_table_id=main_table_id,
                llm_selection_mode=is_llm_selection_mode  # LLM选表模式使用内联格式【可选值: xxx】
            )
            user_prompt, enum_suggestions, enum_prompt_metadata = strategy.apply_strategy(
                original_user_prompt=user_prompt,
                enum_matches=enum_matches,
            )

            # 仅当确实向 LLM 上下文注入了枚举信息时，才增强 system prompt（避免冗余）
            guidance_enabled = bool(get_retrieval_param("llm_prompt.enum_guidance.enabled", True))
            has_enum_injection = bool(enum_suggestions) or any(
                enum_prompt_metadata.get(key)
                for key in ("prefill", "suggest", "hint")
            ) or bool(enum_prompt_metadata.get("has_cross_table_conflict"))

            if guidance_enabled and has_enum_injection:
                enhanced_system_prompt = strategy.enhance_system_prompt(SYSTEM_PROMPT)

        # 构建messages
        user_message = {"role": "user", "content": user_prompt}
        
        # 如果有enum_suggestions，添加到user_message（某些模型支持）
        if enum_suggestions:
            user_message["enum_suggestions"] = enum_suggestions

        messages = [
            {"role": "system", "content": enhanced_system_prompt},
            user_message
        ]

        # 调试：最终 user_prompt 中的表数量
        final_table_count = len(re.findall(r"### 表\d+:", user_prompt))
        logger.info(
            "最终 user_prompt 表数量",
            table_count=final_table_count,
            has_enum_matches=bool(enum_matches),
            enum_match_count=len(enum_matches) if enum_matches else 0,
        )
        
        # 保存调试信息
        self.last_system_prompt = enhanced_system_prompt
        self.last_user_prompt = user_prompt
        self.last_messages = messages
        self.last_retrieval_summary = self._build_retrieval_summary(
            hierarchical_result=hierarchical_result,
            detected_domain_id=detected_domain_id,
            detected_domain_name=domain_name,
            user_domain_id=user_specified_domain,
            enum_matches=enum_matches,
            enum_triples=enum_triples,
            candidate_fields=candidate_fields,
            enum_suggestions=enum_suggestions,
            enum_prompt_metadata=enum_prompt_metadata,
            global_rule_count=len(global_rules),
            user_prompt=user_prompt,
            system_prompt=enhanced_system_prompt
        )

        direct_samples = (
            hierarchical_result.few_shot_direct_candidates
            or hierarchical_result.few_shot_examples
        )
        direct_decision, decision_debug = select_direct_execution_candidate(
            question=question,
            normalized_question=((hierarchical_result.table_retrieval_info or {}).get("few_shot") or {}).get(
                "normalized_question"
            )
            if hierarchical_result.table_retrieval_info
            else None,
            samples=direct_samples,
            table_results=hierarchical_result.table_results,
            domain_id=hierarchical_result.domain_id,
        )
        self._record_direct_decision_debug(hierarchical_result, decision_debug)
        if direct_decision:
            direct_ir = self._build_ir_from_sample(
                decision=direct_decision,
                hierarchical_result=hierarchical_result,
                question=question,
                detected_domain_id=detected_domain_id,
                domain_name=domain_name,
            )
            if direct_ir:
                confidence = max(
                    direct_decision.sample.quality_score or RetrievalConfig.few_shot_direct_min_quality(),
                    RetrievalConfig.few_shot_direct_min_quality(),
                )
                self._mark_direct_execution(direct_decision, hierarchical_result)
                logger.debug(
                    "Few-Shot直连执行命中",
                    sample_question=direct_decision.sample.question,
                    sample_id=direct_decision.sample.sample_id,
                )
                return direct_ir, confidence

        # 3. 调用 LLM（带重试）
        last_error = None

        for attempt in range(retry_count + 1):
            try:
                logger.debug(f"LLM 调用尝试 {attempt + 1}/{retry_count + 1}")

                # 调用 LLM
                response = await self.llm_client.chat_completion(
                    messages=messages,
                    tools=[PRODUCE_IR_TOOL],
                    tool_choice={"type": "function", "function": {"name": "produce_ir"}}
                )

                # 提取函数参数
                ir_json = self.llm_client.extract_function_call(response)

                if not ir_json:
                    raise ParseError("LLM 未返回函数调用")

                # 记录原始输出，便于追踪
                self.last_raw_ir_json = copy.deepcopy(ir_json)

                # 添加原始问题
                ir_json["original_question"] = question

                # 修正常见格式错误
                ir_json = self._fix_common_format_errors(ir_json)

                logger.debug(
                    "IR 初始JSON",
                    query_type=ir_json.get("query_type"),
                    metrics=ir_json.get("metrics"),
                    dimensions=ir_json.get("dimensions"),
                    filters=ir_json.get("filters"),
                    with_total=ir_json.get("with_total")
                )

                # 验证并构建 IR
                ir = IntermediateRepresentation(**ir_json)

                logger.debug(
                    "IR 构建后",
                    query_type=ir.query_type,
                    metrics=ir.metrics,
                    dimensions=ir.dimensions,
                    filters=[f.model_dump() for f in ir.filters] if ir.filters else None
                )

                # IR验证和修正
                ir = self.validator.validate_and_fix(ir)

                logger.debug(
                    "IR 验证后",
                    query_type=ir.query_type,
                    metrics=ir.metrics,
                    dimensions=ir.dimensions,
                    filters=[f.model_dump() for f in ir.filters] if ir.filters else None
                )
                # 保存验证/修正备注（用于trace）
                try:
                    self.last_validation_notes = self.validator.get_notes()
                except Exception:
                    self.last_validation_notes = None

                # 添加业务域信息
                ir.domain_id = str(detected_domain_id) if detected_domain_id else None
                ir.domain_name = domain_name

                # 添加主表提示：使用表级检索结果中的 Top1 作为默认主表
                try:
                    if hasattr(ir, "primary_table_id") and hierarchical_result.table_results:
                        top_table_id = hierarchical_result.table_results[0].table_id
                        ir.primary_table_id = top_table_id
                        logger.debug(
                            "设置IR默认主表提示（来自表检索Top1）",
                            primary_table_id=top_table_id,
                            domain_id=ir.domain_id,
                        )
                        # 在“TOP1 不稳定”的场景下，允许用 IR 中实际引用的字段对 TOP3 进行投票校正主表
                        try:
                            ir = self._maybe_override_primary_table_by_ir(ir, hierarchical_result)
                        except Exception as e:
                            logger.debug("主表投票校正失败，保留Top1", error=str(e))
                        # 主表提示确定后，再做一次“主表字段对齐”，避免跨表字段导致编译丢条件
                        ir = self.validator.align_filters_to_primary_table(ir)
                except Exception as e:
                    logger.exception("设置IR默认主表提示失败", error=str(e))
                
                # 设置选中的多表列表（用于跨年UNION等场景）
                if effective_table_ids and len(effective_table_ids) > 1:
                    ir.selected_table_ids = effective_table_ids
                    
                    # 填充表物理信息（用于 multi_join 等跨域场景，编译器可能无法从模型中获取）
                    if hierarchical_result.table_structures:
                        for table_struct in hierarchical_result.table_structures:
                            if table_struct.table_id in effective_table_ids:
                                ir.selected_table_info[table_struct.table_id] = {
                                    "table_name": table_struct.physical_table_name or table_struct.table_name,
                                    "schema_name": table_struct.schema_name,
                                    "display_name": table_struct.display_name
                                }
                    
                    logger.debug(
                        "设置IR多表列表（跨年/多表查询）",
                        selected_table_ids=effective_table_ids,
                        count=len(effective_table_ids),
                        table_info_count=len(ir.selected_table_info)
                    )

                    # 设置跨分区查询标志（用于触发UNION ALL或对比JOIN编译）
                    if is_cross_partition_query:
                        ir.cross_partition_query = True
                        
                        # 根据表选择阶段的模式决定编译器行为
                        # 模式统一：single/compare/union/multi_join（兼容旧版 cross_year_* 格式）
                        if cross_partition_mode:
                            # 模式映射（统一到 compare/union/multi_join）
                            mode_mapping = {
                                "compare": "compare",
                                "union": "union", 
                                "multi_join": "multi_join",
                                # 旧版兼容
                                "cross_year_compare": "compare",
                                "cross_year_union": "union",
                                "cross_year": "union",
                                "cross_partition": "union",
                            }
                            ir.cross_partition_mode = mode_mapping.get(cross_partition_mode, "union")
                            
                            # multi_join 模式需要额外构建字段映射
                            if ir.cross_partition_mode == "multi_join":
                                if hierarchical_result.table_structures and effective_table_ids:
                                    ir.multi_join_field_mappings = self._build_multi_join_field_mappings(
                                        hierarchical_result.table_structures,
                                        effective_table_ids,
                                        ir.compare_join_keys or [],
                                        ir.cross_table_field_mappings or {}
                                    )
                        else:
                            # 默认使用union模式
                            ir.cross_partition_mode = "union"
                        
                        # 设置对比关联字段（完全由 NL2IR 阶段的 LLM 决定）
                        # 将显示名转换为字段ID，保持与其他字段（metrics, dimensions, filters）一致
                        # 
                        # 职责划分：
                        # - 表选择阶段：只负责选表和确定查询模式（single/compare/union/multi_join）
                        # - NL2IR 阶段：负责决定具体的关联字段（compare_join_keys）
                        if hasattr(ir, 'compare_join_keys') and ir.compare_join_keys:
                            # NL2IR 阶段的 LLM 已经设置了，进行规范化转换
                            ir.compare_join_keys = self._convert_join_field_names_to_ids(
                                ir.compare_join_keys,
                                effective_table_ids
                            )
                            logger.debug(
                                "使用 NL2IR 阶段 LLM 设置的 compare_join_keys",
                                compare_join_keys=ir.compare_join_keys
                            )
                        else:
                            # NL2IR 阶段没有设置 compare_join_keys
                            # 这通常意味着用户只想要简单的汇总对比，不需要按维度拆分
                            ir.compare_join_keys = []
                            logger.debug(
                                "NL2IR 阶段未设置 compare_join_keys，使用简单汇总对比模式"
                            )
                        
                        # 构建跨表字段映射（用于编译器精确查找其他表的对应字段）
                        # 
                        # 关键：即使 LLM 生成了部分映射，也需要补充缺失的 metrics/dimensions 映射
                        # LLM 通常只生成 filters 的映射，容易遗漏度量字段
                        
                        # 确保 cross_table_field_mappings 是字典
                        if not ir.cross_table_field_mappings:
                            ir.cross_table_field_mappings = {}
                        else:
                            # LLM生成的映射，先做规范化（把显示名转换为UUID）
                            ir.cross_table_field_mappings = self._normalize_cross_table_field_mappings(
                                ir.cross_table_field_mappings,
                                effective_table_ids
                            )
                        
                        # 自动构建完整的映射（包括 metrics、dimensions、filters、compare_join_keys）
                        auto_mappings = self._build_cross_table_field_mappings(
                            ir,
                            effective_table_ids,
                            primary_table_id=ir.primary_table_id
                        )
                        
                        # 合并：LLM 的映射优先，自动构建的作为补充
                        if auto_mappings:
                            for field_id, table_mappings in auto_mappings.items():
                                if field_id not in ir.cross_table_field_mappings:
                                    ir.cross_table_field_mappings[field_id] = table_mappings
                                    logger.debug(
                                        "自动补充跨表字段映射",
                                        field_id=field_id,
                                        table_mappings=table_mappings
                                    )
                                else:
                                    # 合并表级映射
                                    for table_id, mapped_field_id in table_mappings.items():
                                        if table_id not in ir.cross_table_field_mappings[field_id]:
                                            ir.cross_table_field_mappings[field_id][table_id] = mapped_field_id
                        
                        logger.debug(
                            "设置跨分区查询标志",
                            cross_partition_query=True,
                            cross_partition_mode=ir.cross_partition_mode,
                            compare_join_keys=getattr(ir, 'compare_join_keys', None),
                            cross_table_field_mappings=ir.cross_table_field_mappings
                        )

                # ========== 验证精确匹配的枚举值是否被使用 ==========
                # 重要：必须在 primary_table_id 设置之后执行，避免将其他表/其他连接的枚举强行补入 filters
                if enum_matches:
                    self._verify_exact_enum_matches(
                        ir,
                        enum_matches,
                        primary_table_id=getattr(ir, "primary_table_id", None),
                    )
                    # 枚举补滤后再对齐一次，确保不会引入跨表字段导致后续连接解析/编译失败
                    try:
                        ir = self.validator.align_filters_to_primary_table(ir)
                    except Exception:
                        pass

                # 跨域查询验证
                if self.cross_domain_validator and not detected_domain_id:
                    # 只在未明确指定业务域时检查跨域
                    cross_domain_warnings = self.cross_domain_validator.validate(ir)
                    if cross_domain_warnings:
                        ir.ambiguities.extend(cross_domain_warnings)

                # 计算置信度
                confidence = self._calculate_confidence(ir, hierarchical_result)

                logger.debug(
                    "解析成功",
                    metrics=ir.metrics,
                    dimensions=ir.dimensions,
                    confidence=confidence,
                    domain=domain_name
                )
                logger.debug("NL→IR 解析完成", confidence=confidence)

                return ir, confidence

            except ValidationError as e:
                last_error = e
                error_msg = str(e)
                logger.warning(f"IR 验证失败 (尝试 {attempt + 1})", error=error_msg)

                if attempt < retry_count:
                    # 添加错误反馈到消息
                    messages.append({
                        "role": "assistant",
                        "content": f"上次尝试失败，错误: {error_msg}。"
                    })
                    messages.append({
                        "role": "user",
                        "content": "请修正错误，重新调用 produce_ir 函数。"
                    })
                    continue
                else:
                    break

            except Exception as e:
                last_error = e
                logger.error(f"解析异常 (尝试 {attempt + 1})", error=str(e))

                if attempt < retry_count:
                    continue
                else:
                    break

        # 所有尝试都失败
        raise ParseError(
            f"IR 解析失败（已重试 {retry_count} 次）: {str(last_error)}",
            details={"question": question, "last_error": str(last_error)}
        )

    async def parse_with_feedback(
        self,
        question: str,
        *,
        feedback: str,
        user_specified_domain: str = None,
        retry_count: int = 0,
        user_id: str = None,
        user_role: str = None
    ) -> Tuple[IntermediateRepresentation, float]:
        """
        在"编译/执行失败"后，把错误信息和约束反馈给模型再生成一次 IR。
        目标：系统性纠偏（不硬编码），提升跨数据源/跨库鲁棒性。
        """
        # 复用 parse 的整体流程，但在 user_prompt 末尾附加一段强约束反馈
        # 这里不增加额外的 validation 重试（由调用方控制次数）
        self.last_retrieval_summary = None

        if retry_count is None:
            retry_count = 0

        # 加载全局规则（派生指标、自定义规则）
        global_rules = []
        if self.global_rules_loader:
            try:
                global_rules = await self.global_rules_loader.load_active_rules(
                    rule_types=['derived_metric', 'custom_instruction'],
                    domain_id=user_specified_domain
                )
            except Exception:
                global_rules = []

        hierarchical_result = await self.hierarchical_retriever.retrieve(
            question=question,
            user_domain_id=user_specified_domain,
            top_k_tables=RetrievalConfig.table_top_k(),
            global_rules=global_rules,
            user_id=user_id,
            user_role=user_role
        )
        if not hierarchical_result.table_structures:
            raise ParseError("未找到相关数据表，请检查问题描述或先同步元数据。")

        # ========== 注意：llm_top_k 截断移到枚举加成之后 ==========
        # 原因：枚举加成可能改变表的排序，必须先执行枚举加成再截断

        # 枚举匹配（parse_with_feedback 始终走向量选表模式，在所有候选表中检索）
        enum_matches = []
        if self.enum_retriever and hierarchical_result.table_structures:
            try:
                candidate_fields = []
                for table in hierarchical_result.table_structures:
                    candidate_fields.extend(table.dimensions)
                    candidate_fields.extend(table.identifiers)
                if candidate_fields:
                    enum_matches = await self.enum_retriever.match_enum_values(
                        user_input=question,
                        candidate_fields=candidate_fields,
                        top_k=RetrievalConfig.enum_top_k(),  # 统一使用配置值
                        keyword_profile=getattr(hierarchical_result, "keyword_profile", None),
                    )
            except Exception:
                enum_matches = []

        # V3 Pipeline：动态打分（无论是否命中枚举，都执行）
        if hierarchical_result.table_results:
            hierarchical_result = await self.hierarchical_retriever.apply_enum_boost_and_rerank(
                result=hierarchical_result,
                enum_matches=enum_matches or [],
                question=question,
                global_rules=global_rules,
            )

        user_prompt = self.hierarchical_retriever.format_prompt_for_llm(hierarchical_result, question)

        # 将反馈注入 prompt（避免硬编码：只给“失败原因+约束”，让模型自我修正）
        feedback_block = (
            "\n\n### 纠偏反馈（系统自动生成）\n"
            f"{feedback.strip()}\n"
            "\n- 请仅使用上面提供的表结构中的字段ID/名称，不要编造指标/字段。\n"
            "- 若无法满足（例如指标不存在），请改用最接近的可用字段或派生规则，并保持过滤条件不丢失。\n"
        )
        user_prompt = f"{user_prompt}{feedback_block}"

        user_message = {"role": "user", "content": user_prompt}
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            user_message
        ]

        response = await self.llm_client.chat_completion(
            messages=messages,
            tools=[PRODUCE_IR_TOOL],
            tool_choice={"type": "function", "function": {"name": "produce_ir"}}
        )
        ir_json = self.llm_client.extract_function_call(response)
        if not ir_json:
            raise ParseError("LLM 未返回函数调用")
        self.last_raw_ir_json = copy.deepcopy(ir_json)
        ir_json["original_question"] = question
        ir_json = self._fix_common_format_errors(ir_json)
        ir = IntermediateRepresentation(**ir_json)
        ir = self.validator.validate_and_fix(ir)
        # 修复：在补滤前设置 primary_table_id，避免枚举强补误引入跨连接字段
        try:
            if hasattr(ir, "primary_table_id") and hierarchical_result.table_results:
                ir.primary_table_id = hierarchical_result.table_results[0].table_id
                try:
                    ir = self._maybe_override_primary_table_by_ir(ir, hierarchical_result)
                except Exception:
                    pass
                ir = self.validator.align_filters_to_primary_table(ir)
        except Exception:
            pass
        if enum_matches:
            self._verify_exact_enum_matches(
                ir,
                enum_matches,
                primary_table_id=getattr(ir, "primary_table_id", None),
            )
        confidence = self._calculate_confidence(ir, hierarchical_result)
        return ir, confidence

    def _build_multi_join_field_mappings(
        self,
        table_structures: list,
        table_ids: list,
        join_keys: list,
        cross_table_field_mappings: dict = None
    ) -> list:
        """
        为 multi_join 模式构建字段映射
        
        Args:
            table_structures: 表结构列表
            table_ids: 参与关联的表ID列表
            join_keys: 关联字段列表（可以是显示名或UUID）
            cross_table_field_mappings: LLM生成的跨表字段映射（用于异名字段关联）
                格式: {主表字段UUID: {其他表UUID: 对应字段UUID}}
        
        Returns:
            字段映射列表，每个元素是 {display_name: "字段名", table_id_1: "column_1", ...}
        """
        cross_table_field_mappings = cross_table_field_mappings or {}
        
        # 构建字段ID到字段信息的映射
        field_id_map = {}  # {field_id: {table_id, display_name, column}}
        # 构建表ID到字段的映射
        table_fields_map = {}  # {table_id: {display_name: column_name}}
        
        for table_struct in table_structures:
            if table_struct.table_id in table_ids:
                fields_map = {}
                # 收集所有字段类型
                all_fields = (
                    (table_struct.dimensions or []) +
                    (table_struct.identifiers or []) +
                    (table_struct.measures or []) +
                    (table_struct.timestamps or [])
                )
                for field in all_fields:
                    fields_map[field.display_name] = field.column
                    # 建立字段ID映射
                    if hasattr(field, 'field_id') and field.field_id:
                        field_id_map[field.field_id] = {
                            "table_id": table_struct.table_id,
                            "display_name": field.display_name,
                            "column": field.column
                        }
                    # 也加入别名
                    for alias in (getattr(field, 'aliases', []) or []):
                        fields_map[alias] = field.column
                table_fields_map[table_struct.table_id] = fields_map
        
        result = []
        
        # 辅助函数：判断是否为UUID格式
        def is_uuid(s: str) -> bool:
            import re
            uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            return bool(re.match(uuid_pattern, s, re.IGNORECASE))
        
        # 辅助函数：在表中查找字段（精确 + 模糊匹配）
        def find_column_in_table(field_name: str, table_id: str) -> str:
            if table_id not in table_fields_map:
                return None
            fields = table_fields_map[table_id]
            # 精确匹配
            if field_name in fields:
                return fields[field_name]
            # 模糊匹配：字段名包含关键词
            for name, column in fields.items():
                if field_name in name or name in field_name:
                    logger.debug(f"multi_join 模糊匹配: {field_name} -> {name} ({table_id})")
                    return column
            return None
        
        # 辅助函数：通过字段ID获取字段信息
        def get_field_by_id(field_id: str) -> dict:
            return field_id_map.get(field_id)
        
        # 如果指定了关联字段，使用它们
        if join_keys:
            processed_keys = set()  # 避免重复处理
            
            for key_name in join_keys:
                if key_name in processed_keys:
                    continue
                    
                # 检查是否是UUID格式
                if is_uuid(key_name):
                    field_info = get_field_by_id(key_name)
                    if not field_info:
                        logger.warning(f"multi_join 未找到字段ID: {key_name}")
                        continue
                    
                    main_table_id = field_info["table_id"]
                    display_name = field_info["display_name"]
                    main_column = field_info["column"]
                    
                    # 检查 cross_table_field_mappings 是否有异名字段映射
                    if key_name in cross_table_field_mappings:
                        # 有异名字段映射，使用它
                        mapping = {"display_name": display_name}
                        mapping[main_table_id] = main_column
                        
                        for other_table_id, other_field_id in cross_table_field_mappings[key_name].items():
                            other_field_info = get_field_by_id(other_field_id)
                            if other_field_info:
                                mapping[other_table_id] = other_field_info["column"]
                                # 更新显示名以反映异名关联
                                other_display = other_field_info["display_name"]
                                if other_display != display_name:
                                    mapping["display_name"] = f"{display_name}={other_display}"
                                # 标记已处理
                                processed_keys.add(other_field_id)
                            else:
                                logger.warning(f"multi_join 跨表映射中未找到字段: {other_field_id}")
                        
                        if len(mapping) > 2:  # display_name + 至少2个表
                            result.append(mapping)
                            logger.info(
                                f"multi_join 通过 cross_table_field_mappings 建立字段映射: {mapping}"
                            )
                        processed_keys.add(key_name)
                        continue
                    
                    # 没有异名映射，尝试同名字段
                    mapping = {"display_name": display_name}
                    mapping[main_table_id] = main_column
                    
                    for table_id in table_ids:
                        if table_id != main_table_id:
                            column = find_column_in_table(display_name, table_id)
                            if column:
                                mapping[table_id] = column
                    
                    if len(mapping) > 2:
                        result.append(mapping)
                        logger.debug(f"multi_join UUID字段映射: {display_name} -> {len(mapping)-1}个表")
                    else:
                        logger.warning(
                            f"multi_join UUID字段 '{display_name}' 只在1个表中找到，尝试使用 cross_table_field_mappings"
                        )
                    
                    processed_keys.add(key_name)
                    continue
                
                # 检查是否是异名字段关联格式: "字段A->字段B"
                if "->" in key_name:
                    parts = key_name.split("->")
                    if len(parts) == 2 and len(table_ids) >= 2:
                        field1_name = parts[0].strip()
                        field2_name = parts[1].strip()
                        
                        # 表1 用 field1_name，表2 用 field2_name
                        table1_id = table_ids[0]
                        table2_id = table_ids[1]
                        
                        col1 = find_column_in_table(field1_name, table1_id)
                        col2 = find_column_in_table(field2_name, table2_id)
                        
                        if col1 and col2:
                            mapping = {
                                "display_name": f"{field1_name}={field2_name}",
                                table1_id: col1,
                                table2_id: col2
                            }
                            result.append(mapping)
                            logger.info(
                                f"multi_join 异名字段映射: {field1_name}({col1}) <-> {field2_name}({col2})"
                            )
                        else:
                            logger.warning(
                                f"multi_join 异名字段映射失败: {field1_name}={col1}, {field2_name}={col2}"
                            )
                    continue
                
                # 同名字段关联：所有表使用相同的字段名
                mapping = {"display_name": key_name}
                for table_id in table_ids:
                    column = find_column_in_table(key_name, table_id)
                    if column:
                        mapping[table_id] = column
                
                # 至少有2个表有这个字段才添加
                if len(mapping) > 2:  # display_name + 至少2个表
                    result.append(mapping)
                    logger.debug(f"multi_join 字段映射: {key_name} -> {len(mapping)-1}个表")
                else:
                    # 记录警告：字段未能在足够多的表中找到
                    matched_tables = [tid for tid in table_ids if tid in mapping]
                    logger.warning(
                        f"multi_join 字段 '{key_name}' 只在 {len(matched_tables)}/{len(table_ids)} 个表中找到，跳过"
                    )
        
        # 如果没有指定或没有找到，尝试自动推断公共字段
        if not result:
            # 查找所有表都有的公共字段
            if table_fields_map:
                first_table_id = table_ids[0]
                if first_table_id in table_fields_map:
                    common_names = set(table_fields_map[first_table_id].keys())
                    for table_id in table_ids[1:]:
                        if table_id in table_fields_map:
                            common_names &= set(table_fields_map[table_id].keys())
                    
                    # 优先选择标识类字段
                    priority_keywords = ["编号", "代码", "行政区", "区域", "项目", "地块"]
                    for name in common_names:
                        for keyword in priority_keywords:
                            if keyword in name:
                                mapping = {"display_name": name}
                                for table_id in table_ids:
                                    if table_id in table_fields_map:
                                        mapping[table_id] = table_fields_map[table_id][name]
                                result.append(mapping)
                                logger.debug(f"multi_join 推断字段映射: {name}")
                                break
        
        return result

    def _convert_join_field_names_to_ids(
        self,
        field_names: List[str],
        table_ids: List[str]
    ) -> List[str]:
        """
        将 JOIN 关联字段的显示名转换为字段ID（统一使用ID传递）
        
        Args:
            field_names: 关联字段的显示名列表（如["行政区", "地类名称"]）
            table_ids: 参与关联的表ID列表
        
        Returns:
            字段ID列表
        """
        if not self.semantic_model or not field_names or not table_ids:
            return field_names  # 无法转换时返回原值
        
        result = []
        fields = getattr(self.semantic_model, "fields", {}) or {}
        
        def field_matches_name(field, name: str) -> bool:
            """检查字段是否匹配给定名称"""
            # 精确匹配 display_name
            if field.display_name == name:
                return True
            # 检查 synonyms
            if hasattr(field, 'synonyms') and field.synonyms:
                if name in field.synonyms:
                    return True
            # 模糊匹配：display_name 中包含该名称
            if name in field.display_name:
                return True
            return False
        
        for name in field_names:
            found_id = None
            # 优先在指定的表中查找
            for table_id in table_ids:
                for field_id, field in fields.items():
                    if (getattr(field, 'datasource_id', None) == table_id and 
                        field_matches_name(field, name)):
                        found_id = field_id
                        logger.debug(
                            f"JOIN字段转换: '{name}' -> field_id={field_id}, table={table_id}"
                        )
                        break
                if found_id:
                    break
            
            if found_id:
                result.append(found_id)
            else:
                # 无法转换时保留原名（编译器会尝试模糊匹配）
                result.append(name)
                logger.warning(
                    f"无法将JOIN字段 '{name}' 转换为字段ID，保留原名"
                )
        
        return result

    def _normalize_cross_table_field_mappings(
        self,
        mappings: Dict[str, Dict[str, str]],
        table_ids: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """
        规范化跨表字段映射：把表显示名转换为表UUID
        
        LLM可能输出表显示名作为key，但编译器期望的是表UUID。
        
        Args:
            mappings: LLM输出的原始映射 {字段UUID: {表显示名或UUID: 字段UUID}}
            table_ids: 参与查询的表ID列表
        
        Returns:
            规范化后的映射 {字段UUID: {表UUID: 字段UUID}}
        """
        if not mappings or not self.semantic_model:
            return mappings
        
        # 构建显示名到UUID的映射
        display_name_to_uuid: Dict[str, str] = {}
        for table_id in table_ids:
            ds = self.semantic_model.datasources.get(table_id)
            if ds:
                # 同时映射显示名和物理表名
                if hasattr(ds, 'display_name') and ds.display_name:
                    display_name_to_uuid[ds.display_name] = table_id
                if hasattr(ds, 'datasource_name') and ds.datasource_name:
                    display_name_to_uuid[ds.datasource_name] = table_id
        
        # 规范化映射
        normalized: Dict[str, Dict[str, str]] = {}
        for field_id, table_mappings in mappings.items():
            normalized_table_mappings: Dict[str, str] = {}
            for table_key, mapped_field_id in table_mappings.items():
                # 尝试转换：如果是显示名则转为UUID，否则保持原样
                actual_table_id = display_name_to_uuid.get(table_key, table_key)
                normalized_table_mappings[actual_table_id] = mapped_field_id
                
                if table_key != actual_table_id:
                    logger.debug(
                        "跨表字段映射规范化",
                        field_id=field_id,
                        original_table_key=table_key,
                        normalized_table_id=actual_table_id
                    )
            
            if normalized_table_mappings:
                normalized[field_id] = normalized_table_mappings
        
        return normalized

    def _build_cross_table_field_mappings(
        self,
        ir: IntermediateRepresentation,
        table_ids: List[str],
        primary_table_id: Optional[str] = None
    ) -> Dict[str, Dict[str, str]]:
        """
        构建跨表字段映射：主表字段UUID -> {其他表ID: 对应字段UUID}
        
        用于编译器在多表查询时精确查找其他表的对应字段，避免模糊匹配。
        
        Args:
            ir: 中间表示对象
            table_ids: 参与查询的表ID列表
            primary_table_id: 主表ID（默认使用第一个表）
        
        Returns:
            映射字典：{主表字段UUID: {其他表ID: 对应字段UUID}}
        """
        if not self.semantic_model or not table_ids or len(table_ids) < 2:
            return {}
        
        primary_table_id = primary_table_id or table_ids[0]
        other_table_ids = [tid for tid in table_ids if tid != primary_table_id]
        
        if not other_table_ids:
            return {}
        
        # 构建 datasource_id/display_name 到实际表ID的映射
        # 因为 field.datasource_id 可能存储的是显示名而不是UUID
        datasource_to_table_id: Dict[str, str] = {}
        for table_id in table_ids:
            datasource = self.semantic_model.datasources.get(table_id) if self.semantic_model.datasources else None
            if datasource:
                # 同时映射 datasource_id 和 display_name
                datasource_to_table_id[table_id] = table_id  # UUID -> UUID
                if hasattr(datasource, 'datasource_id'):
                    datasource_to_table_id[datasource.datasource_id] = table_id
                if hasattr(datasource, 'display_name') and datasource.display_name:
                    datasource_to_table_id[datasource.display_name] = table_id
                if hasattr(datasource, 'datasource_name') and datasource.datasource_name:
                    datasource_to_table_id[datasource.datasource_name] = table_id
        
        def get_table_id_for_field(field) -> Optional[str]:
            """获取字段所属的实际表ID"""
            field_ds_id = getattr(field, 'datasource_id', None)
            if not field_ds_id:
                return None
            return datasource_to_table_id.get(field_ds_id)
        
        # 收集需要映射的主表字段UUID
        primary_field_ids: set = set()
        
        # 1. 从 filters 收集
        for flt in (ir.filters or []):
            try:
                field_id = str(flt.field)
                field = self.semantic_model.fields.get(field_id)
                if field and get_table_id_for_field(field) == primary_table_id:
                    primary_field_ids.add(field_id)
            except Exception:
                continue
        
        # 2. 从 compare_join_keys 收集（已转换为UUID的）
        for field_id in (ir.compare_join_keys or []):
            if field_id and not field_id.startswith("derived:"):
                field = self.semantic_model.fields.get(field_id)
                if field and get_table_id_for_field(field) == primary_table_id:
                    primary_field_ids.add(field_id)
        
        # 3. 从 metrics 收集（跨分区对比时需要度量字段的映射）
        for metric_item in (ir.metrics or []):
            # 提取字段ID（兼容字符串和 MetricSpec 格式）
            metric_id = metric_item if isinstance(metric_item, str) else (
                metric_item.get("field") if isinstance(metric_item, dict) else 
                getattr(metric_item, "field", str(metric_item))
            )
            if metric_id and not str(metric_id).startswith("derived:") and metric_id != "__row_count__":
                field = self.semantic_model.fields.get(metric_id)
                if field and get_table_id_for_field(field) == primary_table_id:
                    primary_field_ids.add(metric_id)
        
        # 4. 从 dimensions 收集（跨分区对比时也可能需要维度字段的映射）
        for dim_id in (ir.dimensions or []):
            if dim_id and not dim_id.startswith("derived:"):
                field = self.semantic_model.fields.get(dim_id)
                if field and get_table_id_for_field(field) == primary_table_id:
                    primary_field_ids.add(dim_id)
        
        if not primary_field_ids:
            return {}
        
        # 构建映射
        mappings: Dict[str, Dict[str, str]] = {}
        fields = self.semantic_model.fields or {}
        
        def field_matches(field, target_field) -> bool:
            """检查字段是否与目标字段匹配（基于显示名、同义词和物理列名）"""
            target_name = target_field.display_name
            target_synonyms = getattr(target_field, 'synonyms', []) or []
            target_column = getattr(target_field, 'column', None)
            
            field_name = field.display_name
            field_synonyms = getattr(field, 'synonyms', []) or []
            field_column = getattr(field, 'column', None)
            
            # 1. 显示名精确匹配
            if field_name == target_name:
                return True
            
            # 2. 物理列名精确匹配（忽略大小写）
            # 这对于跨年表中"TBMJ"(2023) vs "图斑面积"(2024但物理列名都是TBMJ)的情况很重要
            if target_column and field_column:
                if target_column.upper() == field_column.upper():
                    return True
            
            # 3. 同义词匹配
            if target_name in field_synonyms or field_name in target_synonyms:
                return True
            
            # 4. 物理列名作为同义词匹配（显示名可能是中文，物理列名可能是英文缩写）
            if target_column:
                if target_column.upper() == field_name.upper():
                    return True
                if target_column.upper() in [s.upper() for s in field_synonyms]:
                    return True
            if field_column:
                if field_column.upper() == target_name.upper():
                    return True
                if field_column.upper() in [s.upper() for s in target_synonyms]:
                    return True
            
            # 5. 提取括号内关键词进行匹配
            import re
            def extract_keywords(name: str) -> List[str]:
                keywords = [name]
                paren_matches = re.findall(r'[（(]([^）)]+)[）)]', name)
                for match in paren_matches:
                    parts = re.split(r'[/／、,，]', match)
                    keywords.extend([p.strip() for p in parts if p.strip()])
                return keywords
            
            target_keywords = extract_keywords(target_name)
            for synonym in target_synonyms:
                target_keywords.extend(extract_keywords(synonym))
            # 物理列名也作为关键词
            if target_column:
                target_keywords.append(target_column.upper())
            
            field_keywords = extract_keywords(field_name)
            for synonym in field_synonyms:
                field_keywords.extend(extract_keywords(synonym))
            # 物理列名也作为关键词
            if field_column:
                field_keywords.append(field_column.upper())
            
            # 检查关键词交集
            if set(k.upper() for k in target_keywords) & set(k.upper() for k in field_keywords):
                return True
            
            return False
        
        for primary_field_id in primary_field_ids:
            primary_field = fields.get(primary_field_id)
            if not primary_field:
                continue
            
            other_mappings: Dict[str, str] = {}
            
            for other_table_id in other_table_ids:
                # 在其他表中查找匹配字段
                for field_id, field in fields.items():
                    # 使用转换函数获取字段的实际表ID
                    field_actual_table_id = get_table_id_for_field(field)
                    if field_actual_table_id == other_table_id and field_matches(field, primary_field):
                        other_mappings[other_table_id] = field_id
                        logger.debug(
                            "跨表字段映射",
                            primary_field_id=primary_field_id,
                            primary_field_name=primary_field.display_name,
                            other_table_id=other_table_id,
                            mapped_field_id=field_id,
                            mapped_field_name=field.display_name
                        )
                        break
            
            if other_mappings:
                mappings[primary_field_id] = other_mappings
        
        return mappings

    def _maybe_override_primary_table_by_ir(self, ir: IntermediateRepresentation, hierarchical_result) -> IntermediateRepresentation:
        """
        兼容“表检索 Top1 不稳定”的现实：LLM 虽然看到 TOP3 表，但可能用到非 Top1 表的字段。
        这里对 TOP3 候选表做一个轻量投票：
        - 统计 IR 中 metrics/dimensions/filters 引用字段所属 datasource_id 的次数
        - 若某个表在 TOP3 内获得明显多数票（默认>=60%），则覆盖 ir.primary_table_id
        这样能提升多数据源/多数据库场景下的鲁棒性，避免后续编译/权限校验走错表。
        """
        if not self.semantic_model or not hierarchical_result or not getattr(hierarchical_result, "table_results", None):
            return ir
        top_tables = [tr.table_id for tr in (hierarchical_result.table_results or [])[:3] if getattr(tr, "table_id", None)]
        if not top_tables:
            return ir

        # 收集 IR 引用的 field_id
        field_ids: list[str] = []
        for metric_item in (ir.metrics or []):
            # 提取字段ID（兼容字符串和 MetricSpec 格式）
            fid = metric_item if isinstance(metric_item, str) else (
                metric_item.get("field") if isinstance(metric_item, dict) else 
                getattr(metric_item, "field", None)
            )
            if fid and isinstance(fid, str) and not fid.startswith("derived:"):
                field_ids.append(fid)
        for fid in (ir.dimensions or []):
            if isinstance(fid, str):
                field_ids.append(fid)
        for flt in (ir.filters or []):
            try:
                field_ids.append(str(flt.field))
            except Exception:
                continue

        if not field_ids:
            return ir

        votes: dict[str, int] = {}
        total = 0
        for fid in field_ids:
            fld = (getattr(self.semantic_model, "fields", {}) or {}).get(str(fid))
            if not fld:
                continue
            table_id = str(getattr(fld, "datasource_id", "") or "")
            if not table_id or table_id not in top_tables:
                continue
            votes[table_id] = votes.get(table_id, 0) + 1
            total += 1

        if total <= 0 or not votes:
            return ir

        best_table, best_votes = max(votes.items(), key=lambda kv: kv[1])
        ratio = best_votes / max(1, total)
        if ratio >= 0.6 and str(getattr(ir, "primary_table_id", "") or "") != best_table:
            old = getattr(ir, "primary_table_id", None)
            ir.primary_table_id = best_table
            logger.info(
                "主表投票校正：覆盖 primary_table_id",
                old_primary=old,
                new_primary=best_table,
                ratio=round(ratio, 3),
                votes=votes,
            )
        return ir

    def _calculate_confidence(
        self,
        ir: IntermediateRepresentation,
        hierarchical_result
    ) -> float:
        """
        计算置信度（基于层次化检索结果）

        Args:
            ir: 生成的 IR
            hierarchical_result: 层次化检索结果

        Returns:
            置信度分数 (0-1)
        """
        # 从 IR 中获取的置信度作为基础
        score = ir.confidence if ir.confidence else 0.8

        # 检查是否有歧义标记
        if ir.ambiguities:
            score *= 0.8

        # 检查时间范围合理性
        if ir.time and ir.time.type == "relative":
            if ir.time.last_n and ir.time.last_n > 365:
                score *= 0.85  # 超过一年可能有误

        # 检查是否成功识别业务域
        if hierarchical_result.domain_id:
            score *= 1.05  # 明确识别业务域，提升置信度
        else:
            score *= 0.9   # 未识别业务域，降低置信度

        # 检查是否检索到相关表
        if hierarchical_result.table_structures:
            table_count = len(hierarchical_result.table_structures)
            if table_count >= 1:
                score *= 1.02  # 检索到表，提升置信度
            if table_count > 3:
                score *= 0.95  # 表太多可能不够精确
        else:
            score *= 0.7  # 未检索到表，大幅降低置信度

        return min(max(score, 0.0), 1.0)  # 确保在 0-1 范围内

    def _build_ir_from_sample(
        self,
        decision: FewShotDirectDecision,
        hierarchical_result,
        question: str,
        detected_domain_id,
        domain_name: Optional[str],
    ) -> Optional[IntermediateRepresentation]:
        sample = decision.sample
        if not sample.ir_json:
            logger.debug("Few-Shot直连执行跳过：缺少IR", sample=sample.question)
            return None
        try:
            payload = json.loads(sample.ir_json) if isinstance(sample.ir_json, str) else sample.ir_json
        except json.JSONDecodeError as exc:
            logger.warning("Few-Shot直连执行失败：IR解析错误", error=str(exc))
            return None
        if not isinstance(payload, dict):
            logger.warning("Few-Shot直连执行失败：IR格式异常", sample=sample.question)
            return None

        payload.setdefault("original_question", question)
        payload = self._fix_common_format_errors(payload)

        try:
            ir = IntermediateRepresentation(**payload)
        except ValidationError as exc:
            logger.warning("Few-Shot直连执行失败：IR构建异常", error=str(exc))
            return None

        try:
            ir = self.validator.validate_and_fix(ir)
            self.last_validation_notes = self.validator.get_notes()
        except Exception:
            self.last_validation_notes = None

        if sample.domain_id:
            ir.domain_id = str(sample.domain_id)
        elif detected_domain_id:
            ir.domain_id = str(detected_domain_id)
        else:
            ir.domain_id = None
        ir.domain_name = domain_name
        ir.original_question = question
        ir.confidence = sample.quality_score or RetrievalConfig.few_shot_direct_min_quality()

        try:
            if hasattr(ir, "primary_table_id") and hierarchical_result.table_results:
                ir.primary_table_id = hierarchical_result.table_results[0].table_id
        except Exception as exc:
            logger.debug("设置直连IR主表失败", error=str(exc))

        return ir

    def _mark_direct_execution(
        self,
        decision: FewShotDirectDecision,
        hierarchical_result,
    ) -> None:
        trace_payload = decision.to_trace()

        table_info = hierarchical_result.table_retrieval_info or {}
        table_info = dict(table_info)
        few_shot_meta = dict(table_info.get("few_shot") or {})
        few_shot_meta["direct_candidate"] = trace_payload
        table_info["few_shot"] = few_shot_meta
        hierarchical_result.table_retrieval_info = table_info

        summary = dict(self.last_retrieval_summary or {})
        summary["few_shot_direct"] = trace_payload
        self.last_retrieval_summary = summary

    def _record_direct_decision_debug(
        self,
        hierarchical_result,
        decision_debug: Optional[Dict[str, Any]],
    ) -> None:
        if not decision_debug:
            return

        table_info = dict(hierarchical_result.table_retrieval_info or {})
        few_shot_meta = dict(table_info.get("few_shot") or {})
        few_shot_meta["decision_debug"] = decision_debug
        table_info["few_shot"] = few_shot_meta
        hierarchical_result.table_retrieval_info = table_info

        summary = dict(self.last_retrieval_summary or {})
        summary["few_shot_direct_debug"] = decision_debug
        self.last_retrieval_summary = summary

    def _build_retrieval_summary(
        self,
        hierarchical_result,
        detected_domain_id,
        detected_domain_name,
        user_domain_id,
        enum_matches,
        enum_triples,
        candidate_fields,
        enum_suggestions,
        enum_prompt_metadata,
        global_rule_count,
        user_prompt: Optional[str],
        system_prompt: Optional[str]
    ) -> Dict[str, Any]:
        """构建检索阶段的概览，便于写入 trace。"""

        def _safe_table_name(datasource):
            if not datasource:
                return None
            return (
                getattr(datasource, "display_name", None)
                or getattr(datasource, "datasource_name", None)
                or getattr(datasource, "table_name", None)
            )

        table_summaries = []
        for idx, table_result in enumerate(hierarchical_result.table_results or []):
            # 获取枚举回流加权trace
            enum_boost_trace = getattr(table_result, "enum_boost_trace", None)
            rescue_reason = getattr(table_result, "rescue_reason", None)
            
            table_summaries.append({
                "rank": idx + 1,
                "table_id": table_result.table_id,
                "table_name": _safe_table_name(table_result.datasource),
                "score": round(table_result.score or 0, 4),
                "field_count": table_result.field_count,
                "dense_score": round(table_result.dense_score, 4) if table_result.dense_score is not None else None,
                "sparse_score": round(table_result.sparse_score, 4) if table_result.sparse_score is not None else None,
                "rrf_score": round(table_result.rrf_score, 4) if table_result.rrf_score is not None else None,
                "reranker_score": round(table_result.reranker_score, 4) if table_result.reranker_score is not None else None,
                # Trace增强：枚举回流加权
                "enum_boost_trace": enum_boost_trace,
                # Trace增强：表救援原因
                "rescue_reason": rescue_reason,
            })

        structure_summaries = []
        for structure in hierarchical_result.table_structures or []:
            structure_summaries.append({
                "table_id": structure.table_id,
                "display_name": structure.display_name,
                "table_name": structure.table_name,
                "schema_name": getattr(structure, "schema_name", None),
                "physical_table_name": getattr(structure, "physical_table_name", None),
                "domain_name": structure.domain_name,
                "dimensions": len(structure.dimensions),
                "measures": len(structure.measures),
                "identifiers": len(structure.identifiers),
                "timestamps": len(structure.timestamps),
                "geometries": len(structure.geometries),
                "total_fields": structure.total_fields,
                "tags": list(structure.tags or []),
                "aliases": list(structure.aliases or [])
            })

        enum_summary = []
        for match in enum_matches[:10]:  # 增加到10条以便调试
            # 增强Trace：记录更多枚举匹配详情
            trace = getattr(match, "trace", {}) or {}
            enum_summary.append({
                "field": match.field_name,
                "field_id": match.field_id,
                "value": match.value,
                "display_name": match.display_name,
                "score": round(
                    (match.final_score if match.final_score is not None else match.similarity or 0), 4
                ),
                "similarity": round(match.similarity or 0, 4) if match.similarity else None,
                "match_type": match.match_type,
                "table_name": getattr(match, "table_name", None),
                "table_id": getattr(match, "table_id", None),
                "domain_id": getattr(match, "domain_id", None),
                # Trace增强：放行/拒绝理由
                "pass_reason": trace.get("pass_reason"),
                "reject_reason": trace.get("reject_reason"),
                # Trace增强：关键词命中
                "keyword_hit": trace.get("field_keyword_hit"),
                "enum_keyword_hit": trace.get("enum_keyword_hit"),
                # Trace增强：RRF来源
                "rrf_sources": getattr(match, "rrf_sources", None),
                # Trace增强：负向信号
                "negative_penalty": trace.get("negative_signal_penalty"),
                "deduplicated": trace.get("deduplicated"),
            })

        enum_triple_summary = []
        for triple in (enum_triples or [])[:5]:
            enum_triple_summary.append({
                "table_id": triple.get("table_id"),
                "table_name": triple.get("table_name"),
                "field_id": triple.get("field_id"),
                "field_name": triple.get("field_name"),
                "value": triple.get("value"),
                "final_score": triple.get("final_score"),
                "trace": triple.get("trace"),
            })

        few_shot_summary = []
        for idx, sample in enumerate(hierarchical_result.few_shot_examples or []):
            sql_preview = (sample.sql[:160] + "…") if sample.sql and len(sample.sql) > 160 else sample.sql
            few_shot_summary.append({
                "rank": idx + 1,
                "question": sample.question,
                "score": round(sample.score or 0, 4),
                "final_score": round(sample.score or 0, 4),
                "dense_rank": sample.dense_rank,
                "final_rank": sample.final_rank,
                "tables": sample.tables,
                "source_tag": sample.source_tag,
                "sql": sql_preview,
                "reranker_score": round(sample.reranker_score, 4) if sample.reranker_score is not None else None,
                "dense_score": round(sample.dense_score, 4) if sample.dense_score is not None else None,
                "raw_similarity": round(sample.raw_similarity, 4) if sample.raw_similarity is not None else None,
            })

        prompt_preview = (
            user_prompt[:400] + "…"
            if user_prompt and len(user_prompt) > 400
            else user_prompt
        )

        domain_candidates_summary = getattr(hierarchical_result, "domain_candidates", None) or []
        direct_candidates_summary = []
        direct_candidates = getattr(
            hierarchical_result,
            "few_shot_direct_candidates",
            None,
        ) or []
        for idx, sample in enumerate(direct_candidates):
            sql_preview = (
                sample.sql[:160] + "…"
                if sample.sql and len(sample.sql) > 160
                else sample.sql
            )
            direct_candidates_summary.append(
                {
                    "rank": idx + 1,
                    "question": sample.question,
                    "score": round(sample.score or 0, 4),
                    "dense_rank": sample.dense_rank,
                    "tables": sample.tables,
                    "source_tag": sample.source_tag,
                    "sql": sql_preview,
                    "raw_similarity": round(sample.raw_similarity, 4)
                    if sample.raw_similarity is not None
                    else None,
                    "quality_score": round(sample.quality_score, 4)
                    if sample.quality_score is not None
                    else None,
                }
            )

        # ========== 新增：主表和表候选详情 ==========
        main_table_info = None
        table_candidates_detail = []
        metric_table_map = {}
        filter_table_map = {}
        
        # 辅助函数：从 TableRetrievalResult 获取表名
        def _get_table_name(tr) -> str:
            """从 TableRetrievalResult 获取表名"""
            if hasattr(tr, 'datasource') and tr.datasource:
                return tr.datasource.display_name or tr.datasource.datasource_name or tr.table_id
            return tr.table_id
        
        # 获取主表信息（排序后第一个表）
        if hierarchical_result.table_results:
            main_table_result = hierarchical_result.table_results[0]
            main_table_info = {
                "table_id": main_table_result.table_id,
                "table_name": _get_table_name(main_table_result),
                "score": round(main_table_result.score or 0, 4),
            }
            
            # 构建候选表详情
            for idx, tr in enumerate(hierarchical_result.table_results):
                table_detail = {
                    "rank": idx + 1,
                    "table_id": tr.table_id,
                    "table_name": _get_table_name(tr),
                    "score": round(tr.score or 0, 4),
                    "is_main_table": idx == 0,
                    "score_breakdown": {},
                    "enum_availability": [],
                    "measure_availability": [],
                }
                # 获取得分构成（如果有enum_boost_trace）
                if hasattr(tr, 'enum_boost_trace') and tr.enum_boost_trace:
                    table_detail["score_breakdown"] = tr.enum_boost_trace
                
                # 获取该表的枚举匹配
                table_enum_matches = [
                    m for m in enum_matches 
                    if getattr(m, 'table_id', None) == tr.table_id
                ]
                table_detail["enum_availability"] = [
                    {"field": m.field_name, "value": m.value, "score": round(m.final_score, 4)}
                    for m in table_enum_matches[:3]
                ]
                
                # 获取该表的度量字段
                for struct in hierarchical_result.table_structures or []:
                    if struct.table_id == tr.table_id:
                        table_detail["measure_availability"] = [
                            {"name": m.display_name, "id": getattr(m, 'field_id', None)}
                            for m in (struct.measures or [])[:5]
                        ]
                        break
                
                table_candidates_detail.append(table_detail)
        
        # 构建度量→表映射和过滤→表映射
        for struct in hierarchical_result.table_structures or []:
            for measure in (struct.measures or []):
                field_id = getattr(measure, 'field_id', None)
                if field_id:
                    metric_table_map[field_id] = {
                        "table_id": struct.table_id,
                        "table_name": struct.table_name,
                        "metric_name": measure.display_name,
                    }
        
        # 构建过滤字段→表映射（从枚举匹配中提取）
        for match in enum_matches:
            field_id = match.field_id
            if field_id and field_id not in filter_table_map:
                filter_table_map[field_id] = {
                    "table_id": getattr(match, 'table_id', None),
                    "table_name": getattr(match, 'table_name', None),
                    "field_name": match.field_name,
                }
        
        return {
            "domain": {
                "user_specified": user_domain_id,
                "detected_id": str(detected_domain_id) if detected_domain_id else None,
                "detected_name": detected_domain_name,
                "detection_method": getattr(hierarchical_result, "domain_detection_method", None),
                "candidates": domain_candidates_summary,
                "fallback_reason": getattr(hierarchical_result, "domain_fallback_reason", None),
            },
            "global_rule_count": global_rule_count,
            # ========== 新增：主表信息 ==========
            "main_table": main_table_info,
            "table_candidates": table_candidates_detail,
            "metric_table_map": metric_table_map,
            "filter_table_map": filter_table_map,
            # ========== 原有字段 ==========
            "tables": table_summaries,
            "table_structures": structure_summaries,
            "enum_matches": enum_summary,
            "enum_triples": enum_triple_summary,
            "enum_prompt": enum_prompt_metadata or {},
            "few_shot_examples": few_shot_summary,
            "few_shot_direct_candidates": direct_candidates_summary,
            "enum_stats": {
                "candidate_fields": len(candidate_fields),
                "match_count": len(enum_matches),
                "has_suggestions": bool(enum_suggestions)
            },
            "table_retrieval": {
                "method": getattr(hierarchical_result, "table_retrieval_method", None),
                "info": getattr(hierarchical_result, "table_retrieval_info", None),
                "keyword_fallback_used": getattr(hierarchical_result, "keyword_fallback_used", False)
            },
            "prompt_stats": {
                "user_prompt_length": len(user_prompt or ""),
                "system_prompt_length": len(system_prompt or ""),
                # 使用实际 prompt 中的表数量，而不是 table_structures 长度
                "table_count": len(re.findall(r"### 表\d+:", user_prompt or "")),
                "table_structures_count": len(hierarchical_result.table_structures or []),  # 保留原始数量用于诊断
                "query_vector_dim": getattr(hierarchical_result, "query_vector_dim", None)
            },
            "prompt_preview": prompt_preview
        }

    def _verify_exact_enum_matches(
        self,
        ir: IntermediateRepresentation,
        enum_matches: list,
        primary_table_id: Optional[str] = None,
    ):
        """
        验证精确匹配的枚举值是否被使用，如果未使用则强制添加
        
        新触发条件（收紧）：
        - 仅以下情况触发强制补滤：
          - exact/synonym 匹配
          - "明确提及 + 高分"的 value_vector（分数≥0.9）
        - 字段类型检查：必须为维度/枚举型（排除度量字段）
        - 用户意图检查：分类查询（包含"各"、"分别"、"不同"等）不强制补滤
        
        Args:
            ir: 生成的IR对象
            enum_matches: 枚举值匹配结果列表
        """
        from server.models.ir import FilterCondition
        
        # 高分阈值：value_vector匹配需达到此分数才触发强制补滤（从配置加载）
        HIGH_SCORE_THRESHOLD = RetrievalConfig.enum_force_filter_min_score()

        # 用户意图检查：分类查询不强制补滤
        # 当用户问题包含"各"、"分别"、"不同"等关键词时，用户意图可能是按该维度分组
        # 而不是过滤到特定值，此时强制补滤会改变用户意图
        original_question = getattr(ir, "original_question", "") or ""
        classification_keywords = ["各", "分别", "不同", "按", "每个", "每一个", "所有"]
        is_classification_query = any(kw in original_question for kw in classification_keywords)
        
        if is_classification_query:
            logger.debug(
                "跳过强制枚举补滤：检测到分类查询意图",
                question=original_question[:100],
                keywords_detected=[kw for kw in classification_keywords if kw in original_question]
            )
            return

        # 若未能确定主表（primary_table_id），则不做"强制补滤"。
        # 原因：在全局检索/多连接模式下，枚举可能来自其他表/其他连接；
        # 在主表未确定前强行补滤，极易引入跨连接字段，触发 CROSS_CONNECTION_NOT_SUPPORTED。
        primary_table_id = primary_table_id or getattr(ir, "primary_table_id", None)
        if not primary_table_id:
            logger.debug("跳过强制枚举补滤：primary_table_id未设置")
            return
        
        # 筛选符合强制补滤条件的枚举值
        force_add_matches = []
        for m in enum_matches:
            # 仅对齐到 IR 主表：避免跨表/跨连接 enum 信号被硬塞入 filters 导致“跨连接查询”误判
            #
            # 关键修复：
            # - 有些 EnumMatch 可能不携带 table_id（None），但 field_id 可映射回 datasource_id；
            # - 若无法确认枚举属于主表，则不要强制补滤（最多由 LLM 自然生成，或由后续链路提示）。
            if primary_table_id:
                match_table_id = getattr(m, "table_id", None)
                if not match_table_id:
                    try:
                        field_id = str(getattr(m, "field_id", "") or "")
                        if field_id and self.semantic_model and getattr(self.semantic_model, "fields", None):
                            field_obj = self.semantic_model.fields.get(field_id)
                            match_table_id = getattr(field_obj, "datasource_id", None) if field_obj else None
                    except Exception:
                        match_table_id = None

                # 能解析到 table_id 且不属于主表：直接跳过（禁止跨表强补）
                if match_table_id and str(match_table_id) != str(primary_table_id):
                    continue
                # 完全无法解析 table_id：不安全，跳过强补（避免把“行政区=长江新区”等补到错误连接的表上）
                if not match_table_id:
                    continue
            # 条件1：exact/synonym 匹配
            if m.match_type in {"exact", "synonym"}:
                force_add_matches.append(m)
                continue
            
            # 条件2：value_vector且"明确提及 + 高分"
            if m.match_type == "value_vector":
                # 检查是否被明确提及（trace中有pass_reason包含value_mentioned）
                trace = getattr(m, "trace", {}) or {}
                pass_reason = trace.get("pass_reason", "")
                value_mentioned = "value_mentioned" in pass_reason
                
                # 高分 + 明确提及
                if m.final_score >= HIGH_SCORE_THRESHOLD and value_mentioned:
                    force_add_matches.append(m)
        
        if not force_add_matches:
            return
        
        # 选择"最可信"的枚举（避免同一值在多表重复出现时误用）
        def _enum_priority(m) -> tuple:
            # 优先 exact/synonym，再优先来自 IR 主表提示的 table_id
            mt = getattr(m, "match_type", None) or ""
            mt_rank = 0 if mt in {"exact", "synonym"} else 1
            primary_match = 0
            try:
                if getattr(ir, "primary_table_id", None) and getattr(m, "table_id", None):
                    primary_match = 0 if str(m.table_id) == str(ir.primary_table_id) else 1
            except Exception:
                primary_match = 1
            score = -(float(getattr(m, "final_score", 0.0) or 0.0))
            return (mt_rank, primary_match, score)

        best_by_name_value = {}
        for m in force_add_matches:
            key = (str(getattr(m, "field_name", "") or ""), str(getattr(m, "value", "") or ""))
            if not key[0] or not key[1]:
                continue
            cur = best_by_name_value.get(key)
            if cur is None or _enum_priority(m) < _enum_priority(cur):
                best_by_name_value[key] = m

        chosen_matches = list(best_by_name_value.values())

        # 获取字段名映射（field_id -> display_name）
        field_name_map = {}
        try:
            if self.semantic_model and getattr(self.semantic_model, "fields", None):
                for fid, fld in self.semantic_model.fields.items():
                    try:
                        field_name_map[str(fid)] = getattr(fld, "display_name", None) or str(fid)
                    except Exception:
                        continue
        except Exception:
            field_name_map = {}

        # 建索引：已有 filters（支持单值/多值，包含 IN/NOT IN 列表值）
        def _iter_values(v):
            if v is None:
                return []
            if isinstance(v, list):
                return [str(x) for x in v if x is not None]
            return [str(v)]

        existing_keys = set()
        existing_by_name_value = set()
        existing_filter_values = set()  # 用于按值去重
        for f in (ir.filters or []):
            for v in _iter_values(f.value):
                existing_keys.add((str(f.field), v))
                existing_by_name_value.add((str(field_name_map.get(str(f.field), str(f.field))), v))
                existing_filter_values.add(v)
        
        # 同时检查 ratio_metrics 中的 numerator_condition 和 denominator_condition
        # 这些条件已被正确放入占比计算逻辑，不应被重复添加到全局 filters
        for rm in (ir.ratio_metrics or []):
            for cond in [getattr(rm, 'numerator_condition', None), getattr(rm, 'denominator_condition', None)]:
                if cond and hasattr(cond, 'field') and hasattr(cond, 'value'):
                    for v in _iter_values(cond.value):
                        existing_keys.add((str(cond.field), v))
                        existing_by_name_value.add((str(field_name_map.get(str(cond.field), str(cond.field))), v))
                        existing_filter_values.add(v)

        for match in chosen_matches:
            match_field_id = str(getattr(match, "field_id", "") or "")
            match_field_name = str(getattr(match, "field_name", "") or "")
            match_value = str(getattr(match, "value", "") or "")
            if not match_field_id or not match_value:
                continue

            # 检查1：按 field_id + value 精确匹配
            filter_exists_by_id = (match_field_id, match_value) in existing_keys
            # 检查2：按字段名 + value 语义匹配
            filter_exists_by_name = (match_field_name, match_value) in existing_by_name_value
            # 检查3：值已存在（保守策略）
            value_exists = match_value in existing_filter_values
            
            if filter_exists_by_id or filter_exists_by_name or value_exists:
                logger.debug(
                    "跳过重复过滤条件",
                    field=match_field_name,
                    value=match_value,
                    exists_by_id=filter_exists_by_id,
                    exists_by_name=filter_exists_by_name,
                    value_exists=value_exists
                )
                continue

            # 若 IR 已经用"同名字段 + 同值"写了过滤，但 field_id 不同：优先修正到 enum_match 的 field_id
            rewrote = False
            for f in (ir.filters or []):
                if match_value not in _iter_values(f.value):
                    continue
                f_name = str(field_name_map.get(str(f.field), str(f.field)))
                if f_name == match_field_name and str(f.field) != match_field_id:
                    old_field = str(f.field)
                    f.field = match_field_id
                    rewrote = True
                    existing_keys.add((match_field_id, match_value))
                    existing_by_name_value.add((match_field_name, match_value))
                    try:
                        self.validator._add_note(  # type: ignore[attr-defined]
                            f"枚举纠偏：将过滤字段从 {old_field}({f_name}) 重写为 {match_field_id}({match_field_name})，值={match_value}"
                        )
                    except Exception:
                        pass
                    logger.warning(
                        "枚举纠偏：重写过滤字段到高置信枚举字段",
                        old_field=old_field,
                        new_field=match_field_id,
                        field=match_field_name,
                        value=match_value,
                    )
                    break

            if rewrote:
                continue
            
            # 记录强制补滤原因
            trigger_reason = (
                f"match_type={match.match_type}, "
                f"score={match.final_score:.3f}"
            )
            
            logger.warning(
                "LLM未使用高置信枚举值，强制添加",
                field=match.field_name,
                value=match.value,
                trigger_reason=trigger_reason
            )
            
            # 强制添加过滤条件
            if ir.filters is None:
                ir.filters = []
            
            ir.filters.append(FilterCondition(
                field=match.field_id,
                op="=",
                value=match.value
            ))

            # 更新已存在集合
            existing_keys.add((match_field_id, match_value))
            existing_by_name_value.add((match_field_name, match_value))
            try:
                self.validator._add_note(  # type: ignore[attr-defined]
                    f"枚举补滤：添加 {match_field_name}={match_value} (field_id={match_field_id}, type={match.match_type})"
                )
            except Exception:
                pass
            
            logger.debug(
                "已强制添加过滤条件",
                field=match.field_name,
                value=match.value,
                match_type=match.match_type
            )

    def _fix_common_format_errors(self, ir_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        修正常见的格式错误，提高解析成功率

        Args:
            ir_json: 原始IR JSON数据

        Returns:
            修正后的IR JSON数据
        """
        fixed_ir = ir_json.copy()

        # 修正metrics字段格式
        if "metrics" in fixed_ir:
            metrics = fixed_ir["metrics"]
            if isinstance(metrics, list):
                fixed_metrics = []
                for item in metrics:
                    if isinstance(item, dict):
                        # 检查是否为新的 MetricSpec 格式 (field + aggregation + alias)
                        if "field" in item and "aggregation" in item:
                            # 保留 MetricSpec 格式
                            fixed_metrics.append(item)
                        # 兼容旧的错误格式
                        elif "name" in item:
                            fixed_metrics.append(item["name"])
                        elif "id" in item:
                            fixed_metrics.append(item["id"])
                        else:
                            logger.warning(f"无法识别的metrics项格式: {item}")
                            continue
                    elif isinstance(item, str):
                        fixed_metrics.append(item)
                    else:
                        logger.warning(f"metrics项类型错误: {type(item)}, 值: {item}")
                        continue
                fixed_ir["metrics"] = fixed_metrics

                if fixed_metrics != metrics:
                    logger.debug(f"已修正metrics格式: {metrics} → {fixed_metrics}")

        # 修正dimensions字段格式
        if "dimensions" in fixed_ir:
            dimensions = fixed_ir["dimensions"]
            if isinstance(dimensions, list):
                fixed_dimensions = []
                for item in dimensions:
                    if isinstance(item, dict):
                        # 如果是字典格式，提取name字段
                        if "name" in item:
                            fixed_dimensions.append(item["name"])
                        elif "id" in item:
                            fixed_dimensions.append(item["id"])
                        else:
                            # 尝试将整个字典转换为字符串
                            logger.warning(f"无法识别的dimensions项格式: {item}")
                            continue
                    elif isinstance(item, str):
                        fixed_dimensions.append(item)
                    else:
                        logger.warning(f"dimensions项类型错误: {type(item)}, 值: {item}")
                        continue
                fixed_ir["dimensions"] = fixed_dimensions

                if fixed_dimensions != dimensions:
                    logger.debug(f"已修正dimensions格式: {dimensions} → {fixed_dimensions}")

        # 修正filters字段格式（如果需要）
        if "filters" in fixed_ir and isinstance(fixed_ir["filters"], list):
            fixed_filters = []
            for filter_item in fixed_ir["filters"]:
                if isinstance(filter_item, dict):
                    # 确保包含必要的字段
                    if "field" in filter_item and "op" in filter_item:
                        # 如果缺少value字段，设置默认值
                        if "value" not in filter_item:
                            filter_item["value"] = None
                        fixed_filters.append(filter_item)
                    else:
                        logger.warning(f"filter项缺少必要字段: {filter_item}")
                else:
                    logger.warning(f"filter项格式错误: {type(filter_item)}, 值: {filter_item}")
            fixed_ir["filters"] = fixed_filters

        # 修正order_by字段格式（兼容模型将其序列化为字符串等情况）
        if "order_by" in fixed_ir:
            order_by = fixed_ir["order_by"]

            # 情况1：整个数组被当成字符串返回（例如 "\\n[{\"field\": \"id\", \"desc\": true}]\\n"）
            if isinstance(order_by, str):
                raw_value = order_by
                try:
                    parsed = json.loads(order_by.strip())
                    order_by = parsed
                except Exception:
                    logger.warning(
                        "order_by 字段为字符串且 JSON 解析失败，将忽略该排序配置",
                        value_preview=str(raw_value)[:200],
                    )
                    order_by = []

            # 情况2：单个对象，自动包装为数组
            if isinstance(order_by, dict):
                order_by = [order_by]

            if isinstance(order_by, list):
                fixed_list = []
                for item in order_by:
                    if isinstance(item, dict):
                        field = item.get("field")
                        if not field:
                            logger.warning("order_by 项缺少 field 字段，已跳过", item=item)
                            continue
                        desc = item.get("desc")
                        # 默认为 True（降序），与 IR 模型的默认值保持一致
                        if not isinstance(desc, bool):
                            desc = True
                        fixed_list.append({"field": field, "desc": desc})
                    elif isinstance(item, str):
                        # 兼容 ["gmv"] 这种简写
                        fixed_list.append({"field": item, "desc": True})
                    else:
                        logger.warning(
                            "order_by 项类型错误，已跳过",
                            type=type(item).__name__,
                            value=item,
                        )

                if fixed_list != order_by:
                    logger.debug(
                        "已修正order_by格式",
                        original=str(order_by)[:200],
                        fixed=str(fixed_list)[:200],
                    )
                fixed_ir["order_by"] = fixed_list
            else:
                # 类型完全异常时，直接清空以避免 Pydantic 校验失败
                logger.warning(
                    "order_by 字段类型异常，已清空",
                    type=type(order_by).__name__,
                    value=str(order_by)[:200],
                )
                fixed_ir["order_by"] = []

        # 修正复杂数组字段格式（LLM可能将数组序列化为字符串返回）
        # 涉及字段：ratio_metrics, conditional_metrics, calculated_fields, having_filters
        complex_array_fields = [
            "ratio_metrics",
            "conditional_metrics",
            "calculated_fields",
            "having_filters"
        ]

        for field_name in complex_array_fields:
            if field_name not in fixed_ir:
                continue

            field_value = fixed_ir[field_name]

            # 情况1：字段值为字符串（LLM将数组序列化为JSON字符串）
            if isinstance(field_value, str):
                raw_value = field_value
                try:
                    parsed = json.loads(field_value.strip())
                    if isinstance(parsed, list):
                        fixed_ir[field_name] = parsed
                        logger.debug(
                            f"已修正 {field_name} 格式: 字符串 → 数组",
                            original_preview=raw_value[:100] if len(raw_value) > 100 else raw_value
                        )
                    else:
                        logger.warning(
                            f"{field_name} JSON解析结果不是数组，已清空",
                            type=type(parsed).__name__
                        )
                        fixed_ir[field_name] = []
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"{field_name} 字符串 JSON 解析失败，已清空",
                        error=str(e),
                        value_preview=raw_value[:200] if len(raw_value) > 200 else raw_value
                    )
                    fixed_ir[field_name] = []

            # 情况2：字段值为单个对象（应为数组）
            elif isinstance(field_value, dict):
                fixed_ir[field_name] = [field_value]
                logger.debug(f"已修正 {field_name} 格式: 单对象 → 数组")

            # 情况3：字段值类型完全异常
            elif not isinstance(field_value, list):
                logger.warning(
                    f"{field_name} 字段类型异常，已清空",
                    type=type(field_value).__name__
                )
                fixed_ir[field_name] = []

        return fixed_ir
