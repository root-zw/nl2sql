"""
向量表选择器（第二条链路）
基于向量检索召回的TOP-K表，使用LLM3同时完成选表和IR生成

功能：
1. 从文件加载提示词模板
2. 构建包含完整字段信息的 Prompt
3. 调用 LLM3 进行表选择和IR生成
4. 置信度评估和分流决策
5. 支持多表查询判断
6. 支持全局规则和 Few-Shot 示例注入
"""

from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime
import structlog

from server.config import settings, RetrievalConfig, get_retrieval_param
from server.models.api import TableCandidate, TableSelectionCard
from server.nl2ir.llm_client import LLMClient
from server.nl2ir.table_retriever import TableRetrievalResult
from server.utils.prompt_loader import resolve_path, load_json, load_text

if TYPE_CHECKING:
    from server.nl2ir.few_shot_retriever import FewShotSample

logger = structlog.get_logger()

# 提示词文件路径（可配置）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_PROMPTS_DIR = _PROJECT_ROOT / "prompts" / "vector_table_selector"
PROMPTS_DIR = resolve_path(settings.vector_table_selector_prompts_dir, _DEFAULT_PROMPTS_DIR)
SYSTEM_PROMPT_FILE = resolve_path(
    settings.vector_table_selector_system_prompt_file, PROMPTS_DIR / "system.txt"
)
FUNCTION_SCHEMA_FILE = resolve_path(
    settings.vector_table_selector_function_schema_file, PROMPTS_DIR / "function_schema.json"
)
USER_TEMPLATE_FILE = resolve_path(
    settings.vector_table_selector_user_template_file, PROMPTS_DIR / "user_template.txt"
)


# ============================================================
# 提示词加载
# ============================================================
def _load_system_prompt() -> str:
    """从文件加载系统提示词"""
    return load_text(
        SYSTEM_PROMPT_FILE, default=_DEFAULT_SYSTEM_PROMPT, prompt_name="vector_table_selector_system"
    )


def _load_function_schema() -> Dict[str, Any]:
    """从文件加载 Function Schema"""
    return load_json(
        FUNCTION_SCHEMA_FILE,
        default=_DEFAULT_FUNCTION_SCHEMA,
        prompt_name="vector_table_selector_function_schema",
    )


def _load_user_template() -> str:
    """从文件加载用户提示词模板"""
    return load_text(
        USER_TEMPLATE_FILE, default=_DEFAULT_USER_TEMPLATE, prompt_name="vector_table_selector_user_template"
    )


# ============================================================
# 默认提示词（文件不存在时使用）
# ============================================================
_DEFAULT_SYSTEM_PROMPT = """你是一个专业的数据分析专家。根据用户问题和候选表信息，选择最相关的表并生成IR。
请严格调用 select_and_generate_ir 函数返回结果。"""

_DEFAULT_FUNCTION_SCHEMA = {
    "type": "function",
    "function": {
        "name": "select_and_generate_ir",
        "description": "选择表并生成IR",
        "parameters": {
            "type": "object",
            "properties": {
                "selected_table_id": {"type": "string"},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "selection_reason": {"type": "string"},
                "is_multi_table_query": {"type": "boolean"},
                "multi_table_mode": {"type": "string"},
                "ir": {"type": "object"}
            },
            "required": ["selected_table_id", "confidence", "selection_reason", "is_multi_table_query", "multi_table_mode", "ir"]
        }
    }
}

_DEFAULT_USER_TEMPLATE = """## 用户问题
「{question}」

## 候选数据表
{tables_section}

请选择最相关的表并生成IR。"""


# ============================================================
# 数据结构
# ============================================================
@dataclass
class FieldDetail:
    """字段详细信息"""
    field_id: str
    display_name: str
    field_type: str  # dimension, measure, identifier
    description: Optional[str] = None
    synonyms: List[str] = field(default_factory=list)
    enum_values: List[str] = field(default_factory=list)  # 枚举值示例


@dataclass
class TableWithFields:
    """包含完整字段信息的表"""
    table_id: str
    display_name: str
    description: str
    domain_name: Optional[str] = None
    domain_id: Optional[str] = None
    connection_id: Optional[str] = None
    data_year: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    # 向量检索分数
    retrieval_score: float = 0.0
    dense_score: Optional[float] = None
    sparse_score: Optional[float] = None
    reranker_score: Optional[float] = None
    
    # 字段信息
    dimensions: List[FieldDetail] = field(default_factory=list)
    measures: List[FieldDetail] = field(default_factory=list)
    identifiers: List[FieldDetail] = field(default_factory=list)
    
    @property
    def field_count(self) -> int:
        return len(self.dimensions) + len(self.measures) + len(self.identifiers)


@dataclass
class VectorSelectionResult:
    """向量表选择+IR生成结果"""
    # 选表结果
    selected_table_id: str
    selected_table_name: Optional[str] = None
    confidence: float = 0.0
    selection_reason: str = ""
    alternative_table_ids: List[str] = field(default_factory=list)
    
    # 多表查询判断
    is_multi_table_query: bool = False
    multi_table_mode: str = "single"  # single, cross_year, cross_partition, multi_join
    multi_table_hint: Optional[str] = None
    
    # IR结果
    ir: Optional[Dict[str, Any]] = None
    
    # 分流决策
    action: str = "execute"  # execute(高置信度直接执行) | confirm(需确认) | fallback(降级到LLM2)
    needs_confirmation: bool = False
    table_selection_card: Optional[TableSelectionCard] = None
    
    # Trace信息
    system_prompt: Optional[str] = None
    user_prompt: Optional[str] = None
    llm_response: Optional[Dict[str, Any]] = None
    
    # 向量检索信息
    retrieval_scores: Dict[str, float] = field(default_factory=dict)


class VectorTableSelector:
    """
    向量表选择器
    
    基于向量检索召回的TOP-K表，使用LLM3同时完成选表和IR生成
    """
    
    def __init__(self, llm_client: LLMClient):
        self.llm_client = llm_client
        
        # 加载提示词
        self.system_prompt = _load_system_prompt()
        self.function_schema = _load_function_schema()
        self.user_template = _load_user_template()
        
        # 调试信息
        self.last_system_prompt: Optional[str] = None
        self.last_user_prompt: Optional[str] = None
        self.last_response: Optional[Dict[str, Any]] = None
        self.last_result_json: Optional[Dict[str, Any]] = None
    
    async def select_and_generate_ir(
        self,
        question: str,
        tables_with_fields: List[TableWithFields],
        retrieval_hints: Optional[Dict[str, Any]] = None,
        global_rules: Optional[List[Dict[str, Any]]] = None,
        few_shot_examples: Optional[List["FewShotSample"]] = None,
    ) -> VectorSelectionResult:
        """
        选表并生成IR
        
        Args:
            question: 用户问题
            tables_with_fields: 向量检索召回的表（含完整字段信息）
            retrieval_hints: 检索辅助信息（枚举匹配、度量意图等）
            global_rules: 全局业务规则（派生指标、自定义指令等）
            few_shot_examples: Few-Shot 示例列表
        
        Returns:
            VectorSelectionResult
        """
        if not tables_with_fields:
            logger.warning("向量表选择器：无候选表")
            return VectorSelectionResult(
                selected_table_id="",
                confidence=0.0,
                selection_reason="无可用候选表",
                action="fallback"
            )
        
        # 1. 构建Prompt
        tables_section = self._build_tables_section(tables_with_fields)
        hints_section = self._build_hints_section(retrieval_hints)
        rules_section = self._build_rules_section(global_rules)
        few_shot_section = self._build_few_shot_section(few_shot_examples)
        current_time = datetime.now().strftime("%Y年%m月%d日")
        
        user_prompt = self.user_template.format(
            question=question,
            tables_section=tables_section,
            retrieval_hints=hints_section,
            global_rules=rules_section,
            few_shot_examples=few_shot_section,
            current_time=current_time
        )
        
        # 保存调试信息
        self.last_system_prompt = self.system_prompt
        self.last_user_prompt = user_prompt
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        # 2. 调用LLM
        try:
            response = await self.llm_client.chat_completion(
                messages=messages,
                tools=[self.function_schema],
                tool_choice={"type": "function", "function": {"name": "select_and_generate_ir"}}
            )
            self.last_response = response
            
            # 3. 解析结果
            result_json = self.llm_client.extract_function_call(response)
            self.last_result_json = result_json
            
            if not result_json:
                logger.warning("LLM3未返回有效结果，降级处理")
                return self._fallback_result(tables_with_fields, "LLM未返回有效结果")
            
            # 4. 构建结果
            selected_table_id = result_json.get("selected_table_id", "")
            confidence = float(result_json.get("confidence", 0.0))
            
            # 查找选中表的名称
            selected_table_name = None
            for t in tables_with_fields:
                if t.table_id == selected_table_id:
                    selected_table_name = t.display_name
                    break
            
            # 构建检索分数字典
            retrieval_scores = {
                t.table_id: t.retrieval_score
                for t in tables_with_fields
            }
            
            result = VectorSelectionResult(
                selected_table_id=selected_table_id,
                selected_table_name=selected_table_name,
                confidence=confidence,
                selection_reason=result_json.get("selection_reason", ""),
                alternative_table_ids=result_json.get("alternative_table_ids", []),
                is_multi_table_query=result_json.get("is_multi_table_query", False),
                multi_table_mode=result_json.get("multi_table_mode", "single"),
                multi_table_hint=result_json.get("multi_table_hint"),
                ir=result_json.get("ir"),
                system_prompt=self.system_prompt,
                user_prompt=user_prompt,
                llm_response=result_json,
                retrieval_scores=retrieval_scores
            )
            
            # 5. 分流决策
            result = self._evaluate_and_decide(result, tables_with_fields, question)
            
            logger.info(
                "向量表选择完成",
                selected_table=selected_table_name,
                confidence=confidence,
                action=result.action,
                is_multi_table=result.is_multi_table_query
            )
            
            return result
            
        except Exception as e:
            logger.exception("LLM3调用失败", error=str(e))
            return self._fallback_result(tables_with_fields, str(e))
    
    def _build_tables_section(self, tables: List[TableWithFields]) -> str:
        """构建候选表描述部分（包含完整字段信息）"""
        lines = []
        
        for idx, table in enumerate(tables, 1):
            lines.append(f"### 表{idx}: {table.display_name}")
            lines.append(f"- **表ID**: `{table.table_id}` ← 返回时请精确复制此UUID")
            lines.append(f"- **向量检索分数**: {table.retrieval_score:.4f}")
            
            if table.description:
                lines.append(f"- **描述**: {table.description}")
            
            if table.domain_name:
                lines.append(f"- **业务域**: {table.domain_name}")
            
            if table.data_year:
                lines.append(f"- **数据年份**: {table.data_year}")
            
            if table.tags:
                lines.append(f"- **标签**: {', '.join(table.tags)}")
            
            # 维度字段（完整信息）
            if table.dimensions:
                lines.append(f"\n#### 维度字段 ({len(table.dimensions)}个)")
                for f in table.dimensions:
                    field_line = f"  - `{f.field_id}`: **{f.display_name}**"
                    if f.description:
                        field_line += f" - {f.description}"
                    if f.synonyms:
                        field_line += f" (别名: {', '.join(f.synonyms[:3])})"
                    if f.enum_values:
                        # 只展示前5个枚举值
                        enum_preview = f.enum_values[:5]
                        if len(f.enum_values) > 5:
                            enum_preview.append(f"...共{len(f.enum_values)}个值")
                        field_line += f" [可选值: {', '.join(enum_preview)}]"
                    lines.append(field_line)
            
            # 度量字段（完整信息）
            if table.measures:
                lines.append(f"\n#### 度量字段 ({len(table.measures)}个)")
                for f in table.measures:
                    field_line = f"  - `{f.field_id}`: **{f.display_name}**"
                    if f.description:
                        field_line += f" - {f.description}"
                    if f.synonyms:
                        field_line += f" (别名: {', '.join(f.synonyms[:3])})"
                    lines.append(field_line)
            
            # 标识字段
            if table.identifiers:
                lines.append(f"\n#### 标识字段 ({len(table.identifiers)}个)")
                for f in table.identifiers:
                    field_line = f"  - `{f.field_id}`: **{f.display_name}**"
                    if f.description:
                        field_line += f" - {f.description}"
                    lines.append(field_line)
            
            lines.append(f"\n- **字段总数**: {table.field_count}")
            lines.append("")  # 空行分隔
        
        return "\n".join(lines)
    
    def _build_hints_section(self, hints: Optional[Dict[str, Any]]) -> str:
        """构建检索辅助信息"""
        if not hints:
            return "无额外检索辅助信息"
        
        lines = []
        
        # 枚举匹配信息
        if hints.get("enum_matches"):
            lines.append("**枚举值匹配**:")
            for match in hints["enum_matches"][:5]:
                lines.append(f"  - 字段「{match.get('field_name', '')}」匹配值「{match.get('value', '')}」")
        
        # 度量意图
        if hints.get("measure_intent"):
            lines.append(f"**度量意图**: {', '.join(hints['measure_intent'])}")
        
        # 时间提示
        if hints.get("time_hint"):
            lines.append(f"**时间范围**: {hints['time_hint']}")
        
        # 关键词
        if hints.get("keywords"):
            lines.append(f"**关键词**: {', '.join(hints['keywords'][:10])}")
        
        return "\n".join(lines) if lines else "无额外检索辅助信息"
    
    def _build_rules_section(self, global_rules: Optional[List[Dict[str, Any]]]) -> str:
        """
        构建全局业务规则区块
        
        Args:
            global_rules: 全局规则列表
            
        Returns:
            格式化的规则文本
        """
        if not global_rules:
            return "无全局业务规则"
        
        lines = []
        
        # 按类型分组
        derived_metrics = []
        custom_instructions = []
        
        for rule in global_rules:
            rule_type = rule.get('rule_type')
            if rule_type == 'derived_metric':
                derived_metrics.append(rule)
            elif rule_type == 'custom_instruction':
                custom_instructions.append(rule)
        
        # 1. 派生指标
        if derived_metrics:
            lines.append("### 🎯 派生指标（必读，优先使用）")
            lines.append("")
            lines.append("**重要：以下指标用于聚合计算，当用户问题匹配同义词时，必须优先使用派生指标（格式: `derived:显示名`）！**")
            lines.append("")
            
            for rule in derived_metrics:
                rule_def = rule.get('rule_definition', {})
                display_name = rule_def.get('display_name', rule.get('rule_name'))
                formula = rule_def.get('formula', '')
                unit = rule_def.get('unit', '')
                desc = rule.get('description', '')
                synonyms = rule_def.get('synonyms', [])
                field_deps = rule_def.get('field_dependencies', [])
                
                # 判断是否为通用度量（无字段依赖）
                is_universal = not field_deps and 'COUNT' in formula.upper()
                
                title = f"**{display_name}**"
                if is_universal:
                    title += " ⭐ [通用度量，适用所有表]"
                lines.append(title)
                
                if desc:
                    lines.append(f"  - 说明: {desc}")
                if formula:
                    lines.append(f"  - 公式: `{formula}`")
                if unit:
                    lines.append(f"  - 单位: {unit}")
                if synonyms:
                    syn_text = ", ".join(synonyms)
                    lines.append(f"  - 同义词: {syn_text}")
                
                lines.append(f"  - 使用方法: 在 metrics 中填写 `derived:{display_name}`")
                lines.append("")
        
        # 2. 自定义指令
        if custom_instructions:
            lines.append("### 特殊说明")
            lines.append("")
            for rule in custom_instructions:
                rule_def = rule.get('rule_definition', {})
                instruction = rule_def.get('instruction', '')
                if instruction:
                    lines.append(instruction)
                    lines.append("")
        
        return "\n".join(lines) if lines else "无全局业务规则"
    
    def _build_few_shot_section(self, few_shot_examples: Optional[List["FewShotSample"]]) -> str:
        """
        构建 Few-Shot 示例区块
        
        Args:
            few_shot_examples: Few-Shot 示例列表
            
        Returns:
            格式化的示例文本
        """
        if not few_shot_examples:
            return "无历史示例"
        
        lines = []
        lines.append("以下是与当前问题相似的历史问答示例，供参考：")
        lines.append("")
        
        # 按相似度排序，取前3个
        sorted_examples = sorted(few_shot_examples, key=lambda x: x.score, reverse=True)[:3]
        
        for idx, sample in enumerate(sorted_examples, 1):
            lines.append(f"**示例 {idx}**：")
            lines.append(f"- 问题：{sample.question}")
            
            # 优先使用 IR JSON，如果没有则回退到 SQL
            if sample.ir_json:
                try:
                    ir_dict = json.loads(sample.ir_json)
                    ir_formatted = json.dumps(ir_dict, ensure_ascii=False, indent=2)
                    lines.append("- 答案 (IR)：")
                    lines.append("```json")
                    lines.append(ir_formatted)
                    lines.append("```")
                except Exception:
                    lines.append(f"- 答案：{sample.ir_json}")
            elif sample.sql:
                lines.append(f"- 答案 (SQL)：`{sample.sql}`")
            
            lines.append("")
        
        return "\n".join(lines) if lines else "无历史示例"
    
    def _evaluate_and_decide(
        self,
        result: VectorSelectionResult,
        tables: List[TableWithFields],
        question: str
    ) -> VectorSelectionResult:
        """
        评估置信度并决定分流
        
        分流策略：
        - 高置信度(>=high_threshold)且分差足够: action=execute, 直接使用LLM3的IR
        - 中置信度(>=medium_threshold): action=confirm, 需要用户确认表选择
        - 低置信度(<medium_threshold): action=fallback, 降级到LLM2
        """
        from server.nl2ir.vector_selection_confidence import VectorSelectionConfidenceCalculator
        
        # 获取配置阈值（统一从 retrieval_config.yaml 读取）
        high_threshold = get_retrieval_param(
            "vector_table_selection.high_confidence", 0.85
        )
        medium_threshold = get_retrieval_param(
            "vector_table_selection.medium_confidence", 0.50
        )
        min_gap = get_retrieval_param(
            "vector_table_selection.min_score_gap", 0.15
        )
        
        # 使用置信度计算器进行综合评估
        calculator = VectorSelectionConfidenceCalculator()
        eval_result = calculator.evaluate(
            llm_confidence=result.confidence,
            tables=tables,
            selected_table_id=result.selected_table_id,
            ir=result.ir,
            question=question
        )
        
        # 使用综合置信度
        final_confidence = eval_result.final_confidence
        
        # 计算TOP1和TOP2的分差
        sorted_tables = sorted(tables, key=lambda t: t.retrieval_score, reverse=True)
        top1_score = sorted_tables[0].retrieval_score if sorted_tables else 0
        top2_score = sorted_tables[1].retrieval_score if len(sorted_tables) > 1 else 0
        score_gap = top1_score - top2_score
        
        logger.debug(
            "置信度评估",
            llm_confidence=result.confidence,
            final_confidence=final_confidence,
            top1_score=top1_score,
            score_gap=score_gap,
            high_threshold=high_threshold,
            medium_threshold=medium_threshold
        )
        
        # 分流决策
        if final_confidence >= high_threshold and score_gap >= min_gap:
            # 高置信度：直接执行
            result.action = "execute"
            result.needs_confirmation = False
            logger.info("高置信度，直接使用LLM3结果", confidence=final_confidence)
            
        elif final_confidence >= medium_threshold:
            # 中置信度：需要确认
            result.action = "confirm"
            result.needs_confirmation = True
            result.table_selection_card = self._build_selection_card(
                tables, result.selected_table_id, question
            )
            logger.info("中置信度，需要用户确认", confidence=final_confidence)
            
        else:
            # 低置信度：降级到LLM2
            result.action = "fallback"
            result.needs_confirmation = True
            result.table_selection_card = self._build_selection_card(
                tables, result.selected_table_id, question
            )
            logger.info("低置信度，降级到LLM2", confidence=final_confidence)
        
        # 更新置信度为综合评估结果
        result.confidence = final_confidence
        
        return result
    
    def _build_selection_card(
        self,
        tables: List[TableWithFields],
        selected_table_id: str,
        question: str
    ) -> TableSelectionCard:
        """构建表选择卡"""
        # 统一从 retrieval_config.yaml 读取
        max_candidates = get_retrieval_param(
            "vector_table_selection.max_candidates", 5
        )
        
        candidates = []
        for table in tables[:max_candidates]:
            # 提取关键字段
            key_dims = [f.display_name for f in table.dimensions[:5]]
            key_measures = [f.display_name for f in table.measures[:5]]
            
            candidate = TableCandidate(
                table_id=table.table_id,
                table_name=table.display_name,
                description=table.description,
                confidence=table.retrieval_score,
                reason=f"向量检索分数: {table.retrieval_score:.4f}",
                tags=table.tags,
                key_dimensions=key_dims,
                key_measures=key_measures,
                domain_name=table.domain_name,
                domain_id=table.domain_id,
                data_year=table.data_year
            )
            candidates.append(candidate)
        
        # 按检索分数排序
        candidates.sort(key=lambda c: c.confidence, reverse=True)
        
        return TableSelectionCard(
            candidates=candidates,
            question=question,
            message="向量检索找到了多个可能相关的表，请确认您要查询的是哪张表：",
            allow_multi_select=True,  # 允许多选
            page_size=get_retrieval_param("vector_table_selection.page_size", 5),
            total_candidates=len(candidates)
        )
    
    def _fallback_result(
        self,
        tables: List[TableWithFields],
        error_msg: str
    ) -> VectorSelectionResult:
        """降级处理"""
        if not tables:
            return VectorSelectionResult(
                selected_table_id="",
                confidence=0.0,
                selection_reason=f"降级处理失败: {error_msg}",
                action="fallback"
            )
        
        # 使用检索分数最高的表
        best_table = max(tables, key=lambda t: t.retrieval_score)
        
        return VectorSelectionResult(
            selected_table_id=best_table.table_id,
            selected_table_name=best_table.display_name,
            confidence=0.3,  # 低置信度
            selection_reason=f"降级选择检索分数最高的表（原因: {error_msg}）",
            action="fallback",
            needs_confirmation=True,
            table_selection_card=self._build_selection_card(tables, best_table.table_id, ""),
            system_prompt=self.last_system_prompt,
            user_prompt=self.last_user_prompt,
            retrieval_scores={t.table_id: t.retrieval_score for t in tables}
        )


async def load_tables_with_full_fields(
    table_results: List[TableRetrievalResult],
    semantic_model: Any = None
) -> List[TableWithFields]:
    """
    加载表的完整字段信息
    
    Args:
        table_results: 向量检索结果
        semantic_model: 语义模型（可选，用于获取字段信息）
    
    Returns:
        List[TableWithFields]
    """
    from server.utils.db_pool import get_metadata_pool
    from uuid import UUID
    
    if not table_results:
        return []
    
    tables_with_fields: List[TableWithFields] = []
    table_ids = [UUID(r.table_id) for r in table_results]
    
    # 构建检索分数映射
    score_map = {
        r.table_id: {
            "score": r.score,
            "dense_score": r.dense_score,
            "sparse_score": r.sparse_score,
            "reranker_score": r.reranker_score,
            "connection_id": r.connection_id
        }
        for r in table_results
    }
    
    try:
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 1. 查询表基本信息
            table_rows = await conn.fetch("""
                SELECT 
                    t.table_id,
                    t.connection_id,
                    t.display_name,
                    t.description,
                    t.tags,
                    t.domain_id,
                    t.data_year,
                    bd.domain_name
                FROM db_tables t
                LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                WHERE t.table_id = ANY($1::uuid[])
            """, table_ids)
            
            # 2. 查询字段信息
            field_rows = await conn.fetch("""
                SELECT 
                    dc.table_id,
                    f.field_id,
                    f.display_name,
                    f.description,
                    f.field_type,
                    f.synonyms
                FROM fields f
                JOIN db_columns dc ON f.source_column_id = dc.column_id
                WHERE dc.table_id = ANY($1::uuid[])
                  AND f.is_active = TRUE
                ORDER BY dc.table_id, f.priority NULLS LAST, f.display_name
            """, table_ids)
            
            # 3. 查询枚举值（前5个）
            enum_rows = await conn.fetch("""
                SELECT 
                    dc.table_id,
                    f.field_id,
                    fev.original_value as value
                FROM field_enum_values fev
                JOIN fields f ON fev.field_id = f.field_id
                JOIN db_columns dc ON f.source_column_id = dc.column_id
                WHERE dc.table_id = ANY($1::uuid[])
                  AND f.is_active = TRUE
                ORDER BY dc.table_id, f.field_id, fev.frequency DESC NULLS LAST
            """, table_ids)
            
            # 构建字段映射
            fields_map: Dict[str, Dict[str, List[FieldDetail]]] = {}
            for row in field_rows:
                tid = str(row["table_id"])
                if tid not in fields_map:
                    fields_map[tid] = {"dimensions": [], "measures": [], "identifiers": []}
                
                field_type = row["field_type"] or "dimension"
                synonyms = row["synonyms"] or []
                
                field_detail = FieldDetail(
                    field_id=str(row["field_id"]),
                    display_name=row["display_name"] or "",
                    field_type=field_type,
                    description=row["description"] or "",
                    synonyms=synonyms if isinstance(synonyms, list) else [],
                    enum_values=[]
                )
                
                if field_type == "measure":
                    fields_map[tid]["measures"].append(field_detail)
                elif field_type == "identifier":
                    fields_map[tid]["identifiers"].append(field_detail)
                else:
                    fields_map[tid]["dimensions"].append(field_detail)
            
            # 填充枚举值
            enum_map: Dict[str, List[str]] = {}
            for row in enum_rows:
                fid = str(row["field_id"])
                if fid not in enum_map:
                    enum_map[fid] = []
                if len(enum_map[fid]) < 10:  # 每个字段最多10个枚举值
                    enum_map[fid].append(str(row["value"]))
            
            # 将枚举值填入字段
            for tid, field_groups in fields_map.items():
                for field_list in field_groups.values():
                    for field_detail in field_list:
                        if field_detail.field_id in enum_map:
                            field_detail.enum_values = enum_map[field_detail.field_id]
            
            # 4. 构建最终结果
            for row in table_rows:
                tid = str(row["table_id"])
                scores = score_map.get(tid, {})
                table_fields = fields_map.get(tid, {"dimensions": [], "measures": [], "identifiers": []})
                
                table = TableWithFields(
                    table_id=tid,
                    display_name=row["display_name"] or "",
                    description=row["description"] or "",
                    domain_name=row["domain_name"],
                    domain_id=str(row["domain_id"]) if row.get("domain_id") else None,
                    connection_id=scores.get("connection_id"),
                    data_year=row["data_year"],
                    tags=row["tags"] or [],
                    retrieval_score=scores.get("score", 0.0),
                    dense_score=scores.get("dense_score"),
                    sparse_score=scores.get("sparse_score"),
                    reranker_score=scores.get("reranker_score"),
                    dimensions=table_fields["dimensions"],
                    measures=table_fields["measures"],
                    identifiers=table_fields["identifiers"]
                )
                tables_with_fields.append(table)
        
        # 按检索分数排序
        tables_with_fields.sort(key=lambda t: t.retrieval_score, reverse=True)
        
        logger.debug(
            "加载表完整字段信息完成",
            table_count=len(tables_with_fields),
            total_fields=sum(t.field_count for t in tables_with_fields)
        )
        
    except Exception as e:
        logger.exception("加载表字段信息失败", error=str(e))
    
    return tables_with_fields
