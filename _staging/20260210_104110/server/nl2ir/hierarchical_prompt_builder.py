"""
层次化Prompt构建器
将表结构转换为清晰的、按表分组的Prompt

新增功能：
- 结构化输出：Tables/Filters/Metrics/Instructions
- 可信度分级：高可信度精简输出，低可信度完整输出
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
import structlog

from server.models.semantic import SemanticModel, Field, FieldEnumValue
from server.nl2ir.table_structure_loader import TableStructure
from server.nl2ir.table_retriever import TableRetrievalResult
from server.nl2ir.few_shot_retriever import FewShotSample
from server.config import settings, RetrievalConfig, get_retrieval_param

logger = structlog.get_logger()

MAX_ENUM_VALUES_IN_PROMPT = settings.max_enum_values_in_prompt  # 枚举值提示数量上限


@dataclass
class StructuredPromptContext:
    """结构化 Prompt 上下文"""
    tables_section: str = ""        # 【Tables】部分
    filters_section: str = ""       # 【Filters】部分
    metrics_section: str = ""       # 【Metrics】部分
    instructions_section: str = ""  # 【Instructions】部分
    confidence_level: str = "medium"  # high / medium / low
    warnings: List[str] = field(default_factory=list)


class HierarchicalPromptBuilder:
    """
    层次化Prompt构建器

    生成按表分组、结构清晰的Prompt，供LLM使用
    """

    def __init__(self, semantic_model: SemanticModel):
        """
        初始化Prompt构建器

        Args:
            semantic_model: 语义模型
        """
        self.model = semantic_model

        # 加载层次化检索的说明文本

    def build_context(
        self,
        table_structures: List[TableStructure],
        question: str,
        domain_name: Optional[str] = None,
        global_rules: Optional[List[Dict[str, Any]]] = None,
        few_shot_examples: Optional[List[FewShotSample]] = None,
        table_scores: Optional[Dict[str, TableRetrievalResult]] = None,
        enum_matches: Optional[List[Any]] = None,
    ) -> str:
        """
        构建给LLM的上下文

        Args:
            table_structures: 表结构列表（按相关性排序）
            question: 用户问题
            domain_name: 业务域名称
            global_rules: 全局规则列表

        Returns:
            格式化的Prompt文本
        """
        lines = []

        # 保存当前问题文本用于后续枚举值高亮
        self._current_question = (question or "").lower()
        # 保存枚举命中字段ID（用于控制枚举可选值示例是否注入，减少噪声）
        enum_hit_field_ids: set[str] = set()
        if enum_matches:
            for match in enum_matches:
                try:
                    fid = getattr(match, "field_id", None)
                    if fid:
                        enum_hit_field_ids.add(str(fid))
                except Exception:
                    continue
        self._enum_hit_field_ids = enum_hit_field_ids

        # 1. 业务域信息
        if domain_name:
            lines.append(f"## 业务域: {domain_name}")
            lines.append("")

        # 1.1 全局业务规则（置于最前，强调优先级）
        if global_rules:
            lines.extend(self._build_rules_section(global_rules))

        # 2. 候选表（按表分组展示）
        lines.append("## 候选数据表（按相关性排序）")
        lines.append("")

        if not table_structures:
            lines.append("*未找到相关数据表*")
            lines.append("")
        else:
            # 跨表同名字段冲突提示：默认全局只提示一次（字段级仍会标记[多表同名字段]）
            field_conflicts: Dict[str, List[str]] = {}
            if len(table_structures) > 1:
                field_conflicts = self._detect_field_conflicts_across_tables(table_structures)
            conflict_warning_mode = str(
                get_retrieval_param("llm_prompt.conflict_warning.mode", "global_once") or "global_once"
            ).strip().lower()
            emit_conflict_warning = conflict_warning_mode == "per_table"
            if field_conflicts and conflict_warning_mode == "global_once":
                lines.append("> **【重要提示】**: 以下字段列表中，部分字段名在多个表中都存在（已标记[多表同名字段]）。")
                lines.append("> **必须只选择当前表的字段，严格禁止混合选择其他表的同名字段。**")
                lines.append("")

            score_map = table_scores or {}
            for i, structure in enumerate(table_structures, 1):
                lines.extend(
                    self._build_table_section(
                        i,
                        structure,
                        table_structures,
                        score_map,
                        field_conflicts=field_conflicts,
                        emit_conflict_warning=emit_conflict_warning,
                    )
                )

        if few_shot_examples:
            lines.extend(self._format_few_shot_section(few_shot_examples))

        from server.utils.timezone_helper import now_with_tz
        current_dt = now_with_tz()
        current_date = current_dt.strftime("%Y-%m-%d")
        current_year = current_dt.year

        # 只有当问题中包含时间相关词汇时，才注入当前日期
        # 扩展时间关键词列表以覆盖更多场景
        time_keywords = [
            # 相对时间词
            "今年", "本年", "明年", "去年", "前年",
            "本月", "上月", "下月",
            "本周", "上周", "下周",
            "本季度", "上季度", "下季度",
            "当前", "现在", "今天", "最近", "近", 
            
            # 比较与趋势词
            "同比", "环比", "增长", "下降", "趋势", "变化",
            
            # 时间单位词（配合数字出现）
            "年", "月", "日", "天", "周", "季"
        ]
        
        # 简单的正则匹配以提高准确率（可选，目前先用关键词包含匹配）
        # 如果问题中包含任何时间关键词，或者看起来像是在问时间相关的问题
        if any(k in self._current_question for k in time_keywords):
            lines.append("")
            lines.append(f"当前日期：{current_date}（当前年份：{current_year}）")

        return "\n".join(lines)

    def build_structured_context(
        self,
        table_structures: List[TableStructure],
        question: str,
        enum_matches: Optional[List[Any]] = None,
        confidence_level: str = "medium",
        domain_name: Optional[str] = None,
        table_scores: Optional[Dict[str, TableRetrievalResult]] = None,
    ) -> StructuredPromptContext:
        """
        构建结构化的 Prompt 上下文
        
        根据可信度分级构建不同详细程度的 prompt：
        - high: 主表确定、枚举高置信 → 精简输出
        - low: 主表不确定或枚举低置信 → 完整输出 + 警告
        
        Args:
            table_structures: 表结构列表
            question: 用户问题
            enum_matches: 枚举匹配结果
            confidence_level: 可信度级别 (high/medium/low)
            domain_name: 业务域名称
            table_scores: 表得分映射
            
        Returns:
            StructuredPromptContext 结构化上下文
        """
        context = StructuredPromptContext(confidence_level=confidence_level)
        score_map = table_scores or {}
        
        # ========== 【Tables】部分 ==========
        tables_lines = ["## 【Tables】候选数据表"]
        tables_lines.append("")
        
        if not table_structures:
            tables_lines.append("*未找到相关数据表*")
        else:
            for i, struct in enumerate(table_structures, 1):
                table_result = score_map.get(struct.table_id)
                score_info = ""
                if table_result:
                    score_info = f" (得分: {table_result.score:.3f})"
                
                # 主表标注
                main_label = " **[主表]**" if i == 1 else ""
                
                tables_lines.append(f"### {i}. {struct.table_name}{main_label}{score_info}")
                
                # 表描述
                if struct.description:
                    tables_lines.append(f"   描述: {struct.description}")
                
                # 为何相关（高置信时简化）
                if confidence_level != "high":
                    if table_result and hasattr(table_result, 'enum_boost_trace'):
                        trace = table_result.enum_boost_trace
                        if trace:
                            tables_lines.append(f"   得分构成: 原始={trace.get('original_score', 0):.3f}, 枚举加权={trace.get('enum_boost', 0):.3f}")
                
                tables_lines.append("")
        
        context.tables_section = "\n".join(tables_lines)
        
        # ========== 【Filters】部分 ==========
        filters_lines = ["## 【Filters】过滤条件"]
        filters_lines.append("")
        
        if enum_matches:
            # 按表分组
            by_table: Dict[str, List[Any]] = {}
            for match in enum_matches:
                table_name = getattr(match, 'table_name', '未知表') or '未知表'
                if table_name not in by_table:
                    by_table[table_name] = []
                by_table[table_name].append(match)
            
            for table_name, matches in by_table.items():
                # 判断是否为主表
                is_main = table_name == (table_structures[0].table_name if table_structures else "")
                main_label = " **(主表)**" if is_main else ""
                
                filters_lines.append(f"### 表: {table_name}{main_label}")
                
                for match in matches[:5]:  # 每表最多显示5个
                    confidence_pct = min(100, int(match.final_score * 100))
                    match_type_label = {
                        "exact": "精确",
                        "synonym": "同义词",
                        "value_vector": "语义"
                    }.get(match.match_type, match.match_type)
                    
                    # 一致性标记
                    consistency = "✓" if is_main else "⚠"
                    
                    filters_lines.append(
                        f"- {match.field_name} = '{match.value}' "
                        f"[{match_type_label}, {confidence_pct}%, {consistency}]"
                    )
                
                filters_lines.append("")
        else:
            filters_lines.append("*未检测到过滤条件*")
        
        context.filters_section = "\n".join(filters_lines)
        
        # ========== 【Metrics】部分 ==========
        metrics_lines = ["## 【Metrics】可用度量"]
        metrics_lines.append("")
        
        if table_structures:
            main_struct = table_structures[0]
            if main_struct.measures:
                metrics_lines.append(f"### 主表 [{main_struct.table_name}] 度量字段:")
                for measure in main_struct.measures[:10]:  # 最多显示10个
                    unit = f" ({measure.unit})" if hasattr(measure, 'unit') and measure.unit else ""
                    metrics_lines.append(f"- {measure.display_name}{unit}")
                metrics_lines.append("")
            
            # 低可信度时显示其他表的度量
            if confidence_level == "low" and len(table_structures) > 1:
                for struct in table_structures[1:3]:  # 最多显示其他2个表
                    if struct.measures:
                        metrics_lines.append(f"### 其他表 [{struct.table_name}] 度量字段:")
                        for measure in struct.measures[:5]:
                            metrics_lines.append(f"- {measure.display_name}")
                        metrics_lines.append("")
        
        context.metrics_section = "\n".join(metrics_lines)
        
        # ========== 【Instructions】部分 ==========
        instructions_lines = ["## 【Instructions】生成指导"]
        instructions_lines.append("")
        
        if confidence_level == "high":
            instructions_lines.extend([
                "1. 主表已确定，过滤条件高置信，请按以下步骤生成 IR：",
                "2. 使用【主表】作为查询主体",
                '3. 必须包含【Filters】中标记为"精确"的过滤条件',
                "4. 从【Metrics】中选择与问题相关的度量字段",
            ])
        elif confidence_level == "low":
            instructions_lines.extend([
                "⚠️ **低置信度警告**：过滤条件或主表选择可能不准确",
                "",
                "1. 请仔细核对【Filters】中的过滤条件是否与问题相符",
                "2. 如果过滤条件与问题不匹配，可以忽略",
                "3. 注意检查字段的表归属（⚠标记表示跨表）",
                "4. 可参考【Tables】中其他候选表",
            ])
            context.warnings.append("低置信过滤，需谨慎选择")
        else:
            instructions_lines.extend([
                "1. 先确定主表（【Tables】中排名第一的表）",
                "2. 检查【Filters】中的过滤条件，优先使用与主表一致的",
                "3. 从主表的【Metrics】中选择度量字段",
            ])
        
        context.instructions_section = "\n".join(instructions_lines)
        
        return context
    
    def render_structured_context(self, context: StructuredPromptContext) -> str:
        """
        将结构化上下文渲染为最终的 Prompt 文本
        
        Args:
            context: 结构化上下文
            
        Returns:
            渲染后的 Prompt 文本
        """
        parts = []
        
        # 添加警告（如果有）
        if context.warnings:
            parts.append("⚠️ **警告**:")
            for warning in context.warnings:
                parts.append(f"- {warning}")
            parts.append("")
        
        # 按顺序添加各部分
        if context.tables_section:
            parts.append(context.tables_section)
        
        if context.filters_section:
            parts.append(context.filters_section)
        
        if context.metrics_section:
            parts.append(context.metrics_section)
        
        if context.instructions_section:
            parts.append(context.instructions_section)
        
        return "\n".join(parts)

    def _format_few_shot_section(self, examples: List[FewShotSample]) -> List[str]:
        """格式化 Few-Shot 示例区块
        
        示例已经按相似度和质量分数排序，直接使用动态数量控制后的结果
        格式：示例 (Few-Shot Examples) - 强制阅读
        问题：从Milvus中检索的问题
        答案：优先展示IR JSON格式，如果没有则回退到SQL
        """
        if not examples:
            return []

        # 确保示例按相似度分数降序排列（few_shot_retriever 已排序，这里再次确认）
        sorted_examples = sorted(examples, key=lambda x: x.score, reverse=True)

        lines: List[str] = []
        # 使用固定的标题格式：示例 (Few-Shot Examples) - 强制阅读
        lines.append("## 示例 (Few-Shot Examples) - 强制阅读")
        lines.append("")

        # 使用动态数量控制后的示例列表
        for idx, sample in enumerate(sorted_examples, 1):
            lines.append(f"**问题**：{sample.question}")
            
            # 优先使用 IR JSON，如果没有则回退到 SQL
            if sample.ir_json:
                try:
                    import json
                    # 解析并格式化 JSON，使其更易读
                    ir_dict = json.loads(sample.ir_json)
                    ir_formatted = json.dumps(ir_dict, ensure_ascii=False, indent=2)
                    lines.append("**答案**：")
                    lines.append("```json")
                    lines.append(ir_formatted)
                    lines.append("```")
                except Exception as e:
                    # JSON 解析失败，使用原始格式
                    logger.warning("IR JSON 解析失败，使用原始格式", error=str(e))
                    lines.append(f"**答案**：{sample.ir_json}")
            elif sample.sql:
                # 向后兼容：如果没有 ir_json，使用 sql
                lines.append(f"**答案**：{sample.sql}")
            else:
                # 既没有 ir_json 也没有 sql，跳过
                logger.warning("Few-Shot示例既没有ir_json也没有sql，已跳过", question=sample.question)
                continue
            
            lines.append("")

        return lines

    def build_enum_match_section(self, enum_matches: Optional[List[Any]], limit: int = 3) -> str:
        """构建枚举值匹配区块，供 LLM 参考过滤条件"""
        if not enum_matches:
            return ""

        visible_matches = enum_matches[: max(1, limit)]
        lines = ["## 检测到的过滤值", ""]
        for match in visible_matches:
            table_name = getattr(match, "table_name", None) or "未知表"
            field_name = getattr(match, "field_name", None) or "未知字段"
            field_id = getattr(match, "field_id", None) or ""
            value = getattr(match, "display_name", None) or getattr(match, "value", None) or "-"
            
            # 简化输出，只保留关键信息
            field_ref = f"({field_id})" if field_id else ""
            lines.append(f"- {field_name}{field_ref} = '{value}' [表: {table_name}]")

        lines.append("")
        return "\n".join(lines)

    def _build_table_section(
        self,
        index: int,
        structure: TableStructure,
        all_structures: List[TableStructure] = None,
        table_scores: Optional[Dict[str, TableRetrievalResult]] = None,
        *,
        field_conflicts: Optional[Dict[str, List[str]]] = None,
        emit_conflict_warning: bool = False,
    ) -> List[str]:
        """
        构建单个表的section

        Args:
            index: 表序号
            structure: 表结构
            all_structures: 所有表结构（用于字段冲突检测）

        Returns:
            文本行列表
        """
        lines = []
        score_entry = (table_scores or {}).get(structure.table_id)

        # 表标题
        lines.append(f"### 表{index}: {structure.display_name}")
        lines.append(f"**表ID**: `{structure.table_id}`")
        if structure.description:
            lines.append(f"**说明**: {structure.description}")
        if structure.domain_name:
            lines.append(f"**所属业务域**: {structure.domain_name}")
        lines.append(f"**字段数**: {structure.total_fields}")
        if structure.geometries:
            lines.append(f"**包含空间字段**: 是（{len(structure.geometries)}个）")
        year_text = structure.data_year or "未设置"
        lines.append(f"**年份**: {year_text}")

        if score_entry:
            score_bits = []
            if score_entry.reranker_score is not None:
                score_bits.append(f"reranker={score_entry.reranker_score:.3f}")
            if score_entry.dense_score is not None:
                score_bits.append(f"dense={score_entry.dense_score:.3f}")
            if score_entry.sparse_score is not None:
                score_bits.append(f"sparse={score_entry.sparse_score:.3f}")
            if score_entry.rrf_score is not None:
                score_bits.append(f"rrf={score_entry.rrf_score:.3f}")
            if score_bits:
                lines.append(f"**命中得分**: {', '.join(score_bits)}")
            graph_hit = (score_entry.evidence or {}).get("graph_text")
            if graph_hit:
                lines.append(f"**Graph命中**: {graph_hit}")

        tag_candidates = []
        if structure.tags:
            tag_candidates.extend(structure.tags)
        if structure.aliases:
            tag_candidates.extend(structure.aliases)
        dedup_tags = []
        seen_tags = set()
        for tag in tag_candidates:
            cleaned = tag.strip() if isinstance(tag, str) else tag
            if cleaned and cleaned not in seen_tags:
                seen_tags.add(cleaned)
                dedup_tags.append(cleaned)
        if dedup_tags:
            tag_preview = "、".join(dedup_tags[:8])
            if len(dedup_tags) > 8:
                tag_preview += " 等"
            lines.append(f"**标签/同义词**: {tag_preview}")
        else:
            lines.append("**标签/同义词**: 未设置")
        lines.append("")

        # 可选：跨表同名字段冲突提示（默认由 build_context 全局输出一次）
        if emit_conflict_warning and field_conflicts:
            lines.append("> **【重要提示】**: 以下字段列表中，部分字段名在多个表中都存在（已标记[多表同名字段]）。")
            lines.append("> **必须只选择当前表的字段，严格禁止混合选择其他表的同名字段。**")
            lines.append("")

        # 维度字段
        if structure.dimensions:
            lines.append("####  维度字段（用于分组和过滤）")

            for dim in structure.dimensions:  # 显示所有维度字段
                lines.extend(self._format_dimension_field(dim, structure.display_name, field_conflicts))
            lines.append("")

        # 度量字段
        if structure.measures:
            lines.append("####  度量字段（用于计算和排序）")
            for measure in structure.measures:  # 显示所有度量字段
                lines.extend(self._format_measure_field(measure, structure.display_name, field_conflicts))
            lines.append("")

        # 标识字段（用于唯一标识记录，如编号、代码等）
        if structure.identifiers:
            lines.append("####  标识字段（用于唯一标识记录）")
            for identifier in structure.identifiers:
                lines.extend(self._format_identifier_field(identifier, structure.display_name, field_conflicts))
            lines.append("")

        # 时间戳字段
        if structure.timestamps:
            lines.append("####  时间字段")
            for ts in structure.timestamps:
                lines.append(f"- **{ts.field_id}** `{ts.display_name}`: {ts.description or '时间戳字段'}")
            lines.append("")

        # 空间字段
        if structure.geometries:
            lines.append("####  空间字段（几何字段）")
            for geo in structure.geometries:
                geo_type = getattr(geo, 'data_type', 'geometry')
                lines.append(f"- **{geo.field_id}** `{geo.display_name}`: {geo.description or '空间几何字段'} *[类型: {geo_type}]*")
            lines.append("")

        lines.append("---")
        lines.append("")

        return lines

    def _format_dimension_field(self, field: Field, table_name: str = "", field_conflicts: Dict[str, List[str]] = None) -> List[str]:
        """
        格式化维度字段，支持表级冲突标识

        Args:
            field: 维度字段
            table_name: 表名
            field_conflicts: 字段冲突映射 {字段名: [表名列表]}

        Returns:
            文本行列表
        """
        lines = []

        # 字段基本信息（包含类型标签）
        desc = f": {field.description}" if field.description else ""

        # 添加维度类型标签
        type_label = ""
        if hasattr(field, 'dimension_props') and field.dimension_props:
            dim_type = getattr(field.dimension_props, 'dimension_type', None)
            if dim_type == 'temporal':
                type_label = "时间维度"
            elif dim_type == 'hierarchical':
                type_label = "层级维度"

        # 检查字段冲突并添加标识
        conflict_indicator = ""
        display_name = field.display_name
        
        if field_conflicts and field.display_name in field_conflicts:
            if len(field_conflicts[field.display_name]) > 1:
                # 多表冲突：显示警告标识（不改变字段名）
                conflict_indicator = f" [所属表: {table_name}] [多表同名字段]"

        lines.append(f"- **{field.field_id}** `{display_name}`{type_label}{conflict_indicator}{desc}")

        # 同义词（不做数量限制）
        if field.synonyms and len(field.synonyms) > 0:
            syn_text = ", ".join(field.synonyms)
            lines.append(f"  *同义词: {syn_text}*")

        # 枚举值（按配置展示Top-N）
        if hasattr(self.model, 'field_enums') and field.field_id in self.model.field_enums:
            enum_values = self.model.field_enums[field.field_id] or []
            if self._should_include_enum_examples(field):
                enum_lines = self._format_enum_values(enum_values)
                if enum_lines:
                    lines.extend(enum_lines)

        lines.append("")
        return lines

    def _should_include_enum_examples(self, field: Field) -> bool:
        """
        控制是否将某字段的“可选值示例”注入到 prompt 中。

        目标：保持 TOP3 表字段清单全量输出，但将大量无关枚举值示例降噪，避免干扰模型。
        """
        mode = str(get_retrieval_param("llm_prompt.enum_values.mode", "relevant_only") or "relevant_only").strip().lower()
        if mode == "all":
            return True
        if mode != "relevant_only":
            return False

        question_text = getattr(self, "_current_question", "") or ""
        field_id = str(getattr(field, "field_id", "") or "")
        display_name = str(getattr(field, "display_name", "") or "")

        always_include = get_retrieval_param("llm_prompt.enum_values.always_include_fields", []) or []
        always_set = {str(item).strip().lower() for item in always_include if item and str(item).strip()}
        if field_id and field_id.strip().lower() in always_set:
            return True
        if display_name and display_name.strip().lower() in always_set:
            return True

        hit_ids = getattr(self, "_enum_hit_field_ids", set()) or set()
        if field_id and field_id in hit_ids:
            return True

        # 字段名/同义词出现在问题中 → 认为相关
        if display_name and display_name.strip().lower() in question_text:
            return True
        for synonym in (getattr(field, "synonyms", None) or []):
            syn = str(synonym or "").strip().lower()
            if syn and syn in question_text:
                return True

        return False

    def _detect_field_conflicts_across_tables(self, table_structures: List[TableStructure]) -> Dict[str, List[str]]:
        """
        检测跨表字段冲突

        Args:
            table_structures: 表结构列表

        Returns:
            Dict[str, List[str]]: 字段名到表名的映射，用于标识冲突字段
        """
        field_name_to_tables = {}

        # 收集所有字段的显示名和所属表
        for structure in table_structures:
            # 检查维度字段
            for field in structure.dimensions:
                field_name = getattr(field, 'display_name', '')
                if field_name:
                    if field_name not in field_name_to_tables:
                        field_name_to_tables[field_name] = []
                    field_name_to_tables[field_name].append(structure.display_name)

            # 检查度量字段
            for field in structure.measures:
                field_name = getattr(field, 'display_name', '')
                if field_name:
                    if field_name not in field_name_to_tables:
                        field_name_to_tables[field_name] = []
                    field_name_to_tables[field_name].append(structure.display_name)

        # 只返回存在冲突的字段（出现在多张表中）
        conflicts = {
            field_name: tables for field_name, tables in field_name_to_tables.items()
            if len(tables) > 1
        }

        logger.debug("检测到字段冲突", conflicts=conflicts)
        return conflicts

    def _format_measure_field(self, field: Field, table_name: str = "", field_conflicts: Dict[str, List[str]] = None) -> List[str]:
        """
        格式化度量字段，支持表级冲突标识

        Args:
            field: 度量字段
            table_name: 表名
            field_conflicts: 字段冲突映射 {字段名: [表名列表]}

        Returns:
            文本行列表
        """
        lines = []

        # 字段基本信息
        desc = f": {field.description}" if field.description else ""

        # 单位信息
        unit_text = ""
        if hasattr(field, 'measure_props') and field.measure_props:
            unit = field.measure_props.unit
            if unit:
                unit_text = f" *[单位: {unit}]*"

        # 检查字段冲突并添加标识
        conflict_indicator = ""
        display_name = field.display_name
        
        if field_conflicts and field.display_name in field_conflicts:
            if len(field_conflicts[field.display_name]) > 1:
                # 多表冲突：显示警告标识（不改变字段名）
                conflict_indicator = f" [所属表: {table_name}] [多表同名字段]"

        lines.append(f"- **{field.field_id}** `{display_name}`{conflict_indicator}{desc}{unit_text}")

        # 同义词（不做数量限制）
        if field.synonyms and len(field.synonyms) > 0:
            syn_text = ", ".join(field.synonyms)
            lines.append(f"  *同义词: {syn_text}*")

        lines.append("")
        return lines

    def _format_identifier_field(self, field: Field, table_name: str = "", field_conflicts: Dict[str, List[str]] = None) -> List[str]:
        """
        格式化标识字段，支持表级冲突标识

        Args:
            field: 标识字段
            table_name: 表名
            field_conflicts: 字段冲突映射 {字段名: [表名列表]}

        Returns:
            文本行列表
        """
        lines = []

        # 字段基本信息
        desc = f": {field.description}" if field.description else ""

        # 检查字段冲突并添加标识
        conflict_indicator = ""
        display_name = field.display_name
        
        if field_conflicts and field.display_name in field_conflicts:
            if len(field_conflicts[field.display_name]) > 1:
                # 多表冲突：显示警告标识（不改变字段名）
                conflict_indicator = f" [所属表: {table_name}] [多表同名字段]"

        lines.append(f"- **{field.field_id}** `{display_name}`{conflict_indicator}{desc}")

        # 同义词（不做数量限制）
        if field.synonyms and len(field.synonyms) > 0:
            syn_text = ", ".join(field.synonyms)
            lines.append(f"  *同义词: {syn_text}*")

        lines.append("")
        return lines

    def _format_enum_values(self, enum_values: List[FieldEnumValue]) -> List[str]:
        """
        将枚举值格式化为提示文本，最多显示配置数量
        """
        active_values = [
            ev for ev in enum_values
            if getattr(ev, "is_active", True) and (ev.standard_value or "").strip()
        ]
        if not active_values:
            return []

        question_text = getattr(self, "_current_question", "")

        sorted_values = sorted(
            active_values,
            key=lambda ev: (
                -(ev.record_count or 0),
                getattr(ev, "sort_order", 0),
                ev.standard_value
            )
        )

        # 优先将问题中提到的枚举值置顶
        matched_values = []
        matched_ids = set()
        if question_text:
            for ev in sorted_values:
                if self._enum_value_mentioned(ev, question_text):
                    matched_values.append(ev)
                    matched_ids.add(ev.value_id)

        remaining_values = [ev for ev in sorted_values if ev.value_id not in matched_ids]
        priority_values = matched_values + remaining_values
        limited_values = priority_values[:MAX_ENUM_VALUES_IN_PROMPT]

        display_values = []
        for ev in limited_values:
            value_text = ev.standard_value
            if ev.synonyms:
                synonyms = [s.synonym_text for s in ev.synonyms if getattr(s, "synonym_text", None)]
                if synonyms:
                    value_text = f"{value_text} (同义词: {', '.join(synonyms)})"
            if ev.value_id in matched_ids:
                value_text = f"{value_text} [命中]"
            display_values.append(value_text)

        if not display_values:
            return []

        lines = []
        values_text = ", ".join(display_values)
        total = len(sorted_values)

        if total > MAX_ENUM_VALUES_IN_PROMPT:
            remaining = total - MAX_ENUM_VALUES_IN_PROMPT
            lines.append(
                f"  *可选值示例(前{MAX_ENUM_VALUES_IN_PROMPT}/{total}): "
                f"{values_text} …（另有{remaining}个省略）*"
            )
        else:
            lines.append(f"  *可选值: {values_text}*")

        return lines

    def _enum_value_mentioned(self, enum_value: FieldEnumValue, question_text: str) -> bool:
        """判断枚举值或其同义词是否在问题文本中出现"""
        candidates = [enum_value.standard_value or ""]
        if enum_value.display_name:
            candidates.append(enum_value.display_name)
        if enum_value.synonyms:
            candidates.extend(
                s.synonym_text for s in enum_value.synonyms if getattr(s, "synonym_text", None)
            )

        for candidate in candidates:
            normalized = (candidate or "").strip().lower()
            if normalized and normalized in question_text:
                return True
        return False


    def _build_rules_section(
        self,
        global_rules: List[Dict[str, Any]]
    ) -> List[str]:
        """
        构建全局规则部分

        Args:
            global_rules: 全局规则列表

        Returns:
            文本行列表
        """
        if not global_rules:
            return []

        lines = [
            "##  全局业务规则（最高优先级，必须严格执行）",
            "",
            "**执行要求**：以下规则高于模型自行推理，必须无条件遵守。若某条规则因数据缺失等原因无法执行，务必在 `ambiguities` 中说明原因，并将 `confidence` 调低到 < 0.8。",
            ""
        ]

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
            lines.append("### 派生指标")
            lines.append("")
            lines.append("**使用规则**：")
            lines.append("- 仅当用户问题中**明确出现**下列同义词时，才使用对应的派生指标")
            lines.append("- 当用户提到「面积」「金额」「总价」等具体度量时，应使用候选表的**度量字段**，而非派生指标")
            lines.append("- 派生指标格式：`derived:显示名`")
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

                # 添加特殊标记
                title = f"**{display_name}**"
                if is_universal:
                    title += " [适用于统计记录数量]"
                lines.append(title)
                
                if desc:
                    lines.append(f"  - 说明: {desc}")
                if formula:
                    lines.append(f"  - 公式: `{formula}`")
                if unit:
                    lines.append(f"  - 单位: {unit}")
                if synonyms:
                    # 展示全部同义词（不做数量限制）
                    syn_text = ", ".join(synonyms)
                    lines.append(f"  - 同义词: {syn_text}")
                
                # 使用示例
                lines.append(f"  - 使用方法: 在 metrics 中填写 `derived:{display_name}`")
                lines.append("")

        # 2. 自定义指令
        if custom_instructions:
            lines.append("###  特殊说明")
            lines.append("")
            for rule in custom_instructions:
                rule_def = rule.get('rule_definition', {})
                instruction = rule_def.get('instruction', '')
                if instruction:
                    lines.append(instruction)
                    lines.append("")

        return lines

    def build_simple_context(
        self,
        table_structures: List[TableStructure],
        question: str
    ) -> str:
        """
        构建简化版本的Prompt（用于token限制的场景）

        Args:
            table_structures: 表结构列表
            question: 用户问题

        Returns:
            简化的Prompt文本
        """
        lines = []

        lines.append("## 可用数据表")
        lines.append("")

        for i, structure in enumerate(table_structures, 1):
            lines.append(f"### 表{i}: {structure.display_name}")
            lines.append(f"- 维度字段: {len(structure.dimensions)}个")
            lines.append(f"- 度量字段: {len(structure.measures)}个")
            if structure.geometries:
                lines.append(f"- 空间字段: {len(structure.geometries)}个")

            # 只列出字段ID和名称
            if structure.dimensions:
                dim_names = [f"{d.field_id}(`{d.display_name}`)" for d in structure.dimensions[:10]]
                lines.append(f"- 主要维度: {', '.join(dim_names)}")

            if structure.measures:
                measure_names = [f"{m.field_id}(`{m.display_name}`)" for m in structure.measures[:10]]
                lines.append(f"- 主要度量: {', '.join(measure_names)}")

            if structure.geometries:
                geo_names = [f"{g.field_id}(`{g.display_name}`)" for g in structure.geometries[:5]]
                lines.append(f"- 空间字段: {', '.join(geo_names)}")

            lines.append("")

        return "\n".join(lines)
