"""
枚举值混合传递策略

根据枚举值匹配的置信度，选择不同的传递方式：
- confidence ≥ 0.98 (精确/同义词) → 预填充（强制使用）
- confidence 0.90-0.98 (高相似)    → Function参数（建议使用）
- confidence 0.70-0.90 (中等相似)  → Prompt提示（供参考）
- confidence < 0.70                → 丢弃

新增功能：
- 跨表分组提示：同名字段跨表分组，标注表名/得分/是否主表
- 高噪声字段过滤：高噪声低分项不传给LLM
"""

from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import structlog

from server.config import RetrievalConfig

logger = structlog.get_logger()


@dataclass
class EnumSuggestion:
    """枚举值建议"""
    field_id: str
    field_name: str
    value: str
    display_name: str
    confidence: float
    match_type: str
    # 新增：表信息
    table_id: Optional[str] = None
    table_name: Optional[str] = None
    is_main_table: bool = False
    # 新增：高噪声标记
    is_high_noise: bool = False


@dataclass
class CrossTableFieldGroup:
    """跨表字段分组"""
    field_name: str
    tables: List[Dict[str, Any]] = field(default_factory=list)
    has_conflict: bool = False  # 同名字段存在于多表


class EnumPromptStrategy:
    """枚举值Prompt传递策略"""
    
    def __init__(self, main_table_id: Optional[str] = None, llm_selection_mode: bool = False):
        # 从配置加载置信度阈值
        self.FORCE_USE_THRESHOLD = RetrievalConfig.prompt_enum_force_use_threshold()
        self.STRONG_SUGGEST_THRESHOLD = RetrievalConfig.prompt_enum_strong_suggest_threshold()
        self.WEAK_HINT_THRESHOLD = RetrievalConfig.prompt_enum_weak_hint_threshold()
        # 新增：高噪声字段配置
        self.HIGH_NOISE_FIELDS = frozenset(RetrievalConfig.enum_high_noise_fields())
        self.HIGH_NOISE_THRESHOLD = RetrievalConfig.enum_high_noise_threshold()
        self.EXCLUDE_HIGH_NOISE_FROM_PROMPT = RetrievalConfig.enum_exclude_from_llm_prompt()
        # 主表ID
        self.main_table_id = main_table_id
        # LLM 选表模式：简化处理，直接按相似度取前 N 个
        self.llm_selection_mode = llm_selection_mode
    
    def set_main_table(self, table_id: str):
        """设置主表ID"""
        self.main_table_id = table_id
    
    def _is_high_noise_field(self, field_name: str) -> bool:
        """检查是否为高噪声字段"""
        if not field_name:
            return False
        normalized = field_name.lower().strip()
        for noise_field in self.HIGH_NOISE_FIELDS:
            if noise_field.lower() in normalized or normalized in noise_field.lower():
                return True
        return False
    
    def categorize_matches(
        self,
        enum_matches: List[Any]  # List[EnumMatch]
    ) -> Tuple[List[EnumSuggestion], List[EnumSuggestion], List[EnumSuggestion]]:
        """
        将枚举值匹配结果按置信度分类
        
        LLM 选表模式：直接按相似度取前 N 个，全部放入 hint（让 LLM 自己判断）
        向量检索模式：使用阈值分类
        
        Returns:
            (prefill_matches, suggest_matches, hint_matches)
        """
        prefill = []    # ≥0.98，强制使用
        suggest = []    # 0.90-0.98，建议
        hint = []       # 0.70-0.90，参考
        
        # LLM 选表模式：简化处理，每个字段按相似度取前 N 个，全部放入 hint
        if self.llm_selection_mode:
            from server.config import settings
            max_per_field = settings.llm_table_selection_enum_per_field
            
            # 先按字段分组
            field_groups: Dict[str, List[Any]] = {}
            for match in enum_matches:
                field_id = getattr(match, 'field_id', None)
                if field_id not in field_groups:
                    field_groups[field_id] = []
                field_groups[field_id].append(match)
            
            # 每个字段内部按分数排序，取前 N 个
            for field_id, matches in field_groups.items():
                # 字段内部按分数排序
                sorted_field_matches = sorted(
                    matches,
                    key=lambda x: getattr(x, 'final_score', 0) or 0,
                    reverse=True
                )
                # 取前 N 个
                top_matches = sorted_field_matches[:max_per_field]
                
                for match in top_matches:
                    table_id = getattr(match, 'table_id', None)
                    is_main = table_id == self.main_table_id if table_id and self.main_table_id else False
                    
                    hint.append(EnumSuggestion(
                        field_id=match.field_id,
                        field_name=match.field_name,
                        value=match.value,
                        display_name=match.display_name,
                        confidence=match.final_score,
                        match_type=match.match_type,
                        table_id=table_id,
                        table_name=getattr(match, 'table_name', None),
                        is_main_table=is_main,
                        is_high_noise=False,
                    ))
            
            logger.info(
                "LLM模式枚举分类",
                total=len(enum_matches),
                hint=len(hint),
                fields=len(field_groups)
            )
            return prefill, suggest, hint
        
        # 向量检索模式：使用阈值分类
        for match in enum_matches:
            is_high_noise = self._is_high_noise_field(match.field_name)
            
            # 高噪声字段过滤：低分高噪声项不传给LLM
            if self.EXCLUDE_HIGH_NOISE_FROM_PROMPT and is_high_noise:
                if match.final_score < self.HIGH_NOISE_THRESHOLD:
                    logger.debug(
                        "高噪声字段低分项被过滤",
                        field=match.field_name,
                        value=match.value,
                        score=match.final_score
                    )
                    continue
            
            table_id = getattr(match, 'table_id', None)
            is_main = table_id == self.main_table_id if table_id and self.main_table_id else False
            
            suggestion = EnumSuggestion(
                field_id=match.field_id,
                field_name=match.field_name,
                value=match.value,
                display_name=match.display_name,
                confidence=match.final_score,
                match_type=match.match_type,
                table_id=table_id,
                table_name=getattr(match, 'table_name', None),
                is_main_table=is_main,
                is_high_noise=is_high_noise,
            )
            
            if match.final_score >= self.FORCE_USE_THRESHOLD:
                prefill.append(suggestion)
            elif match.final_score >= self.STRONG_SUGGEST_THRESHOLD:
                suggest.append(suggestion)
            elif match.final_score >= self.WEAK_HINT_THRESHOLD:
                hint.append(suggestion)
            # else: 丢弃
        
        logger.debug(
            "枚举值分类完成",
            prefill=len(prefill),
            suggest=len(suggest),
            hint=len(hint),
            discarded=len(enum_matches) - len(prefill) - len(suggest) - len(hint)
        )
        
        return prefill, suggest, hint
    
    def _group_by_field_across_tables(
        self,
        suggestions: List[EnumSuggestion]
    ) -> Dict[str, CrossTableFieldGroup]:
        """
        按字段名分组，识别跨表同名字段
        
        Returns:
            {field_name: CrossTableFieldGroup}
        """
        groups: Dict[str, CrossTableFieldGroup] = {}
        
        for sugg in suggestions:
            field_name = sugg.field_name
            if field_name not in groups:
                groups[field_name] = CrossTableFieldGroup(field_name=field_name)
            
            group = groups[field_name]
            
            # 检查该表是否已在分组中
            table_exists = any(
                t.get('table_id') == sugg.table_id 
                for t in group.tables
            )
            
            if not table_exists and sugg.table_id:
                group.tables.append({
                    'table_id': sugg.table_id,
                    'table_name': sugg.table_name,
                    'is_main_table': sugg.is_main_table,
                    'values': []
                })
            
            # 添加值到对应表
            for table_info in group.tables:
                if table_info.get('table_id') == sugg.table_id:
                    table_info['values'].append({
                        'value': sugg.value,
                        'display_name': sugg.display_name,
                        'confidence': sugg.confidence,
                        'match_type': sugg.match_type,
                    })
                    break
            
            # 检查是否存在跨表冲突
            if len(group.tables) > 1:
                group.has_conflict = True
        
        return groups
    
    def _serialize_channel_entries(
        self,
        matches: List[EnumSuggestion],
        channel: str,
    ) -> List[Dict[str, Any]]:
        """将分类结果序列化，便于写入 Trace/报告。"""
        serialized: List[Dict[str, Any]] = []
        for match in matches:
            serialized.append(
                {
                    "field_id": match.field_id,
                    "field_name": match.field_name,
                    "value": match.value,
                    "display_name": match.display_name,
                    "confidence": match.confidence,
                    "match_type": match.match_type,
                    "channel": channel,
                    # 新增：表信息
                    "table_id": match.table_id,
                    "table_name": match.table_name,
                    "is_main_table": match.is_main_table,
                    "is_high_noise": match.is_high_noise,
                }
            )
        return serialized

    def build_prefill_text(
        self,
        prefill_matches: List[EnumSuggestion]
    ) -> str:
        """
        构建预填充文本（在user_prompt开头）
        
        新增：跨表标注，同名字段显示表名
        
        示例：
        **已确认的过滤条件**（置信度≥98%，请必须包含）：
        - 行政区 = '武昌区' [精确匹配, 100%, 表:建设用地批准书(主表)]
        """
        if not prefill_matches:
            return ""
        
        lines = ["**已确认的过滤条件**（置信度≥98%，请必须包含）："]
        
        # 按字段分组检测跨表
        field_groups = self._group_by_field_across_tables(prefill_matches)
        
        for match in prefill_matches:
            match_type_label = {
                "exact": "精确匹配",
                "synonym": "同义词"
            }.get(match.match_type, "高置信度")
            
            # 展示层clamp: 百分比不能超过100%
            confidence_pct = min(100, int(match.confidence * 100))
            
            # 构建表标注
            table_label = ""
            group = field_groups.get(match.field_name)
            if group and (group.has_conflict or match.table_name):
                table_suffix = "(主表)" if match.is_main_table else ""
                table_label = f", 表:{match.table_name or '未知'}{table_suffix}"
                if group.has_conflict:
                    table_label += " ⚠️跨表"
            
            lines.append(
                f"- {match.field_name} = '{match.value}' "
                f"[{match_type_label}, {confidence_pct}%{table_label}]"
            )
        
        lines.append("")  # 空行分隔
        
        return "\n".join(lines)
    
    def build_function_suggestions(
        self,
        suggest_matches: List[EnumSuggestion]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        构建function参数中的enum_suggestions
        
        新增：包含表信息
        
        Returns:
            {
                "field_id_1": [
                    {"value": "武昌区", "display_name": "武昌区", "confidence": 0.95, 
                     "table_name": "建设用地批准书", "is_main_table": true}
                ],
                ...
            }
        """
        if not suggest_matches:
            return {}
        
        suggestions = {}
        
        for match in suggest_matches:
            if match.field_id not in suggestions:
                suggestions[match.field_id] = []
            
            suggestions[match.field_id].append({
                "value": match.value,
                "display_name": match.display_name,
                "confidence": match.confidence,
                "match_type": match.match_type,
                "table_name": match.table_name,
                "is_main_table": match.is_main_table,
            })
        
        return suggestions
    
    def build_hint_text(
        self,
        hint_matches: List[EnumSuggestion]
    ) -> str:
        """
        构建提示文本（在user_prompt末尾）
        
        新增：跨表标注
        
        示例：
        **可能相关的枚举值**（仅供参考）：
        - 所属街道: 洪山路街 (82%, 表:建设用地批准书)
        """
        if not hint_matches:
            return ""
        
        lines = ["\n**可能相关的枚举值**（仅供参考，置信度70%-90%）："]
        
        # 按字段分组检测跨表
        field_groups = self._group_by_field_across_tables(hint_matches)
        
        for match in hint_matches:
            # 展示层clamp: 百分比不能超过100%
            confidence_pct = min(100, int(match.confidence * 100))
            
            # 构建表标注
            table_label = ""
            group = field_groups.get(match.field_name)
            if group and (group.has_conflict or match.table_name):
                table_suffix = ",主表" if match.is_main_table else ""
                table_label = f", 表:{match.table_name or '未知'}{table_suffix}"
            
            lines.append(
                f"- {match.field_name}: {match.value} ({confidence_pct}%{table_label})"
            )
        
        return "\n".join(lines)
    
    def build_cross_table_warning(
        self,
        enum_matches: List[Any]
    ) -> Optional[str]:
        """
        构建跨表冲突警告文本
        
        Returns:
            警告文本，如果无冲突则返回None
        """
        # 按字段名分组
        field_tables: Dict[str, set] = defaultdict(set)
        for match in enum_matches:
            table_name = getattr(match, 'table_name', None)
            if table_name:
                field_tables[match.field_name].add(table_name)
        
        conflicts = []
        for field_name, tables in field_tables.items():
            if len(tables) > 1:
                conflicts.append(f"- {field_name}: 存在于 {', '.join(tables)}")
        
        if not conflicts:
            return None
        
        warning = "\n**⚠️ 跨表字段冲突警告**：以下字段存在于多个表中，请谨慎选择：\n"
        warning += "\n".join(conflicts)
        return warning
    
    def enhance_system_prompt(self, original_prompt: str) -> str:
        """
        增强system prompt，说明如何使用枚举值建议
        """
        enhancement = """

## 枚举值使用指南

系统已通过语义检索为您准备了枚举值匹配结果，分三个层次：

### 1. 已确认的过滤条件（置信度≥98%）
这些条件在用户问题开头列出，标记为"已确认"。
**处理方式**：**必须**在生成的filters中包含这些条件。

### 2. 建议的枚举值（置信度90%-98%）
通过function参数中的`enum_suggestions`字段提供。
**处理方式**：优先从建议列表中选择匹配的值，但需结合问题语境判断。

### 3. 参考的枚举值（置信度70%-90%）
在用户问题末尾列出，标记为"仅供参考"。
**处理方式**：可作为备选，但优先级低于前两者。

### 跨表字段处理
- 标注为"主表"的字段优先使用
- 标注"⚠️跨表"表示该字段存在于多个表中，需谨慎选择
- 优先使用与主表一致的枚举值

**注意事项**：
- 不要在返回的IR中包含`enum_suggestions`字段
- 如果系统提供的枚举值与问题语境不符，可以忽略
- 精确匹配（100%）的值必须使用
"""
        
        return original_prompt + enhancement
    
    def apply_strategy(
        self,
        original_user_prompt: str,
        enum_matches: List[Any]
    ) -> Tuple[str, Dict[str, Any], Dict[str, List[Dict[str, Any]]]]:
        """
        应用混合传递策略
        
        Args:
            original_user_prompt: 原始用户prompt
            enum_matches: 枚举值匹配结果
            
        Returns:
            (enhanced_user_prompt, enum_suggestions, prompt_metadata)
        """
        if not enum_matches:
            return original_user_prompt, {}, {}
        
        # 分类
        prefill, suggest, hint = self.categorize_matches(enum_matches)
        
        # LLM 选表模式：将枚举值内联到字段后面
        if self.llm_selection_mode and hint:
            enhanced_prompt = self._inline_enum_to_fields(original_user_prompt, hint)
            prompt_metadata = {
                "prefill": [],
                "suggest": [],
                "hint": self._serialize_channel_entries(hint, "hint"),
                "has_cross_table_conflict": False,
                "inline_mode": True,
            }
            logger.info(
                "LLM模式枚举内联",
                fields_enhanced=len(set(h.field_id for h in hint)),
                enum_count=len(hint)
            )
            return enhanced_prompt, {}, prompt_metadata
        
        # 向量检索模式：使用原有的块状提示
        # 构建增强内容
        prefill_text = self.build_prefill_text(prefill)
        enum_suggestions = self.build_function_suggestions(suggest)
        hint_text = self.build_hint_text(hint)
        cross_table_warning = self.build_cross_table_warning(enum_matches)
        
        # 增强user_prompt
        enhanced_prompt = ""
        
        if prefill_text:
            enhanced_prompt += prefill_text + "\n"
        
        if cross_table_warning:
            enhanced_prompt += cross_table_warning + "\n\n"
        
        enhanced_prompt += original_user_prompt
        
        if hint_text:
            enhanced_prompt += hint_text

        prompt_metadata = {
            "prefill": self._serialize_channel_entries(prefill, "prefill"),
            "suggest": self._serialize_channel_entries(suggest, "suggest"),
            "hint": self._serialize_channel_entries(hint, "hint"),
            "has_cross_table_conflict": cross_table_warning is not None,
        }
        
        return enhanced_prompt, enum_suggestions, prompt_metadata
    
    def _inline_enum_to_fields(
        self,
        original_prompt: str,
        hints: List[EnumSuggestion]
    ) -> str:
        """
        LLM 模式：将枚举值内联到对应字段行的后面
        
        例如：
        - **field_id** `使用状态`
        变为：
        - **field_id** `使用状态` 【可选值: 正常使用, 停用, 维修中, 预留】
        """
        import re
        
        # 按 field_id 分组枚举值
        field_enums: Dict[str, List[str]] = {}
        for hint in hints:
            if hint.field_id not in field_enums:
                field_enums[hint.field_id] = []
            field_enums[hint.field_id].append(hint.value)
        
        enhanced = original_prompt
        
        for field_id, values in field_enums.items():
            # 构建枚举值文本
            values_text = ", ".join(values[:10])  # 最多显示 10 个
            enum_suffix = f" 【可选值: {values_text}】"
            
            # 查找字段行并追加枚举值
            # 格式: - **field_id** `字段名`...
            pattern = rf"(\*\*{re.escape(field_id)}\*\*\s+`[^`]+`)([^\n]*)"
            
            def add_enum(match):
                field_part = match.group(1)
                rest = match.group(2)
                # 如果已经有【可选值】，不重复添加
                if "【可选值" in rest:
                    return match.group(0)
                return field_part + rest + enum_suffix
            
            enhanced = re.sub(pattern, add_enum, enhanced, count=1)
        
        return enhanced


def format_enum_matches_for_display(
    enum_matches: List[Any],
    max_per_field: int = 3
) -> str:
    """
    格式化枚举值匹配结果用于显示（调试/日志）
    
    Args:
        enum_matches: 匹配结果列表
        max_per_field: 每个字段最多显示几个
        
    Returns:
        格式化的文本
    """
    if not enum_matches:
        return "（无匹配结果）"
    
    # 按字段分组
    by_field = {}
    for match in enum_matches:
        if match.field_name not in by_field:
            by_field[match.field_name] = []
        by_field[match.field_name].append(match)
    
    lines = []
    for field_name, matches in by_field.items():
        lines.append(f"【{field_name}】")
        for i, match in enumerate(matches[:max_per_field], 1):
            match_type_emoji = {
                "exact": "✅",
                "synonym": "🔄",
                "value_vector": "🔍",
                "context_vector": "🎯"
            }.get(match.match_type, "")
            
            # 展示层clamp: 百分比不能超过100%
            confidence_pct = min(100, int(match.final_score * 100))
            
            # 添加表名标注
            table_label = ""
            table_name = getattr(match, 'table_name', None)
            if table_name:
                table_label = f" [{table_name}]"
            
            lines.append(
                f"  {i}. {match.value} "
                f"{match_type_emoji} {confidence_pct}%{table_label}"
            )
    
    return "\n".join(lines)
