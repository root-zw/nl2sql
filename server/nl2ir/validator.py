"""IR验证和修正模块"""

import re
from typing import List, Union, Any
import structlog

from server.models.ir import IntermediateRepresentation, MetricSpec
from server.config import settings

logger = structlog.get_logger()


def get_metric_field_id(metric_item: Union[str, MetricSpec, dict, Any]) -> str:
    """
    从 metrics 元素中提取字段ID
    
    支持两种格式：
    1. 字符串：直接返回
    2. MetricSpec 对象或字典：提取 field 字段
    
    Args:
        metric_item: metrics 列表中的元素
        
    Returns:
        字段ID字符串
    """
    if isinstance(metric_item, str):
        return metric_item
    elif isinstance(metric_item, MetricSpec):
        return metric_item.field
    elif isinstance(metric_item, dict):
        return metric_item.get("field", str(metric_item))
    else:
        return str(metric_item)


class IRStrictModeError(Exception):
    """IR 严格模式错误：当启用 IR_STRICT_MODE 时，任何修复操作都会抛出此异常"""
    pass


class IRValidator:
    """IR验证和修正

    用于修正LLM可能产生的理解偏差，例如：
    1. 将被精确过滤的维度从dimensions中移除
    2. 验证时间对象和年份过滤的一致性
    """

    def __init__(self, semantic_model=None):
        """
        初始化验证器

        Args:
            semantic_model: 语义模型（可选），用于从配置读取业务规则
        """
        self.semantic_model = semantic_model
        # 记录验证与修正备注（用于trace）
        self._notes: list[str] = []

    def _add_note(self, text: str, ir: IntermediateRepresentation = None, action: str = None, field: str = None, original=None, fixed=None):
        """添加一条修正备注，并可选地记录到 IR 的修复日志
        
        Args:
            text: 修正备注文本
            ir: IR 对象（可选，用于记录修复日志）
            action: 修复动作类型（可选）
            field: 涉及的字段（可选）
            original: 原始值（可选）
            fixed: 修复后的值（可选）
            
        Raises:
            IRStrictModeError: 当启用 IR_STRICT_MODE 时抛出
        """
        try:
            if text:
                self._notes.append(str(text))
                # 同时记录到 IR 的修复日志
                if ir is not None and hasattr(ir, 'add_fix_log'):
                    ir.add_fix_log(
                        stage="validator",
                        action=action or "fix",
                        field=field,
                        original=original,
                        fixed=fixed,
                        reason=text
                    )
                # 严格模式下抛出错误
                if settings.ir_strict_mode:
                    raise IRStrictModeError(
                        f"IR 严格模式：检测到修复操作 [{action or 'fix'}] - {text}\n"
                        f"字段: {field}, 原始值: {original}, 修复值: {fixed}"
                    )
        except IRStrictModeError:
            raise  # 重新抛出严格模式错误
        except Exception:
            pass

    def get_notes(self) -> list:
        """获取修正备注副本"""
        return list(self._notes)

    def validate_and_fix(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """验证并修正IR"""
        # 重置备注
        self._notes = []
        #  顺序很重要！
        # 0. 跨分区查询一致性验证（最先执行，确保后续逻辑正确）
        ir = self._validate_cross_partition_consistency(ir)
        # 1. 先应用值标准化（如"武汉市"→IN 16个区）
        ir = self._apply_value_normalization(ir)
        # 2. 验证时间
        ir = self._validate_time_and_filters(ir)
        # 2.1 趋势问法与明确同比/环比分流
        ir = self._normalize_trend_comparison_intent(ir)
        # 3. 展开值包含关系（如"住宅用地"展开为"住宅用地"+"住宅、商服用地"）
        ir = self._expand_value_includes(ir)
        # 4. 最后移除冗余维度（此时filter已经是正确的IN操作了）
        ir = self._remove_filtered_dimensions(ir)
        # 5. 明细查询：信任LLM选择的展示字段，仅做字段类型修正（度量→metrics）
        ir = self._normalize_detail_dimensions_by_question(ir)
        # 6. 明细查询：守护——将误放在 metrics 中的维度/标识字段移到 dimensions（避免编译报错）
        ir = self._sanitize_detail_fields(ir)
        # 7. 窗口函数明细查询：验证必需字段
        ir = self._validate_window_detail_query(ir)
        # 8. 确保当用户使用"分别/各自"等触发词并列出多个具体值时，进行分组显示
        ir = self._ensure_grouping_for_multivalue_filters(ir)
        # 9. 去重 filters（新增）
        ir = self._deduplicate_filters(ir)
        # 10. 过滤字段对齐到主表（跨表同名字段/年份误挂字段纠偏）
        ir = self.align_filters_to_primary_table(ir)
        # 11. 维度字段对齐到主表（跨分区对比查询时移除非主表字段）
        ir = self._align_dimensions_to_primary_table(ir)
        # 12. 验证混合架构扩展字段（calculated_fields, conditional_metrics, ratio_metrics）
        ir = self._validate_extended_fields(ir)
        return ir

    def _normalize_trend_comparison_intent(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        将“趋势/变化情况”与“明确同比/环比”分流。

        原则：
        - 用户明确说了“同比/环比/去年同期/增长率/涨跌幅”等对比语义时，保留 comparison_type
        - 仅出现“趋势/走势/变化情况”等趋势语义，且查询覆盖多个时间点时，不默认做同比
        """
        comparison_type = getattr(ir, "comparison_type", None)
        if not comparison_type:
            return ir

        question = str(getattr(ir, "original_question", "") or "")
        normalized_question = re.sub(r"\s+", "", question).lower()
        if not normalized_question:
            return ir

        if not self._has_multi_period_scope(ir):
            return ir

        explicit_comparison_keywords = (
            "同比",
            "环比",
            "去年同期",
            "上年同期",
            "较上年",
            "较去年",
            "较上月",
            "较上季度",
            "较上周",
            "比上年",
            "比去年",
            "比上月",
            "比上季度",
            "比上周",
            "相比",
            "对比",
            "vs",
            "增长率",
            "变化率",
            "涨幅",
            "降幅",
            "涨跌",
            "增长",
            "下降",
            "上升",
        )
        if any(keyword in normalized_question for keyword in explicit_comparison_keywords):
            return ir

        trend_keywords = (
            "趋势",
            "走势",
            "变化情况",
            "变化趋势",
            "历年变化",
            "波动情况",
            "波动趋势",
        )
        if not any(keyword in normalized_question for keyword in trend_keywords):
            return ir

        old_comparison_type = ir.comparison_type
        old_show_growth_rate = bool(getattr(ir, "show_growth_rate", False))
        old_show_previous = bool(getattr(ir, "show_previous_period_value", False))

        ir.comparison_type = None
        ir.show_growth_rate = False
        ir.show_previous_period_value = False
        self._add_note(
            "检测到多期趋势问法且缺少明确同比/环比关键词，已取消默认同比展示，仅保留按时间展开的趋势结果。",
            ir=ir,
            action="clear_comparison_for_trend",
            field="comparison_type",
            original={
                "comparison_type": old_comparison_type,
                "show_growth_rate": old_show_growth_rate,
                "show_previous_period_value": old_show_previous,
            },
            fixed={
                "comparison_type": None,
                "show_growth_rate": False,
                "show_previous_period_value": False,
            },
        )
        logger.info(
            "趋势问法取消默认同比展示",
            comparison_type=old_comparison_type,
            show_growth_rate=old_show_growth_rate,
            question=question[:120],
        )
        return ir

    def _has_multi_period_scope(self, ir: IntermediateRepresentation) -> bool:
        """判断查询是否覆盖多个时间点。"""
        time_range = getattr(ir, "time", None)
        if time_range and getattr(time_range, "type", None) == "relative":
            last_n = int(getattr(time_range, "last_n", 0) or 0)
            if last_n > 1:
                return True

        for filter_obj in getattr(ir, "filters", []) or []:
            field_id = str(getattr(filter_obj, "field", "") or "")
            if not field_id or not self._is_year_like_field(field_id):
                continue

            op = getattr(filter_obj, "op", None)
            value = getattr(filter_obj, "value", None)
            if op == "IN" and isinstance(value, list) and len(value) > 1:
                return True
            if op in [">=", ">", "<=", "<", "BETWEEN"]:
                return True

        return False

    def _validate_cross_partition_consistency(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        验证跨分区查询的一致性，修正冲突的配置。
        
        解决的问题：
        1. cross_partition_query=true 和 comparison_type 互斥
           - 跨分区对比通过 CTE+JOIN 实现变化率计算
           - 单表同比通过 LAG 窗口函数实现
           - 两者不应同时存在
        
        2. cross_partition_query=true 时必须有多表
           - 如果 selected_table_ids 只有1个表，则不应该是跨分区查询
        
        3. compare 模式需要至少2个表
           - 如果表数量不足，自动调整为单表查询
        """
        # 检查 1：cross_partition_query 和 comparison_type 互斥
        if getattr(ir, "cross_partition_query", False) and getattr(ir, "comparison_type", None):
            # 跨分区查询优先，清除 comparison_type
            old_comparison_type = ir.comparison_type
            ir.comparison_type = None
            ir.show_growth_rate = False
            self._add_note(
                f"跨分区查询不支持 comparison_type，已清除 comparison_type={old_comparison_type}。"
                f"变化率通过跨分区对比(CTE+JOIN)计算，而非窗口函数(LAG)。"
            )
            logger.warning(
                "跨分区查询与comparison_type互斥",
                cross_partition_mode=getattr(ir, "cross_partition_mode", None),
                old_comparison_type=old_comparison_type
            )
        
        # 检查 2：cross_partition_query=true 时必须有多表
        selected_table_ids = getattr(ir, "selected_table_ids", []) or []
        if getattr(ir, "cross_partition_query", False):
            if len(selected_table_ids) < 2:
                # 只有1个表，不应该是跨分区查询
                ir.cross_partition_query = False
                ir.cross_partition_mode = "union"  # 重置为默认值
                self._add_note(
                    f"只有1个表(selected_table_ids={selected_table_ids})，"
                    "已将 cross_partition_query 设为 False。"
                )
                logger.warning(
                    "跨分区查询需要多表，已自动修正为单表查询",
                    selected_table_ids=selected_table_ids
                )
        
        # 检查 3：compare 模式至少需要2个表
        if getattr(ir, "cross_partition_mode", None) == "compare":
            if len(selected_table_ids) < 2:
                # 表数量不足，无法对比
                ir.cross_partition_query = False
                ir.cross_partition_mode = "union"
                self._add_note(
                    f"对比模式需要至少2个表，当前只有{len(selected_table_ids)}个，"
                    "已调整为单表查询。"
                )
                logger.warning(
                    "对比模式表数量不足",
                    selected_table_ids=selected_table_ids,
                    expected_min=2
                )
        
        return ir

    def _align_dimensions_to_primary_table(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        对齐维度字段到主表，移除不存在于主表中的维度。
        
        解决的问题：
        - 跨分区对比查询时，LLM 可能使用了只存在于非主表的字段（如2024年表的"数据年份"）
        - 对于 cross_partition_mode=compare 的查询，年份区分由表本身决定，不需要年份维度
        
        策略：
        1. 如果维度字段ID不在主表中，尝试按显示名在主表中查找
        2. 如果找不到对应字段，且是跨分区对比查询，则移除该维度
        3. 记录移除日志
        """
        try:
            if not self.semantic_model or not ir.dimensions:
                return ir
            
            primary_table_id = getattr(ir, "primary_table_id", None)
            if not primary_table_id:
                return ir
            
            # 获取主表的所有字段
            primary_table_fields = {}  # field_id -> field_name
            primary_table_field_names = {}  # field_name -> field_id
            
            # 从 model.fields 获取主表字段
            fields = getattr(self.semantic_model, "fields", {}) or {}
            for field_id, field in fields.items():
                if getattr(field, "datasource_id", None) == primary_table_id:
                    name = getattr(field, "display_name", "") or getattr(field, "name", "")
                    primary_table_fields[field_id] = name
                    if name:
                        primary_table_field_names[name] = field_id
            
            # 从 model.dimensions 和 model.measures 补充
            for dim_id, dim in (getattr(self.semantic_model, "dimensions", {}) or {}).items():
                if getattr(dim, "table", None) == primary_table_id:
                    name = getattr(dim, "display_name", "") or getattr(dim, "name", "")
                    primary_table_fields[dim_id] = name
                    if name:
                        primary_table_field_names[name] = dim_id
            
            for m_id, m in (getattr(self.semantic_model, "measures", {}) or {}).items():
                if getattr(m, "table", None) == primary_table_id:
                    name = getattr(m, "display_name", "") or getattr(m, "name", "")
                    primary_table_fields[m_id] = name
                    if name:
                        primary_table_field_names[name] = m_id
            
            if not primary_table_fields:
                return ir
            
            # 是否为跨分区对比查询
            is_cross_partition_compare = (
                getattr(ir, "cross_partition_query", False) and 
                getattr(ir, "cross_partition_mode", None) == "compare"
            )
            
            # 获取用户原问题，用于检查用户是否明确提及某个维度
            original_question = getattr(ir, "original_question", "") or ""
            
            def is_dimension_mentioned_in_question(dim_id: str) -> bool:
                """检查维度是否在用户问题中被明确提及"""
                if not original_question:
                    return False
                
                # 获取维度的显示名和同义词
                field = fields.get(dim_id)
                if not field:
                    return False
                
                display_name = getattr(field, "display_name", "") or ""
                synonyms = getattr(field, "synonyms", []) or []
                
                # 检查显示名是否在问题中
                if display_name and display_name in original_question:
                    return True
                
                # 检查同义词是否在问题中
                for synonym in synonyms:
                    if synonym and synonym in original_question:
                        return True
                
                return False
            
            new_dimensions = []
            removed_dimensions = []
            
            for dim_id in ir.dimensions:
                # 字段ID在主表中
                if dim_id in primary_table_fields:
                    new_dimensions.append(dim_id)
                    continue
                
                # 尝试按显示名查找（dim_id 可能是显示名而非UUID）
                if dim_id in primary_table_field_names:
                    # 转换为主表的字段ID
                    new_id = primary_table_field_names[dim_id]
                    new_dimensions.append(new_id)
                    self._add_note(
                        f"维度字段 '{dim_id}' 转换为主表字段ID: {new_id}",
                        ir=ir, action="remap_dimension", field=new_id, original=dim_id, fixed=new_id
                    )
                    logger.debug(
                        "维度字段按显示名对齐到主表",
                        original=dim_id,
                        new_id=new_id,
                        primary_table_id=primary_table_id
                    )
                    continue
                
                # 字段不在主表中
                if is_cross_partition_compare:
                    # 检查用户是否明确提及该维度
                    if is_dimension_mentioned_in_question(dim_id):
                        # 用户明确提及的维度，保留并记录
                        new_dimensions.append(dim_id)
                        self._add_note(
                            f"保留用户明确提及的维度: {dim_id}",
                            ir=ir, action="keep_mentioned_dimension", field=dim_id
                        )
                        logger.info(
                            "跨分区对比查询：保留用户明确提及的维度字段",
                            dimension=dim_id,
                            primary_table_id=primary_table_id,
                            reason="用户问题中明确提及该维度"
                        )
                    else:
                        # 跨分区对比查询，移除非主表且用户未明确提及的维度
                        removed_dimensions.append(dim_id)
                        self._add_note(
                            f"移除非主表维度: {dim_id}（跨分区对比查询不需要年份维度）",
                            ir=ir, action="remove_dimension", field=dim_id
                        )
                        logger.warning(
                            "跨分区对比查询：移除非主表维度字段",
                            dimension=dim_id,
                            primary_table_id=primary_table_id,
                            reason="年份区分由表本身决定"
                        )
                else:
                    # 非跨分区查询，保留但记录警告
                    new_dimensions.append(dim_id)
                    logger.warning(
                        "维度字段不在主表中",
                        dimension=dim_id,
                        primary_table_id=primary_table_id
                    )
            
            if removed_dimensions:
                ir.dimensions = new_dimensions
                logger.info(
                    "维度字段对齐完成",
                    removed=removed_dimensions,
                    remaining=new_dimensions
                )
            
            return ir
            
        except Exception as e:
            logger.exception("_align_dimensions_to_primary_table 处理异常", error=str(e))
            return ir

    def align_filters_to_primary_table(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """在已确定 primary_table_id 后执行主表对齐（可在 parser 里二次调用）。"""
        return self._align_filters_to_primary_table(ir)

    def _align_filters_to_primary_table(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        将 filters 尽量对齐到 IR.primary_table_id 对应的数据表字段，提升“条件字段落地到 SQL”的稳定性。

        解决的典型问题：
        - 枚举/字段ID来自别的年份表或别的表，导致编译阶段无法落地（SQL 丢值）。
        - 年份列表（如 2020..2024）被错误挂到“行政区/区划”等非年份字段上。

        策略（保守、可迁移到其他数据源）：
        1) 若 filter.field 不属于 primary_table_id，但主表存在同 display_name 字段，则重写 field_id；
        2) 若值形态为“年份列表”，但字段非年份类 → 尝试改挂到主表的年份字段；
        3) 若无法对齐且该条件值未在原问题出现（疑似模型推断噪声），则移除该 filter，避免“丢值误报/无效条件”。
        """
        try:
            if not self.semantic_model or not ir.filters:
                return ir
            primary_table_id = getattr(ir, "primary_table_id", None)
            if not primary_table_id:
                return ir

            question = (ir.original_question or "").strip()

            def value_mentioned(v) -> bool:
                if v is None:
                    return True
                if isinstance(v, list):
                    return all(value_mentioned(x) for x in v)
                s = str(v)
                if s not in question:
                    return False
                # 若该值只是“表名/数据源名称”的子串出现（而不是用户明确条件），则不视为明确提及
                try:
                    for _, ds in (getattr(self.semantic_model, "datasources", {}) or {}).items():
                        name = str(getattr(ds, "display_name", "") or "").strip()
                        if name and name in question and s in name:
                            return False
                except Exception:
                    pass
                return True

            def is_year_str(s: str) -> bool:
                if not s:
                    return False
                if len(s) != 4 or not s.isdigit():
                    return False
                y = int(s)
                return 1900 <= y <= 2100

            def all_years(v) -> bool:
                if v is None:
                    return False
                if isinstance(v, list):
                    vals = [str(x) for x in v if x is not None]
                    return bool(vals) and all(is_year_str(x) for x in vals)
                return is_year_str(str(v))

            # 构建主表 display_name -> field_id 映射
            primary_display_to_id: dict[str, str] = {}
            primary_year_field_id: str | None = None

            for fid, fld in (getattr(self.semantic_model, "fields", {}) or {}).items():
                try:
                    if str(getattr(fld, "datasource_id", "")) != str(primary_table_id):
                        continue
                    if not getattr(fld, "is_active", True):
                        continue
                    display = str(getattr(fld, "display_name", "") or "").strip()
                    if display:
                        primary_display_to_id.setdefault(display, str(fid))
                    # 年份字段候选：timestamp / display_name 含“年/year”
                    cat = str(getattr(fld, "field_category", "") or "")
                    if primary_year_field_id is None:
                        if cat == "timestamp":
                            primary_year_field_id = str(fid)
                        elif display and (("年" in display) or ("year" in display.lower())):
                            primary_year_field_id = str(fid)
                except Exception:
                    continue

            if not primary_display_to_id:
                return ir

            new_filters = []
            removed = 0
            remapped = 0
            year_fixed = 0

            for f in list(ir.filters or []):
                try:
                    field_id = str(getattr(f, "field", "") or "")
                    field_obj = (getattr(self.semantic_model, "fields", {}) or {}).get(field_id)
                    if not field_obj:
                        new_filters.append(f)
                        continue
                    field_table_id = str(getattr(field_obj, "datasource_id", "") or "")
                    display_name = str(getattr(field_obj, "display_name", "") or "").strip()
                    display_lower = display_name.lower()

                    # (A) 年份列表挂错字段：优先修正到主表年份字段
                    if all_years(f.value):
                        is_year_field = ("年" in display_name) or ("year" in display_lower) or (getattr(field_obj, "field_category", None) == "timestamp")
                        if (not is_year_field) and primary_year_field_id:
                            old = f.field
                            f.field = primary_year_field_id
                            year_fixed += 1
                            self._add_note(
                                f"纠偏：将年份条件从 {old}({display_name}) 迁移到主表年份字段 {primary_year_field_id}",
                                ir=ir, action="remap_year_filter", field=primary_year_field_id, original=old, fixed=primary_year_field_id
                            )

                    # (B) 字段不在主表：仅按 display_name 精确匹配映射到主表同名字段
                    # 注意：移除了模糊匹配逻辑，避免错误映射
                    if field_table_id and field_table_id != str(primary_table_id):
                        mapped = primary_display_to_id.get(display_name) if display_name else None
                        if mapped and mapped != field_id:
                            old = f.field
                            f.field = mapped
                            remapped += 1
                            self._add_note(
                                f"纠偏：将过滤字段从 {old}({display_name}) 映射到主表字段 {mapped}",
                                ir=ir, action="remap_filter_to_primary", field=mapped, original=old, fixed=mapped
                            )
                        else:
                            # (C) 无法映射且值未在问题中出现：认为是推断噪声，移除
                            if not value_mentioned(f.value):
                                removed += 1
                                self._add_note(
                                    f"移除：无法对齐到主表且值未出现在问题中的过滤条件 {display_name or field_id}={f.value}",
                                    ir=ir, action="remove_unaligned_filter", field=field_id, original=f.value
                                )
                                continue

                    new_filters.append(f)
                except Exception:
                    new_filters.append(f)

            if removed or remapped or year_fixed:
                ir.filters = new_filters
                logger.info(
                    "IR filters 主表对齐完成",
                    removed=removed,
                    remapped=remapped,
                    year_fixed=year_fixed,
                    remaining=len(new_filters),
                )

            # 进一步纠偏：若存在“短值/长值”并存（如 工业 vs 工业用地），且长值在问题中出现，
            # 则优先用长值替换短值（更贴近用户意图），并删除冗余长值过滤（若存在）。
            try:
                q = (ir.original_question or "").strip()
                if q and ir.filters:
                    # 收集字符串等值过滤
                    eq_filters = [f for f in ir.filters if getattr(f, "op", None) == "=" and isinstance(getattr(f, "value", None), str)]
                    if len(eq_filters) >= 2:
                        to_remove = set()
                        for f_short in eq_filters:
                            v_short = (f_short.value or "").strip()
                            if not v_short:
                                continue
                            for f_long in eq_filters:
                                if f_long is f_short:
                                    continue
                                v_long = (f_long.value or "").strip()
                                if not v_long or len(v_long) <= len(v_short):
                                    continue
                                if v_short in v_long and v_long in q:
                                    # 将短值升级为长值
                                    f_short.value = v_long
                                    to_remove.add(id(f_long))
                                    self._add_note(
                                        f"纠偏：将过滤值从 '{v_short}' 升级为 '{v_long}'（来自原问题）",
                                        ir=ir, action="upgrade_filter_value", original=v_short, fixed=v_long
                                    )
                        if to_remove:
                            ir.filters = [f for f in ir.filters if id(f) not in to_remove]
            except Exception:
                pass

            return ir
        except Exception as e:
            logger.exception("_align_filters_to_primary_table 处理异常", error=str(e))
            return ir
    
    def _deduplicate_filters(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        去重 filters：移除重复的 (field, op, value) 组合
        
        原因：校验/补滤阶段可能重复添加相同过滤条件
        """
        if not ir.filters:
            return ir
        
        seen = set()
        unique_filters = []
        removed_count = 0
        
        for f in ir.filters:
            # 构建去重键
            key = (f.field, f.op, str(f.value) if f.value is not None else None)
            
            if key not in seen:
                seen.add(key)
                unique_filters.append(f)
            else:
                removed_count += 1
        
        if removed_count > 0:
            ir.filters = unique_filters
            self._add_note(f"去重 filters：移除 {removed_count} 个重复过滤条件")
            logger.debug(
                "IR filters 去重完成",
                removed_count=removed_count,
                remaining_count=len(unique_filters)
            )
        
        return ir

    def _sanitize_detail_fields(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        明细查询字段守护：
        - 当 query_type = detail 时，若 metrics 中出现非度量字段（维度/标识/时间），自动移至 dimensions；
        - 保留 measure 与以 'derived:' 开头的派生指标在 metrics 中；
        - 避免重复加入 dimensions，保持原有顺序并将迁移的字段追加到末尾。

        目的：避免 LLM 误把如“竞得人”之类的维度放进 metrics 导致编译报错。
        """
        try:
            # 仅处理普通明细查询
            if ir.query_type != "detail" or not self.semantic_model:
                return ir

            if not ir.metrics:
                return ir

            moved = []
            kept_metrics = []  # 保留原始格式（字符串或 MetricSpec）

            # 构建便捷映射
            measures = set(getattr(self.semantic_model, 'measures', {}).keys())
            fields = getattr(self.semantic_model, 'fields', {}) or {}

            for metric_item in ir.metrics:
                # 提取字段ID用于比较
                mid = get_metric_field_id(metric_item)
                
                # 保留派生指标
                if isinstance(mid, str) and mid.startswith('derived:'):
                    kept_metrics.append(metric_item)
                    continue

                # 明确为度量 → 保留
                if mid in measures:
                    kept_metrics.append(metric_item)
                    continue

                # 其他：尝试根据字段分类判定
                fld = fields.get(mid)
                if fld and getattr(fld, 'field_category', None) in ('dimension', 'identifier', 'timestamp'):
                    # 移动到dimensions（只移动字段ID，不移动整个 MetricSpec）
                    if mid not in ir.dimensions:
                        ir.dimensions.append(mid)
                    moved.append(mid)
                    continue

                # 无法判定，保守起见保留在metrics，但记录警告
                kept_metrics.append(metric_item)
                logger.warning(
                    "明细守护：无法判定metrics项的字段类别，暂不迁移",
                    field_id=mid
                )

            if moved:
                logger.debug(
                    "明细守护：将非度量字段从metrics迁移到dimensions",
                    moved=moved,
                    final_dimensions=ir.dimensions
                )
                self._add_note(f"明细守护：将非度量字段从metrics迁移到dimensions: {moved}")

            ir.metrics = kept_metrics
            return ir
        except Exception as e:
            logger.exception("_sanitize_detail_fields 处理异常", error=str(e))
            return ir

    def _normalize_detail_dimensions_by_question(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        明细查询维度处理（优化版）：
        
        设计理念：信任LLM智能选择的展示字段，仅做必要的字段类型修正。
        - LLM根据问题上下文推断相关的展示字段
        - 程序只负责兜底：当 dimensions 和 metrics 均为空时，由编译器使用 show_in_detail=True 的默认列
        
        处理内容：
        1. 将误放在 dimensions 中的度量字段迁移到 metrics
        2. 移除空间/几何字段（这类字段需要特殊处理，不适合直接展示）
        3. 信任LLM选择的其他字段，不再根据"用户是否在问题中提及"来删除
        """
        try:
            # 只处理普通明细查询
            if ir.query_type != "detail":
                return ir

            if not self.semantic_model:
                return ir

            # 辅助函数：判断字段类型
            def is_measure(field_id: str) -> bool:
                try:
                    if hasattr(self.semantic_model, 'measures') and field_id in getattr(self.semantic_model, 'measures', {}):
                        return True
                    if hasattr(self.semantic_model, 'fields') and field_id in self.semantic_model.fields:
                        fld = self.semantic_model.fields[field_id]
                        return getattr(fld, 'field_category', '') == 'measure'
                except Exception:
                    pass
                return False

            def is_geometry_field(field_id: str) -> bool:
                """判断是否为几何/空间字段（不适合直接展示）"""
                try:
                    if hasattr(self.semantic_model, 'fields') and field_id in self.semantic_model.fields:
                        fld = self.semantic_model.fields[field_id]
                        cat = getattr(fld, 'field_category', '') or ''
                        if cat in ('geometry', 'spatial', 'geometry/spatial'):
                            return True
                        # 也检查数据类型
                        data_type = getattr(fld, 'data_type', '') or ''
                        if data_type.lower() in ('geometry', 'geography'):
                            return True
                except Exception:
                    pass
                return False

            if not ir.dimensions:
                return ir

            original_dims = list(ir.dimensions)
            
            # 1) 移除被误加的"度量"字段，并迁移到 metrics
            misplaced_measures = [fid for fid in original_dims if is_measure(fid)]
            ir.dimensions = [fid for fid in original_dims if not is_measure(fid)]

            if misplaced_measures:
                # 提取现有 metrics 的字段ID（使用模块级辅助函数）
                existing_metric_ids = {get_metric_field_id(m) for m in (ir.metrics or [])}
                added = []
                for mid in misplaced_measures:
                    if mid not in existing_metric_ids:
                        ir.metrics.append(mid)
                        existing_metric_ids.add(mid)
                        added.append(mid)
                if added:
                    logger.debug(
                        "明细守护：从dimensions移除度量字段并加入metrics",
                        moved=added
                    )
                    self._add_note(f"明细守护：从dimensions移除了度量字段并加入metrics: {added}")

            # 2) 移除几何/空间字段（这类字段需要特殊转换才能展示，由编译器处理）
            geometry_fields = [fid for fid in ir.dimensions if is_geometry_field(fid)]
            if geometry_fields:
                ir.dimensions = [fid for fid in ir.dimensions if not is_geometry_field(fid)]
                logger.debug(
                    "明细查询移除空间字段",
                    removed=geometry_fields
                )

            # 3) 信任LLM选择的其他字段，不再删除
            # 如果 dimensions 和 metrics 都为空，由编译器使用 show_in_detail=True 的默认列兜底

            if ir.dimensions or ir.metrics:
                logger.debug(
                    "明细查询保留LLM选择的展示字段",
                    dimensions=ir.dimensions,
                    metrics=ir.metrics
                )
            else:
                logger.debug("明细查询无展示字段，将使用默认明细列兜底")

            return ir
        except Exception as e:
            logger.exception("_normalize_detail_dimensions_by_question 处理异常", error=str(e))
            return ir

    def _remove_filtered_dimensions(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        移除被精确过滤的维度（仅聚合查询）

        逻辑：如果对某个维度使用了精确过滤（op="=" 或 op="IN"），则不应该在dimensions中包含该维度

        例外情况：
        1. 明细查询不处理：dimensions 用于指定显示列，不是分组，用户可能希望在结果中看到被过滤的字段
        2. 如果字段配置了includes_values且发生了值展开，则保留该维度（用于分组显示）

        例如1（正常移除）：
        - 问题："2024年的成交量"
        - LLM可能错误生成：dimensions=["deal_year"], filters=[{field: "deal_year", op: "=", value: 2024}]
        - 修正后：dimensions=[], filters=[{field: "deal_year", op: "=", value: 2024}]

        例如2（保留，因为有值展开）：
        - 问题："住宅用地的数量"
        - 值展开后：filters=[{field: "用途", op: "IN", value: ["住宅用地", "住宅、商服用地"]}], dimensions=["用途"]
        - 保留维度：dimensions=["用途"]（因为需要分组显示各个值）
        """
        if not ir.filters or not ir.dimensions:
            return ir

        # 明细查询不移除：dimensions 用于指定显示列，不是分组
        # 用户可能希望在结果中看到被过滤的字段（如"查一下2024年洪山区的地块，显示年份和行政区"）
        if ir.query_type == "detail":
            logger.debug(
                "明细查询跳过移除被过滤维度",
                dimensions=ir.dimensions,
                reason="明细查询的dimensions用于指定显示列，不是分组"
            )
            return ir

        # 分类查询特殊处理：用户说"各类/各种/不同类型/分别"时，需要按类型分组显示
        question = (ir.original_question or "").strip()
        classification_keywords = ["各类", "各种", "不同类型", "分别", "分类", "按类型", "按类别"]
        is_classification_query = any(kw in question for kw in classification_keywords)
        
        if is_classification_query:
            # 检测举例特征词（表示用户只是举例，不是要过滤）
            example_indicators = ["等", "如", "比如", "包括", "例如"]
            has_example_indicator = any(ind in question for ind in example_indicators)
            
            if has_example_indicator and ir.dimensions and ir.filters:
                # 移除与 dimensions 相同字段的 IN 单值过滤（这些可能是举例被误当成过滤）
                dimension_set = set(ir.dimensions)
                original_filters = ir.filters.copy()
                ir.filters = [
                    f for f in ir.filters
                    if not (
                        f.field in dimension_set and 
                        f.op == "IN" and 
                        isinstance(f.value, list) and 
                        len(f.value) == 1
                    )
                ]
                removed_filters = [f for f in original_filters if f not in ir.filters]
                if removed_filters:
                    logger.debug(
                        "分类查询：移除举例误当过滤的条件",
                        removed=[{"field": f.field, "value": f.value} for f in removed_filters],
                        reason="用户问题包含分类词+举例词，这些条件可能是举例而非过滤"
                    )
            
            logger.debug(
                "分类查询保留维度",
                dimensions=ir.dimensions,
                reason="用户问题包含分类关键词，需要按类型分组显示"
            )
            return ir

        # 获取字段枚举值配置
        field_enums = getattr(self.semantic_model, 'field_enums', {}) if self.semantic_model else {}

        # 找出被 "=" 或 "IN" 精确过滤的字段，但排除以下情况：
        # 1. 有值展开配置的字段
        # 2. IN 操作符且有多个值的字段（用户需要分组显示）
        filtered_fields = set()
        for f in ir.filters:
            if f.op not in ["=", "IN"]:
                continue

            # 如果是 IN 且有多个值，保留维度（用户需要分组显示各值）
            if f.op == "IN" and isinstance(f.value, list) and len(f.value) > 1:
                logger.debug(
                    "保留维度：IN过滤有多个值",
                    field=f.field,
                    value_count=len(f.value)
                )
                continue

            # 检查该字段是否配置了includes_values
            has_includes = False
            if f.field in field_enums:
                for enum in field_enums[f.field]:
                    if enum.includes_values:
                        has_includes = True
                        break

            # 如果有includes配置，说明可能发生了值展开，保留该维度
            if not has_includes:
                filtered_fields.add(f.field)

        # 从dimensions中移除这些字段
        original_dims = ir.dimensions.copy()
        ir.dimensions = [
            dim for dim in ir.dimensions
            if dim not in filtered_fields
        ]

        removed = set(original_dims) - set(ir.dimensions)
        if removed:
            logger.debug(
                "移除冗余分组维度",
                removed=list(removed),
                reason="这些维度已被过滤（= 或 IN），且未配置值展开，不应该再分组",
                original_dims=original_dims,
                corrected_dims=ir.dimensions
            )

        return ir

    def _expand_value_includes(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        展开值包含关系

        当枚举值配置了includes_values时，自动展开查询范围并添加分组维度。

        示例：
        - 配置："住宅、商服用地" includes_values = ["住宅用地", "商服用地"]
        - 用户查询："住宅用地的数量"
        - 展开前：filters=[{field: "用途", op: "IN", value: ["住宅用地"]}], dimensions=[]
        - 展开后：filters=[{field: "用途", op: "IN", value: ["住宅用地", "住宅、商服用地"]}], dimensions=["用途"]
        - 结果：显示"住宅用地"和"住宅、商服用地"两行数据及合计
        """
        if not self.semantic_model or not ir.filters:
            return ir

        # 获取字段枚举值配置
        field_enums = getattr(self.semantic_model, 'field_enums', {})
        if not field_enums:
            return ir

        # 记录哪些字段进行了值展开（需要添加到dimensions）
        expanded_fields = set()

        for filter_cond in ir.filters:
            field_id = filter_cond.field

            # 只处理IN操作
            if filter_cond.op not in ["IN", "="]:
                continue

            # 获取该字段的枚举值配置
            if field_id not in field_enums:
                continue

            enum_values = field_enums[field_id]

            # 构建值到包含关系的映射
            includes_map = {}
            for enum in enum_values:
                if enum.includes_values:
                    includes_map[enum.standard_value] = enum.includes_values

            if not includes_map:
                continue

            # 获取过滤值
            if filter_cond.op == "=":
                filter_values = [filter_cond.value]
            else:  # IN
                filter_values = filter_cond.value if isinstance(filter_cond.value, list) else [filter_cond.value]

            # 展开值
            expanded_values = set(filter_values)
            for val in filter_values:
                # 查找包含当前值的其他值
                for enum in enum_values:
                    if enum.includes_values and val in enum.includes_values:
                        expanded_values.add(enum.standard_value)
                        logger.debug(
                            f"展开值包含关系: {val} ← {enum.standard_value}",
                            field_id=field_id,
                            original_value=val,
                            included_by=enum.standard_value
                        )

            # 如果发生了展开，更新过滤条件
            if len(expanded_values) > len(filter_values):
                filter_cond.op = "IN"
                filter_cond.value = sorted(list(expanded_values))
                expanded_fields.add(field_id)

                logger.debug(
                    "值包含关系展开",
                    field_id=field_id,
                    original_values=filter_values,
                    expanded_values=list(expanded_values),
                    reason="该字段配置了includes_values，需要展开查询并分组显示"
                )

        # 将展开的字段添加到dimensions（仅聚合查询需要分组）
        # 业务逻辑：当发生 includes_values 展开时，自动分组统计
        # 例如：用户问"住宅用地"，展开为["住宅用地", "住宅、商服用地"]后，
        # 应该分开统计两种用途的数据，因为它们是不同类型但有包含关系
        if ir.query_type == "aggregation" and expanded_fields:
            for field_id in expanded_fields:
                if field_id not in ir.dimensions:
                    ir.dimensions.append(field_id)
                    logger.debug(
                        "值展开：添加分组维度",
                        field_id=field_id,
                        reason="includes_values 展开后自动分组，分开统计不同类型的数据"
                    )
        elif expanded_fields:
            logger.debug(
                f"值展开：跳过添加 dimensions（查询类型={ir.query_type}，不需要分组）",
                expanded_fields=list(expanded_fields)
            )

        return ir

    # 保持严格按字段名与语义模型配置解析，避免过度魔法带来的误判。

    def _validate_time_and_filters(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        灵活验证时间对象和年份过滤的关系

        更宽松的验证逻辑：
        - 支持任何字段作为年份筛选条件（不限制必须是特定年份维度）
        - 允许年份过滤与时间对象存在合理偏差
        - 仅在明显不一致时记录警告，不强制纠正

        配置路径：
        - 数据库配置 tenant_config.defaults -> year_field, datetime_field
        - 数据库配置 tenant_config.rules -> time_semantic_rules.full_year_query.use_field
        """
        if not ir.time:
            return ir

        # 尝试从配置读取年份维度ID（用于日志记录，不强制使用）
        preferred_year_dimension_id = self._get_year_dimension_id()

        # 检查所有可能的年份过滤字段（不限制特定字段）
        year_like_filters = []
        for filter_obj in ir.filters:
            if self._is_year_like_field(filter_obj.field):
                year_like_filters.append(filter_obj)

        if year_like_filters and ir.time.type == "absolute":
            time_year = str(ir.time.start_date.year) if ir.time.start_date else None

            # 对每个年份相关过滤进行灵活验证
            for year_filter in year_like_filters:
                filter_year = str(year_filter.value)

                # 检查年份是否是有效数字
                try:
                    filter_year_int = int(filter_year)
                    time_year_int = int(time_year) if time_year else None

                    if time_year_int:
                        # 允许前后2年的偏差（支持财年、统计年度等场景）
                        if abs(filter_year_int - time_year_int) > 2:
                            logger.debug(
                                "年份过滤与时间对象存在较大偏差",
                                filter_field=year_filter.field,
                                filter_year=filter_year,
                                time_year=time_year,
                                suggestion="查询可能涉及跨年度数据分析"
                            )
                        else:
                            logger.debug(
                                "年份过滤与时间对象基本一致",
                                filter_field=year_filter.field,
                                filter_year=filter_year,
                                time_year=time_year
                            )
                except (ValueError, TypeError):
                    # 如果不是数字年份，跳过验证
                    logger.debug(
                        "跳过非数字年份验证",
                        filter_field=year_filter.field,
                        filter_value=filter_year
                    )

        return ir

    def _is_year_like_field(self, field_id: str) -> bool:
        """
        判断字段是否可能是年份相关字段

        兼容运行时 UUID 字段场景，综合字段元数据判断：
        1. 字段/展示名包含明确的时间关键词（如“年份”“日期”“季度”）
        2. 字段自身被建模为 timestamp / temporal
        3. 字段存在典型年份枚举值
        """
        strong_time_keywords = (
            "年份",
            "年度",
            "年月",
            "年",
            "日期",
            "时间",
            "季度",
            "月份",
            "month",
            "quarter",
            "week",
            "date",
            "time",
            "year",
        )
        negative_keywords = ("年限", "年龄", "周年")

        def _normalize_text(value: Any) -> str:
            return re.sub(r"\s+", "", str(value or "")).lower()

        def _looks_like_temporal_name(value: Any) -> bool:
            text = _normalize_text(value)
            if not text or any(keyword in text for keyword in negative_keywords):
                return False
            if "年份" in text or "年度" in text or "年月" in text:
                return True
            if any(keyword in text for keyword in strong_time_keywords if keyword != "年"):
                return True
            return text.endswith("年")

        if _looks_like_temporal_name(field_id):
            return True

        field_obj = None
        if hasattr(self.semantic_model, "fields"):
            field_obj = self.semantic_model.fields.get(field_id)

        # 检查字段类型与元数据
        if field_obj is not None:
            metadata_texts = [
                getattr(field_obj, "display_name", None),
                getattr(field_obj, "field_name", None),
                getattr(field_obj, "physical_column_name", None),
                getattr(field_obj, "description", None),
            ]
            metadata_texts.extend(getattr(field_obj, "synonyms", None) or [])
            if any(_looks_like_temporal_name(text) for text in metadata_texts):
                return True

            field_category = str(getattr(field_obj, "field_category", "") or "").lower()
            data_type = str(getattr(field_obj, "data_type", "") or "").lower()
            if field_category == "timestamp" or data_type in {"date", "datetime", "timestamp", "timestamptz"}:
                return True

            timestamp_props = getattr(field_obj, "timestamp_props", None)
            if timestamp_props and getattr(timestamp_props, "time_granularity", None):
                return True

            dimension_props = getattr(field_obj, "dimension_props", None)
            if str(getattr(dimension_props, "dimension_type", "") or "").lower() == "temporal":
                return True

        # 检查维度类型（如果是旧模型 dimensions 映射）
        if hasattr(self.semantic_model, 'dimensions') and field_id in self.semantic_model.dimensions:
            dim_obj = self.semantic_model.dimensions[field_id]
            dim_type = str(getattr(dim_obj, 'dimension_type', '') or '').lower()
            if dim_type == 'temporal':
                return True

        # 检查枚举值是否包含年份
        if hasattr(self.semantic_model, 'field_enums') and field_id in self.semantic_model.field_enums:
            enum_values = self.semantic_model.field_enums[field_id]
            for enum in enum_values or []:
                if hasattr(enum, 'standard_value') and enum.standard_value:
                    # 检查是否是4位数字（年份格式）
                    if str(enum.standard_value).isdigit() and len(str(enum.standard_value)) == 4:
                        return True

        return False

    def _ensure_grouping_for_multivalue_filters(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        当为聚合查询，且原始问题包含“分别/各自”等分组触发词，并且某个维度使用了 IN 且列出了多个具体值时：
        - 自动将该维度加入 dimensions 以便分别统计（每个值一行）
        - 同时确保 with_total = true（符合“分别 + 汇总”规则）

        说明：
        - 不依赖具体业务值（不硬编码行政区等数据），仅根据 IR 结构和问题文本触发，具有通用性。
        - 若维度已在 dimensions 中，则不重复添加。
        - IN 但仅 1 个值，等同精确过滤，不触发分组。
        """
        try:
            if ir.query_type != "aggregation":
                return ir

            question = (ir.original_question or "").strip()
            if not question:
                return ir

            # 分组触发词（尽量保守，避免误触发）
            grouping_triggers = ["分别", "各自"]
            if not any(trigger in question for trigger in grouping_triggers):
                return ir

            # 保守策略：如果 LLM 已经设置了 dimensions，说明 LLM 已正确理解分组需求，
            # 不应再自动添加其他字段（避免把业务规则展开的过滤条件误当作分组维度）
            if ir.dimensions:
                logger.debug(
                    "LLM已设置dimensions，跳过自动添加",
                    existing_dimensions=ir.dimensions,
                    reason="LLM已正确理解分组需求，不干预"
                )
                return ir

            # 查找满足条件的 IN 过滤（列出多个具体值）
            added = False
            for f in ir.filters or []:
                try:
                    if f.op == "IN" and isinstance(f.value, list) and len(f.value) >= 2:
                        if f.field not in ir.dimensions:
                            ir.dimensions.append(f.field)
                            added = True
                            logger.debug(
                                "分组修正：按多值IN的维度分组",
                                field_id=f.field,
                                values_count=len(f.value)
                            )
                except Exception:
                    # 防御性：忽略单条过滤的异常，继续处理其他过滤
                    continue

            # 若添加了分组维度，且未开启汇总，则开启 with_total
            if added and not getattr(ir, "with_total", False):
                ir.with_total = True
                logger.debug("分组修正：启用 with_total 以添加汇总行")

            return ir
        except Exception as e:
            logger.exception("_ensure_grouping_for_multivalue_filters 处理异常", error=str(e))
            return ir

    def _get_year_dimension_id(self) -> str:
        """
        从配置获取年份维度ID

        优先级：
        1. 数据库配置 tenant_config.rules -> time_semantic_rules.full_year_query.use_dimension
        2. 默认值 "deal_year"（向后兼容）

        Returns:
            年份维度ID
        """
        # 尝试从配置读取
        if self.semantic_model and hasattr(self.semantic_model, 'rules') and self.semantic_model.rules:
            time_rules = self.semantic_model.rules.time_semantic_rules
            if time_rules and isinstance(time_rules, dict) and time_rules.get('enabled'):
                full_year_config = time_rules.get('full_year_query', {})
                if isinstance(full_year_config, dict):
                    year_dimension_id = full_year_config.get('use_dimension')
                    if year_dimension_id:
                        logger.debug(f"从配置读取年份维度ID: {year_dimension_id}")
                        return year_dimension_id

        # 默认值（向后兼容）
        logger.debug("使用默认年份维度ID: deal_year")
        return "deal_year"

    def _apply_value_normalization(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        应用字段值标准化规则

         支持两种方式：
        1. 枚举同义词映射（从field_enums中读取）- 优先级更高
        2. 全局配置规则（从rules配置读取）

        - 数据库配置 tenant_config.administrative_divisions (行政区划全局数据)
        - 数据库配置 tenant_config.rules -> value_normalization.global_rules (全局映射规则)
        - 数据库配置 tenant_config.rules -> value_normalization.dimension_rules (维度级应用)
        """
        if not ir.filters:
            return ir

        #  首先尝试使用枚举同义词进行值标准化
        ir = self._apply_enum_synonym_normalization(ir)

        # 检查是否启用value_normalization规则
        if not self.semantic_model or not hasattr(self.semantic_model, 'rules') or not self.semantic_model.rules:
            return ir

        rules = self.semantic_model.rules
        if not hasattr(rules, 'value_normalization') or not rules.value_normalization:
            return ir

        value_norm_config = rules.value_normalization
        if not isinstance(value_norm_config, dict) or not value_norm_config.get('enabled'):
            return ir

        # 获取全局规则和维度规则
        global_rules = value_norm_config.get('global_rules', {})
        dimension_rules = value_norm_config.get('dimension_rules', [])

        if not global_rules and not dimension_rules:
            return ir

        # 构建维度到全局规则的映射
        dimension_to_globals = {}
        for dim_rule in dimension_rules:
            if isinstance(dim_rule, dict):
                dim_id = dim_rule.get('dimension_id')
                apply_globals = dim_rule.get('apply_global', [])
                if dim_id and apply_globals:
                    dimension_to_globals[dim_id] = apply_globals

        # 应用规则
        new_filters = []
        for filter_obj in ir.filters:
            field_id = filter_obj.field
            filter_value = filter_obj.value

            # 检查该字段是否需要应用全局规则
            if field_id not in dimension_to_globals:
                new_filters.append(filter_obj)
                continue

            apply_rules = dimension_to_globals[field_id]
            processed = False

            # 应用每个全局规则
            for rule_name in apply_rules:
                if rule_name not in global_rules:
                    continue

                rule_config = global_rules[rule_name]

                # 处理 district_mapping 规则（城市名→行政区列表）
                if rule_name == 'district_mapping':
                    triggers = rule_config.get('triggers', [])
                    if filter_value in triggers:
                        # 从数据库配置获取全局行政区列表
                        # 支持在规则中配置自定义的 scope_key
                        scope_key = rule_config.get('scope_key')
                        districts = self._get_global_districts(scope_key)

                        if districts:
                            logger.debug(
                                "应用全局规则：城市名映射为行政区列表",
                                field=field_id,
                                original_value=filter_value,
                                scope_key=scope_key or self._get_default_scope_key(),
                                values_count=len(districts),
                                reason=rule_config.get('reason', '')
                            )
                            filter_obj.op = rule_config.get('operator', 'IN')
                            filter_obj.value = districts
                            new_filters.append(filter_obj)
                            processed = True
                            break

                # 处理 district_abbreviations 规则（简称映射）
                elif rule_name == 'district_abbreviations':
                    if not processed and filter_value in rule_config:
                        mapped_value = rule_config[filter_value]
                        logger.debug(
                            "应用全局规则：行政区简称映射",
                            field=field_id,
                            original_value=filter_value,
                            mapped_value=mapped_value
                        )
                        filter_obj.value = mapped_value
                        new_filters.append(filter_obj)
                        processed = True
                        break

            # 如果没有处理，保持原样
            if not processed:
                new_filters.append(filter_obj)

        # 更新filters
        ir.filters = new_filters

        return ir

    def _apply_enum_synonym_normalization(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        使用枚举同义词进行值标准化

        从semantic_model的field_enums中读取同义词映射，
        将filter中的同义词值替换为标准值
        """
        if not self.semantic_model or not hasattr(self.semantic_model, 'field_enums'):
            return ir

        field_enums = self.semantic_model.field_enums
        if not field_enums:
            return ir

        # 构建同义词到标准值的映射 {field_id: {synonym_lower: standard_value}}
        synonym_maps = {}
        for field_id, enum_values in field_enums.items():
            if not enum_values:
                continue

            field_map = {}
            for enum_value in enum_values:
                standard_value = enum_value.standard_value

                # 添加标准值自身的映射（确保标准值也能匹配）
                field_map[str(standard_value).strip().lower()] = standard_value

                # 添加所有同义词的映射
                if enum_value.synonyms:
                    for synonym in enum_value.synonyms:
                        field_map[str(synonym.synonym_text).strip().lower()] = standard_value

            if field_map:
                synonym_maps[field_id] = field_map

        if not synonym_maps:
            return ir

        # 应用同义词映射到filters
        for filter_obj in ir.filters:
            field_id = filter_obj.field

            if field_id not in synonym_maps:
                continue

            field_map = synonym_maps[field_id]

            # 处理单值（大小写不敏感，去除空白）
            if filter_obj.op in ["=", "!=", ">", ">=", "<", "<=", "LIKE"]:
                key = str(filter_obj.value).strip().lower() if filter_obj.value is not None else ""
                if key in field_map:
                    old_value = filter_obj.value
                    filter_obj.value = field_map[key]
                    if old_value != filter_obj.value:
                        logger.debug(
                            "应用枚举同义词映射",
                            field=field_id,
                            original_value=old_value,
                            standard_value=filter_obj.value
                        )

            # 处理列表值（IN / NOT IN）（大小写不敏感，去除空白）
            elif filter_obj.op in ["IN", "NOT IN"]:
                if isinstance(filter_obj.value, list):
                    old_values = filter_obj.value.copy()
                    new_values = []
                    for val in filter_obj.value:
                        key = str(val).strip().lower()
                        if key in field_map:
                            mapped_val = field_map[key]
                            # 去重：如果映射后的值已经在列表中，就不重复添加
                            if mapped_val not in new_values:
                                new_values.append(mapped_val)
                        else:
                            # 如果没有映射，保留原值
                            if val not in new_values:
                                new_values.append(val)

                    filter_obj.value = new_values

                    # 记录日志
                    if old_values != new_values:
                        logger.debug(
                            "应用枚举同义词映射（批量）",
                            field=field_id,
                            original_values=old_values,
                            standard_values=new_values,
                            mapped_count=len([v for v in old_values if v in field_map])
                        )

        return ir

    
    
    def _validate_window_detail_query(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        验证窗口函数明细查询（分组TopN）的必需字段
        
        必需字段：
        1. partition_by: 分组字段列表（至少1个）
        2. sort_by: 排序字段（必需）
        3. window_limit: 每组记录数限制（必需）
        
        自动修正：
        - dimensions应该保持为空（因为不是聚合查询）
        - metrics应该保持为空（明细查询不需要聚合）
        """
        if ir.query_type != "window_detail":
            return ir
        
        logger.debug("开始验证窗口函数明细查询")
        
        # 1. 验证 partition_by
        if not ir.partition_by or len(ir.partition_by) == 0:
            logger.error("窗口函数明细查询缺少 partition_by 字段")
            raise ValueError("窗口函数明细查询必须指定分组字段 (partition_by)")
        
        # 2. 验证 sort_by
        if not ir.sort_by:
            logger.error("窗口函数明细查询缺少 sort_by 字段")
            raise ValueError("窗口函数明细查询必须指定排序字段 (sort_by)")
        
        # 3. 验证 window_limit
        if not ir.window_limit or ir.window_limit <= 0:
            logger.error("窗口函数明细查询缺少有效的 window_limit")
            raise ValueError("窗口函数明细查询必须指定每组记录数限制 (window_limit > 0)")
        
        # 4. 自动修正：清空 dimensions（窗口查询不需要聚合分组）
        if ir.dimensions:
            logger.debug(
                "窗口函数明细查询：清空 dimensions",
                original_dimensions=ir.dimensions,
                reason="窗口查询使用 partition_by 字段，不需要聚合分组"
            )
            ir.dimensions = []
        
        # 5. 自动修正：清空普通 metrics，但保留派生指标（derived:xxx）
        if ir.metrics:
            # 保留派生指标（以 derived: 开头的指标）
            derived_metrics = [m for m in ir.metrics if isinstance(m, str) and m.startswith("derived:")]
            regular_metrics = [m for m in ir.metrics if not (isinstance(m, str) and m.startswith("derived:"))]

            if regular_metrics:
                logger.debug(
                    "窗口函数明细查询：清空普通 metrics",
                    original_metrics=regular_metrics,
                    reason="窗口查询是明细查询，不需要聚合指标"
                )

            if derived_metrics:
                logger.debug(
                    "窗口函数明细查询：保留派生指标",
                    derived_metrics=derived_metrics,
                    reason="派生指标是用户明确要求的计算字段，需要保留"
                )

            ir.metrics = derived_metrics
        
        logger.debug(
            "窗口函数明细查询验证通过",
            partition_by=ir.partition_by,
            sort_by=ir.sort_by,
            window_limit=ir.window_limit
        )
        
        return ir

    def _validate_extended_fields(self, ir: IntermediateRepresentation) -> IntermediateRepresentation:
        """
        验证混合架构扩展字段的有效性
        
        检查内容：
        1. calculated_fields 中的字段引用是否存在
        2. conditional_metrics 中的字段是否存在
        3. ratio_metrics 中的分子分母字段是否存在
        4. having_filters 中的字段是否存在
        
        注意：此验证仅记录警告，不阻止查询执行（因为字段可能是派生指标等动态字段）
        """
        try:
            if not self.semantic_model:
                return ir
            
            fields = getattr(self.semantic_model, "fields", {}) or {}
            
            # 辅助函数：检查字段是否存在
            def field_exists(field_id: str) -> bool:
                if not field_id:
                    return True  # 空字段ID视为有效（可能是可选字段）
                if field_id.startswith("derived:"):
                    return True  # 派生指标不在 fields 中
                if field_id == "__row_count__":
                    return True  # 保留字
                return field_id in fields
            
            # 1. 验证 calculated_fields
            if hasattr(ir, 'calculated_fields') and ir.calculated_fields:
                for calc_field in ir.calculated_fields:
                    alias = getattr(calc_field, 'alias', '')
                    field_refs = getattr(calc_field, 'field_refs', []) or []
                    numerator_refs = getattr(calc_field, 'numerator_refs', []) or []
                    denominator_refs = getattr(calc_field, 'denominator_refs', []) or []
                    
                    # 检查 field_refs
                    for ref in field_refs:
                        if not field_exists(ref):
                            logger.warning(
                                f"计算字段 '{alias}' 引用的字段不存在: {ref}",
                                calc_field_alias=alias,
                                missing_field=ref
                            )
                    
                    # 检查 numerator_refs
                    for ref in numerator_refs:
                        if not field_exists(ref):
                            logger.warning(
                                f"计算字段 '{alias}' 的分子引用字段不存在: {ref}",
                                calc_field_alias=alias,
                                missing_field=ref
                            )
                    
                    # 检查 denominator_refs
                    for ref in denominator_refs:
                        if not field_exists(ref):
                            logger.warning(
                                f"计算字段 '{alias}' 的分母引用字段不存在: {ref}",
                                calc_field_alias=alias,
                                missing_field=ref
                            )
            
            # 2. 验证 conditional_metrics
            if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
                for cond_metric in ir.conditional_metrics:
                    alias = getattr(cond_metric, 'alias', '')
                    field_id = getattr(cond_metric, 'field', None)
                    condition = getattr(cond_metric, 'condition', None)
                    
                    # 检查聚合字段
                    if field_id and not field_exists(field_id):
                        logger.warning(
                            f"条件聚合指标 '{alias}' 的聚合字段不存在: {field_id}",
                            cond_metric_alias=alias,
                            missing_field=field_id
                        )
                    
                    # 检查条件字段
                    if condition:
                        cond_field = getattr(condition, 'field', None)
                        if cond_field and not field_exists(cond_field):
                            logger.warning(
                                f"条件聚合指标 '{alias}' 的条件字段不存在: {cond_field}",
                                cond_metric_alias=alias,
                                missing_field=cond_field
                            )
            
            # 3. 验证 ratio_metrics
            if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
                for ratio_metric in ir.ratio_metrics:
                    alias = getattr(ratio_metric, 'alias', '')
                    numerator_field = getattr(ratio_metric, 'numerator_field', None)
                    denominator_field = getattr(ratio_metric, 'denominator_field', None)
                    numerator_condition = getattr(ratio_metric, 'numerator_condition', None)
                    denominator_condition = getattr(ratio_metric, 'denominator_condition', None)
                    
                    # 检查分子字段
                    if numerator_field and not field_exists(numerator_field):
                        logger.warning(
                            f"占比指标 '{alias}' 的分子字段不存在: {numerator_field}",
                            ratio_metric_alias=alias,
                            missing_field=numerator_field
                        )
                    
                    # 检查分母字段
                    if denominator_field and not field_exists(denominator_field):
                        logger.warning(
                            f"占比指标 '{alias}' 的分母字段不存在: {denominator_field}",
                            ratio_metric_alias=alias,
                            missing_field=denominator_field
                        )
                    
                    # 检查分子条件字段
                    if numerator_condition:
                        cond_field = getattr(numerator_condition, 'field', None)
                        if cond_field and not field_exists(cond_field):
                            logger.warning(
                                f"占比指标 '{alias}' 的分子条件字段不存在: {cond_field}",
                                ratio_metric_alias=alias,
                                missing_field=cond_field
                            )
                    
                    # 检查分母条件字段
                    if denominator_condition:
                        cond_field = getattr(denominator_condition, 'field', None)
                        if cond_field and not field_exists(cond_field):
                            logger.warning(
                                f"占比指标 '{alias}' 的分母条件字段不存在: {cond_field}",
                                ratio_metric_alias=alias,
                                missing_field=cond_field
                            )
            
            # 4. 验证 having_filters
            if hasattr(ir, 'having_filters') and ir.having_filters:
                for having_filter in ir.having_filters:
                    field_id = getattr(having_filter, 'field', None)
                    if field_id and not field_exists(field_id):
                        logger.warning(
                            f"HAVING过滤条件的字段不存在: {field_id}",
                            missing_field=field_id
                        )
            
            return ir
            
        except Exception as e:
            logger.exception("_validate_extended_fields 处理异常", error=str(e))
            return ir

    def get_validation_summary(self, ir: IntermediateRepresentation) -> dict:
        """获取IR验证摘要（用于调试）"""
        return {
            "metrics_count": len(ir.metrics),
            "dimensions_count": len(ir.dimensions),
            "filters_count": len(ir.filters),
            "has_time": ir.time is not None,
            "confidence": ir.confidence,
            "has_ambiguities": len(ir.ambiguities) > 0
        }
