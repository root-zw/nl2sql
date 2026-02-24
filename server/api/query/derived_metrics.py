"""
派生指标说明构建

支持以下类型的计算说明：
- 基础聚合指标（SUM/COUNT/AVG等）
- 派生指标（derived:xxx）
- 占比指标（ratio_metrics）
- 条件聚合指标（conditional_metrics）
- 计算字段（calculated_fields）
"""

from typing import Dict, Any, List, Tuple, Optional
import structlog

from server.models.ir import IntermediateRepresentation

logger = structlog.get_logger()


def _get_field_display_name(field_id: str, semantic_model) -> str:
    """获取字段的显示名称"""
    if not field_id or not semantic_model:
        return field_id or ""
    
    fields_map = getattr(semantic_model, "fields", {}) or {}
    if field_id in fields_map:
        field_obj = fields_map[field_id]
        return getattr(field_obj, "display_name", field_id) or field_id
    
    # 特殊处理 __row_count__
    if field_id == "__row_count__":
        return "记录数"
    
    # 处理 derived: 前缀
    if field_id.startswith("derived:"):
        return field_id.replace("derived:", "")
    
    return field_id


async def build_derived_metrics_explanation(
    ir: IntermediateRepresentation,
    semantic_model,
    result_columns: List[Dict[str, Any]],
    result_rows: List[List[Any]],
    connection_id: str
) -> List[Dict[str, Any]]:
    """
    构建计算说明，覆盖：
    - 派生指标与基础聚合指标（SUM/COUNT等）
    - 占比指标（ratio_metrics）
    - 条件聚合指标（conditional_metrics）
    - 计算字段（calculated_fields）
    """
    explanations: List[Dict[str, Any]] = []

    try:
        # 为重复检测查询添加计算说明
        if ir.query_type == "duplicate_detection" and ir.duplicate_by:
            # 获取 duplicate_by 字段的显示名称
            fields_map = getattr(semantic_model, "fields", {}) or {}
            dup_field_names = []
            for field_id in ir.duplicate_by:
                if field_id in fields_map:
                    field_obj = fields_map[field_id]
                    dup_field_names.append(getattr(field_obj, "display_name", field_id) or field_id)
                else:
                    dup_field_names.append(field_id)
            
            dup_fields_str = "、".join(dup_field_names)
            explanations.append({
                "metric_id": "_duplicate_count",
                "display_name": "重复检测",
                "formula": f"COUNT(*) OVER (PARTITION BY {dup_fields_str})",
                "formula_detailed": f"统计每条记录的【{dup_fields_str}】在表中出现的次数，筛选出现次数 > 1 的记录",
                "unit": "条",
                "type": "duplicate_detection"
            })
            return explanations
        
        # 检查是否有任何需要解释的指标
        has_metrics = bool(ir.metrics)
        has_ratio_metrics = hasattr(ir, 'ratio_metrics') and bool(ir.ratio_metrics)
        has_conditional_metrics = hasattr(ir, 'conditional_metrics') and bool(ir.conditional_metrics)
        has_calculated_fields = hasattr(ir, 'calculated_fields') and bool(ir.calculated_fields)
        
        if not any([has_metrics, has_ratio_metrics, has_conditional_metrics, has_calculated_fields]):
            return explanations

        # 采样首行数据，方便展示示例值
        sample_by_column: Dict[str, Any] = {}
        sample_by_base: Dict[str, Any] = {}
        if result_rows:
            first_row = result_rows[0]
            for idx, col in enumerate(result_columns):
                if idx >= len(first_row):
                    continue
                col_name = col.get("name", "")
                value = first_row[idx]
                sample_by_column[col_name] = value
                base = col_name.split('(')[0].strip() if '(' in col_name else col_name
                if base:
                    sample_by_base[base] = value

        metrics_map = getattr(semantic_model, "metrics", {}) or {}
        fields_map = getattr(semantic_model, "fields", {}) or {}

        datasources_map = getattr(semantic_model, "datasources", {}) or {}

        def _resolve_display_name(metric_id: str) -> Tuple[str, Any]:
            if metric_id in metrics_map:
                metric_obj = metrics_map[metric_id]
                return getattr(metric_obj, "display_name", None) or metric_id, metric_obj
            if metric_id in fields_map:
                field_obj = fields_map[metric_id]
                return getattr(field_obj, "display_name", None) or metric_id, field_obj
            return metric_id, None

        def _extract_unit_from_column(name: str) -> Optional[str]:
            if '(' in name and name.rstrip().endswith(')'):
                start = name.rfind('(')
                inside = name[start + 1 : -1].strip()
                if inside:
                    return inside
            return None

        # 加载派生指标定义
        from server.dependencies import get_global_rules_loader
        rules_loader = get_global_rules_loader(connection_id)
        derived_defs: Dict[str, Dict[str, Any]] = {}
        if rules_loader:
            global_rules = await rules_loader.load_active_rules(
                rule_types=['derived_metric'],
                domain_id=None
            )
            for rule in global_rules:
                rule_def = rule.get('rule_definition', {})
                display_name = rule_def.get('display_name') or rule.get('rule_name')
                if display_name:
                    derived_defs[display_name] = rule_def

        def _match_sample_value(candidates: List[str]) -> Any:
            for candidate in candidates:
                if candidate in sample_by_column:
                    return sample_by_column[candidate]
                if candidate in sample_by_base:
                    return sample_by_base[candidate]
            return None

        def _add_entry(entry: Dict[str, Any]):
            explanations.append(entry)

        for metric_item in ir.metrics:
            # 兼容字符串和 MetricSpec 格式
            metric_alias = None
            if isinstance(metric_item, str):
                metric_id = metric_item
            elif isinstance(metric_item, dict):
                metric_id = metric_item.get("field", metric_item)
                metric_alias = metric_item.get("alias")
            elif hasattr(metric_item, "field"):
                metric_id = metric_item.field
                metric_alias = getattr(metric_item, "alias", None)
            else:
                metric_id = str(metric_item)
            
            # 处理保留字：__row_count__
            if metric_id == "__row_count__":
                # 优先使用 LLM 指定的别名，否则使用默认的"记录数"
                display_name = metric_alias if metric_alias else "记录数"
                formula = "COUNT(*)"
                formula_detailed = f"{display_name} = COUNT(*)"
                
                sample_value = _match_sample_value([display_name, "记录数", "__row_count__", f"{display_name}()"])
                
                _add_entry({
                    "metric_id": metric_id,
                    "name": metric_id,
                    "display_name": display_name,
                    "formula": formula,
                    "formula_detailed": formula_detailed,
                    "unit": None,
                    "description": f"统计{display_name}",
                    "data_source": None,
                    "sample_value": sample_value,
                    "type": "basic_metric"
                })
                continue
            
            if metric_id.startswith("derived:"):
                metric_name = metric_id.replace("derived:", "")
                metric_def = derived_defs.get(metric_name)
                if not metric_def:
                    logger.debug("派生指标未找到定义", metric=metric_name)
                    continue

                display_name = metric_def.get('display_name', metric_name)
                formula = metric_def.get('formula', '')
                unit = metric_def.get('unit', '')
                description = metric_def.get('description', '')
                field_deps = metric_def.get('field_dependencies') or []

                formula_detailed = f"{display_name} = {formula}" if formula else description or ""
                if unit:
                    formula_detailed += f"（单位：{unit}）"

                data_sources = []
                for dep in field_deps:
                    alias = dep.get('alias') or dep.get('placeholder')
                    field_name = dep.get('field_display_name') or dep.get('field_name') or dep.get('field_id')
                    if alias and field_name:
                        data_sources.append(f"{alias}→{field_name}")
                    elif field_name:
                        data_sources.append(field_name)
                data_source = "；".join(data_sources) if data_sources else None

                sample_value = _match_sample_value([display_name, metric_name])

                _add_entry({
                    "metric_id": metric_id,
                    "name": metric_name,
                    "display_name": display_name,
                    "formula": formula,
                    "formula_detailed": formula_detailed,
                    "unit": unit,
                    "description": description,
                    "data_source": data_source,
                    "sample_value": sample_value,
                    "type": "derived_metric"
                })
                continue

            # 基础指标（SUM/COUNT等）
            display_name, metric_obj = _resolve_display_name(metric_id)
            field_obj = fields_map.get(metric_id)
            agg = None
            unit = None
            description = ""

            if metric_obj:
                unit = getattr(metric_obj, "unit", None)
                description = getattr(metric_obj, "description", "") or ""
                atomic_def = getattr(metric_obj, "atomic_def", None)
                if atomic_def and getattr(atomic_def, "aggregation", None):
                    agg = atomic_def.aggregation
                if not agg and hasattr(metric_obj, "default_aggregation"):
                    agg = metric_obj.default_aggregation

            if not agg:
                agg = "SUM"

            agg_upper = str(agg).upper()
            formula = f"{agg_upper}({display_name})"
            formula_detailed = f"{display_name} = {agg_upper}({display_name})"

            # 单位与单位换算说明
            original_unit = unit
            unit_conversion = None
            source_desc = None

            if field_obj:
                if hasattr(field_obj, "measure_props") and field_obj.measure_props:
                    original_unit = field_obj.measure_props.unit or original_unit
                unit_conversion = getattr(field_obj, "unit_conversion", None) or unit_conversion
                datasource_name = None
                datasource_entry = datasources_map.get(field_obj.datasource_id)
                if datasource_entry:
                    datasource_name = getattr(datasource_entry, "display_name", None) or datasource_entry.datasource_name
                if datasource_name:
                    source_desc = f"{datasource_name}.{field_obj.field_name}"
                else:
                    source_desc = f"{field_obj.datasource_id}.{field_obj.field_name}"

            if metric_obj and not unit_conversion:
                unit_conversion = getattr(metric_obj, "unit_conversion", None)
            if metric_obj and not original_unit:
                original_unit = getattr(metric_obj, "unit", None)

            display_unit = unit or original_unit
            conversion_note = ""
            if unit_conversion and unit_conversion.get("enabled"):
                conversion_cfg = unit_conversion.get("conversion", {}) or {}
                factor = conversion_cfg.get("factor")
                method = conversion_cfg.get("method", "divide")
                display_unit = unit_conversion.get("display_unit", display_unit or original_unit)
                action = "除以" if method == "divide" else "乘以"
                if factor:
                    conversion_note = f"；单位换算：原单位{original_unit or '原值'}，{action}{factor}得到{display_unit}"
                else:
                    conversion_note = f"；单位换算：显示单位为{display_unit}"

            # 如果列名里带单位，作为兜底
            if not display_unit:
                for col in result_columns:
                    col_name = col.get("name", "")
                    base = col_name.split('(')[0].strip() if '(' in col_name else col_name
                    if base == display_name or col_name == display_name:
                        display_unit = _extract_unit_from_column(col_name)
                        break

            if display_unit:
                formula_detailed += f"（单位：{display_unit}）"
            if conversion_note:
                formula_detailed += conversion_note

            sample_value = _match_sample_value([
                display_name,
                metric_id,
                f"{display_name}({display_unit})" if display_unit else display_name
            ])

            _add_entry({
                "metric_id": metric_id,
                "name": metric_id,
                "display_name": display_name,
                "formula": formula,
                "formula_detailed": formula_detailed,
                "unit": display_unit,
                "description": description,
                "data_source": source_desc,
                "sample_value": sample_value,
                "type": "basic_metric"
            })

        # ========== 处理条件聚合指标 (conditional_metrics) ==========
        if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
            for cond_metric in ir.conditional_metrics:
                alias = cond_metric.alias
                field_id = cond_metric.field
                condition = cond_metric.condition
                aggregation = getattr(cond_metric, 'aggregation', 'SUM')
                
                # 获取字段显示名
                field_display = _get_field_display_name(field_id, semantic_model)
                
                # 构建条件描述
                condition_desc = ""
                if condition:
                    cond_field_display = _get_field_display_name(condition.field, semantic_model)
                    cond_op = condition.op
                    cond_value = condition.value
                    
                    if cond_op == "=":
                        condition_desc = f"{cond_field_display}={cond_value}"
                    elif cond_op == "IN":
                        if isinstance(cond_value, list):
                            values_str = "、".join(str(v) for v in cond_value[:3])
                            if len(cond_value) > 3:
                                values_str += f"等{len(cond_value)}个值"
                            condition_desc = f"{cond_field_display}∈[{values_str}]"
                        else:
                            condition_desc = f"{cond_field_display}∈[{cond_value}]"
                    else:
                        condition_desc = f"{cond_field_display}{cond_op}{cond_value}"
                
                # 构建公式
                agg_upper = str(aggregation).upper() if aggregation else "SUM"
                if field_id == "__row_count__":
                    if condition_desc:
                        formula = f"SUM(CASE WHEN {condition_desc} THEN 1 ELSE 0 END)"
                        formula_detailed = f"{alias} = 统计满足【{condition_desc}】条件的记录数"
                    else:
                        formula = "COUNT(*)"
                        formula_detailed = f"{alias} = COUNT(*)"
                else:
                    if condition_desc:
                        formula = f"{agg_upper}(CASE WHEN {condition_desc} THEN {field_display} ELSE 0 END)"
                        formula_detailed = f"{alias} = 仅统计【{condition_desc}】条件下的{field_display}（{agg_upper}汇总）"
                    else:
                        formula = f"{agg_upper}({field_display})"
                        formula_detailed = f"{alias} = {agg_upper}({field_display})"
                
                sample_value = _match_sample_value([alias, field_display])
                
                _add_entry({
                    "metric_id": f"conditional:{alias}",
                    "name": alias,
                    "display_name": alias,
                    "formula": formula,
                    "formula_detailed": formula_detailed,
                    "unit": None,
                    "description": f"条件聚合：{condition_desc}" if condition_desc else "聚合统计",
                    "data_source": None,
                    "sample_value": sample_value,
                    "type": "conditional_metric",
                    "condition": condition_desc
                })

        # ========== 处理占比指标 (ratio_metrics) ==========
        if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
            for ratio_metric in ir.ratio_metrics:
                alias = ratio_metric.alias
                numerator_field = ratio_metric.numerator_field
                denominator_field = ratio_metric.denominator_field
                numerator_condition = ratio_metric.numerator_condition
                denominator_condition = getattr(ratio_metric, 'denominator_condition', None)
                as_percentage = getattr(ratio_metric, 'as_percentage', True)
                decimal_places = getattr(ratio_metric, 'decimal_places', 2)
                
                # 获取字段显示名
                num_display = _get_field_display_name(numerator_field, semantic_model)
                denom_display = _get_field_display_name(denominator_field, semantic_model)
                
                # 构建分子条件描述
                num_cond_desc = ""
                if numerator_condition:
                    num_cond_field = _get_field_display_name(numerator_condition.field, semantic_model)
                    num_cond_op = numerator_condition.op
                    num_cond_value = numerator_condition.value
                    if num_cond_op == "=":
                        num_cond_desc = f"{num_cond_field}={num_cond_value}"
                    elif num_cond_op == "IN":
                        if isinstance(num_cond_value, list):
                            values_str = "、".join(str(v) for v in num_cond_value[:3])
                            num_cond_desc = f"{num_cond_field}∈[{values_str}]"
                        else:
                            num_cond_desc = f"{num_cond_field}∈[{num_cond_value}]"
                    else:
                        num_cond_desc = f"{num_cond_field}{num_cond_op}{num_cond_value}"
                
                # 构建分母条件描述
                denom_cond_desc = ""
                if denominator_condition:
                    denom_cond_field = _get_field_display_name(denominator_condition.field, semantic_model)
                    denom_cond_op = denominator_condition.op
                    denom_cond_value = denominator_condition.value
                    if denom_cond_op == "=":
                        denom_cond_desc = f"{denom_cond_field}={denom_cond_value}"
                    elif denom_cond_op == "IN":
                        if isinstance(denom_cond_value, list):
                            values_str = "、".join(str(v) for v in denom_cond_value[:3])
                            denom_cond_desc = f"{denom_cond_field}∈[{values_str}]"
                        else:
                            denom_cond_desc = f"{denom_cond_field}∈[{denom_cond_value}]"
                    else:
                        denom_cond_desc = f"{denom_cond_field}{denom_cond_op}{denom_cond_value}"
                
                # 构建公式
                multiplier = "× 100" if as_percentage else ""
                unit = "%" if as_percentage else None
                
                # 判断占比类型
                if numerator_field == denominator_field and not numerator_condition:
                    # 分类分组占比：每个分组的值占总量的比例
                    ratio_type = "分类占比"
                    formula = f"SUM({num_display}) / SUM({denom_display}) OVER () {multiplier}"
                    formula_detailed = f"{alias} = 每个分组的{num_display}占全部{denom_display}总量的比例"
                elif numerator_condition and not denominator_condition:
                    # 条件占比：满足条件的部分占总量的比例
                    ratio_type = "条件占比"
                    formula = f"SUM(CASE WHEN {num_cond_desc} THEN {num_display} ELSE 0 END) / SUM({denom_display}) {multiplier}"
                    formula_detailed = f"{alias} = 满足【{num_cond_desc}】的{num_display}占总{denom_display}的比例"
                elif numerator_condition and denominator_condition:
                    # 双条件占比
                    ratio_type = "双条件占比"
                    formula = f"条件分子 / 条件分母 {multiplier}"
                    formula_detailed = f"{alias} = 满足【{num_cond_desc}】的{num_display}与满足【{denom_cond_desc}】的{denom_display}之比"
                else:
                    # 普通占比
                    ratio_type = "普通占比"
                    formula = f"SUM({num_display}) / SUM({denom_display}) {multiplier}"
                    formula_detailed = f"{alias} = {num_display}与{denom_display}之比"
                
                if as_percentage:
                    formula_detailed += f"（转换为百分比，保留{decimal_places}位小数）"
                
                sample_value = _match_sample_value([alias])
                
                _add_entry({
                    "metric_id": f"ratio:{alias}",
                    "name": alias,
                    "display_name": alias,
                    "formula": formula,
                    "formula_detailed": formula_detailed,
                    "unit": unit,
                    "description": ratio_type,
                    "data_source": None,
                    "sample_value": sample_value,
                    "type": "ratio_metric",
                    "ratio_type": ratio_type,
                    "as_percentage": as_percentage,
                    "decimal_places": decimal_places
                })

        # ========== 处理计算字段 (calculated_fields) ==========
        if hasattr(ir, 'calculated_fields') and ir.calculated_fields:
            for calc_field in ir.calculated_fields:
                alias = calc_field.alias
                expression = calc_field.expression
                field_refs = getattr(calc_field, 'field_refs', []) or []
                aggregation = getattr(calc_field, 'aggregation', None)
                unit = getattr(calc_field, 'unit', None)
                decimal_places = getattr(calc_field, 'decimal_places', 2)
                total_strategy = getattr(calc_field, 'total_strategy', None)
                numerator_refs = getattr(calc_field, 'numerator_refs', None)
                denominator_refs = getattr(calc_field, 'denominator_refs', None)
                
                # 将表达式中的字段ID替换为显示名
                formula_display = expression
                for field_id in field_refs:
                    field_display = _get_field_display_name(field_id, semantic_model)
                    formula_display = formula_display.replace(f"{{{field_id}}}", field_display)
                
                # 构建详细说明
                if aggregation and aggregation != "NONE":
                    formula_detailed = f"{alias} = {aggregation}({formula_display})"
                else:
                    formula_detailed = f"{alias} = {formula_display}"
                
                if unit:
                    formula_detailed += f"（单位：{unit}）"
                
                # 判断计算类型
                calc_type = "表达式计算"
                if numerator_refs and denominator_refs:
                    calc_type = "比率计算"
                    num_names = [_get_field_display_name(ref, semantic_model) for ref in numerator_refs]
                    denom_names = [_get_field_display_name(ref, semantic_model) for ref in denominator_refs]
                    formula_detailed += f"；分子：{'+'.join(num_names)}，分母：{'+'.join(denom_names)}"
                
                if total_strategy:
                    strategy_desc = {
                        'sum': '直接求和',
                        'recalculate': '重新计算',
                        'weighted_avg': '加权平均',
                        'none': '不显示'
                    }.get(total_strategy, total_strategy)
                    formula_detailed += f"（合计行策略：{strategy_desc}）"
                
                sample_value = _match_sample_value([alias])
                
                _add_entry({
                    "metric_id": f"calculated:{alias}",
                    "name": alias,
                    "display_name": alias,
                    "formula": formula_display,
                    "formula_detailed": formula_detailed,
                    "unit": unit,
                    "description": calc_type,
                    "data_source": None,
                    "sample_value": sample_value,
                    "type": "calculated_field",
                    "aggregation": aggregation,
                    "decimal_places": decimal_places,
                    "total_strategy": total_strategy
                })

    except Exception as e:
        logger.warning("构建计算说明失败", error=str(e), exc_info=True)

    return explanations

