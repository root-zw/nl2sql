"""查询结果说明与洞察生成器（确定性，无需大模型）"""

from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
from statistics import mean, median
from decimal import Decimal
from datetime import date, datetime

from server.models.ir import IntermediateRepresentation, FilterCondition
from server.config import settings


def _match_field_display_name(
    field_id: str,
    display_columns: List[str],
    keyword_mapping: Optional[Dict[str, List[str]]] = None
) -> str:
    """
    智能匹配字段的显示名称（通用函数）

    Args:
        field_id: 原始字段ID（如 "deal_price", "land_area"）
        display_columns: 格式化后的列名列表（如 ["总价(万元)", "出让面积(平方米)"]）
        keyword_mapping: 关键词映射配置（从数据库 tenant_config.formatting 读取）

    Returns:
        匹配到的显示名称（如 "总价"），找不到则返回原始ID

    匹配策略：
        1. 精确匹配：field_id 完全匹配某个列名
        2. 基础名匹配：去除单位后匹配（如 "总价(万元)" → "总价"）
        3. 关键词匹配：使用 keyword_mapping 进行语义匹配
    """
    if not display_columns:
        return field_id

    # 策略1: 精确匹配
    if field_id in display_columns:
        return field_id

    # 提取所有列的基础名（去除单位）
    col_base_map = {}  # {基础名: 完整列名}
    for col in display_columns:
        col_base = col.split("(")[0].strip() if "(" in col else col
        col_base_map[col_base] = col

    # 策略2: 基础名精确匹配
    if field_id in col_base_map:
        return col_base_map[field_id]

    # 策略3: 关键词匹配（使用配置）
    if keyword_mapping:
        # 规范化 field_id：转小写，去除下划线和前缀
        field_normalized = field_id.lower().replace("_", "")

        # 遍历关键词映射
        for category, keywords in keyword_mapping.items():
            # 检查 field_id 是否包含该类别的任何关键词
            if any(kw.lower() in field_normalized for kw in keywords if kw):
                # 在列名中查找包含相同类别关键词的列
                for col_base, col_full in col_base_map.items():
                    if any(kw in col_base or kw in col_full for kw in keywords if kw):
                        return col_base

    # 策略4: 后备方案 - 简单的包含匹配
    field_lower = field_id.lower()
    for col_base, col_full in col_base_map.items():
        col_lower = col_base.lower()
        # 双向包含检查
        if field_lower in col_lower or col_lower in field_lower:
            return col_base

    # 找不到匹配，返回原始ID
    return field_id


def _is_number(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    # 字符串两位小数字符，如 "123.00"
    if isinstance(value, str):
        try:
            float(value)
            return True
        except Exception:
            return False
    return False


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        try:
            return float(str(value))
        except Exception:
            return None


def _date_to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _format_date_cn(value: Any) -> str:
    date_str = _date_to_str(value)
    if not date_str:
        return ""
    date_part = date_str.split("T")[0]
    parts = date_part.split("-")
    if len(parts) >= 3:
        y, m, d = parts[:3]
        return f"{y}年{m}月{d}日"
    return date_part


def build_process_explanation(
    ir: IntermediateRepresentation,
    display_columns: List[str],
    keyword_mapping: Optional[Dict[str, List[str]]] = None,
    semantic_model = None
) -> List[str]:
    """基于 IR 生成"过程说明"（确定性，通俗易懂）。

    Args:
        ir: 中间表示
        display_columns: 格式化后的列名列表
        keyword_mapping: 字段关键词映射（从数据库配置 formatting 读取）
        semantic_model: 语义模型（用于获取字段显示名）

    返回：若干条中文步骤说明。
    """
    steps: List[str] = []

    #  使用统一的字段显示名映射工具
    from server.utils.field_display import get_field_display_name as _get_display_name
    
    def get_field_display_name(field_id: str) -> str:
        """将字段ID转换为友好的显示名称（使用统一工具）"""
        return _get_display_name(field_id, semantic_model)

    # 🔹 时间范围说明（更口语化）
    if ir.time and ir.time.type == "absolute" and ir.time.start_date and ir.time.end_date:
        start_str = _date_to_str(ir.time.start_date)
        end_str = _date_to_str(ir.time.end_date)
        if start_str and end_str:
            # 判断是否同年
            if start_str[:4] == end_str[:4]:
                year = start_str[:4]
                if start_str.endswith('-01-01') and end_str.endswith('-12-31'):
                    steps.append(f" 查的是 {year}年 整整一年的数据")
                else:
                    start = _format_date_cn(start_str)
                    end = _format_date_cn(end_str)
                    steps.append(f" 查的是 {start} 到 {end} 这段时间")
            else:
                start = _format_date_cn(start_str)
                end = _format_date_cn(end_str)
                steps.append(f" 查的是 {start} 到 {end} 这段时间")
    elif ir.time and ir.time.type == "absolute" and ir.time.start_date and not ir.time.end_date:
        start = _format_date_cn(ir.time.start_date)
        steps.append(f" 查的是 {start} 之后的数据")
    elif ir.time and ir.time.type == "relative" and ir.time.last_n and ir.time.unit:
        from server.utils.text_templates import get_time_units
        unit_map = get_time_units()
        unit_text = unit_map.get(ir.time.unit, ir.time.unit)
        steps.append(f" 查的是最近 {ir.time.last_n} 个{unit_text}")

    # 🔹 筛选条件说明（更口语化）
    if ir.filters:
        filter_descriptions = []
        for f in ir.filters[:5]:
            field_name = get_field_display_name(f.field)

            if f.op == "=":
                filter_descriptions.append(f"{field_name}等于【{f.value}】")
            elif f.op == "IN":
                if isinstance(f.value, list):
                    values = f.value[:5]  # 最多显示5个
                    values_str = "、".join(map(str, values))
                    count = len(f.value)
                    if count > 5:
                        filter_descriptions.append(f"看了 {values_str} 这几个{field_name}（共{count}个）")
                    elif count > 1:
                        filter_descriptions.append(f"看了 {values_str} 这{count}个{field_name}")
                    else:
                        filter_descriptions.append(f"{field_name}是【{values_str}】")
            elif f.op == ">":
                filter_descriptions.append(f"{field_name}大于 {f.value}")
            elif f.op == ">=":
                filter_descriptions.append(f"{field_name}≥ {f.value}")
            elif f.op == "<":
                filter_descriptions.append(f"{field_name}小于 {f.value}")
            elif f.op == "<=":
                filter_descriptions.append(f"{field_name}≤ {f.value}")
            elif f.op == "LIKE":
                filter_descriptions.append(f"{field_name}里包含【{f.value}】")
            else:
                # 其他操作符，保留原样
                val = f.value if not isinstance(f.value, list) else "、".join(map(str, f.value[:3]))
                filter_descriptions.append(f"{field_name} {f.op} {val}")

        if filter_descriptions:
            steps.append(" 筛选条件：" + "，".join(filter_descriptions))

    # 🔹 统计内容说明（更口语化）
    if ir.query_type == "aggregation":
        metric_descriptions = []
        if ir.metrics:
            metric_names = [get_field_display_name(m) for m in ir.metrics]
            if len(metric_names) == 1:
                metric_descriptions.append(f" 算了一下{metric_names[0]}")
            elif len(metric_names) == 2:
                metric_descriptions.append(f" 算了{metric_names[0]}和{metric_names[1]}")
            else:
                metric_descriptions.append(f" 算了{len(metric_names)}个指标：{' 、'.join(metric_names[:3])}" + ("等" if len(metric_names) > 3 else ""))

        if ir.dimensions:
            dim_names = [get_field_display_name(d) for d in ir.dimensions]
            if len(dim_names) == 1:
                metric_descriptions.append(f"按【{dim_names[0]}】分组统计")
            elif len(dim_names) == 2:
                metric_descriptions.append(f"按【{dim_names[0]}】和【{dim_names[1]}】分组统计")
            else:
                metric_descriptions.append(f"按{len(dim_names)}个维度分组：{' 、'.join(['【' + d + '】' for d in dim_names[:3]])}" + ("等" if len(dim_names) > 3 else ""))

        steps.extend(metric_descriptions)
    
    # 🔹 窗口函数明细查询说明（分组TopN）
    if ir.query_type == "window_detail":
        # 分组字段
        if ir.partition_by:
            partition_names = [get_field_display_name(p) for p in ir.partition_by]
            if len(partition_names) == 1:
                steps.append(f"按【{partition_names[0]}】分别统计")
            else:
                partition_str = "】、【".join(partition_names)
                steps.append(f"按【{partition_str}】分别统计")
        
        # 排序+限制
        if ir.sort_by and ir.window_limit:
            sort_display_name = get_field_display_name(ir.sort_by)
            order_text = "从大到小" if ir.sort_order == 'desc' else "从小到大"
            steps.append(f" 在每个分组内，按【{sort_display_name}】{order_text}排序，取前 {ir.window_limit} 条")

    # 🔹 重复检测查询说明
    elif ir.query_type == "duplicate_detection" and ir.duplicate_by:
        dup_names = [get_field_display_name(d) for d in ir.duplicate_by]
        if len(dup_names) == 1:
            steps.append(f" 找出【{dup_names[0]}】重复的记录")
        else:
            dup_str = "】、【".join(dup_names)
            steps.append(f" 找出【{dup_str}】组合重复的记录")

    # 🔹 排序说明（更口语化）
    elif ir.query_type == "detail" and ir.sort_by:
        # 优先使用统一的字段映射工具，回退到display_columns匹配
        sort_display_name = get_field_display_name(ir.sort_by)
        if sort_display_name == ir.sort_by:
            # 如果没有从semantic_model找到，尝试从display_columns匹配
            sort_display_name = _match_field_display_name(ir.sort_by, display_columns, keyword_mapping)
        order_text = "从大到小" if ir.sort_order == 'desc' else "从小到大"
        steps.append(f" 按【{sort_display_name}】{order_text}排序")
    elif ir.order_by:
        ob = ir.order_by[0]
        # 优先使用统一的字段映射工具
        order_display_name = get_field_display_name(ob.field)
        if order_display_name == ob.field:
            # 如果没有从semantic_model找到，尝试从display_columns匹配
            order_display_name = _match_field_display_name(ob.field, display_columns, keyword_mapping)
        order_text = "从大到小" if ob.desc else "从小到大"
        steps.append(f" 按【{order_display_name}】{order_text}排序")

    # 🔹 数量限制说明（口语化）
    if ir.limit and ir.limit < 10000:
        if ir.limit <= 10:
            steps.append(f" 为您展示前 {ir.limit} 条数据")
        elif ir.limit <= 100:
            steps.append(f" 为您展示前 {ir.limit} 条数据")
        else:
            steps.append(f" 最多返回 {ir.limit} 条数据")

    # 🔹 同比/环比说明（增长率计算）
    if ir.comparison_type and ir.show_growth_rate:
        comparison_label = {
            'yoy': '年同比',
            'qoq': '季环比',
            'mom': '月环比',
            'wow': '周环比'
        }.get(ir.comparison_type, '同比')
        
        # 根据过滤条件中的时间点数量判断展示模式
        time_dim_id = None
        for dim_id in ir.dimensions:
            # 简单判断：包含"年份"、"年度"的可能是时间维度
            # 实际项目中可能需要更精确的判断
            if dim_id and isinstance(dim_id, str):
                time_dim_id = dim_id
                break
        
        year_count = 0
        if time_dim_id:
            for f in ir.filters:
                if f.field == time_dim_id:
                    if f.op == "IN" and isinstance(f.value, list):
                        year_count = len(f.value)
                    elif f.op == "=":
                        year_count = 1
                    elif f.op in [">=", "<=", ">", "<", "BETWEEN"]:
                        year_count = 10  # 范围查询视为多年
                    break
        
        if year_count == 2:
            # Pivot 模式：两期对比，横向展开
            steps.append(f" 进行了{comparison_label}对比，横向展示两期数据及增长率")
        elif year_count > 2:
            # Vertical 模式：多期同比，纵向显示
            steps.append(f" 计算了历年{comparison_label}增长率，纵向展示趋势变化")
        else:
            steps.append(f" 计算了{comparison_label}增长率")

    return steps


def _percentiles(sorted_vals: List[float], ps: List[float]) -> Dict[str, float]:
    def pct(p: float) -> float:
        if not sorted_vals:
            return 0.0
        k = (len(sorted_vals) - 1) * p
        f = int(k)
        c = min(f + 1, len(sorted_vals) - 1)
        if f == c:
            return sorted_vals[f]
        return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)

    mapping = {}
    for p in ps:
        mapping[f"p{int(p*100)}"] = round(pct(p), 2)
    return mapping


def _detect_missing_dimension_values(
    columns: List[Dict[str, str]],
    rows: List[List[Any]],
    ir: Optional[IntermediateRepresentation],
    semantic_model
) -> Optional[Dict[str, Any]]:
    """
    检测用户明确查询了但实际没有数据的维度值

    使用场景：当用户问"A和B分别是多少"时，如果B没有数据，应该明确告知用户

    Returns:
        {
            "dimension_field": "行政区",
            "dimension_field_id": "xxx-xxx-xxx",
            "queried_values": ["江汉区", "武昌区", "洪山区", ...],
            "actual_values": ["江汉区", "武昌区", ...],
            "missing_values": ["洪山区"],
            "message": "查询了6个行政区，但洪山区没有数据"
        }
    """
    if not ir or not semantic_model:
        return None

    # 只处理有分组的聚合查询（with_total场景）
    if ir.query_type != "aggregation" or not ir.dimensions or not ir.with_total:
        return None

    # 查找IN类型的filter，且该filter的字段也在dimensions中
    # 这表示用户明确指定了多个值进行分组对比
    target_filter = None
    for filter_cond in ir.filters:
        if filter_cond.op == "IN" and filter_cond.field in ir.dimensions:
            if isinstance(filter_cond.value, list) and len(filter_cond.value) > 1:
                target_filter = filter_cond
                break

    if not target_filter:
        return None

    # 获取字段显示名
    field_id = target_filter.field
    field_display_name = None

    if hasattr(semantic_model, 'fields') and field_id in semantic_model.fields:
        field_display_name = semantic_model.fields[field_id].display_name
    elif hasattr(semantic_model, 'dimensions') and field_id in semantic_model.dimensions:
        field_display_name = semantic_model.dimensions[field_id].display_name

    if not field_display_name:
        return None

    # 查询的期望值
    queried_values = target_filter.value

    # 从结果中提取实际返回的值
    # 找到对应维度列的索引
    col_names = [c.get("name") for c in columns]

    # 尝试匹配列名（可能带单位）
    target_col_idx = None
    for i, col_name in enumerate(col_names):
        col_base = col_name.split("(")[0].strip() if "(" in col_name else col_name
        if col_base == field_display_name or col_name == field_display_name:
            target_col_idx = i
            break

    if target_col_idx is None:
        return None

    # 提取实际返回的值（排除"合计"行）
    actual_values = []
    for row in rows:
        val = row[target_col_idx]
        if val and str(val) != "合计":  # 排除合计行
            actual_values.append(str(val))

    # 对比找出缺失的值
    missing_values = [v for v in queried_values if v not in actual_values]

    if not missing_values:
        return None

    # 生成友好的提示信息
    if len(missing_values) == 1:
        message = f"查询了{len(queried_values)}个{field_display_name}，但{missing_values[0]}没有数据"
    elif len(missing_values) == len(queried_values):
        message = f"查询的{len(queried_values)}个{field_display_name}均没有数据"
    else:
        missing_str = "、".join(missing_values[:3])
        if len(missing_values) > 3:
            missing_str += f"等{len(missing_values)}个{field_display_name}"
        message = f"查询了{len(queried_values)}个{field_display_name}，但{missing_str}没有数据"

    return {
        "dimension_field": field_display_name,
        "dimension_field_id": field_id,
        "queried_values": queried_values,
        "actual_values": actual_values,
        "missing_values": missing_values,
        "message": message
    }


def build_insights(
    columns: List[Dict[str, str]],
    rows: List[List[Any]],
    sort_field: Optional[str],
    keyword_mapping: Optional[Dict[str, List[str]]] = None,
    ir: Optional[IntermediateRepresentation] = None,
    semantic_model = None
) -> Dict[str, Any]:
    """对当前结果做轻量统计与分布分析（确定性）。

    Args:
        columns: 列信息
        rows: 行数据
        sort_field: 排序字段ID
        keyword_mapping: 字段关键词映射（从数据库配置 formatting 读取）
        ir: 中间表示（用于检测缺失的维度值）
        semantic_model: 语义模型（用于获取字段显示名）

    Returns:
        统计洞察数据
    """
    insights: Dict[str, Any] = {"records": len(rows)}

    #  检测"查询了但没有数据"的维度值
    missing_dimension_values = _detect_missing_dimension_values(
        columns, rows, ir, semantic_model
    )
    if missing_dimension_values:
        insights["missing_dimension_values"] = missing_dimension_values

    # 构造列名列表
    col_names = [c.get("name") for c in columns]
    name_to_idx = {name: i for i, name in enumerate(col_names)}

    # 选择一个主数值列：优先使用排序字段
    numeric_cols: List[str] = []
    for i, name in enumerate(col_names):
        # 检查前几行是否可转为数字
        sample_vals = [rows[r][i] for r in range(min(len(rows), 20))]
        if any(_is_number(v) for v in sample_vals):
            numeric_cols.append(name)

    #  智能匹配排序字段（使用公共函数和配置）
    target_col = None
    if sort_field:
        matched_name = _match_field_display_name(sort_field, numeric_cols, keyword_mapping)
        # 检查匹配结果是否在数值列中
        if matched_name in numeric_cols:
            target_col = matched_name
        else:
            # 如果匹配结果不在数值列中，尝试在数值列中查找包含匹配名称的列
            for col in numeric_cols:
                col_base = col.split("(")[0].strip() if "(" in col else col
                if matched_name == col_base or matched_name in col_base:
                    target_col = col
                    break

    # 如果还是找不到，使用第一个数值列
    if not target_col and numeric_cols:
        target_col = numeric_cols[0]

    #  统计所有数值列，而不是只分析一个主要列
    for col_name in numeric_cols:
        idx = name_to_idx[col_name]
        # 排除"合计"行（ROLLUP生成的汇总行）
        nums = [_to_float(r[idx]) for r in rows if r[0] != "合计"]
        nums = [n for n in nums if n is not None]
        if nums:
            #  特殊处理：只有1行数据时（通常是汇总查询）
            if len(nums) == 1:
                value = round(nums[0], 2)
                insights[col_name] = {
                    "value": value,
                    "summary": f"值为{value}"
                }
                continue

            nums_sorted = sorted(nums)
            avg = mean(nums_sorted)
            med = median(nums_sorted)

            # 基础统计数据（供后续分析使用）
            stat_data = {
                "min": round(nums_sorted[0], 2),
                "max": round(nums_sorted[-1], 2),
                "mean": round(avg, 2),
                "median": round(med, 2),
                **_percentiles(nums_sorted, [0.25, 0.75])
            }

            #  生成业务友好的描述
            range_diff = stat_data["max"] - stat_data["min"]
            # is_concentrated = (range_diff / stat_data["max"] < 0.5) if stat_data["max"] > 0 else False

            summary_parts = []
            summary_parts.append(f"最小{stat_data['min']}、最大{stat_data['max']}")
            summary_parts.append(f"平均值{stat_data['mean']}")

            # 用通俗语言描述分布特征
            # if is_concentrated:
            #     summary_parts.append(f"数据较集中")
            # else:
            #     summary_parts.append(f"数据较分散（差异{round(range_diff, 2)}）")

            # 如果均值和中位数差异大，说明有偏斜（避免中位数为0时除零）
            median_val = stat_data["median"]
            mean_val = stat_data["mean"]
            if abs(median_val) > 1e-9:
                skew_ratio = abs(mean_val - median_val) / abs(median_val)
                if skew_ratio > 0.2:
                    if mean_val > median_val:
                        summary_parts.append("少数较大值拉高了平均值")
                    else:
                        summary_parts.append("少数较小值拉低了平均值")

            insights[col_name] = {
                **stat_data,
                "summary": "、".join(summary_parts)  # 业务友好摘要
            }

            #  异常值检测（使用IQR方法）
            if len(nums_sorted) >= 4:
                q1 = nums_sorted[len(nums_sorted) // 4]
                q3 = nums_sorted[len(nums_sorted) * 3 // 4]
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                outliers_low = [n for n in nums if n < lower_bound]
                outliers_high = [n for n in nums if n > upper_bound]

                if outliers_low or outliers_high:
                    outlier_desc = []
                    if outliers_high:
                        outlier_desc.append(f"有{len(outliers_high)}个异常高值（最高{round(max(outliers_high), 2)}）")
                    if outliers_low:
                        outlier_desc.append(f"有{len(outliers_low)}个异常低值（最低{round(min(outliers_low), 2)}）")

                    insights[col_name]["outliers"] = {
                        "count": len(outliers_low) + len(outliers_high),
                        "low_count": len(outliers_low),
                        "high_count": len(outliers_high),
                        "extreme_low": round(min(outliers_low), 2) if outliers_low else None,
                        "extreme_high": round(max(outliers_high), 2) if outliers_high else None,
                        "description": "，".join(outlier_desc)
                    }

    # 类别分布：挑选两个常见分类字段（启发式）
    categorical_candidates = [n for n in col_names if any(k in n for k in ["用途", "行政区", "区域", "区", "类别", "类型"])]
    for cat in categorical_candidates[:2]:
        idx = name_to_idx[cat]
        freq: Dict[str, int] = {}
        max_rows = max(0, settings.explainer_max_rows)
        for r in rows[:max_rows]:  # 限制最多配置行以控资源
            key = str(r[idx]) if r[idx] is not None else ""
            freq[key] = freq.get(key, 0) + 1
        top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:5]
        insights[cat] = [{"label": k, "count": v} for k, v in top]

    #  占比分析：如果有数值字段，计算各类别的占比和集中度
    if categorical_candidates and numeric_cols and len(rows) > 0:
        cat = categorical_candidates[0]  # 使用第一个分类字段
        cat_idx = name_to_idx[cat]
        value_col = numeric_cols[0]  # 使用第一个数值列
        value_idx = name_to_idx[value_col]

        # 按类别汇总数值
        cat_totals: Dict[str, float] = {}
        for r in rows:
            cat_val = str(r[cat_idx]) if r[cat_idx] is not None else ""
            num_val = _to_float(r[value_idx])
            if num_val is not None:
                cat_totals[cat_val] = cat_totals.get(cat_val, 0) + num_val

        if cat_totals:
            total_sum = sum(cat_totals.values())
            # 计算占比并排序
            cat_shares = []
            for cat_val, cat_sum in cat_totals.items():
                share_pct = (cat_sum / total_sum * 100) if total_sum > 0 else 0
                cat_shares.append({
                    "category": cat_val,
                    "value": round(cat_sum, 2),
                    "share_pct": round(share_pct, 2)
                })
            cat_shares.sort(key=lambda x: x["value"], reverse=True)

            # 计算Top3占比（集中度指标）
            top3_sum = sum(s["value"] for s in cat_shares[:3])
            top3_pct = (top3_sum / total_sum * 100) if total_sum > 0 else 0

            # 生成易读的描述
            top3_names = "、".join([s["category"] for s in cat_shares[:3]])
            concentration_desc = f"前三名（{top3_names}）占比{round(top3_pct, 2)}%"

            if top3_pct > 70:
                concentration_desc += "，高度集中"
            elif top3_pct > 50:
                concentration_desc += "，较为集中"
            else:
                concentration_desc += "，分布较均衡"

            insights["distribution"] = {
                "field": cat,
                "target": target_col,
                "categories": cat_shares[:10],  # 最多显示前10个
                "top3_concentration": round(top3_pct, 2),  # Top3集中度
                "total": round(total_sum, 2),
                "summary": concentration_desc  # 业务友好摘要
            }

    #  趋势分析：识别时间维度并计算趋势
    time_candidates = [n for n in col_names if any(k in n.lower() for k in ["year", "年份", "date", "时间", "日期"])]
    if time_candidates and target_col:
        time_col = time_candidates[0]
        time_idx = name_to_idx[time_col]
        value_idx = name_to_idx[target_col]

        # 按时间分组统计
        time_groups: Dict[Any, List[float]] = {}
        for r in rows:
            time_val = r[time_idx]
            num_val = _to_float(r[value_idx])
            if time_val is not None and num_val is not None:
                if time_val not in time_groups:
                    time_groups[time_val] = []
                time_groups[time_val].append(num_val)

        if len(time_groups) >= 2:  # 至少有2个时间点才能分析趋势
            # 计算每个时间点的总和
            time_series = []
            for time_val in sorted(time_groups.keys()):
                total = sum(time_groups[time_val])
                time_series.append({
                    "period": str(time_val),
                    "value": round(total, 2),
                    "count": len(time_groups[time_val])
                })

            # 计算变化趋势
            if len(time_series) >= 2:
                first_val = time_series[0]["value"]
                last_val = time_series[-1]["value"]
                change = last_val - first_val
                change_pct = (change / first_val * 100) if first_val != 0 else 0

                # 判断趋势方向
                if change_pct > 5:
                    trend = "上升"
                elif change_pct < -5:
                    trend = "下降"
                else:
                    trend = "稳定"

                #  计算逐期增长率（同比/环比）
                period_changes = []
                for i in range(1, len(time_series)):
                    prev_val = time_series[i-1]["value"]
                    curr_val = time_series[i]["value"]
                    period_change = curr_val - prev_val
                    period_change_pct = (period_change / prev_val * 100) if prev_val != 0 else 0
                    period_changes.append({
                        "from": time_series[i-1]["period"],
                        "to": time_series[i]["period"],
                        "change": round(period_change, 2),
                        "change_pct": round(period_change_pct, 2)
                    })

                insights["time_trend"] = {
                    "field": time_col,
                    "target": target_col,
                    "series": time_series,
                    "change": round(change, 2),
                    "change_pct": round(change_pct, 2),
                    "trend": trend,
                    "period_changes": period_changes  # 逐期变化
                }

    return insights


