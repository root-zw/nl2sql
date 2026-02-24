"""结果格式化器 - 为查询结果添加单位和格式化数值"""

from typing import List, Dict, Any, Optional, Tuple, Union
import structlog
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
import base64
import re

from server.models.semantic import SemanticModel, Metric, Measure
from server.models.ir import IntermediateRepresentation, MetricSpec

logger = structlog.get_logger()


def _get_metric_field_ids(metrics: List[Union[str, MetricSpec, dict]]) -> set:
    """
    从 metrics 列表中提取所有字段ID
    
    Args:
        metrics: metrics 列表，可能包含字符串或 MetricSpec 对象
        
    Returns:
        字段ID的集合
    """
    field_ids = set()
    for metric_item in (metrics or []):
        if isinstance(metric_item, str):
            field_ids.add(metric_item)
        elif isinstance(metric_item, dict):
            field_ids.add(metric_item.get("field", str(metric_item)))
        elif hasattr(metric_item, "field"):
            field_ids.add(metric_item.field)
        else:
            field_ids.add(str(metric_item))
    return field_ids


def _ensure_json_serializable(value: Any) -> Any:
    """
    将值转换为 JSON 可序列化格式，避免 bytes/memoryview 导致的 UTF-8 错误。
    """
    if value is None:
        return None

    if isinstance(value, memoryview):
        value = value.tobytes()

    if isinstance(value, (bytes, bytearray)):
        try:
            return base64.b64encode(bytes(value)).decode("ascii")
        except Exception:
            # 退化为十六进制字符串
            try:
                return bytes(value).hex()
            except Exception:
                return str(value)

    return value


def apply_unit_conversion(
    value: Any,
    unit_conversion_config: Optional[Dict[str, Any]],
    original_unit: Optional[str]
) -> Tuple[Any, str]:
    """
    应用单位转换配置

    Args:
        value: 原始值
        unit_conversion_config: 单位转换配置
        original_unit: 原始单位

    Returns:
        (转换后的值, 显示单位)

    配置格式:
    {
        "enabled": true,
        "display_unit": "公顷",
        "conversion": {
            "factor": 10000,
            "method": "divide",  # divide 或 multiply
            "precision": 2,
            "threshold": 10000  # 可选：小于此值不转换
        }
    }
    """
    # 如果没有配置或未启用，返回原值
    if not unit_conversion_config or not unit_conversion_config.get('enabled'):
        return value, original_unit or ''

    # 空值处理
    if value is None or value == '':
        return value, original_unit or ''

    # 尝试转换为数值
    try:
        if isinstance(value, str):
            # 移除可能的千分位逗号
            numeric_value = float(value.replace(',', ''))
        else:
            numeric_value = float(value)
    except (ValueError, TypeError, AttributeError):
        # 无法转换为数值，返回原值
        return value, original_unit or ''

    # 提取转换配置
    conversion = unit_conversion_config.get('conversion', {})
    # 取消阈值逻辑：无论数值大小，只要启用就转换

    # 执行转换
    factor = conversion.get('factor', 1)
    method = conversion.get('method', 'divide')

    if method == 'divide':
        converted_value = numeric_value / factor
    elif method == 'multiply':
        converted_value = numeric_value * factor
    else:
        # 未知方法，返回原值
        return value, original_unit or ''

    # 格式化精度
    precision = conversion.get('precision', 2)
    converted_value = round(converted_value, precision)

    # 返回转换后的值和显示单位
    display_unit = unit_conversion_config.get('display_unit', original_unit or '')

    logger.debug(
        "单位转换",
        original_value=numeric_value,
        converted_value=converted_value,
        original_unit=original_unit,
        display_unit=display_unit,
        method=method,
        factor=factor
    )

    return converted_value, display_unit


def _format_value_global(
    value: Any,
    col_name: str,
    format_map: Dict[str, Dict[str, Any]]
) -> Any:
    """
    格式化单个值（全局函数版本）

    Args:
        value: 原始值
        col_name: 列名
        format_map: 格式化映射

    Returns:
        格式化后的值
    """
    # 处理 None
    if value is None:
        return None

    # 确保值可序列化
    value = _ensure_json_serializable(value)

    # 获取格式化信息
    if col_name not in format_map:
        logger.debug(
            "列名不在format_map中",
            col_name=col_name,
            format_map_keys=list(format_map.keys())[:10]
        )
        return value

    info = format_map[col_name]
    
    # 防御性检查：确保必要的键存在
    if 'format' not in info:
        logger.error(
            "format_map中缺少'format'键",
            col_name=col_name,
            info_keys=list(info.keys()),
            info=info,
            format_map_keys=list(format_map.keys())[:10]
        )
        # 设置默认值，避免崩溃
        format_type = 'number'
        decimal_places = info.get('decimal_places', 2)
    else:
        format_type = info['format']
        decimal_places = info.get('decimal_places', 2)

    # 非数值类型直接返回
    if format_type == 'string':
        return value

    #  应用单位转换（在格式化之前）
    unit_conversion = info.get('unit_conversion')
    original_unit = info.get('original_unit')
    if unit_conversion:
        converted_value, _ = apply_unit_conversion(value, unit_conversion, original_unit)
        value = converted_value

    # 转换为数值
    try:
        from decimal import Decimal, ROUND_HALF_UP
        if isinstance(value, (int, float, Decimal)):
            numeric_value = Decimal(str(value))
        else:
            # 尝试转换字符串
            numeric_value = Decimal(str(value))
    except:
        # 无法转换，返回原值
        return value

    # 根据格式类型处理
    if format_type == 'integer':
        # 整数类型：不显示小数点
        return str(int(numeric_value))

    elif format_type in ('number', 'decimal', 'currency'):
        # 保留指定小数位数
        if decimal_places is not None:
            quantize_str = '0.' + '0' * decimal_places
            formatted = numeric_value.quantize(
                Decimal(quantize_str),
                rounding=ROUND_HALF_UP
            )
            # 返回字符串格式，保留末尾的 .00
            return format(formatted, f'.{decimal_places}f')
        # 如果没指定小数位，默认保留2位
        formatted = numeric_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        return format(formatted, '.2f')

    elif format_type == 'percentage':
        # 百分比：保留2位小数
        formatted = numeric_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        return format(formatted, '.2f')

    # 默认返回字符串格式的两位小数
    formatted = numeric_value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    return format(formatted, '.2f')


class ResultFormatter:
    """查询结果格式化器"""

    def __init__(self, semantic_model: SemanticModel):
        self.model = semantic_model

        # 派生指标缓存，用于总计计算时查找公式
        self._derived_metrics_cache = None

    def format_results(
        self,
        columns: List[Dict[str, str]],
        rows: List[List[Any]],
        ir: IntermediateRepresentation,
        global_rules: List[Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        格式化查询结果

        Args:
            columns: 列信息 [{"name": "metric_id", "type": "float"}, ...]
            rows: 行数据 [[value1, value2], [value3, value4], ...]
            ir: 中间表示（包含查询的指标和维度信息）

        Returns:
            格式化后的结果（字典列表）
        """
        if not rows:
            return []

        logger.debug("开始格式化结果", row_count=len(rows), column_count=len(columns))

        # 构建列格式化映射
        format_map = self._build_format_map(ir)
        logger.debug("format_map构建完成", format_map_keys=list(format_map.keys()))

        # 提取列名
        column_names = [col["name"] for col in columns]

        # 判断是否需要过滤/添加合计行
        # - 如果 with_total=True：SQL已生成合计，保留它
        # - 如果 with_total=False：SQL未生成合计，格式化层添加
        should_add_total = not getattr(ir, "with_total", False)

        # 如果SQL未生成合计，过滤掉数据库中可能存在的合计行（防止重复）
        from server.utils.text_templates import get_total_keywords
        total_keywords = get_total_keywords()
        filtered_rows = []

        for row in rows:
            # 如果SQL已经生成了合计（with_total=True），保留所有行
            if not should_add_total:
                filtered_rows.append(row)
                continue

            # 如果SQL未生成合计（with_total=False），过滤掉可能的合计行
            first_value = str(row[0]) if row and row[0] is not None else ""
            if any(keyword in first_value for keyword in total_keywords):
                logger.debug("跳过数据库中的合计行（will be recalculated）", first_value=first_value)
                continue
            filtered_rows.append(row)

        formatted_results = []
        total_rows_indices = []  # 记录合计行的索引，用于后续重新计算派生指标
        
        for row_idx, row in enumerate(filtered_rows):
            formatted_row = {}

            # 检查是否是合计行（SQL生成的）
            # 合计行可能出现在任何位置，需要检查所有维度列
            is_total_row = False
            if not should_add_total and row:
                # 检查所有维度列（通常是前几列）是否包含合计关键词
                dim_count = len(ir.dimensions) if ir.dimensions else 1
                for i in range(min(dim_count, len(row))):
                    cell_value = str(row[i]) if row[i] is not None else ""
                    if any(keyword in cell_value for keyword in total_keywords):
                        is_total_row = True
                        total_rows_indices.append(row_idx)
                        logger.debug(f"识别到合计行，第{i}列包含合计关键词: {cell_value}")
                        break

            # 将列表转换为字典
            for col_name, value in zip(column_names, row):
                #  隐藏内部列（以 _ 开头，如 _row_num）
                if col_name.startswith('_'):
                    continue

                # 格式化列名（添加单位）
                display_name = self._get_display_name_with_unit(col_name, format_map)
                if col_name != display_name:
                    logger.debug(f"列名转换: {col_name} -> {display_name}")

                # 格式化数值
                formatted_value = self._format_value(value, col_name, format_map)

                # 如果是合计行，添加加粗标记（包括"合计"文本本身）
                if is_total_row and formatted_value and str(formatted_value).strip():
                    # 合计行的所有内容都加粗，包括"合计"文本和数值
                    formatted_value = f"**{formatted_value}**"

                formatted_row[display_name] = formatted_value

            formatted_results.append(formatted_row)
        
        # 对于SQL生成的合计行，检查并重新计算派生指标
        if not should_add_total and total_rows_indices and global_rules:
            # 收集所有非合计行的数据，用于重新计算合计行的派生指标
            non_total_rows = [row for idx, row in enumerate(filtered_rows) if idx not in total_rows_indices]
            
            for total_idx in total_rows_indices:
                total_row_dict = formatted_results[total_idx]
                
                # 检查合计行中的每个指标列，如果是比率类派生指标，重新计算
                for col_name in column_names:
                    # 跳过内部列
                    if col_name.startswith('_'):
                        continue

                    display_name = self._get_display_name_with_unit(col_name, format_map)
                    # 检查是否为派生指标（包括 global_rules 和 IR calculated_fields）
                    metric_info = self._identify_derived_metric_type(col_name, global_rules, ir)
                    if metric_info.get('is_derived') and metric_info.get('calculation_type') == 'ratio':
                        # 比率类派生指标：需要重新计算
                        logger.debug(f"SQL生成的合计行中检测到比率类派生指标 {col_name}，重新计算")
                        
                        # 构建非合计行的格式化数据（用于字段查找与合计计算）
                        non_total_formatted_rows = []
                        for non_total_row in non_total_rows:
                            non_total_dict = {}
                            for nc_name, nc_value in zip(column_names, non_total_row):
                                nc_display = self._get_display_name_with_unit(nc_name, format_map)
                                non_total_dict[nc_display] = nc_value
                            non_total_formatted_rows.append(non_total_dict)
                        
                        # 获取 LLM 提供的分子分母字段引用和小数位数
                        numerator_refs = metric_info.get('numerator_refs', [])
                        denominator_refs = metric_info.get('denominator_refs', [])
                        decimal_places = metric_info.get('decimal_places', 2)
                        
                        # 重新计算比率总计
                        recalculated_total = self._recalculate_ratio_total(
                            col_name, metric_info.get('formula', ''), non_total_formatted_rows, global_rules,
                            non_total_rows, column_names, format_map, ir,
                            numerator_refs=numerator_refs, denominator_refs=denominator_refs,
                            decimal_places=decimal_places
                        )
                        
                        if recalculated_total:
                            # 更新合计行的值（保留加粗标记）
                            old_value = total_row_dict.get(display_name, "")
                            if old_value and old_value.startswith("**") and old_value.endswith("**"):
                                total_row_dict[display_name] = f"**{recalculated_total}**"
                            else:
                                total_row_dict[display_name] = f"**{recalculated_total}**"
                            logger.debug(f"已重新计算合计行中的派生指标 {col_name}: {recalculated_total}")

        # 添加合计行（仅当SQL未生成合计且是聚合查询时，且数据行大于1条）
        if should_add_total and formatted_results and len(formatted_results) > 1 and ir.metrics and getattr(ir, "query_type", None) == "aggregation":
            total_row = self._calculate_total_row_for_normal_table(
                formatted_results,
                column_names,
                ir,
                format_map,
                global_rules,
                filtered_rows  # 传递原始数据
            )
            if total_row:
                # 为合计行的所有数值添加加粗标记
                total_row = self._add_bold_style_to_row(total_row)
                formatted_results.append(total_row)
                logger.debug("已添加合计行到普通表格（SQL未生成合计）")
        elif should_add_total and len(formatted_results) == 1:
            logger.debug("只有1条数据，跳过合计行")
        elif not should_add_total:
            logger.debug("SQL已生成合计行（with_total=True），格式化层不重复添加")

        logger.debug("结果格式化完成")
        return formatted_results

    def should_pivot(self, ir: IntermediateRepresentation) -> bool:
        """
        判断是否应该生成透视表

        条件：
        1. 有年份维度（deal_year, reply_year, approval_year等）
        2. 有其他分组维度（district, reply_district等）
        3. 有度量/指标
        4. 查询类型为聚合查询
        """
        if not ir.dimensions or len(ir.dimensions) < 2:
            return False

        if getattr(ir, "query_type", None) != "aggregation":
            return False

        # 检查是否有年份维度
        year_dimensions = ["deal_year", "reply_year", "approval_year", "year"]
        has_year = any(dim in year_dimensions for dim in ir.dimensions)

        if not has_year:
            return False

        # 检查是否有其他分组维度
        non_year_dims = [dim for dim in ir.dimensions if dim not in year_dimensions]
        if not non_year_dims:
            return False

        # 检查是否有指标
        if not ir.metrics:
            return False

        logger.debug("检测到需要透视的查询", dimensions=ir.dimensions, metrics=ir.metrics)
        return True

    def pivot_results(
        self,
        columns: List[Dict[str, str]],
        rows: List[List[Any]],
        ir: IntermediateRepresentation,
        global_rules: List[Dict] = None
    ) -> Dict[str, Any]:
        """
        将查询结果转换为透视表格式

        Returns:
            {
                "columns": [...],  # 新的列结构
                "rows": [...],     # 透视后的行数据
                "is_pivoted": True
            }
        """
        logger.debug("开始生成透视表", row_count=len(rows))

        # 1. 识别年份维度和其他维度
        year_dimensions = ["deal_year", "reply_year", "approval_year", "year"]
        column_names = [col["name"] for col in columns]

        year_col = None
        row_dims = []
        metric_cols = []

        metric_field_ids = _get_metric_field_ids(ir.metrics)
        for col_name in column_names:
            if col_name in year_dimensions:
                year_col = col_name
            elif col_name in (ir.dimensions or []):
                row_dims.append(col_name)
            elif col_name in metric_field_ids:
                metric_cols.append(col_name)

        if not year_col or not row_dims or not metric_cols:
            logger.warning("无法生成透视表：缺少必要的列", year_col=year_col, row_dims=row_dims, metric_cols=metric_cols)
            return None

        logger.debug("透视表维度识别", year_col=year_col, row_dims=row_dims, metric_cols=metric_cols)

        # 2. 构建格式化映射
        format_map = self._build_format_map(ir)

        # 3. 将数据转换为字典列表（保留原始数据用于派生指标重新计算）
        data_dicts = []
        original_data_dicts = []  # 保存原始数据，用于派生指标重新计算
        for row in rows:
            row_dict = dict(zip(column_names, row))
            data_dicts.append(row_dict)
            original_data_dicts.append(row_dict)  # 保存原始数据副本

        # 4. 提取所有年份并排序
        # 过滤掉合计关键词（SQL生成的合计行年份也是"合计"）和 None 值
        from server.utils.text_templates import get_total_keywords
        total_keywords = get_total_keywords()
        all_years = set(row[year_col] for row in data_dicts)
        years = sorted([
            y for y in all_years
            if y is not None and not any(keyword in str(y) for keyword in total_keywords)
        ])
        logger.debug("检测到的年份", years=years, filtered_out=[y for y in all_years if y not in years])

        # 5. 按行维度分组数据
        grouped_data = defaultdict(lambda: defaultdict(dict))
        sql_total_row_data = None  # 保存SQL生成的合计行数据
        from server.utils.text_templates import get_total_keywords
        total_keywords = get_total_keywords()

        # 判断是否需要过滤/添加合计行
        should_add_total = not getattr(ir, "with_total", False)

        for row in data_dicts:
            # 检查是否是合计行（所有行维度都包含合计关键词）
            row_values = [str(row[dim]) if row[dim] is not None else "" for dim in row_dims]
            is_total_row = any(any(keyword in value for keyword in total_keywords) for value in row_values)

            if is_total_row:
                if should_add_total:
                    # SQL未生成合计，跳过数据库中可能存在的合计行
                    logger.debug("跳过数据库中的合计行（will be recalculated）", row_values=row_values)
                    continue
                else:
                    # SQL已生成合计，保存合计行数据，稍后单独处理
                    if sql_total_row_data is None:
                        sql_total_row_data = {}
                        for metric_col in metric_cols:
                            sql_total_row_data[metric_col] = row[metric_col]
                        logger.debug("保存SQL生成的合计行数据", metrics=list(sql_total_row_data.keys()))
                    continue

            # 构建行键（多个行维度的组合）
            row_key = tuple(row[dim] for dim in row_dims)
            year = row[year_col]

            # 保存该组合的指标值
            for metric_col in metric_cols:
                grouped_data[row_key][year][metric_col] = row[metric_col]

        # 6. 构建新的列结构
        new_columns = []

        # 添加行维度列
        for dim in row_dims:
            display_name = self._get_dimension_display_name(dim)
            new_columns.append({"name": display_name, "type": "string"})

        # 添加年份+指标列
        for year in years:
            for metric_col in metric_cols:
                display_name = self._get_display_name_with_unit(metric_col, format_map)
                col_name = f"{year}年{display_name}"
                new_columns.append({"name": col_name, "type": "string"})

        # 添加总计列
        for metric_col in metric_cols:
            display_name = self._get_display_name_with_unit(metric_col, format_map)
            col_name = f"总计{display_name}"
            new_columns.append({"name": col_name, "type": "string"})

        # 7. 构建新的行数据
        new_rows = []

        # 数据库的合计行已被过滤，这里只需要简单排序
        # 注意：排序时将 None 值当作空字符串处理，避免类型错误
        def sort_key(key):
            return tuple("" if v is None else v for v in key)

        for row_key in sorted(grouped_data.keys(), key=sort_key):
            new_row = []

            # 添加行维度值
            for dim_value in row_key:
                new_row.append(str(dim_value) if dim_value is not None else "")

            # 添加各年份的指标值
            metric_totals = defaultdict(Decimal)

            for year in years:
                for metric_col in metric_cols:
                    value = grouped_data[row_key][year].get(metric_col)

                    # 格式化数值
                    if value is not None:
                        formatted_value = self._format_value(value, metric_col, format_map)
                        new_row.append(formatted_value)

                        # 累计总计
                        try:
                            metric_totals[metric_col] += Decimal(str(value))
                        except:
                            pass
                    else:
                        new_row.append("")

            # 添加总计列（横向总计：跨年份合计）
            for metric_col in metric_cols:
                # 检查是否为派生指标（包括 global_rules 和 IR calculated_fields）
                metric_info = self._identify_derived_metric_type(metric_col, global_rules, ir)
                if metric_info.get('is_derived') and metric_info.get('calculation_type') == 'ratio':
                    # 比率类派生指标：尝试从原始数据重新计算
                    # 收集该行所有年份的原始数据行
                    row_original_rows = []
                    for year in years:
                        # 从原始数据中查找该行该年份的数据
                        for orig_row in original_data_dicts:
                            if (all(orig_row.get(dim) == row_key[i] for i, dim in enumerate(row_dims)) and
                                orig_row.get(year_col) == year):
                                row_original_rows.append(orig_row)
                                break
                    
                    if row_original_rows:
                        # 构建临时格式化行用于计算
                        temp_formatted_rows = []
                        for orig_row in row_original_rows:
                            display_name = self._get_display_name_with_unit(metric_col, format_map)
                            value = orig_row.get(metric_col)
                            if value is not None:
                                temp_formatted_rows.append({display_name: value})
                        
                        # 获取 LLM 提供的分子分母字段引用和小数位数
                        numerator_refs = metric_info.get('numerator_refs', [])
                        denominator_refs = metric_info.get('denominator_refs', [])
                        decimal_places = metric_info.get('decimal_places', 2)
                        
                        # 尝试重新计算比率总计
                        formatted_total = self._recalculate_ratio_total(
                            metric_col, metric_info['formula'], temp_formatted_rows, global_rules,
                            None, column_names, format_map, ir,
                            numerator_refs=numerator_refs, denominator_refs=denominator_refs,
                            decimal_places=decimal_places
                        )
                        
                        if not formatted_total:
                            # 降级：使用简单求和
                            total = metric_totals.get(metric_col, Decimal(0))
                            formatted_total = self._format_value(float(total), metric_col, format_map)
                    else:
                        # 无法找到原始数据，使用简单求和
                        total = metric_totals.get(metric_col, Decimal(0))
                        formatted_total = self._format_value(float(total), metric_col, format_map)
                else:
                    # 非比率类或非派生指标：直接求和
                    total = metric_totals.get(metric_col, Decimal(0))
                    formatted_total = self._format_value(float(total), metric_col, format_map)
                
                new_row.append(formatted_total)

            new_rows.append(new_row)

        # 8. 添加总计行（纵向汇总，仅当数据行大于1条时）
        if new_rows and len(new_rows) > 1:
            # 无论SQL是否生成合计，都需要格式化层计算各年份的纵向合计
            # 因为SQL的合计是全表汇总（不分年份），而透视表需要分年份的合计
            total_row = self._calculate_total_row(
                new_rows,
                row_dims,
                years,
                metric_cols,
                format_map,
                global_rules,
                grouped_data,  # 传递分组后的原始数据
                column_names,
                ir  # 传递 IR 用于识别 calculated_fields 中的比率类型
            )

            # 为合计行添加加粗标记
            bold_total_row = []
            for i, value in enumerate(total_row):
                if value and str(value).strip():
                    bold_total_row.append(f"**{value}**")
                else:
                    bold_total_row.append(value)

            new_rows.append(bold_total_row)

            if should_add_total:
                logger.debug("已添加合计行到透视表（格式化层计算）")
            else:
                logger.debug("已添加合计行到透视表（格式化层重新计算，因为SQL的合计不分年份）")
        elif new_rows and len(new_rows) == 1:
            logger.debug("透视表只有1条数据，跳过合计行")

        logger.debug("透视表生成完成", pivot_rows=len(new_rows), pivot_cols=len(new_columns))

        return {
            "columns": new_columns,
            "rows": new_rows,
            "is_pivoted": True
        }

    def _calculate_total_row(
        self,
        data_rows: List[List[Any]],
        row_dims: List[str],
        years: List[int],
        metric_cols: List[str],
        format_map: Dict[str, Dict[str, Any]],
        global_rules: List[Dict] = None,
        grouped_data: Dict = None,
        column_names: List[str] = None,
        ir: IntermediateRepresentation = None
    ) -> List[str]:
        """
        计算总计行（纵向汇总所有行的数据）

        Args:
            data_rows: 数据行（已格式化的透视表行）
            row_dims: 行维度列表
            years: 年份列表
            metric_cols: 指标列表
            format_map: 格式化映射
            global_rules: 全局规则列表
            grouped_data: 分组后的原始数据（用于派生指标重新计算）
            column_names: 原始列名列表
            ir: 中间表示（包含 calculated_fields 信息）

        Returns:
            总计行数据
        """
        total_row = []

        # 第一列显示"合计"
        total_row.append("合计")

        # 其他行维度列为空
        for _ in range(len(row_dims) - 1):
            total_row.append("")

        # 计算每一列的合计
        # 跳过行维度列，从年份指标列开始
        num_dim_cols = len(row_dims)
        num_value_cols = len(years) * len(metric_cols) + len(metric_cols)  # 年份指标 + 总计指标

        for col_idx in range(num_value_cols):
            # 判断是哪个指标和年份（或总计列）
            metric_idx = col_idx // (len(years) + 1)  # 每个指标有len(years)个年份列 + 1个总计列
            position_in_metric = col_idx % (len(years) + 1)
            
            if metric_idx < len(metric_cols):
                metric_col = metric_cols[metric_idx]
                
                # 检查是否为派生指标（包括 global_rules 定义的和 IR calculated_fields 中的）
                metric_info = self._identify_derived_metric_type(metric_col, global_rules, ir)
                is_derived_ratio = (metric_info.get('is_derived') and 
                                  metric_info.get('calculation_type') == 'ratio')
                
                if is_derived_ratio and position_in_metric < len(years) and grouped_data:
                    # 比率类派生指标的年份列：需要重新计算
                    year = years[position_in_metric]
                    
                    # 收集该年份所有行的原始数据
                    year_values = []
                    for row_key in grouped_data.keys():
                        value = grouped_data[row_key][year].get(metric_col)
                        if value is not None:
                            year_values.append(value)
                    
                    if year_values:
                        # 构建临时格式化行
                        display_name = self._get_display_name_with_unit(metric_col, format_map)
                        temp_formatted_rows = [{display_name: v} for v in year_values]
                        
                        # 获取 LLM 提供的分子分母字段引用和小数位数
                        numerator_refs = metric_info.get('numerator_refs', [])
                        denominator_refs = metric_info.get('denominator_refs', [])
                        decimal_places = metric_info.get('decimal_places', 2)
                        
                        # 重新计算比率总计
                        formatted_total = self._recalculate_ratio_total(
                            metric_col, metric_info['formula'], temp_formatted_rows, global_rules,
                            None, column_names, format_map, ir,
                            numerator_refs=numerator_refs, denominator_refs=denominator_refs,
                            decimal_places=decimal_places
                        )
                        
                        if formatted_total:
                            total_row.append(formatted_total)
                        else:
                            # 降级：使用简单求和
                            col_total = Decimal(0)
                            for row in data_rows:
                                value_idx = num_dim_cols + col_idx
                                if value_idx < len(row):
                                    value_str = row[value_idx]
                                    if value_str and value_str != "":
                                        try:
                                            clean_value = str(value_str).replace(",", "")
                                            col_total += Decimal(clean_value)
                                        except:
                                            pass
                            total_row.append(self._format_decimal_value(col_total) if col_total != 0 else "")
                    else:
                        total_row.append("")
                else:
                    # 非比率类派生指标或总计列：直接求和
                    col_total = Decimal(0)
                    col_has_value = False

                    for row in data_rows:
                        value_idx = num_dim_cols + col_idx
                        if value_idx < len(row):
                            value_str = row[value_idx]
                            if value_str and value_str != "":
                                try:
                                    # 移除千分位分隔符
                                    clean_value = str(value_str).replace(",", "")
                                    col_total += Decimal(clean_value)
                                    col_has_value = True
                                except:
                                    pass

                    # 格式化合计值
                    if col_has_value:
                        # 使用与其他行相同的格式化逻辑
                        formatted_total = self._format_decimal_value(col_total)
                        total_row.append(formatted_total)
                    else:
                        total_row.append("")
            else:
                # 超出范围，使用默认求和
                col_total = Decimal(0)
                col_has_value = False

                for row in data_rows:
                    value_idx = num_dim_cols + col_idx
                    if value_idx < len(row):
                        value_str = row[value_idx]
                        if value_str and value_str != "":
                            try:
                                clean_value = str(value_str).replace(",", "")
                                col_total += Decimal(clean_value)
                                col_has_value = True
                            except:
                                pass

                if col_has_value:
                    total_row.append(self._format_decimal_value(col_total))
                else:
                    total_row.append("")

        return total_row

    def _format_decimal_value(self, value: Decimal, decimal_places: int = 2) -> str:
        """
        格式化 Decimal 数值
        
        Args:
            value: 要格式化的 Decimal 值
            decimal_places: 小数位数，默认2位
        """
        # 动态生成量化精度
        quantize_str = '0.' + '0' * decimal_places if decimal_places > 0 else '1'
        formatted = value.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        return format(formatted, f'.{decimal_places}f')

    def _identify_derived_metric_type(self, metric_name: str, global_rules: List[Dict] = None, 
                                       ir: IntermediateRepresentation = None) -> Dict[str, Any]:
        """
        识别派生指标的类型和计算策略

        Args:
            metric_name: 指标名称（可能是原始列名或显示名称）
            global_rules: 全局规则列表
            ir: 中间表示（包含 calculated_fields 信息）

        Returns:
            包含派生指标信息的字典 {
                'is_derived': bool,
                'formula': str,
                'calculation_type': str,  # 'sum', 'ratio', 'weighted_average', 'complex'
                'field_dependencies': List[Dict]  # 完整的依赖信息，包含field_id和placeholder
            }
        """
        metric_name = metric_name or ""
        # 统一处理带单位后缀的列名：
        # - "每亩单价(万元/亩)" -> "每亩单价"
        # - "每亩单价（万元/亩）" -> "每亩单价"
        import re
        metric_name_base = re.split(r"[（(]", metric_name, maxsplit=1)[0].strip()

        # 1. 首先检查是否是 IR 中的 calculated_field（LLM 动态生成的计算字段）
        if ir and hasattr(ir, 'calculated_fields') and ir.calculated_fields:
            for calc_field in ir.calculated_fields:
                alias = getattr(calc_field, 'alias', '')
                # 支持带单位后缀的列名匹配，如 "平均溢价率" 匹配 "平均溢价率(万元)"
                if alias and (alias == metric_name_base or metric_name.startswith(alias + '(') or metric_name.startswith(alias + '（')):
                    expression = getattr(calc_field, 'expression', '') or ''
                    field_refs = getattr(calc_field, 'field_refs', []) or []
                    aggregation = getattr(calc_field, 'aggregation', None)
                    
                    # 优先使用 LLM 指定的 total_strategy
                    total_strategy = getattr(calc_field, 'total_strategy', None)
                    
                    # 根据 total_strategy 确定 calculation_type
                    if total_strategy:
                        strategy_to_type = {
                            'sum': 'sum',
                            'recalculate': 'ratio',  # 需要重新计算公式
                            'weighted_avg': 'weighted_average',
                            'max': 'max',
                            'min': 'min',
                            'none': 'none'
                        }
                        calculation_type = strategy_to_type.get(total_strategy, 'complex')
                        logger.debug(f"使用 LLM 指定的 total_strategy: {alias}, strategy={total_strategy}, type={calculation_type}")
                    else:
                        # 降级：根据表达式和聚合类型推断
                        # 比率类型：表达式包含除法运算
                        is_ratio = '/' in expression
                        # 如果使用 AVG 聚合且包含除法，也是比率/加权平均类型
                        is_weighted_avg = aggregation == 'AVG' and '/' in expression
                        
                        if is_ratio or is_weighted_avg:
                            calculation_type = 'ratio' if is_ratio else 'weighted_average'
                        elif aggregation == 'MAX':
                            calculation_type = 'max'
                        elif aggregation == 'MIN':
                            calculation_type = 'min'
                        elif aggregation == 'AVG':
                            calculation_type = 'weighted_average'
                        else:
                            calculation_type = 'sum'
                        logger.debug(f"推断 calculated_field 类型: {alias}, expression={expression}, aggregation={aggregation}, type={calculation_type}")
                    
                    # 构建字段依赖信息
                    field_dependencies = [{'field_id': ref, 'placeholder': ref} for ref in field_refs]
                    
                    # 获取 LLM 提供的分子分母字段引用（用于精确计算合计行）
                    numerator_refs = getattr(calc_field, 'numerator_refs', None) or []
                    denominator_refs = getattr(calc_field, 'denominator_refs', None) or []
                    
                    # 获取小数位数
                    decimal_places = getattr(calc_field, 'decimal_places', 2)
                    
                    return {
                        'is_derived': True,
                        'formula': expression,
                        'calculation_type': calculation_type,
                        'field_dependencies': field_dependencies,
                        'display_name': alias,
                        'source': 'calculated_field',
                        'numerator_refs': numerator_refs,
                        'denominator_refs': denominator_refs,
                        'decimal_places': decimal_places
                    }
        
        # 2. 检查是否是派生指标（支持多种格式）
        derived_name = None
        if metric_name.startswith('derived:'):
            derived_name = metric_name[8:]  # 移除"derived:"前缀
        else:
            # 可能是显示名称，需要从全局规则中查找
            if global_rules:
                for rule in global_rules:
                    if rule.get('rule_type') == 'derived_metric':
                        rule_def = rule.get('rule_definition', {})
                        display_name = rule_def.get('display_name', rule.get('rule_name', '').replace('（派生）', ''))
                        # 宽松匹配：允许中英文括号差异、以及“显示名已内含单位”的情况
                        display_base = re.split(r"[（(]", display_name or "", maxsplit=1)[0].strip()
                        if display_name == metric_name_base or display_base == metric_name_base:
                            derived_name = display_name
                            break

        if not derived_name:
            return {'is_derived': False}

        # 从全局规则中查找派生指标定义
        if global_rules:
            for rule in global_rules:
                if rule.get('rule_type') == 'derived_metric':
                    rule_def = rule.get('rule_definition', {})
                    display_name = rule_def.get('display_name', rule.get('rule_name', '').replace('（派生）', ''))
                    if display_name == derived_name:
                        formula = rule_def.get('formula', '')
                        field_deps = rule_def.get('field_dependencies', [])

                        # 分析公式类型，确定总计计算策略
                        calculation_type = self._analyze_formula_type(formula)

                        return {
                            'is_derived': True,
                            'formula': formula,
                            'calculation_type': calculation_type,
                            'field_dependencies': field_deps,  # 返回完整的依赖信息
                            'display_name': display_name,
                            'source': 'global_rules'
                        }

        return {'is_derived': False}

    def _analyze_formula_type(self, formula: str) -> str:
        """
        分析派生指标公式的类型，决定总计计算方式

        Args:
            formula: 计算公式

        Returns:
            'sum': 可直接求和的绝对值指标
            'ratio': 比率指标，需要重新计算
            'weighted_average': 加权平均指标
            'complex': 复杂计算，暂时求和
        """
        if not formula:
            return 'sum'

        formula = formula.upper().strip()

        # 比率类型：包含除法运算
        if ' / ' in formula and ('SUM(' in formula or 'COUNT(' in formula or 'AVG(' in formula):
            return 'ratio'

        # 加权平均：包含平均函数和分母
        if 'AVG(' in formula and ' / ' in formula:
            return 'weighted_average'

        # 简单加减：可以直接求和
        if 'SUM(' in formula and (' + ' in formula or ' - ' in formula) and ' / ' not in formula:
            return 'sum'

        # 默认为复杂计算，暂时求和
        return 'complex'

    def _calculate_derived_metric_total(self, metric_name: str, formatted_rows: List[Dict[str, Any]],
                                      global_rules: List[Dict] = None,
                                      original_rows: List[List[Any]] = None,
                                      column_names: List[str] = None,
                                      format_map: Dict[str, Dict[str, Any]] = None,
                                      ir: IntermediateRepresentation = None) -> str:
        """
        计算派生指标的总计值

        Args:
            metric_name: 派生指标名称（可能是原始列名或显示名称）
            formatted_rows: 格式化后的数据行
            global_rules: 全局规则列表
            original_rows: 原始数据行（未格式化）
            column_names: 原始列名列表
            format_map: 格式化映射
            ir: 中间表示（包含 calculated_fields 信息）

        Returns:
            格式化后的总计值
        """
        metric_info = self._identify_derived_metric_type(metric_name, global_rules, ir)

        if not metric_info['is_derived']:
            # 不是派生指标，使用原有的求和逻辑
            return self._calculate_simple_total(metric_name, formatted_rows)

        calculation_type = metric_info['calculation_type']
        formula = metric_info['formula']
        # 获取 LLM 提供的分子分母字段引用（如果有）
        numerator_refs = metric_info.get('numerator_refs', [])
        denominator_refs = metric_info.get('denominator_refs', [])
        # 获取小数位数
        decimal_places = metric_info.get('decimal_places', 2)

        if calculation_type == 'sum':
            # 绝对值指标：可以直接求和
            return self._calculate_simple_total(metric_name, formatted_rows)
        elif calculation_type == 'ratio':
            # 比率指标：需要重新计算公式
            # 优先使用 LLM 提供的 numerator_refs/denominator_refs，否则解析公式推断
            return self._recalculate_ratio_total(
                metric_name, formula, formatted_rows, global_rules,
                original_rows, column_names, format_map, ir,
                numerator_refs=numerator_refs, denominator_refs=denominator_refs,
                decimal_places=decimal_places
            )
        elif calculation_type == 'weighted_average':
            # 加权平均：计算加权平均值
            return self._calculate_weighted_average_total(metric_name, formula, formatted_rows, global_rules)
        elif calculation_type == 'max':
            # 最大值：取所有行中的最大值
            return self._calculate_max_total(metric_name, formatted_rows)
        elif calculation_type == 'min':
            # 最小值：取所有行中的最小值
            return self._calculate_min_total(metric_name, formatted_rows)
        elif calculation_type == 'none':
            # 不显示合计
            return ""
        else:
            # 复杂计算：暂时求和
            logger.warning(f"派生指标 {metric_name} 使用复杂计算，暂时按求和处理")
            return self._calculate_simple_total(metric_name, formatted_rows)

    def _calculate_simple_total(self, column_name: str, formatted_rows: List[Dict[str, Any]]) -> str:
        """
        计算简单的列总计（原有逻辑）
        """
        col_total = Decimal(0)
        col_has_value = False

        for row in formatted_rows:
            value_str = row.get(column_name, "")
            if value_str and value_str != "":
                try:
                    # 移除千分位分隔符
                    clean_value = str(value_str).replace(",", "")
                    col_total += Decimal(clean_value)
                    col_has_value = True
                except:
                    pass

        return self._format_decimal_value(col_total) if col_has_value else ""

    def _recalculate_ratio_total(self, metric_name: str, formula: str, formatted_rows: List[Dict[str, Any]],
                                global_rules: List[Dict] = None,
                                original_rows: List[List[Any]] = None,
                                column_names: List[str] = None,
                                format_map: Dict[str, Dict[str, Any]] = None,
                                ir: IntermediateRepresentation = None,
                                numerator_refs: List[str] = None,
                                denominator_refs: List[str] = None,
                                decimal_places: int = 2) -> str:
        """
        重新计算比率类指标的总计值
        例如：毛利率 = (收入 - 成本) / 收入，总计应该重新计算而不是求和
        
        核心逻辑：
        1. 优先使用 LLM 提供的 numerator_refs 和 denominator_refs 精确计算
        2. 如果没有提供，则解析公式推断分子分母表达式
        
        Args:
            numerator_refs: LLM 提供的分子字段ID列表
            denominator_refs: LLM 提供的分母字段ID列表
        """
        try:
            multiplier = Decimal(1)
            numerator_total = Decimal(0)
            denominator_total = Decimal(0)
            use_refs_success = False  # 标记是否成功使用字段引用计算
            
            # 优先使用 LLM 提供的 numerator_refs 和 denominator_refs
            if numerator_refs and denominator_refs:
                logger.debug(f"使用 LLM 提供的字段引用计算比率合计: numerator_refs={numerator_refs}, denominator_refs={denominator_refs}")
                
                # 检查公式中是否有乘数（如 * 100）
                if formula:
                    import re
                    mult_match = re.search(r'\*\s*(\d+(?:\.\d+)?)\s*$', formula)
                    if mult_match:
                        multiplier = Decimal(mult_match.group(1))
                
                # 计算分子总和：累加所有分子字段的合计
                numerator_found_count = 0
                for ref_id in numerator_refs:
                    ref_total = self._get_field_total_by_id(
                        ref_id, formatted_rows, original_rows, column_names, format_map, ir
                    )
                    if ref_total is not None:
                        numerator_total += ref_total
                        numerator_found_count += 1
                    else:
                        logger.warning(f"比率指标 {metric_name}: 分子字段 {ref_id} 找不到数据")
                
                # 计算分母总和：累加所有分母字段的合计
                denominator_found_count = 0
                for ref_id in denominator_refs:
                    ref_total = self._get_field_total_by_id(
                        ref_id, formatted_rows, original_rows, column_names, format_map, ir
                    )
                    if ref_total is not None:
                        denominator_total += ref_total
                        denominator_found_count += 1
                    else:
                        logger.warning(f"比率指标 {metric_name}: 分母字段 {ref_id} 找不到数据")
                
                # 检查是否所有字段都找到了
                if numerator_found_count == len(numerator_refs) and denominator_found_count == len(denominator_refs):
                    use_refs_success = True
                    logger.debug(f"使用字段引用计算: numerator_total={numerator_total}, denominator_total={denominator_total}, multiplier={multiplier}")
                else:
                    logger.warning(f"比率指标 {metric_name}: 部分字段引用无效，降级到公式解析")
            
            # 如果字段引用无效或未提供，降级到公式解析
            if not use_refs_success:
                # 0) 优先尝试“聚合公式直接求值”（支持 SUM(...) / SUM(...) 这类派生指标公式）
                # 这类公式在全局规则中很常见，但不符合 {A}/{B} 的占位符格式
                agg_value = self._try_evaluate_aggregate_formula_total(
                    formula,
                    metric_name=metric_name,
                    formatted_rows=formatted_rows,
                    global_rules=global_rules,
                    original_rows=original_rows,
                    column_names=column_names,
                    format_map=format_map,
                    ir=ir
                )
                if agg_value is not None:
                    return self._format_decimal_value(agg_value, decimal_places)

                # 重置计算值
                multiplier = Decimal(1)
                numerator_total = Decimal(0)
                denominator_total = Decimal(0)
                
                # 解析公式，提取分子和分母表达式
                numerator_expr, denominator_expr, multiplier = self._parse_ratio_formula(formula)
                
                if not numerator_expr or not denominator_expr:
                    logger.warning(f"无法解析比率公式: {formula}")
                    return self._calculate_simple_total(metric_name, formatted_rows)
                
                logger.debug(f"解析比率公式: numerator={numerator_expr}, denominator={denominator_expr}, multiplier={multiplier}")
                
                # 计算分子表达式的合计值
                numerator_total = self._evaluate_expression_total(
                    numerator_expr, formatted_rows, global_rules, 
                    original_rows, column_names, format_map, ir
                )
                
                # 计算分母表达式的合计值
                denominator_total = self._evaluate_expression_total(
                    denominator_expr, formatted_rows, global_rules,
                    original_rows, column_names, format_map, ir
                )
            
            logger.debug(f"比率合计计算: numerator_total={numerator_total}, denominator_total={denominator_total}")

            # 计算比率
            if denominator_total and denominator_total != 0:
                ratio_total = numerator_total / denominator_total * multiplier
                return self._format_decimal_value(ratio_total, decimal_places)
            elif numerator_total and numerator_total != 0:
                logger.warning(f"比率指标 {metric_name} 分母为0，无法计算")
                return ""
            else:
                return ""

        except Exception as e:
            logger.error(f"重新计算比率指标 {metric_name} 失败: {e}", exc_info=True)

        # 降级处理：使用简单求和
        return self._calculate_simple_total(metric_name, formatted_rows)
    
    def _try_evaluate_aggregate_formula_total(
        self,
        formula: str,
        metric_name: str,
        formatted_rows: List[Dict[str, Any]],
        global_rules: List[Dict] = None,
        original_rows: List[List[Any]] = None,
        column_names: List[str] = None,
        format_map: Dict[str, Dict[str, Any]] = None,
        ir: IntermediateRepresentation = None
    ) -> Optional[Decimal]:
        """
        尝试对包含 SUM/COUNT/AVG 等聚合函数的“派生指标公式”进行合计求值。

        典型示例：
        - SUM(总价) * 10000 / SUM(建筑面积)
        - SUM(总价) / (SUM(出让面积) * 15) * 10000

        返回：
        - 计算成功：Decimal
        - 不支持/无法解析：None（由上层继续降级）
        """
        if not formula:
            return None

        # 仅在包含聚合函数时启用；否则让占位符解析逻辑处理
        f = str(formula).strip()
        upper = f.upper()
        if "SUM(" not in upper and "COUNT(" not in upper and "AVG(" not in upper:
            return None

        # 兼容符号：× ÷
        f = f.replace("×", "*").replace("÷", "/")

        import re
        import ast
        from decimal import DivisionByZero, InvalidOperation

        # 安全求值：仅允许 + - * / () 和数字（以及少量必要节点）
        allowed_nodes = (
            ast.Expression, ast.BinOp, ast.UnaryOp,
            ast.Add, ast.Sub, ast.Mult, ast.Div,
            ast.USub, ast.UAdd,
            ast.Constant, ast.Num,
            ast.Load,
            ast.Pow  # 保险起见：不鼓励但允许，后面可按需移除
        )

        def _eval(node) -> Decimal:
            if not isinstance(node, allowed_nodes):
                raise ValueError(f"unsupported node: {type(node).__name__}")
            if isinstance(node, ast.Expression):
                return _eval(node.body)
            if isinstance(node, ast.Constant):
                return Decimal(str(node.value))
            if isinstance(node, ast.Num):  # py<3.8
                return Decimal(str(node.n))
            if isinstance(node, ast.UnaryOp):
                val = _eval(node.operand)
                if isinstance(node.op, ast.USub):
                    return -val
                if isinstance(node.op, ast.UAdd):
                    return val
                raise ValueError("unsupported unary op")
            if isinstance(node, ast.BinOp):
                left = _eval(node.left)
                right = _eval(node.right)
                if isinstance(node.op, ast.Add):
                    return left + right
                if isinstance(node.op, ast.Sub):
                    return left - right
                if isinstance(node.op, ast.Mult):
                    return left * right
                if isinstance(node.op, ast.Div):
                    if right == 0:
                        raise DivisionByZero()
                    return left / right
                if isinstance(node.op, ast.Pow):
                    return left ** int(right)
                raise ValueError("unsupported bin op")
            raise ValueError("unsupported expr")

        def _normalize_field_name(name: str) -> str:
            if name is None:
                return ""
            s = str(name).strip()
            # 去掉单位后缀与常见SQL包裹
            s = s.replace("[", "").replace("]", "").replace('"', "").replace("`", "").strip()
            # 只取最后一段（去表前缀）
            if "." in s:
                s = s.split(".")[-1].strip()
            # 兼容中英文括号单位后缀
            import re
            s = re.split(r"[（(]", s, maxsplit=1)[0].strip()
            return s

        # 将 SUM(x) / COUNT(x) / AVG(x) 替换为“对应字段的合计值”常量
        # 注意：这里只处理“合计行”场景，所以 SUM(x) 直接使用 x 的总和。
        pattern = re.compile(r"(SUM|COUNT|AVG)\s*\(\s*([^)]+?)\s*\)", re.IGNORECASE)

        def _to_decimal(s: str) -> Optional[Decimal]:
            if s is None:
                return None
            try:
                return Decimal(str(s).replace(",", "").replace("**", "").strip())
            except Exception:
                return None

        replaced_any = False
        missing_sum_fields: List[str] = []
        resolved_sum_fields: Dict[str, Decimal] = {}
        for m in list(pattern.finditer(f)):
            agg = (m.group(1) or "").upper()
            inner = (m.group(2) or "").strip()

            # COUNT(*) 特例：用原始行数（如果可用）兜底
            if agg == "COUNT" and inner == "*":
                if original_rows is not None:
                    total_dec = Decimal(len(original_rows))
                else:
                    total_dec = Decimal(len(formatted_rows or []))
            else:
                # SUM/COUNT/AVG(x)：这里统一用 x 的合计值（合计行语义）
                total_str = self._find_and_calculate_field_total(
                    inner,
                    formatted_rows=formatted_rows,
                    global_rules=global_rules,
                    original_rows=original_rows,
                    column_names=column_names,
                    format_map=format_map,
                    ir=ir
                )
                total_dec = _to_decimal(total_str)
                if total_dec is None:
                    if agg == "SUM":
                        missing_sum_fields.append(inner)
                    else:
                        return None
                else:
                    resolved_sum_fields[inner] = total_dec

            # 用数值常量替换该聚合调用
            if total_dec is not None:
                f = f.replace(m.group(0), str(total_dec))
                replaced_any = True

        # 如果 SUM(...) 有缺失字段，尝试用“反推分母”方式补齐（典型：楼面地价缺 SUM(建筑面积) 列）
        # 支持形态：metric = SUM(A) * k / (SUM(B) * m) * t
        if missing_sum_fields:
            try:
                # 仅处理“缺失 1 个 SUM 字段”的场景
                if len(missing_sum_fields) == 1:
                    missing_field = missing_sum_fields[0]

                    # 使用已有解析函数拆分比率公式（不依赖 {placeholder}）
                    numerator_expr, denominator_expr, trailing_multiplier = self._parse_ratio_formula(formula.replace("×", "*").replace("÷", "/"))
                    if numerator_expr and denominator_expr:
                        def _extract_sum_field_and_factor(expr: str) -> Optional[Tuple[str, Decimal]]:
                            mm = re.search(r"SUM\s*\(\s*([^)]+?)\s*\)", expr, re.IGNORECASE)
                            if not mm:
                                return None
                            field = mm.group(1).strip()
                            expr_num = re.sub(r"SUM\s*\(\s*([^)]+?)\s*\)", "1", expr, flags=re.IGNORECASE).strip()
                            if not expr_num:
                                return field, Decimal(1)
                            if not re.fullmatch(r"[\d\.\s\*\+/\-\(\)]+", expr_num):
                                return None
                            tree = ast.parse(expr_num, mode="eval")
                            return field, _eval(tree)

                        num_info = _extract_sum_field_and_factor(numerator_expr)
                        den_info = _extract_sum_field_and_factor(denominator_expr)
                        if num_info and den_info:
                            num_field, num_factor = num_info
                            den_field, den_factor = den_info

                            # 如果缺失的是 den_field，对其进行反推
                            if _normalize_field_name(den_field) == _normalize_field_name(missing_field):
                                # k = num_factor * trailing_multiplier / den_factor
                                k = (num_factor * (trailing_multiplier or Decimal(1))) / (den_factor or Decimal(1))

                                # 找到每行的 A_i（num_field）与 metric_i
                                def _get_col_key_by_base(base: str) -> Optional[str]:
                                    if not formatted_rows:
                                        return None
                                    b = _normalize_field_name(base)
                                    for key in formatted_rows[0].keys():
                                        if _normalize_field_name(key) == b:
                                            return key
                                    return None

                                metric_key = _get_col_key_by_base(metric_name)
                                num_key = _get_col_key_by_base(num_field)

                                # num_key 优先用原始列（避免单位换算）；取不到再降级 formatted_rows
                                num_series = []
                                if original_rows and column_names and num_field:
                                    idx = None
                                    nf = _normalize_field_name(num_field)
                                    for i, cn in enumerate(column_names):
                                        if _normalize_field_name(cn) == nf:
                                            idx = i
                                            break
                                    if idx is not None:
                                        for r in original_rows:
                                            num_series.append(r[idx] if idx < len(r) else None)
                                if not num_series and num_key:
                                    for r in formatted_rows:
                                        num_series.append(r.get(num_key))

                                if metric_key and num_series and len(num_series) == len(formatted_rows):
                                    denom_total = Decimal(0)
                                    denom_has = False
                                    for row, a_val in zip(formatted_rows, num_series):
                                        m_val = row.get(metric_key)
                                        a_dec = _to_decimal(a_val)
                                        m_dec = _to_decimal(m_val)
                                        if a_dec is None or m_dec is None or m_dec == 0:
                                            continue
                                        denom_total += (a_dec * k) / m_dec
                                        denom_has = True

                                    if denom_has:
                                        a_total_str = self._find_and_calculate_field_total(
                                            num_field,
                                            formatted_rows=formatted_rows,
                                            global_rules=global_rules,
                                            original_rows=original_rows,
                                            column_names=column_names,
                                            format_map=format_map,
                                            ir=ir
                                        )
                                        a_total = _to_decimal(a_total_str)
                                        if a_total is not None and denom_total != 0:
                                            return (a_total * k) / denom_total
            except Exception:
                pass

        if not replaced_any:
            return None

        # 处理常见的 NULLIF(x, 0) 防除零包装：
        # - 在“合计行求值”语义下，可将 NULLIF(x,0) 视为 x
        # - 除零保护由 _eval 的 DivisionByZero 捕获
        try:
            nullif_pattern = re.compile(
                r"NULLIF\s*\(\s*([^,]+?)\s*,\s*0+(?:\.0+)?\s*\)",
                flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
            )
            f = nullif_pattern.sub(r"(\1)", f)
        except Exception:
            pass

        try:
            tree = ast.parse(f, mode="eval")
            return _eval(tree)
        except (SyntaxError, ValueError, DivisionByZero, InvalidOperation):
            return None

    def _get_field_total_by_id(
        self,
        field_id: str,
        formatted_rows: List[Dict[str, Any]],
        original_rows: List[List[Any]],
        column_names: List[str],
        format_map: Dict[str, Dict[str, Any]],
        ir: IntermediateRepresentation
    ) -> Optional[Decimal]:
        """
        根据字段ID获取该字段的合计值
        
        Args:
            field_id: 字段UUID（可能是度量字段ID或派生指标如 'derived:宗数'）
        
        Returns:
            字段的合计值，如果无法计算则返回 None
        """
        try:
            # 1. 处理派生指标（如 'derived:宗数'）
            if field_id.startswith('derived:'):
                derived_name = field_id[8:]
                # 在 formatted_rows 中查找对应的列
                if formatted_rows:
                    for col_key in formatted_rows[0].keys():
                        import re
                        clean_key = re.split(r"[（(]", col_key, maxsplit=1)[0].strip()
                        # 精确匹配：列名完全等于派生指标名，避免 "宗数" 误匹配 "宗数占比"
                        if clean_key == derived_name:
                            # 找到列，计算合计
                            col_total = Decimal(0)
                            for r in formatted_rows:
                                value_str = str(r.get(col_key, "0")).replace(",", "").replace("-", "0").replace("**", "")
                                if value_str.strip():
                                    try:
                                        col_total += Decimal(value_str)
                                    except:
                                        pass
                            return col_total
                return None
            
            # 2. 处理普通字段ID（UUID）
            # 尝试从 model 获取字段显示名
            field_display_name = None
            if hasattr(self, 'model'):
                if hasattr(self.model, 'fields') and field_id in self.model.fields:
                    field_display_name = self.model.fields[field_id].display_name
                elif hasattr(self.model, 'metrics') and field_id in self.model.metrics:
                    field_display_name = self.model.metrics[field_id].display_name
            
            # 3. 在 formatted_rows 中查找对应的列并计算合计
            if formatted_rows:
                for col_key in formatted_rows[0].keys():
                    # 兼容中英文括号单位后缀
                    import re
                    clean_key = re.split(r"[（(]", col_key, maxsplit=1)[0].strip()
                    # 精确匹配：列名完全等于显示名或字段ID
                    # 避免 "总用地" 误匹配 "总用地占比"
                    if (field_display_name and clean_key == field_display_name) or clean_key == field_id:
                        # 找到列，计算合计
                        col_total = Decimal(0)
                        for r in formatted_rows:
                            value_str = str(r.get(col_key, "0")).replace(",", "").replace("-", "0").replace("**", "")
                            if value_str.strip():
                                try:
                                    col_total += Decimal(value_str)
                                except:
                                    pass
                        return col_total
            
            # 4. 尝试从 column_names 和 original_rows 计算
            if column_names and original_rows and field_display_name:
                for idx, col_name in enumerate(column_names):
                    # 精确匹配
                    if (
                        col_name == field_display_name
                        or col_name == field_id
                        # 编译器可能注入的依赖聚合隐藏列：_dep_<field_id>（用于派生指标合计计算）
                        or col_name == f"_dep_{field_id}"
                        or col_name == f"_dep_{field_id.replace('-', '_')}"
                        or col_name == f"_dep_{field_id.replace('-', '')}"
                    ):
                        col_total = Decimal(0)
                        for row in original_rows:
                            if idx < len(row) and row[idx] is not None:
                                try:
                                    col_total += Decimal(str(row[idx]))
                                except:
                                    pass
                        return col_total
            
            logger.debug(f"无法找到字段 {field_id} 的数据列")
            return None
            
        except Exception as e:
            logger.warning(f"获取字段 {field_id} 的合计值失败: {e}")
            return None

    def _parse_ratio_formula(self, formula: str) -> Tuple[str, str, Decimal]:
        """
        解析比率公式，提取分子表达式、分母表达式和乘数
        
        支持的格式：
        - {A} / {B}
        - ({A} - {B}) / {C}
        - ({A} - {B}) / {C} * 100
        - {A} / NULLIF({B}, 0) * 100
        
        Returns:
            (numerator_expr, denominator_expr, multiplier)
        """
        if not formula or ' / ' not in formula:
            return None, None, Decimal(1)
        
        # 提取乘数（如 * 100）
        multiplier = Decimal(1)
        formula_clean = formula
        
        # 检查是否有 * 100 或类似的乘数
        import re
        mult_match = re.search(r'\*\s*(\d+(?:\.\d+)?)\s*$', formula)
        if mult_match:
            multiplier = Decimal(mult_match.group(1))
            formula_clean = formula[:mult_match.start()].strip()
        
        # 按 / 分割（考虑 NULLIF 等函数中的逗号）
        # 简单策略：找到主除号位置
        div_pos = self._find_main_division(formula_clean)
        if div_pos == -1:
            return None, None, Decimal(1)
        
        numerator_expr = formula_clean[:div_pos].strip()
        denominator_expr = formula_clean[div_pos + 1:].strip()
        
        # 去掉外层括号
        numerator_expr = self._strip_outer_parens(numerator_expr)
        denominator_expr = self._strip_outer_parens(denominator_expr)
        
        # 处理 NULLIF({field}, 0) 格式
        nullif_match = re.match(r'NULLIF\s*\(\s*(.+?)\s*,\s*0\s*\)', denominator_expr, re.IGNORECASE)
        if nullif_match:
            denominator_expr = nullif_match.group(1)
        
        return numerator_expr, denominator_expr, multiplier

    def _find_main_division(self, expr: str) -> int:
        """找到表达式中主除号的位置（不在括号内的 /）"""
        depth = 0
        for i, char in enumerate(expr):
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == '/' and depth == 0:
                return i
        return -1

    def _strip_outer_parens(self, expr: str) -> str:
        """去掉表达式外层的括号"""
        expr = expr.strip()
        if expr.startswith('(') and expr.endswith(')'):
            # 检查是否是匹配的外层括号
            depth = 0
            for i, char in enumerate(expr):
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                if depth == 0 and i < len(expr) - 1:
                    # 括号在中间就闭合了，说明不是外层括号
                    return expr
            return expr[1:-1].strip()
        return expr

    def _evaluate_expression_total(self, expr: str, formatted_rows: List[Dict[str, Any]],
                                   global_rules: List[Dict] = None,
                                   original_rows: List[List[Any]] = None,
                                   column_names: List[str] = None,
                                   format_map: Dict[str, Dict[str, Any]] = None,
                                   ir: IntermediateRepresentation = None) -> Decimal:
        """
        计算表达式的合计值
        
        支持：
        - 单个字段：{field_id}
        - 加法：{A} + {B}
        - 减法：{A} - {B}
        - 混合：{A} + {B} - {C}
        
        对于每个字段，先计算其 SUM，然后按运算符组合
        """
        if not expr:
            return Decimal(0)
        
        # 解析表达式中的字段和运算符
        # 格式：{field1} + {field2} - {field3}
        import re
        
        # 提取所有字段引用和运算符
        # 匹配模式：可选的运算符 + 字段引用
        pattern = r'([+-])?\s*\{([^}]+)\}'
        matches = re.findall(pattern, expr)
        
        if not matches:
            # 尝试直接作为字段名处理
            field_total = self._find_and_calculate_field_total(
                expr.strip(), formatted_rows, global_rules,
                original_rows, column_names, format_map, ir
            )
            if field_total:
                try:
                    return Decimal(field_total.replace(",", ""))
                except:
                    pass
            return Decimal(0)
        
        total = Decimal(0)
        first_field = True
        
        for operator, field_id in matches:
            field_total = self._find_and_calculate_field_total(
                field_id, formatted_rows, global_rules,
                original_rows, column_names, format_map, ir
            )
            
            if field_total:
                try:
                    value = Decimal(field_total.replace(",", ""))
                    
                    if first_field and not operator:
                        # 第一个字段且没有运算符，直接加
                        total = value
                    elif operator == '-':
                        total -= value
                    else:  # operator == '+' 或第一个字段
                        total += value
                    
                    first_field = False
                except Exception as e:
                    logger.warning(f"解析字段 {field_id} 的值失败: {e}")
        
        return total

    def _calculate_max_total(self, column_name: str, formatted_rows: List[Dict[str, Any]]) -> str:
        """
        计算列的最大值（用于 MAX 聚合的合计）
        """
        max_value = None
        
        for row in formatted_rows:
            value_str = row.get(column_name, "")
            if value_str and value_str != "":
                try:
                    clean_value = str(value_str).replace(",", "")
                    value = Decimal(clean_value)
                    if max_value is None or value > max_value:
                        max_value = value
                except:
                    pass
        
        return self._format_decimal_value(max_value) if max_value is not None else ""

    def _calculate_min_total(self, column_name: str, formatted_rows: List[Dict[str, Any]]) -> str:
        """
        计算列的最小值（用于 MIN 聚合的合计）
        """
        min_value = None
        
        for row in formatted_rows:
            value_str = row.get(column_name, "")
            if value_str and value_str != "":
                try:
                    clean_value = str(value_str).replace(",", "")
                    value = Decimal(clean_value)
                    if min_value is None or value < min_value:
                        min_value = value
                except:
                    pass
        
        return self._format_decimal_value(min_value) if min_value is not None else ""

    def _find_and_calculate_field_total(self, field_name: str, formatted_rows: List[Dict[str, Any]],
                                      global_rules: List[Dict] = None, 
                                      original_rows: List[List[Any]] = None,
                                      column_names: List[str] = None,
                                      format_map: Dict[str, Dict[str, Any]] = None,
                                      ir: IntermediateRepresentation = None) -> str:
        """
        查找字段并计算其总计

        这个方法会尝试多种方式来查找字段：
        1. 从field_dependencies中查找field_id对应的显示名称
        2. 从语义模型中通过 field_id 查找显示名称
        3. 直接按字段名查找（支持placeholder和显示名称）
        4. 从全局规则中查找匹配的字段
        5. 模糊匹配（包括带单位后缀的列名）
        """
        def _normalize_field_token(token: str) -> str:
            """归一化字段名token，尽量消除SQL/显示层噪声。"""
            if token is None:
                return ""
            s = str(token).strip()
            # 去掉常见括号/引号
            s = s.replace("[", "").replace("]", "").replace('"', "").replace("`", "").strip()
            # 去掉表前缀
            if "." in s:
                s = s.split(".")[-1].strip()
            # 去掉单位后缀
            import re
            s = re.split(r"[（(]", s, maxsplit=1)[0].strip()
            return s

        field_name_norm = _normalize_field_token(field_name)

        # 0. 语义模型兜底：如果 field_name 是字段“显示名”，先映射回 field_id，再按 field_id 求合计
        # 目的：支持派生指标公式中出现 “成交价/土地面积/建筑面积” 这类显示名，
        #      但结果集列名可能是别名（如“总价”“出让面积”），无法直接匹配。
        #      编译器若注入隐藏依赖列（_dep_<field_id>），这里可稳定取到分子/分母合计。
        try:
            if self.model and hasattr(self.model, "fields") and self.model.fields and field_name_norm:
                for fid, f in self.model.fields.items():
                    if getattr(f, "display_name", None) == field_name_norm:
                        total_dec = self._get_field_total_by_id(
                            fid, formatted_rows, original_rows, column_names, format_map or {}, ir
                        )
                        if total_dec is not None:
                            return self._format_decimal_value(total_dec)
                        break
        except Exception:
            # 仅兜底，不影响后续常规路径
            pass

        # 1. 如果提供了原始数据和列名，尝试从field_dependencies中查找
        if original_rows and column_names and format_map and global_rules:
            # 查找field_id对应的字段
            field_id = None
            for rule in global_rules:
                if rule.get('rule_type') == 'derived_metric':
                    rule_def = rule.get('rule_definition', {})
                    field_deps = rule_def.get('field_dependencies', [])
                    for dep in field_deps:
                        if dep.get('placeholder') == field_name:
                            field_id = dep.get('field_id')
                            break
                    if field_id:
                        break
            
            # 如果找到field_id，尝试从原始数据中计算
            if field_id:
                # 查找field_id对应的列名（可能是field_id本身或显示名称）
                col_idx = None
                for idx, col_name in enumerate(column_names):
                    import re
                    base = re.split(r"[（(]", col_name, maxsplit=1)[0].strip()
                    # 既支持字段ID匹配，也支持“别名/显示名”匹配（列名可能带单位后缀）
                    if col_name == field_id or base == field_name:
                        col_idx = idx
                        break
                
                if col_idx is not None:
                    # 从原始数据中计算总计
                    col_total = Decimal(0)
                    col_has_value = False
                    for row in original_rows:
                        if col_idx < len(row):
                            value = row[col_idx]
                            if value is not None and value != "":
                                try:
                                    if isinstance(value, (int, float, Decimal)):
                                        col_total += Decimal(str(value))
                                    else:
                                        clean_value = str(value).replace(",", "")
                                        col_total += Decimal(clean_value)
                                    col_has_value = True
                                except:
                                    pass
                    
                    if col_has_value:
                        return self._format_decimal_value(col_total)

        # 1.5 没有 field_dependencies 映射时，也尽量用原始数据按“列名 base”匹配求和
        # 目的：避免显示层单位换算（如 平方米->公顷）影响派生指标合计的分子/分母统计
        if original_rows and column_names and field_name_norm:
            col_idx = None
            for idx, col_name in enumerate(column_names):
                base = _normalize_field_token(col_name)
                if base == field_name_norm:
                    col_idx = idx
                    break
            if col_idx is not None:
                col_total = Decimal(0)
                col_has_value = False
                for row in original_rows:
                    if col_idx < len(row):
                        value = row[col_idx]
                        if value is None or value == "":
                            continue
                        try:
                            if isinstance(value, (int, float, Decimal)):
                                col_total += Decimal(str(value))
                            else:
                                clean_value = str(value).replace(",", "").replace("**", "").strip()
                                if clean_value:
                                    col_total += Decimal(clean_value)
                            col_has_value = True
                        except Exception:
                            pass
                if col_has_value:
                    return self._format_decimal_value(col_total)
        
        # 2. 直接查找（支持placeholder和显示名称）
        total = self._calculate_simple_total(field_name_norm or field_name, formatted_rows)
        if total:
            return total
        
        # 2.5 从语义模型中通过 field_id 查找显示名称
        if self.model and hasattr(self.model, 'fields') and field_name in self.model.fields:
            field = self.model.fields[field_name]
            display_name = getattr(field, 'display_name', None)
            if display_name:
                # 尝试按显示名称查找
                total = self._calculate_simple_total(display_name, formatted_rows)
                if total:
                    logger.debug(f"通过语义模型找到字段: {field_name} -> {display_name}")
                    return total
                
                # 尝试带单位后缀的列名
                for col_name in formatted_rows[0].keys() if formatted_rows else []:
                    if col_name.startswith(display_name + '('):
                        total = self._calculate_simple_total(col_name, formatted_rows)
                        if total:
                            logger.debug(f"通过语义模型找到带单位列: {field_name} -> {col_name}")
                            return total
        
        # 2.6 如果语义模型中也有 measures，也尝试查找
        if self.model and hasattr(self.model, 'measures') and field_name in self.model.measures:
            measure = self.model.measures[field_name]
            display_name = getattr(measure, 'display_name', None)
            if display_name:
                total = self._calculate_simple_total(display_name, formatted_rows)
                if total:
                    logger.debug(f"通过语义模型measures找到字段: {field_name} -> {display_name}")
                    return total
                
                # 尝试带单位后缀的列名
                for col_name in formatted_rows[0].keys() if formatted_rows else []:
                    if col_name.startswith(display_name + '('):
                        total = self._calculate_simple_total(col_name, formatted_rows)
                        if total:
                            logger.debug(f"通过语义模型measures找到带单位列: {field_name} -> {col_name}")
                            return total

        # 3. 查找派生指标显示名称
        if global_rules:
            for rule in global_rules:
                if rule.get('rule_type') == 'derived_metric':
                    rule_def = rule.get('rule_definition', {})
                    display_name = rule_def.get('display_name', '')
                    metric_id = rule_def.get('metric_id', '')

                    # 检查是否匹配
                    if (field_name == display_name or
                        field_name == metric_id or
                        f"derived:{field_name}" == metric_id):

                        # 尝试按显示名称查找
                        total = self._calculate_simple_total(display_name, formatted_rows)
                        if total:
                            return total

                        # 尝试按metric_id查找
                        total = self._calculate_simple_total(metric_id, formatted_rows)
                        if total:
                            return total

        # 4. 模糊匹配（查找包含字段名的列）
        # 注意：需要避免 "宗数" 误匹配 "宗数占比"
        for column_name in formatted_rows[0].keys() if formatted_rows else []:
            clean_col = column_name.split('(')[0].strip()
            # 排除占比列，避免误匹配
            if '占比' in column_name:
                continue
            # 只有当字段名完整包含在列名中，且列名不是明显的派生列时才匹配
            if clean_col == field_name or (field_name in clean_col and len(clean_col) - len(field_name) <= 2):
                total = self._calculate_simple_total(column_name, formatted_rows)
                if total:
                    logger.debug(f"模糊匹配找到字段: {field_name} -> {column_name}")
                    return total

        logger.warning(f"无法找到字段 {field_name} 的总计值")
        return ""

    def _calculate_weighted_average_total(self, metric_name: str, formula: str, formatted_rows: List[Dict[str, Any]],
                                        global_rules: List[Dict]) -> str:
        """
        计算加权平均指标的总计值
        """
        try:
            # 这里可以实现更复杂的加权平均逻辑
            # 暂时降级为简单求和
            return self._calculate_simple_total(metric_name, formatted_rows)
        except Exception as e:
            logger.error(f"计算加权平均指标 {metric_name} 失败: {e}")
            return self._calculate_simple_total(metric_name, formatted_rows)

    def _extract_formula_dependencies(self, formula: str) -> List[str]:
        """
        从公式中提取依赖的字段名
        
        支持多种格式：
        1. {placeholder} 格式：如 {total_price}
        2. SQL表达式中的字段名：从field_dependencies中获取
        """
        dependencies = []
        
        # 1. 提取 {placeholder} 格式的依赖
        pattern = r'\{([^}]+)\}'
        placeholder_deps = re.findall(pattern, formula)
        dependencies.extend(placeholder_deps)
        
        return dependencies

    def _split_formula_dependencies(self, formula: str) -> Tuple[List[str], List[str]]:
        """
        分割公式中的分子和分母依赖项
        这是一个简化的实现，可能需要根据实际公式格式调整
        """
        dependencies = self._extract_formula_dependencies(formula)

        if ' / ' in formula:
            parts = formula.split(' / ')
            numerator_part = parts[0]
            denominator_part = parts[1] if len(parts) > 1 else ''

            numerator_deps = self._extract_formula_dependencies(numerator_part)
            denominator_deps = self._extract_formula_dependencies(denominator_part) if denominator_part else []

            return numerator_deps, denominator_deps
        else:
            # 如果没有除法，所有依赖都在分子
            return dependencies, []

    def _calculate_total_row_for_normal_table(
        self,
        formatted_rows: List[Dict[str, Any]],
        column_names: List[str],
        ir: IntermediateRepresentation,
        format_map: Dict[str, Dict[str, Any]],
        global_rules: List[Dict] = None,
        original_rows: List[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        计算普通表格的合计行

        Args:
            formatted_rows: 已格式化的数据行
            column_names: 原始列名列表
            ir: 中间表示
            format_map: 格式化映射

        Returns:
            合计行数据（字典）
        """
        if not formatted_rows:
            return None

        total_row = {}

        # 获取第一行的所有列名（带单位的显示列名）
        display_columns = list(formatted_rows[0].keys())

        # 构建原始列名到显示列名的映射
        col_name_to_display = {}
        for col_name in column_names:
            display_name = self._get_display_name_with_unit(col_name, format_map)
            col_name_to_display[col_name] = display_name

        # 标记第一列为已处理（用于显示"合计"）
        first_col_processed = False
        
        # 检查是否为跨分区查询（UNION ALL 生成的）
        is_cross_partition = getattr(ir, "cross_partition_query", False)

        # 识别年份列的关键词
        year_dimensions = ["deal_year", "reply_year", "approval_year", "year"]
        year_keywords = ["年份", "年度", "年"]
        
        # 判断列是否为年份列
        def is_year_column(col_name: str, display_col: str) -> bool:
            """判断列是否为年份列"""
            # 1. 检查列名是否在年份维度列表中
            if col_name in year_dimensions:
                return True
            
            # 2. 检查显示名是否包含年份关键词
            if any(keyword in display_col for keyword in year_keywords):
                return True
            
            # 3. 检查字段定义（如果有语义模型）
            if hasattr(self.model, 'fields') and col_name in self.model.fields:
                field = self.model.fields[col_name]
                # 检查维度类型
                if hasattr(field, 'dimension_props') and field.dimension_props:
                    if getattr(field.dimension_props, 'dimension_type', None) == 'temporal':
                        return True
                # 检查显示名
                if hasattr(field, 'display_name') and field.display_name:
                    if any(keyword in field.display_name for keyword in year_keywords):
                        return True
            
            # 4. 检查维度定义（如果有语义模型）
            if hasattr(self.model, 'dimensions') and col_name in self.model.dimensions:
                dim = self.model.dimensions[col_name]
                if hasattr(dim, 'dimension_props') and dim.dimension_props:
                    if getattr(dim.dimension_props, 'dimension_type', None) == 'temporal':
                        return True
            
            return False
        
        # 遍历每一列
        metric_field_ids = _get_metric_field_ids(ir.metrics)
        for col_idx, col_name in enumerate(column_names):
            display_col = col_name_to_display.get(col_name, col_name)

            #  隐藏内部列（以 _ 开头，如 _row_num / _dep_<field_id>）
            if col_name.startswith('_') or str(display_col).startswith('_'):
                continue
            
            # 跨分区查询：第一列是分区标识列（如"数据年份"），需要显示"合计"
            if is_cross_partition and col_idx == 0 and not first_col_processed:
                total_row[display_col] = "合计"
                first_col_processed = True
            elif (ir.dimensions and col_idx < len(ir.dimensions)) or col_name in (ir.dimensions or []):
                # 维度列：第一个显示"合计"，其余为空
                if not first_col_processed:
                    total_row[display_col] = "合计"
                    first_col_processed = True
                else:
                    total_row[display_col] = ""
            elif is_year_column(col_name, display_col):
                # 年份列：第一个显示"合计"，其余为空（年份列不应该被求和）
                if not first_col_processed:
                    total_row[display_col] = "合计"
                    first_col_processed = True
                else:
                    total_row[display_col] = ""
            else:
                # 先判断是否是派生指标（包括 global_rules 派生指标、IR calculated_fields）
                # 注意：列名可能带单位后缀，如 "每亩单价(万元/亩)"，识别逻辑会自动做 base name 归一化
                metric_info = self._identify_derived_metric_type(col_name, global_rules, ir)
                if metric_info.get('is_derived'):
                    total_value = self._calculate_derived_metric_total(
                        col_name, formatted_rows, global_rules,
                        original_rows, column_names, format_map, ir
                    )
                    total_row[display_col] = total_value
                    continue

                if col_name in metric_field_ids:
                    # 指标列（非派生）：沿用原来的合计逻辑
                    total_value = self._calculate_derived_metric_total(
                        col_name, formatted_rows, global_rules,
                        original_rows, column_names, format_map, ir
                    )
                    total_row[display_col] = total_value
                    continue

                # 检查是否是 calculated_field（LLM 动态生成的计算字段）
                is_calc_field = False
                if ir and hasattr(ir, 'calculated_fields') and ir.calculated_fields:
                    for calc_field in ir.calculated_fields:
                        alias = getattr(calc_field, 'alias', '')
                        if alias and (alias == col_name or col_name.startswith(alias + '(')):
                            is_calc_field = True
                            break
                
                # 检查是否是 ratio_metrics（占比指标）
                # 包括原始列（如"工业用地面积占比"）和同比列（如"上年工业用地面积占比"）
                is_ratio_metric = False
                ratio_metric_info = None
                if ir and hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
                    for ratio_metric in ir.ratio_metrics:
                        alias = getattr(ratio_metric, 'alias', '')
                        if alias:
                            # 检查是否匹配原始列或"上年XX"列
                            check_name = col_name
                            if col_name.startswith("上年"):
                                check_name = col_name[2:]  # 移除"上年"前缀
                            # 去掉单位后缀进行匹配
                            clean_check_name = check_name.split('(')[0].strip()
                            # 精确匹配或前缀匹配（带单位后缀的情况）
                            if alias == clean_check_name or alias == check_name or check_name.startswith(alias + '('):
                                is_ratio_metric = True
                                ratio_metric_info = ratio_metric
                                break
                
                # 检查是否是 conditional_metrics（条件聚合指标）
                # 包括原始列（如"工业用地面积"）和同比列（如"上年工业用地面积"）
                is_cond_metric = False
                cond_metric_info = None
                if ir and hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
                    for cond_metric in ir.conditional_metrics:
                        alias = getattr(cond_metric, 'alias', '')
                        if alias:
                            # 检查是否匹配原始列或"上年XX"列
                            check_name = col_name
                            if col_name.startswith("上年"):
                                check_name = col_name[2:]  # 移除"上年"前缀
                            # 去掉单位后缀进行匹配
                            clean_check_name = check_name.split('(')[0].strip()
                            # 精确匹配或前缀匹配（带单位后缀的情况）
                            if alias == clean_check_name or alias == check_name or check_name.startswith(alias + '('):
                                is_cond_metric = True
                                cond_metric_info = cond_metric
                                break
                
                if is_calc_field:
                    # 计算字段：检查是否为比率类型，使用对应的计算方法
                    total_value = self._calculate_derived_metric_total(
                        col_name, formatted_rows, global_rules,
                        original_rows, column_names, format_map, ir
                    )
                    total_row[display_col] = total_value
                elif is_ratio_metric:
                    # 占比指标：重新计算合计（分子总和 / 分母总和 * 100）
                    # 判断当前列是否是"上年"列
                    is_prev_year = col_name.startswith("上年")
                    total_value = self._calculate_ratio_metric_total(
                        ratio_metric_info, formatted_rows, original_rows, column_names, ir,
                        is_prev_year=is_prev_year
                    )
                    total_row[display_col] = total_value
                elif is_cond_metric:
                    # 条件聚合指标（如"工业用地面积"）：直接求和
                    col_total = Decimal(0)
                    col_has_value = False
                    
                    for row in formatted_rows:
                        value_str = row.get(display_col, "")
                        if value_str and value_str != "" and value_str != "--":
                            try:
                                clean_value = str(value_str).replace(",", "").replace("-", "0")
                                if clean_value.strip():
                                    col_total += Decimal(clean_value)
                                    col_has_value = True
                            except:
                                pass
                    
                    if col_has_value:
                        total_row[display_col] = self._format_decimal_value(col_total)
                        logger.debug(f"条件聚合指标 {display_col} 合计值: {col_total}")
                    else:
                        total_row[display_col] = ""
                else:
                    # 其他列（可能是度量）：也计算合计
                    col_total = Decimal(0)
                    col_has_value = False

                    for row in formatted_rows:
                        value_str = row.get(display_col, "")
                        if value_str and value_str != "":
                            try:
                                clean_value = str(value_str).replace(",", "")
                                # 尝试转换为数字
                                col_total += Decimal(clean_value)
                                col_has_value = True
                            except:
                                # 不是数字列，为空
                                pass

                    if col_has_value:
                        total_row[display_col] = self._format_decimal_value(col_total)
                    else:
                        total_row[display_col] = ""

        return total_row

    def _calculate_ratio_metric_total(
        self,
        ratio_metric,
        formatted_rows: List[Dict[str, Any]],
        original_rows: List[List[Any]],
        column_names: List[str],
        ir,
        is_prev_year: bool = False
    ) -> str:
        """
        计算占比指标的合计值（重新计算而非简单求和）
        
        策略：
        1. 先检查是否为分类分组占比（numerator_field == denominator_field 且没有 numerator_condition）
           - 如果是，合计行显示 "--"（因为各类别占比之和本就是100%，合计无意义）
        2. 对于条件占比（有 numerator_condition）：
           - 找到分母字段对应的列（如"出让面积"或"上年出让面积"）
           - 用每行的占比值和分母值反推每行的分子值
           - 合计占比 = 分子总和 / 分母总和 * 100
        
        Args:
            is_prev_year: 是否是上年列（如"上年工业用地面积占比"）
        """
        try:
            alias = getattr(ratio_metric, 'alias', '')
            numerator_field = getattr(ratio_metric, 'numerator_field', None)
            denominator_field = getattr(ratio_metric, 'denominator_field', None)
            numerator_condition = getattr(ratio_metric, 'numerator_condition', None)
            denominator_condition = getattr(ratio_metric, 'denominator_condition', None)
            as_percentage = getattr(ratio_metric, 'as_percentage', True)
            decimal_places = getattr(ratio_metric, 'decimal_places', 2)
            
            # 检测分类分组占比：分子分母字段相同且没有分子条件
            # 这种情况下，每行占比 = 该行值 / 总值 * 100，所有行加起来是100%
            # 合计行显示 "--" 更合理，因为占比的合计在语义上没有意义
            if numerator_field and denominator_field and numerator_field == denominator_field and not numerator_condition:
                logger.debug(f"占比指标 {alias} 是分类分组占比（分子=分母，无条件），合计显示 '--'")
                return "--"
            
            if not denominator_field:
                logger.warning(f"占比指标 {alias} 缺少分母字段定义")
                return "--"
            
            # 如果有分母条件，反推逻辑可能不准确，显示警告并返回 "--"
            if denominator_condition:
                logger.debug(f"占比指标 {alias} 有分母条件，合计计算复杂度较高，显示 '--'")
                return "--"
            
            # 获取分母字段的显示名（多种方式尝试）
            denominator_display = None
            
            # 方式1：从 model.metrics 获取
            if hasattr(self.model, 'metrics') and denominator_field in self.model.metrics:
                denominator_display = self.model.metrics[denominator_field].display_name
            
            # 方式2：从 model.fields 获取
            if not denominator_display and hasattr(self.model, 'fields') and denominator_field in self.model.fields:
                denominator_display = self.model.fields[denominator_field].display_name
            
            # 方式3：从 IR 的 metrics 映射获取（通过 column_names 反查）
            if not denominator_display and ir and hasattr(ir, 'metrics'):
                # IR 的 metrics 是 UUID 列表或 MetricSpec 对象，尝试从 column_names 中找到对应的列名
                # 通常 metrics 和 column_names 的顺序对应
                metric_field_ids = _get_metric_field_ids(ir.metrics)
                if denominator_field in metric_field_ids:
                    # 找到 denominator_field 在 metrics 中的索引
                    metric_idx = None
                    for idx, m in enumerate(ir.metrics):
                        m_id = m if isinstance(m, str) else (m.get("field") if isinstance(m, dict) else getattr(m, "field", None))
                        if m_id == denominator_field:
                            metric_idx = idx
                            break
                    if metric_idx is not None:
                        # 跳过维度列，找到度量列
                        dim_count = len(ir.dimensions) if ir.dimensions else 0
                        if metric_idx + dim_count < len(column_names):
                            denominator_display = column_names[metric_idx + dim_count]
            
            # 方式4：如果还是找不到，尝试从 column_names 中找包含"面积"的列
            if not denominator_display:
                for col_name in column_names:
                    # 跳过占比列本身
                    if alias and alias in col_name:
                        continue
                    # 找包含"面积"但不是"占比"的列
                    clean_col = col_name.split('(')[0].strip()
                    if '面积' in clean_col and '占比' not in clean_col and '上年' not in clean_col:
                        denominator_display = clean_col
                        break
            
            if not denominator_display:
                logger.debug(f"占比指标 {alias}：无法获取分母字段 {denominator_field} 的显示名")
                return "--"
            
            logger.debug(f"占比指标 {alias}：分母字段显示名={denominator_display}")
            
            # 在 formatted_rows 中查找分母列和占比列
            denominator_col_key = None
            ratio_col_key = None
            
            for key in formatted_rows[0].keys() if formatted_rows else []:
                clean_key = key.split('(')[0].strip()
                
                if is_prev_year:
                    # 上年列：需要匹配"上年XX"的列
                    # 找分母列（如"上年出让面积"），排除占比列
                    if key.startswith('上年') and '占比' not in key:
                        if clean_key == '上年' + denominator_display or denominator_display == clean_key.replace('上年', ''):
                            denominator_col_key = key
                    # 找占比列（如"上年工业用地面积占比"）
                    if alias and key.startswith('上年') and (alias in clean_key or alias in key):
                        ratio_col_key = key
                else:
                    # 当期列：排除"上年"开头的列
                    # 找分母列（精确匹配优先），排除占比列
                    if not key.startswith('上年') and '占比' not in key:
                        if clean_key == denominator_display:
                            denominator_col_key = key
                        # 如果没有精确匹配，尝试包含匹配（但仍排除占比列）
                        elif not denominator_col_key and denominator_display in clean_key:
                            denominator_col_key = key
                    # 找占比列
                    if alias and (alias == clean_key or alias in key) and not key.startswith('上年'):
                        ratio_col_key = key
            
            if not denominator_col_key:
                logger.debug(f"占比指标 {alias}：找不到分母列 {denominator_display}，可用列：{list(formatted_rows[0].keys()) if formatted_rows else []}")
                return "--"
            
            if not ratio_col_key:
                logger.debug(f"占比指标 {alias}：找不到占比列")
                return "--"
            
            logger.debug(f"占比指标 {alias}：分母列={denominator_col_key}，占比列={ratio_col_key}")
            
            # 计算分子总和和分母总和
            numerator_total = Decimal(0)
            denominator_total = Decimal(0)
            
            # 如果 as_percentage=True，乘数是100（百分比形式）；否则乘数是1（小数形式）
            multiplier = Decimal(100) if as_percentage else Decimal(1)
            
            for row in formatted_rows:
                try:
                    # 跳过合计行
                    first_col_value = str(list(row.values())[0]) if row else ""
                    if first_col_value == "合计" or "**合计**" in first_col_value:
                        continue
                    
                    # 获取分母值
                    denom_value_str = str(row.get(denominator_col_key, "0")).replace(",", "").replace("-", "0").replace("**", "")
                    if not denom_value_str.strip():
                        denom_value_str = "0"
                    denom_value = Decimal(denom_value_str)
                    denominator_total += denom_value
                    
                    # 获取占比值
                    ratio_value_str = str(row.get(ratio_col_key, "0")).replace(",", "").replace("-", "0").replace("%", "").replace("**", "")
                    if not ratio_value_str.strip():
                        ratio_value_str = "0"
                    ratio_value = Decimal(ratio_value_str)
                    
                    # 反推分子值：分子 = 分母 * 占比 / 乘数
                    if ratio_value > 0 and denom_value > 0:
                        numer_value = denom_value * ratio_value / multiplier
                        numerator_total += numer_value
                        
                except Exception as e:
                    logger.debug(f"计算占比行值时出错: {e}")
                    continue
            
            # 计算合计占比
            if denominator_total > 0:
                total_ratio = (numerator_total / denominator_total) * multiplier
                logger.debug(f"占比指标 {alias}：分子总和={numerator_total}，分母总和={denominator_total}，合计占比={total_ratio}，as_percentage={as_percentage}，decimal_places={decimal_places}")
                # 格式化输出，使用指定的小数位数
                return self._format_decimal_value(total_ratio, decimal_places)
            else:
                logger.debug(f"占比指标 {alias}：分母总和为0")
                return "--"
            
        except Exception as e:
            logger.warning(f"计算占比指标合计失败: {e}")
            import traceback
            logger.debug(f"详细错误: {traceback.format_exc()}")
            return "--"

    def _add_bold_style_to_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        为行数据添加加粗标记（用于合计行）

        使用 Markdown 的 ** 语法，如果前端支持会显示为加粗
        """
        bold_row = {}
        for key, value in row.items():
            if value and str(value).strip():
                # 为非空值添加加粗标记
                bold_row[key] = f"**{value}**"
            else:
                bold_row[key] = value
        return bold_row

    def _get_dimension_display_name(self, dimension_id: str) -> str:
        """获取维度的显示名称"""
        if dimension_id in self.model.dimensions:
            return self.model.dimensions[dimension_id].display_name
        return dimension_id

    def _build_format_map(self, ir: IntermediateRepresentation) -> Dict[str, Dict[str, Any]]:
        """
        构建格式化映射

        Returns:
            {
                'metric_id': {
                    'display_name': '每亩单价(万元/亩)',
                    'unit': '万元/亩',
                    'format': 'decimal',
                    'decimal_places': 2
                }
            }
        """
        format_map = {}

        # 处理指标
        for metric_item in ir.metrics:
            # 兼容字符串和 MetricSpec 格式
            metric_alias = None  # MetricSpec 中指定的别名
            metric_decimal_places = None  # MetricSpec 中指定的小数位数
            
            if isinstance(metric_item, str):
                metric_id = metric_item
            elif isinstance(metric_item, dict):
                metric_id = metric_item.get("field", str(metric_item))
                metric_alias = metric_item.get("alias")
                metric_decimal_places = metric_item.get("decimal_places")
            elif hasattr(metric_item, "field"):
                metric_id = metric_item.field
                metric_alias = getattr(metric_item, "alias", None)
                metric_decimal_places = getattr(metric_item, "decimal_places", None)
            else:
                metric_id = str(metric_item)
            
            if metric_id in self.model.metrics:
                metric = self.model.metrics[metric_id]
                unit_conversion = getattr(metric, 'unit_conversion', None)
                original_unit = getattr(metric, 'unit', None)

                #  应用单位转换配置
                display_unit = original_unit
                if unit_conversion and unit_conversion.get('enabled'):
                    display_unit = unit_conversion.get('display_unit', original_unit)

                # 优先使用 MetricSpec 中指定的小数位数
                decimal_places = metric_decimal_places if metric_decimal_places is not None else (
                    getattr(metric, 'decimal_places', None) or 2
                )

                info = {
                    'display_name': metric_alias or metric.display_name,
                    'unit': display_unit,  # 使用转换后的单位
                    'original_unit': original_unit,
                    'unit_conversion': unit_conversion,  #  保存转换配置
                    'format': getattr(metric, 'format', None) or 'number',
                    'decimal_places': decimal_places
                }
                # 同时支持以显示名作为键（SQL 已可能使用显示名作为别名）
                format_map[metric_id] = info
                if metric.display_name:
                    format_map[metric.display_name] = info
                # 支持 MetricSpec 中的自定义别名作为键
                if metric_alias and metric_alias != metric.display_name:
                    format_map[metric_alias] = info
            elif metric_id in getattr(self.model, 'measures', {}):
                # 容错：当 IR.metrics 实际是度量ID时，使用度量的显示名/单位
                measure = self.model.measures[metric_id]
                unit_conversion = getattr(measure, 'unit_conversion', None)
                original_unit = getattr(measure, 'unit', None)

                #  应用单位转换配置
                display_unit = original_unit
                if unit_conversion and unit_conversion.get('enabled'):
                    display_unit = unit_conversion.get('display_unit', original_unit)

                # 优先使用 MetricSpec 中指定的小数位数
                decimal_places = metric_decimal_places if metric_decimal_places is not None else 2

                info = {
                    'display_name': metric_alias or getattr(measure, 'display_name', metric_id),
                    'unit': display_unit,  # 使用转换后的单位
                    'original_unit': original_unit,
                    'unit_conversion': unit_conversion,  #  保存转换配置
                    'format': 'decimal',
                    'decimal_places': decimal_places
                }
                format_map[metric_id] = info
                dn = getattr(measure, 'display_name', None)
                if dn:
                    format_map[dn] = info
                # 支持 MetricSpec 中的自定义别名作为键
                if metric_alias and metric_alias != dn:
                    format_map[metric_alias] = info

        # 处理维度（包括分组维度）
        for dim_id in ir.dimensions:
            if dim_id in self.model.dimensions:
                dim = self.model.dimensions[dim_id]
                dim_format_info = {
                    'display_name': dim.display_name,
                    'unit': None,
                    'format': 'string',
                    'decimal_places': None
                }
                # 添加两个映射：dim_id 和 dim_id_label（兼容SQL别名）
                format_map[dim_id] = dim_format_info
                format_map[f"{dim_id}_label"] = dim_format_info

        #  处理标注字段：来自filters的维度（不在dimensions中）
        if ir.filters:
            for filter_cond in ir.filters:
                dim_id = filter_cond.field
                # 如果是维度字段且不在dimensions中，添加为标注字段映射
                if dim_id not in ir.dimensions and dim_id in self.model.dimensions:
                    dim = self.model.dimensions[dim_id]
                    dim_format_info = {
                        'display_name': dim.display_name,
                        'unit': None,
                        'format': 'string',
                        'decimal_places': None
                    }
                    # 只添加 dim_id_label 映射（标注字段使用这个别名）
                    format_map[f"{dim_id}_label"] = dim_format_info
                    format_map[dim_id] = dim_format_info  # 也添加不带后缀的版本以防万一

        #  处理从统一字段表（fields）读取的度量字段
        if hasattr(self.model, 'fields') and self.model.fields:
            for metric_item in ir.metrics:
                # 兼容字符串和 MetricSpec 格式
                metric_alias = None
                metric_decimal_places = None
                
                if isinstance(metric_item, str):
                    metric_id = metric_item
                elif isinstance(metric_item, dict):
                    metric_id = metric_item.get("field", str(metric_item))
                    metric_alias = metric_item.get("alias")
                    metric_decimal_places = metric_item.get("decimal_places")
                elif hasattr(metric_item, "field"):
                    metric_id = metric_item.field
                    metric_alias = getattr(metric_item, "alias", None)
                    metric_decimal_places = getattr(metric_item, "decimal_places", None)
                else:
                    metric_id = str(metric_item)
                
                if metric_id in self.model.fields:
                    field = self.model.fields[metric_id]
                    if field.field_category == 'measure' and field.measure_props:
                        unit_conversion = field.unit_conversion
                        original_unit = field.measure_props.unit

                        # 应用单位转换配置
                        display_unit = original_unit
                        if unit_conversion and unit_conversion.get('enabled'):
                            display_unit = unit_conversion.get('display_unit', original_unit)

                        # 优先使用 MetricSpec 中指定的小数位数
                        decimal_places = metric_decimal_places if metric_decimal_places is not None else (
                            field.measure_props.decimal_places or 2
                        )

                        info = {
                            'display_name': metric_alias or field.display_name,
                            'unit': display_unit,
                            'original_unit': original_unit,
                            'unit_conversion': unit_conversion,
                            'format': 'decimal',
                            'decimal_places': decimal_places
                        }
                        format_map[metric_id] = info
                        if field.display_name:
                            format_map[field.display_name] = info
                        # 支持 MetricSpec 中的自定义别名作为键
                        if metric_alias and metric_alias != field.display_name:
                            format_map[metric_alias] = info

        # 处理度量（用于排序字段）
        if ir.order_by:
            for order_item in ir.order_by:
                field = order_item.field
                if field in self.model.measures:
                    measure = self.model.measures[field]
                    unit_conversion = getattr(measure, 'unit_conversion', None)
                    original_unit = getattr(measure, 'unit', None)

                    # 应用单位转换配置
                    display_unit = original_unit
                    if unit_conversion and unit_conversion.get('enabled'):
                        display_unit = unit_conversion.get('display_unit', original_unit)

                    format_map[field] = {
                        'display_name': measure.display_name,
                        'unit': display_unit,
                        'original_unit': original_unit,
                        'unit_conversion': unit_conversion,
                        'format': 'decimal',
                        'decimal_places': 2
                    }

        # 处理 ratio_metrics 自动生成的分子列
        # 编译器会为每个有 numerator_condition 的 ratio_metric 自动生成分子列
        # 这里需要为这些列添加格式化信息
        if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
            for ratio_metric in ir.ratio_metrics:
                # 只处理有 numerator_condition 的 ratio_metric
                if not ratio_metric.numerator_condition:
                    continue
                
                # 获取分子字段的单位信息
                numerator_field = ratio_metric.numerator_field
                unit = None
                decimal_places = 2
                
                if numerator_field != "__row_count__":
                    if hasattr(self.model, 'fields') and numerator_field in self.model.fields:
                        field = self.model.fields[numerator_field]
                        if field.field_category == 'measure' and hasattr(field, 'measure_props') and field.measure_props:
                            unit = field.measure_props.unit
                            decimal_places = field.measure_props.decimal_places or 2
                            # 检查单位转换配置
                            unit_conversion = getattr(field, 'unit_conversion', None)
                            if unit_conversion and isinstance(unit_conversion, dict) and unit_conversion.get('enabled'):
                                unit = unit_conversion.get('display_unit', unit)
                    elif numerator_field in getattr(self.model, 'measures', {}):
                        measure = self.model.measures[numerator_field]
                        unit = getattr(measure, 'unit', None)
                
                # 生成分子列的别名（与 ast_builder.py 中的逻辑保持一致）
                alias = ratio_metric.alias
                if alias and alias.endswith("占比"):
                    numerator_alias = alias[:-2]  # 去掉"占比"后缀
                elif alias and alias.endswith("比例"):
                    numerator_alias = alias[:-2]  # 去掉"比例"后缀
                else:
                    cond_value = ratio_metric.numerator_condition.value
                    cond_value_str = str(cond_value) if cond_value else "分子"
                    numerator_alias = f"{cond_value_str}面积"
                
                # 如果有单位，别名可能包含单位后缀
                numerator_alias_with_unit = numerator_alias
                if unit and f"({unit})" not in numerator_alias:
                    numerator_alias_with_unit = f"{numerator_alias}({unit})"
                
                # 添加格式化信息（同时添加带单位和不带单位的版本）
                numerator_info = {
                    'display_name': numerator_alias_with_unit,
                    'unit': unit,
                    'format': 'decimal',
                    'decimal_places': decimal_places
                }
                format_map[numerator_alias] = numerator_info
                if numerator_alias_with_unit != numerator_alias:
                    format_map[numerator_alias_with_unit] = numerator_info
                
                logger.debug(f"添加占比分子列格式化信息: {numerator_alias_with_unit}, 单位: {unit}")

        return format_map

    def _get_display_name_with_unit(
        self,
        col_name: str,
        format_map: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        获取带单位的显示名称

        Args:
            col_name: 原始列名（如 'deal_count', 'district'）
            format_map: 格式化映射

        Returns:
            显示名称（如 '成交宗数', '行政区', '每亩单价(万元/亩)'）
        """
        if col_name not in format_map:
            return col_name

        info = format_map[col_name]
        display_name = info['display_name']
        unit = info['unit']

        # 如果显示名称已经包含单位（如"每亩单价(万元/亩)" 或 "每亩单价（万元/亩）"），直接返回
        if unit and (f"({unit})" in display_name or f"（{unit}）" in display_name):
            return display_name

        # 否则，如果有单位，则添加
        if unit:
            return f"{display_name}({unit})"

        return display_name

    def _format_value(
        self,
        value: Any,
        col_name: str,
        format_map: Dict[str, Dict[str, Any]]
    ) -> Any:
        """
        格式化单个值

        Args:
            value: 原始值
            col_name: 列名
            format_map: 格式化映射

        Returns:
            格式化后的值
        """
        return _ensure_json_serializable(_format_value_global(value, col_name, format_map))


#  默认单位映射配置（作为后备，优先使用配置文件）
# 已迁移到数据库 tenant_config.formatting
# 保留此处是为了向后兼容（如果配置文件不存在时使用）
_DEFAULT_FIELD_UNIT_MAP = {
    # 面积类
    '出让面积': '平方米',
    '总用地': '公顷',
    '计容用地面积': '平方米',
    '地下出让面积': '平方米',
    '地下建筑面积': '平方米',
    '地上出让面积': '平方米',
    '建筑面积_上限': '平方米',
    '建筑面积_下限': '平方米',
    '建筑面积': '平方米',

    # 价格类
    '总价': '万元',
    '每亩单价': '万元/亩',
    '楼面地价': '元/平方米',
    '基准地价': '元/平方米',
    '竞买保证金': '万元',
    '起始价': '万元',
    '出让最高价': '万元',

    # 空间数据
    'Shape.STArea': '平方米',
}

#  默认显示名映射（作为后备，优先使用配置文件）
# 已迁移到数据库 tenant_config.formatting
def add_units_to_detail_columns(columns: List[str], semantic_model=None) -> List[str]:
    """
    为明细查询的列名添加单位

    Args:
        columns: 原始列名列表（物理列名）
        semantic_model: 语义模型（可选），用于从配置读取单位映射

    Returns:
        带单位的列名列表
    """
    #  使用统一的字段映射工具
    from server.utils.field_display import build_column_display_map
    
    #  从配置读取单位映射
    field_unit_map = _DEFAULT_FIELD_UNIT_MAP.copy()
    display_name_map = build_column_display_map(semantic_model)

    # 从语义模型的 fields 表中构建单位映射
    if semantic_model and hasattr(semantic_model, 'fields') and semantic_model.fields:
        import structlog
        logger = structlog.get_logger()
        logger.debug(f"开始构建字段单位映射，共{len(semantic_model.fields)}个字段")
        
        for field_id, field in semantic_model.fields.items():
            # 获取物理列名
            physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
            
            # 如果是度量字段，添加单位映射
            if field.field_category == 'measure' and hasattr(field, 'measure_props') and field.measure_props:
                # 获取单位（考虑单位转换配置）
                unit = field.measure_props.unit
                unit_conversion = getattr(field, 'unit_conversion', None)
                if unit_conversion and unit_conversion.get('enabled'):
                    unit = unit_conversion.get('display_unit', unit)
                
                if unit:
                    # 同时为物理列名和显示名添加单位映射
                    if physical_col:
                        field_unit_map[physical_col] = unit
                    if field.display_name and field.display_name != physical_col:
                        field_unit_map[field.display_name] = unit
        
        logger.debug(f"单位映射完成，共映射 {len(field_unit_map)} 个单位")

    # 从 formatting 配置中读取额外的映射（覆盖优先级更高）
    if semantic_model and hasattr(semantic_model, 'formatting') and semantic_model.formatting:
        if semantic_model.formatting.field_units:
            field_unit_map.update(semantic_model.formatting.field_units)

    result = []
    for col in columns:
        #  隐藏内部列（以 _ 开头，如 _row_num）
        if col.startswith('_'):
            continue

        # 先做显示名映射（如 GZQH_GLQMC → 行政区，或物理列名 → display_name）
        display_col = display_name_map.get(col, col)
        
        # 再附加单位（需要同时检查原列名和显示名）
        # 因为SQL可能已经使用了display_name作为别名
        unit = field_unit_map.get(col) or field_unit_map.get(display_col)
        if unit:
            # 如果已经包含单位，不重复添加
            if f"({unit})" not in display_col:
                result.append(f"{display_col}({unit})")
            else:
                result.append(display_col)
        else:
            result.append(display_col)
    
    return result


def format_detail_rows(rows: List[List[Any]], column_names: List[str], semantic_model=None) -> List[Dict[str, Any]]:
    """
    批量格式化明细查询的多行数据（包括单位转换）

    优化版本：只构建一次格式化映射，避免重复计算

    Args:
        rows: 原始行数据列表
        column_names: 列名列表
        semantic_model: 语义模型（可选），用于从配置读取单位映射和单位转换

    Returns:
        格式化后的行数据列表
    """
    if not rows:
        return []

    # 只构建一次格式化映射
    field_unit_map = _DEFAULT_FIELD_UNIT_MAP.copy()
    field_conversion_map = {}

    if semantic_model:
        # 处理字段单位映射
        if hasattr(semantic_model, 'formatting') and semantic_model.formatting:
            if semantic_model.formatting.field_units:
                field_unit_map.update(semantic_model.formatting.field_units)

        # 处理字段单位及单位转换映射
        if hasattr(semantic_model, 'fields') and semantic_model.fields:
            for field_id, field in semantic_model.fields.items():
                display_name = getattr(field, 'display_name', None)
                physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or getattr(field, 'field_name', None)

                # 先根据度量配置补充单位映射，确保后续能够识别为数值列
                if getattr(field, 'field_category', None) == 'measure' and getattr(field, 'measure_props', None):
                    unit = field.measure_props.unit
                    unit_conv_cfg = getattr(field, 'unit_conversion', None)
                    parsed_unit_conv = None

                    if unit_conv_cfg:
                        parsed_unit_conv = unit_conv_cfg
                        if isinstance(unit_conv_cfg, str):
                            try:
                                import json
                                parsed_unit_conv = json.loads(unit_conv_cfg)
                            except Exception:
                                parsed_unit_conv = None

                        if isinstance(parsed_unit_conv, dict) and parsed_unit_conv.get('enabled'):
                            unit = parsed_unit_conv.get('display_unit') or parsed_unit_conv.get('to_unit') or unit

                    if unit:
                        if physical_col:
                            field_unit_map[physical_col] = unit
                        if display_name and display_name != physical_col:
                            field_unit_map[display_name] = unit
                else:
                    parsed_unit_conv = None

                # 处理单位转换映射（包括非度量字段的配置）
                unit_conv_source = getattr(field, 'unit_conversion', None)
                if unit_conv_source:
                    unit_conv = unit_conv_source
                    if isinstance(unit_conv, str):
                        try:
                            import json
                            unit_conv = json.loads(unit_conv)
                        except:
                            continue

                    if isinstance(unit_conv, dict) and unit_conv.get('enabled'):
                        if display_name:
                            field_conversion_map[display_name] = unit_conv
                        if physical_col:
                            field_conversion_map[physical_col] = unit_conv

    # 构建格式化映射（只计算一次）
    format_map = {}
    logger.debug(f"开始构建format_map，共{len(column_names)}个列", column_names=column_names[:10])
    
    for col_name in column_names:
        #  隐藏内部列（以 _ 开头，如 _row_num）
        if col_name.startswith('_'):
            continue

        # 提取原始列名（去掉单位部分）
        original_col = col_name
        if '(' in col_name:
            original_col = col_name.split('(')[0]

        # 获取显示名和单位
        display_name = original_col
        unit = field_unit_map.get(original_col, "")

        # 处理单位转换配置
        unit_conversion = field_conversion_map.get(display_name) or field_conversion_map.get(original_col)
        decimal_places = 2  # 默认保留2位小数
        original_unit = unit  # 保存原始单位

        if unit_conversion and isinstance(unit_conversion, dict):
            # 如果是JSON字符串，解析它
            if isinstance(unit_conversion, str):
                try:
                    import json
                    unit_conversion = json.loads(unit_conversion)
                except:
                    unit_conversion = None
            
            if unit_conversion and isinstance(unit_conversion, dict):
                # 获取小数位数（无论是否启用）
                if 'decimal_places' in unit_conversion:
                    decimal_places = unit_conversion.get('decimal_places', 2)
                
                # 只有在启用时才使用转换后的单位
                if unit_conversion.get('enabled'):
                    target_unit = unit_conversion.get('to_unit') or unit_conversion.get('display_unit')
                    if target_unit:
                        unit = target_unit
                    
                    logger.debug(
                        "构建format_map时发现单位转换配置（已启用）",
                        col_name=col_name,
                        original_col=original_col,
                        original_unit=original_unit,
                        target_unit=unit,
                        decimal_places=decimal_places
                    )
                else:
                    logger.debug(
                        "构建format_map时发现单位转换配置（未启用）",
                        col_name=col_name,
                        original_unit=original_unit
                    )

        # 判断格式类型：如果有单位，通常是数值类型；否则可能是字符串
        format_type = 'number' if unit else 'string'
        
        format_map[col_name] = {
            'display_name': display_name,
            'unit': unit,  # 使用转换后的单位
            'original_unit': original_unit,  # 保存原始单位，用于单位转换计算
            'format': format_type,  # 添加format键，避免KeyError
            'decimal_places': decimal_places,
            'unit_conversion': unit_conversion  # 保存单位转换配置
        }
        
        # 仅在调试模式下记录详细日志（避免日志过多）
        # logger.debug(
        #     "构建format_map条目",
        #     col_name=col_name,
        #     display_name=display_name,
        #     unit=unit,
        #     format_type=format_type,
        #     decimal_places=decimal_places
        # )

    # 批量格式化所有行
    formatted_rows = []
    for row_idx, row in enumerate(rows):
        row_dict = {col_name: value for col_name, value in zip(column_names, row)}
        formatted_row = {}

        for col_name, value in row_dict.items():
            #  隐藏内部列（以 _ 开头，如 _row_num）
            if col_name.startswith('_'):
                continue

            # 格式化列名（注意：column_names已经包含单位，直接使用作为键）
            # 这样可以确保返回的字典键与 new_column_names 一致
            display_name = col_name
            
            # 格式化数值（使用原始列名进行格式化）
            try:
                formatted_value = _format_value_with_conversion(
                    value, col_name, format_map, field_conversion_map
                )
            except KeyError as e:
                logger.error(
                    "格式化数值时发生KeyError",
                    error=str(e),
                    row_idx=row_idx,
                    col_name=col_name,
                    value=value,
                    format_map_keys=list(format_map.keys())[:10],
                    format_map_entry=format_map.get(col_name),
                    exc_info=True
                )
                # 容错：返回原值
                formatted_value = value
            except Exception as e:
                logger.error(
                    "格式化数值时发生异常",
                    error=str(e),
                    row_idx=row_idx,
                    col_name=col_name,
                    value=value,
                    exc_info=True
                )
                # 容错：返回原值
                formatted_value = value

            formatted_row[display_name] = formatted_value

        formatted_rows.append(formatted_row)
    
    # 验证返回的键名是否与输入的列名一致
    if formatted_rows:
        returned_keys = list(formatted_rows[0].keys())
        if returned_keys != column_names:
            logger.warning(
                "format_detail_rows返回的键名与输入列名不一致",
                input_column_names=column_names,
                returned_keys=returned_keys,
                mismatch_keys=[k for k in column_names if k not in returned_keys]
            )

    return formatted_rows


def _format_value_with_conversion(value, col_name: str, format_map: Dict, field_conversion_map: Dict) -> Any:
    """
    格式化单个数值（包括单位转换）
    """
    if value is None:
        return None

    # 获取格式化信息
    fmt_info = format_map.get(col_name, {})
    
    # 添加日志，帮助定位问题
    if not fmt_info:
        logger.warning(
            "format_map中找不到列名",
            col_name=col_name,
            format_map_keys=list(format_map.keys())[:10]
        )
        return value
    
    # 优先使用 format_map 中的 unit_conversion（已经正确构建）
    unit_conversion = fmt_info.get('unit_conversion')
    
    # 如果 format_map 中没有，尝试从 field_conversion_map 获取（向后兼容）
    if not unit_conversion or not isinstance(unit_conversion, dict):
        original_col = col_name.split('(')[0] if '(' in col_name else col_name
        unit_conversion = field_conversion_map.get(fmt_info.get('display_name')) or field_conversion_map.get(original_col)

    # 如果没有单位转换配置，使用常规格式化（_format_value_global 也会处理 unit_conversion）
    if not unit_conversion or not isinstance(unit_conversion, dict):
        return _ensure_json_serializable(_format_value_global(value, col_name, format_map))

    # 检查是否启用单位转换
    if not unit_conversion.get('enabled'):
        return _ensure_json_serializable(_format_value_global(value, col_name, format_map))

    try:
        # 使用 apply_unit_conversion 函数处理单位转换（保持一致性）
        original_unit = fmt_info.get('original_unit')
        converted_value, display_unit = apply_unit_conversion(value, unit_conversion, original_unit)
        
        logger.debug(
            "应用单位转换",
            col_name=col_name,
            original_value=value,
            converted_value=converted_value,
            original_unit=original_unit,
            display_unit=display_unit,
            unit_conversion_enabled=unit_conversion.get('enabled')
        )
        
        # 转换后，使用常规格式化逻辑格式化数值
        # 注意：apply_unit_conversion 已经处理了精度，但我们需要确保格式一致
        decimal_places = fmt_info.get('decimal_places', 2)
        
        # 如果转换后的值已经是数值，使用 Decimal 进行格式化
        from decimal import Decimal, ROUND_HALF_UP
        if isinstance(converted_value, (int, float, Decimal)):
            numeric_value = Decimal(str(converted_value))
            quantize_str = '0.' + '0' * decimal_places
            formatted = numeric_value.quantize(
                Decimal(quantize_str),
                rounding=ROUND_HALF_UP
            )
            return format(formatted, f'.{decimal_places}f')
        else:
            # 如果转换失败或返回了非数值，返回原值（确保可序列化）
            return _ensure_json_serializable(converted_value)

    except Exception as e:
        # 转换失败，使用常规格式化
        logger.warning(
            "单位转换失败，使用常规格式化",
            error=str(e),
            value=value,
            col_name=col_name,
            unit_conversion=unit_conversion,
            exc_info=True
        )

    # Fallback到常规格式化
    return _ensure_json_serializable(_format_value_global(value, col_name, format_map))


def format_detail_row(row: Dict[str, Any], semantic_model=None) -> Dict[str, Any]:
    """
    格式化明细查询的单行数据（包括单位转换）

    Args:
        row: 原始行数据
        semantic_model: 语义模型（可选），用于从配置读取单位映射和单位转换

    Returns:
        格式化后的行数据（应用单位转换并保留两位小数）
    """
    #  从配置读取单位映射
    field_unit_map = _DEFAULT_FIELD_UNIT_MAP.copy()

    if semantic_model and hasattr(semantic_model, 'formatting') and semantic_model.formatting:
        if semantic_model.formatting.field_units:
            field_unit_map.update(semantic_model.formatting.field_units)

    # 构建字段名到 unit_conversion 配置的映射
    field_conversion_map = {}
    if semantic_model and hasattr(semantic_model, 'fields') and semantic_model.fields:
        for field_id, field in semantic_model.fields.items():
            # 获取字段的显示名
            display_name = field.display_name if hasattr(field, 'display_name') else None
            
            # 获取单位转换配置
            if hasattr(field, 'unit_conversion') and field.unit_conversion:
                unit_conv = field.unit_conversion
                # 如果是JSON字符串，解析它
                if isinstance(unit_conv, str):
                    try:
                        import json
                        unit_conv = json.loads(unit_conv)
                    except:
                        continue
                
                # 如果是字典且已启用，添加到映射
                if isinstance(unit_conv, dict) and unit_conv.get('enabled'):
                    if display_name:
                        field_conversion_map[display_name] = unit_conv
                    # 同时为物理列名添加映射
                    physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None)
                    if physical_col:
                        field_conversion_map[physical_col] = unit_conv

    formatted = {}
    for col, value in row.items():
        #  隐藏内部列（以 _ 开头，如 _row_num）
        if col.startswith('_'):
            continue

        # 提取原始列名（去掉单位部分）
        if '(' in col:
            original_col = col.split('(')[0]
        else:
            original_col = col

        # 首先应用单位转换（如果有配置）
        converted_value = value
        display_unit = None
        if original_col in field_conversion_map and value is not None:
            unit_config = field_conversion_map[original_col]
            # 获取原始单位
            original_unit = unit_config.get('from_unit') or unit_config.get('original_unit')
            # 应用单位转换
            converted_value, display_unit = apply_unit_conversion(
                value, 
                unit_config, 
                original_unit
            )
            logger.debug(f"明细查询应用单位转换: {col} {value} -> {converted_value} ({display_unit})")

        # 然后格式化数值
        if original_col in field_unit_map and converted_value is not None:
            try:
                numeric_value = Decimal(str(converted_value))
                formatted_value = numeric_value.quantize(
                    Decimal('0.01'),
                    rounding=ROUND_HALF_UP
                )
                # 返回字符串，确保保留末尾的 .00
                formatted[col] = format(formatted_value, '.2f')
            except:
                formatted[col] = converted_value
        else:
            formatted[col] = converted_value

    return formatted

