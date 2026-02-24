"""
可视化建议
"""

from typing import Dict, Any, List, Optional

from server.models.ir import IntermediateRepresentation


def suggest_visualization(
    ir: IntermediateRepresentation,
    columns: List[Dict[str, Any]],
    rows: List[List[Any]]
) -> Optional[str]:
    """
    使用简单启发式为聚合查询推荐可视化类型。
    
    逻辑：
    - 仅在聚合查询且存在维度与数值列时给出建议
    - 时间维度优先推荐折线图
    - 单指标且类别数较少尝试饼图
    - 其他情况使用柱状图
    """
    if (
        not ir
        or ir.query_type != "aggregation"
        or not ir.dimensions
        or not columns
        or not rows
    ):
        return None

    # 过滤掉合计等汇总行，避免影响类别统计
    data_rows = [
        row for row in rows
        if row and not (isinstance(row[0], str) and row[0] == "合计")
    ]
    if not data_rows:
        return None

    # 判断哪些列包含数值
    numeric_indices: List[int] = []
    for idx in range(len(columns)):
        for row in data_rows:
            if idx < len(row) and isinstance(row[idx], (int, float)):
                numeric_indices.append(idx)
                break
    if not numeric_indices:
        return None

    # 根据首个维度列名判断是否为时间序列
    dim_col_name = columns[0].get("name", "") if columns else ""
    time_keywords = ("年", "月", "日", "日期", "时间", "周", "季度")
    is_temporal_dimension = any(keyword in dim_col_name for keyword in time_keywords)
    if is_temporal_dimension and len(data_rows) >= 2:
        return "line"

    # 小类别、单指标 → 尝试饼图
    if len(data_rows) <= 6 and len(ir.metrics) == 1:
        numeric_idx = numeric_indices[0]
        numeric_values = [
            float(row[numeric_idx])
            for row in data_rows
            if numeric_idx < len(row) and isinstance(row[numeric_idx], (int, float))
        ]
        if numeric_values and sum(numeric_values) > 0:
            return "pie"

    # 默认推荐柱状图
    return "bar"

