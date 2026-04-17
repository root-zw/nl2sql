"""
统一确认阶段辅助工具。

负责：
- 把“原问题 + 修改意见”拼成新的解析输入
- 生成 draft_confirmation 阶段给用户看的安全摘要
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def get_revision_text(revision_request: Optional[Dict[str, Any]]) -> str:
    if not revision_request:
        return ""
    for key in ("text", "source_text", "natural_language_reply", "question"):
        value = revision_request.get(key)
        if value:
            return str(value).strip()
    return ""


def compose_question_with_revision(
    question_text: Optional[str],
    revision_request: Optional[Dict[str, Any]],
) -> str:
    base_text = (question_text or "").strip()
    revision_text = get_revision_text(revision_request)

    if not revision_text:
        return base_text
    if not base_text:
        return revision_text

    return f"{base_text}\n\n补充修改要求：{revision_text}"


def describe_time_range(time_info: Optional[Dict[str, Any]]) -> str:
    if not time_info:
        return ""

    range_type = time_info.get("type")
    if range_type == "absolute":
        start = time_info.get("start_date")
        end = time_info.get("end_date")
        if start and end:
            return f"{start} 至 {end}"
        return start or end or ""

    if range_type == "relative":
        last_n = time_info.get("last_n")
        unit = time_info.get("unit")
        unit_map = {
            "day": "天",
            "week": "周",
            "month": "个月",
            "quarter": "个季度",
            "year": "年",
        }
        if last_n and unit:
            return f"最近{last_n}{unit_map.get(unit, unit)}"
        return ""

    if range_type == "rolling":
        grain = time_info.get("grain")
        offset = int(time_info.get("offset") or 0)
        grain_map = {
            "week": "周",
            "month": "月",
            "quarter": "季度",
            "year": "年",
        }
        grain_text = grain_map.get(grain, grain or "")
        if not grain_text:
            return ""
        if offset == 0:
            return f"本{grain_text}"
        if offset == -1:
            return f"上{grain_text}"
        if offset > 0:
            return f"{offset}个{grain_text}后"
        return f"{abs(offset)}个{grain_text}前"

    return ""


def build_draft_confirmation_summary(
    ir_display: Dict[str, Any],
    *,
    selected_table_names: Optional[List[str]] = None,
    revision_request: Optional[Dict[str, Any]] = None,
) -> str:
    table_names = [name for name in (selected_table_names or []) if name]
    metrics = [item for item in (ir_display.get("metrics") or []) if item]
    dimensions = [item for item in (ir_display.get("dimensions") or []) if item]
    filters = ir_display.get("filters") or []
    order_by = ir_display.get("order_by") or []
    limit = ir_display.get("limit")
    time_text = describe_time_range(ir_display.get("time"))
    revision_text = get_revision_text(revision_request)

    parts: List[str] = []
    if table_names:
        parts.append(f"我要基于【{'、'.join(table_names)}】进行查询")
    else:
        parts.append("我要继续按当前已选语义草稿生成查询")

    if metrics:
        parts.append(f"统计【{'、'.join(metrics)}】")
    elif dimensions:
        parts.append(f"返回【{'、'.join(dimensions)}】")

    if dimensions and metrics:
        parts.append(f"按【{'、'.join(dimensions)}】展开")

    filter_parts: List[str] = []
    for condition in filters:
        field = condition.get("field")
        op = condition.get("op")
        value = condition.get("value")
        if field and op:
            filter_parts.append(f"【{field}】{op}{repr(value)}")
    if filter_parts:
        parts.append(f"筛选条件为 {'、'.join(filter_parts)}")

    if time_text:
        parts.append(f"时间范围为【{time_text}】")

    if order_by:
        order_parts = []
        for item in order_by:
            field = item.get("field")
            if field:
                order_parts.append(f"【{field}】{'降序' if item.get('desc') else '升序'}")
        if order_parts:
            parts.append(f"排序方式为 {'、'.join(order_parts)}")

    if limit:
        parts.append(f"结果条数限制为 {limit}")

    if revision_text:
        parts.append(f"本轮已吸收您的修改意见：{revision_text}")

    return "；".join(parts) + "。请确认是否继续。"
