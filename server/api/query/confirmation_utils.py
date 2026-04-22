"""
统一确认阶段辅助工具。

负责：
- 把“原问题 + 修改意见”拼成新的解析输入
- 生成 draft_confirmation 阶段给用户看的安全摘要
- 统一 adaptive / always_confirm 的暂停判定
- 把结果后追问上下文结构化注入 IR
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from server.models.ir import IntermediateRepresentation

ALLOWED_CONFIRMATION_MODES = {"adaptive", "always_confirm"}


def _is_probably_uuid(value: Optional[str]) -> bool:
    if not value:
        return False
    try:
        UUID(str(value))
        return True
    except (TypeError, ValueError):
        return False


def resolve_confirmation_mode(
    request_mode: Optional[str],
    existing_mode: Optional[str],
    default_mode: Optional[str],
) -> str:
    for candidate in (request_mode, existing_mode, default_mode, "always_confirm"):
        if candidate in ALLOWED_CONFIRMATION_MODES:
            return str(candidate)
    return "always_confirm"


def get_revision_text(revision_request: Optional[Dict[str, Any]]) -> str:
    if not revision_request:
        return ""
    for key in ("text", "source_text", "natural_language_reply", "question"):
        value = revision_request.get(key)
        if value:
            return str(value).strip()
    return ""


def _sanitize_table_names(table_names: Optional[List[Any]]) -> List[str]:
    normalized_names: List[str] = []
    seen_names: set[str] = set()
    for item in table_names or []:
        name = str(item).strip()
        if not name or _is_probably_uuid(name) or name in seen_names:
            continue
        seen_names.add(name)
        normalized_names.append(name)
    return normalized_names


def _sanitize_summary_items(items: Optional[List[Any]]) -> List[str]:
    normalized_items: List[str] = []
    for item in items or []:
        text = str(item).strip()
        if not text or _is_probably_uuid(text):
            continue
        for prefix in ("当前数据表：", "当前涉及数据表："):
            if text.startswith(prefix):
                table_names = _sanitize_table_names(text[len(prefix):].split("、"))
                if not table_names:
                    text = ""
                else:
                    text = f"{prefix}{'、'.join(table_names)}"
                break
        if text:
            normalized_items.append(text)
    return normalized_items


def _format_filter_value(value: Any) -> str:
    if isinstance(value, list):
        return "、".join(str(item) for item in value)
    if isinstance(value, tuple):
        return "、".join(str(item) for item in value)
    if isinstance(value, str):
        return value
    return str(value)


def _format_filter_condition(condition: Dict[str, Any]) -> str:
    field = condition.get("field")
    op = condition.get("op")
    value = condition.get("value")
    if not field or not op:
        return ""
    if op in {"IS NULL", "IS NOT NULL"}:
        return f"【{field}】{op}"
    return f"【{field}】{op}{_format_filter_value(value)}"


def _extract_named_items(items: Optional[List[Dict[str, Any]]], key: str = "alias") -> List[str]:
    names: List[str] = []
    seen: set[str] = set()
    for item in items or []:
        if not isinstance(item, dict):
            continue
        value = str(item.get(key) or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        names.append(value)
    return names


def _describe_comparison_type(comparison_type: Optional[str], periods: int = 1) -> str:
    label_map = {
        "yoy": "同比",
        "mom": "环比",
        "qoq": "季度环比",
        "wow": "周环比",
    }
    label = label_map.get(str(comparison_type or "").strip(), "")
    if not label:
        return ""
    if periods and periods > 1:
        return f"{label}（对比前{periods}期）"
    return label


def _describe_cross_partition_mode(mode: Optional[str]) -> str:
    mode_map = {
        "union": "跨表合并",
        "compare": "跨表对比",
        "multi_join": "多表关联",
    }
    return mode_map.get(str(mode or "").strip(), "")


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
    query_type = str(ir_display.get("query_type") or "aggregation")
    table_names = _sanitize_table_names(selected_table_names)
    metrics = [item for item in (ir_display.get("metrics") or []) if item]
    dimensions = [item for item in (ir_display.get("dimensions") or []) if item]
    duplicate_by = [item for item in (ir_display.get("duplicate_by") or []) if item]
    partition_by = [item for item in (ir_display.get("partition_by") or []) if item]
    window_limit = ir_display.get("window_limit")
    filters = ir_display.get("filters") or []
    order_by = ir_display.get("order_by") or []
    sort_by = ir_display.get("sort_by")
    sort_order = str(ir_display.get("sort_order") or "desc")
    limit = ir_display.get("limit")
    with_total = bool(ir_display.get("with_total"))
    ratio_metric_names = _extract_named_items(ir_display.get("ratio_metrics"))
    conditional_metric_names = _extract_named_items(ir_display.get("conditional_metrics"))
    calculated_field_names = _extract_named_items(ir_display.get("calculated_fields"))
    comparison_text = _describe_comparison_type(
        ir_display.get("comparison_type"),
        int(ir_display.get("comparison_periods") or 1),
    )
    cumulative_metrics = [item for item in (ir_display.get("cumulative_metrics") or []) if item]
    moving_average_metrics = [item for item in (ir_display.get("moving_average_metrics") or []) if item]
    moving_average_window = ir_display.get("moving_average_window")
    cross_partition_text = (
        _describe_cross_partition_mode(ir_display.get("cross_partition_mode"))
        if ir_display.get("cross_partition_query")
        else ""
    )
    time_text = describe_time_range(ir_display.get("time"))
    revision_text = get_revision_text(revision_request)

    parts: List[str] = []
    if table_names:
        parts.append(f"我要基于【{'、'.join(table_names)}】进行查询")
    else:
        parts.append("我要继续按当前已选语义草稿生成查询")

    if query_type == "duplicate_detection":
        if duplicate_by:
            parts.append(f"识别按【{'、'.join(duplicate_by)}】判定的重复记录")
        else:
            parts.append("识别重复记录")
    elif query_type == "window_detail":
        if partition_by and window_limit:
            parts.append(f"按【{'、'.join(partition_by)}】分组取前 {window_limit} 条明细")
        elif window_limit:
            parts.append(f"返回前 {window_limit} 条窗口明细")
        else:
            parts.append("返回窗口明细")
        if dimensions:
            parts.append(f"展示字段为【{'、'.join(dimensions)}】")
    elif query_type == "detail":
        if dimensions:
            parts.append(f"返回明细字段【{'、'.join(dimensions)}】")
        else:
            parts.append("返回明细记录")
        if sort_by:
            parts.append(f"按【{sort_by}】{'降序' if sort_order == 'desc' else '升序'}排列")
    else:
        if metrics:
            parts.append(f"统计【{'、'.join(metrics)}】")
        elif dimensions:
            parts.append(f"返回【{'、'.join(dimensions)}】")

        if dimensions and (metrics or ratio_metric_names or conditional_metric_names or calculated_field_names):
            parts.append(f"按【{'、'.join(dimensions)}】展开")

    if ratio_metric_names:
        parts.append(f"计算占比指标【{'、'.join(ratio_metric_names)}】")

    if conditional_metric_names:
        parts.append(f"补充条件指标【{'、'.join(conditional_metric_names)}】")

    if calculated_field_names:
        parts.append(f"计算字段【{'、'.join(calculated_field_names)}】")

    filter_parts: List[str] = []
    for condition in filters:
        if not isinstance(condition, dict):
            continue
        rendered = _format_filter_condition(condition)
        if rendered:
            filter_parts.append(rendered)
    if filter_parts:
        parts.append(f"筛选条件为 {'、'.join(filter_parts)}")

    if time_text:
        parts.append(f"时间范围为【{time_text}】")

    if comparison_text:
        growth_suffix = "，并显示增长率" if ir_display.get("show_growth_rate") else ""
        parts.append(f"分析方式为【{comparison_text}】{growth_suffix}")

    if cumulative_metrics:
        parts.append(f"计算【{'、'.join(cumulative_metrics)}】累计值")

    if moving_average_metrics and moving_average_window:
        parts.append(f"计算【{'、'.join(moving_average_metrics)}】的 {moving_average_window} 期移动平均")

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

    if with_total:
        parts.append("结果会附带合计行")

    if cross_partition_text:
        parts.append(f"查询方式为【{cross_partition_text}】")

    if revision_text:
        parts.append(f"本轮已吸收您的修改意见：{revision_text}")

    return "；".join(parts) + "。请确认是否继续。"


def build_draft_confirmation_open_points(
    *,
    confidence: Optional[float],
    confidence_threshold: float,
    ambiguities: Optional[List[str]] = None,
    revision_request: Optional[Dict[str, Any]] = None,
) -> List[str]:
    open_points: List[str] = []
    normalized_ambiguities = [str(item).strip() for item in (ambiguities or []) if str(item).strip()]

    if confidence is not None and confidence < confidence_threshold:
        open_points.append(
            f"当前语义理解置信度为 {confidence:.0%}，低于自动放行阈值 {confidence_threshold:.0%}。"
        )

    for ambiguity in normalized_ambiguities[:3]:
        open_points.append(ambiguity)

    if get_revision_text(revision_request):
        open_points.append("请确认系统是否正确吸收了本轮修改意见。")

    if not open_points:
        open_points.append("请确认当前查询草稿是否符合预期。")

    return open_points


def build_safe_summary(
    *,
    question_text: Optional[str],
    analysis_context: Optional[Dict[str, Any]] = None,
    domain_hint: Optional[str] = None,
    selected_table_names: Optional[List[str]] = None,
    ir_display: Optional[Dict[str, Any]] = None,
    open_points: Optional[List[str]] = None,
    known_constraints: Optional[List[str]] = None,
) -> Dict[str, Any]:
    normalized_constraints = _sanitize_summary_items(known_constraints)
    normalized_open_points = _sanitize_summary_items(open_points)
    normalized_table_names = _sanitize_table_names(selected_table_names)

    if normalized_table_names:
        normalized_constraints.append(f"当前数据表：{'、'.join(normalized_table_names)}")

    context_scope_summary = ""
    analysis_context = analysis_context or {}
    if analysis_context:
        context_scope_summary = str(analysis_context.get("scope_summary") or "").strip()
        if context_scope_summary:
            normalized_constraints.append(f"承接上一结果：{context_scope_summary}")

    ir_display = ir_display or {}
    metrics = [str(item).strip() for item in (ir_display.get("metrics") or []) if str(item).strip()]
    dimensions = [str(item).strip() for item in (ir_display.get("dimensions") or []) if str(item).strip()]
    time_text = describe_time_range(ir_display.get("time"))

    if metrics:
        normalized_constraints.append(f"统计指标：{'、'.join(metrics[:3])}")
    ratio_metric_names = _extract_named_items(ir_display.get("ratio_metrics"))
    if ratio_metric_names:
        normalized_constraints.append(f"占比指标：{'、'.join(ratio_metric_names[:3])}")
    conditional_metric_names = _extract_named_items(ir_display.get("conditional_metrics"))
    if conditional_metric_names:
        normalized_constraints.append(f"条件指标：{'、'.join(conditional_metric_names[:3])}")
    calculated_field_names = _extract_named_items(ir_display.get("calculated_fields"))
    if calculated_field_names:
        normalized_constraints.append(f"计算字段：{'、'.join(calculated_field_names[:3])}")
    if dimensions:
        normalized_constraints.append(f"分析维度：{'、'.join(dimensions[:3])}")
    if time_text:
        normalized_constraints.append(f"时间范围：{time_text}")
    comparison_text = _describe_comparison_type(
        ir_display.get("comparison_type"),
        int(ir_display.get("comparison_periods") or 1),
    )
    if comparison_text:
        growth_suffix = " + 增长率" if ir_display.get("show_growth_rate") else ""
        normalized_constraints.append(f"分析方式：{comparison_text}{growth_suffix}")
    cumulative_metrics = [str(item).strip() for item in (ir_display.get("cumulative_metrics") or []) if str(item).strip()]
    if cumulative_metrics:
        normalized_constraints.append(f"累计指标：{'、'.join(cumulative_metrics[:3])}")
    moving_average_metrics = [str(item).strip() for item in (ir_display.get("moving_average_metrics") or []) if str(item).strip()]
    moving_average_window = ir_display.get("moving_average_window")
    if moving_average_metrics and moving_average_window:
        normalized_constraints.append(
            f"移动平均：{'、'.join(moving_average_metrics[:3])}（{moving_average_window}期）"
        )

    deduped_constraints: List[str] = []
    seen_constraints: set[str] = set()
    for constraint in normalized_constraints:
        if constraint not in seen_constraints:
            seen_constraints.add(constraint)
            deduped_constraints.append(constraint)

    deduped_open_points: List[str] = []
    seen_open_points: set[str] = set()
    for point in normalized_open_points:
        if point not in seen_open_points:
            seen_open_points.add(point)
            deduped_open_points.append(point)

    return {
        "user_goal_summary": (question_text or context_scope_summary or "").strip(),
        "domain_hint": None if _is_probably_uuid(domain_hint) else domain_hint,
        "known_constraints": deduped_constraints[:6],
        "open_points": deduped_open_points[:5],
    }


def should_pause_for_draft_confirmation(
    *,
    confirmation_mode: str,
    request_has_ir: bool,
    existing_requires_draft_confirmation: bool,
    existing_has_confirmed_draft: bool,
    confidence: Optional[float],
    ambiguities: Optional[List[str]] = None,
    confidence_threshold: float,
) -> bool:
    if request_has_ir or existing_has_confirmed_draft:
        return False

    if existing_requires_draft_confirmation:
        return True

    if confirmation_mode == "always_confirm":
        return True

    if confirmation_mode != "adaptive":
        return False

    if confidence is not None and confidence < confidence_threshold:
        return True

    return any(str(item).strip() for item in (ambiguities or []))


def apply_analysis_context_to_ir(
    ir: "IntermediateRepresentation",
    analysis_context: Optional[Dict[str, Any]],
) -> "IntermediateRepresentation":
    if not analysis_context:
        return ir

    carry_over_flags = analysis_context.get("carry_over_flags") or {}
    base_refs = analysis_context.get("base_result_refs") or []
    base_ref = base_refs[0] if base_refs else {}

    if carry_over_flags.get("metrics") and not list(ir.metrics or []):
        inherited_metrics = [str(item) for item in (base_ref.get("metric_ids") or []) if item]
        if inherited_metrics:
            ir.metrics = inherited_metrics

    if carry_over_flags.get("dimensions") and not list(ir.dimensions or []):
        inherited_dimensions = [str(item) for item in (base_ref.get("dimension_ids") or []) if item]
        if inherited_dimensions:
            ir.dimensions = inherited_dimensions

    if carry_over_flags.get("table"):
        inherited_table_ids = [str(item) for item in (base_ref.get("table_ids") or []) if item]
        if inherited_table_ids:
            if not list(ir.selected_table_ids or []):
                ir.selected_table_ids = inherited_table_ids
            if not ir.primary_table_id:
                ir.primary_table_id = inherited_table_ids[0]

    return ir
