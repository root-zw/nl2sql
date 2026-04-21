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
    table_names = _sanitize_table_names(selected_table_names)
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
    if dimensions:
        normalized_constraints.append(f"分析维度：{'、'.join(dimensions[:3])}")
    if time_text:
        normalized_constraints.append(f"时间范围：{time_text}")

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
