"""统一系统理解生成服务。

面向用户的确认内容只展示一个“系统理解”列表；
内部则把模型输出、默认过滤和权限范围统一收敛成最终 bullets。
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import structlog
from sqlglot import exp, parse_one

from server.api.query.confirmation_utils import (
    describe_time_range,
    get_revision_text,
)
from server.models.api import UnderstandingItem
from server.utils.field_display import get_field_display_name

logger = structlog.get_logger()

STRUCTURED_UNDERSTANDING_PREFIXES = (
    "当前数据表：",
    "当前涉及数据表：",
    "统计指标：",
    "占比指标：",
    "条件指标：",
    "计算字段：",
    "分析维度：",
    "分析方式：",
    "时间范围：",
    "累计指标：",
    "移动平均：",
)

TEMPLATED_UNDERSTANDING_PREFIXES = (
    "我要基于【",
    "统计【",
    "按【",
    "计算占比指标【",
    "补充条件指标【",
    "计算字段【",
    "筛选条件为",
    "时间范围为【",
    "分析方式为【",
    "排序方式为",
    "结果条数限制为",
    "结果会附带合计行",
    "查询方式为【",
    "返回明细字段【",
    "返回明细记录",
    "识别按【",
    "识别重复记录",
)


def _normalize_text(text: Any) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return ""
    return normalized.rstrip("；; \n")


def normalize_understanding_items(
    items: Optional[Iterable[Any]],
    *,
    default_source: str = "merged",
) -> List[UnderstandingItem]:
    normalized_items: List[UnderstandingItem] = []
    seen_texts: set[str] = set()

    for item in items or []:
        if isinstance(item, UnderstandingItem):
            text = _normalize_text(item.text)
            anchors = [str(anchor).strip() for anchor in item.anchors if str(anchor).strip()]
            source = item.source
            material = item.material
        elif isinstance(item, dict):
            text = _normalize_text(item.get("text"))
            anchors = [str(anchor).strip() for anchor in (item.get("anchors") or []) if str(anchor).strip()]
            source = str(item.get("source") or default_source)
            material = bool(item.get("material", True))
        else:
            text = _normalize_text(item)
            anchors = []
            source = default_source
            material = True

        if not text or text in seen_texts:
            continue

        seen_texts.add(text)
        normalized_items.append(
            UnderstandingItem(
                text=text,
                anchors=anchors,
                source=source if source in {"model", "system", "merged"} else default_source,
                material=material,
            )
        )

    return normalized_items


def summarize_understanding_items(items: Optional[Iterable[Any]]) -> str:
    normalized_items = normalize_understanding_items(items)
    if not normalized_items:
        return ""
    parts = [item.text.rstrip("。") for item in normalized_items if item.text]
    if not parts:
        return ""
    return "；".join(parts) + "。"


def _split_summary_to_items(summary: str) -> List[UnderstandingItem]:
    stripped = str(summary or "").strip().rstrip("。")
    if not stripped:
        return []
    return normalize_understanding_items(
        [{"text": part.strip()} for part in stripped.split("；") if part.strip()],
        default_source="system",
    )


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


def _normalize_name_list(items: Optional[List[Any]]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for item in items or []:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _looks_like_structured_understanding_text(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized:
        return False
    if any(normalized.startswith(prefix) for prefix in STRUCTURED_UNDERSTANDING_PREFIXES):
        return True
    if any(normalized.startswith(prefix) for prefix in TEMPLATED_UNDERSTANDING_PREFIXES):
        return True
    if "【" in normalized and "】" in normalized:
        for token in ("统计", "筛选条件", "分析方式", "时间范围", "排序方式", "查询方式", "占比指标", "条件指标"):
            if token in normalized:
                return True
    return False


def _render_ir_filter_text(condition: Dict[str, Any]) -> str:
    if not isinstance(condition, dict):
        return ""
    field_display = str(condition.get("field") or "").strip()
    operator = str(condition.get("op") or "").strip()
    value = condition.get("value")
    if not field_display or not operator:
        return ""
    return _resolve_filter_condition_text(field_display, operator, value)


def _join_preview(items: List[str], *, max_items: int = 3) -> str:
    normalized = [str(item).strip() for item in items if str(item).strip()]
    if not normalized:
        return ""
    preview = "、".join(normalized[:max_items])
    if len(normalized) > max_items:
        preview = f"{preview} 等{len(normalized)}项"
    return preview


def _build_natural_fallback_items(
    *,
    ir_display: Dict[str, Any],
    selected_table_names: Optional[List[str]] = None,
) -> List[UnderstandingItem]:
    query_type = str(ir_display.get("query_type") or "aggregation")
    table_names = _normalize_name_list(selected_table_names)
    metrics = _normalize_name_list(ir_display.get("metrics") or [])
    dimensions = _normalize_name_list(ir_display.get("dimensions") or [])
    duplicate_by = _normalize_name_list(ir_display.get("duplicate_by") or [])
    partition_by = _normalize_name_list(ir_display.get("partition_by") or [])
    ratio_metric_names = _extract_named_items(ir_display.get("ratio_metrics"))
    conditional_metric_names = _extract_named_items(ir_display.get("conditional_metrics"))
    calculated_field_names = _extract_named_items(ir_display.get("calculated_fields"))
    cumulative_metrics = _normalize_name_list(ir_display.get("cumulative_metrics") or [])
    moving_average_metrics = _normalize_name_list(ir_display.get("moving_average_metrics") or [])
    moving_average_window = ir_display.get("moving_average_window")
    comparison_text = str(ir_display.get("comparison_type") or "").strip()
    show_growth_rate = bool(ir_display.get("show_growth_rate"))
    limit = ir_display.get("limit")
    time_text = describe_time_range(ir_display.get("time"))
    filter_texts = [
        rendered
        for rendered in (_render_ir_filter_text(condition) for condition in (ir_display.get("filters") or []))
        if rendered
    ]

    comparison_label_map = {
        "yoy": "同比",
        "mom": "环比",
        "qoq": "季度环比",
        "wow": "周环比",
    }
    comparison_label = comparison_label_map.get(comparison_text, "")

    bullets: List[UnderstandingItem] = []

    if table_names:
        table_preview = _join_preview(table_names)
        if len(table_names) == 1:
            bullets.append(
                UnderstandingItem(
                    text=f"我会基于{table_preview}这张表来查询。",
                    anchors=["table"],
                    source="system",
                    material=True,
                )
            )
        else:
            bullets.append(
                UnderstandingItem(
                    text=f"我会基于{table_preview}这些表来完成这次查询。",
                    anchors=["table"],
                    source="system",
                    material=True,
                )
            )

    if query_type == "duplicate_detection":
        duplicate_preview = _join_preview(duplicate_by)
        if duplicate_preview:
            bullets.append(
                UnderstandingItem(
                    text=f"我会按{duplicate_preview}来识别重复记录。",
                    anchors=["duplicate_by"],
                    source="system",
                    material=True,
                )
            )
    elif query_type == "window_detail":
        partition_preview = _join_preview(partition_by)
        dimension_preview = _join_preview(dimensions)
        window_limit = ir_display.get("window_limit")
        if partition_preview and window_limit:
            bullets.append(
                UnderstandingItem(
                    text=f"结果会按{partition_preview}分组，并在每组里取前{window_limit}条明细。",
                    anchors=["partition_by", "window_limit"],
                    source="system",
                    material=True,
                )
            )
        elif dimension_preview:
            bullets.append(
                UnderstandingItem(
                    text=f"结果会返回{dimension_preview}相关的明细记录。",
                    anchors=["dimensions"],
                    source="system",
                    material=True,
                )
            )
    elif query_type == "detail":
        dimension_preview = _join_preview(dimensions)
        if dimension_preview:
            bullets.append(
                UnderstandingItem(
                    text=f"结果会返回{dimension_preview}这些字段对应的明细记录。",
                    anchors=["dimensions"],
                    source="system",
                    material=True,
                )
            )
        else:
            bullets.append(
                UnderstandingItem(
                    text="这次会直接返回符合条件的明细记录。",
                    anchors=["query_type"],
                    source="system",
                    material=True,
                )
            )
    else:
        metric_preview = _join_preview(metrics)
        dimension_preview = _join_preview(dimensions)
        if metric_preview and dimension_preview:
            bullets.append(
                UnderstandingItem(
                    text=f"结果会按{dimension_preview}展开，统计{metric_preview}。",
                    anchors=["dimensions", "metrics"],
                    source="system",
                    material=True,
                )
            )
        elif metric_preview:
            bullets.append(
                UnderstandingItem(
                    text=f"我会先统计{metric_preview}。",
                    anchors=["metrics"],
                    source="system",
                    material=True,
                )
            )
        elif dimension_preview:
            bullets.append(
                UnderstandingItem(
                    text=f"结果会围绕{dimension_preview}来展示。",
                    anchors=["dimensions"],
                    source="system",
                    material=True,
                )
            )

    extra_metric_parts: List[str] = []
    ratio_preview = _join_preview(ratio_metric_names)
    conditional_preview = _join_preview(conditional_metric_names)
    calculated_preview = _join_preview(calculated_field_names)
    if ratio_preview:
        extra_metric_parts.append(f"计算{ratio_preview}")
    if conditional_preview:
        extra_metric_parts.append(f"补充{conditional_preview}这类条件指标")
    if calculated_preview:
        extra_metric_parts.append(f"补充{calculated_preview}这类计算字段")
    if extra_metric_parts:
        bullets.append(
            UnderstandingItem(
                text=f"同时会{'，并'.join(extra_metric_parts)}。",
                anchors=["ratio_metrics", "conditional_metrics", "calculated_fields"],
                source="system",
                material=True,
            )
        )

    if time_text and filter_texts:
        bullets.append(
            UnderstandingItem(
                text=f"筛选范围会限制在{time_text}，并且只看{'、'.join(filter_texts)}的记录。",
                anchors=["time", "filters"],
                source="system",
                material=True,
            )
        )
    elif time_text:
        bullets.append(
            UnderstandingItem(
                text=f"筛选范围会限制在{time_text}。",
                anchors=["time"],
                source="system",
                material=True,
            )
        )
    elif filter_texts:
        bullets.append(
            UnderstandingItem(
                text=f"筛选范围会限制为{'、'.join(filter_texts)}。",
                anchors=["filters"],
                source="system",
                material=True,
            )
        )

    analysis_parts: List[str] = []
    if comparison_label:
        base_analysis = f"结果会做{comparison_label}分析"
        if show_growth_rate:
            base_analysis += "并显示增长率"
        analysis_parts.append(base_analysis)
    elif show_growth_rate:
        analysis_parts.append("结果会显示增长率")
    if cumulative_metrics:
        analysis_parts.append(f"同时补充{_join_preview(cumulative_metrics)}的累计值")
    if moving_average_metrics and moving_average_window:
        analysis_parts.append(f"并计算{_join_preview(moving_average_metrics)}的{moving_average_window}期移动平均")
    if limit:
        analysis_parts.append(f"结果最多返回{limit}条")
    if analysis_parts:
        bullets.append(
            UnderstandingItem(
                text="，".join(analysis_parts) + "。",
                anchors=["comparison", "cumulative_metrics", "moving_average_metrics", "limit"],
                source="system",
                material=True,
            )
        )

    return normalize_understanding_items(bullets, default_source="system")


def _get_selected_table_scope(
    ir,
    *,
    selected_table_ids: Optional[List[str]] = None,
) -> set[str]:
    scope = {
        str(table_id).strip()
        for table_id in (selected_table_ids or getattr(ir, "selected_table_ids", None) or [])
        if str(table_id).strip()
    }
    primary_table_id = str(getattr(ir, "primary_table_id", "") or "").strip()
    if primary_table_id:
        scope.add(primary_table_id)
    return scope


def _resolve_filter_condition_text(field_display: str, operator: str, value: Any) -> str:
    op = str(operator or "").upper()
    if not field_display:
        field_display = "该字段"

    if op == "=":
        return f"{field_display}为“{value}”"
    if op == "!=":
        return f"{field_display}不为“{value}”"
    if op == ">":
        return f"{field_display}大于 {value}"
    if op == ">=":
        return f"{field_display}大于等于 {value}"
    if op == "<":
        return f"{field_display}小于 {value}"
    if op == "<=":
        return f"{field_display}小于等于 {value}"
    if op == "LIKE":
        raw = str(value or "")
        clean = raw.replace("%", "").strip()
        if clean:
            return f"{field_display}包含“{clean}”"
        return f"{field_display}满足模糊匹配条件"
    if op == "IN":
        values = value if isinstance(value, list) else [value]
        preview = "、".join(str(item) for item in values[:3] if str(item).strip())
        if len(values) > 3:
            preview = f"{preview} 等{len(values)}个值"
        return f"{field_display}属于 {preview}"
    if op == "NOT IN":
        values = value if isinstance(value, list) else [value]
        preview = "、".join(str(item) for item in values[:3] if str(item).strip())
        if len(values) > 3:
            preview = f"{preview} 等{len(values)}个值"
        return f"{field_display}不属于 {preview}"
    if op == "IS NULL":
        return f"{field_display}为空"
    if op == "IS NOT NULL":
        return f"{field_display}不为空"
    return f"{field_display}{operator}{value}"


def _render_default_filter_text(
    *,
    field_display: str,
    operator: str,
    value: Any,
    prefix: str = "系统默认只统计",
) -> str:
    condition_text = _resolve_filter_condition_text(field_display, operator, value)
    return f"{prefix}{condition_text}的记录。"


def _extract_sql_filter_parts(filter_sql: str, semantic_model=None) -> Optional[Dict[str, Any]]:
    try:
        expression = parse_one(filter_sql, dialect="tsql")
    except Exception:
        logger.debug("解析默认过滤 SQL 失败，跳过展示", filter_sql=filter_sql)
        return None

    column_name = ""
    operator = ""
    value: Any = None

    if isinstance(expression, exp.In):
        column_name = expression.this.name if isinstance(expression.this, exp.Column) else expression.this.sql()
        operator = "IN"
        value = [
            item.this if isinstance(item, exp.Literal) else item.sql()
            for item in (expression.expressions or [])
        ]
    elif isinstance(expression, exp.Like):
        column_name = expression.this.name if isinstance(expression.this, exp.Column) else expression.this.sql()
        operator = "LIKE"
        rhs = expression.expression
        value = rhs.this if isinstance(rhs, exp.Literal) else rhs.sql()
    elif isinstance(expression, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        lhs = expression.this
        rhs = expression.expression
        column_name = lhs.name if isinstance(lhs, exp.Column) else lhs.sql()
        operator = {
            exp.EQ: "=",
            exp.NEQ: "!=",
            exp.GT: ">",
            exp.GTE: ">=",
            exp.LT: "<",
            exp.LTE: "<=",
        }.get(type(expression), expression.key.upper())
        value = rhs.this if isinstance(rhs, exp.Literal) else rhs.sql()
    elif isinstance(expression, exp.Is):
        lhs = expression.this
        rhs = expression.expression
        column_name = lhs.name if isinstance(lhs, exp.Column) else lhs.sql()
        rhs_sql = rhs.sql().upper()
        operator = "IS NOT NULL" if "NOT NULL" in rhs_sql else "IS NULL"
        value = None
    else:
        return None

    field_display = get_field_display_name(column_name, semantic_model)
    return {
        "field_display": field_display or column_name,
        "operator": operator,
        "value": value,
    }


def _build_metric_default_filter_items(ir, semantic_model=None) -> List[str]:
    items: List[str] = []
    seen_sql: set[str] = set()

    for metric_item in list(getattr(ir, "metrics", None) or []):
        if isinstance(metric_item, str):
            metric_id = metric_item
        elif isinstance(metric_item, dict):
            metric_id = str(metric_item.get("field") or metric_item)
        else:
            metric_id = getattr(metric_item, "field", str(metric_item))

        metric = getattr(semantic_model, "metrics", {}).get(metric_id) if semantic_model else None
        if not metric:
            continue

        for filter_sql in list(getattr(metric, "default_filters", None) or []):
            normalized_sql = str(filter_sql or "").strip()
            if not normalized_sql or normalized_sql in seen_sql:
                continue
            seen_sql.add(normalized_sql)

            parsed = _extract_sql_filter_parts(normalized_sql, semantic_model)
            if parsed:
                items.append(
                    _render_default_filter_text(
                        field_display=parsed["field_display"],
                        operator=parsed["operator"],
                        value=parsed["value"],
                        prefix="系统默认会按指标口径只统计",
                    )
                )

    return items


def _build_data_quality_filter_items(
    ir,
    semantic_model=None,
    *,
    selected_table_ids: Optional[List[str]] = None,
) -> List[str]:
    items: List[str] = []
    rules = getattr(getattr(semantic_model, "rules", None), "data_quality_rules", None)
    if not isinstance(rules, dict) or not rules.get("enabled"):
        return items

    default_filter = rules.get("default_record_filter") or {}
    apply_to = default_filter.get("apply_to", {}) or {}
    is_detail = getattr(ir, "query_type", None) in {"detail", "window_detail", "duplicate_detection"}
    is_aggregation = getattr(ir, "query_type", None) == "aggregation"
    should_apply = bool(
        (is_detail and apply_to.get("detail_queries"))
        or (is_aggregation and apply_to.get("aggregation_queries"))
    )
    if not should_apply:
        return items

    field_name = str(default_filter.get("field_name") or "").strip()
    operator = str(default_filter.get("operator") or "=").strip()
    value = default_filter.get("value")
    if not field_name:
        return items

    selected_scope = _get_selected_table_scope(ir, selected_table_ids=selected_table_ids)
    apply_to_tables = set(str(table).strip() for table in (default_filter.get("apply_to_tables") or []) if str(table).strip())
    exception_tables = set(str(table).strip() for table in (default_filter.get("exception_tables") or []) if str(table).strip())

    if apply_to_tables and selected_scope:
        if not (apply_to_tables & selected_scope):
            return items
    if exception_tables and selected_scope and selected_scope.issubset(exception_tables):
        return items

    field_display = get_field_display_name(field_name, semantic_model)
    items.append(
        _render_default_filter_text(
            field_display=field_display or field_name,
            operator=operator,
            value=value,
        )
    )
    return items


def _build_global_default_filter_items(
    ir,
    semantic_model=None,
    *,
    global_rules: Optional[List[Dict[str, Any]]] = None,
    selected_table_ids: Optional[List[str]] = None,
) -> List[str]:
    items: List[str] = []
    selected_scope = _get_selected_table_scope(ir, selected_table_ids=selected_table_ids)

    for rule in global_rules or []:
        if rule.get("rule_type") != "default_filter":
            continue

        rule_def = rule.get("rule_definition") or {}
        table_id = str(rule_def.get("table_id") or "").strip()
        if table_id and selected_scope and table_id not in selected_scope:
            continue

        field_name = str(rule_def.get("filter_field") or "").strip()
        operator = str(rule_def.get("filter_operator") or "=").strip()
        value = rule_def.get("filter_value")
        if not field_name:
            continue

        field_display = get_field_display_name(field_name, semantic_model)
        items.append(
            _render_default_filter_text(
                field_display=field_display or field_name,
                operator=operator,
                value=value,
            )
        )

    return items


def _build_legacy_detail_view_default_filter_items(
    ir,
    semantic_model=None,
    *,
    selected_table_ids: Optional[List[str]] = None,
) -> List[str]:
    if getattr(ir, "query_type", None) != "detail" or not semantic_model:
        return []

    selected_scope = _get_selected_table_scope(ir, selected_table_ids=selected_table_ids)
    if not selected_scope:
        return []

    items: List[str] = []
    sources = getattr(semantic_model, "sources", None) or {}
    for table_id in selected_scope:
        source = sources.get(table_id)
        detail_view = getattr(source, "detail_view", None) if source else None
        default_filters = getattr(detail_view, "default_filters", None) or []
        for filter_sql in default_filters:
            normalized_sql = str(filter_sql or "").strip()
            if not normalized_sql:
                continue
            parsed = _extract_sql_filter_parts(normalized_sql, semantic_model)
            if not parsed:
                continue
            items.append(
                _render_default_filter_text(
                    field_display=parsed["field_display"],
                    operator=parsed["operator"],
                    value=parsed["value"],
                )
            )

    return items


def preview_default_filter_items(
    ir,
    semantic_model=None,
    *,
    global_rules: Optional[List[Dict[str, Any]]] = None,
    selected_table_ids: Optional[List[str]] = None,
) -> List[str]:
    items = []
    items.extend(_build_metric_default_filter_items(ir, semantic_model))
    items.extend(_build_data_quality_filter_items(ir, semantic_model, selected_table_ids=selected_table_ids))
    items.extend(
        _build_legacy_detail_view_default_filter_items(
            ir,
            semantic_model,
            selected_table_ids=selected_table_ids,
        )
    )
    items.extend(
        _build_global_default_filter_items(
            ir,
            semantic_model,
            global_rules=global_rules,
            selected_table_ids=selected_table_ids,
        )
    )

    deduped_items: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = _normalize_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        deduped_items.append(f"{text.rstrip('。')}。")
    return deduped_items


def build_permission_scope_items(
    permission_info: Optional[Dict[str, Any]],
    *,
    semantic_model=None,
    max_preview_values: int = 3,
) -> List[str]:
    if not permission_info or not permission_info.get("applied"):
        return []

    restricted_fields = permission_info.get("restricted_fields") or {}
    items: List[str] = []
    for field_name, allowed_values in restricted_fields.items():
        values = allowed_values if isinstance(allowed_values, list) else [allowed_values]
        values = [str(value).strip() for value in values if str(value).strip()]
        if not values:
            continue

        display_name = get_field_display_name(field_name, semantic_model) or str(field_name)
        if len(values) <= max_preview_values:
            preview = "、".join(values)
        else:
            preview = "、".join(values[:max_preview_values]) + f" 等{len(values)}个值"

        items.append(f"当前结果仅包含你有权限访问的{display_name}：{preview}。")

    return items


def build_system_understanding(
    *,
    ir_display: Dict[str, Any],
    selected_table_names: Optional[List[str]] = None,
    model_understanding: Optional[Iterable[Any]] = None,
    revision_request: Optional[Dict[str, Any]] = None,
    default_filter_items: Optional[List[str]] = None,
    permission_scope_items: Optional[List[str]] = None,
) -> List[UnderstandingItem]:
    model_items = normalize_understanding_items(model_understanding, default_source="model")
    natural_model_items = [
        item
        for item in model_items
        if not _looks_like_structured_understanding_text(item.text)
    ]

    if natural_model_items and (len(natural_model_items) >= 2 or len(natural_model_items) == len(model_items)):
        base_items = natural_model_items
    else:
        if model_items:
            logger.info(
                "系统理解命中结构化模板输出，已回退到自然语言 fallback",
                raw_count=len(model_items),
                kept_count=len(natural_model_items),
            )
        base_items = _build_natural_fallback_items(
            ir_display=ir_display,
            selected_table_names=selected_table_names,
        )

    revision_text = get_revision_text(revision_request)
    merged_items: List[UnderstandingItem] = []
    if revision_text:
        merged_items.append(
            UnderstandingItem(
                text=f"已吸收本轮修改：{revision_text}。",
                anchors=["revision"],
                source="system",
                material=False,
            )
        )

    merged_items.extend(base_items)
    merged_items.extend(
        UnderstandingItem(text=text, anchors=["default_filters"], source="system", material=True)
        for text in (default_filter_items or [])
    )
    merged_items.extend(
        UnderstandingItem(text=text, anchors=["permission_scope"], source="system", material=True)
        for text in (permission_scope_items or [])
    )

    return normalize_understanding_items(merged_items)
