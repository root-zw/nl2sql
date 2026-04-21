"""
IR 格式化、显示工具

注意：旧的 check_confirmation_needed 函数已移除
现在统一使用表选择确认卡（TableSelectionCard）进行用户确认
"""

from typing import Dict, Any, List, Optional

from server.models.ir import IntermediateRepresentation


def ir_to_display_dict(ir: IntermediateRepresentation, semantic_model) -> Dict[str, Any]:
    """将 IR 转换为便于展示的字典"""
    if not ir:
        return {}

    def resolve_field(field_id: str) -> str:
        if not field_id:
            return field_id

        field_id = str(field_id)
        if field_id == "__row_count__":
            return "记录数"
        if field_id.startswith("derived:"):
            derived_name = field_id[8:].strip()
            return derived_name or field_id

        if not semantic_model:
            return field_id
        fields = getattr(semantic_model, "fields", {}) or {}
        if field_id in fields:
            field = fields[field_id]
            return getattr(field, "display_name", None) or getattr(field, "field_name", None) or field_id
        metrics = getattr(semantic_model, "metrics", {}) or {}
        if field_id in metrics:
            metric = metrics[field_id]
            return getattr(metric, "display_name", None) or field_id
        return field_id
    
    def get_metric_display(metric_item) -> str:
        """获取 metric 的显示名（兼容字符串和 MetricSpec 格式）"""
        if isinstance(metric_item, str):
            return resolve_field(metric_item)
        elif isinstance(metric_item, dict):
            field_id = metric_item.get("field", "")
            alias = metric_item.get("alias")
            return alias or resolve_field(field_id)
        elif hasattr(metric_item, "field"):
            field_id = metric_item.field
            alias = getattr(metric_item, "alias", None)
            return alias or resolve_field(field_id)
        return str(metric_item)

    display_filters = [{
        "field": resolve_field(flt.field),
        "op": flt.op,
        "value": flt.value
    } for flt in ir.filters]

    return {
        "query_type": ir.query_type,
        "metrics": [get_metric_display(m) for m in ir.metrics],
        "dimensions": [resolve_field(d) for d in ir.dimensions],
        "time": ir.time.model_dump() if ir.time else None,
        "filters": display_filters,
        "limit": ir.limit,
        "order_by": [{"field": resolve_field(o.field), "desc": o.desc} for o in ir.order_by],
        "original_question": ir.original_question
    }


def build_derived_rule_map(global_rules: Optional[List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """构建派生规则映射"""
    mapping: Dict[str, Dict[str, Any]] = {}
    if not global_rules:
        return mapping
    for rule in global_rules:
        if rule.get("rule_type") != "derived_metric":
            continue
        definition = rule.get("rule_definition") or {}
        display_name = definition.get("display_name")
        if not display_name:
            display_name = rule.get("rule_name", "").replace("（派生）", "")
        if display_name:
            mapping[display_name] = definition
    return mapping


def field_matches_table(field_id: Optional[str], table_id: Optional[str], semantic_model) -> bool:
    """检查字段是否属于指定表"""
    if not field_id or not table_id:
        return False
    field = semantic_model.fields.get(field_id)
    if field and getattr(field, "datasource_id", None) == table_id:
        return True
    dim = semantic_model.dimensions.get(field_id)
    if dim and (getattr(dim, "datasource_id", None) == table_id or getattr(dim, "table", None) == table_id):
        return True
    measure = semantic_model.measures.get(field_id)
    if measure and (getattr(measure, "datasource_id", None) == table_id or getattr(measure, "table", None) == table_id):
        return True
    return False


def metric_uses_table(
    metric_id: str,
    table_id: str,
    semantic_model,
    derived_rules_map: Dict[str, Dict[str, Any]]
) -> bool:
    """检查指标是否使用指定表"""
    if not table_id:
        return False

    if metric_id.startswith("derived:"):
        derived_name = metric_id[8:]
        definition = derived_rules_map.get(derived_name)
        if not definition:
            return False
        for dep in definition.get("field_dependencies", []) or []:
            if field_matches_table(dep.get("field_id"), table_id, semantic_model):
                return True
        return False

    metric = semantic_model.metrics.get(metric_id)
    if metric and metric.metric_type == "atomic" and metric.atomic_def:
        base_field_id = metric.atomic_def.base_field_id
        if field_matches_table(base_field_id, table_id, semantic_model):
            return True

    if metric and metric.dependencies:
        for dep in metric.dependencies:
            if dep.depends_on_type == "field" and field_matches_table(dep.depends_on_id, table_id, semantic_model):
                return True

    if metric_id in semantic_model.fields:
        return field_matches_table(metric_id, table_id, semantic_model)

    measure = semantic_model.measures.get(metric_id)
    if measure and getattr(measure, "datasource_id", None) == table_id:
        return True
    return False
