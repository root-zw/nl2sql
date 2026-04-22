"""
查询模块

使用惰性导出，避免导入 confirmation_utils 等子模块时连带加载 routes，
从而触发不必要的循环依赖。
"""

from __future__ import annotations

from importlib import import_module

_LAZY_EXPORTS = {
    "router": (".routes", "router"),
    "QueryStreamEmitter": (".stream_emitter", "QueryStreamEmitter"),
    "stream_progress": (".stream_emitter", "stream_progress"),
    "stream_result": (".stream_emitter", "stream_result"),
    "stream_error": (".stream_emitter", "stream_error"),
    "stream_confirmation": (".stream_emitter", "stream_confirmation"),
    "stream_table_selection": (".stream_emitter", "stream_table_selection"),
    "save_query_history": (".history", "save_query_history"),
    "suggest_visualization": (".visualization", "suggest_visualization"),
    "ir_to_display_dict": (".ir_utils", "ir_to_display_dict"),
    "build_derived_rule_map": (".ir_utils", "build_derived_rule_map"),
    "field_matches_table": (".ir_utils", "field_matches_table"),
    "metric_uses_table": (".ir_utils", "metric_uses_table"),
    "maybe_fix_join_path_error": (".sql_fallback", "maybe_fix_join_path_error"),
    "force_single_table_fallback": (".sql_fallback", "force_single_table_fallback"),
    "compile_with_join_fallback": (".sql_fallback", "compile_with_join_fallback"),
    "format_table_names": (".permission_checker", "format_table_names"),
    "format_field_names": (".permission_checker", "format_field_names"),
    "collect_ir_field_ids": (".permission_checker", "collect_ir_field_ids"),
    "collect_ir_table_ids": (".permission_checker", "collect_ir_table_ids"),
    "enforce_schema_permissions": (".permission_checker", "enforce_schema_permissions"),
    "auto_detect_connection": (".permission_checker", "auto_detect_connection"),
    "llm_select_table": (".table_selection", "llm_select_table"),
    "llm_select_table_cross_connections": (".table_selection", "llm_select_table_cross_connections"),
    "build_derived_metrics_explanation": (".derived_metrics", "build_derived_metrics_explanation"),
}

__all__ = list(_LAZY_EXPORTS.keys())


def __getattr__(name: str):
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _LAZY_EXPORTS[name]
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)

