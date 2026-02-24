"""
查询模块

这是 query API 的入口点，所有路由从这里导出。
"""

# 导出路由（核心导出，供 main.py 使用）
from .routes import router

# 导出子模块供外部使用
from .stream_emitter import (
    QueryStreamEmitter,
    stream_progress,
    stream_result,
    stream_error,
    stream_confirmation,
    stream_table_selection,
)

from .history import save_query_history

from .visualization import suggest_visualization

from .ir_utils import (
    ir_to_display_dict,
    build_derived_rule_map,
    field_matches_table,
    metric_uses_table,
    # check_confirmation_needed 已移除 - 旧的"请确认AI理解"功能已废弃
)

from .sql_fallback import (
    maybe_fix_join_path_error,
    force_single_table_fallback,
    compile_with_join_fallback,
)

from .permission_checker import (
    format_table_names,
    format_field_names,
    collect_ir_field_ids,
    collect_ir_table_ids,
    enforce_schema_permissions,
    auto_detect_connection,
)

from .table_selection import llm_select_table, llm_select_table_cross_connections

from .derived_metrics import build_derived_metrics_explanation

__all__ = [
    # 路由
    "router",
    # Stream Emitter
    "QueryStreamEmitter",
    "stream_progress",
    "stream_result", 
    "stream_error",
    "stream_confirmation",
    "stream_table_selection",
    # History
    "save_query_history",
    # Visualization
    "suggest_visualization",
    # IR Utils
    "ir_to_display_dict",
    "build_derived_rule_map",
    "field_matches_table",
    "metric_uses_table",
    # check_confirmation_needed 已移除
    # SQL Fallback
    "maybe_fix_join_path_error",
    "force_single_table_fallback",
    "compile_with_join_fallback",
    # Permission
    "format_table_names",
    "format_field_names",
    "collect_ir_field_ids",
    "collect_ir_table_ids",
    "enforce_schema_permissions",
    "auto_detect_connection",
    # Table Selection
    "llm_select_table",
    "llm_select_table_cross_connections",
    # Derived Metrics
    "build_derived_metrics_explanation",
]

