"""
SQL 编译回退逻辑

处理 Join 路径缺失等编译错误，通过移除或映射跨表字段来修复
"""

import re
from typing import Dict, Any, List, Optional, Tuple
from collections import Counter
import structlog

from server.models.ir import IntermediateRepresentation, FilterCondition
from server.exceptions import CompilationError
from .ir_utils import build_derived_rule_map, field_matches_table, metric_uses_table

logger = structlog.get_logger()


def maybe_fix_join_path_error(
    error: CompilationError,
    ir: IntermediateRepresentation,
    semantic_model,
    global_rules: Optional[List[Dict[str, Any]]]
) -> Optional[Dict[str, Any]]:
    """
    尝试通过移除或映射跨表指标/维度解决Join路径缺失问题。
    
    V2 改进：跨表字段先尝试映射到主表的同名字段，映射失败才移除。
    
    策略：将所有属于"缺失表"的字段尝试映射到主表，映射失败则移除。
    如果移除后没有保留任何内容，则尝试反向操作（保留缺失表，移除主表）。
    """
    message = getattr(error, "message", "") or ""
    if "无法找到从" not in message:
        return None

    details = getattr(error, "details", {}) or {}
    missing_table = details.get("to")
    main_table = details.get("from")
    if not missing_table:
        match = re.search(r"无法找到从\s+([0-9a-fA-F-]+)\s+到\s+([0-9a-fA-F-]+)", message)
        if match:
            main_table = match.group(1)
            missing_table = match.group(2)

    if not missing_table:
        return None

    derived_map = build_derived_rule_map(global_rules)
    
    # 构建主表的字段名到字段ID映射
    def build_field_name_map(table_id: str) -> Dict[str, str]:
        """构建指定表的字段名到字段ID映射"""
        name_to_id: Dict[str, str] = {}
        for field_id, field in semantic_model.fields.items():
            if str(field.datasource_id) == table_id and field.is_active:
                name_to_id[field.display_name] = field_id
        return name_to_id
    
    def try_remove_or_map_table(table_to_remove: str, target_table: str) -> Optional[Dict[str, Any]]:
        """尝试将指定表的字段映射到目标表，映射失败则移除"""
        target_field_map = build_field_name_map(target_table)
        
        retained_metrics: List[str] = []
        removed_metrics: List[str] = []
        removed_dimensions: List[str] = []
        removed_filters: List[str] = []
        field_mappings: List[Dict[str, Any]] = []

        # 处理指标
        for metric_item in ir.metrics:
            # 兼容字符串和 MetricSpec 格式
            if isinstance(metric_item, str):
                metric_id = metric_item
            elif isinstance(metric_item, dict):
                metric_id = metric_item.get("field", str(metric_item))
            elif hasattr(metric_item, "field"):
                metric_id = metric_item.field
            else:
                metric_id = str(metric_item)
            
            if metric_uses_table(metric_id, table_to_remove, semantic_model, derived_map):
                removed_metrics.append(metric_item)
            else:
                retained_metrics.append(metric_item)

        # 处理维度（V2：先尝试映射）
        retained_dimensions: List[str] = []
        for dim_id in ir.dimensions:
            dim_field = semantic_model.fields.get(dim_id)
            if dim_field and str(dim_field.datasource_id) == table_to_remove:
                # 尝试映射到目标表的同名字段
                field_name = dim_field.display_name
                if field_name in target_field_map:
                    mapped_id = target_field_map[field_name]
                    retained_dimensions.append(mapped_id)
                    field_mappings.append({
                        "original_field": dim_id,
                        "mapped_field": mapped_id,
                        "field_name": field_name,
                        "type": "dimension"
                    })
                else:
                    removed_dimensions.append(dim_id)
            else:
                retained_dimensions.append(dim_id)

        # 处理过滤条件（V2：先尝试映射）
        retained_filters = []
        for flt in ir.filters:
            filter_field = semantic_model.fields.get(flt.field)
            if filter_field and str(filter_field.datasource_id) == table_to_remove:
                # 尝试映射到目标表的同名字段
                field_name = filter_field.display_name
                if field_name in target_field_map:
                    mapped_id = target_field_map[field_name]
                    mapped_filter = FilterCondition(
                        field=mapped_id,
                        op=flt.op,
                        value=flt.value
                    )
                    retained_filters.append(mapped_filter)
                    field_mappings.append({
                        "original_field": flt.field,
                        "mapped_field": mapped_id,
                        "field_name": field_name,
                        "type": "filter",
                        "value": flt.value
                    })
                else:
                    removed_filters.append(flt.field)
            else:
                retained_filters.append(flt)

        has_changes = removed_metrics or removed_dimensions or removed_filters or field_mappings
        has_retained = retained_metrics or retained_dimensions
        
        if not has_changes or not has_retained:
            return None
        
        # 日志记录映射信息
        if field_mappings:
            logger.info(
                "跨表字段映射成功",
                target_table=target_table,
                mappings=field_mappings
            )
            
        return {
            "retained_metrics": retained_metrics,
            "retained_dimensions": retained_dimensions,
            "retained_filters": retained_filters,
            "removed_metrics": removed_metrics,
            "removed_dimensions": removed_dimensions,
            "removed_filters": removed_filters,
            "field_mappings": field_mappings,
            "removed_table": table_to_remove,
        }

    # 首先尝试移除/映射"缺失表"的字段到"主表"
    result = try_remove_or_map_table(missing_table, main_table)
    
    # 如果失败，尝试移除"主表"（反向操作）
    if not result and main_table:
        result = try_remove_or_map_table(main_table, missing_table)
        if result:
            # 交换 main_table 和 missing_table
            result["removed_table"], missing_table = main_table, result["removed_table"]
            main_table = missing_table
    
    if not result:
        return None

    # 更新 IR
    ir.metrics = result["retained_metrics"]
    ir.dimensions = result["retained_dimensions"]
    ir.filters = result["retained_filters"]

    return {
        "removed_metrics": result["removed_metrics"],
        "removed_dimensions": result["removed_dimensions"],
        "removed_filters": result["removed_filters"],
        "field_mappings": result.get("field_mappings", []),
        "missing_table": result["removed_table"],
        "main_table": main_table
    }


def force_single_table_fallback(
    ir: IntermediateRepresentation,
    semantic_model,
    global_rules: Optional[List[Dict[str, Any]]]
) -> Optional[Dict[str, Any]]:
    """
    强制单表模式：统计各字段所属表的出现次数，保留出现最多的表。
    
    V2 改进：跨表字段先尝试映射到主表的同名字段，映射失败才移除。
    
    当常规fallback失败时使用此策略。
    """
    table_counts = Counter()
    field_to_table: Dict[str, str] = {}
    field_to_name: Dict[str, str] = {}  # field_id -> display_name
    
    # 统计维度和过滤条件的表归属
    for dim_id in ir.dimensions:
        dim_field = semantic_model.fields.get(dim_id)
        if dim_field:
            table_id = str(dim_field.datasource_id)
            table_counts[table_id] += 1
            field_to_table[dim_id] = table_id
            field_to_name[dim_id] = dim_field.display_name
    
    for flt in ir.filters:
        filter_field = semantic_model.fields.get(flt.field)
        if filter_field:
            table_id = str(filter_field.datasource_id)
            table_counts[table_id] += 1
            field_to_table[flt.field] = table_id
            field_to_name[flt.field] = filter_field.display_name
    
    # 统计指标的表归属（使用派生规则映射）
    derived_map = build_derived_rule_map(global_rules)
    for metric_item in ir.metrics:
        # 兼容字符串和 MetricSpec 格式
        if isinstance(metric_item, str):
            metric_id = metric_item
        elif isinstance(metric_item, dict):
            metric_id = metric_item.get("field", str(metric_item))
        elif hasattr(metric_item, "field"):
            metric_id = metric_item.field
        else:
            metric_id = str(metric_item)
        
        metric = semantic_model.metrics.get(metric_id) if hasattr(semantic_model, 'metrics') else None
        if metric:
            table_id = str(metric.datasource_id) if hasattr(metric, 'datasource_id') else None
            if table_id:
                table_counts[table_id] += 1
                field_to_table[metric_id] = table_id
                field_to_name[metric_id] = getattr(metric, 'display_name', metric_id)
    
    if not table_counts:
        return None
    
    # 使用 IR 中指定的主表，如果没有则选择出现最多的表
    primary_table = ir.primary_table_id if ir.primary_table_id else table_counts.most_common(1)[0][0]
    
    # 构建主表的字段名到字段ID映射
    primary_table_field_map: Dict[str, str] = {}  # display_name -> field_id
    for field_id, field in semantic_model.fields.items():
        if str(field.datasource_id) == primary_table and field.is_active:
            primary_table_field_map[field.display_name] = field_id
    
    # V2 改进：尝试将跨表字段映射到主表的同名字段
    field_mappings = []  # 记录映射信息
    
    retained_dimensions = []
    for d in ir.dimensions:
        if field_to_table.get(d) == primary_table:
            retained_dimensions.append(d)
        else:
            # 尝试映射到主表的同名字段
            field_name = field_to_name.get(d)
            if field_name and field_name in primary_table_field_map:
                mapped_id = primary_table_field_map[field_name]
                retained_dimensions.append(mapped_id)
                field_mappings.append({
                    "original_field": d,
                    "mapped_field": mapped_id,
                    "field_name": field_name,
                    "type": "dimension"
                })
    
    retained_filters = []
    for f in ir.filters:
        if field_to_table.get(f.field) == primary_table:
            retained_filters.append(f)
        else:
            # 尝试映射到主表的同名字段
            field_name = field_to_name.get(f.field)
            if field_name and field_name in primary_table_field_map:
                mapped_id = primary_table_field_map[field_name]
                # 创建新的 filter 条件，使用映射后的字段ID
                mapped_filter = FilterCondition(
                    field=mapped_id,
                    op=f.op,
                    value=f.value
                )
                retained_filters.append(mapped_filter)
                field_mappings.append({
                    "original_field": f.field,
                    "mapped_field": mapped_id,
                    "field_name": field_name,
                    "type": "filter",
                    "value": f.value
                })
            # 如果映射失败，则移除该过滤条件
    
    # 辅助函数：从 metric 项提取字段ID
    def _get_metric_id(m):
        if isinstance(m, str):
            return m
        elif isinstance(m, dict):
            return m.get("field", str(m))
        elif hasattr(m, "field"):
            return m.field
        return str(m)
    
    retained_metrics = [m for m in ir.metrics if field_to_table.get(_get_metric_id(m), primary_table) == primary_table]
    
    removed_dimensions = [d for d in ir.dimensions if d not in retained_dimensions and d not in [m.get("original_field") for m in field_mappings]]
    retained_metric_ids = {_get_metric_id(m) for m in retained_metrics}
    removed_metrics = [m for m in ir.metrics if _get_metric_id(m) not in retained_metric_ids]
    removed_filters = [f.field for f in ir.filters if f not in retained_filters and f.field not in [m.get("original_field") for m in field_mappings]]
    
    # 如果没有任何变化，返回None
    if not (removed_dimensions or removed_metrics or removed_filters or field_mappings):
        return None
    
    # 更新IR
    ir.dimensions = retained_dimensions
    ir.metrics = retained_metrics
    ir.filters = retained_filters
    
    # 日志记录映射信息
    if field_mappings:
        logger.info(
            "跨表字段映射成功",
            primary_table=primary_table,
            mappings=field_mappings
        )
    
    return {
        "removed_metrics": removed_metrics,
        "removed_dimensions": removed_dimensions,
        "removed_filters": removed_filters,
        "field_mappings": field_mappings,
        "primary_table": primary_table,
        "strategy": "force_single_table"
    }


async def compile_with_join_fallback(
    compiler,
    ir: IntermediateRepresentation,
    user_context: Dict[str, Any],
    semantic_model,
    global_rules: Optional[List[Dict[str, Any]]],
    max_retries: int = 2
) -> Tuple[str, List[Dict[str, Any]]]:
    """编译SQL，必要时自动移除跨表指标并重试。"""
    join_fallbacks: List[Dict[str, Any]] = []
    tried_force_single = False

    while True:
        try:
            sql = await compiler.compile_async(ir, user_context, global_rules)
            return sql, join_fallbacks
        except CompilationError as ce:
            # 首先尝试常规fallback
            fallback_info = maybe_fix_join_path_error(ce, ir, semantic_model, global_rules)
            
            # 如果常规fallback失败，尝试强制单表模式
            if not fallback_info and not tried_force_single:
                tried_force_single = True
                fallback_info = force_single_table_fallback(ir, semantic_model, global_rules)
                if fallback_info:
                    logger.warning(
                        "Join路径缺失，强制使用单表模式",
                        primary_table=fallback_info.get("primary_table"),
                        removed_dimensions=fallback_info.get("removed_dimensions"),
                        removed_metrics=fallback_info.get("removed_metrics")
                    )
            
            if not fallback_info or len(join_fallbacks) >= max_retries:
                raise
            
            join_fallbacks.append(fallback_info)
            if fallback_info.get("strategy") != "force_single_table":
                logger.warning(
                    "Join路径缺失，已移除跨表字段",
                    missing_table=fallback_info.get("missing_table"),
                    main_table=fallback_info.get("main_table"),
                    removed_metrics=fallback_info.get("removed_metrics"),
                    removed_dimensions=fallback_info.get("removed_dimensions"),
                    removed_filters=fallback_info.get("removed_filters")
                )

