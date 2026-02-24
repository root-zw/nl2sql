"""
权限检查模块

负责校验用户对表和字段的访问权限
"""

from typing import Dict, Any, List, Optional, Set
from uuid import UUID
import structlog

from server.models.ir import IntermediateRepresentation
from server.exceptions import AuthorizationError
from server.services.schema_filter_service import SchemaFilterService
from .stream_emitter import QueryStreamEmitter, stream_progress

logger = structlog.get_logger()


def format_table_names(table_ids: Set[str], semantic_model) -> List[str]:
    """根据语义模型格式化表名"""
    names = []
    for table_id in table_ids:
        datasource = semantic_model.datasources.get(table_id)
        if datasource:
            names.append(
                datasource.display_name
                or datasource.datasource_name
                or datasource.table_name
                or table_id
            )
        else:
            names.append(table_id)
    return names


def format_field_names(field_ids: Set[str], semantic_model) -> List[str]:
    """根据语义模型格式化字段名"""
    names = []
    for field_id in field_ids:
        field = semantic_model.fields.get(field_id)
        if field:
            names.append(field.display_name or field.field_name or field_id)
        else:
            names.append(field_id)
    return names


def collect_ir_field_ids(ir: IntermediateRepresentation, semantic_model) -> Set[str]:
    """收集 IR 中使用到的字段ID
    
    包括：
    - 基础字段：dimensions, metrics, filters 等
    - 条件聚合：conditional_metrics 中的 field 和 condition.field
    - 计算字段：calculated_fields 中的 field_refs, numerator_refs, denominator_refs
    - 占比指标：ratio_metrics 中的 numerator_field, denominator_field 及条件中的字段
    """
    field_ids: Set[str] = set()

    def _add(field_id: Optional[str]):
        if field_id and field_id in semantic_model.fields:
            field_ids.add(field_id)

    # 基础字段
    for dim in ir.dimensions:
        _add(dim)
    for metric_item in ir.metrics:
        # 兼容字符串和 MetricSpec 格式
        if isinstance(metric_item, str):
            _add(metric_item)
        elif isinstance(metric_item, dict):
            _add(metric_item.get("field"))
        elif hasattr(metric_item, "field"):
            _add(metric_item.field)
    for field_id in ir.duplicate_by:
        _add(field_id)
    for field_id in ir.partition_by:
        _add(field_id)
    for field_id in ir.cumulative_metrics:
        _add(field_id)
    for field_id in ir.moving_average_metrics:
        _add(field_id)
    if ir.cumulative_order_by:
        _add(ir.cumulative_order_by)
    if ir.sort_by:
        _add(ir.sort_by)
    for order in ir.order_by:
        _add(order.field)
    for filter_cond in ir.filters:
        _add(filter_cond.field)
    
    # 条件聚合指标 (conditional_metrics)
    for cond_metric in ir.conditional_metrics:
        _add(cond_metric.field)
        if cond_metric.condition:
            _add(cond_metric.condition.field)
    
    # 计算字段 (calculated_fields)
    for calc_field in ir.calculated_fields:
        # 表达式中引用的字段
        for ref in calc_field.field_refs or []:
            _add(ref)
        # 分子引用的字段（比率类指标）
        for ref in calc_field.numerator_refs or []:
            _add(ref)
        # 分母引用的字段（比率类指标）
        for ref in calc_field.denominator_refs or []:
            _add(ref)
    
    # 占比指标 (ratio_metrics)
    for ratio_metric in ir.ratio_metrics:
        _add(ratio_metric.numerator_field)
        _add(ratio_metric.denominator_field)
        # 分子条件中的字段
        if ratio_metric.numerator_condition:
            _add(ratio_metric.numerator_condition.field)
        # 分母条件中的字段
        if ratio_metric.denominator_condition:
            _add(ratio_metric.denominator_condition.field)
    
    # HAVING 过滤条件 (having_filters)
    for having_filter in ir.having_filters:
        _add(having_filter.field)

    return field_ids


def collect_ir_table_ids(
    ir: IntermediateRepresentation,
    semantic_model,
    field_ids: Set[str]
) -> Set[str]:
    """根据字段与显式设置收集 IR 涉及的表ID"""
    table_ids: Set[str] = set()

    for field_id in field_ids:
        field = semantic_model.fields.get(field_id)
        if field and field.datasource_id:
            table_ids.add(str(field.datasource_id))

    if ir.primary_table_id:
        table_ids.add(str(ir.primary_table_id))
    if ir.anti_join_table:
        table_ids.add(str(ir.anti_join_table))

    return table_ids


def _collect_filter_field_ids(ir: IntermediateRepresentation, semantic_model) -> Set[str]:
    """收集 IR 中用于过滤条件的字段ID（用于检查 restricted_filter_columns）"""
    filter_field_ids: Set[str] = set()
    
    def _add(field_id: Optional[str]):
        if field_id and field_id in semantic_model.fields:
            filter_field_ids.add(field_id)
    
    # filters 中的字段
    for filter_cond in ir.filters:
        _add(filter_cond.field)
    
    # having_filters 中的字段
    for having_filter in ir.having_filters:
        _add(having_filter.field)
    
    # conditional_metrics 的条件字段
    for cond_metric in ir.conditional_metrics:
        if cond_metric.condition:
            _add(cond_metric.condition.field)
    
    # ratio_metrics 的条件字段
    for ratio_metric in ir.ratio_metrics:
        if ratio_metric.numerator_condition:
            _add(ratio_metric.numerator_condition.field)
        if ratio_metric.denominator_condition:
            _add(ratio_metric.denominator_condition.field)
    
    return filter_field_ids


def _collect_aggregate_field_ids(ir: IntermediateRepresentation, semantic_model) -> Set[str]:
    """收集 IR 中用于聚合的字段ID（用于检查 restricted_aggregate_columns）"""
    agg_field_ids: Set[str] = set()
    
    def _add(field_id: Optional[str]):
        if field_id and field_id in semantic_model.fields:
            agg_field_ids.add(field_id)
    
    # 只有聚合查询需要检查
    if ir.query_type != "aggregation":
        return agg_field_ids
    
    # metrics 中的字段
    for metric_item in ir.metrics:
        # 兼容字符串和 MetricSpec 格式
        if isinstance(metric_item, str):
            _add(metric_item)
        elif isinstance(metric_item, dict):
            _add(metric_item.get("field"))
        elif hasattr(metric_item, "field"):
            _add(metric_item.field)
    
    # conditional_metrics 中被聚合的字段
    for cond_metric in ir.conditional_metrics:
        _add(cond_metric.field)
    
    # calculated_fields 中引用的字段（会被聚合）
    for calc_field in ir.calculated_fields:
        for ref in calc_field.field_refs or []:
            _add(ref)
    
    # ratio_metrics 中的分子分母字段
    for ratio_metric in ir.ratio_metrics:
        _add(ratio_metric.numerator_field)
        _add(ratio_metric.denominator_field)
    
    # cumulative_metrics
    for field_id in ir.cumulative_metrics:
        _add(field_id)
    
    # moving_average_metrics
    for field_id in ir.moving_average_metrics:
        _add(field_id)
    
    return agg_field_ids


async def enforce_schema_permissions(
    ir: IntermediateRepresentation,
    semantic_model,
    connection_id: str,
    user_id: Optional[str],
    user_role: Optional[str],
    tracer,
    stream: Optional[QueryStreamEmitter]
):
    """确保用户仅能访问有权限的表与字段
    
    检查内容：
    1. 表级权限：用户是否有权访问查询涉及的表
    2. 字段级权限：用户是否有权访问查询涉及的字段
    3. 列级限制：
       - restricted_filter_columns: 禁止在 WHERE/HAVING 中使用的列
       - restricted_aggregate_columns: 禁止聚合的列
    """
    if not user_id:
        return
    if user_role == 'admin':
        # 系统管理员默认拥有全部表权限
        return

    step_name = "表/列权限校验"
    step_desc = "校验角色可访问的表与字段"
    step = tracer.start_step(step_name, "permission", step_desc)
    await stream_progress(stream, step_name, "started", step_desc)

    try:
        try:
            connection_uuid = UUID(connection_id)
            user_uuid = UUID(user_id)
        except ValueError as exc:
            raise AuthorizationError(
                "用户或连接ID格式无效，无法校验数据权限",
                details={"error": str(exc)}
            ) from exc

        from server.utils.db_pool import get_metadata_pool

        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            schema_service = SchemaFilterService(conn)
            allowed_table_ids = await schema_service.get_allowed_table_ids(connection_uuid, user_uuid)
            allowed_field_ids = await schema_service.get_allowed_field_ids(connection_uuid, user_uuid)
            
            # 获取列级权限限制
            column_restrictions = await schema_service.get_column_restrictions(connection_uuid, user_uuid)

        allowed_table_strs = {str(table_id) for table_id in allowed_table_ids if table_id}
        allowed_field_strs = {str(field_id) for field_id in allowed_field_ids if field_id}
        
        # 转换列级限制为字符串集合
        restricted_filter_strs = {str(fid) for fid in column_restrictions.get('restricted_filter_column_ids', set()) if fid}
        restricted_aggregate_strs = {str(fid) for fid in column_restrictions.get('restricted_aggregate_column_ids', set()) if fid}

        if not allowed_table_strs:
            raise AuthorizationError(
                "当前用户未被授予任何表的访问权限，请联系管理员配置表权限。",
                details={"scope": "table"}
            )

        used_field_ids = collect_ir_field_ids(ir, semantic_model)
        used_table_ids = collect_ir_table_ids(ir, semantic_model, used_field_ids)

        # 1. 检查表级权限
        unauthorized_tables = {
            table_id for table_id in used_table_ids if table_id not in allowed_table_strs
        }
        if unauthorized_tables:
            raise AuthorizationError(
                "查询中包含未授权的数据表。",
                details={
                    "tables": format_table_names(unauthorized_tables, semantic_model)
                }
            )

        # 2. 检查字段级权限
        unauthorized_fields = {
            field_id for field_id in used_field_ids if field_id not in allowed_field_strs
        }
        if unauthorized_fields:
            raise AuthorizationError(
                "查询中包含未授权的字段或指标。",
                details={
                    "fields": format_field_names(unauthorized_fields, semantic_model)
                }
            )
        
        # 3. 检查列级限制 - restricted_filter_columns
        if restricted_filter_strs:
            filter_field_ids = _collect_filter_field_ids(ir, semantic_model)
            restricted_filter_used = filter_field_ids & restricted_filter_strs
            if restricted_filter_used:
                raise AuthorizationError(
                    "查询条件中包含禁止过滤的字段。",
                    details={
                        "fields": format_field_names(restricted_filter_used, semantic_model),
                        "restriction_type": "filter"
                    }
                )
        
        # 4. 检查列级限制 - restricted_aggregate_columns
        if restricted_aggregate_strs:
            aggregate_field_ids = _collect_aggregate_field_ids(ir, semantic_model)
            restricted_aggregate_used = aggregate_field_ids & restricted_aggregate_strs
            if restricted_aggregate_used:
                raise AuthorizationError(
                    "查询中包含禁止聚合的字段。",
                    details={
                        "fields": format_field_names(restricted_aggregate_used, semantic_model),
                        "restriction_type": "aggregate"
                    }
                )

        step.set_output({
            "tables_checked": len(used_table_ids),
            "fields_checked": len(used_field_ids),
            "filter_restrictions_checked": len(restricted_filter_strs),
            "aggregate_restrictions_checked": len(restricted_aggregate_strs)
        })
        tracer.end_step()
        await stream_progress(
            stream,
            step_name,
            "success",
            step_desc,
            {
                "tables_checked": len(used_table_ids),
                "fields_checked": len(used_field_ids)
            }
        )
    except AuthorizationError as auth_err:
        step.set_error(auth_err.message)
        tracer.end_step()
        await stream_progress(
            stream,
            step_name,
            "error",
            step_desc,
            {"reason": auth_err.message}
        )
        raise
    except Exception as exc:
        step.set_error(str(exc))
        tracer.end_step()
        await stream_progress(
            stream,
            step_name,
            "warning",
            step_desc,
            {"error": str(exc)}
        )
        raise


async def auto_detect_connection(
    question: Optional[str],
    user_id: str,
    user_role: str,
    domain_id: Optional[str] = None
) -> Optional[str]:
    """
    自动检测应该使用的数据库连接
    
    逻辑：
    1. 如果用户只有一个可访问的连接，直接使用
    2. 如果指定了业务域，使用该业务域关联的连接
    3. 未来可以基于问题内容进行智能匹配
    
    Returns:
        connection_id: 检测到的连接ID，如果无法确定则返回 None
    """
    try:
        from server.utils.db_pool import get_metadata_pool
        pool = await get_metadata_pool()
        
        async with pool.acquire() as conn:
            # 1. 获取用户可访问的连接
            user_uuid = UUID(user_id) if user_id else None
            
            if user_role == 'admin':
                # 管理员可以访问所有连接
                connections = await conn.fetch("""
                    SELECT connection_id, connection_name 
                    FROM database_connections 
                    WHERE is_active = TRUE
                    ORDER BY connection_name
                """)
            else:
                # 检查用户是否有 scope_type='all' 的角色
                has_all = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM user_data_roles udr
                        JOIN data_roles dr ON udr.role_id = dr.role_id
                        WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                        AND dr.is_active = TRUE AND dr.scope_type = 'all'
                    )
                """, user_uuid)
                
                if has_all:
                    connections = await conn.fetch("""
                        SELECT connection_id, connection_name 
                        FROM database_connections 
                        WHERE is_active = TRUE
                        ORDER BY connection_name
                    """)
                else:
                    # 通过表权限获取可访问的连接
                    connections = await conn.fetch("""
                        SELECT DISTINCT dc.connection_id, dc.connection_name
                        FROM user_data_roles udr
                        JOIN data_roles dr ON udr.role_id = dr.role_id
                        JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                        JOIN db_tables t ON rtp.table_id = t.table_id
                        JOIN database_connections dc ON t.connection_id = dc.connection_id
                        WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                        AND dr.is_active = TRUE AND dc.is_active = TRUE
                        ORDER BY dc.connection_name
                    """, user_uuid)
            
            # 2. 如果只有一个连接，直接使用
            if len(connections) == 1:
                return str(connections[0]['connection_id'])
            
            # 3. 如果指定了业务域，尝试从业务域关联的表中确定连接
            if domain_id:
                domain_uuid = UUID(domain_id)
                # 获取该业务域下表最多的连接
                domain_conn = await conn.fetchrow("""
                    SELECT t.connection_id, COUNT(*) as table_count
                    FROM db_tables t
                    WHERE t.domain_id = $1 AND t.is_included = TRUE
                    GROUP BY t.connection_id
                    ORDER BY table_count DESC
                    LIMIT 1
                """, domain_uuid)
                
                if domain_conn:
                    return str(domain_conn['connection_id'])
            
            # 4. 无法自动确定，返回 None
            return None
            
    except Exception as e:
        logger.error("自动检测连接失败", error=str(e))
        return None

