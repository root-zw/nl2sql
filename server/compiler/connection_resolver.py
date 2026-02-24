"""连接解析器 - 从IR反推目标数据库连接"""

from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import Counter
import structlog

from server.models.ir import IntermediateRepresentation
from server.models.semantic import SemanticModel
from server.exceptions import (
    ConnectionDetectionFailed,
    TableNotFoundError,
    CrossConnectionNotSupported
)
from server.config import RetrievalConfig

logger = structlog.get_logger()


@dataclass
class ConnectionResolutionResult:
    """连接解析结果"""
    status: str  # "single" | "multiple" | "none"
    connection_id: Optional[str] = None  # 主连接（单连接时）
    confidence: float = 0.0  # 置信度 (0-1)
    all_tables_same_connection: bool = False  # 是否所有表在同一连接
    
    # 详细信息
    table_connection_map: Dict[str, str] = field(default_factory=dict)  # {table_id: connection_id}
    candidate_connections: Dict[str, List[str]] = field(default_factory=dict)  # {connection_id: [table_ids]}
    involved_connections: List[str] = field(default_factory=list)  # 涉及的连接ID列表
    
    # 冲突与警告
    conflicts: Optional[List[str]] = None  # 跨连接冲突的表列表
    conflict_details: Optional[str] = None  # 冲突详情
    warning_message: Optional[str] = None  # 警告信息


class ConnectionResolver:
    """连接解析器 - 从IR反推目标连接"""
    
    def __init__(self, semantic_model: SemanticModel):
        self.model = semantic_model
    
    def resolve_connection_from_ir(
        self,
        ir: IntermediateRepresentation,
        hint_connection_id: Optional[str] = None,
        user_specified_connection_id: Optional[str] = None
    ) -> ConnectionResolutionResult:
        """
        从IR解析目标连接
        
        Args:
            ir: 中间表示
            hint_connection_id: 提示连接（来自检索阶段）
            user_specified_connection_id: 用户显式指定的连接
        
        Returns:
            ConnectionResolutionResult
            
        Raises:
            TableNotFoundError: 表未找到
            CrossConnectionNotSupported: 跨连接查询
            ConnectionDetectionFailed: 连接检测失败
        """
        import time
        from server.config import RetrievalConfig
        
        # 检查是否启用指标收集
        metrics_enabled = RetrievalConfig.enable_connection_metrics()
        
        start_time = time.time() if metrics_enabled else None
        
        logger.debug(
            "开始连接解析",
            hint_connection=hint_connection_id,
            user_specified=user_specified_connection_id
        )
        
        # 1. 从IR提取涉及的表
        involved_tables = self._extract_tables_from_ir(ir)
        
        if not involved_tables:
            logger.warning("IR中没有涉及任何表")
            return ConnectionResolutionResult(
                status="none",
                involved_connections=[],
                warning_message="查询中未涉及任何数据表"
            )
        
        logger.debug("提取到涉及的表", tables=list(involved_tables))
        
        # 2. 构建表到连接的映射
        table_connection_map, missing_tables = self._build_table_connection_map(involved_tables)
        
        # 2.1 检查是否有表无法映射
        if missing_tables:
            logger.error("部分表无法映射到连接", missing_tables=missing_tables)
            
            # 记录指标
            if metrics_enabled:
                try:
                    from server.metrics.connection_metrics import table_not_found_errors
                    table_not_found_errors.inc()
                except ImportError:
                    pass
            
            raise TableNotFoundError(
                table_ids=list(missing_tables),
                message=f"查询中引用的部分表在当前数据源中不存在: {', '.join(missing_tables)}"
            )
        
        # 3. 统计连接分布
        connection_counter = Counter(table_connection_map.values())
        candidate_connections = {}
        for conn_id, count in connection_counter.items():
            candidate_connections[conn_id] = [
                table_id for table_id, cid in table_connection_map.items()
                if cid == conn_id
            ]
        
        logger.debug(
            "连接分布统计",
            connection_count=len(candidate_connections),
            distribution={k: len(v) for k, v in candidate_connections.items()}
        )
        
        # 4. 判断场景
        if len(candidate_connections) == 0:
            # 无连接
            return ConnectionResolutionResult(
                status="none",
                table_connection_map=table_connection_map,
                involved_connections=[],
                warning_message="无法确定目标数据库连接"
            )
        
        elif len(candidate_connections) == 1:
            # 单连接 - 理想情况
            final_connection_id = list(candidate_connections.keys())[0]
            conflict_details = None
            warning_message = None

            # 如果用户指定了连接，验证是否一致
            if user_specified_connection_id and user_specified_connection_id != final_connection_id:
                logger.warning(
                    "用户指定连接与推断不一致",
                    user_specified=user_specified_connection_id,
                    inferred=final_connection_id
                )

                resolution_mode = RetrievalConfig.user_specified_conflict_resolution()

                if resolution_mode == "trust_user":
                    final_connection_id = user_specified_connection_id
                    conflict_details = "用户显式指定的连接覆盖了解析结果"
                elif resolution_mode == "smart":
                    # 仅当用户指定的连接出现在候选列表中时才信任用户
                    if user_specified_connection_id in candidate_connections:
                        final_connection_id = user_specified_connection_id
                        conflict_details = "用户指定连接在候选列表中，已采用用户配置"
                    else:
                        conflict_details = "用户指定连接未包含查询涉及的表，已自动切换到表所属连接"
                        warning_message = conflict_details
                else:  # trust_inference 或未知配置
                    conflict_details = "用户指定连接与表所属连接不一致，已使用推断结果"
                    warning_message = conflict_details
            
            result = ConnectionResolutionResult(
                status="single",
                connection_id=final_connection_id,
                confidence=1.0,
                all_tables_same_connection=True,
                table_connection_map=table_connection_map,
                candidate_connections=candidate_connections,
                involved_connections=list(candidate_connections.keys()),
                conflict_details=conflict_details,
                warning_message=warning_message
            )
            
            # 记录指标
            if metrics_enabled:
                try:
                    from server.metrics.connection_metrics import (
                        connection_resolution_total,
                        connection_resolution_duration,
                        connection_resolution_success_rate
                    )
                    
                    connection_resolution_total.labels(
                        status='single',
                        method='ir_inference'
                    ).inc()
                    
                    connection_resolution_success_rate.labels(
                        connection_id=final_connection_id
                    ).inc()
                    
                    if start_time:
                        duration = time.time() - start_time
                        connection_resolution_duration.observe(duration)
                except ImportError:
                    pass
            
            return result
        
        else:
            # 多连接 - 跨库查询
            logger.warning(
                "检测到跨连接查询",
                candidate_connections=list(candidate_connections.keys()),
                table_distribution={k: len(v) for k, v in candidate_connections.items()}
            )
            
            # 根据配置决定处理方式
            from server.config import RetrievalConfig
            handling = RetrievalConfig.cross_connection_handling()
            
            if handling == "reject":
                # MVP阶段：拒绝执行
                # 记录指标
                if metrics_enabled:
                    try:
                        from server.metrics.connection_metrics import (
                            cross_connection_conflicts,
                            connection_resolution_total
                        )
                        cross_connection_conflicts.inc()
                        connection_resolution_total.labels(
                            status='multiple',
                            method='ir_inference'
                        ).inc()
                    except ImportError:
                        pass
                
                raise CrossConnectionNotSupported(
                    candidate_connections=candidate_connections,
                    message="您的查询涉及多个数据库，当前版本暂不支持跨库查询"
                )
            
            elif handling == "auto_select":
                # 自动选择主连接（表数量最多的）
                primary_connection = max(
                    candidate_connections.items(),
                    key=lambda x: len(x[1])
                )[0]
                
                logger.warning(
                    "自动选择主连接",
                    primary_connection=primary_connection,
                    table_count=len(candidate_connections[primary_connection])
                )
                
                return ConnectionResolutionResult(
                    status="multiple",
                    connection_id=primary_connection,
                    confidence=0.5,
                    all_tables_same_connection=False,
                    table_connection_map=table_connection_map,
                    candidate_connections=candidate_connections,
                    involved_connections=list(candidate_connections.keys()),
                    conflicts=list(involved_tables),
                    conflict_details=f"查询涉及{len(candidate_connections)}个数据库，已自动选择主连接",
                    warning_message="查询涉及多个数据库，已自动选择主连接，部分表可能无法访问"
                )
            
            else:  # "warn"
                # 返回多连接状态，由上层决定
                return ConnectionResolutionResult(
                    status="multiple",
                    connection_id=None,
                    confidence=0.0,
                    all_tables_same_connection=False,
                    table_connection_map=table_connection_map,
                    candidate_connections=candidate_connections,
                    involved_connections=list(candidate_connections.keys()),
                    conflicts=list(involved_tables),
                    warning_message=f"查询涉及{len(candidate_connections)}个数据库"
                )
    
    def resolve_with_user_hint(
        self,
        ir: IntermediateRepresentation,
        user_specified_connection_id: Optional[str],
        inferred_connection_id: Optional[str],
        table_connection_map: Dict[str, str]
    ) -> ConnectionResolutionResult:
        """
        结合用户指定和推断结果解析连接
        
        处理用户指定与推断冲突的场景
        """
        # 场景D: 用户指定且推断失败 → 直接使用用户指定
        if user_specified_connection_id and not inferred_connection_id:
            logger.info(
                "推断失败，使用用户指定连接",
                connection_id=user_specified_connection_id
            )
            return ConnectionResolutionResult(
                status="single",
                connection_id=user_specified_connection_id,
                confidence=0.8,
                involved_connections=[user_specified_connection_id],
                conflict_details="推断失败，使用用户指定连接"
            )
        
        # 场景A/B: 用户指定且推断成功 → 需要校验
        if user_specified_connection_id and inferred_connection_id:
            # 检查用户指定的连接是否包含所有表
            user_connection_tables = [
                tid for tid, cid in table_connection_map.items()
                if cid == user_specified_connection_id
            ]
            all_tables = list(table_connection_map.keys())
            
            if set(user_connection_tables) == set(all_tables):
                # 场景A: 用户连接包含所有表，信任用户
                if user_specified_connection_id != inferred_connection_id:
                    logger.warning(
                        "用户指定连接与推断不一致，但包含所有表，使用用户指定",
                        user_connection=user_specified_connection_id,
                        inferred_connection=inferred_connection_id
                    )
                return ConnectionResolutionResult(
                    status="single",
                    connection_id=user_specified_connection_id,
                    confidence=0.9,
                    all_tables_same_connection=True,
                    table_connection_map=table_connection_map,
                    involved_connections=list(set(table_connection_map.values())),
                    conflict_details="用户显式指定了连接（与系统推断不同）"
                )
            else:
                # 场景B: 用户连接不包含部分表，拒绝
                missing_tables = set(all_tables) - set(user_connection_tables)
                raise ConnectionDetectionFailed(
                    f"您选择的数据库连接不包含以下表: {', '.join(missing_tables)}。"
                    f"建议切换到推荐的连接"
                )
        
        # 场景C: 无用户指定，纯推断
        return ConnectionResolutionResult(
            status="single" if inferred_connection_id else "none",
            connection_id=inferred_connection_id,
            confidence=1.0 if inferred_connection_id else 0.0,
            all_tables_same_connection=True if inferred_connection_id else False,
            table_connection_map=table_connection_map,
            involved_connections=list(set(table_connection_map.values())) if table_connection_map else []
        )
    
    def _extract_tables_from_ir(self, ir: IntermediateRepresentation) -> Set[str]:
        """从IR提取涉及的表ID（datasource_id）"""
        tables = set()
        
        # 从指标提取
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
            
            if metric_id in self.model.metrics:
                metric = self.model.metrics[metric_id]
                # 从指标的字段依赖中提取表
                if hasattr(metric, 'field_dependencies'):
                    for dep in metric.field_dependencies:
                        field_id = dep.get('field_id')
                        if field_id and field_id in self.model.fields:
                            field = self.model.fields[field_id]
                            tables.add(field.datasource_id)
        
        # 从维度提取
        for dim_id in ir.dimensions:
            if dim_id in self.model.dimensions:
                dim = self.model.dimensions[dim_id]
                tables.add(dim.table)
            elif dim_id in self.model.fields:
                field = self.model.fields[dim_id]
                tables.add(field.datasource_id)
        
        # 从过滤条件提取
        for filter_cond in ir.filters:
            if filter_cond.field in self.model.dimensions:
                dim = self.model.dimensions[filter_cond.field]
                tables.add(dim.table)
            elif filter_cond.field in self.model.fields:
                field = self.model.fields[filter_cond.field]
                tables.add(field.datasource_id)
        
        # 从排序字段提取
        for order in ir.order_by:
            if order.field in self.model.dimensions:
                dim = self.model.dimensions[order.field]
                tables.add(dim.table)
            elif order.field in self.model.fields:
                field = self.model.fields[order.field]
                tables.add(field.datasource_id)
        
        return tables
    
    def _build_table_connection_map(
        self,
        table_ids: Set[str]
    ) -> Tuple[Dict[str, str], Set[str]]:
        """
        构建表到连接的映射
        
        Returns:
            (table_connection_map, missing_tables)
        """
        table_connection_map = {}
        missing_tables = set()
        
        for table_id in table_ids:
            datasource = self.model.datasources.get(table_id)
            if datasource and datasource.connection_id:
                table_connection_map[table_id] = datasource.connection_id
            else:
                missing_tables.add(table_id)
                logger.warning(
                    "表无法映射到连接",
                    table_id=table_id,
                    exists_in_model=table_id in self.model.datasources
                )
        
        return table_connection_map, missing_tables
