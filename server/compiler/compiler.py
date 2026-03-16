"""SQL 编译器主模块"""

from collections import OrderedDict
from typing import Dict, Set, List, Any, Optional
import structlog
import sqlglot
from sqlglot import exp, parse_one

from server.models.ir import IntermediateRepresentation
from server.models.semantic import SemanticModel, Join
from server.metadata.semantic_graph import SemanticGraph
from server.compiler.planner import JoinPlanner
from server.compiler.ast_builder import ASTBuilder
from server.compiler.dialect_profiles import get_dialect_profile, normalize_db_type
from server.compiler.rules import RulesEngine
from server.compiler.dialect_tsql import TSQLDialect
from server.compiler.dialect_mysql import MySQLDialect
from server.compiler.dialect_postgres import PostgreSQLDialect
from server.exceptions import CompilationError
from server.config import settings

logger = structlog.get_logger()


class SQLCompiler:
    """SQL 编译器 - 将 IR 转换为可执行的 SQL（支持多数据库方言）"""

    def __init__(
        self,
        semantic_model: SemanticModel,
        semantic_graph: SemanticGraph,
        dialect: str = "tsql",
        db_type: str = "sqlserver",
        global_rules_loader=None
    ):
        self.model = semantic_model
        self.graph = semantic_graph
        self.profile = get_dialect_profile(db_type or dialect)
        self.dialect = self.profile.compiler_dialect
        self.db_type = self.profile.db_type

        # 子模块
        self.planner = JoinPlanner(semantic_model, semantic_graph)
        self.ast_builder = ASTBuilder(semantic_model, self.dialect, [], db_type=self.db_type)
        self.rules_engine = RulesEngine(semantic_model, self.dialect, global_rules_loader)

        # 根据数据库类型选择方言处理器
        self.dialect_handler = self._get_dialect_handler(db_type)

    def _get_dialect_handler(self, db_type: str):
        """
        根据数据库类型获取方言处理器

        Args:
            db_type: 数据库类型 (sqlserver/mysql/postgresql)

        Returns:
            对应的方言处理器实例
        """
        db_type = normalize_db_type(db_type)
        handlers = {
            "sqlserver": TSQLDialect,
            "mysql": MySQLDialect,
            "mariadb": MySQLDialect,
            "postgresql": PostgreSQLDialect
        }

        handler_class = handlers.get(db_type, TSQLDialect)
        logger.debug(f"选择方言处理器: {db_type} -> {handler_class.__name__}")
        return handler_class()

    def _get_metric_expression(self, metric) -> Optional[str]:
        """
        统一获取指标的表达式（兼容新旧数据模型）

        优先级：
        0. 保留字（如 __row_count__ 代表 COUNT(*)）
        1. expression（完整SQL表达式，支持单位转换等复杂逻辑）
        2. derived_def（派生指标公式）
        3. atomic_def（简单聚合表达式）
        """
        # 优先0：检查是否为保留字
        if hasattr(metric, 'metric_id') and metric.metric_id == "__row_count__":
            logger.debug("检测到保留字 __row_count__，生成 COUNT(*)")
            return "COUNT(*)"
        
        #  优先1：如果有expression，直接使用（支持单位转换等复杂逻辑）
        if hasattr(metric, 'expression') and metric.expression:
            return metric.expression

        # 优先2：派生指标，使用formula
        if hasattr(metric, 'derived_def') and metric.derived_def:
            return metric.derived_def.formula

        # 优先3：原子指标，根据atomic_def构建聚合表达式
        if hasattr(metric, 'atomic_def') and metric.atomic_def:
            agg = metric.atomic_def.aggregation
            field_id = metric.atomic_def.base_field_id
            if field_id and field_id in self.model.fields:
                field = self.model.fields[field_id]
                # 构建表达式：AGG(datasource.physical_column_name)
                ds_id = field.datasource_id
                column_name = field.column  # 使用column属性（返回物理列名）

                # 按方言转义标识符
                def _q(name: str) -> str:
                    s = str(name)
                    if self.dialect == "tsql":
                        return f"[{s.replace(']', ']]')}]"
                    elif self.dialect == "mysql":
                        return f"`{s.replace('`', '``')}`"
                    elif self.dialect in ("postgres", "postgresql"):
                        return f'"{s}"'
                    return s
                qualified = f"{_q(ds_id)}.{_q(column_name)}"

                # 特殊处理：COUNT_DISTINCT -> COUNT(DISTINCT column)
                if agg == 'COUNT_DISTINCT':
                    return f"COUNT(DISTINCT {qualified})"
                else:
                    return f"{agg}({qualified})"
            elif agg in ['COUNT', 'COUNT_DISTINCT']:
                # COUNT(*) - COUNT_DISTINCT无字段时回退到COUNT(*)
                if agg == 'COUNT_DISTINCT':
                    logger.warning(f"COUNT_DISTINCT without base_field_id, falling back to COUNT(*) for metric {metric.metric_id}")
                return "COUNT(*)"

        return None

    async def compile_async(
        self,
        ir: IntermediateRepresentation,
        user_context: Dict[str, Any],
        global_rules: List[dict] = None
    ) -> str:
        """
        异步编译 IR 为 SQL（支持全局规则传递）

        Args:
            ir: 中间表示
            user_context: 用户上下文 {user_id, tenant_id, role}
            global_rules: 预加载的全局规则（可选）

        Returns:
            可执行的 SQL 字符串

        Raises:
            CompilationError: 编译失败
        """
        # 如果没有传入全局规则，尝试加载
        if global_rules is None and self.rules_engine.global_rules_loader:
            try:
                domain_id = getattr(ir, "domain_id", None)
                domain_sequence = []
                if domain_id:
                    domain_sequence.append(domain_id)
                domain_sequence.append(None)
                domain_sequence = list(OrderedDict.fromkeys(domain_sequence))

                rule_map: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
                for domain in domain_sequence:
                    rules = await self.rules_engine.global_rules_loader.load_active_rules(
                        rule_types=["derived_metric", "custom_instruction", "default_filter"],
                        domain_id=domain
                    )
                    for rule in rules:
                        rid = rule.get("rule_id") or f"anon_{id(rule)}"
                        if rid not in rule_map:
                            rule_map[rid] = rule
                global_rules = list(rule_map.values())
                logger.debug(
                    f"加载了 {len(global_rules)} 条全局规则",
                    domains=[d or "global" for d in domain_sequence]
                )
            except Exception as e:
                logger.warning(f"加载全局规则失败: {e}")
                global_rules = []

        return self.compile(ir, user_context, global_rules or [])

    def compile(
        self,
        ir: IntermediateRepresentation,
        user_context: Dict[str, Any],
        global_rules: List[dict] = None
    ) -> str:
        """
        编译 IR 为 SQL

        Args:
            ir: 中间表示
            user_context: 用户上下文 {user_id, tenant_id, role}

        Returns:
            可执行的 SQL 字符串

        Raises:
            CompilationError: 编译失败
        """
        logger.debug("开始编译 IR 到 SQL", ir=ir.model_dump())

        try:
            # 0. 使用传入的全局规则
            if global_rules is None:
                global_rules = []
                logger.warning("未传入全局规则，派生指标和默认过滤将不可用")

            # 临时存储全局规则供其他方法使用
            self._current_global_rules = global_rules
            # 同步到 ASTBuilder（包含派生指标定义）
            if hasattr(self.ast_builder, "set_global_rules"):
                self.ast_builder.set_global_rules(global_rules)

            # 判断是否为 multi_join 模式（多表关联查询）
            # multi_join 模式下，表之间没有预定义的外键关系，由 ASTBuilder 直接构建 INNER JOIN
            is_multi_join_mode = (
                ir.cross_partition_query 
                and ir.selected_table_ids 
                and len(ir.selected_table_ids) > 1 
                and ir.cross_partition_mode == "multi_join"
            )

            # 0.5 跨表一致性守卫（multi_join 模式跳过，因为这些表没有预定义的外键关系）
            if not is_multi_join_mode:
                consistency_result = self._validate_table_consistency(ir)
                if not consistency_result.get("valid", True):
                    # 根据配置决定处理方式
                    from server.config import RetrievalConfig
                    if RetrievalConfig.compiler_cross_table_guard_strict():
                        raise CompilationError(
                            f"跨表一致性检查失败: {consistency_result.get('error_message', '未知错误')}"
                        )
                    else:
                        logger.warning(
                            "跨表一致性检查警告",
                            details=consistency_result
                        )
            else:
                logger.debug(
                    "multi_join 模式，跳过跨表一致性检查",
                    selected_table_ids=ir.selected_table_ids,
                    cross_partition_mode=ir.cross_partition_mode
                )

            # 1. 路径规划（multi_join 模式跳过，由 ASTBuilder 直接处理）
            join_path = []
            if not is_multi_join_mode:
                involved_tables = self._get_involved_tables(ir)
                logger.debug("涉及的表", tables=list(involved_tables))

                #  传入JOIN策略和参照表
                join_path = self.planner.plan_join_path(
                    involved_tables,
                    ir.join_strategy,
                    ir.anti_join_table
                )
                logger.debug("Join 路径规划完成", path_length=len(join_path), strategy=ir.join_strategy)
            else:
                logger.debug(
                    "multi_join 模式，跳过标准 JOIN 路径规划",
                    table_count=len(ir.selected_table_ids)
                )

            # 2. 构建基础 AST（传递全局规则）
            ast = self.ast_builder.build(ir, join_path, global_rules)
            logger.debug("基础 AST 构建完成")

            # 3. 规则注入
            # 传递全局规则给规则引擎
            self.rules_engine._current_global_rules = global_rules
            ast = self.rules_engine.apply_all(ast, ir, user_context)
            logger.debug("规则注入完成")

            # 4. 方言转换
            sql = ast.sql(dialect=self.dialect, pretty=True)

            # 5. 数据库特定处理
            sql = self.dialect_handler.add_unicode_prefix(sql)

            if self.db_type == "sqlserver":
                logger.debug("已应用 SQL Server 特定处理（中文字符串 N 前缀）")
            elif self.profile.is_mysql_family:
                logger.debug("已应用 MySQL/MariaDB 特定处理", db_type=self.db_type)
            elif self.db_type == "postgresql":
                logger.debug("已应用 PostgreSQL 特定处理")

            logger.debug("SQL 编译完成", sql_length=len(sql), db_type=self.db_type, dialect=self.dialect)

            return sql

        except Exception as e:
            logger.error("SQL 编译失败", error=str(e))
            raise CompilationError(f"SQL 编译失败: {str(e)}")

    def _get_involved_tables(self, ir: IntermediateRepresentation) -> Set[str]:
        """解析 IR 中涉及的所有表"""
        tables = set()

        # 从指标表达式提取表名
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
            
            # 处理保留字：__row_count__ 不需要从 model 中查找，直接跳过
            if metric_id == "__row_count__":
                logger.debug("检测到保留字 __row_count__，跳过表名提取（将使用主表）")
                continue
            
            #  处理派生指标（从依赖字段提取表）
            if isinstance(metric_id, str) and metric_id.startswith('derived:'):
                derived_name = metric_id[8:]  # 移除"derived:"前缀
                logger.debug(f"处理派生指标: {derived_name}")
                logger.debug(f"全局规则数量: {len(getattr(self, '_current_global_rules', []))}")
                
                # 从全局规则中查找派生指标定义（支持同义词匹配）
                found = False
                matched_rule_def = None
                matched_display_name = None
                
                for rule in getattr(self, '_current_global_rules', []):
                    if rule.get('rule_type') == 'derived_metric':
                        rule_def = rule.get('rule_definition', {})
                        display_name = rule_def.get('display_name', rule.get('rule_name', '').replace('（派生）', ''))
                        synonyms = rule_def.get('synonyms', []) or []
                        
                        # 检查直接匹配或同义词匹配
                        if display_name == derived_name or derived_name in synonyms:
                            found = True
                            matched_rule_def = rule_def
                            matched_display_name = display_name
                            logger.debug(f" 找到派生指标: {derived_name} -> {display_name}")
                            break
                        
                        # 模糊匹配：检查包含关系
                        if display_name in derived_name or derived_name in display_name:
                            found = True
                            matched_rule_def = rule_def
                            matched_display_name = display_name
                            logger.debug(f" 模糊匹配派生指标: {derived_name} -> {display_name}")
                            break
                
                if found and matched_rule_def:
                    # 从依赖字段中提取表
                    field_deps = matched_rule_def.get('field_dependencies', [])

                    #  处理无依赖字段的派生指标（如COUNT(*)）
                    # 这类指标不依赖具体字段，而是对整个查询结果集的聚合
                    # 表名将从其他指标、维度或过滤条件中推断
                    if not field_deps:
                        logger.debug(f"派生指标 {matched_display_name} 无字段依赖（如COUNT(*)），表名将从查询上下文推断")
                    else:
                        # 有依赖字段的派生指标：从依赖字段提取表
                        for dep in field_deps:
                            field_id = dep.get('field_id')
                            if field_id and field_id in self.model.measures:
                                measure = self.model.measures[field_id]
                                tables.add(measure.table)
                                logger.debug(f"从派生指标 {matched_display_name} 的依赖字段 {field_id} 提取表: {measure.table}")
                            elif field_id and field_id in self.model.fields:
                                # 新架构：从统一字段表查找
                                field = self.model.fields[field_id]
                                tables.add(field.datasource_id)
                                logger.debug(f"从派生指标 {matched_display_name} 的依赖字段 {field_id} 提取表: {field.datasource_id}")
                else:
                    logger.warning(f"未找到派生指标: {derived_name}，将尝试使用通用 COUNT(*) 处理")

                # 派生指标不阻止后续处理，继续检查其他来源
                continue

            # 职责明确：IR 中的 metric_id 必须是有效的 UUID
            # 如果在 metrics 中找不到，检查是否在 measures 或统一字段表中
            if metric_id not in self.model.metrics:
                # 检查 measures（度量字段）
                if metric_id in self.model.measures:
                    measure = self.model.measures[metric_id]
                    tables.add(measure.table)
                    logger.debug(f"从 measures 中提取表: {metric_id} -> {measure.table}")
                    continue
                # 检查统一字段表
                elif metric_id in self.model.fields:
                    field = self.model.fields[metric_id]
                    tables.add(field.datasource_id)
                    logger.debug(f"从统一字段表提取表: {metric_id} -> {field.datasource_id}")
                    continue
                else:
                    raise CompilationError(f"指标不存在: {metric_id}（IR 必须使用有效的字段 UUID）")

            metric = self.model.metrics[metric_id]
            # 简单提取：查找 source_id.column_name 模式
            # 派生指标可能没有 expression，跳过
            expression = self._get_metric_expression(metric)
            if not expression:
                continue
            for source_id in self.model.sources.keys():
                if source_id in expression:
                    tables.add(source_id)

        # 从维度提取
        for dim_id in ir.dimensions:
            if dim_id not in self.model.dimensions:
                # 从统一字段表中查找
                if dim_id in self.model.fields:
                    field = self.model.fields[dim_id]
                    tables.add(field.datasource_id)
                    logger.debug(f"从统一字段表提取维度的表: {dim_id} -> {field.datasource_id}")
                    continue
                raise CompilationError(f"维度不存在: {dim_id}（IR 必须使用有效的字段 UUID）")

            dim = self.model.dimensions[dim_id]
            tables.add(dim.table)

        # 从过滤条件提取
        for filter_cond in ir.filters:
            if filter_cond.field in self.model.dimensions:
                dim = self.model.dimensions[filter_cond.field]
                tables.add(dim.table)
            elif filter_cond.field in self.model.fields:
                #  新架构支持：从统一字段表查找
                field = self.model.fields[filter_cond.field]
                tables.add(field.datasource_id)
                logger.debug(f"从统一字段表提取过滤字段的表: {filter_cond.field} -> {field.datasource_id}")

        #  处理反向匹配：添加参照表
        if ir.join_strategy in ["left_unmatched", "right_unmatched"]:
            if ir.anti_join_table:
                # 优先使用LLM明确指定的表
                tables.add(ir.anti_join_table)
                logger.debug(
                    "使用显式指定的反向匹配参照表",
                    strategy=ir.join_strategy,
                    anti_join_table=ir.anti_join_table
                )
            else:
                # Fallback: 自动推断（仅在只有一个JOIN关系时）
                inferred_table = self._infer_anti_join_table(tables, ir.join_strategy)
                if inferred_table:
                    tables.add(inferred_table)
                    logger.warning(
                        "未指定anti_join_table，自动推断参照表",
                        strategy=ir.join_strategy,
                        inferred_table=inferred_table,
                        suggestion="建议在IR中明确指定anti_join_table字段"
                    )
                else:
                    logger.error(
                        "无法推断反向匹配参照表",
                        strategy=ir.join_strategy,
                        current_tables=list(tables),
                        available_joins=len(self.model.joins)
                    )

        # 从排序字段提取（用于明细查询）
        if ir.order_by:
            for order_item in ir.order_by:
                if order_item.field in self.model.dimensions:
                    dim = self.model.dimensions[order_item.field]
                    tables.add(dim.table)
                elif order_item.field in self.model.measures:
                    measure = self.model.measures[order_item.field]
                    tables.add(measure.table)

        # 如果仍然没有表（通常是明细查询场景），根据 IR 提示和业务域选择默认主表
        if not tables:
            logger.warning("无法从IR推断涉及的表，尝试根据IR提示和业务域选择默认主表")

            default_table: Optional[str] = None

            # 1. 优先使用 IR 中的主表提示（来自表级检索 Top1）
            try:
                primary_table_id = getattr(ir, "primary_table_id", None)
                if primary_table_id and getattr(self.model, "sources", None):
                    if primary_table_id in self.model.sources:
                        default_table = primary_table_id
                        logger.debug(
                            "根据IR的primary_table_id选择默认主表",
                            table=default_table,
                        )
            except Exception as e:
                logger.exception(
                    "按IR的primary_table_id选择默认主表失败，将尝试按业务域选择",
                    error=str(e),
                )

            # 2. 若没有主表提示或提示无效，再根据 IR 中的业务域（domain_id）在 sources 中筛选同域表
            try:
                domain_id = getattr(ir, "domain_id", None)
                if not default_table and domain_id and getattr(self.model, "sources", None):
                    for table_id, source in self.model.sources.items():
                        if getattr(source, "domain_id", None) == domain_id:
                            default_table = table_id
                            logger.debug(
                                "根据IR的业务域选择默认主表",
                                domain_id=domain_id,
                                table=default_table,
                            )
                            break
            except Exception as e:
                logger.exception(
                    "按业务域从 sources 选择默认主表失败，将回退到全局默认",
                    error=str(e),
                )

            # 3. 若按主表提示和业务域都未选出表，则回退到原有逻辑：使用第一个 source
            if not default_table and getattr(self.model, "sources", None):
                default_table = list(self.model.sources.keys())[0]
                logger.debug(
                    "按业务域未找到默认主表，回退到全局默认主表",
                    table=default_table,
                )

            # 4. 极端情况下（sources 也为空），尝试从 datasources 中选一个表
            if not default_table and getattr(self.model, "datasources", None):
                default_table = next(iter(self.model.datasources.keys()))
                logger.warning(
                    "sources 为空，从 datasources 中选择默认主表",
                    table=default_table,
                )

            if not default_table:
                # 彻底没有可用表，直接抛出编译错误，避免生成误导性的 SQL
                logger.error("语义模型中没有可用的数据源，无法选择默认主表")
                raise CompilationError("语义模型中没有可用的数据源，无法选择默认主表")

            tables.add(default_table)

        return tables

    def _validate_table_consistency(
        self,
        ir: IntermediateRepresentation
    ) -> Dict[str, Any]:
        """
        验证 IR 中 filters/metrics/dimensions 的表一致性
        
        检查所有字段是否来自同一表或有有效的 JOIN 路径
        
        Args:
            ir: 中间表示
            
        Returns:
            {
                "valid": bool,
                "error_message": str | None,
                "inconsistent_fields": [...] | None,
                "suggested_action": str | None
            }
        """
        from server.config import RetrievalConfig
        
        # 检查是否启用
        if not RetrievalConfig.compiler_cross_table_guard_enabled():
            return {"valid": True}
        
        # 收集所有字段及其所属表
        field_table_map: Dict[str, str] = {}  # field_id -> table_id
        inconsistent_fields: List[Dict[str, Any]] = []
        
        # 从 metrics 收集
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
            
            if isinstance(metric_id, str) and metric_id.startswith('derived:'):
                continue  # 跳过派生指标
            
            table_id = None
            if metric_id in self.model.metrics:
                metric = self.model.metrics[metric_id]
                # 尝试从 expression 推断表
                expression = self._get_metric_expression(metric)
                if expression:
                    for source_id in self.model.sources.keys():
                        if source_id in expression:
                            table_id = source_id
                            break
            elif metric_id in self.model.measures:
                table_id = self.model.measures[metric_id].table
            elif metric_id in self.model.fields:
                table_id = self.model.fields[metric_id].datasource_id
            
            if table_id:
                field_table_map[metric_id] = table_id
        
        # 从 dimensions 收集
        for dim_id in ir.dimensions:
            if dim_id in self.model.dimensions:
                table_id = self.model.dimensions[dim_id].table
                field_table_map[dim_id] = table_id
            elif dim_id in self.model.fields:
                table_id = self.model.fields[dim_id].datasource_id
                field_table_map[dim_id] = table_id
        
        # 从 filters 收集
        for filter_cond in ir.filters:
            field_id = filter_cond.field
            table_id = None
            
            if field_id in self.model.dimensions:
                table_id = self.model.dimensions[field_id].table
            elif field_id in self.model.fields:
                table_id = self.model.fields[field_id].datasource_id
            
            if table_id:
                field_table_map[field_id] = table_id
        
        if not field_table_map:
            return {"valid": True}
        
        # 检查所有字段是否来自同一表
        unique_tables = set(field_table_map.values())
        
        if len(unique_tables) <= 1:
            # 所有字段来自同一表，一致性通过
            return {"valid": True}
        
        # 多表情况：检查是否有有效的 JOIN 路径
        # 构建可达表集合
        reachable_tables = set()
        tables_to_check = list(unique_tables)
        
        if tables_to_check:
            # 从第一个表开始 BFS
            start_table = tables_to_check[0]
            queue = [start_table]
            reachable_tables.add(start_table)
            
            while queue:
                current = queue.pop(0)
                for join in self.model.joins:
                    if join.from_table == current and join.to_table not in reachable_tables:
                        reachable_tables.add(join.to_table)
                        queue.append(join.to_table)
                    elif join.to_table == current and join.from_table not in reachable_tables:
                        reachable_tables.add(join.from_table)
                        queue.append(join.from_table)
        
        # 检查是否所有表都可达
        unreachable = unique_tables - reachable_tables
        
        if unreachable:
            # 存在不可达的表，收集不一致的字段
            for field_id, table_id in field_table_map.items():
                if table_id in unreachable:
                    inconsistent_fields.append({
                        "field_id": field_id,
                        "table_id": table_id,
                        "reason": "表无法通过 JOIN 连接到主表"
                    })
            
            return {
                "valid": False,
                "error_message": f"跨表字段无法通过 JOIN 连接: 表 {unreachable} 与其他表没有关联",
                "inconsistent_fields": inconsistent_fields,
                "involved_tables": list(unique_tables),
                "reachable_tables": list(reachable_tables),
                "suggested_action": "请检查过滤条件是否选择了正确的表，或添加缺失的 JOIN 关系"
            }
        
        # 所有表可达，但需要 JOIN
        logger.debug(
            "跨表查询检测通过",
            tables=list(unique_tables),
            join_required=len(unique_tables) > 1
        )
        
        return {
            "valid": True,
            "tables_involved": list(unique_tables),
            "join_required": len(unique_tables) > 1
        }

    def _infer_anti_join_table(
        self,
        current_tables: Set[str],
        strategy: str
    ) -> Optional[str]:
        """
        智能推断反向匹配的参照表（Fallback机制）

        规则：
        1. 查找所有与current_tables相关的JOIN
        2. 如果只有一个候选，返回该表
        3. 如果有多个候选，返回None（需要LLM明确指定）

        Args:
            current_tables: 当前已涉及的表集合
            strategy: JOIN策略

        Returns:
            推断出的参照表ID，如果无法推断则返回None
        """
        candidates = set()

        # 遍历所有JOIN关系，找到与当前表相关的其他表
        for join in self.model.joins:
            for table in current_tables:
                if join.from_table == table:
                    candidates.add(join.to_table)
                elif join.to_table == table:
                    candidates.add(join.from_table)

        # 移除已在current_tables中的表
        candidates -= current_tables

        if len(candidates) == 1:
            # 唯一候选，可以安全推断
            return list(candidates)[0]
        elif len(candidates) > 1:
            # 多个候选，无法推断
            logger.warning(
                "存在多个可能的JOIN参照表，无法自动推断",
                candidates=list(candidates),
                hint="需要在IR中明确指定anti_join_table"
            )
            return None
        else:
            # 没有候选
            return None
