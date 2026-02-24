"""规则注入引擎"""

from typing import Dict, Any, List
import structlog
from sqlglot import exp, parse_one

from server.models.semantic import SemanticModel
from server.models.ir import IntermediateRepresentation
from server.config import settings

logger = structlog.get_logger()


class RulesEngine:
    """规则注入引擎 - 应用系统级规则到 SQL"""

    def __init__(self, semantic_model: SemanticModel, dialect: str = "tsql", global_rules_loader=None):
        self.model = semantic_model
        self.dialect = dialect
        self.global_rules_loader = global_rules_loader
        self._current_global_rules = None  # 缓存当前编译使用的全局规则

    def apply_all(
        self,
        ast: exp.Expression,
        ir: IntermediateRepresentation,
        user_context: Dict[str, Any]
    ) -> exp.Expression:
        """
        应用所有规则

        Args:
            ast: SQL AST（Select 或 Union）
            ir: 中间表示
            user_context: 用户上下文

        Returns:
            修改后的 AST
        """
        logger.debug("开始应用规则")

        # 标准Select查询
        if not isinstance(ast, exp.Select):
            logger.warning(f"未知的AST类型: {type(ast)}")
            return ast

        # 默认过滤（从指标定义）
        ast = self.inject_default_filters(ast, ir)

        # 确保有 LIMIT（但with_total查询不需要）
        if ir.with_total:
            logger.debug("with_total查询不应用LIMIT限制（已在子查询中处理排序）")
        else:
            ast = self.inject_default_limit(ast, ir)

        logger.debug("规则应用完成")
        return ast

    def inject_default_filters(
        self,
        ast: exp.Select,
        ir: IntermediateRepresentation
    ) -> exp.Select:
        """
        注入默认过滤条件

        1. 从指标的 default_filters 注入（聚合查询）
        2. 从数据库中的数据质量规则注入（配置化）
        3. 从数据源的 detail_view.default_filters 注入（向后兼容）

        特殊处理：
        - with_total=True 现在使用 ROLLUP 优化（SQL Server 2012+），是普通查询，无需特殊处理
        - 对于窗口函数包装查询（window_detail），默认过滤需要注入到子查询内部
        """
        #  检测是否为窗口函数包装查询（window_detail 或 duplicate_detection）
        # 特征：FROM 子句是一个子查询，且子查询包含窗口函数
        is_window_wrapper = self._is_window_wrapper_query(ast)
        
        if is_window_wrapper:
            logger.debug("检测到窗口函数包装查询（window_detail场景），将默认过滤注入到子查询内部")
            return self._inject_filters_into_window_subquery(ast, ir)

        default_filter_conditions = []
        seen_filters = set()  # 用于去重

        # 1. 从指标注入默认过滤（聚合查询）
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
            
            metric = self.model.metrics.get(metric_id)
            if metric and metric.default_filters:
                for filter_str in metric.default_filters:
                    # 去重：检查是否已经添加过相同的过滤条件
                    if filter_str in seen_filters:
                        logger.debug(f"跳过重复的默认过滤: {filter_str}")
                        continue

                    try:
                        filter_expr = parse_one(filter_str, dialect=self.dialect)
                        default_filter_conditions.append(filter_expr)
                        seen_filters.add(filter_str)
                        logger.debug(f"注入默认过滤: {filter_str}")
                    except Exception as e:
                        logger.error(f"解析默认过滤条件失败: {filter_str}", error=str(e))

        # 2.  从数据库中的数据质量规则注入（配置化）
        data_quality_filters = self._get_data_quality_filters(ast, ir)
        for filter_str in data_quality_filters:
            if filter_str in seen_filters:
                continue
            try:
                filter_expr = parse_one(filter_str, dialect=self.dialect)
                default_filter_conditions.append(filter_expr)
                seen_filters.add(filter_str)
                logger.debug(f"数据质量规则：注入过滤 {filter_str}")
            except Exception as e:
                logger.error("解析数据质量过滤失败", error=str(e), filter=filter_str)

        # 2.5  从全局规则（global_rules表）注入默认过滤
        global_default_filters = self._get_global_default_filters(ast, ir)
        for filter_str in global_default_filters:
            if filter_str in seen_filters:
                continue
            try:
                filter_expr = parse_one(filter_str, dialect=self.dialect)
                default_filter_conditions.append(filter_expr)
                seen_filters.add(filter_str)
                logger.debug(f"全局规则：注入默认过滤 {filter_str}")
            except Exception as e:
                logger.error("解析全局默认过滤失败", error=str(e), filter=filter_str)

        # 3. 从数据源的默认过滤注入（向后兼容，优先级低于数据质量规则）
        if ir.query_type == "detail":
            # 检测涉及的表
            from_tables = self._extract_tables_from_ast(ast)

            # 从数据源的 detail_view.default_filters 注入
            for table_alias in from_tables:
                source = self.model.sources.get(table_alias)
                if not source or not getattr(source, "detail_view", None):
                    continue
                for filter_str in getattr(source.detail_view, "default_filters", []) or []:
                    if filter_str in seen_filters:
                        continue
                    try:
                        filter_expr = parse_one(filter_str, dialect=self.dialect)
                        default_filter_conditions.append(filter_expr)
                        seen_filters.add(filter_str)
                        logger.debug(f"明细查询：注入模型默认过滤: {filter_str}")
                    except Exception as e:
                        logger.error("解析模型默认过滤失败", error=str(e), filter=filter_str)

        # 合并到 WHERE（带去重）
        if default_filter_conditions:
            existing_where = ast.args.get("where")

            if existing_where:
                # 展平已有条件
                existing_conds = self._flatten_conditions(existing_where.this)

                # 去重：只添加不存在的条件
                new_conds = []
                for new_cond in default_filter_conditions:
                    if not self._condition_exists(new_cond, existing_conds):
                        new_conds.append(new_cond)
                    else:
                        logger.debug(f"跳过重复条件: {new_cond.sql()}")

                if new_conds:
                    # 重新构建WHERE，避免嵌套
                    # 关键：先删除原WHERE，再用完整条件重建
                    all_conditions = existing_conds + new_conds
                    ast.set("where", None)  # 清除原WHERE
                    ast = ast.where(exp.and_(*all_conditions))
            else:
                ast = ast.where(exp.and_(*default_filter_conditions))

        return ast

    def _get_data_quality_filters(
        self,
        ast: exp.Select,
        ir: IntermediateRepresentation
    ) -> List[str]:
        """
        从数据库中的数据质量规则获取过滤条件

        Returns:
            过滤条件字符串列表（如 ["f_public_deal.approvestate = '已审核'"]）
        """
        filters = []

        # 检查是否配置了数据质量规则
        if not hasattr(self.model, 'rules') or not self.model.rules:
            return filters
        if not hasattr(self.model.rules, 'data_quality_rules') or not self.model.rules.data_quality_rules:
            return filters

        dq_rules = self.model.rules.data_quality_rules
        if not isinstance(dq_rules, dict) or not dq_rules.get('enabled'):
            return filters

        # 获取默认记录过滤规则
        default_filter = dq_rules.get('default_record_filter')
        if not default_filter:
            return filters

        # 检查是否应用到当前查询类型
        apply_to = default_filter.get('apply_to', {})
        is_detail = ir.query_type in ["detail", "window_detail", "duplicate_detection"]
        #  优先判断 query_type，避免被错误的 dimensions 误导
        is_aggregation = (ir.query_type == "aggregation") or (
            ir.query_type not in ["detail", "window_detail", "duplicate_detection"] and (bool(ir.metrics) or bool(ir.dimensions))
        )

        should_apply = False
        if is_detail and apply_to.get('detail_queries'):
            should_apply = True
        elif is_aggregation and apply_to.get('aggregation_queries'):
            should_apply = True

        if not should_apply:
            logger.debug(f"数据质量规则不适用于当前查询类型 (detail={is_detail}, agg={is_aggregation})")
            return filters

        # 获取涉及的表
        from_tables = self._extract_tables_from_ast(ast)

        #  支持白名单模式（优先）和黑名单模式
        apply_to_tables = default_filter.get('apply_to_tables', [])  # 白名单
        exception_tables = default_filter.get('exception_tables', [])  # 黑名单

        # 为每个表生成过滤条件
        field_name = default_filter.get('field_name')
        operator = default_filter.get('operator', '=')
        value = default_filter.get('value')

        for table_alias in from_tables:
            #  白名单模式：只对白名单中的表应用过滤
            if apply_to_tables:
                if table_alias not in apply_to_tables:
                    logger.debug(f"表 {table_alias} 不在白名单中，跳过数据质量过滤")
                    continue
            # 黑名单模式：对不在黑名单中的表应用过滤
            elif table_alias in exception_tables:
                logger.debug(f"表 {table_alias} 在例外列表中，跳过数据质量过滤")
                continue

            # 检查表是否包含该字段
            source = self.model.sources.get(table_alias)
            if not source:
                continue

            # 检查字段是否存在于表中
            has_field = False
            for col in source.columns:
                if col.name == field_name:
                    has_field = True
                    break

            if not has_field:
                logger.debug(f"表 {table_alias} 没有字段 {field_name}，跳过数据质量过滤")
                continue

            # 生成过滤条件
            filter_str = f"{table_alias}.{field_name} {operator} '{value}'"
            filters.append(filter_str)
            logger.debug(f"生成数据质量过滤: {filter_str}")

        return filters

    def _get_global_default_filters(
        self,
        ast: exp.Select,
        ir: IntermediateRepresentation
    ) -> List[str]:
        """
        从全局规则表（global_rules）获取默认过滤条件

        Returns:
            过滤条件字符串列表（如 ["orders.status = '已审核'"]）
        """
        filters = []

        # 优先使用缓存的全局规则
        if self._current_global_rules is not None:
            global_rules = [r for r in self._current_global_rules if r.get('rule_type') == 'default_filter']
        elif self.global_rules_loader:
            # 如果没有缓存，尝试加载（但应该避免到这里）
            logger.warning("未使用缓存的全局规则，正在重新加载（可能影响性能）")
            return filters  # 暂时返回空，避免event loop问题
        else:
            return filters

        if not global_rules:
            return filters

        try:

            # 获取涉及的表ID（从语义模型）
            from_tables = self._extract_tables_from_ast(ast)
            logger.debug(f"全局默认过滤：从AST提取的表列表: {from_tables}")
            logger.debug(f"全局默认过滤：语义模型中的数据源数量: {len(self.model.sources)}")

            # 将表别名映射到表ID
            # from_tables中的值是SQL中使用的表别名（通常是table_name）
            table_alias_to_id = {}
            for table_alias in from_tables:
                found_source = None

                # 方法1: 尝试直接用datasource_id获取（table_alias可能就是UUID）
                if table_alias in self.model.sources:
                    found_source = self.model.sources[table_alias]
                    logger.debug(f"方法1成功：表别名 {table_alias} 直接匹配datasource_id")

                # 方法2: 通过table_name匹配（最常见）
                if not found_source:
                    for datasource_id, src in self.model.sources.items():
                        src_table_name = src.table_name
                        if src_table_name == table_alias:
                            found_source = src
                            logger.debug(f"方法2成功：表别名 {table_alias} 匹配 table_name")
                            break

                # 方法3: 通过display_name匹配
                if not found_source:
                    for datasource_id, src in self.model.sources.items():
                        if src.display_name == table_alias:
                            found_source = src
                            logger.debug(f"方法3成功：表别名 {table_alias} 匹配 display_name")
                            break

                if found_source:
                    # 使用datasource_id作为table_id
                    table_alias_to_id[table_alias] = found_source.datasource_id
                    logger.debug(f"表别名映射成功: {table_alias} -> {found_source.datasource_id[:8]}...")
                else:
                    # 打印所有可用的 table_name 帮助诊断
                    available_table_names = [src.table_name for src in self.model.sources.values()]
                    logger.warning(f"未找到表别名对应的source: {table_alias!r}, 可用table_name: {available_table_names[:5]}...")

            # 为每个表应用对应的默认过滤规则
            for rule in global_rules:
                rule_def = rule.get('rule_definition', {})
                table_id = rule_def.get('table_id')
                filter_field = rule_def.get('filter_field')
                filter_operator = rule_def.get('filter_operator', '=')
                filter_value = rule_def.get('filter_value')

                logger.debug(f"处理规则: {rule.get('rule_name')}, table_id={table_id[:8] if table_id else 'N/A'}...")

                if not all([table_id, filter_field, filter_value]):
                    logger.warning(f"全局默认过滤规则配置不完整: {rule.get('rule_name')}")
                    continue

                # 查找对应的表别名
                matched_alias = None
                for alias, tid in table_alias_to_id.items():
                    logger.debug(f"比较: rule.table_id={table_id[:8]}... vs mapped.tid={tid[:8]}...")
                    if str(tid) == str(table_id):
                        matched_alias = alias
                        logger.debug(f"找到匹配的表: {alias}")
                        break

                if not matched_alias:
                    logger.warning(f"规则指定的表 {table_id[:8]}... 不在当前查询中（候选: {list(table_alias_to_id.values())}）")
                    continue

                # 检查表是否包含该字段（支持显示名和列名）
                # 注意：matched_alias是table_name，需要通过它找到source
                source = None
                for datasource_id, src in self.model.sources.items():
                    if src.table_name == matched_alias or datasource_id == matched_alias:
                        source = src
                        break

                if not source:
                    logger.warning(f"无法获取表{matched_alias}的source对象")
                    continue

                logger.debug(f"开始查找字段: {filter_field} 在表 {matched_alias}")

                # 查找字段（优先通过显示名，其次通过列名）
                field_column_name = None
                for field_id, field in self.model.fields.items():
                    # 匹配显示名
                    if field.display_name == filter_field:
                        # 获取字段的column_id（尝试多个可能的属性名）
                        field_col_id = (
                            getattr(field, 'physical_column_id', None) or
                            getattr(field, 'source_column_id', None) or
                            getattr(field, 'column_id', None)
                        )
                        if not field_col_id:
                            logger.debug(f"字段 {filter_field} 没有column_id相关属性")
                            continue

                        # 检查字段的列是否属于当前表
                        for col in source.columns:
                            if col.column_id == field_col_id:
                                field_column_name = col.column_name if hasattr(col, 'column_name') else col.name
                                logger.debug(f"通过显示名找到字段: {filter_field} -> 列名: {field_column_name}")
                                break
                        if field_column_name:
                            break

                # 如果没找到，尝试直接匹配列名
                if not field_column_name:
                    for col in source.columns:
                        col_name = col.column_name if hasattr(col, 'column_name') else getattr(col, 'name', None)
                        if col_name == filter_field:
                            field_column_name = col_name
                            logger.debug(f"通过列名找到字段: {filter_field}")
                            break

                if not field_column_name:
                    logger.warning(f"表 {matched_alias} 没有字段 {filter_field}，跳过全局默认过滤")
                    continue

                # 生成过滤条件（使用实际列名）
                if filter_operator in ['IN', 'NOT IN']:
                    # IN操作符，filter_value应该是列表
                    if isinstance(filter_value, list):
                        value_str = "(" + ", ".join([f"N'{v}'" for v in filter_value]) + ")"
                    else:
                        value_str = f"(N'{filter_value}')"
                    filter_str = f"[{matched_alias}].[{field_column_name}] {filter_operator} {value_str}"
                elif filter_operator in ['IS NULL', 'IS NOT NULL']:
                    filter_str = f"[{matched_alias}].[{field_column_name}] {filter_operator}"
                else:
                    # 普通操作符
                    filter_str = f"[{matched_alias}].[{field_column_name}] {filter_operator} N'{filter_value}'"

                filters.append(filter_str)
                logger.debug(f"生成全局默认过滤: {filter_str} (来自规则: {rule.get('rule_name')})")

        except Exception as e:
            logger.exception("获取全局默认过滤规则失败", error=str(e))

        return filters

    def _flatten_conditions(self, condition: exp.Expression) -> List[exp.Expression]:
        """
        展平AND条件树为条件列表

        例如: (A AND B) AND C → [A, B, C]
        """
        if isinstance(condition, exp.And):
            left = self._flatten_conditions(condition.this)
            right = self._flatten_conditions(condition.expression)
            return left + right
        else:
            return [condition]

    def _condition_exists(self, new_cond: exp.Expression, existing_conds: List[exp.Expression]) -> bool:
        """
        检查条件是否已存在（通过比较SQL字符串）
        """
        new_sql = new_cond.sql(dialect=self.dialect).strip().lower()

        for exist_cond in existing_conds:
            exist_sql = exist_cond.sql(dialect=self.dialect).strip().lower()
            if exist_sql == new_sql:
                return True

        return False

    def _extract_tables_from_ast(self, ast: exp.Select) -> List[str]:
        """
        从 AST 中提取所有涉及的表别名（alias）

        优先提取 alias，如果没有 alias 则使用表名
        
        注意：不同方言下 AST 结构可能不同，使用 find_all(exp.Table) 通用方法
        """
        tables = set()

        # 方法1: 使用 find_all(exp.Table) 直接遍历所有 Table 节点（兼容所有方言）
        for table_expr in ast.find_all(exp.Table):
            # 优先使用 alias，如果没有则使用表名
            alias = table_expr.alias if table_expr.alias else table_expr.name
            if alias:
                # 移除 SQL Server 方括号标识符（如 [表名] -> 表名）
                clean_alias = alias.strip('[]') if alias else alias
                tables.add(clean_alias)
                logger.debug(f"从AST提取表: name={table_expr.name!r}, alias={table_expr.alias!r}, 清理后={clean_alias!r}")

        # 方法2: 备用 - 尝试从 FROM 子句提取（某些方言可能需要）
        if not tables:
            from_clause = ast.args.get("from_")
            if from_clause:
                for table_expr in from_clause.find_all(exp.Table):
                    alias = table_expr.alias if table_expr.alias else table_expr.name
                    if alias:
                        clean_alias = alias.strip('[]') if alias else alias
                        tables.add(clean_alias)

            # 从 JOIN 子句提取
            joins = ast.args.get("joins") or []
            for join in joins:
                for table_expr in join.find_all(exp.Table):
                    alias = table_expr.alias if table_expr.alias else table_expr.name
                    if alias:
                        clean_alias = alias.strip('[]') if alias else alias
                        tables.add(clean_alias)

        logger.debug(f"从AST提取的表别名列表: {list(tables)}")
        return list(tables)

    def inject_default_limit(
        self,
        ast: exp.Select,
        ir: IntermediateRepresentation
    ) -> exp.Select:
        """
        确保查询有 LIMIT（防止意外返回海量数据）

        对于聚合查询（包含 GROUP BY 或聚合函数），通常返回较少的行，
        不需要默认 LIMIT。但如果用户明确指定了 limit（如 limit: 1），则保留。
        对于明细查询，确保有合理的 LIMIT。
        """
        max_allowed = settings.query_max_limit
        
        # 检测是否为聚合查询
        is_aggregation = self._is_aggregation_query(ast, ir)

        # 如果用户在IR中明确指定了limit，保留它（无论聚合还是明细查询）
        if ir.limit is not None:
            limit_value = min(ir.limit, max_allowed)
            # 检查AST中是否已有LIMIT
            if ast.args.get("limit"):
                existing_limit = ast.args.get("limit").this
                if hasattr(existing_limit, 'this'):
                    existing_value = int(existing_limit.this)
                    if existing_value != limit_value:
                        # LIMIT值不一致，更新为用户指定的值
                        ast.set("limit", None)
                        ast = ast.limit(limit_value)
                        logger.debug(f"更新 LIMIT 为用户指定值: {limit_value}")
            else:
                ast = ast.limit(limit_value)
                logger.debug(f"应用用户指定 LIMIT: {limit_value}")
            return ast

        if is_aggregation:
            # 聚合查询且用户未指定limit：移除 LIMIT（如果有的话）
            if ast.args.get("limit"):
                logger.debug("检测到聚合查询，移除不必要的默认 LIMIT")
                ast.set("limit", None)
            else:
                logger.debug("检测到聚合查询，跳过 LIMIT 注入")
            return ast

        # 非聚合查询：只在必要时应用 LIMIT
        if ast.args.get("limit"):
            # 已有 LIMIT，检查是否超过最大限制
            existing_limit = ast.args.get("limit").this
            if hasattr(existing_limit, 'this'):
                limit_value = int(existing_limit.this)
                if limit_value > max_allowed:
                    logger.debug(f"限制 LIMIT 从 {limit_value} 到最大值 {max_allowed}")
                    ast.set("limit", None)
                    ast = ast.limit(max_allowed)
            return ast

        logger.debug("未指定LIMIT，返回所有结果（受数据库和最大限制约束）")
        return ast

    def _is_aggregation_query(
        self,
        ast: exp.Select,
        ir: IntermediateRepresentation
    ) -> bool:
        """
        判断是否为聚合查询

        聚合查询的特征：
        1. 有 GROUP BY 子句
        2. SELECT 中包含聚合函数（COUNT, SUM, AVG, MAX, MIN 等）
        3. IR 中定义了 metrics（指标）或 dimensions（维度）

        Returns:
            True 表示聚合查询，False 表示明细查询
        """
        #  方法0: 优先检查 query_type（最准确）
        if ir.query_type == "detail":
            logger.debug("聚合检测：query_type=detail，判定为明细查询")
            return False
        elif ir.query_type == "aggregation":
            logger.debug("聚合检测：query_type=aggregation，判定为聚合查询")
            return True

        # 方法1: 检查 AST 中是否有 GROUP BY
        has_group_by = bool(ast.args.get("group"))
        if has_group_by:
            logger.debug("聚合检测：通过 GROUP BY 判定为聚合查询")
            return True

        # 方法2: 检查 IR 中是否有指标或维度（语义层聚合）
        has_metrics = bool(ir.metrics)
        has_dimensions = bool(ir.dimensions)
        if has_metrics or has_dimensions:
            logger.debug(f"聚合检测：通过 IR 判定为聚合查询 (metrics: {bool(ir.metrics)}, dimensions: {bool(ir.dimensions)})")
            return True

        # 方法3: 检查 SELECT 子句中是否包含聚合函数
        select_expressions = ast.args.get("expressions", [])
        for expr in select_expressions:
            if self._contains_aggregation_function(expr):
                logger.debug("聚合检测：通过 SELECT 聚合函数判定为聚合查询")
                return True

        logger.debug("聚合检测：判定为明细查询")
        return False

    def _contains_aggregation_function(self, expr: exp.Expression) -> bool:
        """
        递归检查表达式中是否包含聚合函数

        常见聚合函数: COUNT, SUM, AVG, MAX, MIN, COUNT_BIG 等
        """
        # 检查当前节点是否为聚合函数
        if isinstance(expr, (
            exp.Count,
            exp.Sum,
            exp.Avg,
            exp.Max,
            exp.Min,
            exp.CountIf,
            exp.AnyValue,
            exp.ArrayAgg,
            exp.GroupConcat
        )):
            return True

        # 递归检查子节点
        for child in expr.iter_expressions():
            if self._contains_aggregation_function(child):
                return True

        return False

    def _is_union_wrapper_query(self, ast: exp.Select) -> bool:
        """
        检测是否为 UNION 包装查询（with_total=True 场景）

        特征：
        - 外层是 SELECT * FROM (subquery) AS alias
        - 子查询包含 UNION 或 UNION ALL

        这种查询的默认过滤条件应该在子查询内部添加，而不是外层。

        Returns:
            True 表示是 UNION 包装查询
        """
        # 获取 FROM 子句
        from_clause = ast.args.get("from_")
        if not from_clause:
            return False

        # 检查 FROM 是否为子查询
        from_expr = from_clause.this
        if not isinstance(from_expr, exp.Subquery):
            return False

        # 获取子查询内容
        subquery = from_expr.this

        # 检查子查询是否包含 UNION
        if isinstance(subquery, exp.Union):
            logger.debug("检测到UNION包装查询结构")
            return True

        return False
    
    def _is_window_wrapper_query(self, ast: exp.Select) -> bool:
        """
        检测是否为子查询包装查询，需要将默认过滤注入到子查询内部
        
        支持以下场景：
        1. window_detail 场景：外层过滤 _row_num
        2. duplicate_detection 场景：外层过滤 _duplicate_count
        3. having_filters 场景（派生指标过滤）：外层过滤派生指标

        特征：
        - 外层是 SELECT * FROM (subquery) AS alias
        - 外层 WHERE 只引用子查询别名（如 t.xxx），不引用原始表名

        这种查询的默认过滤条件应该在子查询内部添加，而不是外层。

        Returns:
            True 表示是子查询包装查询，需要将默认过滤注入到子查询内部
        """
        # 获取 FROM 子句（注意：sqlglot 使用 'from_' 而不是 'from'，因为 from 是 Python 保留关键字）
        from_clause = ast.args.get("from_")
        if not from_clause:
            return False

        # 检查 FROM 是否为子查询
        from_expr = from_clause.this
        if not isinstance(from_expr, exp.Subquery):
            return False

        # 获取子查询内容
        subquery = from_expr.this
        if not isinstance(subquery, exp.Select):
            return False
        
        # 获取子查询的别名（如 "t"）
        subquery_alias = from_expr.alias
        if not subquery_alias:
            return False

        # 检查外层 SELECT 是否为 SELECT *
        outer_select = ast.args.get("expressions", [])
        is_select_star = len(outer_select) == 1 and isinstance(outer_select[0], exp.Star)
        
        if not is_select_star:
            # 非 SELECT * 的结构，可能是其他类型的查询
            return False
        
        # 检测场景1: 窗口函数包装（_row_num 或 _duplicate_count）
        select_exprs = subquery.args.get("expressions", [])
        for expr in select_exprs:
            if isinstance(expr, exp.Alias):
                alias_name = expr.alias
                # 检查是否为 ROW_NUMBER() OVER (...) AS _row_num（window_detail）
                if alias_name == "_row_num":
                    if isinstance(expr.this, exp.Window):
                        window_func = expr.this.this
                        if isinstance(window_func, exp.RowNumber):
                            logger.debug("检测到窗口函数包装查询结构（ROW_NUMBER）")
                            return True
                # 检查是否为 COUNT(*) OVER (...) AS _duplicate_count（duplicate_detection）
                elif alias_name == "_duplicate_count":
                    if isinstance(expr.this, exp.Window):
                        window_func = expr.this.this
                        if isinstance(window_func, exp.Count):
                            logger.debug("检测到窗口函数包装查询结构（COUNT - duplicate_detection）")
                            return True
        
        # 检测场景2: having_filters 包装（派生指标过滤）
        # 特征：外层 WHERE 条件中的列引用使用子查询别名（如 t.xxx）
        outer_where = ast.args.get("where")
        if outer_where:
            # 检查 WHERE 条件中是否引用了子查询别名
            for col in outer_where.find_all(exp.Column):
                table_ref = col.table
                if table_ref and table_ref == subquery_alias:
                    # 外层 WHERE 引用了子查询别名，说明是 having_filters 包装
                    logger.debug(f"检测到 having_filters 包装查询结构（外层 WHERE 引用子查询别名 {subquery_alias}）")
                    return True

        return False
    
    def _inject_filters_into_window_subquery(
        self,
        ast: exp.Select,
        ir: IntermediateRepresentation
    ) -> exp.Select:
        """
        将默认过滤条件注入到窗口查询的子查询中
        
        适用于 window_detail 场景，生成的查询结构为：
        SELECT * FROM (
          SELECT 
            [字段列表],
            ROW_NUMBER() OVER (...) AS _row_num
          FROM table
          WHERE user_filters
        ) AS RankedData
        WHERE _row_num <= N
        
        需要将默认过滤添加到子查询的 WHERE 中：
        SELECT * FROM (
          SELECT 
            [字段列表],
            ROW_NUMBER() OVER (...) AS _row_num
          FROM table
          WHERE user_filters AND default_filters
        ) AS RankedData
        WHERE _row_num <= N
        """
        # 1. 收集默认过滤条件
        default_filter_conditions = []
        seen_filters = set()
        
        # 从数据质量规则注入（最常见的全局过滤）
        data_quality_filters = self._get_data_quality_filters(ast, ir)
        for filter_str in data_quality_filters:
            if filter_str in seen_filters:
                continue
            try:
                filter_expr = parse_one(filter_str, dialect=self.dialect)
                default_filter_conditions.append(filter_expr)
                seen_filters.add(filter_str)
                logger.debug(f"窗口查询：注入数据质量过滤 {filter_str}")
            except Exception as e:
                logger.error("解析数据质量过滤失败", error=str(e), filter=filter_str)
        
        # 从全局规则注入
        global_default_filters = self._get_global_default_filters(ast, ir)
        for filter_str in global_default_filters:
            if filter_str in seen_filters:
                continue
            try:
                filter_expr = parse_one(filter_str, dialect=self.dialect)
                default_filter_conditions.append(filter_expr)
                seen_filters.add(filter_str)
                logger.debug(f"窗口查询：注入全局默认过滤 {filter_str}")
            except Exception as e:
                logger.error("解析全局默认过滤失败", error=str(e), filter=filter_str)
        
        if not default_filter_conditions:
            logger.debug("窗口查询：无需注入默认过滤条件")
            return ast
        
        # 2. 获取子查询
        from_clause = ast.args.get("from_")
        subquery_expr = from_clause.this
        subquery = subquery_expr.this
        
        # 3. 将过滤条件注入到子查询的WHERE中
        existing_where = subquery.args.get("where")
        if existing_where:
            # 合并现有WHERE条件和默认过滤条件
            combined_conditions = [existing_where.this] + default_filter_conditions
            new_where = exp.Where(this=exp.and_(*combined_conditions))
        else:
            # 创建新的WHERE条件
            new_where = exp.Where(this=exp.and_(*default_filter_conditions))
        
        subquery.set("where", new_where)
        logger.debug(f"窗口查询：已将 {len(default_filter_conditions)} 个默认过滤条件注入到子查询")
        
        return ast

    def _inject_filters_into_union_subqueries(
        self,
        ast: exp.Select,
        ir: IntermediateRepresentation
    ) -> exp.Select:
        """
        将默认过滤条件注入到 UNION 的每个子查询中

        适用于 with_total=True 场景，生成的查询结构为：
        SELECT * FROM (
          SELECT ... WHERE user_filters  -- 子查询1
          UNION ALL
          SELECT ... WHERE user_filters  -- 子查询2
        ) AS _union_result

        需要将默认过滤添加到每个子查询的 WHERE 中：
        SELECT * FROM (
          SELECT ... WHERE user_filters AND default_filters
          UNION ALL
          SELECT ... WHERE user_filters AND default_filters
        ) AS _union_result
        """
        # 1. 收集默认过滤条件
        default_filter_conditions = []
        seen_filters = set()

        # 从指标注入默认过滤
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
            
            metric = self.model.metrics.get(metric_id)
            if metric and metric.default_filters:
                for filter_str in metric.default_filters:
                    if filter_str not in seen_filters:
                        try:
                            filter_expr = parse_one(filter_str, dialect=self.dialect)
                            default_filter_conditions.append(filter_expr)
                            seen_filters.add(filter_str)
                            logger.debug(f"UNION子查询默认过滤: {filter_str}")
                        except Exception as e:
                            logger.error(f"解析默认过滤条件失败: {filter_str}", error=str(e))

        # 从数据库配置注入数据质量过滤
        # 注意：这里需要提取UNION子查询涉及的表
        from_clause = ast.args.get("from_")
        subquery_node = from_clause.this  # Subquery节点
        union_node = subquery_node.this  # Union节点

        # 从第一个子查询中提取表名
        first_select = union_node.this if hasattr(union_node, 'this') else union_node.left
        if isinstance(first_select, exp.Select):
            data_quality_filters = self._get_data_quality_filters(first_select, ir)
            for filter_str in data_quality_filters:
                if filter_str not in seen_filters:
                    try:
                        filter_expr = parse_one(filter_str, dialect=self.dialect)
                        default_filter_conditions.append(filter_expr)
                        seen_filters.add(filter_str)
                        logger.debug(f"UNION子查询数据质量过滤: {filter_str}")
                    except Exception as e:
                        logger.error("解析数据质量过滤失败", error=str(e), filter=filter_str)

        # 如果没有默认过滤条件，直接返回
        if not default_filter_conditions:
            logger.debug("没有需要注入的默认过滤条件")
            return ast

        # 2. 将默认过滤注入到 UNION 的每个子查询中
        logger.debug(f"开始注入 {len(default_filter_conditions)} 个默认过滤到UNION子查询")
        self._inject_into_union_recursive(union_node, default_filter_conditions)

        return ast

    def _inject_into_union_recursive(
        self,
        union_node: exp.Union,
        filter_conditions: List[exp.Expression]
    ):
        """
        递归地将过滤条件注入到 UNION 的所有子查询中
        """
        # 处理左侧
        if hasattr(union_node, 'left') and isinstance(union_node.left, exp.Select):
            self._inject_filters_into_select(union_node.left, filter_conditions)
        elif hasattr(union_node, 'this') and isinstance(union_node.this, exp.Select):
            self._inject_filters_into_select(union_node.this, filter_conditions)
        elif hasattr(union_node, 'this') and isinstance(union_node.this, exp.Union):
            # 嵌套UNION
            self._inject_into_union_recursive(union_node.this, filter_conditions)

        # 处理右侧
        if hasattr(union_node, 'expression') and isinstance(union_node.expression, exp.Select):
            self._inject_filters_into_select(union_node.expression, filter_conditions)
        elif hasattr(union_node, 'expression') and isinstance(union_node.expression, exp.Union):
            # 嵌套UNION
            self._inject_into_union_recursive(union_node.expression, filter_conditions)

    def _inject_filters_into_select(
        self,
        select_node: exp.Select,
        filter_conditions: List[exp.Expression]
    ):
        """
        将过滤条件注入到单个 SELECT 查询中
        """
        existing_where = select_node.args.get("where")

        if existing_where:
            # 已有WHERE，合并条件
            existing_conds = self._flatten_conditions(existing_where.this)

            # 去重：只添加不存在的条件
            new_conds = []
            for new_cond in filter_conditions:
                # 复制条件节点，避免多个子查询共享同一个节点对象
                new_cond_copy = new_cond.copy()
                if not self._condition_exists(new_cond_copy, existing_conds):
                    new_conds.append(new_cond_copy)

            if new_conds:
                all_conditions = existing_conds + new_conds
                select_node.set("where", None)
                select_node.set("where", exp.Where(this=exp.and_(*all_conditions)))
                logger.debug(f"已向子查询注入 {len(new_conds)} 个默认过滤")
        else:
            # 没有WHERE，直接添加
            filter_copies = [f.copy() for f in filter_conditions]
            select_node.set("where", exp.Where(this=exp.and_(*filter_copies)))
            logger.debug(f"已向子查询添加 {len(filter_copies)} 个默认过滤")
