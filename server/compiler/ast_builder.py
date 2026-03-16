"""AST 构建器 - 使用 SQLGlot 构建 SQL"""

from typing import List, Optional, Any, Tuple, Dict
from types import SimpleNamespace
import re
import structlog
import sqlglot
from sqlglot import exp, parse_one, select, table
from datetime import timedelta, date
import calendar

from server.models.ir import IntermediateRepresentation, TimeRange, FilterCondition
from server.models.semantic import SemanticModel, Join
from server.exceptions import CompilationError
from server.utils.timezone_helper import now_with_tz, now_utc, get_datetime_with_delta
from server.compiler.synonym_resolver import SynonymResolver
from server.compiler.dialect_profiles import get_dialect_profile

logger = structlog.get_logger()


class ASTBuilder:
    """AST 构建器"""

    def __init__(
        self,
        semantic_model: SemanticModel,
        dialect: str = "tsql",
        global_rules=None,
        db_type: Optional[str] = None,
    ):
        self.model = semantic_model
        self.profile = get_dialect_profile(db_type or dialect)
        self.dialect = self.profile.compiler_dialect
        self.db_type = self.profile.db_type
        # 构建期表别名缓存：table_id -> friendly alias（物理表名）
        self._alias_cache = {}

        #  同义词解析器（统一值处理）
        self.synonym_resolver = SynonymResolver(semantic_model)

        # 全局规则（包含派生指标定义）
        self.global_rules = global_rules or []
        self._derived_metrics_cache = None  # 派生指标缓存

    def _field_belongs_to_table(self, field, table_id: str) -> bool:
        """
        检查字段是否属于指定表（兼容 datasource_id 存储显示名的情况）
        
        Args:
            field: 字段对象
            table_id: 表ID (UUID)
        
        Returns:
            是否属于该表
        """
        ds_id = getattr(field, 'datasource_id', None)
        if not ds_id:
            return False
        # 直接匹配UUID
        if ds_id == table_id:
            return True
        # 如果 ds_id 是显示名，尝试通过 sources 查找
        source = self.model.sources.get(table_id) if self.model.sources else None
        if source:
            if getattr(source, 'display_name', None) == ds_id:
                return True
            if getattr(source, 'datasource_name', None) == ds_id:
                return True
        return False

    def _get_derived_metrics(self):
        """获取派生指标定义（缓存）"""
        if self._derived_metrics_cache is None:
            self._derived_metrics_cache = {}
            for rule in self.global_rules:
                if rule.get('rule_type') == 'derived_metric':
                    rule_def = rule.get('rule_definition', {})
                    # 优先从rule_definition中获取display_name，否则从rule_name中移除"（派生）"后缀
                    display_name = rule_def.get('display_name')
                    if not display_name:
                        display_name = rule.get('rule_name', '').replace('（派生）', '')
                    self._derived_metrics_cache[display_name] = rule_def
                    logger.debug(f"缓存派生指标: {display_name}")
        return self._derived_metrics_cache

    def _is_derived_metric(self, metric_id: str) -> tuple:
        """
        检查metric_id是否为派生指标

        Returns:
            (is_derived, metric_name, definition) 元组
        """
        if metric_id.startswith('derived:'):
            metric_name = metric_id[8:]  # 移除"derived:"前缀
            derived_metrics = self._get_derived_metrics()
            if metric_name in derived_metrics:
                return (True, metric_name, derived_metrics[metric_name])
        return (False, None, None)

    def _get_field_data_type(self, field_id: str) -> Optional[str]:
        """
        获取字段的数据类型
        
        Args:
            field_id: 字段ID
            
        Returns:
            数据类型字符串（如 'numeric', 'nvarchar', 'datetime2'），未找到返回 None
        """
        # 先从统一字段表获取
        if field_id in self.model.fields:
            field = self.model.fields[field_id]
            if hasattr(field, 'data_type') and field.data_type:
                return field.data_type
        
        # 尝试从 measures 获取
        if field_id in self.model.measures:
            measure = self.model.measures[field_id]
            if hasattr(measure, 'data_type') and measure.data_type:
                return measure.data_type
        
        # 尝试从 dimensions 获取
        if field_id in self.model.dimensions:
            dim = self.model.dimensions[field_id]
            if hasattr(dim, 'data_type') and dim.data_type:
                return dim.data_type
        
        return None
    
    def _is_string_type(self, data_type: Optional[str]) -> bool:
        """
        判断数据类型是否为字符串类型（需要类型转换才能做数学运算）
        
        Args:
            data_type: 数据类型字符串
            
        Returns:
            True 如果是字符串类型
        """
        if not data_type:
            return False
        dt_lower = data_type.lower()
        string_types = ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext', 'string')
        return any(st in dt_lower for st in string_types)
    
    def _wrap_with_safe_cast(self, col_expr: str, data_type: Optional[str]) -> str:
        """
        对字符串类型的列添加安全类型转换（用于数值运算）
        
        Args:
            col_expr: 列表达式（如 [表名].[列名]）
            data_type: 字段的数据类型
            
        Returns:
            如果是字符串类型，返回 TRY_CAST(col AS DECIMAL(18,4))；否则返回原表达式
        """
        if self._is_string_type(data_type):
            if self.dialect == "tsql":
                # SQL Server 使用 TRY_CAST 安全转换，失败返回 NULL
                return f"TRY_CAST({col_expr} AS DECIMAL(18,4))"
            elif self.dialect in ("postgres", "postgresql"):
                # PostgreSQL 使用 CAST 或 ::numeric
                return f"({col_expr})::numeric"
            elif self.dialect == "mysql":
                # MySQL 使用 CAST
                return f"CAST({col_expr} AS DECIMAL(18,4))"
            else:
                # 默认使用 CAST
                return f"CAST({col_expr} AS DECIMAL(18,4))"
        return col_expr

    def _quote_ident(self, name: str) -> str:
        """
        按方言引用标识符（表名/列名）。
        - T-SQL: [name]
        - MySQL: `name`
        - PostgreSQL: "name"
        """
        if name is None:
            return ""
        if self.dialect == "tsql":
            return f"[{name}]"
        if self.dialect == "mysql":
            return f"`{name}`"
        if self.dialect in ("postgres", "postgresql"):
            return f"\"{name}\""
        # 兜底：ANSI 双引号
        return f"\"{name}\""

    def _build_qualified_col_expr(self, table_name: str, col_name: str) -> str:
        """构建带表限定的列引用字符串（用于派生指标字符串替换路径）"""
        return f"{self._quote_ident(table_name)}.{self._quote_ident(col_name)}"

    def _build_derived_metric_expression(self, metric_name: str, definition: dict, main_table: str) -> tuple:
        """
        构建派生指标的SQL表达式

        Args:
            metric_name: 派生指标名称
            definition: 派生指标定义
            main_table: 主表名

        Returns:
            (expression_str, alias) 元组
            如果依赖字段不完整，返回 (None, metric_name) 表示无法构建
        """
        formula = definition.get('formula', '')
        field_deps = definition.get('field_dependencies', [])

        logger.debug(f"构建派生指标 {metric_name}, 公式: {formula}, 依赖字段数: {len(field_deps)}")

        # 特殊处理：COUNT(*) 等无依赖字段的公式
        if not field_deps and 'COUNT(*)' in formula.upper():
            logger.debug(f"派生指标 {metric_name} 是通用计数指标，无需字段替换")
            return (formula, metric_name)

        # 构建替换映射
        replacements = {}
        # 裸字段名替换（用于处理 field * field 等复合表达式）
        bare_replacements = {}
        # 跟踪未解析的字段
        unresolved_fields = []
        # 跟踪已解析的字段数
        resolved_count = 0
        
        for dep in field_deps:
            field_id = dep.get('field_id')
            field_name_hint = dep.get('field_name')  # 配置中可能使用 field_name 而非 field_id
            aggregation = dep.get('aggregation')  # 可能为 None
            
            # 尝试通过 field_id 查找字段
            measure = None
            resolved_field_id = None
            
            if field_id and field_id in self.model.measures:
                measure = self.model.measures[field_id]
                resolved_field_id = field_id
            elif field_id and field_id in self.model.dimensions:
                measure = self.model.dimensions[field_id]
                resolved_field_id = field_id
            elif field_name_hint:
                # 如果没有 field_id，尝试通过 field_name 查找
                resolved_field_id = self._find_field_by_name(field_name_hint, main_table)
                if resolved_field_id:
                    if resolved_field_id in self.model.measures:
                        measure = self.model.measures[resolved_field_id]
                    elif resolved_field_id in self.model.dimensions:
                        # 维度字段也可能用于计算
                        measure = self.model.dimensions[resolved_field_id]
            
            if measure and resolved_field_id:
                resolved_count += 1
                col_name = self._get_physical_column_name(resolved_field_id)
                field_display_name = measure.display_name
                
                # 获取字段数据类型，判断是否需要类型转换
                data_type = self._get_field_data_type(resolved_field_id)
                
                # 构建物理列表达式（可能带类型转换）
                raw_col_expr = self._build_qualified_col_expr(main_table, col_name)
                col_expr = self._wrap_with_safe_cast(raw_col_expr, data_type)
                
                if data_type and self._is_string_type(data_type):
                    logger.debug(f"字段 {field_name_hint or field_id} 是字符串类型({data_type})，添加安全类型转换")
                
                # 1. 如果有聚合函数，添加聚合替换规则
                if aggregation:
                    agg_expr = f"{aggregation}({col_expr})"
                    
                    if field_name_hint:
                        agg_pattern = f"{aggregation}({field_name_hint})"
                        replacements[agg_pattern] = agg_expr
                        logger.debug(f"添加替换规则(by name): {agg_pattern} -> {agg_expr}")
                    
                    if field_display_name and field_display_name != field_name_hint:
                        agg_pattern2 = f"{aggregation}({field_display_name})"
                        replacements[agg_pattern2] = agg_expr
                        logger.debug(f"添加替换规则(by display): {agg_pattern2} -> {agg_expr}")
                
                # 2. 添加裸字段名替换（用于处理复合表达式如 容积率 * 出让面积）
                if field_name_hint:
                    bare_replacements[field_name_hint] = col_expr
                if field_display_name and field_display_name != field_name_hint:
                    bare_replacements[field_display_name] = col_expr
            else:
                unresolved_fields.append(field_name_hint or field_id or "unknown")
                logger.warning(f"派生指标 {metric_name} 依赖的字段不存在: field_id={field_id}, field_name={field_name_hint}")

        # 检查是否所有依赖字段都已解析
        if field_deps and resolved_count == 0:
            # 没有任何字段被解析，无法构建此派生指标
            logger.warning(
                f"派生指标 {metric_name} 的所有依赖字段都不存在，跳过此指标",
                unresolved_fields=unresolved_fields
            )
            return (None, metric_name)
        
        if unresolved_fields:
            # 部分字段未解析，记录警告但尝试继续（可能公式中有些字段是可选的）
            logger.warning(
                f"派生指标 {metric_name} 有部分依赖字段未找到: {unresolved_fields}，"
                f"已解析 {resolved_count}/{len(field_deps)} 个字段"
            )

        # 执行替换
        sql_formula = formula
        
        # 1. 先替换聚合表达式（按长度降序，避免短模式误替换长模式）
        for pattern in sorted(replacements.keys(), key=len, reverse=True):
            if pattern in sql_formula:
                sql_formula = sql_formula.replace(pattern, replacements[pattern])
                logger.debug(f"替换聚合表达式 {pattern} -> {replacements[pattern]}")
        
        # 2. 再替换裸字段名（处理复合表达式如 容积率 * 出让面积）
        for pattern in sorted(bare_replacements.keys(), key=len, reverse=True):
            # 使用单词边界替换，避免误替换已经处理过的内容
            # 只替换不在任何“标识符引用”中的裸字段名（tsql:[], mysql:``, pg:""）
            if pattern in sql_formula and f"[{pattern}]" not in sql_formula and f"`{pattern}`" not in sql_formula and f"\"{pattern}\"" not in sql_formula:
                sql_formula = sql_formula.replace(pattern, bare_replacements[pattern])
                logger.debug(f"替换裸字段名 {pattern} -> {bare_replacements[pattern]}")

        # 3. 检查公式中是否还有未替换的中文字段名（表示依赖字段不完整）
        import re
        # 提取所有被引用的标识符内容（表名和列名），这些是已替换的
        bracketed_content = set()
        for g1, g2, g3 in re.findall(r'\[([^\]]+)\]|`([^`]+)`|"([^"]+)"', sql_formula):
            bracketed_content.add(g1 or g2 or g3)
        
        # 提取所有中文字符序列
        remaining_chinese = re.findall(r'[\u4e00-\u9fff]+', sql_formula)
        
        # 排除 SQL 关键字和已知的函数/常量
        sql_keywords = {'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'NULL', 'AND', 'OR', 'NOT', 'AS'}
        
        # 检查中文词是否是方括号内内容的一部分（包括表名如"公开成交_问数测试"的部分）
        def is_in_brackets(word):
            for bracketed in bracketed_content:
                if word in bracketed:
                    return True
            return False
        
        actual_unresolved = [
            w for w in remaining_chinese 
            if w not in sql_keywords and not is_in_brackets(w)
        ]
        
        if actual_unresolved:
            logger.warning(
                f"派生指标 {metric_name} 公式中仍有未替换的字段: {actual_unresolved}，无法构建此指标"
            )
            return (None, metric_name)

        logger.debug(f"派生指标SQL: {sql_formula}")
        return (sql_formula, metric_name)

    def _build_derived_metric_for_detail(self, metric_name: str, definition: dict, main_table: str) -> tuple:
        """
        为明细查询构建派生指标的SQL表达式（去除聚合函数，转换为行级计算）
        
        Args:
            metric_name: 派生指标名称
            definition: 派生指标定义
            main_table: 主表名
        
        Returns:
            (expression_str, alias) 元组，如果无法转换则返回 (None, None)
        """
        formula = definition.get('formula', '')
        field_deps = definition.get('field_dependencies', [])
        
        logger.debug(f"构建明细查询派生指标 {metric_name}, 原公式: {formula}")
        
        # 检查是否为纯聚合指标（如 COUNT(*)）
        if formula.strip().upper() == 'COUNT(*)':
            logger.warning(f"派生指标 {metric_name} 是纯聚合指标(COUNT)，在明细查询中跳过")
            return (None, None)
        
        # 检查公式中是否只有聚合函数，没有实际字段
        import re
        if not field_deps or len(field_deps) == 0:
            logger.warning(f"派生指标 {metric_name} 没有依赖字段，可能是纯聚合指标，在明细查询中跳过")
            return (None, None)
        
        # 构建替换映射（去除聚合函数，直接使用字段）
        replacements = {}
        bare_replacements = {}
        # 跟踪未解析的字段
        unresolved_fields = []
        resolved_count = 0
        
        for dep in field_deps:
            field_id = dep.get('field_id')
            field_name_hint = dep.get('field_name')  # 配置中可能使用 field_name 而非 field_id
            aggregation = dep.get('aggregation')  # 可能为 None
            
            # 尝试通过 field_id 查找字段
            measure = None
            resolved_field_id = None
            
            if field_id and field_id in self.model.measures:
                measure = self.model.measures[field_id]
                resolved_field_id = field_id
            elif field_id and field_id in self.model.dimensions:
                measure = self.model.dimensions[field_id]
                resolved_field_id = field_id
            elif field_name_hint:
                # 如果没有 field_id，尝试通过 field_name 查找
                resolved_field_id = self._find_field_by_name(field_name_hint, main_table)
                if resolved_field_id:
                    if resolved_field_id in self.model.measures:
                        measure = self.model.measures[resolved_field_id]
                    elif resolved_field_id in self.model.dimensions:
                        measure = self.model.dimensions[resolved_field_id]
            
            if measure and resolved_field_id:
                resolved_count += 1
                col_name = self._get_physical_column_name(resolved_field_id)
                field_display_name = measure.display_name
                
                # 获取字段数据类型，判断是否需要类型转换
                data_type = self._get_field_data_type(resolved_field_id)
                
                # 明细查询：直接使用字段值，不加聚合函数（可能带类型转换）
                raw_row_expr = self._build_qualified_col_expr(main_table, col_name)
                row_expr = self._wrap_with_safe_cast(raw_row_expr, data_type)
                
                if data_type and self._is_string_type(data_type):
                    logger.debug(f"字段 {field_name_hint or field_id} 是字符串类型({data_type})，添加安全类型转换")
                
                # 1. 如果有聚合函数，添加替换规则去掉聚合
                if aggregation:
                    if field_name_hint:
                        agg_pattern = f"{aggregation}({field_name_hint})"
                        replacements[agg_pattern] = row_expr
                        logger.debug(f"添加明细查询替换规则(by name): {agg_pattern} -> {row_expr}")
                    
                    if field_display_name and field_display_name != field_name_hint:
                        agg_pattern2 = f"{aggregation}({field_display_name})"
                        replacements[agg_pattern2] = row_expr
                        logger.debug(f"添加明细查询替换规则(by display): {agg_pattern2} -> {row_expr}")
                
                # 2. 添加裸字段名替换
                if field_name_hint:
                    bare_replacements[field_name_hint] = row_expr
                if field_display_name and field_display_name != field_name_hint:
                    bare_replacements[field_display_name] = row_expr
            else:
                unresolved_fields.append(field_name_hint or field_id or "unknown")
                logger.warning(f"派生指标 {metric_name} 依赖的字段不存在: field_id={field_id}, field_name={field_name_hint}")
        
        # 检查是否所有依赖字段都已解析
        if field_deps and resolved_count == 0:
            logger.warning(f"派生指标 {metric_name} 的所有依赖字段都不存在，跳过此指标")
            return (None, None)
        
        # 执行替换
        sql_formula = formula
        
        # 1. 先替换聚合表达式
        for pattern in sorted(replacements.keys(), key=len, reverse=True):
            if pattern in sql_formula:
                sql_formula = sql_formula.replace(pattern, replacements[pattern])
                logger.debug(f"替换聚合表达式 {pattern} -> {replacements[pattern]}")
        
        # 2. 再替换裸字段名
        for pattern in sorted(bare_replacements.keys(), key=len, reverse=True):
            if pattern in sql_formula and f"[{pattern}]" not in sql_formula and f"`{pattern}`" not in sql_formula and f"\"{pattern}\"" not in sql_formula:
                sql_formula = sql_formula.replace(pattern, bare_replacements[pattern])
                logger.debug(f"替换裸字段名 {pattern} -> {bare_replacements[pattern]}")
        
        # 若仍残留聚合函数（可能因为依赖字段未匹配），尝试按语义降级为行级表达式
        sql_formula = self._strip_remaining_aggregations(sql_formula, main_table)
        
        # 检查公式中是否还有未替换的中文字段名（表示依赖字段不完整）
        # 提取所有被引用的标识符内容（表名和列名），这些是已替换的
        bracketed_content = set()
        for g1, g2, g3 in re.findall(r'\[([^\]]+)\]|`([^`]+)`|"([^"]+)"', sql_formula):
            bracketed_content.add(g1 or g2 or g3)
        
        remaining_chinese = re.findall(r'[\u4e00-\u9fff]+', sql_formula)
        sql_keywords = {'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'NULL', 'AND', 'OR', 'NOT', 'AS'}
        
        # 检查中文词是否是方括号内内容的一部分
        def is_in_brackets(word):
            for bracketed in bracketed_content:
                if word in bracketed:
                    return True
            return False
        
        actual_unresolved = [
            w for w in remaining_chinese 
            if w not in sql_keywords and not is_in_brackets(w)
        ]
        
        if actual_unresolved:
            logger.warning(
                f"派生指标 {metric_name} 公式中仍有未替换的字段: {actual_unresolved}，无法构建此指标"
            )
            return (None, None)
        
        logger.debug(f"明细查询派生指标SQL: {sql_formula}")
        return (sql_formula, metric_name)

    def _strip_remaining_aggregations(self, expression: str, table_alias: str) -> str:
        """
        将仍包含的 SUM/AVG/MIN/MAX 聚合函数替换为行级字段引用，避免明细查询生成聚合表达式。
        """
        import re
        agg_regex = re.compile(r'\b(SUM|AVG|MIN|MAX)\s*\(\s*([^\(\)]+?)\s*\)', re.IGNORECASE)

        def _normalize_column(col: str) -> str:
            candidate = col.strip()
            if candidate == '*':
                return candidate
            # 如果已经包含别名/表前缀，则直接返回
            if '.' in candidate or candidate.startswith('['):
                return candidate
            sanitized = candidate.strip('`"[]')
            return f"[{table_alias}].[{sanitized}]"

        def repl(match):
            inner = match.group(2)
            return _normalize_column(inner)

        new_expression = agg_regex.sub(repl, expression)
        return new_expression

    def _get_physical_column_name(self, field_id: str) -> str:
        """
        获取字段的物理列名

        Args:
            field_id: 字段ID

        Returns:
            物理列名（如果找不到，返回逻辑字段名）
        """
        if field_id not in self.model.fields:
            logger.warning(f"字段 {field_id} 不存在")
            return field_id

        field = self.model.fields[field_id]
        datasource_id = field.datasource_id
        physical_column_id = field.physical_column_id

        # 查找对应的物理列
        if datasource_id in self.model.datasources:
            datasource = self.model.datasources[datasource_id]
            for physical_table in datasource.physical_tables:
                for column in physical_table.columns:
                    if column.column_id == physical_column_id:
                        logger.debug(f"字段 {field_id} 映射到物理列: {column.column_name}")
                        return column.column_name

        # 如果找不到，回退到逻辑字段名
        logger.warning(f"未找到字段 {field_id} 的物理列映射，使用逻辑字段名: {field.field_name}")
        return field.field_name

    def _get_table_alias(self, table_id: str) -> str:
        """返回生成 SQL 时使用的友好表别名（优先物理表名）。"""
        if table_id in self._alias_cache:
            return self._alias_cache[table_id]
        try:
            source = self.model.sources.get(table_id)
            alias = source.table_name if source and getattr(source, 'table_name', None) else table_id
        except Exception:
            alias = table_id
        self._alias_cache[table_id] = alias
        return alias
    
    def _find_field_by_name(self, field_name: str, main_table: str) -> Optional[str]:
        """
        通过字段显示名查找字段ID
        
        优先查找主表中的字段，支持模糊匹配。
        
        Args:
            field_name: 字段显示名（如"总价"、"出让面积"）
            main_table: 主表名
        
        Returns:
            字段ID（UUID字符串），如果未找到返回 None
        """
        # 先尝试精确匹配主表中的度量字段
        for field_id, measure in self.model.measures.items():
            if measure.display_name == field_name:
                # 检查是否属于主表
                if hasattr(measure, 'datasource_id'):
                    ds = self.model.datasources.get(measure.datasource_id)
                    if ds and ds.table_name == main_table:
                        logger.debug(f"通过显示名精确匹配到字段: {field_name} -> {field_id}")
                        return field_id
        
        # 再尝试匹配所有度量字段（跨表）
        for field_id, measure in self.model.measures.items():
            if measure.display_name == field_name:
                logger.debug(f"通过显示名匹配到字段(跨表): {field_name} -> {field_id}")
                return field_id
        
        # 尝试匹配维度字段
        for field_id, dimension in self.model.dimensions.items():
            if dimension.display_name == field_name:
                logger.debug(f"通过显示名匹配到维度字段: {field_name} -> {field_id}")
                return field_id
        
        # 尝试通过同义词匹配
        for field_id, field in self.model.fields.items():
            if hasattr(field, 'synonyms') and field.synonyms:
                if field_name in field.synonyms:
                    logger.debug(f"通过同义词匹配到字段: {field_name} -> {field_id}")
                    return field_id
        
        logger.debug(f"未找到字段: {field_name}")
        return None
    
    def _build_window_function_columns(
        self,
        ir: IntermediateRepresentation,
        main_table: str
    ) -> List[exp.Expression]:
        """
        构建窗口函数列（SQL Server 2012+）
        
        支持:
        - 同比/环比分析 (LAG/LEAD)
        - 累计统计 (SUM OVER)
        - 移动平均 (AVG OVER ROWS BETWEEN)
        """
        window_exprs = []
        
        # 1. 同比/环比分析 (LAG)
        if ir.comparison_type:
            logger.debug(f"检测到同比/环比分析: {ir.comparison_type}")
            
            # 确定排序维度（智能选择时间维度）
            if not ir.dimensions:
                logger.warning("同比/环比需要时间维度，但 dimensions 为空")
            else:
                order_dim_id = self._select_time_dimension(ir.dimensions)
                order_dim = self.model.dimensions.get(order_dim_id) or self.model.fields.get(order_dim_id)
                if not order_dim:
                    logger.warning(f"未找到排序维度: {order_dim_id}")
                else:
                    order_expr = exp.column(order_dim.column, table=self._get_table_alias(order_dim.table if hasattr(order_dim, 'table') else order_dim.datasource_id))
                    
                    # 确定LAG的偏移量
                    offset_map = {
                        'yoy': 12,  # 年同比：12个月
                        'qoq': 4,   # 季环比：4个季度
                        'mom': 1,   # 月环比：1个月
                        'wow': 1,   # 周环比：1周
                    }
                    lag_offset = offset_map.get(ir.comparison_type, 1) * ir.comparison_periods
                    
                    # 为每个指标添加LAG列
                    for metric_item in ir.metrics:
                        # 规范化 metric：提取字段ID
                        metric_id, _, _, _ = self._normalize_metric_spec(metric_item)
                        
                        # 跳过保留字
                        if metric_id == "__row_count__":
                            logger.debug("同比/环比查询中跳过保留字 __row_count__")
                            continue
                        
                        # 获取指标表达式
                        if metric_id in self.model.metrics:
                            metric = self.model.metrics[metric_id]
                            expression = self._get_metric_expression(metric)
                            if expression:
                                try:
                                    metric_expr = parse_one(expression, dialect=self.dialect)
                                    
                                    # 构建 LAG(metric_expr, offset) OVER (ORDER BY time_dim)
                                    lag_func = exp.Lag(
                                        this=metric_expr,
                                        offset=exp.Literal.number(lag_offset)
                                    )
                                    window_spec = exp.Window(
                                        this=lag_func,
                                        order=exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)])
                                    )
                                    
                                    # 生成别名：如 "销售额_上期"
                                    comparison_label = {
                                        'yoy': '去年同期',
                                        'qoq': '上季度',
                                        'mom': '上月',
                                        'wow': '上周'
                                    }.get(ir.comparison_type, '上期')
                                    
                                    lag_alias = f"{metric_id}_{comparison_label}"
                                    window_exprs.append(window_spec.as_(lag_alias))
                                    logger.debug(f"添加LAG列: {lag_alias}")
                                    
                                    # 如果需要显示增长率，添加增长率计算列
                                    if ir.show_growth_rate:
                                        # 增长率 = (当前值 - 上期值) / 上期值 * 100
                                        # 使用CASE避免除零错误
                                        growth_rate_expr = exp.Case(
                                            ifs=[
                                                exp.If(
                                                    this=exp.Or(
                                                        this=exp.Is(this=window_spec.copy(), expression=exp.Null()),
                                                        expression=exp.EQ(this=window_spec.copy(), expression=exp.Literal.number(0))
                                                    ),
                                                    true=exp.Null()
                                                )
                                            ],
                                            default=exp.Mul(
                                                this=exp.Div(
                                                    this=exp.Sub(this=metric_expr.copy(), expression=window_spec.copy()),
                                                    expression=window_spec.copy()
                                                ),
                                                expression=exp.Literal.number(100)
                                            )
                                        )
                                        
                                        growth_alias = f"{metric_id}_增长率"
                                        window_exprs.append(growth_rate_expr.as_(growth_alias))
                                        logger.debug(f"添加增长率列: {growth_alias}")
                                        
                                except Exception as e:
                                    logger.error(f"构建LAG窗口函数失败: {metric_id}", error=str(e))
        
        # 2. 累计统计 (SUM OVER)
        if ir.cumulative_metrics:
            logger.debug(f"检测到累计统计: {ir.cumulative_metrics}")
            
            # 确定排序维度（优先用户指定，否则智能选择时间维度）
            if ir.cumulative_order_by:
                order_dim_id = ir.cumulative_order_by
            elif ir.dimensions:
                order_dim_id = self._select_time_dimension(ir.dimensions)
            else:
                order_dim_id = None
            
            if not order_dim_id:
                logger.warning("累计统计需要排序维度，但未指定")
            else:
                order_dim = self.model.dimensions.get(order_dim_id) or self.model.fields.get(order_dim_id)
                if not order_dim:
                    logger.warning(f"未找到排序维度: {order_dim_id}")
                else:
                    order_expr = exp.column(order_dim.column, table=self._get_table_alias(order_dim.table if hasattr(order_dim, 'table') else order_dim.datasource_id))
                    
                    # 为每个累计指标添加窗口函数列
                    for metric_id in ir.cumulative_metrics:
                        if metric_id in self.model.metrics:
                            metric = self.model.metrics[metric_id]
                            expression = self._get_metric_expression(metric)
                            if expression:
                                try:
                                    metric_expr = parse_one(expression, dialect=self.dialect)
                                    
                                    # 构建 SUM(metric_expr) OVER (ORDER BY time_dim ROWS UNBOUNDED PRECEDING)
                                    sum_func = exp.Sum(this=metric_expr)
                                    window_spec = exp.Window(
                                        this=sum_func,
                                        order=exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)]),
                                        spec=exp.WindowSpec(
                                            kind="ROWS",
                                            start="UNBOUNDED PRECEDING",
                                            end="CURRENT ROW"
                                        )
                                    )
                                    
                                    cumulative_alias = f"累计{metric_id}"
                                    window_exprs.append(window_spec.as_(cumulative_alias))
                                    logger.debug(f"添加累计列: {cumulative_alias}")
                                    
                                except Exception as e:
                                    logger.error(f"构建累计窗口函数失败: {metric_id}", error=str(e))
        
        # 3. 移动平均 (AVG OVER ROWS BETWEEN)
        if ir.moving_average_metrics and ir.moving_average_window:
            logger.debug(f"检测到移动平均: {ir.moving_average_metrics}, 窗口={ir.moving_average_window}")
            
            # 确定排序维度（智能选择时间维度）
            if not ir.dimensions:
                logger.warning("移动平均需要排序维度，但 dimensions 为空")
                order_dim_id = None
            else:
                order_dim_id = self._select_time_dimension(ir.dimensions)
            
            if order_dim_id:
                order_dim = self.model.dimensions.get(order_dim_id) or self.model.fields.get(order_dim_id)
                if not order_dim:
                    logger.warning(f"未找到排序维度: {order_dim_id}")
                else:
                    order_expr = exp.column(order_dim.column, table=self._get_table_alias(order_dim.table if hasattr(order_dim, 'table') else order_dim.datasource_id))
                    
                    # 为每个移动平均指标添加窗口函数列
                    for metric_id in ir.moving_average_metrics:
                        if metric_id in self.model.metrics:
                            metric = self.model.metrics[metric_id]
                            expression = self._get_metric_expression(metric)
                            if expression:
                                try:
                                    metric_expr = parse_one(expression, dialect=self.dialect)
                                    
                                    # 构建 AVG(metric_expr) OVER (ORDER BY time_dim ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)
                                    avg_func = exp.Avg(this=metric_expr)
                                    window_spec = exp.Window(
                                        this=avg_func,
                                        order=exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)]),
                                        spec=exp.WindowSpec(
                                            kind="ROWS",
                                            start=f"{ir.moving_average_window - 1} PRECEDING",
                                            end="CURRENT ROW"
                                        )
                                    )
                                    
                                    ma_alias = f"{metric_id}_{ir.moving_average_window}日均线"
                                    window_exprs.append(window_spec.as_(ma_alias))
                                    logger.debug(f"添加移动平均列: {ma_alias}")
                                    
                                except Exception as e:
                                    logger.error(f"构建移动平均窗口函数失败: {metric_id}", error=str(e))
        
        return window_exprs
    
    def _select_time_dimension(self, dimension_ids: List[str]) -> str:
        """
        从维度列表中智能选择时间维度
        
        优先级：
        1. field_category='timestamp' 或 dimension_type='temporal'
        2. 字段名包含时间关键词（时间、日期、月份、年份、date、time、month、year）
        3. 回退到第一个维度
        
        Args:
            dimension_ids: 候选维度ID列表
        
        Returns:
            选中的时间维度ID
        """
        if not dimension_ids:
            return None
        
        # 时间关键词列表
        time_keywords = [
            '时间', '日期', '月份', '年份', '年月', '日', '月', '年',
            'date', 'time', 'datetime', 'timestamp', 'month', 'year', 'day'
        ]
        
        # 遍历所有候选维度，寻找最佳时间维度
        # 优先级1: 明确标记为时间类型的字段
        for dim_id in dimension_ids:
            # 尝试从 fields（新架构）获取
            field = self.model.fields.get(dim_id) if hasattr(self.model, 'fields') else None
            if field:
                # 1.1 检查 field_category 是否为 timestamp（时间戳字段）
                if getattr(field, 'field_category', None) == 'timestamp':
                    logger.debug(f"选择时间维度（timestamp字段）: {dim_id}")
                    return dim_id
                
                # 1.2 检查 dimension_props.dimension_type 是否为 temporal（时间维度）
                # 例如：年份、季度等作为维度使用的时间字段
                dim_props = getattr(field, 'dimension_props', None)
                if dim_props and getattr(dim_props, 'dimension_type', None) == 'temporal':
                    logger.debug(f"选择时间维度（temporal维度）: {dim_id}")
                    return dim_id
            
            # 尝试从 dimensions（旧架构）获取
            dimension = self.model.dimensions.get(dim_id)
            if dimension:
                # 检查 dimension_type 是否为 temporal
                dim_type = getattr(dimension, 'type', None) or getattr(dimension, 'dimension_type', None)
                if dim_type == 'temporal':
                    logger.debug(f"选择时间维度（旧架构temporal）: {dim_id}")
                    return dim_id
        
        # 第二优先级：字段名包含时间关键词
        for dim_id in dimension_ids:
            dim_id_lower = dim_id.lower()
            
            # 检查字段 ID
            if any(keyword in dim_id_lower for keyword in time_keywords):
                logger.debug(f"选择时间维度（字段名包含时间关键词）: {dim_id}")
                return dim_id
            
            # 检查显示名称
            field = self.model.fields.get(dim_id) if hasattr(self.model, 'fields') else None
            if field:
                display_name = getattr(field, 'display_name', '').lower()
                if any(keyword in display_name for keyword in time_keywords):
                    logger.debug(f"选择时间维度（显示名称包含时间关键词）: {dim_id}")
                    return dim_id
            
            dimension = self.model.dimensions.get(dim_id)
            if dimension:
                display_name = getattr(dimension, 'display_name', '').lower()
                if any(keyword in display_name for keyword in time_keywords):
                    logger.debug(f"选择时间维度（显示名称包含时间关键词）: {dim_id}")
                    return dim_id
        
        # 回退：使用第一个维度
        logger.debug(f"未找到明确的时间维度，使用第一个维度: {dimension_ids[0]}")
        return dimension_ids[0]

    def _get_dimension_alias(self, dim_id: str) -> str:
        """维度在SQL中的列别名：优先使用显示名。"""
        try:
            if dim_id in self.model.dimensions:
                display = getattr(self.model.dimensions[dim_id], 'display_name', None)
                if display:
                    return display
            # 兼容统一字段模型（fields）
            if hasattr(self.model, 'fields') and dim_id in self.model.fields:
                display = getattr(self.model.fields[dim_id], 'display_name', None)
                if display:
                    return display
        except Exception:
            pass
        return dim_id

    def _get_metric_or_measure_alias(self, field_id: str) -> str:
        """指标/度量在SQL中的列别名：优先使用显示名。"""
        try:
            if field_id in self.model.metrics:
                m = self.model.metrics[field_id]
                return getattr(m, 'display_name', None) or field_id
            if field_id in self.model.measures:
                f = self.model.measures[field_id]
                return getattr(f, 'display_name', None) or field_id
            # 兼容统一字段模型（fields）
            if hasattr(self.model, 'fields') and field_id in self.model.fields:
                f = self.model.fields[field_id]
                return getattr(f, 'display_name', None) or field_id
        except Exception:
            pass
        return field_id

    def _get_alias_from_ir_metrics(self, ir, field_id: str) -> Optional[str]:
        """
        从 IR 的 metrics 列表中获取指定字段的别名
        
        当 IR 中为 metrics 指定了自定义 alias（如带单位的别名），
        ORDER BY 应使用该 alias 以保持与 SELECT 一致，避免 SQL Server GROUP BY 错误。
        
        Args:
            ir: IntermediateRepresentation
            field_id: 字段 ID（UUID 或保留字如 __row_count__）
            
        Returns:
            IR 中指定的 alias，如果没有则返回 None
        """
        if not hasattr(ir, 'metrics') or not ir.metrics:
            return None
        
        for metric_item in ir.metrics:
            # 支持字符串格式（旧格式）
            if isinstance(metric_item, str):
                continue
            
            # 支持 dict 格式
            if isinstance(metric_item, dict):
                item_field = metric_item.get("field")
                item_alias = metric_item.get("alias")
            # 支持 MetricSpec 对象格式
            elif hasattr(metric_item, "field"):
                item_field = metric_item.field
                item_alias = getattr(metric_item, "alias", None)
            else:
                continue
            
            # 匹配字段 ID
            if item_field == field_id and item_alias:
                return item_alias
        
        return None
    
    def _get_field_unit(self, field_id: str) -> Optional[str]:
        """获取字段的单位（用于构建带单位的别名）"""
        try:
            # 优先从 fields 表获取
            if hasattr(self.model, 'fields') and field_id in self.model.fields:
                f = self.model.fields[field_id]
                if hasattr(f, 'measure_props') and f.measure_props:
                    return getattr(f.measure_props, 'unit', None)
            # 从 measures 获取
            if field_id in self.model.measures:
                f = self.model.measures[field_id]
                if hasattr(f, 'measure_props') and f.measure_props:
                    return getattr(f.measure_props, 'unit', None)
                # 兼容旧模型直接有 unit 属性
                return getattr(f, 'unit', None)
        except Exception:
            pass
        return None
    
    def _build_alias_with_unit(self, base_alias: str, field_id: str) -> str:
        """构建带单位的别名，如 '最大值(万元)'"""
        unit = self._get_field_unit(field_id)
        if unit:
            return f"{base_alias}({unit})"
        return base_alias

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
                column_name = field.column  # 使用column属性（返回缓存的物理列名）

                # 选择友好的表别名（物理表名）
                table_alias = self._get_table_alias(ds_id)

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
                qualified = f"{_q(table_alias)}.{_q(column_name)}"

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

    def build(
        self,
        ir: IntermediateRepresentation,
        joins: List[Join],
        global_rules: List[dict] = None
    ) -> exp.Expression:
        """
        构建 SQL AST

        Args:
            ir: 中间表示
            joins: Join 路径
            global_rules: 全局规则列表（包含派生指标定义）

        Returns:
            SQLGlot Expression（Select 或 Union）
        """
        # 更新全局规则
        if global_rules is not None:
            self.global_rules = global_rules
            self._derived_metrics_cache = None  # 清除缓存

        logger.debug("开始构建 AST")

        #  0. 处理跨分区查询
        if ir.cross_partition_query and ir.selected_table_ids and len(ir.selected_table_ids) > 1:
            if ir.cross_partition_mode == "compare":
                # 对比模式：生成 JOIN 查询，计算差值和变化率
                # 注意：compare 模式只支持 2 表对比。超过 2 表时自动降级到 union 模式
                if len(ir.selected_table_ids) > 2:
                    logger.warning(
                        "跨年对比模式只支持2表对比，当前选择了{}张表，自动降级为UNION ALL合并模式".format(
                            len(ir.selected_table_ids)
                        ),
                        selected_table_ids=ir.selected_table_ids
                    )
                    return self._build_union_query(ir, joins)
                logger.debug("检测到跨分区对比查询，使用 JOIN 对比")
                return self._build_compare_query(ir, joins)
            elif ir.cross_partition_mode == "multi_join":
                # 多表关联模式：生成 INNER JOIN 查询，找出同时存在于多个表中的记录
                logger.debug("检测到多表关联查询（multi_join），使用 INNER JOIN")
                return self._build_multi_join_query(ir, joins)
            else:
                # 合并模式（默认）：生成 UNION ALL 查询
                logger.debug("检测到跨分区合并查询，使用 UNION ALL")
                return self._build_union_query(ir, joins)

        #  0.1. 处理重复检测查询（使用窗口函数）
        if ir.query_type == "duplicate_detection":
            logger.debug("检测到重复检测查询，使用窗口函数")
            return self._build_duplicate_detection_query(ir, joins)
        
        #  0.2. 处理窗口函数明细查询（分组TopN）
        if ir.query_type == "window_detail":
            logger.debug("检测到窗口函数明细查询（分组TopN），使用ROW_NUMBER()")
            return self._build_window_detail_query(ir, joins)

        # 1. 确定主表
        main_table = None  # 初始化，避免未定义引用
        if joins:
            main_table = joins[0].from_table
        else:
            # 从维度中提取
            if ir.dimensions:
                #  新架构支持：兼容dimensions和fields
                if ir.dimensions[0] in self.model.dimensions:
                    dim = self.model.dimensions[ir.dimensions[0]]
                    main_table = dim.table
                elif ir.dimensions[0] in self.model.fields:
                    field = self.model.fields[ir.dimensions[0]]
                    main_table = field.datasource_id
                    logger.debug(f"从统一字段表确定主表: {main_table}")
            # 从指标表达式提取
            elif ir.metrics:
                for metric_item in ir.metrics:
                    # 规范化 metric：提取字段ID
                    metric_id, _, _, _ = self._normalize_metric_spec(metric_item)
                    
                    # 跳过保留字
                    if metric_id == "__row_count__":
                        # __row_count__ 不依赖特定表，主表从其他字段推断
                        logger.debug("主表判断中跳过保留字 __row_count__")
                        continue
                    
                    #  容错：如果是 measure ID 而不是 metric ID，从 measures 中获取表
                    if metric_id in self.model.metrics:
                        metric = self.model.metrics[metric_id]
                        # 派生指标可能没有 expression，跳过
                        expression = self._get_metric_expression(metric)
                        if not expression:
                            continue
                        for source_id in self.model.sources.keys():
                            if source_id in expression:
                                main_table = source_id
                                break
                        if main_table:  # 找到主表就退出
                            break
                    elif metric_id in self.model.measures:
                        # 从 measure 中获取表（处理明细查询中 LLM 混淆的情况）
                        measure = self.model.measures[metric_id]
                        main_table = measure.table
                        logger.warning(f"从 measures 中获取主表: {main_table} (metric_id: {metric_id})")
                        break

            # 如果还未找到主表，从过滤条件中提取（明细查询场景）
            if not main_table and ir.filters:
                for filter_cond in ir.filters:
                    if filter_cond.field in self.model.dimensions:
                        dim = self.model.dimensions[filter_cond.field]
                        main_table = dim.table
                        break
                    elif filter_cond.field in self.model.fields:
                        #  新架构支持
                        field = self.model.fields[filter_cond.field]
                        main_table = field.datasource_id
                        logger.debug(f"从过滤条件的统一字段表确定主表: {main_table}")
                        break

            # 如果仍然没有主表，尝试使用 IR 中的 primary_table_id 作为默认主表提示
            if not main_table and hasattr(ir, "primary_table_id") and ir.primary_table_id:
                main_table = ir.primary_table_id
                logger.debug(
                    "从IR的primary_table_id确定主表（表级检索Top1）",
                    main_table=main_table,
                    query_type=ir.query_type,
                )

            # 最后的fallback：使用第一个source
            if not main_table:
                main_table = list(self.model.sources.keys())[0]
                logger.warning(f"无法从IR推断主表，使用默认表: {main_table}")

        # 2. 构建 SELECT 子句（传入ir以便处理排序字段）
        select_exprs = self._build_select_clause(ir, main_table)

        # 3. 构建 FROM 子句
        source = self.model.sources[main_table]
        main_alias = self._get_table_alias(main_table)
        from_table = table(
            source.table_name,
            db=source.schema_name,
            alias=main_alias
        )

        # 4. 创建基础查询
        query = select(*select_exprs).from_(from_table)

        # 5. 添加 JOIN 子句
        query = self._add_joins(query, joins)

        # 6. 添加 WHERE 子句（ 传入joins以支持JOIN策略）
        where_conditions = self._build_where_clause(ir, main_table, joins)
        if where_conditions:
            query = query.where(exp.and_(*where_conditions))

        # 7. 添加 GROUP BY 子句
        #  明细查询不应该有 GROUP BY（即使 LLM 错误地返回了 dimensions）
        if ir.dimensions and ir.query_type != "detail":
            group_by_exprs = []
            for dim_id in ir.dimensions:
                #  新架构支持：兼容dimensions和fields
                if dim_id in self.model.dimensions:
                    dim = self.model.dimensions[dim_id]
                    group_by_exprs.append(
                        exp.column(dim.column, table=self._get_table_alias(dim.table))
                    )
                elif dim_id in self.model.fields:
                    field = self.model.fields[dim_id]
                    group_by_exprs.append(
                        exp.column(field.column, table=self._get_table_alias(field.datasource_id))
                    )
                    logger.debug(f"从统一字段表添加GROUP BY: {dim_id}")

            #  添加标注字段到 GROUP BY（如果有）
            # 对于被过滤但未分组的维度，如果配置了 include_in_result，也加入 GROUP BY
            if ir.filters:
                filtered_dim_ids = {f.field for f in ir.filters
                                  if f.field in self.model.dimensions or f.field in self.model.fields}
                for dim_id in filtered_dim_ids:
                    if dim_id not in ir.dimensions:  # 不在原分组中
                        if dim_id in self.model.dimensions:
                            dim = self.model.dimensions[dim_id]
                            if dim.include_in_result:
                                logger.debug(f"标注字段加入 GROUP BY: {dim.column}")
                                group_by_exprs.append(
                                    exp.column(dim.column, table=self._get_table_alias(dim.table))
                                )
                        elif dim_id in self.model.fields:
                            field = self.model.fields[dim_id]
                            if hasattr(field, 'include_in_result') and field.include_in_result:
                                logger.debug(f"标注字段(fields)加入 GROUP BY: {field.column}")
                                group_by_exprs.append(
                                    exp.column(field.column, table=self._get_table_alias(field.datasource_id))
                                )

            query = query.group_by(*group_by_exprs)
        elif ir.dimensions and ir.query_type == "detail":
            # 明细查询中的 dimensions 应该被忽略
            logger.warning(f"明细查询中忽略 dimensions: {ir.dimensions}，明细查询不使用 GROUP BY")

        # 7.5 添加 HAVING 子句（混合架构扩展：聚合后过滤）
        if hasattr(ir, 'having_filters') and ir.having_filters and ir.query_type == "aggregation":
            having_conditions = self._build_having_clause(ir, main_table)
            if having_conditions:
                query = query.having(exp.and_(*having_conditions))
                logger.debug(f"添加 HAVING 子句: {len(having_conditions)} 个条件")
        
        # 7.6 处理 detail 查询中的 having_filters（需要使用子查询包装）
        # 对于 detail 查询，having_filters 用于过滤派生指标（如每亩单价>=10）
        # 这类条件需要先计算派生指标值，再在外层过滤
        needs_subquery_for_having = (
            hasattr(ir, 'having_filters') and 
            ir.having_filters and 
            ir.query_type == "detail"
        )
        if needs_subquery_for_having:
            logger.debug(f"明细查询中检测到 having_filters，将使用子查询包装")
            # 记录需要在最终输出时包装子查询（在 build 方法末尾处理）
            # 这里只是标记，实际包装在返回前执行
            pass  # 标记逻辑在下方 "处理 detail 查询的 having_filters" 处执行

        # 8. 添加 ORDER BY 子句（优先使用sort_by）
        if ir.sort_by:
            # 明细查询：按物理列排序（字段本身就出现在SELECT中）
            if ir.query_type == "detail":
                order_column = None
                order_table = None
                order_expr = None
                desc = (ir.sort_order == "desc")

                # 0) 检查是否为派生指标
                is_derived, derived_name, derived_def = self._is_derived_metric(ir.sort_by)
                if is_derived and derived_name and derived_def:
                    # 明细查询中，派生指标需要转换为行级计算表达式
                    table_alias = self._get_table_alias(main_table)
                    formula_sql, alias = self._build_derived_metric_for_detail(derived_name, derived_def, table_alias)
                    if formula_sql:
                        try:
                            order_expr = parse_one(formula_sql, dialect=self.dialect)
                            logger.debug(f"明细查询按派生指标排序: {derived_name} -> {formula_sql}")
                        except Exception as e:
                            logger.warning(f"解析派生指标排序表达式失败: {formula_sql}, error={e}")
                    else:
                        logger.warning(f"明细查询中无法按派生指标 {derived_name} 排序（依赖字段不完整或为纯聚合指标）")
                # 1) 从measures中查找
                elif ir.sort_by in self.model.measures:
                    measure = self.model.measures[ir.sort_by]
                    order_column = measure.column
                    order_table = measure.table
                # 2) 从dimensions中查找
                elif ir.sort_by in self.model.dimensions:
                    dim = self.model.dimensions[ir.sort_by]
                    order_column = dim.column
                    order_table = dim.table
                else:
                    logger.warning(f"未找到排序字段: {ir.sort_by}")

                if order_column:
                    order_expr = exp.column(order_column, table=self._get_table_alias(order_table))
                
                if order_expr is not None:
                    # 使用exp.Ordered来指定排序方向
                    ordered_expr = exp.Ordered(this=order_expr, desc=desc)
                    query = query.order_by(ordered_expr)
                    logger.debug(f"添加明细查询 ORDER BY: {ir.sort_by} {ir.sort_order.upper()}")

            # 聚合查询：优先按聚合列/别名排序，避免违反 GROUP BY 规则
            elif ir.query_type == "aggregation":
                order_expr = None
                desc = (ir.sort_order == "desc")

                # 0) 若sort_by指向派生指标，使用派生指标的显示名（别名）
                is_derived, derived_name, derived_def = self._is_derived_metric(ir.sort_by)
                if is_derived and derived_name:
                    # 派生指标的别名就是其名称
                    order_expr = exp.column(derived_name)
                    logger.debug(f"聚合查询按派生指标别名排序: {derived_name}")
                # 1) 若sort_by指向metric，使用metric别名
                elif ir.sort_by in getattr(self.model, "metrics", {}):
                    # 优先使用 IR 中指定的别名（如带单位），否则使用 display_name
                    alias = self._get_alias_from_ir_metrics(ir, ir.sort_by) or self._get_metric_or_measure_alias(ir.sort_by)
                    order_expr = exp.column(alias)
                    logger.debug(f"聚合查询按指标别名排序: {alias}")
                # 2) 若sort_by指向measure（度量字段），使用度量别名（与SELECT中的AS一致）
                elif ir.sort_by in getattr(self.model, "measures", {}):
                    # 优先使用 IR 中指定的别名（如带单位），否则使用 display_name
                    alias = self._get_alias_from_ir_metrics(ir, ir.sort_by) or self._get_metric_or_measure_alias(ir.sort_by)
                    order_expr = exp.column(alias)
                    logger.debug(f"聚合查询按度量别名排序: {alias}")
                # 3) 若sort_by指向维度，退回按维度物理列排序
                elif ir.sort_by in getattr(self.model, "dimensions", {}):
                    dim = self.model.dimensions[ir.sort_by]
                    order_expr = exp.column(dim.column, table=self._get_table_alias(dim.table))
                    logger.debug(f"聚合查询按维度物理列排序: {dim.column}")
                # 4) 若sort_by指向统一字段表中的度量字段，使用别名排序
                elif hasattr(self.model, 'fields') and ir.sort_by in self.model.fields:
                    field = self.model.fields[ir.sort_by]
                    if field.field_category == 'measure':
                        # 优先使用 IR 中指定的别名（如带单位），否则使用 display_name
                        alias = self._get_alias_from_ir_metrics(ir, ir.sort_by) or field.display_name or field.column_name
                        order_expr = exp.column(alias)
                        logger.debug(f"聚合查询按统一字段度量别名排序: {alias}")
                    else:
                        # 维度字段按物理列排序
                        order_expr = exp.column(field.column_name, table=self._get_table_alias(field.datasource_id))
                        logger.debug(f"聚合查询按统一字段维度物理列排序: {field.column_name}")
                else:
                    logger.warning(f"聚合查询中未找到排序字段: {ir.sort_by}")

                if order_expr is not None:
                    ordered_expr = exp.Ordered(this=order_expr, desc=desc)
                    query = query.order_by(ordered_expr)
                    logger.debug(f"添加聚合查询 ORDER BY: sort_by={ir.sort_by}, desc={desc}")

        elif ir.order_by:
            # 兼容旧的order_by字段
            for order in ir.order_by:
                order_expr = None

                # 聚合查询：对度量/指标优先按别名排序，避免违反GROUP BY
                if ir.query_type == "aggregation":
                    # 0) 若order.field指向派生指标，使用派生指标的显示名（别名）
                    is_derived, derived_name, derived_def = self._is_derived_metric(order.field)
                    if is_derived and derived_name:
                        order_expr = exp.column(derived_name)
                        logger.debug(f"聚合(order_by)按派生指标别名排序: {derived_name}")
                    elif order.field in getattr(self.model, "metrics", {}):
                        # 优先使用 IR 中指定的别名（如带单位），否则使用 display_name
                        alias = self._get_alias_from_ir_metrics(ir, order.field) or self._get_metric_or_measure_alias(order.field)
                        order_expr = exp.column(alias)
                        logger.debug(f"聚合(order_by)按指标别名排序: {alias}")
                    elif order.field in getattr(self.model, "measures", {}):
                        # 优先使用 IR 中指定的别名（如带单位），否则使用 display_name
                        alias = self._get_alias_from_ir_metrics(ir, order.field) or self._get_metric_or_measure_alias(order.field)
                        order_expr = exp.column(alias)
                        logger.debug(f"聚合(order_by)按度量别名排序: {alias}")
                    elif order.field in getattr(self.model, "dimensions", {}):
                        dim = self.model.dimensions[order.field]
                        order_expr = exp.column(dim.column, table=self._get_table_alias(dim.table))
                        logger.debug(f"聚合(order_by)按维度物理列排序: {dim.column}")
                    # 检查统一字段表中的度量字段
                    elif hasattr(self.model, 'fields') and order.field in self.model.fields:
                        field = self.model.fields[order.field]
                        if field.field_category == 'measure':
                            # 优先使用 IR 中指定的别名（如带单位），否则使用 display_name
                            alias = self._get_alias_from_ir_metrics(ir, order.field) or field.display_name or field.column_name
                            order_expr = exp.column(alias)
                            logger.debug(f"聚合(order_by)按统一字段度量别名排序: {alias}")
                        else:
                            # 维度字段按物理列排序
                            order_expr = exp.column(field.column_name, table=self._get_table_alias(field.datasource_id))
                            logger.debug(f"聚合(order_by)按统一字段维度物理列排序: {field.column_name}")
                    else:
                        order_expr = exp.column(order.field)
                        logger.warning(f"聚合(order_by)未知排序字段，按原名排序: {order.field}")
                else:
                    # 非聚合或明细查询：保持原有行为，按物理列排序
                    # 0) 检查是否为派生指标
                    is_derived, derived_name, derived_def = self._is_derived_metric(order.field)
                    if is_derived and derived_name and derived_def:
                        # 明细查询中，派生指标需要转换为行级计算表达式
                        table_alias = self._get_table_alias(main_table)
                        formula_sql, alias = self._build_derived_metric_for_detail(derived_name, derived_def, table_alias)
                        if formula_sql:
                            try:
                                order_expr = parse_one(formula_sql, dialect=self.dialect)
                                logger.debug(f"非聚合(order_by)按派生指标排序: {derived_name} -> {formula_sql}")
                            except Exception as e:
                                logger.warning(f"解析派生指标排序表达式失败: {formula_sql}, error={e}")
                                order_expr = exp.column(order.field)
                        else:
                            logger.warning(f"非聚合查询中无法按派生指标 {derived_name} 排序")
                            order_expr = exp.column(order.field)
                    elif order.field in self.model.dimensions:
                        dim = self.model.dimensions[order.field]
                        order_expr = exp.column(dim.column, table=self._get_table_alias(dim.table))
                    elif order.field in self.model.measures:
                        measure = self.model.measures[order.field]
                        order_expr = exp.column(measure.column, table=self._get_table_alias(measure.table))
                    else:
                        order_expr = exp.column(order.field)

                if order_expr is not None:
                    ordered_expr = exp.Ordered(this=order_expr, desc=order.desc)
                    query = query.order_by(ordered_expr)

        # 9. 添加 LIMIT（后续会被规则引擎处理）
        if ir.limit:
            query = query.limit(ir.limit)

        # 10. 处理汇总行（with_total）
        if ir.with_total and ir.dimensions:
            logger.debug("检测到 with_total=True，生成带汇总行的查询")
            query = self._add_total_row(query, ir, main_table, joins, where_conditions)

        # 11. 处理 detail 查询的 having_filters（派生指标过滤）
        # 使用子查询包装：SELECT * FROM (原查询) t WHERE 派生指标条件
        if (hasattr(ir, 'having_filters') and ir.having_filters and ir.query_type == "detail"):
            query = self._wrap_detail_query_with_having_filters(query, ir, main_table)

        # 12. 处理同比/环比计算（需要子查询包装）
        # 派生指标和原子指标都包含聚合函数，不能直接放在 LAG() 窗口函数内部，需要先聚合再计算
        if ir.comparison_type and ir.show_growth_rate:
            # 检查是否有需要同比计算的指标（派生指标或原子指标）
            def get_metric_id(m):
                if isinstance(m, str):
                    return m
                elif isinstance(m, dict):
                    return m.get("field", "")
                elif hasattr(m, "field"):
                    return m.field
                return ""
            has_metrics_for_comparison = any(
                get_metric_id(m) != "__row_count__" for m in ir.metrics
            )
            if has_metrics_for_comparison:
                logger.debug("检测到同比计算需求，使用子查询包装")
                query = self._wrap_with_comparison_window_functions(query, ir, main_table)

        logger.debug("AST 构建完成")
        return query

    def _build_select_clause(self, ir: IntermediateRepresentation, main_table: str) -> List[exp.Expression]:
        """构建 SELECT 子句（支持标注字段和明细查询）"""
        select_exprs = []
        user_specified_exprs = []  # 用户明确要求的字段，优先显示
        default_exprs = []  # 默认字段，放在后面

        # 明细查询仅在"未明确展示字段"时才使用默认列；
        # 若用户已通过 dimensions 明确字段，则按常规分支构建 SELECT。
        is_detail_query = ir.query_type == "detail"
        suppress_detail_defaults = is_detail_query and ir.suppress_detail_defaults
        if is_detail_query and not ir.dimensions and not ir.metrics:
            if suppress_detail_defaults:
                logger.warning(
                    "明细查询被指示禁止默认列，但未提供任何字段，回退到默认列以避免空结果",
                    table=main_table
                )
            logger.debug(f"明细查询（未明确字段），返回表 {main_table} 的默认列")
            return self._get_detail_columns(main_table, ir)
        
        # 明细查询中有LLM指定的字段：优先显示LLM字段
        # 兜底策略：
        # - 如果LLM选择的字段数量足够多（≥3个），不追加默认字段
        # - 如果LLM选择的字段数量较少（<3个），追加默认字段作为补充（show_in_detail=True的字段）
        should_add_defaults = False
        llm_field_count = len(ir.dimensions or []) + len(ir.metrics or [])
        
        if (
            is_detail_query
            and (ir.dimensions or ir.metrics)
            and not suppress_detail_defaults
        ):
            if llm_field_count < 3:
                # LLM选择的字段太少，需要补充默认字段
                logger.debug(f"明细查询：LLM选择了{llm_field_count}个字段（<3），将补充默认列")
                should_add_defaults = True
            else:
                # LLM选择的字段足够多，信任LLM的选择
                logger.debug(f"明细查询：LLM选择了{llm_field_count}个字段（≥3），使用LLM选择的字段")

        # 添加维度（去重）
        seen_dimensions = set()
        for dim_id in ir.dimensions:
            if dim_id in seen_dimensions:
                logger.debug(f"跳过重复的维度: {dim_id}")
                continue
            seen_dimensions.add(dim_id)

            #  新架构支持：兼容旧架构(dimensions)和新架构(fields)
            dim_expr = None
            if dim_id in self.model.dimensions:
                dim = self.model.dimensions[dim_id]
                dim_expr = exp.column(dim.column, table=self._get_table_alias(dim.table)).as_(self._get_dimension_alias(dim_id))
            elif dim_id in self.model.fields:
                field = self.model.fields[dim_id]
                dim_expr = exp.column(field.column, table=self._get_table_alias(field.datasource_id))
                
                # 检查是否为空间字段，如果是则转换
                if self._is_spatial_type(field.data_type):
                    dim_expr = self._wrap_spatial_expression(dim_expr)
                    logger.debug(f"空间维度转换为WKT: {dim_id}")
                
                dim_expr = dim_expr.as_(self._get_dimension_alias(dim_id))
                logger.debug(f"从统一字段表获取维度: {dim_id}")
            else:
                raise CompilationError(f"维度字段不存在: {dim_id}")
            
            # 根据是否为明细查询+用户指定，决定添加到哪个列表
            if should_add_defaults:
                user_specified_exprs.append(dim_expr)
            else:
                select_exprs.append(dim_expr)

        #  添加标注字段：检查被过滤但未分组的维度
        # 如果维度配置了 include_in_result=true 且被用于过滤，则添加到 SELECT
        if ir.filters:
            #  新架构支持：同时检查dimensions和fields
            filtered_dim_ids = {f.field for f in ir.filters
                              if f.field in self.model.dimensions or f.field in self.model.fields}
            for dim_id in filtered_dim_ids:
                if dim_id not in ir.dimensions:  # 不在分组维度中
                    # 尝试从dimensions或fields中获取
                    dim = None
                    if dim_id in self.model.dimensions:
                        dim = self.model.dimensions[dim_id]
                        column_name = dim.column
                        table_name = dim.table
                    elif dim_id in self.model.fields:
                        field = self.model.fields[dim_id]
                        # 检查field是否有include_in_result属性（新架构可能没有）
                        if not hasattr(field, 'include_in_result') or not field.include_in_result:
                            continue
                        column_name = field.column
                        table_name = field.datasource_id
                    else:
                        continue

                    if dim and hasattr(dim, 'include_in_result') and dim.include_in_result:
                        # 添加标注字段
                        # 如果查询有维度分组，直接添加字段并加入GROUP BY
                        # 如果查询只有聚合（无分组），用MAX/MIN包裹避免SQL错误
                        display_name = dim.display_name if dim else field.display_name
                        logger.debug(f"添加标注字段: {column_name} (用于标识{display_name})")

                        if ir.dimensions:
                            # 有分组：直接添加字段
                            select_exprs.append(
                                exp.column(column_name, table=self._get_table_alias(table_name)).as_(f"{self._get_dimension_alias(dim_id)}")
                            )
                        else:
                            # 无分组：用MAX聚合包裹（因为LIKE匹配后理论上只有1个值或少数值）
                            logger.debug(f"使用MAX聚合包裹标注字段（无GROUP BY场景）")
                            label_expr = exp.Max(
                                this=exp.column(column_name, table=self._get_table_alias(table_name))
                            )
                            select_exprs.append(label_expr.as_(self._get_dimension_alias(dim_id)))

        # 添加指标（去重）
        # 重要：严格按照IR中的顺序添加指标，不做任何重排序
        seen_metrics = set()  # 跟踪已添加的指标，避免重复
        # 派生指标依赖字段（用于合计行/比率重算）：避免重复注入
        added_derived_dep_fields = set()
        for metric_item in ir.metrics:
            # 规范化 metric：支持字符串或 MetricSpec 格式
            metric_id, metric_agg, metric_alias, metric_decimal = self._normalize_metric_spec(metric_item)
            
            # 跳过重复的指标（使用 field_id + aggregation 作为唯一键）
            metric_key = f"{metric_id}:{metric_agg}"
            if metric_key in seen_metrics:
                logger.debug(f"跳过重复的指标: {metric_key}")
                continue
            seen_metrics.add(metric_key)
            
            # 处理保留字：__row_count__
            if metric_id == "__row_count__":
                # 明细查询(detail)不支持聚合函数，跳过 __row_count__
                if is_detail_query:
                    logger.debug("明细查询中跳过 __row_count__（COUNT(*)与detail查询不兼容）")
                    continue
                logger.debug("检测到保留字 __row_count__，生成 COUNT(*)")
                count_expr = exp.Count(this=exp.Star())
                # 优先使用 LLM 指定的别名，否则使用默认的"记录数"
                alias = metric_alias if metric_alias else "记录数"
                if should_add_defaults:
                    user_specified_exprs.append(count_expr.as_(alias))
                else:
                    select_exprs.append(count_expr.as_(alias))
                logger.debug(f"添加记录数统计: COUNT(*) AS {alias}")
                continue
            
            #  检查是否为派生指标
            is_derived, derived_name, derived_def = self._is_derived_metric(metric_id)
            if is_derived:
                try:
                    # 使用表别名而不是table_id
                    table_alias = self._get_table_alias(main_table)
                    
                    # 根据查询类型选择构建方法
                    if is_detail_query:
                        # 明细查询：去除聚合函数，转换为行级计算
                        formula_sql, alias = self._build_derived_metric_for_detail(derived_name, derived_def, table_alias)
                        if formula_sql is None:
                            # 纯聚合指标（如COUNT），在明细查询中跳过
                            logger.debug(f"明细查询中跳过纯聚合派生指标: {derived_name}")
                            continue
                    else:
                        # 聚合查询：使用完整的聚合公式
                        formula_sql, alias = self._build_derived_metric_expression(derived_name, derived_def, table_alias)
                    
                    # 检查是否成功构建派生指标（字段不完整时返回 None）
                    if formula_sql is None:
                        logger.warning(f"派生指标 {derived_name} 依赖字段不完整，跳过此指标")
                        continue
                    
                    derived_expr = parse_one(formula_sql, dialect=self.dialect)
                    # 根据是否为明细查询+用户指定，决定添加到哪个列表
                    if should_add_defaults:
                        user_specified_exprs.append(derived_expr.as_(alias))
                    else:
                        select_exprs.append(derived_expr.as_(alias))
                    logger.debug(f"添加派生指标: {derived_name} -> {formula_sql}")

                    # 重要：当 SQL 未生成合计行（with_total=False）时，格式化层会补合计行并尝试重算“比率类派生指标”的合计值。
                    # 但派生指标公式往往引用“字段显示名”（如 成交价/土地面积/建筑面积），而结果集里未必包含这些字段的聚合列。
                    # 这里注入派生指标依赖字段的聚合隐藏列（_dep_<field_id>），供格式化层稳定获取分子/分母合计。
                    if (not is_detail_query) and (not getattr(ir, "with_total", False)):
                        try:
                            field_deps = (derived_def or {}).get("field_dependencies") or []
                            for dep in field_deps:
                                dep_field_id = dep.get("field_id")
                                if not dep_field_id or dep_field_id in added_derived_dep_fields:
                                    continue
                                if not hasattr(self.model, "fields") or dep_field_id not in self.model.fields:
                                    continue
                                dep_field = self.model.fields[dep_field_id]
                                dep_col_name = getattr(dep_field, "column", None) or getattr(dep_field, "field_name", None)
                                dep_ds_id = getattr(dep_field, "datasource_id", None) or main_table
                                dep_col_expr = exp.column(dep_col_name, table=self._get_table_alias(dep_ds_id))
                                dep_agg = (dep.get("aggregation") or "SUM").upper()
                                dep_agg_expr = self._build_aggregation_expr(dep_col_expr, dep_agg, None)
                                dep_alias = f"_dep_{dep_field_id}"
                                select_exprs.append(dep_agg_expr.as_(dep_alias))
                                added_derived_dep_fields.add(dep_field_id)
                        except Exception as e:
                            logger.warning("注入派生指标依赖隐藏列失败（已忽略）", error=str(e), metric=derived_name)
                    continue
                except Exception as e:
                    logger.error(f"构建派生指标表达式失败: {derived_name}", error=str(e))
                    raise CompilationError(f"派生指标 {derived_name} 构建失败: {str(e)}")

            #  容错：如果 metric_id 不在 metrics 中，检查是否在 measures 中
            if metric_id not in self.model.metrics:
                if metric_id in self.model.measures:
                    if ir.query_type == "detail":
                        # 明细查询中，如果用户明确指定了 measure 字段，添加到 SELECT
                        # （注意：默认列可能已经包含，但为了确保显示，这里显式添加）
                        measure_field = self.model.measures[metric_id]
                        col_name = self._get_physical_column_name(metric_id)
                        col_expr = exp.column(col_name, table=self._get_table_alias(main_table))
                        
                        # 检查是否为空间字段，如果是则转换
                        if self._is_spatial_type(measure_field.data_type):
                            col_expr = self._wrap_spatial_expression(col_expr)
                            logger.debug(f"空间度量转换为WKT: {metric_id}")
                        
                        # 使用显示名作为别名
                        display_name = measure_field.display_name if hasattr(measure_field, 'display_name') else col_name
                        # 根据是否为明细查询+用户指定，决定添加到哪个列表
                        if should_add_defaults:
                            user_specified_exprs.append(col_expr.as_(display_name))
                        else:
                            select_exprs.append(col_expr.as_(display_name))
                        logger.debug(f"明细查询中添加用户指定的度量字段: {display_name}")
                        continue
                    else:
                        # 聚合查询：使用指定的聚合函数构建聚合表达式
                        logger.debug(f"聚合查询中使用度量字段 '{metric_id}' [{metric_agg}]，构建聚合表达式")
                        measure_field = self.model.measures[metric_id]
                        
                        # 构建列表达式
                        col_name = getattr(measure_field, 'column', None) or getattr(measure_field, 'field_name', None)
                        ds_id = getattr(measure_field, 'datasource_id', None) or main_table
                        col_expr = exp.column(col_name, table=self._get_table_alias(ds_id))
                        agg_expr = self._build_aggregation_expr(col_expr, metric_agg, metric_decimal)
                        
                        # 确定别名：优先使用 MetricSpec 中指定的别名
                        if metric_alias:
                            alias_name = metric_alias
                        elif metric_agg != "SUM":
                            # 非默认聚合时，标注聚合类型
                            base_name = self._get_metric_or_measure_alias(metric_id)
                            alias_name = f"{base_name}_{metric_agg.lower()}"
                        else:
                            alias_name = self._get_metric_or_measure_alias(metric_id)
                        
                        if should_add_defaults:
                            user_specified_exprs.append(agg_expr.as_(alias_name))
                        else:
                            select_exprs.append(agg_expr.as_(alias_name))
                        logger.debug(f"添加度量聚合列: {metric_id} [{metric_agg}] -> {alias_name}")
                        continue  # 跳过后面的 metric 处理逻辑
                # 检查统一字段表
                elif metric_id in self.model.fields:
                    field = self.model.fields[metric_id]
                    if field.field_category == 'measure':
                        if ir.query_type == "detail":
                            # 明细查询：直接输出字段
                            col_name = field.column_name
                            col_expr = exp.column(col_name, table=self._get_table_alias(field.datasource_id))
                            display_name = field.display_name if hasattr(field, 'display_name') else col_name
                            select_exprs.append(col_expr.as_(display_name))
                            logger.debug(f"明细查询中添加度量字段: {display_name}")
                            continue
                        else:
                            # 聚合查询：使用指定的聚合函数构建聚合表达式
                            col_name = field.column_name
                            col_expr = exp.column(col_name, table=self._get_table_alias(field.datasource_id))
                            agg_expr = self._build_aggregation_expr(col_expr, metric_agg, metric_decimal)
                            
                            # 确定别名：优先使用 MetricSpec 中指定的别名
                            if metric_alias:
                                alias_name = metric_alias
                            elif metric_agg != "SUM":
                                # 非默认聚合时，在显示名中标注聚合类型
                                base_name = field.display_name or col_name
                                alias_name = f"{base_name}_{metric_agg.lower()}"
                            else:
                                alias_name = field.display_name or col_name
                            
                            if should_add_defaults:
                                user_specified_exprs.append(agg_expr.as_(alias_name))
                            else:
                                select_exprs.append(agg_expr.as_(alias_name))
                            logger.debug(f"添加统一字段度量聚合列: {metric_id} [{metric_agg}] -> {alias_name}")
                            continue
                else:
                    raise CompilationError(f"指标不存在: {metric_id}（IR 必须使用有效的字段 UUID）")

            metric = self.model.metrics[metric_id]

            # 获取指标表达式
            expression = self._get_metric_expression(metric)

            # 检查是否为派生指标
            if metric.metric_type == "derived":
                # 派生指标：使用 formula 生成 SQL
                try:
                    derived_expr = self._build_derived_metric(metric_id, metric, alias_override=self._get_metric_or_measure_alias(metric_id))
                    select_exprs.append(derived_expr)
                    logger.debug(f"派生指标生成成功: {metric_id}")
                except Exception as e:
                    logger.error(f"派生指标生成失败: {metric_id}", error=str(e))
                    raise CompilationError(f"派生指标 '{metric_id}' 生成失败: {str(e)}")
            else:
                # 原子指标：解析 expression
                if not expression:
                    logger.error(f"指标缺少 expression: {metric_id}")
                    raise CompilationError(f"指标 '{metric_id}' 缺少 expression 定义")

                try:
                    metric_expr = parse_one(expression, dialect=self.dialect)
                    select_exprs.append(metric_expr.as_(self._get_metric_or_measure_alias(metric_id)))
                except Exception as e:
                    logger.error(f"解析指标表达式失败: {metric_id}", error=str(e))
                    raise CompilationError(f"指标表达式错误 {metric_id}: {str(e)}")
        
        # 添加窗口函数列（SQL Server 2012+）
        select_exprs.extend(self._build_window_function_columns(ir, main_table))

        # ========== 混合架构扩展：条件聚合、计算字段、占比指标 ==========
        
        # 处理条件聚合 (conditional_metrics)
        if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
            for cond_metric in ir.conditional_metrics:
                cond_expr = self._build_conditional_metric_expression(cond_metric, main_table)
                if cond_expr:
                    if should_add_defaults:
                        user_specified_exprs.append(cond_expr)
                    else:
                        select_exprs.append(cond_expr)
                    logger.debug(f"添加条件聚合指标: {cond_metric.alias}")
        
        # 处理计算字段 (calculated_fields)
        # 判断是否为聚合查询（有 dimensions 或 query_type 为 aggregation）
        is_aggregation_query = (
            ir.query_type == "aggregation" or 
            (bool(ir.dimensions) and ir.query_type not in ("detail", "window_detail", "duplicate_detection"))
        )
        if hasattr(ir, 'calculated_fields') and ir.calculated_fields:
            for calc_field in ir.calculated_fields:
                calc_expr = self._build_calculated_field_expression(
                    calc_field, main_table, is_aggregation_query=is_aggregation_query
                )
                if calc_expr:
                    if should_add_defaults:
                        user_specified_exprs.append(calc_expr)
                    else:
                        select_exprs.append(calc_expr)
                    logger.debug(f"添加计算字段: {calc_field.alias}")
        
        # 处理占比指标 (ratio_metrics)
        if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
            # 判断是否需要全局分母（当有分组维度且没有 numerator_condition 时）
            has_dimensions = bool(ir.dimensions)
            
            # 收集 conditional_metrics 已生成的分子列签名，避免重复生成
            # 签名格式: (numerator_field_id, condition_field_id, condition_op, condition_value)
            existing_conditional_signatures = set()
            if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
                for cond_metric in ir.conditional_metrics:
                    if cond_metric.condition:
                        sig = (
                            cond_metric.field,
                            cond_metric.condition.field,
                            cond_metric.condition.op,
                            str(cond_metric.condition.value)
                        )
                        existing_conditional_signatures.add(sig)
            
            for ratio_metric in ir.ratio_metrics:
                # 自动为有 numerator_condition 的占比指标生成分子列
                # 让用户能够看到分子的具体数值，而不仅仅是占比
                # 但如果 conditional_metrics 已经生成了相同的分子列，则跳过
                if ratio_metric.numerator_condition:
                    ratio_sig = (
                        ratio_metric.numerator_field,
                        ratio_metric.numerator_condition.field,
                        ratio_metric.numerator_condition.op,
                        str(ratio_metric.numerator_condition.value)
                    )
                    
                    if ratio_sig in existing_conditional_signatures:
                        logger.debug(
                            f"跳过自动生成占比分子列: {ratio_metric.numerator_condition.value}（已有 conditional_metrics 生成）"
                        )
                    else:
                        numerator_expr = self._build_ratio_numerator_column(
                            ratio_metric, main_table
                        )
                        if numerator_expr:
                            if should_add_defaults:
                                user_specified_exprs.append(numerator_expr)
                            else:
                                select_exprs.append(numerator_expr)
                            logger.debug(f"自动添加占比分子列: {ratio_metric.numerator_condition.value}")
                
                ratio_expr = self._build_ratio_metric_expression(ratio_metric, main_table, has_dimensions)
                if ratio_expr:
                    if should_add_defaults:
                        user_specified_exprs.append(ratio_expr)
                    else:
                        select_exprs.append(ratio_expr)
                    logger.debug(f"添加占比指标: {ratio_metric.alias}")

        # 如果需要添加默认字段，合并列表：用户字段在前，默认字段在后
        if should_add_defaults:
            # 获取默认字段
            default_exprs = self._get_detail_columns(main_table, ir)
            
            if user_specified_exprs:
                logger.debug(f"合并字段列表：{len(user_specified_exprs)}个用户字段 + 默认字段")
                # 提取用户字段的别名，避免重复显示
                user_field_aliases = set()
                for expr in user_specified_exprs:
                    if hasattr(expr, 'alias'):
                        user_field_aliases.add(expr.alias)
                
                # 过滤掉已经在用户字段中的默认字段
                filtered_defaults = []
                for expr in default_exprs:
                    alias = expr.alias if hasattr(expr, 'alias') else None
                    if alias not in user_field_aliases:
                        filtered_defaults.append(expr)
                    else:
                        logger.debug(f"跳过重复的默认字段: {alias}")
                
                logger.debug(f"过滤后保留 {len(filtered_defaults)} 个默认字段（已去重）")
                # 用户字段 + 过滤后的默认字段
                return user_specified_exprs + filtered_defaults
            else:
                # 用户指定的字段全部被跳过（如派生指标依赖字段不存在），回退到默认列
                logger.warning("用户指定的字段全部无法解析，回退使用默认列")
                return default_exprs
        
        # 最后检查：确保 select_exprs 不为空
        if not select_exprs:
            logger.error("SELECT 列表为空，回退使用默认列")
            return self._get_detail_columns(main_table, ir)

        return select_exprs

    def _normalize_metric_spec(self, metric_item) -> tuple:
        """
        规范化 metrics 中的元素，支持两种格式：
        1. 字符串：字段ID，使用默认 SUM 聚合
        2. MetricSpec 对象或字典：指定聚合函数
        
        Args:
            metric_item: str 或 MetricSpec 或 dict
            
        Returns:
            (field_id, aggregation, alias, decimal_places) 元组
            - field_id: 字段ID
            - aggregation: 聚合函数类型（SUM/AVG/MIN/MAX/COUNT）
            - alias: 别名（可能为 None）
            - decimal_places: 小数位数
        """
        from server.models.ir import MetricSpec
        
        # 字符串格式：使用默认 SUM
        if isinstance(metric_item, str):
            return (metric_item, "SUM", None, 2)
        
        # MetricSpec 对象
        if isinstance(metric_item, MetricSpec):
            return (
                metric_item.field,
                metric_item.aggregation,
                metric_item.alias,
                metric_item.decimal_places
            )
        
        # 字典格式（兼容 JSON 反序列化）
        if isinstance(metric_item, dict):
            return (
                metric_item.get("field"),
                metric_item.get("aggregation", "SUM"),
                metric_item.get("alias"),
                metric_item.get("decimal_places", 2)
            )
        
        # 未知格式，尝试转换为字符串
        logger.warning(f"未知的 metric 格式: {type(metric_item)}, 尝试转换为字符串")
        return (str(metric_item), "SUM", None, 2)
    
    def _build_aggregation_expr(
        self, 
        col_expr: exp.Expression, 
        aggregation: str,
        decimal_places: int = 2
    ) -> exp.Expression:
        """
        根据聚合类型构建聚合表达式
        
        Args:
            col_expr: 列表达式
            aggregation: 聚合函数类型（SUM/AVG/MIN/MAX/COUNT）
            decimal_places: 小数位数
            
        Returns:
            聚合表达式
        """
        agg_upper = aggregation.upper()
        
        if agg_upper == "SUM":
            agg_expr = exp.Sum(this=col_expr)
        elif agg_upper == "AVG":
            agg_expr = exp.Avg(this=col_expr)
        elif agg_upper == "MIN":
            agg_expr = exp.Min(this=col_expr)
        elif agg_upper == "MAX":
            agg_expr = exp.Max(this=col_expr)
        elif agg_upper == "COUNT":
            agg_expr = exp.Count(this=col_expr)
        elif agg_upper == "COUNT_DISTINCT":
            agg_expr = exp.Count(this=col_expr, distinct=True)
        else:
            # 默认使用 SUM
            logger.warning(f"未知聚合类型 {aggregation}，使用默认 SUM")
            agg_expr = exp.Sum(this=col_expr)
        
        # 对数值聚合添加 ROUND 处理（COUNT 除外，因为是整数）
        if agg_upper not in ("COUNT", "COUNT_DISTINCT") and decimal_places is not None:
            agg_expr = exp.Round(this=agg_expr, decimals=exp.Literal.number(decimal_places))
        
        return agg_expr

    def _build_agg_for_measure(self, field) -> str:
        """根据度量字段构建聚合表达式，支持 COUNT_DISTINCT / SUM 等。
        返回可被 sqlglot 解析的字符串表达式。
        """
        ds_id = getattr(field, 'datasource_id', None)
        column = getattr(field, 'column', None) or getattr(field, 'field_name', None)
        agg = None
        try:
            if hasattr(field, 'measure_props') and field.measure_props:
                agg = getattr(field.measure_props, 'default_aggregation', None)
        except Exception:
            pass
        if not agg:
            agg = 'SUM'

        # 按方言对标识符进行转义，确保如 GUID 表别名、中文列名 可被正确解析
        def _q(name: str) -> str:
            s = str(name)
            if self.dialect == "tsql":
                return f"[{s.replace(']', ']]')}]"
            elif self.dialect == "mysql":
                return f"`{s.replace('`', '``')}`"
            elif self.dialect in ("postgres", "postgresql"):
                return f'"{s}"'
            return s

        # 使用友好别名
        table_alias = self._get_table_alias(ds_id)
        qualified = f"{_q(table_alias)}.{_q(column)}"
        if agg.upper() == 'COUNT_DISTINCT':
            return f"COUNT(DISTINCT {qualified})"
        return f"{agg}({qualified})"

    def _build_agg_for_field(self, field) -> str:
        """根据统一字段表中的度量字段构建聚合表达式。
        返回可被 sqlglot 解析的字符串表达式。
        """
        ds_id = field.datasource_id
        column = field.column_name

        # 从 measure_props 获取聚合类型
        agg = 'SUM'
        if hasattr(field, 'measure_props') and field.measure_props:
            agg = getattr(field.measure_props, 'default_aggregation', 'SUM') or 'SUM'

        # 按方言对标识符进行转义
        def _q(name: str) -> str:
            s = str(name)
            if self.dialect == "tsql":
                return f"[{s.replace(']', ']]')}]"
            elif self.dialect == "mysql":
                return f"`{s.replace('`', '``')}`"
            elif self.dialect in ("postgres", "postgresql"):
                return f'"{s}"'
            return s

        table_alias = self._get_table_alias(ds_id)
        qualified = f"{_q(table_alias)}.{_q(column)}"
        if agg.upper() == 'COUNT_DISTINCT':
            return f"COUNT(DISTINCT {qualified})"
        return f"{agg}({qualified})"

    def _build_derived_metric(self, metric_id: str, metric, alias_override: str | None = None) -> exp.Expression:
        """
        构建派生指标的 SQL 表达式

        派生指标的计算逻辑：
        1. 解析 formula 字段（如 "SUM(total_price) / (SUM(deal_area) * 0.0015)"）
        2. 将 formula 中的指标名称替换为实际的 SQL 表达式
        3. 返回完整的计算表达式

        Args:
            metric_id: 派生指标的 ID
            metric: 派生指标对象

        Returns:
            SQLGlot 表达式，带有别名

        Example:
            calculated_price_per_mu:
              formula: "SUM(total_price) / (SUM(deal_area) * 0.0015)"
              depends_on:
                - metric: total_deal_price  # expression: "SUM(f_public_deal.总价)"
                - metric: total_deal_area   # expression: "SUM(f_public_deal.出让面积)"

            生成 SQL:
            (SUM(f_public_deal.总价) / (SUM(f_public_deal.出让面积) * 0.0015)) AS calculated_price_per_mu
        """
        # 获取派生指标的公式
        formula = None
        if hasattr(metric, 'derived_def') and metric.derived_def:
            formula = metric.derived_def.formula
        elif hasattr(metric, 'formula'):
            formula = metric.formula

        if not formula:
            raise CompilationError(f"派生指标 '{metric_id}' 缺少 formula 定义")

        logger.debug(f"开始构建派生指标: {metric_id}", formula=formula)

        # 构建指标名称到表达式的映射
        metric_map = {}
        dependencies = metric.dependencies if hasattr(metric, 'dependencies') else (metric.depends_on if hasattr(metric, 'depends_on') else [])
        for dep in dependencies:
            dep_metric_id = dep.depends_on_id if hasattr(dep, 'depends_on_id') else (dep.metric if hasattr(dep, 'metric') else None)
            if not dep_metric_id:
                continue
            if dep_metric_id not in self.model.metrics:
                raise CompilationError(
                    f"派生指标 '{metric_id}' 依赖的指标不存在: {dep_metric_id}"
                )

            dep_metric = self.model.metrics[dep_metric_id]
            dep_expression = self._get_metric_expression(dep_metric)
            if not dep_expression:
                raise CompilationError(
                    f"派生指标 '{metric_id}' 依赖的指标 '{dep_metric_id}' 缺少 expression"
                )

            # 存储依赖指标的表达式
            metric_map[dep_metric_id] = dep_expression
            logger.debug(f"映射依赖: {dep_metric_id} -> {dep_expression}")

        # 替换 formula 中的指标名称
        # formula 示例: "SUM(total_price) / (SUM(deal_area) * 0.0015)"
        # 需要替换: total_price -> total_deal_price 的 expression
        #          deal_area -> total_deal_area 的 expression

        formula_sql = formula

        # 简单的字符串替换策略
        # 对于 "SUM(total_price)" 这样的模式，我们需要：
        # 1. 找到 total_price，映射到 total_deal_price
        # 2. 获取 total_deal_price 的 expression: "SUM(f_public_deal.总价)"
        # 3. 提取聚合内部的字段: "f_public_deal.总价"
        # 4. 替换回去: "SUM(f_public_deal.总价)"

        # 更简单的方法：直接用依赖指标的完整表达式替换
        # "SUM(total_price)" -> 找到 depends_on 中 component=numerator 的指标

        # 为了简化，我们采用另一种方法：
        # 直接将 formula 中的占位符（如 total_price）替换为实际表达式的内容

        #  SQL Server不支持在同一SELECT中引用列别名，需要用原始表达式替换
        # 将 formula 中的指标ID替换为对应的原始SQL表达式

        for dep in dependencies:
            dep_metric_id = dep.depends_on_id if hasattr(dep, 'depends_on_id') else (dep.metric if hasattr(dep, 'metric') else None)
            if not dep_metric_id:
                continue
            if dep_metric_id not in self.model.metrics:
                raise CompilationError(
                    f"派生指标 '{metric_id}' 依赖的指标不存在: {dep_metric_id}"
                )

            dep_metric = self.model.metrics[dep_metric_id]
            dep_expression = self._get_metric_expression(dep_metric)
            if not dep_expression:
                raise CompilationError(
                    f"派生指标 '{metric_id}' 依赖的指标 '{dep_metric_id}' 缺少 expression"
                )

            # 替换公式中的指标ID为其表达式
            if dep_metric_id in formula_sql:
                # 用括号包围表达式避免运算符优先级问题
                dep_expr = f"({dep_expression})"
                formula_sql = formula_sql.replace(dep_metric_id, dep_expr)
                logger.debug(f"替换派生指标依赖: {dep_metric_id} -> {dep_expr}")

        logger.debug(f"最终派生指标公式: {formula_sql}")

        # 解析替换后的 formula
        try:
            derived_expr = parse_one(formula_sql, dialect=self.dialect)
            alias_name = alias_override or metric_id
            return derived_expr.as_(alias_name)
        except Exception as e:
            logger.error(
                f"解析派生指标 formula 失败: {metric_id}",
                formula=formula_sql,
                error=str(e)
            )
            raise CompilationError(
                f"派生指标 '{metric_id}' 的 formula 解析失败: {str(e)}\n"
                f"Formula: {formula_sql}"
            )

    def _add_joins(self, query: exp.Select, joins: List[Join]) -> exp.Select:
        """添加 JOIN 子句"""
        for join in joins:
            to_source = self.model.sources[join.to_table]
            to_alias = self._get_table_alias(join.to_table)
            to_table_expr = table(
                to_source.table_name,
                db=to_source.schema_name,
                alias=to_alias
            )

            # 解析 JOIN 条件
            try:
                # 使用友好别名替换条件中的表ID
                on_sql = join.on
                try:
                    on_sql = on_sql.replace(join.from_table, self._get_table_alias(join.from_table))
                    on_sql = on_sql.replace(join.to_table, to_alias)
                except Exception:
                    pass
                join_condition = parse_one(on_sql, dialect=self.dialect)
            except Exception as e:
                logger.error(f"解析 JOIN 条件失败: {join.on}", error=str(e))
                raise CompilationError(f"JOIN 条件错误: {str(e)}")

            # 添加 JOIN
            query = query.join(
                to_table_expr,
                on=join_condition,
                join_type=join.type
            )

        return query

    def _build_where_clause(
        self,
        ir: IntermediateRepresentation,
        main_table: str,
        joins: List[Join] = None
    ) -> List[exp.Expression]:
        """构建 WHERE 条件"""
        conditions = []

        # 1. 过滤条件
        for filter_cond in ir.filters:
            cond_expr = self._build_filter_expression(filter_cond)
            if cond_expr:
                conditions.append(cond_expr)

        # 2. 时间条件
        if ir.time:
            # 始终添加时间过滤条件，确保数据准确性
            time_cond = self._build_time_condition(ir.time, main_table, ir.dimensions)
            if time_cond:
                conditions.append(time_cond)
                logger.debug(f"添加时间过滤条件: {time_cond.sql()}")

        #  3. JOIN策略条件（反向匹配）
        if ir.join_strategy in ["left_unmatched", "right_unmatched"] and joins:
            null_conditions = self._build_join_strategy_conditions(ir, joins)
            conditions.extend(null_conditions)

        return conditions

    def _build_join_strategy_conditions(
        self,
        ir: IntermediateRepresentation,
        joins: List[Join]
    ) -> List[exp.Expression]:
        """
        根据JOIN策略构建额外的过滤条件

        Args:
            ir: 中间表示
            joins: JOIN列表

        Returns:
            条件表达式列表
        """
        conditions = []

        for join in joins:
            if ir.join_strategy == "left_unmatched" and join.type == "LEFT":
                # 左表未匹配：右表的关键字段应该为NULL
                target_table = join.to_table
                source = self.model.sources.get(target_table)

                if source and source.primary_key:
                    pk_field = source.primary_key[0]
                    null_cond = exp.Is(
                        this=exp.column(pk_field, table=self._get_table_alias(target_table)),
                        expression=exp.Null()
                    )
                    conditions.append(null_cond)
                    logger.debug(
                        "添加反向匹配条件",
                        strategy=ir.join_strategy,
                        table=target_table,
                        field=pk_field,
                        condition=f"{target_table}.{pk_field} IS NULL"
                    )

            elif ir.join_strategy == "right_unmatched" and join.type == "RIGHT":
                # 右表未匹配：左表的关键字段应该为NULL
                source_table = join.from_table
                source = self.model.sources.get(source_table)

                if source and source.primary_key:
                    pk_field = source.primary_key[0]
                    null_cond = exp.Is(
                        this=exp.column(pk_field, table=self._get_table_alias(source_table)),
                        expression=exp.Null()
                    )
                    conditions.append(null_cond)
                    logger.debug(
                        "添加反向匹配条件",
                        strategy=ir.join_strategy,
                        table=source_table,
                        field=pk_field,
                        condition=f"{source_table}.{pk_field} IS NULL"
                    )

        return conditions

    def _build_filter_expression(self, filter_cond: FilterCondition) -> Optional[exp.Expression]:
        """构建单个过滤表达式（支持模糊匹配）"""

        # 0. 特殊操作符跳过同义词解析（例如子查询注入）
        if filter_cond.op in ["IN_SUBQUERY", "NOT IN_SUBQUERY"]:
            filter_value = filter_cond.value
        else:
            #  1. 同义词解析：将同义词替换为标准值
            resolved_value = self.synonym_resolver.resolve_filter_value(
                filter_cond.field,
                filter_cond.value
            )

            # 如果值被替换了，记录日志
            if resolved_value != filter_cond.value:
                logger.debug("过滤值同义词解析",
                           field=filter_cond.field,
                           original=filter_cond.value,
                           resolved=resolved_value)
            
            filter_value = resolved_value

        #  字段间比较检测：检查value是否是另一个字段的ID
        # 如果是，则构建字段间比较表达式（如 column1 > column2）
        value_field_expr: Optional[exp.Expression] = None
        is_field_comparison = False
        
        if isinstance(filter_value, str):
            value_str = str(filter_value).strip()

            # 检查是否是维度ID - 支持字段间比较
            if value_str in self.model.dimensions:
                value_dim = self.model.dimensions[value_str]
                value_field_expr = exp.column(
                    value_dim.column, 
                    table=self._get_table_alias(value_dim.table)
                )
                is_field_comparison = True
                logger.info(
                    "检测到字段间比较：左字段与维度字段比较",
                    left_field=filter_cond.field,
                    right_field=value_str,
                    right_column=value_dim.column
                )

            # 检查是否是指标ID - 支持字段间比较
            elif value_str in self.model.metrics:
                value_metric = self.model.metrics[value_str]
                value_field_expr = exp.column(
                    value_metric.column,
                    table=self._get_table_alias(value_metric.table)
                )
                is_field_comparison = True
                logger.info(
                    "检测到字段间比较：左字段与指标字段比较",
                    left_field=filter_cond.field,
                    right_field=value_str,
                    right_column=value_metric.column
                )

            # 检查是否是度量ID - 支持字段间比较
            elif value_str in self.model.measures:
                value_measure = self.model.measures[value_str]
                value_field_expr = exp.column(
                    value_measure.column,
                    table=self._get_table_alias(value_measure.table)
                )
                is_field_comparison = True
                logger.info(
                    "检测到字段间比较：左字段与度量字段比较",
                    left_field=filter_cond.field,
                    right_field=value_str,
                    right_column=value_measure.column
                )
            
            # 检查是否是统一字段表中的字段ID - 支持字段间比较
            elif value_str in self.model.fields:
                value_field_obj = self.model.fields[value_str]
                value_physical_col = (
                    getattr(value_field_obj, "physical_column_name", None)
                    or getattr(value_field_obj, "column", None)
                    or getattr(value_field_obj, "field_name", None)
                    or value_str
                )
                value_field_expr = exp.column(
                    value_physical_col,
                    table=self._get_table_alias(value_field_obj.datasource_id)
                )
                is_field_comparison = True
                logger.info(
                    "检测到字段间比较：左字段与统一字段表字段比较",
                    left_field=filter_cond.field,
                    right_field=value_str,
                    right_column=value_physical_col
                )

        # 将字段ID统一转换为字符串，避免上游传入UUID对象导致字符串操作失败
        field_id = str(filter_cond.field) if filter_cond.field is not None else None

        # 获取字段信息
        dim = None
        field_obj = None
        column_name_for_log = None
        if field_id in self.model.dimensions:
            dim = self.model.dimensions[field_id]
            column_name_for_log = dim.column
            field_expr = exp.column(dim.column, table=self._get_table_alias(dim.table))
        elif field_id in self.model.fields:
            #  新架构支持：从统一字段表获取
            field_obj = self.model.fields[field_id]
            # 统一获取物理列名：优先 physical_column_name，其次 column，最后 field_name/字段ID
            physical_col = (
                getattr(field_obj, "physical_column_name", None)
                or getattr(field_obj, "column", None)
                or getattr(field_obj, "field_name", None)
                or field_id
            )
            column_name_for_log = physical_col
            field_expr = exp.column(physical_col, table=self._get_table_alias(field_obj.datasource_id))
            logger.debug(f"从统一字段表构建WHERE条件: {field_id} -> {physical_col}")
        else:
            # 直接使用字段名（通常为物理列名）
            column_name_for_log = field_id
            field_expr = exp.column(field_id)

        # 构建条件
        # 根据是否为字段间比较选择右侧表达式
        def _get_value_expression() -> exp.Expression:
            """获取过滤条件右侧的表达式（字段表达式或字面量）"""
            if is_field_comparison and value_field_expr is not None:
                return value_field_expr
            else:
                return exp.Literal.string(str(filter_value))
        
        if filter_cond.op == "=":
            #  关键：检查维度是否配置为包含匹配
            match_mode = None
            if dim:
                match_mode = dim.match_mode
            elif field_obj and hasattr(field_obj, 'match_mode'):
                match_mode = field_obj.match_mode

            if match_mode == "contains" and not is_field_comparison:
                # 自动转换为 LIKE '%value%'（仅适用于字面量值）
                value = str(filter_value)  # 使用解析后的值
                column_name = column_name_for_log or filter_cond.field
                logger.debug(f"使用包含匹配: {column_name} LIKE '%{value}%'")
                return exp.Like(
                    this=field_expr,
                    expression=exp.Literal.string(f'%{value}%')
                )
            else:
                # 精确匹配（默认）或字段间比较
                return exp.EQ(this=field_expr, expression=_get_value_expression())
        elif filter_cond.op == "!=":
            return exp.NEQ(this=field_expr, expression=_get_value_expression())
        elif filter_cond.op == ">":
            return exp.GT(this=field_expr, expression=_get_value_expression())
        elif filter_cond.op == ">=":
            return exp.GTE(this=field_expr, expression=_get_value_expression())
        elif filter_cond.op == "<":
            return exp.LT(this=field_expr, expression=_get_value_expression())
        elif filter_cond.op == "<=":
            return exp.LTE(this=field_expr, expression=_get_value_expression())
        elif filter_cond.op == "IN":
            if isinstance(filter_value, list):
                values = [exp.Literal.string(str(v)) for v in filter_value]
                return exp.In(this=field_expr, expressions=values)
        elif filter_cond.op == "NOT IN":
            if isinstance(filter_value, list):
                values = [exp.Literal.string(str(v)) for v in filter_value]
                in_expr = exp.In(this=field_expr, expressions=values)
                return exp.Not(this=in_expr)
        elif filter_cond.op == "IN_SUBQUERY":
            # 子查询注入: value 是 SQL 字符串
            if not isinstance(filter_value, str):
                logger.error("IN_SUBQUERY 的值必须是 SQL 字符串")
                return None
            try:
                # 解析 SQL 字符串为 Expression
                subquery_expr = parse_one(filter_value)
                # 确保它是 Subquery (如果不是，尝试转换)
                # sqlglot 中，exp.In 需要右侧是 List[Expression] 或者 Subquery
                return exp.In(this=field_expr, expressions=[subquery_expr])
            except Exception as e:
                logger.error(f"解析子查询失败: {filter_value}", error=str(e))
                return None
        elif filter_cond.op == "NOT IN_SUBQUERY":
            if not isinstance(filter_value, str):
                logger.error("NOT IN_SUBQUERY 的值必须是 SQL 字符串")
                return None
            try:
                subquery_expr = parse_one(filter_value)
                in_expr = exp.In(this=field_expr, expressions=[subquery_expr])
                return exp.Not(this=in_expr)
            except Exception as e:
                logger.error(f"解析子查询失败: {filter_value}", error=str(e))
                return None
        elif filter_cond.op == "LIKE":
            # 如果用户没有显式提供通配符，则默认做包含匹配
            like_val = str(filter_value) if filter_value is not None else ""
            if "%" not in like_val and "_" not in like_val:
                like_val = f"%{like_val}%"
            return exp.Like(this=field_expr, expression=exp.Literal.string(like_val))
        elif filter_cond.op == "IS NULL":
            return exp.Is(this=field_expr, expression=exp.Null())
        elif filter_cond.op == "IS NOT NULL":
            # sqlglot 没有 IsNot 类，使用 Not 包装 Is 来表示 IS NOT NULL
            return exp.Not(this=exp.Is(this=field_expr, expression=exp.Null()))
        elif filter_cond.op == "ST_INTERSECTS":
            # 空间查询处理
            wkt = str(filter_value)
            
            # 1. 构建几何对象: geometry::STGeomFromText('wkt', 0)
            # 这里的 geometry::STGeomFromText 是 SQL Server 的 CLR 静态方法，必须严格保持大小写
            # 且 :: 语法在 sqlglot 中容易被误处理或 normalization 导致变大写
            # 解决方案：手动构建 SQL 字符串，作为 Raw Identifier 注入
            
            # 构建参数表达式
            wkt_arg = exp.Literal.string(wkt)
            srid_arg = exp.Literal.number(0)
            
            # 生成参数的 SQL（处理转义等）
            # 注意：ASTBuilder 初始化时传入了 dialect，这里使用该 dialect 生成 SQL
            wkt_sql = wkt_arg.sql(self.dialect)
            srid_sql = srid_arg.sql(self.dialect)
            
            # 手动拼接静态方法调用字符串
            raw_geom_sql = f"geometry::STGeomFromText({wkt_sql}, {srid_sql})"
            
            # 使用 unquoted Identifier 注入，避免 sqlglot 进行 normalization (uppercasing)
            geom_expr = exp.Identifier(this=raw_geom_sql, quoted=False)
            
            # 2. 构建 STIntersects 调用
            # 注意：T-SQL 中是 .STIntersects(...)，大小写敏感
            # 经测试，在 Dot 表达式中，Anonymous 函数名的大小写会被保留
            st_intersects = exp.Anonymous(
                this="STIntersects",
                expressions=[geom_expr]
            )
            
            # 3. field.STIntersects(...)
            # 使用 Dot 节点连接字段和方法调用
            # sqlglot 的 Dot 节点生成 sql 时默认会加点
            method_call = exp.Dot(
                this=field_expr,
                expression=st_intersects
            )
            
            # 4. 判断结果是否为 1
            return exp.EQ(this=method_call, expression=exp.Literal.number(1))

        return None

    def _build_time_condition(
        self,
        time_range: TimeRange,
        main_table: str,
        dimensions: List[str] = None
    ) -> Optional[exp.Expression]:
        """构建时间条件（智能判断使用哪个字段）

        支持两种场景：
        1. 年份在字段中：如"成交年份"字段（整数类型如 2020、2021）
        2. 年份在日期字段中：使用表配置的 time_field（日期类型）

        Args:
            time_range: 时间范围配置
            main_table: 主表ID
            dimensions: 当前查询的维度字段ID列表（用于识别年份字段）
        """
        source = self.model.sources.get(main_table)

        # 优先处理：当 time.unit == 'year' 且 dimensions 中有年份字段时
        # 使用年份字段进行整数过滤
        if time_range.type == "relative" and time_range.unit == "year" and dimensions:
            year_field_info = self._find_year_field_in_dimensions(dimensions, main_table)
            if year_field_info:
                field_column, field_table = year_field_info
                now = now_with_tz()
                # 计算起始年份：当前年份 - (last_n - 1)
                # 例如：近6年 = 2020-2025（包含2020和2025）
                start_year = now.year - time_range.last_n + 1
                end_year = now.year

                logger.debug(f"使用年份字段过滤: {field_column} >= {start_year} AND <= {end_year}")

                year_field_expr = exp.column(field_column, table=self._get_table_alias(field_table))

                # 生成 年份字段 >= start_year AND 年份字段 <= end_year
                return exp.And(
                    this=exp.GTE(
                        this=year_field_expr,
                        expression=exp.Literal.number(start_year)
                    ),
                    expression=exp.LTE(
                        this=year_field_expr,
                        expression=exp.Literal.number(end_year)
                    )
                )

        # 回退到日期类型的 time_field
        time_field_name = getattr(source, 'time_field', None) if source else None
        if not source or not time_field_name:
            logger.warning(f"表 {main_table} 没有配置时间字段，且未找到年份维度字段")
            return None

        time_field = exp.column(time_field_name, table=self._get_table_alias(main_table))

        if time_range.type == "relative":
            # 相对时间：最近 N 天/周/月等
            if not time_range.last_n or not time_range.unit:
                return None

            # 计算起始日期
            now = now_with_tz()
            if time_range.unit == "day":
                start_date = now - timedelta(days=time_range.last_n)
            elif time_range.unit == "week":
                start_date = now - timedelta(weeks=time_range.last_n)
            elif time_range.unit == "month":
                # 简化处理，按 30 天计算
                start_date = now - timedelta(days=time_range.last_n * 30)
            elif time_range.unit == "year":
                start_date = now - timedelta(days=time_range.last_n * 365)
            else:
                return None

            # >= start_date
            return exp.GTE(
                this=time_field,
                expression=exp.Literal.string(start_date.strftime("%Y-%m-%d"))
            )

        elif time_range.type == "absolute":
            # 绝对时间
            conditions = []

            if time_range.start_date:
                conditions.append(exp.GTE(
                    this=time_field,
                    expression=exp.Literal.string(str(time_range.start_date))
                ))

            if time_range.end_date:
                conditions.append(exp.LTE(
                    this=time_field,
                    expression=exp.Literal.string(str(time_range.end_date))
                ))

            if len(conditions) == 2:
                return exp.And(this=conditions[0], expression=conditions[1])
            elif len(conditions) == 1:
                return conditions[0]

        elif time_range.type == "rolling":
            # 滚动时间窗口（如本月至今、今年以来）
            # grain 决定周期粒度，offset 决定周期偏移（0=当前，-1=上一个）
            grain = time_range.grain or "month"
            offset = time_range.offset or 0
            
            now = now_with_tz()
            today = now.date()
            
            # 计算周期起始日期和截止日期
            if grain == "week":
                # 本周一（weekday()返回0-6，0是周一）
                week_start = today - timedelta(days=today.weekday())
                if offset != 0:
                    week_start = week_start + timedelta(weeks=offset)
                start_date = week_start
                # 截止日期：当前周期是今天，历史周期是同一天
                if offset == 0:
                    end_date = today
                else:
                    end_date = start_date + timedelta(days=today.weekday())
                    
            elif grain == "month":
                # 本月1日
                if offset == 0:
                    start_date = today.replace(day=1)
                    end_date = today
                else:
                    # 计算偏移后的月份
                    year = today.year
                    month = today.month + offset
                    while month <= 0:
                        month += 12
                        year -= 1
                    while month > 12:
                        month -= 12
                        year += 1
                    start_date = date(year, month, 1)
                    # 截止日期是该月的同一天（如果天数超出则取月末）
                    try:
                        end_date = date(year, month, today.day)
                    except ValueError:
                        # 如该月没有31号，取月末
                        last_day = calendar.monthrange(year, month)[1]
                        end_date = date(year, month, last_day)
                        
            elif grain == "quarter":
                # 本季度第一天
                quarter = (today.month - 1) // 3
                quarter_start_month = quarter * 3 + 1
                
                if offset == 0:
                    start_date = date(today.year, quarter_start_month, 1)
                    end_date = today
                else:
                    # 计算偏移后的季度
                    total_quarters = today.year * 4 + quarter + offset
                    year = total_quarters // 4
                    q = total_quarters % 4
                    quarter_start_month = q * 3 + 1
                    start_date = date(year, quarter_start_month, 1)
                    # 计算同一天在该季度的位置
                    days_into_quarter = (today - date(today.year, (quarter * 3 + 1), 1)).days
                    end_date = start_date + timedelta(days=days_into_quarter)
                    
            elif grain == "year":
                # 今年1月1日
                if offset == 0:
                    start_date = date(today.year, 1, 1)
                    end_date = today
                else:
                    year = today.year + offset
                    start_date = date(year, 1, 1)
                    # 截止日期是该年的同一天
                    try:
                        end_date = date(year, today.month, today.day)
                    except ValueError:
                        # 处理闰年2月29日的情况
                        end_date = date(year, today.month, 28)
            else:
                logger.warning(f"不支持的 rolling grain: {grain}")
                return None
            
            logger.debug(f"rolling 时间范围: {grain} offset={offset} -> {start_date} 到 {end_date}")
            
            # 构建 >= start_date AND <= end_date
            return exp.And(
                this=exp.GTE(
                    this=time_field,
                    expression=exp.Literal.string(start_date.strftime("%Y-%m-%d"))
                ),
                expression=exp.LTE(
                    this=time_field,
                    expression=exp.Literal.string(end_date.strftime("%Y-%m-%d"))
                )
            )

        return None

    def _find_year_field_in_dimensions(
        self,
        dimensions: List[str],
        main_table: str
    ) -> Optional[tuple]:
        """在 dimensions 中查找年份类型的字段

        识别年份字段的方式：
        1. 字段名称包含年份相关关键词（"年份"、"年度"等）
        2. 字段的 dimension_type 是 'temporal'
        3. 字段的数据类型是整数且名称包含"年"

        Args:
            dimensions: 维度字段ID列表
            main_table: 主表ID

        Returns:
            (物理列名, 表ID) 或 None
        """
        # 年份相关关键词
        year_keywords = {'年份', '年度', '成交年份', '数据年份', '统计年份', '报告年份'}
        # 更宽松的匹配：只包含"年"但要排除一些干扰词
        year_suffix_keywords = {'年'}
        exclude_keywords = {'年限', '年龄', '年级', '年代', '年月', '年月日'}

        for dim_id in dimensions:
            field = self.model.fields.get(dim_id)
            if not field:
                continue

            # 确保字段属于主表
            if field.datasource_id != main_table:
                continue

            display_name = field.display_name or ''
            field_name = field.field_name or ''

            # 检查是否是年份字段
            is_year_field = False

            # 1. 精确匹配年份关键词
            if display_name in year_keywords or field_name in year_keywords:
                is_year_field = True

            # 2. 包含"年份"或"年度"
            elif '年份' in display_name or '年度' in display_name:
                is_year_field = True

            # 3. 字段名以"年"结尾，但不是排除的词
            elif display_name.endswith('年') and not any(ex in display_name for ex in exclude_keywords):
                # 进一步检查数据类型（应该是整数或字符串表示的年份）
                if field.data_type and field.data_type.lower() in ('int', 'integer', 'smallint', 'bigint', 'numeric', 'decimal', 'nvarchar', 'varchar'):
                    is_year_field = True

            # 4. 检查 dimension_type 是否是 temporal
            dim_props = getattr(field, 'dimension_props', None)
            if dim_props and getattr(dim_props, 'dimension_type', None) == 'temporal':
                # 进一步确认是年份级别的时间维度
                if '年' in display_name:
                    is_year_field = True

            if is_year_field:
                # 获取物理列名
                physical_column = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
                logger.debug(f"找到年份字段: {display_name} -> {physical_column}")
                return (physical_column, field.datasource_id)

        return None

    def _is_spatial_type(self, data_type: str) -> bool:
        """检查是否为空间数据类型"""
        if not data_type:
            return False
        spatial_types = {'geometry', 'geography'}
        return data_type.lower() in spatial_types

    def _wrap_spatial_expression(self, expr: exp.Expression) -> exp.Expression:
        """将空间类型表达式转换为文本 (WKT)"""
        # T-SQL: column.STAsText()
        # 注意大小写敏感
        method_call = exp.Anonymous(
            this="STAsText",
            expressions=[]
        )
        # 使用 Dot 节点
        return exp.Dot(
            this=expr,
            expression=method_call
        )

    def _get_detail_columns(self, main_table: str, ir: IntermediateRepresentation) -> List[exp.Expression]:
        """
        获取明细查询的默认字段列表
        
        对于明细查询（如"有哪些"），选择表的关键业务字段
        如果有sort_by，确保排序字段也被包含
        """
        source = self.model.sources.get(main_table)
        if not source:
            raise CompilationError(f"未找到数据源: {main_table}")

        #: 从Fields配置获取明细字段
        # 优先使用is_primary的字段，然后按priority排序
        
        # 支持空间字段，但在后续处理中需要转换为 WKT
        # spatial_categories = {'geometry', 'spatial', 'geometry/spatial'}
        # spatial_types = {'geometry', 'geography', 'hierarchyid', 'xml'}
        
        # 这里不基于类别过滤，而是在后面基于类型做转换
        # 排除不支持的类型：hierarchyid, xml (暂不处理)
        unsupported_types = {'hierarchyid', 'xml'}

        # 先拿到该表所有“可用字段”（只要在语义层 is_active=true，就认为是可用字段）
        datasource_fields_all = [
            field for field in self.model.fields.values()
            if field.datasource_id == main_table
            and field.is_active
            and field.data_type.lower() not in unsupported_types  # 排除不支持的数据类型
        ]

        # 明细查询优先返回 show_in_detail=true 的字段
        datasource_fields = [
            field for field in datasource_fields_all
            if getattr(field, 'show_in_detail', True)
        ]

        if datasource_fields:
            # 按优先级排序：is_primary > priority高 > 普通字段
            sorted_fields = sorted(
                datasource_fields,
                key=lambda f: (not f.is_primary, -f.priority)
            )
            field_names = [field.column for field in sorted_fields]
        elif datasource_fields_all:
            # 有字段配置，但都被设置为 show_in_detail=false
            # 这时绝不能回退到“全部物理列”，否则会把未启用/被禁用的列也一并返回
            sorted_fields = sorted(
                datasource_fields_all,
                key=lambda f: (not f.is_primary, -f.priority)
            )
            field_names = [field.column for field in sorted_fields]
            logger.warning(
                f"表 {main_table} 未配置任何 show_in_detail=true 的字段，已回退到该表所有 is_active=true 的字段（避免泄露禁用字段）"
            )
        else:
            # 如果没有任何 Fields 配置（语义层没有字段），回退到物理列
            if not source.columns:
                raise CompilationError(f"表 {main_table} 没有定义任何列或字段")
            field_names = [
                col.column_name for col in source.columns
                if hasattr(col, 'column_name') and col.data_type.lower() not in unsupported_types
            ]
            logger.warning(f"表 {main_table} 没有Fields配置，使用全部支持的物理列")

        # 添加排序字段（如果有sort_by且字段不在默认列表中）
        if ir.sort_by:
            sort_column = None

            # 从measures中查找
            if ir.sort_by in self.model.measures:
                measure = self.model.measures[ir.sort_by]
                if measure.table == main_table:
                    sort_column = measure.column
            # 从dimensions中查找
            elif ir.sort_by in self.model.dimensions:
                dim = self.model.dimensions[ir.sort_by]
                if dim.table == main_table:
                    sort_column = dim.column

            # 如果找到了排序字段且不在默认列表中，添加到开头
            if sort_column and sort_column not in field_names:
                field_names.insert(0, sort_column)
                logger.debug(f"添加排序字段到SELECT: {sort_column}")

        # 基于用户提问智能补充“被问到的字段”（如：楼面地价、每亩单价等）
        try:
            question_text = (ir.original_question or "")
            if question_text:
                # 遍历同表的度量，若提问命中其显示名或同义词，则将该度量的物理列加入返回列
                for measure in self.model.measures.values():
                    if getattr(measure, "table", None) != main_table:
                        continue
                    candidate_names = [
                        getattr(measure, "display_name", ""),
                        getattr(measure, "column", ""),
                    ]
                    # 同义词
                    try:
                        for syn in getattr(measure, "synonyms", []) or []:
                            candidate_names.append(syn)
                    except Exception:
                        pass
                    # 命中则加入（例如：问题中出现“楼面地价”“每亩单价”等）
                    if any(name and name in question_text for name in candidate_names):
                        col_name = measure.column
                        if col_name and col_name not in field_names:
                            field_names.append(col_name)
                            logger.debug(f"根据问题补充返回列(追加末尾): {col_name}")
        except Exception as e:
            logger.warning("基于问题补充返回列失败", error=str(e))

        # 构建 SELECT 表达式
        select_exprs = []
        available_columns = {col.column_name: col for col in source.columns}

        #  不支持类型列表
        unsupported_types = {'hierarchyid', 'xml'}

        # 构建物理列名到显示名的映射
        column_display_name_map = {}
        for field_id, field in self.model.fields.items():
            if field.datasource_id == main_table:
                # 获取物理列名
                physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
                if physical_col and field.display_name:
                    column_display_name_map[physical_col] = field.display_name

        for field_name in field_names:
            if field_name in available_columns:
                col = available_columns[field_name]
                # 跳过不支持的类型
                if col.data_type.lower() in unsupported_types:
                    logger.debug(f"跳过特殊字段: {field_name} (类型: {col.data_type})")
                    continue
                
                # 为字段添加别名（使用显示名称）
                col_expr = exp.column(field_name, table=self._get_table_alias(main_table))
                
                # 处理空间类型：转换为 WKT
                if self._is_spatial_type(col.data_type):
                    col_expr = self._wrap_spatial_expression(col_expr)
                    logger.debug(f"空间字段转换为WKT: {field_name}")

                if field_name in column_display_name_map:
                    display_name = column_display_name_map[field_name]
                    col_expr = col_expr.as_(display_name)
                    logger.debug(f"明细查询字段添加别名: {field_name} AS {display_name}")
                
                select_exprs.append(col_expr)
            else:
                logger.warning(f"字段 {field_name} 在表 {main_table} 中不存在，跳过")

        if not select_exprs:
            # 如果一个字段都没有，至少返回主键
            for col in source.columns:
                if col.column_name in source.primary_key:
                    select_exprs.append(exp.column(col.column_name, table=self._get_table_alias(main_table)))
                    break

        logger.debug(f"明细查询返回 {len(select_exprs)} 个字段")
        return select_exprs

    def _add_total_row(
        self,
        detail_query: exp.Select,
        ir: IntermediateRepresentation,
        main_table: str,
        joins: List[Join],
        where_conditions: List[exp.Expression]
    ) -> exp.Select:
        """
        生成带汇总行的查询。

        Args:
            detail_query: 原始的分组查询
            ir: 中间表示
            main_table: 主表ID
            joins: JOIN路径
            where_conditions: WHERE条件列表

        Returns:
            使用 UNION ALL 合并的查询（跨数据库兼容）
        """
        detail_part = detail_query.copy()
        if detail_part.args.get("order"):
            detail_part.set("order", None)
        if detail_part.args.get("limit"):
            detail_part.set("limit", None)

        total_query = detail_part.copy()
        total_select_exprs: List[exp.Expression] = []
        dimension_aliases = {self._get_dimension_alias(dim_id) for dim_id in ir.dimensions}

        for expr in total_query.args.get("expressions", []):
            expr_alias = expr.alias if hasattr(expr, "alias") and expr.alias else None
            if expr_alias in dimension_aliases:
                total_select_exprs.append(exp.Literal.string("合计").as_(expr_alias))
            else:
                total_select_exprs.append(expr)

        total_query.set("expressions", total_select_exprs)
        total_query.set("group", None)
        total_query.set("order", None)
        total_query.set("limit", None)

        union_result = exp.Union(
            this=detail_part,
            expression=total_query,
            distinct=False,
        )

        result_alias = "summary_result"
        result_query = select("*").from_(
            exp.Subquery(
                this=union_result,
                alias=exp.TableAlias(this=exp.to_identifier(result_alias)),
            )
        )

        if ir.dimensions:
            first_alias = self._get_dimension_alias(ir.dimensions[0])
            order_exprs: List[exp.Expression] = [
                exp.Ordered(
                    this=exp.Case(
                        ifs=[
                            exp.If(
                                this=exp.EQ(
                                    this=exp.column(first_alias, table=result_alias),
                                    expression=exp.Literal.string("合计"),
                                ),
                                true=exp.Literal.number(1),
                            )
                        ],
                        default=exp.Literal.number(0),
                    ),
                    desc=False,
                )
            ]
            for dim_id in ir.dimensions:
                order_exprs.append(
                    exp.Ordered(
                        this=exp.column(self._get_dimension_alias(dim_id), table=result_alias),
                        desc=False,
                    )
                )
            result_query = result_query.order_by(*order_exprs)

        logger.debug("已生成带汇总行的 UNION ALL 查询")
        return result_query

    def _build_union_query(
        self,
        ir: IntermediateRepresentation,
        joins: List[Join]
    ) -> exp.Expression:
        """
        构建 UNION ALL 查询（用于跨年/跨分区查询）
        
        为每个选中的表生成子查询，然后用 UNION ALL 合并。
        自动添加分区标识列（如"数据年份"）。
        
        Args:
            ir: 中间表示
            joins: Join 路径（跨分区查询通常不使用JOIN，此参数保留兼容性）
        
        Returns:
            SQLGlot Expression（Union）
        """
        if not ir.selected_table_ids or len(ir.selected_table_ids) < 2:
            raise CompilationError("跨分区查询需要至少选择2个表")
        
        logger.debug(
            "构建 UNION ALL 查询",
            table_count=len(ir.selected_table_ids),
            mode=ir.cross_partition_mode
        )
        
        # 获取分区标识字段名
        partition_label = ir.partition_label_field or "数据年份"
        
        # 收集所有表的公共字段（用于 UNION）
        common_columns = self._get_common_columns_for_union(ir.selected_table_ids)
        if not common_columns:
            logger.warning("未找到公共字段，将使用 IR 中指定的字段")
        
        union_queries = []
        
        for table_id in ir.selected_table_ids:
            # 获取表的数据源信息
            source = self.model.sources.get(table_id)
            if not source:
                # 尝试从 IR 的 selected_table_info 中获取（跨域表场景）
                if ir.selected_table_info and table_id in ir.selected_table_info:
                    table_info = ir.selected_table_info[table_id]
                    source = SimpleNamespace(
                        table_id=table_id,
                        table_name=table_info["table_name"],
                        schema_name=table_info.get("schema_name"),
                        display_name=table_info.get("display_name")
                    )
                    logger.debug(f"跨分区查询：从 IR.selected_table_info 获取表信息 {table_id}")
                else:
                    logger.warning(f"跨分区查询：未找到表 {table_id}，跳过")
                    continue
            
            # 获取表的分区值（如年份）
            partition_value = self._get_table_partition_value(table_id, source)
            
            # 构建该表的子查询
            sub_query = self._build_union_subquery(
                ir=ir,
                table_id=table_id,
                source=source,
                partition_label=partition_label,
                partition_value=partition_value,
                common_columns=common_columns
            )
            
            if sub_query:
                union_queries.append(sub_query)
        
        if not union_queries:
            raise CompilationError("跨分区查询：所有表都无法生成子查询")
        
        # 使用 UNION ALL 合并所有子查询
        union_result = union_queries[0]
        for sub_query in union_queries[1:]:
            union_result = exp.Union(
                this=union_result,
                expression=sub_query,
                distinct=False  # UNION ALL
            )
        
        # 将 UNION 查询包装在子查询中，使别名在外层 ORDER BY 中可用
        # 这是为了解决 SQL Server 中 ORDER BY 的 CASE WHEN 表达式无法引用 SELECT 别名的问题
        # SQLGlot 在生成 T-SQL 时会将 NULLS LAST 转换为 CASE WHEN [alias] IS NULL THEN 1 ELSE 0 END
        result = exp.select(exp.Star()).from_(
            exp.Subquery(
                this=union_result,
                alias=exp.TableAlias(this=exp.to_identifier("union_result"))
            )
        )
        
        # 添加外层的 ORDER BY 和 LIMIT
        if ir.dimensions or ir.order_by:
            order_exprs = []
            # 优先按分区标识排序
            order_exprs.append(exp.Ordered(
                this=exp.Column(this=exp.to_identifier(partition_label)),
                desc=True
            ))
            # 然后按维度排序（使用显示名/别名，因为 UNION 子查询输出的是别名）
            for dim_id in ir.dimensions[:2]:  # 限制排序字段数量
                dim_alias = self._get_dimension_alias(dim_id)
                if dim_alias:
                    order_exprs.append(exp.Ordered(
                        this=exp.Column(this=exp.to_identifier(dim_alias)),
                        desc=False
                    ))
            result = result.order_by(*order_exprs)
        
        if ir.limit:
            result = result.limit(ir.limit)
        
        logger.debug(
            "UNION ALL 查询构建完成",
            subquery_count=len(union_queries)
        )
        
        return result

    def _build_union_subquery(
        self,
        ir: IntermediateRepresentation,
        table_id: str,
        source,
        partition_label: str,
        partition_value: str,
        common_columns: Optional[List[str]] = None
    ) -> Optional[exp.Select]:
        """
        构建 UNION 的单个子查询
        
        Args:
            ir: 中间表示
            table_id: 表ID
            source: 数据源对象
            partition_label: 分区标识字段名
            partition_value: 分区值（如"2024"）
            common_columns: 公共列列表
        
        Returns:
            SQLGlot Select 表达式
        """
        table_alias = self._get_table_alias(table_id)
        
        # 构建 SELECT 子句
        select_exprs = []
        
        # 1. 添加分区标识列（作为第一列）
        select_exprs.append(exp.Alias(
            this=exp.Literal.string(partition_value),
            alias=exp.to_identifier(partition_label)
        ))
        
        # 2. 添加维度列
        cross_mappings = getattr(ir, 'cross_table_field_mappings', None) or {}
        for dim_id in ir.dimensions:
            col_info = self._resolve_field_column_for_table(dim_id, table_id, cross_mappings)
            if col_info:
                col_name, display_name = col_info
                select_exprs.append(exp.Alias(
                    this=exp.column(col_name, table=table_alias),
                    alias=exp.to_identifier(display_name)
                ))
        
        # 3. 添加度量列（带聚合）
        for metric_item in ir.metrics:
            # 规范化 metric：提取字段ID和聚合类型
            metric_id, metric_agg, metric_alias, metric_decimal = self._normalize_metric_spec(metric_item)
            
            if metric_id == "__row_count__":
                # 优先使用 LLM 指定的别名，否则使用默认的"记录数"
                row_count_alias = metric_alias if metric_alias else "记录数"
                select_exprs.append(exp.Alias(
                    this=exp.Count(this=exp.Star()),
                    alias=exp.to_identifier(row_count_alias)
                ))
            elif isinstance(metric_id, str) and metric_id.startswith("derived:"):
                # 处理派生指标
                derived_name = metric_id.replace("derived:", "")
                is_derived, _, derived_def = self._is_derived_metric(metric_id)
                if is_derived and derived_def:
                    formula = derived_def.get('formula', '')
                    # 特殊处理 COUNT(*) 类型的派生指标
                    if 'COUNT(*)' in formula.upper():
                        select_exprs.append(exp.Alias(
                            this=exp.Count(this=exp.Star()),
                            alias=exp.to_identifier(derived_name)
                        ))
                    else:
                        # 对于复杂公式，需要构建表达式
                        formula_expr, alias = self._build_derived_metric_formula_for_table(
                            derived_def, derived_name, table_id, table_alias, cross_mappings
                        )
                        if formula_expr:
                            select_exprs.append(exp.Alias(
                                this=exp.maybe_parse(formula_expr, into=exp.Expression),
                                alias=exp.to_identifier(alias)
                            ))
                        else:
                            logger.warning(f"跨分区查询：无法解析派生指标 {derived_name}")
                else:
                    logger.warning(f"跨分区查询：未找到派生指标定义 {derived_name}")
            elif metric_id in self.model.measures:
                measure = self.model.measures[metric_id]
                col_name = self._get_physical_column_name(metric_id)
                select_exprs.append(exp.Alias(
                    this=exp.Sum(this=exp.column(col_name, table=table_alias)),
                    alias=exp.to_identifier(measure.display_name)
                ))
        
        # 构建 FROM 子句
        from_table = table(
            source.table_name,
            db=source.schema_name,
            alias=table_alias
        )
        
        # 构建子查询
        sub_query = select(*select_exprs).from_(from_table)
        
        # 添加 WHERE 子句
        where_conditions = []
        for filter_cond in ir.filters:
            cond_expr = self._build_filter_condition_for_table(filter_cond, table_id, table_alias, cross_mappings)
            if cond_expr:
                where_conditions.append(cond_expr)
        
        if where_conditions:
            sub_query = sub_query.where(exp.and_(*where_conditions))
        
        # 添加 GROUP BY 子句（如果是聚合查询）
        if ir.query_type == "aggregation" and ir.dimensions:
            group_by_exprs = []
            for dim_id in ir.dimensions:
                col_info = self._resolve_field_column_for_table(dim_id, table_id, cross_mappings)
                if col_info:
                    col_name, _ = col_info
                    group_by_exprs.append(exp.column(col_name, table=table_alias))
            if group_by_exprs:
                sub_query = sub_query.group_by(*group_by_exprs)
        
        return sub_query

    def _build_derived_metric_formula_for_table(
        self,
        definition: dict,
        metric_name: str,
        table_id: str,
        table_alias: str,
        cross_table_mappings: Optional[Dict[str, Dict[str, str]]] = None
    ) -> tuple:
        """
        为特定表构建派生指标的SQL表达式（用于UNION子查询）
        
        Args:
            definition: 派生指标定义
            metric_name: 派生指标名称
            table_id: 表ID
            table_alias: 表别名
            cross_table_mappings: 跨表字段映射
        
        Returns:
            (expression_str, alias) 元组，如果无法构建返回 (None, metric_name)
        """
        formula = definition.get('formula', '')
        field_deps = definition.get('field_dependencies', [])
        
        # 无字段依赖的公式直接返回
        if not field_deps:
            return (formula, metric_name)
        
        # 构建替换映射
        result_formula = formula
        for dep in field_deps:
            field_id = dep.get('field_id')
            field_name = dep.get('field_name', '')
            aggregation = dep.get('aggregation', 'SUM')
            
            # 解析该表中对应的物理列
            col_info = self._resolve_field_column_for_table(field_id, table_id, cross_table_mappings)
            if not col_info:
                # 尝试通过字段名查找
                for fid, field in self.model.fields.items():
                    if field.datasource_id == table_id and field.display_name == field_name:
                        col_info = (field.column, field.display_name)
                        break
            
            if col_info:
                col_name, _ = col_info
                qualified = f"[{table_alias}].[{col_name}]"
                agg_expr = f"{aggregation}({qualified})"
                
                # 替换公式中的聚合表达式
                if aggregation:
                    pattern = f"{aggregation}({field_name})"
                    result_formula = result_formula.replace(pattern, agg_expr)
                    # 也尝试替换带括号的格式
                    pattern2 = f"{aggregation}([{field_name}])"
                    result_formula = result_formula.replace(pattern2, agg_expr)
            else:
                logger.warning(f"跨分区查询：无法解析派生指标字段 {field_name} in table {table_id}")
                return (None, metric_name)
        
        return (result_formula, metric_name)

    def _build_compare_query(
        self,
        ir: IntermediateRepresentation,
        joins: List[Join]
    ) -> exp.Expression:
        """
        构建跨分区对比查询（用于年份分区表的跨年对比分析）
        
        使用 CTE（公共表表达式）策略：先在各分区表内聚合，再 JOIN 聚合结果。
        这样可以避免笛卡尔积问题，确保聚合结果正确。
        
        Args:
            ir: 中间表示
            joins: Join 路径（通常不使用，保留兼容性）
        
        Returns:
            SQLGlot Expression
        
        Example output:
            WITH cte_current AS (
                SELECT 行政区, 地类名称, SUM(面积) as total_面积
                FROM table_2024
                WHERE 行政区 = '东湖区' AND 地类名称 = '耕地'
                GROUP BY 行政区, 地类名称
            ),
            cte_base AS (
                SELECT 行政区, 地类名称, SUM(面积) as total_面积
                FROM table_2023
                WHERE 行政区 = '东湖区' AND 地类名称 = '耕地'
                GROUP BY 行政区, 地类名称
            )
            SELECT 
                COALESCE(c.行政区, b.行政区) as 行政区,
                COALESCE(c.地类名称, b.地类名称) as 地类名称,
                c.total_面积 as "2024年面积",
                b.total_面积 as "2023年面积",
                (c.total_面积 - b.total_面积) as "变化量",
                CASE WHEN b.total_面积 != 0 
                     THEN (c.total_面积 - b.total_面积) / b.total_面积 * 100 
                     ELSE NULL END as "变化率(%)"
            FROM cte_current c
            FULL OUTER JOIN cte_base b 
              ON c.行政区 = b.行政区 AND c.地类名称 = b.地类名称
        """
        if not ir.selected_table_ids or len(ir.selected_table_ids) < 2:
            raise CompilationError("跨分区对比查询需要至少选择2个表")
        
        # 获取跨表字段映射（用于精确查找其他表的对应字段）
        cross_mappings = getattr(ir, 'cross_table_field_mappings', None) or {}
        
        logger.debug(
            "构建跨分区对比查询（Compare模式 - CTE策略）",
            table_count=len(ir.selected_table_ids),
            compare_join_keys=ir.compare_join_keys,
            cross_table_mappings=cross_mappings
        )
        
        # 获取当期表和基期表（默认第一个是当期，第二个是基期）
        current_table_id = ir.compare_base_table_id or ir.selected_table_ids[0]
        base_table_id = ir.selected_table_ids[1] if ir.selected_table_ids[0] == current_table_id else ir.selected_table_ids[0]
        
        current_source = self.model.sources.get(current_table_id)
        base_source = self.model.sources.get(base_table_id)
        
        if not current_source or not base_source:
            raise CompilationError(f"跨分区对比查询：无法找到表定义 current={current_table_id}, base={base_table_id}")
        
        # 获取年份标签
        current_year = self._get_table_partition_value(current_table_id, current_source)
        base_year = self._get_table_partition_value(base_table_id, base_source)
        
        # CTE 和主查询的别名
        current_cte_name = "cte_current"
        base_cte_name = "cte_base"
        current_alias = "c"
        base_alias = "b"
        
        # 解析对比关联字段
        # 
        # 职责说明：
        # - compare_join_keys 由 NL2IR 阶段 LLM 决定
        # - 空列表 [] 表示用户只需要简单汇总对比（不按维度拆分）
        # - 非空列表表示用户需要按维度拆分的明细对比
        # 
        # 注意：当 compare_join_keys 明确为空列表时，不应该再从 dimensions/filters 推断
        # 这是 NL2IR 阶段的明确意图，表示用户只想看整体变化率
        
        explicit_join_keys = getattr(ir, 'compare_join_keys', None)
        
        if explicit_join_keys is not None and len(explicit_join_keys) == 0:
            # NL2IR 明确设置为空列表 → 简单汇总对比，不需要关联字段
            join_key_columns = []
            logger.debug(
                "跨分区对比查询：使用简单汇总模式（无关联字段），将返回整体对比结果"
            )
        else:
            # NL2IR 设置了关联字段 或 未设置（None）需要推断
            join_key_columns = self._resolve_compare_join_keys(
                explicit_join_keys or [],
                current_table_id,
                base_table_id,
                cross_mappings
            )
            
            if not join_key_columns:
                # 如果没有指定关联字段，尝试从IR dimensions获取
                join_key_columns = self._infer_join_keys_from_dimensions(
                    ir.dimensions,
                    current_table_id,
                    base_table_id
                )
            
            # 从filters中推断额外的JOIN字段（防止笛卡尔积）
            filter_join_keys = self._infer_join_keys_from_filters(
                ir.filters,
                current_table_id,
                base_table_id,
                join_key_columns
            )
            if filter_join_keys:
                join_key_columns = join_key_columns + filter_join_keys
                logger.debug(
                    f"从filters补充JOIN字段: {[k[2] for k in filter_join_keys]}"
                )
            
            if not join_key_columns:
                logger.warning("跨分区对比查询：未找到关联字段，将使用笛卡尔积（可能产生大量数据）")
        
        # 解析度量字段
        metric_columns = []  # [(current_col, base_col, metric_name, agg_alias, is_row_count), ...]
        for metric_item in ir.metrics:
            # 规范化 metric：提取字段ID和别名
            metric_id, _, metric_alias, _ = self._normalize_metric_spec(metric_item)
            
            if metric_id == "__row_count__":
                # 优先使用 LLM 指定的别名，否则使用默认的"记录数"
                row_count_display = metric_alias if metric_alias else "记录数"
                metric_columns.append((None, None, row_count_display, "total_record_count", True))
            else:
                current_col, base_col, metric_name = self._resolve_metric_for_compare(
                    metric_id, current_table_id, base_table_id, cross_mappings
                )
                # 派生指标（如 COUNT(*)）返回 (None, None, "记录数")，需要特殊处理
                if metric_name == "记录数":
                    # 使用用户指定的别名或默认值
                    row_count_display = metric_alias if metric_alias else "记录数"
                    # 检查是否已经添加过记录数（避免重复）
                    if not any(len(m) > 4 and m[4] for m in metric_columns):
                        metric_columns.append((None, None, row_count_display, "total_record_count", True))
                elif current_col and base_col:
                    # 两边都解析成功，正常添加
                    agg_alias = f"total_{metric_name}".replace(" ", "_")
                    metric_columns.append((current_col, base_col, metric_name, agg_alias, False))
                elif current_col or base_col:
                    # 只有一边解析成功，仍然添加（可能只在一个表中有数据）
                    agg_alias = f"total_{metric_name}".replace(" ", "_")
                    metric_columns.append((current_col, base_col, metric_name, agg_alias, False))
                    logger.warning(
                        f"跨分区对比度量字段只在一侧解析成功: {metric_name}",
                        current_col=current_col,
                        base_col=base_col
                    )
        
        # ========== 构建当期表 CTE ==========
        # 注意：对于 __row_count__，col 为 None，但仍需传入 CTE 以生成 COUNT(*)
        current_cte_query = self._build_compare_cte_subquery(
            table_id=current_table_id,
            source=current_source,
            join_key_columns=[(col_current, display_name) for col_current, _, display_name in join_key_columns],
            metric_columns=[(col, name, alias) for col, _, name, alias, is_row_count in metric_columns if col is not None or is_row_count],
            filters=ir.filters,
            cross_mappings=cross_mappings
        )
        
        # ========== 构建基期表 CTE ==========
        # 注意：对于 __row_count__，col 为 None，但仍需传入 CTE 以生成 COUNT(*)
        base_cte_query = self._build_compare_cte_subquery(
            table_id=base_table_id,
            source=base_source,
            join_key_columns=[(col_base, display_name) for _, col_base, display_name in join_key_columns],
            metric_columns=[(col, name, alias) for _, col, name, alias, is_row_count in metric_columns if col is not None or is_row_count],
            filters=ir.filters,
            cross_mappings=cross_mappings
        )
        
        # ========== 构建主查询 ==========
        main_select_exprs = []
        
        # 1. 添加关联字段（使用COALESCE确保NULL值处理）
        # CTE中的分组列使用显示名作为别名
        for _, _, display_name in join_key_columns:
            main_select_exprs.append(exp.Alias(
                this=exp.Coalesce(
                    this=exp.column(display_name, table=current_alias),
                    expressions=[exp.column(display_name, table=base_alias)]
                ),
                alias=exp.to_identifier(display_name)
            ))
        
        # 2. 添加度量列的对比（从CTE的聚合结果中读取）
        for _, _, metric_name, agg_alias, is_row_count in metric_columns:
            if is_row_count:
                # 记录数使用 COUNT(*)，使用用户指定的别名（如"宗数"、"记录数"等）
                main_select_exprs.append(exp.Alias(
                    this=exp.Coalesce(
                        this=exp.column(agg_alias, table=current_alias),
                        expressions=[exp.Literal.number(0)]
                    ),
                    alias=exp.to_identifier(f"{current_year}年{metric_name}")
                ))
                main_select_exprs.append(exp.Alias(
                    this=exp.Coalesce(
                        this=exp.column(agg_alias, table=base_alias),
                        expressions=[exp.Literal.number(0)]
                    ),
                    alias=exp.to_identifier(f"{base_year}年{metric_name}")
                ))
            else:
                # 当期值
                main_select_exprs.append(exp.Alias(
                    this=exp.column(agg_alias, table=current_alias),
                    alias=exp.to_identifier(f"{current_year}年{metric_name}")
                ))
                
                # 基期值
                main_select_exprs.append(exp.Alias(
                    this=exp.column(agg_alias, table=base_alias),
                    alias=exp.to_identifier(f"{base_year}年{metric_name}")
                ))
                
                # 变化量 = 当期 - 基期（使用COALESCE处理NULL）
                diff_expr = exp.Sub(
                    this=exp.Coalesce(
                        this=exp.column(agg_alias, table=current_alias),
                        expressions=[exp.Literal.number(0)]
                    ),
                    expression=exp.Coalesce(
                        this=exp.column(agg_alias, table=base_alias),
                        expressions=[exp.Literal.number(0)]
                    )
                )
                main_select_exprs.append(exp.Alias(
                    this=diff_expr,
                    alias=exp.to_identifier(f"{metric_name}变化量")
                ))
                
                # 变化率(%) = (当期 - 基期) / 基期 * 100
                # 使用 CASE WHEN 处理除零和NULL
                rate_expr = exp.Case(
                    ifs=[exp.If(
                        this=exp.And(
                            this=exp.column(agg_alias, table=base_alias).is_(exp.Null()).not_(),
                            expression=exp.NEQ(
                                this=exp.column(agg_alias, table=base_alias),
                                expression=exp.Literal.number(0)
                            )
                        ),
                        true=exp.Mul(
                            this=exp.Div(
                                this=exp.Sub(
                                    this=exp.Coalesce(
                                        this=exp.column(agg_alias, table=current_alias),
                                        expressions=[exp.Literal.number(0)]
                                    ),
                                    expression=exp.column(agg_alias, table=base_alias)
                                ),
                                expression=exp.column(agg_alias, table=base_alias)
                            ),
                            expression=exp.Literal.number(100)
                        )
                    )],
                    default=exp.Null()
                )
                main_select_exprs.append(exp.Alias(
                    this=rate_expr,
                    alias=exp.to_identifier(f"{metric_name}变化率(%)")
                ))
        
        current_cte_table = exp.Table(
            this=exp.to_identifier(current_cte_name),
            alias=exp.to_identifier(current_alias),
        )
        base_cte_table = exp.Table(
            this=exp.to_identifier(base_cte_name),
            alias=exp.to_identifier(base_alias),
        )

        # 构建 JOIN 条件（基于CTE中的分组列）
        join_condition = None
        for _, _, display_name in join_key_columns:
            cond = exp.EQ(
                this=exp.column(display_name, table=current_alias),
                expression=exp.column(display_name, table=base_alias)
            )
            if join_condition is None:
                join_condition = cond
            else:
                join_condition = exp.And(this=join_condition, expression=cond)

        def _build_main_select_query() -> exp.Select:
            return select(*[expr.copy() for expr in main_select_exprs])

        if join_condition:
            if self.profile.supports_full_outer_join:
                main_relation = _build_main_select_query().from_(current_cte_table.copy()).join(
                    base_cte_table.copy(),
                    on=join_condition.copy(),
                    join_type="FULL OUTER",
                )
            else:
                left_query = _build_main_select_query().from_(current_cte_table.copy()).join(
                    base_cte_table.copy(),
                    on=join_condition.copy(),
                    join_type="LEFT",
                )

                right_only_query = _build_main_select_query().from_(base_cte_table.copy()).join(
                    current_cte_table.copy(),
                    on=join_condition.copy(),
                    join_type="LEFT",
                )

                unmatched_condition = None
                for _, _, display_name in join_key_columns:
                    is_null_cond = exp.column(display_name, table=current_alias).is_(exp.Null())
                    if unmatched_condition is None:
                        unmatched_condition = is_null_cond
                    else:
                        unmatched_condition = exp.And(
                            this=unmatched_condition,
                            expression=is_null_cond,
                        )

                if unmatched_condition is not None:
                    right_only_query = right_only_query.where(unmatched_condition)

                main_relation = exp.Union(
                    this=left_query,
                    expression=right_only_query,
                    distinct=False,
                )
                logger.debug(
                    "当前方言不支持 FULL OUTER JOIN，已改写为 LEFT JOIN + UNION ALL",
                    db_type=self.db_type,
                )
        else:
            main_relation = _build_main_select_query().from_(current_cte_table.copy()).join(
                base_cte_table.copy(),
                join_type="CROSS",
            )

        result_alias = "compare_result"
        main_query = select("*").from_(
            exp.Subquery(
                this=main_relation,
                alias=exp.TableAlias(this=exp.to_identifier(result_alias)),
            )
        )

        # 添加 ORDER BY
        if join_key_columns:
            order_exprs = []
            for _, _, display_name in join_key_columns:
                order_exprs.append(exp.Ordered(
                    this=exp.column(display_name, table=result_alias),
                    desc=False
                ))
            main_query = main_query.order_by(*order_exprs)

        # 添加 LIMIT
        if ir.limit:
            main_query = main_query.limit(ir.limit)

        # ========== 组装 WITH 子句 ==========
        # 使用 SQLGlot 的 with_ 方法正确构建 CTE
        main_query = main_query.with_(current_cte_name, as_=current_cte_query)
        main_query = main_query.with_(base_cte_name, as_=base_cte_query, append=True)
        
        logger.debug(
            "跨分区对比查询构建完成（CTE策略）",
            current_year=current_year,
            base_year=base_year,
            join_keys=[k[2] for k in join_key_columns] if join_key_columns else []
        )
        
        return main_query
    
    def _build_compare_cte_subquery(
        self,
        table_id: str,
        source: Any,
        join_key_columns: List[Tuple[str, str]],
        metric_columns: List[Tuple[str, str, str]],
        filters: List[FilterCondition],
        cross_mappings: Optional[Dict[str, Dict[str, str]]] = None
    ) -> exp.Expression:
        """
        构建跨分区对比查询的 CTE 子查询
        
        在单个分区表内进行聚合，避免笛卡尔积问题。
        
        Args:
            table_id: 表ID
            source: 数据源定义
            join_key_columns: [(physical_col, display_name), ...] 分组/关联字段
            metric_columns: [(physical_col, metric_name, agg_alias), ...] 度量字段
            filters: 过滤条件列表
            cross_mappings: 跨表字段映射
        
        Returns:
            SQLGlot Select 表达式
        """
        t_alias = "t"
        
        # 构建 SELECT 子句
        select_exprs = []
        
        # 1. 添加分组字段（使用显示名作为别名，便于主查询引用）
        for physical_col, display_name in join_key_columns:
            select_exprs.append(exp.Alias(
                this=exp.column(physical_col, table=t_alias),
                alias=exp.to_identifier(display_name)
            ))
        
        # 2. 添加聚合度量
        for physical_col, metric_name, agg_alias in metric_columns:
            if physical_col is None:
                # 无物理列 → 记录数统计（COUNT(*)），使用用户指定的别名
                select_exprs.append(exp.Alias(
                    this=exp.Count(this=exp.Star()),
                    alias=exp.to_identifier(agg_alias)
                ))
            else:
                select_exprs.append(exp.Alias(
                    this=exp.Sum(this=exp.column(physical_col, table=t_alias)),
                    alias=exp.to_identifier(agg_alias)
                ))
        
        # 空SELECT保护：如果没有任何字段，抛出明确错误而非生成无效SQL
        if not select_exprs:
            raise CompilationError(
                f"跨分区对比查询CTE构建失败：表 '{source.table_name}' 没有可用的SELECT字段。"
                f"请检查 join_key_columns={join_key_columns} 和 metric_columns={metric_columns} 是否正确解析。"
            )
        
        # 构建 FROM 子句
        from_table = table(
            source.table_name,
            db=source.schema_name,
            alias=t_alias
        )
        
        # 构建查询
        cte_query = select(*select_exprs).from_(from_table)
        
        # 添加 WHERE 子句
        where_conditions = []
        for filter_cond in filters:
            cond = self._build_filter_condition_for_table(filter_cond, table_id, t_alias, cross_mappings)
            if cond:
                where_conditions.append(cond)
        
        if where_conditions:
            combined_where = where_conditions[0]
            for cond in where_conditions[1:]:
                combined_where = exp.And(this=combined_where, expression=cond)
            cte_query = cte_query.where(combined_where)
        
        # 添加 GROUP BY
        if join_key_columns:
            group_exprs = [exp.column(physical_col, table=t_alias) for physical_col, _ in join_key_columns]
            cte_query = cte_query.group_by(*group_exprs)
        
        return cte_query

    def _build_multi_join_query(
        self,
        ir: IntermediateRepresentation,
        joins: List[Join]
    ) -> exp.Expression:
        """
        构建多表关联查询（用于找出同时存在于多个表中的记录）
        
        生成 INNER JOIN 连接多个表，使用指定的关联字段进行匹配。
        
        Args:
            ir: 中间表示
            joins: Join 路径（通常不使用，保留兼容性）
        
        Returns:
            SQLGlot Expression
        
        Example output:
            SELECT 
                t1.地块编号, t1.行政区, t1.用地面积,
                '新增建设用地批复' as 来源表1,
                '建设用地批准书' as 来源表2,
                '公开成交' as 来源表3
            FROM 新增建设用地批复 t1
            INNER JOIN 建设用地批准书 t2 ON t1.关联字段 = t2.关联字段
            INNER JOIN 公开成交 t3 ON t1.关联字段 = t3.关联字段
        """
        if not ir.selected_table_ids or len(ir.selected_table_ids) < 2:
            raise CompilationError("多表关联查询需要至少选择2个表")
        
        logger.debug(
            "构建多表关联查询（multi_join 模式）",
            table_count=len(ir.selected_table_ids),
            compare_join_keys=ir.compare_join_keys
        )
        
        # 获取所有表的数据源信息
        # 优先从模型中获取，如果不在模型中则从 IR 的 selected_table_info 中获取（跨域场景）
        table_sources = []
        for table_id in ir.selected_table_ids:
            source = self.model.sources.get(table_id)
            if source:
                table_sources.append((table_id, source))
            elif ir.selected_table_info and table_id in ir.selected_table_info:
                # 使用 IR 携带的表物理信息（跨域场景）
                table_info = ir.selected_table_info[table_id]
                # 创建一个简化的 source 对象
                from types import SimpleNamespace
                fallback_source = SimpleNamespace(
                    table_name=table_info.get("table_name", ""),
                    schema_name=table_info.get("schema_name"),
                    display_name=table_info.get("display_name", ""),
                    table=table_info.get("table_name", "")  # 兼容性字段
                )
                table_sources.append((table_id, fallback_source))
                logger.debug(f"多表关联查询：使用 IR 携带的表信息: {table_id} -> {fallback_source.table_name}")
            else:
                logger.warning(f"多表关联查询：未找到表 {table_id}，跳过")
        
        if len(table_sources) < 2:
            raise CompilationError(
                f"多表关联查询：有效表数量不足 ({len(table_sources)}/{len(ir.selected_table_ids)})。"
                "请确保表结构信息正确传递。"
            )
        
        # 主表（第一个表）
        main_table_id, main_source = table_sources[0]
        main_alias = "t1"
        
        # 解析关联字段
        # 优先使用 IR 中预先构建的字段映射（跨域场景）
        join_key_mappings = []
        
        if ir.multi_join_field_mappings:
            # 使用 parser 预先构建的字段映射
            join_key_mappings = ir.multi_join_field_mappings
            logger.debug(
                "使用 IR 预构建的字段映射",
                mapping_count=len(join_key_mappings)
            )
        else:
            # 尝试使用 compare_join_keys 从模型中解析
            join_key_mappings = self._resolve_multi_join_keys(
                ir.compare_join_keys or [],
                [ts[0] for ts in table_sources]
            )
            
            if not join_key_mappings:
                # 如果没有指定，尝试从公共字段中推断
                join_key_mappings = self._infer_multi_join_keys([ts[0] for ts in table_sources])
        
        if not join_key_mappings:
            logger.warning("多表关联查询：未找到关联字段，无法生成有效查询")
            raise CompilationError("多表关联查询需要指定关联字段（如行政区、地块编号等）")
        
        # 获取主表的 JOIN 键列名列表（用于去重）
        main_join_cols = []
        for join_key in join_key_mappings:
            main_col = join_key.get(main_table_id)
            if main_col:
                main_join_cols.append(main_col)
        
        # 检查主表是否需要提供 JOIN 键以外的字段（维度、度量、过滤条件）
        # 如果需要，则不能使用简单的子查询去重，必须使用完整表
        main_table_has_other_fields = False
        
        # 检查维度字段
        for dim_id in (ir.dimensions or []):
            field = self.model.fields.get(dim_id)
            if field and field.datasource_id == main_table_id:
                main_table_has_other_fields = True
                break
        
        # 检查度量字段
        if not main_table_has_other_fields:
            for metric_item in (ir.metrics or []):
                if isinstance(metric_item, dict):
                    metric_id = metric_item.get("field", "")
                else:
                    metric_id = str(metric_item)
                if metric_id and metric_id != "__row_count__":
                    field = self.model.fields.get(metric_id)
                    if field and field.datasource_id == main_table_id:
                        main_table_has_other_fields = True
                        break
        
        # 检查过滤条件字段
        if not main_table_has_other_fields:
            for f in (ir.filters or []):
                field_id = f.field if hasattr(f, 'field') else f.get('field', '')
                field = self.model.fields.get(field_id)
                if field and field.datasource_id == main_table_id:
                    main_table_has_other_fields = True
                    break
        
        # 构建 FROM 子句（主表）
        # 根据 IR 配置决定是否对主表 JOIN 键进行去重（避免笛卡尔积）
        deduplicate_join_keys = getattr(ir, 'deduplicate_join_keys', True)
        
        # 只有当主表不需要提供其他字段时，才能使用子查询去重
        can_use_subquery_dedup = main_join_cols and deduplicate_join_keys and not main_table_has_other_fields
        
        if can_use_subquery_dedup:
            # 构建子查询：SELECT DISTINCT join_keys FROM main_table
            distinct_cols = [exp.column(col) for col in main_join_cols]
            main_subquery = exp.Select(
                expressions=distinct_cols
            ).from_(
                exp.Table(
                    this=exp.to_identifier(main_source.table_name),
                    db=exp.to_identifier(main_source.schema_name) if main_source.schema_name else None
                )
            ).distinct()  # 使用 .distinct() 方法
            from_expr = exp.Subquery(
                this=main_subquery,
                alias=exp.to_identifier(main_alias)
            )
            logger.debug(
                f"multi_join 主表去重: SELECT DISTINCT {main_join_cols} FROM {main_source.table_name}"
            )
        else:
            # 不去重或主表需要其他字段：直接使用原始表
            from_expr = exp.Table(
                this=exp.to_identifier(main_source.table_name),
                db=exp.to_identifier(main_source.schema_name) if main_source.schema_name else None,
                alias=exp.to_identifier(main_alias)
            )
            if main_join_cols and deduplicate_join_keys and main_table_has_other_fields:
                logger.debug(
                    f"multi_join 跳过去重: 主表需要提供其他字段（维度/度量/过滤条件）"
                )
            elif main_join_cols and not deduplicate_join_keys:
                logger.debug(
                    f"multi_join 保留完整关联（deduplicate_join_keys=False）"
                )
        
        # 根据 join_strategy 决定 JOIN 类型
        join_strategy = getattr(ir, 'join_strategy', 'matched') or 'matched'
        if join_strategy == "left_unmatched":
            join_kind = "LEFT"
        elif join_strategy == "right_unmatched":
            join_kind = "RIGHT"
        else:
            join_kind = "INNER"
        
        logger.debug(
            f"multi_join JOIN策略: {join_strategy} -> {join_kind} JOIN"
        )
        
        # 构建 JOIN 子句
        join_exprs = []
        # 记录用于反连接 IS NULL 检查的列（用于 unmatched 策略）
        anti_join_null_checks = []
        
        for idx, (table_id, source) in enumerate(table_sources[1:], start=2):
            alias = f"t{idx}"
            
            # 构建 ON 条件
            on_conditions = []
            for join_key in join_key_mappings:
                # join_key 是一个字典，包含各表的列名映射
                main_col = join_key.get(main_table_id)
                other_col = join_key.get(table_id)
                
                if main_col and other_col:
                    on_conditions.append(exp.EQ(
                        this=exp.column(main_col, table=main_alias),
                        expression=exp.column(other_col, table=alias)
                    ))
                    # 对于 left_unmatched，记录右表的列用于 IS NULL 检查
                    if join_strategy == "left_unmatched":
                        anti_join_null_checks.append((alias, other_col))
                    # 对于 right_unmatched，记录左表的列用于 IS NULL 检查
                    elif join_strategy == "right_unmatched":
                        anti_join_null_checks.append((main_alias, main_col))
            
            if not on_conditions:
                logger.warning(f"表 {table_id} 没有可用的关联字段，跳过")
                continue
            
            # 合并所有 ON 条件
            on_expr = on_conditions[0]
            for cond in on_conditions[1:]:
                on_expr = exp.And(this=on_expr, expression=cond)
            
            join_exprs.append(exp.Join(
                this=exp.Table(
                    this=exp.to_identifier(source.table_name),
                    db=exp.to_identifier(source.schema_name) if source.schema_name else None,
                    alias=exp.to_identifier(alias)
                ),
                kind=join_kind,
                on=on_expr
            ))
        
        # 构建表ID到别名的映射
        table_alias_map = {main_table_id: main_alias}
        for idx, (table_id, _) in enumerate(table_sources[1:], start=2):
            table_alias_map[table_id] = f"t{idx}"
        
        # 辅助函数：计算两个字符串的相似度（用于容错LLM幻觉的字段ID）
        def string_similarity(s1: str, s2: str) -> float:
            """计算两个字符串的相似度（0-1之间）"""
            if s1 == s2:
                return 1.0
            if not s1 or not s2:
                return 0.0
            # 简单的字符匹配相似度
            matches = sum(1 for a, b in zip(s1, s2) if a == b)
            return matches / max(len(s1), len(s2))
        
        # 辅助函数：根据字段ID找到字段信息和所属表别名
        def get_field_info(field_id, fallback_display_name=None):
            # 先从 model.fields 精确查找
            field = self.model.fields.get(field_id)
            if field:
                alias = table_alias_map.get(field.datasource_id, main_alias)
                return field, alias
            
            # 如果精确匹配失败，尝试模糊匹配（容错LLM生成的错误字段ID）
            # 查找相似度最高的字段ID
            best_match = None
            best_similarity = 0.0
            SIMILARITY_THRESHOLD = 0.95  # 相似度阈值，防止错误匹配
            
            for fid, fobj in self.model.fields.items():
                # 只在参与关联的表中查找
                if fobj.datasource_id not in table_alias_map:
                    continue
                    
                similarity = string_similarity(field_id, fid)
                if similarity > best_similarity and similarity >= SIMILARITY_THRESHOLD:
                    best_similarity = similarity
                    best_match = fobj
            
            if best_match:
                alias = table_alias_map.get(best_match.datasource_id, main_alias)
                logger.warning(
                    f"multi_join 字段ID模糊匹配: '{field_id}' -> '{best_match.field_id}' "
                    f"(相似度: {best_similarity:.2%}, 字段: {best_match.display_name})"
                )
                return best_match, alias
            
            # 如果提供了显示名，尝试通过显示名查找
            if fallback_display_name:
                for fid, fobj in self.model.fields.items():
                    if fobj.datasource_id not in table_alias_map:
                        continue
                    if fobj.display_name == fallback_display_name:
                        alias = table_alias_map.get(fobj.datasource_id, main_alias)
                        logger.warning(
                            f"multi_join 通过显示名查找字段: '{fallback_display_name}' -> {fobj.field_id}"
                        )
                        return fobj, alias
            
            return None, None
        
        # 构建 SELECT 子句
        select_exprs = []
        group_by_cols = []  # 用于 GROUP BY
        is_aggregation = ir.query_type == "aggregation"
        
        # 添加维度字段
        if ir.dimensions:
            for dim_id in ir.dimensions:
                field, alias = get_field_info(dim_id)
                if field:
                    col_expr = exp.column(field.column, table=alias)
                    select_exprs.append(exp.Alias(
                        this=col_expr,
                        alias=exp.to_identifier(field.display_name)
                    ))
                    if is_aggregation:
                        group_by_cols.append(col_expr)
                else:
                    logger.warning(f"multi_join 未找到维度字段: {dim_id}")
        
        # 添加度量字段
        if ir.metrics:
            for metric_item in ir.metrics:
                # 规范化 metric：提取字段ID和聚合类型
                metric_id, metric_agg, metric_alias_override, metric_decimal = self._normalize_metric_spec(metric_item)
                
                if metric_id == "__row_count__":
                    # 优先使用 LLM 指定的别名，否则使用默认的"记录数"
                    row_count_alias = metric_alias_override if metric_alias_override else "记录数"
                    select_exprs.append(exp.Alias(
                        this=exp.Count(this=exp.Star()),
                        alias=exp.to_identifier(row_count_alias)
                    ))
                else:
                    # 使用 alias 作为 fallback 显示名（从alias中提取可能的字段名）
                    fallback_name = None
                    if metric_alias_override:
                        # 从 alias 中提取字段名（去掉单位后缀如"(公顷)"）
                        import re
                        match = re.match(r'^([^(（]+)', metric_alias_override)
                        if match:
                            fallback_name = match.group(1).strip()
                    
                    field, alias = get_field_info(metric_id, fallback_display_name=fallback_name)
                    if field:
                        col_expr = exp.column(field.column, table=alias)
                        if is_aggregation:
                            # 聚合查询：使用指定的聚合函数
                            agg_expr = self._build_aggregation_expr(col_expr, metric_agg, metric_decimal)
                            display_name = metric_alias_override or field.display_name
                            select_exprs.append(exp.Alias(
                                this=agg_expr,
                                alias=exp.to_identifier(display_name)
                            ))
                        else:
                            select_exprs.append(exp.Alias(
                                this=col_expr,
                                alias=exp.to_identifier(field.display_name)
                            ))
                    else:
                        logger.warning(f"multi_join 未找到度量字段: {metric_id}, alias={metric_alias_override}")
        
        # 如果没有指定任何字段，选择关联字段
        if not select_exprs:
            for join_key in join_key_mappings:
                main_col = join_key.get(main_table_id)
                if main_col:
                    display_name = join_key.get("display_name", main_col)
                    select_exprs.append(exp.Alias(
                        this=exp.column(main_col, table=main_alias),
                        alias=exp.to_identifier(display_name)
                    ))
            # 添加各表的来源标识（仅在无字段时）
            for idx, (table_id, source) in enumerate(table_sources, start=1):
                select_exprs.append(exp.Alias(
                    this=exp.Literal.string(source.display_name or source.table_name),
                    alias=exp.to_identifier(f"来源表{idx}")
                ))
        
        # 构建最终查询
        query = exp.Select(expressions=select_exprs).from_(from_expr)
        
        # 添加 JOIN
        for join_expr in join_exprs:
            query = query.join(join_expr)
        
        # 添加 WHERE 过滤条件
        where_conditions = []
        
        # 对于 unmatched 策略，添加反连接条件（IS NULL）
        if anti_join_null_checks:
            # 只需要检查第一个关联列是否为 NULL（任意一个即可判断未匹配）
            null_alias, null_col = anti_join_null_checks[0]
            null_check = exp.Is(
                this=exp.column(null_col, table=null_alias),
                expression=exp.Null()
            )
            where_conditions.append(null_check)
            logger.debug(
                f"multi_join 反连接条件: {null_alias}.{null_col} IS NULL"
            )
        
        if ir.filters:
            for f in ir.filters:
                field, alias = get_field_info(f.field)
                if field:
                    col_expr = exp.column(field.column, table=alias)
                    if f.op == "=":
                        where_conditions.append(exp.EQ(this=col_expr, expression=exp.Literal.string(f.value)))
                    elif f.op == "IN":
                        values = [exp.Literal.string(v) for v in f.value] if isinstance(f.value, list) else [exp.Literal.string(f.value)]
                        where_conditions.append(exp.In(this=col_expr, expressions=values))
                    elif f.op == "LIKE":
                        where_conditions.append(exp.Like(this=col_expr, expression=exp.Literal.string(f.value)))
                    elif f.op == ">=":
                        where_conditions.append(exp.GTE(this=col_expr, expression=exp.Literal.number(f.value) if isinstance(f.value, (int, float)) else exp.Literal.string(f.value)))
                    elif f.op == "<=":
                        where_conditions.append(exp.LTE(this=col_expr, expression=exp.Literal.number(f.value) if isinstance(f.value, (int, float)) else exp.Literal.string(f.value)))
                    elif f.op == ">":
                        where_conditions.append(exp.GT(this=col_expr, expression=exp.Literal.number(f.value) if isinstance(f.value, (int, float)) else exp.Literal.string(f.value)))
                    elif f.op == "<":
                        where_conditions.append(exp.LT(this=col_expr, expression=exp.Literal.number(f.value) if isinstance(f.value, (int, float)) else exp.Literal.string(f.value)))
                    else:
                        logger.warning(f"multi_join 不支持的过滤操作符: {f.op}")
                else:
                    logger.warning(f"multi_join 未找到过滤字段: {f.field}")
        
        if where_conditions:
            where_expr = where_conditions[0]
            for cond in where_conditions[1:]:
                where_expr = exp.And(this=where_expr, expression=cond)
            query = query.where(where_expr)
        
        # 添加 GROUP BY（聚合查询）
        if is_aggregation and group_by_cols:
            query = query.group_by(*group_by_cols)
        
        # 添加 LIMIT
        if ir.limit:
            query = query.limit(ir.limit)
        
        logger.debug(
            "多表关联查询构建完成",
            table_count=len(table_sources),
            join_keys=[jk.get("display_name", "unknown") for jk in join_key_mappings]
        )
        
        return query
    
    def _resolve_multi_join_keys(
        self,
        compare_join_keys: List[str],
        table_ids: List[str]
    ) -> List[Dict[str, str]]:
        """
        解析多表关联的字段映射
        
        Args:
            compare_join_keys: 关联字段显示名列表（如 ["行政区", "地块编号"]）
            table_ids: 参与关联的表ID列表
        
        Returns:
            关联字段映射列表，每个元素是 {table_id: column_name, ..., "display_name": display_name}
        """
        result = []
        
        for key_name in compare_join_keys:
            mapping = {"display_name": key_name}
            
            for table_id in table_ids:
                # 在该表中查找匹配的字段
                for field in self.model.fields.values():
                    if field.datasource_id == table_id:
                        if field.display_name == key_name or key_name in (field.synonyms or []):
                            mapping[table_id] = field.column
                            break
            
            # 只有当所有表都有这个字段时才添加
            if len(mapping) > len(table_ids):  # display_name + 所有表
                result.append(mapping)
            elif len(mapping) > 1:  # 至少有一个表有这个字段
                logger.warning(f"关联字段 '{key_name}' 只在部分表中找到: {list(mapping.keys())}")
        
        return result
    
    def _infer_multi_join_keys(self, table_ids: List[str]) -> List[Dict[str, str]]:
        """
        自动推断多表关联的字段
        
        查找在所有表中都存在的公共字段，优先使用：
        1. 名称包含"编号"、"代码"的标识类字段
        2. 名称包含"行政区"的维度字段
        
        Args:
            table_ids: 参与关联的表ID列表
        
        Returns:
            关联字段映射列表
        """
        # 收集每个表的字段
        table_fields = {}
        for table_id in table_ids:
            fields = {}
            for field in self.model.fields.values():
                if field.datasource_id == table_id:
                    # 使用显示名作为匹配键
                    fields[field.display_name] = field.column
            table_fields[table_id] = fields
        
        # 查找公共字段
        if not table_fields:
            return []
        
        common_names = set(table_fields[table_ids[0]].keys())
        for table_id in table_ids[1:]:
            common_names &= set(table_fields[table_id].keys())
        
        # 优先选择标识类字段
        priority_keywords = ["编号", "代码", "ID", "id", "行政区", "区域"]
        result = []
        
        for name in common_names:
            for keyword in priority_keywords:
                if keyword in name:
                    mapping = {"display_name": name}
                    for table_id in table_ids:
                        mapping[table_id] = table_fields[table_id][name]
                    result.append(mapping)
                    break
        
        return result

    def _resolve_compare_join_keys(
        self,
        join_key_ids: List[str],
        current_table_id: str,
        base_table_id: str,
        cross_table_mappings: Optional[Dict[str, Dict[str, str]]] = None
    ) -> List[Tuple[str, str, str]]:
        """
        解析对比关联字段（从字段ID到物理列名）
        
        优先使用 cross_table_mappings 精确映射，如不存在则使用模糊匹配（后备方案）。
        
        Args:
            join_key_ids: 关联字段ID列表（或显示名，用于向后兼容）
            current_table_id: 当期表ID
            base_table_id: 基期表ID
            cross_table_mappings: 跨表字段映射 {主表字段UUID: {其他表ID: 对应字段UUID}}
        
        Returns:
            [(current_col, base_col, display_name), ...]
        """
        result = []
        cross_mappings = cross_table_mappings or {}
        
        def field_matches_name(field, name: str) -> bool:
            """检查字段是否匹配给定名称（用于向后兼容显示名匹配）"""
            if field.display_name == name:
                return True
            if hasattr(field, 'synonyms') and field.synonyms:
                if name in field.synonyms:
                    return True
            if name in field.display_name:
                return True
            return False
        
        for key_id in join_key_ids:
            current_col = None
            current_display_name = None
            base_col = None
            base_display_name = None
            
            # 方式1：通过字段ID直接查找
            field = self.model.fields.get(key_id)
            if field:
                # 确定该字段属于哪个表（使用兼容函数）
                if self._field_belongs_to_table(field, current_table_id):
                    current_col = field.column
                    current_display_name = field.display_name
                    
                    # 使用 cross_table_mappings 查找 base 表的对应字段
                    if key_id in cross_mappings and base_table_id in cross_mappings[key_id]:
                        mapped_field_id = cross_mappings[key_id][base_table_id]
                        mapped_field = self.model.fields.get(mapped_field_id)
                        if mapped_field and self._field_belongs_to_table(mapped_field, base_table_id):
                            base_col = mapped_field.column
                            base_display_name = mapped_field.display_name
                            logger.debug(
                                "使用cross_table_mappings解析JOIN字段",
                                key_id=key_id,
                                current_col=current_col,
                                base_col=base_col,
                                mapped_field_id=mapped_field_id
                            )
                
                elif self._field_belongs_to_table(field, base_table_id):
                    base_col = field.column
                    base_display_name = field.display_name
                    
                    # 反向：使用 cross_table_mappings 查找 current 表
                    for primary_id, mappings in cross_mappings.items():
                        if mappings.get(base_table_id) == key_id:
                            primary_field = self.model.fields.get(primary_id)
                            if primary_field and self._field_belongs_to_table(primary_field, current_table_id):
                                current_col = primary_field.column
                                current_display_name = primary_field.display_name
                                break
            
            # 方式2：后备 - 如果cross_mappings没有完全解析，使用模糊匹配
            if not (current_col and base_col) and field:
                display_name = field.display_name
                synonyms = getattr(field, 'synonyms', []) or []
                
                match_keywords = [display_name] + synonyms
                paren_match = re.search(r'[（(]([^）)]+)[）)]', display_name)
                if paren_match:
                    inner = paren_match.group(1)
                    for kw in re.split(r'[/、]', inner):
                        kw = kw.strip()
                        if kw and kw not in match_keywords:
                            match_keywords.append(kw)
                
                def matches_field(f, keywords: list) -> bool:
                    for kw in keywords:
                        if f.display_name == kw:
                            return True
                        if kw in f.display_name:
                            return True
                        f_synonyms = getattr(f, 'synonyms', []) or []
                        if kw in f_synonyms:
                            return True
                    return False
                
                for fid, f in self.model.fields.items():
                    if not current_col and self._field_belongs_to_table(f, current_table_id) and matches_field(f, match_keywords):
                        current_col = f.column
                        current_display_name = f.display_name
                    if not base_col and self._field_belongs_to_table(f, base_table_id) and matches_field(f, match_keywords):
                        base_col = f.column
                        base_display_name = f.display_name
            
            # 方式3：向后兼容 - 如果字段ID本身不存在于模型中，尝试按显示名匹配
            if not (current_col and base_col):
                for field_id, fld in self.model.fields.items():
                    if not current_col and self._field_belongs_to_table(fld, current_table_id) and field_matches_name(fld, key_id):
                        current_col = fld.column
                        current_display_name = fld.display_name
                    if not base_col and self._field_belongs_to_table(fld, base_table_id) and field_matches_name(fld, key_id):
                        base_col = fld.column
                        base_display_name = fld.display_name
            
            if current_col and base_col:
                final_display_name = current_display_name or base_display_name or key_id
                result.append((current_col, base_col, final_display_name))
                logger.debug(
                    f"解析关联字段成功: '{key_id}' -> current={current_col}, base={base_col}, display={final_display_name}"
                )
            elif current_col and not base_col:
                # 只有当期表有该字段，尝试用物理列名在基期表查找
                for fid, f in self.model.fields.items():
                    if self._field_belongs_to_table(f, base_table_id):
                        # 尝试物理列名匹配（忽略大小写）
                        if f.column and current_col and f.column.upper() == current_col.upper():
                            base_col = f.column
                            base_display_name = f.display_name
                            logger.debug(
                                f"关联字段通过物理列名匹配成功: {key_id}",
                                current_col=current_col,
                                base_col=base_col
                            )
                            break
                
                if base_col:
                    final_display_name = current_display_name or base_display_name or key_id
                    result.append((current_col, base_col, final_display_name))
                else:
                    logger.warning(
                        f"跨分区对比：关联字段只在当期表存在，跳过",
                        key_id=key_id,
                        current_col=current_col,
                        current_display_name=current_display_name
                    )
            elif base_col and not current_col:
                # 只有基期表有该字段，尝试用物理列名在当期表查找
                for fid, f in self.model.fields.items():
                    if self._field_belongs_to_table(f, current_table_id):
                        if f.column and base_col and f.column.upper() == base_col.upper():
                            current_col = f.column
                            current_display_name = f.display_name
                            logger.debug(
                                f"关联字段通过物理列名匹配成功: {key_id}",
                                current_col=current_col,
                                base_col=base_col
                            )
                            break
                
                if current_col:
                    final_display_name = current_display_name or base_display_name or key_id
                    result.append((current_col, base_col, final_display_name))
                else:
                    logger.warning(
                        f"跨分区对比：关联字段只在基期表存在，跳过",
                        key_id=key_id,
                        base_col=base_col,
                        base_display_name=base_display_name
                    )
            else:
                logger.warning(
                    f"跨分区对比：无法解析关联字段 '{key_id}'，两表均未找到匹配字段"
                )
        
        # 记录最终解析结果统计
        if join_key_ids:
            success_count = len(result)
            total_count = len(join_key_ids)
            if success_count < total_count:
                logger.warning(
                    f"关联字段解析：成功 {success_count}/{total_count}，部分字段可能不在两表共有"
                )
        
        return result

    def _infer_join_keys_from_dimensions(
        self,
        dimension_ids: List[str],
        current_table_id: str,
        base_table_id: str
    ) -> List[Tuple[str, str, str]]:
        """
        从IR的dimensions推断关联字段
        
        Args:
            dimension_ids: IR中的维度ID列表
            current_table_id: 当期表ID
            base_table_id: 基期表ID
        
        Returns:
            [(current_col, base_col, display_name), ...]
        """
        result = []
        
        for dim_id in dimension_ids:
            dim = self.model.dimensions.get(dim_id)
            if not dim:
                continue
            
            display_name = dim.display_name
            
            # 在两个表中查找同名字段
            current_col = None
            base_col = None
            
            for field_id, field in self.model.fields.items():
                if field.datasource_id == current_table_id and field.display_name == display_name:
                    current_col = field.column
                if field.datasource_id == base_table_id and field.display_name == display_name:
                    base_col = field.column
            
            if current_col and base_col:
                result.append((current_col, base_col, display_name))
        
        return result

    def _infer_join_keys_from_filters(
        self,
        filters: List[FilterCondition],
        current_table_id: str,
        base_table_id: str,
        existing_join_keys: List[Tuple[str, str, str]]
    ) -> List[Tuple[str, str, str]]:
        """
        从IR的filters推断额外的关联字段（防止笛卡尔积）
        
        对于跨分区对比查询，筛选条件中的维度字段也应该加入JOIN条件，
        否则会导致笛卡尔积。例如：
        - 用户问"东湖区2023-2024年耕地变化"
        - filters中有"行政区=东湖区"和"地类=耕地"
        - 如果只按"地类"JOIN，会导致东湖区的耕地与所有区的耕地JOIN
        
        Args:
            filters: IR中的过滤条件列表
            current_table_id: 当期表ID
            base_table_id: 基期表ID
            existing_join_keys: 已有的JOIN字段（避免重复）
        
        Returns:
            新增的 [(current_col, base_col, display_name), ...]
        """
        result = []
        
        # 已有JOIN字段的display_name集合
        existing_display_names = {k[2] for k in existing_join_keys}
        
        for filter_cond in filters:
            field_id = filter_cond.field
            
            # 只处理等值过滤条件（=, IN），这些才适合加入JOIN条件
            if filter_cond.op not in ("=", "IN"):
                continue
            
            # 尝试通过字段ID查找维度
            dim = self.model.dimensions.get(field_id)
            if not dim:
                # 尝试通过显示名查找
                for d_id, d in self.model.dimensions.items():
                    if d.display_name == field_id:
                        dim = d
                        break
            
            if not dim:
                continue
            
            display_name = dim.display_name
            
            # 跳过已存在的JOIN字段
            if display_name in existing_display_names:
                continue
            
            # 在两个表中查找同名字段
            current_col = None
            base_col = None
            
            for field_id_iter, field in self.model.fields.items():
                if field.datasource_id == current_table_id and field.display_name == display_name:
                    current_col = field.column
                if field.datasource_id == base_table_id and field.display_name == display_name:
                    base_col = field.column
            
            if current_col and base_col:
                result.append((current_col, base_col, display_name))
                existing_display_names.add(display_name)  # 防止重复添加
                logger.debug(
                    f"从filters推断JOIN字段: {display_name}",
                    current_col=current_col,
                    base_col=base_col
                )
        
        return result

    def _resolve_metric_for_compare(
        self,
        metric_id: str,
        current_table_id: str,
        base_table_id: str,
        cross_mappings: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Tuple[Optional[str], Optional[str], str]:
        """
        解析度量字段用于对比查询
        
        Args:
            metric_id: 度量ID
            current_table_id: 当期表ID
            base_table_id: 基期表ID
            cross_mappings: 跨表字段映射 {主表字段ID: {其他表ID: 对应字段ID}}
        
        Returns:
            (current_col, base_col, metric_name)
            对于派生指标（如 derived:宗数），返回 (None, None, "记录数") 以触发 COUNT(*) 处理
        """
        cross_mappings = cross_mappings or {}
        
        if metric_id == "__row_count__":
            return (None, None, "记录数")
        
        # 处理派生指标（derived:xxx 格式）
        if metric_id.startswith("derived:"):
            metric_name = metric_id[8:]  # 移除 "derived:" 前缀
            # 检查派生指标定义
            is_derived, _, derived_def = self._is_derived_metric(metric_id)
            if is_derived:
                # 如果派生指标是 COUNT(*) 类型（如"宗数"），返回记录数标记
                formula = derived_def.get("formula", "") if derived_def else ""
                if "COUNT(*)" in formula.upper() or metric_name in ["宗数", "记录数", "数量", "条数"]:
                    return (None, None, "记录数")
                # 其他复杂派生指标暂不支持跨表对比，跳过
                logger.warning(
                    f"跨分区对比查询暂不支持复杂派生指标: {metric_id}，已跳过"
                )
                return (None, None, metric_name)
        
        measure = self.model.measures.get(metric_id)
        if not measure:
            # 可能是直接使用字段ID
            field = self.model.fields.get(metric_id)
            if field:
                # 这是一个字段，需要在两个表中找对应的列
                metric_name = field.display_name
                current_col = None
                base_col = None
                
                # 如果字段属于当前表，直接使用
                if self._field_belongs_to_table(field, current_table_id):
                    current_col = field.column
                    # 尝试从 cross_mappings 获取基期表的对应字段
                    if metric_id in cross_mappings and base_table_id in cross_mappings[metric_id]:
                        base_field_id = cross_mappings[metric_id][base_table_id]
                        base_field = self.model.fields.get(base_field_id)
                        if base_field:
                            base_col = base_field.column
                            logger.debug(
                                f"通过跨表映射找到基期度量字段: {metric_name} -> {base_field.display_name}",
                                current_col=current_col,
                                base_col=base_col
                            )
                        else:
                            # 字段映射存在但字段未加载，可能是 is_active=FALSE 或其他原因
                            # 尝试使用源字段的物理列名（假设两表结构相似）
                            logger.warning(
                                f"跨表映射的字段未在模型中找到: base_field_id={base_field_id}，"
                                f"尝试使用源字段物理列名: {field.column}",
                                metric_id=metric_id,
                                current_col=current_col,
                                base_table_id=base_table_id
                            )
                            # 使用源字段的物理列名作为 fallback
                            base_col = field.column
                elif self._field_belongs_to_table(field, base_table_id):
                    base_col = field.column
                    # 尝试从 cross_mappings 反向查找当期表的对应字段
                    found_mapping = False
                    for primary_field_id, table_mappings in cross_mappings.items():
                        if base_table_id in table_mappings and table_mappings[base_table_id] == metric_id:
                            current_field = self.model.fields.get(primary_field_id)
                            if current_field and self._field_belongs_to_table(current_field, current_table_id):
                                current_col = current_field.column
                                found_mapping = True
                                break
                            elif not current_field:
                                # 字段映射存在但字段未加载，使用 base 字段的物理列名作为 fallback
                                logger.warning(
                                    f"跨表映射的字段未在模型中找到: primary_field_id={primary_field_id}，"
                                    f"尝试使用基期字段物理列名: {field.column}",
                                    metric_id=metric_id,
                                    base_col=base_col,
                                    current_table_id=current_table_id
                                )
                                current_col = field.column
                                found_mapping = True
                                break
                    # 如果没有找到映射，使用 base 字段的物理列名
                    if not found_mapping and not current_col:
                        current_col = field.column
                        logger.debug(
                            f"未找到跨表映射，使用基期字段物理列名: {field.column}",
                            metric_id=metric_id
                        )
                
                # 如果已经通过 cross_mappings 找到了两边的列，直接返回
                if current_col and base_col:
                    return (current_col, base_col, metric_name)
                
                # 如果还没有找到，尝试通过显示名匹配
                for fid, f in self.model.fields.items():
                    if not current_col and self._field_belongs_to_table(f, current_table_id) and f.display_name == metric_name:
                        current_col = f.column
                    if not base_col and self._field_belongs_to_table(f, base_table_id) and f.display_name == metric_name:
                        base_col = f.column
                
                if current_col and base_col:
                    return (current_col, base_col, metric_name)
                
                # 如果还没找到，尝试通过物理列名匹配
                if current_col or base_col:
                    target_table_id = base_table_id if current_col else current_table_id
                    source_col = field.column  # 源字段的物理列名
                    for fid, f in self.model.fields.items():
                        if self._field_belongs_to_table(f, target_table_id):
                            # 物理列名匹配（忽略大小写）
                            if source_col and f.column and source_col.upper() == f.column.upper():
                                if not current_col:
                                    current_col = f.column
                                else:
                                    base_col = f.column
                                logger.debug(
                                    f"通过物理列名匹配找到度量字段: {metric_name} -> {f.display_name}",
                                    source_col=source_col,
                                    matched_col=f.column
                                )
                                break
                            # 使用更宽松的匹配：显示名相似或同义词匹配
                            if (f.display_name == metric_name or 
                                metric_name in (getattr(f, 'synonyms', []) or []) or
                                metric_name in f.display_name or
                                f.display_name in metric_name):
                                if not current_col:
                                    current_col = f.column
                                else:
                                    base_col = f.column
                                break
                    return (current_col, base_col, metric_name)
            
            return (None, None, metric_id)
        
        metric_name = measure.display_name
        
        # 在两个表中查找度量字段
        current_col = None
        base_col = None
        
        # 1. 先尝试通过 cross_mappings 查找（最可靠）
        if metric_id in cross_mappings:
            # metric_id 对应当期表的字段
            current_field = self.model.fields.get(metric_id)
            if current_field and self._field_belongs_to_table(current_field, current_table_id):
                current_col = current_field.column
                # 查找基期表的对应字段
                if base_table_id in cross_mappings[metric_id]:
                    base_field_id = cross_mappings[metric_id][base_table_id]
                    base_field = self.model.fields.get(base_field_id)
                    if base_field:
                        base_col = base_field.column
                        logger.debug(
                            f"通过跨表映射找到度量字段: {metric_name}",
                            current_col=current_col,
                            base_col=base_col,
                            base_field_display_name=base_field.display_name
                        )
                    else:
                        # 字段映射存在但字段未加载，使用源字段物理列名
                        base_col = current_field.column
                        logger.warning(
                            f"跨表映射的度量字段未在模型中找到，使用源字段物理列名: {current_field.column}",
                            metric_id=metric_id,
                            base_field_id=base_field_id
                        )
        
        # 2. 通过显示名匹配查找
        if not current_col or not base_col:
            for field_id, field in self.model.fields.items():
                if not current_col and self._field_belongs_to_table(field, current_table_id) and field.display_name == metric_name:
                    current_col = field.column
                if not base_col and self._field_belongs_to_table(field, base_table_id) and field.display_name == metric_name:
                    base_col = field.column
        
        # 3. 通过物理列名匹配查找
        if not current_col:
            current_col = self._get_physical_column_name_for_table(metric_id, current_table_id)
        if not base_col:
            base_col = self._get_physical_column_name_for_table(metric_id, base_table_id)
        
        # 4. 最后的 fallback：如果当期表字段存在且两表结构相似，使用相同的物理列名
        if current_col and not base_col:
            current_field = self.model.fields.get(metric_id)
            if current_field:
                # 检查基期表是否有相同物理列名的字段
                for fid, f in self.model.fields.items():
                    if self._field_belongs_to_table(f, base_table_id) and f.column == current_field.column:
                        base_col = f.column
                        logger.debug(
                            f"通过物理列名匹配找到基期度量字段: {metric_name} -> {f.display_name}",
                            current_col=current_col,
                            base_col=base_col
                        )
                        break
        
        return (current_col, base_col, metric_name)

    def _get_physical_column_name_for_table(
        self,
        field_id: str,
        table_id: str
    ) -> Optional[str]:
        """
        获取指定表中字段的物理列名
        
        Args:
            field_id: 字段ID
            table_id: 表ID
        
        Returns:
            物理列名
        """
        # 尝试直接获取
        field = self.model.fields.get(field_id)
        if field and field.datasource_id == table_id:
            return field.column
        
        # 如果不是同一个表的字段，尝试通过显示名匹配
        if field:
            for fid, f in self.model.fields.items():
                if f.datasource_id == table_id and f.display_name == field.display_name:
                    return f.column
        
        return None

    def _get_common_columns_for_union(self, table_ids: List[str]) -> List[str]:
        """
        获取多个表的公共列名（用于 UNION 兼容性检查）
        
        Args:
            table_ids: 表ID列表
        
        Returns:
            公共列名列表
        """
        if not table_ids:
            return []
        
        # 获取第一个表的所有列
        first_table_id = table_ids[0]
        first_columns = set()
        
        for field_id, field in self.model.fields.items():
            if field.datasource_id == first_table_id:
                first_columns.add(field.column)
        
        # 求所有表的交集
        common = first_columns
        for table_id in table_ids[1:]:
            table_columns = set()
            for field_id, field in self.model.fields.items():
                if field.datasource_id == table_id:
                    table_columns.add(field.column)
            common = common.intersection(table_columns)
        
        return list(common)

    def _get_table_partition_value(self, table_id: str, source) -> str:
        """
        获取表的分区值（如年份）
        
        尝试从以下来源获取：
        1. source.data_year 属性
        2. 从表名中提取年份
        3. 使用表名作为默认值
        
        Args:
            table_id: 表ID
            source: 数据源对象
        
        Returns:
            分区值字符串
        """
        # 尝试从 source 获取 data_year
        if hasattr(source, 'data_year') and source.data_year:
            return str(source.data_year)
        
        # 尝试从表名提取年份（如 "XXXX年业务数据"）
        import re
        table_name = source.table_name or ""
        year_match = re.search(r'(20\d{2})', table_name)
        if year_match:
            return year_match.group(1)
        
        # 默认使用表名
        return source.table_name or table_id

    def _resolve_field_column_for_table(
        self,
        field_id: str,
        table_id: str,
        cross_table_mappings: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Optional[tuple]:
        """
        解析字段在指定表中的列名
        
        对于跨年查询，同一个逻辑字段（如"大类名称"）在不同表中可能有不同的UUID，
        需要通过以下顺序查找：
        1. 字段直接属于目标表 -> 直接返回
        2. 使用 cross_table_mappings 精确映射 -> 通过UUID查找
        3. 模糊匹配（后备方案）
        
        Args:
            field_id: 字段ID（可能是主表的字段ID）
            table_id: 目标表ID
            cross_table_mappings: 跨表字段映射 {主表字段UUID: {其他表ID: 对应字段UUID}}
        
        Returns:
            (column_name, display_name) 或 None
        """
        # 1. 首先检查该字段是否直接属于目标表
        if field_id in self.model.fields:
            field = self.model.fields[field_id]
            if self._field_belongs_to_table(field, table_id):
                return (field.column, field.display_name)
            
            # 2. 使用 cross_table_mappings 精确映射（优先级最高）
            if cross_table_mappings and field_id in cross_table_mappings:
                table_mappings = cross_table_mappings[field_id]
                if table_id in table_mappings:
                    mapped_field_id = table_mappings[table_id]
                    mapped_field = self.model.fields.get(mapped_field_id)
                    if mapped_field and self._field_belongs_to_table(mapped_field, table_id):
                        logger.debug(
                            "跨表字段映射（精确UUID映射）",
                            original_field_id=field_id,
                            target_table_id=table_id,
                            mapped_field_id=mapped_field_id,
                            original_display_name=field.display_name,
                            mapped_display_name=mapped_field.display_name,
                            column=mapped_field.column
                        )
                        return (mapped_field.column, mapped_field.display_name)
                    elif not mapped_field:
                        # 字段映射存在但目标字段未加载到模型中（可能是 is_active=FALSE）
                        # 使用源字段的物理列名作为 fallback
                        logger.warning(
                            "跨表字段映射：目标字段未在模型中找到，使用源字段物理列名",
                            original_field_id=field_id,
                            target_table_id=table_id,
                            mapped_field_id=mapped_field_id,
                            source_column=field.column,
                            source_display_name=field.display_name
                        )
                        return (field.column, field.display_name)
            
            # 3. 后备：通过显示名模糊匹配（兼容旧逻辑）
            display_name = field.display_name
            synonyms = getattr(field, 'synonyms', []) or []
            
            # 构建匹配关键词列表
            match_keywords = [display_name] + synonyms
            # 如果显示名包含括号，提取括号内的关键词
            paren_match = re.search(r'[（(]([^）)]+)[）)]', display_name)
            if paren_match:
                inner = paren_match.group(1)
                for kw in re.split(r'[/、]', inner):
                    kw = kw.strip()
                    if kw and kw not in match_keywords:
                        match_keywords.append(kw)
            
            def field_fuzzy_matches(f, keywords: list) -> bool:
                """检查字段是否匹配任一关键词"""
                for kw in keywords:
                    if f.display_name == kw:
                        return True
                    if kw in f.display_name:
                        return True
                    f_synonyms = getattr(f, 'synonyms', []) or []
                    if kw in f_synonyms:
                        return True
                return False
            
            # 3.1 精确匹配
            for fid, f in self.model.fields.items():
                if self._field_belongs_to_table(f, table_id) and f.display_name == display_name:
                    logger.debug(
                        "跨表字段映射（精确显示名匹配）",
                        original_field_id=field_id,
                        target_table_id=table_id,
                        mapped_field_id=fid,
                        display_name=display_name,
                        column=f.column
                    )
                    return (f.column, f.display_name)
            
            # 3.2 模糊匹配（使用关键词）
            for fid, f in self.model.fields.items():
                if self._field_belongs_to_table(f, table_id) and field_fuzzy_matches(f, match_keywords):
                    logger.debug(
                        "跨表字段映射（模糊匹配，后备方案）",
                        original_field_id=field_id,
                        target_table_id=table_id,
                        mapped_field_id=fid,
                        original_display_name=display_name,
                        target_display_name=f.display_name,
                        column=f.column
                    )
                    return (f.column, f.display_name)
            
            # 3. 显示名匹配失败，尝试通过物理列名匹配
            col_name = field.column
            for fid, f in self.model.fields.items():
                if self._field_belongs_to_table(f, table_id) and f.column == col_name:
                    return (f.column, f.display_name)
            
            # 4. 都失败了，返回 None（该表可能没有对应字段）
            logger.warning(
                "跨表字段映射失败：目标表中未找到对应字段",
                field_id=field_id,
                display_name=display_name,
                target_table_id=table_id
            )
            return None
        
        if field_id in self.model.dimensions:
            dim = self.model.dimensions[field_id]
            if hasattr(dim, 'datasource_id') and self._field_belongs_to_table(dim, table_id):
                return (dim.column, dim.display_name)
            
            # 尝试通过显示名查找
            display_name = dim.display_name
            for did, d in self.model.dimensions.items():
                if hasattr(d, 'datasource_id') and self._field_belongs_to_table(d, table_id) and d.display_name == display_name:
                    return (d.column, d.display_name)
            
            return (dim.column, dim.display_name)
        
        if field_id in self.model.measures:
            measure = self.model.measures[field_id]
            if hasattr(measure, 'datasource_id') and self._field_belongs_to_table(measure, table_id):
                col_name = self._get_physical_column_name(field_id)
                return (col_name, measure.display_name)
            
            # 尝试通过显示名查找
            display_name = measure.display_name
            for mid, m in self.model.measures.items():
                if hasattr(m, 'datasource_id') and self._field_belongs_to_table(m, table_id) and m.display_name == display_name:
                    col_name = self._get_physical_column_name(mid)
                    return (col_name, m.display_name)
            
            col_name = self._get_physical_column_name(field_id)
            return (col_name, measure.display_name)

        # 最后的后备：field_id 可能是显示名而不是字段ID
        # 在目标表中按显示名查找字段
        for fid, f in self.model.fields.items():
            if f.datasource_id == table_id and f.display_name == field_id:
                logger.debug(
                    "通过显示名查找字段成功",
                    display_name=field_id,
                    table_id=table_id,
                    field_id=fid,
                    column=f.column
                )
                return (f.column, f.display_name)

        # 在 dimensions 中按显示名查找
        for did, d in self.model.dimensions.items():
            if hasattr(d, 'datasource_id') and d.datasource_id == table_id and d.display_name == field_id:
                return (d.column, d.display_name)

        # 在 measures 中按显示名查找
        for mid, m in self.model.measures.items():
            if hasattr(m, 'datasource_id') and m.datasource_id == table_id and m.display_name == field_id:
                col_name = self._get_physical_column_name(mid)
                return (col_name, m.display_name)

        logger.warning(
            "无法解析字段（既非字段ID也未通过显示名匹配）",
            field_id=field_id,
            table_id=table_id
        )
        return None

    def _get_dimension_column_name(self, dim_id: str) -> Optional[str]:
        """获取维度的列名"""
        if dim_id in self.model.dimensions:
            return self.model.dimensions[dim_id].column
        if dim_id in self.model.fields:
            return self.model.fields[dim_id].column
        return None

    def _build_filter_condition_for_table(
        self,
        filter_cond: FilterCondition,
        table_id: str,
        table_alias: str,
        cross_table_mappings: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Optional[exp.Expression]:
        """
        为指定表构建过滤条件表达式
        
        Args:
            filter_cond: 过滤条件
            table_id: 表ID
            table_alias: 表别名
            cross_table_mappings: 跨表字段映射
        
        Returns:
            SQLGlot 条件表达式
        """
        col_info = self._resolve_field_column_for_table(filter_cond.field, table_id, cross_table_mappings)
        if not col_info:
            # 记录详细警告，便于调试跨表过滤问题
            original_field = self.model.fields.get(filter_cond.field)
            original_display_name = original_field.display_name if original_field else filter_cond.field
            logger.warning(
                f"跨表过滤条件构建失败：无法在目标表中找到对应字段",
                filter_field=filter_cond.field,
                filter_display_name=original_display_name,
                filter_value=filter_cond.value,
                target_table_id=table_id,
                has_cross_mappings=bool(cross_table_mappings),
                suggestion="请确保该字段已添加到 cross_table_field_mappings 中"
            )
            return None
        
        col_name, _ = col_info
        column_expr = exp.column(col_name, table=table_alias)
        
        op = filter_cond.op.upper()
        value = filter_cond.value
        
        if op == "=":
            return column_expr.eq(self._value_to_expression(value))
        elif op == "!=":
            return column_expr.neq(self._value_to_expression(value))
        elif op == ">":
            return exp.GT(this=column_expr, expression=self._value_to_expression(value))
        elif op == ">=":
            return exp.GTE(this=column_expr, expression=self._value_to_expression(value))
        elif op == "<":
            return exp.LT(this=column_expr, expression=self._value_to_expression(value))
        elif op == "<=":
            return exp.LTE(this=column_expr, expression=self._value_to_expression(value))
        elif op == "IN":
            if isinstance(value, list):
                return column_expr.isin(*[self._value_to_expression(v) for v in value])
            return column_expr.eq(self._value_to_expression(value))
        elif op == "NOT IN":
            if isinstance(value, list):
                return exp.Not(this=column_expr.isin(*[self._value_to_expression(v) for v in value]))
            return column_expr.neq(self._value_to_expression(value))
        elif op == "LIKE":
            return exp.Like(this=column_expr, expression=self._value_to_expression(value))
        elif op == "IS NULL":
            return column_expr.is_(exp.Null())
        elif op == "IS NOT NULL":
            return exp.Not(this=column_expr.is_(exp.Null()))
        
        return None

    def _build_duplicate_detection_query(
        self,
        ir: IntermediateRepresentation,
        joins: List[Join]
    ) -> exp.Expression:
        """
        构建重复检测查询（使用窗口函数）

        生成类似如下的SQL:
        SELECT * FROM (
          SELECT
            [字段列表],
            COUNT(*) OVER (PARTITION BY [duplicate_by字段]) as _duplicate_count
          FROM table
          WHERE [filters]
        ) t
        WHERE _duplicate_count > 1
        ORDER BY [duplicate_by字段], [主键]

        Args:
            ir: 中间表示
            joins: Join 路径

        Returns:
            SQLGlot Expression
        """
        logger.debug("开始构建重复检测查询")

        # 1. 确定主表
        main_table = None  # 初始化，避免未定义引用
        if joins:
            main_table = joins[0].from_table
        else:
            # 从 duplicate_by 中提取
            if ir.duplicate_by:
                dup_field_id = ir.duplicate_by[0]
                if dup_field_id in self.model.dimensions:
                    dim = self.model.dimensions[dup_field_id]
                    main_table = dim.table
                elif dup_field_id in self.model.fields:
                    # 支持标识字段（identifier）
                    field = self.model.fields[dup_field_id]
                    main_table = field.datasource_id
                    logger.debug(f"从统一字段模型确定主表: {main_table}")
            # 从 filters 中提取
            if not main_table and ir.filters:
                for filter_cond in ir.filters:
                    if filter_cond.field in self.model.dimensions:
                        dim = self.model.dimensions[filter_cond.field]
                        main_table = dim.table
                        break
                    elif filter_cond.field in self.model.fields:
                        field = self.model.fields[filter_cond.field]
                        main_table = field.datasource_id
                        break

            # 最后的fallback：使用第一个source
            if not main_table:
                main_table = list(self.model.sources.keys())[0]

        logger.debug(f"重复检测查询主表: {main_table}")

        # 2.: 获取明细字段（优先使用IR中指定的dimensions）
        source = self.model.sources[main_table]
        select_exprs = []
        field_names = []
        field_names_set = set()
        spatial_columns = set()  # 记录空间字段，用于后续格式转换

        # 首先添加IR中指定的dimensions字段（这些是用户真正想看的字段）
        # 保存 column -> display_name 的映射，用于后续生成 SQL 别名
        column_display_map = {}
        if ir.dimensions:
            logger.debug(f"IR dimensions: {ir.dimensions}, self.model.fields keys: {list(self.model.fields.keys())[:10]}")
            for dim_id in ir.dimensions:
                dim_column = None
                display_name = None
                is_spatial = False
                if dim_id in self.model.dimensions:
                    dim = self.model.dimensions[dim_id]
                    dim_column = dim.column
                    display_name = dim.display_name
                    is_spatial = dim.field_category in ('geometry', 'spatial')
                    logger.debug(f"在 dimensions 中找到字段 {dim_id}: column={dim_column}, display_name={display_name}, is_spatial={is_spatial}")
                elif dim_id in self.model.fields:
                    field = self.model.fields[dim_id]
                    dim_column = field.column
                    display_name = field.display_name
                    is_spatial = field.field_category in ('geometry', 'spatial')
                    logger.debug(f"在 fields 中找到字段 {dim_id}: column={dim_column}, display_name={display_name}, category={field.field_category}, is_spatial={is_spatial}")
                else:
                    logger.warning(f"字段 {dim_id} 在 dimensions 和 fields 中都未找到")
                
                if dim_column and dim_column not in field_names_set:
                    field_names.append(dim_column)
                    field_names_set.add(dim_column)
                    if display_name and display_name != dim_column:
                        column_display_map[dim_column] = display_name
                    if is_spatial:
                        spatial_columns.add(dim_column)
            logger.debug(f"从IR dimensions获取字段: {field_names}, 显示名映射: {column_display_map}, 空间字段: {spatial_columns}")

        # 如果IR没有指定dimensions，则从Fields配置获取
        if not field_names:
            datasource_fields = [
                field for field in self.model.fields.values()
                if field.datasource_id == main_table and field.is_active
            ]

            if datasource_fields:
                # 按优先级排序
                sorted_fields = sorted(
                    datasource_fields,
                    key=lambda f: (not f.is_primary, -f.priority)
                )
                for field in sorted_fields[:10]:
                    field_names.append(field.column)
                    if field.display_name and field.display_name != field.column:
                        column_display_map[field.column] = field.display_name
                    if field.field_category in ('geometry', 'spatial'):
                        spatial_columns.add(field.column)
                        logger.debug(f"回退逻辑：识别到空间字段 {field.column}, category={field.field_category}")
                field_names_set = set(field_names)
                logger.debug(f"回退逻辑：从Fields配置获取字段: {field_names}, 显示名映射: {column_display_map}, 空间字段: {spatial_columns}")
            else:
                # 回退到物理列
                if not source.columns:
                    field_names = []
                    logger.warning(f"表 {main_table} 没有Fields配置也没有物理列")
                else:
                    # 常见的空间字段名（用于识别，不是排除）
                    spatial_column_names = {'shape', 'geometry', 'geom', 'the_geom', 'wkb_geometry'}
                    for col in source.columns[:10]:
                        if hasattr(col, 'column_name'):
                            field_names.append(col.column_name)
                            if col.column_name.lower() in spatial_column_names:
                                spatial_columns.add(col.column_name)
                    field_names_set = set(field_names)
                    logger.warning(f"表 {main_table} 没有Fields配置，使用物理列")

        # 确保主键字段被包含在 SELECT 列表中（用于 ORDER BY）
        if source.primary_key:
            for pk_col in source.primary_key:
                if pk_col not in field_names_set:
                    field_names.append(pk_col)
                    field_names_set.add(pk_col)
                    logger.debug(f"添加主键字段到 SELECT 列表: {pk_col}")

        # 确保 duplicate_by 字段被包含在 SELECT 列表中（用于 PARTITION BY 和 ORDER BY）
        for dup_field_id in ir.duplicate_by:
            dup_column = None
            if dup_field_id in self.model.dimensions:
                dup_column = self.model.dimensions[dup_field_id].column
            elif dup_field_id in self.model.fields:
                dup_column = self.model.fields[dup_field_id].column
            
            if dup_column and dup_column not in field_names_set:
                field_names.append(dup_column)
                field_names_set.add(dup_column)
                logger.debug(f"添加 duplicate_by 字段到 SELECT 列表: {dup_column}")

        # 添加所有字段到 SELECT（空间字段转换为WKT格式，使用显示名作为别名）
        table_alias = self._get_table_alias(main_table)
        for field_name in field_names:
            # 获取显示名（如果有），否则使用物理列名
            display_name = column_display_map.get(field_name, field_name)
            
            if field_name in spatial_columns:
                # 对空间字段使用 .STAsText() 转换为可读的WKT格式 (SQL Server)
                # 生成: [table].[Shape].STAsText() AS [显示名]
                col_ref = exp.column(field_name, table=table_alias)
                # 使用 Dot 表达式来生成方法调用
                wkt_expr = exp.Dot(
                    this=col_ref,
                    expression=exp.Anonymous(this="STAsText")
                )
                select_exprs.append(wkt_expr.as_(display_name))
            else:
                # 使用显示名作为别名
                col_expr = exp.column(field_name, table=table_alias)
                if display_name != field_name:
                    select_exprs.append(col_expr.as_(display_name))
                else:
                    select_exprs.append(col_expr)

        # 3. 添加窗口函数：COUNT(*) OVER (PARTITION BY duplicate_by字段)
        partition_by_exprs = []
        for dim_id in ir.duplicate_by:
            # 支持在 dimensions、fields（包括标识字段）中查找
            if dim_id in self.model.dimensions:
                dim = self.model.dimensions[dim_id]
                partition_by_exprs.append(exp.column(dim.column, table=self._get_table_alias(dim.table)))
            elif dim_id in self.model.fields:
                # 支持统一字段模型（包括标识字段 identifier）
                field = self.model.fields[dim_id]
                partition_by_exprs.append(exp.column(field.column, table=self._get_table_alias(field.datasource_id)))
                logger.debug(f"duplicate_by 使用统一字段: {field.display_name}")
            else:
                logger.warning(f"duplicate_by 中的字段未找到: {dim_id}")

        if not partition_by_exprs:
            raise CompilationError("重复检测查询必须指定 duplicate_by 字段")

        # 构建窗口函数: COUNT(*) OVER (PARTITION BY ...)
        window_spec = exp.Window(
            this=exp.Count(this=exp.Star()),
            partition_by=partition_by_exprs
        )
        select_exprs.append(window_spec.as_("_duplicate_count"))

        # 4. 构建子查询
        from_table = table(
            source.table_name,
            db=source.schema_name,
            alias=self._get_table_alias(main_table)
        )

        subquery = select(*select_exprs).from_(from_table)

        # 添加 JOIN
        subquery = self._add_joins(subquery, joins)

        # 添加 WHERE 条件（ 传入joins以支持JOIN策略）
        where_conditions = self._build_where_clause(ir, main_table, joins)
        if where_conditions:
            subquery = subquery.where(exp.and_(*where_conditions))

        # 5. 构建外层查询：SELECT * FROM (subquery) WHERE _duplicate_count > 1
        subquery_alias = subquery.subquery(alias="t")

        # 外层 SELECT *
        outer_select_exprs = [exp.Star()]
        outer_query = select(*outer_select_exprs).from_(subquery_alias)

        # 添加 WHERE _duplicate_count > 1
        duplicate_filter = exp.GT(
            this=exp.column("_duplicate_count", table="t"),
            expression=exp.Literal.number(1)
        )
        outer_query = outer_query.where(duplicate_filter)

        # 6. 添加 ORDER BY（按 duplicate_by 字段排序，让重复的记录排在一起）
        # 注意：需要使用子查询中的别名（display_name），而不是物理列名
        order_exprs = []
        for dim_id in ir.duplicate_by:
            order_col = None
            if dim_id in self.model.dimensions:
                dim = self.model.dimensions[dim_id]
                # 使用显示名（如果在子查询中使用了别名）
                order_col = column_display_map.get(dim.column, dim.column)
            elif dim_id in self.model.fields:
                # 支持统一字段模型（包括标识字段）
                field = self.model.fields[dim_id]
                order_col = column_display_map.get(field.column, field.column)
            
            if order_col:
                order_exprs.append(
                    exp.Ordered(
                        this=exp.column(order_col, table="t"),
                        desc=False
                    )
                )

        # 添加主键排序（如果有）
        if source.primary_key:
            for pk_col in source.primary_key:
                # 主键通常没有显示名映射，直接使用物理列名
                order_col = column_display_map.get(pk_col, pk_col)
                order_exprs.append(
                    exp.Ordered(
                        this=exp.column(order_col, table="t"),
                        desc=False
                    )
                )

        if order_exprs:
            outer_query = outer_query.order_by(*order_exprs)

        # 7. 添加 LIMIT
        if ir.limit:
            outer_query = outer_query.limit(ir.limit)

        logger.debug(f"重复检测查询构建完成，按 {ir.duplicate_by} 分组检测重复")
        return outer_query
    
    def _build_window_detail_query(
        self,
        ir: IntermediateRepresentation,
        joins: List[Join]
    ) -> exp.Expression:
        """
        构建窗口函数明细查询（分组TopN）
        
        生成类似如下的SQL:
        SELECT * FROM (
          SELECT
            [字段列表],
            ROW_NUMBER() OVER (PARTITION BY [partition_by字段] ORDER BY [sort_by] DESC) as _row_num
          FROM table
          WHERE [filters]
        ) t
        WHERE _row_num <= [window_limit]
        ORDER BY [partition_by字段], _row_num
        
        Args:
            ir: 中间表示
            joins: Join 路径
            
        Returns:
            SQLGlot Expression
        """
        logger.debug("开始构建窗口函数明细查询（分组TopN）")
        
        # 1. 确定主表
        main_table = None
        if joins:
            main_table = joins[0].from_table
        else:
            # 从 partition_by 中提取
            if ir.partition_by:
                partition_field_id = ir.partition_by[0]
                if partition_field_id in self.model.fields:
                    field = self.model.fields[partition_field_id]
                    main_table = field.datasource_id
                elif partition_field_id in self.model.dimensions:
                    dim = self.model.dimensions[partition_field_id]
                    main_table = dim.table
            # 从 sort_by 中提取
            elif ir.sort_by:
                # 检查是否为派生指标
                is_derived, derived_name, derived_def = self._is_derived_metric(ir.sort_by)
                if is_derived and derived_def:
                    # 从派生指标的依赖字段中提取表
                    field_deps = derived_def.get('field_dependencies', [])
                    for dep in field_deps:
                        dep_field_id = dep.get('field_id')
                        if dep_field_id and dep_field_id in self.model.measures:
                            main_table = self.model.measures[dep_field_id].table
                            break
                        elif dep_field_id and dep_field_id in self.model.dimensions:
                            main_table = self.model.dimensions[dep_field_id].table
                            break
                elif ir.sort_by in self.model.fields:
                    field = self.model.fields[ir.sort_by]
                    main_table = field.datasource_id
                elif ir.sort_by in self.model.measures:
                    main_table = self.model.measures[ir.sort_by].table
                elif ir.sort_by in self.model.dimensions:
                    main_table = self.model.dimensions[ir.sort_by].table
            # 从 filters 中提取
            elif ir.filters:
                for filter_cond in ir.filters:
                    if filter_cond.field in self.model.fields:
                        field = self.model.fields[filter_cond.field]
                        main_table = field.datasource_id
                        break
                    elif filter_cond.field in self.model.dimensions:
                        dim = self.model.dimensions[filter_cond.field]
                        main_table = dim.table
                        break
            
            # 最后的fallback：使用第一个source
            if not main_table:
                main_table = list(self.model.sources.keys())[0]
        
        logger.debug(f"窗口函数明细查询主表: {main_table}")
        
        # 2. 获取明细字段（复用_get_detail_columns方法）
        select_exprs = self._get_detail_columns(main_table, ir)
        source = self.model.sources[main_table]

        if not select_exprs:
            raise CompilationError("窗口函数明细查询没有可显示的字段")

        # 2.1 如果 sort_by 是派生指标，添加到 SELECT 列表中以便用户看到排序依据的值
        added_derived_metrics = set()  # 记录已添加的派生指标，避免重复
        if ir.sort_by:
            is_derived, derived_name, derived_def = self._is_derived_metric(ir.sort_by)
            if is_derived and derived_name and derived_def:
                table_alias = self._get_table_alias(main_table)
                formula_sql, alias = self._build_derived_metric_for_detail(derived_name, derived_def, table_alias)
                if formula_sql:
                    try:
                        derived_expr = parse_one(formula_sql, dialect=self.dialect)
                        # 使用派生指标名称作为别名
                        select_exprs.append(derived_expr.as_(derived_name))
                        added_derived_metrics.add(derived_name)
                        logger.debug(f"窗口函数明细查询添加派生指标列: {derived_name}")
                    except Exception as e:
                        logger.warning(f"添加派生指标列失败: {derived_name}, error={e}")

        # 2.2 处理 ir.metrics 中的派生指标（用户明确要求查看的计算字段）
        if ir.metrics:
            table_alias = self._get_table_alias(main_table)
            for metric_item in ir.metrics:
                # 规范化 metric：提取字段ID
                metric_id, _, _, _ = self._normalize_metric_spec(metric_item)
                
                is_derived, derived_name, derived_def = self._is_derived_metric(metric_id)
                if is_derived and derived_name and derived_def:
                    # 避免重复添加（sort_by可能已经添加过）
                    if derived_name in added_derived_metrics:
                        logger.debug(f"派生指标 {derived_name} 已作为排序字段添加，跳过")
                        continue

                    formula_sql, alias = self._build_derived_metric_for_detail(derived_name, derived_def, table_alias)
                    if formula_sql:
                        try:
                            derived_expr = parse_one(formula_sql, dialect=self.dialect)
                            select_exprs.append(derived_expr.as_(derived_name))
                            added_derived_metrics.add(derived_name)
                            logger.debug(f"窗口函数明细查询添加用户请求的派生指标: {derived_name}")
                        except Exception as e:
                            logger.warning(f"添加派生指标列失败: {derived_name}, error={e}")

        # 构建物理列名到显示名的映射（用于ORDER BY）
        column_display_name_map = {}
        for field_id, field in self.model.fields.items():
            if field.datasource_id == main_table:
                physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
                if physical_col and field.display_name:
                    column_display_name_map[physical_col] = field.display_name
        
        # 3. 构建窗口函数：ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
        
        # 3.1 构建PARTITION BY表达式
        partition_by_exprs = []
        for field_id in ir.partition_by:
            if field_id in self.model.fields:
                field = self.model.fields[field_id]
                physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
                partition_by_exprs.append(
                    exp.column(physical_col, table=self._get_table_alias(field.datasource_id))
                )
            elif field_id in self.model.dimensions:
                dim = self.model.dimensions[field_id]
                partition_by_exprs.append(
                    exp.column(dim.column, table=self._get_table_alias(dim.table))
                )
            else:
                logger.warning(f"partition_by 中的字段无法识别: {field_id}")
        
        if not partition_by_exprs:
            raise CompilationError("窗口函数明细查询必须指定 partition_by 字段")
        
        # 3.2 构建ORDER BY表达式
        if not ir.sort_by:
            raise CompilationError("窗口函数明细查询必须指定 sort_by 字段")

        sort_field_id = ir.sort_by
        sort_expr = None

        # 0) 检查是否为派生指标
        is_derived, derived_name, derived_def = self._is_derived_metric(sort_field_id)
        if is_derived and derived_name and derived_def:
            # 窗口函数中，派生指标需要转换为行级计算表达式
            table_alias = self._get_table_alias(main_table)
            formula_sql, alias = self._build_derived_metric_for_detail(derived_name, derived_def, table_alias)
            if formula_sql:
                try:
                    sort_expr = parse_one(formula_sql, dialect=self.dialect)
                    logger.debug(f"窗口函数明细查询按派生指标排序: {derived_name} -> {formula_sql}")
                except Exception as e:
                    logger.warning(f"解析派生指标排序表达式失败: {formula_sql}, error={e}")
            else:
                logger.warning(f"窗口函数明细查询中无法按派生指标 {derived_name} 排序（依赖字段不完整或为纯聚合指标）")
        # 1) 从 fields 中查找
        elif sort_field_id in self.model.fields:
            field = self.model.fields[sort_field_id]
            physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
            sort_expr = exp.column(physical_col, table=self._get_table_alias(field.datasource_id))
        # 2) 从 dimensions 中查找
        elif sort_field_id in self.model.dimensions:
            dim = self.model.dimensions[sort_field_id]
            sort_expr = exp.column(dim.column, table=self._get_table_alias(dim.table))
        # 3) 从 measures 中查找
        elif sort_field_id in self.model.measures:
            measure = self.model.measures[sort_field_id]
            sort_expr = exp.column(measure.column, table=self._get_table_alias(measure.table))

        if not sort_expr:
            raise CompilationError(f"窗口函数明细查询无法识别排序字段: {sort_field_id}")
        
        # 3.3 构建窗口函数: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
        window_spec = exp.Window(
            this=exp.RowNumber(),
            partition_by=partition_by_exprs,
            order=exp.Order(
                expressions=[exp.Ordered(this=sort_expr, desc=(ir.sort_order == 'desc'))]
            )
        )
        select_exprs.append(window_spec.as_("_row_num"))
        
        # 4. 构建子查询
        from_table = table(
            source.table_name,
            db=source.schema_name,
            alias=self._get_table_alias(main_table)
        )
        
        subquery = select(*select_exprs).from_(from_table)
        
        # 添加 JOIN
        subquery = self._add_joins(subquery, joins)
        
        # 添加 WHERE 条件
        where_conditions = self._build_where_clause(ir, main_table, joins)
        if where_conditions:
            subquery = subquery.where(exp.and_(*where_conditions))
        
        # 5. 构建外层查询：SELECT * FROM (subquery) WHERE _row_num <= window_limit
        subquery_alias = subquery.subquery(alias="RankedData")
        
        # 外层 SELECT *（排除_row_num）
        outer_select_exprs = [exp.Star()]
        outer_query = select(*outer_select_exprs).from_(subquery_alias)
        
        # 添加 WHERE _row_num <= window_limit
        if ir.window_limit:
            row_num_filter = exp.LTE(
                this=exp.column("_row_num", table="RankedData"),
                expression=exp.Literal.number(ir.window_limit)
            )
            outer_query = outer_query.where(row_num_filter)
        
        # 6. 添加 ORDER BY（按分组字段+行号排序，让结果更清晰）
        order_exprs = []
        for field_id in ir.partition_by:
            if field_id in self.model.fields:
                field = self.model.fields[field_id]
                physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
                # 如果有显示名映射，使用显示名
                col_name = column_display_name_map.get(physical_col, physical_col)
                order_exprs.append(
                    exp.Ordered(
                        this=exp.column(col_name, table="RankedData"),
                        desc=False
                    )
                )
            elif field_id in self.model.dimensions:
                dim = self.model.dimensions[field_id]
                col_name = column_display_name_map.get(dim.column, dim.column)
                order_exprs.append(
                    exp.Ordered(
                        this=exp.column(col_name, table="RankedData"),
                        desc=False
                    )
                )
        
        # 添加行号排序
        order_exprs.append(
            exp.Ordered(
                this=exp.column("_row_num", table="RankedData"),
                desc=False
            )
        )
        
        if order_exprs:
            outer_query = outer_query.order_by(*order_exprs)
        
        # 7. 添加全局 LIMIT（可选）
        if ir.limit:
            outer_query = outer_query.limit(ir.limit)
        
        logger.debug(f"窗口函数明细查询构建完成，分组字段: {ir.partition_by}, 排序字段: {ir.sort_by}, 每组Top{ir.window_limit}")
        return outer_query

    # ========== 混合架构扩展：条件聚合、计算字段、占比指标、HAVING ==========

    def _build_conditional_metric_expression(self, cond_metric, main_table: str) -> Optional[exp.Expression]:
        """
        构建条件聚合指标的 SQL 表达式
        
        生成形如: ROUND(SUM(CASE WHEN condition THEN value ELSE 0 END), 2) AS alias
        
        Args:
            cond_metric: ConditionalMetric 对象
            main_table: 主表ID
        
        Returns:
            带别名的聚合表达式
        """
        from server.models.ir import ConditionalMetric
        
        try:
            # 获取条件字段信息
            cond_field_id = cond_metric.condition.field
            cond_op = cond_metric.condition.op
            cond_value = cond_metric.condition.value
            
            # 解析条件字段的物理列
            cond_col_expr = self._resolve_field_to_column_expr(cond_field_id)
            if cond_col_expr is None:
                logger.warning(f"条件聚合：无法解析条件字段 {cond_field_id}")
                return None
            
            # 构建 CASE WHEN 条件
            case_condition = self._build_filter_condition_expr(cond_col_expr, cond_op, cond_value)
            
            # 处理 THEN 部分（聚合字段或 1）
            if cond_metric.field == "__row_count__":
                # COUNT 模式：CASE WHEN cond THEN 1 ELSE 0 END
                then_expr = exp.Literal.number(1)
            else:
                # 其他聚合：CASE WHEN cond THEN field ELSE 0 END
                then_expr = self._resolve_field_to_column_expr(cond_metric.field)
                if then_expr is None:
                    logger.warning(f"条件聚合：无法解析聚合字段 {cond_metric.field}")
                    return None
            
            # ELSE 部分
            else_value = cond_metric.else_value if hasattr(cond_metric, 'else_value') and cond_metric.else_value is not None else 0
            else_expr = exp.Literal.number(else_value)
            
            # 构建 CASE 表达式
            case_expr = exp.Case(
                ifs=[exp.If(this=case_condition, true=then_expr)],
                default=else_expr
            )
            
            # 包装聚合函数
            agg_type = getattr(cond_metric, 'aggregation', 'SUM').upper()
            if agg_type == 'COUNT':
                agg_expr = exp.Sum(this=case_expr)  # COUNT 模式用 SUM(CASE WHEN ... THEN 1)
            elif agg_type == 'SUM':
                agg_expr = exp.Sum(this=case_expr)
            elif agg_type == 'AVG':
                agg_expr = exp.Avg(this=case_expr)
            elif agg_type == 'MIN':
                agg_expr = exp.Min(this=case_expr)
            elif agg_type == 'MAX':
                agg_expr = exp.Max(this=case_expr)
            else:
                agg_expr = exp.Sum(this=case_expr)  # 默认 SUM
            
            # 添加 ROUND 处理（格式化小数位数）
            decimal_places = getattr(cond_metric, 'decimal_places', 2)
            rounded_expr = exp.Round(this=agg_expr, decimals=exp.Literal.number(decimal_places))
            
            return rounded_expr.as_(cond_metric.alias)
            
        except Exception as e:
            logger.error(f"构建条件聚合表达式失败: {cond_metric.alias}", error=str(e))
            return None

    def _build_ratio_numerator_column(
        self, 
        ratio_metric, 
        main_table: str
    ) -> Optional[exp.Expression]:
        """
        为占比指标自动生成分子列
        
        当 ratio_metric 有 numerator_condition 时，自动生成对应的分子列，
        让用户能够看到分子的具体数值，而不仅仅是占比百分比。
        
        生成形如: SUM(CASE WHEN condition THEN value ELSE 0 END) AS "分子名称"
        
        Args:
            ratio_metric: RatioMetric 对象
            main_table: 主表ID
        
        Returns:
            带别名的聚合表达式，或 None（如果无法生成）
        """
        try:
            # 只有当有 numerator_condition 时才生成分子列
            if not ratio_metric.numerator_condition:
                return None
            
            # 获取条件字段信息
            cond = ratio_metric.numerator_condition
            cond_field_id = cond.field
            cond_op = cond.op
            cond_value = cond.value
            
            # 解析条件字段的物理列
            cond_col_expr = self._resolve_field_to_column_expr(cond_field_id)
            if cond_col_expr is None:
                logger.warning(f"占比分子列：无法解析条件字段 {cond_field_id}")
                return None
            
            # 构建 CASE WHEN 条件
            case_condition = self._build_filter_condition_expr(cond_col_expr, cond_op, cond_value)
            
            # 获取分子字段
            numerator_field = ratio_metric.numerator_field
            if numerator_field == "__row_count__":
                # COUNT 模式：CASE WHEN cond THEN 1 ELSE 0 END
                then_expr = exp.Literal.number(1)
            else:
                then_expr = self._resolve_field_to_column_expr(numerator_field)
                if then_expr is None:
                    logger.warning(f"占比分子列：无法解析分子字段 {numerator_field}")
                    return None
            
            # ELSE 部分
            else_expr = exp.Literal.number(0)
            
            # 构建 CASE 表达式
            case_expr = exp.Case(
                ifs=[exp.If(this=case_condition, true=then_expr)],
                default=else_expr
            )
            
            # 使用 SUM 聚合
            agg_expr = exp.Sum(this=case_expr)
            
            # 生成别名：从 numerator_condition 的值推断，或使用占比别名的前缀
            # 例如: "商服用地面积占比" -> "商服用地面积"
            alias = ratio_metric.alias
            if alias and alias.endswith("占比"):
                numerator_alias = alias[:-2]  # 去掉"占比"后缀
            elif alias and alias.endswith("比例"):
                numerator_alias = alias[:-2]  # 去掉"比例"后缀
            else:
                # 使用条件值作为别名的一部分
                cond_value_str = str(cond_value) if cond_value else "分子"
                numerator_alias = f"{cond_value_str}面积"
            
            # 获取分子字段的单位信息并添加到别名中
            unit = None
            if numerator_field != "__row_count__" and numerator_field in self.model.fields:
                field = self.model.fields[numerator_field]
                if field.field_category == 'measure' and hasattr(field, 'measure_props') and field.measure_props:
                    unit = field.measure_props.unit
                    # 检查是否有单位转换配置
                    unit_conversion = getattr(field, 'unit_conversion', None)
                    if unit_conversion and isinstance(unit_conversion, dict) and unit_conversion.get('enabled'):
                        unit = unit_conversion.get('display_unit', unit)
            elif numerator_field != "__row_count__" and numerator_field in getattr(self.model, 'measures', {}):
                measure = self.model.measures[numerator_field]
                unit = getattr(measure, 'unit', None)
            
            # 如果有单位且别名中还没有包含单位，则添加单位
            if unit and f"({unit})" not in numerator_alias:
                numerator_alias = f"{numerator_alias}({unit})"
            
            logger.debug(f"生成占比分子列: {numerator_alias}, 单位: {unit}")
            return agg_expr.as_(numerator_alias)
            
        except Exception as e:
            logger.error(f"构建占比分子列失败: {ratio_metric.alias}", error=str(e))
            return None

    def _build_calculated_field_expression(
        self, 
        calc_field, 
        main_table: str,
        is_aggregation_query: bool = False
    ) -> Optional[exp.Expression]:
        """
        构建计算字段的 SQL 表达式
        
        支持表达式如: {field_a} * {field_b}, {metric_a} / NULLIF({metric_b}, 0) * 100
        
        在聚合查询中，如果指定了 aggregation 属性（如 AVG），则对表达式结果应用聚合函数。
        如果未指定 aggregation 且是聚合查询，默认使用 AVG 对表达式求平均。
        
        Args:
            calc_field: CalculatedField 对象
            main_table: 主表ID
            is_aggregation_query: 是否为聚合查询（有 GROUP BY）
        
        Returns:
            带别名的计算表达式
        """
        try:
            expression = calc_field.expression
            field_refs = getattr(calc_field, 'field_refs', []) or []
            aggregation = getattr(calc_field, 'aggregation', None)
            decimal_places = getattr(calc_field, 'decimal_places', 2)
            
            # 替换表达式中的字段引用
            resolved_expr = expression
            for field_id in field_refs:
                # 获取字段的物理列名（带表别名）
                col_sql = self._get_field_column_sql(field_id)
                if col_sql:
                    resolved_expr = resolved_expr.replace(f"{{{field_id}}}", col_sql)
            
            # 解析表达式
            parsed_expr = parse_one(resolved_expr, dialect=self.dialect)
            
            # 在聚合查询中处理聚合逻辑
            if is_aggregation_query:
                # 如果明确指定 NONE，表示表达式内部已包含聚合逻辑，不再包装
                if aggregation == "NONE":
                    # 添加 ROUND 处理
                    rounded_expr = exp.Round(this=parsed_expr, decimals=exp.Literal.number(decimal_places))
                    return rounded_expr.as_(calc_field.alias)
                
                # 如果指定了聚合函数或默认使用 AVG
                agg_func = aggregation if aggregation else "AVG"
                
                if agg_func == "AVG":
                    final_expr = exp.Avg(this=parsed_expr)
                elif agg_func == "SUM":
                    final_expr = exp.Sum(this=parsed_expr)
                elif agg_func == "MAX":
                    final_expr = exp.Max(this=parsed_expr)
                elif agg_func == "MIN":
                    final_expr = exp.Min(this=parsed_expr)
                else:
                    # 未知聚合类型，保持原样
                    final_expr = parsed_expr
                
                # 添加 ROUND 处理
                rounded_expr = exp.Round(this=final_expr, decimals=exp.Literal.number(decimal_places))
                logger.debug(f"聚合查询中对计算字段应用 {agg_func} 并 ROUND({decimal_places}): {calc_field.alias}")
                return rounded_expr.as_(calc_field.alias)
            
            # 非聚合查询也添加 ROUND 处理
            rounded_expr = exp.Round(this=parsed_expr, decimals=exp.Literal.number(decimal_places))
            return rounded_expr.as_(calc_field.alias)
            
        except Exception as e:
            logger.error(f"构建计算字段表达式失败: {calc_field.alias}", error=str(e))
            return None

    def _build_ratio_metric_expression(self, ratio_metric, main_table: str, has_dimensions: bool = False) -> Optional[exp.Expression]:
        """
        构建占比/通过率指标的 SQL 表达式

        生成形如: ROUND(分子 * 100.0 / NULLIF(分母, 0), 2) AS alias

        Args:
            ratio_metric: RatioMetric 对象
            main_table: 主表ID
            has_dimensions: 是否有分组维度（GROUP BY）

        Returns:
            带别名的占比表达式

        逻辑说明:
            - 如果指定了 numerator_field/denominator_field，使用 SUM(field) 进行聚合
            - 如果同时有 condition，使用 SUM(CASE WHEN cond THEN field ELSE 0 END)
            - 如果 field 为 __row_count__ 或未指定，使用 COUNT(*) 或条件计数
            - 当有分组维度且没有 numerator_condition 时，分母使用窗口函数获取全局总量
        """
        try:
            # 构建分子
            num_field = getattr(ratio_metric, 'numerator_field', None)
            num_field_col = None

            # 解析分子字段
            if num_field and num_field != "__row_count__":
                num_field_col = self._resolve_field_to_column_expr(num_field)

            if ratio_metric.numerator_condition:
                # 有条件的分子
                num_cond_field = ratio_metric.numerator_condition.field
                num_cond_col = self._resolve_field_to_column_expr(num_cond_field)
                if num_cond_col is None:
                    return None

                num_case_cond = self._build_filter_condition_expr(
                    num_cond_col,
                    ratio_metric.numerator_condition.op,
                    ratio_metric.numerator_condition.value
                )

                if num_field_col is not None:
                    # SUM(CASE WHEN cond THEN field ELSE 0 END)
                    num_case = exp.Case(
                        ifs=[exp.If(this=num_case_cond, true=num_field_col)],
                        default=exp.Literal.number(0)
                    )
                    numerator = exp.Sum(this=num_case)
                else:
                    # SUM(CASE WHEN cond THEN 1 ELSE 0 END) - 计数模式
                    num_case = exp.Case(
                        ifs=[exp.If(this=num_case_cond, true=exp.Literal.number(1))],
                        default=exp.Literal.number(0)
                    )
                    numerator = exp.Sum(this=num_case)
            elif num_field_col is not None:
                # 无条件，有字段：SUM(field)
                numerator = exp.Sum(this=num_field_col)
            else:
                # 无条件，无字段：COUNT(*)
                numerator = exp.Count(this=exp.Star())

            # 构建分母
            denom_field = getattr(ratio_metric, 'denominator_field', None)
            denom_field_col = None

            # 兜底逻辑：如果分母字段为空但分子是度量字段（非 __row_count__），
            # 则自动使用分子字段作为分母（面积占比场景）
            if not denom_field and num_field and num_field != "__row_count__":
                denom_field = num_field

            # 解析分母字段
            if denom_field and denom_field != "__row_count__":
                denom_field_col = self._resolve_field_to_column_expr(denom_field)

            if ratio_metric.denominator_condition:
                # 有条件的分母
                denom_cond_field = ratio_metric.denominator_condition.field
                denom_cond_col = self._resolve_field_to_column_expr(denom_cond_field)
                if denom_cond_col is None:
                    return None

                denom_case_cond = self._build_filter_condition_expr(
                    denom_cond_col,
                    ratio_metric.denominator_condition.op,
                    ratio_metric.denominator_condition.value
                )

                if denom_field_col is not None:
                    # SUM(CASE WHEN cond THEN field ELSE 0 END)
                    denom_case = exp.Case(
                        ifs=[exp.If(this=denom_case_cond, true=denom_field_col)],
                        default=exp.Literal.number(0)
                    )
                    denominator = exp.Sum(this=denom_case)
                else:
                    # SUM(CASE WHEN cond THEN 1 ELSE 0 END) - 计数模式
                    denom_case = exp.Case(
                        ifs=[exp.If(this=denom_case_cond, true=exp.Literal.number(1))],
                        default=exp.Literal.number(0)
                    )
                    denominator = exp.Sum(this=denom_case)
            elif denom_field_col is not None:
                # 无条件，有字段：SUM(field)
                denominator = exp.Sum(this=denom_field_col)
            else:
                # 无条件，无字段：COUNT(*)
                denominator = exp.Count(this=exp.Star())
            
            # 当有分组维度且没有 numerator_condition 时，分母需要是全局总量
            # 使用窗口函数 SUM(...) OVER () 获取全局值
            use_global_denominator = has_dimensions and not ratio_metric.numerator_condition
            if use_global_denominator:
                # 将分母包装为窗口函数：SUM(分母聚合) OVER ()
                # 例如：SUM(SUM(field)) OVER () 或 SUM(COUNT(*)) OVER ()
                # 使用 exp.Window 而不是 .over() 方法
                denominator = exp.Window(this=exp.Sum(this=denominator))
                logger.debug(f"占比指标使用全局分母: {ratio_metric.alias}")
            
            # 构建除法表达式，使用 NULLIF 防止除零
            nullif_expr = exp.Nullif(this=denominator, expression=exp.Literal.number(0))
            
            # 是否转换为百分比
            as_percentage = getattr(ratio_metric, 'as_percentage', True)
            if as_percentage:
                # 分子 * 100.0 / NULLIF(分母, 0)
                ratio_expr = exp.Div(
                    this=exp.Mul(this=numerator, expression=exp.Literal.number(100.0)),
                    expression=nullif_expr
                )
            else:
                # 分子 / NULLIF(分母, 0)
                ratio_expr = exp.Div(this=numerator, expression=nullif_expr)
            
            # ROUND
            decimal_places = getattr(ratio_metric, 'decimal_places', 2)
            rounded_expr = exp.Round(this=ratio_expr, decimals=exp.Literal.number(decimal_places))
            
            return rounded_expr.as_(ratio_metric.alias)
            
        except Exception as e:
            logger.error(f"构建占比指标表达式失败: {ratio_metric.alias}", error=str(e))
            return None

    def _build_having_clause(self, ir: IntermediateRepresentation, main_table: str) -> List[exp.Expression]:
        """
        构建 HAVING 子句
        
        Args:
            ir: 中间表示
            main_table: 主表ID
        
        Returns:
            HAVING 条件表达式列表
        """
        having_conditions = []
        
        for having_filter in ir.having_filters:
            field_id = having_filter.field
            op = having_filter.op
            value = having_filter.value
            
            # HAVING 条件通常是对聚合结果的过滤，需要使用聚合表达式
            agg_expr = None
            
            # 1. 检查是否为派生指标（如 derived:每亩单价）
            is_derived, derived_name, derived_def = self._is_derived_metric(field_id)
            if is_derived and derived_name and derived_def:
                # 使用派生指标的聚合公式
                table_alias = self._get_table_alias(main_table)
                formula_sql, alias = self._build_derived_metric_expression(derived_name, derived_def, table_alias)
                if formula_sql:
                    try:
                        agg_expr = parse_one(formula_sql, dialect=self.dialect)
                        logger.debug(f"HAVING 子句使用派生指标: {derived_name} -> {formula_sql}")
                    except Exception as e:
                        logger.warning(f"HAVING 解析派生指标失败: {derived_name}, error={e}")
            # 2. 检查是否为 metrics
            elif field_id in self.model.metrics:
                # 从 metrics 获取聚合表达式
                metric = self.model.metrics[field_id]
                expression = self._get_metric_expression(metric)
                if expression:
                    try:
                        agg_expr = parse_one(expression, dialect=self.dialect)
                    except Exception:
                        pass
            # 3. 检查是否为 measures
            elif field_id in self.model.measures:
                # 从 measures 构建聚合表达式
                measure = self.model.measures[field_id]
                agg_sql = self._build_agg_for_measure(measure)
                try:
                    agg_expr = parse_one(agg_sql, dialect=self.dialect)
                except Exception:
                    pass
            # 4. 检查是否为 __row_count__
            elif field_id == "__row_count__":
                agg_expr = exp.Count(this=exp.Star())
            
            if agg_expr is None:
                logger.warning(f"HAVING 子句：无法解析聚合字段 {field_id}")
                continue
            
            # 构建比较条件
            value_expr = self._value_to_expression(value)
            
            if op == "=":
                cond = exp.EQ(this=agg_expr, expression=value_expr)
            elif op == "!=":
                cond = exp.NEQ(this=agg_expr, expression=value_expr)
            elif op == ">":
                cond = exp.GT(this=agg_expr, expression=value_expr)
            elif op == ">=":
                cond = exp.GTE(this=agg_expr, expression=value_expr)
            elif op == "<":
                cond = exp.LT(this=agg_expr, expression=value_expr)
            elif op == "<=":
                cond = exp.LTE(this=agg_expr, expression=value_expr)
            else:
                logger.warning(f"HAVING 子句：不支持的操作符 {op}")
                continue
            
            having_conditions.append(cond)
        
        return having_conditions

    def _wrap_with_comparison_window_functions(
        self,
        query: exp.Expression,
        ir: IntermediateRepresentation,
        main_table: str
    ) -> exp.Expression:
        """
        为派生指标的同比/环比计算包装子查询
        
        根据时间点数量自动选择展示模式：
        - Pivot 模式（2个时间点）：横向展开，如"2023年楼面地价、2024年楼面地价、增长率"
        - Vertical 模式（>2个时间点）：纵向显示，保留年份列，如"年份、楼面地价、同比增长率"
        
        Args:
            query: 原始聚合查询表达式
            ir: 中间表示
            main_table: 主表ID
            
        Returns:
            包装后的查询表达式
        """
        logger.debug(f"开始包装派生指标同比计算，comparison_type={ir.comparison_type}")
        
        # 确定排序维度（时间维度）
        order_dim_id = self._select_time_dimension(ir.dimensions) if ir.dimensions else None
        if not order_dim_id:
            logger.warning("同比/环比计算需要时间维度，但未找到，跳过窗口函数包装")
            return query
        
        # 获取时间维度的显示名/别名
        time_dim_alias = self._get_dimension_alias(order_dim_id)
        if not time_dim_alias:
            logger.warning(f"无法获取时间维度别名: {order_dim_id}")
            return query
        
        # 确定分区维度（非时间维度的其他维度，如行政区）
        partition_dim_ids = [d for d in ir.dimensions if d != order_dim_id]
        partition_dim_aliases = []
        for dim_id in partition_dim_ids:
            alias = self._get_dimension_alias(dim_id)
            if alias:
                partition_dim_aliases.append(alias)
        
        # 确定 LAG 偏移量
        offset_map = {
            'yoy': 1,   # 年同比：1年
            'qoq': 1,   # 季环比：1季度
            'mom': 1,   # 月环比：1个月
            'wow': 1,   # 周环比：1周
        }
        lag_offset = offset_map.get(ir.comparison_type, 1) * (ir.comparison_periods or 1)
        
        # 从过滤条件中提取时间维度的年份值
        year_values = []
        for f in ir.filters:
            if f.field == order_dim_id:
                if f.op == "IN" and isinstance(f.value, list):
                    year_values = [v for v in f.value if isinstance(v, (int, float))]
                elif f.op == "=" and isinstance(f.value, (int, float)):
                    year_values = [f.value]
                elif f.op in [">=", ">", "<=", "<", "BETWEEN"]:
                    # 范围查询，无法确定具体年份，使用 vertical 模式
                    year_values = list(range(2000, 2100))  # 标记为多年
                break
        
        # 排序年份值
        year_values = sorted(set(year_values)) if year_values else []
        num_years = len(year_values)
        
        # 根据时间点数量选择模式
        # 2个时间点：Pivot 模式（横向展开）
        # >2个时间点：Vertical 模式（纵向显示）
        use_pivot_mode = (num_years == 2)
        
        logger.debug(f"时间点数量: {num_years}，使用{'Pivot' if use_pivot_mode else 'Vertical'}模式")
        
        if use_pivot_mode:
            return self._build_pivot_comparison_query(
                query, ir, time_dim_alias, partition_dim_aliases, 
                year_values, lag_offset
            )
        else:
            return self._build_vertical_comparison_query(
                query, ir, time_dim_alias, partition_dim_aliases, lag_offset
            )
    
    def _build_pivot_comparison_query(
        self,
        query: exp.Expression,
        ir: IntermediateRepresentation,
        time_dim_alias: str,
        partition_dim_aliases: List[str],
        year_values: List,
        lag_offset: int
    ) -> exp.Expression:
        """
        构建 Pivot 模式的同比查询（横向展开，适用于两期对比）
        
        结果格式：行政区, 用途分类, 2023年楼面地价, 2024年楼面地价, 增长率
        """
        logger.debug("构建 Pivot 模式同比查询")
        
        target_year = year_values[-1]  # 最大年份（如 2024）
        prev_year = year_values[0]     # 最小年份（如 2023）
        
        # 构建外层 SELECT 列表
        outer_select_exprs = []
        
        # 1. 添加非时间维度列（如行政区、用途分类）
        for alias in partition_dim_aliases:
            outer_select_exprs.append(exp.column(alias, table="t").as_(alias))
        
        # 2. 为每个指标添加横向展开的列（支持派生指标和原子指标）
        first_metric_alias = None
        for metric_item in ir.metrics:
            # 规范化 metric：提取字段ID
            metric_id, _, _, _ = self._normalize_metric_spec(metric_item)
            
            if metric_id == "__row_count__":
                continue
            
            # 获取指标别名（在内层查询中的列名）
            if isinstance(metric_id, str) and metric_id.startswith('derived:'):
                metric_alias = metric_id[8:]  # 移除 "derived:" 前缀
            else:
                # 原子指标：使用显示名作为别名
                metric_alias = self._get_metric_or_measure_alias(metric_id)
            
            if not metric_alias:
                logger.warning(f"Pivot: 无法获取指标别名，跳过: {metric_id}")
                continue
            
            if first_metric_alias is None:
                first_metric_alias = metric_alias
            
            # 构建 PARTITION BY 和 ORDER BY 表达式
            partition_by_exprs = [exp.column(a, table="t") for a in partition_dim_aliases]
            order_expr = exp.column(time_dim_alias, table="t")
            
            # 2.1 上期值列（如"2023年楼面地价"）
            lag_func = exp.Lag(
                this=exp.column(metric_alias, table="t"),
                offset=exp.Literal.number(lag_offset)
            )
            window_kwargs = {
                "this": lag_func,
                "order": exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)])
            }
            if partition_by_exprs:
                window_kwargs["partition_by"] = partition_by_exprs
            window_spec = exp.Window(**window_kwargs)
            
            prev_year_alias = f"{prev_year}年{metric_alias}"
            outer_select_exprs.append(window_spec.as_(prev_year_alias))
            logger.debug(f"Pivot: 添加上期值列: {prev_year_alias}")
            
            # 2.2 当期值列（如"2024年楼面地价"）
            current_value = exp.column(metric_alias, table="t")
            target_year_alias = f"{target_year}年{metric_alias}"
            outer_select_exprs.append(current_value.as_(target_year_alias))
            logger.debug(f"Pivot: 添加当期值列: {target_year_alias}")
            
            # 2.3 增长率列
            lag_func_for_growth = exp.Lag(
                this=exp.column(metric_alias, table="t"),
                offset=exp.Literal.number(lag_offset)
            )
            window_spec_for_growth = exp.Window(
                this=lag_func_for_growth,
                order=exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)]),
                **({"partition_by": partition_by_exprs} if partition_by_exprs else {})
            )
            
            growth_rate_expr = exp.Case(
                ifs=[
                    exp.If(
                        this=exp.Or(
                            this=exp.Is(this=window_spec_for_growth.copy(), expression=exp.Null()),
                            expression=exp.EQ(this=window_spec_for_growth.copy(), expression=exp.Literal.number(0))
                        ),
                        true=exp.Null()
                    )
                ],
                default=exp.Mul(
                    this=exp.Div(
                        this=exp.Sub(this=current_value.copy(), expression=window_spec_for_growth.copy()),
                        expression=window_spec_for_growth.copy()
                    ),
                    expression=exp.Literal.number(100)
                )
            )
            
            growth_alias = f"{metric_alias}_增长率"
            outer_select_exprs.append(growth_rate_expr.as_(growth_alias))
            logger.debug(f"Pivot: 添加增长率列: {growth_alias}")
        
        # 3. 添加 conditional_metrics（条件聚合指标）列 - 这些列在内层查询已计算，外层需要透传并添加同比
        # 注意：conditional_metrics 应该在 ratio_metrics 之前，因为占比是基于条件聚合结果计算的
        if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
            for cond_metric in ir.conditional_metrics:
                cond_alias = cond_metric.alias
                if cond_alias:
                    # 构建 PARTITION BY 和 ORDER BY 表达式
                    partition_by_exprs = [exp.column(a, table="t") for a in partition_dim_aliases]
                    order_expr = exp.column(time_dim_alias, table="t")
                    
                    # 3.1 上期条件指标列（如"2023年工业用地面积"）
                    lag_func = exp.Lag(
                        this=exp.column(cond_alias, table="t"),
                        offset=exp.Literal.number(lag_offset)
                    )
                    window_kwargs = {
                        "this": lag_func,
                        "order": exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)])
                    }
                    if partition_by_exprs:
                        window_kwargs["partition_by"] = partition_by_exprs
                    window_spec = exp.Window(**window_kwargs)
                    
                    prev_cond_alias = f"{prev_year}年{cond_alias}"
                    outer_select_exprs.append(window_spec.as_(prev_cond_alias))
                    logger.debug(f"Pivot: 添加上期条件指标列: {prev_cond_alias}")
                    
                    # 3.2 当期条件指标列（如"2024年工业用地面积"）
                    current_cond = exp.column(cond_alias, table="t")
                    target_cond_alias = f"{target_year}年{cond_alias}"
                    outer_select_exprs.append(current_cond.as_(target_cond_alias))
                    logger.debug(f"Pivot: 添加当期条件指标列: {target_cond_alias}")
        
        # 4. 添加 ratio_metrics（占比指标）列 - 这些列在内层查询已计算，外层需要透传并添加同比
        if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
            for ratio_metric in ir.ratio_metrics:
                ratio_alias = ratio_metric.alias
                if ratio_alias:
                    # 构建 PARTITION BY 和 ORDER BY 表达式
                    partition_by_exprs = [exp.column(a, table="t") for a in partition_dim_aliases]
                    order_expr = exp.column(time_dim_alias, table="t")
                    
                    # 4.1 上期占比列（如"2023年工业用地面积占比"）
                    lag_func = exp.Lag(
                        this=exp.column(ratio_alias, table="t"),
                        offset=exp.Literal.number(lag_offset)
                    )
                    window_kwargs = {
                        "this": lag_func,
                        "order": exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)])
                    }
                    if partition_by_exprs:
                        window_kwargs["partition_by"] = partition_by_exprs
                    window_spec = exp.Window(**window_kwargs)
                    
                    prev_ratio_alias = f"{prev_year}年{ratio_alias}"
                    outer_select_exprs.append(window_spec.as_(prev_ratio_alias))
                    logger.debug(f"Pivot: 添加上期占比列: {prev_ratio_alias}")
                    
                    # 4.2 当期占比列（如"2024年工业用地面积占比"）
                    current_ratio = exp.column(ratio_alias, table="t")
                    target_ratio_alias = f"{target_year}年{ratio_alias}"
                    outer_select_exprs.append(current_ratio.as_(target_ratio_alias))
                    logger.debug(f"Pivot: 添加当期占比列: {target_ratio_alias}")
        
        # 移除内层查询的 ORDER BY 子句（SQL Server 兼容性）
        inner_query = query.copy()
        if hasattr(inner_query, 'args') and 'order' in inner_query.args:
            inner_query.args['order'] = None
        
        # 重要：需要在 SELECT 列表中保留时间维度，用于后续过滤
        # 因为 WHERE 在窗口函数之前执行，所以需要用两层子查询：
        # 1. 第一层：计算 LAG 和增长率（包含所有年份）
        # 2. 第二层：过滤只保留目标年份
        
        # 添加时间维度到 SELECT 列表（用于后续过滤）
        outer_select_exprs_with_time = list(outer_select_exprs)
        outer_select_exprs_with_time.insert(0, exp.column(time_dim_alias, table="t").as_(time_dim_alias))
        
        # 构建中间层查询（包含 LAG 计算，但不过滤年份）
        middle_query = select(*outer_select_exprs_with_time).from_(
            inner_query.subquery(alias="t")
        )
        
        # 构建最外层查询：从中间层选择并过滤年份
        # 这样 LAG 在包含所有年份的数据上计算，然后再过滤
        final_select_exprs = []
        for alias in partition_dim_aliases:
            final_select_exprs.append(exp.column(alias, table="t2"))
        
        # 添加指标相关的列
        for metric_item in ir.metrics:
            # 规范化 metric：提取字段ID
            metric_id, _, _, _ = self._normalize_metric_spec(metric_item)
            
            if metric_id == "__row_count__":
                continue
            
            if isinstance(metric_id, str) and metric_id.startswith('derived:'):
                metric_alias = metric_id[8:]
            else:
                metric_alias = self._get_metric_or_measure_alias(metric_id)
            
            if metric_alias:
                # 上期值列
                prev_year_alias = f"{prev_year}年{metric_alias}"
                final_select_exprs.append(exp.column(prev_year_alias, table="t2"))
                # 当期值列
                target_year_alias = f"{target_year}年{metric_alias}"
                final_select_exprs.append(exp.column(target_year_alias, table="t2"))
                # 增长率列
                growth_alias = f"{metric_alias}_增长率"
                final_select_exprs.append(exp.column(growth_alias, table="t2"))
        
        # 添加 conditional_metrics 列到最外层查询（在 ratio_metrics 之前）
        if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
            for cond_metric in ir.conditional_metrics:
                cond_alias = cond_metric.alias
                if cond_alias:
                    # 上期条件指标列
                    prev_cond_alias = f"{prev_year}年{cond_alias}"
                    final_select_exprs.append(exp.column(prev_cond_alias, table="t2"))
                    # 当期条件指标列
                    target_cond_alias = f"{target_year}年{cond_alias}"
                    final_select_exprs.append(exp.column(target_cond_alias, table="t2"))
                    logger.debug(f"Pivot: 最外层添加条件指标列: {prev_cond_alias}, {target_cond_alias}")
        
        # 添加 ratio_metrics 列到最外层查询
        if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
            for ratio_metric in ir.ratio_metrics:
                ratio_alias = ratio_metric.alias
                if ratio_alias:
                    # 上期占比列
                    prev_ratio_alias = f"{prev_year}年{ratio_alias}"
                    final_select_exprs.append(exp.column(prev_ratio_alias, table="t2"))
                    # 当期占比列
                    target_ratio_alias = f"{target_year}年{ratio_alias}"
                    final_select_exprs.append(exp.column(target_ratio_alias, table="t2"))
                    logger.debug(f"Pivot: 最外层添加占比列: {prev_ratio_alias}, {target_ratio_alias}")
        
        # 构建最外层查询
        outer_query = select(*final_select_exprs).from_(
            middle_query.subquery(alias="t2")
        )
        
        # 在最外层添加 WHERE 过滤只保留目标年份
        # 这样 LAG 已经在包含所有年份的数据上计算完成了
        outer_query = outer_query.where(
            exp.EQ(
                this=exp.column(time_dim_alias, table="t2"),
                expression=exp.Literal.string(str(target_year))
            )
        )
        logger.debug(f"Pivot: 添加最外层 WHERE 过滤，只保留 {target_year} 年数据（LAG 已在完整数据上计算）")
        
        # 添加 ORDER BY（按增长率降序）
        if first_metric_alias:
            outer_query = outer_query.order_by(
                exp.Ordered(this=exp.column(f"{first_metric_alias}_增长率"), desc=True)
            )
        
        if ir.limit:
            outer_query = outer_query.limit(ir.limit)
        
        logger.debug("Pivot 模式同比查询构建完成")
        return outer_query
    
    def _build_vertical_comparison_query(
        self,
        query: exp.Expression,
        ir: IntermediateRepresentation,
        time_dim_alias: str,
        partition_dim_aliases: List[str],
        lag_offset: int
    ) -> exp.Expression:
        """
        构建 Vertical 模式的同比查询（纵向显示，适用于多期同比）
        
        结果格式：年份, 行政区, 用途分类, 楼面地价, 上年楼面地价, 同比增长率
        所有年份都显示，最早年份的增长率为 NULL
        """
        logger.debug("构建 Vertical 模式同比查询")
        
        # 构建外层 SELECT 列表
        outer_select_exprs = []
        
        # 1. 添加时间维度列（保留年份）
        outer_select_exprs.append(exp.column(time_dim_alias, table="t").as_(time_dim_alias))
        
        # 2. 添加非时间维度列（如行政区、用途分类）
        for alias in partition_dim_aliases:
            outer_select_exprs.append(exp.column(alias, table="t").as_(alias))
        
        # 3. 为每个指标添加当期值、上期值和增长率（支持派生指标和原子指标）
        for metric_item in ir.metrics:
            # 规范化 metric：提取字段ID
            metric_id, _, _, _ = self._normalize_metric_spec(metric_item)
            
            if metric_id == "__row_count__":
                continue
            
            # 获取指标别名（在内层查询中的列名）
            if isinstance(metric_id, str) and metric_id.startswith('derived:'):
                metric_alias = metric_id[8:]  # 移除 "derived:" 前缀
            else:
                # 原子指标：使用显示名作为别名
                metric_alias = self._get_metric_or_measure_alias(metric_id)
            
            if not metric_alias:
                logger.warning(f"Vertical: 无法获取指标别名，跳过: {metric_id}")
                continue
            
            # 构建 PARTITION BY 和 ORDER BY 表达式
            partition_by_exprs = [exp.column(a, table="t") for a in partition_dim_aliases]
            order_expr = exp.column(time_dim_alias, table="t")
            
            # 3.1 当期值列
            current_value = exp.column(metric_alias, table="t")
            outer_select_exprs.append(current_value.as_(metric_alias))
            logger.debug(f"Vertical: 添加当期值列: {metric_alias}")
            
            # 3.2 上期值列（使用通用名称"上年"）
            lag_func = exp.Lag(
                this=exp.column(metric_alias, table="t"),
                offset=exp.Literal.number(lag_offset)
            )
            window_kwargs = {
                "this": lag_func,
                "order": exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)])
            }
            if partition_by_exprs:
                window_kwargs["partition_by"] = partition_by_exprs
            window_spec = exp.Window(**window_kwargs)
            
            prev_alias = f"上年{metric_alias}"
            outer_select_exprs.append(window_spec.as_(prev_alias))
            logger.debug(f"Vertical: 添加上期值列: {prev_alias}")
            
            # 3.3 增长率列
            lag_func_for_growth = exp.Lag(
                this=exp.column(metric_alias, table="t"),
                offset=exp.Literal.number(lag_offset)
            )
            window_spec_for_growth = exp.Window(
                this=lag_func_for_growth,
                order=exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)]),
                **({"partition_by": partition_by_exprs} if partition_by_exprs else {})
            )
            
            growth_rate_expr = exp.Case(
                ifs=[
                    exp.If(
                        this=exp.Or(
                            this=exp.Is(this=window_spec_for_growth.copy(), expression=exp.Null()),
                            expression=exp.EQ(this=window_spec_for_growth.copy(), expression=exp.Literal.number(0))
                        ),
                        true=exp.Null()
                    )
                ],
                default=exp.Mul(
                    this=exp.Div(
                        this=exp.Sub(this=current_value.copy(), expression=window_spec_for_growth.copy()),
                        expression=window_spec_for_growth.copy()
                    ),
                    expression=exp.Literal.number(100)
                )
            )
            
            growth_alias = f"{metric_alias}_同比增长率"
            outer_select_exprs.append(growth_rate_expr.as_(growth_alias))
            logger.debug(f"Vertical: 添加增长率列: {growth_alias}")
        
        # 4. 添加 conditional_metrics（条件聚合指标）列 - 这些列在内层查询已计算，外层需要透传
        # 注意：conditional_metrics 应该在 ratio_metrics 之前，因为占比是基于条件聚合结果计算的
        if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
            for cond_metric in ir.conditional_metrics:
                cond_alias = cond_metric.alias
                if cond_alias:
                    # 直接从内层选择条件指标列
                    outer_select_exprs.append(exp.column(cond_alias, table="t").as_(cond_alias))
                    logger.debug(f"Vertical: 添加条件指标列: {cond_alias}")
                    
                    # 可选：为条件指标也添加上年值
                    partition_by_exprs = [exp.column(a, table="t") for a in partition_dim_aliases]
                    order_expr = exp.column(time_dim_alias, table="t")
                    
                    lag_func = exp.Lag(
                        this=exp.column(cond_alias, table="t"),
                        offset=exp.Literal.number(lag_offset)
                    )
                    window_kwargs = {
                        "this": lag_func,
                        "order": exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)])
                    }
                    if partition_by_exprs:
                        window_kwargs["partition_by"] = partition_by_exprs
                    window_spec = exp.Window(**window_kwargs)
                    
                    prev_cond_alias = f"上年{cond_alias}"
                    outer_select_exprs.append(window_spec.as_(prev_cond_alias))
                    logger.debug(f"Vertical: 添加上年条件指标列: {prev_cond_alias}")
        
        # 5. 添加 ratio_metrics（占比指标）列 - 这些列在内层查询已计算，外层需要透传
        if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
            for ratio_metric in ir.ratio_metrics:
                ratio_alias = ratio_metric.alias
                if ratio_alias:
                    # 直接从内层选择占比列（占比本身不需要同比计算，只需透传）
                    outer_select_exprs.append(exp.column(ratio_alias, table="t").as_(ratio_alias))
                    logger.debug(f"Vertical: 添加占比指标列: {ratio_alias}")
                    
                    # 可选：为占比指标也添加同比（占比的变化）
                    # 构建 PARTITION BY 和 ORDER BY 表达式
                    partition_by_exprs = [exp.column(a, table="t") for a in partition_dim_aliases]
                    order_expr = exp.column(time_dim_alias, table="t")
                    
                    # 上年占比
                    lag_func = exp.Lag(
                        this=exp.column(ratio_alias, table="t"),
                        offset=exp.Literal.number(lag_offset)
                    )
                    window_kwargs = {
                        "this": lag_func,
                        "order": exp.Order(expressions=[exp.Ordered(this=order_expr, desc=False)])
                    }
                    if partition_by_exprs:
                        window_kwargs["partition_by"] = partition_by_exprs
                    window_spec = exp.Window(**window_kwargs)
                    
                    prev_ratio_alias = f"上年{ratio_alias}"
                    outer_select_exprs.append(window_spec.as_(prev_ratio_alias))
                    logger.debug(f"Vertical: 添加上年占比列: {prev_ratio_alias}")
        
        # 移除内层查询的 ORDER BY 子句（SQL Server 兼容性）
        inner_query = query.copy()
        if hasattr(inner_query, 'args') and 'order' in inner_query.args:
            inner_query.args['order'] = None
        
        # 构建外层查询（不过滤年份，保留所有年份）
        outer_query = select(*outer_select_exprs).from_(
            inner_query.subquery(alias="t")
        )
        
        # 添加 ORDER BY（按非时间维度和时间维度排序）
        order_by_exprs = []
        for alias in partition_dim_aliases:
            order_by_exprs.append(exp.Ordered(this=exp.column(alias), desc=False))
        order_by_exprs.append(exp.Ordered(this=exp.column(time_dim_alias), desc=False))
        
        if order_by_exprs:
            outer_query = outer_query.order_by(*order_by_exprs)
        
        if ir.limit:
            outer_query = outer_query.limit(ir.limit)
        
        logger.debug("Vertical 模式同比查询构建完成")
        return outer_query

    def _wrap_detail_query_with_having_filters(
        self, 
        query: exp.Expression, 
        ir: IntermediateRepresentation, 
        main_table: str
    ) -> exp.Expression:
        """
        为 detail 查询包装子查询以支持 having_filters（派生指标过滤）
        
        生成 SQL 形如:
        SELECT * FROM (
            <原查询，包含派生指标列>
        ) AS t
        WHERE <派生指标> >= <值>
        
        关键修复：
        1. 派生指标的计算表达式必须添加到内层子查询的 SELECT 中
        2. 这样外层 WHERE 才能引用 t.[派生指标名]
        
        Args:
            query: 原始查询表达式
            ir: 中间表示
            main_table: 主表ID
            
        Returns:
            包装后的查询表达式
        """
        if not ir.having_filters:
            return query
        
        logger.debug(f"开始包装 detail 查询以支持 {len(ir.having_filters)} 个 having_filters")
        
        # 获取表别名
        table_alias = self._get_table_alias(main_table)
        
        # 构建外层 WHERE 条件，并收集需要添加到子查询的派生指标
        outer_conditions = []
        derived_metric_exprs = []  # (sql_expr, alias) 列表
        
        for having_filter in ir.having_filters:
            field_id = having_filter.field
            op = having_filter.op
            value = having_filter.value
            
            # 获取字段别名（在子查询中的列名）
            # 对于派生指标，使用其显示名作为别名
            field_alias = None
            
            # 检查是否为派生指标
            is_derived, derived_name, derived_def = self._is_derived_metric(field_id)
            if is_derived and derived_name:
                field_alias = derived_name
                logger.debug(f"having_filter 使用派生指标别名: {derived_name}")
                
                # 关键修复：为派生指标构建计算表达式，添加到子查询 SELECT 中
                if derived_def:
                    formula_sql, alias = self._build_derived_metric_for_detail(
                        derived_name, derived_def, table_alias
                    )
                    if formula_sql:
                        derived_metric_exprs.append((formula_sql, alias or derived_name))
                        logger.debug(f"派生指标 {derived_name} 计算表达式: {formula_sql}")
                    else:
                        logger.warning(f"无法构建派生指标 {derived_name} 的计算表达式，跳过此过滤条件")
                        continue
            elif field_id in self.model.measures:
                measure = self.model.measures[field_id]
                field_alias = measure.display_name if hasattr(measure, 'display_name') else measure.column
            elif field_id in self.model.dimensions:
                dim = self.model.dimensions[field_id]
                field_alias = self._get_dimension_alias(field_id)
            else:
                # 尝试使用字段名本身
                field_alias = field_id
                logger.warning(f"having_filter 未知字段类型，使用原始 field_id: {field_id}")
            
            if not field_alias:
                logger.warning(f"无法确定 having_filter 字段别名: {field_id}")
                continue
            
            # 构建列引用（引用子查询中的别名）
            col_expr = exp.column(field_alias, table="t")
            
            # 构建值表达式
            value_expr = self._value_to_expression(value)
            
            # 构建比较条件
            if op == "=":
                cond = exp.EQ(this=col_expr, expression=value_expr)
            elif op == "!=":
                cond = exp.NEQ(this=col_expr, expression=value_expr)
            elif op == ">":
                cond = exp.GT(this=col_expr, expression=value_expr)
            elif op == ">=":
                cond = exp.GTE(this=col_expr, expression=value_expr)
            elif op == "<":
                cond = exp.LT(this=col_expr, expression=value_expr)
            elif op == "<=":
                cond = exp.LTE(this=col_expr, expression=value_expr)
            else:
                logger.warning(f"detail having_filter 不支持的操作符: {op}")
                continue
            
            outer_conditions.append(cond)
            logger.debug(f"添加 detail having_filter: {field_alias} {op} {value}")
        
        if not outer_conditions:
            logger.warning("没有有效的 having_filter 条件，返回原查询")
            return query
        
        # 关键修复：将派生指标的计算表达式添加到子查询的 SELECT 列表中
        if derived_metric_exprs:
            # 获取当前 SELECT 表达式列表
            current_select = query.args.get("expressions", [])
            
            for formula_sql, alias in derived_metric_exprs:
                try:
                    # 解析派生指标计算表达式
                    formula_expr = parse_one(formula_sql, dialect=self.dialect)
                    # 创建别名表达式
                    alias_expr = exp.Alias(
                        this=formula_expr,
                        alias=exp.to_identifier(alias)
                    )
                    # 添加到 SELECT 列表（避免重复）
                    alias_exists = any(
                        isinstance(e, exp.Alias) and e.alias == alias 
                        for e in current_select
                    )
                    if not alias_exists:
                        current_select.append(alias_expr)
                        logger.debug(f"添加派生指标到子查询 SELECT: {alias}")
                except Exception as e:
                    logger.warning(f"解析派生指标表达式失败: {formula_sql}, error={e}")
            
            # 更新 SELECT 表达式
            query.set("expressions", current_select)
        
        # SQL Server 限制：子查询（派生表）中不能有 ORDER BY，除非配合 TOP/OFFSET
        # 解决方案：将 ORDER BY 从子查询移到外层查询
        inner_order = query.args.get("order")
        if inner_order:
            # 保存 ORDER BY 表达式，用于后续添加到外层
            # 需要重写列引用，使用子查询别名 "t"
            logger.debug("检测到子查询中有 ORDER BY，将移到外层查询")
            query.set("order", None)  # 从子查询中移除 ORDER BY
        
        # 构建外层查询：SELECT * FROM (subquery) AS t WHERE conditions
        subquery_alias = query.subquery(alias="t")
        outer_query = select(exp.Star()).from_(subquery_alias)
        
        # 添加 WHERE 条件
        if len(outer_conditions) == 1:
            outer_query = outer_query.where(outer_conditions[0])
        else:
            outer_query = outer_query.where(exp.and_(*outer_conditions))
        
        # 如果有 ORDER BY，添加到外层查询
        # 对于派生指标排序，使用外层别名引用（t.派生指标名）
        if inner_order:
            # 遍历 ORDER BY 表达式，将列引用转换为使用子查询别名
            new_order_exprs = []
            for ordered in inner_order.expressions:
                # ordered 是 exp.Ordered 对象，包含 this（列表达式）和 desc 属性
                order_col = ordered.this
                is_desc = ordered.args.get("desc", False)
                
                # 检查是否为派生指标的表达式（复杂计算）
                # 如果是复杂表达式，尝试匹配对应的派生指标别名
                matched_alias = None
                for formula_sql, alias in derived_metric_exprs:
                    # 比较 SQL 表达式
                    try:
                        order_sql = order_col.sql(dialect=self.dialect)
                        if order_sql.replace(" ", "") == formula_sql.replace(" ", ""):
                            matched_alias = alias
                            break
                    except:
                        pass
                
                if matched_alias:
                    # 使用派生指标别名
                    new_col = exp.column(matched_alias, table="t")
                    logger.debug(f"ORDER BY 派生指标映射: {matched_alias}")
                else:
                    # 对于普通列，尝试提取别名或列名
                    if isinstance(order_col, exp.Column):
                        col_name = order_col.name
                        new_col = exp.column(col_name, table="t")
                    else:
                        # 复杂表达式，保持原样（可能无法正确引用）
                        new_col = order_col
                        logger.warning(f"ORDER BY 复杂表达式无法映射到子查询别名: {order_col}")
                
                # 创建新的 Ordered 表达式
                new_ordered = exp.Ordered(this=new_col, desc=is_desc)
                new_order_exprs.append(new_ordered)
            
            if new_order_exprs:
                outer_query = outer_query.order_by(*new_order_exprs)
                logger.debug(f"已将 ORDER BY 移到外层查询")
        
        logger.debug(f"detail 查询已包装子查询，添加 {len(outer_conditions)} 个外层过滤条件")
        return outer_query

    def _resolve_field_to_column_expr(self, field_id: str) -> Optional[exp.Expression]:
        """
        将字段ID解析为带表别名的列表达式
        
        Args:
            field_id: 字段ID
        
        Returns:
            列表达式，或 None
        """
        if field_id in self.model.fields:
            field = self.model.fields[field_id]
            table_alias = self._get_table_alias(field.datasource_id)
            col_name = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None) or field.field_name
            return exp.column(col_name, table=table_alias)
        elif field_id in self.model.dimensions:
            dim = self.model.dimensions[field_id]
            table_alias = self._get_table_alias(dim.table)
            return exp.column(dim.column, table=table_alias)
        elif field_id in self.model.measures:
            measure = self.model.measures[field_id]
            table_alias = self._get_table_alias(measure.table)
            return exp.column(measure.column, table=table_alias)
        return None

    def _get_field_column_sql(self, field_id: str) -> Optional[str]:
        """
        获取字段的 SQL 表达式字符串（带表别名）
        
        Args:
            field_id: 字段ID
        
        Returns:
            SQL 字符串，如 "t1.column_name"
        """
        col_expr = self._resolve_field_to_column_expr(field_id)
        if col_expr:
            return col_expr.sql(dialect=self.dialect)
        return None

    def _build_filter_condition_expr(
        self,
        col_expr: exp.Expression,
        op: str,
        value: Any
    ) -> exp.Expression:
        """
        构建过滤条件表达式（用于 CASE WHEN）
        
        Args:
            col_expr: 列表达式
            op: 操作符
            value: 值
        
        Returns:
            条件表达式
        """
        value_expr = self._value_to_expression(value)
        
        if op == "=":
            return exp.EQ(this=col_expr, expression=value_expr)
        elif op == "!=":
            return exp.NEQ(this=col_expr, expression=value_expr)
        elif op == ">":
            return exp.GT(this=col_expr, expression=value_expr)
        elif op == ">=":
            return exp.GTE(this=col_expr, expression=value_expr)
        elif op == "<":
            return exp.LT(this=col_expr, expression=value_expr)
        elif op == "<=":
            return exp.LTE(this=col_expr, expression=value_expr)
        elif op == "IN":
            if isinstance(value, list):
                values = [self._value_to_expression(v) for v in value]
                return exp.In(this=col_expr, expressions=values)
            return exp.EQ(this=col_expr, expression=value_expr)
        elif op == "NOT IN":
            if isinstance(value, list):
                values = [self._value_to_expression(v) for v in value]
                return exp.Not(this=exp.In(this=col_expr, expressions=values))
            return exp.NEQ(this=col_expr, expression=value_expr)
        elif op == "LIKE":
            return exp.Like(this=col_expr, expression=value_expr)
        elif op == "IS NULL":
            return exp.Is(this=col_expr, expression=exp.Null())
        elif op == "IS NOT NULL":
            return exp.Not(this=exp.Is(this=col_expr, expression=exp.Null()))
        else:
            return exp.EQ(this=col_expr, expression=value_expr)

    def _value_to_expression(self, value: Any) -> exp.Expression:
        """
        将Python值转换为SQLGlot表达式
        
        Args:
            value: Python值
        
        Returns:
            SQLGlot表达式
        """
        if value is None:
            return exp.Null()
        elif isinstance(value, bool):
            return exp.Boolean(this=value)
        elif isinstance(value, int):
            return exp.Literal.number(value)
        elif isinstance(value, float):
            return exp.Literal.number(value)
        elif isinstance(value, str):
            return exp.Literal.string(value)
        else:
            return exp.Literal.string(str(value))

