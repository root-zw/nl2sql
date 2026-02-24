"""
SQL 后处理器 - 混合架构核心组件

对 LLM 直接生成的 SQL 进行安全检查、权限注入和格式规范化。
确保直接生成的 SQL 也能享受 IR 流程的安全保障。
"""

import re
from typing import Dict, Any, List, Optional, Set, Tuple
import structlog
from sqlglot import parse_one, exp
from sqlglot.errors import ParseError
from dataclasses import dataclass, field

from server.exceptions import SecurityError, CompilationError

logger = structlog.get_logger()


@dataclass
class SQLValidationResult:
    """SQL 验证结果"""
    is_valid: bool
    sql: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    tables_used: Set[str] = field(default_factory=set)
    columns_used: Set[str] = field(default_factory=set)
    has_dangerous_operations: bool = False
    applied_filters: List[Dict[str, Any]] = field(default_factory=list)
    applied_limit: Optional[int] = None


class SQLPostProcessor:
    """
    SQL 后处理器
    
    功能：
    1. 安全检查：防止危险操作（DELETE, UPDATE, DROP, TRUNCATE等）
    2. 表/列白名单验证：只允许访问已注册的表和列
    3. 权限过滤注入：自动添加行级权限过滤条件
    4. 默认过滤注入：添加数据质量规则、全局默认过滤
    5. LIMIT 保护：确保大表查询有结果限制
    6. SQL 格式规范化：统一方言和格式
    """
    
    # 危险的 SQL 操作关键字
    DANGEROUS_KEYWORDS = {
        'DELETE', 'UPDATE', 'INSERT', 'DROP', 'TRUNCATE', 'ALTER', 
        'CREATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL',
        'XP_', 'SP_', 'SHUTDOWN', 'BACKUP', 'RESTORE'
    }
    
    # 危险的函数
    DANGEROUS_FUNCTIONS = {
        'OPENROWSET', 'OPENQUERY', 'OPENDATASOURCE', 'BULK',
        'XP_CMDSHELL', 'SP_EXECUTESQL', 'DBCC'
    }
    
    def __init__(
        self,
        dialect: str = "tsql",
        allowed_tables: Optional[Dict[str, Set[str]]] = None,
        allowed_columns: Optional[Dict[str, Set[str]]] = None,
        default_limit: int = 1000,
        max_limit: int = 10000
    ):
        """
        初始化 SQL 后处理器
        
        Args:
            dialect: SQL 方言（tsql, mysql, postgresql 等）
            allowed_tables: 允许访问的表白名单 {schema.table_name: {column1, column2, ...}}
            allowed_columns: 按表分组的允许列 {table_name: {column1, column2, ...}}
            default_limit: 默认结果限制
            max_limit: 最大结果限制
        """
        self.dialect = dialect
        self.allowed_tables = allowed_tables or {}
        self.allowed_columns = allowed_columns or {}
        self.default_limit = default_limit
        self.max_limit = max_limit
        
    def process(
        self,
        sql: str,
        user_context: Optional[Dict[str, Any]] = None,
        row_level_filters: Optional[List[Dict[str, Any]]] = None,
        default_filters: Optional[List[Dict[str, Any]]] = None,
        skip_table_validation: bool = False
    ) -> SQLValidationResult:
        """
        处理 SQL，进行安全检查和权限注入
        
        Args:
            sql: 原始 SQL
            user_context: 用户上下文 {user_id, role, tenant_id, ...}
            row_level_filters: 行级权限过滤条件列表
            default_filters: 默认过滤条件列表
            skip_table_validation: 是否跳过表白名单验证（开发模式）
        
        Returns:
            SQLValidationResult 对象
        """
        result = SQLValidationResult(is_valid=True, sql=sql)
        
        try:
            # 1. 基础安全检查
            self._check_dangerous_operations(sql, result)
            if not result.is_valid:
                return result
            
            # 2. 解析 SQL
            try:
                ast = parse_one(sql, dialect=self.dialect)
            except ParseError as e:
                result.is_valid = False
                result.errors.append(f"SQL 解析失败: {str(e)}")
                return result
            
            # 3. 提取使用的表和列
            self._extract_tables_and_columns(ast, result)
            
            # 4. 表白名单验证（如果启用）
            if not skip_table_validation and self.allowed_tables:
                self._validate_tables(result)
                if not result.is_valid:
                    return result
            
            # 5. 列白名单验证（如果启用）
            if not skip_table_validation and self.allowed_columns:
                self._validate_columns(result)
                if not result.is_valid:
                    return result
            
            # 6. 注入行级权限过滤
            if row_level_filters:
                ast = self._inject_row_level_filters(ast, row_level_filters, result)
            
            # 7. 注入默认过滤条件
            if default_filters:
                ast = self._inject_default_filters(ast, default_filters, result)
            
            # 8. 确保有 LIMIT 保护
            ast = self._ensure_limit(ast, result)
            
            # 9. 生成处理后的 SQL
            result.sql = ast.sql(dialect=self.dialect, pretty=True)
            # 10. 方言后处理（与 IR 编译器保持一致）
            result.sql = self._apply_dialect_postprocess(result.sql)
            
        except Exception as e:
            logger.error("SQL 后处理失败", error=str(e), sql=sql[:200])
            result.is_valid = False
            result.errors.append(f"SQL 后处理失败: {str(e)}")
        
        return result

    def _apply_dialect_postprocess(self, sql: str) -> str:
        """
        对 SQL 做方言级字符串后处理。

        说明：
        - 直接 SQL 流程会 parse→rewrite→sqlglot 输出，这一步可能引入方言不兼容写法
          （例如 MySQL 的 ROLLUP 语法）。
        - IR 编译器路径已有同名后处理（Compiler.compile -> dialect_handler.add_unicode_prefix）。
          这里保持一致，避免 direct_sql 与 ir_compiler 行为差异。
        """
        if not sql:
            return sql
        try:
            if self.dialect == "mysql":
                from server.compiler.dialect_mysql import MySQLDialect
                return MySQLDialect().add_unicode_prefix(sql)
            if self.dialect in ("tsql", "mssql", "sqlserver"):
                from server.compiler.dialect_tsql import TSQLDialect
                return TSQLDialect().add_unicode_prefix(sql)
            if self.dialect in ("postgres", "postgresql"):
                from server.compiler.dialect_postgres import PostgreSQLDialect
                return PostgreSQLDialect().add_unicode_prefix(sql)
        except Exception as e:
            logger.warning("SQL 方言后处理失败（已忽略）", error=str(e), dialect=self.dialect)
        return sql
    
    def _check_dangerous_operations(self, sql: str, result: SQLValidationResult) -> None:
        """检查危险操作"""
        sql_upper = sql.upper()
        
        # 检查危险关键字
        for keyword in self.DANGEROUS_KEYWORDS:
            # 使用正则确保是独立关键字，而非字符串的一部分
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, sql_upper):
                result.is_valid = False
                result.has_dangerous_operations = True
                result.errors.append(f"检测到危险操作: {keyword}")
                logger.warning("检测到危险 SQL 操作", keyword=keyword, sql=sql[:100])
        
        # 检查危险函数
        for func in self.DANGEROUS_FUNCTIONS:
            if func in sql_upper:
                result.is_valid = False
                result.has_dangerous_operations = True
                result.errors.append(f"检测到危险函数: {func}")
                logger.warning("检测到危险 SQL 函数", function=func, sql=sql[:100])
        
        # 检查注释注入尝试
        if '--' in sql or '/*' in sql:
            # 允许正常的注释，但需要记录警告
            result.warnings.append("SQL 中包含注释，请确认非注入攻击")
        
        # 检查多语句（分号分隔）
        # 简单的分号检测可能有误报，这里只做警告
        if sql.count(';') > 1:
            result.warnings.append("SQL 中包含多个分号，可能存在多语句注入风险")
    
    def _extract_tables_and_columns(self, ast: exp.Expression, result: SQLValidationResult) -> None:
        """提取 SQL 中使用的表和列"""
        # 提取表
        for table in ast.find_all(exp.Table):
            table_name = table.name
            schema_name = table.db if hasattr(table, 'db') else None
            if schema_name:
                result.tables_used.add(f"{schema_name}.{table_name}")
            result.tables_used.add(table_name)
        
        # 提取列
        for column in ast.find_all(exp.Column):
            col_name = column.name
            result.columns_used.add(col_name)
    
    def _validate_tables(self, result: SQLValidationResult) -> None:
        """验证表是否在白名单中"""
        for table in result.tables_used:
            if table not in self.allowed_tables:
                result.is_valid = False
                result.errors.append(f"表 '{table}' 不在允许访问的范围内")
    
    def _validate_columns(self, result: SQLValidationResult) -> None:
        """验证列是否在白名单中"""
        # 收集所有允许的列
        all_allowed_columns = set()
        for cols in self.allowed_columns.values():
            all_allowed_columns.update(cols)
        
        # 如果白名单为空，跳过验证
        if not all_allowed_columns:
            return
        
        for column in result.columns_used:
            # 跳过 * 和常见的系统列
            if column in ('*', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'):
                continue
            if column not in all_allowed_columns:
                result.warnings.append(f"列 '{column}' 可能不在允许访问的范围内")
    
    def _inject_row_level_filters(
        self,
        ast: exp.Expression,
        row_level_filters: List[Dict[str, Any]],
        result: SQLValidationResult
    ) -> exp.Expression:
        """
        注入行级权限过滤条件
        
        Args:
            ast: SQL AST
            row_level_filters: 行级过滤条件列表，格式如:
                [{"table": "orders", "column": "tenant_id", "op": "=", "value": "T001"}]
            result: 验证结果对象
        
        Returns:
            修改后的 AST
        """
        if not isinstance(ast, exp.Select):
            return ast
        
        for filter_def in row_level_filters:
            table_name = filter_def.get('table')
            column_name = filter_def.get('column')
            op = filter_def.get('op', '=')
            value = filter_def.get('value')
            
            if not column_name or value is None:
                continue
            
            # 构建过滤条件
            col_expr = exp.Column(this=exp.Identifier(this=column_name))
            
            if isinstance(value, (list, tuple)):
                # IN 操作
                values = [self._to_literal(v) for v in value]
                filter_expr = exp.In(this=col_expr, expressions=values)
            else:
                # 单值比较
                value_expr = self._to_literal(value)
                if op == '=':
                    filter_expr = exp.EQ(this=col_expr, expression=value_expr)
                elif op == '!=':
                    filter_expr = exp.NEQ(this=col_expr, expression=value_expr)
                elif op == '>':
                    filter_expr = exp.GT(this=col_expr, expression=value_expr)
                elif op == '>=':
                    filter_expr = exp.GTE(this=col_expr, expression=value_expr)
                elif op == '<':
                    filter_expr = exp.LT(this=col_expr, expression=value_expr)
                elif op == '<=':
                    filter_expr = exp.LTE(this=col_expr, expression=value_expr)
                else:
                    filter_expr = exp.EQ(this=col_expr, expression=value_expr)
            
            # 添加到 WHERE 子句
            if ast.args.get('where'):
                # 已有 WHERE，用 AND 连接
                existing_where = ast.args['where'].this
                new_where = exp.And(this=existing_where, expression=filter_expr)
                ast.args['where'] = exp.Where(this=new_where)
            else:
                # 新建 WHERE
                ast.args['where'] = exp.Where(this=filter_expr)
            
            result.applied_filters.append({
                'type': 'row_level',
                'column': column_name,
                'op': op,
                'value': value
            })
            logger.debug(f"注入行级权限过滤: {column_name} {op} {value}")
        
        return ast
    
    def _inject_default_filters(
        self,
        ast: exp.Expression,
        default_filters: List[Dict[str, Any]],
        result: SQLValidationResult
    ) -> exp.Expression:
        """注入默认过滤条件（数据质量规则等）"""
        # 复用行级过滤的逻辑
        for filter_def in default_filters:
            filter_def_with_type = filter_def.copy()
            if 'type' not in filter_def_with_type:
                filter_def_with_type['type'] = 'default'
        
        return self._inject_row_level_filters(ast, default_filters, result)
    
    def _ensure_limit(self, ast: exp.Expression, result: SQLValidationResult) -> exp.Expression:
        """确保查询有 LIMIT 保护"""
        if not isinstance(ast, exp.Select):
            return ast
        
        # 检查是否已有 LIMIT
        existing_limit = ast.args.get('limit')
        
        if existing_limit:
            # 检查现有 LIMIT 是否超过最大值
            try:
                limit_node = existing_limit
                if hasattr(limit_node, 'this'):
                    limit_value = int(limit_node.this.this) if hasattr(limit_node.this, 'this') else self.max_limit
                    if limit_value > self.max_limit:
                        # 覆盖为最大值
                        ast.args['limit'] = exp.Limit(this=exp.Literal.number(self.max_limit))
                        result.applied_limit = self.max_limit
                        result.warnings.append(f"LIMIT 值 {limit_value} 超过最大限制 {self.max_limit}，已调整")
                    else:
                        result.applied_limit = limit_value
            except (ValueError, AttributeError):
                pass
        else:
            # 检查是否是聚合查询（可能不需要 LIMIT）
            has_group_by = ast.args.get('group')
            has_aggregation = any(
                isinstance(node, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max))
                for node in ast.walk()
            )
            
            # 如果有 GROUP BY 但结果可能很多，也添加 LIMIT
            if not has_aggregation or has_group_by:
                ast.args['limit'] = exp.Limit(this=exp.Literal.number(self.default_limit))
                result.applied_limit = self.default_limit
                result.warnings.append(f"已添加默认 LIMIT {self.default_limit} 保护")
        
        return ast
    
    def _to_literal(self, value: Any) -> exp.Expression:
        """将 Python 值转换为 SQLGlot 字面量"""
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

    @classmethod
    def from_semantic_model(cls, model, dialect: str = "tsql") -> "SQLPostProcessor":
        """
        从语义模型创建 SQL 后处理器
        
        自动提取允许的表和列白名单
        
        Args:
            model: SemanticModel 对象
            dialect: SQL 方言
        
        Returns:
            SQLPostProcessor 实例
        """
        allowed_tables = {}
        allowed_columns = {}
        
        # 从 sources 提取表信息
        if hasattr(model, 'sources') and model.sources:
            for source_id, source in model.sources.items():
                table_name = getattr(source, 'table_name', source_id)
                schema_name = getattr(source, 'schema_name', None)
                
                full_name = f"{schema_name}.{table_name}" if schema_name else table_name
                allowed_tables[full_name] = set()
                allowed_tables[table_name] = set()
        
        # 从 fields/measures/dimensions 提取列信息
        for attr_name in ('fields', 'measures', 'dimensions'):
            fields_dict = getattr(model, attr_name, {})
            if isinstance(fields_dict, dict):
                for field_id, field_obj in fields_dict.items():
                    col_name = getattr(field_obj, 'column', None) or getattr(field_obj, 'field_name', None)
                    table_id = getattr(field_obj, 'datasource_id', None) or getattr(field_obj, 'table', None)
                    
                    if col_name:
                        if table_id not in allowed_columns:
                            allowed_columns[table_id] = set()
                        allowed_columns[table_id].add(col_name)
        
        return cls(
            dialect=dialect,
            allowed_tables=allowed_tables,
            allowed_columns=allowed_columns
        )

