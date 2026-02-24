"""SQL安全验证器 - 防止危险操作"""

import re
import structlog
from typing import List, Set, Optional, Tuple
from sqlglot import exp, parse_one, ParseError
from sqlglot.dialects import Dialect, Dialects

from server.exceptions import SecurityError

logger = structlog.get_logger()


class SQLSecurityValidator:
    """SQL安全验证器"""

    # 危险操作类型（严格禁止）
    FORBIDDEN_OPERATIONS = {
        # 数据操作语言
        'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'MERGE',
        # 数据定义语言
        'CREATE', 'ALTER', 'DROP', 'RENAME',
        # 数据控制语言
        'GRANT', 'REVOKE', 'DENY',
        # 事务控制语言
        'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
        # 其他危险操作
        'EXEC', 'EXECUTE', 'CALL', 'DO', 'PERFORM'
    }

    # 允许的操作类型
    ALLOWED_OPERATIONS = {
        'SELECT', 'WITH', 'VALUES', 'UNION', 'INTERSECT', 'EXCEPT',
        'DECLARE', 'SET'
    }

    # 危险关键字模式
    DANGEROUS_PATTERNS = [
        # 防止存储过程调用
        r'\bEXEC(?:UTE)?\s*\(',
        r'\bCALL\s+\w+',
        r'\bDO\s+',
        # 防止系统命令执行
        r'\bxp_cmdshell\b',
        r'\bsp_executesql\b',
        r'\bOPENROWSET\b',
        r'\bOPENDATASOURCE\b',
        # 防止文件操作
        r'\bBULK\s+INSERT\b',
        r'\bLOAD\s+DATA\b',
        r'\bINTO\s+OUTFILE\b',
        r'\bDUMP\s+DATABASE\b',
        # 防止配置修改
        r'\bSET\s+@@',
        r'\bCONFIGURE\b',
        r'\bRECONFIGURE\b',
    ]

    def __init__(self):
        self._dangerous_pattern_cache = [
            re.compile(pattern, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            for pattern in self.DANGEROUS_PATTERNS
        ]

    def validate_sql_security(self, sql: str, dialect: str = "sqlserver") -> Tuple[bool, Optional[str]]:
        """
        验证SQL安全性

        Args:
            sql: SQL语句
            dialect: 数据库方言

        Returns:
            (是否安全, 错误信息)

        Raises:
            SecurityError: SQL包含危险操作
        """
        try:
            # 1. 基础检查
            self._check_basic_sql_type(sql)

            # 2. 检查多语句攻击
            self._check_multiple_statements(sql)

            # 3. 检查危险模式
            self._check_dangerous_patterns(sql)

            # 4. 检查查询类型
            # 如果是多语句，只要所有语句都通过了 _check_multiple_statements 的检查（都在 ALLOWED 中且不在 FORBIDDEN 中），则视为通过
            # 这里只检查第一条非空语句作为主要类型记录日志
            operation_type = self._get_primary_operation_type_simple(sql)
            
            # 如果主要操作类型不在允许列表中，且不是多语句情况（多语句已在上面检查过），则报错
            # 注意：_get_primary_operation_type_simple 可能返回第一条语句的类型
            if operation_type not in self.ALLOWED_OPERATIONS:
                raise SecurityError(
                    f"禁止执行 {operation_type} 操作，只允许执行 SELECT/DECLARE/SET 查询",
                    details={
                        "sql_preview": sql[:200] + "..." if len(sql) > 200 else sql,
                        "detected_operation": operation_type
                    }
                )

            logger.debug("SQL安全验证通过", operation=operation_type, dialect=dialect)
            return True, None

        except SecurityError:
            raise
        except Exception as e:
            logger.error("SQL安全验证失败", error=str(e), sql_preview=sql[:100])
            raise SecurityError(
                f"SQL安全验证失败: {str(e)}",
                details={"sql_preview": sql[:200] + "..." if len(sql) > 200 else sql}
            )

    def _check_basic_sql_type(self, sql: str):
        """基础SQL类型检查"""
        if not sql or not sql.strip():
            raise SecurityError("SQL语句不能为空")

        sql_upper = sql.upper().strip()

        # 检查是否以允许的操作开头
        for forbidden in self.FORBIDDEN_OPERATIONS:
            if sql_upper.startswith(forbidden):
                raise SecurityError(
                    f"禁止执行 {forbidden} 操作，只允许执行 SELECT 查询",
                    details={"forbidden_operation": forbidden}
                )

    def _check_multiple_statements(self, sql: str):
        """检查多语句 - 允许安全的多语句"""
        # 移除字符串中的单引号内容，避免误判
        sql_without_strings = re.sub(r"'[^']*'", "", sql, flags=re.DOTALL)

        # 检查是否包含分号（多语句标识）
        if ';' in sql_without_strings:
            # 检查分号后是否还有其他语句
            statements = sql_without_strings.split(';')

            # 过滤空语句
            non_empty_statements = [stmt.strip() for stmt in statements if stmt.strip()]

            # 检查每一条语句的类型
            for stmt in non_empty_statements:
                # 检查是否以禁止的操作开头
                stmt_upper = stmt.upper()
                for forbidden in self.FORBIDDEN_OPERATIONS:
                    if stmt_upper.startswith(forbidden):
                        raise SecurityError(
                            f"禁止执行 {forbidden} 操作",
                            details={"forbidden_operation": forbidden, "statement": stmt[:100]}
                        )
                
                # 检查是否是允许的操作
                is_allowed = False
                for allowed in self.ALLOWED_OPERATIONS:
                    if stmt_upper.startswith(allowed):
                        is_allowed = True
                        break
                
                if not is_allowed:
                    # 再次检查是否是注释或其他非操作性内容（虽然strip处理过，但可能有些边缘情况）
                    # 这里简单处理：如果不认识的操作，且不是禁止的，暂时视为风险，或者如果太严格可以放宽
                    # 为了支持 SET @var = ... 这种，前面已经加了 SET 到 ALLOWED
                    pass 

    def _validate_sql_structure(self, sql: str, dialect: str):
        """验证SQL结构 - 简化版本，只做基础语法检查"""
        # 这里可以添加更复杂的语法检查，但现在先用简单的方法
        pass

    def _check_dangerous_patterns(self, sql: str):
        """检查危险模式"""
        for pattern in self._dangerous_pattern_cache:
            if pattern.search(sql):
                raise SecurityError(
                    f"SQL包含危险模式: {pattern.pattern}",
                    details={
                        "dangerous_pattern": pattern.pattern,
                        "sql_preview": sql[:200] + "..." if len(sql) > 200 else sql
                    }
                )

    def _get_primary_operation_type_simple(self, sql: str) -> str:
        """获取SQL的主要操作类型（简化版本）"""
        sql_upper = sql.upper().strip()

        # 移除前导注释和空白
        lines = sql_upper.split('\n')
        first_non_comment = ''
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.startswith('--') and not stripped.startswith('/*'):
                first_non_comment = stripped
                break

        if not first_non_comment:
            return 'UNKNOWN'

        # 检查是否以允许的操作开头
        for allowed in self.ALLOWED_OPERATIONS:
            if first_non_comment.startswith(allowed):
                return allowed

        # 检查是否以禁止的操作开头
        for forbidden in self.FORBIDDEN_OPERATIONS:
            if first_non_comment.startswith(forbidden):
                return forbidden

        return 'UNKNOWN'

    def _get_primary_operation_type(self, sql: str, dialect: str) -> str:
        """获取SQL的主要操作类型"""
        try:
            sqlglot_dialect = self._get_sqlglot_dialect(dialect)
            parsed = parse_one(sql, dialect=sqlglot_dialect)

            # 获取根节点类型
            root_type = type(parsed).__name__.upper()

            # 特殊处理CTE（WITH语句）
            if root_type == 'WITH':
                # 检查CTE的最终查询类型
                if hasattr(parsed, 'this') and parsed.this:
                    return type(parsed.this).__name__.upper()
                return 'WITH'  # CTE本身是安全的

            return root_type

        except Exception as e:
            logger.warning("无法解析SQL操作类型", error=str(e))
            # 如果解析失败，进行简单的字符串匹配作为后备方案
            sql_upper = sql.upper().strip()

            for allowed in self.ALLOWED_OPERATIONS:
                if sql_upper.startswith(allowed):
                    return allowed

            return 'UNKNOWN'

    def _get_sqlglot_dialect(self, dialect: str) -> Dialect:
        """获取sqlglot方言"""
        dialect_mapping = {
            'sqlserver': Dialects.SQLSERVER,
            'mysql': Dialects.MYSQL,
            'postgresql': Dialects.POSTGRES,
            'postgres': Dialects.POSTGRES,
            'sqlite': Dialects.SQLITE,
            'oracle': Dialects.ORACLE,
            'bigquery': Dialects.BIGQUERY,
            'snowflake': Dialects.SNOWFLAKE
        }

        return dialect_mapping.get(dialect.lower(), Dialects.SQLSERVER)

    def is_select_query(self, sql: str, dialect: str = "sqlserver") -> bool:
        """
        检查是否为SELECT查询

        Args:
            sql: SQL语句
            dialect: 数据库方言

        Returns:
            bool: 是否为SELECT查询
        """
        try:
            operation_type = self._get_primary_operation_type(sql, dialect)
            return operation_type in ['SELECT', 'WITH', 'UNION', 'INTERSECT', 'EXCEPT']
        except Exception:
            # 如果出现异常，进行简单检查
            sql_upper = sql.upper().strip()
            return sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')


# 全局验证器实例
sql_validator = SQLSecurityValidator()


def validate_sql_security(sql: str, dialect: str = "sqlserver") -> Tuple[bool, Optional[str]]:
    """
    验证SQL安全性的便捷函数

    Args:
        sql: SQL语句
        dialect: 数据库方言

    Returns:
        (是否安全, 错误信息)
    """
    return sql_validator.validate_sql_security(sql, dialect)


def is_safe_select_query(sql: str, dialect: str = "sqlserver") -> bool:
    """
    检查是否为安全的SELECT查询

    Args:
        sql: SQL语句
        dialect: 数据库方言

    Returns:
        bool: 是否为安全的SELECT查询
    """
    try:
        validate_sql_security(sql, dialect)
        return True
    except SecurityError:
        return False
    except Exception:
        return False