"""MySQL 方言处理"""

import structlog
from typing import Dict, Any

logger = structlog.get_logger()


class MySQLDialect:
    """MySQL 方言处理器"""
    
    def __init__(self):
        self.timezone_mapping = self._build_timezone_mapping()
    
    def _build_timezone_mapping(self) -> Dict[str, str]:
        """
        IANA 时区映射
        MySQL 直接支持 IANA 时区名称
        """
        return {
            "Asia/Shanghai": "Asia/Shanghai",
            "America/New_York": "America/New_York",
            "America/Los_Angeles": "America/Los_Angeles",
            "Europe/London": "Europe/London",
            "UTC": "UTC"
        }
    
    def convert_timezone(self, iana_tz: str) -> str:
        """
        MySQL 直接支持 IANA 时区
        
        Args:
            iana_tz: IANA 时区名称（如 Asia/Shanghai）
        
        Returns:
            IANA 时区名称
        """
        return self.timezone_mapping.get(iana_tz, "UTC")
    
    def date_trunc_expression(self, grain: str, field: str) -> str:
        """
        生成 MySQL 日期截断表达式
        
        Args:
            grain: 时间粒度 (year/quarter/month/week/day/hour)
            field: 字段名
        
        Returns:
            MySQL DATE_FORMAT 表达式
        """
        # MySQL 使用 DATE_FORMAT
        format_map = {
            "year": f"DATE_FORMAT({field}, '%Y-01-01')",
            "quarter": f"CONCAT(YEAR({field}), '-', LPAD((QUARTER({field})-1)*3+1, 2, '0'), '-01')",
            "month": f"DATE_FORMAT({field}, '%Y-%m-01')",
            "week": f"DATE_SUB({field}, INTERVAL WEEKDAY({field}) DAY)",
            "day": f"DATE({field})",
            "hour": f"DATE_FORMAT({field}, '%Y-%m-%d %H:00:00')"
        }
        return format_map.get(grain, f"DATE({field})")
    
    def add_unicode_prefix(self, sql: str) -> str:
        """
        MySQL 不需要 N 前缀。
        但需要做少量兼容性后处理：
        - 将 SQLGlot 输出的 `GROUP BY ROLLUP (a, b)` 转为 MySQL 支持的 `GROUP BY a, b WITH ROLLUP`
        """
        try:
            import re

            # 兼容 sqlglot 在 mysql 方言下仍输出标准/PG 风格 ROLLUP 语法：
            #   GROUP BY ROLLUP ( a, b )
            # MySQL 8.x 通用写法：
            #   GROUP BY a, b WITH ROLLUP
            #
            # 注意：这里做字符串级别替换，避免改动 AST 结构带来的方言分支复杂度。
            pattern = re.compile(
                r"GROUP\s+BY\s+ROLLUP\s*\(\s*(?P<exprs>[^)]+?)\s*\)",
                flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
            )

            def _repl(m: re.Match) -> str:
                exprs = m.group("exprs").strip()
                # 防御：空表达式不替换
                if not exprs:
                    return m.group(0)
                return f"GROUP BY {exprs} WITH ROLLUP"

            sql = pattern.sub(_repl, sql)
        except Exception as e:
            logger.warning("MySQL SQL 后处理失败（已忽略）", error=str(e))

        return sql
    
    def limit_clause(self, limit: int, offset: int = 0) -> str:
        """
        生成 MySQL LIMIT 子句
        
        Args:
            limit: 限制行数
            offset: 偏移量
        
        Returns:
            LIMIT 子句
        """
        if offset > 0:
            return f"LIMIT {offset}, {limit}"
        return f"LIMIT {limit}"
    
    def escape_identifier(self, identifier: str) -> str:
        """
        转义 MySQL 标识符
        
        Args:
            identifier: 表名或列名
        
        Returns:
            转义后的标识符（使用反引号）
        """
        return f"`{identifier}`"
    
    def string_concat(self, *parts: str) -> str:
        """
        字符串拼接
        
        Args:
            parts: 要拼接的部分
        
        Returns:
            MySQL CONCAT 表达式
        """
        return f"CONCAT({', '.join(parts)})"
    
    def regex_match(self, field: str, pattern: str) -> str:
        """
        正则表达式匹配
        
        Args:
            field: 字段名
            pattern: 正则表达式模式
        
        Returns:
            MySQL REGEXP 表达式
        """
        return f"{field} REGEXP {pattern}"

