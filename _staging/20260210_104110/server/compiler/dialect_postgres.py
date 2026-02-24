"""PostgreSQL 方言处理"""

import structlog
from typing import Dict, Any

logger = structlog.get_logger()


class PostgreSQLDialect:
    """PostgreSQL 方言处理器"""
    
    def __init__(self):
        self.timezone_mapping = self._build_timezone_mapping()
    
    def _build_timezone_mapping(self) -> Dict[str, str]:
        """
        IANA 时区映射
        PostgreSQL 直接支持 IANA 时区名称
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
        PostgreSQL 直接支持 IANA 时区
        
        Args:
            iana_tz: IANA 时区名称（如 Asia/Shanghai）
        
        Returns:
            IANA 时区名称
        """
        return self.timezone_mapping.get(iana_tz, "UTC")
    
    def date_trunc_expression(self, grain: str, field: str) -> str:
        """
        生成 PostgreSQL 日期截断表达式
        
        Args:
            grain: 时间粒度 (year/quarter/month/week/day/hour)
            field: 字段名
        
        Returns:
            PostgreSQL date_trunc 表达式
        """
        # PostgreSQL 原生支持 date_trunc
        grain_map = {
            "year": "year",
            "quarter": "quarter",
            "month": "month",
            "week": "week",
            "day": "day",
            "hour": "hour"
        }
        pg_grain = grain_map.get(grain, "day")
        return f"date_trunc('{pg_grain}', {field})"
    
    def add_unicode_prefix(self, sql: str) -> str:
        """
        PostgreSQL 不需要 N 前缀
        直接返回原SQL
        """
        return sql
    
    def limit_clause(self, limit: int, offset: int = 0) -> str:
        """
        生成 PostgreSQL LIMIT 子句
        
        Args:
            limit: 限制行数
            offset: 偏移量
        
        Returns:
            LIMIT OFFSET 子句
        """
        clause = f"LIMIT {limit}"
        if offset > 0:
            clause += f" OFFSET {offset}"
        return clause
    
    def escape_identifier(self, identifier: str) -> str:
        """
        转义 PostgreSQL 标识符
        
        Args:
            identifier: 表名或列名
        
        Returns:
            转义后的标识符（使用双引号）
        """
        return f'"{identifier}"'
    
    def string_concat(self, *parts: str) -> str:
        """
        字符串拼接
        
        Args:
            parts: 要拼接的部分
        
        Returns:
            PostgreSQL || 运算符
        """
        return " || ".join(parts)
    
    def regex_match(self, field: str, pattern: str) -> str:
        """
        正则表达式匹配
        
        Args:
            field: 字段名
            pattern: 正则表达式模式
        
        Returns:
            PostgreSQL ~ 运算符
        """
        return f"{field} ~ {pattern}"
    
    def cast_to_text(self, field: str) -> str:
        """
        转换为文本类型
        
        Args:
            field: 字段名
        
        Returns:
            PostgreSQL CAST 表达式
        """
        return f"{field}::text"
    
    def cast_to_integer(self, field: str) -> str:
        """
        转换为整数类型
        
        Args:
            field: 字段名
        
        Returns:
            PostgreSQL CAST 表达式
        """
        return f"{field}::integer"

