"""SQL Server (T-SQL) 方言处理"""

import structlog
from typing import Dict, Any

logger = structlog.get_logger()


class TSQLDialect:
    """SQL Server 方言处理器"""
    
    def __init__(self):
        self.timezone_mapping = self._build_timezone_mapping()
    
    def _build_timezone_mapping(self) -> Dict[str, str]:
        """
        IANA 时区到 Windows 时区的映射
        SQL Server 使用 Windows 时区名称
        """
        return {
            "Asia/Shanghai": "China Standard Time",
            "America/New_York": "Eastern Standard Time",
            "America/Los_Angeles": "Pacific Standard Time",
            "Europe/London": "GMT Standard Time",
            "UTC": "UTC"
        }
    
    def convert_timezone(self, iana_tz: str) -> str:
        """
        转换 IANA 时区到 Windows 时区
        
        Args:
            iana_tz: IANA 时区名称（如 Asia/Shanghai）
        
        Returns:
            Windows 时区名称
        """
        return self.timezone_mapping.get(iana_tz, "UTC")
    
    def date_trunc_expression(self, grain: str, field: str) -> str:
        """
        生成日期截断表达式（时间分桶）
        
        Args:
            grain: 粒度 (day/week/month/quarter/year)
            field: 时间字段
        
        Returns:
            SQL 表达式
        """
        if grain == "day":
            # 截断到天
            return f"DATEADD(day, DATEDIFF(day, 0, {field}), 0)"
        
        elif grain == "week":
            # 截断到周一（ISO 周）
            return f"DATEADD(week, DATEDIFF(week, 0, {field}), 0)"
        
        elif grain == "month":
            # 截断到月初
            return f"DATEADD(month, DATEDIFF(month, 0, {field}), 0)"
        
        elif grain == "quarter":
            # 截断到季度初
            return f"DATEADD(quarter, DATEDIFF(quarter, 0, {field}), 0)"
        
        elif grain == "year":
            # 截断到年初
            return f"DATEADD(year, DATEDIFF(year, 0, {field}), 0)"
        
        else:
            logger.warning(f"不支持的时间粒度: {grain}")
            return field
    
    def escape_identifier(self, name: str) -> str:
        """
        转义标识符（使用方括号）
        
        Args:
            name: 标识符名称
        
        Returns:
            转义后的标识符
        """
        # 如果已经有方括号，不再转义
        if name.startswith("[") and name.endswith("]"):
            return name
        
        return f"[{name}]"
    
    def pagination_clause(self, limit: int, offset: int = 0) -> str:
        """
        生成分页子句
        
        SQL Server 2012+ 使用 OFFSET ... FETCH
        
        Args:
            limit: 限制行数
            offset: 偏移量
        
        Returns:
            分页 SQL 子句
        """
        if offset > 0:
            return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        else:
            # 如果不需要偏移，可以用 TOP
            return f"TOP ({limit})"
    
    def add_unicode_prefix(self, sql: str) -> str:
        """
        为包含中文的字符串字面量添加 N 前缀
        
        SQL Server 需要 N'...' 格式来正确处理 Unicode 字符（如中文）
        
        Args:
            sql: 原始 SQL 字符串
        
        Returns:
            添加 N 前缀后的 SQL
        """
        import re
        
        # 匹配单引号字符串，且包含中文字符
        # 中文 Unicode 范围：\u4e00-\u9fff
        def has_chinese(s):
            return bool(re.search(r'[\u4e00-\u9fff]', s))
        
        # 查找所有单引号字符串
        # 注意：需要避免匹配已经有 N 前缀的
        # 模式：(?<!N)'([^']*)'
        pattern = r"(?<!N)'([^']*)'"
        
        def replace_func(match):
            content = match.group(1)
            if has_chinese(content):
                return f"N'{content}'"
            else:
                return match.group(0)
        
        sql_with_prefix = re.sub(pattern, replace_func, sql)
        return sql_with_prefix

