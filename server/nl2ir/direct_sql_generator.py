"""
直接 SQL 生成器 - 混合架构核心组件

当查询过于复杂、无法用 IR 表达时，直接调用 LLM 生成 SQL。
生成的 SQL 会经过 SQLPostProcessor 进行安全检查和权限注入。
"""

import json
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import structlog

from server.config import settings
from server.compiler.dialect_profiles import get_dialect_profile
from server.utils.prompt_loader import resolve_path, load_text

logger = structlog.get_logger()

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


_DEFAULT_SYSTEM_PROMPT_TEMPLATE = """你是一个专业的 SQL 生成专家，负责根据用户的自然语言问题生成精确的 SQL 查询。

## 核心约束
1. **只能使用提供的表和字段**：你只能使用下方候选表中列出的表和字段，严禁使用未列出的表或字段。
2. **生成的 SQL 必须是 SELECT 语句**：严禁生成 INSERT、UPDATE、DELETE、DROP 等修改数据的语句。
3. **使用 {dialect} 方言**：生成的 SQL 必须符合 {dialect} 语法。
4. **必须添加适当的 LIMIT**：除非是聚合查询且结果明确只有少量行，否则应添加 LIMIT 限制结果数量。

## 候选表和字段
{table_schema}

## 输出格式
请以 JSON 格式输出，包含以下字段：
```json
{{
  "sql": "你生成的 SQL 语句",
  "explanation": "SQL 的简要说明",
  "tables_used": ["使用的表名列表"],
  "confidence": 0.0-1.0 之间的置信度数值
}}
```

## 注意事项
- 对于复杂的多步骤查询，优先使用 CTE (WITH 子句) 来提高可读性
- 注意处理 NULL 值，使用 COALESCE 或 NULLIF 避免除零错误
- 日期时间格式化请使用 {dialect} 的标准函数
- 字符串比较注意大小写敏感性
"""

_DEFAULT_USER_PROMPT_TEMPLATE = """请根据以下问题生成 SQL 查询：

**用户问题**：{question}

**额外上下文**（如果有）：
{context}

请生成符合要求的 SQL 查询。
"""


_DEFAULT_PROMPTS_DIR = _PROJECT_ROOT / "prompts" / "direct_sql"
PROMPTS_DIR = resolve_path(getattr(settings, "direct_sql_prompts_dir", None), _DEFAULT_PROMPTS_DIR)
SYSTEM_PROMPT_FILE = resolve_path(getattr(settings, "direct_sql_system_prompt_file", None), PROMPTS_DIR / "system.txt")
USER_TEMPLATE_FILE = resolve_path(
    getattr(settings, "direct_sql_user_template_file", None),
    PROMPTS_DIR / "user_template.txt",
)


@dataclass
class DirectSQLResult:
    """直接 SQL 生成结果"""
    success: bool
    sql: str
    confidence: float
    explanation: str
    tables_used: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    raw_response: Optional[str] = None


class DirectSQLGenerator:
    """
    直接 SQL 生成器
    
    使用 LLM 直接生成 SQL，适用于超出 IR 表达能力的复杂查询。
    """
    
    SYSTEM_PROMPT_TEMPLATE = load_text(
        SYSTEM_PROMPT_FILE, default=_DEFAULT_SYSTEM_PROMPT_TEMPLATE, prompt_name="direct_sql_system"
    )
    USER_PROMPT_TEMPLATE = load_text(
        USER_TEMPLATE_FILE, default=_DEFAULT_USER_PROMPT_TEMPLATE, prompt_name="direct_sql_user_template"
    )

    def __init__(
        self,
        llm_client,
        dialect: str = "tsql",
        default_limit: int = 1000
    ):
        """
        初始化直接 SQL 生成器
        
        Args:
            llm_client: LLM 客户端
            dialect: SQL 方言
            default_limit: 默认结果限制
        """
        self.llm_client = llm_client
        self.profile = get_dialect_profile(dialect)
        self.dialect = self.profile.sqlglot_dialect
        self.default_limit = default_limit
    
    async def generate(
        self,
        question: str,
        table_schema: str,
        context: Optional[Dict[str, Any]] = None
    ) -> DirectSQLResult:
        """
        根据问题生成 SQL
        
        Args:
            question: 用户问题
            table_schema: 表结构描述（Markdown 格式）
            context: 额外上下文
        
        Returns:
            DirectSQLResult 对象
        """
        try:
            # 构建提示词
            system_prompt = self.SYSTEM_PROMPT_TEMPLATE.format(
                dialect=self._dialect_display_name(),
                table_schema=table_schema
            )
            if self.profile.is_mysql_family:
                system_prompt += (
                    "\n- 禁止使用 FULL OUTER JOIN，需改写为 LEFT/RIGHT JOIN + UNION ALL 等价形式"
                    "\n- 禁止使用 GROUPING()，汇总行请用显式 UNION ALL 生成"
                    "\n- 优先使用 WITH ROLLUP 或显式总计子查询，不要输出 SQL Server 专属语法"
                )
            
            context_str = ""
            if context:
                context_items = []
                for key, value in context.items():
                    context_items.append(f"- {key}: {value}")
                context_str = "\n".join(context_items)
            else:
                context_str = "无"
            
            user_prompt = self.USER_PROMPT_TEMPLATE.format(
                question=question,
                context=context_str
            )
            
            # 调用 LLM
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            logger.debug(
                "调用 LLM 直接生成 SQL",
                question=question[:100],
                dialect=self.dialect
            )
            
            response = await self.llm_client.chat_async(messages)
            raw_response = response.get("content", "") if isinstance(response, dict) else str(response)
            
            # 解析响应
            result = self._parse_response(raw_response)
            result.raw_response = raw_response
            
            return result
            
        except Exception as e:
            logger.error("直接 SQL 生成失败", error=str(e), question=question[:100])
            return DirectSQLResult(
                success=False,
                sql="",
                confidence=0.0,
                explanation=f"SQL 生成失败: {str(e)}",
                warnings=[str(e)]
            )
    
    def _parse_response(self, response: str) -> DirectSQLResult:
        """解析 LLM 响应"""
        try:
            # 尝试提取 JSON 块
            json_match = None
            
            # 尝试 markdown 代码块格式
            import re
            json_pattern = r'```(?:json)?\s*\n([\s\S]*?)\n```'
            match = re.search(json_pattern, response)
            if match:
                json_str = match.group(1)
            else:
                # 尝试直接解析
                json_str = response.strip()
                # 如果以 { 开头，找到最后一个 }
                if json_str.startswith('{'):
                    brace_count = 0
                    end_idx = 0
                    for i, char in enumerate(json_str):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end_idx = i + 1
                                break
                    json_str = json_str[:end_idx]
            
            # 解析 JSON
            data = json.loads(json_str)
            
            sql = data.get("sql", "").strip()
            if not sql:
                return DirectSQLResult(
                    success=False,
                    sql="",
                    confidence=0.0,
                    explanation="LLM 未生成有效的 SQL",
                    warnings=["响应中未包含 SQL"]
                )
            
            return DirectSQLResult(
                success=True,
                sql=sql,
                confidence=float(data.get("confidence", 0.8)),
                explanation=data.get("explanation", ""),
                tables_used=data.get("tables_used", []),
                warnings=[]
            )
            
        except json.JSONDecodeError as e:
            # JSON 解析失败，尝试直接提取 SQL
            logger.warning("JSON 解析失败，尝试直接提取 SQL", error=str(e))
            
            # 尝试从 markdown SQL 代码块提取
            import re
            sql_pattern = r'```sql\s*\n([\s\S]*?)\n```'
            match = re.search(sql_pattern, response, re.IGNORECASE)
            if match:
                sql = match.group(1).strip()
                return DirectSQLResult(
                    success=True,
                    sql=sql,
                    confidence=0.6,
                    explanation="从代码块提取的 SQL",
                    warnings=["响应格式非标准，已自动提取 SQL"]
                )
            
            return DirectSQLResult(
                success=False,
                sql="",
                confidence=0.0,
                explanation="无法解析 LLM 响应",
                warnings=[f"JSON 解析错误: {str(e)}"]
            )
    
    def _dialect_display_name(self) -> str:
        """获取方言的显示名称"""
        return self.profile.display_name
    
    @classmethod
    def build_table_schema_from_model(cls, model, connection_id: str = None) -> str:
        """
        从语义模型构建表结构描述
        
        Args:
            model: SemanticModel 对象
            connection_id: 连接ID（用于过滤）
        
        Returns:
            Markdown 格式的表结构描述
        """
        schema_parts = []
        
        # 遍历数据源
        if hasattr(model, 'sources') and model.sources:
            for source_id, source in model.sources.items():
                table_name = getattr(source, 'table_name', source_id)
                schema_name = getattr(source, 'schema_name', 'dbo')
                display_name = getattr(source, 'display_name', table_name)
                
                schema_parts.append(f"### 表: {schema_name}.{table_name}")
                if display_name != table_name:
                    schema_parts.append(f"显示名: {display_name}")
                
                # 收集该表的字段
                columns = []
                
                # 从 fields 获取
                if hasattr(model, 'fields'):
                    for field_id, field_obj in model.fields.items():
                        field_source = getattr(field_obj, 'datasource_id', None)
                        if field_source == source_id:
                            col_name = getattr(field_obj, 'column', None) or getattr(field_obj, 'field_name', None)
                            col_type = getattr(field_obj, 'data_type', 'unknown')
                            display = getattr(field_obj, 'display_name', col_name)
                            desc = getattr(field_obj, 'description', '')
                            
                            col_info = f"- `{col_name}` ({col_type})"
                            if display and display != col_name:
                                col_info += f" - {display}"
                            if desc:
                                col_info += f": {desc}"
                            columns.append(col_info)
                
                # 从 dimensions 获取
                if hasattr(model, 'dimensions'):
                    for dim_id, dim_obj in model.dimensions.items():
                        dim_table = getattr(dim_obj, 'table', None)
                        if dim_table == source_id:
                            col_name = getattr(dim_obj, 'column', dim_id)
                            display = getattr(dim_obj, 'display_name', col_name)
                            
                            col_info = f"- `{col_name}` (维度)"
                            if display and display != col_name:
                                col_info += f" - {display}"
                            columns.append(col_info)
                
                # 从 measures 获取
                if hasattr(model, 'measures'):
                    for meas_id, meas_obj in model.measures.items():
                        meas_table = getattr(meas_obj, 'table', None)
                        if meas_table == source_id:
                            col_name = getattr(meas_obj, 'column', meas_id)
                            col_type = getattr(meas_obj, 'data_type', 'numeric')
                            display = getattr(meas_obj, 'display_name', col_name)
                            
                            col_info = f"- `{col_name}` ({col_type}, 度量)"
                            if display and display != col_name:
                                col_info += f" - {display}"
                            columns.append(col_info)
                
                if columns:
                    schema_parts.append("字段:")
                    schema_parts.extend(columns)
                
                schema_parts.append("")  # 空行分隔
        
        return "\n".join(schema_parts)


async def get_direct_sql_generator(
    connection_id: str,
    dialect: str = "tsql"
) -> DirectSQLGenerator:
    """
    获取直接 SQL 生成器实例
    
    Args:
        connection_id: 数据库连接ID
        dialect: SQL 方言
    
    Returns:
        DirectSQLGenerator 实例
    """
    from server.dependencies import get_direct_sql_llm_client
    llm_client = get_direct_sql_llm_client()
    return DirectSQLGenerator(
        llm_client=llm_client,
        dialect=dialect
    )
