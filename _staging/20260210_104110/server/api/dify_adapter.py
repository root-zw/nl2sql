"""Dify 工具适配器 API

将 NL2SQL 系统作为 Dify 的自定义工具（Tool）集成。
Dify 可以通过 HTTP 请求调用此 API，实现自然语言到 SQL 的查询功能。
"""

from typing import Dict, Any, Optional
import structlog
import json
from fastapi import APIRouter, HTTPException, Query, Body, Request, Form
from fastapi.responses import JSONResponse, PlainTextResponse

from server.models.api import QueryRequest, QueryResponse
from server.api.query.routes import query as query_handler
from server.dependencies import get_query_cache

logger = structlog.get_logger()
router = APIRouter()


@router.post("/dify/tool/NL2SQL")
async def dify_tool_NL2SQL(
    http_request: Request
):
    """
    Dify 工具调用端点
    
    接收 Dify 发送的工具调用请求，执行 NL2SQL 查询，并返回结果。
    
    支持多种请求格式：
    1. Dify 标准格式（嵌套 parameters）:
    {
        "user_id": "user123",
        "parameters": {
            "question": "查询最近7天的销售总额",
            "connection_id": "xxx-xxx-xxx"
        }
    }
    
    2. 直接参数格式:
    {
        "question": "查询最近7天的销售总额",
        "connection_id": "xxx-xxx-xxx",
        "user_id": "user123"
    }
    
    响应格式（Dify Tool 标准格式）:
    {
        "result": "查询结果的自然语言描述或数据",
        "error": null (可选),
        "metadata": {} (可选)
    }
    """
    try:
        # 检查 Content-Type 以确定请求格式
        content_type = http_request.headers.get("content-type", "").lower()
        is_form_data = "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type
        
        if is_form_data:
            # 处理 form-data 或 x-www-form-urlencoded
            form_data = await http_request.form()
            request = {}
            for key, value in form_data.items():
                request[key] = value
            
            logger.debug("收到 form-data 请求", form_keys=list(request.keys()))
            
            # form-data 格式：参数直接在顶层
            parameters = request
            user_id = request.get("user_id", "dify_user")
        else:
            # 处理 JSON 格式
            try:
                body = await http_request.body()
                if not body:
                    logger.warning("收到空请求体")
                    raise HTTPException(
                        status_code=400,
                        detail="请求体不能为空。请确保请求包含 JSON 数据或 form-data。"
                    )
                request = json.loads(body)
            except json.JSONDecodeError as e:
                body_str = body.decode('utf-8', errors='ignore')[:200] if body else "empty"
                logger.error("请求体 JSON 解析失败", error=str(e), body_preview=body_str)
                raise HTTPException(
                    status_code=400,
                    detail=f"请求体格式错误，不是有效的 JSON: {str(e)}。请求体预览: {body_str}"
                )
            
            # 记录原始请求（用于调试）
            logger.debug("收到 JSON 请求", request_keys=list(request.keys()) if isinstance(request, dict) else "not_dict")
            
            # 支持两种 JSON 请求格式：
            # 1. 嵌套格式: {"parameters": {...}, "user_id": ...}
            # 2. 直接格式: {"question": ..., "connection_id": ...}
            
            if not isinstance(request, dict):
                raise HTTPException(
                    status_code=400,
                    detail="请求体必须是 JSON 对象"
                )
            
            # 尝试从嵌套格式提取参数
            if "parameters" in request and isinstance(request.get("parameters"), dict):
                parameters = request.get("parameters", {})
                user_id = request.get("user_id", "dify_user")
            else:
                # 直接格式：所有参数都在顶层
                parameters = request
                user_id = request.get("user_id", "dify_user")
        
        # 必需参数
        question = parameters.get("question") or parameters.get("query")
        if not question:
            logger.error("缺少 question 参数", request_keys=list(request.keys()))
            raise HTTPException(
                status_code=400,
                detail="缺少必需参数: question 或 query"
            )
        
        connection_id = parameters.get("connection_id") or parameters.get("connectionId")
        if not connection_id:
            logger.error("缺少 connection_id 参数", request_keys=list(request.keys()))
            raise HTTPException(
                status_code=400,
                detail="缺少必需参数: connection_id"
            )
        
        # 可选参数
        domain_id = parameters.get("domain_id") or parameters.get("domainId")
        force_execute = parameters.get("force_execute", False) or parameters.get("forceExecute", False)
        explain_only = parameters.get("explain_only", False) or parameters.get("explainOnly", False)
        
        logger.debug(
            "收到 Dify 工具调用请求",
            user_id=user_id,
            question=question[:50] if question else None,
            connection_id=connection_id
        )
        
        # 构建 QueryRequest
        # 对于 Dify 工具调用场景，将叙述生成交给 Dify 主模型处理，
        # 因此这里显式关闭内部的二次大模型叙述（disable_narrative=True），避免额外等待。
        query_request = QueryRequest(
            text=question,
            connection_id=connection_id,
            user_id=user_id,
            role="viewer",
            domain_id=domain_id,
            force_execute=force_execute,
            explain_only=explain_only,
            skip_cache=False,
            disable_narrative=True
        )
        
        # 调用查询处理器
        cache = get_query_cache()  # get_query_cache 是同步函数，不需要 await
        json_response = await query_handler(query_request, cache)
        
        # query_handler 返回的是 JSONResponse，需要提取内容
        if isinstance(json_response, JSONResponse):
            # 从 JSONResponse 中提取 JSON 内容
            response_body = json_response.body
            response_dict = json.loads(response_body.decode('utf-8'))
            # 将字典转换为 QueryResponse 对象
            query_response = QueryResponse(**response_dict)
        else:
            # 如果不是 JSONResponse，尝试直接使用
            query_response = json_response
        
        # 转换为 Dify 工具响应格式
        dify_response = _convert_to_dify_format(query_response)
        
        logger.debug(
            "Dify 工具调用完成",
            status=query_response.status,
            query_id=query_response.query_id
        )
        
        response_text = dify_response.get("result", "") or ""
        error_text = dify_response.get("error")
        status_code = 200 if not error_text else 400
        
        if error_text and not response_text:
            response_text = error_text
        elif error_text:
            response_text = f"{response_text}\n\n错误: {error_text}"
        
        headers = {}
        metadata = dify_response.get("metadata") or {}
        if metadata.get("query_id"):
            headers["X-Query-ID"] = metadata["query_id"]
        
        return PlainTextResponse(
            response_text,
            media_type="text/markdown; charset=utf-8",
            status_code=status_code,
            headers=headers
        )
        
    except HTTPException as e:
        # 尝试获取 request 变量（可能在某些情况下未定义）
        try:
            request_keys = list(request.keys()) if isinstance(request, dict) else []
        except:
            request_keys = []
        logger.error(
            "Dify 工具调用参数错误",
            status_code=e.status_code,
            detail=e.detail,
            request_keys=request_keys
        )
        raise
    except Exception as e:
        # 尝试获取 request 变量（可能在某些情况下未定义）
        try:
            request_keys = list(request.keys()) if isinstance(request, dict) else []
        except:
            request_keys = []
        logger.error(
            "Dify 工具调用失败",
            error=str(e),
            error_type=type(e).__name__,
            request_keys=request_keys,
            exc_info=True
        )
        return JSONResponse(
            status_code=500,
            content={
                "result": "",
                "error": f"查询处理失败: {str(e)}",
                "metadata": {
                    "error_type": type(e).__name__
                }
            }
        )


@router.post("/dify/tool/NL2SQL/simple")
async def dify_tool_NL2SQL_simple(
    question: str = Body(..., embed=True),
    connection_id: str = Body(..., embed=True),
    user_id: str = Body(default="dify_user", embed=True),
    domain_id: Optional[str] = Body(default=None, embed=True),
    force_execute: bool = Body(default=False, embed=True),
    explain_only: bool = Body(default=False, embed=True)
):
    """
    简化的 Dify 工具调用端点（备用）
    
    使用独立的参数，而不是嵌套对象。
    适用于某些 Dify 配置方式。
    """
    try:
        logger.debug(
            "收到 Dify 简化请求",
            user_id=user_id,
            question=question[:50] if question else None,
            connection_id=connection_id
        )
        
        # 构建 QueryRequest（同上：Dify 场景不在后端生成叙述，交给 Dify 自己的 LLM 来总结）
        query_request = QueryRequest(
            text=question,
            connection_id=connection_id,
            user_id=user_id,
            role="viewer",
            domain_id=domain_id,
            force_execute=force_execute,
            explain_only=explain_only,
            skip_cache=False,
            disable_narrative=True
        )
        
        # 调用查询处理器
        cache = get_query_cache()  # get_query_cache 是同步函数，不需要 await
        json_response = await query_handler(query_request, cache)
        
        # query_handler 返回的是 JSONResponse，需要提取内容
        if isinstance(json_response, JSONResponse):
            # 从 JSONResponse 中提取 JSON 内容
            response_body = json_response.body
            response_dict = json.loads(response_body.decode('utf-8'))
            # 将字典转换为 QueryResponse 对象
            query_response = QueryResponse(**response_dict)
        else:
            # 如果不是 JSONResponse，尝试直接使用
            query_response = json_response
        
        # 转换为 Dify 工具响应格式
        dify_response = _convert_to_dify_format(query_response)
        
        logger.debug(
            "Dify 简化请求处理完成",
            status=query_response.status,
            query_id=query_response.query_id
        )
        
        response_text = dify_response.get("result", "") or ""
        error_text = dify_response.get("error")
        status_code = 200 if not error_text else 400
        
        if error_text and not response_text:
            response_text = error_text
        elif error_text:
            response_text = f"{response_text}\n\n错误: {error_text}"
        
        headers = {}
        metadata = dify_response.get("metadata") or {}
        if metadata.get("query_id"):
            headers["X-Query-ID"] = metadata["query_id"]
        
        return PlainTextResponse(
            response_text,
            media_type="text/markdown; charset=utf-8",
            status_code=status_code,
            headers=headers
        )
        
    except Exception as e:
        logger.error("Dify 简化请求处理失败", error=str(e), exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "result": "",
                "error": f"查询处理失败: {str(e)}",
                "metadata": {}
            }
        )


@router.get("/dify/tool/schema")
async def dify_tool_schema():
    """
    返回 Dify 工具定义 Schema
    
    用于在 Dify 平台配置工具时使用。
    """
    schema = {
        "identity": {
            "author": "NL2SQL",
            "name": "NL2SQL_query",
            "label": {
                "en_US": "Text to SQL Query",
                "zh_Hans": "自然语言转SQL查询"
            },
            "icon": "🔍",
            "tags": ["database", "sql", "query"]
        },
        "parameters": [
            {
                "name": "question",
                "type": "string",
                "required": True,
                "label": {
                    "en_US": "Question",
                    "zh_Hans": "查询问题"
                },
                "human_description": {
                    "en_US": "Natural language question to query the database",
                    "zh_Hans": "用于查询数据库的自然语言问题"
                }
            },
            {
                "name": "connection_id",
                "type": "string",
                "required": True,
                "label": {
                    "en_US": "Database Connection ID",
                    "zh_Hans": "数据库连接ID"
                },
                "human_description": {
                    "en_US": "The ID of the database connection to query",
                    "zh_Hans": "要查询的数据库连接ID"
                }
            },
            {
                "name": "domain_id",
                "type": "string",
                "required": False,
                "label": {
                    "en_US": "Business Domain ID",
                    "zh_Hans": "业务域ID"
                },
                "human_description": {
                    "en_US": "Optional business domain ID to scope the query",
                    "zh_Hans": "可选的业务域ID，用于限定查询范围"
                }
            },
            {
                "name": "force_execute",
                "type": "boolean",
                "required": False,
                "label": {
                    "en_US": "Force Execute",
                    "zh_Hans": "强制执行"
                },
                "human_description": {
                    "en_US": "Skip cost check and force execute the query",
                    "zh_Hans": "跳过成本检查并强制执行查询"
                },
                "default": False
            },
            {
                "name": "explain_only",
                "type": "boolean",
                "required": False,
                "label": {
                    "en_US": "Explain Only",
                    "zh_Hans": "仅解释"
                },
                "human_description": {
                    "en_US": "Only return SQL without executing",
                    "zh_Hans": "仅返回SQL而不执行查询"
                },
                "default": False
            }
        ]
    }
    
    return JSONResponse(content=schema)


def _convert_to_dify_format(query_response: QueryResponse) -> Dict[str, Any]:
    """
    将 QueryResponse 转换为 Dify 工具响应格式
    
    Args:
        query_response: NL2SQL 查询响应
        
    Returns:
        Dify 工具响应格式的字典
    """
    if query_response.status == "success" and query_response.result:
        result = query_response.result
        
        # 构建自然语言结果描述（内部已包含摘要）
        result_text = _format_result_as_text(result, query_response)
        
        # 构建元数据
        metadata = {
            "query_id": query_response.query_id,
            "timestamp": query_response.timestamp,
            "row_count": len(result.rows) if result.rows else 0,
            "column_count": len(result.columns) if result.columns else 0,
            "sql": result.meta.get("sql", ""),
            "cache_hit": result.meta.get("cache_hit", False),
            "latency_ms": result.meta.get("latency_ms", 0)
        }
        
        return {
            "result": result_text,
            "error": None,
            "metadata": metadata
        }
    
    elif query_response.status == "confirm_needed" and query_response.confirmation:
        # 需要确认的情况
        confirm = query_response.confirmation
        warnings_text = "\n".join([f"⚠️ {w}" for w in confirm.warnings])
        
        result_text = f"""需要确认查询：

{confirm.natural_language}

{warnings_text}

请确认是否继续执行此查询。"""
        
        return {
            "result": result_text,
            "error": None,
            "metadata": {
                "query_id": query_response.query_id,
                "timestamp": query_response.timestamp,
                "needs_confirm": True,
                "confirmation": {
                    "natural_language": confirm.natural_language,
                    "warnings": confirm.warnings,
                    "suggestions": confirm.suggestions
                }
            }
        }
    
    elif query_response.status == "error":
        # 错误情况
        error = query_response.error or {}
        error_message = error.get("message", "查询失败")
        error_code = error.get("code", "UNKNOWN_ERROR")
        
        return {
            "result": "",
            "error": f"[{error_code}] {error_message}",
            "metadata": {
                "query_id": query_response.query_id,
                "timestamp": query_response.timestamp,
                "error_code": error_code,
                "error_details": error
            }
        }
    
    else:
        # 未知状态
        return {
            "result": "",
            "error": "未知的响应状态",
            "metadata": {
                "query_id": query_response.query_id,
                "timestamp": query_response.timestamp,
                "status": query_response.status
            }
        }


def _convert_formula_to_latex(formula: str) -> str:
    """
    将公式文本转换为LaTeX格式，支持常见的数学表达式
    
    例如：
    - SUM(x) / SUM(y) -> \frac{\sum x}{\sum y}
    - COUNT(*) -> \text{COUNT}(*)
    - AVG(x) -> \bar{x} 或 \text{AVG}(x)
    """
    import re
    
    if not formula or not isinstance(formula, str):
        return formula
    
    # 保护已存在的LaTeX公式（用$包裹的部分）
    latex_pattern = r'\$[^$]+\$'
    latex_parts = {}
    placeholder = "___LATEX_PLACEHOLDER_{}___"
    placeholders = []
    
    def replace_latex(match):
        idx = len(placeholders)
        placeholders.append(match.group(0))
        return placeholder.format(idx)
    
    # 先提取已存在的LaTeX公式
    protected_formula = re.sub(latex_pattern, replace_latex, formula)
    
    # 转换常见的数学函数和运算符
    # SUM(x) -> \sum x 或 \text{SUM}(x)（如果x是复杂表达式）
    protected_formula = re.sub(
        r'SUM\s*\(([^)]+)\)',
        lambda m: r'\text{SUM}(' + m.group(1) + ')',
        protected_formula,
        flags=re.IGNORECASE
    )
    
    protected_formula = re.sub(
        r'COUNT\s*\(([^)]*)\)',
        lambda m: r'\text{COUNT}(' + (m.group(1) if m.group(1) else '*') + ')',
        protected_formula,
        flags=re.IGNORECASE
    )
    
    protected_formula = re.sub(
        r'AVG\s*\(([^)]+)\)',
        lambda m: r'\text{AVG}(' + m.group(1) + ')',
        protected_formula,
        flags=re.IGNORECASE
    )
    
    protected_formula = re.sub(
        r'MAX\s*\(([^)]+)\)',
        lambda m: r'\text{MAX}(' + m.group(1) + ')',
        protected_formula,
        flags=re.IGNORECASE
    )
    
    protected_formula = re.sub(
        r'MIN\s*\(([^)]+)\)',
        lambda m: r'\text{MIN}(' + m.group(1) + ')',
        protected_formula,
        flags=re.IGNORECASE
    )
    
    # 转换除法：x / y -> \frac{x}{y}（简单情况）
    # 注意：这里只处理简单的除法，复杂情况需要更复杂的解析
    protected_formula = re.sub(
        r'([^/\s]+)\s*/\s*([^/\s]+)',
        r'\\frac{\1}{\2}',
        protected_formula
    )
    
    # 转换乘法：x * y -> x \times y（在公式中）
    protected_formula = re.sub(
        r'([^\*\s]+)\s*\*\s*([^\*\s]+)',
        r'\1 \\times \2',
        protected_formula
    )
    
    # 恢复LaTeX占位符
    for idx, latex in enumerate(placeholders):
        protected_formula = protected_formula.replace(placeholder.format(idx), latex)
    
    # 如果公式包含数学符号，用$包裹（如果还没有）
    if any(op in protected_formula for op in ['\\frac', '\\sum', '\\text', '\\times']):
        if not (protected_formula.strip().startswith('$') and protected_formula.strip().endswith('$')):
            # 检查是否已经是LaTeX格式
            if '\\' in protected_formula:
                protected_formula = f'${protected_formula}$'
    
    return protected_formula


def _format_result_as_text(result, query_response: QueryResponse) -> str:
    """
    将查询结果格式化为更易读的 Markdown 文本，顺序：
    1) 查询摘要 2) 查询说明 3) 计算说明 4) 查询结果（全部行）
    """
    def _clean_text(text: str) -> str:
        if not isinstance(text, str):
            return text
        stripped = text.strip()
        for icon in ["📂", "📊", "📋", "📐", "📁"]:
            if stripped.startswith(icon):
                stripped = stripped[len(icon):].lstrip()
        return stripped or text
    
    def _format_as_list(value):
        rendered = []
        if isinstance(value, list):
            for item in value:
                rendered.append(f"- { _clean_text(item) }")
        elif isinstance(value, dict):
            for k, v in value.items():
                rendered.append(f"- **{_clean_text(k)}**: {_clean_text(v)}")
        elif value:
            rendered.append(_clean_text(str(value)))
        return rendered
    
    def _format_value(value):
        if value is None:
            return "-"
        if isinstance(value, (int, float)):
            return str(value)
        return str(value).replace("\n", " ")
    
    lines: list[str] = []
    
    # 1. 查询摘要（仅当真实有摘要时才输出；没有就完全省略这一段，避免出现“暂无摘要。”）
    summary = result.summary or result.meta.get("process_summary")
    if summary:
        lines.append("### 查询摘要")
        lines.append(_clean_text(summary.strip()))
        lines.append("")

    # 2. 查询说明
    process_explanation = result.meta.get("process_explanation")
    if process_explanation:
        lines.append("### 查询说明")
        lines.extend(_format_as_list(process_explanation))
        lines.append("")
    
    # 3. 计算说明（仅当存在时显示，使用列表格式）
    derived = result.meta.get("derived_calculations") or []
    if derived:
        lines.append("### 计算说明")
        for calc in derived:
            if isinstance(calc, dict):
                name = calc.get("display_name") or calc.get("metric_id")
                formula = calc.get("formula_detailed") or calc.get("formula")
                if name and formula:
                    # 将公式转换为LaTeX格式
                    latex_formula = _convert_formula_to_latex(_clean_text(formula))
                    lines.append(f"- **{_clean_text(name)}**：{latex_formula}")
        lines.append("")
    
    # 4. 查询结果（全部展示）
    column_names = [col["name"] for col in result.columns]
    if result.rows:
        lines.append("### 查询结果")
        lines.append("| " + " | ".join(column_names) + " |")
        lines.append("| " + " | ".join(["---"] * len(column_names)) + " |")
        for row in result.rows:
            formatted_row = [_format_value(value) for value in row]
            lines.append("| " + " | ".join(formatted_row) + " |")
    
    # 5. 生成 SQL（放在最后）
    sql_text = (result.meta or {}).get("sql")
    if sql_text:
        lines.append("")
        lines.append("### 生成 SQL")
        lines.append("```sql")
        lines.append(sql_text.strip())
        lines.append("```")
    
    return "\n".join(lines).strip()


