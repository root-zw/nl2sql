"""查询 API"""

import asyncio
from typing import Dict, Any, List, Optional, Tuple, Set
import uuid
import contextvars
from datetime import datetime, date
import json
import re
import structlog
from fastapi import APIRouter, HTTPException, Depends, Query, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from server.models.api import (
    QueryRequest,
    QueryResponse,
    QueryResult,
    ConfirmationCard,
    TableSelectionCard,
    AccessibleTableItem,
    AccessibleTablesResponse,
    QuerySessionActionRequest,
)
from server.models.ir import IntermediateRepresentation
from server.models.permission import UserAccessibleConnectionsResponse
from server.exceptions import NL2SQLError, CostExceededError, SecurityError, CompilationError, AuthorizationError
from server.dependencies import (
    create_sql_compiler,
    create_query_executor,  # 动态创建executor
    get_query_cache,
    get_metadata_manager,
    create_nl2ir_parser
)
from server.formatter.result_formatter import (
    ResultFormatter,
    add_units_to_detail_columns,
    format_detail_row,
    format_detail_rows,
)
from server.explain.explainer import build_process_explanation, build_insights
from server.explain.narrative import generate_narrative, stream_narrative
from server.dependencies import get_llm_client, get_narrative_llm_client
from server.config import settings, RetrievalConfig, get_retrieval_param
from server.trace import create_tracer, get_or_resume_tracer  # 查询追踪
from server.enhanced_validation.loop import get_validation_loop
from server.nl2ir.few_shot_writer import FewShotWriter
from server.middleware.auth import get_optional_user, get_current_active_user
from server.models.admin import User as AdminUser
import asyncpg
from uuid import UUID
from server.services.schema_filter_service import SchemaFilterService
from server.services.permission_service import UserConnectionAccessService
from server.services.conversation_service import ConversationService, ActiveQueryRegistry
from server.services.draft_action_service import DraftActionService
from server.services.query_session_service import QuerySessionService
from server.services.stop_signal_service import StopSignalService, QueryStoppedException
from server.utils.db_pool import get_metadata_pool
from server.utils.json_utils import sanitize_for_json

# 全局查询取消检查器
_cancel_check_queries: Dict[str, bool] = {}

# 使用 contextvars 传递 message_id（用于停止信号检查）
_message_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('message_id', default=None)
_query_id_ctx: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("query_id", default=None)

# ============================================================
# 从拆分模块导入
# ============================================================
# 从拆分模块导入（使用相对导入）
from .stream_emitter import (
    QueryStreamEmitter,
    stream_progress as _stream_progress,
    stream_result as _stream_result,
    stream_error as _stream_error,
    stream_confirmation as _stream_confirmation,
    stream_table_selection as _stream_table_selection,
    stream_thinking as _stream_thinking,
)
from .history import save_query_history as _save_query_history
from .visualization import suggest_visualization as _suggest_visualization
from .ir_utils import (
    ir_to_display_dict as _ir_to_display_dict,
    build_derived_rule_map as _build_derived_rule_map,
    field_matches_table as _field_matches_table,
    metric_uses_table as _metric_uses_table,
    # check_confirmation_needed 已移除 - 旧的"请确认AI理解"功能已废弃
)
from .sql_fallback import (
    maybe_fix_join_path_error as _maybe_fix_join_path_error,
    force_single_table_fallback as _force_single_table_fallback,
    compile_with_join_fallback as _compile_with_join_fallback,
)
from .permission_checker import (
    format_table_names as _format_table_names,
    format_field_names as _format_field_names,
    collect_ir_field_ids as _collect_ir_field_ids,
    collect_ir_table_ids as _collect_ir_table_ids,
    enforce_schema_permissions as _enforce_schema_permissions,
    auto_detect_connection as _auto_detect_connection,
)
from .table_selection import (
    llm_select_table_cross_connections as _llm_select_table_cross_connections,
)
from .derived_metrics import (
    build_derived_metrics_explanation as _build_derived_metrics_explanation,
)
from .confirmation_utils import (
    build_draft_confirmation_summary as _build_draft_confirmation_summary,
    compose_question_with_revision as _compose_question_with_revision,
    resolve_confirmation_mode as _resolve_confirmation_mode,
)

logger = structlog.get_logger()
router = APIRouter()
_few_shot_writer = FewShotWriter()


class MockRequestState:
    """模拟 HTTP Request 的 state 对象，用于 WebSocket 场景"""
    def __init__(self, auth_attempted: bool = False, auth_success: bool = True, 
                 auth_error_code: Optional[str] = None, auth_provider: Optional[str] = None):
        self.auth_attempted = auth_attempted
        self.auth_success = auth_success
        self.auth_error_code = auth_error_code
        self.auth_provider = auth_provider


class MockRequest:
    """模拟 HTTP Request 对象，用于 WebSocket 调用 query 函数"""
    def __init__(self, state: MockRequestState = None):
        self.state = state or MockRequestState()

_stream_emitter_ctx: contextvars.ContextVar[Optional["QueryStreamEmitter"]] = contextvars.ContextVar(
    "query_stream_emitter",
    default=None
)


def _try_uuid(value: Optional[str]) -> Optional[UUID]:
    if not value:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


async def _load_table_display_names(table_ids: List[str]) -> List[str]:
    valid_ids: List[UUID] = []
    for table_id in table_ids:
        try:
            valid_ids.append(UUID(str(table_id)))
        except (TypeError, ValueError):
            continue

    if not valid_ids:
        return []

    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT table_id::text AS table_id, display_name
            FROM db_tables
            WHERE table_id = ANY($1::uuid[])
            """,
            valid_ids,
        )

    name_map = {
        str(row["table_id"]): row["display_name"]
        for row in rows
        if row.get("display_name")
    }
    return [name_map[str(table_id)] for table_id in table_ids if str(table_id) in name_map]


async def _check_stop_signal(step_name: str = ""):
    """
    检查停止信号，如果已停止则抛出异常

    这个辅助函数用于在 query() 函数的关键步骤之间检查是否需要停止

    Args:
        step_name: 当前步骤名称（用于日志）

    Raises:
        QueryStoppedException: 如果检测到停止信号
    """
    message_id = _message_id_ctx.get(None)
    if message_id and StopSignalService.check_stop_signal(message_id):
        logger.info("检测到停止信号", message_id=message_id, step=step_name)
        raise QueryStoppedException(message_id, f"在{step_name}步骤被取消" if step_name else "用户取消")


def _build_vector_fallback_selection_card(
    tables_with_fields: List[Any],
    question: str,
    fallback_reason: str = "ir_vote_failed"
) -> TableSelectionCard:
    """
    将向量检索结果转换为完整的 TableSelectionCard
    
    当向量检索链路的IR投票未通过时，使用此函数生成表选择确认卡，
    复用 LLM选表链路的表选择确认界面格式，确保用户确认后的流程能正常执行。
    
    设计原则：
    1. 复用 LLM选表链路的 TableSelectionCard 格式，保证前端兼容性
    2. 候选表来源是向量检索结果，不调用 LLM1，提升响应速度
    3. 用户确认后复用 LLM选表链路的后续流程（NL2IR解析等）
    4. 所有必要数据（table_id, connection_id等）通过 TableCandidate 传递
    
    Args:
        tables_with_fields: 向量检索结果的候选表列表（TableWithFields 对象）
        question: 用户原始问题
        fallback_reason: 降级原因（用于日志和调试）
        
    Returns:
        TableSelectionCard: 完整的表选择卡对象，与 LLM选表链路格式一致
    """
    from server.models.api import TableCandidate, TableSelectionCard
    
    # 从配置获取降级时的候选表数量
    fallback_count = get_retrieval_param(
        "vector_table_selection.fallback_candidate_count",
        5  # 默认值
    )
    
    # 复用 LLM选表链路的分页配置，保持一致性
    page_size = settings.llm_table_selection_page_size
    
    candidates = []
    for table in tables_with_fields[:fallback_count]:
        # 提取关键维度字段预览（最多5个）
        dimensions = getattr(table, 'dimensions', None) or []
        key_dims = []
        for f in dimensions[:5]:
            display_name = getattr(f, 'display_name', None) or getattr(f, 'name', '')
            if display_name:
                key_dims.append(display_name)
        
        # 提取关键度量字段预览（最多5个）
        measures = getattr(table, 'measures', None) or []
        key_measures = []
        for f in measures[:5]:
            display_name = getattr(f, 'display_name', None) or getattr(f, 'name', '')
            if display_name:
                key_measures.append(display_name)
        
        # 获取检索分数并归一化到 0-1 范围
        retrieval_score = getattr(table, 'retrieval_score', 0.0) or 0.0
        # 确保分数在有效范围内
        normalized_score = min(1.0, max(0.0, retrieval_score))
        
        # 构建候选表对象，包含用户确认后流程所需的所有信息
        candidate = TableCandidate(
            table_id=table.table_id,
            table_name=getattr(table, 'display_name', '') or getattr(table, 'name', '') or '',
            description=getattr(table, 'description', '') or '',
            confidence=normalized_score,
            reason=f"向量检索匹配度: {normalized_score:.1%}",
            tags=getattr(table, 'tags', None) or [],
            key_dimensions=key_dims,
            key_measures=key_measures,
            domain_name=getattr(table, 'domain_name', None),
            domain_id=str(getattr(table, 'domain_id', '')) if getattr(table, 'domain_id', None) else None,
            data_year=getattr(table, 'data_year', None)
        )
        candidates.append(candidate)
    
    # 按检索分数降序排序
    candidates.sort(key=lambda c: c.confidence, reverse=True)
    
    # 根据降级原因生成提示消息
    if fallback_reason == "ir_vote_failed":
        message = "系统对查询理解存在不确定性，请确认您要查询的数据表："
    elif fallback_reason == "low_confidence":
        message = "系统找到多个可能相关的表，请确认您要查询的是哪张表："
    else:
        message = "请确认您要查询的数据表："
    
    # 设置确认原因
    if fallback_reason == "ir_vote_failed":
        confirmation_reason = "AI 对查询结果的可靠性评估未通过，请确认数据表选择"
    elif fallback_reason == "low_confidence":
        confirmation_reason = "AI 对表选择的置信度较低，请帮助确认"
    else:
        confirmation_reason = "存在多个可能匹配的表，请选择最合适的"
    
    return TableSelectionCard(
        candidates=candidates,
        question=question,
        message=message,
        confirmation_reason=confirmation_reason,
        allow_multi_select=False,  # 向量链路降级默认单表选择
        page_size=page_size,
        total_candidates=len(candidates),
        # 跨年查询相关（向量链路降级时默认不启用）
        is_cross_year_query=False,
        cross_year_hint=None
    )


async def get_metadata_connection():
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


def custom_json_dumps(obj):
    """自定义 JSON 序列化，支持 date 类型"""
    def default(o):
        if isinstance(o, date):
            return o.isoformat()
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")
    return json.dumps(obj, default=default, ensure_ascii=False)


@router.get("/domains")
async def get_domains_for_query(
    connection_id: Optional[str] = Query(default=None, description="数据库连接ID")
) -> List[Dict[str, Any]]:
    """获取活跃的业务域列表（公开API，用于查询页面）

    按数据库连接返回业务域，无需认证
    """
    try:
        import asyncpg

        # 连接到元数据库
        conn = await asyncpg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            database=settings.postgres_db
        )

        try:
            # 查询business_domains表
            if connection_id:
                rows = await conn.fetch(
                    """
                    SELECT domain_id, domain_name, description, icon, keywords
                    FROM business_domains
                    WHERE connection_id = $1::uuid AND is_active = TRUE
                    ORDER BY domain_name
                    """,
                    connection_id
                )
            else:
                # 返回所有活跃的业务域
                rows = await conn.fetch(
                    """
                    SELECT domain_id, domain_name, description, icon, keywords
                    FROM business_domains
                    WHERE is_active = TRUE
                    ORDER BY domain_name
                    LIMIT 50
                    """
                )

            return [
                {
                    "domain_id": str(row["domain_id"]),
                    "domain_name": row["domain_name"],
                    "description": row["description"],
                    "icon": row["icon"],
                    "keywords": row["keywords"] or []
                }
                for row in rows
            ]
        finally:
            await conn.close()

    except Exception as e:
        logger.error("获取业务域列表失败", error=str(e))
        return []


@router.get("/query/connections", response_model=UserAccessibleConnectionsResponse)
async def get_accessible_connections_for_query(
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_metadata_connection)
):
    """
    获取当前登录用户可访问的数据库连接列表
    
    普通查询页面使用此接口根据数据角色过滤连接。
    """
    service = UserConnectionAccessService(db)
    return await service.get_accessible_connections(
        UUID(str(current_user.user_id)),
        current_user.role
    )


@router.get("/query/accessible-tables", response_model=AccessibleTablesResponse)
async def get_accessible_tables_for_query(
    connection_id: Optional[str] = Query(None, description="可选，筛选指定连接的表"),
    search: Optional[str] = Query(None, description="可选，搜索表名或描述"),
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_metadata_connection)
):
    """
    获取当前登录用户可访问的所有数据表
    
    用于查询页面"展开全部"功能，让用户可以浏览所有可选择的表。
    根据用户的数据角色权限进行过滤。
    """
    user_id = UUID(str(current_user.user_id))
    system_role = current_user.role
    is_admin = system_role == 'admin'
    
    # 检查是否有全量访问权限
    has_all_access_query = """
        SELECT EXISTS(
            SELECT 1 FROM user_data_roles udr
            JOIN data_roles dr ON udr.role_id = dr.role_id
            WHERE udr.user_id = $1 AND udr.is_active = TRUE AND dr.is_active = TRUE
            AND dr.scope_type = 'all'
        ) as has_all_access
    """
    result = await db.fetchrow(has_all_access_query, user_id)
    has_all_access = result['has_all_access'] if result else False
    
    # 构建基础查询
    base_query = """
        SELECT 
            t.table_id::text,
            t.display_name as table_name,
            t.schema_name,
            t.description,
            t.connection_id::text,
            dc.connection_name,
            t.domain_id::text,
            bd.domain_name,
            t.data_year,
            t.tags
        FROM db_tables t
        JOIN database_connections dc ON t.connection_id = dc.connection_id
        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
        WHERE t.is_included = TRUE AND dc.is_active = TRUE
    """
    
    params = []
    param_idx = 1
    
    # 根据权限添加过滤条件
    if is_admin or has_all_access:
        # 管理员或全量访问用户：可以看到所有表
        pass
    else:
        # 受限用户：只能看到有权限的表
        base_query += f"""
            AND t.table_id IN (
                SELECT DISTINCT rtp.table_id
                FROM user_data_roles udr
                JOIN data_roles dr ON udr.role_id = dr.role_id
                JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                WHERE udr.user_id = ${param_idx} AND udr.is_active = TRUE 
                AND dr.is_active = TRUE AND dr.scope_type = 'limited'
                AND rtp.can_query = TRUE
            )
        """
        params.append(user_id)
        param_idx += 1
    
    # 添加连接ID过滤
    if connection_id:
        base_query += f" AND t.connection_id = ${param_idx}::uuid"
        params.append(connection_id)
        param_idx += 1
    
    # 添加搜索过滤
    if search:
        base_query += f" AND (t.display_name ILIKE ${param_idx} OR t.description ILIKE ${param_idx})"
        params.append(f"%{search}%")
        param_idx += 1
    
    # 排序
    base_query += " ORDER BY dc.connection_name, t.display_name"
    
    rows = await db.fetch(base_query, *params)
    
    # 获取每个表的关键字段（批量查询以提高性能）
    table_ids = [row['table_id'] for row in rows]
    
    # 批量获取字段信息（fields 通过 db_columns 关联到表）
    fields_map = {}
    if table_ids:
        fields_query = """
            SELECT 
                dc.table_id::text,
                f.display_name,
                f.field_type
            FROM fields f
            JOIN db_columns dc ON f.source_column_id = dc.column_id
            WHERE dc.table_id = ANY($1::uuid[]) AND f.is_active = TRUE
            ORDER BY dc.table_id, f.priority DESC NULLS LAST, dc.ordinal_position
        """
        field_rows = await db.fetch(fields_query, table_ids)
        
        for field_row in field_rows:
            tid = field_row['table_id']
            if tid not in fields_map:
                fields_map[tid] = {'dimensions': [], 'measures': []}
            
            if field_row['field_type'] == 'dimension':
                if len(fields_map[tid]['dimensions']) < 3:  # 最多取3个
                    fields_map[tid]['dimensions'].append(field_row['display_name'])
            elif field_row['field_type'] == 'measure':
                if len(fields_map[tid]['measures']) < 3:  # 最多取3个
                    fields_map[tid]['measures'].append(field_row['display_name'])
    
    # 构建响应
    tables = []
    for row in rows:
        tid = row['table_id']
        field_info = fields_map.get(tid, {'dimensions': [], 'measures': []})
        
        tables.append(AccessibleTableItem(
            table_id=row['table_id'],
            table_name=row['table_name'] or '',
            schema_name=row['schema_name'],
            description=row['description'],
            connection_id=row['connection_id'],
            connection_name=row['connection_name'],
            domain_id=row['domain_id'],
            domain_name=row['domain_name'],
            data_year=row['data_year'],
            tags=row['tags'] or [],
            key_dimensions=field_info['dimensions'],
            key_measures=field_info['measures']
        ))
    
    return AccessibleTablesResponse(
        tables=tables,
        total=len(tables),
        is_admin=is_admin,
        has_all_access=has_all_access
    )


@router.post("/query")
async def query(
    request: QueryRequest,
    http_request: Request,
    cache=Depends(get_query_cache),
    current_user: Optional[AdminUser] = Depends(get_optional_user)
):
    """
    查询接口

    支持两种模式：
    1. 传入 text - 自然语言问题，系统自动解析
    2. 传入 ir - 直接提供中间表示

    核心特性：
    - 必须传入 connection_id 指定数据库连接
    - 动态创建 executor 和 compiler（根据 connection_id 和数据库类型）
    - 支持数据权限控制（需提供有效Token）
    """
    from server.utils.timezone_helper import to_isoformat, now_with_tz
    from server.models.api import AuthStatus
    import time

    stream = _stream_emitter_ctx.get(None)

    query_id = request.original_query_id or _query_id_ctx.get() or str(uuid.uuid4())
    timestamp = to_isoformat(now_with_tz())
    start_time = time.time()  # 记录开始时间
    result_streamed = False
    query_session_service = QuerySessionService()
    existing_session = None
    existing_state: Dict[str, Any] = {}
    query_uuid = _try_uuid(query_id)
    if query_uuid is not None:
        try:
            existing_session = await query_session_service.get_session(query_uuid)
            existing_state = dict(existing_session.get("state_json") or {}) if existing_session else {}
        except Exception as exc:
            logger.warning("读取既有 query_session 失败", query_id=query_id, error=str(exc))
    confirmation_mode = _resolve_confirmation_mode(
        request.confirmation_mode,
        existing_state.get("confirmation_mode"),
        settings.confirmation_mode,
    )
    revision_request = existing_state.get("revision_request")
    if request.ir and existing_state.get("resolved_question_text"):
        effective_question_text = existing_state.get("resolved_question_text")
    elif request.text and existing_state.get("draft_confirmation_required"):
        effective_question_text = _compose_question_with_revision(request.text, revision_request)
    else:
        effective_question_text = request.text
    
    # 构建认证状态信息
    auth_status = AuthStatus(
        authenticated=current_user is not None,
        auth_attempted=getattr(http_request.state, 'auth_attempted', False),
        error_code=getattr(http_request.state, 'auth_error_code', None),
        provider=getattr(http_request.state, 'auth_provider', None)
    )

    if stream:
        stream.bind_query(query_id)
    
    # 如果用户已认证，使用Token中的用户信息覆盖请求中的user_id
    actual_user_id = request.user_id
    actual_role = request.role
    if current_user:
        actual_user_id = str(current_user.user_id)
        actual_role = current_user.role
        logger.debug("使用Token认证的用户", user_id=actual_user_id, role=actual_role)

    session_user_id = _try_uuid(actual_user_id)
    session_conversation_id = _try_uuid(request.conversation_id)
    session_message_id = _try_uuid(_message_id_ctx.get() or request.message_id)

    async def _write_query_session(
        *,
        status: str,
        current_node: str,
        state_updates: Optional[Dict[str, Any]] = None,
        last_error: Optional[str] = None,
    ) -> None:
        try:
            await query_session_service.upsert_session(
                query_id=UUID(query_id),
                user_id=session_user_id,
                conversation_id=session_conversation_id,
                message_id=session_message_id,
                status=status,
                current_node=current_node,
                state_json=state_updates,
                last_error=last_error,
            )
        except Exception as exc:
            logger.warning("写入 query_sessions 失败", query_id=query_id, error=str(exc))

    async def _update_query_session(
        *,
        status: Optional[str] = None,
        current_node: Optional[str] = None,
        state_updates: Optional[Dict[str, Any]] = None,
        last_error: Optional[str] = None,
    ) -> None:
        try:
            await query_session_service.update_session(
                UUID(query_id),
                status=status,
                current_node=current_node,
                state_updates=state_updates,
                conversation_id=session_conversation_id,
                message_id=session_message_id,
                last_error=last_error,
            )
        except Exception as exc:
            logger.warning("更新 query_sessions 失败", query_id=query_id, error=str(exc))

    async def _return_query_response(
        response: QueryResponse,
        *,
        status: str,
        current_node: str,
        state_updates: Optional[Dict[str, Any]] = None,
        last_error: Optional[str] = None,
    ):
        await _update_query_session(
            status=status,
            current_node=current_node,
            state_updates=state_updates,
            last_error=last_error,
        )
        return response

    async def _return_json_query_response(
        response: QueryResponse,
        *,
        status: str,
        current_node: str,
        state_updates: Optional[Dict[str, Any]] = None,
        last_error: Optional[str] = None,
    ):
        await _update_query_session(
            status=status,
            current_node=current_node,
            state_updates=state_updates,
            last_error=last_error,
        )
        return JSONResponse(
            content=json.loads(response.model_dump_json()),
            media_type="application/json"
        )

    await _write_query_session(
        status="running",
        current_node="question_intake",
        state_updates={
            "question_text": request.text,
            "resolved_question_text": effective_question_text,
            "confirmation_mode": confirmation_mode,
            "pending_actions": [],
            "selected_table_ids": request.get_selected_table_ids(),
            "request_context": {
                "connection_id": request.connection_id,
                "domain_id": request.domain_id,
                "skip_cache": request.skip_cache,
                "force_execute": request.force_execute,
                "explain_only": request.explain_only,
                "confirmation_mode": confirmation_mode,
            },
        },
    )

    # 创建或恢复查询追踪器
    # 如果请求带有 original_query_id（用户确认表选择后的续接请求），
    # 则恢复原始 tracer 继续追踪，确保完整流程记录在同一个日志文件中
    tracer = get_or_resume_tracer(
        query_id=query_id,
        question=effective_question_text or request.text or "(直接提供IR)",
        connection_id=request.connection_id,
        original_query_id=request.original_query_id
    )

    logger.info(
        "收到查询请求",
        query_id=query_id,
        user_id=actual_user_id,
        authenticated=current_user is not None,
        connection_id=request.connection_id,
        has_text=bool(request.text),
        has_ir=bool(request.ir)
    )

    # ========== 第1步：LLM 表选择（直接从数据库，不需要 semantic_model） ==========
    # 这一步会确定 connection_id 和 selected_table_id
    actual_connection_id = request.connection_id
    # 支持多表选择：优先使用 selected_table_ids，向后兼容 selected_table_id
    selected_table_ids = request.get_selected_table_ids()
    effective_selected_table_id = selected_table_ids[0] if selected_table_ids else None
    candidate_snapshot = None

    # 跨分区查询标志（跨年UNION/对比等场景）
    is_cross_partition_query = False
    cross_partition_mode = None

    # 如果选择了多表，说明用户确认了跨年/跨分区查询
    if len(selected_table_ids) > 1:
        is_cross_partition_query = True
        # 优先使用请求中指定的模式，否则默认使用union模式
        cross_partition_mode = request.multi_table_mode or "union"
        logger.info(
            "多表选择模式（用户确认的跨年/跨分区查询）",
            query_id=query_id,
            selected_table_count=len(selected_table_ids),
            primary_table_id=effective_selected_table_id,
            all_table_ids=selected_table_ids,
            is_cross_partition_query=is_cross_partition_query,
            cross_partition_mode=cross_partition_mode
        )
    
    # 导入统一的 LLM 表选择函数
    from server.api.query.table_selection import llm_select_table
    
    # 情况0：用户已选择表，从数据库获取 connection_id
    if effective_selected_table_id and not actual_connection_id:
        try:
            pool = await get_metadata_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT connection_id, display_name 
                    FROM db_tables 
                    WHERE table_id = $1
                """, UUID(effective_selected_table_id))
                if row:
                    actual_connection_id = str(row["connection_id"])
                    logger.info(
                        "从已选表获取数据库连接",
                        query_id=query_id,
                        selected_table_id=effective_selected_table_id,
                        selected_table_name=row["display_name"],
                        connection_id=actual_connection_id
                    )
        except Exception as e:
            logger.warning("从已选表获取连接失败", error=str(e), table_id=effective_selected_table_id)
    
    # 情况1：未指定 connection_id，需要自动检测或跨连接选表
    if not actual_connection_id:
        try:
            # 先尝试自动检测（用户只有一个连接的情况）
            actual_connection_id = await _auto_detect_connection(
                question=effective_question_text,
                user_id=actual_user_id,
                user_role=actual_role,
                domain_id=request.domain_id
            )
            if actual_connection_id:
                logger.info(
                    "自动检测到数据库连接",
                    query_id=query_id,
                    detected_connection_id=actual_connection_id
                )
                # 自动检测成功后，如果未选择表且启用了 LLM 选表，进行单连接 LLM 选表
                if settings.llm_table_selection_enabled and effective_question_text and not effective_selected_table_id:
                    logger.info("单连接 LLM 表选择（自动检测连接后）", query_id=query_id, connection_id=actual_connection_id)
                    
                    table_selection_result = await llm_select_table(
                        question=effective_question_text,
                        user_id=actual_user_id,
                        user_role=actual_role,
                        connection_id=actual_connection_id,  # 单连接模式
                        domain_id=request.domain_id,
                        selected_table_id=None,
                        tracer=tracer,
                        stream=stream,
                        query_id=query_id,
                        timestamp=timestamp
                    )
                    
                    if table_selection_result.get("status") == "need_confirmation":
                        tracer.finalize({"status": "table_selection_needed"}, save_to_file=False)
                        pending_response = table_selection_result.get("response")
                        pending_card = pending_response.table_selection if pending_response else None
                        await _update_query_session(
                            status="awaiting_user_action",
                            current_node="table_resolution",
                            state_updates={
                                "pending_actions": ["confirm", "change_table", "request_explanation", "exit_current"],
                                "candidate_snapshot": pending_card.model_dump() if pending_card else {},
                                "recommended_table_ids": list(getattr(pending_card, "recommended_table_ids", []) or []),
                                "selected_table_ids": selected_table_ids,
                            },
                        )
                        return pending_response
                    elif table_selection_result.get("status") == "success":
                        effective_selected_table_id = table_selection_result.get("selected_table_id")
                        candidate_snapshot = table_selection_result.get("candidate_snapshot")
                        # 获取多表选择结果（跨年查询时可能有多个表）
                        result_table_ids = table_selection_result.get("selected_table_ids", [])
                        if result_table_ids:
                            selected_table_ids = result_table_ids
                        elif effective_selected_table_id:
                            selected_table_ids = [effective_selected_table_id]
                        # 获取跨分区查询标志
                        if table_selection_result.get("is_multi_table_query"):
                            is_cross_partition_query = True
                            cross_partition_mode = table_selection_result.get("multi_table_mode")
                        # 使用表选择返回的 connection_id（可能从表元数据中获取，更准确）
                        returned_connection_id = table_selection_result.get("connection_id")
                        if returned_connection_id:
                            actual_connection_id = returned_connection_id
                        logger.info(
                            "单连接 LLM 表选择成功（自动检测连接后）",
                            query_id=query_id,
                            selected_table=table_selection_result.get("selected_table"),
                            selected_table_id=effective_selected_table_id,
                            selected_table_ids=selected_table_ids,
                            connection_id=actual_connection_id,
                            is_multi_table=table_selection_result.get("is_multi_table_query", False),
                            cross_partition_mode=cross_partition_mode
                        )
                    elif table_selection_result.get("status") != "skipped":
                        error_info = table_selection_result.get("error", {
                            "code": "TABLE_SELECTION_FAILED",
                            "message": "LLM 表选择失败"
                        })
                        await _stream_error(stream, error_info)
                        tracer.finalize()
                        return await _return_query_response(
                            QueryResponse(
                            status="error",
                            error=error_info,
                            auth_status=auth_status,
                            query_id=query_id,
                            timestamp=timestamp
                            ),
                            status="failed",
                            current_node="table_resolution",
                            state_updates={"selected_table_ids": selected_table_ids},
                            last_error=error_info.get("message"),
                        )
            elif settings.llm_table_selection_enabled and effective_question_text:
                # 跨连接 LLM 表选择（仅当用户未选择表时）
                logger.info("尝试通过 LLM 表选择确定数据库连接", query_id=query_id)
                
                table_selection_result = await llm_select_table(
                    question=effective_question_text,
                    user_id=actual_user_id,
                    user_role=actual_role,
                    connection_id=None,  # 跨连接模式
                    domain_id=request.domain_id,
                    selected_table_id=effective_selected_table_id,
                    tracer=tracer,
                    stream=stream,
                    query_id=query_id,
                    timestamp=timestamp
                )
                
                if table_selection_result.get("status") == "need_confirmation":
                    # 暂停 trace（不立即保存，等待用户确认后续接继续追踪）
                    tracer.finalize({"status": "table_selection_needed"}, save_to_file=False)
                    pending_response = table_selection_result.get("response")
                    pending_card = pending_response.table_selection if pending_response else None
                    await _update_query_session(
                        status="awaiting_user_action",
                        current_node="table_resolution",
                        state_updates={
                            "pending_actions": ["confirm", "change_table", "request_explanation", "exit_current"],
                            "candidate_snapshot": pending_card.model_dump() if pending_card else {},
                            "recommended_table_ids": list(getattr(pending_card, "recommended_table_ids", []) or []),
                            "selected_table_ids": selected_table_ids,
                        },
                    )
                    return pending_response
                elif table_selection_result.get("status") == "success":
                    actual_connection_id = table_selection_result.get("connection_id")
                    effective_selected_table_id = table_selection_result.get("selected_table_id")
                    candidate_snapshot = table_selection_result.get("candidate_snapshot")
                    # 获取多表选择结果（跨年查询时可能有多个表）
                    result_table_ids = table_selection_result.get("selected_table_ids", [])
                    if result_table_ids:
                        selected_table_ids = result_table_ids
                    elif effective_selected_table_id:
                        selected_table_ids = [effective_selected_table_id]
                    # 获取跨分区查询标志
                    if table_selection_result.get("is_multi_table_query"):
                        is_cross_partition_query = True
                        cross_partition_mode = table_selection_result.get("multi_table_mode")
                    logger.info(
                        "通过 LLM 表选择确定数据库连接",
                        query_id=query_id,
                        detected_connection_id=actual_connection_id,
                        selected_table=table_selection_result.get("selected_table"),
                        selected_table_id=effective_selected_table_id,
                        selected_table_ids=selected_table_ids,
                        is_multi_table=table_selection_result.get("is_multi_table_query", False),
                        cross_partition_mode=cross_partition_mode
                    )
                else:
                    error_info = table_selection_result.get("error", {
                        "code": "TABLE_SELECTION_FAILED",
                        "message": "LLM 表选择失败"
                    })
                    await _stream_error(stream, error_info)
                    tracer.finalize()
                    return await _return_query_response(
                        QueryResponse(
                        status="error",
                        error=error_info,
                        auth_status=auth_status,
                        query_id=query_id,
                        timestamp=timestamp
                        ),
                        status="failed",
                        current_node="table_resolution",
                        state_updates={"selected_table_ids": selected_table_ids},
                        last_error=error_info.get("message"),
                    )
            else:
                # LLM表选择关闭时，不要直接报错，让第二条链路（向量检索+LLM3）来处理
                # 第二条链路会在后面的代码中执行向量检索并确定连接
                logger.info(
                    "无法自动检测数据库连接，将通过第二条链路（向量检索+LLM3）处理",
                    query_id=query_id
                )
        except Exception as e:
            logger.error("自动检测连接失败", error=str(e))
            tracer.finalize()
            error_info = {"code": "AUTO_DETECT_ERROR", "message": f"自动检测数据库连接失败: {str(e)}"}
            await _stream_error(stream, error_info)
            return await _return_query_response(
                QueryResponse(
                status="error",
                error=error_info,
                auth_status=auth_status,
                query_id=query_id,
                timestamp=timestamp
                ),
                status="failed",
                current_node="table_resolution",
                state_updates={"selected_table_ids": selected_table_ids},
                last_error=error_info["message"],
            )
    
    # 情况2：已指定 connection_id，但未选择表，需要单连接 LLM 表选择
    elif settings.llm_table_selection_enabled and effective_question_text and not effective_selected_table_id:
        logger.info("单连接 LLM 表选择", query_id=query_id, connection_id=actual_connection_id)
        
        table_selection_result = await llm_select_table(
            question=effective_question_text,
            user_id=actual_user_id,
            user_role=actual_role,
            connection_id=actual_connection_id,  # 单连接模式
            domain_id=request.domain_id,
            selected_table_id=None,
            tracer=tracer,
            stream=stream,
            query_id=query_id,
            timestamp=timestamp
        )
        
        if table_selection_result.get("status") == "need_confirmation":
            # 暂停 trace（不立即保存，等待用户确认后续接继续追踪）
            tracer.finalize({"status": "table_selection_needed"}, save_to_file=False)
            pending_response = table_selection_result.get("response")
            pending_card = pending_response.table_selection if pending_response else None
            await _update_query_session(
                status="awaiting_user_action",
                current_node="table_resolution",
                state_updates={
                    "pending_actions": ["confirm", "change_table", "request_explanation", "exit_current"],
                    "candidate_snapshot": pending_card.model_dump() if pending_card else {},
                    "recommended_table_ids": list(getattr(pending_card, "recommended_table_ids", []) or []),
                    "selected_table_ids": selected_table_ids,
                },
            )
            return pending_response
        elif table_selection_result.get("status") == "success":
            effective_selected_table_id = table_selection_result.get("selected_table_id")
            candidate_snapshot = table_selection_result.get("candidate_snapshot")
            # 获取多表选择结果（跨年查询时可能有多个表）
            result_table_ids = table_selection_result.get("selected_table_ids", [])
            if result_table_ids:
                selected_table_ids = result_table_ids
            elif effective_selected_table_id:
                selected_table_ids = [effective_selected_table_id]
            # 获取跨分区查询标志
            if table_selection_result.get("is_multi_table_query"):
                is_cross_partition_query = True
                cross_partition_mode = table_selection_result.get("multi_table_mode")
            # 使用表选择返回的 connection_id（可能从表元数据中获取，更准确）
            returned_connection_id = table_selection_result.get("connection_id")
            if returned_connection_id:
                actual_connection_id = returned_connection_id
            logger.info(
                "单连接 LLM 表选择成功",
                query_id=query_id,
                selected_table=table_selection_result.get("selected_table"),
                selected_table_id=effective_selected_table_id,
                selected_table_ids=selected_table_ids,
                connection_id=actual_connection_id,
                is_multi_table=table_selection_result.get("is_multi_table_query", False),
                cross_partition_mode=cross_partition_mode
            )
        elif table_selection_result.get("status") != "skipped":
            error_info = table_selection_result.get("error", {
                "code": "TABLE_SELECTION_FAILED",
                "message": "LLM 表选择失败"
            })
            await _stream_error(stream, error_info)
            tracer.finalize()
            return await _return_query_response(
                QueryResponse(
                status="error",
                error=error_info,
                auth_status=auth_status,
                query_id=query_id,
                timestamp=timestamp
                ),
                status="failed",
                current_node="table_resolution",
                state_updates={"selected_table_ids": selected_table_ids},
                last_error=error_info.get("message"),
            )
    
    # 更新 tracer 的 connection_id
    tracer.connection_id = actual_connection_id
    table_resolution_state_updates = {
        "connection_id": actual_connection_id,
        "selected_table_ids": selected_table_ids,
        "selected_table_id": effective_selected_table_id,
        "cross_partition_mode": cross_partition_mode,
        "is_cross_partition_query": is_cross_partition_query,
        "recommended_table_ids": selected_table_ids,
    }
    if candidate_snapshot:
        table_resolution_state_updates["candidate_snapshot"] = candidate_snapshot
    await _update_query_session(
        status="running",
        current_node="table_resolved",
        state_updates=table_resolution_state_updates,
    )

    # ========== 记录表选择状态 ==========
    # 如果用户请求中已带有 selected_table_id，说明这是用户确认LLM表选择结果后的请求
    # 需要记录表的基本信息，以便追踪
    if request.selected_table_id and effective_selected_table_id:
        step_name = "使用已选表"
        step_desc = "使用用户从LLM表选择结果中确认的数据表"
        table_step = tracer.start_step(step_name, "table_selection", step_desc)
        
        # 尝试获取表的基本信息
        table_info = {}
        try:
            # get_metadata_pool 已在文件顶部导入
            pool = await get_metadata_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT 
                        t.display_name,
                        t.description,
                        t.tags,
                        bd.domain_name
                    FROM db_tables t
                    LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                    WHERE t.table_id = $1
                """, UUID(effective_selected_table_id))
                if row:
                    table_info = {
                        "table_name": row["display_name"],
                        "description": row["description"],
                        "domain": row["domain_name"],
                        "tags": row["tags"] or []
                    }
        except Exception as e:
            logger.warning("获取已选表信息失败", error=str(e))
        
        table_step.set_input({
            "selected_table_id": effective_selected_table_id,
            "source": "user_confirmed_from_llm_selection",
            "original_query_id": request.original_query_id
        })
        table_step.set_output({
            "action": "use_confirmed",
            "table_id": effective_selected_table_id,
            **table_info
        })
        
        # 检查是否需要嵌入原始 LLM 表选择信息
        # 如果 tracer 是从原始请求恢复的（同一个 tracer），则已经包含 LLM 表选择步骤，无需重复嵌入
        has_llm_selection_step = any(
            s.step_type == "table_selection" and "LLM" in s.step_name 
            for s in tracer.steps[:-1]  # 排除当前步骤
        )
        
        if request.original_query_id and not has_llm_selection_step:
            # 原始 tracer 未能恢复（可能已被清理），记录提示
            table_step.add_metadata("original_trace_note", 
                f"原始 LLM 表选择 trace 不在内存中，请查看 trace 文件: query_id={request.original_query_id}")
        
        tracer.end_step()
        await _stream_progress(stream, step_name, "success", step_desc, {
            "table_id": effective_selected_table_id,
            "table_name": table_info.get("table_name")
        })

    # ========== 延迟初始化：所有组件在需要时才创建 ==========
    executor = None
    compiler = None
    semantic_model = None
    parser = None
    formatter = None
    validation_loop = None
    
    # 第二条链路相关变量初始化
    ir_from_llm3 = False  # 标识IR是否由LLM3生成
    global_semantic_model = None  # 全局语义模型（跨连接检索时使用）
    global_retrieval_result = None  # 全局检索结果（用于连接解析提示）

    async def ensure_compiler():
        """延迟创建 SQL 编译器"""
        nonlocal compiler
        if compiler is None:
            compiler = await create_sql_compiler(actual_connection_id)
            if compiler is None:
                raise RuntimeError(f"无法创建SQL编译器: connection_id={actual_connection_id}")
            logger.debug("SQL编译器创建成功（延迟初始化）", connection_id=actual_connection_id)
        return compiler

    async def ensure_executor():
        """延迟创建查询执行器"""
        nonlocal executor
        if executor is None:
            executor = await create_query_executor(actual_connection_id)
            if executor is None:
                raise RuntimeError(f"无法创建查询执行器: connection_id={actual_connection_id}")
            logger.debug("查询执行器创建成功（延迟初始化）", connection_id=actual_connection_id)
        return executor

    async def ensure_semantic_model():
        """延迟加载语义模型"""
        nonlocal semantic_model, formatter
        if semantic_model is None:
            manager = get_metadata_manager()
            semantic_model = await manager.get_connection_model(actual_connection_id)
            # ResultFormatter 已在文件顶部导入
            formatter = ResultFormatter(semantic_model)
            logger.debug("语义模型加载成功（延迟初始化）", connection_id=actual_connection_id)
        return semantic_model

    async def ensure_parser():
        """延迟创建 NL2IR 解析器"""
        nonlocal parser, validation_loop
        if parser is None:
            parser = await create_nl2ir_parser(actual_connection_id)
            validation_loop = get_validation_loop()
            logger.debug("NL2IR 解析器创建成功（延迟初始化）", connection_id=actual_connection_id)
        return parser

    try:
        async def _schedule_few_shot_learning(
            sql_text: Optional[str],
            ir_obj: Optional[IntermediateRepresentation],
            query_result: Optional[QueryResult]
        ) -> Dict[str, Any]:
            import asyncio
            
            info = {
                "scheduled": False,
                "auto_sync": settings.few_shot_immediate_sync,
            }

            if not settings.few_shot_enabled:
                info["reason"] = "feature_disabled"
                return info
            if not effective_question_text or not sql_text:
                info["reason"] = "missing_question_or_sql"
                return info
            if not parser or not getattr(parser, "last_retrieval_summary", None):
                info["reason"] = "retrieval_summary_unavailable"
                return info
            if ir_obj is None or query_result is None:
                info["reason"] = "missing_ir_or_result"
                return info

            async def _writer_task():
                await _few_shot_writer.record_successful_query(
                    connection_id=actual_connection_id,
                    question=effective_question_text,
                    sql_text=sql_text,
                    retrieval_summary=parser.last_retrieval_summary,
                    domain_id=getattr(ir_obj, "domain_id", None),
                    confidence=confidence,
                    ir=ir_obj,
                )

            asyncio.create_task(_writer_task())
            info["scheduled"] = True
            return info

        # ========== 第2步：NL2IR 解析（此时才加载 semantic_model 和 parser） ==========
        if request.ir:
            # 用户直接提供 IR，只需加载 semantic_model 用于后续处理
            step_name = "使用直接IR"
            step_desc = "用户直接提供IR，跳过NL2IR解析"
            step = tracer.start_step(step_name, "parsing", step_desc)
            await _stream_progress(stream, step_name, "started", step_desc, None)
            step.set_input({"ir": request.ir.model_dump()})

            ir = request.ir
            confidence = 1.0
            logger.debug("使用直接提供的 IR")

            # 延迟加载语义模型（后续格式化需要）
            semantic_model = await ensure_semantic_model()

            step.set_output({"confidence": confidence})
            tracer.end_step()
            await _stream_progress(stream, step_name, "success", step_desc, {"confidence": confidence})
        elif effective_question_text:
            # ========== 第二条链路：向量检索 + LLM3选表+IR生成（当 LLM 表选择关闭时）==========
            # 当 LLM_TABLE_SELECTION_ENABLED=false 时，使用向量检索召回TOP-K表，
            # 然后由LLM3同时完成选表和IR生成，根据置信度决定分流
            if (not settings.llm_table_selection_enabled 
                and not effective_selected_table_id):
                
                step_name = "向量表选择"
                step_desc = "向量检索并智能选表"
                table_step = tracer.start_step(step_name, "vector_table_selection", step_desc)
                await _stream_progress(stream, step_name, "started", step_desc, {"question": effective_question_text})
                # 思考过程：开始检索
                await _stream_thinking(stream, "table_selection", f"正在分析问题：「{effective_question_text[:50]}{'...' if len(effective_question_text) > 50 else ''}」\n正在检索相关数据表...", done=False, step_status="started")
                
                try:
                    # 1. 加载全局语义模型和检索器（跨连接模式）
                    # 由于此时没有确定 connection_id，需要使用全局模型
                    from server.nl2ir.hierarchical_retriever import HierarchicalRetriever
                    from server.nl2ir.domain_detector import DomainDetector
                    
                    manager = get_metadata_manager()
                    global_semantic_model = await manager.get_global_model()
                    
                    # 创建全局域检测器（不绑定特定连接）
                    global_domain_detector = DomainDetector(global_semantic_model, connection_id=None)
                    try:
                        await global_domain_detector.initialize(manager, connection_id=None)
                        logger.debug("全局业务域检测器已初始化")
                    except Exception as e:
                        logger.warning("全局业务域检测器初始化失败", error=str(e))
                    
                    # 创建全局层次化检索器
                    global_retriever = None
                    try:
                        db_pool = await get_metadata_pool()
                        
                        milvus_client = None
                        embedding_client = None
                        if settings.milvus_enabled:
                            from pymilvus import MilvusClient
                            from server.utils.model_clients import EmbeddingClient
                            milvus_client = MilvusClient(uri=settings.milvus_uri, token=settings.milvus_token)
                            embedding_client = EmbeddingClient(
                                base_url=settings.embedding_base_url,
                                api_key=settings.embedding_api_key,
                                model=settings.embedding_model,
                                timeout=settings.embedding_timeout
                            )
                        
                        global_retriever = HierarchicalRetriever(
                            semantic_model=global_semantic_model,
                            domain_detector=global_domain_detector,
                            milvus_client=milvus_client,
                            embedding_client=embedding_client,
                            connection_id=None,  # 跨连接模式
                            db_pool=db_pool
                        )
                        logger.debug("全局层次化检索器创建成功")
                    except Exception as e:
                        logger.warning("全局层次化检索器创建失败", error=str(e))
                    
                    # 2. 执行向量检索
                    # 使用 RetrievalConfig.table_top_k() 与测试脚本保持一致
                    # 确保召回足够多的候选表供后续打分增强
                    top_k = RetrievalConfig.table_top_k()
                    
                    if global_retriever:
                        retrieval_result = await global_retriever.retrieve(
                            question=effective_question_text,
                            user_domain_id=request.domain_id,
                            top_k_tables=top_k,
                            global_rules=None,
                            user_id=actual_user_id,
                            user_role=actual_role
                        )
                    else:
                        # 如果全局检索器创建失败，创建空结果
                        from server.nl2ir.hierarchical_retriever import HierarchicalRetrievalResult
                        retrieval_result = HierarchicalRetrievalResult(
                            domain_id=None, domain_name=None,
                            table_results=[], table_structures=[],
                            prompt_context=""
                        )
                    
                    table_results = retrieval_result.table_results or []
                    
                    # ========== 步骤2.1: 枚举值智能匹配（与测试脚本一致）==========
                    # 在 llm_top_k 截断之前执行，确保枚举加成能影响排序
                    enum_matches = []
                    reranker_client = None
                    try:
                        # 初始化 Reranker 客户端（用于枚举检索器）
                        from server.utils.model_clients import RerankerClient
                        reranker_client = RerankerClient()
                        reranker_enabled = reranker_client.is_enabled()
                        
                        # 初始化枚举检索器
                        from server.nl2ir.enum_value_retriever import DualVectorEnumRetriever
                        enum_retriever = DualVectorEnumRetriever(
                            db_pool=db_pool,
                            milvus_client=milvus_client,
                            embedding_client=embedding_client,
                            collection_name=settings.milvus_enum_collection,
                            connection_id=None,  # 全局模式
                            reranker=reranker_client if reranker_enabled else None,
                        )
                        
                        # 从候选表中提取字段用于枚举匹配
                        candidate_fields = []
                        if retrieval_result.table_structures:
                            for table_struct in retrieval_result.table_structures:
                                candidate_fields.extend(getattr(table_struct, "dimensions", []) or [])
                                candidate_fields.extend(getattr(table_struct, "identifiers", []) or [])
                        
                        if candidate_fields:
                            enum_matches = await enum_retriever.match_enum_values(
                                user_input=effective_question_text,
                                candidate_fields=candidate_fields,
                                top_k=5,
                                keyword_profile=getattr(retrieval_result, "keyword_profile", None),
                            )
                            if enum_matches:
                                logger.info(
                                    "向量选表链路枚举匹配完成",
                                    enum_count=len(enum_matches),
                                    top_matches=[
                                        {"field": m.field_name, "value": m.value, "score": round(m.final_score, 3)}
                                        for m in enum_matches[:3]
                                    ]
                                )
                    except Exception as e:
                        logger.warning("向量选表链路枚举匹配失败（忽略）", error=str(e))
                        enum_matches = []
                    
                    # ========== 步骤2.2: 动态打分阶段（枚举加成+度量加成+年份加成等）==========
                    # 与测试脚本一致：无论是否命中枚举，都执行打分增强
                    if retrieval_result.table_results and global_retriever:
                        try:
                            retrieval_result = await global_retriever.apply_enum_boost_and_rerank(
                                result=retrieval_result,
                                enum_matches=enum_matches or [],
                                question=effective_question_text,
                                global_rules=None,
                            )
                            logger.info(
                                "向量选表链路打分增强完成",
                                table_count=len(retrieval_result.table_results) if retrieval_result.table_results else 0,
                                top_scores=[
                                    {"table": t.datasource.display_name or t.datasource.name, "score": round(t.score, 4)}
                                    for t in (retrieval_result.table_results or [])[:3]
                                ]
                            )
                        except Exception as e:
                            logger.warning("向量选表链路打分增强失败（忽略）", error=str(e))
                    
                    # ========== 步骤2.3: 应用 llm_top_k 截断（仅截断 prompt_context 和 table_structures）==========
                    # 注意：apply_llm_top_k_truncation 保留完整的 table_results，
                    # 以支持 fallback_candidate_count 配置（用于用户确认卡）
                    if global_retriever and hasattr(global_retriever, 'apply_llm_top_k_truncation'):
                        retrieval_result = global_retriever.apply_llm_top_k_truncation(
                            result=retrieval_result,
                            question=effective_question_text,
                            global_rules=None,
                            enum_matches=enum_matches,
                        )
                    
                    # 获取配置参数
                    llm_top_k = get_retrieval_param("table_retrieval.llm_top_k", None)
                    fallback_candidate_count = get_retrieval_param(
                        "vector_table_selection.fallback_candidate_count", 5
                    )
                    
                    # 完整的表结果（用于加载字段信息和构建 fallback 卡）
                    table_results = retrieval_result.table_results or []
                    
                    # 保存检索结果供后续连接解析使用
                    global_retrieval_result = retrieval_result
                    
                    # === 输出思考过程：向量检索结果 ===
                    
                    # 从检索结果获取连接ID
                    if hasattr(retrieval_result, 'detected_connection_id') and retrieval_result.detected_connection_id:
                        actual_connection_id = retrieval_result.detected_connection_id
                        # 更新 tracer 的 connection_id
                        tracer.connection_id = actual_connection_id
                        logger.info(
                            "从向量检索结果检测到连接",
                            connection_id=actual_connection_id,
                            involved_connections=getattr(retrieval_result, 'involved_connections', {})
                        )
                    
                    if not table_results:
                        # 无检索结果，无法继续
                        logger.warning("向量检索无结果，无法确定数据表")
                        table_step.set_output({"table_count": 0, "error": "no_tables_found"})
                        tracer.end_step()
                        
                        # 返回错误响应
                        tracer.finalize({"status": "error", "reason": "no_tables_found"})
                        return await _return_query_response(
                            QueryResponse(
                            status="error",
                            error={
                                "code": "NO_TABLES_FOUND",
                                "message": "未找到相关数据表，请检查问题描述或确保已同步元数据。"
                            },
                            auth_status=auth_status,
                            query_id=query_id,
                            timestamp=timestamp
                            ),
                            status="failed",
                            current_node="table_resolution",
                            last_error="未找到相关数据表，请检查问题描述或确保已同步元数据。",
                        )
                    else:
                        # 3. 加载表的完整字段信息
                        # 只加载需要的表数量：max(llm_top_k, fallback_candidate_count)
                        # 避免加载过多表的字段信息以提升性能
                        from server.nl2ir.vector_table_selector import (
                            VectorTableSelector,
                            load_tables_with_full_fields,
                            TableWithFields
                        )
                        
                        # 计算需要加载字段信息的表数量
                        tables_to_load_count = max(
                            llm_top_k or len(table_results),
                            fallback_candidate_count
                        )
                        tables_for_field_loading = table_results[:tables_to_load_count]
                        
                        logger.info(
                            "向量选表链路加载表字段信息",
                            total_table_results=len(table_results),
                            llm_top_k=llm_top_k,
                            fallback_candidate_count=fallback_candidate_count,
                            tables_to_load=len(tables_for_field_loading),
                        )
                        
                        tables_with_fields = await load_tables_with_full_fields(
                            tables_for_field_loading, 
                            global_semantic_model
                        )
                        
                        if not tables_with_fields:
                            logger.warning("加载表字段信息失败，无法继续")
                            table_step.set_output({"table_count": len(table_results), "field_load_failed": True})
                            tracer.end_step()
                            
                            # 返回错误响应
                            tracer.finalize({"status": "error", "reason": "field_load_failed"})
                            return await _return_query_response(
                                QueryResponse(
                                status="error",
                                error={
                                    "code": "FIELD_LOAD_FAILED",
                                    "message": "加载表字段信息失败，请稍后重试。"
                                },
                                query_id=query_id,
                                timestamp=timestamp
                                ),
                                status="failed",
                                current_node="table_resolution",
                                last_error="加载表字段信息失败，请稍后重试。",
                            )
                        else:
                            # 4. 构建检索辅助信息（使用步骤2.1获取的enum_matches）
                            retrieval_hints = {}
                            if enum_matches:
                                retrieval_hints["enum_matches"] = [
                                    {
                                        "field_name": getattr(m, "field_name", ""),
                                        "value": getattr(m, "value", ""),
                                        "score": round(getattr(m, "final_score", 0), 3),
                                        "match_type": getattr(m, "match_type", ""),
                                        "table_name": getattr(m, "table_name", None),
                                    }
                                    for m in enum_matches[:5]
                                ]
                            
                            # 4.0 记录检索详情到 trace（补充检索摘要信息）
                            retrieval_summary = {
                                "table_count": len(table_results),
                                "enum_match_count": len(enum_matches) if enum_matches else 0,
                                "top_tables": [
                                    {
                                        "table_name": t.datasource.display_name or t.datasource.name,
                                        "score": round(t.score, 4),
                                        "table_id": t.table_id[:8] + "..." if t.table_id else None
                                    }
                                    for t in table_results[:5]
                                ],
                            }
                            if enum_matches:
                                retrieval_summary["top_enum_matches"] = [
                                    {
                                        "field": getattr(m, "field_name", ""),
                                        "value": getattr(m, "value", ""),
                                        "score": round(getattr(m, "final_score", 0), 3),
                                        "match_type": getattr(m, "match_type", ""),
                                        "table": getattr(m, "table_name", None),
                                    }
                                    for m in enum_matches[:5]
                                ]
                            table_step.add_metadata("retrieval_summary", retrieval_summary)
                            
                            # 4.1 加载全局规则
                            # 规则来源：1. 配置文件 global_rules.yaml（始终加载）
                            #          2. 数据库 global_rules 表（如果有连接ID）
                            global_rules = []
                            detected_conn_id = getattr(retrieval_result, 'detected_connection_id', None) or actual_connection_id
                            try:
                                from server.utils.global_rules_loader import GlobalRulesLoader, _load_rules_from_config
                                
                                if detected_conn_id:
                                    # 有连接ID时，从数据库和配置文件合并加载
                                    conn_uuid = UUID(detected_conn_id) if isinstance(detected_conn_id, str) else detected_conn_id
                                    rules_loader = GlobalRulesLoader(conn_uuid)
                                    global_rules = await rules_loader.load_active_rules(
                                        rule_types=['derived_metric', 'custom_instruction']
                                    )
                                else:
                                    # 无连接ID时，仅从配置文件加载（确保武汉市展开等规则可用）
                                    all_config_rules = _load_rules_from_config()
                                    global_rules = [
                                        r for r in all_config_rules 
                                        if r.get('rule_type') in ['derived_metric', 'custom_instruction']
                                    ]
                                
                                logger.debug("向量表选择器加载全局规则", rule_count=len(global_rules))
                            except Exception as e:
                                logger.warning("加载全局规则失败", error=str(e))
                            
                            # 4.2 加载 Few-Shot 示例（如果有检测到的连接ID且功能开启）
                            few_shot_examples = []
                            if detected_conn_id and RetrievalConfig.few_shot_switch_enabled():
                                try:
                                    # FewShotRetriever 需要 milvus_client 和 embedding_client
                                    # 通过 HierarchicalRetriever 的实例获取已初始化的 few_shot_retriever
                                    # 或从 retrieval_result 中获取 few_shot_examples
                                    if hasattr(retrieval_result, 'few_shot_examples') and retrieval_result.few_shot_examples:
                                        few_shot_examples = retrieval_result.few_shot_examples
                                        logger.debug("向量表选择器从检索结果获取Few-Shot示例", example_count=len(few_shot_examples))
                                except Exception as e:
                                    logger.warning("加载Few-Shot示例失败", error=str(e))
                            
                            # 5. 调用LLM3选表+生成IR
                            # 思考过程：开始AI分析
                            await _stream_thinking(stream, "table_selection", f"正在通过AI分析选择最匹配的数据表...", done=False, step_status="started")
                            
                            # 注意：只传递 llm_top_k 个表给 LLM，减少上下文大小和延迟
                            # tables_with_fields 保持完整（用于 fallback 卡）
                            from server.dependencies import get_vector_selector_llm_client
                            llm_client = get_vector_selector_llm_client()
                            vector_selector = VectorTableSelector(llm_client)
                            
                            # 截断传递给 LLM 的表数量
                            tables_for_llm = tables_with_fields
                            if llm_top_k is not None and len(tables_with_fields) > llm_top_k:
                                tables_for_llm = tables_with_fields[:llm_top_k]
                                logger.info(
                                    "向量选表链路截断 LLM 输入表数量",
                                    original_count=len(tables_with_fields),
                                    llm_top_k=llm_top_k,
                                    tables_for_llm=[t.display_name for t in tables_for_llm]
                                )
                            
                            selection_result = await vector_selector.select_and_generate_ir(
                                question=effective_question_text,
                                tables_with_fields=tables_for_llm,
                                retrieval_hints=retrieval_hints,
                                global_rules=global_rules,
                                few_shot_examples=few_shot_examples
                            )
                            
                            # 记录LLM3 prompts到trace（输出在投票验证后更新）
                            if selection_result.system_prompt:
                                table_step.add_metadata("llm3_system_prompt", selection_result.system_prompt)
                            if selection_result.user_prompt:
                                table_step.add_metadata("llm3_user_prompt", selection_result.user_prompt)
                            if selection_result.llm_response:
                                table_step.add_metadata("llm3_response", selection_result.llm_response)
                            
                            # 6. IR投票验证（优化：投票通过则直接执行，减少LLM调用）
                            # 注意：在end_step之前执行，以便记录投票结果到trace
                            ir_vote_passed = False
                            ir_vote_result = None
                            
                            if selection_result.ir:
                                from server.nl2ir.ir_vote_validator import (
                                    IRVoteValidator,
                                    is_ir_vote_validation_enabled
                                )
                                
                                if is_ir_vote_validation_enabled():
                                    try:
                                        vote_validator = IRVoteValidator()
                                        # 使用传递给 LLM 的表列表进行验证（与 LLM 看到的一致）
                                        ir_vote_result = vote_validator.validate(
                                            ir=selection_result.ir,
                                            selected_table_id=selection_result.selected_table_id,
                                            tables_with_fields=tables_for_llm,
                                            global_rules=global_rules
                                        )
                                        ir_vote_passed = ir_vote_result.passed
                                        
                                        # 记录投票验证结果到trace
                                        table_step.add_metadata("ir_vote_validation", ir_vote_result.to_dict())
                                        
                                        logger.info(
                                            "IR投票验证结果",
                                            passed=ir_vote_passed,
                                            score=round(ir_vote_result.score, 4),
                                            field_existence=round(ir_vote_result.field_existence_rate, 4),
                                            table_vote=round(ir_vote_result.table_vote_rate, 4),
                                            original_action=selection_result.action
                                        )
                                    except Exception as vote_error:
                                        logger.warning(
                                            "IR投票验证异常，回退到原置信度逻辑",
                                            error=str(vote_error)
                                        )
                                        ir_vote_passed = False
                                        table_step.add_metadata("ir_vote_validation", {
                                            "error": str(vote_error),
                                            "fallback": True
                                        })
                            
                            # === 输出思考过程：LLM选表结果 ===
                            thinking_lines = [
                                f"\n正在分析选择最相关的数据表...",
                                f"选定表: **{selection_result.selected_table_name}**",
                                f"置信度: {selection_result.confidence:.1%}"
                            ]
                            if ir_vote_passed:
                                thinking_lines.append(f"→ IR投票验证通过（得分: {ir_vote_result.score:.2f}），直接执行查询")
                            elif selection_result.action == "execute":
                                thinking_lines.append("→ 置信度高，直接执行查询")
                            elif selection_result.action == "confirm":
                                thinking_lines.append("→ 需要确认表选择")
                            await _stream_thinking(stream, "table_selection", "\n".join(thinking_lines), done=True, step_status="success")
                            
                            # 更新trace输出，包含投票验证结果
                            table_step.set_output({
                                "table_count": len(table_results),
                                "selected_table_id": selection_result.selected_table_id,
                                "selected_table_name": selection_result.selected_table_name,
                                "confidence": round(selection_result.confidence, 4),
                                "action": selection_result.action,
                                "is_multi_table": selection_result.is_multi_table_query,
                                "ir_vote_passed": ir_vote_passed,
                                "ir_vote_score": round(ir_vote_result.score, 4) if ir_vote_result else None,
                                "final_action": "ir_vote_execute" if ir_vote_passed else selection_result.action
                            })
                            
                            tracer.end_step()
                            
                            # 7. 分流决策：投票通过 > 原置信度评估
                            # 如果投票通过，直接使用LLM3的IR（无需人工确认）
                            if ir_vote_passed and selection_result.ir:
                                # 投票验证通过：直接使用LLM3的IR
                                logger.info(
                                    "IR投票验证通过，直接使用LLM3的IR",
                                    selected_table=selection_result.selected_table_name,
                                    vote_score=round(ir_vote_result.score, 4) if ir_vote_result else 0,
                                    original_confidence=selection_result.confidence,
                                    original_action=selection_result.action
                                )
                                
                                # 构建IR参数，处理可选字段避免None覆盖默认值
                                ir_kwargs = {
                                    "query_type": selection_result.ir.get("query_type", "aggregation"),
                                    "datasource_id": selection_result.selected_table_id,
                                    "metrics": selection_result.ir.get("metrics", []),
                                    "dimensions": selection_result.ir.get("dimensions", []),
                                    "filters": selection_result.ir.get("filters", []),
                                    "having_filters": selection_result.ir.get("having_filters", []),
                                    "conditional_metrics": selection_result.ir.get("conditional_metrics", []),
                                    "ratio_metrics": selection_result.ir.get("ratio_metrics", []),
                                    "calculated_fields": selection_result.ir.get("calculated_fields", []),
                                    "with_total": selection_result.ir.get("with_total", False),
                                    "partition_by": selection_result.ir.get("partition_by", []),
                                    "duplicate_by": selection_result.ir.get("duplicate_by", []),
                                    "cross_partition_query": selection_result.ir.get("cross_partition_query", False),
                                    "ambiguities": selection_result.ir.get("ambiguities", []),
                                    "original_question": effective_question_text,
                                }
                                
                                # 只有非None时才添加这些可选字段，避免覆盖模型默认值
                                if selection_result.ir.get("sort_by") is not None:
                                    ir_kwargs["sort_by"] = selection_result.ir.get("sort_by")
                                if selection_result.ir.get("sort_order") is not None:
                                    ir_kwargs["sort_order"] = selection_result.ir.get("sort_order")
                                if selection_result.ir.get("limit") is not None:
                                    ir_kwargs["limit"] = selection_result.ir.get("limit")
                                if selection_result.ir.get("time") is not None:
                                    ir_kwargs["time"] = selection_result.ir.get("time")
                                if selection_result.ir.get("time_grain") is not None:
                                    ir_kwargs["time_grain"] = selection_result.ir.get("time_grain")
                                if selection_result.ir.get("window_limit") is not None:
                                    ir_kwargs["window_limit"] = selection_result.ir.get("window_limit")
                                if selection_result.ir.get("cross_partition_mode") is not None:
                                    ir_kwargs["cross_partition_mode"] = selection_result.ir.get("cross_partition_mode")
                                
                                ir = IntermediateRepresentation(**ir_kwargs)
                                confidence = selection_result.confidence
                                
                                # 设置连接ID
                                selected_table = next(
                                    (t for t in tables_with_fields if t.table_id == selection_result.selected_table_id),
                                    None
                                )
                                if selected_table and selected_table.connection_id:
                                    actual_connection_id = selected_table.connection_id
                                
                                effective_selected_table_id = selection_result.selected_table_id
                                candidate_snapshot = _build_vector_fallback_selection_card(
                                    tables_with_fields=tables_with_fields,
                                    question=effective_question_text,
                                    fallback_reason="ir_vote_failed",
                                ).model_dump()
                                
                                await _stream_progress(stream, step_name, "success", "IR投票验证通过，直接执行", {
                                    "confidence": confidence,
                                    "vote_score": round(ir_vote_result.score, 4) if ir_vote_result else 0,
                                    "selected_table": selection_result.selected_table_name,
                                    "action": "ir_vote_execute"
                                })
                                
                                ir_from_llm3 = True
                                
                            elif selection_result.action == "execute":
                                # 原逻辑：高置信度直接执行（投票未通过但置信度高）
                                logger.info(
                                    "LLM3高置信度，直接使用IR",
                                    selected_table=selection_result.selected_table_name,
                                    confidence=selection_result.confidence
                                )
                                
                                if selection_result.ir:
                                    ir_kwargs = {
                                        "query_type": selection_result.ir.get("query_type", "aggregation"),
                                        "datasource_id": selection_result.selected_table_id,
                                        "metrics": selection_result.ir.get("metrics", []),
                                        "dimensions": selection_result.ir.get("dimensions", []),
                                        "filters": selection_result.ir.get("filters", []),
                                        "having_filters": selection_result.ir.get("having_filters", []),
                                        "conditional_metrics": selection_result.ir.get("conditional_metrics", []),
                                        "ratio_metrics": selection_result.ir.get("ratio_metrics", []),
                                        "calculated_fields": selection_result.ir.get("calculated_fields", []),
                                        "with_total": selection_result.ir.get("with_total", False),
                                        "partition_by": selection_result.ir.get("partition_by", []),
                                        "duplicate_by": selection_result.ir.get("duplicate_by", []),
                                        "cross_partition_query": selection_result.ir.get("cross_partition_query", False),
                                        "ambiguities": selection_result.ir.get("ambiguities", []),
                                        "original_question": effective_question_text,
                                    }
                                    
                                    # 只有非None时才添加这些可选字段，避免覆盖模型默认值
                                    if selection_result.ir.get("sort_by") is not None:
                                        ir_kwargs["sort_by"] = selection_result.ir.get("sort_by")
                                    if selection_result.ir.get("sort_order") is not None:
                                        ir_kwargs["sort_order"] = selection_result.ir.get("sort_order")
                                    if selection_result.ir.get("limit") is not None:
                                        ir_kwargs["limit"] = selection_result.ir.get("limit")
                                    if selection_result.ir.get("time") is not None:
                                        ir_kwargs["time"] = selection_result.ir.get("time")
                                    if selection_result.ir.get("time_grain") is not None:
                                        ir_kwargs["time_grain"] = selection_result.ir.get("time_grain")
                                    if selection_result.ir.get("window_limit") is not None:
                                        ir_kwargs["window_limit"] = selection_result.ir.get("window_limit")
                                    if selection_result.ir.get("cross_partition_mode") is not None:
                                        ir_kwargs["cross_partition_mode"] = selection_result.ir.get("cross_partition_mode")
                                    
                                    ir = IntermediateRepresentation(**ir_kwargs)
                                    confidence = selection_result.confidence
                                    
                                    selected_table = next(
                                        (t for t in tables_with_fields if t.table_id == selection_result.selected_table_id),
                                        None
                                    )
                                    if selected_table and selected_table.connection_id:
                                        actual_connection_id = selected_table.connection_id
                                    
                                    effective_selected_table_id = selection_result.selected_table_id
                                    candidate_snapshot = _build_vector_fallback_selection_card(
                                        tables_with_fields=tables_with_fields,
                                        question=effective_question_text,
                                        fallback_reason="low_confidence",
                                    ).model_dump()
                                    
                                    await _stream_progress(stream, step_name, "success", "LLM3直接生成IR", {
                                        "confidence": confidence,
                                        "selected_table": selection_result.selected_table_name,
                                        "action": "execute"
                                    })
                                    
                                    ir_from_llm3 = True
                                else:
                                    logger.warning("LLM3未返回有效IR，降级到LLM2")
                                    effective_selected_table_id = selection_result.selected_table_id
                                    ir_from_llm3 = False
                                    await _stream_progress(stream, step_name, "success", "降级到NL2IR解析", {
                                        "selected_table": selection_result.selected_table_name,
                                        "fallback_reason": "invalid_ir"
                                    })
                                    
                            elif selection_result.action == "confirm":
                                # IR投票未通过 + 中置信度：需要用户确认表选择
                                # 降级到 LLM选表链路的表选择确认流程
                                logger.info(
                                    "IR投票未通过，降级到表选择确认流程",
                                    confidence=selection_result.confidence,
                                    vote_score=round(ir_vote_result.score, 4) if ir_vote_result else 0,
                                    vote_reasons=ir_vote_result.reasons[:3] if ir_vote_result else [],
                                    fallback_reason="ir_vote_failed"
                                )
                                
                                # 使用向量检索结果构建完整的 TableSelectionCard
                                # 复用 LLM选表链路的格式，用户确认后走 LLM选表链路的后续流程
                                fallback_card = _build_vector_fallback_selection_card(
                                    tables_with_fields=tables_with_fields,
                                    question=effective_question_text,
                                    fallback_reason="ir_vote_failed"
                                )
                                
                                await _stream_progress(stream, step_name, "pending", "等待用户确认表选择", {
                                    "candidate_count": len(fallback_card.candidates),
                                    "fallback_reason": "ir_vote_failed"
                                })
                                await _stream_table_selection(stream, fallback_card, query_id)
                                
                                tracer.finalize({
                                    "status": "table_selection_needed",
                                    "action": "vector_fallback",
                                    "reason": "ir_vote_failed",
                                    "candidate_count": len(fallback_card.candidates)
                                })
                                pending_response = QueryResponse(
                                    status="table_selection_needed",
                                    table_selection=fallback_card,
                                    query_id=query_id,
                                    timestamp=timestamp
                                )
                                await _update_query_session(
                                    status="awaiting_user_action",
                                    current_node="table_resolution",
                                    state_updates={
                                        "pending_actions": ["confirm", "change_table", "request_explanation", "exit_current"],
                                        "candidate_snapshot": fallback_card.model_dump(),
                                        "recommended_table_ids": list(fallback_card.recommended_table_ids or []),
                                        "selected_table_ids": selected_table_ids,
                                    },
                                )
                                return pending_response
                                    
                            else:
                                # IR投票未通过 + 低置信度：同样降级到表选择确认流程
                                logger.info(
                                    "IR投票未通过且低置信度，降级到表选择确认流程",
                                    confidence=selection_result.confidence,
                                    vote_score=round(ir_vote_result.score, 4) if ir_vote_result else 0,
                                    vote_reasons=ir_vote_result.reasons[:3] if ir_vote_result else [],
                                    fallback_reason="low_confidence"
                                )
                                
                                # 使用向量检索结果构建完整的 TableSelectionCard
                                fallback_card = _build_vector_fallback_selection_card(
                                    tables_with_fields=tables_with_fields,
                                    question=effective_question_text,
                                    fallback_reason="low_confidence"
                                )
                                
                                await _stream_progress(stream, step_name, "pending", "等待用户确认表选择", {
                                    "candidate_count": len(fallback_card.candidates),
                                    "fallback_reason": "low_confidence"
                                })
                                await _stream_table_selection(stream, fallback_card, query_id)
                                
                                tracer.finalize({
                                    "status": "table_selection_needed",
                                    "action": "vector_fallback",
                                    "reason": "low_confidence",
                                    "candidate_count": len(fallback_card.candidates)
                                })
                                pending_response = QueryResponse(
                                    status="table_selection_needed",
                                    table_selection=fallback_card,
                                    query_id=query_id,
                                    timestamp=timestamp
                                )
                                await _update_query_session(
                                    status="awaiting_user_action",
                                    current_node="table_resolution",
                                    state_updates={
                                        "pending_actions": ["confirm", "change_table", "request_explanation", "exit_current"],
                                        "candidate_snapshot": fallback_card.model_dump(),
                                        "recommended_table_ids": list(fallback_card.recommended_table_ids or []),
                                        "selected_table_ids": selected_table_ids,
                                    },
                                )
                                return pending_response
                        
                except Exception as e:
                    logger.exception("向量表选择失败", error=str(e))
                    if 'table_step' in dir() and table_step:
                        table_step.set_output({"error": str(e), "fallback": True})
                        tracer.end_step()
                    
                    # 返回错误响应
                    tracer.finalize({"status": "error", "reason": "vector_selection_failed"})
                    return await _return_query_response(
                        QueryResponse(
                        status="error",
                        error={
                            "code": "VECTOR_SELECTION_FAILED",
                            "message": f"向量表选择过程失败: {str(e)}"
                        },
                        query_id=query_id,
                        timestamp=timestamp
                        ),
                        status="failed",
                        current_node="table_resolution",
                        state_updates={"selected_table_ids": selected_table_ids},
                        last_error=str(e),
                    )
            else:
                ir_from_llm3 = False
            
            # 如果已经从LLM3获得了IR，跳过NL2IR解析
            if ir_from_llm3 and ir is not None:
                # IR已由LLM3生成，跳过NL2IR解析步骤
                logger.debug("使用LLM3生成的IR，跳过NL2IR解析")
                # 确保semantic_model和formatter已加载（后续步骤需要）
                # 注意：第二条链路使用了 global_semantic_model，需要切换到目标连接的模型
                if global_semantic_model is not None:
                    # 如果使用了全局模型，切换到目标连接的模型
                    if actual_connection_id:
                        manager = get_metadata_manager()
                        semantic_model = await manager.get_connection_model(actual_connection_id)
                        formatter = ResultFormatter(semantic_model)
                    else:
                        # 如果没有确定连接，使用全局模型
                        semantic_model = global_semantic_model
                        formatter = ResultFormatter(semantic_model)
                elif semantic_model is None:
                    semantic_model = await ensure_semantic_model()
                    formatter = ResultFormatter(semantic_model)
            else:
                # NL2IR 解析：此时加载 semantic_model 和 parser
                step_name = "NL2IR解析"
                step_desc = "将自然语言转换为中间表示"
                step = tracer.start_step(step_name, "parsing", step_desc)
                await _stream_progress(stream, step_name, "started", step_desc, {"question": effective_question_text})
                # 思考过程：开始解析
                await _stream_thinking(stream, "nl2ir", f"正在解析查询意图...", done=False, step_status="started")
                step.set_input({
                    "question": request.text,
                    "resolved_question": effective_question_text,
                    "domain_id": request.domain_id,
                    "selected_table_id": effective_selected_table_id
                })

                # 延迟加载 semantic_model 和 parser
                semantic_model = await ensure_semantic_model()
                parser = await ensure_parser()
                
                logger.debug(
                    "NL2IR 解析开始",
                    selected_table_id=effective_selected_table_id,
                    has_selected_table=bool(effective_selected_table_id),
                    selected_table_ids=selected_table_ids,
                    is_cross_partition_query=is_cross_partition_query,
                    cross_partition_mode=cross_partition_mode
                )

                # NL → IR（传入用户指定的业务域和已选择的表ID）
                # 如果 LLM 表选择已在第1步完成，hierarchical_retriever 会直接使用 selected_table_id
                ir, confidence = await parser.parse(
                    effective_question_text,
                    user_specified_domain=request.domain_id,
                    pre_retrieved_result=None,  # 不再传递 pre_retrieved_result
                    selected_table_id=effective_selected_table_id,  # 兼容单表模式
                    selected_table_ids=selected_table_ids if len(selected_table_ids) > 1 else None,  # 传递多表列表（跨年查询等）
                    user_id=actual_user_id,
                    user_role=actual_role,
                    is_cross_partition_query=is_cross_partition_query,  # 跨分区查询标志
                    cross_partition_mode=cross_partition_mode  # 跨分区模式（compare/union/multi_join）
                )
                logger.debug("NL→IR 解析完成", confidence=confidence, domain=ir.domain_name)

                # 添加LLM提示词到metadata（用于调试）
                prompt_blocks = []
                if parser.last_system_prompt:
                    prompt_blocks.append({
                        "__type__": "markdown",
                        "title": "System Prompt",
                        "content": parser.last_system_prompt
                    })
                if parser.last_user_prompt:
                    prompt_blocks.append({
                        "__type__": "markdown",
                        "title": "User Prompt",
                        "content": parser.last_user_prompt
                    })
                if prompt_blocks:
                    step.add_metadata("llm_prompts", prompt_blocks)
                    step.add_metadata("llm_prompt_stats", {
                        "system_prompt_chars": len(parser.last_system_prompt or ""),
                        "user_prompt_chars": len(parser.last_user_prompt or "")
                    })
                if getattr(parser, "last_raw_ir_json", None):
                    step.add_metadata("llm_raw_ir", parser.last_raw_ir_json)
                if getattr(parser, 'last_validation_notes', None):
                    step.add_metadata("validation_notes", parser.last_validation_notes)
                if getattr(parser, "last_retrieval_summary", None):
                    step.add_metadata("retrieval_summary", parser.last_retrieval_summary)
                # 添加 IR 修复日志到 Trace
                if hasattr(ir, 'get_fix_log') and ir.get_fix_log():
                    step.add_metadata("ir_fix_log", ir.get_fix_log())

                # 输出可读版IR
                step.set_output({
                    "ir_display": _ir_to_display_dict(ir, semantic_model),
                    "confidence": confidence,
                    "domain": ir.domain_name
                })
                
                # === 输出思考过程：NL2IR解析结果 ===
                ir_thinking_lines = ["正在解析查询意图..."]
                if ir.metrics:
                    def _get_metric_display_name(m):
                        if isinstance(m, str):
                            return m
                        elif isinstance(m, dict):
                            return m.get('alias') or m.get('name') or m.get('field', str(m))
                        elif hasattr(m, 'alias') and m.alias:
                            return m.alias
                        elif hasattr(m, 'field'):
                            return m.field
                        return str(m)
                    metrics_display = [_get_metric_display_name(m) for m in ir.metrics[:5]]
                    ir_thinking_lines.append(f"识别指标: {', '.join(metrics_display)}")
                if ir.dimensions:
                    dims_display = [d if isinstance(d, str) else d.get('name', str(d)) for d in ir.dimensions[:5]]
                    ir_thinking_lines.append(f"识别维度: {', '.join(dims_display)}")
                if ir.filters:
                    filters_count = len(ir.filters)
                    ir_thinking_lines.append(f"过滤条件: {filters_count} 个")
                if ir.time:
                    time_info = ir.time if isinstance(ir.time, str) else str(ir.time)
                    ir_thinking_lines.append(f"时间范围: {time_info[:50]}")
                if ir.sort_by:
                    ir_thinking_lines.append(f"排序: {ir.sort_by} {ir.sort_order or 'ASC'}")
                if ir.limit:
                    ir_thinking_lines.append(f"限制: {ir.limit} 条")
                ir_thinking_lines.append(f"置信度: {confidence:.1%}")
                await _stream_thinking(stream, "nl2ir", "\n".join(ir_thinking_lines), done=True, step_status="success")
                
                tracer.end_step()
                await _stream_progress(stream, step_name, "success", step_desc, {
                    "confidence": confidence,
                    "domain": ir.domain_name
                })

                # 检查停止信号
                await _check_stop_signal("NL2IR解析")
        else:
            raise HTTPException(status_code=400, detail="必须提供 text 或 ir 参数")

        # 2.45 连接解析 - 如果还没有确定连接，从IR反推目标数据库连接
        if not actual_connection_id:
            from server.compiler.connection_resolver import (
                ConnectionResolver,
                ConnectionResolutionResult
            )
            from server.exceptions import TableNotFoundError, CrossConnectionNotSupported
            
            step_name = "连接解析"
            step_desc = "从查询内容确定目标数据库连接"
            step = tracer.start_step(step_name, "connection_resolution", step_desc)
            await _stream_progress(stream, step_name, "started", step_desc)
            
            try:
                resolver = ConnectionResolver(semantic_model)
                
                # 从检索结果获取提示连接
                hint_connection_id = None
                # 优先使用全局检索结果（第二条链路）
                if global_retrieval_result is not None:
                    if hasattr(global_retrieval_result, "detected_connection_id"):
                        hint_connection_id = global_retrieval_result.detected_connection_id
                # 其次使用 parser 的检索结果（第一条链路或传统NL2IR流程）
                elif parser and hasattr(parser, "last_retrieval_result"):
                    retrieval_result = parser.last_retrieval_result
                    if retrieval_result and hasattr(retrieval_result, "detected_connection_id"):
                        hint_connection_id = retrieval_result.detected_connection_id
                
                resolution_result = resolver.resolve_connection_from_ir(
                    ir=ir,
                    hint_connection_id=hint_connection_id,
                    user_specified_connection_id=None
                )
                
                if resolution_result.connection_id:
                    actual_connection_id = resolution_result.connection_id
                    logger.info(
                        "连接解析成功",
                        connection_id=actual_connection_id,
                        status=resolution_result.status,
                        confidence=resolution_result.confidence
                    )
                    
                    # 更新 tracer 的 connection_id
                    tracer.connection_id = actual_connection_id
                
                step.set_output({
                    "status": resolution_result.status,
                    "connection_id": resolution_result.connection_id,
                    "involved_connections": resolution_result.involved_connections,
                    "confidence": resolution_result.confidence,
                    "all_tables_same_connection": resolution_result.all_tables_same_connection
                })
                
                if resolution_result.warning_message:
                    step.add_metadata("warning", resolution_result.warning_message)
                
                tracer.end_step()
                await _stream_progress(stream, step_name, "success", step_desc, {
                    "connection_id": actual_connection_id
                })
                
            except TableNotFoundError as e:
                step.set_error(str(e))
                tracer.end_step()
                error_info = {
                    "code": "TABLE_NOT_FOUND",
                    "message": e.message
                }
                await _stream_error(stream, error_info)
                tracer.finalize()
                return await _return_query_response(
                    QueryResponse(
                    status="error",
                    error=error_info,
                    auth_status=auth_status,
                    query_id=query_id,
                    timestamp=timestamp
                    ),
                    status="failed",
                    current_node="permission_resolution",
                    last_error=error_info["message"],
                )
            except CrossConnectionNotSupported as e:
                step.set_error(str(e))
                tracer.end_step()
                error_info = {
                    "code": "CROSS_CONNECTION_NOT_SUPPORTED",
                    "message": e.message
                }
                await _stream_error(stream, error_info)
                tracer.finalize()
                return await _return_query_response(
                    QueryResponse(
                    status="error",
                    error=error_info,
                    auth_status=auth_status,
                    query_id=query_id,
                    timestamp=timestamp
                    ),
                    status="failed",
                    current_node="permission_resolution",
                    last_error=error_info["message"],
                )
            except Exception as e:
                step.set_error(str(e))
                tracer.end_step()
                logger.warning("连接解析失败", error=str(e))
                # 连接解析失败时继续执行，后续步骤可能会报错
        
        # 如果连接解析后仍然没有连接，返回错误
        if not actual_connection_id:
            error_info = {
                "code": "CONNECTION_REQUIRED",
                "message": "无法确定目标数据库连接。请选择一个数据源或在问题中提及具体的表名。"
            }
            await _stream_error(stream, error_info)
            tracer.finalize()
            return await _return_query_response(
                QueryResponse(
                status="error",
                error=error_info,
                auth_status=auth_status,
                query_id=query_id,
                timestamp=timestamp
                ),
                status="failed",
                current_node="connection_resolution",
                last_error=error_info["message"],
            )

        # 2.5 Schema Linking 校正
        if validation_loop is None:
            validation_loop = get_validation_loop()
        ir = await validation_loop.align_ir(ir, actual_connection_id)
        current_ir_display = _ir_to_display_dict(ir, semantic_model)

        # 2.55 表/列权限校验（需先完成IR校正）
        await _enforce_schema_permissions(
            ir,
            semantic_model,
            actual_connection_id,
            actual_user_id,
            actual_role,
            tracer,
            stream
        )

        should_pause_for_draft_confirmation = (
            not request.ir
            and (
                confirmation_mode == "always_confirm"
                or bool(existing_state.get("draft_confirmation_required"))
            )
            and not bool(existing_state.get("draft_confirmation_approved"))
        )
        if should_pause_for_draft_confirmation:
            selected_table_names: List[str] = []
            if selected_table_ids:
                try:
                    selected_table_names = await _load_table_display_names(selected_table_ids)
                except Exception as exc:
                    logger.warning("加载确认阶段表名失败", query_id=query_id, error=str(exc))

            draft_warnings: List[str] = []
            if len(selected_table_ids) > 1:
                draft_warnings.append(f"本次草稿基于 {len(selected_table_ids)} 张表的联合查询。")
            if existing_state.get("revision_request"):
                draft_warnings.append("已按您刚才的修改意见重算当前草稿。")

            confirm_card = ConfirmationCard(
                ir=ir,
                natural_language=_build_draft_confirmation_summary(
                    current_ir_display,
                    selected_table_names=selected_table_names,
                    revision_request=existing_state.get("revision_request"),
                ),
                warnings=draft_warnings,
            )

            tracer.finalize({"status": "draft_confirmation_needed"}, save_to_file=False)
            response = QueryResponse(
                status="confirm_needed",
                confirmation=confirm_card,
                auth_status=auth_status,
                query_id=query_id,
                timestamp=timestamp,
            )
            await _stream_confirmation(stream, confirm_card)
            return await _return_query_response(
                response,
                status="awaiting_user_action",
                current_node="draft_confirmation",
                state_updates={
                    "pending_actions": ["confirm", "revise", "change_table", "request_explanation", "exit_current"],
                    "draft_confirmation_card": sanitize_for_json(confirm_card.model_dump()),
                    "ir_snapshot": sanitize_for_json(ir.model_dump()),
                    "ir_ready": True,
                    "selected_table_ids": selected_table_ids,
                    "selected_table_id": effective_selected_table_id,
                    "draft_confirmation_required": False,
                    "draft_confirmation_approved": False,
                },
            )

        # 2.6 权限过滤注入（如果用户已认证且启用RLS）
        permission_info = {"applied": False}
        if current_user and settings.rls_enabled:
            step_name = "权限过滤"
            step_desc = "注入行级过滤条件"
            step = tracer.start_step(step_name, "permission", step_desc)
            await _stream_progress(stream, step_name, "started", step_desc)
            
            try:
                from server.services.permission_injector import PermissionInjector
                # get_metadata_pool 已在文件顶部导入
                
                pool = await get_metadata_pool()
                async with pool.acquire() as conn:
                    injector = PermissionInjector(conn)
                    # 确保 UUID 参数是字符串格式再转换，避免 asyncpg UUID 类型不兼容
                    ir, permission_info = await injector.inject_permissions(
                        ir,
                        UUID(str(current_user.user_id)),
                        UUID(str(actual_connection_id))
                    )
                
                step.set_output(permission_info)
                tracer.end_step()
                
                # 检查是否存在权限冲突
                if permission_info.get("permission_conflict"):
                    conflict_details = permission_info.get("conflict_details", [])
                    # 构建用户友好的错误消息
                    conflict_msgs = []
                    for conflict in conflict_details:
                        field = conflict.get("field", "未知字段")
                        user_requested = conflict.get("user_requested", "未知值")
                        allowed_values = conflict.get("allowed_values", [])
                        allowed_str = "、".join(str(v) for v in allowed_values[:5])
                        if len(allowed_values) > 5:
                            allowed_str += f" 等{len(allowed_values)}个值"
                        conflict_msgs.append(
                            f"您查询的「{user_requested}」不在您的权限范围内，您只能访问：{allowed_str}"
                        )
                    
                    error_message = "权限不足：" + "；".join(conflict_msgs)
                    
                    await _stream_progress(stream, step_name, "error", error_message, {
                        "conflict_details": conflict_details
                    })
                    
                    tracer.finalize({"status": "error", "error": error_message})
                    
                    response = QueryResponse(
                        status="error",
                        error={
                            "code": "PERMISSION_DENIED",
                            "message": error_message,
                            "details": {"conflict_details": conflict_details}
                        },
                        query_id=query_id,
                        timestamp=timestamp
                    )
                    await _stream_error(stream, response.error or {})
                    return await _return_query_response(
                        response,
                        status="failed",
                        current_node="permission_guard",
                        last_error=error_message,
                    )
                
                if permission_info.get("applied"):
                    await _stream_progress(stream, step_name, "success", step_desc, {
                        "injected_filters": permission_info.get("injected_filters", 0)
                    })
                    logger.info("权限过滤已注入", **permission_info)
                else:
                    await _stream_progress(stream, step_name, "skipped", 
                        permission_info.get("reason", "无需注入"))
            except Exception as e:
                logger.warning(f"权限注入失败，继续执行查询: {e}")
                step.set_error(str(e))
                tracer.end_step()
                await _stream_progress(stream, step_name, "warning", f"权限注入失败: {e}")

        # 3. 用户上下文
        user_context = {
            "user_id": actual_user_id,
            "connection_id": actual_connection_id,  # 使用 connection_id 代替 tenant_id
            "role": request.role
        }

        # === 混合架构路由检查（完全依赖 LLM 在 IR 中的标记）===
        use_direct_sql = False
        use_complex_split = False
        direct_sql_reason = None
        complex_split_reason = None
        routing_decision = None
        
        if settings.hybrid_architecture_enabled:
            from server.query_complexity.router import get_query_router
            hybrid_router = get_query_router(
                enable_complex_split=settings.enable_complex_query_auto_execution,
                enable_direct_sql=settings.direct_sql_enabled
            )
            routing_decision = hybrid_router.route(effective_question_text or "", ir)
            
            # 记录路由决策到 tracer
            route_step = tracer.start_step("混合路由", "routing", "基于LLM标记进行路由决策")
            route_step.set_output({
                "route": routing_decision.route,
                "confidence": routing_decision.confidence,
                "reason": routing_decision.reason,
                "detected_features": routing_decision.detected_features[:10] if routing_decision.detected_features else [],
                "fallback_route": routing_decision.fallback_route
            })
            tracer.end_step()
            
            # 根据路由决策设置标记
            if routing_decision.route == "direct_sql":
                use_direct_sql = True
                direct_sql_reason = routing_decision.reason
                logger.info(
                    "路由决策：直接SQL流程",
                    query_id=query_id,
                    reason=direct_sql_reason,
                    confidence=routing_decision.confidence
                )
            elif routing_decision.route == "complex_split":
                use_complex_split = True
                complex_split_reason = routing_decision.reason
                logger.info(
                    "路由决策：复杂拆分流程",
                    query_id=query_id,
                    reason=complex_split_reason,
                    confidence=routing_decision.confidence
                )
            else:
                # standard_ir 流程，可能使用增强IR功能
                if routing_decision.detected_features:
                    logger.debug(
                        "路由决策：标准IR流程（含增强功能）",
                        query_id=query_id,
                        features=routing_decision.detected_features
                    )
        
        # 如果需要直接 SQL 生成，跳转到直接 SQL 流程
        if use_direct_sql:
            try:
                step_name = "直接SQL生成"
                step_desc = f"使用 LLM 直接生成 SQL: {direct_sql_reason}"
                step = tracer.start_step(step_name, "direct_sql", step_desc)
                await _stream_progress(stream, step_name, "started", step_desc, {"reason": direct_sql_reason})
                
                from server.services.hybrid_query_service import get_hybrid_query_service
                hybrid_service = await get_hybrid_query_service(
                    connection_id=actual_connection_id,
                    semantic_model=semantic_model,
                    dialect=getattr(await ensure_executor(), 'db_type', 'mssql')
                )
                
                # 获取行级权限过滤（如果已应用）
                row_level_filters = None
                if permission_info.get("applied"):
                    row_level_filters = permission_info.get("injected_filter_details", [])
                
                direct_result = await hybrid_service.process_with_direct_sql(
                    question=effective_question_text,
                    semantic_model=semantic_model,
                    user_context=user_context,
                    row_level_filters=row_level_filters
                )
                
                if direct_result.success:
                    sql = direct_result.sql
                    step.set_output({
                        "sql_length": len(sql),
                        "confidence": direct_result.confidence,
                        "applied_filters": len(direct_result.metadata.get("applied_filters", [])),
                        "warnings": direct_result.warnings
                    })
                    tracer.end_step()
                    await _stream_progress(stream, step_name, "success", step_desc, {
                        "sql_length": len(sql),
                        "confidence": direct_result.confidence
                    })
                    
                    # 继续到执行阶段，跳过常规的 IR 编译
                    # 标记已经有 SQL，后续会直接执行
                    logger.info("直接 SQL 生成成功", query_id=query_id, sql_length=len(sql))
                    
                else:
                    # 直接 SQL 生成失败，回退到常规 IR 流程
                    step.set_error("; ".join(direct_result.errors))
                    tracer.end_step()
                    await _stream_progress(stream, step_name, "warning", 
                        f"直接 SQL 生成失败，回退到 IR 流程: {'; '.join(direct_result.errors)}", None)
                    logger.warning(
                        "直接 SQL 生成失败，回退到 IR 流程",
                        query_id=query_id,
                        errors=direct_result.errors
                    )
                    use_direct_sql = False  # 标记回退
                    
            except Exception as e:
                logger.error("直接 SQL 生成异常，回退到 IR 流程", error=str(e), query_id=query_id)
                if tracer.current_step:
                    tracer.current_step.set_error(str(e))
                    tracer.end_step()
                await _stream_progress(stream, "直接SQL生成", "error", f"异常: {str(e)}", None)
                use_direct_sql = False  # 标记回退

        # === 复杂查询自动化执行（基于路由决策）===
        is_complex_executed = False
        result = None  # 提前定义 result

        # 只在路由决策为 complex_split 且未强制执行时进入 CoT + DAG 流程
        if use_complex_split and not request.force_execute:
            logger.debug(
                "进入复杂拆分流程（CoT + DAG）",
                query_id=query_id,
                reason=complex_split_reason
            )
            
            try:
                cot_name = "CoT规划与执行"
                cot_desc = f"复杂查询自动编排: {complex_split_reason}"
                step_cot = tracer.start_step(cot_name, "execution", cot_desc)
                await _stream_progress(stream, cot_name, "started", cot_desc, {
                    "reason": complex_split_reason,
                    "suggested_subquestions": getattr(ir, 'suggested_subquestions', [])
                })
                
                # 1. CoT Planning
                from server.cot_planner.planner import get_cot_planner
                planner = get_cot_planner()
                plan_steps = await planner.generate_plan(effective_question_text, {"domain_name": ir.domain_name})
                step_cot.add_metadata("cot_plan", plan_steps)
                
                # 2. DAG Building（带用户权限过滤）
                from server.dag_executor.builder import get_dag_builder
                builder = get_dag_builder()
                dag_plan = await builder.build(
                    plan_steps, 
                    actual_connection_id, 
                    request.domain_id,
                    user_id=actual_user_id,
                    user_role=actual_role
                )
                
                # 3. DAG Execution
                from server.dag_executor.executor import get_dag_executor
                dag_executor = get_dag_executor()
                result, final_ir, dag_debug_nodes = await dag_executor.execute(dag_plan, user_context)
                
                if final_ir:
                    ir = final_ir
                    current_ir_display = _ir_to_display_dict(ir, semantic_model)
                
                is_complex_executed = True
                logger.debug("复杂查询自动执行成功", plan_id=dag_plan.plan_id)
                
                dag_debug_nodes = dag_debug_nodes or []
                dag_step_details = []
                for node_info in dag_debug_nodes:
                    node_entry = {
                        "node_id": node_info.get("node_id"),
                        "type": node_info.get("type"),
                        "description": node_info.get("description"),
                        "dependencies": node_info.get("dependencies"),
                        "sql": node_info.get("sql"),
                        "row_count": node_info.get("row_count"),
                        "column_count": node_info.get("column_count"),
                    }
                    node_ir = node_info.get("ir")
                    if node_ir:
                        try:
                            node_ir_obj = IntermediateRepresentation.model_validate(node_ir)
                            node_entry["ir_display"] = _ir_to_display_dict(node_ir_obj, semantic_model)
                        except Exception:
                            node_entry["ir_raw"] = node_ir
                    if node_info.get("context_exports"):
                        node_entry["context_exports"] = node_info.get("context_exports")
                    dag_step_details.append(node_entry)

                if result:
                    if result.meta is None:
                        result.meta = {}
                    result.meta["dag_node_traces"] = dag_step_details

                    process_steps = []
                    for node_entry in dag_step_details:
                        summary = node_entry.get("description") or node_entry.get("node_id")
                        highlights = []
                        for export in node_entry.get("context_exports") or []:
                            if export.get("mode") == "value_list":
                                preview_values = export.get("preview_values") or []
                                if preview_values:
                                    highlights.extend(preview_values)
                        if highlights:
                            preview_text = "、".join(highlights[:3])
                            if len(highlights) > 3:
                                preview_text += "等"
                            summary = f"{summary}（{preview_text}）"
                        process_steps.append(summary)

                    if process_steps:
                        existing_steps = result.meta.get("dag_process_steps") or []
                        result.meta["dag_process_steps"] = existing_steps + process_steps

                step_cot.set_output({
                    "status": "success", 
                    "plan_id": dag_plan.plan_id,
                    "steps": len(plan_steps),
                    "final_ir_display": current_ir_display,
                    "node_traces": dag_step_details
                })
                tracer.end_step()
                await _stream_progress(stream, cot_name, "success", cot_desc, {"steps": len(plan_steps)})
                
            except Exception as e:
                logger.warning("复杂查询自动执行失败，回退到常规流程", error=str(e))
                if tracer.current_step:
                    tracer.current_step.set_error(str(e))
                    tracer.end_step()
                await _stream_progress(stream, "CoT规划与执行", "error", "复杂查询自动编排", {"error": str(e)})
                # Fallback: 继续往下走

        # 旧的"请确认AI理解"确认检查已移除
        # 现在统一使用表选择确认卡（TableSelectionCard）进行用户确认
        # 当向量检索链路 IR 投票未通过时，会显示表选择确认界面

        # 如果已经通过 DAG 执行或直接 SQL 生成得到了结果/SQL，跳过常规的编译步骤
        # 注意：直接 SQL 生成时 sql 变量已经在上面被赋值
        if 'sql' not in locals() or not sql:
            sql = ""
        
        # 跳过条件：复杂查询已执行 或 直接 SQL 已生成
        skip_ir_compilation = is_complex_executed or (use_direct_sql and sql)
        
        if not skip_ir_compilation:
            # 4. 检查缓存
            if not request.skip_cache and cache is not None:
                step_name = "缓存检查"
                step_desc = "检查是否有缓存结果"
                step = tracer.start_step(step_name, "caching", step_desc)
                await _stream_progress(stream, step_name, "started", step_desc, None)
                cache_error = None
                cache_hit = False
                try:
                    cache_key = cache.get_cache_key(ir, user_context)
                    step.set_input({"cache_key": cache_key})

                    cached_result = await cache.get(cache_key)

                    if cached_result:
                        cache_hit = True
                        step.set_output({"hit": True, "rows": len(cached_result.rows)})
                        tracer.end_step()
                        tracer.finalize({"status": "success", "from_cache": True})

                        response = QueryResponse(
                            status="success",
                            result=cached_result,
                            query_id=query_id,
                            timestamp=timestamp
                        )
                        await _stream_result(stream, query_id, timestamp, cached_result)
                        result_streamed = True
                        return await _return_query_response(
                            response,
                            status="completed",
                            current_node="completed",
                            state_updates={
                                "cache_hit": True,
                                "ir_ready": True,
                                "selected_table_ids": selected_table_ids,
                                "selected_table_id": effective_selected_table_id,
                                "ir_snapshot": sanitize_for_json(ir.model_dump()) if hasattr(ir, "model_dump") else None,
                            },
                        )

                    step.set_output({"hit": False})
                except Exception as e:
                    logger.warning("缓存检查失败", error=str(e))
                    cache_error = str(e)
                    step.set_output({"error": str(e)})
                finally:
                    tracer.end_step()
                    status = "error" if cache_error else "success"
                    meta: Dict[str, Any] = {"hit": cache_hit}
                    if cache_error:
                        meta["error"] = cache_error
                    await _stream_progress(stream, step_name, status, step_desc, meta)

            # 5. 编译 IR → SQL
            step_name = "IR2SQL编译"
            step_desc = "将IR编译为可执行SQL"
            step = tracer.start_step(step_name, "compilation", step_desc)
            await _stream_progress(stream, step_name, "started", step_desc, None)
            # 思考过程：开始编译
            await _stream_thinking(stream, "compile", "正在将查询意图编译为SQL语句...", done=False, step_status="started")
            step.set_input({"ir": ir.model_dump(), "user_context": user_context})

            # 加载全局规则（包含派生指标）
            global_rules = []
            try:
                from server.dependencies import get_global_rules_loader
                rules_loader = get_global_rules_loader(actual_connection_id)
                if rules_loader:
                    global_rules = await rules_loader.load_active_rules(
                        rule_types=['derived_metric', 'custom_instruction', 'default_filter'],
                        domain_id=None
                    )
                    step.add_metadata("global_rules", {
                        "count": len(global_rules),
                        "rule_types": sorted({r.get("rule_type") for r in global_rules if r.get("rule_type")})
                    })
            except Exception as e:
                logger.warning("加载全局规则失败", error=str(e))
                step.add_metadata("global_rules_error", str(e))

            try:
                # 延迟初始化：在编译前创建 compiler
                actual_compiler = await ensure_compiler()
                sql, join_fallbacks = await _compile_with_join_fallback(
                    actual_compiler,
                    ir,
                    user_context,
                    semantic_model,
                    global_rules
                )
            except CompilationError as ce:
                step.set_error(str(ce))
                tracer.end_step()
                raise

            current_ir_display = _ir_to_display_dict(ir, semantic_model)
            step.set_output({
                "sql": sql,
                "sql_length": len(sql),
                "ir_display": current_ir_display
            })
            if join_fallbacks:
                step.add_metadata("join_fallbacks", join_fallbacks)
            tracer.end_step()
            await _stream_progress(stream, step_name, "success", step_desc, {"sql_length": len(sql)})

            # === 输出思考过程：SQL编译结果 ===
            sql_preview = sql[:200] + "..." if len(sql) > 200 else sql
            sql_thinking_lines = [
                "正在编译生成SQL...",
                f"```sql\n{sql_preview}\n```"
            ]
            await _stream_thinking(stream, "compile", "\n".join(sql_thinking_lines), done=True, step_status="success")

            # 检查停止信号
            await _check_stop_signal("SQL编译")

            # 6. 如果只需要查看 SQL
            if request.explain_only:
                tracer.finalize({"status": "success", "explain_only": True})

                explain_result = QueryResult(
                    columns=[{"name": "sql", "type": "string"}],
                    rows=[[sql]],
                    meta={"sql": sql, "explain_only": True}
                )
                response = QueryResponse(
                    status="success",
                    result=explain_result,
                    query_id=query_id,
                    timestamp=timestamp
                )
                await _stream_result(stream, query_id, timestamp, explain_result)
                result_streamed = True
                return await _return_query_response(
                    response,
                    status="completed",
                    current_node="ir_ready",
                    state_updates={
                        "ir_ready": True,
                        "selected_table_ids": selected_table_ids,
                        "selected_table_id": effective_selected_table_id,
                        "sql_preview": sql,
                        "ir_snapshot": sanitize_for_json(ir.model_dump()),
                    },
                )

            # 7. 成本守护（SHOWPLAN）
            # 延迟初始化：在成本守护/执行前创建 executor
            actual_executor = await ensure_executor()
            
            if not request.force_execute:
                step_name = "成本守护"
                step_desc = "评估查询成本"
                step = tracer.start_step(step_name, "validation", step_desc)
                await _stream_progress(stream, step_name, "started", step_desc, None)
                step.set_input({"sql": sql})

                from server.exec.showplan import ShowPlanGuard

                engine = await actual_executor.ensure_engine()
                async with engine.connect() as conn:
                    guard = ShowPlanGuard(conn)
                    cost_info = await guard.estimate_cost(sql)

                step.set_output(cost_info)
                tracer.end_step()
                await _stream_progress(stream, step_name, "success", step_desc, cost_info)

                if not cost_info["safe_to_execute"]:
                    # 成本超限，需要确认
                    logger.warning("查询成本超限", **cost_info)
                    tracer.finalize({"status": "cost_exceeded"})

                    confirm_card = ConfirmationCard(
                        ir=ir,
                        natural_language=f"此查询将扫描约 {cost_info['estimated_rows']:,} 行数据",
                        warnings=cost_info["warnings"],
                        estimated_cost={
                            "rows": cost_info["estimated_rows"],
                            "cost": cost_info["estimated_cost"]
                        }
                    )

                    response = QueryResponse(
                        status="confirm_needed",
                        confirmation=confirm_card,
                        query_id=query_id,
                        timestamp=timestamp
                    )
                    await _stream_confirmation(stream, confirm_card)
                    return await _return_query_response(
                        response,
                        status="awaiting_user_action",
                        current_node="execution_guard",
                        state_updates={
                            "pending_actions": ["execution_decision", "revise", "change_table", "request_explanation", "exit_current"],
                            "execution_guard": sanitize_for_json(confirm_card.model_dump()),
                            "ir_snapshot": sanitize_for_json(ir.model_dump()),
                            "ir_ready": True,
                        },
                    )

            # 8. 执行查询
            # 检查停止信号（执行前最后确认）
            await _check_stop_signal("SQL执行前")

            step_name = "SQL执行"
            step_desc = "执行SQL查询"
            step = tracer.start_step(step_name, "execution", step_desc)
            await _stream_progress(stream, step_name, "started", step_desc, None)
            # 思考过程：开始执行
            await _stream_thinking(stream, "execute", "正在执行SQL查询，获取数据...", done=False, step_status="started")
            step.set_input({"sql": sql})

            dry_run_ok = await validation_loop.dry_run(
                sql,
                actual_connection_id,
                actual_executor.db_type,
                actual_executor
            )
            if not dry_run_ok:
                if settings.dry_run_mandatory:
                    raise HTTPException(status_code=500, detail="SQL Dry Run 校验失败")
                logger.warning(
                    "Dry Run 未通过，但根据配置继续执行",
                    query_id=query_id,
                    connection_id=actual_connection_id
                )

            result = await actual_executor.execute_async(sql)
            
            row_count = len(result.rows)
            column_count = len(result.columns)
            step.set_output({
                "row_count": row_count,
                "column_count": column_count
            })
            
            # 思考过程：执行完成
            await _stream_thinking(stream, "execute", f"查询完成，返回 {row_count} 条记录", done=True, step_status="success")
            tracer.end_step()
            await _stream_progress(stream, step_name, "success", step_desc, {
                "row_count": row_count,
                "column_count": column_count
            })

        # 8.5 为指标列添加单位信息和格式化配置（支持派生指标、原子指标和计算字段）
        derived_metrics_info = {}  # 存储指标信息，供后续格式化使用
        derived_metric_unit_changes: List[Dict[str, Any]] = []
        
        # 条件：有派生指标 或 有同比计算（无论是派生还是原子指标） 或 有计算字段
        has_derived = "derived:" in str(ir.metrics)
        has_comparison = ir.comparison_type and ir.show_growth_rate
        has_calculated_fields = bool(getattr(ir, 'calculated_fields', None))
        
        if has_derived or has_comparison or has_calculated_fields:
            try:
                # 构建指标名称到配置的映射（包含派生指标和原子指标）
                derived_config = {}
                
                # 1. 加载派生指标配置
                from server.dependencies import get_global_rules_loader
                rules_loader = get_global_rules_loader(actual_connection_id)
                if rules_loader:
                    global_rules = await rules_loader.load_active_rules(
                        rule_types=['derived_metric'],
                        domain_id=None
                    )
                    for rule in global_rules:
                        rule_def = rule.get('rule_definition', {})
                        display_name = rule_def.get('display_name', '')
                        unit = rule_def.get('unit', '')
                        decimal_places = rule_def.get('decimal_places', 2)
                        if display_name:
                            derived_config[display_name] = {
                                'unit': unit,
                                'decimal_places': decimal_places
                            }
                
                # 2. 加载原子指标配置（从 semantic_model 中获取）
                if semantic_model and has_comparison:
                    # 遍历 IR 中的指标，获取原子指标的单位信息
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
                        
                        if metric_id == "__row_count__" or (isinstance(metric_id, str) and metric_id.startswith('derived:')):
                            continue
                        
                        # 尝试从 semantic_model 获取字段信息
                        field = None
                        if hasattr(semantic_model, 'fields') and metric_id in semantic_model.fields:
                            field = semantic_model.fields[metric_id]
                        elif hasattr(semantic_model, 'measures') and metric_id in semantic_model.measures:
                            field = semantic_model.measures[metric_id]
                        
                        if field:
                            display_name = getattr(field, 'display_name', None) or getattr(field, 'name', '')
                            if display_name and display_name not in derived_config:
                                # 获取单位和小数位数
                                unit = ''
                                decimal_places = 2
                                if hasattr(field, 'measure_props') and field.measure_props:
                                    unit = getattr(field.measure_props, 'unit', '') or ''
                                    decimal_places = getattr(field.measure_props, 'decimal_places', 2)
                                elif hasattr(field, 'unit_conversion') and field.unit_conversion:
                                    unit = field.unit_conversion.get('display_unit', '')
                                
                                derived_config[display_name] = {
                                    'unit': unit,
                                    'decimal_places': decimal_places
                                }
                                logger.debug(f"添加原子指标配置: {display_name}, unit={unit}")

                # 3. 加载计算字段配置（从 calculated_fields 中获取，利用 LLM 提供的元信息）
                if semantic_model and has_calculated_fields:
                    for calc_field in ir.calculated_fields:
                        alias = calc_field.alias
                        if alias and alias not in derived_config:
                            # 优先使用 LLM 提供的配置
                            unit = getattr(calc_field, 'unit', None) or ''
                            decimal_places = getattr(calc_field, 'decimal_places', 2)
                            total_strategy = getattr(calc_field, 'total_strategy', None)
                            expression = getattr(calc_field, 'expression', '') or ''
                            aggregation = getattr(calc_field, 'aggregation', None)
                            
                            # 判断是否为比率类计算字段
                            # 1. 优先使用 LLM 的 total_strategy（recalculate 明确标识比率类）
                            # 2. sum/max/min/weighted_avg 明确标识非比率类
                            # 3. none（不显示合计）和其他情况：根据表达式推断
                            if total_strategy == 'recalculate':
                                is_ratio_calc = True
                            elif total_strategy in ('sum', 'max', 'min', 'weighted_avg'):
                                is_ratio_calc = False
                            else:
                                # 降级：根据表达式推断（包含除法符号的是比率类）
                                # total_strategy 为 'none' 或空时，仍需根据表达式判断
                                is_ratio_calc = '/' in expression
                            
                            # 如果计算字段没有定义单位，从引用字段继承
                            # 注意：比率类计算字段（包含除法）不应继承单位，因为比率的单位与原字段单位不同
                            if not unit and calc_field.field_refs and not is_ratio_calc:
                                for ref_id in calc_field.field_refs:
                                    ref_field = None
                                    if hasattr(semantic_model, 'fields') and ref_id in semantic_model.fields:
                                        ref_field = semantic_model.fields[ref_id]
                                    elif hasattr(semantic_model, 'measures') and ref_id in semantic_model.measures:
                                        ref_field = semantic_model.measures[ref_id]
                                    
                                    if ref_field:
                                        if hasattr(ref_field, 'measure_props') and ref_field.measure_props:
                                            unit = getattr(ref_field.measure_props, 'unit', '') or ''
                                            if decimal_places == 2:  # 只有默认值时才继承
                                                decimal_places = getattr(ref_field.measure_props, 'decimal_places', 2)
                                        elif hasattr(ref_field, 'unit_conversion') and ref_field.unit_conversion:
                                            unit = ref_field.unit_conversion.get('display_unit', '')
                                        
                                        if unit:
                                            break  # 找到单位后停止
                            elif is_ratio_calc and not unit:
                                logger.debug(f"比率类计算字段 {alias} 不继承单位，expression={expression}")
                            
                            derived_config[alias] = {
                                'unit': unit,
                                'decimal_places': decimal_places,
                                'is_ratio': is_ratio_calc,
                                'total_strategy': total_strategy,  # 保存 LLM 提供的合计策略
                                'expression': expression,
                                'aggregation': aggregation
                            }
                            logger.debug(f"添加计算字段配置: {alias}, unit={unit}, decimal_places={decimal_places}, is_ratio={is_ratio_calc}, total_strategy={total_strategy}")

                # 3.1 加载 conditional_metrics 配置（如"工业用地面积"）
                if semantic_model and hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
                    for cond_metric in ir.conditional_metrics:
                        alias = getattr(cond_metric, 'alias', '')
                        field_id = getattr(cond_metric, 'field', '')
                        if alias and alias not in derived_config:
                            # 从引用字段获取单位
                            unit = ''
                            decimal_places = 2
                            
                            if field_id:
                                ref_field = None
                                if hasattr(semantic_model, 'fields') and field_id in semantic_model.fields:
                                    ref_field = semantic_model.fields[field_id]
                                elif hasattr(semantic_model, 'measures') and field_id in semantic_model.measures:
                                    ref_field = semantic_model.measures[field_id]
                                
                                if ref_field:
                                    if hasattr(ref_field, 'measure_props') and ref_field.measure_props:
                                        unit = getattr(ref_field.measure_props, 'unit', '') or ''
                                        decimal_places = getattr(ref_field.measure_props, 'decimal_places', 2)
                                    elif hasattr(ref_field, 'unit_conversion') and ref_field.unit_conversion:
                                        unit = ref_field.unit_conversion.get('display_unit', '')
                            
                            derived_config[alias] = {
                                'unit': unit,
                                'decimal_places': decimal_places,
                                'is_ratio': False
                            }
                            logger.debug(f"添加条件聚合指标配置: {alias}, unit={unit}, decimal_places={decimal_places}")

                # 3.2 加载 ratio_metrics 配置（如"工业用地面积占比"）
                if semantic_model and hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
                    for ratio_metric in ir.ratio_metrics:
                        alias = getattr(ratio_metric, 'alias', '')
                        if alias and alias not in derived_config:
                            decimal_places = getattr(ratio_metric, 'decimal_places', 2)
                            derived_config[alias] = {
                                'unit': '%',  # 占比指标单位固定为 %
                                'decimal_places': decimal_places,
                                'is_ratio': True,
                                'total_strategy': 'recalculate'
                            }
                            logger.debug(f"添加占比指标配置: {alias}, unit=%, decimal_places={decimal_places}")

                # 4. 为指标列添加单位，并记录格式化信息（移到外层，支持所有情况）
                # 支持多种同比/增长率列名格式：
                # - Pivot模式：2023年楼面地价、2024年楼面地价、楼面地价_增长率
                # - Vertical模式：楼面地价、上年楼面地价、楼面地价_同比增长率
                
                import re
                
                # 后缀匹配模式
                comparison_suffixes = ['_去年同期', '_上季度', '_上月', '_上周', '_上期']
                growth_suffixes = ['_增长率', '_同比增长率', '_环比增长率']
                
                # 前缀匹配模式（用于 Pivot 和 Vertical 模式）
                # 如 "2023年楼面地价"、"上年楼面地价"
                year_prefix_pattern = re.compile(r'^(\d{4})年(.+)$')  # 2023年楼面地价
                prev_prefix_pattern = re.compile(r'^(上年|上月|上季度|上周)(.+)$')  # 上年楼面地价
                
                for col in result.columns:
                    col_name = col['name']
                    matched = False
                    
                    # 1. 处理原始派生指标列和计算字段
                    if col_name in derived_config:
                        config = derived_config[col_name]
                        original_name = col_name
                        derived_metrics_info[col_name] = config
                        if config['unit']:
                            new_name = f"{col_name}({config['unit']})"
                            col['name'] = new_name
                            derived_metrics_info[new_name] = config
                            derived_metric_unit_changes.append({
                                "column": original_name,
                                "unit": config['unit'],
                                "renamed_to": new_name,
                                "decimal_places": config.get('decimal_places')
                            })
                        else:
                            derived_metric_unit_changes.append({
                                "column": original_name,
                                "unit": None,
                                "decimal_places": config.get('decimal_places')
                            })
                        matched = True
                    
                    if matched:
                        continue
                    
                    # 2. 处理年份前缀格式（Pivot模式）：2023年楼面地价
                    year_match = year_prefix_pattern.match(col_name)
                    if year_match:
                        year_part = year_match.group(1)  # 2023
                        metric_name = year_match.group(2)  # 楼面地价
                        if metric_name in derived_config:
                            config = derived_config[metric_name]
                            derived_metrics_info[col_name] = config
                            if config['unit']:
                                new_name = f"{col_name}({config['unit']})"
                                col['name'] = new_name
                                derived_metrics_info[new_name] = config
                                derived_metric_unit_changes.append({
                                    "column": col_name,
                                    "unit": config['unit'],
                                    "renamed_to": new_name,
                                    "decimal_places": config.get('decimal_places')
                                })
                            matched = True
                    
                    if matched:
                        continue
                    
                    # 3. 处理通用前缀格式（Vertical模式）：上年楼面地价
                    prev_match = prev_prefix_pattern.match(col_name)
                    if prev_match:
                        prefix_part = prev_match.group(1)  # 上年
                        metric_name = prev_match.group(2)  # 楼面地价
                        if metric_name in derived_config:
                            config = derived_config[metric_name]
                            derived_metrics_info[col_name] = config
                            if config['unit']:
                                new_name = f"{col_name}({config['unit']})"
                                col['name'] = new_name
                                derived_metrics_info[new_name] = config
                                derived_metric_unit_changes.append({
                                    "column": col_name,
                                    "unit": config['unit'],
                                    "renamed_to": new_name,
                                    "decimal_places": config.get('decimal_places')
                                })
                            matched = True
                    
                    if matched:
                        continue
                    
                    # 4. 处理后缀格式：楼面地价_去年同期
                    for suffix in comparison_suffixes:
                        if col_name.endswith(suffix):
                            base_name = col_name[:-len(suffix)]
                            if base_name in derived_config:
                                config = derived_config[base_name]
                                derived_metrics_info[col_name] = config
                                if config['unit']:
                                    new_name = f"{col_name}({config['unit']})"
                                    col['name'] = new_name
                                    derived_metrics_info[new_name] = config
                                    derived_metric_unit_changes.append({
                                        "column": col_name,
                                        "unit": config['unit'],
                                        "renamed_to": new_name,
                                        "decimal_places": config.get('decimal_places')
                                    })
                            matched = True
                            break
                    
                    if matched:
                        continue
                    
                    # 5. 处理增长率列：楼面地价_增长率、楼面地价_同比增长率
                    # 增长率列的单位总是 %，不需要检查 base_name 是否在配置中
                    for suffix in growth_suffixes:
                        if col_name.endswith(suffix):
                            growth_config = {
                                'unit': '%',
                                'decimal_places': 2
                            }
                            derived_metrics_info[col_name] = growth_config
                            new_name = f"{col_name}(%)"
                            col['name'] = new_name
                            derived_metrics_info[new_name] = growth_config
                            derived_metric_unit_changes.append({
                                "column": col_name,
                                "unit": '%',
                                "renamed_to": new_name,
                                "decimal_places": 2
                            })
                            matched = True
                            break
            except Exception as e:
                logger.warning(f"添加派生指标单位失败: {e}")

        # 9. 格式化结果（添加单位、格式化数值）
        step_name = "结果格式化"
        step_desc = "格式化查询结果"
        step = tracer.start_step(step_name, "formatting", step_desc)
        await _stream_progress(stream, step_name, "started", step_desc, {
            "row_count": len(result.rows),
            "column_count": len(result.columns)
        })
        step.set_input({
            "query_type": ir.query_type,
            "row_count": len(result.rows),
            "column_count": len(result.columns)
        })
        if derived_metric_unit_changes:
            step.add_metadata("derived_metric_units", derived_metric_unit_changes)

        # 9.1 预处理：格式化派生指标列的数值
        derived_metric_value_details: List[Dict[str, Any]] = []
        if result.rows and derived_metrics_info:
            from decimal import Decimal, ROUND_HALF_UP

            # 找到需要格式化的列索引
            col_indices_to_format = {}
            for idx, col in enumerate(result.columns):
                if col['name'] in derived_metrics_info:
                    col_indices_to_format[idx] = derived_metrics_info[col['name']]['decimal_places']

            # 格式化每行数据中的派生指标列
            if col_indices_to_format:
                formatted_rows = []
                for row in result.rows:
                    formatted_row = list(row)
                    for col_idx, decimal_places in col_indices_to_format.items():
                        value = formatted_row[col_idx]
                        if value is not None:
                            try:
                                # 转换为Decimal并格式化
                                if isinstance(value, (int, float)):
                                    numeric_value = Decimal(str(value))
                                elif isinstance(value, Decimal):
                                    numeric_value = value
                                else:
                                    continue

                                # 四舍五入到指定小数位
                                quantize_str = '0.' + '0' * decimal_places
                                formatted = numeric_value.quantize(
                                    Decimal(quantize_str),
                                    rounding=ROUND_HALF_UP
                                )
                                # 转换为字符串，保留小数位
                                formatted_row[col_idx] = format(formatted, f'.{decimal_places}f')
                            except Exception as e:
                                logger.warning(f"格式化派生指标数值失败: {e}, value={value}")
                    formatted_rows.append(formatted_row)

                result.rows = formatted_rows
                logger.debug(f"已格式化{len(col_indices_to_format)}个派生指标列")
                derived_metric_value_details = [
                    {
                        "column": result.columns[idx]['name'],
                        "decimal_places": decimal_places
                    }
                    for idx, decimal_places in col_indices_to_format.items()
                ]
        if derived_metric_value_details:
            step.add_metadata("derived_metric_value_formatting", derived_metric_value_details)

        if result.rows:
            if getattr(ir, "query_type", None) in ["detail", "window_detail", "duplicate_detection"]:
                # 明细查询、窗口明细或重复检测：为列名添加单位，并对数值保留两位小数
                original_column_names = [col["name"] for col in result.columns]
                new_column_names = add_units_to_detail_columns(original_column_names, semantic_model)  # 传入语义模型

                # 使用批量格式化，避免重复构建格式化映射
                try:
                    logger.debug(
                        "开始格式化明细查询结果",
                        row_count=len(result.rows),
                        column_count=len(new_column_names),
                        column_names=new_column_names[:10]
                    )
                    formatted_dicts = format_detail_rows(result.rows, new_column_names, semantic_model)
                    logger.debug("明细查询结果格式化完成", rows=len(formatted_dicts))
                except Exception as e:
                    logger.error(
                        "格式化明细查询结果时发生异常",
                        error=str(e),
                        row_count=len(result.rows),
                        column_names=new_column_names[:10],
                        exc_info=True
                    )
                    raise

                if formatted_dicts:
                    # 获取格式化后的实际键名（可能与 new_column_names 不完全一致）
                    actual_keys = list(formatted_dicts[0].keys())
                    logger.debug(
                        "格式化后的键名",
                        expected_keys=new_column_names[:10],
                        actual_keys=actual_keys[:10],
                        keys_match=actual_keys == new_column_names
                    )
                    
                    # 如果键名不匹配，使用实际键名并记录警告
                    if actual_keys != new_column_names:
                        missing_keys = [k for k in new_column_names if k not in actual_keys]
                        extra_keys = [k for k in actual_keys if k not in new_column_names]
                        logger.warning(
                            "格式化后的键名与预期不一致",
                            expected_keys=new_column_names,
                            actual_keys=actual_keys,
                            missing_keys=missing_keys,
                            extra_keys=extra_keys
                        )
                        # 使用实际键名重新构建
                        new_column_names = actual_keys
                        new_rows = [[r.get(name, "") for name in new_column_names] for r in formatted_dicts]
                    else:
                        try:
                            new_rows = [[r[name] for name in new_column_names] for r in formatted_dicts]
                        except KeyError as e:
                            logger.error(
                                "访问格式化结果时发生KeyError",
                                error=str(e),
                                missing_key=str(e).strip("'"),
                                expected_keys=new_column_names[:10],
                                actual_keys=actual_keys[:10],
                                first_row_keys=list(formatted_dicts[0].keys())[:10] if formatted_dicts else [],
                                exc_info=True
                            )
                            # 使用实际键名重新构建
                            new_column_names = actual_keys
                            new_rows = [[r.get(name, "") for name in new_column_names] for r in formatted_dicts]
                    
                    new_columns = [{"name": name, "type": "string"} for name in new_column_names]
                    result.columns = new_columns
                    result.rows = new_rows
                    logger.debug("明细结果已格式化", rows=len(new_rows))
                    alias_mapping = []
                    for original, formatted in zip(original_column_names, new_column_names):
                        if original != formatted:
                            alias_mapping.append({"original": original, "formatted": formatted})
                    if alias_mapping:
                        step.add_metadata("detail_column_aliases", alias_mapping)
            else:
                # 聚合/其他查询：检查是否需要生成透视表
                if formatter.should_pivot(ir):
                    # 生成透视表
                    pivot_result = formatter.pivot_results(result.columns, result.rows, ir, global_rules)
                    if pivot_result:
                        result.columns = pivot_result["columns"]
                        result.rows = pivot_result["rows"]
                        logger.debug("已生成透视表", rows=len(result.rows), cols=len(result.columns))
                    else:
                        # 透视失败，使用常规格式化
                        formatted_dicts = formatter.format_results(result.columns, result.rows, ir, global_rules)
                        if formatted_dicts:
                            new_column_names = list(formatted_dicts[0].keys())
                            new_columns = [{"name": name, "type": "string"} for name in new_column_names]
                            new_rows = [[row[col] for col in new_column_names] for row in formatted_dicts]
                            result.columns = new_columns
                            result.rows = new_rows
                            logger.debug("结果已格式化", rows=len(new_rows))
                else:
                    # 常规格式化
                    formatted_dicts = formatter.format_results(result.columns, result.rows, ir, global_rules)
                    if formatted_dicts:
                        new_column_names = list(formatted_dicts[0].keys())
                        new_columns = [{"name": name, "type": "string"} for name in new_column_names]
                        new_rows = [[row[col] for col in new_column_names] for row in formatted_dicts]
                        result.columns = new_columns
                        result.rows = new_rows
                        logger.debug("结果已格式化", rows=len(new_rows))

        step.add_metadata(
            "final_columns",
            [col["name"] for col in result.columns]
        )
        step.set_output({
            "formatted": True,
            "row_count": len(result.rows),
            "column_count": len(result.columns)
        })
        tracer.end_step()
        await _stream_progress(stream, step_name, "success", step_desc, {
            "row_count": len(result.rows),
            "column_count": len(result.columns)
        })

        # 9.5 添加查询元信息（用于前端展示）
        if ir.query_type == "detail" and ir.sort_by:
            result.meta["sort_by"] = ir.sort_by
            result.meta["sort_order"] = ir.sort_order
            logger.debug("添加排序信息到meta", sort_by=ir.sort_by, sort_order=ir.sort_order)
        elif ir.query_type == "duplicate_detection" and ir.duplicate_by:
            result.meta["duplicate_by"] = ir.duplicate_by
            logger.debug("添加重复检测字段到meta", duplicate_by=ir.duplicate_by)

        # 9.6 生成过程说明与洞察（确定性）
        explain_step_name = "生成说明"
        explain_step_desc = "生成过程说明和洞察"
        step = tracer.start_step(explain_step_name, "explanation", explain_step_desc)
        await _stream_progress(stream, explain_step_name, "started", explain_step_desc, None)
        try:
            display_columns = [c["name"] for c in result.columns]
            # 获取字段关键词映射配置
            keyword_mapping = None
            if semantic_model.formatting and semantic_model.formatting.field_keyword_mapping:
                keyword_mapping = semantic_model.formatting.field_keyword_mapping

            step.set_input({
                "row_count": len(result.rows),
                "column_count": len(result.columns),
                "metrics_count": len(ir.metrics or []),
                "dimensions_count": len(ir.dimensions or []),
                "has_dag_steps": bool(result.meta.get("dag_process_steps") if result.meta else False)
            })

            process_explain = build_process_explanation(ir, display_columns, keyword_mapping, semantic_model)
            dag_steps = result.meta.get("dag_process_steps") if result.meta else None
            if dag_steps:
                if process_explain:
                    process_explain = list(dag_steps) + list(process_explain)
                else:
                    process_explain = list(dag_steps)
            # 确定排序字段：detail用sort_by，duplicate_detection用duplicate_by，其他用order_by
            sort_field = None
            if ir.query_type == "detail" and ir.sort_by:
                sort_field = ir.sort_by
            elif ir.query_type == "duplicate_detection" and ir.duplicate_by:
                sort_field = ir.duplicate_by[0] if ir.duplicate_by else None
            elif ir.order_by:
                sort_field = ir.order_by[0].field
            
            insights = build_insights(
                result.columns,
                result.rows,
                sort_field,
                keyword_mapping,
                ir,  # 传递IR用于检测缺失的维度值
                semantic_model  # 传递语义模型
            )
            result.meta["process_explanation"] = process_explain
            result.meta["insights"] = insights

            # 生成派生指标计算说明（增强版：包含数据来源和实际数值）
            derived_metrics_calc = await _build_derived_metrics_explanation(
                ir=ir,
                semantic_model=semantic_model,
                result_columns=result.columns,
                result_rows=result.rows,
                connection_id=actual_connection_id
            )
            logger.debug(f"生成了 {len(derived_metrics_calc)} 个指标说明", metrics=ir.metrics, calc_count=len(derived_metrics_calc))
            if derived_metrics_calc:
                result.meta["derived_calculations"] = derived_metrics_calc
                logger.debug("已添加计算指标说明到结果", calculations=derived_metrics_calc)
            else:
                logger.warning("未生成任何计算指标说明", metrics=ir.metrics)

            # 智能推荐可视化类型
            viz_hint = _suggest_visualization(ir, result.columns, result.rows)
            if viz_hint:
                result.visualization_hint = viz_hint

            if process_explain:
                step.add_metadata(
                    "process_explanation_preview",
                    process_explain[: min(5, len(process_explain))]
                )
            if insights:
                if isinstance(insights, dict):
                    preview_keys = list(insights.keys())[:3]
                    insights_preview = {k: insights[k] for k in preview_keys}
                elif isinstance(insights, list):
                    insights_preview = insights[: min(3, len(insights))]
                else:
                    # 其他类型（如字符串/元组）直接原样截断为字符串
                    insights_preview = insights
                step.add_metadata("insights_preview", insights_preview)
            if derived_metrics_calc:
                step.add_metadata(
                    "derived_calculation_fields",
                    [item.get("metric_name") or item.get("metric_id") for item in derived_metrics_calc][:5]
                )

            step.set_output({
                "has_process_explanation": bool(result.meta.get("process_explanation")),
                "has_insights": bool(result.meta.get("insights")),
                "derived_calculations_count": len(result.meta.get("derived_calculations", [])),
                "insight_count": len(insights or []),
                "process_explanation_lines": len(process_explain or [])
            })
            tracer.end_step()
            await _stream_progress(stream, explain_step_name, "success", explain_step_desc, {
                "has_process_explanation": bool(result.meta.get("process_explanation")),
                "insight_count": len(insights or [])
            })

        except Exception as e:
            logger.warning("生成说明/洞察失败", error=str(e))
            if tracer.current_step:
                tracer.current_step.set_error(str(e))
                tracer.end_step()
            await _stream_progress(stream, explain_step_name, "error", explain_step_desc, {"error": str(e)})

        if stream and not result_streamed and result:
            await _stream_result(stream, query_id, timestamp, result)
            result_streamed = True

        # 10. 生成自然语言叙述：仅同步等待（可配置超时），失败即放弃，不进行异步
        # 注意：调用方可以通过 request.disable_narrative 显式关闭该步骤（例如 Dify 工具调用场景）
        try:
            if (
                result.rows
                and settings.narrative_enabled
                and not getattr(request, "disable_narrative", False)
            ):
                narrative_step_name = "生成叙述"
                narrative_step_desc = "生成自然语言叙述"
                step = tracer.start_step(narrative_step_name, "narrative", narrative_step_desc)
                await _stream_progress(stream, narrative_step_name, "started", narrative_step_desc, None)
                
                # 构建facts，包含分组数据
                # 获取表名（优先使用 display_name，若无则使用实际表名）
                table_name = None
                selected_tables = []
                
                def get_display_name(ds):
                    """获取数据源的显示名，优先使用 display_name"""
                    # 如果 display_name 存在且不等于 datasource_name，说明是真正的显示名
                    if ds.display_name and ds.display_name != ds.datasource_name:
                        return ds.display_name
                    # 否则返回 display_name（可能等于 datasource_name）或 datasource_name
                    return ds.display_name or ds.datasource_name
                
                # 对于 multi_join 等多表查询，优先使用 ir.selected_table_info 获取所有表名
                if ir.cross_partition_query and ir.selected_table_ids and len(ir.selected_table_ids) > 1:
                    # 从 selected_table_info 获取所有参与查询的表的显示名
                    if ir.selected_table_info:
                        for tid in ir.selected_table_ids:
                            info = ir.selected_table_info.get(tid)
                            if info and info.get("display_name"):
                                selected_tables.append(info["display_name"])
                    
                    # 如果 selected_table_info 没有，尝试从 semantic_model.datasources 获取
                    if not selected_tables and semantic_model and semantic_model.datasources:
                        for tid in ir.selected_table_ids:
                            ds = semantic_model.datasources.get(tid)
                            if ds:
                                selected_tables.append(get_display_name(ds))
                    
                    # 设置主表名（使用第一个表作为主表）
                    if selected_tables:
                        table_name = selected_tables[0]
                else:
                    # 单表查询：优先从 IR 的 primary_table_id 获取主表的显示名
                    if ir.primary_table_id and semantic_model and semantic_model.datasources:
                        primary_ds = semantic_model.datasources.get(ir.primary_table_id)
                        if primary_ds:
                            table_name = get_display_name(primary_ds)
                            selected_tables.append(table_name)
                    
                    # 如果没有从 primary_table_id 获取到，遍历所有数据源
                    if not table_name and semantic_model and semantic_model.datasources:
                        for ds_id, ds in semantic_model.datasources.items():
                            ds_name = get_display_name(ds)
                            if ds_name:
                                selected_tables.append(ds_name)
                                if table_name is None:
                                    table_name = ds_name
                
                facts = {
                    "table_name": table_name,  # 主表名称
                    "selected_tables": selected_tables if len(selected_tables) > 1 else None,  # 多表查询时的表名列表
                    "process_explanation": result.meta.get("process_explanation"),
                    "insights": result.meta.get("insights"),
                    "derived_calculations": result.meta.get("derived_calculations"),  # 计算说明
                    "sql": sql,  # 生成的SQL语句
                    "sort_by": result.meta.get("sort_by"),
                    "sort_order": result.meta.get("sort_order"),
                    "original_question": ir.original_question,
                    "row_count": len(result.rows),
                    "column_count": len(result.columns),
                    "column_names": [c["name"] for c in result.columns],
                }
                
                # 添加结构化的筛选范围信息，让 LLM 准确获取数据来源范围
                if ir.filters:
                    from server.utils.field_display import get_field_display_name
                    filter_scope = []
                    for f in ir.filters:
                        field_display = get_field_display_name(f.field, semantic_model)
                        scope_item = {
                            "field": field_display,
                            "field_id": f.field,
                            "operator": f.op,
                        }
                        if f.op == "IN" and isinstance(f.value, list):
                            # 多值筛选：明确告诉 LLM 总数量
                            scope_item["value_count"] = len(f.value)
                            scope_item["value_preview"] = f.value[:5]  # 预览前5个
                            scope_item["all_values"] = f.value  # 全部值（供 LLM 参考）
                        else:
                            scope_item["value"] = f.value
                        filter_scope.append(scope_item)
                    if filter_scope:
                        facts["filter_scope"] = filter_scope
                
                # 添加权限上下文信息，让叙述生成器知道哪些字段被权限限制
                if permission_info.get("applied") and permission_info.get("restricted_fields"):
                    restricted_fields = permission_info.get("restricted_fields", {})
                    permission_context = []
                    for field_name, allowed_values in restricted_fields.items():
                        if isinstance(allowed_values, list) and len(allowed_values) == 1:
                            permission_context.append(f"数据已按{field_name}={allowed_values[0]}进行权限过滤")
                        elif isinstance(allowed_values, list):
                            values_preview = "、".join(str(v) for v in allowed_values[:3])
                            if len(allowed_values) > 3:
                                values_preview += f"等{len(allowed_values)}个值"
                            permission_context.append(f"数据已按{field_name}范围({values_preview})进行权限过滤")
                    if permission_context:
                        facts["permission_context"] = permission_context
                        facts["data_scope_note"] = "注意：返回数据仅包含当前用户有权访问的范围"

                context_highlights = []
                context_summaries = []
                dag_context_exports = result.meta.get("dag_context_exports")
                if dag_context_exports:
                    facts["dag_context_exports"] = dag_context_exports
                    for export_node in dag_context_exports:
                        node_exports = export_node.get("exports") or []
                        highlight_items = []
                        for export in node_exports:
                            if not export:
                                continue
                            if export.get("mode") == "value_list":
                                values = export.get("preview_values") or []
                                value_count = export.get("value_count")
                                highlight_items.append({
                                    "mode": export.get("mode"),
                                    "target_field": export.get("target_field"),
                                    "values": values,
                                    "value_count": value_count
                                })
                                if values:
                                    # 构造便于叙述使用的总结文本
                                    desc = export_node.get("description") or ""
                                    target_field = export.get("target_field") or ""
                                    value_preview = "、".join(values[:3])
                                    if value_count and value_count > len(values):
                                        value_preview = f"{value_preview}等{value_count}个"
                                    elif value_count and value_count == 1:
                                        value_preview = values[0]
                                    summary_parts = []
                                    if desc:
                                        summary_parts.append(desc)
                                    if target_field:
                                        summary_parts.append(target_field)
                                    summary_prefix = "的".join(summary_parts) if len(summary_parts) > 1 else "".join(summary_parts)
                                    if summary_prefix:
                                        context_summaries.append(f"{summary_prefix}为{value_preview}")
                                    else:
                                        context_summaries.append(f"{value_preview}")
                            else:
                                highlight_items.append({
                                    "mode": export.get("mode"),
                                    "target_field": export.get("target_field"),
                                    "summary": export.get("subquery")
                                })
                        if highlight_items:
                            context_highlights.append({
                                "step_id": export_node.get("node_id"),
                                "description": export_node.get("description"),
                                "highlights": highlight_items
                            })
                    if context_highlights:
                        facts["context_highlights"] = context_highlights
                    if context_summaries:
                        facts["context_summaries"] = context_summaries
                        facts["leading_conclusion"] = context_summaries[0]
                dag_process_steps = result.meta.get("dag_process_steps")
                if dag_process_steps:
                    facts["dag_process_steps"] = dag_process_steps
                dag_node_traces = result.meta.get("dag_node_traces")
                if dag_node_traces:
                    facts["dag_node_traces"] = dag_node_traces

                # 添加示例行（默认使用全部数据，可通过配置限制/禁用）
                sample_limit = getattr(settings, "narrative_sample_rows", None)
                if sample_limit is None or sample_limit < 0:
                    # None 或负数 → 不限数量，使用所有数据（仅用于测试/演示）
                    effective_limit = len(result.rows)
                elif sample_limit == 0:
                    effective_limit = 0  # 明确禁用样本
                else:
                    effective_limit = sample_limit

                if effective_limit > 0 and result.rows:
                    sample_rows = []
                    col_names = facts["column_names"]
                    for row in result.rows[:effective_limit]:
                        row_dict = {}
                        for idx, col_name in enumerate(col_names):
                            if idx < len(row):
                                row_dict[col_name] = row[idx]
                        if row_dict:
                            sample_rows.append(row_dict)
                    if sample_rows:
                        facts["sample_rows"] = sample_rows
                        facts["has_more_rows"] = len(result.rows) > effective_limit
                
                # 对于分组聚合查询，添加每个分组的具体数据
                if ir.query_type == "aggregation" and ir.dimensions and result.rows:
                    grouped_data = []
                    col_names = [c["name"] for c in result.columns]
                    
                    for row in result.rows:
                        # 跳过"合计"行（如果有with_total）
                        if row[0] == "合计":
                            continue
                        
                        # 构建结构化数据
                        row_dict = {}
                        for i, col_name in enumerate(col_names):
                            if i < len(row):
                                row_dict[col_name] = row[i]
                        grouped_data.append(row_dict)
                    
                    if grouped_data:
                        facts["grouped_data"] = grouped_data
                        logger.debug(f"添加了{len(grouped_data)}个分组的数据到facts")
                    
                    # 如果有合计行，单独提取
                    if ir.with_total and result.rows:
                        last_row = result.rows[-1]
                        if last_row[0] == "合计":
                            total_dict = {}
                            for i, col_name in enumerate(col_names):
                                if i < len(last_row):
                                    total_dict[col_name] = last_row[i]
                            facts["total_row"] = total_dict
                            logger.debug("添加了合计行数据到facts")
                step.set_input({
                    "row_count": len(result.rows),
                    "column_count": len(result.columns),
                    "sample_limit": effective_limit,
                    "has_process_explanation": bool(result.meta.get("process_explanation")),
                    "has_insights": bool(result.meta.get("insights"))
                })
                step.add_metadata(
                    "facts_overview",
                    {
                        "process_explanation_lines": len(facts.get("process_explanation") or []),
                        "insight_count": len(facts.get("insights") or []),
                        "sample_rows": len(facts.get("sample_rows") or []),
                        "has_context_highlights": bool(facts.get("context_highlights")),
                        "grouped_data_count": len(facts.get("grouped_data") or [])
                    }
                )
                import asyncio
                narrative_chunks: List[str] = []

                async def _gen():
                    # 使用叙述生成场景的客户端（支持独立配置模型）
                    llm = get_narrative_llm_client()
                    if stream:
                        async def _handle_chunk(chunk_text: str, done: bool):
                            if not stream:
                                return
                            if chunk_text:
                                narrative_chunks.append(chunk_text)
                                await stream.emit_narrative(chunk_text, False)
                            if done:
                                await stream.emit_narrative("", True)

                        # 传入 message_id 用于停止信号检查（通过 contextvars 获取）
                        message_id_for_check = _message_id_ctx.get(None)
                        try:
                            text = await stream_narrative(
                                facts,
                                chunk_callback=_handle_chunk,
                                llm_client=llm,
                                message_id=message_id_for_check
                            )
                        except QueryStoppedException:
                            # 流式生成被停止，保存当前已生成的内容
                            text = "".join(narrative_chunks) if narrative_chunks else None
                            logger.info("流式生成被停止，保存已生成内容", chunks_count=len(narrative_chunks), message_id=message_id_for_check)
                            raise  # 重新抛出，让上层处理
                        
                        if not text and narrative_chunks:
                            text = "".join(narrative_chunks)
                        return text

                    return await generate_narrative(facts, llm_client=llm)
                sync_timeout = settings.narrative_sync_timeout_seconds
                if sync_timeout and sync_timeout > 0:
                    try:
                        text = await asyncio.wait_for(_gen(), timeout=sync_timeout)
                        if text:
                            result.summary = text
                            preview = text[:120] + ("..." if len(text) > 120 else "")
                            step.set_output({"narrative_preview": preview, "length": len(text)})
                            await _stream_progress(stream, narrative_step_name, "success", narrative_step_desc, {"length": len(text)})
                        else:
                            result.meta["narrative_status"] = "failed"
                            step.set_output({"status": "failed"})
                            await _stream_progress(stream, narrative_step_name, "success", narrative_step_desc, {"status": "empty"})
                    except Exception as e:
                        result.meta["narrative_status"] = "failed"
                        step.set_error(str(e))
                        await _stream_progress(stream, narrative_step_name, "error", narrative_step_desc, {"error": str(e)})
                else:
                    # 关闭同步等待功能
                    result.meta["narrative_status"] = "disabled"
                    step.set_output({"status": "disabled"})
                    await _stream_progress(stream, narrative_step_name, "success", narrative_step_desc, {"status": "disabled"})

                tracer.end_step()
        except Exception as e:
            logger.warning("叙述生成触发失败", error=str(e))
            if tracer.current_step:
                tracer.current_step.set_error(str(e))
                tracer.end_step()

        # 11. 缓存结果
        if not request.skip_cache and not is_complex_executed: # 复杂查询暂不缓存，或者需要更复杂的缓存Key
             # 简单起见，只有非复杂查询才写缓存
             try:
                 await cache.set(cache_key, result)
             except Exception:
                 pass

        # 12. 计算总耗时（包括解析、编译、执行、格式化等所有步骤）
        total_time_ms = int((time.time() - start_time) * 1000) if 'start_time' in locals() else None
        
        # 将总耗时添加到结果元数据中
        if total_time_ms is not None:
            if result.meta is None:
                result.meta = {}
            # 保存总耗时（包括所有步骤）
            result.meta["total_time_ms"] = total_time_ms
            # 如果已有latency_ms（SQL执行时间），保留它用于调试
            # 但前端优先显示total_time_ms
            if "latency_ms" not in result.meta:
                result.meta["latency_ms"] = total_time_ms

        # 13. 保存查询历史
        try:
            await _save_query_history(
                query_id=query_id,
                connection_id=actual_connection_id,
                user_id=actual_user_id,  # 使用实际的用户ID（可能来自Token）
                original_question=effective_question_text or request.text or "(直接提供IR)",
                generated_sql=sql if 'sql' in locals() else None,
                execution_status="success",
                execution_time_ms=total_time_ms,
                result_row_count=len(result.rows) if result else None,
                error_message=None,
                intent_detection_result=ir.model_dump() if hasattr(ir, 'model_dump') else None
            )
        except Exception as e:
            logger.warning("保存查询历史失败", error=str(e), query_id=query_id)

        # 14. 返回结果
        logger.info("查询成功完成", rows=len(result.rows), query_id=query_id, total_time_ms=total_time_ms)

        few_shot_learning_info = {
            "scheduled": False,
            "auto_sync": settings.few_shot_immediate_sync
        }
        if is_complex_executed:
            few_shot_learning_info["reason"] = "complex_query_pipeline"
        else:
            few_shot_learning_info = await _schedule_few_shot_learning(sql, ir, result)
            if few_shot_learning_info.get("scheduled"):
                if not result.meta:
                    result.meta = {}
                result.meta["few_shot_learning"] = few_shot_learning_info

        # 完成追踪
        tracer.finalize({
            "status": "success",
            "row_count": len(result.rows),
            "column_count": len(result.columns),
            "few_shot_learning": few_shot_learning_info,
            "total_time_ms": total_time_ms
        })

        response = QueryResponse(
            status="success",
            result=result,
            auth_status=auth_status,
            query_id=query_id,
            timestamp=timestamp
        )

        if stream and not result_streamed and result:
            await _stream_result(stream, query_id, timestamp, result)
            result_streamed = True

        # 使用自定义序列化返回
        return await _return_json_query_response(
            response,
            status="completed",
            current_node="completed",
            state_updates={
                "ir_ready": True,
                "selected_table_ids": selected_table_ids,
                "selected_table_id": effective_selected_table_id,
                "sql_preview": sql if 'sql' in locals() else None,
                "ir_snapshot": sanitize_for_json(ir.model_dump()) if 'ir' in locals() and hasattr(ir, "model_dump") else None,
                "result_meta": result.meta if result else None,
            },
        )
    except QueryStoppedException as e:
        # 查询被用户取消
        logger.info("查询被用户取消", query_id=query_id, message_id=e.message_id, reason=e.reason)

        # 记录取消并完成追踪
        if tracer.current_step:
            tracer.current_step.set_error(f"用户取消: {e.reason}")
            tracer.end_step()
        tracer.finalize({
            "status": "cancelled",
            "reason": e.reason,
            "message_id": e.message_id,
            "cancelled_at_step": tracer.current_step.step_name if tracer.current_step else None
        })
        await _update_query_session(
            status="cancelled",
            current_node="cancelled",
            state_updates={
                "cancel_reason": e.reason,
                "cancelled_at_step": tracer.current_step.step_name if tracer.current_step else None,
            },
            last_error=e.reason,
        )

        # 重新抛出，让上层处理
        raise
    except (NL2SQLError, SecurityError) as e:
        # ... (保留原有错误处理) ...
        # 业务异常和安全异常
        logger.error("查询失败", error=str(e), query_id=query_id)

        # 保存查询历史（失败记录）
        try:
            error_msg = e.to_dict() if hasattr(e, 'to_dict') else str(e)
            # 使用 actual_connection_id 如果已定义，否则使用原始请求的 connection_id
            save_connection_id = actual_connection_id if 'actual_connection_id' in locals() and actual_connection_id else request.connection_id
            await _save_query_history(
                query_id=query_id,
                connection_id=save_connection_id,
                user_id=actual_user_id,  # 使用实际的用户ID（可能来自Token）
                original_question=effective_question_text or request.text or "(直接提供IR)",
                generated_sql=getattr(e, 'sql', None) or sql if 'sql' in locals() else None,
                execution_status="failed",
                execution_time_ms=None,
                result_row_count=None,
                error_message=error_msg.get("message") if isinstance(error_msg, dict) else str(error_msg),
                intent_detection_result=None
            )
        except Exception as save_error:
            logger.warning("保存查询历史失败", error=str(save_error), query_id=query_id)

        # 记录错误并完成追踪
        if tracer.current_step:
            tracer.current_step.set_error(str(e))
            tracer.end_step()
        tracer.finalize({"status": "error", "error": e.to_dict() if hasattr(e, 'to_dict') else str(e)})

        # 处理SecurityError
        if isinstance(e, SecurityError):
            response = QueryResponse(
                status="error",
                error=e.to_dict(),
                auth_status=auth_status,
                    query_id=query_id,
                timestamp=timestamp
            )
        else:
            response = QueryResponse(
                status="error",
                error=e.to_dict(),
                auth_status=auth_status,
                    query_id=query_id,
                timestamp=timestamp
            )
        await _stream_error(stream, response.error or {})
        return await _return_json_query_response(
            response,
            status="failed",
            current_node="failed",
            state_updates={"selected_table_ids": selected_table_ids if 'selected_table_ids' in locals() else []},
            last_error=(response.error or {}).get("message") if response.error else str(e),
        )


@router.get("/query-sessions/{query_id}")
async def get_query_session_snapshot(query_id: str):
    session_service = QuerySessionService()
    session = await session_service.get_session(UUID(query_id))
    if not session:
        raise HTTPException(status_code=404, detail="查询会话不存在")

    state = session.get("state_json") or {}
    confirmation_view = session.get("confirmation_view") or QuerySessionService.build_confirmation_view(session)
    return {
        "query_id": session["query_id"],
        "status": session["status"],
        "current_node": session["current_node"],
        "pending_actions": state.get("pending_actions", []),
        "confirmation_view": confirmation_view,
        "state": state,
        "session": session,
    }


@router.post("/query-sessions/{query_id}/actions")
async def submit_query_session_action(
    query_id: str,
    request: QuerySessionActionRequest,
    current_user: Optional[AdminUser] = Depends(get_optional_user),
):
    actor_type = request.actor_type
    actor_id = request.actor_id
    if current_user:
        actor_id = str(current_user.user_id)
        actor_type = "user"

    service = DraftActionService()
    try:
        result = await service.apply_action(
            query_id=UUID(query_id),
            action_type=request.action_type,
            payload=request.payload,
            natural_language_reply=request.natural_language_reply,
            draft_version=request.draft_version,
            actor_type=actor_type,
            actor_id=actor_id,
            idempotency_key=request.idempotency_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return result


@router.websocket("/query/stream")
async def query_stream_socket(websocket: WebSocket):
    """WebSocket 查询流接口：同一连接内推送进度、结果与叙述。支持会话管理和查询取消。"""
    await websocket.accept()
    cache = get_query_cache()
    emitter = QueryStreamEmitter(websocket)
    query_id: Optional[str] = None
    
    # 用于检查取消状态的变量
    query_cancelled = False
    registry = None
    db_conn = None

    try:
        payload = await websocket.receive_json()
    except WebSocketDisconnect:
        return
    except Exception as e:
        await websocket.send_json({
            "event": "error",
            "payload": {
                "error": {
                    "code": "INVALID_PAYLOAD",
                    "message": "无法解析请求数据",
                    "details": str(e)
                }
            }
        })
        await emitter.close()
        return

    try:
        request = QueryRequest(**payload)
    except ValidationError as e:
        await emitter.emit_error({
            "code": "INVALID_REQUEST",
            "message": "参数校验失败",
            "details": e.errors()
        })
        await emitter.close()
        return

    query_id = request.original_query_id or str(uuid.uuid4())
    emitter.bind_query(query_id)

    # 从WebSocket请求中尝试获取用户信息（如果前端传递了token）
    current_user = None
    auth_error = None  # 用于记录认证错误
    auth_token = payload.get('token') or payload.get('authorization')
    if auth_token:
        try:
            from server.services.auth_service import auth_service
            import asyncpg
            
            # 如果是 "Bearer xxx" 格式，提取token
            if auth_token.startswith('Bearer '):
                auth_token = auth_token[7:]
            
            token_data = auth_service.decode_access_token(auth_token)
            if not token_data:
                # Token 无效或已过期
                auth_error = {"code": "TOKEN_EXPIRED", "message": "登录已过期，请重新登录"}
            elif token_data:
                conn = await asyncpg.connect(
                    host=settings.postgres_host,
                    port=settings.postgres_port,
                    user=settings.postgres_user,
                    password=settings.postgres_password,
                    database=settings.postgres_db
                )
                try:
                    row = await conn.fetchrow(
                        """
                        SELECT 
                            user_id, username, role, is_active, 
                            email, full_name, created_at, updated_at, 
                            last_login_at AS last_login
                        FROM users 
                        WHERE user_id = $1 AND is_active = TRUE
                        """,
                        token_data.user_id
                    )
                    if row:
                        # 直接使用数据库角色值
                        current_user = AdminUser(
                            user_id=row['user_id'],
                            username=row['username'],
                            role=row['role'],
                            is_active=row['is_active'],
                            email=row.get('email'),
                            full_name=row.get('full_name'),
                            created_at=row['created_at'],
                            updated_at=row['updated_at'],
                            last_login=row.get('last_login')
                        )
                        logger.debug("WebSocket用户认证成功", user_id=str(row['user_id']), username=row['username'])
                    else:
                        # 用户不存在或已被禁用
                        auth_error = {"code": "USER_DISABLED", "message": "用户不存在或已被禁用"}
                finally:
                    await conn.close()
        except Exception as e:
            logger.warning(f"WebSocket Token解析失败: {e}")
            auth_error = {"code": "TOKEN_EXPIRED", "message": "登录已过期，请重新登录"}

    # 如果有认证错误，发送错误事件并关闭连接
    if auth_error:
        await websocket.send_json({
            "event": "auth_error",
            "payload": {
                "error": auth_error
            }
        })
        await emitter.close()
        return

    response_payload: Optional[Dict[str, Any]] = None
    conversation_id = request.conversation_id
    message_id = request.message_id
    
    # 实际使用的 message_id（可能是新创建的占位消息）
    actual_message_id = message_id
    assistant_message_id: Optional[UUID] = None

    # 注册查询到活跃查询表（用于取消功能）
    def is_valid_uuid(val: str) -> bool:
        """检查字符串是否为有效的 UUID 格式"""
        try:
            UUID(val)
            return True
        except (ValueError, TypeError):
            return False
    
    # ========== 先占位：立即创建消息记录 ==========
    # 请求开始时立即创建 processing 状态的记录
    if conversation_id and current_user:
        try:
            pool = await get_metadata_pool()
            db_conn = await pool.acquire()
            conv_service = ConversationService(db_conn)
            
            # 先保存用户消息
            user_msg = await conv_service.add_message(
                conversation_id=UUID(conversation_id),
                role='user',
                content=request.text or '',
                status='completed',
                query_params={
                    'connection_id': request.connection_id,
                    'domain_id': request.domain_id,
                    'explain_only': request.explain_only,
                    'force_execute': request.force_execute
                }
            )
            
            # 创建 assistant 占位消息（状态为 running）
            assistant_msg = await conv_service.create_placeholder_message(
                conversation_id=UUID(conversation_id),
                role='assistant',
                query_id=UUID(query_id),
                query_params={
                    'connection_id': request.connection_id,
                    'domain_id': request.domain_id
                }
            )
            assistant_message_id = assistant_msg['message_id']
            actual_message_id = str(assistant_message_id)
            
            # 设置 contextvar，供 query() 函数内部使用
            _message_id_ctx.set(actual_message_id)
            
            # 绑定 message_id 到 emitter（用于流式输出时的停止检查）
            emitter.bind_message(actual_message_id)
            
            logger.info("占位消息已创建", 
                       conversation_id=conversation_id, 
                       message_id=actual_message_id,
                       query_id=query_id)
        except Exception as e:
            logger.warning("创建占位消息失败", error=str(e), conversation_id=conversation_id)
            db_conn = None
    else:
        db_conn = None
    
    # 注册查询到活跃查询表（用于取消功能）
    try:
        if not db_conn:
            pool = await get_metadata_pool()
            db_conn = await pool.acquire()
        registry = ActiveQueryRegistry(db_conn)
        
        if current_user:
            await registry.register_query(
                query_id=UUID(query_id),
                user_id=UUID(str(current_user.user_id)),
                query_text=request.text or "",
                conversation_id=UUID(conversation_id) if conversation_id and is_valid_uuid(conversation_id) else None,
                message_id=assistant_message_id if assistant_message_id else (UUID(message_id) if message_id and is_valid_uuid(message_id) else None),
                connection_id=UUID(request.connection_id) if request.connection_id and is_valid_uuid(request.connection_id) else None,
                ws_connection_id=str(id(websocket))
            )
            logger.debug("查询已注册到活跃查询表", query_id=query_id)
    except Exception as e:
        logger.warning("注册查询到活跃查询表失败", error=str(e))

    # 启动后台任务监听取消请求
    async def check_cancel():
        nonlocal query_cancelled
        while not emitter.closed and not query_cancelled:
            try:
                if registry and current_user:
                    is_cancelled = await registry.is_cancelled(UUID(query_id))
                    if is_cancelled:
                        query_cancelled = True
                        logger.info("查询被用户取消", query_id=query_id)
                        await emitter.emit_cancelled("用户取消")
                        break
                await asyncio.sleep(0.5)  # 每0.5秒检查一次
            except Exception:
                break

    cancel_check_task = asyncio.create_task(check_cancel())

    ws_token = _stream_emitter_ctx.set(emitter)
    query_id_token = _query_id_ctx.set(query_id)
    query_error = None
    is_stopped_by_signal = False
    try:
        # 检查是否已取消（兼容旧逻辑）
        if query_cancelled:
            raise HTTPException(status_code=499, detail="查询已取消")
        
        # 检查 Redis 停止信号（新逻辑）
        if actual_message_id and StopSignalService.check_stop_signal(actual_message_id):
            is_stopped_by_signal = True
            logger.info("检测到停止信号，中断查询", message_id=actual_message_id, query_id=query_id)
            raise HTTPException(status_code=499, detail="查询已取消")
        
        # 创建 mock request 对象用于 WebSocket 场景
        mock_http_request = MockRequest(MockRequestState(
            auth_attempted=bool(auth_token),
            auth_success=current_user is not None,
            auth_error_code=None,
            auth_provider='websocket'
        ))
        response = await query(request, mock_http_request, cache, current_user)
        
        # 再次检查停止信号（查询过程中可能被停止）
        if actual_message_id and StopSignalService.check_stop_signal(actual_message_id):
            is_stopped_by_signal = True
            logger.info("查询过程中检测到停止信号", message_id=actual_message_id, query_id=query_id)
            raise HTTPException(status_code=499, detail="查询已取消")
        
        # 查询完成，标记为已完成
        if registry and current_user:
            await registry.mark_completed(UUID(query_id))
        
        if isinstance(response, JSONResponse):
            body = response.body
            if isinstance(body, (bytes, bytearray)):
                response_payload = json.loads(body.decode("utf-8"))
            else:
                response_payload = json.loads(body)
        elif isinstance(response, QueryResponse):
            response_payload = json.loads(response.model_dump_json())
        else:
            response_payload = None
    except QueryStoppedException as e:
        # 流式生成被停止信号中断
        is_stopped_by_signal = True
        query_cancelled = True
        logger.info("查询被停止信号中断", message_id=e.message_id, reason=e.reason)
        response_payload = {"status": "cancelled", "query_id": query_id}
    except HTTPException as e:
        if e.status_code == 499:
            # 查询取消，不发送错误
            is_stopped_by_signal = True
            query_cancelled = True
            response_payload = {"status": "cancelled", "query_id": query_id}
        else:
            query_error = e.detail if isinstance(e.detail, dict) else {"message": str(e.detail)}
            await emitter.emit_error({
                "code": "HTTP_ERROR",
                "message": "查询过程遇到错误",
                "details": query_error
            })
    except Exception as e:
        logger.error("WebSocket 查询流异常", error=str(e), exc_info=True)
        query_error = str(e)
        
        # 根据错误类型提供更友好的错误消息
        error_code = "INTERNAL_ERROR"
        error_message = "服务内部错误"
        
        error_str = str(e).lower()
        if "unable to connect" in error_str or "connection refused" in error_str:
            error_code = "DATABASE_CONNECTION_ERROR"
            error_message = "数据库连接失败，请检查数据源配置是否正确"
        elif "timeout" in error_str or "timed out" in error_str:
            error_code = "DATABASE_TIMEOUT"
            error_message = "数据库查询超时，请稍后重试"
        elif "permission denied" in error_str or "access denied" in error_str:
            error_code = "DATABASE_PERMISSION_ERROR"
            error_message = "数据库访问权限不足"
        elif "syntax error" in error_str or "near" in error_str:
            error_code = "SQL_SYNTAX_ERROR"
            error_message = "SQL语法错误"
        elif "does not exist" in error_str and ("table" in error_str or "column" in error_str):
            error_code = "SCHEMA_ERROR"
            error_message = "表或字段不存在，请检查数据源元数据"
        elif "adaptive server is unavailable" in error_str:
            error_code = "DATABASE_CONNECTION_ERROR"
            error_message = "SQL Server 数据库不可用，请检查数据源配置"
        
        await emitter.emit_error({
            "code": error_code,
            "message": error_message,
            "details": query_error
        })
    finally:
        _stream_emitter_ctx.reset(ws_token)
        _query_id_ctx.reset(query_id_token)
        cancel_check_task.cancel()
        try:
            await cancel_check_task
        except asyncio.CancelledError:
            pass
    
    # 更新消息到会话（占位消息已创建，这里只需要更新）
    if conversation_id and db_conn and current_user and assistant_message_id:
        try:
            conv_service = ConversationService(db_conn)
            
            # 检查是否被停止（使用 Redis 停止信号）
            is_stopped = False
            if actual_message_id:
                is_stopped = StopSignalService.check_stop_signal(actual_message_id)
            
            result = response_payload.get('result') if response_payload else None
            # 优先使用停止信号，其次使用 query_cancelled
            msg_status = 'cancelled' if (is_stopped or query_cancelled) else ('error' if query_error else 'completed')
            
            # 构建 result_data（包含 meta 信息）
            save_result_data = None
            if result:
                rows_data = result.get('rows', result.get('data', []))
                if rows_data:
                    save_result_data = {
                        'columns': result.get('columns', []),
                        'rows': rows_data,
                        'meta': result.get('meta', {}),
                        'visualization_hint': result.get('visualization_hint')
                    }
            
            # 更新占位消息
            await conv_service.update_message(
                message_id=assistant_message_id,
                content=result.get('summary', '') if result else '',
                sql_text=result.get('meta', {}).get('sql') or result.get('sql') if result else None,
                result_summary=result.get('summary') or result.get('explanation') if result else None,
                result_data=save_result_data,
                status=msg_status,
                error_message=str(query_error) if query_error else None
            )
            
            # 清除停止信号（如果存在）
            if actual_message_id:
                StopSignalService.clear_stop_signal(actual_message_id)
            
            # 自动生成会话标题（如果是第一条消息）
            if request.text:
                await conv_service.auto_generate_title(UUID(conversation_id), request.text)
                
            logger.debug("消息已更新", conversation_id=conversation_id, message_id=str(assistant_message_id), query_id=query_id, status=msg_status)
        except Exception as e:
            logger.warning("更新消息到会话失败", error=str(e), conversation_id=conversation_id, message_id=str(assistant_message_id) if assistant_message_id else None)
    
    # 释放数据库连接
    if db_conn:
        try:
            pool = await get_metadata_pool()
            await pool.release(db_conn)
        except Exception:
            pass

    if response_payload and not query_cancelled:
        await emitter.emit_completed(response_payload)
    elif not query_cancelled:
        await emitter.emit_completed({"status": "error"})
    await emitter.close()
