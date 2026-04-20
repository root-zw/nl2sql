"""
会话管理 API 路由
提供多轮对话会话的 REST API
"""

from typing import List, Optional, Dict, Any
from uuid import UUID
import structlog
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from server.middleware.auth import get_current_active_user
from server.models.admin import User as AdminUser
from server.services.conversation_service import ConversationService, ActiveQueryRegistry
from server.services.followup_context_service import FollowupContextService
from server.utils.db_pool import get_metadata_pool

logger = structlog.get_logger()
router = APIRouter(prefix="/conversations", tags=["conversations"])


# ==================== Pydantic 模型 ====================

class CreateConversationRequest(BaseModel):
    """创建会话请求"""
    title: Optional[str] = Field(None, max_length=200, description="会话标题")
    connection_id: Optional[str] = Field(None, description="数据库连接ID")
    domain_id: Optional[str] = Field(None, description="业务域ID")


class UpdateConversationRequest(BaseModel):
    """更新会话请求"""
    title: Optional[str] = Field(None, max_length=200, description="会话标题")
    connection_id: Optional[str] = Field(None, description="数据库连接ID")
    domain_id: Optional[str] = Field(None, description="业务域ID")
    is_pinned: Optional[bool] = Field(None, description="是否置顶")


class ConversationResponse(BaseModel):
    """会话响应"""
    conversation_id: str
    user_id: str
    title: Optional[str] = None
    connection_id: Optional[str] = None
    domain_id: Optional[str] = None
    connection_name: Optional[str] = None
    domain_name: Optional[str] = None
    is_active: bool = True
    is_pinned: bool = False
    message_count: Optional[int] = None
    created_at: str
    updated_at: str
    last_message_at: Optional[str] = None


class ConversationListResponse(BaseModel):
    """会话列表响应"""
    items: List[ConversationResponse]
    total: int


class MessageResponse(BaseModel):
    """消息响应"""
    message_id: str
    conversation_id: str
    role: str  # 'user' or 'assistant'
    content: str
    query_id: Optional[str] = None
    sql_text: Optional[str] = None
    result_summary: Optional[str] = None
    result_data: Optional[dict] = None
    status: str = 'completed'
    error_message: Optional[str] = None
    query_params: Optional[dict] = None
    context_message_ids: Optional[List[str]] = None
    metadata: Optional[dict] = None
    created_at: str
    updated_at: str


class ConversationDetailResponse(BaseModel):
    """会话详情响应（包含消息）"""
    conversation: ConversationResponse
    messages: List[MessageResponse]


class FollowupContextResolutionRequest(BaseModel):
    """结果后追问上下文解析请求"""
    text: str = Field(..., min_length=1, description="用户输入")
    context_depth: Optional[int] = Field(None, ge=0, le=20, description="上下文深度（覆盖默认值）")


class FollowupContextResolutionResponse(BaseModel):
    """结果后追问上下文解析响应"""
    resolution: str
    analysis_context: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


# ==================== 依赖注入 ====================

async def get_db_connection():
    """获取数据库连接"""
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


# ==================== 会话 CRUD API ====================

@router.post("", response_model=ConversationResponse)
async def create_conversation(
    request: CreateConversationRequest,
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """创建新会话"""
    service = ConversationService(db)
    
    # 解析 UUID
    connection_id = UUID(request.connection_id) if request.connection_id else None
    domain_id = UUID(request.domain_id) if request.domain_id else None
    
    # 清理旧会话（如果超出限制）
    await service.cleanup_old_conversations(UUID(str(current_user.user_id)))
    
    # 创建会话
    conversation = await service.create_conversation(
        user_id=UUID(str(current_user.user_id)),
        title=request.title,
        connection_id=connection_id,
        domain_id=domain_id
    )
    
    return ConversationResponse(**conversation)


@router.get("", response_model=ConversationListResponse)
async def list_conversations(
    limit: int = Query(50, ge=1, le=200, description="返回数量"),
    offset: int = Query(0, ge=0, description="偏移量"),
    include_inactive: bool = Query(False, description="是否包含已删除的会话"),
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """获取会话列表"""
    service = ConversationService(db)
    
    conversations = await service.list_conversations(
        user_id=UUID(str(current_user.user_id)),
        limit=limit,
        offset=offset,
        include_inactive=include_inactive
    )
    
    return ConversationListResponse(
        items=[ConversationResponse(**c) for c in conversations],
        total=len(conversations)
    )


# ==================== 查询取消 API（必须在 /{conversation_id} 之前定义）====================

class StopMessageRequest(BaseModel):
    """停止消息请求"""
    message_id: str = Field(..., description="消息ID")


@router.post("/messages/stop", response_model=Dict[str, Any])
async def stop_message(
    request: StopMessageRequest = Body(...),
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """
    停止消息生成
    使用 Redis 停止信号机制，立即中断流式生成
    """
    from server.services.stop_signal_service import StopSignalService
    from server.services.conversation_service import ConversationService
    
    message_id = request.message_id
    
    try:
        msg_uuid = UUID(message_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="无效的消息ID格式")
    
    # 验证消息是否存在且属于当前用户
    conv_service = ConversationService(db)
    message = await conv_service.get_message(msg_uuid)
    
    if not message:
        raise HTTPException(status_code=404, detail="消息不存在")
    
    # 检查消息状态
    if message.get('status') not in ('running', 'pending'):
        return {
            "success": False,
            "message": f"消息状态为 {message.get('status')}，无法停止",
            "message_id": message_id
        }
    
    # 设置 Redis 停止信号
    success = StopSignalService.set_stop_signal(message_id)
    
    if not success:
        return {
            "success": False,
            "message": "设置停止信号失败，Redis可能未启用",
            "message_id": message_id
        }
    
    # 同时标记查询为取消中（兼容旧逻辑）
    query_id = message.get('query_id')
    if query_id:
        try:
            from server.services.conversation_service import ActiveQueryRegistry
            registry = ActiveQueryRegistry(db)
            await registry.mark_cancelling(UUID(query_id), UUID(str(current_user.user_id)))
        except Exception as e:
            logger.warning("标记查询为取消中失败", query_id=query_id, error=str(e))
    
    logger.info("停止信号已设置", message_id=message_id, user_id=str(current_user.user_id))
    
    return {
        "success": True,
        "message": "停止信号已发送",
        "message_id": message_id
    }


@router.get("/queries/running")
async def get_running_queries(
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """获取当前用户正在执行的查询"""
    registry = ActiveQueryRegistry(db)
    
    queries = await registry.get_user_running_queries(UUID(str(current_user.user_id)))
    
    return {
        "queries": [
            {
                "query_id": str(q['query_id']),
                "conversation_id": str(q['conversation_id']) if q.get('conversation_id') else None,
                "message_id": str(q['message_id']) if q.get('message_id') else None,
                "query_text": q['query_text'],
                "started_at": q['started_at'].isoformat() if q.get('started_at') else None
            }
            for q in queries
        ]
    }


# ==================== 会话详情 API ====================

@router.get("/{conversation_id}", response_model=ConversationDetailResponse)
async def get_conversation(
    conversation_id: str,
    include_result_data: bool = Query(True, description="是否包含查询结果数据"),
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """获取会话详情（包含消息历史）"""
    service = ConversationService(db)
    
    conversation = await service.get_conversation(
        conversation_id=UUID(conversation_id),
        user_id=UUID(str(current_user.user_id))
    )
    
    if not conversation:
        raise HTTPException(status_code=404, detail="会话不存在")
    
    messages = await service.get_messages(
        conversation_id=UUID(conversation_id),
        include_result_data=include_result_data
    )
    
    return ConversationDetailResponse(
        conversation=ConversationResponse(**conversation),
        messages=[MessageResponse(**m) for m in messages]
    )


@router.patch("/{conversation_id}", response_model=ConversationResponse)
async def update_conversation(
    conversation_id: str,
    request: UpdateConversationRequest,
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """更新会话"""
    service = ConversationService(db)
    
    # 解析 UUID
    connection_id = UUID(request.connection_id) if request.connection_id else None
    domain_id = UUID(request.domain_id) if request.domain_id else None
    
    conversation = await service.update_conversation(
        conversation_id=UUID(conversation_id),
        user_id=UUID(str(current_user.user_id)),
        title=request.title,
        connection_id=connection_id,
        domain_id=domain_id,
        is_pinned=request.is_pinned
    )
    
    if not conversation:
        raise HTTPException(status_code=404, detail="会话不存在")
    
    return ConversationResponse(**conversation)


@router.delete("/{conversation_id}")
async def delete_conversation(
    conversation_id: str,
    hard_delete: bool = Query(False, description="是否硬删除"),
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """删除会话"""
    service = ConversationService(db)
    
    deleted = await service.delete_conversation(
        conversation_id=UUID(conversation_id),
        user_id=UUID(str(current_user.user_id)),
        hard_delete=hard_delete
    )
    
    if not deleted:
        raise HTTPException(status_code=404, detail="会话不存在")
    
    return {"success": True, "message": "会话已删除"}


# ==================== 上下文 API ====================

@router.get("/{conversation_id}/context")
async def get_conversation_context(
    conversation_id: str,
    depth: Optional[int] = Query(None, ge=0, le=20, description="上下文深度（覆盖默认值）"),
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """获取会话的对话上下文（用于多轮对话）"""
    service = ConversationService(db)
    
    # 验证会话归属
    conversation = await service.get_conversation(
        conversation_id=UUID(conversation_id),
        user_id=UUID(str(current_user.user_id))
    )
    
    if not conversation:
        raise HTTPException(status_code=404, detail="会话不存在")
    
    context = await service.get_recent_context(
        conversation_id=UUID(conversation_id),
        depth=depth
    )
    
    return {
        "conversation_id": conversation_id,
        "context_depth": depth,
        "messages": context
    }


@router.post("/{conversation_id}/followup-context-resolution", response_model=FollowupContextResolutionResponse)
async def resolve_followup_context(
    conversation_id: str,
    request: FollowupContextResolutionRequest,
    current_user: AdminUser = Depends(get_current_active_user),
    db=Depends(get_db_connection)
):
    """显式解析结果后追问上下文"""
    service = ConversationService(db)

    conversation = await service.get_conversation(
        conversation_id=UUID(conversation_id),
        user_id=UUID(str(current_user.user_id))
    )
    if not conversation:
        raise HTTPException(status_code=404, detail="会话不存在")

    context_messages = await service.get_recent_context(
        conversation_id=UUID(conversation_id),
        depth=request.context_depth
    )
    resolution = FollowupContextService.resolve_followup_context(
        request.text,
        context_messages,
    )
    return FollowupContextResolutionResponse(**resolution)
