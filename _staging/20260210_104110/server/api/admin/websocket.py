"""
WebSocket API端点 - 用于自动同步系统的实时状态通信
"""

import json
import structlog
from typing import Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from server.api.admin.auth import decode_token
from server.api.admin.auto_sync import (
    get_sync_service,
    build_auto_sync_status_payload,
    build_pending_changes_payload,
    build_sync_health_payload
)
from server.sync.unified_sync_service import UnifiedSyncService
from server.websocket_manager import websocket_manager
from server.config import settings

logger = structlog.get_logger()
router = APIRouter()


@router.websocket("/ws/{connection_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    connection_id: str
):
    """WebSocket连接端点"""
    token = websocket.query_params.get("token")

    if not token:
        await websocket.close(code=4401, reason="Unauthorized")
        return

    try:
        user = await _authenticate_websocket_user(token)
        user_id = str(user['user_id'])
    except Exception as auth_error:
        logger.warning("WebSocket身份验证失败", error=str(auth_error))
        await websocket.close(code=4401, reason="Unauthorized")
        return

    try:
        sync_service = None
        try:
            sync_service = get_sync_service()
        except Exception as service_error:
            logger.warning("同步服务未初始化，WebSocket将以降级模式运行", error=str(service_error))

        websocket_id = await websocket_manager.connect(websocket, connection_id, user_id)

        welcome_message = {
            "type": "connection_established",
            "data": {
                "connection_id": connection_id,
                "user_id": user_id,
                "websocket_id": websocket_id,
                "message": "WebSocket连接已建立"
            }
        }
        await websocket_manager.send_personal_message(welcome_message, websocket)

        # 监听客户端消息
        while True:
            try:
                # 接收客户端消息
                data = await websocket.receive_text()
                message = json.loads(data)

                # 处理不同类型的消息
                await handle_client_message(
                    message,
                    websocket,
                    connection_id,
                    user_id,
                    sync_service
                )

            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                error_message = {
                    "type": "error",
                    "data": {
                        "error": "无效的JSON格式",
                        "timestamp": "now"
                    }
                }
                await websocket_manager.send_personal_message(error_message, websocket)
            except Exception as e:
                logger.error("处理WebSocket消息失败", error=str(e), user_id=user_id, connection_id=connection_id)
                break

    except WebSocketDisconnect:
        logger.debug("WebSocket连接断开", connection_id=connection_id, user_id=user_id)
    except Exception as e:
        logger.error("WebSocket连接异常", error=str(e), connection_id=connection_id, user_id=user_id)
    finally:
        # 清理连接
        await websocket_manager.disconnect(websocket, connection_id, user_id)


async def handle_client_message(
    message: dict,
    websocket: WebSocket,
    connection_id: str,
    user_id: str,
    sync_service: Optional[UnifiedSyncService]
):
    """处理客户端发送的消息"""
    message_type = message.get("type")
    data = message.get("data", {})

    if message_type in {"get_status", "get_pending_changes", "get_sync_health"} and not sync_service:
        await websocket_manager.send_personal_message({
            "type": "error",
            "data": {
                "error": "同步服务未初始化",
                "error_type": "sync_service_unavailable"
            }
        }, websocket)
        return

    if message_type == "ping":
        # 响应ping消息
        pong_message = {
            "type": "pong",
            "data": {
                "timestamp": data.get("timestamp"),
                "server_timestamp": "now"
            }
        }
        await websocket_manager.send_personal_message(pong_message, websocket)

    elif message_type == "get_status":
        try:
            payload = await build_auto_sync_status_payload(
                UUID(connection_id),
                sync_service
            )

            await websocket_manager.send_personal_message({
                "type": "status_update",
                "data": payload
            }, websocket)

        except Exception as e:
            await websocket_manager.send_personal_message({
                "type": "error",
                "data": {
                    "error": f"获取状态失败: {str(e)}",
                    "error_type": "status_fetch_failed"
                }
            }, websocket)

    elif message_type == "get_pending_changes":
        try:
            limit = int(data.get("limit", 50))
            entity_types = data.get("entity_types")

            payload = await build_pending_changes_payload(
                UUID(connection_id),
                sync_service,
                limit=limit,
                entity_types=entity_types
            )

            await websocket_manager.send_personal_message({
                "type": "pending_changes_update",
                "data": payload
            }, websocket)

        except Exception as e:
            await websocket_manager.send_personal_message({
                "type": "error",
                "data": {
                    "error": f"获取待同步变更失败: {str(e)}",
                    "error_type": "changes_fetch_failed"
                }
            }, websocket)

    elif message_type == "get_sync_health":
        try:
            payload = await build_sync_health_payload(
                UUID(connection_id),
                sync_service
            )

            await websocket_manager.send_personal_message({
                "type": "health_update",
                "data": {
                    "health_info": payload
                }
            }, websocket)

        except Exception as e:
            await websocket_manager.send_personal_message({
                "type": "error",
                "data": {
                    "error": f"获取健康状态失败: {str(e)}",
                    "error_type": "health_fetch_failed"
                }
            }, websocket)

    elif message_type == "subscribe_events":
        # 订阅特定事件（客户端可以指定感兴趣的事件类型）
        events = data.get("events", [])

        # 这里可以实现事件订阅逻辑
        # 目前所有连接都会收到所有事件，后续可以优化为基于订阅的过滤

        subscribe_message = {
            "type": "subscription_confirmed",
            "data": {
                "subscribed_events": events,
                "message": "事件订阅成功"
            }
        }
        await websocket_manager.send_personal_message(subscribe_message, websocket)

    else:
        # 未知消息类型
        error_message = {
            "type": "error",
            "data": {
                "error": f"未知消息类型: {message_type}",
                "error_type": "unknown_message_type"
            }
        }
        await websocket_manager.send_personal_message(error_message, websocket)


async def _authenticate_websocket_user(token: str) -> dict:
    """基于JWT令牌的WebSocket身份认证"""
    payload = decode_token(token)
    user_id = payload.get("user_id")

    if not user_id:
        raise ValueError("令牌中缺少用户ID")

    conn = await asyncpg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        database=settings.postgres_db
    )
    try:
        user = await conn.fetchrow(
            """
            SELECT user_id, username, role, is_active
            FROM users
            WHERE user_id = $1
            """,
            UUID(user_id)
        )

        if not user or not user['is_active']:
            raise ValueError("用户不存在或已被禁用")

        return dict(user)
    finally:
        await conn.close()


@router.get("/ws/stats")
async def get_websocket_stats():
    """获取WebSocket连接统计信息"""
    try:
        stats = await websocket_manager.get_connection_stats()
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error("获取WebSocket统计信息失败", error=str(e))
        return {
            "success": False,
            "error": str(e)
        }


# 导入时启动WebSocket后台任务
try:
    from server.websocket_manager import start_websocket_background_tasks
    import asyncio

    # 在应用启动时调用
    async def init_websocket():
        await start_websocket_background_tasks()
        logger.debug("WebSocket服务初始化完成")

    # 这里可以注册到应用生命周期中
    # 在main.py中调用init_websocket()

except Exception as e:
    logger.error("WebSocket服务初始化失败", error=str(e))
