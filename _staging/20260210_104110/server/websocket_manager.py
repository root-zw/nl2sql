"""
WebSocket连接管理器
用于自动同步系统的实时状态通信
"""

import asyncio
import json
import structlog
from typing import Dict, List, Set, Optional, Any
from datetime import datetime
from enum import Enum

from fastapi import WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
import uuid

logger = structlog.get_logger()


class MessageType(str, Enum):
    """WebSocket消息类型"""
    SYNC_STATUS_UPDATE = "sync_status_update"
    SYNC_PROGRESS = "sync_progress"
    PENDING_CHANGES_UPDATE = "pending_changes_update"
    SYNC_STARTED = "sync_started"
    SYNC_COMPLETED = "sync_completed"
    SYNC_FAILED = "sync_failed"
    HEALTH_UPDATE = "health_update"
    ERROR = "error"


class WebSocketManager:
    """WebSocket连接管理器"""

    def __init__(self):
        # 活跃连接 {connection_id: {user_id: Set[WebSocket]}}
        self.active_connections: Dict[str, Dict[str, Set[WebSocket]]] = {}
        # 连接元数据 {connection_id: {user_id: {websocket_id: metadata}}}
        self.connection_metadata: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, connection_id: str, user_id: str):
        """接受WebSocket连接"""
        await websocket.accept()

        async with self._lock:
            if connection_id not in self.active_connections:
                self.active_connections[connection_id] = {}
                self.connection_metadata[connection_id] = {}

            if user_id not in self.active_connections[connection_id]:
                self.active_connections[connection_id][user_id] = set()
                self.connection_metadata[connection_id][user_id] = {}

            # 生成WebSocket ID并添加到连接池
            websocket_id = str(uuid.uuid4())
            self.active_connections[connection_id][user_id].add(websocket)

            # 记录连接元数据
            self.connection_metadata[connection_id][user_id][websocket_id] = {
                "websocket": websocket,
                "connected_at": datetime.utcnow(),
                "last_ping": datetime.utcnow()
            }

            logger.debug(
                "WebSocket连接已建立",
                connection_id=connection_id,
                user_id=user_id,
                websocket_id=websocket_id,
                total_connections=self._get_total_connections()
            )

            return websocket_id

    async def disconnect(self, websocket: WebSocket, connection_id: str, user_id: str):
        """断开WebSocket连接"""
        async with self._lock:
            if (connection_id in self.active_connections and
                user_id in self.active_connections[connection_id]):

                # 找到并移除WebSocket
                self.active_connections[connection_id][user_id].discard(websocket)

                # 清理连接元数据
                if connection_id in self.connection_metadata and user_id in self.connection_metadata[connection_id]:
                    metadata_to_remove = []
                    for ws_id, metadata in self.connection_metadata[connection_id][user_id].items():
                        if metadata["websocket"] == websocket:
                            metadata_to_remove.append(ws_id)

                    for ws_id in metadata_to_remove:
                        del self.connection_metadata[connection_id][user_id][ws_id]

                    # 如果用户没有其他连接，清理用户记录
                    if not self.connection_metadata[connection_id][user_id]:
                        del self.connection_metadata[connection_id][user_id]
                        del self.active_connections[connection_id][user_id]

                # 如果数据库连接没有其他用户连接，清理数据库连接记录
                if not self.active_connections[connection_id]:
                    del self.active_connections[connection_id]

                logger.debug(
                    "WebSocket连接已断开",
                    connection_id=connection_id,
                    user_id=user_id,
                    total_connections=self._get_total_connections()
                )

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """发送个人消息"""
        try:
            await websocket.send_text(json.dumps(jsonable_encoder(message), ensure_ascii=False))
        except Exception as e:
            logger.error("发送个人消息失败", error=str(e))

    async def broadcast_to_connection(self, message: dict, connection_id: str, exclude_websocket: Optional[WebSocket] = None):
        """向特定数据库连接的所有用户广播消息"""
        if connection_id not in self.active_connections:
            return

        message_text = json.dumps(jsonable_encoder(message), ensure_ascii=False)
        disconnected_websockets = []

        for user_id, websockets in self.active_connections[connection_id].items():
            for websocket in websockets:
                if websocket == exclude_websocket:
                    continue

                try:
                    await websocket.send_text(message_text)
                except Exception as e:
                    logger.warning("发送广播消息失败，标记连接为断开",
                                 connection_id=connection_id,
                                 user_id=user_id,
                                 error=str(e))
                    disconnected_websockets.append((websocket, connection_id, user_id))

        # 清理断开的连接
        for websocket, conn_id, user_id in disconnected_websockets:
            await self.disconnect(websocket, conn_id, user_id)

    async def broadcast_to_all(self, message: dict):
        """向所有连接广播消息"""
        for connection_id in list(self.active_connections.keys()):
            await self.broadcast_to_connection(message, connection_id)

    def _get_total_connections(self) -> int:
        """获取总连接数"""
        total = 0
        for connection_data in self.active_connections.values():
            for websockets in connection_data.values():
                total += len(websockets)
        return total

    async def get_connection_stats(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        async with self._lock:
            stats = {
                "total_connections": self._get_total_connections(),
                "database_connections": len(self.active_connections),
                "connection_details": {}
            }

            for connection_id, user_connections in self.active_connections.items():
                connection_stats = {
                    "total_users": len(user_connections),
                    "total_websockets": sum(len(websockets) for websockets in user_connections.values()),
                    "users": []
                }

                for user_id, websockets in user_connections.items():
                    user_stats = {
                        "user_id": user_id,
                        "websocket_count": len(websockets)
                    }
                    connection_stats["users"].append(user_stats)

                stats["connection_details"][connection_id] = connection_stats

            return stats

    async def ping_all_connections(self):
        """定期ping所有连接以保持活跃"""
        async with self._lock:
            all_connections = []
            for connection_id, user_connections in self.active_connections.items():
                for user_id, websockets in user_connections.items():
                    for websocket in websockets:
                        all_connections.append((websocket, connection_id, user_id))

        # 并发发送ping
        tasks = []
        for websocket, connection_id, user_id in all_connections:
            tasks.append(self._ping_websocket(websocket, connection_id, user_id))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _ping_websocket(self, websocket: WebSocket, connection_id: str, user_id: str):
        """ping单个WebSocket连接"""
        try:
            ping_message = {
                "type": "ping",
                "timestamp": datetime.utcnow().isoformat()
            }
            await websocket.send_text(json.dumps(ping_message))
        except Exception as e:
            logger.debug("ping失败，移除断开的连接",
                        connection_id=connection_id,
                        user_id=user_id,
                        error=str(e))
            await self.disconnect(websocket, connection_id, user_id)


# 全局WebSocket管理器实例
websocket_manager = WebSocketManager()


class SyncEventBroadcaster:
    """同步事件广播器"""

    def __init__(self, ws_manager: WebSocketManager):
        self.ws_manager = ws_manager

    async def broadcast_sync_started(self, connection_id: str, sync_id: str, sync_type: str):
        """广播同步开始事件"""
        message = {
            "type": MessageType.SYNC_STARTED,
            "data": {
                "connection_id": connection_id,
                "sync_id": sync_id,
                "sync_type": sync_type,
                "started_at": datetime.utcnow().isoformat()
            }
        }
        await self.ws_manager.broadcast_to_connection(message, connection_id)

    async def broadcast_sync_progress(self, connection_id: str, sync_id: str, progress: dict):
        """广播同步进度更新"""
        message = {
            "type": MessageType.SYNC_PROGRESS,
            "data": {
                "connection_id": connection_id,
                "sync_id": sync_id,
                "progress": progress,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        await self.ws_manager.broadcast_to_connection(message, connection_id)

    async def broadcast_sync_completed(self, connection_id: str, sync_id: str, result: dict):
        """广播同步完成事件"""
        message = {
            "type": MessageType.SYNC_COMPLETED,
            "data": {
                "connection_id": connection_id,
                "sync_id": sync_id,
                "result": result,
                "completed_at": datetime.utcnow().isoformat()
            }
        }
        await self.ws_manager.broadcast_to_connection(message, connection_id)

    async def broadcast_sync_failed(self, connection_id: str, sync_id: str, error: str):
        """广播同步失败事件"""
        message = {
            "type": MessageType.SYNC_FAILED,
            "data": {
                "connection_id": connection_id,
                "sync_id": sync_id,
                "error": error,
                "failed_at": datetime.utcnow().isoformat()
            }
        }
        await self.ws_manager.broadcast_to_connection(message, connection_id)

    async def broadcast_pending_changes_update(self, connection_id: str, changes_stats: dict):
        """广播待同步变更更新"""
        message = {
            "type": MessageType.PENDING_CHANGES_UPDATE,
            "data": {
                "connection_id": connection_id,
                "changes_stats": changes_stats,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        await self.ws_manager.broadcast_to_connection(message, connection_id)

    async def broadcast_health_update(self, connection_id: str, health_info: dict):
        """广播健康状态更新"""
        message = {
            "type": MessageType.HEALTH_UPDATE,
            "data": {
                "connection_id": connection_id,
                "health_info": health_info,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        await self.ws_manager.broadcast_to_connection(message, connection_id)

    async def broadcast_error(self, connection_id: str, error_info: dict):
        """广播错误信息"""
        message = {
            "type": MessageType.ERROR,
            "data": {
                "connection_id": connection_id,
                "error_info": error_info,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        await self.ws_manager.broadcast_to_connection(message, connection_id)


# 全局事件广播器实例
sync_event_broadcaster = SyncEventBroadcaster(websocket_manager)


async def start_websocket_background_tasks():
    """启动WebSocket后台任务"""
    # 定期ping任务
    async def ping_task():
        while True:
            try:
                await websocket_manager.ping_all_connections()
                await asyncio.sleep(30)  # 每30秒ping一次
            except Exception as e:
                logger.error("WebSocket ping任务失败", error=str(e))
                await asyncio.sleep(30)

    # 启动后台任务
    asyncio.create_task(ping_task())
    logger.info("WebSocket后台任务已启动")
