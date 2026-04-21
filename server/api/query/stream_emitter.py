"""
WebSocket 查询流事件发送器
"""

from typing import Dict, Any, Optional
import json
import structlog
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder

from server.models.api import ConfirmationCard, TableSelectionCard, QueryResult
from server.utils.json_utils import sanitize_for_json

logger = structlog.get_logger()


class QueryStreamEmitter:
    """WebSocket 查询流事件发送器"""

    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.query_id: Optional[str] = None
        self.message_id: Optional[str] = None
        self.thinking_steps: list[Dict[str, Any]] = []
        self.narrative_chunks: list[str] = []
        self.closed = False

    def bind_query(self, query_id: str):
        self.query_id = query_id
    
    def bind_message(self, message_id: str):
        """绑定消息ID（用于停止信号检查）"""
        self.message_id = message_id

    def _upsert_thinking_step(
        self,
        step: str,
        content: str,
        *,
        done: bool = False,
        step_status: str = "started",
    ) -> None:
        step_payload = {
            "step": step,
            "content": content,
            "done": done,
            "status": step_status,
        }
        for index, existing in enumerate(self.thinking_steps):
            if existing.get("step") == step:
                self.thinking_steps[index] = step_payload
                return
        self.thinking_steps.append(step_payload)

    def get_thinking_steps(self) -> list[Dict[str, Any]]:
        return sanitize_for_json(self.thinking_steps)

    def get_narrative_text(self) -> str:
        return "".join(self.narrative_chunks).strip()

    async def _send(self, message: Dict[str, Any]) -> bool:
        """发送消息到 WebSocket，返回是否成功"""
        if self.closed:
            return False
        try:
            # 使用 sanitize_for_json 确保所有数据（包括 datetime、UUID 等）可被序列化
            safe_message = sanitize_for_json(message)
            await self.websocket.send_json(safe_message)
            return True
        except WebSocketDisconnect:
            self.closed = True
            return False
        except Exception as e:
            logger.warning("WebSocket 发送消息失败", error=str(e), message_keys=list(message.keys()))
            self.closed = True
            return False

    async def emit(self, event: str, payload: Dict[str, Any]):
        # 如果 message_id 已设置，添加到 payload 中（供前端使用）
        message_payload = payload.copy()
        if hasattr(self, 'message_id') and self.message_id:
            message_payload['message_id'] = self.message_id
        
        await self._send({
            "event": event,
            "query_id": self.query_id,
            "payload": message_payload
        })

    async def emit_progress(
        self,
        step: str,
        status: str,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        await self.emit("progress", {
            "step": step,
            "status": status,
            "description": description,
            "metadata": metadata or {}
        })

    async def emit_result(self, payload: Dict[str, Any]):
        await self.emit("result", payload)

    async def emit_confirmation(
        self,
        confirmation: ConfirmationCard,
        *,
        current_node: Optional[str] = None,
        query_text: Optional[str] = None,
    ):
        payload = {
            "confirmation": confirmation.model_dump()
        }
        if current_node:
            payload["current_node"] = current_node
        if query_text:
            payload["query_text"] = query_text
        await self.emit("confirm", payload)

    async def emit_error(self, error_payload: Dict[str, Any]):
        await self.emit("error", {
            "error": error_payload
        })

    async def emit_narrative(self, chunk: str, done: bool):
        """推送叙述流式内容，支持停止信号检查"""
        # 在发送前检查停止信号（如果 message_id 已设置）
        if hasattr(self, 'message_id') and self.message_id:
            from server.services.stop_signal_service import StopSignalService, QueryStoppedException
            if StopSignalService.check_stop_signal(self.message_id):
                logger.info("发送叙述前检测到停止信号，中断发送", message_id=self.message_id)
                raise QueryStoppedException(self.message_id, "用户取消")

        if chunk:
            self.narrative_chunks.append(chunk)

        await self.emit("narrative", {
            "chunk": chunk,
            "done": done
        })

    async def emit_thinking(
        self,
        step: str,
        content: str,
        done: bool = False,
        step_status: str = "started"
    ):
        """
        推送思考/处理过程的详细内容（类似 Deep Research 效果）
        
        Args:
            step: 步骤名称，如 'table_selection', 'nl2ir', 'compile'
            content: 思考内容，可以是增量文本
            done: 该步骤是否完成
            step_status: 步骤状态 (started/success/error/warning)
        """
        self._upsert_thinking_step(step, content, done=done, step_status=step_status)
        await self.emit("thinking", {
            "step": step,
            "content": content,
            "done": done,
            "step_status": step_status
        })

    async def emit_completed(self, response_payload: Dict[str, Any]):
        await self.emit("completed", response_payload)

    async def emit_cancelled(self, reason: str = "用户取消"):
        """推送查询取消事件"""
        await self.emit("cancelled", {
            "reason": reason,
            "query_id": self.query_id
        })

    async def emit_table_selection(self, table_selection: TableSelectionCard, query_id: Optional[str] = None, query_text: Optional[str] = None):
        """推送表选择确认卡到前端"""
        await self.emit("table_selection", {
            "table_selection": table_selection.model_dump(),
            "query_id": query_id,  # 用于追踪关联
            "query_text": query_text or table_selection.question  # 原始查询文本
        })

    async def close(self):
        if not self.closed:
            self.closed = True
            try:
                await self.websocket.close()
            except RuntimeError:
                # 已关闭
                pass


# ============================================================
# 流推送辅助函数
# ============================================================

async def stream_progress(
    stream: Optional[QueryStreamEmitter],
    step: str,
    status: str,
    description: Optional[str],
    metadata: Optional[Dict[str, Any]] = None
):
    """推送进度到 WebSocket"""
    if stream:
        await stream.emit_progress(step, status, description, metadata)


async def stream_result(
    stream: Optional[QueryStreamEmitter],
    query_id: str,
    timestamp: str,
    result: QueryResult
) -> None:
    """推送结果到 WebSocket"""
    if stream and result:
        await stream.emit_result({
            "query_id": query_id,
            "timestamp": timestamp,
            "result": result.model_dump()
        })


async def stream_error(stream: Optional[QueryStreamEmitter], error_payload: Dict[str, Any]):
    """推送错误到 WebSocket"""
    if stream:
        await stream.emit_error(error_payload)


async def stream_confirmation(
    stream: Optional[QueryStreamEmitter],
    confirmation: ConfirmationCard,
    *,
    current_node: Optional[str] = None,
    query_text: Optional[str] = None,
):
    """推送 IR 确认卡到 WebSocket"""
    if stream and confirmation:
        await stream.emit_confirmation(
            confirmation,
            current_node=current_node,
            query_text=query_text,
        )


async def stream_table_selection(stream: Optional[QueryStreamEmitter], table_selection: TableSelectionCard, query_id: Optional[str] = None):
    """推送表选择确认卡到 WebSocket"""
    if stream and table_selection:
        await stream.emit_table_selection(table_selection, query_id)


async def stream_thinking(
    stream: Optional[QueryStreamEmitter],
    step: str,
    content: str,
    done: bool = False,
    step_status: str = "started"
):
    """推送思考/处理过程内容到 WebSocket"""
    if stream:
        await stream.emit_thinking(step, content, done, step_status)
