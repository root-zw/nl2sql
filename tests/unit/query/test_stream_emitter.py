from __future__ import annotations

import pytest

from server.models.api import ConfirmationCard
from server.api.query.stream_emitter import QueryStreamEmitter
from server.models.ir import IntermediateRepresentation


class DummyWebSocket:
    def __init__(self):
        self.messages = []

    async def send_json(self, payload):
        self.messages.append(payload)


@pytest.mark.asyncio
async def test_emit_narrative_accumulates_streamed_text():
    websocket = DummyWebSocket()
    emitter = QueryStreamEmitter(websocket)
    emitter.bind_query("q1")
    emitter.bind_message("m1")

    await emitter.emit_narrative("第一段", False)
    await emitter.emit_narrative("第二段", False)
    await emitter.emit_narrative("", True)

    assert emitter.get_narrative_text() == "第一段第二段"
    assert websocket.messages[-1]["event"] == "narrative"
    assert websocket.messages[-1]["payload"]["done"] is True


@pytest.mark.asyncio
async def test_emit_confirmation_includes_current_node_metadata():
    websocket = DummyWebSocket()
    emitter = QueryStreamEmitter(websocket)
    emitter.bind_query("q1")
    emitter.bind_message("m1")

    confirmation = ConfirmationCard(
        ir=IntermediateRepresentation(query_type="detail", original_question="查询今年成交总价"),
        natural_language="按当前方案执行",
    )

    await emitter.emit_confirmation(
        confirmation,
        current_node="execution_guard",
        query_text="查询今年成交总价",
    )

    assert websocket.messages[-1]["event"] == "confirm"
    assert websocket.messages[-1]["payload"]["current_node"] == "execution_guard"
    assert websocket.messages[-1]["payload"]["query_text"] == "查询今年成交总价"
    assert websocket.messages[-1]["payload"]["message_id"] == "m1"
