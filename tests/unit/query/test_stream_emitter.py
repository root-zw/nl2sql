from __future__ import annotations

import pytest

from server.api.query.stream_emitter import QueryStreamEmitter


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
