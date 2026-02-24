"""基于确定性事实的异步自然语言叙述生成（安全模式）。

- 输入：facts（由 process_explanation、insights、可选 metric_explanations 组成）
- 输出：简洁中文说明，不引入事实外信息。
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict, Any, Optional, Awaitable, Callable, AsyncIterator
import structlog
from fastapi.encoders import jsonable_encoder

from server.config import settings
from server.nl2ir.llm_client import LLMClient
from server.utils.prompt_loader import resolve_path

logger = structlog.get_logger()


def _load_prompt() -> str:
    """加载提示词模板。若找不到文件，使用配置的默认模板。"""
    default_path = Path(__file__).resolve().parents[2] / "prompts" / "narrative" / "prompt.txt"
    path = resolve_path(getattr(settings, "narrative_prompt_path", None), default_path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        # 从配置加载默认提示词
        from server.utils.text_templates import get_narrative_default_prompt
        default_prompt = get_narrative_default_prompt()
        if default_prompt:
            return default_prompt
        # 最后的后备方案
        return (
            "你是数据分析助手。必须先直接回答用户的问题（给出明确结论），"
            "随后用简短语言说明依据（检索条件/排序口径）与关键数据点（如分位数/极值/Top），"
            "所有数值必须来自 facts，不得编造。数值保留两位小数，语言自然通俗，输出 2-4 句话。"
        )


async def generate_narrative(facts: Dict[str, Any], llm_client: Optional[LLMClient] = None) -> Optional[str]:
    """使用大模型将确定性事实润色为自然语言叙述。

    非阻塞调用者流程：调用方应使用 asyncio.create_task 包装本函数。
    """
    if not settings.narrative_enabled:
        return None

    try:
        prompt = _load_prompt()
        # 如果未传入客户端，使用叙述场景的配置创建
        llm = llm_client or LLMClient(scenario="narrative")

        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(jsonable_encoder(facts), ensure_ascii=False)}
        ]

        # 使用可配置温度，默认0.0（确定性输出）
        resp = await llm.chat_completion(messages=messages, temperature=settings.narrative_temperature)
        text = llm.get_text_content(resp).strip()
        # 简单长度/安全校验
        if not text:
            return None
        max_length = max(0, settings.narrative_max_text_length)
        if max_length and len(text) > max_length:
            text = text[:max_length]
        return text
    except Exception as e:
        logger.warning("生成叙述失败", error=str(e))
        return None


async def stream_narrative(
    facts: Dict[str, Any],
    chunk_callback: Optional[Callable[[str, bool], Awaitable[None]]] = None,
    llm_client: Optional[LLMClient] = None,
    message_id: Optional[str] = None
) -> Optional[str]:
    """
    流式生成自然语言叙述。

    Args:
        facts: 事实数据
        chunk_callback: 每次生成片段时的回调，签名 (chunk_text, done)
        llm_client: 可复用的 LLM 客户端
        message_id: 消息ID，用于检查停止信号（可选）
    """
    # 导入停止信号服务（在 try 之前导入，确保 except 块可用）
    from server.services.stop_signal_service import StopSignalService, QueryStoppedException

    if not settings.narrative_enabled:
        return None

    try:
        prompt = _load_prompt()
        # 如果未传入客户端，使用叙述场景的配置创建
        llm = llm_client or LLMClient(scenario="narrative")

        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(jsonable_encoder(facts), ensure_ascii=False)}
        ]

        stream: AsyncIterator[Dict[str, Any]] = await llm.chat_completion_stream(
            messages=messages,
            temperature=settings.narrative_temperature
        )

        buffer: list[str] = []
        max_length = max(0, settings.narrative_max_text_length)

        async for chunk in stream:
            # 检查停止信号（每次循环都检查）
            if message_id:
                try:
                    StopSignalService.check_and_raise_if_stopped(message_id)
                except QueryStoppedException:
                    # 停止信号已触发，保存当前已生成的内容
                    logger.info("流式生成被停止", message_id=message_id, buffer_length=len("".join(buffer)))
                    if chunk_callback:
                        await chunk_callback("", True)  # 标记为完成，但实际是中断
                    return "".join(buffer).strip() or None
            
            text = llm.extract_stream_text(chunk)
            if not text:
                continue

            if max_length:
                remaining = max_length - len("".join(buffer))
                if remaining <= 0:
                    continue
                if len(text) > remaining:
                    text = text[:remaining]

            buffer.append(text)
            if chunk_callback:
                await chunk_callback(text, False)

        full_text = "".join(buffer).strip() or None
        if chunk_callback:
            await chunk_callback("", True)
        return full_text
    except QueryStoppedException:
        # 停止信号异常，重新抛出让上层处理
        raise
    except Exception as e:
        logger.warning("流式生成叙述失败", error=str(e))
        if chunk_callback:
            await chunk_callback("", True)
        return None
