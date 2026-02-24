"""
Embedding 批量生成工具
"""

from __future__ import annotations

from typing import Iterable, List

import structlog

logger = structlog.get_logger()


async def generate_embeddings(embedding_client, texts: Iterable[str],
                              batch_size: int = 16, context: str | None = None) -> List[List[float]]:
    """
    批量生成向量，若批量接口失败则自动回退到单条生成
    """
    texts = list(texts)
    if not texts:
        return []

    try:
        vectors = await embedding_client.embed_batch(texts, batch_size=batch_size)
        if vectors:
            return vectors
    except Exception as exc:
        logger.debug(
            "批量生成向量失败，尝试回退到单条模式",
            error=str(exc),
            context=context
        )

    results: List[List[float]] = []
    for text in texts:
        vector = await embedding_client.embed_single(text)
        results.append(vector)

    return results




















