"""
Milvus 度量字段（field）级混合检索：Dense + Sparse(BM25) + RRF

用于生成“表级度量信号”：
1) 先检索 field（entity_type=field & semantic_type=measure）
2) 再按 table_id 聚合得到 table_score
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import structlog

from server.config import RetrievalConfig, get_retrieval_param
from server.nl2ir.hybrid_utils import build_sparse_query, rrf_merge_hits, HitExtractor

logger = structlog.get_logger()


@dataclass
class MeasureFieldHit:
    field_id: str
    table_id: str
    score: float
    rrf_score: Optional[float] = None
    dense_score: Optional[float] = None
    sparse_score: Optional[float] = None
    display_name: Optional[str] = None
    source: str = "rrf"
    payload: Optional[Dict[str, Any]] = None


class MeasureFieldMilvusRetriever:
    def __init__(
        self,
        *,
        milvus_client,
        embedding_client,
        collection_name: str,
        connection_id: Optional[str],
    ):
        self.milvus_client = milvus_client
        self.embedding_client = embedding_client
        self.collection_name = collection_name
        self.connection_id = connection_id

    def _build_filter(self) -> str:
        filters = ['entity_type == "field"', 'semantic_type == "measure"', "is_active == true"]
        if self.connection_id:
            filters.append(f'connection_id == "{self.connection_id}"')
        return " and ".join(filters)

    async def _search_dense(self, query_vector: List[float], filter_expr: str, limit: int) -> List[Dict[str, Any]]:
        if not query_vector:
            return []
        output_fields = [
            "item_id",
            "table_id",
            "domain_id",
            "display_name",
            "description",
            "json_meta",
            "bm25_text",
        ]
        nprobe = int(get_retrieval_param("table_scoring.measure_field_milvus.milvus_search_params.dense.nprobe", 10) or 10)
        results = await asyncio.to_thread(
            self.milvus_client.search,
            collection_name=self.collection_name,
            data=[query_vector],
            anns_field="dense_vector",
            search_params={"metric_type": "COSINE", "params": {"nprobe": nprobe}},
            filter=filter_expr,
            limit=limit,
            output_fields=output_fields,
        )
        hits = HitExtractor.extract_all(
            results,
            identity_field="item_id",
            metric_type="COSINE",
            score_type="dense",
            fields=output_fields,
        )
        return hits

    async def _search_sparse(self, query_text: str, filter_expr: str, limit: int) -> List[Dict[str, Any]]:
        sparse_query = build_sparse_query(query_text)
        if not sparse_query["text"]:
            return []
        output_fields = [
            "item_id",
            "table_id",
            "domain_id",
            "display_name",
            "description",
            "json_meta",
            "bm25_text",
        ]

        drop_ratio_search = float(get_retrieval_param("table_scoring.measure_field_milvus.milvus_search_params.sparse.drop_ratio_search", 0.2) or 0.2)

        async def _do_search(data):
            return await asyncio.to_thread(
                self.milvus_client.search,
                collection_name=self.collection_name,
                data=[data],
                anns_field="sparse_vector",
                search_params={
                    "metric_type": "BM25",
                    "params": {"drop_ratio_search": drop_ratio_search},
                },
                filter=filter_expr,
                limit=limit,
                output_fields=output_fields,
            )

        results = await _do_search(sparse_query["text"])
        if (not results or len(results[0]) == 0) and sparse_query.get("payload"):
            try:
                results = await _do_search(sparse_query["payload"])
            except Exception:
                pass

        hits = HitExtractor.extract_all(
            results,
            identity_field="item_id",
            metric_type="BM25",
            score_type="sparse",
            fields=output_fields,
        )
        return hits

    async def retrieve(
        self,
        *,
        question: str,
        measure_query: str,
        query_vector: Optional[List[float]],
        top_k_fields: int,
        min_field_score: float = 0.0,
        min_field_score_ratio: float = 0.0,
        use_measure_query_vector: bool = False,
    ) -> List[MeasureFieldHit]:
        """
        返回字段命中列表（按 rrf_score 降序），并附带 dense/sparse 分数。
        """
        if not self.milvus_client or not self.embedding_client:
            return []

        filter_expr = self._build_filter()
        limit = max(1, int(top_k_fields or 1))
        expansion = 3
        raw_limit = limit * expansion

        vector = query_vector
        if use_measure_query_vector:
            try:
                vector = await self.embedding_client.embed_single(measure_query or question)
            except Exception:
                vector = query_vector

        dense_task = (
            asyncio.create_task(self._search_dense(vector or [], filter_expr, raw_limit))
            if vector
            else None
        )
        sparse_task = asyncio.create_task(self._search_sparse(measure_query or question, filter_expr, raw_limit))
        dense_hits = await dense_task if dense_task else []
        sparse_hits = await sparse_task
        merged = rrf_merge_hits({"dense": dense_hits, "sparse": sparse_hits})
        merged = merged[:limit]

        # A2：相对阈值过滤（更稳健）
        ratio = float(min_field_score_ratio or 0.0)
        relative_threshold = 0.0
        top_rrf = 0.0
        if ratio > 0.0 and merged:
            try:
                top_rrf = max(float(e.get("rrf_score") or 0.0) for e in merged)
                relative_threshold = max(0.0, top_rrf * ratio)
            except Exception:
                top_rrf = 0.0
                relative_threshold = 0.0

        hits: List[MeasureFieldHit] = []
        for entry in merged:
            payload = entry.get("payload") or {}
            fid = payload.get("item_id") or ""
            tid = payload.get("table_id") or ""
            if not fid or not tid:
                continue
            rrf_score = float(entry.get("rrf_score") or 0.0)
            threshold = max(float(min_field_score or 0.0), float(relative_threshold or 0.0))
            if rrf_score < threshold:
                continue
            hits.append(
                MeasureFieldHit(
                    field_id=str(fid),
                    table_id=str(tid),
                    score=rrf_score,
                    rrf_score=rrf_score,
                    dense_score=entry.get("dense_score"),
                    sparse_score=entry.get("sparse_score"),
                    display_name=payload.get("display_name"),
                    source="rrf",
                    payload=payload,
                )
            )

        logger.debug(
            "Milvus度量字段检索完成",
            filter=filter_expr,
            measure_query_preview=(measure_query or "")[:80],
            dense=len(dense_hits),
            sparse=len(sparse_hits),
            returned=len(hits),
            min_field_score=float(min_field_score or 0.0),
            min_field_score_ratio=ratio,
            top_rrf=top_rrf,
            relative_threshold=relative_threshold,
        )
        return hits


def aggregate_table_scores(hits: List[MeasureFieldHit]) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    将字段命中聚合到表级分数（默认取 max）。
    返回：
    - table_score_raw: {table_id: raw_score}
    - debug: 记录每表 top hit
    """
    table_score: Dict[str, float] = {}
    top_hit: Dict[str, Any] = {}
    for h in hits:
        prev = table_score.get(h.table_id)
        if prev is None or h.score > prev:
            table_score[h.table_id] = float(h.score)
            top_hit[h.table_id] = {
                "field_id": h.field_id,
                "display_name": h.display_name,
                "score": h.score,
                "rrf_score": h.rrf_score,
                "dense_score": h.dense_score,
                "sparse_score": h.sparse_score,
            }
    return table_score, {"top_hit": top_hit}
