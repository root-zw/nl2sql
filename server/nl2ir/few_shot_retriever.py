"""
Few-Shot 问答对检索器
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple, Pattern

import json
import math
import re
import asyncio
import structlog

from server.config import settings, RetrievalConfig, get_retrieval_param
from server.nl2ir.hybrid_utils import normalize_dense_score, rrf_merge_hits, HitExtractor
from server.utils.model_clients import RerankerClient
from server.models.semantic import SemanticModel

logger = structlog.get_logger()
_WORD_CHAR_CLASS = r"0-9A-Za-z_\u4e00-\u9fff"


@dataclass
class FewShotSample:
    """Milvus中召回的Few-Shot样本"""

    question: str
    sql: str
    ir_json: Optional[str] = None
    tables: List[str] = None
    domain_id: Optional[str] = None
    score: float = 0.0
    source_tag: Optional[str] = None
    reranker_score: Optional[float] = None
    dense_score: Optional[float] = None
    sparse_score: Optional[float] = None
    sample_type: Optional[str] = None
    quality_score: Optional[float] = None
    error_msg: Optional[str] = None
    dense_rank: Optional[int] = None
    raw_similarity: Optional[float] = None
    final_rank: Optional[int] = None
    sample_id: Optional[str] = None
    is_active: Optional[bool] = None


@dataclass(frozen=True)
class PhraseRule:
    pattern: Pattern[str]
    replacement: str
    duplicate_token: Optional[str] = None


class FewShotRetriever:
    """Few-Shot问答对检索器"""

    def __init__(
        self,
        milvus_client,
        embedding_client,
        collection_name: Optional[str] = None,
        top_k: Optional[int] = None,
        similarity_threshold: Optional[float] = None,
        min_quality_score: Optional[float] = None,
        reranker: Optional[RerankerClient] = None,
        semantic_model: Optional[SemanticModel] = None,
        prompt_max_examples: Optional[int] = None,
    ):
        self.milvus = milvus_client
        self.embedding = embedding_client
        self.collection_name = collection_name or settings.milvus_few_shot_collection
        self.top_k = top_k or RetrievalConfig.few_shot_top_k()
        self.metric_type = "COSINE"
        self.similarity_threshold = (
            similarity_threshold
            if similarity_threshold is not None
            else RetrievalConfig.few_shot_similarity_threshold()
        )
        self.min_quality_score = (
            min_quality_score
            if min_quality_score is not None
            else RetrievalConfig.few_shot_min_quality_score()
        )

        self.last_retrieval_info: Dict[str, Any] = {}
        self.reranker = reranker
        self.semantic_model = semantic_model
        self.prompt_max_examples = (
            prompt_max_examples or RetrievalConfig.few_shot_prompt_max_examples()
        )
        self.prompt_max_examples = min(self.prompt_max_examples, self.top_k)
        self.dense_priority_limit = max(
            1, min(RetrievalConfig.few_shot_dense_priority(), self.prompt_max_examples)
        )
        self.normalization_enabled = RetrievalConfig.few_shot_normalize_question()
        self._synonym_map = self._build_synonym_map()
        self._synonym_patterns = self._build_synonym_patterns(self._synonym_map)
        self._phrase_rules = self._build_phrase_rules()
        self.last_prompt_samples: List[FewShotSample] = []
        self.last_direct_candidates: List[FewShotSample] = []

    async def retrieve(
        self,
        question: str,
        connection_id: str,
        query_vector: Optional[List[float]] = None,
        source_domain_id: Optional[str] = None,
        resolved_domain_id: Optional[str] = None,
    ) -> List[FewShotSample]:
        if not self.milvus or not self.embedding:
            logger.debug("Milvus或Embedding客户端缺失，跳过Few-Shot检索")
            self.last_retrieval_info = {"available": False}
            return []

        if not connection_id:
            logger.debug("缺少connection_id，跳过Few-Shot检索")
            return []

        try:
            (
                normalized_question,
                question_changed,
                normalization_warning,
            ) = self._normalize_question(question)
            if question_changed:
                logger.debug(
                    "Few-Shot问题归一化",
                    original=question,
                    normalized=normalized_question,
                )

            if query_vector is None or question_changed:
                query_vector = await self.embedding.embed_single(normalized_question)

            filter_expr = self._build_filter(connection_id)
            dense_hits = await self._search_dense(query_vector, filter_expr, self.top_k * 5)
            sparse_hits: List[Dict] = []
            dense_top10 = self._summarize_dense_hits(dense_hits)
            merged = rrf_merge_hits({"dense": dense_hits})

            if not merged:
                logger.debug(
                    "Few-Shot检索为空",
                    filter=filter_expr,
                    dense_candidates=len(dense_hits),
                    sparse_candidates=len(sparse_hits),
                )
                self.last_prompt_samples = []
                self.last_direct_candidates = []
                self.last_retrieval_info = {
                    "filter": filter_expr,
                    "found": 0,
                    "dense_candidates": len(dense_hits),
                    "sparse_candidates": 0,
                    "source_domain_id": source_domain_id,
                    "resolved_domain_id": resolved_domain_id,
                    "normalized_question": normalized_question if question_changed else question,
                    "prompt_max_examples": self.prompt_max_examples,
                    "dense_top10": dense_top10,
                    "normalization_warning": normalization_warning,
                    "direct_candidates": [],
                    "prompt_samples": [],
                }
                return []

            limited = merged[: self.top_k * 3]
            reranker_scores = await self._apply_reranker(
                normalized_question, [entry["payload"] for entry in limited]
            )
            dense_rank_map = self._build_dense_rank_map(dense_hits)

            samples: List[FewShotSample] = []
            for idx, entry in enumerate(limited):
                payload = entry.get("payload") or {}
                json_meta = self._safe_json(payload.get("json_meta"))
                tables = json_meta.get("tables") or []
                rerank_score = reranker_scores[idx] if idx < len(reranker_scores) else None
                final_score = self._blend_reranker_score(entry.get("rrf_score"), rerank_score)
                raw_similarity = entry.get("dense_raw_similarity") or entry.get("raw_similarity")
                samples.append(
                    FewShotSample(
                        sample_id=payload.get("sample_id"),
                        question=payload.get("question", ""),
                        sql=payload.get("sql_context") or payload.get("sql", ""),
                        ir_json=payload.get("ir_json"),
                        tables=self._parse_tables_field(tables),
                        domain_id=payload.get("domain_id"),
                        score=final_score,
                        source_tag=json_meta.get("source_tag"),
                        reranker_score=rerank_score,
                        dense_score=entry.get("dense_score"),
                        sparse_score=entry.get("sparse_score"),
                        sample_type=payload.get("sample_type"),
                        quality_score=payload.get("quality_score"),
                        error_msg=payload.get("error_msg"),
                        dense_rank=dense_rank_map.get(payload.get("question")),
                        raw_similarity=raw_similarity,
                        is_active=payload.get("is_active"),
                    )
                )

            samples.sort(key=lambda s: s.score, reverse=True)
            for idx, sample in enumerate(samples, start=1):
                sample.final_rank = idx

            retrieved_samples = samples[: self.top_k]
            prompt_samples = self._select_prompt_samples(
                retrieved_samples,
                normalized_question if question_changed else question,
                self.prompt_max_examples,
                self.dense_priority_limit,
            )
            self.last_direct_candidates = list(retrieved_samples)
            self.last_prompt_samples = list(prompt_samples)

            self.last_retrieval_info = {
                "filter": filter_expr,
                "found": len(prompt_samples),
                "retrieved_total": len(retrieved_samples),
                "dense_candidates": len(dense_hits),
                "sparse_candidates": 0,
                "reranker_used": bool(self.reranker and self.reranker.is_enabled()),
                "source_domain_id": source_domain_id,
                "resolved_domain_id": resolved_domain_id,
                "normalized_question": normalized_question if question_changed else question,
                "prompt_max_examples": self.prompt_max_examples,
                "dense_top10": dense_top10,
                "normalization_warning": normalization_warning,
                "dense_priority_limit": self.dense_priority_limit,
                "direct_candidates": self._summarize_samples(
                    retrieved_samples, limit=self.top_k
                ),
                "prompt_samples": self._summarize_samples(
                    prompt_samples, limit=self.prompt_max_examples
                ),
            }
            logger.debug(
                "Few-Shot检索完成",
                filter=filter_expr,
                found=len(prompt_samples),
                reranker_used=self.last_retrieval_info["reranker_used"],
                source_domain_id=source_domain_id,
                resolved_domain_id=resolved_domain_id,
            )
            return prompt_samples

        except Exception as e:
            logger.exception("Few-Shot检索失败", error=str(e))
            self.last_retrieval_info = {"error": str(e)}
            return []

    def _build_filter(self, connection_id: str) -> str:
        return (
            f'connection_id == "{connection_id}" and is_active == true and quality_score >= {self.min_quality_score}'
        )

    async def _search_dense(self, query_vector, filter_expr: str, limit: int) -> List[Dict]:
        """Dense 向量检索（使用 HitExtractor 统一处理）。"""
        output_fields = [
            "sample_id",
            "question",
            "sql_context",
            "ir_json",
            "json_meta",
            "domain_id",
            "sample_type",
            "quality_score",
            "error_msg",
            "is_active",
        ]
        metric_type = self.metric_type
        nprobe = int(get_retrieval_param("few_shot_retrieval.milvus_search_params.dense.nprobe", 10) or 10)
        results = await asyncio.to_thread(
            self.milvus.search,
            collection_name=self.collection_name,
            data=[query_vector],
            anns_field="dense_vector",
            search_params={"metric_type": metric_type, "params": {"nprobe": nprobe}},
            filter=filter_expr,
            limit=limit,
            output_fields=output_fields,
        )
        hits = HitExtractor.extract_all(
            results,
            identity_field="question",  # few-shot 用 question 作为 identity
            metric_type=metric_type,
            score_type="dense",
            fields=output_fields,
        )
        # 后处理：json_meta 安全解析
        for hit in hits:
            payload = hit.get("payload", {})
            payload["json_meta"] = self._safe_json(payload.get("json_meta"))
        return hits

    async def _apply_reranker(
        self,
        question: str,
        payloads: List[Dict[str, Any]],
    ) -> List[Optional[float]]:
        if not payloads:
            return []
        if not self.reranker or not self.reranker.is_enabled():
            return [None for _ in payloads]

        docs = self._build_reranker_docs(payloads)
        if not docs:
            return [None for _ in payloads]
        return await self.reranker.rerank(question, docs)

    @staticmethod
    def _parse_tables_field(value: Any) -> List[str]:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return []
        if not value:
            return []
        parsed: List[str] = []
        for item in value:
            if isinstance(item, dict):
                label = (
                    item.get("display_name")
                    or item.get("table_name")
                    or item.get("physical_table_name")
                    or item.get("table_id")
                )
                if label:
                    parsed.append(str(label))
            elif item:
                parsed.append(str(item))
        return parsed

    @staticmethod
    def _safe_json(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return {}
        return {}

    @staticmethod
    def _blend_reranker_score(
        rrf_score: Optional[float],
        rerank_score: Optional[float],
    ) -> Optional[float]:
        if rerank_score is None:
            return rrf_score
        base = rrf_score or 0.0
        weight = RetrievalConfig.reranker_weight()
        normalized_rerank = FewShotRetriever._normalize_rerank_score(rerank_score)
        return (1 - weight) * base + weight * normalized_rerank

    @staticmethod
    def _normalize_rerank_score(value: float) -> float:
        try:
            return 1.0 / (1.0 + math.exp(-value))
        except OverflowError:
            return 0.0 if value < 0 else 1.0

    def _normalize_question(self, question: str) -> Tuple[str, bool, Optional[str]]:
        if not self.normalization_enabled:
            return question, False, None

        original = question or ""
        normalized = original.strip()
        changed = normalized != original
        warning: Optional[str] = None

        for rule in self._phrase_rules:
            before = normalized
            normalized = rule.pattern.sub(rule.replacement, normalized)
            if normalized != before:
                changed = True
                if rule.duplicate_token and self._has_duplicate_phrase(
                    normalized, rule.duplicate_token
                ):
                    warning = f"duplicate_phrase:{rule.duplicate_token}"
                    normalized = original
                    changed = False
                    break

        if not warning:
            normalized, synonym_changed = self._apply_synonym_rules(normalized)
            changed = changed or synonym_changed

        normalized = re.sub(r"\s+", " ", normalized)
        changed = changed or normalized != original
        return normalized, changed, warning

    def _build_synonym_map(self) -> Dict[str, str]:
        if not self.normalization_enabled or not self.semantic_model:
            return {}

        mapping: Dict[str, str] = {}
        for field in (self.semantic_model.fields or {}).values():
            canonical = (field.display_name or field.field_name or "").strip()
            if not canonical:
                continue
            synonyms = field.synonyms or []
            for synonym in synonyms:
                text = (synonym or "").strip()
                if not text or text == canonical:
                    continue
                if len(text) == 1:
                    continue
                mapping[text] = canonical

        # 按长度降序，避免较短词提前替换
        return dict(sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True))

    def _apply_synonym_rules(self, text: str) -> Tuple[str, bool]:
        if not self._synonym_patterns:
            return text, False

        updated = text
        changed = False
        for synonym, canonical in self._synonym_map.items():
            pattern = self._synonym_patterns.get(synonym)
            if not pattern:
                continue
            updated_value, count = pattern.subn(canonical, updated)
            if count > 0:
                updated = updated_value
                changed = True
        return updated, changed

    @staticmethod
    def _build_phrase_rules() -> List[PhraseRule]:
        base_rules = [
            (r"所属街道为", "所属街道"),
            (r"街道办事处", "所属街道"),
            (r"所属乡镇为", "所属乡镇"),
            (r"所属社区为", "所属社区"),
        ]
        return [
            PhraseRule(pattern=re.compile(pattern), replacement=replacement, duplicate_token=replacement)
            for pattern, replacement in base_rules
        ]

    @staticmethod
    def _build_dense_rank_map(hits: List[Dict]) -> Dict[str, int]:
        rank_map: Dict[str, int] = {}
        for idx, hit in enumerate(hits):
            identity = hit.get("identity")
            if identity and identity not in rank_map:
                rank_map[identity] = idx + 1
        return rank_map

    @staticmethod
    def _has_duplicate_phrase(text: str, phrase: str) -> bool:
        if not phrase:
            return False
        pattern = re.compile(rf"{re.escape(phrase)}\s*{re.escape(phrase)}")
        return bool(pattern.search(text))

    @staticmethod
    def _build_synonym_patterns(mapping: Dict[str, str]) -> Dict[str, Pattern[str]]:
        if not mapping:
            return {}
        patterns: Dict[str, Pattern[str]] = {}
        for synonym in mapping:
            boundary = rf"(?<![{_WORD_CHAR_CLASS}]){re.escape(synonym)}(?![{_WORD_CHAR_CLASS}])"
            patterns[synonym] = re.compile(boundary)
        return patterns

    @staticmethod
    def _summarize_samples(
        samples: List[FewShotSample],
        limit: int = 6,
    ) -> List[Dict[str, Any]]:
        summary: List[Dict[str, Any]] = []
        for sample in samples[:limit]:
            summary.append(
                {
                    "question": sample.question,
                    "dense_rank": sample.dense_rank,
                    "final_rank": sample.final_rank,
                    "score": sample.score,
                    "raw_similarity": sample.raw_similarity,
                    "tables": sample.tables,
                }
            )
        return summary

    @staticmethod
    def _summarize_dense_hits(hits: List[Dict], limit: int = 10) -> List[Dict[str, Any]]:
        summary: List[Dict[str, Any]] = []
        for hit in hits[:limit]:
            payload = hit.get("payload") or {}
            summary.append(
                {
                    "question": payload.get("question"),
                    "raw_similarity": hit.get("raw_similarity"),
                    "dense_score": hit.get("score"),
                }
            )
        return summary

    @staticmethod
    def _select_prompt_samples(
        samples: List[FewShotSample],
        normalized_question: str,
        limit: int,
        dense_priority_limit: int,
    ) -> List[FewShotSample]:
        if len(samples) <= limit:
            return samples

        grouped: Dict[str, List[FewShotSample]] = {}
        for sample in samples:
            key = (sample.question or "").strip()
            grouped.setdefault(key, []).append(sample)

        prioritized: List[FewShotSample] = []
        normalized_key = (normalized_question or "").strip()
        if normalized_key and normalized_key in grouped:
            prioritized.extend(grouped.pop(normalized_key))

        dense_limit = max(1, min(dense_priority_limit, limit))

        def append_unique(target: List[FewShotSample], sample: FewShotSample):
            question_key = (sample.question or "").strip()
            if not question_key:
                return
            if any(entry.question == question_key for entry in target):
                return
            target.append(sample)

        # Step 1: guarantee dense priority samples
        dense_priority = sorted(
            [
                sample
                for sample in samples
                if sample.dense_rank and sample.dense_rank <= dense_limit
            ],
            key=lambda item: (item.dense_rank or 0, -(item.score or 0.0)),
        )

        ordered: List[FewShotSample] = []
        for sample in prioritized:
            append_unique(ordered, sample)

        for sample in dense_priority:
            append_unique(ordered, sample)

        # Step 2: fill the rest by final score while preserving original ranking
        remaining_groups = sorted(
            grouped.values(),
            key=lambda group: group[0].score or 0.0,
            reverse=True,
        )
        for group in remaining_groups:
            for sample in group:
                append_unique(ordered, sample)
                if len(ordered) >= limit:
                    return ordered[:limit]

        # Step 3: fallback to any leftover samples
        for sample in samples:
            append_unique(ordered, sample)
            if len(ordered) >= limit:
                break

        return ordered[:limit]

    @staticmethod
    def _build_reranker_docs(payloads: List[Dict[str, Any]]) -> List[str]:
        docs: List[str] = []
        for payload in payloads:
            if not payload:
                docs.append("")
                continue
            question = payload.get("question") or ""
            ir_content = FewShotRetriever._extract_ir_text(payload)
            source = (payload.get("json_meta") or {}).get("source_tag")
            doc = "\n".join(filter(None, [question, ir_content, source]))
            docs.append(doc)
        return docs

    @staticmethod
    def _extract_ir_text(payload: Dict[str, Any]) -> str:
        ir_value = payload.get("ir_json")
        if isinstance(ir_value, dict):
            try:
                return json.dumps(ir_value, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                pass
        if isinstance(ir_value, str) and ir_value.strip():
            return ir_value.strip()
        sql_text = payload.get("sql_context") or payload.get("sql") or ""
        return FewShotRetriever._trim_text(sql_text)

    @staticmethod
    def _trim_text(text: str, limit: int = 600) -> str:
        if not text:
            return ""
        normalized = text.strip()
        if len(normalized) <= limit:
            return normalized
        return normalized[: limit - 3] + "..."
