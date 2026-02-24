"""Few-Shot 直连执行判定逻辑。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple, Set

from server.config import settings, RetrievalConfig
from server.nl2ir.few_shot_retriever import FewShotSample
from server.nl2ir.table_retriever import TableRetrievalResult


@dataclass
class FewShotDirectDecision:
    sample: FewShotSample
    reasons: List[str]
    match_type: str
    normalized_match: bool
    table_overlap: List[str]

    def to_trace(self) -> Dict[str, Any]:
        return {
            "question": self.sample.question,
            "sample_id": self.sample.sample_id,
            "quality_score": self.sample.quality_score,
            "raw_similarity": self.sample.raw_similarity,
            "dense_rank": self.sample.dense_rank,
            "match_type": self.match_type,
            "reasons": self.reasons,
            "table_overlap": self.table_overlap,
            "source_tag": self.sample.source_tag,
        }


def select_direct_execution_candidate(
    question: str,
    normalized_question: Optional[str],
    samples: List[FewShotSample],
    table_results: List[TableRetrievalResult],
    domain_id: Optional[str],
) -> Tuple[Optional[FewShotDirectDecision], Dict[str, Any]]:
    """根据配置判断是否存在可直接执行的 Few-Shot 样本，同时给出阻断原因。"""
    debug: Dict[str, Any] = {
        "status": "skipped",
        "blocked_by": None,
        "enabled": bool(RetrievalConfig.few_shot_direct_execution_enabled()),
        "tried_samples": [],
    }

    if not RetrievalConfig.few_shot_direct_execution_enabled():
        debug["status"] = "rejected"
        debug["blocked_by"] = "direct_disabled"
        return None, debug

    if not samples:
        debug["status"] = "rejected"
        debug["blocked_by"] = "no_samples"
        return None, debug

    if not table_results:
        debug["status"] = "rejected"
        debug["blocked_by"] = "no_table_results"
        return None, debug

    normalized_input = _normalize_text(normalized_question) or _normalize_text(question)
    table_aliases = _collect_table_aliases(table_results)
    whitelist = _build_source_whitelist()
    semantic_rank_limit = RetrievalConfig.few_shot_dense_priority()
    min_similarity = RetrievalConfig.few_shot_direct_min_similarity()
    min_quality = RetrievalConfig.few_shot_direct_min_quality()

    debug.update(
        {
            "status": "evaluating",
            "thresholds": {
                "semantic_rank_limit": semantic_rank_limit,
                "min_similarity": min_similarity,
                "min_quality": min_quality,
            },
            "whitelist": sorted(whitelist) if whitelist else [],
        }
    )

    ordered_samples = sorted(
        samples,
        key=lambda item: (
            item.final_rank or float("inf"),
            -(item.score or 0.0),
        ),
    )

    last_block: Optional[str] = None
    for sample in ordered_samples:
        decision, block_reason = _evaluate_sample(
            sample=sample,
            normalized_input=normalized_input,
            semantic_rank_limit=semantic_rank_limit,
            min_similarity=min_similarity,
            min_quality=min_quality,
            whitelist=whitelist,
            table_aliases=table_aliases,
            expected_domain=domain_id,
        )
        if decision:
            debug.update(
                {
                    "status": "accepted",
                    "blocked_by": None,
                    "accepted_sample_id": sample.sample_id,
                    "accepted_reasons": list(decision.reasons),
                }
            )
            return decision, debug

        reason = block_reason or "unknown"
        last_block = reason
        debug["tried_samples"].append(
            {
                "sample_id": sample.sample_id,
                "blocked_by": reason,
                "quality": sample.quality_score,
                "raw_similarity": sample.raw_similarity,
                "dense_rank": sample.dense_rank,
                "source_tag": sample.source_tag,
            }
        )

    debug.update(
        {
            "status": "rejected",
            "blocked_by": last_block or "no_candidate_passed",
        }
    )
    return None, debug


def _evaluate_sample(
    sample: FewShotSample,
    normalized_input: Optional[str],
    semantic_rank_limit: int,
    min_similarity: float,
    min_quality: float,
    whitelist: Set[str],
    table_aliases: Set[str],
    expected_domain: Optional[str],
) -> Tuple[Optional[FewShotDirectDecision], Optional[str]]:
    question_equal, high_similarity = _check_semantic_match(
        sample,
        normalized_input,
        semantic_rank_limit,
        min_similarity,
    )
    if not (question_equal or high_similarity):
        return None, "semantic_mismatch"

    quality = float(sample.quality_score or 0.0)
    if quality < min_quality:
        return None, "quality_threshold"

    source_tag = (sample.source_tag or "").strip().lower()
    if whitelist and source_tag not in whitelist:
        return None, "source_whitelist"

    if sample.error_msg:
        return None, "has_error"

    if sample.is_active is False:
        return None, "inactive"

    if (
        expected_domain
        and sample.domain_id
        and str(sample.domain_id) != str(expected_domain)
    ):
        return None, "domain_mismatch"

    table_ok, overlap = _check_table_overlap(sample, table_aliases)
    if not table_ok:
        return None, "table_mismatch"

    reasons: List[str] = []
    match_type = "exact_question" if question_equal else "high_similarity"
    reasons.append(f"semantic:{match_type}")
    reasons.append(f"quality:{quality:.2f}")
    if source_tag:
        reasons.append(f"source:{source_tag}")
    if overlap:
        reasons.append("structure:table_overlap")
    if sample.domain_id and expected_domain and str(sample.domain_id) == str(expected_domain):
        reasons.append("structure:domain_match")

    return (
        FewShotDirectDecision(
            sample=sample,
            reasons=reasons,
            match_type=match_type,
            normalized_match=question_equal,
            table_overlap=overlap,
        ),
        None,
    )


def _check_semantic_match(
    sample: FewShotSample,
    normalized_input: Optional[str],
    semantic_rank_limit: int,
    min_similarity: float,
) -> Tuple[bool, bool]:
    question_equal = False
    if normalized_input:
        question_equal = _normalize_text(sample.question) == normalized_input

    similarity = sample.raw_similarity or 0.0
    dense_rank = sample.dense_rank or 0
    high_similarity = (
        similarity >= min_similarity and dense_rank > 0 and dense_rank <= semantic_rank_limit
    )
    return question_equal, high_similarity


def _check_table_overlap(
    sample: FewShotSample,
    table_aliases: Set[str],
) -> Tuple[bool, List[str]]:
    sample_tables = {
        t.strip().lower()
        for t in (sample.tables or [])
        if isinstance(t, str) and t.strip()
    }
    if not sample_tables:
        # 无结构信息则放宽限制
        return True, []

    overlap = sorted(sample_tables & table_aliases)
    if not overlap:
        return False, []
    return True, overlap


def _collect_table_aliases(table_results: List[TableRetrievalResult]) -> Set[str]:
    aliases: Set[str] = set()
    for result in table_results:
        datasource = result.datasource
        candidates = [
            getattr(datasource, "display_name", None),
            getattr(datasource, "datasource_name", None),
            getattr(datasource, "table_name", None),
        ]
        for candidate in candidates:
            if not candidate:
                continue
            aliases.add(candidate.strip().lower())
    return aliases


def _normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _build_source_whitelist() -> Set[str]:
    configured = RetrievalConfig.few_shot_direct_source_whitelist() or []
    return {item.strip().lower() for item in configured if item and item.strip()}

