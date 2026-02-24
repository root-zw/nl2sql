"""
TableRankerV3: 鲁棒的表最终排名打分器（可自适应/可调参）

设计目标：
- 兼容现有 V2：默认关闭，不改变现有分数/排序
- 鲁棒：跨不同数据库/不同问法，减少“度量因子误惩罚”等导致的排序抖动
- 可调参：所有权重从 config/retrieval_config.yaml 读取，便于自动搜索最优参数
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from server.config import get_retrieval_param


def _contains_any(text: str, keywords: list[str]) -> bool:
    if not text or not keywords:
        return False
    return any(kw and kw in text for kw in keywords)

def _count_contains(text: str, keywords: list[str]) -> int:
    if not text or not keywords:
        return 0
    seen = set()
    for kw in keywords:
        if not kw:
            continue
        if kw in text:
            seen.add(kw)
    return len(seen)


def detect_measure_intent(question: str) -> bool:
    """
    判断是否具备“度量意图”：
    - 命中 table_scoring.measure_coverage.keywords / universal_keywords / compound_keywords 的 key
    - 或命中常见聚合词（数量/多少/总/合计/平均/最大/最小...）
    """
    if not question:
        return False

    gate_cfg = get_retrieval_param("table_scoring.v3_ranker.measure_gate", {}) or {}
    min_hits = int(gate_cfg.get("min_hits", 1) or 1)
    min_hits = max(1, min_hits)

    keywords = get_retrieval_param("table_scoring.measure_coverage.keywords", []) or []
    universal = get_retrieval_param("table_scoring.measure_coverage.universal_keywords", []) or []
    compound = get_retrieval_param("table_scoring.measure_coverage.compound_keywords", {}) or {}
    compound_keys = list(compound.keys())

    hit_count = 0
    hit_count += _count_contains(question, keywords)
    hit_count += _count_contains(question, universal)
    hit_count += _count_contains(question, compound_keys)

    agg_hints = get_retrieval_param("measure_retrieval.aggregation_hints", {}) or {}
    for _, hint_list in agg_hints.items():
        hit_count += _count_contains(question, hint_list or [])

    # 兜底：一些常见词
    hit_count += _count_contains(question, ["多少", "数量", "个数", "宗数", "总", "合计", "累计", "平均", "最大", "最小"])
    return hit_count >= min_hits


def rank_score_v3(
    *,
    question: str,
    base_score: float,
    top_base_score: float,
    measure_factor: float,
    measure_intent: Optional[bool] = None,
    domain_is_match: bool,
    domain_weight: float,
    gated_enum_boost: float,
    tag_match_boost: float,
    rescue_boost: float = 0.0,
    evidence: Optional[Dict[str, Any]] = None,
) -> float:
    """
    V3 最终分数（按公式实现，保持可解释、可调参）：

    S_final = (S_base * f_measure) + w_domain * B(d_user, d_table) + S_enum + S_tag (+ S_rescue)

    额外鲁棒性：
    - base 稳健化（可选）：S_base *= (base_rel ** beta)
    - 度量意图门控：若问题无明确度量意图，可将 f_measure 视为 1.0（避免误惩罚）
    - 年份 mismatch 可选额外惩罚（乘法）
    """
    eps = 1e-12
    base_rel = base_score / (top_base_score + eps) if top_base_score > 0 else 0.0
    base_rel_beta = float(get_retrieval_param("table_scoring.v3_ranker.base_rel_beta", 0.0) or 0.0)
    # 防止负数的非整数幂导致复数
    if base_rel_beta > 0 and base_rel > 0:
        stable_base = base_score * (base_rel ** base_rel_beta)
    else:
        stable_base = base_score

    # measure gate
    gate_cfg = get_retrieval_param("table_scoring.v3_ranker.measure_gate", {}) or {}
    gate_enabled = bool(gate_cfg.get("enabled", True))
    if gate_enabled:
        resolved_intent = bool(measure_intent) if measure_intent is not None else detect_measure_intent(question)
    else:
        resolved_intent = True
    effective_measure_factor = float(measure_factor) if resolved_intent else 1.0

    domain_bonus = float(domain_weight) if domain_is_match else 0.0
    final_score = (
        stable_base * effective_measure_factor
        + domain_bonus
        + float(gated_enum_boost)
        + float(tag_match_boost)
        + float(rescue_boost or 0.0)
    )

    # 年份 mismatch 额外乘法惩罚（可选）
    year_mismatch_multiplier = float(get_retrieval_param("table_scoring.v3_ranker.year_mismatch_multiplier", 1.0) or 1.0)
    if evidence and evidence.get("year_mismatch") and year_mismatch_multiplier != 1.0:
        final_score *= year_mismatch_multiplier

    return float(final_score)
