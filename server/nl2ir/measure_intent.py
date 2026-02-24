"""
度量意图与度量短语抽取（统一归一化 + 噪声守卫 + 最长子串回退）

设计原则：
- 配置驱动：不在代码硬编码业务词
- 鲁棒：分词不稳定时可回退到子串最长匹配，但要有噪声守卫
- 可回溯：提供足够 evidence 便于 trace/诊断
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import re
import unicodedata


def normalize_text(text: str, cfg: Dict[str, Any]) -> str:
    if not text:
        return ""
    out = text

    if cfg.get("nfkc", True):
        out = unicodedata.normalize("NFKC", out)
    if cfg.get("lowercase", True):
        out = out.lower()
    if cfg.get("collapse_whitespace", True):
        out = re.sub(r"\s+", " ", out).strip()
    if cfg.get("remove_punct", True):
        # 保留中文、字母数字与空格，其余视为标点/符号移除
        out = re.sub(r"[^\u4e00-\u9fff0-9a-zA-Z ]+", "", out)
        out = re.sub(r"\s+", " ", out).strip()
    return out


def _is_word_char(ch: str) -> bool:
    if not ch:
        return False
    code = ord(ch)
    if 0x4E00 <= code <= 0x9FFF:  # CJK
        return True
    return ch.isalnum()


@dataclass
class PhraseMatch:
    phrase: str
    start: int
    end: int
    stage: str  # token | substring
    dropped_reason: Optional[str] = None


@dataclass
class MeasureIntent:
    is_measure_intent: bool
    is_universal_only: bool
    required_concepts: List[str]
    matched_phrases: List[str]
    matched_field_ids: List[str]
    evidence: Dict[str, Any] = field(default_factory=dict)


def build_phrase_index(
    *,
    all_measures: Optional[Iterable[Dict[str, Any]]],
    keywords: List[str],
    universal_keywords: List[str],
    compound_keywords: Dict[str, str],
    measure_families: Dict[str, str],
    normalization_cfg: Dict[str, Any],
) -> Tuple[Set[str], Dict[str, Set[str]]]:
    """
    构建短语集合与短语->field_id 索引（均使用归一化后的短语）。
    返回：
    - phrases: set(normalized_phrase)
    - phrase_to_field_ids: {normalized_phrase: {field_id,...}}
    """
    phrases: Set[str] = set()
    phrase_to_field_ids: Dict[str, Set[str]] = {}

    def _add_phrase(p: str, field_id: Optional[str] = None) -> None:
        pn = normalize_text(p, normalization_cfg)
        if not pn:
            return
        phrases.add(pn)
        if field_id:
            phrase_to_field_ids.setdefault(pn, set()).add(str(field_id))

    for kw in keywords or []:
        _add_phrase(kw)
    for uk in universal_keywords or []:
        _add_phrase(uk)
    for ck in (compound_keywords or {}).keys():
        _add_phrase(ck)
    for fam_kw in (measure_families or {}).keys():
        _add_phrase(fam_kw)

    if all_measures:
        for m in all_measures:
            if not isinstance(m, dict):
                continue
            fid = m.get("field_id")
            _add_phrase(m.get("display_name") or "", fid)
            for syn in (m.get("synonyms") or []):
                _add_phrase(syn, fid)

    return phrases, phrase_to_field_ids


def map_phrase_to_concept(
    phrase_norm: str,
    *,
    compound_keywords: Dict[str, str],
    measure_families: Dict[str, str],
    normalization_cfg: Dict[str, Any],
) -> str:
    """
    将命中的“短语”映射到统一的“概念 key”。
    优先级：
    1) measure_families
    2) compound_keywords
    3) 原短语
    """
    if not phrase_norm:
        return ""
    fam = normalize_text(measure_families.get(phrase_norm, ""), normalization_cfg) if measure_families else ""
    if fam:
        return fam
    comp = normalize_text(compound_keywords.get(phrase_norm, ""), normalization_cfg) if compound_keywords else ""
    if comp:
        return comp
    return phrase_norm


def _select_longest_non_overlapping(matches: List[PhraseMatch]) -> List[PhraseMatch]:
    # 长度降序 + 起点升序，贪心选不重叠命中
    ordered = sorted(matches, key=lambda m: (-(m.end - m.start), m.start, m.end))
    selected: List[PhraseMatch] = []
    occupied: List[Tuple[int, int]] = []

    def _overlaps(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
        return not (a[1] <= b[0] or b[1] <= a[0])

    for m in ordered:
        span = (m.start, m.end)
        if any(_overlaps(span, s) for s in occupied):
            continue
        selected.append(m)
        occupied.append(span)
    # 还原自然顺序
    return sorted(selected, key=lambda m: (m.start, m.end))


def extract_measure_intent(
    *,
    question: str,
    tokens: List[str],
    all_measures: Optional[Iterable[Dict[str, Any]]],
    keywords: List[str],
    universal_keywords: List[str],
    compound_keywords: Dict[str, str],
    measure_families: Dict[str, str],
    generic_terms: List[str],
    suffix_keywords: List[str],
    normalization_cfg: Dict[str, Any],
    min_phrase_len: int,
    aggregation_hints: Optional[Dict[str, List[str]]] = None,
    unit_hints: Optional[Dict[str, List[str]]] = None,
    unit_require_number: bool = True,
) -> MeasureIntent:
    """
    抽取度量意图与“可用于表区分”的 required_concepts（不含通用统计词）。
    """
    qn = normalize_text(question, normalization_cfg)
    universal_set = {normalize_text(x, normalization_cfg) for x in (universal_keywords or []) if x}
    generic_set = {normalize_text(x, normalization_cfg) for x in (generic_terms or []) if x}
    suffix_set = {normalize_text(x, normalization_cfg) for x in (suffix_keywords or []) if x}

    phrases, phrase_to_field_ids = build_phrase_index(
        all_measures=all_measures,
        keywords=keywords,
        universal_keywords=universal_keywords,
        compound_keywords=compound_keywords,
        measure_families=measure_families,
        normalization_cfg=normalization_cfg,
    )

    token_norm = [normalize_text(t, normalization_cfg) for t in (tokens or [])]

    stage1_hits: List[PhraseMatch] = []
    for t in token_norm:
        if t and t in phrases:
            stage1_hits.append(PhraseMatch(phrase=t, start=-1, end=-1, stage="token"))

    # 判断是否仅命中通用统计/短泛词：触发 Stage2
    stage1_non_universal = [h for h in stage1_hits if h.phrase not in universal_set]
    stage1_specific = [h for h in stage1_non_universal if h.phrase not in generic_set]
    trigger_stage2 = (not stage1_hits) or (not stage1_specific)

    stage2_matches: List[PhraseMatch] = []
    dropped_by_guard: List[Dict[str, Any]] = []

    if trigger_stage2 and qn:
        # 候选短语：长度 + 非 generic_terms（避免“面积/金额”子串误匹配）
        candidates = [
            p for p in phrases
            if len(p) >= max(1, int(min_phrase_len or 1))
            and p not in generic_set
            and p not in universal_set
        ]

        for p in candidates:
            # 简单子串查找（候选集通常较小；未来可用AC自动机优化）
            start = 0
            while True:
                idx = qn.find(p, start)
                if idx < 0:
                    break
                end = idx + len(p)

                # 噪声守卫：边界检查（或包含度量后缀可放宽）
                left = qn[idx - 1] if idx - 1 >= 0 else ""
                right = qn[end] if end < len(qn) else ""
                has_boundary = (not _is_word_char(left)) and (not _is_word_char(right))
                has_suffix = any(sfx and sfx in p for sfx in suffix_set)
                if not has_boundary and not has_suffix:
                    dropped_by_guard.append(
                        {"phrase": p, "start": idx, "end": end, "reason": "boundary_guard"}
                    )
                    start = idx + 1
                    continue

                stage2_matches.append(PhraseMatch(phrase=p, start=idx, end=end, stage="substring"))
                start = end

        stage2_matches = _select_longest_non_overlapping(stage2_matches)

    # 合并命中短语（去重、保持稳定顺序）
    merged_phrases: List[str] = []
    for hit in stage1_hits:
        if hit.phrase not in merged_phrases:
            merged_phrases.append(hit.phrase)
    for hit in stage2_matches:
        if hit.phrase not in merged_phrases:
            merged_phrases.append(hit.phrase)

    # 概念映射 + field_id 映射
    required_concepts: List[str] = []
    matched_field_ids: Set[str] = set()
    universal_hits: List[str] = []

    # 单位提示（概念级）：优先使用“数字+单位”命中，降低噪声
    unit_concepts: List[str] = []
    unit_hits_detail: List[Dict[str, Any]] = []
    if unit_hints:
        raw_q = question or ""
        raw_q_lower = raw_q.lower()
        for concept_key, units in (unit_hints or {}).items():
            concept_norm = normalize_text(concept_key, normalization_cfg)
            if not concept_norm:
                continue
            for u in units or []:
                if not u:
                    continue
                u_norm = normalize_text(str(u), normalization_cfg)
                # raw 匹配用于支持 ㎡ / m² 等符号（归一化可能移除）
                raw_hit = False
                norm_hit = False
                if unit_require_number:
                    try:
                        # 数字+单位：支持小数、可选空格
                        pattern_raw = re.compile(
                            rf"\d+(?:\.\d+)?\s*{re.escape(str(u))}",
                            re.IGNORECASE,
                        )
                        if pattern_raw.search(raw_q):
                            raw_hit = True
                    except Exception:
                        raw_hit = False
                    if u_norm:
                        try:
                            pattern_norm = re.compile(
                                rf"\d+(?:\.\d+)?\s*{re.escape(u_norm)}"
                            )
                            if pattern_norm.search(qn):
                                norm_hit = True
                        except Exception:
                            norm_hit = False
                else:
                    raw_hit = str(u).lower() in raw_q_lower
                    norm_hit = bool(u_norm and u_norm in qn)

                if raw_hit or norm_hit:
                    if concept_norm not in unit_concepts:
                        unit_concepts.append(concept_norm)
                    unit_hits_detail.append(
                        {"concept": concept_norm, "unit": str(u), "unit_norm": u_norm, "raw_hit": raw_hit, "norm_hit": norm_hit}
                    )
                    break

    # 预归一化 mapping key（避免 mapping 表没做 normalize 导致命中失败）
    comp_map_norm = {
        normalize_text(k, normalization_cfg): v for k, v in (compound_keywords or {}).items() if k
    }
    fam_map_norm = {
        normalize_text(k, normalization_cfg): v for k, v in (measure_families or {}).items() if k
    }

    for p in merged_phrases:
        if p in universal_set:
            universal_hits.append(p)
            continue

        concept = map_phrase_to_concept(
            p,
            compound_keywords=comp_map_norm,
            measure_families=fam_map_norm,
            normalization_cfg=normalization_cfg,
        )
        if concept and concept not in required_concepts:
            required_concepts.append(concept)

        for fid in phrase_to_field_ids.get(p, set()):
            matched_field_ids.add(str(fid))

    # 单位提示加入 required_concepts（仅当不是通用统计意图）
    for c in unit_concepts:
        if c and c not in required_concepts:
            required_concepts.append(c)

    # 兜底：聚合 hint（不引入通用统计词到 required_concepts）
    agg_hit = False
    agg_keywords_hit: List[str] = []
    if aggregation_hints:
        for hint_list in aggregation_hints.values():
            for kw in hint_list or []:
                kwn = normalize_text(kw, normalization_cfg)
                if kwn and kwn in qn:
                    agg_hit = True
                    agg_keywords_hit.append(kwn)

    is_universal_only = bool(universal_hits) and not required_concepts
    is_measure_intent = bool(required_concepts) or bool(universal_hits) or agg_hit

    return MeasureIntent(
        is_measure_intent=is_measure_intent,
        is_universal_only=is_universal_only,
        required_concepts=required_concepts,
        matched_phrases=merged_phrases,
        matched_field_ids=sorted(matched_field_ids),
        evidence={
            "normalized_question": qn,
            "trigger_stage2": trigger_stage2,
            "stage1_hits": [h.phrase for h in stage1_hits],
            "stage2_hits": [{"phrase": h.phrase, "start": h.start, "end": h.end} for h in stage2_matches],
            "universal_hits": universal_hits,
            "unit_concepts": unit_concepts,
            "unit_hits": unit_hits_detail[:50],
            "dropped_by_guard": dropped_by_guard[:50],
            "agg_keywords_hit": agg_keywords_hit[:20],
        },
    )
