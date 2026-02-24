"""
TableRankerV4: 打分表 V4.0 最终排名打分器（可解释/可调参）

目标：
- 解决“Boost 多项叠加越权”、“Top1/Top2 轻微波动翻转”、“低语义时排序近似随机”等线上问题
- 所有权重/阈值均从 config/retrieval_config.yaml 读取，禁止硬编码

定稿公式（来自 docs/table_score_optimize.md）：
1) S_penalty = S_base * M_year * M_domain * M_structure  （工程上：S_base 可先由 V3 的 stable_base 得到）
2) B_clamped = min(B_raw, lambda * S_penalty)
3) S_final = S_penalty + Gate(S_penalty) * B_clamped + B_rescue
4) Safety Clamp: S_final = max(S_final, S_penalty)

工程增强：
- Anti-flip: 当 |S_final(A)-S_final(B)| < epsilon 时，用 S_penalty 做 tie-break（在排序阶段实现）
- Low-confidence: 当 max(S_penalty) < tau_low_fallback 时，进入保守降级模式（在上游批量排序阶段实现）
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from server.config import get_retrieval_param


def _clamp(value: float, lo: float, hi: float) -> float:
    try:
        v = float(value)
    except Exception:
        v = lo
    return max(lo, min(hi, v))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


@dataclass
class V4ScoreComponents:
    # 基础/惩罚
    base_score: float
    stable_base: float
    s_penalty: float

    # Boost
    b_raw: float
    b_clamped: float
    gate: float
    rescue_boost: float

    # 输出
    s_final: float

    # 辅助信息
    year_multiplier: float = 1.0
    structure_multiplier: float = 1.0
    lambda_cap: float = 0.0


class TableRankerV4:
    @staticmethod
    def enabled() -> bool:
        return bool(get_retrieval_param("table_scoring.v4_ranker.enabled", False))

    @staticmethod
    def gate_value(s_penalty: float) -> float:
        cfg = get_retrieval_param("table_scoring.v4_ranker.gate", {}) or {}
        tau_low = _safe_float(cfg.get("tau_low", 0.2), 0.2)
        tau_high = _safe_float(cfg.get("tau_high", 0.5), 0.5)
        mid_value = _safe_float(cfg.get("mid_value", 0.3), 0.3)

        if s_penalty < tau_low:
            return 0.0
        if s_penalty < tau_high:
            return max(0.0, min(1.0, mid_value))
        return 1.0

    @staticmethod
    def compute(
        *,
        base_score: float,
        top_base_score: float,
        measure_factor: float,
        # 这些都视为 B_raw 的组成部分（最终会被全局 cap）
        domain_bonus: float,
        enum_boost: float,
        tag_boost: float,
        measure_pg_boost: float = 0.0,
        measure_milvus_boost: float = 0.0,
        # rescue 独立于 cap 与 gate（但仍受 safety clamp 的“不能低于 S_penalty”约束）
        rescue_boost: float = 0.0,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> V4ScoreComponents:
        """
        计算 V4 单表最终分数与中间项（不负责 anti-flip/low-confidence 的跨表逻辑）。
        """
        eps = 1e-12

        base = float(base_score or 0.0)
        top_base = float(top_base_score or 0.0)
        base_rel = base / (top_base + eps) if top_base > 0 else 0.0
        beta = float(get_retrieval_param("table_scoring.v4_ranker.base_rel_beta", 0.0) or 0.0)

        # 稳健化：避免负数的非整数幂；base_rel 在 [0, +inf) 内
        if beta > 0 and base_rel > 0:
            stable_base = base * (base_rel ** beta)
        else:
            stable_base = base

        # S_penalty：以“稳定基础分”为起点叠加乘法因子
        s_penalty = stable_base * float(measure_factor or 1.0)

        # 年份强冲突乘法惩罚（可选）
        year_multiplier = float(get_retrieval_param("table_scoring.v4_ranker.year_mismatch_multiplier", 1.0) or 1.0)
        if evidence and evidence.get("year_mismatch") and year_multiplier != 1.0:
            s_penalty *= year_multiplier

        # 结构惩罚（可选）
        structure_multiplier = 1.0
        struct_cfg = get_retrieval_param("table_scoring.v4_ranker.structure_penalty", {}) or {}
        if bool(struct_cfg.get("enabled", False)):
            min_field_count = int(struct_cfg.get("min_field_count", 1) or 1)
            min_field_count = max(1, min_field_count)
            multiplier = float(struct_cfg.get("multiplier", 0.7) or 0.7)
            try:
                field_count = int((evidence or {}).get("field_count") or 0)
            except Exception:
                field_count = 0
            if field_count and field_count <= min_field_count:
                structure_multiplier = multiplier
                s_penalty *= structure_multiplier

        s_penalty = float(max(0.0, s_penalty))

        # B_raw：把所有加分信号汇总；统一做全局 cap
        b_raw = (
            float(domain_bonus or 0.0)
            + float(enum_boost or 0.0)
            + float(tag_boost or 0.0)
            + float(measure_pg_boost or 0.0)
            + float(measure_milvus_boost or 0.0)
        )

        # 全局 Boost cap：lambda ∈ [0.3, 0.6]（运行时 clamp 防止误配）
        cap_cfg = get_retrieval_param("table_scoring.v4_ranker.global_boost_cap", {}) or {}
        lambda_cap = _clamp(_safe_float(cap_cfg.get("lambda", 0.4), 0.4), 0.3, 0.6)
        b_clamped = min(float(b_raw), float(lambda_cap) * float(s_penalty))

        gate = TableRankerV4.gate_value(s_penalty)

        s_final = float(s_penalty) + float(gate) * float(b_clamped) + float(rescue_boost or 0.0)

        # Safety clamp：boost 只能锦上添花
        if bool(get_retrieval_param("table_scoring.v4_ranker.safety_clamp.enabled", True)):
            s_final = max(s_final, s_penalty)

        return V4ScoreComponents(
            base_score=base,
            stable_base=float(stable_base),
            s_penalty=float(s_penalty),
            b_raw=float(b_raw),
            b_clamped=float(b_clamped),
            gate=float(gate),
            rescue_boost=float(rescue_boost or 0.0),
            s_final=float(s_final),
            year_multiplier=float(year_multiplier),
            structure_multiplier=float(structure_multiplier),
            lambda_cap=float(lambda_cap),
        )
