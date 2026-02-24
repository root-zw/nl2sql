"""
表-字段-枚举值联合后融合（Post-Fusion）

根据 `threee.md` 的四步策略：
1. Enum confirms Table
2. Enum rescues Table
3. Table constrains Enum
4. Joint Ranking
"""

from __future__ import annotations

from typing import List, Dict, Any, Optional
import math
import numpy as np

from server.nl2ir.table_retriever import TableRetrievalResult
from server.nl2ir.enum_value_retriever import EnumMatch


class PostFusionScorer:
    """负责将表检索结果与枚举值匹配结果融合为三元组"""

    def __init__(
        self,
        enum_weight: float = 0.45,
        table_weight: float = 0.35,
        context_weight: float = 0.20,
        exact_boost: float = 0.20,
        rescue_threshold: float = 0.98,
        table_constraint_threshold: float = 0.25,
        max_results: int = 5,
    ):
        self.enum_weight = enum_weight
        self.table_weight = table_weight
        self.context_weight = context_weight
        self.exact_boost = exact_boost
        self.rescue_threshold = rescue_threshold
        self.table_constraint_threshold = table_constraint_threshold
        self.max_results = max_results

    def combine(
        self,
        tables: List[TableRetrievalResult],
        enums: List[EnumMatch],
        question_vector: Optional[List[float]] = None,
    ) -> List[Dict[str, Any]]:
        if not enums:
            return []

        table_map: Dict[str, TableRetrievalResult] = {
            t.table_id: t for t in tables or [] if t.table_id
        }

        results: List[Dict[str, Any]] = []

        for match in enums:
            table_id = match.table_id
            if not table_id and match.payload:
                table_id = match.payload.get("table_id")
            if not table_id:
                continue

            table_entry = table_map.get(table_id)
            table_score = table_entry.score if table_entry else 0.0
            table_norm = self._normalize_score(table_score)
            enum_norm = self._normalize_score(match.final_score or match.similarity)

            rescued = False
            if not table_entry and enum_norm >= self.rescue_threshold:
                table_norm = 0.5  # 默认给一个中性分
                rescued = True

            constrained = False
            if table_entry and table_norm < self.table_constraint_threshold:
                enum_norm *= 0.6
                constrained = True

            context_score = 0.0
            if question_vector and match.context_vector:
                context_score = max(
                    0.0, self._cosine_similarity(question_vector, match.context_vector)
                )

            joint_score = (
                self.enum_weight * enum_norm
                + self.table_weight * table_norm
                + self.context_weight * context_score
            )

            exact_boost_applied = False
            if match.match_type in {"exact", "synonym"}:
                joint_score += self.exact_boost
                exact_boost_applied = True

            table_name = (
                getattr(table_entry.datasource, "display_name", None)
                if table_entry and table_entry.datasource
                else match.table_name
            )

            trace = {
                "enum_confirms_table": bool(table_entry),
                "rescued_table": rescued,
                "table_constrained": constrained,
                "exact_boost": exact_boost_applied,
                "context_score": round(context_score, 4),
                "table_score": round(table_norm, 4),
                "enum_score": round(enum_norm, 4),
            }

            results.append(
                {
                    "table_id": table_id,
                    "table_name": table_name,
                    "field_id": match.field_id,
                    "field_name": match.field_name,
                    "value": match.display_name or match.value,
                    "raw_value": match.value,
                    "match_type": match.match_type,
                    "final_score": round(joint_score, 4),
                    "enum_final_score": round(match.final_score or 0.0, 4),
                    "trace": trace | match.trace,
                }
            )

        results.sort(key=lambda item: item["final_score"], reverse=True)
        return results[: self.max_results]

    @staticmethod
    def _normalize_score(value: Optional[float]) -> float:
        if value is None or math.isnan(value):
            return 0.0
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        try:
            v1 = np.array(vec1)
            v2 = np.array(vec2)
            denom = np.linalg.norm(v1) * np.linalg.norm(v2)
            if denom == 0:
                return 0.0
            return float(np.dot(v1, v2) / denom)
        except Exception:
            return 0.0


__all__ = ["PostFusionScorer"]

