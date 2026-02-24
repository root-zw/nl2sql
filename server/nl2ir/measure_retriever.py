"""
度量字段专用检索器

度量字段与维度字段检索逻辑不同：
- 维度字段：基于枚举值匹配
- 度量字段：基于聚合意图 + 字段语义匹配

本模块专门处理度量字段的检索逻辑。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

import structlog

from server.config import RetrievalConfig

logger = structlog.get_logger()


@dataclass
class MeasureMatch:
    """度量字段匹配结果"""
    field_id: str
    field_name: str
    display_name: str
    table_id: str
    table_name: str
    score: float = 0.0
    aggregation_type: Optional[str] = None  # SUM, COUNT, AVG, MAX, MIN
    match_type: str = "semantic"  # keyword, semantic, default
    evidence: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_id": self.field_id,
            "field_name": self.field_name,
            "display_name": self.display_name,
            "table_id": self.table_id,
            "table_name": self.table_name,
            "score": self.score,
            "aggregation_type": self.aggregation_type,
            "match_type": self.match_type,
            "evidence": self.evidence,
        }


class MeasureRetriever:
    """
    度量字段检索器
    
    检索策略：
    1. 提取聚合意图（SUM/COUNT/AVG/MAX/MIN）
    2. 从语义模型加载候选度量字段
    3. 基于关键词和语义匹配打分
    """
    
    # 聚合意图关键词映射
    DEFAULT_AGGREGATION_HINTS: Dict[str, List[str]] = {
        "SUM": ["总", "合计", "累计", "总计", "加总", "总共"],
        "COUNT": ["数量", "个数", "多少", "宗数", "几个", "数目", "多少个"],
        "AVG": ["平均", "均值", "平均值", "均"],
        "MAX": ["最大", "最高", "最多", "最大值"],
        "MIN": ["最小", "最低", "最少", "最小值"],
    }
    
    # 度量字段关键词（用于识别度量意图）
    DEFAULT_MEASURE_KEYWORDS = [
        "面积", "金额", "数量", "总计", "用地", "土地", "数", "量",
        "价格", "成本", "收入", "利润", "费用", "金", "额",
    ]
    
    def __init__(
        self,
        db_pool: Optional[Any] = None,
        aggregation_hints: Optional[Dict[str, List[str]]] = None,
        measure_keywords: Optional[List[str]] = None,
    ):
        self.db_pool = db_pool
        self.aggregation_hints = aggregation_hints or self._load_aggregation_hints()
        self.measure_keywords = measure_keywords or self._load_measure_keywords()
        
        # 构建反向映射：关键词 -> 聚合类型
        self._keyword_to_agg: Dict[str, str] = {}
        for agg_type, keywords in self.aggregation_hints.items():
            for kw in keywords:
                self._keyword_to_agg[kw] = agg_type
    
    def _load_aggregation_hints(self) -> Dict[str, List[str]]:
        """从配置加载聚合意图关键词"""
        hints = RetrievalConfig.aggregation_hints()
        return hints if hints else self.DEFAULT_AGGREGATION_HINTS

    def _load_measure_keywords(self) -> List[str]:
        """从配置加载度量关键词，失败则回退默认值"""
        try:
            kw = RetrievalConfig.measure_retrieval_keywords()
            return kw if kw else self.DEFAULT_MEASURE_KEYWORDS
        except Exception:
            return self.DEFAULT_MEASURE_KEYWORDS
    
    def extract_aggregation_hint(self, question: str) -> Tuple[Optional[str], List[str]]:
        """
        从问题中提取聚合意图
        
        Args:
            question: 用户问题
        
        Returns:
            (aggregation_type, matched_keywords)
        """
        matched_keywords = []
        agg_type = None
        
        for keyword, agg in self._keyword_to_agg.items():
            if keyword in question:
                matched_keywords.append(keyword)
                # 优先级：COUNT > SUM > AVG > MAX/MIN
                if agg_type is None:
                    agg_type = agg
                elif agg == "COUNT":
                    agg_type = "COUNT"
                elif agg == "SUM" and agg_type not in ("COUNT",):
                    agg_type = "SUM"
        
        return agg_type, matched_keywords
    
    def extract_measure_intent(self, question: str) -> List[str]:
        """
        从问题中提取度量意图关键词
        
        Args:
            question: 用户问题
        
        Returns:
            匹配的度量关键词列表
        """
        matched = []
        for keyword in self.measure_keywords:
            if keyword in question:
                matched.append(keyword)
        return matched
    
    async def retrieve(
        self,
        question: str,
        table_ids: List[str],
        connection_id: Optional[str] = None,
        top_k: int = 5,
    ) -> List[MeasureMatch]:
        """
        检索度量字段
        
        Args:
            question: 用户问题
            table_ids: 候选表ID列表
            connection_id: 数据库连接ID
            top_k: 返回数量限制
        
        Returns:
            度量字段匹配结果列表
        """
        if not table_ids:
            return []
        
        # 1. 提取聚合意图
        agg_type, agg_keywords = self.extract_aggregation_hint(question)
        
        # 2. 提取度量意图关键词
        measure_keywords = self.extract_measure_intent(question)
        
        # 3. 加载候选度量字段
        measures = await self._load_measures(table_ids, connection_id)
        
        if not measures:
            return []
        
        # 4. 计算匹配分数
        scored_matches: List[MeasureMatch] = []
        for measure in measures:
            score, match_type, evidence = self._compute_relevance(
                question=question,
                measure=measure,
                agg_type=agg_type,
                agg_keywords=agg_keywords,
                measure_keywords=measure_keywords,
            )
            
            if score > 0:
                scored_matches.append(
                    MeasureMatch(
                        field_id=str(measure.get("field_id", "")),
                        field_name=measure.get("field_name", ""),
                        display_name=measure.get("display_name", ""),
                        table_id=str(measure.get("table_id", "")),
                        table_name=measure.get("table_name", ""),
                        score=score,
                        aggregation_type=agg_type,
                        match_type=match_type,
                        evidence=evidence,
                    )
                )
        
        # 5. 排序并返回
        scored_matches.sort(key=lambda m: m.score, reverse=True)
        
        logger.debug(
            "度量字段检索完成",
            question=question[:50],
            table_count=len(table_ids),
            candidate_count=len(measures),
            match_count=len(scored_matches),
            agg_type=agg_type,
        )
        
        return scored_matches[:top_k]
    
    async def _load_measures(
        self,
        table_ids: List[str],
        connection_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        从数据库加载度量字段
        
        Args:
            table_ids: 表ID列表
            connection_id: 连接ID
        
        Returns:
            度量字段列表
        """
        if not self.db_pool or not table_ids:
            return []
        
        try:
            # 转换为UUID列表
            uuid_list = []
            for tid in table_ids:
                try:
                    uuid_list.append(UUID(str(tid)))
                except Exception:
                    continue
            
            query = """
                SELECT
                    f.field_id,
                    f.display_name AS field_name,
                    f.display_name,
                    f.description,
                    f.unit,
                    f.default_aggregation AS aggregation_rule,
                    t.table_id,
                    t.display_name AS table_name,
                    c.data_type
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE t.table_id = ANY($1::uuid[])
                  AND f.field_type = 'measure'
                  AND f.is_active = TRUE
                ORDER BY f.display_name
            """

            if hasattr(self.db_pool, "acquire"):
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch(query, uuid_list)
            else:
                # 兼容：传入的可能是 asyncpg.Connection
                rows = await self.db_pool.fetch(query, uuid_list)
            
            return [dict(row) for row in rows]
        
        except Exception as e:
            logger.error("加载度量字段失败", error=str(e))
            return []
    
    def _compute_relevance(
        self,
        question: str,
        measure: Dict[str, Any],
        agg_type: Optional[str],
        agg_keywords: List[str],
        measure_keywords: List[str],
    ) -> Tuple[float, str, Dict[str, Any]]:
        """
        计算度量字段与问题的相关性
        
        Args:
            question: 用户问题
            measure: 度量字段信息
            agg_type: 聚合类型
            agg_keywords: 聚合关键词
            measure_keywords: 度量关键词
        
        Returns:
            (score, match_type, evidence)
        """
        from server.config import get_retrieval_param
        
        # 从配置读取评分参数（消除硬编码）
        aggregation_match_boost = get_retrieval_param(
            "measure_retrieval.scoring.aggregation_match_boost", 0.3)
        measure_relevance_boost = get_retrieval_param(
            "measure_retrieval.scoring.measure_relevance_boost", 0.2)
        partial_match_boost = get_retrieval_param(
            "measure_retrieval.scoring.partial_match_boost", 0.1)
        name_match_boost = get_retrieval_param(
            "measure_retrieval.scoring.name_match_boost", 0.1)
        default_score = get_retrieval_param(
            "measure_retrieval.scoring.default_score", 0.05)
        
        score = 0.0
        match_type = "default"
        evidence: Dict[str, Any] = {
            "matched_keywords": [],
            "agg_type": agg_type,
        }
        
        display_name = measure.get("display_name", "")
        description = measure.get("description", "") or ""
        field_name = measure.get("field_name", "")
        
        # 1. 精确匹配字段名
        if display_name and display_name in question:
            score += 1.0
            match_type = "keyword"
            evidence["matched_keywords"].append(display_name)
        
        # 2. 度量关键词匹配
        for kw in measure_keywords:
            if kw in display_name or kw in description:
                if kw in question:
                    score += aggregation_match_boost  # 从配置读取
                    evidence["matched_keywords"].append(kw)
        
        # 3. 聚合意图匹配（如果字段有预定义聚合规则）
        agg_rule = measure.get("aggregation_rule") or measure.get("default_aggregation")
        if agg_type and agg_rule:
            if agg_rule.upper() == agg_type:
                score += measure_relevance_boost  # 从配置读取
                evidence["agg_rule_match"] = True
        
        # 4. 数据类型加分（数值类型更可能是度量）
        data_type = (measure.get("data_type") or "").lower()
        if any(t in data_type for t in ["int", "float", "decimal", "numeric", "money"]):
            score += partial_match_boost  # 从配置读取
        
        # 5. 描述语义匹配
        for kw in agg_keywords:
            if kw in description:
                score += name_match_boost  # 从配置读取
        
        # 如果没有任何匹配，给一个基础分（确保所有度量字段都有候选）
        if score == 0 and measure_keywords:
            score = default_score  # 从配置读取
            match_type = "default"
        
        return score, match_type, evidence
    
    def suggest_aggregation(
        self,
        question: str,
        field_name: str,
        data_type: Optional[str] = None,
    ) -> str:
        """
        根据问题和字段信息推荐聚合函数
        
        Args:
            question: 用户问题
            field_name: 字段名
            data_type: 数据类型
        
        Returns:
            推荐的聚合函数 (SUM, COUNT, AVG, MAX, MIN)
        """
        agg_type, _ = self.extract_aggregation_hint(question)
        
        if agg_type:
            return agg_type
        
        # 默认逻辑：
        # - 带"数量"、"个数"的字段默认COUNT
        # - 带"金额"、"面积"的字段默认SUM
        # - 其他默认SUM
        name_lower = field_name.lower()
        
        if any(kw in name_lower for kw in ["数量", "个数", "数目"]):
            return "COUNT"
        elif any(kw in name_lower for kw in ["金额", "面积", "价格", "成本"]):
            return "SUM"
        else:
            return "SUM"
