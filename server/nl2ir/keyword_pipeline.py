"""
关键词三段提取：实体值 / 字段 Token / 数字

结合 `threee.md` 规范，预先对语义模型中的字段、枚举构建倒排索引，
在检索早期就标记出和问题强相关的表、字段和值，供后续模块做 Boost。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Any
import re
import structlog

from server.models.semantic import SemanticModel, Field, FieldEnumValue
from server.nl2ir.sparse_utils import (
    protect_sql_identifiers,
    restore_sql_identifiers,
)

logger = structlog.get_logger()


@dataclass
class KeywordExtractionResult:
    """统一返回的关键词提取结果"""

    entities: List[str] = field(default_factory=list)
    field_tokens: List[str] = field(default_factory=list)
    numbers: List[str] = field(default_factory=list)
    raw_tokens: List[str] = field(default_factory=list)
    field_hits: Dict[str, List[str]] = field(default_factory=dict)  # field_id -> tokens
    table_boosts: Dict[str, float] = field(default_factory=dict)  # table_id -> weight
    enum_hits: Dict[str, List[str]] = field(default_factory=dict)  # field_id -> normalized value tokens
    normalized_enum_tokens: Set[str] = field(default_factory=set)
    trace: Dict[str, Any] = field(default_factory=dict)
    
    # P0新增：度量字段识别
    measure_hits: Dict[str, List[str]] = field(default_factory=dict)  # field_id -> matched tokens
    measure_tokens: List[str] = field(default_factory=list)  # 匹配到的度量关键词
    dimension_hits: Dict[str, List[str]] = field(default_factory=dict)  # field_id -> matched tokens


class KeywordExtractor:
    """
    根据语义模型构建字段/枚举倒排索引：
    1) Field display_name/field_name/synonyms → field_id
    2) 枚举值 standard_value/display_name/synonym_text → field_id
    3) P0新增：度量字段单独索引，用于度量识别
    """

    def __init__(self, semantic_model: Optional[SemanticModel] = None):
        self.semantic_model = semantic_model
        self.field_token_index: Dict[str, Set[str]] = {}  # 所有字段（维度+度量）
        self.enum_token_index: Dict[str, List[FieldEnumValue]] = {}
        self.field_to_table: Dict[str, str] = {}
        
        # P0新增：按类型分离的字段索引
        self.measure_token_index: Dict[str, Set[str]] = {}  # 度量字段索引
        self.dimension_token_index: Dict[str, Set[str]] = {}  # 维度字段索引
        self.field_category_map: Dict[str, str] = {}  # field_id -> category
        
        # 从配置中读取权重（消除硬编码）
        self._load_boost_weights()
        # 分词与噪声控制参数（提升鲁棒性，避免滑窗爆炸/噪声 Token）
        self._load_tokenization_config()

        if semantic_model:
            self._build_indexes()
    
    def _load_boost_weights(self) -> None:
        """从配置文件加载权重系数"""
        try:
            from server.config import get_retrieval_param
            self.measure_boost_weight = get_retrieval_param(
                "keyword_extractor.boost_weights.measure", 0.3
            )
            self.dimension_boost_weight = get_retrieval_param(
                "keyword_extractor.boost_weights.dimension", 1.0
            )
            self.enum_boost_weight = get_retrieval_param(
                "keyword_extractor.boost_weights.enum", 1.0
            )
        except Exception:
            # 配置加载失败时使用默认值
            self.measure_boost_weight = 0.3
            self.dimension_boost_weight = 1.0
            self.enum_boost_weight = 1.0

    def _load_tokenization_config(self) -> None:
        """
        从 retrieval_config.yaml 加载 KeywordExtractor 的分词/噪声控制配置。
        目标：避免过度依赖硬编码/写死规则，通过可配置方式增强不同数据源下的鲁棒性。
        """
        try:
            from server.config import get_retrieval_param

            self.min_token_len = int(get_retrieval_param("keyword_extractor.tokenization.min_token_len", 2) or 2)
            # 是否使用统一 Tokenizer（推荐：更贴近系统分词策略+自定义词典）
            self.use_shared_tokenizer = bool(get_retrieval_param("keyword_extractor.tokenization.use_shared_tokenizer", True))
            # Token 数量上限（防止极长问题导致 token 爆炸 -> 噪声 boost）
            self.max_tokens = int(get_retrieval_param("keyword_extractor.tokenization.max_tokens", 128) or 128)
            # 额外停用词（项目可配置，不在代码硬编码业务词）
            self.extra_stopwords = set(get_retrieval_param("keyword_extractor.tokenization.extra_stopwords", []) or [])

            # 仅在 regex fallback 模式启用滑窗
            self.enable_sliding_window = bool(get_retrieval_param("keyword_extractor.tokenization.enable_sliding_window", False))
            self.sliding_window_max_window = int(get_retrieval_param("keyword_extractor.tokenization.sliding_window.max_window", 4) or 4)
            self.sliding_window_max_expansions = int(get_retrieval_param("keyword_extractor.tokenization.sliding_window.max_expansions", 200) or 200)
        except Exception:
            # 保守默认：使用共享 Tokenizer + 上限，禁用滑窗，避免噪声
            self.min_token_len = 2
            self.use_shared_tokenizer = True
            self.max_tokens = 128
            self.extra_stopwords = set()
            self.enable_sliding_window = False
            self.sliding_window_max_window = 4
            self.sliding_window_max_expansions = 200

    def update_model(self, semantic_model: SemanticModel) -> None:
        """允许运行时切换语义模型"""
        self.semantic_model = semantic_model
        self.field_token_index.clear()
        self.enum_token_index.clear()
        self.field_to_table.clear()
        self.measure_token_index.clear()
        self.dimension_token_index.clear()
        self.field_category_map.clear()
        if semantic_model:
            self._build_indexes()

    def extract(self, question: str) -> KeywordExtractionResult:
        """执行关键词三段提取
        
        P0改进：
        - 度量字段恢复到索引中，使用较低权重
        - 分别记录度量/维度命中，供后续精准判断
        """
        if not question:
            return KeywordExtractionResult()

        tokens = self._tokenize(question)
        numbers = self._extract_numbers(question)
        cn_entities = self._extract_cn_entities(question)

        result = KeywordExtractionResult(
            entities=cn_entities.copy(),
            numbers=numbers,
            raw_tokens=tokens.copy(),
        )

        field_hits_counter: Dict[str, int] = {}
        entities_set = set(result.entities)
        enum_hit_counter: Dict[str, int] = {}
        measure_hit_counter: Dict[str, int] = {}
        dimension_hit_counter: Dict[str, int] = {}

        for token in tokens:
            normalized = self._normalize_token(token)
            if not normalized:
                continue

            # 1. 度量字段索引匹配（P0新增）
            measure_fields = self.measure_token_index.get(normalized)
            if measure_fields:
                result.measure_tokens.append(token)
                for field_id in measure_fields:
                    measure_hit_counter[field_id] = measure_hit_counter.get(field_id, 0) + 1
                    if field_id not in result.measure_hits:
                        result.measure_hits[field_id] = []
                    result.measure_hits[field_id].append(token)
                    # 度量字段对表打分使用较低权重（从配置读取）
                    table_id = self.field_to_table.get(field_id)
                    if table_id:
                        result.table_boosts[table_id] = (
                            result.table_boosts.get(table_id, 0.0) 
                            + self.measure_boost_weight
                        )
            
            # 2. 维度字段索引匹配
            dimension_fields = self.dimension_token_index.get(normalized)
            if dimension_fields:
                for field_id in dimension_fields:
                    dimension_hit_counter[field_id] = dimension_hit_counter.get(field_id, 0) + 1
                    if field_id not in result.dimension_hits:
                        result.dimension_hits[field_id] = []
                    result.dimension_hits[field_id].append(token)
                    # 维度字段使用标准权重（从配置读取）
                    table_id = self.field_to_table.get(field_id)
                    if table_id:
                        result.table_boosts[table_id] = (
                            result.table_boosts.get(table_id, 0.0) 
                            + self.dimension_boost_weight
                        )

            # 3. 综合字段索引匹配（兼容旧逻辑）
            matched_fields = self.field_token_index.get(normalized)
            if matched_fields:
                result.field_tokens.append(token)
                for field_id in matched_fields:
                    field_hits_counter[field_id] = field_hits_counter.get(field_id, 0) + 1
                    if field_id not in result.field_hits:
                        result.field_hits[field_id] = []
                    result.field_hits[field_id].append(token)

            # 4. 枚举倒排索引
            enum_values = self.enum_token_index.get(normalized)
            if enum_values:
                result.normalized_enum_tokens.add(normalized)
                for enum in enum_values:
                    field_id = enum.field_id
                    enum_hit_counter[field_id] = enum_hit_counter.get(field_id, 0) + 1
                    if field_id not in result.enum_hits:
                        result.enum_hits[field_id] = []
                    result.enum_hits[field_id].append(normalized)
                    table_id = self.field_to_table.get(field_id)
                    if table_id:
                        result.table_boosts[table_id] = (
                            result.table_boosts.get(table_id, 0.0) 
                            + self.enum_boost_weight  # 从配置读取
                        )
                    display_value = enum.display_name or enum.standard_value
                    if display_value:
                        entities_set.add(display_value)

        result.entities = list(dict.fromkeys(list(entities_set)))
        result.field_tokens = list(dict.fromkeys(result.field_tokens))
        result.measure_tokens = list(dict.fromkeys(result.measure_tokens))

        result.trace = {
            "field_hit_count": sum(field_hits_counter.values()),
            "enum_hit_count": sum(enum_hit_counter.values()),
            "measure_hit_count": sum(measure_hit_counter.values()),  # P0新增
            "dimension_hit_count": sum(dimension_hit_counter.values()),  # P0新增
            "table_boosts": len(result.table_boosts),
            "raw_token_count": len(tokens),
        }
        
        # 调试日志
        if result.measure_hits:
            logger.debug(
                "度量字段命中",
                measure_tokens=result.measure_tokens,
                measure_field_count=len(result.measure_hits),
            )

        return result

    # --------------------------------------------------------------------- #
    # 构建索引
    # --------------------------------------------------------------------- #
    # P0改进：度量字段恢复索引，但使用分离的索引和较低权重
    
    def _build_indexes(self) -> None:
        """构建字段和枚举的倒排索引
        
        P0改进：
        - 度量字段恢复到综合索引（field_token_index）
        - 新增分离的度量索引（measure_token_index）和维度索引（dimension_token_index）
        - 记录字段类别映射（field_category_map）
        """
        if not self.semantic_model:
            return

        measure_count = 0
        dimension_count = 0
        other_count = 0
        
        for field_id, field in (self.semantic_model.fields or {}).items():
            table_id = getattr(field, "datasource_id", None)
            if not table_id:
                continue
            self.field_to_table[field_id] = table_id
            
            # 记录字段类别
            field_category = getattr(field, "field_category", None) or "dimension"
            self.field_category_map[field_id] = field_category
            
            # 提取字段tokens
            field_tokens = self._field_tokens(field)
            
            for token in field_tokens:
                # 综合索引（所有字段）
                self.field_token_index.setdefault(token, set()).add(field_id)
                
                # 按类别分离索引
                if field_category == "measure":
                    self.measure_token_index.setdefault(token, set()).add(field_id)
                    measure_count += 1
                elif field_category == "dimension":
                    self.dimension_token_index.setdefault(token, set()).add(field_id)
                    dimension_count += 1
                else:
                    other_count += 1

        # 枚举值索引
        for field_id, enum_values in (self.semantic_model.field_enums or {}).items():
            for enum in enum_values or []:
                for token in self._enum_tokens(enum):
                    self.enum_token_index.setdefault(token, []).append(enum)

        logger.info(
            "关键词索引构建完成（P0增强）",
            total_field_tokens=len(self.field_token_index),
            measure_tokens=len(self.measure_token_index),
            dimension_tokens=len(self.dimension_token_index),
            enum_tokens=len(self.enum_token_index),
            measure_fields=measure_count,
            dimension_fields=dimension_count,
        )

    def _field_tokens(self, field: Field) -> Set[str]:
        tokens: Set[str] = set()
        for raw in [
            field.display_name,
            field.field_name,
            getattr(field, "description", None),
        ] + list(field.synonyms or []):
            normalized = self._normalize_token(raw)
            if normalized:
                tokens.add(normalized)
        return tokens

    def _enum_tokens(self, enum_value: FieldEnumValue) -> Set[str]:
        tokens: Set[str] = set()
        candidates = [
            enum_value.standard_value,
            enum_value.display_name,
        ]
        for synonym in enum_value.synonyms or []:
            synonym_text = getattr(synonym, "synonym_text", None)
            if synonym_text:
                candidates.append(synonym_text)
        for raw in candidates:
            normalized = self._normalize_token(raw)
            if normalized:
                tokens.add(normalized)
        return tokens

    # --------------------------------------------------------------------- #
    # 基础工具
    # --------------------------------------------------------------------- #
    def _tokenize(self, text: str) -> List[str]:
        """
        分词函数
        
        优化：
        - 使用 protect_sql_identifiers 保护SQL标识符（如 t1.col_name）
        - 滑动窗口生成短词变体，提高召回率
        """
        if not text:
            return []
        
        # 优先使用统一 Tokenizer（更鲁棒：自定义词典、停用词策略、SQL标识符保护一致）
        if getattr(self, "use_shared_tokenizer", True):
            try:
                from server.nl2ir.tokenizer import Tokenizer

                tokenizer = Tokenizer.get_instance()
                # KeywordExtractor 用于“Boost”，更接近 sparse 策略（去停用词、偏短词召回）
                tokens = tokenizer.tokenize_for_sparse(text)
                filtered = []
                for t in tokens:
                    if not t:
                        continue
                    if len(t) < self.min_token_len:
                        continue
                    if t in self.extra_stopwords:
                        continue
                    # 过滤纯数字 token（年份由 year_sensitive_match 专门处理，避免在 keyword_boost 中引入噪声）
                    if t.isdigit():
                        continue
                    filtered.append(t)
                # 防爆：最多保留 max_tokens 个 token（保持顺序）
                return filtered[: self.max_tokens]
            except Exception as e:
                logger.warning("KeywordExtractor 使用共享分词器失败，回退到regex分词", error=str(e))

        # ========== regex fallback ==========
        # SQL标识符保护
        protected_text, placeholders = protect_sql_identifiers(text)

        # 基础分词（中文块/英文数字块）
        base_tokens = re.findall(r"[A-Za-z0-9_]+|[\u4e00-\u9fa5]+", protected_text)

        # 恢复SQL标识符
        if placeholders:
            base_tokens = restore_sql_identifiers(base_tokens, placeholders)

        expanded: List[str] = []
        expansion_budget = getattr(self, "sliding_window_max_expansions", 200)
        for token in base_tokens:
            expanded.append(token)

            if not getattr(self, "enable_sliding_window", False):
                continue

            # 对中文 token 做滑动窗口（可配置、可限流）
            if not re.fullmatch(r"[\u4e00-\u9fa5]+", token):
                continue
            if len(token) <= self.min_token_len:
                continue

            max_window = min(getattr(self, "sliding_window_max_window", 4), len(token))
            for window in range(self.min_token_len, max_window + 1):
                for start in range(0, len(token) - window + 1):
                    if len(expanded) >= expansion_budget:
                        break
                    expanded.append(token[start : start + window])
                if len(expanded) >= expansion_budget:
                    break

        ordered_unique: List[str] = []
        seen = set()
        for token in expanded:
            if not token:
                continue
            token = token.strip()
            if not token:
                continue
            if len(token) < self.min_token_len:
                continue
            if token in self.extra_stopwords:
                continue
            # 过滤纯数字 token（年份由 year_sensitive_match 专门处理，避免在 keyword_boost 中引入噪声）
            if token.isdigit():
                continue
            if token in seen:
                continue
            seen.add(token)
            ordered_unique.append(token)
            if len(ordered_unique) >= self.max_tokens:
                break
        return ordered_unique

    @staticmethod
    def _extract_numbers(text: str) -> List[str]:
        return re.findall(r"-?\d+(?:\.\d+)?", text)

    @staticmethod
    def _extract_cn_entities(text: str) -> List[str]:
        matches = re.findall(r"[\u4e00-\u9fa5]{2,}", text)
        ordered_unique: List[str] = []
        seen = set()
        for word in matches:
            if word in seen:
                continue
            seen.add(word)
            ordered_unique.append(word)
        return ordered_unique

    def _normalize_token(self, token: Optional[str]) -> str:
        if not token:
            return ""
        normalized = re.sub(r"\s+", "", str(token)).lower()
        return normalized if len(normalized) >= getattr(self, "min_token_len", 2) else ""


__all__ = ["KeywordExtractionResult", "KeywordExtractor"]
