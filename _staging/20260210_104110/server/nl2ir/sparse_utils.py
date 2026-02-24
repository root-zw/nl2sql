"""
通用的稀疏向量/文本处理工具
供同步与检索流程共享，避免重复实现 BM25 文本构造逻辑。

优化版本:
- 支持SQL标识符保护（避免切分 t1.col_name 等）
- 差异化分词策略：Dense保留上下文，Sparse剔除停用词
- 领域词典加载
- P1新增：实体预识别与长词优先匹配
- P2新增：统一分词管理器（带缓存、可配置）
"""

from __future__ import annotations

import json
import math
import re
import hashlib
from collections import Counter
from typing import Dict, List, Sequence, Tuple, Optional, TYPE_CHECKING

from server.utils.text_templates import get_stopwords

if TYPE_CHECKING:
    from server.nl2ir.entity_recognizer import EntityRecognizer, EntityRecognitionResult

# 停用词集合
STOPWORDS = set(get_stopwords())

# 正则表达式
WHITESPACE_RE = re.compile(r"\s+")
ALNUM_RE = re.compile(r"[A-Za-z0-9_]+")
CJK_RE = re.compile(r"[\u4e00-\u9fff]")

# SQL标识符保护模式（只匹配ASCII字符）
SQL_IDENTIFIER_PATTERNS = [
    re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)(?=[^a-zA-Z0-9_]|$)'),  # t1.col_name
    re.compile(r'([a-zA-Z][a-zA-Z0-9]*_[a-zA-Z][a-zA-Z0-9_]*)(?=[^a-zA-Z0-9_]|$)'),  # field_name with underscore
]

# 实体占位符模式（P1新增）
ENTITY_PLACEHOLDER_RE = re.compile(r'__(?:DOMAIN|TABLE|FIELD|ENUM)_[a-f0-9]{8}__')

# 全局实体识别器（可选，按需初始化）
_entity_recognizer: Optional["EntityRecognizer"] = None

# 是否使用统一分词器（默认启用）
_use_unified_tokenizer = True


def _get_jieba():
    """获取jieba模块（延迟导入，优先使用统一分词器）"""
    if _use_unified_tokenizer:
        try:
            from server.nl2ir.tokenizer import get_tokenizer
            return get_tokenizer()
        except Exception:
            pass
    
    # Fallback到直接使用jieba
    import jieba
    return jieba


def _jieba_cut(text: str) -> List[str]:
    """统一的分词入口"""
    tokenizer = _get_jieba()
    if hasattr(tokenizer, 'cut'):
        # 使用统一分词器
        return tokenizer.cut(text)
    else:
        # 直接使用jieba
        return list(tokenizer.lcut(text))


def set_entity_recognizer(recognizer: "EntityRecognizer") -> None:
    """设置全局实体识别器（P1新增）"""
    global _entity_recognizer
    _entity_recognizer = recognizer


def get_entity_recognizer() -> Optional["EntityRecognizer"]:
    """获取全局实体识别器"""
    return _entity_recognizer


def protect_sql_identifiers(text: str) -> Tuple[str, Dict[str, str]]:
    """
    保护SQL标识符不被分词切分
    
    Args:
        text: 原始文本
    
    Returns:
        (protected_text, placeholders_dict)
        protected_text: 替换后的文本
        placeholders_dict: 占位符到原始值的映射
    """
    if not text:
        return text, {}
    
    placeholders: Dict[str, str] = {}
    protected = text
    counter = 0
    
    for pattern in SQL_IDENTIFIER_PATTERNS:
        for match in pattern.findall(protected):
            if match not in placeholders.values():
                placeholder = f"__SQLID_{counter}__"
                placeholders[placeholder] = match
                protected = protected.replace(match, placeholder, 1)
                counter += 1
    
    return protected, placeholders


def restore_sql_identifiers(tokens: List[str], placeholders: Dict[str, str]) -> List[str]:
    """
    恢复被保护的SQL标识符
    
    Args:
        tokens: 分词结果
        placeholders: 占位符映射
    
    Returns:
        恢复后的分词结果
    """
    if not placeholders:
        return tokens
    
    result = []
    for token in tokens:
        if token in placeholders:
            result.append(placeholders[token])
        else:
            result.append(token)
    return result


def normalize_whitespace(text: str) -> str:
    return WHITESPACE_RE.sub(" ", text or "").strip()


def truncate_text(text: str, limit: int) -> str:
    if not text or limit <= 0:
        return text or ""
    return text[:limit]


def ensure_list(value) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if x]
        except json.JSONDecodeError:
            return [s.strip() for s in value.split(",") if s.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(x).strip() for x in value if x]
    return [str(value).strip()]


def _contains_cjk(text: str) -> bool:
    return bool(CJK_RE.search(text))


def _base_tokenize(
    text: str,
    protect_sql: bool = True,
    use_entity_recognition: bool = False,
) -> Tuple[List[str], Dict[str, str], Optional["EntityRecognitionResult"]]:
    """
    基础分词函数
    
    P1优化：支持实体预识别和长词优先匹配
    
    Args:
        text: 待分词文本
        protect_sql: 是否保护SQL标识符
        use_entity_recognition: 是否使用实体预识别（P1新增）
    
    Returns:
        (tokens, sql_placeholders, entity_result)
    """
    # 注意：领域词典已由统一分词器（Tokenizer）在初始化时加载，无需重复加载
    
    cleaned = normalize_whitespace(text)
    if not cleaned:
        return [], {}, None
    
    entity_result: Optional["EntityRecognitionResult"] = None
    
    # P1新增：实体预识别（在SQL保护之前）
    if use_entity_recognition and _entity_recognizer:
        from server.nl2ir.entity_recognizer import EntityRecognitionResult
        entity_result = _entity_recognizer.recognize(cleaned)
        if entity_result.recognized_entities:
            cleaned = entity_result.protected_text
    
    # SQL标识符保护
    sql_placeholders: Dict[str, str] = {}
    if protect_sql:
        cleaned, sql_placeholders = protect_sql_identifiers(cleaned)
    
    tokens: List[str] = []
    lowered = cleaned.lower()
    
    # CJK分词（使用统一分词器）
    if _contains_cjk(cleaned):
        tokens.extend(_jieba_cut(cleaned))
    
    # 提取字母数字token
    tokens.extend(ALNUM_RE.findall(lowered))
    
    # 空格分词（作为补充）
    tokens.extend(lowered.split(" "))
    
    # 恢复SQL标识符
    if sql_placeholders:
        tokens = restore_sql_identifiers(tokens, sql_placeholders)
    
    # P1新增：恢复实体占位符为标准名称
    if entity_result and entity_result.placeholder_map:
        tokens = entity_result.restore_tokens(tokens)
    
    return tokens, sql_placeholders, entity_result


def tokenize_for_dense(
    text: str,
    protect_sql: bool = True,
    use_entity_recognition: bool = False,
) -> List[str]:
    """
    Dense向量专用分词
    - 保留上下文
    - 不去除停用词（保留语义信息）
    - 保护SQL标识符
    - P1新增：支持实体预识别
    
    Args:
        text: 待分词文本
        protect_sql: 是否保护SQL标识符
        use_entity_recognition: 是否使用实体预识别
    
    Returns:
        分词结果列表
    """
    tokens, _, _ = _base_tokenize(
        text,
        protect_sql=protect_sql,
        use_entity_recognition=use_entity_recognition,
    )
    
    # Dense模式：仅去除空白token，保留停用词
    normalized_tokens: List[str] = []
    for token in tokens:
        item = token.strip().lower()
        if not item:
            continue
        # 跳过未恢复的占位符
        if ENTITY_PLACEHOLDER_RE.match(item):
            continue
        normalized_tokens.append(item)
    
    return normalized_tokens


def tokenize_for_sparse(
    text: str,
    protect_sql: bool = True,
    use_entity_recognition: bool = False,
) -> List[str]:
    """
    Sparse向量/BM25专用分词
    - 剔除停用词（提升检索精度）
    - 保护SQL标识符
    - P1新增：支持实体预识别
    
    Args:
        text: 待分词文本
        protect_sql: 是否保护SQL标识符
        use_entity_recognition: 是否使用实体预识别
    
    Returns:
        分词结果列表
    """
    tokens, _, _ = _base_tokenize(
        text,
        protect_sql=protect_sql,
        use_entity_recognition=use_entity_recognition,
    )
    
    # Sparse模式：去除停用词
    normalized_tokens: List[str] = []
    for token in tokens:
        item = token.strip().lower()
        if not item:
            continue
        if item in STOPWORDS:
            continue
        # 跳过未恢复的占位符
        if ENTITY_PLACEHOLDER_RE.match(item):
            continue
        normalized_tokens.append(item)
    
    return normalized_tokens


def tokenize_for_bm25(text: str, use_entity_recognition: bool = False) -> List[str]:
    """
    为BM25分词（兼容旧接口）
    实际调用 tokenize_for_sparse
    """
    return tokenize_for_sparse(text, protect_sql=True, use_entity_recognition=use_entity_recognition)


def tokenize_with_entity_recognition(text: str) -> Tuple[List[str], Optional["EntityRecognitionResult"]]:
    """
    P1新增：带实体识别结果的分词
    
    返回分词结果和实体识别详情，供调用方进一步处理
    """
    tokens, _, entity_result = _base_tokenize(
        text,
        protect_sql=True,
        use_entity_recognition=True,
    )
    
    # 去除停用词和空白
    normalized_tokens: List[str] = []
    for token in tokens:
        item = token.strip().lower()
        if not item:
            continue
        if item in STOPWORDS:
            continue
        if ENTITY_PLACEHOLDER_RE.match(item):
            continue
        normalized_tokens.append(item)
    
    return normalized_tokens, entity_result


def _hash_token(token: str) -> int:
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=4).digest()
    return int.from_bytes(digest, byteorder="big")


def build_sparse_vector(text: str) -> Dict[str, List[float]]:
    tokens = tokenize_for_bm25(text)
    if not tokens:
        return {"indices": [], "values": []}

    counts = Counter(tokens)
    norm = math.sqrt(sum(v * v for v in counts.values()))
    if not norm:
        return {"indices": [], "values": []}

    aggregated: Dict[int, float] = {}
    for token, freq in counts.items():
        idx = _hash_token(token)
        aggregated[idx] = aggregated.get(idx, 0.0) + freq / norm

    sorted_items = sorted(aggregated.items(), key=lambda item: item[0])
    return {
        "indices": [idx for idx, _ in sorted_items],
        "values": [val for _, val in sorted_items],
    }


def prepare_bm25_text(parts: Sequence[str], limit: int) -> str:
    joined = " ".join(normalize_whitespace(part) for part in parts if part)
    return truncate_text(joined, limit)


def prepare_dense_text(text: str, limit: int) -> str:
    return truncate_text(normalize_whitespace(text), limit)


