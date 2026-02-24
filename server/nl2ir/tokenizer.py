"""
统一分词管理器

提供：
1. 单例模式统一管理jieba分词器
2. 词典预加载和热更新
3. 分词结果LRU缓存
4. 可配置的分词参数
5. 性能监控和统计

用法：
    from server.nl2ir.tokenizer import Tokenizer
    
    # 获取单例
    tokenizer = Tokenizer.get_instance()
    
    # 分词
    tokens = tokenizer.cut("武汉市江岸区建设用地")
    
    # 带词性标注
    tokens_with_pos = tokenizer.cut_with_pos("武汉市江岸区建设用地")
"""

from __future__ import annotations

import os
import re
import time
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set
import structlog

logger = structlog.get_logger()


@dataclass
class TokenizerConfig:
    """分词器配置"""
    # 词典配置
    domain_dict_path: str = "config/domain_dict.txt"
    extra_dict_paths: List[str] = field(default_factory=list)
    preload_on_startup: bool = True
    warmup_text: str = "武汉市江岸区建设用地批准书用地面积统计"
    
    # SQL标识符保护
    protect_sql_identifiers: bool = True
    sql_identifier_patterns: List[str] = field(default_factory=lambda: [
        r'\b[a-zA-Z_]\w*\.[a-zA-Z_]\w*\b',
        r'\b[a-zA-Z_]\w*_[a-zA-Z_]\w*\b',
    ])
    
    # jieba参数
    use_hmm: bool = True
    cut_all: bool = False
    enable_pos_tagging: bool = True
    
    # Dense分词
    dense_remove_stopwords: bool = False
    dense_lowercase: bool = True
    dense_use_entity_recognition: bool = False
    
    # Sparse分词
    sparse_remove_stopwords: bool = True
    sparse_lowercase: bool = True
    sparse_use_entity_recognition: bool = True
    
    # 缓存配置
    cache_enabled: bool = True
    cache_max_size: int = 10000
    cache_ttl: int = 0
    
    # 性能监控
    log_timing: bool = False
    slow_threshold_ms: float = 50.0


class LRUCache:
    """线程安全的LRU缓存"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.cache: OrderedDict[str, Tuple[List[str], float]] = OrderedDict()
        self.lock = threading.RLock()
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[List[str]]:
        with self.lock:
            if key in self.cache:
                # 移到末尾（最近使用）
                self.cache.move_to_end(key)
                self.hits += 1
                return self.cache[key][0]
            self.misses += 1
            return None
    
    def put(self, key: str, value: List[str]) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.max_size:
                    # 删除最久未使用
                    self.cache.popitem(last=False)
            self.cache[key] = (value, time.time())
    
    def clear(self) -> None:
        with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0
    
    def stats(self) -> Dict[str, Any]:
        with self.lock:
            total = self.hits + self.misses
            hit_rate = self.hits / total if total > 0 else 0
            return {
                "size": len(self.cache),
                "max_size": self.max_size,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": f"{hit_rate:.2%}",
            }


class Tokenizer:
    """
    统一分词管理器（单例）
    
    特性：
    - 单例模式，全局唯一
    - 词典预加载和热更新
    - LRU缓存避免重复分词
    - 可配置的分词参数
    - 性能监控
    """
    
    _instance: Optional["Tokenizer"] = None
    _lock = threading.Lock()
    
    def __init__(self, config: Optional[TokenizerConfig] = None):
        """私有构造函数，使用get_instance()获取实例"""
        self.config = config or self._load_config()
        self._initialized = False
        self._jieba = None
        self._jieba_posseg = None
        self._cache: Optional[LRUCache] = None
        self._stopwords: Set[str] = set()
        self._sql_patterns: List[re.Pattern] = []
        
        # 性能统计
        self._total_calls = 0
        self._total_time_ms = 0.0
        
        # 初始化
        self._initialize()
    
    @classmethod
    def get_instance(cls, config: Optional[TokenizerConfig] = None) -> "Tokenizer":
        """获取分词器单例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(config)
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（用于测试或热更新配置）"""
        with cls._lock:
            if cls._instance is not None:
                cls._instance._cache.clear() if cls._instance._cache else None
                cls._instance = None
    
    def _load_config(self) -> TokenizerConfig:
        """从retrieval_config.yaml加载配置"""
        try:
            from server.config import get_retrieval_param
            
            return TokenizerConfig(
                domain_dict_path=get_retrieval_param(
                    "tokenization.domain_dict_path", "config/domain_dict.txt"
                ),
                extra_dict_paths=get_retrieval_param(
                    "tokenization.extra_dict_paths", []
                ),
                preload_on_startup=get_retrieval_param(
                    "tokenization.preload_on_startup", True
                ),
                warmup_text=get_retrieval_param(
                    "tokenization.warmup_text", "武汉市江岸区建设用地批准书用地面积统计"
                ),
                protect_sql_identifiers=get_retrieval_param(
                    "tokenization.protect_sql_identifiers", True
                ),
                sql_identifier_patterns=get_retrieval_param(
                    "tokenization.sql_identifier_patterns", []
                ),
                use_hmm=get_retrieval_param("tokenization.jieba.use_hmm", True),
                cut_all=get_retrieval_param("tokenization.jieba.cut_all", False),
                enable_pos_tagging=get_retrieval_param(
                    "tokenization.jieba.enable_pos_tagging", True
                ),
                dense_remove_stopwords=get_retrieval_param(
                    "tokenization.dense.remove_stopwords", False
                ),
                dense_lowercase=get_retrieval_param(
                    "tokenization.dense.lowercase", True
                ),
                dense_use_entity_recognition=get_retrieval_param(
                    "tokenization.dense.use_entity_recognition", False
                ),
                sparse_remove_stopwords=get_retrieval_param(
                    "tokenization.sparse.remove_stopwords", True
                ),
                sparse_lowercase=get_retrieval_param(
                    "tokenization.sparse.lowercase", True
                ),
                sparse_use_entity_recognition=get_retrieval_param(
                    "tokenization.sparse.use_entity_recognition", True
                ),
                cache_enabled=get_retrieval_param(
                    "tokenization.cache.enabled", True
                ),
                cache_max_size=get_retrieval_param(
                    "tokenization.cache.max_size", 10000
                ),
                cache_ttl=get_retrieval_param("tokenization.cache.ttl", 0),
                log_timing=get_retrieval_param(
                    "tokenization.performance.log_timing", False
                ),
                slow_threshold_ms=get_retrieval_param(
                    "tokenization.performance.slow_threshold_ms", 50.0
                ),
            )
        except Exception as e:
            logger.warning("加载分词配置失败，使用默认配置", error=str(e))
            return TokenizerConfig()
    
    def _initialize(self) -> None:
        """初始化分词器"""
        if self._initialized:
            return
        
        import jieba
        import jieba.posseg as posseg
        
        self._jieba = jieba
        self._jieba_posseg = posseg
        
        # 1. 加载词典
        self._load_dictionaries()
        
        # 2. 编译SQL标识符模式
        if self.config.protect_sql_identifiers:
            for pattern in self.config.sql_identifier_patterns:
                try:
                    self._sql_patterns.append(re.compile(pattern))
                except re.error as e:
                    logger.warning("SQL标识符模式编译失败", pattern=pattern, error=str(e))
        
        # 3. 加载停用词
        self._load_stopwords()
        
        # 4. 初始化缓存
        if self.config.cache_enabled:
            self._cache = LRUCache(max_size=self.config.cache_max_size)
        
        # 5. 预热
        if self.config.preload_on_startup:
            self._warmup()
        
        self._initialized = True
        logger.info(
            "分词器初始化完成",
            cache_enabled=self.config.cache_enabled,
            cache_max_size=self.config.cache_max_size,
            preloaded=self.config.preload_on_startup,
        )
    
    def _load_dictionaries(self) -> None:
        """加载所有词典"""
        # 主词典
        dict_path = self.config.domain_dict_path
        if dict_path and not os.path.isabs(dict_path):
            # 转换为绝对路径
            project_root = os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            dict_path = os.path.join(project_root, dict_path)
        
        if dict_path and os.path.exists(dict_path):
            self._jieba.load_userdict(dict_path)
            logger.info("加载领域词典", path=dict_path)
        
        # 额外词典
        for extra_path in self.config.extra_dict_paths:
            if extra_path and os.path.exists(extra_path):
                self._jieba.load_userdict(extra_path)
                logger.info("加载额外词典", path=extra_path)
    
    def _load_stopwords(self) -> None:
        """加载停用词"""
        try:
            from server.utils.text_templates import get_stopwords
            self._stopwords = set(get_stopwords())
        except Exception:
            # 默认停用词
            self._stopwords = {
                "的", "了", "是", "在", "有", "和", "与", "等", "或", "及",
                "但", "如", "就", "都", "而", "及", "着", "之", "于", "把",
                "被", "比", "这", "那", "这个", "那个", "什么", "怎么",
            }
    
    def _warmup(self) -> None:
        """预热分词器（触发jieba构建前缀词典）"""
        start = time.time()
        warmup_text = self.config.warmup_text
        _ = list(self._jieba.cut(warmup_text))
        elapsed_ms = (time.time() - start) * 1000
        logger.info("分词器预热完成", warmup_time_ms=f"{elapsed_ms:.1f}")
    
    def cut(
        self,
        text: str,
        use_hmm: Optional[bool] = None,
        cut_all: Optional[bool] = None,
    ) -> List[str]:
        """
        基础分词
        
        Args:
            text: 待分词文本
            use_hmm: 是否使用HMM（None=使用配置值）
            cut_all: 是否使用全模式（None=使用配置值）
        
        Returns:
            分词结果列表
        """
        if not text:
            return []
        
        # 生成缓存key
        hmm = use_hmm if use_hmm is not None else self.config.use_hmm
        all_mode = cut_all if cut_all is not None else self.config.cut_all
        cache_key = f"{text}|{hmm}|{all_mode}"
        
        # 检查缓存
        if self._cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached
        
        # 执行分词
        start = time.time()
        
        if all_mode:
            tokens = list(self._jieba.cut(text, cut_all=True))
        else:
            tokens = list(self._jieba.lcut(text, HMM=hmm))
        
        # 性能统计
        elapsed_ms = (time.time() - start) * 1000
        self._total_calls += 1
        self._total_time_ms += elapsed_ms
        
        if self.config.log_timing and elapsed_ms > self.config.slow_threshold_ms:
            logger.warning(
                "慢分词",
                text_len=len(text),
                elapsed_ms=f"{elapsed_ms:.2f}",
                token_count=len(tokens),
            )
        
        # 写入缓存
        if self._cache:
            self._cache.put(cache_key, tokens)
        
        return tokens
    
    def cut_with_pos(self, text: str) -> List[Tuple[str, str]]:
        """
        带词性标注的分词
        
        Returns:
            [(word, pos), ...] 列表
        """
        if not text:
            return []
        
        return [(w.word, w.flag) for w in self._jieba_posseg.cut(text)]
    
    def cut_for_search(self, text: str) -> List[str]:
        """
        搜索引擎模式分词（更细粒度）
        
        适用于构建倒排索引
        """
        if not text:
            return []
        return list(self._jieba.cut_for_search(text))
    
    def tokenize_for_dense(self, text: str) -> List[str]:
        """
        Dense向量专用分词
        
        特点：
        - 保留停用词（保持语义完整性）
        - 小写化
        """
        if not text:
            return []
        
        tokens = self.cut(text)
        
        result = []
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            if self.config.dense_lowercase:
                token = token.lower()
            result.append(token)
        
        return result
    
    def tokenize_for_sparse(self, text: str) -> List[str]:
        """
        Sparse向量/BM25专用分词
        
        特点：
        - 剔除停用词
        - 小写化
        """
        if not text:
            return []
        
        tokens = self.cut(text)
        
        result = []
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            if self.config.sparse_lowercase:
                token = token.lower()
            if self.config.sparse_remove_stopwords and token in self._stopwords:
                continue
            result.append(token)
        
        return result
    
    def add_word(self, word: str, freq: Optional[int] = None, tag: Optional[str] = None) -> None:
        """动态添加词到词典"""
        self._jieba.add_word(word, freq=freq, tag=tag)
        # 清除缓存（词典变化后缓存失效）
        if self._cache:
            self._cache.clear()
    
    def del_word(self, word: str) -> None:
        """从词典删除词"""
        self._jieba.del_word(word)
        if self._cache:
            self._cache.clear()
    
    def suggest_freq(self, segment: str, tune: bool = True) -> int:
        """调整词频以影响分词结果"""
        return self._jieba.suggest_freq(segment, tune=tune)
    
    def reload_dictionaries(self) -> None:
        """重新加载词典（热更新）"""
        self._jieba.initialize()
        self._load_dictionaries()
        if self._cache:
            self._cache.clear()
        logger.info("词典已重新加载")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取分词器统计信息"""
        avg_time = self._total_time_ms / self._total_calls if self._total_calls > 0 else 0
        stats = {
            "total_calls": self._total_calls,
            "total_time_ms": f"{self._total_time_ms:.2f}",
            "avg_time_ms": f"{avg_time:.3f}",
        }
        if self._cache:
            stats["cache"] = self._cache.stats()
        return stats
    
    def clear_cache(self) -> None:
        """清除缓存"""
        if self._cache:
            self._cache.clear()


# 便捷函数
def get_tokenizer() -> Tokenizer:
    """获取全局分词器实例"""
    return Tokenizer.get_instance()


def tokenize(text: str) -> List[str]:
    """快捷分词函数"""
    return get_tokenizer().cut(text)


def tokenize_for_dense(text: str) -> List[str]:
    """快捷Dense分词"""
    return get_tokenizer().tokenize_for_dense(text)


def tokenize_for_sparse(text: str) -> List[str]:
    """快捷Sparse分词"""
    return get_tokenizer().tokenize_for_sparse(text)


__all__ = [
    "Tokenizer",
    "TokenizerConfig",
    "get_tokenizer",
    "tokenize",
    "tokenize_for_dense",
    "tokenize_for_sparse",
]
