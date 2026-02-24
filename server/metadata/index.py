"""关键词索引 - 用于检索字段和指标"""

from typing import Dict, List, Set, Tuple, Union
from collections import defaultdict
import re
import structlog

from server.models.semantic import SemanticModel, Metric, Field
# 导入兼容性类型
from server.models.semantic import Dimension, Measure

logger = structlog.get_logger()


class KeywordIndex:
    """关键词索引，支持倒排索引和模糊匹配"""
    
    def __init__(self, semantic_model: SemanticModel):
        self.model = semantic_model
        
        # 倒排索引：keyword -> List[(type, id, score)]
        self._metric_index: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        self._field_index: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        
        # 向后兼容：维度和度量的独立索引
        self._dimension_index: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        self._measure_index: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        
        self._build_index()
    
    def _build_index(self) -> None:
        """构建倒排索引"""
        logger.info("开始构建关键词索引")
        
        # 索引指标
        for metric_id, metric in self.model.metrics.items():
            # ID 本身（最高权重）
            self._add_to_index(self._metric_index, metric_id.lower(), metric_id, 1.0)
            
            # Display name（高权重）
            for token in self._tokenize(metric.display_name):
                self._add_to_index(self._metric_index, token, metric_id, 0.9)
            
            # Synonyms（高权重）
            for synonym in metric.synonyms:
                for token in self._tokenize(synonym):
                    self._add_to_index(self._metric_index, token, metric_id, 0.95)
            
            # Description（中权重）
            if metric.description:
                for token in self._tokenize(metric.description):
                    self._add_to_index(self._metric_index, token, metric_id, 0.5)
        
        # 索引字段（统一索引）
        for field_id, field in self.model.fields.items():
            # ID 本身
            self._add_to_index(self._field_index, field_id.lower(), field_id, 1.0)
            
            # Display name
            for token in self._tokenize(field.display_name):
                self._add_to_index(self._field_index, token, field_id, 0.9)
            
            # Synonyms（高权重）
            for synonym in field.synonyms:
                for token in self._tokenize(synonym):
                    self._add_to_index(self._field_index, token, field_id, 0.95)
            
            # Description
            if field.description:
                for token in self._tokenize(field.description):
                    self._add_to_index(self._field_index, token, field_id, 0.5)
            
            # 向后兼容：同时添加到维度或度量索引
            if field.field_category == 'dimension':
                self._add_to_index(self._dimension_index, field_id.lower(), field_id, 1.0)
                for token in self._tokenize(field.display_name):
                    self._add_to_index(self._dimension_index, token, field_id, 0.9)
                for synonym in field.synonyms:
                    for token in self._tokenize(synonym):
                        self._add_to_index(self._dimension_index, token, field_id, 0.95)
                if field.description:
                    for token in self._tokenize(field.description):
                        self._add_to_index(self._dimension_index, token, field_id, 0.5)
            
            elif field.field_category == 'measure':
                self._add_to_index(self._measure_index, field_id.lower(), field_id, 1.0)
                for token in self._tokenize(field.display_name):
                    self._add_to_index(self._measure_index, token, field_id, 0.9)
                for synonym in field.synonyms:
                    for token in self._tokenize(synonym):
                        self._add_to_index(self._measure_index, token, field_id, 0.95)
                if field.description:
                    for token in self._tokenize(field.description):
                        self._add_to_index(self._measure_index, token, field_id, 0.5)
        
        logger.info(
            "关键词索引构建完成",
            metric_keywords=len(self._metric_index),
            field_keywords=len(self._field_index),
            dimension_keywords=len(self._dimension_index),
            measure_keywords=len(self._measure_index)
        )
    
    def _add_to_index(
        self,
        index: Dict[str, List[Tuple[str, float]]],
        keyword: str,
        item_id: str,
        score: float
    ) -> None:
        """添加到索引"""
        # 避免重复添加
        for existing_id, existing_score in index[keyword]:
            if existing_id == item_id:
                # 保留更高的分数
                if score > existing_score:
                    index[keyword].remove((existing_id, existing_score))
                    index[keyword].append((item_id, score))
                return
        
        index[keyword].append((item_id, score))
    
    def _tokenize(self, text: str) -> List[str]:
        """
        分词（简单实现）
        
        对于中文，按字符切分；对于英文，按单词切分
        """
        if not text:
            return []
        
        text = text.lower()
        tokens = []
        
        # 提取英文单词
        english_words = re.findall(r'[a-z]+', text)
        tokens.extend(english_words)
        
        # 提取中文字符（2-gram 和单字）
        chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
        if chinese_chars:
            # 单字
            tokens.extend(chinese_chars)
            
            # 2-gram
            for i in range(len(chinese_chars) - 1):
                tokens.append(chinese_chars[i] + chinese_chars[i + 1])
        
        return list(set(tokens))  # 去重
    
    def search_metrics(self, query: str, top_k: int = 10) -> List[Tuple[str, Metric, float]]:
        """
        搜索指标
        
        Args:
            query: 查询文本
            top_k: 返回前 K 个结果
        
        Returns:
            List of (metric_id, Metric, score)
        """
        tokens = self._tokenize(query)
        
        # 累积分数
        scores: Dict[str, float] = defaultdict(float)
        
        for token in tokens:
            # 精确匹配
            if token in self._metric_index:
                for metric_id, score in self._metric_index[token]:
                    scores[metric_id] += score
            
            # 模糊匹配（前缀）
            for keyword, items in self._metric_index.items():
                if keyword.startswith(token) and keyword != token:
                    for metric_id, score in items:
                        scores[metric_id] += score * 0.7  # 降低权重
        
        # 排序
        sorted_results = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        
        return [
            (metric_id, self.model.metrics[metric_id], score)
            for metric_id, score in sorted_results
            if metric_id in self.model.metrics
        ]
    
    def search_fields(
        self,
        query: str,
        top_k: int = 10,
        category: str = None
    ) -> List[Tuple[str, Field, float]]:
        """
        搜索字段（新方法）
        
        Args:
            query: 查询文本
            top_k: 返回前 K 个结果
            category: 字段类别过滤 (dimension, measure, identifier, timestamp)
        
        Returns:
            List of (field_id, Field, score)
        """
        tokens = self._tokenize(query)
        
        scores: Dict[str, float] = defaultdict(float)
        
        for token in tokens:
            # 精确匹配
            if token in self._field_index:
                for field_id, score in self._field_index[token]:
                    scores[field_id] += score
            
            # 模糊匹配（前缀）
            for keyword, items in self._field_index.items():
                if keyword.startswith(token) and keyword != token:
                    for field_id, score in items:
                        scores[field_id] += score * 0.7
        
        # 排序
        sorted_results = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        # 过滤并返回
        results = []
        for field_id, score in sorted_results:
            if field_id not in self.model.fields:
                continue
            
            field = self.model.fields[field_id]
            
            # 类别过滤
            if category and field.field_category != category:
                continue
            
            results.append((field_id, field, score))
            
            if len(results) >= top_k:
                break
        
        return results
    
    def search_dimensions(self, query: str, top_k: int = 10) -> List[Tuple[str, Union[Dimension, Field], float]]:
        """
        搜索维度（向后兼容方法）
        
        Args:
            query: 查询文本
            top_k: 返回前 K 个结果
        
        Returns:
            List of (dim_id, Dimension/Field, score)
        """
        # 使用新的字段搜索方法，过滤维度类型
        return self.search_fields(query, top_k, category='dimension')
    
    def search_measures(self, query: str, top_k: int = 10) -> List[Tuple[str, Union[Measure, Field], float]]:
        """
        搜索度量（向后兼容方法）
        
        Args:
            query: 查询文本
            top_k: 返回前 K 个结果
        
        Returns:
            List of (measure_id, Measure/Field, score)
        """
        # 使用新的字段搜索方法，过滤度量类型
        return self.search_fields(query, top_k, category='measure')
    
    def search_all(self, query: str, top_k: int = 10) -> Dict[str, List]:
        """
        搜索所有类型
        
        Returns:
            {
                "metrics": List[(id, Metric, score)],
                "dimensions": List[(id, Field, score)],
                "measures": List[(id, Field, score)],
                "fields": List[(id, Field, score)]  # 新增：所有字段
            }
        """
        return {
            "metrics": self.search_metrics(query, top_k),
            "dimensions": self.search_dimensions(query, top_k),
            "measures": self.search_measures(query, top_k),
            "fields": self.search_fields(query, top_k)  # 新增
        }
