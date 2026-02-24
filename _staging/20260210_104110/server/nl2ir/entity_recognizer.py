"""
实体预识别器（P1优化）

在分词前识别业务实体（业务域、表名、字段名、枚举值），
使用占位符保护它们不被切分，分词后恢复为标准名称。

核心特性：
1. 长词优先匹配（避免"建设用地批复"被切成"建设用地"+"批复"）
2. 实体归一化（映射到canonical_name或display_name）
3. 占位符保护机制（兼容jieba分词）
4. 从PostgreSQL/SemanticModel动态加载实体词典
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any
import structlog

from server.models.semantic import SemanticModel

logger = structlog.get_logger()


@dataclass
class RecognizedEntity:
    """识别到的实体"""
    entity_type: str  # domain, table, field, enum
    entity_id: str  # 实体的唯一ID
    original_text: str  # 原始匹配文本
    canonical_name: str  # 标准名称
    start_pos: int  # 起始位置
    end_pos: int  # 结束位置
    confidence: float = 1.0  # 置信度（精确匹配=1.0，同义词=0.9）
    match_type: str = "exact"  # exact, synonym, partial
    
    @property
    def placeholder(self) -> str:
        """生成占位符"""
        return f"__{self.entity_type.upper()}_{self.entity_id[:8]}__"


@dataclass
class EntityRecognitionResult:
    """实体识别结果"""
    recognized_entities: List[RecognizedEntity] = field(default_factory=list)
    protected_text: str = ""  # 用占位符替换后的文本
    placeholder_map: Dict[str, RecognizedEntity] = field(default_factory=dict)  # 占位符 -> 实体
    trace: Dict[str, Any] = field(default_factory=dict)
    
    def restore_tokens(self, tokens: List[str]) -> List[str]:
        """将分词结果中的占位符恢复为标准名称"""
        result = []
        for token in tokens:
            if token in self.placeholder_map:
                entity = self.placeholder_map[token]
                result.append(entity.canonical_name)
            else:
                result.append(token)
        return result
    
    def get_entities_by_type(self, entity_type: str) -> List[RecognizedEntity]:
        """按类型获取实体"""
        return [e for e in self.recognized_entities if e.entity_type == entity_type]


class EntityRecognizer:
    """
    实体预识别器
    
    使用AC自动机或倒排索引实现高效的多模式匹配，
    支持长词优先策略。
    """
    
    def __init__(self, semantic_model: Optional[SemanticModel] = None):
        self.semantic_model = semantic_model
        
        # 实体词典：{normalized_text: [(entity_type, entity_id, canonical_name, match_type)]}
        self.entity_dict: Dict[str, List[Tuple[str, str, str, str]]] = {}
        
        # 按长度分组的pattern列表（用于长词优先匹配）
        self.patterns_by_length: Dict[int, Set[str]] = {}
        
        # 最大pattern长度（用于优化匹配）
        self.max_pattern_length: int = 0
        
        if semantic_model:
            self._build_entity_dict()
    
    def update_model(self, semantic_model: SemanticModel) -> None:
        """更新语义模型并重建词典"""
        self.semantic_model = semantic_model
        self.entity_dict.clear()
        self.patterns_by_length.clear()
        self.max_pattern_length = 0
        if semantic_model:
            self._build_entity_dict()
    
    def recognize(self, text: str) -> EntityRecognitionResult:
        """
        识别文本中的业务实体
        
        使用长词优先策略：
        1. 按pattern长度降序尝试匹配
        2. 已匹配位置不再重复匹配（贪婪策略）
        3. 用占位符替换匹配到的实体
        
        Args:
            text: 输入文本
            
        Returns:
            EntityRecognitionResult: 识别结果
        """
        if not text or not self.entity_dict:
            return EntityRecognitionResult(protected_text=text)
        
        recognized: List[RecognizedEntity] = []
        placeholder_map: Dict[str, RecognizedEntity] = {}
        
        # 记录已匹配的位置（避免重叠）
        matched_positions: Set[int] = set()
        
        # 按长度降序遍历patterns（长词优先）
        text_lower = text.lower()
        text_len = len(text)
        
        for length in sorted(self.patterns_by_length.keys(), reverse=True):
            patterns = self.patterns_by_length[length]
            
            for pattern in patterns:
                # 查找所有匹配位置
                start = 0
                while start <= text_len - length:
                    pos = text_lower.find(pattern, start)
                    if pos == -1:
                        break
                    
                    end_pos = pos + length
                    
                    # 检查是否与已匹配位置重叠
                    overlaps = any(p in matched_positions for p in range(pos, end_pos))
                    if overlaps:
                        start = pos + 1
                        continue
                    
                    # 检查边界（避免匹配词中间，如"建设用地"不应匹配"新建设用地管理"中间）
                    if not self._is_valid_boundary(text, pos, end_pos):
                        start = pos + 1
                        continue
                    
                    # 获取实体信息
                    entities_info = self.entity_dict.get(pattern, [])
                    if entities_info:
                        # 取第一个匹配（可以根据需要调整优先级策略）
                        entity_type, entity_id, canonical_name, match_type = entities_info[0]
                        
                        original_text = text[pos:end_pos]
                        entity = RecognizedEntity(
                            entity_type=entity_type,
                            entity_id=entity_id,
                            original_text=original_text,
                            canonical_name=canonical_name,
                            start_pos=pos,
                            end_pos=end_pos,
                            confidence=1.0 if match_type == "exact" else 0.9,
                            match_type=match_type,
                        )
                        
                        recognized.append(entity)
                        placeholder_map[entity.placeholder] = entity
                        
                        # 标记已匹配位置
                        for p in range(pos, end_pos):
                            matched_positions.add(p)
                    
                    start = end_pos
        
        # 按位置排序
        recognized.sort(key=lambda e: e.start_pos)
        
        # 构建替换后的文本
        protected_text = self._build_protected_text(text, recognized)
        
        result = EntityRecognitionResult(
            recognized_entities=recognized,
            protected_text=protected_text,
            placeholder_map=placeholder_map,
            trace={
                "total_recognized": len(recognized),
                "by_type": {
                    "domain": len([e for e in recognized if e.entity_type == "domain"]),
                    "table": len([e for e in recognized if e.entity_type == "table"]),
                    "field": len([e for e in recognized if e.entity_type == "field"]),
                    "enum": len([e for e in recognized if e.entity_type == "enum"]),
                },
            },
        )
        
        if recognized:
            logger.debug(
                "实体识别完成",
                recognized_count=len(recognized),
                types={k: v for k, v in result.trace["by_type"].items() if v > 0},
            )
        
        return result
    
    def _build_entity_dict(self) -> None:
        """从SemanticModel构建实体词典"""
        if not self.semantic_model:
            return
        
        # 1. 业务域
        for domain_id, domain in (self.semantic_model.domains or {}).items():
            domain_name = getattr(domain, "domain_name", None)
            if domain_name:
                self._add_pattern(domain_name, "domain", domain_id, domain_name, "exact")
            
            # 添加域关键词
            keywords = getattr(domain, "keywords", []) or []
            for kw in keywords:
                if kw and len(kw) >= 2:
                    self._add_pattern(kw, "domain", domain_id, domain_name, "synonym")
        
        # 2. 表
        for table_id, table in (self.semantic_model.datasources or {}).items():
            display_name = getattr(table, "display_name", None)
            if display_name:
                self._add_pattern(display_name, "table", table_id, display_name, "exact")
            
            # 添加表标签
            tags = getattr(table, "tags", []) or []
            for tag in tags:
                if tag and len(tag) >= 2:
                    self._add_pattern(tag, "table", table_id, display_name, "synonym")
        
        # 3. 字段
        for field_id, field_obj in (self.semantic_model.fields or {}).items():
            display_name = getattr(field_obj, "display_name", None)
            if display_name and len(display_name) >= 2:
                self._add_pattern(display_name, "field", field_id, display_name, "exact")
            
            # 添加字段同义词
            synonyms = getattr(field_obj, "synonyms", []) or []
            for syn in synonyms:
                if syn and len(syn) >= 2:
                    self._add_pattern(syn, "field", field_id, display_name, "synonym")
        
        # 4. 枚举值
        for field_id, enums in (self.semantic_model.field_enums or {}).items():
            for enum in enums or []:
                standard_value = getattr(enum, "standard_value", None)
                display_name = getattr(enum, "display_name", None) or standard_value
                enum_id = getattr(enum, "enum_id", None) or f"{field_id}_{standard_value}"
                
                if standard_value and len(standard_value) >= 2:
                    self._add_pattern(standard_value, "enum", enum_id, display_name, "exact")
                
                if display_name and display_name != standard_value and len(display_name) >= 2:
                    self._add_pattern(display_name, "enum", enum_id, display_name, "exact")
                
                # 枚举同义词
                for syn in getattr(enum, "synonyms", []) or []:
                    syn_text = getattr(syn, "synonym_text", None)
                    if syn_text and len(syn_text) >= 2:
                        self._add_pattern(syn_text, "enum", enum_id, display_name, "synonym")
        
        logger.info(
            "实体词典构建完成",
            pattern_count=len(self.entity_dict),
            max_length=self.max_pattern_length,
            length_distribution={k: len(v) for k, v in sorted(self.patterns_by_length.items(), reverse=True)[:5]},
        )
    
    def _add_pattern(
        self,
        text: str,
        entity_type: str,
        entity_id: str,
        canonical_name: str,
        match_type: str,
    ) -> None:
        """添加一个pattern到词典"""
        if not text:
            return
        
        normalized = text.lower().strip()
        if not normalized or len(normalized) < 2:
            return
        
        # 添加到词典
        if normalized not in self.entity_dict:
            self.entity_dict[normalized] = []
        
        # 检查是否已存在相同实体（避免重复）
        exists = any(
            e[0] == entity_type and e[1] == entity_id
            for e in self.entity_dict[normalized]
        )
        if not exists:
            self.entity_dict[normalized].append(
                (entity_type, entity_id, canonical_name, match_type)
            )
        
        # 按长度分组
        length = len(normalized)
        if length not in self.patterns_by_length:
            self.patterns_by_length[length] = set()
        self.patterns_by_length[length].add(normalized)
        
        # 更新最大长度
        if length > self.max_pattern_length:
            self.max_pattern_length = length
    
    def _is_valid_boundary(self, text: str, start: int, end: int) -> bool:
        """
        检查匹配边界是否有效
        
        规则：
        - 中文字符之间不需要边界检查
        - 英文/数字需要检查单词边界
        """
        # 检查左边界
        if start > 0:
            left_char = text[start - 1]
            first_char = text[start]
            
            # 如果左边是中文，当前也是中文，则需要额外判断
            # 这里简化处理：允许所有中文边界
            if not self._is_cjk(left_char) and self._is_word_char(left_char):
                if self._is_word_char(first_char):
                    return False
        
        # 检查右边界
        if end < len(text):
            right_char = text[end]
            last_char = text[end - 1]
            
            if not self._is_cjk(right_char) and self._is_word_char(right_char):
                if self._is_word_char(last_char):
                    return False
        
        return True
    
    @staticmethod
    def _is_cjk(char: str) -> bool:
        """判断是否是CJK字符"""
        return '\u4e00' <= char <= '\u9fff'
    
    @staticmethod
    def _is_word_char(char: str) -> bool:
        """判断是否是单词字符（字母、数字、下划线）"""
        return char.isalnum() or char == '_'
    
    def _build_protected_text(
        self,
        text: str,
        entities: List[RecognizedEntity],
    ) -> str:
        """用占位符替换实体"""
        if not entities:
            return text
        
        result = []
        last_end = 0
        
        for entity in entities:
            # 添加实体之前的文本
            if entity.start_pos > last_end:
                result.append(text[last_end:entity.start_pos])
            
            # 添加占位符
            result.append(entity.placeholder)
            last_end = entity.end_pos
        
        # 添加最后一段文本
        if last_end < len(text):
            result.append(text[last_end:])
        
        return "".join(result)


__all__ = ["EntityRecognizer", "EntityRecognitionResult", "RecognizedEntity"]
