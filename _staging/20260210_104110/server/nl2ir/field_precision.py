"""
字段精度动态推断模块

根据字段名、字段类型、枚举值基数等多维度信息，
动态推断字段的匹配精度要求，用于优化枚举值匹配准确度。
"""

import os
import re
from typing import Dict, Any, Optional
import yaml
import structlog

logger = structlog.get_logger()

# 全局配置缓存
_field_precision_config: Optional[Dict[str, Any]] = None


def _load_field_precision_config() -> Dict[str, Any]:
    """加载字段精度配置"""
    global _field_precision_config
    
    if _field_precision_config is not None:
        return _field_precision_config
    
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "config",
        "enum_field_precision.yaml"
    )
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            _field_precision_config = yaml.safe_load(f)
        logger.info("字段精度配置加载成功", path=config_path)
    except FileNotFoundError:
        logger.warning("字段精度配置文件不存在，使用默认配置", path=config_path)
        _field_precision_config = _get_default_config()
    except Exception as e:
        logger.error("字段精度配置加载失败", error=str(e), path=config_path)
        _field_precision_config = _get_default_config()
    
    return _field_precision_config


def get_enum_boost_config() -> Dict[str, Any]:
    """
    获取枚举加成配置
    
    从 config/retrieval_config.yaml 的 table_scoring.enum_boost 节点读取配置
    （配置已从 enum_field_precision.yaml 迁移）
    
    Returns:
        包含以下键的字典：
        - context_threshold: 门控阈值
        - exact_boost: exact/synonym 匹配加成
        - vector_boost: value_vector 匹配加成
        - max_boost: 单表枚举加成上限
    """
    # 从 retrieval_config.yaml 读取配置
    from server.config import get_retrieval_param
    
    return {
        "context_threshold": get_retrieval_param("table_scoring.enum_boost.context_threshold", 0.2),
        "exact_boost": get_retrieval_param("table_scoring.enum_boost.exact_boost", 0.02),
        "vector_boost": get_retrieval_param("table_scoring.enum_boost.vector_boost", 0.01),
        "max_boost": get_retrieval_param("table_scoring.enum_boost.max_boost", 0.03),
    }


def _get_default_config() -> Dict[str, Any]:
    """获取默认配置（P2增强版）"""
    return {
        "field_precision_rules": {
            "name_patterns": {
                # P2增强：更完整的高精度字段匹配规则
                "high_precision": [
                    ".*行政区.*", ".*街道.*", ".*乡镇.*", ".*社区.*", ".*村.*",
                    ".*征收单位.*", ".*用地单位.*", ".*建设单位.*", ".*供地单位.*",
                    ".*受让人.*", ".*竞得人.*",
                    ".*年份.*", ".*季度.*", ".*月份.*",
                    ".*批次.*", ".*编号.*", ".*证号.*", ".*文号.*",
                ],
                "medium_precision": [
                    ".*用途.*", ".*地类.*", ".*类型.*", ".*性质.*",
                    ".*状态.*", ".*方式.*", ".*来源.*", ".*级别.*", ".*分类.*",
                ],
                "low_precision": [
                    ".*备注.*", ".*说明.*", ".*描述.*", ".*摘要.*",
                    ".*内容.*", ".*详情.*", ".*项目名称.*", ".*批次名.*",
                ]
            },
            "type_based": {
                "identifier": "high_precision",
                "dimension": "medium_precision",
                "measure": "low_precision"
            },
            "cardinality_based": {
                "low_cardinality": {"precision": "high_precision", "reason": "枚举值少，语义明确"},
                "medium_cardinality": {"precision": "medium_precision", "reason": "枚举值适中"},
                "high_cardinality": {"precision": "low_precision", "reason": "枚举值多，可能是描述性文本"},
                "thresholds": {"low": 30, "high": 100}
            },
            "default": "medium_precision"
        },
        "precision_levels": {
            "high_precision": {"threshold": 0.85, "boost": 0.02, "description": "高精度字段"},
            "medium_precision": {"threshold": 0.70, "boost": 0.02, "description": "中精度字段"},
            "low_precision": {"threshold": 0.60, "boost": 0.015, "description": "低精度字段"}
        },
        # P2新增：快速匹配关键词（不需要正则）
        "quick_match_keywords": {
            "high_precision": ["行政区", "街道", "乡镇", "年份", "季度", "月份", "编号", "证号"],
            "low_precision": ["备注", "说明", "描述", "项目名称", "批次名", "坐落", "地址"],
        },
        "debug": {"log_precision_inference": False}
    }


class FieldPrecisionInferencer:
    """字段精度推断器（P2增强版）"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化字段精度推断器
        
        Args:
            config: 可选的配置字典，如果不提供则从文件加载
        """
        self.config = config or _load_field_precision_config()
        self.rules = self.config.get('field_precision_rules', {})
        self.precision_levels = self.config.get('precision_levels', {})
        self.debug_enabled = self.config.get('debug', {}).get('log_precision_inference', False)
        
        # P2新增：快速匹配关键词缓存
        self.quick_match_keywords = self.config.get('quick_match_keywords', {})
        
        # P2新增：缓存已推断的字段精度（提升性能）
        self._inference_cache: Dict[str, Dict[str, Any]] = {}
    
    def infer_precision(
        self,
        field_name: str,
        field_type: Optional[str] = None,
        enum_count: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        动态推断字段精度等级（P2增强版）
        
        优先级：
        0. 快速关键词匹配（P2新增，最高优先级）
        1. 字段名模式匹配
        2. 字段类型
        3. 枚举值基数
        4. 默认值
        
        Args:
            field_name: 字段显示名称
            field_type: 字段类型（identifier/dimension/measure）
            enum_count: 枚举值数量
            
        Returns:
            {
                'precision_level': 'high_precision' | 'medium_precision' | 'low_precision',
                'threshold': 0.85 | 0.70 | 0.60,
                'boost': 0.02 | 0.015,
                'matched_rule': '规则描述',
                'rule_type': 'quick_match' | 'name_pattern' | 'field_type' | 'cardinality' | 'default'
            }
        """
        # P2新增：缓存检查
        cache_key = f"{field_name}|{field_type}|{enum_count}"
        if cache_key in self._inference_cache:
            return self._inference_cache[cache_key]
        
        precision_level = None
        matched_rule = None
        rule_type = None
        
        # P2新增：规则0 - 快速关键词匹配（不需要正则，性能最优）
        precision_level, matched_rule = self._quick_keyword_match(field_name)
        if precision_level:
            rule_type = 'quick_match'
        
        # 规则1：字段名模式匹配
        if not precision_level:
            precision_level, matched_rule = self._match_by_name_pattern(field_name)
            if precision_level:
                rule_type = 'name_pattern'
        
        # 规则2：字段类型
        if not precision_level and field_type:
            precision_level, matched_rule = self._match_by_field_type(field_type)
            if precision_level:
                rule_type = 'field_type'
        
        # 规则3：枚举值基数
        if not precision_level and enum_count is not None:
            precision_level, matched_rule = self._match_by_cardinality(enum_count)
            if precision_level:
                rule_type = 'cardinality'
        
        # 规则4：默认值
        if not precision_level:
            precision_level = self.rules.get('default', 'medium_precision')
            matched_rule = "使用默认精度"
            rule_type = 'default'
        
        # 获取精度等级对应的参数
        level_config = self.precision_levels.get(precision_level, {})
        threshold = level_config.get('threshold', 0.70)
        boost = level_config.get('boost', 0.02)
        description = level_config.get('description', '')
        
        result = {
            'precision_level': precision_level,
            'threshold': threshold,
            'boost': boost,
            'matched_rule': matched_rule,
            'rule_type': rule_type,
            'description': description
        }
        
        # 缓存结果
        self._inference_cache[cache_key] = result
        
        # 调试日志
        if self.debug_enabled:
            logger.info(
                "字段精度推断（P2增强）",
                field_name=field_name,
                field_type=field_type,
                enum_count=enum_count,
                precision_level=precision_level,
                threshold=threshold,
                matched_rule=matched_rule,
                rule_type=rule_type
            )
        
        return result
    
    def _quick_keyword_match(self, field_name: str) -> tuple:
        """
        P2新增：快速关键词匹配（不使用正则，性能优先）
        
        Returns:
            (precision_level, matched_keyword) 或 (None, None)
        """
        if not field_name or not self.quick_match_keywords:
            return (None, None)
        
        field_name_lower = field_name.lower()
        
        # 先检查高精度关键词
        for kw in self.quick_match_keywords.get('high_precision', []):
            if kw in field_name_lower:
                return ('high_precision', f"快速匹配: 包含'{kw}'")
        
        # 再检查低精度关键词
        for kw in self.quick_match_keywords.get('low_precision', []):
            if kw in field_name_lower:
                return ('low_precision', f"快速匹配: 包含'{kw}'")
        
        return (None, None)
    
    def _match_by_name_pattern(self, field_name: str) -> tuple:
        """
        根据字段名模式匹配精度
        
        Returns:
            (precision_level, matched_pattern) 或 (None, None)
        """
        name_patterns = self.rules.get('name_patterns', {})
        
        for precision_level in ['high_precision', 'medium_precision', 'low_precision']:
            patterns = name_patterns.get(precision_level, [])
            for pattern in patterns:
                try:
                    if re.match(pattern, field_name, re.IGNORECASE):
                        return (precision_level, f"字段名匹配模式: {pattern}")
                except re.error as e:
                    logger.warning(f"正则表达式错误: {pattern}", error=str(e))
                    continue
        
        return (None, None)
    
    def _match_by_field_type(self, field_type: str) -> tuple:
        """
        根据字段类型匹配精度
        
        Returns:
            (precision_level, rule_description) 或 (None, None)
        """
        type_based = self.rules.get('type_based', {})
        precision_level = type_based.get(field_type)
        
        if precision_level:
            return (precision_level, f"字段类型: {field_type}")
        
        return (None, None)
    
    def _match_by_cardinality(self, enum_count: int) -> tuple:
        """
        根据枚举值基数匹配精度
        
        Returns:
            (precision_level, rule_description) 或 (None, None)
        """
        cardinality_based = self.rules.get('cardinality_based', {})
        thresholds = cardinality_based.get('thresholds', {})
        low_threshold = thresholds.get('low', 30)
        high_threshold = thresholds.get('high', 100)
        
        if enum_count < low_threshold:
            config = cardinality_based.get('low_cardinality', {})
            precision_level = config.get('precision')
            reason = config.get('reason', '')
            return (precision_level, f"低基数 ({enum_count}<{low_threshold}): {reason}")
        
        elif enum_count < high_threshold:
            config = cardinality_based.get('medium_cardinality', {})
            precision_level = config.get('precision')
            reason = config.get('reason', '')
            return (precision_level, f"中基数 ({low_threshold}≤{enum_count}<{high_threshold}): {reason}")
        
        else:
            config = cardinality_based.get('high_cardinality', {})
            precision_level = config.get('precision')
            reason = config.get('reason', '')
            return (precision_level, f"高基数 ({enum_count}≥{high_threshold}): {reason}")
    
    def get_keyword_extraction_config(self, field_name: str) -> Optional[Dict[str, Any]]:
        """
        获取字段的关键词提取配置
        
        Args:
            field_name: 字段显示名称
            
        Returns:
            关键词提取配置，如果字段不需要特殊处理则返回None
        """
        keyword_config = self.config.get('keyword_extraction', {})
        
        # 检查地理字段
        geo_fields = keyword_config.get('geographic_fields', {})
        geo_patterns = geo_fields.get('patterns', [])
        for pattern in geo_patterns:
            if re.match(pattern, field_name, re.IGNORECASE):
                return {
                    'field_type': 'geographic',
                    'extraction_rules': geo_fields.get('extraction_rules', [])
                }
        
        # 检查机构字段
        org_fields = keyword_config.get('organization_fields', {})
        org_patterns = org_fields.get('patterns', [])
        for pattern in org_patterns:
            if re.match(pattern, field_name, re.IGNORECASE):
                return {
                    'field_type': 'organization',
                    'extraction_rules': org_fields.get('extraction_rules', [])
                }
        
        # 检查时间字段
        temporal_fields = keyword_config.get('temporal_fields', {})
        temporal_patterns = temporal_fields.get('patterns', [])
        for pattern in temporal_patterns:
            if re.match(pattern, field_name, re.IGNORECASE):
                return {
                    'field_type': 'temporal',
                    'extraction_rules': temporal_fields.get('extraction_rules', [])
                }
        
        return None


# 全局单例
_global_inferencer: Optional[FieldPrecisionInferencer] = None


def get_field_precision_inferencer() -> FieldPrecisionInferencer:
    """获取全局字段精度推断器单例"""
    global _global_inferencer
    
    if _global_inferencer is None:
        _global_inferencer = FieldPrecisionInferencer()
    
    return _global_inferencer

