"""
同义词解析器
在SQL生成时将同义词和非标准值替换为标准值
"""

from typing import Any, Optional, Dict, List
import structlog

from server.models.semantic import SemanticModel

logger = structlog.get_logger()


class SynonymResolver:
    """
    同义词解析器
    
    在SQL编译阶段，将过滤条件中的同义词和非标准值替换为标准值
    """
    
    def __init__(self, semantic_model: SemanticModel):
        """
        初始化同义词解析器
        
        Args:
            semantic_model: 语义模型
        """
        self.model = semantic_model
        
        # 构建同义词映射表
        self.enum_synonym_map = self._build_synonym_maps()
        
        logger.debug("同义词解析器初始化完成",
                    fields_with_enums=len(self.enum_synonym_map))
    
    def _build_synonym_maps(self) -> Dict[str, Dict[str, str]]:
        """
        构建同义词到标准值的映射表
        
        Returns:
            {field_id: {synonym: standard_value, ...}}
        """
        synonym_map = {}
        
        if not hasattr(self.model, 'field_enums'):
            logger.warning("语义模型中没有field_enums，跳过同义词映射构建")
            return synonym_map
        
        for field_id, enum_values in self.model.field_enums.items():
            field_map = {}
            
            for enum_val in enum_values:
                standard = enum_val.standard_value
                
                # 标准值映射到自己
                field_map[standard] = standard
                field_map[standard.lower()] = standard  # 小写版本
                field_map[standard.upper()] = standard  # 大写版本
                
                # 同义词映射到标准值
                if hasattr(enum_val, 'synonyms') and enum_val.synonyms:
                    for syn_obj in enum_val.synonyms:
                        syn_text = syn_obj.synonym_text
                        field_map[syn_text] = standard
                        field_map[syn_text.lower()] = standard
                        field_map[syn_text.upper()] = standard
                        
                        # 处理常见变体（去除空格、年等后缀）
                        normalized = syn_text.strip().rstrip('年').rstrip('月').rstrip('日')
                        if normalized != syn_text:
                            field_map[normalized] = standard
            
            if field_map:
                synonym_map[field_id] = field_map
                logger.debug(f"字段{field_id}的同义词映射: {len(field_map)}个")
        
        return synonym_map
    
    def resolve_filter_value(
        self,
        field_id: str,
        user_value: Any
    ) -> Any:
        """
        解析过滤值中的同义词
        
        Args:
            field_id: 字段ID
            user_value: 用户输入的值（可能是同义词）
            
        Returns:
            标准值
        """
        # 如果该字段没有枚举值，直接返回原值
        if field_id not in self.enum_synonym_map:
            return user_value
        
        field_map = self.enum_synonym_map[field_id]
        
        # 处理单值
        if isinstance(user_value, str):
            resolved = self._resolve_single_value(user_value, field_map)
            if resolved != user_value:
                logger.debug(f"同义词解析: {user_value} → {resolved}",
                           field_id=field_id)
            return resolved
        
        # 处理列表值（IN查询）
        if isinstance(user_value, list):
            resolved_list = [
                self._resolve_single_value(v, field_map)
                for v in user_value
            ]
            return resolved_list
        
        # 其他类型直接返回
        return user_value
    
    def _resolve_single_value(
        self,
        value: str,
        field_map: Dict[str, str]
    ) -> str:
        """
        解析单个值
        
        Args:
            value: 用户输入的值
            field_map: 字段的同义词映射表
            
        Returns:
            标准值
        """
        # 1. 精确匹配
        if value in field_map:
            return field_map[value]
        
        # 2. 去除空格后匹配
        stripped = value.strip()
        if stripped in field_map:
            return field_map[stripped]
        
        # 3. 去除常见后缀后匹配（年、月、日、区、市等）
        normalized = stripped.rstrip('年').rstrip('月').rstrip('日').rstrip('区').rstrip('市')
        if normalized in field_map:
            return field_map[normalized]
        
        # 4. 大小写不敏感匹配
        lower_value = value.lower()
        if lower_value in field_map:
            return field_map[lower_value]
        
        # 5. 模糊匹配（包含关系）
        for syn, standard in field_map.items():
            if value in syn or syn in value:
                logger.debug(f"模糊匹配: {value} ≈ {syn} → {standard}")
                return standard
        
        # 未找到匹配，返回原值
        logger.debug(f"未找到匹配的标准值: {value}")
        return value
    
    def resolve_all_filters(
        self,
        filters: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        批量解析过滤条件
        
        Args:
            filters: 过滤条件列表
            
        Returns:
            解析后的过滤条件列表
        """
        resolved_filters = []
        
        for filter_cond in filters:
            field_id = filter_cond.get('field')
            value = filter_cond.get('value')
            
            if field_id and value is not None:
                # 解析同义词
                resolved_value = self.resolve_filter_value(field_id, value)
                
                # 创建新的过滤条件
                resolved_filter = filter_cond.copy()
                resolved_filter['value'] = resolved_value
                resolved_filters.append(resolved_filter)
            else:
                # 保持原样
                resolved_filters.append(filter_cond)
        
        return resolved_filters
    
    def get_standard_values(
        self,
        field_id: str
    ) -> Optional[List[str]]:
        """
        获取字段的所有标准值列表
        
        Args:
            field_id: 字段ID
            
        Returns:
            标准值列表，如果字段没有枚举值则返回None
        """
        if not hasattr(self.model, 'field_enums'):
            return None
        
        if field_id not in self.model.field_enums:
            return None
        
        enum_values = self.model.field_enums[field_id]
        return [ev.standard_value for ev in enum_values]
    
    def get_synonyms_for_value(
        self,
        field_id: str,
        standard_value: str
    ) -> List[str]:
        """
        获取标准值的所有同义词
        
        Args:
            field_id: 字段ID
            standard_value: 标准值
            
        Returns:
            同义词列表
        """
        if not hasattr(self.model, 'field_enums'):
            return []
        
        if field_id not in self.model.field_enums:
            return []
        
        # 找到对应的枚举值对象
        enum_values = self.model.field_enums[field_id]
        for enum_val in enum_values:
            if enum_val.standard_value == standard_value:
                if hasattr(enum_val, 'synonyms') and enum_val.synonyms:
                    return [syn.synonym_text for syn in enum_val.synonyms]
                break
        
        return []

