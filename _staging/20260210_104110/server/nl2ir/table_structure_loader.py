"""
表结构加载器
从语义模型中加载表的完整字段结构
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import structlog

from server.models.semantic import SemanticModel, Field
from server.config import RetrievalConfig

logger = structlog.get_logger()


@dataclass
class TableStructure:
    """表结构（包含所有字段）"""
    table_id: str
    table_name: str
    display_name: str
    description: str
    domain_id: Optional[str] = None
    domain_name: Optional[str] = None
    schema_name: Optional[str] = None
    physical_table_name: Optional[str] = None
    dimensions: List[Field] = field(default_factory=list)   # 维度字段
    measures: List[Field] = field(default_factory=list)     # 度量字段
    identifiers: List[Field] = field(default_factory=list)  # 标识字段
    timestamps: List[Field] = field(default_factory=list)   # 时间戳字段
    geometries: List[Field] = field(default_factory=list)   # 空间字段（几何字段）
    
    tags: List[str] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)
    data_year: Optional[str] = None
    
    @property
    def total_fields(self) -> int:
        """总字段数"""
        return (len(self.dimensions) + len(self.measures) + len(self.identifiers) + 
                len(self.timestamps) + len(self.geometries))


class TableStructureLoader:
    """
    表结构加载器
    
    从语义模型中加载指定表的所有字段
    """
    
    def __init__(self, semantic_model: SemanticModel):
        """
        初始化表结构加载器
        
        Args:
            semantic_model: 语义模型
        """
        self.model = semantic_model
        # 从配置加载字段排序参数
        self._enum_feedback_alpha = RetrievalConfig.field_enum_feedback_alpha()
        self._enum_boost_multiplier = RetrievalConfig.field_enum_boost_multiplier()
    
    def load_table_structure(self, table_id: str) -> TableStructure:
        """
        加载表的完整结构
        
        Args:
            table_id: 表ID
            
        Returns:
            TableStructure对象
            
        Raises:
            ValueError: 如果表不存在
        """
        # 获取表信息
        if table_id not in self.model.datasources:
            raise ValueError(f"表不存在: {table_id}")
        
        datasource = self.model.datasources[table_id]
        
        # 加载该表的所有字段
        fields_by_category = self._load_fields_by_table(table_id)
        
        # 业务域名称
        domain_id = datasource.domain_id
        domain_name = None
        if domain_id and hasattr(self.model, "domains"):
            domain_obj = self.model.domains.get(domain_id)
            if domain_obj:
                domain_name = domain_obj.domain_name
        
        # 标签 & 同义词（含核心短语）
        tags = list(dict.fromkeys(datasource.tags or []))
        alias_candidates: List[str] = []
        identity = getattr(datasource, "identity", None)
        if identity:
            if getattr(identity, "core_phrases", None):
                alias_candidates.extend([
                    phrase.text for phrase in identity.core_phrases
                    if getattr(phrase, "text", None)
                ])
            if getattr(identity, "unique_terms", None):
                alias_candidates.extend(identity.unique_terms)
        alias_candidates.extend(tags)
        aliases: List[str] = []
        seen = set()
        for alias in alias_candidates:
            if alias and alias not in seen:
                seen.add(alias)
                aliases.append(alias)
        
        data_year = getattr(datasource, "data_year", None)
        if data_year is not None:
            data_year = str(data_year)
        
        structure = TableStructure(
            table_id=table_id,
            table_name=datasource.datasource_name,
            display_name=datasource.display_name,
            description=datasource.description or "",
            domain_id=domain_id,
            domain_name=domain_name,
            schema_name=datasource.schema_name,
            physical_table_name=datasource.table_name,
            dimensions=fields_by_category.get("dimensions", []),
            measures=fields_by_category.get("measures", []),
            identifiers=fields_by_category.get("identifiers", []),
            timestamps=fields_by_category.get("timestamps", []),
            geometries=fields_by_category.get("geometries", []),
            tags=tags,
            aliases=aliases,
            data_year=data_year
        )
        
        logger.debug("加载表结构完成",
                    table_id=table_id,
                    table_name=datasource.display_name,
                    dimensions=len(structure.dimensions),
                    measures=len(structure.measures),
                    identifiers=len(structure.identifiers),
                    timestamps=len(structure.timestamps),
                    geometries=len(structure.geometries),
                    total_fields=structure.total_fields)
        
        return structure
    
    def _load_fields_by_table(self, table_id: str) -> Dict[str, List[Field]]:
        """
        加载表的所有字段，按类别分组
        
        Args:
            table_id: 表ID
            
        Returns:
            字段字典: {
                "dimensions": [...],
                "measures": [...],
                "identifiers": [...],
                "timestamps": [...]
            }
        """
        result = {
            "dimensions": [],
            "measures": [],
            "identifiers": [],
            "timestamps": [],
            "geometries": []
        }
        
        # 遍历所有字段
        for field_id, field in self.model.fields.items():
            # 检查字段是否属于该表
            if field.datasource_id != table_id:
                continue
            
            # 检查字段是否活跃
            if hasattr(field, 'is_active') and not field.is_active:
                continue
            
            # 按类别分类
            category = field.field_category
            if category == "dimension":
                result["dimensions"].append(field)
            elif category == "measure":
                result["measures"].append(field)
            elif category == "identifier":
                result["identifiers"].append(field)
            elif category == "timestamp":
                result["timestamps"].append(field)
            elif category in ["geometry", "spatial"]:
                result["geometries"].append(field)
        
        # 按优先级和名称排序
        for category in result:
            result[category].sort(key=lambda f: (
                -getattr(f, 'priority', 5),  # 优先级高的在前
                f.display_name  # 同优先级按名称排序
            ))
        
        return result
    
    def load_multiple_tables(self, table_ids: List[str]) -> List[TableStructure]:
        """
        批量加载多个表的结构
        
        Args:
            table_ids: 表ID列表
            
        Returns:
            TableStructure列表
        """
        structures = []
        
        for table_id in table_ids:
            try:
                structure = self.load_table_structure(table_id)
                structures.append(structure)
            except ValueError as e:
                logger.warning(f"跳过无效的表: {e}")
                continue
        
        logger.info("批量加载表结构完成",
                   requested=len(table_ids),
                   loaded=len(structures))
        
        return structures
    
    def get_field_by_id(self, field_id: str) -> Field:
        """
        根据字段ID获取字段对象
        
        Args:
            field_id: 字段ID
            
        Returns:
            Field对象
            
        Raises:
            ValueError: 如果字段不存在
        """
        if field_id not in self.model.fields:
            raise ValueError(f"字段不存在: {field_id}")
        
        return self.model.fields[field_id]
    
    def get_fields_by_category(
        self,
        table_id: str,
        category: str
    ) -> List[Field]:
        """
        获取指定表的指定类别字段
        
        Args:
            table_id: 表ID
            category: 字段类别 (dimension, measure, identifier, timestamp)
            
        Returns:
            字段列表
        """
        fields_dict = self._load_fields_by_table(table_id)
        category_key = f"{category}s"  # dimension -> dimensions
        return fields_dict.get(category_key, [])

    def sort_fields_by_enum_feedback(
        self,
        fields: List[Field],
        enum_matches: List[Any]
    ) -> List[Field]:
        """
        按枚举命中反馈对字段排序
        
        枚举命中的字段在Prompt中优先展示
        
        字段得分计算: field_score = base_priority + α * max(enum_final_score for field)
        
        Args:
            fields: 字段列表
            enum_matches: 枚举匹配结果
            
        Returns:
            排序后的字段列表
        """
        if not enum_matches:
            return fields
        
        # 构建字段ID -> 最大枚举分数的映射
        field_enum_scores: Dict[str, float] = {}
        for enum in enum_matches:
            field_id = getattr(enum, 'field_id', None)
            if not field_id:
                continue
            score = getattr(enum, 'final_score', 0.0) or 0.0
            if field_id not in field_enum_scores:
                field_enum_scores[field_id] = score
            else:
                field_enum_scores[field_id] = max(field_enum_scores[field_id], score)
        
        def get_sort_key(f: Field):
            base_priority = getattr(f, 'priority', 5)
            enum_boost = self._enum_feedback_alpha * field_enum_scores.get(str(f.field_id), 0.0)
            # 返回负值使得高分在前（降序）
            combined_score = base_priority - enum_boost * self._enum_boost_multiplier
            return (combined_score, f.display_name)
        
        sorted_fields = sorted(fields, key=get_sort_key)
        
        # 日志记录枚举命中的字段
        boosted_fields = [
            f.display_name for f in sorted_fields
            if str(f.field_id) in field_enum_scores
        ]
        if boosted_fields:
            logger.debug(
                "字段按枚举反馈排序",
                boosted_fields=boosted_fields[:5]
            )
        
        return sorted_fields

    def load_table_structure_with_enum_feedback(
        self,
        table_id: str,
        enum_matches: List[Any]
    ) -> TableStructure:
        """
        加载表结构并按枚举反馈排序字段
        
        Args:
            table_id: 表ID
            enum_matches: 枚举匹配结果
            
        Returns:
            TableStructure对象（字段已按枚举反馈排序）
        """
        structure = self.load_table_structure(table_id)
        
        # 对每个类别的字段应用枚举反馈排序
        structure.dimensions = self.sort_fields_by_enum_feedback(
            structure.dimensions, enum_matches
        )
        structure.measures = self.sort_fields_by_enum_feedback(
            structure.measures, enum_matches
        )
        structure.identifiers = self.sort_fields_by_enum_feedback(
            structure.identifiers, enum_matches
        )
        structure.timestamps = self.sort_fields_by_enum_feedback(
            structure.timestamps, enum_matches
        )
        structure.geometries = self.sort_fields_by_enum_feedback(
            structure.geometries, enum_matches
        )
        
        return structure

