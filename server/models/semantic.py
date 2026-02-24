"""语义模型数据结构"""

from typing import Optional, List, Dict, Literal, Any
from pydantic import BaseModel
from pydantic import Field as PydanticField
from decimal import Decimal

from server.config import settings

# ============================================================================
# 第一层：租户与组织
# ============================================================================

class TenantConfig(BaseModel):
    """租户全局配置"""
    default_year_field: Optional[str] = None
    default_datetime_field: Optional[str] = None
    default_currency: str = "CNY"
    timezone: str = settings.timezone
    query_timeout_seconds: int = 120
    query_max_rows: int = 10000
    cache_ttl_seconds: int = 1800


class BusinessDomain(BaseModel):
    """业务域"""
    domain_id: str
    domain_code: str
    domain_name: str
    description: Optional[str] = None
    parent_domain_id: Optional[str] = None
    icon: str = ""
    color: str = "#409eff"
    sort_order: int = 0
    keywords: List[str] = []


# ============================================================================
# 第二层：数据源（逻辑层）
# ============================================================================

class PhysicalColumn(BaseModel):
    """物理列定义"""
    column_id: str
    column_name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    max_length: Optional[int] = None
    precision_val: Optional[int] = None
    scale_val: Optional[int] = None
    ordinal_position: Optional[int] = None
    default_value: Optional[str] = None
    column_comment: Optional[str] = None


class PhysicalTable(BaseModel):
    """物理表映射"""
    table_id: str
    schema_name: str
    table_name: str
    db_type: str
    connection_config: Optional[Dict[str, Any]] = None
    columns: List[PhysicalColumn] = []


class PhraseConfig(BaseModel):
    """短语配置（用于表识别）"""
    text: str
    weight: float = 1.0


class DatasourceIdentity(BaseModel):
    """数据源识别配置"""
    core_phrases: List[PhraseConfig] = []
    unique_terms: List[str] = []
    exclusion_terms: List[str] = []
    typical_queries: List[str] = []


class Datasource(BaseModel):
    """数据源（逻辑抽象）"""
    datasource_id: str
    datasource_name: str
    display_name: str
    description: Optional[str] = None
    datasource_type: str = "table"
    domain_id: Optional[str] = None
    owner: Optional[str] = None
    is_sensitive: bool = False
    tags: List[str] = []
    data_year: Optional[str] = None

    # 多连接支持：标识数据源所属的数据库连接
    connection_id: Optional[str] = None

    # 关联的物理表
    physical_tables: List[PhysicalTable] = []

    # 表识别配置
    identity: Optional[DatasourceIdentity] = None

    # 向后兼容属性
    @property
    def schema_name(self) -> Optional[str]:
        """向后兼容：获取主物理表的schema"""
        if self.physical_tables:
            return self.physical_tables[0].schema_name
        return None

    @property
    def table_name(self) -> Optional[str]:
        """向后兼容：获取主物理表的table_name"""
        if self.physical_tables:
            return self.physical_tables[0].table_name
        return None

    @property
    def sensitive(self) -> bool:
        """向后兼容：is_sensitive的别名"""
        return self.is_sensitive

    @property
    def columns(self) -> List:
        """向后兼容：从第一个物理表获取列"""
        if self.physical_tables and self.physical_tables[0].columns:
            return self.physical_tables[0].columns
        return []

    @property
    def detail_view(self):
        """向后兼容：返回None，由Fields配置决定明细字段"""
        return None

    @property
    def primary_key(self) -> List[str]:
        """获取主键列名列表"""
        pk_columns = []
        if self.physical_tables:
            for col in self.physical_tables[0].columns:
                if col.is_primary_key:
                    pk_columns.append(col.column_name)
        return pk_columns


# ============================================================================
# 第三层：字段（统一建模）
# ============================================================================

class FieldDimensionProps(BaseModel):
    """维度字段扩展属性"""
    dimension_type: Optional[str] = None  # categorical, temporal, hierarchical, geographic
    cardinality: Optional[int] = None
    has_hierarchy: bool = False
    hierarchy_level: Optional[int] = None
    parent_field_id: Optional[str] = None
    match_pattern: str = "exact"  # exact, fuzzy, prefix, contains


class FieldMeasureProps(BaseModel):
    """度量字段扩展属性"""
    unit: Optional[str] = None
    aggregatable: bool = True
    default_aggregation: Optional[str] = None  # SUM, AVG, COUNT, MAX, MIN
    min_value: Optional[Decimal] = None
    max_value: Optional[Decimal] = None
    decimal_places: int = 2


class FieldTimestampProps(BaseModel):
    """时间字段扩展属性"""
    time_granularity: Optional[str] = None  # second, minute, hour, day, month, quarter, year
    is_partition_key: bool = False
    timezone: Optional[str] = None
    format_pattern: Optional[str] = None


class Field(BaseModel):
    """字段统一表"""
    field_id: str
    datasource_id: str
    physical_column_id: str
    field_name: str
    display_name: str
    description: Optional[str] = None
    field_category: str  # dimension, measure, identifier, timestamp, geometry/spatial
    data_type: str

    # 同义词（字段名称同义词）
    synonyms: List[str] = []

    # 优先级控制
    priority: int = 5
    is_primary: bool = False
    field_role: str = "primary"  # primary, secondary, auxiliary

    # 状态
    is_active: bool = True
    # 明细查询默认关闭
    show_in_detail: bool = False

    #  单位转换配置
    unit_conversion: Optional[Dict[str, Any]] = None

    # 扩展属性（根据field_category填充）
    dimension_props: Optional[FieldDimensionProps] = None
    measure_props: Optional[FieldMeasureProps] = None
    timestamp_props: Optional[FieldTimestampProps] = None

    # 缓存的物理列名（从数据库JOIN查询时填充）
    physical_column_name: Optional[str] = None

    # 向后兼容属性
    @property
    def table(self) -> str:
        """向后兼容：datasource_id的别名"""
        return self.datasource_id

    @property
    def column(self) -> str:
        """
        向后兼容：获取物理列名
        返回缓存的物理列名（如果有），否则回退到逻辑字段名
        """
        return self.physical_column_name or self.field_name

    @property
    def type(self) -> str:
        """向后兼容：维度类型"""
        if self.dimension_props:
            return self.dimension_props.dimension_type or "categorical"
        return "categorical"

    @property
    def unit(self) -> Optional[str]:
        """向后兼容：度量的单位"""
        if self.measure_props:
            return self.measure_props.unit
        return None

    @property
    def sortable(self) -> bool:
        """向后兼容：是否可排序"""
        return True  # 默认所有字段都可排序

    @property
    def filterable(self) -> bool:
        """向后兼容：是否可过滤"""
        return True  # 默认所有字段都可过滤

    @property
    def aggregatable(self) -> bool:
        """向后兼容：是否可聚合"""
        if self.measure_props:
            return self.measure_props.aggregatable
        return self.field_category == 'measure'

    @property
    def include_in_result(self) -> bool:
        """向后兼容：是否在结果中包含（默认False，除非是primary字段）"""
        return self.is_primary

    @property
    def match_mode(self) -> str:
        """向后兼容：match_pattern的别名"""
        if self.dimension_props:
            return self.dimension_props.match_pattern
        return "exact"  # 默认精确匹配
    
    def get_connection_id(self, semantic_model: 'SemanticModel') -> Optional[str]:
        """
        通过 datasource_id 反推 connection_id
        
        Args:
            semantic_model: 语义模型实例
            
        Returns:
            connection_id 或 None
        """
        if not semantic_model or not semantic_model.datasources:
            return None
        datasource = semantic_model.datasources.get(self.datasource_id)
        return datasource.connection_id if datasource else None


# ============================================================================
# 第四层：枚举值管理
# ============================================================================

class FieldEnumSynonym(BaseModel):
    """枚举值同义词"""
    synonym_id: str
    synonym_text: str
    match_type: str = "exact"  # exact, fuzzy, partial, regex
    confidence_score: Decimal = Decimal("1.0")
    source_type: str = "manual"  # manual, imported, auto_detected, ai_suggested
    match_count: int = 0


class FieldEnumValue(BaseModel):
    """字段枚举值"""
    value_id: str
    field_id: str
    standard_value: str
    display_name: Optional[str] = None
    value_category: Optional[str] = None
    sort_order: int = 0
    is_active: bool = True
    record_count: int = 0

    # 同义词列表
    synonyms: List[FieldEnumSynonym] = []

    # 包含关系：该值包含哪些其他标准值（用于查询展开）
    includes_values: Optional[List[str]] = None


# ============================================================================
# 第五层：指标（计算层）
# ============================================================================

class MetricAtomic(BaseModel):
    """原子指标定义"""
    base_field_id: Optional[str] = None  # COUNT/COUNT_DISTINCT 可以为空
    aggregation: str  # SUM, AVG, COUNT, MAX, MIN, COUNT_DISTINCT, MEDIAN
    filter_condition: Optional[str] = None


class MetricDerived(BaseModel):
    """派生指标定义"""
    formula: str
    formula_type: str = "arithmetic"  # arithmetic, custom_sql, expression


class MetricDependency(BaseModel):
    """指标依赖关系"""
    depends_on_type: str  # field, metric
    depends_on_id: str
    dependency_role: str = "operand"  # operand, filter, context
    transform_expression: Optional[str] = None


class Metric(BaseModel):
    """指标表"""
    metric_id: str
    metric_name: str
    display_name: str
    description: Optional[str] = None
    metric_type: str  # atomic, derived, ratio
    domain_id: Optional[str] = None

    # 同义词（指标名称同义词）
    synonyms: List[str] = []

    # 格式化
    unit: Optional[str] = None
    format_pattern: Optional[str] = None
    decimal_places: int = 2

    # 业务属性
    is_sensitive: bool = False
    threshold_warning: Optional[Decimal] = None
    threshold_critical: Optional[Decimal] = None

    # 指标类型定义
    atomic_def: Optional[MetricAtomic] = None
    derived_def: Optional[MetricDerived] = None

    # 依赖关系
    dependencies: List[MetricDependency] = []

    #  向后兼容：expression字段（完整SQL表达式，包含单位转换等）
    expression: Optional[str] = None

    # 向后兼容属性
    @property
    def default_filters(self) -> List[str]:
        """向后兼容：默认过滤条件（从atomic_def.filter_condition提取）"""
        if self.atomic_def and self.atomic_def.filter_condition:
            return [self.atomic_def.filter_condition]
        return []

    @property
    def format(self) -> Optional[str]:
        """向后兼容：format_pattern的别名"""
        return self.format_pattern


# ============================================================================
# 第六层：关系定义
# ============================================================================

class DatasourceJoin(BaseModel):
    """数据源关系"""
    join_id: str
    from_datasource_id: str
    to_datasource_id: str
    join_type: str  # left, inner, right, full
    join_condition: str
    is_bidirectional: bool = True
    priority: int = 5
    cardinality: Optional[str] = None  # 1:1, 1:N, N:1, N:M
    dedup_strategy: Optional[str] = None
    dedup_order_by: Optional[str] = None

    # 向后兼容属性
    @property
    def from_table(self) -> str:
        """向后兼容：from_datasource_id的别名"""
        return self.from_datasource_id

    @property
    def to_table(self) -> str:
        """向后兼容：to_datasource_id的别名"""
        return self.to_datasource_id

    @property
    def on(self) -> str:
        """向后兼容：join_condition的别名"""
        return self.join_condition

    @property
    def type(self) -> str:
        """向后兼容：join_type的别名，转换为大写"""
        return self.join_type.upper()

    @property
    def bidirectional(self) -> bool:
        """向后兼容：is_bidirectional的别名"""
        return self.is_bidirectional


# ============================================================================
# 第七层：规则与配置
# ============================================================================

class FieldValidationRule(BaseModel):
    """字段验证规则"""
    rule_id: str
    field_id: str
    rule_type: str  # range, regex, enum, custom
    rule_expression: str
    error_message: Optional[str] = None
    is_enabled: bool = True


class FieldNormalizationRule(BaseModel):
    """字段值标准化规则"""
    rule_id: str
    field_id: str
    rule_type: str  # synonym_map, case_normalize, trim, custom
    from_value: Optional[str] = None
    to_value: Optional[str] = None
    rule_priority: int = 5
    is_enabled: bool = True


class MetricBusinessRule(BaseModel):
    """指标业务规则"""
    rule_id: str
    metric_id: str
    rule_type: str  # default_filter, threshold_warning, threshold_critical, auto_dimension
    rule_config: Dict[str, Any]
    is_enabled: bool = True


class RowLevelSecurityRule(BaseModel):
    """行级安全规则"""
    rule_id: str
    datasource_id: str
    role_name: str
    filter_condition: str
    is_enabled: bool = True


# ============================================================================
# 第八层：参考数据
# ============================================================================

class ReferenceData(BaseModel):
    """参考数据"""
    data_id: str
    data_type: str  # administrative_division, industry_category, etc.
    data_code: str
    data_name: str
    data_value: Optional[str] = None
    parent_code: Optional[str] = None
    sort_order: int = 0
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# 第九层：表识别配置
# ============================================================================

class TableResolutionConfig(BaseModel):
    """表识别配置"""
    confidence_threshold_high: Decimal = Decimal("0.85")
    confidence_threshold_medium: Decimal = Decimal("0.55")
    require_clarification: bool = True
    max_clarification_options: int = 3
    signal_weight_unique_field: Decimal = Decimal("10.0")
    signal_weight_phrase_match: Decimal = Decimal("1.0")
    signal_weight_common_field: Decimal = Decimal("1.0")
    multi_table_min_confidence: Decimal = Decimal("0.7")


class FormattingConfig(BaseModel):
    """格式化配置"""
    field_units: Dict[str, str] = {}  # {field_name: unit}
    display_names: Dict[str, str] = {}  # {field_name: display_name}
    number_formatting: Optional[Dict[str, Any]] = None
    field_keyword_mapping: Optional[Dict[str, List[str]]] = None


# ============================================================================
# 完整语义模型
# ============================================================================

class SemanticModel(BaseModel):
    """完整语义模型"""
    version: str = "2.0"

    # 租户配置
    tenant_config: Optional[TenantConfig] = None

    # 业务域
    domains: Dict[str, BusinessDomain] = {}

    # 数据源（逻辑层）
    datasources: Dict[str, Datasource] = {}

    # 字段（统一建模）
    fields: Dict[str, Field] = {}

    # 枚举值
    field_enums: Dict[str, List[FieldEnumValue]] = {}  # {field_id: [values]}

    # 指标
    metrics: Dict[str, Metric] = {}

    # 关系
    joins: List[DatasourceJoin] = []

    # 规则
    field_validation_rules: List[FieldValidationRule] = []
    field_normalization_rules: List[FieldNormalizationRule] = []
    metric_business_rules: List[MetricBusinessRule] = []
    rls_rules: List[RowLevelSecurityRule] = []

    # 参考数据
    reference_data: Dict[str, List[ReferenceData]] = {}  # {data_type: [data]}

    # 表识别配置
    table_resolution_config: Optional[TableResolutionConfig] = None

    # 格式化配置
    formatting: Optional[FormattingConfig] = None

    # ========================================================================
    # 向后兼容属性
    # ========================================================================

    @property
    def dimensions(self) -> Dict[str, Field]:
        """向后兼容：返回维度字段"""
        return {
            field_id: field
            for field_id, field in self.fields.items()
            if field.field_category == 'dimension'
        }

    @property
    def measures(self) -> Dict[str, Field]:
        """向后兼容：返回度量字段"""
        return {
            field_id: field
            for field_id, field in self.fields.items()
            if field.field_category == 'measure'
        }

    @property
    def sources(self) -> Dict[str, Datasource]:
        """向后兼容：datasources的别名"""
        return self.datasources


# ============================================================================
# 兼容性映射（用于过渡期）
# ============================================================================

class ColumnDef(BaseModel):
    """列定义（兼容旧版本）"""
    name: str
    type: str  # VARCHAR, INT, DECIMAL, DATE, DATETIME 等
    nullable: bool = True
    description: Optional[str] = None


class Source(BaseModel):
    """数据源（兼容旧版本）"""
    id: str
    schema_name: str = "dbo"
    table_name: str
    display_name: str
    description: Optional[str] = None
    columns: List[Dict[str, Any]] = []
    primary_key: List[str] = []
    domain_id: Optional[str] = None
    time_field: Optional[str] = None
    owner: Optional[str] = None
    sensitive: bool = False
    tags: List[str] = []


class Dimension(BaseModel):
    """维度（兼容旧版本）"""
    id: str
    table: str
    column: str
    display_name: str
    description: Optional[str] = None
    synonyms: List[str] = []
    domain_id: Optional[str] = None
    sample_values: List[str] = []
    match_pattern: str = "exact"
    priority: int = 5
    cardinality: Optional[int] = None
    unit_conversion: Optional[Dict[str, Any]] = None  #  单位转换配置


class Measure(BaseModel):
    """度量（兼容旧版本）"""
    id: str
    table: str
    column: str
    display_name: str
    description: Optional[str] = None
    data_type: str = "numeric"
    unit: Optional[str] = None
    unit_conversion: Optional[Dict[str, Any]] = None  #  单位转换配置
    synonyms: List[str] = []
    domain_id: Optional[str] = None
    sortable: bool = True
    filterable: bool = True
    aggregatable: bool = True


class Join(BaseModel):
    """表关联（兼容旧版本）"""
    from_table: str
    to_table: str
    on: str
    type: str = "INNER"
    bidirectional: bool = True
    cardinality: Optional[str] = None  # 1:1, 1:N, N:1, N:M
    dedup_strategy: Optional[str] = None
    dedup_order_by: Optional[str] = None
