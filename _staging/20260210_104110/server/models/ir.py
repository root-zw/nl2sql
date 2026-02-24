"""中间表示 (IR) 数据模型"""

from typing import Literal, Optional, List, Any, Dict, Union
from pydantic import BaseModel, Field, ConfigDict, field_serializer
from datetime import date


class TimeRange(BaseModel):
    """时间范围"""

    type: Literal["absolute", "relative", "rolling"]

    # absolute 模式
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    # relative 模式（例如：最近30天）
    last_n: Optional[int] = None
    unit: Optional[Literal["day", "week", "month", "quarter", "year"]] = None

    # rolling 模式（例如：本月至今）
    grain: Optional[Literal["week", "month", "quarter", "year"]] = None
    offset: int = 0  # 0=当前周期，-1=上一周期

    @field_serializer('start_date', 'end_date')
    def serialize_date(self, value: Optional[date]) -> Optional[str]:
        """序列化日期为 ISO 格式字符串"""
        return value.isoformat() if value else None


from pydantic import field_validator


class FilterCondition(BaseModel):
    """过滤条件"""
    field: str  # 维度ID或列名
    op: Literal["=", "!=", ">", ">=", "<", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL", "ST_INTERSECTS", "IN_SUBQUERY", "NOT IN_SUBQUERY"]
    value: Any = None  # 单值或列表

    @field_validator("field", mode="before")
    @classmethod
    def ensure_field_str(cls, v):
        if v is None:
            return v
        return str(v)


class OrderBy(BaseModel):
    """排序规则"""
    field: str  # 指标ID或维度ID
    desc: bool = True


class MetricSpec(BaseModel):
    """度量字段规格 - 支持指定聚合函数"""
    
    field: str = Field(description="度量字段ID")
    aggregation: Literal["SUM", "AVG", "MIN", "MAX", "COUNT"] = Field(
        default="SUM",
        description="聚合函数类型，默认为 SUM"
    )
    alias: Optional[str] = Field(
        default=None,
        description="结果列别名（可选），不指定则使用'聚合函数(字段显示名)'"
    )
    decimal_places: int = Field(
        default=2,
        description="小数位数，默认2位"
    )
    
    @field_validator("field", mode="before")
    @classmethod
    def ensure_field_str(cls, v):
        if v is None:
            return v
        return str(v)


class ConditionalMetric(BaseModel):
    """条件聚合指标 - 支持 SUM(CASE WHEN cond THEN value ELSE 0 END) 模式"""
    
    field: str = Field(description="要聚合的度量字段ID")
    condition: 'FilterCondition' = Field(description="聚合条件")
    aggregation: Literal["SUM", "COUNT", "AVG", "MIN", "MAX"] = Field(
        default="SUM",
        description="聚合函数类型"
    )
    alias: str = Field(description="结果列别名")
    else_value: Optional[Any] = Field(
        default=0,
        description="条件不满足时的默认值，COUNT时通常为0，SUM时也为0"
    )
    decimal_places: int = Field(
        default=2,
        description="小数位数，默认2位"
    )


class CalculatedField(BaseModel):
    """计算字段 - 支持字段级表达式如 price * quantity"""
    
    expression: str = Field(
        description="计算表达式，可引用字段ID或别名，如 '{field_a} * {field_b}' 或 '{metric_a} / NULLIF({metric_b}, 0) * 100'"
    )
    alias: str = Field(description="结果列别名")
    field_refs: List[str] = Field(
        default_factory=list,
        description="表达式中引用的字段ID列表，用于编译器解析"
    )
    aggregation: Optional[Literal["AVG", "SUM", "MAX", "MIN", "NONE"]] = Field(
        default=None,
        description="在聚合查询中对表达式结果应用的聚合函数。AVG表示求平均，NONE表示表达式内部已包含聚合逻辑"
    )
    unit: Optional[str] = Field(
        default=None,
        description="单位（可选），如不指定则从引用字段继承。注意：比率类指标（如溢价率、增长率）不应有单位或单位为'%'"
    )
    decimal_places: int = Field(
        default=2,
        description="小数位数，默认2位"
    )
    total_strategy: Optional[Literal["sum", "recalculate", "weighted_avg", "max", "min", "none"]] = Field(
        default=None,
        description="合计行计算策略：sum=直接求和（绝对值指标如金额），recalculate=重新计算公式（比率类如溢价率、占比），weighted_avg=加权平均，max/min=取最大/最小值，none=不显示合计。如不指定则根据 aggregation 和 expression 自动推断"
    )
    numerator_refs: Optional[List[str]] = Field(
        default=None,
        description="分子引用的字段ID列表（仅比率类指标需要，用于合计行重新计算）"
    )
    denominator_refs: Optional[List[str]] = Field(
        default=None,
        description="分母引用的字段ID列表（仅比率类指标需要，用于合计行重新计算）"
    )


class RatioMetric(BaseModel):
    """占比/通过率指标 - 高频模式的简化表达"""
    
    numerator_field: str = Field(description="分子字段ID")
    numerator_condition: Optional['FilterCondition'] = Field(
        default=None,
        description="分子条件（可选），不指定则使用 COUNT(*)"
    )
    denominator_field: Optional[str] = Field(
        default=None,
        description="分母字段ID（可选），不指定则使用 COUNT(*)"
    )
    denominator_condition: Optional['FilterCondition'] = Field(
        default=None,
        description="分母条件（可选）"
    )
    alias: str = Field(description="结果列别名")
    as_percentage: bool = Field(
        default=True,
        description="是否转换为百分比（乘以100）"
    )
    decimal_places: int = Field(
        default=2,
        description="小数位数"
    )


class IntermediateRepresentation(BaseModel):
    """中间表示 - 系统内部的标准化查询指令"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "metrics": ["gmv", "order_count"],
                "dimensions": ["category", "brand"],
                "time": {
                    "type": "relative",
                    "last_n": 30,
                    "unit": "day"
                },
                "filters": [
                    {"field": "region", "op": "=", "value": "华东"},
                    {"field": "refund_flag", "op": "=", "value": False}
                ],
                "order_by": [{"field": "gmv", "desc": True}],
                "limit": 10,
                "confidence": 0.95,
                "original_question": "最近30天华东地区各品类GMV排名前10"
            }
        }
    )

    # 查询类型
    query_type: Literal["aggregation", "detail", "duplicate_detection", "window_detail"] = Field(
        default="aggregation",
        description="查询类型：aggregation（聚合统计）、detail（明细列表）、duplicate_detection（重复检测）或 window_detail（窗口函数明细查询）"
    )

    # 重复检测专用字段
    duplicate_by: List[str] = Field(
        default_factory=list,
        description="用于判断重复的字段列表（dimension ID 列表，仅用于 duplicate_detection 查询）"
    )
    
    # 窗口函数专用字段
    partition_by: List[str] = Field(
        default_factory=list,
        description="窗口函数分区字段列表（dimension ID 列表，仅用于 window_detail 查询）。用于'分别'、'各自'等分组TopN场景"
    )
    window_limit: Optional[int] = Field(
        default=None,
        description="窗口内记录数限制（仅用于 window_detail 查询）。例如'前5名'对应window_limit=5"
    )
    
    # 同比/环比分析（SQL Server 2012+ LAG/LEAD）
    comparison_type: Optional[Literal["yoy", "mom", "qoq", "wow"]] = Field(
        default=None,
        description="对比类型：yoy(同比-年), mom(环比-月), qoq(环比-季), wow(环比-周)"
    )
    comparison_periods: int = Field(
        default=1,
        description="对比周期数，默认1（例如：对比上一年/上一月）"
    )
    show_growth_rate: bool = Field(
        default=True,
        description="是否显示增长率（同比/环比使用）"
    )
    
    # 累计统计（SQL Server 2012+ SUM/AVG OVER）
    cumulative_metrics: List[str] = Field(
        default_factory=list,
        description="需要累计计算的指标ID列表（例如：累计销售额）"
    )
    cumulative_order_by: Optional[str] = Field(
        default=None,
        description="累计统计的排序字段（通常是时间维度ID），不指定则使用第一个dimension"
    )
    
    # 移动平均（SQL Server 2012+ AVG OVER ROWS BETWEEN）
    moving_average_window: Optional[int] = Field(
        default=None,
        description="移动平均窗口大小（例如：7日均线对应window=7）"
    )
    moving_average_metrics: List[str] = Field(
        default_factory=list,
        description="需要计算移动平均的指标ID列表"
    )

    # 核心要素
    # metrics 支持两种格式：
    # 1. 字符串：字段ID，使用默认 SUM 聚合，如 "field_uuid"
    # 2. MetricSpec 对象：指定聚合函数，如 {"field": "field_uuid", "aggregation": "MAX", "alias": "最大值"}
    metrics: List[Union[str, MetricSpec]] = Field(
        default_factory=list, 
        description="指标列表。支持字符串（默认SUM聚合）或MetricSpec对象（指定聚合函数）"
    )
    dimensions: List[str] = Field(default_factory=list, description="维度ID列表")

    # 时间控制
    time: Optional[TimeRange] = Field(default=None, description="时间范围")
    time_grain: Optional[Literal["day", "week", "month", "quarter", "year"]] = None

    # 过滤与排序
    filters: List[FilterCondition] = Field(default_factory=list)
    order_by: List[OrderBy] = Field(default_factory=list)  # 保留旧字段用于兼容

    #  JOIN策略（用于多表关联查询）
    join_strategy: Literal["matched", "left_unmatched", "right_unmatched"] = Field(
        default="matched",
        description="JOIN策略：matched(查询匹配的记录-INNER JOIN), left_unmatched(查询左表未匹配的记录-LEFT JOIN+IS NULL), right_unmatched(查询右表未匹配的记录-RIGHT JOIN+IS NULL)"
    )

    #  反向匹配参照表（可选）
    anti_join_table: Optional[str] = Field(
        default=None,
        description="反向匹配时的参照表ID（表的source_id）。当join_strategy为left_unmatched/right_unmatched时，指定用于判断'未匹配'的参照表。如果未指定且只有一个JOIN关系，系统会自动推断。"
    )

    # 主表提示（可选，由表级检索结果提供）
    primary_table_id: Optional[str] = Field(
        default=None,
        description="候选表检索阶段排名第一的表ID，用作默认主表提示（通常对应语义最相关的明细表）。"
    )
    
    # ========== 多表/跨分区查询支持 ==========
    
    # 选中的多个表（用于跨年UNION等场景）
    selected_table_ids: List[str] = Field(
        default_factory=list,
        description="选中的表ID列表（用于跨年UNION等多表场景）。为空时使用 primary_table_id。"
    )
    
    # 选中表的物理信息（用于 multi_join 等跨域场景，编译器可能无法从模型中获取）
    selected_table_info: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="表ID到物理信息的映射 {table_id: {table_name, schema_name, display_name}}，用于编译器直接使用"
    )
    
    # 跨分区查询配置
    cross_partition_query: bool = Field(
        default=False,
        description="是否为跨分区查询（如跨年对比、历年数据汇总）"
    )
    cross_partition_mode: Literal["union", "compare", "multi_join"] = Field(
        default="union",
        description="跨分区模式：union(UNION ALL合并数据), compare(对比分析，生成JOIN计算变化), multi_join(多表关联查询，找出同时存在于多个表中的记录)"
    )
    partition_label_field: Optional[str] = Field(
        default=None,
        description="分区标签字段名（如'数据年份'），UNION时自动添加该列区分来源表"
    )
    
    # 跨分区对比专用字段
    compare_join_keys: List[str] = Field(
        default_factory=list,
        description="跨分区对比时的JOIN关联字段ID列表（与metrics/dimensions/filters保持一致使用字段ID）"
    )
    compare_base_table_id: Optional[str] = Field(
        default=None,
        description="对比查询的基准表ID（当期数据所在的表），默认使用selected_table_ids[0]"
    )
    
    # multi_join 专用：字段映射（避免编译器依赖 model.fields 查找跨域表的字段）
    # 格式: [{display_name: "行政区", table_id_1: "column_name_1", table_id_2: "column_name_2", ...}, ...]
    multi_join_field_mappings: List[Dict[str, str]] = Field(
        default_factory=list,
        description="multi_join 模式下的字段映射列表，每个元素包含 display_name 和各表的列名"
    )
    
    # multi_join 专用：是否对主表的 JOIN 键进行去重
    # 默认 True 避免笛卡尔积导致的数据膨胀，适用于"匹配检查"类查询
    # 设为 False 适用于需要保留完整关联关系的场景
    deduplicate_join_keys: bool = Field(
        default=True,
        description="multi_join 模式下是否对主表的 JOIN 键进行 DISTINCT 去重。默认 True 避免笛卡尔积，设为 False 保留完整关联。"
    )
    
    # 跨表字段映射（LLM生成，用于多表查询时精确映射字段）
    # 格式: {主表字段UUID: {其他表ID: 对应字段UUID, ...}, ...}
    # 示例: {"99cf5fcd-...": {"917983ce-...": "abc123-..."}}
    cross_table_field_mappings: Dict[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="跨表字段映射：主表字段UUID到其他表对应字段UUID的映射。LLM在多表查询时生成，用于编译器精确查找跨表字段。"
    )

    # 明细查询专用排序字段
    sort_by: Optional[str] = Field(
        default=None,
        description="排序字段（measure ID 或 dimension ID，仅用于明细查询）"
    )
    sort_order: Literal["asc", "desc"] = Field(default="desc", description="排序方向")

    # 明细查询默认列控制
    suppress_detail_defaults: bool = Field(
        default=False,
        description="当为True时，明细查询不会自动附加模型配置的默认列，仅返回IR显式指定的字段"
    )

    # 结果控制
    limit: Optional[int] = Field(default=None, le=10000, description="返回记录数限制，None表示不限制（但会受最大限制约束）")
    with_total: bool = Field(default=False, description="是否在分组结果后添加汇总行")

    # 元数据
    confidence: float = Field(default=1.0, ge=0.0, le=1.0, description="LLM解析置信度")
    ambiguities: List[str] = Field(default_factory=list, description="不确定的地方")
    original_question: str = Field(description="用户原始问题")

    # 业务域（可选，由用户指定或系统自动检测）
    domain_id: Optional[str] = Field(default=None, description="业务域ID")
    domain_name: Optional[str] = Field(default=None, description="业务域名称（用于显示）")

    # 复杂问题标识（用于引导用户拆解）
    is_too_complex: bool = Field(default=False, description="问题是否过于复杂，建议拆解为多个子问题")
    complexity_reason: Optional[str] = Field(default=None, description="复杂性原因说明")
    suggested_subquestions: List[str] = Field(default_factory=list, description="建议的子问题列表")

    # ========== 混合架构扩展字段 ==========
    
    # 条件聚合（支持 SUM(CASE WHEN...) 模式）
    conditional_metrics: List[ConditionalMetric] = Field(
        default_factory=list,
        description="条件聚合指标列表，用于'其中XX的数量'、'满足条件的合计'等场景"
    )
    
    # HAVING子句（聚合后过滤）
    having_filters: List[FilterCondition] = Field(
        default_factory=list,
        description="HAVING过滤条件，用于'销售额大于100万的'等聚合后过滤场景"
    )
    
    # 计算字段（字段级表达式）
    calculated_fields: List[CalculatedField] = Field(
        default_factory=list,
        description="计算字段列表，用于'单价*数量'、'完成率=实际/目标'等场景"
    )
    
    # 占比/通过率（高频模式简化）
    ratio_metrics: List[RatioMetric] = Field(
        default_factory=list,
        description="占比指标列表，用于'XX占比'、'通过率'、'命中率'等场景"
    )
    
    # 复杂查询路由标记（用于混合架构）
    requires_direct_sql: bool = Field(
        default=False,
        description="是否需要直接SQL生成（当IR无法表达时由LLM标记）"
    )
    direct_sql_reason: Optional[str] = Field(
        default=None,
        description="需要直接SQL的原因（如CTE、递归、PIVOT等）"
    )
    
    # ========== 修复审计 ==========
    
    # IR 修复日志（由 validator 和 compiler 阶段填充）
    fix_log: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="IR修复日志，记录所有修复操作：[{stage, action, field, original, fixed, reason}, ...]"
    )
    
    def add_fix_log(
        self,
        stage: str,
        action: str,
        field: Optional[str] = None,
        original: Any = None,
        fixed: Any = None,
        reason: Optional[str] = None
    ):
        """
        添加一条修复日志
        
        Args:
            stage: 修复阶段（validator, compiler, parser 等）
            action: 修复动作（remove_dimension, remap_filter, add_filter, etc.）
            field: 涉及的字段ID（可选）
            original: 原始值（可选）
            fixed: 修复后的值（可选）
            reason: 修复原因（可选）
        """
        log_entry = {
            "stage": stage,
            "action": action
        }
        if field is not None:
            log_entry["field"] = field
        if original is not None:
            log_entry["original"] = str(original) if not isinstance(original, (str, int, float, bool, type(None))) else original
        if fixed is not None:
            log_entry["fixed"] = str(fixed) if not isinstance(fixed, (str, int, float, bool, type(None))) else fixed
        if reason is not None:
            log_entry["reason"] = reason
        
        self.fix_log.append(log_entry)
    
    def get_fix_log(self) -> List[Dict[str, Any]]:
        """获取修复日志"""
        return self.fix_log
    
    def has_fixes(self) -> bool:
        """是否有修复操作"""
        return len(self.fix_log) > 0

