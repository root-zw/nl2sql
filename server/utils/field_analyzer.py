"""
字段自动识别分析器
基于列名、数据类型、样本值自动识别字段类型
"""

import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import structlog
logger = structlog.get_logger()


@dataclass
class FieldAnalysisResult:
    """字段分析结果"""
    field_type: str  # 'dimension', 'measure', 'timestamp', 'identifier', 'spatial'
    display_name: str
    confidence_score: float  # 0-1

    # 基础指标配置（如果是measure）
    default_aggregation: Optional[str] = None
    allowed_aggregations: List[str] = None
    unit: Optional[str] = None
    is_additive: bool = True  # 是否可加性（度量字段）

    # 维度配置
    dimension_type: Optional[str] = None  # 'categorical', 'hierarchical', 'temporal'

    # 标识配置
    is_unique: bool = False

    # 建议信息
    suggested_synonyms: List[str] = None

    def __post_init__(self):
        if self.allowed_aggregations is None:
            self.allowed_aggregations = []
        if self.suggested_synonyms is None:
            self.suggested_synonyms = []


class FieldAnalyzer:
    """字段分析器"""

    # 度量字段关键词（英文 + 中文）
    MEASURE_KEYWORDS = {
        # 英文关键词
        'amount', 'price', 'cost', 'fee', 'total', 'sum', 'count', 'quantity',
        'qty', 'num', 'number', 'value', 'rate', 'percent', 'ratio', 'avg',
        'average', 'min', 'max', 'score', 'weight', 'balance', 'income',
        'revenue', 'profit', 'sales', 'spend', 'expense', 'salary', 'wage',
        'volume', 'size', 'length', 'width', 'height', 'area', 'distance',
        # 中文关键词
        '金额', '价格', '单价', '总价', '成本', '费用', '工资', '薪资',
        '收入', '营收', '销售额', '利润', '支出', '开支', '消费',
        '数量', '数目', '个数', '件数', '笔数', '次数', '人数',
        '总计', '总和', '合计', '累计', '汇总',
        '比率', '比例', '百分比', '占比', '比重',
        '得分', '分数', '评分', '积分', '评价',
        '重量', '体重', '净重', '毛重',
        '长度', '宽度', '高度', '面积', '体积', '容量',
        '距离', '里程'
    }

    # 时间字段关键词（英文 + 中文）
    TIMESTAMP_KEYWORDS = {
        # 英文关键词
        'time', 'date', 'datetime', 'timestamp', 'created', 'updated', 'modified',
        'deleted', 'started', 'ended', 'finished', 'year', 'month', 'day',
        'hour', 'minute', 'second', 'at', 'on',
        # 中文关键词
        '时间', '日期', '年份', '月份', '日',
        '创建', '更新', '修改', '删除',
        '开始', '结束', '完成'
    }

    # 标识字段关键词（英文 + 中文）
    IDENTIFIER_KEYWORDS = {
        # 英文关键词
        'id', 'uuid', 'guid', 'key', 'code', 'no', 'number', 'serial',
        # 中文关键词
        '编号', '代码', '编码', '序号', '序列号', '主键'
    }

    # 单位映射（支持中英文）
    UNIT_PATTERNS = {
        r'amount|price|cost|fee|salary|income|revenue|金额|价格|单价|总价|成本|费用|工资|薪资|收入|营收|销售额': '元',
        r'count|qty|quantity|num|number|数量|数目|个数|件数|笔数|次数|人数': '个',
        r'percent|rate|ratio|百分比|比率|比例|占比': '%',
        r'weight|重量|体重|净重|毛重': 'kg',
        r'length|width|height|distance|长度|宽度|高度|距离|里程': 'm',
        r'area|面积': 'm²',
        r'volume|体积|容量': 'm³',
        r'score|得分|分数|评分|积分': '分'
    }

    # 数值类型
    NUMERIC_TYPES = {
        'int', 'integer', 'bigint', 'smallint', 'tinyint',
        'decimal', 'numeric', 'float', 'double', 'real', 'money'
    }

    # 日期时间类型
    DATETIME_TYPES = {
        'date', 'datetime', 'datetime2', 'timestamp', 'time',
        'smalldatetime', 'datetimeoffset'
    }

    # 字符串类型
    STRING_TYPES = {
        'char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext',
        'string', 'character'
    }

    # 空间类型
    SPATIAL_TYPES = {
        'geometry', 'geography', 'point', 'linestring', 'polygon',
        'multipoint', 'multilinestring', 'multipolygon',
        'geometrycollection', 'geom', 'shape'
    }

    @staticmethod
    def clean_column_name(column_name: str) -> str:
        """清理列名（去除前缀、转小写）"""
        # 去除常见前缀
        for prefix in ['tbl_', 'tb_', 'col_', 'fld_']:
            if column_name.lower().startswith(prefix):
                column_name = column_name[len(prefix):]

        return column_name.lower()

    @staticmethod
    def generate_display_name(column_name: str) -> str:
        """生成显示名称"""
        # 移除下划线，转为空格，首字母大写
        clean = column_name.replace('_', ' ').strip()

        # 驼峰命名拆分
        clean = re.sub(r'([a-z])([A-Z])', r'\1 \2', clean)

        # 首字母大写
        return clean.title()

    @staticmethod
    def extract_unit(column_name: str) -> Optional[str]:
        """从列名提取单位"""
        column_lower = column_name.lower()

        for pattern, unit in FieldAnalyzer.UNIT_PATTERNS.items():
            if re.search(pattern, column_lower):
                return unit

        return None

    @staticmethod
    def is_measure_type(column_name: str, data_type: str) -> bool:
        """
        判断是否为度量类型

        策略：
        1. 优先：数值类型 + 字段名包含度量关键词 → 度量
        2. 兜底：数值类型但字段名不包含标识符关键词 → 度量（默认）
        """
        if data_type.lower() not in FieldAnalyzer.NUMERIC_TYPES:
            return False

        column_lower = FieldAnalyzer.clean_column_name(column_name)

        #  如果字段名包含度量关键词，明确是度量
        for keyword in FieldAnalyzer.MEASURE_KEYWORDS:
            if keyword in column_lower:
                return True

        #  如果字段名包含标识符关键词，明确不是度量
        for keyword in FieldAnalyzer.IDENTIFIER_KEYWORDS:
            if keyword in column_lower:
                return False

        #  兜底：数值类型默认为度量（除非已被标识符规则排除）
        # 这里返回True，意味着"不知道字段名含义时，数值类型默认可聚合"
        return True

    @staticmethod
    def is_timestamp_type(column_name: str, data_type: str) -> bool:
        """判断是否为时间戳类型"""
        # 日期时间类型
        if data_type.lower() in FieldAnalyzer.DATETIME_TYPES:
            return True

        # 列名包含时间关键词
        column_lower = FieldAnalyzer.clean_column_name(column_name)
        for keyword in FieldAnalyzer.TIMESTAMP_KEYWORDS:
            if keyword in column_lower:
                return True

        return False

    @staticmethod
    def is_spatial_type(data_type: str) -> bool:
        """判断是否为空间类型"""
        data_type_lower = data_type.lower()
        for spatial_type in FieldAnalyzer.SPATIAL_TYPES:
            if spatial_type in data_type_lower:
                return True
        return False

    @staticmethod
    def is_identifier_type(
        column_name: str,
        is_primary_key: bool,
        is_unique: bool = False
    ) -> bool:
        """判断是否为标识类型"""
        # 主键
        if is_primary_key:
            return True

        # 列名包含标识关键词
        column_lower = FieldAnalyzer.clean_column_name(column_name)

        # 以id结尾
        if column_lower.endswith('_id') or column_lower == 'id':
            return True

        for keyword in FieldAnalyzer.IDENTIFIER_KEYWORDS:
            if keyword in column_lower:
                return True

        return False

    @staticmethod
    def analyze_field(
        column_name: str,
        data_type: str,
        is_primary_key: bool = False,
        is_foreign_key: bool = False,
        is_unique: bool = False,
        distinct_count: Optional[int] = None,
        total_count: Optional[int] = None
    ) -> FieldAnalysisResult:
        """
        分析字段类型

        Args:
            column_name: 列名
            data_type: 数据类型
            is_primary_key: 是否主键
            is_foreign_key: 是否外键
            is_unique: 是否唯一
            distinct_count: 不同值数量
            total_count: 总记录数

        Returns:
            FieldAnalysisResult
        """
        column_lower = FieldAnalyzer.clean_column_name(column_name)
        display_name = FieldAnalyzer.generate_display_name(column_name)

        #  判断优先级：Spatial > Identifier > Timestamp > Measure > Dimension

        #  空间字段（最高优先级，数据类型非常明确）
        if FieldAnalyzer.is_spatial_type(data_type):
            return FieldAnalysisResult(
                field_type='spatial',
                display_name=display_name,
                confidence_score=0.99,
                suggested_synonyms=[]
            )

        #  标识字段
        if FieldAnalyzer.is_identifier_type(column_name, is_primary_key, is_unique):
            return FieldAnalysisResult(
                field_type='identifier',
                display_name=display_name,
                confidence_score=0.95 if is_primary_key else 0.85,
                is_unique=is_primary_key or is_unique,
                suggested_synonyms=[]
            )

        #  时间戳字段
        if FieldAnalyzer.is_timestamp_type(column_name, data_type):
            return FieldAnalysisResult(
                field_type='timestamp',
                display_name=display_name,
                confidence_score=0.95 if data_type.lower() in FieldAnalyzer.DATETIME_TYPES else 0.75,
                dimension_type='temporal',
                suggested_synonyms=[]
            )

        #  度量字段
        if FieldAnalyzer.is_measure_type(column_name, data_type):
            # 提取单位
            unit = FieldAnalyzer.extract_unit(column_name)

            # 确定默认聚合函数（支持中英文关键词）
            if any(kw in column_lower for kw in ['count', 'num', 'quantity', 'qty', '数量', '个数', '件数', '笔数', '次数', '人数']):
                default_agg = 'SUM'
                allowed_aggs = ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN']
            elif any(kw in column_lower for kw in ['avg', 'average', '平均']):
                default_agg = 'AVG'
                allowed_aggs = ['AVG', 'MAX', 'MIN']
            elif any(kw in column_lower for kw in ['rate', 'percent', 'ratio', '比率', '比例', '百分比', '占比']):
                default_agg = 'AVG'
                allowed_aggs = ['AVG', 'MAX', 'MIN']
            elif any(kw in column_lower for kw in ['max', 'maximum', '最大', '最高']):
                default_agg = 'MAX'
                allowed_aggs = ['MAX', 'MIN', 'AVG']
            elif any(kw in column_lower for kw in ['min', 'minimum', '最小', '最低']):
                default_agg = 'MIN'
                allowed_aggs = ['MIN', 'MAX', 'AVG']
            else:
                default_agg = 'SUM'
                allowed_aggs = ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN']

            return FieldAnalysisResult(
                field_type='measure',
                display_name=display_name,
                confidence_score=0.90,
                default_aggregation=default_agg,
                allowed_aggregations=allowed_aggs,
                unit=unit,
                is_additive=default_agg == 'SUM',
                suggested_synonyms=[]
            )

        #  维度字段（默认）
        # 判断维度类型
        if data_type.lower() in FieldAnalyzer.STRING_TYPES:
            # 根据distinct count判断
            if distinct_count and total_count:
                cardinality = distinct_count / total_count

                if cardinality < 0.01:  # 低基数，分类维度
                    dim_type = 'categorical'
                elif cardinality > 0.8:  # 高基数，可能是层级或标识
                    dim_type = 'hierarchical'
                else:
                    dim_type = 'categorical'
            else:
                dim_type = 'categorical'
        else:
            dim_type = 'categorical'

        return FieldAnalysisResult(
            field_type='dimension',
            display_name=display_name,
            confidence_score=0.70,
            dimension_type=dim_type,
            suggested_synonyms=[]
        )

    @staticmethod
    def batch_analyze_table(
        columns: List[Dict]
    ) -> List[Tuple[str, FieldAnalysisResult]]:
        """
        批量分析表的所有列

        Args:
            columns: 列信息列表，每个dict包含：
                - column_id
                - column_name
                - data_type
                - is_primary_key
                - is_foreign_key
                - distinct_count
                - ...

        Returns:
            List of (column_id, FieldAnalysisResult)
        """
        results = []

        for col in columns:
            result = FieldAnalyzer.analyze_field(
                column_name=col['column_name'],
                data_type=col['data_type'],
                is_primary_key=col.get('is_primary_key', False),
                is_foreign_key=col.get('is_foreign_key', False),
                is_unique=False,  # TODO: 从数据库获取
                distinct_count=col.get('distinct_count'),
                total_count=col.get('total_count')
            )

            results.append((col['column_id'], result))

        return results


def test_analyzer():
    """测试字段分析器"""
    test_cases = [
        # (列名, 数据类型, 是否主键, 预期类型)
        # 英文测试用例
        ('user_id', 'int', True, 'identifier'),
        ('customer_name', 'varchar', False, 'dimension'),
        ('order_amount', 'decimal', False, 'measure'),
        ('order_date', 'datetime', False, 'timestamp'),
        ('created_at', 'datetime', False, 'timestamp'),
        ('product_count', 'int', False, 'measure'),
        ('status', 'varchar', False, 'dimension'),
        ('email', 'varchar', False, 'dimension'),
        # 中文测试用例
        ('总价', 'numeric', False, 'measure'),
        ('单价', 'decimal', False, 'measure'),
        ('数量', 'int', False, 'measure'),
        ('用户编号', 'int', False, 'identifier'),
        ('姓名', 'varchar', False, 'dimension'),
        ('创建时间', 'datetime', False, 'timestamp'),
        ('地区', 'varchar', False, 'dimension'),
        ('评分', 'decimal', False, 'measure'),
    ]

    print("\n 字段分析器测试\n" + "=" * 50)

    for col_name, data_type, is_pk, expected_type in test_cases:
        result = FieldAnalyzer.analyze_field(col_name, data_type, is_pk)

        status = "" if result.field_type == expected_type else ""

        print(f"{status} {col_name:20} | {data_type:15} | "
              f"{result.field_type:12} (预期: {expected_type:12}) | "
              f"置信度: {result.confidence_score:.2f}")

        if result.field_type == 'measure':
            print(f"   └─ 默认聚合: {result.default_aggregation}, 单位: {result.unit}")

    print("=" * 50 + "\n")


if __name__ == "__main__":
    test_analyzer()

