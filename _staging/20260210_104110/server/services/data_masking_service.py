"""
数据脱敏服务
对查询结果中的敏感列进行脱敏处理
"""

import re
from typing import Optional, List, Dict, Any, Set
from uuid import UUID
from enum import Enum
import structlog

logger = structlog.get_logger()


class MaskingType(str, Enum):
    """脱敏类型"""
    PHONE = "phone"           # 手机号
    ID_CARD = "id_card"       # 身份证
    NAME = "name"             # 姓名
    EMAIL = "email"           # 邮箱
    BANK_CARD = "bank_card"   # 银行卡
    ADDRESS = "address"       # 地址
    FULL = "full"             # 完全隐藏
    PARTIAL = "partial"       # 部分隐藏
    AUTO = "auto"             # 自动识别


class DataMaskingService:
    """数据脱敏服务"""
    
    # 脱敏规则
    MASKING_RULES = {
        MaskingType.PHONE: {
            'pattern': r'^(\d{3})\d{4}(\d{4})$',
            'replacement': r'\1****\2',
            'example': '138****5678'
        },
        MaskingType.ID_CARD: {
            'pattern': r'^(\d{6})\d{8}(\d{4})$',
            'replacement': r'\1********\2',
            'example': '420111********1234'
        },
        MaskingType.NAME: {
            'pattern': r'^(.)(.*)$',
            'replacement': lambda m: m.group(1) + '*' * len(m.group(2)),
            'example': '张**'
        },
        MaskingType.EMAIL: {
            'pattern': r'^(.{2})(.*)(@.*)$',
            'replacement': r'\1****\3',
            'example': 'te****@example.com'
        },
        MaskingType.BANK_CARD: {
            'pattern': r'^(\d{4})\d+(\d{4})$',
            'replacement': r'\1****\2',
            'example': '6222****7890'
        },
        MaskingType.ADDRESS: {
            'pattern': r'^(.{6})(.*)$',
            'replacement': lambda m: m.group(1) + '****',
            'example': '北京市海淀区****'
        },
        MaskingType.FULL: {
            'pattern': r'.*',
            'replacement': '******',
            'example': '******'
        },
        MaskingType.PARTIAL: {
            'pattern': r'^(.{2})(.*)(.{2})$',
            'replacement': lambda m: m.group(1) + '*' * min(len(m.group(2)), 4) + m.group(3),
            'example': 'ab****cd'
        }
    }
    
    # 字段名与脱敏类型的映射（用于自动识别）
    FIELD_TYPE_MAPPING = {
        # 手机号
        'phone': MaskingType.PHONE,
        'mobile': MaskingType.PHONE,
        'tel': MaskingType.PHONE,
        'telephone': MaskingType.PHONE,
        '手机': MaskingType.PHONE,
        '电话': MaskingType.PHONE,
        
        # 身份证
        'id_card': MaskingType.ID_CARD,
        'idcard': MaskingType.ID_CARD,
        'id_number': MaskingType.ID_CARD,
        'identity': MaskingType.ID_CARD,
        '身份证': MaskingType.ID_CARD,
        
        # 姓名
        'name': MaskingType.NAME,
        'user_name': MaskingType.NAME,
        'real_name': MaskingType.NAME,
        '姓名': MaskingType.NAME,
        
        # 邮箱
        'email': MaskingType.EMAIL,
        'mail': MaskingType.EMAIL,
        '邮箱': MaskingType.EMAIL,
        
        # 银行卡
        'bank_card': MaskingType.BANK_CARD,
        'card_no': MaskingType.BANK_CARD,
        'account': MaskingType.BANK_CARD,
        '银行卡': MaskingType.BANK_CARD,
        
        # 地址
        'address': MaskingType.ADDRESS,
        'addr': MaskingType.ADDRESS,
        '地址': MaskingType.ADDRESS,
    }
    
    def __init__(self, db_pool=None):
        self.db = db_pool
        self._masking_config: Dict[UUID, Dict[str, MaskingType]] = {}
    
    async def load_masking_config(
        self, 
        table_id: UUID, 
        masked_column_ids: Set[UUID]
    ) -> Dict[str, MaskingType]:
        """
        加载脱敏配置
        
        Args:
            table_id: 表ID
            masked_column_ids: 需要脱敏的字段ID集合
            
        Returns:
            Dict[column_name, MaskingType]
        """
        if not masked_column_ids:
            return {}
        
        # 获取字段名和列名
        query = """
            SELECT f.field_id, f.display_name, c.column_name
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            WHERE f.field_id = ANY($1)
        """
        rows = await self.db.fetch(query, list(masked_column_ids))
        
        config = {}
        for row in rows:
            column_name = row['column_name']
            display_name = row['display_name'].lower()
            
            # 自动识别脱敏类型
            masking_type = self._detect_masking_type(column_name, display_name)
            config[column_name] = masking_type
        
        self._masking_config[table_id] = config
        return config
    
    def _detect_masking_type(self, column_name: str, display_name: str) -> MaskingType:
        """根据字段名自动识别脱敏类型"""
        # 先检查列名
        col_lower = column_name.lower()
        for key, mtype in self.FIELD_TYPE_MAPPING.items():
            if key in col_lower:
                return mtype
        
        # 再检查显示名
        for key, mtype in self.FIELD_TYPE_MAPPING.items():
            if key in display_name:
                return mtype
        
        # 默认使用部分隐藏
        return MaskingType.PARTIAL
    
    def mask_value(self, value: Any, masking_type: MaskingType) -> str:
        """
        对单个值进行脱敏
        
        Args:
            value: 原始值
            masking_type: 脱敏类型
            
        Returns:
            脱敏后的值
        """
        if value is None:
            return None
        
        str_value = str(value)
        if not str_value or str_value.strip() == '':
            return str_value
        
        rule = self.MASKING_RULES.get(masking_type, self.MASKING_RULES[MaskingType.PARTIAL])
        pattern = rule['pattern']
        replacement = rule['replacement']
        
        try:
            if callable(replacement):
                match = re.match(pattern, str_value)
                if match:
                    return replacement(match)
                return str_value[:2] + '****' + str_value[-2:] if len(str_value) > 4 else '****'
            else:
                result = re.sub(pattern, replacement, str_value)
                if result == str_value and masking_type != MaskingType.FULL:
                    # 没有匹配到模式，使用默认脱敏
                    return str_value[:2] + '****' + str_value[-2:] if len(str_value) > 4 else '****'
                return result
        except Exception as e:
            logger.warning(f"脱敏处理失败: {e}")
            return '******'
    
    def mask_row(
        self, 
        row: Dict[str, Any], 
        masking_config: Dict[str, MaskingType]
    ) -> Dict[str, Any]:
        """
        对一行数据进行脱敏
        
        Args:
            row: 原始数据行
            masking_config: 脱敏配置 {column_name: MaskingType}
            
        Returns:
            脱敏后的数据行
        """
        if not masking_config:
            return row
        
        masked_row = dict(row)
        for column_name, masking_type in masking_config.items():
            if column_name in masked_row:
                masked_row[column_name] = self.mask_value(
                    masked_row[column_name], 
                    masking_type
                )
        
        return masked_row
    
    def mask_results(
        self, 
        results: List[Dict[str, Any]], 
        masking_config: Dict[str, MaskingType]
    ) -> List[Dict[str, Any]]:
        """
        对查询结果进行批量脱敏
        
        Args:
            results: 查询结果列表
            masking_config: 脱敏配置
            
        Returns:
            脱敏后的结果列表
        """
        if not masking_config or not results:
            return results
        
        return [self.mask_row(row, masking_config) for row in results]
    
    @staticmethod
    def get_masking_examples() -> Dict[str, str]:
        """获取所有脱敏类型的示例"""
        return {
            mtype.value: rule['example']
            for mtype, rule in DataMaskingService.MASKING_RULES.items()
        }


class ColumnUsageValidator:
    """列使用权限验证器 - 防止推断攻击"""
    
    def __init__(self, db_pool=None):
        self.db = db_pool
    
    async def validate_query(
        self,
        table_id: UUID,
        where_columns: Set[str],
        aggregate_columns: Set[str],
        group_by_columns: Set[str],
        order_by_columns: Set[str],
        restricted_filter: Set[UUID],
        restricted_aggregate: Set[UUID],
        restricted_group_by: Set[UUID],
        restricted_order_by: Set[UUID]
    ) -> Dict[str, Any]:
        """
        验证查询中的列使用是否合规
        
        Returns:
            {
                'valid': bool,
                'errors': List[str],
                'warnings': List[str]
            }
        """
        errors = []
        warnings = []
        
        # 获取列名到field_id的映射
        if self.db:
            field_mapping = await self._get_field_mapping(table_id)
        else:
            field_mapping = {}
        
        # 检查WHERE列
        for col in where_columns:
            field_id = field_mapping.get(col)
            if field_id and field_id in restricted_filter:
                errors.append(f"列 '{col}' 不允许用于筛选条件（防止推断攻击）")
        
        # 检查聚合列
        for col in aggregate_columns:
            field_id = field_mapping.get(col)
            if field_id and field_id in restricted_aggregate:
                errors.append(f"列 '{col}' 不允许用于聚合计算")
        
        # 检查GROUP BY列
        for col in group_by_columns:
            field_id = field_mapping.get(col)
            if field_id and field_id in restricted_group_by:
                errors.append(f"列 '{col}' 不允许用于分组")
        
        # 检查ORDER BY列
        for col in order_by_columns:
            field_id = field_mapping.get(col)
            if field_id and field_id in restricted_order_by:
                errors.append(f"列 '{col}' 不允许用于排序")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
    
    async def _get_field_mapping(self, table_id: UUID) -> Dict[str, UUID]:
        """获取列名到field_id的映射"""
        query = """
            SELECT c.column_name, f.field_id
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            WHERE c.table_id = $1 AND f.is_active = TRUE
        """
        rows = await self.db.fetch(query, table_id)
        return {row['column_name']: row['field_id'] for row in rows}

