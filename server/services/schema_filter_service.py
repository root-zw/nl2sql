"""
Schema预过滤服务
根据用户权限过滤可见的表和字段，用于Prompt构建和向量检索
"""

import json
from typing import Optional, List, Dict, Any, Set
from uuid import UUID
from dataclasses import dataclass, field
import structlog

from server.models.permission import ColumnAccessMode

logger = structlog.get_logger()


@dataclass
class FilteredColumn:
    """过滤后的列信息"""
    field_id: UUID
    column_name: str
    display_name: str
    field_type: str
    data_type: str
    description: Optional[str] = None
    is_masked: bool = False  # 是否需要脱敏
    restricted_filter: bool = False  # 是否禁止WHERE
    restricted_aggregate: bool = False  # 是否禁止聚合


@dataclass
class FilteredTable:
    """过滤后的表信息"""
    table_id: UUID
    table_name: str
    schema_name: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: List[FilteredColumn] = field(default_factory=list)


@dataclass
class FilteredSchema:
    """过滤后的Schema"""
    connection_id: UUID
    tables: List[FilteredTable] = field(default_factory=list)
    row_filters: Dict[str, str] = field(default_factory=dict)  # table_id -> WHERE条件


class SchemaFilterService:
    """Schema预过滤服务 - 根据权限过滤可见Schema"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def get_filtered_schema(
        self,
        connection_id: UUID,
        user_id: UUID,
        include_row_filters: bool = True
    ) -> FilteredSchema:
        """
        获取用户有权限的Schema
        
        Args:
            connection_id: 数据库连接ID
            user_id: 用户ID
            include_row_filters: 是否包含行级过滤条件
            
        Returns:
            FilteredSchema: 只包含有权限的表和列
        """
        # 1. 获取用户的数据角色
        user_roles = await self._get_user_roles(user_id, connection_id)
        
        if not user_roles:
            logger.info(f"用户 {user_id} 在连接 {connection_id} 上没有数据角色")
            return FilteredSchema(connection_id=connection_id, tables=[], row_filters={})
        
        role_ids = [r['role_id'] for r in user_roles]
        
        # 2. 获取所有角色可访问的表（并集）
        allowed_tables = await self._get_allowed_tables(role_ids)
        
        if not allowed_tables:
            return FilteredSchema(connection_id=connection_id, tables=[], row_filters={})
        
        # 3. 获取每个表的列权限配置
        column_permissions = await self._get_column_permissions(role_ids, list(allowed_tables.keys()))
        
        # 4. 构建过滤后的Schema
        filtered_tables = []
        for table_id, table_info in allowed_tables.items():
            # 获取表的所有启用字段
            active_fields = await self._get_active_fields(table_id)
            
            # 应用列权限过滤
            filtered_columns = self._apply_column_permissions(
                active_fields, 
                column_permissions.get(str(table_id), {})
            )
            
            if filtered_columns:  # 至少有一列可见才包含此表
                filtered_tables.append(FilteredTable(
                    table_id=table_id,
                    table_name=table_info['table_name'],
                    schema_name=table_info.get('schema_name'),
                    display_name=table_info.get('display_name'),
                    description=table_info.get('description'),
                    columns=filtered_columns
                ))
        
        # 5. 获取行级过滤条件
        row_filters = {}
        if include_row_filters:
            user_attrs = await self._get_user_attributes(user_id)
            row_filters = await self._get_row_filters(role_ids, user_attrs)
        
        return FilteredSchema(
            connection_id=connection_id,
            tables=filtered_tables,
            row_filters=row_filters
        )
    
    async def get_allowed_table_ids(
        self,
        connection_id: UUID,
        user_id: UUID
    ) -> Set[UUID]:
        """获取用户可访问的表ID集合（用于向量检索过滤）"""
        user_roles = await self._get_user_roles(user_id, connection_id)
        if not user_roles:
            return set()
        
        role_ids = [r['role_id'] for r in user_roles]
        allowed_tables = await self._get_allowed_tables(role_ids)
        return set(allowed_tables.keys())
    
    async def get_allowed_field_ids(
        self,
        connection_id: UUID,
        user_id: UUID,
        table_id: Optional[UUID] = None
    ) -> Set[UUID]:
        """获取用户可访问的字段ID集合（用于向量检索过滤）"""
        user_roles = await self._get_user_roles(user_id, connection_id)
        if not user_roles:
            return set()
        
        role_ids = [r['role_id'] for r in user_roles]
        
        # 获取可访问的表
        if table_id:
            table_ids = [table_id]
        else:
            allowed_tables = await self._get_allowed_tables(role_ids)
            table_ids = list(allowed_tables.keys())
        
        if not table_ids:
            return set()
        
        # 获取列权限
        column_permissions = await self._get_column_permissions(role_ids, table_ids)
        
        allowed_fields = set()
        for tid in table_ids:
            active_fields = await self._get_active_fields(tid)
            perm = column_permissions.get(str(tid), {})
            
            for field in active_fields:
                if self._is_column_visible(field['field_id'], perm, active_fields):
                    allowed_fields.add(field['field_id'])
        
        return allowed_fields
    
    async def get_column_restrictions(
        self,
        connection_id: UUID,
        user_id: UUID
    ) -> Dict[str, Set[UUID]]:
        """获取用户的列级权限限制
        
        Args:
            connection_id: 数据库连接ID
            user_id: 用户ID
            
        Returns:
            {
                'restricted_filter_column_ids': Set[UUID],  # 禁止 WHERE 的列
                'restricted_aggregate_column_ids': Set[UUID],  # 禁止聚合的列
                'masked_column_ids': Set[UUID]  # 需要脱敏的列
            }
        """
        user_roles = await self._get_user_roles(user_id, connection_id)
        if not user_roles:
            return {
                'restricted_filter_column_ids': set(),
                'restricted_aggregate_column_ids': set(),
                'masked_column_ids': set()
            }
        
        role_ids = [r['role_id'] for r in user_roles]
        
        # 获取可访问的表
        allowed_tables = await self._get_allowed_tables(role_ids)
        table_ids = list(allowed_tables.keys())
        
        if not table_ids:
            return {
                'restricted_filter_column_ids': set(),
                'restricted_aggregate_column_ids': set(),
                'masked_column_ids': set()
            }
        
        # 获取列权限配置
        column_permissions = await self._get_column_permissions(role_ids, table_ids)
        
        # 合并所有表的列级限制（多角色取交集）
        restricted_filter: Optional[Set[UUID]] = None
        restricted_aggregate: Optional[Set[UUID]] = None
        masked: Optional[Set[UUID]] = None
        
        for table_key, perm in column_permissions.items():
            # restricted_filter_column_ids - 多角色取交集
            filter_ids = perm.get('restricted_filter_column_ids', set())
            if filter_ids:
                if restricted_filter is None:
                    restricted_filter = set(filter_ids)
                else:
                    restricted_filter &= filter_ids
            
            # restricted_aggregate_column_ids - 多角色取交集
            agg_ids = perm.get('restricted_aggregate_column_ids', set())
            if agg_ids:
                if restricted_aggregate is None:
                    restricted_aggregate = set(agg_ids)
                else:
                    restricted_aggregate &= agg_ids
            
            # masked_column_ids - 多角色取交集
            mask_ids = perm.get('masked_column_ids', set())
            if mask_ids:
                if masked is None:
                    masked = set(mask_ids)
                else:
                    masked &= mask_ids
        
        return {
            'restricted_filter_column_ids': restricted_filter or set(),
            'restricted_aggregate_column_ids': restricted_aggregate or set(),
            'masked_column_ids': masked or set()
        }
    
    async def _get_user_roles(self, user_id: UUID, connection_id: UUID) -> List[Dict]:
        """
        获取用户在指定连接上的数据角色
        
        逻辑（简化后）：
        1. scope_type='all' 的角色直接包含
        2. scope_type='limited' 的角色通过表权限确定是否有该连接的访问权限
        """
        query = """
            SELECT DISTINCT dr.role_id, dr.role_name, dr.role_code, dr.scope_type
            FROM user_data_roles udr
            JOIN data_roles dr ON udr.role_id = dr.role_id
            LEFT JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
            LEFT JOIN db_tables t ON rtp.table_id = t.table_id AND t.connection_id = $2
            WHERE udr.user_id = $1 
              AND udr.is_active = TRUE 
              AND dr.is_active = TRUE
              AND (udr.expires_at IS NULL OR udr.expires_at > CURRENT_TIMESTAMP)
              AND (dr.scope_type = 'all' OR t.table_id IS NOT NULL)
        """
        rows = await self.db.fetch(query, user_id, connection_id)
        return [dict(row) for row in rows]
    
    async def _get_allowed_tables(self, role_ids: List[UUID]) -> Dict[UUID, Dict]:
        """获取所有角色可访问的表（并集）"""
        if not role_ids:
            return {}
        
        query = """
            SELECT DISTINCT t.table_id, t.table_name, t.schema_name, t.display_name, t.description
            FROM role_table_permissions rtp
            JOIN db_tables t ON rtp.table_id = t.table_id
            WHERE rtp.role_id = ANY($1) AND rtp.can_query = TRUE AND t.is_included = TRUE
        """
        rows = await self.db.fetch(query, role_ids)
        return {row['table_id']: dict(row) for row in rows}
    
    async def _get_column_permissions(
        self, 
        role_ids: List[UUID], 
        table_ids: List[UUID]
    ) -> Dict[str, Dict]:
        """获取列权限配置（合并多角色权限）"""
        if not role_ids or not table_ids:
            return {}
        
        query = """
            SELECT table_id, column_access_mode, included_column_ids, excluded_column_ids,
                   masked_column_ids, restricted_filter_column_ids, restricted_aggregate_column_ids
            FROM role_table_permissions
            WHERE role_id = ANY($1) AND table_id = ANY($2)
        """
        rows = await self.db.fetch(query, role_ids, table_ids)
        
        # 合并多角色权限
        merged = {}
        for row in rows:
            table_key = str(row['table_id'])
            if table_key not in merged:
                merged[table_key] = {
                    'column_access_mode': row['column_access_mode'],
                    'included_column_ids': set(row['included_column_ids'] or []),
                    'excluded_column_ids': set(row['excluded_column_ids'] or []),
                    'masked_column_ids': set(row['masked_column_ids'] or []),
                    'restricted_filter_column_ids': set(row['restricted_filter_column_ids'] or []),
                    'restricted_aggregate_column_ids': set(row['restricted_aggregate_column_ids'] or [])
                }
            else:
                # 多角色合并规则：
                # - 可见列取并集
                # - 脱敏/限制列取交集
                existing = merged[table_key]
                existing['included_column_ids'].update(row['included_column_ids'] or [])
                existing['excluded_column_ids'] &= set(row['excluded_column_ids'] or [])
                existing['masked_column_ids'] &= set(row['masked_column_ids'] or [])
                existing['restricted_filter_column_ids'] &= set(row['restricted_filter_column_ids'] or [])
                existing['restricted_aggregate_column_ids'] &= set(row['restricted_aggregate_column_ids'] or [])
        
        return merged
    
    async def _get_active_fields(self, table_id: UUID) -> List[Dict]:
        """获取表的所有启用字段"""
        query = """
            SELECT f.field_id, f.display_name, f.field_type, f.description,
                   c.column_name, c.data_type
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            WHERE c.table_id = $1 AND f.is_active = TRUE
            ORDER BY f.priority DESC, f.display_name
        """
        rows = await self.db.fetch(query, table_id)
        return [dict(row) for row in rows]
    
    def _apply_column_permissions(
        self, 
        fields: List[Dict], 
        permissions: Dict
    ) -> List[FilteredColumn]:
        """应用列权限过滤"""
        if not permissions:
            # 无权限配置，返回所有字段
            return [
                FilteredColumn(
                    field_id=f['field_id'],
                    column_name=f['column_name'],
                    display_name=f['display_name'],
                    field_type=f['field_type'],
                    data_type=f['data_type'],
                    description=f.get('description')
                )
                for f in fields
            ]
        
        result = []
        for f in fields:
            field_id = f['field_id']
            
            # 检查可见性
            if not self._is_column_visible(field_id, permissions, fields):
                continue
            
            result.append(FilteredColumn(
                field_id=field_id,
                column_name=f['column_name'],
                display_name=f['display_name'],
                field_type=f['field_type'],
                data_type=f['data_type'],
                description=f.get('description'),
                is_masked=field_id in permissions.get('masked_column_ids', set()),
                restricted_filter=field_id in permissions.get('restricted_filter_column_ids', set()),
                restricted_aggregate=field_id in permissions.get('restricted_aggregate_column_ids', set())
            ))
        
        return result
    
    def _is_column_visible(
        self, 
        field_id: UUID, 
        permissions: Dict,
        all_fields: List[Dict]
    ) -> bool:
        """判断列是否可见"""
        mode = permissions.get('column_access_mode', 'blacklist')
        
        if mode == 'whitelist':
            # 白名单模式：必须在included中
            return field_id in permissions.get('included_column_ids', set())
        else:
            # 黑名单模式：不在excluded中
            return field_id not in permissions.get('excluded_column_ids', set())
    
    async def _get_user_attributes(self, user_id: UUID) -> Dict[str, str]:
        """获取用户属性"""
        query = "SELECT attribute_name, attribute_value FROM user_attributes WHERE user_id = $1"
        rows = await self.db.fetch(query, user_id)
        return {row['attribute_name']: row['attribute_value'] for row in rows}
    
    async def _get_row_filters(
        self, 
        role_ids: List[UUID], 
        user_attrs: Dict[str, str]
    ) -> Dict[str, str]:
        """获取行级过滤条件并解析"""
        if not role_ids:
            return {}
        
        query = """
            SELECT rrf.table_id, rrf.filter_definition
            FROM role_row_filters rrf
            WHERE rrf.role_id = ANY($1) AND rrf.is_active = TRUE
            ORDER BY rrf.priority DESC
        """
        rows = await self.db.fetch(query, role_ids)
        
        # 按表合并过滤条件（OR逻辑）
        filters_by_table = {}
        for row in rows:
            table_key = str(row['table_id']) if row['table_id'] else '__all__'
            filter_def = row['filter_definition']
            if isinstance(filter_def, str):
                filter_def = json.loads(filter_def)
            
            parsed = self._parse_filter_definition(filter_def, user_attrs)
            if parsed:
                if table_key in filters_by_table:
                    # 多角色OR合并
                    filters_by_table[table_key] = f"({filters_by_table[table_key]}) OR ({parsed})"
                else:
                    filters_by_table[table_key] = parsed
        
        return filters_by_table
    
    def _parse_filter_definition(
        self, 
        filter_def: Dict[str, Any], 
        user_attrs: Dict[str, str]
    ) -> Optional[str]:
        """解析过滤条件定义为SQL片段"""
        conditions = filter_def.get('conditions', [])
        logic = filter_def.get('logic', 'AND')
        
        sql_parts = []
        for cond in conditions:
            field_name = cond.get('field_name')
            operator = cond.get('operator', '=')
            value_type = cond.get('value_type', 'static')
            value = cond.get('value')
            
            if value_type == 'user_attr':
                attr_value = user_attrs.get(value)
                if attr_value is None:
                    continue
                # 处理数组
                if attr_value.startswith('['):
                    try:
                        parsed_list = json.loads(attr_value)
                        if operator.upper() == 'IN':
                            quoted = ', '.join([f"'{v}'" for v in parsed_list])
                            sql_parts.append(f"{field_name} IN ({quoted})")
                        continue
                    except json.JSONDecodeError:
                        pass
                value = attr_value
            elif value_type == 'expression':
                sql_parts.append(f"{field_name} {operator} {value}")
                continue
            
            if operator.upper() == 'IN':
                if isinstance(value, list):
                    quoted = ', '.join([f"'{v}'" for v in value])
                    sql_parts.append(f"{field_name} IN ({quoted})")
                else:
                    sql_parts.append(f"{field_name} IN ('{value}')")
            elif operator.upper() == 'LIKE':
                sql_parts.append(f"{field_name} LIKE '{value}'")
            else:
                sql_parts.append(f"{field_name} {operator} '{value}'")
        
        if not sql_parts:
            return None
        
        return f" {logic} ".join(sql_parts)
    
    def format_schema_for_prompt(self, schema: FilteredSchema) -> str:
        """将过滤后的Schema格式化为Prompt文本"""
        lines = []
        for table in schema.tables:
            table_desc = f"- **{table.display_name or table.table_name}**"
            if table.schema_name:
                table_desc += f" ({table.schema_name}.{table.table_name})"
            else:
                table_desc += f" ({table.table_name})"
            
            if table.description:
                table_desc += f": {table.description}"
            
            lines.append(table_desc)
            
            col_descs = []
            for col in table.columns:
                col_info = f"  - {col.display_name} ({col.column_name}, {col.data_type})"
                if col.description:
                    col_info += f": {col.description}"
                if col.is_masked:
                    col_info += " [脱敏]"
                col_descs.append(col_info)
            
            lines.extend(col_descs)
            lines.append("")
        
        return "\n".join(lines)

