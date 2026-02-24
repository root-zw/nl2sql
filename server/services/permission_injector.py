"""
权限注入服务
将数据权限（行级过滤、列权限）注入到查询流程中
"""

import json
from typing import Optional, List, Dict, Any, Set, Tuple, Union
from uuid import UUID
from copy import deepcopy
import structlog

from server.models.ir import IntermediateRepresentation, FilterCondition
from server.models.permission import ColumnAccessMode

logger = structlog.get_logger()


def _convert_row(row) -> Dict[str, Any]:
    """将asyncpg行记录转换为字典，处理UUID类型转换"""
    if row is None:
        return None
    result = {}
    for key, value in dict(row).items():
        # 将asyncpg的UUID转换为Python标准UUID
        if hasattr(value, 'hex') and hasattr(value, 'int') and not isinstance(value, (int, float)):
            result[key] = UUID(str(value))
        elif isinstance(value, list):
            result[key] = [UUID(str(v)) if hasattr(v, 'hex') and hasattr(v, 'int') and not isinstance(v, (int, float)) else v for v in value]
        else:
            result[key] = value
    return result


class PermissionInjector:
    """权限注入器 - 将数据权限应用到IR"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def inject_permissions(
        self,
        ir: IntermediateRepresentation,
        user_id: UUID,
        connection_id: UUID
    ) -> Tuple[IntermediateRepresentation, Dict[str, Any]]:
        """
        将用户权限注入到IR中
        
        Args:
            ir: 原始IR
            user_id: 用户ID
            connection_id: 数据库连接ID
            
        Returns:
            (修改后的IR, 权限应用信息)
        """
        # 获取用户数据角色
        user_roles = await self._get_user_roles(user_id, connection_id)
        
        if not user_roles:
            logger.debug(f"用户 {user_id} 没有数据角色，跳过权限注入")
            return ir, {"applied": False, "reason": "no_data_roles"}
        
        role_ids = [r['role_id'] for r in user_roles]
        
        # 获取用户属性
        user_attrs = await self._get_user_attributes(user_id)
        
        # 获取行级过滤规则
        row_filters = await self._get_row_filters(role_ids)
        
        if not row_filters:
            logger.debug("没有配置行级过滤规则，跳过注入")
            return ir, {"applied": False, "reason": "no_row_filters"}
        
        # 深拷贝IR避免修改原始对象
        modified_ir = deepcopy(ir)
        
        # 解析并注入过滤条件
        injected_filters = []
        for filter_rule in row_filters:
            filter_def = filter_rule['filter_definition']
            if isinstance(filter_def, str):
                filter_def = json.loads(filter_def)
            
            table_id = filter_rule.get('table_id')
            
            # 将过滤定义转换为IR FilterCondition
            parsed_conditions = self._parse_filter_definition(filter_def, user_attrs)
            
            for cond in parsed_conditions:
                # 创建FilterCondition对象
                filter_cond = FilterCondition(
                    field=cond['field_name'],
                    op=self._normalize_operator(cond['operator']),
                    value=cond['value']
                )
                injected_filters.append(filter_cond)
        
        # 智能合并同字段条件，消除冗余
        permission_conflicts = []  # 记录所有权限冲突
        
        if injected_filters:
            if modified_ir.filters is None:
                modified_ir.filters = []
            
            merged_count = 0
            for perm_filter in injected_filters:
                merged, modified_ir.filters, need_append, conflict_info = self._smart_merge_filter(
                    perm_filter, modified_ir.filters
                )
                if conflict_info:
                    # 记录权限冲突
                    permission_conflicts.append(conflict_info)
                elif merged:
                    merged_count += 1
                    if need_append:
                        # 合并时移除了IR条件，但权限条件仍需追加
                        modified_ir.filters.append(perm_filter)
                else:
                    # 没有合并，直接追加
                    modified_ir.filters.append(perm_filter)
            
            logger.info(
                "权限过滤条件已注入IR",
                user_id=str(user_id),
                injected_count=len(injected_filters),
                merged_count=merged_count,
                total_filters=len(modified_ir.filters),
                conflicts=len(permission_conflicts)
            )
        
        # 收集被权限限制的字段信息
        restricted_fields = {}
        for perm_filter in injected_filters:
            field_name = perm_filter.field
            if perm_filter.op.upper() == 'IN' and isinstance(perm_filter.value, list):
                restricted_fields[field_name] = perm_filter.value
            elif perm_filter.op == '=':
                restricted_fields[field_name] = [perm_filter.value]
        
        result_info = {
            "applied": True,
            "injected_filters": len(injected_filters),
            "user_roles": [r['role_name'] for r in user_roles],
            "user_attributes": user_attrs,
            "restricted_fields": restricted_fields  # 告知哪些字段被权限限制
        }
        
        # 如果存在权限冲突，标记并返回冲突详情
        if permission_conflicts:
            result_info["permission_conflict"] = True
            result_info["conflict_details"] = permission_conflicts
            logger.warning(
                "检测到权限冲突，用户查询的值不在权限允许范围内",
                user_id=str(user_id),
                conflicts=permission_conflicts
            )
        
        return modified_ir, result_info
    
    async def check_table_permission(
        self,
        user_id: UUID,
        connection_id: UUID,
        table_ids: List[str]
    ) -> Dict[str, bool]:
        """
        检查用户对表的访问权限
        
        Returns:
            {table_id: has_permission}
        """
        user_roles = await self._get_user_roles(user_id, connection_id)
        
        if not user_roles:
            # 没有数据角色，默认拒绝所有
            return {tid: False for tid in table_ids}
        
        role_ids = [r['role_id'] for r in user_roles]
        
        # 查询允许的表
        query = """
            SELECT DISTINCT table_id::text
            FROM role_table_permissions
            WHERE role_id = ANY($1) AND can_query = TRUE
        """
        rows = await self.db.fetch(query, role_ids)
        allowed_tables = {str(row['table_id']) for row in rows}
        
        return {tid: tid in allowed_tables for tid in table_ids}
    
    async def get_column_restrictions(
        self,
        user_id: UUID,
        connection_id: UUID,
        table_id: str
    ) -> Dict[str, Any]:
        """
        获取用户对表的列级限制
        
        Returns:
            {
                'visible_columns': Set[str],  # 可见列
                'masked_columns': Set[str],   # 需脱敏列
                'restricted_filter_columns': Set[str],  # 禁止WHERE的列
                'restricted_aggregate_columns': Set[str]  # 禁止聚合的列
            }
        """
        user_roles = await self._get_user_roles(user_id, connection_id)
        
        if not user_roles:
            return {
                'visible_columns': set(),
                'masked_columns': set(),
                'restricted_filter_columns': set(),
                'restricted_aggregate_columns': set()
            }
        
        role_ids = [r['role_id'] for r in user_roles]
        
        # 获取列权限配置
        query = """
            SELECT column_access_mode, included_column_ids, excluded_column_ids,
                   masked_column_ids, restricted_filter_column_ids, restricted_aggregate_column_ids
            FROM role_table_permissions
            WHERE role_id = ANY($1) AND table_id = $2::uuid
        """
        rows = await self.db.fetch(query, role_ids, table_id)
        
        if not rows:
            # 没有配置，返回空限制
            return {
                'visible_columns': None,  # None表示不限制
                'masked_columns': set(),
                'restricted_filter_columns': set(),
                'restricted_aggregate_columns': set()
            }
        
        # 合并多角色权限
        # 注意：对于 included，使用并集（多角色取最大可见范围）
        # 对于 excluded/masked/restricted，使用交集（多角色取最小限制范围）
        all_included = set()
        all_excluded = None  # None 表示未初始化
        masked = None
        restricted_filter = None
        restricted_aggregate = None
        
        for row in rows:
            if row['included_column_ids']:
                all_included.update(str(cid) for cid in row['included_column_ids'])
            if row['excluded_column_ids']:
                new_excluded = set(str(cid) for cid in row['excluded_column_ids'])
                if all_excluded is None:
                    all_excluded = new_excluded
                else:
                    all_excluded &= new_excluded  # 交集
            if row['masked_column_ids']:
                new_masked = set(str(cid) for cid in row['masked_column_ids'])
                if masked is None:
                    masked = new_masked
                else:
                    masked &= new_masked  # 交集
            if row['restricted_filter_column_ids']:
                new_restricted_filter = set(str(cid) for cid in row['restricted_filter_column_ids'])
                if restricted_filter is None:
                    restricted_filter = new_restricted_filter
                else:
                    restricted_filter &= new_restricted_filter
            if row['restricted_aggregate_column_ids']:
                new_restricted_aggregate = set(str(cid) for cid in row['restricted_aggregate_column_ids'])
                if restricted_aggregate is None:
                    restricted_aggregate = new_restricted_aggregate
                else:
                    restricted_aggregate &= new_restricted_aggregate
        
        return {
            'visible_columns': all_included if all_included else None,
            'excluded_columns': all_excluded or set(),
            'masked_columns': masked or set(),
            'restricted_filter_columns': restricted_filter or set(),
            'restricted_aggregate_columns': restricted_aggregate or set()
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
        return [_convert_row(row) for row in rows]
    
    async def _get_user_attributes(self, user_id: UUID) -> Dict[str, str]:
        """获取用户属性"""
        query = "SELECT attribute_name, attribute_value FROM user_attributes WHERE user_id = $1"
        rows = await self.db.fetch(query, user_id)
        return {row['attribute_name']: row['attribute_value'] for row in rows}
    
    async def _get_row_filters(self, role_ids: List[UUID]) -> List[Dict]:
        """获取角色的行级过滤规则"""
        query = """
            SELECT filter_id, role_id, filter_name, table_id, filter_definition
            FROM role_row_filters
            WHERE role_id = ANY($1) AND is_active = TRUE
            ORDER BY priority DESC
        """
        rows = await self.db.fetch(query, role_ids)
        return [_convert_row(row) for row in rows]
    
    def _parse_filter_definition(
        self, 
        filter_def: Dict[str, Any], 
        user_attrs: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """解析过滤条件定义"""
        conditions = filter_def.get('conditions', [])
        logic = str(filter_def.get('logic', 'AND')).upper()
        result = []
        
        for cond in conditions:
            field_name = cond.get('field_name')
            if field_name is None:
                logger.warning("行级过滤条件缺少 field_name，已跳过", condition=cond)
                continue
            # asyncpg 可能将 UUID 类型还原为 pgproto 对象，统一转为字符串供后续处理
            if not isinstance(field_name, str):
                field_name = str(field_name)
            operator = cond.get('operator', '=')
            value_type = cond.get('value_type', 'static')
            value = cond.get('value')
            
            if value_type == 'user_attr':
                # 从用户属性获取值
                attr_value = user_attrs.get(value)
                if attr_value is None:
                    logger.warning(f"用户属性 '{value}' 不存在，跳过此条件")
                    continue
                # 处理数组
                if attr_value.startswith('['):
                    try:
                        value = json.loads(attr_value)
                    except json.JSONDecodeError:
                        value = attr_value
                else:
                    value = attr_value
            elif value_type == 'expression':
                # SQL表达式，保持原样
                pass
            
            result.append({
                'field_name': field_name,
                'operator': operator,
                'value': value
            })
        
        # 当逻辑为 OR 且同字段出现多条 "=" / "IN" 条件时，合并为单个 IN 以避免后续覆盖
        if logic == 'OR' and result:
            merged: List[Dict[str, Any]] = []
            grouped: Dict[str, List[Any]] = {}
            passthrough: List[Dict[str, Any]] = []

            for item in result:
                op_upper = str(item['operator']).upper()
                field = item['field_name']
                val = item['value']

                if op_upper in ('=', '=='):
                    grouped.setdefault(field, []).append(val)
                elif op_upper == 'IN' and isinstance(val, list):
                    grouped.setdefault(field, []).extend(val)
                else:
                    passthrough.append(item)

            for field, values in grouped.items():
                # 去重同时保留顺序
                deduped = []
                seen = set()
                for v in values:
                    key = str(v)
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(v)
                merged.append({
                    'field_name': field,
                    'operator': 'IN',
                    'value': deduped
                })

            # 将合并后的与无法合并的条件一起返回
            return merged + passthrough

        return result
    
    def _normalize_operator(self, op: str) -> str:
        """标准化操作符为IR支持的格式"""
        op_mapping = {
            '=': '=',
            '==': '=',
            '!=': '!=',
            '<>': '!=',
            '>': '>',
            '>=': '>=',
            '<': '<',
            '<=': '<=',
            'IN': 'IN',
            'in': 'IN',
            'NOT IN': 'NOT IN',
            'not in': 'NOT IN',
            'LIKE': 'LIKE',
            'like': 'LIKE'
        }
        return op_mapping.get(op, op)
    
    def _smart_merge_filter(
        self,
        perm_filter: FilterCondition,
        ir_filters: List[FilterCondition]
    ) -> Tuple[bool, List[FilterCondition], bool, Optional[Dict[str, Any]]]:
        """
        智能合并权限条件和IR条件
        
        当权限条件和IR条件作用于同一字段时，进行智能合并：
        - 权限 = 'A' + IR IN ['A','B','C'] → 移除IR条件，追加权限条件
        - 权限 IN ['A','B'] + IR IN ['A','B','C'] → 取交集 IN ['A','B']（不需再追加权限条件）
        - 权限 IN ['A','B'] + IR = 'C' → 权限冲突，用户查询的值不在权限范围内
        - 权限 = 'A' + IR = 'A' → 移除IR条件，追加权限条件
        - 权限 = 'A' + IR = 'B' → 权限冲突
        
        Args:
            perm_filter: 权限过滤条件
            ir_filters: IR中现有的过滤条件列表
            
        Returns:
            (是否合并成功, 更新后的IR过滤条件列表, 是否需要追加权限条件, 冲突信息)
        """
        perm_field = perm_filter.field.lower() if perm_filter.field else ""
        perm_op = perm_filter.op.upper() if perm_filter.op else ""
        perm_value = perm_filter.value
        
        new_filters = []
        merged = False
        need_append_perm = False  # 是否需要追加权限条件
        conflict_info = None  # 权限冲突信息
        
        for ir_filter in ir_filters:
            ir_field = ir_filter.field.lower() if ir_filter.field else ""
            ir_op = ir_filter.op.upper() if ir_filter.op else ""
            ir_value = ir_filter.value
            
            # 检查是否是同一字段
            if ir_field != perm_field:
                new_filters.append(ir_filter)
                continue
            
            # 同字段，尝试合并
            merged_filter = self._merge_same_field_conditions(
                perm_op, perm_value, ir_op, ir_value, ir_filter.field
            )
            
            if merged_filter is not None:
                # 合并成功
                merged = True
                if merged_filter == "remove":
                    # 移除IR条件，需要追加权限条件
                    need_append_perm = True
                    logger.debug(
                        "同字段条件已合并：移除IR条件，将追加权限条件",
                        field=ir_filter.field,
                        perm_condition=f"{perm_op} {perm_value}",
                        ir_condition=f"{ir_op} {ir_value}"
                    )
                elif merged_filter == "conflict":
                    # 权限冲突：用户查询的值不在权限允许范围内
                    conflict_info = {
                        "field": ir_filter.field,
                        "user_requested": ir_value,
                        "allowed_values": perm_value if isinstance(perm_value, list) else [perm_value],
                    }
                    logger.warning(
                        "权限冲突：用户查询的值不在权限允许范围内",
                        field=ir_filter.field,
                        user_requested=ir_value,
                        allowed_values=perm_value
                    )
                    # 不保留IR条件，也不追加权限条件（因为冲突了）
                    need_append_perm = False
                elif isinstance(merged_filter, FilterCondition):
                    # 合并为新条件（如交集），不需要再追加权限条件
                    new_filters.append(merged_filter)
                    need_append_perm = False
                    logger.debug(
                        "同字段条件已合并为新条件",
                        field=ir_filter.field,
                        perm_condition=f"{perm_op} {perm_value}",
                        ir_condition=f"{ir_op} {ir_value}",
                        result=f"{merged_filter.op} {merged_filter.value}"
                    )
            else:
                # 无法合并，保留原条件
                new_filters.append(ir_filter)
        
        return merged, new_filters, need_append_perm, conflict_info
    
    def _merge_same_field_conditions(
        self,
        perm_op: str,
        perm_value: Any,
        ir_op: str,
        ir_value: Any,
        field_name: str
    ) -> Union[FilterCondition, str, None]:
        """
        合并同字段的两个条件
        
        Returns:
            - FilterCondition: 合并后的条件
            - "remove": 表示应移除IR条件（权限条件会单独添加）
            - "conflict": 表示权限冲突，用户查询的值不在权限允许范围内
            - None: 无法合并，保留原条件
        """
        # 权限 = 'A' + IR IN ['A','B','C']
        if perm_op == '=' and ir_op == 'IN':
            if isinstance(ir_value, list):
                # 检查权限值是否在IR列表中
                if perm_value in ir_value or str(perm_value) in [str(v) for v in ir_value]:
                    # 权限值在列表中，移除IR条件（权限条件更严格）
                    return "remove"
                else:
                    # 权限值不在用户查询列表中，权限冲突
                    return "conflict"
        
        # 权限 IN ['A','B'] + IR = 'C' （用户查询单个值，但不在权限范围内）
        if perm_op == 'IN' and ir_op == '=':
            if isinstance(perm_value, list):
                perm_set = set(str(v) for v in perm_value)
                if str(ir_value) in perm_set:
                    # 用户查询的值在权限允许范围内，移除IR条件，用权限条件中的这个值
                    return FilterCondition(
                        field=field_name,
                        op='=',
                        value=ir_value
                    )
                else:
                    # 用户查询的值不在权限允许范围内，权限冲突
                    return "conflict"
        
        # 权限 IN ['A','B'] + IR IN ['A','B','C']
        if perm_op == 'IN' and ir_op == 'IN':
            if isinstance(perm_value, list) and isinstance(ir_value, list):
                # 取交集
                perm_set = set(str(v) for v in perm_value)
                ir_set = set(str(v) for v in ir_value)
                intersection = perm_set & ir_set
                
                if intersection:
                    if len(intersection) == 1:
                        # 交集只有一个值，转为 =
                        return FilterCondition(
                            field=field_name,
                            op='=',
                            value=list(intersection)[0]
                        )
                    else:
                        # 交集多个值，保持 IN
                        return FilterCondition(
                            field=field_name,
                            op='IN',
                            value=list(intersection)
                        )
                else:
                    # 交集为空，权限冲突
                    return "conflict"
        
        # 权限 = 'A' + IR = 'A' 或 'B'
        if perm_op == '=' and ir_op == '=':
            if str(perm_value) == str(ir_value):
                # 值相同，移除IR条件
                return "remove"
            else:
                # 值不同，权限冲突
                return "conflict"
        
        # 其他情况无法合并
        return None


async def get_permission_injector(db_pool) -> PermissionInjector:
    """获取权限注入器实例"""
    return PermissionInjector(db_pool)

