"""
数据权限服务模块
包含数据角色、表权限、行级过滤、用户属性的管理服务

权限层级（简化后）：
  数据角色（全局）
  ├── scope_type = 'all' → 可访问所有表
  └── scope_type = 'limited' → 需配置表权限
      └── role_table_permissions（表权限）
          └── role_row_filters（行权限）

注：已移除连接权限层（role_connection_scopes），权限直接在表级别控制
"""

import json
import re
from datetime import datetime
from typing import Optional, List, Dict, Any, Set
from uuid import UUID
import structlog

from server.models.permission import (
    DataRoleCreate, DataRoleUpdate, DataRoleInDB, DataRoleResponse,
    TablePermissionCreate, TablePermissionUpdate, TablePermissionInDB, TablePermissionResponse,
    RowFilterCreate, RowFilterUpdate, RowFilterInDB, RowFilterResponse, RowFilterFromTemplate,
    RLSTemplateInDB, RLSTemplateResponse,
    UserDataRoleCreate, UserDataRoleInDB, UserDataRoleResponse,
    UserAttributeCreate, UserAttributeUpdate, UserAttributeInDB, UserAttributeBatchUpdate,
    PermissionCheckResult, RLSPreviewResponse,
    UserAccessibleConnection, UserAccessibleConnectionsResponse,
    ColumnAccessMode, FilterValueType, RoleScopeType
)

logger = structlog.get_logger()


def _convert_row(row) -> Dict[str, Any]:
    """将asyncpg行记录转换为字典，处理UUID类型转换"""
    if row is None:
        return None
    result = {}
    for key, value in dict(row).items():
        # 将asyncpg的UUID转换为Python标准UUID
        if hasattr(value, 'hex') and hasattr(value, 'int'):
            # 这是一个UUID类型对象
            result[key] = UUID(str(value))
        elif isinstance(value, list):
            # 处理UUID数组
            result[key] = [UUID(str(v)) if hasattr(v, 'hex') and hasattr(v, 'int') else v for v in value]
        else:
            result[key] = value
    return result


class DataRoleService:
    """数据角色服务 - 全局角色管理"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def create_role(self, role: DataRoleCreate, created_by: Optional[UUID] = None) -> DataRoleInDB:
        """创建数据角色（全局）"""
        query = """
            INSERT INTO data_roles (role_name, role_code, description, scope_type, is_default, is_active, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING role_id, role_name, role_code, description, scope_type, is_default, is_active, 
                      created_at, created_by, updated_at
        """
        row = await self.db.fetchrow(
            query,
            role.role_name, role.role_code, role.description,
            role.scope_type.value if role.scope_type else 'limited',
            role.is_default, role.is_active, created_by
        )
        return DataRoleInDB(**_convert_row(row))
    
    async def get_role(self, role_id: UUID) -> Optional[DataRoleResponse]:
        """获取单个数据角色"""
        query = """
            SELECT dr.*,
                   (SELECT COUNT(*) FROM user_data_roles udr WHERE udr.role_id = dr.role_id AND udr.is_active = TRUE) as user_count,
                   0 as connection_count,
                   (SELECT COUNT(*) FROM role_table_permissions rtp WHERE rtp.role_id = dr.role_id) as table_permission_count,
                   (SELECT COUNT(*) FROM role_row_filters rrf WHERE rrf.role_id = dr.role_id AND rrf.is_active = TRUE) as row_filter_count
            FROM data_roles dr
            WHERE dr.role_id = $1
        """
        row = await self.db.fetchrow(query, role_id)
        if row:
            return DataRoleResponse(**_convert_row(row))
        return None
    
    async def list_roles(
        self, 
        is_active: Optional[bool] = None,
        search: Optional[str] = None,
        scope_type: Optional[str] = None
    ) -> List[DataRoleResponse]:
        """获取数据角色列表（全局）"""
        where_clauses = ["1=1"]
        params = []
        
        if is_active is not None:
            params.append(is_active)
            where_clauses.append(f"dr.is_active = ${len(params)}")
        
        if search:
            params.append(f"%{search}%")
            where_clauses.append(f"(dr.role_name ILIKE ${len(params)} OR dr.role_code ILIKE ${len(params)})")
        
        if scope_type:
            params.append(scope_type)
            where_clauses.append(f"dr.scope_type = ${len(params)}")
        
        query = f"""
            SELECT dr.*,
                   (SELECT COUNT(*) FROM user_data_roles udr WHERE udr.role_id = dr.role_id AND udr.is_active = TRUE) as user_count,
                   0 as connection_count,
                   (SELECT COUNT(*) FROM role_table_permissions rtp WHERE rtp.role_id = dr.role_id) as table_permission_count,
                   (SELECT COUNT(*) FROM role_row_filters rrf WHERE rrf.role_id = dr.role_id AND rrf.is_active = TRUE) as row_filter_count
            FROM data_roles dr
            WHERE {' AND '.join(where_clauses)}
            ORDER BY dr.created_at DESC
        """
        rows = await self.db.fetch(query, *params)
        return [DataRoleResponse(**_convert_row(row)) for row in rows]
    
    async def update_role(self, role_id: UUID, update: DataRoleUpdate) -> Optional[DataRoleInDB]:
        """更新数据角色"""
        update_fields = []
        params = [role_id]
        
        if update.role_name is not None:
            params.append(update.role_name)
            update_fields.append(f"role_name = ${len(params)}")
        
        if update.description is not None:
            params.append(update.description)
            update_fields.append(f"description = ${len(params)}")
        
        if update.scope_type is not None:
            params.append(update.scope_type.value)
            update_fields.append(f"scope_type = ${len(params)}")
        
        if update.is_default is not None:
            params.append(update.is_default)
            update_fields.append(f"is_default = ${len(params)}")
        
        if update.is_active is not None:
            params.append(update.is_active)
            update_fields.append(f"is_active = ${len(params)}")
        
        if not update_fields:
            return await self.get_role(role_id)
        
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        
        query = f"""
            UPDATE data_roles SET {', '.join(update_fields)}
            WHERE role_id = $1
            RETURNING role_id, role_name, role_code, description, scope_type, is_default, is_active,
                      created_at, created_by, updated_at
        """
        row = await self.db.fetchrow(query, *params)
        if row:
            return DataRoleInDB(**_convert_row(row))
        return None
    
    async def delete_role(self, role_id: UUID) -> bool:
        """删除数据角色"""
        result = await self.db.execute("DELETE FROM data_roles WHERE role_id = $1", role_id)
        return result == "DELETE 1"


# [已删除] ConnectionScopeService 类
# 新架构中移除了连接权限层，权限直接在表级别控制


class TablePermissionService:
    """表权限服务"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def set_permission(self, permission: TablePermissionCreate) -> TablePermissionInDB:
        """设置表权限（存在则更新，不存在则创建）"""
        query = """
            INSERT INTO role_table_permissions (
                role_id, table_id, can_query, can_export, column_access_mode,
                included_column_ids, excluded_column_ids, masked_column_ids,
                restricted_filter_column_ids, restricted_aggregate_column_ids,
                restricted_group_by_column_ids, restricted_order_by_column_ids
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (role_id, table_id) DO UPDATE SET
                can_query = EXCLUDED.can_query,
                can_export = EXCLUDED.can_export,
                column_access_mode = EXCLUDED.column_access_mode,
                included_column_ids = EXCLUDED.included_column_ids,
                excluded_column_ids = EXCLUDED.excluded_column_ids,
                masked_column_ids = EXCLUDED.masked_column_ids,
                restricted_filter_column_ids = EXCLUDED.restricted_filter_column_ids,
                restricted_aggregate_column_ids = EXCLUDED.restricted_aggregate_column_ids,
                restricted_group_by_column_ids = EXCLUDED.restricted_group_by_column_ids,
                restricted_order_by_column_ids = EXCLUDED.restricted_order_by_column_ids,
                updated_at = CURRENT_TIMESTAMP
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            permission.role_id, permission.table_id,
            permission.can_query, permission.can_export,
            permission.column_access_mode.value if permission.column_access_mode else 'blacklist',
            permission.included_column_ids, permission.excluded_column_ids,
            permission.masked_column_ids, permission.restricted_filter_column_ids,
            permission.restricted_aggregate_column_ids, permission.restricted_group_by_column_ids,
            permission.restricted_order_by_column_ids
        )
        return TablePermissionInDB(**_convert_row(row))
    
    async def get_permissions_by_role(self, role_id: UUID, connection_id: Optional[UUID] = None) -> List[TablePermissionResponse]:
        """获取角色的表权限（可按连接过滤）"""
        where_clause = "rtp.role_id = $1"
        params = [role_id]
        
        if connection_id:
            params.append(connection_id)
            where_clause += f" AND t.connection_id = ${len(params)}"
        
        query = f"""
            SELECT 
                rtp.*, 
                t.table_name, 
                t.schema_name, 
                t.display_name,
                t.connection_id,
                dc.connection_name
            FROM role_table_permissions rtp
            JOIN db_tables t ON rtp.table_id = t.table_id
            JOIN database_connections dc ON t.connection_id = dc.connection_id
            WHERE {where_clause}
            ORDER BY t.schema_name, t.table_name
        """
        rows = await self.db.fetch(query, *params)
        return [TablePermissionResponse(**_convert_row(row)) for row in rows]
    
    async def get_permission(self, role_id: UUID, table_id: UUID) -> Optional[TablePermissionResponse]:
        """获取单个表权限"""
        query = """
            SELECT 
                rtp.*, 
                t.table_name, 
                t.schema_name, 
                t.display_name,
                t.connection_id,
                dc.connection_name
            FROM role_table_permissions rtp
            JOIN db_tables t ON rtp.table_id = t.table_id
            JOIN database_connections dc ON t.connection_id = dc.connection_id
            WHERE rtp.role_id = $1 AND rtp.table_id = $2
        """
        row = await self.db.fetchrow(query, role_id, table_id)
        if row:
            return TablePermissionResponse(**_convert_row(row))
        return None
    
    async def delete_permission(self, role_id: UUID, table_id: UUID) -> bool:
        """删除表权限"""
        result = await self.db.execute(
            "DELETE FROM role_table_permissions WHERE role_id = $1 AND table_id = $2",
            role_id, table_id
        )
        return result == "DELETE 1"
    
    async def batch_set_permissions(self, role_id: UUID, permissions: List[TablePermissionCreate]) -> int:
        """批量设置表权限"""
        count = 0
        for perm in permissions:
            perm.role_id = role_id
            await self.set_permission(perm)
            count += 1
        return count


class RowFilterService:
    """行级过滤规则服务"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def create_filter(self, filter_data: RowFilterCreate) -> RowFilterInDB:
        """创建行级过滤规则"""
        query = """
            INSERT INTO role_row_filters (role_id, filter_name, description, table_id, filter_definition, priority, is_active)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
        """
        row = await self.db.fetchrow(
            query,
            filter_data.role_id, filter_data.filter_name, filter_data.description,
            filter_data.table_id, json.dumps(filter_data.filter_definition),
            filter_data.priority, filter_data.is_active
        )
        result = _convert_row(row)
        if isinstance(result.get('filter_definition'), str):
            result['filter_definition'] = json.loads(result['filter_definition'])
        return RowFilterInDB(**result)
    
    async def get_filter(self, filter_id: UUID) -> Optional[RowFilterResponse]:
        """获取单个行级过滤规则"""
        query = """
            SELECT rrf.*, t.table_name
            FROM role_row_filters rrf
            LEFT JOIN db_tables t ON rrf.table_id = t.table_id
            WHERE rrf.filter_id = $1
        """
        row = await self.db.fetchrow(query, filter_id)
        if row:
            result = _convert_row(row)
            if isinstance(result.get('filter_definition'), str):
                result['filter_definition'] = json.loads(result['filter_definition'])
            return RowFilterResponse(**result)
        return None
    
    async def get_filters_by_role(self, role_id: UUID, is_active: Optional[bool] = None) -> List[RowFilterResponse]:
        """获取角色的所有行级过滤规则"""
        where_clause = "rrf.role_id = $1"
        params = [role_id]
        
        if is_active is not None:
            params.append(is_active)
            where_clause += f" AND rrf.is_active = ${len(params)}"
        
        query = f"""
            SELECT rrf.*, t.table_name
            FROM role_row_filters rrf
            LEFT JOIN db_tables t ON rrf.table_id = t.table_id
            WHERE {where_clause}
            ORDER BY rrf.priority DESC, rrf.created_at
        """
        rows = await self.db.fetch(query, *params)
        result = []
        for row in rows:
            data = _convert_row(row)
            if isinstance(data.get('filter_definition'), str):
                data['filter_definition'] = json.loads(data['filter_definition'])
            result.append(RowFilterResponse(**data))
        return result
    
    async def update_filter(self, filter_id: UUID, update: RowFilterUpdate) -> Optional[RowFilterInDB]:
        """更新行级过滤规则"""
        update_fields = []
        params = [filter_id]
        
        if update.filter_name is not None:
            params.append(update.filter_name)
            update_fields.append(f"filter_name = ${len(params)}")
        
        if update.description is not None:
            params.append(update.description)
            update_fields.append(f"description = ${len(params)}")
        
        if update.table_id is not None:
            params.append(update.table_id)
            update_fields.append(f"table_id = ${len(params)}")
        
        if update.filter_definition is not None:
            params.append(json.dumps(update.filter_definition))
            update_fields.append(f"filter_definition = ${len(params)}")
        
        if update.priority is not None:
            params.append(update.priority)
            update_fields.append(f"priority = ${len(params)}")
        
        if update.is_active is not None:
            params.append(update.is_active)
            update_fields.append(f"is_active = ${len(params)}")
        
        if not update_fields:
            return await self.get_filter(filter_id)
        
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        
        query = f"""
            UPDATE role_row_filters SET {', '.join(update_fields)}
            WHERE filter_id = $1
            RETURNING *
        """
        row = await self.db.fetchrow(query, *params)
        if row:
            result = _convert_row(row)
            if isinstance(result.get('filter_definition'), str):
                result['filter_definition'] = json.loads(result['filter_definition'])
            return RowFilterInDB(**result)
        return None
    
    async def delete_filter(self, filter_id: UUID) -> bool:
        """删除行级过滤规则"""
        result = await self.db.execute("DELETE FROM role_row_filters WHERE filter_id = $1", filter_id)
        return result == "DELETE 1"
    
    async def create_from_template(
        self, 
        role_id: UUID, 
        template_data: RowFilterFromTemplate
    ) -> Optional[RowFilterInDB]:
        """从模板创建行级过滤规则"""
        # 获取模板
        template = await self.db.fetchrow(
            "SELECT * FROM rls_rule_templates WHERE template_code = $1 AND is_active = TRUE",
            template_data.template_code
        )
        if not template:
            return None
        
        # 解析模板定义，替换参数
        template_def = template['template_definition']
        if isinstance(template_def, str):
            template_def = json.loads(template_def)
        
        # 替换参数占位符
        def replace_params(obj, params):
            if isinstance(obj, str):
                for key, value in params.items():
                    obj = obj.replace(f"{{{{{key}}}}}", str(value))
                return obj
            elif isinstance(obj, dict):
                return {k: replace_params(v, params) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_params(item, params) for item in obj]
            return obj
        
        filter_definition = replace_params(template_def, template_data.params)
        
        # 创建过滤规则
        filter_create = RowFilterCreate(
            role_id=role_id,
            filter_name=template_data.filter_name,
            description=f"从模板 '{template['template_name']}' 创建",
            table_id=template_data.table_id,
            filter_definition=filter_definition,
            priority=0,
            is_active=True
        )
        return await self.create_filter(filter_create)


class RLSTemplateService:
    """RLS规则模板服务"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def list_templates(self, category: Optional[str] = None) -> List[RLSTemplateResponse]:
        """获取模板列表"""
        where_clause = "is_active = TRUE"
        params = []
        
        if category:
            params.append(category)
            where_clause += f" AND category = ${len(params)}"
        
        query = f"""
            SELECT * FROM rls_rule_templates
            WHERE {where_clause}
            ORDER BY is_system DESC, category, template_name
        """
        rows = await self.db.fetch(query, *params)
        result = []
        for row in rows:
            data = _convert_row(row)
            # 确保JSON字段正确解析
            for field in ['template_definition', 'required_params', 'optional_params', 'example_params']:
                if isinstance(data.get(field), str):
                    data[field] = json.loads(data[field]) if data[field] else None
            result.append(RLSTemplateResponse(**data))
        return result
    
    async def get_template(self, template_id: UUID) -> Optional[RLSTemplateResponse]:
        """获取单个模板"""
        row = await self.db.fetchrow(
            "SELECT * FROM rls_rule_templates WHERE template_id = $1",
            template_id
        )
        if row:
            data = _convert_row(row)
            for field in ['template_definition', 'required_params', 'optional_params', 'example_params']:
                if isinstance(data.get(field), str):
                    data[field] = json.loads(data[field]) if data[field] else None
            return RLSTemplateResponse(**data)
        return None
    
    async def get_template_by_code(self, template_code: str) -> Optional[RLSTemplateResponse]:
        """根据编码获取模板"""
        row = await self.db.fetchrow(
            "SELECT * FROM rls_rule_templates WHERE template_code = $1 AND is_active = TRUE",
            template_code
        )
        if row:
            data = _convert_row(row)
            for field in ['template_definition', 'required_params', 'optional_params', 'example_params']:
                if isinstance(data.get(field), str):
                    data[field] = json.loads(data[field]) if data[field] else None
            return RLSTemplateResponse(**data)
        return None


class UserDataRoleService:
    """用户-数据角色关联服务"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def assign_role(
        self, 
        user_id: UUID, 
        role_id: UUID, 
        granted_by: Optional[UUID] = None,
        expires_at: Optional[datetime] = None
    ) -> UserDataRoleInDB:
        """为用户分配数据角色"""
        query = """
            INSERT INTO user_data_roles (user_id, role_id, granted_by, expires_at, is_active)
            VALUES ($1, $2, $3, $4, TRUE)
            ON CONFLICT (user_id, role_id) DO UPDATE SET
                is_active = TRUE,
                granted_by = EXCLUDED.granted_by,
                granted_at = CURRENT_TIMESTAMP,
                expires_at = EXCLUDED.expires_at
            RETURNING *
        """
        row = await self.db.fetchrow(query, user_id, role_id, granted_by, expires_at)
        return UserDataRoleInDB(**_convert_row(row))
    
    async def remove_role(self, user_id: UUID, role_id: UUID) -> bool:
        """移除用户的数据角色"""
        result = await self.db.execute(
            "UPDATE user_data_roles SET is_active = FALSE WHERE user_id = $1 AND role_id = $2",
            user_id, role_id
        )
        return "UPDATE" in result
    
    async def get_user_roles(self, user_id: UUID) -> List[UserDataRoleResponse]:
        """获取用户的所有数据角色"""
        query = """
            SELECT udr.*, dr.role_name, dr.role_code, dr.scope_type,
                   0 as connection_count
            FROM user_data_roles udr
            JOIN data_roles dr ON udr.role_id = dr.role_id
            WHERE udr.user_id = $1 AND udr.is_active = TRUE AND dr.is_active = TRUE
            ORDER BY dr.role_name
        """
        rows = await self.db.fetch(query, user_id)
        return [UserDataRoleResponse(**_convert_row(row)) for row in rows]
    
    async def get_role_users(self, role_id: UUID) -> List[Dict[str, Any]]:
        """获取拥有某角色的所有用户"""
        query = """
            SELECT u.user_id, u.username, u.full_name, u.email, udr.granted_at, udr.expires_at
            FROM user_data_roles udr
            JOIN users u ON udr.user_id = u.user_id
            WHERE udr.role_id = $1 AND udr.is_active = TRUE AND u.is_active = TRUE
            ORDER BY u.username
        """
        rows = await self.db.fetch(query, role_id)
        return [_convert_row(row) for row in rows]


class UserAttributeService:
    """用户属性服务"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def set_attribute(self, user_id: UUID, name: str, value: str) -> UserAttributeInDB:
        """设置用户属性"""
        query = """
            INSERT INTO user_attributes (user_id, attribute_name, attribute_value)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, attribute_name) DO UPDATE SET
                attribute_value = EXCLUDED.attribute_value,
                updated_at = CURRENT_TIMESTAMP
            RETURNING *
        """
        row = await self.db.fetchrow(query, user_id, name, value)
        return UserAttributeInDB(**_convert_row(row))
    
    async def get_attributes(self, user_id: UUID) -> Dict[str, str]:
        """获取用户的所有属性（返回字典）"""
        query = "SELECT attribute_name, attribute_value FROM user_attributes WHERE user_id = $1"
        rows = await self.db.fetch(query, user_id)
        return {row['attribute_name']: row['attribute_value'] for row in rows}
    
    async def get_attributes_list(self, user_id: UUID) -> List[UserAttributeInDB]:
        """获取用户的所有属性（返回列表）"""
        query = "SELECT * FROM user_attributes WHERE user_id = $1 ORDER BY attribute_name"
        rows = await self.db.fetch(query, user_id)
        return [UserAttributeInDB(**_convert_row(row)) for row in rows]
    
    async def delete_attribute(self, user_id: UUID, name: str) -> bool:
        """删除用户属性"""
        result = await self.db.execute(
            "DELETE FROM user_attributes WHERE user_id = $1 AND attribute_name = $2",
            user_id, name
        )
        return result == "DELETE 1"
    
    async def batch_update_attributes(self, user_id: UUID, attributes: Dict[str, str]) -> int:
        """批量更新用户属性"""
        count = 0
        for name, value in attributes.items():
            await self.set_attribute(user_id, name, value)
            count += 1
        return count
    
    async def parse_attribute_value(self, value: str) -> Any:
        """解析属性值（支持JSON数组）"""
        if value.startswith('[') and value.endswith(']'):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        return value


class UserConnectionAccessService:
    """用户可访问连接服务 - 用于前端数据源选择"""
    
    def __init__(self, db_pool):
        self.db = db_pool
    
    async def get_accessible_connections(
        self, 
        user_id: UUID, 
        system_role: str = 'viewer'
    ) -> UserAccessibleConnectionsResponse:
        """
        获取用户可访问的数据库连接列表
        
        逻辑（简化后）：
        1. 系统管理员(admin)可以访问所有连接
        2. 拥有 scope_type='all' 角色的用户可以访问所有连接
        3. 其他用户通过表权限(role_table_permissions)确定可访问的连接
        """
        is_admin = system_role == 'admin'
        
        # 检查用户是否拥有 scope_type='all' 的角色
        has_all_access_query = """
            SELECT EXISTS(
                SELECT 1 FROM user_data_roles udr
                JOIN data_roles dr ON udr.role_id = dr.role_id
                WHERE udr.user_id = $1 AND udr.is_active = TRUE AND dr.is_active = TRUE
                AND dr.scope_type = 'all'
            ) as has_all_access
        """
        result = await self.db.fetchrow(has_all_access_query, user_id)
        has_all_access = result['has_all_access'] if result else False
        
        # 如果是管理员或有全量访问权限，返回所有活跃连接
        if is_admin or has_all_access:
            connections_query = """
                SELECT connection_id, connection_name, description, db_type
                FROM database_connections
                WHERE is_active = TRUE
                ORDER BY connection_name
            """
            rows = await self.db.fetch(connections_query)
            connections = [
                UserAccessibleConnection(
                    connection_id=row['connection_id'],
                    connection_name=row['connection_name'],
                    description=row['description'],
                    db_type=row['db_type'],
                    can_query=True,
                    can_export=is_admin  # 管理员默认可导出
                )
                for row in rows
            ]
        else:
            # 通过表权限确定可访问的连接
            # scope_type='limited' 的用户，根据其角色的表权限确定可访问的连接
            connections_query = """
                SELECT DISTINCT dc.connection_id, dc.connection_name, dc.description, dc.db_type,
                       BOOL_OR(rtp.can_query) as can_query,
                       BOOL_OR(rtp.can_export) as can_export
                FROM user_data_roles udr
                JOIN data_roles dr ON udr.role_id = dr.role_id
                JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                JOIN db_tables t ON rtp.table_id = t.table_id
                JOIN database_connections dc ON t.connection_id = dc.connection_id
                WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                AND dr.is_active = TRUE AND dc.is_active = TRUE
                AND dr.scope_type = 'limited'
                GROUP BY dc.connection_id, dc.connection_name, dc.description, dc.db_type
                ORDER BY dc.connection_name
            """
            rows = await self.db.fetch(connections_query, user_id)
            connections = [
                UserAccessibleConnection(
                    connection_id=row['connection_id'],
                    connection_name=row['connection_name'],
                    description=row['description'],
                    db_type=row['db_type'],
                    can_query=row['can_query'] or False,
                    can_export=row['can_export'] or False
                )
                for row in rows
            ]
        
        return UserAccessibleConnectionsResponse(
            user_id=user_id,
            is_admin=is_admin,
            has_all_access=has_all_access,
            connections=connections
        )


class PermissionCheckerService:
    """权限检查服务 - 用于查询时的权限验证"""
    
    def __init__(self, db_pool):
        self.db = db_pool
        self.data_role_service = DataRoleService(db_pool)
        self.table_permission_service = TablePermissionService(db_pool)
        self.row_filter_service = RowFilterService(db_pool)
        self.user_data_role_service = UserDataRoleService(db_pool)
        self.user_attribute_service = UserAttributeService(db_pool)
        self.user_connection_service = UserConnectionAccessService(db_pool)
    
    async def check_connection_access(
        self, 
        user_id: UUID, 
        connection_id: UUID,
        system_role: str = 'viewer'
    ) -> bool:
        """
        检查用户是否有权限访问指定连接
        
        逻辑（简化后）：
        1. 系统管理员可以访问所有连接
        2. scope_type='all' 的角色可以访问所有连接
        3. 通过表权限确定是否有该连接下的表访问权限
        """
        if system_role == 'admin':
            return True
        
        # 检查是否有 scope_type='all' 的角色
        has_all_query = """
            SELECT EXISTS(
                SELECT 1 FROM user_data_roles udr
                JOIN data_roles dr ON udr.role_id = dr.role_id
                WHERE udr.user_id = $1 AND udr.is_active = TRUE AND dr.is_active = TRUE
                AND dr.scope_type = 'all'
            ) as has_access
        """
        result = await self.db.fetchrow(has_all_query, user_id)
        if result and result['has_access']:
            return True
        
        # 检查是否有该连接下的表权限
        has_table_query = """
            SELECT EXISTS(
                SELECT 1 FROM user_data_roles udr
                JOIN data_roles dr ON udr.role_id = dr.role_id
                JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                JOIN db_tables t ON rtp.table_id = t.table_id
                WHERE udr.user_id = $1 AND t.connection_id = $2
                AND udr.is_active = TRUE AND dr.is_active = TRUE AND rtp.can_query = TRUE
            ) as has_access
        """
        result = await self.db.fetchrow(has_table_query, user_id, connection_id)
        return result and result['has_access']
    
    async def get_user_permissions(
        self, 
        user_id: UUID, 
        connection_id: UUID,
        system_role: str = 'viewer'
    ) -> PermissionCheckResult:
        """获取用户在指定连接上的完整权限"""
        # 首先检查连接访问权限
        has_connection_access = await self.check_connection_access(user_id, connection_id, system_role)
        
        if not has_connection_access:
            return PermissionCheckResult(
                user_id=user_id,
                connection_id=connection_id,
                has_permission=False,
                accessible_tables=[],
                row_filters={},
                column_permissions={}
            )
        
        # 获取用户的数据角色
        user_roles = await self.user_data_role_service.get_user_roles(user_id)
        
        if not user_roles and system_role != 'admin':
            return PermissionCheckResult(
                user_id=user_id,
                connection_id=connection_id,
                has_permission=False,
                accessible_tables=[],
                row_filters={},
                column_permissions={}
            )
        
        # 管理员或 scope_type='all' 的角色：所有表可访问
        is_admin = system_role == 'admin'
        has_all_access = any(r.scope_type == 'all' for r in user_roles)
        
        if is_admin or has_all_access:
            # 获取连接下的所有表
            all_tables_query = """
                SELECT table_id FROM db_tables
                WHERE connection_id = $1 AND is_included = TRUE
            """
            rows = await self.db.fetch(all_tables_query, connection_id)
            accessible_tables = [row['table_id'] for row in rows]
            
            return PermissionCheckResult(
                user_id=user_id,
                connection_id=connection_id,
                has_permission=True,
                accessible_tables=accessible_tables,
                row_filters={},  # 全量访问无行级过滤
                column_permissions={}  # 全量访问无列限制
            )
        
        # 受限访问：根据角色配置获取表权限
        accessible_tables: Set[UUID] = set()
        column_permissions: Dict[str, Dict[str, Any]] = {}
        
        for role in user_roles:
            # 只处理 limited scope 且有该连接权限的角色
            if role.scope_type != 'limited':
                continue
            
            permissions = await self.table_permission_service.get_permissions_by_role(role.role_id, connection_id)
            for perm in permissions:
                if perm.can_query:
                    accessible_tables.add(perm.table_id)
                    # 合并列权限
                    table_key = str(perm.table_id)
                    if table_key not in column_permissions:
                        column_permissions[table_key] = {
                            'column_access_mode': perm.column_access_mode,
                            'included_column_ids': set(perm.included_column_ids or []),
                            'excluded_column_ids': set(perm.excluded_column_ids or []),
                            'masked_column_ids': set(perm.masked_column_ids or []),
                            'restricted_filter_column_ids': set(perm.restricted_filter_column_ids or []),
                            'restricted_aggregate_column_ids': set(perm.restricted_aggregate_column_ids or [])
                        }
                    else:
                        # 多角色权限合并：可见列取并集，脱敏列取交集，限制列取交集
                        existing = column_permissions[table_key]
                        existing['included_column_ids'].update(perm.included_column_ids or [])
                        existing['excluded_column_ids'] &= set(perm.excluded_column_ids or [])
                        existing['masked_column_ids'] &= set(perm.masked_column_ids or [])
                        existing['restricted_filter_column_ids'] &= set(perm.restricted_filter_column_ids or [])
        
        # 获取行级过滤规则并合并
        user_attrs = await self.user_attribute_service.get_attributes(user_id)
        row_filters: Dict[str, str] = {}
        
        for role in user_roles:
            filters = await self.row_filter_service.get_filters_by_role(role.role_id, is_active=True)
            for f in filters:
                table_key = str(f.table_id) if f.table_id else '__all__'
                parsed = self._parse_filter_definition(f.filter_definition, user_attrs)
                if parsed:
                    if table_key in row_filters:
                        # 多角色过滤条件：OR合并
                        row_filters[table_key] = f"({row_filters[table_key]}) OR ({parsed})"
                    else:
                        row_filters[table_key] = parsed
        
        # 转换集合为列表
        for key in column_permissions:
            for field in ['included_column_ids', 'excluded_column_ids', 'masked_column_ids', 
                         'restricted_filter_column_ids', 'restricted_aggregate_column_ids']:
                column_permissions[key][field] = list(column_permissions[key][field])
        
        return PermissionCheckResult(
            user_id=user_id,
            connection_id=connection_id,
            has_permission=True,
            accessible_tables=list(accessible_tables),
            row_filters=row_filters,
            column_permissions=column_permissions
        )
    
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
                # 从用户属性获取值
                attr_value = user_attrs.get(value)
                if attr_value is None:
                    logger.warning(f"用户属性 '{value}' 不存在，跳过此条件")
                    continue
                # 处理数组类型
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
            
            # 静态值或解析后的用户属性
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
    
    async def preview_rls(
        self, 
        filter_definition: Dict[str, Any], 
        user_id: UUID
    ) -> RLSPreviewResponse:
        """预览RLS规则解析结果"""
        user_attrs = await self.user_attribute_service.get_attributes(user_id)
        parsed = self._parse_filter_definition(filter_definition, user_attrs)
        
        # 找出使用的用户属性
        used_attrs = {}
        for cond in filter_definition.get('conditions', []):
            if cond.get('value_type') == 'user_attr':
                attr_name = cond.get('value')
                if attr_name in user_attrs:
                    used_attrs[attr_name] = user_attrs[attr_name]
        
        warnings = []
        for cond in filter_definition.get('conditions', []):
            if cond.get('value_type') == 'user_attr':
                attr_name = cond.get('value')
                if attr_name not in user_attrs:
                    warnings.append(f"用户属性 '{attr_name}' 不存在")
        
        return RLSPreviewResponse(
            parsed_condition=parsed or "无有效条件",
            user_attributes_used=used_attrs,
            warnings=warnings
        )
