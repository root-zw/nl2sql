"""
数据权限管理API
包含数据角色、表权限、行级过滤规则、用户属性的管理接口

权限层级（简化后）：
  数据角色（全局）
  ├── scope_type = 'all' → 可访问所有表
  └── scope_type = 'limited' → 需配置表权限
      └── role_table_permissions（表权限）
          └── role_row_filters（行权限）

注：已移除连接权限层（role_connection_scopes），权限直接在表级别控制
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import Optional, List, Dict, Any
from uuid import UUID
import structlog
from datetime import datetime

from server.models.permission import (
    DataRoleCreate, DataRoleUpdate, DataRoleResponse,
    TablePermissionCreate, TablePermissionUpdate, TablePermissionResponse,
    RowFilterCreate, RowFilterUpdate, RowFilterResponse, RowFilterFromTemplate,
    RLSTemplateResponse,
    UserDataRoleAssignRequest, UserDataRoleCreate, UserDataRoleResponse,
    UserAttributeCreate, UserAttributeUpdate, UserAttributeInDB, UserAttributeBatchUpdate,
    PermissionCheckRequest, PermissionCheckResult, RLSPreviewRequest, RLSPreviewResponse,
    UserPermissionSummary, UserAccessibleConnectionsResponse, RoleScopeType
)
from server.services.permission_service import (
    DataRoleService, TablePermissionService, RowFilterService,
    RLSTemplateService, UserDataRoleService, UserAttributeService,
    PermissionCheckerService, UserConnectionAccessService
)
from server.middleware.auth import require_data_admin, require_admin
from server.models.admin import User as AdminUser
from server.models.database import UserRole

logger = structlog.get_logger()

router = APIRouter(prefix="/permissions", tags=["数据权限管理"])


async def get_db_pool():
    """获取数据库连接池"""
    from server.utils.db_pool import get_metadata_pool
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


# ============================================================================
# 数据角色管理（全局角色）
# ============================================================================

@router.get("/data-roles", response_model=List[DataRoleResponse])
async def list_data_roles(
    is_active: Optional[bool] = Query(None, description="是否启用"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    scope_type: Optional[str] = Query(None, description="范围类型: all/limited"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取数据角色列表（全局）"""
    service = DataRoleService(db)
    return await service.list_roles(is_active, search, scope_type)


@router.post("/data-roles", response_model=DataRoleResponse, status_code=status.HTTP_201_CREATED)
async def create_data_role(
    role: DataRoleCreate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """创建数据角色（全局）"""
    service = DataRoleService(db)
    try:
        created_by = UUID(str(current_user.user_id))
        result = await service.create_role(role, created_by)
        return await service.get_role(result.role_id)
    except Exception as e:
        logger.error(f"创建数据角色失败: {e}")
        if "unique" in str(e).lower():
            raise HTTPException(status_code=400, detail="角色编码已存在")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-roles/{role_id}", response_model=DataRoleResponse)
async def get_data_role(
    role_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取数据角色详情"""
    service = DataRoleService(db)
    role = await service.get_role(role_id)
    if not role:
        raise HTTPException(status_code=404, detail="角色不存在")
    return role


@router.put("/data-roles/{role_id}", response_model=DataRoleResponse)
async def update_data_role(
    role_id: UUID,
    update: DataRoleUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """更新数据角色"""
    service = DataRoleService(db)
    result = await service.update_role(role_id, update)
    if not result:
        raise HTTPException(status_code=404, detail="角色不存在")
    return await service.get_role(role_id)


@router.delete("/data-roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_data_role(
    role_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """删除数据角色"""
    service = DataRoleService(db)
    if not await service.delete_role(role_id):
        raise HTTPException(status_code=404, detail="角色不存在")


# ============================================================================
# [已删除] 角色连接权限管理
# 新架构中移除了连接权限层，权限直接在表级别控制
# ============================================================================


# ============================================================================
# 表权限管理
# ============================================================================

@router.get("/data-roles/{role_id}/tables", response_model=List[TablePermissionResponse])
async def get_role_table_permissions(
    role_id: UUID,
    connection_id: Optional[UUID] = Query(None, description="按连接过滤"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取角色的表权限列表（可按连接过滤）"""
    service = TablePermissionService(db)
    return await service.get_permissions_by_role(role_id, connection_id)


@router.put("/data-roles/{role_id}/tables/{table_id}", response_model=TablePermissionResponse)
async def set_table_permission(
    role_id: UUID,
    table_id: UUID,
    permission: TablePermissionUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """设置单个表的权限"""
    service = TablePermissionService(db)
    create_data = TablePermissionCreate(
        role_id=role_id,
        table_id=table_id,
        can_query=permission.can_query if permission.can_query is not None else True,
        can_export=permission.can_export if permission.can_export is not None else False,
        column_access_mode=permission.column_access_mode,
        included_column_ids=permission.included_column_ids,
        excluded_column_ids=permission.excluded_column_ids,
        masked_column_ids=permission.masked_column_ids,
        restricted_filter_column_ids=permission.restricted_filter_column_ids,
        restricted_aggregate_column_ids=permission.restricted_aggregate_column_ids,
        restricted_group_by_column_ids=permission.restricted_group_by_column_ids,
        restricted_order_by_column_ids=permission.restricted_order_by_column_ids
    )
    await service.set_permission(create_data)
    return await service.get_permission(role_id, table_id)


@router.delete("/data-roles/{role_id}/tables/{table_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_table_permission(
    role_id: UUID,
    table_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """删除表权限"""
    service = TablePermissionService(db)
    if not await service.delete_permission(role_id, table_id):
        raise HTTPException(status_code=404, detail="权限配置不存在")


@router.put("/data-roles/{role_id}/tables", response_model=Dict[str, int])
async def batch_set_table_permissions(
    role_id: UUID,
    permissions: List[TablePermissionCreate],
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """批量设置表权限"""
    service = TablePermissionService(db)
    count = await service.batch_set_permissions(role_id, permissions)
    return {"updated": count}


# ============================================================================
# 行级过滤规则管理
# ============================================================================

@router.get("/data-roles/{role_id}/row-filters", response_model=List[RowFilterResponse])
async def get_role_row_filters(
    role_id: UUID,
    is_active: Optional[bool] = Query(None, description="是否启用"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取角色的行级过滤规则"""
    service = RowFilterService(db)
    return await service.get_filters_by_role(role_id, is_active)


@router.post("/data-roles/{role_id}/row-filters", response_model=RowFilterResponse, status_code=status.HTTP_201_CREATED)
async def create_row_filter(
    role_id: UUID,
    filter_data: RowFilterCreate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """创建行级过滤规则"""
    filter_data.role_id = role_id
    service = RowFilterService(db)
    result = await service.create_filter(filter_data)
    return await service.get_filter(result.filter_id)


@router.post("/data-roles/{role_id}/row-filters/from-template", response_model=RowFilterResponse, status_code=status.HTTP_201_CREATED)
async def create_row_filter_from_template(
    role_id: UUID,
    template_data: RowFilterFromTemplate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """从模板创建行级过滤规则"""
    service = RowFilterService(db)
    result = await service.create_from_template(role_id, template_data)
    if not result:
        raise HTTPException(status_code=404, detail="模板不存在")
    return await service.get_filter(result.filter_id)


@router.get("/row-filters/{filter_id}", response_model=RowFilterResponse)
async def get_row_filter(
    filter_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取行级过滤规则详情"""
    service = RowFilterService(db)
    result = await service.get_filter(filter_id)
    if not result:
        raise HTTPException(status_code=404, detail="规则不存在")
    return result


@router.put("/row-filters/{filter_id}", response_model=RowFilterResponse)
async def update_row_filter(
    filter_id: UUID,
    update: RowFilterUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """更新行级过滤规则"""
    service = RowFilterService(db)
    result = await service.update_filter(filter_id, update)
    if not result:
        raise HTTPException(status_code=404, detail="规则不存在")
    return await service.get_filter(filter_id)


@router.delete("/row-filters/{filter_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_row_filter(
    filter_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """删除行级过滤规则"""
    service = RowFilterService(db)
    if not await service.delete_filter(filter_id):
        raise HTTPException(status_code=404, detail="规则不存在")


# ============================================================================
# RLS规则模板
# ============================================================================

@router.get("/rls-templates", response_model=List[RLSTemplateResponse])
async def list_rls_templates(
    category: Optional[str] = Query(None, description="分类"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取RLS规则模板列表"""
    service = RLSTemplateService(db)
    return await service.list_templates(category)


@router.get("/rls-templates/{template_id}", response_model=RLSTemplateResponse)
async def get_rls_template(
    template_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取RLS规则模板详情"""
    service = RLSTemplateService(db)
    result = await service.get_template(template_id)
    if not result:
        raise HTTPException(status_code=404, detail="模板不存在")
    return result


# ============================================================================
# 用户角色分配
# ============================================================================

@router.get("/users/{user_id}/data-roles", response_model=List[UserDataRoleResponse])
async def get_user_data_roles(
    user_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取用户的数据角色列表"""
    service = UserDataRoleService(db)
    return await service.get_user_roles(user_id)


@router.post("/users/{user_id}/data-roles", response_model=UserDataRoleResponse, status_code=status.HTTP_201_CREATED)
async def assign_user_data_role(
    user_id: UUID,
    assignment: UserDataRoleAssignRequest,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """为用户分配数据角色"""
    service = UserDataRoleService(db)
    granted_by = UUID(str(current_user.user_id))
    result = await service.assign_role(
        user_id,
        assignment.role_id,
        granted_by,
        assignment.expires_at
    )
    # 返回完整信息
    roles = await service.get_user_roles(user_id)
    for r in roles:
        if r.role_id == assignment.role_id:
            return r
    return result


@router.delete("/users/{user_id}/data-roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_user_data_role(
    user_id: UUID,
    role_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """移除用户的数据角色"""
    service = UserDataRoleService(db)
    if not await service.remove_role(user_id, role_id):
        raise HTTPException(status_code=404, detail="用户角色关联不存在")


@router.get("/data-roles/{role_id}/users", response_model=List[Dict[str, Any]])
async def get_role_users(
    role_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取拥有某角色的所有用户"""
    service = UserDataRoleService(db)
    return await service.get_role_users(role_id)


# ============================================================================
# 用户属性管理
# ============================================================================

@router.get("/users/{user_id}/attributes", response_model=List[UserAttributeInDB])
async def get_user_attributes(
    user_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取用户的所有属性"""
    service = UserAttributeService(db)
    return await service.get_attributes_list(user_id)


@router.put("/users/{user_id}/attributes/{attribute_name}", response_model=UserAttributeInDB)
async def set_user_attribute(
    user_id: UUID,
    attribute_name: str,
    update: UserAttributeUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """设置用户属性"""
    service = UserAttributeService(db)
    return await service.set_attribute(user_id, attribute_name, update.attribute_value)


@router.delete("/users/{user_id}/attributes/{attribute_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user_attribute(
    user_id: UUID,
    attribute_name: str,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """删除用户属性"""
    service = UserAttributeService(db)
    if not await service.delete_attribute(user_id, attribute_name):
        raise HTTPException(status_code=404, detail="属性不存在")


@router.put("/users/{user_id}/attributes", response_model=Dict[str, int])
async def batch_update_user_attributes(
    user_id: UUID,
    batch: UserAttributeBatchUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """批量更新用户属性"""
    service = UserAttributeService(db)
    count = await service.batch_update_attributes(user_id, batch.attributes)
    return {"updated": count}


# ============================================================================
# 用户可访问连接（用于前端数据源选择）
# ============================================================================

@router.get("/users/{user_id}/accessible-connections", response_model=UserAccessibleConnectionsResponse)
async def get_user_accessible_connections(
    user_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """
    获取用户可访问的数据库连接列表
    
    用于前端数据源选择器，根据用户权限过滤可选连接
    """
    # 获取目标用户的系统角色
    user_row = await db.fetchrow("SELECT role FROM users WHERE user_id = $1", user_id)
    if not user_row:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    service = UserConnectionAccessService(db)
    return await service.get_accessible_connections(user_id, user_row['role'])


@router.get("/my/accessible-connections", response_model=UserAccessibleConnectionsResponse)
async def get_my_accessible_connections(
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """
    获取当前登录用户可访问的数据库连接列表
    
    用于前端数据源选择器
    """
    service = UserConnectionAccessService(db)
    return await service.get_accessible_connections(
        UUID(str(current_user.user_id)), 
        current_user.role
    )


# ============================================================================
# 权限检查与测试
# ============================================================================

@router.post("/check", response_model=PermissionCheckResult)
async def check_permissions(
    request: PermissionCheckRequest,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """检查用户权限"""
    # 获取目标用户的系统角色
    user_row = await db.fetchrow("SELECT role FROM users WHERE user_id = $1", request.user_id)
    system_role = user_row['role'] if user_row else 'viewer'
    
    service = PermissionCheckerService(db)
    return await service.get_user_permissions(request.user_id, request.connection_id, system_role)


@router.post("/rls-preview", response_model=RLSPreviewResponse)
async def preview_rls_rule(
    request: RLSPreviewRequest,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """预览RLS规则解析结果"""
    service = PermissionCheckerService(db)
    return await service.preview_rls(request.filter_definition, request.user_id)


@router.get("/users/{user_id}/summary", response_model=UserPermissionSummary)
async def get_user_permission_summary(
    user_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取用户权限摘要"""
    # 获取用户基本信息
    user_row = await db.fetchrow(
        "SELECT user_id, username, role FROM users WHERE user_id = $1",
        user_id
    )
    if not user_row:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    # 获取数据角色
    role_service = UserDataRoleService(db)
    roles = await role_service.get_user_roles(user_id)
    
    # 获取用户可访问的连接
    conn_service = UserConnectionAccessService(db)
    accessible = await conn_service.get_accessible_connections(user_id, user_row['role'])
    
    # 构建连接列表
    connections = [
        {
            "connection_id": str(conn.connection_id),
            "connection_name": conn.connection_name,
            "can_query": conn.can_query,
            "can_export": conn.can_export
        }
        for conn in accessible.connections
    ]
    
    return UserPermissionSummary(
        user_id=user_row['user_id'],
        username=user_row['username'],
        system_role=user_row['role'],
        data_role_count=len(roles),
        connections=connections
    )


# ============================================================================
# 权限配置导入导出
# ============================================================================

from pydantic import BaseModel


class ImportResult(BaseModel):
    """导入结果"""
    success: bool
    roles_imported: int = 0
    permissions_imported: int = 0  # 表权限
    filters_imported: int = 0  # 行过滤规则
    errors: List[str] = []
    warnings: List[str] = []


@router.get("/export")
async def export_permissions(
    role_id: Optional[UUID] = Query(None, description="导出指定角色，不指定则导出所有"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """
    导出权限配置
    
    返回JSON格式的权限配置，包含：
    - 数据角色定义
    - 连接权限配置
    - 表权限配置
    - 行级过滤规则
    """
    import json
    from fastapi.responses import Response
    
    # 导出数据角色
    where_clause = "1=1"
    params = []
    if role_id:
        params.append(role_id)
        where_clause = f"role_id = ${len(params)}"
    
    roles_query = f"""
        SELECT role_id, role_name, role_code, description, scope_type, is_default, is_active
        FROM data_roles
        WHERE {where_clause}
        ORDER BY role_name
    """
    roles = await db.fetch(roles_query, *params)
    
    export_data = {
        "version": "2.0",
        "exported_at": str(datetime.now()),
        "data_roles": []
    }
    
    for role in roles:
        role_data = {
            "role_id": str(role['role_id']),
            "role_name": role['role_name'],
            "role_code": role['role_code'],
            "description": role['description'],
            "scope_type": role['scope_type'],
            "is_default": role['is_default'],
            "is_active": role['is_active'],
            "table_permissions": [],
            "row_filters": []
        }
        
        # 导出表权限（已移除连接权限层）
        perms_query = """
            SELECT rtp.*, t.table_name, t.schema_name, dc.connection_name
            FROM role_table_permissions rtp
            JOIN db_tables t ON rtp.table_id = t.table_id
            JOIN database_connections dc ON t.connection_id = dc.connection_id
            WHERE rtp.role_id = $1
        """
        perms = await db.fetch(perms_query, role['role_id'])
        for perm in perms:
            role_data["table_permissions"].append({
                "connection_name": perm['connection_name'],
                "table_name": perm['table_name'],
                "schema_name": perm['schema_name'],
                "can_query": perm['can_query'],
                "can_export": perm['can_export'],
                "column_access_mode": perm['column_access_mode'],
                "excluded_column_ids": [str(c) for c in (perm['excluded_column_ids'] or [])],
                "masked_column_ids": [str(c) for c in (perm['masked_column_ids'] or [])],
                "restricted_filter_column_ids": [str(c) for c in (perm['restricted_filter_column_ids'] or [])]
            })
        
        # 导出行过滤规则
        filters_query = """
            SELECT rrf.*, t.table_name
            FROM role_row_filters rrf
            LEFT JOIN db_tables t ON rrf.table_id = t.table_id
            WHERE rrf.role_id = $1
        """
        filters = await db.fetch(filters_query, role['role_id'])
        for f in filters:
            filter_def = f['filter_definition']
            if isinstance(filter_def, str):
                filter_def = json.loads(filter_def)
            role_data["row_filters"].append({
                "filter_name": f['filter_name'],
                "description": f['description'],
                "table_name": f['table_name'],
                "filter_definition": filter_def,
                "priority": f['priority'],
                "is_active": f['is_active']
            })
        
        export_data["data_roles"].append(role_data)
    
    # 返回JSON文件
    content = json.dumps(export_data, ensure_ascii=False, indent=2)
    filename = f"permissions_{role_id}.json" if role_id else "permissions_all.json"
    return Response(
        content=content,
        media_type="application/json",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@router.post("/import", response_model=ImportResult)
async def import_permissions(
    import_data: Dict[str, Any],
    merge_mode: bool = Query(True, description="True=合并现有配置，False=覆盖"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """
    导入权限配置
    
    Args:
        import_data: 导出的权限配置JSON
        merge_mode: 合并模式(True)或覆盖模式(False)
    """
    import json
    
    result = ImportResult(success=True)
    
    try:
        data_roles = import_data.get("data_roles", [])
        
        for role_data in data_roles:
            # 检查角色是否已存在
            existing = await db.fetchrow(
                "SELECT role_id FROM data_roles WHERE role_code = $1",
                role_data['role_code']
            )
            
            if existing and not merge_mode:
                # 覆盖模式：删除现有角色
                await db.execute("DELETE FROM data_roles WHERE role_id = $1", existing['role_id'])
                existing = None
            
            if existing:
                role_id = existing['role_id']
                # 更新角色
                await db.execute("""
                    UPDATE data_roles SET 
                        role_name = $1, description = $2, scope_type = $3, is_default = $4, is_active = $5
                    WHERE role_id = $6
                """, role_data['role_name'], role_data.get('description'), 
                    role_data.get('scope_type', 'limited'),
                    role_data.get('is_default', False), role_data.get('is_active', True), role_id)
            else:
                # 创建新角色
                row = await db.fetchrow("""
                    INSERT INTO data_roles (role_name, role_code, description, scope_type, is_default, is_active, created_by)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING role_id
                """, role_data['role_name'], role_data['role_code'],
                    role_data.get('description'), role_data.get('scope_type', 'limited'),
                    role_data.get('is_default', False), role_data.get('is_active', True), 
                    UUID(str(current_user.user_id)))
                role_id = row['role_id']
                result.roles_imported += 1
            
            # 导入表权限（已移除连接权限层）
            for perm in role_data.get('table_permissions', []):
                # 查找连接和表ID
                table_row = await db.fetchrow("""
                    SELECT t.table_id FROM db_tables t
                    JOIN database_connections dc ON t.connection_id = dc.connection_id
                    WHERE dc.connection_name = $1 AND t.table_name = $2 
                    AND (t.schema_name = $3 OR ($3 IS NULL AND t.schema_name IS NULL))
                """, perm['connection_name'], perm['table_name'], perm.get('schema_name'))
                
                if not table_row:
                    result.warnings.append(f"表 {perm.get('connection_name')}.{perm.get('schema_name', '')}.{perm['table_name']} 不存在，跳过")
                    continue
                
                table_id = table_row['table_id']
                
                await db.execute("""
                    INSERT INTO role_table_permissions (role_id, table_id, can_query, can_export, column_access_mode)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (role_id, table_id) DO UPDATE SET
                        can_query = EXCLUDED.can_query,
                        can_export = EXCLUDED.can_export,
                        column_access_mode = EXCLUDED.column_access_mode
                """, role_id, table_id, perm.get('can_query', True), 
                    perm.get('can_export', False), perm.get('column_access_mode', 'blacklist'))
                result.permissions_imported += 1
            
            # 导入行过滤规则
            for f in role_data.get('row_filters', []):
                # 查找表ID（如果指定）
                table_id = None
                if f.get('table_name'):
                    table_row = await db.fetchrow(
                        "SELECT table_id FROM db_tables WHERE table_name = $1",
                        f['table_name']
                    )
                    if table_row:
                        table_id = table_row['table_id']
                
                await db.execute("""
                    INSERT INTO role_row_filters (role_id, filter_name, description, table_id, filter_definition, priority, is_active)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, role_id, f['filter_name'], f.get('description'), table_id,
                    json.dumps(f['filter_definition']), f.get('priority', 0), f.get('is_active', True))
                result.filters_imported += 1
        
        logger.info(f"权限导入完成", **result.model_dump())
        
    except Exception as e:
        logger.error(f"权限导入失败: {e}")
        result.success = False
        result.errors.append(str(e))
    
    return result


@router.post("/clone/{source_role_id}")
async def clone_role(
    source_role_id: UUID,
    new_role_code: str = Query(..., description="新角色编码"),
    new_role_name: str = Query(..., description="新角色名称"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """
    克隆数据角色（包含所有权限配置）
    """
    import json
    
    # 获取源角色
    source = await db.fetchrow("SELECT * FROM data_roles WHERE role_id = $1", source_role_id)
    if not source:
        raise HTTPException(status_code=404, detail="源角色不存在")
    
    # 创建新角色
    new_role = await db.fetchrow("""
        INSERT INTO data_roles (role_name, role_code, description, scope_type, is_default, is_active, created_by)
        VALUES ($1, $2, $3, $4, FALSE, TRUE, $5)
        RETURNING role_id
    """, new_role_name, new_role_code, 
        f"克隆自 {source['role_name']}", source['scope_type'], UUID(str(current_user.user_id)))
    
    new_role_id = new_role['role_id']
    
    # 复制表权限（已移除连接权限层）
    await db.execute("""
        INSERT INTO role_table_permissions (
            role_id, table_id, can_query, can_export, column_access_mode,
            included_column_ids, excluded_column_ids, masked_column_ids,
            restricted_filter_column_ids, restricted_aggregate_column_ids
        )
        SELECT $1, table_id, can_query, can_export, column_access_mode,
               included_column_ids, excluded_column_ids, masked_column_ids,
               restricted_filter_column_ids, restricted_aggregate_column_ids
        FROM role_table_permissions
        WHERE role_id = $2
    """, new_role_id, source_role_id)
    
    # 复制行过滤规则
    await db.execute("""
        INSERT INTO role_row_filters (role_id, filter_name, description, table_id, filter_definition, priority, is_active)
        SELECT $1, filter_name, description, table_id, filter_definition, priority, is_active
        FROM role_row_filters
        WHERE role_id = $2
    """, new_role_id, source_role_id)
    
    return {
        "success": True,
        "new_role_id": str(new_role_id),
        "message": f"角色已克隆为 {new_role_name}"
    }
