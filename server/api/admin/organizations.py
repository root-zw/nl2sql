"""
组织架构管理API
提供组织架构的CRUD、用户分配、数据角色关联等接口
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import Optional, List, Dict, Any
from uuid import UUID
import structlog

from server.models.organization import (
    OrganizationCreate, OrganizationUpdate, OrganizationResponse,
    OrganizationTreeNode, OrgType,
    ExternalOrgInfo, OrgSyncRequest, OrgSyncResult,
    UserOrgAssignment, UserOrgBatchAssignment, UserWithOrg,
    OrgDataRoleAssign, OrgDataRoleResponse,
    UserEffectiveRolesResponse,
    OrgMembersQuery, OrgMembersResponse
)
from server.services.organization_service import (
    OrganizationService, UserOrganizationService, 
    OrgDataRoleService, UserEffectiveRoleService
)
from server.middleware.auth import require_data_admin, require_admin
from server.models.admin import User as AdminUser

logger = structlog.get_logger()

router = APIRouter(prefix="/organizations", tags=["组织架构管理"])


async def get_db_pool():
    """获取数据库连接池"""
    from server.utils.db_pool import get_metadata_pool
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


# ============================================================================
# 组织架构 CRUD
# ============================================================================

@router.get("", response_model=List[OrganizationResponse])
async def list_organizations(
    parent_id: Optional[UUID] = Query(None, description="父组织ID，NULL获取根组织"),
    is_active: Optional[bool] = Query(None, description="是否启用"),
    org_type: Optional[OrgType] = Query(None, description="组织类型"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    source_idp: Optional[str] = Query(None, description="外部来源（空字符串表示本地创建）"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取组织列表"""
    service = OrganizationService(db)
    return await service.list_organizations(parent_id, is_active, org_type, search, source_idp)


@router.get("/tree", response_model=List[OrganizationTreeNode])
async def get_organization_tree(
    root_id: Optional[UUID] = Query(None, description="根节点ID，NULL获取完整树"),
    include_inactive: bool = Query(False, description="是否包含停用的组织"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取组织树形结构"""
    service = OrganizationService(db)
    return await service.get_organization_tree(root_id, include_inactive)


@router.get("/roots", response_model=List[OrganizationResponse])
async def get_root_organizations(
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取根组织列表"""
    service = OrganizationService(db)
    return await service.get_root_organizations()


@router.post("", response_model=OrganizationResponse, status_code=status.HTTP_201_CREATED)
async def create_organization(
    org: OrganizationCreate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """创建组织"""
    service = OrganizationService(db)
    try:
        created_by = UUID(str(current_user.user_id))
        result = await service.create_organization(org, created_by)
        return await service.get_organization(result.org_id)
    except Exception as e:
        logger.error(f"创建组织失败: {e}")
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=400, detail="组织编码已存在")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取组织详情"""
    service = OrganizationService(db)
    org = await service.get_organization(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="组织不存在")
    return org


@router.put("/{org_id}", response_model=OrganizationResponse)
async def update_organization(
    org_id: UUID,
    update: OrganizationUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """更新组织"""
    service = OrganizationService(db)
    result = await service.update_organization(org_id, update)
    if not result:
        raise HTTPException(status_code=404, detail="组织不存在")
    return await service.get_organization(org_id)


@router.delete("/{org_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_organization(
    org_id: UUID,
    current_user: AdminUser = Depends(require_admin),
    db=Depends(get_db_pool)
):
    """删除组织（仅管理员）"""
    service = OrganizationService(db)
    try:
        if not await service.delete_organization(org_id):
            raise HTTPException(status_code=404, detail="组织不存在")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{org_id}/children", response_model=List[OrganizationResponse])
async def get_organization_children(
    org_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取直接子组织"""
    service = OrganizationService(db)
    return await service.get_children(org_id)


@router.get("/{org_id}/descendants", response_model=List[OrganizationResponse])
async def get_organization_descendants(
    org_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取所有后代组织"""
    service = OrganizationService(db)
    return await service.get_descendants(org_id)


@router.get("/{org_id}/ancestors", response_model=List[OrganizationResponse])
async def get_organization_ancestors(
    org_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取所有祖先组织（从父到根）"""
    service = OrganizationService(db)
    return await service.get_ancestors(org_id)


# ============================================================================
# 组织成员管理
# ============================================================================

@router.get("/{org_id}/members", response_model=OrgMembersResponse)
async def get_organization_members(
    org_id: UUID,
    include_children: bool = Query(False, description="是否包含子组织成员"),
    is_active: Optional[bool] = Query(None, description="用户状态筛选"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取组织成员列表"""
    service = UserOrganizationService(db)
    query = OrgMembersQuery(
        include_children=include_children,
        is_active=is_active,
        search=search,
        page=page,
        page_size=page_size
    )
    try:
        return await service.get_org_members(org_id, query)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/users/unassigned", response_model=List[UserWithOrg])
async def get_unassigned_users(
    search: Optional[str] = Query(None, description="搜索关键词"),
    is_active: Optional[bool] = Query(True, description="用户状态"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取未分配组织的用户"""
    service = UserOrganizationService(db)
    return await service.get_unassigned_users(search, is_active)


@router.put("/users/{user_id}/org")
async def assign_user_to_organization(
    user_id: UUID,
    org_id: Optional[UUID] = Query(None, description="组织ID，NULL表示取消分配"),
    position: Optional[str] = Query(None, description="职位"),
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """分配用户到组织"""
    service = UserOrganizationService(db)
    success = await service.assign_user_to_org(user_id, org_id, position)
    if not success:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    # 返回更新后的用户信息
    user = await service.get_user_with_org(user_id)
    return user


@router.put("/users/batch-assign")
async def batch_assign_users_to_organization(
    assignment: UserOrgBatchAssignment,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """批量分配用户到组织"""
    service = UserOrganizationService(db)
    count = await service.batch_assign_users_to_org(assignment)
    return {"updated": count}


@router.get("/users/{user_id}", response_model=UserWithOrg)
async def get_user_with_organization(
    user_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取用户及其组织信息"""
    service = UserOrganizationService(db)
    user = await service.get_user_with_org(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    return user


# ============================================================================
# 组织数据角色管理
# ============================================================================

@router.get("/{org_id}/data-roles", response_model=List[OrgDataRoleResponse])
async def get_organization_data_roles(
    org_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取组织的数据角色列表"""
    service = OrgDataRoleService(db)
    return await service.get_org_roles(org_id)


@router.post("/{org_id}/data-roles", response_model=OrgDataRoleResponse, status_code=status.HTTP_201_CREATED)
async def assign_data_role_to_organization(
    org_id: UUID,
    assignment: OrgDataRoleAssign,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """为组织分配数据角色"""
    service = OrgDataRoleService(db)
    granted_by = UUID(str(current_user.user_id))
    return await service.assign_role_to_org(
        org_id, 
        assignment.role_id, 
        granted_by,
        assignment.inherit_to_children
    )


@router.delete("/{org_id}/data-roles/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_data_role_from_organization(
    org_id: UUID,
    role_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """从组织移除数据角色"""
    service = OrgDataRoleService(db)
    if not await service.remove_role_from_org(org_id, role_id):
        raise HTTPException(status_code=404, detail="组织角色关联不存在")


@router.get("/data-roles/{role_id}/organizations", response_model=List[Dict[str, Any]])
async def get_data_role_organizations(
    role_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取拥有某数据角色的所有组织"""
    service = OrgDataRoleService(db)
    return await service.get_role_organizations(role_id)


# ============================================================================
# 用户有效权限（包含组织继承）
# ============================================================================

@router.get("/users/{user_id}/effective-roles", response_model=UserEffectiveRolesResponse)
async def get_user_effective_data_roles(
    user_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取用户的有效数据角色（包含组织继承）"""
    service = UserEffectiveRoleService(db)
    try:
        return await service.get_user_effective_roles(user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ============================================================================
# 外部组织同步
# ============================================================================

@router.post("/sync", response_model=OrgSyncResult)
async def sync_external_organizations(
    request: OrgSyncRequest,
    current_user: AdminUser = Depends(require_admin),
    db=Depends(get_db_pool)
):
    """
    同步外部组织架构
    
    用于从OIDC/LDAP等外部系统同步组织结构
    """
    service = OrganizationService(db)
    created_by = UUID(str(current_user.user_id))
    return await service.sync_external_organizations(
        request.provider_key,
        request.organizations,
        request.clear_existing,
        created_by
    )


@router.get("/by-source/{source_idp}", response_model=List[OrganizationResponse])
async def get_organizations_by_source(
    source_idp: str,
    current_user: AdminUser = Depends(require_data_admin),
    db=Depends(get_db_pool)
):
    """获取指定来源的组织列表"""
    service = OrganizationService(db)
    return await service.list_organizations(source_idp=source_idp)

