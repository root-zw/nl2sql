"""
组织架构服务
提供组织架构的CRUD、用户分配、数据角色关联等功能
"""

from typing import Optional, List, Dict, Any
from uuid import UUID
from datetime import datetime
import structlog

from server.models.organization import (
    OrganizationCreate, OrganizationUpdate, OrganizationInDB, OrganizationResponse,
    OrganizationTreeNode, OrgType,
    ExternalOrgInfo, OrgSyncResult,
    UserOrgAssignment, UserOrgBatchAssignment, UserWithOrg,
    OrgDataRoleAssign, OrgDataRoleResponse,
    UserEffectiveRole, UserEffectiveRolesResponse,
    OrgMembersQuery, OrgMembersResponse
)

logger = structlog.get_logger()


class OrganizationService:
    """组织架构服务"""

    def __init__(self, db):
        self.db = db

    # ========================================================================
    # 组织 CRUD
    # ========================================================================

    async def create_organization(
        self, 
        org: OrganizationCreate, 
        created_by: Optional[UUID] = None
    ) -> OrganizationInDB:
        """创建组织"""
        row = await self.db.fetchrow("""
            INSERT INTO organizations (
                org_code, org_name, parent_id, org_type, description, 
                sort_order, is_active, created_by
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
        """, 
            org.org_code, org.org_name, org.parent_id, org.org_type.value,
            org.description, org.sort_order, org.is_active, created_by
        )
        return OrganizationInDB(**dict(row))

    async def get_organization(self, org_id: UUID) -> Optional[OrganizationResponse]:
        """获取组织详情（含统计信息）"""
        row = await self.db.fetchrow("""
            SELECT * FROM v_organization_stats WHERE org_id = $1
        """, org_id)
        if not row:
            return None
        return OrganizationResponse(**dict(row))

    async def get_organization_by_code(self, org_code: str) -> Optional[OrganizationInDB]:
        """根据编码获取组织"""
        row = await self.db.fetchrow("""
            SELECT * FROM organizations WHERE org_code = $1
        """, org_code)
        if not row:
            return None
        return OrganizationInDB(**dict(row))

    async def list_organizations(
        self,
        parent_id: Optional[UUID] = None,
        is_active: Optional[bool] = None,
        org_type: Optional[OrgType] = None,
        search: Optional[str] = None,
        source_idp: Optional[str] = None
    ) -> List[OrganizationResponse]:
        """获取组织列表"""
        conditions = ["1=1"]
        params = []
        param_idx = 1

        if parent_id is not None:
            conditions.append(f"parent_id = ${param_idx}")
            params.append(parent_id)
            param_idx += 1
        
        if is_active is not None:
            conditions.append(f"is_active = ${param_idx}")
            params.append(is_active)
            param_idx += 1
        
        if org_type is not None:
            conditions.append(f"org_type = ${param_idx}")
            params.append(org_type.value)
            param_idx += 1
        
        if search:
            conditions.append(f"(org_name ILIKE ${param_idx} OR org_code ILIKE ${param_idx})")
            params.append(f"%{search}%")
            param_idx += 1
        
        if source_idp is not None:
            if source_idp == "":
                conditions.append("source_idp IS NULL")
            else:
                conditions.append(f"source_idp = ${param_idx}")
                params.append(source_idp)
                param_idx += 1

        where_clause = " AND ".join(conditions)
        rows = await self.db.fetch(f"""
            SELECT * FROM v_organization_stats
            WHERE {where_clause}
            ORDER BY level, sort_order, org_name
        """, *params)
        
        return [OrganizationResponse(**dict(row)) for row in rows]

    async def get_root_organizations(self) -> List[OrganizationResponse]:
        """获取根组织列表"""
        return await self.list_organizations(parent_id=None, is_active=True)

    async def update_organization(
        self, 
        org_id: UUID, 
        update: OrganizationUpdate
    ) -> Optional[OrganizationInDB]:
        """更新组织"""
        # 构建更新语句
        update_fields = []
        params = []
        param_idx = 1
        
        update_dict = update.model_dump(exclude_unset=True)
        for field, value in update_dict.items():
            if field == "org_type" and value is not None:
                value = value.value
            update_fields.append(f"{field} = ${param_idx}")
            params.append(value)
            param_idx += 1
        
        if not update_fields:
            return await self.get_organization(org_id)
        
        params.append(org_id)
        row = await self.db.fetchrow(f"""
            UPDATE organizations
            SET {", ".join(update_fields)}, updated_at = NOW()
            WHERE org_id = ${param_idx}
            RETURNING *
        """, *params)
        
        if not row:
            return None
        return OrganizationInDB(**dict(row))

    async def delete_organization(self, org_id: UUID) -> bool:
        """删除组织"""
        # 检查是否有子组织
        child_count = await self.db.fetchval("""
            SELECT COUNT(*) FROM organizations WHERE parent_id = $1
        """, org_id)
        if child_count > 0:
            raise ValueError("无法删除有子组织的组织，请先删除或移动子组织")
        
        # 将该组织的用户设为未分配
        await self.db.execute("""
            UPDATE users SET org_id = NULL WHERE org_id = $1
        """, org_id)
        
        result = await self.db.execute("""
            DELETE FROM organizations WHERE org_id = $1
        """, org_id)
        return result == "DELETE 1"

    # ========================================================================
    # 组织树
    # ========================================================================

    async def get_organization_tree(
        self, 
        root_id: Optional[UUID] = None,
        include_inactive: bool = False
    ) -> List[OrganizationTreeNode]:
        """获取组织树"""
        # 获取所有组织
        active_filter = "" if include_inactive else "WHERE is_active = TRUE"
        rows = await self.db.fetch(f"""
            SELECT 
                o.org_id, o.org_code, o.org_name, o.org_type, o.level,
                o.parent_id, o.is_active,
                (SELECT COUNT(*) FROM users u WHERE u.org_id = o.org_id AND u.is_active = TRUE) AS direct_user_count
            FROM organizations o
            {active_filter}
            ORDER BY o.level, o.sort_order, o.org_name
        """)
        
        # 构建节点字典
        nodes: Dict[UUID, OrganizationTreeNode] = {}
        for row in rows:
            node = OrganizationTreeNode(
                org_id=row['org_id'],
                org_code=row['org_code'],
                org_name=row['org_name'],
                org_type=OrgType(row['org_type']),
                level=row['level'],
                parent_id=row['parent_id'],
                is_active=row['is_active'],
                direct_user_count=row['direct_user_count'],
                children=[]
            )
            nodes[row['org_id']] = node
        
        # 构建树结构
        tree: List[OrganizationTreeNode] = []
        for node in nodes.values():
            if node.parent_id is None:
                if root_id is None:
                    tree.append(node)
            elif node.parent_id in nodes:
                nodes[node.parent_id].children.append(node)
        
        # 如果指定了根节点
        if root_id is not None and root_id in nodes:
            return [nodes[root_id]]
        
        return tree

    async def get_children(self, org_id: UUID) -> List[OrganizationResponse]:
        """获取直接子组织"""
        return await self.list_organizations(parent_id=org_id, is_active=True)

    async def get_descendants(self, org_id: UUID) -> List[OrganizationResponse]:
        """获取所有后代组织（递归）"""
        rows = await self.db.fetch("""
            WITH RECURSIVE descendants AS (
                SELECT org_id, org_code, org_name, parent_id, level
                FROM organizations
                WHERE parent_id = $1 AND is_active = TRUE
                
                UNION ALL
                
                SELECT o.org_id, o.org_code, o.org_name, o.parent_id, o.level
                FROM organizations o
                INNER JOIN descendants d ON o.parent_id = d.org_id
                WHERE o.is_active = TRUE
            )
            SELECT vs.* FROM v_organization_stats vs
            JOIN descendants d ON vs.org_id = d.org_id
            ORDER BY vs.level, vs.sort_order
        """, org_id)
        
        return [OrganizationResponse(**dict(row)) for row in rows]

    async def get_ancestors(self, org_id: UUID) -> List[OrganizationResponse]:
        """获取所有祖先组织（从父到根）"""
        rows = await self.db.fetch("""
            WITH RECURSIVE ancestors AS (
                SELECT org_id, parent_id, 0 AS depth
                FROM organizations
                WHERE org_id = $1
                
                UNION ALL
                
                SELECT o.org_id, o.parent_id, a.depth + 1
                FROM organizations o
                INNER JOIN ancestors a ON o.org_id = a.parent_id
                WHERE o.is_active = TRUE
            )
            SELECT vs.* FROM v_organization_stats vs
            JOIN ancestors a ON vs.org_id = a.org_id
            WHERE a.depth > 0
            ORDER BY a.depth DESC
        """, org_id)
        
        return [OrganizationResponse(**dict(row)) for row in rows]

    # ========================================================================
    # 外部组织同步
    # ========================================================================

    async def sync_external_organizations(
        self,
        provider_key: str,
        organizations: List[ExternalOrgInfo],
        clear_existing: bool = False,
        created_by: Optional[UUID] = None
    ) -> OrgSyncResult:
        """同步外部组织"""
        result = OrgSyncResult(total=len(organizations))
        
        try:
            if clear_existing:
                # 清除该来源的现有组织
                await self.db.execute("""
                    DELETE FROM organizations WHERE source_idp = $1
                """, provider_key)
                logger.info(f"已清除来源 {provider_key} 的现有组织")
            
            # 第一遍：创建/更新所有组织（不设置父级）
            org_id_map: Dict[str, UUID] = {}  # external_id -> org_id
            
            for ext_org in organizations:
                try:
                    # 查找是否已存在
                    existing = await self.db.fetchrow("""
                        SELECT org_id FROM organizations
                        WHERE source_idp = $1 AND external_org_id = $2
                    """, provider_key, ext_org.external_org_id)
                    
                    org_code = ext_org.org_code or f"{provider_key}_{ext_org.external_org_id}"
                    
                    if existing:
                        # 更新
                        await self.db.execute("""
                            UPDATE organizations SET
                                org_name = $1,
                                org_type = $2,
                                description = $3,
                                updated_at = NOW()
                            WHERE org_id = $4
                        """, ext_org.org_name, ext_org.org_type or 'department',
                            ext_org.description, existing['org_id'])
                        org_id_map[ext_org.external_org_id] = existing['org_id']
                        result.updated += 1
                    else:
                        # 创建
                        row = await self.db.fetchrow("""
                            INSERT INTO organizations (
                                org_code, org_name, org_type, description,
                                source_idp, external_org_id, created_by
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            RETURNING org_id
                        """, org_code, ext_org.org_name, ext_org.org_type or 'department',
                            ext_org.description, provider_key, ext_org.external_org_id, created_by)
                        org_id_map[ext_org.external_org_id] = row['org_id']
                        result.created += 1
                        
                except Exception as e:
                    result.errors.append(f"组织 {ext_org.external_org_id}: {str(e)}")
                    result.skipped += 1
            
            # 第二遍：设置父级关系
            for ext_org in organizations:
                if ext_org.parent_external_id and ext_org.external_org_id in org_id_map:
                    parent_org_id = org_id_map.get(ext_org.parent_external_id)
                    if parent_org_id:
                        await self.db.execute("""
                            UPDATE organizations SET parent_id = $1
                            WHERE org_id = $2
                        """, parent_org_id, org_id_map[ext_org.external_org_id])
            
            result.success = len(result.errors) == 0
            logger.info(f"组织同步完成", **result.model_dump())
            
        except Exception as e:
            logger.error(f"组织同步失败: {e}")
            result.success = False
            result.errors.append(str(e))
        
        return result


class UserOrganizationService:
    """用户-组织关联服务"""

    def __init__(self, db):
        self.db = db

    async def assign_user_to_org(
        self,
        user_id: UUID,
        org_id: Optional[UUID],
        position: Optional[str] = None
    ) -> bool:
        """分配用户到组织"""
        result = await self.db.execute("""
            UPDATE users SET org_id = $1, position = $2, updated_at = NOW()
            WHERE user_id = $3
        """, org_id, position, user_id)
        return result == "UPDATE 1"

    async def batch_assign_users_to_org(
        self,
        assignment: UserOrgBatchAssignment
    ) -> int:
        """批量分配用户到组织"""
        result = await self.db.execute("""
            UPDATE users SET org_id = $1, position = $2, updated_at = NOW()
            WHERE user_id = ANY($3)
        """, assignment.org_id, assignment.position, assignment.user_ids)
        # 解析 UPDATE N
        if result.startswith("UPDATE "):
            return int(result.split(" ")[1])
        return 0

    async def get_user_with_org(self, user_id: UUID) -> Optional[UserWithOrg]:
        """获取用户及其组织信息"""
        row = await self.db.fetchrow("""
            SELECT 
                u.user_id, u.username, u.email, u.full_name, u.role,
                u.is_active, u.org_id, u.position, u.external_idp,
                o.org_name, o.org_code
            FROM users u
            LEFT JOIN organizations o ON u.org_id = o.org_id
            WHERE u.user_id = $1
        """, user_id)
        if not row:
            return None
        return UserWithOrg(**dict(row))

    async def get_org_members(
        self,
        org_id: UUID,
        query: OrgMembersQuery
    ) -> OrgMembersResponse:
        """获取组织成员列表"""
        # 获取组织信息
        org = await self.db.fetchrow("""
            SELECT org_id, org_name FROM organizations WHERE org_id = $1
        """, org_id)
        if not org:
            raise ValueError("组织不存在")
        
        # 构建查询条件
        conditions = []
        params = []
        param_idx = 1
        
        if query.include_children:
            # 包含子组织成员
            conditions.append(f"""
                u.org_id IN (
                    WITH RECURSIVE descendants AS (
                        SELECT org_id FROM organizations WHERE org_id = ${param_idx}
                        UNION ALL
                        SELECT o.org_id FROM organizations o
                        INNER JOIN descendants d ON o.parent_id = d.org_id
                    )
                    SELECT org_id FROM descendants
                )
            """)
        else:
            conditions.append(f"u.org_id = ${param_idx}")
        params.append(org_id)
        param_idx += 1
        
        if query.is_active is not None:
            conditions.append(f"u.is_active = ${param_idx}")
            params.append(query.is_active)
            param_idx += 1
        
        if query.search:
            conditions.append(f"(u.username ILIKE ${param_idx} OR u.full_name ILIKE ${param_idx} OR u.email ILIKE ${param_idx})")
            params.append(f"%{query.search}%")
            param_idx += 1
        
        where_clause = " AND ".join(conditions)
        
        # 查询总数
        total = await self.db.fetchval(f"""
            SELECT COUNT(*) FROM users u WHERE {where_clause}
        """, *params)
        
        # 查询成员
        offset = (query.page - 1) * query.page_size
        params.extend([query.page_size, offset])
        
        rows = await self.db.fetch(f"""
            SELECT 
                u.user_id, u.username, u.email, u.full_name, u.role,
                u.is_active, u.org_id, u.position, u.external_idp,
                o.org_name, o.org_code
            FROM users u
            LEFT JOIN organizations o ON u.org_id = o.org_id
            WHERE {where_clause}
            ORDER BY u.username
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """, *params)
        
        members = [UserWithOrg(**dict(row)) for row in rows]
        
        return OrgMembersResponse(
            org_id=org['org_id'],
            org_name=org['org_name'],
            include_children=query.include_children,
            total=total,
            page=query.page,
            page_size=query.page_size,
            members=members
        )

    async def get_unassigned_users(
        self,
        search: Optional[str] = None,
        is_active: Optional[bool] = True
    ) -> List[UserWithOrg]:
        """获取未分配组织的用户"""
        conditions = ["u.org_id IS NULL"]
        params = []
        param_idx = 1
        
        if is_active is not None:
            conditions.append(f"u.is_active = ${param_idx}")
            params.append(is_active)
            param_idx += 1
        
        if search:
            conditions.append(f"(u.username ILIKE ${param_idx} OR u.full_name ILIKE ${param_idx})")
            params.append(f"%{search}%")
            param_idx += 1
        
        where_clause = " AND ".join(conditions)
        rows = await self.db.fetch(f"""
            SELECT 
                u.user_id, u.username, u.email, u.full_name, u.role,
                u.is_active, u.org_id, u.position, u.external_idp,
                NULL AS org_name, NULL AS org_code
            FROM users u
            WHERE {where_clause}
            ORDER BY u.username
        """, *params)
        
        return [UserWithOrg(**dict(row)) for row in rows]


class OrgDataRoleService:
    """组织-数据角色关联服务"""

    def __init__(self, db):
        self.db = db

    async def assign_role_to_org(
        self,
        org_id: UUID,
        role_id: UUID,
        granted_by: Optional[UUID] = None,
        inherit_to_children: bool = True
    ) -> OrgDataRoleResponse:
        """为组织分配数据角色"""
        await self.db.execute("""
            INSERT INTO org_data_roles (org_id, role_id, granted_by, inherit_to_children)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (org_id, role_id) DO UPDATE SET
                granted_by = EXCLUDED.granted_by,
                granted_at = NOW(),
                inherit_to_children = EXCLUDED.inherit_to_children,
                is_active = TRUE
        """, org_id, role_id, granted_by, inherit_to_children)
        
        return await self.get_org_role(org_id, role_id)

    async def get_org_role(self, org_id: UUID, role_id: UUID) -> Optional[OrgDataRoleResponse]:
        """获取组织的单个数据角色"""
        row = await self.db.fetchrow("""
            SELECT 
                odr.*, dr.role_name, dr.role_code, dr.scope_type,
                o.org_name
            FROM org_data_roles odr
            JOIN data_roles dr ON odr.role_id = dr.role_id
            JOIN organizations o ON odr.org_id = o.org_id
            WHERE odr.org_id = $1 AND odr.role_id = $2
        """, org_id, role_id)
        if not row:
            return None
        return OrgDataRoleResponse(**dict(row))

    async def get_org_roles(self, org_id: UUID) -> List[OrgDataRoleResponse]:
        """获取组织的所有数据角色"""
        rows = await self.db.fetch("""
            SELECT 
                odr.*, dr.role_name, dr.role_code, dr.scope_type,
                o.org_name
            FROM org_data_roles odr
            JOIN data_roles dr ON odr.role_id = dr.role_id
            JOIN organizations o ON odr.org_id = o.org_id
            WHERE odr.org_id = $1 AND odr.is_active = TRUE
            ORDER BY dr.role_name
        """, org_id)
        return [OrgDataRoleResponse(**dict(row)) for row in rows]

    async def remove_role_from_org(self, org_id: UUID, role_id: UUID) -> bool:
        """从组织移除数据角色"""
        result = await self.db.execute("""
            DELETE FROM org_data_roles WHERE org_id = $1 AND role_id = $2
        """, org_id, role_id)
        return result == "DELETE 1"

    async def get_role_organizations(self, role_id: UUID) -> List[Dict[str, Any]]:
        """获取拥有某角色的所有组织"""
        rows = await self.db.fetch("""
            SELECT 
                o.org_id, o.org_code, o.org_name, o.org_type, o.level,
                odr.inherit_to_children, odr.granted_at,
                (SELECT COUNT(*) FROM users u WHERE u.org_id = o.org_id AND u.is_active = TRUE) AS user_count
            FROM org_data_roles odr
            JOIN organizations o ON odr.org_id = o.org_id
            WHERE odr.role_id = $1 AND odr.is_active = TRUE AND o.is_active = TRUE
            ORDER BY o.level, o.org_name
        """, role_id)
        return [dict(row) for row in rows]


class UserEffectiveRoleService:
    """用户有效权限服务（包含组织继承）"""

    def __init__(self, db):
        self.db = db

    async def get_user_effective_roles(self, user_id: UUID) -> UserEffectiveRolesResponse:
        """获取用户的有效数据角色（包含组织继承）"""
        # 获取用户基本信息
        user = await self.db.fetchrow("""
            SELECT u.user_id, u.username, u.org_id, o.org_name
            FROM users u
            LEFT JOIN organizations o ON u.org_id = o.org_id
            WHERE u.user_id = $1
        """, user_id)
        
        if not user:
            raise ValueError("用户不存在")
        
        # 从视图获取有效角色
        rows = await self.db.fetch("""
            SELECT * FROM v_user_effective_data_roles
            WHERE user_id = $1
        """, user_id)
        
        roles = [UserEffectiveRole(**dict(row)) for row in rows]
        
        # 统计
        direct_count = sum(1 for r in roles if r.grant_source == 'direct')
        inherited_count = len(roles) - direct_count
        
        return UserEffectiveRolesResponse(
            user_id=user['user_id'],
            username=user['username'],
            org_id=user['org_id'],
            org_name=user['org_name'],
            roles=roles,
            direct_roles_count=direct_count,
            inherited_roles_count=inherited_count
        )

    async def check_user_has_role(self, user_id: UUID, role_code: str) -> bool:
        """检查用户是否拥有某角色（包含继承）"""
        result = await self.db.fetchval("""
            SELECT COUNT(*) FROM v_user_effective_data_roles
            WHERE user_id = $1 AND role_code = $2
        """, user_id, role_code)
        return result > 0

