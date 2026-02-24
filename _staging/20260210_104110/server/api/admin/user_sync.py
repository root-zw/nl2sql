"""
第三方用户同步 API：批量/单条同步外部用户，默认系统角色为 user，并绑定默认数据角色。
支持按认证提供者同步，支持数据角色和用户属性同步。
"""
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
import asyncpg
import json
import structlog

from server.middleware.auth import require_admin
from server.utils.db_pool import get_metadata_pool
from server.utils.password_validator import validate_password_strength
from server.api.admin.auth import hash_password, _bind_default_data_roles
from server.services.auth_sync_service import sync_auth_context_to_db
from server.config import settings
from pydantic import BaseModel, Field, EmailStr

logger = structlog.get_logger()

router = APIRouter()


class ExternalUser(BaseModel):
    """外部用户信息"""
    external_idp: str = Field(..., min_length=1, max_length=100, description="认证提供者标识")
    external_uid: str = Field(..., min_length=1, max_length=200, description="外部用户ID")
    username: Optional[str] = Field(None, min_length=3, max_length=50, description="用户名")
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    role: str = Field(default="user", description="系统角色，默认 user")
    is_active: bool = True
    profile: Optional[dict] = None
    # 新增：数据角色和用户属性
    data_roles: Optional[List[str]] = Field(default=None, description="数据角色列表")
    user_attributes: Optional[Dict[str, Any]] = Field(default=None, description="用户属性（用于行级权限）")


class SyncResult(BaseModel):
    """同步结果"""
    created: int
    updated: int
    skipped: int
    roles_synced: int = 0
    attributes_synced: int = 0
    provider_key: Optional[str] = None


class ProviderSyncRequest(BaseModel):
    """按提供者同步用户请求"""
    users: List[ExternalUser]


async def _upsert_external_user(conn, data: ExternalUser) -> tuple[bool, Optional[UUID]]:
    """
    创建或更新外部用户
    
    Returns:
        (is_created, user_id): 是否新建，用户ID
    """
    row = await conn.fetchrow(
        "SELECT user_id, is_active FROM users WHERE external_idp = $1 AND external_uid = $2",
        data.external_idp,
        data.external_uid,
    )
    profile_json = json.dumps(data.profile or {}, ensure_ascii=False)
    
    if not row:
        password_hash = hash_password(uuid4().hex)  # 随机占位
        new_row = await conn.fetchrow(
            """
            INSERT INTO users (username, password_hash, email, full_name, role, is_active, external_idp, external_uid, profile_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING user_id
            """,
            data.username or f"{data.external_idp}_{data.external_uid}",
            password_hash,
            data.email,
            data.full_name,
            data.role or "user",
            data.is_active,
            data.external_idp,
            data.external_uid,
            profile_json,
        )
        return True, new_row['user_id'] if new_row else None
    else:
        # 注意：不更新 role 字段，保留管理员手动修改的角色
        await conn.execute(
            """
            UPDATE users
            SET email = $2,
                full_name = $3,
                is_active = $4,
                profile_json = $5,
                updated_at = NOW()
            WHERE external_idp = $1 AND external_uid = $6
            """,
            data.external_idp,
            data.email,
            data.full_name,
            data.is_active,
            profile_json,
            data.external_uid,
        )
        return False, row['user_id']


async def _sync_one(conn, data: ExternalUser) -> Dict[str, Any]:
    """
    同步单个用户，包括数据角色和用户属性
    
    Returns:
        同步结果字典
    """
    result = {
        "created": False,
        "roles_synced": 0,
        "attributes_synced": 0
    }
    
    # 1. 创建或更新用户
    is_created, user_id = await _upsert_external_user(conn, data)
    result["created"] = is_created
    
    if not user_id:
        # 获取用户ID
        row = await conn.fetchrow(
            "SELECT user_id FROM users WHERE external_idp = $1 AND external_uid = $2",
            data.external_idp,
            data.external_uid,
        )
        user_id = row['user_id'] if row else None
    
    if not user_id:
        return result
    
    # 2. 同步数据角色和用户属性
    data_roles = data.data_roles or []
    user_attributes = data.user_attributes or {}
    
    if data_roles or user_attributes:
        try:
            sync_result = await sync_auth_context_to_db(
                conn=conn,
                user_id=user_id,
                data_roles=data_roles,
                user_attributes=user_attributes,
                attribute_claims=list(user_attributes.keys()),
                auto_create_role=settings.oidc_auto_create_data_role,
                source=data.external_idp
            )
            result["roles_synced"] = sync_result.get("roles", {}).get("linked_roles", 0)
            result["attributes_synced"] = sync_result.get("attributes", {}).get("synced_attributes", 0)
        except Exception as e:
            logger.warning(
                "同步数据角色/属性失败",
                user_id=str(user_id),
                external_idp=data.external_idp,
                error=str(e)
            )
    
    # 3. 绑定默认数据角色（补充）
    await _bind_default_data_roles(conn, user_id)
    
    return result


async def get_db_conn():
    pool = await get_metadata_pool()
    async with pool.acquire() as conn:
        yield conn


@router.post("/users/sync", response_model=SyncResult)
async def sync_users(
    users: List[ExternalUser],
    conn = Depends(get_db_conn),
    current_user=Depends(require_admin),
):
    """
    批量同步外部用户（通用接口）
    
    支持同步：
    - 用户基本信息
    - 数据角色（data_roles 字段）
    - 用户属性（user_attributes 字段）
    """
    created = 0
    updated = 0
    skipped = 0
    roles_synced = 0
    attributes_synced = 0

    for item in users:
        try:
            result = await _sync_one(conn, item)
            if result["created"]:
                created += 1
            else:
                updated += 1
            roles_synced += result.get("roles_synced", 0)
            attributes_synced += result.get("attributes_synced", 0)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "sync user failed",
                external_idp=item.external_idp,
                external_uid=item.external_uid,
                error=str(exc)
            )
            skipped += 1
    
    logger.info(
        "批量用户同步完成",
        created=created,
        updated=updated,
        skipped=skipped,
        roles_synced=roles_synced,
        attributes_synced=attributes_synced
    )

    return SyncResult(
        created=created,
        updated=updated,
        skipped=skipped,
        roles_synced=roles_synced,
        attributes_synced=attributes_synced
    )


@router.get("/users/by-provider/{provider_key}")
async def get_users_by_provider(
    provider_key: str,
    conn = Depends(get_db_conn),
    current_user=Depends(require_admin),
):
    """获取指定认证提供者的用户列表"""
    rows = await conn.fetch(
        """
        SELECT user_id, username, email, full_name, role, is_active, 
               external_idp, external_uid, profile_json, created_at, last_login_at
        FROM users
        WHERE external_idp = $1
        ORDER BY created_at DESC
        """,
        provider_key,
    )
    
    return {
        "provider_key": provider_key,
        "total": len(rows),
        "users": [dict(r) for r in rows]
    }


@router.get("/users/sync-stats")
async def get_sync_stats(
    conn = Depends(get_db_conn),
    current_user=Depends(require_admin),
):
    """获取各认证提供者的用户同步统计"""
    rows = await conn.fetch(
        """
        SELECT 
            external_idp,
            COUNT(*) as total_users,
            COUNT(*) FILTER (WHERE is_active = TRUE) as active_users,
            MAX(created_at) as last_sync_at
        FROM users
        WHERE external_idp IS NOT NULL
        GROUP BY external_idp
        ORDER BY total_users DESC
        """
    )
    
    return {
        "providers": [
            {
                "provider_key": r["external_idp"],
                "total_users": r["total_users"],
                "active_users": r["active_users"],
                "last_sync_at": r["last_sync_at"].isoformat() if r["last_sync_at"] else None,
            }
            for r in rows
        ]
    }

