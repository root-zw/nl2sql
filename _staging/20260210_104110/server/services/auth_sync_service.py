"""
认证同步服务：负责从认证服务器同步数据角色和用户属性到本地数据库

功能：
1. 自动创建数据角色（data_roles 表）
2. 关联用户与数据角色（user_data_roles 表）
3. 同步用户属性（user_attributes 表）
"""

import json
from typing import Any, Dict, List, Optional
from uuid import UUID

import structlog

logger = structlog.get_logger()


async def sync_data_roles_from_auth(
    conn,
    user_id: UUID,
    external_roles: List[str],
    auto_create: bool = True,
    source: str = "oidc"
) -> Dict[str, Any]:
    """
    从认证服务器同步数据角色到本地
    
    Args:
        conn: 数据库连接
        user_id: 本地用户ID
        external_roles: 认证服务器返回的角色列表
        auto_create: 是否自动创建不存在的角色
        source: 认证来源标识
    
    Returns:
        同步结果统计
    """
    if not external_roles:
        logger.debug("无数据角色需要同步", user_id=str(user_id))
        return {"created_roles": 0, "linked_roles": 0, "disabled_roles": 0}
    
    created_roles = 0
    linked_roles = 0
    disabled_roles = 0
    
    try:
        # 1. 自动创建不存在的数据角色
        if auto_create:
            for role_code in external_roles:
                if not role_code or not role_code.strip():
                    continue
                role_code = role_code.strip()
                
                result = await conn.execute("""
                    INSERT INTO data_roles (role_code, role_name, description, scope_type, is_active)
                    VALUES ($1, $1, $2, 'limited', TRUE)
                    ON CONFLICT (role_code) DO NOTHING
                """, role_code, f"从 {source} 自动创建的数据角色")
                
                if result == "INSERT 0 1":
                    created_roles += 1
                    logger.info("自动创建数据角色", role_code=role_code, source=source)
        
        # 2. 获取角色ID列表
        role_rows = await conn.fetch("""
            SELECT role_id, role_code FROM data_roles 
            WHERE role_code = ANY($1) AND is_active = TRUE
        """, external_roles)
        
        role_id_map = {row['role_code']: row['role_id'] for row in role_rows}
        
        # 3. 关联用户到角色（激活已有关联或创建新关联）
        for role_code in external_roles:
            role_code = role_code.strip() if role_code else ""
            if not role_code or role_code not in role_id_map:
                continue
            
            role_id = role_id_map[role_code]
            
            result = await conn.execute("""
                INSERT INTO user_data_roles (user_id, role_id, is_active, granted_at)
                VALUES ($1, $2, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id, role_id) DO UPDATE SET 
                    is_active = TRUE,
                    granted_at = CURRENT_TIMESTAMP
            """, user_id, role_id)
            
            linked_roles += 1
        
        # 4. 禁用用户不再拥有的角色（仅处理自动同步的，保留手动分配的）
        # 通过 granted_by IS NULL 判断是自动同步的角色
        result = await conn.execute("""
            UPDATE user_data_roles 
            SET is_active = FALSE
            WHERE user_id = $1 
            AND is_active = TRUE
            AND granted_by IS NULL
            AND role_id NOT IN (
                SELECT role_id FROM data_roles WHERE role_code = ANY($2) AND is_active = TRUE
            )
        """, user_id, external_roles)
        
        # 解析受影响的行数
        if result and "UPDATE" in result:
            try:
                disabled_roles = int(result.split(" ")[1])
            except (IndexError, ValueError):
                pass
        
        if disabled_roles > 0:
            logger.info("禁用失效数据角色", user_id=str(user_id), count=disabled_roles)
        
        logger.info(
            "数据角色同步完成",
            user_id=str(user_id),
            created=created_roles,
            linked=linked_roles,
            disabled=disabled_roles,
            roles=external_roles
        )
        
        return {
            "created_roles": created_roles,
            "linked_roles": linked_roles,
            "disabled_roles": disabled_roles
        }
        
    except Exception as e:
        logger.error("数据角色同步失败", user_id=str(user_id), error=str(e))
        raise


async def sync_user_attributes_from_auth(
    conn,
    user_id: UUID,
    claims: Dict[str, Any],
    attribute_claims: List[str],
    source: str = "oidc"
) -> Dict[str, Any]:
    """
    从认证服务器同步用户属性到本地
    
    Args:
        conn: 数据库连接
        user_id: 本地用户ID
        claims: 认证服务器返回的所有 claims
        attribute_claims: 需要同步的属性字段名列表
        source: 认证来源标识
    
    Returns:
        同步结果统计
    """
    if not attribute_claims:
        return {"synced_attributes": 0}
    
    synced_count = 0
    synced_attrs = {}
    
    try:
        for attr_name in attribute_claims:
            attr_name = attr_name.strip() if attr_name else ""
            if not attr_name or attr_name not in claims:
                continue
            
            value = claims[attr_name]
            
            # 空值跳过
            if value is None:
                continue
            
            # 数组类型转换为 JSON 字符串存储
            if isinstance(value, (list, dict)):
                value_str = json.dumps(value, ensure_ascii=False)
            else:
                value_str = str(value)
            
            await conn.execute("""
                INSERT INTO user_attributes (user_id, attribute_name, attribute_value)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, attribute_name) DO UPDATE SET
                    attribute_value = EXCLUDED.attribute_value,
                    updated_at = CURRENT_TIMESTAMP
            """, user_id, attr_name, value_str)
            
            synced_count += 1
            synced_attrs[attr_name] = value_str
        
        if synced_count > 0:
            logger.info(
                "用户属性同步完成",
                user_id=str(user_id),
                source=source,
                synced_count=synced_count,
                attributes=list(synced_attrs.keys())
            )
        
        return {
            "synced_attributes": synced_count,
            "attributes": synced_attrs
        }
        
    except Exception as e:
        logger.error("用户属性同步失败", user_id=str(user_id), error=str(e))
        raise


async def sync_auth_context_to_db(
    conn,
    user_id: UUID,
    data_roles: List[str],
    user_attributes: Dict[str, Any],
    attribute_claims: List[str],
    auto_create_role: bool = True,
    source: str = "oidc"
) -> Dict[str, Any]:
    """
    从 AuthContext 同步数据角色和用户属性到数据库（便捷封装）
    
    Args:
        conn: 数据库连接
        user_id: 本地用户ID
        data_roles: 数据角色列表
        user_attributes: 用户属性字典（原始 claims 中的属性）
        attribute_claims: 需要同步的属性字段名列表
        auto_create_role: 是否自动创建角色
        source: 认证来源
    
    Returns:
        同步结果统计
    """
    result = {
        "roles": {},
        "attributes": {}
    }
    
    # 同步数据角色
    if data_roles:
        result["roles"] = await sync_data_roles_from_auth(
            conn, user_id, data_roles, auto_create_role, source
        )
    
    # 同步用户属性
    if attribute_claims and user_attributes:
        result["attributes"] = await sync_user_attributes_from_auth(
            conn, user_id, user_attributes, attribute_claims, source
        )
    
    return result


async def get_user_data_roles(conn, user_id: UUID) -> List[str]:
    """
    获取用户的数据角色列表
    
    Args:
        conn: 数据库连接
        user_id: 用户ID
    
    Returns:
        角色编码列表
    """
    rows = await conn.fetch("""
        SELECT dr.role_code
        FROM user_data_roles udr
        JOIN data_roles dr ON udr.role_id = dr.role_id
        WHERE udr.user_id = $1 AND udr.is_active = TRUE AND dr.is_active = TRUE
        ORDER BY dr.role_code
    """, user_id)
    
    return [row['role_code'] for row in rows]


async def get_user_attributes_dict(conn, user_id: UUID) -> Dict[str, str]:
    """
    获取用户的属性字典
    
    Args:
        conn: 数据库连接
        user_id: 用户ID
    
    Returns:
        属性名到属性值的字典
    """
    rows = await conn.fetch("""
        SELECT attribute_name, attribute_value
        FROM user_attributes
        WHERE user_id = $1
    """, user_id)
    
    return {row['attribute_name']: row['attribute_value'] for row in rows}

