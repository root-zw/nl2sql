"""
认证提供者配置服务：读写 auth_providers 表。
"""
from typing import List, Optional
from uuid import UUID
import json
import structlog

from server.models.auth_provider import (
    AuthProviderCreate,
    AuthProviderUpdate,
    AuthProviderInDB,
    AuthProviderBase,
)

logger = structlog.get_logger()


def _convert_row(row) -> dict:
    """转换数据库行为字典，处理 JSONB 字段"""
    if not row:
        return None
    data = dict(row)
    # 确保 config_json 是字典类型
    if 'config_json' in data:
        cfg = data['config_json']
        if isinstance(cfg, str):
            try:
                data['config_json'] = json.loads(cfg)
            except (json.JSONDecodeError, TypeError):
                data['config_json'] = {}
        elif cfg is None:
            data['config_json'] = {}
    return data


class AuthProviderService:
    def __init__(self, db_pool):
        self.db = db_pool

    async def create(self, data: AuthProviderCreate) -> AuthProviderInDB:
        # 确保 config_json 是 JSON 字符串
        config_json = data.config_json
        if isinstance(config_json, dict):
            config_json = json.dumps(config_json, ensure_ascii=False)
        
        row = await self.db.fetchrow(
            """
            INSERT INTO auth_providers (provider_key, provider_type, config_json, enabled, priority)
            VALUES ($1, $2, $3::jsonb, $4, $5)
            RETURNING *
            """,
            data.provider_key,
            data.provider_type,
            config_json,
            data.enabled,
            data.priority,
        )
        return AuthProviderInDB(**_convert_row(row))

    async def list(self) -> List[AuthProviderInDB]:
        rows = await self.db.fetch("SELECT * FROM auth_providers ORDER BY priority DESC, created_at DESC")
        return [AuthProviderInDB(**_convert_row(r)) for r in rows]

    async def get(self, provider_id: UUID) -> Optional[AuthProviderInDB]:
        row = await self.db.fetchrow("SELECT * FROM auth_providers WHERE provider_id = $1", provider_id)
        return AuthProviderInDB(**_convert_row(row)) if row else None

    async def get_by_key(self, provider_key: str) -> Optional[AuthProviderInDB]:
        row = await self.db.fetchrow("SELECT * FROM auth_providers WHERE provider_key = $1", provider_key)
        return AuthProviderInDB(**_convert_row(row)) if row else None

    async def update(self, provider_id: UUID, data: AuthProviderUpdate) -> Optional[AuthProviderInDB]:
        update_fields = []
        params = [provider_id]

        if data.config_json is not None:
            # 确保 config_json 是 JSON 字符串
            config_json = data.config_json
            if isinstance(config_json, dict):
                config_json = json.dumps(config_json, ensure_ascii=False)
            params.append(config_json)
            update_fields.append(f"config_json = ${len(params)}::jsonb")
        if data.enabled is not None:
            params.append(data.enabled)
            update_fields.append(f"enabled = ${len(params)}")
        if data.priority is not None:
            params.append(data.priority)
            update_fields.append(f"priority = ${len(params)}")

        if not update_fields:
            return await self.get(provider_id)

        update_fields.append("updated_at = CURRENT_TIMESTAMP")

        row = await self.db.fetchrow(
            f"""
            UPDATE auth_providers
            SET {', '.join(update_fields)}
            WHERE provider_id = $1
            RETURNING *
            """,
            *params,
        )
        return AuthProviderInDB(**_convert_row(row)) if row else None

    async def delete(self, provider_id: UUID) -> bool:
        res = await self.db.execute("DELETE FROM auth_providers WHERE provider_id = $1", provider_id)
        return res == "DELETE 1"

