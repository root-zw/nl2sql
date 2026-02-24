"""模型供应商管理服务

提供模型供应商、凭证、模型的CRUD操作，以及场景配置管理。
"""

import json
from typing import List, Optional, Dict, Any, Tuple
from uuid import UUID
from datetime import datetime
import structlog
from cryptography.fernet import Fernet
import httpx

from server.config import settings
from server.utils.db_pool import get_metadata_pool
from server.models.model_provider import (
    ProviderType, ModelType, LLMScenario,
    ProviderCreate, ProviderUpdate, ProviderInfo, ProviderDetail,
    CredentialCreate, CredentialUpdate, CredentialInfo,
    ModelCreate, ModelUpdate, ModelInfo,
    ScenarioConfigCreate, ScenarioConfigUpdate, ScenarioConfigInfo,
    AvailableModel, PresetProvider, PresetProviderModel,
)

logger = structlog.get_logger()

# 预置供应商定义
PRESET_PROVIDERS: List[PresetProvider] = [
    PresetProvider(
        name="dashscope",
        display_name="阿里云百炼",
        type=ProviderType.OPENAI_COMPATIBLE,
        icon="dashscope",
        default_base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        description="阿里云大模型服务平台，支持通义千问系列模型",
        models=[]  # 从API动态获取
    ),
    PresetProvider(
        name="deepseek",
        display_name="DeepSeek",
        type=ProviderType.OPENAI_COMPATIBLE,
        icon="deepseek",
        default_base_url="https://api.deepseek.com/v1",
        description="DeepSeek AI，高性价比的大模型服务",
        models=[]  # 从API动态获取
    ),
    PresetProvider(
        name="siliconflow",
        display_name="硅基流动",
        type=ProviderType.OPENAI_COMPATIBLE,
        icon="siliconflow",
        default_base_url="https://api.siliconflow.cn/v1",
        description="硅基流动，聚合多种开源模型的推理服务",
        models=[]  # 从API动态获取
    ),
    PresetProvider(
        name="openai_compatible",
        display_name="OpenAI 兼容接口",
        type=ProviderType.OPENAI_COMPATIBLE,
        icon="openai",
        default_base_url=None,
        description="自定义OpenAI兼容的API接口，支持从接口动态获取模型列表",
        models=[]  # 从API动态获取
    ),
]


class ModelProviderService:
    """模型供应商服务"""
    
    def __init__(self):
        self._fernet: Optional[Fernet] = None
    
    @property
    def fernet(self) -> Fernet:
        """获取加密器"""
        if self._fernet is None:
            self._fernet = Fernet(settings.encryption_key.encode())
        return self._fernet
    
    def _encrypt_api_key(self, api_key: str) -> str:
        """加密API Key"""
        return self.fernet.encrypt(api_key.encode()).decode()
    
    def _decrypt_api_key(self, encrypted_key: str) -> str:
        """解密API Key"""
        try:
            return self.fernet.decrypt(encrypted_key.encode()).decode()
        except Exception as e:
            logger.warning("解密API Key失败", error=str(e))
            return ""
    
    def _mask_api_key(self, api_key: str) -> str:
        """脱敏API Key显示"""
        if not api_key or len(api_key) < 8:
            return "****"
        return f"{api_key[:4]}****{api_key[-4:]}"
    
    # ========================================================================
    # 预置供应商
    # ========================================================================
    
    def get_preset_providers(self) -> List[PresetProvider]:
        """获取预置供应商列表"""
        return PRESET_PROVIDERS
    
    def get_preset_provider(self, provider_name: str) -> Optional[PresetProvider]:
        """获取指定预置供应商"""
        for provider in PRESET_PROVIDERS:
            if provider.name == provider_name:
                return provider
        return None
    
    # ========================================================================
    # 供应商管理
    # ========================================================================
    
    async def list_providers(self, include_disabled: bool = False) -> List[ProviderInfo]:
        """获取所有供应商列表"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            query = """
                SELECT 
                    p.*,
                    COUNT(DISTINCT c.credential_id) as credential_count,
                    COUNT(DISTINCT m.model_id) as model_count
                FROM model_providers p
                LEFT JOIN provider_credentials c ON p.provider_id = c.provider_id AND c.is_active = TRUE
                LEFT JOIN provider_models m ON p.provider_id = m.provider_id AND m.is_enabled = TRUE
            """
            if not include_disabled:
                query += " WHERE p.is_enabled = TRUE"
            query += " GROUP BY p.provider_id ORDER BY p.created_at"
            
            rows = await conn.fetch(query)
            return [self._row_to_provider_info(row) for row in rows]
    
    async def get_provider(self, provider_id: UUID) -> Optional[ProviderDetail]:
        """获取供应商详情"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT p.*,
                    COUNT(DISTINCT c.credential_id) as credential_count,
                    COUNT(DISTINCT m.model_id) as model_count
                FROM model_providers p
                LEFT JOIN provider_credentials c ON p.provider_id = c.provider_id AND c.is_active = TRUE
                LEFT JOIN provider_models m ON p.provider_id = m.provider_id AND m.is_enabled = TRUE
                WHERE p.provider_id = $1
                GROUP BY p.provider_id
            """, provider_id)
            
            if not row:
                return None
            
            # 获取凭证列表
            credentials = await self.list_credentials(provider_id)
            
            # 获取模型列表
            models = await self.list_models(provider_id)
            
            provider_info = self._row_to_provider_info(row)
            return ProviderDetail(
                **provider_info.model_dump(),
                credentials=credentials,
                models=models
            )
    
    async def get_provider_by_name(self, provider_name: str) -> Optional[ProviderInfo]:
        """根据名称获取供应商"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT p.*,
                    COUNT(DISTINCT c.credential_id) as credential_count,
                    COUNT(DISTINCT m.model_id) as model_count
                FROM model_providers p
                LEFT JOIN provider_credentials c ON p.provider_id = c.provider_id AND c.is_active = TRUE
                LEFT JOIN provider_models m ON p.provider_id = m.provider_id AND m.is_enabled = TRUE
                WHERE p.provider_name = $1
                GROUP BY p.provider_id
            """, provider_name)
            
            if not row:
                return None
            return self._row_to_provider_info(row)
    
    async def create_provider(
        self, 
        data: ProviderCreate, 
        created_by: Optional[UUID] = None
    ) -> ProviderInfo:
        """创建供应商"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO model_providers (
                    provider_name, display_name, provider_type, base_url, icon, description, created_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING *
            """, 
                data.provider_name,
                data.display_name,
                data.provider_type.value,
                data.base_url,
                data.icon,
                data.description,
                created_by
            )
            
            logger.info("创建供应商", provider_name=data.provider_name)
            return self._row_to_provider_info(row)
    
    async def update_provider(self, provider_id: UUID, data: ProviderUpdate) -> Optional[ProviderInfo]:
        """更新供应商"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 构建更新语句
            updates = []
            values = []
            idx = 1
            
            for field, value in data.model_dump(exclude_unset=True).items():
                updates.append(f"{field} = ${idx}")
                values.append(value)
                idx += 1
            
            if not updates:
                return await self.get_provider(provider_id)
            
            values.append(provider_id)
            query = f"""
                UPDATE model_providers SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                WHERE provider_id = ${idx}
                RETURNING *
            """
            
            row = await conn.fetchrow(query, *values)
            if row:
                logger.info("更新供应商", provider_id=str(provider_id))
                return self._row_to_provider_info(row)
            return None
    
    async def delete_provider(self, provider_id: UUID) -> bool:
        """删除供应商"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM model_providers WHERE provider_id = $1
            """, provider_id)
            
            deleted = result == "DELETE 1"
            if deleted:
                logger.info("删除供应商", provider_id=str(provider_id))
            return deleted
    
    async def add_provider_from_preset(
        self, 
        preset_name: str, 
        api_key: str,
        credential_name: str = "默认凭证",
        base_url: Optional[str] = None,
        created_by: Optional[UUID] = None
    ) -> Tuple[ProviderInfo, CredentialInfo]:
        """从预置模板添加供应商"""
        preset = self.get_preset_provider(preset_name)
        if not preset:
            raise ValueError(f"未找到预置供应商: {preset_name}")
        
        # 检查是否已存在
        existing = await self.get_provider_by_name(preset_name)
        if existing:
            raise ValueError(f"供应商已存在: {preset_name}")
        
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 创建供应商
                provider_row = await conn.fetchrow("""
                    INSERT INTO model_providers (
                        provider_name, display_name, provider_type, base_url, icon, description, created_by
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING *
                """, 
                    preset.name,
                    preset.display_name,
                    preset.type.value,
                    base_url or preset.default_base_url,
                    preset.icon,
                    preset.description,
                    created_by
                )
                provider_id = provider_row['provider_id']
                
                # 创建凭证
                encrypted_key = self._encrypt_api_key(api_key)
                credential_row = await conn.fetchrow("""
                    INSERT INTO provider_credentials (
                        provider_id, credential_name, encrypted_api_key, is_default
                    ) VALUES ($1, $2, $3, TRUE)
                    RETURNING *
                """, provider_id, credential_name, encrypted_key)
                
                # 添加预置模型
                for model in preset.models:
                    await conn.execute("""
                        INSERT INTO provider_models (
                            provider_id, model_name, display_name, model_type,
                            supports_function_calling, supports_json_mode, supports_vision,
                            context_window
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """, 
                        provider_id,
                        model.name,
                        model.display_name,
                        model.type.value,
                        model.supports_function_calling,
                        model.supports_json_mode,
                        model.supports_vision,
                        model.context_window
                    )
                
                logger.info("从预置模板添加供应商", 
                           preset_name=preset_name, 
                           provider_id=str(provider_id),
                           model_count=len(preset.models))
                
                provider_info = self._row_to_provider_info(provider_row)
                credential_info = self._row_to_credential_info(credential_row)
                
                return provider_info, credential_info
    
    # ========================================================================
    # 凭证管理
    # ========================================================================
    
    async def list_credentials(self, provider_id: UUID, include_inactive: bool = False) -> List[CredentialInfo]:
        """获取供应商的凭证列表"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            query = "SELECT * FROM provider_credentials WHERE provider_id = $1"
            if not include_inactive:
                query += " AND is_active = TRUE"
            query += " ORDER BY is_default DESC, created_at"
            
            rows = await conn.fetch(query, provider_id)
            return [self._row_to_credential_info(row) for row in rows]
    
    async def get_credential(self, credential_id: UUID) -> Optional[CredentialInfo]:
        """获取凭证详情"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM provider_credentials WHERE credential_id = $1
            """, credential_id)
            
            if row:
                return self._row_to_credential_info(row)
            return None
    
    async def get_default_credential(self, provider_id: UUID) -> Optional[CredentialInfo]:
        """获取供应商的默认凭证"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM provider_credentials 
                WHERE provider_id = $1 AND is_default = TRUE AND is_active = TRUE
            """, provider_id)
            
            if row:
                return self._row_to_credential_info(row)
            
            # 如果没有默认凭证，返回第一个激活的凭证
            row = await conn.fetchrow("""
                SELECT * FROM provider_credentials 
                WHERE provider_id = $1 AND is_active = TRUE
                ORDER BY created_at LIMIT 1
            """, provider_id)
            
            if row:
                return self._row_to_credential_info(row)
            return None
    
    async def create_credential(self, provider_id: UUID, data: CredentialCreate) -> CredentialInfo:
        """创建凭证"""
        encrypted_key = self._encrypt_api_key(data.api_key)
        
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 检查是否是第一个凭证（设为默认）
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM provider_credentials WHERE provider_id = $1
            """, provider_id)
            is_default = count == 0
            
            row = await conn.fetchrow("""
                INSERT INTO provider_credentials (
                    provider_id, credential_name, encrypted_api_key, extra_config, is_default
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            """, 
                provider_id,
                data.credential_name,
                encrypted_key,
                json.dumps(data.extra_config) if data.extra_config else "{}",
                is_default
            )
            
            logger.info("创建凭证", provider_id=str(provider_id), credential_name=data.credential_name)
            return self._row_to_credential_info(row)
    
    async def update_credential(self, credential_id: UUID, data: CredentialUpdate) -> Optional[CredentialInfo]:
        """更新凭证"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            updates = []
            values = []
            idx = 1
            
            for field, value in data.model_dump(exclude_unset=True).items():
                if field == 'api_key' and value:
                    updates.append(f"encrypted_api_key = ${idx}")
                    values.append(self._encrypt_api_key(value))
                elif field == 'extra_config':
                    updates.append(f"{field} = ${idx}")
                    values.append(json.dumps(value) if value else "{}")
                else:
                    updates.append(f"{field} = ${idx}")
                    values.append(value)
                idx += 1
            
            if not updates:
                return await self.get_credential(credential_id)
            
            values.append(credential_id)
            query = f"""
                UPDATE provider_credentials SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                WHERE credential_id = ${idx}
                RETURNING *
            """
            
            row = await conn.fetchrow(query, *values)
            if row:
                logger.info("更新凭证", credential_id=str(credential_id))
                return self._row_to_credential_info(row)
            return None
    
    async def delete_credential(self, credential_id: UUID) -> bool:
        """删除凭证"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM provider_credentials WHERE credential_id = $1
            """, credential_id)
            
            deleted = result == "DELETE 1"
            if deleted:
                logger.info("删除凭证", credential_id=str(credential_id))
            return deleted
    
    async def set_default_credential(self, credential_id: UUID) -> bool:
        """设置默认凭证"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 获取凭证的供应商ID
            row = await conn.fetchrow("""
                SELECT provider_id FROM provider_credentials WHERE credential_id = $1
            """, credential_id)
            
            if not row:
                return False
            
            provider_id = row['provider_id']
            
            async with conn.transaction():
                # 取消其他凭证的默认状态
                await conn.execute("""
                    UPDATE provider_credentials SET is_default = FALSE
                    WHERE provider_id = $1 AND credential_id != $2
                """, provider_id, credential_id)
                
                # 设置当前凭证为默认
                await conn.execute("""
                    UPDATE provider_credentials SET is_default = TRUE
                    WHERE credential_id = $1
                """, credential_id)
            
            logger.info("设置默认凭证", credential_id=str(credential_id))
            return True
    
    async def get_decrypted_api_key(self, credential_id: UUID) -> Optional[str]:
        """获取解密后的API Key（仅内部使用）"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT encrypted_api_key FROM provider_credentials WHERE credential_id = $1
            """, credential_id)
            
            if row:
                return self._decrypt_api_key(row['encrypted_api_key'])
            return None
    
    # ========================================================================
    # 从供应商API获取模型列表
    # ========================================================================
    
    async def fetch_models_from_provider(
        self,
        provider_id: UUID,
        credential_id: Optional[UUID] = None
    ) -> List[Dict[str, Any]]:
        """
        从OpenAI兼容接口获取可用模型列表
        
        Args:
            provider_id: 供应商ID
            credential_id: 凭证ID（可选，不传则使用默认凭证）
            
        Returns:
            模型列表，格式: [{"id": "model-name", "object": "model", ...}, ...]
        """
        # 获取供应商信息
        provider = await self.get_provider(provider_id)
        if not provider:
            raise ValueError(f"供应商不存在: {provider_id}")
        
        # 只支持OpenAI兼容接口
        if provider.provider_type != ProviderType.OPENAI_COMPATIBLE:
            raise ValueError(f"该供应商类型不支持动态获取模型列表: {provider.provider_type}")
        
        if not provider.base_url:
            raise ValueError("供应商未配置base_url")
        
        # 获取凭证
        if not credential_id:
            credential = await self.get_default_credential(provider_id)
            if not credential:
                raise ValueError("供应商未配置凭证")
        else:
            credential = await self.get_credential(credential_id)
            if not credential:
                raise ValueError(f"凭证不存在: {credential_id}")
        
        api_key = await self.get_decrypted_api_key(credential.credential_id)
        if not api_key:
            raise ValueError("无法获取API Key")
        
        # 调用 /v1/models 接口
        base_url = provider.base_url.rstrip('/')
        models_url = f"{base_url}/models"
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    models_url,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                data = response.json()
                
                # 解析响应
                if isinstance(data, dict) and "data" in data:
                    models = data["data"]
                elif isinstance(data, list):
                    models = data
                else:
                    logger.warning("无法解析模型列表响应", response=data)
                    return []
                
                return models
                
        except httpx.HTTPStatusError as e:
            logger.error("获取模型列表失败", status_code=e.response.status_code, error=str(e))
            raise ValueError(f"获取模型列表失败: HTTP {e.response.status_code}")
        except httpx.RequestError as e:
            logger.error("请求模型列表失败", error=str(e))
            raise ValueError(f"请求模型列表失败: {str(e)}")
        except Exception as e:
            logger.error("获取模型列表异常", error=str(e))
            raise ValueError(f"获取模型列表异常: {str(e)}")
    
    async def sync_models_from_provider(
        self,
        provider_id: UUID,
        credential_id: Optional[UUID] = None,
        model_type_filter: Optional[ModelType] = None
    ) -> List[ModelInfo]:
        """
        从供应商API同步模型列表到数据库
        
        Args:
            provider_id: 供应商ID
            credential_id: 凭证ID（可选）
            model_type_filter: 只同步指定类型的模型（可选）
            
        Returns:
            同步的模型列表
        """
        # 获取模型列表
        models_data = await self.fetch_models_from_provider(provider_id, credential_id)
        
        if not models_data:
            logger.warning("未获取到模型列表", provider_id=str(provider_id))
            return []
        
        # 获取供应商信息
        provider = await self.get_provider(provider_id)
        if not provider:
            raise ValueError(f"供应商不存在: {provider_id}")
        
        # 模型名称到类型的映射（根据常见命名规则推断）
        def infer_model_type(model_id: str) -> Optional[ModelType]:
            model_id_lower = model_id.lower()
            if any(keyword in model_id_lower for keyword in ['embedding', 'embed']):
                return ModelType.EMBEDDING
            elif any(keyword in model_id_lower for keyword in ['rerank', 'reranker']):
                return ModelType.RERANK
            elif any(keyword in model_id_lower for keyword in ['chat', 'completion', 'instruct', 'gpt', 'qwen', 'deepseek', 'llama']):
                return ModelType.LLM
            return None
        
        # 同步模型到数据库
        synced_models = []
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                for model_data in models_data:
                    model_id_str = model_data.get("id", "")
                    if not model_id_str:
                        continue
                    
                    # 推断模型类型
                    inferred_type = infer_model_type(model_id_str)
                    if model_type_filter and inferred_type != model_type_filter:
                        continue
                    if not inferred_type:
                        # 无法推断类型，默认使用LLM
                        inferred_type = ModelType.LLM
                    
                    # 检查模型是否已存在
                    existing = await conn.fetchrow("""
                        SELECT model_id FROM provider_models 
                        WHERE provider_id = $1 AND model_name = $2
                    """, provider_id, model_id_str)
                    
                    if existing:
                        # 更新现有模型
                        await conn.execute("""
                            UPDATE provider_models 
                            SET is_enabled = TRUE, updated_at = CURRENT_TIMESTAMP
                            WHERE model_id = $1
                        """, existing['model_id'])
                        model_info = await self.get_model(existing['model_id'])
                        if model_info:
                            synced_models.append(model_info)
                    else:
                        # 创建新模型
                        display_name = model_id_str.replace('/', ' ').replace('-', ' ').title()
                        
                        # 根据模型名称推断特性
                        supports_function_calling = any(keyword in model_id_str.lower() 
                                                         for keyword in ['chat', 'instruct', 'gpt-4', 'qwen', 'deepseek'])
                        supports_json_mode = supports_function_calling
                        supports_vision = any(keyword in model_id_str.lower() 
                                             for keyword in ['vision', 'gpt-4-vision', 'gpt-4o'])
                        
                        # 根据模型名称推断上下文窗口
                        context_window = 8192  # 默认值
                        if 'gpt-4' in model_id_str.lower() or 'qwen' in model_id_str.lower():
                            context_window = 131072
                        elif 'deepseek' in model_id_str.lower():
                            context_window = 128000
                        
                        row = await conn.fetchrow("""
                            INSERT INTO provider_models (
                                provider_id, model_name, display_name, model_type,
                                supports_function_calling, supports_json_mode, supports_vision,
                                context_window, is_custom
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE)
                            RETURNING *
                        """, 
                            provider_id,
                            model_id_str,
                            display_name,
                            inferred_type.value,
                            supports_function_calling,
                            supports_json_mode,
                            supports_vision,
                            context_window
                        )
                        
                        model_info = self._row_to_model_info(row)
                        synced_models.append(model_info)
        
        logger.info("同步模型列表完成", 
                   provider_id=str(provider_id), 
                   synced_count=len(synced_models))
        
        return synced_models
    
    # ========================================================================
    # 模型管理
    # ========================================================================
    
    async def list_models(
        self, 
        provider_id: Optional[UUID] = None, 
        model_type: Optional[ModelType] = None,
        include_disabled: bool = False
    ) -> List[ModelInfo]:
        """获取模型列表"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            query = """
                SELECT m.*, p.provider_name, p.display_name as provider_display_name
                FROM provider_models m
                JOIN model_providers p ON m.provider_id = p.provider_id
                WHERE 1=1
            """
            params = []
            idx = 1
            
            if provider_id:
                query += f" AND m.provider_id = ${idx}"
                params.append(provider_id)
                idx += 1
            
            if model_type:
                query += f" AND m.model_type = ${idx}"
                params.append(model_type.value)
                idx += 1
            
            if not include_disabled:
                query += " AND m.is_enabled = TRUE AND p.is_enabled = TRUE"
            
            query += " ORDER BY p.display_name, m.model_name"
            
            rows = await conn.fetch(query, *params)
            return [self._row_to_model_info(row) for row in rows]
    
    async def get_model(self, model_id: UUID) -> Optional[ModelInfo]:
        """获取模型详情"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT m.*, p.provider_name, p.display_name as provider_display_name
                FROM provider_models m
                JOIN model_providers p ON m.provider_id = p.provider_id
                WHERE m.model_id = $1
            """, model_id)
            
            if row:
                return self._row_to_model_info(row)
            return None
    
    async def create_model(self, provider_id: UUID, data: ModelCreate) -> ModelInfo:
        """创建模型"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO provider_models (
                    provider_id, model_name, display_name, model_type,
                    supports_function_calling, supports_json_mode, supports_streaming, supports_vision,
                    context_window, max_output_tokens,
                    default_temperature, default_top_p, default_max_tokens,
                    is_custom
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, TRUE)
                RETURNING *
            """, 
                provider_id,
                data.model_name,
                data.display_name,
                data.model_type.value,
                data.supports_function_calling,
                data.supports_json_mode,
                data.supports_streaming,
                data.supports_vision,
                data.context_window,
                data.max_output_tokens,
                data.default_temperature,
                data.default_top_p,
                data.default_max_tokens
            )
            
            logger.info("创建模型", provider_id=str(provider_id), model_name=data.model_name)
            return self._row_to_model_info(row)
    
    async def update_model(self, model_id: UUID, data: ModelUpdate) -> Optional[ModelInfo]:
        """更新模型"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            updates = []
            values = []
            idx = 1
            
            for field, value in data.model_dump(exclude_unset=True).items():
                updates.append(f"{field} = ${idx}")
                values.append(value)
                idx += 1
            
            if not updates:
                return await self.get_model(model_id)
            
            values.append(model_id)
            query = f"""
                UPDATE provider_models SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                WHERE model_id = ${idx}
                RETURNING *
            """
            
            row = await conn.fetchrow(query, *values)
            if row:
                logger.info("更新模型", model_id=str(model_id))
                return self._row_to_model_info(row)
            return None
    
    async def delete_model(self, model_id: UUID) -> bool:
        """删除模型"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM provider_models WHERE model_id = $1
            """, model_id)
            
            deleted = result == "DELETE 1"
            if deleted:
                logger.info("删除模型", model_id=str(model_id))
            return deleted
    
    # ========================================================================
    # 可用模型（用于选择器）
    # ========================================================================
    
    async def get_available_models(
        self, 
        model_type: Optional[ModelType] = None,
        scenario: Optional[LLMScenario] = None
    ) -> List[AvailableModel]:
        """获取可用模型列表（用于前端选择器）"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            query = """
                SELECT 
                    m.model_id, m.model_name, m.display_name, m.model_type,
                    m.supports_function_calling, m.supports_json_mode, m.supports_vision,
                    p.provider_id, p.provider_name, p.display_name as provider_display_name,
                    EXISTS(
                        SELECT 1 FROM provider_credentials c 
                        WHERE c.provider_id = p.provider_id AND c.is_active = TRUE
                    ) as has_valid_credential
                FROM provider_models m
                JOIN model_providers p ON m.provider_id = p.provider_id
                WHERE m.is_enabled = TRUE AND p.is_enabled = TRUE
            """
            params = []
            idx = 1
            
            if model_type:
                query += f" AND m.model_type = ${idx}"
                params.append(model_type.value)
                idx += 1
            
            query += " ORDER BY p.display_name, m.model_name"
            
            rows = await conn.fetch(query, *params)
            
            result = []
            for row in rows:
                features = []
                if row['supports_function_calling']:
                    features.append("function_calling")
                if row['supports_json_mode']:
                    features.append("json_mode")
                if row['supports_vision']:
                    features.append("vision")
                
                result.append(AvailableModel(
                    model_id=row['model_id'],
                    model_name=row['model_name'],
                    display_name=row['display_name'],
                    model_type=ModelType(row['model_type']),
                    provider_id=row['provider_id'],
                    provider_name=row['provider_name'],
                    provider_display_name=row['provider_display_name'],
                    features=features,
                    has_valid_credential=row['has_valid_credential']
                ))
            
            return result
    
    # ========================================================================
    # 场景配置管理
    # ========================================================================
    
    async def get_scenario_config(self, scenario: LLMScenario) -> Optional[ScenarioConfigInfo]:
        """获取场景配置"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    sc.*,
                    m.model_name, m.display_name as model_display_name,
                    p.provider_name, p.display_name as provider_display_name
                FROM scenario_model_configs sc
                LEFT JOIN provider_models m ON sc.model_id = m.model_id
                LEFT JOIN model_providers p ON m.provider_id = p.provider_id
                WHERE sc.scenario = $1 AND sc.is_enabled = TRUE
                ORDER BY sc.priority
                LIMIT 1
            """, scenario.value)
            
            if row:
                return self._row_to_scenario_config_info(row)
            return None
    
    async def list_scenario_configs(self) -> List[ScenarioConfigInfo]:
        """
        获取所有场景配置
        
        返回所有预定义场景的配置，包括未在数据库中配置的场景（显示为"未配置"）
        """
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 获取数据库中的配置
            rows = await conn.fetch("""
                SELECT 
                    sc.*,
                    m.model_name, m.display_name as model_display_name,
                    p.provider_name, p.display_name as provider_display_name
                FROM scenario_model_configs sc
                LEFT JOIN provider_models m ON sc.model_id = m.model_id
                LEFT JOIN model_providers p ON m.provider_id = p.provider_id
                ORDER BY sc.scenario, sc.priority
            """)
            
            # 已配置的场景
            configured = {row['scenario']: self._row_to_scenario_config_info(row) for row in rows}
            
            # 返回所有场景，未配置的用默认值填充
            result = []
            for scenario in LLMScenario:
                if scenario.value in configured:
                    result.append(configured[scenario.value])
                else:
                    # 未配置的场景，config_id 和时间戳为 None
                    result.append(ScenarioConfigInfo(
                        scenario=scenario,
                        config_id=None,
                        model_id=None,
                        credential_id=None,
                        temperature=None,
                        top_p=None,
                        max_tokens=None,
                        timeout_seconds=60,
                        max_retries=2,
                        extra_params={},
                        priority=0,
                        is_enabled=True,
                        created_at=None,
                        updated_at=None,
                        model_name=None,
                        model_display_name=None,
                        provider_name=None,
                        provider_display_name=None,
                    ))
            return result
    
    async def upsert_scenario_config(
        self, 
        scenario: LLMScenario, 
        data: ScenarioConfigUpdate
    ) -> ScenarioConfigInfo:
        """更新或创建场景配置"""
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 检查是否存在
            existing = await conn.fetchrow("""
                SELECT config_id FROM scenario_model_configs 
                WHERE scenario = $1 AND priority = 0
            """, scenario.value)
            
            if existing:
                # 更新
                updates = []
                values = []
                idx = 1
                
                for field, value in data.model_dump(exclude_unset=True).items():
                    if field == 'extra_params':
                        updates.append(f"{field} = ${idx}")
                        values.append(json.dumps(value) if value else "{}")
                    else:
                        updates.append(f"{field} = ${idx}")
                        values.append(value if not isinstance(value, UUID) else value)
                    idx += 1
                
                if updates:
                    values.append(existing['config_id'])
                    query = f"""
                        UPDATE scenario_model_configs 
                        SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                        WHERE config_id = ${idx}
                    """
                    await conn.execute(query, *values)
            else:
                # 创建
                await conn.execute("""
                    INSERT INTO scenario_model_configs (
                        scenario, model_id, credential_id, 
                        temperature, top_p, max_tokens, timeout_seconds, max_retries,
                        extra_params, priority
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0)
                """,
                    scenario.value,
                    data.model_id,
                    data.credential_id,
                    data.temperature,
                    data.top_p,
                    data.max_tokens,
                    data.timeout_seconds or 60,
                    data.max_retries or 2,
                    json.dumps(data.extra_params) if data.extra_params else "{}"
                )
            
            logger.info("更新场景配置", scenario=scenario.value)
            return await self.get_scenario_config(scenario)
    
    # ========================================================================
    # 获取LLM配置（供LLMClient使用）
    # ========================================================================
    
    async def get_llm_config_for_scenario(self, scenario: LLMScenario) -> Optional[Dict[str, Any]]:
        """获取场景的LLM配置（供LLMClient使用）"""
        config = await self.get_scenario_config(scenario)
        
        if not config or not config.model_id:
            # 尝试使用default场景的配置作为fallback
            if scenario != LLMScenario.DEFAULT:
                default_config = await self.get_scenario_config(LLMScenario.DEFAULT)
                if default_config and default_config.model_id:
                    config = default_config
                else:
                    return None
            else:
                return None
        
        # 使用场景配置
        model = await self.get_model(config.model_id)
        if not model:
            return None
        
        credential_id = config.credential_id
        if not credential_id:
            provider = await self.get_provider_by_name(model.provider_name)
            if provider:
                default_cred = await self.get_default_credential(provider.provider_id)
                credential_id = default_cred.credential_id if default_cred else None
        
        if not credential_id:
            return None
        
        api_key = await self.get_decrypted_api_key(credential_id)
        if not api_key:
            return None
        
        provider = await self.get_provider_by_name(model.provider_name)
        
        return {
            "base_url": provider.base_url if provider else None,
            "api_key": api_key,
            "model": model.model_name,
            "temperature": config.temperature if config.temperature is not None else model.default_temperature,
            "max_tokens": config.max_tokens if config.max_tokens is not None else model.default_max_tokens,
            "timeout": config.timeout_seconds,
            "max_retries": config.max_retries,
            "extra_params": config.extra_params or {},
        }
    
    # ========================================================================
    # 辅助方法
    # ========================================================================
    
    def _row_to_provider_info(self, row) -> ProviderInfo:
        """将数据库行转换为ProviderInfo"""
        return ProviderInfo(
            provider_id=row['provider_id'],
            provider_name=row['provider_name'],
            display_name=row['display_name'],
            provider_type=ProviderType(row['provider_type']),
            base_url=row.get('base_url'),
            icon=row.get('icon'),
            description=row.get('description'),
            is_enabled=row.get('is_enabled', True),
            is_valid=row.get('is_valid', False),
            last_validated_at=row.get('last_validated_at'),
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            credential_count=row.get('credential_count', 0),
            model_count=row.get('model_count', 0),
        )
    
    def _row_to_credential_info(self, row) -> CredentialInfo:
        """将数据库行转换为CredentialInfo"""
        encrypted_key = row.get('encrypted_api_key', '')
        decrypted_key = self._decrypt_api_key(encrypted_key) if encrypted_key else ''
        
        return CredentialInfo(
            credential_id=row['credential_id'],
            provider_id=row['provider_id'],
            credential_name=row['credential_name'],
            extra_config=json.loads(row.get('extra_config') or '{}'),
            is_active=row.get('is_active', True),
            is_default=row.get('is_default', False),
            total_requests=row.get('total_requests', 0),
            total_tokens=row.get('total_tokens', 0),
            last_used_at=row.get('last_used_at'),
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            api_key_masked=self._mask_api_key(decrypted_key),
        )
    
    def _row_to_model_info(self, row) -> ModelInfo:
        """将数据库行转换为ModelInfo"""
        return ModelInfo(
            model_id=row['model_id'],
            provider_id=row['provider_id'],
            model_name=row['model_name'],
            display_name=row.get('display_name'),
            model_type=ModelType(row['model_type']),
            supports_function_calling=row.get('supports_function_calling', False),
            supports_json_mode=row.get('supports_json_mode', False),
            supports_streaming=row.get('supports_streaming', True),
            supports_vision=row.get('supports_vision', False),
            context_window=row.get('context_window'),
            max_output_tokens=row.get('max_output_tokens'),
            default_temperature=float(row.get('default_temperature') or 0),
            default_top_p=float(row.get('default_top_p') or 1),
            default_max_tokens=row.get('default_max_tokens') or 2048,
            is_enabled=row.get('is_enabled', True),
            is_custom=row.get('is_custom', False),
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            provider_name=row.get('provider_name'),
            provider_display_name=row.get('provider_display_name'),
        )
    
    def _row_to_scenario_config_info(self, row) -> ScenarioConfigInfo:
        """将数据库行转换为ScenarioConfigInfo"""
        return ScenarioConfigInfo(
            config_id=row['config_id'],
            scenario=LLMScenario(row['scenario']),
            model_id=row.get('model_id'),
            credential_id=row.get('credential_id'),
            temperature=float(row['temperature']) if row.get('temperature') is not None else None,
            top_p=float(row['top_p']) if row.get('top_p') is not None else None,
            max_tokens=row.get('max_tokens'),
            timeout_seconds=row.get('timeout_seconds') or 60,
            max_retries=row.get('max_retries') or 2,
            extra_params=json.loads(row.get('extra_params') or '{}'),
            priority=row.get('priority', 0),
            is_enabled=row.get('is_enabled', True),
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            model_name=row.get('model_name'),
            model_display_name=row.get('model_display_name'),
            provider_name=row.get('provider_name'),
            provider_display_name=row.get('provider_display_name'),
        )


# 服务单例
_model_provider_service: Optional[ModelProviderService] = None


def get_model_provider_service() -> ModelProviderService:
    """获取模型供应商服务实例"""
    global _model_provider_service
    if _model_provider_service is None:
        _model_provider_service = ModelProviderService()
    return _model_provider_service

