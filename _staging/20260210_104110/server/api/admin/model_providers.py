"""模型供应商管理API

提供模型供应商、凭证、模型、场景配置的管理接口。
"""

from typing import List, Optional, Dict, Any
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Query
import structlog

from server.middleware.auth import require_admin
from server.models.admin import User as AdminUser
from server.services.model_provider_service import get_model_provider_service, ModelProviderService
from server.models.model_provider import (
    ProviderType, ModelType, LLMScenario,
    ProviderCreate, ProviderUpdate, ProviderInfo, ProviderDetail,
    CredentialCreate, CredentialUpdate, CredentialInfo,
    ModelCreate, ModelUpdate, ModelInfo,
    ScenarioConfigUpdate, ScenarioConfigInfo,
    AvailableModel, AvailableModelsResponse,
    PresetProvider,
)

logger = structlog.get_logger()

router = APIRouter(prefix="/model-providers", tags=["模型供应商管理"])


# ============================================================================
# 预置供应商
# ============================================================================

@router.get("/presets", response_model=List[PresetProvider])
async def list_preset_providers(
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取预置供应商列表"""
    return service.get_preset_providers()


@router.post("/presets/{preset_name}/add")
async def add_provider_from_preset(
    preset_name: str,
    api_key: str,
    credential_name: str = "默认凭证",
    base_url: Optional[str] = None,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """从预置模板添加供应商"""
    try:
        provider, credential = await service.add_provider_from_preset(
            preset_name=preset_name,
            api_key=api_key,
            credential_name=credential_name,
            base_url=base_url,
            created_by=current_user.user_id
        )
        return {
            "provider": provider,
            "credential": credential,
            "message": f"成功添加供应商: {provider.display_name}"
        }
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


# ============================================================================
# 可用模型（用于选择器）- 必须放在 /{provider_id} 路由之前
# ============================================================================

@router.get("/available-models", response_model=AvailableModelsResponse)
async def get_available_models(
    model_type: Optional[ModelType] = Query(None, description="模型类型过滤"),
    scenario: Optional[LLMScenario] = Query(None, description="场景推荐"),
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取可用模型列表（用于前端选择器）"""
    models = await service.get_available_models(model_type=model_type, scenario=scenario)
    return AvailableModelsResponse(models=models, total=len(models))


# ============================================================================
# 供应商管理
# ============================================================================

@router.get("", response_model=List[ProviderInfo])
async def list_providers(
    include_disabled: bool = Query(False, description="是否包含已禁用的供应商"),
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取所有供应商列表"""
    return await service.list_providers(include_disabled=include_disabled)


@router.get("/{provider_id}", response_model=ProviderDetail)
async def get_provider(
    provider_id: UUID,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取供应商详情"""
    provider = await service.get_provider(provider_id)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="供应商不存在")
    return provider


@router.post("", response_model=ProviderInfo, status_code=status.HTTP_201_CREATED)
async def create_provider(
    data: ProviderCreate,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """创建供应商"""
    try:
        return await service.create_provider(data, created_by=current_user.user_id)
    except Exception as e:
        logger.error("创建供应商失败", error=str(e))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.put("/{provider_id}", response_model=ProviderInfo)
async def update_provider(
    provider_id: UUID,
    data: ProviderUpdate,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """更新供应商"""
    provider = await service.update_provider(provider_id, data)
    if not provider:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="供应商不存在")
    return provider


@router.delete("/{provider_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_provider(
    provider_id: UUID,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """删除供应商"""
    deleted = await service.delete_provider(provider_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="供应商不存在")


# ============================================================================
# 凭证管理
# ============================================================================

@router.get("/{provider_id}/credentials", response_model=List[CredentialInfo])
async def list_credentials(
    provider_id: UUID,
    include_inactive: bool = Query(False, description="是否包含已禁用的凭证"),
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取供应商的凭证列表"""
    return await service.list_credentials(provider_id, include_inactive=include_inactive)


@router.post("/{provider_id}/credentials", response_model=CredentialInfo, status_code=status.HTTP_201_CREATED)
async def create_credential(
    provider_id: UUID,
    data: CredentialCreate,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """创建凭证"""
    try:
        return await service.create_credential(provider_id, data)
    except Exception as e:
        logger.error("创建凭证失败", error=str(e))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.put("/{provider_id}/credentials/{credential_id}", response_model=CredentialInfo)
async def update_credential(
    provider_id: UUID,
    credential_id: UUID,
    data: CredentialUpdate,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """更新凭证"""
    credential = await service.update_credential(credential_id, data)
    if not credential:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="凭证不存在")
    return credential


@router.delete("/{provider_id}/credentials/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_credential(
    provider_id: UUID,
    credential_id: UUID,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """删除凭证"""
    deleted = await service.delete_credential(credential_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="凭证不存在")


@router.post("/{provider_id}/credentials/{credential_id}/set-default", status_code=status.HTTP_200_OK)
async def set_default_credential(
    provider_id: UUID,
    credential_id: UUID,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """设置默认凭证"""
    success = await service.set_default_credential(credential_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="凭证不存在")
    return {"message": "设置成功"}


# ============================================================================
# 模型管理
# ============================================================================

@router.get("/{provider_id}/models", response_model=List[ModelInfo])
async def list_provider_models(
    provider_id: UUID,
    model_type: Optional[ModelType] = Query(None, description="模型类型过滤"),
    include_disabled: bool = Query(False, description="是否包含已禁用的模型"),
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取供应商的模型列表"""
    return await service.list_models(
        provider_id=provider_id, 
        model_type=model_type, 
        include_disabled=include_disabled
    )


@router.get("/{provider_id}/models/fetch", response_model=List[Dict[str, Any]])
async def fetch_models_from_provider(
    provider_id: UUID,
    credential_id: Optional[UUID] = Query(None, description="凭证ID（可选，不传则使用默认凭证）"),
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """从供应商API获取可用模型列表（不保存到数据库）"""
    try:
        models = await service.fetch_models_from_provider(provider_id, credential_id)
        return models
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error("获取模型列表失败", error=str(e))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{provider_id}/models/sync", response_model=List[ModelInfo])
async def sync_models_from_provider(
    provider_id: UUID,
    credential_id: Optional[UUID] = Query(None, description="凭证ID（可选，不传则使用默认凭证）"),
    model_type: Optional[ModelType] = Query(None, description="只同步指定类型的模型"),
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """从供应商API同步模型列表到数据库"""
    try:
        models = await service.sync_models_from_provider(
            provider_id, 
            credential_id, 
            model_type_filter=model_type
        )
        return models
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error("同步模型列表失败", error=str(e), exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{provider_id}/models", response_model=ModelInfo, status_code=status.HTTP_201_CREATED)
async def create_model(
    provider_id: UUID,
    data: ModelCreate,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """创建模型"""
    try:
        return await service.create_model(provider_id, data)
    except Exception as e:
        logger.error("创建模型失败", error=str(e))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.put("/{provider_id}/models/{model_id}", response_model=ModelInfo)
async def update_model(
    provider_id: UUID,
    model_id: UUID,
    data: ModelUpdate,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """更新模型"""
    model = await service.update_model(model_id, data)
    if not model:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="模型不存在")
    return model


@router.delete("/{provider_id}/models/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_model(
    provider_id: UUID,
    model_id: UUID,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """删除模型"""
    deleted = await service.delete_model(model_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="模型不存在")


# ============================================================================
# 场景配置
# ============================================================================

scenario_router = APIRouter(prefix="/scenario-configs", tags=["模型配置"])


@scenario_router.get("", response_model=List[ScenarioConfigInfo])
async def list_scenario_configs(
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取所有场景配置"""
    return await service.list_scenario_configs()


@scenario_router.get("/{scenario}", response_model=Optional[ScenarioConfigInfo])
async def get_scenario_config(
    scenario: LLMScenario,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """获取指定场景配置"""
    return await service.get_scenario_config(scenario)


@scenario_router.put("/{scenario}", response_model=ScenarioConfigInfo)
async def update_scenario_config(
    scenario: LLMScenario,
    data: ScenarioConfigUpdate,
    current_user: AdminUser = Depends(require_admin),
    service: ModelProviderService = Depends(get_model_provider_service)
):
    """更新场景配置"""
    return await service.upsert_scenario_config(scenario, data)
