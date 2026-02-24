"""提示词模板管理API

提供提示词模板的CRUD操作接口，支持：
- 查看所有场景的提示词列表
- 获取/编辑提示词内容
- 从文件同步到数据库
- 导出到文件
- 版本历史和回滚
"""

from typing import List, Optional, Dict, Any
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Body
from pydantic import BaseModel, Field
import structlog

from server.middleware.auth import require_admin
from server.models.admin import User as AdminUser
from server.services.prompt_service import (
    get_prompt_service, 
    PromptService,
    PromptScenario,
    PromptType,
    SCENARIO_LABELS,
    TYPE_LABELS,
)

logger = structlog.get_logger()

router = APIRouter(prefix="/prompts", tags=["提示词管理"])


# ============================================================================
# 数据模型
# ============================================================================

class PromptInfo(BaseModel):
    """提示词信息"""
    scenario: str
    prompt_type: str
    type_label: str
    file_path: Optional[str] = None
    has_file: bool = False
    has_db_version: bool = False
    is_active: bool = False
    version: int = 0
    template_id: Optional[str] = None


class ScenarioInfo(BaseModel):
    """场景信息"""
    scenario: str
    label: str
    description: str
    prompts: List[PromptInfo] = []


class PromptDetail(BaseModel):
    """提示词详情"""
    template_id: Optional[str] = None
    scenario: str
    prompt_type: str
    display_name: str
    description: Optional[str] = None
    content: str
    version: int = 1
    is_active: bool = False
    file_path: Optional[str] = None
    has_file: bool = False
    file_content: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PromptSaveRequest(BaseModel):
    """保存提示词请求"""
    content: str = Field(..., description="提示词内容")
    display_name: Optional[str] = Field(None, description="显示名称")
    description: Optional[str] = Field(None, description="描述")
    is_active: bool = Field(True, description="是否激活")
    change_reason: Optional[str] = Field(None, description="变更原因")
    sync_to_file: bool = Field(True, description="是否同步到文件")


class PromptToggleRequest(BaseModel):
    """切换激活状态请求"""
    is_active: bool = Field(..., description="是否激活")


class HistoryItem(BaseModel):
    """历史记录项"""
    history_id: str
    template_id: str
    content: str
    version: int
    change_reason: Optional[str] = None
    changed_by: Optional[str] = None
    changed_by_name: Optional[str] = None
    changed_at: Optional[str] = None


class RollbackRequest(BaseModel):
    """回滚请求"""
    version: int = Field(..., description="目标版本号")


# ============================================================================
# API 路由
# ============================================================================

@router.get("/scenarios", response_model=List[ScenarioInfo])
async def list_scenarios(
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    获取所有提示词场景列表
    
    返回所有场景及其包含的提示词类型，包括：
    - 场景标识和描述
    - 每个场景的提示词列表
    - 是否有数据库版本、是否已激活等状态
    """
    scenarios = await service.list_scenarios()
    return [
        ScenarioInfo(
            scenario=s.scenario,
            label=s.label,
            description=s.description,
            prompts=[PromptInfo(**p) for p in s.prompts]
        )
        for s in scenarios
    ]


@router.get("/types")
async def get_prompt_types(
    current_user: AdminUser = Depends(require_admin)
):
    """获取所有提示词类型枚举"""
    return {
        "scenarios": [
            {"value": s.value, "label": SCENARIO_LABELS.get(s, s.value)}
            for s in PromptScenario
        ],
        "types": [
            {"value": t.value, "label": TYPE_LABELS.get(t, t.value)}
            for t in PromptType
        ]
    }


@router.get("/{scenario}/{prompt_type}", response_model=PromptDetail)
async def get_prompt(
    scenario: str,
    prompt_type: str,
    include_file: bool = True,
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    获取指定场景和类型的提示词详情
    
    Args:
        scenario: 场景标识（table_selector, nl2ir, direct_sql, narrative, cot_planner, vector_table_selector）
        prompt_type: 类型（system, user_template, function_schema）
        include_file: 是否包含文件内容（用于对比）
    """
    template = await service.get_prompt(
        scenario=scenario,
        prompt_type=prompt_type,
        include_file_content=include_file
    )
    
    if not template:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"提示词不存在: {scenario}/{prompt_type}"
        )
    
    return PromptDetail(
        template_id=str(template.template_id) if template.template_id else None,
        scenario=template.scenario,
        prompt_type=template.prompt_type,
        display_name=template.display_name,
        description=template.description,
        content=template.content,
        version=template.version,
        is_active=template.is_active,
        file_path=template.file_path,
        has_file=template.has_file,
        file_content=template.file_content,
        created_at=template.created_at,
        updated_at=template.updated_at,
    )


@router.put("/{scenario}/{prompt_type}", response_model=PromptDetail)
async def save_prompt(
    scenario: str,
    prompt_type: str,
    request: PromptSaveRequest,
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    保存提示词到数据库
    
    Args:
        scenario: 场景标识
        prompt_type: 类型
        request: 保存请求
    """
    template = await service.save_prompt(
        scenario=scenario,
        prompt_type=prompt_type,
        content=request.content,
        display_name=request.display_name,
        description=request.description,
        is_active=request.is_active,
        user_id=current_user.user_id,
        change_reason=request.change_reason,
        sync_to_file=request.sync_to_file
    )
    
    return PromptDetail(
        template_id=str(template.template_id) if template.template_id else None,
        scenario=template.scenario,
        prompt_type=template.prompt_type,
        display_name=template.display_name,
        description=template.description,
        content=template.content,
        version=template.version,
        is_active=template.is_active,
        file_path=template.file_path,
        has_file=template.has_file,
        created_at=template.created_at,
        updated_at=template.updated_at,
    )


@router.patch("/{scenario}/{prompt_type}/toggle", response_model=PromptDetail)
async def toggle_prompt_active(
    scenario: str,
    prompt_type: str,
    request: PromptToggleRequest,
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    切换提示词激活状态
    
    激活后使用数据库版本，禁用后回退到文件版本
    """
    template = await service.toggle_active(
        scenario=scenario,
        prompt_type=prompt_type,
        is_active=request.is_active,
        user_id=current_user.user_id
    )
    
    if not template:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"提示词不存在: {scenario}/{prompt_type}"
        )
    
    return PromptDetail(
        template_id=str(template.template_id) if template.template_id else None,
        scenario=template.scenario,
        prompt_type=template.prompt_type,
        display_name=template.display_name,
        description=template.description,
        content=template.content,
        version=template.version,
        is_active=template.is_active,
        file_path=template.file_path,
        has_file=template.has_file,
        created_at=template.created_at,
        updated_at=template.updated_at,
    )


@router.post("/{scenario}/{prompt_type}/sync-from-file", response_model=PromptDetail)
async def sync_from_file(
    scenario: str,
    prompt_type: str,
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    从文件同步到数据库
    
    将文件中的提示词内容导入到数据库（默认不激活）
    """
    template = await service.sync_from_file(
        scenario=scenario,
        prompt_type=prompt_type,
        user_id=current_user.user_id
    )
    
    if not template:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"无法从文件同步: {scenario}/{prompt_type}"
        )
    
    return PromptDetail(
        template_id=str(template.template_id) if template.template_id else None,
        scenario=template.scenario,
        prompt_type=template.prompt_type,
        display_name=template.display_name,
        description=template.description,
        content=template.content,
        version=template.version,
        is_active=template.is_active,
        file_path=template.file_path,
        has_file=template.has_file,
        created_at=template.created_at,
        updated_at=template.updated_at,
    )


@router.post("/{scenario}/{prompt_type}/export-to-file")
async def export_to_file(
    scenario: str,
    prompt_type: str,
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    导出数据库版本到文件
    
    将数据库中的提示词内容写入到对应的文件
    """
    success = await service.export_to_file(
        scenario=scenario,
        prompt_type=prompt_type
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"导出失败: {scenario}/{prompt_type}"
        )
    
    return {"success": True, "message": "已导出到文件"}


@router.get("/{scenario}/{prompt_type}/history", response_model=List[HistoryItem])
async def get_history(
    scenario: str,
    prompt_type: str,
    limit: int = 10,
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    获取提示词版本历史
    
    Args:
        scenario: 场景标识
        prompt_type: 类型
        limit: 返回条数限制
    """
    history = await service.get_history(
        scenario=scenario,
        prompt_type=prompt_type,
        limit=limit
    )
    
    return [
        HistoryItem(
            history_id=str(h["history_id"]),
            template_id=str(h["template_id"]),
            content=h["content"],
            version=h["version"],
            change_reason=h.get("change_reason"),
            changed_by=str(h["changed_by"]) if h.get("changed_by") else None,
            changed_by_name=h.get("changed_by_name"),
            changed_at=str(h["changed_at"]) if h.get("changed_at") else None,
        )
        for h in history
    ]


@router.post("/{scenario}/{prompt_type}/rollback", response_model=PromptDetail)
async def rollback_to_version(
    scenario: str,
    prompt_type: str,
    request: RollbackRequest,
    current_user: AdminUser = Depends(require_admin),
    service: PromptService = Depends(get_prompt_service)
):
    """
    回滚到指定版本
    
    Args:
        scenario: 场景标识
        prompt_type: 类型
        request: 包含目标版本号
    """
    template = await service.rollback_to_version(
        scenario=scenario,
        prompt_type=prompt_type,
        version=request.version,
        user_id=current_user.user_id
    )
    
    if not template:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"版本不存在: {scenario}/{prompt_type} v{request.version}"
        )
    
    return PromptDetail(
        template_id=str(template.template_id) if template.template_id else None,
        scenario=template.scenario,
        prompt_type=template.prompt_type,
        display_name=template.display_name,
        description=template.description,
        content=template.content,
        version=template.version,
        is_active=template.is_active,
        file_path=template.file_path,
        has_file=template.has_file,
        created_at=template.created_at,
        updated_at=template.updated_at,
    )

