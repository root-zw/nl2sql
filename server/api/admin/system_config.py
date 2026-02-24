"""系统配置API"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import Dict, Any

from server.api.admin.auth import require_role
from server.models.database import UserRole
from server.config import settings, reload_retrieval_config, get_retrieval_config

router = APIRouter(prefix="/system-config", tags=["系统配置"])
require_admin = require_role(UserRole.ADMIN)


def mask_secret(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:4]}****{value[-4:]}"


def format_scenario_value(value, default_text="(使用默认)"):
    """格式化场景配置值，None时显示默认提示"""
    if value is None:
        return default_text
    return value


@router.get("/model")
async def get_model_config(
    current_user: dict = Depends(require_admin)
) -> dict:
    """
    返回所有模型相关配置（只读）
    
    包含：
    - 默认 LLM 配置（环境变量兜底配置）
    - 所有场景特定 LLM 配置
    - Embedding 配置
    - Reranker 配置
    
    说明：
    - 这些是环境变量(.env)中的静态配置
    - 模型供应商管理中的数据库配置优先级更高
    - 当数据库中未配置某场景时，会回退到这些环境变量配置
    """

    return {
        "llm": {
            "provider": settings.llm_provider,
            "base_url": settings.nl2sql_base_url,
            "model": settings.llm_model,
            "api_key": mask_secret(settings.nl2sql_api_key),
            "timeout": settings.llm_timeout,
            "max_retries": settings.llm_max_retries,
            "use_tools": settings.llm_use_tools,
            "use_json_mode": settings.llm_use_json_mode,
            "temperature": settings.llm_temperature,
            "top_p": settings.llm_top_p,
            "max_tokens": settings.llm_max_tokens,
            "enable_thinking": settings.llm_enable_thinking,
        },
        "embedding": {
            "base_url": settings.embedding_base_url,
            "model": settings.embedding_model,
            "api_key": mask_secret(settings.embedding_api_key),
            "timeout": settings.embedding_timeout,
            "dimension": settings.embedding_dim,
        },
        "reranker": {
            "endpoint": settings.reranker_endpoint or "(未配置)",
            "model": settings.reranker_model,
            "api_key": mask_secret(settings.reranker_api_key) if settings.reranker_api_key else "(未配置)",
            "timeout": settings.reranker_timeout,
            "weight": settings.reranker_weight,
            "max_concurrent": settings.reranker_max_concurrent,
        },
        # LLM 场景特定配置
        "llm_scenarios": {
            "table_selection": {
                "label": "LLM 表选择",
                "description": "用于从候选表中智能选择最相关的表",
                "base_url": format_scenario_value(settings.llm_table_selection_base_url),
                "model": format_scenario_value(settings.llm_table_selection_model),
                "api_key": mask_secret(settings.llm_table_selection_api_key) if settings.llm_table_selection_api_key else "(使用默认)",
                "temperature": format_scenario_value(settings.llm_table_selection_temperature),
                "max_tokens": settings.llm_table_selection_max_tokens,
                "timeout": format_scenario_value(settings.llm_table_selection_timeout),
                "enabled": settings.llm_table_selection_enabled,
            },
            "nl2ir": {
                "label": "NL2IR 解析",
                "description": "将自然语言转换为中间表示(IR)",
                "base_url": format_scenario_value(settings.llm_nl2ir_base_url),
                "model": format_scenario_value(settings.llm_nl2ir_model),
                "api_key": mask_secret(settings.llm_nl2ir_api_key) if settings.llm_nl2ir_api_key else "(使用默认)",
                "temperature": format_scenario_value(settings.llm_nl2ir_temperature),
                "max_tokens": format_scenario_value(settings.llm_nl2ir_max_tokens),
                "timeout": format_scenario_value(settings.llm_nl2ir_timeout),
            },
            "narrative": {
                "label": "叙述生成",
                "description": "生成查询结果的自然语言描述",
                "base_url": format_scenario_value(settings.llm_narrative_base_url),
                "model": format_scenario_value(settings.llm_narrative_model),
                "api_key": mask_secret(settings.llm_narrative_api_key) if settings.llm_narrative_api_key else "(使用默认)",
                "temperature": settings.narrative_temperature,
                "max_tokens": format_scenario_value(settings.llm_narrative_max_tokens),
                "timeout": format_scenario_value(settings.llm_narrative_timeout),
            },
            "direct_sql": {
                "label": "直接SQL生成",
                "description": "直接生成SQL（用于IR无法表达的复杂查询）",
                "base_url": format_scenario_value(settings.llm_direct_sql_base_url),
                "model": format_scenario_value(settings.llm_direct_sql_model),
                "api_key": mask_secret(settings.llm_direct_sql_api_key) if settings.llm_direct_sql_api_key else "(使用默认)",
                "temperature": settings.llm_direct_sql_temperature,
                "max_tokens": settings.llm_direct_sql_max_tokens,
                "timeout": settings.llm_direct_sql_timeout,
                "enabled": settings.direct_sql_enabled,
            },
            "vector_selector": {
                "label": "向量表选择",
                "description": "基于向量检索的表选择（LLM表选择禁用时使用）",
                "base_url": format_scenario_value(settings.llm_vector_selector_base_url),
                "model": format_scenario_value(settings.llm_vector_selector_model),
                "api_key": mask_secret(settings.llm_vector_selector_api_key) if settings.llm_vector_selector_api_key else "(使用默认)",
                "temperature": settings.llm_vector_selector_temperature,
                "max_tokens": settings.llm_vector_selector_max_tokens,
                "timeout": settings.llm_vector_selector_timeout,
            },
        },
    }


@router.post("/retrieval/reload")
async def reload_retrieval_config_api(
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """
    重新加载 retrieval_config.yaml（热加载）
    
    注意：
    - 此操作会清除配置缓存并从文件重新加载
    - 仅对后续请求生效，已处理的请求不受影响
    - 需要管理员权限
    """
    try:
        config = reload_retrieval_config()
        version = config.get("version", "unknown")
        return {
            "success": True,
            "message": "检索配置已重新加载",
            "version": version,
            "config_path": "config/retrieval_config.yaml"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"重新加载配置失败: {str(e)}"
        )


@router.get("/retrieval/info")
async def get_retrieval_config_info(
    current_user: dict = Depends(require_admin)
) -> Dict[str, Any]:
    """
    获取当前检索配置信息（只读）
    
    返回配置版本和关键参数摘要，不返回完整配置内容
    """
    try:
        config = get_retrieval_config()
        version = config.get("version", "unknown")
        ranker_version = config.get("table_scoring", {}).get("ranker_version", "unknown")
        v4_enabled = config.get("table_scoring", {}).get("v4_ranker", {}).get("enabled", False)
        
        return {
            "version": version,
            "ranker_version": ranker_version,
            "v4_ranker_enabled": v4_enabled,
            "config_path": "config/retrieval_config.yaml"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取配置信息失败: {str(e)}"
        )

