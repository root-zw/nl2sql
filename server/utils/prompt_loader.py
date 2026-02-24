"""Prompt 文件加载工具（统一路径解析与回退策略）。

支持：
1. 从数据库加载激活的提示词（优先）
2. 从文件加载提示词（回退）
3. 使用默认值（最终回退）
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from functools import lru_cache

import structlog

logger = structlog.get_logger()

_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 提示词缓存（场景+类型 -> 内容）
_prompt_cache: Dict[Tuple[str, str], str] = {}
_json_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}


def resolve_path(path_value: str | None, default: Path) -> Path:
    """解析配置中的相对或绝对路径（相对路径以项目根目录为基准）。"""
    if path_value:
        candidate = Path(path_value)
        if not candidate.is_absolute():
            candidate = _PROJECT_ROOT / candidate
        return candidate
    return default


def load_text(path: Path, *, default: str, prompt_name: str) -> str:
    """读取文本提示词；失败时回退到 default。"""
    try:
        return path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        logger.warning("提示词文件不存在，使用默认配置", prompt=prompt_name, file=str(path))
        return default.strip()
    except Exception as e:
        logger.error("加载提示词失败，使用默认配置", prompt=prompt_name, file=str(path), error=str(e))
        return default.strip()


def load_json(path: Path, *, default: Dict[str, Any], prompt_name: str) -> Dict[str, Any]:
    """读取 JSON Schema；失败时回退到 default。"""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning("提示词文件不存在，使用默认配置", prompt=prompt_name, file=str(path))
        return default
    except Exception as e:
        logger.error("加载提示词失败，使用默认配置", prompt=prompt_name, file=str(path), error=str(e))
        return default


# ============================================================================
# 数据库优先加载函数（异步）
# ============================================================================

async def load_text_with_db(
    scenario: str,
    prompt_type: str,
    file_path: Path,
    *,
    default: str,
    use_cache: bool = True
) -> str:
    """
    从数据库或文件加载文本提示词
    
    优先级：
    1. 缓存（如果启用）
    2. 数据库中激活的版本
    3. 文件版本
    4. 默认值
    
    Args:
        scenario: 场景标识（如 table_selector, nl2ir）
        prompt_type: 提示词类型（如 system, user_template）
        file_path: 文件路径（回退用）
        default: 默认值（最终回退）
        use_cache: 是否使用缓存
    
    Returns:
        提示词内容
    """
    cache_key = (scenario, prompt_type)
    
    # 1. 检查缓存
    if use_cache and cache_key in _prompt_cache:
        return _prompt_cache[cache_key]
    
    # 2. 尝试从数据库获取
    db_content = await _get_from_db(scenario, prompt_type)
    if db_content:
        if use_cache:
            _prompt_cache[cache_key] = db_content
        logger.debug("从数据库加载提示词", scenario=scenario, prompt_type=prompt_type)
        return db_content
    
    # 3. 从文件加载
    prompt_name = f"{scenario}_{prompt_type}"
    content = load_text(file_path, default=default, prompt_name=prompt_name)
    
    if use_cache:
        _prompt_cache[cache_key] = content
    
    return content


async def load_json_with_db(
    scenario: str,
    prompt_type: str,
    file_path: Path,
    *,
    default: Dict[str, Any],
    use_cache: bool = True
) -> Dict[str, Any]:
    """
    从数据库或文件加载 JSON Schema
    
    优先级同 load_text_with_db
    
    Args:
        scenario: 场景标识
        prompt_type: 提示词类型
        file_path: 文件路径
        default: 默认值
        use_cache: 是否使用缓存
    
    Returns:
        JSON Schema 字典
    """
    cache_key = (scenario, prompt_type)
    
    # 1. 检查缓存
    if use_cache and cache_key in _json_cache:
        return _json_cache[cache_key]
    
    # 2. 尝试从数据库获取
    db_content = await _get_from_db(scenario, prompt_type)
    if db_content:
        try:
            result = json.loads(db_content)
            if use_cache:
                _json_cache[cache_key] = result
            logger.debug("从数据库加载JSON提示词", scenario=scenario, prompt_type=prompt_type)
            return result
        except json.JSONDecodeError as e:
            logger.warning("数据库中的JSON解析失败，回退到文件", 
                         scenario=scenario, prompt_type=prompt_type, error=str(e))
    
    # 3. 从文件加载
    prompt_name = f"{scenario}_{prompt_type}"
    result = load_json(file_path, default=default, prompt_name=prompt_name)
    
    if use_cache:
        _json_cache[cache_key] = result
    
    return result


async def _get_from_db(scenario: str, prompt_type: str) -> Optional[str]:
    """从数据库获取激活的提示词内容"""
    try:
        from server.utils.db_pool import get_metadata_pool
        
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT content FROM prompt_templates
                WHERE scenario = $1 AND prompt_type = $2 AND is_active = TRUE
            """, scenario, prompt_type)
            
            if row:
                return row["content"]
        return None
    except Exception as e:
        # 数据库不可用时静默回退
        logger.debug("从数据库加载提示词失败，回退到文件", 
                    scenario=scenario, prompt_type=prompt_type, error=str(e))
        return None


def clear_prompt_cache():
    """清除提示词缓存（在更新提示词后调用）"""
    global _prompt_cache, _json_cache
    _prompt_cache.clear()
    _json_cache.clear()
    logger.info("提示词缓存已清除")


def clear_cache_for_prompt(scenario: str, prompt_type: str):
    """清除指定提示词的缓存"""
    cache_key = (scenario, prompt_type)
    _prompt_cache.pop(cache_key, None)
    _json_cache.pop(cache_key, None)
    logger.debug("清除提示词缓存", scenario=scenario, prompt_type=prompt_type)


# ============================================================================
# 同步版本（用于模块初始化时）
# ============================================================================

def load_text_sync_with_db_check(
    scenario: str,
    prompt_type: str,
    file_path: Path,
    *,
    default: str
) -> str:
    """
    同步加载提示词（优先数据库，但同步调用）
    
    注意：此函数在事件循环中会尝试异步加载，否则仅从文件加载。
    用于模块初始化时的提示词加载。
    """
    # 尝试使用已有的事件循环
    try:
        loop = asyncio.get_running_loop()
        # 如果已有事件循环，不能在此处运行异步代码
        # 直接从文件加载（异步加载会在后续请求时触发）
        prompt_name = f"{scenario}_{prompt_type}"
        return load_text(file_path, default=default, prompt_name=prompt_name)
    except RuntimeError:
        # 没有运行中的事件循环，可以创建新的
        try:
            return asyncio.run(_load_text_async(scenario, prompt_type, file_path, default))
        except Exception:
            # 任何异常都回退到文件
            prompt_name = f"{scenario}_{prompt_type}"
            return load_text(file_path, default=default, prompt_name=prompt_name)


async def _load_text_async(
    scenario: str, 
    prompt_type: str, 
    file_path: Path, 
    default: str
) -> str:
    """内部异步加载函数"""
    return await load_text_with_db(scenario, prompt_type, file_path, default=default, use_cache=False)
