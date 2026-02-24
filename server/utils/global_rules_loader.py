"""
全局规则加载器
从配置文件和数据库加载全局规则（派生指标、默认过滤、自定义规则）

加载优先级：
1. 配置文件 (config/global_rules.yaml) - 作为基础规则
2. PostgreSQL 数据库 (global_rules 表) - 可覆盖配置文件中的同名规则

特性：
- 配置文件中的规则作为兜底，确保常用派生指标始终可用
- 数据库中的规则优先级更高，可覆盖配置文件
- 支持规则去重（按 display_name 去重）
"""

import os
import time
from copy import deepcopy
from pathlib import Path
from typing import List, Dict, Optional, Any
from uuid import UUID
import asyncpg
import structlog
import yaml

from server.config import settings

logger = structlog.get_logger()

# 配置文件路径
GLOBAL_RULES_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "global_rules.yaml"

# 配置文件规则缓存（进程级别）
_config_rules_cache: Optional[List[Dict[str, Any]]] = None
_config_rules_cache_mtime: Optional[float] = None


def _load_rules_from_config() -> List[Dict[str, Any]]:
    """
    从配置文件加载全局规则
    
    Returns:
        规则列表，每个规则都是符合数据库格式的字典
    """
    global _config_rules_cache, _config_rules_cache_mtime
    
    if not GLOBAL_RULES_CONFIG_PATH.exists():
        logger.warning(f"全局规则配置文件不存在: {GLOBAL_RULES_CONFIG_PATH}")
        return []
    
    try:
        # 检查文件修改时间，决定是否使用缓存
        current_mtime = os.path.getmtime(GLOBAL_RULES_CONFIG_PATH)
        if _config_rules_cache is not None and _config_rules_cache_mtime == current_mtime:
            return _config_rules_cache
        
        # 加载配置文件
        with open(GLOBAL_RULES_CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config:
            return []
        
        rules = []
        
        # 1. 加载派生指标
        derived_metrics = config.get('derived_metrics', [])
        for idx, dm in enumerate(derived_metrics):
            if not dm.get('is_active', True):
                continue
            
            # 构建与数据库格式兼容的规则字典
            rule = {
                "rule_id": f"config_derived_{dm.get('name', idx)}",
                "rule_type": "derived_metric",
                "rule_name": dm.get('name', ''),
                "description": dm.get('description', ''),
                "rule_definition": {
                    "display_name": dm.get('display_name', dm.get('name', '')),
                    "formula": dm.get('formula', ''),
                    "field_dependencies": _convert_field_dependencies(dm.get('field_dependencies', [])),
                    "unit": dm.get('unit', ''),
                    "decimal_places": dm.get('decimal_places', 2),
                    "synonyms": dm.get('synonyms', [])
                },
                "scope": dm.get('scope', 'global'),
                "domain_id": None,
                "priority": dm.get('priority', 0),
                "is_active": True,
                "_source": "config"  # 标记来源
            }
            rules.append(rule)
        
        # 2. 加载默认过滤规则
        default_filters = config.get('default_filters', [])
        for idx, df in enumerate(default_filters):
            if not df.get('is_active', True):
                continue
            
            rule = {
                "rule_id": f"config_filter_{idx}",
                "rule_type": "default_filter",
                "rule_name": df.get('name', f'默认过滤_{idx}'),
                "description": df.get('description', ''),
                "rule_definition": df.get('definition', {}),
                "scope": df.get('scope', 'global'),
                "domain_id": None,
                "priority": df.get('priority', 0),
                "is_active": True,
                "_source": "config"
            }
            rules.append(rule)
        
        # 3. 加载自定义指令规则
        custom_instructions = config.get('custom_instructions', [])
        for idx, ci in enumerate(custom_instructions):
            if not ci.get('is_active', True):
                continue
            
            rule = {
                "rule_id": f"config_instruction_{idx}",
                "rule_type": "custom_instruction",
                "rule_name": ci.get('name', f'自定义指令_{idx}'),
                "description": ci.get('description', ''),
                "rule_definition": ci.get('definition', {}),
                "scope": ci.get('scope', 'global'),
                "domain_id": None,
                "priority": ci.get('priority', 0),
                "is_active": True,
                "_source": "config"
            }
            rules.append(rule)
        
        # 更新缓存
        _config_rules_cache = rules
        _config_rules_cache_mtime = current_mtime
        
        logger.debug(f"从配置文件加载了 {len(rules)} 条全局规则")
        return rules
        
    except Exception as e:
        logger.exception("加载全局规则配置文件失败", error=str(e))
        return []


def _convert_field_dependencies(deps: List[Any]) -> List[Dict[str, Any]]:
    """
    转换字段依赖格式
    
    配置文件格式:
        - field_name: "总价"
          aggregation: "SUM"
    
    数据库格式:
        - field_id: "uuid"
          aggregation: "SUM"
    
    注意：配置文件中使用 field_name，编译时需要动态解析为 field_id
    """
    result = []
    for dep in deps:
        if isinstance(dep, dict):
            result.append({
                "field_name": dep.get('field_name', ''),
                "field_id": dep.get('field_id'),  # 可能为空，编译时解析
                "aggregation": dep.get('aggregation')
            })
        elif isinstance(dep, str):
            # 兼容简单字符串格式
            result.append({
                "field_name": dep,
                "field_id": None,
                "aggregation": "SUM"
            })
    return result


def _merge_rules(config_rules: List[Dict], db_rules: List[Dict]) -> List[Dict]:
    """
    合并配置文件规则和数据库规则
    
    策略：
    - 数据库规则优先级更高，会覆盖配置文件中的同名规则
    - 按 display_name（派生指标）或 rule_name（其他规则）去重
    """
    merged = []
    seen_names = set()
    
    # 先添加数据库规则（优先级高）
    for rule in db_rules:
        rule_type = rule.get('rule_type', '')
        rule_def = rule.get('rule_definition', {})
        
        # 获取规则的唯一标识名
        if rule_type == 'derived_metric':
            name = rule_def.get('display_name') or rule.get('rule_name', '')
        else:
            name = rule.get('rule_name', '')
        
        if name and name not in seen_names:
            merged.append(rule)
            seen_names.add(name)
    
    # 再添加配置文件规则（不覆盖已存在的）
    for rule in config_rules:
        rule_type = rule.get('rule_type', '')
        rule_def = rule.get('rule_definition', {})
        
        if rule_type == 'derived_metric':
            name = rule_def.get('display_name') or rule.get('rule_name', '')
        else:
            name = rule.get('rule_name', '')
        
        if name and name not in seen_names:
            merged.append(rule)
            seen_names.add(name)
            logger.debug(f"从配置文件补充规则: {name}")
    
    return merged


class GlobalRulesLoader:
    """
    全局规则加载器
    
    支持从配置文件和数据库两个来源加载规则：
    - 配置文件 (config/global_rules.yaml)：作为兜底规则
    - PostgreSQL 数据库 (global_rules 表)：可覆盖配置文件
    """
    
    def __init__(self, connection_id: UUID):
        self.connection_id = connection_id
        self._cache: Dict[str, Any] = {}
        self._cache_time: Optional[float] = None
        self._cache_ttl = 300  # 5分钟缓存
    
    async def load_active_rules(
        self,
        rule_types: Optional[List[str]] = None,
        domain_id: Optional[UUID] = None,
        field_ids: Optional[List[UUID]] = None,
        allow_cross_table: bool = True
    ) -> List[Dict]:
        """
        加载激活的全局规则，支持字段级过滤
        
        加载顺序：
        1. 从配置文件加载规则（作为兜底）
        2. 从数据库加载规则（优先级更高）
        3. 合并去重（数据库规则覆盖同名配置规则）

        Args:
            rule_types: 规则类型过滤（如 ['derived_metric', 'custom_instruction']）
            domain_id: 业务域ID过滤（如果指定，只返回该域或全局规则）
            field_ids: 字段ID过滤（用于字段级规则）
            allow_cross_table: 是否允许跨表规则

        Returns:
            规则列表
        """
        # 允许调用方传入 str/UUID，统一进行规范化
        if domain_id and not isinstance(domain_id, UUID):
            try:
                domain_id = UUID(str(domain_id))
            except (ValueError, TypeError):
                logger.warning("domain_id 无法解析为 UUID，已忽略", domain_id=str(domain_id))
                domain_id = None

        # 缓存键（包含字段级过滤参数）
        cache_key = f"{rule_types}_{domain_id}_{field_ids}_{allow_cross_table}"

        # 缓存检查
        now = time.time()
        if self._cache_time and (now - self._cache_time) < self._cache_ttl:
            cached_rules = self._cache.get(cache_key)
            if cached_rules is not None:
                logger.debug("从缓存加载全局规则", count=len(cached_rules))
                return cached_rules

        # 1. 从配置文件加载规则（作为兜底）
        config_rules = _load_rules_from_config()
        
        # 按规则类型过滤配置文件规则
        if rule_types:
            config_rules = [r for r in config_rules if r.get('rule_type') in rule_types]
        
        # 2. 从数据库加载规则
        db_rules = []
        try:
            from server.utils.db_pool import get_metadata_pool
            pool = await get_metadata_pool()

            async with pool.acquire() as conn:
                db_rules = await self._fetch_rules(conn, rule_types, domain_id, field_ids, allow_cross_table)
                
        except Exception as e:
            logger.warning("从数据库加载全局规则失败，将使用配置文件规则", error=str(e))
        
        # 3. 合并规则（数据库优先，配置文件兜底）
        merged_rules = _merge_rules(config_rules, db_rules)
        
        # 更新缓存
        self._cache[cache_key] = merged_rules
        self._cache_time = now

        logger.debug(
            "全局规则加载完成",
            total=len(merged_rules),
            from_db=len(db_rules),
            from_config=len([r for r in merged_rules if r.get('_source') == 'config']),
            rule_types=rule_types,
            domain_id=str(domain_id) if domain_id else None
        )

        return merged_rules
    
    async def _get_db_connection(self):
        """获取数据库连接（使用连接池）"""
        from server.utils.db_pool import get_metadata_pool
        pool = await get_metadata_pool()
        return await pool.acquire()
    
    async def _fetch_rules(
        self,
        conn: asyncpg.Connection,
        rule_types: Optional[List[str]],
        domain_id: Optional[UUID],
        field_ids: Optional[List[UUID]] = None,
        allow_cross_table: bool = True
    ) -> List[Dict]:
        """从数据库查询规则，支持字段级过滤"""

        # 构建查询（兼容全局派生指标 connection_id 为空的场景）
        where_clauses = ["(connection_id = $1 OR connection_id IS NULL)", "is_active = TRUE"]
        params = [self.connection_id]
        param_index = 2

        # 规则类型过滤
        if rule_types:
            where_clauses.append(f"rule_type = ANY(${param_index})")
            params.append(rule_types)
            param_index += 1

        # 业务域过滤（包含全局规则和指定域的规则）
        if domain_id:
            where_clauses.append(f"(scope = 'global' OR domain_id = ${param_index})")
            params.append(domain_id)
            param_index += 1
        else:
            # 如果没有指定域，只返回全局规则
            where_clauses.append("scope = 'global'")

        # 字段级过滤和跨表规则控制
        if field_ids and not allow_cross_table:
            # 严格字段级过滤：只加载指定字段的规则
            where_clauses.append(f"""
                (rule_definition->>'field_id' = ANY(${param_index})
                 OR rule_type NOT IN ('derived_metric'))
            """)
            params.append(field_ids)
            param_index += 1
        elif field_ids and allow_cross_table:
            # 灵活过滤：加载指定字段 + 跨表通用规则
            where_clauses.append(f"""
                (rule_definition->>'field_id' = ANY(${param_index})
                 OR rule_definition->>'scope' = 'cross_table'
                 OR rule_type NOT IN ('derived_metric'))
            """)
            params.append(field_ids)
            param_index += 1

        query = f"""
            SELECT
                rule_id, rule_type, rule_name, description,
                rule_definition, scope, domain_id, priority, is_active,
                created_at, updated_at
            FROM global_rules
            WHERE {' AND '.join(where_clauses)}
            ORDER BY priority DESC, rule_name
        """

        rows = await conn.fetch(query, *params)

        # 转换为字典并确保 rule_definition 是字典类型
        rules = []
        for row in rows:
            rule_dict = dict(row)

            # 确保 rule_definition 是字典（asyncpg 可能返回字符串）
            rule_def = rule_dict.get('rule_definition')
            if isinstance(rule_def, str):
                import json
                try:
                    rule_dict['rule_definition'] = json.loads(rule_def)
                except json.JSONDecodeError:
                    logger.warning(f"无法解析规则定义: {rule_dict.get('rule_name')}", rule_def=rule_def)
                    continue

            rules.append(rule_dict)

        return rules
    
    
    def invalidate_cache(self):
        """清除缓存"""
        self._cache.clear()
        self._cache_time = None
        logger.debug("全局规则缓存已清除")
    
    async def reload_rules(
        self,
        rule_types: Optional[List[str]] = None,
        domain_id: Optional[UUID] = None,
        field_ids: Optional[List[UUID]] = None,
        allow_cross_table: bool = True
    ) -> List[Dict]:
        """强制重新加载规则（清除缓存）"""
        self.invalidate_cache()
        return await self.load_active_rules(rule_types, domain_id, field_ids, allow_cross_table)


class GlobalRulesManager:
    """全局规则管理器（单例模式）"""
    
    _loaders: Dict[UUID, GlobalRulesLoader] = {}
    
    @classmethod
    def get_loader(cls, connection_id: UUID) -> GlobalRulesLoader:
        """获取指定连接的规则加载器"""
        if connection_id not in cls._loaders:
            cls._loaders[connection_id] = GlobalRulesLoader(connection_id)
        return cls._loaders[connection_id]
    
    @classmethod
    def invalidate_all(cls):
        """清除所有缓存"""
        for loader in cls._loaders.values():
            loader.invalidate_cache()
        logger.debug("所有全局规则缓存已清除")
