"""
同步流程的实体构建层
负责基于查询结果创建Milvus写入所需的数据结构
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional, Sequence
from uuid import UUID

import structlog
import re

from server.config import settings, RetrievalConfig
from server.nl2ir.sparse_utils import (
    ensure_list,
    prepare_bm25_text,
    prepare_dense_text,
)
logger = structlog.get_logger()


def _parse_metadata(value) -> Dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _format_graph_text(current_name: str, relations) -> str:
    if not relations:
        return ""
    if isinstance(relations, str):
        try:
            relations = json.loads(relations)
        except json.JSONDecodeError:
            relations = []
    lines: List[str] = []
    for rel in relations or []:
        target = rel.get("target_table_name") or rel.get("target_table_id")
        if not target:
            continue
        rel_type = rel.get("relationship_type") or "related"
        join_type = rel.get("join_type")
        lines.append(
            f"{current_name} --[{rel_type}{'/' + join_type if join_type else ''}]--> {target}"
        )
    return "; ".join(lines)


def normalize_tags(tags) -> List[str]:
    """兼容不同类型的标签字段"""
    return ensure_list(tags)


# 缓存同步增强配置
_sync_enhancement_config: Optional[Dict[str, Any]] = None


def _get_sync_enhancement_config() -> Dict[str, Any]:
    """获取同步增强配置（从retrieval_config.yaml读取）"""
    global _sync_enhancement_config
    
    if _sync_enhancement_config is not None:
        return _sync_enhancement_config
    
    try:
        from server.config import get_retrieval_param
        
        _sync_enhancement_config = {
            "high_value_dimensions": get_retrieval_param(
                "sync_enhancement.high_value_dimensions",
                ["行政区", "年份", "用途", "类型", "状态", "街道"]
            ),
            "admin_suffixes": get_retrieval_param(
                "sync_enhancement.enum_variant_generation.admin_suffixes",
                ["区", "县", "市", "镇", "乡", "街道", "街"]
            ),
            "zone_replacements": get_retrieval_param(
                "sync_enhancement.enum_variant_generation.zone_replacements",
                [
                    {"pattern": "新技术开发区", "replacement": "高新"},
                    {"pattern": "经济技术开发区", "replacement": "经开"},
                    {"pattern": "开发区", "replacement": ""},
                    {"pattern": "新区", "replacement": ""},
                ]
            ),
            "location_aliases": get_retrieval_param(
                "sync_enhancement.enum_variant_generation.location_aliases",
                {}
            ),
            "important_field_keywords": get_retrieval_param(
                "sync_enhancement.important_field_keywords",
                {
                    "measure": ["面积", "数量", "金额", "总", "数", "量", "值", "额"],
                    "dimension": ["行政区", "区域", "年份", "年度", "类型", "名称", "状态", "代码"],
                    "time": ["时间", "日期", "日", "时"],
                }
            ),
        }
    except Exception:
        # 配置加载失败时使用默认值
        _sync_enhancement_config = {
            "high_value_dimensions": ["行政区", "年份", "用途", "类型", "状态", "街道"],
            "admin_suffixes": ["区", "县", "市", "镇", "乡", "街道", "街"],
            "zone_replacements": [
                {"pattern": "新技术开发区", "replacement": "高新"},
                {"pattern": "经济技术开发区", "replacement": "经开"},
                {"pattern": "开发区", "replacement": ""},
                {"pattern": "新区", "replacement": ""},
            ],
            "location_aliases": {},
            "important_field_keywords": {
                "measure": ["面积", "数量", "金额", "总", "数", "量", "值", "额"],
                "dimension": ["行政区", "区域", "年份", "年度", "类型", "名称", "状态", "代码"],
                "time": ["时间", "日期", "日", "时"],
            },
        }
    
    return _sync_enhancement_config


def _generate_enum_variants(value: str) -> List[str]:
    """
    P2新增：自动生成枚举值变体（同义词扩展）
    
    配置驱动，从retrieval_config.yaml读取：
    - sync_enhancement.enum_variant_generation.admin_suffixes
    - sync_enhancement.enum_variant_generation.zone_replacements
    - sync_enhancement.enum_variant_generation.location_aliases
    
    Args:
        value: 原始枚举值
        
    Returns:
        变体列表
    """
    if not value:
        return []
    
    config = _get_sync_enhancement_config()
    variants = []
    value_stripped = value.strip()
    
    # 1. 行政区变体：去除后缀（从配置读取）
    admin_suffixes = config.get("admin_suffixes", [])
    for suffix in admin_suffixes:
        if value_stripped.endswith(suffix) and len(value_stripped) > len(suffix) + 1:
            short_form = value_stripped[:-len(suffix)]
            if short_form:
                variants.append(short_form)
    
    # 2. 开发区/新区替换（从配置读取）
    zone_replacements = config.get("zone_replacements", [])
    for rule in zone_replacements:
        pattern = rule.get("pattern", "")
        replacement = rule.get("replacement", "")
        if pattern and pattern in value_stripped:
            short_name = value_stripped.replace(pattern, replacement)
            if short_name and short_name != value_stripped:
                variants.append(short_name)
    
    # 3. 地名别名映射（从配置读取）
    location_aliases = config.get("location_aliases", {})
    if value_stripped in location_aliases:
        aliases = location_aliases[value_stripped]
        if isinstance(aliases, list):
            variants.extend(aliases)
    
    # 4. 年份变体
    if re.match(r"^\d{4}$", value_stripped):
        variants.append(f"{value_stripped}年")
    elif re.match(r"^\d{4}年$", value_stripped):
        variants.append(value_stripped[:-1])
    
    # 5. 去除空格变体
    compact = re.sub(r"\s+", "", value_stripped)
    if compact != value_stripped:
        variants.append(compact)
    
    # 去重并返回
    return list(set(v for v in variants if v and v != value_stripped))


def _ensure_list(value) -> List[str]:
    """内部使用的列表归一化（保持与 ensure_list 行为一致）"""
    return ensure_list(value)


def _stringify_tables_for_text(tables_payload) -> List[str]:
    """将Few-Shot表信息转换为可读文本（支持dict/str/list）。"""
    if not tables_payload:
        return []
    items = tables_payload
    if isinstance(items, str):
        try:
            items = json.loads(items) or []
        except json.JSONDecodeError:
            items = [items]
    if not isinstance(items, list):
        items = [items]

    result: List[str] = []
    for item in items:
        if isinstance(item, dict):
            label = (
                item.get("display_name")
                or item.get("table_display_name")
                or item.get("physical_table_name")
                or item.get("table_name")
                or item.get("table_id")
            )
            if label:
                result.append(str(label))
        elif item:
            result.append(str(item))
    return result


def build_index_text(name: str, description: Optional[str], keywords: Optional[Sequence[str]]) -> str:
    """构建业务域索引文本"""
    parts = [name] if name else []

    if description:
        parts.append(description)

    if keywords:
        if isinstance(keywords, str):
            try:
                parsed = json.loads(keywords)
                if isinstance(parsed, list):
                    parts.extend([str(x) for x in parsed if x])
                else:
                    parts.extend([s.strip() for s in keywords.split(",") if s.strip()])
            except json.JSONDecodeError:
                parts.extend([s.strip() for s in keywords.split(",") if s.strip()])
        elif isinstance(keywords, (list, tuple, set)):
            parts.extend([str(x) for x in keywords if x])
        else:
            parts.append(str(keywords))

    return " ".join(parts)


def build_rich_table_index_text(
    display_name: str,
    description: Optional[str],
    tags,
    field_names: Sequence[str],
    field_count: int,
    data_year: Optional[int],
) -> str:
    """构建表级索引文本（包含字段、标签等富文本信息）"""
    parts = [display_name]

    if description:
        parts.append(description)

    normalized_tags = normalize_tags(tags)
    if normalized_tags:
        parts.append("标签: " + ", ".join(normalized_tags))

    if data_year:
        parts.append(f"数据年份: {data_year}")

    parts.append(f"字段数: {field_count}")

    if field_names:
        parts.append("字段列表: " + ", ".join(field_names))

    return "\n".join(parts)


async def build_domain_entities(
    domains: Iterable,
    embedding_client,
    connection_id: Optional[UUID] = None,
) -> List[dict]:
    """构建业务域实体（带稠密/稀疏向量与元信息）
    
    P0优化：增强bm25_text，包含关联表名和代表性字段，提升稀疏检索准确性
    
    Args:
        domains: 业务域记录列表（需包含table_names, representative_fields）
        embedding_client: 嵌入客户端
        connection_id: 可选的连接ID，为None时从记录中获取
    """
    entities: List[dict] = []
    for domain in domains:
        # 从记录中获取connection_id或使用传入的参数
        record_conn_id = domain.get("connection_id") or connection_id
        conn_id_str = str(record_conn_id) if record_conn_id else ""
        
        # 提取关联表名列表（P0新增）
        table_names = _ensure_list(domain.get("table_names"))
        # 提取代表性字段名列表（P0新增）
        representative_fields = _ensure_list(domain.get("representative_fields"))
        
        # 构建增强的BM25文本（P0优化）
        # 包含：类型标识、域名、code、描述、关键词、关联表名、代表性字段
        bm25_parts = [
            "type:domain",
            domain["domain_name"],
            domain.get("domain_code"),
            domain.get("description"),
            " ".join(normalize_tags(domain.get("keywords"))),
        ]
        
        # 添加关联表名（提升"建设用地批准书属于哪个域"类问题的召回）
        if table_names:
            bm25_parts.append("tables: " + " ".join(table_names[:20]))  # 限制前20个
        
        # 添加代表性字段（提升"用地面积在哪个域"类问题的召回）
        if representative_fields:
            bm25_parts.append("fields: " + " ".join(representative_fields[:30]))  # 限制前30个
        
        bm25_text = prepare_bm25_text(bm25_parts, RetrievalConfig.bm25_text_limit())
        
        # 构建Dense文本（保持语义完整性）
        dense_parts = [
            "domain",
            domain["domain_name"],
            f"code: {domain.get('domain_code')}" if domain.get("domain_code") else "",
            domain.get("description") or "",
            " ".join(normalize_tags(domain.get("keywords"))),
        ]
        if table_names:
            dense_parts.append(f"关联表: {', '.join(table_names[:10])}")
        
        dense_text = prepare_dense_text(
            "\n".join(filter(None, dense_parts)),
            RetrievalConfig.dense_text_limit(),
        )
        
        try:
            vector = await embedding_client.embed_single(dense_text or domain["domain_name"])
        except Exception as embed_error:
            logger.warning(
                "生成业务域向量失败",
                domain_id=str(domain["domain_id"]),
                error=str(embed_error),
            )
            continue

        # 构建增强的json_meta（P0优化）
        table_count = int(domain.get("table_count") or 0)
        json_meta = {
            "domain_code": domain.get("domain_code"),
            "keywords": normalize_tags(domain.get("keywords")),
            "table_count": table_count,
            # P0新增：关联表名列表（用于检索验证和调试）
            "table_names": table_names[:20] if table_names else [],
            # P0新增：代表性字段列表
            "representative_fields": representative_fields[:30] if representative_fields else [],
        }
        
        entities.append(
            {
                "item_id": str(domain["domain_id"]),
                "connection_id": conn_id_str,
                "domain_id": str(domain["domain_id"]),
                "table_id": "",
                "entity_type": "domain",
                "semantic_type": "domain",
                "schema_name": "",
                "table_name": "",
                "column_name": "",
                "display_name": domain["domain_name"],
                "description": domain.get("description") or "",
                "graph_text": "",
                "field_id": "",
                "dense_vector": vector,
                # sparse_vector 由 BM25 Function 自动生成，无需手动构建
                "bm25_text": bm25_text,
                "json_meta": json_meta,
                "is_active": bool(domain.get("is_active", True)),
            }
        )
        
        logger.debug(
            "业务域实体构建完成",
            domain_name=domain["domain_name"],
            table_count=table_count,
            table_names_count=len(table_names),
            bm25_length=len(bm25_text),
        )

    return entities


def _identify_important_fields(fields: List[Dict], max_count: int = 8) -> List[str]:
    """
    基于语义自动识别重要字段
    
    策略:
    - 度量字段（数值型）：优先选择带"面积"、"数量"、"金额"等描述的字段
    - 维度字段：优先选择"行政区"、"年份"、"类型"等
    - 时间字段：自动识别timestamp类型
    
    Args:
        fields: 字段列表（包含 display_name, field_type, data_type, description 等）
        max_count: 最大返回数量
    
    Returns:
        重要字段名称列表
    """
    if not fields:
        return []
    
    # 从配置读取关键词（消除硬编码）
    config = _get_sync_enhancement_config()
    important_kw_config = config.get("important_field_keywords", {})
    measure_keywords = important_kw_config.get("measure", ["面积", "数量", "金额", "总", "数", "量", "值", "额"])
    dimension_keywords = important_kw_config.get("dimension", ["行政区", "区域", "年份", "年度", "类型", "名称", "状态", "代码"])
    time_keywords = important_kw_config.get("time", ["时间", "日期", "日", "时"])
    
    scored_fields = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        score = 0
        display_name = field.get("display_name", "")
        field_type = field.get("field_type", "")
        data_type = (field.get("data_type") or "").lower()
        description = field.get("description", "") or ""
        
        # 时间字段加分
        if "time" in data_type or "date" in data_type:
            score += 3
        for kw in time_keywords:
            if kw in display_name or kw in description:
                score += 2
        
        # 度量字段加分
        if field_type == "measure":
            score += 2
            for kw in measure_keywords:
                if kw in display_name or kw in description:
                    score += 1
        
        # 维度字段加分
        if field_type == "dimension":
            for kw in dimension_keywords:
                if kw in display_name or kw in description:
                    score += 2
        
        scored_fields.append((display_name, score))
    
    # 按分数排序，取前N个
    scored_fields.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in scored_fields[:max_count] if name]


def _parse_foreign_keys(relations: List) -> List[Dict]:
    """
    从关系信息中解析外键定义
    
    Args:
        relations: 表关系列表
    
    Returns:
        外键定义列表
    """
    if not relations:
        return []
    
    foreign_keys = []
    for rel in relations:
        if not isinstance(rel, dict):
            continue
        
        join_columns = rel.get("join_columns") or []
        target_table = rel.get("target_table_name") or rel.get("target_table_id")
        
        if not target_table:
            continue
        
        for join_col in join_columns:
            if isinstance(join_col, dict):
                fk = {
                    "field": join_col.get("source_column") or join_col.get("source"),
                    "ref_table": target_table,
                    "ref_field": join_col.get("target_column") or join_col.get("target")
                }
                if fk["field"] and fk["ref_field"]:
                    foreign_keys.append(fk)
    
    return foreign_keys


async def build_table_entities(
    tables: Iterable,
    embedding_client,
    connection_id: Optional[UUID] = None,
) -> List[dict]:
    """构建表实体（含 graph_text 与稀疏文本）
    
    Args:
        tables: 表记录列表
        embedding_client: 嵌入客户端
        connection_id: 可选的连接ID，为None时从记录中获取
    """
    entities: List[dict] = []

    for table in tables:
        # 从记录中获取connection_id或使用传入的参数
        record_conn_id = table.get("connection_id") or connection_id
        conn_id_str = str(record_conn_id) if record_conn_id else ""
        
        display_name = table["display_name"] or table["table_name"]
        field_names = table.get("field_names") or []
        graph_text = _format_graph_text(display_name, table.get("relations"))
        domain_id = table.get("domain_id")
        domain_name = table.get("domain_name") or ""
        
        # 解析字段详情以识别重要字段
        field_details = table.get("field_details") or []
        if isinstance(field_details, str):
            try:
                import json
                field_details = json.loads(field_details) or []
            except Exception:
                field_details = []
        important_fields = _identify_important_fields(field_details)
        if not important_fields and field_names:
            # 如果没有详细字段信息，使用前8个字段名
            important_fields = field_names[:8]
        
        # 解析外键
        relations = table.get("relations") or []
        foreign_keys = _parse_foreign_keys(relations)
        
        # 解析主键
        primary_keys = table.get("primary_keys") or []
        if isinstance(primary_keys, str):
            try:
                primary_keys = json.loads(primary_keys)
            except json.JSONDecodeError:
                primary_keys = [primary_keys] if primary_keys else []
        
        # P1增强：从字段详情中提取度量和维度字段
        measure_fields = []
        dimension_fields = []
        for fd in field_details:
            if isinstance(fd, dict):
                field_type = fd.get("field_type", "")
                field_display = fd.get("display_name", "")
                if field_type == "measure" and field_display:
                    measure_fields.append(field_display)
                elif field_type == "dimension" and field_display:
                    dimension_fields.append(field_display)
        
        # P1增强：构建层级路径（domain > table）
        hierarchy_path = f"{domain_name} > {display_name}" if domain_name else display_name
        
        # P1增强：构建增强的BM25文本
        bm25_parts = [
            display_name,
            table["table_name"],
            domain_name,
            table.get("description"),
            " ".join(field_names[:20]),
            " ".join(normalize_tags(table.get("tags"))),
            " ".join(primary_keys) if primary_keys else "",
            " ".join(important_fields) if important_fields else "",
            graph_text,
        ]
        
        # 添加层级路径标识（提升"土地管理审批下的表"类问题的召回）
        if domain_name:
            bm25_parts.append(f"path:{hierarchy_path}")
        
        # 添加度量字段列表（提升"有用地面积的表"类问题的召回）
        if measure_fields:
            bm25_parts.append("measures: " + " ".join(measure_fields[:10]))
        
        # 添加核心维度字段（提升"有行政区字段的表"类问题的召回）
        if dimension_fields:
            # 选择前5个维度字段
            bm25_parts.append("dimensions: " + " ".join(dimension_fields[:5]))
        
        bm25_text = prepare_bm25_text(bm25_parts, RetrievalConfig.bm25_text_limit())
        dense_text = prepare_dense_text(
            "\n".join(
                filter(
                    None,
                    [
                        display_name,
                        table.get("description") or "",
                        f"Domain: {domain_name}" if domain_name else "",
                        f"PK: {', '.join(primary_keys)}" if primary_keys else "",
                        f"Important: {', '.join(important_fields)}" if important_fields else "",
                        f"Fields: {', '.join(field_names[:20])}" if field_names else "",
                        f"Tags: {', '.join(normalize_tags(table.get('tags')))}",
                        graph_text,
                    ],
                ),
            )
            ,
            RetrievalConfig.dense_text_limit(),
        )
        try:
            vector = await embedding_client.embed_single(dense_text or display_name)
        except Exception as embed_error:
            logger.warning(
                "生成表向量失败",
                table_id=str(table["table_id"]),
                error=str(embed_error),
            )
            continue

        stats = {
            "row_count": int(table.get("row_count") or 0),
            "field_count": int(table.get("field_count") or 0),
            "is_partitioned": bool(table.get("is_partitioned", False)),
        }
        # 构建增强的 json_meta
        json_meta = {
            # 核心标识
            "entity_type": "table",
            "domain_id": str(domain_id) if domain_id else "",
            "domain_name": table.get("domain_name") or "",
            "table_name": table["table_name"],
            "display_name": display_name,
            "description": table.get("description") or "",
            
            # 关键信息（用于Prompt构建和Join推断）
            "primary_keys": primary_keys if isinstance(primary_keys, list) else [],
            "foreign_keys": foreign_keys,
            "important_fields": important_fields,
            
            # 统计信息
            "stats": stats,
            
            # 其他元信息
            "tags": normalize_tags(table.get("tags")),
            "data_year": table.get("data_year"),
            "relations_count": len(relations),
            "schema_name": table.get("schema_name") or "",
        }
        # 顶层冗余字段，兼容旧版消费逻辑
        json_meta["field_count"] = stats["field_count"]

        entities.append(
            {
                "item_id": str(table["table_id"]),
                "connection_id": conn_id_str,
                "domain_id": str(domain_id) if domain_id else "",
                "table_id": str(table["table_id"]),
                "entity_type": "table",
                "semantic_type": "table",
                "schema_name": table["schema_name"],
                "table_name": table["table_name"],
                "column_name": "",
                "display_name": display_name,
                "description": table.get("description") or "",
                "graph_text": graph_text,
                "field_id": "",
                "dense_vector": vector,
                # sparse_vector 由 BM25 Function 自动生成，无需手动构建
                "bm25_text": bm25_text,
                "json_meta": json_meta,
                "is_active": True,
            }
        )

    return entities


async def build_field_entities(
    fields: Iterable,
    embedding_client,
    connection_id: Optional[UUID] = None,
) -> List[dict]:
    """构建字段实体
    
    Args:
        fields: 字段记录列表
        embedding_client: 嵌入客户端
        connection_id: 可选的连接ID，为None时从记录中获取
    """
    entities: List[dict] = []
    for field in fields:
        # 从记录中获取connection_id或使用传入的参数
        record_conn_id = field.get("connection_id") or connection_id
        conn_id_str = str(record_conn_id) if record_conn_id else ""
        
        display_name = field["display_name"]
        semantic_type = field.get("field_type") or "dimension"
        synonyms = _ensure_list(field.get("synonyms"))
        domain_name = field.get("domain_name") or ""
        table_display_name = field.get("table_display_name") or ""
        
        # P1增强：构建完整层级路径（domain > table > field）
        hierarchy_parts = []
        if domain_name:
            hierarchy_parts.append(domain_name)
        if table_display_name:
            hierarchy_parts.append(table_display_name)
        hierarchy_parts.append(display_name)
        hierarchy_path = " > ".join(hierarchy_parts)
        
        # P1增强：构建增强的BM25文本
        bm25_parts = [
            "type:field",
            f"semantic:{semantic_type}",
            display_name,
            field.get("column_name"),
            field.get("description"),
            table_display_name,
            domain_name,
            semantic_type,
            " ".join(synonyms),
        ]
        
        # 添加完整层级路径（提升"土地管理审批/建设用地批准书/行政区"类问题的召回）
        bm25_parts.append(f"path:{hierarchy_path}")
        
        # 如果是度量字段，添加度量标识增强
        if semantic_type == "measure":
            bm25_parts.append("度量字段 数值 统计 聚合")
        
        # 如果是高价值维度字段，添加额外标识（从配置读取）
        config = _get_sync_enhancement_config()
        high_value_dims = set(config.get("high_value_dimensions", []))
        if display_name in high_value_dims or any(hv in display_name for hv in high_value_dims):
            bm25_parts.append(f"核心维度 {display_name}")
        
        bm25_text = prepare_bm25_text(bm25_parts, RetrievalConfig.bm25_text_limit())
        dense_text = prepare_dense_text(
            "\n".join(
                filter(
                    None,
                    [
                        f"field {semantic_type}",
                        display_name,
                        f"Column: {field.get('column_name')}",
                        f"Table: {field.get('table_display_name')}",
                        f"Domain: {domain_name}" if domain_name else "",
                        semantic_type,
                        field.get("description") or "",
                        "Synonyms: " + ", ".join(synonyms) if synonyms else "",
                    ],
                ),
            )
            ,
            RetrievalConfig.dense_text_limit(),
        )
        try:
            vector = await embedding_client.embed_single(dense_text or display_name)
        except Exception as embed_error:
            logger.warning(
                "生成字段向量失败",
                field_id=str(field["field_id"]),
                error=str(embed_error),
            )
            continue

        entities.append(
            {
                "item_id": str(field["field_id"]),
                "connection_id": conn_id_str,
                "domain_id": str(field.get("domain_id") or ""),
                "table_id": str(field.get("table_id") or ""),
                "entity_type": "field",
                "semantic_type": semantic_type or "none",
                "schema_name": field.get("schema_name") or "",
                "table_name": field.get("table_name") or "",
                "column_name": field.get("column_name") or "",
                "display_name": display_name,
                "description": field.get("description") or "",
                "graph_text": "",
                "field_id": str(field["field_id"]),
                "dense_vector": vector,
                # sparse_vector 由 BM25 Function 自动生成，无需手动构建
                "bm25_text": bm25_text,
                "json_meta": {
                    "synonyms": synonyms,
                    "table_display_name": field.get("table_display_name"),
                    "data_type": field.get("data_type"),
                    "unit": field.get("unit"),
                    "format_pattern": field.get("format_pattern"),
                    "domain_name": field.get("domain_name"),
                },
                "is_active": True,
            }
        )

    return entities


async def build_enum_entities(
    enum_rows: Iterable,
    embedding_client,
    connection_id: Optional[UUID] = None,
) -> List[dict]:
    """构建枚举值实体（双向量 + 稀疏）
    
    Args:
        enum_rows: 枚举值记录列表
        embedding_client: 嵌入客户端
        connection_id: 可选的连接ID，为None时从记录中获取
    """
    import asyncio

    entities: List[dict] = []
    threshold = RetrievalConfig.enum_cardinality_threshold()

    for enum in enum_rows:
        # 从记录中获取connection_id或使用传入的参数
        record_conn_id = enum.get("connection_id") or connection_id
        conn_id_str = str(record_conn_id) if record_conn_id else ""
        
        distinct_count = enum.get("distinct_count") or 0
        if threshold and distinct_count and distinct_count > threshold:
            logger.debug(
                "跳过高基数字段枚举同步",
                field_id=str(enum["field_id"]),
                distinct_count=distinct_count,
            )
            continue

        synonyms = _ensure_list(enum.get("synonyms"))
        value_boost = max(1, min(3, int(RetrievalConfig.hybrid_value_boost())))
        boosted_value = " ".join(
            [enum["original_value"]] * value_boost
        )
        compact_value = re.sub(r"\s+", "", enum["original_value"])
        
        field_name = enum.get("field_name") or ""
        table_display_name = enum.get("table_display_name") or ""
        domain_name = enum.get("domain_name") or ""
        
        # P2增强：构建完整层级路径（domain > table > field > value）
        hierarchy_parts = []
        if domain_name:
            hierarchy_parts.append(domain_name)
        if table_display_name:
            hierarchy_parts.append(table_display_name)
        if field_name:
            hierarchy_parts.append(field_name)
        hierarchy_path = " > ".join(hierarchy_parts) if hierarchy_parts else ""
        
        # P2增强：自动生成变体同义词
        auto_synonyms = _generate_enum_variants(enum["original_value"])
        all_synonyms = list(set(synonyms + auto_synonyms))
        
        # P2增强：构建增强的BM25文本
        bm25_parts = [
            boosted_value,
            compact_value,
            enum.get("display_value"),
            " ".join(all_synonyms),
            field_name,
            enum.get("field_description"),  # 添加字段说明以增强语义关联
            table_display_name,
        ]
        
        # 添加业务域名称（提升"土地管理审批的行政区"类问题的召回）
        if domain_name:
            bm25_parts.append(domain_name)
        
        # 添加层级路径（提升精准匹配）
        if hierarchy_path:
            bm25_parts.append(f"path:{hierarchy_path}")
        
        bm25_text = prepare_bm25_text(bm25_parts, RetrievalConfig.bm25_text_limit())
        value_index_text = prepare_dense_text(
            f"{enum['original_value']} {enum.get('display_value') or ''} {' '.join(synonyms)}"
            ,
            RetrievalConfig.dense_text_limit(),
        )
        context_snippets = [
            f"表【{enum.get('table_display_name') or '未知表'}】的字段【{enum.get('field_name')}】",
            f"枚举值为「{enum['original_value']}」",
        ]
        if enum.get("field_description"):
            context_snippets.append(f"字段说明：{enum['field_description']}")
        if synonyms:
            context_snippets.append(f"同义词：{'、'.join(synonyms[:3])}")
        context_index_text = prepare_dense_text(
            "。".join(context_snippets),
            RetrievalConfig.dense_text_limit(),
        )

        try:
            value_vector, context_vector = await asyncio.gather(
                embedding_client.embed_single(value_index_text or enum["original_value"]),
                embedding_client.embed_single(context_index_text or enum["field_name"]),
            )
        except Exception as embed_error:
            logger.warning(
                "生成枚举值向量失败",
                enum_value=str(enum["enum_value_id"]),
                error=str(embed_error),
            )
            continue

        # 构建增强的 json_meta
        enum_json_meta = {
            # 字段信息
            "field_id": str(enum["field_id"]),
            "field_name": enum.get("field_name") or "",
            "field_display_name": enum.get("field_display_name") or enum.get("field_name") or "",
            "field_type": enum.get("field_type") or "dimension",
            "field_description": enum.get("field_description") or "",
            
            # 表信息
            "table_id": str(enum.get("table_id") or ""),
            "table_name": enum.get("table_name") or "",
            "table_display_name": enum.get("table_display_name") or "",
            
            # 业务域信息
            "domain_id": str(enum.get("domain_id") or ""),
            "domain_name": enum.get("domain_name") or "",
            
            # 枚举值元信息
            "synonyms": synonyms,
            "frequency": enum.get("frequency") or 0,
            
            # 原始信息
            "schema_name": enum.get("schema_name") or "",
            "column_name": enum.get("column_name") or "",
        }
        
        entities.append(
            {
                "value_id": str(enum["enum_value_id"]),
                "field_id": str(enum["field_id"]),
                "table_id": str(enum.get("table_id") or ""),
                "domain_id": str(enum.get("domain_id") or ""),
                "field_name": enum.get("field_name"),
                "table_name": enum.get("table_display_name"),
                "connection_id": conn_id_str,
                "value": enum["original_value"],
                "display_name": enum.get("display_value")
                or enum["original_value"],
                "synonyms": json.dumps(synonyms, ensure_ascii=False),
                "frequency": enum.get("frequency") or 0,
                "value_index_text": value_index_text,
                "context_index_text": context_index_text,
                "bm25_text": bm25_text,
                "json_meta": enum_json_meta,
                "value_vector": value_vector,
                "context_vector": context_vector,
                # sparse_vector 由 BM25 Function 自动生成，无需手动构建
                "is_active": True,
            }
        )

    return entities


def _extract_related_fields_from_ir(ir_json: str) -> List[str]:
    """
    从IR JSON中提取相关字段名
    
    Args:
        ir_json: IR JSON字符串
    
    Returns:
        字段名列表
    """
    if not ir_json:
        return []
    
    try:
        ir_data = json.loads(ir_json) if isinstance(ir_json, str) else ir_json
    except json.JSONDecodeError:
        return []
    
    fields = set()
    
    # 从 measures 提取
    for measure in ir_data.get("measures", []):
        if isinstance(measure, dict):
            field_name = measure.get("field") or measure.get("field_name")
            if field_name:
                fields.add(field_name)
        elif isinstance(measure, str):
            fields.add(measure)
    
    # 从 dimensions 提取
    for dim in ir_data.get("dimensions", []):
        if isinstance(dim, dict):
            field_name = dim.get("field") or dim.get("field_name")
            if field_name:
                fields.add(field_name)
        elif isinstance(dim, str):
            fields.add(dim)
    
    # 从 filters 提取
    for flt in ir_data.get("filters", []):
        if isinstance(flt, dict):
            field_name = flt.get("field") or flt.get("field_name")
            if field_name:
                fields.add(field_name)
    
    return list(fields)


def _infer_query_type(ir_json: str) -> str:
    """
    从IR JSON推断查询类型
    
    Args:
        ir_json: IR JSON字符串
    
    Returns:
        查询类型: aggregate, filter, join, simple
    """
    if not ir_json:
        return "simple"
    
    try:
        ir_data = json.loads(ir_json) if isinstance(ir_json, str) else ir_json
    except json.JSONDecodeError:
        return "simple"
    
    has_measures = bool(ir_data.get("measures"))
    has_filters = bool(ir_data.get("filters"))
    has_group_by = bool(ir_data.get("dimensions") or ir_data.get("group_by"))
    tables = ir_data.get("tables", [])
    has_join = len(tables) > 1 if isinstance(tables, list) else False
    
    if has_join:
        return "join"
    elif has_measures and has_group_by:
        return "aggregate"
    elif has_filters:
        return "filter"
    else:
        return "simple"


async def build_few_shot_entities(
    samples: Iterable,
    embedding_client,
    connection_id: Optional[UUID] = None,
) -> List[dict]:
    """构建Few-Shot实体（稠密 + 稀疏）
    
    Args:
        samples: Few-Shot样本记录列表
        embedding_client: 嵌入客户端
        connection_id: 可选的连接ID，为None时从记录中获取
    """
    entities: List[dict] = []
    for sample in samples:
        # 从记录中获取connection_id或使用传入的参数
        record_conn_id = sample.get("connection_id") or connection_id
        conn_id_str = str(record_conn_id) if record_conn_id else ""
        
        question = sample["question"]
        dense_text = prepare_dense_text(question, RetrievalConfig.dense_text_limit())
        try:
            vector = await embedding_client.embed_single(dense_text or question)
        except Exception as embed_error:
            logger.warning(
                "生成Few-Shot向量失败",
                sample_id=str(sample["sample_id"]),
                error=str(embed_error),
            )
            continue

        metadata = _parse_metadata(sample.get("metadata"))

        tables_payload = sample.get("tables_json") or metadata.get("tables")
        if isinstance(tables_payload, str):
            try:
                tables_data = json.loads(tables_payload) if tables_payload else []
            except json.JSONDecodeError:
                tables_data = sample.get("tables") or []
        else:
            tables_data = tables_payload or sample.get("tables") or []
        tables_for_text = _stringify_tables_for_text(tables_data)

        ir_json_payload = sample.get("ir_json")
        if isinstance(ir_json_payload, str):
            ir_json_text = ir_json_payload
        elif ir_json_payload is not None:
            ir_json_text = json.dumps(ir_json_payload, ensure_ascii=False)
        else:
            ir_json_text = ""

        # sql_context 已由查询处理：COALESCE(metadata->>'sql_context', sql_text)
        sql_context = sample.get("sql_context") or ""
        # error_msg 已由查询处理：metadata->>'error_msg'
        error_msg = sample.get("error_msg") or ""
        # sample_type 已由查询处理：COALESCE(metadata->>'sample_type', 'standard')
        sample_type = sample.get("sample_type") or "standard"
        bm25_text = prepare_bm25_text(
            [
                question,
                " ".join(tables_for_text),
                error_msg,
            ],
            RetrievalConfig.bm25_text_limit(),
        )
        
        # 提取相关字段
        related_fields = _extract_related_fields_from_ir(ir_json_text)
        
        # 推断查询类型
        query_type = _infer_query_type(ir_json_text)
        
        # 构建增强的 json_meta
        few_shot_json_meta = {
            # 原有字段
            "tables": tables_data,
            "source_tag": sample.get("source_tag") or "auto",
            "last_verified_at": (
                sample.get("last_verified_at").isoformat()
                if sample.get("last_verified_at")
                else None
            ),
            
            # 新增字段
            "domain_id": str(sample["domain_id"]) if sample.get("domain_id") else "",
            "domain_name": sample.get("domain_name") or "",
            "related_fields": related_fields,
            "query_type": query_type,
            
            # 质量信息
            "quality_score": float(sample.get("quality_score") or 0.0),
            "is_verified": bool(sample.get("is_verified", False)),
        }

        entities.append(
            {
                "sample_id": str(sample["sample_id"]),
                "connection_id": conn_id_str,
                "domain_id": str(sample["domain_id"]) if sample.get("domain_id") else "",
                "sample_type": sample_type,
                "question": question,
                "ir_json": ir_json_text,
                "sql_context": sql_context,
                "error_msg": error_msg,
                "quality_score": float(sample.get("quality_score") or 0.0),
                "bm25_text": bm25_text,
                "json_meta": few_shot_json_meta,
                "dense_vector": vector,
                # sparse_vector 由 BM25 Function 自动生成，无需手动构建
                "is_active": bool(sample.get("is_active", True)),
            }
        )

    return entities


