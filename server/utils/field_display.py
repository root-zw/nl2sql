"""统一的字段显示名映射工具
用于在所有显示位置统一处理字段名到显示名的映射
"""

from typing import Optional
import structlog

logger = structlog.get_logger()


def get_field_display_name(field_id_or_spec, semantic_model=None) -> str:
    """
    将字段ID/物理列名/MetricSpec 映射为显示名称（统一工具函数）
    
    查找顺序：
    0. 保留字（如 __row_count__ 代表记录数）
    1. semantic_model.fields（优先，包含所有字段配置）
    2. semantic_model.dimensions（兼容旧版）
    3. semantic_model.measures（兼容旧版）
    4. semantic_model.metrics（指标）
    5. semantic_model.formatting.display_names（手动配置）
    
    Args:
        field_id_or_spec: 字段ID、物理列名、逻辑字段名，或 MetricSpec 对象/字典
        semantic_model: 语义模型
    
    Returns:
        显示名称（找不到则返回原始field_id）
    """
    if not field_id_or_spec:
        return str(field_id_or_spec) if field_id_or_spec else ""
    
    # 兼容 MetricSpec 格式（dict 或 Pydantic 对象）
    field_id = field_id_or_spec
    explicit_alias = None
    if isinstance(field_id_or_spec, dict):
        field_id = field_id_or_spec.get("field", str(field_id_or_spec))
        explicit_alias = field_id_or_spec.get("alias")
    elif hasattr(field_id_or_spec, "field"):
        # Pydantic MetricSpec 对象
        field_id = field_id_or_spec.field
        explicit_alias = getattr(field_id_or_spec, "alias", None)
    
    # 如果有显式别名，优先使用
    if explicit_alias:
        return explicit_alias
    
    # 0. 处理保留字
    if field_id == "__row_count__":
        return "记录数"
    
    if not semantic_model:
        return field_id
    
    try:
        # 1. 从统一字段表查找（优先）
        if hasattr(semantic_model, 'fields') and semantic_model.fields:
            # 1.1 按field_id查找
            if field_id in semantic_model.fields:
                field = semantic_model.fields[field_id]
                if hasattr(field, 'display_name') and field.display_name:
                    return field.display_name
            
            # 1.2 按physical_column_name查找
            for fid, field in semantic_model.fields.items():
                physical_col = getattr(field, 'physical_column_name', None)
                if physical_col == field_id:
                    if hasattr(field, 'display_name') and field.display_name:
                        return field.display_name
                    break
        
        # 2. 从旧版dimensions查找
        if hasattr(semantic_model, 'dimensions') and field_id in semantic_model.dimensions:
            dim = semantic_model.dimensions[field_id]
            if hasattr(dim, 'display_name') and dim.display_name:
                return dim.display_name
        
        # 3. 从measures查找
        if hasattr(semantic_model, 'measures') and field_id in semantic_model.measures:
            measure = semantic_model.measures[field_id]
            if hasattr(measure, 'display_name') and measure.display_name:
                return measure.display_name
        
        # 4. 从metrics查找
        if hasattr(semantic_model, 'metrics') and field_id in semantic_model.metrics:
            metric = semantic_model.metrics[field_id]
            if hasattr(metric, 'display_name') and metric.display_name:
                return metric.display_name
        
        # 5. 从formatting配置查找
        if hasattr(semantic_model, 'formatting') and semantic_model.formatting:
            if hasattr(semantic_model.formatting, 'display_names') and semantic_model.formatting.display_names:
                if field_id in semantic_model.formatting.display_names:
                    return semantic_model.formatting.display_names[field_id]
    
    except Exception as e:
        logger.warning(f"获取字段显示名失败: field_id={field_id}, error={e}")
    
    # 找不到，返回原始ID
    return field_id


def get_physical_column_name(field_id: str, semantic_model=None) -> str:
    """
    获取字段的物理列名
    
    Args:
        field_id: 字段ID
        semantic_model: 语义模型
    
    Returns:
        物理列名（找不到则返回field_id）
    """
    if not semantic_model or not field_id:
        return field_id
    
    try:
        if hasattr(semantic_model, 'fields') and field_id in semantic_model.fields:
            field = semantic_model.fields[field_id]
            physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None)
            if physical_col:
                return physical_col
    except Exception as e:
        logger.warning(f"获取物理列名失败: field_id={field_id}, error={e}")
    
    return field_id


def build_column_display_map(semantic_model=None) -> dict:
    """
    构建物理列名到显示名的完整映射（用于批量转换）
    
    Returns:
        {physical_column_name: display_name}
    """
    display_map = {}
    
    if not semantic_model:
        return display_map
    
    try:
        # 从fields表构建映射
        if hasattr(semantic_model, 'fields') and semantic_model.fields:
            for field_id, field in semantic_model.fields.items():
                physical_col = getattr(field, 'physical_column_name', None) or getattr(field, 'column', None)
                display_name = getattr(field, 'display_name', None)
                
                if physical_col and display_name:
                    display_map[physical_col] = display_name
                    # 也为field_name建立映射
                    if hasattr(field, 'field_name') and field.field_name != physical_col:
                        display_map[field.field_name] = display_name
        
        # 从formatting配置读取额外映射
        if hasattr(semantic_model, 'formatting') and semantic_model.formatting:
            if hasattr(semantic_model.formatting, 'display_names') and semantic_model.formatting.display_names:
                display_map.update(semantic_model.formatting.display_names)
    
    except Exception as e:
        logger.warning(f"构建字段映射失败: {e}")
    
    return display_map

