"""
JSON 序列化工具函数

提供通用的 JSON 序列化处理器，支持 datetime、date、UUID、Decimal 等特殊类型。
用于 WebSocket 消息发送、API 响应等场景。
"""

import json
from datetime import datetime, date, time
from decimal import Decimal
from uuid import UUID
from typing import Any, Dict, Optional


def json_serializer(obj: Any) -> Any:
    """
    通用 JSON 序列化器，处理 Python 中常见的非 JSON 原生类型。
    
    支持的类型：
    - datetime, date, time: 转为 ISO 8601 格式字符串
    - UUID: 转为字符串
    - Decimal: 转为浮点数
    - bytes: 转为 UTF-8 字符串（如失败则转为 hex）
    - set, frozenset: 转为列表
    - 其他对象: 尝试使用 __dict__ 或 str()
    
    Args:
        obj: 需要序列化的对象
        
    Returns:
        可被 JSON 序列化的值
        
    Raises:
        TypeError: 如果对象无法序列化
    """
    # datetime 类型（必须在 date 之前检查，因为 datetime 是 date 的子类）
    if isinstance(obj, datetime):
        return obj.isoformat()
    
    # date 类型
    if isinstance(obj, date):
        return obj.isoformat()
    
    # time 类型
    if isinstance(obj, time):
        return obj.isoformat()
    
    # UUID 类型
    if isinstance(obj, UUID):
        return str(obj)
    
    # Decimal 类型
    if isinstance(obj, Decimal):
        return float(obj)
    
    # bytes 类型
    if isinstance(obj, bytes):
        try:
            return obj.decode('utf-8')
        except UnicodeDecodeError:
            return obj.hex()
    
    # set/frozenset 类型
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    
    # 尝试使用对象的 __dict__
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    
    # 最后尝试转为字符串
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def safe_json_dumps(data: Any, **kwargs) -> str:
    """
    安全的 JSON 序列化，自动处理特殊类型。
    
    Args:
        data: 需要序列化的数据
        **kwargs: 传递给 json.dumps 的其他参数
        
    Returns:
        JSON 字符串
    """
    return json.dumps(data, default=json_serializer, **kwargs)


def safe_json_loads(json_str: str, **kwargs) -> Any:
    """
    安全的 JSON 反序列化。
    
    Args:
        json_str: JSON 字符串
        **kwargs: 传递给 json.loads 的其他参数
        
    Returns:
        解析后的 Python 对象
    """
    return json.loads(json_str, **kwargs)


def sanitize_for_json(data: Any) -> Any:
    """
    递归清理数据，确保所有值都可以被 JSON 序列化。
    
    Args:
        data: 需要清理的数据（字典、列表或其他）
        
    Returns:
        清理后的数据
    """
    if data is None:
        return None
    
    if isinstance(data, dict):
        return {
            key: sanitize_for_json(value)
            for key, value in data.items()
        }
    
    if isinstance(data, (list, tuple)):
        return [sanitize_for_json(item) for item in data]
    
    if isinstance(data, (set, frozenset)):
        return [sanitize_for_json(item) for item in data]
    
    # datetime 必须在 date 之前检查
    if isinstance(data, datetime):
        return data.isoformat()
    
    if isinstance(data, date):
        return data.isoformat()
    
    if isinstance(data, time):
        return data.isoformat()
    
    if isinstance(data, UUID):
        return str(data)
    
    if isinstance(data, Decimal):
        return float(data)
    
    if isinstance(data, bytes):
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return data.hex()
    
    # 原生 JSON 类型直接返回
    if isinstance(data, (str, int, float, bool)):
        return data
    
    # Pydantic 模型
    if hasattr(data, 'model_dump'):
        return sanitize_for_json(data.model_dump())
    
    # 其他对象尝试转为字符串
    return str(data)
