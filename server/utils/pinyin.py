"""
拼音转换工具

用于将中文姓名转换为拼音格式，用作用户名。
"""

import re
from typing import Optional

try:
    import pypinyin
    HAS_PYPINYIN = True
except ImportError:
    HAS_PYPINYIN = False


def chinese_to_pinyin(text: str) -> str:
    """
    将中文文本转换为拼音。
    
    Args:
        text: 中文文本（如姓名）
        
    Returns:
        拼音格式的字符串，小写，无音调
        例如: "张威" -> "zhangwei"
    
    如果 pypinyin 未安装或转换失败，返回原文本
    """
    if not text:
        return text
    
    if not HAS_PYPINYIN:
        # pypinyin 未安装，返回原文本
        return text
    
    try:
        # 将中文转换为拼音，使用 NORMAL 风格（无音调）
        pinyin_list = pypinyin.pinyin(text, style=pypinyin.NORMAL)
        # 拼接所有拼音
        result = ''.join([item[0] for item in pinyin_list])
        return result.lower()
    except Exception:
        return text


def name_to_username(name: str, fallback: Optional[str] = None) -> str:
    """
    将姓名转换为适合作为用户名的格式。
    
    规则:
    1. 如果是纯中文，转换为拼音（小写，无音调）
    2. 如果是英文或混合，保留字母数字，转小写
    3. 移除特殊字符
    
    Args:
        name: 姓名
        fallback: 转换失败时的备用值
        
    Returns:
        适合作为用户名的字符串
    """
    if not name:
        return fallback or ""
    
    # 判断是否包含中文字符
    has_chinese = bool(re.search(r'[\u4e00-\u9fff]', name))
    
    if has_chinese:
        # 包含中文，转换为拼音
        username = chinese_to_pinyin(name)
    else:
        # 纯英文/数字
        username = name
    
    # 清理：只保留字母、数字、下划线
    username = re.sub(r'[^a-zA-Z0-9_]', '', username)
    
    # 转小写
    username = username.lower()
    
    # 确保不为空
    if not username:
        return fallback or name
    
    return username


def is_chinese_name(text: str) -> bool:
    """
    判断是否为中文姓名（纯中文或以中文为主）
    
    Args:
        text: 待检测文本
        
    Returns:
        是否为中文姓名
    """
    if not text:
        return False
    
    # 统计中文字符数量
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
    return chinese_chars > 0 and chinese_chars >= len(text) * 0.5

