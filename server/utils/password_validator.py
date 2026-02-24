"""密码验证工具模块"""

import re
from typing import Tuple


def validate_password_strength(password: str) -> Tuple[bool, str]:
    """
    验证密码强度
    
    要求：
    - 最少8位
    - 需要大写字母、小写字母、数字、特殊字符至少三种组合
    
    Args:
        password: 待验证的密码
        
    Returns:
        Tuple[bool, str]: (是否通过验证, 错误信息)
    """
    if not password:
        return False, "密码不能为空"
    
    # 检查长度
    if len(password) < 8:
        return False, "密码长度至少为8位"
    
    # 检查字符类型
    has_upper = bool(re.search(r'[A-Z]', password))
    has_lower = bool(re.search(r'[a-z]', password))
    has_digit = bool(re.search(r'\d', password))
    has_special = bool(re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]', password))
    
    # 统计包含的字符类型数量
    type_count = sum([has_upper, has_lower, has_digit, has_special])
    
    if type_count < 3:
        missing_types = []
        if not has_upper:
            missing_types.append("大写字母")
        if not has_lower:
            missing_types.append("小写字母")
        if not has_digit:
            missing_types.append("数字")
        if not has_special:
            missing_types.append("特殊字符")
        
        return False, f"密码必须包含大写字母、小写字母、数字、特殊字符中的至少三种（当前缺少：{', '.join(missing_types)}）"
    
    return True, ""

