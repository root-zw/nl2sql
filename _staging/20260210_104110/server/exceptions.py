"""自定义异常定义"""

from typing import Optional, Dict, Any, List


class NL2SQLError(Exception):
    """基础异常"""
    
    def __init__(
        self,
        message: str,
        code: str = "UNKNOWN_ERROR",
        details: Optional[Dict[str, Any]] = None,
        suggestions: Optional[List[str]] = None
    ):
        self.message = message
        self.code = code
        self.details = details or {}
        self.suggestions = suggestions or []
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "suggestions": self.suggestions
        }


class ModelError(NL2SQLError):
    """语义模型相关错误"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="MODEL_ERROR", **kwargs)


class ModelValidationError(ModelError):
    """模型校验失败"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, **kwargs)
        self.code = "MODEL_VALIDATION_ERROR"


class AmbiguousModelError(ModelError):
    """模型定义存在歧义（如多路径 Join）"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, **kwargs)
        self.code = "AMBIGUOUS_MODEL"


class ParseError(NL2SQLError):
    """NL → IR 解析错误"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="PARSE_ERROR", **kwargs)


class CompilationError(NL2SQLError):
    """IR → SQL 编译错误"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="COMPILATION_ERROR", **kwargs)


class AmbiguousJoinError(CompilationError):
    """Join 路径不唯一"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, **kwargs)
        self.code = "AMBIGUOUS_JOIN"


class ExecutionError(NL2SQLError):
    """SQL 执行错误"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="EXECUTION_ERROR", **kwargs)


class CostExceededError(ExecutionError):
    """查询成本超限"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, **kwargs)
        self.code = "COST_EXCEEDED"


class AuthenticationError(NL2SQLError):
    """认证错误"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="AUTHENTICATION_ERROR", **kwargs)


class AuthorizationError(NL2SQLError):
    """授权错误"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="AUTHORIZATION_ERROR", **kwargs)


class SecurityError(NL2SQLError):
    """SQL安全错误 - 非法操作类型"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="SECURITY_ERROR", **kwargs)
        self.suggestions = [
            "系统只允许执行SELECT查询语句",
            "请检查SQL语句是否包含非法操作",
            "如需数据修改，请联系管理员"
        ]


# ============================================================================
# 多连接相关异常
# ============================================================================

class MultiConnectionError(NL2SQLError):
    """多连接相关错误基类"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, code="MULTI_CONNECTION_ERROR", **kwargs)


class ConnectionDetectionFailed(MultiConnectionError):
    """连接检测失败异常"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, **kwargs)
        self.code = "CONNECTION_DETECTION_FAILED"


class TableNotFoundError(MultiConnectionError):
    """表未找到错误"""
    
    def __init__(self, table_ids: List[str], message: str = None, **kwargs):
        self.table_ids = table_ids
        msg = message or f"以下表在语义模型中不存在: {', '.join(table_ids)}"
        super().__init__(msg, **kwargs)
        self.code = "TABLE_NOT_FOUND"
        self.details["missing_tables"] = table_ids


class CrossConnectionNotSupported(MultiConnectionError):
    """跨连接查询不支持"""
    
    def __init__(self, candidate_connections: Dict[str, List[str]], message: str = None, **kwargs):
        self.candidate_connections = candidate_connections
        msg = message or "查询涉及多个数据库连接，当前版本不支持跨库查询"
        super().__init__(msg, **kwargs)
        self.code = "CROSS_CONNECTION_NOT_SUPPORTED"
        self.details["candidate_connections"] = candidate_connections

