"""
管理系统API模块
NL2SQL - 完全重构版
"""

# 导出所有管理API模块
__all__ = [
    'auth',            # 用户认证
    'auto_sync',       # 自动同步管理
    'datasources',     # 数据库连接管理
    'domains',         # 业务域管理
    'fields',          # 字段配置
    'joins',           # 表关系管理
    'rules',           # 全局规则管理
    'milvus',          # Milvus同步
    'cache',           # 缓存管理
    'history',         # 查询历史
    'monitor',         # 系统监控
    'sync',            # 同步
    'tables',          # 数据表配置
    'metadata_io',     # 元数据批量导入导出
    'governance_candidates', # 治理候选
    'model_providers', # 模型供应商管理
    'prompts',         # 提示词模板管理
]
