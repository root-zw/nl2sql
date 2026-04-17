"""配置管理模块"""

from pathlib import Path
from typing import List, Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict, field_validator, model_validator
import yaml


def _find_project_root() -> Path:
    """查找项目根目录（包含 config/retrieval_config.yaml 的目录）"""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        # 检查是否有 config/retrieval_config.yaml（更精确的判断）
        retrieval_config = current / "config" / "retrieval_config.yaml"
        if retrieval_config.exists():
            return current
        # .env 文件也是项目根目录的标志
        if (current / ".env").exists():
            return current
        parent = current.parent
        if parent == current:
            break
        current = parent
    return Path(__file__).resolve().parent.parent


def _load_retrieval_config() -> Dict[str, Any]:
    """加载检索配置YAML文件"""
    project_root = _find_project_root()
    config_path = project_root / "config" / "retrieval_config.yaml"
    
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}
    return {}


def _find_env_file() -> str:
    """查找项目根目录的 .env 文件"""
    # 从当前文件位置向上查找项目根目录（包含 .env 文件的目录）
    current = Path(__file__).resolve().parent
    # 向上查找最多 5 层，找到包含 .env 的目录
    for _ in range(5):
        env_file = current / ".env"
        if env_file.exists():
            return str(env_file)
        parent = current.parent
        if parent == current:  # 已到根目录
            break
        current = parent
    # 如果找不到，返回相对路径（保持向后兼容）
    return ".env"


class Settings(BaseSettings):
    """应用配置"""
    
    model_config = ConfigDict(
        env_file=_find_env_file(),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # 忽略未定义的环境变量（如MODELS_DIR）
    )
    
    # === 应用基础配置 ===
    app_env: str = Field(default="development", alias="APP_ENV")
    app_name: str = Field(default="智能问数系统", alias="APP_NAME")
    app_version: str = Field(default="1.0.0", alias="APP_VERSION")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    timezone: str = Field(default="Asia/Shanghai", alias="TIMEZONE")
    server_host: str = Field(default="0.0.0.0", alias="SERVER_HOST")
    server_port: int = Field(default=8891, alias="SERVER_PORT")
    cors_allow_origins: str = Field(default="*", alias="CORS_ALLOW_ORIGINS")
    
    # === 数据库配置 ===
    # 支持两种方式：
    # 1. 直接提供 DSN 字符串（优先使用）
    # 2. 分别提供各个配置项（自动构建 DSN）
    db_dsn: Optional[str] = Field(default=None, alias="DB_DSN")
    
    # 数据库连接参数（当 db_dsn 为空时使用）
    db_host: str = Field(default="localhost", alias="DB_HOST")
    db_port: int = Field(default=1433, alias="DB_PORT")
    db_name: str = Field(default="sde", alias="DB_NAME")
    db_user: str = Field(default="sa", alias="DB_USER")
    db_password: str = Field(default="", alias="DB_PASSWORD")
    db_driver: str = Field(default="ODBC Driver 17 for SQL Server", alias="DB_DRIVER")
    db_encrypt: str = Field(default="no", alias="DB_ENCRYPT")
    db_trust_server_certificate: str = Field(default="yes", alias="DB_TRUST_SERVER_CERTIFICATE")
    db_use_windows_auth: bool = Field(default=False, alias="DB_USE_WINDOWS_AUTH")

    # FreeTDS配置
    freetds_driver_path: str = Field(default="/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so", alias="FREETDS_DRIVER_PATH")
    freetds_version: str = Field(default="7.3", alias="FREETDS_VERSION")
    
    # 连接池配置
    db_pool_size: int = Field(default=10, alias="DB_POOL_SIZE")
    db_pool_max_overflow: int = Field(default=20, alias="DB_POOL_MAX_OVERFLOW")
    db_pool_timeout: int = Field(default=30, alias="DB_POOL_TIMEOUT")
    db_connect_timeout: int = Field(default=30, alias="DB_CONNECT_TIMEOUT")
    db_command_timeout: int = Field(default=60, alias="DB_COMMAND_TIMEOUT")
    db_echo: bool = Field(default=False, alias="DB_ECHO")
    target_db_pool_size: int = Field(default=20, alias="TARGET_DB_POOL_SIZE")
    target_db_pool_max_overflow: int = Field(default=50, alias="TARGET_DB_POOL_MAX_OVERFLOW")
    target_db_pool_timeout: int = Field(default=30, alias="TARGET_DB_POOL_TIMEOUT")
    target_db_pool_recycle: int = Field(default=3600, alias="TARGET_DB_POOL_RECYCLE")
    
    def get_db_dsn(self) -> str:
        """
        获取数据库 DSN 连接字符串
        如果提供了 db_dsn，直接使用；否则根据各个参数构建
        """
        # 如果直接提供了 DSN，优先使用
        if self.db_dsn:
            return self.db_dsn
        
        # 否则根据各个参数构建 DSN
        from urllib.parse import quote_plus
        
        if self.db_use_windows_auth:
            # Windows 认证 - FreeTDS 不支持 Windows 认证，使用原始驱动
            dsn = (
                f"mssql+pyodbc://{self.db_host}:{self.db_port}/{self.db_name}"
                f"?driver={self.db_driver.replace(' ', '+')}"
                f"&Trusted_Connection=yes"
            )
            # 添加加密相关参数
            if self.db_encrypt.lower() in ['yes', 'true', '1']:
                dsn += "&Encrypt=yes"
            else:
                dsn += "&Encrypt=no"
            
            if self.db_trust_server_certificate.lower() in ['yes', 'true', '1']:
                dsn += "&TrustServerCertificate=yes"
        else:
            # SQL Server 认证 - 使用 FreeTDS 驱动（libtdsodbc.so）避免 SSL 兼容性问题
            # 转义用户名和密码中的特殊字符
            def escape_odbc_value(value: str) -> str:
                """转义 ODBC 连接字符串中的值"""
                if not value:
                    return value
                if any(char in value for char in [';', '=', '{', '}', ']']):
                    escaped = value.replace('}', '}}').replace('{', '{{')
                    return f"{{{escaped}}}"
                return value
            
            escaped_username = escape_odbc_value(self.db_user)
            escaped_password = escape_odbc_value(self.db_password)
            escaped_database = escape_odbc_value(self.db_name)
            
            odbc_connect = (
                f"Driver={self.freetds_driver_path};"
                f"Server={self.db_host},{self.db_port};"
                f"Database={escaped_database};"
                f"UID={escaped_username};"
                f"PWD={escaped_password};"
                f"TDS_Version={self.freetds_version};"
                f"Encrypt=no;"
                f"ClientCharset=UTF-8;"
            )
            quoted_odbc_connect = quote_plus(odbc_connect)
            dsn = f"mssql+pyodbc:///?odbc_connect={quoted_odbc_connect}"
        
        return dsn
    
    # === Redis 配置 ===
    redis_url: str = Field(alias="REDIS_URL")
    redis_password: Optional[str] = Field(default=None, alias="REDIS_PASSWORD")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    redis_max_connections: int = Field(default=50, alias="REDIS_MAX_CONNECTIONS")

    # 自动同步系统专用Redis配置
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_enabled: bool = Field(default=True, alias="REDIS_ENABLED")

    # === 超时与重试配置 ===
    sync_default_timeout_seconds: int = Field(default=300, alias="SYNC_DEFAULT_TIMEOUT_SECONDS")
    sync_max_retry_attempts: int = Field(default=3, alias="SYNC_MAX_RETRY_ATTEMPTS")
    sync_retry_delay_base: int = Field(default=5, alias="SYNC_RETRY_DELAY_BASE")
    sync_notification_queue_timeout: float = Field(default=1.0, alias="SYNC_NOTIFICATION_QUEUE_TIMEOUT")
    sync_lock_timeout_seconds: int = Field(default=300, alias="SYNC_LOCK_TIMEOUT_SECONDS")
    sync_max_concurrent: int = Field(default=3, alias="SYNC_MAX_CONCURRENT")
    sync_queue_key_prefix: str = Field(default="milvus_sync_queue", alias="SYNC_QUEUE_KEY_PREFIX")
    sync_lock_key_prefix: str = Field(default="milvus_lock", alias="SYNC_LOCK_KEY_PREFIX")
    sync_status_key_prefix: str = Field(default="milvus_status", alias="SYNC_STATUS_KEY_PREFIX")
    db_inspector_timeout: int = Field(default=5, alias="DB_INSPECTOR_TIMEOUT")
    metadata_db_command_timeout: int = Field(default=60, alias="METADATA_DB_COMMAND_TIMEOUT")
    
    # === LLM 配置 ===
    # 默认配置（全局回退）
    llm_provider: str = Field(default="custom", alias="LLM_PROVIDER")
    # OpenAI 兼容接口改名为 NL2SQL_*，便于独立管理
    nl2sql_base_url: Optional[str] = Field(default=None, alias="NL2SQL_BASE_URL")
    nl2sql_api_key: str = Field(alias="NL2SQL_API_KEY")
    llm_model: str = Field(alias="LLM_MODEL")
    llm_timeout: int = Field(default=60, alias="LLM_TIMEOUT")
    llm_max_retries: int = Field(default=2, alias="LLM_MAX_RETRIES")
    # 编译/执行失败后的“自动回馈重试”次数（用于系统性修复 IR → SQL 失败场景，默认 1 次即可）
    llm_post_compile_retries: int = Field(default=1, alias="LLM_POST_COMPILE_RETRIES")
    llm_use_tools: bool = Field(default=True, alias="LLM_USE_TOOLS")
    llm_use_json_mode: bool = Field(default=False, alias="LLM_USE_JSON_MODE")
    
    # LLM 额外参数（可选）
    llm_temperature: float = Field(default=0.0, alias="LLM_TEMPERATURE")
    llm_enable_thinking: Optional[bool] = Field(default=None, alias="LLM_ENABLE_THINKING")
    llm_max_tokens: Optional[int] = Field(default=None, alias="LLM_MAX_TOKENS")
    llm_top_p: Optional[float] = Field(default=None, alias="LLM_TOP_P")
    
    # === LLM 多场景配置 ===
    # 每个场景可单独指定模型，未配置时回退到默认配置
    
    # 表选择场景（LLM_TABLE_SELECTION_*）
    llm_table_selection_base_url: Optional[str] = Field(default=None, alias="LLM_TABLE_SELECTION_BASE_URL")
    llm_table_selection_api_key: Optional[str] = Field(default=None, alias="LLM_TABLE_SELECTION_API_KEY")
    llm_table_selection_model: Optional[str] = Field(default=None, alias="LLM_TABLE_SELECTION_MODEL")
    llm_table_selection_temperature: Optional[float] = Field(default=None, alias="LLM_TABLE_SELECTION_TEMPERATURE")
    llm_table_selection_max_tokens: Optional[int] = Field(default=2048, alias="LLM_TABLE_SELECTION_MAX_TOKENS")  # 表选择场景需要足够token避免JSON截断
    llm_table_selection_timeout: Optional[int] = Field(default=None, alias="LLM_TABLE_SELECTION_TIMEOUT")
    
    # NL2IR 解析场景（LLM_NL2IR_*）
    llm_nl2ir_base_url: Optional[str] = Field(default=None, alias="LLM_NL2IR_BASE_URL")
    llm_nl2ir_api_key: Optional[str] = Field(default=None, alias="LLM_NL2IR_API_KEY")
    llm_nl2ir_model: Optional[str] = Field(default=None, alias="LLM_NL2IR_MODEL")
    llm_nl2ir_temperature: Optional[float] = Field(default=None, alias="LLM_NL2IR_TEMPERATURE")
    llm_nl2ir_max_tokens: Optional[int] = Field(default=None, alias="LLM_NL2IR_MAX_TOKENS")
    llm_nl2ir_timeout: Optional[int] = Field(default=None, alias="LLM_NL2IR_TIMEOUT")
    
    # 叙述生成场景（LLM_NARRATIVE_*）
    llm_narrative_base_url: Optional[str] = Field(default=None, alias="LLM_NARRATIVE_BASE_URL")
    llm_narrative_api_key: Optional[str] = Field(default=None, alias="LLM_NARRATIVE_API_KEY")
    llm_narrative_model: Optional[str] = Field(default=None, alias="LLM_NARRATIVE_MODEL")
    # 注意：llm_narrative_temperature 使用 narrative_temperature（已存在）
    llm_narrative_max_tokens: Optional[int] = Field(default=None, alias="LLM_NARRATIVE_MAX_TOKENS")
    llm_narrative_timeout: Optional[int] = Field(default=None, alias="LLM_NARRATIVE_TIMEOUT")
    
    # === 查询控制 ===
    query_timeout_seconds: int = Field(default=120, alias="QUERY_TIMEOUT_SECONDS")
    query_default_limit: int = Field(default=1000, alias="QUERY_DEFAULT_LIMIT")
    query_max_limit: int = Field(default=10000, alias="QUERY_MAX_LIMIT")
    
    # === 混合架构配置 ===
    # 主开关：启用混合架构（IR + 直接SQL）
    hybrid_architecture_enabled: bool = Field(
        default=True, 
        alias="HYBRID_ARCHITECTURE_ENABLED",
        description="是否启用混合架构，支持IR流程和直接SQL生成流程"
    )
    
    # 直接SQL开关：允许LLM直接生成SQL（当IR标记 requires_direct_sql=true 时使用）
    direct_sql_enabled: bool = Field(
        default=True,
        alias="DIRECT_SQL_ENABLED",
        description="是否允许直接SQL生成（用于IR无法表达的复杂查询）"
    )
    
    # 直接SQL场景的LLM配置
    llm_direct_sql_base_url: Optional[str] = Field(default=None, alias="LLM_DIRECT_SQL_BASE_URL")
    llm_direct_sql_api_key: Optional[str] = Field(default=None, alias="LLM_DIRECT_SQL_API_KEY")
    llm_direct_sql_model: Optional[str] = Field(default=None, alias="LLM_DIRECT_SQL_MODEL")
    llm_direct_sql_temperature: Optional[float] = Field(default=0.0, alias="LLM_DIRECT_SQL_TEMPERATURE")
    llm_direct_sql_max_tokens: Optional[int] = Field(default=4096, alias="LLM_DIRECT_SQL_MAX_TOKENS")
    llm_direct_sql_timeout: Optional[int] = Field(default=60, alias="LLM_DIRECT_SQL_TIMEOUT")
    
    # SQL后处理安全配置
    sql_post_processor_enabled: bool = Field(
        default=True,
        alias="SQL_POST_PROCESSOR_ENABLED",
        description="是否对直接生成的SQL进行后处理（安全检查、权限注入）"
    )
    sql_post_processor_skip_table_validation: bool = Field(
        default=False,
        alias="SQL_POST_PROCESSOR_SKIP_TABLE_VALIDATION",
        description="是否跳过表白名单验证（仅开发模式使用）"
    )
    sql_post_processor_inject_filters: bool = Field(
        default=True,
        alias="SQL_POST_PROCESSOR_INJECT_FILTERS",
        description="是否在直接SQL中注入权限过滤和默认过滤"
    )
    showplan_enabled: bool = Field(default=True, alias="SHOWPLAN_ENABLED")
    showplan_rows_max: int = Field(default=50_000_000, alias="SHOWPLAN_ROWS_MAX")
    showplan_cost_max: float = Field(default=5.0, alias="SHOWPLAN_COST_MAX")
    sampling_threshold_rows: int = Field(default=100_000_000, alias="SAMPLING_THRESHOLD_ROWS")
    enhanced_validation_enabled: bool = Field(default=True, alias="ENHANCED_VALIDATION_ENABLED")
    dry_run_mandatory: bool = Field(default=True, alias="DRY_RUN_MANDATORY")
    complex_dag_max_concurrency: int = Field(default=0, alias="COMPLEX_DAG_MAX_CONCURRENCY")
    complex_dag_fallback_enabled: bool = Field(default=True, alias="COMPLEX_DAG_FALLBACK_ENABLED")
    smart_context_value_threshold: int = Field(default=100, alias="SMART_CONTEXT_VALUE_THRESHOLD")
    smart_context_value_preview_limit: int = Field(default=5, alias="SMART_CONTEXT_VALUE_PREVIEW_LIMIT")
    smart_context_subquery_distinct: bool = Field(default=True, alias="SMART_CONTEXT_SUBQUERY_DISTINCT")
    smart_context_subquery_alias: str = Field(default="ctx_subquery", alias="SMART_CONTEXT_SUBQUERY_ALIAS")
    
    # === 权限控制 ===
    rls_enabled: bool = Field(default=True, alias="RLS_ENABLED")
    rls_bypass_roles: str = Field(default="sysadmin,data_admin", alias="RLS_BYPASS_ROLES")
    column_masking_enabled: bool = Field(default=False, alias="COLUMN_MASKING_ENABLED")
    
    # === IR 验证与调试 ===
    # 是否启用 IR 严格模式（开启后任何修复操作都会报错，而非自动处理）
    ir_strict_mode: bool = Field(default=False, alias="IR_STRICT_MODE")
    
    # === 多轮对话会话管理配置 ===
    # 多轮对话上下文深度（包含多少轮历史对话作为上下文，0=不使用上下文）
    conversation_context_depth: int = Field(default=5, alias="CONVERSATION_CONTEXT_DEPTH", ge=0, le=20)
    # 会话标题最大长度（自动从第一个问题截取）
    conversation_title_max_length: int = Field(default=50, alias="CONVERSATION_TITLE_MAX_LENGTH", ge=10, le=200)
    # 每个用户最大会话数量（0=不限制，超过时自动删除最旧的非置顶会话）
    conversation_max_per_user: int = Field(default=100, alias="CONVERSATION_MAX_PER_USER", ge=0)
    # 会话消息中是否保存完整的查询结果数据（true=保存完整数据，false=仅保存摘要）
    conversation_save_full_result: bool = Field(default=True, alias="CONVERSATION_SAVE_FULL_RESULT")
    # 查询结果最大保存行数（仅当 conversation_save_full_result=true 时有效）
    conversation_max_result_rows: int = Field(default=500, alias="CONVERSATION_MAX_RESULT_ROWS", ge=0)
    # 查询取消等待超时（秒）
    query_cancel_timeout_seconds: int = Field(default=10, alias="QUERY_CANCEL_TIMEOUT_SECONDS", ge=1, le=60)
    
    # === 缓存配置 ===
    cache_enabled: bool = Field(default=True, alias="CACHE_ENABLED")
    cache_ttl_seconds: int = Field(default=600, alias="CACHE_TTL_SECONDS")
    cache_key_prefix: str = Field(default="NL2SQL:", alias="CACHE_KEY_PREFIX")
    
    # === 确认卡触发策略 ===
    confirmation_mode: str = Field(default="always_confirm", alias="CONFIRMATION_MODE")
    enable_complex_query_auto_execution: bool = Field(default=True, alias="ENABLE_COMPLEX_QUERY_AUTO_EXECUTION")
    confirm_time_span_days: int = Field(default=90, alias="CONFIRM_TIME_SPAN_DAYS")
    confirm_sensitive_metrics: str = Field(default="revenue,profit,salary", alias="CONFIRM_SENSITIVE_METRICS")
    confirm_low_confidence_threshold: float = Field(default=0.7, alias="CONFIRM_LOW_CONFIDENCE_THRESHOLD")
    
    # === 观测与审计 ===
    otel_enabled: bool = Field(default=False, alias="OTEL_ENABLED")
    otel_endpoint: Optional[str] = Field(default=None, alias="OTEL_ENDPOINT")
    audit_log_dir: str = Field(default="./logs/audit", alias="AUDIT_LOG_DIR")
    audit_log_retention_days: int = Field(default=90, alias="AUDIT_LOG_RETENTION_DAYS")
    monitor_online_window_minutes: int = Field(default=15, alias="MONITOR_ONLINE_WINDOW_MINUTES")
    monitor_query_preview_limit: int = Field(default=50, alias="MONITOR_QUERY_PREVIEW_LIMIT")

    # === 叙述生成功能（可开关） ===
    narrative_enabled: bool = Field(default=False, alias="NARRATIVE_ENABLED")
    narrative_prompt_path: str = Field(default="prompts/narrative/prompt.txt", alias="NARRATIVE_PROMPT_PATH")
    narrative_temperature: float = Field(default=0.0, alias="NARRATIVE_TEMPERATURE")
    # 同步生成开关与超时（秒）。>0 表示在返回前等待至多N秒生成 summary
    narrative_sync_timeout_seconds: int = Field(default=3, alias="NARRATIVE_SYNC_TIMEOUT_SECONDS")
    # 叙述示例行数量（None/-1=全部，0=禁用，>0=前N行）
    narrative_sample_rows: Optional[int] = Field(default=None, alias="NARRATIVE_SAMPLE_ROWS")
    narrative_max_text_length: int = Field(default=1200, alias="NARRATIVE_MAX_TEXT_LENGTH")
    
    # === 向量检索配置（可选）===
    # Milvus配置
    milvus_enabled: bool = Field(default=False, alias="MILVUS_ENABLED")
    milvus_uri: str = Field(default="./milvus_demo.db", alias="MILVUS_URI")  # 本地文件或 http://localhost:19530
    milvus_token: Optional[str] = Field(default=None, alias="MILVUS_TOKEN")  # Zilliz Cloud需要
    milvus_collection: str = Field(default="semantic_metadata", alias="MILVUS_COLLECTION")
    milvus_enum_collection: str = Field(default="enum_values_dual", alias="MILVUS_ENUM_COLLECTION")
    milvus_few_shot_collection: str = Field(default="qa_few_shot_samples", alias="MILVUS_FEW_SHOT_COLLECTION")
    milvus_pool_min_size: int = Field(default=1, alias="MILVUS_POOL_MIN_SIZE")
    milvus_pool_max_size: int = Field(default=2, alias="MILVUS_POOL_MAX_SIZE")
    
    # Milvus 中文 Analyzer 配置
    # 是否使用自定义中文 analyzer（启用后可配置停用词和过滤器）
    # false: 使用内置的 "chinese" analyzer（不支持自定义参数）
    # true: 使用自定义 analyzer（支持停用词、removepunct 等配置）
    milvus_use_custom_chinese_analyzer: bool = Field(default=True, alias="MILVUS_USE_CUSTOM_CHINESE_ANALYZER")
    # 中文停用词列表（逗号分隔，仅在启用自定义 analyzer 时生效）
    milvus_chinese_stopwords: str = Field(
        default=(
            "的,了,在,是,我,有,和,就,不,人,都,一,一个,上,也,很,到,说,要,去,你,会,着,没有,看,好,自己,这,"
            "我们,你们,他们,她们,这个,那个,这些,那些,哪里,哪个,怎么,为什么,可能,可以,应该,非常,以及,并且,"
            "但是,如果,因为,所以,还是,就是,那么,这么,这样,那样,吗,呢,啊,哦,唉,的话,已经,正在,仍然,而且,"
            "与,及,由于,通过,其中,以上,以下,每个,一些"
        ),
        alias="MILVUS_CHINESE_STOPWORDS"
    )
    # 是否移除标点符号（仅在启用自定义 analyzer 时生效）
    milvus_analyzer_remove_punct: bool = Field(default=True, alias="MILVUS_ANALYZER_REMOVE_PUNCT")
    
    # Embedding API配置（支持 Ollama 和 vLLM/OpenAI 兼容接口）
    # 显式指定 Embedding API 风格：ollama | openai（vLLM/OpenAI兼容）| auto（默认，尽量从 URL 推断）
    embedding_api_style: str = Field(default="auto", alias="EMBEDDING_API_STYLE")
    embedding_base_url: Optional[str] = Field(default=None, alias="EMBEDDING_BASE_URL")
    embedding_api_key: Optional[str] = Field(default=None, alias="EMBEDDING_API_KEY")
    embedding_model: str = Field(default="text-embedding-3-small", alias="EMBEDDING_MODEL")
    embedding_timeout: int = Field(default=30, alias="EMBEDDING_TIMEOUT")
    embedding_batch_size: int = Field(default=16, alias="EMBEDDING_BATCH_SIZE", ge=1, le=128)
    embedding_max_concurrent: int = Field(default=10, alias="EMBEDDING_MAX_CONCURRENT", ge=1, le=50)
    
    # 向量维度（根据模型自动推断，或手动指定）
    # text-embedding-3-small: 1536
    # text-embedding-3-large: 3072
    # bge-small-zh-v1.5: 512
    # bge-large-zh-v1.5: 1024
    embedding_dim: int = Field(default=1536, alias="EMBEDDING_DIM")
    
    # === 检索配置（层次化检索）===
    # 业务域向量检索阈值（相似度低于此值时降级到关键词匹配）
    domain_vector_threshold: float = Field(default=0.4, alias="DOMAIN_VECTOR_THRESHOLD", ge=0.0, le=1.0)

    # 表级向量检索阈值
    table_vector_threshold: float = Field(default=0.3, alias="TABLE_VECTOR_THRESHOLD", ge=0.0, le=1.0)
    hybrid_value_boost: float = Field(
        default=3.0, alias="HYBRID_VALUE_BOOST", ge=1.0, le=3.0
    )
    enum_cardinality_threshold: int = Field(
        default=5000, alias="ENUM_CARDINALITY_THRESHOLD", ge=1
    )
    enum_exact_rrf_boost: float = Field(
        default=0.6,
        alias="ENUM_EXACT_RRF_BOOST",
        ge=0.0,
        le=2.0,
    )
    enum_synonym_rrf_boost: float = Field(
        default=0.4,
        alias="ENUM_SYNONYM_RRF_BOOST",
        ge=0.0,
        le=2.0,
    )
    enum_min_final_score: float = Field(
        default=0.0,
        alias="ENUM_MIN_FINAL_SCORE",
        ge=0.0,
        le=1.0,
    )
    bm25_text_limit: int = Field(default=512, alias="BM25_TEXT_LIMIT", ge=128)
    dense_text_limit: int = Field(default=1024, alias="DENSE_TEXT_LIMIT", ge=128)
    # Reranker 配置（vLLM /v1/rerank 接口）
    reranker_endpoint: str = Field(
        default="", alias="RERANKER_ENDPOINT"
    )
    reranker_model: str = Field(
        default="bge-reranker-v2-m3", alias="RERANKER_MODEL"
    )
    reranker_api_key: Optional[str] = Field(
        default=None, alias="RERANKER_API_KEY"
    )
    reranker_weight: float = Field(
        default=0.3,
        alias="RERANKER_WEIGHT",
        ge=0.0,
        le=1.0,
    )
    reranker_timeout: int = Field(default=30, alias="RERANKER_TIMEOUT", ge=5, le=120)
    reranker_max_concurrent: int = Field(default=5, alias="RERANKER_MAX_CONCURRENT", ge=1, le=20)

    # LLM 表选择配置
    llm_table_selection_enabled: bool = Field(
        default=True, alias="LLM_TABLE_SELECTION_ENABLED"
    )
    llm_table_selection_high_confidence: float = Field(
        default=0.85, alias="LLM_TABLE_SELECTION_HIGH_CONFIDENCE", ge=0.0, le=1.0
    )
    llm_table_selection_medium_confidence: float = Field(
        default=0.5, alias="LLM_TABLE_SELECTION_MEDIUM_CONFIDENCE", ge=0.0, le=1.0
    )
    llm_table_selection_min_gap: float = Field(
        default=0.3, alias="LLM_TABLE_SELECTION_MIN_GAP", ge=0.0, le=1.0
    )
    llm_table_selection_max_candidates: int = Field(
        default=3, alias="LLM_TABLE_SELECTION_MAX_CANDIDATES", ge=1, le=10
    )
    # 总候选表数量（用于"换一批"和"查看更多"功能）
    llm_table_selection_total_candidates: int = Field(
        default=10, alias="LLM_TABLE_SELECTION_TOTAL_CANDIDATES", ge=3, le=20
    )
    # 每页展示数量
    llm_table_selection_page_size: int = Field(
        default=3, alias="LLM_TABLE_SELECTION_PAGE_SIZE", ge=2, le=10
    )
    # LLM 模式下每个字段最多传递的枚举值数量（让 LLM 自己判断用哪个）
    llm_table_selection_enum_per_field: int = Field(
        default=5, alias="LLM_TABLE_SELECTION_ENUM_PER_FIELD", ge=1, le=20
    )
    # 跨年查询自动执行的置信度阈值（当 recommended_table_ids 中所有表的置信度都 >= 此值时，直接执行无需确认）
    llm_table_selection_cross_year_confidence: float = Field(
        default=0.75, alias="LLM_TABLE_SELECTION_CROSS_YEAR_CONFIDENCE", ge=0.0, le=1.0
    )
    
    # === 向量表选择配置 ===
    # 向量选表的业务参数已统一移至 config/retrieval_config.yaml 的 vector_table_selection 配置块
    # 此处仅保留 LLM3 模型的 API 配置
    
    # LLM3（向量表选择器）模型配置（独立于LLM1和LLM2）
    llm_vector_selector_base_url: Optional[str] = Field(default=None, alias="LLM_VECTOR_SELECTOR_BASE_URL")
    llm_vector_selector_api_key: Optional[str] = Field(default=None, alias="LLM_VECTOR_SELECTOR_API_KEY")
    llm_vector_selector_model: Optional[str] = Field(default=None, alias="LLM_VECTOR_SELECTOR_MODEL")
    llm_vector_selector_temperature: Optional[float] = Field(default=0.1, alias="LLM_VECTOR_SELECTOR_TEMPERATURE")
    llm_vector_selector_max_tokens: Optional[int] = Field(default=4096, alias="LLM_VECTOR_SELECTOR_MAX_TOKENS")
    llm_vector_selector_timeout: Optional[int] = Field(default=60, alias="LLM_VECTOR_SELECTOR_TIMEOUT")
    
    # 表召回配置
    # 默认召回表数量上限
    default_top_k: int = Field(default=3, alias="DEFAULT_TOP_K", ge=1, le=10)
    few_shot_enabled: bool = Field(default=True, alias="FEW_SHOT_ENABLED")
    few_shot_top_k: int = Field(default=6, alias="FEW_SHOT_TOP_K", ge=1, le=12)
    few_shot_similarity_threshold: float = Field(
        default=0.3, alias="FEW_SHOT_SIMILARITY_THRESHOLD", ge=0.0, le=1.0
    )
    few_shot_min_quality_score: float = Field(
        default=0.7, alias="FEW_SHOT_MIN_QUALITY_SCORE", ge=0.0, le=1.0
    )
    few_shot_prompt_section_title: str = Field(
        default="【参考示例】", alias="FEW_SHOT_PROMPT_SECTION_TITLE"
    )
    few_shot_immediate_sync: bool = Field(
        default=False, alias="FEW_SHOT_IMMEDIATE_SYNC"
    )
    few_shot_prompt_max_examples: int = Field(
        default=3, alias="FEW_SHOT_PROMPT_MAX_EXAMPLES", ge=1, le=8
    )
    few_shot_normalize_question: bool = Field(
        default=True, alias="FEW_SHOT_NORMALIZE_QUESTION"
    )
    few_shot_dense_priority: int = Field(
        default=2, alias="FEW_SHOT_DENSE_PRIORITY", ge=1, le=5
    )
    few_shot_direct_execution_enabled: bool = Field(
        default=False, alias="FEW_SHOT_DIRECT_EXECUTION_ENABLED"
    )
    few_shot_direct_min_similarity: float = Field(
        default=0.95, alias="FEW_SHOT_DIRECT_MIN_SIMILARITY", ge=0.0, le=1.0
    )
    few_shot_direct_min_quality: float = Field(
        default=0.9, alias="FEW_SHOT_DIRECT_MIN_QUALITY", ge=0.0, le=1.0
    )
    few_shot_direct_source_whitelist: List[str] = Field(
        default_factory=lambda: ["manual", "auto"],
        alias="FEW_SHOT_DIRECT_SOURCE_WHITELIST",
    )

    @field_validator("few_shot_direct_source_whitelist", mode="before")
    @classmethod
    def _split_direct_source_whitelist(cls, value):
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value
    
    # === 元数据库配置（架构切换）===
    use_metadata_db: bool = Field(default=True, alias="USE_METADATA_DB")
    metadata_cache_ttl: int = Field(default=1800, alias="METADATA_CACHE_TTL")  # 30分钟
    metadata_db_pool_min_size: int = Field(default=10, alias="METADATA_DB_POOL_MIN_SIZE")
    metadata_db_pool_max_size: int = Field(default=100, alias="METADATA_DB_POOL_MAX_SIZE")
    metadata_db_max_queries: int = Field(default=50000, alias="METADATA_DB_MAX_QUERIES")
    metadata_db_max_inactive_lifetime: int = Field(default=300, alias="METADATA_DB_MAX_INACTIVE_LIFETIME")
    
    # 🟢: PostgreSQL元数据库配置（用于管理系统）
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    # NOTE: defaults should match env.template to avoid "silent" mismatches when env vars are missing.
    postgres_db: str = Field(default="text2sql_metadata", alias="POSTGRES_DB")
    postgres_user: str = Field(default="text2sql_user", alias="POSTGRES_USER")
    # 安全：生产环境必须显式提供；开发环境可在 .env 中提供
    postgres_password: str = Field(default="", alias="POSTGRES_PASSWORD")
    
    @property
    def metadata_db_url(self) -> str:
        """自动生成元数据库连接URL"""
        from urllib.parse import quote_plus
        user = quote_plus(self.postgres_user)
        password = quote_plus(self.postgres_password)
        return f"postgresql://{user}:{password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    # ===安全配置 ===
    # 安全：生产环境必须显式提供（禁止使用代码默认值）
    jwt_secret: str = Field(default="", alias="JWT_SECRET")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    jwt_expire_minutes: int = Field(default=30, alias="JWT_EXPIRE_MINUTES")  # Access Token 30分钟
    jwt_refresh_expire_days: int = Field(default=7, alias="JWT_REFRESH_EXPIRE_DAYS")  # Refresh Token 7天
    # 安全：生产环境必须显式提供（禁止使用代码默认值）
    encryption_key: str = Field(default="", alias="ENCRYPTION_KEY")

    # === 认证配置 ===
    # 是否启用登录验证码校验（生产建议保持 true；自动化测试可设置为 false）
    auth_captcha_enabled: bool = Field(default=True, alias="AUTH_CAPTCHA_ENABLED")

    # 前端 URL（用于 OIDC 回调重定向等场景）
    frontend_url: str = Field(default="", alias="FRONTEND_URL", description="前端基础URL，如 http://localhost:5173")
    
    auth_mode: str = Field(default="local", alias="AUTH_MODE", description="local/oidc/api_gateway/ldap/chain")
    auth_providers: str = Field(default="local", alias="AUTH_PROVIDERS", description="逗号分隔的 provider 列表")
    oidc_enabled: bool = Field(default=False, alias="OIDC_ENABLED")
    oidc_issuer_url: str = Field(default="", alias="OIDC_ISSUER_URL")
    oidc_client_id: str = Field(default="", alias="OIDC_CLIENT_ID")
    oidc_client_secret: str = Field(default="", alias="OIDC_CLIENT_SECRET")
    oidc_redirect_uri: str = Field(default="", alias="OIDC_REDIRECT_URI")
    oidc_scope: str = Field(default="openid profile email", alias="OIDC_SCOPE")
    oidc_role_mapping: str = Field(default="{}", alias="OIDC_ROLE_MAPPING")
    # 数据角色同步配置
    oidc_data_role_claim: str = Field(default="roles", alias="OIDC_DATA_ROLE_CLAIM")
    oidc_auto_create_data_role: bool = Field(default=True, alias="OIDC_AUTO_CREATE_DATA_ROLE")
    oidc_user_attribute_claims: str = Field(default="department,region", alias="OIDC_USER_ATTRIBUTE_CLAIMS")
    
    api_gateway_enabled: bool = Field(default=False, alias="API_GATEWAY_ENABLED")
    gateway_signature_secret: str = Field(default="", alias="GATEWAY_SIGNATURE_SECRET")
    trusted_gateway_ips: str = Field(default="", alias="TRUSTED_GATEWAY_IPS")
    api_gateway_auto_create_role: bool = Field(default=True, alias="API_GATEWAY_AUTO_CREATE_ROLE")
    
    ldap_enabled: bool = Field(default=False, alias="LDAP_ENABLED")
    ldap_server: str = Field(default="", alias="LDAP_SERVER")
    ldap_base_dn: str = Field(default="", alias="LDAP_BASE_DN")
    ldap_bind_dn: str = Field(default="", alias="LDAP_BIND_DN")
    ldap_bind_password: str = Field(default="", alias="LDAP_BIND_PASSWORD")
    
    @property
    def oidc_user_attribute_claims_list(self) -> List[str]:
        """获取需要同步的用户属性字段列表"""
        return [a.strip() for a in self.oidc_user_attribute_claims.split(",") if a.strip()]
    
    # ===同步任务配置 ===
    immediate_sync_on_field_update: bool = Field(default=False, alias="IMMEDIATE_SYNC_ON_FIELD_UPDATE")
    immediate_sync_on_table_update: bool = Field(default=False, alias="IMMEDIATE_SYNC_ON_TABLE_UPDATE")
    few_shot_sync_in_full_sync: bool = Field(default=True, alias="FEW_SHOT_SYNC_IN_FULL_SYNC")
    sync_batch_window_seconds: int = Field(default=5, alias="SYNC_BATCH_WINDOW_SECONDS")
    sync_full_timeout_seconds: int = Field(default=900, alias="SYNC_FULL_TIMEOUT_SECONDS")
    sync_milvus_batch_size: int = Field(default=100, alias="SYNC_MILVUS_BATCH_SIZE")
    auto_sync_enabled: bool = Field(default=True, alias="AUTO_SYNC_ENABLED")
    auto_sync_mode: str = Field(default="auto", alias="AUTO_SYNC_MODE")
    auto_sync_domains: bool = Field(default=True, alias="AUTO_SYNC_DOMAINS")
    auto_sync_tables: bool = Field(default=True, alias="AUTO_SYNC_TABLES")
    auto_sync_fields: bool = Field(default=True, alias="AUTO_SYNC_FIELDS")
    auto_sync_enums: bool = Field(default=True, alias="AUTO_SYNC_ENUMS")
    auto_sync_few_shot: bool = Field(default=True, alias="AUTO_SYNC_FEW_SHOT")

    # === 数据限制配置 ===
    explainer_max_rows: int = Field(default=1000, alias="EXPLAINER_MAX_ROWS")
    max_enum_values_in_prompt: int = Field(default=5, alias="MAX_ENUM_VALUES_IN_PROMPT")

    # === 文件与模板配置 ===
    nl2ir_prompts_dir: str = Field(default="prompts/nl2ir", alias="NL2IR_PROMPTS_DIR")
    nl2ir_system_prompt_file: Optional[str] = Field(default=None, alias="NL2IR_SYSTEM_PROMPT_FILE")
    nl2ir_function_schema_file: Optional[str] = Field(default=None, alias="NL2IR_FUNCTION_SCHEMA_FILE")
    table_selector_prompts_dir: str = Field(default="prompts/table_selector", alias="TABLE_SELECTOR_PROMPTS_DIR")
    table_selector_system_prompt_file: Optional[str] = Field(default=None, alias="TABLE_SELECTOR_SYSTEM_PROMPT_FILE")
    table_selector_function_schema_file: Optional[str] = Field(default=None, alias="TABLE_SELECTOR_FUNCTION_SCHEMA_FILE")
    table_selector_user_template_file: Optional[str] = Field(default=None, alias="TABLE_SELECTOR_USER_TEMPLATE_FILE")
    vector_table_selector_prompts_dir: str = Field(
        default="prompts/vector_table_selector", alias="VECTOR_TABLE_SELECTOR_PROMPTS_DIR"
    )
    vector_table_selector_system_prompt_file: Optional[str] = Field(
        default=None, alias="VECTOR_TABLE_SELECTOR_SYSTEM_PROMPT_FILE"
    )
    vector_table_selector_function_schema_file: Optional[str] = Field(
        default=None, alias="VECTOR_TABLE_SELECTOR_FUNCTION_SCHEMA_FILE"
    )
    vector_table_selector_user_template_file: Optional[str] = Field(
        default=None, alias="VECTOR_TABLE_SELECTOR_USER_TEMPLATE_FILE"
    )
    direct_sql_prompts_dir: str = Field(default="prompts/direct_sql", alias="DIRECT_SQL_PROMPTS_DIR")
    direct_sql_system_prompt_file: Optional[str] = Field(default=None, alias="DIRECT_SQL_SYSTEM_PROMPT_FILE")
    direct_sql_user_template_file: Optional[str] = Field(default=None, alias="DIRECT_SQL_USER_TEMPLATE_FILE")
    cot_planner_prompt_path: str = Field(default="prompts/cot_planner/prompt.txt", alias="COT_PLANNER_PROMPT_PATH")
    text_templates_file: str = Field(default="prompts/common/text_templates.json", alias="TEXT_TEMPLATES_FILE")
    
    @property
    def rls_bypass_roles_list(self) -> List[str]:
        """获取 RLS 豁免角色列表"""
        return [r.strip() for r in self.rls_bypass_roles.split(",") if r.strip()]
    
    @property
    def confirm_sensitive_metrics_list(self) -> List[str]:
        """获取敏感指标列表"""
        return [m.strip() for m in self.confirm_sensitive_metrics.split(",") if m.strip()]

    @property
    def cors_allow_origin_list(self) -> List[str]:
        """获取CORS允许域名列表"""
        origins = [origin.strip() for origin in self.cors_allow_origins.split(",") if origin.strip()]
        return origins or ["*"]

    @model_validator(mode="after")
    def _validate_production_secrets(self) -> "Settings":
        """
        生产环境强校验：禁止使用空密钥/密码。
        说明：仅校验必须显式提供的敏感项，避免误用代码默认值。
        """
        env = (self.app_env or "").strip().lower()
        is_prod = env in {"prod", "production"}
        if not is_prod:
            return self

        missing = []
        if not (self.jwt_secret or "").strip():
            missing.append("JWT_SECRET")
        if not (self.encryption_key or "").strip():
            missing.append("ENCRYPTION_KEY")
        if not (self.postgres_password or "").strip():
            missing.append("POSTGRES_PASSWORD")

        if missing:
            raise ValueError(f"生产环境缺少敏感配置: {', '.join(missing)}")
        return self


# 全局配置实例
settings = Settings()

# 检索配置（从YAML加载）
_retrieval_config_cache: Optional[Dict[str, Any]] = None
_retrieval_config_override: Optional[Dict[str, Any]] = None


def get_retrieval_config() -> Dict[str, Any]:
    """获取检索配置（带缓存）"""
    global _retrieval_config_cache
    global _retrieval_config_override
    if _retrieval_config_override is not None:
        return _retrieval_config_override
    if _retrieval_config_cache is None:
        _retrieval_config_cache = _load_retrieval_config()
    return _retrieval_config_cache


def reload_retrieval_config() -> Dict[str, Any]:
    """
    重新加载 retrieval_config.yaml（清除缓存）。
    用途：
    - 自动调参/诊断脚本在同一进程内多次评估不同参数组合
    """
    global _retrieval_config_cache
    _retrieval_config_cache = None
    return get_retrieval_config()


def set_retrieval_config_override(config: Optional[Dict[str, Any]]) -> None:
    """
    设置检索配置覆盖（进程内生效）。
    - config=None: 取消覆盖，回退到 YAML 缓存加载
    """
    global _retrieval_config_override
    _retrieval_config_override = config


def get_retrieval_param(path: str, default: Any = None) -> Any:
    """
    获取检索配置参数
    
    Args:
        path: 点分隔的配置路径，如 "rrf.k" 或 "table_retrieval.top_k"
        default: 默认值
    
    Returns:
        配置值或默认值
    """
    config = get_retrieval_config()
    keys = path.split(".")
    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    return value


def get_domain_dict_path() -> Optional[str]:
    """获取领域词典路径"""
    project_root = _find_project_root()
    dict_path = project_root / "config" / "domain_dict.txt"
    if dict_path.exists():
        return str(dict_path)
    return None


class RetrievalConfig:
    """检索配置访问器"""
    
    # ========== 功能开关（统一管理）==========
    @staticmethod
    def dense_enabled() -> bool:
        """Dense向量检索是否启用"""
        return get_retrieval_param("feature_switches.dense_enabled", True)
    
    @staticmethod
    def sparse_enabled() -> bool:
        """Sparse稀疏检索是否启用"""
        return get_retrieval_param("feature_switches.sparse_enabled", True)
    
    @staticmethod
    def rrf_enabled() -> bool:
        """RRF分数融合是否启用"""
        return get_retrieval_param("feature_switches.rrf_enabled", True)
    
    @staticmethod
    def reranker_enabled() -> bool:
        """Reranker精排是否启用"""
        return get_retrieval_param("feature_switches.reranker_enabled", True)
    
    @staticmethod
    def keyword_boost_enabled() -> bool:
        """关键词加分是否启用"""
        return get_retrieval_param("feature_switches.keyword_boost_enabled", True)
    
    @staticmethod
    def name_keyword_match_enabled() -> bool:
        """表名关键词匹配是否启用"""
        return get_retrieval_param("feature_switches.name_keyword_match_enabled", True)
    
    @staticmethod
    def exact_table_name_match_enabled() -> bool:
        """表名精确匹配是否启用"""
        return get_retrieval_param("feature_switches.exact_table_name_match_enabled", True)
    
    @staticmethod
    def measure_coverage_switch_enabled() -> bool:
        """度量覆盖因子开关（来自feature_switches）"""
        return get_retrieval_param("feature_switches.measure_coverage_enabled", True)
    
    @staticmethod
    def tag_match_enabled() -> bool:
        """语义标签匹配是否启用"""
        return get_retrieval_param("feature_switches.tag_match_enabled", True)
    
    @staticmethod
    def enum_boost_switch_enabled() -> bool:
        """枚举加成开关（来自feature_switches）"""
        return get_retrieval_param("feature_switches.enum_boost_enabled", False)
    
    @staticmethod
    def table_rescue_switch_enabled() -> bool:
        """表救援机制开关（来自feature_switches）"""
        return get_retrieval_param("feature_switches.table_rescue_enabled", True)
    
    @staticmethod
    def cross_domain_fallback_switch_enabled() -> bool:
        """跨域兜底开关（来自feature_switches）"""
        return get_retrieval_param("feature_switches.cross_domain_fallback_enabled", True)
    
    @staticmethod
    def multi_domain_retrieval_switch_enabled() -> bool:
        """多域全局检索开关（来自feature_switches）"""
        return get_retrieval_param("feature_switches.multi_domain_retrieval_enabled", True)
    
    @staticmethod
    def few_shot_switch_enabled() -> bool:
        """Few-Shot检索开关（来自feature_switches）"""
        return get_retrieval_param("feature_switches.few_shot_enabled", True)
    
    @staticmethod
    def enum_context_rerank_enabled() -> bool:
        """枚举context_vector精排开关"""
        return get_retrieval_param("feature_switches.enum_context_rerank_enabled", True)
    
    @staticmethod
    def enum_score_normalization_enabled() -> bool:
        """枚举分数归一化开关"""
        return get_retrieval_param("feature_switches.enum_score_normalization_enabled", True)
    
    @staticmethod
    def cross_table_guard_enabled() -> bool:
        """跨表一致性守卫开关"""
        return get_retrieval_param("feature_switches.cross_table_guard_enabled", True)
    
    @staticmethod
    def enum_sync_integrity_check_enabled() -> bool:
        """枚举同步完整性检查开关"""
        return get_retrieval_param("feature_switches.enum_sync_integrity_check_enabled", True)
    
    # ========== RRF 配置 ==========
    @staticmethod
    def rrf_k() -> int:
        return get_retrieval_param("rrf.k", 60)
    
    @staticmethod
    def rrf_weighted() -> bool:
        """RRF 是否启用加权融合（用于兼容旧配置/单测）。"""
        return bool(get_retrieval_param("rrf.weighted", False))

    @staticmethod
    def rrf_dense_weight() -> float:
        return get_retrieval_param("rrf.default_dense_weight", 0.6)
    
    @staticmethod
    def rrf_sparse_weight() -> float:
        return get_retrieval_param("rrf.default_sparse_weight", 0.4)
    
    @staticmethod
    def reranker_weight() -> float:
        return get_retrieval_param("reranker.weight", 0.3)
    
    @staticmethod
    def reranker_normalize() -> bool:
        return get_retrieval_param("reranker.normalize_output", True)
    
    @staticmethod
    def table_top_k() -> int:
        return get_retrieval_param("table_retrieval.top_k", 3)
    
    @staticmethod
    def table_dense_weight() -> float:
        return get_retrieval_param("table_retrieval.dense_weight", 0.5)
    
    @staticmethod
    def table_sparse_weight() -> float:
        return get_retrieval_param("table_retrieval.sparse_weight", 0.5)
    
    @staticmethod
    def domain_top_k() -> int:
        return get_retrieval_param("domain_retrieval.top_k", 3)
    
    @staticmethod
    def domain_dense_weight() -> float:
        return get_retrieval_param("domain_retrieval.dense_weight", 0.6)
    
    @staticmethod
    def domain_sparse_weight() -> float:
        return get_retrieval_param("domain_retrieval.sparse_weight", 0.4)
    
    @staticmethod
    def enum_dense_weight() -> float:
        return get_retrieval_param("enum_retrieval.dense_weight", 0.5)
    
    @staticmethod
    def enum_sparse_weight() -> float:
        return get_retrieval_param("enum_retrieval.sparse_weight", 0.5)
    
    @staticmethod
    def confidence_weights() -> Dict[str, float]:
        return get_retrieval_param("confidence.weights", {
            'A1_domain_dense': 0.12,
            'A2_table_score': 0.20,
            'A3_enum_exact_count': 0.12,
            'A4_measure_match': 0.08,
            'A5_triplet_count': 0.08,
            'A6_rrf_top': 0.15,
            'A7_reranker': 0.10,
            'B1_fewshot_exact': 0.10,
            'B2_keyword_hit_ratio': 0.05
        })
    
    @staticmethod
    def confidence_high_threshold() -> float:
        return get_retrieval_param("confidence.thresholds.high", 0.75)
    
    @staticmethod
    def confidence_medium_threshold() -> float:
        return get_retrieval_param("confidence.thresholds.medium", 0.45)
    
    @staticmethod
    def tokenization_protect_sql() -> bool:
        return get_retrieval_param("tokenization.protect_sql_identifiers", True)
    
    @staticmethod
    def tokenization_dense_remove_stopwords() -> bool:
        return get_retrieval_param("tokenization.dense.remove_stopwords", False)
    
    @staticmethod
    def tokenization_sparse_remove_stopwords() -> bool:
        return get_retrieval_param("tokenization.sparse.remove_stopwords", True)

    @staticmethod
    def tokenization_dense_use_entity_recognition() -> bool:
        """
        是否在 Dense 分词前启用结构实体预识别。

        注意：实际是否生效取决于调用方是否提供 entity_recognizer（按连接/按请求），
        以避免全局单例在多连接场景下串扰。
        """
        return get_retrieval_param("tokenization.dense.use_entity_recognition", False)

    @staticmethod
    def tokenization_sparse_use_entity_recognition() -> bool:
        """
        是否在 Sparse/BM25 分词前启用结构实体预识别。

        注意：实际是否生效取决于调用方是否提供 entity_recognizer（按连接/按请求），
        以避免全局单例在多连接场景下串扰。
        """
        return get_retrieval_param("tokenization.sparse.use_entity_recognition", True)
    
    @staticmethod
    def aggregation_hints() -> Dict[str, List[str]]:
        return get_retrieval_param("measure_retrieval.aggregation_hints", {
            "SUM": ["总", "合计", "累计"],
            "COUNT": ["数量", "个数", "多少"],
            "AVG": ["平均", "均值"],
            "MAX": ["最大", "最高"],
            "MIN": ["最小", "最低"]
        })

    @staticmethod
    def measure_retrieval_keywords() -> List[str]:
        return get_retrieval_param(
            "measure_retrieval.measure_keywords",
            ["面积", "金额", "数量", "总计", "用地", "土地"],
        )
    
    # ========== 枚举值检索配置 ==========
    @staticmethod
    def enum_per_field_limit() -> int:
        return get_retrieval_param("enum_retrieval.per_field_limit", 3)
    
    @staticmethod
    def enum_high_confidence_threshold() -> float:
        return get_retrieval_param("enum_retrieval.high_confidence_threshold", 0.3)
    
    @staticmethod
    def enum_high_noise_threshold() -> float:
        return get_retrieval_param("enum_retrieval.high_noise_threshold", 0.5)
    
    @staticmethod
    def enum_high_noise_fields() -> List[str]:
        return get_retrieval_param("enum_retrieval.high_noise_fields", [
            "批次名", "批次名称", "项目名称", "坐落", "备注", "描述",
            "项目", "名称", "地址", "详细地址", "说明", "内容",
        ])
    
    @staticmethod
    def enum_negative_signal_score_gap() -> float:
        return get_retrieval_param("enum_retrieval.negative_signal_score_gap", 0.3)
    
    @staticmethod
    def enum_negative_signal_max_penalty() -> float:
        return get_retrieval_param("enum_retrieval.negative_signal_max_penalty", 0.15)
    
    @staticmethod
    def enum_force_filter_min_score() -> float:
        return get_retrieval_param("enum_retrieval.force_filter_min_score", 0.9)
    
    # ========== 表打分 V2 配置 ==========
    # ---- Reranker 增强输入 ----
    @staticmethod
    def reranker_include_description() -> bool:
        return get_retrieval_param("table_scoring.reranker_input.include_description", True)
    
    @staticmethod
    def reranker_include_tags() -> bool:
        return get_retrieval_param("table_scoring.reranker_input.include_tags", True)
    
    @staticmethod
    def reranker_include_measures() -> bool:
        return get_retrieval_param("table_scoring.reranker_input.include_measures", True)
    
    # ---- 度量覆盖（乘法因子）----
    @staticmethod
    def measure_coverage_enabled() -> bool:
        """度量覆盖因子是否启用（读取feature_switches）"""
        return get_retrieval_param("feature_switches.measure_coverage_enabled", True)
    
    @staticmethod
    def measure_coverage_partial_min() -> float:
        return get_retrieval_param("table_scoring.measure_coverage.partial_min", 0.3)
    
    @staticmethod
    def measure_coverage_keywords() -> List[str]:
        return get_retrieval_param("table_scoring.measure_coverage.keywords", [
            "面积", "土地面积", "用地面积", "建筑面积", "图形面积", "出让面积",
            "金额", "总价", "地价", "楼面地价", "容积率", "成交价", "成交总价"
        ])

    @staticmethod
    def measure_extraction_normalization() -> Dict[str, Any]:
        return get_retrieval_param(
            "table_scoring.measure_coverage.extraction.normalization",
            {
                "nfkc": True,
                "lowercase": True,
                "remove_punct": True,
                "collapse_whitespace": True,
            },
        )

    @staticmethod
    def measure_extraction_min_phrase_len() -> int:
        return int(get_retrieval_param("table_scoring.measure_coverage.extraction.min_phrase_len", 3) or 3)

    @staticmethod
    def measure_extraction_generic_terms() -> List[str]:
        return get_retrieval_param(
            "table_scoring.measure_coverage.extraction.generic_terms",
            ["面积", "金额", "数量", "总", "数", "量", "值", "额"],
        )

    @staticmethod
    def measure_extraction_suffix_keywords() -> List[str]:
        return get_retrieval_param(
            "table_scoring.measure_coverage.extraction.suffix_keywords",
            ["面积", "金额", "价", "单价", "总价", "均价", "率", "比", "容积率"],
        )

    @staticmethod
    def measure_extraction_unit_require_number() -> bool:
        return bool(get_retrieval_param("table_scoring.measure_coverage.extraction.unit_detection.require_number", True))

    @staticmethod
    def measure_extraction_unit_hints() -> Dict[str, List[str]]:
        return get_retrieval_param("table_scoring.measure_coverage.extraction.unit_hints", {})

    @staticmethod
    def measure_universal_keywords() -> List[str]:
        """通用度量关键词（不参与覆盖率惩罚，任何表都能计算）"""
        return get_retrieval_param("table_scoring.measure_coverage.universal_keywords", [
            "宗数", "数量", "条数", "笔数", "件数", "记录数", "个数", "多少"
        ])

    @staticmethod
    def measure_compound_keywords() -> Dict[str, str]:
        """复合度量词映射（复合词 -> 基础度量词）"""
        return get_retrieval_param("table_scoring.measure_coverage.compound_keywords", {
            "耕地面积": "面积",
            "农用地面积": "面积",
            "建设用地面积": "面积",
            "未利用地面积": "面积",
            "总用地面积": "面积",
            "总用地": "面积",
            "耕地": "面积",
            "农用地": "面积",
            "建设用地": "面积",
            "未利用地": "面积",
            "成交总价": "总价",
            "总面积": "面积",
        })

    @staticmethod
    def measure_families() -> Dict[str, str]:
        """度量族映射（同族短语 -> 统一概念 key）"""
        return get_retrieval_param("table_scoring.measure_coverage.measure_families", {})

    # ---- 度量字段（PG）表级信号 ----
    @staticmethod
    def measure_pg_enabled() -> bool:
        # 统一总开关（feature_switches）+ 子模块开关（table_scoring），兼容旧配置缺失情况
        master = bool(get_retrieval_param("feature_switches.measure_pg_enabled", True))
        local = bool(get_retrieval_param("table_scoring.measure_pg.enabled", True))
        return bool(master and local)

    @staticmethod
    def measure_pg_weight() -> float:
        return float(get_retrieval_param("table_scoring.measure_pg.weight", 0.12) or 0.12)

    @staticmethod
    def measure_pg_top_k_fields() -> int:
        return int(get_retrieval_param("table_scoring.measure_pg.top_k_fields", 20) or 20)

    @staticmethod
    def measure_pg_table_agg() -> str:
        return str(get_retrieval_param("table_scoring.measure_pg.table_agg", "max") or "max")

    @staticmethod
    def measure_pg_apply_when_measure_intent() -> bool:
        return bool(get_retrieval_param("table_scoring.measure_pg.apply_when_measure_intent", True))

    # ---- 度量字段（Milvus）表级信号 ----
    @staticmethod
    def measure_field_milvus_enabled() -> bool:
        master = bool(get_retrieval_param("feature_switches.measure_field_milvus_enabled", True))
        local = bool(get_retrieval_param("table_scoring.measure_field_milvus.enabled", True))
        return bool(master and local)

    @staticmethod
    def measure_field_milvus_weight() -> float:
        return float(get_retrieval_param("table_scoring.measure_field_milvus.weight", 0.12) or 0.12)

    @staticmethod
    def measure_field_milvus_top_k_fields() -> int:
        return int(get_retrieval_param("table_scoring.measure_field_milvus.top_k_fields", 50) or 50)

    @staticmethod
    def measure_field_milvus_min_field_score() -> float:
        return float(get_retrieval_param("table_scoring.measure_field_milvus.min_field_score", 0.0) or 0.0)

    @staticmethod
    def measure_field_milvus_min_field_score_ratio() -> float:
        """A2：相对阈值（按 top_rrf 的比例过滤 0~1；0 表示禁用，仅用绝对阈值）。"""
        return float(get_retrieval_param("table_scoring.measure_field_milvus.min_field_score_ratio", 0.0) or 0.0)

    @staticmethod
    def measure_field_milvus_use_measure_query_vector() -> bool:
        return bool(get_retrieval_param("table_scoring.measure_field_milvus.use_measure_query_vector", False))

    @staticmethod
    def measure_field_milvus_apply_when_measure_intent() -> bool:
        return bool(get_retrieval_param("table_scoring.measure_field_milvus.apply_when_measure_intent", True))

    # ---- 度量救援 ----
    @staticmethod
    def measure_rescue_enabled() -> bool:
        master = bool(get_retrieval_param("feature_switches.measure_rescue_enabled", True))
        local = bool(get_retrieval_param("table_scoring.measure_rescue.enabled", True))
        return bool(master and local)

    @staticmethod
    def measure_rescue_threshold() -> float:
        return float(get_retrieval_param("table_scoring.measure_rescue.threshold", 0.85) or 0.85)
    
    # ---- 枚举加成全局开关 ----
    @staticmethod
    def enum_boost_enabled() -> bool:
        """枚举加成是否启用（读取feature_switches）"""
        return get_retrieval_param("feature_switches.enum_boost_enabled", False)
    
    # ---- 枚举门控（加法加成）----
    # 注意：参数统一从 config/retrieval_config.yaml 的 table_scoring.enum_boost 节点读取
    @staticmethod
    def enum_gate_context_threshold() -> float:
        return get_retrieval_param("table_scoring.enum_boost.context_threshold", 0.2)
    
    @staticmethod
    def enum_gate_exact_boost() -> float:
        return get_retrieval_param("table_scoring.enum_boost.exact_boost", 0.02)
    
    @staticmethod
    def enum_gate_vector_boost() -> float:
        return get_retrieval_param("table_scoring.enum_boost.vector_boost", 0.01)
    
    @staticmethod
    def enum_gate_max_boost() -> float:
        return get_retrieval_param("table_scoring.enum_boost.max_boost", 0.03)
    
    # ========== 表检索 - 基础配置 ==========
    @staticmethod
    def table_expansion_factor() -> int:
        """候选扩展因子（实际召回数 = top_k * expansion_factor）"""
        return get_retrieval_param("table_retrieval.expansion_factor", 5)
    
    # ========== 表检索 - 表救援机制配置 ==========
    @staticmethod
    def table_rescue_enabled() -> bool:
        """表救援机制是否启用（读取feature_switches）"""
        return get_retrieval_param("feature_switches.table_rescue_enabled", True)
    
    @staticmethod
    def table_rescue_min_score() -> float:
        return get_retrieval_param("table_retrieval.rescue.min_rescue_score", 0.2)
    
    # ========== 表检索 - 跨域兜底配置 ==========
    @staticmethod
    def cross_domain_fallback_enabled() -> bool:
        """跨域兜底是否启用（读取feature_switches）"""
        return get_retrieval_param("feature_switches.cross_domain_fallback_enabled", True)
    
    @staticmethod
    def cross_domain_max_candidates() -> int:
        return get_retrieval_param("table_retrieval.cross_domain_fallback.max_candidates", 3)
    
    @staticmethod
    def cross_domain_allow_no_domain() -> bool:
        return get_retrieval_param("table_retrieval.cross_domain_fallback.allow_no_domain", True)
    
    @staticmethod
    def cross_domain_fallback_top_k() -> Optional[int]:
        return get_retrieval_param("table_retrieval.cross_domain_fallback.fallback_top_k", None)
    
    @staticmethod
    def cross_domain_score_threshold() -> Optional[float]:
        return get_retrieval_param("table_retrieval.cross_domain_fallback.score_threshold", None)
    
    @staticmethod
    def cross_domain_score_threshold_delta() -> float:
        return get_retrieval_param("table_retrieval.cross_domain_fallback.score_threshold_delta", 0.05)
    
    @staticmethod
    def cross_domain_min_results() -> int:
        return get_retrieval_param("table_retrieval.cross_domain_fallback.min_results", 1)
    
    # ========== 表检索 - 多域并行检索配置 ==========
    @staticmethod
    def multi_domain_enabled() -> bool:
        """多域检索是否启用（读取feature_switches）"""
        return get_retrieval_param("feature_switches.multi_domain_retrieval_enabled", True)
    
    @staticmethod
    def multi_domain_candidate_count() -> int:
        return get_retrieval_param("table_retrieval.multi_domain_retrieval.candidate_domain_count", 3)
    
    @staticmethod
    def multi_domain_parallel_enabled() -> bool:
        return get_retrieval_param("table_retrieval.multi_domain_retrieval.parallel", False)
    
    @staticmethod
    def multi_domain_quality_weights() -> Dict[str, float]:
        return get_retrieval_param(
            "table_retrieval.multi_domain_retrieval.quality_weights",
            {"table_score": 0.4, "field_count": 0.3, "domain_score": 0.3},
        )
    
    @staticmethod
    def global_table_retrieval_limit() -> int:
        """全局表检索的候选数量限制（5个域 * 5个表/域 = 25）"""
        return get_retrieval_param(
            "table_retrieval.multi_domain_retrieval.global_top_k", 25
        )
    
    @staticmethod
    def multi_domain_min_field_count() -> int:
        return get_retrieval_param("table_retrieval.multi_domain_retrieval.min_field_count", 1)
    
    @staticmethod
    def multi_domain_prioritize_usable_tables() -> bool:
        return get_retrieval_param(
            "table_retrieval.multi_domain_retrieval.prioritize_usable_tables", True
        )
    
    @staticmethod
    def multi_domain_max_field_for_normalization() -> int:
        return get_retrieval_param(
            "table_retrieval.multi_domain_retrieval.max_field_count_for_normalization", 50
        )

    # ========== 字段排序配置 ==========
    @staticmethod
    def field_enum_feedback_alpha() -> float:
        return get_retrieval_param("field_sorting.enum_feedback_alpha", 0.3)
    
    @staticmethod
    def field_enum_boost_multiplier() -> float:
        return get_retrieval_param("field_sorting.enum_boost_multiplier", 10)
    
    # ========== Prompt策略配置 ==========
    @staticmethod
    def prompt_enum_force_use_threshold() -> float:
        return get_retrieval_param("prompt_strategy.enum_thresholds.force_use", 0.98)
    
    @staticmethod
    def prompt_enum_strong_suggest_threshold() -> float:
        return get_retrieval_param("prompt_strategy.enum_thresholds.strong_suggest", 0.90)
    
    @staticmethod
    def prompt_enum_weak_hint_threshold() -> float:
        return get_retrieval_param("prompt_strategy.enum_thresholds.weak_hint", 0.70)
    
    # ========== 分数归一化配置 ==========
    @staticmethod
    def score_normalization_enabled() -> bool:
        return get_retrieval_param("enum_retrieval.score_normalization.enabled", True)
    
    @staticmethod
    def score_normalization_method() -> str:
        return get_retrieval_param("enum_retrieval.score_normalization.method", "sigmoid")
    
    @staticmethod
    def score_normalization_sparse_scale() -> float:
        return get_retrieval_param("enum_retrieval.score_normalization.sparse_scale", 10.0)
    
    # ========== 高噪声字段增强配置 ==========
    @staticmethod
    def enum_per_high_noise_field_limit() -> int:
        return get_retrieval_param("enum_retrieval.per_high_noise_field_limit", 1)
    
    @staticmethod
    def enum_exclude_from_llm_prompt() -> bool:
        return get_retrieval_param("enum_retrieval.exclude_from_llm_prompt", True)
    
    # ========== 编译器跨表守卫配置 ==========
    @staticmethod
    def compiler_cross_table_guard_enabled() -> bool:
        return get_retrieval_param("compiler.cross_table_guard.enabled", True)
    
    @staticmethod
    def compiler_cross_table_guard_strict() -> bool:
        return get_retrieval_param("compiler.cross_table_guard.strict", False)
    
    # ========================================================================
    # 多连接查询配置
    # ========================================================================
    
    @staticmethod
    def enable_global_model() -> bool:
        """是否启用全局语义模型"""
        return get_retrieval_param("multi_connection.enable_global_model", True)
    
    @staticmethod
    def connection_detection_mode() -> str:
        """连接检测模式: strict | auto | flexible"""
        return get_retrieval_param("multi_connection.connection_detection.mode", "auto")
    
    @staticmethod
    def cross_connection_handling() -> str:
        """跨连接处理策略: reject | warn | auto_select"""
        return get_retrieval_param(
            "multi_connection.connection_detection.cross_connection_handling",
            "reject"
        )
    
    @staticmethod
    def return_candidate_details() -> bool:
        """是否返回候选连接详情"""
        return get_retrieval_param(
            "multi_connection.connection_detection.return_candidate_details",
            True
        )
    
    @staticmethod
    def user_specified_conflict_resolution() -> str:
        """用户指定冲突处理: smart | trust_user | trust_inference"""
        return get_retrieval_param(
            "multi_connection.connection_detection.user_specified_conflict_resolution",
            "smart"
        )
    
    @staticmethod
    def compiler_cross_connection_strict() -> bool:
        """编译器跨连接严格模式"""
        return get_retrieval_param(
            "multi_connection.compiler.cross_connection_strict",
            True
        )
    
    @staticmethod
    def table_not_found_handling() -> str:
        """表映射失败处理: error | warn | skip"""
        return get_retrieval_param(
            "multi_connection.compiler.table_not_found_handling",
            "error"
        )
    
    @staticmethod
    def global_model_cache_ttl() -> int:
        """全局模型缓存TTL（秒）"""
        return get_retrieval_param(
            "multi_connection.performance.global_model_cache_ttl",
            1800
        )
    
    @staticmethod
    def parallel_load_connections() -> bool:
        """是否并行加载连接模型"""
        return get_retrieval_param(
            "multi_connection.performance.parallel_load_connections",
            True
        )
    
    @staticmethod
    def max_parallel_connections() -> int:
        """最大并行连接数"""
        return get_retrieval_param(
            "multi_connection.performance.max_parallel_connections",
            4
        )
    
    @staticmethod
    def enable_connection_resolution_tracer() -> bool:
        """是否启用连接解析tracer"""
        return get_retrieval_param(
            "multi_connection.observability.enable_connection_resolution_tracer",
            True
        )
    
    @staticmethod
    def enable_connection_metrics() -> bool:
        """是否启用连接指标收集"""
        return get_retrieval_param(
            "multi_connection.observability.enable_connection_metrics",
            True
        )
    
    @staticmethod
    def connection_resolution_log_level() -> str:
        """连接解析日志级别"""
        return get_retrieval_param(
            "multi_connection.observability.connection_resolution_log_level",
            "info"
        )
    
    # ========== Trace 配置 ==========
    @staticmethod
    def trace_include_table_info() -> bool:
        return get_retrieval_param("trace.include_table_info", True)
    
    # ========== 枚举同步完整性检查配置 ==========
    @staticmethod
    def enum_sync_integrity_check_enabled() -> bool:
        return get_retrieval_param("enum_sync.integrity_check.enabled", True)
    
    @staticmethod
    def enum_sync_min_coverage_ratio() -> float:
        return get_retrieval_param("enum_sync.integrity_check.min_coverage_ratio", 0.8)
    
    @staticmethod
    def enum_sync_high_value_fields() -> List[str]:
        return get_retrieval_param("enum_sync.integrity_check.high_value_fields", ["行政区", "年份", "用途"])
    
    # ========== 文本处理配置 ==========
    @staticmethod
    def bm25_text_limit() -> int:
        return get_retrieval_param("text_processing.bm25_text_limit", 512)
    
    @staticmethod
    def dense_text_limit() -> int:
        return get_retrieval_param("text_processing.dense_text_limit", 1024)
    
    @staticmethod
    def enum_cardinality_threshold() -> int:
        return get_retrieval_param("text_processing.enum_cardinality_threshold", 5000)
    
    @staticmethod
    def hybrid_value_boost() -> float:
        return get_retrieval_param("text_processing.hybrid_value_boost", 3.0)
    
    # ========== 阈值配置 ==========
    @staticmethod
    def table_threshold() -> float:
        return get_retrieval_param("table_retrieval.threshold", 0.3)
    
    @staticmethod
    def domain_threshold() -> float:
        return get_retrieval_param("domain_retrieval.threshold", 0.3)
    
    # ========== 枚举检索 RRF 加权 ==========
    @staticmethod
    def enum_exact_rrf_boost() -> float:
        return get_retrieval_param("enum_retrieval.exact_boost", 0.6)
    
    @staticmethod
    def enum_synonym_rrf_boost() -> float:
        return get_retrieval_param("enum_retrieval.synonym_boost", 0.4)
    
    @staticmethod
    def enum_top_k() -> int:
        return get_retrieval_param("enum_retrieval.top_k", 5)
    
    # ========== Few-Shot 检索配置 ==========
    @staticmethod
    def few_shot_top_k() -> int:
        return get_retrieval_param("few_shot_retrieval.top_k", 6)
    
    @staticmethod
    def few_shot_similarity_threshold() -> float:
        return get_retrieval_param("few_shot_retrieval.similarity_threshold", 0.3)
    
    @staticmethod
    def few_shot_min_quality_score() -> float:
        return get_retrieval_param("few_shot_retrieval.min_quality_score", 0.7)
    
    @staticmethod
    def few_shot_prompt_max_examples() -> int:
        return get_retrieval_param("few_shot_retrieval.prompt_max_examples", 3)
    
    @staticmethod
    def few_shot_dense_priority() -> int:
        return get_retrieval_param("few_shot_retrieval.dense_priority", 2)
    
    @staticmethod
    def few_shot_normalize_question() -> bool:
        return get_retrieval_param("few_shot_retrieval.normalize_question", True)
    
    @staticmethod
    def few_shot_immediate_sync() -> bool:
        return get_retrieval_param("few_shot_retrieval.immediate_sync", False)
    
    @staticmethod
    def few_shot_sync_in_full_sync() -> bool:
        return get_retrieval_param("few_shot_retrieval.sync_in_full_sync", True)
    
    @staticmethod
    def few_shot_direct_execution_enabled() -> bool:
        return get_retrieval_param("few_shot_retrieval.direct_execution.enabled", False)
    
    @staticmethod
    def few_shot_direct_min_similarity() -> float:
        return get_retrieval_param("few_shot_retrieval.direct_execution.min_similarity", 0.95)
    
    @staticmethod
    def few_shot_direct_min_quality() -> float:
        return get_retrieval_param("few_shot_retrieval.direct_execution.min_quality", 0.9)
    
    @staticmethod
    def few_shot_direct_source_whitelist() -> List[str]:
        return get_retrieval_param("few_shot_retrieval.direct_execution.source_whitelist", ["manual", "auto"])
