-- ============================================================================
-- NL2SQL - 元数据库统一初始化脚本
-- 版本: v2.0 (2025-11-05)
-- 架构说明：
-- 1. 移除多租户架构，简化为：用户 → 数据库连接 → 业务域 → 表 → 字段
-- 2. 业务域用于两步意图识别
-- 3. 基础指标融合到字段，派生指标作为全局规则
-- 4. 表关系自动识别+手动确认
-- 5. 支持枚举值同步到Milvus向量库
-- ============================================================================

-- 设置时区为亚洲/上海时间 (UTC+8)
SET timezone = 'Asia/Shanghai';
-- 确保日志时区也为上海时间
SET log_timezone = 'Asia/Shanghai';

-- 验证时区设置
DO $$
BEGIN
    RAISE NOTICE '数据库时区设置为: %', current_setting('timezone');
    RAISE NOTICE '当前时间: %', NOW();
END $$;

-- 启用UUID扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- 第一层：用户与权限
-- ============================================================================

-- 用户表
CREATE TABLE users (
 user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

 -- 基本信息
 username VARCHAR(50) NOT NULL UNIQUE,
 password_hash VARCHAR(255) NOT NULL,
 email VARCHAR(100),
 full_name VARCHAR(100),

 -- 外部身份映射（支持多认证源）
 external_idp VARCHAR(100),          -- 外部身份提供方标识，如 'oidc_corpA'
 external_uid VARCHAR(200),          -- 外部用户唯一ID，如 sub
 profile_json JSONB,                 -- 外部返回的用户信息快照

 -- 系统角色（控制管理后台功能权限）
 role VARCHAR(20) NOT NULL DEFAULT 'user',
 -- 'admin': 系统管理员 - 所有管理功能
 -- 'data_admin': 数据管理员 - 数据库连接/元数据/同步/数据权限
 -- 'user': 普通用户 - 仅查询，不能登录后台

 -- 组织架构关联（一对一，一个用户只属于一个组织）
 org_id UUID,                        -- 所属组织ID（外键在organizations表创建后添加）
 position VARCHAR(100),              -- 用户在组织中的职位

 -- 状态
 is_active BOOLEAN DEFAULT TRUE,
 last_login_at TIMESTAMP WITH TIME ZONE,

 -- 时间戳
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_role ON users(role);
CREATE INDEX idx_users_external ON users(external_idp, external_uid);
CREATE INDEX idx_users_org ON users(org_id);
-- 外部身份唯一性：仅当 external_idp 与 external_uid 均不为空时约束
CREATE UNIQUE INDEX uq_users_external ON users(external_idp, external_uid) WHERE external_idp IS NOT NULL AND external_uid IS NOT NULL;

COMMENT ON TABLE users IS '用户表';
COMMENT ON COLUMN users.role IS '系统角色: admin(系统管理员-全部权限), data_admin(数据管理员-数据相关功能), user(普通用户-仅查询)。控制管理后台功能访问';
COMMENT ON COLUMN users.external_idp IS '外部身份提供方标识，如 oidc_corpA';
COMMENT ON COLUMN users.external_uid IS '外部用户唯一ID，如 OIDC sub';
COMMENT ON COLUMN users.profile_json IS '外部用户信息快照（JSON）';
COMMENT ON COLUMN users.org_id IS '所属组织ID（一个用户只属于一个组织）';
COMMENT ON COLUMN users.position IS '用户在组织中的职位';

-- ============================================================================
-- 第1.2层：组织架构
-- ============================================================================

-- 组织架构表（树形结构）
CREATE TABLE organizations (
    org_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- 基本信息
    org_code VARCHAR(50) NOT NULL,                    -- 组织编码，如 'tech_dept'
    org_name VARCHAR(100) NOT NULL,                   -- 组织名称，如 '技术部'
    parent_id UUID REFERENCES organizations(org_id) ON DELETE SET NULL,
    org_type VARCHAR(20) DEFAULT 'department',        -- 'company', 'department', 'team', 'group'
    description TEXT,
    
    -- 外部来源追踪（从OIDC/LDAP同步时记录）
    source_idp VARCHAR(100),                          -- 来自哪个认证提供者的 provider_key
    external_org_id VARCHAR(200),                     -- 外部系统中的组织ID
    
    -- 层级辅助（便于查询和展示）
    org_path TEXT,                                    -- 物化路径，如 '/总公司/技术中心/研发部'
    level INT DEFAULT 0,                              -- 层级深度（0=根节点）
    sort_order INT DEFAULT 0,                         -- 同级排序
    
    -- 状态
    is_active BOOLEAN DEFAULT TRUE,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID,                                  -- 不加外键避免循环依赖
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 组织编码全局唯一
    CONSTRAINT uq_organizations_code UNIQUE (org_code)
);

-- 组织架构索引
CREATE INDEX idx_organizations_parent ON organizations(parent_id);
CREATE INDEX idx_organizations_source ON organizations(source_idp) WHERE source_idp IS NOT NULL;
CREATE INDEX idx_organizations_active ON organizations(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_organizations_path ON organizations(org_path);
CREATE INDEX idx_organizations_type ON organizations(org_type);
CREATE INDEX idx_organizations_level ON organizations(level);

-- 外部来源唯一约束
CREATE UNIQUE INDEX uq_organizations_external 
    ON organizations(source_idp, external_org_id) 
    WHERE source_idp IS NOT NULL AND external_org_id IS NOT NULL;

COMMENT ON TABLE organizations IS '组织架构表（树形结构，支持外部系统同步）';
COMMENT ON COLUMN organizations.org_code IS '组织编码，全局唯一';
COMMENT ON COLUMN organizations.org_name IS '组织名称';
COMMENT ON COLUMN organizations.parent_id IS '父组织ID，NULL表示根节点';
COMMENT ON COLUMN organizations.org_type IS '组织类型：company(公司), department(部门), team(团队), group(小组)';
COMMENT ON COLUMN organizations.source_idp IS '外部来源：认证提供者的provider_key，NULL表示本地创建';
COMMENT ON COLUMN organizations.external_org_id IS '外部系统中的组织ID';
COMMENT ON COLUMN organizations.org_path IS '物化路径，便于层级查询';
COMMENT ON COLUMN organizations.level IS '层级深度，0表示根节点';

-- 添加 users 表的组织外键约束
ALTER TABLE users ADD CONSTRAINT fk_users_org FOREIGN KEY (org_id) REFERENCES organizations(org_id) ON DELETE SET NULL;

-- 认证提供者配置表
CREATE TABLE auth_providers (
    provider_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_key VARCHAR(100) NOT NULL,           -- 自定义唯一标识，如 'oidc_main'
    provider_type VARCHAR(50) NOT NULL,           -- 'local' | 'oidc' | 'api_gateway' | 'ldap'
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb, -- 提供者配置（密钥应加密存储）
    enabled BOOLEAN DEFAULT TRUE,
    priority INT DEFAULT 100,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_auth_providers_key UNIQUE (provider_key)
);

CREATE INDEX idx_auth_providers_type ON auth_providers(provider_type);
CREATE INDEX idx_auth_providers_enabled ON auth_providers(enabled) WHERE enabled = TRUE;

COMMENT ON TABLE auth_providers IS '认证提供者配置表（支持多provider开关与优先级）';

-- ============================================================================
-- 第1.5层：数据权限管理（数据角色、用户属性）
-- 说明：
-- - 系统角色(users.role): 控制管理后台功能权限
-- - 数据角色(data_roles): 控制查询时的数据访问范围
-- ============================================================================

-- 用户业务属性表（供行级权限使用）
CREATE TABLE user_attributes (
    attribute_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    
    -- 属性定义
    attribute_name VARCHAR(100) NOT NULL,     -- 属性名，如 dept_code, region, allowed_regions
    attribute_value TEXT NOT NULL,            -- 属性值（数组用JSON格式存储，如 ["华东","华南"]）
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(user_id, attribute_name)
);

CREATE INDEX idx_user_attributes_user ON user_attributes(user_id);

COMMENT ON TABLE user_attributes IS '用户业务属性表 - 用于行级权限的动态过滤';
COMMENT ON COLUMN user_attributes.attribute_value IS '属性值，支持单值或JSON数组格式如["华东","华南"]';

-- ============================================================================
-- 第二层：数据库连接管理
-- ============================================================================

-- 数据库连接表
CREATE TABLE IF NOT EXISTS database_connections (
 connection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

 -- 连接信息
 connection_name VARCHAR(100) NOT NULL,
 description TEXT,

 -- 数据库配置
 db_type VARCHAR(20) NOT NULL DEFAULT 'sqlserver', -- 'sqlserver', 'mysql', 'postgresql'
 host VARCHAR(255) NOT NULL,
 port INT NOT NULL,
 database_name VARCHAR(100) NOT NULL,
 username VARCHAR(100) NOT NULL,
 password_encrypted TEXT NOT NULL, -- 加密存储

 -- 连接池配置
 max_connections INT DEFAULT 10,
 connection_timeout INT DEFAULT 30, -- 秒

 -- 状态
 is_active BOOLEAN DEFAULT TRUE,
 last_sync_at TIMESTAMP WITH TIME ZONE, -- 最后同步时间
 sync_status VARCHAR(20), -- 'success', 'failed', 'syncing'
 sync_message TEXT,

 -- 统计信息
 table_count INT DEFAULT 0,
 field_count INT DEFAULT 0,

 -- 时间戳
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 created_by UUID REFERENCES users(user_id),
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

 UNIQUE(connection_name)
);

CREATE INDEX IF NOT EXISTS idx_connections_name ON database_connections(connection_name);
CREATE INDEX IF NOT EXISTS idx_connections_active ON database_connections(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE database_connections IS '数据库连接配置表';
COMMENT ON COLUMN database_connections.password_encrypted IS '加密存储的数据库密码';

-- ============================================================================
-- 第三层：业务域（用于意图识别）
-- ============================================================================

-- 业务域表（全局，不再绑定特定连接）
CREATE TABLE business_domains (
 domain_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 -- connection_id 改为可选，用于向后兼容；新架构下业务域是全局的
 connection_id UUID REFERENCES database_connections(connection_id) ON DELETE SET NULL,

 -- 基本信息
 domain_code VARCHAR(50) NOT NULL, -- 'finance', 'hr', 'sales'
 domain_name VARCHAR(100) NOT NULL, -- '财务域', '人力资源域'

 -- 意图识别核心字段
 description TEXT DEFAULT '', -- 详细描述，用于向量检索
 keywords TEXT[], -- 关键词数组，用于关键词检索
 typical_queries TEXT[], -- 典型查询示例

 -- 展示配置
 icon VARCHAR(50) DEFAULT '📊',
 color VARCHAR(20) DEFAULT '#409eff',
 sort_order INT DEFAULT 0,

 -- 统计信息
 table_count INT DEFAULT 0,

 -- 状态
 is_active BOOLEAN DEFAULT TRUE,

 -- 时间戳
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 created_by UUID REFERENCES users(user_id),
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

 -- 业务域编码全局唯一（移除 connection_id 的联合唯一约束）
 UNIQUE(domain_code)
);

CREATE INDEX idx_domains_connection ON business_domains(connection_id) WHERE connection_id IS NOT NULL;
CREATE INDEX idx_domains_active ON business_domains(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_domains_code ON business_domains(domain_code);

COMMENT ON TABLE business_domains IS '业务域表（全局），用于意图识别：定位业务域';
COMMENT ON COLUMN business_domains.connection_id IS '可选，向后兼容。新架构下业务域不再绑定特定连接';
COMMENT ON COLUMN business_domains.description IS '详细描述，用于Milvus向量检索';
COMMENT ON COLUMN business_domains.keywords IS '关键词数组，用于关键词检索';

-- ============================================================================
-- 第四层：数据表与列（物理层）
-- ============================================================================

-- 数据表
CREATE TABLE db_tables (
 table_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 connection_id UUID NOT NULL REFERENCES database_connections(connection_id) ON DELETE CASCADE,
 domain_id UUID REFERENCES business_domains(domain_id) ON DELETE SET NULL, -- 关联业务域

 -- 物理信息
 schema_name VARCHAR(100), -- 'dbo', 'public'
 table_name VARCHAR(100) NOT NULL,

 -- 逻辑信息（手动配置）
 display_name VARCHAR(100), -- 显示名称
 description TEXT, -- 表描述
 tags TEXT[], -- 标签

 -- 统计信息
 row_count BIGINT,
 column_count INT,
 data_size_mb NUMERIC(10,2),
 last_updated_at TIMESTAMP WITH TIME ZONE,

 -- 配置
 is_included BOOLEAN DEFAULT TRUE, -- 是否纳入查询范围

 -- 时间戳
 discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 首次发现时间
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 data_year VARCHAR(4), -- 数据所属年份（如：2024）

 UNIQUE(connection_id, schema_name, table_name)
);

CREATE INDEX idx_tables_connection ON db_tables(connection_id);
CREATE INDEX idx_tables_domain ON db_tables(domain_id);
CREATE INDEX idx_tables_name ON db_tables(table_name);
CREATE INDEX idx_tables_included ON db_tables(is_included) WHERE is_included = TRUE;

COMMENT ON TABLE db_tables IS '数据表元数据';
COMMENT ON COLUMN db_tables.domain_id IS '所属业务域（用于意图识别第二步）';
COMMENT ON COLUMN db_tables.data_year IS '数据所属年份（如：2024）';
COMMENT ON COLUMN db_tables.tags IS '表的同义词和标签（JSON数组）';

-- 数据列
CREATE TABLE db_columns (
 column_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 table_id UUID NOT NULL REFERENCES db_tables(table_id) ON DELETE CASCADE,

 -- 物理信息
 column_name VARCHAR(100) NOT NULL,
 data_type VARCHAR(50) NOT NULL,
 max_length BIGINT,
 is_nullable BOOLEAN DEFAULT TRUE,
 is_primary_key BOOLEAN DEFAULT FALSE,
 is_foreign_key BOOLEAN DEFAULT FALSE,
 ordinal_position INT DEFAULT 999, -- 列在表中的顺序位置

 -- 外键信息（如果是外键）
 referenced_table_id UUID REFERENCES db_tables(table_id),
 referenced_column_id UUID, -- 可能是循环引用，所以不设外键约束

 -- 统计信息
 distinct_count BIGINT,
 null_count BIGINT,
 sample_values TEXT[], -- 采样值

 -- 时间戳
 discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

 UNIQUE(table_id, column_name)
);

CREATE INDEX idx_columns_table ON db_columns(table_id);
CREATE INDEX idx_columns_name ON db_columns(column_name);
CREATE INDEX idx_columns_pk ON db_columns(is_primary_key) WHERE is_primary_key = TRUE;
CREATE INDEX idx_columns_fk ON db_columns(is_foreign_key) WHERE is_foreign_key = TRUE;
CREATE INDEX idx_columns_ordinal ON db_columns(table_id, ordinal_position); -- 顺序索引

COMMENT ON TABLE db_columns IS '数据列元数据';
COMMENT ON COLUMN db_columns.ordinal_position IS '列在表中的顺序位置（从1开始，用于保持与源数据库一致的显示顺序）';

-- ============================================================================
-- 第五层：字段配置（语义层 - 自动识别+手动精调）
-- ============================================================================

-- 字段配置表（统一维度、度量、时间戳）
CREATE TABLE fields (
 field_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 connection_id UUID NOT NULL REFERENCES database_connections(connection_id) ON DELETE CASCADE,

 -- 关联物理列（一对一或一对多）
 source_type VARCHAR(20) NOT NULL, -- 'column', 'expression'
 source_column_id UUID REFERENCES db_columns(column_id) ON DELETE CASCADE, -- 如果是列
 source_expression TEXT, -- 如果是表达式

 -- 字段类型
 field_type VARCHAR(20) NOT NULL, -- 'dimension', 'measure', 'timestamp', 'identifier', 'spatial'

 -- 显示信息
 display_name VARCHAR(100) NOT NULL,
 description TEXT,
 synonyms TEXT[], -- 同义词

 -- 基础指标配置（融合到字段）
 default_aggregation VARCHAR(20), -- 'SUM', 'AVG', 'COUNT', 'MAX', 'MIN'
 allowed_aggregations TEXT[], -- ['SUM', 'AVG'] - 允许的聚合函数
 unit VARCHAR(50), -- '元', '人', '%'
 format_pattern VARCHAR(50), -- '{:,.2f}', '{:,.0f}'
 unit_conversion JSONB, -- 单位转换配置，如：{"original_unit": "平方米", "target_unit": "公顷", "conversion_factor": 10000, "conversion_method": "divide"}

 -- 维度字段专属配置
 dimension_type VARCHAR(20), -- 'categorical', 'hierarchical', 'temporal'
 hierarchy_level INT, -- 层级（如：国家=1, 省=2, 市=3）
 parent_field_id UUID REFERENCES fields(field_id), -- 父字段（层级关系）

 -- 度量字段专属配置
 is_additive BOOLEAN DEFAULT TRUE, -- 是否可加

 -- 标识字段专属配置
 is_unique BOOLEAN DEFAULT FALSE, -- 是否唯一

 -- 自动识别配置
 auto_detected BOOLEAN DEFAULT FALSE, -- 是否自动识别
 confidence_score NUMERIC(3,2), -- 识别置信度 0-1

 -- 业务标签
 tags TEXT[],
 business_category VARCHAR(100), -- '财务指标', '用户属性'

 -- 明细显示控制
 priority INTEGER DEFAULT 50, -- 优先级：1-100，数值越大越靠前
 show_in_detail BOOLEAN DEFAULT TRUE, -- 是否在明细查询中默认显示

 -- 枚举值同步配置（新增）
 enum_sync_config JSONB DEFAULT '{
   "top_n": 10000,
   "enabled": true,
   "strategy": "all",
   "min_frequency": 10,
   "include_all_with_synonyms": true
 }'::jsonb,
 
 -- 枚举角色标签（新增）
 -- dimension: 普通维度字段，用于过滤
 -- measure_filter: 度量相关枚举（分类限定），可入向量库
 enum_role VARCHAR(32) DEFAULT 'dimension',

 -- 状态
 is_active BOOLEAN DEFAULT TRUE,

 -- 时间戳
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 created_by UUID REFERENCES users(user_id),
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

 CONSTRAINT fields_priority_check CHECK (priority >= 1 AND priority <= 100)
);

CREATE INDEX idx_fields_connection ON fields(connection_id);
CREATE INDEX idx_fields_column ON fields(source_column_id);
CREATE INDEX idx_fields_type ON fields(field_type);
CREATE INDEX idx_fields_active ON fields(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_fields_auto ON fields(auto_detected) WHERE auto_detected = TRUE;
CREATE INDEX idx_fields_priority ON fields(priority DESC);
CREATE INDEX idx_fields_unit_conversion ON fields USING gin(unit_conversion);
CREATE INDEX idx_fields_show_in_detail ON fields(show_in_detail) WHERE show_in_detail = TRUE;

COMMENT ON TABLE fields IS '字段配置表（统一维度、度量、时间戳、标识字段）';
COMMENT ON COLUMN fields.default_aggregation IS '默认聚合函数（基础指标融合到字段）';
COMMENT ON COLUMN fields.auto_detected IS '是否通过AI自动识别';
COMMENT ON COLUMN fields.unit_conversion IS '单位转换配置：支持自定义显示单位和转换规则。格式：{"enabled":true,"display_unit":"公顷","conversion":{"factor":10000,"method":"divide","precision":2,"threshold":10000}}';
COMMENT ON COLUMN fields.priority IS '字段显示优先级（1-100，数值越大越靠前）用于控制明细查询时字段的显示顺序';
COMMENT ON COLUMN fields.show_in_detail IS '是否在明细查询中默认显示（仅当 is_active=true 时生效）';
COMMENT ON COLUMN fields.enum_role IS '枚举角色标签: dimension(普通维度), measure_filter(度量相关枚举)';
COMMENT ON COLUMN fields.enum_sync_config IS '枚举值同步配置:
- enabled: 是否启用同步
- strategy: 同步策略 (auto|top_n|all|manual)
- top_n: Top-N数量（strategy=top_n时生效）
- min_frequency: 最小频次（strategy=auto时生效）
- include_all_with_synonyms: 是否同步所有有同义词的枚举值';

-- ============================================================================
-- 第六层：枚举值管理
-- ============================================================================

-- 枚举值表（针对维度字段）
CREATE TABLE field_enum_values (
 enum_value_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 field_id UUID NOT NULL REFERENCES fields(field_id) ON DELETE CASCADE,

 -- 原始值与显示值
 original_value VARCHAR(200) NOT NULL,
 display_value VARCHAR(200),

 -- 同义词（用于模糊匹配）
 synonyms TEXT[],

 -- 统计信息
 frequency BIGINT DEFAULT 0, -- 出现频次

 -- 值包含关系（用于查询展开）
 includes_values TEXT[], -- 该枚举值包含的其他标准值列表

 -- 同步状态（新增）
 is_synced_to_milvus BOOLEAN DEFAULT FALSE,
 last_synced_at TIMESTAMP WITH TIME ZONE,

 -- 状态
 is_active BOOLEAN DEFAULT TRUE,

 -- 时间戳
 sampled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- 采样时间
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

 UNIQUE(field_id, original_value)
);

CREATE INDEX idx_enum_field ON field_enum_values(field_id);
CREATE INDEX idx_enum_value ON field_enum_values(original_value);
CREATE INDEX idx_enum_active ON field_enum_values(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_field_enum_values_synced ON field_enum_values(is_synced_to_milvus);
CREATE INDEX idx_field_enum_values_last_synced ON field_enum_values(last_synced_at);

COMMENT ON TABLE field_enum_values IS '枚举值表，针对维度字段';
COMMENT ON COLUMN field_enum_values.synonyms IS '同义词数组，用于模糊匹配';
COMMENT ON COLUMN field_enum_values.includes_values IS '该枚举值包含的其他标准值列表，用于查询时自动展开';
COMMENT ON COLUMN field_enum_values.is_synced_to_milvus IS '是否已同步到Milvus';
COMMENT ON COLUMN field_enum_values.last_synced_at IS '最后同步时间';

-- ============================================================================
-- 第七层：数据权限角色管理（全局角色 + 表级权限）
-- 说明：
-- - 数据角色（data_roles）：全局角色，不绑定特定连接
-- - 与系统角色（users.role）分离，实现"功能权限"与"数据权限"的解耦
-- - 支持从 OIDC/API 网关自动同步角色
-- 
-- 权限层级（简化架构）：
--   数据角色（全局）
--   ├── scope_type = 'all' → 可访问所有表
--   └── scope_type = 'limited' → 需配置具体表权限
--       └── role_table_permissions（表权限）
--           └── role_row_filters（行权限）
-- ============================================================================

-- 数据权限角色表（全局角色）
CREATE TABLE data_roles (
    role_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- 角色信息
    role_name VARCHAR(100) NOT NULL,
    role_code VARCHAR(50) NOT NULL,           -- 角色编码，如 'sales_manager', 'regional_viewer'
    description TEXT,
    
    -- 范围类型：'all'=全量访问所有连接, 'limited'=受限访问（需配置具体连接）
    scope_type VARCHAR(20) NOT NULL DEFAULT 'limited',
    
    -- 是否为默认角色（新用户自动分配）
    is_default BOOLEAN DEFAULT FALSE,
    
    -- 状态
    is_active BOOLEAN DEFAULT TRUE,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 角色编码全局唯一
    CONSTRAINT uq_data_roles_role_code UNIQUE (role_code),
    -- 检查范围类型
    CONSTRAINT chk_data_roles_scope_type CHECK (scope_type IN ('all', 'limited'))
);

CREATE INDEX idx_data_roles_active ON data_roles(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_data_roles_scope ON data_roles(scope_type);

COMMENT ON TABLE data_roles IS '数据权限角色表（全局） - 用于配置表级和行级数据访问权限';
COMMENT ON COLUMN data_roles.scope_type IS '范围类型: all=全量访问所有连接, limited=受限访问需配置连接';

-- [已删除] role_connection_scopes 表
-- 新架构中移除了连接权限层，权限直接在表级别控制
-- 数据角色通过 scope_type='all' 或配置 role_table_permissions 来控制访问

-- 角色表权限表
CREATE TABLE role_table_permissions (
    permission_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role_id UUID NOT NULL REFERENCES data_roles(role_id) ON DELETE CASCADE,
    table_id UUID NOT NULL REFERENCES db_tables(table_id) ON DELETE CASCADE,
    
    -- === 基础权限 ===
    can_query BOOLEAN DEFAULT TRUE,           -- 是否可查询
    can_export BOOLEAN DEFAULT FALSE,         -- 是否可导出
    
    -- === 列可见性控制（黑白名单机制） ===
    -- 默认策略：'whitelist'=白名单模式(默认全不可见), 'blacklist'=黑名单模式(默认全可见)
    column_access_mode VARCHAR(20) DEFAULT 'blacklist',
    
    -- 白名单：明确允许访问的字段ID（当 mode='whitelist' 时生效）
    -- 引用 fields.field_id（仅is_active=true的字段有效）
    included_column_ids UUID[],
    
    -- 黑名单：明确禁止访问的字段ID（当 mode='blacklist' 时生效）
    -- 引用 fields.field_id（仅is_active=true的字段有效）
    excluded_column_ids UUID[],
    
    -- === 列脱敏控制 ===
    -- 需要脱敏显示的字段ID（可见但内容脱敏）
    masked_column_ids UUID[],
    
    -- === 列使用限制（防推断攻击） ===
    -- 禁止用于WHERE条件的字段ID
    restricted_filter_column_ids UUID[],
    -- 禁止用于聚合函数(SUM/AVG/MAX/MIN/COUNT DISTINCT)的字段ID
    restricted_aggregate_column_ids UUID[],
    -- 禁止用于GROUP BY的字段ID
    restricted_group_by_column_ids UUID[],
    -- 禁止用于ORDER BY的字段ID
    restricted_order_by_column_ids UUID[],
    
    -- === 时间戳 ===
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- === 约束 ===
    UNIQUE(role_id, table_id),
    CONSTRAINT valid_column_access_mode CHECK (column_access_mode IN ('whitelist', 'blacklist'))
);

CREATE INDEX idx_role_table_perm_role ON role_table_permissions(role_id);
CREATE INDEX idx_role_table_perm_table ON role_table_permissions(table_id);

COMMENT ON TABLE role_table_permissions IS '角色表权限 - 控制角色可访问的表和列';
COMMENT ON COLUMN role_table_permissions.column_access_mode IS '列访问模式: whitelist=白名单(默认不可见), blacklist=黑名单(默认可见)';
COMMENT ON COLUMN role_table_permissions.included_column_ids IS '白名单模式下允许访问的字段ID，引用 fields.field_id';
COMMENT ON COLUMN role_table_permissions.excluded_column_ids IS '黑名单模式下禁止访问的字段ID，引用 fields.field_id';
COMMENT ON COLUMN role_table_permissions.restricted_filter_column_ids IS '禁止在WHERE中使用的字段，防止推断攻击';

-- 角色行级过滤规则表
CREATE TABLE role_row_filters (
    filter_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role_id UUID NOT NULL REFERENCES data_roles(role_id) ON DELETE CASCADE,
    
    -- 过滤规则名称
    filter_name VARCHAR(100) NOT NULL,
    description TEXT,
    
    -- 作用的表（NULL表示应用到所有有权限的表）
    table_id UUID REFERENCES db_tables(table_id) ON DELETE CASCADE,
    
    -- 过滤条件定义（JSON格式）
    filter_definition JSONB NOT NULL,
    
    -- 优先级（数值越大优先级越高）
    priority INT DEFAULT 0,
    
    -- 状态
    is_active BOOLEAN DEFAULT TRUE,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_role_row_filters_role ON role_row_filters(role_id);
CREATE INDEX idx_role_row_filters_table ON role_row_filters(table_id);
CREATE INDEX idx_role_row_filters_active ON role_row_filters(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE role_row_filters IS '角色行级过滤规则 - 配置角色的数据行过滤条件';
COMMENT ON COLUMN role_row_filters.filter_definition IS '过滤条件JSON格式：{"conditions":[{"field_name":"dept","operator":"=","value_type":"user_attr","value":"dept_code"}],"logic":"AND"}';

-- RLS规则模板表
CREATE TABLE rls_rule_templates (
    template_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- 模板基本信息
    template_name VARCHAR(100) NOT NULL,
    template_code VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    category VARCHAR(50),
    
    -- 模板定义
    template_definition JSONB NOT NULL,
    required_params JSONB,
    optional_params JSONB,
    
    -- 示例
    example_params JSONB,
    example_sql TEXT,
    
    -- 适用范围
    applicable_tables TEXT[],
    
    -- 状态
    is_system BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE rls_rule_templates IS 'RLS规则模板 - 预置常用过滤规则模板';

-- 用户-数据角色关联表
CREATE TABLE user_data_roles (
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES data_roles(role_id) ON DELETE CASCADE,
    
    -- 授权信息
    granted_by UUID REFERENCES users(user_id),
    granted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 有效期（NULL表示永久有效）
    expires_at TIMESTAMP WITH TIME ZONE,
    
    -- 状态
    is_active BOOLEAN DEFAULT TRUE,
    
    PRIMARY KEY (user_id, role_id)
);

CREATE INDEX idx_user_data_roles_user ON user_data_roles(user_id);
CREATE INDEX idx_user_data_roles_role ON user_data_roles(role_id);
CREATE INDEX idx_user_data_roles_active ON user_data_roles(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE user_data_roles IS '用户数据角色关联表';

-- 组织-数据角色关联表（组织层面分配数据权限）
CREATE TABLE org_data_roles (
    org_id UUID NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES data_roles(role_id) ON DELETE CASCADE,
    
    -- 授权信息
    granted_by UUID REFERENCES users(user_id) ON DELETE SET NULL,
    granted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 是否继承给子组织
    inherit_to_children BOOLEAN DEFAULT TRUE,
    
    -- 状态
    is_active BOOLEAN DEFAULT TRUE,
    
    PRIMARY KEY (org_id, role_id)
);

CREATE INDEX idx_org_data_roles_org ON org_data_roles(org_id);
CREATE INDEX idx_org_data_roles_role ON org_data_roles(role_id);
CREATE INDEX idx_org_data_roles_active ON org_data_roles(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE org_data_roles IS '组织-数据角色关联表（组织层面分配数据权限）';
COMMENT ON COLUMN org_data_roles.inherit_to_children IS '是否继承给子组织（TRUE=子组织也拥有此角色）';
COMMENT ON COLUMN org_data_roles.granted_by IS '授权人';
COMMENT ON COLUMN org_data_roles.granted_at IS '授权时间';

-- 权限审计日志表
CREATE TABLE permission_audit_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- 用户信息
    user_id UUID REFERENCES users(user_id),
    username VARCHAR(100),
    
    -- 查询信息
    query_id UUID,
    connection_id UUID REFERENCES database_connections(connection_id),
    original_question TEXT,
    
    -- 应用的权限
    applied_roles UUID[],
    applied_table_filters JSONB,
    applied_row_filters JSONB,
    applied_column_masks JSONB,
    
    -- 生成的SQL
    generated_sql TEXT,
    
    -- 结果
    result_row_count INT,
    execution_time_ms INT,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_permission_audit_user ON permission_audit_log(user_id);
CREATE INDEX idx_permission_audit_connection ON permission_audit_log(connection_id);
CREATE INDEX idx_permission_audit_time ON permission_audit_log(created_at DESC);

COMMENT ON TABLE permission_audit_log IS '权限审计日志 - 记录查询时应用的权限';

-- 预置RLS规则模板
INSERT INTO rls_rule_templates (template_code, template_name, category, description, template_definition, required_params, example_params, example_sql, is_system) VALUES
('dept_isolation', '部门数据隔离', 'dept', '用户只能看到自己部门的数据', 
 '{"conditions": [{"field_name": "{{dept_field}}", "operator": "=", "value_type": "user_attr", "value": "dept_code"}], "logic": "AND"}',
 '{"dept_field": "部门字段名"}', '{"dept_field": "dept_code"}', 'WHERE dept_code = 用户.dept_code', TRUE),
('region_filter', '区域数据过滤', 'region', '用户只能看到授权区域的数据',
 '{"conditions": [{"field_name": "{{region_field}}", "operator": "IN", "value_type": "user_attr", "value": "allowed_regions"}], "logic": "AND"}',
 '{"region_field": "区域字段名"}', '{"region_field": "region"}', 'WHERE region IN (用户.allowed_regions)', TRUE),
('owner_only', '仅查看自己的数据', 'owner', '用户只能看到自己创建的数据',
 '{"conditions": [{"field_name": "{{owner_field}}", "operator": "=", "value_type": "user_attr", "value": "user_id"}], "logic": "AND"}',
 '{"owner_field": "创建人字段"}', '{"owner_field": "created_by"}', 'WHERE created_by = 用户.user_id', TRUE),
('status_filter', '状态过滤', 'status', '只能看到特定状态的数据',
 '{"conditions": [{"field_name": "{{status_field}}", "operator": "IN", "value_type": "static", "value": "{{allowed_statuses}}"}], "logic": "AND"}',
 '{"status_field": "状态字段", "allowed_statuses": "状态值列表"}', '{"status_field": "status", "allowed_statuses": ["published"]}', 'WHERE status IN (''published'')', TRUE),
('recent_days', '时间范围过滤', 'time', '只能看到最近N天的数据',
 '{"conditions": [{"field_name": "{{date_field}}", "operator": ">=", "value_type": "expression", "value": "CURRENT_DATE - INTERVAL ''{{days}} days''"}], "logic": "AND"}',
 '{"date_field": "日期字段", "days": "天数"}', '{"date_field": "created_at", "days": "90"}', 'WHERE created_at >= CURRENT_DATE - INTERVAL ''90 days''', TRUE),
('dept_region_combo', '部门+区域组合', 'combo', '同时按部门和区域过滤',
 '{"conditions": [{"field_name": "{{dept_field}}", "operator": "=", "value_type": "user_attr", "value": "dept_code"}, {"field_name": "{{region_field}}", "operator": "IN", "value_type": "user_attr", "value": "allowed_regions"}], "logic": "AND"}',
 '{"dept_field": "部门字段", "region_field": "区域字段"}', '{"dept_field": "dept_code", "region_field": "region"}', 'WHERE dept_code = 用户.dept_code AND region IN (用户.allowed_regions)', TRUE);

-- ============================================================================
-- 第十二层：Few-Shot SQL 问答样本
-- ============================================================================

CREATE TABLE IF NOT EXISTS qa_few_shot_samples (
    sample_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES database_connections(connection_id) ON DELETE CASCADE,
    question TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    ir_json JSONB,
    tables TEXT[] DEFAULT ARRAY[]::text[],
    tables_json JSONB DEFAULT '[]'::jsonb,
    domain_id UUID REFERENCES business_domains(domain_id) ON DELETE SET NULL,
    quality_score NUMERIC(3,2) DEFAULT 0.80,
    source_tag VARCHAR(64),
    sample_type VARCHAR(32) DEFAULT 'standard',
    sql_context TEXT,
    error_msg TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    last_verified_at TIMESTAMP WITH TIME ZONE,
    is_verified BOOLEAN DEFAULT TRUE,  -- 是否已人工校验
    notes TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_by UUID REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT qa_few_shot_samples_quality_check CHECK (quality_score >= 0 AND quality_score <= 1)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_few_shot_unique_q ON qa_few_shot_samples (connection_id, question, sql_text);
CREATE INDEX IF NOT EXISTS idx_few_shot_connection ON qa_few_shot_samples (connection_id);
CREATE INDEX IF NOT EXISTS idx_few_shot_domain ON qa_few_shot_samples (domain_id);
CREATE INDEX IF NOT EXISTS idx_few_shot_quality ON qa_few_shot_samples (quality_score);

COMMENT ON TABLE qa_few_shot_samples IS 'Few-Shot SQL 问答对样本表';
COMMENT ON COLUMN qa_few_shot_samples.tables IS '解析到的逻辑表列表（text[]）';
COMMENT ON COLUMN qa_few_shot_samples.tables_json IS '表列表的JSON表示，供Milvus过滤';
COMMENT ON COLUMN qa_few_shot_samples.ir_json IS 'IR中间表示的JSON格式，用于Few-Shot示例展示';
COMMENT ON COLUMN qa_few_shot_samples.quality_score IS '样本质量分（0-1）';
COMMENT ON COLUMN qa_few_shot_samples.sample_type IS '样本类型：standard（标准）/correction（纠错）';
COMMENT ON COLUMN qa_few_shot_samples.sql_context IS 'SQL 模板或 UUID 映射上下文，用于短码解析';
COMMENT ON COLUMN qa_few_shot_samples.error_msg IS '纠错样本的历史错误信息';

-- 确保 ir_json 字段存在（用于已存在的数据库）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'qa_few_shot_samples' 
        AND column_name = 'ir_json'
    ) THEN
        ALTER TABLE qa_few_shot_samples ADD COLUMN ir_json JSONB;
        COMMENT ON COLUMN qa_few_shot_samples.ir_json IS 'IR中间表示的JSON格式，用于Few-Shot示例展示';
        RAISE NOTICE '已添加 ir_json 字段到 qa_few_shot_samples 表';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'qa_few_shot_samples'
          AND column_name = 'sample_type'
    ) THEN
        ALTER TABLE qa_few_shot_samples ADD COLUMN sample_type VARCHAR(32) DEFAULT 'standard';
        COMMENT ON COLUMN qa_few_shot_samples.sample_type IS '样本类型：standard（标准）/correction（纠错）';
        UPDATE qa_few_shot_samples SET sample_type = COALESCE(metadata->>'sample_type', 'standard');
        RAISE NOTICE '已添加 sample_type 字段到 qa_few_shot_samples 表';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'qa_few_shot_samples'
          AND column_name = 'sql_context'
    ) THEN
        ALTER TABLE qa_few_shot_samples ADD COLUMN sql_context TEXT;
        COMMENT ON COLUMN qa_few_shot_samples.sql_context IS 'SQL 模板或 UUID 映射上下文，用于短码解析';
        UPDATE qa_few_shot_samples SET sql_context = metadata->>'sql_context';
        RAISE NOTICE '已添加 sql_context 字段到 qa_few_shot_samples 表';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'qa_few_shot_samples'
          AND column_name = 'error_msg'
    ) THEN
        ALTER TABLE qa_few_shot_samples ADD COLUMN error_msg TEXT;
        COMMENT ON COLUMN qa_few_shot_samples.error_msg IS '纠错样本的历史错误信息';
        UPDATE qa_few_shot_samples SET error_msg = metadata->>'error_msg';
        RAISE NOTICE '已添加 error_msg 字段到 qa_few_shot_samples 表';
    END IF;
END $$;
COMMENT ON COLUMN qa_few_shot_samples.source_tag IS '样本来源标签，如manual、feedback等';

-- 注意：update_few_shot_samples_updated_at 触发器需在 update_updated_at_column()
-- 函数创建后再添加，避免空库初始化时报函数不存在

CREATE TABLE IF NOT EXISTS qa_few_shot_feedback (
    feedback_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sample_id UUID NOT NULL REFERENCES qa_few_shot_samples(sample_id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(user_id),
    rating INT CHECK (rating BETWEEN 1 AND 5),
    is_helpful BOOLEAN,
    comment TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_few_shot_feedback_sample ON qa_few_shot_feedback (sample_id);

COMMENT ON TABLE qa_few_shot_feedback IS 'Few-Shot 样本反馈表';
COMMENT ON COLUMN qa_few_shot_feedback.rating IS '1-5的评分，辅助质量评估';

-- ============================================================================
-- 第七层：元数据变更日志
-- ============================================================================

CREATE TABLE metadata_change_log (
 change_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 table_name VARCHAR(100) NOT NULL,
 record_id VARCHAR(100) NOT NULL,
 operation VARCHAR(20) NOT NULL,
 old_snapshot JSONB,
 new_snapshot JSONB,
 changed_by VARCHAR(100),
 change_reason TEXT,
 changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 diff JSONB
);

CREATE INDEX IF NOT EXISTS idx_change_log_table ON metadata_change_log(table_name);
CREATE INDEX IF NOT EXISTS idx_change_log_record ON metadata_change_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_change_log_date ON metadata_change_log(changed_at DESC);

-- ============================================================================
-- 第八层：表关系（自动识别+手动确认）
-- ============================================================================

-- 表关系表
CREATE TABLE table_relationships (
 relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 connection_id UUID NOT NULL REFERENCES database_connections(connection_id) ON DELETE CASCADE,

 -- 关系两端
 left_table_id UUID NOT NULL REFERENCES db_tables(table_id) ON DELETE CASCADE,
 right_table_id UUID NOT NULL REFERENCES db_tables(table_id) ON DELETE CASCADE,

 -- JOIN条件
 left_column_id UUID NOT NULL REFERENCES db_columns(column_id) ON DELETE CASCADE,
 right_column_id UUID NOT NULL REFERENCES db_columns(column_id) ON DELETE CASCADE,

 -- 关系类型
 relationship_type VARCHAR(20) NOT NULL DEFAULT 'one_to_many',
 -- 'one_to_one', 'one_to_many', 'many_to_many'

 -- JOIN类型
 join_type VARCHAR(20) DEFAULT 'INNER', -- 'INNER', 'LEFT', 'RIGHT'

 -- 自动识别状态
 detection_method VARCHAR(50), -- 'foreign_key', 'name_similarity', 'data_analysis', 'manual'
 confidence_score NUMERIC(3,2), -- 识别置信度 0-1
 is_confirmed BOOLEAN DEFAULT FALSE, -- 是否已人工确认

 -- 业务信息
 relationship_name VARCHAR(100), -- '订单-客户关系'
 description TEXT,

 -- 状态
 is_active BOOLEAN DEFAULT TRUE,

 -- 时间戳
 detected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 confirmed_at TIMESTAMP WITH TIME ZONE,
 confirmed_by UUID REFERENCES users(user_id),
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_relationships_connection ON table_relationships(connection_id);
CREATE INDEX idx_relationships_left ON table_relationships(left_table_id);
CREATE INDEX idx_relationships_right ON table_relationships(right_table_id);
CREATE INDEX idx_relationships_confirmed ON table_relationships(is_confirmed);
CREATE INDEX idx_relationships_active ON table_relationships(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE table_relationships IS '表关系，自动识别+手动确认';
COMMENT ON COLUMN table_relationships.detection_method IS '识别方法：外键/名称相似度/数据分析/手动';
COMMENT ON COLUMN table_relationships.is_confirmed IS '是否已人工确认';

-- ============================================================================
-- 第九层：全局规则（派生指标、枚举值展开、默认过滤、自定义规则）
-- ============================================================================

-- 全局规则表（可不绑定特定连接）
CREATE TABLE global_rules (
 rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 -- connection_id 改为可选，NULL 表示全局规则（跨连接共享）
 connection_id UUID REFERENCES database_connections(connection_id) ON DELETE SET NULL,

 -- 规则类型
 rule_type VARCHAR(50) NOT NULL,
 -- 'derived_metric': 派生指标（基于基础字段计算的虚拟指标）
 -- 'enum_expansion': 枚举值展开（层级数据映射，如武汉市→16个区）
 -- 'default_filter': 默认过滤（表级自动过滤条件）
 -- 'custom_instruction': 自定义规则（自然语言文本，直接传递给大模型）

 -- 规则名称
 rule_name VARCHAR(100) NOT NULL,
 description TEXT,

 -- 规则定义（JSONB格式，不同类型结构不同）
 rule_definition JSONB NOT NULL,
 -- 示例（派生指标）:
 -- {
 -- "metric_id": "derived_profit",
 -- "display_name": "利润",
 -- "formula": "SUM({revenue}) - SUM({cost})",
 -- "field_dependencies": [
 -- {"placeholder": "revenue", "field_id": "field_xxx", "aggregation": "SUM"}
 -- ],
 -- "unit": "元"
 -- }
 -- 示例（枚举值展开）:
 -- {
 -- "field_id": "field_district",
 -- "parent_value": "武汉市",
 -- "parent_synonyms": ["武汉", "wuhan"],
 -- "child_values": ["江岸区", "江汉区", ...]
 -- }
 -- 示例（默认过滤）:
 -- {
 -- "table_id": "table_orders",
 -- "filter_field": "approvestate",
 -- "filter_operator": "=",
 -- "filter_value": "已审核"
 -- }
 -- 示例（自定义规则）:
 -- {
 -- "instruction": "当用户询问周末数据时...",
 -- "trigger_keywords": ["周末", "weekend"]
 -- }

 -- 作用范围（支持多业务域）
 scope VARCHAR(20) DEFAULT 'global', -- 'global' 或 'domain'
 domain_id UUID REFERENCES business_domains(domain_id), -- 向后兼容，单个业务域
 domain_ids UUID[], -- 关联的业务域ID列表（支持多选）

 -- 优先级
 priority INT DEFAULT 0,

 -- 状态
 is_active BOOLEAN DEFAULT TRUE,

 -- 时间戳
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
 created_by UUID REFERENCES users(user_id),
 updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

 -- 规则名称在同类型中唯一（移除 connection_id 的联合唯一约束）
 UNIQUE(rule_type, rule_name)
);

CREATE INDEX idx_rules_connection ON global_rules(connection_id) WHERE connection_id IS NOT NULL;
CREATE INDEX idx_rules_type ON global_rules(rule_type);
CREATE INDEX idx_rules_active ON global_rules(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_rules_definition ON global_rules USING gin(rule_definition);
CREATE INDEX idx_rules_scope ON global_rules(scope);
CREATE INDEX idx_rules_domain ON global_rules(domain_id) WHERE domain_id IS NOT NULL;
CREATE INDEX idx_rules_domain_ids ON global_rules USING gin(domain_ids);

COMMENT ON TABLE global_rules IS '全局规则：派生指标、枚举值展开、默认过滤、自定义规则（支持多业务域）';
COMMENT ON COLUMN global_rules.connection_id IS '可选，NULL 表示全局规则，不绑定特定连接';
COMMENT ON COLUMN global_rules.rule_type IS '规则类型：derived_metric(派生指标)/enum_expansion(枚举值展开)/default_filter(默认过滤)/custom_instruction(自定义规则)';
COMMENT ON COLUMN global_rules.rule_definition IS 'JSONB格式的规则定义，灵活扩展';
COMMENT ON COLUMN global_rules.scope IS '作用范围：global(全局)/domain(特定业务域)';
COMMENT ON COLUMN global_rules.domain_ids IS '关联的业务域ID列表（支持多选）';
COMMENT ON COLUMN global_rules.priority IS '优先级，数值越大优先级越高';

-- ============================================================================
-- 第十层：查询历史
-- ============================================================================

-- 查询历史表
CREATE TABLE query_history (
 query_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 connection_id UUID REFERENCES database_connections(connection_id) ON DELETE SET NULL,
 user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,

 -- 查询信息
 original_question TEXT NOT NULL,
 generated_sql TEXT,

 -- 意图识别结果（JSON）
 intent_detection_result JSONB,
 -- {
 -- "domain": {...},
 -- "tables": [...],
 -- "fields": {...},
 -- "strategy": "domain_aware"
 -- }

 -- 执行信息
 execution_status VARCHAR(20), -- 'success', 'failed', 'cancelled'
 execution_time_ms INT,
 result_row_count INT,
 error_message TEXT,

 -- 质量评分
 quality_score NUMERIC(3,2),
 user_feedback VARCHAR(20), -- 'good', 'bad', 'neutral'

 -- 时间戳
 created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_history_connection ON query_history(connection_id);
CREATE INDEX idx_history_user ON query_history(user_id);
CREATE INDEX idx_history_created ON query_history(created_at DESC);
CREATE INDEX idx_history_status ON query_history(execution_status);

COMMENT ON TABLE query_history IS '查询历史记录';
COMMENT ON COLUMN query_history.intent_detection_result IS '意图识别结果（JSON）';

-- ============================================================================
-- 第八层：模型供应商与提示词模板
-- 说明：
-- - 当前后台已启用模型供应商管理、场景模型配置、提示词模板管理
-- - 这些表需在空库初始化时直接创建，避免后台进入相关页面时报缺表
-- ============================================================================

CREATE TABLE IF NOT EXISTS model_providers (
    provider_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_name VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(100) NOT NULL,
    provider_type VARCHAR(50) NOT NULL,
    base_url VARCHAR(500),
    icon VARCHAR(100),
    description TEXT,
    is_enabled BOOLEAN DEFAULT TRUE,
    is_valid BOOLEAN DEFAULT FALSE,
    last_validated_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id) ON DELETE SET NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_model_providers_enabled
    ON model_providers(is_enabled) WHERE is_enabled = TRUE;
CREATE INDEX IF NOT EXISTS idx_model_providers_type
    ON model_providers(provider_type);

COMMENT ON TABLE model_providers IS '模型供应商表';

CREATE TABLE IF NOT EXISTS provider_credentials (
    credential_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_id UUID NOT NULL REFERENCES model_providers(provider_id) ON DELETE CASCADE,
    credential_name VARCHAR(100) NOT NULL,
    encrypted_api_key TEXT NOT NULL,
    extra_config JSONB DEFAULT '{}'::jsonb,
    is_active BOOLEAN DEFAULT TRUE,
    is_default BOOLEAN DEFAULT FALSE,
    total_requests BIGINT DEFAULT 0,
    total_tokens BIGINT DEFAULT 0,
    last_used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_provider_credentials_name UNIQUE (provider_id, credential_name)
);

CREATE INDEX IF NOT EXISTS idx_provider_credentials_provider
    ON provider_credentials(provider_id);
CREATE INDEX IF NOT EXISTS idx_provider_credentials_active
    ON provider_credentials(provider_id, is_active);
CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_credentials_default
    ON provider_credentials(provider_id) WHERE is_default = TRUE;

COMMENT ON TABLE provider_credentials IS '模型供应商凭证表';
COMMENT ON COLUMN provider_credentials.encrypted_api_key IS '加密存储的 API Key';

CREATE TABLE IF NOT EXISTS provider_models (
    model_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider_id UUID NOT NULL REFERENCES model_providers(provider_id) ON DELETE CASCADE,
    model_name VARCHAR(100) NOT NULL,
    display_name VARCHAR(100),
    model_type VARCHAR(32) NOT NULL,
    supports_function_calling BOOLEAN DEFAULT FALSE,
    supports_json_mode BOOLEAN DEFAULT FALSE,
    supports_streaming BOOLEAN DEFAULT TRUE,
    supports_vision BOOLEAN DEFAULT FALSE,
    context_window INT,
    max_output_tokens INT,
    default_temperature NUMERIC(4,2) DEFAULT 0.0,
    default_top_p NUMERIC(4,2) DEFAULT 1.0,
    default_max_tokens INT DEFAULT 2048,
    is_enabled BOOLEAN DEFAULT TRUE,
    is_custom BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_provider_models_name UNIQUE (provider_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_provider_models_provider
    ON provider_models(provider_id);
CREATE INDEX IF NOT EXISTS idx_provider_models_type
    ON provider_models(model_type);
CREATE INDEX IF NOT EXISTS idx_provider_models_enabled
    ON provider_models(is_enabled) WHERE is_enabled = TRUE;

COMMENT ON TABLE provider_models IS '供应商模型表';

CREATE TABLE IF NOT EXISTS scenario_model_configs (
    config_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario VARCHAR(50) NOT NULL,
    model_id UUID REFERENCES provider_models(model_id) ON DELETE SET NULL,
    credential_id UUID REFERENCES provider_credentials(credential_id) ON DELETE SET NULL,
    temperature NUMERIC(4,2),
    top_p NUMERIC(4,2),
    max_tokens INT,
    timeout_seconds INT DEFAULT 60,
    max_retries INT DEFAULT 2,
    extra_params JSONB DEFAULT '{}'::jsonb,
    priority INT DEFAULT 0,
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_scenario_model_configs UNIQUE (scenario, priority)
);

CREATE INDEX IF NOT EXISTS idx_scenario_model_configs_enabled
    ON scenario_model_configs(is_enabled) WHERE is_enabled = TRUE;
CREATE INDEX IF NOT EXISTS idx_scenario_model_configs_model
    ON scenario_model_configs(model_id);

COMMENT ON TABLE scenario_model_configs IS '模型场景配置表';

CREATE TABLE IF NOT EXISTS prompt_templates (
    template_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scenario VARCHAR(50) NOT NULL,
    prompt_type VARCHAR(50) NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    content TEXT NOT NULL,
    version INT DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_by UUID REFERENCES users(user_id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_prompt_templates UNIQUE (scenario, prompt_type)
);

CREATE INDEX IF NOT EXISTS idx_prompt_templates_active
    ON prompt_templates(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_prompt_templates_scenario
    ON prompt_templates(scenario, prompt_type);

COMMENT ON TABLE prompt_templates IS '提示词模板表';

CREATE TABLE IF NOT EXISTS prompt_template_history (
    history_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    template_id UUID NOT NULL REFERENCES prompt_templates(template_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    version INT NOT NULL,
    change_reason TEXT,
    changed_by UUID REFERENCES users(user_id) ON DELETE SET NULL,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prompt_template_history_template
    ON prompt_template_history(template_id, version DESC);

COMMENT ON TABLE prompt_template_history IS '提示词模板历史表';

-- ============================================================================
-- 第九层：多轮会话与活跃查询
-- 说明：
-- - 当前查询前台已启用 conversations 路由
-- - 需要会话、消息、活跃查询三张表支撑多轮对话与取消查询能力
-- ============================================================================

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    title VARCHAR(255),
    connection_id UUID REFERENCES database_connections(connection_id) ON DELETE SET NULL,
    domain_id UUID REFERENCES business_domains(domain_id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT TRUE,
    is_pinned BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_message_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_conversations_user_active
    ON conversations(user_id, is_active);
CREATE INDEX IF NOT EXISTS idx_conversations_user_last_message
    ON conversations(user_id, last_message_at DESC);

COMMENT ON TABLE conversations IS '多轮会话表';

CREATE TABLE IF NOT EXISTS conversation_messages (
    message_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    query_id UUID,
    sql_text TEXT,
    result_summary TEXT,
    result_data JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'completed',
    error_message TEXT,
    query_params JSONB,
    context_message_ids UUID[],
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_conversation_messages_conversation
    ON conversation_messages(conversation_id, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_conversation_messages_query
    ON conversation_messages(query_id);
CREATE INDEX IF NOT EXISTS idx_conversation_messages_status
    ON conversation_messages(status);

COMMENT ON TABLE conversation_messages IS '会话消息表';

CREATE TABLE IF NOT EXISTS active_queries (
    query_id UUID PRIMARY KEY,
    conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
    message_id UUID REFERENCES conversation_messages(message_id) ON DELETE SET NULL,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    query_text TEXT NOT NULL,
    connection_id UUID REFERENCES database_connections(connection_id) ON DELETE SET NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    ws_connection_id VARCHAR(255),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_active_queries_user_status
    ON active_queries(user_id, status);
CREATE INDEX IF NOT EXISTS idx_active_queries_conversation
    ON active_queries(conversation_id);

COMMENT ON TABLE active_queries IS '活跃查询状态表';

CREATE TABLE IF NOT EXISTS query_sessions (
    query_id UUID PRIMARY KEY,
    conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
    message_id UUID REFERENCES conversation_messages(message_id) ON DELETE SET NULL,
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    status VARCHAR(32) NOT NULL,
    current_node VARCHAR(64) NOT NULL,
    state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_query_sessions_user_status
    ON query_sessions(user_id, status);
CREATE INDEX IF NOT EXISTS idx_query_sessions_conversation
    ON query_sessions(conversation_id);
CREATE INDEX IF NOT EXISTS idx_query_sessions_updated
    ON query_sessions(updated_at DESC);

COMMENT ON TABLE query_sessions IS '查询会话状态表';

CREATE TABLE IF NOT EXISTS draft_actions (
    action_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID NOT NULL REFERENCES query_sessions(query_id) ON DELETE CASCADE,
    draft_id UUID,
    draft_version INT,
    action_type VARCHAR(64) NOT NULL,
    actor_type VARCHAR(32) NOT NULL,
    actor_id VARCHAR(128) NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_draft_actions_query_idempotency
    ON draft_actions(query_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_draft_actions_query_created
    ON draft_actions(query_id, created_at DESC);

COMMENT ON TABLE draft_actions IS '查询动作时间线表';

CREATE TABLE IF NOT EXISTS learning_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_key VARCHAR(128) NOT NULL,
    query_id UUID REFERENCES query_sessions(query_id) ON DELETE SET NULL,
    conversation_id UUID REFERENCES conversations(conversation_id) ON DELETE SET NULL,
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    event_type VARCHAR(64) NOT NULL,
    event_version INT NOT NULL DEFAULT 1,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_component VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_learning_events_event_key
    ON learning_events(event_key);
CREATE INDEX IF NOT EXISTS idx_learning_events_query_created
    ON learning_events(query_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_learning_events_type_created
    ON learning_events(event_type, created_at DESC);

COMMENT ON TABLE learning_events IS '统一学习事件事实表';

-- ============================================================================
-- 初始化数据
-- ============================================================================

-- 创建默认认证提供者（本地认证）
INSERT INTO auth_providers (provider_key, provider_type, config_json, enabled, priority)
VALUES
  ('local', 'local', '{}'::jsonb, TRUE, 100);

-- 创建默认管理员用户（密码: admin123，实际使用时需要修改）
INSERT INTO users (username, password_hash, email, full_name, role, is_active)
VALUES
 ('admin', '$2b$12$lwwMKTLiqkaXrD/Xh2oeNu1FUNMnR/ZN2perhJI.3tRa0iQPm54vi', 'admin@example.com', '系统管理员', 'admin', TRUE),
 ('viewer', '$2b$12$lwwMKTLiqkaXrD/Xh2oeNu1FUNMnR/ZN2perhJI.3tRa0iQPm54vi', 'viewer@example.com', '访客', 'viewer', TRUE);

-- ============================================================================
-- 触发器：自动更新updated_at
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
 NEW.updated_at = CURRENT_TIMESTAMP;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_updated_at_column() IS '自动更新updated_at字段的触发器函数';

-- 为各表添加更新触发器
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_connections_updated_at BEFORE UPDATE ON database_connections
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_domains_updated_at BEFORE UPDATE ON business_domains
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_tables_updated_at BEFORE UPDATE ON db_tables
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_columns_updated_at BEFORE UPDATE ON db_columns
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fields_updated_at BEFORE UPDATE ON fields
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_enum_updated_at BEFORE UPDATE ON field_enum_values
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_relationships_updated_at BEFORE UPDATE ON table_relationships
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rules_updated_at BEFORE UPDATE ON global_rules
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_model_providers_updated_at BEFORE UPDATE ON model_providers
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_provider_credentials_updated_at BEFORE UPDATE ON provider_credentials
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_provider_models_updated_at BEFORE UPDATE ON provider_models
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_scenario_model_configs_updated_at BEFORE UPDATE ON scenario_model_configs
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_prompt_templates_updated_at BEFORE UPDATE ON prompt_templates
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_conversations_updated_at BEFORE UPDATE ON conversations
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_conversation_messages_updated_at BEFORE UPDATE ON conversation_messages
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_query_sessions_updated_at BEFORE UPDATE ON query_sessions
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_few_shot_samples_updated_at ON qa_few_shot_samples;
CREATE TRIGGER update_few_shot_samples_updated_at BEFORE UPDATE ON qa_few_shot_samples
 FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Schema扩展：视图
-- ============================================================================

-- 创建连接统计视图（便于快速查询）
CREATE OR REPLACE VIEW connection_stats AS
SELECT
 c.connection_id,
 c.connection_name,
 COUNT(DISTINCT t.table_id) AS table_count,
 COUNT(DISTINCT CASE WHEN t.is_included THEN t.table_id END) AS active_table_count,
 COUNT(DISTINCT CASE WHEN t.display_name IS NOT NULL THEN t.table_id END) AS configured_table_count,
 COUNT(DISTINCT f.field_id) AS field_count,
 COUNT(DISTINCT CASE WHEN f.auto_detected = FALSE THEN f.field_id END) AS configured_field_count,
 COUNT(DISTINCT d.domain_id) AS domain_count,
 COUNT(DISTINCT r.relationship_id) AS join_count,
 COUNT(DISTINCT gr.rule_id) AS rule_count
FROM database_connections c
LEFT JOIN db_tables t ON c.connection_id = t.connection_id
LEFT JOIN db_columns col ON t.table_id = col.table_id
LEFT JOIN fields f ON col.column_id = f.source_column_id
LEFT JOIN business_domains d ON c.connection_id = d.connection_id AND d.is_active = TRUE
LEFT JOIN table_relationships r ON c.connection_id = r.connection_id AND r.is_active = TRUE
LEFT JOIN global_rules gr ON c.connection_id = gr.connection_id AND gr.is_active = TRUE
GROUP BY c.connection_id, c.connection_name;

COMMENT ON VIEW connection_stats IS '数据库连接统计视图';

-- 注意：以下两个视图依赖 milvus_pending_changes / milvus_sync_history，
-- 需在相关表创建完成后再创建，避免空库初始化时报 relation does not exist

-- ============================================================================
-- 兼容性补丁：数据修复
-- ============================================================================

-- 为已存在的数据设置默认 ordinal_position（仅处理 NULL 值）
-- 注：新安装时所有列都有默认值999，此处理主要用于升级旧版本数据库
DO $$
DECLARE
 null_count INTEGER;
BEGIN
 -- 检查是否有NULL值需要修复
 SELECT COUNT(*) INTO null_count FROM db_columns WHERE ordinal_position IS NULL;

 IF null_count > 0 THEN
 -- 按字母顺序设置临时顺序
 UPDATE db_columns
 SET ordinal_position = subq.row_num
 FROM (
 SELECT
 column_id,
 ROW_NUMBER() OVER (PARTITION BY table_id ORDER BY column_name) as row_num
 FROM db_columns
 WHERE ordinal_position IS NULL
 ) subq
 WHERE db_columns.column_id = subq.column_id
 AND db_columns.ordinal_position IS NULL;

 RAISE NOTICE '✓ 已修复 % 个列的 ordinal_position 字段', null_count;
 END IF;
END $$;

-- ============================================================================
-- 数据优化：为已有的维度字段设置默认配置
-- ============================================================================

-- 为已有的维度字段设置默认枚举同步配置
UPDATE fields
SET enum_sync_config = '{
  "enabled": true,
  "strategy": "all",
  "top_n": 1000,
  "min_frequency": 5,
  "include_all_with_synonyms": true
}'::jsonb
WHERE field_type = 'dimension'
  AND enum_sync_config IS NULL;

-- ============================================================================
-- 第十一层：自动同步系统
-- ============================================================================

-- 同步历史表
CREATE TABLE IF NOT EXISTS milvus_sync_history (
    sync_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES database_connections(connection_id),

    -- 同步配置
    sync_type VARCHAR(20) NOT NULL, -- 'full', 'incremental', 'enums'
    triggered_by VARCHAR(20) NOT NULL DEFAULT 'auto', -- 'auto', 'manual'

    -- 状态信息
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'running', 'completed', 'failed', 'cancelled'
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,

    -- 审计字段
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- 统计信息
    total_entities INTEGER DEFAULT 0,
    synced_entities INTEGER DEFAULT 0,
    failed_entities INTEGER DEFAULT 0,

    -- 同步内容
    entity_changes JSONB, -- 记录具体变更的实体信息
    sync_config JSONB,   -- 同步配置参数

    -- 错误信息
    error_message TEXT,
    error_details JSONB,

    -- 进度跟踪
    current_step VARCHAR(50),
    progress_percentage INTEGER DEFAULT 0,

    -- 元数据
    created_by UUID REFERENCES users(user_id)
);

-- 为同步历史表创建索引
CREATE INDEX IF NOT EXISTS idx_sync_history_connection_status ON milvus_sync_history (connection_id, status);
CREATE INDEX IF NOT EXISTS idx_sync_history_type_triggered ON milvus_sync_history (sync_type, triggered_by);
CREATE INDEX IF NOT EXISTS idx_sync_history_started_at ON milvus_sync_history (started_at);
CREATE INDEX IF NOT EXISTS idx_sync_history_status_triggered ON milvus_sync_history (status, triggered_by);

-- 变更待同步表
CREATE TABLE IF NOT EXISTS milvus_pending_changes (
    change_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL REFERENCES database_connections(connection_id),

    -- 变更信息
    entity_type VARCHAR(20) NOT NULL, -- 'domain', 'table', 'field', 'enum'
    entity_id UUID NOT NULL,
    operation VARCHAR(10) NOT NULL,   -- 'INSERT', 'UPDATE', 'DELETE'

    -- 变更数据
    old_data JSONB,  -- UPDATE/DELETE时的旧数据
    new_data JSONB,  -- INSERT/UPDATE时的新数据

    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- 同步状态
    sync_status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'syncing', 'synced', 'failed'
    sync_id UUID REFERENCES milvus_sync_history(sync_id),
    synced_at TIMESTAMP WITH TIME ZONE,

    -- 优先级
    priority INTEGER DEFAULT 5 -- 1-10, 数字越小优先级越高
);

-- 为变更待同步表创建索引
CREATE INDEX IF NOT EXISTS idx_pending_changes_connection_type_status ON milvus_pending_changes (connection_id, entity_type, sync_status);
CREATE INDEX IF NOT EXISTS idx_pending_changes_status_priority_created ON milvus_pending_changes (sync_status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_pending_changes_entity_operation ON milvus_pending_changes (entity_id, operation);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_changes_unique_operation ON milvus_pending_changes (connection_id, entity_type, entity_id, operation, created_at);

-- 同步配置表（扁平化结构）
CREATE TABLE IF NOT EXISTS milvus_sync_config (
    config_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id UUID NOT NULL UNIQUE REFERENCES database_connections(connection_id) ON DELETE CASCADE,
    
    -- 自动同步配置
    auto_sync_enabled BOOLEAN DEFAULT TRUE NOT NULL,
    auto_sync_mode VARCHAR(16) DEFAULT 'auto' NOT NULL,
    auto_sync_domains BOOLEAN DEFAULT TRUE NOT NULL,
    auto_sync_tables BOOLEAN DEFAULT TRUE NOT NULL,
    auto_sync_fields BOOLEAN DEFAULT TRUE NOT NULL,
    auto_sync_enums BOOLEAN DEFAULT TRUE NOT NULL,
    auto_sync_few_shot BOOLEAN DEFAULT TRUE NOT NULL,
    
    -- 同步策略配置
    batch_window_seconds INTEGER DEFAULT 5,      -- 批量变更合并窗口（秒）
    max_batch_size INTEGER DEFAULT 100,          -- 最大批量大小
    sync_timeout_seconds INTEGER DEFAULT 300,    -- 同步超时时间
    
    -- 优先级配置
    domain_priority INTEGER DEFAULT 1,           -- 业务域变更优先级
    table_priority INTEGER DEFAULT 2,            -- 表变更优先级
    field_priority INTEGER DEFAULT 3,            -- 字段变更优先级
    enum_priority INTEGER DEFAULT 4,             -- 枚举值变更优先级
    
    -- 同步频率限制
    min_sync_interval_seconds INTEGER DEFAULT 60, -- 最小同步间隔
    
    -- 错误处理配置
    max_retry_attempts INTEGER DEFAULT 3,
    retry_delay_seconds INTEGER DEFAULT 10,
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id)
);

-- 全局同步配置表
-- 创建更新时间触发器（如果不存在）
DROP TRIGGER IF EXISTS update_sync_config_updated_at ON milvus_sync_config;
CREATE TRIGGER update_sync_config_updated_at 
BEFORE UPDATE ON milvus_sync_config
FOR EACH ROW 
EXECUTE FUNCTION update_updated_at_column();

-- 添加表注释
COMMENT ON TABLE milvus_sync_config IS 'Milvus同步配置表';

-- 兼容旧版本，追加缺失列
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS inherits_global BOOLEAN DEFAULT FALSE NOT NULL;
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS global_setting_id UUID;
COMMENT ON COLUMN milvus_sync_config.inherits_global IS '是否沿用最近一次全局模板';
COMMENT ON COLUMN milvus_sync_config.global_setting_id IS '最近一次应用的全局模板标识（无独立表，仅作追踪）';
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS auto_sync_mode VARCHAR(16) DEFAULT 'auto' NOT NULL;
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS auto_sync_domains BOOLEAN DEFAULT TRUE NOT NULL;
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS auto_sync_tables BOOLEAN DEFAULT TRUE NOT NULL;
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS auto_sync_fields BOOLEAN DEFAULT TRUE NOT NULL;
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS auto_sync_enums BOOLEAN DEFAULT TRUE NOT NULL;
ALTER TABLE milvus_sync_config
    ADD COLUMN IF NOT EXISTS auto_sync_few_shot BOOLEAN DEFAULT TRUE NOT NULL;

-- ==========================================
-- 同步错误表（自动同步系统错误处理和重试）
-- ==========================================
CREATE TABLE IF NOT EXISTS sync_errors (
    error_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- 错误分类
    error_type VARCHAR(50) NOT NULL, -- 'network_error', 'database_error', 'milvus_error', etc.
    severity VARCHAR(20) NOT NULL,  -- 'low', 'medium', 'high', 'critical'
    
    -- 错误信息
    error_message TEXT NOT NULL,
    error_details JSONB,
    stack_trace TEXT,
    
    -- 时间戳
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,
    
    -- 上下文信息（包含 connection_id, sync_id 等）
    context JSONB NOT NULL,
    
    -- 重试信息
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    
    -- 状态
    resolved BOOLEAN DEFAULT FALSE,
    active BOOLEAN DEFAULT TRUE
);

-- 为同步错误表创建索引
CREATE INDEX IF NOT EXISTS idx_sync_errors_context ON sync_errors (context);
CREATE INDEX IF NOT EXISTS idx_sync_errors_next_retry ON sync_errors (next_retry_at) WHERE resolved = FALSE AND next_retry_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sync_errors_resolved ON sync_errors (resolved);
CREATE INDEX IF NOT EXISTS idx_sync_errors_timestamp ON sync_errors ("timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_sync_errors_type_severity ON sync_errors (error_type, severity);

COMMENT ON TABLE sync_errors IS '同步错误跟踪表，记录和监控同步过程中的错误';
COMMENT ON COLUMN sync_errors.error_type IS '错误类型: network_error/database_error/milvus_error/embedding_error/validation_error/concurrency_error/resource_error/timeout_error/unknown_error';
COMMENT ON COLUMN sync_errors.severity IS '错误严重程度: low/medium/high/critical';
COMMENT ON COLUMN sync_errors.context IS '上下文信息，包含sync_id、connection_id、entity_type等';
COMMENT ON COLUMN sync_errors.next_retry_at IS '下次重试时间，NULL表示不重试';
COMMENT ON COLUMN sync_errors.resolved IS '错误是否已解决';
COMMENT ON COLUMN sync_errors.active IS '是否在活跃重试队列中';

-- 依赖 Milvus 同步表的统计视图：必须放在相关表创建之后
CREATE OR REPLACE VIEW v_pending_changes_stats AS
SELECT
    milvus_pending_changes.connection_id,
    milvus_pending_changes.entity_type,
    COUNT(*) AS pending_count,
    MIN(milvus_pending_changes.created_at) AS earliest_change,
    MAX(milvus_pending_changes.created_at) AS latest_change
FROM public.milvus_pending_changes
WHERE milvus_pending_changes.sync_status = 'pending'
GROUP BY milvus_pending_changes.connection_id, milvus_pending_changes.entity_type
ORDER BY pending_count DESC;

COMMENT ON VIEW v_pending_changes_stats IS '待同步变更统计视图';

CREATE OR REPLACE VIEW v_sync_history_stats AS
SELECT
    date_trunc('day', milvus_sync_history.started_at) AS sync_date,
    milvus_sync_history.connection_id,
    milvus_sync_history.sync_type,
    milvus_sync_history.triggered_by,
    milvus_sync_history.status,
    COUNT(*) AS sync_count,
    AVG(milvus_sync_history.duration_seconds) AS avg_duration,
    SUM(milvus_sync_history.synced_entities) AS total_synced_entities
FROM public.milvus_sync_history
WHERE milvus_sync_history.started_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date_trunc('day', milvus_sync_history.started_at),
         milvus_sync_history.connection_id,
         milvus_sync_history.sync_type,
         milvus_sync_history.triggered_by,
         milvus_sync_history.status
ORDER BY date_trunc('day', milvus_sync_history.started_at) DESC;

COMMENT ON VIEW v_sync_history_stats IS '同步历史统计视图（最近30天）';

-- 为自动同步表添加触发器
CREATE TRIGGER update_sync_history_updated_at BEFORE UPDATE ON milvus_sync_history
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pending_changes_updated_at BEFORE UPDATE ON milvus_pending_changes
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 统一Milvus待同步跟踪触发函数（确保任意业务表写入都会落地 milvus_pending_changes）
CREATE OR REPLACE FUNCTION business_domains_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, new_data, priority)
        VALUES (NEW.connection_id, 'business_domains', NEW.domain_id, 'INSERT', row_to_json(NEW), 5);

        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', NEW.connection_id::text,
                'entity_type', 'business_domains',
                'entity_id', NEW.domain_id::text,
                'operation', 'INSERT',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, new_data, priority)
        VALUES (NEW.connection_id, 'business_domains', NEW.domain_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), 5);

        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', NEW.connection_id::text,
                'entity_type', 'business_domains',
                'entity_id', NEW.domain_id::text,
                'operation', 'UPDATE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, priority)
        VALUES (OLD.connection_id, 'business_domains', OLD.domain_id, 'DELETE', row_to_json(OLD), 5);

        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', OLD.connection_id::text,
                'entity_type', 'business_domains',
                'entity_id', OLD.domain_id::text,
                'operation', 'DELETE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION milvus_sync_trigger_function()
RETURNS TRIGGER AS $$
DECLARE
    v_entity_id UUID;
    v_connection_id UUID;
    has_changes BOOLEAN := FALSE;
BEGIN
    CASE TG_TABLE_NAME
        WHEN 'business_domains' THEN
            v_entity_id := NEW.domain_id;
            v_connection_id := NEW.connection_id;
        WHEN 'db_tables' THEN
            v_entity_id := NEW.table_id;
            v_connection_id := NEW.connection_id;
        WHEN 'fields' THEN
            v_entity_id := NEW.field_id;
            v_connection_id := NEW.connection_id;
        WHEN 'field_enum_values' THEN
            v_entity_id := NEW.enum_value_id;
            SELECT f.connection_id INTO v_connection_id
            FROM fields f
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE f.field_id = NEW.field_id;
        ELSE
            RAISE EXCEPTION '未知的表名: %', TG_TABLE_NAME;
    END CASE;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, new_data, priority)
        VALUES (v_connection_id, TG_TABLE_NAME, v_entity_id, 'INSERT', row_to_json(NEW), 5);
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        CASE TG_TABLE_NAME
            WHEN 'business_domains' THEN
                v_entity_id := OLD.domain_id;
                v_connection_id := OLD.connection_id;
                IF OLD.domain_name IS DISTINCT FROM NEW.domain_name
                   OR OLD.description IS DISTINCT FROM NEW.description THEN
                    has_changes := TRUE;
                END IF;
            WHEN 'db_tables' THEN
                v_entity_id := OLD.table_id;
                v_connection_id := OLD.connection_id;
                IF OLD.table_name IS DISTINCT FROM NEW.table_name
                   OR OLD.display_name IS DISTINCT FROM NEW.display_name
                   OR OLD.description IS DISTINCT FROM NEW.description
                   OR OLD.tags IS DISTINCT FROM NEW.tags
                   OR OLD.domain_id IS DISTINCT FROM NEW.domain_id
                   OR OLD.is_included IS DISTINCT FROM NEW.is_included
                   OR OLD.data_year IS DISTINCT FROM NEW.data_year THEN
                    has_changes := TRUE;
                END IF;
            WHEN 'fields' THEN
                v_entity_id := OLD.field_id;
                v_connection_id := OLD.connection_id;
                IF OLD.display_name IS DISTINCT FROM NEW.display_name
                   OR OLD.description IS DISTINCT FROM NEW.description
                   OR OLD.field_type IS DISTINCT FROM NEW.field_type
                   OR OLD.synonyms IS DISTINCT FROM NEW.synonyms
                   OR OLD.is_active IS DISTINCT FROM NEW.is_active
                   OR OLD.show_in_detail IS DISTINCT FROM NEW.show_in_detail
                   OR OLD.priority IS DISTINCT FROM NEW.priority
                   OR OLD.unit IS DISTINCT FROM NEW.unit
                   OR OLD.default_aggregation IS DISTINCT FROM NEW.default_aggregation
                   OR OLD.unit_conversion IS DISTINCT FROM NEW.unit_conversion THEN
                    has_changes := TRUE;
                END IF;
            WHEN 'field_enum_values' THEN
                v_entity_id := OLD.enum_value_id;
                SELECT f.connection_id INTO v_connection_id
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE f.field_id = OLD.field_id;

                IF OLD.original_value IS DISTINCT FROM NEW.original_value
                   OR OLD.display_value IS DISTINCT FROM NEW.display_value
                   OR OLD.synonyms IS DISTINCT FROM NEW.synonyms
                   OR OLD.is_active IS DISTINCT FROM NEW.is_active THEN
                    has_changes := TRUE;
                END IF;
            ELSE
                has_changes := TRUE;
        END CASE;

        IF has_changes THEN
            INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, new_data, priority)
            VALUES (v_connection_id, TG_TABLE_NAME, v_entity_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), 5);
        END IF;

        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        CASE TG_TABLE_NAME
            WHEN 'business_domains' THEN
                v_entity_id := OLD.domain_id;
                v_connection_id := OLD.connection_id;
            WHEN 'db_tables' THEN
                v_entity_id := OLD.table_id;
                v_connection_id := OLD.connection_id;
            WHEN 'fields' THEN
                v_entity_id := OLD.field_id;
                v_connection_id := OLD.connection_id;
            WHEN 'field_enum_values' THEN
                v_entity_id := OLD.enum_value_id;
                SELECT f.connection_id INTO v_connection_id
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE f.field_id = OLD.field_id;
        END CASE;

        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, priority)
        VALUES (v_connection_id, TG_TABLE_NAME, v_entity_id, 'DELETE', row_to_json(OLD), 5);
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 业务域变更触发器函数（智能过滤 + 实时通知）
CREATE OR REPLACE FUNCTION record_domain_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, new_data, priority)
        VALUES (NEW.connection_id, 'domain', NEW.domain_id, 'INSERT', row_to_json(NEW), 1);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', NEW.connection_id::text,
                'entity_type', 'domain',
                'entity_id', NEW.domain_id::text,
                'operation', 'INSERT',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN NEW;
        
    ELSIF TG_OP = 'UPDATE' THEN
        -- 只有关键字段变化才触发
        IF OLD.domain_name IS DISTINCT FROM NEW.domain_name
           OR OLD.description IS DISTINCT FROM NEW.description
           OR OLD.keywords IS DISTINCT FROM NEW.keywords
           OR OLD.is_active IS DISTINCT FROM NEW.is_active THEN
            
            INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, new_data, priority)
            VALUES (NEW.connection_id, 'domain', NEW.domain_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), 1);
            
            PERFORM pg_notify(
                'milvus_sync_changes',
                json_build_object(
                    'connection_id', NEW.connection_id::text,
                    'entity_type', 'domain',
                    'entity_id', NEW.domain_id::text,
                    'operation', 'UPDATE',
                    'timestamp', extract(epoch FROM now())::text
                )::text
            );
        END IF;
        
        RETURN NEW;
        
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, priority)
        VALUES (OLD.connection_id, 'domain', OLD.domain_id, 'DELETE', row_to_json(OLD), 1);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', OLD.connection_id::text,
                'entity_type', 'domain',
                'entity_id', OLD.domain_id::text,
                'operation', 'DELETE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 数据表变更触发器函数（智能过滤 + 实时通知）
CREATE OR REPLACE FUNCTION record_table_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, new_data, priority)
        VALUES (NEW.connection_id, 'table', NEW.table_id, 'INSERT', row_to_json(NEW), 2);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', NEW.connection_id::text,
                'entity_type', 'table',
                'entity_id', NEW.table_id::text,
                'operation', 'INSERT',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN NEW;
        
    ELSIF TG_OP = 'UPDATE' THEN
        -- 只有影响Milvus的字段变化才触发
        IF OLD.table_name IS DISTINCT FROM NEW.table_name
           OR OLD.display_name IS DISTINCT FROM NEW.display_name
           OR OLD.description IS DISTINCT FROM NEW.description
           OR OLD.tags IS DISTINCT FROM NEW.tags
           OR OLD.domain_id IS DISTINCT FROM NEW.domain_id
           OR OLD.data_year IS DISTINCT FROM NEW.data_year
           OR OLD.is_included IS DISTINCT FROM NEW.is_included THEN
            
            INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, new_data, priority)
            VALUES (NEW.connection_id, 'table', NEW.table_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), 2);
            
            PERFORM pg_notify(
                'milvus_sync_changes',
                json_build_object(
                    'connection_id', NEW.connection_id::text,
                    'entity_type', 'table',
                    'entity_id', NEW.table_id::text,
                    'operation', 'UPDATE',
                    'timestamp', extract(epoch FROM now())::text
                )::text
            );
        END IF;
        
        RETURN NEW;
        
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, priority)
        VALUES (OLD.connection_id, 'table', OLD.table_id, 'DELETE', row_to_json(OLD), 2);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', OLD.connection_id::text,
                'entity_type', 'table',
                'entity_id', OLD.table_id::text,
                'operation', 'DELETE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 字段变更触发器函数（智能过滤 + 实时通知）
CREATE OR REPLACE FUNCTION record_field_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, new_data, priority)
        VALUES (NEW.connection_id, 'field', NEW.field_id, 'INSERT', row_to_json(NEW), 3);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', NEW.connection_id::text,
                'entity_type', 'field',
                'entity_id', NEW.field_id::text,
                'operation', 'INSERT',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN NEW;
        
    ELSIF TG_OP = 'UPDATE' THEN
        -- 字段名称或配置变化才触发
        IF OLD.display_name IS DISTINCT FROM NEW.display_name
           OR OLD.description IS DISTINCT FROM NEW.description
           OR OLD.synonyms IS DISTINCT FROM NEW.synonyms
           OR OLD.field_type IS DISTINCT FROM NEW.field_type
           OR OLD.enum_sync_config IS DISTINCT FROM NEW.enum_sync_config
           OR OLD.is_active IS DISTINCT FROM NEW.is_active THEN
            
            INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, new_data, priority)
            VALUES (NEW.connection_id, 'field', NEW.field_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), 3);
            
            PERFORM pg_notify(
                'milvus_sync_changes',
                json_build_object(
                    'connection_id', NEW.connection_id::text,
                    'entity_type', 'field',
                    'entity_id', NEW.field_id::text,
                    'operation', 'UPDATE',
                    'timestamp', extract(epoch FROM now())::text
                )::text
            );
        END IF;
        
        RETURN NEW;
        
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, priority)
        VALUES (OLD.connection_id, 'field', OLD.field_id, 'DELETE', row_to_json(OLD), 3);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', OLD.connection_id::text,
                'entity_type', 'field',
                'entity_id', OLD.field_id::text,
                'operation', 'DELETE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 枚举值变更触发器函数（智能过滤 + 实时通知）
CREATE OR REPLACE FUNCTION record_enum_change()
RETURNS TRIGGER AS $$
DECLARE
    v_connection_id UUID;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT f.connection_id INTO v_connection_id 
        FROM fields f 
        WHERE f.field_id = NEW.field_id;
        
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, new_data, priority)
        VALUES (v_connection_id, 'enum', NEW.enum_value_id, 'INSERT', row_to_json(NEW), 4);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', v_connection_id::text,
                'entity_type', 'enum',
                'entity_id', NEW.enum_value_id::text,
                'operation', 'INSERT',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN NEW;
        
    ELSIF TG_OP = 'UPDATE' THEN
        -- 只有枚举值相关字段变化才触发
        IF OLD.original_value IS DISTINCT FROM NEW.original_value
           OR OLD.display_value IS DISTINCT FROM NEW.display_value
           OR OLD.synonyms IS DISTINCT FROM NEW.synonyms
           OR OLD.is_active IS DISTINCT FROM NEW.is_active THEN
            
            SELECT f.connection_id INTO v_connection_id 
            FROM fields f 
            WHERE f.field_id = NEW.field_id;
            
            INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, new_data, priority)
            VALUES (v_connection_id, 'enum', NEW.enum_value_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), 4);
            
            PERFORM pg_notify(
                'milvus_sync_changes',
                json_build_object(
                    'connection_id', v_connection_id::text,
                    'entity_type', 'enum',
                    'entity_id', NEW.enum_value_id::text,
                    'operation', 'UPDATE',
                    'timestamp', extract(epoch FROM now())::text
                )::text
            );
        END IF;
        
        RETURN NEW;
        
    ELSIF TG_OP = 'DELETE' THEN
        SELECT f.connection_id INTO v_connection_id 
        FROM fields f 
        WHERE f.field_id = OLD.field_id;
        
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, priority)
        VALUES (v_connection_id, 'enum', OLD.enum_value_id, 'DELETE', row_to_json(OLD), 4);
        
        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', v_connection_id::text,
                'entity_type', 'enum',
                'entity_id', OLD.enum_value_id::text,
                'operation', 'DELETE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 智能变更跟踪触发器
CREATE TRIGGER trigger_business_domain_change AFTER INSERT OR DELETE OR UPDATE ON business_domains
FOR EACH ROW EXECUTE FUNCTION record_domain_change();

CREATE TRIGGER trigger_db_table_change AFTER INSERT OR DELETE OR UPDATE ON db_tables
FOR EACH ROW EXECUTE FUNCTION record_table_change();

CREATE TRIGGER trigger_field_change AFTER INSERT OR DELETE OR UPDATE ON fields
FOR EACH ROW EXECUTE FUNCTION record_field_change();

CREATE TRIGGER trigger_enum_change AFTER INSERT OR DELETE OR UPDATE ON field_enum_values
FOR EACH ROW EXECUTE FUNCTION record_enum_change();

-- 低粒度写入 -> 直接同步至 milvus_pending_changes
DROP TRIGGER IF EXISTS track_milvus_sync_changes ON business_domains;
CREATE TRIGGER track_milvus_sync_changes
AFTER INSERT OR DELETE OR UPDATE ON business_domains
FOR EACH ROW EXECUTE FUNCTION business_domains_trigger_function();

DROP TRIGGER IF EXISTS track_milvus_sync_changes ON db_tables;
CREATE TRIGGER track_milvus_sync_changes
AFTER INSERT OR DELETE OR UPDATE ON db_tables
FOR EACH ROW EXECUTE FUNCTION milvus_sync_trigger_function();

DROP TRIGGER IF EXISTS track_milvus_sync_changes ON fields;
CREATE TRIGGER track_milvus_sync_changes
AFTER INSERT OR DELETE OR UPDATE ON fields
FOR EACH ROW EXECUTE FUNCTION milvus_sync_trigger_function();

DROP TRIGGER IF EXISTS track_milvus_sync_changes ON field_enum_values;
CREATE TRIGGER track_milvus_sync_changes
AFTER INSERT OR DELETE OR UPDATE ON field_enum_values
FOR EACH ROW EXECUTE FUNCTION milvus_sync_trigger_function();

CREATE OR REPLACE FUNCTION record_few_shot_sample_change()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, new_data, priority)
        VALUES (NEW.connection_id, 'few_shot', NEW.sample_id, 'INSERT', row_to_json(NEW), 5);

        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', NEW.connection_id::text,
                'entity_type', 'few_shot',
                'entity_id', NEW.sample_id::text,
                'operation', 'INSERT',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, new_data, priority)
        VALUES (NEW.connection_id, 'few_shot', NEW.sample_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), 5);

        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', NEW.connection_id::text,
                'entity_type', 'few_shot',
                'entity_id', NEW.sample_id::text,
                'operation', 'UPDATE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO milvus_pending_changes (connection_id, entity_type, entity_id, operation, old_data, priority)
        VALUES (OLD.connection_id, 'few_shot', OLD.sample_id, 'DELETE', row_to_json(OLD), 5);

        PERFORM pg_notify(
            'milvus_sync_changes',
            json_build_object(
                'connection_id', OLD.connection_id::text,
                'entity_type', 'few_shot',
                'entity_id', OLD.sample_id::text,
                'operation', 'DELETE',
                'timestamp', extract(epoch FROM now())::text
            )::text
        );
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_few_shot_sample_change AFTER INSERT OR DELETE OR UPDATE ON qa_few_shot_samples
FOR EACH ROW EXECUTE FUNCTION record_few_shot_sample_change();

CREATE TRIGGER trigger_field_enum_values_updated_at BEFORE UPDATE ON field_enum_values
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_milvus_sync_config_updated_at BEFORE UPDATE ON milvus_sync_config
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 数据权限相关表的 updated_at 触发器
CREATE TRIGGER trigger_data_roles_updated_at BEFORE UPDATE ON data_roles
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- [已删除] role_connection_scopes 表的触发器（该表已在新架构中移除）

CREATE TRIGGER trigger_role_table_permissions_updated_at BEFORE UPDATE ON role_table_permissions
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_role_row_filters_updated_at BEFORE UPDATE ON role_row_filters
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_user_attributes_updated_at BEFORE UPDATE ON user_attributes
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 用户完整权限视图（简化版，移除了已删除的 role_connection_scopes 依赖）
-- 新架构中，权限通过 scope_type 和 role_table_permissions 控制
CREATE OR REPLACE VIEW v_user_permissions AS
SELECT 
    u.user_id,
    u.username,
    u.role AS system_role,
    dr.role_id AS data_role_id,
    dr.role_name AS data_role_name,
    dr.role_code AS data_role_code,
    dr.scope_type,
    udr.is_active AS role_assignment_active,
    udr.expires_at AS role_expires_at,
    udr.granted_at,
    (SELECT COUNT(*) FROM role_table_permissions rtp WHERE rtp.role_id = dr.role_id) AS table_permission_count,
    (SELECT COUNT(*) FROM role_row_filters rrf WHERE rrf.role_id = dr.role_id AND rrf.is_active = TRUE) AS row_filter_count
FROM users u
LEFT JOIN user_data_roles udr ON u.user_id = udr.user_id AND udr.is_active = TRUE
LEFT JOIN data_roles dr ON udr.role_id = dr.role_id AND dr.is_active = TRUE;

COMMENT ON VIEW v_user_permissions IS '用户完整权限视图 - 展示用户的系统角色和数据角色（新架构：权限通过表级别控制）';

-- ============================================================================
-- 组织架构相关视图和函数
-- ============================================================================

-- 获取组织完整路径的函数
CREATE OR REPLACE FUNCTION get_org_path(p_org_id UUID)
RETURNS TEXT AS $$
DECLARE
    v_path TEXT := '';
    v_current_id UUID := p_org_id;
    v_name TEXT;
BEGIN
    WHILE v_current_id IS NOT NULL LOOP
        SELECT org_name, parent_id INTO v_name, v_current_id
        FROM organizations WHERE org_id = v_current_id;
        
        IF v_name IS NOT NULL THEN
            v_path := '/' || v_name || v_path;
        END IF;
    END LOOP;
    
    RETURN v_path;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_org_path(UUID) IS '获取组织的完整路径，如 /总公司/技术部/研发组';

-- 自动更新组织路径的触发器函数
CREATE OR REPLACE FUNCTION update_org_path()
RETURNS TRIGGER AS $$
BEGIN
    NEW.org_path := get_org_path(NEW.org_id);
    
    -- 计算层级
    IF NEW.parent_id IS NULL THEN
        NEW.level := 0;
    ELSE
        SELECT level + 1 INTO NEW.level
        FROM organizations WHERE org_id = NEW.parent_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 组织路径更新触发器
CREATE TRIGGER trigger_update_org_path
    BEFORE INSERT OR UPDATE OF parent_id, org_name ON organizations
    FOR EACH ROW EXECUTE FUNCTION update_org_path();

-- 组织架构表的 updated_at 触发器
CREATE TRIGGER update_organizations_updated_at
    BEFORE UPDATE ON organizations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 用户有效数据角色视图（包含组织继承）
CREATE OR REPLACE VIEW v_user_effective_data_roles AS
WITH RECURSIVE org_ancestors AS (
    -- 基础：用户直接所属的组织
    SELECT 
        o.org_id,
        o.parent_id,
        o.org_name,
        o.org_code,
        0 as depth
    FROM organizations o
    WHERE o.is_active = TRUE
    
    UNION ALL
    
    -- 递归：获取所有上级组织
    SELECT 
        parent.org_id,
        parent.parent_id,
        parent.org_name,
        parent.org_code,
        oa.depth + 1
    FROM organizations parent
    INNER JOIN org_ancestors oa ON parent.org_id = oa.parent_id
    WHERE parent.is_active = TRUE
)
SELECT DISTINCT
    u.user_id,
    u.username,
    u.org_id AS user_org_id,
    o_user.org_name AS user_org_name,
    dr.role_id,
    dr.role_code,
    dr.role_name,
    dr.scope_type,
    CASE 
        WHEN udr.user_id IS NOT NULL THEN 'direct'           -- 用户直接分配
        WHEN odr.org_id = u.org_id THEN 'org_direct'         -- 用户所属组织直接分配
        ELSE 'org_inherited'                                  -- 上级组织继承
    END AS grant_source,
    COALESCE(odr.org_id, NULL) AS source_org_id,
    o_source.org_name AS source_org_name
FROM users u
-- 用户所属组织信息
LEFT JOIN organizations o_user ON u.org_id = o_user.org_id AND o_user.is_active = TRUE
-- 用户直接分配的角色
LEFT JOIN user_data_roles udr 
    ON u.user_id = udr.user_id 
    AND udr.is_active = TRUE
    AND (udr.expires_at IS NULL OR udr.expires_at > NOW())
-- 用户所属组织及其上级组织
LEFT JOIN org_ancestors oa ON u.org_id = oa.org_id OR u.org_id IN (
    SELECT org_id FROM org_ancestors WHERE parent_id = oa.parent_id
)
-- 组织分配的角色（包含继承）
LEFT JOIN org_data_roles odr 
    ON oa.org_id = odr.org_id 
    AND odr.is_active = TRUE
    AND (odr.inherit_to_children = TRUE OR oa.depth = 0)
-- 角色来源组织信息
LEFT JOIN organizations o_source ON odr.org_id = o_source.org_id
-- 角色信息
LEFT JOIN data_roles dr 
    ON dr.role_id = COALESCE(udr.role_id, odr.role_id) 
    AND dr.is_active = TRUE
WHERE u.is_active = TRUE
  AND dr.role_id IS NOT NULL;

COMMENT ON VIEW v_user_effective_data_roles IS '用户有效数据角色视图（包含直接分配和组织继承）';

-- 组织统计视图（先删除旧视图再创建，避免列顺序冲突）
DROP VIEW IF EXISTS v_organization_stats CASCADE;

CREATE VIEW v_organization_stats AS
SELECT 
    o.org_id,
    o.org_code,
    o.org_name,
    o.org_type,
    o.parent_id,
    po.org_name AS parent_org_name,
    o.level,
    o.org_path,
    o.source_idp,
    o.external_org_id,
    o.is_active,
    o.sort_order,
    o.description,
    o.created_at,
    o.created_by,
    o.updated_at,
    -- 直接成员数
    (SELECT COUNT(*) FROM users u WHERE u.org_id = o.org_id AND u.is_active = TRUE) AS direct_user_count,
    -- 子组织数
    (SELECT COUNT(*) FROM organizations sub WHERE sub.parent_id = o.org_id AND sub.is_active = TRUE) AS child_org_count,
    -- 关联的数据角色数
    (SELECT COUNT(*) FROM org_data_roles odr WHERE odr.org_id = o.org_id AND odr.is_active = TRUE) AS data_role_count
FROM organizations o
LEFT JOIN organizations po ON o.parent_id = po.org_id;

COMMENT ON VIEW v_organization_stats IS '组织统计视图（包含成员数、子组织数、角色数）';

COMMENT ON TABLE milvus_sync_history IS 'Milvus同步历史记录表';
COMMENT ON TABLE milvus_pending_changes IS 'Milvus待同步变更表';
COMMENT ON TABLE milvus_sync_config IS 'Milvus同步配置表';
COMMENT ON COLUMN milvus_sync_history.sync_type IS '同步类型: full(全量)/incremental(增量)/enums(枚举值)';
COMMENT ON COLUMN milvus_sync_history.triggered_by IS '触发方式: auto(自动)/manual(手动)';
COMMENT ON COLUMN milvus_pending_changes.entity_type IS '实体类型: domain, table, field, enum';
COMMENT ON COLUMN milvus_pending_changes.operation IS '操作类型: INSERT, UPDATE, DELETE';
COMMENT ON COLUMN milvus_pending_changes.priority IS '优先级: 1-10，数字越小优先级越高';

-- ============================================================================
-- 完成
-- ============================================================================

COMMENT ON DATABASE postgres IS 'NL2SQL - 多用户数据库管理工具';
COMMENT ON SCHEMA public IS 'NL2SQL 数据库Schema已创建完成（含自动同步系统）';

-- 输出初始化完成信息
DO $$
DECLARE
 v_count INTEGER;
BEGIN
 -- 统计表数量
 SELECT COUNT(*) INTO v_count
 FROM information_schema.tables
 WHERE table_schema = 'public' AND table_type = 'BASE TABLE';

 RAISE NOTICE '========================================';
 RAISE NOTICE 'NL2SQL 数据库初始化完成！';
 RAISE NOTICE '========================================';
 RAISE NOTICE '✓ 已创建 % 个数据表', v_count;
    RAISE NOTICE '✓ 已创建触发器和视图';
    RAISE NOTICE '✓ 已插入默认用户数据';
    RAISE NOTICE '✓ 时区已设置为 Asia/Shanghai';
    RAISE NOTICE '✓ 支持枚举值同步到Milvus';
    RAISE NOTICE '✓ 新增自动同步变更跟踪系统';
    RAISE NOTICE '✓ 支持增量同步和自动触发';
    RAISE NOTICE '✓ Milvus 枚举集合按 connection_id 分区，语义集合分区键为 entity_type';
    RAISE NOTICE '✓ 新增数据权限管理（数据角色、表权限、行级过滤）';
    RAISE NOTICE '✓ 预置6个RLS规则模板';
    RAISE NOTICE '✓ 新增组织架构支持（树形结构、外部同步、权限继承）';
    RAISE NOTICE '========================================';
END $$;
