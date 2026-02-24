"""元数据数据库管理器"""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

import asyncpg
import structlog

from server.config import settings
from server.models.semantic import (
    BusinessDomain,
    Datasource,
    DatasourceJoin,
    Field,
    FieldDimensionProps,
    FieldEnumSynonym,
    FieldEnumValue,
    FieldMeasureProps,
    FieldTimestampProps,
    PhysicalColumn,
    PhysicalTable,
    SemanticModel,
    TenantConfig,
)
from server.utils.timezone_helper import now_with_tz

logger = structlog.get_logger()


class MetadataManager:
    """按 connection_id 构建语义模型的元数据管理器"""

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool: Optional[asyncpg.Pool] = None
        self._cache: Dict[str, Tuple[SemanticModel, datetime]] = {}
        self.cache_ttl = 1800  # 30 分钟缓存

        logger.info(
            "元数据管理器初始化",
            db_url=db_url.split('@')[0] + '@***' if '@' in db_url else 'local'
        )

    async def connect(self):
        """建立数据库连接池"""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.db_url,
                min_size=5,
                max_size=20,
                command_timeout=60,
            )
            logger.info("元数据库连接池创建成功")

    async def close(self):
        """关闭连接池"""
        if self.pool:
            await self.pool.close()
            logger.info("元数据库连接池已关闭")

    async def get_connection_model(
        self,
        connection_id: str,
        force_reload: bool = False,
    ) -> SemanticModel:
        """按数据库连接加载语义模型"""

        cache_key = f"conn_{connection_id}"
        if not force_reload and cache_key in self._cache:
            model, expire_time = self._cache[cache_key]
            if now_with_tz() < expire_time:
                logger.debug("从缓存加载语义模型", connection_id=connection_id)
                return model

        logger.debug("从数据库加载语义模型", connection_id=connection_id)

        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            domains_list = await self._load_domains(conn, connection_id)
            domains = {d.domain_id: d for d in domains_list}

            datasources = await self._load_datasources(conn, connection_id)
            fields = await self._load_fields(conn, connection_id)
            field_enums = await self._load_field_enums(conn, connection_id)
            joins = await self._load_joins(conn, connection_id)

            tenant_config = TenantConfig(
                default_domain_id=domains_list[0].domain_id if domains_list else "default",
                timezone=settings.timezone,
            )

            model = SemanticModel(
                version="2.0",
                tenant_config=tenant_config,
                domains=domains,
                datasources=datasources,
                fields=fields,
                field_enums=field_enums,
                metrics={},
                joins=joins,
                field_validation_rules=[],
                field_normalization_rules=[],
                metric_business_rules=[],
                rls_rules=[],
                reference_data={},
                table_resolution_config=None,
                formatting=None,
            )

            expire_time = now_with_tz() + timedelta(seconds=self.cache_ttl)
            self._cache[cache_key] = (model, expire_time)

            logger.debug(
                "语义模型加载完成",
                connection_id=connection_id,
                domains=len(domains),
                datasources=len(datasources),
                fields=len(fields),
                joins=len(joins),
            )
            return model

    def invalidate_cache(self, cache_key: str):
        if cache_key in self._cache:
            del self._cache[cache_key]
            logger.debug("缓存已清除", key=cache_key)

    def clear_all_cache(self):
        self._cache.clear()
        logger.debug("所有缓存已清除")
    
    async def get_active_connections(self) -> List[Dict[str, Any]]:
        """
        获取所有活跃的数据库连接列表
        
        Returns:
            连接列表，每个元素包含 connection_id, connection_name, db_type
        """
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT connection_id, connection_name, db_type, host, port, database_name
                FROM database_connections
                WHERE is_active = TRUE
                ORDER BY connection_name
            """)
            
            connections = [
                {
                    "connection_id": str(row['connection_id']),
                    "connection_name": row['connection_name'],
                    "db_type": row['db_type'],
                    "host": row['host'],
                    "port": row['port'],
                    "database_name": row['database_name']
                }
                for row in rows
            ]
            
            logger.debug("获取活跃连接列表", count=len(connections))
            return connections
    
    async def get_global_model(
        self,
        force_reload: bool = False,
    ) -> SemanticModel:
        """
        加载全局语义模型（聚合所有活跃连接的元数据）
        
        Args:
            force_reload: 是否强制刷新缓存
            
        Returns:
            全局语义模型
        """
        cache_key = "global_model"
        if not force_reload and cache_key in self._cache:
            model, expire_time = self._cache[cache_key]
            if now_with_tz() < expire_time:
                logger.debug("从缓存加载全局语义模型")
                return model
        
        logger.debug("从数据库加载全局语义模型")
        
        if not self.pool:
            await self.connect()
        
        # 获取所有活跃连接
        connections = await self.get_active_connections()
        
        if not connections:
            logger.warning("没有活跃的数据库连接")
            # 返回空模型
            return SemanticModel(
                version="2.0",
                tenant_config=TenantConfig(
                    default_domain_id="default",
                    timezone=settings.timezone,
                ),
                domains={},
                datasources={},
                fields={},
                field_enums={},
                metrics={},
                joins=[],
                field_validation_rules=[],
                field_normalization_rules=[],
                metric_business_rules=[],
                rls_rules=[],
                reference_data={},
                table_resolution_config=None,
                formatting=None,
            )
        
        # 并行加载各连接的元数据
        import asyncio
        
        all_domains = {}
        all_datasources = {}
        all_fields = {}
        all_field_enums = {}
        all_joins = []
        
        async with self.pool.acquire() as conn:
            for connection_info in connections:
                connection_id = connection_info['connection_id']
                
                try:
                    # 加载业务域（支持全局域和连接域）
                    domains_list = await self._load_domains_global(conn, connection_id)
                    for domain in domains_list:
                        all_domains[domain.domain_id] = domain
                    
                    # 加载数据源（带connection_id标识）
                    datasources = await self._load_datasources_global(conn, connection_id)
                    all_datasources.update(datasources)
                    
                    # 加载字段
                    fields = await self._load_fields(conn, connection_id)
                    all_fields.update(fields)
                    
                    # 加载枚举值
                    field_enums = await self._load_field_enums(conn, connection_id)
                    all_field_enums.update(field_enums)
                    
                    # 加载JOIN关系
                    joins = await self._load_joins(conn, connection_id)
                    all_joins.extend(joins)
                    
                    logger.debug(
                        "加载连接元数据",
                        connection_id=connection_id,
                        connection_name=connection_info['connection_name'],
                        domains=len(domains_list),
                        datasources=len(datasources),
                        fields=len(fields)
                    )
                    
                except Exception as e:
                    logger.error(
                        "加载连接元数据失败",
                        connection_id=connection_id,
                        connection_name=connection_info['connection_name'],
                        error=str(e)
                    )
                    # 继续加载其他连接
                    continue
        
        # 构建全局模型
        tenant_config = TenantConfig(
            default_domain_id=list(all_domains.keys())[0] if all_domains else "default",
            timezone=settings.timezone,
        )
        
        model = SemanticModel(
            version="2.0",
            tenant_config=tenant_config,
            domains=all_domains,
            datasources=all_datasources,
            fields=all_fields,
            field_enums=all_field_enums,
            metrics={},
            joins=all_joins,
            field_validation_rules=[],
            field_normalization_rules=[],
            metric_business_rules=[],
            rls_rules=[],
            reference_data={},
            table_resolution_config=None,
            formatting=None,
        )
        
        # 缓存
        expire_time = now_with_tz() + timedelta(seconds=self.cache_ttl)
        self._cache[cache_key] = (model, expire_time)
        
        logger.info(
            "全局语义模型加载完成",
            connections=len(connections),
            domains=len(all_domains),
            datasources=len(all_datasources),
            fields=len(all_fields),
            joins=len(all_joins),
        )
        
        return model
    
    async def _load_domains_global(self, conn, connection_id: str) -> List[BusinessDomain]:
        """加载业务域（支持全局域和连接域）"""
        rows = await conn.fetch(
            """
            SELECT domain_id, domain_code, domain_name, description,
                   icon, color, sort_order, keywords, is_active, connection_id
            FROM business_domains
            WHERE (connection_id = $1 OR connection_id IS NULL) AND is_active = true
            ORDER BY domain_name
            """,
            connection_id,
        )
        
        domains: List[BusinessDomain] = []
        for row in rows:
            keywords_data = row.get('keywords', [])
            if isinstance(keywords_data, str):
                keywords_data = json.loads(keywords_data) if keywords_data else []
            
            domain = BusinessDomain(
                domain_id=str(row['domain_id']),
                domain_code=row['domain_code'],
                domain_name=row['domain_name'],
                description=row.get('description'),
                icon=row.get('icon', ''),
                color=row.get('color', '#409eff'),
                sort_order=row.get('sort_order', 0),
                keywords=keywords_data or [],
            )
            domains.append(domain)
        
        return domains
    
    async def _load_datasources_global(
        self,
        conn,
        connection_id: str,
    ) -> Dict[str, Datasource]:
        """加载数据源（带connection_id标识）"""
        rows = await conn.fetch(
            """
            SELECT t.table_id, t.schema_name, t.table_name, t.display_name,
                   t.description, t.domain_id, t.tags, t.is_included,
                   t.data_year, t.connection_id
            FROM db_tables t
            WHERE t.connection_id = $1 AND t.is_included = true
            ORDER BY t.table_name
            """,
            connection_id,
        )
        
        datasources: Dict[str, Datasource] = {}
        for row in rows:
            columns_rows = await conn.fetch(
                """
                SELECT column_id, column_name, data_type,
                       is_nullable, is_primary_key
                FROM db_columns
                WHERE table_id = $1
                ORDER BY column_name
                """,
                row['table_id'],
            )
            
            columns = [
                PhysicalColumn(
                    column_id=str(col_row['column_id']),
                    column_name=col_row['column_name'],
                    data_type=col_row['data_type'],
                    is_nullable=col_row['is_nullable'],
                    is_primary_key=col_row['is_primary_key'],
                )
                for col_row in columns_rows
            ]
            
            physical_table = PhysicalTable(
                table_id=str(row['table_id']),
                schema_name=row.get('schema_name', 'dbo'),
                table_name=row['table_name'],
                db_type='sqlserver',
                columns=columns,
            )
            
            tags_data = row.get('tags', [])
            if isinstance(tags_data, str):
                tags_data = json.loads(tags_data) if tags_data else []
            
            datasource = Datasource(
                datasource_id=str(row['table_id']),
                datasource_name=row['table_name'],
                display_name=row.get('display_name') or row['table_name'],
                description=row.get('description'),
                datasource_type='table',
                domain_id=str(row['domain_id']) if row.get('domain_id') else None,
                tags=tags_data or [],
                data_year=row.get('data_year'),
                connection_id=str(row['connection_id']),  # 关键：标识所属连接
                physical_tables=[physical_table],
            )
            
            datasources[datasource.datasource_id] = datasource
        
        return datasources

    async def _load_domains(self, conn, connection_id: str) -> List[BusinessDomain]:
        rows = await conn.fetch(
            """
            SELECT domain_id, domain_code, domain_name, description,
                   icon, color, sort_order, keywords, is_active
            FROM business_domains
            WHERE connection_id = $1 AND is_active = true
            ORDER BY domain_name
            """,
            connection_id,
        )

        domains: List[BusinessDomain] = []
        for row in rows:
            keywords_data = row.get('keywords', [])
            if isinstance(keywords_data, str):
                keywords_data = json.loads(keywords_data) if keywords_data else []

            domain = BusinessDomain(
                domain_id=str(row['domain_id']),
                domain_code=row['domain_code'],
                domain_name=row['domain_name'],
                description=row.get('description'),
                icon=row.get('icon', ''),
                color=row.get('color', '#409eff'),
                sort_order=row.get('sort_order', 0),
                keywords=keywords_data or [],
            )
            domains.append(domain)

        logger.debug("加载业务域", connection_id=connection_id, count=len(domains))
        return domains

    async def _load_datasources(
        self,
        conn,
        connection_id: str,
    ) -> Dict[str, Datasource]:
        rows = await conn.fetch(
            """
            SELECT t.table_id, t.schema_name, t.table_name, t.display_name,
                   t.description, t.domain_id, t.tags, t.is_included,
                   t.data_year
            FROM db_tables t
            WHERE t.connection_id = $1 AND t.is_included = true
            ORDER BY t.table_name
            """,
            connection_id,
        )

        datasources: Dict[str, Datasource] = {}
        for row in rows:
            columns_rows = await conn.fetch(
                """
                SELECT column_id, column_name, data_type,
                       is_nullable, is_primary_key
                FROM db_columns
                WHERE table_id = $1
                ORDER BY column_name
                """,
                row['table_id'],
            )

            columns = [
                PhysicalColumn(
                    column_id=str(col_row['column_id']),
                    column_name=col_row['column_name'],
                    data_type=col_row['data_type'],
                    is_nullable=col_row['is_nullable'],
                    is_primary_key=col_row['is_primary_key'],
                )
                for col_row in columns_rows
            ]

            physical_table = PhysicalTable(
                table_id=str(row['table_id']),
                schema_name=row.get('schema_name', 'dbo'),
                table_name=row['table_name'],
                db_type='sqlserver',
                columns=columns,
            )

            tags_data = row.get('tags', [])
            if isinstance(tags_data, str):
                tags_data = json.loads(tags_data) if tags_data else []

            datasource = Datasource(
                datasource_id=str(row['table_id']),
                datasource_name=row['table_name'],
                display_name=row['display_name'] or row['table_name'],
                description=row.get('description'),
                datasource_type='table',
                domain_id=str(row['domain_id']) if row['domain_id'] else None,
                tags=tags_data or [],
                physical_tables=[physical_table],
                data_year=row.get('data_year'),
            )
            datasources[str(row['table_id'])] = datasource

        logger.debug("加载数据源", connection_id=connection_id, count=len(datasources))
        return datasources

    async def _load_fields(self, conn, connection_id: str) -> Dict[str, Field]:
        rows = await conn.fetch(
            """
            SELECT f.field_id, f.display_name, f.field_type, f.source_type,
                   f.source_column_id, f.source_expression, f.synonyms,
                   f.default_aggregation, f.unit, f.unit_conversion, f.description,
                   f.show_in_detail,
                   f.dimension_type, f.hierarchy_level, f.parent_field_id,
                   f.is_additive, f.is_unique,
                   f.tags, f.business_category,
                   c.table_id, c.column_name, c.data_type
            FROM fields f
            LEFT JOIN db_columns c ON f.source_column_id = c.column_id
            LEFT JOIN db_tables t ON c.table_id = t.table_id
            WHERE t.connection_id = $1
              AND f.is_active = TRUE
              AND t.is_included = TRUE
            ORDER BY t.table_name, c.column_name NULLS LAST
            """,
            connection_id,
        )

        fields: Dict[str, Field] = {}
        for row in rows:
            field_type = row['field_type']

            synonyms_data = row.get('synonyms', [])
            if isinstance(synonyms_data, str):
                synonyms_data = json.loads(synonyms_data) if synonyms_data else []

            unit_conversion_data = row.get('unit_conversion')
            if unit_conversion_data and isinstance(unit_conversion_data, str):
                try:
                    unit_conversion_data = json.loads(unit_conversion_data)
                except (json.JSONDecodeError, TypeError):
                    unit_conversion_data = None

            dimension_props = None
            measure_props = None
            timestamp_props = None

            if field_type == 'dimension':
                dim_type = row.get('dimension_type') or 'categorical'
                dimension_props = FieldDimensionProps(
                    dimension_type=dim_type,
                    hierarchy_level=row.get('hierarchy_level'),
                    parent_field_id=str(row['parent_field_id']) if row.get('parent_field_id') else None,
                    match_pattern='exact',
                    cardinality=None,
                    has_hierarchy=row.get('parent_field_id') is not None,
                )
            elif field_type == 'measure':
                agg = row.get('default_aggregation', 'SUM')
                measure_props = FieldMeasureProps(
                    unit=row.get('unit'),
                    aggregatable=True,
                    default_aggregation=agg.upper() if agg else 'SUM',
                    decimal_places=2,
                    is_additive=row.get('is_additive', True),
                )
            elif field_type == 'timestamp':
                timestamp_props = FieldTimestampProps(
                    time_granularity='day',
                    is_partition_key=False,
                )

            field = Field(
                field_id=str(row['field_id']),
                datasource_id=str(row['table_id']) if row['table_id'] else 'unknown',
                physical_column_id=str(row['source_column_id']) if row['source_column_id'] else 'unknown',
                field_name=row['column_name'] if row['column_name'] else row['display_name'],
                display_name=row['display_name'],
                description=row.get('description'),
                field_category=field_type,
                data_type=row.get('data_type', 'string'),
                synonyms=synonyms_data or [],
                priority=5,
                is_primary=False,
                field_role='primary',
                is_active=True,
                show_in_detail=row.get('show_in_detail', False),
                dimension_props=dimension_props,
                measure_props=measure_props,
                timestamp_props=timestamp_props,
                physical_column_name=row['column_name'],
                unit_conversion=unit_conversion_data,
            )

            fields[str(row['field_id'])] = field

        logger.debug("加载字段", connection_id=connection_id, count=len(fields))
        return fields

    async def _load_field_enums(
        self,
        conn,
        connection_id: str,
    ) -> Dict[str, List[FieldEnumValue]]:
        rows = await conn.fetch(
            """
            SELECT e.enum_value_id, e.field_id, e.original_value,
                   e.display_value, e.synonyms, e.frequency, e.is_active,
                   e.includes_values
            FROM field_enum_values e
            JOIN fields f ON e.field_id = f.field_id
            JOIN db_columns c ON f.source_column_id = c.column_id
            JOIN db_tables t ON c.table_id = t.table_id
            WHERE t.connection_id = $1::uuid
              AND f.is_active = TRUE
              AND t.is_included = TRUE
              AND e.is_active = TRUE
            ORDER BY e.field_id, e.original_value
            """,
            connection_id,
        )

        field_enums: Dict[str, List[FieldEnumValue]] = {}
        for row in rows:
            field_id = str(row['field_id'])
            if field_id not in field_enums:
                field_enums[field_id] = []

            synonyms_list = row.get('synonyms', []) or []
            synonym_objs = [
                FieldEnumSynonym(
                    synonym_id=str(row['enum_value_id']) + '_' + syn_text[:10],
                    synonym_text=syn_text,
                    match_type='exact',
                    confidence_score=Decimal("1.0"),
                    source_type='manual',
                    match_count=0,
                )
                for syn_text in synonyms_list
            ]

            enum = FieldEnumValue(
                value_id=str(row['enum_value_id']),
                field_id=field_id,
                standard_value=row['original_value'],
                display_name=row.get('display_value'),
                is_active=row['is_active'],
                record_count=row.get('frequency', 0),
                synonyms=synonym_objs,
                includes_values=list(row['includes_values']) if row.get('includes_values') else None,
            )
            field_enums[field_id].append(enum)

        logger.debug(
            "加载字段枚举值",
            connection_id=connection_id,
            total=sum(len(v) for v in field_enums.values()),
            fields=len(field_enums),
        )
        return field_enums

    async def _load_joins(self, conn, connection_id: str) -> List[DatasourceJoin]:
        rows = await conn.fetch(
            """
            SELECT relationship_id, left_table_id, right_table_id,
                   left_column_id, right_column_id,
                   relationship_type, join_type, is_active
            FROM table_relationships
            WHERE connection_id = $1 AND is_active = true
            ORDER BY left_table_id, right_table_id
            """,
            connection_id,
        )

        joins: List[DatasourceJoin] = []
        for row in rows:
            left_col_row = await conn.fetchrow(
                "SELECT column_name FROM db_columns WHERE column_id = $1",
                row['left_column_id'],
            )
            right_col_row = await conn.fetchrow(
                "SELECT column_name FROM db_columns WHERE column_id = $1",
                row['right_column_id'],
            )

            if left_col_row and right_col_row:
                join_condition = (
                    f"{row['left_table_id']}.{left_col_row['column_name']} = "
                    f"{row['right_table_id']}.{right_col_row['column_name']}"
                )
                joins.append(
                    DatasourceJoin(
                        join_id=str(row['relationship_id']),
                        from_datasource_id=str(row['left_table_id']),
                        to_datasource_id=str(row['right_table_id']),
                        join_type=row['join_type'] or 'inner',
                        join_condition=join_condition,
                        is_bidirectional=True,
                        priority=5,
                    )
                )

        logger.debug("加载表关系", connection_id=connection_id, count=len(joins))
        return joins
