"""
枚举同步完整性检查器

功能：
- 同步后检查高频枚举覆盖率
- 缺失高频枚举时报警或对该表降权
- 不阻塞查询，只记录警告
"""

from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
import structlog

from server.config import RetrievalConfig

logger = structlog.get_logger()


@dataclass
class EnumCoverageResult:
    """枚举覆盖率检查结果"""
    table_id: str
    table_name: str
    total_high_value_fields: int
    covered_fields: List[str]
    missing_fields: List[str]
    coverage_ratio: float
    passed: bool
    penalty_weight: float = 0.0  # 降权权重，0 表示不降权


@dataclass
class EnumSyncCheckReport:
    """枚举同步检查报告"""
    check_enabled: bool
    total_tables_checked: int
    passed_tables: int
    failed_tables: int
    table_results: List[EnumCoverageResult] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class EnumSyncChecker:
    """枚举同步完整性检查器"""
    
    def __init__(self, db_pool, milvus_client=None):
        """
        初始化检查器
        
        Args:
            db_pool: PostgreSQL 数据库连接池
            milvus_client: Milvus 客户端（可选）
        """
        self.db = db_pool
        self.milvus = milvus_client
        
        # 从配置加载参数
        self.enabled = RetrievalConfig.enum_sync_integrity_check_enabled()
        self.min_coverage_ratio = RetrievalConfig.enum_sync_min_coverage_ratio()
        self.high_value_fields = set(RetrievalConfig.enum_sync_high_value_fields())
    
    async def check_table_coverage(
        self,
        table_id: str,
        table_name: str,
        connection_id: Optional[str] = None
    ) -> EnumCoverageResult:
        """
        检查单个表的高价值枚举覆盖率
        
        Args:
            table_id: 表ID
            table_name: 表名
            connection_id: 连接ID（可选）
            
        Returns:
            EnumCoverageResult 覆盖率检查结果
        """
        if not self.enabled:
            return EnumCoverageResult(
                table_id=table_id,
                table_name=table_name,
                total_high_value_fields=0,
                covered_fields=[],
                missing_fields=[],
                coverage_ratio=1.0,
                passed=True
            )
        
        try:
            # 查询该表的所有维度字段
            query = """
                SELECT 
                    f.field_id,
                    f.display_name,
                    f.field_category
                FROM fields f
                JOIN db_columns c ON f.source_column_id = c.column_id
                JOIN db_tables t ON c.table_id = t.table_id
                WHERE t.table_id = $1
                  AND f.is_active = true
                  AND f.field_category IN ('dimension', 'identifier')
            """
            
            params = [table_id]
            if connection_id:
                query = query.replace("WHERE t.table_id = $1", 
                                     "WHERE t.table_id = $1 AND t.connection_id = $2")
                params.append(connection_id)
            
            fields = await self.db.fetch(query, *params)
            
            # 识别高价值字段
            high_value_field_names: Set[str] = set()
            table_field_names: Set[str] = set()
            
            for f in fields:
                display_name = f['display_name']
                table_field_names.add(display_name)
                
                # 检查是否为高价值字段
                for hv_field in self.high_value_fields:
                    if hv_field in display_name or display_name in hv_field:
                        high_value_field_names.add(display_name)
                        break
            
            if not high_value_field_names:
                # 该表没有高价值字段，视为通过
                return EnumCoverageResult(
                    table_id=table_id,
                    table_name=table_name,
                    total_high_value_fields=0,
                    covered_fields=[],
                    missing_fields=[],
                    coverage_ratio=1.0,
                    passed=True
                )
            
            # 查询 Milvus 中该表的枚举覆盖情况
            covered_fields: Set[str] = set()
            
            if self.milvus:
                try:
                    # 查询 Milvus 中有枚举值的字段
                    milvus_query = f'table_id == "{table_id}"'
                    results = self.milvus.query(
                        collection_name="enum_values_dual",
                        filter=milvus_query,
                        output_fields=["field_name"],
                        limit=1000
                    )
                    
                    for r in results:
                        field_name = r.get("field_name")
                        if field_name:
                            covered_fields.add(field_name)
                            
                except Exception as e:
                    logger.warning(
                        "Milvus枚举查询失败，使用PostgreSQL回退",
                        table_id=table_id,
                        error=str(e)
                    )
            
            # 如果 Milvus 查询失败或不可用，回退到 PostgreSQL
            if not covered_fields:
                enum_query = """
                    SELECT DISTINCT f.display_name
                    FROM field_enum_values e
                    JOIN fields f ON e.field_id = f.field_id
                    JOIN db_columns c ON f.source_column_id = c.column_id
                    WHERE c.table_id = $1
                      AND e.is_active = true
                """
                enum_results = await self.db.fetch(enum_query, table_id)
                covered_fields = {r['display_name'] for r in enum_results}
            
            # 计算覆盖率
            covered_high_value = high_value_field_names & covered_fields
            missing_high_value = high_value_field_names - covered_fields
            
            total = len(high_value_field_names)
            covered_count = len(covered_high_value)
            coverage_ratio = covered_count / total if total > 0 else 1.0
            
            passed = coverage_ratio >= self.min_coverage_ratio
            
            # 计算降权权重（覆盖率越低，降权越大）
            penalty_weight = 0.0
            if not passed:
                # 线性降权：覆盖率每低于阈值 10%，降权 0.1
                gap = self.min_coverage_ratio - coverage_ratio
                penalty_weight = min(0.5, gap * 1.0)  # 最大降权 0.5
            
            result = EnumCoverageResult(
                table_id=table_id,
                table_name=table_name,
                total_high_value_fields=total,
                covered_fields=list(covered_high_value),
                missing_fields=list(missing_high_value),
                coverage_ratio=coverage_ratio,
                passed=passed,
                penalty_weight=penalty_weight
            )
            
            if not passed:
                logger.warning(
                    "表枚举覆盖率不足",
                    table_id=table_id,
                    table_name=table_name,
                    coverage_ratio=f"{coverage_ratio:.2%}",
                    missing_fields=list(missing_high_value),
                    penalty_weight=penalty_weight
                )
            
            return result
            
        except Exception as e:
            logger.exception(
                "枚举覆盖率检查失败",
                table_id=table_id,
                error=str(e)
            )
            # 检查失败时不降权，但记录警告
            return EnumCoverageResult(
                table_id=table_id,
                table_name=table_name,
                total_high_value_fields=0,
                covered_fields=[],
                missing_fields=[],
                coverage_ratio=1.0,
                passed=True  # 失败时视为通过，避免误降权
            )
    
    async def check_all_tables(
        self,
        connection_id: Optional[str] = None
    ) -> EnumSyncCheckReport:
        """
        检查所有表的枚举覆盖率
        
        Args:
            connection_id: 连接ID（可选）
            
        Returns:
            EnumSyncCheckReport 完整检查报告
        """
        report = EnumSyncCheckReport(
            check_enabled=self.enabled,
            total_tables_checked=0,
            passed_tables=0,
            failed_tables=0
        )
        
        if not self.enabled:
            report.warnings.append("枚举同步完整性检查已禁用")
            return report
        
        try:
            # 获取所有已启用的表
            query = """
                SELECT table_id, display_name
                FROM db_tables
                WHERE is_included = true
            """
            params = []
            if connection_id:
                query += " AND connection_id = $1"
                params.append(connection_id)
            
            tables = await self.db.fetch(query, *params)
            
            for table in tables:
                table_id = str(table['table_id'])
                table_name = table['display_name']
                
                result = await self.check_table_coverage(
                    table_id, table_name, connection_id
                )
                
                report.table_results.append(result)
                report.total_tables_checked += 1
                
                if result.passed:
                    report.passed_tables += 1
                else:
                    report.failed_tables += 1
                    report.warnings.append(
                        f"表 [{table_name}] 高价值枚举覆盖率不足: "
                        f"{result.coverage_ratio:.2%}, 缺失: {result.missing_fields}"
                    )
            
            logger.info(
                "枚举同步完整性检查完成",
                total=report.total_tables_checked,
                passed=report.passed_tables,
                failed=report.failed_tables
            )
            
            return report
            
        except Exception as e:
            logger.exception("枚举同步完整性检查失败", error=str(e))
            report.warnings.append(f"检查过程出错: {str(e)}")
            return report
    
    def get_table_penalty(self, table_id: str, check_results: List[EnumCoverageResult]) -> float:
        """
        获取表的降权权重
        
        Args:
            table_id: 表ID
            check_results: 检查结果列表
            
        Returns:
            降权权重（0~1），0 表示不降权
        """
        for result in check_results:
            if result.table_id == table_id:
                return result.penalty_weight
        return 0.0
