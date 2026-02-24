"""
混合架构查询服务 - 统一入口

根据 LLM 在 IR 中的标记智能选择处理流程：
1. 标准 IR 流程 - 使用 NL2IR + 编译器（包括增强IR功能）
2. 复杂拆分流程 - is_too_complex=true，使用 CoT + DAG
3. 直接 SQL 流程 - requires_direct_sql=true，LLM 直接生成 SQL + 后处理

路由策略完全依赖 LLM 判断，不使用程序化的关键字匹配。
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
import structlog

from server.config import settings
from server.models.ir import IntermediateRepresentation
from server.query_complexity.router import QueryRouter, RoutingDecision, get_query_router
from server.compiler.sql_post_processor import SQLPostProcessor, SQLValidationResult
from server.nl2ir.direct_sql_generator import DirectSQLGenerator, DirectSQLResult

logger = structlog.get_logger()


@dataclass
class HybridQueryResult:
    """混合查询结果"""
    success: bool
    route_used: str  # standard_ir, complex_split, direct_sql
    sql: str
    ir: Optional[IntermediateRepresentation] = None
    confidence: float = 0.0
    routing_decision: Optional[RoutingDecision] = None
    post_processor_result: Optional[SQLValidationResult] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class HybridQueryService:
    """
    混合架构查询服务
    
    根据 LLM 在 IR 中的标记，智能选择最优的处理流程。
    不使用程序化的关键字匹配或规则推断。
    """
    
    def __init__(
        self,
        router: Optional[QueryRouter] = None,
        post_processor: Optional[SQLPostProcessor] = None,
        direct_sql_generator: Optional[DirectSQLGenerator] = None
    ):
        """
        初始化混合查询服务
        
        Args:
            router: 查询路由器
            post_processor: SQL 后处理器
            direct_sql_generator: 直接 SQL 生成器
        """
        self.router = router or get_query_router(
            enable_complex_split=settings.enable_complex_query_auto_execution,
            enable_direct_sql=settings.direct_sql_enabled
        )
        self.post_processor = post_processor
        self.direct_sql_generator = direct_sql_generator
        
    def analyze_query(
        self,
        question: str,
        ir: Optional[IntermediateRepresentation] = None
    ) -> RoutingDecision:
        """
        分析查询并决定路由
        
        完全基于 IR 中的 LLM 标记进行路由决策。
        
        Args:
            question: 用户问题
            ir: 已解析的 IR（必须提供）
        
        Returns:
            RoutingDecision 对象
        """
        return self.router.route(question, ir)
    
    def should_use_direct_sql(
        self,
        ir: Optional[IntermediateRepresentation] = None
    ) -> Tuple[bool, str]:
        """
        判断是否应该使用直接 SQL 生成
        
        Args:
            ir: 已解析的 IR
        
        Returns:
            (should_use, reason) 元组
        """
        if not settings.hybrid_architecture_enabled:
            return False, "混合架构未启用"
        
        if not settings.direct_sql_enabled:
            return False, "直接SQL生成未启用"
        
        return self.router.should_use_direct_sql(ir)
    
    def should_use_complex_split(
        self,
        ir: Optional[IntermediateRepresentation] = None
    ) -> Tuple[bool, str, List[str]]:
        """
        判断是否应该使用复杂拆分流程（CoT + DAG）
        
        Args:
            ir: 已解析的 IR
        
        Returns:
            (should_use, reason, suggested_subquestions) 元组
        """
        if not settings.hybrid_architecture_enabled:
            return False, "混合架构未启用", []
        
        if not settings.enable_complex_query_auto_execution:
            return False, "复杂查询自动执行未启用", []
        
        return self.router.should_use_complex_split(ir)
    
    def get_detected_features(
        self,
        ir: Optional[IntermediateRepresentation] = None
    ) -> List[str]:
        """
        获取 IR 中使用的增强功能列表（用于追踪）
        
        Args:
            ir: 已解析的 IR
        
        Returns:
            检测到的增强功能列表
        """
        if ir is None:
            return []
        
        decision = self.router.route("", ir)
        return decision.detected_features
    
    async def process_with_direct_sql(
        self,
        question: str,
        semantic_model,
        user_context: Optional[Dict[str, Any]] = None,
        row_level_filters: Optional[List[Dict[str, Any]]] = None,
        default_filters: Optional[List[Dict[str, Any]]] = None
    ) -> HybridQueryResult:
        """
        使用直接 SQL 生成流程处理查询
        
        Args:
            question: 用户问题
            semantic_model: 语义模型
            user_context: 用户上下文
            row_level_filters: 行级权限过滤
            default_filters: 默认过滤
        
        Returns:
            HybridQueryResult 对象
        """
        result = HybridQueryResult(
            success=False,
            route_used="direct_sql",
            sql=""
        )
        
        try:
            # 1. 确保有直接 SQL 生成器
            if self.direct_sql_generator is None:
                result.errors.append("直接SQL生成器未初始化")
                return result
            
            # 2. 构建表结构描述
            table_schema = DirectSQLGenerator.build_table_schema_from_model(semantic_model)
            
            # 3. 生成 SQL
            sql_result = await self.direct_sql_generator.generate(
                question=question,
                table_schema=table_schema,
                context=user_context
            )
            
            if not sql_result.success:
                result.errors.append(sql_result.explanation)
                result.warnings.extend(sql_result.warnings)
                return result
            
            result.sql = sql_result.sql
            result.confidence = sql_result.confidence
            result.metadata["tables_used"] = sql_result.tables_used
            result.metadata["llm_explanation"] = sql_result.explanation
            
            # 4. SQL 后处理（安全检查 + 权限注入）
            if settings.sql_post_processor_enabled and self.post_processor:
                post_result = self.post_processor.process(
                    sql=sql_result.sql,
                    user_context=user_context,
                    row_level_filters=row_level_filters if settings.sql_post_processor_inject_filters else None,
                    default_filters=default_filters if settings.sql_post_processor_inject_filters else None,
                    skip_table_validation=settings.sql_post_processor_skip_table_validation
                )
                
                result.post_processor_result = post_result
                result.warnings.extend(post_result.warnings)
                
                if not post_result.is_valid:
                    result.errors.extend(post_result.errors)
                    return result
                
                result.sql = post_result.sql
                
                if post_result.applied_filters:
                    result.metadata["applied_filters"] = post_result.applied_filters
                if post_result.applied_limit:
                    result.metadata["applied_limit"] = post_result.applied_limit
            
            result.success = True
            logger.info(
                "直接SQL生成成功",
                question=question[:50],
                sql_length=len(result.sql),
                confidence=result.confidence
            )
            
        except Exception as e:
            logger.error("直接SQL生成流程失败", error=str(e), question=question[:50])
            result.errors.append(f"直接SQL生成失败: {str(e)}")
        
        return result
    
    def validate_ir_for_enhanced_features(
        self,
        ir: IntermediateRepresentation
    ) -> Tuple[bool, List[str]]:
        """
        验证 IR 是否正确使用了增强功能
        
        Args:
            ir: 中间表示
        
        Returns:
            (is_valid, issues) 元组
        """
        issues = []
        
        # 检查条件聚合
        if hasattr(ir, 'conditional_metrics') and ir.conditional_metrics:
            for cond_metric in ir.conditional_metrics:
                if not hasattr(cond_metric, 'alias') or not cond_metric.alias:
                    issues.append("条件聚合指标缺少别名")
                if not hasattr(cond_metric, 'condition') or not cond_metric.condition:
                    issues.append("条件聚合指标缺少条件")
        
        # 检查 HAVING
        if hasattr(ir, 'having_filters') and ir.having_filters:
            if ir.query_type != "aggregation":
                issues.append("HAVING过滤只能用于聚合查询")
        
        # 检查占比指标
        if hasattr(ir, 'ratio_metrics') and ir.ratio_metrics:
            for ratio_metric in ir.ratio_metrics:
                if not hasattr(ratio_metric, 'alias') or not ratio_metric.alias:
                    issues.append("占比指标缺少别名")
                if not hasattr(ratio_metric, 'numerator_field') or not ratio_metric.numerator_field:
                    issues.append("占比指标缺少分子字段")
        
        # 检查计算字段
        if hasattr(ir, 'calculated_fields') and ir.calculated_fields:
            for calc_field in ir.calculated_fields:
                if not hasattr(calc_field, 'alias') or not calc_field.alias:
                    issues.append("计算字段缺少别名")
                if not hasattr(calc_field, 'expression') or not calc_field.expression:
                    issues.append("计算字段缺少表达式")
        
        return len(issues) == 0, issues


async def get_hybrid_query_service(
    connection_id: str,
    semantic_model=None,
    dialect: str = "tsql"
) -> HybridQueryService:
    """
    获取混合查询服务实例
    
    Args:
        connection_id: 数据库连接ID
        semantic_model: 语义模型（可选）
        dialect: SQL 方言
    
    Returns:
        HybridQueryService 实例
    """
    # 创建路由器
    router = get_query_router(
        enable_complex_split=settings.enable_complex_query_auto_execution,
        enable_direct_sql=settings.direct_sql_enabled
    )
    
    # 创建后处理器
    post_processor = None
    if settings.sql_post_processor_enabled:
        if semantic_model:
            post_processor = SQLPostProcessor.from_semantic_model(semantic_model, dialect)
        else:
            post_processor = SQLPostProcessor(dialect=dialect)
    
    # 创建直接 SQL 生成器
    direct_sql_generator = None
    if settings.direct_sql_enabled:
        from server.nl2ir.direct_sql_generator import get_direct_sql_generator
        direct_sql_generator = await get_direct_sql_generator(connection_id, dialect)
    
    return HybridQueryService(
        router=router,
        post_processor=post_processor,
        direct_sql_generator=direct_sql_generator
    )
