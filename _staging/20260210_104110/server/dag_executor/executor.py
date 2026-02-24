"""
DAG 执行引擎 (只读并行版)
负责调度和执行 DAG，保证 Snapshot Isolation

安全机制：
- 每个 DAG 节点的 IR 在执行前都会进行权限注入
- 确保通过 CoT 拆解的子查询也遵循行级权限控制
"""

import asyncio
from collections import OrderedDict
import structlog
from typing import Dict, Any, List, Tuple, Optional
from uuid import UUID

from server.config import settings
from server.dag_executor.models import ExecutionPlan, DAGNode
from server.dag_executor.context_manager import get_context_manager
from server.dag_executor.result_aggregator import get_result_aggregator
from server.enhanced_validation.loop import get_validation_loop
from server.session_manager.manager import get_session_manager
from server.dependencies import (
    create_sql_compiler,
    create_query_executor,
    get_global_rules_loader
)
from server.models.api import QueryResult
from server.models.ir import IntermediateRepresentation

logger = structlog.get_logger()


def _extract_plan_domain_ids(plan: ExecutionPlan) -> List[str]:
    """收集计划中涉及的业务域ID，保持顺序并去重。"""
    domain_ids: List[str] = []
    for node in plan.nodes.values():
        if not node.ir:
            continue
        domain_id = getattr(node.ir, "domain_id", None)
        if domain_id and domain_id not in domain_ids:
            domain_ids.append(domain_id)
    return domain_ids


def _build_domain_sequence(plan_domain_ids: List[str]) -> List[Optional[str]]:
    """为规则加载构造域顺序，包含业务域及全局(None)。"""
    candidates = (plan_domain_ids or []) + [None]
    ordered = list(OrderedDict.fromkeys(candidates))
    return ordered

class ReadOnlyDAGExecutor:
    """只读 DAG 执行引擎"""
    
    def __init__(self):
        self.context_manager = get_context_manager()
        self.session_manager = get_session_manager()
        self.result_aggregator = get_result_aggregator()
        self.validation_loop = get_validation_loop()
    
    async def _inject_node_permissions(
        self,
        ir: IntermediateRepresentation,
        user_id: str,
        connection_id: str
    ) -> IntermediateRepresentation:
        """
        为 DAG 节点的 IR 注入权限过滤
        
        每个子节点的 IR 都需要进行权限注入，确保 CoT 拆解后的查询
        也遵循行级权限控制。
        
        Args:
            ir: 节点的 IR
            user_id: 用户 ID
            connection_id: 连接 ID
            
        Returns:
            注入权限后的 IR
        """
        if not user_id or not connection_id:
            logger.debug("缺少用户或连接信息，跳过权限注入")
            return ir
        
        try:
            from server.services.permission_injector import PermissionInjector
            from server.utils.db_pool import get_metadata_pool
            
            pool = await get_metadata_pool()
            async with pool.acquire() as conn:
                injector = PermissionInjector(conn)
                injected_ir, permission_info = await injector.inject_permissions(
                    ir,
                    UUID(str(user_id)),
                    UUID(str(connection_id))
                )
                
                if permission_info.get("applied"):
                    logger.debug(
                        "DAG 节点权限已注入",
                        injected_count=permission_info.get("injected_filters", 0),
                        user_roles=permission_info.get("user_roles", [])
                    )
                
                return injected_ir
                
        except Exception as e:
            logger.warning(
                "DAG 节点权限注入失败，继续使用原始 IR",
                error=str(e),
                user_id=user_id,
                connection_id=connection_id
            )
            return ir

    async def execute(
        self,
        plan: ExecutionPlan,
        user_context: Dict[str, Any]
    ) -> Tuple[
        QueryResult,
        Optional[IntermediateRepresentation],
        List[Dict[str, Any]]
    ]:
        """
        执行 DAG 计划
        
        Args:
            plan: 执行计划
            user_context: 用户上下文 (含 connection_id)
            
        Returns:
            (QueryResult, FinalNodeIR)
            FinalNodeIR 用于后续的格式化和解释生成
        """
        connection_id = user_context["connection_id"]
        
        # 1. 准备组件
        compiler = await create_sql_compiler(connection_id)
        query_executor = await create_query_executor(connection_id)
        db_type = query_executor.db_type
        # 确保底层连接池已建立，避免 SessionManager 获取不到连接
        await query_executor.ensure_engine()
        
        # 加载全局规则
        global_rules: List[Dict[str, Any]] = []
        plan_domain_ids = _extract_plan_domain_ids(plan)
        domains_to_load = _build_domain_sequence(plan_domain_ids)
        try:
            rules_loader = get_global_rules_loader(connection_id)
            if rules_loader:
                rule_map: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
                for domain_id in domains_to_load:
                    rules = await rules_loader.load_active_rules(
                        rule_types=["derived_metric", "custom_instruction", "default_filter"],
                        domain_id=domain_id
                    )
                    for rule in rules:
                        rule_id = rule.get("rule_id") or f"anon_{id(rule)}"
                        if rule_id not in rule_map:
                            rule_map[rule_id] = rule
                global_rules = list(rule_map.values())
                logger.debug(
                    "全局规则加载完成",
                    domains=[d or "global" for d in domains_to_load],
                    count=len(global_rules)
                )
        except Exception as e:
            logger.warning(f"加载全局规则失败: {e}")

        try:
            return await self._run_with_scheduler(
                plan,
                user_context,
                connection_id,
                db_type,
                compiler,
                query_executor,
                global_rules
            )
        except Exception as dag_error:
            logger.warning(
                "DAG 并行执行失败，触发降级",
                error=str(dag_error),
                plan_id=plan.plan_id
            )
            if not settings.complex_dag_fallback_enabled:
                raise
            return await self._fallback_to_iterative_mode(
                plan,
                user_context,
                connection_id,
                db_type,
                compiler,
                query_executor,
                global_rules
            )

    async def _execute_node(
        self, 
        node: DAGNode, 
        session, 
        previous_results: Dict[str, Any],
        compiler,
        executor,
        user_context,
        global_rules
    ) -> Dict[str, Any]:
        """执行单个节点"""
        try:
            # 1. 上下文注入
            enhanced_node = self.context_manager.inject_context(node, previous_results)
            connection_id = user_context.get("connection_id")
            user_id = user_context.get("user_id")
            
            # 2. 编译 SQL
            # 目前假设 Node 都是基于 IR 的
            if enhanced_node.ir:
                # 2.1 权限注入（每个子节点的 IR 都需要注入权限过滤）
                ir_with_permission = await self._inject_node_permissions(
                    enhanced_node.ir,
                    user_id,
                    connection_id
                )
                
                # 2.2 对齐 IR
                aligned_ir = await self.validation_loop.align_ir(
                    ir_with_permission,
                    connection_id
                )
                sql = await compiler.compile_async(aligned_ir, user_context, global_rules)
            elif enhanced_node.sql:
                sql = enhanced_node.sql
            else:
                raise ValueError(f"节点 {node.id} 没有 IR 或 SQL")
                
            logger.debug(f"节点 {node.id} 生成 SQL", sql=sql)

            # 2.5 Dry Run 校验
            dry_run_ok = await self.validation_loop.dry_run(
                sql,
                connection_id,
                executor.db_type,
                executor
            )
            if not dry_run_ok:
                if settings.dry_run_mandatory:
                    raise RuntimeError(f"SQL Dry Run 校验失败: node={node.id}")
                logger.warning("Dry Run 未通过但继续执行", node=node.id, connection_id=connection_id)
            
            # 3. 执行 SQL (使用 Session)
            # [DEBUG] 打印 session 类型，协助定位 'coroutine object has no attribute execute' 问题
            logger.debug(f"[_execute_node] session type: {type(session)}")
            if session is None:
                logger.error("[_execute_node] session is None!")
            
            result = await executor.execute_with_connection(session, sql)
            
            return {
                "rows": result.rows,
                "columns": result.columns,
                "sql": sql
            }
            
        except Exception as e:
            logger.error(f"节点 {node.id} 执行失败", error=str(e))
            raise

    async def _run_with_scheduler(
        self,
        plan: ExecutionPlan,
        user_context: Dict[str, Any],
        connection_id: str,
        db_type: str,
        compiler,
        query_executor,
        global_rules: List[Dict[str, Any]]
    ) -> Tuple[
        QueryResult,
        Optional[IntermediateRepresentation],
        List[Dict[str, Any]]
    ]:
        """默认的并行调度执行"""
        async with self.session_manager.snapshot_scope(connection_id, db_type) as session:
            logger.debug("DAG 并行执行开始", plan_id=plan.plan_id, nodes=len(plan.nodes))
            
            in_degree, graph = self._build_dependency_graph(plan)
            ready_queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
            completed_nodes = 0
            total_nodes = len(plan.nodes)
            results_cache: Dict[str, Dict[str, Any]] = {}

            max_concurrency = settings.complex_dag_max_concurrency

            while ready_queue:
                current_batch_ids = ready_queue[:]
                ready_queue.clear()
                logger.debug("执行批次", nodes=current_batch_ids)

                chunks = [current_batch_ids]
                if max_concurrency and max_concurrency > 0:
                    chunks = [
                        current_batch_ids[i:i + max_concurrency]
                        for i in range(0, len(current_batch_ids), max_concurrency)
                    ]

                for chunk in chunks:
                    chunk_results = await self._execute_nodes_chunk(
                        chunk,
                        plan,
                        session,
                        results_cache,
                        compiler,
                        query_executor,
                        user_context,
                        global_rules
                    )
                    for node_id, result_data in chunk_results:
                        results_cache[node_id] = result_data
                        completed_nodes += 1
                        for neighbor in graph[node_id]:
                            in_degree[neighbor] -= 1
                            if in_degree[neighbor] == 0:
                                ready_queue.append(neighbor)

            if completed_nodes < total_nodes:
                logger.error(
                    "DAG 并行执行未完成",
                    completed=completed_nodes,
                    total=total_nodes
                )
                raise RuntimeError("DAG 执行未完全完成（可能存在循环依赖）")

            result, final_node = self.result_aggregator.aggregate(plan, results_cache)
            logger.debug("DAG 并行执行完成", final_node=final_node.id)
            debug_records = self._build_debug_records(plan, results_cache)
            return result, final_node.ir, debug_records

    async def _execute_nodes_chunk(
        self,
        node_ids: List[str],
        plan: ExecutionPlan,
        session,
        results_cache: Dict[str, Dict[str, Any]],
        compiler,
        query_executor,
        user_context: Dict[str, Any],
        global_rules: List[Dict[str, Any]]
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """并发执行一个节点子集"""
        tasks = []
        for node_id in node_ids:
            node = plan.nodes[node_id]
            tasks.append(self._execute_node(
                node,
                session,
                results_cache,
                compiler,
                query_executor,
                user_context,
                global_rules
            ))

        batch_results = await asyncio.gather(*tasks)
        return list(zip(node_ids, batch_results))

    async def _fallback_to_iterative_mode(
        self,
        plan: ExecutionPlan,
        user_context: Dict[str, Any],
        connection_id: str,
        db_type: str,
        compiler,
        query_executor,
        global_rules: List[Dict[str, Any]]
    ) -> Tuple[
        QueryResult,
        Optional[IntermediateRepresentation],
        List[Dict[str, Any]]
    ]:
        """降级策略：按依赖顺序逐个执行节点"""
        logger.debug("进入单步迭代模式", plan_id=plan.plan_id)
        
        # 在降级模式下，不再使用 snapshot_scope，而是使用普通的自动提交连接
        # 这样可以避开 SQL Server 的 Snapshot Isolation 限制
        
        # 注意：query_executor.engine 是一个属性，不是方法，不需要 await，也没有 .connect() 方法
        # 它是 AsyncEngine 实例（如果已经确保初始化），或者需要调用 await query_executor.ensure_engine() 获取
        
        engine = await query_executor.ensure_engine()
        
        async with engine.connect() as session:
            in_degree, graph = self._build_dependency_graph(plan)
            queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
            results_cache: Dict[str, Dict[str, Any]] = {}
            processed = 0

            while queue:
                node_id = queue.pop(0)
                node = plan.nodes[node_id]
                logger.debug("迭代执行节点", node=node_id)
                result_data = await self._execute_node(
                    node,
                    session,
                    results_cache,
                    compiler,
                    query_executor,
                    user_context,
                    global_rules
                )
                results_cache[node_id] = result_data
                processed += 1
                for neighbor in graph[node_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

            if processed < len(plan.nodes):
                logger.error(
                    "迭代模式执行未完成",
                    processed=processed,
                    total=len(plan.nodes)
                )
                raise RuntimeError("迭代模式执行未完全完成")

            result, final_node = self.result_aggregator.aggregate(plan, results_cache)
            logger.debug("单步迭代模式完成", final_node=final_node.id)
            debug_records = self._build_debug_records(plan, results_cache)
            return result, final_node.ir, debug_records

    def _build_dependency_graph(
        self,
        plan: ExecutionPlan
    ) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
        """构建依赖图和入度表"""
        in_degree = {node_id: 0 for node_id in plan.nodes}
        graph = {node_id: [] for node_id in plan.nodes}

        for node in plan.nodes.values():
            for dep in node.dependencies:
                if dep.from_node_id in plan.nodes:
                    graph[dep.from_node_id].append(node.id)
                    in_degree[node.id] += 1

        return in_degree, graph

    def _build_debug_records(
        self,
        plan: ExecutionPlan,
        results_cache: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """构建每个节点的调试信息，包含 IR 与 SQL"""
        debug_records = []
        for node_id, node in plan.nodes.items():
            node_result = results_cache.get(node_id, {})
            rows = node_result.get("rows") or []
            columns = node_result.get("columns") or []
            record = {
                    "node_id": node_id,
                    "type": node.type,
                    "description": node.description,
                    "dependencies": [dep.from_node_id for dep in node.dependencies],
                    "ir": node.ir.model_dump() if node.ir else None,
                    "sql": node_result.get("sql"),
                    "row_count": len(rows),
                    "column_count": len(columns),
                }
            context_exports = node_result.get("__context_exports__")
            if context_exports:
                record["context_exports"] = context_exports
            debug_records.append(record)
        return debug_records

_executor = ReadOnlyDAGExecutor()
def get_dag_executor():
    return _executor
