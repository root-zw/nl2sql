"""
DAG 结果聚合器
负责在存在多个终点节点时，组织和返回统一的查询结果
"""

from typing import Dict, Any, List, Tuple
import structlog

from server.models.api import QueryResult
from server.dag_executor.models import ExecutionPlan, DAGNode

logger = structlog.get_logger()


class ResultAggregator:
    """结果聚合器"""

    def aggregate(
        self,
        plan: ExecutionPlan,
        results_cache: Dict[str, Dict[str, Any]],
    ) -> Tuple[QueryResult, DAGNode]:
        """
        聚合 DAG 终点节点的结果

        Returns:
            (QueryResult, DAGNode) -> 主结果及对应节点
        """
        final_nodes = self._get_final_nodes(plan)
        if not final_nodes:
            raise RuntimeError("DAG 结果聚合失败：未找到终点节点")

        primary_node = self._select_primary_node(plan, final_nodes)
        primary_data = results_cache.get(primary_node.id)
        if not primary_data:
            raise RuntimeError(f"DAG 结果缺失: {primary_node.id}")

        meta = {
            "sql": primary_data["sql"],
            "dag_plan_id": plan.plan_id,
            "dag_primary_node_id": primary_node.id,
            "is_complex_dag": True,
        }

        additional_outputs = self._build_additional_outputs(
            primary_node, final_nodes, results_cache
        )
        if additional_outputs:
            meta["dag_additional_outputs"] = additional_outputs

        context_exports = self._collect_context_exports(plan, results_cache)
        if context_exports:
            meta["dag_context_exports"] = context_exports

        result = QueryResult(
            columns=primary_data["columns"],
            rows=primary_data["rows"],
            meta=meta,
        )
        return result, primary_node

    def _get_final_nodes(self, plan: ExecutionPlan) -> List[DAGNode]:
        """获取出度为0的终点节点"""
        out_degree = {node_id: 0 for node_id in plan.nodes}
        for node in plan.nodes.values():
            for dep in node.dependencies:
                if dep.from_node_id in out_degree:
                    out_degree[dep.from_node_id] += 1

        final_node_ids = [node_id for node_id, degree in out_degree.items() if degree == 0]
        if not final_node_ids:
            return []

        return [plan.nodes[node_id] for node_id in final_node_ids]

    def _select_primary_node(
        self,
        plan: ExecutionPlan,
        final_nodes: List[DAGNode],
    ) -> DAGNode:
        """选择一个主输出节点"""
        # 1) 优先选择 type 为 aggregation 的节点
        for node in final_nodes:
            if node.type == "aggregation":
                return node

        # 2) 其次根据 execution_order 给出的顺序
        if plan.execution_order:
            ordered_ids = [node_id for batch in plan.execution_order for node_id in batch]
            for node_id in reversed(ordered_ids):
                for node in final_nodes:
                    if node.id == node_id:
                        return node

        # 3) 否则选最后一个终点
        return final_nodes[-1]

    def _build_additional_outputs(
        self,
        primary_node: DAGNode,
        final_nodes: List[DAGNode],
        results_cache: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """构建额外终点节点的输出，存入 meta"""
        extras = []
        for node in final_nodes:
            if node.id == primary_node.id:
                continue
            data = results_cache.get(node.id)
            if not data:
                logger.warning("终点节点缺少结果", node=node.id)
                continue
            extras.append(
                {
                    "node_id": node.id,
                    "description": node.description,
                    "columns": data["columns"],
                    "rows": data["rows"],
                    "sql": data["sql"],
                }
            )
        return extras

    def _collect_context_exports(
        self,
        plan: ExecutionPlan,
        results_cache: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """聚合节点导出的上下文取值摘要"""
        exports: List[Dict[str, Any]] = []
        for node_id, node in plan.nodes.items():
            node_result = results_cache.get(node_id) or {}
            node_exports = node_result.get("__context_exports__")
            if not node_exports:
                continue
            exports.append({
                "node_id": node_id,
                "description": node.description,
                "exports": node_exports
            })
        return exports


_aggregator = ResultAggregator()


def get_result_aggregator() -> ResultAggregator:
    return _aggregator

