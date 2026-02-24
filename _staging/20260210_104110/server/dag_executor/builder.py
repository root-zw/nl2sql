"""
DAG 构建器
负责将 CoT 规划步骤转换为包含 IR 的可执行 DAG
"""

import structlog
from typing import List, Dict, Any
import uuid
import asyncio

from server.dag_executor.models import ExecutionPlan, DAGNode, DAGDependency
from server.dependencies import create_nl2ir_parser

logger = structlog.get_logger()

class DAGBuilder:
    """DAG 构建器"""
    
    async def build(
        self, 
        plan_steps: List[Dict[str, Any]], 
        connection_id: str, 
        domain_id: str = None,
        user_id: str = None,
        user_role: str = None
    ) -> ExecutionPlan:
        """
        将 CoT 步骤转换为可执行的 DAG
        
        Args:
            plan_steps: CoTPlanner 生成的步骤列表
            connection_id: 数据库连接ID
            domain_id: 业务域ID (可选)
            user_id: 用户ID (用于权限过滤)
            user_role: 用户角色 (用于权限过滤)
            
        Returns:
            ExecutionPlan
        """
        logger.debug("开始构建 DAG", steps_count=len(plan_steps), connection_id=connection_id)
        
        # 获取 Parser
        parser = await create_nl2ir_parser(connection_id)
        
        nodes: Dict[str, DAGNode] = {}
        
        # 准备并行解析任务
        async def parse_step(step: Dict[str, Any]) -> DAGNode:
            step_id = step["id"]
            sub_q = step["sub_question"]
            
            logger.debug(f"解析步骤: {step_id}", question=sub_q)
            
            try:
                # 调用 NL2IR（带用户权限过滤）
                ir, confidence = await parser.parse(
                    sub_q, 
                    user_specified_domain=domain_id,
                    user_id=user_id,
                    user_role=user_role
                )
                
                # 构建依赖对象
                deps = []
                for d in step.get("dependencies", []):
                    deps.append(DAGDependency(
                        from_node_id=d["from_id"],
                        type=d.get("type", "filter_in"),
                        target_field=d.get("target_field_hint"),
                        source_column=d.get("source_column") or d.get("source_field_hint")
                    ))
                
                return DAGNode(
                    id=step_id,
                    description=step["description"],
                    dependencies=deps,
                    ir=ir,
                    type="query"
                )
            except Exception as e:
                logger.error(f"步骤 {step_id} 解析失败", question=sub_q, error=str(e))
                raise

        # 顺序解析所有步骤，避免并发导致的 LLM 状态污染
        for step in plan_steps:
            node = await parse_step(step)
            nodes[node.id] = node
            
        # 标记拥有下游依赖的节点，便于执行阶段进行上下文优化
        downstream_map = {node_id: [] for node_id in nodes}
        for node in nodes.values():
            for dep in node.dependencies:
                if dep.from_node_id in downstream_map:
                    downstream_map[dep.from_node_id].append(node.id)

        for node_id, children in downstream_map.items():
            if not children:
                continue
            node = nodes[node_id]
            node.metadata["downstream_nodes"] = list(children)
            node.metadata["context_only"] = True
            if node.ir and node.ir.query_type == "detail":
                node.ir.suppress_detail_defaults = True
                logger.debug(
                    "已为上下文节点启用精简模式",
                    node_id=node_id,
                    downstream=children
                )
        
        plan = ExecutionPlan(
            plan_id=str(uuid.uuid4()),
            nodes=nodes
        )
        logger.debug("DAG 构建完成", plan_id=plan.plan_id, nodes_count=len(nodes))
        return plan

# 全局实例
_builder = DAGBuilder()

def get_dag_builder() -> DAGBuilder:
    return _builder



