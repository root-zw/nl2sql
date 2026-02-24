"""
DAG 执行引擎的数据模型
定义 Node, Edge, DAG 等核心结构
"""

from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field
from server.models.ir import IntermediateRepresentation

class DAGDependency(BaseModel):
    """节点依赖关系"""
    from_node_id: str = Field(..., description="依赖的上游节点ID")
    
    # 依赖类型
    # filter_in: 上游结果作为当前节点的 IN 条件 (WHERE field IN (...))
    # filter_value: 上游结果(单值)作为当前节点的等值条件 (WHERE field = ...)
    # filter_not_in: 上游结果作为当前节点的排除条件 (WHERE field NOT IN (...))
    # aggregate_input: 上游结果作为当前步骤的聚合输入
    # join_key: 上游结果作为关联键
    # subquery: 上游SQL作为当前节点的子查询
    type: Literal["filter_in", "filter_value", "filter_not_in", "aggregate_input", "join_key", "subquery"] = "filter_in"
    
    # 注入目标
    target_field: Optional[str] = Field(None, description="依赖注入到当前节点的哪个字段（维度ID或列名）")
    source_column: Optional[str] = Field(None, description="上游结果中用于传递的列名或别名")


class DAGNode(BaseModel):
    """DAG 节点"""
    id: str
    description: str = Field(..., description="节点功能的自然语言描述")
    
    # 依赖配置
    dependencies: List[DAGDependency] = Field(default_factory=list)
    
    # 执行内容 (二选一)
    # 1. 基于 IR (标准模式)
    ir: Optional[IntermediateRepresentation] = None
    
    # 2. 基于纯 SQL (特殊模式，如直接执行特定查询)
    sql: Optional[str] = None
    
    # 节点类型
    type: Literal["query", "process", "aggregation"] = "query"

    # 附加元数据（用于执行优化/调试）
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExecutionPlan(BaseModel):
    """
    执行计划 (DAG)
    包含所有节点及其关系
    """
    plan_id: str
    nodes: Dict[str, DAGNode]
    
    # 执行顺序通常由调度器动态决定，但这里可以存储拓扑排序后的推荐顺序
    execution_order: List[List[str]] = Field(default_factory=list, description="推荐的并行执行批次 [[node1, node2], [node3]]")

    @property
    def root_nodes(self) -> List[DAGNode]:
        """没有依赖的节点"""
        return [n for n in self.nodes.values() if not n.dependencies]



