"""Join 路径规划器"""

from typing import Set, List, Optional
import structlog

from server.models.semantic import SemanticModel, Join
from server.metadata.semantic_graph import SemanticGraph
from server.exceptions import CompilationError, AmbiguousJoinError

logger = structlog.get_logger()


class JoinPlanner:
    """Join 路径规划器"""

    def __init__(self, semantic_model: SemanticModel, semantic_graph: SemanticGraph):
        self.model = semantic_model
        self.graph = semantic_graph

    def plan_join_path(
        self,
        tables: Set[str],
        join_strategy: str = "matched",
        anti_join_table: Optional[str] = None
    ) -> List[Join]:
        """
        规划 Join 路径

        Args:
            tables: 涉及的所有表
            join_strategy: JOIN策略 (matched/left_unmatched/right_unmatched)
            anti_join_table: 反向匹配时的参照表（可选）

        Returns:
            Join 序列

        Raises:
            CompilationError: 无法找到路径
            AmbiguousJoinError: 路径不唯一
        """
        if len(tables) == 1:
            # 只有一张表，不需要 Join
            return []

        # 选择主表（通常是事实表）
        main_table = self._select_main_table(tables, join_strategy, anti_join_table)
        logger.debug("选择主表", main_table=main_table, join_strategy=join_strategy, anti_join_table=anti_join_table)

        # 从主表到其他表的路径
        join_sequence = []
        visited = {main_table}

        for table in tables:
            if table == main_table:
                continue

            # 查找路径
            path = self.graph.find_path(main_table, table)

            if not path:
                raise CompilationError(
                    f"无法找到从 {main_table} 到 {table} 的 Join 路径",
                    details={"from": main_table, "to": table},
                    suggestions=["检查语义模型中的 joins 配置", "确保表之间有连接关系"]
                )

            # 检查路径唯一性
            all_paths = self.graph.find_all_paths(main_table, table, max_depth=4)
            if len(all_paths) > 1:
                raise AmbiguousJoinError(
                    f"从 {main_table} 到 {table} 存在 {len(all_paths)} 条路径",
                    details={
                        "from": main_table,
                        "to": table,
                        "path_count": len(all_paths)
                    },
                    suggestions=["在语义模型中指定 Join 权重", "明确查询中的维度组合"]
                )

            #  根据JOIN策略动态调整JOIN类型
            for join in path:
                # 检查是否已经添加过这个 Join
                join_key = (join.from_table, join.to_table)
                if join_key not in [(j.from_table, j.to_table) for j in join_sequence]:
                    # 创建JOIN的副本并调整类型
                    adjusted_join = Join(
                        from_table=join.from_table,
                        to_table=join.to_table,
                        on=join.on,
                        type=self._get_join_type(join_strategy),
                        cardinality=join.cardinality,
                        dedup_strategy=join.dedup_strategy,
                        dedup_order_by=join.dedup_order_by
                    )
                    join_sequence.append(adjusted_join)
                    logger.debug(
                        "添加JOIN",
                        from_table=join.from_table,
                        to_table=join.to_table,
                        original_type=join.type,
                        adjusted_type=adjusted_join.type,
                        strategy=join_strategy
                    )

            visited.add(table)

        logger.debug("Join 路径规划完成", joins=len(join_sequence), strategy=join_strategy)
        return join_sequence

    def _get_join_type(self, strategy: str) -> str:
        """
        根据JOIN策略返回对应的JOIN类型

        Args:
            strategy: JOIN策略

        Returns:
            JOIN类型 (INNER/LEFT/RIGHT)
        """
        if strategy == "left_unmatched":
            return "LEFT"
        elif strategy == "right_unmatched":
            return "RIGHT"
        else:  # matched (default)
            return "INNER"

    def _select_main_table(
        self,
        tables: Set[str],
        join_strategy: str = "matched",
        anti_join_table: Optional[str] = None
    ) -> str:
        """
        选择主表（事实表优先）

        规则：
        1.  反向匹配时，排除anti_join_table，剩余表作为主表
        2. 以 f_ 开头的表（事实表）
        3. 表名中包含 fact 的表
        4. 否则选择第一个
        """
        #  反向匹配：排除参照表
        if join_strategy in ["left_unmatched", "right_unmatched"] and anti_join_table:
            candidate_tables = tables - {anti_join_table}
            if len(candidate_tables) == 1:
                main_table = list(candidate_tables)[0]
                logger.debug(
                    "反向匹配主表选择",
                    strategy=join_strategy,
                    anti_join_table=anti_join_table,
                    selected_main_table=main_table
                )
                return main_table
            # 如果有多个候选，继续使用下面的规则

        fact_tables = []

        for table in tables:
            source = self.model.sources.get(table)
            if not source:
                continue

            # 检查是否是事实表
            if table.startswith("f_") or "fact" in table.lower():
                fact_tables.append(table)

        if fact_tables:
            return fact_tables[0]

        # 否则返回第一个
        return list(tables)[0]

