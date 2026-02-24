"""语义图构建 - 用于 Join 路径规划"""

from typing import Dict, List, Optional, Set, Tuple
from collections import deque
import structlog

from server.models.semantic import Join
from server.exceptions import AmbiguousModelError

logger = structlog.get_logger()


class Edge:
    """图的边"""
    
    def __init__(self, to_table: str, join: Join):
        self.to_table = to_table
        self.join = join


class SemanticGraph:
    """语义图 - 管理表之间的 Join 关系"""
    
    def __init__(self, joins: List[Join]):
        self._graph: Dict[str, List[Edge]] = {}
        self._joins = joins
        self._build_graph(joins)
    
    def _build_graph(self, joins: List[Join]) -> None:
        """构建图结构（无向图）"""
        for join in joins:
            # 添加正向边
            if join.from_table not in self._graph:
                self._graph[join.from_table] = []
            self._graph[join.from_table].append(Edge(join.to_table, join))
            
            # 添加反向边（Join 是可以双向的）
            if join.to_table not in self._graph:
                self._graph[join.to_table] = []
            
            # 创建反向 Join
            reverse_join = Join(
                from_table=join.to_table,
                to_table=join.from_table,
                on=join.on,
                type=join.type,
                cardinality=self._reverse_cardinality(join.cardinality),
                dedup_strategy=join.dedup_strategy,
                dedup_order_by=join.dedup_order_by
            )
            self._graph[join.to_table].append(Edge(join.from_table, reverse_join))
        
        logger.info("语义图构建完成", nodes=len(self._graph), edges=len(joins) * 2)
    
    def _reverse_cardinality(self, cardinality: Optional[str]) -> Optional[str]:
        """反转基数关系"""
        if not cardinality:
            return None
        
        mapping = {
            "1:1": "1:1",
            "1:N": "N:1",
            "N:1": "1:N",
            "N:M": "N:M"
        }
        return mapping.get(cardinality, cardinality)
    
    def find_path(self, from_table: str, to_table: str) -> Optional[List[Join]]:
        """
        BFS 查找最短 Join 路径
        
        Args:
            from_table: 起始表
            to_table: 目标表
        
        Returns:
            Join 序列，如果不存在路径则返回 None
        """
        if from_table == to_table:
            return []
        
        if from_table not in self._graph:
            logger.warning("起始表不在语义图中", table=from_table)
            return None
        
        # BFS
        queue = deque([(from_table, [])])
        visited = {from_table}
        
        while queue:
            current, path = queue.popleft()
            
            # 遍历邻居
            for edge in self._graph.get(current, []):
                if edge.to_table in visited:
                    continue
                
                new_path = path + [edge.join]
                
                if edge.to_table == to_table:
                    return new_path
                
                visited.add(edge.to_table)
                queue.append((edge.to_table, new_path))
        
        return None
    
    def find_all_paths(
        self,
        from_table: str,
        to_table: str,
        max_depth: int = 5
    ) -> List[List[Join]]:
        """
        查找所有路径，用于检测歧义
        
        Args:
            from_table: 起始表
            to_table: 目标表
            max_depth: 最大深度
        
        Returns:
            所有可能的路径列表
        """
        if from_table == to_table:
            return [[]]
        
        all_paths = []
        
        def dfs(current: str, target: str, path: List[Join], visited: Set[str]):
            if len(path) > max_depth:
                return
            
            if current == target:
                all_paths.append(path[:])
                return
            
            for edge in self._graph.get(current, []):
                if edge.to_table not in visited:
                    visited.add(edge.to_table)
                    path.append(edge.join)
                    dfs(edge.to_table, target, path, visited)
                    path.pop()
                    visited.remove(edge.to_table)
        
        dfs(from_table, to_table, [], {from_table})
        return all_paths
    
    def validate_uniqueness(self) -> List[str]:
        """
        检查所有表对的路径唯一性
        
        Returns:
            警告信息列表，如果有多路径情况
        """
        warnings = []
        tables = list(self._graph.keys())
        
        for i, table1 in enumerate(tables):
            for table2 in tables[i + 1:]:
                paths = self.find_all_paths(table1, table2, max_depth=3)
                
                if len(paths) > 1:
                    warning = f"表 {table1} 到 {table2} 存在 {len(paths)} 条路径，可能导致 Join 歧义"
                    warnings.append(warning)
                    logger.warning(
                        "检测到多路径 Join",
                        from_table=table1,
                        to_table=table2,
                        path_count=len(paths)
                    )
        
        return warnings
    
    def get_all_tables(self) -> List[str]:
        """获取图中所有表名"""
        return list(self._graph.keys())
    
    def get_connected_tables(self, table: str) -> List[str]:
        """获取与指定表直接连接的表"""
        if table not in self._graph:
            return []
        return [edge.to_table for edge in self._graph[table]]

