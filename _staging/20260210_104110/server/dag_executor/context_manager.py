"""
智能上下文管理器
负责在 DAG 节点执行前，根据依赖关系注入上下文（值注入或子查询重写）
"""

import structlog
from typing import Dict, Any
from server.config import settings
from server.dag_executor.models import DAGNode
from server.models.ir import FilterCondition

logger = structlog.get_logger()

class SmartContextManager:
    """智能上下文管理器"""

    def inject_context(self, node: DAGNode, previous_results: Dict[str, Dict[str, Any]]) -> DAGNode:
        """
        注入上下文依赖
        
        Args:
            node: 当前节点
            previous_results: 之前节点的结果 {node_id: {"rows": [], "columns": [], "sql": "..."}}
            
        Returns:
            注入上下文后的新 Node (副本)
        """
        if not node.dependencies or not node.ir:
            return node
            
        # 创建副本以避免修改原始计划
        new_node = node.model_copy(deep=True)
        
        for dep in new_node.dependencies:
            result_data = previous_results.get(dep.from_node_id)
            if not result_data:
                logger.warning(f"未找到依赖节点结果: {dep.from_node_id}")
                continue
                
            target_field = dep.target_field
            if not target_field:
                # 如果 LLM 没有指定 target_field，这里尝试猜测或者跳过
                # 暂时跳过
                logger.warning(f"依赖未指定目标字段: {dep.from_node_id} -> {node.id}")
                continue

            # 策略选择
            rows = result_data.get("rows", [])
            columns = result_data.get("columns", [])
            sql = result_data.get("sql", "")
            column_idx, column_name = self._resolve_column_pointer(columns, dep)

            if column_idx is None:
                logger.warning(
                    "无法解析依赖列，跳过上下文注入",
                    node_id=node.id,
                    dep_node=dep.from_node_id
                )
                continue
            
            threshold = max(settings.smart_context_value_threshold, 1)
            dep_type = dep.type or "filter_in"
            
            # 根据依赖类型选择处理策略
            if dep_type == "filter_value":
                # 单值过滤：取第一个值
                values = self._extract_column_values(rows, column_idx)
                single_value = values[0] if values else None
                if single_value is not None:
                    logger.debug(
                        "上下文注入 (SingleValue)",
                        node_id=node.id,
                        dep_node=dep.from_node_id,
                        value=single_value,
                        target_field=target_field
                    )
                    new_node.ir.filters.append(FilterCondition(
                        field=target_field,
                        op="=",
                        value=single_value
                    ))
                    self._record_value_exports(result_data, column_name, [single_value], target_field)
                else:
                    logger.warning("依赖结果为空，无法注入单值", node_id=node.id, dep_node=dep.from_node_id)
                    new_node.ir.filters.append(FilterCondition(field=target_field, op="=", value="__EMPTY_RESULT__"))
                    
            elif dep_type == "filter_not_in":
                # NOT IN 过滤：排除集合
                values = self._extract_column_values(rows, column_idx)
                if values:
                    logger.debug(
                        "上下文注入 (NOT IN)",
                        node_id=node.id,
                        dep_node=dep.from_node_id,
                        value_count=len(values),
                        target_field=target_field
                    )
                    new_node.ir.filters.append(FilterCondition(
                        field=target_field,
                        op="NOT IN",
                        value=values
                    ))
                    self._record_value_exports(result_data, column_name, values, target_field)
                else:
                    # 排除集合为空，不需要添加过滤条件
                    logger.debug("排除集合为空，跳过 NOT IN 注入", node_id=node.id, dep_node=dep.from_node_id)
                    
            elif dep_type in ("aggregate_input", "join_key"):
                # 这些类型暂时按 filter_in 处理，后续可扩展
                logger.debug(
                    f"上下文注入 ({dep_type}，按 IN 处理)",
                    node_id=node.id,
                    dep_node=dep.from_node_id
                )
                values = self._extract_column_values(rows, column_idx)
                if values:
                    new_node.ir.filters.append(FilterCondition(field=target_field, op="IN", value=values))
                    self._record_value_exports(result_data, column_name, values, target_field)
                    
            else:
                # 默认 filter_in 逻辑
                # 策略A: 小数据量 -> 值注入 (IN list)
                if len(rows) < threshold:
                    values = self._extract_column_values(rows, column_idx)
                    
                    if values:
                        logger.debug(
                            "上下文注入 (Values)",
                            node_id=node.id,
                            dep_node=dep.from_node_id,
                            value_count=len(values),
                            target_field=target_field,
                            source_column=column_name
                        )
                        new_node.ir.filters.append(FilterCondition(
                            field=target_field,
                            op="IN",
                            value=values
                        ))
                        self._record_value_exports(
                            result_data,
                            column_name,
                            values,
                            target_field
                        )
                    else:
                        logger.warning(
                            "依赖结果为空，注入空条件",
                            node_id=node.id,
                            dep_node=dep.from_node_id
                        )
                        new_node.ir.filters.append(FilterCondition(
                            field=target_field,
                            op="=",
                            value="__EMPTY_RESULT__" 
                        ))

                # 策略B: 大数据量 -> 子查询重写
                else:
                    if sql:
                        subquery_sql = self._build_subquery(sql, column_name)
                        logger.debug(
                            "上下文注入 (Subquery)",
                            node_id=node.id,
                            dep_node=dep.from_node_id,
                            target_field=target_field,
                            source_column=column_name
                        )
                        new_node.ir.filters.append(FilterCondition(
                            field=target_field,
                            op="IN_SUBQUERY",
                            value=subquery_sql
                        ))
                        self._record_subquery_export(
                            result_data,
                            column_name,
                            target_field,
                            subquery_sql
                        )
                    else:
                        logger.error(f"无法执行子查询注入，缺少SQL: {dep.from_node_id}")
                        values = self._extract_column_values(rows, column_idx)
                        new_node.ir.filters.append(FilterCondition(
                            field=target_field,
                            op="IN",
                            value=values
                        ))
                        self._record_value_exports(
                            result_data,
                            column_name,
                            values,
                            target_field
                        )

        return new_node

    def _resolve_column_pointer(
        self,
        columns,
        dependency
    ):
        """根据依赖信息推断上游列索引和列名"""
        preferred_names = []
        if dependency.source_column:
            preferred_names.append(dependency.source_column)
        if dependency.target_field:
            preferred_names.append(dependency.target_field)

        for name in preferred_names:
            idx = self._find_column_index(columns, name)
            if idx is not None:
                return idx, self._get_column_name(columns, idx)

        if columns:
            return 0, self._get_column_name(columns, 0)

        return None, None

    def _find_column_index(self, columns, target_name: str):
        """根据列名（忽略大小写与单位）定位列索引"""
        if not target_name:
            return None
        normalized_target = self._normalize_name(target_name)

        for idx, col in enumerate(columns):
            col_name = self._get_column_name(columns, idx)
            if col_name and self._normalize_name(col_name) == normalized_target:
                return idx
        return None

    def _get_column_name(self, columns, idx: int) -> str:
        if idx is None or idx >= len(columns):
            return ""
        col = columns[idx]
        if isinstance(col, dict):
            return col.get("name") or ""
        return str(col)

    def _normalize_name(self, name: str) -> str:
        return (name or "").split('(')[0].strip().lower()

    def _extract_column_values(self, rows, column_idx: int):
        """提取指定列的所有非空值，并去重"""
        if column_idx is None:
            return []
        seen = set()
        values = []
        for row in rows:
            if not row or column_idx >= len(row):
                continue
            value = row[column_idx]
            if value is None or value in seen:
                continue
            seen.add(value)
            values.append(value)
        return values

    def _build_subquery(self, source_sql: str, column_name: str) -> str:
        """根据列名构造只返回必要列的子查询"""
        if not column_name:
            return source_sql
        quoted_name = self._quote_identifier(column_name)
        select_keyword = "SELECT DISTINCT" if settings.smart_context_subquery_distinct else "SELECT"
        alias = settings.smart_context_subquery_alias or "ctx_subquery"
        alias = self._quote_identifier(alias)
        return f"{select_keyword} {quoted_name} FROM ({source_sql}) AS {alias}"

    def _quote_identifier(self, name: str) -> str:
        """简单的标识符引用，避免特殊字符导致语法错误"""
        if not name:
            return ""
        parts = [part.strip() for part in name.split(".")]
        quoted_parts = []
        for part in parts:
            if not part:
                continue
            if part.startswith('"') and part.endswith('"'):
                quoted_parts.append(part)
            elif part.startswith('[') and part.endswith(']'):
                quoted_parts.append(part)
            else:
                escaped = part.replace('"', '""')
                quoted_parts.append(f'"{escaped}"')
        return ".".join(quoted_parts) or f'"{name}"'

    def _record_value_exports(self, result_data, column_name, values, target_field):
        """记录用于上下文注入的取值摘要"""
        if result_data is None:
            return
        preview_limit = max(settings.smart_context_value_preview_limit, 1)
        export_entry = {
            "mode": "value_list",
            "column": column_name,
            "target_field": target_field,
            "value_count": len(values or []),
            "preview_values": (values or [])[:preview_limit]
        }
        exports = result_data.setdefault("__context_exports__", [])
        exports.append(export_entry)

    def _record_subquery_export(self, result_data, column_name, target_field, subquery_sql):
        """记录使用子查询注入时的摘要"""
        if result_data is None:
            return
        export_entry = {
            "mode": "subquery",
            "column": column_name,
            "target_field": target_field,
            "subquery": subquery_sql
        }
        exports = result_data.setdefault("__context_exports__", [])
        exports.append(export_entry)

# 全局实例
_context_manager = SmartContextManager()

def get_context_manager() -> SmartContextManager:
    return _context_manager



