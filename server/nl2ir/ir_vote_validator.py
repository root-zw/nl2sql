"""
IR投票验证器

对LLM3生成的IR进行投票验证，用于判断是否可以直接执行。
验证通过则跳过置信度评估直接执行，否则走人工确认→LLM2链路。

验证维度：
1. 字段存在率：IR中引用的字段ID在候选表中的存在比例
2. 表投票率：字段归属投票集中在选中表的比例
3. IR结构完整性：query_type/metrics/dimensions等核心字段的完整度
4. 过滤条件覆盖：问题中的关键条件是否被识别

使用场景：
- 向量表选择链路（第二条链路）
- LLM3生成IR后的质量验证
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
import structlog

from server.config import get_retrieval_param

if TYPE_CHECKING:
    from server.nl2ir.vector_table_selector import TableWithFields

logger = structlog.get_logger()


@dataclass
class IRVoteResult:
    """IR投票验证结果"""
    # 是否通过验证
    passed: bool = False
    
    # 综合评分 [0, 1]
    score: float = 0.0
    
    # 各维度评分
    field_existence_rate: float = 0.0   # 字段存在率
    table_vote_rate: float = 0.0        # 表投票率
    ir_completeness: float = 0.0        # IR结构完整性
    filter_coverage: float = 0.0        # 过滤条件覆盖率
    
    # 各维度是否达标
    field_existence_passed: bool = False
    table_vote_passed: bool = False
    ir_completeness_passed: bool = False
    
    # 详细信息
    total_fields: int = 0               # IR中引用的字段总数
    existing_fields: int = 0            # 存在的字段数
    missing_fields: List[str] = field(default_factory=list)  # 不存在的字段ID
    derived_metrics: List[str] = field(default_factory=list)  # 派生指标列表
    missing_derived: List[str] = field(default_factory=list)  # 未找到定义的派生指标
    
    # 投票详情
    vote_distribution: Dict[str, int] = field(default_factory=dict)  # 表ID -> 票数
    selected_table_votes: int = 0       # 选中表的票数
    
    # 验证说明
    reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典用于日志和trace"""
        return {
            "passed": self.passed,
            "score": round(self.score, 4),
            "field_existence_rate": round(self.field_existence_rate, 4),
            "table_vote_rate": round(self.table_vote_rate, 4),
            "ir_completeness": round(self.ir_completeness, 4),
            "filter_coverage": round(self.filter_coverage, 4),
            "field_existence_passed": self.field_existence_passed,
            "table_vote_passed": self.table_vote_passed,
            "ir_completeness_passed": self.ir_completeness_passed,
            "total_fields": self.total_fields,
            "existing_fields": self.existing_fields,
            "missing_fields": self.missing_fields[:5],  # 只展示前5个
            "derived_metrics": self.derived_metrics,
            "missing_derived": self.missing_derived,
            "vote_distribution": self.vote_distribution,
            "selected_table_votes": self.selected_table_votes,
            "reasons": self.reasons,
            "warnings": self.warnings,
        }


class IRVoteValidator:
    """
    IR投票验证器
    
    验证LLM3生成的IR质量，判断是否可以直接执行
    """
    
    def __init__(self):
        """初始化验证器，从配置加载阈值和权重"""
        self._load_config()
    
    def _load_config(self) -> None:
        """从配置文件加载验证参数"""
        # 是否启用
        self.enabled = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.enabled",
            True
        )
        
        # 综合通过阈值
        self.pass_threshold = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.pass_threshold",
            0.75
        )
        
        # 各验证项阈值
        self.field_existence_threshold = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.thresholds.field_existence",
            0.85
        )
        self.table_vote_threshold = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.thresholds.table_vote",
            0.60
        )
        self.ir_completeness_threshold = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.thresholds.ir_completeness",
            0.60
        )
        
        # 各验证项权重
        self.weight_field_existence = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.weights.field_existence",
            0.40
        )
        self.weight_table_vote = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.weights.table_vote",
            0.25
        )
        self.weight_ir_completeness = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.weights.ir_completeness",
            0.20
        )
        self.weight_filter_coverage = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.weights.filter_coverage",
            0.15
        )
        
        # 派生指标验证
        self.validate_derived_existence = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.derived_metric.validate_existence",
            True
        )
        self.derived_missing_handling = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.derived_metric.missing_handling",
            "warn"
        )
        
        # 异常处理
        self.fallback_strategy = get_retrieval_param(
            "vector_table_selection.ir_vote_validation.error_handling.fallback_strategy",
            "original"
        )
    
    def is_enabled(self) -> bool:
        """检查是否启用IR投票验证"""
        return self.enabled
    
    def validate(
        self,
        ir: Dict[str, Any],
        selected_table_id: str,
        tables_with_fields: List["TableWithFields"],
        global_rules: Optional[List[Dict[str, Any]]] = None,
    ) -> IRVoteResult:
        """
        验证IR质量
        
        Args:
            ir: LLM3生成的IR字典
            selected_table_id: 选中的表ID
            tables_with_fields: 候选表列表（含完整字段信息）
            global_rules: 全局规则（用于验证派生指标）
        
        Returns:
            IRVoteResult 验证结果
        """
        result = IRVoteResult()
        
        try:
            if not ir:
                result.reasons.append("IR为空")
                return result
            
            if not selected_table_id:
                result.reasons.append("未选中表")
                return result
            
            if not tables_with_fields:
                result.reasons.append("无候选表信息")
                return result
            
            # 1. 验证字段存在率
            self._validate_field_existence(ir, selected_table_id, tables_with_fields, result)
            
            # 2. 验证表投票率
            self._validate_table_vote(ir, selected_table_id, tables_with_fields, result)
            
            # 3. 验证IR结构完整性
            self._validate_ir_completeness(ir, result)
            
            # 4. 验证过滤条件覆盖
            self._validate_filter_coverage(ir, result)
            
            # 5. 验证派生指标（如果启用）
            if self.validate_derived_existence:
                self._validate_derived_metrics(ir, global_rules, result)
            
            # 6. 计算综合评分
            self._calculate_final_score(result)
            
            # 7. 判断是否通过
            result.passed = self._evaluate_pass(result)
            
            logger.info(
                "IR投票验证完成",
                passed=result.passed,
                score=round(result.score, 4),
                field_existence=round(result.field_existence_rate, 4),
                table_vote=round(result.table_vote_rate, 4),
                ir_completeness=round(result.ir_completeness, 4),
            )
            
        except Exception as e:
            logger.exception("IR投票验证异常", error=str(e))
            result.reasons.append(f"验证异常: {str(e)}")
            # 根据配置决定异常时的处理
            if self.fallback_strategy == "pass":
                result.passed = True
                result.warnings.append("验证异常，按配置默认通过")
            elif self.fallback_strategy == "fail":
                result.passed = False
                result.warnings.append("验证异常，按配置默认不通过")
            # fallback_strategy == "original" 时返回 passed=False，让外层回退到原逻辑
        
        return result
    
    def _validate_field_existence(
        self,
        ir: Dict[str, Any],
        selected_table_id: str,
        tables_with_fields: List["TableWithFields"],
        result: IRVoteResult
    ) -> None:
        """验证字段存在率"""
        # 找到选中的表
        selected_table = None
        for table in tables_with_fields:
            if table.table_id == selected_table_id:
                selected_table = table
                break
        
        if not selected_table:
            result.reasons.append(f"选中表ID {selected_table_id} 不在候选列表中")
            result.field_existence_rate = 0.0
            return
        
        # 收集表中所有字段ID
        table_field_ids: Set[str] = set()
        for f in selected_table.dimensions:
            table_field_ids.add(f.field_id)
        for f in selected_table.measures:
            table_field_ids.add(f.field_id)
        for f in selected_table.identifiers:
            table_field_ids.add(f.field_id)
        
        # 从IR中提取所有引用的字段ID
        ir_field_ids: List[str] = []
        derived_metrics: List[str] = []
        
        # metrics
        for metric in ir.get("metrics", []):
            if isinstance(metric, str):
                if metric.startswith("derived:"):
                    derived_metrics.append(metric)
                elif metric != "__row_count__":
                    ir_field_ids.append(metric)
        
        # dimensions
        for dim in ir.get("dimensions", []):
            if isinstance(dim, str):
                ir_field_ids.append(dim)
        
        # filters
        for flt in ir.get("filters", []):
            if isinstance(flt, dict):
                field_id = flt.get("field")
                if field_id and isinstance(field_id, str):
                    ir_field_ids.append(field_id)
        
        # sort_by
        sort_by = ir.get("sort_by")
        if sort_by and isinstance(sort_by, str) and not sort_by.startswith("derived:"):
            ir_field_ids.append(sort_by)
        
        # having_filters
        for hf in ir.get("having_filters", []):
            if isinstance(hf, dict):
                field_id = hf.get("field")
                if field_id and isinstance(field_id, str) and not field_id.startswith("derived:"):
                    ir_field_ids.append(field_id)
        
        # 去重
        ir_field_ids = list(set(ir_field_ids))
        
        result.total_fields = len(ir_field_ids)
        result.derived_metrics = derived_metrics
        
        if not ir_field_ids:
            # 没有普通字段引用（可能全是派生指标或__row_count__）
            if derived_metrics or "__row_count__" in str(ir.get("metrics", [])):
                result.field_existence_rate = 1.0
                result.field_existence_passed = True
                result.reasons.append("仅使用派生指标或计数，字段验证跳过")
            else:
                result.field_existence_rate = 0.0
                result.reasons.append("IR未引用任何字段")
            return
        
        # 检查字段存在性
        existing_fields = []
        missing_fields = []
        for fid in ir_field_ids:
            if fid in table_field_ids:
                existing_fields.append(fid)
            else:
                missing_fields.append(fid)
        
        result.existing_fields = len(existing_fields)
        result.missing_fields = missing_fields
        result.field_existence_rate = len(existing_fields) / len(ir_field_ids)
        result.field_existence_passed = result.field_existence_rate >= self.field_existence_threshold
        
        if missing_fields:
            result.warnings.append(f"以下字段在选中表中不存在: {missing_fields[:3]}")
    
    def _validate_table_vote(
        self,
        ir: Dict[str, Any],
        selected_table_id: str,
        tables_with_fields: List["TableWithFields"],
        result: IRVoteResult
    ) -> None:
        """验证表投票率"""
        # 构建字段ID -> 表ID映射
        field_to_table: Dict[str, str] = {}
        for table in tables_with_fields:
            for f in table.dimensions:
                field_to_table[f.field_id] = table.table_id
            for f in table.measures:
                field_to_table[f.field_id] = table.table_id
            for f in table.identifiers:
                field_to_table[f.field_id] = table.table_id
        
        # 从IR中提取所有引用的字段ID（同上）
        ir_field_ids: List[str] = []
        
        for metric in ir.get("metrics", []):
            if isinstance(metric, str) and not metric.startswith("derived:") and metric != "__row_count__":
                ir_field_ids.append(metric)
        
        for dim in ir.get("dimensions", []):
            if isinstance(dim, str):
                ir_field_ids.append(dim)
        
        for flt in ir.get("filters", []):
            if isinstance(flt, dict):
                field_id = flt.get("field")
                if field_id and isinstance(field_id, str):
                    ir_field_ids.append(field_id)
        
        sort_by = ir.get("sort_by")
        if sort_by and isinstance(sort_by, str) and not sort_by.startswith("derived:"):
            ir_field_ids.append(sort_by)
        
        if not ir_field_ids:
            # 无可投票字段，默认通过
            result.table_vote_rate = 1.0
            result.table_vote_passed = True
            return
        
        # 投票统计
        votes: Dict[str, int] = {}
        total_votes = 0
        for fid in ir_field_ids:
            table_id = field_to_table.get(fid)
            if table_id:
                votes[table_id] = votes.get(table_id, 0) + 1
                total_votes += 1
        
        result.vote_distribution = votes
        
        if total_votes == 0:
            result.table_vote_rate = 0.0
            result.table_vote_passed = False
            result.reasons.append("所有引用字段均无法映射到候选表")
            return
        
        # 计算选中表的投票率
        selected_votes = votes.get(selected_table_id, 0)
        result.selected_table_votes = selected_votes
        result.table_vote_rate = selected_votes / total_votes
        result.table_vote_passed = result.table_vote_rate >= self.table_vote_threshold
        
        if not result.table_vote_passed:
            # 找出得票最多的表
            if votes:
                best_table = max(votes.items(), key=lambda x: x[1])
                if best_table[0] != selected_table_id:
                    result.warnings.append(
                        f"字段投票显示表 {best_table[0]} 更匹配（{best_table[1]}票），"
                        f"但LLM选择了 {selected_table_id}（{selected_votes}票）"
                    )
    
    def _validate_ir_completeness(self, ir: Dict[str, Any], result: IRVoteResult) -> None:
        """验证IR结构完整性"""
        score = 0.0
        max_score = 1.0
        
        # 必须有query_type (0.3分)
        query_type = ir.get("query_type")
        if query_type in ["aggregation", "detail", "window_detail", "duplicate_detection"]:
            score += 0.3
        else:
            result.reasons.append(f"query_type无效或缺失: {query_type}")
        
        # 必须有metrics或dimensions (0.3分)
        has_metrics = bool(ir.get("metrics"))
        has_dimensions = bool(ir.get("dimensions"))
        
        if has_metrics or has_dimensions:
            score += 0.3
        else:
            result.reasons.append("metrics和dimensions均为空")
        
        # 如果是aggregation，建议有metrics (0.1分)
        if query_type == "aggregation" and has_metrics:
            score += 0.1
        elif query_type == "aggregation" and not has_metrics:
            result.warnings.append("aggregation查询但metrics为空")
        else:
            score += 0.1  # 非aggregation不要求
        
        # 如果是detail+最值查询，检查sort_by (0.15分)
        if query_type == "detail":
            if ir.get("sort_by") and ir.get("limit"):
                score += 0.15
            elif ir.get("limit"):
                score += 0.1
                result.warnings.append("detail查询有limit但无sort_by")
            else:
                score += 0.15  # 普通明细查询
        else:
            score += 0.15
        
        # 没有严重歧义 (0.15分)
        ambiguities = ir.get("ambiguities", [])
        if not ambiguities:
            score += 0.15
        elif len(ambiguities) <= 1:
            score += 0.1
            result.warnings.append(f"存在{len(ambiguities)}个歧义")
        else:
            result.warnings.append(f"存在{len(ambiguities)}个歧义")
        
        result.ir_completeness = score / max_score
        result.ir_completeness_passed = result.ir_completeness >= self.ir_completeness_threshold
    
    def _validate_filter_coverage(self, ir: Dict[str, Any], result: IRVoteResult) -> None:
        """验证过滤条件覆盖率"""
        # 简单评估：有filters就给基础分，越多越好
        filters = ir.get("filters", [])
        
        if not filters:
            # 没有过滤条件，可能是全量查询
            result.filter_coverage = 0.5  # 给中等分
            return
        
        # 有过滤条件
        valid_filters = 0
        for flt in filters:
            if isinstance(flt, dict):
                field_id = flt.get("field")
                op = flt.get("op")
                value = flt.get("value")
                
                # 检查过滤条件的完整性
                if field_id and op:
                    # value可以是None（IS NULL操作）
                    if value is not None or op in ["IS NULL", "IS NOT NULL"]:
                        valid_filters += 1
        
        if valid_filters == 0:
            result.filter_coverage = 0.3
        elif valid_filters == 1:
            result.filter_coverage = 0.7
        elif valid_filters == 2:
            result.filter_coverage = 0.9
        else:
            result.filter_coverage = 1.0
    
    def _validate_derived_metrics(
        self,
        ir: Dict[str, Any],
        global_rules: Optional[List[Dict[str, Any]]],
        result: IRVoteResult
    ) -> None:
        """验证派生指标定义存在"""
        if not result.derived_metrics:
            return
        
        if not global_rules:
            # 无全局规则，无法验证
            result.warnings.append("无全局规则，跳过派生指标验证")
            return
        
        # 提取所有派生指标名称
        defined_metrics: Set[str] = set()
        for rule in global_rules:
            if rule.get("rule_type") == "derived_metric":
                rule_def = rule.get("rule_definition", {})
                display_name = rule_def.get("display_name", rule.get("rule_name", ""))
                if display_name:
                    defined_metrics.add(display_name)
        
        # 检查IR中使用的派生指标是否已定义
        for dm in result.derived_metrics:
            metric_name = dm.replace("derived:", "")
            if metric_name not in defined_metrics:
                result.missing_derived.append(metric_name)
        
        if result.missing_derived:
            msg = f"以下派生指标未找到定义: {result.missing_derived}"
            if self.derived_missing_handling == "fail":
                result.reasons.append(msg)
            else:
                result.warnings.append(msg)
    
    def _calculate_final_score(self, result: IRVoteResult) -> None:
        """计算综合评分"""
        score = 0.0
        
        score += result.field_existence_rate * self.weight_field_existence
        score += result.table_vote_rate * self.weight_table_vote
        score += result.ir_completeness * self.weight_ir_completeness
        score += result.filter_coverage * self.weight_filter_coverage
        
        result.score = min(1.0, max(0.0, score))
    
    def _evaluate_pass(self, result: IRVoteResult) -> bool:
        """评估是否通过验证"""
        # 必须满足的条件
        if result.missing_derived and self.derived_missing_handling == "fail":
            result.reasons.append("存在未定义的派生指标（配置为fail）")
            return False
        
        # 综合分达标
        if result.score >= self.pass_threshold:
            # 还需要检查各维度是否达标（至少字段存在率和表投票率要达标）
            if result.field_existence_passed and result.table_vote_passed:
                return True
            elif result.field_existence_passed:
                # 表投票率不达标，但字段存在率达标，综合分也达标，可以通过
                result.warnings.append("表投票率未达标但综合分达标，允许通过")
                return True
            else:
                result.reasons.append(f"字段存在率不达标: {result.field_existence_rate:.2%} < {self.field_existence_threshold:.2%}")
                return False
        else:
            result.reasons.append(f"综合分不达标: {result.score:.4f} < {self.pass_threshold}")
            return False


# 便捷函数
def validate_ir_by_voting(
    ir: Dict[str, Any],
    selected_table_id: str,
    tables_with_fields: List["TableWithFields"],
    global_rules: Optional[List[Dict[str, Any]]] = None,
) -> IRVoteResult:
    """
    便捷函数：验证IR质量
    
    Args:
        ir: LLM3生成的IR字典
        selected_table_id: 选中的表ID
        tables_with_fields: 候选表列表（含完整字段信息）
        global_rules: 全局规则（用于验证派生指标）
    
    Returns:
        IRVoteResult 验证结果
    """
    validator = IRVoteValidator()
    return validator.validate(ir, selected_table_id, tables_with_fields, global_rules)


def is_ir_vote_validation_enabled() -> bool:
    """检查IR投票验证是否启用"""
    return get_retrieval_param(
        "vector_table_selection.ir_vote_validation.enabled",
        True
    )
