"""跨域查询验证器"""

from typing import List, Set
import structlog
from server.models.ir import IntermediateRepresentation
from server.models.semantic import SemanticModel

logger = structlog.get_logger()


class CrossDomainValidator:
    """跨域查询验证器

    检测查询是否涉及多个业务域，并给出建议
    """

    def __init__(self, semantic_model: SemanticModel):
        self.model = semantic_model

    def validate(self, ir: IntermediateRepresentation) -> List[str]:
        """验证IR是否涉及多个业务域

        Args:
            ir: 中间表示

        Returns:
            警告列表（如果为空，表示没有跨域问题）
        """
        warnings = []

        # 收集所有涉及的业务域
        involved_domains = self._collect_domains(ir)

        # 如果涉及多个业务域，发出警告
        if len(involved_domains) > 1:
            domain_names = self._get_domain_names(involved_domains)

            warnings.append(
                f" 该查询涉及多个业务域：{', '.join(domain_names)}"
            )
            warnings.append(
                " 建议：为了获得更精确的结果，可以将问题拆分为多个子问题，分别针对每个业务域进行查询"
            )

            logger.info(
                "检测到跨域查询",
                domains=list(involved_domains),
                count=len(involved_domains)
            )

        return warnings

    def _collect_domains(self, ir: IntermediateRepresentation) -> Set[str]:
        """收集IR中涉及的所有业务域"""
        domains = set()

        # 从指标收集
        for metric_item in ir.metrics:
            # 兼容字符串和 MetricSpec 格式
            if isinstance(metric_item, str):
                metric_id = metric_item
            elif isinstance(metric_item, dict):
                metric_id = metric_item.get("field", str(metric_item))
            elif hasattr(metric_item, "field"):
                metric_id = metric_item.field
            else:
                metric_id = str(metric_item)
            
            if metric_id in self.model.metrics:
                metric = self.model.metrics[metric_id]
                if hasattr(metric, 'domain_id') and metric.domain_id:
                    domains.add(metric.domain_id)

        # 从维度收集
        for dim_id in ir.dimensions:
            if dim_id in self.model.dimensions:
                dim = self.model.dimensions[dim_id]
                if hasattr(dim, 'domain_id') and dim.domain_id:
                    domains.add(dim.domain_id)

        # 从度量收集（明细查询场景）
        if ir.sort_by and ir.sort_by in self.model.measures:
            measure = self.model.measures[ir.sort_by]
            if hasattr(measure, 'domain_id') and measure.domain_id:
                domains.add(measure.domain_id)

        # 从过滤条件收集
        for filter_cond in ir.filters:
            if filter_cond.field in self.model.dimensions:
                dim = self.model.dimensions[filter_cond.field]
                if hasattr(dim, 'domain_id') and dim.domain_id:
                    domains.add(dim.domain_id)

        return domains

    def _get_domain_names(self, domain_ids: Set[str]) -> List[str]:
        """从domain_id获取域名称（用于显示）

        注意：这里只是示例，实际应该从业务域表查询
        """
        # 简单返回domain_id作为名称
        # 在实际使用中，应该通过DomainDetector获取域名
        return list(domain_ids)

