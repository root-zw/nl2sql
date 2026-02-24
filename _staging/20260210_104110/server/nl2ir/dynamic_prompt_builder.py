"""
动态Prompt构建器

基于置信度等级动态调整Prompt内容：
- 高置信度：精简模式，仅提供匹配的字段
- 中等置信度：标准模式，提供完整表结构
- 低置信度：澄清模式，提示可能需要用户澄清
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import structlog

from server.config import get_retrieval_param

if TYPE_CHECKING:
    from server.nl2ir.parallel_retriever import RetrievalBundle
    from server.nl2ir.confidence_calculator import ConfidenceResult

logger = structlog.get_logger()


@dataclass
class PromptConfig:
    """Prompt配置"""
    max_tables: int = 3
    fields_mode: str = "all"  # all, important_only
    max_enums: int = 10
    max_enum_per_field: int = 5
    include_foreign_keys: bool = True
    include_few_shot: bool = True
    max_few_shot: int = 3
    require_clarification: bool = False
    
    @classmethod
    def for_high_confidence(cls) -> "PromptConfig":
        """高置信度配置"""
        return cls(
            max_tables=2,
            fields_mode="important_only",
            max_enums=5,
            max_enum_per_field=3,
            include_foreign_keys=True,
            include_few_shot=True,
            max_few_shot=2,
            require_clarification=False,
        )
    
    @classmethod
    def for_medium_confidence(cls) -> "PromptConfig":
        """中等置信度配置"""
        return cls(
            max_tables=3,
            fields_mode="all",
            max_enums=10,
            max_enum_per_field=5,
            include_foreign_keys=True,
            include_few_shot=True,
            max_few_shot=3,
            require_clarification=False,
        )
    
    @classmethod
    def for_low_confidence(cls) -> "PromptConfig":
        """低置信度配置"""
        return cls(
            max_tables=3,
            fields_mode="all",
            max_enums=0,  # 不提供枚举避免误导
            max_enum_per_field=0,
            include_foreign_keys=False,
            include_few_shot=False,
            max_few_shot=0,
            require_clarification=True,
        )


@dataclass
class DynamicPromptResult:
    """动态Prompt构建结果"""
    # Prompt模式
    mode: str = "standard"  # precise, standard, clarify
    
    # Prompt配置
    config: PromptConfig = field(default_factory=PromptConfig)
    
    # 构建的Context各部分
    tables_context: str = ""
    fields_context: str = ""
    enums_context: str = ""
    few_shot_context: str = ""
    clarification_hint: str = ""
    
    # 元信息
    table_count: int = 0
    field_count: int = 0
    enum_count: int = 0
    few_shot_count: int = 0
    
    def get_full_context(self) -> str:
        """获取完整上下文"""
        parts = []
        
        if self.tables_context:
            parts.append("【数据表结构】")
            parts.append(self.tables_context)
        
        if self.enums_context:
            parts.append("\n【枚举值参考】")
            parts.append(self.enums_context)
        
        if self.few_shot_context:
            parts.append("\n【参考示例】")
            parts.append(self.few_shot_context)
        
        if self.clarification_hint:
            parts.append("\n【注意】")
            parts.append(self.clarification_hint)
        
        return "\n".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "table_count": self.table_count,
            "field_count": self.field_count,
            "enum_count": self.enum_count,
            "few_shot_count": self.few_shot_count,
            "has_clarification": bool(self.clarification_hint),
        }


class DynamicPromptBuilder:
    """
    动态Prompt构建器
    
    根据置信度动态调整Prompt内容和详细程度。
    """
    
    def __init__(self, db_pool: Optional[Any] = None):
        self.db_pool = db_pool
    
    def build(
        self,
        question: str,
        bundle: "RetrievalBundle",
        confidence: "ConfidenceResult",
        table_schemas: Optional[List[Dict[str, Any]]] = None,
    ) -> DynamicPromptResult:
        """
        构建动态Prompt
        
        Args:
            question: 用户问题
            bundle: 检索结果束
            confidence: 置信度计算结果
            table_schemas: 表结构信息（可选，如果不提供则使用bundle中的信息）
        
        Returns:
            DynamicPromptResult
        """
        # 根据置信度选择配置
        if confidence.level == "high":
            config = PromptConfig.for_high_confidence()
            mode = "precise"
        elif confidence.level == "medium":
            config = PromptConfig.for_medium_confidence()
            mode = "standard"
        else:
            config = PromptConfig.for_low_confidence()
            mode = "clarify"
        
        result = DynamicPromptResult(mode=mode, config=config)
        
        # 构建各部分
        result.tables_context, result.table_count, result.field_count = self._build_tables_context(
            bundle=bundle,
            config=config,
            table_schemas=table_schemas,
        )
        
        if config.max_enums > 0:
            result.enums_context, result.enum_count = self._build_enums_context(
                bundle=bundle,
                config=config,
            )
        
        if config.include_few_shot and bundle.few_shot_samples:
            result.few_shot_context, result.few_shot_count = self._build_few_shot_context(
                bundle=bundle,
                config=config,
            )
        
        if config.require_clarification:
            result.clarification_hint = self._build_clarification_hint(
                question=question,
                bundle=bundle,
                confidence=confidence,
            )
        
        logger.debug(
            "动态Prompt构建完成",
            mode=mode,
            confidence_level=confidence.level,
            table_count=result.table_count,
            field_count=result.field_count,
        )
        
        return result
    
    def _build_tables_context(
        self,
        bundle: "RetrievalBundle",
        config: PromptConfig,
        table_schemas: Optional[List[Dict[str, Any]]] = None,
    ) -> tuple:
        """
        构建表结构上下文
        
        Returns:
            (context_str, table_count, field_count)
        """
        lines = []
        total_tables = 0
        total_fields = 0
        
        # 获取表候选
        tables = bundle.table_candidates[:config.max_tables]
        
        for table in tables:
            # 提取表信息
            table_name = ""
            display_name = ""
            fields = []
            json_meta = {}
            
            if hasattr(table, "table_name"):
                table_name = table.table_name
                display_name = getattr(table, "display_name", table_name)
                json_meta = getattr(table, "json_meta", {}) or {}
            elif isinstance(table, dict):
                table_name = table.get("table_name", "")
                display_name = table.get("display_name", table_name)
                json_meta = table.get("json_meta", {}) or {}
                # 从payload中获取
                payload = table.get("payload", {})
                if not json_meta and payload:
                    json_meta = payload.get("json_meta", {}) or {}
            
            if not table_name:
                continue
            
            total_tables += 1
            
            # 表头
            lines.append(f"\n表名: {display_name} ({table_name})")
            
            # 表描述
            description = json_meta.get("description", "")
            if description:
                lines.append(f"说明: {description}")
            
            # 主键
            primary_keys = json_meta.get("primary_keys", [])
            if primary_keys:
                lines.append(f"主键: {', '.join(primary_keys)}")
            
            # 外键（仅在启用时显示）
            if config.include_foreign_keys:
                foreign_keys = json_meta.get("foreign_keys", [])
                if foreign_keys:
                    fk_lines = []
                    for fk in foreign_keys[:3]:  # 最多显示3个外键
                        if isinstance(fk, dict):
                            fk_lines.append(
                                f"{fk.get('field', '')} -> {fk.get('ref_table', '')}.{fk.get('ref_field', '')}"
                            )
                    if fk_lines:
                        lines.append(f"关联: {', '.join(fk_lines)}")
            
            # 字段列表
            if config.fields_mode == "important_only":
                # 仅显示重要字段
                important_fields = json_meta.get("important_fields", [])
                if important_fields:
                    lines.append(f"核心字段: {', '.join(important_fields[:8])}")
                    total_fields += len(important_fields[:8])
            else:
                # 显示所有字段（从table_schemas获取或使用json_meta中的信息）
                stats = json_meta.get("stats", {})
                field_count = stats.get("field_count", 0)
                if field_count:
                    lines.append(f"字段数: {field_count}")
                    total_fields += field_count
                
                # 如果有重要字段，也显示
                important_fields = json_meta.get("important_fields", [])
                if important_fields:
                    lines.append(f"核心字段: {', '.join(important_fields[:8])}")
        
        return "\n".join(lines), total_tables, total_fields
    
    def _build_enums_context(
        self,
        bundle: "RetrievalBundle",
        config: PromptConfig,
    ) -> tuple:
        """
        构建枚举值上下文
        
        Returns:
            (context_str, enum_count)
        """
        if not bundle.enum_matches:
            return "", 0
        
        lines = []
        total_enums = 0
        
        # 按字段分组
        field_enums: Dict[str, List[Dict]] = {}
        for enum in bundle.enum_matches[:config.max_enums]:
            field_name = enum.get("field_name", "未知字段")
            if field_name not in field_enums:
                field_enums[field_name] = []
            if len(field_enums[field_name]) < config.max_enum_per_field:
                field_enums[field_name].append(enum)
                total_enums += 1
        
        # 生成文本
        for field_name, enums in field_enums.items():
            values = [e.get("value", "") for e in enums if e.get("value")]
            if values:
                lines.append(f"- {field_name}: {', '.join(values)}")
        
        return "\n".join(lines), total_enums
    
    def _build_few_shot_context(
        self,
        bundle: "RetrievalBundle",
        config: PromptConfig,
    ) -> tuple:
        """
        构建Few-Shot上下文
        
        Returns:
            (context_str, few_shot_count)
        """
        if not bundle.few_shot_samples:
            return "", 0
        
        lines = []
        count = 0
        
        for sample in bundle.few_shot_samples[:config.max_few_shot]:
            question = sample.get("question", "")
            ir_json = sample.get("ir_json", "")
            
            if question and ir_json:
                lines.append(f"\n问题: {question}")
                
                # 尝试格式化IR
                try:
                    if isinstance(ir_json, str):
                        ir_data = json.loads(ir_json)
                    else:
                        ir_data = ir_json
                    
                    # 简化显示
                    if "tables" in ir_data:
                        tables = ir_data.get("tables", [])
                        if tables:
                            table_names = [
                                t.get("display_name", t.get("table_name", ""))
                                for t in tables if isinstance(t, dict)
                            ]
                            lines.append(f"涉及表: {', '.join(table_names)}")
                    
                    if "measures" in ir_data:
                        measures = ir_data.get("measures", [])
                        if measures:
                            measure_names = [
                                m.get("field", str(m)) if isinstance(m, dict) else str(m)
                                for m in measures
                            ]
                            lines.append(f"度量: {', '.join(measure_names)}")
                    
                    if "dimensions" in ir_data:
                        dims = ir_data.get("dimensions", [])
                        if dims:
                            dim_names = [
                                d.get("field", str(d)) if isinstance(d, dict) else str(d)
                                for d in dims
                            ]
                            lines.append(f"维度: {', '.join(dim_names)}")
                    
                except (json.JSONDecodeError, TypeError):
                    lines.append(f"IR: {str(ir_json)[:200]}...")
                
                count += 1
        
        return "\n".join(lines), count
    
    def _build_clarification_hint(
        self,
        question: str,
        bundle: "RetrievalBundle",
        confidence: "ConfidenceResult",
    ) -> str:
        """
        构建澄清提示
        
        Args:
            question: 用户问题
            bundle: 检索结果束
            confidence: 置信度结果
        
        Returns:
            澄清提示文本
        """
        hints = []
        
        # 分析置信度因素
        factors = confidence.factors
        
        if factors.A1_domain_dense < 0.3:
            hints.append("业务域可能未正确识别")
        
        if factors.A2_table_score < 0.3:
            hints.append("未找到高度匹配的数据表")
        
        if factors.A3_enum_exact_count == 0:
            hints.append("问题中的条件值未在数据中找到精确匹配")
        
        if not hints:
            hints.append("整体匹配置信度较低")
        
        return (
            f"当前问题匹配置信度较低（{hints[0]}），"
            "建议：1) 确认查询的业务领域；2) 使用更具体的表名或字段名；3) 确认筛选条件的准确值。"
        )


def build_dynamic_prompt(
    question: str,
    bundle: "RetrievalBundle",
    confidence: "ConfidenceResult",
    db_pool: Optional[Any] = None,
) -> DynamicPromptResult:
    """
    便捷函数：构建动态Prompt
    
    Args:
        question: 用户问题
        bundle: 检索结果束
        confidence: 置信度结果
        db_pool: 数据库连接池（可选）
    
    Returns:
        DynamicPromptResult
    """
    builder = DynamicPromptBuilder(db_pool=db_pool)
    return builder.build(question, bundle, confidence)

