"""
LLM 表选择器
基于LLM的智能表选择，替代向量检索

功能：
1. 从文件加载提示词模板
2. 构建包含丰富元数据的 Prompt（字段同义词、表关系、字段描述）
3. 调用 LLM 进行表选择
4. 置信度判断和用户确认策略
5. 支持 Trace 追踪
"""

from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
import structlog

from server.config import settings
from server.models.api import TableCandidate, TableSelectionCard
from server.nl2ir.llm_client import LLMClient
from server.nl2ir.table_structure_loader import TableStructureLoader, TableStructure
from server.utils.prompt_loader import resolve_path, load_json, load_text

logger = structlog.get_logger()

# 提示词文件路径（可配置）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_PROMPTS_DIR = _PROJECT_ROOT / "prompts" / "table_selector"
PROMPTS_DIR = resolve_path(settings.table_selector_prompts_dir, _DEFAULT_PROMPTS_DIR)
SYSTEM_PROMPT_FILE = resolve_path(settings.table_selector_system_prompt_file, PROMPTS_DIR / "system.txt")
FUNCTION_SCHEMA_FILE = resolve_path(
    settings.table_selector_function_schema_file, PROMPTS_DIR / "function_schema.json"
)
USER_TEMPLATE_FILE = resolve_path(settings.table_selector_user_template_file, PROMPTS_DIR / "user_template.txt")


# ============================================================
# 提示词加载
# ============================================================
def _load_system_prompt() -> str:
    """从文件加载系统提示词"""
    return load_text(SYSTEM_PROMPT_FILE, default=_DEFAULT_SYSTEM_PROMPT, prompt_name="table_selector_system")


def _load_function_schema() -> Dict[str, Any]:
    """从文件加载 Function Schema"""
    return load_json(
        FUNCTION_SCHEMA_FILE, default=_DEFAULT_FUNCTION_SCHEMA, prompt_name="table_selector_function_schema"
    )


def _load_user_template() -> str:
    """从文件加载用户提示词模板"""
    return load_text(USER_TEMPLATE_FILE, default=_DEFAULT_USER_TEMPLATE, prompt_name="table_selector_user_template")


# ============================================================
# 默认提示词（文件不存在时使用）
# ============================================================
_DEFAULT_SYSTEM_PROMPT = """你是一个专业的数据表选择助手。根据用户问题，从候选表中选择最相关的表。
请严格调用 select_tables 函数返回结果。"""

_DEFAULT_FUNCTION_SCHEMA = {
    "type": "function",
    "function": {
        "name": "select_tables",
        "description": "从候选表中选择与用户问题最相关的数据表",
        "parameters": {
            "type": "object",
            "properties": {
                "candidates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "table_id": {"type": "string"},
                            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                            "reason": {"type": "string"}
                        },
                        "required": ["table_id", "confidence", "reason"]
                    }
                },
                "primary_table": {"type": "string"},
                "selection_summary": {"type": "string"}
            },
            "required": ["candidates", "primary_table", "selection_summary"]
        }
    }
}

_DEFAULT_USER_TEMPLATE = """## 当前时间

{current_time}

---

## 用户问题
「{question}」

## 候选数据表
{tables_section}

请分析用户问题，选择最相关的数据表。注意：只输出置信度>=0.3的候选表（最多5个），reason要简洁（不超过50字）。"""


# ============================================================
# 配置常量（从 settings 读取）
# ============================================================
def _get_high_confidence() -> float:
    return settings.llm_table_selection_high_confidence


def _get_medium_confidence() -> float:
    return settings.llm_table_selection_medium_confidence


def _get_min_gap() -> float:
    return settings.llm_table_selection_min_gap


def _get_max_candidates() -> int:
    return settings.llm_table_selection_max_candidates


def _get_cross_year_confidence() -> float:
    return settings.llm_table_selection_cross_year_confidence


# ============================================================
# 数据结构
# ============================================================
@dataclass
class FieldInfo:
    """字段信息（用于 LLM 输入）"""
    display_name: str
    description: Optional[str] = None
    synonyms: List[str] = field(default_factory=list)
    field_type: str = "dimension"  # dimension, measure, identifier


@dataclass
class TableRelation:
    """表关系信息"""
    target_table_name: str
    relationship_type: str  # one_to_one, one_to_many, many_to_many
    relationship_name: Optional[str] = None


@dataclass
class TableMeta:
    """表的元数据（用于LLM输入）"""
    table_id: str
    display_name: str
    description: str
    domain_name: Optional[str]
    tags: List[str]
    
    # 所属连接
    connection_id: Optional[str] = None
    
    # 业务域ID
    domain_id: Optional[str] = None
    
    # 数据年份（用于跨年查询识别）
    data_year: Optional[str] = None
    
    # 字段信息（包含同义词和描述）
    dimensions: List[FieldInfo] = field(default_factory=list)
    measures: List[FieldInfo] = field(default_factory=list)
    identifiers: List[FieldInfo] = field(default_factory=list)
    
    # 表关系
    relations: List[TableRelation] = field(default_factory=list)
    
    # 统计信息
    field_count: int = 0


@dataclass
class TableSelectionResult:
    """表选择结果"""
    candidates: List[TableCandidate]
    primary_table_id: Optional[str]
    selection_summary: str
    needs_confirmation: bool
    action: str  # "execute" | "confirm" | "clarify"
    
    # 多表查询判断（由 LLM 返回）
    is_multi_table_query: bool = False  # 是否需要多表查询
    multi_table_mode: str = "single"  # "single" | "compare" | "union" | "multi_join"
    multi_table_hint: Optional[str] = None  # 给用户的多表提示信息
    recommended_table_ids: List[str] = field(default_factory=list)  # LLM 推荐选择的表ID列表

    # Trace 信息
    system_prompt: Optional[str] = None
    user_prompt: Optional[str] = None
    llm_response: Optional[Dict[str, Any]] = None


class TableSelectionStrategy:
    """表选择策略：判断是直接执行还是需要用户确认"""

    def __init__(
        self,
        high_threshold: Optional[float] = None,
        medium_threshold: Optional[float] = None,
        min_gap: Optional[float] = None,
        cross_year_threshold: Optional[float] = None
    ):
        self.high_threshold = high_threshold if high_threshold is not None else _get_high_confidence()
        self.medium_threshold = medium_threshold if medium_threshold is not None else _get_medium_confidence()
        self.min_gap = min_gap if min_gap is not None else _get_min_gap()
        self.cross_year_threshold = cross_year_threshold if cross_year_threshold is not None else _get_cross_year_confidence()

    def evaluate(
        self,
        candidates: List[TableCandidate],
        question: str,
        is_multi_table_query: bool = False,
        multi_table_mode: str = "single",
        recommended_table_ids: Optional[List[str]] = None
    ) -> Tuple[str, Optional[TableSelectionCard]]:
        """
        评估选择结果，决定是直接执行还是需要确认

        Args:
            candidates: 候选表列表
            question: 用户问题
            is_multi_table_query: 是否多表查询
            multi_table_mode: 多表查询模式 (single/cross_year/cross_partition)
            recommended_table_ids: LLM 推荐的表ID列表

        Returns:
            (action, card)
            action: "execute" | "confirm" | "clarify"
            card: 确认卡（如果需要）
        """
        if not candidates:
            return "clarify", None

        top1 = candidates[0]
        top2 = candidates[1] if len(candidates) > 1 else None

        # 场景0: 多表查询 - 检查推荐表的置信度
        # 支持的模式（新旧兼容）：compare, union, multi_join, 以及旧版的 cross_year_compare, cross_year_union 等
        multi_table_modes = ("compare", "union", "multi_join", "cross_year_compare", "cross_year_union", "cross_year", "cross_partition")
        if is_multi_table_query and multi_table_mode in multi_table_modes and recommended_table_ids:
            # 获取推荐表的置信度
            recommended_confidences = []
            candidate_map = {c.table_id: c for c in candidates}
            for table_id in recommended_table_ids:
                if table_id in candidate_map:
                    recommended_confidences.append(candidate_map[table_id].confidence)

            # 如果所有推荐表的置信度都 >= 跨年阈值，直接执行
            if recommended_confidences and all(conf >= self.cross_year_threshold for conf in recommended_confidences):
                logger.debug(
                    "表选择：跨年查询高置信度直接执行",
                    multi_table_mode=multi_table_mode,
                    recommended_tables=recommended_table_ids,
                    confidences=recommended_confidences,
                    threshold=self.cross_year_threshold
                )
                return "execute", None

            # 否则需要确认，但预选推荐的表
            meaningful_candidates = [c for c in candidates if c.confidence > 0.1]
            max_candidates = _get_max_candidates()
            
            # 根据模式设置合适的确认消息
            if multi_table_mode == "multi_join":
                confirm_message = "检测到跨表关联查询，请确认需要关联的数据表："
                confirm_reason = "跨表关联查询，请确认需要查询的表"
            else:
                confirm_message = "系统找到了多个可能相关的表，请确认您要查询的是哪张表："
                confirm_reason = "跨年度查询的部分年份置信度较低，请确认"
            
            card = TableSelectionCard(
                candidates=meaningful_candidates[:max_candidates],
                question=question,
                message=confirm_message,
                confirmation_reason=confirm_reason
            )
            logger.debug(
                "表选择：多表查询需要用户确认",
                multi_table_mode=multi_table_mode,
                recommended_tables=recommended_table_ids,
                confidences=recommended_confidences
            )
            return "confirm", card

        # 场景1: 单表查询 - Top1 高置信度 + 与Top2分差足够大 → 直接执行
        if top1.confidence >= self.high_threshold:
            if top2 is None or (top1.confidence - top2.confidence) >= self.min_gap:
                logger.debug(
                    "表选择：高置信度直接执行",
                    primary_table=top1.table_name,
                    confidence=top1.confidence,
                    gap=top1.confidence - top2.confidence if top2 else 1.0
                )
                return "execute", None

        # 场景2: Top1 中等置信度 或 与Top2分差较小 → 需要确认
        if top1.confidence >= self.medium_threshold:
            # 只展示有意义的候选（置信度>0.1）
            meaningful_candidates = [c for c in candidates if c.confidence > 0.1]
            max_candidates = _get_max_candidates()
            
            # 生成确认原因
            if top2 and (top1.confidence - top2.confidence) < self.min_gap:
                confirmation_reason = f"两个表的匹配度接近（{top1.table_name} {top1.confidence:.0%} vs {top2.table_name} {top2.confidence:.0%}）"
            else:
                confirmation_reason = f"AI 置信度为 {top1.confidence:.0%}，请确认选择"
            
            card = TableSelectionCard(
                candidates=meaningful_candidates[:max_candidates],
                question=question,
                message="系统找到了多个可能相关的表，请确认您要查询的是哪张表：",
                confirmation_reason=confirmation_reason
            )
            logger.debug(
                "表选择：需要用户确认",
                primary_table=top1.table_name,
                confidence=top1.confidence,
                candidates=[c.table_name for c in meaningful_candidates[:3]]
            )
            return "confirm", card

        # 场景3: 置信度过低 → 需要用户澄清问题
        logger.debug(
            "表选择：置信度过低需澄清",
            top_confidence=top1.confidence,
            threshold=self.medium_threshold
        )
        return "clarify", None


class LLMTableSelector:
    """
    LLM 表选择器
    
    使用LLM从所有表中选择与用户问题最相关的表
    """
    
    def __init__(
        self,
        llm_client: LLMClient,
        structure_loader: TableStructureLoader
    ):
        self.llm_client = llm_client
        self.structure_loader = structure_loader
        self.strategy = TableSelectionStrategy()
        
        # 加载提示词
        self.system_prompt = _load_system_prompt()
        self.function_schema = _load_function_schema()
        self.user_template = _load_user_template()
        
        # 调试信息（用于 Trace）
        self.last_system_prompt: Optional[str] = None
        self.last_user_prompt: Optional[str] = None
        self.last_response: Optional[Dict[str, Any]] = None
        self.last_result_json: Optional[Dict[str, Any]] = None
    
    async def select_tables(
        self,
        question: str,
        all_tables_meta: List[TableMeta]
    ) -> TableSelectionResult:
        """
        使用LLM选择表
        
        Args:
            question: 用户问题
            all_tables_meta: 所有表的元数据
        
        Returns:
            TableSelectionResult
        """
        if not all_tables_meta:
            logger.warning("表选择器：无可用表")
            return TableSelectionResult(
                candidates=[],
                primary_table_id=None,
                selection_summary="无可用数据表",
                needs_confirmation=False,
                action="clarify"
            )
        
        # 1. 构建 Prompt
        from datetime import datetime
        tables_section = self._build_tables_section(all_tables_meta)
        current_time = datetime.now().strftime("%Y年%m月%d日")
        user_prompt = self.user_template.format(
            question=question,
            tables_section=tables_section,
            current_time=current_time
        )
        
        # 保存调试信息
        self.last_system_prompt = self.system_prompt
        self.last_user_prompt = user_prompt
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        # 2. 调用LLM
        try:
            response = await self.llm_client.chat_completion(
                messages=messages,
                tools=[self.function_schema],
                tool_choice={"type": "function", "function": {"name": "select_tables"}}
            )
            self.last_response = response
            
            # 3. 解析结果
            result_json = self.llm_client.extract_function_call(response)
            self.last_result_json = result_json
            
            if not result_json:
                logger.warning("LLM表选择：未返回有效结果")
                return self._fallback_result(all_tables_meta, "LLM未返回有效结果")
            
            # 4. 构建候选列表
            candidates = self._parse_candidates(result_json, all_tables_meta)
            primary_table_id = result_json.get("primary_table")
            selection_summary = result_json.get("selection_summary", "")
            
            # 解析多表查询判断（由 LLM 返回）
            is_multi_table_query = result_json.get("is_multi_table_query", False)
            multi_table_mode = result_json.get("multi_table_mode", "single")
            multi_table_hint = result_json.get("multi_table_hint")
            recommended_table_ids = result_json.get("recommended_table_ids", [])

            # 如果 LLM 没有返回 recommended_table_ids，则使用 primary_table
            if not recommended_table_ids and primary_table_id:
                recommended_table_ids = [primary_table_id]

            logger.debug(
                "LLM表选择完成",
                primary_table=primary_table_id,
                candidate_count=len(candidates),
                summary=selection_summary[:100],
                is_multi_table_query=is_multi_table_query,
                multi_table_mode=multi_table_mode,
                recommended_table_ids=recommended_table_ids
            )

            # 5. 评估是否需要确认
            action, card = self.strategy.evaluate(
                candidates=candidates,
                question=question,
                is_multi_table_query=is_multi_table_query,
                multi_table_mode=multi_table_mode,
                recommended_table_ids=recommended_table_ids
            )

            return TableSelectionResult(
                candidates=candidates,
                primary_table_id=primary_table_id,
                selection_summary=selection_summary,
                needs_confirmation=(action == "confirm"),
                action=action,
                is_multi_table_query=is_multi_table_query,
                multi_table_mode=multi_table_mode,
                multi_table_hint=multi_table_hint,
                recommended_table_ids=recommended_table_ids,
                system_prompt=self.system_prompt,
                user_prompt=user_prompt,
                llm_response=result_json
            )
            
        except Exception as e:
            logger.exception("LLM表选择失败", error=str(e))
            return self._fallback_result(all_tables_meta, str(e))
    
    def _build_tables_section(self, tables: List[TableMeta]) -> str:
        """构建候选表描述部分"""
        lines = []
        
        for table in tables:
            # 不使用序号前缀，避免LLM将序号与table_id混淆
            lines.append(f"### {table.display_name}")
            lines.append(f"- **表ID**: `{table.table_id}`  ← 返回时请精确复制此UUID")
            
            if table.description:
                lines.append(f"- **描述**: {table.description}")
            
            if table.domain_name:
                lines.append(f"- **业务域**: {table.domain_name}")
            
            # 数据年份（用于跨年查询判断，非常重要）
            if table.data_year:
                lines.append(f"- **数据年份**: {table.data_year}")
            
            if table.tags:
                lines.append(f"- **标签/同义词**: {', '.join(table.tags)}")
            
            # 维度字段（包含同义词和描述）
            if table.dimensions:
                dim_items = []
                for f in table.dimensions:
                    item = f.display_name
                    if f.synonyms:
                        item += f"（{'/'.join(f.synonyms[:2])}）"
                    dim_items.append(item)
                lines.append(f"- **维度字段**: {', '.join(dim_items)}")
            
            # 度量字段（包含同义词和描述）
            if table.measures:
                measure_items = []
                for f in table.measures:
                    item = f.display_name
                    if f.synonyms:
                        item += f"（{'/'.join(f.synonyms[:2])}）"
                    measure_items.append(item)
                lines.append(f"- **度量字段**: {', '.join(measure_items)}")
            
            # 标识字段
            if table.identifiers:
                id_items = [f.display_name for f in table.identifiers]
                lines.append(f"- **标识字段**: {', '.join(id_items)}")
            
            # 表关系
            if table.relations:
                rel_items = []
                for rel in table.relations[:3]:
                    rel_str = f"{rel.target_table_name}"
                    if rel.relationship_name:
                        rel_str = f"{rel.relationship_name}→{rel.target_table_name}"
                    rel_items.append(rel_str)
                lines.append(f"- **关联表**: {', '.join(rel_items)}")
            
            lines.append(f"- **字段总数**: {table.field_count}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _parse_candidates(
        self,
        result_json: Dict[str, Any],
        all_tables_meta: List[TableMeta]
    ) -> List[TableCandidate]:
        """解析LLM返回的候选列表"""
        # 构建表ID到元数据的映射
        table_map = {t.table_id: t for t in all_tables_meta}
        
        candidates = []
        raw_candidates = result_json.get("candidates", [])
        
        for raw in raw_candidates:
            table_id = raw.get("table_id")
            if not table_id or table_id not in table_map:
                continue
            
            meta = table_map[table_id]
            # 提取关键字段名
            key_dims = [f.display_name for f in meta.dimensions[:5]]
            key_measures = [f.display_name for f in meta.measures[:5]]
            
            candidate = TableCandidate(
                table_id=table_id,
                table_name=meta.display_name,
                description=meta.description,
                confidence=float(raw.get("confidence", 0)),
                reason=raw.get("reason", ""),
                tags=meta.tags,
                key_dimensions=key_dims,
                key_measures=key_measures,
                domain_name=meta.domain_name,
                domain_id=meta.domain_id,
                data_year=meta.data_year
            )
            candidates.append(candidate)
        
        # 按置信度排序
        candidates.sort(key=lambda c: c.confidence, reverse=True)
        
        return candidates
    
    def _fallback_result(
        self,
        all_tables_meta: List[TableMeta],
        error_msg: str
    ) -> TableSelectionResult:
        """降级处理：返回第一张表"""
        if not all_tables_meta:
            return TableSelectionResult(
                candidates=[],
                primary_table_id=None,
                selection_summary=f"选择失败: {error_msg}",
                needs_confirmation=False,
                action="clarify"
            )
        
        # 使用第一张表作为候选
        first_table = all_tables_meta[0]
        key_dims = [f.display_name for f in first_table.dimensions[:5]]
        key_measures = [f.display_name for f in first_table.measures[:5]]
        
        candidate = TableCandidate(
            table_id=first_table.table_id,
            table_name=first_table.display_name,
            description=first_table.description,
            confidence=0.5,  # 中等置信度
            reason="系统自动选择（降级模式）",
            tags=first_table.tags,
            key_dimensions=key_dims,
            key_measures=key_measures,
            domain_name=first_table.domain_name
        )
        
        return TableSelectionResult(
            candidates=[candidate],
            primary_table_id=first_table.table_id,
            selection_summary=f"降级选择第一张表（原因: {error_msg}）",
            needs_confirmation=True,  # 需要确认
            action="confirm",
            system_prompt=self.last_system_prompt,
            user_prompt=self.last_user_prompt
        )


async def load_all_tables_meta(
    connection_id: Optional[str],
    structure_loader: TableStructureLoader,
    connection_ids: Optional[List[str]] = None,
    user_id: Optional[str] = None,
    user_role: Optional[str] = None
) -> List[TableMeta]:
    """
    加载表的完整元数据（带用户权限过滤）
    
    支持两种模式：
    1. 指定 connection_id: 加载该连接下所有表
    2. 指定 connection_ids: 加载多个连接下所有表（用于跨连接 LLM 表选择）
    3. 都不指定: 加载所有活跃连接的表
    
    权限过滤：
    - 如果提供 user_id 和 user_role，会根据用户的数据角色过滤表
    - admin 用户或 scope_type='all' 的角色可以访问所有表
    - limited 角色只能访问被授权的表
    
    包含：
    - 表基本信息（名称、描述、标签）
    - 字段信息（包含同义词和描述）
    - 表关系信息
    
    Args:
        connection_id: 单个数据库连接ID（可选）
        structure_loader: 表结构加载器
        connection_ids: 多个连接ID列表（可选）
        user_id: 用户ID（可选，用于权限过滤）
        user_role: 用户角色（可选，如 'admin', 'viewer' 等）
    
    Returns:
        List[TableMeta]
    """
    from server.utils.db_pool import get_metadata_pool
    from uuid import UUID
    
    tables_meta: List[TableMeta] = []
    
    try:
        pool = await get_metadata_pool()
        async with pool.acquire() as conn:
            # 检查用户是否需要权限过滤
            needs_permission_filter = False
            user_uuid = None
            
            if user_id and user_role and user_role != 'admin':
                user_uuid = UUID(user_id)
                # 检查用户是否有 scope_type='all' 的角色
                has_all_access = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM user_data_roles udr
                        JOIN data_roles dr ON udr.role_id = dr.role_id
                        WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                        AND dr.is_active = TRUE AND dr.scope_type = 'all'
                    )
                """, user_uuid)
                
                if not has_all_access:
                    needs_permission_filter = True
            
            # 1. 查询表
            if connection_id:
                # 单连接模式
                if needs_permission_filter:
                    table_rows = await conn.fetch("""
                        SELECT 
                            t.table_id,
                            t.connection_id,
                            t.display_name,
                            t.description,
                            t.tags,
                            t.domain_id,
                            t.data_year,
                            bd.domain_name
                        FROM db_tables t
                        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                        WHERE t.connection_id = $1 
                          AND t.is_included = TRUE
                          AND t.table_id IN (
                              SELECT DISTINCT rtp.table_id
                              FROM user_data_roles udr
                              JOIN data_roles dr ON udr.role_id = dr.role_id
                              JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                              WHERE udr.user_id = $2 AND udr.is_active = TRUE 
                              AND dr.is_active = TRUE AND dr.scope_type = 'limited'
                              AND rtp.can_query = TRUE
                          )
                        ORDER BY t.display_name
                    """, UUID(connection_id), user_uuid)
                else:
                    table_rows = await conn.fetch("""
                        SELECT 
                            t.table_id,
                            t.connection_id,
                            t.display_name,
                            t.description,
                            t.tags,
                            t.domain_id,
                            t.data_year,
                            bd.domain_name
                        FROM db_tables t
                        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                        WHERE t.connection_id = $1 
                          AND t.is_included = TRUE
                        ORDER BY t.display_name
                    """, UUID(connection_id))
                
                # 查询表关系
                relation_rows = await conn.fetch("""
                    SELECT 
                        tr.left_table_id,
                        t_right.display_name as right_table_name,
                        tr.relationship_type,
                        tr.relationship_name
                    FROM table_relationships tr
                    JOIN db_tables t_right ON tr.right_table_id = t_right.table_id
                    WHERE tr.connection_id = $1 
                      AND tr.is_active = TRUE
                """, UUID(connection_id))
            elif connection_ids:
                # 多连接模式
                conn_uuids = [UUID(cid) for cid in connection_ids]
                if needs_permission_filter:
                    table_rows = await conn.fetch("""
                        SELECT 
                            t.table_id,
                            t.connection_id,
                            t.display_name,
                            t.description,
                            t.tags,
                            t.domain_id,
                            t.data_year,
                            bd.domain_name
                        FROM db_tables t
                        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                        WHERE t.connection_id = ANY($1) 
                          AND t.is_included = TRUE
                          AND t.table_id IN (
                              SELECT DISTINCT rtp.table_id
                              FROM user_data_roles udr
                              JOIN data_roles dr ON udr.role_id = dr.role_id
                              JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                              WHERE udr.user_id = $2 AND udr.is_active = TRUE 
                              AND dr.is_active = TRUE AND dr.scope_type = 'limited'
                              AND rtp.can_query = TRUE
                          )
                        ORDER BY t.display_name
                    """, conn_uuids, user_uuid)
                else:
                    table_rows = await conn.fetch("""
                        SELECT 
                            t.table_id,
                            t.connection_id,
                            t.display_name,
                            t.description,
                            t.tags,
                            t.domain_id,
                            t.data_year,
                            bd.domain_name
                        FROM db_tables t
                        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                        WHERE t.connection_id = ANY($1) 
                          AND t.is_included = TRUE
                        ORDER BY t.display_name
                    """, conn_uuids)
                
                # 查询表关系
                relation_rows = await conn.fetch("""
                    SELECT 
                        tr.left_table_id,
                        t_right.display_name as right_table_name,
                        tr.relationship_type,
                        tr.relationship_name
                    FROM table_relationships tr
                    JOIN db_tables t_right ON tr.right_table_id = t_right.table_id
                    WHERE tr.connection_id = ANY($1) 
                      AND tr.is_active = TRUE
                """, conn_uuids)
            else:
                # 所有活跃连接
                if needs_permission_filter:
                    table_rows = await conn.fetch("""
                        SELECT 
                            t.table_id,
                            t.connection_id,
                            t.display_name,
                            t.description,
                            t.tags,
                            t.domain_id,
                            t.data_year,
                            bd.domain_name
                        FROM db_tables t
                        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                        JOIN database_connections dc ON t.connection_id = dc.connection_id
                        WHERE t.is_included = TRUE
                          AND dc.is_active = TRUE
                          AND t.table_id IN (
                              SELECT DISTINCT rtp.table_id
                              FROM user_data_roles udr
                              JOIN data_roles dr ON udr.role_id = dr.role_id
                              JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                              WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                              AND dr.is_active = TRUE AND dr.scope_type = 'limited'
                              AND rtp.can_query = TRUE
                          )
                        ORDER BY t.display_name
                    """, user_uuid)
                else:
                    table_rows = await conn.fetch("""
                        SELECT 
                            t.table_id,
                            t.connection_id,
                            t.display_name,
                            t.description,
                            t.tags,
                            t.domain_id,
                            t.data_year,
                            bd.domain_name
                        FROM db_tables t
                        LEFT JOIN business_domains bd ON t.domain_id = bd.domain_id
                        JOIN database_connections dc ON t.connection_id = dc.connection_id
                        WHERE t.is_included = TRUE
                          AND dc.is_active = TRUE
                        ORDER BY t.display_name
                    """)
                
                # 查询所有表关系
                relation_rows = await conn.fetch("""
                    SELECT 
                        tr.left_table_id,
                        t_right.display_name as right_table_name,
                        tr.relationship_type,
                        tr.relationship_name
                    FROM table_relationships tr
                    JOIN db_tables t_right ON tr.right_table_id = t_right.table_id
                    WHERE tr.is_active = TRUE
                """)
            
            # 构建表关系映射
            relations_map: Dict[str, List[TableRelation]] = {}
            for rel_row in relation_rows:
                left_id = str(rel_row["left_table_id"])
                if left_id not in relations_map:
                    relations_map[left_id] = []
                relations_map[left_id].append(TableRelation(
                    target_table_name=rel_row["right_table_name"] or "",
                    relationship_type=rel_row["relationship_type"] or "one_to_many",
                    relationship_name=rel_row["relationship_name"]
                ))
            
            # 3. 直接从数据库加载字段信息（不依赖 semantic_model）
            # fields 表通过 source_column_id 关联到 db_columns 表，db_columns 有 table_id
            table_ids = [row["table_id"] for row in table_rows]
            
            # 批量查询所有表的字段
            field_rows = await conn.fetch("""
                SELECT 
                    dc.table_id,
                    f.field_id,
                    f.display_name,
                    f.description,
                    f.field_type,
                    f.synonyms
                FROM fields f
                JOIN db_columns dc ON f.source_column_id = dc.column_id
                WHERE dc.table_id = ANY($1::uuid[])
                  AND f.is_active = TRUE
                ORDER BY dc.table_id, f.priority NULLS LAST, f.display_name
            """, table_ids)
            
            # 构建字段映射 {table_id: {"dimensions": [], "measures": [], "identifiers": []}}
            fields_map: Dict[str, Dict[str, List[FieldInfo]]] = {}
            for field_row in field_rows:
                tid = str(field_row["table_id"])
                if tid not in fields_map:
                    fields_map[tid] = {"dimensions": [], "measures": [], "identifiers": []}
                
                # field_type 可能是 "dimension" 或 "measure"
                field_type = field_row["field_type"] or "dimension"
                synonyms = field_row["synonyms"] or []
                
                field_info = FieldInfo(
                    display_name=field_row["display_name"] or "",
                    description=field_row["description"] or "",
                    synonyms=synonyms if isinstance(synonyms, list) else [],
                    field_type=field_type
                )
                
                if field_type == "measure":
                    fields_map[tid]["measures"].append(field_info)
                elif field_type == "identifier":
                    fields_map[tid]["identifiers"].append(field_info)
                else:
                    fields_map[tid]["dimensions"].append(field_info)
            
            # 4. 构建表元数据
            for row in table_rows:
                table_id = str(row["table_id"])
                table_fields = fields_map.get(table_id, {"dimensions": [], "measures": [], "identifiers": []})
                
                dimensions = table_fields["dimensions"]
                measures = table_fields["measures"]
                identifiers = table_fields["identifiers"]
                field_count = len(dimensions) + len(measures) + len(identifiers)
                
                tables_meta.append(TableMeta(
                    table_id=table_id,
                    display_name=row["display_name"] or "",
                    description=row["description"] or "",
                    domain_name=row["domain_name"],
                    tags=row["tags"] or [],
                    connection_id=str(row["connection_id"]) if row.get("connection_id") else None,
                    domain_id=str(row["domain_id"]) if row.get("domain_id") else None,
                    data_year=row["data_year"] if row.get("data_year") else None,
                    dimensions=dimensions,
                    measures=measures,
                    identifiers=identifiers,
                    relations=relations_map.get(table_id, []),
                    field_count=field_count
                ))
        
        logger.debug(
            "加载表元数据完成",
            connection_id=connection_id,
            table_count=len(tables_meta),
            total_fields=sum(t.field_count for t in tables_meta)
        )
        
    except Exception as e:
        logger.exception("加载表元数据失败", error=str(e))
    
    return tables_meta
