"""提示词模板管理服务

提供提示词模板的CRUD操作，支持：
- 从数据库加载提示词
- 版本管理和历史记录
- 文件与数据库的同步
"""

from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
import json
import structlog

from server.utils.db_pool import get_metadata_pool

logger = structlog.get_logger()

# 项目根目录
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROMPTS_DIR = _PROJECT_ROOT / "prompts"


class PromptScenario(str, Enum):
    """提示词场景"""
    TABLE_SELECTOR = "table_selector"
    NL2IR = "nl2ir"
    DIRECT_SQL = "direct_sql"
    NARRATIVE = "narrative"
    COT_PLANNER = "cot_planner"
    VECTOR_TABLE_SELECTOR = "vector_table_selector"


class PromptType(str, Enum):
    """提示词类型"""
    SYSTEM = "system"
    USER_TEMPLATE = "user_template"
    FUNCTION_SCHEMA = "function_schema"


# 场景显示名称
SCENARIO_LABELS = {
    PromptScenario.TABLE_SELECTOR: "LLM表选择器",
    PromptScenario.NL2IR: "NL2IR解析",
    PromptScenario.DIRECT_SQL: "直接SQL生成",
    PromptScenario.NARRATIVE: "叙述生成",
    PromptScenario.COT_PLANNER: "CoT规划器",
    PromptScenario.VECTOR_TABLE_SELECTOR: "向量表选择器",
}

# 类型显示名称
TYPE_LABELS = {
    PromptType.SYSTEM: "系统提示词",
    PromptType.USER_TEMPLATE: "用户提示模板",
    PromptType.FUNCTION_SCHEMA: "函数Schema",
}

# 场景提示词文件映射
SCENARIO_FILES = {
    PromptScenario.TABLE_SELECTOR: {
        PromptType.SYSTEM: "table_selector/system.txt",
        PromptType.USER_TEMPLATE: "table_selector/user_template.txt",
        PromptType.FUNCTION_SCHEMA: "table_selector/function_schema.json",
    },
    PromptScenario.NL2IR: {
        PromptType.SYSTEM: "nl2ir/system.txt",
        PromptType.FUNCTION_SCHEMA: "nl2ir/function_schema.json",
    },
    PromptScenario.DIRECT_SQL: {
        PromptType.SYSTEM: "direct_sql/system.txt",
        PromptType.USER_TEMPLATE: "direct_sql/user_template.txt",
    },
    PromptScenario.NARRATIVE: {
        PromptType.SYSTEM: "narrative/prompt.txt",  # narrative 只有一个提示词文件
    },
    PromptScenario.COT_PLANNER: {
        PromptType.SYSTEM: "cot_planner/prompt.txt",
    },
    PromptScenario.VECTOR_TABLE_SELECTOR: {
        PromptType.SYSTEM: "vector_table_selector/system.txt",
        PromptType.USER_TEMPLATE: "vector_table_selector/user_template.txt",
        PromptType.FUNCTION_SCHEMA: "vector_table_selector/function_schema.json",
    },
}


@dataclass
class PromptTemplate:
    """提示词模板数据类"""
    template_id: Optional[UUID] = None
    scenario: str = ""
    prompt_type: str = ""
    display_name: str = ""
    description: Optional[str] = None
    content: str = ""
    version: int = 1
    is_active: bool = True
    created_by: Optional[UUID] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    # 额外信息
    file_path: Optional[str] = None  # 对应的文件路径
    has_file: bool = False  # 是否有对应的文件
    file_content: Optional[str] = None  # 文件内容（用于比较）


@dataclass
class PromptScenarioInfo:
    """场景信息"""
    scenario: str
    label: str
    description: str
    prompts: List[Dict[str, Any]] = field(default_factory=list)


class PromptService:
    """提示词管理服务"""
    
    def __init__(self):
        self._cache: Dict[Tuple[str, str], PromptTemplate] = {}
    
    async def list_scenarios(self) -> List[PromptScenarioInfo]:
        """获取所有场景及其提示词列表"""
        scenarios = []
        
        for scenario in PromptScenario:
            prompts = []
            scenario_files = SCENARIO_FILES.get(scenario, {})
            
            for prompt_type in PromptType:
                if prompt_type not in scenario_files:
                    continue
                    
                file_path = scenario_files[prompt_type]
                full_path = PROMPTS_DIR / file_path
                
                # 从数据库查询
                db_template = await self._get_from_db(scenario.value, prompt_type.value)
                
                # 检查文件
                file_exists = full_path.exists()
                
                prompt_info = {
                    "scenario": scenario.value,
                    "prompt_type": prompt_type.value,
                    "type_label": TYPE_LABELS.get(prompt_type, prompt_type.value),
                    "file_path": file_path,
                    "has_file": file_exists,
                    "has_db_version": db_template is not None,
                    "is_active": db_template.is_active if db_template else False,
                    "version": db_template.version if db_template else 0,
                    "template_id": str(db_template.template_id) if db_template else None,
                }
                prompts.append(prompt_info)
            
            scenarios.append(PromptScenarioInfo(
                scenario=scenario.value,
                label=SCENARIO_LABELS.get(scenario, scenario.value),
                description=self._get_scenario_description(scenario),
                prompts=prompts
            ))
        
        return scenarios
    
    def _get_scenario_description(self, scenario: PromptScenario) -> str:
        """获取场景描述"""
        descriptions = {
            PromptScenario.TABLE_SELECTOR: "基于LLM从候选表中智能选择最相关的数据表",
            PromptScenario.NL2IR: "将自然语言问题转换为中间表示(IR)",
            PromptScenario.DIRECT_SQL: "直接生成SQL语句（用于IR无法表达的复杂查询）",
            PromptScenario.NARRATIVE: "将查询结果转换为自然语言叙述",
            PromptScenario.COT_PLANNER: "链式思维规划器，用于复杂问题拆解",
            PromptScenario.VECTOR_TABLE_SELECTOR: "基于向量检索的表选择器",
        }
        return descriptions.get(scenario, "")
    
    async def get_prompt(
        self, 
        scenario: str, 
        prompt_type: str,
        include_file_content: bool = False
    ) -> Optional[PromptTemplate]:
        """
        获取提示词模板
        
        Args:
            scenario: 场景标识
            prompt_type: 提示词类型
            include_file_content: 是否包含文件内容（用于对比）
        
        Returns:
            PromptTemplate 或 None
        """
        # 先尝试从数据库获取
        template = await self._get_from_db(scenario, prompt_type)
        
        # 获取文件路径
        try:
            scenario_enum = PromptScenario(scenario)
            type_enum = PromptType(prompt_type)
            file_path = SCENARIO_FILES.get(scenario_enum, {}).get(type_enum)
        except ValueError:
            file_path = None
        
        if template:
            template.file_path = file_path
            template.has_file = file_path and (PROMPTS_DIR / file_path).exists()
            
            if include_file_content and template.has_file:
                template.file_content = self._read_file(file_path)
            
            return template
        
        # 如果数据库没有，从文件读取
        if file_path:
            content = self._read_file(file_path)
            if content:
                return PromptTemplate(
                    scenario=scenario,
                    prompt_type=prompt_type,
                    display_name=f"{SCENARIO_LABELS.get(scenario_enum, scenario)}-{TYPE_LABELS.get(type_enum, prompt_type)}",
                    content=content,
                    is_active=False,  # 未激活数据库版本
                    file_path=file_path,
                    has_file=True,
                    file_content=content if include_file_content else None
                )
        
        return None
    
    async def get_active_prompt(self, scenario: str, prompt_type: str) -> Optional[str]:
        """
        获取激活的提示词内容（用于实际调用）
        
        优先级：数据库激活版本 > 文件版本
        
        Returns:
            提示词内容字符串
        """
        # 尝试从数据库获取激活版本
        template = await self._get_from_db(scenario, prompt_type, active_only=True)
        if template and template.is_active:
            return template.content
        
        # 从文件读取
        try:
            scenario_enum = PromptScenario(scenario)
            type_enum = PromptType(prompt_type)
            file_path = SCENARIO_FILES.get(scenario_enum, {}).get(type_enum)
            if file_path:
                return self._read_file(file_path)
        except ValueError:
            pass
        
        return None
    
    async def save_prompt(
        self,
        scenario: str,
        prompt_type: str,
        content: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        is_active: bool = True,
        user_id: Optional[UUID] = None,
        change_reason: Optional[str] = None,
        sync_to_file: bool = True  # 默认同步到文件
    ) -> PromptTemplate:
        """
        保存提示词模板到数据库
        
        Args:
            scenario: 场景标识
            prompt_type: 提示词类型
            content: 提示词内容
            display_name: 显示名称
            description: 描述
            is_active: 是否激活
            user_id: 操作用户ID
            change_reason: 变更原因
        
        Returns:
            保存后的模板
        """
        pool = await get_metadata_pool()
        
        # 生成默认显示名称
        if not display_name:
            try:
                scenario_enum = PromptScenario(scenario)
                type_enum = PromptType(prompt_type)
                display_name = f"{SCENARIO_LABELS.get(scenario_enum, scenario)}-{TYPE_LABELS.get(type_enum, prompt_type)}"
            except ValueError:
                display_name = f"{scenario}-{prompt_type}"
        
        async with pool.acquire() as conn:
            # 检查是否已存在
            existing = await conn.fetchrow("""
                SELECT template_id, version, content FROM prompt_templates
                WHERE scenario = $1 AND prompt_type = $2
            """, scenario, prompt_type)
            
            if existing:
                # 更新现有记录
                new_version = existing["version"] + 1
                
                # 先保存历史
                await conn.execute("""
                    INSERT INTO prompt_template_history 
                    (template_id, content, version, change_reason, changed_by)
                    VALUES ($1, $2, $3, $4, $5)
                """, existing["template_id"], existing["content"], existing["version"], 
                   change_reason, user_id)
                
                # 更新模板
                row = await conn.fetchrow("""
                    UPDATE prompt_templates SET
                        content = $1,
                        display_name = COALESCE($2, display_name),
                        description = COALESCE($3, description),
                        is_active = $4,
                        version = $5
                    WHERE template_id = $6
                    RETURNING *
                """, content, display_name, description, is_active, 
                   new_version, existing["template_id"])
            else:
                # 插入新记录
                row = await conn.fetchrow("""
                    INSERT INTO prompt_templates 
                    (scenario, prompt_type, display_name, description, content, is_active, created_by)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING *
                """, scenario, prompt_type, display_name, description, content, is_active, user_id)
            
            template = self._row_to_template(row)
            
            # 清除缓存使修改生效
            from server.utils.prompt_loader import clear_cache_for_prompt
            clear_cache_for_prompt(scenario, prompt_type)
            
            # 同步到文件（默认启用）
            if sync_to_file:
                self._write_to_file(scenario, prompt_type, content)
            
            return template
    
    async def toggle_active(
        self,
        scenario: str,
        prompt_type: str,
        is_active: bool,
        user_id: Optional[UUID] = None
    ) -> Optional[PromptTemplate]:
        """切换提示词激活状态"""
        pool = await get_metadata_pool()
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE prompt_templates SET is_active = $1
                WHERE scenario = $2 AND prompt_type = $3
                RETURNING *
            """, is_active, scenario, prompt_type)
            
            if row:
                return self._row_to_template(row)
            return None
    
    async def sync_from_file(
        self,
        scenario: str,
        prompt_type: str,
        user_id: Optional[UUID] = None
    ) -> Optional[PromptTemplate]:
        """从文件同步到数据库"""
        try:
            scenario_enum = PromptScenario(scenario)
            type_enum = PromptType(prompt_type)
            file_path = SCENARIO_FILES.get(scenario_enum, {}).get(type_enum)
        except ValueError:
            return None
        
        if not file_path:
            return None
        
        content = self._read_file(file_path)
        if not content:
            return None
        
        return await self.save_prompt(
            scenario=scenario,
            prompt_type=prompt_type,
            content=content,
            is_active=False,  # 同步后默认不激活
            user_id=user_id,
            change_reason="从文件同步"
        )
    
    async def export_to_file(
        self,
        scenario: str,
        prompt_type: str
    ) -> bool:
        """将数据库版本导出到文件"""
        template = await self._get_from_db(scenario, prompt_type)
        if not template:
            return False
        
        try:
            scenario_enum = PromptScenario(scenario)
            type_enum = PromptType(prompt_type)
            file_path = SCENARIO_FILES.get(scenario_enum, {}).get(type_enum)
        except ValueError:
            return False
        
        if not file_path:
            return False
        
        full_path = PROMPTS_DIR / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(template.content)
            return True
        except Exception as e:
            logger.error("导出提示词到文件失败", error=str(e), path=str(full_path))
            return False
    
    async def get_history(
        self,
        scenario: str,
        prompt_type: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """获取提示词版本历史"""
        pool = await get_metadata_pool()
        
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT h.*, u.username as changed_by_name
                FROM prompt_template_history h
                JOIN prompt_templates t ON h.template_id = t.template_id
                LEFT JOIN users u ON h.changed_by = u.user_id
                WHERE t.scenario = $1 AND t.prompt_type = $2
                ORDER BY h.version DESC
                LIMIT $3
            """, scenario, prompt_type, limit)
            
            return [dict(row) for row in rows]
    
    async def rollback_to_version(
        self,
        scenario: str,
        prompt_type: str,
        version: int,
        user_id: Optional[UUID] = None
    ) -> Optional[PromptTemplate]:
        """回滚到指定版本"""
        pool = await get_metadata_pool()
        
        async with pool.acquire() as conn:
            # 获取历史版本内容
            history = await conn.fetchrow("""
                SELECT h.content FROM prompt_template_history h
                JOIN prompt_templates t ON h.template_id = t.template_id
                WHERE t.scenario = $1 AND t.prompt_type = $2 AND h.version = $3
            """, scenario, prompt_type, version)
            
            if not history:
                return None
            
            return await self.save_prompt(
                scenario=scenario,
                prompt_type=prompt_type,
                content=history["content"],
                is_active=True,
                user_id=user_id,
                change_reason=f"回滚到版本 {version}"
            )
    
    async def _get_from_db(
        self, 
        scenario: str, 
        prompt_type: str,
        active_only: bool = False
    ) -> Optional[PromptTemplate]:
        """从数据库获取模板"""
        pool = await get_metadata_pool()
        
        async with pool.acquire() as conn:
            if active_only:
                row = await conn.fetchrow("""
                    SELECT * FROM prompt_templates
                    WHERE scenario = $1 AND prompt_type = $2 AND is_active = TRUE
                """, scenario, prompt_type)
            else:
                row = await conn.fetchrow("""
                    SELECT * FROM prompt_templates
                    WHERE scenario = $1 AND prompt_type = $2
                """, scenario, prompt_type)
            
            if row:
                return self._row_to_template(row)
            return None
    
    def _row_to_template(self, row) -> PromptTemplate:
        """将数据库行转换为模板对象"""
        return PromptTemplate(
            template_id=row["template_id"],
            scenario=row["scenario"],
            prompt_type=row["prompt_type"],
            display_name=row["display_name"],
            description=row.get("description"),
            content=row["content"],
            version=row["version"],
            is_active=row["is_active"],
            created_by=row.get("created_by"),
            created_at=str(row["created_at"]) if row.get("created_at") else None,
            updated_at=str(row["updated_at"]) if row.get("updated_at") else None,
        )
    
    def _read_file(self, file_path: str) -> Optional[str]:
        """读取提示词文件"""
        full_path = PROMPTS_DIR / file_path
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            logger.warning("读取提示词文件失败", path=str(full_path), error=str(e))
            return None
    
    def _write_to_file(self, scenario: str, prompt_type: str, content: str) -> bool:
        """将提示词写入文件"""
        try:
            scenario_enum = PromptScenario(scenario)
            type_enum = PromptType(prompt_type)
            file_path = SCENARIO_FILES.get(scenario_enum, {}).get(type_enum)
        except ValueError:
            logger.warning("无效的场景或类型", scenario=scenario, prompt_type=prompt_type)
            return False
        
        if not file_path:
            logger.warning("未找到对应的文件路径", scenario=scenario, prompt_type=prompt_type)
            return False
        
        full_path = PROMPTS_DIR / file_path
        try:
            full_path.parent.mkdir(parents=True, exist_ok=True)
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info("提示词已同步到文件", path=str(full_path))
            return True
        except Exception as e:
            logger.error("写入提示词文件失败", path=str(full_path), error=str(e))
            return False


# 全局服务实例
_prompt_service: Optional[PromptService] = None


async def get_prompt_service() -> PromptService:
    """获取提示词服务实例"""
    global _prompt_service
    if _prompt_service is None:
        _prompt_service = PromptService()
    return _prompt_service

