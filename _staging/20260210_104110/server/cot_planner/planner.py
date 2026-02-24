"""
思维链规划器 (Chain-of-Thought Planner)
"""

import json
import structlog
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from server.nl2ir.llm_client import LLMClient
from server.cot_planner.prompts import COT_PLANNING_SYSTEM_PROMPT
from server.dag_executor.models import ExecutionPlan, DAGNode, DAGDependency

logger = structlog.get_logger()


@dataclass
class PlanResult:
    """规划结果"""
    should_split: bool
    split_reason: str
    thought: str
    steps: List[Dict[str, Any]]


class CoTPlanner:
    """
    基于 CoT 的查询规划器
    """
    
    def __init__(self, llm_client: Optional[LLMClient] = None):
        # 使用 NL2IR 场景的配置（复杂查询规划与 NL2IR 类似）
        self.llm = llm_client or LLMClient(scenario="nl2ir")

    async def generate_plan(self, question: str, context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        生成规划步骤 (中间格式)
        
        Returns:
            List of steps (dict)
        """
        result = await self.generate_plan_with_info(question, context)
        return result.steps

    async def generate_plan_with_info(self, question: str, context: Dict[str, Any] = None) -> PlanResult:
        """
        生成规划步骤，返回完整信息
        
        Returns:
            PlanResult 包含 should_split、split_reason、thought、steps
        """
        messages = [
            {"role": "system", "content": COT_PLANNING_SYSTEM_PROMPT},
            {"role": "user", "content": f"用户问题: {question}"}
        ]
        
        # 添加上下文信息（如业务域）
        if context and context.get("domain_name"):
            messages[1]["content"] += f"\n\n业务域: {context['domain_name']}"

        try:
            response = await self.llm.chat_completion(
                messages=messages,
                temperature=0.1, # 规划需要精确
                response_format={"type": "json_object"} # 强制 JSON 输出
            )
            
            content = self.llm.get_text_content(response)
            plan_data = self._parse_json_response(content)
            
            # 解析规划结果
            should_split = plan_data.get("should_split", True)
            split_reason = plan_data.get("split_reason", "")
            thought = plan_data.get("thought", "")
            steps = plan_data.get("steps", [])
            
            logger.debug(
                "CoT 规划完成",
                thought=thought,
                should_split=should_split,
                split_reason=split_reason,
                steps_count=len(steps)
            )
            
            return PlanResult(
                should_split=should_split,
                split_reason=split_reason,
                thought=thought,
                steps=steps
            )
            
        except Exception as e:
            logger.error("CoT 规划失败", error=str(e))
            # 如果规划失败，可以抛出异常，让上层回退到单步模式
            raise

    async def should_use_dag(self, question: str, context: Dict[str, Any] = None) -> Tuple[bool, str]:
        """
        判断问题是否需要使用 DAG 执行
        
        Returns:
            (需要DAG, 原因说明)
        """
        result = await self.generate_plan_with_info(question, context)
        return result.should_split, result.split_reason

    def _parse_json_response(self, content: str) -> Dict[str, Any]:
        """解析 JSON 响应"""
        try:
            # 清理可能存在的 Markdown 标记
            if "```" in content:
                content = content.split("```json")[-1].split("```")[0]
            
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error("JSON 解析失败", content=content[:100], error=str(e))
            raise ValueError("LLM 返回的计划格式不正确")

# 全局实例
_planner = CoTPlanner()

def get_cot_planner() -> CoTPlanner:
    return _planner


