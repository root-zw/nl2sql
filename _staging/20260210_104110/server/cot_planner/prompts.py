"""
CoT 规划器的 Prompt 模板
"""

from pathlib import Path

from server.config import settings
from server.utils.prompt_loader import resolve_path, load_text


_DEFAULT_PROMPT = """你是一个链式思维（CoT）规划器。若未能加载 prompts/cot_planner/prompt.txt，请至少：
- 判断问题是否需要拆分（should_split: true/false）；
- 如需拆分，将问题拆解为多个原子步骤；
- 返回 {"thought": "...", "should_split": true/false, "split_reason": "...", "steps": [...]} 结构；
- 为每个步骤标注 dependencies，类型包括：filter_in, filter_value, filter_not_in, aggregate_input, join_key, subquery。
"""


def _load_prompt() -> str:
    default_path = Path(__file__).resolve().parents[2] / "prompts" / "cot_planner" / "prompt.txt"
    prompt_path = resolve_path(getattr(settings, "cot_planner_prompt_path", None), default_path)
    return load_text(prompt_path, default=_DEFAULT_PROMPT, prompt_name="cot_planner_system")


COT_PLANNING_SYSTEM_PROMPT = _load_prompt()
