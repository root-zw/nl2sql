"""
查询追踪器 - 记录NL2SQL全流程的输入输出
用于调试和理解系统运行过程

追踪日志会保存到: trace_logs/ (项目根目录下)
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
import json
import os
from pathlib import Path
import structlog

logger = structlog.get_logger()

# 追踪日志输出目录（相对于项目根目录）
# 从 server/trace/tracer.py 向上3级到项目根目录
TRACE_LOG_DIR = Path(__file__).parent.parent.parent / "trace_logs"
TRACE_LOG_DIR.mkdir(parents=True, exist_ok=True)


class QueryTraceStep:
    """单个追踪步骤"""
    def __init__(
        self,
        step_name: str,
        step_type: str,
        description: str
    ):
        self.step_name = step_name
        self.step_type = step_type  # retrieval, parsing, compilation, execution
        self.description = description
        self.input_data: Optional[Dict[str, Any]] = None
        self.output_data: Optional[Dict[str, Any]] = None
        self.metadata: Dict[str, Any] = {}
        self.error: Optional[str] = None
        self.start_time: datetime = datetime.now()
        self.end_time: Optional[datetime] = None
        self.duration_ms: Optional[float] = None

    def set_input(self, data: Dict[str, Any]):
        """设置输入数据"""
        self.input_data = data

    def set_output(self, data: Dict[str, Any]):
        """设置输出数据"""
        self.output_data = data
        self.end_time = datetime.now()
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000

    def set_error(self, error: str):
        """设置错误信息"""
        self.error = error
        self.end_time = datetime.now()
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000

    def add_metadata(self, key: str, value: Any):
        """添加元数据"""
        self.metadata[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "step_name": self.step_name,
            "step_type": self.step_type,
            "description": self.description,
            "input": self.input_data,
            "output": self.output_data,
            "metadata": self.metadata,
            "error": self.error,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": round(self.duration_ms, 2) if self.duration_ms else None
        }


class QueryTracer:
    """查询追踪器 - 记录完整的查询流程"""

    def __init__(self, query_id: str, question: str, connection_id: str):
        self.query_id = query_id
        self.question = question
        self.connection_id = connection_id
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        self.steps: List[QueryTraceStep] = []
        self.current_step: Optional[QueryTraceStep] = None
        self.final_result: Optional[Dict[str, Any]] = None
        self.total_duration_ms: Optional[float] = None
        # 标记是否已完成（用于支持恢复续接）
        self._is_finalized: bool = False
        # 原始query_id（如果是从另一个tracer续接的）
        self.original_query_id: Optional[str] = None

    def start_step(
        self,
        step_name: str,
        step_type: str,
        description: str
    ) -> QueryTraceStep:
        """开始一个新步骤"""
        step = QueryTraceStep(step_name, step_type, description)
        self.steps.append(step)
        self.current_step = step

        logger.info(
            "step_start",
            query_id=self.query_id,
            step=step_name,
            step_type=step_type
        )

        return step

    def end_step(self):
        """结束当前步骤"""
        if self.current_step:
            logger.info(
                "step_end",
                query_id=self.query_id,
                step=self.current_step.step_name,
                duration_ms=self.current_step.duration_ms
            )
            self.current_step = None

    def finalize(self, final_result: Optional[Dict[str, Any]] = None, save_to_file: bool = True):
        """完成追踪
        
        Args:
            final_result: 最终结果
            save_to_file: 是否保存到文件。当表选择需要确认时，设为False暂不保存，
                          等待用户确认后续接tracer继续追踪
        """
        self.end_time = datetime.now()
        self.total_duration_ms = (self.end_time - self.start_time).total_seconds() * 1000
        self.final_result = final_result
        self._is_finalized = True

        logger.info(
            "trace_complete",
            query_id=self.query_id,
            total_steps=len(self.steps),
            total_duration_ms=round(self.total_duration_ms, 2),
            saved=save_to_file
        )

        # 保存追踪日志到文件
        if save_to_file:
            self._save_to_file()
    
    def resume(self, new_query_id: str) -> "QueryTracer":
        """恢复已暂停的tracer继续追踪
        
        当表选择需要确认后，用户确认时会发起新请求。此方法将新请求的步骤
        合并到原始tracer中，确保完整流程记录在同一个日志文件。
        
        Args:
            new_query_id: 新请求的query_id（用于日志追踪）
            
        Returns:
            self - 返回自身以支持链式调用
        """
        # 重置完成状态，允许继续添加步骤
        self._is_finalized = False
        self.end_time = None
        self.total_duration_ms = None
        self.final_result = None
        
        logger.info(
            "trace_resumed",
            query_id=self.query_id,
            resumed_with=new_query_id,
            existing_steps=len(self.steps)
        )
        
        return self
    
    @property
    def is_finalized(self) -> bool:
        """是否已完成"""
        return self._is_finalized

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于API返回）"""
        return {
            "query_id": self.query_id,
            "question": self.question,
            "connection_id": self.connection_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_duration_ms": round(self.total_duration_ms, 2) if self.total_duration_ms else None,
            "steps": [step.to_dict() for step in self.steps],
            "flow_overview": self._build_flow_overview(),
            "final_result": self.final_result,
            "summary": {
                "total_steps": len(self.steps),
                "successful_steps": len([s for s in self.steps if not s.error]),
                "failed_steps": len([s for s in self.steps if s.error]),
                "retrieval_time_ms": sum(
                    s.duration_ms or 0
                    for s in self.steps
                    if s.step_type == "retrieval"
                ),
                "parsing_time_ms": sum(
                    s.duration_ms or 0
                    for s in self.steps
                    if s.step_type == "parsing"
                ),
                "compilation_time_ms": sum(
                    s.duration_ms or 0
                    for s in self.steps
                    if s.step_type == "compilation"
                ),
                "execution_time_ms": sum(
                    s.duration_ms or 0
                    for s in self.steps
                    if s.step_type == "execution"
                )
            }
        }

    def to_markdown(self) -> str:
        """转换为Markdown格式（用于阅读）"""
        lines = []
        lines.append("# 查询追踪报告")
        lines.append("")
        lines.append(f"- 查询ID: `{self.query_id}`")
        lines.append(f"- 用户问题: {self.question}")
        lines.append(f"- 连接ID: `{self.connection_id}`")
        lines.append(f"- 总耗时: {round(self.total_duration_ms, 2)}ms")
        lines.append("")
        
        lines.append("## 流程概览")
        lines.append("")
        lines.append("| 序号 | 步骤 | 类型 | 状态 | 耗时(ms) |")
        lines.append("| --- | --- | --- | --- | --- |")
        overview = self._build_flow_overview()
        for item in overview:
            status_icon = "❌" if item['status'] == 'error' else "✅"
            lines.append(
                f"| {item['index']} | {status_icon} {item['step_name']} | {item['step_type']} | "
                f"{'失败' if item['status']=='error' else '成功'} | {item['duration_ms'] or 0:.2f} |"
            )
        lines.append("")
        lines.append("---")
        lines.append("")

        for i, step in enumerate(self.steps, 1):
            lines.append(f"## 步骤 {i}: {step.step_name}")
            lines.append("")
            lines.append(f"- 类型: {step.step_type}")
            lines.append(f"- 描述: {step.description}")
            lines.append(f"- 状态: {'失败' if step.error else '成功'}")
            lines.append(f"- 耗时: {round(step.duration_ms, 2) if step.duration_ms else 0}ms")
            lines.append(f"- 开始时间: {step.start_time.isoformat()}")
            lines.append(f"- 结束时间: {step.end_time.isoformat() if step.end_time else 'N/A'}")
            if step.error:
                lines.append(f"- 错误: {step.error}")
            lines.append("")

            input_block = self._format_json_block(step.input_data)
            if input_block:
                lines.append("### 输入")
                lines.extend(input_block)
                lines.append("")

            output_block = self._format_json_block(step.output_data)
            if output_block:
                lines.append("### 输出")
                lines.extend(output_block)
                lines.append("")

            metadata_block = self._format_json_block(step.metadata)
            if metadata_block:
                lines.append("### 元数据")
                lines.extend(metadata_block)
                lines.append("")

            lines.append("---")
            lines.append("")

        final_block = self._format_json_block(self.final_result)
        if final_block:
            lines.append("## 最终结果")
            lines.append("")
            lines.extend(final_block)
            lines.append("")

        return "\n".join(lines)

    def print_summary(self):
        """打印摘要到控制台"""
        print("\n" + "="*80)
        print(f"   查询追踪摘要")
        print("="*80)
        print(f"查询ID: {self.query_id}")
        print(f"问题: {self.question}")
        print(f"总耗时: {round(self.total_duration_ms, 2)}ms")
        print(f"")
        print(f"步骤明细:")
        for i, step in enumerate(self.steps, 1):
            status = "" if step.error else ""
            duration = round(step.duration_ms, 2) if step.duration_ms else 0
            print(f"  {i}. {status} {step.step_name} ({duration}ms)")
        print("="*80 + "\n")

    def _save_to_file(self):
        """保存追踪日志到文件"""
        try:
            # 生成文件名：YYYYMMDD_HHMMSS_{query_id}.md
            timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
            md_filename = f"{timestamp}_{self.query_id[:8]}.md"
            md_filepath = TRACE_LOG_DIR / md_filename
            
            # 保存Markdown格式（便于阅读）
            with open(md_filepath, 'w', encoding='utf-8') as f:
                f.write(self.to_markdown())

            logger.info(
                "trace_saved",
                md_file=str(md_filepath)
            )
        except Exception as e:
            logger.warning("保存追踪日志失败", error=str(e))

    def _build_flow_overview(self) -> List[Dict[str, Any]]:
        flow = []
        for idx, step in enumerate(self.steps, 1):
            flow.append({
                "index": idx,
                "step_name": step.step_name,
                "step_type": step.step_type,
                "description": step.description,
                "status": "error" if step.error else "success",
                "duration_ms": round(step.duration_ms, 2) if step.duration_ms else None,
                "start_time": step.start_time.isoformat(),
                "end_time": step.end_time.isoformat() if step.end_time else None
            })
        return flow

    def _build_flow_diagram(self) -> str:
        """生成 ASCII 流程图"""
        # 步骤类型分组和图标映射
        type_icons = {
            "table_selection": "🔍",
            "parsing": "📝",
            "validation": "✓",
            "caching": "💾",
            "compilation": "⚙️",
            "execution": "▶️",
            "formatting": "📊",
            "explanation": "💡",
            "narrative": "📖",
            "permission": "🔐",
        }
        
        # 阶段分组
        phases = {
            "表选择": ["table_selection"],
            "解析": ["parsing"],
            "验证": ["validation", "caching", "permission"],
            "编译": ["compilation"],
            "执行": ["execution"],
            "输出": ["formatting", "explanation", "narrative"]
        }
        
        # 按阶段统计
        phase_steps = {}
        phase_times = {}
        for phase_name, types in phases.items():
            phase_steps[phase_name] = []
            phase_times[phase_name] = 0.0
            for step in self.steps:
                if step.step_type in types:
                    phase_steps[phase_name].append(step)
                    phase_times[phase_name] += step.duration_ms or 0
        
        lines = []
        lines.append("```")
        
        # 生成流程图
        active_phases = [(name, steps) for name, steps in phase_steps.items() if steps]
        
        for i, (phase_name, steps) in enumerate(active_phases):
            time_ms = phase_times[phase_name]
            step_names = [s.step_name for s in steps]
            icon = type_icons.get(steps[0].step_type, "•")
            
            # 状态
            has_error = any(s.error for s in steps)
            status = "❌" if has_error else "✅"
            
            # 格式化
            if len(step_names) == 1:
                content = step_names[0]
            else:
                content = ", ".join(step_names)
            
            lines.append(f"┌─{'─'*50}┐")
            lines.append(f"│ {status} {phase_name}: {content:<42} │")
            lines.append(f"│    耗时: {time_ms:,.0f}ms{' '*(38-len(f'{time_ms:,.0f}'))} │")
            lines.append(f"└─{'─'*50}┘")
            
            # 连接线
            if i < len(active_phases) - 1:
                lines.append("              │")
                lines.append("              ▼")
        
        lines.append("```")
        
        # 添加耗时汇总
        lines.append("")
        lines.append("**耗时分布:**")
        total_time = sum(phase_times.values())
        for phase_name, time_ms in phase_times.items():
            if time_ms > 0:
                pct = (time_ms / total_time * 100) if total_time > 0 else 0
                bar_len = int(pct / 5)  # 20格满
                bar = "█" * bar_len + "░" * (20 - bar_len)
                lines.append(f"- {phase_name}: {bar} {time_ms:,.0f}ms ({pct:.1f}%)")
        
        return "\n".join(lines)

    def _format_json_block(self, data: Optional[Dict[str, Any]]) -> List[str]:
        if not data:
            return []
        if isinstance(data, dict):
            markdown_sections: List[tuple] = []
            regular_entries: Dict[str, Any] = {}
            
            for key, value in data.items():
                # 检查顶层的 markdown 内容
                sections = self._collect_markdown_sections(value, default_title=key)
                if sections:
                    markdown_sections.extend(sections)
                # 检查嵌套的 markdown 内容（如 original_llm_selection.llm_prompts）
                elif isinstance(value, dict) and "llm_prompts" in value:
                    # 提取嵌套的 llm_prompts
                    nested_prompts = value.get("llm_prompts", [])
                    nested_sections = self._collect_markdown_sections(nested_prompts, default_title="llm_prompts")
                    if nested_sections:
                        markdown_sections.extend(nested_sections)
                    # 其他字段作为普通 JSON
                    other_fields = {k: v for k, v in value.items() if k != "llm_prompts"}
                    if other_fields:
                        regular_entries[key] = other_fields
                else:
                    regular_entries[key] = value

            lines: List[str] = []
            if regular_entries:
                lines.append("```json")
                lines.append(json.dumps(regular_entries, ensure_ascii=False, indent=2))
                lines.append("```")
            lines.extend(self._render_markdown_sections(markdown_sections))
            return lines

        if isinstance(data, list):
            if data and all(isinstance(item, dict) and item.get("__type__") == "markdown" for item in data):
                sections = [
                    (item.get("title") or "内容", item.get("content") or "")
                    for item in data
                ]
                return self._render_markdown_sections(sections)
            return [
                "```json",
                json.dumps(data, ensure_ascii=False, indent=2),
                "```"
            ]

        return [
            "```json",
            json.dumps(data, ensure_ascii=False, indent=2),
            "```"
        ]

    @staticmethod
    def _collect_markdown_sections(value: Any, default_title: str) -> List[tuple]:
        sections: List[tuple] = []
        if isinstance(value, dict) and value.get("__type__") == "markdown":
            sections.append((value.get("title") or default_title, value.get("content") or ""))
            return sections
        if isinstance(value, list) and value and all(isinstance(item, dict) and item.get("__type__") == "markdown" for item in value):
            for idx, item in enumerate(value):
                sections.append((item.get("title") or f"{default_title} #{idx + 1}", item.get("content") or ""))
            return sections
        return []

    @staticmethod
    def _render_markdown_sections(sections: List[tuple]) -> List[str]:
        """渲染 markdown 内容段落（如 LLM 提示词），使用折叠块避免干扰大纲"""
        if not sections:
            return []
        lines: List[str] = []
        for title, content in sections:
            # 使用 <details> 折叠块
            lines.append(f"<details>")
            lines.append(f"<summary><b>{title or '详情'}</b></summary>")
            lines.append("")
            if content:
                # 将内容中的 # 标题替换，避免被 VSCode 大纲解析
                # VSCode 大纲是基于正则扫描整个文件的 # 开头行
                sanitized = str(content)
                # 替换行首的 # 为 ⌗（视觉相似但不会被解析为标题）
                sanitized_lines = []
                for line in sanitized.splitlines():
                    if line.startswith("#"):
                        # 替换行首的连续 # 为 ⌗
                        i = 0
                        while i < len(line) and line[i] == "#":
                            i += 1
                        sanitized_lines.append("⌗" * i + line[i:])
                    else:
                        sanitized_lines.append(line)
                
                lines.append("```text")
                lines.extend(sanitized_lines)
                lines.append("```")
            else:
                lines.append("(空)")
            lines.append("")
            lines.append("</details>")
            lines.append("")
        return lines


# 全局存储（内存中保留最近N个查询的追踪）
_trace_storage: Dict[str, QueryTracer] = {}
_max_traces = 100  # 最多保留100个


def create_tracer(query_id: str, question: str, connection_id: str) -> QueryTracer:
    """创建一个新的追踪器"""
    tracer = QueryTracer(query_id, question, connection_id)

    # 保存到全局存储
    _trace_storage[query_id] = tracer

    # 如果超过最大数量，删除最老的
    if len(_trace_storage) > _max_traces:
        oldest_key = next(iter(_trace_storage))
        del _trace_storage[oldest_key]

    return tracer


def get_or_resume_tracer(
    query_id: str, 
    question: str, 
    connection_id: str,
    original_query_id: Optional[str] = None
) -> QueryTracer:
    """获取或恢复追踪器
    
    当提供 original_query_id 时，尝试恢复原始 tracer 继续追踪，
    这样可以确保一次完整的查询流程（包括表选择确认）记录在同一个日志文件中。
    
    Args:
        query_id: 当前请求的 query_id
        question: 用户问题
        connection_id: 连接ID
        original_query_id: 原始请求的 query_id（用户确认表选择后的续接请求）
        
    Returns:
        QueryTracer 实例
    """
    # 如果有原始 query_id，尝试恢复原始 tracer
    if original_query_id:
        original_tracer = get_tracer(original_query_id)
        if original_tracer:
            # 恢复原始 tracer 继续追踪
            original_tracer.resume(query_id)
            
            # 同时在新 query_id 下也存储引用，方便后续查找
            _trace_storage[query_id] = original_tracer
            
            logger.info(
                "tracer_resumed",
                original_query_id=original_query_id,
                new_query_id=query_id,
                existing_steps=len(original_tracer.steps)
            )
            
            return original_tracer
        else:
            # 原始 tracer 不在内存中，创建新的并记录关联
            logger.warning(
                "original_tracer_not_found",
                original_query_id=original_query_id,
                new_query_id=query_id
            )
            tracer = create_tracer(query_id, question, connection_id)
            tracer.original_query_id = original_query_id
            return tracer
    
    # 没有原始 query_id，创建新的 tracer
    return create_tracer(query_id, question, connection_id)


def get_tracer(query_id: str) -> Optional[QueryTracer]:
    """获取追踪器"""
    return _trace_storage.get(query_id)


def get_all_tracers() -> List[QueryTracer]:
    """获取所有追踪器"""
    return list(_trace_storage.values())


def clear_tracers():
    """清空所有追踪器"""
    _trace_storage.clear()

