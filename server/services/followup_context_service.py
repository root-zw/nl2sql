"""
结果后追问上下文解析服务

在当前阶段使用显式规则，把“基于上一结果继续分析”和“独立新问题”区分开。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class FollowupContextService:
    """结果后追问上下文解析服务"""

    @staticmethod
    def _find_latest_result_message(messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for message in reversed(messages or []):
            if message.get("role") != "assistant":
                continue
            if message.get("status") != "completed":
                continue
            if message.get("query_id") and (message.get("result_data") or message.get("result_summary") or message.get("content")):
                return message
        return None

    @staticmethod
    def _looks_like_compare(text: str) -> bool:
        compare_markers = (
            "对比",
            "比较",
            "相比",
            "相较",
            "和刚才",
            "跟刚才",
            "与刚才",
            "对照",
        )
        return any(marker in text for marker in compare_markers)

    @staticmethod
    def _looks_like_followup(text: str) -> bool:
        followup_prefixes = (
            "那",
            "那么",
            "再",
            "继续",
            "基于这个结果",
            "在这个结果里",
            "这个结果",
            "刚才",
            "上一条",
            "上一个结果",
            "其中",
            "分别",
            "按",
        )
        followup_markers = (
            "这个结果",
            "刚才",
            "上一条",
            "上一个结果",
            "其中",
            "分别",
            "明细",
            "展开",
            "细分",
            "按区域",
            "按城市",
            "按区县",
            "按月份",
        )
        return (
            text.startswith(followup_prefixes)
            or any(marker in text for marker in followup_markers)
            or (text.endswith("呢") and len(text) <= 12)
        )

    @staticmethod
    def _looks_like_standalone_query(text: str) -> bool:
        standalone_prefixes = (
            "查",
            "查询",
            "统计",
            "分析",
            "看",
            "列出",
            "展示",
            "告诉我",
            "我想看",
            "帮我查",
            "帮我统计",
            "请查",
            "请帮我查",
        )
        standalone_markers = (
            "多少",
            "哪些",
            "什么",
            "怎么",
            "趋势",
            "排名",
            "top",
            "同比",
            "环比",
            "分布",
            "情况",
            "？",
            "?",
        )
        lowered = text.lower()
        return text.startswith(standalone_prefixes) or any(marker in text for marker in standalone_markers) or "top" in lowered

    @staticmethod
    def _build_result_ref(message: Dict[str, Any]) -> Dict[str, Any]:
        result_data = message.get("result_data") or {}
        meta = result_data.get("meta") or {}
        columns = result_data.get("columns") or []
        rows = result_data.get("rows") or []
        ir_snapshot = meta.get("ir") or {}

        row_count = result_data.get("_original_row_count")
        if row_count is None:
            row_count = len(rows) if isinstance(rows, list) else None

        metric_ids: List[str] = []
        dimension_ids: List[str] = []
        if isinstance(ir_snapshot, dict):
            metric_ids = [item for item in ir_snapshot.get("metrics") or [] if isinstance(item, str)]
            dimension_ids = [item for item in ir_snapshot.get("dimensions") or [] if isinstance(item, str)]

        return {
            "query_id": message.get("query_id"),
            "message_id": message.get("message_id"),
            "result_source": "conversation_message",
            "table_ids": meta.get("selected_table_ids") or meta.get("table_ids") or [],
            "metric_ids": metric_ids,
            "dimension_ids": dimension_ids,
            "filter_summary": meta.get("filters") or None,
            "result_summary": message.get("result_summary") or message.get("content"),
            "row_count": row_count,
            "columns": [column.get("name") for column in columns if isinstance(column, dict) and column.get("name")],
        }

    @staticmethod
    def _build_analysis_context(mode: str, message: Dict[str, Any]) -> Dict[str, Any]:
        result_ref = FollowupContextService._build_result_ref(message)
        return {
            "context_mode": mode,
            "inherit_from_query_id": message.get("query_id"),
            "base_result_refs": [result_ref],
            "comparison_result_refs": [result_ref] if mode == "compare" else [],
            "scope_summary": result_ref.get("result_summary"),
            "carry_over_flags": {
                "table": True,
                "filters": True,
                "metrics": mode == "followup",
                "dimensions": True,
                "sort": mode == "followup",
            },
        }

    @staticmethod
    def resolve_followup_context(text: str, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        normalized = (text or "").strip()
        if not normalized:
            return {
                "resolution": "need_clarification",
                "message": "请说明你是要基于上一结果继续分析，还是要发起一个新问题。",
            }

        latest_result_message = FollowupContextService._find_latest_result_message(messages)
        if not latest_result_message:
            return {
                "resolution": "resolved_to_new_query",
            }

        if FollowupContextService._looks_like_compare(normalized):
            return {
                "resolution": "compare_with_result",
                "analysis_context": FollowupContextService._build_analysis_context("compare", latest_result_message),
            }

        if FollowupContextService._looks_like_followup(normalized):
            return {
                "resolution": "continue_on_result",
                "analysis_context": FollowupContextService._build_analysis_context("followup", latest_result_message),
            }

        if FollowupContextService._looks_like_standalone_query(normalized):
            return {
                "resolution": "resolved_to_new_query",
            }

        return {
            "resolution": "need_clarification",
            "message": "我还不确定你是在继续追问上一结果，还是已经开始了一个新问题。请补充得更明确一点。",
        }
