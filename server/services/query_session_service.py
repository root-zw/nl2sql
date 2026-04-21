"""
查询会话状态服务

统一维护查询产品级状态，避免状态仅散落在消息、活跃查询和 trace 中。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional
from uuid import UUID

import asyncpg
import structlog

from server.utils.db_pool import get_metadata_pool
from server.utils.json_utils import sanitize_for_json

logger = structlog.get_logger()

RESULT_ACTION_NODES = {
    "question_intake",
    "draft_generation",
    "connection_resolution",
    "permission_resolution",
    "table_resolved",
    "ir_ready",
    "completed",
    "failed",
}

PENDING_USER_ACTION_NODES = (
    "table_resolution",
    "execution_guard",
    "draft_confirmation",
)


class QuerySessionService:
    """查询会话状态服务"""

    def __init__(self, db_conn: Optional[asyncpg.Connection] = None):
        self.db = db_conn

    @asynccontextmanager
    async def _acquire(self) -> AsyncIterator[asyncpg.Connection]:
        if self.db is not None:
            yield self.db
            return

        pool = await get_metadata_pool()
        conn = await pool.acquire()
        try:
            yield conn
        finally:
            await pool.release(conn)

    @staticmethod
    def _dump_state(state: Optional[Dict[str, Any]]) -> str:
        return json.dumps(sanitize_for_json(QuerySessionService._normalize_state(state)), ensure_ascii=False)

    @staticmethod
    def _normalize_state(state: Optional[Any]) -> Dict[str, Any]:
        if state is None:
            return {}

        if isinstance(state, str):
            try:
                state = json.loads(state)
            except json.JSONDecodeError:
                logger.warning("query_session state_json 不是合法 JSON，已按空对象处理")
                return {}

        if isinstance(state, Mapping):
            return dict(state)

        logger.warning(
            "query_session state_json 类型异常，已按空对象处理",
            state_type=type(state).__name__,
        )
        return {}

    @staticmethod
    def _merge_state(current_state: Optional[Dict[str, Any]], updates: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        merged = QuerySessionService._normalize_state(current_state)
        if updates:
            normalized_updates = QuerySessionService._normalize_state(updates)
            delete_keys = normalized_updates.pop("__delete_keys__", []) or []
            merged.update(sanitize_for_json(normalized_updates))
            for key in delete_keys:
                if key:
                    merged.pop(str(key), None)
        return merged

    @staticmethod
    def _normalize_confirmation_state(payload: Optional[Any]) -> Dict[str, Any]:
        if payload is None:
            return {}
        if hasattr(payload, "model_dump"):
            payload = payload.model_dump()
        return QuerySessionService._normalize_state(payload)

    @staticmethod
    def _sanitize_table_names(items: Optional[list[Any]]) -> list[str]:
        normalized_names: list[str] = []
        seen_names: set[str] = set()
        for item in items or []:
            name = str(item).strip()
            if not name or QuerySessionService._looks_like_uuid(name) or name in seen_names:
                continue
            seen_names.add(name)
            normalized_names.append(name)
        return normalized_names

    @staticmethod
    def _sanitize_summary_items(items: Optional[list[Any]]) -> list[str]:
        normalized_items: list[str] = []
        for item in items or []:
            text = str(item).strip()
            if not text or QuerySessionService._looks_like_uuid(text):
                continue
            for prefix in ("当前数据表：", "当前涉及数据表："):
                if text.startswith(prefix):
                    table_names = QuerySessionService._sanitize_table_names(text[len(prefix):].split("、"))
                    if not table_names:
                        text = ""
                    else:
                        text = f"{prefix}{'、'.join(table_names)}"
                    break
            if text:
                normalized_items.append(text)
        return normalized_items

    @staticmethod
    def build_table_resolution_state(
        payload: Optional[Any] = None,
        *,
        question_text: Optional[str] = None,
        recommended_table_ids: Optional[list[str]] = None,
        selected_table_ids: Optional[list[str]] = None,
        rejected_table_ids: Optional[list[str]] = None,
        multi_table_mode: Optional[str] = None,
        manual_table_override: Optional[bool] = None,
    ) -> Dict[str, Any]:
        table_state = QuerySessionService._normalize_confirmation_state(payload)
        return sanitize_for_json(
            {
                "question": table_state.get("question") or question_text,
                "message": table_state.get("message"),
                "reason_summary": table_state.get("reason_summary") or table_state.get("confirmation_reason"),
                "candidates": list(table_state.get("candidates") or []),
                "recommended_table_ids": list(
                    recommended_table_ids
                    if recommended_table_ids is not None
                    else table_state.get("recommended_table_ids") or []
                ),
                "selected_table_ids": list(
                    selected_table_ids
                    if selected_table_ids is not None
                    else table_state.get("selected_table_ids") or []
                ),
                "rejected_table_ids": list(
                    rejected_table_ids
                    if rejected_table_ids is not None
                    else table_state.get("rejected_table_ids") or []
                ),
                "allow_multi_select": bool(table_state.get("allow_multi_select")),
                "multi_table_mode": multi_table_mode or table_state.get("multi_table_mode"),
                "manual_table_override": (
                    bool(manual_table_override)
                    if manual_table_override is not None
                    else bool(table_state.get("manual_table_override"))
                ),
            }
        )

    @staticmethod
    def build_draft_state(
        payload: Optional[Any] = None,
        *,
        status: Optional[str] = None,
        natural_language: Optional[str] = None,
        draft_json: Optional[Dict[str, Any]] = None,
        warnings: Optional[list[Any]] = None,
        suggestions: Optional[list[Any]] = None,
        confidence: Optional[float] = None,
        ambiguities: Optional[list[Any]] = None,
        open_points: Optional[list[Any]] = None,
        selected_table_names: Optional[list[str]] = None,
        confirmed: Optional[bool] = None,
        confirmation_required: Optional[bool] = None,
        table_dependent: Optional[bool] = None,
        invalidate_on_table_change: Optional[bool] = None,
    ) -> Dict[str, Any]:
        draft_state = QuerySessionService._normalize_confirmation_state(payload)
        resolved_table_dependent = True if table_dependent is None else bool(table_dependent)
        if table_dependent is None and draft_state.get("table_dependent") is not None:
            resolved_table_dependent = bool(draft_state.get("table_dependent"))

        resolved_invalidate_on_table_change = (
            True if invalidate_on_table_change is None else bool(invalidate_on_table_change)
        )
        if invalidate_on_table_change is None and draft_state.get("invalidate_on_table_change") is not None:
            resolved_invalidate_on_table_change = bool(draft_state.get("invalidate_on_table_change"))

        return sanitize_for_json(
            {
                "status": status or draft_state.get("status"),
                "natural_language": natural_language or draft_state.get("natural_language"),
                "draft_json": draft_json if draft_json is not None else draft_state.get("draft_json") or draft_state.get("ir"),
                "warnings": list(warnings if warnings is not None else draft_state.get("warnings") or []),
                "suggestions": list(suggestions if suggestions is not None else draft_state.get("suggestions") or []),
                "confidence": (
                    confidence
                    if confidence is not None
                    else draft_state.get("confidence")
                ),
                "ambiguities": list(
                    ambiguities
                    if ambiguities is not None
                    else draft_state.get("ambiguities") or []
                ),
                "open_points": list(
                    open_points
                    if open_points is not None
                    else draft_state.get("open_points") or []
                ),
                "selected_table_names": list(
                    QuerySessionService._sanitize_table_names(
                        selected_table_names
                        if selected_table_names is not None
                        else draft_state.get("selected_table_names") or []
                    )
                ),
                "confirmed": bool(confirmed) if confirmed is not None else bool(draft_state.get("confirmed")),
                "confirmation_required": (
                    bool(confirmation_required)
                    if confirmation_required is not None
                    else bool(draft_state.get("confirmation_required"))
                ),
                "table_dependent": resolved_table_dependent,
                "invalidate_on_table_change": resolved_invalidate_on_table_change,
            }
        )

    @staticmethod
    def build_provisional_draft_state(
        payload: Optional[Any] = None,
        *,
        status: Optional[str] = None,
        question_text: Optional[str] = None,
        selected_table_ids: Optional[list[str]] = None,
        recommended_table_ids: Optional[list[str]] = None,
        manual_table_override: Optional[bool] = None,
        allow_multi_select: Optional[bool] = None,
        natural_language: Optional[str] = None,
        draft_json: Optional[Dict[str, Any]] = None,
        warnings: Optional[list[Any]] = None,
        suggestions: Optional[list[Any]] = None,
        confidence: Optional[float] = None,
        ambiguities: Optional[list[Any]] = None,
        open_points: Optional[list[Any]] = None,
        selected_table_names: Optional[list[str]] = None,
        confirmation_required: Optional[bool] = None,
        table_dependent: Optional[bool] = None,
        invalidate_on_table_change: Optional[bool] = None,
    ) -> Dict[str, Any]:
        draft_state = QuerySessionService._normalize_confirmation_state(payload)
        resolved_confirmation_required = confirmation_required
        if resolved_confirmation_required is None and status in {"pending_generation", "awaiting_confirmation"}:
            resolved_confirmation_required = True
        if resolved_confirmation_required is None and draft_state.get("confirmation_required") is not None:
            resolved_confirmation_required = bool(draft_state.get("confirmation_required"))
        if resolved_confirmation_required is None:
            resolved_confirmation_required = False

        # 如果传入的已经是草稿形态，直接规整为 provisional draft。
        if any(
            value is not None
            for value in (
                natural_language,
                draft_json,
                warnings,
                suggestions,
                status,
                confirmation_required,
                table_dependent,
                invalidate_on_table_change,
            )
        ) or any(
            draft_state.get(field) is not None
            for field in ("natural_language", "draft_json", "status", "warnings", "suggestions")
        ) and not draft_state.get("candidates"):
            return QuerySessionService.build_draft_state(
                draft_state,
                status=status or draft_state.get("status") or "provisional",
                natural_language=natural_language or draft_state.get("natural_language"),
                draft_json=(
                    draft_json
                    if draft_json is not None
                    else draft_state.get("draft_json")
                ),
                warnings=list(warnings if warnings is not None else draft_state.get("warnings") or []),
                suggestions=list(suggestions if suggestions is not None else draft_state.get("suggestions") or []),
                confidence=(
                    confidence
                    if confidence is not None
                    else draft_state.get("confidence")
                ),
                ambiguities=list(
                    ambiguities
                    if ambiguities is not None
                    else draft_state.get("ambiguities") or []
                ),
                open_points=list(
                    open_points
                    if open_points is not None
                    else draft_state.get("open_points") or []
                ),
                selected_table_names=list(
                    selected_table_names
                    if selected_table_names is not None
                    else draft_state.get("selected_table_names") or []
                ),
                confirmed=False,
                confirmation_required=resolved_confirmation_required,
                table_dependent=table_dependent,
                invalidate_on_table_change=invalidate_on_table_change,
            )

        candidates = list(draft_state.get("candidates") or [])
        preferred_table_ids = list(selected_table_ids or recommended_table_ids or [])

        candidate_names: list[str] = []
        seen_names: set[str] = set()
        for candidate in candidates:
            table_id = candidate.get("table_id")
            table_name = str(candidate.get("table_name") or "").strip()
            if not table_name or table_name in seen_names:
                continue
            if QuerySessionService._looks_like_uuid(table_name):
                continue
            if preferred_table_ids and table_id not in preferred_table_ids:
                continue
            seen_names.add(table_name)
            candidate_names.append(table_name)

        if not candidate_names:
            for candidate in candidates:
                table_name = str(candidate.get("table_name") or "").strip()
                if not table_name or table_name in seen_names:
                    continue
                if QuerySessionService._looks_like_uuid(table_name):
                    continue
                seen_names.add(table_name)
                candidate_names.append(table_name)
                if len(candidate_names) >= 2:
                    break

        candidate_names = candidate_names[:2]
        table_hint = "、".join(candidate_names)
        resolved_manual_table_override = (
            bool(manual_table_override)
            if manual_table_override is not None
            else bool(draft_state.get("manual_table_override"))
        )

        if resolved_manual_table_override:
            natural_language = (
                f"当前问题是“{question_text}”。系统已切换为手动选表，确认数据表后会重新生成查询草稿。"
                if question_text
                else "系统已切换为手动选表，确认数据表后会重新生成查询草稿。"
            )
        elif table_hint and question_text:
            natural_language = (
                f"当前暂按候选表“{table_hint}”理解您的问题“{question_text}”，"
                "确认选表后系统会继续细化查询草稿。"
            )
        elif question_text:
            natural_language = f"系统已根据问题“{question_text}”生成暂定查询理解，确认选表后会继续细化查询草稿。"
        elif table_hint:
            natural_language = f"系统已基于候选表“{table_hint}”生成暂定查询理解，确认选表后会继续细化查询草稿。"
        else:
            natural_language = "系统已生成暂定查询理解，确认选表后会继续细化查询草稿。"

        resolved_allow_multi_select = (
            bool(allow_multi_select)
            if allow_multi_select is not None
            else bool(draft_state.get("allow_multi_select"))
        )
        warnings: list[str] = []
        if len(preferred_table_ids) > 1 or resolved_allow_multi_select:
            warnings.append("当前问题可能涉及多表查询，最终草稿会在确认选表后重新生成。")

        return QuerySessionService.build_draft_state(
            status=status or "provisional",
            natural_language=natural_language,
            draft_json=None,
            warnings=warnings,
            suggestions=[],
            confidence=None,
            ambiguities=[],
            open_points=[],
            selected_table_names=candidate_names,
            confirmed=False,
            confirmation_required=False,
            table_dependent=True,
            invalidate_on_table_change=True,
        )

    @staticmethod
    def has_confirmed_draft(state: Optional[Any]) -> bool:
        session_state = QuerySessionService._normalize_state(state)
        confirmed_draft = QuerySessionService._normalize_state(session_state.get("confirmed_draft"))
        if not confirmed_draft:
            return False

        if confirmed_draft.get("confirmed") is not None:
            return bool(confirmed_draft.get("confirmed"))
        if confirmed_draft.get("status"):
            return str(confirmed_draft.get("status")) == "confirmed"
        return any(
            confirmed_draft.get(field)
            for field in ("natural_language", "draft_json", "warnings", "suggestions")
        )

    @staticmethod
    def requires_draft_confirmation(state: Optional[Any]) -> bool:
        session_state = QuerySessionService._normalize_state(state)
        provisional_draft = QuerySessionService._normalize_state(session_state.get("provisional_draft"))
        if not provisional_draft or QuerySessionService.has_confirmed_draft(session_state):
            return False

        if provisional_draft.get("confirmation_required") is not None:
            return bool(provisional_draft.get("confirmation_required"))

        return str(provisional_draft.get("status") or "") in {"pending_generation", "awaiting_confirmation"}

    @staticmethod
    def build_confirmed_draft_state(
        payload: Optional[Any] = None,
        *,
        natural_language: Optional[str] = None,
        draft_json: Optional[Dict[str, Any]] = None,
        warnings: Optional[list[Any]] = None,
        suggestions: Optional[list[Any]] = None,
        confidence: Optional[float] = None,
        ambiguities: Optional[list[Any]] = None,
        open_points: Optional[list[Any]] = None,
        selected_table_names: Optional[list[str]] = None,
    ) -> Dict[str, Any]:
        draft_state = QuerySessionService._normalize_confirmation_state(payload)
        return QuerySessionService.build_draft_state(
            draft_state,
            status="confirmed",
            natural_language=natural_language or draft_state.get("natural_language"),
            draft_json=(
                draft_json
                if draft_json is not None
                else draft_state.get("draft_json") or draft_state.get("ir")
            ),
            warnings=list(warnings if warnings is not None else draft_state.get("warnings") or []),
            suggestions=list(suggestions if suggestions is not None else draft_state.get("suggestions") or []),
            confidence=(
                confidence
                if confidence is not None
                else draft_state.get("confidence")
            ),
            ambiguities=list(
                ambiguities
                if ambiguities is not None
                else draft_state.get("ambiguities") or []
            ),
            open_points=list(
                open_points
                if open_points is not None
                else draft_state.get("open_points") or []
            ),
            selected_table_names=list(
                selected_table_names
                if selected_table_names is not None
                else draft_state.get("selected_table_names") or []
            ),
            confirmed=True,
            confirmation_required=False,
            table_dependent=True,
            invalidate_on_table_change=True,
        )

    @staticmethod
    def build_downstream_confirmed_draft_state(
        state: Optional[Any] = None,
        *,
        draft_json: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        session_state = QuerySessionService._normalize_state(state)
        explicit_confirmed_draft = QuerySessionService._normalize_state(session_state.get("confirmed_draft"))
        provisional_draft = QuerySessionService._normalize_state(session_state.get("provisional_draft"))
        ir_snapshot = QuerySessionService._normalize_state(session_state.get("ir_snapshot"))

        if explicit_confirmed_draft:
            base_draft = explicit_confirmed_draft
        else:
            provisional_confirmed = bool(provisional_draft.get("confirmed")) or str(
                provisional_draft.get("status") or ""
            ) == "confirmed"
            if not provisional_confirmed:
                return None
            base_draft = provisional_draft

        resolved_draft_json = (
            draft_json
            if draft_json is not None
            else base_draft.get("draft_json") or base_draft.get("ir") or ir_snapshot or None
        )
        resolved_natural_language = base_draft.get("natural_language")
        resolved_warnings = list(base_draft.get("warnings") or [])
        resolved_suggestions = list(base_draft.get("suggestions") or [])
        resolved_confidence = base_draft.get("confidence")
        resolved_ambiguities = list(base_draft.get("ambiguities") or [])
        resolved_open_points = list(base_draft.get("open_points") or [])
        resolved_selected_table_names = list(base_draft.get("selected_table_names") or [])

        if not any([
            resolved_natural_language,
            resolved_draft_json,
            resolved_warnings,
            resolved_suggestions,
            resolved_ambiguities,
            resolved_open_points,
        ]):
            return None

        return QuerySessionService.build_confirmed_draft_state(
            base_draft,
            natural_language=resolved_natural_language,
            draft_json=resolved_draft_json,
            warnings=resolved_warnings,
            suggestions=resolved_suggestions,
            confidence=resolved_confidence,
            ambiguities=resolved_ambiguities,
            open_points=resolved_open_points,
            selected_table_names=resolved_selected_table_names,
        )

    @staticmethod
    def build_execution_guard_state(
        payload: Optional[Any] = None,
        *,
        status: Optional[str] = None,
        natural_language: Optional[str] = None,
        warnings: Optional[list[Any]] = None,
        estimated_cost: Optional[Dict[str, Any]] = None,
        ir: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        guard_state = QuerySessionService._normalize_confirmation_state(payload)
        return sanitize_for_json(
            {
                "status": status or guard_state.get("status"),
                "natural_language": natural_language or guard_state.get("natural_language"),
                "warnings": list(warnings if warnings is not None else guard_state.get("warnings") or []),
                "estimated_cost": estimated_cost if estimated_cost is not None else guard_state.get("estimated_cost"),
                "ir": ir if ir is not None else guard_state.get("ir"),
            }
        )

    @staticmethod
    def _get_selected_table_ids(state: Dict[str, Any]) -> list[str]:
        table_resolution_state = QuerySessionService._normalize_confirmation_state(state.get("table_resolution_state"))
        selected_table_ids = list(state.get("selected_table_ids") or table_resolution_state.get("selected_table_ids") or [])
        selected_table_id = state.get("selected_table_id")
        if not selected_table_id and table_resolution_state.get("selected_table_ids"):
            selected_table_id = list(table_resolution_state.get("selected_table_ids") or [None])[0]
        if not selected_table_ids and selected_table_id:
            selected_table_ids = [selected_table_id]
        return selected_table_ids

    @staticmethod
    def _map_pending_action(current_node: Optional[str], action_type: str) -> str:
        if action_type == "confirm":
            if current_node == "table_resolution":
                return "choose_table"
            if current_node == "draft_confirmation":
                return "confirm_draft"
        if action_type == "execution_decision":
            return "approve_execution"
        if action_type == "exit_current":
            return "cancel_query"
        return action_type

    @staticmethod
    def _resolve_selected_table_names(state: Dict[str, Any]) -> list[str]:
        draft_sources = [
            QuerySessionService._normalize_state(state.get("provisional_draft")),
            QuerySessionService._normalize_state(state.get("confirmed_draft")),
        ]
        for draft_state in draft_sources:
            selected_table_names = QuerySessionService._sanitize_table_names(draft_state.get("selected_table_names") or [])
            if selected_table_names:
                return selected_table_names

        table_resolution_state = QuerySessionService._normalize_confirmation_state(state.get("table_resolution_state"))
        selected_table_ids = set(QuerySessionService._get_selected_table_ids(state))
        resolved_names: list[str] = []
        seen_names: set[str] = set()
        for candidate in table_resolution_state.get("candidates") or []:
            table_id = candidate.get("table_id")
            table_name = str(candidate.get("table_name") or "").strip()
            if not table_name or table_name in seen_names:
                continue
            if QuerySessionService._looks_like_uuid(table_name):
                continue
            if selected_table_ids and table_id not in selected_table_ids:
                continue
            seen_names.add(table_name)
            resolved_names.append(table_name)
        return resolved_names

    @staticmethod
    def _build_safe_summary(current_node: Optional[str], state: Dict[str, Any]) -> Dict[str, Any]:
        safe_summary = QuerySessionService._normalize_state(state.get("safe_summary"))
        draft = QuerySessionService._normalize_state(state.get("provisional_draft")) or QuerySessionService._normalize_state(
            state.get("confirmed_draft")
        )
        open_points = QuerySessionService._sanitize_summary_items(safe_summary.get("open_points") or [])
        if not open_points:
            if current_node == "table_resolution":
                open_points = ["需确认应使用哪张数据表"]
            elif current_node == "draft_confirmation":
                open_points = QuerySessionService._sanitize_summary_items(
                    draft.get("open_points") or draft.get("ambiguities") or []
                )
                if not open_points:
                    open_points = ["需确认当前查询草稿是否符合预期"]
            elif current_node == "execution_guard":
                open_points = ["需确认是否执行当前查询"]

        known_constraints = QuerySessionService._sanitize_summary_items(safe_summary.get("known_constraints") or [])
        if not known_constraints:
            selected_table_names = QuerySessionService._resolve_selected_table_names(state)
            if selected_table_names:
                known_constraints.append(f"当前数据表：{'、'.join(selected_table_names)}")

            analysis_context = QuerySessionService._normalize_state(state.get("analysis_context"))
            scope_summary = str(analysis_context.get("scope_summary") or "").strip()
            if scope_summary and not QuerySessionService._looks_like_uuid(scope_summary):
                known_constraints.append(f"承接上一结果：{scope_summary}")

            confidence = draft.get("confidence")
            if confidence is not None:
                try:
                    known_constraints.append(f"当前理解置信度：{round(float(confidence) * 100)}%")
                except (TypeError, ValueError):
                    pass

        return {
            "user_goal_summary": (
                safe_summary.get("user_goal_summary")
                or state.get("resolved_question_text")
                or state.get("question_text")
            ),
            "domain_hint": (
                safe_summary.get("domain_hint")
                if not QuerySessionService._looks_like_uuid(safe_summary.get("domain_hint"))
                else None
            )
            or state.get("domain_name")
            or (None if QuerySessionService._looks_like_uuid(state.get("domain_id")) else state.get("domain_id")),
            "known_constraints": known_constraints,
            "open_points": open_points,
        }

    @staticmethod
    def _looks_like_uuid(value: Optional[Any]) -> bool:
        if value in (None, ""):
            return False
        try:
            UUID(str(value))
            return True
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _build_table_resolution(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        table_resolution_state = QuerySessionService._normalize_confirmation_state(state.get("table_resolution_state"))
        selected_table_ids = QuerySessionService._get_selected_table_ids(state)
        recommended_table_ids = list(
            state.get("recommended_table_ids")
            or table_resolution_state.get("recommended_table_ids")
            or []
        )
        rejected_table_ids = list(state.get("rejected_table_ids") or table_resolution_state.get("rejected_table_ids") or [])
        candidates = list(table_resolution_state.get("candidates") or [])

        if not any([table_resolution_state, selected_table_ids, recommended_table_ids, rejected_table_ids]):
            return None

        status = "idle"
        if current_node == "table_resolution":
            status = "awaiting_confirmation"
        elif selected_table_ids:
            status = "confirmed"

        return {
            "status": status,
            "question": table_resolution_state.get("question") or state.get("question_text"),
            "message": table_resolution_state.get("message"),
            "reason_summary": table_resolution_state.get("reason_summary"),
            "candidates": candidates,
            "recommended_table_ids": recommended_table_ids,
            "selected_table_ids": selected_table_ids,
            "rejected_table_ids": rejected_table_ids,
            "allow_multi_select": bool(table_resolution_state.get("allow_multi_select")),
            "multi_table_mode": (
                state.get("multi_table_mode")
                or table_resolution_state.get("multi_table_mode")
            ),
            "manual_table_override": bool(
                state.get("manual_table_override")
                if state.get("manual_table_override") is not None
                else table_resolution_state.get("manual_table_override")
            ),
        }

    @staticmethod
    def _build_table_resolution_provisional_draft(
        current_node: Optional[str],
        state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if current_node != "table_resolution":
            return None

        table_resolution_state = QuerySessionService._normalize_confirmation_state(state.get("table_resolution_state"))
        if not table_resolution_state:
            return None

        return QuerySessionService.build_provisional_draft_state(
            table_resolution_state,
            question_text=(
                state.get("resolved_question_text")
                or state.get("question_text")
                or table_resolution_state.get("question")
            ),
            selected_table_ids=QuerySessionService._get_selected_table_ids(state),
            recommended_table_ids=list(
                state.get("recommended_table_ids")
                or table_resolution_state.get("recommended_table_ids")
                or []
            ),
            manual_table_override=(
                state.get("manual_table_override")
                if state.get("manual_table_override") is not None
                else table_resolution_state.get("manual_table_override")
            ),
            allow_multi_select=table_resolution_state.get("allow_multi_select"),
        )

    @staticmethod
    def _build_draft(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        provisional_draft = QuerySessionService._normalize_state(state.get("provisional_draft"))
        confirmed_draft = QuerySessionService._normalize_state(state.get("confirmed_draft"))
        table_resolution_provisional_draft = QuerySessionService._build_table_resolution_provisional_draft(
            current_node,
            state,
        )

        if not any([
            provisional_draft,
            confirmed_draft,
            table_resolution_provisional_draft,
        ]):
            return None

        active_draft_state: Dict[str, Any]
        if provisional_draft:
            status = provisional_draft.get("status") or "provisional"
            natural_language = provisional_draft.get("natural_language")
            draft_json = provisional_draft.get("draft_json")
            warnings = list(provisional_draft.get("warnings") or [])
            suggestions = list(provisional_draft.get("suggestions") or [])
            confidence = provisional_draft.get("confidence")
            ambiguities = list(provisional_draft.get("ambiguities") or [])
            open_points = list(provisional_draft.get("open_points") or [])
            selected_table_names = QuerySessionService._sanitize_table_names(
                provisional_draft.get("selected_table_names") or []
            )
            active_draft_state = provisional_draft
        elif confirmed_draft:
            status = confirmed_draft.get("status") or "confirmed"
            natural_language = confirmed_draft.get("natural_language")
            draft_json = confirmed_draft.get("draft_json")
            warnings = list(confirmed_draft.get("warnings") or [])
            suggestions = list(confirmed_draft.get("suggestions") or [])
            confidence = confirmed_draft.get("confidence")
            ambiguities = list(confirmed_draft.get("ambiguities") or [])
            open_points = list(confirmed_draft.get("open_points") or [])
            selected_table_names = QuerySessionService._sanitize_table_names(
                confirmed_draft.get("selected_table_names") or []
            )
            active_draft_state = confirmed_draft
        elif table_resolution_provisional_draft:
            status = table_resolution_provisional_draft.get("status") or "provisional"
            natural_language = table_resolution_provisional_draft.get("natural_language")
            draft_json = table_resolution_provisional_draft.get("draft_json")
            warnings = list(table_resolution_provisional_draft.get("warnings") or [])
            suggestions = list(table_resolution_provisional_draft.get("suggestions") or [])
            confidence = table_resolution_provisional_draft.get("confidence")
            ambiguities = list(table_resolution_provisional_draft.get("ambiguities") or [])
            open_points = list(table_resolution_provisional_draft.get("open_points") or [])
            selected_table_names = QuerySessionService._sanitize_table_names(
                table_resolution_provisional_draft.get("selected_table_names") or []
            )
            active_draft_state = table_resolution_provisional_draft
        else:
            return None

        confirmed = active_draft_state.get("confirmed")
        confirmation_required = active_draft_state.get("confirmation_required")

        table_dependent = active_draft_state.get("table_dependent")
        if table_dependent is None:
            table_dependent = True

        invalidate_on_table_change = active_draft_state.get("invalidate_on_table_change")
        if invalidate_on_table_change is None:
            invalidate_on_table_change = True

        return {
            "status": status,
            "table_dependent": bool(table_dependent),
            "invalidate_on_table_change": bool(invalidate_on_table_change),
            "natural_language": natural_language,
            "draft_json": draft_json,
            "warnings": warnings,
            "suggestions": suggestions,
            "confidence": confidence,
            "ambiguities": ambiguities,
            "open_points": open_points,
            "selected_table_names": selected_table_names,
            "confirmed": bool(confirmed),
            "confirmation_required": bool(confirmation_required),
        }

    @staticmethod
    def _build_execution_guard(current_node: Optional[str], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        execution_guard_state = QuerySessionService._normalize_confirmation_state(state.get("execution_guard_state"))
        execution_guard = QuerySessionService._normalize_state(state.get("execution_guard"))
        derived_execution_guard = execution_guard_state or QuerySessionService.build_execution_guard_state(execution_guard)
        if not derived_execution_guard and current_node != "execution_guard":
            return None

        status = derived_execution_guard.get("status") or "available"
        if current_node == "execution_guard":
            status = "awaiting_confirmation"
        elif state.get("execution_decision") == "approve":
            status = "approved"

        return {
            "status": status,
            "natural_language": derived_execution_guard.get("natural_language"),
            "warnings": list(derived_execution_guard.get("warnings") or []),
            "estimated_cost": derived_execution_guard.get("estimated_cost"),
            "ir": derived_execution_guard.get("ir") or state.get("ir_snapshot"),
        }

    @staticmethod
    def _build_result_actions(
        current_node: Optional[str],
        raw_pending_actions: list[str],
    ) -> Optional[Dict[str, Any]]:
        if current_node not in RESULT_ACTION_NODES:
            return None

        action_bindings = {action_type: action_type for action_type in raw_pending_actions}
        return {
            "source_node": current_node,
            "available_actions": list(raw_pending_actions),
            "action_bindings": dict(action_bindings),
            "raw_pending_actions": list(raw_pending_actions),
        }

    @staticmethod
    def build_confirmation_view(session: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not session:
            return None

        current_node = session.get("current_node")
        state = QuerySessionService._normalize_state(session.get("state_json"))
        raw_pending_actions = list(state.get("pending_actions") or [])
        pending_actions = []
        action_bindings: Dict[str, str] = {}
        for action_type in raw_pending_actions:
            semantic_action = QuerySessionService._map_pending_action(current_node, action_type)
            pending_actions.append(semantic_action)
            action_bindings.setdefault(semantic_action, action_type)

        selected_table_ids = QuerySessionService._get_selected_table_ids(state)
        draft = QuerySessionService._build_draft(current_node, state)
        return {
            "query_id": session.get("query_id"),
            "session": {
                "status": session.get("status"),
                "current_node": current_node,
            },
            "context": {
                "question_text": state.get("question_text") or state.get("resolved_question_text"),
                "safe_summary": QuerySessionService._build_safe_summary(current_node, state),
            },
            "table_resolution": QuerySessionService._build_table_resolution(current_node, state),
            "draft": draft,
            "execution_guard": QuerySessionService._build_execution_guard(current_node, state),
            "pending_actions": pending_actions,
            "result_actions": QuerySessionService._build_result_actions(
                current_node,
                raw_pending_actions,
            ),
            "dependency_meta": {
                "draft_version": state.get("draft_version"),
                "selected_table_ids": selected_table_ids,
                "invalidated_artifacts": list(state.get("invalidated_artifacts") or []),
                "raw_pending_actions": raw_pending_actions,
                "action_bindings": action_bindings,
                "analysis_context": state.get("analysis_context"),
                "invalidate_on_table_change": bool(
                    draft
                    or state.get("ir_snapshot")
                    or state.get("sql_preview")
                    or state.get("result_meta")
                ),
            },
        }

    @staticmethod
    def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        session = {
            "query_id": str(row["query_id"]),
            "conversation_id": str(row["conversation_id"]) if row["conversation_id"] else None,
            "message_id": str(row["message_id"]) if row["message_id"] else None,
            "user_id": str(row["user_id"]) if row["user_id"] else None,
            "status": row["status"],
            "current_node": row["current_node"],
            "state_json": QuerySessionService._normalize_state(row["state_json"]),
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        session["confirmation_view"] = QuerySessionService.build_confirmation_view(session)
        return session

    async def get_session(self, query_id: UUID) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT query_id, conversation_id, message_id, user_id, status, current_node,
                       state_json, last_error, created_at, updated_at
                FROM query_sessions
                WHERE query_id = $1
                """,
                query_id,
            )
        return self._row_to_dict(row)

    async def get_latest_pending_session_for_conversation(self, conversation_id: UUID) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT query_id, conversation_id, message_id, user_id, status, current_node,
                       state_json, last_error, created_at, updated_at
                FROM query_sessions
                WHERE conversation_id = $1
                  AND status = 'awaiting_user_action'
                  AND current_node = ANY($2::text[])
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                conversation_id,
                list(PENDING_USER_ACTION_NODES),
            )
        return self._row_to_dict(row)

    async def upsert_session(
        self,
        *,
        query_id: UUID,
        user_id: Optional[UUID],
        status: str,
        current_node: str,
        state_json: Optional[Dict[str, Any]] = None,
        conversation_id: Optional[UUID] = None,
        message_id: Optional[UUID] = None,
        last_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        async with self._acquire() as conn:
            existing = await conn.fetchrow(
                """
                SELECT query_id, conversation_id, message_id, user_id, status, current_node,
                       state_json, last_error, created_at, updated_at
                FROM query_sessions
                WHERE query_id = $1
                """,
                query_id,
            )

            merged_state = self._merge_state(existing["state_json"] if existing else None, state_json)
            row = await conn.fetchrow(
                """
                INSERT INTO query_sessions (
                    query_id, conversation_id, message_id, user_id,
                    status, current_node, state_json, last_error
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                ON CONFLICT (query_id) DO UPDATE SET
                    conversation_id = COALESCE(EXCLUDED.conversation_id, query_sessions.conversation_id),
                    message_id = COALESCE(EXCLUDED.message_id, query_sessions.message_id),
                    user_id = COALESCE(EXCLUDED.user_id, query_sessions.user_id),
                    status = EXCLUDED.status,
                    current_node = EXCLUDED.current_node,
                    state_json = EXCLUDED.state_json,
                    last_error = EXCLUDED.last_error,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING query_id, conversation_id, message_id, user_id, status, current_node,
                          state_json, last_error, created_at, updated_at
                """,
                query_id,
                conversation_id,
                message_id,
                user_id,
                status,
                current_node,
                self._dump_state(merged_state),
                last_error,
            )

        logger.debug("查询会话已写入", query_id=str(query_id), status=status, current_node=current_node)
        return self._row_to_dict(row) or {}

    async def update_session(
        self,
        query_id: UUID,
        *,
        status: Optional[str] = None,
        current_node: Optional[str] = None,
        state_updates: Optional[Dict[str, Any]] = None,
        conversation_id: Optional[UUID] = None,
        message_id: Optional[UUID] = None,
        last_error: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        async with self._acquire() as conn:
            existing = await conn.fetchrow(
                """
                SELECT query_id, conversation_id, message_id, user_id, status, current_node,
                       state_json, last_error, created_at, updated_at
                FROM query_sessions
                WHERE query_id = $1
                """,
                query_id,
            )
            if not existing:
                return None

            row = await conn.fetchrow(
                """
                UPDATE query_sessions
                SET conversation_id = COALESCE($2, conversation_id),
                    message_id = COALESCE($3, message_id),
                    status = COALESCE($4, status),
                    current_node = COALESCE($5, current_node),
                    state_json = $6::jsonb,
                    last_error = $7,
                    updated_at = CURRENT_TIMESTAMP
                WHERE query_id = $1
                RETURNING query_id, conversation_id, message_id, user_id, status, current_node,
                          state_json, last_error, created_at, updated_at
                """,
                query_id,
                conversation_id,
                message_id,
                status,
                current_node,
                self._dump_state(self._merge_state(existing["state_json"], state_updates)),
                last_error,
            )

        logger.debug(
            "查询会话已更新",
            query_id=str(query_id),
            status=status or existing["status"],
            current_node=current_node or existing["current_node"],
        )
        return self._row_to_dict(row)
