"""
Few-Shot 问答沉淀工具
"""

from __future__ import annotations

import asyncio
from typing import List, Optional, Dict, Any, Tuple
from uuid import UUID

import asyncpg

import structlog

from server.nl2ir.few_shot_dataset_service import FewShotDatasetService, FewShotSampleInput
from server.models.ir import IntermediateRepresentation
from server.config import settings, RetrievalConfig
from server.models.sync import EntityType
from server.sync.auto_sync_policy import manual_sync_allowed
from server.utils.timezone_helper import now_with_tz

logger = structlog.get_logger()


class FewShotWriter:
    """将成功的 NL → SQL 问答写入 qa_few_shot_samples 表。"""

    def __init__(self, db_pool=None):
        self._service = FewShotDatasetService(db_pool=db_pool)

    async def record_successful_query(
        self,
        *,
        connection_id: str,
        question: Optional[str],
        sql_text: Optional[str],
        retrieval_summary: Optional[Dict[str, Any]],
        domain_id: Optional[str],
        confidence: float,
        ir: Optional[IntermediateRepresentation] = None,
        source_tag: str = "auto",
    ) -> None:
        """将问答对写入 Few-Shot 表，供后续检索使用。"""
        if not RetrievalConfig.few_shot_switch_enabled():
            return

        if not question or not sql_text:
            return

        tables_json, table_tokens = self._prepare_table_metadata(ir, retrieval_summary)
        metadata = {
            "confidence": round(confidence, 4),
            "source": "query_api",
            "table_token_count": len(table_tokens),
            "retrieval_table_count": len(tables_json),
        }
        if tables_json:
            metadata["sampled_tables"] = [
                {
                    "table_id": entry.get("table_id"),
                    "display_name": entry.get("display_name"),
                    "rank": entry.get("rank"),
                    "score": entry.get("score")
                }
                for entry in tables_json[: min(3, len(tables_json))]
            ]

        # 将 IR 序列化为 JSON 字符串
        ir_json_str = None
        if ir:
            try:
                ir_json_str = ir.model_dump_json(exclude_none=True, ensure_ascii=False)
            except Exception as e:
                logger.warning("IR序列化失败，将跳过ir_json字段", error=str(e))

        sample = FewShotSampleInput(
            question=question.strip(),
            sql_text=sql_text.strip(),
            ir_json=ir_json_str,
            tables=table_tokens or None,
            tables_json=tables_json or None,
            domain_id=domain_id or getattr(ir, "domain_id", None),
            quality_score=self._normalize_quality(confidence),
            source_tag=source_tag,
            metadata=metadata,
            last_verified_at=now_with_tz(),
            is_active=True,
        )

        try:
            await self._service.upsert_samples(connection_id, [sample])
            logger.debug(
                "Few-Shot示例已写入",
                connection_id=connection_id,
                table_count=len(tables_json),
                token_count=len(table_tokens),
                confidence=confidence,
            )
            if (
                tables_json
                and RetrievalConfig.few_shot_immediate_sync()
                and manual_sync_allowed(EntityType.FEW_SHOT)
            ):
                asyncio.create_task(self._trigger_immediate_sync(connection_id))
        except Exception as exc:
            logger.warning(
                "Few-Shot示例写入失败，已跳过",
                connection_id=connection_id,
                error=str(exc),
            )

    def _prepare_table_metadata(
        self,
        ir: Optional[IntermediateRepresentation],
        retrieval_summary: Optional[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """基于IR与检索摘要生成结构化表信息与过滤token。"""
        if not retrieval_summary:
            return [], []

        structures = retrieval_summary.get("table_structures") or []
        ranked_tables = retrieval_summary.get("tables") or []
        rank_map = {
            tbl.get("table_id"): tbl
            for tbl in ranked_tables
            if tbl.get("table_id")
        }

        def _ordered_table_ids() -> List[str]:
            ordered: List[str] = []
            primary_id = getattr(ir, "primary_table_id", None) if ir else None
            if primary_id:
                ordered.append(primary_id)
            for table in ranked_tables:
                table_id = table.get("table_id")
                if table_id and table_id not in ordered:
                    ordered.append(table_id)
            for struct in structures:
                table_id = struct.get("table_id")
                if table_id and table_id not in ordered:
                    ordered.append(table_id)
            return ordered

        structure_map = {
            struct.get("table_id"): struct
            for struct in structures
            if struct.get("table_id")
        }

        tables_json: List[Dict[str, Any]] = []
        global_tokens: List[str] = []
        seen_tokens: set[str] = set()
        max_global_tokens = 48

        for rank, table_id in enumerate(_ordered_table_ids(), start=1):
            struct = structure_map.get(table_id, {})
            payload = self._format_table_entry(struct, rank, rank_map.get(table_id))
            if not payload:
                continue
            tables_json.append(payload)
            for token in payload.get("tokens", []):
                if token in seen_tokens:
                    continue
                seen_tokens.add(token)
                global_tokens.append(token)
                if len(global_tokens) >= max_global_tokens:
                    break
            if len(global_tokens) >= max_global_tokens:
                break

        return tables_json, global_tokens

    def _format_table_entry(
        self,
        struct_entry: Dict[str, Any],
        rank: int,
        rank_info: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        table_id = struct_entry.get("table_id")
        if not table_id:
            return None

        display_name = struct_entry.get("display_name") or struct_entry.get("table_name")
        table_name = struct_entry.get("table_name")
        physical_name = struct_entry.get("physical_table_name")
        schema_name = struct_entry.get("schema_name")
        aliases = struct_entry.get("aliases") or []
        tags = struct_entry.get("tags") or []

        def _push_token(dest: List[str], token: Optional[str], *, include_lower: bool = True) -> None:
            if not token:
                return
            normalized = str(token).strip()
            if not normalized:
                return
            if normalized not in dest:
                dest.append(normalized)
            lowered = normalized.lower()
            if include_lower and lowered != normalized and lowered not in dest:
                dest.append(lowered)

        tokens: List[str] = []
        _push_token(tokens, table_id, include_lower=False)
        _push_token(tokens, display_name)
        _push_token(tokens, table_name)
        _push_token(tokens, physical_name)
        if schema_name and table_name:
            _push_token(tokens, f"{schema_name}.{table_name}", include_lower=False)
        if schema_name and physical_name and physical_name != table_name:
            _push_token(tokens, f"{schema_name}.{physical_name}", include_lower=False)
        for alias in aliases:
            _push_token(tokens, alias)
        for tag in tags:
            _push_token(tokens, tag)

        max_per_table = 10
        trimmed_tokens = tokens[:max_per_table]

        payload = {
            "table_id": table_id,
            "display_name": display_name,
            "table_name": table_name,
            "schema_name": schema_name,
            "physical_table_name": physical_name,
            "rank": rank,
            "score": rank_info.get("score") if rank_info else None,
            "tokens": trimmed_tokens,
            "aliases": aliases,
            "tags": tags
        }
        return payload

    @staticmethod
    def _normalize_quality(confidence: float) -> float:
        try:
            value = float(confidence)
        except (TypeError, ValueError):
            value = RetrievalConfig.few_shot_min_quality_score()
        return max(0.0, min(1.0, value))

    async def _trigger_immediate_sync(self, connection_id: str) -> None:
        """可选地触发Few-Shot样本的Milvus增量同步。"""
        if not settings.milvus_enabled:
            return

        try:
            conn_uuid = UUID(str(connection_id))
        except ValueError:
            logger.debug("Few-Shot同步跳过：connection_id 不是有效UUID", connection_id=connection_id)
            return

        db_conn: Optional[asyncpg.Connection] = None
        try:
            db_conn = await asyncpg.connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                user=settings.postgres_user,
                password=settings.postgres_password,
                database=settings.postgres_db
            )
            from server.models.admin import MilvusFewShotSyncRequest
            from server.api.admin.milvus import sync_few_shot_samples

            payload = MilvusFewShotSyncRequest(
                min_quality_score=RetrievalConfig.few_shot_min_quality_score(),
                only_verified=False,
                include_inactive=False
            )
            await sync_few_shot_samples(conn_uuid, payload, db_conn, recreate_collection=False)
            logger.debug("已触发Few-Shot即时同步", connection_id=connection_id)
        except Exception as exc:
            logger.warning("Few-Shot即时同步失败", connection_id=connection_id, error=str(exc))
        finally:
            if db_conn and not db_conn.is_closed():
                await db_conn.close()


