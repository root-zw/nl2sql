"""
Few-Shot SQL 问答样本治理服务

提供黄金样本的导入、校验与辅助清洗能力，确保写入 Milvus 之前的数据质量。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Sequence, Any
from uuid import UUID

import asyncpg
import structlog
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from server.config import settings, RetrievalConfig
from server.security.sql_validator import SQLSecurityValidator

logger = structlog.get_logger()


@dataclass
class FewShotSampleInput:
    """用于导入的Few-Shot样本结构"""

    question: str
    sql_text: str
    ir_json: Optional[str] = None
    domain_id: Optional[UUID] = None
    tables: Optional[List[str]] = None
    tables_json: Optional[List[Any]] = None
    quality_score: Optional[float] = None
    source_tag: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    last_verified_at: Optional[datetime] = None
    is_active: bool = True


class FewShotDatasetService:
    """
    Few-Shot样本治理服务

    功能：
    1. SQL安全校验
    2. 自动解析 FROM/JOIN 表
    3. 写入 qa_few_shot_samples 表并触发增量同步
    """

    def __init__(self, db_pool: Optional[asyncpg.Pool] = None):
        self._db_pool = db_pool
        self._sql_validator = SQLSecurityValidator()

    async def _get_pool(self) -> asyncpg.Pool:
        if self._db_pool:
            return self._db_pool
        from server.utils.db_pool import get_metadata_pool

        self._db_pool = await get_metadata_pool()
        return self._db_pool

    async def upsert_samples(
        self,
        connection_id: UUID,
        samples: Sequence[FewShotSampleInput],
        *,
        dialect: Optional[str] = None,
    ) -> List[UUID]:
        """
        批量导入/更新 Few-Shot 样本。

        Args:
            connection_id: 数据库连接ID
            samples: 样本列表
            dialect: 可选SQL方言（默认为None，让sqlglot自动推断）
        """
        if not samples:
            return []

        pool = await self._get_pool()
        inserted_ids: List[UUID] = []

        async with pool.acquire() as conn:
            async with conn.transaction():
                for sample in samples:
                    inserted_id = await self._upsert_single_sample(
                        conn,
                        connection_id,
                        sample,
                        dialect=dialect,
                    )
                    inserted_ids.append(inserted_id)

        logger.info(
            "Few-Shot样本导入完成",
            connection_id=str(connection_id),
            count=len(inserted_ids),
        )
        return inserted_ids

    async def _upsert_single_sample(
        self,
        conn: asyncpg.Connection,
        connection_id: UUID,
        sample: FewShotSampleInput,
        *,
        dialect: Optional[str],
    ) -> UUID:
        """写入单条样本"""
        self._sql_validator.validate_sql_security(sample.sql_text)

        raw_tables = sample.tables or self._extract_tables(sample.sql_text, dialect)
        tables = self._normalize_tables(raw_tables)

        if not tables:
            raise ValueError("无法从SQL解析出任何表名，请显式提供 tables 参数。")

        tables_json_payload = sample.tables_json if sample.tables_json is not None else tables
        tables_json = json.dumps(tables_json_payload, ensure_ascii=False)
        metadata_json = json.dumps(sample.metadata or {}, ensure_ascii=False)
        quality_score = self._normalize_quality_score(sample.quality_score)
        domain_id = sample.domain_id or await self._infer_domain_id(conn, connection_id, tables)

        inserted_id = await conn.fetchval(
            """
            INSERT INTO qa_few_shot_samples (
                connection_id,
                question,
                sql_text,
                ir_json,
                tables,
                tables_json,
                domain_id,
                quality_score,
                source_tag,
                metadata,
                last_verified_at,
                is_active
            )
            VALUES ($1, $2, $3, $4::jsonb, $5::text[], $6::jsonb, $7, $8, $9, $10::jsonb, $11, $12)
            ON CONFLICT (connection_id, question, sql_text)
            DO UPDATE SET
                sql_text = EXCLUDED.sql_text,
                ir_json = EXCLUDED.ir_json,
                tables = EXCLUDED.tables,
                tables_json = EXCLUDED.tables_json,
                domain_id = COALESCE(EXCLUDED.domain_id, qa_few_shot_samples.domain_id),
                quality_score = EXCLUDED.quality_score,
                source_tag = COALESCE(EXCLUDED.source_tag, qa_few_shot_samples.source_tag),
                metadata = COALESCE(EXCLUDED.metadata, qa_few_shot_samples.metadata),
                last_verified_at = COALESCE(EXCLUDED.last_verified_at, qa_few_shot_samples.last_verified_at),
                is_active = EXCLUDED.is_active,
                updated_at = CURRENT_TIMESTAMP
            RETURNING sample_id;
            """,
            connection_id,
            sample.question.strip(),
            sample.sql_text.strip(),
            sample.ir_json,  # 新增 ir_json 参数
            tables,
            tables_json,
            domain_id,
            quality_score,
            sample.source_tag,
            metadata_json,
            sample.last_verified_at,
            sample.is_active,
        )

        return inserted_id

    async def _infer_domain_id(
        self,
        conn: asyncpg.Connection,
        connection_id: UUID,
        tables: List[str],
    ) -> Optional[UUID]:
        """根据首个表推断业务域"""
        if not tables:
            return None

        base_table = tables[0].split(".")[-1]
        row = await conn.fetchrow(
            """
            SELECT domain_id
            FROM db_tables
            WHERE connection_id = $1
              AND LOWER(table_name) = LOWER($2)
            ORDER BY updated_at DESC
            LIMIT 1;
            """,
            connection_id,
            base_table,
        )
        return row["domain_id"] if row and row["domain_id"] else None

    def _extract_tables(self, sql_text: str, dialect: Optional[str]) -> List[str]:
        """使用sqlglot解析SQL中的表"""
        try:
            expression = sqlglot.parse_one(sql_text, read=dialect) if dialect else sqlglot.parse_one(sql_text)
        except ParseError as err:
            logger.warning("sqlglot解析失败，尝试使用默认解析器", error=str(err))
            expression = sqlglot.parse_one(sql_text)

        tables: List[str] = []
        if not expression:
            return tables

        for table in expression.find_all(exp.Table):
            parts = [part for part in [table.catalog, table.db, table.name] if part]
            if not parts and hasattr(table, "this") and table.this:
                parts = [table.this]
            identifier = ".".join(parts)
            if identifier:
                tables.append(identifier)

        return tables

    def _normalize_tables(self, tables: List[str]) -> List[str]:
        """去重、清洗表名"""
        normalized: List[str] = []
        seen = set()
        for table in tables:
            clean = table.strip().strip('"')
            if not clean:
                continue
            lower = clean.lower()
            if lower in seen:
                continue
            seen.add(lower)
            normalized.append(clean)
        return normalized

    def _normalize_quality_score(self, score: Optional[float]) -> float:
        base = score if score is not None else RetrievalConfig.few_shot_min_quality_score()
        return max(0.0, min(1.0, float(base)))

