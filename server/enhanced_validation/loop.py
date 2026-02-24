"""
增强型 SQL 验证回路
提供实体对齐（Schema Linking）和 SQL Dry Run 能力
"""

from typing import Optional
import structlog
from sqlalchemy import text

from server.config import settings
from server.models.ir import IntermediateRepresentation

logger = structlog.get_logger()


class EntityLinker:
    """占位版实体对齐器，后续可接入真实 Schema Linking"""

    async def link(
        self,
        ir: IntermediateRepresentation,
        connection_id: str
    ) -> IntermediateRepresentation:
        if ir is None:
            return ir

        # 这里预留扩展点：可根据 connection_id 查询实体映射关系
        linked_ir = ir.model_copy(deep=True)
        logger.debug(
            "实体对齐完成（占位）",
            connection_id=connection_id,
            filters=len(linked_ir.filters)
        )
        return linked_ir


class ExecutionValidator:
    """SQL Dry Run Validator"""

    async def dry_run(
        self,
        sql: str,
        connection_id: str,
        db_type: str,
        query_executor
    ) -> bool:
        try:
            engine = await query_executor.ensure_engine()
            async with engine.connect() as conn:
                if db_type == "sqlserver":
                    # SQL Server SHOWPLAN 模式不启动真正的事务，需要禁用 SQLAlchemy 的自动事务管理
                    # 否则退出时 rollback 会报错: "ROLLBACK TRANSACTION has no corresponding BEGIN TRANSACTION"
                    raw_conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
                    try:
                        await raw_conn.execute(text("SET SHOWPLAN_XML ON"))
                        await raw_conn.execute(text(sql))
                    finally:
                        await raw_conn.execute(text("SET SHOWPLAN_XML OFF"))
                else:
                    explain_sql = self._build_explain_sql(db_type, sql)
                    await conn.execute(text(explain_sql))

            logger.debug("Dry Run 成功", connection_id=connection_id)
            return True

        except Exception as e:
            logger.warning(
                "SQL Dry Run 失败",
                error=str(e),
                connection_id=connection_id
            )
            return False

    def _build_explain_sql(self, db_type: str, sql: str) -> str:
        sql_clean = sql.rstrip(" ;")
        if db_type == "mysql":
            return f"EXPLAIN {sql_clean}"
        # PostgreSQL 以及默认走 EXPLAIN
        return f"EXPLAIN {sql_clean}"


class EnhancedValidationLoop:
    """组合实体对齐与 Dry Run 的验证回路"""

    def __init__(self):
        self.entity_linker = EntityLinker()
        self.execution_validator = ExecutionValidator()

    async def align_ir(
        self,
        ir: IntermediateRepresentation,
        connection_id: str
    ) -> IntermediateRepresentation:
        if not settings.enhanced_validation_enabled:
            return ir
        return await self.entity_linker.link(ir, connection_id)

    async def dry_run(
        self,
        sql: str,
        connection_id: str,
        db_type: str,
        query_executor
    ) -> bool:
        if not settings.enhanced_validation_enabled:
            return True
        return await self.execution_validator.dry_run(
            sql,
            connection_id,
            db_type,
            query_executor
        )


_loop = EnhancedValidationLoop()


def get_validation_loop() -> EnhancedValidationLoop:
    return _loop

