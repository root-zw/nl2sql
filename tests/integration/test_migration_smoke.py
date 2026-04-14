import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT_DIR = Path(__file__).resolve().parents[2]


@pytest.mark.integration
def test_alembic_upgrade_head_against_explicit_test_database():
    database_url = os.getenv("NL2SQL_INTEGRATION_DATABASE_URL")
    if not database_url:
        pytest.skip("未设置 NL2SQL_INTEGRATION_DATABASE_URL，跳过真实数据库迁移测试")

    env = os.environ.copy()
    env["ALEMBIC_DATABASE_URL"] = database_url

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "alembic.ini", "upgrade", "head"],
        cwd=ROOT_DIR,
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout

    asyncpg = pytest.importorskip("asyncpg")

    async def verify_tables():
        conn = await asyncpg.connect(database_url)
        try:
            users_exists = await conn.fetchval("SELECT to_regclass('public.users') IS NOT NULL")
            version = await conn.fetchval("SELECT version_num FROM alembic_version")
        finally:
            await conn.close()
        return users_exists, version

    users_exists, version = asyncio.run(verify_tables())

    assert users_exists is True
    assert version == "20260414_0001"
