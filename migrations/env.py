from __future__ import annotations

import os
from logging.config import fileConfig
from pathlib import Path
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_local_env() -> None:
    """加载仓库根目录的 .env，避免迁移命令依赖外部 shell export。"""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def build_database_url() -> str:
    override = os.getenv("ALEMBIC_DATABASE_URL")
    if override:
        return override

    load_local_env()

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "25432")
    database = os.getenv("POSTGRES_DB", "text2sql_metadata")
    user = quote_plus(os.getenv("POSTGRES_USER", "text2sql_user"))
    password = quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"


def get_config_section() -> dict:
    section = config.get_section(config.config_ini_section, {}) or {}
    section["sqlalchemy.url"] = build_database_url()
    return section


def run_migrations_offline() -> None:
    """以 offline 模式运行迁移。"""
    url = build_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """以 asyncpg online 模式运行迁移。"""
    connectable = async_engine_from_config(
        get_config_section(),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    import asyncio

    asyncio.run(run_migrations_online())
