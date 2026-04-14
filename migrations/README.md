# Metadata DB Migrations

当前仓库已经接入 Alembic，但元数据库仍以手写 SQL 为主，不使用 ORM 自动生成迁移。

## 常用命令

```bash
conda run -n nl2sql-py312 alembic -c alembic.ini upgrade head
conda run -n nl2sql-py312 alembic -c alembic.ini current
conda run -n nl2sql-py312 alembic -c alembic.ini history
conda run -n nl2sql-py312 alembic -c alembic.ini revision -m "新增xxx"
```

## 基线说明

- `20260414_0001` 是基于 `docker/init-scripts/init_database_complete.sql` 收编出来的基线迁移。
- 全新空库可直接执行 `upgrade head`。
- 已经通过旧 SQL 初始化过、但还没有 `alembic_version` 的老库，不要直接 `upgrade head`，先执行：

```bash
conda run -n nl2sql-py312 alembic -c alembic.ini stamp 20260414_0001
```

## 编写要求

- 优先使用手写迁移，不要依赖 `--autogenerate`。
- 涉及元数据库结构变更时，同时更新：
  - 对应 Alembic revision
  - 如仍需支持 Docker 首次初始化，则同步评估 `docker/init-scripts/`
  - 对应 schema / regression tests
