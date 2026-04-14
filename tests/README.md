# Test Layout

- `tests/unit/`: 纯单元与快速回归测试，不依赖真实外部服务。
- `tests/schema/`: 元数据库初始化 SQL、Alembic 迁移、Schema 基线相关测试。
- `tests/integration/`: 需要真实数据库或外部依赖的集成测试，默认可跳过。

常用命令：

```bash
conda run -n nl2sql-py312 pytest -q tests/unit
conda run -n nl2sql-py312 pytest -q tests/schema
conda run -n nl2sql-py312 pytest -q -m integration
```
