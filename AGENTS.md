# Repository Guidelines

## Source Of Truth
- 先看代码、脚本和当前机器实际状态，再下结论；不要只依据仓库文档。
- 仓库内部分文档可能已经过期。所有分析、命令和建议都必须能在当前代码、当前脚本或当前机器配置中找到依据。
- 若仓库文档与代码、脚本、service 配置或当前机器实际配置不一致，以代码、脚本和当前机器实际配置为准，并同步修正文档。
- 不确定运行约定时，优先检查 `server/config.py`、当前 `.env`、`docker/docker-compose.yml`、现有测试和当前机器环境。

## Current Repository Layout
- 当前仓库根目录实际存在的核心目录是：`server/`、`frontend/`、`config/`、`docker/`、`migrations/`、`deploy/`、`prompts/`、`tests/`。
- 当前仓库根目录还包含：`README.md`、`requirements.txt`、`env.template`、`.env`。
- 不要假设 `docs/`、`scripts/` 等目录一定存在；如需引用，先检查当前仓库实际结构。
- `server/`: FastAPI 后端。入口是 `server/main.py`；接口在 `server/api/`；核心服务在 `server/services/`；问数主链路在 `server/nl2ir/`、`server/compiler/`、`server/exec/`；通用工具在 `server/utils/`。
- `frontend/`: Vue 3 + Vite 前端。源码在 `frontend/src/`，包含 `api/`、`router/`、`stores/`、`views/` 等。
- `docker/`: 当前实际存在的依赖编排和初始化脚本。重点文件是 `docker/docker-compose.yml`、`docker/init-scripts/init_database_complete.sql`、`docker/init-scripts/999_stamp_alembic_baseline.sql`。
- `migrations/`: 当前正式迁移体系。Alembic 入口在 `alembic.ini` 和 `migrations/env.py`，版本文件在 `migrations/versions/`。
- `tests/`: 当前测试目录。公共初始化在 `tests/conftest.py`，并按 `tests/unit/`、`tests/schema/`、`tests/integration/` 分层。

## Environment And Commands
- 当前机器上的 Python 开发默认环境是 conda 环境 `nl2sql-py312`。
- 已核实当前机器存在该环境：`/home/zhangwei/miniconda3/envs/nl2sql-py312`，版本为 `Python 3.12.13`。
- 执行后端初始化、SQL 脚本维护、测试、启动等命令时，默认先执行 `conda activate nl2sql-py312`，或直接使用 `conda run -n nl2sql-py312 ...`。
- 若后续实际运行环境与此不一致，必须先以当前机器实际配置为准完成核对，再执行命令。
- 后端常用命令优先写成：
  - `conda run -n nl2sql-py312 pip install -r requirements.txt`
  - `conda run -n nl2sql-py312 alembic -c alembic.ini upgrade head`
  - `conda run -n nl2sql-py312 uvicorn server.main:app --reload --host 0.0.0.0 --port 8891`
  - `conda run -n nl2sql-py312 pytest -q tests/unit`
  - `conda run -n nl2sql-py312 pytest -q tests/schema`
- 当前机器执行 `npm config get registry` 返回 `https://registry.npmjs.org/`。
- 当前机器执行 `conda run -n nl2sql-py312 pip config list` 未发现显式 `index-url` 配置；实际安装 Python 依赖时还出现过 pip 缓存目录权限警告。
- 当前代码和 `.env` 已核实 `SERVER_PORT=8891`。
- 依赖栈当前以 `docker/docker-compose.yml` 为准。已核实常用启动命令是：
  - `docker compose -p nl2sql -f docker/docker-compose.yml up -d`
- 当前机器 `.env` 已核实 `REDIS_URL=redis://localhost:26379/0`，且 `docker/docker-compose.yml` 暴露 Redis `26379:6379`。涉及 Redis 排障或命令时，应以这一实际配置为准。
- 当前机器 `.env` 已核实 `POSTGRES_PORT=25432`，且 `docker/docker-compose.yml` 暴露 PostgreSQL `25432:5432`。
- 当前前端发布方式是 `cd frontend && npm run build` 生成 `frontend/dist/`，再由 `server/main.py` 托管；开发态仍可使用 Vite。
- 当前仓库包含 systemd 单元模板：`deploy/systemd/nl2sql-infra.service` 和 `deploy/systemd/nl2sql-backend.service`。
- `deploy/systemd/nl2sql-backend.service` 当前已配置 `ExecStartPre` 自动执行 `alembic -c alembic.ini upgrade head`。

## Schema And Migration Reality
- 当前仓库已经落地 Alembic 迁移体系，入口是 `alembic.ini`，脚本目录是 `migrations/`。
- 当前基线 revision 是 `20260414_0001`，来源于 `docker/init-scripts/init_database_complete.sql`。
- 全新空库默认执行：
  - `conda run -n nl2sql-py312 alembic -c alembic.ini upgrade head`
- 历史库如果已由旧初始化 SQL 建好、但还没有 `alembic_version`，不要直接 `upgrade head`；先执行：
  - `conda run -n nl2sql-py312 alembic -c alembic.ini stamp 20260414_0001`
- Docker 首次建库仍会执行 `docker/init-scripts/`，并通过 `999_stamp_alembic_baseline.sql` 写入基线版本；后续结构演进统一以 Alembic revision 为准。
- 涉及元数据库结构、初始化数据、触发器、视图或默认记录变更时，应同时评估三处是否需要同步：
  - `migrations/versions/` 中的正式迁移
  - `docker/init-scripts/` 中的首次建库基线
  - `tests/schema/` 中的回归测试
- 当前迁移仍以手写 SQL / 手写 revision 为主，不要假设可直接依赖 ORM `autogenerate` 生成准确迁移。

## Development Workflow
- 做功能开发、缺陷修复或行为变更时，先定位实际生效代码路径，再修改；不要按文档猜实现。
- 禁止使用 Git 恢复代码，除非用户明确指定要这样做。
- 不要使用 `git restore`、`git checkout -- <file>`、`git reset --hard`、强制回滚未确认改动等方式清理代码，除非用户明确要求。
- 代码改动完成后要及时 commit。原则是：完成一组可验证、可解释的改动后，尽快提交，不要长期积累大批未提交修改。
- 提交信息延续当前仓库风格，优先使用简短、明确的中文或中英混合描述；可使用日期前缀，例如 `260414 清理历史备份目录`。

## Testing Requirements
- 任何功能开发或行为变更后，必须检查是否存在对应测试。
- 有现成测试就运行，并优先运行与改动直接相关的最小测试集。
- 没有对应测试就补齐，再运行新增测试和相关回归测试。
- Bug 修复必须尽量带回归测试，避免同类问题再次出现。
- 当前项目已在 `requirements.txt` 中声明 `pytest` 与 `pytest-asyncio`，测试命令默认使用 `conda run -n nl2sql-py312 pytest ...`。
- 当前测试已分为 `unit`、`schema`、`integration` 三层；默认优先跑 `unit` 和 `schema`，需要真实数据库时再显式跑 `integration`。
- 改动 `docker/docker-compose.yml`、`deploy/systemd/`、`.env` 关键端口、初始化 SQL 或启动命令时，除了相关 pytest，还应补充实际命令验证，并记录失败点或环境阻塞点。

## Test Organization
- 新建测试脚本时，需符合当前项目的分层测试体系。
- 当前仓库的实际测试形态是：
  - `tests/unit/`: 纯单元与快速回归
  - `tests/schema/`: 初始化 SQL、Alembic、Schema 基线
  - `tests/integration/`: 真实依赖集成测试
  - `tests/conftest.py`: 公共初始化与按目录自动打 marker
- 因此新增测试优先遵循现有模式：
  - 文件命名使用 `test_<feature>.py`
  - 优先按代码模块或变更层级落到对应分层目录
  - 能并入现有测试文件时，优先就近追加
- 不要把一次性调试脚本、人工验证脚本、临时输出文件混入正式测试体系。

## NL2SQL Fix Principles
- 修复智能问数问题时，禁止使用行业偏置或面向单一示例的硬编码规则。
- 例如，不允许把某个行业词直接硬编码映射成某个固定指标、维度、业务语义或 SQL 模板。
- 问数修复应优先采用以下通用方案：
  - 语义模型驱动
  - 通用结构约束解析
  - 发布校验
  - 运行时兜底
- 仅允许保留与行业无关的通用语言结构规则，例如：
  - 时间解析
  - 排序解析
  - 范围解析
  - 对比解析
  - 聚合、TopN、环比同比等通用结构识别
- 若一个修复只能靠行业词硬编码生效，默认认为方案不合格，应继续追溯语义模型、解析链路、校验机制或运行时容错。

## Documentation Maintenance
- 修改代码后，如果 README、AGENTS、初始化说明、部署说明或其他仓库文档与当前实现不一致，应同步更新。
- 文档更新不能凭印象写，必须基于当前代码、当前脚本和当前机器已验证状态。
- 不要把 `.env` 中的敏感信息、密钥或口令直接写进文档。

## Dependency Installation
- 安装依赖前，先看当前机器的源配置和网络连通性，不要默认假设官方源一定可用，也不要默认假设国内镜像已经配置好。
- Python 依赖安装若出现超时、解析失败、拉包失败等网络问题，可优先在命令行显式切换国内镜像，再继续安装；优先使用一次性命令参数，不要未经确认直接改全局 pip 配置。
- Python 依赖安装可参考：
  - `conda run -n nl2sql-py312 pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt`
  - 若仍有缓存权限问题，可再追加 `--no-cache-dir`
- 前端依赖安装若出现网络问题，也先核对当前 `npm` registry；必要时可显式切到国内镜像，例如：
  - `npm install --registry=https://registry.npmmirror.com`
- 若最终采用了新的镜像源、代理或安装前置条件，并且它会影响后续开发或部署，应同步更新 `AGENTS.md`、README 或对应部署文档。

## Practical Review Checklist
- 改动前：确认实际代码入口、配置来源和运行环境。
- 改动时：避免硬编码业务偏置；优先做通用、可解释、可测试的修复。
- 改动后：补或跑对应测试；核对文档是否需要同步。
- 收尾时：及时 commit，并保持工作区可追踪、可回滚、可复现。
