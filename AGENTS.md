# Repository Guidelines

## Source Of Truth
- 先看代码、脚本和当前机器实际状态，再下结论；不要只依据仓库文档。
- 仓库内部分文档可能已经过期。所有分析、命令和建议都必须能在当前代码、当前脚本或当前机器配置中找到依据。
- 若仓库文档与代码、脚本、service 配置或当前机器实际配置不一致，以代码、脚本和当前机器实际配置为准，并同步修正文档。
- 不确定运行约定时，优先检查 `server/config.py`、当前 `.env`、`docker/docker-compose.yml`、现有测试和当前机器环境。

## Current Repository Layout
- 当前仓库根目录实际存在的核心目录是：`server/`、`frontend/`、`config/`、`docker/`、`prompts/`、`tests/`。
- 当前仓库根目录还包含：`README.md`、`requirements.txt`、`env.template`、`.env`。
- 不要假设 `docs/`、`scripts/` 等目录一定存在；如需引用，先检查当前仓库实际结构。
- `server/`: FastAPI 后端。入口是 `server/main.py`；接口在 `server/api/`；核心服务在 `server/services/`；问数主链路在 `server/nl2ir/`、`server/compiler/`、`server/exec/`；通用工具在 `server/utils/`。
- `frontend/`: Vue 3 + Vite 前端。源码在 `frontend/src/`，包含 `api/`、`router/`、`stores/`、`views/` 等。
- `docker/`: 当前实际存在的依赖编排和初始化脚本。重点文件是 `docker/docker-compose.yml` 和 `docker/init-scripts/init_database_complete.sql`。
- `tests/`: 当前测试目录。现状是 `tests/conftest.py` 加若干按功能拆分的 `tests/test_<feature>.py` 文件。

## Environment And Commands
- Python 开发的默认约定环境名记录为 conda 环境 `nl2sql`。这是用户明确要求保留的环境名信息。
- 但必须先以当前机器实际配置为准。当前机器已核实可用的新增环境是 `nl2sql-py312`，路径为 `/home/zhangwei/miniconda3/envs/nl2sql-py312`，版本为 `Python 3.12.13`。
- 当前机器未发现 `/home/zhangwei/miniconda3/envs/nl2sql`；因此在本机执行后端初始化、迁移、测试、启动等命令前，必须先核对目标环境是否实际存在。
- 若后续本机重新创建了 `nl2sql`，则默认命令仍应优先写成 `conda activate nl2sql` 或 `conda run -n nl2sql ...`；若未创建，则应改用当前机器已验证可用的环境执行。
- 后端常用命令优先写成：
  - `conda run -n nl2sql pip install -r requirements.txt`
  - `conda run -n nl2sql uvicorn server.main:app --reload --host 0.0.0.0 --port 8000`
  - `conda run -n nl2sql pytest -q`
- 当前机器上若需要直接使用已验证可用的 Python 3.12 环境，可使用：
  - `conda run -n nl2sql-py312 python --version`
- 当前代码和 `.env` 已核实 `SERVER_PORT=8000`。
- 依赖栈当前以 `docker/docker-compose.yml` 为准。已核实常用启动命令是：
  - `docker compose -f docker/docker-compose.yml up -d`
- 当前机器 `.env` 已核实 `REDIS_URL=redis://localhost:6380/0`，且 `docker/docker-compose.yml` 暴露 Redis `6380:6379`。涉及 Redis 排障或命令时，应以这一实际配置为准。

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
- 当前项目已在 `requirements.txt` 中声明 `pytest` 与 `pytest-asyncio`，测试命令默认使用 `conda run -n nl2sql pytest ...`。

## Test Organization
- 新建测试脚本时，需符合当前项目的分层测试体系。
- 当前仓库的实际测试形态是：`tests/` 根目录下按功能模块拆分测试文件，公共初始化放在 `tests/conftest.py`。
- 因此新增测试优先遵循现有模式：
  - 文件命名使用 `tests/test_<feature>.py`
  - 按功能或模块归类，而不是按临时任务名随意命名
  - 能并入现有测试文件时，优先就近追加
- 若后续某一模块测试明显增多，可在 `tests/` 下新增子目录分层，但必须保持结构清晰、可发现、与当前代码模块对应。
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

## Practical Review Checklist
- 改动前：确认实际代码入口、配置来源和运行环境。
- 改动时：避免硬编码业务偏置；优先做通用、可解释、可测试的修复。
- 改动后：补或跑对应测试；核对文档是否需要同步。
- 收尾时：及时 commit，并保持工作区可追踪、可回滚、可复现。
