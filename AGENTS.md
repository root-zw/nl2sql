# Repository Guidelines

## Project Structure & Module Organization
- `server/`: FastAPI backend (Python 3.11+). Entry point is `server/main.py`; API routers live in `server/api/`, business logic in `server/services/`, SQL/IR pipeline in `server/nl2ir/`, `server/compiler/`, `server/exec/`, and shared helpers in `server/utils/`.
- `frontend/`: Vue 3 + Vite UI. Source under `frontend/src/` (views, components, router, store, api clients).
- `config/`: retrieval and domain config (e.g., `retrieval_config.yaml`, dictionaries).
- `docs/`: design and ops guides.
- `scripts/` and `docker/`: migration/diagnostic scripts and local dependency stack.
- `tests/`: pytest suite (currently minimal).

## Build, Test, and Development Commands
Backend:
- Activate the conda env: `conda activate nl2sql`
- `pip install -r requirements.txt`
- `cp env.template .env` and edit required keys (LLM, metadata DB).
- `uvicorn server.main:app --reload --host 0.0.0.0 --port 8000` to run locally.

Dependencies:
- `docker compose -f docker/docker-compose.yml up -d` starts Redis/Postgres/Milvus.

Frontend:
- `cd frontend`
- `npm install`
- `npm run dev` (hot reload), `npm run build`, `npm run preview`.

Tests:
- `pytest` from repo root; use `pytest -q` for quick runs.

## Coding Style & Naming Conventions
- Python: 4-space indent, PEP8, type hints encouraged, `snake_case` for functions/vars, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants. Prefer async I/O patterns consistent with FastAPI.
- Frontend: match existing Vue/Vite style; components in `PascalCase.vue`, composables/utilities in `camelCase`/`kebab-case` as used nearby.
- No enforced formatter yet; optional local use of `black`/`ruff` is welcome if changes stay consistent.

## Testing Guidelines
- Use `pytest` and `pytest-asyncio` for async code.
- Name tests `test_<feature>.py` and keep them focused on new behavior; add regression tests for bug fixes.

## Commit & Pull Request Guidelines
- Commit messages are short, descriptive summaries (often Chinese), sometimes prefixed with a date/version. Follow that pattern, e.g., `251212 修复OIDC同步边界` or `Improve retrieval stats`.
- PRs should include: what/why, how to test, linked issue or context, and screenshots for UI changes.
