# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NL2SQL (Natural Language to SQL) is a system that converts natural language questions to SQL queries. It uses a hybrid architecture combining LLM-based table selection with vector retrieval, an Intermediate Representation (IR) layer, and multi-dialect SQL compilation.

## Build and Development Commands

### Backend (Python 3.11+, Conda env: `nl2sql`)
```bash
# Activate environment
conda activate nl2sql

# Install dependencies
pip install -r requirements.txt

# Run server (default port 8000, or 8890 in some configs)
uvicorn server.main:app --host 0.0.0.0 --port 8890 --reload

# Run tests
pytest
pytest -q  # quick run
```

### Frontend (Vue 3 + Vite)
```bash
cd frontend
npm install
npm run dev      # development server on port 3000
npm run build
npm run preview
```

### Infrastructure (Docker)
```bash
docker compose -f docker/docker-compose.yml up -d   # Start Redis, PostgreSQL, Milvus
docker compose -f docker/docker-compose.yml down
```

## Architecture

### Core Pipeline Flow
1. **Table Selection** - Dual-path: LLM-based (`llm_table_selector.py`) or Vector-based (`vector_table_selector.py`)
2. **NL2IR Parsing** - Natural language → Intermediate Representation (`server/nl2ir/parser.py`)
3. **IR Compilation** - IR → SQL AST (`server/compiler/ast_builder.py`, `compiler.py`)
4. **Dialect Translation** - SQL AST → target dialect (T-SQL, MySQL, PostgreSQL) via `dialect_*.py`
5. **Execution** - Query execution with row-level security filters (`server/exec/`)

### Key Directories
- `server/nl2ir/` - NL parsing, table/enum retrieval, Few-Shot, LLM clients
- `server/compiler/` - IR→SQL compilation, dialect support, rule application
- `server/api/` - FastAPI routes; `admin/` subpackage for management APIs
- `server/auth/` - Authentication (local, OIDC, LDAP, API gateway)
- `server/sync/` - Auto-sync between PostgreSQL metadata and Milvus vectors
- `server/metadata/` - Semantic model management (domains, tables, fields)
- `prompts/` - LLM prompt templates for different stages (nl2ir, table_selector, narrative)
- `config/` - `retrieval_config.yaml` (retrieval parameters), `global_rules.yaml`, dictionaries

### Hybrid Architecture
Controlled by `HYBRID_ARCHITECTURE_ENABLED` and `DIRECT_SQL_ENABLED`:
- IR path: Structured parsing for standard queries
- Direct SQL path: LLM generates SQL directly for complex queries (marked via `requires_direct_sql` in IR)

### Data Flow
- **PostgreSQL** (`text2sql_metadata`): Stores semantic model metadata (domains, tables, fields, enums, rules)
- **Milvus**: Vector store for semantic retrieval (dense embeddings + sparse BM25)
- **Redis**: Caching, distributed locks for sync operations

## Configuration

- `env.template` → copy to `.env` for environment variables
- `config/retrieval_config.yaml` - All retrieval algorithm parameters, feature switches
- When adding new env vars, update `env.template`
- When modifying PostgreSQL schema, sync to `docker/init-scripts/init_database_complete.sql`

## Key Environment Variables
- `LLM_TABLE_SELECTION_ENABLED` - Toggle LLM vs Vector table selection
- `HYBRID_ARCHITECTURE_ENABLED` / `DIRECT_SQL_ENABLED` - Enable direct SQL generation path
- `MILVUS_*` - Vector database connection and collection settings
- `POSTGRES_*` - Metadata database connection

## Code Conventions

- Python: `snake_case` for functions/variables, `PascalCase` for classes, type hints encouraged
- Frontend: Vue SFC in `PascalCase.vue`, utilities in `camelCase`
- All code must be executable (no pseudocode)
- Conda environment `nl2sql` required for all Python operations
- Only write `is_active=true` fields to Milvus (human-curated active fields)

## Commit Style
Short descriptive summaries, often Chinese with date prefix, e.g., `20251216双链路表检索` or `Improve retrieval stats`.
