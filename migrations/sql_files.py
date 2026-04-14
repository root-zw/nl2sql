from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DOLLAR_QUOTE_PATTERN = re.compile(r"\$[A-Za-z0-9_]*\$")


def load_sql_script(relative_path: str) -> str:
    sql_path = PROJECT_ROOT / relative_path
    return sql_path.read_text(encoding="utf-8")


def split_sql_statements(sql: str) -> list[str]:
    """按 PostgreSQL 常见语法安全拆分 SQL 语句。"""
    statements: list[str] = []
    buffer: list[str] = []
    idx = 0
    length = len(sql)
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    block_comment_depth = 0
    dollar_tag: str | None = None

    while idx < length:
        current = sql[idx]
        pair = sql[idx : idx + 2]

        if in_line_comment:
            buffer.append(current)
            idx += 1
            if current == "\n":
                in_line_comment = False
            continue

        if block_comment_depth:
            if pair == "/*":
                buffer.append(pair)
                idx += 2
                block_comment_depth += 1
                continue
            if pair == "*/":
                buffer.append(pair)
                idx += 2
                block_comment_depth -= 1
                continue
            buffer.append(current)
            idx += 1
            continue

        if dollar_tag is not None:
            if sql.startswith(dollar_tag, idx):
                buffer.append(dollar_tag)
                idx += len(dollar_tag)
                dollar_tag = None
                continue
            buffer.append(current)
            idx += 1
            continue

        if in_single_quote:
            buffer.append(current)
            idx += 1
            if current == "'" and idx < length and sql[idx] == "'":
                buffer.append(sql[idx])
                idx += 1
                continue
            if current == "'":
                in_single_quote = False
            continue

        if in_double_quote:
            buffer.append(current)
            idx += 1
            if current == '"' and idx < length and sql[idx] == '"':
                buffer.append(sql[idx])
                idx += 1
                continue
            if current == '"':
                in_double_quote = False
            continue

        if pair == "--":
            buffer.append(pair)
            idx += 2
            in_line_comment = True
            continue

        if pair == "/*":
            buffer.append(pair)
            idx += 2
            block_comment_depth = 1
            continue

        if current == "'":
            buffer.append(current)
            idx += 1
            in_single_quote = True
            continue

        if current == '"':
            buffer.append(current)
            idx += 1
            in_double_quote = True
            continue

        if current == "$":
            match = _DOLLAR_QUOTE_PATTERN.match(sql, idx)
            if match:
                dollar_tag = match.group(0)
                buffer.append(dollar_tag)
                idx += len(dollar_tag)
                continue

        if current == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            idx += 1
            continue

        buffer.append(current)
        idx += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)

    return statements


def iter_sql_file(relative_path: str):
    return split_sql_statements(load_sql_script(relative_path))
