from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from analytics_assistant.config import settings
from analytics_assistant.models import Source, ToolResponse

WRITE_KEYWORDS = {
    "alter",
    "attach",
    "create",
    "delete",
    "detach",
    "drop",
    "insert",
    "merge",
    "pragma",
    "replace",
    "truncate",
    "update",
    "vacuum",
}


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)


def _validate_read_only_sql(sql: str) -> str:
    cleaned = _strip_sql_comments(sql).strip()
    if not cleaned:
        raise ValueError("SQL query is empty.")

    statements = [part.strip() for part in cleaned.split(";") if part.strip()]
    if len(statements) != 1:
        raise ValueError("Only one SQL statement is allowed per tool call.")

    statement = statements[0]
    first_token = statement.split(None, 1)[0].lower()
    if first_token not in {"select", "with"}:
        raise ValueError("Only read-only SELECT statements are allowed.")

    tokens = {token.lower() for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", statement)}
    blocked = sorted(tokens & WRITE_KEYWORDS)
    if blocked:
        raise ValueError(f"Blocked SQL keyword(s): {', '.join(blocked)}")

    return statement


def _extract_table_names(sql: str) -> list[str]:
    matches = re.findall(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_.$]*)", sql, re.IGNORECASE)
    return sorted(set(matches))


def secure_sql_query(
    sql: str,
    database_path: str | None = None,
    row_limit: int | None = None,
) -> ToolResponse:
    statement = _validate_read_only_sql(sql)
    limit = min(row_limit or settings.max_sql_rows, settings.max_sql_rows)
    db_path = Path(database_path) if database_path else settings.default_sqlite_path

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    limited_statement = f"SELECT * FROM ({statement}) AS secure_query LIMIT ?"
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.execute(limited_statement, (limit,))
        rows = [dict(row) for row in cursor.fetchall()]
        columns = [description[0] for description in cursor.description or []]

    tables = _extract_table_names(statement)
    sources = [
        Source(
            type="sql_table",
            name=table,
            details={"database": str(db_path), "columns_returned": columns},
        )
        for table in tables
    ]

    return ToolResponse(
        data=rows,
        sources=sources,
        explainability={
            "query_type": "read_only_select",
            "row_limit_applied": limit,
            "validated_sql": statement,
        },
    )

