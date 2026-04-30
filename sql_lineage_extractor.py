#!/usr/bin/env python3
"""
Discover physical tables used across SQL statements.

Current scope:
- source tables read by each query
- target tables written by each query
- immediate parent tables for written tables

This intentionally stays at table level for now so we can build lineage
incrementally before moving to joins and column-level tracing.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

try:
    import sqlglot
    from sqlglot import exp
    from sqlglot.optimizer.scope import Scope, build_scope
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    sqlglot = None
    exp = None
    Scope = None
    build_scope = None


@dataclass
class QueryTables:
    sql: str
    statement_type: str = "UNKNOWN"
    source_tables: List[str] = field(default_factory=list)
    target_tables: List[str] = field(default_factory=list)
    ctes: List[str] = field(default_factory=list)
    table_columns: Dict[str, List[str]] = field(default_factory=dict)
    unresolved_columns: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        return {
            "statement_type": self.statement_type,
            "source_tables": self.source_tables,
            "target_tables": self.target_tables,
            "ctes": self.ctes,
            "table_columns": self.table_columns,
            "unresolved_columns": self.unresolved_columns,
        }


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def strip_outer_quotes(text: str) -> str:
    stripped = text.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] == '"':
        return stripped[1:-1].replace('""', '"')
    return stripped


def read_sql_statements(path: Path) -> List[str]:
    content = path.read_text(encoding="utf-8")
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if lines and all(line.startswith('"') and line.endswith('"') for line in lines):
        return [strip_outer_quotes(line) for line in lines]
    return [stmt for stmt in split_sql_statements(content) if stmt.strip()]


def split_sql_statements(text: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    depth = 0
    in_single = False
    in_double = False

    for ch in text:
        if ch == "'" and not in_double:
            in_single = not in_single
            current.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            current.append(ch)
            continue

        if not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == ";" and depth == 0:
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                continue

        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def _require_sqlglot() -> None:
    if sqlglot is None or exp is None or build_scope is None:
        raise RuntimeError(
            "sqlglot is required to use this extractor. Install it with `pip install sqlglot`."
        )


def _identifier_name(identifier: object) -> Optional[str]:
    if identifier is None:
        return None
    if isinstance(identifier, exp.Identifier):
        return identifier.this.lower()
    text = str(identifier).strip().strip('"')
    return text.lower() or None


def _table_name(table: exp.Table) -> str:
    catalog = _identifier_name(table.args.get("catalog"))
    db = _identifier_name(table.args.get("db"))
    name = _identifier_name(table.args.get("this"))
    parts = [part for part in (catalog, db, name) if part]
    full_name = ".".join(parts)
    db_link = _identifier_name(table.args.get("db_link"))
    return f"{full_name}@{db_link}" if db_link else full_name


class TableDiscoveryExtractor:
    def __init__(self, dialect: str = "oracle") -> None:
        _require_sqlglot()
        self.dialect = dialect

    def parse_query(self, sql: str) -> QueryTables:
        normalized_sql = normalize_whitespace(sql.strip().rstrip(";"))
        try:
            ast = sqlglot.parse_one(normalized_sql, read=self.dialect, error_level="ignore")
        except Exception:
            ast = None

        if ast is None:
            return QueryTables(sql=normalized_sql)

        ctes = self._collect_cte_names(ast)
        virtual_tables = set(ctes) | self._collect_subquery_aliases(ast)
        targets = self._extract_target_tables(ast)
        sources = self._extract_source_tables(ast, virtual_tables, set(targets))
        table_columns, unresolved_columns = self._extract_table_columns(ast)

        return QueryTables(
            sql=normalized_sql,
            statement_type=ast.key.upper(),
            source_tables=sorted(sources),
            target_tables=targets,
            ctes=sorted(ctes),
            table_columns=table_columns,
            unresolved_columns=unresolved_columns,
        )

    def _collect_cte_names(self, ast: exp.Expression) -> Set[str]:
        names: Set[str] = set()
        with_clause = ast.args.get("with_") or ast.args.get("with")
        if not with_clause:
            return names

        for cte in with_clause.find_all(exp.CTE):
            alias = _identifier_name(cte.alias)
            if alias:
                names.add(alias)
        return names

    def _collect_subquery_aliases(self, ast: exp.Expression) -> Set[str]:
        aliases: Set[str] = set()
        for subquery in ast.find_all(exp.Subquery):
            alias = _identifier_name(subquery.alias)
            if alias:
                aliases.add(alias)
        return aliases

    def _extract_target_tables(self, ast: exp.Expression) -> List[str]:
        targets: List[str] = []

        if isinstance(ast, exp.Insert) and isinstance(ast.this, exp.Table):
            targets.append(_table_name(ast.this))
        elif isinstance(ast, exp.Create) and isinstance(ast.this, exp.Table) and ast.args.get("expression"):
            targets.append(_table_name(ast.this))
        elif isinstance(ast, exp.Merge) and isinstance(ast.this, exp.Table):
            targets.append(_table_name(ast.this))
        elif isinstance(ast, exp.Update) and isinstance(ast.this, exp.Table):
            targets.append(_table_name(ast.this))

        return targets

    def _extract_source_tables(
        self,
        ast: exp.Expression,
        virtual_tables: Set[str],
        target_tables: Set[str],
    ) -> Set[str]:
        sources: Set[str] = set()

        for table in ast.find_all(exp.Table):
            table_name = _table_name(table)
            alias = _identifier_name(table.alias)

            if not table_name:
                continue
            if table_name in virtual_tables or (alias and alias in virtual_tables):
                continue

            # For DML, exclude the destination table from read sources.
            if table_name in target_tables and self._is_write_target_reference(table):
                continue

            sources.add(table_name)

        return sources

    def _is_write_target_reference(self, table: exp.Table) -> bool:
        parent = table.parent
        if isinstance(parent, (exp.Insert, exp.Update, exp.Merge, exp.Create)) and parent.this is table:
            return True
        return False

    def _extract_table_columns(self, ast: exp.Expression) -> tuple[Dict[str, List[str]], List[str]]:
        scope = build_scope(ast)
        if scope is None:
            return {}, []

        table_columns: Dict[str, Set[str]] = {}
        unresolved_columns: Set[str] = set()
        self._collect_scope_columns(scope, table_columns, unresolved_columns)

        return (
            {table: sorted(columns) for table, columns in sorted(table_columns.items())},
            sorted(unresolved_columns),
        )

    def _collect_scope_columns(
        self,
        scope: Scope,
        table_columns: Dict[str, Set[str]],
        unresolved_columns: Set[str],
    ) -> None:
        alias_to_table: Dict[str, str] = {}
        physical_tables: Set[str] = set()

        for alias, source in scope.sources.items():
            if isinstance(source, exp.Table):
                table_name = _table_name(source)
                alias_to_table[alias.lower()] = table_name
                physical_tables.add(table_name)

        for column in scope.expression.find_all(exp.Column):
            column_name = _identifier_name(column.args.get("this")) or column.name.lower()
            table_alias = _identifier_name(column.args.get("table"))

            if table_alias and table_alias in alias_to_table:
                table_name = alias_to_table[table_alias]
                table_columns.setdefault(table_name, set()).add(column_name)
            elif not table_alias and len(physical_tables) == 1:
                table_name = next(iter(physical_tables))
                table_columns.setdefault(table_name, set()).add(column_name)
            elif not table_alias:
                unresolved_columns.add(column_name)

        for child_scope in scope.cte_scopes:
            self._collect_scope_columns(child_scope, table_columns, unresolved_columns)
        for child_scope in scope.subquery_scopes:
            self._collect_scope_columns(child_scope, table_columns, unresolved_columns)
        for child_scope in scope.union_scopes:
            self._collect_scope_columns(child_scope, table_columns, unresolved_columns)


def parse_query_tables(sql: str, dialect: str = "oracle") -> QueryTables:
    return TableDiscoveryExtractor(dialect=dialect).parse_query(sql)


def discover_tables(sql_statements: List[str], dialect: str = "oracle") -> Dict[str, object]:
    extractor = TableDiscoveryExtractor(dialect=dialect)
    queries = [extractor.parse_query(sql) for sql in sql_statements]

    written_by_target: Dict[str, Set[str]] = {}
    all_tables: Set[str] = set()
    catalog_columns: Dict[str, Set[str]] = {}
    unresolved_columns: Set[str] = set()

    for query in queries:
        all_tables.update(query.source_tables)
        all_tables.update(query.target_tables)
        unresolved_columns.update(query.unresolved_columns)

        for table, columns in query.table_columns.items():
            catalog_columns.setdefault(table, set()).update(columns)

        for target in query.target_tables:
            written_by_target.setdefault(target, set()).update(query.source_tables)

    table_graph = [
        {
            "table": table,
            "columns": sorted(catalog_columns.get(table, set())),
            "parent_tables": sorted(written_by_target.get(table, set())),
            "is_written_by_query": table in written_by_target,
        }
        for table in sorted(all_tables)
    ]

    return {
        "query_count": len(queries),
        "queries": [
            {
                "statement_index": index,
                "sql": query.sql,
                "tables": query.as_dict(),
            }
            for index, query in enumerate(queries, start=1)
        ],
        "tables": table_graph,
        "unresolved_columns": sorted(unresolved_columns),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover source and target tables across SQL statements."
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to a text/sql file containing SQL statements or one quoted SQL statement per line.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Optional path to write JSON output.",
    )
    parser.add_argument(
        "--dialect",
        default="oracle",
        help="sqlglot dialect to use when parsing. Default: oracle",
    )
    args = parser.parse_args()

    statements = read_sql_statements(args.input)
    try:
        payload = discover_tables(statements, dialect=args.dialect)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    json_output = json.dumps(payload, indent=2)
    if args.output:
        args.output.write_text(json_output, encoding="utf-8")
    else:
        print(json_output)


if __name__ == "__main__":
    main()
