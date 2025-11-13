#!/usr/bin/env python3
"""
Run predefined analytics queries against the e-commerce SQLite database.

The script reads SQL statements from queries/analytics.sql, executes them
sequentially, prints formatted results to stdout, and writes the same output
to results/insights.txt for downstream consumption.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple


DEFAULT_DB_PATH = Path("database") / "ecommerce.db"
DEFAULT_SQL_PATH = Path("queries") / "analytics.sql"
OUTPUT_PATH = Path("results") / "insights.txt"


def load_queries(sql_path: Path) -> List[Tuple[str, str]]:
    """
    Parse the analytics SQL file and return a list of (query_name, sql) tuples.

    Queries are expected to be preceded by a "-- Query: name" comment and end
    with a semicolon. Additional comments are preserved for context.
    """
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    queries: List[Tuple[str, str]] = []
    current_lines: List[str] = []
    current_name: str | None = None

    with sql_path.open("r", encoding="utf-8") as sql_file:
        for raw_line in sql_file:
            line = raw_line.rstrip("\n")
            stripped = line.strip()

            if stripped.startswith("-- Query:"):
                current_name = stripped.split(":", 1)[1].strip()

            current_lines.append(line)

            if stripped.endswith(";"):
                sql_statement = "\n".join(current_lines).strip()
                if sql_statement:
                    query_name = current_name or f"query_{len(queries) + 1}"
                    queries.append((query_name, sql_statement))
                current_lines = []
                current_name = None

    # Capture any trailing query without semicolon
    if current_lines:
        sql_statement = "\n".join(current_lines).strip()
        if sql_statement:
            query_name = current_name or f"query_{len(queries) + 1}"
            queries.append((query_name, sql_statement))

    return queries


def format_value(value) -> str:
    """Format a value for display, handling None and floats."""
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def format_results(columns: Sequence[str], rows: Sequence[Sequence]) -> str:
    """
    Convert query results into a simple table with aligned columns.
    """
    if not rows:
        return "No rows returned.\n"

    # Determine column widths based on headers and data
    col_widths = [len(col) for col in columns]
    for row in rows:
        for idx, value in enumerate(row):
            col_widths[idx] = max(col_widths[idx], len(format_value(value)))

    # Build table lines
    header = " | ".join(col.ljust(col_widths[idx]) for idx, col in enumerate(columns))
    separator = "-+-".join("-" * col_widths[idx] for idx in range(len(columns)))
    data_lines = [
        " | ".join(format_value(value).ljust(col_widths[idx]) for idx, value in enumerate(row))
        for row in rows
    ]

    return "\n".join([header, separator, *data_lines]) + "\n"


def execute_query(connection: sqlite3.Connection, sql: str) -> Tuple[List[str], List[sqlite3.Row]]:
    """Execute a single SQL statement and return column names and rows."""
    cursor = connection.execute(sql)
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description] if cursor.description else []
    return columns, rows


def run_queries(db_path: Path, sql_path: Path) -> str:
    """Load, execute, and capture output for all analytics queries."""
    queries = load_queries(sql_path)
    if not queries:
        raise ValueError(f"No queries found in {sql_path}")

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    output_lines: List[str] = []

    try:
        for query_name, sql in queries:
            output_lines.append(f"## {query_name}")
            columns, rows = execute_query(connection, sql)
            table_text = format_results(columns, rows)
            output_lines.append(table_text.rstrip())
            output_lines.append("")  # blank line separator
            print(f"Query: {query_name}")
            print(table_text)
    finally:
        connection.close()

    return "\n".join(output_lines).strip() + "\n"


def save_output(content: str, output_path: Path) -> None:
    """Persist the analytics output to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as outfile:
        outfile.write(content)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run analytics queries against the e-commerce database.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite database file.")
    parser.add_argument("--sql-path", type=Path, default=DEFAULT_SQL_PATH, help="Path to analytics SQL file.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH, help="Path to write query results.")
    args = parser.parse_args()

    output_content = run_queries(args.db_path, args.sql_path)
    save_output(output_content, args.output)
    print(f"Analytics results written to {args.output}")


if __name__ == "__main__":
    main()

