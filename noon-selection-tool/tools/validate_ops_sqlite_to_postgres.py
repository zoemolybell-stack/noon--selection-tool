from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


TABLES = [
    "tasks",
    "task_runs",
    "workers",
    "crawl_plans",
    "crawl_rounds",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ops SQLite -> Postgres row counts.")
    parser.add_argument("--sqlite-db", type=Path, required=True)
    parser.add_argument("--postgres-dsn", required=True)
    return parser.parse_args()


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def main() -> None:
    args = parse_args()
    report: dict[str, dict[str, int | bool]] = {}

    source = sqlite3.connect(str(args.sqlite_db))
    try:
        with psycopg.connect(args.postgres_dsn, row_factory=dict_row) as target:
            for table_name in TABLES:
                sqlite_count = 0
                if sqlite_table_exists(source, table_name):
                    sqlite_count = int(source.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
                postgres_count = int(
                    target.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()["count"]
                )
                report[table_name] = {
                    "sqlite": sqlite_count,
                    "postgres": postgres_count,
                    "match": sqlite_count == postgres_count,
                }
    finally:
        source.close()

    print(json.dumps(report, ensure_ascii=False, indent=2))
    if any(not row["match"] for row in report.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
