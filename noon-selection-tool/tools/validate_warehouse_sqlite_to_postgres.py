from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


TABLES = [
    "source_databases",
    "product_identity",
    "observation_events",
    "product_category_membership",
    "product_keyword_membership",
    "keyword_catalog",
    "keyword_runs_log",
    "keyword_metric_snapshots",
    "keyword_expansion_edges",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate warehouse SQLite -> Postgres row counts.")
    parser.add_argument("--sqlite-db", type=Path, required=True)
    parser.add_argument("--postgres-dsn", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report: dict[str, dict[str, int | bool]] = {}

    source = sqlite3.connect(str(args.sqlite_db))
    try:
        with psycopg.connect(args.postgres_dsn, row_factory=dict_row) as target:
            for table_name in TABLES:
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
    if any(not entry["match"] for entry in report.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
