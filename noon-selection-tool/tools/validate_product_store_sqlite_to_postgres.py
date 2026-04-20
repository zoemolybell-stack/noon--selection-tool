from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


TABLES = [
    "products",
    "price_history",
    "rank_history",
    "sales_snapshot",
    "crawl_observations",
    "keywords",
    "keyword_edges",
    "keyword_runs",
    "keyword_metrics_snapshots",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate product_store migration from SQLite to Postgres.")
    parser.add_argument("--sqlite-db", type=Path, required=True, help="Source SQLite product_store.db path")
    parser.add_argument("--postgres-dsn", required=True, help="Target Postgres DSN")
    return parser.parse_args()


def sqlite_count(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row else 0


def postgres_count(conn: psycopg.Connection, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS count FROM {table_name}")
        row = cur.fetchone()
        return int(row["count"]) if row else 0


def main() -> int:
    args = parse_args()
    sqlite_conn = sqlite3.connect(str(args.sqlite_db.expanduser()))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg.connect(args.postgres_dsn, row_factory=dict_row)
    try:
        report: dict[str, dict[str, int | bool]] = {}
        has_mismatch = False
        for table_name in TABLES:
            sqlite_rows = sqlite_count(sqlite_conn, table_name)
            postgres_rows = postgres_count(pg_conn, table_name)
            match = sqlite_rows == postgres_rows
            has_mismatch = has_mismatch or not match
            report[table_name] = {
                "sqlite": sqlite_rows,
                "postgres": postgres_rows,
                "match": match,
            }
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1 if has_mismatch else 0
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
