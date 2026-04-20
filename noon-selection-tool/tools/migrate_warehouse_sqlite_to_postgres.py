from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SQLITE_TABLES = [
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
    parser = argparse.ArgumentParser(description="Migrate warehouse SQLite data into Postgres.")
    parser.add_argument("--sqlite-db", type=Path, required=True)
    parser.add_argument("--postgres-dsn", required=True)
    parser.add_argument("--truncate", action="store_true")
    return parser.parse_args()


def _copy_table(src: sqlite3.Connection, dst: psycopg.Connection, table_name: str) -> int:
    rows = src.execute(f"SELECT * FROM {table_name}").fetchall()
    if not rows:
        return 0
    columns = [col[0] for col in src.execute(f"SELECT * FROM {table_name} LIMIT 0").description]
    with dst.cursor() as cur:
        with cur.copy(
            f"COPY {table_name} ({', '.join(columns)}) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(tuple(row))
    return len(rows)


def main() -> None:
    args = parse_args()

    if not args.sqlite_db.exists():
        raise SystemExit(f"sqlite warehouse db not found: {args.sqlite_db}")

    os.environ["NOON_WAREHOUSE_DATABASE_URL"] = args.postgres_dsn

    from build_analytics_warehouse import AnalyticsWarehouseBuilder
    from web_beta.app import _open_db_connection, rebuild_web_read_models

    builder = AnalyticsWarehouseBuilder(ROOT / "data" / "analytics" / "warehouse.db")
    try:
        if args.truncate:
            builder.reset()
    finally:
        builder.close()

    source_conn = sqlite3.connect(str(args.sqlite_db))
    try:
        with psycopg.connect(args.postgres_dsn, autocommit=False) as target_conn:
            if args.truncate:
                with target_conn.cursor() as cur:
                    for table_name in reversed(SQLITE_TABLES):
                        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            copied = {}
            for table_name in SQLITE_TABLES:
                copied[table_name] = _copy_table(source_conn, target_conn, table_name)
            target_conn.commit()

        read_model_conn = _open_db_connection()
        try:
            rebuild_web_read_models(read_model_conn)
            if hasattr(read_model_conn, "commit"):
                read_model_conn.commit()
        finally:
            read_model_conn.close()
    finally:
        source_conn.close()

    for table_name in SQLITE_TABLES:
        print(f"{table_name}: {copied[table_name]} rows")


if __name__ == "__main__":
    main()
