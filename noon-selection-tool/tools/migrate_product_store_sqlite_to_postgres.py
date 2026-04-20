from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.product_store import ProductStore


TABLE_COPY_ORDER = [
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

SERIAL_ID_TABLES = {
    "price_history",
    "rank_history",
    "sales_snapshot",
    "crawl_observations",
    "keyword_edges",
    "keyword_runs",
    "keyword_metrics_snapshots",
}

PRODUCT_IDENTITY_TABLES = {
    "products",
    "price_history",
    "rank_history",
    "sales_snapshot",
    "crawl_observations",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate product_store SQLite data into Postgres.")
    parser.add_argument("--sqlite-db", type=Path, required=True, help="Source SQLite product_store.db path")
    parser.add_argument("--postgres-dsn", required=True, help="Target Postgres DSN")
    parser.add_argument("--truncate", action="store_true", help="Truncate target tables before import")
    return parser.parse_args()


def _source_table_exists(sqlite_conn: sqlite3.Connection, table_name: str) -> bool:
    row = sqlite_conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def _truncate_target_tables(store: ProductStore) -> None:
    joined = ", ".join(TABLE_COPY_ORDER)
    with store.conn.transaction():
        with store.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {joined} RESTART IDENTITY CASCADE")


def _sync_sequence(store: ProductStore, table_name: str) -> None:
    if table_name not in SERIAL_ID_TABLES:
        return
    with store.conn.transaction():
        with store.conn.cursor() as cur:
            cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table_name,))
            row = cur.fetchone()
            sequence_name = next(iter(row.values())) if row else None
            if not sequence_name:
                return
            cur.execute(
                f"SELECT setval(%s, COALESCE((SELECT MAX(id) FROM {table_name}), 1), true)",
                (sequence_name,),
            )


def _build_product_key(product_id: object, platform: object) -> str:
    normalized_id = str(product_id or "").strip()
    if not normalized_id:
        raise ValueError("product_id is required to backfill product_key")
    normalized_platform = str(platform or "").strip() or "noon"
    return f"{normalized_platform}::{normalized_id}"


def _normalize_row(table_name: str, row: dict[str, object]) -> dict[str, object]:
    normalized = dict(row)
    if table_name in PRODUCT_IDENTITY_TABLES:
        platform = str(normalized.get("platform") or "").strip() or "noon"
        normalized["platform"] = platform
        current_key = str(normalized.get("product_key") or "").strip()
        if not current_key:
            normalized["product_key"] = _build_product_key(normalized.get("product_id"), platform)
    return normalized


def _copy_table(sqlite_conn: sqlite3.Connection, store: ProductStore, table_name: str) -> int:
    if not _source_table_exists(sqlite_conn, table_name):
        return 0
    rows = [dict(row) for row in sqlite_conn.execute(f"SELECT * FROM {table_name}").fetchall()]
    if not rows:
        return 0

    columns = list(rows[0].keys())
    if table_name in PRODUCT_IDENTITY_TABLES:
        if "platform" not in columns:
            columns.append("platform")
        if "product_key" not in columns:
            columns.append("product_key")
    copy_sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN"
    with store.conn.transaction():
        with store.conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in rows:
                    normalized = _normalize_row(table_name, row)
                    copy.write_row(tuple(normalized[column] for column in columns))
    _sync_sequence(store, table_name)
    return len(rows)


def main() -> int:
    args = parse_args()
    source_db = args.sqlite_db.expanduser()
    if not source_db.exists():
        raise SystemExit(f"SQLite source not found: {source_db}")

    sqlite_conn = sqlite3.connect(str(source_db))
    sqlite_conn.row_factory = sqlite3.Row
    previous = os.environ.get("NOON_PRODUCT_STORE_DATABASE_URL")
    os.environ["NOON_PRODUCT_STORE_DATABASE_URL"] = args.postgres_dsn
    try:
        store = ProductStore(ROOT / "data" / "product_store.db")
        try:
            if not store.is_postgres:
                raise SystemExit("Target ProductStore backend is not postgres")
            if args.truncate:
                _truncate_target_tables(store)

            counts: dict[str, int] = {}
            for table_name in TABLE_COPY_ORDER:
                counts[table_name] = _copy_table(sqlite_conn, store, table_name)

            print("product_store sqlite -> postgres migration complete")
            for table_name in TABLE_COPY_ORDER:
                print(f"{table_name}: {counts.get(table_name, 0)} rows")
        finally:
            store.close()
    finally:
        sqlite_conn.close()
        if previous is None:
            os.environ.pop("NOON_PRODUCT_STORE_DATABASE_URL", None)
        else:
            os.environ["NOON_PRODUCT_STORE_DATABASE_URL"] = previous
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
