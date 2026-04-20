from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.task_store import OpsStore


TABLE_COPY_ORDER = [
    "tasks",
    "task_runs",
    "workers",
    "crawl_plans",
    "crawl_rounds",
]

SERIAL_ID_TABLES = {
    "tasks",
    "task_runs",
    "crawl_plans",
    "crawl_rounds",
}

BOOLEAN_COLUMNS = {
    "crawl_plans": {"enabled"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate ops SQLite data into Postgres.")
    parser.add_argument("--sqlite-db", type=Path, required=True, help="Source SQLite ops.db path")
    parser.add_argument("--postgres-dsn", required=True, help="Target Postgres DSN")
    parser.add_argument("--truncate", action="store_true", help="Truncate target tables before import")
    return parser.parse_args()


def _source_table_exists(sqlite_conn: sqlite3.Connection, table_name: str) -> bool:
    row = sqlite_conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def _truncate_target_tables(pg_store: OpsStore) -> None:
    joined = ", ".join(TABLE_COPY_ORDER)
    with pg_store.conn.transaction():
        with pg_store.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {joined} RESTART IDENTITY CASCADE")
    pg_store.conn.commit()


def _sync_sequence(pg_store: OpsStore, table_name: str) -> None:
    if table_name not in SERIAL_ID_TABLES:
        return
    with pg_store.conn.transaction():
        with pg_store.conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_get_serial_sequence(%s, 'id')
                """,
                (table_name,),
            )
            row = cur.fetchone()
            sequence_name = next(iter(row.values())) if row else None
            if not sequence_name:
                return
            cur.execute(
                f"""
                SELECT setval(%s, COALESCE((SELECT MAX(id) FROM {table_name}), 1), true)
                """,
                (sequence_name,),
            )
    pg_store.conn.commit()


def _normalize_value(table_name: str, column_name: str, value: Any) -> Any:
    if column_name in BOOLEAN_COLUMNS.get(table_name, set()):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "f", "no", "n", "off", ""}:
            return False
    return value


def _copy_table(sqlite_conn: sqlite3.Connection, pg_store: OpsStore, table_name: str, *, truncate: bool) -> int:
    if not _source_table_exists(sqlite_conn, table_name):
        return 0
    rows = [dict(row) for row in sqlite_conn.execute(f"SELECT * FROM {table_name}").fetchall()]
    if not rows:
        return 0

    column_names = list(rows[0].keys())
    placeholders = ", ".join("?" for _ in column_names)
    sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"

    with pg_store.conn.transaction():
        with pg_store.conn.cursor() as cur:
            if truncate:
                cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            for row in rows:
                cur.execute(
                    sql.replace("?", "%s"),
                    tuple(_normalize_value(table_name, column, row[column]) for column in column_names),
                )
    pg_store.conn.commit()
    _sync_sequence(pg_store, table_name)
    return len(rows)


def main() -> int:
    args = parse_args()
    source_db = args.sqlite_db.expanduser()
    if not source_db.exists():
        raise SystemExit(f"SQLite source not found: {source_db}")

    sqlite_conn = sqlite3.connect(str(source_db))
    sqlite_conn.row_factory = sqlite3.Row
    previous = os.environ.get("NOON_OPS_DATABASE_URL")
    os.environ["NOON_OPS_DATABASE_URL"] = args.postgres_dsn
    try:
        pg_store = OpsStore()
        try:
            counts: dict[str, int] = {}
            if args.truncate:
                _truncate_target_tables(pg_store)
            for table_name in TABLE_COPY_ORDER:
                copied = _copy_table(
                    sqlite_conn,
                    pg_store,
                    table_name,
                    truncate=False,
                )
                counts[table_name] = copied
            print("ops sqlite -> postgres migration complete")
            for table_name in TABLE_COPY_ORDER:
                print(f"{table_name}: {counts.get(table_name, 0)} rows")
        finally:
            pg_store.close()
    finally:
        sqlite_conn.close()
        if previous is None:
            os.environ.pop("NOON_OPS_DATABASE_URL", None)
        else:
            os.environ["NOON_OPS_DATABASE_URL"] = previous
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
