from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).parent))

from db.config import DatabaseConfig, database_config_from_reference, get_warehouse_database_config


ROOT = Path(__file__).parent
DEFAULT_CATEGORY_DB = ROOT / "data" / "product_store.db"
DEFAULT_RUNTIME_DIR = ROOT / "runtime_data"
DEFAULT_KEYWORD_DB = DEFAULT_RUNTIME_DIR / "keyword" / "product_store.db"
DEFAULT_WAREHOUSE_DB = ROOT / "data" / "analytics" / "warehouse.db"
DEFAULT_REPORTS_DIR = ROOT / "data" / "reports"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("analytics-warehouse")


def configure_stdio() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            continue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build analytics warehouse from category and keyword stage stores.")
    parser.add_argument("--category-db", type=str, default=str(DEFAULT_CATEGORY_DB), help="Category stage database path or DSN.")
    parser.add_argument(
        "--keyword-dbs",
        type=str,
        nargs="*",
        default=None,
        help="Keyword stage database paths or DSNs. If omitted, discover from runtime_data.",
    )
    parser.add_argument("--warehouse-db", type=str, default=str(DEFAULT_WAREHOUSE_DB), help="Warehouse database output path or DSN.")
    parser.add_argument("--replace", action="store_true", help="Rebuild warehouse before import.")
    parser.add_argument(
        "--discover-keyword-dbs",
        action="store_true",
        help="Also discover additional keyword stage databases under runtime_data.",
    )
    parser.add_argument(
        "--allow-legacy-keyword-fallback",
        action="store_true",
        help="only use discovered historical keyword DBs as a fallback when the official keyword DB is empty or missing",
    )
    parser.add_argument(
        "--skip-stats",
        action="store_true",
        help="Skip expensive warehouse-wide stats collection. Intended for frequent incremental syncs.",
    )
    parser.add_argument(
        "--skip-reports",
        action="store_true",
        help="Skip writing summary report files. Intended for frequent incremental syncs.",
    )
    return parser.parse_args()


def discover_keyword_stage_dbs(runtime_dir: Path) -> list[Path]:
    if not runtime_dir.exists():
        return []

    candidates: list[Path] = []
    for db_path in runtime_dir.glob("**/product_store.db"):
        if "keyword" not in str(db_path).lower():
            continue
        candidates.append(db_path)
    return sorted(set(candidates), key=lambda path: str(path))


def make_source_label(db_path: Path) -> str:
    try:
        raw = str(db_path.relative_to(ROOT))
    except ValueError:
        raw = str(db_path)
    return raw.replace("\\", "__").replace("/", "__").replace(":", "").replace(".", "_").lower()


def make_database_label(source_ref: str | Path) -> str:
    config = database_config_from_reference(source_ref)
    if config.is_postgres:
        parsed = urlparse(config.dsn or "")
        host = (parsed.hostname or "localhost").replace(".", "_")
        dbname = (parsed.path or "/postgres").lstrip("/") or "postgres"
        port = parsed.port or 5432
        return f"postgres__{host}__{port}__{dbname}".replace("-", "_").lower()
    return make_source_label(config.sqlite_path_or_raise("source database"))


def detect_scope(db_path: Path) -> str:
    lowered = str(db_path).lower()
    if lowered == str(DEFAULT_CATEGORY_DB).lower():
        return "category_stage"
    if "keyword" in lowered:
        return "keyword_stage"
    return "shared_stage"


def sql_window_start(timestamp_expr: str, days: int, backend: str) -> str:
    if backend == "postgres":
        return f"(({timestamp_expr})::timestamp - INTERVAL '{int(days)} day')"
    return f"datetime({timestamp_expr}, '-{int(days)} day')"


def sql_latest_non_null_event_value(column_name: str) -> str:
    return f"""
                (
                    SELECT oe.{column_name}
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND oe.{column_name} IS NOT NULL
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS latest_{column_name}
    """


def sql_growth_metric(column_name: str, days: int, backend: str) -> str:
    scraped_at_expr = "oe.scraped_at::timestamp" if backend == "postgres" else "oe.scraped_at"
    latest_timestamp_expr = """
                    SELECT {scraped_at_expr}
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND oe.{column_name} IS NOT NULL
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
    """.format(column_name=column_name, scraped_at_expr=scraped_at_expr)
    earliest_value_expr = """
                    SELECT oe.{column_name}
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND oe.{column_name} IS NOT NULL
                      AND {scraped_at_expr} >= {window_start}
                    ORDER BY oe.scraped_at ASC, oe.id ASC
                    LIMIT 1
    """.format(
        column_name=column_name,
        scraped_at_expr=scraped_at_expr,
        window_start=sql_window_start(f"({latest_timestamp_expr})", days, backend),
    )
    return f"""
                CASE
                    WHEN latest.{column_name} IS NULL THEN NULL
                    ELSE latest.{column_name} - ({earliest_value_expr})
                END AS {column_name}_growth_{days}d
    """


def connect_sqlite_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def connect_stage_database(source_ref: str | Path) -> tuple[Any, str]:
    config = database_config_from_reference(source_ref)
    if config.is_postgres:
        return (
            psycopg.connect(
                config.dsn,
                row_factory=dict_row,
                autocommit=True,
            ),
            "postgres",
        )
    return connect_sqlite_db(config.sqlite_path_or_raise("stage database")), "sqlite"


def _query_placeholder(backend: str) -> str:
    return "%s" if backend == "postgres" else "?"


def _build_since_clause(
    *,
    backend: str,
    available_columns: set[str],
    timestamp_candidates: tuple[str, ...],
    cutoff_iso: str | None,
) -> tuple[str, tuple[Any, ...]]:
    if not cutoff_iso:
        return "", ()
    selected = [column for column in timestamp_candidates if column in available_columns]
    if not selected:
        return "", ()
    expr = selected[0] if len(selected) == 1 else f"COALESCE({', '.join(selected)})"
    return f" WHERE {expr} >= {_query_placeholder(backend)}", (cutoff_iso,)


def _first_value(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    try:
        return row[0]
    except (IndexError, KeyError, TypeError):
        return next(iter(row), None) if hasattr(row, "__iter__") else None


def table_exists(conn: Any, table_name: str, *, backend: str = "sqlite") -> bool:
    if backend == "postgres":
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return row is not None
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(conn: Any, table_name: str, *, backend: str = "sqlite") -> set[str]:
    if backend == "postgres":
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        ).fetchall()
        return {row["column_name"] for row in rows}
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def inspect_stage_db(db_ref: str | Path) -> dict[str, Any]:
    conn, backend = connect_stage_database(db_ref)
    try:
        stats = {
            "db_path": str(db_ref),
            "products": 0,
            "crawl_observations": 0,
            "keywords": 0,
            "keyword_runs": 0,
            "keyword_metrics_snapshots": 0,
            "backend": backend,
        }
        for table_name in (
            "products",
            "crawl_observations",
            "keywords",
            "keyword_runs",
            "keyword_metrics_snapshots",
        ):
            if table_exists(conn, table_name, backend=backend):
                stats[table_name] = int(_first_value(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()) or 0)

        stats["has_payload"] = any(
            stats[key] > 0
            for key in ("products", "crawl_observations", "keyword_metrics_snapshots")
        )
        return stats
    finally:
        conn.close()


def _keyword_db_rank(stats: dict[str, Any], db_ref: str | Path) -> tuple[int, int, int, int, int]:
    modified_at = 0
    config = database_config_from_reference(db_ref)
    if config.is_sqlite:
        db_path = config.sqlite_path_or_raise("keyword stage")
        modified_at = int(db_path.stat().st_mtime) if db_path.exists() else 0
    return (
        int(stats.get("keyword_metrics_snapshots", 0) or 0),
        int(stats.get("crawl_observations", 0) or 0),
        int(stats.get("products", 0) or 0),
        int(stats.get("keyword_runs", 0) or 0),
        modified_at,
    )


def select_keyword_stage_dbs(
    explicit_keyword_dbs: list[str] | None,
    runtime_dir: Path,
    *,
    include_discovery: bool = False,
    allow_legacy_fallback: bool = False,
) -> tuple[list[str], dict[str, Any]]:
    if explicit_keyword_dbs:
        selected: list[str] = []
        for db_ref in explicit_keyword_dbs:
            config = database_config_from_reference(db_ref)
            if config.is_sqlite and not config.sqlite_path_or_raise("keyword stage").exists():
                continue
            normalized = str(db_ref)
            if normalized not in selected:
                selected.append(normalized)
        return selected, {
            "mode": "explicit",
            "official_keyword_db": str(DEFAULT_KEYWORD_DB),
            "selected_keyword_dbs": selected,
            "allow_legacy_fallback": allow_legacy_fallback,
        }

    selection_info: dict[str, Any] = {
        "mode": "official_only",
        "official_keyword_db": str(DEFAULT_KEYWORD_DB),
        "selected_keyword_dbs": [],
        "fallback_keyword_db": None,
        "allow_legacy_fallback": allow_legacy_fallback,
        "discovered_keyword_dbs": [],
        "discovered_candidate_count": 0,
    }

    official_has_payload = False
    if DEFAULT_KEYWORD_DB.exists():
        official_stats = inspect_stage_db(DEFAULT_KEYWORD_DB)
        selection_info["official_stats"] = official_stats
        official_has_payload = bool(official_stats.get("has_payload"))
        if not official_has_payload:
            selection_info["mode"] = "official_empty"
    else:
        selection_info["mode"] = "official_missing"

    discovered = [
        path
        for path in discover_keyword_stage_dbs(runtime_dir)
        if path.exists() and path != DEFAULT_KEYWORD_DB
    ]
    ranked: list[tuple[tuple[int, int, int, int, int], Path, dict[str, Any]]] = []
    for db_path in discovered:
        stats = inspect_stage_db(db_path)
        if not stats.get("has_payload"):
            continue
        ranked.append((_keyword_db_rank(stats, db_path), db_path, stats))

    ranked.sort(key=lambda item: item[0], reverse=True)
    selection_info["discovered_keyword_dbs"] = [str(item[1]) for item in ranked]
    selection_info["discovered_candidate_count"] = len(ranked)

    if include_discovery:
        selected: list[str] = []
        if official_has_payload:
            selected.append(str(DEFAULT_KEYWORD_DB))
        selected.extend(str(item[1]) for item in ranked if str(item[1]) not in selected)
        if not selected:
            return [], selection_info
        selection_info["mode"] = "official_plus_discovery" if official_has_payload else "discovery_only"
        selection_info["selected_keyword_dbs"] = selected
        selection_info["fallback_keyword_db"] = str(ranked[0][1]) if ranked and not official_has_payload else None
        return selected, selection_info

    if official_has_payload:
        selection_info["mode"] = "official_only"
        selection_info["selected_keyword_dbs"] = [str(DEFAULT_KEYWORD_DB)]
        return [str(DEFAULT_KEYWORD_DB)], selection_info

    if not ranked:
        return [], selection_info

    selection_info["recommended_fallback_keyword_db"] = str(ranked[0][1])
    selection_info["recommended_fallback_stats"] = ranked[0][2]
    if not allow_legacy_fallback:
        selection_info["mode"] = f"{selection_info['mode']}_no_fallback"
        selection_info["fallback_disabled_reason"] = "official keyword db has no payload and legacy fallback is not enabled"
        return [], selection_info

    best_path = ranked[0][1]
    selection_info["mode"] = "legacy_fallback"
    selection_info["selected_keyword_dbs"] = [str(best_path)]
    selection_info["fallback_keyword_db"] = str(best_path)
    selection_info["fallback_stats"] = ranked[0][2]
    return [str(best_path)], selection_info


class AnalyticsWarehouseBuilder:
    def __init__(self, warehouse_db: Path | str):
        self.warehouse_db = Path(str(warehouse_db)).expanduser()
        self.db_config: DatabaseConfig = get_warehouse_database_config(self.warehouse_db)
        if self.db_config.is_sqlite:
            self.db_config.sqlite_path_or_raise("analytics warehouse").parent.mkdir(parents=True, exist_ok=True)
        self.conn = self._connect()
        self._init_schema()

    @property
    def backend(self) -> str:
        return self.db_config.backend

    @property
    def is_sqlite(self) -> bool:
        return self.db_config.is_sqlite

    @property
    def is_postgres(self) -> bool:
        return self.db_config.is_postgres

    def _connect(self):
        if self.db_config.is_sqlite:
            return connect_sqlite_db(self.db_config.sqlite_path_or_raise("analytics warehouse"))
        return psycopg.connect(
            self.db_config.dsn,
            row_factory=dict_row,
            autocommit=False,
        )

    def _adapt_sql(self, sql: str) -> str:
        if self.is_sqlite:
            return sql
        return sql.replace("?", "%s")

    def _execute(self, sql: str, params: tuple | list = ()):
        return self.conn.execute(self._adapt_sql(sql), tuple(params))

    def _fetchone(self, sql: str, params: tuple | list = ()):
        return self._execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: tuple | list = ()) -> list[Any]:
        return self._execute(sql, params).fetchall()

    def _scalar(self, sql: str, params: tuple | list = ()) -> Any:
        return _first_value(self._fetchone(sql, params))

    def _commit(self) -> None:
        self.conn.commit()

    def _rollback(self) -> None:
        self.conn.rollback()

    def _sync_postgres_identity_sequences(self) -> None:
        if not self.is_postgres:
            return
        sequence_tables = (
            "observation_events",
            "product_keyword_rank_events",
            "keyword_runs_log",
            "keyword_metric_snapshots",
            "keyword_expansion_edges",
        )
        for table_name in sequence_tables:
            self._execute(
                """
                SELECT setval(
                    pg_get_serial_sequence(%s, 'id'),
                    GREATEST(COALESCE((SELECT MAX(id) FROM """ + table_name + """), 0), 1),
                    COALESCE((SELECT MAX(id) FROM """ + table_name + """), 0) > 0
                )
                """,
                (table_name,),
            )

    def _create_table(self, sql: str) -> None:
        ddl = sql
        if self.is_postgres:
            ddl = ddl.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        self._execute(ddl)

    def close(self):
        if self.conn:
            self.conn.close()

    def reset(self):
        if self.is_sqlite:
            self.close()
            target_path = self.db_config.sqlite_path_or_raise("analytics warehouse")
            if target_path.exists():
                target_path.unlink()
            self.conn = self._connect()
            self._init_schema()
            return

        for view_name in (
            "vw_product_summary",
            "vw_product_signal_memory",
            "vw_product_source_coverage",
            "vw_latest_product_observation",
        ):
            self._execute(f"DROP VIEW IF EXISTS {view_name}")

        for table_name in (
            "keyword_expansion_edges",
            "keyword_metric_snapshots",
            "keyword_runs_log",
            "keyword_catalog",
            "product_keyword_rank_events",
            "product_keyword_membership",
            "product_category_membership",
            "observation_events",
            "product_identity",
            "source_databases",
        ):
            self._execute(f"DROP TABLE IF EXISTS {table_name}")
        self._commit()
        self._init_schema()

    def _init_schema(self):
        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS source_databases (
                source_label TEXT PRIMARY KEY,
                db_path TEXT NOT NULL,
                source_scope TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                source_product_count INTEGER DEFAULT 0,
                source_observation_count INTEGER DEFAULT 0,
                source_keyword_count INTEGER DEFAULT 0
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS product_identity (
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                title TEXT,
                brand TEXT,
                seller_name TEXT,
                product_url TEXT,
                image_url TEXT,
                latest_category_path TEXT,
                last_public_signals_json TEXT,
                first_seen TEXT,
                last_seen TEXT,
                is_active INTEGER DEFAULT 1,
                PRIMARY KEY (platform, product_id)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS observation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                snapshot_id TEXT,
                source_type TEXT NOT NULL,
                source_value TEXT NOT NULL DEFAULT '',
                category_name TEXT,
                scraped_at TEXT NOT NULL,
                price REAL,
                original_price REAL,
                currency TEXT DEFAULT 'SAR',
                rating REAL,
                review_count INTEGER,
                search_rank INTEGER,
                bsr_rank INTEGER,
                seller_name TEXT,
                delivery_type TEXT,
                is_express INTEGER DEFAULT 0,
                is_bestseller INTEGER DEFAULT 0,
                is_ad INTEGER DEFAULT 0,
                sold_recently_count INTEGER,
                sold_recently_text TEXT,
                stock_left_count INTEGER,
                stock_signal_text TEXT,
                delivery_eta_signal_text TEXT,
                lowest_price_signal_text TEXT,
                ranking_signal_text TEXT,
                badge_texts_json TEXT,
                public_signals_json TEXT,
                category_path TEXT,
                product_url TEXT,
                origin_db_label TEXT,
                UNIQUE(platform, product_id, source_type, source_value, scraped_at)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS product_category_membership (
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                category_name TEXT NOT NULL DEFAULT '',
                category_path TEXT NOT NULL DEFAULT '',
                first_seen TEXT,
                last_seen TEXT,
                source_db_label TEXT,
                PRIMARY KEY (platform, product_id, category_name, category_path)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS product_keyword_membership (
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                keyword TEXT NOT NULL,
                first_seen TEXT,
                last_seen TEXT,
                source_db_label TEXT,
                PRIMARY KEY (platform, product_id, keyword)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS product_keyword_rank_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                product_id TEXT NOT NULL,
                keyword TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                rank_position INTEGER,
                rank_type TEXT NOT NULL DEFAULT 'organic',
                source_platform TEXT,
                snapshot_id TEXT,
                source_db_label TEXT,
                UNIQUE(platform, product_id, keyword, observed_at, rank_type, source_platform)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keyword_catalog (
                keyword TEXT PRIMARY KEY,
                display_keyword TEXT,
                status TEXT,
                tracking_mode TEXT,
                source_type TEXT,
                source_platform TEXT,
                priority INTEGER,
                notes TEXT,
                metadata_json TEXT,
                first_seen TEXT,
                last_seen TEXT,
                last_crawled_at TEXT,
                last_analyzed_at TEXT,
                last_snapshot_id TEXT,
                origin_db_label TEXT
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keyword_runs_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_db_label TEXT NOT NULL,
                source_run_id INTEGER NOT NULL,
                run_type TEXT NOT NULL,
                trigger_mode TEXT,
                seed_keyword TEXT,
                snapshot_id TEXT,
                platforms_json TEXT,
                status TEXT,
                keyword_count INTEGER,
                metadata_json TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                UNIQUE(source_db_label, source_run_id)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keyword_metric_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_db_label TEXT NOT NULL,
                keyword TEXT NOT NULL,
                snapshot_id TEXT,
                analyzed_at TEXT NOT NULL,
                grade TEXT,
                rank INTEGER,
                total_score REAL,
                demand_index REAL,
                competition_density REAL,
                supply_gap_ratio REAL,
                price_safety_ratio REAL,
                margin_war_pct REAL,
                margin_peace_pct REAL,
                noon_total INTEGER,
                noon_express INTEGER,
                noon_median_price REAL,
                amazon_total INTEGER,
                amazon_median_price REAL,
                amazon_bsr_count INTEGER,
                google_interest REAL,
                v6_category TEXT,
                payload_json TEXT,
                UNIQUE(source_db_label, keyword, snapshot_id, analyzed_at)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keyword_expansion_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_db_label TEXT NOT NULL,
                parent_keyword TEXT NOT NULL,
                child_keyword TEXT NOT NULL,
                source_platform TEXT,
                source_type TEXT,
                source_run_id INTEGER,
                discovered_at TEXT NOT NULL,
                UNIQUE(
                    source_db_label,
                    parent_keyword,
                    child_keyword,
                    source_platform,
                    source_type,
                    discovered_at
                )
            )
            """
        )

        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_observation_lookup ON observation_events(platform, product_id, scraped_at DESC)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_category_membership_lookup ON product_category_membership(category_name, category_path)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_keyword_membership_lookup ON product_keyword_membership(keyword)"
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_keyword_rank_events_lookup
            ON product_keyword_rank_events(platform, product_id, observed_at DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_keyword_rank_events_keyword
            ON product_keyword_rank_events(keyword, observed_at DESC)
            """
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_keyword_runs_lookup ON keyword_runs_log(started_at DESC)"
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_keyword_expansion_parent
            ON keyword_expansion_edges(parent_keyword, source_platform, discovered_at DESC)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_keyword_expansion_child
            ON keyword_expansion_edges(child_keyword, source_platform, discovered_at DESC)
            """
        )
        self._execute(
            """
            INSERT INTO product_keyword_rank_events (
                platform,
                product_id,
                keyword,
                observed_at,
                rank_position,
                rank_type,
                source_platform,
                snapshot_id,
                source_db_label
            )
            SELECT
                oe.platform,
                oe.product_id,
                oe.source_value,
                oe.scraped_at,
                oe.search_rank,
                CASE WHEN COALESCE(oe.is_ad, 0) = 1 THEN 'ad' ELSE 'organic' END,
                oe.platform,
                oe.snapshot_id,
                oe.origin_db_label
            FROM observation_events oe
            WHERE oe.source_type = 'keyword'
              AND COALESCE(oe.source_value, '') <> ''
            ON CONFLICT(platform, product_id, keyword, observed_at, rank_type, source_platform) DO UPDATE SET
                rank_position = COALESCE(excluded.rank_position, product_keyword_rank_events.rank_position),
                snapshot_id = COALESCE(excluded.snapshot_id, product_keyword_rank_events.snapshot_id),
                source_db_label = COALESCE(excluded.source_db_label, product_keyword_rank_events.source_db_label)
            """
        )

        self._sync_postgres_identity_sequences()

        self._execute("DROP VIEW IF EXISTS vw_product_summary")
        self._execute("DROP VIEW IF EXISTS vw_product_signal_memory")
        self._execute("DROP VIEW IF EXISTS vw_product_source_coverage")
        self._execute("DROP VIEW IF EXISTS vw_latest_product_observation")
        self._execute(
            """
            CREATE VIEW vw_latest_product_observation AS
            WITH ranked AS (
                SELECT oe.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY oe.platform, oe.product_id
                           ORDER BY oe.scraped_at DESC, oe.id DESC
                       ) AS rn
                FROM observation_events oe
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            """
        )

        self._execute(
            """
            CREATE VIEW vw_product_source_coverage AS
            SELECT
                platform,
                product_id,
                COUNT(*) AS observation_count,
                COUNT(DISTINCT source_type) AS source_type_count,
                MAX(CASE WHEN source_type = 'category' THEN 1 ELSE 0 END) AS has_category,
                MAX(CASE WHEN source_type = 'keyword' THEN 1 ELSE 0 END) AS has_keyword,
                MIN(scraped_at) AS first_observed_at,
                MAX(scraped_at) AS last_observed_at
            FROM observation_events
            GROUP BY platform, product_id
            """
        )

        self._execute(
            """
            CREATE VIEW vw_product_signal_memory AS
            SELECT
                pi.platform,
                pi.product_id,
                (
                    SELECT oe.sold_recently_text
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.sold_recently_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_sold_recently_text,
                (
                    SELECT oe.scraped_at
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.sold_recently_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_sold_recently_seen_at,
                (
                    SELECT oe.stock_signal_text
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.stock_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_stock_signal_text,
                (
                    SELECT oe.scraped_at
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.stock_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_stock_signal_seen_at,
                (
                    SELECT oe.lowest_price_signal_text
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.lowest_price_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_lowest_price_signal_text,
                (
                    SELECT oe.scraped_at
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.lowest_price_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_lowest_price_signal_seen_at,
                (
                    SELECT oe.ranking_signal_text
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.ranking_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_ranking_signal_text,
                (
                    SELECT oe.scraped_at
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.ranking_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_ranking_signal_seen_at,
                (
                    SELECT oe.delivery_eta_signal_text
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.delivery_eta_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_delivery_eta_signal_text,
                (
                    SELECT oe.scraped_at
                    FROM observation_events oe
                    WHERE oe.platform = pi.platform
                      AND oe.product_id = pi.product_id
                      AND COALESCE(oe.delivery_eta_signal_text, '') <> ''
                    ORDER BY oe.scraped_at DESC, oe.id DESC
                    LIMIT 1
                ) AS sticky_delivery_eta_signal_seen_at
            FROM product_identity pi
            """
        )

        self._execute(
            f"""
            CREATE VIEW vw_product_summary AS
            SELECT
                pi.platform,
                pi.product_id,
                pi.title,
                pi.brand,
                pi.seller_name,
                pi.product_url,
                pi.image_url,
                pi.latest_category_path,
                pi.first_seen,
                pi.last_seen,
                pi.is_active,
                coverage.observation_count,
                coverage.source_type_count,
                coverage.has_category,
                coverage.has_keyword,
                coverage.first_observed_at,
                coverage.last_observed_at,
                latest.source_type AS latest_source_type,
                latest.source_value AS latest_source_value,
                latest.scraped_at AS latest_observed_at,
                latest.price AS latest_price,
                latest.original_price AS latest_original_price,
                latest.currency AS latest_currency,
                latest.rating AS latest_rating,
                latest.review_count AS latest_review_count,
                latest.sold_recently_count AS latest_sold_recently_count,
                latest.stock_left_count AS latest_stock_left_count,
                latest.search_rank AS latest_search_rank,
                latest.bsr_rank AS latest_bsr_rank,
                latest.delivery_type AS latest_delivery_type,
                latest.is_ad AS latest_is_ad,
                latest.is_express AS latest_is_express,
                latest.is_bestseller AS latest_is_bestseller,
                latest.delivery_eta_signal_text AS latest_delivery_eta_signal_text,
                latest.lowest_price_signal_text AS latest_lowest_price_signal_text,
                latest.sold_recently_text AS latest_sold_recently_text,
                latest.stock_signal_text AS latest_stock_signal_text,
                latest.ranking_signal_text AS latest_ranking_signal_text,
                latest.category_path AS latest_observed_category_path,
                signal_memory.sticky_delivery_eta_signal_text,
                signal_memory.sticky_delivery_eta_signal_seen_at,
                signal_memory.sticky_lowest_price_signal_text,
                signal_memory.sticky_lowest_price_signal_seen_at,
                signal_memory.sticky_sold_recently_text,
                signal_memory.sticky_sold_recently_seen_at,
                signal_memory.sticky_stock_signal_text,
                signal_memory.sticky_stock_signal_seen_at,
                signal_memory.sticky_ranking_signal_text,
                signal_memory.sticky_ranking_signal_seen_at,
                COALESCE(
                    latest.sold_recently_count,
                    (
                        SELECT oe.sold_recently_count
                        FROM observation_events oe
                        WHERE oe.platform = pi.platform
                          AND oe.product_id = pi.product_id
                          AND oe.sold_recently_count IS NOT NULL
                        ORDER BY oe.scraped_at DESC, oe.id DESC
                        LIMIT 1
                    )
                ) AS monthly_sales_estimate,
                COALESCE(
                    latest.stock_left_count,
                    (
                        SELECT oe.stock_left_count
                        FROM observation_events oe
                        WHERE oe.platform = pi.platform
                          AND oe.product_id = pi.product_id
                          AND oe.stock_left_count IS NOT NULL
                        ORDER BY oe.scraped_at DESC, oe.id DESC
                        LIMIT 1
                    )
                ) AS inventory_left_estimate,
                CASE
                    WHEN COALESCE(
                        latest.stock_left_count,
                        (
                            SELECT oe.stock_left_count
                            FROM observation_events oe
                            WHERE oe.platform = pi.platform
                              AND oe.product_id = pi.product_id
                              AND oe.stock_left_count IS NOT NULL
                            ORDER BY oe.scraped_at DESC, oe.id DESC
                            LIMIT 1
                        )
                    ) IS NOT NULL
                     AND COALESCE(
                        latest.stock_left_count,
                        (
                            SELECT oe.stock_left_count
                            FROM observation_events oe
                            WHERE oe.platform = pi.platform
                              AND oe.product_id = pi.product_id
                              AND oe.stock_left_count IS NOT NULL
                            ORDER BY oe.scraped_at DESC, oe.id DESC
                            LIMIT 1
                        )
                    ) <= 5 THEN 'critical'
                    WHEN COALESCE(
                        latest.stock_left_count,
                        (
                            SELECT oe.stock_left_count
                            FROM observation_events oe
                            WHERE oe.platform = pi.platform
                              AND oe.product_id = pi.product_id
                              AND oe.stock_left_count IS NOT NULL
                            ORDER BY oe.scraped_at DESC, oe.id DESC
                            LIMIT 1
                        )
                    ) IS NOT NULL
                     AND COALESCE(
                        latest.stock_left_count,
                        (
                            SELECT oe.stock_left_count
                            FROM observation_events oe
                            WHERE oe.platform = pi.platform
                              AND oe.product_id = pi.product_id
                              AND oe.stock_left_count IS NOT NULL
                            ORDER BY oe.scraped_at DESC, oe.id DESC
                            LIMIT 1
                        )
                    ) <= 20 THEN 'low'
                    WHEN COALESCE(NULLIF(latest.stock_signal_text, ''), NULLIF(signal_memory.sticky_stock_signal_text, ''), '') <> '' THEN 'signaled'
                    ELSE ''
                END AS inventory_signal_bucket,
                {sql_growth_metric("review_count", 7, self.backend).strip()},
                {sql_growth_metric("review_count", 14, self.backend).strip()},
                {sql_growth_metric("rating", 7, self.backend).strip()},
                {sql_growth_metric("rating", 14, self.backend).strip()}
            FROM product_identity pi
            LEFT JOIN vw_product_source_coverage coverage
              ON coverage.platform = pi.platform AND coverage.product_id = pi.product_id
            LEFT JOIN vw_latest_product_observation latest
              ON latest.platform = pi.platform AND latest.product_id = pi.product_id
            LEFT JOIN vw_product_signal_memory signal_memory
              ON signal_memory.platform = pi.platform AND signal_memory.product_id = pi.product_id
            """
        )

        self._commit()

    def _upsert_source_database(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO source_databases (
                source_label, db_path, source_scope, imported_at,
                source_product_count, source_observation_count, source_keyword_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_label) DO UPDATE SET
                db_path = excluded.db_path,
                source_scope = excluded.source_scope,
                imported_at = excluded.imported_at,
                source_product_count = excluded.source_product_count,
                source_observation_count = excluded.source_observation_count,
                source_keyword_count = excluded.source_keyword_count
            """,
            (
                payload["source_label"],
                payload["db_path"],
                payload["source_scope"],
                payload["imported_at"],
                payload["source_product_count"],
                payload["source_observation_count"],
                payload["source_keyword_count"],
            ),
        )

    def _get_previous_imported_at(self, source_label: str) -> str | None:
        row = self._fetchone(
            """
            SELECT imported_at
            FROM source_databases
            WHERE source_label = ?
            LIMIT 1
            """,
            (source_label,),
        )
        if row is None:
            return None
        if isinstance(row, dict):
            value = row.get("imported_at")
        else:
            value = row["imported_at"]
        normalized = str(value or "").strip()
        return normalized or None

    def _upsert_product_identity(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO product_identity (
                platform, product_id, title, brand, seller_name, product_url, image_url,
                latest_category_path, last_public_signals_json, first_seen, last_seen, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, product_id) DO UPDATE SET
                title = CASE WHEN COALESCE(excluded.title, '') <> '' THEN excluded.title ELSE product_identity.title END,
                brand = CASE WHEN COALESCE(excluded.brand, '') <> '' THEN excluded.brand ELSE product_identity.brand END,
                seller_name = CASE WHEN COALESCE(excluded.seller_name, '') <> '' THEN excluded.seller_name ELSE product_identity.seller_name END,
                product_url = CASE WHEN COALESCE(excluded.product_url, '') <> '' THEN excluded.product_url ELSE product_identity.product_url END,
                image_url = CASE WHEN COALESCE(excluded.image_url, '') <> '' THEN excluded.image_url ELSE product_identity.image_url END,
                latest_category_path = CASE
                    WHEN COALESCE(excluded.latest_category_path, '') <> '' THEN excluded.latest_category_path
                    ELSE product_identity.latest_category_path
                END,
                last_public_signals_json = CASE
                    WHEN COALESCE(excluded.last_public_signals_json, '') <> '' THEN excluded.last_public_signals_json
                    ELSE product_identity.last_public_signals_json
                END,
                first_seen = CASE
                    WHEN product_identity.first_seen IS NULL THEN excluded.first_seen
                    WHEN excluded.first_seen IS NULL THEN product_identity.first_seen
                    WHEN excluded.first_seen < product_identity.first_seen THEN excluded.first_seen
                    ELSE product_identity.first_seen
                END,
                last_seen = CASE
                    WHEN product_identity.last_seen IS NULL THEN excluded.last_seen
                    WHEN excluded.last_seen IS NULL THEN product_identity.last_seen
                    WHEN excluded.last_seen > product_identity.last_seen THEN excluded.last_seen
                    ELSE product_identity.last_seen
                END,
                is_active = excluded.is_active
            """,
            (
                payload["platform"],
                payload["product_id"],
                payload.get("title"),
                payload.get("brand"),
                payload.get("seller_name"),
                payload.get("product_url"),
                payload.get("image_url"),
                payload.get("latest_category_path"),
                payload.get("last_public_signals_json"),
                payload.get("first_seen"),
                payload.get("last_seen"),
                int(bool(payload.get("is_active", 1))),
            ),
        )

    def _upsert_observation_event(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO observation_events (
                platform, product_id, snapshot_id, source_type, source_value, category_name,
                scraped_at, price, original_price, currency, rating, review_count, search_rank,
                bsr_rank, seller_name, delivery_type, is_express, is_bestseller, is_ad,
                sold_recently_count, sold_recently_text, stock_left_count, stock_signal_text,
                delivery_eta_signal_text, lowest_price_signal_text, ranking_signal_text,
                badge_texts_json, public_signals_json, category_path, product_url, origin_db_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, product_id, source_type, source_value, scraped_at) DO UPDATE SET
                snapshot_id = excluded.snapshot_id,
                category_name = COALESCE(NULLIF(excluded.category_name, ''), observation_events.category_name),
                price = COALESCE(excluded.price, observation_events.price),
                original_price = COALESCE(excluded.original_price, observation_events.original_price),
                currency = COALESCE(NULLIF(excluded.currency, ''), observation_events.currency),
                rating = COALESCE(excluded.rating, observation_events.rating),
                review_count = COALESCE(excluded.review_count, observation_events.review_count),
                search_rank = COALESCE(excluded.search_rank, observation_events.search_rank),
                bsr_rank = COALESCE(excluded.bsr_rank, observation_events.bsr_rank),
                seller_name = COALESCE(NULLIF(excluded.seller_name, ''), observation_events.seller_name),
                delivery_type = COALESCE(NULLIF(excluded.delivery_type, ''), observation_events.delivery_type),
                is_express = CASE WHEN excluded.is_express = 1 THEN 1 ELSE observation_events.is_express END,
                is_bestseller = CASE WHEN excluded.is_bestseller = 1 THEN 1 ELSE observation_events.is_bestseller END,
                is_ad = CASE WHEN excluded.is_ad = 1 THEN 1 ELSE observation_events.is_ad END,
                sold_recently_count = COALESCE(excluded.sold_recently_count, observation_events.sold_recently_count),
                sold_recently_text = COALESCE(NULLIF(excluded.sold_recently_text, ''), observation_events.sold_recently_text),
                stock_left_count = COALESCE(excluded.stock_left_count, observation_events.stock_left_count),
                stock_signal_text = COALESCE(NULLIF(excluded.stock_signal_text, ''), observation_events.stock_signal_text),
                delivery_eta_signal_text = COALESCE(NULLIF(excluded.delivery_eta_signal_text, ''), observation_events.delivery_eta_signal_text),
                lowest_price_signal_text = COALESCE(NULLIF(excluded.lowest_price_signal_text, ''), observation_events.lowest_price_signal_text),
                ranking_signal_text = COALESCE(NULLIF(excluded.ranking_signal_text, ''), observation_events.ranking_signal_text),
                badge_texts_json = COALESCE(NULLIF(excluded.badge_texts_json, ''), observation_events.badge_texts_json),
                public_signals_json = COALESCE(NULLIF(excluded.public_signals_json, ''), observation_events.public_signals_json),
                category_path = COALESCE(NULLIF(excluded.category_path, ''), observation_events.category_path),
                product_url = COALESCE(NULLIF(excluded.product_url, ''), observation_events.product_url),
                origin_db_label = excluded.origin_db_label
            """,
            (
                payload["platform"],
                payload["product_id"],
                payload.get("snapshot_id"),
                payload["source_type"],
                payload.get("source_value", ""),
                payload.get("category_name"),
                payload["scraped_at"],
                payload.get("price"),
                payload.get("original_price"),
                payload.get("currency", "SAR"),
                payload.get("rating"),
                payload.get("review_count"),
                payload.get("search_rank"),
                payload.get("bsr_rank"),
                payload.get("seller_name"),
                payload.get("delivery_type"),
                int(bool(payload.get("is_express"))),
                int(bool(payload.get("is_bestseller"))),
                int(bool(payload.get("is_ad"))),
                payload.get("sold_recently_count"),
                payload.get("sold_recently_text"),
                payload.get("stock_left_count"),
                payload.get("stock_signal_text"),
                payload.get("delivery_eta_signal_text"),
                payload.get("lowest_price_signal_text"),
                payload.get("ranking_signal_text"),
                payload.get("badge_texts_json"),
                payload.get("public_signals_json"),
                payload.get("category_path"),
                payload.get("product_url"),
                payload.get("origin_db_label"),
            ),
        )

    def _upsert_category_membership(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO product_category_membership (
                platform, product_id, category_name, category_path, first_seen, last_seen, source_db_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, product_id, category_name, category_path) DO UPDATE SET
                first_seen = CASE
                    WHEN product_category_membership.first_seen IS NULL THEN excluded.first_seen
                    WHEN excluded.first_seen IS NULL THEN product_category_membership.first_seen
                    WHEN excluded.first_seen < product_category_membership.first_seen THEN excluded.first_seen
                    ELSE product_category_membership.first_seen
                END,
                last_seen = CASE
                    WHEN product_category_membership.last_seen IS NULL THEN excluded.last_seen
                    WHEN excluded.last_seen IS NULL THEN product_category_membership.last_seen
                    WHEN excluded.last_seen > product_category_membership.last_seen THEN excluded.last_seen
                    ELSE product_category_membership.last_seen
                END,
                source_db_label = excluded.source_db_label
            """,
            (
                payload["platform"],
                payload["product_id"],
                payload.get("category_name", ""),
                payload.get("category_path", ""),
                payload.get("first_seen"),
                payload.get("last_seen"),
                payload.get("source_db_label"),
            ),
        )

    def _upsert_keyword_membership(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO product_keyword_membership (
                platform, product_id, keyword, first_seen, last_seen, source_db_label
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, product_id, keyword) DO UPDATE SET
                first_seen = CASE
                    WHEN product_keyword_membership.first_seen IS NULL THEN excluded.first_seen
                    WHEN excluded.first_seen IS NULL THEN product_keyword_membership.first_seen
                    WHEN excluded.first_seen < product_keyword_membership.first_seen THEN excluded.first_seen
                    ELSE product_keyword_membership.first_seen
                END,
                last_seen = CASE
                    WHEN product_keyword_membership.last_seen IS NULL THEN excluded.last_seen
                    WHEN excluded.last_seen IS NULL THEN product_keyword_membership.last_seen
                    WHEN excluded.last_seen > product_keyword_membership.last_seen THEN excluded.last_seen
                    ELSE product_keyword_membership.last_seen
                END,
                source_db_label = excluded.source_db_label
            """,
            (
                payload["platform"],
                payload["product_id"],
                payload["keyword"],
                payload.get("first_seen"),
                payload.get("last_seen"),
                payload.get("source_db_label"),
            ),
        )

    def _upsert_keyword_rank_event(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO product_keyword_rank_events (
                platform,
                product_id,
                keyword,
                observed_at,
                rank_position,
                rank_type,
                source_platform,
                snapshot_id,
                source_db_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, product_id, keyword, observed_at, rank_type, source_platform) DO UPDATE SET
                rank_position = COALESCE(excluded.rank_position, product_keyword_rank_events.rank_position),
                snapshot_id = COALESCE(excluded.snapshot_id, product_keyword_rank_events.snapshot_id),
                source_db_label = COALESCE(excluded.source_db_label, product_keyword_rank_events.source_db_label)
            """,
            (
                payload["platform"],
                payload["product_id"],
                payload["keyword"],
                payload["observed_at"],
                payload.get("rank_position"),
                payload.get("rank_type") or "organic",
                payload.get("source_platform") or payload["platform"],
                payload.get("snapshot_id"),
                payload.get("source_db_label"),
            ),
        )

    def _upsert_keyword_catalog(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO keyword_catalog (
                keyword, display_keyword, status, tracking_mode, source_type, source_platform, priority,
                notes, metadata_json, first_seen, last_seen, last_crawled_at, last_analyzed_at,
                last_snapshot_id, origin_db_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(keyword) DO UPDATE SET
                display_keyword = COALESCE(NULLIF(excluded.display_keyword, ''), keyword_catalog.display_keyword),
                status = COALESCE(NULLIF(excluded.status, ''), keyword_catalog.status),
                tracking_mode = COALESCE(NULLIF(excluded.tracking_mode, ''), keyword_catalog.tracking_mode),
                source_type = COALESCE(NULLIF(excluded.source_type, ''), keyword_catalog.source_type),
                source_platform = COALESCE(NULLIF(excluded.source_platform, ''), keyword_catalog.source_platform),
                priority = COALESCE(excluded.priority, keyword_catalog.priority),
                notes = COALESCE(NULLIF(excluded.notes, ''), keyword_catalog.notes),
                metadata_json = COALESCE(NULLIF(excluded.metadata_json, ''), keyword_catalog.metadata_json),
                first_seen = CASE
                    WHEN keyword_catalog.first_seen IS NULL THEN excluded.first_seen
                    WHEN excluded.first_seen IS NULL THEN keyword_catalog.first_seen
                    WHEN excluded.first_seen < keyword_catalog.first_seen THEN excluded.first_seen
                    ELSE keyword_catalog.first_seen
                END,
                last_seen = CASE
                    WHEN keyword_catalog.last_seen IS NULL THEN excluded.last_seen
                    WHEN excluded.last_seen IS NULL THEN keyword_catalog.last_seen
                    WHEN excluded.last_seen > keyword_catalog.last_seen THEN excluded.last_seen
                    ELSE keyword_catalog.last_seen
                END,
                last_crawled_at = COALESCE(excluded.last_crawled_at, keyword_catalog.last_crawled_at),
                last_analyzed_at = COALESCE(excluded.last_analyzed_at, keyword_catalog.last_analyzed_at),
                last_snapshot_id = COALESCE(NULLIF(excluded.last_snapshot_id, ''), keyword_catalog.last_snapshot_id),
                origin_db_label = excluded.origin_db_label
            """,
            (
                payload["keyword"],
                payload.get("display_keyword"),
                payload.get("status"),
                payload.get("tracking_mode"),
                payload.get("source_type"),
                payload.get("source_platform"),
                payload.get("priority"),
                payload.get("notes"),
                payload.get("metadata_json"),
                payload.get("first_seen"),
                payload.get("last_seen"),
                payload.get("last_crawled_at"),
                payload.get("last_analyzed_at"),
                payload.get("last_snapshot_id"),
                payload.get("origin_db_label"),
            ),
        )

    def _upsert_keyword_run(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO keyword_runs_log (
                source_db_label, source_run_id, run_type, trigger_mode, seed_keyword, snapshot_id,
                platforms_json, status, keyword_count, metadata_json, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_db_label, source_run_id) DO UPDATE SET
                run_type = excluded.run_type,
                trigger_mode = excluded.trigger_mode,
                seed_keyword = excluded.seed_keyword,
                snapshot_id = excluded.snapshot_id,
                platforms_json = excluded.platforms_json,
                status = excluded.status,
                keyword_count = excluded.keyword_count,
                metadata_json = excluded.metadata_json,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at
            """,
            (
                payload["source_db_label"],
                payload["source_run_id"],
                payload["run_type"],
                payload.get("trigger_mode"),
                payload.get("seed_keyword"),
                payload.get("snapshot_id"),
                payload.get("platforms_json"),
                payload.get("status"),
                payload.get("keyword_count"),
                payload.get("metadata_json"),
                payload.get("started_at"),
                payload.get("finished_at"),
            ),
        )

    def _upsert_keyword_metric_snapshot(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO keyword_metric_snapshots (
                source_db_label, keyword, snapshot_id, analyzed_at, grade, rank, total_score,
                demand_index, competition_density, supply_gap_ratio, price_safety_ratio, margin_war_pct,
                margin_peace_pct, noon_total, noon_express, noon_median_price, amazon_total,
                amazon_median_price, amazon_bsr_count, google_interest, v6_category, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_db_label, keyword, snapshot_id, analyzed_at) DO UPDATE SET
                grade = excluded.grade,
                rank = excluded.rank,
                total_score = excluded.total_score,
                demand_index = excluded.demand_index,
                competition_density = excluded.competition_density,
                supply_gap_ratio = excluded.supply_gap_ratio,
                price_safety_ratio = excluded.price_safety_ratio,
                margin_war_pct = excluded.margin_war_pct,
                margin_peace_pct = excluded.margin_peace_pct,
                noon_total = excluded.noon_total,
                noon_express = excluded.noon_express,
                noon_median_price = excluded.noon_median_price,
                amazon_total = excluded.amazon_total,
                amazon_median_price = excluded.amazon_median_price,
                amazon_bsr_count = excluded.amazon_bsr_count,
                google_interest = excluded.google_interest,
                v6_category = excluded.v6_category,
                payload_json = excluded.payload_json
            """,
            (
                payload["source_db_label"],
                payload["keyword"],
                payload.get("snapshot_id"),
                payload["analyzed_at"],
                payload.get("grade"),
                payload.get("rank"),
                payload.get("total_score"),
                payload.get("demand_index"),
                payload.get("competition_density"),
                payload.get("supply_gap_ratio"),
                payload.get("price_safety_ratio"),
                payload.get("margin_war_pct"),
                payload.get("margin_peace_pct"),
                payload.get("noon_total"),
                payload.get("noon_express"),
                payload.get("noon_median_price"),
                payload.get("amazon_total"),
                payload.get("amazon_median_price"),
                payload.get("amazon_bsr_count"),
                payload.get("google_interest"),
                payload.get("v6_category"),
                payload.get("payload_json"),
            ),
        )

    def _upsert_keyword_expansion_edge(self, payload: dict[str, Any]):
        self._execute(
            """
            INSERT INTO keyword_expansion_edges (
                source_db_label, parent_keyword, child_keyword, source_platform,
                source_type, source_run_id, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(
                source_db_label,
                parent_keyword,
                child_keyword,
                source_platform,
                source_type,
                discovered_at
            ) DO UPDATE SET
                source_run_id = COALESCE(excluded.source_run_id, keyword_expansion_edges.source_run_id)
            """,
            (
                payload["source_db_label"],
                payload["parent_keyword"],
                payload["child_keyword"],
                payload.get("source_platform"),
                payload.get("source_type"),
                payload.get("source_run_id"),
                payload["discovered_at"],
            ),
        )

    def import_stage_db(self, db_ref: str | Path) -> dict[str, Any]:
        source_conn, source_backend = connect_stage_database(db_ref)
        config = database_config_from_reference(db_ref)
        source_label = make_database_label(db_ref)
        previous_imported_at = self._get_previous_imported_at(source_label)
        summary = {
            "source_label": source_label,
            "db_path": str(db_ref),
            "source_scope": detect_scope(config.sqlite_path_or_raise("stage database")) if config.is_sqlite else "shared_stage",
            "imported_at": datetime.now().isoformat(),
            "source_product_count": 0,
            "source_observation_count": 0,
            "source_keyword_count": 0,
            "backend": source_backend,
            "incremental_since": previous_imported_at,
        }

        try:
            product_rows = []
            if table_exists(source_conn, "products", backend=source_backend):
                product_columns = get_table_columns(source_conn, "products", backend=source_backend)
                product_where, product_params = _build_since_clause(
                    backend=source_backend,
                    available_columns=product_columns,
                    timestamp_candidates=("last_seen", "first_seen"),
                    cutoff_iso=previous_imported_at,
                )
                product_rows = source_conn.execute(
                    """
                    SELECT product_id, platform, title, brand, seller_name, category_path, product_url,
                           image_url, last_public_signals_json, first_seen, last_seen, is_active
                    FROM products
                    """
                    + product_where,
                    product_params,
                ).fetchall()

            for row in product_rows:
                self._upsert_product_identity(
                    {
                        "platform": row["platform"] or "noon",
                        "product_id": row["product_id"],
                        "title": row["title"],
                        "brand": row["brand"],
                        "seller_name": row["seller_name"],
                        "product_url": row["product_url"],
                        "image_url": row["image_url"],
                        "latest_category_path": row["category_path"],
                        "last_public_signals_json": row["last_public_signals_json"],
                        "first_seen": row["first_seen"],
                        "last_seen": row["last_seen"],
                        "is_active": row["is_active"],
                    }
                )
            summary["source_product_count"] = len(product_rows)

            observation_rows = []
            if table_exists(source_conn, "crawl_observations", backend=source_backend):
                selected_columns = [
                    "product_id",
                    "platform",
                    "snapshot_id",
                    "source_type",
                    "source_value",
                    "category_name",
                    "scraped_at",
                    "price",
                    "original_price",
                    "currency",
                    "rating",
                    "review_count",
                    "search_rank",
                    "bsr_rank",
                    "seller_name",
                    "delivery_type",
                    "is_express",
                    "is_bestseller",
                    "is_ad",
                    "sold_recently_count",
                    "sold_recently_text",
                    "stock_left_count",
                    "stock_signal_text",
                    "delivery_eta_signal_text",
                    "lowest_price_signal_text",
                    "ranking_signal_text",
                    "badge_texts_json",
                    "public_signals_json",
                    "category_path",
                    "product_url",
                ]
                observation_columns = get_table_columns(source_conn, "crawl_observations", backend=source_backend)
                available_columns = [col for col in selected_columns if col in observation_columns]
                observation_where, observation_params = _build_since_clause(
                    backend=source_backend,
                    available_columns=observation_columns,
                    timestamp_candidates=("scraped_at",),
                    cutoff_iso=previous_imported_at,
                )
                observation_rows = source_conn.execute(
                    f"SELECT {', '.join(available_columns)} FROM crawl_observations{observation_where}",
                    observation_params,
                ).fetchall()

            for row in observation_rows:
                payload = {key: row[key] for key in row.keys()}
                payload["platform"] = payload.get("platform") or "noon"
                payload["source_value"] = payload.get("source_value") or ""
                payload["origin_db_label"] = source_label
                self._upsert_observation_event(payload)

                if payload.get("source_type") == "category":
                    self._upsert_category_membership(
                        {
                            "platform": payload["platform"],
                            "product_id": payload["product_id"],
                            "category_name": payload.get("category_name") or "",
                            "category_path": payload.get("category_path") or "",
                            "first_seen": payload.get("scraped_at"),
                            "last_seen": payload.get("scraped_at"),
                            "source_db_label": source_label,
                        }
                    )
                elif payload.get("source_type") == "keyword" and payload.get("source_value"):
                    self._upsert_keyword_membership(
                        {
                            "platform": payload["platform"],
                            "product_id": payload["product_id"],
                            "keyword": payload["source_value"],
                            "first_seen": payload.get("scraped_at"),
                            "last_seen": payload.get("scraped_at"),
                            "source_db_label": source_label,
                        }
                    )
                    self._upsert_keyword_rank_event(
                        {
                            "platform": payload["platform"],
                            "product_id": payload["product_id"],
                            "keyword": payload["source_value"],
                            "observed_at": payload.get("scraped_at"),
                            "rank_position": payload.get("search_rank"),
                            "rank_type": "ad" if int(bool(payload.get("is_ad"))) == 1 else "organic",
                            "source_platform": payload.get("platform"),
                            "snapshot_id": payload.get("snapshot_id"),
                            "source_db_label": source_label,
                        }
                    )
            summary["source_observation_count"] = len(observation_rows)

            if table_exists(source_conn, "keywords", backend=source_backend):
                keyword_columns = get_table_columns(source_conn, "keywords", backend=source_backend)
                keyword_where, keyword_params = _build_since_clause(
                    backend=source_backend,
                    available_columns=keyword_columns,
                    timestamp_candidates=("last_seen", "last_crawled_at", "last_analyzed_at", "first_seen"),
                    cutoff_iso=previous_imported_at,
                )
                keyword_rows = source_conn.execute(
                    """
                    SELECT keyword, display_keyword, status, tracking_mode, source_type, source_platform,
                           priority, notes, metadata_json, first_seen, last_seen, last_crawled_at,
                           last_analyzed_at, last_snapshot_id
                    FROM keywords
                    """
                    + keyword_where,
                    keyword_params,
                ).fetchall()
                for row in keyword_rows:
                    self._upsert_keyword_catalog(
                        {
                            "keyword": row["keyword"],
                            "display_keyword": row["display_keyword"],
                            "status": row["status"],
                            "tracking_mode": row["tracking_mode"],
                            "source_type": row["source_type"],
                            "source_platform": row["source_platform"],
                            "priority": row["priority"],
                            "notes": row["notes"],
                            "metadata_json": row["metadata_json"],
                            "first_seen": row["first_seen"],
                            "last_seen": row["last_seen"],
                            "last_crawled_at": row["last_crawled_at"],
                            "last_analyzed_at": row["last_analyzed_at"],
                            "last_snapshot_id": row["last_snapshot_id"],
                            "origin_db_label": source_label,
                        }
                    )
                summary["source_keyword_count"] = len(keyword_rows)

            if table_exists(source_conn, "keyword_runs", backend=source_backend):
                keyword_run_columns = get_table_columns(source_conn, "keyword_runs", backend=source_backend)
                keyword_run_where, keyword_run_params = _build_since_clause(
                    backend=source_backend,
                    available_columns=keyword_run_columns,
                    timestamp_candidates=("finished_at", "started_at"),
                    cutoff_iso=previous_imported_at,
                )
                for row in source_conn.execute(
                    """
                    SELECT id, run_type, trigger_mode, seed_keyword, snapshot_id, platforms_json,
                           status, keyword_count, metadata_json, started_at, finished_at
                    FROM keyword_runs
                    """
                    + keyword_run_where,
                    keyword_run_params,
                ).fetchall():
                    self._upsert_keyword_run(
                        {
                            "source_db_label": source_label,
                            "source_run_id": row["id"],
                            "run_type": row["run_type"],
                            "trigger_mode": row["trigger_mode"],
                            "seed_keyword": row["seed_keyword"],
                            "snapshot_id": row["snapshot_id"],
                            "platforms_json": row["platforms_json"],
                            "status": row["status"],
                            "keyword_count": row["keyword_count"],
                            "metadata_json": row["metadata_json"],
                            "started_at": row["started_at"],
                            "finished_at": row["finished_at"],
                        }
                    )

            if table_exists(source_conn, "keyword_metrics_snapshots", backend=source_backend):
                metric_columns = get_table_columns(source_conn, "keyword_metrics_snapshots", backend=source_backend)
                metric_where, metric_params = _build_since_clause(
                    backend=source_backend,
                    available_columns=metric_columns,
                    timestamp_candidates=("analyzed_at",),
                    cutoff_iso=previous_imported_at,
                )
                for row in source_conn.execute(
                    """
                    SELECT keyword, snapshot_id, analyzed_at, grade, rank, total_score, demand_index,
                           competition_density, supply_gap_ratio, price_safety_ratio, margin_war_pct,
                           margin_peace_pct, noon_total, noon_express, noon_median_price, amazon_total,
                           amazon_median_price, amazon_bsr_count, google_interest, v6_category, payload_json
                    FROM keyword_metrics_snapshots
                    """
                    + metric_where,
                    metric_params,
                ).fetchall():
                    self._upsert_keyword_metric_snapshot(
                        {
                            "source_db_label": source_label,
                            "keyword": row["keyword"],
                            "snapshot_id": row["snapshot_id"],
                            "analyzed_at": row["analyzed_at"],
                            "grade": row["grade"],
                            "rank": row["rank"],
                            "total_score": row["total_score"],
                            "demand_index": row["demand_index"],
                            "competition_density": row["competition_density"],
                            "supply_gap_ratio": row["supply_gap_ratio"],
                            "price_safety_ratio": row["price_safety_ratio"],
                            "margin_war_pct": row["margin_war_pct"],
                            "margin_peace_pct": row["margin_peace_pct"],
                            "noon_total": row["noon_total"],
                            "noon_express": row["noon_express"],
                            "noon_median_price": row["noon_median_price"],
                            "amazon_total": row["amazon_total"],
                            "amazon_median_price": row["amazon_median_price"],
                            "amazon_bsr_count": row["amazon_bsr_count"],
                            "google_interest": row["google_interest"],
                            "v6_category": row["v6_category"],
                            "payload_json": row["payload_json"],
                        }
                    )

            if table_exists(source_conn, "keyword_edges", backend=source_backend):
                edge_columns = get_table_columns(source_conn, "keyword_edges", backend=source_backend)
                edge_where, edge_params = _build_since_clause(
                    backend=source_backend,
                    available_columns=edge_columns,
                    timestamp_candidates=("discovered_at",),
                    cutoff_iso=previous_imported_at,
                )
                for row in source_conn.execute(
                    """
                    SELECT parent_keyword, child_keyword, source_platform, source_type, run_id, discovered_at
                    FROM keyword_edges
                    """
                    + edge_where,
                    edge_params,
                ).fetchall():
                    self._upsert_keyword_expansion_edge(
                        {
                            "source_db_label": source_label,
                            "parent_keyword": row["parent_keyword"],
                            "child_keyword": row["child_keyword"],
                            "source_platform": row["source_platform"],
                            "source_type": row["source_type"],
                            "source_run_id": row["run_id"],
                            "discovered_at": row["discovered_at"],
                        }
                    )

            self._upsert_source_database(summary)
            self._commit()
            return summary
        except Exception:
            self._rollback()
            raise
        finally:
            source_conn.close()

    def collect_warehouse_stats(self) -> dict[str, Any]:
        stats: dict[str, Any] = {}

        for table_name in (
            "source_databases",
            "product_identity",
            "observation_events",
            "product_category_membership",
            "product_keyword_membership",
            "product_keyword_rank_events",
            "keyword_catalog",
            "keyword_runs_log",
            "keyword_metric_snapshots",
            "keyword_expansion_edges",
        ):
            stats[table_name] = int(self._scalar(f"SELECT COUNT(*) FROM {table_name}") or 0)

        stats["cross_source_product_overlap"] = int(
            self._scalar(
            """
            SELECT COUNT(*) FROM (
                SELECT platform, product_id,
                       MAX(CASE WHEN source_type = 'category' THEN 1 ELSE 0 END) AS has_category,
                       MAX(CASE WHEN source_type = 'keyword' THEN 1 ELSE 0 END) AS has_keyword
                FROM observation_events
                GROUP BY platform, product_id
                HAVING MAX(CASE WHEN source_type = 'category' THEN 1 ELSE 0 END) = 1
                   AND MAX(CASE WHEN source_type = 'keyword' THEN 1 ELSE 0 END) = 1
            )
            """
            )
            or 0
        )

        stats["overlap_samples"] = [dict(row) for row in self._fetchall(
            """
            SELECT platform, product_id, COUNT(*) AS observation_count
            FROM observation_events
            GROUP BY platform, product_id
            HAVING MAX(CASE WHEN source_type = 'category' THEN 1 ELSE 0 END) = 1
               AND MAX(CASE WHEN source_type = 'keyword' THEN 1 ELSE 0 END) = 1
            ORDER BY observation_count DESC, platform, product_id
            LIMIT 10
            """
        )]

        stats["observation_type_breakdown"] = [dict(row) for row in self._fetchall(
            """
            SELECT source_type, COUNT(*) AS count
            FROM observation_events
            GROUP BY source_type
            ORDER BY count DESC
            """
        )]

        web_views: dict[str, int] = {}
        for view_name in (
            "vw_latest_product_observation",
            "vw_product_source_coverage",
            "vw_product_summary",
        ):
            web_views[view_name] = int(self._scalar(f"SELECT COUNT(*) FROM {view_name}") or 0)
        stats["web_views"] = web_views

        stats["latest_delivery_type_breakdown"] = [dict(row) for row in self._fetchall(
            """
            SELECT delivery_type, COUNT(*) AS count
            FROM vw_latest_product_observation
            GROUP BY delivery_type
            ORDER BY count DESC, delivery_type
            """
        )]

        stats["web_overlap_rows"] = int(
            self._scalar(
            """
            SELECT COUNT(*)
            FROM vw_product_summary
            WHERE has_category = 1 AND has_keyword = 1
            """
            )
            or 0
        )

        stats["web_overlap_samples"] = [dict(row) for row in self._fetchall(
            """
            SELECT
                platform,
                product_id,
                title,
                latest_source_type,
                latest_source_value,
                latest_delivery_type,
                latest_observed_at
            FROM vw_product_summary
            WHERE has_category = 1 AND has_keyword = 1
            ORDER BY latest_observed_at DESC, platform, product_id
            LIMIT 5
            """
        )]

        return stats


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Analytics Warehouse Build",
        "",
        f"- Generated at: {summary['generated_at']}",
        f"- Warehouse DB: {summary['warehouse_db']}",
        "",
    ]

    selection = summary.get("keyword_db_selection") or {}
    if selection:
        lines.extend(
            [
                "## Keyword DB Selection",
                f"- mode={selection.get('mode')}",
                f"- official_keyword_db={selection.get('official_keyword_db')}",
                f"- selected_keyword_dbs={', '.join(selection.get('selected_keyword_dbs') or []) or 'none'}",
                f"- fallback_keyword_db={selection.get('fallback_keyword_db') or 'none'}",
                f"- allow_legacy_fallback={selection.get('allow_legacy_fallback')}",
                f"- discovered_candidate_count={selection.get('discovered_candidate_count') or 0}",
                f"- recommended_fallback_keyword_db={selection.get('recommended_fallback_keyword_db') or 'none'}",
                "",
            ]
        )

    lines.extend(
        [
        "## Imported Sources",
        ]
    )

    for source in summary["sources"]:
        lines.append(
            f"- `{source['source_label']}`: scope={source['source_scope']}, "
            f"products={source['source_product_count']}, observations={source['source_observation_count']}, "
            f"keywords={source['source_keyword_count']}"
        )

    if summary.get("skipped_sources"):
        lines.extend(["", "## Skipped Sources"])
        for source in summary["skipped_sources"]:
            lines.append(
                f"- `{source['db_path']}`: products={source['products']}, observations={source['crawl_observations']}, "
                f"keyword_runs={source['keyword_runs']}, metrics={source['keyword_metrics_snapshots']}"
            )

    stats = summary["warehouse_stats"]
    lines.extend(
        [
            "",
            "## Warehouse Stats",
            f"- product_identity={stats['product_identity']}",
            f"- observation_events={stats['observation_events']}",
            f"- product_category_membership={stats['product_category_membership']}",
            f"- product_keyword_membership={stats['product_keyword_membership']}",
            f"- product_keyword_rank_events={stats['product_keyword_rank_events']}",
            f"- keyword_catalog={stats['keyword_catalog']}",
            f"- keyword_runs_log={stats['keyword_runs_log']}",
            f"- keyword_metric_snapshots={stats['keyword_metric_snapshots']}",
            f"- keyword_expansion_edges={stats['keyword_expansion_edges']}",
            f"- cross_source_product_overlap={stats['cross_source_product_overlap']}",
            "",
            "## Observation Types",
        ]
    )

    for row in stats.get("observation_type_breakdown", []):
        lines.append(f"- `{row['source_type']}`: {row['count']}")

    lines.extend(["", "## Web Read Models"])
    for view_name, row_count in stats.get("web_views", {}).items():
        lines.append(f"- `{view_name}`: {row_count}")
    lines.append(f"- overlap rows in `vw_product_summary`: {stats.get('web_overlap_rows', 0)}")

    if stats.get("latest_delivery_type_breakdown"):
        lines.extend(["", "## Latest Delivery Types"])
        for row in stats["latest_delivery_type_breakdown"]:
            delivery_type = row["delivery_type"] or "blank"
            lines.append(f"- `{delivery_type}`: {row['count']}")

    lines.extend(["", "## Overlap Samples"])
    if stats.get("overlap_samples"):
        for row in stats["overlap_samples"]:
            lines.append(f"- `{row['platform']}:{row['product_id']}` observations={row['observation_count']}")
    else:
        lines.append("- No cross-source overlap detected yet.")

    lines.extend(["", "## Web Overlap Samples"])
    if stats.get("web_overlap_samples"):
        for row in stats["web_overlap_samples"]:
            lines.append(
                f"- `{row['platform']}:{row['product_id']}` latest={row['latest_source_type']}/{row['latest_source_value']} "
                f"delivery={row['latest_delivery_type'] or 'blank'}"
            )
    else:
        lines.append("- No cross-source rows available in vw_product_summary yet.")

    return "\n".join(lines) + "\n"


def main():
    configure_stdio()
    args = parse_args()

    stage_dbs: list[str] = []
    keyword_stage_dbs, keyword_db_selection = select_keyword_stage_dbs(
        args.keyword_dbs,
        DEFAULT_RUNTIME_DIR,
        include_discovery=args.discover_keyword_dbs,
        allow_legacy_fallback=args.allow_legacy_keyword_fallback,
    )
    category_config = database_config_from_reference(args.category_db)
    if category_config.is_postgres:
        stage_dbs.append(str(args.category_db))
    elif category_config.sqlite_path_or_raise("category stage").exists():
        stage_dbs.append(str(args.category_db))
    else:
        logger.warning("类目 stage 库不存在，跳过: %s", args.category_db)

    logger.info("关键词库选源模式: %s", keyword_db_selection.get("mode"))
    logger.info("关键词官方库: %s", keyword_db_selection.get("official_keyword_db"))
    if keyword_db_selection.get("selected_keyword_dbs"):
        logger.info("本次导入关键词库: %s", ", ".join(keyword_db_selection["selected_keyword_dbs"]))
    if keyword_db_selection.get("discovered_candidate_count"):
        logger.info("发现可用历史关键词库候选: %s", keyword_db_selection["discovered_candidate_count"])
    if keyword_db_selection.get("recommended_fallback_keyword_db"):
        logger.warning(
            "存在可回退关键词库但当前未启用: %s",
            keyword_db_selection["recommended_fallback_keyword_db"],
        )
    if keyword_db_selection.get("fallback_disabled_reason"):
        logger.warning("legacy fallback 已禁用: %s", keyword_db_selection["fallback_disabled_reason"])
    if keyword_db_selection.get("fallback_keyword_db"):
        logger.warning("当前使用 legacy fallback 关键词库: %s", keyword_db_selection["fallback_keyword_db"])

    for db_ref in keyword_stage_dbs:
        if db_ref not in stage_dbs:
            stage_dbs.append(db_ref)

    stage_dbs = list(dict.fromkeys(stage_dbs))

    if not stage_dbs:
        raise SystemExit("未发现可导入的 stage 数据库")

    builder = AnalyticsWarehouseBuilder(args.warehouse_db)
    try:
        if args.replace:
            logger.info("重建 warehouse.db: %s", args.warehouse_db)
            builder.reset()

        imported_sources = []
        skipped_sources = []
        for db_path in stage_dbs:
            stage_stats = inspect_stage_db(db_path)
            if not stage_stats["has_payload"]:
                logger.info("跳过空 stage 数据库: %s", db_path)
                skipped_sources.append(stage_stats)
                continue
            logger.info("导入 stage 数据库: %s", db_path)
            imported_sources.append(builder.import_stage_db(db_path))

        summary = {
            "generated_at": datetime.now().isoformat(),
            "warehouse_db": str(args.warehouse_db),
            "keyword_db_selection": keyword_db_selection,
            "sources": imported_sources,
            "skipped_sources": skipped_sources,
            "warehouse_stats": {} if args.skip_stats else builder.collect_warehouse_stats(),
        }
    finally:
        builder.close()

    logger.info("Warehouse 构建完成: %s", args.warehouse_db)
    if args.skip_reports:
        logger.info("构建摘要已跳过: skip_reports=true")
        return

    DEFAULT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_json = DEFAULT_REPORTS_DIR / f"analytics_warehouse_build_{timestamp}.json"
    summary_md = DEFAULT_REPORTS_DIR / f"analytics_warehouse_build_{timestamp}.md"
    summary_json.write_text(json_dumps(summary), encoding="utf-8")
    summary_md.write_text(render_markdown(summary), encoding="utf-8")
    logger.info("构建摘要: %s", summary_json)


if __name__ == "__main__":
    main()
