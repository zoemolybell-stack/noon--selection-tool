import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import sys
import threading
import time
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.keyword_opportunity_engine import (
    build_keyword_graph_payload,
    build_opportunity_items,
    build_opportunity_summary,
    build_quality_issue_summary,
)
from analysis.keyword_warehouse_health import build_keyword_warehouse_health_payload
from config.product_store import ProductStore
from db.config import DatabaseConfig, get_warehouse_database_config
from ops.auto_dispatch import annotate_crawl_plans, build_auto_dispatch_summary
from ops.crawler_control import (
    build_crawler_catalog,
    get_crawler_run_detail,
    launch_plan_now,
    list_crawler_runs,
    normalize_plan_payload,
)
from ops.keyword_control_state import (
    KEYWORD_CONTROL_SCOPES,
    KEYWORD_ROOT_MATCH_MODES,
    get_monitor_active_keywords,
    get_keyword_control_state,
    load_monitor_config_payload,
    update_monitor_baseline_keywords,
    update_monitor_blocked_root_rules,
    update_monitor_disabled_keyword_rules,
    update_monitor_exclusion_rule,
)
from ops.task_store import OpsStore
from tools.keyword_runtime_health import (
    ACTIVE_LOG_WINDOW_SECONDS,
    build_keyword_quality_truth_model,
    build_quality_payload,
    collect_batch_logs,
    collect_lock_payload,
    collect_summary_payload,
)
from warehouse_sync import read_sync_state

WEB_DIR = Path(__file__).resolve().parent
STATIC_DIR = WEB_DIR / "static"
INDEX_PATH = STATIC_DIR / "index.html"
DEFAULT_DB_PATH = ROOT / "data" / "analytics" / "warehouse.db"
DEFAULT_SEED_CATALOG_PATH = ROOT / "config" / "keyword_seed_catalog.json"
WEB_USER_REGISTRY_PATH = ROOT / "config" / "web_user_registry.json"
UI_RUNTIME_DIR = ROOT / "data" / "runtime" / "ui_workbench"
PRODUCT_FAVORITES_DIR = UI_RUNTIME_DIR / "product_favorites"
REPORT_ROOT = Path(
    os.getenv(
        "NOON_REPORT_ROOT",
        str(ROOT / "data" / "runtime" / "crawl_reports"),
    )
)
REPORT_STATE_DIR = REPORT_ROOT / "state"
WATCHDOG_LATEST_JSON_PATH = REPORT_STATE_DIR / "watchdog_latest.json"
KEYWORD_RUNTIME_ROOT = Path(
    os.getenv(
        "NOON_KEYWORD_RUNTIME_DIR",
        str(ROOT / "runtime_data" / "keyword"),
    )
)
KEYWORD_MONITOR_DIR = KEYWORD_RUNTIME_ROOT / "monitor"
KEYWORD_PRODUCT_STORE_PATH = KEYWORD_RUNTIME_ROOT / "product_store.db"

CANONICAL_CATEGORY_PREFIX = "Home > "
DEFAULT_PAGE_LIMIT = 50
DEFAULT_EXPORT_LIMIT = 5000
WEB_LATEST_TABLE = "web_latest_product_observation"
WEB_SOURCE_COVERAGE_TABLE = "web_product_source_coverage"
WEB_PRODUCT_SUMMARY_TABLE = "web_product_summary"
WEB_CACHE_META_TABLE = "web_cache_meta"
READ_MODEL_SCHEMA_VERSION = "20260404a"
READ_MODEL_CHECK_INTERVAL_SECONDS = 20.0
SYNC_FRESH_SECONDS = 2 * 3600
SYNC_DELAYED_SECONDS = 12 * 3600
CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")
CSV_FORMULA_LEADING_CHARS = ("\t", "\r", "\n")
READ_MODEL_STATE = {
    "last_checked_at": 0.0,
    "signature": None,
}
READ_MODEL_LOCK = threading.Lock()
PAYLOAD_CACHE_LOCK = threading.Lock()
# Main workbench payloads are expensive enough that a 20s TTL causes
# unstable cold-path regressions during routine operator checks. Keep them
# warm longer; read-model invalidation still handles correctness on import.
PAYLOAD_CACHE_TTL_SECONDS = 900.0
PAYLOAD_CACHE_MAX_ITEMS = 256
PAYLOAD_CACHE: dict[tuple[object, ...], dict[str, object]] = {}
STATIC_PAYLOAD_CACHE_LOCK = threading.Lock()
STATIC_PAYLOAD_CACHE_TTL_SECONDS = 1800.0
STATIC_PAYLOAD_CACHE_MAX_ITEMS = 64
STATIC_PAYLOAD_CACHE: dict[tuple[object, ...], dict[str, object]] = {}
PRODUCT_FAVORITES_LOCK = threading.Lock()
MARKET_CURRENCY_MAP = {
    "ksa": "SAR",
    "uae": "AED",
}


def _freeze_payload_cache_part(value: object) -> object:
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_payload_cache_part(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted((str(key), _freeze_payload_cache_part(item)) for key, item in value.items()))
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    return str(value)


def _payload_cache_key(namespace: str, *parts: object) -> tuple[object, ...]:
    return (namespace, _warehouse_backend(), *(_freeze_payload_cache_part(part) for part in parts))


def _read_cache_entry(
    cache: dict[tuple[object, ...], dict[str, object]],
    lock: threading.Lock,
    namespace: str,
    *parts: object,
):
    key = _payload_cache_key(namespace, *parts)
    now = time.time()
    with lock:
        entry = cache.get(key)
        if not entry:
            return None
        expires_at = float(entry.get("expires_at") or 0.0)
        if expires_at <= now:
            cache.pop(key, None)
            return None
        entry["last_access_at"] = now
        value = entry.get("value")
    return deepcopy(value)


def _write_cache_entry(
    cache: dict[tuple[object, ...], dict[str, object]],
    lock: threading.Lock,
    ttl_seconds: float,
    max_items: int,
    namespace: str,
    *parts: object,
    value: object,
) -> object:
    key = _payload_cache_key(namespace, *parts)
    now = time.time()
    cached_value = deepcopy(value)
    with lock:
        cache[key] = {
            "value": cached_value,
            "updated_at": now,
            "last_access_at": now,
            "expires_at": now + ttl_seconds,
        }
        if len(cache) > max_items:
            stale_keys = sorted(
                cache.items(),
                key=lambda item: (float(item[1].get("expires_at") or 0.0), float(item[1].get("last_access_at") or 0.0)),
            )
            for stale_key, stale_entry in stale_keys:
                if len(cache) <= max_items:
                    break
                if float(stale_entry.get("expires_at") or 0.0) <= now or len(cache) > max_items:
                    cache.pop(stale_key, None)
    return deepcopy(cached_value)


def _read_payload_cache(namespace: str, *parts: object):
    return _read_cache_entry(PAYLOAD_CACHE, PAYLOAD_CACHE_LOCK, namespace, *parts)


def _write_payload_cache(namespace: str, *parts: object, value: object) -> object:
    return _write_cache_entry(
        PAYLOAD_CACHE,
        PAYLOAD_CACHE_LOCK,
        PAYLOAD_CACHE_TTL_SECONDS,
        PAYLOAD_CACHE_MAX_ITEMS,
        namespace,
        *parts,
        value=value,
    )


def _read_static_payload_cache(namespace: str, *parts: object):
    return _read_cache_entry(STATIC_PAYLOAD_CACHE, STATIC_PAYLOAD_CACHE_LOCK, namespace, *parts)


def _write_static_payload_cache(namespace: str, *parts: object, value: object) -> object:
    return _write_cache_entry(
        STATIC_PAYLOAD_CACHE,
        STATIC_PAYLOAD_CACHE_LOCK,
        STATIC_PAYLOAD_CACHE_TTL_SECONDS,
        STATIC_PAYLOAD_CACHE_MAX_ITEMS,
        namespace,
        *parts,
        value=value,
    )


def normalize_market_currency(market: str = "") -> str:
    return MARKET_CURRENCY_MAP.get(str(market or "").strip().lower(), "")

PRODUCT_SELECT_COLUMNS = """
    summary.platform,
    summary.product_id,
    summary.title,
    summary.brand,
    summary.seller_name,
    summary.product_url,
    summary.image_url,
    summary.latest_category_path,
    summary.latest_observed_category_path,
    summary.latest_source_type,
    summary.latest_source_value,
    summary.latest_observed_at,
    summary.latest_price,
    summary.latest_original_price,
    summary.latest_currency,
    summary.latest_rating,
    summary.latest_review_count,
    summary.latest_sold_recently_count,
    summary.latest_stock_left_count,
    summary.latest_search_rank,
    summary.latest_bsr_rank,
    summary.latest_visible_bsr_rank,
    summary.monthly_sales_estimate,
    summary.inventory_left_estimate,
    summary.inventory_signal_bucket,
    summary.review_count_growth_7d,
    summary.review_count_growth_14d,
    summary.rating_growth_7d,
    summary.rating_growth_14d,
    summary.latest_delivery_type,
    summary.latest_is_ad,
    summary.latest_is_bestseller,
    summary.latest_delivery_eta_signal_text,
    summary.latest_lowest_price_signal_text,
    summary.latest_sold_recently_text,
    summary.latest_stock_signal_text,
    summary.latest_ranking_signal_text,
    summary.sticky_delivery_eta_signal_text,
    summary.sticky_delivery_eta_signal_seen_at,
    summary.sticky_lowest_price_signal_text,
    summary.sticky_lowest_price_signal_seen_at,
    summary.sticky_sold_recently_text,
    summary.sticky_sold_recently_seen_at,
    summary.sticky_stock_signal_text,
    summary.sticky_stock_signal_seen_at,
    summary.sticky_ranking_signal_text,
    summary.sticky_ranking_signal_seen_at,
    summary.latest_signal_count,
    summary.has_sold_signal,
    summary.has_stock_signal,
    summary.has_lowest_price_signal,
    summary.has_category,
    summary.has_keyword,
    summary.observation_count
"""

KEYWORD_RANKED_METRICS_CTE = """
WITH ranked_metrics AS (
    SELECT
        kms.*,
        ROW_NUMBER() OVER (
            PARTITION BY kms.keyword
            ORDER BY kms.analyzed_at DESC, kms.id DESC
        ) AS rn
    FROM keyword_metric_snapshots kms
)
"""


def build_signal_count_sql(alias: str = "summary", *, sticky_supported: bool = True) -> str:
    def signal_expr(signal_name: str) -> str:
        latest_expr = f"NULLIF({alias}.latest_{signal_name}, '')"
        if not sticky_supported:
            return f"COALESCE({latest_expr}, '')"
        sticky_expr = f"NULLIF({alias}.sticky_{signal_name}, '')"
        return f"COALESCE({latest_expr}, {sticky_expr}, '')"

    return f"""
    (
        CASE WHEN {signal_expr("lowest_price_signal_text")} <> '' THEN 1 ELSE 0 END +
        CASE WHEN {signal_expr("stock_signal_text")} <> '' THEN 1 ELSE 0 END +
        CASE WHEN {signal_expr("sold_recently_text")} <> '' THEN 1 ELSE 0 END +
        CASE WHEN {signal_expr("ranking_signal_text")} <> '' THEN 1 ELSE 0 END +
        CASE WHEN {signal_expr("delivery_eta_signal_text")} <> '' THEN 1 ELSE 0 END
    )
    """


def _summary_signal_expr(alias: str, signal_name: str, *, sticky_supported: bool = True) -> str:
    latest_expr = f"NULLIF({alias}.latest_{signal_name}, '')"
    if not sticky_supported:
        return f"COALESCE({latest_expr}, '')"
    sticky_expr = f"NULLIF({alias}.sticky_{signal_name}, '')"
    return f"COALESCE({latest_expr}, {sticky_expr}, '')"


def _sticky_projection_sql(alias: str = "base", *, sticky_supported: bool = True) -> str:
    if sticky_supported:
        return ""
    fields = {
        "delivery_eta_signal_text": "sticky_delivery_eta_signal_seen_at",
        "lowest_price_signal_text": "sticky_lowest_price_signal_seen_at",
        "sold_recently_text": "sticky_sold_recently_seen_at",
        "stock_signal_text": "sticky_stock_signal_seen_at",
        "ranking_signal_text": "sticky_ranking_signal_seen_at",
    }
    columns: list[str] = []
    for field, seen_field in fields.items():
        if sticky_supported:
            columns.append(f"{alias}.sticky_{field}")
        else:
            columns.append(f"NULL AS sticky_{field}")
            columns.append(f"NULL AS {seen_field}")
    return ",\n            " + ",\n            ".join(columns)


def _sticky_seen_field(signal_name: str) -> str:
    field_map = {
        "delivery_eta_signal_text": "sticky_delivery_eta_signal_seen_at",
        "lowest_price_signal_text": "sticky_lowest_price_signal_seen_at",
        "sold_recently_text": "sticky_sold_recently_seen_at",
        "stock_signal_text": "sticky_stock_signal_seen_at",
        "ranking_signal_text": "sticky_ranking_signal_seen_at",
    }
    return field_map[signal_name]


def get_warehouse_db_config(default_path: Path | None = None) -> DatabaseConfig:
    return get_warehouse_database_config(default_path or DEFAULT_DB_PATH)


def get_keyword_product_store() -> ProductStore:
    return ProductStore(KEYWORD_PRODUCT_STORE_PATH)


def get_stale_worker_max_age_seconds() -> int:
    raw_value = str(os.getenv("NOON_STALE_WORKER_MAX_AGE_SECONDS", "1800")).strip()
    try:
        parsed = int(raw_value)
    except ValueError:
        return 1800
    return max(parsed, 60)


def _first_value(row: object) -> object:
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    try:
        return row[0]
    except (IndexError, KeyError, TypeError):
        return next(iter(row), None) if hasattr(row, "__iter__") else None


def _row_to_dict(row: object) -> dict:
    if not row:
        return {}
    return dict(row)


def _adapt_sql(sql: str, *, backend: str) -> str:
    if backend == "sqlite":
        return sql
    return sql.replace("?", "%s")


def _warehouse_backend(db_path: Path | None = None) -> str:
    return get_warehouse_db_config(db_path).backend


def _normalize_keyword_control_token(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_keyword_control_list(values: list[object] | tuple[object, ...] | set[object] | None) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values or []:
        token = _normalize_keyword_control_token(value)
        if not token or token in seen:
            continue
        seen.add(token)
        items.append(token)
    return items


def _normalize_keyword_control_sources(values: list[object] | tuple[object, ...] | set[object] | None) -> list[str]:
    items = [
        _normalize_keyword_control_token(value)
        for value in values or []
    ]
    normalized = [item for item in items if item in KEYWORD_CONTROL_SCOPES]
    return normalized or list(KEYWORD_CONTROL_SCOPES)


def _validate_keyword_control_sources(blocked_sources: list[object] | tuple[object, ...] | set[object] | None) -> None:
    unknown_sources = [
        str(item or "").strip()
        for item in blocked_sources or []
        if str(item or "").strip().lower() not in KEYWORD_CONTROL_SCOPES
    ]
    if unknown_sources:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported blocked_sources: {', '.join(sorted(set(unknown_sources)))}",
        )


def _coalesce_keyword_control_keywords(
    keywords: list[object] | tuple[object, ...] | set[object] | None,
    *,
    keyword: object = "",
) -> list[str]:
    return _normalize_keyword_control_list([*(keywords or []), keyword])


def _register_monitor_baseline_keywords_immediately(monitor_config: str, keywords: list[str]) -> int:
    normalized_keywords = _normalize_keyword_control_list(keywords)
    if not normalized_keywords:
        return 0
    _, config_payload = load_monitor_config_payload(monitor_config)
    tracked_priority = int(config_payload.get("tracked_priority") or 30)
    store = get_keyword_product_store()
    try:
        return store.upsert_keywords(
            normalized_keywords,
            tracking_mode="tracked",
            source_type="baseline",
            priority=tracked_priority,
            metadata={
                "registration_source": "baseline",
                "monitor_config": monitor_config,
            },
        )
    finally:
        store.close()


def _build_effective_active_keyword_rows(monitor_config: str) -> list[dict]:
    payload = get_monitor_active_keywords(monitor_config)
    return list(payload.get("items") or [])


def _sql_round_avg(expr: str, *, backend: str | None = None, digits: int = 2) -> str:
    target_backend = backend or _warehouse_backend()
    if target_backend == "postgres":
        return f"ROUND(CAST(AVG({expr}) AS numeric), {digits})"
    return f"ROUND(AVG({expr}), {digits})"


def get_db_path() -> Path:
    config = get_warehouse_db_config()
    if config.is_sqlite:
        return config.sqlite_path_or_raise("web warehouse read model")
    return DEFAULT_DB_PATH


def build_visible_bsr_expr(*, bsr_expr: str, ranking_signal_expr: str, backend: str | None = None) -> str:
    target_backend = backend or _warehouse_backend()
    if target_backend == "postgres":
        return f"""
        CASE
            WHEN {bsr_expr} IS NOT NULL THEN CAST({bsr_expr} AS INTEGER)
            WHEN LEFT(COALESCE({ranking_signal_expr}, ''), 1) = '#'
                 AND POSITION(' in ' IN {ranking_signal_expr}) > 2
            THEN CAST(SUBSTRING({ranking_signal_expr} FROM 2 FOR POSITION(' in ' IN {ranking_signal_expr}) - 2) AS INTEGER)
            ELSE NULL
        END
        """
    return f"""
    CASE
        WHEN {bsr_expr} IS NOT NULL THEN CAST({bsr_expr} AS INTEGER)
        WHEN SUBSTR(COALESCE({ranking_signal_expr}, ''), 1, 1) = '#'
             AND INSTR({ranking_signal_expr}, ' in ') > 2
        THEN CAST(SUBSTR({ranking_signal_expr}, 2, INSTR({ranking_signal_expr}, ' in ') - 2) AS INTEGER)
        ELSE NULL
    END
    """


def build_visible_bsr_sql(alias: str = "summary", *, backend: str | None = None) -> str:
    return build_visible_bsr_expr(
        bsr_expr=f"{alias}.latest_bsr_rank",
        ranking_signal_expr=f"{alias}.latest_ranking_signal_text",
        backend=backend,
    )


def resolve_sort_sql(sort: str = "latest_observed_at") -> str:
    sort_key = (sort or "latest_observed_at").strip().lower()
    sort_map = {
        "latest_observed_at": "summary.latest_observed_at DESC, summary.last_seen DESC",
        "sales_desc": "CASE WHEN summary.monthly_sales_estimate IS NULL THEN 1 ELSE 0 END ASC, summary.monthly_sales_estimate DESC, summary.latest_observed_at DESC",
        "bsr_asc": "CASE WHEN summary.latest_visible_bsr_rank IS NULL THEN 1 ELSE 0 END ASC, summary.latest_visible_bsr_rank ASC, summary.latest_observed_at DESC",
        "price_asc": "CASE WHEN summary.latest_price IS NULL THEN 1 ELSE 0 END ASC, summary.latest_price ASC, summary.latest_observed_at DESC",
        "price_desc": "CASE WHEN summary.latest_price IS NULL THEN 1 ELSE 0 END ASC, summary.latest_price DESC, summary.latest_observed_at DESC",
        "review_desc": "COALESCE(summary.latest_review_count, 0) DESC, summary.latest_rating DESC, summary.latest_observed_at DESC",
        "ad_first": "COALESCE(summary.latest_is_ad, 0) DESC, summary.latest_observed_at DESC",
        "signal_count_desc": "COALESCE(summary.latest_signal_count, 0) DESC, COALESCE(summary.latest_review_count, 0) DESC, summary.latest_observed_at DESC",
    }
    return sort_map.get(sort_key, sort_map["latest_observed_at"])


def _open_db_connection(db_path: Path | None = None):
    config = get_warehouse_db_config(db_path)
    if config.is_sqlite:
        target_db_path = config.sqlite_path_or_raise("web warehouse read model")
        conn = sqlite3.connect(str(target_db_path))
        conn.row_factory = sqlite3.Row
        return conn
    return psycopg.connect(
        config.dsn,
        row_factory=dict_row,
        autocommit=True,
    )


def _execute_fetchone(conn, sql: str, params: tuple | list = ()) -> object:
    return conn.execute(_adapt_sql(sql, backend=_warehouse_backend()), tuple(params)).fetchone()


def _execute_fetchall(conn, sql: str, params: tuple | list = ()) -> list[object]:
    return conn.execute(_adapt_sql(sql, backend=_warehouse_backend()), tuple(params)).fetchall()


def _relation_exists(conn, relation_name: str) -> bool:
    backend = _warehouse_backend()
    if backend == "postgres":
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            UNION ALL
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = 'public' AND table_name = %s
            LIMIT 1
            """,
            (relation_name, relation_name),
        ).fetchone()
        return row is not None
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
        (relation_name,),
    ).fetchone()
    return row is not None


def _relation_columns(conn, relation_name: str) -> set[str]:
    backend = _warehouse_backend()
    if backend == "postgres":
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (relation_name,),
        ).fetchall()
        return {row["column_name"] for row in rows}
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({relation_name})").fetchall()}


def fetch_read_model_signature(conn) -> dict:
    row = _row_to_dict(
        _execute_fetchone(
        conn,
        """
        SELECT
            (SELECT COUNT(*) FROM product_identity) AS product_count,
            (SELECT COUNT(*) FROM observation_events) AS observation_count,
            (SELECT COUNT(*) FROM product_category_membership) AS category_membership_count,
            (SELECT COUNT(*) FROM product_keyword_membership) AS keyword_membership_count,
            (SELECT COALESCE(MAX(imported_at), '') FROM source_databases) AS last_imported_at
        """,
        )
    )
    return {
        "schema_version": READ_MODEL_SCHEMA_VERSION,
        "product_count": int((row.get("product_count") if row else 0) or 0),
        "observation_count": int((row.get("observation_count") if row else 0) or 0),
        "category_membership_count": int((row.get("category_membership_count") if row else 0) or 0),
        "keyword_membership_count": int((row.get("keyword_membership_count") if row else 0) or 0),
        "last_imported_at": (row.get("last_imported_at") if row else "") or "",
    }


def rebuild_web_read_models(conn) -> None:
    backend = _warehouse_backend()
    base_summary_columns = _relation_columns(conn, "vw_product_summary")
    sticky_summary_fields = {
        "sticky_delivery_eta_signal_text",
        "sticky_delivery_eta_signal_seen_at",
        "sticky_lowest_price_signal_text",
        "sticky_lowest_price_signal_seen_at",
        "sticky_sold_recently_text",
        "sticky_sold_recently_seen_at",
        "sticky_stock_signal_text",
        "sticky_stock_signal_seen_at",
        "sticky_ranking_signal_text",
        "sticky_ranking_signal_seen_at",
    }
    sticky_supported = sticky_summary_fields.issubset(base_summary_columns)
    derived_summary_columns = {
        "latest_sold_recently_count": "INTEGER",
        "latest_stock_left_count": "INTEGER",
        "monthly_sales_estimate": "REAL",
        "inventory_left_estimate": "REAL",
        "inventory_signal_bucket": "TEXT",
        "review_count_growth_7d": "REAL",
        "review_count_growth_14d": "REAL",
        "rating_growth_7d": "REAL",
        "rating_growth_14d": "REAL",
    }
    missing_derived_summary_columns = {
        column_name: column_type
        for column_name, column_type in derived_summary_columns.items()
        if column_name not in base_summary_columns
    }
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {WEB_CACHE_META_TABLE} (
            cache_key TEXT PRIMARY KEY,
            cache_value TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_category_membership_path_product
        ON product_category_membership(category_path, platform, product_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_keyword_membership_keyword_product
        ON product_keyword_membership(keyword, platform, product_id)
        """,
        f"DROP TABLE IF EXISTS {WEB_LATEST_TABLE}",
        f"CREATE TABLE {WEB_LATEST_TABLE} AS SELECT * FROM vw_latest_product_observation",
        f"CREATE UNIQUE INDEX idx_{WEB_LATEST_TABLE}_product ON {WEB_LATEST_TABLE}(platform, product_id)",
        f"CREATE INDEX idx_{WEB_LATEST_TABLE}_source ON {WEB_LATEST_TABLE}(source_type, source_value)",
        f"DROP TABLE IF EXISTS {WEB_SOURCE_COVERAGE_TABLE}",
        f"CREATE TABLE {WEB_SOURCE_COVERAGE_TABLE} AS SELECT * FROM vw_product_source_coverage",
        f"CREATE UNIQUE INDEX idx_{WEB_SOURCE_COVERAGE_TABLE}_product ON {WEB_SOURCE_COVERAGE_TABLE}(platform, product_id)",
        f"CREATE INDEX idx_{WEB_SOURCE_COVERAGE_TABLE}_flags ON {WEB_SOURCE_COVERAGE_TABLE}(has_category, has_keyword)",
        f"DROP TABLE IF EXISTS {WEB_PRODUCT_SUMMARY_TABLE}",
        f"""
        CREATE TABLE {WEB_PRODUCT_SUMMARY_TABLE} AS
        SELECT
            base.*
            {_sticky_projection_sql("base", sticky_supported=sticky_supported)},
            {build_visible_bsr_sql("base", backend=backend)} AS latest_visible_bsr_rank,
            {build_signal_count_sql("base", sticky_supported=sticky_supported)} AS latest_signal_count,
            CASE WHEN {_summary_signal_expr("base", "sold_recently_text", sticky_supported=sticky_supported)} <> '' THEN 1 ELSE 0 END AS has_sold_signal,
            CASE WHEN {_summary_signal_expr("base", "stock_signal_text", sticky_supported=sticky_supported)} <> '' THEN 1 ELSE 0 END AS has_stock_signal,
            CASE WHEN {_summary_signal_expr("base", "lowest_price_signal_text", sticky_supported=sticky_supported)} <> '' THEN 1 ELSE 0 END AS has_lowest_price_signal
        FROM vw_product_summary base
        """,
        *[
            f"ALTER TABLE {WEB_PRODUCT_SUMMARY_TABLE} ADD COLUMN {column_name} {column_type}"
            for column_name, column_type in missing_derived_summary_columns.items()
        ],
        f"CREATE UNIQUE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_product ON {WEB_PRODUCT_SUMMARY_TABLE}(platform, product_id)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_latest ON {WEB_PRODUCT_SUMMARY_TABLE}(latest_observed_at DESC)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_visible_bsr ON {WEB_PRODUCT_SUMMARY_TABLE}(latest_visible_bsr_rank)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_delivery ON {WEB_PRODUCT_SUMMARY_TABLE}(latest_delivery_type)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_ad ON {WEB_PRODUCT_SUMMARY_TABLE}(latest_is_ad)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_coverage ON {WEB_PRODUCT_SUMMARY_TABLE}(has_category, has_keyword)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_price ON {WEB_PRODUCT_SUMMARY_TABLE}(latest_price)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_reviews ON {WEB_PRODUCT_SUMMARY_TABLE}(latest_review_count)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_sales ON {WEB_PRODUCT_SUMMARY_TABLE}(monthly_sales_estimate)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_inventory ON {WEB_PRODUCT_SUMMARY_TABLE}(inventory_left_estimate, inventory_signal_bucket)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_review_growth ON {WEB_PRODUCT_SUMMARY_TABLE}(review_count_growth_7d, review_count_growth_14d)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_rating_growth ON {WEB_PRODUCT_SUMMARY_TABLE}(rating_growth_7d, rating_growth_14d)",
        f"CREATE INDEX idx_{WEB_PRODUCT_SUMMARY_TABLE}_signals ON {WEB_PRODUCT_SUMMARY_TABLE}(latest_signal_count, has_sold_signal, has_stock_signal, has_lowest_price_signal)",
    ]
    for statement in statements:
        conn.execute(statement)


def ensure_web_read_models(force: bool = False, allow_missing: bool = False) -> None:
    db_config = get_warehouse_db_config(DEFAULT_DB_PATH)
    if db_config.is_sqlite and not db_config.sqlite_path_or_raise("web warehouse read model").exists():
        if allow_missing:
            READ_MODEL_STATE["signature"] = None
            READ_MODEL_STATE["last_checked_at"] = time.monotonic()
            return
        raise HTTPException(status_code=503, detail=f"warehouse db not found: {db_config.sqlite_path_or_raise('web warehouse read model')}")
    now = time.monotonic()
    if not force and READ_MODEL_STATE["last_checked_at"] and (now - READ_MODEL_STATE["last_checked_at"] < READ_MODEL_CHECK_INTERVAL_SECONDS):
        return

    with READ_MODEL_LOCK:
        now = time.monotonic()
        if not force and READ_MODEL_STATE["last_checked_at"] and (now - READ_MODEL_STATE["last_checked_at"] < READ_MODEL_CHECK_INTERVAL_SECONDS):
            return

        conn = _open_db_connection(db_config.sqlite_path if db_config.is_sqlite else DEFAULT_DB_PATH)
        try:
            current_signature = fetch_read_model_signature(conn)
            signature_json = json.dumps(current_signature, sort_keys=True, ensure_ascii=False)

            meta_table_exists = _relation_exists(conn, WEB_CACHE_META_TABLE)
            summary_table_exists = _relation_exists(conn, WEB_PRODUCT_SUMMARY_TABLE)
            stored_signature = None
            if meta_table_exists:
                row = _row_to_dict(
                    _execute_fetchone(
                    conn,
                    f"SELECT cache_value FROM {WEB_CACHE_META_TABLE} WHERE cache_key = 'read_model_signature'",
                )
                )
                stored_signature = row.get("cache_value") if row else None

            summary_table_has_required_columns = False
            if summary_table_exists:
                required_columns = {
                    "latest_visible_bsr_rank",
                    "latest_signal_count",
                    "has_sold_signal",
                    "has_stock_signal",
                    "has_lowest_price_signal",
                    "sticky_sold_recently_text",
                    "sticky_sold_recently_seen_at",
                    "sticky_stock_signal_text",
                    "sticky_stock_signal_seen_at",
                    "sticky_lowest_price_signal_text",
                    "sticky_lowest_price_signal_seen_at",
                    "sticky_ranking_signal_text",
                    "sticky_ranking_signal_seen_at",
                    "sticky_delivery_eta_signal_text",
                    "sticky_delivery_eta_signal_seen_at",
                    "monthly_sales_estimate",
                    "inventory_left_estimate",
                    "inventory_signal_bucket",
                    "review_count_growth_7d",
                    "review_count_growth_14d",
                    "rating_growth_7d",
                    "rating_growth_14d",
                }
                existing_columns = _relation_columns(conn, WEB_PRODUCT_SUMMARY_TABLE)
                summary_table_has_required_columns = required_columns.issubset(existing_columns)

            if force or not summary_table_exists or not summary_table_has_required_columns or stored_signature != signature_json:
                rebuild_web_read_models(conn)
                conn.execute(
                    _adapt_sql(
                    f"""
                    INSERT INTO {WEB_CACHE_META_TABLE}(cache_key, cache_value)
                    VALUES('read_model_signature', ?)
                    ON CONFLICT(cache_key) DO UPDATE SET cache_value = excluded.cache_value
                    """,
                    backend=db_config.backend,
                    ),
                    (signature_json,),
                )
                if hasattr(conn, "commit"):
                    conn.commit()

            READ_MODEL_STATE["signature"] = signature_json
            READ_MODEL_STATE["last_checked_at"] = now
        finally:
            conn.close()


def connect_db():
    ensure_web_read_models()
    return _open_db_connection()


def connect_ops_store() -> OpsStore:
    return OpsStore()


def describe_ops_store(store: OpsStore) -> str:
    if store.db_path is not None:
        return _safe_storage_label(store.db_path)
    if store.db_config.dsn:
        return _safe_storage_label(store.db_config.dsn)
    return "unknown"


class TaskCreateRequest(BaseModel):
    task_type: str
    payload: dict = Field(default_factory=dict)
    created_by: str = "web"
    priority: int = 100
    schedule_type: str = "manual"
    schedule_expr: str = ""
    next_run_at: str | None = None
    worker_type: str | None = None


class CrawlerPlanCreateRequest(BaseModel):
    plan_type: str
    name: str
    created_by: str = "crawler_console"
    enabled: bool = True
    schedule_kind: str = "manual"
    schedule_json: dict = Field(default_factory=dict)
    payload: dict = Field(default_factory=dict)


class CrawlerPlanUpdateRequest(BaseModel):
    name: str | None = None
    enabled: bool | None = None
    schedule_kind: str | None = None
    schedule_json: dict | None = None
    payload: dict | None = None


class KeywordControlBaselineRequest(BaseModel):
    monitor_config: str
    keywords: list[str] = Field(default_factory=list)
    mode: str = "add"


class KeywordControlExclusionRequest(BaseModel):
    monitor_config: str
    keyword: str
    blocked_sources: list[str] = Field(default_factory=list)
    reason: str = ""
    mode: str = "upsert"


class KeywordControlDisableRequest(BaseModel):
    monitor_config: str
    keyword: str = ""
    keywords: list[str] = Field(default_factory=list)
    blocked_sources: list[str] = Field(default_factory=list)
    reason: str = ""


class KeywordControlRestoreRequest(BaseModel):
    monitor_config: str
    keyword: str = ""
    keywords: list[str] = Field(default_factory=list)


class KeywordControlRootsRequest(BaseModel):
    monitor_config: str
    keyword: str = ""
    keywords: list[str] = Field(default_factory=list)
    blocked_sources: list[str] = Field(default_factory=list)
    reason: str = ""
    match_mode: str = "exact"
    mode: str = "upsert"


class UiFilterPresetCreateRequest(BaseModel):
    view: str
    preset_name: str
    route_payload: dict = Field(default_factory=dict)
    is_default: bool = False


class UiFilterPresetUpdateRequest(BaseModel):
    preset_name: str | None = None
    route_payload: dict | None = None
    is_default: bool | None = None
    mark_used: bool = False


class UiFilterHistoryRequest(BaseModel):
    view: str
    route_payload: dict = Field(default_factory=dict)
    summary: dict = Field(default_factory=dict)


class ProductFavoriteRequest(BaseModel):
    platform: str
    product_id: str


def _extract_authenticated_email(request: Request) -> str:
    return (
        request.headers.get("Cf-Access-Authenticated-User-Email")
        or request.headers.get("cf-access-authenticated-user-email")
        or request.headers.get("X-Auth-Request-Email")
        or ""
    ).strip().lower()


def _normalize_web_role(value: object, default: str = "operator") -> str:
    token = str(value or default).strip().lower()
    return "admin" if token == "admin" else "operator"


def _to_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = str(value or "").strip().lower()
    if not token:
        return default
    return token in {"1", "true", "yes", "on"}


def _load_web_user_registry() -> dict[str, object]:
    defaults = {
        "local_beta_role": "admin",
        "local_beta_allow_role_switch": True,
        "lan_role": "operator",
        "lan_allow_role_switch": False,
        "access_email_role": "operator",
        "access_email_allow_role_switch": False,
    }
    payload: dict[str, object] = {"defaults": defaults, "users": {}}
    path = WEB_USER_REGISTRY_PATH
    if not path.exists():
        return payload
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return payload
    if not isinstance(raw, dict):
        return payload
    raw_defaults = raw.get("defaults")
    if isinstance(raw_defaults, dict):
        payload["defaults"] = {**defaults, **raw_defaults}
    raw_users = raw.get("users")
    if isinstance(raw_users, dict):
        normalized_users: dict[str, dict[str, object]] = {}
        for key, value in raw_users.items():
            email = str(key or "").strip().lower()
            if not email or not isinstance(value, dict):
                continue
            normalized_users[email] = value
        payload["users"] = normalized_users
    return payload


def resolve_ui_session(request: Request) -> dict[str, object]:
    email = _extract_authenticated_email(request)
    registry = _load_web_user_registry()
    defaults = dict(registry.get("defaults") or {})
    users = dict(registry.get("users") or {})
    user_key, is_local_beta = resolve_ui_user_key(request)
    host = (request.url.hostname or "").strip().lower()

    if is_local_beta:
        role = _normalize_web_role(defaults.get("local_beta_role"), default="admin")
        allow_role_switch = _to_bool(defaults.get("local_beta_allow_role_switch"), default=True)
        return {
            "user_key": user_key,
            "email": "",
            "display_name": "local-beta",
            "role": role,
            "allow_role_switch": allow_role_switch,
            "auth_mode": "local_beta",
            "is_local_beta": True,
            "host": host,
        }

    if email:
        user_record = users.get(email, {})
        role = _normalize_web_role(user_record.get("role") or defaults.get("access_email_role"), default="operator")
        allow_role_switch = _to_bool(
            user_record.get("allow_role_switch"),
            default=_to_bool(defaults.get("access_email_allow_role_switch"), default=False),
        )
        display_name = str(user_record.get("display_name") or email.split("@", 1)[0]).strip() or email
        return {
            "user_key": user_key,
            "email": email,
            "display_name": display_name,
            "role": role,
            "allow_role_switch": allow_role_switch,
            "auth_mode": "cloudflare_access",
            "is_local_beta": False,
            "host": host,
        }

    role = _normalize_web_role(defaults.get("lan_role"), default="operator")
    allow_role_switch = _to_bool(defaults.get("lan_allow_role_switch"), default=False)
    return {
        "user_key": user_key,
        "email": "",
        "display_name": "lan-user",
        "role": role,
        "allow_role_switch": allow_role_switch,
        "auth_mode": "lan",
        "is_local_beta": False,
        "host": host,
    }


def require_admin_request(request: Request) -> dict[str, object]:
    session = resolve_ui_session(request)
    if str(session.get("role") or "") != "admin":
        raise HTTPException(status_code=403, detail="admin role required")
    return session


def resolve_ui_user_key(request: Request) -> tuple[str, bool]:
    email = _extract_authenticated_email(request)
    if email:
        return email, False
    host = (request.url.hostname or "").strip().lower()
    if host in {"127.0.0.1", "localhost"}:
        return "local-beta", True
    forwarded_host = (request.headers.get("host") or "").strip().lower()
    if forwarded_host.startswith("127.0.0.1") or forwarded_host.startswith("localhost"):
        return "local-beta", True
    remote_host = (
        request.headers.get("Cf-Connecting-Ip")
        or request.headers.get("cf-connecting-ip")
        or request.headers.get("X-Forwarded-For")
        or request.headers.get("x-forwarded-for")
        or request.headers.get("X-Real-Ip")
        or request.headers.get("x-real-ip")
        or ""
    ).strip()
    if "," in remote_host:
        remote_host = remote_host.split(",", 1)[0].strip()
    if not remote_host and request.client and request.client.host:
        remote_host = str(request.client.host).strip()
    if remote_host and remote_host not in {"127.0.0.1", "::1", "localhost"}:
        digest = hashlib.sha1(remote_host.encode("utf-8")).hexdigest()[:12]
        return f"lan-{digest}", False
    return "lan-unknown", False


def _safe_storage_label(raw_value: object) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered.startswith(("postgresql://", "postgres://")):
        return "postgres"
    if lowered.startswith("sqlite://"):
        text = text[9:]
    return Path(text).name or "storage"


def _sanitize_storage_details(value: object):
    if isinstance(value, dict):
        return {str(key): _sanitize_storage_details(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_storage_details(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_storage_details(item) for item in value]
    if isinstance(value, str):
        text = value
        patterns = [
            r"postgres(?:ql)?://[^\s\",']+",
            r"sqlite://[^\s\",']+",
            r"(?:[A-Za-z]:\\|/)[^\s\",']+",
        ]
        for pattern in patterns:
            text = re.sub(pattern, lambda match: _safe_storage_label(match.group(0)), text, flags=re.IGNORECASE)
        return text
    return value


def _ensure_product_favorites_dir() -> Path:
    PRODUCT_FAVORITES_DIR.mkdir(parents=True, exist_ok=True)
    return PRODUCT_FAVORITES_DIR


def _product_favorites_path(user_key: str) -> Path:
    digest = hashlib.sha1(str(user_key).strip().lower().encode("utf-8")).hexdigest()[:16]
    return _ensure_product_favorites_dir() / f"{digest}.json"


def _read_product_favorites(user_key: str) -> list[dict[str, str]]:
    path = _product_favorites_path(user_key)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    items = payload.get("items") if isinstance(payload, dict) else payload
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw_item in items or []:
        platform = str((raw_item or {}).get("platform") or "").strip().lower()
        product_id = str((raw_item or {}).get("product_id") or "").strip()
        created_at = str((raw_item or {}).get("created_at") or "").strip()
        if not platform or not product_id:
            continue
        key = (platform, product_id)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "platform": platform,
                "product_id": product_id,
                "created_at": created_at or datetime.utcnow().isoformat(timespec="seconds"),
            }
        )
    return normalized


def _write_product_favorites(user_key: str, items: list[dict[str, str]]) -> None:
    path = _product_favorites_path(user_key)
    temp_path = path.with_suffix(".tmp")
    payload = {
        "version": 1,
        "user_key": user_key,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "items": items,
    }
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def add_product_favorite(user_key: str, platform: str, product_id: str) -> dict[str, str]:
    normalized_platform = str(platform or "").strip().lower()
    normalized_product_id = str(product_id or "").strip()
    if not normalized_platform or not normalized_product_id:
        raise HTTPException(status_code=400, detail="platform and product_id are required")
    with PRODUCT_FAVORITES_LOCK:
        items = _read_product_favorites(user_key)
        items = [
            item
            for item in items
            if not (
                item.get("platform") == normalized_platform
                and item.get("product_id") == normalized_product_id
            )
        ]
        entry = {
            "platform": normalized_platform,
            "product_id": normalized_product_id,
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        items.insert(0, entry)
        _write_product_favorites(user_key, items)
    return entry


def remove_product_favorite(user_key: str, platform: str, product_id: str) -> bool:
    normalized_platform = str(platform or "").strip().lower()
    normalized_product_id = str(product_id or "").strip()
    with PRODUCT_FAVORITES_LOCK:
        items = _read_product_favorites(user_key)
        filtered = [
            item
            for item in items
            if not (
                item.get("platform") == normalized_platform
                and item.get("product_id") == normalized_product_id
            )
        ]
        deleted = len(filtered) != len(items)
        if deleted:
            _write_product_favorites(user_key, filtered)
    return deleted


def fetch_product_favorites_payload(user_key: str, q: str = "") -> dict[str, object]:
    entries = _read_product_favorites(user_key)
    q_normalized = str(q or "").strip().lower()
    items: list[dict[str, object]] = []
    stale_count = 0
    conn = connect_db()
    try:
        for entry in entries:
            row = fetch_one_from_conn(
                conn,
                """
                SELECT
                    {columns}
                FROM {summary_table} summary
                WHERE summary.platform = ? AND summary.product_id = ?
                """.format(columns=PRODUCT_SELECT_COLUMNS, summary_table=WEB_PRODUCT_SUMMARY_TABLE),
                (entry["platform"], entry["product_id"]),
            )
            if not row:
                stale_count += 1
                continue
            haystack = " ".join(
                [
                    str(row.get("title") or ""),
                    str(row.get("brand") or ""),
                    str(row.get("seller_name") or ""),
                    str(row.get("product_id") or ""),
                ]
            ).strip().lower()
            if q_normalized and q_normalized not in haystack:
                continue
            row["favorite_created_at"] = entry["created_at"]
            items.append(row)
    finally:
        conn.close()

    return {
        "items": items,
        "total_count": len(items),
        "summary": {
            "favorite_count": len(items),
            "platform_count": len({str(item.get("platform") or "") for item in items if item.get("platform")}),
            "last_favorited_at": items[0].get("favorite_created_at") if items else "",
            "sales_signal_count": sum(1 for item in items if float(item.get("monthly_sales_estimate") or 0) > 0),
            "stock_signal_count": sum(
                1
                for item in items
                if item.get("has_stock_signal")
                or item.get("inventory_left_estimate")
                or item.get("latest_stock_signal_text")
            ),
            "stale_count": stale_count,
        },
    }


def _warehouse_runtime_label() -> str:
    config = get_warehouse_db_config()
    if config.is_sqlite:
        return _safe_storage_label(config.sqlite_path_or_raise("web warehouse read model"))
    return "postgres"


def normalize_ui_view_name(value: object) -> str:
    token = str(value or "").strip().lower() or "selection"
    if token in {"products", "selection"}:
        return "selection"
    if token in {"category_products", "keyword_products"}:
        return token
    return "selection"


def normalize_signal_text(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def encode_signal_option_key(group: str, signal_text: str) -> str:
    normalized = normalize_signal_text(signal_text)
    return f"{group}:{normalized.encode('utf-8').hex()}"


def decode_signal_option_key(token: str) -> tuple[str, str] | None:
    raw = str(token or "").strip()
    if ":" not in raw:
        return None
    group, hex_value = raw.split(":", 1)
    if not group or not hex_value:
        return None
    try:
        text = bytes.fromhex(hex_value).decode("utf-8")
    except Exception:
        return None
    normalized = normalize_signal_text(text)
    if not normalized:
        return None
    return group.strip().lower(), normalized


def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    conn = connect_db()
    try:
        return [_row_to_dict(row) for row in _execute_fetchall(conn, sql, params)]
    finally:
        conn.close()


def fetch_one(sql: str, params: tuple = ()) -> dict:
    conn = connect_db()
    try:
        row = _execute_fetchone(conn, sql, params)
        return _row_to_dict(row)
    finally:
        conn.close()


def fetch_all_from_conn(conn, sql: str, params: tuple = ()) -> list[dict]:
    return [_row_to_dict(row) for row in _execute_fetchall(conn, sql, params)]


def fetch_one_from_conn(conn, sql: str, params: tuple = ()) -> dict:
    row = _execute_fetchone(conn, sql, params)
    return _row_to_dict(row)


def neutralize_csv_cell(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    text = str(value)
    if not text:
        return ""
    if text.startswith(CSV_FORMULA_LEADING_CHARS):
        return "'" + text
    stripped = text.lstrip(" ")
    if stripped and stripped[0] in CSV_FORMULA_PREFIXES:
        return "'" + text
    return text


def _parse_iso_datetime(raw_value: object) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _age_seconds(raw_value: object) -> int | None:
    parsed = _parse_iso_datetime(raw_value)
    if not parsed:
        return None
    now = datetime.now(parsed.tzinfo) if parsed.tzinfo is not None else datetime.now()
    return max(0, int((now - parsed).total_seconds()))


def _classify_freshness(age_seconds: int | None) -> str:
    if age_seconds is None:
        return "unknown"
    if age_seconds <= SYNC_FRESH_SECONDS:
        return "fresh"
    if age_seconds <= SYNC_DELAYED_SECONDS:
        return "delayed"
    return "stale"


def _build_stage_sync_payload(
    *,
    scope: str,
    imported_at: object,
    observed_at: object,
) -> dict[str, object]:
    import_age_seconds = _age_seconds(imported_at)
    observed_age_seconds = _age_seconds(observed_at)
    return {
        "scope": scope,
        "last_imported_at": str(imported_at or ""),
        "last_import_age_seconds": import_age_seconds,
        "import_freshness_state": _classify_freshness(import_age_seconds),
        "last_observed_at": str(observed_at or ""),
        "last_observed_age_seconds": observed_age_seconds,
        "observation_freshness_state": _classify_freshness(observed_age_seconds),
    }


def _build_stage_diagnosis(
    *,
    scope: str,
    imported_at: object,
    observed_at: object,
    shared_sync: dict[str, object],
) -> dict[str, str]:
    imported_age_seconds = _age_seconds(imported_at)
    observed_age_seconds = _age_seconds(observed_at)
    shared_state = str(shared_sync.get("state") or shared_sync.get("status") or "unknown").strip().lower()
    shared_updated_at = str(shared_sync.get("updated_at") or "")
    scope_label = "类目 / Category" if scope == "category_stage" else "关键词 / Keyword"
    if not imported_at and not observed_at:
        return {
            "diagnosis_code": "no_warehouse_data",
            "diagnosis_state": "warning",
            "diagnosis_summary": f"{scope_label} 在统一仓库中还没有可见数据 / No warehouse-visible data for {scope_label}.",
            "recommended_action": f"先确认对应爬虫是否已产出并完成入仓 / Verify the {scope_label} runtime produced data and completed warehouse sync.",
        }

    if shared_state == "failed":
        return {
            "diagnosis_code": "shared_sync_failed",
            "diagnosis_state": "warning",
            "diagnosis_summary": f"{scope_label} 最新状态无法确认，因为共享同步最近失败 / Latest {scope_label} state is uncertain because shared warehouse sync failed.",
            "recommended_action": "先查看共享同步失败原因，再决定是否重跑入仓 / Inspect the shared sync failure reason before rerunning sync.",
        }

    if shared_state == "queued" and not shared_updated_at:
        return {
            "diagnosis_code": "runtime_sync_unreported",
            "diagnosis_state": "attention",
            "diagnosis_summary": f"{scope_label} 当前只有 warehouse 视角，还没有 runtime 同步上报 / Only warehouse-visible state is available for {scope_label}; runtime sync has not reported yet.",
            "recommended_action": "等待对应窗口接入 shared warehouse sync contract；当前不要把该状态当成 runtime 真相 / Wait for runtime adoption of the shared sync contract before treating this as runtime truth.",
        }

    if imported_age_seconds is None and observed_age_seconds is not None:
        return {
            "diagnosis_code": "observations_without_import_marker",
            "diagnosis_state": "warning",
            "diagnosis_summary": f"{scope_label} 有观测数据，但缺少导入时间标记 / {scope_label} observations exist without a warehouse import marker.",
            "recommended_action": "检查 source_databases 记录和入仓写入流程 / Check source_databases rows and warehouse import writes.",
        }

    if imported_age_seconds is not None and imported_age_seconds > SYNC_DELAYED_SECONDS:
        return {
            "diagnosis_code": "warehouse_import_stale",
            "diagnosis_state": "warning",
            "diagnosis_summary": f"{scope_label} 在 warehouse 中的数据偏旧 / {scope_label} data looks stale inside warehouse.",
            "recommended_action": f"检查 {scope_label} 爬虫是否继续运行，以及最近一次共享同步是否完成 / Verify the {scope_label} runtime is still producing data and the latest shared sync completed.",
        }

    if observed_age_seconds is not None and observed_age_seconds > SYNC_DELAYED_SECONDS:
        return {
            "diagnosis_code": "warehouse_observation_stale",
            "diagnosis_state": "attention",
            "diagnosis_summary": f"{scope_label} 最近观测时间偏旧 / Latest observed {scope_label} data in warehouse looks stale.",
            "recommended_action": f"优先检查爬虫产出是否停滞，再检查入仓频率 / Check whether the {scope_label} runtime stalled before tuning warehouse sync cadence.",
        }

    return {
        "diagnosis_code": "warehouse_visible_ok",
        "diagnosis_state": "healthy",
        "diagnosis_summary": f"{scope_label} 在 warehouse 里的可见状态正常 / Warehouse-visible state for {scope_label} looks healthy.",
        "recommended_action": "继续观察 shared sync 状态；若需要 runtime 真相，以 shared sync 为准 / Continue watching shared sync state; use shared sync as runtime truth when available.",
    }


def _normalize_shared_sync_state(status: object, skip_reason: object, updated_age_seconds: object) -> str:
    status_text = str(status or "").strip().lower()
    skip_reason_text = str(skip_reason or "").strip().lower()
    has_timestamp = updated_age_seconds is not None

    if status_text == "running":
        return "running"
    if status_text == "completed":
        return "completed"
    if status_text == "failed":
        return "failed"
    if status_text == "skipped" or skip_reason_text == "lock_active":
        return "skipped_due_to_active_lock"
    if status_text in {"idle", "unknown", ""} and not has_timestamp:
        return "queued"
    if has_timestamp:
        return "completed"
    return "queued"


def _build_shared_sync_status_payload() -> dict[str, object]:
    payload = read_sync_state()
    updated_at = payload.get("updated_at") or payload.get("finished_at") or payload.get("started_at") or ""
    updated_age_seconds = _age_seconds(updated_at)
    freshness_state = _classify_freshness(updated_age_seconds)
    raw_status = str(payload.get("status") or "unknown").strip().lower()
    skip_reason = str(payload.get("skip_reason") or "").strip().lower()
    if raw_status == "running":
        freshness_state = "active"
    elif raw_status == "failed":
        freshness_state = "warning"
    elif raw_status == "skipped":
        freshness_state = "attention"
    elif raw_status in {"idle", "unknown"} and updated_age_seconds is None:
        freshness_state = "unknown"

    normalized_state = _normalize_shared_sync_state(raw_status, skip_reason, updated_age_seconds)
    status_summary_map = {
        "running": "共享同步正在进行 / Shared warehouse sync is running.",
        "failed": "共享同步最近失败 / Shared warehouse sync failed recently.",
        "skipped_due_to_active_lock": "共享同步因有效锁被跳过 / Shared warehouse sync was skipped because the lock is active.",
        "queued": "共享同步已排队或尚未上报 / Shared warehouse sync is queued or not yet reported.",
        "completed": "共享同步已完成 / Shared warehouse sync completed.",
    }
    recommended_action_map = {
        "running": "等待当前同步完成，再判断 warehouse 是否落后 / Wait for the active sync to finish before judging warehouse lag.",
        "failed": "先处理失败原因，再决定是否重跑同步 / Resolve the sync failure cause before rerunning.",
        "skipped_due_to_active_lock": "检查锁持有者和 skip reason，再决定是否补跑同步 / Inspect the lock holder before rerunning another sync.",
        "queued": "当前先以 warehouse 已可见数据为准，并继续观察下一次正式同步 / Rely on warehouse-visible data until runtime windows adopt the shared sync contract.",
        "completed": "继续观察最近更新时间和 stale 提示 / Continue monitoring updated time and stale indicators.",
    }
    return {
        "status": raw_status,
        "state": normalized_state,
        "actor": payload.get("actor") or "",
        "reason": payload.get("reason") or "",
        "skip_reason": payload.get("skip_reason") or "",
        "error": payload.get("error") or "",
        "requested_at": str(payload.get("requested_at") or ""),
        "started_at": str(payload.get("started_at") or ""),
        "finished_at": str(payload.get("finished_at") or ""),
        "updated_at": str(updated_at or ""),
        "updated_age_seconds": updated_age_seconds,
        "freshness_state": freshness_state,
        "warehouse_db": _safe_storage_label(payload.get("warehouse_db")),
        "trigger_db": _safe_storage_label(payload.get("trigger_db")),
        "metadata": _sanitize_storage_details(payload.get("metadata") or {}),
        "status_summary": status_summary_map.get(normalized_state, "共享同步状态未知 / Shared warehouse sync state is unknown."),
        "recommended_action": recommended_action_map.get(normalized_state, "继续观察 shared sync 与 warehouse 可见性 / Continue monitoring shared sync and warehouse visibility."),
    }


def compute_sync_visibility_payload() -> dict[str, object]:
    overview = fetch_one(
        """
        SELECT
            (SELECT MAX(imported_at) FROM source_databases) AS warehouse_last_imported_at,
            (SELECT MAX(imported_at) FROM source_databases WHERE source_scope = 'category_stage') AS category_last_imported_at,
            (SELECT MAX(imported_at) FROM source_databases WHERE source_scope = 'keyword_stage') AS keyword_last_imported_at,
            (SELECT MAX(scraped_at) FROM observation_events WHERE source_type = 'category') AS category_last_observed_at,
            (SELECT MAX(scraped_at) FROM observation_events WHERE source_type = 'keyword') AS keyword_last_observed_at
        """
    )
    warehouse_last_imported_at = overview.get("warehouse_last_imported_at") or ""
    warehouse_last_import_age_seconds = _age_seconds(warehouse_last_imported_at)
    shared_sync = _build_shared_sync_status_payload()
    shared_sync_state = str(shared_sync.get("state") or shared_sync.get("status") or "queued").strip().lower()
    category_stage = _build_stage_sync_payload(
        scope="category_stage",
        imported_at=overview.get("category_last_imported_at"),
        observed_at=overview.get("category_last_observed_at"),
    )
    keyword_stage = _build_stage_sync_payload(
        scope="keyword_stage",
        imported_at=overview.get("keyword_last_imported_at"),
        observed_at=overview.get("keyword_last_observed_at"),
    )
    category_stage.update(
        _build_stage_diagnosis(
            scope="category_stage",
            imported_at=overview.get("category_last_imported_at"),
            observed_at=overview.get("category_last_observed_at"),
            shared_sync=shared_sync,
        )
    )
    keyword_stage.update(
        _build_stage_diagnosis(
            scope="keyword_stage",
            imported_at=overview.get("keyword_last_imported_at"),
            observed_at=overview.get("keyword_last_observed_at"),
            shared_sync=shared_sync,
        )
    )
    warehouse_visible_freshness = _classify_freshness(warehouse_last_import_age_seconds)
    stage_import_states = [
        str(category_stage.get("import_freshness_state") or "unknown").strip().lower(),
        str(keyword_stage.get("import_freshness_state") or "unknown").strip().lower(),
    ]
    if "stale" in stage_import_states:
        stage_import_freshness = "stale"
    elif "delayed" in stage_import_states:
        stage_import_freshness = "delayed"
    elif stage_import_states and all(state == "fresh" for state in stage_import_states):
        stage_import_freshness = "fresh"
    else:
        stage_import_freshness = "unknown"
    operator_freshness_state = warehouse_visible_freshness
    if operator_freshness_state == "unknown":
        operator_freshness_state = str(shared_sync.get("freshness_state") or "unknown").strip().lower()
    if shared_sync_state == "failed":
        operator_freshness_state = "stale"
    elif shared_sync_state in {"queued", "skipped_due_to_active_lock"} and operator_freshness_state == "fresh":
        operator_freshness_state = "delayed"
    warnings: list[str] = []
    diagnostic_notes: list[str] = []
    if shared_sync_state == "failed":
        warnings.append("统一仓库最近一次同步失败 / Latest warehouse sync failed.")
    if shared_sync_state == "queued" and not shared_sync["updated_at"]:
        warnings.append("共享同步状态尚未上报 / Shared warehouse sync has not reported yet.")
    if shared_sync_state == "skipped_due_to_active_lock":
        warnings.append("共享同步因锁冲突被跳过 / Shared warehouse sync was skipped because the lock is active.")
    if warehouse_visible_freshness == "stale":
        warnings.append("统一仓库可见数据偏旧 / Warehouse-visible freshness looks stale.")
    if category_stage["import_freshness_state"] == "stale":
        diagnostic_notes.append("类目 stage 导入时间偏旧，但不直接代表 operator freshness / Category stage import marker is stale but diagnostic only.")
    if keyword_stage["import_freshness_state"] == "stale":
        diagnostic_notes.append("关键词 stage 导入时间偏旧，但不直接代表 operator freshness / Keyword stage import marker is stale but diagnostic only.")

    overall_state = "healthy"
    if shared_sync_state == "failed" or operator_freshness_state == "stale":
        overall_state = "warning"
    elif (
        operator_freshness_state in {"delayed", "unknown"}
        or shared_sync_state in {"queued", "skipped_due_to_active_lock"}
        or shared_sync["freshness_state"] in {"attention", "unknown"}
    ):
        overall_state = "attention"
    visibility_scope_note = (
        "当前 freshness 视图只代表 warehouse 已可见状态；runtime 真相需要同时看 shared sync 状态。"
        " / Current freshness reflects warehouse-visible state only; runtime truth must be judged together with shared sync state."
    )
    return {
        "warehouse": {
            "last_imported_at": warehouse_last_imported_at,
            "last_import_age_seconds": warehouse_last_import_age_seconds,
            "import_freshness_state": warehouse_visible_freshness,
            "status_summary": (
                "统一仓库导入记录可见 / Warehouse import timestamp is visible."
                if warehouse_last_imported_at
                else "统一仓库暂时没有导入记录 / No warehouse import timestamp is visible yet."
            ),
        },
        "category_stage": category_stage,
        "keyword_stage": keyword_stage,
        "shared_sync": shared_sync,
        "shared_sync_state": shared_sync_state,
        "truth_source": "warehouse_visibility_plus_shared_sync",
        "warehouse_visible_freshness": warehouse_visible_freshness,
        "stage_import_freshness": stage_import_freshness,
        "operator_freshness_state": operator_freshness_state,
        "overall_state": overall_state,
        "visibility_scope_note": visibility_scope_note,
        "warnings": warnings,
        "diagnostic_notes": diagnostic_notes,
    }


def load_seed_catalog() -> dict:
    if not DEFAULT_SEED_CATALOG_PATH.exists():
        return {"groups": []}
    try:
        payload = json.loads(DEFAULT_SEED_CATALOG_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {"groups": []}
    except Exception:
        return {"groups": []}


def fetch_keyword_opportunity_rows(limit: int = 200) -> list[dict]:
    return fetch_all(
        f"""
        {KEYWORD_RANKED_METRICS_CTE},
        latest_metrics AS (
            SELECT * FROM ranked_metrics WHERE rn = 1
        ),
        previous_metrics AS (
            SELECT
                keyword,
                analyzed_at AS previous_analyzed_at,
                total_score AS previous_total_score
            FROM ranked_metrics
            WHERE rn = 2
        )
        SELECT
            kc.keyword,
            COALESCE(kc.display_keyword, kc.keyword) AS display_keyword,
            kc.status,
            kc.tracking_mode,
            kc.source_type,
            kc.source_platform,
            kc.priority,
            kc.metadata_json,
            lm.analyzed_at AS latest_analyzed_at,
            lm.grade,
            lm.rank,
            lm.total_score AS latest_total_score,
            pm.previous_total_score,
            lm.demand_index,
            lm.competition_density,
            lm.supply_gap_ratio,
            lm.margin_war_pct,
            lm.margin_peace_pct,
            lm.noon_total,
            lm.amazon_total,
            COUNT(DISTINCT pkm.platform || '|' || pkm.product_id) AS matched_product_count
        FROM keyword_catalog kc
        LEFT JOIN latest_metrics lm
          ON lm.keyword = kc.keyword
        LEFT JOIN previous_metrics pm
          ON pm.keyword = kc.keyword
        LEFT JOIN product_keyword_membership pkm
          ON pkm.keyword = kc.keyword
        GROUP BY
            kc.keyword,
            kc.display_keyword,
            kc.status,
            kc.tracking_mode,
            kc.source_type,
            kc.source_platform,
            kc.priority,
            kc.metadata_json,
            lm.analyzed_at,
            lm.grade,
            lm.rank,
            lm.total_score,
            pm.previous_total_score,
            lm.demand_index,
            lm.competition_density,
            lm.supply_gap_ratio,
            lm.margin_war_pct,
            lm.margin_peace_pct,
            lm.noon_total,
            lm.amazon_total
        ORDER BY COALESCE(lm.total_score, 0) DESC, kc.keyword ASC
        LIMIT ?
        """,
        (limit,),
    )


def fetch_keyword_graph_rows(root_keyword: str, *, depth: int = 2, limit: int = 200) -> list[dict]:
    return fetch_all(
        """
        WITH RECURSIVE graph_edges(depth, parent_keyword, child_keyword, source_platform, source_type, discovered_at) AS (
            SELECT
                1 AS depth,
                parent_keyword,
                child_keyword,
                source_platform,
                source_type,
                discovered_at
            FROM keyword_expansion_edges
            WHERE parent_keyword = ?
            UNION ALL
            SELECT
                graph_edges.depth + 1,
                ke.parent_keyword,
                ke.child_keyword,
                ke.source_platform,
                ke.source_type,
                ke.discovered_at
            FROM keyword_expansion_edges ke
            JOIN graph_edges
              ON ke.parent_keyword = graph_edges.child_keyword
            WHERE graph_edges.depth < ?
        )
        SELECT *
        FROM graph_edges
        ORDER BY depth ASC, discovered_at DESC, parent_keyword ASC, child_keyword ASC
        LIMIT ?
        """,
        (root_keyword, depth, limit),
    )


def fetch_keyword_graph_metric_rows(keywords: list[str]) -> list[dict]:
    if not keywords:
        return []

    placeholders = ", ".join("?" for _ in keywords)
    return fetch_all(
        f"""
        {KEYWORD_RANKED_METRICS_CTE}
        SELECT
            kc.keyword,
            COALESCE(kc.display_keyword, kc.keyword) AS display_keyword,
            kc.tracking_mode,
            kc.source_type,
            kc.source_platform,
            kc.metadata_json,
            rm.grade,
            rm.rank,
            rm.total_score,
            rm.demand_index,
            rm.competition_density,
            rm.supply_gap_ratio,
            rm.margin_war_pct,
            rm.margin_peace_pct,
            rm.noon_total,
            rm.amazon_total,
            COUNT(DISTINCT pkm.platform || '|' || pkm.product_id) AS matched_product_count
        FROM keyword_catalog kc
        LEFT JOIN ranked_metrics rm
          ON rm.keyword = kc.keyword
         AND rm.rn = 1
        LEFT JOIN product_keyword_membership pkm
          ON pkm.keyword = kc.keyword
        WHERE kc.keyword IN ({placeholders})
        GROUP BY
            kc.keyword,
            kc.display_keyword,
            kc.tracking_mode,
            kc.source_type,
            kc.source_platform,
            kc.metadata_json,
            rm.grade,
            rm.rank,
            rm.total_score,
            rm.demand_index,
            rm.competition_density,
            rm.supply_gap_ratio,
            rm.margin_war_pct,
            rm.margin_peace_pct,
            rm.noon_total,
            rm.amazon_total
        """,
        tuple(keywords),
    )


def parse_json_value(raw_value):
    if raw_value in (None, "", "null"):
        return None
    try:
        return json.loads(raw_value)
    except (TypeError, ValueError):
        return None


def _read_json_file(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalize_watchdog_severity(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"critical", "warning", "info", "ok"}:
        return text
    return "unknown"


def _build_watchdog_summary_payload() -> dict[str, object]:
    payload = _read_json_file(WATCHDOG_LATEST_JSON_PATH)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    issues = payload.get("issues") if isinstance(payload.get("issues"), list) else []
    normalized_issues: list[dict[str, object]] = []
    severity_counter = {"critical": 0, "warning": 0, "info": 0}
    for raw_issue in issues:
        if not isinstance(raw_issue, dict):
            continue
        severity = _normalize_watchdog_severity(raw_issue.get("severity"))
        if severity in severity_counter:
            severity_counter[severity] += 1
        normalized_issues.append(
            {
                "check": str(raw_issue.get("check") or "").strip(),
                "severity": severity,
                "message": str(raw_issue.get("message") or "").strip(),
            }
        )
    status = _normalize_watchdog_severity(summary.get("status"))
    latest_report_age_hours = summary.get("latest_report_age_hours")
    try:
        latest_report_age_hours = float(latest_report_age_hours) if latest_report_age_hours is not None else None
    except (TypeError, ValueError):
        latest_report_age_hours = None
    return {
        "exists": bool(payload),
        "generated_at": str(summary.get("generated_at") or payload.get("generated_at") or ""),
        "status": status,
        "state": status,
        "release": str(summary.get("release") or ""),
        "issue_count": int(summary.get("issue_count") or len(normalized_issues) or 0),
        "severity_breakdown": severity_counter,
        "issues": normalized_issues,
        "report_missing_dates": list(summary.get("report_missing_dates") or []),
        "historical_report_missing_dates": list(summary.get("historical_report_missing_dates") or []),
        "current_report_missing": bool(summary.get("current_report_missing")) if payload else False,
        "current_report_stale": bool(summary.get("current_report_stale")) if payload else False,
        "current_alert_summary": summary.get("current_alert_summary") if isinstance(summary.get("current_alert_summary"), dict) else {},
        "latest_report_age_hours": latest_report_age_hours,
        "api_health_ok": bool(summary.get("api_health_ok")) if payload else False,
        "system_health_ok": bool(summary.get("system_health_ok")) if payload else False,
        "host_alternating_service_active": bool(summary.get("host_alternating_service_active")) if payload else False,
        "auto_dispatch_entry": str(summary.get("auto_dispatch_entry") or ""),
        "scheduler_heartbeat_ok": bool(summary.get("scheduler_heartbeat_ok")) if payload else False,
        "canonical_auto_plans": list(summary.get("canonical_auto_plans") or []) if payload else [],
        "auto_dispatch_conflict": bool(summary.get("auto_dispatch_conflict")) if payload else False,
        "stale_worker_count": int(summary.get("stale_worker_count") or 0),
        "stale_running_task_count": int(summary.get("stale_running_task_count") or 0),
    }


def _classify_keyword_quality_state(
    *,
    run_status: str,
    crawl_status: str,
    platform_stats: dict[str, object],
    errors: list[object],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    normalized_run_status = str(run_status or "").strip().lower()
    normalized_crawl_status = str(crawl_status or "").strip().lower()
    if normalized_run_status == "failed":
        return "degraded", ["run_failed"]
    if normalized_run_status not in {"completed", "partial"}:
        return "unknown", []

    platform_quality: list[str] = []
    for platform, raw_stats in platform_stats.items():
        if not isinstance(raw_stats, dict):
            continue
        platform_name = str(platform or "").strip().lower() or "unknown"
        status = str(raw_stats.get("status") or "").strip().lower()
        error_text = str(raw_stats.get("error") or "").strip().lower()
        error_evidence = [str(item or "").strip().lower() for item in list(raw_stats.get("error_evidence") or [])]
        failed_keywords = list(raw_stats.get("failed_keywords") or [])
        products_count = int(raw_stats.get("products_count") or 0)
        zero_result_keywords = list(raw_stats.get("zero_result_keywords") or [])

        if status == "failed":
            platform_quality.append("degraded")
            reasons.append(f"{platform_name}_failed")
            continue
        if "beautifulsoup4_unavailable" in error_text or any(
            "beautifulsoup4_unavailable" in item for item in error_evidence
        ):
            platform_quality.append("degraded")
            reasons.append(f"{platform_name}_beautifulsoup4_unavailable")
            continue
        if failed_keywords:
            platform_quality.append("partial")
            reasons.append(f"{platform_name}_failed_keywords")
            continue
        if status == "partial":
            platform_quality.append("partial")
            reasons.append(f"{platform_name}_partial")
            continue
        if status == "zero_results" and not products_count and zero_result_keywords:
            platform_quality.append("partial")
            reasons.append(f"{platform_name}_zero_results")
            continue
        platform_quality.append("full")

    if normalized_crawl_status == "failed":
        reasons.append("crawl_status_failed")
        return "degraded", dedupe_texts(reasons)
    if errors:
        reasons.append("monitor_errors_present")
    if "degraded" in platform_quality:
        return "degraded", dedupe_texts(reasons)
    if normalized_run_status == "partial" or normalized_crawl_status == "partial" or errors or "partial" in platform_quality:
        return "partial", dedupe_texts(reasons)
    if platform_quality:
        return "full", dedupe_texts(reasons)
    return "unknown", dedupe_texts(reasons)


def _fallback_keyword_quality_source_breakdown(
    *,
    crawl_status: str,
    platform_stats: dict[str, object],
    errors: list[object],
) -> dict[str, object]:
    sources: dict[str, object] = {}
    crawl_reasons: list[str] = []
    for platform, raw_stats in platform_stats.items():
        if not isinstance(raw_stats, dict):
            continue
        platform_name = str(platform or "").strip().lower() or "unknown"
        status = str(raw_stats.get("status") or "unknown").strip().lower()
        reason_codes: list[str] = []
        error_evidence = [str(item or "").strip().lower() for item in list(raw_stats.get("error_evidence") or [])]
        if "beautifulsoup4_unavailable" in str(raw_stats.get("error") or "").strip().lower() or any(
            "beautifulsoup4_unavailable" in item for item in error_evidence
        ):
            reason_codes.append("dependency_missing")
        elif status == "failed":
            reason_codes.append("runtime_error")
        elif status == "partial":
            reason_codes.append("partial_results")
        elif status == "zero_results":
            reason_codes.append("zero_results")
        for code in reason_codes:
            crawl_reasons.append(f"{platform_name}:{code}")
        sources[platform_name] = {
            "state": "degraded" if status == "failed" else "partial" if status in {"partial", "zero_results"} else "full",
            "status": status,
            "reason_codes": reason_codes,
            "primary_reason": reason_codes[0] if reason_codes else "",
            "products_count": int(raw_stats.get("products_count") or 0),
            "total_results": int(raw_stats.get("total_results") or 0),
        }
    sources["crawl"] = {
        "state": "degraded" if str(crawl_status or "").strip().lower() == "failed" else "partial" if errors else "full",
        "reason_codes": dedupe_texts(crawl_reasons),
        "primary_reason": dedupe_texts(crawl_reasons)[0] if crawl_reasons else "",
    }
    return sources


def _summarize_keyword_quality(recent_runs: list[dict[str, object]]) -> dict[str, object]:
    summary = build_keyword_quality_truth_model(recent_runs)
    live_task_count = 0
    live_snapshot_id = ""
    live_seed_keyword = ""
    live_task_status = ""
    with closing(connect_ops_store()) as store:
        active_keyword_tasks: list[dict[str, object]] = []
        for status_name in ("running", "leased", "pending", "retrying"):
            active_keyword_tasks.extend(store.list_tasks(status=status_name, worker_type="keyword", limit=20))
        live_task_count = len(active_keyword_tasks)
        if active_keyword_tasks:
            active_task = dict(active_keyword_tasks[0] or {})
            payload = dict(active_task.get("payload") or {})
            live_snapshot_id = str(payload.get("snapshot") or "")
            live_seed_keyword = str(payload.get("monitor_seed_keyword") or "")
            live_task_status = str(active_task.get("status") or "").strip().lower()

    lock_payload = collect_lock_payload(KEYWORD_MONITOR_DIR / "keyword_monitor.lock")
    summary_payload = collect_summary_payload(KEYWORD_MONITOR_DIR / "keyword_monitor_last_run.json")
    batch_logs_payload = collect_batch_logs(KEYWORD_MONITOR_DIR / "batch_logs", preview_lines=0)

    monitor_state = str(lock_payload.get("state") or "idle").strip().lower()
    if monitor_state == "idle" and summary_payload.get("exists"):
        monitor_state = "idle_with_summary"
    latest_log_age_seconds = batch_logs_payload.get("latest_log_age_seconds")
    if live_task_count > 0:
        if live_task_status == "pending":
            activity_state = "queued"
        else:
            activity_state = "running"
    elif monitor_state == "running":
        if latest_log_age_seconds is None:
            activity_state = "running_no_logs"
        elif int(latest_log_age_seconds) <= int(ACTIVE_LOG_WINDOW_SECONDS):
            activity_state = "running_active_logs"
        else:
            activity_state = "running_quiet_logs"
    elif monitor_state == "stale_lock":
        activity_state = "stale_lock"
    elif monitor_state == "idle_with_summary":
        activity_state = "idle_with_summary"
    else:
        activity_state = "idle"

    active_overlay_states = {
        "queued",
        "running",
        "running_no_logs",
        "running_active_logs",
        "running_quiet_logs",
        "stale_lock",
    }
    live_quality = build_quality_payload(
        summary_payload,
        live_batch_state=activity_state,
        latest_terminal_quality_state=str(
            summary.get("latest_terminal_batch_state")
            or summary.get("latest_terminal_quality_state")
            or ""
        ),
        operator_quality_state=str(summary.get("operator_quality_state") or ""),
    )

    summary_active_snapshot_count = int(summary.get("active_snapshot_count") or 0)
    summary_stale_running_snapshot_count = int(summary.get("stale_running_snapshot_count") or 0)

    if activity_state in active_overlay_states:
        summary["truth_source"] = "keyword_snapshot_batch_aggregate+runtime_monitor_overlay"
        summary["live_batch_state"] = str(live_quality.get("live_batch_state") or activity_state)
        summary["live_batch_snapshot_id"] = live_snapshot_id or str(lock_payload.get("snapshot_id") or "")
        summary["live_seed_keyword"] = live_seed_keyword or str(summary.get("live_seed_keyword") or "")
        summary["live_quality_reasons"] = list(live_quality.get("quality_reasons") or summary.get("live_quality_reasons") or [])
        summary["live_quality_evidence"] = list(live_quality.get("quality_evidence") or summary.get("live_quality_evidence") or [])
        summary["live_quality_source_breakdown"] = dict(
            live_quality.get("quality_source_breakdown") or summary.get("live_quality_source_breakdown") or {}
        )
        summary["active_snapshot_count"] = max(summary_active_snapshot_count, max(live_task_count, 1))
        summary["stale_running_snapshot_count"] = max(
            0,
            summary["active_snapshot_count"] - 1,
        )
    else:
        summary["active_snapshot_count"] = live_task_count
        summary["stale_running_snapshot_count"] = max(
            summary_stale_running_snapshot_count,
            max(summary_active_snapshot_count - live_task_count, 0),
        )

    return summary


def dedupe_texts(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def build_raw_signal_texts(detail_row: dict) -> list[str]:
    candidates: list[str] = []

    for field in (
        "lowest_price_signal_text",
        "stock_signal_text",
        "sold_recently_text",
        "ranking_signal_text",
        "delivery_eta_signal_text",
    ):
        if detail_row.get(field):
            candidates.append(detail_row[field])

    badge_texts = parse_json_value(detail_row.get("badge_texts_json"))
    if isinstance(badge_texts, list):
        candidates.extend(str(item) for item in badge_texts)

    public_signals = parse_json_value(detail_row.get("public_signals_json"))
    if isinstance(public_signals, dict):
        if public_signals.get("sold_recently_text"):
            candidates.append(public_signals["sold_recently_text"])
        elif public_signals.get("sold_recently_count"):
            candidates.append(f"{public_signals['sold_recently_count']} sold recently")

        if public_signals.get("stock_signal_text"):
            candidates.append(public_signals["stock_signal_text"])
        elif public_signals.get("stock_left_count"):
            candidates.append(f"Only {public_signals['stock_left_count']} left in stock")

        if public_signals.get("ranking_signal_text"):
            candidates.append(public_signals["ranking_signal_text"])

        if isinstance(public_signals.get("badge_texts"), list):
            candidates.extend(str(item) for item in public_signals["badge_texts"])

    last_public_signals = parse_json_value(detail_row.get("last_public_signals_json"))
    if isinstance(last_public_signals, dict):
        for key in ("sold_recently_text", "stock_signal_text", "ranking_signal_text"):
            if last_public_signals.get(key):
                candidates.append(last_public_signals[key])
        if isinstance(last_public_signals.get("badge_texts"), list):
            candidates.extend(str(item) for item in last_public_signals["badge_texts"])

    return dedupe_texts(candidates)


def _pick_effective_signal_text(*values: object) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def resolve_effective_signal_payload(summary: dict, detail_row: dict) -> dict[str, object]:
    latest_row = dict(detail_row or {})
    latest_seen_at = latest_row.get("scraped_at") or summary.get("latest_observed_at")
    payload: dict[str, object] = {}
    raw_signal_texts = build_raw_signal_texts(latest_row)

    for signal_name in (
        "delivery_eta_signal_text",
        "lowest_price_signal_text",
        "sold_recently_text",
        "stock_signal_text",
        "ranking_signal_text",
    ):
        latest_value = _pick_effective_signal_text(latest_row.get(signal_name), summary.get(f"latest_{signal_name}"))
        sticky_value = _pick_effective_signal_text(summary.get(f"sticky_{signal_name}"))
        effective_value = latest_value or sticky_value
        if latest_value:
            last_seen_at = latest_seen_at
        elif sticky_value:
            last_seen_at = summary.get(_sticky_seen_field(signal_name))
        else:
            last_seen_at = None
        payload[signal_name] = effective_value
        payload[f"{signal_name}_last_seen_at"] = last_seen_at
        payload[f"{signal_name}_is_sticky"] = bool((not latest_value) and sticky_value)
        if effective_value:
            raw_signal_texts.append(effective_value)

    payload["raw_signal_texts"] = dedupe_texts(raw_signal_texts)
    return payload


def parse_signal_tag_tokens(raw_value: str | None) -> list[tuple[str, str]]:
    parsed: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for chunk in str(raw_value or "").split(","):
        decoded = decode_signal_option_key(chunk)
        if decoded is None or decoded in seen:
            continue
        seen.add(decoded)
        parsed.append(decoded)
    return parsed


def normalize_category_scope_paths(
    category_path: str = "",
    selected_category_paths: list[str] | None = None,
) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    legacy_path = str(category_path or "").strip()
    if legacy_path:
        seen.add(legacy_path)
        normalized.append(legacy_path)
    for raw_path in selected_category_paths or []:
        decoded = str(raw_path or "").strip()
        if not decoded or decoded in seen:
            continue
        seen.add(decoded)
        normalized.append(decoded)
    return normalized


def _summary_signal_case_sql(signal_name: str) -> str:
    return f"LOWER(TRIM(COALESCE(NULLIF(summary.latest_{signal_name}, ''), NULLIF(summary.sticky_{signal_name}, ''), '')))"


def build_product_filters(
    *,
    q: str = "",
    market: str = "",
    platform: str = "",
    source_scope: str = "",
    delivery_type: str = "",
    is_ad: int | None = None,
    tab: str = "all",
    bsr_min: int | None = None,
    bsr_max: int | None = None,
    review_min: int | None = None,
    review_max: int | None = None,
    rating_min: float | None = None,
    rating_max: float | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    sales_min: int | None = None,
    sales_max: int | None = None,
    inventory_min: int | None = None,
    inventory_max: int | None = None,
    review_growth_7d_min: int | None = None,
    review_growth_14d_min: int | None = None,
    rating_growth_7d_min: float | None = None,
    rating_growth_14d_min: float | None = None,
    has_sold_signal: bool | None = None,
    has_stock_signal: bool | None = None,
    has_lowest_price_signal: bool | None = None,
    signal_tags: str = "",
    signal_text: str = "",
    category_path: str = "",
    selected_category_paths: list[str] | None = None,
    keyword: str = "",
) -> tuple[str, list]:
    where_parts = ["1=1"]
    params: list = []

    if q.strip():
        where_parts.append(
            """
            (
                summary.title LIKE ?
                OR COALESCE(summary.brand, '') LIKE ?
                OR COALESCE(summary.seller_name, '') LIKE ?
                OR summary.product_id LIKE ?
            )
            """
        )
        like_value = f"%{q.strip()}%"
        params.extend([like_value, like_value, like_value, like_value])

    if platform.strip():
        where_parts.append("summary.platform = ?")
        params.append(platform.strip())

    market_currency = normalize_market_currency(market)
    if market_currency:
        where_parts.append("COALESCE(summary.latest_currency, '') = ?")
        params.append(market_currency)

    normalized_source_scope = source_scope.strip().lower()
    if normalized_source_scope == "category":
        where_parts.append("summary.has_category = 1")
    elif normalized_source_scope == "keyword":
        where_parts.append("summary.has_keyword = 1")
    elif normalized_source_scope == "both":
        where_parts.append("summary.has_category = 1 AND summary.has_keyword = 1")

    if delivery_type.strip():
        where_parts.append("COALESCE(summary.latest_delivery_type, '') = ?")
        params.append(delivery_type.strip())

    if is_ad is not None:
        where_parts.append("COALESCE(summary.latest_is_ad, 0) = ?")
        params.append(int(is_ad))

    normalized_tab = (tab or "ranked").strip().lower()
    if normalized_tab == "ranked":
        where_parts.append("summary.latest_visible_bsr_rank IS NOT NULL")
    elif normalized_tab == "unranked":
        where_parts.append("summary.latest_visible_bsr_rank IS NULL")

    if bsr_min is not None:
        where_parts.append("COALESCE(summary.latest_visible_bsr_rank, 2147483647) >= ?")
        params.append(int(bsr_min))
    if bsr_max is not None:
        where_parts.append("summary.latest_visible_bsr_rank IS NOT NULL AND summary.latest_visible_bsr_rank <= ?")
        params.append(int(bsr_max))

    if review_min is not None:
        where_parts.append("COALESCE(summary.latest_review_count, 0) >= ?")
        params.append(int(review_min))
    if review_max is not None:
        where_parts.append("COALESCE(summary.latest_review_count, 0) <= ?")
        params.append(int(review_max))
    if rating_min is not None:
        where_parts.append("COALESCE(summary.latest_rating, 0) >= ?")
        params.append(float(rating_min))
    if rating_max is not None:
        where_parts.append("COALESCE(summary.latest_rating, 0) <= ?")
        params.append(float(rating_max))

    if price_min is not None:
        where_parts.append("COALESCE(summary.latest_price, 0) >= ?")
        params.append(float(price_min))
    if price_max is not None:
        where_parts.append("COALESCE(summary.latest_price, 0) <= ?")
        params.append(float(price_max))
    if sales_min is not None:
        where_parts.append("COALESCE(summary.monthly_sales_estimate, 0) >= ?")
        params.append(int(sales_min))
    if sales_max is not None:
        where_parts.append("COALESCE(summary.monthly_sales_estimate, 0) <= ?")
        params.append(int(sales_max))
    if inventory_min is not None:
        where_parts.append("COALESCE(summary.inventory_left_estimate, 0) >= ?")
        params.append(int(inventory_min))
    if inventory_max is not None:
        where_parts.append("COALESCE(summary.inventory_left_estimate, 0) <= ?")
        params.append(int(inventory_max))
    if review_growth_7d_min is not None:
        where_parts.append("COALESCE(summary.review_count_growth_7d, 0) >= ?")
        params.append(int(review_growth_7d_min))
    if review_growth_14d_min is not None:
        where_parts.append("COALESCE(summary.review_count_growth_14d, 0) >= ?")
        params.append(int(review_growth_14d_min))
    if rating_growth_7d_min is not None:
        where_parts.append("COALESCE(summary.rating_growth_7d, 0) >= ?")
        params.append(float(rating_growth_7d_min))
    if rating_growth_14d_min is not None:
        where_parts.append("COALESCE(summary.rating_growth_14d, 0) >= ?")
        params.append(float(rating_growth_14d_min))

    if has_sold_signal is not None:
        where_parts.append("COALESCE(summary.has_sold_signal, 0) = ?")
        params.append(1 if has_sold_signal else 0)
    if has_stock_signal is not None:
        where_parts.append("COALESCE(summary.has_stock_signal, 0) = ?")
        params.append(1 if has_stock_signal else 0)
    if has_lowest_price_signal is not None:
        where_parts.append("COALESCE(summary.has_lowest_price_signal, 0) = ?")
        params.append(1 if has_lowest_price_signal else 0)

    signal_tag_tokens = parse_signal_tag_tokens(signal_tags)
    if signal_tag_tokens:
        tag_clauses: list[str] = []
        signal_group_map = {
            "sales": _summary_signal_case_sql("sold_recently_text"),
            "stock": _summary_signal_case_sql("stock_signal_text"),
            "price": _summary_signal_case_sql("lowest_price_signal_text"),
            "ranking": _summary_signal_case_sql("ranking_signal_text"),
            "delivery": _summary_signal_case_sql("delivery_eta_signal_text"),
        }
        for group_name, normalized_text in signal_tag_tokens:
            expr = signal_group_map.get(group_name)
            if not expr:
                continue
            tag_clauses.append(f"{expr} = ?")
            params.append(normalized_text)
        if tag_clauses:
            where_parts.append(f"({' OR '.join(tag_clauses)})")

    if signal_text.strip():
        where_parts.append(
            """
            (
                COALESCE(NULLIF(summary.latest_sold_recently_text, ''), NULLIF(summary.sticky_sold_recently_text, ''), '') LIKE ?
                OR COALESCE(NULLIF(summary.latest_stock_signal_text, ''), NULLIF(summary.sticky_stock_signal_text, ''), '') LIKE ?
                OR COALESCE(NULLIF(summary.latest_lowest_price_signal_text, ''), NULLIF(summary.sticky_lowest_price_signal_text, ''), '') LIKE ?
                OR COALESCE(NULLIF(summary.latest_ranking_signal_text, ''), NULLIF(summary.sticky_ranking_signal_text, ''), '') LIKE ?
                OR COALESCE(NULLIF(summary.latest_delivery_eta_signal_text, ''), NULLIF(summary.sticky_delivery_eta_signal_text, ''), '') LIKE ?
            )
            """
        )
        like_value = f"%{signal_text.strip()}%"
        params.extend([like_value, like_value, like_value, like_value, like_value])

    normalized_category_paths = normalize_category_scope_paths(
        category_path=category_path,
        selected_category_paths=selected_category_paths,
    )
    if normalized_category_paths:
        path_clauses: list[str] = []
        for path in normalized_category_paths:
            path_clauses.append("(pcm.category_path = ? OR pcm.category_path LIKE ?)")
            params.extend([path, f"{path} > %"])
        where_parts.append(
            """
            EXISTS (
                SELECT 1
                FROM product_category_membership pcm
                WHERE pcm.platform = summary.platform
                  AND pcm.product_id = summary.product_id
                  AND (
                      """
            + " OR ".join(path_clauses)
            + """
                  )
            )
            """
        )

    if keyword.strip():
        where_parts.append(
            """
            EXISTS (
                SELECT 1
                FROM product_keyword_membership pkm
                WHERE pkm.platform = summary.platform
                  AND pkm.product_id = summary.product_id
                  AND pkm.keyword = ?
            )
            """
        )
        params.append(keyword.strip())

    return " AND ".join(where_parts), params


def fetch_products_rows(
    *,
    q: str = "",
    market: str = "",
    platform: str = "",
    source_scope: str = "",
    delivery_type: str = "",
    is_ad: int | None = None,
    tab: str = "all",
    bsr_min: int | None = None,
    bsr_max: int | None = None,
    review_min: int | None = None,
    review_max: int | None = None,
    rating_min: float | None = None,
    rating_max: float | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    sales_min: int | None = None,
    sales_max: int | None = None,
    inventory_min: int | None = None,
    inventory_max: int | None = None,
    review_growth_7d_min: int | None = None,
    review_growth_14d_min: int | None = None,
    rating_growth_7d_min: float | None = None,
    rating_growth_14d_min: float | None = None,
    has_sold_signal: bool | None = None,
    has_stock_signal: bool | None = None,
    has_lowest_price_signal: bool | None = None,
    signal_tags: str = "",
    signal_text: str = "",
    category_path: str = "",
    selected_category_paths: list[str] | None = None,
    keyword: str = "",
    sort: str = "sales_desc",
    limit: int | None = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> list[dict]:
    where_sql, params = build_product_filters(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        review_growth_7d_min=review_growth_7d_min,
        review_growth_14d_min=review_growth_14d_min,
        rating_growth_7d_min=rating_growth_7d_min,
        rating_growth_14d_min=rating_growth_14d_min,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        category_path=category_path,
        selected_category_paths=selected_category_paths,
        keyword=keyword,
    )

    order_sql = resolve_sort_sql(sort)
    limit_sql = ""
    final_params: list = [*params]
    if limit is not None:
        limit_sql = "LIMIT ? OFFSET ?"
        final_params.extend([limit, offset])

    return fetch_all(
        f"""
        SELECT
            {PRODUCT_SELECT_COLUMNS}
        FROM {WEB_PRODUCT_SUMMARY_TABLE} summary
        WHERE {where_sql}
        ORDER BY {order_sql}
        {limit_sql}
        """,
        tuple(final_params),
    )


def fetch_products_payload(
    *,
    q: str = "",
    market: str = "",
    platform: str = "",
    source_scope: str = "",
    delivery_type: str = "",
    is_ad: int | None = None,
    tab: str = "all",
    bsr_min: int | None = None,
    bsr_max: int | None = None,
    review_min: int | None = None,
    review_max: int | None = None,
    rating_min: float | None = None,
    rating_max: float | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    sales_min: int | None = None,
    sales_max: int | None = None,
    inventory_min: int | None = None,
    inventory_max: int | None = None,
    review_growth_7d_min: int | None = None,
    review_growth_14d_min: int | None = None,
    rating_growth_7d_min: float | None = None,
    rating_growth_14d_min: float | None = None,
    has_sold_signal: bool | None = None,
    has_stock_signal: bool | None = None,
    has_lowest_price_signal: bool | None = None,
    signal_tags: str = "",
    signal_text: str = "",
    category_path: str = "",
    selected_category_paths: list[str] | None = None,
    keyword: str = "",
    sort: str = "sales_desc",
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> dict:
    where_sql, params = build_product_filters(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        review_growth_7d_min=review_growth_7d_min,
        review_growth_14d_min=review_growth_14d_min,
        rating_growth_7d_min=rating_growth_7d_min,
        rating_growth_14d_min=rating_growth_14d_min,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        category_path=category_path,
        selected_category_paths=selected_category_paths,
        keyword=keyword,
    )

    total_row = fetch_one(
        f"""
        SELECT COUNT(*) AS total_count
        FROM {WEB_PRODUCT_SUMMARY_TABLE} summary
        WHERE {where_sql}
        """,
        tuple(params),
    )

    items = fetch_products_rows(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        review_growth_7d_min=review_growth_7d_min,
        review_growth_14d_min=review_growth_14d_min,
        rating_growth_7d_min=rating_growth_7d_min,
        rating_growth_14d_min=rating_growth_14d_min,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        category_path=category_path,
        selected_category_paths=selected_category_paths,
        keyword=keyword,
        sort=sort,
        limit=limit,
        offset=offset,
    )

    return {
        "items": items,
        "total_count": total_row.get("total_count", 0),
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "tab": tab,
        "selected_category_paths": normalize_category_scope_paths(
            category_path=category_path,
            selected_category_paths=selected_category_paths,
        ),
    }


def fetch_product_signal_options(
    *,
    q: str = "",
    market: str = "",
    platform: str = "",
    source_scope: str = "",
    delivery_type: str = "",
    is_ad: int | None = None,
    tab: str = "ranked",
    bsr_min: int | None = None,
    bsr_max: int | None = None,
    review_min: int | None = None,
    review_max: int | None = None,
    rating_min: float | None = None,
    rating_max: float | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    sales_min: int | None = None,
    sales_max: int | None = None,
    inventory_min: int | None = None,
    inventory_max: int | None = None,
    category_path: str = "",
    selected_category_paths: list[str] | None = None,
    keyword: str = "",
) -> list[dict[str, object]]:
    where_sql, params = build_product_filters(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        category_path=category_path,
        selected_category_paths=selected_category_paths,
        keyword=keyword,
    )
    rows = fetch_all(
        f"""
        WITH scoped_products AS (
            SELECT *
            FROM {WEB_PRODUCT_SUMMARY_TABLE} summary
            WHERE {where_sql}
        ),
        raw_signals AS (
            SELECT 'sales' AS signal_group, TRIM(COALESCE(NULLIF(latest_sold_recently_text, ''), NULLIF(sticky_sold_recently_text, ''), '')) AS signal_text
            FROM scoped_products
            UNION ALL
            SELECT 'stock' AS signal_group, TRIM(COALESCE(NULLIF(latest_stock_signal_text, ''), NULLIF(sticky_stock_signal_text, ''), '')) AS signal_text
            FROM scoped_products
            UNION ALL
            SELECT 'price' AS signal_group, TRIM(COALESCE(NULLIF(latest_lowest_price_signal_text, ''), NULLIF(sticky_lowest_price_signal_text, ''), '')) AS signal_text
            FROM scoped_products
            UNION ALL
            SELECT 'ranking' AS signal_group, TRIM(COALESCE(NULLIF(latest_ranking_signal_text, ''), NULLIF(sticky_ranking_signal_text, ''), '')) AS signal_text
            FROM scoped_products
            UNION ALL
            SELECT 'delivery' AS signal_group, TRIM(COALESCE(NULLIF(latest_delivery_eta_signal_text, ''), NULLIF(sticky_delivery_eta_signal_text, ''), '')) AS signal_text
            FROM scoped_products
        )
        SELECT
            signal_group,
            LOWER(signal_text) AS normalized_text,
            MIN(signal_text) AS sample_text,
            COUNT(*) AS match_count
        FROM raw_signals
        WHERE signal_text <> ''
        GROUP BY signal_group, LOWER(signal_text)
        ORDER BY signal_group ASC, match_count DESC, sample_text ASC
        """,
        tuple(params),
    )
    output: list[dict[str, object]] = []
    group_label_map = {
        "sales": "销量",
        "stock": "库存",
        "price": "最低价",
        "ranking": "排名",
        "delivery": "配送时效",
    }
    for row in rows:
        signal_text = str(row.get("sample_text") or "").strip()
        if not signal_text:
            continue
        group_name = str(row.get("signal_group") or "").strip().lower()
        output.append(
            {
                "key": encode_signal_option_key(group_name, signal_text),
                "label": signal_text,
                "group": group_name,
                "group_label": group_label_map.get(group_name, group_name),
                "example_text": signal_text,
                "match_count": int(row.get("match_count") or 0),
            }
        )
    return output


def fetch_canonical_category_counts() -> list[dict]:
    return fetch_all(
        """
        SELECT
            category_path,
            COUNT(DISTINCT platform || '|' || product_id) AS product_count
        FROM product_category_membership
        WHERE category_path LIKE ?
        GROUP BY category_path
        ORDER BY category_path ASC
        """,
        (f"{CANONICAL_CATEGORY_PREFIX}%",),
    )


def split_category_path(category_path: str) -> list[str]:
    return [part.strip() for part in str(category_path or "").split(" > ") if part.strip()]


def list_immediate_child_categories(parent_path: str = "") -> list[dict]:
    rows = fetch_canonical_category_counts()
    normalized_parent = parent_path.strip() or "Home"
    parent_parts = split_category_path(normalized_parent)
    target_depth = len(parent_parts) + 1
    children_map: dict[str, dict] = {}
    for row in rows:
        path = row["category_path"]
        parts = split_category_path(path)
        if len(parts) < target_depth:
            continue
        if normalized_parent != "Home" and not path.startswith(f"{normalized_parent} > "):
            continue
        child_path = " > ".join(parts[:target_depth])
        if child_path not in children_map:
            children_map[child_path] = {
                "path": child_path,
                "label": parts[target_depth - 1],
                "direct_product_count": 0,
            }
    items = list(children_map.values())
    items.sort(key=lambda item: item["label"])
    return items


def fetch_category_overviews_for_paths(category_paths: list[str]) -> dict[str, dict]:
    normalized_paths: list[str] = []
    seen: set[str] = set()
    for raw_path in category_paths:
        category_path = str(raw_path or "").strip()
        if not category_path or category_path in seen:
            continue
        seen.add(category_path)
        normalized_paths.append(category_path)

    if not normalized_paths:
        return {}

    cached = _read_payload_cache("category-overviews", tuple(normalized_paths))
    if cached is not None:
        return cached

    values_sql = ", ".join("(?, ?, ?)" for _ in normalized_paths)
    params: list[str] = []
    for category_path in normalized_paths:
        breadcrumb = split_category_path(category_path)
        params.extend([category_path, f"{category_path} > %", breadcrumb[-1] if breadcrumb else category_path])

    backend = _warehouse_backend()
    conn = connect_db()
    try:
        rows = fetch_all_from_conn(
            conn,
            f"""
            WITH target_categories(category_path, prefix_like, label) AS (
                VALUES {values_sql}
            ),
            matching_products AS (
                SELECT
                    tc.category_path AS target_path,
                    pcm.platform,
                    pcm.product_id
                FROM target_categories tc
                JOIN product_category_membership pcm
                  ON pcm.category_path = tc.category_path
                  OR pcm.category_path LIKE tc.prefix_like
                GROUP BY tc.category_path, pcm.platform, pcm.product_id
            )
            SELECT
                tc.category_path AS path,
                tc.label,
                COUNT(mp.product_id) AS product_count,
                SUM(CASE WHEN summary.latest_is_ad = 1 THEN 1 ELSE 0 END) AS ad_count,
                {_sql_round_avg("summary.latest_price", backend=backend)} AS avg_price,
                {_sql_round_avg("summary.latest_rating", backend=backend)} AS avg_rating,
                {_sql_round_avg("summary.latest_review_count", backend=backend)} AS avg_review_count,
                MIN(summary.latest_price) AS min_price,
                MAX(summary.latest_price) AS max_price,
                SUM(CASE WHEN summary.latest_visible_bsr_rank IS NOT NULL THEN 1 ELSE 0 END) AS bsr_coverage_count,
                {_sql_round_avg("summary.latest_visible_bsr_rank", backend=backend)} AS avg_visible_bsr_rank,
                SUM(CASE WHEN COALESCE(summary.latest_stock_signal_text, '') <> '' THEN 1 ELSE 0 END) AS low_stock_count,
                SUM(CASE WHEN COALESCE(summary.latest_lowest_price_signal_text, '') <> '' THEN 1 ELSE 0 END) AS lowest_price_count,
                SUM(CASE WHEN COALESCE(summary.latest_sold_recently_text, '') <> '' THEN 1 ELSE 0 END) AS sold_recently_count,
                SUM(CASE WHEN COALESCE(summary.latest_signal_count, 0) > 0 THEN 1 ELSE 0 END) AS signal_product_count,
                SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'express' THEN 1 ELSE 0 END) AS express_count,
                SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'supermall' THEN 1 ELSE 0 END) AS supermall_count,
                SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'global' THEN 1 ELSE 0 END) AS global_count,
                SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'marketplace' THEN 1 ELSE 0 END) AS marketplace_count,
                SUM(
                    CASE
                        WHEN summary.latest_visible_bsr_rank IS NOT NULL
                         AND COALESCE(summary.latest_sold_recently_text, '') <> ''
                        THEN 1 ELSE 0
                    END
                ) AS sales_ready_overlap_count,
                MAX(summary.latest_observed_at) AS latest_observed_at
            FROM target_categories tc
            LEFT JOIN matching_products mp
              ON mp.target_path = tc.category_path
            LEFT JOIN {WEB_PRODUCT_SUMMARY_TABLE} summary
              ON summary.platform = mp.platform
             AND summary.product_id = mp.product_id
            GROUP BY tc.category_path, tc.label
            """,
            tuple(params),
        )
    finally:
        conn.close()

    output: dict[str, dict] = {}
    for overview in rows:
        total_count = int(overview.get("product_count") or 0)
        category_path = overview["path"]
        breadcrumb = split_category_path(category_path)
        overview["breadcrumb"] = breadcrumb
        overview["label"] = overview.get("label") or (breadcrumb[-1] if breadcrumb else category_path)
        overview["ad_share_pct"] = pct(overview.get("ad_count"), total_count)
        overview["bsr_coverage_pct"] = pct(overview.get("bsr_coverage_count"), total_count)
        overview["express_share_pct"] = pct(overview.get("express_count"), total_count)
        overview["supermall_share_pct"] = pct(overview.get("supermall_count"), total_count)
        overview["global_share_pct"] = pct(overview.get("global_count"), total_count)
        overview["marketplace_share_pct"] = pct(overview.get("marketplace_count"), total_count)
        overview["signal_coverage_pct"] = pct(overview.get("signal_product_count"), total_count)
        overview["sold_signal_pct"] = pct(overview.get("sold_recently_count"), total_count)
        overview["stock_signal_pct"] = pct(overview.get("low_stock_count"), total_count)
        overview["lowest_price_signal_pct"] = pct(overview.get("lowest_price_count"), total_count)
        overview["sales_estimation_readiness"] = {
            "sold_signal_products": int(overview.get("sold_recently_count") or 0),
            "bsr_products": int(overview.get("bsr_coverage_count") or 0),
            "overlap_products": int(overview.get("sales_ready_overlap_count") or 0),
            "status": "Not active yet",
        }
        output[category_path] = overview
    return _write_payload_cache("category-overviews", tuple(normalized_paths), value=output)


def pct(value: int | float | None, total: int | float | None) -> float:
    denominator = float(total or 0)
    if denominator <= 0:
        return 0.0
    return round((float(value or 0) / denominator) * 100, 2)


def coerce_optional_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def fetch_category_overview(category_path: str) -> dict:
    cached = _read_payload_cache("category-overview", category_path)
    if cached is not None:
        return cached
    prefix_like = f"{category_path} > %"
    backend = _warehouse_backend()
    overview = fetch_one(
        f"""
        WITH matching_products AS (
            SELECT DISTINCT platform, product_id
            FROM product_category_membership
            WHERE category_path = ? OR category_path LIKE ?
        )
        SELECT
            COUNT(*) AS product_count,
            SUM(CASE WHEN summary.latest_is_ad = 1 THEN 1 ELSE 0 END) AS ad_count,
            {_sql_round_avg("summary.latest_price", backend=backend)} AS avg_price,
            {_sql_round_avg("summary.latest_rating", backend=backend)} AS avg_rating,
            {_sql_round_avg("summary.latest_review_count", backend=backend)} AS avg_review_count,
            MIN(summary.latest_price) AS min_price,
            MAX(summary.latest_price) AS max_price,
            SUM(CASE WHEN summary.latest_visible_bsr_rank IS NOT NULL THEN 1 ELSE 0 END) AS bsr_coverage_count,
            {_sql_round_avg("summary.latest_visible_bsr_rank", backend=backend)} AS avg_visible_bsr_rank,
            SUM(CASE WHEN COALESCE(summary.latest_stock_signal_text, '') <> '' THEN 1 ELSE 0 END) AS low_stock_count,
            SUM(CASE WHEN COALESCE(summary.latest_lowest_price_signal_text, '') <> '' THEN 1 ELSE 0 END) AS lowest_price_count,
            SUM(CASE WHEN COALESCE(summary.latest_sold_recently_text, '') <> '' THEN 1 ELSE 0 END) AS sold_recently_count,
            SUM(CASE WHEN COALESCE(summary.latest_signal_count, 0) > 0 THEN 1 ELSE 0 END) AS signal_product_count,
            SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'express' THEN 1 ELSE 0 END) AS express_count,
            SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'supermall' THEN 1 ELSE 0 END) AS supermall_count,
            SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'global' THEN 1 ELSE 0 END) AS global_count,
            SUM(CASE WHEN COALESCE(summary.latest_delivery_type, '') = 'marketplace' THEN 1 ELSE 0 END) AS marketplace_count,
            SUM(
                CASE
                    WHEN summary.latest_visible_bsr_rank IS NOT NULL
                     AND COALESCE(summary.latest_sold_recently_text, '') <> ''
                    THEN 1 ELSE 0
                END
            ) AS sales_ready_overlap_count,
            MAX(summary.latest_observed_at) AS latest_observed_at
        FROM matching_products mp
        JOIN {WEB_PRODUCT_SUMMARY_TABLE} summary
          ON summary.platform = mp.platform
         AND summary.product_id = mp.product_id
        """,
        (category_path, prefix_like),
    )

    total_count = int(overview.get("product_count") or 0)
    if not total_count:
        raise HTTPException(status_code=404, detail=f"category not found or empty: {category_path}")

    breadcrumb = split_category_path(category_path)
    overview["path"] = category_path
    overview["breadcrumb"] = breadcrumb
    overview["label"] = breadcrumb[-1] if breadcrumb else category_path
    overview["ad_share_pct"] = pct(overview.get("ad_count"), total_count)
    overview["bsr_coverage_pct"] = pct(overview.get("bsr_coverage_count"), total_count)
    overview["express_share_pct"] = pct(overview.get("express_count"), total_count)
    overview["supermall_share_pct"] = pct(overview.get("supermall_count"), total_count)
    overview["global_share_pct"] = pct(overview.get("global_count"), total_count)
    overview["marketplace_share_pct"] = pct(overview.get("marketplace_count"), total_count)
    overview["signal_coverage_pct"] = pct(overview.get("signal_product_count"), total_count)
    overview["sold_signal_pct"] = pct(overview.get("sold_recently_count"), total_count)
    overview["stock_signal_pct"] = pct(overview.get("low_stock_count"), total_count)
    overview["lowest_price_signal_pct"] = pct(overview.get("lowest_price_count"), total_count)
    overview["sales_estimation_readiness"] = {
        "sold_signal_products": int(overview.get("sold_recently_count") or 0),
        "bsr_products": int(overview.get("bsr_coverage_count") or 0),
        "overlap_products": int(overview.get("sales_ready_overlap_count") or 0),
        "status": "Not active yet",
    }
    return _write_payload_cache("category-overview", category_path, value=overview)


def category_passes_filters(
    item: dict,
    *,
    avg_price_min: float | None = None,
    avg_price_max: float | None = None,
    express_share_min: float | None = None,
    express_share_max: float | None = None,
    ad_share_min: float | None = None,
    ad_share_max: float | None = None,
    product_count_min: int | None = None,
    product_count_max: int | None = None,
    bsr_coverage_min: float | None = None,
    bsr_coverage_max: float | None = None,
    signal_coverage_min: float | None = None,
    signal_coverage_max: float | None = None,
) -> bool:
    rules = [
        ("avg_price", avg_price_min, avg_price_max),
        ("express_share_pct", express_share_min, express_share_max),
        ("ad_share_pct", ad_share_min, ad_share_max),
        ("product_count", product_count_min, product_count_max),
        ("bsr_coverage_pct", bsr_coverage_min, bsr_coverage_max),
        ("signal_coverage_pct", signal_coverage_min, signal_coverage_max),
    ]
    for field, min_value, max_value in rules:
        value = item.get(field)
        normalized_min = coerce_optional_number(min_value)
        normalized_max = coerce_optional_number(max_value)
        if normalized_min is not None and (value is None or float(value) < float(normalized_min)):
            return False
        if normalized_max is not None and (value is None or float(value) > float(normalized_max)):
            return False
    return True


def sort_category_items(items: list[dict], sort: str = "product_count_desc") -> list[dict]:
    sort_key = sort.strip().lower() if isinstance(sort, str) and sort.strip() else "product_count_desc"
    key_map = {
        "product_count_desc": lambda item: (-int(item.get("product_count") or 0), item.get("label") or ""),
        "avg_price_desc": lambda item: (-(float(item.get("avg_price") or 0)), item.get("label") or ""),
        "avg_price_asc": lambda item: (float(item.get("avg_price") or 0), item.get("label") or ""),
        "express_share_desc": lambda item: (-(float(item.get("express_share_pct") or 0)), item.get("label") or ""),
        "ad_share_desc": lambda item: (-(float(item.get("ad_share_pct") or 0)), item.get("label") or ""),
        "bsr_coverage_desc": lambda item: (-(float(item.get("bsr_coverage_pct") or 0)), item.get("label") or ""),
        "signal_coverage_desc": lambda item: (-(float(item.get("signal_coverage_pct") or 0)), item.get("label") or ""),
    }
    return sorted(items, key=key_map.get(sort_key, key_map["product_count_desc"]))


def compute_bucket_counts(values: list[float], buckets: list[tuple[str, float | None, float | None]]) -> list[dict]:
    output: list[dict] = []
    for label, min_value, max_value in buckets:
        count = 0
        for raw_value in values:
            if raw_value is None:
                continue
            value = float(raw_value)
            if min_value is not None and value < min_value:
                continue
            if max_value is not None and value >= max_value:
                continue
            count += 1
        output.append({"label": label, "count": count})
    return output


def compute_category_benchmarks_payload(category_path: str) -> dict:
    cached = _read_payload_cache("category-benchmarks", category_path)
    if cached is not None:
        return cached
    prefix_like = f"{category_path} > %"
    overview = fetch_category_overview(category_path)
    rows = fetch_all(
        f"""
        WITH matching_products AS (
            SELECT DISTINCT platform, product_id
            FROM product_category_membership
            WHERE category_path = ? OR category_path LIKE ?
        )
        SELECT
            {PRODUCT_SELECT_COLUMNS}
        FROM matching_products mp
        JOIN {WEB_PRODUCT_SUMMARY_TABLE} summary
          ON summary.platform = mp.platform
         AND summary.product_id = mp.product_id
        ORDER BY summary.latest_observed_at DESC
        """,
        (category_path, prefix_like),
    )

    price_values = [row.get("latest_price") for row in rows if row.get("latest_price") is not None]
    review_values = [row.get("latest_review_count") for row in rows if row.get("latest_review_count") is not None]
    rating_values = [row.get("latest_rating") for row in rows if row.get("latest_rating") is not None]

    delivery_breakdown = fetch_all(
        f"""
        WITH matching_products AS (
            SELECT DISTINCT platform, product_id
            FROM product_category_membership
            WHERE category_path = ? OR category_path LIKE ?
        )
        SELECT
            COALESCE(summary.latest_delivery_type, '') AS delivery_type,
            COUNT(*) AS product_count
        FROM matching_products mp
        JOIN {WEB_PRODUCT_SUMMARY_TABLE} summary
          ON summary.platform = mp.platform
         AND summary.product_id = mp.product_id
        GROUP BY COALESCE(summary.latest_delivery_type, '')
        ORDER BY product_count DESC
        """,
        (category_path, prefix_like),
    )

    child_categories = fetch_all(
        """
        SELECT
            category_path,
            COUNT(DISTINCT platform || '|' || product_id) AS product_count
        FROM product_category_membership
        WHERE category_path LIKE ?
        GROUP BY category_path
        ORDER BY product_count DESC, category_path ASC
        LIMIT 8
        """,
        (f"{category_path} > %",),
    )

    payload = {
        "category_path": category_path,
        "summary": {
            **overview,
            "avg_price": round(sum(float(value) for value in price_values) / len(price_values), 2) if price_values else None,
            "avg_review_count": round(sum(float(value) for value in review_values) / len(review_values), 2) if review_values else None,
            "avg_rating": round(sum(float(value) for value in rating_values) / len(rating_values), 2) if rating_values else None,
        },
        "price_bands": compute_bucket_counts(
            price_values,
            [
                ("< 50", None, 50),
                ("50 - 100", 50, 100),
                ("100 - 200", 100, 200),
                ("200 - 500", 200, 500),
                ("500+", 500, None),
            ],
        ),
        "review_bands": compute_bucket_counts(
            review_values,
            [
                ("0", 0, 1),
                ("1 - 20", 1, 21),
                ("21 - 100", 21, 101),
                ("101 - 500", 101, 501),
                ("500+", 501, None),
            ],
        ),
        "rating_bands": compute_bucket_counts(
            rating_values,
            [
                ("< 3.0", None, 3.0),
                ("3.0 - 4.0", 3.0, 4.0),
                ("4.0 - 4.5", 4.0, 4.5),
                ("4.5+", 4.5, None),
            ],
        ),
        "signal_summary": {
            "stock": int(overview.get("low_stock_count") or 0),
            "lowest_price": int(overview.get("lowest_price_count") or 0),
            "sold_recently": int(overview.get("sold_recently_count") or 0),
            "signal_total": sum(int(row.get("latest_signal_count") or 0) for row in rows),
        },
        "delivery_breakdown": delivery_breakdown,
        "top_child_categories": child_categories,
        "sales_estimation_readiness": overview["sales_estimation_readiness"],
    }
    return _write_payload_cache("category-benchmarks", category_path, value=payload)


def compute_keyword_benchmarks_payload(keyword: str) -> dict:
    # Benchmarks must reflect the full matched keyword pool, not only ranked rows.
    rows = fetch_products_rows(keyword=keyword, tab="all", limit=None)
    if not rows:
        raise HTTPException(status_code=404, detail=f"keyword benchmark not found: {keyword}")

    delivery_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    price_values = [row.get("latest_price") for row in rows if row.get("latest_price") is not None]
    review_values = [row.get("latest_review_count") for row in rows if row.get("latest_review_count") is not None]
    rating_values = [row.get("latest_rating") for row in rows if row.get("latest_rating") is not None]

    ad_count = 0
    both_count = 0
    for row in rows:
        delivery_key = row.get("latest_delivery_type") or ""
        delivery_counts[delivery_key] = delivery_counts.get(delivery_key, 0) + 1

        category_key = row.get("latest_observed_category_path") or row.get("latest_category_path") or "Uncategorized"
        category_counts[category_key] = category_counts.get(category_key, 0) + 1

        if row.get("latest_is_ad"):
            ad_count += 1
        if row.get("has_category") and row.get("has_keyword"):
            both_count += 1

    return {
        "keyword": keyword,
        "summary": {
            "matched_product_count": len(rows),
            "ad_share_pct": round((ad_count / len(rows)) * 100, 2) if rows else 0,
            "avg_price": round(sum(float(value) for value in price_values) / len(price_values), 2) if price_values else None,
            "avg_review_count": round(sum(float(value) for value in review_values) / len(review_values), 2) if review_values else None,
            "avg_rating": round(sum(float(value) for value in rating_values) / len(rating_values), 2) if rating_values else None,
            "both_source_pct": round((both_count / len(rows)) * 100, 2) if rows else 0,
        },
        "delivery_breakdown": [
            {"delivery_type": key, "product_count": value}
            for key, value in sorted(delivery_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "top_categories": [
            {"category_path": key, "product_count": value}
            for key, value in sorted(category_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
        ],
        "price_bands": compute_bucket_counts(
            price_values,
            [
                ("< 50", None, 50),
                ("50 - 100", 50, 100),
                ("100 - 200", 100, 200),
                ("200 - 500", 200, 500),
                ("500+", 500, None),
            ],
        ),
        "review_bands": compute_bucket_counts(
            review_values,
            [
                ("0", 0, 1),
                ("1 - 20", 1, 21),
                ("21 - 100", 21, 101),
                ("101 - 500", 101, 501),
                ("500+", 501, None),
            ],
        ),
    }


def build_category_tree(rows: list[dict]) -> list[dict]:
    root = {
        "id": "Home",
        "label": "Home",
        "path": "Home",
        "level": 0,
        "direct_product_count": 0,
        "descendant_product_count": 0,
        "children_map": {},
        "children": [],
    }

    for row in rows:
        category_path = row["category_path"]
        product_count = int(row["product_count"] or 0)
        parts = category_path.split(" > ")
        current = root
        current["descendant_product_count"] += product_count

        for index in range(1, len(parts)):
            segment = parts[index]
            node_path = " > ".join(parts[: index + 1])
            children_map = current.setdefault("children_map", {})
            if segment not in children_map:
                children_map[segment] = {
                    "id": node_path,
                    "label": segment,
                    "path": node_path,
                    "level": index,
                    "direct_product_count": 0,
                    "descendant_product_count": 0,
                    "children_map": {},
                    "children": [],
                }
            current = children_map[segment]
            current["descendant_product_count"] += product_count

        current["direct_product_count"] += product_count

    def finalize(node: dict) -> dict:
        children = [finalize(child) for child in node.get("children_map", {}).values()]
        children.sort(key=lambda item: (-item["descendant_product_count"], item["label"]))
        return {
            "id": node["id"],
            "label": node["label"],
            "path": node["path"],
            "level": node["level"],
            "direct_product_count": node["direct_product_count"],
            "descendant_product_count": node["descendant_product_count"],
            "child_count": len(children),
            "children": children,
        }

    final_root = finalize(root)
    return final_root["children"]


def build_daily_import_series(days: int = 30) -> dict[str, object]:
    cached = _read_static_payload_cache("daily-import-series", days)
    if cached is not None:
        return cached
    end_date = datetime.now().date()
    labels = [
        (end_date - timedelta(days=offset)).isoformat()
        for offset in range(days - 1, -1, -1)
    ]
    counts = {
        "category": {day: 0 for day in labels},
        "keyword": {day: 0 for day in labels},
    }
    cutoff = labels[0]
    # Use warehouse first_seen + source coverage as the primary signal for
    # "new products by day". This remains correct after the runtime switched
    # from separate category/keyword stage imports to shared_stage imports.
    rows = fetch_all(
        """
        SELECT
            SUBSTR(COALESCE(pi.first_seen, ''), 1, 10) AS observed_day,
            SUM(CASE WHEN COALESCE(coverage.has_category, 0) = 1 THEN 1 ELSE 0 END) AS category_count,
            SUM(CASE WHEN COALESCE(coverage.has_keyword, 0) = 1 THEN 1 ELSE 0 END) AS keyword_count
        FROM product_identity pi
        LEFT JOIN vw_product_source_coverage coverage
          ON coverage.platform = pi.platform AND coverage.product_id = pi.product_id
        WHERE pi.first_seen >= ?
        GROUP BY SUBSTR(COALESCE(pi.first_seen, ''), 1, 10)
        ORDER BY observed_day ASC
        """,
        (cutoff,),
    )
    for row in rows:
        day = str(row.get("observed_day") or "")[:10]
        if day not in counts["category"]:
            continue
        counts["category"][day] += int(row.get("category_count") or 0)
        counts["keyword"][day] += int(row.get("keyword_count") or 0)
    return _write_static_payload_cache(
        "daily-import-series",
        days,
        value={
            "days": labels,
            "category_products": [counts["category"][day] for day in labels],
            "keyword_products": [counts["keyword"][day] for day in labels],
        },
    )


def build_dashboard_source_coverage() -> list[dict[str, object]]:
    cached = _read_static_payload_cache("dashboard-source-coverage")
    if cached is not None:
        return cached
    return _write_static_payload_cache(
        "dashboard-source-coverage",
        value=fetch_all(
        f"""
        SELECT
            CASE
                WHEN has_category = 1 AND has_keyword = 1 THEN 'both'
                WHEN has_category = 1 THEN 'category_only'
                WHEN has_keyword = 1 THEN 'keyword_only'
                ELSE 'unknown'
            END AS source_type,
            COUNT(*) AS product_count
        FROM {WEB_SOURCE_COVERAGE_TABLE}
        GROUP BY
            CASE
                WHEN has_category = 1 AND has_keyword = 1 THEN 'both'
                WHEN has_category = 1 THEN 'category_only'
                WHEN has_keyword = 1 THEN 'keyword_only'
                ELSE 'unknown'
            END
        ORDER BY product_count DESC, source_type ASC
        """
        ),
    )


def build_dashboard_recent_imports(limit: int = 8) -> list[dict[str, object]]:
    cached = _read_static_payload_cache("dashboard-recent-imports", limit)
    if cached is not None:
        return cached
    return _write_static_payload_cache(
        "dashboard-recent-imports",
        limit,
        value=fetch_all(
        """
        SELECT
            source_label,
            source_scope,
            imported_at,
            source_product_count,
            source_observation_count,
            source_keyword_count
        FROM source_databases
        ORDER BY imported_at DESC, source_label DESC
        LIMIT ?
        """,
        (limit,),
        ),
    )


def build_dashboard_recent_keyword_runs(limit: int = 8) -> list[dict[str, object]]:
    cached = _read_static_payload_cache("dashboard-recent-keyword-runs", limit)
    if cached is not None:
        return cached
    return _write_static_payload_cache(
        "dashboard-recent-keyword-runs",
        limit,
        value=fetch_all(
        """
        SELECT
            run_type,
            trigger_mode,
            seed_keyword,
            snapshot_id,
            status,
            keyword_count,
            started_at,
            finished_at
        FROM keyword_runs_log
        ORDER BY started_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
        ),
    )


def build_global_scope_payload() -> dict[str, object]:
    cached = _read_payload_cache("scope-payload", "global")
    if cached is not None:
        return cached
    backend = _warehouse_backend()
    summary = fetch_one(
        f"""
        SELECT
            COUNT(*) AS product_count,
            {_sql_round_avg("latest_price", backend=backend)} AS avg_price,
            MAX(latest_observed_at) AS latest_observed_at,
            SUM(CASE WHEN latest_delivery_type = 'express' THEN 1 ELSE 0 END) AS express_count,
            SUM(CASE WHEN latest_is_ad = 1 THEN 1 ELSE 0 END) AS ad_count,
            SUM(CASE WHEN latest_visible_bsr_rank IS NOT NULL THEN 1 ELSE 0 END) AS bsr_covered_count,
            SUM(CASE WHEN COALESCE(latest_signal_count, 0) > 0 THEN 1 ELSE 0 END) AS signal_covered_count,
            SUM(CASE WHEN has_sold_signal = 1 THEN 1 ELSE 0 END) AS sold_signal_count,
            SUM(CASE WHEN has_stock_signal = 1 THEN 1 ELSE 0 END) AS stock_signal_count,
            SUM(CASE WHEN has_lowest_price_signal = 1 THEN 1 ELSE 0 END) AS lowest_price_count
        FROM {WEB_PRODUCT_SUMMARY_TABLE}
        """
    )
    total_products = int(summary.get("product_count") or 0)
    delivery_breakdown = fetch_all(
        f"""
        SELECT
            COALESCE(latest_delivery_type, '') AS delivery_type,
            COUNT(*) AS product_count
        FROM {WEB_PRODUCT_SUMMARY_TABLE}
        GROUP BY COALESCE(latest_delivery_type, '')
        ORDER BY product_count DESC
        """
    )
    price_bands_row = fetch_one(
        f"""
        SELECT
            SUM(CASE WHEN latest_price < 50 THEN 1 ELSE 0 END) AS band_lt_50,
            SUM(CASE WHEN latest_price >= 50 AND latest_price < 100 THEN 1 ELSE 0 END) AS band_50_100,
            SUM(CASE WHEN latest_price >= 100 AND latest_price < 200 THEN 1 ELSE 0 END) AS band_100_200,
            SUM(CASE WHEN latest_price >= 200 AND latest_price < 500 THEN 1 ELSE 0 END) AS band_200_500,
            SUM(CASE WHEN latest_price >= 500 THEN 1 ELSE 0 END) AS band_500_plus
        FROM {WEB_PRODUCT_SUMMARY_TABLE}
        """
    )
    child_items = sort_category_items(
        [
            overview
            for overview in fetch_category_overviews_for_paths(
                [item["path"] for item in list_immediate_child_categories("")]
            ).values()
            if overview
        ],
        "product_count_desc",
    )
    payload = {
        "scope_path": "",
        "scope_label": "全平台 / 大盘",
        "summary": {
            "product_count": total_products,
            "avg_price": summary.get("avg_price"),
            "express_share_pct": round((int(summary.get("express_count") or 0) / total_products) * 100, 2) if total_products else 0,
            "ad_share_pct": round((int(summary.get("ad_count") or 0) / total_products) * 100, 2) if total_products else 0,
            "bsr_coverage_pct": round((int(summary.get("bsr_covered_count") or 0) / total_products) * 100, 2) if total_products else 0,
            "signal_coverage_pct": round((int(summary.get("signal_covered_count") or 0) / total_products) * 100, 2) if total_products else 0,
            "sold_signal_pct": round((int(summary.get("sold_signal_count") or 0) / total_products) * 100, 2) if total_products else 0,
            "stock_signal_pct": round((int(summary.get("stock_signal_count") or 0) / total_products) * 100, 2) if total_products else 0,
            "lowest_price_signal_pct": round((int(summary.get("lowest_price_count") or 0) / total_products) * 100, 2) if total_products else 0,
            "latest_observed_at": summary.get("latest_observed_at"),
        },
        "delivery_breakdown": delivery_breakdown,
        "price_bands": [
            {"label": "< 50", "count": int(price_bands_row.get("band_lt_50") or 0)},
            {"label": "50 - 100", "count": int(price_bands_row.get("band_50_100") or 0)},
            {"label": "100 - 200", "count": int(price_bands_row.get("band_100_200") or 0)},
            {"label": "200 - 500", "count": int(price_bands_row.get("band_200_500") or 0)},
            {"label": "500+", "count": int(price_bands_row.get("band_500_plus") or 0)},
        ],
        "ad_structure": [
            {"label": "Organic", "count": max(total_products - int(summary.get("ad_count") or 0), 0)},
            {"label": "广告 / Ad", "count": int(summary.get("ad_count") or 0)},
        ],
        "signal_structure": [
            {"label": "销量信号", "count": int(summary.get("sold_signal_count") or 0)},
            {"label": "库存信号", "count": int(summary.get("stock_signal_count") or 0)},
            {"label": "最低价信号", "count": int(summary.get("lowest_price_count") or 0)},
        ],
        "child_categories": child_items,
    }
    return _write_payload_cache("scope-payload", "global", value=payload)


def build_category_scope_payload(category_path: str) -> dict[str, object]:
    cached = _read_payload_cache("scope-payload", "category", category_path)
    if cached is not None:
        return cached
    benchmark_payload = compute_category_benchmarks_payload(category_path)
    child_items = sort_category_items(
        [
            overview
            for overview in fetch_category_overviews_for_paths(
                [item["path"] for item in list_immediate_child_categories(category_path)]
            ).values()
            if overview
        ],
        "product_count_desc",
    )
    summary = dict(benchmark_payload.get("summary") or {})
    product_count = int(summary.get("product_count") or 0)
    payload = {
        "scope_path": category_path,
        "scope_label": category_path,
        "summary": summary,
        "delivery_breakdown": benchmark_payload.get("delivery_breakdown") or [],
        "price_bands": benchmark_payload.get("price_bands") or [],
        "ad_structure": [
            {"label": "Organic", "count": max(product_count - int(summary.get("ad_count") or 0), 0)},
            {"label": "广告 / Ad", "count": int(summary.get("ad_count") or 0)},
        ],
        "signal_structure": [
            {"label": "销量信号", "count": int(summary.get("sold_recently_count") or 0)},
            {"label": "库存信号", "count": int(summary.get("low_stock_count") or 0)},
            {"label": "最低价信号", "count": int(summary.get("lowest_price_count") or 0)},
        ],
        "child_categories": child_items,
    }
    return _write_payload_cache("scope-payload", "category", category_path, value=payload)


def build_multi_category_scope_payload(category_paths: list[str]) -> dict[str, object]:
    normalized_paths = normalize_category_scope_paths(selected_category_paths=category_paths)
    if not normalized_paths:
        return build_global_scope_payload()
    if len(normalized_paths) == 1:
        return build_category_scope_payload(normalized_paths[0])

    cached = _read_payload_cache("scope-payload", "multi", tuple(normalized_paths))
    if cached is not None:
        return cached

    overview_map = fetch_category_overviews_for_paths(normalized_paths)
    items = [overview_map[path] for path in normalized_paths if overview_map.get(path)]
    if not items:
        return build_global_scope_payload()

    total_products = sum(int(item.get("product_count") or 0) for item in items)

    def weighted_avg(field: str) -> float | None:
        numerator = 0.0
        denominator = 0
        for item in items:
            count = int(item.get("product_count") or 0)
            value = item.get(field)
            if count <= 0 or value is None:
                continue
            numerator += float(value) * count
            denominator += count
        if denominator <= 0:
            return None
        return round(numerator / denominator, 2)

    summary = {
        "scope_count": len(items),
        "product_count": total_products,
        "ad_count": sum(int(item.get("ad_count") or 0) for item in items),
        "sold_recently_count": sum(int(item.get("sold_recently_count") or 0) for item in items),
        "low_stock_count": sum(int(item.get("low_stock_count") or 0) for item in items),
        "lowest_price_count": sum(int(item.get("lowest_price_count") or 0) for item in items),
        "signal_product_count": sum(int(item.get("signal_product_count") or 0) for item in items),
        "bsr_coverage_count": sum(int(item.get("bsr_coverage_count") or 0) for item in items),
        "avg_price": weighted_avg("avg_price"),
        "avg_rating": weighted_avg("avg_rating"),
        "avg_review_count": weighted_avg("avg_review_count"),
        "avg_visible_bsr_rank": weighted_avg("avg_visible_bsr_rank"),
        "latest_observed_at": max((item.get("latest_observed_at") or "" for item in items), default=""),
    }
    summary["ad_share_pct"] = pct(summary.get("ad_count"), total_products)
    summary["bsr_coverage_pct"] = pct(summary.get("bsr_coverage_count"), total_products)
    summary["signal_coverage_pct"] = pct(summary.get("signal_product_count"), total_products)
    summary["sold_signal_pct"] = pct(summary.get("sold_recently_count"), total_products)
    summary["stock_signal_pct"] = pct(summary.get("low_stock_count"), total_products)
    summary["lowest_price_signal_pct"] = pct(summary.get("lowest_price_count"), total_products)
    summary["sales_estimation_readiness"] = {
        "sold_signal_products": int(summary.get("sold_recently_count") or 0),
        "bsr_products": int(summary.get("bsr_coverage_count") or 0),
        "overlap_products": min(
            int(summary.get("sold_recently_count") or 0),
            int(summary.get("bsr_coverage_count") or 0),
        ),
        "status": "Not active yet",
    }
    delivery_breakdown = [
        {"delivery_type": "express", "product_count": sum(int(item.get("express_count") or 0) for item in items)},
        {"delivery_type": "supermall", "product_count": sum(int(item.get("supermall_count") or 0) for item in items)},
        {"delivery_type": "global", "product_count": sum(int(item.get("global_count") or 0) for item in items)},
        {"delivery_type": "marketplace", "product_count": sum(int(item.get("marketplace_count") or 0) for item in items)},
    ]
    signal_structure = [
        {"label": "销量信号", "count": int(summary.get("sold_recently_count") or 0)},
        {"label": "库存信号", "count": int(summary.get("low_stock_count") or 0)},
        {"label": "最低价信号", "count": int(summary.get("lowest_price_count") or 0)},
    ]
    payload = {
        "scope_path": "|".join(normalized_paths),
        "scope_label": f"{len(items)} 个类目",
        "selected_categories": items,
        "summary": summary,
        "delivery_breakdown": [item for item in delivery_breakdown if int(item.get("product_count") or 0) > 0],
        "price_bands": [],
        "ad_structure": [
            {"label": "Organic", "count": max(total_products - int(summary.get("ad_count") or 0), 0)},
            {"label": "广告 / Ad", "count": int(summary.get("ad_count") or 0)},
        ],
        "signal_structure": signal_structure,
        "child_categories": items,
    }
    return _write_payload_cache("scope-payload", "multi", tuple(normalized_paths), value=payload)


def build_compact_category_context_payload(category_path: str) -> dict[str, object]:
    cached = _read_payload_cache("context-scope-payload", category_path)
    if cached is not None:
        return cached
    overview = fetch_category_overview(category_path)
    product_count = int(overview.get("product_count") or 0)
    payload = {
        "scope_path": category_path,
        "scope_label": category_path,
        "summary": overview,
        "delivery_breakdown": [
            {"delivery_type": "express", "product_count": int(overview.get("express_count") or 0)},
            {"delivery_type": "supermall", "product_count": int(overview.get("supermall_count") or 0)},
            {"delivery_type": "global", "product_count": int(overview.get("global_count") or 0)},
            {"delivery_type": "marketplace", "product_count": int(overview.get("marketplace_count") or 0)},
        ],
        "ad_structure": [
            {"label": "Organic", "count": max(product_count - int(overview.get("ad_count") or 0), 0)},
            {"label": "广告 / Ad", "count": int(overview.get("ad_count") or 0)},
        ],
        "signal_structure": [
            {"label": "销量信号", "count": int(overview.get("sold_recently_count") or 0)},
            {"label": "库存信号", "count": int(overview.get("low_stock_count") or 0)},
            {"label": "最低价信号", "count": int(overview.get("lowest_price_count") or 0)},
        ],
        # Drawer context is a quick judgment surface. Do not run child-category
        # fanout aggregation on first paint.
        "child_categories": [],
    }
    return _write_payload_cache("context-scope-payload", category_path, value=payload)


def build_category_context_levels(category_paths: list[str]) -> tuple[list[dict[str, object]], dict[str, object]]:
    if not category_paths:
        return [], {}
    normalized_paths = normalize_category_scope_paths(selected_category_paths=category_paths)
    cached = _read_payload_cache("context-levels", tuple(normalized_paths))
    if cached is not None:
        return tuple(cached)
    primary_path = sorted(normalized_paths, key=lambda path: (len(split_category_path(path)), len(path)), reverse=True)[0]
    breadcrumb = split_category_path(primary_path)
    contexts: list[dict[str, object]] = []
    effective_context: dict[str, object] = {}
    for index in range(1, len(breadcrumb) + 1):
        scoped_path = " > ".join(breadcrumb[:index])
        try:
            payload = build_compact_category_context_payload(scoped_path)
        except HTTPException:
            continue
        level_payload = {
            "level": f"L{index}",
            "path": scoped_path,
            "label": breadcrumb[index - 1],
            "summary": payload,
        }
        contexts.append(level_payload)
        effective_context = level_payload
    return tuple(_write_payload_cache("context-levels", tuple(normalized_paths), value=[contexts, effective_context]))


def _parse_event_datetime(raw_value: object) -> datetime | None:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return None
    try:
        return datetime.fromisoformat(raw_text.replace("Z", "+00:00"))
    except ValueError:
        return None


def build_keyword_ranking_timeline(
    platform: str,
    product_id: str,
    *,
    days: int = 30,
    max_keywords: int = 5,
    limit: int = 2000,
    conn=None,
) -> list[dict[str, object]]:
    cutoff = (datetime.now() - timedelta(days=max(days, 1))).isoformat(timespec="seconds")
    sql = """
        SELECT
            keyword,
            rank_position,
            rank_type,
            observed_at,
            source_platform
        FROM product_keyword_rank_events
        WHERE platform = ? AND product_id = ? AND observed_at >= ?
        ORDER BY observed_at DESC, rank_position ASC, keyword ASC
        LIMIT ?
        """
    params = (platform, product_id, cutoff, limit)
    rows = fetch_all_from_conn(conn, sql, params) if conn is not None else fetch_all(sql, params)
    if not rows:
        return []

    keyword_order: list[str] = []
    seen_keywords: set[str] = set()
    for row in rows:
        keyword = str(row.get("keyword") or "").strip()
        if keyword and keyword not in seen_keywords:
            seen_keywords.add(keyword)
            keyword_order.append(keyword)
        if len(keyword_order) >= max_keywords:
            break
    allowed_keywords = set(keyword_order[:max_keywords])
    if not allowed_keywords:
        return []

    collapsed: dict[tuple[str, str, str], dict[str, object]] = {}
    for row in rows:
        keyword = str(row.get("keyword") or "").strip()
        if keyword not in allowed_keywords:
            continue
        observed_at = str(row.get("observed_at") or "").strip()
        observed_dt = _parse_event_datetime(observed_at)
        observed_day = observed_dt.date().isoformat() if observed_dt else observed_at[:10]
        rank_type = str(row.get("rank_type") or "organic").strip().lower() or "organic"
        key = (keyword, rank_type, observed_day)
        if key in collapsed:
            continue
        collapsed[key] = {
            "keyword": keyword,
            "rank_position": row.get("rank_position"),
            "rank_type": rank_type,
            "observed_at": observed_at,
            "observed_day": observed_day,
            "source_platform": row.get("source_platform") or platform,
        }

    def sort_key(item: dict[str, object]) -> tuple[str, int, str]:
        rank_type = str(item.get("rank_type") or "organic")
        type_rank = 0 if rank_type == "organic" else 1
        return (
            str(item.get("observed_day") or ""),
            type_rank,
            str(item.get("keyword") or ""),
        )

    return sorted(collapsed.values(), key=sort_key)


def build_signal_timeline(
    platform: str,
    product_id: str,
    *,
    days: int = 30,
    limit: int = 1200,
    conn=None,
) -> list[dict[str, object]]:
    cutoff = (datetime.now() - timedelta(days=max(days, 1))).isoformat(timespec="seconds")
    sql = """
        SELECT
            scraped_at,
            sold_recently_text,
            stock_signal_text,
            lowest_price_signal_text,
            ranking_signal_text,
            delivery_eta_signal_text
        FROM observation_events
        WHERE platform = ? AND product_id = ? AND scraped_at >= ?
        ORDER BY scraped_at DESC, id DESC
        LIMIT ?
        """
    params = (platform, product_id, cutoff, limit)
    rows = fetch_all_from_conn(conn, sql, params) if conn is not None else fetch_all(sql, params)
    if not rows:
        return []

    timeline: dict[str, dict[str, object]] = {}
    ordered_days: list[str] = []
    for row in rows:
        scraped_at = str(row.get("scraped_at") or "").strip()
        scraped_dt = _parse_event_datetime(scraped_at)
        observed_day = scraped_dt.date().isoformat() if scraped_dt else scraped_at[:10]
        if observed_day not in timeline:
            timeline[observed_day] = {
                "observed_at": observed_day,
                "signal_count": 0,
                "new_signals": [],
            }
            ordered_days.append(observed_day)
        bucket = timeline[observed_day]
        current_signals: list[str] = []
        for value in (
            row.get("sold_recently_text"),
            row.get("stock_signal_text"),
            row.get("lowest_price_signal_text"),
            row.get("ranking_signal_text"),
            row.get("delivery_eta_signal_text"),
        ):
            signal_text = str(value or "").strip()
            if signal_text:
                current_signals.append(signal_text)
        if not current_signals:
            continue
        existing = set(bucket["new_signals"])
        for signal_text in current_signals:
            if signal_text not in existing:
                bucket["new_signals"].append(signal_text)
                existing.add(signal_text)
        bucket["signal_count"] = len(bucket["new_signals"])

    return [timeline[day] for day in sorted(ordered_days)]


def compute_dashboard_payload(category_path: str = "", selected_category_paths: list[str] | None = None) -> dict:
    selected_paths = normalize_category_scope_paths(
        category_path=category_path,
        selected_category_paths=selected_category_paths,
    )
    cached = _read_payload_cache("dashboard-payload", tuple(selected_paths))
    if cached is not None:
        return cached
    overview = fetch_one(
        f"""
        SELECT
            (SELECT COUNT(*) FROM product_identity) AS product_count,
            (SELECT COUNT(*) FROM keyword_catalog) AS keyword_count,
            (SELECT COUNT(*) FROM {WEB_PRODUCT_SUMMARY_TABLE} WHERE has_category = 1 AND has_keyword = 1) AS overlap_count,
            (SELECT MAX(imported_at) FROM source_databases) AS last_imported_at
        """
    )
    sync_visibility = compute_sync_visibility_payload()
    overview["dual_source_product_count"] = int(overview.get("overlap_count") or 0)
    overview["last_sync_at"] = (
        sync_visibility.get("shared_sync", {}).get("updated_at")
        or sync_visibility.get("warehouse", {}).get("last_imported_at")
        or overview.get("last_imported_at")
        or ""
    )
    if not selected_paths:
        scope_payload = build_global_scope_payload()
    elif len(selected_paths) == 1:
        scope_payload = build_category_scope_payload(selected_paths[0])
    else:
        scope_payload = build_multi_category_scope_payload(selected_paths)
    recent_imports = build_dashboard_recent_imports(limit=8)
    recent_keyword_runs = build_dashboard_recent_keyword_runs(limit=8)
    payload = {
        "overview": overview,
        "import_series": build_daily_import_series(days=30),
        "scope": scope_payload,
        "sync_visibility": sync_visibility,
        "delivery_breakdown": scope_payload.get("delivery_breakdown") or [],
        "source_coverage": build_dashboard_source_coverage(),
        "priority_categories": scope_payload.get("child_categories") or [],
        "recent_imports": recent_imports,
        "recent_keyword_runs": recent_keyword_runs,
        "selected_category_paths": selected_paths,
    }
    return _write_payload_cache("dashboard-payload", tuple(selected_paths), value=payload)


app = FastAPI(
    title="Noon Data ERP Beta",
    description="Noon 类目与关键词统一数据工作台 / Unified Category & Keyword Web Beta",
    version="0.2.0",
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.on_event("startup")
def prepare_web_read_models():
    ensure_web_read_models(force=False, allow_missing=True)
    try:
        build_daily_import_series(days=30)
        build_dashboard_source_coverage()
        build_dashboard_recent_imports(limit=8)
        build_dashboard_recent_keyword_runs(limit=8)
        compute_dashboard_payload()
    except Exception as exc:
        print(f"[web_beta.startup] dashboard prewarm skipped: {exc}")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(
        INDEX_PATH,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/health")
def health():
    db_config = get_warehouse_db_config()
    if db_config.is_sqlite and not db_config.sqlite_path_or_raise("web warehouse read model").exists():
        return {
            "status": "degraded",
            "warehouse_db": _safe_storage_label(db_config.sqlite_path_or_raise("web warehouse read model")),
            "product_count": 0,
            "observation_count": 0,
            "detail": "warehouse_db_missing",
        }
    overview = fetch_one(
        f"""
        SELECT
            (SELECT COUNT(*) FROM product_identity) AS product_count,
            (SELECT COUNT(*) FROM observation_events) AS observation_count
        """
    )
    return {
        "status": "ok",
        "warehouse_db": _warehouse_runtime_label(),
        "product_count": overview.get("product_count", 0),
        "observation_count": overview.get("observation_count", 0),
    }


@app.get("/api/tasks")
def tasks(
    status: str = Query(default=""),
    worker_type: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
):
    with closing(connect_ops_store()) as store:
        items = store.list_tasks(status=status, worker_type=worker_type, limit=limit)
        counts = store.get_status_counts()
        return {
            "ops_db": describe_ops_store(store),
            "counts": counts,
            "items": items,
        }


@app.post("/api/tasks")
def create_task(http_request: Request, request: TaskCreateRequest):
    require_admin_request(http_request)
    with closing(connect_ops_store()) as store:
        try:
            task = store.create_task(
                task_type=request.task_type,
                payload=request.payload,
                created_by=request.created_by,
                priority=request.priority,
                schedule_type=request.schedule_type,
                schedule_expr=request.schedule_expr,
                next_run_at=request.next_run_at,
                worker_type=request.worker_type,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return task


@app.post("/api/tasks/{task_id}/cancel")
def cancel_task_api(task_id: int, request: Request):
    require_admin_request(request)
    with closing(connect_ops_store()) as store:
        task = store.cancel_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"task not found: {task_id}")
        return task


@app.post("/api/tasks/{task_id}/retry")
def retry_task_api(task_id: int, request: Request):
    require_admin_request(request)
    with closing(connect_ops_store()) as store:
        task = store.retry_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"task not found: {task_id}")
        return task


@app.get("/api/task-runs")
def task_runs(task_id: int | None = Query(default=None), limit: int = Query(default=100, ge=1, le=500)):
    with closing(connect_ops_store()) as store:
        return {
            "ops_db": describe_ops_store(store),
            "items": store.list_task_runs(task_id=task_id, limit=limit),
        }


def _env_flag_enabled(name: str, default: bool = False) -> bool:
    raw_value = str(os.getenv(name, "")).strip().lower()
    if not raw_value:
        return default
    return raw_value in {"1", "true", "yes", "on"}


def _normalize_worker_payload(worker: object) -> dict[str, object]:
    normalized = dict(worker or {}) if isinstance(worker, dict) else {}
    details = normalized.get("details") if isinstance(normalized.get("details"), dict) else {}
    worker_type = str(normalized.get("worker_type") or "").strip().lower()
    node_role = str(
        details.get("node_role")
        or normalized.get("node_role")
        or normalized.get("worker_role")
        or ""
    ).strip().lower()
    node_host = str(details.get("node_host") or normalized.get("node_host") or "").strip()
    is_remote_category_worker = node_role == "remote_category"
    is_category_worker = worker_type == "category" or is_remote_category_worker
    if isinstance(details, dict):
        normalized_details = dict(details)
        normalized_details["node_role"] = node_role
        if node_host or "node_host" in normalized_details:
            normalized_details["node_host"] = node_host
        normalized["details"] = normalized_details
    normalized["node_role"] = node_role
    normalized["node_host"] = node_host
    normalized["is_remote_category_worker"] = is_remote_category_worker
    normalized["is_category_worker"] = is_category_worker
    normalized["category_worker_mode"] = (
        "remote_category"
        if is_remote_category_worker
        else ("local_category" if worker_type == "category" else "")
    )
    return normalized


def _summarize_worker_inventory(worker_items: list[dict[str, object]]) -> dict[str, object]:
    worker_type_counts: dict[str, int] = {}
    node_role_counts: dict[str, int] = {}
    remote_category_hosts: list[str] = []
    category_worker_hosts: list[str] = []
    remote_category_worker_count = 0
    local_category_worker_count = 0
    category_worker_count = 0
    for raw_worker in worker_items:
        worker = _normalize_worker_payload(raw_worker)
        worker_type = str(worker.get("worker_type") or "").strip().lower()
        if worker_type:
            worker_type_counts[worker_type] = worker_type_counts.get(worker_type, 0) + 1
        node_role = str(worker.get("node_role") or "").strip().lower()
        if node_role:
            node_role_counts[node_role] = node_role_counts.get(node_role, 0) + 1
        if worker.get("is_category_worker"):
            category_worker_count += 1
            node_host = str(worker.get("node_host") or "").strip()
            if node_host and node_host not in category_worker_hosts:
                category_worker_hosts.append(node_host)
        if worker.get("is_remote_category_worker"):
            remote_category_worker_count += 1
            node_host = str(worker.get("node_host") or "").strip()
            if node_host and node_host not in remote_category_hosts:
                remote_category_hosts.append(node_host)
        elif worker_type == "category":
            local_category_worker_count += 1
    category_worker_heartbeat_state = "present" if category_worker_count > 0 else "missing"
    return {
        "worker_count": len(worker_items),
        "worker_type_counts": worker_type_counts,
        "node_role_counts": node_role_counts,
        "remote_category_node_enabled": _env_flag_enabled("NOON_REMOTE_CATEGORY_NODE_ENABLED"),
        "category_worker_count": category_worker_count,
        "category_worker_heartbeat_state": category_worker_heartbeat_state,
        "category_worker_heartbeat_present": category_worker_count > 0,
        "remote_category_worker_count": remote_category_worker_count,
        "local_category_worker_count": local_category_worker_count,
        "remote_category_hosts": sorted(remote_category_hosts),
        "category_worker_hosts": sorted(category_worker_hosts),
    }


def _build_auto_dispatch_payload(
    *,
    plans: list[dict[str, object]],
    worker_items: list[dict[str, object]],
    watchdog: dict[str, object] | None = None,
) -> dict[str, object]:
    return build_auto_dispatch_summary(
        [dict(item) for item in plans],
        worker_items=[dict(item) for item in worker_items],
        host_alternating_service_active=bool((watchdog or {}).get("host_alternating_service_active")),
    )


@app.get("/api/workers")
def workers():
    with closing(connect_ops_store()) as store:
        stale_worker_max_age = get_stale_worker_max_age_seconds()
        store.prune_stale_workers(max_age_seconds=stale_worker_max_age)
        worker_items = [
            _normalize_worker_payload(item)
            for item in store.list_workers(max_age_seconds=stale_worker_max_age)
        ]
        return {
            "ops_db": describe_ops_store(store),
            "items": worker_items,
            "summary": _summarize_worker_inventory(worker_items),
        }


@app.get("/api/crawler/catalog")
def crawler_catalog(request: Request):
    require_admin_request(request)
    return build_crawler_catalog()


@app.get("/api/crawler/keyword-controls")
def crawler_keyword_controls(request: Request, monitor_config: str = Query(default="")):
    require_admin_request(request)
    monitor_token = str(monitor_config or "").strip()
    if not monitor_token:
        raise HTTPException(status_code=400, detail="monitor_config is required")
    try:
        payload = get_keyword_control_state(monitor_token)
        effective_rows = _build_effective_active_keyword_rows(monitor_token)
        payload["effective_keyword_stats"] = {
            "baseline_count": len(payload.get("baseline_keywords") or []),
            "active_keyword_count": len(effective_rows),
            "disabled_keyword_count": len(payload.get("disabled_keywords") or []),
            "blocked_root_count": len(payload.get("blocked_roots") or []),
        }
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/crawler/keyword-controls/active-keywords")
def crawler_keyword_control_active_keywords(
    request: Request,
    monitor_config: str = Query(default=""),
    q: str = Query(default=""),
    source_type: str = Query(default=""),
    tracking_mode: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    require_admin_request(request)
    monitor_token = str(monitor_config or "").strip()
    if not monitor_token:
        raise HTTPException(status_code=400, detail="monitor_config is required")
    query_token = _normalize_keyword_control_token(q)
    source_token = _normalize_keyword_control_token(source_type)
    tracking_token = _normalize_keyword_control_token(tracking_mode)
    payload = get_monitor_active_keywords(monitor_token)
    items = list(payload.get("items") or [])
    if query_token:
        items = [
            item for item in items
            if query_token in _normalize_keyword_control_token(item.get("keyword"))
        ]
    if source_token:
        items = [
            item for item in items
            if _normalize_keyword_control_token(item.get("source_type")) == source_token
        ]
    if tracking_token:
        items = [
            item for item in items
            if _normalize_keyword_control_token(item.get("tracking_mode")) == tracking_token
        ]
    total = len(items)
    return {
        **payload,
        "monitor_config": monitor_token,
        "q": query_token,
        "source_type": source_token,
        "tracking_mode": tracking_token,
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items[offset:offset + limit],
    }


@app.get("/api/crawler/keyword-controls/roots")
def crawler_keyword_control_roots(request: Request, monitor_config: str = Query(default="")):
    require_admin_request(request)
    monitor_token = str(monitor_config or "").strip()
    if not monitor_token:
        raise HTTPException(status_code=400, detail="monitor_config is required")
    payload = get_keyword_control_state(monitor_token)
    return {
        "monitor_config": payload.get("monitor_config") or monitor_token,
        "monitor_label": payload.get("monitor_label") or "",
        "blocked_roots": list(payload.get("blocked_roots") or []),
        "available_sources": list(payload.get("available_sources") or KEYWORD_CONTROL_SCOPES),
        "available_root_match_modes": list(payload.get("available_root_match_modes") or KEYWORD_ROOT_MATCH_MODES),
        "updated_at": payload.get("updated_at") or "",
    }


@app.post("/api/crawler/keyword-controls/baseline")
def update_crawler_keyword_baseline(http_request: Request, request: KeywordControlBaselineRequest):
    require_admin_request(http_request)
    try:
        payload = update_monitor_baseline_keywords(
            request.monitor_config,
            keywords=request.keywords,
            mode=request.mode,
        )
        if str(request.mode or "").strip().lower() == "add":
            update_monitor_disabled_keyword_rules(
                request.monitor_config,
                keywords=request.keywords,
                blocked_sources=[],
                mode="restore",
            )
            payload = get_keyword_control_state(request.monitor_config)
            payload["registered_count"] = _register_monitor_baseline_keywords_immediately(
                request.monitor_config,
                request.keywords,
            )
        return payload
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/crawler/keyword-controls/disable")
def update_crawler_keyword_disable(http_request: Request, request: KeywordControlDisableRequest):
    require_admin_request(http_request)
    _validate_keyword_control_sources(request.blocked_sources)
    blocked_sources = _normalize_keyword_control_sources(request.blocked_sources)
    normalized_keywords = _coalesce_keyword_control_keywords(request.keywords, keyword=request.keyword)
    if not normalized_keywords:
        raise HTTPException(status_code=400, detail="keywords is required")
    try:
        update_monitor_baseline_keywords(
            request.monitor_config,
            keywords=normalized_keywords,
            mode="remove",
        )
        return update_monitor_disabled_keyword_rules(
            request.monitor_config,
            keywords=normalized_keywords,
            blocked_sources=blocked_sources,
            reason=request.reason,
            mode="disable",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/crawler/keyword-controls/restore")
def update_crawler_keyword_restore(http_request: Request, request: KeywordControlRestoreRequest):
    require_admin_request(http_request)
    normalized_keywords = _coalesce_keyword_control_keywords(request.keywords, keyword=request.keyword)
    if not normalized_keywords:
        raise HTTPException(status_code=400, detail="keywords is required")
    try:
        return update_monitor_disabled_keyword_rules(
            request.monitor_config,
            keywords=normalized_keywords,
            blocked_sources=[],
            mode="restore",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/crawler/keyword-controls/roots")
def update_crawler_keyword_roots(http_request: Request, request: KeywordControlRootsRequest):
    require_admin_request(http_request)
    _validate_keyword_control_sources(request.blocked_sources)
    blocked_sources = _normalize_keyword_control_sources(request.blocked_sources)
    normalized_keywords = _coalesce_keyword_control_keywords(request.keywords, keyword=request.keyword)
    if not normalized_keywords:
        raise HTTPException(status_code=400, detail="keywords is required")
    match_mode = _normalize_keyword_control_token(request.match_mode)
    if match_mode not in KEYWORD_ROOT_MATCH_MODES:
        raise HTTPException(status_code=400, detail="match_mode must be exact or contains")
    mode = _normalize_keyword_control_token(request.mode)
    if mode not in {"upsert", "delete"}:
        raise HTTPException(status_code=400, detail="mode must be upsert or delete")
    try:
        return update_monitor_blocked_root_rules(
            request.monitor_config,
            root_keywords=normalized_keywords,
            blocked_sources=blocked_sources,
            reason=request.reason,
            match_mode=match_mode,
            mode=mode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/crawler/keyword-controls/exclusions")
def update_crawler_keyword_exclusions(http_request: Request, request: KeywordControlExclusionRequest):
    require_admin_request(http_request)
    _validate_keyword_control_sources(request.blocked_sources)
    blocked_sources = _normalize_keyword_control_sources(request.blocked_sources)
    try:
        return update_monitor_exclusion_rule(
            request.monitor_config,
            keyword=request.keyword,
            blocked_sources=blocked_sources,
            reason=request.reason,
            mode=request.mode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/crawler/plans")
def crawler_plans(request: Request, enabled: str = Query(default=""), limit: int = Query(default=200, ge=1, le=500)):
    require_admin_request(request)
    enabled_flag: bool | None = None
    if enabled.strip():
        enabled_flag = enabled.strip().lower() in {"1", "true", "yes", "on"}
    with closing(connect_ops_store()) as store:
        stale_worker_max_age = get_stale_worker_max_age_seconds()
        worker_items = [
            _normalize_worker_payload(item)
            for item in store.list_workers(max_age_seconds=stale_worker_max_age)
        ]
        watchdog = _build_watchdog_summary_payload()
        plans = annotate_crawl_plans(store.list_crawl_plans(enabled=enabled_flag, limit=limit))
        return {
            "ops_db": describe_ops_store(store),
            "items": plans,
            "auto_dispatch": _build_auto_dispatch_payload(
                plans=plans,
                worker_items=worker_items,
                watchdog=watchdog,
            ),
        }


@app.post("/api/crawler/plans")
def create_crawler_plan(http_request: Request, request: CrawlerPlanCreateRequest):
    require_admin_request(http_request)
    try:
        normalized_payload = normalize_plan_payload(request.plan_type, request.payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    with closing(connect_ops_store()) as store:
        try:
            plan = store.create_crawl_plan(
                plan_type=request.plan_type,
                name=request.name,
                created_by=request.created_by,
                schedule_kind=request.schedule_kind,
                schedule_json=request.schedule_json,
                payload=normalized_payload,
                enabled=request.enabled,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return plan


@app.patch("/api/crawler/plans/{plan_id}")
def update_crawler_plan(plan_id: int, http_request: Request, request: CrawlerPlanUpdateRequest):
    require_admin_request(http_request)
    changes: dict[str, object] = {}
    if request.name is not None:
        changes["name"] = request.name
    if request.enabled is not None:
        changes["enabled"] = request.enabled
    if request.schedule_kind is not None:
        changes["schedule_kind"] = request.schedule_kind
    if request.schedule_json is not None:
        changes["schedule_json"] = request.schedule_json
    if request.payload is not None:
        current_payload = request.payload
        with closing(connect_ops_store()) as store:
            existing = store.get_crawl_plan(plan_id)
            if existing is None:
                raise HTTPException(status_code=404, detail=f"plan not found: {plan_id}")
            try:
                changes["payload"] = normalize_plan_payload(str(existing["plan_type"]), current_payload)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            try:
                plan = store.update_crawl_plan(plan_id, **changes)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return plan
    with closing(connect_ops_store()) as store:
        if store.get_crawl_plan(plan_id) is None:
            raise HTTPException(status_code=404, detail=f"plan not found: {plan_id}")
        try:
            plan = store.update_crawl_plan(plan_id, **changes)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return plan


@app.post("/api/crawler/plans/{plan_id}/launch")
def launch_crawler_plan(plan_id: int, request: Request):
    require_admin_request(request)
    with closing(connect_ops_store()) as store:
        try:
            task = launch_plan_now(store, plan_id, created_by="crawler_console")
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return task


@app.post("/api/crawler/plans/{plan_id}/pause")
def pause_crawler_plan(plan_id: int, request: Request):
    require_admin_request(request)
    with closing(connect_ops_store()) as store:
        plan = store.pause_crawl_plan(plan_id)
        if plan is None:
            raise HTTPException(status_code=404, detail=f"plan not found: {plan_id}")
        return plan


@app.post("/api/crawler/plans/{plan_id}/resume")
def resume_crawler_plan(plan_id: int, request: Request):
    require_admin_request(request)
    with closing(connect_ops_store()) as store:
        try:
            plan = store.resume_crawl_plan(plan_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if plan is None:
            raise HTTPException(status_code=404, detail=f"plan not found: {plan_id}")
        return plan


@app.get("/api/crawler/runs")
def crawler_runs(request: Request, limit: int = Query(default=100, ge=1, le=500)):
    require_admin_request(request)
    with closing(connect_ops_store()) as store:
        return {
            "ops_db": describe_ops_store(store),
            "items": list_crawler_runs(store, limit=limit),
        }


@app.get("/api/crawler/runs/{task_id}")
def crawler_run_detail(task_id: int, request: Request):
    require_admin_request(request)
    with closing(connect_ops_store()) as store:
        payload = get_crawler_run_detail(store, task_id)
        if payload is None:
            raise HTTPException(status_code=404, detail=f"task not found: {task_id}")
        return payload


@app.get("/api/system/health")
def system_health():
    base_health = health()
    sync_state = _build_shared_sync_status_payload()
    sync_visibility = compute_sync_visibility_payload()
    watchdog = _build_watchdog_summary_payload()
    keyword_quality = _summarize_keyword_quality(
        fetch_all(
            """
            SELECT
                run_type,
                trigger_mode,
                seed_keyword,
                snapshot_id,
                status,
                keyword_count,
                started_at,
                finished_at,
                metadata_json
            FROM keyword_runs_log
            ORDER BY started_at DESC, id DESC
            LIMIT 20
            """
        )
    )
    with closing(connect_ops_store()) as store:
        stale_worker_max_age = get_stale_worker_max_age_seconds()
        store.prune_stale_workers(max_age_seconds=stale_worker_max_age)
        workers_payload = [
            _normalize_worker_payload(item)
            for item in store.list_workers(max_age_seconds=stale_worker_max_age)
        ]
        plans_payload = annotate_crawl_plans(store.list_crawl_plans(limit=200))
        worker_summary = _summarize_worker_inventory(workers_payload)
        task_counts = store.get_status_counts()
        latest_runs = store.list_task_runs(limit=10)
        auto_dispatch = _build_auto_dispatch_payload(
            plans=plans_payload,
            worker_items=workers_payload,
            watchdog=watchdog,
        )
        return {
            "status": "ok",
            "warehouse": base_health,
            "ops": {
                "ops_db": describe_ops_store(store),
                "task_status_counts": task_counts,
                "worker_count": len(workers_payload),
                "workers": workers_payload,
                "worker_summary": worker_summary,
                "plans": plans_payload,
                "recent_runs": latest_runs,
            },
            "shared_sync": sync_state,
            "sync_visibility": sync_visibility,
            "keyword_quality": keyword_quality,
            "auto_dispatch": auto_dispatch,
            "watchdog": watchdog,
        }


@app.get("/api/dashboard")
def dashboard(
    request: Request,
    category_path: str = Query(default=""),
    selected_category_paths: list[str] = Query(default=[]),
):
    explicit_scope_paths = request.query_params.getlist("selected_category_paths") or selected_category_paths
    return compute_dashboard_payload(
        category_path=category_path,
        selected_category_paths=explicit_scope_paths,
    )


@app.get("/api/categories/tree")
def categories_tree():
    rows = fetch_canonical_category_counts()
    return {
        "items": build_category_tree(rows),
        "flat_items": rows,
    }


@app.get("/api/categories/summary")
def categories_summary(
    parent_path: str = Query(default=""),
    category_path: str = Query(default=""),
    sort: str = Query(default="product_count_desc"),
    avg_price_min: float | None = Query(default=None, ge=0),
    avg_price_max: float | None = Query(default=None, ge=0),
    express_share_min: float | None = Query(default=None, ge=0, le=100),
    express_share_max: float | None = Query(default=None, ge=0, le=100),
    ad_share_min: float | None = Query(default=None, ge=0, le=100),
    ad_share_max: float | None = Query(default=None, ge=0, le=100),
    product_count_min: int | None = Query(default=None, ge=0),
    product_count_max: int | None = Query(default=None, ge=0),
    bsr_coverage_min: float | None = Query(default=None, ge=0, le=100),
    bsr_coverage_max: float | None = Query(default=None, ge=0, le=100),
    signal_coverage_min: float | None = Query(default=None, ge=0, le=100),
    signal_coverage_max: float | None = Query(default=None, ge=0, le=100),
):
    scope_path = parent_path.strip() if isinstance(parent_path, str) else ""
    if not scope_path and isinstance(category_path, str):
        scope_path = category_path.strip()
    items = list_immediate_child_categories(scope_path)
    overviews_by_path = fetch_category_overviews_for_paths([item["path"] for item in items])
    leaderboard: list[dict] = []
    for item in items:
        overview = overviews_by_path.get(item["path"])
        if not overview:
            continue
        if not category_passes_filters(
            overview,
            avg_price_min=avg_price_min,
            avg_price_max=avg_price_max,
            express_share_min=express_share_min,
            express_share_max=express_share_max,
            ad_share_min=ad_share_min,
            ad_share_max=ad_share_max,
            product_count_min=product_count_min,
            product_count_max=product_count_max,
            bsr_coverage_min=bsr_coverage_min,
            bsr_coverage_max=bsr_coverage_max,
            signal_coverage_min=signal_coverage_min,
            signal_coverage_max=signal_coverage_max,
        ):
            continue
        leaderboard.append(overview)

    return {
        "parent_path": scope_path or "Home",
        "items": sort_category_items(leaderboard, sort),
        "total_count": len(leaderboard),
        "sort": sort,
    }


@app.get("/api/categories/products")
def categories_products(
    category_path: str = Query(..., min_length=1),
    q: str = Query(default=""),
    platform: str = Query(default=""),
    source_scope: str = Query(default=""),
    delivery_type: str = Query(default=""),
    is_ad: int | None = Query(default=None, ge=0, le=1),
    tab: str = Query(default="ranked"),
    bsr_min: int | None = Query(default=None, ge=1),
    bsr_max: int | None = Query(default=None, ge=1),
    review_min: int | None = Query(default=None, ge=0),
    review_max: int | None = Query(default=None, ge=0),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    has_sold_signal: bool | None = Query(default=None),
    has_stock_signal: bool | None = Query(default=None),
    has_lowest_price_signal: bool | None = Query(default=None),
    signal_tags: str = Query(default=""),
    signal_text: str = Query(default=""),
    sort: str = Query(default="latest_observed_at"),
    limit: int = Query(default=DEFAULT_PAGE_LIMIT, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    payload = fetch_products_payload(
        q=q,
        platform=platform,
        source_scope=source_scope,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        price_min=price_min,
        price_max=price_max,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        category_path=category_path,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    payload["category_path"] = category_path
    return payload


@app.get("/api/categories/benchmarks")
def categories_benchmarks(category_path: str = Query(..., min_length=1)):
    return compute_category_benchmarks_payload(category_path)


@app.get("/api/products/signal-options")
def product_signal_options(
    q: str = Query(default=""),
    market: str = Query(default=""),
    platform: str = Query(default=""),
    source_scope: str = Query(default=""),
    category_path: str = Query(default=""),
    selected_category_paths: list[str] = Query(default=[]),
    keyword: str = Query(default=""),
    delivery_type: str = Query(default=""),
    is_ad: int | None = Query(default=None, ge=0, le=1),
    tab: str = Query(default="all"),
    bsr_min: int | None = Query(default=None, ge=1),
    bsr_max: int | None = Query(default=None, ge=1),
    review_min: int | None = Query(default=None, ge=0),
    review_max: int | None = Query(default=None, ge=0),
    rating_min: float | None = Query(default=None, ge=0),
    rating_max: float | None = Query(default=None, le=5),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    sales_min: int | None = Query(default=None, ge=0),
    sales_max: int | None = Query(default=None, ge=0),
    inventory_min: int | None = Query(default=None, ge=0),
    inventory_max: int | None = Query(default=None, ge=0),
):
    return {
        "items": fetch_product_signal_options(
            q=q,
            market=market,
            platform=platform,
            source_scope=source_scope,
            category_path=category_path,
            selected_category_paths=selected_category_paths,
            keyword=keyword,
            delivery_type=delivery_type,
            is_ad=is_ad,
            tab=tab,
            bsr_min=bsr_min,
            bsr_max=bsr_max,
            review_min=review_min,
            review_max=review_max,
            rating_min=rating_min,
            rating_max=rating_max,
            price_min=price_min,
            price_max=price_max,
            sales_min=sales_min,
            sales_max=sales_max,
            inventory_min=inventory_min,
            inventory_max=inventory_max,
        )
    }


@app.get("/api/ui/filter-presets")
def list_ui_filter_presets(request: Request, view: str = Query(default="products")):
    user_key, _ = resolve_ui_user_key(request)
    with closing(connect_ops_store()) as store:
        return {
            "user_key": user_key,
            "view": normalize_ui_view_name(view),
            "items": store.list_ui_filter_presets(user_key=user_key, view_name=normalize_ui_view_name(view)),
        }


@app.post("/api/ui/filter-presets")
def create_ui_filter_preset(request: Request, payload: UiFilterPresetCreateRequest):
    user_key, _ = resolve_ui_user_key(request)
    view_name = normalize_ui_view_name(payload.view)
    with closing(connect_ops_store()) as store:
        item = store.create_ui_filter_preset(
            user_key=user_key,
            view_name=view_name,
            preset_name=payload.preset_name,
            route_payload=payload.route_payload,
            is_default=payload.is_default,
        )
        return {"user_key": user_key, "view": view_name, "item": item}


@app.patch("/api/ui/filter-presets/{preset_id}")
def patch_ui_filter_preset(request: Request, preset_id: int, payload: UiFilterPresetUpdateRequest):
    user_key, _ = resolve_ui_user_key(request)
    route_payload = dict(payload.route_payload) if payload.route_payload is not None else None
    with closing(connect_ops_store()) as store:
        current = store.get_ui_filter_preset(preset_id)
        if current is None:
            raise HTTPException(status_code=404, detail=f"preset not found: {preset_id}")
        item = store.update_ui_filter_preset(
            preset_id,
            user_key=user_key,
            view_name=str(current["view_name"]),
            preset_name=payload.preset_name,
            route_payload=route_payload,
            is_default=payload.is_default,
            mark_used=payload.mark_used,
        )
        if item is None:
            raise HTTPException(status_code=404, detail=f"preset not found: {preset_id}")
        return {"item": item}


@app.delete("/api/ui/filter-presets/{preset_id}")
def remove_ui_filter_preset(request: Request, preset_id: int):
    user_key, _ = resolve_ui_user_key(request)
    with closing(connect_ops_store()) as store:
        current = store.get_ui_filter_preset(preset_id)
        if current is None:
            raise HTTPException(status_code=404, detail=f"preset not found: {preset_id}")
        deleted = store.delete_ui_filter_preset(
            preset_id,
            user_key=user_key,
            view_name=str(current["view_name"]),
        )
        return {"deleted": deleted}


@app.get("/api/ui/filter-history")
def list_ui_filter_history(request: Request, view: str = Query(default="products"), limit: int = Query(default=12, ge=1, le=24)):
    user_key, _ = resolve_ui_user_key(request)
    view_name = normalize_ui_view_name(view)
    with closing(connect_ops_store()) as store:
        return {
            "user_key": user_key,
            "view": view_name,
            "items": store.list_ui_filter_history(user_key=user_key, view_name=view_name, limit=limit),
        }


@app.post("/api/ui/filter-history")
def create_ui_filter_history(request: Request, payload: UiFilterHistoryRequest):
    user_key, _ = resolve_ui_user_key(request)
    view_name = normalize_ui_view_name(payload.view)
    with closing(connect_ops_store()) as store:
        items = store.record_ui_filter_history(
            user_key=user_key,
            view_name=view_name,
            route_payload=payload.route_payload,
            summary=payload.summary,
        )
        return {"user_key": user_key, "view": view_name, "items": items}


@app.get("/api/ui/product-favorites")
def list_product_favorites(request: Request, q: str = Query(default="")):
    user_key, _ = resolve_ui_user_key(request)
    payload = fetch_product_favorites_payload(user_key=user_key, q=q)
    return {"user_key": user_key, **payload}


@app.post("/api/ui/product-favorites")
def create_product_favorite(request: Request, payload: ProductFavoriteRequest):
    user_key, _ = resolve_ui_user_key(request)
    item = add_product_favorite(user_key=user_key, platform=payload.platform, product_id=payload.product_id)
    return {"user_key": user_key, "item": item}


@app.delete("/api/ui/product-favorites/{platform}/{product_id}")
def delete_product_favorite(request: Request, platform: str, product_id: str):
    user_key, _ = resolve_ui_user_key(request)
    deleted = remove_product_favorite(user_key=user_key, platform=platform, product_id=product_id)
    return {"user_key": user_key, "deleted": deleted}


@app.get("/api/keywords/summary")
def keywords_summary(limit: int = Query(default=50, ge=1, le=200)):
    items = fetch_all(
        f"""
        {KEYWORD_RANKED_METRICS_CTE}
        SELECT
            kc.keyword,
            COALESCE(kc.display_keyword, kc.keyword) AS display_keyword,
            kc.status,
            kc.tracking_mode,
            kc.source_platform,
            kc.priority,
            kc.last_crawled_at,
            kc.last_analyzed_at,
            rm.grade,
            rm.rank,
            rm.total_score,
            rm.demand_index,
            rm.competition_density,
            rm.supply_gap_ratio,
            rm.margin_war_pct,
            rm.margin_peace_pct,
            COUNT(DISTINCT pkm.platform || '|' || pkm.product_id) AS matched_product_count
        FROM keyword_catalog kc
        LEFT JOIN ranked_metrics rm
          ON rm.keyword = kc.keyword
         AND rm.rn = 1
        LEFT JOIN product_keyword_membership pkm
          ON pkm.keyword = kc.keyword
        GROUP BY
            kc.keyword,
            kc.display_keyword,
            kc.status,
            kc.tracking_mode,
            kc.source_platform,
            kc.priority,
            kc.last_crawled_at,
            kc.last_analyzed_at,
            rm.grade,
            rm.rank,
            rm.total_score,
            rm.demand_index,
            rm.competition_density,
            rm.supply_gap_ratio,
            rm.margin_war_pct,
            rm.margin_peace_pct
        ORDER BY COALESCE(rm.total_score, 0) DESC, kc.keyword ASC
        LIMIT ?
        """,
        (limit,),
    )

    summary = fetch_one(
        """
        SELECT
            COUNT(*) AS keyword_count,
            SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_keyword_count,
            MAX(last_analyzed_at) AS last_analyzed_at
        FROM keyword_catalog
        """
    )

    summary["top_keyword"] = items[0]["keyword"] if items else ""
    summary["avg_score"] = round(
        sum(float(item.get("total_score") or 0) for item in items) / len(items),
        2,
    ) if items else 0

    return {"summary": summary, "items": items}


@app.get("/api/keywords/opportunities")
def keywords_opportunities(
    limit: int = Query(default=50, ge=1, le=200),
    opportunity_type: str = Query(default=""),
    root_keyword: str = Query(default=""),
    risk_level: str = Query(default=""),
    priority_band: str = Query(default=""),
    evidence_strength: str = Query(default=""),
):
    rows = fetch_keyword_opportunity_rows(limit=max(limit * 4, 200))
    items = build_opportunity_items(rows, limit=max(limit * 4, 200))

    normalized_root = root_keyword.strip().lower()
    normalized_type = opportunity_type.strip().lower()
    normalized_risk = risk_level.strip().lower()
    normalized_priority = priority_band.strip().lower()
    normalized_evidence = evidence_strength.strip().lower()

    filtered = []
    for item in items:
        if normalized_type and str(item.get("opportunity_type") or "").lower() != normalized_type:
            continue
        if normalized_risk and str(item.get("risk_level") or "").lower() != normalized_risk:
            continue
        if normalized_priority and str(item.get("priority_band") or "").lower() != normalized_priority:
            continue
        if normalized_evidence and str(item.get("evidence_strength") or "").lower() != normalized_evidence:
            continue
        if normalized_root and str(item.get("root_seed_keyword") or "").lower() != normalized_root:
            continue
        filtered.append(item)

    return {
        "summary": build_opportunity_summary(
            filtered,
            limit=limit,
            opportunity_type=normalized_type,
            root_keyword=normalized_root,
            risk_level=normalized_risk,
            priority_band=normalized_priority,
            evidence_strength=normalized_evidence,
        ),
        "items": filtered[:limit],
    }


@app.get("/api/keywords/quality-issues")
def keywords_quality_issues(limit: int = Query(default=50, ge=1, le=200)):
    items = [
        item
        for item in build_opportunity_items(fetch_keyword_opportunity_rows(limit=max(limit * 4, 200)), limit=max(limit * 4, 200))
        if item.get("quality_flags")
    ]
    return {
        "summary": build_quality_issue_summary(items, limit=limit),
        "items": items[:limit],
    }


@app.get("/api/keywords/graph")
def keywords_graph(
    root_keyword: str = Query(..., min_length=1),
    depth: int = Query(default=2, ge=1, le=3),
    limit: int = Query(default=200, ge=1, le=500),
):
    normalized_root = root_keyword.strip().lower()
    edge_rows = fetch_keyword_graph_rows(normalized_root, depth=depth, limit=limit)
    keyword_set = {normalized_root}
    for row in edge_rows:
        if row.get("parent_keyword"):
            keyword_set.add(str(row["parent_keyword"]).strip().lower())
        if row.get("child_keyword"):
            keyword_set.add(str(row["child_keyword"]).strip().lower())

    metric_rows = fetch_keyword_graph_metric_rows(sorted(keyword_set))
    payload = build_keyword_graph_payload(normalized_root, edge_rows, metric_rows)
    payload["depth"] = depth
    payload["limit"] = limit
    return payload


@app.get("/api/keywords/seed-groups")
def keywords_seed_groups():
    catalog = load_seed_catalog()
    catalog_rows = fetch_all(
        """
        SELECT keyword, tracking_mode, last_crawled_at
        FROM keyword_catalog
        """
    )
    keyword_index = {
        str(row.get("keyword") or "").strip().lower(): row
        for row in catalog_rows
        if str(row.get("keyword") or "").strip()
    }
    items = []
    for group in catalog.get("groups") or []:
        slug = str(group.get("slug") or "").strip().lower()
        group_keywords = [
            str(item).strip().lower()
            for item in (group.get("keywords") or [])
            if str(item).strip()
        ]
        matched_rows = [keyword_index[item] for item in group_keywords if item in keyword_index]
        items.append(
            {
                "slug": slug,
                "label": group.get("label") or slug,
                "priority": int(group.get("priority") or 30),
                "seed_count": len(group_keywords),
                "keyword_count": len(matched_rows),
                "tracked_count": sum(1 for row in matched_rows if row.get("tracking_mode") == "tracked"),
                "crawled_count": sum(1 for row in matched_rows if row.get("last_crawled_at")),
            }
        )
    return {"items": items}


@app.get("/api/keywords/products")
def keywords_products(
    keyword: str = Query(..., min_length=1),
    q: str = Query(default=""),
    market: str = Query(default=""),
    platform: str = Query(default=""),
    source_scope: str = Query(default=""),
    delivery_type: str = Query(default=""),
    is_ad: int | None = Query(default=None, ge=0, le=1),
    tab: str = Query(default="ranked"),
    bsr_min: int | None = Query(default=None, ge=1),
    bsr_max: int | None = Query(default=None, ge=1),
    review_min: int | None = Query(default=None, ge=0),
    review_max: int | None = Query(default=None, ge=0),
    rating_min: float | None = Query(default=None, ge=0),
    rating_max: float | None = Query(default=None, le=5),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    sales_min: int | None = Query(default=None, ge=0),
    sales_max: int | None = Query(default=None, ge=0),
    inventory_min: int | None = Query(default=None, ge=0),
    inventory_max: int | None = Query(default=None, ge=0),
    review_growth_7d_min: int | None = Query(default=None),
    review_growth_14d_min: int | None = Query(default=None),
    rating_growth_7d_min: float | None = Query(default=None),
    rating_growth_14d_min: float | None = Query(default=None),
    has_sold_signal: bool | None = Query(default=None),
    has_stock_signal: bool | None = Query(default=None),
    has_lowest_price_signal: bool | None = Query(default=None),
    signal_tags: str = Query(default=""),
    signal_text: str = Query(default=""),
    sort: str = Query(default="latest_observed_at"),
    limit: int = Query(default=DEFAULT_PAGE_LIMIT, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    payload = fetch_products_payload(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        review_growth_7d_min=review_growth_7d_min,
        review_growth_14d_min=review_growth_14d_min,
        rating_growth_7d_min=rating_growth_7d_min,
        rating_growth_14d_min=rating_growth_14d_min,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        keyword=keyword,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    payload["keyword"] = keyword
    return payload


@app.get("/api/keywords/benchmarks")
def keywords_benchmarks(keyword: str = Query(..., min_length=1)):
    return compute_keyword_benchmarks_payload(keyword)


@app.get("/api/products")
def products(
    q: str = Query(default=""),
    market: str = Query(default=""),
    platform: str = Query(default=""),
    source_scope: str = Query(default=""),
    category_path: str = Query(default=""),
    selected_category_paths: list[str] = Query(default=[]),
    keyword: str = Query(default=""),
    delivery_type: str = Query(default=""),
    is_ad: int | None = Query(default=None, ge=0, le=1),
    tab: str = Query(default="all"),
    bsr_min: int | None = Query(default=None, ge=1),
    bsr_max: int | None = Query(default=None, ge=1),
    review_min: int | None = Query(default=None, ge=0),
    review_max: int | None = Query(default=None, ge=0),
    rating_min: float | None = Query(default=None, ge=0),
    rating_max: float | None = Query(default=None, le=5),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    sales_min: int | None = Query(default=None, ge=0),
    sales_max: int | None = Query(default=None, ge=0),
    inventory_min: int | None = Query(default=None, ge=0),
    inventory_max: int | None = Query(default=None, ge=0),
    review_growth_7d_min: int | None = Query(default=None),
    review_growth_14d_min: int | None = Query(default=None),
    rating_growth_7d_min: float | None = Query(default=None),
    rating_growth_14d_min: float | None = Query(default=None),
    has_sold_signal: bool | None = Query(default=None),
    has_stock_signal: bool | None = Query(default=None),
    has_lowest_price_signal: bool | None = Query(default=None),
    signal_tags: str = Query(default=""),
    signal_text: str = Query(default=""),
    sort: str = Query(default="sales_desc"),
    limit: int = Query(default=DEFAULT_PAGE_LIMIT, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    return fetch_products_payload(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        review_growth_7d_min=review_growth_7d_min,
        review_growth_14d_min=review_growth_14d_min,
        rating_growth_7d_min=rating_growth_7d_min,
        rating_growth_14d_min=rating_growth_14d_min,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        category_path=category_path,
        selected_category_paths=selected_category_paths,
        keyword=keyword,
        sort=sort,
        limit=limit,
        offset=offset,
    )


@app.get("/api/export/products.csv")
def export_products(
    q: str = Query(default=""),
    market: str = Query(default=""),
    platform: str = Query(default=""),
    source_scope: str = Query(default=""),
    category_path: str = Query(default=""),
    selected_category_paths: list[str] = Query(default=[]),
    keyword: str = Query(default=""),
    delivery_type: str = Query(default=""),
    is_ad: int | None = Query(default=None, ge=0, le=1),
    tab: str = Query(default="ranked"),
    bsr_min: int | None = Query(default=None, ge=1),
    bsr_max: int | None = Query(default=None, ge=1),
    review_min: int | None = Query(default=None, ge=0),
    review_max: int | None = Query(default=None, ge=0),
    rating_min: float | None = Query(default=None, ge=0),
    rating_max: float | None = Query(default=None, le=5),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    sales_min: int | None = Query(default=None, ge=0),
    sales_max: int | None = Query(default=None, ge=0),
    inventory_min: int | None = Query(default=None, ge=0),
    inventory_max: int | None = Query(default=None, ge=0),
    review_growth_7d_min: int | None = Query(default=None),
    review_growth_14d_min: int | None = Query(default=None),
    rating_growth_7d_min: float | None = Query(default=None),
    rating_growth_14d_min: float | None = Query(default=None),
    has_sold_signal: bool | None = Query(default=None),
    has_stock_signal: bool | None = Query(default=None),
    has_lowest_price_signal: bool | None = Query(default=None),
    signal_tags: str = Query(default=""),
    signal_text: str = Query(default=""),
    sort: str = Query(default="sales_desc"),
    limit: int = Query(default=DEFAULT_EXPORT_LIMIT, ge=1, le=20000),
):
    return export_products_csv(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        category_path=category_path,
        selected_category_paths=selected_category_paths,
        keyword=keyword,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        review_growth_7d_min=review_growth_7d_min,
        review_growth_14d_min=review_growth_14d_min,
        rating_growth_7d_min=rating_growth_7d_min,
        rating_growth_14d_min=rating_growth_14d_min,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        sort=sort,
        limit=limit,
    )


@app.get("/api/products/{platform}/{product_id}")
def product_detail(platform: str, product_id: str):
    cached = _read_payload_cache("product-detail", platform, product_id)
    if cached is not None:
        return cached

    conn = connect_db()
    try:
        summary = fetch_one_from_conn(
            conn,
            """
            SELECT
                {columns}
            FROM {summary_table} summary
            WHERE summary.platform = ? AND summary.product_id = ?
            """.format(columns=PRODUCT_SELECT_COLUMNS, summary_table=WEB_PRODUCT_SUMMARY_TABLE),
            (platform, product_id),
        )

        if not summary:
            raise HTTPException(status_code=404, detail=f"product not found: {platform}/{product_id}")

        latest_observation = fetch_one_from_conn(
            conn,
            """
            SELECT
                oe.*,
                pi.last_public_signals_json
            FROM observation_events oe
            LEFT JOIN product_identity pi
              ON pi.platform = oe.platform
             AND pi.product_id = oe.product_id
            WHERE oe.platform = ? AND oe.product_id = ?
            ORDER BY oe.scraped_at DESC, oe.id DESC
            LIMIT 1
            """,
            (platform, product_id),
        )

        category_paths = fetch_all_from_conn(
            conn,
            """
            SELECT
                category_path,
                category_name,
                first_seen,
                last_seen
            FROM product_category_membership
            WHERE platform = ? AND product_id = ? AND category_path LIKE ?
            ORDER BY LENGTH(category_path) ASC, category_path ASC
            """,
            (platform, product_id, "Home > %"),
        )

        keyword_paths = fetch_all_from_conn(
            conn,
            """
            SELECT
                keyword,
                first_seen,
                last_seen
            FROM product_keyword_membership
            WHERE platform = ? AND product_id = ?
            ORDER BY keyword ASC
            """,
            (platform, product_id),
        )

        keyword_rankings = fetch_all_from_conn(
            conn,
            """
            SELECT
                keyword,
                rank_position,
                rank_type,
                observed_at,
                source_platform
            FROM product_keyword_rank_events
            WHERE platform = ? AND product_id = ?
            ORDER BY observed_at DESC, rank_position ASC, keyword ASC
            LIMIT 200
            """,
            (platform, product_id),
        )

        source_coverage = fetch_all_from_conn(
            conn,
            """
            SELECT
                source_type,
                source_value,
                COUNT(*) AS observation_count,
                MIN(scraped_at) AS first_observed_at,
                MAX(scraped_at) AS last_observed_at
            FROM observation_events
            WHERE platform = ? AND product_id = ?
            GROUP BY source_type, source_value
            ORDER BY last_observed_at DESC
            """,
            (platform, product_id),
        )

        signal_bundle = resolve_effective_signal_payload(summary, latest_observation or {})

        primary_category_path = (
            summary.get("latest_observed_category_path")
            or summary.get("latest_category_path")
            or (category_paths[0]["category_path"] if category_paths else "")
        )
        category_summary = {}
        if primary_category_path:
            try:
                category_summary = fetch_category_overview(primary_category_path)
            except HTTPException:
                category_summary = {}
        category_context_levels, effective_category_context = build_category_context_levels(
            [item["category_path"] for item in category_paths]
        )
        keyword_ranking_timeline = build_keyword_ranking_timeline(platform, product_id, conn=conn)
        signal_timeline = build_signal_timeline(platform, product_id, conn=conn)
    finally:
        conn.close()

    payload = {
        "summary": summary,
        "latest_observation": latest_observation,
        "category_paths": category_paths,
        "keywords": keyword_paths,
        "keyword_rankings": keyword_rankings,
        "keyword_ranking_timeline": keyword_ranking_timeline,
        "source_coverage": source_coverage,
        "signals": signal_bundle,
        "signal_timeline": signal_timeline,
        "primary_category_path": primary_category_path,
        "category_summary": category_summary,
        "category_context_levels": category_context_levels,
        "effective_category_context": effective_category_context,
    }
    return _write_payload_cache("product-detail", platform, product_id, value=payload)


@app.get("/api/products/{platform}/{product_id}/history")
def product_history(
    platform: str,
    product_id: str,
    limit: int = Query(default=120, ge=1, le=500),
):
    rows = fetch_all(
        f"""
        SELECT
            source_type,
            source_value,
            scraped_at,
            price,
            original_price,
            rating,
            review_count,
            search_rank,
            bsr_rank,
            {build_visible_bsr_expr(bsr_expr='bsr_rank', ranking_signal_expr='ranking_signal_text')} AS visible_bsr_rank,
            delivery_type,
            is_ad,
            sold_recently_text,
            stock_signal_text,
            lowest_price_signal_text,
            ranking_signal_text,
            delivery_eta_signal_text,
            category_path
        FROM observation_events
        WHERE platform = ? AND product_id = ?
        ORDER BY scraped_at DESC, id DESC
        LIMIT ?
        """,
        (platform, product_id, limit),
    )
    rows.reverse()
    return {"items": rows}


@app.get("/api/session")
def ui_session(request: Request):
    return resolve_ui_session(request)


@app.get("/api/runs/summary")
def runs_summary():
    recent_imports = fetch_all(
        """
        SELECT
            source_label,
            source_scope,
            imported_at,
            source_product_count,
            source_observation_count,
            source_keyword_count
        FROM source_databases
        ORDER BY imported_at DESC
        LIMIT 20
        """
    )

    keyword_runs = fetch_all(
        """
        SELECT
            run_type,
            trigger_mode,
            seed_keyword,
            snapshot_id,
            status,
            keyword_count,
            started_at,
            finished_at,
            metadata_json
        FROM keyword_runs_log
        ORDER BY started_at DESC, id DESC
        LIMIT 20
        """
    )

    health_overview = fetch_one(
        """
        SELECT
            (SELECT COUNT(*) FROM source_databases WHERE source_scope = 'category_stage') AS category_source_db_count,
            (SELECT COUNT(*) FROM source_databases WHERE source_scope = 'keyword_stage') AS keyword_source_db_count,
            (SELECT MAX(imported_at) FROM source_databases) AS last_imported_at,
            (SELECT COUNT(*) FROM keyword_runs_log WHERE status IN ('queued', 'pending', 'running', 'leased', 'retrying')) AS active_keyword_run_count,
            (SELECT COUNT(*) FROM keyword_runs_log WHERE status IN ('queued', 'pending', 'running', 'leased', 'retrying')) AS incomplete_keyword_run_count
        """
    )
    with closing(connect_ops_store()) as store:
        active_keyword_task_count = 0
        for status_name in ("running", "leased", "pending", "retrying"):
            active_keyword_task_count += len(store.list_tasks(status=status_name, worker_type="keyword", limit=20))
    if health_overview is None:
        health_overview = {}
    health_overview = dict(health_overview)
    freshness = compute_sync_visibility_payload()
    watchdog = _build_watchdog_summary_payload()
    keyword_quality = _summarize_keyword_quality(keyword_runs)
    active_run_row_count = max(
        int(health_overview.get("active_keyword_run_count") or 0),
        active_keyword_task_count,
    )
    incomplete_run_row_count = max(
        int(health_overview.get("incomplete_keyword_run_count") or 0),
        active_keyword_task_count,
    )
    operator_active_snapshot_count = int(keyword_quality.get("active_snapshot_count") or 0)
    health_overview["active_keyword_run_row_count"] = active_run_row_count
    health_overview["incomplete_keyword_run_row_count"] = incomplete_run_row_count
    health_overview["active_keyword_run_count"] = operator_active_snapshot_count
    health_overview["incomplete_keyword_run_count"] = operator_active_snapshot_count

    return {
        "health": health_overview,
        "recent_imports": recent_imports,
        "keyword_runs": keyword_runs,
        "freshness": freshness,
        "watchdog": watchdog,
        "keyword_quality": keyword_quality,
    }


@app.get("/api/keywords/warehouse-health")
def keywords_warehouse_health(
    import_limit: int = Query(default=10, ge=1, le=100),
    run_limit: int = Query(default=20, ge=1, le=100),
):
    overview_row = fetch_one(
        """
        SELECT
            (SELECT COUNT(*) FROM source_databases WHERE source_scope = 'keyword_stage') AS keyword_source_db_count,
            (SELECT COUNT(*) FROM keyword_catalog) AS keyword_catalog_count,
            (SELECT COUNT(*) FROM keyword_metric_snapshots) AS keyword_metric_snapshot_count,
            (SELECT COUNT(*) FROM keyword_runs_log) AS keyword_run_count,
            (SELECT COUNT(*) FROM keyword_runs_log WHERE status IN ('queued', 'pending', 'running', 'leased', 'retrying')) AS active_keyword_run_count,
            (SELECT COUNT(*) FROM keyword_runs_log WHERE status IN ('queued', 'pending', 'running', 'leased', 'retrying')) AS incomplete_keyword_run_count,
            (SELECT MAX(imported_at) FROM source_databases WHERE source_scope = 'keyword_stage') AS last_keyword_imported_at,
            (SELECT MAX(started_at) FROM keyword_runs_log) AS last_keyword_run_started_at,
            (SELECT MAX(finished_at) FROM keyword_runs_log) AS last_keyword_run_finished_at
        """
    )
    status_rows = fetch_all(
        """
        SELECT status, COUNT(*) AS count
        FROM keyword_runs_log
        GROUP BY status
        ORDER BY count DESC, status ASC
        """
    )
    recent_imports = fetch_all(
        """
        SELECT
            source_label,
            imported_at,
            source_keyword_count,
            source_observation_count
        FROM source_databases
        WHERE source_scope = 'keyword_stage'
        ORDER BY imported_at DESC, source_label DESC
        LIMIT ?
        """,
        (import_limit,),
    )
    recent_runs = fetch_all(
        """
        SELECT
            run_type,
            trigger_mode,
            seed_keyword,
            snapshot_id,
            status,
            keyword_count,
            started_at,
            finished_at,
            metadata_json
        FROM keyword_runs_log
        ORDER BY started_at DESC, id DESC
        LIMIT ?
        """,
        (run_limit,),
    )
    return build_keyword_warehouse_health_payload(
        overview_row=overview_row,
        status_rows=status_rows,
        recent_imports=recent_imports,
        recent_runs=recent_runs,
    )


def export_products_csv(
    *,
    q: str = "",
    market: str = "",
    platform: str = "",
    source_scope: str = "",
    category_path: str = "",
    selected_category_paths: list[str] | None = None,
    keyword: str = "",
    delivery_type: str = "",
    is_ad: int | None = None,
    tab: str = "ranked",
    bsr_min: int | None = None,
    bsr_max: int | None = None,
    review_min: int | None = None,
    review_max: int | None = None,
    rating_min: float | None = None,
    rating_max: float | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    sales_min: int | None = None,
    sales_max: int | None = None,
    inventory_min: int | None = None,
    inventory_max: int | None = None,
    review_growth_7d_min: int | None = None,
    review_growth_14d_min: int | None = None,
    rating_growth_7d_min: float | None = None,
    rating_growth_14d_min: float | None = None,
    has_sold_signal: bool | None = None,
    has_stock_signal: bool | None = None,
    has_lowest_price_signal: bool | None = None,
    signal_tags: str = "",
    signal_text: str = "",
    sort: str = "sales_desc",
    limit: int = DEFAULT_EXPORT_LIMIT,
) -> Response:
    rows = fetch_products_rows(
        q=q,
        market=market,
        platform=platform,
        source_scope=source_scope,
        category_path=category_path,
        selected_category_paths=selected_category_paths,
        keyword=keyword,
        delivery_type=delivery_type,
        is_ad=is_ad,
        tab=tab,
        bsr_min=bsr_min,
        bsr_max=bsr_max,
        review_min=review_min,
        review_max=review_max,
        rating_min=rating_min,
        rating_max=rating_max,
        price_min=price_min,
        price_max=price_max,
        sales_min=sales_min,
        sales_max=sales_max,
        inventory_min=inventory_min,
        inventory_max=inventory_max,
        review_growth_7d_min=review_growth_7d_min,
        review_growth_14d_min=review_growth_14d_min,
        rating_growth_7d_min=rating_growth_7d_min,
        rating_growth_14d_min=rating_growth_14d_min,
        has_sold_signal=has_sold_signal,
        has_stock_signal=has_stock_signal,
        has_lowest_price_signal=has_lowest_price_signal,
        signal_tags=signal_tags,
        signal_text=signal_text,
        sort=sort,
        limit=limit,
        offset=0,
    )

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    header = [
        "platform",
        "product_id",
        "title",
        "brand",
        "seller_name",
        "latest_price",
        "latest_original_price",
        "latest_currency",
        "latest_rating",
        "latest_review_count",
        "monthly_sales_estimate",
        "inventory_left_estimate",
        "inventory_signal_bucket",
        "review_count_growth_7d",
        "review_count_growth_14d",
        "rating_growth_7d",
        "rating_growth_14d",
        "latest_visible_bsr_rank",
        "latest_signal_count",
        "latest_delivery_type",
        "latest_delivery_eta_signal_text",
        "latest_is_ad",
        "latest_observed_category_path",
        "latest_source_type",
        "latest_source_value",
        "latest_observed_at",
        "product_url",
    ]
    writer.writerow(header)
    for row in rows:
        writer.writerow([neutralize_csv_cell(row.get(field, "")) for field in header])

    content = buffer.getvalue()
    filename_parts = ["noon_web_export"]
    if category_path:
        filename_parts.append("category")
    elif keyword:
        filename_parts.append("keyword")
    else:
        filename_parts.append("products")
    filename = "_".join(filename_parts) + ".csv"
    return Response(
        content="\ufeff" + content,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/keyword/{keyword}/history")
def keyword_history(
    keyword: str,
    limit: int = Query(default=60, ge=1, le=300),
):
    rows = fetch_all(
        """
        SELECT
            analyzed_at,
            grade,
            rank,
            total_score,
            demand_index,
            competition_density,
            supply_gap_ratio,
            margin_war_pct,
            margin_peace_pct
        FROM keyword_metric_snapshots
        WHERE keyword = ?
        ORDER BY analyzed_at DESC, id DESC
        LIMIT ?
        """,
        (keyword, limit),
    )
    rows.reverse()
    return {"items": rows}


@app.get("/api/overview")
def overview():
    payload = compute_dashboard_payload()
    return {
        **payload["overview"],
        "delivery_breakdown": payload["delivery_breakdown"],
    }


@app.get("/api/source-coverage")
def source_coverage():
    return compute_dashboard_payload()["source_coverage"]


@app.get("/api/category-options")
def category_options(limit: int = Query(default=200, ge=1, le=1000)):
    rows = fetch_canonical_category_counts()[:limit]
    return {"items": rows}


@app.get("/api/category-leaders")
def category_leaders(limit: int = Query(default=12, ge=1, le=100)):
    return {"items": compute_dashboard_payload()["priority_categories"][:limit]}


@app.get("/api/keywords")
def keywords(limit: int = Query(default=50, ge=1, le=200)):
    return {"items": keywords_summary(limit=limit)["items"]}


@app.get("/api/keyword-runs")
def keyword_runs(limit: int = Query(default=20, ge=1, le=100)):
    return {"items": runs_summary()["keyword_runs"][:limit]}
