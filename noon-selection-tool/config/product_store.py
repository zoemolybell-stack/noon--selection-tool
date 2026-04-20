"""
Local SQLite store for crawl history and public page signals.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from db.config import DatabaseConfig, get_product_store_database_config

logger = logging.getLogger(__name__)


class ProductStore:
    """Manage product master data and crawl observations."""

    def __init__(self, db_path: Path | str):
        self.database_config = self._resolve_database_config(db_path)
        self.db_path = self.database_config.sqlite_path
        self._last_keyword_order = 0
        self.conn = self._connect()
        self._init_tables()

    def _resolve_database_config(self, db_path: Path | str) -> DatabaseConfig:
        raw_value = str(db_path).strip()
        if raw_value.lower().startswith(("postgres://", "postgresql://", "postgresql+psycopg://")):
            return DatabaseConfig(backend="postgres", source_env="explicit", dsn=raw_value)
        return get_product_store_database_config(Path(raw_value).expanduser())

    def _connect(self):
        if self.database_config.is_sqlite:
            db_path = self.database_config.sqlite_path_or_raise("ProductStore")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            return conn

        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("psycopg is required for PostgreSQL ProductStore backend") from exc

        return psycopg.connect(
            self.database_config.dsn,
            row_factory=dict_row,
            autocommit=True,
        )

    @property
    def backend(self) -> str:
        return self.database_config.backend

    @property
    def is_sqlite(self) -> bool:
        return self.database_config.is_sqlite

    @property
    def is_postgres(self) -> bool:
        return self.database_config.is_postgres

    def _adapt_sql(self, sql: str) -> str:
        if self.is_sqlite:
            return sql
        return sql.replace("?", "%s")

    def _execute(self, sql: str, params: Iterable[Any] | None = None):
        cursor = self.conn.cursor()
        cursor.execute(self._adapt_sql(sql), tuple(params or ()))
        return cursor

    def _fetchone(self, sql: str, params: Iterable[Any] | None = None):
        return self._execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: Iterable[Any] | None = None):
        return self._execute(sql, params).fetchall()

    def _commit(self):
        if self.is_sqlite:
            self.conn.commit()

    def _normalize_platform(self, platform: Any) -> str:
        value = str(platform or "noon").strip().lower()
        return value or "noon"

    def _build_product_key(self, product_id: Any, platform: Any) -> str:
        raw_product_id = str(product_id or "").strip()
        if not raw_product_id:
            raise ValueError("product_id is required")
        return f"{self._normalize_platform(platform)}::{raw_product_id}"

    def _create_table(self, sql: str):
        if self.is_sqlite:
            self._execute(sql)
            return

        postgres_sql = (
            sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
            .replace("INTEGER PRIMARY KEY", "BIGSERIAL PRIMARY KEY")
        )
        self._execute(postgres_sql)

    def _sqlite_table_exists(self, table: str) -> bool:
        row = self._fetchone(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        )
        return row is not None

    def _sqlite_table_columns(self, table: str) -> set[str]:
        if not self._sqlite_table_exists(table):
            return set()
        return {str(row["name"]) for row in self._fetchall(f"PRAGMA table_info({table})")}

    def _create_product_tables(self) -> None:
        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_key TEXT NOT NULL UNIQUE,
                product_id TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'noon',
                title TEXT,
                brand TEXT,
                seller_name TEXT,
                category_path TEXT,
                product_url TEXT,
                image_url TEXT,
                last_public_signals_json TEXT,
                first_seen TEXT,
                last_seen TEXT,
                is_active INTEGER DEFAULT 1,
                UNIQUE(platform, product_id)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_key TEXT NOT NULL,
                product_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                scraped_at TEXT NOT NULL,
                price REAL,
                original_price REAL,
                currency TEXT DEFAULT 'SAR',
                FOREIGN KEY (product_key) REFERENCES products(product_key)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS rank_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_key TEXT NOT NULL,
                product_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                scraped_at TEXT NOT NULL,
                keyword TEXT,
                search_rank INTEGER,
                bsr_rank INTEGER,
                FOREIGN KEY (product_key) REFERENCES products(product_key)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS sales_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_key TEXT NOT NULL,
                product_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                scraped_at TEXT NOT NULL,
                units_sold INTEGER,
                revenue REAL,
                review_count INTEGER,
                rating REAL,
                sold_recently_count INTEGER,
                sold_recently_text TEXT,
                stock_left_count INTEGER,
                stock_signal_text TEXT,
                signal_source TEXT,
                FOREIGN KEY (product_key) REFERENCES products(product_key)
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS crawl_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_key TEXT NOT NULL,
                product_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                snapshot_id TEXT,
                source_type TEXT NOT NULL,
                source_value TEXT,
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
                FOREIGN KEY (product_key) REFERENCES products(product_key)
            )
            """
        )

    def _migrate_sqlite_product_identity_schema_if_needed(self) -> None:
        if not self.is_sqlite or not self._sqlite_table_exists("products"):
            return
        product_columns = self._sqlite_table_columns("products")
        if "product_key" in product_columns:
            return

        logger.info("migrating product store identity schema to composite platform/product_id")
        legacy_suffix = "__legacy_identity"
        legacy_tables = ["crawl_observations", "sales_snapshot", "rank_history", "price_history", "products"]
        self._execute("PRAGMA foreign_keys=OFF")
        for table in legacy_tables:
            if self._sqlite_table_exists(table):
                self._execute(f"ALTER TABLE {table} RENAME TO {table}{legacy_suffix}")
        self._create_product_tables()

        self._execute(
            f"""
            INSERT INTO products (
                product_key, product_id, platform, title, brand, seller_name, category_path,
                product_url, image_url, last_public_signals_json, first_seen, last_seen, is_active
            )
            SELECT
                COALESCE(NULLIF(platform, ''), 'noon') || '::' || product_id,
                product_id,
                COALESCE(NULLIF(platform, ''), 'noon'),
                title,
                brand,
                seller_name,
                category_path,
                product_url,
                image_url,
                last_public_signals_json,
                first_seen,
                last_seen,
                is_active
            FROM products{legacy_suffix}
            """
        )

        self._execute(
            f"""
            INSERT INTO price_history (
                product_key, product_id, platform, scraped_at, price, original_price, currency
            )
            SELECT
                COALESCE(NULLIF(p.platform, ''), 'noon') || '::' || ph.product_id,
                ph.product_id,
                COALESCE(NULLIF(p.platform, ''), 'noon'),
                ph.scraped_at,
                ph.price,
                ph.original_price,
                COALESCE(ph.currency, 'SAR')
            FROM price_history{legacy_suffix} ph
            LEFT JOIN products{legacy_suffix} p ON p.product_id = ph.product_id
            """
        )

        self._execute(
            f"""
            INSERT INTO rank_history (
                product_key, product_id, platform, scraped_at, keyword, search_rank, bsr_rank
            )
            SELECT
                COALESCE(NULLIF(p.platform, ''), 'noon') || '::' || rh.product_id,
                rh.product_id,
                COALESCE(NULLIF(p.platform, ''), 'noon'),
                rh.scraped_at,
                rh.keyword,
                rh.search_rank,
                rh.bsr_rank
            FROM rank_history{legacy_suffix} rh
            LEFT JOIN products{legacy_suffix} p ON p.product_id = rh.product_id
            """
        )

        self._execute(
            f"""
            INSERT INTO sales_snapshot (
                product_key, product_id, platform, scraped_at, units_sold, revenue, review_count, rating,
                sold_recently_count, sold_recently_text, stock_left_count, stock_signal_text, signal_source
            )
            SELECT
                COALESCE(NULLIF(p.platform, ''), 'noon') || '::' || ss.product_id,
                ss.product_id,
                COALESCE(NULLIF(p.platform, ''), 'noon'),
                ss.scraped_at,
                ss.units_sold,
                ss.revenue,
                ss.review_count,
                ss.rating,
                ss.sold_recently_count,
                ss.sold_recently_text,
                ss.stock_left_count,
                ss.stock_signal_text,
                ss.signal_source
            FROM sales_snapshot{legacy_suffix} ss
            LEFT JOIN products{legacy_suffix} p ON p.product_id = ss.product_id
            """
        )

        self._execute(
            f"""
            INSERT INTO crawl_observations (
                product_key, product_id, platform, snapshot_id, source_type, source_value, category_name,
                scraped_at, price, original_price, currency, rating, review_count, search_rank, bsr_rank,
                seller_name, delivery_type, is_express, is_bestseller, is_ad, sold_recently_count,
                sold_recently_text, stock_left_count, stock_signal_text, delivery_eta_signal_text,
                lowest_price_signal_text, ranking_signal_text, badge_texts_json, public_signals_json,
                category_path, product_url
            )
            SELECT
                COALESCE(NULLIF(co.platform, ''), 'noon') || '::' || co.product_id,
                co.product_id,
                COALESCE(NULLIF(co.platform, ''), 'noon'),
                co.snapshot_id,
                co.source_type,
                co.source_value,
                co.category_name,
                co.scraped_at,
                co.price,
                co.original_price,
                co.currency,
                co.rating,
                co.review_count,
                co.search_rank,
                co.bsr_rank,
                co.seller_name,
                co.delivery_type,
                co.is_express,
                co.is_bestseller,
                co.is_ad,
                co.sold_recently_count,
                co.sold_recently_text,
                co.stock_left_count,
                co.stock_signal_text,
                co.delivery_eta_signal_text,
                co.lowest_price_signal_text,
                co.ranking_signal_text,
                co.badge_texts_json,
                co.public_signals_json,
                co.category_path,
                co.product_url
            FROM crawl_observations{legacy_suffix} co
            """
        )

        for table in legacy_tables:
            self._execute(f"DROP TABLE IF EXISTS {table}{legacy_suffix}")
        self._execute("PRAGMA foreign_keys=ON")
        self._commit()

    def _init_tables(self):
        if self.is_sqlite:
            self._execute("PRAGMA journal_mode=WAL")
            self._execute("PRAGMA busy_timeout=30000")
            self._execute("PRAGMA foreign_keys=ON")
            self._migrate_sqlite_product_identity_schema_if_needed()

        self._create_product_tables()

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keywords (
                keyword TEXT PRIMARY KEY,
                display_keyword TEXT,
                status TEXT DEFAULT 'active',
                tracking_mode TEXT DEFAULT 'tracked',
                source_type TEXT DEFAULT 'manual',
                source_platform TEXT DEFAULT '',
                priority INTEGER DEFAULT 100,
                notes TEXT DEFAULT '',
                metadata_json TEXT,
                first_seen TEXT,
                last_seen TEXT,
                last_crawled_at TEXT,
                last_analyzed_at TEXT,
                last_expanded_at TEXT,
                last_snapshot_id TEXT
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keyword_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_keyword TEXT NOT NULL,
                child_keyword TEXT NOT NULL,
                source_platform TEXT DEFAULT '',
                source_type TEXT DEFAULT 'autocomplete',
                run_id INTEGER,
                discovered_at TEXT NOT NULL
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keyword_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_type TEXT NOT NULL,
                trigger_mode TEXT DEFAULT 'manual',
                seed_keyword TEXT,
                snapshot_id TEXT,
                platforms_json TEXT,
                status TEXT DEFAULT 'running',
                keyword_count INTEGER DEFAULT 0,
                metadata_json TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT
            )
            """
        )

        self._create_table(
            """
            CREATE TABLE IF NOT EXISTS keyword_metrics_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                payload_json TEXT
            )
            """
        )

        self._ensure_column("products", "seller_name", "TEXT")
        self._ensure_column("products", "product_key", "TEXT")
        self._ensure_column("products", "image_url", "TEXT")
        self._ensure_column("products", "last_public_signals_json", "TEXT")
        self._ensure_column("price_history", "product_key", "TEXT")
        self._ensure_column("price_history", "platform", "TEXT")
        self._ensure_column("rank_history", "product_key", "TEXT")
        self._ensure_column("rank_history", "platform", "TEXT")
        self._ensure_column("sales_snapshot", "product_key", "TEXT")
        self._ensure_column("sales_snapshot", "platform", "TEXT")
        self._ensure_column("crawl_observations", "product_key", "TEXT")
        self._ensure_column("sales_snapshot", "sold_recently_count", "INTEGER")
        self._ensure_column("sales_snapshot", "sold_recently_text", "TEXT")
        self._ensure_column("sales_snapshot", "stock_left_count", "INTEGER")
        self._ensure_column("sales_snapshot", "stock_signal_text", "TEXT")
        self._ensure_column("sales_snapshot", "signal_source", "TEXT")
        self._ensure_column("crawl_observations", "delivery_eta_signal_text", "TEXT")
        self._ensure_column("crawl_observations", "lowest_price_signal_text", "TEXT")
        self._ensure_column("keywords", "tracking_mode", "TEXT DEFAULT 'tracked'")
        self._ensure_column("keywords", "last_expanded_at", "TEXT")
        self._ensure_column("keywords", "keyword_order", "BIGINT")
        self._backfill_product_identity_columns()
        self._backfill_keyword_order()

        self._execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_products_platform_product_id ON products(platform, product_id)"
        )
        self._execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_products_product_key ON products(product_key)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_price_product ON price_history(product_key, scraped_at DESC)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_rank_product ON rank_history(product_key, scraped_at DESC)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_sales_product ON sales_snapshot(product_key, scraped_at DESC)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand)"
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_observation_lookup
            ON crawl_observations(product_key, scraped_at DESC, source_type)
            """
        )
        self._execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_observation
            ON crawl_observations(product_key, source_type, source_value, scraped_at)
            """
        )
        self._execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_keyword_edge
            ON keyword_edges(parent_keyword, child_keyword, source_platform, source_type)
            """
        )
        self._execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_keyword_metric_snapshot
            ON keyword_metrics_snapshots(keyword, snapshot_id)
            """
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_keywords_status ON keywords(status, priority, last_seen DESC)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_keywords_refresh ON keywords(status, tracking_mode, priority, last_crawled_at, last_expanded_at)"
        )
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_keyword_runs_type ON keyword_runs(run_type, started_at DESC)"
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_keyword_metrics_lookup
            ON keyword_metrics_snapshots(keyword, analyzed_at DESC)
            """
        )

        self._commit()
        logger.info(
            "product store initialized: backend=%s target=%s",
            self.backend,
            self.db_path or self.database_config.dsn,
        )

    def _ensure_column(self, table: str, column: str, column_type: str):
        if self.is_sqlite:
            rows = self._fetchall(f"PRAGMA table_info({table})")
            existing = {row["name"] for row in rows}
        else:
            rows = self._fetchall(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = %s
                """,
                (table,),
            )
            existing = {row["column_name"] for row in rows}
        if column not in existing:
            self._execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
            self._commit()

    def _backfill_product_identity_columns(self):
        self._execute(
            """
            UPDATE products
            SET platform = COALESCE(NULLIF(platform, ''), 'noon'),
                product_key = COALESCE(product_key, COALESCE(NULLIF(platform, ''), 'noon') || '::' || product_id)
            WHERE product_key IS NULL OR product_key = '' OR platform IS NULL OR platform = ''
            """
        )
        self._execute(
            """
            UPDATE price_history
            SET platform = COALESCE(NULLIF(platform, ''), 'noon'),
                product_key = COALESCE(
                    product_key,
                    COALESCE(NULLIF(platform, ''), 'noon') || '::' || product_id
                )
            WHERE product_key IS NULL OR product_key = '' OR platform IS NULL OR platform = ''
            """
        )
        self._execute(
            """
            UPDATE rank_history
            SET platform = COALESCE(NULLIF(platform, ''), 'noon'),
                product_key = COALESCE(
                    product_key,
                    COALESCE(NULLIF(platform, ''), 'noon') || '::' || product_id
                )
            WHERE product_key IS NULL OR product_key = '' OR platform IS NULL OR platform = ''
            """
        )
        self._execute(
            """
            UPDATE sales_snapshot
            SET platform = COALESCE(NULLIF(platform, ''), 'noon'),
                product_key = COALESCE(
                    product_key,
                    COALESCE(NULLIF(platform, ''), 'noon') || '::' || product_id
                )
            WHERE product_key IS NULL OR product_key = '' OR platform IS NULL OR platform = ''
            """
        )
        self._execute(
            """
            UPDATE crawl_observations
            SET platform = COALESCE(NULLIF(platform, ''), 'noon'),
                product_key = COALESCE(
                    product_key,
                    COALESCE(NULLIF(platform, ''), 'noon') || '::' || product_id
                )
            WHERE product_key IS NULL OR product_key = '' OR platform IS NULL OR platform = ''
            """
        )
        self._commit()

    def _safe_int(self, value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _safe_float(self, value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _next_keyword_order(self) -> int:
        now_ns = time.time_ns()
        if now_ns <= self._last_keyword_order:
            now_ns = self._last_keyword_order + 1
        self._last_keyword_order = now_ns
        return now_ns

    def _backfill_keyword_order(self):
        if self.is_sqlite:
            self._execute(
                """
                UPDATE keywords
                SET keyword_order = rowid
                WHERE keyword_order IS NULL
                """
            )
            self._commit()
            return

        self._execute(
            """
            WITH ordered AS (
                SELECT
                    keyword,
                    ROW_NUMBER() OVER (
                        ORDER BY
                            CASE WHEN first_seen IS NULL OR first_seen = '' THEN 1 ELSE 0 END ASC,
                            first_seen ASC,
                            keyword ASC
                    ) AS ordering
                FROM keywords
                WHERE keyword_order IS NULL
            )
            UPDATE keywords AS target
            SET keyword_order = ordered.ordering
            FROM ordered
            WHERE target.keyword = ordered.keyword
            """
        )
        self._commit()

    def _normalize_keyword(self, value: Any) -> str:
        if value is None:
            return ""
        return " ".join(str(value).strip().lower().split())

    def _safe_text(self, value: Any) -> str:
        if value is None:
            return ""
        return " ".join(str(value).strip().split())

    def _json_dumps(self, payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False, default=str)

    def _resolve_product_identity(self, product_id: str, platform: Optional[str] = None) -> Optional[Dict[str, Any]]:
        raw_product_id = str(product_id or "").strip()
        if not raw_product_id:
            return None
        if platform:
            normalized_platform = self._normalize_platform(platform)
            row = self._fetchone(
                """
                SELECT product_key, product_id, platform
                FROM products
                WHERE product_id = ? AND platform = ?
                LIMIT 1
                """,
                (raw_product_id, normalized_platform),
            )
        else:
            row = self._fetchone(
                """
                SELECT product_key, product_id, platform
                FROM products
                WHERE product_id = ?
                ORDER BY last_seen DESC, id DESC
                LIMIT 1
                """,
                (raw_product_id,),
            )
        if row is None:
            return None
        return {
            "product_key": str(row["product_key"]),
            "product_id": str(row["product_id"]),
            "platform": str(row["platform"]),
        }

    def _build_public_signals(self, product: Dict[str, Any]) -> Dict[str, Any]:
        badge_texts = product.get("badge_texts") or []
        if isinstance(badge_texts, str):
            badge_texts = [badge_texts] if badge_texts else []

        return {
            "signal_source": product.get("signal_source", "public_page"),
            "delivery_type": product.get("delivery_type", ""),
            "delivery_eta_signal_text": product.get("delivery_eta_signal_text", ""),
            "sold_recently_text": product.get("sold_recently_text", ""),
            "sold_recently_count": self._safe_int(product.get("sold_recently")),
            "stock_signal_text": product.get("stock_signal_text", ""),
            "stock_left_count": self._safe_int(product.get("stock_left_count")),
            "lowest_price_signal_text": product.get("lowest_price_signal_text", ""),
            "ranking_signal_text": product.get("ranking_signal_text", ""),
            "badge_texts": badge_texts,
            "is_ad": bool(product.get("is_ad")),
            "is_express": bool(product.get("is_express")),
            "is_bestseller": bool(product.get("is_bestseller")),
        }

    def _observation_exists(
        self,
        product_id: str,
        platform: str,
        source_type: str,
        source_value: str,
        scraped_at: str,
    ) -> bool:
        product_key = self._build_product_key(product_id, platform)
        row = self._fetchone(
            """
            SELECT 1
            FROM crawl_observations
            WHERE product_key = ? AND source_type = ? AND source_value = ? AND scraped_at = ?
            LIMIT 1
            """,
            (product_key, source_type, source_value, scraped_at),
        )
        return row is not None

    def upsert_product(self, product_data: Dict[str, Any], *, commit: bool = True) -> bool:
        now = datetime.now().isoformat()
        product_id = self._safe_text(product_data.get("product_id"))
        if not product_id:
            logger.error("product data missing product_id")
            return False

        platform = self._normalize_platform(product_data.get("platform", "noon"))
        product_key = self._build_product_key(product_id, platform)
        payload = {
            "product_key": product_key,
            "product_id": product_id,
            "platform": platform,
            "title": product_data.get("title", ""),
            "brand": product_data.get("brand"),
            "seller_name": product_data.get("seller_name"),
            "category_path": product_data.get("category_path"),
            "product_url": product_data.get("product_url", ""),
            "image_url": product_data.get("image_url"),
            "last_public_signals_json": product_data.get("last_public_signals_json"),
            "first_seen": now,
            "last_seen": now,
        }

        existing = self._resolve_product_identity(product_id, platform=platform)
        if existing:
            self._execute(
                """
                UPDATE products
                SET title = ?,
                    brand = COALESCE(?, brand),
                    seller_name = COALESCE(?, seller_name),
                    category_path = COALESCE(?, category_path),
                    product_url = COALESCE(NULLIF(?, ''), product_url),
                    image_url = COALESCE(?, image_url),
                    last_public_signals_json = COALESCE(?, last_public_signals_json),
                    last_seen = ?,
                    is_active = 1
                WHERE product_key = ?
                """,
                (
                    payload["title"],
                    payload["brand"],
                    payload["seller_name"],
                    payload["category_path"],
                    payload["product_url"],
                    payload["image_url"],
                    payload["last_public_signals_json"],
                    payload["last_seen"],
                    payload["product_key"],
                ),
            )
        else:
            self._execute(
                """
                INSERT INTO products (
                    product_key, product_id, platform, title, brand, seller_name, category_path,
                    product_url, image_url, last_public_signals_json, first_seen, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["product_key"],
                    payload["product_id"],
                    payload["platform"],
                    payload["title"],
                    payload["brand"],
                    payload["seller_name"],
                    payload["category_path"],
                    payload["product_url"],
                    payload["image_url"],
                    payload["last_public_signals_json"],
                    payload["first_seen"],
                    payload["last_seen"],
                ),
            )

        if commit:
            self._commit()
        return True

    def add_price_record(
        self,
        product_id: str,
        price: float,
        original_price: Optional[float] = None,
        currency: str = "SAR",
        scraped_at: Optional[str] = None,
        *,
        platform: Optional[str] = None,
        commit: bool = True,
    ) -> bool:
        if price in (None, ""):
            return False

        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return False

        now = scraped_at or datetime.now().isoformat()
        self._execute(
            """
            INSERT INTO price_history (product_key, product_id, platform, scraped_at, price, original_price, currency)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity["product_key"],
                identity["product_id"],
                identity["platform"],
                now,
                round(float(price), 2),
                round(float(original_price), 2) if original_price not in (None, "") else None,
                currency,
            ),
        )
        if commit:
            self._commit()
        return True

    def add_rank_record(
        self,
        product_id: str,
        keyword: Optional[str] = None,
        search_rank: Optional[int] = None,
        bsr_rank: Optional[int] = None,
        scraped_at: Optional[str] = None,
        *,
        platform: Optional[str] = None,
        commit: bool = True,
    ) -> bool:
        if not (search_rank or bsr_rank):
            return False

        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return False

        now = scraped_at or datetime.now().isoformat()
        self._execute(
            """
            INSERT INTO rank_history (product_key, product_id, platform, scraped_at, keyword, search_rank, bsr_rank)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity["product_key"],
                identity["product_id"],
                identity["platform"],
                now,
                keyword,
                search_rank,
                bsr_rank,
            ),
        )
        if commit:
            self._commit()
        return True

    def add_sales_snapshot(
        self,
        product_id: str,
        units_sold: Optional[int] = None,
        revenue: Optional[float] = None,
        review_count: Optional[int] = None,
        rating: Optional[float] = None,
        sold_recently_count: Optional[int] = None,
        sold_recently_text: str = "",
        stock_left_count: Optional[int] = None,
        stock_signal_text: str = "",
        signal_source: str = "public_page",
        scraped_at: Optional[str] = None,
        *,
        platform: Optional[str] = None,
        commit: bool = True,
    ) -> bool:
        if not any(
            [
                units_sold,
                revenue,
                review_count,
                rating,
                sold_recently_count,
                sold_recently_text,
                stock_left_count,
                stock_signal_text,
            ]
        ):
            return False

        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return False

        now = scraped_at or datetime.now().isoformat()
        self._execute(
            """
            INSERT INTO sales_snapshot (
                product_key, product_id, platform, scraped_at, units_sold, revenue, review_count, rating,
                sold_recently_count, sold_recently_text, stock_left_count, stock_signal_text, signal_source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity["product_key"],
                identity["product_id"],
                identity["platform"],
                now,
                units_sold,
                revenue,
                review_count,
                rating,
                sold_recently_count,
                sold_recently_text,
                stock_left_count,
                stock_signal_text,
                signal_source,
            ),
        )
        if commit:
            self._commit()
        return True

    def add_crawl_observation(
        self,
        observation: Dict[str, Any],
        *,
        commit: bool = True,
    ) -> bool:
        identity = self._resolve_product_identity(
            observation.get("product_id"),
            platform=observation.get("platform"),
        )
        if identity is None:
            return False

        sql = """
            INSERT INTO crawl_observations (
                product_key, product_id, platform, snapshot_id, source_type, source_value, category_name,
                scraped_at, price, original_price, currency, rating, review_count,
                search_rank, bsr_rank, seller_name, delivery_type, is_express, is_bestseller,
                is_ad, sold_recently_count, sold_recently_text, stock_left_count, stock_signal_text,
                delivery_eta_signal_text, lowest_price_signal_text, ranking_signal_text,
                badge_texts_json, public_signals_json, category_path, product_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            identity["product_key"],
            identity["product_id"],
            identity["platform"],
            observation.get("snapshot_id"),
            observation["source_type"],
            observation.get("source_value"),
            observation.get("category_name"),
            observation["scraped_at"],
            observation.get("price"),
            observation.get("original_price"),
            observation.get("currency", "SAR"),
            observation.get("rating"),
            observation.get("review_count"),
            observation.get("search_rank"),
            observation.get("bsr_rank"),
            observation.get("seller_name"),
            observation.get("delivery_type"),
            int(bool(observation.get("is_express"))),
            int(bool(observation.get("is_bestseller"))),
            int(bool(observation.get("is_ad"))),
            observation.get("sold_recently_count"),
            observation.get("sold_recently_text"),
            observation.get("stock_left_count"),
            observation.get("stock_signal_text"),
            observation.get("delivery_eta_signal_text"),
            observation.get("lowest_price_signal_text"),
            observation.get("ranking_signal_text"),
            observation.get("badge_texts_json"),
            observation.get("public_signals_json"),
            observation.get("category_path"),
            observation.get("product_url"),
        )
        if self.is_postgres:
            sql += " ON CONFLICT DO NOTHING"
        else:
            sql = sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
        self._execute(sql, params)
        if commit:
            self._commit()
        return True

    def persist_products(
        self,
        products: Iterable[Dict[str, Any]],
        *,
        platform: str,
        scraped_at: Optional[str],
        source_type: str,
        source_value: str = "",
        keyword: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        category_name: Optional[str] = None,
    ) -> int:
        count = 0
        observed_at = scraped_at or datetime.now().isoformat()

        for position, product in enumerate(products, start=1):
            product_id = product.get("product_id") or product.get("asin") or product.get("sku")
            if not product_id:
                continue
            if self._observation_exists(product_id, platform, source_type, source_value, observed_at):
                continue

            public_signals = self._build_public_signals(product)
            search_rank = product.get("search_rank")
            if search_rank in (None, 0) and source_type == "category":
                search_rank = position

            upserted = self.upsert_product(
                {
                    "product_id": product_id,
                    "platform": platform,
                    "title": product.get("title", ""),
                    "brand": product.get("brand"),
                    "seller_name": product.get("seller_name"),
                    "category_path": product.get("category_path") or category_name,
                    "product_url": product.get("product_url", ""),
                    "image_url": product.get("image_url"),
                    "last_public_signals_json": json.dumps(public_signals, ensure_ascii=False),
                },
                commit=False,
            )
            if not upserted:
                continue

            price = self._safe_float(product.get("price"))
            if price is not None:
                self.add_price_record(
                    product_id=product_id,
                    price=price,
                    original_price=self._safe_float(product.get("original_price")),
                    currency=product.get("currency", "SAR"),
                    scraped_at=observed_at,
                    platform=platform,
                    commit=False,
                )

            if search_rank or product.get("bsr_rank"):
                self.add_rank_record(
                    product_id=product_id,
                    keyword=keyword,
                    search_rank=search_rank,
                    bsr_rank=self._safe_int(product.get("bsr_rank")),
                    scraped_at=observed_at,
                    platform=platform,
                    commit=False,
                )

            self.add_sales_snapshot(
                product_id=product_id,
                units_sold=self._safe_int(product.get("units_sold")),
                revenue=self._safe_float(product.get("revenue")),
                review_count=self._safe_int(product.get("review_count")),
                rating=self._safe_float(product.get("rating")),
                sold_recently_count=public_signals["sold_recently_count"],
                sold_recently_text=public_signals["sold_recently_text"],
                stock_left_count=public_signals["stock_left_count"],
                stock_signal_text=public_signals["stock_signal_text"],
                signal_source=public_signals["signal_source"],
                scraped_at=observed_at,
                platform=platform,
                commit=False,
            )

            self.add_crawl_observation(
                {
                    "product_id": product_id,
                    "platform": platform,
                    "snapshot_id": snapshot_id,
                    "source_type": source_type,
                    "source_value": source_value,
                    "category_name": category_name,
                    "scraped_at": observed_at,
                    "price": price,
                    "original_price": self._safe_float(product.get("original_price")),
                    "currency": product.get("currency", "SAR"),
                    "rating": self._safe_float(product.get("rating")),
                    "review_count": self._safe_int(product.get("review_count")),
                    "search_rank": search_rank,
                    "bsr_rank": self._safe_int(product.get("bsr_rank")),
                    "seller_name": product.get("seller_name"),
                    "delivery_type": public_signals["delivery_type"],
                    "is_express": bool(product.get("is_express")),
                    "is_bestseller": bool(product.get("is_bestseller")),
                    "is_ad": bool(product.get("is_ad")),
                    "sold_recently_count": public_signals["sold_recently_count"],
                    "sold_recently_text": public_signals["sold_recently_text"],
                    "stock_left_count": public_signals["stock_left_count"],
                    "stock_signal_text": public_signals["stock_signal_text"],
                    "delivery_eta_signal_text": public_signals["delivery_eta_signal_text"],
                    "lowest_price_signal_text": public_signals["lowest_price_signal_text"],
                    "ranking_signal_text": public_signals["ranking_signal_text"],
                    "badge_texts_json": json.dumps(public_signals["badge_texts"], ensure_ascii=False),
                    "public_signals_json": json.dumps(public_signals, ensure_ascii=False),
                    "category_path": product.get("category_path") or category_name,
                    "product_url": product.get("product_url", ""),
                },
                commit=False,
            )

            count += 1

        self._commit()
        return count

    def upsert_keyword(
        self,
        keyword: str,
        *,
        display_keyword: Optional[str] = None,
        status: Optional[str] = "active",
        tracking_mode: Optional[str] = "tracked",
        source_type: Optional[str] = "manual",
        source_platform: Optional[str] = "",
        priority: Optional[int] = 100,
        notes: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        last_crawled_at: Optional[str] = None,
        last_analyzed_at: Optional[str] = None,
        last_expanded_at: Optional[str] = None,
        last_snapshot_id: Optional[str] = None,
        commit: bool = True,
    ) -> bool:
        normalized = self._normalize_keyword(keyword)
        if not normalized:
            return False

        now = datetime.now().isoformat()
        display = self._safe_text(display_keyword or keyword or normalized)
        metadata_json = self._json_dumps(metadata) if metadata is not None else None
        exists = self._fetchone("SELECT keyword FROM keywords WHERE keyword = ?", (normalized,)) is not None

        if exists:
            self._execute(
                """
                UPDATE keywords
                SET display_keyword = COALESCE(NULLIF(?, ''), display_keyword),
                    status = COALESCE(NULLIF(?, ''), status),
                    tracking_mode = COALESCE(NULLIF(?, ''), tracking_mode),
                    source_type = COALESCE(NULLIF(?, ''), source_type),
                    source_platform = COALESCE(NULLIF(?, ''), source_platform),
                    priority = COALESCE(?, priority),
                    keyword_order = COALESCE(keyword_order, ?),
                    notes = COALESCE(?, notes),
                    metadata_json = COALESCE(?, metadata_json),
                    last_seen = ?,
                    last_crawled_at = COALESCE(?, last_crawled_at),
                    last_analyzed_at = COALESCE(?, last_analyzed_at),
                    last_expanded_at = COALESCE(?, last_expanded_at),
                    last_snapshot_id = COALESCE(?, last_snapshot_id)
                WHERE keyword = ?
                """,
                (
                    display,
                    status,
                    tracking_mode,
                    source_type,
                    source_platform,
                    priority,
                    self._next_keyword_order(),
                    notes,
                    metadata_json,
                    now,
                    last_crawled_at,
                    last_analyzed_at,
                    last_expanded_at,
                    last_snapshot_id,
                    normalized,
                ),
            )
        else:
            self._execute(
                """
                INSERT INTO keywords (
                    keyword, display_keyword, status, tracking_mode, source_type, source_platform,
                    priority, keyword_order, notes, metadata_json, first_seen, last_seen,
                    last_crawled_at, last_analyzed_at, last_expanded_at, last_snapshot_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized,
                    display,
                    status or "active",
                    tracking_mode or "tracked",
                    source_type or "manual",
                    source_platform or "",
                    priority if priority is not None else 100,
                    self._next_keyword_order(),
                    notes or "",
                    metadata_json,
                    now,
                    now,
                    last_crawled_at,
                    last_analyzed_at,
                    last_expanded_at,
                    last_snapshot_id,
                ),
            )

        if commit:
            self._commit()
        return True

    def upsert_keywords(
        self,
        keywords: Iterable[str],
        *,
        display_keywords: Optional[Dict[str, str]] = None,
        status: Optional[str] = "active",
        tracking_mode: Optional[str] = "tracked",
        source_type: Optional[str] = "manual",
        source_platform: Optional[str] = "",
        priority: Optional[int] = 100,
        notes: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        last_crawled_at: Optional[str] = None,
        last_analyzed_at: Optional[str] = None,
        last_expanded_at: Optional[str] = None,
        last_snapshot_id: Optional[str] = None,
    ) -> int:
        count = 0
        display_keywords = display_keywords or {}
        for keyword in keywords:
            normalized = self._normalize_keyword(keyword)
            if not normalized:
                continue
            if self.upsert_keyword(
                normalized,
                display_keyword=display_keywords.get(normalized, keyword),
                status=status,
                tracking_mode=tracking_mode,
                source_type=source_type,
                source_platform=source_platform,
                priority=priority,
                notes=notes,
                metadata=metadata,
                last_crawled_at=last_crawled_at,
                last_analyzed_at=last_analyzed_at,
                last_expanded_at=last_expanded_at,
                last_snapshot_id=last_snapshot_id,
                commit=False,
            ):
                count += 1
        self._commit()
        return count

    def list_keywords(
        self,
        *,
        status: Optional[str] = "active",
        tracking_mode: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT keyword, display_keyword, status, tracking_mode, source_type, source_platform,
                   priority, first_seen, last_seen, last_crawled_at, last_analyzed_at,
                   last_expanded_at, metadata_json,
                   last_snapshot_id
            FROM keywords
        """
        params: List[Any] = []
        conditions: List[str] = []
        if status:
            conditions.append("status = ?")
            params.append(status)
        if tracking_mode:
            conditions.append("tracking_mode = ?")
            params.append(tracking_mode)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY priority ASC, COALESCE(last_crawled_at, '') ASC, COALESCE(keyword_order, 9223372036854775807) ASC, keyword ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows: List[Dict[str, Any]] = []
        for row in self._fetchall(query, params):
            payload = dict(row)
            metadata_raw = payload.get("metadata_json")
            if metadata_raw:
                try:
                    payload["metadata"] = json.loads(metadata_raw)
                except Exception:
                    payload["metadata"] = {}
            else:
                payload["metadata"] = {}
            rows.append(payload)
        return rows

    def get_keyword(self, keyword: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_keyword(keyword)
        if not normalized:
            return None

        row = self._fetchone("SELECT * FROM keywords WHERE keyword = ?", (normalized,))
        if row is None:
            return None

        payload = dict(row)
        metadata_raw = payload.get("metadata_json")
        if metadata_raw:
            try:
                payload["metadata"] = json.loads(metadata_raw)
            except Exception:
                payload["metadata"] = {}
        else:
            payload["metadata"] = {}
        return payload

    def get_keywords_for_crawl(
        self,
        *,
        tracking_mode: str = "tracked",
        status: str = "active",
        limit: Optional[int] = None,
        stale_hours: Optional[int] = None,
    ) -> List[str]:
        query = """
            SELECT keyword, display_keyword
            FROM keywords
            WHERE status = ? AND tracking_mode = ?
        """
        params: List[Any] = [status, tracking_mode]

        if stale_hours is not None:
            if self.is_postgres:
                query += """
                    AND (
                        last_crawled_at IS NULL
                        OR last_crawled_at = ''
                        OR CAST(last_crawled_at AS timestamp) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')
                    )
                """
                params.append(int(stale_hours))
            else:
                query += """
                    AND (
                        last_crawled_at IS NULL
                        OR datetime(last_crawled_at) <= datetime('now', ?)
                    )
                """
                params.append(f"-{int(stale_hours)} hours")

        query += " ORDER BY priority ASC, COALESCE(last_crawled_at, '') ASC, COALESCE(keyword_order, 9223372036854775807) ASC, keyword ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        rows = self._fetchall(query, params)
        return [row["display_keyword"] or row["keyword"] for row in rows]

    def get_keywords_for_crawl_rows(
        self,
        *,
        tracking_mode: str = "tracked",
        status: str = "active",
        limit: Optional[int] = None,
        stale_hours: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT keyword, display_keyword, tracking_mode, source_type, source_platform,
                   priority, last_seen, last_crawled_at, last_analyzed_at, last_expanded_at,
                   metadata_json, last_snapshot_id, keyword_order
            FROM keywords
            WHERE status = ? AND tracking_mode = ?
        """
        params: List[Any] = [status, tracking_mode]

        if stale_hours is not None:
            if self.is_postgres:
                query += """
                    AND (
                        last_crawled_at IS NULL
                        OR last_crawled_at = ''
                        OR CAST(last_crawled_at AS timestamp) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')
                    )
                """
                params.append(int(stale_hours))
            else:
                query += """
                    AND (
                        last_crawled_at IS NULL
                        OR datetime(last_crawled_at) <= datetime('now', ?)
                    )
                """
                params.append(f"-{int(stale_hours)} hours")

        query += " ORDER BY priority ASC, COALESCE(last_crawled_at, '') ASC, COALESCE(keyword_order, 9223372036854775807) ASC, keyword ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        rows: List[Dict[str, Any]] = []
        for row in self._fetchall(query, params):
            payload = dict(row)
            metadata_raw = payload.get("metadata_json")
            if metadata_raw:
                try:
                    payload["metadata"] = json.loads(metadata_raw)
                except Exception:
                    payload["metadata"] = {}
            else:
                payload["metadata"] = {}
            rows.append(payload)
        return rows

    def get_keywords_for_expand(
        self,
        *,
        tracking_mode: str = "tracked",
        status: str = "active",
        limit: Optional[int] = None,
        stale_hours: Optional[int] = None,
        include_source_types: Optional[Iterable[str]] = None,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT keyword, display_keyword, tracking_mode, source_type, source_platform,
                   priority, last_seen, last_crawled_at, last_analyzed_at, last_expanded_at,
                   metadata_json, last_snapshot_id
            FROM keywords
            WHERE status = ? AND tracking_mode = ?
        """
        params: List[Any] = [status, tracking_mode]

        normalized_sources = [
            self._safe_text(source)
            for source in (include_source_types or [])
            if self._safe_text(source)
        ]
        if normalized_sources:
            placeholders = ", ".join("?" for _ in normalized_sources)
            query += f" AND source_type IN ({placeholders})"
            params.extend(normalized_sources)

        if stale_hours is not None:
            if self.is_postgres:
                query += """
                    AND (
                        last_expanded_at IS NULL
                        OR last_expanded_at = ''
                        OR CAST(last_expanded_at AS timestamp) <= CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')
                    )
                """
                params.append(int(stale_hours))
            else:
                query += """
                    AND (
                        last_expanded_at IS NULL
                        OR datetime(last_expanded_at) <= datetime('now', ?)
                    )
                """
                params.append(f"-{int(stale_hours)} hours")

        query += " ORDER BY priority ASC, COALESCE(last_expanded_at, '') ASC, COALESCE(keyword_order, 9223372036854775807) ASC, keyword ASC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        rows: List[Dict[str, Any]] = []
        for row in self._fetchall(query, params):
            payload = dict(row)
            metadata_raw = payload.get("metadata_json")
            if metadata_raw:
                try:
                    payload["metadata"] = json.loads(metadata_raw)
                except Exception:
                    payload["metadata"] = {}
            else:
                payload["metadata"] = {}
            rows.append(payload)
        return rows

    def start_keyword_run(
        self,
        run_type: str,
        *,
        trigger_mode: str = "manual",
        seed_keyword: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        platforms: Optional[Iterable[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        now = datetime.now().isoformat()
        params = (
            run_type,
            trigger_mode,
            self._normalize_keyword(seed_keyword) if seed_keyword else None,
            snapshot_id,
            self._json_dumps(list(platforms or [])),
            "running",
            0,
            self._json_dumps(metadata) if metadata is not None else None,
            now,
        )
        if self.is_postgres:
            row = self._fetchone(
                """
                INSERT INTO keyword_runs (
                    run_type, trigger_mode, seed_keyword, snapshot_id, platforms_json,
                    status, keyword_count, metadata_json, started_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                params,
            )
            return int(row["id"])

        cursor = self._execute(
            """
            INSERT INTO keyword_runs (
                run_type, trigger_mode, seed_keyword, snapshot_id, platforms_json,
                status, keyword_count, metadata_json, started_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        self._commit()
        return int(cursor.lastrowid)

    def finish_keyword_run(
        self,
        run_id: int,
        *,
        status: str = "completed",
        keyword_count: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        metadata_json = None
        if metadata is not None:
            row = self._fetchone("SELECT metadata_json FROM keyword_runs WHERE id = ?", (run_id,))
            merged_metadata: Dict[str, Any] = {}
            if row and row["metadata_json"]:
                try:
                    merged_metadata = json.loads(row["metadata_json"])
                except Exception:
                    merged_metadata = {}
            merged_metadata.update(metadata)
            metadata_json = self._json_dumps(merged_metadata)
        cursor = self._execute(
            """
            UPDATE keyword_runs
            SET status = ?,
                keyword_count = COALESCE(?, keyword_count),
                metadata_json = COALESCE(?, metadata_json),
                finished_at = ?
            WHERE id = ?
            """,
            (
                status,
                keyword_count,
                metadata_json,
                datetime.now().isoformat(),
                run_id,
            ),
        )
        self._commit()
        return cursor.rowcount > 0

    def record_keyword_edge(
        self,
        parent_keyword: str,
        child_keyword: str,
        *,
        source_platform: str = "",
        source_type: str = "autocomplete",
        run_id: Optional[int] = None,
        discovered_at: Optional[str] = None,
        commit: bool = True,
    ) -> bool:
        parent = self._normalize_keyword(parent_keyword)
        child = self._normalize_keyword(child_keyword)
        if not parent or not child or parent == child:
            return False

        sql = """
            INSERT INTO keyword_edges (
                parent_keyword, child_keyword, source_platform, source_type, run_id, discovered_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (
            parent,
            child,
            source_platform or "",
            source_type,
            run_id,
            discovered_at or datetime.now().isoformat(),
        )
        if self.is_postgres:
            sql += " ON CONFLICT DO NOTHING"
        else:
            sql = sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
        cursor = self._execute(sql, params)
        if commit:
            self._commit()
        return cursor.rowcount > 0

    def record_keyword_edges(
        self,
        parent_keyword: str,
        child_keywords: Iterable[str],
        *,
        source_platform: str = "",
        source_type: str = "autocomplete",
        run_id: Optional[int] = None,
    ) -> int:
        count = 0
        discovered_at = datetime.now().isoformat()
        for child in child_keywords:
            if self.record_keyword_edge(
                parent_keyword,
                child,
                source_platform=source_platform,
                source_type=source_type,
                run_id=run_id,
                discovered_at=discovered_at,
                commit=False,
            ):
                count += 1
        self._commit()
        return count

    def record_keyword_metrics(
        self,
        records: Iterable[Dict[str, Any]],
        *,
        snapshot_id: Optional[str],
        analyzed_at: Optional[str] = None,
    ) -> int:
        ts = analyzed_at or datetime.now().isoformat()
        count = 0

        for record in records:
            normalized = self._normalize_keyword(record.get("keyword"))
            if not normalized:
                continue

            self.upsert_keyword(
                normalized,
                display_keyword=record.get("keyword"),
                status="active",
                source_type="",
                source_platform="",
                priority=None,
                last_analyzed_at=ts,
                last_snapshot_id=snapshot_id,
                commit=False,
            )

            self._execute(
                """
                INSERT INTO keyword_metrics_snapshots (
                    keyword, snapshot_id, analyzed_at, grade, rank, total_score,
                    demand_index, competition_density, supply_gap_ratio, price_safety_ratio,
                    margin_war_pct, margin_peace_pct, noon_total, noon_express, noon_median_price,
                    amazon_total, amazon_median_price, amazon_bsr_count, google_interest,
                    v6_category, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(keyword, snapshot_id) DO UPDATE SET
                    analyzed_at = excluded.analyzed_at,
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
                    normalized,
                    snapshot_id,
                    ts,
                    self._safe_text(record.get("grade")),
                    self._safe_int(record.get("rank")),
                    self._safe_float(record.get("total_score")),
                    self._safe_float(record.get("demand_index")),
                    self._safe_float(record.get("competition_density")),
                    self._safe_float(record.get("supply_gap_ratio")),
                    self._safe_float(record.get("price_safety_ratio")),
                    self._safe_float(record.get("margin_war_pct")),
                    self._safe_float(record.get("margin_peace_pct")),
                    self._safe_int(record.get("noon_total")),
                    self._safe_int(record.get("noon_express")),
                    self._safe_float(record.get("noon_median_price")),
                    self._safe_int(record.get("amazon_total")),
                    self._safe_float(record.get("amazon_median_price")),
                    self._safe_int(record.get("amazon_bsr_count")),
                    self._safe_float(record.get("google_interest")),
                    self._safe_text(record.get("v6_category")),
                    self._json_dumps(record),
                ),
            )
            count += 1

        self._commit()
        return count

    def process_scraper_result(
        self,
        result: Dict[str, Any],
        *,
        source_type: str = "keyword",
        source_name: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        category_name: Optional[str] = None,
    ) -> int:
        keyword = result.get("keyword", "")
        platform = result.get("platform") or result.get("_meta", {}).get("platform", "noon")
        scraped_at = (
            result.get("_meta", {}).get("scraped_at")
            or result.get("scraped_at")
            or datetime.now().isoformat()
        )
        source_value = source_name if source_name is not None else keyword

        return self.persist_products(
            result.get("products", []),
            platform=platform,
            scraped_at=scraped_at,
            source_type=source_type,
            source_value=source_value,
            keyword=keyword if source_type == "keyword" else None,
            snapshot_id=snapshot_id,
            category_name=category_name or result.get("primary_category"),
        )

    def batch_process_results(self, results: List[Dict[str, Any]]) -> int:
        total = 0
        for result in results:
            total += self.process_scraper_result(result)
        logger.info("批量处理完成: %s 个结果, %s 个商品", len(results), total)
        return total

    def import_keyword_directory(
        self,
        json_dir: Path,
        *,
        platform: Optional[str] = None,
        snapshot_id: Optional[str] = None,
    ) -> int:
        total = 0
        for json_file in sorted(Path(json_dir).glob("*.json")):
            try:
                result = json.loads(json_file.read_text(encoding="utf-8"))
                total += self.process_scraper_result(
                    result,
                    source_type="keyword",
                    snapshot_id=snapshot_id,
                    category_name=result.get("primary_category"),
                )
            except Exception as exc:
                logger.warning("导入 %s 失败: %s", json_file, exc)
        return total

    def import_category_directory(
        self,
        category_dir: Path,
        *,
        platform: str = "noon",
        snapshot_id: Optional[str] = None,
        category_name: Optional[str] = None,
    ) -> int:
        category_dir = Path(category_dir)
        category_name = category_name or category_dir.name
        total = 0

        subcategory_dir = category_dir / "subcategory"
        if subcategory_dir.exists():
            files = sorted(subcategory_dir.glob("*.json"))
            for json_file in files:
                try:
                    result = json.loads(json_file.read_text(encoding="utf-8"))
                    total += self.persist_products(
                        result.get("products", []),
                        platform=platform,
                        scraped_at=result.get("scraped_at"),
                        source_type="category",
                        source_value=result.get("subcategory", json_file.stem),
                        snapshot_id=snapshot_id,
                        category_name=category_name,
                    )
                except Exception as exc:
                    logger.warning("导入 %s 失败: %s", json_file, exc)
            return total

        all_products_file = category_dir / "all_products.json"
        if all_products_file.exists():
            try:
                products = json.loads(all_products_file.read_text(encoding="utf-8"))
                if isinstance(products, list):
                    total += self.persist_products(
                        products,
                        platform=platform,
                        scraped_at=datetime.now().isoformat(),
                        source_type="category",
                        source_value=category_name,
                        snapshot_id=snapshot_id,
                        category_name=category_name,
                    )
            except Exception as exc:
                logger.warning("导入 %s 失败: %s", all_products_file, exc)
        return total

    def import_category_file(
        self,
        json_file: Path,
        *,
        platform: str = "noon",
        snapshot_id: Optional[str] = None,
        category_name: Optional[str] = None,
    ) -> int:
        json_file = Path(json_file)
        if not json_file.exists():
            return 0

        try:
            result = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("瀵煎叆 %s 澶辫触: %s", json_file, exc)
            return 0

        if not isinstance(result, dict):
            return 0

        resolved_category_name = category_name or result.get("primary_category") or json_file.parent.parent.name
        return self.persist_products(
            result.get("products", []),
            platform=platform,
            scraped_at=result.get("scraped_at"),
            source_type="category",
            source_value=result.get("subcategory", json_file.stem),
            snapshot_id=snapshot_id,
            category_name=resolved_category_name,
        )

    def get_product(self, product_id: str, platform: Optional[str] = None) -> Optional[Dict[str, Any]]:
        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return None
        row = self._fetchone("SELECT * FROM products WHERE product_key = ?", (identity["product_key"],))
        return dict(row) if row else None

    def get_price_history(
        self,
        product_id: str,
        limit: int = 100,
        *,
        platform: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return []
        rows = self._fetchall(
            """
            SELECT scraped_at, price, original_price, currency
            FROM price_history
            WHERE product_key = ?
            ORDER BY scraped_at DESC
            LIMIT ?
            """,
            (identity["product_key"], limit),
        )
        return [dict(row) for row in rows]

    def get_rank_history(
        self,
        product_id: str,
        limit: int = 100,
        *,
        platform: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        identity = self._resolve_product_identity(product_id, platform=platform)
        if identity is None:
            return []
        rows = self._fetchall(
            """
            SELECT scraped_at, keyword, search_rank, bsr_rank
            FROM rank_history
            WHERE product_key = ?
            ORDER BY scraped_at DESC
            LIMIT ?
            """,
            (identity["product_key"], limit),
        )
        return [dict(row) for row in rows]

    def search_products(
        self,
        keyword: str,
        limit: int = 100,
        *,
        platform: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        search_pattern = f"%{keyword}%"
        params: List[Any] = [search_pattern, search_pattern]
        platform_sql = ""
        if platform:
            platform_sql = " AND p.platform = ?"
            params.append(self._normalize_platform(platform))

        rows = self._fetchall(
            f"""
            SELECT
                p.product_key, p.product_id, p.platform, p.title, p.brand, p.product_url,
                p.first_seen, p.last_seen, p.is_active,
                (SELECT price FROM price_history ph
                 WHERE ph.product_key = p.product_key
                 ORDER BY ph.scraped_at DESC LIMIT 1) AS last_price,
                (SELECT bsr_rank FROM rank_history rh
                 WHERE rh.product_key = p.product_key AND rh.bsr_rank IS NOT NULL
                 ORDER BY rh.scraped_at DESC LIMIT 1) AS last_bsr
            FROM products p
            WHERE (p.product_id LIKE ? OR p.title LIKE ?){platform_sql}
            ORDER BY p.last_seen DESC
            LIMIT ?
            """,
            [*params, limit],
        )
        return [dict(row) for row in rows]

    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {}

        stats["total_products"] = self._fetchone("SELECT COUNT(*) AS count FROM products")["count"]
        stats["platform_distribution"] = [
            dict(row) for row in self._fetchall("SELECT platform, COUNT(*) AS count FROM products GROUP BY platform")
        ]
        stats["total_price_records"] = self._fetchone("SELECT COUNT(*) AS count FROM price_history")["count"]
        stats["total_rank_records"] = self._fetchone("SELECT COUNT(*) AS count FROM rank_history")["count"]
        stats["total_sales_snapshots"] = self._fetchone("SELECT COUNT(*) AS count FROM sales_snapshot")["count"]
        stats["total_crawl_observations"] = self._fetchone("SELECT COUNT(*) AS count FROM crawl_observations")["count"]
        stats["total_keywords"] = self._fetchone("SELECT COUNT(*) AS count FROM keywords")["count"]
        stats["active_keywords"] = self._fetchone("SELECT COUNT(*) AS count FROM keywords WHERE status = 'active'")["count"]
        stats["tracked_keywords"] = self._fetchone(
            "SELECT COUNT(*) AS count FROM keywords WHERE status = 'active' AND tracking_mode = 'tracked'"
        )["count"]
        stats["adhoc_keywords"] = self._fetchone(
            "SELECT COUNT(*) AS count FROM keywords WHERE status = 'active' AND tracking_mode = 'adhoc'"
        )["count"]
        stats["keyword_source_distribution"] = [
            dict(row)
            for row in self._fetchall(
                "SELECT source_type, COUNT(*) AS count FROM keywords WHERE status = 'active' GROUP BY source_type"
            )
        ]
        stats["total_keyword_runs"] = self._fetchone("SELECT COUNT(*) AS count FROM keyword_runs")["count"]
        stats["total_keyword_edges"] = self._fetchone("SELECT COUNT(*) AS count FROM keyword_edges")["count"]
        stats["total_keyword_metric_snapshots"] = self._fetchone(
            "SELECT COUNT(*) AS count FROM keyword_metrics_snapshots"
        )["count"]

        return stats

    def close(self):
        if self.conn:
            self.conn.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
