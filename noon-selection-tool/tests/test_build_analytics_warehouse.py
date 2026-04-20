from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import build_analytics_warehouse as warehouse_builder


def create_stage_db(
    db_path: Path,
    *,
    products: int = 0,
    crawl_observations: int = 0,
    keywords: int = 0,
    keyword_runs: int = 0,
    keyword_metrics_snapshots: int = 0,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS crawl_observations (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS keywords (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS keyword_runs (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS keyword_metrics_snapshots (id INTEGER PRIMARY KEY)")
        for table_name, row_count in (
            ("products", products),
            ("crawl_observations", crawl_observations),
            ("keywords", keywords),
            ("keyword_runs", keyword_runs),
            ("keyword_metrics_snapshots", keyword_metrics_snapshots),
        ):
            conn.executemany(
                f"INSERT INTO {table_name} DEFAULT VALUES",
                [tuple() for _ in range(max(row_count, 0))],
            )
        conn.commit()
    finally:
        conn.close()


class KeywordStageSelectionTests(unittest.TestCase):
    def test_select_keyword_stage_dbs_prefers_official_db_by_default(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime_data"
            official_db = runtime_dir / "keyword" / "product_store.db"
            discovered_db = runtime_dir / "keyword_monitor_smoke" / "product_store.db"
            create_stage_db(official_db, products=2, crawl_observations=3, keyword_metrics_snapshots=1)
            create_stage_db(discovered_db, products=9, crawl_observations=9, keyword_metrics_snapshots=2)

            with mock.patch.object(warehouse_builder, "DEFAULT_KEYWORD_DB", official_db):
                selected, selection = warehouse_builder.select_keyword_stage_dbs(
                    None,
                    runtime_dir,
                    include_discovery=False,
                    allow_legacy_fallback=False,
                )

            self.assertEqual(selected, [str(official_db)])
            self.assertEqual(selection["mode"], "official_only")
            self.assertEqual(selection["selected_keyword_dbs"], [str(official_db)])
            self.assertEqual(selection["discovered_candidate_count"], 1)

    def test_select_keyword_stage_dbs_disables_legacy_fallback_by_default(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime_data"
            official_db = runtime_dir / "keyword" / "product_store.db"
            discovered_db = runtime_dir / "keyword_monitor_smoke" / "product_store.db"
            create_stage_db(official_db)
            create_stage_db(discovered_db, products=7, crawl_observations=8, keyword_metrics_snapshots=1)

            with mock.patch.object(warehouse_builder, "DEFAULT_KEYWORD_DB", official_db):
                selected, selection = warehouse_builder.select_keyword_stage_dbs(
                    None,
                    runtime_dir,
                    include_discovery=False,
                    allow_legacy_fallback=False,
                )

            self.assertEqual(selected, [])
            self.assertEqual(selection["mode"], "official_empty_no_fallback")
            self.assertEqual(selection["recommended_fallback_keyword_db"], str(discovered_db))
            self.assertEqual(selection["discovered_candidate_count"], 1)
            self.assertTrue(selection["fallback_disabled_reason"])

    def test_select_keyword_stage_dbs_allows_legacy_fallback_only_when_explicit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime_data"
            official_db = runtime_dir / "keyword" / "product_store.db"
            weaker_db = runtime_dir / "keyword_old" / "product_store.db"
            stronger_db = runtime_dir / "keyword_monitor_smoke" / "product_store.db"
            create_stage_db(official_db)
            create_stage_db(weaker_db, products=4, crawl_observations=5, keyword_metrics_snapshots=1)
            create_stage_db(stronger_db, products=8, crawl_observations=12, keyword_metrics_snapshots=3)

            with mock.patch.object(warehouse_builder, "DEFAULT_KEYWORD_DB", official_db):
                selected, selection = warehouse_builder.select_keyword_stage_dbs(
                    None,
                    runtime_dir,
                    include_discovery=False,
                    allow_legacy_fallback=True,
                )

            self.assertEqual(selected, [str(stronger_db)])
            self.assertEqual(selection["mode"], "legacy_fallback")
            self.assertEqual(selection["fallback_keyword_db"], str(stronger_db))
            self.assertEqual(selection["selected_keyword_dbs"], [str(stronger_db)])

    def test_select_keyword_stage_dbs_include_discovery_keeps_official_and_discovered(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime_data"
            official_db = runtime_dir / "keyword" / "product_store.db"
            discovered_db = runtime_dir / "keyword_monitor_smoke" / "product_store.db"
            create_stage_db(official_db, products=2, crawl_observations=3, keyword_metrics_snapshots=1)
            create_stage_db(discovered_db, products=7, crawl_observations=8, keyword_metrics_snapshots=2)

            with mock.patch.object(warehouse_builder, "DEFAULT_KEYWORD_DB", official_db):
                selected, selection = warehouse_builder.select_keyword_stage_dbs(
                    None,
                    runtime_dir,
                    include_discovery=True,
                    allow_legacy_fallback=False,
                )

            self.assertEqual(selected, [str(official_db), str(discovered_db)])
            self.assertEqual(selection["mode"], "official_plus_discovery")
            self.assertEqual(
                selection["selected_keyword_dbs"],
                [str(official_db), str(discovered_db)],
            )


class StickySignalMemoryTests(unittest.TestCase):
    def test_product_summary_preserves_last_seen_public_signal_texts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            warehouse_db = Path(temp_dir) / "warehouse.db"
            builder = warehouse_builder.AnalyticsWarehouseBuilder(warehouse_db)
            try:
                conn = builder.conn
                conn.execute(
                    """
                    INSERT INTO product_identity(
                        platform,
                        product_id,
                        title,
                        brand,
                        seller_name,
                        product_url,
                        latest_category_path,
                        first_seen,
                        last_seen
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "noon",
                        "P-100",
                        "Pet Brush",
                        "Acme",
                        "Acme Store",
                        "https://example.com/p/100",
                        "Home > Pet Supplies > Grooming",
                        "2026-03-20T00:00:00+00:00",
                        "2026-03-25T00:00:00+00:00",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO observation_events(
                        platform,
                        product_id,
                        source_type,
                        source_value,
                        scraped_at,
                        sold_recently_text,
                        stock_signal_text,
                        delivery_eta_signal_text,
                        category_path
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "noon",
                        "P-100",
                        "category",
                        "pets",
                        "2026-03-20T00:00:00+00:00",
                        "120+ sold recently",
                        "Only 2 left in stock",
                        "Get it tomorrow",
                        "Home > Pet Supplies > Grooming",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO observation_events(
                        platform,
                        product_id,
                        source_type,
                        source_value,
                        scraped_at,
                        sold_recently_text,
                        stock_signal_text,
                        delivery_eta_signal_text,
                        category_path
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "noon",
                        "P-100",
                        "category",
                        "pets",
                        "2026-03-25T00:00:00+00:00",
                        "",
                        "",
                        "",
                        "Home > Pet Supplies > Grooming",
                    ),
                )
                conn.commit()

                row = conn.execute(
                    """
                    SELECT
                        latest_sold_recently_text,
                        sticky_sold_recently_text,
                        sticky_sold_recently_seen_at,
                        latest_stock_signal_text,
                        sticky_stock_signal_text,
                        sticky_stock_signal_seen_at,
                        latest_delivery_eta_signal_text,
                        sticky_delivery_eta_signal_text,
                        sticky_delivery_eta_signal_seen_at
                    FROM vw_product_summary
                    WHERE platform = 'noon' AND product_id = 'P-100'
                    """
                ).fetchone()

                self.assertEqual(row["latest_sold_recently_text"], "")
                self.assertEqual(row["sticky_sold_recently_text"], "120+ sold recently")
                self.assertEqual(row["sticky_sold_recently_seen_at"], "2026-03-20T00:00:00+00:00")
                self.assertEqual(row["latest_stock_signal_text"], "")
                self.assertEqual(row["sticky_stock_signal_text"], "Only 2 left in stock")
                self.assertEqual(row["sticky_stock_signal_seen_at"], "2026-03-20T00:00:00+00:00")
                self.assertEqual(row["latest_delivery_eta_signal_text"], "")
                self.assertEqual(row["sticky_delivery_eta_signal_text"], "Get it tomorrow")
                self.assertEqual(row["sticky_delivery_eta_signal_seen_at"], "2026-03-20T00:00:00+00:00")
            finally:
                builder.close()


class IncrementalWarehouseImportTests(unittest.TestCase):
    def test_import_stage_db_only_replays_rows_newer_than_last_import(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            stage_db = temp_root / "stage.db"
            warehouse_db = temp_root / "warehouse.db"

            conn = sqlite3.connect(str(stage_db))
            try:
                conn.execute(
                    """
                    CREATE TABLE products (
                        product_id TEXT PRIMARY KEY,
                        platform TEXT,
                        title TEXT,
                        brand TEXT,
                        seller_name TEXT,
                        category_path TEXT,
                        product_url TEXT,
                        image_url TEXT,
                        last_public_signals_json TEXT,
                        first_seen TEXT,
                        last_seen TEXT,
                        is_active INTEGER
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE crawl_observations (
                        product_id TEXT,
                        platform TEXT,
                        source_type TEXT,
                        source_value TEXT,
                        scraped_at TEXT,
                        category_name TEXT,
                        category_path TEXT,
                        product_url TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO products(
                        product_id, platform, title, first_seen, last_seen, is_active
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("p-1", "noon", "Product 1", "2026-04-01T00:00:00+00:00", "2026-04-01T00:00:00+00:00", 1),
                )
                conn.execute(
                    """
                    INSERT INTO crawl_observations(
                        product_id, platform, source_type, source_value, scraped_at, category_name, category_path, product_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("p-1", "noon", "category", "pets", "2026-04-01T00:00:00+00:00", "pets", "Home > Pets", "https://example.com/p1"),
                )
                conn.commit()
            finally:
                conn.close()

            builder = warehouse_builder.AnalyticsWarehouseBuilder(warehouse_db)
            try:
                first_summary = builder.import_stage_db(stage_db)
                first_stats = builder.collect_warehouse_stats()

                conn = sqlite3.connect(str(stage_db))
                try:
                    conn.execute(
                        """
                        INSERT INTO products(
                            product_id, platform, title, first_seen, last_seen, is_active
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        ("p-2", "noon", "Product 2", "2099-04-02T00:00:00+00:00", "2099-04-02T00:00:00+00:00", 1),
                    )
                    conn.execute(
                        """
                        INSERT INTO crawl_observations(
                            product_id, platform, source_type, source_value, scraped_at, category_name, category_path, product_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        ("p-2", "noon", "category", "pets", "2099-04-02T00:00:00+00:00", "pets", "Home > Pets", "https://example.com/p2"),
                    )
                    conn.commit()
                finally:
                    conn.close()

                second_summary = builder.import_stage_db(stage_db)
                second_stats = builder.collect_warehouse_stats()
            finally:
                builder.close()

            self.assertEqual(first_summary["source_product_count"], 1)
            self.assertEqual(first_summary["source_observation_count"], 1)
            self.assertEqual(first_stats["product_identity"], 1)
            self.assertEqual(first_stats["observation_events"], 1)
            self.assertEqual(second_summary["source_product_count"], 1)
            self.assertEqual(second_summary["source_observation_count"], 1)
            self.assertEqual(second_stats["product_identity"], 2)
            self.assertEqual(second_stats["observation_events"], 2)
            self.assertTrue(second_summary["incremental_since"])


if __name__ == "__main__":
    unittest.main()
