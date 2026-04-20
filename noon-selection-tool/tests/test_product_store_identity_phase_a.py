from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.product_store import ProductStore


class ProductStoreIdentityPhaseATests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "product_store.db"
        self.store = ProductStore(self.db_path)

        self.store.upsert_product(
            {
                "product_id": "sku-noon-1",
                "platform": "noon",
                "title": "Alpha Camera",
                "brand": "Alpha",
                "product_url": "https://example.com/noon/sku-noon-1",
            }
        )
        self.store.upsert_product(
            {
                "product_id": "sku-amazon-1",
                "platform": "amazon",
                "title": "Beta Camera",
                "brand": "Beta",
                "product_url": "https://example.com/amazon/sku-amazon-1",
            }
        )

        self.store.add_price_record(
            "sku-noon-1",
            price=100,
            original_price=120,
            scraped_at="2026-03-29T10:00:00",
        )
        self.store.add_price_record(
            "sku-amazon-1",
            price=80,
            original_price=95,
            scraped_at="2026-03-29T11:00:00",
        )
        self.store.add_rank_record(
            "sku-noon-1",
            keyword="camera",
            bsr_rank=12,
            scraped_at="2026-03-29T10:05:00",
        )
        self.store.add_rank_record(
            "sku-amazon-1",
            keyword="camera",
            bsr_rank=27,
            scraped_at="2026-03-29T11:05:00",
        )

    def tearDown(self):
        self.store.close()
        self.temp_dir.cleanup()

    def test_get_price_history_respects_platform_when_provided(self):
        noon_rows = self.store.get_price_history("sku-noon-1", platform="noon")
        mismatch_rows = self.store.get_price_history("sku-noon-1", platform="amazon")

        self.assertEqual(len(noon_rows), 1)
        self.assertEqual(noon_rows[0]["price"], 100.0)
        self.assertEqual(mismatch_rows, [])

    def test_get_rank_history_respects_platform_when_provided(self):
        noon_rows = self.store.get_rank_history("sku-noon-1", platform="noon")
        mismatch_rows = self.store.get_rank_history("sku-noon-1", platform="amazon")

        self.assertEqual(len(noon_rows), 1)
        self.assertEqual(noon_rows[0]["bsr_rank"], 12)
        self.assertEqual(mismatch_rows, [])

    def test_search_products_supports_platform_filter(self):
        all_rows = self.store.search_products("Camera")
        noon_rows = self.store.search_products("Camera", platform="noon")
        amazon_rows = self.store.search_products("Camera", platform="amazon")

        self.assertEqual(len(all_rows), 2)
        self.assertEqual(len(noon_rows), 1)
        self.assertEqual(len(amazon_rows), 1)
        self.assertEqual(noon_rows[0]["platform"], "noon")
        self.assertEqual(noon_rows[0]["last_price"], 100.0)
        self.assertEqual(noon_rows[0]["last_bsr"], 12)
        self.assertEqual(amazon_rows[0]["platform"], "amazon")
        self.assertEqual(amazon_rows[0]["last_price"], 80.0)
        self.assertEqual(amazon_rows[0]["last_bsr"], 27)


if __name__ == "__main__":
    unittest.main()


class ProductStoreConfigResolutionTests(unittest.TestCase):
    def test_product_store_honors_environment_override_for_default_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            explicit_default = Path(temp_dir) / "default.db"
            overridden = Path(temp_dir) / "override.db"
            previous = os.environ.get("NOON_PRODUCT_STORE_DB")
            try:
                os.environ["NOON_PRODUCT_STORE_DB"] = str(overridden)
                store = ProductStore(explicit_default)
                try:
                    self.assertTrue(store.is_sqlite)
                    self.assertEqual(store.database_config.source_env, "NOON_PRODUCT_STORE_DB")
                    self.assertEqual(store.database_config.sqlite_path, overridden)
                finally:
                    store.close()
            finally:
                if previous is None:
                    os.environ.pop("NOON_PRODUCT_STORE_DB", None)
                else:
                    os.environ["NOON_PRODUCT_STORE_DB"] = previous
