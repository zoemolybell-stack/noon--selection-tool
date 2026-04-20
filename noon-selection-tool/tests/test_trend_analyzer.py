from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.trend_analyzer import TrendAnalyzer
from config.product_store import ProductStore


class TrendAnalyzerTests(unittest.TestCase):
    def test_price_trend_uses_platform_scoped_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "product_store.db"
            store = ProductStore(db_path)
            try:
                product_id = "SKU-1"
                store.upsert_product({"product_id": product_id, "title": "Noon Item", "platform": "noon"})
                store.upsert_product({"product_id": product_id, "title": "Amazon Item", "platform": "amazon"})

                base_time = datetime.now() - timedelta(days=2)
                noon_times = [
                    (base_time + timedelta(days=0)).isoformat(),
                    (base_time + timedelta(days=1)).isoformat(),
                ]
                amazon_times = [
                    (base_time + timedelta(days=0)).isoformat(),
                    (base_time + timedelta(days=1)).isoformat(),
                ]

                store.add_price_record(product_id, 100, scraped_at=noon_times[0], platform="noon")
                store.add_price_record(product_id, 120, scraped_at=noon_times[1], platform="noon")
                store.add_price_record(product_id, 50, scraped_at=amazon_times[0], platform="amazon")
                store.add_price_record(product_id, 45, scraped_at=amazon_times[1], platform="amazon")
            finally:
                store.close()

            analyzer = TrendAnalyzer(db_path)
            try:
                noon_result = analyzer.analyze_price_trend(product_id, days=30, platform="noon")
                amazon_result = analyzer.analyze_price_trend(product_id, days=30, platform="amazon")
                self.assertEqual(noon_result["status"], "ok")
                self.assertEqual(noon_result["trend"], "increasing")
                self.assertEqual(noon_result["first_price"], 100.0)
                self.assertEqual(noon_result["last_price"], 120.0)
                self.assertEqual(noon_result["platform"], "noon")

                self.assertEqual(amazon_result["status"], "ok")
                self.assertEqual(amazon_result["trend"], "decreasing")
                self.assertEqual(amazon_result["first_price"], 50.0)
                self.assertEqual(amazon_result["last_price"], 45.0)
                self.assertEqual(amazon_result["platform"], "amazon")
            finally:
                analyzer.close()


if __name__ == "__main__":
    unittest.main()
