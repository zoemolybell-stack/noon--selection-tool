from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.noon_category_signals import augment_product_from_signals


class NoonCategorySignalsTests(unittest.TestCase):
    def test_augment_product_from_signals_extracts_runtime_evidence_and_markers(self):
        product = {
            "title": "Adjustable Dumbbell Set",
            "seller_name": "Noon",
            "brand": "Noon",
            "delivery_type": "",
            "is_express": False,
        }
        data = {
            "signalTexts": [
                "100+ sold recently",
                "Only 5 left in stock",
                "Lowest price in 30 days",
            ],
            "cardText": "Get it by 14 April\nTop Rated",
            "deliveryMarkers": [
                {"alt": "Supermall", "className": "delivery-badge"},
            ],
            "adMarkers": [
                {"text": "Sponsored", "className": "ad-badge"},
            ],
        }

        enriched = augment_product_from_signals(product, data)

        self.assertEqual(enriched["sold_recently_text"], "100+ sold recently")
        self.assertEqual(enriched["sold_recently"], "100")
        self.assertEqual(enriched["stock_signal_text"], "Only 5 left in stock")
        self.assertEqual(enriched["stock_left_count"], 5)
        self.assertEqual(enriched["lowest_price_signal_text"], "Lowest price in 30 days")
        self.assertEqual(enriched["delivery_eta_signal_text"], "Get it by 14 April")
        self.assertEqual(enriched["delivery_type"], "supermall")
        self.assertTrue(enriched["is_ad"])
        self.assertIn("Top Rated", enriched["badge_texts"])
        self.assertIn("Get it by 14 April", enriched["delivery_signal_texts"])
        self.assertEqual(enriched["signal_source"], "public_page_card_text+snippets+markers")

    def test_augment_product_from_signals_filters_noise_and_keeps_existing_identity_fields(self):
        product = {
            "title": "Portable Picnic Table",
            "seller_name": "Outdoor Hub",
            "brand": "Outdoor Hub",
            "delivery_type": "",
            "is_express": True,
        }
        data = {
            "signalTexts": [
                "Portable Picnic Table",
                "Outdoor Hub",
                "12345",
                "Only 1 left in stock",
            ],
            "cardText": "Get in 45 mins\nFree Delivery",
            "deliveryMarkers": [],
            "adMarkers": [],
        }

        enriched = augment_product_from_signals(product, data)

        self.assertEqual(enriched["raw_signal_texts"], ["Only 1 left in stock", "Get in 45 mins", "Free Delivery"])
        self.assertEqual(enriched["stock_left_count"], 1)
        self.assertEqual(enriched["delivery_type"], "express")
        self.assertFalse(enriched.get("is_ad", False))


if __name__ == "__main__":
    unittest.main()
