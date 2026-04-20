from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.noon_product_parser import _parse_rating_block, parse_noon_product_payload


class NoonProductParserTests(unittest.TestCase):
    def test_parse_rating_block_handles_thousands_separator(self):
        rating, review_count = _parse_rating_block("4.3 (1,463 ratings)")

        self.assertEqual(rating, 4.3)
        self.assertEqual(review_count, 1463)

    def test_parse_rating_block_handles_compact_thousands(self):
        rating, review_count = _parse_rating_block("4.1 (1K+ ratings)")

        self.assertEqual(rating, 4.1)
        self.assertEqual(review_count, 1000)

    def test_parse_rating_block_handles_compact_decimal_thousands(self):
        rating, review_count = _parse_rating_block("4.8 1.2K+ ratings")

        self.assertEqual(rating, 4.8)
        self.assertEqual(review_count, 1200)

    def test_parse_noon_product_payload_keeps_full_review_count(self):
        payload = parse_noon_product_payload(
            {
                "href": "/saudi-en/example/Z00ACEF5601B7059377D8Z/p/?o=z00acef5601b7059377d8z-1",
                "title": "Example product",
                "priceText": "SAR 21.80",
                "ratingText": "4.3 (1,463 ratings)",
                "cardText": "#1 in Loofahs, Sponges & Poufs\n230+ sold recently\nGet it by 4 April",
            }
        )

        self.assertEqual(payload["product_id"], "Z00ACEF5601B7059377D8Z")
        self.assertEqual(payload["rating"], 4.3)
        self.assertEqual(payload["review_count"], 1463)

    def test_parse_noon_product_payload_uses_compact_review_count(self):
        payload = parse_noon_product_payload(
            {
                "href": "/saudi-en/example/N15475105A/p/?o=b9755d76ef2eb01f",
                "title": "Wallet",
                "priceText": "SAR 116.00",
                "ratingText": "4.1 (1K+ ratings)",
                "cardText": "10+ sold recently\nExpress",
            }
        )

        self.assertEqual(payload["product_id"], "N15475105A")
        self.assertEqual(payload["rating"], 4.1)
        self.assertEqual(payload["review_count"], 1000)
        self.assertTrue(payload["review_count_is_estimated"])

    def test_parse_noon_product_payload_keeps_main_image_url(self):
        payload = parse_noon_product_payload(
            {
                "href": "/saudi-en/example/N15475105A/p/?o=b9755d76ef2eb01f",
                "title": "Wallet",
                "priceText": "SAR 116.00",
                "ratingText": "4.1 (23 ratings)",
                "imageUrl": "https://f.nooncdn.com/p/pnsku/N15475105A/45/_/1730000000/example.jpg",
                "cardText": "10+ sold recently\nExpress",
            }
        )

        self.assertEqual(
            payload["image_url"],
            "https://f.nooncdn.com/p/pnsku/N15475105A/45/_/1730000000/example.jpg",
        )

    def test_parse_noon_product_payload_uses_delivery_markers_for_express(self):
        payload = parse_noon_product_payload(
            {
                "href": "/saudi-en/example/ZCDA4F4D4EAF396642538Z/p/?o=zcda4f4d4eaf396642538z-1",
                "title": "Bottle",
                "priceText": "SAR 72.73",
                "ratingText": "4.3 (541 ratings)",
                "cardText": "Free Delivery\nGet it by 15 April",
                "signalTexts": ["78% OFF", "Free Delivery", "100+ sold recently", "Get it by 15 April"],
                "deliveryMarkers": [
                    {"merged": "Free Delivery"},
                    {"merged": "noon-express"},
                    {"merged": "Get it by"},
                ],
                "isExpress": False,
            }
        )

        self.assertEqual(payload["delivery_type"], "express")
        self.assertEqual(payload["delivery_eta_signal_text"], "Get it by 15 April")
        self.assertIn("Free Delivery", payload["delivery_signal_texts"])

    def test_parse_noon_product_payload_keeps_eta_without_forcing_delivery_type(self):
        payload = parse_noon_product_payload(
            {
                "href": "/saudi-en/example/Z37A8F3865311D6813080Z/p/?o=e5ddbc224c5cb9aa",
                "title": "Mug",
                "priceText": "SAR 42.30",
                "ratingText": "4.1 (409 ratings)",
                "cardText": "Selling out fast\n210+ sold recently\nGET IN 31 MINS",
                "signalTexts": ["Selling out fast", "210+ sold recently", "GET IN 31 MINS"],
                "deliveryMarkers": [],
                "isExpress": False,
            }
        )

        self.assertEqual(payload["delivery_type"], "")
        self.assertEqual(payload["delivery_eta_signal_text"], "GET IN 31 MINS")
        self.assertIn("GET IN 31 MINS", payload["delivery_signal_texts"])


if __name__ == "__main__":
    unittest.main()
