from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.noon_delivery_detection import detect_delivery_type, normalize_delivery_marker_texts


class NoonDeliveryDetectionTests(unittest.TestCase):
    def test_normalize_delivery_marker_texts_supports_marker_dicts(self):
        marker_texts = normalize_delivery_marker_texts(
            [
                {"merged": "noon-express"},
                {"alt": "noon-express"},
                {"text": "supermall"},
                "Get it by 12 April",
            ]
        )

        self.assertEqual(
            marker_texts,
            ["noon-express", "supermall", "Get it by 12 April"],
        )

    def test_detect_delivery_type_prefers_explicit_marker(self):
        delivery_type = detect_delivery_type(
            "Free Delivery | Get it by 15 April",
            False,
            delivery_markers=[{"merged": "noon-express"}],
            delivery_signal_texts=["Free Delivery", "Get it by 15 April"],
        )

        self.assertEqual(delivery_type, "express")

    def test_detect_delivery_type_identifies_supermall_marker(self):
        delivery_type = detect_delivery_type(
            "GET IN 31 MINS",
            False,
            delivery_markers=[{"merged": "supermall"}],
            delivery_signal_texts=["GET IN 31 MINS"],
        )

        self.assertEqual(delivery_type, "supermall")

    def test_detect_delivery_type_keeps_eta_without_forcing_type(self):
        delivery_type = detect_delivery_type(
            "GET IN 31 MINS",
            False,
            delivery_markers=[],
            delivery_signal_texts=["GET IN 31 MINS"],
        )

        self.assertEqual(delivery_type, "")


if __name__ == "__main__":
    unittest.main()
