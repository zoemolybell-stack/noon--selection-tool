from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops import crawler_control


class CrawlerControlTests(unittest.TestCase):
    def test_load_runtime_category_map_prefers_richest_batch_scan_map_when_config_is_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            empty_config = base / "config-runtime-category-map.json"
            empty_config.write_text("{}", encoding="utf-8")
            newer_partial_map = base / "plan_9_round_18_runtime_category_map.json"
            newer_partial_map.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-11T01:04:06+08:00",
                        "categories": {
                            "home_kitchen": {
                                "cookware": {
                                    "config_id": "cookware",
                                    "display_name": "Cookware",
                                    "breadcrumb_path": "Home > Home & Kitchen > Cookware",
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            batch_map = base / "ready_scan_20260403_134641_runtime_category_map.json"
            batch_map.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-03T13:46:41+08:00",
                        "categories": {
                            "sports": {
                                "fitness_rollers": {
                                    "config_id": "fitness_rollers",
                                    "display_name": "Fitness Rollers",
                                    "breadcrumb_path": "Home > Sports > Fitness > Fitness Rollers",
                                    "parent_config_id": "fitness",
                                }
                            },
                            "pets": {
                                "dog_beds": {
                                    "config_id": "dog_beds",
                                    "display_name": "Dog Beds",
                                    "breadcrumb_path": "Home > Pets > Beds > Dog Beds",
                                    "parent_config_id": "beds",
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            newer_partial_map.touch()
            batch_map.touch()

            with mock.patch.object(
                crawler_control,
                "_runtime_category_map_candidate_paths",
                return_value=[empty_config, newer_partial_map, batch_map],
            ):
                runtime_map = crawler_control.load_runtime_category_map()

        self.assertEqual(runtime_map.get("generated_at"), "2026-04-03T13:46:41+08:00")
        self.assertEqual(runtime_map.get("_source_path"), str(batch_map))
        self.assertIn("sports", runtime_map.get("categories", {}))

    def test_build_crawler_catalog_exposes_flattened_subcategories(self):
        runtime_map = {
            "generated_at": "2026-04-13T00:00:00",
            "categories": {
                "sports": {
                    "fitness_rollers": {
                        "config_id": "fitness_rollers",
                        "display_name": "Fitness Rollers",
                        "breadcrumb_path": "Home > Sports > Fitness > Fitness Rollers",
                        "parent_config_id": "fitness",
                    }
                }
            },
        }

        with mock.patch.object(crawler_control, "load_runtime_category_map", return_value=runtime_map):
            catalog = crawler_control.build_crawler_catalog()

        self.assertIn("subcategory_catalog", catalog)
        self.assertEqual(catalog["runtime_category_map"]["subcategory_count"], 1)
        self.assertEqual(catalog["subcategory_catalog"][0]["config_id"], "fitness_rollers")
        self.assertEqual(catalog["subcategory_catalog"][0]["display_name"], "Fitness Rollers")
        self.assertEqual(catalog["subcategory_catalog"][0]["breadcrumb_path"], "Home > Sports > Fitness > Fitness Rollers")
        self.assertEqual(catalog["subcategory_catalog"][0]["parent_config_id"], "fitness")
        self.assertEqual(catalog["subcategory_catalog"][0]["top_level_category"], "sports")

    def test_normalize_category_single_payload_derives_category_from_target_subcategory(self):
        runtime_map = {
            "categories": {
                "sports": {
                    "fitness_rollers": {
                        "config_id": "fitness_rollers",
                        "display_name": "Fitness Rollers",
                        "breadcrumb_path": "Home > Sports > Fitness > Fitness Rollers",
                        "parent_config_id": "fitness",
                    }
                }
            },
        }

        with mock.patch.object(crawler_control, "load_runtime_category_map", return_value=runtime_map):
            payload = crawler_control._normalize_category_single_payload(
                {
                    "target_subcategory": "fitness_rollers",
                    "product_count": 180,
                }
            )

        self.assertEqual(payload["category"], "sports")
        self.assertEqual(payload["target_subcategory"], "fitness_rollers")
        self.assertEqual(payload["product_count"], 180)

    def test_normalize_category_single_payload_rejects_mismatched_target_subcategory(self):
        runtime_map = {
            "categories": {
                "sports": {
                    "fitness_rollers": {
                        "config_id": "fitness_rollers",
                        "display_name": "Fitness Rollers",
                        "breadcrumb_path": "Home > Sports > Fitness > Fitness Rollers",
                        "parent_config_id": "fitness",
                    }
                }
            },
        }

        with mock.patch.object(crawler_control, "load_runtime_category_map", return_value=runtime_map):
            with self.assertRaises(ValueError):
                crawler_control._normalize_category_single_payload(
                    {
                        "category": "pets",
                        "target_subcategory": "fitness_rollers",
                    }
                )

    def test_normalize_category_ready_scan_payload_validates_subcategory_overrides(self):
        runtime_map = {
            "categories": {
                "sports": {
                    "fitness_rollers": {
                        "config_id": "fitness_rollers",
                        "display_name": "Fitness Rollers",
                        "breadcrumb_path": "Home > Sports > Fitness > Fitness Rollers",
                        "parent_config_id": "fitness",
                    }
                }
            },
        }

        with mock.patch.object(crawler_control, "load_runtime_category_map", return_value=runtime_map):
            payload = crawler_control._normalize_category_ready_scan_payload(
                {
                    "categories": ["sports"],
                    "default_product_count_per_leaf": 120,
                    "category_overrides": {"sports": 180},
                    "subcategory_overrides": {"fitness_rollers": {"product_count": 240}},
                }
            )

        self.assertEqual(payload["default_product_count_per_leaf"], 120)
        self.assertEqual(payload["category_overrides"]["sports"], 180)
        self.assertEqual(payload["subcategory_overrides"]["fitness_rollers"], 240)

    def test_normalize_category_ready_scan_payload_rejects_unknown_subcategory_config_id(self):
        runtime_map = {"categories": {"sports": {}}}

        with mock.patch.object(crawler_control, "load_runtime_category_map", return_value=runtime_map):
            with self.assertRaises(ValueError):
                crawler_control._normalize_category_ready_scan_payload(
                    {
                        "categories": ["sports"],
                        "subcategory_overrides": {"unknown_leaf": 100},
                    }
                )

    def test_normalize_category_ready_scan_payload_rejects_subcategory_override_outside_selected_categories(self):
        runtime_map = {
            "categories": {
                "sports": {
                    "fitness_rollers": {
                        "config_id": "fitness_rollers",
                        "display_name": "Fitness Rollers",
                        "breadcrumb_path": "Home > Sports > Fitness > Fitness Rollers",
                        "parent_config_id": "fitness",
                    }
                },
                "pets": {
                    "dog_beds": {
                        "config_id": "dog_beds",
                        "display_name": "Dog Beds",
                        "breadcrumb_path": "Home > Pets > Beds > Dog Beds",
                        "parent_config_id": "beds",
                    }
                },
            },
        }

        with mock.patch.object(crawler_control, "load_runtime_category_map", return_value=runtime_map):
            with self.assertRaises(ValueError):
                crawler_control._normalize_category_ready_scan_payload(
                    {
                        "categories": ["sports"],
                        "subcategory_overrides": {"dog_beds": 120},
                    }
                )


if __name__ == "__main__":
    unittest.main()
