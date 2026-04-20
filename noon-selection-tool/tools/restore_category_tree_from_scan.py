"""
从批量扫描保存下来的 category_tree 快照中，回填一份更稳妥的 category_tree 展示名。

策略：
- 叶子节点优先使用扫描快照里的叶子名
- 父级节点只有在所有叶子都指向同一个平台父级名时才采用该名称
- 其余父级节点回退到基于 id 的稳定英文名称，避免被某个叶子误带偏
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TREE = ROOT / "config" / "category_tree.json"

PARENT_NAME_OVERRIDES = {
    "art_crafts": "Art & Crafts",
    "baby_care": "Baby Care",
    "baby_feeding": "Baby Feeding",
    "baby_gear": "Baby Gear",
    "baby_nursery": "Baby Nursery",
    "beauty_tools": "Beauty Tools",
    "bedding_bath": "Bedding & Bath",
    "car_accessories": "Car Accessories",
    "car_care_products": "Car Care Products",
    "car_parts": "Car Parts",
    "desk_accessories": "Desk Accessories",
    "fish_aquatics": "Fish & Aquatics",
    "fitness_equipment": "Fitness Equipment",
    "fresh_food": "Fresh Food",
    "garden": "Garden & Outdoor",
    "health_wellness": "Health & Wellness",
    "home_appliances": "Home Appliances",
    "home_decor": "Home Decor",
    "household_supplies": "Household Supplies",
    "kitchen_appliances": "Kitchen Appliances",
    "mobiles_tablets": "Mobiles & Tablets",
    "office_electronics": "Office Electronics",
    "office_supplies": "Office Supplies",
    "outdoor_recreation": "Outdoor Recreation",
    "pantry_staples": "Pantry Staples",
    "personal_care": "Personal Care",
    "pets": "Pets",
    "skincare": "Skincare",
    "snacks_beverages": "Snacks & Beverages",
    "sports_accessories": "Sports Accessories",
    "sports_clothing": "Sports Clothing",
    "storage_organization": "Storage & Organisation",
    "team_sports": "Team Sports",
    "tools_equipment": "Tools & Equipment",
    "womens_fashion": "Women's Fashion",
    "mens_fashion": "Men's Fashion",
    "kids_fashion": "Kids' Fashion",
}


def _humanize(identifier: str) -> str:
    if identifier in PARENT_NAME_OVERRIDES:
        return PARENT_NAME_OVERRIDES[identifier]
    text = identifier.replace("_", " ").replace("-", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return " ".join(word.capitalize() for word in text.split())


def restore_tree(category_tree_path: Path, scan_dir: Path) -> dict:
    category_tree = json.loads(category_tree_path.read_text(encoding="utf-8"))
    leaf_names: dict[str, str] = {}
    parent_candidates: dict[str, set[str]] = defaultdict(set)

    for snapshot_path in sorted(scan_dir.glob("monitoring/categories/*/category_tree.json")):
        records = json.loads(snapshot_path.read_text(encoding="utf-8"))
        for record in records:
            config_id = record.get("config_id")
            parent_config_id = record.get("parent_config_id")
            leaf_name = record.get("name") or (record.get("expected_path") or [None])[-1]
            if config_id and leaf_name:
                leaf_names[config_id] = leaf_name
            expected_path = record.get("expected_path") or []
            if parent_config_id and len(expected_path) >= 2:
                parent_candidates[parent_config_id].add(expected_path[-2])

    update_count = 0
    for category in category_tree.get("categories", []):
        for child in category.get("children", []):
            new_parent_name = None
            candidates = {item for item in parent_candidates.get(child.get("id"), set()) if item}
            if len(candidates) == 1:
                new_parent_name = next(iter(candidates))
            else:
                new_parent_name = _humanize(child.get("id", ""))
            if new_parent_name and child.get("name_en") != new_parent_name:
                child["name_en"] = new_parent_name
                update_count += 1

            for grandchild in child.get("children", []):
                new_leaf_name = leaf_names.get(grandchild.get("id")) or _humanize(grandchild.get("id", ""))
                if new_leaf_name and grandchild.get("name_en") != new_leaf_name:
                    grandchild["name_en"] = new_leaf_name
                    update_count += 1

    category_tree_path.write_text(json.dumps(category_tree, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "category_tree_path": str(category_tree_path),
        "scan_dir": str(scan_dir),
        "update_count": update_count,
        "leaf_name_count": len(leaf_names),
        "parent_candidate_count": len(parent_candidates),
    }


def main():
    parser = argparse.ArgumentParser(description="从扫描快照恢复更稳妥的 category_tree 展示名")
    parser.add_argument("--category-tree", type=Path, default=DEFAULT_TREE)
    parser.add_argument("--scan-dir", type=Path, required=True)
    args = parser.parse_args()
    payload = restore_tree(args.category_tree, args.scan_dir)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
