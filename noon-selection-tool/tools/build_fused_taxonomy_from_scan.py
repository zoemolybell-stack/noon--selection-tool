"""
从类目扫描结果中构建融合 taxonomy。

数据来源：
- 首页导航树
- 子类目页 breadcrumb
- 左侧 CategoryFilter
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOME_NAV = ROOT / "config" / "noon_home_navigation_saudi_en.json"


def _add_path(tree: dict, breadcrumb_items: list[str], leaf_record: dict):
    if not breadcrumb_items:
        return
    node = tree
    for item in breadcrumb_items:
        children = node.setdefault("children", {})
        node = children.setdefault(
            item,
            {
                "name": item,
                "children": {},
                "leaf_refs": [],
            },
        )
    node.setdefault("leaf_refs", []).append(
        {
            "internal_category": leaf_record.get("internal_category"),
            "subcategory": leaf_record.get("subcategory"),
            "resolved_url": leaf_record.get("resolved_url"),
            "product_count": leaf_record.get("product_count"),
            "breadcrumb_match_status": leaf_record.get("breadcrumb_match_status"),
        }
    )


def _tree_to_list(node: dict) -> list[dict]:
    results = []
    for child in sorted(node.get("children", {}).values(), key=lambda item: item["name"]):
        results.append(
            {
                "name": child["name"],
                "leaf_refs": child.get("leaf_refs", []),
                "children": _tree_to_list(child),
            }
        )
    return results


def _load_subcategory_records(scan_dir: Path) -> list[dict]:
    records = []
    for sub_file in sorted(scan_dir.glob("monitoring/categories/*/subcategory/*.json")):
        data = json.loads(sub_file.read_text(encoding="utf-8"))
        internal_category = sub_file.parents[1].name
        breadcrumb_items = data.get("breadcrumb_items") or []
        records.append(
            {
                "internal_category": internal_category,
                "subcategory": data.get("subcategory"),
                "product_count": data.get("product_count", 0),
                "resolved_url": data.get("resolved_url") or data.get("url"),
                "expected_path": data.get("expected_path", []),
                "platform_nav_path": data.get("platform_nav_path", []),
                "breadcrumb_items": breadcrumb_items,
                "breadcrumb_path": data.get("breadcrumb_path", ""),
                "breadcrumb_match_status": data.get("breadcrumb_match_status", "unknown"),
                "category_filter_texts": data.get("category_filter_texts", []),
                "category_filter_links": data.get("category_filter_links", []),
                "source_file": str(sub_file),
            }
        )
    return records


def build_payload(scan_dir: Path, home_nav_path: Path) -> tuple[dict, dict]:
    home_nav = json.loads(home_nav_path.read_text(encoding="utf-8")) if home_nav_path.exists() else {}
    records = _load_subcategory_records(scan_dir)

    platform_tree: dict = {"children": {}}
    gaps_by_category: dict[str, dict] = defaultdict(
        lambda: {
            "subcategory_count": 0,
            "matched": 0,
            "partial": 0,
            "mismatch": 0,
            "missing": 0,
            "missing_category_filter": 0,
            "platform_roots": set(),
        }
    )

    for record in records:
        breadcrumb_items = record.get("breadcrumb_items") or []
        if breadcrumb_items:
            _add_path(platform_tree, breadcrumb_items, record)

        bucket = gaps_by_category[record["internal_category"]]
        bucket["subcategory_count"] += 1
        bucket[record.get("breadcrumb_match_status", "unknown")] = bucket.get(record.get("breadcrumb_match_status", "unknown"), 0) + 1
        if not record.get("category_filter_links"):
            bucket["missing_category_filter"] += 1
        if len(breadcrumb_items) >= 2:
            bucket["platform_roots"].add(breadcrumb_items[1])

    payload = {
        "generated_at": datetime.now().isoformat(),
        "source_scan_dir": str(scan_dir),
        "source_home_navigation": str(home_nav_path),
        "home_navigation_snapshot": {
            "top_category_count": home_nav.get("top_category_count", 0),
            "group_count": home_nav.get("group_count", 0),
            "leaf_count": home_nav.get("leaf_count", 0),
        },
        "summary": {
            "subcategory_records": len(records),
            "breadcrumb_covered": sum(1 for item in records if item.get("breadcrumb_items")),
            "category_filter_covered": sum(1 for item in records if item.get("category_filter_links")),
        },
        "platform_tree": _tree_to_list(platform_tree),
        "leaf_records": records,
    }

    gap_report = {
        "generated_at": payload["generated_at"],
        "source_scan_dir": str(scan_dir),
        "categories": [
            {
                "internal_category": category,
                "subcategory_count": stats["subcategory_count"],
                "matched": stats.get("matched", 0),
                "partial": stats.get("partial", 0),
                "mismatch": stats.get("mismatch", 0),
                "missing": stats.get("missing", 0),
                "missing_category_filter": stats.get("missing_category_filter", 0),
                "platform_roots": sorted(stats["platform_roots"]),
            }
            for category, stats in sorted(gaps_by_category.items())
        ],
    }
    return payload, gap_report


def main():
    parser = argparse.ArgumentParser(description="从扫描结果构建 fused taxonomy")
    parser.add_argument("--scan-dir", type=Path, required=True, help="batch scan 输出目录")
    parser.add_argument("--home-nav", type=Path, default=DEFAULT_HOME_NAV, help="首页导航 JSON")
    parser.add_argument("--output", type=Path, default=None, help="融合 taxonomy 输出路径")
    parser.add_argument("--gap-report", type=Path, default=None, help="缺口报告输出路径")
    args = parser.parse_args()

    output_path = args.output or (args.scan_dir / "fused_platform_taxonomy.json")
    gap_path = args.gap_report or (args.scan_dir / "taxonomy_gap_report.json")

    payload, gap_report = build_payload(args.scan_dir, args.home_nav)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    gap_path.write_text(json.dumps(gap_report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(output_path)
    print(gap_path)


if __name__ == "__main__":
    main()
