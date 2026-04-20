"""
从类目扫描结果构建运行时 category map。

目标：
- 将已验证过的真实 URL、breadcrumb 路径沉淀到 config/runtime_category_map.json
- 让下一轮类目解析优先使用扫描确认过的平台真实路径
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TREE = ROOT / "config" / "category_tree.json"
DEFAULT_OUTPUT = ROOT / "config" / "runtime_category_map.json"


RUNTIME_SOURCE_URL_OVERRIDES = {
    "baby": {
        "baby_food": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/grocery-store/baby-care-food/baby-foods/"
                "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/grocery-store/baby-care-food/baby-foods/"
                    "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
        },
        # Noon 前台这个节点用上级 monitors 页能拿到更完整的商品池，
        # /baby-monitors/ 深页会被平台收得过窄。
        "baby_monitors": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/baby-products/safety-17316/monitors-18094/"
                "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/baby-products/safety-17316/monitors-18094/"
                    "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
                (
                    "https://www.noon.com/saudi-en/baby-products/safety-17316/monitors-18094/"
                    "?q=baby+monitors&sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
        }
    },
    "grocery": {
        "cooking_oils": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/grocery-store/cooking-and-baking-supplies/"
                "oils-vinegars-and-salad-dressings/oils/?sort%5Bby%5D=popularity"
                "&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/grocery-store/cooking-and-baking-supplies/"
                    "oils-vinegars-and-salad-dressings/oils/?sort%5Bby%5D=popularity"
                    "&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
        },
        "laundry_supplies": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/grocery-store/home-care-and-cleaning/"
                "grocery-laundry-care/?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc"
                "&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/grocery-store/home-care-and-cleaning/"
                    "grocery-laundry-care/?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc"
                    "&limit=50&isCarouselView=false"
                ),
            ],
        },
        "snacks": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/grocery-store/snack-foods/savoury_snacks/"
                "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/grocery-store/snack-foods/savoury_snacks/"
                    "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
        },
        "medical_supplies": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/health/medical-supplies-and-equipment/"
                "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/health/medical-supplies-and-equipment/"
                    "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
        },
    },
    "home_kitchen": {
        "refrigerators": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/home-and-kitchen/home-appliances-31235/"
                "large-appliances/refrigerators-and-freezers/refrigerators/"
                "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/home-and-kitchen/home-appliances-31235/"
                    "large-appliances/refrigerators-and-freezers/refrigerators/"
                    "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
        },
    },
    "pets": {
        "cat_litter": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/pet-supplies/cats-16737/health-supplies-16738/"
                "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/pet-supplies/cats-16737/health-supplies-16738/"
                    "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
            "expected_path": ["Pet Supplies", "Cat Supplies", "Cat Health Supplies"],
        },
        "pet_clippers": {
            "resolved_url": (
                "https://www.noon.com/saudi-en/pet-supplies/dogs-16275/grooming-23314/"
                "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
            ),
            "source_urls": [
                (
                    "https://www.noon.com/saudi-en/pet-supplies/dogs-16275/grooming-23314/"
                    "?sort%5Bby%5D=popularity&sort%5Bdir%5D=desc&limit=50&isCarouselView=false"
                ),
            ],
            "expected_path": ["Pet Supplies", "Dog Supplies", "Dog Grooming"],
            "display_name": "Dog Grooming",
        },
    },
}


def _normalize(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower().replace("&", " and ")
    normalized = re.sub(r"\ball\b", " ", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _tokens(*values: str) -> set[str]:
    stopwords = {"all", "and", "for", "the", "with", "of", "saudi", "en", "store", "noon"}
    tokens: set[str] = set()
    for value in values:
        for token in _normalize(value).split():
            if len(token) >= 2 and token not in stopwords:
                tokens.add(token)
    return tokens


def _score_name(query: str, candidate_values: list[str]) -> int:
    query_norm = _normalize(query)
    if not query_norm:
        return 0
    best = 0
    query_tokens = _tokens(query_norm)
    for value in candidate_values:
        value_norm = _normalize(value)
        if not value_norm:
            continue
        score = 0
        if query_norm == value_norm:
            score += 120
        elif query_norm in value_norm or value_norm in query_norm:
            score += 50
        overlap = len(query_tokens & _tokens(value_norm))
        score += overlap * 20
        score += int(SequenceMatcher(None, query_norm, value_norm).ratio() * 40)
        best = max(best, score)
    return best


def _dedupe_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        ordered.append(url)
    return ordered


def _preferred_filter_candidates(record: dict, node: dict) -> list[dict]:
    strict_leaf_norms = {
        _normalize(value)
        for value in [
            record.get("subcategory", ""),
            node.get("config_name", ""),
            (record.get("expected_path") or [])[-1] if record.get("expected_path") else "",
        ]
        if value
    }
    desired_leaf = [
        record.get("subcategory", ""),
        node.get("config_name", ""),
        (record.get("expected_path") or [])[-1] if record.get("expected_path") else "",
        (record.get("breadcrumb_items") or [])[-1] if record.get("breadcrumb_items") else "",
    ]
    desired_parent = [
        node.get("parent_name", ""),
        (record.get("expected_path") or [])[-2] if len(record.get("expected_path") or []) >= 2 else "",
    ]

    candidates: list[dict] = []
    for link in record.get("category_filter_links") or []:
        text = (link.get("text") or "").strip()
        url = (link.get("url") or "").strip()
        if not text or not url:
            continue
        bare_text = re.sub(r"^all\s+", "", text, flags=re.I).strip()
        bare_norm = _normalize(bare_text or text)
        exact_leaf_match = False
        score = _score_name(bare_text or text, desired_leaf)
        score += _score_name(text, desired_leaf) // 2
        score += _score_name(bare_text or text, desired_parent) // 4
        if bare_norm in strict_leaf_norms:
            score += 140
            exact_leaf_match = True
        elif bare_norm.rstrip("s") in {item.rstrip("s") for item in strict_leaf_norms}:
            score += 110
            exact_leaf_match = True
        elif text.lower().startswith("all "):
            score -= 20
        if score < 80:
            continue
        depth = len([part for part in url.split("/") if part])
        candidates.append(
            {
                "text": text,
                "bare_text": bare_text or text,
                "url": url,
                "score": score,
                "depth": depth,
                "exact_leaf_match": exact_leaf_match,
            }
        )

    exact_candidates = [item for item in candidates if item["exact_leaf_match"]]
    if exact_candidates:
        candidates = exact_candidates
    candidates.sort(key=lambda item: (item["score"], item["depth"]), reverse=True)
    if not candidates:
        return []

    best_score = candidates[0]["score"]
    return [
        item
        for item in candidates
        if item["score"] >= max(100, best_score - 12)
    ][:3]


def _derive_expected_path_from_filter(record: dict, selected_text: str) -> list[str]:
    if not selected_text:
        return []
    target_norm = _normalize(re.sub(r"^all\s+", "", selected_text, flags=re.I).strip())
    if not target_norm:
        return []

    ordered: list[str] = []
    seen: set[str] = set()
    for text in record.get("category_filter_texts") or []:
        cleaned = re.sub(r"^all\s+", "", text or "", flags=re.I).strip()
        normalized = _normalize(cleaned)
        if not cleaned or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(cleaned)
        if normalized == target_norm:
            return ordered
    return []


def _iter_leaf_nodes(category_tree: dict) -> dict[str, list[dict]]:
    nodes_by_category: dict[str, list[dict]] = {}
    for category in category_tree.get("categories", []):
        cat_id = category.get("id")
        nodes: list[dict] = []
        for child in category.get("children", []):
            grandchildren = child.get("children", [])
            if grandchildren:
                for grandchild in grandchildren:
                    nodes.append(
                        {
                            "config_id": grandchild.get("id"),
                            "config_name": grandchild.get("name_en"),
                            "parent_config_id": child.get("id"),
                            "parent_name": child.get("name_en"),
                            "variants": [
                                grandchild.get("name_en", ""),
                                grandchild.get("id", ""),
                                (grandchild.get("id", "") or "").replace("_", " "),
                            ],
                            "parent_variants": [
                                child.get("name_en", ""),
                                child.get("id", ""),
                                (child.get("id", "") or "").replace("_", " "),
                            ],
                        }
                    )
            else:
                nodes.append(
                    {
                        "config_id": child.get("id"),
                        "config_name": child.get("name_en"),
                        "parent_config_id": None,
                        "parent_name": None,
                        "variants": [
                            child.get("name_en", ""),
                            child.get("id", ""),
                            (child.get("id", "") or "").replace("_", " "),
                        ],
                        "parent_variants": [],
                    }
                )
        nodes_by_category[cat_id] = nodes
    return nodes_by_category


def _match_record_to_node(record: dict, nodes: list[dict]) -> dict | None:
    desired_name = (record.get("expected_path") or [])[-1] if record.get("expected_path") else record.get("subcategory")
    desired_parent = (record.get("expected_path") or [])[-2] if len(record.get("expected_path") or []) >= 2 else None

    best = None
    best_score = -1
    for node in nodes:
        score = _score_name(desired_name, node["variants"])
        if desired_parent:
            score += _score_name(desired_parent, node["parent_variants"]) // 2
        if record.get("subcategory"):
            score += _score_name(record["subcategory"], node["variants"]) // 3
        if score > best_score:
            best_score = score
            best = node

    if best and best_score >= 70:
        return {**best, "match_score": best_score}
    return None


def _load_scan_records(scan_dir: Path) -> list[dict]:
    records = []
    for sub_file in sorted(scan_dir.glob("monitoring/categories/*/subcategory/*.json")):
        data = json.loads(sub_file.read_text(encoding="utf-8"))
        records.append(
            {
                "internal_category": sub_file.parents[1].name,
                "subcategory": data.get("subcategory"),
                "product_count": data.get("product_count", 0),
                "resolved_url": data.get("resolved_url") or data.get("url"),
                "source_urls": data.get("source_urls", []),
                "config_id": data.get("config_id"),
                "parent_config_id": data.get("parent_config_id"),
                "expected_path": data.get("expected_path", []),
                "platform_nav_path": data.get("platform_nav_path", []),
                "breadcrumb_items": data.get("breadcrumb_items", []),
                "breadcrumb_path": data.get("breadcrumb_path", ""),
                "breadcrumb_match_status": data.get("breadcrumb_match_status", "unknown"),
                "category_filter_texts": data.get("category_filter_texts", []),
                "category_filter_links": data.get("category_filter_links", []),
                "raw_count": data.get("raw_count", 0),
                "deduped_count": data.get("deduped_count", data.get("product_count", 0)),
                "source_runs": data.get("source_runs", []),
            }
        )
    return records


def build_runtime_map(scan_dir: Path, category_tree_path: Path) -> dict:
    category_tree = json.loads(category_tree_path.read_text(encoding="utf-8"))
    nodes_by_category = _iter_leaf_nodes(category_tree)
    records = _load_scan_records(scan_dir)

    categories: dict[str, dict] = {}
    unmapped: list[dict] = []

    for record in records:
        effective_product_count = max(
            record.get("product_count", 0),
            record.get("raw_count", 0),
            max((run.get("product_count", 0) for run in record.get("source_runs") or []), default=0),
        )
        if (
            effective_product_count <= 0
            and not record.get("breadcrumb_items")
            and not record.get("category_filter_links")
        ):
            continue
        if not record.get("resolved_url"):
            continue

        nodes = nodes_by_category.get(record["internal_category"], [])
        match = None
        exact_config_id = record.get("config_id")
        if exact_config_id:
            match = next((node for node in nodes if node.get("config_id") == exact_config_id), None)
            if match:
                match = {**match, "match_score": 9999}
        if not match:
            match = _match_record_to_node(record, nodes)
        if not match:
            unmapped.append(record)
            continue

        category_bucket = categories.setdefault(record["internal_category"], {})
        preferred_filters = _preferred_filter_candidates(record, match)
        source_urls = [item["url"] for item in preferred_filters] if preferred_filters else []
        if record.get("resolved_url"):
            source_urls.append(record["resolved_url"])
        source_urls.extend(record.get("source_urls") or [])
        source_urls = _dedupe_urls(source_urls)

        expected_path = [item for item in record.get("breadcrumb_items", []) if item and _normalize(item) != "home"]
        if preferred_filters:
            filter_path = _derive_expected_path_from_filter(record, preferred_filters[0]["text"])
            if not expected_path:
                expected_path = filter_path
            elif (
                preferred_filters[0].get("exact_leaf_match")
                and _normalize(preferred_filters[0]["bare_text"]) != _normalize(expected_path[-1] if expected_path else "")
            ):
                expected_path = [*expected_path, preferred_filters[0]["bare_text"]]
        if not expected_path:
            expected_path = record.get("expected_path", [])

        candidate = {
            "config_id": match["config_id"],
            "parent_config_id": match["parent_config_id"],
            "config_name": match["config_name"],
            "parent_name": match["parent_name"],
            "subcategory_name": record.get("subcategory"),
            "display_name": (expected_path[-1] if expected_path else None) or record.get("subcategory") or match["config_name"],
            "resolved_url": source_urls[0] if source_urls else record.get("resolved_url"),
            "source_urls": source_urls,
            "expected_path": expected_path,
            "platform_nav_path": record.get("platform_nav_path", []),
            "breadcrumb_path": record.get("breadcrumb_path", ""),
            "breadcrumb_match_status": record.get("breadcrumb_match_status", "unknown"),
            "category_filter_texts": record.get("category_filter_texts", []),
            "category_filter_links": record.get("category_filter_links", []),
            "product_count": effective_product_count,
            "match_score": match["match_score"],
        }

        source_override = (
            RUNTIME_SOURCE_URL_OVERRIDES.get(record["internal_category"], {}).get(match["config_id"])
        )
        if source_override:
            override_urls = _dedupe_urls(
                [
                    *(source_override.get("source_urls") or []),
                    *(candidate.get("source_urls") or []),
                ]
            )
            resolved_url = source_override.get("resolved_url") or candidate.get("resolved_url")
            if resolved_url:
                override_urls = _dedupe_urls([resolved_url, *override_urls])
                candidate["resolved_url"] = resolved_url
            candidate["source_urls"] = override_urls
            if source_override.get("expected_path"):
                candidate["expected_path"] = source_override["expected_path"]
            if source_override.get("display_name"):
                candidate["display_name"] = source_override["display_name"]

        existing = category_bucket.get(match["config_id"])
        if not existing or candidate["product_count"] > existing.get("product_count", 0):
            category_bucket[match["config_id"]] = candidate

    return {
        "generated_at": __import__("datetime").datetime.now().isoformat(),
        "source_scan_dir": str(scan_dir),
        "source_category_tree": str(category_tree_path),
        "categories": categories,
        "unmapped_count": len(unmapped),
        "unmapped_examples": unmapped[:20],
    }


def main():
    parser = argparse.ArgumentParser(description="构建运行时 category map")
    parser.add_argument("--scan-dir", type=Path, required=True, help="batch scan 输出目录")
    parser.add_argument("--category-tree", type=Path, default=DEFAULT_TREE, help="内部 category_tree.json")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="输出 runtime category map")
    args = parser.parse_args()

    payload = build_runtime_map(args.scan_dir, args.category_tree)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
