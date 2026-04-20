from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scrapers.noon_category_crawler import NoonCategoryCrawler


DEFAULT_SORT_QUERY = {
    "sort[by]": "popularity",
    "sort[dir]": "desc",
    "limit": "50",
    "isCarouselView": "false",
}
SUSPICIOUS_URL_TOKENS = ("megadeals", "noon-deals", "noon-supermarket")


def _normalize_probe_url(url: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in DEFAULT_SORT_QUERY.items():
        query.setdefault(key, value)
    path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            path,
            parsed.params,
            urlencode(query),
            "",
        )
    )


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _build_parent_candidate(url: str) -> str | None:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return None
    parent_parts = parts[:-1]
    if len(parent_parts) < 2:
        return None
    parent_path = "/" + "/".join(parent_parts) + "/"
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in DEFAULT_SORT_QUERY.items():
        query.setdefault(key, value)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parent_path,
            parsed.params,
            urlencode(query),
            "",
        )
    )


def _load_records(scan_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for sub_file in sorted(scan_dir.glob("monitoring/categories/*/subcategory/*.json")):
        data = json.loads(sub_file.read_text(encoding="utf-8"))
        records.append(
            {
                "path": sub_file,
                "internal_category": sub_file.parents[1].name,
                "subcategory": data.get("subcategory"),
                "product_count": data.get("product_count", 0),
                "resolved_url": data.get("resolved_url") or data.get("url"),
                "url": data.get("url"),
                "source_urls": data.get("source_urls") or [],
                "expected_path": data.get("expected_path") or [],
                "breadcrumb_path": data.get("breadcrumb_path", ""),
            }
        )
    return records


def _is_suspicious(record: dict[str, Any], threshold: int) -> bool:
    if record.get("product_count", 0) < threshold:
        return True
    url = (record.get("resolved_url") or record.get("url") or "").lower()
    return any(token in url for token in SUSPICIOUS_URL_TOKENS)


def _candidate_urls(record: dict[str, Any]) -> list[str]:
    seed_urls = [
        record.get("resolved_url"),
        record.get("url"),
        *(record.get("source_urls") or []),
    ]
    normalized = [_normalize_probe_url(url) for url in seed_urls if url]
    parent_candidates = [_build_parent_candidate(url) for url in normalized]
    return _dedupe([*normalized, *[url for url in parent_candidates if url]])


async def _probe_record(record: dict[str, Any], output_dir: Path, max_products: int) -> dict[str, Any]:
    crawler = NoonCategoryCrawler(
        record["internal_category"],
        output_dir,
        max_products_per_sub=max_products,
    )
    results: list[dict[str, Any]] = []
    await crawler._start_browser()
    try:
        for candidate_url in _candidate_urls(record):
            try:
                scrape_result = await crawler.scrape_subcategory(
                    candidate_url,
                    record["subcategory"],
                    expected_path=record.get("expected_path") or None,
                )
                breadcrumb = scrape_result.get("breadcrumb") or {}
                results.append(
                    {
                        "requested_url": candidate_url,
                        "resolved_url": scrape_result.get("resolved_url", candidate_url),
                        "breadcrumb_path": breadcrumb.get("path", ""),
                        "breadcrumb_items": breadcrumb.get("items", []),
                        "match_status": scrape_result.get("breadcrumb_match_status"),
                        "product_count": len(scrape_result.get("products") or []),
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "requested_url": candidate_url,
                        "error": str(exc),
                    }
                )
    finally:
        await crawler._stop_browser()

    matched = [item for item in results if item.get("match_status") == "matched"]
    best = max(matched, key=lambda item: item.get("product_count", 0), default=None)
    baseline_url = _normalize_probe_url(record.get("resolved_url") or record.get("url") or "")
    baseline = next((item for item in results if item.get("requested_url") == baseline_url), None)
    improved = (
        bool(best and baseline)
        and best.get("product_count", 0) > baseline.get("product_count", 0)
        and best.get("resolved_url") != baseline.get("resolved_url")
    )
    return {
        "internal_category": record["internal_category"],
        "subcategory": record["subcategory"],
        "expected_path": record.get("expected_path") or [],
        "baseline": baseline,
        "best": best,
        "improved": improved,
        "all_results": results,
    }


async def audit_scan(scan_dir: Path, output_dir: Path, threshold: int, max_products: int) -> dict[str, Any]:
    records = _load_records(scan_dir)
    suspicious = [record for record in records if _is_suspicious(record, threshold)]
    reports: list[dict[str, Any]] = []

    for index, record in enumerate(suspicious, 1):
        print(f"[{index}/{len(suspicious)}] probing {record['internal_category']} / {record['subcategory']}")
        probe_dir = output_dir / "probes" / record["internal_category"]
        probe_dir.mkdir(parents=True, exist_ok=True)
        reports.append(await _probe_record(record, probe_dir, max_products))

    improved = [item for item in reports if item.get("improved")]
    payload = {
        "generated_at": datetime.now().isoformat(),
        "scan_dir": str(scan_dir),
        "threshold": threshold,
        "max_products": max_products,
        "suspicious_count": len(suspicious),
        "improved_count": len(improved),
        "reports": reports,
    }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="审计同 breadcrumb 下是否存在更完整的类目节点页")
    parser.add_argument("--scan-dir", type=Path, required=True, help="batch scan 输出目录")
    parser.add_argument("--output-dir", type=Path, required=True, help="审计输出目录")
    parser.add_argument("--threshold", type=int, default=45, help="低于该 unique 数量的节点进入审计")
    parser.add_argument("--max-products", type=int, default=50, help="每个候选 URL 复查的商品上限")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    payload = asyncio.run(
        audit_scan(
            scan_dir=args.scan_dir,
            output_dir=args.output_dir,
            threshold=args.threshold,
            max_products=args.max_products,
        )
    )
    output_path = args.output_dir / "category_node_breadth_audit.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
