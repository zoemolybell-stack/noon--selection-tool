import argparse
import asyncio
import json
import logging
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.product_store import ProductStore
from output.crawl_data_exporter import export_crawl_data
from scrapers.noon_category_crawler import NoonCategoryCrawler


ROOT = Path(__file__).parent
DEFAULT_OUTPUT_ROOT = ROOT / "data" / "validation_suites"
DEFAULT_DB_PATH = ROOT / "data" / "product_store.db"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("category-validation-suite")


VALIDATION_CASES = [
    {
        "id": "electronics_smartphones",
        "category": "electronics",
        "name": "Smartphones",
        "url": "https://www.noon.com/saudi-en/electronics-and-mobiles/mobiles-and-accessories/mobiles-20905/smartphones/",
        "expected_path": "Home > Electronics & Mobiles > Mobiles & Accessories > Mobile Phones > Smartphones",
        "allow_empty": False,
    },
    {
        "id": "fashion_womens_dresses",
        "category": "fashion",
        "name": "Women's Dresses",
        "url": "https://www.noon.com/saudi-en/fashion/women-31229/clothing-16021/dresses-17612/",
        "expected_path": "Home > Fashion > Women's Fashion > Women's Clothing > Women's Dresses",
        "allow_empty": False,
    },
    {
        "id": "baby_baby_monitors",
        "category": "baby",
        "name": "Baby Monitors",
        "url": "https://www.noon.com/saudi-en/baby-products/safety-17316/monitors-18094/",
        "expected_path": "Home > Baby Products > Safety Equipment > Baby Monitors",
        "allow_empty": False,
    },
    {
        "id": "beauty_face_care",
        "category": "beauty",
        "name": "Face Care",
        "url": "https://www.noon.com/saudi-en/beauty/skin-care-16813/face-17406/",
        "expected_path": "Home > Beauty & Fragrance > Skin Care > Face Care",
        "allow_empty": True,
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行 Noon 类目爬虫回归验证套件")
    parser.add_argument(
        "--cases",
        type=str,
        default=None,
        help="逗号分隔的 case id；不传则运行全部用例",
    )
    parser.add_argument(
        "--product-count",
        type=int,
        default=20,
        help="每个验证用例抓取的商品上限",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="输出目录；默认 data/validation_suites/category_validation_<timestamp>",
    )
    parser.add_argument(
        "--no-export-excel",
        action="store_true",
        help="只输出 JSON/Markdown，不导出 Excel",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="将验证样本持久化到 SQLite 历史库",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite 数据库路径，默认写入共享类目库",
    )
    return parser.parse_args()


def select_cases(cases_arg: str | None) -> list[dict]:
    if not cases_arg:
        return list(VALIDATION_CASES)

    requested = {item.strip() for item in cases_arg.split(",") if item.strip()}
    selected = [case for case in VALIDATION_CASES if case["id"] in requested]
    missing = requested - {case["id"] for case in selected}
    if missing:
        raise SystemExit(f"未找到验证用例: {', '.join(sorted(missing))}")
    return selected


def summarize_products(products: list[dict]) -> dict:
    delivery_counts = Counter((product.get("delivery_type") or "blank") for product in products)
    signal_source_counts = Counter(product.get("signal_source") or "blank" for product in products)
    ambiguous_delivery_count = 0
    rapid_eta_blank_count = 0

    for product in products:
        has_delivery_signal = bool(product.get("delivery_marker_texts")) or bool(product.get("delivery_signal_texts"))
        if not product.get("delivery_type") and has_delivery_signal:
            ambiguous_delivery_count += 1
            if "get in" in (product.get("delivery_eta_signal_text") or "").lower():
                rapid_eta_blank_count += 1

    return {
        "product_count": len(products),
        "delivery_type_counts": dict(delivery_counts),
        "signal_source_counts": dict(signal_source_counts),
        "raw_signal_coverage": sum(1 for product in products if product.get("raw_signal_texts")),
        "delivery_marker_coverage": sum(1 for product in products if product.get("delivery_marker_texts")),
        "ad_marker_coverage": sum(1 for product in products if product.get("ad_marker_texts")),
        "delivery_eta_count": sum(1 for product in products if product.get("delivery_eta_signal_text")),
        "lowest_price_count": sum(1 for product in products if product.get("lowest_price_signal_text")),
        "stock_signal_count": sum(1 for product in products if product.get("stock_signal_text")),
        "sold_recently_count": sum(1 for product in products if product.get("sold_recently_text")),
        "is_ad_count": sum(1 for product in products if product.get("is_ad")),
        "ambiguous_delivery_count": ambiguous_delivery_count,
        "rapid_eta_blank_count": rapid_eta_blank_count,
    }


def build_case_status(case: dict, result: dict, summary: dict) -> str:
    actual_path = result.get("effective_category_path") or result.get("breadcrumb", {}).get("path", "")
    if case.get("expected_path") and actual_path != case["expected_path"]:
        return "attention"
    if not case.get("allow_empty") and summary["product_count"] <= 0:
        return "attention"
    if summary.get("ambiguous_delivery_count", 0) > 0:
        return "attention"
    return "pass"


def build_product_examples(products: list[dict]) -> list[dict]:
    examples: list[dict] = []
    for product in products[:5]:
        examples.append(
            {
                "title": product.get("title", ""),
                "delivery_type": product.get("delivery_type", ""),
                "delivery_marker_texts": product.get("delivery_marker_texts", []),
                "delivery_signal_texts": product.get("delivery_signal_texts", []),
                "delivery_eta_signal_text": product.get("delivery_eta_signal_text", ""),
                "ad_marker_texts": product.get("ad_marker_texts", []),
                "is_ad": product.get("is_ad", False),
                "lowest_price_signal_text": product.get("lowest_price_signal_text", ""),
                "stock_signal_text": product.get("stock_signal_text", ""),
                "sold_recently_text": product.get("sold_recently_text", ""),
            }
        )
    return examples


def render_markdown(summary: dict) -> str:
    lines = [
        "# Category Validation Suite",
        "",
        f"- Generated at: {summary['generated_at']}",
        f"- Product count per case: {summary['product_count_per_case']}",
        "",
        "## Case Summary",
    ]

    for case in summary["cases"]:
        lines.append(
            f"- `{case['id']}`: status={case['status']}, products={case['metrics']['product_count']}, "
            f"path={case['actual_path'] or '-'}"
        )
        lines.append(
            f"  delivery={case['metrics']['delivery_type_counts']}, raw_signal={case['metrics']['raw_signal_coverage']}, "
            f"delivery_marker={case['metrics']['delivery_marker_coverage']}, delivery_eta={case['metrics']['delivery_eta_count']}, "
            f"lowest_price={case['metrics']['lowest_price_count']}, stock={case['metrics']['stock_signal_count']}, "
            f"sold_recently={case['metrics']['sold_recently_count']}, ad={case['metrics']['is_ad_count']}, "
            f"ambiguous_delivery={case['metrics']['ambiguous_delivery_count']}"
        )

    attention_cases = [case for case in summary["cases"] if case["status"] != "pass"]
    lines.extend(["", "## Attention"])
    if attention_cases:
        for case in attention_cases:
            lines.append(
                f"- `{case['id']}`: expected_path={case['expected_path'] or '-'} | actual_path={case['actual_path'] or '-'} | "
                f"ambiguous_delivery={case['metrics']['ambiguous_delivery_count']} | rapid_eta_blank={case['metrics']['rapid_eta_blank_count']}"
            )
    else:
        lines.append("- All validation cases passed the current checks.")

    lines.extend(["", "## Example Signals"])
    for case in summary["cases"]:
        lines.append(f"- `{case['id']}`")
        for example in case["examples"][:2]:
            lines.append(
                f"  - {example['title']} | delivery={example['delivery_type'] or '-'} | "
                f"eta={example['delivery_eta_signal_text'] or '-'} | lowest={example['lowest_price_signal_text'] or '-'} | stock={example['stock_signal_text'] or '-'} | "
                f"sold={example['sold_recently_text'] or '-'} | ad={example['is_ad']}"
            )

    return "\n".join(lines) + "\n"


async def run_case(case: dict, output_dir: Path, product_count: int) -> dict:
    crawler = NoonCategoryCrawler(case["category"], output_dir, max_products_per_sub=product_count)
    await crawler._start_browser()
    try:
        result = await crawler.scrape_subcategory(
            case["url"],
            case["name"],
            expected_path=(case.get("expected_path") or "").split(" > ") if case.get("expected_path") else None,
        )
    finally:
        await crawler._stop_browser()

    meta = {
        "url": case["url"],
        "requested_url": result.get("requested_url", case["url"]),
        "resolved_url": result.get("resolved_url", ""),
        "effective_category_path": result.get("effective_category_path", ""),
        "breadcrumb": result.get("breadcrumb", {}),
        "category_filter": result.get("category_filter", {}),
        "breadcrumb_match_status": result.get("breadcrumb_match_status", ""),
    }
    crawler._save_subcategory(case["name"], result.get("products", []), meta)

    products = result.get("products", [])
    metrics = summarize_products(products)
    actual_path = result.get("effective_category_path") or result.get("breadcrumb", {}).get("path", "")

    return {
        "id": case["id"],
        "category": case["category"],
        "subcategory": case["name"],
        "status": build_case_status(case, result, metrics),
        "expected_path": case.get("expected_path", ""),
        "actual_path": actual_path,
        "requested_url": case["url"],
        "resolved_url": result.get("resolved_url", ""),
        "breadcrumb_match_status": result.get("breadcrumb_match_status", ""),
        "allow_empty": bool(case.get("allow_empty")),
        "metrics": metrics,
        "examples": build_product_examples(products),
    }


async def run_suite(cases: list[dict], output_dir: Path, product_count: int, export_excel: bool) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now().isoformat()
    case_summaries: list[dict] = []

    for index, case in enumerate(cases, 1):
        logger.info("=== [%s/%s] 运行验证用例 %s ===", index, len(cases), case["id"])
        case_summaries.append(await run_case(case, output_dir, product_count))

    exports: dict[str, str] = {}
    if export_excel:
        exports_dir = output_dir / "exports"
        exports_dir.mkdir(parents=True, exist_ok=True)
        for category in sorted({case["category"] for case in cases}):
            category_dir = output_dir / "monitoring" / "categories" / category
            if not category_dir.exists():
                continue
            category_product_count = sum(
                case_summary["metrics"]["product_count"]
                for case_summary in case_summaries
                if case_summary["category"] == category
            )
            if category_product_count <= 0:
                logger.info("跳过空类目 Excel 导出: %s", category)
                continue
            excel_path = exports_dir / f"validation_{category}_{product_count}.xlsx"
            try:
                export_crawl_data([category_dir], excel_path, ["noon"], mode="category")
                exports[category] = str(excel_path)
            except Exception as exc:
                logger.warning("导出验证 Excel 失败 %s: %s", category, exc)

    summary = {
        "generated_at": started_at,
        "product_count_per_case": product_count,
        "output_dir": str(output_dir),
        "exports": exports,
        "cases": case_summaries,
    }
    return summary


def persist_validation_suite(output_dir: Path, cases: list[dict], db_path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    store = ProductStore(db_path)
    try:
        snapshot_id = output_dir.name
        for category in sorted({case["category"] for case in cases}):
            category_dir = output_dir / "monitoring" / "categories" / category
            if not category_dir.exists():
                continue
            counts[category] = store.import_category_directory(
                category_dir,
                platform="noon",
                snapshot_id=snapshot_id,
                category_name=category,
            )
    finally:
        store.close()
    return counts


def main() -> None:
    args = parse_args()
    cases = select_cases(args.cases)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or (DEFAULT_OUTPUT_ROOT / f"category_validation_{timestamp}")

    summary = asyncio.run(
        run_suite(
            cases=cases,
            output_dir=output_dir,
            product_count=args.product_count,
            export_excel=not args.no_export_excel,
        )
    )

    if args.persist:
        summary["persist_counts"] = persist_validation_suite(output_dir, cases, args.db_path)
        summary["db_path"] = str(args.db_path)

    summary_path = output_dir / "validation_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    markdown_path = output_dir / "validation_summary.md"
    markdown_path.write_text(render_markdown(summary), encoding="utf-8")

    logger.info("验证套件完成: %s", summary_path)
    logger.info("验证摘要: %s", markdown_path)


if __name__ == "__main__":
    main()
