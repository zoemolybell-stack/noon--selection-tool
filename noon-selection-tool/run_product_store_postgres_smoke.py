from __future__ import annotations

import argparse
import os
import uuid
from pathlib import Path

from config.product_store import ProductStore
from db.postgres import ensure_postgres_database_exists


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test ProductStore against PostgreSQL.")
    parser.add_argument(
        "--postgres-dsn",
        required=True,
        help="PostgreSQL DSN for ProductStore, e.g. postgresql://user:pass@localhost:5433/noon_stage_smoke",
    )
    parser.add_argument(
        "--sqlite-fallback-path",
        default=str(Path("data") / "product_store.db"),
        help="Fallback SQLite path passed into ProductStore; env DSN takes precedence.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_postgres_database_exists(args.postgres_dsn)
    os.environ["NOON_PRODUCT_STORE_DATABASE_URL"] = args.postgres_dsn

    run_suffix = uuid.uuid4().hex[:8]
    product_id = f"pg-smoke-sku-{run_suffix}"
    keyword = f"smoke keyword {run_suffix}"

    store = ProductStore(args.postgres_dsn)
    try:
        store.upsert_product(
            {
                "product_id": product_id,
                "platform": "noon",
                "title": "Postgres Smoke Product",
                "brand": "Smoke",
                "product_url": f"https://example.com/p/{product_id}",
                "image_url": "https://example.com/image.png",
            }
        )
        store.add_price_record(product_id, 99.9, 129.9, scraped_at="2026-04-01T10:00:00")
        store.add_rank_record(product_id, keyword=keyword, bsr_rank=11, scraped_at="2026-04-01T10:05:00")
        store.add_sales_snapshot(
            product_id,
            review_count=12,
            rating=4.5,
            sold_recently_text="100+ sold recently",
            scraped_at="2026-04-01T10:06:00",
        )
        store.add_crawl_observation(
            {
                "product_id": product_id,
                "platform": "noon",
                "source_type": "keyword",
                "source_value": keyword,
                "scraped_at": "2026-04-01T10:06:00",
                "price": 99.9,
                "original_price": 129.9,
                "currency": "SAR",
                "rating": 4.5,
                "review_count": 12,
                "bsr_rank": 11,
                "delivery_type": "Express",
                "is_express": True,
                "badge_texts_json": "[]",
                "public_signals_json": "{}",
                "category_path": "Home > Test",
                "product_url": f"https://example.com/p/{product_id}",
            }
        )
        store.upsert_keyword(keyword, display_keyword=keyword.title(), source_type="manual", source_platform="noon")
        run_id = store.start_keyword_run("monitor", platforms=["noon"])
        store.finish_keyword_run(run_id, keyword_count=1, metadata={"smoke": True})

        product = store.get_product(product_id, platform="noon")
        prices = store.get_price_history(product_id, platform="noon")
        keywords = store.list_keywords(limit=10)
        stats = store.get_statistics()
        print(
            {
                "backend": store.backend,
                "product_id": product_id,
                "product_title": product["title"] if product else None,
                "price_rows": len(prices),
                "keyword_rows": len(keywords),
                "total_products": stats["total_products"],
                "total_keyword_runs": stats["total_keyword_runs"],
            }
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()
