from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from fastapi.testclient import TestClient

from build_analytics_warehouse import AnalyticsWarehouseBuilder
from config.product_store import ProductStore
from db.postgres import ensure_postgres_database_exists


ROOT = Path(__file__).resolve().parent
DEFAULT_STAGE_DB = ROOT / "data" / "tmp_warehouse_stage_smoke.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a small warehouse + web smoke on Postgres.")
    parser.add_argument("--postgres-dsn", required=True, help="Target Postgres DSN for warehouse smoke.")
    parser.add_argument("--ops-db", type=Path, default=ROOT / "data" / "tmp_ops_web_smoke.db")
    parser.add_argument("--stage-db", type=Path, default=DEFAULT_STAGE_DB)
    return parser.parse_args()


def build_stage_smoke_db(stage_db: Path) -> None:
    if stage_db.exists():
        stage_db.unlink()
    store = ProductStore(stage_db)
    try:
        store.upsert_product(
            {
                "product_id": "warehouse-smoke-1",
                "platform": "noon",
                "title": "Warehouse Smoke Product",
                "brand": "Smoke",
                "seller_name": "Tester",
                "category_path": "Home > Pet Supplies > Dog Supplies",
                "product_url": "https://example.com/p/1",
                "image_url": "https://example.com/i/1.jpg",
                "last_public_signals_json": "{}",
                "first_seen": "2026-04-01T10:00:00",
                "last_seen": "2026-04-01T10:00:00",
                "is_active": 1,
            }
        )
        store.add_crawl_observation(
            {
                "product_id": "warehouse-smoke-1",
                "platform": "noon",
                "snapshot_id": "s1",
                "source_type": "category",
                "source_value": "dog supplies",
                "category_name": "Dog Supplies",
                "scraped_at": "2026-04-01T10:00:00",
                "price": 99.0,
                "original_price": 129.0,
                "currency": "SAR",
                "rating": 4.5,
                "review_count": 42,
                "search_rank": 1,
                "bsr_rank": 1,
                "seller_name": "Tester",
                "delivery_type": "Express",
                "is_express": 1,
                "is_bestseller": 1,
                "is_ad": 0,
                "sold_recently_text": "20+ sold recently",
                "stock_signal_text": "Only 3 left",
                "lowest_price_signal_text": "Lowest in 30 days",
                "ranking_signal_text": "#1 in Dog Supplies",
                "badge_texts_json": "[]",
                "public_signals_json": "{}",
                "category_path": "Home > Pet Supplies > Dog Supplies",
                "product_url": "https://example.com/p/1",
            }
        )
        store.upsert_keyword(
            "dog toys",
            display_keyword="dog toys",
            status="ready",
            tracking_mode="monitor",
            source_type="seed",
            source_platform="noon",
            priority=10,
        )
        store.start_keyword_run(
            "monitor",
            trigger_mode="manual",
            seed_keyword="dog toys",
            platforms=["noon"],
            metadata={"scope": "warehouse_smoke"},
        )
    finally:
        store.close()


def main() -> None:
    args = parse_args()
    build_stage_smoke_db(args.stage_db)

    ensure_postgres_database_exists(args.postgres_dsn)
    os.environ["NOON_WAREHOUSE_DATABASE_URL"] = args.postgres_dsn
    os.environ["NOON_OPS_DB"] = str(args.ops_db)

    builder = AnalyticsWarehouseBuilder(ROOT / "data" / "analytics" / "warehouse.db")
    try:
        builder.reset()
        import_summary = builder.import_stage_db(args.stage_db)
        warehouse_stats = builder.collect_warehouse_stats()
    finally:
        builder.close()

    from web_beta.app import app  # delayed import so env vars are already set

    client = TestClient(app)
    health = client.get("/api/health").json()
    products = client.get("/api/products", params={"limit": 5}).json()

    print(
        json.dumps(
            {
                "backend": "postgres",
                "import_summary": import_summary,
                "warehouse_stats": warehouse_stats,
                "web_health": health,
                "web_item_count": len(products.get("items", [])),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
