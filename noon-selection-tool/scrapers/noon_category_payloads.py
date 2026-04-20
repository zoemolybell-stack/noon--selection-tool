from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from scrapers.noon_category_card_capture import (
    collect_category_product_payloads as collect_category_product_payloads_from_card_capture,
)
from scrapers.noon_category_signals import augment_product_from_signals
from scrapers.noon_product_parser import parse_noon_product_payload


async def collect_category_product_payloads(page: Any) -> list[dict]:
    return await collect_category_product_payloads_from_card_capture(page)


def parse_category_product_payload(
    data: dict,
    *,
    search_rank: int | None = None,
    category_path: str = "",
) -> dict:
    product = parse_noon_product_payload(
        data,
        search_rank=search_rank,
        category_path=category_path,
    )
    return augment_product_from_signals(product, data)


def build_subcategory_payload(sub_name: str, products: list[dict], meta: dict) -> dict:
    return {
        "subcategory": sub_name,
        "product_count": len(products),
        "scraped_at": datetime.now().isoformat(),
        **meta,
        "products": products,
    }


def merge_effective_subcategory_products(payloads: list[tuple[Path, dict]]) -> list[dict]:
    seen: set[str] = set()
    all_products: list[dict] = []
    for file_path, data in payloads:
        sub_name = data.get("subcategory", file_path.stem)
        for product in data.get("products", []):
            pid = product.get("product_id", "")
            if pid and pid in seen:
                continue
            if pid:
                seen.add(pid)
            merged = dict(product)
            merged["_subcategory"] = sub_name
            all_products.append(merged)
    return all_products
