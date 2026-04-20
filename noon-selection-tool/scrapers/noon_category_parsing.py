from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from scrapers.noon_category_payloads import collect_category_product_payloads, parse_category_product_payload


def parse_raw_product(
    data: dict,
    *,
    search_rank: int | None = None,
    category_path: str = "",
) -> dict:
    return parse_category_product_payload(
        data,
        search_rank=search_rank,
        category_path=category_path,
    )


async def parse_products(
    page: Any,
    *,
    rank_offset: int = 0,
    category_path: str = "",
    detail_fetcher: Callable[[str], Awaitable[dict | None]] | None = None,
) -> list[dict]:
    data = await collect_category_product_payloads(page)
    products = []
    for item in data or []:
        if not item.get("title"):
            continue
        product = parse_raw_product(
            item,
            search_rank=rank_offset + len(products) + 1,
            category_path=category_path,
        )
        if (
            detail_fetcher is not None
            and product.get("review_count_is_estimated")
            and product.get("product_url")
        ):
            detail = await detail_fetcher(product["product_url"])
            if detail:
                if detail.get("review_count") is not None:
                    product["review_count"] = detail["review_count"]
                    product["review_count_is_estimated"] = False
                if detail.get("rating") is not None:
                    product["rating"] = detail["rating"]
                if detail.get("image_url"):
                    product["image_url"] = detail["image_url"]
        if product.get("title"):
            products.append(product)
    return products
