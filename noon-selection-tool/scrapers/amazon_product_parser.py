"""
Shared Amazon product parsing helpers.
"""
from __future__ import annotations

import re
from typing import Any


_PRICE_PATTERN = re.compile(r"[\d.]+")
_BOUGHT_PATTERN = re.compile(
    r"(([\d.,]+(?:\s*[km])?)\+?\s+bought\s+in\s+past\s+(?:month|week))",
    re.IGNORECASE,
)
_DELIVERY_PATTERN = re.compile(
    r"(free delivery|delivery\s+\w+|get it by [^\n]+|prime delivery)",
    re.IGNORECASE,
)
_OFF_PATTERN = re.compile(r"\b\d+%\s*off\b", re.IGNORECASE)

_SIGNAL_HINTS = (
    "prime",
    "best seller",
    "amazon's choice",
    "choice",
    "bought in past",
    "delivery",
    "get it by",
    "free delivery",
    "limited time deal",
    "deal",
    "coupon",
    "sponsored",
)


def _to_float(text: str) -> float | None:
    numbers = _PRICE_PATTERN.findall(text or "")
    if not numbers:
        return None
    try:
        return float(numbers[0])
    except ValueError:
        return None


def _to_count(value: str) -> int | None:
    if not value:
        return None

    normalized = value.strip().lower().replace(",", "")
    multiplier = 1
    if normalized.endswith("k"):
        multiplier = 1000
        normalized = normalized[:-1]
    elif normalized.endswith("m"):
        multiplier = 1000000
        normalized = normalized[:-1]

    try:
        return int(float(normalized) * multiplier)
    except ValueError:
        return None


def _extract_signal_lines(card_text: str, title: str, seller_name: str, brand: str) -> list[str]:
    signals: list[str] = []
    seen: set[str] = set()
    ignored = {
        (title or "").strip().lower(),
        (seller_name or "").strip().lower(),
        (brand or "").strip().lower(),
    }

    for raw_line in (card_text or "").splitlines():
        line = raw_line.strip()
        lowered = line.lower()
        if not line:
            continue
        if lowered in ignored:
            continue
        if len(line) > 140:
            continue
        if re.fullmatch(r"[\d.,]+", line):
            continue
        if not any(token in lowered for token in _SIGNAL_HINTS) and not _OFF_PATTERN.search(lowered):
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        signals.append(line)

    return signals


def _parse_rating(value: str) -> float | None:
    numbers = re.findall(r"[\d.]+", value or "")
    if not numbers:
        return None
    try:
        return float(numbers[0])
    except ValueError:
        return None


def _parse_review_count(value: str) -> int:
    numbers = re.findall(r"[\d.,]+", value or "")
    if not numbers:
        return 0
    parsed = _to_count(numbers[0])
    return parsed or 0


def _extract_bought_signal(card_text: str) -> tuple[str, int | None]:
    match = _BOUGHT_PATTERN.search(card_text or "")
    if not match:
        return "", None
    return match.group(1).strip(), _to_count(match.group(2))


def _extract_delivery_signal(lines: list[str], card_text: str) -> str:
    for line in lines:
        if _DELIVERY_PATTERN.search(line):
            return line.strip()
    match = _DELIVERY_PATTERN.search(card_text or "")
    return match.group(1).strip() if match else ""


def _detect_delivery_type(card_text: str, is_express: bool) -> str:
    text = (card_text or "").lower()
    if is_express or "prime" in text:
        return "prime"
    if "free delivery" in text or "delivery" in text:
        return "delivery"
    return ""


def parse_amazon_product_payload(
    data: dict[str, Any],
    *,
    search_rank: int | None = None,
    category_path: str = "",
) -> dict[str, Any]:
    """
    Normalize raw Amazon card data into the shared product schema.
    """
    title = (data.get("title") or "").strip()
    seller_name = (data.get("sellerText") or "").strip()
    brand = (data.get("brandText") or "").strip()
    card_text = data.get("cardText") or ""
    is_choice = bool(data.get("isChoice"))
    is_prime = bool(data.get("isPrime"))
    is_express = is_choice or is_prime

    product: dict[str, Any] = {
        "product_id": (data.get("asin") or "").strip(),
        "title": title,
        "brand": brand,
        "seller_name": seller_name,
        "price": 0.0,
        "original_price": None,
        "currency": "SAR",
        "rating": None,
        "review_count": 0,
        "is_express": is_express,
        "delivery_type": "",
        "is_bestseller": bool(data.get("isBestSeller", False)),
        "is_ad": bool(data.get("isAd", False)),
        "image_count": int(data.get("imageCount", 0) or 0),
        "product_url": "",
        "category_path": category_path,
        "search_rank": search_rank,
        "sold_recently": "",
        "sold_recently_text": "",
        "stock_signal_text": "",
        "stock_left_count": None,
        "ranking_signal_text": "",
        "lowest_price_signal_text": "",
        "delivery_eta_signal_text": "",
        "badge_texts": [],
        "all_signal_texts": [],
        "delivery_signal_texts": [],
        "promotion_signal_texts": [],
        "signal_source": "public_page",
    }

    href = data.get("href") or data.get("productUrl") or ""
    if href:
        product["product_url"] = href

    price = _to_float(data.get("priceText") or "")
    if price is not None:
        product["price"] = price

    original_price = _to_float(data.get("originalPriceText") or "")
    if original_price is not None:
        product["original_price"] = original_price

    product["rating"] = _parse_rating(data.get("ratingText") or "")
    product["review_count"] = _parse_review_count(data.get("reviewText") or "")

    signal_lines = _extract_signal_lines(card_text, title, seller_name, brand)
    bought_text, bought_count = _extract_bought_signal(card_text)
    if bought_text:
        product["sold_recently_text"] = bought_text
        if bought_count is not None:
            product["sold_recently"] = str(bought_count)

    delivery_signal = _extract_delivery_signal(signal_lines, card_text)
    if delivery_signal:
        product["delivery_eta_signal_text"] = delivery_signal

    badge_texts = []
    if is_prime:
        badge_texts.append("Prime")
    if is_choice:
        badge_texts.append("Amazon's Choice")
    if product["is_bestseller"]:
        badge_texts.append("Best Seller")
    if product["is_ad"]:
        badge_texts.append("Sponsored")

    merged_signals = []
    seen: set[str] = set()
    for line in badge_texts + signal_lines:
        normalized = line.lower().strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged_signals.append(line)

    product["badge_texts"] = badge_texts
    product["all_signal_texts"] = merged_signals
    product["delivery_signal_texts"] = [
        line for line in merged_signals if "deliver" in line.lower() or "prime" in line.lower()
    ]
    product["promotion_signal_texts"] = [
        line
        for line in merged_signals
        if _OFF_PATTERN.search(line) or any(token in line.lower() for token in ("deal", "coupon"))
    ]
    product["delivery_type"] = _detect_delivery_type(card_text, is_express)

    return product
