"""
Shared Noon product parsing helpers.
"""
from __future__ import annotations

import re
from typing import Any

from scrapers.noon_delivery_detection import (
    DELIVERY_ETA_PATTERN,
    detect_delivery_type,
    normalize_delivery_marker_texts,
)


_PRICE_PATTERN = re.compile(r"[\d.]+")
_SOLD_PATTERN = re.compile(r"([\d,]+)\+?\s*sold(?:\s+recently)?", re.IGNORECASE)
_COMPACT_NUMBER_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*([KMB])\+?", re.IGNORECASE)
_STOCK_PATTERN = re.compile(
    r"((?:only|last)\s+(\d+)\s+(?:left(?:\s+in\s+stock)?|remaining))",
    re.IGNORECASE,
)
_RANK_PATTERN = re.compile(r"(#\d+\s+in\s+.+)", re.IGNORECASE)
_LOWEST_PRICE_PATTERN = re.compile(
    r"(lowest price in (?:\d+\s+days?|a year|a month|a week))",
    re.IGNORECASE,
)
_OFF_PATTERN = re.compile(r"\b\d+%\s*off\b", re.IGNORECASE)

_SIGNAL_HINTS = (
    "sell out fast",
    "selling out fast",
    "best seller",
    "top rated",
    "limited time",
    "sponsored",
    "ad",
    "express",
    "global",
    "marketplace",
    "only ",
    "last ",
    "sold",
    "#",
    "lowest price",
    "free delivery",
    "get it by",
    "get in ",
    "cashback",
    "big deal",
    "extra ",
)


def _to_float(text: str) -> float | None:
    numbers = _PRICE_PATTERN.findall(text or "")
    if not numbers:
        return None
    try:
        return float(numbers[0])
    except ValueError:
        return None


def _normalize_image_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("data:"):
        return ""
    return raw


def _parse_rating_block(text: str) -> tuple[float | None, int]:
    raw_text = (text or "").strip()
    if not raw_text:
        return None, 0

    rating = None
    rating_match = re.search(r"\d+(?:\.\d+)?", raw_text)
    if rating_match:
        try:
            rating = float(rating_match.group(0))
        except ValueError:
            rating = None

    try:
        remainder = raw_text[rating_match.end() :] if rating_match else raw_text
    except Exception:
        remainder = raw_text

    review_count = 0
    compact_match = _COMPACT_NUMBER_PATTERN.search(remainder)
    if compact_match:
        try:
            number = float(compact_match.group(1))
            suffix = compact_match.group(2).upper()
            multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
            review_count = int(number * multiplier)
        except ValueError:
            review_count = 0
    else:
        review_match = re.search(r"\d[\d,]*", remainder)
        if review_match:
            try:
                review_count = int(review_match.group(0).replace(",", ""))
            except ValueError:
                review_count = 0

    return rating, review_count


def _has_compact_review_count(text: str) -> bool:
    raw_text = (text or "").strip()
    if not raw_text:
        return False
    rating_match = re.search(r"\d+(?:\.\d+)?", raw_text)
    try:
        remainder = raw_text[rating_match.end() :] if rating_match else raw_text
    except Exception:
        remainder = raw_text
    return _COMPACT_NUMBER_PATTERN.search(remainder) is not None


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
        normalized = line.lower()
        if not line:
            continue
        if normalized in ignored:
            continue
        if len(line) > 120:
            continue
        if re.fullmatch(r"[\d.,]+", line):
            continue
        if not any(hint in normalized for hint in _SIGNAL_HINTS) and not _OFF_PATTERN.search(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        signals.append(line)

    return signals


def _extract_signal(pattern: re.Pattern[str], card_text: str) -> tuple[str, int | None]:
    match = pattern.search(card_text or "")
    if not match:
        return "", None

    full_text = match.group(1).strip()
    count = None
    if match.lastindex and match.lastindex >= 2:
        raw_value = match.group(2)
        if raw_value:
            try:
                count = int(raw_value.replace(",", ""))
            except ValueError:
                count = None

    return full_text, count


def _extract_sold_signal(card_text: str) -> tuple[str, int | None]:
    match = _SOLD_PATTERN.search(card_text or "")
    if not match:
        return "", None

    try:
        count = int(match.group(1).replace(",", ""))
    except ValueError:
        count = None

    return match.group(0).strip(), count


def _extract_first_matching_line(lines: list[str], pattern: re.Pattern[str]) -> str:
    for line in lines:
        if pattern.search(line or ""):
            return line.strip()
    return ""


def _extract_lines_by_keywords(lines: list[str], *keywords: str) -> list[str]:
    lowered_keywords = tuple(keyword.lower() for keyword in keywords)
    return [line for line in lines if any(keyword in line.lower() for keyword in lowered_keywords)]


def _normalize_text_list(values: Any) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    if not isinstance(values, list):
        return normalized
    for value in values:
        text = str(value or "").strip()
        lowered = text.lower()
        if not text or lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


def _clean_seller_name(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return ""

    lowered = normalized.lower()
    if lowered in {"best seller", "sponsored", "ad"}:
        return ""
    if "sold recently" in lowered:
        return ""
    if lowered.startswith("#"):
        return ""
    if re.search(r"\b(?:only|last)\s+\d+\s+(?:left|remaining)\b", lowered):
        return ""
    return normalized


def parse_noon_product_payload(
    data: dict[str, Any],
    *,
    search_rank: int | None = None,
    category_path: str = "",
) -> dict[str, Any]:
    """
    Normalize raw Noon card data into a stable product schema.
    """
    title = (data.get("title") or "").strip()
    seller_name = _clean_seller_name(data.get("sellerText") or "")
    brand = (data.get("brandText") or "").strip()
    card_text = data.get("cardText") or ""

    product: dict[str, Any] = {
        "product_id": "",
        "title": title,
        "brand": brand,
        "seller_name": seller_name,
        "price": 0.0,
        "original_price": None,
        "currency": "SAR",
        "rating": None,
        "review_count": 0,
        "review_count_is_estimated": False,
        "is_express": bool(data.get("isExpress", False)),
        "delivery_type": "",
        "is_bestseller": bool(data.get("isBestSeller", False)),
        "is_ad": bool(data.get("isAd", False)),
        "image_count": int(data.get("imgCount", 0) or 0),
        "image_url": "",
        "product_url": "",
        "category_path": category_path,
        "search_rank": search_rank,
        "sold_recently": "",
        "sold_recently_text": "",
        "stock_signal_text": "",
        "stock_left_count": None,
        "delivery_eta_signal_text": "",
        "ranking_signal_text": "",
        "lowest_price_signal_text": "",
        "badge_texts": [],
        "all_signal_texts": [],
        "delivery_signal_texts": [],
        "delivery_marker_texts": [],
        "promotion_signal_texts": [],
        "signal_source": "public_page",
    }

    href = data.get("href") or ""
    if href:
        product["product_url"] = f"https://www.noon.com{href}" if href.startswith("/") else href
        sku_match = re.search(r"/([A-Z0-9]{8,})/p/", product["product_url"], re.IGNORECASE)
        if sku_match:
            product["product_id"] = sku_match.group(1)

    price = _to_float(data.get("priceText") or "")
    if price is not None:
        product["price"] = price

    original_price = _to_float(data.get("wasText") or "")
    if original_price is not None:
        product["original_price"] = original_price

    product["image_url"] = _normalize_image_url(data.get("imageUrl") or "")

    rating, review_count = _parse_rating_block(data.get("ratingText") or "")
    product["rating"] = rating
    product["review_count"] = review_count
    product["review_count_is_estimated"] = _has_compact_review_count(data.get("ratingText") or "")

    signal_lines = _normalize_text_list(data.get("signalTexts")) or _extract_signal_lines(card_text, title, seller_name, brand)
    delivery_marker_texts = normalize_delivery_marker_texts(data.get("deliveryMarkers"))

    sold_text, sold_count = _extract_sold_signal(card_text)
    if sold_text:
        product["sold_recently_text"] = sold_text
        if sold_count is not None:
            product["sold_recently"] = str(sold_count)

    stock_text, stock_count = _extract_signal(_STOCK_PATTERN, card_text)
    if stock_text:
        product["stock_signal_text"] = stock_text
        product["stock_left_count"] = stock_count

    product["lowest_price_signal_text"] = _extract_first_matching_line(signal_lines, _LOWEST_PRICE_PATTERN)
    product["delivery_eta_signal_text"] = _extract_first_matching_line(signal_lines, DELIVERY_ETA_PATTERN)

    rank_match = _RANK_PATTERN.search(card_text)
    if rank_match:
        product["ranking_signal_text"] = rank_match.group(1).strip()

    product["badge_texts"] = signal_lines
    product["all_signal_texts"] = signal_lines
    product["delivery_marker_texts"] = delivery_marker_texts
    product["delivery_signal_texts"] = _extract_lines_by_keywords(
        signal_lines,
        "delivery",
        "get it by",
        "get in ",
        "express",
        "global",
        "marketplace",
        "supermall",
    )
    product["promotion_signal_texts"] = [
        line
        for line in signal_lines
        if _OFF_PATTERN.search(line) or any(token in line.lower() for token in ("cashback", "deal", "extra "))
    ]
    product["delivery_type"] = detect_delivery_type(
        card_text,
        product["is_express"],
        delivery_markers=delivery_marker_texts,
        delivery_signal_texts=product["delivery_signal_texts"],
    )

    return product
