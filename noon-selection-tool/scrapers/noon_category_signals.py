from __future__ import annotations

import re

from scrapers.noon_delivery_detection import (
    DELIVERY_ETA_PATTERN,
    DELIVERY_SIGNAL_RE,
    detect_delivery_type,
)

_SIGNAL_NUMERIC_ONLY_RE = re.compile(r"^[\d\s.,%+-]+$")
_SIGNAL_STOCK_RE = re.compile(
    r"((?:only|last)\s+(\d+)\s+(?:left(?:\s+in\s+stock)?|remaining))",
    re.IGNORECASE,
)
_SIGNAL_SOLD_RE = re.compile(r"(([\d,]+)\+?\s*sold(?:\s+recently)?)", re.IGNORECASE)
_SIGNAL_RANK_RE = re.compile(r"(#\d+\s+in\s+.+)", re.IGNORECASE)
_SIGNAL_LOWEST_PRICE_RE = re.compile(
    r"(lowest price in (?:\d+\s+days?|a year|a month|a week))",
    re.IGNORECASE,
)
_SIGNAL_PROMOTION_RE = re.compile(
    r"(\b\d+%\s*off\b|cashback|deal|extra\s+\d+|lowest price)",
    re.IGNORECASE,
)
_SIGNAL_AD_RE = re.compile(r"(\bsponsored\b|\bpromoted\b|\u0625\u0639\u0644\u0627\u0646)", re.IGNORECASE)
_SIGNAL_BADGE_RE = re.compile(
    r"(best seller|selling out fast|sell out fast|top rated|free delivery|lowest price|cashback|deal|"
    r"sold|only\s+\d+|last\s+\d+|#\d+\s+in|express|global|supermall|marketplace|get it by|get in \d+)",
    re.IGNORECASE,
)


def is_explicit_ad_signal(value: str) -> bool:
    normalized = re.sub(r"\s+", " ", value or "").strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    return lowered == "ad" or bool(_SIGNAL_AD_RE.search(normalized))


def clean_signal_value(
    value: str,
    *,
    title: str,
    seller_name: str,
    brand: str,
) -> str:
    cleaned = re.sub(r"\s+", " ", value or "").strip(" -|,:")
    if not cleaned:
        return ""
    if len(cleaned) > 140:
        return ""
    if _SIGNAL_NUMERIC_ONLY_RE.fullmatch(cleaned):
        return ""

    ignored = {
        (title or "").strip().lower(),
        (seller_name or "").strip().lower(),
        (brand or "").strip().lower(),
    }
    lowered = cleaned.lower()
    if lowered in ignored:
        return ""
    if lowered in {
        "placeholder",
        "wishlist",
        "add-to-cart",
        "add to cart",
        "prev carousel navigation",
        "next carousel navigation",
    }:
        return ""
    if lowered.startswith("prev carousel") or lowered.startswith("next carousel"):
        return ""
    if re.search(r"\bimage\s+\d+\b", lowered) and (title or "").strip().lower() in lowered:
        return ""
    if lowered.startswith("sar "):
        return ""
    return cleaned


def collect_signal_values(data: dict, product: dict) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    title = str(product.get("title", "") or "")
    seller_name = str(product.get("seller_name", "") or "")
    brand = str(product.get("brand", "") or "")

    candidates = []
    candidates.extend(data.get("signalTexts") or [])
    candidates.extend((data.get("cardText") or "").splitlines())

    for raw_value in candidates:
        cleaned = clean_signal_value(
            str(raw_value or ""),
            title=title,
            seller_name=seller_name,
            brand=brand,
        )
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(cleaned)
    return values


def collect_marker_details(data: dict, field_name: str) -> list[dict[str, str]]:
    markers: list[dict[str, str]] = []
    seen: set[str] = set()

    for raw_item in data.get(field_name) or []:
        if not isinstance(raw_item, dict):
            continue

        normalized = {
            key: re.sub(r"\s+", " ", str(raw_item.get(key, "") or "")).strip()
            for key in ("merged", "text", "dataQa", "alt", "title", "ariaLabel", "className", "tag")
        }
        merged = normalized.get("merged") or " | ".join(
            value
            for value in (
                normalized.get("dataQa"),
                normalized.get("alt"),
                normalized.get("title"),
                normalized.get("ariaLabel"),
                normalized.get("className"),
                normalized.get("text"),
            )
            if value
        )
        merged = re.sub(r"\s+", " ", merged).strip()
        if not merged:
            continue

        dedupe_key = " | ".join(
            normalized.get(key, "")
            for key in ("merged", "dataQa", "alt", "title", "ariaLabel", "className", "text")
        ).lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        normalized["merged"] = merged
        markers.append(normalized)

    return markers


def first_signal_match(values: list[str], pattern: re.Pattern[str]) -> tuple[str, int | None]:
    for value in values:
        match = pattern.search(value or "")
        if not match:
            continue
        count = None
        groups = match.groups()
        if len(groups) >= 2:
            raw_count = groups[1]
            if raw_count:
                try:
                    count = int(str(raw_count).replace(",", ""))
                except ValueError:
                    count = None
        return match.group(1).strip(), count
    return "", None


def detect_delivery_type_from_signals(
    values: list[str],
    current_value: str,
    *,
    is_express: bool = False,
    delivery_markers: list[dict[str, str]] | None = None,
) -> str:
    delivery_values = [value for value in values if DELIVERY_SIGNAL_RE.search(value or "")]
    return detect_delivery_type(
        " | ".join(values or []),
        is_express,
        delivery_markers=delivery_markers or [],
        delivery_signal_texts=delivery_values,
        current_value=current_value or "",
    )


def augment_product_from_signals(product: dict, data: dict) -> dict:
    signal_values = collect_signal_values(data, product)
    delivery_markers = collect_marker_details(data, "deliveryMarkers")
    ad_markers = collect_marker_details(data, "adMarkers")

    if not signal_values and not delivery_markers and not ad_markers:
        return product

    merged_signals: list[str] = []
    seen: set[str] = set()
    for source in [product.get("all_signal_texts") or [], signal_values]:
        for item in source:
            normalized = str(item or "").strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            merged_signals.append(normalized)

    delivery_marker_texts = [marker.get("merged", "") for marker in delivery_markers if marker.get("merged")]
    ad_marker_texts = [marker.get("merged", "") for marker in ad_markers if marker.get("merged")]
    badge_signals = [value for value in merged_signals if _SIGNAL_BADGE_RE.search(value or "")]

    product["raw_signal_texts"] = signal_values
    product["all_signal_texts"] = merged_signals
    product["badge_texts"] = badge_signals
    product["delivery_marker_texts"] = delivery_marker_texts
    product["ad_marker_texts"] = ad_marker_texts
    product["delivery_signal_texts"] = [
        value for value in merged_signals if DELIVERY_SIGNAL_RE.search(value or "")
    ]
    product["promotion_signal_texts"] = [
        value for value in merged_signals if _SIGNAL_PROMOTION_RE.search(value or "")
    ]

    if not product.get("sold_recently_text"):
        sold_text, sold_count = first_signal_match(merged_signals, _SIGNAL_SOLD_RE)
        if sold_text:
            product["sold_recently_text"] = sold_text
            if sold_count is not None:
                product["sold_recently"] = str(sold_count)

    if not product.get("stock_signal_text"):
        stock_text, stock_count = first_signal_match(merged_signals, _SIGNAL_STOCK_RE)
        if stock_text:
            product["stock_signal_text"] = stock_text
            product["stock_left_count"] = stock_count

    if not product.get("ranking_signal_text"):
        rank_text, _ = first_signal_match(merged_signals, _SIGNAL_RANK_RE)
        if rank_text:
            product["ranking_signal_text"] = rank_text

    if not product.get("lowest_price_signal_text"):
        lowest_price_text, _ = first_signal_match(merged_signals, _SIGNAL_LOWEST_PRICE_RE)
        if lowest_price_text:
            product["lowest_price_signal_text"] = lowest_price_text

    if not product.get("delivery_eta_signal_text"):
        delivery_eta_text, _ = first_signal_match(merged_signals, DELIVERY_ETA_PATTERN)
        if delivery_eta_text:
            product["delivery_eta_signal_text"] = delivery_eta_text

    if not product.get("is_ad"):
        product["is_ad"] = bool(ad_marker_texts) or any(
            is_explicit_ad_signal(value or "")
            for value in merged_signals
        )

    product["delivery_type"] = detect_delivery_type_from_signals(
        merged_signals,
        str(product.get("delivery_type", "") or ""),
        is_express=bool(product.get("is_express")),
        delivery_markers=delivery_markers,
    )
    product["signal_source"] = "public_page_card_text+snippets+markers"
    return product
