from __future__ import annotations

import re
from typing import Any


DELIVERY_ETA_PATTERN = re.compile(
    r"((?:get it by\s+.+|get in\s+\d+\s*(?:hr|hrs|hour|hours|min|mins|minute|minutes)(?:\s+\d+\s*(?:min|mins|minute|minutes))?))",
    re.IGNORECASE,
)
DELIVERY_EXPRESS_MARKER_RE = re.compile(
    r"(product-noon-express|noon[-\s]?express|\bfbn\b|\bexpress\b)",
    re.IGNORECASE,
)
DELIVERY_GLOBAL_MARKER_RE = re.compile(r"\bglobal\b", re.IGNORECASE)
DELIVERY_MARKETPLACE_MARKER_RE = re.compile(r"(marketplace|market\s*place)", re.IGNORECASE)
DELIVERY_SUPERMALL_MARKER_RE = re.compile(r"\bsupermall\b", re.IGNORECASE)
DELIVERY_SIGNAL_RE = re.compile(
    r"(express|global|supermall|marketplace|free delivery|get it by|get in \d+|delivery)",
    re.IGNORECASE,
)
DELIVERY_MARKER_JS_PATTERN = (
    r"(product-noon-express|noon[-\\s]?express|\\bexpress\\b|\\bglobal\\b|"
    r"marketplace|market\\s*place|\\bsupermall\\b|\\bfbn\\b|"
    r"get it by|get in \\d+|free delivery|delivery)"
)


def normalize_delivery_marker_texts(values: Any) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    if not isinstance(values, list):
        return normalized
    for value in values:
        if isinstance(value, dict):
            text = str(
                value.get("merged")
                or value.get("text")
                or value.get("alt")
                or value.get("title")
                or value.get("ariaLabel")
                or value.get("dataQa")
                or value.get("className")
                or ""
            ).strip()
        else:
            text = str(value or "").strip()
        lowered = text.lower()
        if not text or lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


def detect_delivery_type(
    card_text: str,
    is_express: bool,
    *,
    delivery_markers: list[Any] | None = None,
    delivery_signal_texts: list[str] | None = None,
    current_value: str = "",
) -> str:
    marker_texts = normalize_delivery_marker_texts(delivery_markers)
    marker_blob = " | ".join(marker_texts)
    signal_blob = " | ".join(delivery_signal_texts or [])
    text = " | ".join(part for part in [card_text or "", marker_blob, signal_blob] if part).lower()

    if any(DELIVERY_EXPRESS_MARKER_RE.search(marker or "") for marker in marker_texts):
        return "express"
    if any(DELIVERY_GLOBAL_MARKER_RE.search(marker or "") for marker in marker_texts):
        return "global"
    if any(DELIVERY_SUPERMALL_MARKER_RE.search(marker or "") for marker in marker_texts):
        return "supermall"
    if any(DELIVERY_MARKETPLACE_MARKER_RE.search(marker or "") for marker in marker_texts):
        return "marketplace"

    if is_express or "noon express" in text or re.search(r"\bexpress\b", text):
        return "express"
    if re.search(r"\bglobal\b", text):
        return "global"
    if re.search(r"\bsupermall\b", text):
        return "supermall"
    if "marketplace" in text or "market place" in text:
        return "marketplace"

    if current_value in {"express", "global", "supermall", "marketplace"}:
        return current_value
    return ""
