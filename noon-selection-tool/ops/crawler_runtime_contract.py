from __future__ import annotations

from typing import Any


ACTIVE_MONITOR_SKIP_REASON = "active_monitor"
ACTIVE_CATEGORY_CRAWL_SKIP_REASON = "active_category_crawl"

DUPLICATE_LOCK_PATTERNS = {
    ACTIVE_MONITOR_SKIP_REASON: (
        "keyword monitor is already running",
        "keyword monitor is already active",
    ),
    ACTIVE_CATEGORY_CRAWL_SKIP_REASON: (
        "category crawler is already running",
        "category crawler is already active",
    ),
}


def detect_duplicate_lock_skip_reason(raw_error: Any) -> str:
    text = str(raw_error or "").strip().lower()
    if not text:
        return ""
    for skip_reason, patterns in DUPLICATE_LOCK_PATTERNS.items():
        if any(pattern in text for pattern in patterns):
            return skip_reason
    return ""
