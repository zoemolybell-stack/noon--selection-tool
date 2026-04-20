from __future__ import annotations

from typing import Any

from keyword_runtime_contract import (
    classify_platform_quality_reasons,
    dedupe_failure_details,
    dedupe_strings,
)

PLATFORM_SUCCESS_STATUSES = {"completed", "zero_results", "partial"}
QUALITY_STATES = {"full", "partial", "degraded"}


def merge_platform_stats(
    summary: dict[str, object],
    batch_summary: dict[str, object],
    *,
    batch_index: int,
    batch_keywords: list[str],
) -> None:
    platform_stats = summary.setdefault("platform_stats", {})
    if not isinstance(platform_stats, dict):
        summary["platform_stats"] = {}
        platform_stats = summary["platform_stats"]

    batch_platform_stats = batch_summary.get("platform_stats") or {}
    if not isinstance(batch_platform_stats, dict):
        return

    for platform, raw_stats in batch_platform_stats.items():
        if not isinstance(raw_stats, dict):
            continue
        existing = platform_stats.get(platform)
        if not isinstance(existing, dict):
            existing = {
                "status": str(raw_stats.get("status") or "failed"),
                "attempts": 0,
                "retry_count": 0,
                "keyword_count": 0,
                "products_count": 0,
                "persisted_count": 0,
                "result_files": [],
                "total_results": 0,
                "error": "",
                "error_evidence": [],
                "zero_result_evidence": [],
                "failure_details": [],
                "failed_keywords": [],
                "zero_result_keywords": [],
                "live_suggestion_files": 0,
                "live_suggestion_parents": 0,
                "live_suggestion_keywords": 0,
                "live_suggestion_edges": 0,
                "live_suggestion_rejected": 0,
                "attempt_history": [],
                "batch_count": 0,
                "batch_keywords": [],
            }
            platform_stats[platform] = existing

        existing["attempts"] = int(existing.get("attempts") or 0) + int(raw_stats.get("attempts") or 0)
        existing["retry_count"] = int(existing.get("retry_count") or 0) + int(raw_stats.get("retry_count") or 0)
        existing["keyword_count"] = int(existing.get("keyword_count") or 0) + int(raw_stats.get("keyword_count") or 0)
        existing["products_count"] = int(existing.get("products_count") or 0) + int(raw_stats.get("products_count") or 0)
        existing["persisted_count"] = int(existing.get("persisted_count") or 0) + int(raw_stats.get("persisted_count") or 0)
        existing["total_results"] = int(existing.get("total_results") or 0) + int(raw_stats.get("total_results") or 0)
        existing["live_suggestion_files"] = int(existing.get("live_suggestion_files") or 0) + int(raw_stats.get("live_suggestion_files") or 0)
        existing["live_suggestion_parents"] = int(existing.get("live_suggestion_parents") or 0) + int(raw_stats.get("live_suggestion_parents") or 0)
        existing["live_suggestion_keywords"] = int(existing.get("live_suggestion_keywords") or 0) + int(raw_stats.get("live_suggestion_keywords") or 0)
        existing["live_suggestion_edges"] = int(existing.get("live_suggestion_edges") or 0) + int(raw_stats.get("live_suggestion_edges") or 0)
        existing["live_suggestion_rejected"] = int(existing.get("live_suggestion_rejected") or 0) + int(raw_stats.get("live_suggestion_rejected") or 0)
        existing["batch_count"] = int(existing.get("batch_count") or 0) + 1
        existing["result_files"] = dedupe_strings(list(existing.get("result_files") or []) + list(raw_stats.get("result_files") or []))
        existing["error_evidence"] = dedupe_strings(list(existing.get("error_evidence") or []) + list(raw_stats.get("error_evidence") or []))
        existing["zero_result_evidence"] = dedupe_strings(
            list(existing.get("zero_result_evidence") or []) + list(raw_stats.get("zero_result_evidence") or [])
        )
        existing["failed_keywords"] = dedupe_strings(list(existing.get("failed_keywords") or []) + list(raw_stats.get("failed_keywords") or []))
        existing["zero_result_keywords"] = dedupe_strings(
            list(existing.get("zero_result_keywords") or []) + list(raw_stats.get("zero_result_keywords") or [])
        )
        existing["failure_details"] = dedupe_failure_details(existing.get("failure_details"), raw_stats.get("failure_details"))

        existing_batch_keywords = list(existing.get("batch_keywords") or [])
        existing["batch_keywords"] = dedupe_strings(existing_batch_keywords + list(batch_keywords or []))

        incoming_history: list[dict[str, object]] = []
        for attempt in list(raw_stats.get("attempt_history") or []):
            if isinstance(attempt, dict):
                enriched = dict(attempt)
                enriched["batch_index"] = batch_index
                incoming_history.append(enriched)
        existing["attempt_history"] = list(existing.get("attempt_history") or []) + incoming_history

        incoming_status = str(raw_stats.get("status") or "failed")
        existing_status = str(existing.get("status") or "failed")
        if incoming_status == "failed" or existing_status == "failed":
            existing["status"] = "failed" if existing_status == "failed" and incoming_status == "failed" else "partial"
        elif incoming_status == "partial" or existing_status == "partial":
            existing["status"] = "partial"
        elif incoming_status == "zero_results" or existing_status == "zero_results":
            existing["status"] = "zero_results"
        else:
            existing["status"] = "completed"

        error_parts = [str(existing.get("error") or "").strip(), str(raw_stats.get("error") or "").strip()]
        existing["error"] = " | ".join([part for part in error_parts if part])


def merge_monitor_batch_summary(
    summary: dict[str, object],
    batch_summary: dict[str, object],
    *,
    batch_index: int,
    batch_count: int,
    batch_keywords: list[str],
) -> None:
    summary["crawl_status"] = str(batch_summary.get("status") or summary.get("crawl_status") or "unknown")
    summary["persisted_product_count"] = int(summary.get("persisted_product_count") or 0) + int(
        batch_summary.get("persisted_product_count") or 0
    )
    summary["crawl_batches_completed"] = int(summary.get("crawl_batches_completed") or 0) + 1
    summary["processed_keyword_count"] = int(summary.get("processed_keyword_count") or 0) + len(batch_keywords)

    persist_counts = summary.setdefault("persist_counts", {})
    if not isinstance(persist_counts, dict):
        summary["persist_counts"] = {}
        persist_counts = summary["persist_counts"]
    for platform, count in (batch_summary.get("persist_counts") or {}).items():
        persist_counts[platform] = int(persist_counts.get(platform) or 0) + int(count or 0)

    summary["errors"] = list(summary.get("errors") or []) + list(batch_summary.get("errors") or [])
    summary.setdefault("batch_results", [])
    if isinstance(summary["batch_results"], list):
        summary["batch_results"].append(
            {
                "batch_index": batch_index,
                "batch_count": batch_count,
                "keyword_count": len(batch_keywords),
                "keywords_preview": batch_keywords[:10],
                "status": str(batch_summary.get("status") or "unknown"),
                "persisted_product_count": int(batch_summary.get("persisted_product_count") or 0),
                "persist_counts": dict(batch_summary.get("persist_counts") or {}),
            }
        )

    merge_platform_stats(summary, batch_summary, batch_index=batch_index, batch_keywords=batch_keywords)
    summary["quality_summary"] = summarize_keyword_quality(summary)
    summary["quality_state"] = str(summary["quality_summary"].get("state") or "partial")


def normalize_quality_state(value: object) -> str:
    state = str(value or "").strip().lower()
    return state if state in QUALITY_STATES else "partial"


def quality_state_rank(value: object) -> int:
    return {"degraded": 0, "partial": 1, "full": 2}.get(normalize_quality_state(value), 1)


def worse_quality_state(*states: object) -> str:
    worst = "full"
    worst_rank = quality_state_rank(worst)
    for state in states:
        rank = quality_state_rank(state)
        if rank < worst_rank:
            worst = normalize_quality_state(state)
            worst_rank = rank
    return worst


def _is_quality_neutral_zero_results(reason_codes: list[str]) -> bool:
    normalized = [str(item or "").strip() for item in reason_codes if str(item or "").strip()]
    return bool(normalized) and all(code == "zero_results" for code in normalized)


def build_quality_source_breakdown(
    *,
    requested_platforms: list[str],
    platform_quality: dict[str, dict[str, object]],
    analysis_summary: dict[str, object],
) -> dict[str, object]:
    sources: dict[str, object] = {}
    crawl_reason_codes: list[str] = []
    runtime_reason_groups: dict[str, list[str]] = {
        "dependency_missing": [],
        "runtime_import_error": [],
        "amazon_parse_failure": [],
        "amazon_upstream_blocked": [],
        "result_contract_mismatch": [],
        "timeout": [],
        "external_site_error": [],
        "page_recognition_failed": [],
        "fallback_misfire": [],
        "runtime_error": [],
        "zero_results": [],
        "partial_results": [],
    }
    for platform in requested_platforms or list(platform_quality.keys()):
        payload = dict(platform_quality.get(platform) or {})
        reason_codes = [str(item or "").strip() for item in list(payload.get("reason_codes") or []) if str(item or "").strip()]
        for code in reason_codes:
            if code not in crawl_reason_codes:
                crawl_reason_codes.append(code)
            if code in runtime_reason_groups and platform not in runtime_reason_groups[code]:
                runtime_reason_groups[code].append(platform)
        sources[platform] = {
            "state": str(payload.get("state") or "unknown"),
            "status": str(payload.get("status") or "unknown"),
            "reason_codes": reason_codes,
            "primary_reason": reason_codes[0] if reason_codes else "",
            "evidence": list(payload.get("evidence") or []),
            "failure_details": list(payload.get("failure_details") or []),
            "products_count": int(payload.get("products_count") or 0),
            "total_results": int(payload.get("total_results") or 0),
        }

    analysis_reason_codes: list[str] = []
    if not bool(analysis_summary.get("available")):
        analysis_reason_codes.append("analysis_empty")
    if not bool(analysis_summary.get("google_trends_available")):
        analysis_reason_codes.append("google_trends_missing")
    if not bool(analysis_summary.get("amazon_bsr_available")):
        analysis_reason_codes.append("amazon_bsr_missing")

    sources["analysis"] = {
        "state": str(analysis_summary.get("state") or "unknown"),
        "status": str(analysis_summary.get("state") or "unknown"),
        "reason_codes": analysis_reason_codes,
        "primary_reason": analysis_reason_codes[0] if analysis_reason_codes else "",
        "available": bool(analysis_summary.get("available")),
        "google_trends_available": bool(analysis_summary.get("google_trends_available")),
        "amazon_bsr_available": bool(analysis_summary.get("amazon_bsr_available")),
        "rows": int(analysis_summary.get("rows") or 0),
    }
    sources["crawl"] = {
        "state": worse_quality_state(*[item.get("state") for item in platform_quality.values()] or ["partial"]),
        "reason_codes": crawl_reason_codes,
        "primary_reason": crawl_reason_codes[0] if crawl_reason_codes else "",
    }
    runtime_reason_codes = [code for code, platforms in runtime_reason_groups.items() if platforms]
    sources["runtime"] = {
        "state": "degraded" if any(
            runtime_reason_groups[key]
            for key in (
                "dependency_missing",
                "runtime_import_error",
                "amazon_parse_failure",
                "amazon_upstream_blocked",
                "result_contract_mismatch",
                "timeout",
                "external_site_error",
                "page_recognition_failed",
                "fallback_misfire",
                "runtime_error",
            )
        ) else "partial" if any(runtime_reason_groups[key] for key in ("partial_results",)) else "full",
        "reason_codes": runtime_reason_codes,
        "primary_reason": runtime_reason_codes[0] if runtime_reason_codes else "",
        "reason_breakdown": runtime_reason_groups,
    }
    return sources


def frame_column(frame: object | None, column: str):
    if frame is None or not hasattr(frame, "columns"):
        return None
    try:
        columns = list(getattr(frame, "columns"))
    except Exception:
        return None
    if column not in columns:
        return None
    try:
        return frame[column]
    except Exception:
        return None


def series_any_true(series: object | None) -> bool:
    if series is None:
        return False
    try:
        filled = series.fillna(False)
        return bool(filled.astype(bool).any())
    except Exception:
        pass
    try:
        return any(bool(item) for item in list(series))
    except Exception:
        return False


def series_any_positive(series: object | None) -> bool:
    if series is None:
        return False
    try:
        filled = series.fillna(0)
        return bool((filled.astype(float) > 0).any())
    except Exception:
        pass
    try:
        return any(float(item or 0) > 0 for item in list(series))
    except Exception:
        return False


def summarize_analysis_quality(frame: object | None) -> dict[str, object]:
    available = frame is not None and not bool(getattr(frame, "empty", True))
    rows = 0
    if available:
        try:
            rows = len(frame)  # type: ignore[arg-type]
        except Exception:
            rows = 0

    google_trends_available = series_any_true(frame_column(frame, "has_google_trends"))
    if not google_trends_available:
        google_trends_available = series_any_positive(frame_column(frame, "google_interest"))

    amazon_bsr_available = series_any_positive(frame_column(frame, "amazon_bsr_count"))

    quality_flags: list[str] = []
    quality_evidence: list[str] = []
    if not available:
        quality_flags.append("analysis_empty")
        quality_evidence.append("analysis_empty")
        state = "unknown"
    else:
        state = "full"
        if not google_trends_available:
            quality_flags.append("google_trends_missing")
            quality_evidence.append("google_trends_missing")
        if not amazon_bsr_available:
            quality_flags.append("amazon_bsr_missing")
            quality_evidence.append("amazon_bsr_missing")

    return {
        "state": state,
        "available": available,
        "rows": rows,
        "google_trends_available": google_trends_available,
        "amazon_bsr_available": amazon_bsr_available,
        "quality_flags": quality_flags,
        "quality_evidence": quality_evidence,
    }


def summarize_keyword_quality(summary: dict[str, object], analysis_frame: object | None = None) -> dict[str, object]:
    platform_stats = summary.get("platform_stats") or {}
    if not isinstance(platform_stats, dict):
        platform_stats = {}

    requested_platforms = [str(item).strip().lower() for item in (summary.get("platforms") or []) if str(item).strip()]
    if not requested_platforms:
        requested_platforms = [str(item).strip().lower() for item in platform_stats.keys() if str(item).strip()]
    requested_platforms = list(dict.fromkeys(requested_platforms))

    platform_quality: dict[str, dict[str, object]] = {}
    crawl_states: list[str] = []
    quality_flags: list[str] = []
    quality_evidence: list[str] = []
    quality_reasons: list[str] = []
    noon_success = None
    amazon_success = None
    bs4_unavailable = False

    for platform, raw_stats in platform_stats.items():
        if not isinstance(raw_stats, dict):
            continue
        status = str(raw_stats.get("status") or "").strip().lower()
        error = str(raw_stats.get("error") or "").strip()
        evidence = dedupe_strings(
            list(raw_stats.get("error_evidence") or []) + list(raw_stats.get("zero_result_evidence") or []) + ([error] if error else [])
        )
        has_bs4_issue = any("beautifulsoup4_unavailable" in item.lower() for item in evidence)
        if has_bs4_issue:
            bs4_unavailable = True
        reason_codes = classify_platform_quality_reasons(platform, status=status, evidence=evidence)
        if status == "failed" or has_bs4_issue:
            platform_state = "degraded"
        elif status == "zero_results":
            platform_state = "full" if _is_quality_neutral_zero_results(reason_codes) else "partial"
        elif status == "partial":
            platform_state = "partial"
        elif status == "completed":
            platform_state = "partial" if evidence else "full"
        else:
            platform_state = "partial"

        platform_quality[platform] = {
            "state": platform_state,
            "status": status or "unknown",
            "evidence": evidence,
            "reason_codes": reason_codes,
            "failure_details": dedupe_failure_details([], raw_stats.get("failure_details")),
            "products_count": int(raw_stats.get("products_count") or 0),
            "total_results": int(raw_stats.get("total_results") or 0),
        }
        crawl_states.append(platform_state)
        quality_evidence.extend(evidence)
        if not _is_quality_neutral_zero_results(reason_codes):
            quality_reasons.extend([f"{platform}:{code}" for code in reason_codes])
        if platform_state != "full":
            quality_flags.append(f"{platform}_{platform_state}")
        if platform == "noon":
            noon_success = status in PLATFORM_SUCCESS_STATUSES
        if platform == "amazon":
            amazon_success = status in PLATFORM_SUCCESS_STATUSES

    if requested_platforms:
        for platform in requested_platforms:
            if platform not in platform_quality:
                platform_quality[platform] = {
                    "state": "degraded",
                    "status": "missing",
                    "evidence": [f"{platform}_missing_from_summary"],
                    "reason_codes": ["runtime_error"],
                    "failure_details": [],
                    "products_count": 0,
                    "total_results": 0,
                }
                crawl_states.append("degraded")
                quality_flags.append(f"{platform}_missing")
                quality_evidence.append(f"{platform}_missing_from_summary")
                quality_reasons.append(f"{platform}:runtime_error")

    crawl_state = "full" if crawl_states else "partial"
    if crawl_states:
        crawl_state = worse_quality_state(*crawl_states)

    analysis_summary = summarize_analysis_quality(analysis_frame)
    if analysis_summary["quality_flags"]:
        quality_flags.extend(list(analysis_summary["quality_flags"]))
    if analysis_summary["quality_evidence"]:
        quality_evidence.extend(list(analysis_summary["quality_evidence"]))

    if bs4_unavailable:
        quality_flags.append("beautifulsoup4_unavailable")
        quality_evidence.append("beautifulsoup4_unavailable")

    if crawl_state == "degraded" or bs4_unavailable:
        quality_state = "degraded"
    else:
        analysis_state = str(analysis_summary["state"] or "")
        quality_state = crawl_state if analysis_state == "unknown" else worse_quality_state(crawl_state, analysis_state)

    signals = {
        "noon_success": noon_success,
        "amazon_success": amazon_success,
        "amazon_bsr_available": analysis_summary["amazon_bsr_available"],
        "google_trends_available": analysis_summary["google_trends_available"],
        "beautifulsoup4_unavailable": bs4_unavailable,
    }
    quality_source_breakdown = build_quality_source_breakdown(
        requested_platforms=requested_platforms,
        platform_quality=platform_quality,
        analysis_summary=analysis_summary,
    )
    return {
        "state": quality_state,
        "crawl_state": crawl_state,
        "analysis_state": analysis_summary["state"],
        "platforms": platform_quality,
        "signals": signals,
        "quality_flags": dedupe_strings(quality_flags),
        "quality_reasons": dedupe_strings(quality_reasons),
        "quality_evidence": dedupe_strings(quality_evidence),
        "quality_source_breakdown": quality_source_breakdown,
        "analysis": analysis_summary,
    }
