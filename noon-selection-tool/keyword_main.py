"""
Keyword crawler isolated entrypoint.

Goals:
- Keep keyword runtime isolated from category runtime.
- Persist keyword lifecycle data into the keyword database.
- Use the existing crawl/analyze core without changing category behavior.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent))
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import main as keyword_core
from config.product_store import ProductStore
from config.settings import Settings
from keyword_batch_summary import (
    merge_monitor_batch_summary as merge_runtime_monitor_batch_summary,
    summarize_analysis_quality as summarize_runtime_analysis_quality,
    summarize_keyword_quality as summarize_runtime_keyword_quality,
)
from keyword_monitor_runtime import (
    acquire_monitor_lock as acquire_keyword_monitor_lock,
    checkpoint_monitor_state as checkpoint_keyword_monitor_state,
    load_monitor_profile as load_keyword_monitor_profile,
    monitor_dir as get_keyword_monitor_dir,
    monitor_lock_path as get_keyword_monitor_lock_path,
    monitor_summary_path as get_keyword_monitor_summary_path,
    official_keyword_db_path as get_official_keyword_db_path,
    refresh_monitor_lock as refresh_keyword_monitor_lock,
    release_monitor_lock as release_keyword_monitor_lock,
    sync_warehouse as sync_keyword_warehouse,
    write_monitor_summary as write_keyword_monitor_summary,
)
from keyword_runtime_io import (
    atomic_write_json as runtime_atomic_write_json,
    atomic_write_lines as runtime_atomic_write_lines,
    atomic_write_text as runtime_atomic_write_text,
    collect_manual_keywords as runtime_collect_manual_keywords,
    filter_resume_completed_keywords as runtime_filter_resume_completed_keywords,
    keyword_pool_path as runtime_keyword_pool_path,
    keyword_snapshot_path as runtime_keyword_snapshot_path,
    load_keyword_list_payload as runtime_load_keyword_list_payload,
    load_keywords_from_file as runtime_load_keywords_from_file,
    load_resume_keywords as runtime_load_resume_keywords,
    read_keyword_pool as runtime_read_keyword_pool,
    safe_load_json_file as runtime_safe_load_json_file,
    write_keyword_pool as runtime_write_keyword_pool,
)
from keyword_runtime_stages import (
    build_default_analysis_quality_summary as runtime_build_default_analysis_quality_summary,
    build_default_scrape_quality_summary as runtime_build_default_scrape_quality_summary,
    build_scrape_summary as runtime_build_scrape_summary,
    build_skipped_scrape_summary as runtime_build_skipped_scrape_summary,
    crawl_platform_with_retry as runtime_crawl_platform_with_retry,
    finalize_platform_scrape as runtime_finalize_platform_scrape,
    finalize_scrape_run as runtime_finalize_scrape_run,
)
from keyword_runtime_contract import (
    build_failure_detail as build_runtime_failure_detail,
    classify_platform_quality_reasons as classify_runtime_platform_quality_reasons,
    dedupe_failure_details as dedupe_runtime_failure_details,
    dedupe_strings as dedupe_runtime_strings,
    classify_platform_payload as runtime_classify_platform_payload,
    load_platform_result_payloads as runtime_load_platform_result_payloads,
    result_file_path as runtime_result_file_path,
    safe_int as runtime_safe_int,
    summarize_platform_snapshot as summarize_runtime_platform_snapshot,
)
from ops.keyword_control_state import (
    build_keyword_control_lookup,
    classify_keyword_source_scopes,
    get_effective_baseline_keywords,
    get_keyword_control_state,
    match_keyword_control_rule,
)
from ops.progress_reporter import get_task_progress_reporter_from_env
from scrapers.base_scraper import (
    build_failure_detail as build_scraper_failure_detail,
    build_keyword_result_stem,
    get_keyword_result_payload_error,
    normalize_keyword_result_payload,
)


logger = logging.getLogger("keyword-runtime")

KEYWORD_SNAPSHOT_SUBDIRS = [
    "noon",
    "amazon",
    "alibaba",
    "temu",
    "shein",
    "google_trends",
    "amazon_bsr",
    "processed",
]

MAX_EXPANSION_DEPTH = 2
MONITOR_LOCK_FILENAME = "keyword_monitor.lock"
MONITOR_SUMMARY_FILENAME = "keyword_monitor_last_run.json"
AMAZON_PLATFORM_RETRY_COUNT = 1
AMAZON_PLATFORM_RETRY_DELAY_SECONDS = 15
PLATFORM_SUCCESS_STATUSES = {"completed", "zero_results", "partial"}
DEFAULT_MONITOR_CRAWL_BATCH_SIZE = int(os.getenv("KEYWORD_MONITOR_CRAWL_BATCH_SIZE") or "50")
DEFAULT_MONITOR_INCREMENTAL_SYNC_SECONDS = int(os.getenv("KEYWORD_MONITOR_INCREMENTAL_SYNC_SECONDS") or "300")
DEFAULT_SYNC_LOCK_WAIT_SECONDS = int(os.getenv("KEYWORD_SYNC_LOCK_WAIT_SECONDS") or "180")
DEFAULT_SYNC_LOCK_RETRY_SECONDS = int(os.getenv("SYNC_LOCK_RETRY_SECONDS") or "5")
QUALITY_STATES = {"full", "partial", "degraded"}
QUALITY_REASON_CODES = {
    "dependency_missing",
    "runtime_import_error",
    "amazon_parse_failure",
    "amazon_upstream_blocked",
    "external_site_error",
    "page_recognition_failed",
    "fallback_misfire",
    "zero_results",
    "partial_results",
    "runtime_error",
    "google_trends_missing",
    "amazon_bsr_missing",
    "analysis_empty",
}


def _update_task_progress(
    stage: str,
    *,
    message: str,
    metrics: dict[str, object] | None = None,
    details: dict[str, object] | None = None,
    completed: bool = False,
) -> None:
    reporter = get_task_progress_reporter_from_env()
    if not reporter:
        return
    reporter.update(
        stage,
        message=message,
        metrics=metrics or {},
        details=details or {},
        completed=completed,
    )


def _chunk_keywords(keywords: list[str], batch_size: int) -> list[list[str]]:
    if not keywords:
        return []
    if batch_size <= 0:
        return [list(keywords)]
    return [keywords[idx : idx + batch_size] for idx in range(0, len(keywords), batch_size)]


def _merge_string_lists(existing: list[object] | None, incoming: list[object] | None) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in list(existing or []) + list(incoming or []):
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        merged.append(value)
    return merged


def _dedupe_failure_details(existing: list[object] | None, incoming: list[object] | None) -> list[dict[str, object]]:
    return dedupe_runtime_failure_details(existing, incoming)


def _bs4_available_in_current_runtime() -> bool:
    return importlib.util.find_spec("bs4") is not None


def _build_failure_detail(
    *,
    platform: str,
    keyword: str,
    failure_category: str,
    short_evidence: str,
    expected_result_file: str = "",
    page_url: str = "",
    page_number: int | None = None,
    page_state: str = "",
    snapshot_id: str = "",
) -> dict[str, object]:
    return build_runtime_failure_detail(
        platform=platform,
        keyword=keyword,
        failure_category=failure_category,
        short_evidence=short_evidence,
        expected_result_file=expected_result_file,
        page_url=page_url,
        page_number=page_number,
        page_state=page_state,
        snapshot_id=snapshot_id,
    )


def _merge_platform_stats(
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
        existing["batch_keywords"] = _merge_string_lists(existing.get("batch_keywords"), batch_keywords)
        existing["result_files"] = _merge_string_lists(existing.get("result_files"), raw_stats.get("result_files"))
        existing["error_evidence"] = _merge_string_lists(existing.get("error_evidence"), raw_stats.get("error_evidence"))
        existing["zero_result_evidence"] = _merge_string_lists(
            existing.get("zero_result_evidence"),
            raw_stats.get("zero_result_evidence"),
        )
        existing["failure_details"] = _dedupe_failure_details(
            existing.get("failure_details"),
            raw_stats.get("failure_details"),
        )
        existing["failed_keywords"] = _merge_string_lists(existing.get("failed_keywords"), raw_stats.get("failed_keywords"))
        existing["zero_result_keywords"] = _merge_string_lists(
            existing.get("zero_result_keywords"),
            raw_stats.get("zero_result_keywords"),
        )
        incoming_history = []
        for item in list(raw_stats.get("attempt_history") or []):
            if isinstance(item, dict):
                enriched = dict(item)
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


def _merge_monitor_batch_summary(
    summary: dict[str, object],
    batch_summary: dict[str, object],
    *,
    batch_index: int,
    batch_count: int,
    batch_keywords: list[str],
) -> None:
    merge_runtime_monitor_batch_summary(
        summary,
        batch_summary,
        batch_index=batch_index,
        batch_count=batch_count,
        batch_keywords=batch_keywords,
    )
    return
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

    _merge_platform_stats(summary, batch_summary, batch_index=batch_index, batch_keywords=batch_keywords)
    summary["quality_summary"] = _summarize_keyword_quality(summary)
    summary["quality_state"] = str(summary["quality_summary"].get("state") or "partial")


def _should_run_monitor_incremental_sync(
    *,
    batch_index: int,
    batch_count: int,
    batch_persisted_count: int,
    last_sync_at: float | None,
    interval_seconds: int,
) -> bool:
    if batch_persisted_count <= 0:
        return False
    if batch_index >= batch_count:
        return False
    if interval_seconds <= 0:
        return True
    now = time.time()
    if last_sync_at is None:
        return True
    return (now - last_sync_at) >= interval_seconds


def _derive_monitor_crawl_status(batch_results: list[dict[str, object]]) -> str:
    if not batch_results:
        return "skipped"
    statuses = [str(item.get("status") or "unknown") for item in batch_results]
    if all(status == "completed" for status in statuses):
        return "completed"
    if all(status in {"failed", "skipped"} for status in statuses):
        return "failed"
    if any(status == "partial" for status in statuses):
        return "partial"
    if any(status == "failed" for status in statuses) and any(status in PLATFORM_SUCCESS_STATUSES for status in statuses):
        return "partial"
    if any(status == "completed" for status in statuses) or any(status == "zero_results" for status in statuses):
        return "partial" if any(status == "failed" for status in statuses) else "completed"
    return statuses[-1]


def _normalize_quality_state(value: object) -> str:
    state = str(value or "").strip().lower()
    return state if state in QUALITY_STATES else "partial"


def _quality_state_rank(value: object) -> int:
    return {"degraded": 0, "partial": 1, "full": 2}.get(_normalize_quality_state(value), 1)


def _worse_quality_state(*states: object) -> str:
    worst = "full"
    worst_rank = _quality_state_rank(worst)
    for state in states:
        rank = _quality_state_rank(state)
        if rank < worst_rank:
            worst = _normalize_quality_state(state)
            worst_rank = rank
    return worst


def _is_quality_neutral_zero_results(reason_codes: list[str]) -> bool:
    normalized = [str(item or "").strip() for item in reason_codes if str(item or "").strip()]
    return bool(normalized) and all(code == "zero_results" for code in normalized)


def _classify_platform_quality_reasons(
    platform: str,
    *,
    status: str,
    evidence: list[str],
) -> list[str]:
    return classify_runtime_platform_quality_reasons(platform, status=status, evidence=evidence)


def _build_quality_source_breakdown(
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
        "state": _worse_quality_state(*[item.get("state") for item in platform_quality.values()] or ["partial"]),
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


def _frame_column(frame: object | None, column: str):
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


def _series_any_true(series: object | None) -> bool:
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


def _series_any_positive(series: object | None) -> bool:
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


def _summarize_analysis_quality(frame: object | None) -> dict[str, object]:
    return summarize_runtime_analysis_quality(frame)
    available = frame is not None and not bool(getattr(frame, "empty", True))
    rows = 0
    if available:
        try:
            rows = len(frame)  # type: ignore[arg-type]
        except Exception:
            rows = 0

    google_trends_available = _series_any_true(_frame_column(frame, "has_google_trends"))
    if not google_trends_available:
        google_trends_available = _series_any_positive(_frame_column(frame, "google_interest"))

    amazon_bsr_available = _series_any_positive(_frame_column(frame, "amazon_bsr_count"))

    quality_flags: list[str] = []
    quality_evidence: list[str] = []
    if not available:
        quality_flags.append("analysis_empty")
        quality_evidence.append("analysis_empty")
        state = "unknown"
    else:
        state = "full" if google_trends_available and amazon_bsr_available else "partial"
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


def _summarize_keyword_quality(summary: dict[str, object], analysis_frame: object | None = None) -> dict[str, object]:
    return summarize_runtime_keyword_quality(summary, analysis_frame)
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
        evidence = _dedupe_strings(
            list(raw_stats.get("error_evidence") or []) + list(raw_stats.get("zero_result_evidence") or []) + ([error] if error else [])
        )
        has_bs4_issue = any("beautifulsoup4_unavailable" in item.lower() for item in evidence)
        if has_bs4_issue:
            bs4_unavailable = True
        reason_codes = _classify_platform_quality_reasons(platform, status=status, evidence=evidence)
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
            "failure_details": _dedupe_failure_details([], raw_stats.get("failure_details")),
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
        crawl_state = _worse_quality_state(*crawl_states)

    analysis_summary = _summarize_analysis_quality(analysis_frame)
    if analysis_summary["quality_flags"]:
        quality_flags.extend(list(analysis_summary["quality_flags"]))
    if analysis_summary["quality_evidence"]:
        quality_evidence.extend(list(analysis_summary["quality_evidence"]))
    if not bool(analysis_summary["available"]):
        quality_reasons.append("analysis:analysis_empty")
    if not bool(analysis_summary["google_trends_available"]):
        quality_reasons.append("analysis:google_trends_missing")
    if not bool(analysis_summary["amazon_bsr_available"]):
        quality_reasons.append("analysis:amazon_bsr_missing")

    if bs4_unavailable:
        quality_flags.append("beautifulsoup4_unavailable")
        quality_evidence.append("beautifulsoup4_unavailable")

    if crawl_state == "degraded" or bs4_unavailable:
        quality_state = "degraded"
    else:
        analysis_state = str(analysis_summary["state"] or "")
        quality_state = crawl_state if analysis_state == "unknown" else _worse_quality_state(crawl_state, analysis_state)

    signals = {
        "noon_success": noon_success,
        "amazon_success": amazon_success,
        "amazon_bsr_available": analysis_summary["amazon_bsr_available"],
        "google_trends_available": analysis_summary["google_trends_available"],
        "beautifulsoup4_unavailable": bs4_unavailable,
    }
    quality_source_breakdown = _build_quality_source_breakdown(
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
        "quality_flags": _dedupe_strings(quality_flags),
        "quality_reasons": _dedupe_strings(quality_reasons),
        "quality_evidence": _dedupe_strings(quality_evidence),
        "quality_source_breakdown": quality_source_breakdown,
        "analysis": analysis_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Noon 关键词爬虫独立入口",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python keyword_main.py
  python keyword_main.py --step scrape
  python keyword_main.py --step analyze
  python keyword_main.py --data-root D:/runs/noon_keyword_runtime
        """,
    )
    parser.add_argument(
        "--step",
        choices=[
            "keywords",
            "register",
            "list-keywords",
            "scrape",
            "analyze",
            "refine",
            "report",
            "track",
            "monitor",
            "estimate",
            "expand",
            "cross-analyze",
        ],
        help="仅执行指定步骤",
    )
    parser.add_argument("--resume", action="store_true", help="断点续跑")
    parser.add_argument("--snapshot", type=str, default=None, help="指定快照 ID")
    parser.add_argument("--keyword", type=str, default=None, help="单个关键词")
    parser.add_argument("--keywords-file", type=str, default=None, help="关键词文件，一行一个")
    parser.add_argument(
        "--tracking-mode",
        type=str,
        choices=["tracked", "adhoc"],
        default=None,
        help="关键词模式：tracked 进入长期轮巡，adhoc 仅临时抓取",
    )
    parser.add_argument("--priority", type=int, default=100, help="关键词优先级，数字越小越优先")
    parser.add_argument("--limit", type=int, default=None, help="本次最多处理多少个关键词")
    parser.add_argument("--stale-hours", type=int, default=None, help="仅抓取距离上次抓取超过 N 小时的 tracked 关键词")
    parser.add_argument("--baseline-file", type=str, default=None, help="tracked 基准词文件")
    parser.add_argument("--monitor-config", type=str, default=None, help="monitor 配置 JSON")
    parser.add_argument("--monitor-seed-keyword", type=str, default=None, help="仅运行指定 monitor seed 单元")
    parser.add_argument("--expand-limit", type=int, default=None, help="monitor 阶段最多扩展多少个 tracked 种子词")
    parser.add_argument("--monitor-report", action="store_true", help="monitor 轮巡完成后生成 Excel 报告")
    parser.add_argument("--noon-count", type=int, default=100, help="Noon 每关键词爬取数量")
    parser.add_argument("--amazon-count", type=int, default=100, help="Amazon 每关键词爬取数量")
    parser.add_argument(
        "--platforms",
        type=str,
        nargs="+",
        default=None,
        help="抓取平台列表，如 noon amazon",
    )
    parser.add_argument(
        "--runtime-scope",
        type=str,
        default="keyword",
        help="运行作用域，默认 keyword",
    )
    parser.add_argument(
        "--data-root",
        type=str,
        default=None,
        help="运行数据根目录；不传则默认 noon-selection-tool/runtime_data/keyword",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="SQLite 数据库路径；不传则默认跟随 data-root",
    )
    parser.add_argument(
        "--warehouse-db",
        type=str,
        default=None,
        help="warehouse database path; defaults to configured warehouse db",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="兼容旧参数：等同于 --data-root",
    )
    parser.add_argument("--persist", action="store_true", help="持久化到 SQLite")
    parser.add_argument("--verbose", "-v", action="store_true", help="输出详细日志")
    parser.add_argument(
        "--expand-keyword",
        type=str,
        default=None,
        help='核心关键词，用于在线扩展联想词，如 "car"',
    )
    parser.add_argument(
        "--expand-platforms",
        type=str,
        nargs="+",
        default=["noon", "amazon"],
        help="扩展平台列表（noon/amazon）",
    )
    parser.add_argument("--export-excel", action="store_true", help="导出爬取数据为 Excel")
    parser.add_argument("--expand-stale-hours", type=int, default=None, help="monitor 鎵╁睍闂撮殧锛屼粎鎵╁睍瓒呰繃 N 灏忔椂鏈噸鏂版墿灞曠殑 seed")
    parser.add_argument("--expand-source-types", type=str, nargs="+", default=None, help="monitor 鎵╁睍 seed 鏉ユ簮绫诲瀷锛屽 baseline manual generated tracked")
    return parser


def prepare_settings(args: argparse.Namespace) -> Settings:
    settings = Settings()
    settings.set_runtime_scope(args.runtime_scope or "keyword")

    data_root = args.data_root or args.output_dir
    if data_root:
        settings.set_data_dir(data_root)
    if args.db_path:
        settings.set_product_store_db_path(args.db_path)
    if args.warehouse_db:
        settings.set_warehouse_db_path(args.warehouse_db)

    if args.snapshot:
        snap_dir = settings.data_dir / "snapshots" / args.snapshot
        snap_dir.mkdir(parents=True, exist_ok=True)
        settings.set_snapshot_id(args.snapshot)

    for sub in KEYWORD_SNAPSHOT_SUBDIRS:
        (settings.snapshot_dir / sub).mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "processed").mkdir(parents=True, exist_ok=True)
    return settings


def _keyword_pool_path(settings: Settings) -> Path:
    return runtime_keyword_pool_path(settings)


def _keyword_snapshot_path(settings: Settings) -> Path:
    return runtime_keyword_snapshot_path(settings)


def _atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> Path:
    return runtime_atomic_write_text(path, content, encoding=encoding)


def _atomic_write_json(path: Path, payload: object) -> Path:
    return runtime_atomic_write_json(path, payload)


def _atomic_write_lines(path: Path, lines: list[str]) -> Path:
    return runtime_atomic_write_lines(path, lines)


def _safe_load_json_file(
    path: Path,
    *,
    expected_type: type | tuple[type, ...] | None = None,
    required_keys: list[str] | tuple[str, ...] | None = None,
) -> tuple[object | None, str]:
    return runtime_safe_load_json_file(
        path,
        expected_type=expected_type,
        required_keys=required_keys,
    )


def _load_keyword_list_payload(path: Path) -> tuple[list[str], str]:
    return runtime_load_keyword_list_payload(path)


def _read_keyword_pool(settings: Settings) -> list[str]:
    return runtime_read_keyword_pool(settings)


def _write_keyword_pool(settings: Settings, keywords: list[str]) -> Path:
    return runtime_write_keyword_pool(settings, keywords)


def _collect_manual_keywords(args: argparse.Namespace) -> list[str]:
    return runtime_collect_manual_keywords(args)
    keywords: list[str] = []
    if getattr(args, "keyword", None):
        keywords.extend(part.strip() for part in str(args.keyword).split(","))

    if getattr(args, "keywords_file", None):
        file_path = Path(args.keywords_file)
        if not file_path.exists():
            raise SystemExit(f"关键词文件不存在: {file_path}")
        if file_path.suffix.lower() == ".json":
            payload = json.loads(file_path.read_text(encoding="utf-8-sig"))
            if isinstance(payload, list):
                keywords.extend(str(item).strip() for item in payload)
        else:
            for line in file_path.read_text(encoding="utf-8-sig").splitlines():
                clean = line.strip()
                if clean and not clean.startswith("#"):
                    keywords.append(clean)

    normalized = sorted({kw.strip().lower() for kw in keywords if kw and kw.strip()})
    if getattr(args, "limit", None) is not None:
        normalized = normalized[: args.limit]
    return normalized


def _load_keywords_from_file(file_path: str | Path) -> list[str]:
    return runtime_load_keywords_from_file(file_path)
    path = Path(file_path)
    if not path.exists():
        raise SystemExit(f"关键词文件不存在: {path}")

    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, list):
            return sorted({str(item).strip().lower() for item in payload if str(item).strip()})
        return []

    keywords = [
        line.strip().lower()
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    return sorted(set(keywords))


def _load_monitor_profile(settings: Settings, args: argparse.Namespace) -> dict:
    return load_keyword_monitor_profile(settings, args, logger=logger)
    default_baseline = settings.project_root / "config" / "keyword_baseline_seeds.txt"
    profile = {
        "baseline_file": str(default_baseline) if default_baseline.exists() else args.baseline_file,
        "tracked_priority": 30,
        "expand_limit": args.expand_limit if args.expand_limit is not None else 10,
        "expand_stale_hours": getattr(args, "expand_stale_hours", None) if getattr(args, "expand_stale_hours", None) is not None else 72,
        "expand_source_types": getattr(args, "expand_source_types", None) or ["baseline", "manual", "generated", "tracked"],
        "expand_platforms": args.expand_platforms or ["noon", "amazon"],
        "crawl_platforms": args.platforms or ["noon", "amazon"],
        "crawl_stale_hours": args.stale_hours if args.stale_hours is not None else 24,
        "crawl_limit": args.limit if args.limit is not None else 200,
        "monitor_report": bool(args.monitor_report),
    }

    config_path = Path(args.monitor_config) if args.monitor_config else (settings.project_root / "config" / "keyword_monitor_defaults.json")
    if config_path.exists():
        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                profile.update({k: v for k, v in loaded.items() if v not in (None, "")})
        except Exception as exc:
            logger.warning("monitor 配置读取失败，使用默认参数: %s", exc)

    if args.baseline_file:
        profile["baseline_file"] = args.baseline_file
    if args.expand_limit is not None:
        profile["expand_limit"] = args.expand_limit
    if getattr(args, "expand_stale_hours", None) is not None:
        profile["expand_stale_hours"] = args.expand_stale_hours
    if getattr(args, "expand_source_types", None):
        profile["expand_source_types"] = args.expand_source_types
    if args.expand_platforms:
        profile["expand_platforms"] = args.expand_platforms
    if args.platforms:
        profile["crawl_platforms"] = args.platforms
    if args.stale_hours is not None:
        profile["crawl_stale_hours"] = args.stale_hours
    if args.limit is not None:
        profile["crawl_limit"] = args.limit
    if args.monitor_report:
        profile["monitor_report"] = True
    return profile


def _monitor_dir(settings: Settings) -> Path:
    return get_keyword_monitor_dir(settings)
    path = settings.data_dir / "monitor"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _monitor_lock_path(settings: Settings) -> Path:
    return get_keyword_monitor_lock_path(settings, lock_filename=MONITOR_LOCK_FILENAME)
    return _monitor_dir(settings) / MONITOR_LOCK_FILENAME


def _monitor_summary_path(settings: Settings) -> Path:
    return get_keyword_monitor_summary_path(settings, summary_filename=MONITOR_SUMMARY_FILENAME)
    return _monitor_dir(settings) / MONITOR_SUMMARY_FILENAME


def _safe_int(value, default: int = 0) -> int:
    return runtime_safe_int(value, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _keyword_metadata(row: dict | None) -> dict:
    if not isinstance(row, dict):
        return {}
    metadata = row.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _keyword_depth(row: dict | None) -> int:
    return max(0, _safe_int(_keyword_metadata(row).get("expansion_depth"), 0))


def _keyword_root_seed(row: dict | None) -> str:
    metadata = _keyword_metadata(row)
    root_seed = (
        metadata.get("root_seed_keyword")
        or metadata.get("seed_keyword")
        or (row or {}).get("display_keyword")
        or (row or {}).get("keyword")
    )
    return _normalize_keyword_value(root_seed)


def _extract_root_candidates(row: dict | None, keyword: str | None = None) -> list[str]:
    metadata = _keyword_metadata(row)
    candidates = [
        metadata.get("root_seed_keyword"),
        metadata.get("seed_keyword"),
        (row or {}).get("display_keyword"),
        (row or {}).get("keyword"),
        keyword,
    ]
    seen: set[str] = set()
    items: list[str] = []
    for candidate in candidates:
        token = _normalize_keyword_value(candidate)
        if not token or token in seen:
            continue
        seen.add(token)
        items.append(token)
    return items


def _keyword_display(row: dict | None) -> str:
    if not isinstance(row, dict):
        return ""
    return str(row.get("display_keyword") or row.get("keyword") or "").strip().lower()


def _is_depth_one_expand_candidate(row: dict) -> bool:
    if _keyword_depth(row) != 1:
        return False
    return bool(row.get("last_crawled_at") or row.get("last_analyzed_at"))


def _select_expand_seed_rows(rows: list[dict], limit: int | None) -> list[dict]:
    depth_zero: list[dict] = []
    depth_one: list[dict] = []
    seen: set[str] = set()

    for row in rows:
        keyword_value = _keyword_display(row)
        if not keyword_value or keyword_value in seen:
            continue

        depth = _keyword_depth(row)
        if depth == 0:
            depth_zero.append(row)
            seen.add(keyword_value)
            continue
        if depth == 1 and _is_depth_one_expand_candidate(row):
            depth_one.append(row)
            seen.add(keyword_value)

    selected = depth_zero + depth_one
    if limit is not None:
        selected = selected[: max(limit, 0)]
    return selected


def _normalize_monitor_seed_keyword(value: str | None) -> str:
    return _normalize_keyword_value(value or "")


def _filter_monitor_baseline_keywords(keywords: list[str], monitor_seed_keyword: str | None) -> list[str]:
    normalized_seed = _normalize_monitor_seed_keyword(monitor_seed_keyword)
    if not normalized_seed:
        return list(keywords)
    filtered = [item for item in keywords if _normalize_keyword_value(item) == normalized_seed]
    if filtered:
        return filtered
    return [normalized_seed]


def _keyword_matches_monitor_seed(row: dict | None, monitor_seed_keyword: str | None) -> bool:
    normalized_seed = _normalize_monitor_seed_keyword(monitor_seed_keyword)
    if not normalized_seed:
        return True
    if not isinstance(row, dict):
        return False
    candidates = {
        _normalize_keyword_value((row.get("display_keyword") or row.get("keyword") or "")),
        _keyword_root_seed(row),
        _normalize_keyword_value(_keyword_metadata(row).get("seed_keyword")),
    }
    return normalized_seed in {item for item in candidates if item}


def _filter_monitor_seed_rows(rows: list[dict], monitor_seed_keyword: str | None) -> list[dict]:
    return [row for row in rows if _keyword_matches_monitor_seed(row, monitor_seed_keyword)]


def _filter_monitor_keywords_by_exclusion(
    items: list,
    *,
    exclusion_lookup: dict[str, object],
    source_scope: str | None = None,
) -> tuple[list, list[dict[str, object]]]:
    kept: list = []
    blocked: list[dict[str, object]] = []
    normalized_scope = _normalize_keyword_value(source_scope or "")
    for item in items:
        if isinstance(item, dict):
            keyword = str(item.get("display_keyword") or item.get("keyword") or "").strip().lower()
            source_scopes = classify_keyword_source_scopes(item)
            root_keywords = _extract_root_candidates(item, keyword)
        else:
            keyword = _normalize_keyword_value(item)
            source_scopes = [normalized_scope] if normalized_scope else []
            root_keywords = [keyword] if keyword else []
        rule = match_keyword_control_rule(keyword, source_scopes, exclusion_lookup, root_keywords=root_keywords)
        if rule:
            blocked.append(
                {
                    "keyword": keyword,
                    "root_keywords": list(root_keywords),
                    "source_scopes": list(source_scopes),
                    "rule": rule,
                }
            )
            continue
        kept.append(item)
    return kept, blocked


def _build_monitor_error(stage: str, error: Exception, **extra) -> dict[str, object]:
    payload: dict[str, object] = {"stage": stage, "error": str(error)}
    payload.update({key: value for key, value in extra.items() if value not in (None, "")})
    return payload


def _pid_is_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _acquire_monitor_lock(settings: Settings, snapshot_id: str) -> dict[str, object]:
    return acquire_keyword_monitor_lock(settings, snapshot_id, lock_filename=MONITOR_LOCK_FILENAME)
    lock_path = _monitor_lock_path(settings)
    if lock_path.exists():
        existing_payload, _ = _safe_load_json_file(lock_path, expected_type=dict)
        existing = existing_payload if isinstance(existing_payload, dict) else {}
        existing_pid = _safe_int(existing.get("pid"), 0)
        if _pid_is_alive(existing_pid):
            raise SystemExit(
                f"keyword monitor is already running: pid={existing_pid}, snapshot={existing.get('snapshot_id')}"
            )
        lock_path.unlink(missing_ok=True)

    payload = {
        "pid": os.getpid(),
        "snapshot_id": snapshot_id,
        "started_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "status": "running",
        "current_stage": "starting",
    }
    _atomic_write_json(lock_path, payload)
    return payload


def _release_monitor_lock(settings: Settings):
    release_keyword_monitor_lock(settings, lock_filename=MONITOR_LOCK_FILENAME)
    return
    _monitor_lock_path(settings).unlink(missing_ok=True)


def _refresh_monitor_lock(settings: Settings, lock_payload: dict[str, object], **updates) -> dict[str, object]:
    return refresh_keyword_monitor_lock(
        settings,
        lock_payload,
        lock_filename=MONITOR_LOCK_FILENAME,
        **updates,
    )
    payload = dict(lock_payload or {})
    payload.update({key: value for key, value in updates.items() if value is not None})
    payload["updated_at"] = datetime.now().isoformat()
    _atomic_write_json(_monitor_lock_path(settings), payload)
    return payload


def _write_monitor_summary(settings: Settings, summary: dict[str, object]) -> Path:
    return write_keyword_monitor_summary(settings, summary, summary_filename=MONITOR_SUMMARY_FILENAME)
    summary = dict(summary)
    summary["updated_at"] = datetime.now().isoformat()
    return _atomic_write_json(_monitor_summary_path(settings), summary)


def _checkpoint_monitor_state(
    settings: Settings,
    summary: dict[str, object],
    lock_payload: dict[str, object],
    *,
    stage: str,
    note: str = "",
) -> dict[str, object]:
    return checkpoint_keyword_monitor_state(
        settings,
        summary,
        lock_payload,
        stage=stage,
        lock_filename=MONITOR_LOCK_FILENAME,
        summary_filename=MONITOR_SUMMARY_FILENAME,
        note=note,
    )
    summary["current_stage"] = stage
    if note:
        summary["stage_note"] = note
    elif "stage_note" in summary:
        summary["stage_note"] = ""
    refreshed_lock = _refresh_monitor_lock(
        settings,
        lock_payload,
        status=str(summary.get("status") or "running"),
        current_stage=stage,
        stage_note=note,
        expanded_seed_count=int(summary.get("expanded_seed_count") or 0),
        expanded_keyword_count=int(summary.get("expanded_keyword_count") or 0),
        crawled_keyword_count=int(summary.get("crawled_keyword_count") or 0),
        persisted_product_count=int(summary.get("persisted_product_count") or 0),
        analyzed_keyword_count=int(summary.get("analyzed_keyword_count") or 0),
    )
    summary["lock_payload"] = refreshed_lock
    _write_monitor_summary(settings, summary)
    return refreshed_lock


def _official_keyword_db_path(settings: Settings) -> Path:
    return get_official_keyword_db_path(settings)
    return settings.project_root / "runtime_data" / "keyword" / "product_store.db"


def _sync_warehouse(settings: Settings, *, reason: str, wait_for_lock: bool = True) -> dict[str, object]:
    return sync_keyword_warehouse(
        settings,
        reason=reason,
        wait_for_lock=wait_for_lock,
        actor="keyword_window",
        lock_wait_seconds=DEFAULT_SYNC_LOCK_WAIT_SECONDS,
        lock_retry_seconds=DEFAULT_SYNC_LOCK_RETRY_SECONDS,
        logger=logger,
    )
    current_db = settings.product_store_db_ref
    warehouse_db = settings.warehouse_db_ref
    command = [
        sys.executable,
        str(settings.project_root / "run_shared_warehouse_sync.py"),
        "--actor",
        "keyword_window",
        "--reason",
        reason,
        "--trigger-db",
        current_db,
        "--warehouse-db",
        warehouse_db,
    ]
    deadline = time.time() + (DEFAULT_SYNC_LOCK_WAIT_SECONDS if wait_for_lock else 0)
    while True:
        completed = subprocess.run(
            command,
            cwd=str(settings.project_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        stdout_lines = [line for line in completed.stdout.splitlines() if line.strip()]
        stderr_lines = [line for line in completed.stderr.splitlines() if line.strip()]
        log_lines = stdout_lines + stderr_lines
        result_payload: dict[str, object] | None = None
        for line in reversed(stdout_lines):
            if line.startswith("WAREHOUSE_SYNC_RESULT="):
                try:
                    result_payload = json.loads(line[len("WAREHOUSE_SYNC_RESULT="):])
                except json.JSONDecodeError:
                    result_payload = None
                break
        if completed.returncode != 0:
            detail = "\n".join(log_lines[-20:])
            payload = {
                "status": str((result_payload or {}).get("status") or "failed"),
                "reason": str((result_payload or {}).get("reason") or reason),
                "keyword_db": str(current_db),
                "warehouse_db": str(warehouse_db),
                "command": command,
                "log_tail": log_lines[-20:],
                "returncode": completed.returncode,
                "error": detail or str(completed.returncode),
                "skip_reason": str((result_payload or {}).get("skip_reason") or ""),
                "sync_state": (result_payload or {}).get("sync_state") or {},
            }
            logger.error("keyword warehouse sync failed (%s): %s", reason, payload["error"])
            return payload

        payload = {
            "status": str((result_payload or {}).get("status") or "completed"),
            "reason": str((result_payload or {}).get("reason") or reason),
            "keyword_db": str(current_db),
            "warehouse_db": str(warehouse_db),
            "command": command,
            "log_tail": log_lines[-10:],
            "skip_reason": str((result_payload or {}).get("skip_reason") or ""),
            "sync_state": (result_payload or {}).get("sync_state") or {},
        }
        if payload["status"] == "skipped" and str(payload.get("skip_reason") or "") == "lock_active":
            if wait_for_lock and time.time() < deadline:
                time.sleep(max(DEFAULT_SYNC_LOCK_RETRY_SECONDS, 1))
                continue
            logger.info("skip warehouse sync: shared lock active -> %s", payload.get("skip_reason") or "unknown")
            return payload
        logger.info("keyword warehouse synced: %s", warehouse_db)
        return payload


def _result_file_path(settings: Settings, platform: str, keyword: str) -> Path:
    return runtime_result_file_path(settings, platform, keyword)
    return settings.snapshot_dir / platform / f"{build_keyword_result_stem(keyword)}.json"


def _load_platform_result_payloads(
    settings: Settings,
    platform: str,
    keywords: list[str],
) -> list[dict[str, object]]:
    return runtime_load_platform_result_payloads(settings, platform, keywords)
    payloads: list[dict[str, object]] = []
    for keyword in keywords:
        path = _result_file_path(settings, platform, keyword)
        payload: dict[str, object] | None = None
        load_error = ""
        if path.exists():
            raw, load_error = _safe_load_json_file(path, expected_type=dict)
            if isinstance(raw, dict):
                contract_error = get_keyword_result_payload_error(
                    platform,
                    keyword,
                    raw,
                    require_meta=False,
                )
                if contract_error:
                    load_error = f"incomplete_json:{path.name}:{contract_error}"
                else:
                    payload = normalize_keyword_result_payload(
                        platform,
                        keyword,
                        raw,
                        expected_result_file=str(path),
                    )
        else:
            load_error = f"missing_result_file:{path.name}"
        payloads.append(
            {
                "keyword": keyword,
                "path": path,
                "exists": path.exists(),
                "payload": payload,
                "load_error": load_error,
            }
        )
    return payloads


def _load_resume_keywords(settings: Settings) -> tuple[list[str], dict[str, object]]:
    return runtime_load_resume_keywords(settings)
    candidates = [
        ("snapshot", _keyword_snapshot_path(settings)),
        ("pool", _keyword_pool_path(settings)),
    ]
    details: list[dict[str, str]] = []

    for source, path in candidates:
        keywords, error = _load_keyword_list_payload(path)
        details.append(
            {
                "source": source,
                "path": str(path),
                "status": "completed" if keywords else "invalid",
                "error": error,
            }
        )
        if keywords:
            return keywords, {"source": source, "path": str(path), "candidates": details}

    return [], {"source": "generated", "path": "", "candidates": details}


def _filter_resume_completed_keywords(
    settings: Settings,
    keywords: list[str],
    platforms: list[str],
) -> tuple[list[str], dict[str, object]]:
    return runtime_filter_resume_completed_keywords(settings, keywords, platforms)
    remaining: list[str] = []
    completed: list[str] = []
    for keyword in keywords:
        has_all_platform_results = all(_result_file_path(settings, platform, keyword).exists() for platform in platforms)
        if has_all_platform_results:
            completed.append(keyword)
        else:
            remaining.append(keyword)
    return remaining, {
        "completed_keyword_count": len(completed),
        "completed_keywords_preview": completed[:20],
        "remaining_keyword_count": len(remaining),
    }


def _dedupe_strings(values: list[object]) -> list[str]:
    return dedupe_runtime_strings(values)
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def _classify_platform_payload(
    platform: str,
    keyword: str,
    payload: dict[str, object] | None,
    *,
    load_error: str = "",
) -> dict[str, object]:
    return runtime_classify_platform_payload(platform, keyword, payload, load_error=load_error)
    payload = (
        normalize_keyword_result_payload(
            platform,
            keyword,
            payload,
        )
        if isinstance(payload, dict)
        else None
    )
    products = payload.get("products") if isinstance(payload, dict) else []
    products_count = len(products) if isinstance(products, list) else 0
    total_results = _safe_int(payload.get("total_results"), 0) if isinstance(payload, dict) else 0
    page_state = str(payload.get("page_state") or "").strip().lower() if isinstance(payload, dict) else ""
    page_url = str(payload.get("page_url") or "").strip() if isinstance(payload, dict) else ""
    page_number = payload.get("page_number") if isinstance(payload, dict) else None

    zero_result_evidence = []
    error_evidence = []
    failure_details: list[dict[str, object]] = []
    if isinstance(payload, dict):
        zero_result_evidence = _dedupe_strings(list(payload.get("zero_result_evidence") or []))
        error_evidence = _dedupe_strings(list(payload.get("error_evidence") or []))
        failure_details = _dedupe_failure_details([], payload.get("failure_details"))
        if payload.get("error"):
            error_evidence.append(str(payload.get("error")))
    if load_error:
        error_evidence.append(load_error)
    error_evidence = _dedupe_strings(error_evidence)

    status = "failed"
    partial_signal = page_state == "partial_results" or any(
        any(
            token in item.lower()
            for token in (
                "partial_results",
                "page_recognition_failed:",
                "selector_miss:",
                "fallback_misfire",
                "expected_result_file:",
                "missing_result_file:",
            )
        )
        for item in error_evidence
    )
    if products_count > 0:
        status = "partial" if partial_signal else "completed"
    elif page_state == "partial_results":
        status = "partial"
    elif zero_result_evidence or page_state == "zero_results":
        status = "zero_results"
    elif page_state == "results" and products_count <= 0:
        error_evidence = _dedupe_strings(error_evidence + ["empty_results_without_products"])

    error_message = ""
    if status == "failed":
        if error_evidence:
            error_message = "; ".join(error_evidence[:5])
        else:
            error_message = f"{platform}:{keyword}:empty_result_without_evidence"
    if status in {"failed", "partial"} and not failure_details:
        reason_codes = _classify_platform_quality_reasons(platform, status=status, evidence=error_evidence)
        failure_details = [
            _build_failure_detail(
                platform=platform,
                keyword=keyword,
                failure_category=reason_codes[0] if reason_codes else ("amazon_parse_failure" if platform == "amazon" else "runtime_error"),
                short_evidence=(error_evidence[0] if error_evidence else error_message or f"{platform}:{keyword}:unknown_failure"),
                expected_result_file="",
                page_url=page_url,
                page_number=page_number if isinstance(page_number, int) else None,
                page_state=page_state or status,
            )
        ]

    return {
        "keyword": keyword,
        "status": status,
        "products_count": products_count,
        "total_results": total_results,
        "page_state": page_state,
        "zero_result_evidence": zero_result_evidence,
        "error_evidence": error_evidence,
        "error": error_message,
        "failure_details": failure_details,
    }


def _summarize_platform_snapshot(
    settings: Settings,
    platform: str,
    keywords: list[str],
) -> dict[str, object]:
    return summarize_runtime_platform_snapshot(settings, platform, keywords)
    records = _load_platform_result_payloads(settings, platform, keywords)
    result_files: list[str] = []
    products_count = 0
    total_results = 0
    zero_result_evidence: list[str] = []
    error_evidence: list[str] = []
    failed_keywords: list[str] = []
    zero_result_keywords: list[str] = []
    error_messages: list[str] = []
    failure_details: list[dict[str, object]] = []
    payload_statuses: list[str] = []

    for record in records:
        path = record["path"]
        if bool(record.get("exists")):
            result_files.append(str(path))

        payload_summary = _classify_platform_payload(
            platform,
            str(record.get("keyword") or ""),
            record.get("payload") if isinstance(record.get("payload"), dict) else None,
            load_error=str(record.get("load_error") or ""),
        )
        payload_statuses.append(str(payload_summary["status"]))
        products_count += _safe_int(payload_summary.get("products_count"), 0)
        total_results += _safe_int(payload_summary.get("total_results"), 0)
        zero_result_evidence.extend(list(payload_summary.get("zero_result_evidence") or []))
        error_evidence.extend(list(payload_summary.get("error_evidence") or []))
        detail_items = _dedupe_failure_details([], payload_summary.get("failure_details"))
        if payload_summary["status"] in {"failed", "partial"} and not detail_items:
            reason_codes = _classify_platform_quality_reasons(
                platform,
                status=str(payload_summary.get("status") or "failed"),
                evidence=list(payload_summary.get("error_evidence") or []),
            )
            detail_items = [
                _build_failure_detail(
                    platform=platform,
                    keyword=str(payload_summary.get("keyword") or ""),
                    failure_category=reason_codes[0] if reason_codes else ("amazon_parse_failure" if platform == "amazon" else "runtime_error"),
                    short_evidence=(
                        (list(payload_summary.get("error_evidence") or [""])[0])
                        or str(payload_summary.get("error") or "")
                        or f"{platform}:{payload_summary.get('keyword')}:failed"
                    ),
                    expected_result_file=str(path),
                    page_url=str((record.get("payload") or {}).get("page_url") or ""),
                    page_number=(record.get("payload") or {}).get("page_number"),
                    page_state=str(payload_summary.get("page_state") or payload_summary.get("status") or ""),
                    snapshot_id=str(getattr(settings, "snapshot_id", "") or ""),
                )
            ]
        else:
            for item in detail_items:
                item.setdefault("expected_result_file", str(path))
                item.setdefault("snapshot_id", str(getattr(settings, "snapshot_id", "") or ""))
        failure_details = _dedupe_failure_details(failure_details, detail_items)

        if payload_summary["status"] == "failed":
            failed_keywords.append(str(payload_summary["keyword"]))
            if payload_summary.get("error"):
                error_messages.append(str(payload_summary["error"]))
        elif payload_summary["status"] == "zero_results":
            zero_result_keywords.append(str(payload_summary["keyword"]))

    zero_result_evidence = _dedupe_strings(zero_result_evidence)
    error_evidence = _dedupe_strings(error_evidence)
    error_messages = _dedupe_strings(error_messages)

    status = "failed"
    if not records:
        error_messages = ["no_result_payloads_found"]
    elif "partial" in payload_statuses and any(item in {"completed", "zero_results"} for item in payload_statuses):
        status = "partial"
    elif "partial" in payload_statuses:
        status = "partial"
    elif "failed" in payload_statuses and any(item in {"completed", "zero_results"} for item in payload_statuses):
        status = "partial"
    elif "failed" in payload_statuses:
        status = "failed"
    elif products_count > 0:
        status = "completed"
    elif zero_result_keywords:
        status = "zero_results"

    return {
        "status": status,
        "keyword_count": len(keywords),
        "result_files": result_files,
        "products_count": products_count,
        "total_results": total_results,
        "error": "; ".join(error_messages[:5]),
        "error_evidence": _dedupe_strings(error_evidence),
        "zero_result_evidence": zero_result_evidence,
        "failure_details": failure_details,
        "failed_keywords": failed_keywords,
        "zero_result_keywords": zero_result_keywords,
    }


def _clear_platform_result_files(settings: Settings, platform: str, keywords: list[str]) -> list[str]:
    deleted: list[str] = []
    for keyword in keywords:
        path = _result_file_path(settings, platform, keyword)
        if not path.exists():
            continue
        path.unlink(missing_ok=True)
        deleted.append(str(path))
    return deleted


def _sync_keyword_pool(settings: Settings, store: ProductStore, fallback_keywords: list[str] | None = None) -> list[str]:
    rows = store.list_keywords(status="active", tracking_mode="tracked")
    keywords = [row.get("display_keyword") or row.get("keyword") for row in rows]
    normalized = sorted({str(item).strip().lower() for item in keywords if str(item).strip()})
    if not normalized and fallback_keywords:
        normalized = sorted({kw.strip().lower() for kw in fallback_keywords if kw and kw.strip()})
    _write_keyword_pool(settings, normalized)
    return normalized


def _normalize_keyword_value(value) -> str:
    return str(value or "").strip().lower()


def _filter_expanded_keywords(seed_keyword: str, keywords: list[str]) -> list[str]:
    """过滤明显偏离 seed 的联想词，优先保证正式关键词库质量。"""
    seed = _normalize_keyword_value(seed_keyword)
    seed_tokens = [token for token in seed.split() if token]
    if not seed_tokens:
        return sorted({_normalize_keyword_value(item) for item in keywords if _normalize_keyword_value(item)})

    primary_token = max(seed_tokens, key=len)
    filtered: list[str] = []
    seen: set[str] = set()
    for item in keywords:
        normalized = _normalize_keyword_value(item)
        if not normalized or normalized in seen:
            continue
        if normalized == seed:
            continue
        if primary_token not in normalized:
            continue
        seen.add(normalized)
        filtered.append(normalized)
    return filtered


def _apply_expansion_filter(
    seed_keyword: str,
    keywords: list[str],
    *,
    root_seed_keyword: str | None = None,
) -> dict[str, object]:
    seed = _normalize_keyword_value(seed_keyword)
    root_seed = _normalize_keyword_value(root_seed_keyword or seed_keyword) or seed
    seed_tokens = [token for token in seed.split() if token]
    root_tokens = [token for token in root_seed.split() if token]
    significant_root_tokens = [token for token in root_tokens if len(token) >= 3]
    primary_token = max(significant_root_tokens or seed_tokens or [seed], key=len)
    stop_tokens = {
        "for",
        "with",
        "and",
        "or",
        "of",
        "to",
        "in",
        "on",
        "by",
        "under",
        "over",
        "near",
    }
    filtered: list[str] = []
    seen: set[str] = set()
    rejected_examples: list[dict[str, str]] = []
    rejected_counts: dict[str, int] = {}

    def reject(reason: str, candidate: str):
        rejected_counts[reason] = rejected_counts.get(reason, 0) + 1
        if len(rejected_examples) < 10:
            rejected_examples.append({"keyword": candidate, "reason": reason})

    for item in keywords:
        normalized = _normalize_keyword_value(item)
        if not normalized:
            reject("blank", normalized)
            continue
        if normalized in seen:
            reject("duplicate", normalized)
            continue
        if normalized in {seed, root_seed}:
            reject("same_as_seed", normalized)
            continue

        normalized_tokens = [token for token in normalized.replace("-", " ").split() if token]
        if not normalized_tokens:
            reject("blank", normalized)
            continue
        if len(normalized_tokens) > 8:
            reject("too_long", normalized)
            continue
        if any(token in stop_tokens for token in (normalized_tokens[0], normalized_tokens[-1])):
            reject("truncated_fragment", normalized)
            continue
        if any(char in normalized for char in "{}[]|<>"):
            reject("noisy_chars", normalized)
            continue
        if primary_token and primary_token not in normalized:
            reject("missing_primary_token", normalized)
            continue
        if significant_root_tokens and not all(token in normalized for token in significant_root_tokens):
            reject("missing_root_context", normalized)
            continue

        seen.add(normalized)
        filtered.append(normalized)

    return {
        "accepted": filtered,
        "accepted_count": len(filtered),
        "input_count": len(keywords),
        "rejected_count": sum(rejected_counts.values()),
        "rejected_counts": rejected_counts,
        "rejected_examples": rejected_examples,
        "root_seed_keyword": root_seed,
        "seed_keyword": seed,
    }


def _capture_live_suggestion_edges(
    settings: Settings,
    store: ProductStore,
    *,
    platforms: list[str],
    run_id: int,
    snapshot_id: str,
    fallback_tracking_mode: str,
    fallback_priority: int,
) -> dict[str, int]:
    summary = {
        "platform_files": 0,
        "parent_keywords": 0,
        "discovered_keywords": 0,
        "recorded_edges": 0,
        "rejected_keywords": 0,
    }

    for platform in platforms:
        json_dir = settings.snapshot_dir / platform
        if not json_dir.exists():
            continue

        for json_file in sorted(json_dir.glob("*.json")):
            summary["platform_files"] += 1
            payload, load_error = _safe_load_json_file(json_file, expected_type=dict)
            if not isinstance(payload, dict):
                logger.warning("failed to read suggestion payload %s: %s", json_file, load_error or "invalid_payload")
                continue

            parent_keyword = _normalize_keyword_value(payload.get("keyword") or json_file.stem)
            child_keywords = sorted(
                {
                    _normalize_keyword_value(item)
                    for item in (payload.get("suggested_keywords") or [])
                    if _normalize_keyword_value(item)
                }
            )
            if not parent_keyword or not child_keywords:
                continue

            parent_row = store.get_keyword(parent_keyword) or {}
            parent_metadata = dict(parent_row.get("metadata") or {})
            parent_depth = int(parent_metadata.get("expansion_depth", 0) or 0)
            if parent_depth >= MAX_EXPANSION_DEPTH:
                continue
            root_seed_keyword = (
                parent_metadata.get("root_seed_keyword")
                or parent_metadata.get("seed_keyword")
                or parent_keyword
            )
            filter_result = _apply_expansion_filter(
                parent_keyword,
                child_keywords,
                root_seed_keyword=root_seed_keyword,
            )
            child_keywords = list(filter_result.get("accepted", []))
            if not child_keywords:
                summary["rejected_keywords"] += int(filter_result.get("rejected_count", 0))
                continue
            child_depth = parent_depth + 1
            child_tracking_mode = parent_row.get("tracking_mode") or fallback_tracking_mode
            child_priority = max(int(parent_row.get("priority") or fallback_priority) + 10, 20)
            discovered_at = datetime.now().isoformat()

            inserted_edges = store.record_keyword_edges(
                parent_keyword,
                child_keywords,
                source_platform=platform,
                source_type="live_suggestion",
                run_id=run_id,
            )
            store.upsert_keywords(
                child_keywords,
                tracking_mode=child_tracking_mode,
                source_type="expanded",
                source_platform=platform,
                status="active",
                priority=child_priority,
                metadata={
                    "seed_keyword": parent_keyword,
                    "root_seed_keyword": root_seed_keyword,
                    "snapshot_id": snapshot_id,
                    "source_platform": platform,
                    "discovered_via": "live_suggestion",
                    "expansion_depth": child_depth,
                    "parent_depth": parent_depth,
                    "query_keyword": parent_keyword,
                },
                last_expanded_at=discovered_at,
                last_snapshot_id=snapshot_id,
            )
            store.upsert_keyword(
                parent_keyword,
                display_keyword=parent_row.get("display_keyword") or parent_keyword,
                status=parent_row.get("status") or "active",
                tracking_mode=parent_row.get("tracking_mode") or fallback_tracking_mode,
                source_type=parent_row.get("source_type") or "",
                source_platform=parent_row.get("source_platform") or "",
                priority=parent_row.get("priority") or fallback_priority,
                notes=parent_row.get("notes"),
                metadata={
                    **parent_metadata,
                    "root_seed_keyword": root_seed_keyword,
                    "last_live_suggest_platform": platform,
                    "last_live_suggest_count": len(child_keywords),
                    "last_live_suggest_rejected": int(filter_result.get("rejected_count", 0)),
                },
                last_expanded_at=discovered_at,
                last_snapshot_id=snapshot_id,
            )
            summary["parent_keywords"] += 1
            summary["discovered_keywords"] += len(child_keywords)
            summary["recorded_edges"] += inserted_edges
            summary["rejected_keywords"] += int(filter_result.get("rejected_count", 0))

    return summary


async def run_keywords(settings: Settings, *, sync_warehouse: bool = True) -> list[str]:
    store = ProductStore(settings.product_store_db_ref)
    run_id = store.start_keyword_run(
        "seed_refresh",
        trigger_mode="pipeline",
        snapshot_id=settings.snapshot_id,
        metadata={"step": "keywords"},
    )
    try:
        keywords = await keyword_core.step_keywords(settings)
        store.upsert_keywords(
            keywords,
            tracking_mode="tracked",
            source_type="generated",
            source_platform="system",
            status="active",
            priority=100,
            metadata={"expansion_depth": 0},
            last_snapshot_id=settings.snapshot_id,
        )
        merged = _sync_keyword_pool(settings, store, fallback_keywords=keywords)
        store.finish_keyword_run(
            run_id,
            status="completed",
            keyword_count=len(merged),
            metadata={"generated_keywords": len(keywords), "active_keywords": len(merged)},
        )
        logger.info("关键词池已写入数据库: %s 个活跃关键词", len(merged))
        if sync_warehouse:
            _sync_warehouse(settings, reason="seed_refresh")
        return merged
    except Exception as exc:
        store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()


def run_register(settings: Settings, args: argparse.Namespace, *, sync_warehouse: bool = True) -> list[str]:
    keywords = _collect_manual_keywords(args)
    if not keywords:
        raise SystemExit("请通过 --keyword 或 --keywords-file 提供至少一个关键词")

    tracking_mode = args.tracking_mode or "tracked"
    source_type = getattr(args, "source_type", None) or ("manual" if tracking_mode == "tracked" else "adhoc")
    store = ProductStore(settings.product_store_db_ref)
    run_id = store.start_keyword_run(
        "register",
        trigger_mode="manual",
        seed_keyword=keywords[0] if len(keywords) == 1 else None,
        snapshot_id=settings.snapshot_id,
        metadata={"keyword_count": len(keywords), "tracking_mode": tracking_mode},
    )
    try:
        store.upsert_keywords(
            keywords,
            tracking_mode=tracking_mode,
            source_type=source_type,
            source_platform="system",
            status="active",
            priority=args.priority,
            metadata={"expansion_depth": 0, "registration_source": source_type},
            last_snapshot_id=settings.snapshot_id,
        )
        tracked_pool = _sync_keyword_pool(settings, store)
        store.finish_keyword_run(
            run_id,
            status="completed",
            keyword_count=len(keywords),
            metadata={"tracking_mode": tracking_mode, "tracked_pool_size": len(tracked_pool)},
        )
        logger.info("已注册关键词: %s 个, 模式=%s", len(keywords), tracking_mode)
        logger.info("tracked 关键词池: %s 个", len(tracked_pool))
        if sync_warehouse:
            _sync_warehouse(settings, reason="register")
        return keywords
    except Exception as exc:
        store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()


def run_list_keywords(settings: Settings, args: argparse.Namespace) -> list[dict]:
    store = ProductStore(settings.product_store_db_ref)
    try:
        rows = store.list_keywords(
            status="active",
            tracking_mode=args.tracking_mode,
            limit=args.limit,
        )
        mode_label = args.tracking_mode or "all"
        logger.info("活跃关键词数量(mode=%s): %s", mode_label, len(rows))
        for row in rows[:20]:
            logger.info(
                "[%s] %s | priority=%s | source=%s | last_crawled=%s | last_expanded=%s",
                row.get("tracking_mode"),
                row.get("display_keyword") or row.get("keyword"),
                row.get("priority"),
                row.get("source_type"),
                row.get("last_crawled_at") or "-",
                row.get("last_expanded_at") or "-",
            )
        return rows
    finally:
        store.close()


async def _legacy_run_scrape(settings: Settings, args: argparse.Namespace, keywords: list[str] | None = None):
    store = ProductStore(settings.product_store_db_ref)
    keyword_list = list(keywords or [])

    if not keyword_list:
        if args.keyword or args.keywords_file:
            keyword_list = _collect_manual_keywords(args)
        else:
            keyword_list = store.get_keywords_for_crawl(
                tracking_mode="tracked",
                limit=args.limit,
                stale_hours=args.stale_hours,
            )
            if not keyword_list:
                keyword_list = _read_keyword_pool(settings)

    if not keyword_list:
        store.close()
        raise SystemExit("未找到可抓取关键词，请先执行 register / expand / keywords")

    platforms = keyword_core._normalize_platforms(args.platforms)
    requested_keyword_count = len(keyword_list)
    resume_info: dict[str, object] | None = None
    if getattr(args, "resume", False):
        keyword_list, resume_info = _filter_resume_completed_keywords(settings, keyword_list, platforms)
        if resume_info.get("completed_keyword_count"):
            logger.info(
                "[resume] skip %s keywords with completed platform result files in snapshot=%s",
                resume_info["completed_keyword_count"],
                settings.snapshot_id,
            )
    if not keyword_list:
        return {
            "snapshot_id": settings.snapshot_id,
            "status": "skipped",
            "keyword_count": 0,
            "requested_keyword_count": requested_keyword_count,
            "platforms": platforms,
            "platform_stats": {},
            "persist_counts": {},
            "persisted_product_count": 0,
            "live_suggestion_summary": {
                "platform_files": 0,
                "parent_keywords": 0,
                "discovered_keywords": 0,
                "recorded_edges": 0,
                "rejected_keywords": 0,
            },
            "errors": [],
            "resume_info": resume_info or {},
        }
    trigger_mode = "adhoc" if (args.keyword or args.keywords_file) else "tracked"
    keyword_tracking_mode = args.tracking_mode or ("adhoc" if trigger_mode == "adhoc" else "tracked")
    observed_at = datetime.now().isoformat()
    keyword_source_type = "adhoc" if keyword_tracking_mode == "adhoc" else ""
    keyword_source_platform = "mixed" if keyword_tracking_mode == "adhoc" else ""
    keyword_priority = 20 if keyword_tracking_mode == "adhoc" else min(args.priority, 50)

    run_id = store.start_keyword_run(
        "crawl",
        trigger_mode=trigger_mode,
        seed_keyword=args.keyword,
        snapshot_id=settings.snapshot_id,
        platforms=platforms,
        metadata={"requested_keyword_count": len(keyword_list)},
    )
    try:
        store.upsert_keywords(
            keyword_list,
            tracking_mode=keyword_tracking_mode,
            source_type=keyword_source_type,
            source_platform=keyword_source_platform,
            status="active",
            priority=keyword_priority,
            last_snapshot_id=settings.snapshot_id,
        )

        await keyword_core.step_scrape(
            settings,
            keyword_list,
            platforms=args.platforms,
            noon_count=args.noon_count,
            amazon_count=args.amazon_count,
        )

        if args.persist:
            keyword_core._persist_keyword_results(settings, platforms)

        suggestion_summary = _capture_live_suggestion_edges(
            settings,
            store,
            platforms=platforms,
            run_id=run_id,
            snapshot_id=settings.snapshot_id,
            fallback_tracking_mode=keyword_tracking_mode,
            fallback_priority=keyword_priority,
        )

        store.upsert_keywords(
            keyword_list,
            tracking_mode=keyword_tracking_mode,
            source_type=keyword_source_type,
            source_platform=keyword_source_platform,
            status="active",
            priority=keyword_priority,
            last_crawled_at=observed_at,
            last_snapshot_id=settings.snapshot_id,
        )
        _sync_keyword_pool(settings, store, fallback_keywords=keyword_list)
        store.finish_keyword_run(
            run_id,
            status="completed",
            keyword_count=len(keyword_list),
            metadata={
                "platforms": platforms,
                "persisted_products": bool(args.persist),
                "live_suggestion_files": suggestion_summary["platform_files"],
                "live_suggestion_parents": suggestion_summary["parent_keywords"],
                "live_suggestion_keywords": suggestion_summary["discovered_keywords"],
                "live_suggestion_edges": suggestion_summary["recorded_edges"],
            },
        )
    except Exception as exc:
        store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()


def _legacy_run_analyze(settings: Settings):
    store = ProductStore(settings.product_store_db_ref)
    run_id = store.start_keyword_run(
        "analyze",
        trigger_mode="manual",
        snapshot_id=settings.snapshot_id,
        metadata={"step": "analyze"},
    )
    try:
        df = keyword_core.step_analyze(settings)
        if df is None or getattr(df, "empty", True):
            store.finish_keyword_run(run_id, status="completed", keyword_count=0, metadata={"empty": True})
            return df

        metrics_count = store.record_keyword_metrics(
            df.to_dict(orient="records"),
            snapshot_id=settings.snapshot_id,
            analyzed_at=datetime.now().isoformat(),
        )
        store.finish_keyword_run(
            run_id,
            status="completed",
            keyword_count=metrics_count,
            metadata={"snapshot_id": settings.snapshot_id},
        )
        logger.info("关键词分析快照已写入数据库: %s 条", metrics_count)
        return df
    except Exception as exc:
        store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()


async def _legacy_run_full_pipeline(settings: Settings, args: argparse.Namespace):
    if args.resume:
        keywords, resume_info = _load_resume_keywords(settings)
        if keywords:
            logger.info("[resume] load %s keywords from %s", len(keywords), resume_info.get("source"))
        else:
            logger.warning("[resume] no valid keyword resume source found, regenerate keyword pool")
            keywords = await run_keywords(settings)
    else:
        keywords = await run_keywords(settings)

    if hasattr(keyword_core, "_save_config_snapshot"):
        keyword_core._save_config_snapshot(settings, keyword_count=len(keywords))
    kw_snap = _keyword_snapshot_path(settings)
    _atomic_write_json(kw_snap, keywords)

    await run_scrape(settings, args, keywords=keywords)
    df = run_analyze(settings)
    if df is None:
        return

    await keyword_core.step_refine(settings)
    run_analyze(settings)
    keyword_core.step_report(settings)
    if hasattr(keyword_core, "_update_current_symlink"):
        keyword_core._update_current_symlink(settings)


def run_track(settings: Settings):
    from analysis.trend_analyzer import TrendAnalyzer

    db_path = settings.product_store_db_ref
    store = ProductStore(db_path)
    analyzer = TrendAnalyzer(db_path)

    logger.info("产品追踪模式")
    logger.info("数据库: %s", db_path)
    logger.info("数据目录: %s", settings.data_dir)
    logger.info("数据库统计: %s", store.get_statistics())

    report = analyzer.generate_report(days=30)
    report_path = settings.data_dir / "trend_report.json"
    _atomic_write_json(report_path, report)

    logger.info("分析产品数: %s", report["summary"]["total_products_analyzed"])
    logger.info("告警数量: %s", len(report["alerts"]))
    logger.info("趋势报告已保存: %s", report_path)
    store.close()


def _legacy_expand_keyword_into_db(
    settings: Settings,
    *,
    seed_keyword: str,
    platforms: list[str],
    tracking_mode: str,
    priority: int,
    source_type: str,
    output_path: Path,
    run_type: str,
):
    from keywords.online_expander import expand_keywords

    store = ProductStore(settings.product_store_db_ref)
    run_id = store.start_keyword_run(
        run_type,
        trigger_mode="manual",
        seed_keyword=seed_keyword,
        snapshot_id=settings.snapshot_id,
        platforms=platforms,
        metadata={"output_path": str(output_path)},
    )
    try:
        result = expand_keywords(seed_keyword, platforms, output_path)
        filtered_result: dict[str, list[str]] = {}
        for platform in platforms:
            filtered_result[platform] = _filter_expanded_keywords(seed_keyword, result.get(platform, []))
        filtered_result["all"] = sorted(
            {
                child
                for platform in platforms
                for child in filtered_result.get(platform, [])
            }
        )
        if output_path.suffix.lower() == ".json":
            _atomic_write_json(output_path, filtered_result)
        else:
            lines = [
                f"# 关键词扩展结果 - 种子词：{seed_keyword}",
                f"# 来源：Noon ({len(filtered_result.get('noon', []))}) + Amazon ({len(filtered_result.get('amazon', []))})",
                f"# 总计：{len(filtered_result.get('all', []))}",
                "",
            ]
            lines.extend(filtered_result.get("all", []))
            _atomic_write_lines(output_path, lines)
        expanded_at = datetime.now().isoformat()
        existing_seed = store.get_keyword(seed_keyword) or {}
        existing_metadata = existing_seed.get("metadata") or {}
        parent_depth = int(existing_metadata.get("expansion_depth", 0) or 0)
        child_depth = parent_depth + 1
        store.upsert_keyword(
            seed_keyword,
            display_keyword=seed_keyword,
            tracking_mode=tracking_mode,
            source_type=source_type,
            source_platform="system",
            status="active",
            priority=max(1, priority),
            metadata={
                "expansion_depth": parent_depth,
                "last_expand_run_type": run_type,
                "expanded_child_depth": child_depth,
            },
            last_expanded_at=expanded_at,
            last_snapshot_id=settings.snapshot_id,
            commit=False,
        )
        store.upsert_keywords(
            filtered_result.get("all", []),
            tracking_mode=tracking_mode,
            source_type="expanded",
            source_platform="mixed",
            status="active",
            priority=max(priority + 10, 20),
            metadata={
                "seed_keyword": seed_keyword,
                "snapshot_id": settings.snapshot_id,
                "parent_source_type": source_type,
                "expansion_depth": child_depth,
                "parent_depth": parent_depth,
                "platforms": platforms,
            },
            last_expanded_at=expanded_at,
            last_snapshot_id=settings.snapshot_id,
        )
        for platform in platforms:
            store.record_keyword_edges(
                seed_keyword,
                filtered_result.get(platform, []),
                source_platform=platform,
                source_type="autocomplete",
                run_id=run_id,
            )
        merged = _sync_keyword_pool(settings, store, fallback_keywords=filtered_result.get("all", []))
        store.finish_keyword_run(
            run_id,
            status="completed",
            keyword_count=len(filtered_result.get("all", [])),
            metadata={
                "seed_keyword": seed_keyword,
                "noon_count": len(filtered_result.get("noon", [])),
                "amazon_count": len(filtered_result.get("amazon", [])),
                "active_keywords": len(merged),
            },
        )
    except Exception as exc:
        store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()

    return result


def _legacy_run_expand(settings: Settings, args: argparse.Namespace):
    if not args.expand_keyword:
        raise SystemExit('请指定核心关键词: --expand-keyword "car"')

    output_dir = settings.data_dir / "keywords_expanded"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.expand_keyword.replace(' ', '_')}_expanded.txt"

    tracking_mode = args.tracking_mode or "tracked"
    source_type = "manual" if tracking_mode == "tracked" else "adhoc"
    result = expand_keyword_into_db(
        settings,
        seed_keyword=args.expand_keyword,
        platforms=args.expand_platforms,
        tracking_mode=tracking_mode,
        priority=max(1, min(args.priority, 20)),
        source_type=source_type,
        output_path=output_path,
        run_type="expand",
    )

    logger.info("关键词扩展完成")
    logger.info("种子词: %s", args.expand_keyword)
    logger.info("平台: %s", ", ".join(args.expand_platforms))
    logger.info("Noon: %s 个关键词", len(result["noon"]))
    logger.info("Amazon: %s 个关键词", len(result["amazon"]))
    logger.info("总计(去重): %s 个关键词", len(result["all"]))
    logger.info("结果文件: %s", output_path)
    logger.info("关键词池文件: %s", _keyword_pool_path(settings))


def _legacy_run_monitor(settings: Settings, args: argparse.Namespace) -> dict:
    profile = _load_monitor_profile(settings, args)
    summary: dict[str, object] = {
        "started_at": datetime.now().isoformat(),
        "snapshot_id": settings.snapshot_id,
        "data_root": str(settings.data_dir),
        "profile": profile,
        "baseline_registered": 0,
        "baseline_keywords_preview": [],
        "expanded_seed_count": 0,
        "expanded_seed_keywords": [],
        "expanded_keyword_count": 0,
        "crawled_keyword_count": 0,
        "crawled_keywords_preview": [],
        "analyzed_keyword_count": 0,
        "report_generated": False,
    }

    logger.info("开始关键词轮巡任务: snapshot=%s", settings.snapshot_id)

    baseline_file = profile.get("baseline_file")
    if baseline_file:
        baseline_keywords = _load_keywords_from_file(str(baseline_file))
        if baseline_keywords:
            reg_args = argparse.Namespace(
                keyword=",".join(baseline_keywords),
                keywords_file=None,
                tracking_mode="tracked",
                source_type="baseline",
                priority=int(profile.get("tracked_priority", 30)),
                limit=None,
            )
            run_register(settings, reg_args)
            summary["baseline_registered"] = len(baseline_keywords)
            summary["baseline_keywords_preview"] = baseline_keywords[:20]
            logger.info("已刷新 tracked 基准词: %s 个", len(baseline_keywords))

    store = ProductStore(settings.product_store_db_ref)
    try:
        expand_limit = int(profile.get("expand_limit", 0) or 0)
        if expand_limit > 0:
            seed_rows = store.get_keywords_for_expand(
                tracking_mode="tracked",
                limit=expand_limit,
                stale_hours=profile.get("expand_stale_hours"),
                include_source_types=profile.get("expand_source_types"),
            )
            summary["expanded_seed_count"] = len(seed_rows)
            for row in seed_rows:
                seed_keyword = row.get("display_keyword") or row.get("keyword")
                summary["expanded_seed_keywords"].append(seed_keyword)
                output_dir = settings.data_dir / "keywords_expanded"
                output_dir.mkdir(parents=True, exist_ok=True)
                output_path = output_dir / f"{str(seed_keyword).replace(' ', '_')}_expanded.txt"
                result = expand_keyword_into_db(
                    settings,
                    seed_keyword=str(seed_keyword),
                    platforms=list(profile.get("expand_platforms", ["noon", "amazon"])),
                    tracking_mode="tracked",
                    priority=int(row.get("priority") or profile.get("tracked_priority", 30)),
                    source_type=row.get("source_type") or "manual",
                    output_path=output_path,
                    run_type="monitor_expand",
                )
                summary["expanded_keyword_count"] = int(summary["expanded_keyword_count"]) + len(result.get("all", []))
    finally:
        store.close()

    crawl_limit = profile.get("crawl_limit", 200)
    crawl_stale_hours = profile.get("crawl_stale_hours", 24)
    scrape_args = argparse.Namespace(
        keyword=None,
        keywords_file=None,
        platforms=None,
        noon_count=args.noon_count,
        amazon_count=args.amazon_count,
        persist=True,
        tracking_mode="tracked",
        priority=int(profile.get("tracked_priority", 30)),
        limit=None if crawl_limit is None else int(crawl_limit),
        stale_hours=None if crawl_stale_hours is None else int(crawl_stale_hours),
    )
    if scrape_args.limit is not None and scrape_args.limit <= 0:
        crawled_keywords = []
    else:
        store = ProductStore(settings.product_store_db_ref)
        try:
            crawled_keywords = store.get_keywords_for_crawl(
                tracking_mode="tracked",
                limit=scrape_args.limit,
                stale_hours=scrape_args.stale_hours,
            )
        finally:
            store.close()
    summary["crawled_keyword_count"] = len(crawled_keywords)
    summary["crawled_keywords_preview"] = crawled_keywords[:20]
    if crawled_keywords:
        asyncio.run(run_scrape(settings, scrape_args, keywords=crawled_keywords))
    else:
        logger.info("本轮没有满足 stale_hours 条件的 tracked 关键词")

    if crawled_keywords:
        df = run_analyze(settings)
        summary["analyzed_keyword_count"] = 0 if df is None else len(df)
    else:
        summary["analyzed_keyword_count"] = 0

    if profile.get("monitor_report") and crawled_keywords:
        keyword_core.step_report(settings)
        summary["report_generated"] = True

    summary["finished_at"] = datetime.now().isoformat()
    monitor_dir = settings.data_dir / "monitor"
    monitor_dir.mkdir(parents=True, exist_ok=True)
    summary_path = monitor_dir / "keyword_monitor_last_run.json"
    _atomic_write_json(summary_path, summary)
    logger.info("关键词轮巡任务完成: %s", summary_path)
    return summary


def _build_default_analysis_quality_summary() -> dict[str, object]:
    return runtime_build_default_analysis_quality_summary()


def _build_default_scrape_quality_summary() -> dict[str, object]:
    return runtime_build_default_scrape_quality_summary()


def _build_scrape_summary(
    settings: Settings,
    *,
    keyword_count: int,
    requested_keyword_count: int,
    platforms: list[str],
    resume_info: dict[str, object] | None,
) -> dict[str, object]:
    return runtime_build_scrape_summary(
        settings,
        keyword_count=keyword_count,
        requested_keyword_count=requested_keyword_count,
        platforms=platforms,
        resume_info=resume_info,
    )


def _build_skipped_scrape_summary(
    settings: Settings,
    *,
    requested_keyword_count: int,
    platforms: list[str],
    resume_info: dict[str, object] | None,
) -> dict[str, object]:
    return runtime_build_skipped_scrape_summary(
        settings,
        requested_keyword_count=requested_keyword_count,
        platforms=platforms,
        resume_info=resume_info,
    )


async def _crawl_platform_with_retry(
    settings: Settings,
    args: argparse.Namespace,
    keyword_list: list[str],
    platform: str,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return await runtime_crawl_platform_with_retry(
        settings,
        args,
        keyword_list,
        platform,
        step_scrape=keyword_core.step_scrape,
        summarize_platform_snapshot=_summarize_platform_snapshot,
        clear_platform_result_files=_clear_platform_result_files,
        amazon_retry_count=AMAZON_PLATFORM_RETRY_COUNT,
        amazon_retry_delay_seconds=AMAZON_PLATFORM_RETRY_DELAY_SECONDS,
        logger=logger,
    )


def _finalize_platform_scrape(
    settings: Settings,
    store: ProductStore,
    args: argparse.Namespace,
    summary: dict[str, object],
    *,
    run_id: int,
    keyword_list: list[str],
    platform: str,
    final_platform_summary: dict[str, object],
    attempt_history: list[dict[str, object]],
    keyword_tracking_mode: str,
    keyword_priority: int,
) -> dict[str, object]:
    return runtime_finalize_platform_scrape(
        settings,
        store,
        args,
        summary,
        run_id=run_id,
        keyword_list=keyword_list,
        platform=platform,
        final_platform_summary=final_platform_summary,
        attempt_history=attempt_history,
        keyword_tracking_mode=keyword_tracking_mode,
        keyword_priority=keyword_priority,
        persist_keyword_results=keyword_core._persist_keyword_results,
        capture_live_suggestion_edges=_capture_live_suggestion_edges,
        summarize_keyword_quality=_summarize_keyword_quality,
        update_task_progress=_update_task_progress,
        safe_int=_safe_int,
    )


def _finalize_scrape_run(
    settings: Settings,
    store: ProductStore,
    args: argparse.Namespace,
    summary: dict[str, object],
    *,
    run_id: int,
    keyword_list: list[str],
    platforms: list[str],
    success_platforms: list[str],
    failed_platforms: list[str],
    sync_warehouse: bool,
) -> dict[str, object]:
    return runtime_finalize_scrape_run(
        settings,
        store,
        args,
        summary,
        run_id=run_id,
        keyword_list=keyword_list,
        platforms=platforms,
        success_platforms=success_platforms,
        failed_platforms=failed_platforms,
        sync_warehouse=sync_warehouse,
        sync_keyword_pool=_sync_keyword_pool,
        sync_warehouse_fn=_sync_warehouse,
        update_task_progress=_update_task_progress,
        build_monitor_error=_build_monitor_error,
        summarize_keyword_quality=_summarize_keyword_quality,
    )

    store.finish_keyword_run(
        run_id,
        status=status,
        keyword_count=len(keyword_list) if success_platforms else 0,
        metadata={
            "requested_keyword_count": len(keyword_list),
            "platforms": platforms,
            "success_platforms": success_platforms,
            "failed_platforms": failed_platforms,
            "persisted_products": bool(args.persist),
            "persist_counts": summary["persist_counts"],
            "persisted_product_count": summary["persisted_product_count"],
            "platform_stats": summary["platform_stats"],
            "live_suggestion_summary": summary["live_suggestion_summary"],
            "warehouse_sync": warehouse_sync,
            "errors": summary["errors"],
            "quality_state": summary["quality_state"],
            "quality_reasons": summary["quality_summary"].get("quality_reasons") or [],
            "quality_source_breakdown": summary["quality_summary"].get("quality_source_breakdown") or {},
            "quality_summary": summary["quality_summary"],
        },
    )
    return summary


async def run_scrape(
    settings: Settings,
    args: argparse.Namespace,
    keywords: list[str] | None = None,
    *,
    sync_warehouse: bool = True,
) -> dict[str, object]:
    store = ProductStore(settings.product_store_db_ref)
    keyword_list = list(keywords or [])
    resume_info: dict[str, object] | None = None

    if not keyword_list:
        if args.keyword or args.keywords_file:
            keyword_list = _collect_manual_keywords(args)
        else:
            keyword_list = store.get_keywords_for_crawl(
                tracking_mode="tracked",
                limit=args.limit,
                stale_hours=args.stale_hours,
            )
            if not keyword_list:
                keyword_list = _read_keyword_pool(settings)

    if not keyword_list:
        store.close()
        raise SystemExit("no crawlable keywords found; run register / expand / keywords first")

    requested_keyword_count = len(keyword_list)
    if getattr(args, "resume", False):
        keyword_list, resume_info = _filter_resume_completed_keywords(settings, keyword_list, keyword_core._normalize_platforms(args.platforms))
        if resume_info.get("completed_keyword_count"):
            logger.info(
                "[resume] skip %s keywords with completed platform result files in snapshot=%s",
                resume_info["completed_keyword_count"],
                settings.snapshot_id,
            )
        if not keyword_list:
            store.close()
            return _build_skipped_scrape_summary(
                settings,
                requested_keyword_count=requested_keyword_count,
                platforms=keyword_core._normalize_platforms(args.platforms),
                resume_info=resume_info,
            )

    platforms = keyword_core._normalize_platforms(args.platforms)
    trigger_mode = "adhoc" if (args.keyword or args.keywords_file) else "tracked"
    keyword_tracking_mode = args.tracking_mode or ("adhoc" if trigger_mode == "adhoc" else "tracked")
    observed_at = datetime.now().isoformat()
    keyword_source_type = "adhoc" if keyword_tracking_mode == "adhoc" else ""
    keyword_source_platform = "mixed" if keyword_tracking_mode == "adhoc" else ""
    keyword_priority = 20 if keyword_tracking_mode == "adhoc" else min(args.priority, 50)

    summary: dict[str, object] = _build_scrape_summary(
        settings,
        keyword_count=len(keyword_list),
        requested_keyword_count=requested_keyword_count,
        platforms=platforms,
        resume_info=resume_info,
    )

    run_id = store.start_keyword_run(
        "crawl",
        trigger_mode=trigger_mode,
        seed_keyword=args.keyword,
        snapshot_id=settings.snapshot_id,
        platforms=platforms,
        metadata={"requested_keyword_count": len(keyword_list)},
    )
    summary["run_id"] = run_id
    success_platforms: list[str] = []
    failed_platforms: list[str] = []
    run_finished = False

    try:
        _update_task_progress(
            "runtime_collecting",
            message="starting keyword crawl",
            metrics={
                "keyword_count": len(keyword_list),
                "platform_count": len(platforms),
                "persisted_product_count": 0,
            },
            details={"platforms": platforms},
        )
        store.upsert_keywords(
            keyword_list,
            tracking_mode=keyword_tracking_mode,
            source_type=keyword_source_type,
            source_platform=keyword_source_platform,
            status="active",
            priority=keyword_priority,
            last_snapshot_id=settings.snapshot_id,
        )

        for platform in platforms:
            final_platform_summary, attempt_history = await _crawl_platform_with_retry(
                settings,
                args,
                keyword_list,
                platform,
            )
            platform_stats = _finalize_platform_scrape(
                settings,
                store,
                args,
                summary,
                run_id=run_id,
                keyword_list=keyword_list,
                platform=platform,
                final_platform_summary=final_platform_summary,
                attempt_history=attempt_history,
                keyword_tracking_mode=keyword_tracking_mode,
                keyword_priority=keyword_priority,
            )
            if isinstance(summary["platform_stats"], dict):
                summary["platform_stats"][platform] = platform_stats

            final_platform_status = str(platform_stats.get("status") or "failed")
            if final_platform_status in PLATFORM_SUCCESS_STATUSES:
                success_platforms.append(platform)
            else:
                failed_platforms.append(platform)
                if isinstance(summary["errors"], list):
                    summary["errors"].append(
                        _build_monitor_error(
                            "crawl_platform",
                            RuntimeError(platform_stats["error"] or f"{platform}:crawl_failed"),
                            platform=platform,
                            failed_keywords=",".join(platform_stats["failed_keywords"]),
                            zero_result_evidence=" | ".join(platform_stats["zero_result_evidence"]),
                        )
                    )

            if final_platform_status == "partial" and isinstance(summary["errors"], list):
                summary["errors"].append(
                    _build_monitor_error(
                        "crawl_platform_partial",
                        RuntimeError(platform_stats["error"] or f"{platform}:partial"),
                        platform=platform,
                        failed_keywords=",".join(platform_stats["failed_keywords"]),
                    )
                )

        if success_platforms:
            store.upsert_keywords(
                keyword_list,
                tracking_mode=keyword_tracking_mode,
                source_type=keyword_source_type,
                source_platform=keyword_source_platform,
                status="active",
                priority=keyword_priority,
                last_crawled_at=observed_at,
                last_snapshot_id=settings.snapshot_id,
            )

        summary = _finalize_scrape_run(
            settings,
            store,
            args,
            summary,
            run_id=run_id,
            keyword_list=keyword_list,
            platforms=platforms,
            success_platforms=success_platforms,
            failed_platforms=failed_platforms,
            sync_warehouse=sync_warehouse,
        )
        run_finished = True
        return summary
    except Exception as exc:
        if not run_finished:
            store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()


def run_analyze(settings: Settings, *, sync_warehouse: bool = True):
    store = ProductStore(settings.product_store_db_ref)
    run_id = store.start_keyword_run(
        "analyze",
        trigger_mode="manual",
        snapshot_id=settings.snapshot_id,
        metadata={"step": "analyze"},
    )
    finished = False
    try:
        df = keyword_core.step_analyze(settings)
        metrics_count = 0
        if df is not None and not getattr(df, "empty", True):
            metrics_count = store.record_keyword_metrics(
                df.to_dict(orient="records"),
                snapshot_id=settings.snapshot_id,
                analyzed_at=datetime.now().isoformat(),
            )

        status = "completed"
        warehouse_sync = None
        if sync_warehouse:
            try:
                warehouse_sync = _sync_warehouse(settings, reason="analyze")
                if str((warehouse_sync or {}).get("status") or "") == "failed":
                    status = "partial"
            except Exception as exc:
                status = "partial"
                warehouse_sync = {"status": "failed", "reason": str(exc)}

        analysis_quality = _summarize_analysis_quality(df)
        if str(analysis_quality.get("state") or "") == "unknown":
            analysis_quality["state"] = "partial"
        store.finish_keyword_run(
            run_id,
            status=status,
            keyword_count=metrics_count,
            metadata={
                "snapshot_id": settings.snapshot_id,
                "empty": metrics_count == 0,
                "warehouse_sync": warehouse_sync,
                "quality_state": analysis_quality["state"],
                "quality_reasons": list(analysis_quality.get("quality_evidence") or []),
                "quality_source_breakdown": {
                    "analysis": {
                        "state": str(analysis_quality.get("state") or "unknown"),
                        "status": str(analysis_quality.get("state") or "unknown"),
                        "reason_codes": list(analysis_quality.get("quality_flags") or []),
                        "primary_reason": (list(analysis_quality.get("quality_flags") or []) or [""])[0],
                        "available": bool(analysis_quality.get("available")),
                        "google_trends_available": bool(analysis_quality.get("google_trends_available")),
                        "amazon_bsr_available": bool(analysis_quality.get("amazon_bsr_available")),
                        "rows": int(analysis_quality.get("rows") or 0),
                    }
                },
                "quality_summary": analysis_quality,
            },
        )
        finished = True
        logger.info("keyword analyze snapshot persisted: %s", metrics_count)
        return df
    except Exception as exc:
        if not finished:
            store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()


def expand_keyword_into_db(
    settings: Settings,
    *,
    seed_keyword: str,
    platforms: list[str],
    tracking_mode: str,
    priority: int,
    source_type: str,
    output_path: Path,
    run_type: str,
) -> dict[str, object]:
    from keywords.online_expander import expand_keywords

    store = ProductStore(settings.product_store_db_ref)
    run_id = store.start_keyword_run(
        run_type,
        trigger_mode="manual",
        seed_keyword=seed_keyword,
        snapshot_id=settings.snapshot_id,
        platforms=platforms,
        metadata={"output_path": str(output_path)},
    )
    try:
        existing_seed = store.get_keyword(seed_keyword) or {}
        existing_metadata = dict(existing_seed.get("metadata") or {})
        parent_depth = _keyword_depth(existing_seed)
        root_seed_keyword = _keyword_root_seed(existing_seed) or _normalize_keyword_value(seed_keyword)
        if parent_depth >= MAX_EXPANSION_DEPTH:
            result = {
                "run_id": run_id,
                "seed_keyword": _normalize_keyword_value(seed_keyword),
                "root_seed_keyword": root_seed_keyword,
                "parent_depth": parent_depth,
                "child_depth": parent_depth,
                "platform_results": {},
                "all": [],
                "rejected_count": 0,
                "recorded_edges": 0,
                "active_keywords": len(_sync_keyword_pool(settings, store)),
                "skipped": "max_depth_reached",
            }
            store.finish_keyword_run(
                run_id,
                status="completed",
                keyword_count=0,
                metadata={
                    "seed_keyword": seed_keyword,
                    "root_seed_keyword": root_seed_keyword,
                    "parent_depth": parent_depth,
                    "skipped": "max_depth_reached",
                },
            )
            return result

        raw_result = expand_keywords(seed_keyword, platforms, output_path)
        filtered_result: dict[str, list[str]] = {}
        platform_results: dict[str, dict[str, object]] = {}
        total_rejected = 0
        recorded_edges = 0
        child_depth = parent_depth + 1

        for platform in platforms:
            filter_result = _apply_expansion_filter(
                seed_keyword,
                raw_result.get(platform, []),
                root_seed_keyword=root_seed_keyword,
            )
            accepted_keywords = list(filter_result.get("accepted", []))
            filtered_result[platform] = accepted_keywords
            platform_results[platform] = {
                "raw_count": len(raw_result.get(platform, [])),
                "accepted_count": len(accepted_keywords),
                "rejected_count": int(filter_result.get("rejected_count", 0)),
                "rejected_counts": filter_result.get("rejected_counts", {}),
                "rejected_examples": filter_result.get("rejected_examples", []),
            }
            total_rejected += int(filter_result.get("rejected_count", 0))

        filtered_result["all"] = sorted(
            {
                child
                for platform in platforms
                for child in filtered_result.get(platform, [])
            }
        )

        if output_path.suffix.lower() == ".json":
            _atomic_write_json(output_path, filtered_result)
        else:
            lines = [
                f"# keyword expansion",
                f"# seed: {seed_keyword}",
                f"# root_seed: {root_seed_keyword}",
                f"# total: {len(filtered_result.get('all', []))}",
                "",
            ]
            lines.extend(filtered_result.get("all", []))
            _atomic_write_lines(output_path, lines)

        expanded_at = datetime.now().isoformat()
        store.upsert_keyword(
            seed_keyword,
            display_keyword=existing_seed.get("display_keyword") or seed_keyword,
            tracking_mode=tracking_mode,
            source_type=existing_seed.get("source_type") or source_type,
            source_platform=existing_seed.get("source_platform") or "system",
            status=existing_seed.get("status") or "active",
            priority=max(1, int(existing_seed.get("priority") or priority)),
            metadata={
                **existing_metadata,
                "root_seed_keyword": root_seed_keyword,
                "expansion_depth": parent_depth,
                "last_expand_run_type": run_type,
                "expanded_child_depth": child_depth,
                "last_expand_platforms": platforms,
                "last_expand_rejected": total_rejected,
            },
            last_expanded_at=expanded_at,
            last_snapshot_id=settings.snapshot_id,
            commit=False,
        )
        store.upsert_keywords(
            filtered_result.get("all", []),
            tracking_mode=tracking_mode,
            source_type="expanded",
            source_platform="mixed",
            status="active",
            priority=max(priority + 10, 20),
            metadata={
                "seed_keyword": _normalize_keyword_value(seed_keyword),
                "root_seed_keyword": root_seed_keyword,
                "snapshot_id": settings.snapshot_id,
                "parent_source_type": source_type,
                "expansion_depth": child_depth,
                "parent_depth": parent_depth,
                "platforms": platforms,
                "query_keyword": _normalize_keyword_value(seed_keyword),
            },
            last_expanded_at=expanded_at,
            last_snapshot_id=settings.snapshot_id,
        )
        for platform in platforms:
            recorded_edges += store.record_keyword_edges(
                seed_keyword,
                filtered_result.get(platform, []),
                source_platform=platform,
                source_type="autocomplete",
                run_id=run_id,
            )
        merged = _sync_keyword_pool(settings, store, fallback_keywords=filtered_result.get("all", []))
        result = {
            "run_id": run_id,
            "seed_keyword": _normalize_keyword_value(seed_keyword),
            "root_seed_keyword": root_seed_keyword,
            "parent_depth": parent_depth,
            "child_depth": child_depth,
            "platform_results": platform_results,
            "rejected_count": total_rejected,
            "recorded_edges": recorded_edges,
            "active_keywords": len(merged),
            **filtered_result,
        }
        store.finish_keyword_run(
            run_id,
            status="completed",
            keyword_count=len(filtered_result.get("all", [])),
            metadata={
                "seed_keyword": seed_keyword,
                "root_seed_keyword": root_seed_keyword,
                "parent_depth": parent_depth,
                "child_depth": child_depth,
                "platform_results": platform_results,
                "recorded_edges": recorded_edges,
                "rejected_count": total_rejected,
                "active_keywords": len(merged),
            },
        )
        return result
    except Exception as exc:
        store.finish_keyword_run(run_id, status="failed", metadata={"error": str(exc)})
        raise
    finally:
        store.close()


def run_expand(settings: Settings, args: argparse.Namespace, *, sync_warehouse: bool = True):
    if not args.expand_keyword:
        raise SystemExit('please provide --expand-keyword "car"')

    output_dir = settings.data_dir / "keywords_expanded"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.expand_keyword.replace(' ', '_')}_expanded.txt"

    tracking_mode = args.tracking_mode or "tracked"
    source_type = "manual" if tracking_mode == "tracked" else "adhoc"
    result = expand_keyword_into_db(
        settings,
        seed_keyword=args.expand_keyword,
        platforms=args.expand_platforms,
        tracking_mode=tracking_mode,
        priority=max(1, min(args.priority, 20)),
        source_type=source_type,
        output_path=output_path,
        run_type="expand",
    )
    if sync_warehouse:
        _sync_warehouse(settings, reason="expand")

    logger.info("keyword expansion complete")
    logger.info("seed keyword: %s", args.expand_keyword)
    logger.info("platforms: %s", ", ".join(args.expand_platforms))
    logger.info("noon accepted: %s", len(result.get("noon", [])))
    logger.info("amazon accepted: %s", len(result.get("amazon", [])))
    logger.info("total accepted: %s", len(result.get("all", [])))
    logger.info("filtered out: %s", result.get("rejected_count", 0))
    logger.info("result file: %s", output_path)
    logger.info("keyword pool file: %s", _keyword_pool_path(settings))
    return result


async def run_full_pipeline(settings: Settings, args: argparse.Namespace):
    if args.resume:
        keywords, resume_info = _load_resume_keywords(settings)
        if keywords:
            logger.info("[resume] load %s keywords from %s", len(keywords), resume_info.get("source"))
        else:
            logger.warning("[resume] no valid keyword resume source found, regenerate keyword pool")
            keywords = await run_keywords(settings, sync_warehouse=False)
    else:
        keywords = await run_keywords(settings, sync_warehouse=False)

    if hasattr(keyword_core, "_save_config_snapshot"):
        keyword_core._save_config_snapshot(settings, keyword_count=len(keywords))
    kw_snap = _keyword_snapshot_path(settings)
    _atomic_write_json(kw_snap, keywords)

    await run_scrape(settings, args, keywords=keywords, sync_warehouse=False)
    df = run_analyze(settings, sync_warehouse=False)
    if df is None:
        _sync_warehouse(settings, reason="full_pipeline")
        return

    await keyword_core.step_refine(settings)
    run_analyze(settings, sync_warehouse=False)
    keyword_core.step_report(settings)
    if hasattr(keyword_core, "_update_current_symlink"):
        keyword_core._update_current_symlink(settings)
    _sync_warehouse(settings, reason="full_pipeline")


def run_monitor(settings: Settings, args: argparse.Namespace) -> dict:
    profile = _load_monitor_profile(settings, args)
    monitor_seed_keyword = _normalize_monitor_seed_keyword(getattr(args, "monitor_seed_keyword", None))
    lock_payload = _acquire_monitor_lock(settings, settings.snapshot_id)
    _update_task_progress(
        "runtime_collecting",
        message="starting keyword monitor",
        metrics={"baseline_registered": 0, "persisted_product_count": 0},
        details={"snapshot_id": settings.snapshot_id},
    )
    summary: dict[str, object] = {
        "started_at": datetime.now().isoformat(),
        "snapshot_id": settings.snapshot_id,
        "data_root": str(settings.data_dir),
        "profile": profile,
        "lock_path": str(_monitor_lock_path(settings)),
        "lock_payload": lock_payload,
        "status": "running",
        "monitor_seed_keyword": monitor_seed_keyword,
        "monitor_config": str(profile.get("monitor_config") or getattr(args, "monitor_config", None) or ""),
        "current_stage": "starting",
        "stage_note": "monitor_initialized",
        "baseline_registered": 0,
        "baseline_keywords_preview": [],
        "baseline_excluded_count": 0,
        "baseline_excluded_keywords": [],
        "expanded_seed_count": 0,
        "expanded_seed_keywords": [],
        "expanded_seed_excluded_count": 0,
        "expanded_seed_excluded_keywords": [],
        "expanded_keyword_count": 0,
        "expanded_filtered_count": 0,
        "expanded_edge_count": 0,
        "crawled_keyword_count": 0,
        "crawled_keywords_preview": [],
        "crawled_keyword_excluded_count": 0,
        "crawled_keyword_excluded_keywords": [],
        "processed_keyword_count": 0,
        "crawl_batch_size": 0,
        "crawl_batch_count": 0,
        "crawl_batches_completed": 0,
        "crawl_status": "skipped",
        "persisted_product_count": 0,
        "persist_counts": {},
        "platform_stats": {},
        "batch_results": [],
        "intermediate_sync_count": 0,
        "analyzed_keyword_count": 0,
        "report_generated": False,
        "errors": [],
        "quality_state": "partial",
        "quality_summary": {
            "state": "partial",
            "crawl_state": "partial",
            "analysis_state": "unknown",
            "platforms": {},
            "signals": {
                "noon_success": None,
                "amazon_success": None,
                "amazon_bsr_available": None,
                "google_trends_available": None,
                "beautifulsoup4_unavailable": False,
            },
            "quality_flags": [],
            "quality_reasons": [],
            "quality_evidence": [],
            "quality_source_breakdown": {
                "crawl": {"state": "partial", "reason_codes": [], "primary_reason": ""},
                "analysis": {
                    "state": "unknown",
                    "status": "unknown",
                    "reason_codes": ["analysis_empty", "google_trends_missing", "amazon_bsr_missing"],
                    "primary_reason": "analysis_empty",
                    "available": False,
                    "google_trends_available": False,
                    "amazon_bsr_available": False,
                    "rows": 0,
                },
            },
            "analysis": {
                "state": "unknown",
                "available": False,
                "rows": 0,
                "google_trends_available": False,
                "amazon_bsr_available": False,
                "quality_flags": ["analysis_empty"],
                "quality_evidence": ["analysis_empty"],
            },
        },
    }

    run_store = ProductStore(settings.product_store_db_ref)
    monitor_run_id = run_store.start_keyword_run(
        "monitor",
        trigger_mode="scheduled",
        snapshot_id=settings.snapshot_id,
        platforms=profile.get("crawl_platforms") or profile.get("expand_platforms") or [],
        metadata={"profile": profile},
    )
    run_store.close()
    summary["monitor_run_id"] = monitor_run_id
    lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="starting", note="monitor_initialized")
    keyword_control_state = get_keyword_control_state(
        str(profile.get("monitor_config") or getattr(args, "monitor_config", None) or ""),
        baseline_file_override=str(profile.get("baseline_file") or ""),
    )
    exclusion_lookup = build_keyword_control_lookup(keyword_control_state)
    summary["keyword_control"] = {
        "monitor_config": keyword_control_state.get("monitor_config") or summary.get("monitor_config") or "",
        "baseline_file": keyword_control_state.get("baseline_file") or "",
        "exclusion_count": len(keyword_control_state.get("disabled_keywords") or [])
        + len(keyword_control_state.get("blocked_roots") or []),
    }

    try:
        baseline_file = profile.get("baseline_file")
        if baseline_file:
            try:
                baseline_keywords = get_effective_baseline_keywords(
                    str(profile.get("monitor_config") or getattr(args, "monitor_config", None) or ""),
                    baseline_file_override=str(baseline_file or ""),
                )
                baseline_keywords = _filter_monitor_baseline_keywords(baseline_keywords, monitor_seed_keyword)
                baseline_keywords, blocked_baseline_keywords = _filter_monitor_keywords_by_exclusion(
                    baseline_keywords,
                    exclusion_lookup=exclusion_lookup,
                    source_scope="baseline",
                )
                summary["baseline_excluded_count"] = len(blocked_baseline_keywords)
                summary["baseline_excluded_keywords"] = [
                    item.get("keyword")
                    for item in blocked_baseline_keywords[:20]
                    if str(item.get("keyword") or "").strip()
                ]
                if baseline_keywords:
                    reg_args = argparse.Namespace(
                        keyword=",".join(baseline_keywords),
                        keywords_file=None,
                        tracking_mode="tracked",
                        source_type="baseline",
                        priority=int(profile.get("tracked_priority", 30)),
                        limit=None,
                    )
                    run_register(settings, reg_args, sync_warehouse=False)
                    summary["baseline_registered"] = len(baseline_keywords)
                    summary["baseline_keywords_preview"] = baseline_keywords[:20]
                    _update_task_progress(
                        "runtime_collecting",
                        message="baseline keywords registered",
                        metrics={
                            "baseline_registered": int(summary["baseline_registered"]),
                            "persisted_product_count": int(summary["persisted_product_count"]),
                        },
                        details={"baseline_keywords": len(baseline_keywords)},
                    )
                    lock_payload = _checkpoint_monitor_state(
                        settings,
                        summary,
                        lock_payload,
                        stage="baseline_registered",
                        note=f"registered:{len(baseline_keywords)}",
                    )
            except Exception as exc:
                summary["errors"].append(_build_monitor_error("baseline_register", exc, baseline_file=baseline_file))
                lock_payload = _checkpoint_monitor_state(
                    settings,
                    summary,
                    lock_payload,
                    stage="baseline_failed",
                    note="baseline_register_failed",
                )

        store = ProductStore(settings.product_store_db_ref)
        try:
            expand_limit = int(profile.get("expand_limit", 0) or 0)
            if expand_limit > 0:
                scan_limit = max(expand_limit * 5, expand_limit)
                seed_rows = store.get_keywords_for_expand(
                    tracking_mode="tracked",
                    limit=scan_limit,
                    stale_hours=profile.get("expand_stale_hours"),
                    include_source_types=profile.get("expand_source_types"),
                )
                selected_seed_rows = _select_expand_seed_rows(seed_rows, expand_limit)
                selected_seed_rows = _filter_monitor_seed_rows(selected_seed_rows, monitor_seed_keyword)
                selected_seed_rows, blocked_expand_rows = _filter_monitor_keywords_by_exclusion(
                    selected_seed_rows,
                    exclusion_lookup=exclusion_lookup,
                )
                summary["expanded_seed_count"] = len(selected_seed_rows)
                summary["expanded_seed_keywords"] = [
                    row.get("display_keyword") or row.get("keyword")
                    for row in selected_seed_rows
                ]
                summary["expanded_seed_excluded_count"] = len(blocked_expand_rows)
                summary["expanded_seed_excluded_keywords"] = [
                    item.get("keyword")
                    for item in blocked_expand_rows[:20]
                    if str(item.get("keyword") or "").strip()
                ]
                lock_payload = _checkpoint_monitor_state(
                    settings,
                    summary,
                    lock_payload,
                    stage="expand_selected",
                    note=f"selected:{len(selected_seed_rows)}",
                )
            else:
                selected_seed_rows = []
                lock_payload = _checkpoint_monitor_state(
                    settings,
                    summary,
                    lock_payload,
                    stage="expand_skipped",
                    note="expand_limit=0",
                )
        finally:
            store.close()

        output_dir = settings.data_dir / "keywords_expanded"
        output_dir.mkdir(parents=True, exist_ok=True)
        for row in selected_seed_rows:
            seed_keyword = str(row.get("display_keyword") or row.get("keyword") or "").strip()
            if not seed_keyword:
                continue
            lock_payload = _checkpoint_monitor_state(
                settings,
                summary,
                lock_payload,
                stage="expanding",
                note=seed_keyword,
            )
            try:
                result = expand_keyword_into_db(
                    settings,
                    seed_keyword=seed_keyword,
                    platforms=list(profile.get("expand_platforms", ["noon", "amazon"])),
                    tracking_mode="tracked",
                    priority=int(row.get("priority") or profile.get("tracked_priority", 30)),
                    source_type=row.get("source_type") or "manual",
                    output_path=output_dir / f"{seed_keyword.replace(' ', '_')}_expanded.txt",
                    run_type="monitor_expand",
                )
                summary["expanded_keyword_count"] = int(summary["expanded_keyword_count"]) + len(result.get("all", []))
                summary["expanded_filtered_count"] = int(summary["expanded_filtered_count"]) + int(
                    result.get("rejected_count", 0)
                )
                summary["expanded_edge_count"] = int(summary["expanded_edge_count"]) + int(
                    result.get("recorded_edges", 0)
                )
                lock_payload = _checkpoint_monitor_state(
                    settings,
                    summary,
                    lock_payload,
                    stage="expanding",
                    note=seed_keyword,
                )
            except Exception as exc:
                summary["errors"].append(_build_monitor_error("expand_seed", exc, seed_keyword=seed_keyword))
                lock_payload = _checkpoint_monitor_state(
                    settings,
                    summary,
                    lock_payload,
                    stage="expanding",
                    note=f"{seed_keyword}:failed",
                )

        crawl_limit = profile.get("crawl_limit", 200)
        crawl_stale_hours = profile.get("crawl_stale_hours", 24)
        scrape_args = argparse.Namespace(
            keyword=None,
            keywords_file=None,
            platforms=list(profile.get("crawl_platforms", ["noon", "amazon"])),
            noon_count=args.noon_count,
            amazon_count=args.amazon_count,
            persist=True,
            tracking_mode="tracked",
            priority=int(profile.get("tracked_priority", 30)),
            limit=None if crawl_limit is None else int(crawl_limit),
            stale_hours=None if crawl_stale_hours is None else int(crawl_stale_hours),
        )
        if scrape_args.limit is not None and scrape_args.limit <= 0:
            crawled_keywords = []
        else:
            store = ProductStore(settings.product_store_db_ref)
            try:
                crawl_rows = store.get_keywords_for_crawl_rows(
                    tracking_mode="tracked",
                    limit=scrape_args.limit,
                    stale_hours=scrape_args.stale_hours,
                )
                crawl_rows = _filter_monitor_seed_rows(crawl_rows, monitor_seed_keyword)
                crawl_rows, blocked_crawl_rows = _filter_monitor_keywords_by_exclusion(
                    crawl_rows,
                    exclusion_lookup=exclusion_lookup,
                )
                crawled_keywords = [
                    row.get("display_keyword") or row.get("keyword")
                    for row in crawl_rows
                    if (row.get("display_keyword") or row.get("keyword"))
                ]
                summary["crawled_keyword_excluded_count"] = len(blocked_crawl_rows)
                summary["crawled_keyword_excluded_keywords"] = [
                    item.get("keyword")
                    for item in blocked_crawl_rows[:20]
                    if str(item.get("keyword") or "").strip()
                ]
            finally:
                store.close()
        summary["crawled_keyword_count"] = len(crawled_keywords)
        summary["crawled_keywords_preview"] = crawled_keywords[:20]
        crawl_batch_size = int(profile.get("crawl_batch_size") or DEFAULT_MONITOR_CRAWL_BATCH_SIZE or 0)
        crawl_batches = _chunk_keywords(crawled_keywords, crawl_batch_size)
        summary["crawl_batch_size"] = len(crawl_batches[0]) if crawl_batches else 0
        summary["crawl_batch_count"] = len(crawl_batches)
        lock_payload = _checkpoint_monitor_state(
            settings,
            summary,
            lock_payload,
            stage="crawl_selected",
            note=f"selected:{len(crawled_keywords)} batches:{len(crawl_batches)}",
        )

        last_incremental_sync_at: float | None = None
        incremental_sync_interval = int(
            profile.get("crawl_sync_interval_seconds") or DEFAULT_MONITOR_INCREMENTAL_SYNC_SECONDS or 0
        )
        batch_scrape_success = False
        if crawl_batches:
            for batch_index, batch_keywords in enumerate(crawl_batches, 1):
                _update_task_progress(
                    "runtime_collecting",
                    message=f"keyword crawl batch {batch_index}/{len(crawl_batches)}",
                    metrics={
                        "baseline_registered": int(summary["baseline_registered"]),
                        "keyword_count": int(summary["crawled_keyword_count"]),
                        "processed_keyword_count": int(summary["processed_keyword_count"]),
                        "persisted_product_count": int(summary["persisted_product_count"]),
                    },
                    details={
                        "batch_index": batch_index,
                        "batch_count": len(crawl_batches),
                        "batch_keyword_count": len(batch_keywords),
                        "platforms": scrape_args.platforms,
                    },
                )
                lock_payload = _checkpoint_monitor_state(
                    settings,
                    summary,
                    lock_payload,
                    stage="crawling",
                    note=f"batch:{batch_index}/{len(crawl_batches)} keywords:{len(batch_keywords)}",
                )
                try:
                    batch_summary = asyncio.run(
                        run_scrape(settings, scrape_args, keywords=batch_keywords, sync_warehouse=False)
                    )
                    _merge_monitor_batch_summary(
                        summary,
                        batch_summary,
                        batch_index=batch_index,
                        batch_count=len(crawl_batches),
                        batch_keywords=batch_keywords,
                    )
                    summary["crawl_status"] = _derive_monitor_crawl_status(
                        list(summary.get("batch_results") or [])
                    )
                    batch_status = str(batch_summary.get("status") or "unknown")
                    batch_persisted_count = int(batch_summary.get("persisted_product_count") or 0)
                    if batch_status in {"completed", "partial"}:
                        batch_scrape_success = True
                    if batch_persisted_count > 0:
                        _update_task_progress(
                            "stage_persisted",
                            message=f"keyword batch persisted {batch_index}/{len(crawl_batches)}",
                            metrics={
                                "baseline_registered": int(summary["baseline_registered"]),
                                "keyword_count": int(summary["crawled_keyword_count"]),
                                "processed_keyword_count": int(summary["processed_keyword_count"]),
                                "persisted_product_count": int(summary["persisted_product_count"]),
                            },
                            details={
                                "batch_index": batch_index,
                                "batch_count": len(crawl_batches),
                                "batch_status": batch_status,
                                "batch_persisted_count": batch_persisted_count,
                            },
                        )
                    lock_payload = _checkpoint_monitor_state(
                        settings,
                        summary,
                        lock_payload,
                        stage="crawl_completed",
                        note=f"batch:{batch_index}/{len(crawl_batches)}:{batch_status}",
                    )

                    if _should_run_monitor_incremental_sync(
                        batch_index=batch_index,
                        batch_count=len(crawl_batches),
                        batch_persisted_count=batch_persisted_count,
                        last_sync_at=last_incremental_sync_at,
                        interval_seconds=incremental_sync_interval,
                    ):
                        _update_task_progress(
                            "warehouse_syncing",
                            message=f"syncing keyword warehouse after batch {batch_index}",
                            metrics={
                                "baseline_registered": int(summary["baseline_registered"]),
                                "keyword_count": int(summary["crawled_keyword_count"]),
                                "processed_keyword_count": int(summary["processed_keyword_count"]),
                                "persisted_product_count": int(summary["persisted_product_count"]),
                            },
                            details={"batch_index": batch_index, "batch_count": len(crawl_batches)},
                        )
                        lock_payload = _checkpoint_monitor_state(
                            settings,
                            summary,
                            lock_payload,
                            stage="warehouse_sync",
                            note=f"batch:{batch_index}/{len(crawl_batches)}",
                        )
                        sync_result = _sync_warehouse(
                            settings,
                            reason=f"monitor_batch_{batch_index}",
                            wait_for_lock=False,
                        )
                        summary["intermediate_sync_count"] = int(summary.get("intermediate_sync_count") or 0) + 1
                        summary["last_intermediate_sync"] = sync_result
                        sync_status = str((sync_result or {}).get("status") or "")
                        if sync_status == "completed":
                            last_incremental_sync_at = time.time()
                            _update_task_progress(
                                "partial_visible",
                                message=f"keyword batch visible after sync {batch_index}",
                                metrics={
                                    "baseline_registered": int(summary["baseline_registered"]),
                                    "keyword_count": int(summary["crawled_keyword_count"]),
                                    "processed_keyword_count": int(summary["processed_keyword_count"]),
                                    "persisted_product_count": int(summary["persisted_product_count"]),
                                },
                                details={
                                    "batch_index": batch_index,
                                    "batch_count": len(crawl_batches),
                                    "intermediate_sync_count": int(summary["intermediate_sync_count"]),
                                },
                            )
                            lock_payload = _checkpoint_monitor_state(
                                settings,
                                summary,
                                lock_payload,
                                stage="partial_visible",
                                note=f"batch:{batch_index}/{len(crawl_batches)}",
                            )
                except Exception as exc:
                    summary["errors"].append(_build_monitor_error("scrape", exc, batch_index=batch_index))
                    lock_payload = _checkpoint_monitor_state(
                        settings,
                        summary,
                        lock_payload,
                        stage="crawl_failed",
                        note=f"batch:{batch_index}/{len(crawl_batches)}",
                    )

        if crawl_batches and batch_scrape_success:
            try:
                lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="analyzing", note="keyword_metrics")
                df = run_analyze(settings, sync_warehouse=False)
                summary["analyzed_keyword_count"] = 0 if df is None else len(df)
                summary["analysis_quality_summary"] = _summarize_analysis_quality(df)
                lock_payload = _checkpoint_monitor_state(
                    settings,
                    summary,
                    lock_payload,
                    stage="analyze_completed",
                    note=f"rows:{summary['analyzed_keyword_count']}",
                )
            except Exception as exc:
                summary["errors"].append(_build_monitor_error("analyze", exc))
                lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="analyze_failed", note="analyze_exception")

        if profile.get("monitor_report") and crawl_batches:
            try:
                lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="reporting", note="excel_report")
                keyword_core.step_report(settings)
                summary["report_generated"] = True
                lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="report_completed", note="excel_report")
            except Exception as exc:
                summary["errors"].append(_build_monitor_error("report", exc))
                lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="report_failed", note="report_exception")

        try:
            _update_task_progress(
                "warehouse_syncing",
                message="syncing keyword monitor warehouse",
                metrics={
                    "baseline_registered": int(summary["baseline_registered"]),
                    "crawled_keyword_count": int(summary["crawled_keyword_count"]),
                    "persisted_product_count": int(summary["persisted_product_count"]),
                },
                details={"stage": "monitor"},
            )
            lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="warehouse_sync", note="monitor")
            summary["warehouse_sync"] = _sync_warehouse(settings, reason="monitor")
            if str((summary["warehouse_sync"] or {}).get("status") or "") == "failed":
                error_text = str(
                    (summary["warehouse_sync"] or {}).get("error")
                    or (summary["warehouse_sync"] or {}).get("reason")
                    or "warehouse_sync_failed"
                )
                summary["errors"].append(_build_monitor_error("warehouse_sync", RuntimeError(error_text), step="monitor"))
                lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="warehouse_sync_failed", note="monitor")
        except Exception as exc:
            summary["warehouse_sync"] = {"status": "failed", "reason": str(exc)}
            summary["errors"].append(_build_monitor_error("warehouse_sync", exc, step="monitor"))
            lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="warehouse_sync_failed", note="monitor")

        had_effective_output = any(
            [
                int(summary["baseline_registered"]) > 0,
                int(summary["expanded_keyword_count"]) > 0,
                int(summary["processed_keyword_count"]) > 0,
                int(summary["persisted_product_count"]) > 0,
                int(summary["analyzed_keyword_count"]) > 0,
            ]
        )
        summary["status"] = "completed"
        if summary["errors"]:
            summary["status"] = "partial" if had_effective_output else "failed"
        summary["crawl_status"] = _derive_monitor_crawl_status(list(summary.get("batch_results") or []))
        summary["quality_summary"] = _summarize_keyword_quality(
            summary,
            analysis_frame=locals().get("df"),
        )
        summary["quality_state"] = str(summary["quality_summary"].get("state") or "partial")
        final_stage = "runtime_collecting"
        if int(summary["persisted_product_count"]) > 0:
            final_stage = "stage_persisted"
        if str((summary.get("warehouse_sync") or {}).get("status") or "") == "completed":
            final_stage = "web_visible"
        lock_payload = _checkpoint_monitor_state(
            settings,
            summary,
            lock_payload,
            stage="completed" if summary["status"] == "completed" else "completed_with_errors",
            note=str(summary["status"]),
        )
        _update_task_progress(
            final_stage,
            message=f"keyword monitor {summary['status']}",
            metrics={
                "baseline_registered": int(summary["baseline_registered"]),
                "crawled_keyword_count": int(summary["crawled_keyword_count"]),
                "persisted_product_count": int(summary["persisted_product_count"]),
                "analyzed_keyword_count": int(summary["analyzed_keyword_count"]),
            },
            details={
                "warehouse_sync_status": str((summary.get("warehouse_sync") or {}).get("status") or ""),
                "quality_state": str(summary.get("quality_state") or "partial"),
            },
            completed=True,
        )
        return summary
    except Exception as exc:
        summary["status"] = "failed"
        summary["errors"].append(_build_monitor_error("monitor", exc))
        lock_payload = _checkpoint_monitor_state(settings, summary, lock_payload, stage="failed", note="monitor_exception")
        _update_task_progress(
            "runtime_collecting",
            message="keyword monitor failed",
            metrics={
                "baseline_registered": int(summary["baseline_registered"]),
                "persisted_product_count": int(summary["persisted_product_count"]),
            },
            details={"error": str(exc)},
            completed=True,
        )
        raise
    finally:
        summary["finished_at"] = datetime.now().isoformat()
        store = ProductStore(settings.product_store_db_ref)
        try:
            store.finish_keyword_run(
                int(summary["monitor_run_id"]),
                status=str(summary["status"]),
                keyword_count=int(summary["crawled_keyword_count"]),
                metadata=summary,
            )
        finally:
            store.close()
        try:
            summary["pre_finalize_warehouse_sync"] = dict(summary.get("warehouse_sync") or {})
            summary["warehouse_finalize_sync"] = _sync_warehouse(settings, reason="monitor_finalize")
            summary["warehouse_sync"] = dict(summary.get("warehouse_finalize_sync") or {})
        except Exception as exc:
            summary["warehouse_finalize_sync"] = {"status": "failed", "reason": str(exc)}
            summary["warehouse_sync"] = dict(summary.get("warehouse_finalize_sync") or {})
        summary_path = _write_monitor_summary(settings, summary)
        _release_monitor_lock(settings)
        logger.info("keyword monitor summary written: %s", summary_path)


def run_cross_analyze(settings: Settings):
    from analysis.cross_platform_analyzer import analyze_cross_platform

    noon_dir = settings.snapshot_dir / "noon"
    amazon_dir = settings.snapshot_dir / "amazon"
    if not noon_dir.exists() or not amazon_dir.exists():
        raise SystemExit("未找到爬取数据，请先执行 --step scrape")

    output_dir = settings.data_dir / "cross_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    results = analyze_cross_platform(noon_dir, amazon_dir, output_dir)

    logger.info("跨平台交叉分析完成")
    logger.info("机会点: %s", len(results["overall_summary"]["opportunities"]))
    logger.info("警告: %s", len(results["overall_summary"]["warnings"]))
    logger.info("建议: %s", results["overall_summary"]["recommendation"])
    logger.info("报告目录: %s", output_dir)


def main():
    parser = build_parser()
    args = parser.parse_args()
    keyword_core.setup_logging(args.verbose)

    settings = prepare_settings(args)
    logger.info("Runtime Scope: %s", settings.runtime_scope_name)
    logger.info("Data Root: %s", settings.data_dir)
    logger.info("Product DB: %s", settings.product_store_db_ref)
    logger.info("Snapshot ID: %s", settings.snapshot_id)
    logger.info("Snapshot Dir: %s", settings.snapshot_dir)

    if args.step is None:
        asyncio.run(run_full_pipeline(settings, args))
        if args.export_excel:
            keyword_core._export_crawl_data_to_excel(settings)
        return

    if args.step == "keywords":
        asyncio.run(run_keywords(settings))
        return

    if args.step == "register":
        run_register(settings, args)
        return

    if args.step == "list-keywords":
        run_list_keywords(settings, args)
        return

    if args.step == "scrape":
        asyncio.run(run_scrape(settings, args))
        if args.export_excel:
            keyword_core._export_crawl_data_to_excel(settings)
        return

    if args.step == "analyze":
        run_analyze(settings)
        return

    if args.step == "refine":
        asyncio.run(keyword_core.step_refine(settings))
        return

    if args.step == "report":
        keyword_core.step_report(settings)
        return

    if args.step == "track":
        run_track(settings)
        return

    if args.step == "monitor":
        run_monitor(settings, args)
        return

    if args.step == "estimate":
        print("阶段 1.5 功能：销量估算引擎，待实现")
        return

    if args.step == "expand":
        run_expand(settings, args)
        return

    if args.step == "cross-analyze":
        run_cross_analyze(settings)
        return


if __name__ == "__main__":
    main()
