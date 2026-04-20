from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from typing import Any, Callable

from config.product_store import ProductStore
from config.settings import Settings


def build_default_analysis_quality_summary() -> dict[str, object]:
    return {
        "state": "unknown",
        "available": False,
        "rows": 0,
        "google_trends_available": False,
        "amazon_bsr_available": False,
        "quality_flags": ["analysis_empty"],
        "quality_evidence": ["analysis_empty"],
    }


def build_default_scrape_quality_summary() -> dict[str, object]:
    analysis_summary = build_default_analysis_quality_summary()
    return {
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
        "analysis": analysis_summary,
    }


def build_scrape_summary(
    settings: Settings,
    *,
    keyword_count: int,
    requested_keyword_count: int,
    platforms: list[str],
    resume_info: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "snapshot_id": settings.snapshot_id,
        "status": "running",
        "keyword_count": keyword_count,
        "requested_keyword_count": requested_keyword_count,
        "platforms": platforms,
        "platform_stats": {},
        "persist_counts": {},
        "persisted_product_count": 0,
        "quality_state": "partial",
        "quality_summary": build_default_scrape_quality_summary(),
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


def build_skipped_scrape_summary(
    settings: Settings,
    *,
    requested_keyword_count: int,
    platforms: list[str],
    resume_info: dict[str, object] | None,
) -> dict[str, object]:
    summary = build_scrape_summary(
        settings,
        keyword_count=0,
        requested_keyword_count=requested_keyword_count,
        platforms=platforms,
        resume_info=resume_info,
    )
    summary["status"] = "skipped"
    return summary


async def crawl_platform_with_retry(
    settings: Settings,
    args: argparse.Namespace,
    keyword_list: list[str],
    platform: str,
    *,
    step_scrape: Callable[..., Any],
    summarize_platform_snapshot: Callable[[Settings, str, list[str]], dict[str, object]],
    clear_platform_result_files: Callable[[Settings, str, list[str]], list[str]],
    amazon_retry_count: int,
    amazon_retry_delay_seconds: int,
    logger: Any,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    max_attempts = 1 + (amazon_retry_count if platform == "amazon" else 0)
    attempt_history: list[dict[str, object]] = []
    final_platform_summary: dict[str, object] | None = None

    for attempt in range(1, max_attempts + 1):
        platform_exception: Exception | None = None
        try:
            await step_scrape(
                settings,
                keyword_list,
                platforms=[platform],
                noon_count=args.noon_count,
                amazon_count=args.amazon_count,
            )
        except Exception as exc:
            platform_exception = exc
            logger.exception("crawl platform failed: %s attempt=%s", platform, attempt)

        platform_summary = summarize_platform_snapshot(settings, platform, keyword_list)
        if platform_exception is not None:
            current_error = str(platform_exception)
            if platform_summary.get("error"):
                current_error = f"{platform_summary['error']}; {current_error}"
            platform_summary["error"] = current_error
            if str(platform_summary.get("status")) in {"completed", "zero_results"}:
                platform_summary["status"] = "partial"
            else:
                platform_summary["status"] = "failed"

        attempt_record = {
            "attempt": attempt,
            "status": platform_summary.get("status"),
            "products_count": platform_summary.get("products_count", 0),
            "result_files": len(platform_summary.get("result_files") or []),
            "error": platform_summary.get("error", ""),
            "failed_keywords": list(platform_summary.get("failed_keywords") or []),
            "zero_result_keywords": list(platform_summary.get("zero_result_keywords") or []),
            "zero_result_evidence": list(platform_summary.get("zero_result_evidence") or []),
        }
        attempt_history.append(attempt_record)
        final_platform_summary = platform_summary

        if str(platform_summary.get("status")) != "failed":
            break
        if attempt >= max_attempts:
            break

        retry_keywords = list(platform_summary.get("failed_keywords") or keyword_list)
        deleted_files = clear_platform_result_files(settings, platform, retry_keywords)
        attempt_record["retry_deleted_files"] = deleted_files
        logger.warning(
            "retrying platform crawl: platform=%s attempt=%s/%s failed_keywords=%s wait=%ss",
            platform,
            attempt,
            max_attempts,
            len(retry_keywords),
            amazon_retry_delay_seconds,
        )
        await asyncio.sleep(amazon_retry_delay_seconds)

    if final_platform_summary is None:
        final_platform_summary = {
            "status": "failed",
            "keyword_count": len(keyword_list),
            "result_files": [],
            "products_count": 0,
            "total_results": 0,
            "error": f"{platform}:missing_platform_summary",
            "zero_result_evidence": [],
            "failed_keywords": list(keyword_list),
            "zero_result_keywords": [],
        }
    return final_platform_summary, attempt_history


def finalize_platform_scrape(
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
    persist_keyword_results: Callable[[Settings, list[str]], dict[str, int]],
    capture_live_suggestion_edges: Callable[..., dict[str, int]],
    summarize_keyword_quality: Callable[[dict[str, object]], dict[str, object]],
    update_task_progress: Callable[..., None],
    safe_int: Callable[[Any, int], int],
) -> dict[str, object]:
    final_platform_status = str(final_platform_summary.get("status") or "failed")
    suggestion_summary = {
        "platform_files": 0,
        "parent_keywords": 0,
        "discovered_keywords": 0,
        "recorded_edges": 0,
        "rejected_keywords": 0,
    }
    persisted_products = 0

    if args.persist and final_platform_status in {"completed", "zero_results", "partial"}:
        persist_counts = persist_keyword_results(settings, [platform])
        persisted_products = int(persist_counts.get(platform, 0))
        suggestion_summary = capture_live_suggestion_edges(
            settings,
            store,
            platforms=[platform],
            run_id=run_id,
            snapshot_id=settings.snapshot_id,
            fallback_tracking_mode=keyword_tracking_mode,
            fallback_priority=keyword_priority,
        )
        if isinstance(summary["live_suggestion_summary"], dict):
            for key, value in suggestion_summary.items():
                summary["live_suggestion_summary"][key] = int(summary["live_suggestion_summary"].get(key, 0)) + int(value)

    if isinstance(summary["persist_counts"], dict):
        summary["persist_counts"][platform] = persisted_products
    summary["persisted_product_count"] = int(summary["persisted_product_count"]) + persisted_products
    if persisted_products > 0:
        update_task_progress(
            "stage_persisted",
            message=f"persisted keyword results: {platform}",
            metrics={
                "keyword_count": len(keyword_list),
                "platform_count": len(summary.get("platforms") or []),
                "persisted_product_count": int(summary["persisted_product_count"]),
            },
            details={
                "platform": platform,
                "platform_status": final_platform_status,
                "persisted_count": persisted_products,
            },
        )

    platform_stats = {
        "status": final_platform_status,
        "attempts": len(attempt_history),
        "retry_count": max(0, len(attempt_history) - 1),
        "keyword_count": len(keyword_list),
        "products_count": safe_int(final_platform_summary.get("products_count"), 0),
        "persisted_count": persisted_products,
        "result_files": list(final_platform_summary.get("result_files") or []),
        "total_results": safe_int(final_platform_summary.get("total_results"), 0),
        "error": str(final_platform_summary.get("error") or ""),
        "error_evidence": list(final_platform_summary.get("error_evidence") or []),
        "zero_result_evidence": list(final_platform_summary.get("zero_result_evidence") or []),
        "failure_details": list(final_platform_summary.get("failure_details") or []),
        "failed_keywords": list(final_platform_summary.get("failed_keywords") or []),
        "zero_result_keywords": list(final_platform_summary.get("zero_result_keywords") or []),
        "live_suggestion_files": suggestion_summary["platform_files"],
        "live_suggestion_parents": suggestion_summary["parent_keywords"],
        "live_suggestion_keywords": suggestion_summary["discovered_keywords"],
        "live_suggestion_edges": suggestion_summary["recorded_edges"],
        "live_suggestion_rejected": suggestion_summary["rejected_keywords"],
        "attempt_history": attempt_history,
    }
    platform_quality = summarize_keyword_quality(
        {
            "platforms": [platform],
            "platform_stats": {platform: platform_stats},
        }
    )
    platform_stats["quality_state"] = str(platform_quality.get("state") or "partial")
    platform_stats["quality_evidence"] = list(platform_quality.get("quality_evidence") or [])
    platform_stats["quality_flags"] = list(platform_quality.get("quality_flags") or [])
    return platform_stats


def finalize_scrape_run(
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
    sync_keyword_pool: Callable[..., list[str]],
    sync_warehouse_fn: Callable[..., dict[str, object]],
    update_task_progress: Callable[..., None],
    build_monitor_error: Callable[..., dict[str, object]],
    summarize_keyword_quality: Callable[[dict[str, object]], dict[str, object]],
) -> dict[str, object]:
    if success_platforms:
        sync_keyword_pool(settings, store, fallback_keywords=keyword_list)

    summary["quality_summary"] = summarize_keyword_quality(summary)
    summary["quality_state"] = str(summary["quality_summary"].get("state") or "partial")
    status = "completed"
    if not success_platforms:
        status = "failed"
    elif failed_platforms or any(
        str((summary["platform_stats"] or {}).get(platform, {}).get("status")) == "partial"
        for platform in platforms
    ):
        status = "partial"

    warehouse_sync = None
    if args.persist and success_platforms and sync_warehouse:
        try:
            update_task_progress(
                "warehouse_syncing",
                message="syncing keyword warehouse",
                metrics={
                    "keyword_count": len(keyword_list),
                    "platform_count": len(platforms),
                    "persisted_product_count": int(summary["persisted_product_count"]),
                },
                details={"success_platforms": success_platforms},
            )
            warehouse_sync = sync_warehouse_fn(settings, reason="crawl")
            if str((warehouse_sync or {}).get("status") or "") == "failed":
                error_text = str((warehouse_sync or {}).get("error") or (warehouse_sync or {}).get("reason") or "warehouse_sync_failed")
                if isinstance(summary["errors"], list):
                    summary["errors"].append(build_monitor_error("warehouse_sync", RuntimeError(error_text), step="crawl"))
                status = "partial" if success_platforms else "failed"
        except Exception as exc:
            if isinstance(summary["errors"], list):
                summary["errors"].append(build_monitor_error("warehouse_sync", exc, step="crawl"))
            status = "partial" if success_platforms else "failed"
            warehouse_sync = {"status": "failed", "reason": str(exc)}

    summary["status"] = status
    summary["success_platforms"] = success_platforms
    summary["failed_platforms"] = failed_platforms
    summary["warehouse_sync"] = warehouse_sync
    summary["finished_at"] = datetime.now().isoformat()
    final_stage = "runtime_collecting"
    if int(summary["persisted_product_count"]) > 0:
        final_stage = "stage_persisted"
    if str((warehouse_sync or {}).get("status") or "") == "completed":
        final_stage = "web_visible"
    elif args.persist and success_platforms and sync_warehouse:
        final_stage = "warehouse_syncing"
    update_task_progress(
        final_stage,
        message=f"keyword crawl {status}",
        metrics={
            "keyword_count": len(keyword_list),
            "platform_count": len(platforms),
            "persisted_product_count": int(summary["persisted_product_count"]),
        },
        details={
            "success_platforms": success_platforms,
            "failed_platforms": failed_platforms,
            "warehouse_sync_status": str((warehouse_sync or {}).get("status") or ""),
        },
        completed=True,
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
