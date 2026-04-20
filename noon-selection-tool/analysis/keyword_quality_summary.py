from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any


TERMINAL_RUN_STATUSES = {"completed", "failed", "cancelled", "skipped", "partial", "zero_results"}
QUALITY_STATES = {"full", "partial", "degraded", "unknown"}
QUALITY_STATE_PRIORITY = {"unknown": 0, "full": 1, "partial": 2, "degraded": 3}
RUN_TYPE_PRIORITY = {
    "analyze": 50,
    "crawl": 40,
    "monitor": 30,
    "keyword_monitor": 30,
    "report": 20,
    "refine": 20,
    "register": 10,
    "monitor_expand": 10,
}


def parse_metadata_json(raw_value: Any) -> dict[str, Any]:
    if raw_value in (None, "", "null"):
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def dedupe_texts(values: list[Any]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        text = str(raw_value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


GENERIC_REASON_CODES = {"runtime_error"}
SPECIFIC_REASON_CODES = {
    "dependency_missing",
    "runtime_import_error",
    "amazon_parse_failure",
    "amazon_upstream_blocked",
    "result_contract_mismatch",
    "timeout",
    "page_recognition_failed",
    "fallback_misfire",
    "zero_results",
    "partial_results",
}


def _prune_reason_codes(reason_codes: list[Any], *, analysis_available: bool | None = None) -> list[str]:
    normalized = dedupe_texts(reason_codes)
    normalized = [item for item in normalized if item != "zero_results"]
    if analysis_available:
        normalized = [item for item in normalized if item != "analysis_empty"]
    if "runtime_error" in normalized and any(item in SPECIFIC_REASON_CODES for item in normalized):
        normalized = [item for item in normalized if item not in GENERIC_REASON_CODES]
    return normalized


def _parse_ts(raw_value: Any):
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def normalize_quality_state(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text in QUALITY_STATES else "unknown"


def _quality_state_priority(value: Any) -> int:
    return QUALITY_STATE_PRIORITY.get(normalize_quality_state(value), 0)


def _run_type_priority(value: Any) -> int:
    return RUN_TYPE_PRIORITY.get(str(value or "").strip().lower(), 0)


@lru_cache(maxsize=1)
def bs4_available_in_current_runtime() -> bool:
    return importlib.util.find_spec("bs4") is not None


def _platform_reason_code(raw_value: Any, *, platform: str) -> str:
    text = str(raw_value or "").strip().lower()
    if not text:
        return ""
    if text.startswith("dependency_missing:"):
        return "dependency_missing"
    if "beautifulsoup4_unavailable" in text:
        return "runtime_import_error" if bs4_available_in_current_runtime() else "dependency_missing"
    if "modulenotfounderror" in text or "importerror" in text:
        return "runtime_import_error"
    if text.startswith("missing_result_file:") or text.startswith("expected_result_file:") or "incomplete_json" in text or "invalid_json" in text:
        return "result_contract_mismatch"
    if text.startswith("selector_miss:"):
        return "amazon_parse_failure" if platform == "amazon" else "page_recognition_failed"
    if "google_trends_missing" in text:
        return "google_trends_missing"
    if "amazon_bsr_missing" in text:
        return "amazon_bsr_missing"
    if text.startswith("external_site_error:") or any(
        token in text
        for token in (
            "captcha",
            "robot",
            "service unavailable",
            "temporarily unavailable",
            "something went wrong",
            "we're sorry",
            "timeout",
            "timed out",
            "net::err",
        )
    ):
        return "amazon_upstream_blocked" if platform == "amazon" else "timeout"
    if text.startswith("page_recognition_failed:"):
        return "amazon_parse_failure" if platform == "amazon" else "page_recognition_failed"
    if "fallback_misfire" in text:
        return "amazon_parse_failure" if platform == "amazon" else "fallback_misfire"
    if text in {"runtime_error", "run_failed", "crawl_status_failed"} or text.endswith(":runtime_error"):
        return "amazon_parse_failure" if platform == "amazon" else "runtime_error"
    if text.endswith(":failed") or text.endswith("_failed") or text.endswith(":failed_keywords"):
        return "amazon_parse_failure" if platform == "amazon" else "runtime_error"
    if "zero_results" in text:
        return "zero_results"
    if "partial" in text:
        return "partial_results"
    if "analysis_empty" in text:
        return "analysis_empty"
    return ""


def _select_best_source_payload(
    batch_items: list[dict[str, Any]],
    *,
    source_name: str,
) -> dict[str, Any]:
    selected_payload: dict[str, Any] = {}
    selected_key: tuple[int, int, int, object] | None = None
    normalized_source = str(source_name or "").strip().lower()
    for item in batch_items:
        payload = dict((item.get("quality_source_breakdown") or {}).get(normalized_source) or {})
        if not payload:
            continue
        payload_state = normalize_quality_state(payload.get("state"))
        available_bonus = 1 if normalized_source == "analysis" and bool(payload.get("available")) else 0
        selection_key = (
            available_bonus,
            _quality_state_priority(payload_state),
            _run_type_priority(item.get("run_type")),
            item.get("finished_at_dt") or item.get("started_at_dt") or "",
        )
        if selected_key is None or selection_key > selected_key:
            selected_key = selection_key
            selected_payload = payload
    return selected_payload


def _normalize_source_payload(
    *,
    source_name: str,
    raw_payload: dict[str, Any] | None,
    platform_stats: dict[str, Any] | None,
) -> dict[str, Any]:
    source = str(source_name or "").strip().lower() or "unknown"
    payload = dict(raw_payload or {})
    stats = dict(platform_stats or {})
    raw_reason_codes = list(payload.get("reason_codes") or [])
    evidence = list(payload.get("evidence") or [])
    evidence.extend(list(stats.get("error_evidence") or []))
    error_text = str(stats.get("error") or "").strip()
    if error_text:
        evidence.append(error_text)
    status = str(payload.get("status") or stats.get("status") or "").strip().lower()

    normalized_reason_codes = dedupe_texts(
        [_platform_reason_code(item, platform=source) for item in raw_reason_codes + evidence]
    )
    analysis_available = None
    if source == "analysis":
        analysis_available = bool(payload.get("available"))
    normalized_reason_codes = _prune_reason_codes(normalized_reason_codes, analysis_available=analysis_available)
    primary_reason = normalized_reason_codes[0] if normalized_reason_codes else ""
    state = normalize_quality_state(payload.get("state") or "")
    if state == "unknown":
        if normalized_reason_codes:
            if all(code == "zero_results" for code in normalized_reason_codes):
                state = "full"
            else:
                state = (
                    "degraded"
                    if any(
                        code in {
                            "dependency_missing",
                            "runtime_import_error",
                            "amazon_parse_failure",
                            "amazon_upstream_blocked",
                            "result_contract_mismatch",
                            "runtime_error",
                            "external_site_error",
                            "timeout",
                            "page_recognition_failed",
                            "fallback_misfire",
                        }
                        for code in normalized_reason_codes
                    )
                    else "partial"
                )
        elif status == "partial":
            state = "partial"
        elif status == "zero_results":
            state = "full"
        elif status == "completed":
            state = "full"
    normalized = dict(payload)
    normalized.update(
        {
            "state": state,
            "status": status or str(payload.get("status") or ""),
            "reason_codes": normalized_reason_codes,
            "primary_reason": primary_reason,
            "evidence": dedupe_texts(evidence),
            "failure_details": list(payload.get("failure_details") or stats.get("failure_details") or []),
        }
    )
    return normalized


def _build_quality_source_breakdown(
    *,
    crawl_status: str,
    platform_stats: dict[str, Any],
    explicit_breakdown: dict[str, Any] | None,
    quality_evidence: list[str],
) -> dict[str, Any]:
    breakdown: dict[str, Any] = {}
    explicit = explicit_breakdown if isinstance(explicit_breakdown, dict) else {}
    platform_names = sorted(
        {
            *[str(key or "").strip().lower() for key in platform_stats.keys()],
            *[str(key or "").strip().lower() for key in explicit.keys() if str(key or "").strip().lower() not in {"crawl", "analysis"}],
        }
    )
    crawl_reason_codes: list[str] = []
    for platform_name in platform_names:
        if not platform_name:
            continue
        normalized = _normalize_source_payload(
            source_name=platform_name,
            raw_payload=explicit.get(platform_name) if isinstance(explicit.get(platform_name), dict) else None,
            platform_stats=platform_stats.get(platform_name) if isinstance(platform_stats.get(platform_name), dict) else None,
        )
        breakdown[platform_name] = normalized
        crawl_reason_codes.extend(list(normalized.get("reason_codes") or []))

    analysis_payload = explicit.get("analysis") if isinstance(explicit.get("analysis"), dict) else None
    if analysis_payload:
        normalized_analysis = _normalize_source_payload(
            source_name="analysis",
            raw_payload=analysis_payload,
            platform_stats=None,
        )
        breakdown["analysis"] = normalized_analysis

    crawl_state = normalize_quality_state((explicit.get("crawl") or {}).get("state") if isinstance(explicit.get("crawl"), dict) else "")
    if crawl_state == "unknown":
        normalized_crawl_status = str(crawl_status or "").strip().lower()
        if normalized_crawl_status == "failed" or any(
            code in {
                "dependency_missing",
                "runtime_import_error",
                "amazon_parse_failure",
                "amazon_upstream_blocked",
                "result_contract_mismatch",
                "runtime_error",
                "external_site_error",
                "timeout",
                "page_recognition_failed",
                "fallback_misfire",
            }
            for code in crawl_reason_codes
        ):
            crawl_state = "degraded"
        elif normalized_crawl_status == "partial" or any(code == "partial_results" for code in crawl_reason_codes):
            crawl_state = "partial"
        elif platform_names:
            crawl_state = "full"
    breakdown["crawl"] = {
        "state": crawl_state,
        "status": str(crawl_status or "").strip().lower(),
        "reason_codes": dedupe_texts(crawl_reason_codes),
        "primary_reason": dedupe_texts(crawl_reason_codes)[0] if crawl_reason_codes else "",
        "evidence": dedupe_texts(quality_evidence),
    }
    return breakdown


def normalize_recent_keyword_run(row: dict[str, Any]) -> dict[str, Any]:
    metadata = parse_metadata_json(row.get("metadata_json"))
    quality_summary = metadata.get("quality_summary") if isinstance(metadata.get("quality_summary"), dict) else {}
    explicit_quality_state = normalize_quality_state(metadata.get("quality_state") or quality_summary.get("state"))
    crawl_status = str(metadata.get("crawl_status") or row.get("status") or "").strip().lower()
    platform_stats = metadata.get("platform_stats") if isinstance(metadata.get("platform_stats"), dict) else {}
    errors = list(metadata.get("errors") or [])
    explicit_quality_source_breakdown = metadata.get("quality_source_breakdown")
    if not isinstance(explicit_quality_source_breakdown, dict):
        explicit_quality_source_breakdown = quality_summary.get("quality_source_breakdown") if isinstance(quality_summary.get("quality_source_breakdown"), dict) else {}

    quality_evidence = dedupe_texts(
        list(metadata.get("quality_reasons") or [])
        + list(quality_summary.get("quality_reasons") or [])
        + list(quality_summary.get("quality_evidence") or [])
        + list(quality_summary.get("quality_flags") or [])
        + errors
    )
    quality_reasons = dedupe_texts(
        [
            _platform_reason_code(item, platform="amazon" if "amazon" in str(item or "").lower() else "")
            for item in quality_evidence
        ]
    )
    if explicit_quality_state == "unknown":
        run_status = str(row.get("status") or "").strip().lower()
        if run_status == "failed" or crawl_status == "failed":
            explicit_quality_state = "degraded"
        elif quality_reasons:
            explicit_quality_state = (
                "degraded"
                if any(
                    code in {
                        "dependency_missing",
                        "runtime_import_error",
                        "amazon_parse_failure",
                        "amazon_upstream_blocked",
                        "result_contract_mismatch",
                        "runtime_error",
                        "external_site_error",
                        "timeout",
                        "page_recognition_failed",
                        "fallback_misfire",
                    }
                    for code in quality_reasons
                )
                else "partial"
            )
        elif run_status == "completed":
            explicit_quality_state = "full"
    quality_source_breakdown = _build_quality_source_breakdown(
        crawl_status=crawl_status,
        platform_stats=platform_stats,
        explicit_breakdown=explicit_quality_source_breakdown,
        quality_evidence=quality_evidence,
    )
    analysis_available = bool(((quality_source_breakdown.get("analysis") or {}).get("available")))
    quality_reasons = _prune_reason_codes(quality_reasons, analysis_available=analysis_available)
    if not quality_reasons:
        for payload in quality_source_breakdown.values():
            if isinstance(payload, dict):
                quality_reasons.extend(list(payload.get("reason_codes") or []))
        quality_reasons = _prune_reason_codes(quality_reasons, analysis_available=analysis_available)
    return {
        "run_type": str(row.get("run_type") or ""),
        "trigger_mode": str(row.get("trigger_mode") or ""),
        "seed_keyword": str(row.get("seed_keyword") or ""),
        "snapshot_id": str(row.get("snapshot_id") or ""),
        "status": str(row.get("status") or ""),
        "crawl_status": crawl_status,
        "keyword_count": int(row.get("keyword_count") or 0),
        "started_at": str(row.get("started_at") or ""),
        "finished_at": str(row.get("finished_at") or ""),
        "quality_state": explicit_quality_state,
        "quality_reasons": quality_reasons,
        "quality_evidence": quality_evidence,
        "quality_source_breakdown": quality_source_breakdown,
        "is_terminal": str(row.get("status") or "").strip().lower() in TERMINAL_RUN_STATUSES,
        "started_at_dt": _parse_ts(row.get("started_at")),
        "finished_at_dt": _parse_ts(row.get("finished_at")),
    }


def _summarize_batch(items: list[dict[str, Any]]) -> dict[str, Any]:
    batch_items = sorted(
        items,
        key=lambda item: (
            _quality_state_priority(item.get("quality_state")),
            _run_type_priority(item.get("run_type")),
            item.get("finished_at_dt") or item.get("started_at_dt") or "",
        ),
        reverse=True,
    )
    batch_state = normalize_quality_state(
        max(
            (item.get("quality_state") for item in batch_items),
            key=_quality_state_priority,
            default="unknown",
        )
    )
    dominant_items = [
        item
        for item in batch_items
        if _quality_state_priority(item.get("quality_state")) == _quality_state_priority(batch_state)
    ]
    highest_run_priority = max((_run_type_priority(item.get("run_type")) for item in dominant_items), default=0)
    selected_items = [
        item for item in dominant_items if _run_type_priority(item.get("run_type")) == highest_run_priority
    ] or dominant_items
    live_items = [item for item in batch_items if not item.get("is_terminal")]
    latest_activity_item = max(
        batch_items,
        key=lambda item: item.get("finished_at_dt") or item.get("started_at_dt") or "",
        default=None,
    )
    latest_terminal_item = max(
        (item for item in batch_items if item.get("is_terminal")),
        key=lambda item: item.get("finished_at_dt") or item.get("started_at_dt") or "",
        default=None,
    )
    reasons = dedupe_texts(
        [
            *[reason for item in selected_items for reason in list(item.get("quality_reasons") or [])],
            *[reason for item in dominant_items for reason in list(item.get("quality_reasons") or [])],
        ]
    )
    evidence = dedupe_texts(
        [
            *[token for item in selected_items for token in list(item.get("quality_evidence") or [])],
            *[token for item in dominant_items for token in list(item.get("quality_evidence") or [])],
            *reasons,
        ]
    )
    source_names = dedupe_texts(
        [
            source_name
            for item in batch_items
            for source_name in dict(item.get("quality_source_breakdown") or {}).keys()
        ]
    )
    source_breakdown: dict[str, Any] = {}
    for source_name in source_names:
        payload = _select_best_source_payload(batch_items, source_name=source_name)
        if payload:
            source_breakdown[source_name] = payload
    analysis_available = any(
        bool((((item.get("quality_source_breakdown") or {}).get("analysis") or {}).get("available")))
        for item in batch_items
    )
    reasons = _prune_reason_codes(reasons, analysis_available=analysis_available)
    evidence = dedupe_texts(
        [
            token
            for token in evidence
            if not (analysis_available and str(token or "").strip().lower() == "analysis_empty")
        ]
    )
    if batch_state == "full":
        reasons = []
        evidence = []
        for payload in source_breakdown.values():
            if not isinstance(payload, dict):
                continue
            if normalize_quality_state(payload.get("state")) != "full":
                continue
            payload["reason_codes"] = []
            payload["primary_reason"] = ""
            if "evidence" in payload:
                payload["evidence"] = []
    return {
        "snapshot_id": str(batch_items[0].get("snapshot_id") or ""),
        "quality_state": batch_state,
        "quality_reasons": reasons,
        "quality_evidence": evidence,
        "quality_source_breakdown": source_breakdown,
        "has_live": bool(live_items),
        "live_run_status": str(live_items[0].get("status") or "") if live_items else "",
        "live_crawl_status": str(live_items[0].get("crawl_status") or "") if live_items else "",
        "live_seed_keyword": str(live_items[0].get("seed_keyword") or "") if live_items else "",
        "latest_activity_started_at": str((latest_activity_item or {}).get("started_at") or ""),
        "latest_activity_finished_at": str((latest_activity_item or {}).get("finished_at") or ""),
        "latest_terminal_run_status": str((latest_terminal_item or {}).get("status") or ""),
        "latest_terminal_crawl_status": str((latest_terminal_item or {}).get("crawl_status") or ""),
        "latest_terminal_seed_keyword": str((latest_terminal_item or {}).get("seed_keyword") or ""),
        "latest_terminal_started_at": str((latest_terminal_item or {}).get("started_at") or ""),
        "latest_terminal_finished_at": str((latest_terminal_item or {}).get("finished_at") or ""),
        "run_types": dedupe_texts([item.get("run_type") for item in batch_items]),
        "items": batch_items,
        "sort_key": (
            (latest_activity_item or {}).get("finished_at_dt")
            or (latest_activity_item or {}).get("started_at_dt")
            or ""
        ),
    }


def summarize_recent_keyword_runs(recent_runs: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_items = [normalize_recent_keyword_run(row) for row in recent_runs]
    batches_by_snapshot: dict[str, list[dict[str, Any]]] = {}
    recent_items: list[dict[str, Any]] = []
    for index, item in enumerate(normalized_items):
        recent_items.append(item)
        snapshot_id = str(item.get("snapshot_id") or f"__run_{index:04d}")
        batches_by_snapshot.setdefault(snapshot_id, []).append(item)

    recent_batches = sorted(
        [_summarize_batch(items) for items in batches_by_snapshot.values()],
        key=lambda batch: batch.get("sort_key") or "",
        reverse=True,
    )
    live_batches = [batch for batch in recent_batches if batch.get("has_live")]
    breakdown = {"full": 0, "partial": 0, "degraded": 0, "unknown": 0}
    for batch in recent_batches:
        state = normalize_quality_state(batch.get("quality_state"))
        breakdown[state] = breakdown.get(state, 0) + 1

    live_batch = live_batches[0] if live_batches else None
    latest_terminal_batch = next((batch for batch in recent_batches if not batch.get("has_live")), None)
    active_snapshot_count = len(live_batches)
    stale_running_snapshot_count = max(0, active_snapshot_count - 1)

    operator_batch = None
    if live_batch and normalize_quality_state(live_batch.get("quality_state")) in {"degraded", "partial"}:
        operator_batch = live_batch
    elif latest_terminal_batch is not None:
        operator_batch = latest_terminal_batch
    else:
        operator_batch = live_batch

    operator_state = normalize_quality_state((operator_batch or {}).get("quality_state"))
    if operator_state == "unknown":
        if breakdown.get("degraded"):
            operator_state = "degraded"
        elif breakdown.get("partial"):
            operator_state = "partial"
        elif breakdown.get("full"):
            operator_state = "full"

    live_batch_state = normalize_quality_state((live_batch or {}).get("quality_state"))
    if live_batch is None:
        live_batch_state = "idle"
    latest_terminal_state = normalize_quality_state((latest_terminal_batch or {}).get("quality_state"))

    operator_status_summary = (
        "keyword runtime quality degraded"
        if operator_state == "degraded"
        else "keyword runtime quality is partial"
        if operator_state == "partial"
        else "keyword runtime quality looks stable"
        if operator_state == "full"
        else "keyword runtime quality is unknown"
    )

    return {
        "truth_source": "keyword_snapshot_batch_aggregate",
        "active_snapshot_count": active_snapshot_count,
        "stale_running_snapshot_count": stale_running_snapshot_count,
        "live_batch_state": live_batch_state,
        "live_batch_snapshot_id": str((live_batch or {}).get("snapshot_id") or ""),
        "live_run_status": str((live_batch or {}).get("live_run_status") or ""),
        "live_crawl_status": str((live_batch or {}).get("live_crawl_status") or ""),
        "live_seed_keyword": str((live_batch or {}).get("live_seed_keyword") or ""),
        "live_started_at": str((live_batch or {}).get("latest_activity_started_at") or ""),
        "live_finished_at": str((live_batch or {}).get("latest_activity_finished_at") or ""),
        "live_quality_reasons": list((live_batch or {}).get("quality_reasons") or []),
        "live_quality_evidence": list((live_batch or {}).get("quality_evidence") or []),
        "live_quality_source_breakdown": dict((live_batch or {}).get("quality_source_breakdown") or {}),
        "latest_terminal_batch_state": latest_terminal_state,
        "latest_terminal_batch_snapshot_id": str((latest_terminal_batch or {}).get("snapshot_id") or ""),
        "latest_terminal_quality_state": latest_terminal_state,
        "latest_terminal_run_status": str((latest_terminal_batch or {}).get("latest_terminal_run_status") or ""),
        "latest_terminal_crawl_status": str((latest_terminal_batch or {}).get("latest_terminal_crawl_status") or ""),
        "latest_terminal_seed_keyword": str((latest_terminal_batch or {}).get("latest_terminal_seed_keyword") or ""),
        "latest_terminal_started_at": str((latest_terminal_batch or {}).get("latest_terminal_started_at") or ""),
        "latest_terminal_finished_at": str((latest_terminal_batch or {}).get("latest_terminal_finished_at") or ""),
        "latest_terminal_quality_reasons": list((latest_terminal_batch or {}).get("quality_reasons") or []),
        "latest_terminal_quality_evidence": list((latest_terminal_batch or {}).get("quality_evidence") or []),
        "latest_terminal_quality_source_breakdown": dict((latest_terminal_batch or {}).get("quality_source_breakdown") or {}),
        "operator_quality_state": operator_state,
        "operator_quality_reasons": list((operator_batch or {}).get("quality_reasons") or []),
        "operator_quality_evidence": list((operator_batch or {}).get("quality_evidence") or []),
        "operator_quality_source_breakdown": dict((operator_batch or {}).get("quality_source_breakdown") or {}),
        "quality_state_breakdown": breakdown,
        "recent_runs": recent_items,
        "recent_batches": [
            {
                "snapshot_id": batch.get("snapshot_id"),
                "quality_state": batch.get("quality_state"),
                "has_live": batch.get("has_live"),
                "run_types": list(batch.get("run_types") or []),
                "quality_reasons": list(batch.get("quality_reasons") or []),
            }
            for batch in recent_batches
        ],
        "latest_quality_state": latest_terminal_state,
        "latest_quality_reasons": list((latest_terminal_batch or {}).get("quality_reasons") or []),
        "latest_quality_source_breakdown": dict((latest_terminal_batch or {}).get("quality_source_breakdown") or {}),
        "batch_quality_reasons": list((operator_batch or {}).get("quality_reasons") or []),
        "batch_quality_source_breakdown": dict((operator_batch or {}).get("quality_source_breakdown") or {}),
        "quality_status_summary": operator_status_summary,
        "operator_hint": (
            "keyword_quality_degraded"
            if operator_state == "degraded"
            else "keyword_quality_partial"
            if operator_state == "partial"
            else "keyword_quality_stable"
            if operator_state == "full"
            else "keyword_quality_unknown"
        ),
    }
