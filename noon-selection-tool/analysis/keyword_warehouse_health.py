from __future__ import annotations

from datetime import datetime
from typing import Any

from tools.keyword_runtime_health import build_keyword_quality_truth_model


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_iso_datetime(raw_value: Any) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _age_seconds(raw_value: Any) -> int | None:
    parsed = _parse_iso_datetime(raw_value)
    if not parsed:
        return None
    now = datetime.now(parsed.tzinfo) if parsed.tzinfo is not None else datetime.now()
    return max(0, int((now - parsed).total_seconds()))


def _classify_import_freshness(age_seconds: int | None) -> str:
    if age_seconds is None:
        return "unknown"
    if age_seconds <= 2 * 3600:
        return "fresh"
    if age_seconds <= 12 * 3600:
        return "delayed"
    return "stale"


def _classify_run_activity(age_seconds: int | None, incomplete_run_count: int) -> str:
    if incomplete_run_count > 0:
        return "active"
    if age_seconds is None:
        return "unknown"
    if age_seconds <= 6 * 3600:
        return "recent"
    if age_seconds <= 24 * 3600:
        return "cooldown"
    return "idle"


def _classify_health_state(import_state: str, run_state: str) -> str:
    if import_state == "stale":
        return "warning"
    if run_state == "active":
        return "active"
    if import_state == "fresh" and run_state in {"recent", "cooldown"}:
        return "healthy"
    if import_state == "unknown" and run_state == "unknown":
        return "unknown"
    return "attention"


def _first_known_text(*values: Any, default: str = "unknown") -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "unknown":
            return text
    return default


def build_keyword_warehouse_health_payload(
    *,
    overview_row: dict[str, Any],
    status_rows: list[dict[str, Any]],
    recent_imports: list[dict[str, Any]],
    recent_runs: list[dict[str, Any]],
) -> dict[str, Any]:
    last_keyword_imported_at = overview_row.get("last_keyword_imported_at") or ""
    last_keyword_run_started_at = overview_row.get("last_keyword_run_started_at") or ""
    last_keyword_run_finished_at = overview_row.get("last_keyword_run_finished_at") or ""
    incomplete_keyword_run_count = _safe_int(overview_row.get("incomplete_keyword_run_count"))
    active_keyword_run_count = _safe_int(overview_row.get("active_keyword_run_count"), incomplete_keyword_run_count)
    import_age_seconds = _age_seconds(last_keyword_imported_at)
    last_run_started_age_seconds = _age_seconds(last_keyword_run_started_at)
    last_run_finished_age_seconds = _age_seconds(last_keyword_run_finished_at)
    import_state = _classify_import_freshness(import_age_seconds)
    run_state = _classify_run_activity(last_run_started_age_seconds, incomplete_keyword_run_count)

    status_breakdown = {
        str(row.get("status") or "unknown"): _safe_int(row.get("count"))
        for row in status_rows
    }
    quality_summary = build_keyword_quality_truth_model(recent_runs)
    quality_breakdown = dict(quality_summary.get("quality_state_breakdown") or {})
    normalized_recent_runs = list(quality_summary.get("recent_runs") or [])
    active_run_row_count = active_keyword_run_count
    incomplete_run_row_count = incomplete_keyword_run_count
    active_keyword_run_count = _safe_int(quality_summary.get("active_snapshot_count"), active_keyword_run_count)
    incomplete_keyword_run_count = active_keyword_run_count
    latest_quality_state = _first_known_text(
        quality_summary.get("latest_terminal_quality_state"),
        quality_summary.get("latest_quality_state"),
    )
    latest_quality_reasons = list(quality_summary.get("latest_terminal_quality_reasons") or quality_summary.get("latest_quality_reasons") or [])
    latest_quality_source_breakdown = dict(
        quality_summary.get("latest_terminal_quality_source_breakdown")
        or quality_summary.get("latest_quality_source_breakdown")
        or {}
    )
    overall_quality_state = str(quality_summary.get("operator_quality_state") or "unknown")
    quality_status_summary = str(quality_summary.get("quality_status_summary") or "keyword runtime quality is unknown")

    return {
        "summary": {
            "keyword_source_db_count": _safe_int(overview_row.get("keyword_source_db_count")),
            "keyword_catalog_count": _safe_int(overview_row.get("keyword_catalog_count")),
            "keyword_metric_snapshot_count": _safe_int(overview_row.get("keyword_metric_snapshot_count")),
            "keyword_run_count": _safe_int(overview_row.get("keyword_run_count")),
            "active_keyword_run_count": active_keyword_run_count,
            "incomplete_keyword_run_count": incomplete_keyword_run_count,
            "active_keyword_run_row_count": active_run_row_count,
            "incomplete_keyword_run_row_count": incomplete_run_row_count,
            "last_keyword_imported_at": last_keyword_imported_at,
            "last_keyword_import_age_seconds": import_age_seconds,
            "import_freshness_state": import_state,
            "last_keyword_run_started_at": last_keyword_run_started_at,
            "last_keyword_run_started_age_seconds": last_run_started_age_seconds,
            "last_keyword_run_finished_at": last_keyword_run_finished_at,
            "last_keyword_run_finished_age_seconds": last_run_finished_age_seconds,
            "run_activity_state": run_state,
            "warehouse_health_state": _classify_health_state(import_state, run_state),
            "status_breakdown": status_breakdown,
            "live_batch_state": str(quality_summary.get("live_batch_state") or "idle"),
            "live_quality_reasons": list(quality_summary.get("live_quality_reasons") or []),
            "live_quality_evidence": list(quality_summary.get("live_quality_evidence") or []),
            "live_quality_source_breakdown": dict(quality_summary.get("live_quality_source_breakdown") or {}),
            "latest_quality_state": latest_quality_state,
            "latest_quality_reasons": latest_quality_reasons,
            "latest_quality_evidence": list(quality_summary.get("quality_evidence") or quality_summary.get("latest_quality_evidence") or []),
            "latest_quality_source_breakdown": latest_quality_source_breakdown,
            "latest_terminal_quality_state": str(quality_summary.get("latest_terminal_quality_state") or "unknown"),
            "latest_terminal_quality_reasons": list(quality_summary.get("latest_terminal_quality_reasons") or []),
            "latest_terminal_quality_evidence": list(quality_summary.get("latest_terminal_quality_evidence") or []),
            "latest_terminal_quality_source_breakdown": dict(quality_summary.get("latest_terminal_quality_source_breakdown") or {}),
            "operator_quality_state": overall_quality_state,
            "operator_quality_reasons": list(quality_summary.get("operator_quality_reasons") or []),
            "operator_quality_evidence": list(quality_summary.get("operator_quality_evidence") or []),
            "operator_quality_source_breakdown": dict(quality_summary.get("operator_quality_source_breakdown") or {}),
            "quality_reasons": list(quality_summary.get("quality_reasons") or []),
            "evidence": list(quality_summary.get("quality_evidence") or quality_summary.get("evidence") or []),
            "quality_evidence": list(quality_summary.get("evidence") or []),
            "quality_source_breakdown": dict(quality_summary.get("quality_source_breakdown") or {}),
            "quality_state_breakdown": quality_breakdown,
            "quality_health_state": overall_quality_state,
            "quality_status_summary": quality_status_summary,
            "lag_hint": (
                "warehouse keyword import is stale"
                if import_state == "stale"
                else "keyword runs are active in warehouse"
                if run_state == "active"
                else "warehouse keyword health looks stable"
            ),
        },
        "recent_imports": recent_imports,
        "recent_runs": normalized_recent_runs,
    }
