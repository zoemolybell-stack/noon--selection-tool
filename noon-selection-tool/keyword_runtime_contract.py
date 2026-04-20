from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

from config.settings import Settings
from scrapers.base_scraper import (
    build_failure_detail as build_scraper_failure_detail,
    build_keyword_result_stem,
    get_keyword_result_payload_error,
    normalize_keyword_result_payload,
)

QUALITY_REASON_CODES = {
    "dependency_missing",
    "runtime_import_error",
    "amazon_parse_failure",
    "amazon_upstream_blocked",
    "result_contract_mismatch",
    "timeout",
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


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def dedupe_strings(values: list[object]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def dedupe_failure_details(existing: list[object] | None, incoming: list[object] | None) -> list[dict[str, object]]:
    merged: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in list(existing or []) + list(incoming or []):
        if not isinstance(raw, dict):
            continue
        detail = {
            "platform": str(raw.get("platform") or "").strip(),
            "keyword": str(raw.get("keyword") or "").strip(),
            "failure_category": str(raw.get("failure_category") or "").strip(),
            "short_evidence": str(raw.get("short_evidence") or "").strip(),
            "expected_result_file": str(raw.get("expected_result_file") or "").strip(),
            "page_url": str(raw.get("page_url") or "").strip(),
            "page_number": raw.get("page_number"),
            "page_state": str(raw.get("page_state") or "").strip(),
            "snapshot_id": str(raw.get("snapshot_id") or "").strip(),
        }
        dedupe_key = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(detail)
    return merged


def build_failure_detail(
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
    detail = build_scraper_failure_detail(
        platform,
        keyword,
        failure_category=failure_category,
        short_evidence=short_evidence,
        expected_result_file=expected_result_file,
        page_url=page_url,
        page_number=page_number,
        page_state=page_state,
    )
    detail["snapshot_id"] = str(snapshot_id or "").strip()
    return detail


def _bs4_available_in_current_runtime() -> bool:
    return importlib.util.find_spec("bs4") is not None


def classify_platform_quality_reasons(
    platform: str,
    *,
    status: str,
    evidence: list[str],
) -> list[str]:
    normalized_status = str(status or "").strip().lower()
    lowered = [str(item or "").strip().lower() for item in evidence if str(item or "").strip()]
    reasons: list[str] = []

    def add(reason_code: str) -> None:
        if reason_code in QUALITY_REASON_CODES and reason_code not in reasons:
            reasons.append(reason_code)

    if any("dependency_missing:" in item for item in lowered):
        add("dependency_missing")
    if any("beautifulsoup4_unavailable" in item for item in lowered):
        add("runtime_import_error" if _bs4_available_in_current_runtime() else "dependency_missing")
    if any("modulenotfounderror" in item or "importerror" in item for item in lowered):
        add("runtime_import_error")
    if any("google_trends_missing" in item for item in lowered):
        add("google_trends_missing")
    if any("amazon_bsr_missing" in item for item in lowered):
        add("amazon_bsr_missing")
    if any(
        "timeout" in item
        or "timed out" in item
        or "net::err" in item
        or "captcha" in item
        or "robot" in item
        or "service unavailable" in item
        or "temporarily unavailable" in item
        or "something went wrong" in item
        for item in lowered
    ):
        add("amazon_upstream_blocked" if platform == "amazon" else "timeout")
    if any(
        marker in item
        for item in lowered
        for marker in (
            "missing_result_file:",
            "expected_result_file:",
            "invalid_json:",
            "incomplete_json:",
        )
    ):
        add("result_contract_mismatch")
    if any("fallback_misfire" in item for item in lowered):
        add("amazon_parse_failure" if platform == "amazon" else "fallback_misfire")
    if any(
        marker in item
        for item in lowered
        for marker in (
            "selector_miss:",
            "page_recognition_failed:",
            "page_parse_failure:",
        )
    ):
        add("amazon_parse_failure" if platform == "amazon" else "page_recognition_failed")
    if any("zero_results" in item or "no results" in item for item in lowered):
        add("zero_results")
    if any("partial_results" in item or "partial" in item for item in lowered):
        add("partial_results")
    if any("analysis_empty" in item for item in lowered):
        add("analysis_empty")

    if normalized_status == "failed" and not reasons:
        add("amazon_parse_failure" if platform == "amazon" else "runtime_error")
    elif normalized_status == "partial" and not reasons:
        add("partial_results")
    return reasons


def result_file_path(settings: Settings, platform: str, keyword: str) -> Path:
    return settings.snapshot_dir / platform / f"{build_keyword_result_stem(keyword)}.json"


def _safe_load_json_file(
    path: Path,
    *,
    expected_type: type | tuple[type, ...] | None = None,
    required_keys: list[str] | tuple[str, ...] | None = None,
) -> tuple[object | None, str]:
    if not path.exists():
        return None, f"missing_file:{path.name}"

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, f"invalid_json:{path.name}:{exc}"

    if expected_type is not None and not isinstance(payload, expected_type):
        expected_name = (
            ",".join(item.__name__ for item in expected_type)
            if isinstance(expected_type, tuple)
            else expected_type.__name__
        )
        return None, f"invalid_json_type:{path.name}:expected={expected_name}"

    if required_keys:
        if not isinstance(payload, dict):
            return None, f"incomplete_json:{path.name}:not_object"
        missing_keys = [key for key in required_keys if key not in payload]
        if missing_keys:
            return None, f"incomplete_json:{path.name}:missing={','.join(missing_keys)}"

    return payload, ""


def load_platform_result_payloads(
    settings: Settings,
    platform: str,
    keywords: list[str],
) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    for keyword in keywords:
        path = result_file_path(settings, platform, keyword)
        payload: dict[str, object] | None = None
        load_error = ""
        if path.exists():
            raw, load_error = _safe_load_json_file(
                path,
                expected_type=dict,
                required_keys=["products"],
            )
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


def classify_platform_payload(
    platform: str,
    keyword: str,
    payload: dict[str, object] | None,
    *,
    load_error: str = "",
) -> dict[str, object]:
    products = payload.get("products") if isinstance(payload, dict) else []
    products_count = len(products) if isinstance(products, list) else 0
    total_results = safe_int(payload.get("total_results"), 0) if isinstance(payload, dict) else 0
    page_state = str(payload.get("page_state") or "").strip().lower() if isinstance(payload, dict) else ""
    page_url = str(payload.get("page_url") or "").strip() if isinstance(payload, dict) else ""
    page_number = payload.get("page_number") if isinstance(payload, dict) else None

    zero_result_evidence: list[str] = []
    error_evidence: list[str] = []
    failure_details: list[dict[str, object]] = []
    if isinstance(payload, dict):
        zero_result_evidence = dedupe_strings(list(payload.get("zero_result_evidence") or []))
        error_evidence = dedupe_strings(list(payload.get("error_evidence") or []))
        failure_details = dedupe_failure_details([], payload.get("failure_details"))
        if payload.get("error"):
            error_evidence.append(str(payload.get("error")))
    if load_error:
        error_evidence.append(load_error)
    error_evidence = dedupe_strings(error_evidence)

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
        error_evidence = dedupe_strings(error_evidence + ["empty_results_without_products"])

    error_message = ""
    if status == "failed":
        if error_evidence:
            error_message = "; ".join(error_evidence[:5])
        else:
            error_message = f"{platform}:{keyword}:empty_result_without_evidence"
    if status in {"failed", "partial"} and not failure_details:
        reason_codes = classify_platform_quality_reasons(platform, status=status, evidence=error_evidence)
        failure_details = [
            build_failure_detail(
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


def summarize_platform_snapshot(
    settings: Settings,
    platform: str,
    keywords: list[str],
) -> dict[str, object]:
    records = load_platform_result_payloads(settings, platform, keywords)
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

        payload_summary = classify_platform_payload(
            platform,
            str(record.get("keyword") or ""),
            record.get("payload") if isinstance(record.get("payload"), dict) else None,
            load_error=str(record.get("load_error") or ""),
        )
        payload_statuses.append(str(payload_summary["status"]))
        products_count += safe_int(payload_summary.get("products_count"), 0)
        total_results += safe_int(payload_summary.get("total_results"), 0)
        zero_result_evidence.extend(list(payload_summary.get("zero_result_evidence") or []))
        error_evidence.extend(list(payload_summary.get("error_evidence") or []))
        detail_items = dedupe_failure_details([], payload_summary.get("failure_details"))
        if payload_summary["status"] in {"failed", "partial"} and not detail_items:
            reason_codes = classify_platform_quality_reasons(
                platform,
                status=str(payload_summary.get("status") or "failed"),
                evidence=list(payload_summary.get("error_evidence") or []),
            )
            detail_items = [
                build_failure_detail(
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
                if not str(item.get("expected_result_file") or "").strip():
                    item["expected_result_file"] = str(path)
                if not str(item.get("snapshot_id") or "").strip():
                    item["snapshot_id"] = str(getattr(settings, "snapshot_id", "") or "")
        failure_details = dedupe_failure_details(failure_details, detail_items)

        if payload_summary["status"] == "failed":
            failed_keywords.append(str(payload_summary["keyword"]))
            if payload_summary.get("error"):
                error_messages.append(str(payload_summary["error"]))
        elif payload_summary["status"] == "zero_results":
            zero_result_keywords.append(str(payload_summary["keyword"]))

    zero_result_evidence = dedupe_strings(zero_result_evidence)
    error_evidence = dedupe_strings(error_evidence)
    error_messages = dedupe_strings(error_messages)

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
        "error_evidence": dedupe_strings(error_evidence),
        "zero_result_evidence": zero_result_evidence,
        "failure_details": failure_details,
        "failed_keywords": failed_keywords,
        "zero_result_keywords": zero_result_keywords,
    }
