import argparse
import json
import os
import sys
import time
import urllib.request
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.keyword_runtime_health import build_quality_payload


BASE_URL = os.getenv("NOON_REPORT_BASE_URL", "http://127.0.0.1:8865")
REPORT_ROOT = Path(os.getenv("NOON_REPORT_ROOT", "/volume1/docker/huihaokang-erp/shared/report/crawl"))
STATE_DIR = REPORT_ROOT / "state"
DAILY_DIR = REPORT_ROOT / "daily"
METRICS_FILE = STATE_DIR / "metrics_snapshots.jsonl"
CURRENT_SYMLINK = Path("/volume1/docker/huihaokang-erp/current")
REQUEST_RETRIES = int(os.getenv("NOON_REPORT_REQUEST_RETRIES", "3"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("NOON_REPORT_REQUEST_TIMEOUT_SECONDS", "12"))
REQUEST_BACKOFF_SECONDS = float(os.getenv("NOON_REPORT_REQUEST_BACKOFF_SECONDS", "2"))
SNAPSHOT_LOOKBACK_HOURS = int(os.getenv("NOON_REPORT_SNAPSHOT_LOOKBACK_HOURS", "72"))
KEYWORD_RUNTIME_ROOT = Path(os.getenv("NOON_KEYWORD_RUNTIME_DIR", "/volume1/docker/huihaokang-erp/runtime_data/keyword"))
KEYWORD_MONITOR_SUMMARY_PATH = os.getenv("NOON_KEYWORD_MONITOR_SUMMARY_PATH", "")
DIR_MODE = 0o755
FILE_MODE = 0o644


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def shanghai_now() -> datetime:
    return utc_now().astimezone(timezone(timedelta(hours=8)))


def request_json(path: str, *, retries: int = REQUEST_RETRIES, timeout_seconds: int = REQUEST_TIMEOUT_SECONDS) -> dict:
    last_error: Exception | None = None
    attempts = max(1, retries)
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(f"{BASE_URL}{path}", method="GET")
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = exc
            if attempt + 1 >= attempts:
                break
            time.sleep(REQUEST_BACKOFF_SECONDS * (attempt + 1))
    raise RuntimeError(f"request failed after {attempts} attempts: GET {path}: {last_error}") from last_error


def safe_request_json(path: str) -> tuple[dict, str | None]:
    try:
        payload = request_json(path)
        return (payload if isinstance(payload, dict) else {}), None
    except Exception as exc:  # pragma: no cover - network dependent
        return {}, str(exc)


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def normalize_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_report_date_spec(value: str | None) -> list[date]:
    text = str(value or "").strip()
    if not text:
        return []
    if len(text) == 17 and text[8] == "-":
        start_text, end_text = text.split("-", 1)
        start_day = datetime.strptime(start_text, "%Y%m%d").date()
        end_day = datetime.strptime(end_text, "%Y%m%d").date()
        if end_day < start_day:
            raise ValueError(f"invalid report-date range: {text}")
        days: list[date] = []
        current = start_day
        while current <= end_day:
            days.append(current)
            current += timedelta(days=1)
        return days
    return [datetime.strptime(text, "%Y%m%d").date()]


def shanghai_midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=timezone(timedelta(hours=8)))


def load_snapshots(window_start: datetime, window_end: datetime) -> list[dict]:
    if not METRICS_FILE.exists():
        return []
    rows: list[dict] = []
    with METRICS_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = parse_ts(item.get("ts"))
            if ts and window_start <= ts <= window_end:
                item["_ts"] = ts
                rows.append(item)
    rows.sort(key=lambda row: row.get("_ts") or datetime.min.replace(tzinfo=timezone.utc))
    return rows


def select_snapshot_before(rows: list[dict], boundary: datetime) -> dict | None:
    candidate: dict | None = None
    for item in rows:
        ts = item.get("_ts")
        if isinstance(ts, datetime) and ts <= boundary:
            candidate = item
    return candidate


def format_delta(current: int | None, previous: int | None) -> str:
    current_value = normalize_int(current)
    previous_value = normalize_int(previous)
    if current_value is None or previous_value is None:
        return "n/a"
    return f"{current_value - previous_value:+d}"


def ensure_dirs() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    try:
        REPORT_ROOT.chmod(DIR_MODE)
    except OSError:
        pass
    for path in (STATE_DIR, DAILY_DIR):
        try:
            path.chmod(DIR_MODE)
        except OSError:
            pass


def write_readable_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    try:
        path.chmod(FILE_MODE)
    except OSError:
        pass


def current_release_label() -> str:
    try:
        manifest_candidates = [
            CURRENT_SYMLINK / "release-manifest.json",
            CURRENT_SYMLINK / ".release-manifest.json",
        ]
        for manifest_path in manifest_candidates:
            if not manifest_path.exists():
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            for key in ("release_version", "version", "release"):
                value = str(manifest.get(key) or "").strip()
                if value:
                    return value
        return CURRENT_SYMLINK.resolve().name
    except Exception:
        return "unknown"


def _keyword_monitor_summary_candidates() -> list[Path]:
    candidates: list[Path] = []
    if KEYWORD_MONITOR_SUMMARY_PATH:
        candidates.append(Path(KEYWORD_MONITOR_SUMMARY_PATH))
    candidates.extend(
        [
            KEYWORD_RUNTIME_ROOT / "monitor" / "keyword_monitor_last_run.json",
            Path("/app/runtime_data/keyword/monitor/keyword_monitor_last_run.json"),
            Path("/volume1/docker/huihaokang-erp/runtime_data/keyword/monitor/keyword_monitor_last_run.json"),
            Path(__file__).resolve().parents[1] / "runtime_data" / "keyword" / "monitor" / "keyword_monitor_last_run.json",
        ]
    )
    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def load_latest_keyword_monitor_summary() -> tuple[dict[str, Any], str | None]:
    for path in _keyword_monitor_summary_candidates():
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload, str(path)
    return {}, None


def normalize_keyword_quality_summary(payload: dict[str, Any]) -> dict[str, Any]:
    quality = payload.get("quality_summary") if isinstance(payload.get("quality_summary"), dict) else {}
    candidate = dict(quality or payload)
    normalized = build_quality_payload(
        candidate,
        live_batch_state=str(
            candidate.get("live_batch_state")
            or payload.get("live_batch_state")
            or payload.get("activity_state")
            or payload.get("monitor_state")
            or payload.get("status")
            or "idle"
        ),
        latest_terminal_quality_state=str(
            candidate.get("latest_terminal_batch_state")
            or candidate.get("latest_terminal_quality_state")
            or candidate.get("latest_quality_state")
            or payload.get("latest_terminal_batch_state")
            or payload.get("latest_terminal_quality_state")
            or payload.get("quality_state")
            or "unknown"
        ),
        operator_quality_state=str(
            candidate.get("operator_quality_state")
            or payload.get("operator_quality_state")
            or payload.get("quality_state")
            or "unknown"
        ),
    )
    normalized.update(
        {
            "state": candidate.get("state") or normalized.get("state") or "unknown",
            "crawl_state": candidate.get("crawl_state") or payload.get("crawl_state") or normalized.get("live_batch_state") or "unknown",
            "analysis_state": candidate.get("analysis_state") or payload.get("analysis_state") or "unknown",
            "platforms": dict(candidate.get("platforms") or payload.get("platforms") or {}),
            "signals": dict(candidate.get("signals") or payload.get("signals") or {}),
            "quality_flags": list(candidate.get("quality_flags") or payload.get("quality_flags") or []),
        }
    )
    return normalized


def build_report(*, report_day: date | None = None, backfilled: bool = False) -> tuple[str, dict]:
    report_now = shanghai_now()
    report_day = report_day or report_now.date()
    report_day_key = report_day.strftime("%Y%m%d")
    report_window_start_local = shanghai_midnight(report_day)
    report_window_end_local = report_window_start_local + timedelta(days=1)
    period_start = report_window_start_local.astimezone(timezone.utc)
    period_end = report_window_end_local.astimezone(timezone.utc)
    snapshot_window_start = period_start - timedelta(hours=SNAPSHOT_LOOKBACK_HOURS)
    backfilled = bool(backfilled or report_day != report_now.date())

    health, health_error = safe_request_json("/api/health")
    system, system_error = safe_request_json("/api/system/health")
    runs_summary, runs_summary_error = safe_request_json("/api/runs/summary")
    dashboard, dashboard_error = safe_request_json("/api/dashboard")
    tasks_payload, tasks_error = safe_request_json("/api/tasks?limit=500")
    tasks = tasks_payload.get("items") or []

    snapshots = load_snapshots(snapshot_window_start, period_end)
    baseline = select_snapshot_before(snapshots, period_start)
    latest = select_snapshot_before(snapshots, period_end)

    api_product_count = normalize_int(health.get("product_count"))
    api_observation_count = normalize_int(health.get("observation_count"))
    snapshot_product_count = normalize_int((latest or {}).get("product_count"))
    snapshot_observation_count = normalize_int((latest or {}).get("observation_count"))
    current_product_count = api_product_count if api_product_count is not None else snapshot_product_count
    current_observation_count = api_observation_count if api_observation_count is not None else snapshot_observation_count
    count_source = "api" if api_product_count is not None else ("snapshot" if snapshot_product_count is not None else "n/a")

    baseline_product_count = normalize_int((baseline or {}).get("product_count"))
    baseline_observation_count = normalize_int((baseline or {}).get("observation_count"))

    task_rows = []
    for item in tasks:
        ts = parse_ts(item.get("created_at")) or parse_ts(item.get("updated_at")) or parse_ts(item.get("started_at"))
        if ts and ts >= period_start:
            task_rows.append(item)

    status_counter = Counter()
    type_counter = Counter()
    failures = []
    recent_terminals = []
    for item in task_rows:
        task_type = str(item.get("task_type") or "")
        status = str(item.get("status") or "")
        status_counter[status] += 1
        type_counter[task_type] += 1
        if status == "failed":
            failures.append(item)
        recent_terminals.append(item)
    recent_terminals.sort(key=lambda row: row.get("updated_at") or row.get("created_at") or "", reverse=True)
    recent_terminals = recent_terminals[:12]

    ops = system.get("ops") or {}
    workers = ops.get("workers") or []
    worker_lines = [
        f"- {item.get('worker_type')}: status={item.get('status')} task_id={item.get('task_id')} updated_at={item.get('updated_at')}"
        for item in workers
    ]

    overview = dashboard.get("overview") or {}
    scope = dashboard.get("scope") or {}
    child_categories = scope.get("child_categories") or []
    platform_lines = [
        f"- overview.product_count={overview.get('product_count')} keyword_count={overview.get('keyword_count')} overlap_count={overview.get('overlap_count')} last_sync_at={overview.get('last_sync_at')}",
    ]
    for item in child_categories[:5]:
        platform_lines.append(
            f"- top_category {item.get('label')}: products={item.get('product_count')} avg_price={item.get('avg_price')} signal_pct={item.get('signal_coverage_pct')}"
        )

    keyword_monitor_summary, keyword_monitor_summary_path = load_latest_keyword_monitor_summary()
    keyword_quality_summary = normalize_keyword_quality_summary(keyword_monitor_summary)
    runtime_keyword_quality = (runs_summary.get("keyword_quality") or system.get("keyword_quality") or {}) if isinstance(runs_summary, dict) else {}
    watchdog_summary = (runs_summary.get("watchdog") or system.get("watchdog") or {}) if isinstance(runs_summary, dict) else {}
    watchdog_alert_summary = watchdog_summary.get("current_alert_summary") if isinstance(watchdog_summary.get("current_alert_summary"), dict) else {}
    keyword_quality_platforms = keyword_quality_summary.get("platforms") or {}
    keyword_quality_signals = keyword_quality_summary.get("signals") or {}
    keyword_quality_flags = keyword_quality_summary.get("quality_flags") or []
    keyword_quality_reasons = keyword_quality_summary.get("quality_reasons") or []
    keyword_quality_evidence = keyword_quality_summary.get("quality_evidence") or []
    keyword_quality_source_breakdown = keyword_quality_summary.get("quality_source_breakdown") or {}
    runtime_operator_quality_state = str(
        runtime_keyword_quality.get("operator_quality_state")
        or runtime_keyword_quality.get("latest_terminal_batch_state")
        or runtime_keyword_quality.get("latest_terminal_quality_state")
        or runtime_keyword_quality.get("latest_quality_state")
        or keyword_quality_summary.get("operator_quality_state")
        or keyword_quality_summary.get("latest_terminal_batch_state")
        or keyword_quality_summary.get("latest_terminal_quality_state")
        or keyword_quality_summary.get("state")
        or "unknown"
    )
    runtime_latest_terminal_batch_state = str(
        runtime_keyword_quality.get("latest_terminal_batch_state")
        or runtime_keyword_quality.get("latest_terminal_quality_state")
        or runtime_keyword_quality.get("latest_quality_state")
        or "unknown"
    )
    runtime_live_batch_state = str(
        runtime_keyword_quality.get("live_batch_state")
        or keyword_quality_summary.get("live_batch_state")
        or "idle"
    )

    api_errors = {
        "health": health_error,
        "system": system_error,
        "runs_summary": runs_summary_error,
        "dashboard": dashboard_error,
        "tasks": tasks_error,
    }
    error_lines = [f"- {name}: `{message}`" for name, message in api_errors.items() if message]

    report = []
    report.append("# NAS Crawl Daily Report")
    report.append("")
    report.append(f"- Generated At: {report_now.strftime('%Y-%m-%d %H:%M:%S')} Asia/Shanghai")
    report.append(f"- Report Date: `{report_day.isoformat()}`")
    report.append(f"- Report Kind: `{'backfill' if backfilled else 'daily'}`")
    report.append(
        f"- Window: {(period_start + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')} ~ {(period_end + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')} Asia/Shanghai"
    )
    report.append(f"- Release: `{current_release_label()}`")
    report.append(f"- Warehouse Backend: `{health.get('warehouse_db')}`")
    report.append("")
    report.append("## Current Health")
    report.append("")
    report.append(f"- health.status: `{health.get('status')}`")
    report.append(f"- product_count: `{current_product_count if current_product_count is not None else 'n/a'}`")
    report.append(f"- observation_count: `{current_observation_count if current_observation_count is not None else 'n/a'}`")
    report.append(f"- product_count_source: `{count_source}`")
    report.append(f"- 24h products delta: `{format_delta(current_product_count, baseline_product_count)}`")
    report.append(f"- 24h observations delta: `{format_delta(current_observation_count, baseline_observation_count)}`")
    if baseline:
        baseline_ts = baseline.get("_ts")
        report.append(f"- delta baseline snapshot: `{baseline_ts.isoformat() if isinstance(baseline_ts, datetime) else baseline.get('ts')}`")
    if latest:
        latest_ts = latest.get("_ts")
        report.append(f"- latest snapshot used for fallback: `{latest_ts.isoformat() if isinstance(latest_ts, datetime) else latest.get('ts')}`")
    if error_lines:
        report.append("- api_warnings:")
        report.extend(error_lines)
    report.append("")
    report.append("## Task Summary")
    report.append("")
    report.append(f"- 24h task count: `{len(task_rows)}`")
    report.append(f"- status counts: `{dict(status_counter)}`")
    report.append(f"- task type counts: `{dict(type_counter)}`")
    report.append(f"- failure count: `{len(failures)}`")
    report.append("")
    report.append("## Worker Status")
    report.append("")
    report.extend(worker_lines or ["- no worker data"])
    report.append("")
    report.append("## Platform Overview")
    report.append("")
    report.extend(platform_lines or ["- no platform overview data"])
    report.append("")
    report.append("## Keyword Quality Summary")
    report.append("")
    report.append(f"- current_quality_state: `{keyword_quality_summary.get('state')}`")
    report.append(f"- runtime_operator_quality_state: `{runtime_operator_quality_state}`")
    report.append(f"- runtime_live_batch_state: `{runtime_live_batch_state}`")
    report.append(f"- runtime_latest_terminal_batch_state: `{runtime_latest_terminal_batch_state}`")
    report.append(f"- crawl_state: `{keyword_quality_summary.get('crawl_state')}`")
    report.append(f"- analysis_state: `{keyword_quality_summary.get('analysis_state')}`")
    report.append(f"- runtime_quality_breakdown: `{runtime_keyword_quality.get('quality_state_breakdown') or {}}`")
    report.append(f"- monitor_summary_source: `{keyword_monitor_summary_path or 'n/a'}`")
    report.append(f"- requested_platforms: `{keyword_monitor_summary.get('platforms') or []}`")
    report.append(f"- quality_flags: `{keyword_quality_flags}`")
    report.append(f"- quality_reasons: `{keyword_quality_reasons}`")
    report.append(f"- quality_evidence: `{keyword_quality_evidence[:6]}`")
    report.append(f"- quality_source_breakdown: `{keyword_quality_source_breakdown}`")
    report.append(f"- signals: `{keyword_quality_signals}`")
    if keyword_quality_platforms:
        report.append("- platform_quality:")
        for platform, platform_summary in keyword_quality_platforms.items():
            if not isinstance(platform_summary, dict):
                continue
            report.append(
                f"  - {platform}: state=`{platform_summary.get('state')}` status=`{platform_summary.get('status')}` evidence=`{platform_summary.get('evidence')}`"
            )
    else:
        report.append("- platform_quality: `n/a`")
    report.append(f"- watchdog_status: `{watchdog_summary.get('status') or 'unknown'}`")
    report.append(f"- watchdog_issue_count: `{watchdog_summary.get('issue_count') if watchdog_summary else 'n/a'}`")
    report.append(f"- watchdog_missing_dates: `{watchdog_summary.get('report_missing_dates') or []}`")
    report.append(f"- watchdog_alert_summary: `{watchdog_alert_summary or {}}`")
    report.append("")
    report.append("## Recent Terminal Tasks")
    report.append("")
    report.append("| task_id | type | status | created_by | updated_at |")
    report.append("| --- | --- | --- | --- | --- |")
    for item in recent_terminals:
        report.append(
            f"| {item.get('id')} | {item.get('task_type')} | {item.get('status')} | {item.get('created_by')} | {item.get('updated_at') or item.get('created_at')} |"
        )
    if not recent_terminals:
        report.append("| - | - | - | - | - |")
    report.append("")
    report.append("## Snapshots")
    report.append("")
    report.append(f"- 24h snapshot count: `{len(snapshots)}`")
    if latest:
        report.append(f"- latest snapshot event: `{latest.get('event')}` at `{latest.get('ts')}`")
    else:
        report.append("- latest snapshot event: `n/a`")

    metadata = {
        "generated_at": report_now.isoformat(),
        "report_date": report_day.isoformat(),
        "report_day_key": report_day_key,
        "report_kind": "backfill" if backfilled else "daily",
        "backfilled": backfilled,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "release": current_release_label(),
        "product_count": current_product_count,
        "observation_count": current_observation_count,
        "product_count_source": count_source,
        "product_delta_24h": None if baseline_product_count is None or current_product_count is None else current_product_count - baseline_product_count,
        "observation_delta_24h": None if baseline_observation_count is None or current_observation_count is None else current_observation_count - baseline_observation_count,
        "status_counts": dict(status_counter),
        "type_counts": dict(type_counter),
        "snapshot_count": len(snapshots),
        "api_errors": api_errors,
        "keyword_quality_summary": keyword_quality_summary,
        "runtime_keyword_quality": runtime_keyword_quality,
        "runtime_operator_quality_state": runtime_operator_quality_state,
        "runtime_live_batch_state": runtime_live_batch_state,
        "runtime_latest_terminal_batch_state": runtime_latest_terminal_batch_state,
        "watchdog_summary": watchdog_summary,
        "watchdog_alert_summary": watchdog_alert_summary,
        "keyword_quality_summary_source": keyword_monitor_summary_path,
        "baseline_snapshot_ts": baseline.get("_ts").isoformat() if baseline and isinstance(baseline.get("_ts"), datetime) else None,
        "latest_snapshot_ts": latest.get("_ts").isoformat() if latest and isinstance(latest.get("_ts"), datetime) else None,
    }
    return "\n".join(report) + "\n", metadata


def write_outputs(markdown: str, payload: dict[str, Any], *, update_latest: bool = True) -> Path:
    ensure_dirs()
    report_now = shanghai_now()
    report_day_key = str(payload.get("report_day_key") or report_now.strftime("%Y%m%d"))
    stem = f"crawl_report_{report_day_key}_{report_now.strftime('%H%M')}"
    report_path = DAILY_DIR / f"{stem}.md"
    json_path = DAILY_DIR / f"{stem}.json"
    latest_path = REPORT_ROOT / "latest.md"
    latest_json = REPORT_ROOT / "latest.json"
    metadata_json = json.dumps(payload, ensure_ascii=False, indent=2)
    write_readable_text(report_path, markdown)
    write_readable_text(json_path, metadata_json)
    if update_latest:
        write_readable_text(latest_path, markdown)
        write_readable_text(latest_json, metadata_json)
    print(report_path)
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate crawl daily report")
    parser.add_argument(
        "--report-date",
        dest="report_date",
        default="",
        help="Report date as YYYYMMDD or range YYYYMMDD-YYYYMMDD for backfill",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Mark generated report(s) as backfill and do not overwrite latest.md/latest.json",
    )
    args = parser.parse_args()
    ensure_dirs()
    report_days = parse_report_date_spec(args.report_date)
    if not report_days:
        report_days = [shanghai_now().date()]
    for report_day in report_days:
        report_body, metadata = build_report(report_day=report_day, backfilled=args.backfill)
        write_outputs(report_body, metadata, update_latest=not args.backfill and report_day == shanghai_now().date())


if __name__ == "__main__":
    main()
