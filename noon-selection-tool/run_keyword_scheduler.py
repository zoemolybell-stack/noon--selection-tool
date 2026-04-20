from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from keyword_main import build_parser, keyword_core, logger, prepare_settings, run_monitor


DEFAULT_JOBS_PATH = ROOT / "config" / "keyword_scheduler_jobs.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run configured keyword scheduler jobs")
    parser.add_argument("--jobs-config", type=Path, default=DEFAULT_JOBS_PATH)
    parser.add_argument("--job", type=str, default="", help="Optional single job name")
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args()


def build_monitor_args(job: dict[str, object]) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args([])
    args.step = "monitor"
    args.runtime_scope = "keyword"
    args.monitor_config = str(job["monitor_config"])
    args.noon_count = int(job.get("noon_count") or 30)
    args.amazon_count = int(job.get("amazon_count") or 30)
    args.verbose = False
    return args


def main() -> None:
    args = parse_args()
    jobs_payload = json.loads(args.jobs_config.read_text(encoding="utf-8"))
    jobs = jobs_payload.get("jobs") or []
    selected_name = args.job.strip().lower()

    results = []
    for job in jobs:
        name = str(job.get("name") or "").strip()
        if not name:
            continue
        if not bool(job.get("enabled", True)):
            continue
        if selected_name and name.lower() != selected_name:
            continue

        monitor_args = build_monitor_args(job)
        monitor_args.verbose = args.verbose
        keyword_core.setup_logging(args.verbose)
        settings = prepare_settings(monitor_args)
        logger.info("keyword scheduler starting job: %s", name)
        summary = run_monitor(settings, monitor_args)
        results.append({"job": name, "status": summary.get("status"), "snapshot_id": summary.get("snapshot_id")})

    print(json.dumps({"results": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
