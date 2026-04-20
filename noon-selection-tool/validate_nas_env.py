from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parent

BASE_REQUIRED_KEYS = [
    "NOON_WAREHOUSE_DB",
    "NOON_OPS_DB",
    "NOON_PRODUCT_STORE_DB",
    "NOON_BROWSER_PROFILE_ROOT",
    "NOON_BROWSER_HEADLESS",
    "NOON_MAX_CONCURRENT_TASKS",
    "TASK_LEASE_TIMEOUT_SECONDS",
    "WORKER_POLL_SECONDS",
    "SCHEDULER_POLL_SECONDS",
    "NOON_SCHEDULER_RUNTIME",
]

CLOUDFLARE_REQUIRED_KEYS = [
    "TUNNEL_TOKEN",
]


def validate_env_file(env_path: Path, require_cloudflare: bool = False) -> dict:
    data = dotenv_values(env_path)
    missing: list[str] = []
    warnings: list[str] = []

    required = list(BASE_REQUIRED_KEYS)
    if require_cloudflare:
        required.extend(CLOUDFLARE_REQUIRED_KEYS)

    for key in required:
        if not str(data.get(key, "")).strip():
            missing.append(key)

    warehouse_db = str(data.get("NOON_WAREHOUSE_DB", "")).strip()
    ops_db = str(data.get("NOON_OPS_DB", "")).strip()
    browser_root = str(data.get("NOON_BROWSER_PROFILE_ROOT", "")).strip()
    headless = str(data.get("NOON_BROWSER_HEADLESS", "")).strip().lower()
    scheduler_runtime = str(data.get("NOON_SCHEDULER_RUNTIME", "")).strip().lower()

    if warehouse_db and not warehouse_db.startswith("/app/"):
        warnings.append("NOON_WAREHOUSE_DB is expected to point inside /app for containerized NAS deployment.")
    if ops_db and not ops_db.startswith("/app/"):
        warnings.append("NOON_OPS_DB is expected to point inside /app for containerized NAS deployment.")
    if browser_root and not browser_root.startswith("/app/"):
        warnings.append("NOON_BROWSER_PROFILE_ROOT is expected to point inside /app for containerized NAS deployment.")
    if headless and headless != "true":
        warnings.append("NOON_BROWSER_HEADLESS should be true in NAS stable.")
    if scheduler_runtime and scheduler_runtime not in {"host", "container"}:
        warnings.append("NOON_SCHEDULER_RUNTIME should be either host or container.")
    if scheduler_runtime == "container":
        warnings.append("NOON_SCHEDULER_RUNTIME=container is not the recommended NAS stable mode; host scheduler is preferred.")

    status = "completed" if not missing else "failed"
    return {
        "status": status,
        "env_file": str(env_path),
        "require_cloudflare": require_cloudflare,
        "missing": missing,
        "warnings": warnings,
        "values": {
            "NOON_WAREHOUSE_DB": warehouse_db,
            "NOON_OPS_DB": ops_db,
            "NOON_BROWSER_PROFILE_ROOT": browser_root,
            "NOON_BROWSER_HEADLESS": headless,
            "NOON_SCHEDULER_RUNTIME": scheduler_runtime,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate NAS stable env configuration.")
    parser.add_argument("--env-file", default=str(ROOT / ".env.nas.example"))
    parser.add_argument("--require-cloudflare", action="store_true")
    args = parser.parse_args()

    env_path = Path(args.env_file).resolve()
    if not env_path.exists():
        result = {
            "status": "failed",
            "reason": "env_file_not_found",
            "env_file": str(env_path),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1

    result = validate_env_file(env_path, require_cloudflare=args.require_cloudflare)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
