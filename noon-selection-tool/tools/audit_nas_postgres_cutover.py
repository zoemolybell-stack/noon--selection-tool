from __future__ import annotations

import argparse
import json
from pathlib import Path


REQUIRED_BASE_KEYS = [
    "NOON_HOST_DATA_DIR",
    "NOON_HOST_RUNTIME_DATA_DIR",
    "NOON_HOST_BROWSER_PROFILES_DIR",
    "NOON_HOST_LOGS_DIR",
    "NOON_HOST_POSTGRES_DIR",
    "NOON_POSTGRES_DB",
    "NOON_POSTGRES_USER",
    "NOON_POSTGRES_PASSWORD",
    "NOON_OPS_DB",
    "NOON_PRODUCT_STORE_DB",
    "NOON_WAREHOUSE_DB",
]

POSTGRES_DSN_KEYS = [
    "NOON_OPS_DATABASE_URL",
    "NOON_PRODUCT_STORE_DATABASE_URL",
    "NOON_WAREHOUSE_DATABASE_URL",
]


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit .env.nas readiness for retained-data Postgres cutover.")
    parser.add_argument("--env-file", required=True, help="Path to .env.nas style file.")
    parser.add_argument("--require-postgres-dsn", action="store_true", help="Fail if NOON_*_DATABASE_URL values are empty or not postgres DSNs.")
    parser.add_argument("--check-host-paths", action="store_true", help="Check that host path directories exist on the current machine.")
    args = parser.parse_args()

    env_file = Path(args.env_file).resolve()
    if not env_file.exists():
        print(json.dumps({"status": "failed", "reason": "env_file_missing", "env_file": str(env_file)}, ensure_ascii=False, indent=2))
        return 1

    env_values = load_env_file(env_file)
    missing_base = [key for key in REQUIRED_BASE_KEYS if not env_values.get(key, "").strip()]
    invalid_dsns: list[str] = []
    missing_dsns: list[str] = []
    warnings: list[str] = []
    path_checks: dict[str, dict[str, str | bool]] = {}

    for key in POSTGRES_DSN_KEYS:
        value = env_values.get(key, "").strip()
        if not value:
            missing_dsns.append(key)
            continue
        lowered = value.lower()
        if not (lowered.startswith("postgresql://") or lowered.startswith("postgres://")):
            invalid_dsns.append(key)

    if args.check_host_paths:
        for key in [
            "NOON_HOST_DATA_DIR",
            "NOON_HOST_RUNTIME_DATA_DIR",
            "NOON_HOST_BROWSER_PROFILES_DIR",
            "NOON_HOST_LOGS_DIR",
            "NOON_HOST_POSTGRES_DIR",
        ]:
            raw_value = env_values.get(key, "").strip()
            if not raw_value:
                continue
            path = Path(raw_value)
            path_checks[key] = {
                "path": raw_value,
                "exists": path.exists(),
            }

    if env_values.get("NOON_POSTGRES_PASSWORD", "").strip() in {"", "changeme", "noon_local_dev"}:
        warnings.append("NOON_POSTGRES_PASSWORD is empty or using a placeholder value.")

    status = "completed"
    reason = "ready" if args.require_postgres_dsn else "base_ready"
    failures: list[str] = []
    failures.extend(missing_base)
    if args.require_postgres_dsn:
        failures.extend(missing_dsns)
        failures.extend(invalid_dsns)
    if args.check_host_paths:
        failures.extend([key for key, payload in path_checks.items() if not payload["exists"]])

    if failures:
        status = "failed"
        reason = "cutover_env_incomplete"

    result = {
        "status": status,
        "reason": reason,
        "env_file": str(env_file),
        "missing_base_keys": missing_base,
        "missing_postgres_dsn_keys": missing_dsns,
        "invalid_postgres_dsn_keys": invalid_dsns,
        "path_checks": path_checks,
        "warnings": warnings,
        "require_postgres_dsn": args.require_postgres_dsn,
        "check_host_paths": args.check_host_paths,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
