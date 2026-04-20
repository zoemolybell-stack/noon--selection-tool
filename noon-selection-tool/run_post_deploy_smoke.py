from __future__ import annotations

import argparse
import json
from typing import Any

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:8865"
SESSION = requests.Session()
SESSION.trust_env = False


def get_json(url: str, timeout: int = 30) -> dict[str, Any]:
    response = SESSION.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def post_json(url: str, payload: dict[str, Any], timeout: int = 30) -> dict[str, Any]:
    response = SESSION.post(url, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def run_smoke(base_url: str, create_sync_smoke: bool = False) -> dict[str, Any]:
    base = base_url.rstrip("/")
    result: dict[str, Any] = {
        "status": "completed",
        "base_url": base,
        "checks": {},
    }

    for path in ("/api/health", "/api/system/health", "/api/tasks", "/api/task-runs", "/api/workers"):
        payload = get_json(f"{base}{path}")
        result["checks"][path] = {
            "status": "ok",
            "keys": list(payload)[:8] if isinstance(payload, dict) else [],
        }

    if create_sync_smoke:
        created = post_json(
            f"{base}/api/tasks",
            {
                "task_type": "warehouse_sync",
                "payload": {
                    "reason": "post_deploy_smoke",
                    "actor": "smoke",
                },
                "created_by": "smoke",
                "priority": 8,
                "schedule_type": "manual",
                "schedule_expr": "",
            },
        )
        result["created_task"] = {
            "id": created.get("id"),
            "task_type": created.get("task_type"),
            "status": created.get("status"),
        }

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only post-deploy smoke checks against web endpoints.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--create-sync-smoke", action="store_true")
    args = parser.parse_args()

    try:
        result = run_smoke(args.base_url, create_sync_smoke=args.create_sync_smoke)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:  # pragma: no cover - CLI surface
        result = {
            "status": "failed",
            "base_url": args.base_url,
            "error": str(exc),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
