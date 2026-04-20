from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request


ROOT = Path(__file__).resolve().parent
DEFAULT_PROJECT_NAME = "huihaokang-stable"
STABLE_CONTAINERS = [
    "huihaokang-postgres",
    "huihaokang-web",
    "huihaokang-scheduler",
    "huihaokang-keyword-worker",
    "huihaokang-category-worker",
    "huihaokang-sync-worker",
    "huihaokang-cloudflared",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(raw_value: object) -> datetime | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _elapsed_seconds_since(raw_value: object) -> int | None:
    parsed = _parse_iso_datetime(raw_value)
    if parsed is None:
        return None
    now = datetime.now(parsed.tzinfo) if parsed.tzinfo is not None else datetime.now()
    return max(0, int((now - parsed).total_seconds()))


def _is_truthy_env(raw_value: object) -> bool:
    return str(raw_value or "").strip().lower() in {"1", "true", "yes", "on"}


def _summarize_worker_items(worker_items: list[dict[str, object]]) -> dict[str, object]:
    worker_type_counts: dict[str, int] = {}
    node_role_counts: dict[str, int] = {}
    remote_category_hosts: list[str] = []
    category_worker_hosts: list[str] = []
    remote_category_worker_count = 0
    local_category_worker_count = 0
    category_worker_count = 0
    for raw_worker in worker_items:
        if not isinstance(raw_worker, dict):
            continue
        details = raw_worker.get("details") if isinstance(raw_worker.get("details"), dict) else {}
        worker_type = str(raw_worker.get("worker_type") or "").strip().lower()
        node_role = str(details.get("node_role") or raw_worker.get("node_role") or "").strip().lower()
        node_host = str(details.get("node_host") or raw_worker.get("node_host") or "").strip()
        is_remote_category_worker = node_role == "remote_category"
        is_category_worker = worker_type == "category" or is_remote_category_worker
        if worker_type:
            worker_type_counts[worker_type] = worker_type_counts.get(worker_type, 0) + 1
        if node_role:
            node_role_counts[node_role] = node_role_counts.get(node_role, 0) + 1
        if is_category_worker:
            category_worker_count += 1
            if node_host and node_host not in category_worker_hosts:
                category_worker_hosts.append(node_host)
        if is_remote_category_worker:
            remote_category_worker_count += 1
            if node_host and node_host not in remote_category_hosts:
                remote_category_hosts.append(node_host)
        elif worker_type == "category":
            local_category_worker_count += 1
    return {
        "worker_count": len(worker_items),
        "worker_type_counts": worker_type_counts,
        "node_role_counts": node_role_counts,
        "remote_category_node_enabled": False,
        "category_worker_count": category_worker_count,
        "category_worker_heartbeat_state": "present" if category_worker_count > 0 else "missing",
        "category_worker_heartbeat_present": category_worker_count > 0,
        "remote_category_worker_count": remote_category_worker_count,
        "local_category_worker_count": local_category_worker_count,
        "remote_category_hosts": sorted(remote_category_hosts),
        "category_worker_hosts": sorted(category_worker_hosts),
    }


def switch_current_release(root: Path) -> dict[str, str]:
    current_link = root.parent.parent / "current"
    previous_target = ""
    try:
        previous_target = str(current_link.resolve(strict=True))
    except FileNotFoundError:
        previous_target = ""

    temp_link = current_link.parent / f".current-{root.name}"
    if temp_link.exists() or temp_link.is_symlink():
        temp_link.unlink()
    os.symlink(root, temp_link, target_is_directory=True)
    os.replace(temp_link, current_link)
    return {
        "current_link": str(current_link),
        "previous_target": previous_target,
        "new_target": str(root),
    }


def load_env_file(env_file: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_file.exists():
        raise FileNotFoundError(f"Env file not found: {env_file}")
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def resolve_project_name(env_values: dict[str, str], explicit: str | None) -> str:
    if explicit:
        return explicit.strip()
    if env_values.get("COMPOSE_PROJECT_NAME"):
        return env_values["COMPOSE_PROJECT_NAME"].strip()
    return DEFAULT_PROJECT_NAME


def tunnel_enabled(env_values: dict[str, str], mode: str) -> bool:
    if mode == "enabled":
        return True
    if mode == "disabled":
        return False
    return bool(env_values.get("TUNNEL_TOKEN", "").strip())


def resolve_scheduler_runtime(env_values: dict[str, str]) -> str:
    runtime = str(env_values.get("NOON_SCHEDULER_RUNTIME", "container")).strip().lower()
    if runtime in {"container", "host"}:
        return runtime
    return "container"


def build_compose_command(
    *,
    env_file: Path,
    project_name: str,
    tunnel_mode: str,
    build_images: bool,
) -> tuple[list[str], bool, str]:
    env_values = load_env_file(env_file)
    include_tunnel = tunnel_enabled(env_values, tunnel_mode)
    scheduler_runtime = resolve_scheduler_runtime(env_values)
    remote_category_node_enabled = _is_truthy_env(env_values.get("NOON_REMOTE_CATEGORY_NODE_ENABLED"))
    command = [
        "docker",
        "compose",
        "--project-name",
        project_name,
        "--env-file",
        str(env_file),
    ]
    if include_tunnel:
        command.extend(["--profile", "tunnel"])
    if not remote_category_node_enabled:
        command.extend(["--profile", "local-category"])
    command.extend(["up", "-d", "--remove-orphans"])
    if build_images:
        command.append("--build")
    services = [
        "postgres",
        "web",
        "sync-worker",
        "keyword-worker",
    ]
    if not remote_category_node_enabled:
        services.append("category-worker")
    if scheduler_runtime == "container":
        services.insert(2, "scheduler")
    if include_tunnel:
        services.append("cloudflared")
    command.extend(services)
    return command, include_tunnel, scheduler_runtime


def remove_existing_stable_containers(include_tunnel: bool, scheduler_runtime: str) -> dict[str, list[str]]:
    removed: list[str] = []
    failed: list[str] = []
    targets = list(STABLE_CONTAINERS)
    if include_tunnel and "huihaokang-cloudflared" not in targets:
        targets.append("huihaokang-cloudflared")
    if not include_tunnel:
        targets = [name for name in targets if name != "huihaokang-cloudflared"]
    if scheduler_runtime != "container" and "huihaokang-scheduler" not in targets:
        targets.append("huihaokang-scheduler")
    for container_name in targets:
        probe = subprocess.run(
            ["docker", "ps", "-aq", "--filter", f"name=^{container_name}$"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        container_id = probe.stdout.strip()
        if not container_id:
            continue
        cleanup = subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if cleanup.returncode == 0:
            removed.append(container_name)
        else:
            failed.append(f"{container_name}: {cleanup.stderr.strip() or cleanup.stdout.strip()}")
    return {"removed": removed, "failed": failed}


def run_post_deploy_runtime_reconciliation(
    *,
    env_file: Path,
    deploy_started_at: str,
    wait_seconds: int,
) -> dict[str, object]:
    cleanup_script = ROOT / "tools" / "cleanup_ops_history.py"
    if not cleanup_script.exists():
        raise FileNotFoundError(f"Missing cleanup script: {cleanup_script}")
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    host_command = [
        sys.executable,
        str(cleanup_script),
        "--env-file",
        str(env_file),
        "--apply",
        "--post-deploy-reconcile",
        "--deploy-started-at",
        deploy_started_at,
    ]
    proc = subprocess.run(
        host_command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=600,
        env=os.environ.copy(),
    )
    stdout_text = proc.stdout.strip()
    stderr_text = proc.stderr.strip()
    parsed_output: dict[str, object] = {}
    if stdout_text:
        try:
            parsed_output = json.loads(stdout_text)
        except json.JSONDecodeError:
            parsed_output = {"raw_stdout": stdout_text}
    result = {
        "status": "completed" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "command": host_command,
        "wait_seconds": wait_seconds,
        "stdout": parsed_output,
        "stderr_tail": stderr_text.splitlines()[-40:],
    }
    missing_driver = "ModuleNotFoundError: No module named 'psycopg'" in stderr_text
    if proc.returncode == 0 or not missing_driver:
        return result

    container_command = [
        "docker",
        "exec",
        "huihaokang-web",
        "python",
        "/app/tools/cleanup_ops_history.py",
        "--env-file",
        "/app/.env.nas",
        "--apply",
        "--post-deploy-reconcile",
        "--deploy-started-at",
        deploy_started_at,
    ]
    container_proc = subprocess.run(
        container_command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=600,
        env=os.environ.copy(),
    )
    container_stdout = container_proc.stdout.strip()
    container_stderr = container_proc.stderr.strip()
    container_output: dict[str, object] = {}
    if container_stdout:
        try:
            container_output = json.loads(container_stdout)
        except json.JSONDecodeError:
            container_output = {"raw_stdout": container_stdout}
    return {
        "status": "completed" if container_proc.returncode == 0 else "failed",
        "returncode": container_proc.returncode,
        "command": container_command,
        "wait_seconds": wait_seconds,
        "stdout": container_output,
        "stderr_tail": container_stderr.splitlines()[-40:],
        "fallback_from_host_python": result,
    }


def fetch_json(url: str, *, timeout_seconds: int = 20) -> dict[str, object]:
    with urllib_request.urlopen(url, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def run_post_deploy_health_recheck(
    *,
    base_url: str,
    expected_worker_types: set[str],
    deploy_started_at: str,
    remote_category_node_enabled: bool = False,
    remote_category_grace_seconds: int = 180,
) -> dict[str, object]:
    endpoints = {
        "health": f"{base_url}/api/health",
        "system_health": f"{base_url}/api/system/health",
        "workers": f"{base_url}/api/workers",
    }
    results: dict[str, object] = {"base_url": base_url, "endpoints": endpoints}
    try:
        health_payload = fetch_json(endpoints["health"])
        system_health_payload = fetch_json(endpoints["system_health"])
        workers_payload = fetch_json(endpoints["workers"])
    except (urllib_error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {
            **results,
            "status": "failed",
            "reason": "post_deploy_health_recheck_failed",
            "error": str(exc),
        }

    worker_items = workers_payload.get("items") if isinstance(workers_payload, dict) else []
    if not isinstance(worker_items, list):
        worker_items = []
    observed_worker_types = {
        str(item.get("worker_type") or "").strip().lower()
        for item in worker_items
        if isinstance(item, dict) and str(item.get("worker_type") or "").strip()
    }
    worker_summary = (
        system_health_payload.get("ops", {}).get("worker_summary")
        if isinstance(system_health_payload.get("ops"), dict)
        else {}
    )
    if not isinstance(worker_summary, dict):
        worker_summary = {}
    derived_worker_summary = _summarize_worker_items(worker_items)
    category_worker_count = int(
        worker_summary.get("category_worker_count")
        or derived_worker_summary.get("category_worker_count")
        or 0
    )
    category_worker_heartbeat_state = str(
        worker_summary.get("category_worker_heartbeat_state")
        or derived_worker_summary.get("category_worker_heartbeat_state")
        or ""
    ).strip().lower()
    category_worker_heartbeat_present = (
        bool(worker_summary.get("category_worker_heartbeat_present"))
        or bool(derived_worker_summary.get("category_worker_heartbeat_present"))
        or category_worker_count > 0
    )
    category_worker_age_seconds = _elapsed_seconds_since(deploy_started_at)
    category_worker_grace_applied = False
    effective_expected_worker_types = set(expected_worker_types)
    if remote_category_node_enabled:
        effective_expected_worker_types.discard("category")
        if not category_worker_heartbeat_present:
            if category_worker_age_seconds is not None and category_worker_age_seconds <= int(remote_category_grace_seconds):
                category_worker_grace_applied = True
                category_worker_heartbeat_state = "grace_pending"
            else:
                category_worker_heartbeat_state = "missing_after_grace"
    missing_worker_types = sorted(effective_expected_worker_types - observed_worker_types)
    extra_worker_types = sorted(observed_worker_types - expected_worker_types)
    health_status = str(health_payload.get("status") or "").strip().lower()
    system_status = str(system_health_payload.get("status") or "").strip().lower()
    ok = health_status == "ok" and system_status in {"ok", "degraded", "warning"}
    if missing_worker_types:
        ok = False
    if remote_category_node_enabled and not category_worker_heartbeat_present and not category_worker_grace_applied:
        ok = False
    if remote_category_node_enabled and category_worker_heartbeat_state == "missing_after_grace":
        ok = False

    return {
        **results,
        "status": "completed" if ok else "failed",
        "health_status": health_status,
        "system_status": system_status,
        "worker_count": len(worker_items),
        "observed_worker_types": sorted(observed_worker_types),
        "missing_worker_types": missing_worker_types,
        "extra_worker_types": extra_worker_types,
        "remote_category_node_enabled": remote_category_node_enabled,
        "category_worker_count": category_worker_count,
        "category_worker_heartbeat_state": category_worker_heartbeat_state or ("present" if category_worker_heartbeat_present else "missing"),
        "category_worker_heartbeat_present": category_worker_heartbeat_present,
        "category_worker_age_seconds": category_worker_age_seconds,
        "category_worker_grace_seconds": int(remote_category_grace_seconds),
        "category_worker_grace_applied": category_worker_grace_applied,
        "shared_sync_state": (
            (system_health_payload.get("shared_sync") or {}).get("state")
            if isinstance(system_health_payload.get("shared_sync"), dict)
            else ""
        ),
        "keyword_quality_state": (
            (system_health_payload.get("keyword_quality") or {}).get("operator_quality_state")
            if isinstance(system_health_payload.get("keyword_quality"), dict)
            else ""
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy or redeploy NAS stable with a fixed compose identity.")
    parser.add_argument("--env-file", default=".env.nas", help="Path to NAS env file.")
    parser.add_argument("--project-name", default="", help="Override compose project name.")
    parser.add_argument(
        "--tunnel",
        choices=("auto", "enabled", "disabled"),
        default="auto",
        help="Whether to include the cloudflared profile.",
    )
    parser.add_argument("--no-build", action="store_true", help="Skip docker build during compose up.")
    parser.add_argument("--skip-runtime-reconcile", action="store_true", help="Skip safe post-deploy cleanup of provably stranded running tasks and stale worker rows.")
    parser.add_argument("--skip-health-recheck", action="store_true", help="Skip post-deploy API health and worker sanity recheck.")
    parser.add_argument("--reconcile-wait-seconds", type=int, default=45, help="Seconds to wait after compose up before post-deploy runtime reconciliation. Default: 45.")
    parser.add_argument("--health-base-url", default=os.getenv("NOON_DEPLOY_HEALTH_BASE_URL", "http://127.0.0.1:8865"), help="Base URL for post-deploy health recheck. Default: http://127.0.0.1:8865")
    args = parser.parse_args()

    deploy_started_at = utc_now_iso()
    env_file = Path(args.env_file).resolve()
    env_values = load_env_file(env_file)
    project_name = resolve_project_name(env_values, args.project_name or None)
    if shutil.which("docker") is None:
        result = {
            "status": "skipped",
            "root": str(ROOT),
            "project_name": project_name,
            "reason": "docker_not_installed",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    command, include_tunnel, scheduler_runtime = build_compose_command(
        env_file=env_file,
        project_name=project_name,
        tunnel_mode=args.tunnel,
        build_images=not args.no_build,
    )
    cleanup_result = remove_existing_stable_containers(include_tunnel, scheduler_runtime)
    if cleanup_result["failed"]:
        result = {
            "status": "failed",
            "root": str(ROOT),
            "project_name": project_name,
            "reason": "stable_container_cleanup_failed",
            "cleanup": cleanup_result,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1

    proc = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        timeout=1800,
    )
    result = {
        "status": "completed" if proc.returncode == 0 else "failed",
        "root": str(ROOT),
        "project_name": project_name,
        "tunnel_included": include_tunnel,
        "scheduler_runtime": scheduler_runtime,
        "cleanup": cleanup_result,
        "command": command,
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout.splitlines()[-60:],
        "stderr_tail": proc.stderr.splitlines()[-60:],
        "deploy_started_at": deploy_started_at,
    }
    if proc.returncode == 0 and not args.skip_runtime_reconcile:
        reconcile_result = run_post_deploy_runtime_reconciliation(
            env_file=env_file,
            deploy_started_at=deploy_started_at,
            wait_seconds=max(0, int(args.reconcile_wait_seconds)),
        )
        result["runtime_reconciliation"] = reconcile_result
        if reconcile_result["status"] != "completed":
            result["status"] = "failed"
            result["reason"] = "post_deploy_runtime_reconciliation_failed"
    if proc.returncode == 0 and result["status"] == "completed" and not args.skip_health_recheck:
        remote_category_node_enabled = _is_truthy_env(env_values.get("NOON_REMOTE_CATEGORY_NODE_ENABLED"))
        health_recheck = run_post_deploy_health_recheck(
            base_url=str(args.health_base_url).rstrip("/"),
            expected_worker_types={"category", "keyword", "sync"},
            deploy_started_at=deploy_started_at,
            remote_category_node_enabled=remote_category_node_enabled,
        )
        result["health_recheck"] = health_recheck
        if health_recheck["status"] != "completed":
            result["status"] = "failed"
            result["reason"] = "post_deploy_health_recheck_failed"
    if proc.returncode == 0 and result["status"] == "completed":
        result["current_switch"] = switch_current_release(ROOT)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
