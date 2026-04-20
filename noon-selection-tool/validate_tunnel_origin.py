from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from typing import Any


DEFAULT_NETWORK_NAME = "huihaokang-stable-net"
DEFAULT_PROJECT_NAME = "huihaokang-stable"
DEFAULT_CLOUDFLARED = "huihaokang-cloudflared"
DEFAULT_WEB = "huihaokang-web"


def run_command(command: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout)


def inspect_container(name: str) -> dict[str, Any]:
    proc = run_command(["docker", "inspect", name])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"docker inspect failed for {name}")
    payload = json.loads(proc.stdout)
    if not payload:
        raise RuntimeError(f"Container not found: {name}")
    return payload[0]


def get_network_snapshot(container: dict[str, Any]) -> dict[str, Any]:
    return container.get("NetworkSettings", {}).get("Networks", {}) or {}


def get_aliases(network_snapshot: dict[str, Any], network_name: str) -> list[str]:
    details = network_snapshot.get(network_name, {}) or {}
    aliases = details.get("Aliases") or []
    return [str(item) for item in aliases]


def get_log_tail(container_name: str, line_count: int) -> list[str]:
    proc = run_command(["docker", "logs", "--tail", str(line_count), container_name], timeout=180)
    lines = (proc.stderr or proc.stdout).splitlines()
    return lines[-line_count:]


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the Cloudflare tunnel origin path on NAS stable.")
    parser.add_argument("--network-name", default=DEFAULT_NETWORK_NAME)
    parser.add_argument("--project-name", default=DEFAULT_PROJECT_NAME)
    parser.add_argument("--cloudflared-name", default=DEFAULT_CLOUDFLARED)
    parser.add_argument("--web-name", default=DEFAULT_WEB)
    parser.add_argument("--log-lines", type=int, default=200)
    args = parser.parse_args()

    result: dict[str, Any] = {
        "status": "completed",
        "project_name": args.project_name,
        "network_name": args.network_name,
        "checks": {},
    }

    if shutil.which("docker") is None:
        result["status"] = "skipped"
        result["reason"] = "docker_not_installed"
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    try:
        cloudflared = inspect_container(args.cloudflared_name)
        web = inspect_container(args.web_name)
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1

    cloudflared_networks = get_network_snapshot(cloudflared)
    web_networks = get_network_snapshot(web)
    shared_networks = sorted(set(cloudflared_networks).intersection(web_networks))
    cloudflared_on_expected = args.network_name in cloudflared_networks
    web_on_expected = args.network_name in web_networks
    web_aliases = get_aliases(web_networks, args.network_name) if web_on_expected else []

    log_tail = get_log_tail(args.cloudflared_name, args.log_lines)
    lookup_errors = [
        line for line in log_tail
        if "lookup web" in line.lower() or "no such host" in line.lower() or "unable to reach the origin service" in line.lower()
    ]

    result["checks"]["cloudflared_container"] = {"status": cloudflared.get("State", {}).get("Status")}
    result["checks"]["web_container"] = {"status": web.get("State", {}).get("Status")}
    result["checks"]["shared_networks"] = shared_networks
    result["checks"]["expected_network_membership"] = {
        "cloudflared": cloudflared_on_expected,
        "web": web_on_expected,
    }
    result["checks"]["web_aliases"] = web_aliases
    result["checks"]["origin_resolution_errors"] = lookup_errors[-20:]

    failures: list[str] = []
    if cloudflared.get("State", {}).get("Status") != "running":
        failures.append("cloudflared_not_running")
    if web.get("State", {}).get("Status") != "running":
        failures.append("web_not_running")
    if not cloudflared_on_expected or not web_on_expected:
        failures.append("containers_not_on_expected_network")
    if args.network_name not in shared_networks:
        failures.append("containers_do_not_share_expected_network")
    if "web" not in web_aliases:
        failures.append("web_alias_missing_on_expected_network")
    if lookup_errors:
        failures.append("cloudflared_origin_lookup_errors_present")

    if failures:
        result["status"] = "failed"
        result["failures"] = failures
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
