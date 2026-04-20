from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import warehouse_sync
from build_analytics_warehouse import configure_stdio


ROOT = Path(__file__).parent
DEFAULT_WAREHOUSE_DB = ROOT / "data" / "analytics" / "warehouse.db"
DEFAULT_TRIGGER_DB = ROOT / "data" / "product_store.db"
DEFAULT_KEYWORD_STAGE_DB = ROOT / "runtime_data" / "keyword" / "product_store.db"
RESULT_PREFIX = "WAREHOUSE_SYNC_RESULT="


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("shared-warehouse-sync")


def _configured_reference(url_env_name: str, path_env_name: str, default_value: Path) -> str:
    url_value = str(os.getenv(url_env_name) or "").strip()
    if url_value:
        return url_value
    path_value = str(os.getenv(path_env_name) or "").strip()
    if path_value:
        return path_value
    return str(default_value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute shared warehouse sync with lock/status contract")
    parser.add_argument("--actor", type=str, default="main_window", help="sync actor label")
    parser.add_argument("--reason", type=str, default="manual_sync", help="sync reason label")
    parser.add_argument("--trigger-db", type=str, default=_configured_reference("NOON_PRODUCT_STORE_DATABASE_URL", "NOON_PRODUCT_STORE_DB", DEFAULT_TRIGGER_DB), help="stage DB or DSN that triggered this sync")
    parser.add_argument("--warehouse-db", type=str, default=_configured_reference("NOON_WAREHOUSE_DATABASE_URL", "NOON_WAREHOUSE_DB", DEFAULT_WAREHOUSE_DB), help="warehouse DB path or DSN")
    parser.add_argument(
        "--builder-arg",
        dest="builder_args",
        action="append",
        default=[],
        help="extra argument forwarded to build_analytics_warehouse.py; can be repeated",
    )
    return parser.parse_args()


def _emit_result(payload: dict[str, object]) -> None:
    print(f"{RESULT_PREFIX}{json.dumps(payload, ensure_ascii=False)}")


def _extract_embedded_result(lines: list[str]) -> dict[str, object] | None:
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped.startswith(RESULT_PREFIX):
            continue
        raw_payload = stripped[len(RESULT_PREFIX) :].strip()
        if not raw_payload:
            continue
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _build_builder_command(
    *,
    python_executable: str,
    builder_script: Path,
    trigger_db: str | Path,
    warehouse_db: str | Path,
    builder_args: list[str],
) -> list[str]:
    trigger_ref = str(trigger_db)
    warehouse_ref = str(warehouse_db)
    command = [
        python_executable,
        str(builder_script),
        "--warehouse-db",
        warehouse_ref,
    ]
    normalized_builder_args = list(builder_args)
    for default_arg in ("--skip-stats", "--skip-reports"):
        if default_arg not in normalized_builder_args:
            normalized_builder_args.append(default_arg)

    if trigger_ref.lower().startswith(("postgres://", "postgresql://", "postgresql+psycopg://")):
        command.extend(["--category-db", trigger_ref])
        command.extend(["--keyword-dbs", trigger_ref])
        command.extend(normalized_builder_args)
        return command

    resolved_trigger = Path(trigger_ref).resolve()
    default_category = DEFAULT_TRIGGER_DB.resolve()
    default_keyword = DEFAULT_KEYWORD_STAGE_DB.resolve()

    if resolved_trigger == default_category:
        command.extend(["--category-db", str(resolved_trigger)])
    elif resolved_trigger == default_keyword:
        command.extend(["--keyword-dbs", str(resolved_trigger)])
    else:
        # Custom stage DBs are treated as isolated/shared stages for local rehearsal or targeted repairs.
        command.extend(["--category-db", str(resolved_trigger)])
        command.extend(["--keyword-dbs", str(resolved_trigger)])

    command.extend(normalized_builder_args)
    return command


def main() -> int:
    configure_stdio()
    args = parse_args()

    warehouse_db = args.warehouse_db
    trigger_db = args.trigger_db
    builder_script = ROOT / "build_analytics_warehouse.py"
    command = _build_builder_command(
        python_executable=sys.executable,
        builder_script=builder_script,
        trigger_db=trigger_db,
        warehouse_db=warehouse_db,
        builder_args=list(args.builder_args),
    )

    lock_token = ""
    try:
        state = warehouse_sync.acquire_sync_lock(
            actor=args.actor,
            reason=args.reason,
            warehouse_db=str(warehouse_db),
            trigger_db=str(trigger_db),
            metadata={
                "command": command,
            },
        )
        if state.get("status") == "skipped":
            payload = {
                "status": "skipped",
                "reason": args.reason,
                "skip_reason": state.get("skip_reason") or "lock_active",
                "warehouse_db": str(warehouse_db),
                "trigger_db": str(trigger_db),
                "command": command,
                "sync_state": state,
                "log_tail": [],
            }
            logger.info("shared warehouse sync skipped: %s", payload["skip_reason"])
            _emit_result(payload)
            return 0

        lock_token = str(state.get("lock_token") or "")
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(ROOT),
        )
        stdout_lines = [line for line in completed.stdout.splitlines() if line.strip()]
        stderr_lines = [line for line in completed.stderr.splitlines() if line.strip()]
        log_tail = (stdout_lines + stderr_lines)[-20:]
        embedded_result = _extract_embedded_result(stdout_lines)

        if completed.returncode != 0:
            error = stderr_lines[-1] if stderr_lines else completed.stdout.strip() or str(completed.returncode)
            final_state = warehouse_sync.finalize_sync_state(
                lock_token=lock_token,
                final_status="failed",
                actor=args.actor,
                reason=args.reason,
                warehouse_db=str(warehouse_db),
                trigger_db=str(trigger_db),
                error=error,
                metadata={
                    "command": command,
                    "returncode": completed.returncode,
                    "log_tail": log_tail,
                },
            )
            payload = {
                "status": "failed",
                "reason": args.reason,
                "warehouse_db": str(warehouse_db),
                "trigger_db": str(trigger_db),
                "command": command,
                "returncode": completed.returncode,
                "error": error,
                "sync_state": final_state,
                "log_tail": log_tail,
            }
            logger.error("shared warehouse sync failed: %s", error)
            _emit_result(payload)
            return 1

        if embedded_result:
            embedded_status = str(embedded_result.get("status") or "").strip().lower()
            embedded_skip_reason = str(embedded_result.get("skip_reason") or "").strip()
            if embedded_status == "skipped":
                final_state = warehouse_sync.finalize_sync_state(
                    lock_token=lock_token,
                    final_status="skipped",
                    actor=args.actor,
                    reason=args.reason,
                    warehouse_db=str(warehouse_db),
                    trigger_db=str(trigger_db),
                    skip_reason=embedded_skip_reason or "lock_active",
                    metadata={
                        "command": command,
                        "log_tail": log_tail,
                        "embedded_result": embedded_result,
                    },
                )
                payload = {
                    "status": "skipped",
                    "reason": args.reason,
                    "skip_reason": embedded_skip_reason or "lock_active",
                    "warehouse_db": str(warehouse_db),
                    "trigger_db": str(trigger_db),
                    "command": command,
                    "sync_state": final_state,
                    "log_tail": log_tail,
                }
                logger.info("shared warehouse sync skipped via embedded result: %s", payload["skip_reason"])
                _emit_result(payload)
                return 0
            if embedded_status == "failed":
                error = str(embedded_result.get("error") or "") or (
                    stderr_lines[-1] if stderr_lines else completed.stdout.strip() or "embedded_result_failed"
                )
                final_state = warehouse_sync.finalize_sync_state(
                    lock_token=lock_token,
                    final_status="failed",
                    actor=args.actor,
                    reason=args.reason,
                    warehouse_db=str(warehouse_db),
                    trigger_db=str(trigger_db),
                    error=error,
                    metadata={
                        "command": command,
                        "log_tail": log_tail,
                        "embedded_result": embedded_result,
                    },
                )
                payload = {
                    "status": "failed",
                    "reason": args.reason,
                    "warehouse_db": str(warehouse_db),
                    "trigger_db": str(trigger_db),
                    "command": command,
                    "error": error,
                    "sync_state": final_state,
                    "log_tail": log_tail,
                }
                logger.error("shared warehouse sync failed via embedded result: %s", error)
                _emit_result(payload)
                return 1

        final_state = warehouse_sync.finalize_sync_state(
            lock_token=lock_token,
            final_status="completed",
            actor=args.actor,
            reason=args.reason,
            warehouse_db=str(warehouse_db),
            trigger_db=str(trigger_db),
            metadata={
                "command": command,
                "log_tail": log_tail,
            },
        )
        payload = {
            "status": "completed",
            "reason": args.reason,
            "warehouse_db": str(warehouse_db),
            "trigger_db": str(trigger_db),
            "command": command,
            "sync_state": final_state,
            "log_tail": log_tail,
        }
        logger.info("shared warehouse sync completed: %s", warehouse_db)
        _emit_result(payload)
        return 0
    except Exception as exc:
        if lock_token:
            final_state = warehouse_sync.finalize_sync_state(
                lock_token=lock_token,
                final_status="failed",
                actor=args.actor,
                reason=args.reason,
                warehouse_db=str(warehouse_db),
                trigger_db=str(trigger_db),
                error=str(exc),
                metadata={
                    "command": command,
                    "exception_type": type(exc).__name__,
                    "log_tail": [str(exc)],
                },
            )
            payload = {
                "status": "failed",
                "reason": args.reason,
                "warehouse_db": str(warehouse_db),
                "trigger_db": str(trigger_db),
                "command": command,
                "error": str(exc),
                "sync_state": final_state,
                "log_tail": [str(exc)],
            }
            logger.exception("shared warehouse sync crashed")
            _emit_result(payload)
            return 1
        raise


if __name__ == "__main__":
    raise SystemExit(main())
