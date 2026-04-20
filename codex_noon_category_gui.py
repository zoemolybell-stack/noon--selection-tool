#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""User-facing launcher for codex noon爬虫-类目."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk


PROJECT_ROOT = Path(__file__).parent
TOOL_DIR = PROJECT_ROOT / "noon-selection-tool"
MAIN_PY = TOOL_DIR / "main.py"
READY_SCAN_PY = TOOL_DIR / "run_ready_category_scan.py"
SYNC_PY = TOOL_DIR / "run_shared_warehouse_sync.py"
VENV_PYTHON = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"
VENV_PYTHONW = PROJECT_ROOT / "venv" / "Scripts" / "pythonw.exe"
CATEGORY_TREE_PATH = TOOL_DIR / "config" / "category_tree.json"
READINESS_REPORT_DIR = TOOL_DIR / "data" / "reports"
DEFAULT_BATCH_DIR = TOOL_DIR / "data" / "batch_scans"
DEFAULT_STAGE_DB = TOOL_DIR / "data" / "product_store.db"
DEFAULT_ANALYTICS_DIR = TOOL_DIR / "data" / "analytics"
DEFAULT_SYNC_STATE = DEFAULT_ANALYTICS_DIR / "warehouse_sync_status.json"
DEFAULT_SYNC_LOCK = DEFAULT_ANALYTICS_DIR / "warehouse_sync.lock"
SYNC_RESULT_PREFIX = "WAREHOUSE_SYNC_RESULT="


def load_category_ids() -> list[str]:
    if not CATEGORY_TREE_PATH.exists():
        return []
    try:
        payload = json.loads(CATEGORY_TREE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    categories = payload.get("categories", []) if isinstance(payload, dict) else []
    results = []
    for item in categories:
        if not isinstance(item, dict):
            continue
        category_id = str(item.get("id") or "").strip()
        if category_id:
            results.append(category_id)
    return results


def find_latest_readiness_report() -> Path | None:
    if not READINESS_REPORT_DIR.exists():
        return None
    reports = sorted(
        READINESS_REPORT_DIR.glob("scan_readiness_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return reports[0] if reports else None


def load_ready_category_count(report_path: Path | None) -> int:
    if not report_path or not report_path.exists():
        return 0
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    bucket = payload.get("status_buckets", {}).get("ready_for_scan", [])
    return len(bucket) if isinstance(bucket, list) else 0


def get_python_executable() -> Path:
    if VENV_PYTHON.exists():
        return VENV_PYTHON
    return Path(sys.executable)


def is_path_writable(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path, prefix="codex_probe_", delete=True):
        return True


def list_active_category_processes() -> list[dict[str, str]]:
    ps_command = r"""
$ErrorActionPreference = 'SilentlyContinue'
$items = Get-CimInstance Win32_Process |
  Where-Object {
    $_.CommandLine -and (
      $_.CommandLine -match 'main\.py.+--step\s+category' -or
      $_.CommandLine -match 'run_ready_category_scan\.py'
    )
  } |
  Select-Object ProcessId, Name, CommandLine
if ($items) {
  $items | ConvertTo-Json -Compress
}
"""
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    content = completed.stdout.strip()
    if not content:
        return []
    try:
        payload = json.loads(content)
    except Exception:
        return []
    if isinstance(payload, dict):
        payload = [payload]
    results = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        results.append(
            {
                "pid": str(item.get("ProcessId", "")),
                "name": str(item.get("Name", "")),
                "command": str(item.get("CommandLine", "")),
            }
        )
    return results


def list_active_keyword_processes() -> list[dict[str, str]]:
    ps_command = r"""
$ErrorActionPreference = 'SilentlyContinue'
$items = Get-CimInstance Win32_Process |
  Where-Object {
    $_.CommandLine -and (
      $_.CommandLine -match 'run_keyword_monitor\.py' -or
      $_.CommandLine -match 'keyword_main\.py'
    )
  } |
  Select-Object ProcessId, Name, CommandLine
if ($items) {
  $items | ConvertTo-Json -Compress
}
"""
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    content = completed.stdout.strip()
    if not content:
        return []
    try:
        payload = json.loads(content)
    except Exception:
        return []
    if isinstance(payload, dict):
        payload = [payload]
    results = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        results.append(
            {
                "pid": str(item.get("ProcessId", "")),
                "name": str(item.get("Name", "")),
                "command": str(item.get("CommandLine", "")),
            }
        )
    return results


def load_sync_lock_payload() -> dict:
    if not DEFAULT_SYNC_LOCK.exists():
        return {}
    try:
        payload = json.loads(DEFAULT_SYNC_LOCK.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


class CategoryCrawlerLauncher:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("codex noon爬虫-类目")
        self.root.geometry("1040x760")

        self.categories = load_category_ids()
        self.current_process: subprocess.Popen[str] | None = None
        self.is_running = False
        self.stop_requested = False
        self.latest_output_dir: Path | None = None
        self.latest_readiness_report = find_latest_readiness_report()

        self.mode_var = tk.StringVar(value="global")
        self.category_var = tk.StringVar(value=self.categories[0] if self.categories else "")
        self.product_count_var = tk.StringVar(value="50")
        self.max_depth_var = tk.StringVar(value="3")
        self.persist_var = tk.BooleanVar(value=True)
        self.export_excel_var = tk.BooleanVar(value=False)
        self.status_var = tk.StringVar(value="就绪")
        self.readiness_var = tk.StringVar(value=self._build_readiness_label())

        self._build_ui()
        self._refresh_mode_state()

    def _build_readiness_label(self) -> str:
        report = self.latest_readiness_report
        if not report:
            return "Readiness 报告: 未找到"
        return f"Readiness 报告: {report.name} | ready_for_scan={load_ready_category_count(report)}"

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        main = ttk.Frame(self.root, padding=12)
        main.grid(row=0, column=0, sticky="nsew")
        main.columnconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(2, weight=1)

        header = ttk.Frame(main)
        header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="codex noon爬虫-类目", font=("Microsoft YaHei UI", 16, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self.readiness_var).grid(row=1, column=0, sticky="w", pady=(6, 0))

        left = ttk.LabelFrame(main, text="任务配置", padding=12)
        left.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        left.columnconfigure(1, weight=1)

        ttk.Label(left, text="模式 / Mode").grid(row=0, column=0, sticky="w")
        self.mode_combo = ttk.Combobox(
            left,
            textvariable=self.mode_var,
            state="readonly",
            values=["global", "single"],
            width=22,
        )
        self.mode_combo.grid(row=0, column=1, sticky="ew", pady=(0, 8))
        self.mode_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_mode_state())

        ttk.Label(left, text="单类目 / Category").grid(row=1, column=0, sticky="w")
        self.category_combo = ttk.Combobox(
            left,
            textvariable=self.category_var,
            state="readonly",
            values=self.categories,
            width=24,
        )
        self.category_combo.grid(row=1, column=1, sticky="ew", pady=(0, 8))

        ttk.Label(left, text="每叶子数量 / Products per leaf").grid(row=2, column=0, sticky="w")
        ttk.Entry(left, textvariable=self.product_count_var, width=12).grid(row=2, column=1, sticky="w", pady=(0, 8))

        ttk.Label(left, text="最大深度 / Max depth").grid(row=3, column=0, sticky="w")
        self.max_depth_entry = ttk.Entry(left, textvariable=self.max_depth_var, width=12)
        self.max_depth_entry.grid(row=3, column=1, sticky="w", pady=(0, 8))

        ttk.Checkbutton(left, text="Persist 到统一 stage / Persist", variable=self.persist_var).grid(row=4, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(left, text="导出 Excel / Export Excel", variable=self.export_excel_var).grid(row=5, column=0, columnspan=2, sticky="w", pady=(4, 0))

        hint = (
            "global: run_ready_category_scan.py --product-count 50 --persist\n"
            "single: main.py --step category --persist"
        )
        ttk.Label(left, text=hint, foreground="#666666").grid(row=6, column=0, columnspan=2, sticky="w", pady=(10, 0))

        right = ttk.LabelFrame(main, text="预检与状态", padding=12)
        right.grid(row=1, column=1, sticky="nsew", padx=(8, 0))
        right.columnconfigure(0, weight=1)

        self.preflight_text = tk.Text(right, height=11, width=55, state="disabled")
        self.preflight_text.grid(row=0, column=0, sticky="nsew")
        right.rowconfigure(0, weight=1)

        btns = ttk.Frame(right)
        btns.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        ttk.Button(btns, text="检查环境", command=self.run_preflight).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="打开 Web", command=self.open_web).pack(side="left", padx=(0, 6))
        ttk.Button(btns, text="打开输出目录", command=self.open_output_dir).pack(side="left")

        log_frame = ttk.LabelFrame(main, text="运行日志", padding=12)
        log_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, state="disabled", height=22)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

        controls = ttk.Frame(main)
        controls.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        self.start_button = ttk.Button(controls, text="开始运行", command=self.start_run)
        self.start_button.pack(side="left")
        self.stop_button = ttk.Button(controls, text="停止", command=self.stop_run, state="disabled")
        self.stop_button.pack(side="left", padx=(8, 0))

        status_bar = ttk.Label(main, textvariable=self.status_var, relief="sunken", anchor="w")
        status_bar.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(10, 0))

    def _set_log_text(self, text: str) -> None:
        self.preflight_text.configure(state="normal")
        self.preflight_text.delete("1.0", "end")
        self.preflight_text.insert("1.0", text)
        self.preflight_text.configure(state="disabled")

    def log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _refresh_mode_state(self) -> None:
        mode = self.mode_var.get()
        if mode == "global":
            self.category_combo.configure(state="disabled")
            self.max_depth_entry.configure(state="disabled")
        else:
            self.category_combo.configure(state="readonly")
            self.max_depth_entry.configure(state="normal")

    def run_preflight(self) -> bool:
        errors: list[str] = []
        warnings: list[str] = []
        info: list[str] = []

        python_path = get_python_executable()
        if not python_path.exists():
            errors.append(f"Python runtime 不存在: {python_path}")
        else:
            info.append(f"Python: {python_path}")

        for required in (MAIN_PY, READY_SCAN_PY, SYNC_PY):
            if not required.exists():
                errors.append(f"缺少文件: {required}")
            else:
                info.append(f"OK: {required.name}")

        if self.mode_var.get() == "global":
            report = self.latest_readiness_report or find_latest_readiness_report()
            self.latest_readiness_report = report
            self.readiness_var.set(self._build_readiness_label())
            if not report:
                errors.append("未找到最新 readiness 报告，无法执行全局校对扫描")
            else:
                ready_count = load_ready_category_count(report)
                info.append(f"Readiness: {report.name} | ready_for_scan={ready_count}")
                if ready_count <= 0:
                    errors.append("readiness 报告中没有 ready_for_scan 类目")

        try:
            is_path_writable(DEFAULT_STAGE_DB.parent)
            info.append(f"可写: {DEFAULT_STAGE_DB.parent}")
        except Exception as exc:
            errors.append(f"stage 目录不可写: {DEFAULT_STAGE_DB.parent} ({exc})")

        try:
            is_path_writable(DEFAULT_ANALYTICS_DIR)
            info.append(f"可写: {DEFAULT_ANALYTICS_DIR}")
        except Exception as exc:
            errors.append(f"analytics 目录不可写: {DEFAULT_ANALYTICS_DIR} ({exc})")

        active_processes = list_active_category_processes()
        if active_processes:
            errors.append(f"发现活跃 category 任务: {len(active_processes)} 个")
            for item in active_processes[:3]:
                info.append(f"ACTIVE PID {item['pid']}: {item['command']}")

        active_keyword_processes = list_active_keyword_processes()
        if active_keyword_processes:
            errors.append(
                f"发现活跃 keyword 任务: {len(active_keyword_processes)} 个；当前不允许与类目全局扫描并行运行"
            )
            for item in active_keyword_processes[:3]:
                info.append(f"KEYWORD PID {item['pid']}: {item['command']}")

        lock_payload = load_sync_lock_payload()
        if lock_payload:
            warnings.append(
                "shared sync 锁当前存在；如果运行结束时仍未释放，warehouse sync 可能会 skipped"
            )
            info.append(f"SYNC LOCK actor={lock_payload.get('actor', '')} reason={lock_payload.get('reason', '')}")

        lines = ["[Preflight]"]
        if errors:
            lines.append("Errors:")
            lines.extend(f"- {item}" for item in errors)
        if warnings:
            lines.append("Warnings:")
            lines.extend(f"- {item}" for item in warnings)
        if info:
            lines.append("Info:")
            lines.extend(f"- {item}" for item in info)

        self._set_log_text("\n".join(lines))
        if errors:
            self.status_var.set("环境检查失败")
            return False
        self.status_var.set("环境检查通过")
        return True

    def open_web(self) -> None:
        subprocess.Popen(["powershell", "-NoProfile", "-Command", "Start-Process", "http://127.0.0.1:8865/"])

    def open_output_dir(self) -> None:
        path = self.latest_output_dir or DEFAULT_BATCH_DIR
        path.mkdir(parents=True, exist_ok=True)
        os.startfile(str(path))

    def _build_scan_command(self) -> tuple[list[str], Path | None, str]:
        python_path = str(get_python_executable())
        product_count = int(self.product_count_var.get().strip())
        mode = self.mode_var.get()

        if mode == "global":
            output_dir = DEFAULT_BATCH_DIR / f"gui_rescan_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            cmd = [
                python_path,
                str(READY_SCAN_PY),
                "--product-count",
                str(product_count),
                "--output-dir",
                str(output_dir),
            ]
            if self.persist_var.get():
                cmd.append("--persist")
            if not self.export_excel_var.get():
                cmd.append("--no-export-excel")
            return cmd, output_dir, f"Global Rescan -> {output_dir}"

        category_id = self.category_var.get().strip()
        if not category_id:
            raise ValueError("单类目模式必须选择 category")
        max_depth = int(self.max_depth_var.get().strip())
        cmd = [
            python_path,
            str(MAIN_PY),
            "--step",
            "category",
            "--category",
            category_id,
            "--noon-count",
            str(product_count),
            "--max-depth",
            str(max_depth),
            "--verbose",
        ]
        if self.persist_var.get():
            cmd.append("--persist")
        if self.export_excel_var.get():
            cmd.append("--export-excel")
        return cmd, None, "Single Category -> auto snapshot"

    def start_run(self) -> None:
        if self.is_running:
            return
        try:
            int(self.product_count_var.get().strip())
            int(self.max_depth_var.get().strip())
        except ValueError:
            messagebox.showerror("错误", "数量和深度必须是整数")
            return

        if not self.run_preflight():
            messagebox.showerror("错误", "环境检查未通过，请先修复预检错误")
            return

        self.stop_requested = False
        self.is_running = True
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="normal")
        self.status_var.set("运行中")
        self.log("=" * 72)
        self.log("开始运行类目任务")
        threading.Thread(target=self._run_worker, daemon=True).start()

    def stop_run(self) -> None:
        self.stop_requested = True
        if self.current_process and self.current_process.poll() is None:
            subprocess.run(
                ["taskkill", "/PID", str(self.current_process.pid), "/T", "/F"],
                capture_output=True,
                text=True,
            )
            self.log("已请求停止当前任务")
        self.status_var.set("停止中")

    def _run_subprocess(self, cmd: list[str]) -> tuple[int, list[str]]:
        self.current_process = subprocess.Popen(
            cmd,
            cwd=str(TOOL_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        lines: list[str] = []
        assert self.current_process.stdout is not None
        for raw_line in self.current_process.stdout:
            line = raw_line.rstrip()
            lines.append(line)
            self.log(line)
            if self.stop_requested:
                break
        return_code = self.current_process.wait()
        self.current_process = None
        return return_code, lines

    def _run_worker(self) -> None:
        try:
            cmd, output_dir, output_hint = self._build_scan_command()
            self.latest_output_dir = output_dir

            self.log(f"命令: {' '.join(cmd)}")
            self.log(f"输出: {output_hint}")
            self.log(f"Persist: {'on' if self.persist_var.get() else 'off'}")
            self.log(f"Export Excel: {'on' if self.export_excel_var.get() else 'off'}")

            scan_code, scan_lines = self._run_subprocess(cmd)
            if self.stop_requested:
                self.status_var.set("已停止")
                return

            if scan_code != 0:
                self.status_var.set("scan failed")
                self.log("scan failed")
                return

            self.log("scan completed")
            if self.persist_var.get():
                self.log("persist completed")

            if self.mode_var.get() == "global" and self.persist_var.get():
                sync_cmd = [
                    str(get_python_executable()),
                    str(SYNC_PY),
                    "--actor",
                    "category_desktop",
                    "--reason",
                    "category_gui_global_rescan",
                    "--trigger-db",
                    str(DEFAULT_STAGE_DB),
                ]
                self.log("-" * 72)
                self.log(f"shared sync: {' '.join(sync_cmd)}")
                sync_code, sync_lines = self._run_subprocess(sync_cmd)
                payload = self._parse_sync_result(sync_lines)
                if sync_code == 0 and payload.get("status") == "completed":
                    self.status_var.set("warehouse sync completed")
                    self.log("warehouse sync completed")
                elif sync_code == 0 and payload.get("status") == "skipped":
                    self.status_var.set("warehouse sync skipped")
                    self.log(f"warehouse sync skipped: {payload.get('skip_reason', '')}")
                else:
                    self.status_var.set("warehouse sync failed")
                    self.log(f"warehouse sync failed: {payload.get('error', '')}")
                    return
            else:
                self.status_var.set("completed")
                if self.persist_var.get() and DEFAULT_SYNC_STATE.exists():
                    self._log_latest_sync_state()

            self.log("=" * 72)
            self.log("任务结束")
        except Exception as exc:
            self.status_var.set("执行失败")
            self.log(f"执行失败: {exc}")
        finally:
            self.is_running = False
            self.stop_requested = False
            self.start_button.configure(state="normal")
            self.stop_button.configure(state="disabled")

    def _parse_sync_result(self, lines: list[str]) -> dict:
        for line in reversed(lines):
            if not line.startswith(SYNC_RESULT_PREFIX):
                continue
            try:
                return json.loads(line[len(SYNC_RESULT_PREFIX):])
            except Exception:
                break
        return {}

    def _log_latest_sync_state(self) -> None:
        try:
            payload = json.loads(DEFAULT_SYNC_STATE.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        self.log(
            "shared sync state: "
            f"status={payload.get('status', '')} actor={payload.get('actor', '')} reason={payload.get('reason', '')}"
        )


def main() -> None:
    root = tk.Tk()
    CategoryCrawlerLauncher(root)
    root.mainloop()


if __name__ == "__main__":
    main()
