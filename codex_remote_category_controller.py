#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import tkinter as tk
import urllib.error
import urllib.request
import webbrowser
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk


PROJECT_ROOT = Path(__file__).parent
TOOL_DIR = PROJECT_ROOT / "noon-selection-tool"
MANAGE_PS1 = TOOL_DIR / "tools" / "manage_remote_category_runtime.ps1"
NAS_BASE_URL = "http://192.168.100.20:8865"
NAS_CRAWLER_URL = f"{NAS_BASE_URL}?view=crawler"
SINGLETON_PORT = 47643
AUTO_REFRESH_MS = 15000
DEFAULT_DEPTH = 1000
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0

CATEGORY_LABELS = {
    "automotive": "汽车用品",
    "baby": "母婴玩具",
    "beauty": "美妆个护",
    "electronics": "电子数码",
    "fashion": "时尚服饰",
    "garden": "园艺户外",
    "grocery": "食品杂货",
    "home_kitchen": "家居厨房",
    "office": "办公用品",
    "pets": "宠物用品",
    "sports": "运动户外",
    "tools": "工具家装",
}


def run_manage(action: str) -> dict:
    command = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(MANAGE_PS1),
        "-Action",
        action,
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=CREATE_NO_WINDOW,
    )
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout).strip() or f"action failed: {action}")
    payload = (completed.stdout or "").strip()
    if not payload:
        return {}
    return json.loads(payload)


def request_json(method: str, path: str, body: dict | None = None) -> dict:
    url = NAS_BASE_URL.rstrip("/") + path
    headers = {}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: {detail or exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc
    return json.loads(raw) if raw else {}


def safe_int(value: object, fallback: int) -> int:
    try:
        parsed = int(str(value).strip())
        return parsed if parsed > 0 else fallback
    except Exception:
        return fallback


def format_ts(value: str | None) -> str:
    if not value:
        return "-"
    try:
        return value.replace("T", " ")[:19]
    except Exception:
        return value


class RemoteCategoryController:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Noon Remote Category Controller")
        self.root.geometry("1180x840")
        self.root.minsize(1060, 760)

        self.busy = False
        self.refresh_inflight = False
        self.next_refresh_id: str | None = None
        self.catalog = []
        self.category_vars: dict[str, tk.BooleanVar] = {}

        self.status_var = tk.StringVar(value="整体状态：未刷新")
        self.runtime_hint_var = tk.StringVar(value="当前未加载运行时信息")
        self.chrome_var = tk.StringVar(value="-")
        self.tunnel_var = tk.StringVar(value="-")
        self.worker_var = tk.StringVar(value="-")
        self.nas_var = tk.StringVar(value="-")
        self.depth_var = tk.StringVar(value=str(DEFAULT_DEPTH))
        self.search_var = tk.StringVar(value="")
        self.selection_var = tk.StringVar(value="已选 0 个类目")
        self.progress_var = tk.StringVar(value="当前没有可见的类目任务")
        self.last_action_var = tk.StringVar(value="最近动作：无")

        self._singleton_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._singleton_socket.bind(("127.0.0.1", SINGLETON_PORT))
        except OSError:
            raise SystemExit(0)

        self._build()
        self._load_catalog()
        self._refresh_all(show_dialog=False)

    def _build(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        header = ttk.Frame(self.root, padding=16)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        ttk.Label(
            header,
            text="Noon Remote Category Controller",
            font=("Microsoft YaHei UI", 16, "bold"),
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            text="目标：本地 host Chrome + Docker category worker 长期运维，并通过 NAS 控制面稳定执行类目任务。",
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(header, textvariable=self.status_var).grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Label(header, textvariable=self.runtime_hint_var, foreground="#666").grid(row=3, column=0, sticky="w", pady=(4, 0))

        body = ttk.Frame(self.root, padding=(16, 0, 16, 16))
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(2, weight=1)

        health = ttk.LabelFrame(body, text="运行时状态", padding=12)
        health.grid(row=0, column=0, columnspan=2, sticky="ew")
        for idx in range(4):
            health.columnconfigure(idx, weight=1)
        self._health_item(health, 0, "Host Chrome CDP", self.chrome_var)
        self._health_item(health, 1, "DB Tunnel", self.tunnel_var)
        self._health_item(health, 2, "Category Worker", self.worker_var)
        self._health_item(health, 3, "NAS Heartbeat", self.nas_var)

        left = ttk.LabelFrame(body, text="类目扫描配置", padding=12)
        left.grid(row=1, column=0, sticky="nsew", padx=(0, 8), pady=(12, 0))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(3, weight=1)

        filter_row = ttk.Frame(left)
        filter_row.grid(row=0, column=0, sticky="ew")
        filter_row.columnconfigure(1, weight=1)
        ttk.Label(filter_row, text="筛选类目").grid(row=0, column=0, sticky="w")
        search_entry = ttk.Entry(filter_row, textvariable=self.search_var)
        search_entry.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        self.search_var.trace_add("write", lambda *_: self._render_category_list())

        depth_row = ttk.Frame(left)
        depth_row.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        depth_row.columnconfigure(3, weight=1)
        ttk.Label(depth_row, text="每个子类目深度").grid(row=0, column=0, sticky="w")
        ttk.Entry(depth_row, textvariable=self.depth_var, width=10).grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Label(depth_row, text="建议：100 / 300 / 500 / 1000").grid(row=0, column=2, sticky="w", padx=(12, 0))
        ttk.Label(depth_row, textvariable=self.selection_var, foreground="#666").grid(row=0, column=3, sticky="e")

        select_row = ttk.Frame(left)
        select_row.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        ttk.Button(select_row, text="选择 Pets / Sports", command=self._select_pets_sports).pack(side="left")
        ttk.Button(select_row, text="全选当前筛选结果", command=self._select_filtered).pack(side="left", padx=(8, 0))
        ttk.Button(select_row, text="清空选择", command=self._clear_selection).pack(side="left", padx=(8, 0))

        self.category_list_frame = ttk.Frame(left)
        self.category_list_frame.grid(row=3, column=0, sticky="nsew", pady=(12, 0))
        self.category_list_frame.columnconfigure(0, weight=1)

        right = ttk.LabelFrame(body, text="操作与说明", padding=12)
        right.grid(row=1, column=1, sticky="nsew", pady=(12, 0))
        right.columnconfigure(0, weight=1)

        ttk.Button(
            right,
            text="启动 / 修复本地类目爬虫",
            command=lambda: self._run_manage_action("ensure", "本地类目爬虫已恢复"),
        ).grid(row=0, column=0, sticky="ew")
        ttk.Button(
            right,
            text="安装自动自愈任务",
            command=lambda: self._run_manage_action("install-autostart", "自动自愈任务已安装"),
        ).grid(row=1, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(
            right,
            text="启动所选类目爬取",
            command=self._launch_selected_scan,
        ).grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(
            right,
            text="打开 NAS 爬虫控制台",
            command=lambda: webbrowser.open(NAS_CRAWLER_URL),
        ).grid(row=3, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(right, text="刷新状态", command=self._refresh_all).grid(row=4, column=0, sticky="ew", pady=(8, 0))

        note_text = (
            "业务逻辑：\n"
            "1. 先筛选并勾选类目，再设置统一深度发起任务。\n"
            "2. 若当前已有类目任务在跑，新任务会进入队列，不会中断现有运行。\n"
            "3. 底部运行状态区会持续显示当前 task、阶段、已持久化子类目和 observations。\n"
            "4. 当前作用范围只限本地 remote category node，不影响 NAS keyword 链。"
        )
        ttk.Label(right, text=note_text, justify="left").grid(row=5, column=0, sticky="nw", pady=(12, 0))

        status_frame = ttk.LabelFrame(body, text="运行状态与进度", padding=12)
        status_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(1, weight=1)
        status_frame.rowconfigure(3, weight=1)

        ttk.Label(status_frame, textvariable=self.progress_var, font=("Microsoft YaHei UI", 10, "bold")).grid(
            row=0, column=0, sticky="w"
        )

        columns = (
            "task_id",
            "categories",
            "status",
            "stage",
            "subcategories",
            "observations",
            "updated_at",
            "message",
        )
        self.task_tree = ttk.Treeview(status_frame, columns=columns, show="headings", height=10)
        headings = {
            "task_id": "Task",
            "categories": "类目",
            "status": "状态",
            "stage": "阶段",
            "subcategories": "子类目",
            "observations": "商品数",
            "updated_at": "更新时间",
            "message": "进度消息",
        }
        widths = {
            "task_id": 80,
            "categories": 150,
            "status": 90,
            "stage": 130,
            "subcategories": 80,
            "observations": 90,
            "updated_at": 150,
            "message": 360,
        }
        for column in columns:
            self.task_tree.heading(column, text=headings[column])
            self.task_tree.column(column, width=widths[column], anchor="w")
        self.task_tree.grid(row=1, column=0, sticky="nsew", pady=(8, 0))

        tree_scroll = ttk.Scrollbar(status_frame, orient="vertical", command=self.task_tree.yview)
        self.task_tree.configure(yscrollcommand=tree_scroll.set)
        tree_scroll.grid(row=1, column=1, sticky="ns", pady=(8, 0))

        ttk.Label(status_frame, textvariable=self.last_action_var, foreground="#666").grid(row=2, column=0, sticky="w", pady=(10, 0))
        self.output = tk.Text(status_frame, height=8, wrap="word")
        self.output.grid(row=3, column=0, sticky="nsew", pady=(8, 0))

    def _health_item(self, parent: ttk.Widget, column: int, label: str, value_var: tk.StringVar) -> None:
        box = ttk.Frame(parent)
        box.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 8, 0))
        ttk.Label(box, text=label).grid(row=0, column=0, sticky="w")
        ttk.Label(box, textvariable=value_var, font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=1, column=0, sticky="w", pady=(6, 0)
        )

    def _append_output(self, text: str) -> None:
        self.output.insert("end", text.rstrip() + "\n")
        self.output.see("end")

    def _set_busy(self, busy: bool) -> None:
        self.busy = busy
        self.root.config(cursor="watch" if busy else "")

    def _update_selection_label(self) -> None:
        selected = sum(1 for var in self.category_vars.values() if var.get())
        self.selection_var.set(f"已选 {selected} 个类目")

    def _render_category_list(self) -> None:
        for child in self.category_list_frame.winfo_children():
            child.destroy()

        query = self.search_var.get().strip().lower()
        filtered = []
        for item in self.catalog:
            haystack = " ".join(
                [
                    item["id"],
                    item["label_cn"],
                    item["label_en"],
                ]
            ).lower()
            if query and query not in haystack:
                continue
            filtered.append(item)

        if not filtered:
            ttk.Label(self.category_list_frame, text="没有匹配的类目").grid(row=0, column=0, sticky="w")
            self._update_selection_label()
            return

        for idx, item in enumerate(filtered):
            category_id = item["id"]
            var = self.category_vars.setdefault(category_id, tk.BooleanVar(value=category_id in {"pets", "sports"} and not self.catalog))
            label = f"{item['label_cn']} / {item['label_en']} ({item['subcategory_count']} 子类目)"
            check = ttk.Checkbutton(
                self.category_list_frame,
                text=label,
                variable=var,
                command=self._update_selection_label,
            )
            check.grid(row=idx, column=0, sticky="w", pady=2)

        self._update_selection_label()

    def _load_catalog(self) -> None:
        def worker() -> None:
            try:
                payload = request_json("GET", "/api/crawler/catalog")
                ready_categories = payload.get("ready_categories") or []
                subcategory_catalog = payload.get("subcategory_catalog") or []
                counts: dict[str, int] = {}
                for item in subcategory_catalog:
                    top_level = item.get("top_level_category") or ""
                    counts[top_level] = counts.get(top_level, 0) + 1
                catalog = []
                for category_id in ready_categories:
                    label_cn = CATEGORY_LABELS.get(category_id, category_id)
                    label_en = category_id.replace("_", " ")
                    catalog.append(
                        {
                            "id": category_id,
                            "label_cn": label_cn,
                            "label_en": label_en,
                            "subcategory_count": counts.get(category_id, 0),
                        }
                    )
                self.root.after(0, lambda: self._apply_catalog(catalog))
            except Exception as exc:
                self.root.after(0, lambda: self._append_output(f"[ERROR] 加载类目目录失败: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_catalog(self, catalog: list[dict]) -> None:
        previous_selection = {key for key, var in self.category_vars.items() if var.get()}
        self.catalog = catalog
        for item in self.catalog:
            category_id = item["id"]
            default_selected = category_id in {"pets", "sports"} if not previous_selection else category_id in previous_selection
            self.category_vars[category_id] = tk.BooleanVar(value=default_selected)
        self._render_category_list()

    def _select_pets_sports(self) -> None:
        for category_id, var in self.category_vars.items():
            var.set(category_id in {"pets", "sports"})
        self._update_selection_label()

    def _select_filtered(self) -> None:
        query = self.search_var.get().strip().lower()
        for item in self.catalog:
            haystack = " ".join([item["id"], item["label_cn"], item["label_en"]]).lower()
            if not query or query in haystack:
                self.category_vars[item["id"]].set(True)
        self._update_selection_label()

    def _clear_selection(self) -> None:
        for var in self.category_vars.values():
            var.set(False)
        self._update_selection_label()

    def _refresh_all(self, show_dialog: bool = False) -> None:
        if self.refresh_inflight:
            return
        self.refresh_inflight = True

        def worker() -> None:
            try:
                runtime = run_manage("status")
                tasks = request_json("GET", "/api/tasks?worker_type=category&limit=12")
                self.root.after(0, lambda: self._apply_runtime_status(runtime, tasks))
                if show_dialog:
                    self.root.after(0, lambda: messagebox.showinfo("完成", "状态已刷新"))
            except Exception as exc:
                self.root.after(0, lambda: self._append_output(f"[ERROR] 刷新状态失败: {exc}"))
            finally:
                self.refresh_inflight = False
                self.root.after(0, self._schedule_refresh)

        threading.Thread(target=worker, daemon=True).start()

    def _schedule_refresh(self) -> None:
        if self.next_refresh_id:
            try:
                self.root.after_cancel(self.next_refresh_id)
            except Exception:
                pass
        self.next_refresh_id = self.root.after(AUTO_REFRESH_MS, self._refresh_all)

    def _run_manage_action(self, action: str, success_message: str) -> None:
        if self.busy:
            return

        def worker() -> None:
            self.root.after(0, lambda: self._set_busy(True))
            try:
                payload = run_manage(action)
                self.root.after(0, lambda: self._append_output(json.dumps(payload, ensure_ascii=False, indent=2)))
                self.root.after(0, lambda: self.last_action_var.set(f"最近动作：{success_message}"))
                self.root.after(0, lambda: self._refresh_all(show_dialog=False))
                self.root.after(0, lambda: messagebox.showinfo("完成", success_message))
            except Exception as exc:
                self.root.after(0, lambda: self._append_output(f"[ERROR] {exc}"))
                self.root.after(0, lambda: messagebox.showerror("失败", str(exc)))
            finally:
                self.root.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _launch_selected_scan(self) -> None:
        if self.busy:
            return

        selected_categories = sorted([category_id for category_id, var in self.category_vars.items() if var.get()])
        if not selected_categories:
            messagebox.showwarning("缺少类目", "先选择至少一个类目。")
            return

        depth = safe_int(self.depth_var.get(), DEFAULT_DEPTH)
        self.depth_var.set(str(depth))
        reason = f"desktop controller selected categories x{depth}"

        def worker() -> None:
            self.root.after(0, lambda: self._set_busy(True))
            try:
                ensure_payload = run_manage("ensure")
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                slug = "-".join(selected_categories[:3])
                if len(selected_categories) > 3:
                    slug += "-more"
                plan_name = f"remote-category-manual-{timestamp}-{slug}-x{depth}"
                created = request_json(
                    "POST",
                    "/api/crawler/plans",
                    {
                        "plan_type": "category_ready_scan",
                        "name": plan_name,
                        "created_by": "desktop_controller",
                        "enabled": False,
                        "schedule_kind": "manual",
                        "schedule_json": {},
                        "payload": {
                            "categories": selected_categories,
                            "category_overrides": {},
                            "subcategory_overrides": {},
                            "default_product_count_per_leaf": depth,
                            "export_excel": False,
                            "persist": True,
                            "reason": reason,
                        },
                    },
                )
                task = request_json("POST", f"/api/crawler/plans/{created['id']}/launch")
                self.root.after(0, lambda: self._append_output(json.dumps(ensure_payload, ensure_ascii=False, indent=2)))
                self.root.after(0, lambda: self._append_output(json.dumps(task, ensure_ascii=False, indent=2)))
                self.root.after(
                    0,
                    lambda: self.last_action_var.set(
                        f"最近动作：已启动 task {task.get('id')}，类目 {', '.join(selected_categories)}，深度 {depth}"
                    ),
                )
                self.root.after(0, lambda: self._refresh_all(show_dialog=False))
                self.root.after(
                    0,
                    lambda: messagebox.showinfo(
                        "已启动",
                        f"类目任务已创建。\nTask ID: {task.get('id')}\n类目: {', '.join(selected_categories)}\n深度: {depth}",
                    ),
                )
            except Exception as exc:
                self.root.after(0, lambda: self._append_output(f"[ERROR] {exc}"))
                self.root.after(0, lambda: messagebox.showerror("失败", str(exc)))
            finally:
                self.root.after(0, lambda: self._set_busy(False))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_runtime_status(self, runtime: dict, tasks_payload: dict) -> None:
        self.chrome_var.set("正常" if runtime.get("chrome_ready") else "异常")
        self.tunnel_var.set(runtime.get("tunnel_status") or "-")
        self.worker_var.set(runtime.get("worker_status") or "-")
        if runtime.get("remote_worker_heartbeat_present"):
            self.nas_var.set(runtime.get("remote_worker_status") or "present")
        else:
            self.nas_var.set("missing")
        self.status_var.set("整体状态：{0}".format("可用" if runtime.get("stack_ready") else "待修复"))
        remote_name = runtime.get("remote_worker_name") or "-"
        remote_host = runtime.get("remote_worker_host") or "-"
        self.runtime_hint_var.set(f"Remote worker: {remote_name} | host: {remote_host} | NAS: {runtime.get('nas_base_url')}")

        items = tasks_payload.get("items") or []
        for row_id in self.task_tree.get_children():
            self.task_tree.delete(row_id)

        active_task = None
        for item in items:
            progress = item.get("progress") or {}
            metrics = progress.get("metrics") or {}
            payload = item.get("payload") or {}
            categories = payload.get("categories") or ([payload.get("category")] if payload.get("category") else [])
            category_text = ",".join(categories) if categories else "-"
            row = (
                item.get("id"),
                category_text,
                item.get("status") or "-",
                progress.get("stage") or "-",
                metrics.get("persisted_subcategories", 0),
                metrics.get("persisted_observations", 0),
                format_ts(item.get("updated_at")),
                progress.get("message") or "",
            )
            self.task_tree.insert("", "end", values=row)
            if not active_task and item.get("status") in {"running", "pending"}:
                active_task = item

        if active_task:
            progress = active_task.get("progress") or {}
            metrics = progress.get("metrics") or {}
            details = progress.get("details") or {}
            task_id = active_task.get("id")
            stage = progress.get("stage") or active_task.get("status")
            message = progress.get("message") or "-"
            category_name = details.get("current_category") or ",".join((active_task.get("payload") or {}).get("categories") or [])
            current_subcategory = details.get("current_subcategory") or "-"
            self.progress_var.set(
                f"当前运行：task {task_id} | {category_name} | {stage} | {current_subcategory} | "
                f"{metrics.get('persisted_subcategories', 0)} 子类目 / {metrics.get('persisted_observations', 0)} 商品 | {message}"
            )
        else:
            self.progress_var.set("当前没有运行中的类目任务。新任务会直接进入执行或进入队列。")


def main() -> int:
    root = tk.Tk()
    controller = RemoteCategoryController(root)
    root.protocol("WM_DELETE_WINDOW", root.destroy)
    root.mainloop()
    controller._singleton_socket.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
