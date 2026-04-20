from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


DEFAULT_VAULT_ROOT = Path(r"C:\Users\Admin\Documents\Obsidian Vault\Noon")
DEFAULT_STRUCTURE_DIR = "系统架构知识库"
DEFAULT_METHOD_DIR = "可复用方法论"
DEFAULT_JSON_OUTPUT = Path("tmp") / "obsidian_context_snapshot.json"
DEFAULT_MAX_MILESTONES = 8


@dataclass
class Milestone:
    timestamp: str
    title: str
    theme: list[str]
    changes: list[str]
    validation: list[str]
    boundary: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync authoritative repo docs into the Noon Obsidian knowledge base."
    )
    parser.add_argument("--vault-root", default=str(DEFAULT_VAULT_ROOT))
    parser.add_argument("--structure-dir", default=DEFAULT_STRUCTURE_DIR)
    parser.add_argument("--method-dir", default=DEFAULT_METHOD_DIR)
    parser.add_argument("--json-output", default=str(DEFAULT_JSON_OUTPUT))
    parser.add_argument("--max-milestones", type=int, default=DEFAULT_MAX_MILESTONES)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def docs_root(root: Path) -> Path:
    return root / "noon-selection-tool" / "docs"


def source_docs(root: Path) -> dict[str, Path]:
    base = docs_root(root)
    return {
        "whitepaper": base / "PROJECT_WHITEPAPER.md",
        "handoff": base / "DEV_HANDOFF.md",
        "collab": base / "DEV_COLLAB_LOG.md",
        "codebase": base / "CODEBASE_MAP.md",
        "readme": base / "README.md",
    }


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str, dry_run: bool) -> None:
    if dry_run:
        print(f"[would_update] {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")
    print(f"[updated] {path}")


def section(text: str, level: int, title: str) -> str:
    heading = "#" * level
    pattern = re.compile(
        rf"^{re.escape(heading)}\s+{re.escape(title)}\s*$\n(?P<body>.*?)(?=^#{{1,{level}}}\s+|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(text)
    return match.group("body").strip() if match else ""


def bullets(text: str, limit: int | None = None) -> list[str]:
    items: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("- "):
            items.append(line[2:].strip())
        elif re.match(r"^\d+\.\s+", line):
            items.append(re.sub(r"^\d+\.\s+", "", line).strip())
    return items[:limit] if limit is not None else items


def slice_between(text: str, start_marker: str, end_marker: str | None = None) -> str:
    start = text.find(start_marker)
    if start == -1:
        return text
    sliced = text[start + len(start_marker) :]
    if end_marker:
        end = sliced.find(end_marker)
        if end != -1:
            sliced = sliced[:end]
    return sliced


def numbered_items_only(text: str, limit: int | None = None) -> list[str]:
    items: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = re.match(r"^(\d+)\.\s+(.+)$", line)
        if match:
            items.append(match.group(2).strip())
    return items[:limit] if limit is not None else items


def first_inline_code(text: str, default: str = "") -> str:
    match = re.search(r"`([^`]+)`", text)
    return match.group(1).strip() if match else default


def extract_latest_green_runs(*docs_texts: str) -> dict[str, str]:
    result = {"stabilization": "", "runtime_center": ""}
    for text in docs_texts:
        stab_match = re.search(r"web_beta_stabilization[/\\]([0-9T:\-]+Z)", text, re.IGNORECASE)
        runtime_match = re.search(r"web_beta_runtime_center[/\\]([0-9T:\-]+Z)", text, re.IGNORECASE)
        if not result["stabilization"] and stab_match:
            result["stabilization"] = stab_match.group(1)
        if not result["runtime_center"] and runtime_match:
            result["runtime_center"] = runtime_match.group(1)
    return result


def extract_current_release(*docs_texts: str) -> str:
    for text in docs_texts:
        match = re.search(r"`(huihaokang-nas-[^`]+)`", text)
        if match:
            return match.group(1)
    return ""


def parse_milestones(text: str, limit: int) -> list[Milestone]:
    pattern = re.compile(
        r"^##\s+(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+-\s+(?P<title>.+?)\s*$\n"
        r"(?P<body>.*?)(?=^##\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+-\s+|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    output: list[Milestone] = []
    for match in pattern.finditer(text):
        body = match.group("body").strip()
        output.append(
            Milestone(
                timestamp=match.group("timestamp"),
                title=match.group("title").strip(),
                theme=bullets(section(body, 3, "Theme"), limit=4),
                changes=bullets(section(body, 3, "What Changed"), limit=8),
                validation=bullets(section(body, 3, "Validation"), limit=6),
                boundary=bullets(section(body, 3, "Effect Boundary"), limit=4),
            )
        )
        if len(output) >= limit:
            break
    return output


def render_bullets(items: list[str], default: str = "- 暂无") -> str:
    if not items:
        return default
    return "\n".join(f"- {item}" for item in items)


def md_link(path: Path) -> str:
    return path.as_posix().replace(" ", "%20")


def build_snapshot(root: Path, max_milestones: int) -> dict[str, Any]:
    docs = source_docs(root)
    whitepaper = read_text(docs["whitepaper"])
    handoff = read_text(docs["handoff"])
    collab = read_text(docs["collab"])
    codebase = read_text(docs["codebase"])
    readme = read_text(docs["readme"])
    latest_runs = extract_latest_green_runs(whitepaper, handoff, codebase, readme)
    milestones = parse_milestones(collab, max_milestones)

    delivery_section = section(whitepaper, 2, "Default Delivery Model")
    goal_section = section(whitepaper, 2, "Goal-Closure Workflow")
    priority_section = section(handoff, 2, "Current Priority Stack")

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_phase": first_inline_code(section(whitepaper, 2, "Current Phase"), "unknown"),
        "operating_mode": first_inline_code(section(handoff, 2, "Current Operating Mode"), "unknown"),
        "north_star": first_inline_code(section(whitepaper, 2, "North Star"), "unknown"),
        "stable_architecture": first_inline_code(section(whitepaper, 2, "Stable Architecture"), "unknown"),
        "current_release": extract_current_release(whitepaper, handoff, readme),
        "priority_stack": numbered_items_only(priority_section, limit=8),
        "web_modules": bullets(section(codebase, 3, "Frontend Modules"), limit=12),
        "runtime_tools": bullets(section(codebase, 2, "Runtime Quality / Observability Tools"), limit=10),
        "latest_green_runs": latest_runs,
        "green_run_rule": bullets(section(whitepaper, 2, "Local Beta Gate"), limit=8),
        "delivery_model": bullets(
            slice_between(delivery_section, "Rules:", "Reason for this default:"),
            limit=8,
        ),
        "goal_closure": bullets(
            slice_between(goal_section, "Work stops only when:", "It does not stop only because:"),
            limit=8,
        ),
        "milestones": [milestone.__dict__ for milestone in milestones],
    }


def build_structure_docs(snapshot: dict[str, Any], root: Path) -> dict[str, str]:
    docs = source_docs(root)
    generated_at = snapshot["generated_at"]
    latest_runs = snapshot["latest_green_runs"]
    milestones = [Milestone(**item) for item in snapshot["milestones"]]
    current_release = snapshot["current_release"] or "unknown"

    files: dict[str, str] = {}
    files["00-Noon系统总览.md"] = f"""# Noon 系统总览

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`{generated_at}`

## 当前阶段
- `{snapshot["current_phase"]}`
- 运行模式：`{snapshot["operating_mode"]}`

## North Star
- `{snapshot["north_star"]}`
- 稳定架构：`{snapshot["stable_architecture"]}`

## 当前 NAS 稳定版
- `{current_release}`

## 当前优先级
{render_bullets(snapshot["priority_stack"])}

## 最新绿色基线
- stabilization：`{latest_runs["stabilization"] or "unknown"}`
- runtime-center：`{latest_runs["runtime_center"] or "unknown"}`

## 来源
- [PROJECT_WHITEPAPER.md]({md_link(docs["whitepaper"])})
- [DEV_HANDOFF.md]({md_link(docs["handoff"])})
- [CODEBASE_MAP.md]({md_link(docs["codebase"])})
"""

    files["01-仓库模块地图.md"] = f"""# 仓库模块地图

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`{generated_at}`

## Web 主链模块
{render_bullets(snapshot["web_modules"])}

## Runtime / Observability 工具
{render_bullets(snapshot["runtime_tools"])}

## 最新绿色基线规则
{render_bullets(snapshot["green_run_rule"])}

## 来源
- [CODEBASE_MAP.md]({md_link(docs["codebase"])})
- [README.md]({md_link(docs["readme"])})
"""

    milestone_blocks: list[str] = []
    for milestone in milestones:
        lines = [f"## {milestone.timestamp} - {milestone.title}", ""]
        if milestone.theme:
            lines.append("### 主题")
            lines.extend(f"- {item}" for item in milestone.theme)
            lines.append("")
        if milestone.changes:
            lines.append("### 变更")
            lines.extend(f"- {item}" for item in milestone.changes)
            lines.append("")
        if milestone.validation:
            lines.append("### 验证")
            lines.extend(f"- {item}" for item in milestone.validation)
            lines.append("")
        if milestone.boundary:
            lines.append("### 边界")
            lines.extend(f"- {item}" for item in milestone.boundary)
            lines.append("")
        milestone_blocks.append("\n".join(lines).strip())

    files["90-当前开发上下文.md"] = f"""# 当前开发上下文

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`{generated_at}`

## 当前阶段
- `{snapshot["current_phase"]}`

## 当前执行模式
{render_bullets(snapshot["delivery_model"])}

## Goal Closure 规则
{render_bullets(snapshot["goal_closure"])}

## 当前 NAS 稳定版
- `{current_release}`

## 最新绿色基线
- stabilization：`{latest_runs["stabilization"] or "unknown"}`
- runtime-center：`{latest_runs["runtime_center"] or "unknown"}`

## 来源
- [PROJECT_WHITEPAPER.md]({md_link(docs["whitepaper"])})
- [DEV_HANDOFF.md]({md_link(docs["handoff"])})
"""

    files["91-近期开发节点.md"] = """# 近期开发节点

<!-- generated: scripts/sync_obsidian_context.py -->

""" + "\n\n".join(milestone_blocks)

    return files


def build_method_docs(snapshot: dict[str, Any], root: Path) -> dict[str, str]:
    docs = source_docs(root)
    generated_at = snapshot["generated_at"]
    latest_runs = snapshot["latest_green_runs"]
    current_release = snapshot["current_release"] or "unknown"

    files: dict[str, str] = {}
    files["00-开发与交付工作流.md"] = f"""# 开发与交付工作流

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`{generated_at}`

## 默认模式
{render_bullets(snapshot["delivery_model"])}

## Goal Closure
{render_bullets(snapshot["goal_closure"])}

## 最新绿色基线规则
- repo 文档里的 latest green pointers 是唯一事实源
- Obsidian 只做镜像，不单独指定新的 green run
- 当前 stabilization：`{latest_runs["stabilization"] or "unknown"}`
- 当前 runtime-center：`{latest_runs["runtime_center"] or "unknown"}`

## 来源
- [PROJECT_WHITEPAPER.md]({md_link(docs["whitepaper"])})
- [DEV_HANDOFF.md]({md_link(docs["handoff"])})
"""

    files["01-NAS发布与运行治理.md"] = f"""# NAS 发布与运行治理

<!-- generated: scripts/sync_obsidian_context.py -->

更新时间：`{generated_at}`

## 当前阶段
- `{snapshot["current_phase"]}`

## 当前 NAS 稳定版
- `{current_release}`

## 运行治理关注点
{render_bullets(snapshot["priority_stack"])}

## 关键规则
- NAS 只接受显式 release bundle
- 现网保持 retained-data Postgres
- 发布后必须执行 runtime reconciliation
- 运行中心应优先展示 operator 可读状态，而不是原始任务字符串

## 来源
- [PROJECT_WHITEPAPER.md]({md_link(docs["whitepaper"])})
- [DEV_HANDOFF.md]({md_link(docs["handoff"])})
- [DEV_COLLAB_LOG.md]({md_link(docs["collab"])})
"""

    return files


def write_snapshot(json_output: Path, snapshot: dict[str, Any], dry_run: bool) -> None:
    if dry_run:
        print(f"[would_update] {json_output}")
        return
    json_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )
    print(f"[updated] {json_output}")


def main() -> None:
    args = parse_args()
    root = repo_root()
    snapshot = build_snapshot(root, args.max_milestones)

    vault_root = Path(args.vault_root)
    structure_root = vault_root / args.structure_dir
    method_root = vault_root / args.method_dir

    structure_docs = build_structure_docs(snapshot, root)
    method_docs = build_method_docs(snapshot, root)

    for filename, content in structure_docs.items():
        write_text(structure_root / filename, content, args.dry_run)
    for filename, content in method_docs.items():
        write_text(method_root / filename, content, args.dry_run)

    write_snapshot(Path(args.json_output), snapshot, args.dry_run)


if __name__ == "__main__":
    main()
