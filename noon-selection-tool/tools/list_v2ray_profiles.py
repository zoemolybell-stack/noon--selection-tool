from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from render_v2ray_slot_xray_config import resolve_default_v2rayn_db_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List reusable v2rayN profiles for slot selection.")
    parser.add_argument("--db", type=Path, default=resolve_default_v2rayn_db_path())
    parser.add_argument("--filter", type=str, default="")
    parser.add_argument("--limit", type=int, default=50)
    return parser.parse_args()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    args = parse_args()
    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT IndexId, Remarks, Address, Port, ConfigType, Network, StreamSecurity
            FROM ProfileItem
            ORDER BY Remarks ASC
            """
        ).fetchall()
    finally:
        conn.close()

    filter_text = str(args.filter or "").strip().lower()
    items: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        remarks = str(item.get("Remarks") or "")
        address = str(item.get("Address") or "")
        if filter_text and filter_text not in remarks.lower() and filter_text not in address.lower():
            continue
        items.append(item)
        if len(items) >= max(1, int(args.limit)):
            break

    print(json.dumps(items, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
