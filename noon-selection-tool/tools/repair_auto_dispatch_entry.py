from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.auto_dispatch import sync_canonical_auto_plans
from ops.task_store import OpsStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Repair canonical automatic crawl plans")
    parser.add_argument("--force-due", action="store_true", help="Set canonical auto plans due immediately after repair")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    store = OpsStore()
    try:
        summary = sync_canonical_auto_plans(store, force_due=bool(args.force_due))
    finally:
        store.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
