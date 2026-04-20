from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from build_analytics_warehouse import (
    DEFAULT_KEYWORD_DB,
    DEFAULT_REPORTS_DIR,
    DEFAULT_RUNTIME_DIR,
    discover_keyword_stage_dbs,
    inspect_stage_db,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("promote-keyword-stage-db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="提升 legacy 关键词 stage 数据库到官方路径")
    parser.add_argument("--source-db", type=Path, default=None, help="显式指定源关键词数据库")
    parser.add_argument("--target-db", type=Path, default=DEFAULT_KEYWORD_DB, help="官方关键词数据库路径")
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR, help="关键词 runtime 根目录")
    parser.add_argument("--replace", action="store_true", help="即使目标库已有 payload 也允许覆盖")
    return parser.parse_args()


def checkpoint_sqlite(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA wal_checkpoint(FULL)")
    finally:
        conn.close()


def backup_into_target(source_db: Path, target_db: Path):
    source_conn = sqlite3.connect(str(source_db))
    target_conn = sqlite3.connect(str(target_db))
    try:
        source_conn.execute("PRAGMA wal_checkpoint(FULL)")
        source_conn.backup(target_conn)
    finally:
        target_conn.close()
        source_conn.close()


def rank_source(stats: dict, db_path: Path) -> tuple[int, int, int, int, int]:
    modified_at = int(db_path.stat().st_mtime) if db_path.exists() else 0
    return (
        int(stats.get("keyword_metrics_snapshots", 0) or 0),
        int(stats.get("crawl_observations", 0) or 0),
        int(stats.get("products", 0) or 0),
        int(stats.get("keyword_runs", 0) or 0),
        modified_at,
    )


def select_source_db(source_db: Path | None, runtime_dir: Path, target_db: Path) -> tuple[Path, dict]:
    if source_db is not None:
        if not source_db.exists():
            raise SystemExit(f"源数据库不存在: {source_db}")
        stats = inspect_stage_db(source_db)
        if not stats.get("has_payload"):
            raise SystemExit(f"源数据库为空，不能提升: {source_db}")
        return source_db, stats

    candidates = []
    for db_path in discover_keyword_stage_dbs(runtime_dir):
        if db_path == target_db or not db_path.exists():
            continue
        stats = inspect_stage_db(db_path)
        if not stats.get("has_payload"):
            continue
        candidates.append((rank_source(stats, db_path), db_path, stats))

    if not candidates:
        raise SystemExit("未找到可提升的 legacy 关键词数据库")

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, best_path, best_stats = candidates[0]
    return best_path, best_stats


def main():
    args = parse_args()
    target_db = args.target_db
    target_db.parent.mkdir(parents=True, exist_ok=True)

    target_stats = inspect_stage_db(target_db) if target_db.exists() else None
    if target_stats and target_stats.get("has_payload") and not args.replace:
        raise SystemExit(f"目标数据库已有 payload，如需覆盖请加 --replace: {target_db}")

    source_db, source_stats = select_source_db(args.source_db, args.runtime_dir, target_db)
    logger.info("选择提升源库: %s", source_db)
    logger.info("提升目标库: %s", target_db)

    checkpoint_sqlite(source_db)
    if target_db.exists():
        checkpoint_sqlite(target_db)

    backup_path = None
    if target_db.exists():
        backup_dir = DEFAULT_REPORTS_DIR / "keyword_stage_backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"product_store_{timestamp}.db"
        backup_into_target(target_db, backup_path)
        logger.info("已备份旧目标库: %s", backup_path)

    if target_db.exists():
        target_db.unlink()
    backup_into_target(source_db, target_db)
    logger.info("已完成关键词官方库提升")

    promoted_stats = inspect_stage_db(target_db)
    summary = {
        "promoted_at": datetime.now().isoformat(),
        "source_db": str(source_db),
        "source_stats": source_stats,
        "target_db": str(target_db),
        "target_stats": promoted_stats,
        "backup_path": str(backup_path) if backup_path else None,
    }

    DEFAULT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = DEFAULT_REPORTS_DIR / f"keyword_stage_promotion_{timestamp}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("提升摘要: %s", summary_path)


if __name__ == "__main__":
    main()
