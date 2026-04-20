"""
Noon 多平台交叉选品系统 — CLI 入口

用法:
    python main.py                     # 全流程（漏斗模式）
    python main.py --step keywords     # 仅生成关键词
    python main.py --step scrape       # 仅爬取（需先有关键词）
    python main.py --step analyze      # 仅分析（用已有数据）
    python main.py --step refine       # 对Top关键词跑模式1精确采集
    python main.py --step report       # 仅生成报告
    python main.py --resume            # 断点续跑（跳过已完成步骤）
    python main.py --step expand --keyword "car"  # 扩展关键词联想词
"""
import argparse
import asyncio
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# 确保项目根目录在 Python 路径中
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import Settings
from ops.progress_reporter import get_task_progress_reporter_from_env
from ops.shared_sync_queue import enqueue_and_wait_for_warehouse_sync, enqueue_shared_sync_enabled
from config.category_mapping import map_category
from config.cost_defaults import get_defaults

logger = logging.getLogger("noon-selection")
SYNC_RESULT_PREFIX = "WAREHOUSE_SYNC_RESULT="


KEYWORD_RUNTIME_STEPS = {
    None,
    "keywords",
    "scrape",
    "analyze",
    "refine",
    "report",
    "track",
    "monitor",
    "estimate",
    "expand",
    "cross-analyze",
}


def _infer_runtime_scope(step: str | None) -> str:
    """根据 CLI step 推断默认运行作用域。"""
    if step == "category":
        return "shared"
    if step in KEYWORD_RUNTIME_STEPS:
        return "keyword"
    return "shared"


def _configure_runtime(settings: Settings, args) -> str:
    """
    配置运行时隔离。

    规则:
    - 类目链路默认保持 shared，避免影响现有类目资产
    - 关键词链路默认切到 keyword 独立运行根目录
    - `--data-root` 可显式覆盖数据目录
    - 兼容历史 `--output-dir`，仅对关键词链路作为 `--data-root` 别名生效
    """
    runtime_scope = args.runtime_scope or _infer_runtime_scope(args.step)
    settings.set_runtime_scope(runtime_scope)

    explicit_data_root = args.data_root
    if not explicit_data_root and runtime_scope == "keyword" and args.output_dir:
        explicit_data_root = args.output_dir

    if explicit_data_root:
        settings.set_data_dir(explicit_data_root)
    if args.db_path:
        settings.set_product_store_db_path(args.db_path)

    return runtime_scope


def _save_config_snapshot(settings: Settings, keyword_count: int = 0):
    """保存本次运行的配置快照到 snapshot_dir"""
    snap = {
        "snapshot_id": settings.snapshot_id,
        "timestamp": datetime.now().isoformat(),
        "ksa_exchange_rate": settings.ksa_exchange_rate,
        "shipping_rate_cbm": settings.shipping_rate_cbm,
        "shipping_rate_cbm_peacetime": settings.shipping_rate_cbm_peacetime,
        "default_ad_rate": settings.default_ad_rate,
        "default_return_rate": settings.default_return_rate,
        "keyword_count": keyword_count,
        "proxy_configured": bool(settings.proxy_host),
        "shein_api_configured": bool(settings.shein_api_key),
    }
    path = settings.snapshot_dir / "config_snapshot.json"
    path.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"配置快照已保存: {path}")


def _update_current_symlink(settings: Settings):
    """更新 data/current 指向本次 snapshot（Windows 兼容：写 current.txt 替代 symlink）"""
    link = settings.data_dir / "current"
    target = settings.snapshot_dir
    try:
        if link.exists() or link.is_symlink():
            link.unlink()
        link.symlink_to(target)
        logger.info(f"current → {target.name}")
    except OSError:
        # Windows 非管理员无法创建 symlink，改写 current.txt 记录路径
        txt = settings.data_dir / "current.txt"
        txt.write_text(str(target), encoding="utf-8")
        logger.info(f"current.txt → {target.name} (symlink fallback)")


def _prepare_snapshot_directory(settings: Settings, args) -> None:
    """根据 CLI snapshot 参数准备运行快照目录。"""
    snapshot_id = str(getattr(args, "snapshot", "") or "").strip()
    if not snapshot_id:
        return

    snap_dir = settings.data_dir / "snapshots" / snapshot_id
    settings.set_snapshot_id(snapshot_id)
    if snap_dir.exists():
        logger.info(f"[续跑] 使用已有快照: {snapshot_id}")
        return

    snap_dir.mkdir(parents=True, exist_ok=True)
    if getattr(args, "resume", False):
        logger.info(f"[续跑] 目标快照不存在，已创建新快照: {snapshot_id}")
    else:
        logger.info(f"[snapshot] 已创建快照: {snapshot_id}")


def _save_run_summary(settings: Settings, started_at: datetime,
                      keyword_count: int = 0, scored_df=None, product_df=None):
    """保存运行摘要到 snapshot_dir/run_summary.json"""
    finished_at = datetime.now()
    duration = (finished_at - started_at).total_seconds() / 60

    # 统计各平台数据
    platforms = {}
    for platform in ["noon", "amazon", "google_trends", "alibaba", "temu", "shein"]:
        p_dir = settings.snapshot_dir / platform
        if p_dir.exists():
            files = list(p_dir.glob("*.json"))
            success = len(files)
            platforms[platform] = {"success": success, "failed": 0, "skipped": keyword_count - success}
        else:
            platforms[platform] = {"success": 0, "failed": 0, "skipped": keyword_count, "note": "not configured"}

    # 评分分布
    analysis = {}
    if scored_df is not None and not scored_df.empty:
        analysis["keywords_scored"] = len(scored_df)
        if "grade" in scored_df.columns:
            grade_dist = scored_df["grade"].value_counts().to_dict()
            analysis["grade_distribution"] = {g: int(c) for g, c in grade_dist.items()}
    if product_df is not None and not product_df.empty:
        analysis["products_analyzed"] = len(product_df)

    summary = {
        "snapshot_id": settings.snapshot_id,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_minutes": round(duration, 1),
        "keyword_count": keyword_count,
        "platforms": platforms,
        "analysis": analysis,
    }

    path = settings.snapshot_dir / "run_summary.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"运行摘要已保存: {path}")


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _export_crawl_data_to_excel(settings: Settings):
    """导出爬取数据为 Excel 格式"""
    from output.crawl_data_exporter import CrawlDataExporter

    output_dir = settings.data_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"crawl_data_{settings.snapshot_id}.xlsx"

    logger.info("═══ 导出爬取数据为 Excel ═══")

    exporter = CrawlDataExporter(output_path)

    # 加载 Noon 数据
    noon_dir = settings.snapshot_dir / "noon"
    if noon_dir.exists():
        count = exporter.load_from_json_dir(noon_dir, platform='noon')
        logger.info(f"加载 Noon 产品：{count} 个")

    # 加载 Amazon 数据
    amazon_dir = settings.snapshot_dir / "amazon"
    if amazon_dir.exists():
        count = exporter.load_from_json_dir(amazon_dir, platform='amazon')
        logger.info(f"加载 Amazon 产品：{count} 个")

    if exporter.products:
        exporter.export_to_excel()
        logger.info(f"Excel 已导出：{output_path}")
    else:
        logger.warning("没有产品数据可导出")


def _normalize_platforms(platforms: list[str] | None) -> list[str]:
    if not platforms:
        return ["noon", "amazon", "google_trends", "alibaba", "temu", "shein"]
    return [platform.replace("-", "_").lower() for platform in platforms]


def _load_keywords(settings: Settings, single_keyword: str | None = None) -> list[str]:
    if single_keyword:
        return [single_keyword.strip()]

    kw_file = settings.data_dir / "processed" / "keywords.json"
    if not kw_file.exists():
        logger.error("未找到关键词列表，请先执行 --step keywords 或传入 --keyword")
        sys.exit(1)

    return json.loads(kw_file.read_text(encoding="utf-8"))


def _persist_keyword_results(settings: Settings, platforms: list[str]) -> dict[str, int]:
    from config.product_store import ProductStore

    counts: dict[str, int] = {}
    store = ProductStore(settings.product_store_db_ref)
    try:
        for platform in platforms:
            if platform not in {"noon", "amazon"}:
                continue
            json_dir = settings.snapshot_dir / platform
            if not json_dir.exists():
                continue
            counts[platform] = store.import_keyword_directory(
                json_dir,
                platform=platform,
                snapshot_id=settings.snapshot_id,
            )
    finally:
        store.close()

    for platform, count in counts.items():
        logger.info("已持久化 %s 关键词商品: %s 条 -> %s", platform, count, settings.product_store_db_ref)
    return counts


def _persist_category_results(settings: Settings, category_name: str) -> int:
    from config.product_store import ProductStore

    category_dir = settings.monitoring_dir / "categories" / category_name
    if not category_dir.exists():
        logger.warning("未找到类目结果目录，跳过持久化: %s", category_dir)
        return 0

    store = ProductStore(settings.product_store_db_ref)
    try:
        count = store.import_category_directory(
            category_dir,
            platform="noon",
            snapshot_id=settings.snapshot_id,
            category_name=category_name,
        )
    finally:
        store.close()

    logger.info("已持久化类目商品: %s 条 -> %s", count, settings.product_store_db_ref)
    return count

def _persist_category_subcategory_file(
    settings: Settings,
    *,
    category_name: str,
    json_file: Path,
) -> int:
    from config.product_store import ProductStore

    store = ProductStore(settings.product_store_db_ref)
    try:
        return store.import_category_file(
            json_file,
            platform="noon",
            snapshot_id=settings.snapshot_id,
            category_name=category_name,
        )
    finally:
        store.close()


def _official_category_db_path(settings: Settings) -> Path:
    return settings.project_root / "data" / "product_store.db"


def _sync_category_warehouse(
    settings: Settings,
    *,
    reason: str,
    persisted_count: int,
    category_name: str = "",
) -> dict[str, object]:
    current_db = str(settings.product_store_db_ref).strip()
    current_db_path = Path(settings.product_store_db_path).resolve()
    official_db = str(_official_category_db_path(settings).resolve())
    warehouse_db = str(settings.warehouse_db_ref).strip()
    command = [
        sys.executable,
        str(settings.project_root / "run_shared_warehouse_sync.py"),
        "--actor",
        "category_window",
        "--reason",
        reason,
        "--trigger-db",
        current_db,
        "--warehouse-db",
        warehouse_db,
    ]

    if persisted_count <= 0:
        payload = {
            "status": "skipped",
            "reason": "no_new_category_observations",
            "persisted_count": persisted_count,
            "category_db": current_db,
            "official_category_db": official_db,
            "warehouse_db": warehouse_db,
            "command": command,
            "log_tail": [],
        }
        logger.info("skip category warehouse sync: no new category observations -> %s", current_db)
        return payload

    if enqueue_shared_sync_enabled():
        payload = enqueue_and_wait_for_warehouse_sync(
            actor="category_window",
            reason=reason,
            trigger_db=current_db,
            warehouse_db=warehouse_db,
            snapshot_id=settings.snapshot_id,
            created_by="category_window",
        )
        payload["persisted_count"] = persisted_count
        payload["category_db"] = current_db
        payload["official_category_db"] = official_db
        payload["command"] = ["enqueue_shared_warehouse_sync_task", str(payload.get("task_id") or "")]
        if str(payload.get("status") or "").strip().lower() == "completed":
            logger.info("category warehouse synced via queued shared sync: %s", payload.get("task_id"))
        else:
            logger.info(
                "category warehouse queued sync terminal=%s task_id=%s",
                payload.get("status"),
                payload.get("task_id"),
            )
        return payload

    if str(current_db_path) != official_db:
        payload = {
            "status": "skipped",
            "reason": "non_official_category_db",
            "persisted_count": persisted_count,
            "category_db": current_db,
            "official_category_db": official_db,
            "warehouse_db": warehouse_db,
            "command": command,
            "log_tail": [],
        }
        logger.info("skip category warehouse sync: current category db is not official -> %s", current_db)
        return payload

    completed = subprocess.run(
        command,
        cwd=str(settings.project_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    stdout_lines = [line for line in completed.stdout.splitlines() if line.strip()]
    stderr_lines = [line for line in completed.stderr.splitlines() if line.strip()]
    log_lines = stdout_lines + stderr_lines
    result_payload: dict[str, object] | None = None
    for line in reversed(stdout_lines):
        if not line.startswith(SYNC_RESULT_PREFIX):
            continue
        try:
            result_payload = json.loads(line[len(SYNC_RESULT_PREFIX):])
        except json.JSONDecodeError:
            result_payload = None
        break

    if completed.returncode != 0:
        detail = "\n".join(log_lines[-20:])
        payload = {
            "status": str((result_payload or {}).get("status") or "failed"),
            "reason": str((result_payload or {}).get("reason") or reason),
            "persisted_count": persisted_count,
            "category_db": current_db,
            "official_category_db": official_db,
            "warehouse_db": warehouse_db,
            "command": command,
            "log_tail": log_lines[-20:],
            "returncode": completed.returncode,
            "error": detail or str(completed.returncode),
            "skip_reason": str((result_payload or {}).get("skip_reason") or ""),
            "sync_state": (result_payload or {}).get("sync_state") or {},
        }
        logger.error("category warehouse sync failed (%s): %s", reason, payload["error"])
        return payload

    payload = {
        "status": str((result_payload or {}).get("status") or "completed"),
        "reason": str((result_payload or {}).get("reason") or reason),
        "persisted_count": persisted_count,
        "category_db": current_db,
        "official_category_db": official_db,
        "warehouse_db": warehouse_db,
        "command": command,
        "log_tail": log_lines[-10:],
        "skip_reason": str((result_payload or {}).get("skip_reason") or ""),
        "sync_state": (result_payload or {}).get("sync_state") or {},
    }
    if payload["status"] == "skipped" and payload["skip_reason"]:
        logger.info(
            "skip category warehouse sync: shared runner -> %s",
            payload["skip_reason"],
        )
        return payload
    logger.info("category warehouse synced: %s", warehouse_db)
    return payload


def _run_category_step(settings: Settings, args) -> None:
    if not args.category:
        logger.error("please provide --category automotive")
        sys.exit(1)

    from scrapers.noon_category_crawler import NoonCategoryCrawler

    persisted_count = 0
    progress_reporter = get_task_progress_reporter_from_env()

    if progress_reporter:
        progress_reporter.update(
            "runtime_collecting",
            message=f"starting category crawl: {args.category}",
            metrics={
                "categories_total": 1,
                "categories_completed": 0,
                "persisted_subcategories": 0,
                "persisted_observations": 0,
            },
            details={"current_category": args.category, "product_count_per_leaf": args.noon_count},
        )

    def on_subcategory_saved(sub_name: str, json_file: Path, _payload: dict) -> None:
        nonlocal persisted_count
        if not args.persist:
            return
        imported = _persist_category_subcategory_file(
            settings,
            category_name=args.category,
            json_file=json_file,
        )
        persisted_count += imported
        logger.info(
            "subcategory persisted: %s/%s -> +%s observations (running_total=%s)",
            args.category,
            sub_name,
            imported,
            persisted_count,
        )
        if progress_reporter:
            progress_reporter.update(
                "stage_persisted",
                message=f"persisted {args.category}/{sub_name}",
                metrics={
                    "categories_total": 1,
                    "categories_completed": 0,
                    "persisted_subcategories": 1,
                    "persisted_observations": persisted_count,
                },
                details={
                    "current_category": args.category,
                    "current_subcategory": sub_name,
                    "imported_count": imported,
                },
            )

    crawler = NoonCategoryCrawler(
        args.category,
        settings.data_dir,
        max_products_per_sub=args.noon_count,
        max_depth=args.max_depth,
        target_subcategory=args.target_subcategory,
        on_subcategory_saved=on_subcategory_saved if args.persist else None,
    )
    result = asyncio.run(crawler.run())
    warehouse_sync: dict[str, object] | None = None

    if args.persist and result and "error" not in result:
        if progress_reporter:
            progress_reporter.update(
                "warehouse_syncing",
                message=f"syncing category warehouse: {args.category}",
                metrics={
                    "categories_total": 1,
                    "categories_completed": 0,
                    "persisted_subcategories": 1 if persisted_count else 0,
                    "persisted_observations": persisted_count,
                },
                details={"current_category": args.category},
            )
        warehouse_sync = _sync_category_warehouse(
            settings,
            reason="category_persist",
            persisted_count=persisted_count,
            category_name=args.category,
        )

    if args.export_excel and result and "error" not in result:
        logger.info("exporting category Excel...")
        from output.crawl_data_exporter import export_crawl_data

        json_dir = settings.monitoring_dir / "categories" / args.category
        if json_dir.exists():
            output_path = settings.exports_dir / f"category_{args.category}_products.xlsx"
            output_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                export_crawl_data([json_dir], output_path, ['noon'], mode='category')
                logger.info("category Excel exported: %s", output_path)
            except Exception as e:
                logger.error("category Excel export failed: %s", e)
        else:
            logger.warning("category result directory not found: %s", json_dir)

    if warehouse_sync and warehouse_sync.get("status") == "failed":
        logger.error("category stage persist succeeded, but warehouse sync failed; Web may still be stale")
        if warehouse_sync.get("log_tail"):
            logger.error("warehouse sync log tail:\n%s", "\n".join(warehouse_sync["log_tail"]))
        sys.exit(1)
    if progress_reporter:
        final_stage = "runtime_collecting"
        if args.persist:
            if warehouse_sync and warehouse_sync.get("status") == "completed":
                final_stage = "web_visible"
            elif persisted_count > 0:
                final_stage = "stage_persisted"
        progress_reporter.update(
            final_stage,
            message=f"completed category crawl: {args.category}",
            metrics={
                "categories_total": 1,
                "categories_completed": 1,
                "persisted_subcategories": 1 if persisted_count else 0,
                "persisted_observations": persisted_count,
            },
            details={
                "current_category": args.category,
                "warehouse_sync_status": str((warehouse_sync or {}).get("status") or ""),
            },
            completed=True,
        )


# ═══════════════════════════════════════════════════
# Step 1: 关键词生成
# ═══════════════════════════════════════════════════
async def step_keywords(settings: Settings) -> list[str]:
    """生成并扩展关键词列表"""
    from keywords.seed_generator import generate_seeds
    from keywords.keyword_expander import expand_keywords, extract_bsr_keywords

    data_dir = settings.data_dir

    # 1a. 种子关键词
    logger.info("═══ Step 1a: 生成种子关键词 ═══")
    seeds = await generate_seeds()
    logger.info(f"种子关键词: {len(seeds)} 个")

    # 保存种子词
    processed_dir = data_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    seeds_file = processed_dir / "seeds.json"
    seeds_file.write_text(json.dumps(seeds, ensure_ascii=False, indent=2), encoding="utf-8")

    # 1b. BSR 反向关键词（如果已有 Amazon BSR 数据）
    bsr_keywords = []
    bsr_dir = data_dir / "raw" / "amazon_bsr"
    if bsr_dir.exists():
        bsr_items = []
        for f in bsr_dir.glob("*.json"):
            try:
                items = json.loads(f.read_text(encoding="utf-8"))
                if isinstance(items, list):
                    bsr_items.extend(items)
            except Exception:
                continue
        if bsr_items:
            bsr_keywords = extract_bsr_keywords(bsr_items)
            logger.info(f"BSR 反向关键词: {len(bsr_keywords)} 个")

    # 1c. 扩展关键词
    logger.info("═══ Step 1b: 扩展关键词 ═══")
    noon_data_dir = data_dir / "raw" / "noon"
    amazon_data_dir = data_dir / "raw" / "amazon"
    expanded = expand_keywords(seeds, noon_data_dir, amazon_data_dir, bsr_keywords)
    logger.info(f"扩展后关键词: {len(expanded)} 个")

    # 保存
    kw_file = data_dir / "processed" / "keywords.json"
    kw_file.write_text(json.dumps(expanded, ensure_ascii=False, indent=2), encoding="utf-8")

    return expanded


# ═══════════════════════════════════════════════════
# Step 2: 多平台爬取
# ═══════════════════════════════════════════════════
async def _run_noon(settings: Settings, keywords: list[str], max_products: int | None = None):
    """Noon 爬取任务（独立浏览器实例）"""
    from scrapers.noon_scraper import NoonScraper
    logger.info("═══ [并行] Noon 爬取启动 ═══")
    # 全量扫描时跳过联想词（已在keyword_expander阶段获取，每词省~30秒）
    noon = NoonScraper(
        settings,
        skip_suggestions=(len(keywords) > 20),
        max_products=max_products,
    )
    await noon.run(keywords)
    logger.info("═══ [并行] Noon 爬取完成 ═══")


async def _run_amazon(settings: Settings, keywords: list[str], max_products: int | None = None):
    """Amazon 爬取任务（独立浏览器实例）"""
    from scrapers.amazon_scraper import AmazonScraper
    logger.info("═══ [并行] Amazon 爬取启动 ═══")
    amazon = AmazonScraper(settings, max_products=max_products)
    await amazon.run(keywords)
    # BSR 反向发现（run() 结束后浏览器已关闭，需要重启）
    logger.info("═══ [并行] Amazon BSR 反向发现 ═══")
    try:
        await amazon.start_browser()
        await amazon.scrape_bestsellers()
        await amazon.stop_browser()
    except Exception as e:
        logger.warning(f"[amazon] BSR 反向发现失败（非致命）: {e}")
    logger.info("═══ [并行] Amazon 爬取完成 ═══")


async def step_scrape(
    settings: Settings,
    keywords: list[str],
    *,
    platforms: list[str] | None = None,
    noon_count: int | None = None,
    amazon_count: int | None = None,
):
    """对关键词列表执行多平台爬取（Noon + Amazon 并行）"""
    from scrapers.alibaba_scraper import AlibabaScraper
    from scrapers.temu_scraper import TemuScraper
    from scrapers.shein_scraper import SheinScraper
    from scrapers.google_trends import GoogleTrendsFetcher

    import time
    start = time.time()
    selected_platforms = set(_normalize_platforms(platforms))

    # P0: Noon + Amazon 并行（各自独立浏览器实例）
    primary_tasks = []
    if "noon" in selected_platforms:
        primary_tasks.append(_run_noon(settings, keywords, max_products=noon_count))
    if "amazon" in selected_platforms:
        primary_tasks.append(_run_amazon(settings, keywords, max_products=amazon_count))

    if primary_tasks:
        logger.info(
            "═══ Step 2: 平台爬取启动 (%s 关键词, platforms=%s) ═══",
            len(keywords),
            ", ".join(sorted(selected_platforms)),
        )
        await asyncio.gather(*primary_tasks)
    else:
        logger.warning("未选择 Noon/Amazon，跳过关键词主爬取")

    elapsed = time.time() - start
    logger.info(f"═══ 主平台爬取完成，耗时 {elapsed/60:.1f} 分钟 ═══")

    # 非必要数据源: Google Trends（失败不阻塞流程）
    if "google_trends" in selected_platforms:
        logger.info("═══ Step 2d: Google Trends（可选） ═══")
        try:
            trends = GoogleTrendsFetcher(settings)
            await trends.fetch_all(keywords)
        except Exception as e:
            logger.warning(f"[google_trends] 采集失败（非致命，评分自动降级）: {e}")

    # P0 供应端: Alibaba
    if "alibaba" in selected_platforms:
        logger.info("═══ Step 2e: Alibaba FOB 采集 ═══")
        alibaba = AlibabaScraper(settings)
        await alibaba.run(keywords)

    # P1: Temu
    if "temu" in selected_platforms:
        logger.info("═══ Step 2f: Temu 爬取 ═══")
        try:
            temu = TemuScraper(settings)
            await temu.run(keywords)
        except Exception as e:
            logger.warning(f"Temu 爬取失败（非致命）: {e}")

    # P1: SHEIN
    if "shein" in selected_platforms:
        logger.info("═══ Step 2g: SHEIN 爬取 ═══")
        shein = SheinScraper(settings)
        if shein.is_configured:
            await shein.run(keywords)
        else:
            logger.info("SHEIN API 未配置，跳过")


# ═══════════════════════════════════════════════════
# Step 3: 数据分析
# ═══════════════════════════════════════════════════
def step_analyze(settings: Settings):
    """清洗 + 交叉比对 + 利润计算 + 评分"""
    import pandas as pd
    from analysis.data_cleaner import merge_all
    from analysis.cross_validator import compute_cross_metrics
    from analysis.scoring_model import compute_profit_columns, score_all

    data_dir = settings.snapshot_dir

    # 3a. 数据清洗合并
    logger.info("═══ Step 3a: 数据清洗合并 ═══")
    df = merge_all(data_dir)
    if df.empty:
        logger.error("无数据可分析！请先执行 --step scrape")
        return None

    # 3b. 品类映射（用 primary_category + category_tree 多级匹配）
    logger.info("═══ Step 3b: 品类映射 ═══")
    df["v6_category"] = df.apply(
        lambda r: map_category(
            r.get("noon_category_path", ""),
            r.get("noon_category_tree") if isinstance(r.get("noon_category_tree"), list) else None,
        ),
        axis=1,
    )

    # 3c. 品类默认值
    logger.info("═══ Step 3c: 填充品类默认值 ═══")
    for idx, row in df.iterrows():
        defaults = get_defaults(row["v6_category"])
        df.at[idx, "default_length_cm"] = defaults["l"]
        df.at[idx, "default_width_cm"] = defaults["w"]
        df.at[idx, "default_height_cm"] = defaults["h"]
        df.at[idx, "default_weight_kg"] = defaults["wt"]
        df.at[idx, "cost_ratio"] = defaults["cost_ratio"]

    # 3d. 交叉比对
    logger.info("═══ Step 3d: 交叉比对 ═══")
    df = compute_cross_metrics(df)

    # 3e. 利润计算 + 评分
    logger.info("═══ Step 3e: 利润计算 + 6维度评分 ═══")
    df = score_all(df, settings)

    # 保存中间结果到 snapshot_dir/processed/
    processed_dir = data_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(processed_dir / "scored.parquet", index=False)
    logger.info(f"评分结果已保存: {processed_dir / 'scored.parquet'}")

    # 同时保留兼容路径
    compat_dir = settings.data_dir / "processed"
    compat_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(compat_dir / "scored.parquet", index=False)

    return df


# ═══════════════════════════════════════════════════
# Step 4: 模式1 精确采集（Top N 关键词）
# ═══════════════════════════════════════════════════
async def step_refine(settings: Settings):
    """对 Top N 关键词执行模式1精确采集（Alibaba FOB + 详情页尺寸）"""
    import pandas as pd
    from scrapers.noon_scraper import NoonScraper
    from scrapers.amazon_scraper import AmazonScraper
    from scrapers.alibaba_scraper import AlibabaScraper

    scored_path = settings.snapshot_dir / "processed" / "scored.parquet"
    if not scored_path.exists():
        scored_path = settings.data_dir / "processed" / "scored.parquet"
    if not scored_path.exists():
        logger.error("未找到评分数据！请先执行 --step analyze")
        return

    df = pd.read_parquet(scored_path)
    top_n = settings.funnel_top_n
    top_keywords = df.nsmallest(top_n, "rank")["keyword"].tolist()
    logger.info(f"═══ Step 4: 对 Top {len(top_keywords)} 关键词精确采集 ═══")

    # Alibaba FOB 精确价格
    alibaba = AlibabaScraper(settings)
    await alibaba.run(top_keywords)

    # Noon 详情页尺寸
    snap = settings.snapshot_dir
    noon = NoonScraper(settings)
    noon_details_dir = snap / "noon_details"
    noon_details_dir.mkdir(parents=True, exist_ok=True)
    for kw in top_keywords:
        kw_file = snap / "noon" / f"{kw}.json"
        if kw_file.exists():
            rec = json.loads(kw_file.read_text(encoding="utf-8"))
            products = rec.get("products", [])
            if products:
                url = products[0].get("url", "")
                if url:
                    try:
                        dims = await noon.scrape_product_dimensions(url)
                        if dims:
                            detail_file = noon_details_dir / f"{kw}.json"
                            detail_file.write_text(
                                json.dumps(dims, ensure_ascii=False), encoding="utf-8"
                            )
                    except Exception as e:
                        logger.warning(f"Noon 详情页采集失败 [{kw}]: {e}")

    # Amazon 详情页尺寸
    amazon = AmazonScraper(settings)
    amazon_details_dir = snap / "amazon_details"
    amazon_details_dir.mkdir(parents=True, exist_ok=True)
    for kw in top_keywords:
        kw_file = snap / "amazon" / f"{kw}.json"
        if kw_file.exists():
            rec = json.loads(kw_file.read_text(encoding="utf-8"))
            products = rec.get("products", [])
            if products:
                url = products[0].get("url", "")
                if url:
                    try:
                        dims = await amazon.scrape_product_dimensions(url)
                        if dims:
                            detail_file = amazon_details_dir / f"{kw}.json"
                            detail_file.write_text(
                                json.dumps(dims, ensure_ascii=False), encoding="utf-8"
                            )
                    except Exception as e:
                        logger.warning(f"Amazon 详情页采集失败 [{kw}]: {e}")

    logger.info("精确采集完成，请重新执行 --step analyze 更新评分")


# ═══════════════════════════════════════════════════
# Step 5: 报告输出
# ═══════════════════════════════════════════════════
def step_report(settings: Settings):
    """生成 Excel 报告"""
    import pandas as pd
    from output.excel_report import generate_report

    # 优先从 snapshot 读取
    scored_path = settings.snapshot_dir / "processed" / "scored.parquet"
    if not scored_path.exists():
        scored_path = settings.data_dir / "processed" / "scored.parquet"
    if not scored_path.exists():
        logger.error("未找到评分数据！请先执行 --step analyze")
        return

    df = pd.read_parquet(scored_path)

    report_path = settings.snapshot_dir / f"report_{settings.snapshot_id}.xlsx"

    generate_report(df, report_path)
    logger.info(f"报告路径: {report_path}")

    # 输出概要
    if "grade" in df.columns:
        print("\n" + "=" * 50)
        print("  选品报告概要")
        print("=" * 50)
        print(f"  总关键词数: {len(df)}")
        for grade in ["A", "B", "C", "D", "E"]:
            count = len(df[df["grade"] == grade])
            print(f"  {grade}级: {count} 个")
        if "margin_war_pct" in df.columns:
            valid = df["margin_war_pct"].dropna()
            if not valid.empty:
                print(f"\n  战时利润率中位数: {valid.median():.1f}%")
                print(f"  战时利润率 > 30%: {len(valid[valid > 30])} 个")
        print(f"\n  报告位置: {report_path}")
        print("=" * 50)


# ═══════════════════════════════════════════════════
# 全流程（漏斗模式）
# ═══════════════════════════════════════════════════
async def run_full_pipeline(
    settings: Settings,
    resume: bool = False,
    *,
    persist: bool = False,
    platforms: list[str] | None = None,
    noon_count: int | None = None,
    amazon_count: int | None = None,
):
    """
    全流程漏斗执行:
        关键词 → 爬取 → 分析(模式2粗筛) → Top N精确采集 → 重新分析 → 报告
    """
    logger.info(f"═══ Snapshot ID: {settings.snapshot_id} ═══")
    logger.info(f"═══ 数据目录: {settings.snapshot_dir} ═══")

    data_dir = settings.data_dir

    # Step 1: 关键词
    kw_file = data_dir / "processed" / "keywords.json"
    if resume and kw_file.exists():
        keywords = json.loads(kw_file.read_text(encoding="utf-8"))
        logger.info(f"[resume] 加载已有关键词: {len(keywords)} 个")
    else:
        keywords = await step_keywords(settings)

    # 保存配置快照和关键词到 snapshot_dir
    _save_config_snapshot(settings, keyword_count=len(keywords))
    kw_snap = settings.snapshot_dir / "keywords.json"
    kw_snap.write_text(json.dumps(keywords, ensure_ascii=False, indent=2), encoding="utf-8")

    # Step 2: 爬取
    await step_scrape(
        settings,
        keywords,
        platforms=platforms,
        noon_count=noon_count,
        amazon_count=amazon_count,
    )
    if persist:
        _persist_keyword_results(settings, _normalize_platforms(platforms))

    # Step 3: 第一轮分析（模式2 粗筛）
    df = step_analyze(settings)
    if df is None:
        return

    # Step 4: 模式1 精确采集 Top N
    await step_refine(settings)

    # Step 3 重跑: 用精确数据重新评分
    logger.info("═══ 用精确数据重新评分 ═══")
    df = step_analyze(settings)

    # Step 5: 报告
    step_report(settings)

    # 更新 current 软链接
    _update_current_symlink(settings)


# ═══════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Noon 多平台交叉选品系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py                     # 全流程
  python main.py --step keywords     # 仅生成关键词
  python main.py --step scrape       # 仅爬取
  python main.py --step analyze      # 仅分析
  python main.py --step refine       # Top N 精确采集
  python main.py --step report       # 仅生成报告
  python main.py --resume            # 断点续跑
        """,
    )
    parser.add_argument(
        "--step",
        choices=["keywords", "scrape", "analyze", "refine", "report", "track", "monitor", "estimate", "category", "expand", "cross-analyze"],
        help="仅执行指定步骤",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="断点续跑（跳过已完成的步骤）",
    )
    parser.add_argument(
        "--snapshot",
        type=str,
        default=None,
        help="指定快照ID（断点续跑同一快照，如 2026-03-17_230000）",
    )
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="类目名称（配合 --step category 使用，如 automotive）",
    )
    parser.add_argument(
        "--target-subcategory",
        type=str,
        default=None,
        help="目标子类目（配合 --step category 使用，如 car_accessories）",
    )
    parser.add_argument(
        "--keyword",
        type=str,
        default=None,
        help="单个关键词（配合 --step scrape 使用）",
    )
    parser.add_argument(
        "--noon-count",
        type=int,
        default=100,
        help="Noon 每关键词爬取数量（默认 100）",
    )
    parser.add_argument(
        "--amazon-count",
        type=int,
        default=100,
        help="Amazon 每关键词爬取数量（默认 100）",
    )
    parser.add_argument(
        "--platforms",
        type=str,
        nargs="+",
        default=None,
        help="抓取平台列表（如 noon amazon）；不传则走默认全链路",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="输出目录（默认 data/raw）",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="持久化产品数据到 SQLite 数据库",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="输出详细日志",
    )
    parser.add_argument(
        "--expand-keyword",
        type=str,
        default=None,
        help="核心关键词，用于在线扩展联想词（配合 --step expand 使用）",
    )
    parser.add_argument(
        "--expand-platforms",
        type=str,
        nargs="+",
        default=["noon", "amazon"],
        help="扩展平台列表 (noon/amazon)，默认两者都爬取",
    )
    parser.add_argument(
        "--export-excel",
        action="store_true",
        help="导出爬取数据为 Excel 格式",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=3,
        help="类目爬虫最大深度（默认 3，最大 5）",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    settings = Settings()
    _prepare_snapshot_directory(settings, args)

    # 指定快照ID → 复用已有快照目录
    if args.snapshot:
        snap_dir = settings.data_dir / "snapshots" / args.snapshot
        if not snap_dir.exists():
            logger.error(f"快照目录不存在: {snap_dir}")
            sys.exit(1)
        settings.set_snapshot_id(args.snapshot)
        logger.info(f"[续跑] 使用已有快照: {args.snapshot}")

    logger.info(f"Snapshot ID: {settings.snapshot_id}")
    logger.info(f"Snapshot Dir: {settings.snapshot_dir}")

    # 确保 snapshot 子目录存在
    for sub in ["noon", "amazon", "alibaba", "temu",
                 "shein", "google_trends", "amazon_bsr",
                 "processed"]:
        (settings.snapshot_dir / sub).mkdir(parents=True, exist_ok=True)
    # 兼容旧路径
    (settings.data_dir / "processed").mkdir(parents=True, exist_ok=True)

    if args.step is None:
        # 全流程
        asyncio.run(
            run_full_pipeline(
                settings,
                resume=args.resume,
                persist=args.persist,
                platforms=args.platforms,
                noon_count=args.noon_count,
                amazon_count=args.amazon_count,
            )
        )

        # 导出 Excel（全流程完成后）
        if args.export_excel:
            _export_crawl_data_to_excel(settings)
    elif args.step == "keywords":
        asyncio.run(step_keywords(settings))
    elif args.step == "scrape":
        keywords = _load_keywords(settings, args.keyword)
        asyncio.run(
            step_scrape(
                settings,
                keywords,
                platforms=args.platforms,
                noon_count=args.noon_count,
                amazon_count=args.amazon_count,
            )
        )
        if args.persist:
            _persist_keyword_results(settings, _normalize_platforms(args.platforms))

        # 导出 Excel
        if args.export_excel:
            _export_crawl_data_to_excel(settings)
    elif args.step == "analyze":
        step_analyze(settings)
    elif args.step == "refine":
        asyncio.run(step_refine(settings))
    elif args.step == "report":
        step_report(settings)
    elif args.step == "track":
        # 产品追踪模式 - 增量更新已有产品数据
        from config.product_store import ProductStore
        from analysis.trend_analyzer import TrendAnalyzer

        db_path = settings.product_store_db_path
        store = ProductStore(db_path)
        analyzer = TrendAnalyzer(db_path)

        logger.info(f"═══ 产品追踪模式 - 数据库：{db_path} ═══")

        # 获取统计信息
        stats = store.get_statistics()
        logger.info(f"数据库统计：{stats}")

        # 生成趋势报告
        report = analyzer.generate_report(days=30)
        logger.info(f"分析了 {report['summary']['total_products_analyzed']} 个产品")
        logger.info(f"告警数量：{len(report['alerts'])}")

        # 保存报告
        report_path = settings.data_dir / "trend_report.json"
        import json
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"趋势报告已保存：{report_path}")
    elif args.step == "monitor":
        print("阶段1.5功能：持续监控模块，待实现")
    elif args.step == "estimate":
        print("阶段1.5功能：销量估算引擎，待实现")
    elif args.step == "category":
        _run_category_step(settings, args)
    elif args.step == "expand":
        # 关键词在线扩展模式
        if not args.expand_keyword:
            logger.error("请指定核心关键词：--expand-keyword \"car\"")
            sys.exit(1)

        from keywords.online_expander import expand_keywords

        logger.info(f"═══ 关键词扩展模式 - 种子词：{args.expand_keyword} ═══")
        logger.info(f"平台：{', '.join(args.expand_platforms)}")

        # 输出目录
        output_dir = settings.data_dir / "keywords_expanded"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{args.expand_keyword.replace(' ', '_')}_expanded.txt"

        result = expand_keywords(args.expand_keyword, args.expand_platforms, output_path)

        logger.info(f"扩展结果:")
        logger.info(f"  Noon: {len(result['noon'])} 个关键词")
        logger.info(f"  Amazon: {len(result['amazon'])} 个关键词")
        logger.info(f"  总计 (去重): {len(result['all'])} 个关键词")
        logger.info(f"已保存到：{output_path}")
    elif args.step == "cross-analyze":
        # 跨平台交叉分析模式
        from analysis.cross_platform_analyzer import analyze_cross_platform

        logger.info("═══ 跨平台交叉分析模式 ═══")

        # 数据源路径
        noon_dir = settings.snapshot_dir / "noon"
        amazon_dir = settings.snapshot_dir / "amazon"

        if not noon_dir.exists() or not amazon_dir.exists():
            logger.error("未找到爬取数据！请先执行 --step scrape")
            sys.exit(1)

        # 输出目录
        output_dir = settings.data_dir / "cross_analysis"
        output_dir.mkdir(parents=True, exist_ok=True)

        results = analyze_cross_platform(noon_dir, amazon_dir, output_dir)

        logger.info(f"分析完成!")
        logger.info(f"  机会点：{len(results['overall_summary']['opportunities'])} 个")
        logger.info(f"  警告：{len(results['overall_summary']['warnings'])} 个")
        logger.info(f"  建议：{results['overall_summary']['recommendation']}")
        logger.info(f"报告已导出到：{output_dir}/")


if __name__ == "__main__":
    main()
