"""
全流程管线 V2 — Phase 3/4/5
  Phase 3: 关键词拓展（Noon + Amazon 联想词）
  Phase 4: Noon + Amazon 并行爬取（断点续跑，100条/词）
  Phase 5: 合并新旧数据 → 分析 → Excel报告
"""
import asyncio
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline_v2.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("pipeline-v2")

# ═══ 全量扫描快照（旧数据） ═══
OLD_SNAP = Path("D:/claude noon/noon-selection-tool/data/snapshots/2026-03-17_114142")

# ═══ 种子词 ═══
SEED_KEYWORDS = {
    "运动健身": [
        "cardio training", "exercise machines", "exercise bike", "treadmill",
        "home gym", "fitness", "weight bench", "resistance bands", "kettlebell",
        "dumbbell", "rowing machine", "elliptical", "pull up bar", "ab roller",
        "jump rope fitness", "foam roller", "yoga block", "pilates", "gym accessories",
    ],
    "健康营养": [
        "protein powder", "whey protein", "vitamins supplements", "collagen",
        "omega 3", "probiotics", "multivitamin", "sports nutrition", "energy bars",
        "pre workout", "bcaa", "creatine", "fish oil", "vitamin d", "vitamin c",
        "zinc supplements", "melatonin", "biotin",
    ],
    "汽车配件": [
        "car accessories", "car phone holder", "car seat cover", "car organizer",
        "dash cam", "car charger", "car vacuum", "car air freshener", "car sun shade",
        "car trash can", "jump starter", "car led lights", "steering wheel cover",
        "car floor mat", "car camera", "tire inflator", "car cleaning kit",
    ],
    "美妆护肤": [
        "skincare", "face wash", "moisturizer", "sunscreen", "serum", "face mask",
        "lip balm", "body lotion", "hair oil", "shampoo", "conditioner",
        "perfume oil", "makeup remover", "eye cream", "toner", "cleanser",
        "body butter", "hair mask",
    ],
    "家居收纳": [
        "kitchen organizer", "storage box", "shelf organizer", "closet organizer",
        "bathroom organizer", "spice rack", "shoe rack", "wall shelf",
        "drawer organizer", "laundry basket", "trash can", "towel rack",
        "curtain", "bedding set", "pillow", "blanket", "led desk lamp", "night light",
    ],
}

ALL_SEEDS = []
for group, seeds in SEED_KEYWORDS.items():
    ALL_SEEDS.extend(seeds)

# 已经在全量扫描中爬过的关键词
def _get_already_scraped() -> set:
    """从旧快照获取已爬过的关键词"""
    scraped = set()
    for platform in ["noon", "amazon"]:
        d = OLD_SNAP / platform
        if d.exists():
            for f in d.glob("*.json"):
                kw = f.stem.replace("_", " ")
                scraped.add(kw)
    return scraped

ERRORS = []


# ═══════════════════════════════════════════════════
# Phase 3: 关键词拓展
# ═══════════════════════════════════════════════════
async def phase3_expand_keywords(settings: Settings) -> list[str]:
    """用 Noon + Amazon 联想词拓展种子词"""
    logger.info("=" * 60)
    logger.info("  Phase 3: 关键词拓展")
    logger.info("=" * 60)

    already_scraped = _get_already_scraped()
    logger.info(f"已爬过关键词: {len(already_scraped)} 个")

    all_expanded = set(ALL_SEEDS)  # 种子词本身也保留

    # Noon 联想词
    logger.info(f"\n--- Noon 联想词拓展 ({len(ALL_SEEDS)} 种子词) ---")
    try:
        from scrapers.noon_scraper import NoonScraper
        noon = NoonScraper(settings, skip_suggestions=False)
        await noon.start_browser()
        ctx = await noon.new_context()
        page = await ctx.new_page()

        for i, seed in enumerate(ALL_SEEDS):
            try:
                # 访问搜索页
                url = f"https://www.noon.com/saudi-en/search/?q={seed}"
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_timeout(3000)

                # 获取联想词
                suggestions = await noon._get_suggestions(page, seed)
                if suggestions:
                    for s in suggestions[:10]:
                        all_expanded.add(s.lower().strip())

                if (i + 1) % 20 == 0:
                    logger.info(f"  Noon联想: {i+1}/{len(ALL_SEEDS)}, 累计 {len(all_expanded)} 词")

                await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"  Noon联想失败 [{seed}]: {e}")
                continue

        await page.close()
        await ctx.close()
        await noon.stop_browser()
    except Exception as e:
        logger.error(f"Noon联想词拓展失败: {e}")
        ERRORS.append(f"Phase3 Noon suggestions: {e}")

    logger.info(f"Noon联想后: {len(all_expanded)} 词")

    # Amazon 联想词
    logger.info(f"\n--- Amazon 联想词拓展 ---")
    try:
        from scrapers.amazon_scraper import AmazonScraper
        amz = AmazonScraper(settings)
        await amz.start_browser()
        ctx = await amz.new_context()
        page = await ctx.new_page()

        for i, seed in enumerate(ALL_SEEDS):
            try:
                url = f"https://www.amazon.sa/s?k={seed}"
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_timeout(2000)

                # Amazon 搜索联想：从搜索栏获取
                suggestions = await page.evaluate("""(keyword) => {
                    const results = [];
                    // Related searches at bottom
                    const relLinks = document.querySelectorAll(
                        '.s-related-search a, [class*="apb-browse-refinements-indent-2"] a'
                    );
                    for (const l of relLinks) {
                        const text = (l.textContent || '').trim().toLowerCase();
                        if (text && text.length >= 3 && text !== keyword.toLowerCase()) {
                            results.push(text);
                        }
                    }
                    return results.slice(0, 10);
                }""", seed)

                if suggestions:
                    for s in suggestions:
                        all_expanded.add(s.lower().strip())

                if (i + 1) % 20 == 0:
                    logger.info(f"  Amazon联想: {i+1}/{len(ALL_SEEDS)}, 累计 {len(all_expanded)} 词")

                await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"  Amazon联想失败 [{seed}]: {e}")
                continue

        await page.close()
        await ctx.close()
        await amz.stop_browser()
    except Exception as e:
        logger.error(f"Amazon联想词拓展失败: {e}")
        ERRORS.append(f"Phase3 Amazon suggestions: {e}")

    logger.info(f"Amazon联想后: {len(all_expanded)} 词")

    # 去除已爬过的
    new_keywords = sorted(all_expanded - already_scraped)
    logger.info(f"去重后新关键词: {len(new_keywords)} 个 (排除已爬 {len(all_expanded) - len(new_keywords)} 个)")

    # 保存
    kw_file = settings.snapshot_dir / "keywords.json"
    kw_file.write_text(json.dumps(new_keywords, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"关键词已保存: {kw_file}")

    return new_keywords


# ═══════════════════════════════════════════════════
# Phase 4: Noon + Amazon 并行爬取
# ═══════════════════════════════════════════════════
async def _run_noon_with_recovery(settings: Settings, keywords: list[str]):
    """Noon 爬取，带浏览器崩溃恢复"""
    from scrapers.noon_scraper import NoonScraper
    logger.info(f"[Noon] 开始爬取 {len(keywords)} 个关键词")

    consecutive_failures = 0
    noon = NoonScraper(settings, skip_suggestions=True)
    pending = [kw for kw in keywords if not noon.is_completed(kw)]
    skipped = len(keywords) - len(pending)
    if skipped:
        logger.info(f"[Noon] 跳过 {skipped} 个已完成, 剩余 {len(pending)} 个")

    if not pending:
        logger.info("[Noon] 所有关键词已完成")
        return

    start_time = time.time()
    success = 0
    i = 0

    while i < len(pending):
        # 启动浏览器
        try:
            await noon.start_browser()
        except Exception as e:
            logger.error(f"[Noon] 浏览器启动失败: {e}")
            ERRORS.append(f"Noon browser start: {e}")
            await asyncio.sleep(60)
            continue

        try:
            while i < len(pending):
                kw = pending[i]
                try:
                    result = await noon.scrape_with_retry(kw)
                    if result and result.get("products"):
                        success += 1
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1

                    # 连续5次失败 → 暂停10分钟
                    if consecutive_failures >= 5:
                        logger.warning("[Noon] 连续5次空结果，暂停10分钟...")
                        consecutive_failures = 0
                        await asyncio.sleep(600)

                except Exception as e:
                    logger.warning(f"[Noon] '{kw}' 异常: {e}")
                    consecutive_failures += 1
                    if "browser" in str(e).lower() or "target closed" in str(e).lower():
                        logger.warning("[Noon] 浏览器崩溃，重启...")
                        break  # 跳出内循环，外循环重启浏览器

                i += 1

                # 进度日志
                if i % 50 == 0 or i == len(pending):
                    elapsed = time.time() - start_time
                    eta = (elapsed / i * (len(pending) - i)) / 60 if i > 0 else 0
                    logger.info(
                        f"[Noon] 进度: {i}/{len(pending)} "
                        f"(成功: {success}, 耗时: {elapsed/60:.1f}min, ETA: {eta:.0f}min)"
                    )

                await noon.random_delay()

        except Exception as e:
            logger.error(f"[Noon] 批次异常: {e}")
            ERRORS.append(f"Noon batch: {e}")
        finally:
            try:
                await noon.stop_browser()
            except Exception:
                pass

    logger.info(f"[Noon] 完成: {success}/{len(pending)} 成功")


async def _run_amazon_with_recovery(settings: Settings, keywords: list[str]):
    """Amazon 爬取，带浏览器崩溃恢复"""
    from scrapers.amazon_scraper import AmazonScraper
    logger.info(f"[Amazon] 开始爬取 {len(keywords)} 个关键词")

    consecutive_failures = 0
    amz = AmazonScraper(settings)
    pending = [kw for kw in keywords if not amz.is_completed(kw)]
    skipped = len(keywords) - len(pending)
    if skipped:
        logger.info(f"[Amazon] 跳过 {skipped} 个已完成, 剩余 {len(pending)} 个")

    if not pending:
        logger.info("[Amazon] 所有关键词已完成")
        return

    start_time = time.time()
    success = 0
    i = 0

    while i < len(pending):
        try:
            await amz.start_browser()
        except Exception as e:
            logger.error(f"[Amazon] 浏览器启动失败: {e}")
            ERRORS.append(f"Amazon browser start: {e}")
            await asyncio.sleep(60)
            continue

        try:
            while i < len(pending):
                kw = pending[i]
                try:
                    result = await amz.scrape_with_retry(kw)
                    if result and result.get("products"):
                        success += 1
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1

                    if consecutive_failures >= 5:
                        logger.warning("[Amazon] 连续5次空结果，暂停10分钟...")
                        consecutive_failures = 0
                        await asyncio.sleep(600)

                except Exception as e:
                    logger.warning(f"[Amazon] '{kw}' 异常: {e}")
                    consecutive_failures += 1
                    if "browser" in str(e).lower() or "target closed" in str(e).lower():
                        logger.warning("[Amazon] 浏览器崩溃，重启...")
                        break

                i += 1

                if i % 50 == 0 or i == len(pending):
                    elapsed = time.time() - start_time
                    eta = (elapsed / i * (len(pending) - i)) / 60 if i > 0 else 0
                    logger.info(
                        f"[Amazon] 进度: {i}/{len(pending)} "
                        f"(成功: {success}, 耗时: {elapsed/60:.1f}min, ETA: {eta:.0f}min)"
                    )

                await amz.random_delay()

        except Exception as e:
            logger.error(f"[Amazon] 批次异常: {e}")
            ERRORS.append(f"Amazon batch: {e}")
        finally:
            try:
                await amz.stop_browser()
            except Exception:
                pass

    logger.info(f"[Amazon] 完成: {success}/{len(pending)} 成功")


async def phase4_scrape(settings: Settings, keywords: list[str]):
    """Noon + Amazon 并行爬取"""
    logger.info("=" * 60)
    logger.info(f"  Phase 4: 并行爬取 ({len(keywords)} 关键词)")
    logger.info("=" * 60)

    # 确保 snapshot 子目录存在
    for sub in ["noon", "amazon", "processed"]:
        (settings.snapshot_dir / sub).mkdir(parents=True, exist_ok=True)

    start = time.time()

    # Noon 和 Amazon 并行
    await asyncio.gather(
        _run_noon_with_recovery(settings, keywords),
        _run_amazon_with_recovery(settings, keywords),
    )

    elapsed = time.time() - start
    logger.info(f"Phase 4 完成，总耗时: {elapsed/3600:.1f} 小时")


# ═══════════════════════════════════════════════════
# Phase 5: 分析出报告
# ═══════════════════════════════════════════════════
def phase5_analyze(settings: Settings, keywords: list[str]):
    """合并新旧数据，分析，生成报告"""
    logger.info("=" * 60)
    logger.info("  Phase 5: 数据分析 + 报告")
    logger.info("=" * 60)

    import pandas as pd
    from analysis.data_cleaner import merge_all
    from analysis.cross_validator import compute_cross_metrics
    from analysis.scoring_model import score_all
    from analysis.product_analyzer import analyze_products
    from output.excel_report import generate_report
    from config.category_mapping import map_category
    from config.cost_defaults import get_defaults

    snap = settings.snapshot_dir

    # 5a. 合并旧数据到当前 snapshot
    logger.info("--- 合并旧数据 ---")
    for platform in ["noon", "amazon"]:
        old_dir = OLD_SNAP / platform
        new_dir = snap / platform
        new_dir.mkdir(parents=True, exist_ok=True)
        if old_dir.exists():
            copied = 0
            for f in old_dir.glob("*.json"):
                dst = new_dir / f.name
                if not dst.exists():
                    dst.write_text(f.read_text(encoding="utf-8"), encoding="utf-8")
                    copied += 1
            logger.info(f"  {platform}: 从旧快照复制 {copied} 个文件")

    # 5b. 数据清洗合并
    logger.info("--- 数据清洗合并 ---")
    try:
        df = merge_all(snap)
        if df.empty:
            logger.error("无数据可分析！")
            ERRORS.append("Phase5: merge_all returned empty")
            return
        logger.info(f"  合并后: {len(df)} 行")
    except Exception as e:
        logger.error(f"数据清洗失败: {e}")
        ERRORS.append(f"Phase5 merge_all: {e}")
        traceback.print_exc()
        return

    # 5c. 品类映射 + 默认值
    logger.info("--- 品类映射 ---")
    try:
        df["v6_category"] = df.apply(
            lambda r: map_category(
                r.get("noon_category_path", ""),
                r.get("noon_category_tree") if isinstance(r.get("noon_category_tree"), list) else None,
            ),
            axis=1,
        )
        for idx, row in df.iterrows():
            defaults = get_defaults(row.get("v6_category", ""))
            df.at[idx, "default_length_cm"] = defaults["l"]
            df.at[idx, "default_width_cm"] = defaults["w"]
            df.at[idx, "default_height_cm"] = defaults["h"]
            df.at[idx, "default_weight_kg"] = defaults["wt"]
            df.at[idx, "cost_ratio"] = defaults["cost_ratio"]
    except Exception as e:
        logger.error(f"品类映射失败: {e}")
        ERRORS.append(f"Phase5 category: {e}")

    # 5d. 交叉比对
    logger.info("--- 交叉比对 ---")
    try:
        df = compute_cross_metrics(df)
    except Exception as e:
        logger.error(f"交叉比对失败: {e}")
        ERRORS.append(f"Phase5 cross: {e}")

    # 5e. 评分
    logger.info("--- 利润计算 + 评分 ---")
    try:
        df = score_all(df, settings)
        if "grade" in df.columns:
            grade_dist = df["grade"].value_counts().to_dict()
            logger.info(f"  评分分布: {grade_dist}")
    except Exception as e:
        logger.error(f"评分失败: {e}")
        ERRORS.append(f"Phase5 scoring: {e}")

    # 5f. 产品级分析
    logger.info("--- 产品级分析 ---")
    product_df = None
    try:
        product_df = analyze_products(snap, df, settings, top_n=100, output_n=200)
        logger.info(f"  推荐产品: {len(product_df)} 个")
    except Exception as e:
        logger.error(f"产品分析失败: {e}")
        ERRORS.append(f"Phase5 products: {e}")

    # 5g. 保存评分结果
    processed = snap / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(processed / "scored.parquet", index=False)
    except Exception:
        try:
            df.to_csv(processed / "scored.csv", index=False, encoding="utf-8-sig")
        except Exception:
            pass

    # 5h. Excel 报告
    logger.info("--- Excel报告 ---")
    try:
        report_path = snap / f"report_{settings.snapshot_id}.xlsx"
        generate_report(df, report_path, product_df=product_df)
        logger.info(f"  报告: {report_path}")
    except Exception as e:
        logger.error(f"报告生成失败: {e}")
        ERRORS.append(f"Phase5 report: {e}")

    # 5i. run_summary.json
    logger.info("--- 运行摘要 ---")
    noon_files = list((snap / "noon").glob("*.json")) if (snap / "noon").exists() else []
    amz_files = list((snap / "amazon").glob("*.json")) if (snap / "amazon").exists() else []

    summary = {
        "snapshot_id": settings.snapshot_id,
        "finished_at": datetime.now().isoformat(),
        "total_keywords": len(df) if df is not None else 0,
        "new_keywords_scraped": len(keywords),
        "platforms": {
            "noon": {"files": len(noon_files)},
            "amazon": {"files": len(amz_files)},
        },
        "analysis": {},
        "errors": ERRORS,
    }

    if df is not None and not df.empty and "grade" in df.columns:
        summary["analysis"]["grade_distribution"] = {
            g: int(c) for g, c in df["grade"].value_counts().items()
        }
        if "margin_war_pct" in df.columns:
            valid = df["margin_war_pct"].dropna()
            summary["analysis"]["margin_war_median"] = round(float(valid.median()), 2) if not valid.empty else None

    summary_path = snap / "run_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # ═══ 终端打印汇总 ═══
    print("\n" + "=" * 70)
    print("  全流程V2 最终汇总")
    print("=" * 70)
    print(f"  Snapshot: {settings.snapshot_id}")
    print(f"  总关键词: {len(df) if df is not None else 0}")
    print(f"  Noon 数据文件: {len(noon_files)}")
    print(f"  Amazon 数据文件: {len(amz_files)}")

    if df is not None and not df.empty:
        if "grade" in df.columns:
            print("\n  评分分布:")
            for grade in ["A", "B", "C", "D", "E"]:
                cnt = len(df[df["grade"] == grade])
                print(f"    {grade}级: {cnt} 个")

        if "margin_war_pct" in df.columns:
            valid = df["margin_war_pct"].dropna()
            if not valid.empty:
                print(f"\n  战时利润率中位数: {valid.median():.1f}%")
                print(f"  战时利润率 > 30%: {len(valid[valid > 30])} 个")

        # Top 10 赛道
        if "total_score" in df.columns and "keyword" in df.columns:
            top10 = df.nlargest(10, "total_score")
            print("\n  Top 10 赛道:")
            for _, row in top10.iterrows():
                margin = row.get("margin_war_pct", 0)
                margin_str = f"{margin:.1f}%" if pd.notna(margin) else "N/A"
                print(f"    {row.get('grade','?')} | {row['keyword']:<35} | 分数{row['total_score']:.1f} | 利润{margin_str}")

    if product_df is not None and not product_df.empty:
        print(f"\n  Top 10 产品:")
        for _, row in product_df.head(10).iterrows():
            margin = row.get("margin_war_pct", 0)
            margin_str = f"{margin:.1f}%" if pd.notna(margin) else "N/A"
            print(f"    {row.get('keyword',''):<25} | {row.get('title','')[:35]:<35} | {margin_str}")

    if ERRORS:
        print(f"\n  错误 ({len(ERRORS)}):")
        for e in ERRORS:
            print(f"    - {e}")

    print(f"\n  报告文件: {snap / f'report_{settings.snapshot_id}.xlsx'}")
    print(f"  摘要文件: {summary_path}")
    print("=" * 70)


async def main():
    started_at = datetime.now()
    settings = Settings()
    settings.set_runtime_scope("keyword")
    logger.info(f"全流程V2 启动 — Snapshot: {settings.snapshot_id}")
    logger.info(f"数据目录: {settings.snapshot_dir}")

    # Phase 3: 关键词拓展
    try:
        keywords = await phase3_expand_keywords(settings)
    except Exception as e:
        logger.error(f"Phase 3 失败: {e}")
        ERRORS.append(f"Phase3: {e}")
        traceback.print_exc()
        # Fallback: 仅用种子词
        keywords = ALL_SEEDS
        already_scraped = _get_already_scraped()
        keywords = sorted(set(keywords) - already_scraped)
        logger.info(f"Fallback到种子词: {len(keywords)} 个新词")

    if not keywords:
        logger.warning("没有新关键词需要爬取，直接进入分析阶段")
    else:
        # Phase 4: 爬取
        try:
            await phase4_scrape(settings, keywords)
        except Exception as e:
            logger.error(f"Phase 4 失败: {e}")
            ERRORS.append(f"Phase4: {e}")
            traceback.print_exc()

    # Phase 5: 分析
    try:
        phase5_analyze(settings, keywords)
    except Exception as e:
        logger.error(f"Phase 5 失败: {e}")
        ERRORS.append(f"Phase5: {e}")
        traceback.print_exc()

    elapsed = (datetime.now() - started_at).total_seconds()
    logger.info(f"全流程V2 完成，总耗时: {elapsed/3600:.1f} 小时")


if __name__ == "__main__":
    asyncio.run(main())
