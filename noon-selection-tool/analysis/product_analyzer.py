"""
产品级别分析 — 从赛道聚合下沉到单品维度

对每个关键词的30条商品逐条分析，识别三类机会：
1. 潜力产品：高销量/高评论 + 竞品Listing质量差 → 做更好的Listing抢份额
2. 蓝海产品：评论<50 + 品类需求确认 → 新进入者有机会
3. 利润优选：V6精算净利率最高的商品
"""
import json
import logging
from pathlib import Path

import pandas as pd

from analysis.pricing_engine import calculate_ksa_fbn_profit
from config.category_mapping import map_category
from config.cost_defaults import get_defaults
from config.settings import Settings

logger = logging.getLogger(__name__)

# 搜索相关性停用词
_STOP_WORDS = {"the", "a", "an", "for", "with", "and", "of", "in", "on", "to"}


def _is_relevant(keyword: str, product_title: str) -> bool:
    """
    检查商品标题是否与搜索关键词相关。
    策略:
    1. 完整短语出现在标题中 → 相关
    2. 单词关键词：该词出现在标题中 → 相关
    3. 多词关键词：所有核心词在标题中**近距离**出现（间隔≤5词） → 相关
       如果词都出现但距离远（如 "Under Bed Storage... No Box Spring"）→ 不相关
    """
    if not keyword or not product_title:
        return True

    kw_lower = keyword.lower()
    title_lower = product_title.lower()

    # 完整短语匹配
    if kw_lower in title_lower:
        return True

    kw_core = list(set(kw_lower.split()) - _STOP_WORDS)
    if not kw_core:
        return True

    # 单词关键词
    if len(kw_core) == 1:
        return kw_core[0] in title_lower

    # 多词关键词：所有词必须出现
    positions = {}
    title_words = title_lower.split()
    for w in kw_core:
        found = [i for i, tw in enumerate(title_words) if w in tw]
        if not found:
            return False
        positions[w] = found

    # 检查是否有一组位置使所有核心词在2词跨度内（紧邻或只隔1词）
    from itertools import product as iproduct
    for combo in iproduct(*[positions[w] for w in kw_core]):
        span = max(combo) - min(combo)
        if span <= 2:
            return True

    return False


def analyze_products(
    data_dir: Path,
    scored_df: pd.DataFrame,
    settings: Settings,
    top_n: int = 50,
    output_n: int = 100,
) -> pd.DataFrame:
    """
    产品级别分析。

    参数:
        data_dir: 数据目录
        scored_df: 赛道级评分 DataFrame（含 rank, grade, v6_category 等）
        settings: 全局配置
        top_n: 取排名前 N 的关键词做产品分析
        output_n: 最终输出 Top N 条推荐产品

    返回:
        产品级 DataFrame，每行一个商品，按利润率降序
    """
    # 取 Top N 关键词
    if "rank" in scored_df.columns:
        top_keywords = scored_df.nsmallest(top_n, "rank")
    else:
        top_keywords = scored_df.head(top_n)

    kw_info = {}
    for _, row in top_keywords.iterrows():
        kw_info[row["keyword"]] = {
            "grade": row.get("grade", "?"),
            "rank": int(row.get("rank", 0)),
            "v6_category": row.get("v6_category", "其他通用 General/Other"),
            "noon_total": row.get("noon_total", 0),
            "noon_avg_reviews": row.get("noon_avg_reviews", 0),
            "noon_median_price": row.get("noon_median_price", 0),
        }

    # 加载原始商品数据（兼容 snapshot_dir 和旧 data_dir）
    noon_dir = data_dir / "noon"
    if not noon_dir.exists():
        noon_dir = data_dir / "raw" / "noon"
    all_products = []

    for kw, info in kw_info.items():
        safe_name = kw.replace(" ", "_").replace("/", "_")
        json_path = noon_dir / f"{safe_name}.json"
        if not json_path.exists():
            continue

        rec = json.loads(json_path.read_text(encoding="utf-8"))
        products = rec.get("products", [])
        v6_cat = info["v6_category"]
        defaults = get_defaults(v6_cat)

        for p in products:
            price = p.get("price", 0)
            if not price or price <= 0:
                continue

            # V6 利润精算（用品类默认尺寸 + 售价比例估算采购价）
            purchase_cny = price * settings.ksa_exchange_rate * defaults["cost_ratio"]
            profit_result = _calc_profit(
                purchase_cny, defaults, price, v6_cat, settings
            )

            title = p.get("title", "")
            relevant = _is_relevant(kw, title)

            is_ad = p.get("is_ad", False)
            all_products.append({
                # 赛道信息
                "keyword": kw,
                "keyword_rank": info["rank"],
                "keyword_grade": info["grade"],
                "v6_category": v6_cat,
                "noon_total": info["noon_total"],
                # 相关性
                "relevance_flag": "广告位排名" if is_ad else ("相关" if relevant else "不相关"),
                # 商品信息
                "title": title,
                "product_id": p.get("product_id", ""),
                "product_url": p.get("product_url", ""),
                "price_sar": price,
                "original_price_sar": p.get("original_price"),
                "review_count": p.get("review_count", 0) or 0,
                "rating": p.get("rating"),
                "is_express": p.get("is_express", False),
                "is_bestseller": p.get("is_bestseller", False),
                "is_ad": is_ad,
                "image_count": p.get("image_count", 0) or 0,
                "seller_name": p.get("seller_name", ""),
                "sold_recently": p.get("sold_recently", ""),
                # 利润数据
                **profit_result,
            })

    if not all_products:
        logger.warning("[product_analyzer] 无商品数据")
        return pd.DataFrame()

    df = pd.DataFrame(all_products)

    # ── 机会类型识别 ──
    df["opportunity_type"] = df.apply(
        lambda r: _classify_opportunity(r, kw_info), axis=1
    )

    # 相关产品优先，再按利润率降序
    df["_relevant_sort"] = (df["relevance_flag"] == "不相关").astype(int)
    df = df.sort_values(
        ["_relevant_sort", "margin_war_pct"],
        ascending=[True, False],
        na_position="last",
    )
    df = df.drop(columns=["_relevant_sort"])

    # 输出 Top N
    df = df.head(output_n).reset_index(drop=True)
    df["product_rank"] = range(1, len(df) + 1)

    logger.info(
        f"[product_analyzer] 分析完成: {len(all_products)} 条商品 → Top {len(df)} 推荐"
    )

    # 统计相关性
    rel_counts = df["relevance_flag"].value_counts()
    for flag, cnt in rel_counts.items():
        logger.info(f"  {flag}: {cnt} 条")

    # 统计机会类型分布
    type_counts = df["opportunity_type"].value_counts()
    for t, c in type_counts.items():
        logger.info(f"  {t}: {c} 条")

    return df


def _calc_profit(
    purchase_cny: float,
    defaults: dict,
    price_sar: float,
    category: str,
    settings: Settings,
) -> dict:
    """对单个商品调用 V6 引擎计算利润"""
    try:
        result = calculate_ksa_fbn_profit(
            purchase_price_cny=purchase_cny,
            length_cm=defaults["l"],
            width_cm=defaults["w"],
            height_cm=defaults["h"],
            weight_kg=defaults["wt"],
            target_price_sar=price_sar,
            category_name=category,
            shipping_rate_cbm=settings.shipping_rate_cbm,
            shipping_rate_cbm_peacetime=settings.shipping_rate_cbm_peacetime,
            exchange_rate=settings.ksa_exchange_rate,
            ad_rate=settings.default_ad_rate,
            return_rate=settings.default_return_rate,
        )
        return {
            "purchase_est_cny": round(purchase_cny, 1),
            "margin_war_pct": result["margin_war_pct"],
            "margin_peace_pct": result["margin_peace_pct"],
            "net_profit_war_cny": result["net_profit_war_cny"],
            "fbn_fee_sar": result["fbn_fee_sar"],
            "commission_pct": result["commission_rate_pct"],
            "size_tier": result["size_tier"],
        }
    except Exception:
        return {
            "purchase_est_cny": round(purchase_cny, 1),
            "margin_war_pct": None,
            "margin_peace_pct": None,
            "net_profit_war_cny": None,
            "fbn_fee_sar": None,
            "commission_pct": None,
            "size_tier": None,
        }


def _classify_opportunity(row, kw_info: dict) -> str:
    """
    识别机会类型（可叠加，用 | 分隔）：
    - 潜力产品：高销量/高评论 + Listing质量差
    - 蓝海产品：评论<50 + 品类需求确认
    - 利润优选：利润率>20%
    """
    tags = []
    reviews = row.get("review_count", 0) or 0
    rating = row.get("rating") or 0
    images = row.get("image_count", 0) or 0
    margin = row.get("margin_war_pct")
    sold = row.get("sold_recently", "")

    # 解析销量
    sold_num = 0
    if sold:
        try:
            sold_num = int(str(sold).replace(",", "").replace("+", ""))
        except (ValueError, TypeError):
            pass

    # 潜力产品：有销量信号 + 但 Listing 质量可改进
    has_demand = reviews >= 20 or sold_num >= 50
    poor_listing = images <= 3 or (rating > 0 and rating < 4.3)
    if has_demand and poor_listing:
        tags.append("潜力产品")

    # 蓝海产品：低评论 + 赛道需求已确认
    kw = row.get("keyword", "")
    kw_data = kw_info.get(kw, {})
    category_has_demand = kw_data.get("noon_total", 0) >= 1000
    if reviews < 50 and category_has_demand:
        tags.append("蓝海产品")

    # 利润优选
    if margin is not None and margin > 20:
        tags.append("利润优选")

    return " | ".join(tags) if tags else "常规"
