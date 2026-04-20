"""
6维度加权评分模型

维度与权重:
  1. 需求强度   20% — Google趋势 + Amazon BSR
  2. 供给竞争度 20% — Noon商品数 + Express占比 + 卖家集中度
  3. 竞品质量   15% — 图片数 + 评论数 + Listing质量（越差=机会越大）
  4. 利润空间   25% — V6引擎战时利润率为主，和平时高出15pp+给加分
  5. 进入壁垒   10% — 认证要求、尺寸档位、退货风险
  6. 价格安全   10% — 跨平台价格比

动态分档：使用 P20/P40/P60/P80 百分位，不预设固定阈值。
"""
import logging

import pandas as pd

from analysis.dynamic_thresholds import compute_thresholds, score_value
from analysis.pricing_engine import calculate_ksa_fbn_profit
from config.settings import Settings

logger = logging.getLogger(__name__)

# 维度权重
WEIGHTS = {
    "demand": 0.20,
    "competition": 0.20,
    "competitor_quality": 0.15,
    "profit": 0.25,
    "barrier": 0.10,
    "price_safety": 0.10,
}

# 需要认证的品类关键词（壁垒评分用）
CERTIFICATION_KEYWORDS = {
    "cosmetic", "makeup", "skincare", "perfume", "fragrance",
    "food", "supplement", "vitamin", "health", "nutrition",
    "baby", "infant", "child", "toy",
    "electronics", "charger", "battery", "power bank",
    "medicine", "medical", "pharmaceutical",
}

# 尺寸档位壁垒分（越大越难）
SIZE_TIER_BARRIER = {
    "Small Envelope": 0,
    "Standard Envelope": 0,
    "Large Envelope": 0,
    "Standard Parcel": 1,
    "Oversize": 3,
    "Extra Oversize": 5,
    "Bulky": 7,
}


def compute_profit_columns(df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """
    对每行调用 V6 定价引擎计算利润。
    需要 noon_median_price + 品类映射 + 采购成本/尺寸数据。
    缺少尺寸或采购价的行留空。
    """
    profit_cols = [
        "margin_war_pct", "margin_peace_pct", "net_profit_war_cny",
        "net_profit_peace_cny", "size_tier", "fbn_fee_sar",
        "commission_sar", "commission_rate_pct", "vat_sar",
        "head_cost_war_cny", "head_cost_peace_cny",
        "total_cost_war_cny", "total_cost_peace_cny",
        "revenue_cny", "roi_war_pct", "roi_peace_pct",
    ]
    for col in profit_cols:
        if col not in df.columns:
            df[col] = None

    for idx, row in df.iterrows():
        price = row.get("noon_median_price")
        purchase = row.get("purchase_price_cny")
        length = row.get("length_cm")
        width = row.get("width_cm")
        height = row.get("height_cm")
        weight = row.get("weight_kg")
        category = row.get("v6_category", "其他通用 General/Other")

        # 模式2: 比例估算采购价
        if (purchase is None or pd.isna(purchase)) and price and price > 0:
            cost_ratio = row.get("cost_ratio", 0.30)
            purchase = price * settings.ksa_exchange_rate * cost_ratio

        # 模式2: 使用品类默认尺寸
        if any(v is None or (isinstance(v, float) and pd.isna(v))
               for v in [length, width, height, weight]):
            default_l = row.get("default_length_cm")
            default_w = row.get("default_width_cm")
            default_h = row.get("default_height_cm")
            default_wt = row.get("default_weight_kg")
            if all(v is not None and not (isinstance(v, float) and pd.isna(v))
                   for v in [default_l, default_w, default_h, default_wt]):
                length = default_l
                width = default_w
                height = default_h
                weight = default_wt

        # 必须有完整数据才能计算
        if not all(v is not None and not (isinstance(v, float) and pd.isna(v))
                   for v in [price, purchase, length, width, height, weight]):
            continue
        if price <= 0 or purchase <= 0:
            continue

        try:
            result = calculate_ksa_fbn_profit(
                purchase_price_cny=float(purchase),
                length_cm=float(length),
                width_cm=float(width),
                height_cm=float(height),
                weight_kg=float(weight),
                target_price_sar=float(price),
                category_name=category,
                shipping_rate_cbm=settings.shipping_rate_cbm,
                shipping_rate_cbm_peacetime=settings.shipping_rate_cbm_peacetime,
                exchange_rate=settings.ksa_exchange_rate,
                ad_rate=settings.default_ad_rate,
                return_rate=settings.default_return_rate,
            )
            for col in profit_cols:
                if col in result:
                    df.at[idx, col] = result[col]
        except Exception as e:
            logger.warning(f"[scoring] 定价计算失败 row={idx}: {e}")

    logger.info(f"[scoring] 利润计算完成，{df['margin_war_pct'].notna().sum()}/{len(df)} 行有利润数据")
    return df


def score_all(df: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    """
    对 DataFrame 中每行计算 6 维度评分 + 综合加权分。
    返回带评分列的 DataFrame。
    """
    # 先计算利润列（如果还没有）
    if "margin_war_pct" not in df.columns or df["margin_war_pct"].isna().all():
        df = compute_profit_columns(df, settings)

    # ═══ 1. 需求强度 (higher_is_better) ═══
    demand_th = compute_thresholds(df["demand_index"])
    df["score_demand"] = df["demand_index"].apply(
        lambda v: score_value(v, demand_th, higher_is_better=True)
    )

    # ═══ 2. 供给竞争度 (lower is better — 竞争越少机会越大) ═══
    # 用 competition_density: noon_total / google_interest，越小竞争越轻
    comp_th = compute_thresholds(df["competition_density"])
    df["score_competition"] = df["competition_density"].apply(
        lambda v: score_value(v, comp_th, higher_is_better=False)
    )

    # ═══ 3. 竞品质量 (lower is better — 竞品越差，机会越大) ═══
    cq_th = compute_thresholds(df["competitor_quality_index"])
    df["score_competitor_quality"] = df["competitor_quality_index"].apply(
        lambda v: score_value(v, cq_th, higher_is_better=False)
    )

    # ═══ 4. 利润空间 (higher_is_better) ═══
    margin_th = compute_thresholds(df["margin_war_pct"].dropna())
    df["score_profit"] = df.apply(
        lambda r: _score_profit(r, margin_th), axis=1
    )

    # ═══ 5. 进入壁垒 (lower is better) ═══
    df["barrier_raw"] = df.apply(_compute_barrier_raw, axis=1)
    barrier_th = compute_thresholds(df["barrier_raw"])
    df["score_barrier"] = df["barrier_raw"].apply(
        lambda v: score_value(v, barrier_th, higher_is_better=False)
    )

    # ═══ 6. 价格安全 (higher is better — noon价越高于地板越安全) ═══
    ps_th = compute_thresholds(df["price_safety_ratio"])
    df["score_price_safety"] = df["price_safety_ratio"].apply(
        lambda v: score_value(v, ps_th, higher_is_better=True)
    )

    # ═══ 综合加权分 ═══
    df["total_score"] = (
        df["score_demand"] * WEIGHTS["demand"]
        + df["score_competition"] * WEIGHTS["competition"]
        + df["score_competitor_quality"] * WEIGHTS["competitor_quality"]
        + df["score_profit"] * WEIGHTS["profit"]
        + df["score_barrier"] * WEIGHTS["barrier"]
        + df["score_price_safety"] * WEIGHTS["price_safety"]
    )

    # 排名
    df["rank"] = df["total_score"].rank(ascending=False, method="min").astype(int)

    # 标签
    df["grade"] = df["total_score"].apply(_grade)

    logger.info(f"[scoring] 评分完成: A={len(df[df['grade']=='A'])} B={len(df[df['grade']=='B'])} "
                f"C={len(df[df['grade']=='C'])} D={len(df[df['grade']=='D'])} E={len(df[df['grade']=='E'])}")
    return df


def _score_profit(row, margin_th: dict) -> int:
    """利润维度评分：战时利润率为主，和平时高出15pp+给+1潜力加分"""
    margin_war = row.get("margin_war_pct")
    margin_peace = row.get("margin_peace_pct")

    if margin_war is None or pd.isna(margin_war):
        return 5  # 无数据给中间分

    base = score_value(float(margin_war), margin_th, higher_is_better=True)

    # 和平潜力加分：和平利润率比战时高15pp以上，+1分（上限10）
    if (margin_peace is not None and not pd.isna(margin_peace)
            and float(margin_peace) - float(margin_war) > 15):
        base = min(10, base + 1)

    return base


def _compute_barrier_raw(row) -> float:
    """
    计算进入壁垒原始分（越高壁垒越大）。
    组成：认证需求 + 尺寸档位 + 退货风险
    """
    score = 0.0

    # 认证需求：关键词或品类名包含敏感词 → +3
    keyword = str(row.get("keyword", "")).lower()
    category = str(row.get("v6_category", "")).lower()
    combined = keyword + " " + category
    if any(kw in combined for kw in CERTIFICATION_KEYWORDS):
        score += 3

    # 尺寸档位
    tier = row.get("size_tier", "Standard Parcel")
    if isinstance(tier, str):
        score += SIZE_TIER_BARRIER.get(tier, 1)

    # 退货风险：服装鞋履品类退货率高
    if any(w in combined for w in ["apparel", "clothing", "shoes", "footwear", "服装", "鞋"]):
        score += 2

    return score


def _grade(total_score: float) -> str:
    """综合分 → 等级标签"""
    if total_score >= 8:
        return "A"
    elif total_score >= 6.5:
        return "B"
    elif total_score >= 5:
        return "C"
    elif total_score >= 3.5:
        return "D"
    else:
        return "E"
