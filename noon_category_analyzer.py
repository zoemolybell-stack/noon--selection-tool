#!/usr/bin/env python3
"""
Noon KSA 品类深潜分析器 — 基于监控报告生成 Markdown 分析报告

Usage:
    python noon_category_analyzer.py --report report_automotive.xlsx --category "Dash Cameras" --output analysis_dash_cameras.md
"""

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ── 导入定价引擎 ──
sys.path.insert(0, "D:/claude noon/noon-selection-tool")
from analysis.pricing_engine import calculate_ksa_fbn_profit
from config.cost_defaults import get_defaults
from config.category_mapping import map_category


# ═══════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════

def slugify(text: str) -> str:
    """将品类名转为文件名友好的 slug"""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s]+", "_", text)
    return text


def fuzzy_match_category(target: str, available: list[str]) -> str | None:
    """模糊匹配品类名：先精确，再大小写不敏感，再子串匹配"""
    if target in available:
        return target
    target_lower = target.lower()
    for cat in available:
        if cat.lower() == target_lower:
            return cat
    matches = [cat for cat in available if target_lower in cat.lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # 取最短的（最精确的匹配）
        return min(matches, key=len)
    # 反向匹配：品类名包含在搜索词中
    matches = [cat for cat in available if cat.lower() in target_lower]
    if matches:
        return max(matches, key=len)
    return None


def safe_median(series: pd.Series) -> float:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    return float(vals.median()) if len(vals) > 0 else 0.0


def safe_mean(series: pd.Series) -> float:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    return float(vals.mean()) if len(vals) > 0 else 0.0


def pct(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0.0%"
    return f"{numerator / denominator * 100:.1f}%"


def fmt_sar(val: float) -> str:
    return f"{val:.2f} SAR"


def fmt_cny(val: float) -> str:
    return f"{val:.2f} CNY"


def fmt_pct(val: float) -> str:
    return f"{val:.1f}%"


# ═══════════════════════════════════════════════════
# 价格分段
# ═══════════════════════════════════════════════════

PRICE_TIERS = [
    ("Budget (0-25)", 0, 25),
    ("Low (25-50)", 25, 50),
    ("Mid (50-100)", 50, 100),
    ("High (100-200)", 100, 200),
    ("Premium (200+)", 200, float("inf")),
]


def classify_price_tier(price: float) -> str:
    for label, lo, hi in PRICE_TIERS:
        if lo <= price < hi:
            return label
    return PRICE_TIERS[-1][0]


# ═══════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════

def load_data(report_path: str, category: str):
    """加载并过滤数据，返回 (df_cat, df_overview_row, df_amazon, category_name)"""
    report = Path(report_path)
    if not report.exists():
        print(f"[ERROR] 报告文件不存在: {report}")
        sys.exit(1)

    # 加载 All Products
    df_all = pd.read_excel(report, sheet_name="All Products")

    # 确定子品类字段名
    subcat_col = None
    for col in ["_subcategory", "subcategory", "Subcategory"]:
        if col in df_all.columns:
            subcat_col = col
            break
    if subcat_col is None:
        print(f"[ERROR] 找不到子品类列。可用列: {list(df_all.columns)}")
        sys.exit(1)

    available_cats = sorted(df_all[subcat_col].dropna().unique().tolist())

    matched = fuzzy_match_category(category, available_cats)
    if matched is None:
        print(f"[ERROR] 找不到品类 '{category}'")
        print(f"  可用品类（共 {len(available_cats)} 个）:")
        # 展示包含搜索词的候选
        suggestions = [c for c in available_cats if category.lower() in c.lower()]
        if suggestions:
            for s in suggestions[:20]:
                print(f"    - {s}")
        else:
            for s in available_cats[:30]:
                print(f"    - {s}")
            if len(available_cats) > 30:
                print(f"    ... 共 {len(available_cats)} 个")
        sys.exit(1)

    print(f"[INFO] 匹配品类: {matched}")
    df_cat = df_all[df_all[subcat_col] == matched].copy()

    if len(df_cat) == 0:
        print(f"[ERROR] 品类 '{matched}' 无数据")
        sys.exit(1)

    # 加载 Sub-Category Overview（可选）
    df_overview_row = None
    try:
        df_overview = pd.read_excel(report, sheet_name="Sub-Category Overview")
        ov_col = None
        for col in ["Subcategory", "subcategory", "_subcategory"]:
            if col in df_overview.columns:
                ov_col = col
                break
        if ov_col:
            rows = df_overview[df_overview[ov_col] == matched]
            if len(rows) > 0:
                df_overview_row = rows.iloc[0]
    except Exception:
        pass

    # 加载 Amazon Cross Compare（可选）
    df_amazon = None
    try:
        df_amz = pd.read_excel(report, sheet_name="Amazon Cross Compare")
        # 匹配品类中的产品
        if "noon_product_id" in df_amz.columns and "product_id" in df_cat.columns:
            cat_ids = set(df_cat["product_id"].dropna().astype(str))
            df_amazon = df_amz[df_amz["noon_product_id"].astype(str).isin(cat_ids)].copy()
            if len(df_amazon) == 0:
                df_amazon = None
    except Exception:
        pass

    return df_cat, df_overview_row, df_amazon, matched


# ═══════════════════════════════════════════════════
# 分析模块
# ═══════════════════════════════════════════════════

def section_overview(df: pd.DataFrame, overview_row, cat_name: str) -> str:
    """## 1. 品类概览"""
    n_total = len(df)

    # Express
    express_col = "is_express"
    n_express = int(df[express_col].sum()) if express_col in df.columns else 0

    # 价格
    prices = pd.to_numeric(df["price"], errors="coerce").dropna()
    median_price = float(prices.median()) if len(prices) > 0 else 0
    avg_price = float(prices.mean()) if len(prices) > 0 else 0
    min_price = float(prices.min()) if len(prices) > 0 else 0
    max_price = float(prices.max()) if len(prices) > 0 else 0

    # 利润率
    margin_col = None
    for col in ["margin_war_pct", "margin_peace_pct", "Median Margin %"]:
        if col in df.columns:
            margin_col = col
            break

    if margin_col:
        margins = pd.to_numeric(df[margin_col], errors="coerce").dropna()
        median_margin = float(margins.median()) if len(margins) > 0 else 0
        n_positive = int((margins > 0).sum())
    else:
        median_margin = 0
        n_positive = 0
        margins = pd.Series(dtype=float)

    # 销量
    sold_col = "sold_recently"
    if sold_col in df.columns:
        sold = pd.to_numeric(df[sold_col], errors="coerce").fillna(0)
        n_with_sales = int((sold > 0).sum())
        total_sold = int(sold.sum())
    else:
        n_with_sales = 0
        total_sold = 0

    lines = [
        f"## 1. 品类概览\n",
        f"| 指标 | 数值 |",
        f"|---|---|",
        f"| 产品总数 | {n_total} |",
        f"| Express 产品数 | {n_express} ({pct(n_express, n_total)}) |",
        f"| 中位价格 | {fmt_sar(median_price)} |",
        f"| 平均价格 | {fmt_sar(avg_price)} |",
        f"| 价格范围 | {fmt_sar(min_price)} ~ {fmt_sar(max_price)} |",
    ]

    if margin_col and len(margins) > 0:
        lines.append(f"| 中位利润率 (战时) | {fmt_pct(median_margin)} |")
        lines.append(f"| 正利润产品占比 | {n_positive}/{len(margins)} ({pct(n_positive, len(margins))}) |")

    lines.append(f"| 有销量产品数 | {n_with_sales} ({pct(n_with_sales, n_total)}) |")
    lines.append(f"| 总销量估算 | {total_sold} |")
    lines.append("")

    return "\n".join(lines)


def section_competition(df: pd.DataFrame) -> str:
    """## 2. 竞争格局分析"""
    n_total = len(df)
    lines = ["## 2. 竞争格局分析\n"]

    # ── 2.1 竞争密度 ──
    lines.append("### 2.1 竞争密度\n")

    # Express 占比
    n_express = int(df["is_express"].sum()) if "is_express" in df.columns else 0
    express_ratio = n_express / n_total if n_total > 0 else 0
    if express_ratio > 0.6:
        express_judge = "高度竞争 — Express 卖家占主导地位，新进入者需开通 FBN/FBP"
    elif express_ratio > 0.3:
        express_judge = "中等竞争 — Express 有一定渗透率但未完全覆盖"
    else:
        express_judge = "低竞争密度 — Express 渗透率较低，非 Express 卖家仍有机会"

    lines.append(f"- **Express 卖家占比**: {n_express}/{n_total} ({pct(n_express, n_total)}) — {express_judge}")

    # 广告密度
    if "is_ad" in df.columns:
        n_ad = int(df["is_ad"].sum())
        ad_ratio = n_ad / n_total if n_total > 0 else 0
        if ad_ratio > 0.15:
            ad_judge = "广告竞争激烈，需预留较高广告预算"
        elif ad_ratio > 0.05:
            ad_judge = "广告投放适中"
        else:
            ad_judge = "广告竞争低，自然流量为主"
        lines.append(f"- **广告投放密度**: {n_ad}/{n_total} ({pct(n_ad, n_total)}) — {ad_judge}")

    # 评论壁垒
    if "review_count" in df.columns:
        reviews = pd.to_numeric(df["review_count"], errors="coerce").dropna()
        if len(reviews) > 0:
            avg_rev = float(reviews.mean())
            median_rev = float(reviews.median())
            p75_rev = float(reviews.quantile(0.75))
            n_zero_rev = int((reviews == 0).sum())
            if avg_rev > 100:
                rev_judge = "高评论壁垒，头部产品积累深厚，新品需差异化突围"
            elif avg_rev > 20:
                rev_judge = "中等壁垒，通过持续运营可追赶"
            else:
                rev_judge = "低壁垒，品类整体评论数少，新品有机会快速上位"
            lines.append(f"- **评论壁垒**: 平均 {avg_rev:.0f} 条, 中位 {median_rev:.0f} 条, 75分位 {p75_rev:.0f} 条 — {rev_judge}")
            lines.append(f"- **零评论产品**: {n_zero_rev}/{n_total} ({pct(n_zero_rev, n_total)})")

    # 评分
    if "rating" in df.columns:
        ratings = pd.to_numeric(df["rating"], errors="coerce").dropna()
        ratings_valid = ratings[ratings > 0]
        if len(ratings_valid) > 0:
            lines.append(f"- **平均评分**: {float(ratings_valid.mean()):.2f} (有评分产品 {len(ratings_valid)} 个)")

    lines.append("")

    # ── 2.2 价格分布 ──
    lines.append("### 2.2 价格分布\n")
    prices = pd.to_numeric(df["price"], errors="coerce").dropna()
    sold = pd.to_numeric(df.get("sold_recently", pd.Series(dtype=float)), errors="coerce").fillna(0)

    lines.append("| 价格段 | 产品数 | 占比 | 有销量产品 | 销量合计 | 中位利润率 |")
    lines.append("|---|---|---|---|---|---|")

    sweet_spot = None
    max_sold_tier = 0

    for label, lo, hi in PRICE_TIERS:
        if hi == float("inf"):
            mask = prices >= lo
        else:
            mask = (prices >= lo) & (prices < hi)
        idx = mask[mask].index
        n_tier = len(idx)
        tier_pct = pct(n_tier, len(prices))

        # 销量
        tier_sold = sold.reindex(idx).fillna(0)
        n_with_sold = int((tier_sold > 0).sum())
        total_tier_sold = int(tier_sold.sum())

        # 利润
        margin_col = "margin_war_pct" if "margin_war_pct" in df.columns else None
        if margin_col:
            tier_margins = pd.to_numeric(df.loc[idx, margin_col], errors="coerce").dropna()
            med_margin = fmt_pct(float(tier_margins.median())) if len(tier_margins) > 0 else "N/A"
        else:
            med_margin = "N/A"

        lines.append(f"| {label} | {n_tier} | {tier_pct} | {n_with_sold} | {total_tier_sold} | {med_margin} |")

        if total_tier_sold > max_sold_tier:
            max_sold_tier = total_tier_sold
            sweet_spot = label

    lines.append("")
    if sweet_spot:
        lines.append(f"> **价格甜蜜区**: {sweet_spot} — 该价格段销量最集中，建议优先切入。\n")
    else:
        lines.append("> **价格甜蜜区**: 数据不足，建议参考中位价格定位。\n")

    # ── 2.3 头部产品分析 ──
    lines.append("### 2.3 头部产品分析\n")

    display_cols_map = {
        "title": "标题",
        "price": "价格(SAR)",
        "review_count": "评论数",
        "is_express": "Express",
        "sold_recently": "近期销量",
        "margin_war_pct": "利润率%",
    }

    # Top 10 by sales
    if "sold_recently" in df.columns:
        lines.append("#### Top 10 — 按近期销量\n")
        top_sold = df.nlargest(10, "sold_recently")
        lines.append(_product_table(top_sold, display_cols_map))
        lines.append("")

    # Top 10 by reviews
    if "review_count" in df.columns:
        lines.append("#### Top 10 — 按评论数\n")
        top_rev = df.nlargest(10, "review_count")
        lines.append(_product_table(top_rev, display_cols_map))
        lines.append("")

    # 头部共性总结
    lines.append("#### 头部产品共性总结\n")
    top_products = pd.DataFrame()
    if "sold_recently" in df.columns:
        top_products = df.nlargest(min(20, len(df)), "sold_recently")
    elif "review_count" in df.columns:
        top_products = df.nlargest(min(20, len(df)), "review_count")

    if len(top_products) > 0:
        tp_prices = pd.to_numeric(top_products["price"], errors="coerce").dropna()
        tp_express = int(top_products["is_express"].sum()) if "is_express" in top_products.columns else 0
        tp_bestseller = int(top_products["is_bestseller"].sum()) if "is_bestseller" in top_products.columns else 0

        lines.append(f"- 头部 Top {len(top_products)} 产品价格范围: {fmt_sar(float(tp_prices.min()))} ~ {fmt_sar(float(tp_prices.max()))}, 中位 {fmt_sar(float(tp_prices.median()))}")
        lines.append(f"- Express 占比: {tp_express}/{len(top_products)} ({pct(tp_express, len(top_products))})")
        if tp_bestseller > 0:
            lines.append(f"- Bestseller 标签: {tp_bestseller}/{len(top_products)}")

        if "image_count" in top_products.columns:
            img = pd.to_numeric(top_products["image_count"], errors="coerce").dropna()
            if len(img) > 0:
                lines.append(f"- 平均图片数: {float(img.mean()):.1f}")

        if "rating" in top_products.columns:
            ratings = pd.to_numeric(top_products["rating"], errors="coerce").dropna()
            ratings_valid = ratings[ratings > 0]
            if len(ratings_valid) > 0:
                lines.append(f"- 平均评分: {float(ratings_valid.mean()):.2f}")
    lines.append("")

    return "\n".join(lines)


def _product_table(df_slice: pd.DataFrame, cols_map: dict) -> str:
    """生成产品表格的 Markdown"""
    available = [c for c in cols_map if c in df_slice.columns]
    headers = [cols_map[c] for c in available]

    rows = ["| " + " | ".join(headers) + " |"]
    rows.append("|" + "|".join(["---"] * len(headers)) + "|")

    for _, row in df_slice.iterrows():
        cells = []
        for col in available:
            val = row[col]
            if col == "title":
                title_str = str(val)[:60]
                # 添加 Noon 链接
                if "product_url" in df_slice.columns and pd.notna(row.get("product_url")):
                    title_str = f"[{title_str}]({row['product_url']})"
                elif "product_id" in df_slice.columns and pd.notna(row.get("product_id")):
                    title_str = f"[{title_str}](https://www.noon.com/product/{row['product_id']})"
                cells.append(title_str)
            elif col == "price":
                cells.append(f"{float(val):.2f}" if pd.notna(val) else "N/A")
            elif col == "is_express":
                cells.append("Yes" if val else "No")
            elif col in ("margin_war_pct", "margin_peace_pct"):
                cells.append(f"{float(val):.1f}%" if pd.notna(val) else "N/A")
            elif col in ("review_count", "sold_recently"):
                cells.append(str(int(val)) if pd.notna(val) else "0")
            else:
                cells.append(str(val) if pd.notna(val) else "N/A")
        rows.append("| " + " | ".join(cells) + " |")

    return "\n".join(rows)


def section_profit_simulation(df: pd.DataFrame, cat_name: str) -> str:
    """## 3. 利润测算"""
    lines = ["## 3. 利润测算\n"]

    # 确定 V6 品类
    v6_cat = None
    if "v6_category" in df.columns:
        v6_cats = df["v6_category"].dropna()
        if len(v6_cats) > 0:
            v6_cat = v6_cats.mode().iloc[0]

    if v6_cat is None:
        v6_cat = map_category(cat_name)

    defaults = get_defaults(v6_cat)
    lines.append(f"- **V6 品类映射**: {v6_cat}")
    lines.append(f"- **默认尺寸**: {defaults['l']}x{defaults['w']}x{defaults['h']} cm, 重量 {defaults['wt']} kg")
    lines.append(f"- **默认采购成本比**: {defaults['cost_ratio']*100:.0f}%\n")

    # 选5个代表性价格点
    prices = pd.to_numeric(df["price"], errors="coerce").dropna()
    if len(prices) == 0:
        lines.append("> 无有效价格数据，跳过利润测算。\n")
        return "\n".join(lines)

    p10 = float(prices.quantile(0.10))
    p25 = float(prices.quantile(0.25))
    p50 = float(prices.quantile(0.50))
    p75 = float(prices.quantile(0.75))
    p90 = float(prices.quantile(0.90))

    price_points = [
        (f"P10 ({fmt_sar(p10)})", p10),
        (f"P25 ({fmt_sar(p25)})", p25),
        (f"P50/中位 ({fmt_sar(p50)})", p50),
        (f"P75 ({fmt_sar(p75)})", p75),
        (f"P90 ({fmt_sar(p90)})", p90),
    ]

    # 去重（如果多个分位数相同）
    seen = set()
    unique_points = []
    for label, p in price_points:
        rounded = round(p, 1)
        if rounded not in seen and p > 0:
            seen.add(rounded)
            unique_points.append((label, p))

    lines.append("### 3.1 不同价格段的利润模拟\n")
    lines.append(f"参数假设: 海运战时 2000 CNY/CBM, 和平时 950 CNY/CBM, 汇率 1.90, 广告 5%, 退货 3%\n")

    headers = ["价格点", "采购价(CNY)", "佣金率", "FBN费(SAR)", "战时利润(CNY)", "战时利润率", "和平利润(CNY)", "和平利润率", "ROI(和平)"]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")

    best_margin = -999
    best_price_label = ""

    for label, price_sar in unique_points:
        cost_ratio = defaults["cost_ratio"]
        purchase_cny = price_sar * 1.90 * cost_ratio  # 售价(SAR)*汇率*成本比

        result = calculate_ksa_fbn_profit(
            purchase_price_cny=purchase_cny,
            length_cm=defaults["l"],
            width_cm=defaults["w"],
            height_cm=defaults["h"],
            weight_kg=defaults["wt"],
            target_price_sar=price_sar,
            category_name=v6_cat,
            shipping_rate_cbm=2000,
            shipping_rate_cbm_peacetime=950,
            exchange_rate=1.90,
            ad_rate=0.05,
            return_rate=0.03,
        )

        cells = [
            label,
            f"{purchase_cny:.1f}",
            f"{result['commission_rate_pct']:.1f}%",
            f"{result['fbn_fee_sar']:.2f}",
            f"{result['net_profit_war_cny']:.1f}",
            f"{result['margin_war_pct']:.1f}%",
            f"{result['net_profit_peace_cny']:.1f}",
            f"{result['margin_peace_pct']:.1f}%",
            f"{result['roi_peace_pct']:.1f}%",
        ]
        lines.append("| " + " | ".join(cells) + " |")

        if result["margin_peace_pct"] > best_margin:
            best_margin = result["margin_peace_pct"]
            best_price_label = label

    lines.append("")

    # 3.2 最优定价建议
    lines.append("### 3.2 最优定价区间建议\n")
    if best_margin > 0:
        lines.append(f"- **利润率最优价格点**: {best_price_label}, 和平时期利润率 {fmt_pct(best_margin)}")
    else:
        lines.append(f"- 当前默认参数下所有价格点利润率均为负，需降低采购成本或优化物流成本")

    # 找销量集中的价格段的利润
    if "sold_recently" in df.columns and "price" in df.columns:
        sold = pd.to_numeric(df["sold_recently"], errors="coerce").fillna(0)
        has_sold = df[sold > 0].copy()
        if len(has_sold) > 0:
            sold_prices = pd.to_numeric(has_sold["price"], errors="coerce").dropna()
            if len(sold_prices) > 0:
                sold_median = float(sold_prices.median())
                lines.append(f"- **有销量产品中位价格**: {fmt_sar(sold_median)} — 此为市场验证过的价格带")

    lines.append(f"- **建议**: 综合利润率与市场接受度，建议定价在 {fmt_sar(p25)} ~ {fmt_sar(p75)} 区间测试")
    lines.append("")

    return "\n".join(lines)


def section_strategy(df: pd.DataFrame, cat_name: str) -> str:
    """## 4. 进入策略建议"""
    lines = ["## 4. 进入策略建议\n"]
    n_total = len(df)

    # 基础数据提取
    n_express = int(df["is_express"].sum()) if "is_express" in df.columns else 0
    express_ratio = n_express / n_total if n_total > 0 else 0

    prices = pd.to_numeric(df["price"], errors="coerce").dropna()
    median_price = float(prices.median()) if len(prices) > 0 else 0

    reviews = pd.to_numeric(df.get("review_count", pd.Series(dtype=float)), errors="coerce").dropna()
    avg_reviews = float(reviews.mean()) if len(reviews) > 0 else 0

    margins = pd.to_numeric(df.get("margin_war_pct", pd.Series(dtype=float)), errors="coerce").dropna()
    median_margin = float(margins.median()) if len(margins) > 0 else 0

    sold = pd.to_numeric(df.get("sold_recently", pd.Series(dtype=float)), errors="coerce").fillna(0)
    total_sold = int(sold.sum())

    # 4.1 履约模式
    lines.append("### 4.1 推荐的履约模式\n")
    if median_price < 30:
        lines.append("- **推荐 FBN（Fulfilled by Noon）**: 低客单价品类，FBN 出库费更具优势（低价档费率低），且 Express 标签对转化率影响大")
    elif median_price > 100:
        lines.append("- **可考虑 FBP（海外仓模式）**: 高客单价品类，FBP 配送费相对固定，Reimbursement 补贴可覆盖部分物流成本")
        lines.append("- **FBN 仍为首选**: 享受 Express 标签加持，对搜索排名和转化率提升显著")
    else:
        lines.append("- **推荐 FBN**: 中等价位品类的标准选择，Express 标签对获取流量至关重要")
        if express_ratio > 0.5:
            lines.append("- Express 渗透率已较高，非 Express 产品将处于明显劣势")

    lines.append("")

    # 4.2 差异化方向
    lines.append("### 4.2 差异化方向\n")

    # 基于评论壁垒的建议
    if avg_reviews < 10:
        lines.append("- **评论壁垒低**: 品类处于早期阶段，可通过优质产品+主动索评快速积累评论优势")
    elif avg_reviews < 50:
        lines.append("- **评论壁垒中等**: 建议初期通过 Noon Campaign/促销快速积累评论，目标 20+ 条")
    else:
        lines.append("- **评论壁垒高**: 头部产品评论积累深厚，建议从长尾细分切入，避开正面竞争")

    # 基于图片数的建议
    if "image_count" in df.columns:
        img = pd.to_numeric(df["image_count"], errors="coerce").dropna()
        if len(img) > 0:
            avg_img = float(img.mean())
            if avg_img < 4:
                lines.append(f"- **图片差异化**: 品类平均图片仅 {avg_img:.1f} 张，上传 6+ 张高质量主图+场景图可形成视觉优势")
            else:
                lines.append(f"- **图片质量**: 品类平均已有 {avg_img:.1f} 张图，需在图片质量和 A+ content 上下功夫")

    # 基于价格分布的建议
    if len(prices) > 0:
        p25 = float(prices.quantile(0.25))
        p75 = float(prices.quantile(0.75))
        lines.append(f"- **定价策略**: 主流价格带 {fmt_sar(p25)} ~ {fmt_sar(p75)}，可在此区间内以略低价格切入测试")

    # 有折扣的产品分析
    if "original_price" in df.columns:
        orig = pd.to_numeric(df["original_price"], errors="coerce")
        curr = pd.to_numeric(df["price"], errors="coerce")
        has_discount = ((orig > curr) & orig.notna() & curr.notna())
        n_discount = int(has_discount.sum())
        if n_discount > 0:
            avg_discount = float(((orig - curr) / orig)[has_discount].mean() * 100)
            lines.append(f"- **促销环境**: {n_discount}/{n_total} 产品有划线价 ({pct(n_discount, n_total)})，平均折扣 {avg_discount:.0f}%")

    lines.append("")

    # 4.3 风险提示
    lines.append("### 4.3 风险提示\n")

    risks = []
    if median_margin < 0:
        risks.append(f"**利润率风险**: 品类中位利润率仅 {fmt_pct(median_margin)}，需严格控制采购成本和物流费率")
    if express_ratio > 0.7:
        risks.append(f"**竞争风险**: Express 渗透率达 {pct(n_express, n_total)}，非 Express 产品几乎无生存空间")
    if n_total > 1000:
        risks.append(f"**供给过剩**: 品类产品数达 {n_total}，市场拥挤，需精准差异化")
    if avg_reviews > 80:
        risks.append(f"**评论壁垒**: 平均评论 {avg_reviews:.0f} 条，新品冷启动周期长")
    if total_sold == 0:
        risks.append("**需求不明**: 无近期销量数据，市场需求待验证")

    if not risks:
        risks.append("暂未识别重大风险，但建议小批量测试后再加大投入")

    for r in risks:
        lines.append(f"- {r}")

    lines.append("")
    return "\n".join(lines)


def section_benchmark(df: pd.DataFrame, df_amazon) -> str:
    """## 5. 对标产品清单"""
    lines = ["## 5. 对标产品清单\n"]

    # 筛选推荐跟进的产品：优先有销量 + 正利润的
    candidates = df.copy()

    # 排序逻辑：有销量优先，利润率其次
    sort_cols = []
    if "sold_recently" in candidates.columns:
        candidates["_sold"] = pd.to_numeric(candidates["sold_recently"], errors="coerce").fillna(0)
        sort_cols.append("_sold")
    if "margin_war_pct" in candidates.columns:
        candidates["_margin"] = pd.to_numeric(candidates["margin_war_pct"], errors="coerce").fillna(-999)
        sort_cols.append("_margin")
    if "review_count" in candidates.columns:
        candidates["_reviews"] = pd.to_numeric(candidates["review_count"], errors="coerce").fillna(0)
        sort_cols.append("_reviews")

    if sort_cols:
        candidates = candidates.sort_values(sort_cols, ascending=False)

    top_n = min(10, len(candidates))
    top = candidates.head(top_n)

    if top_n == 0:
        lines.append("> 无推荐产品数据。\n")
        return "\n".join(lines)

    lines.append(f"以下为综合销量、利润、评论筛选的 Top {top_n} 对标产品:\n")

    # 构建Amazon对照字典
    amz_dict = {}
    if df_amazon is not None and len(df_amazon) > 0:
        for _, row in df_amazon.iterrows():
            pid = str(row.get("noon_product_id", ""))
            amz_dict[pid] = row

    headers = ["#", "产品标题", "价格(SAR)", "评论", "销量", "利润率%", "Express", "Amazon比价"]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")

    for i, (_, row) in enumerate(top.iterrows(), 1):
        title = str(row.get("title", "N/A"))[:55]

        # 产品链接
        if pd.notna(row.get("product_url")):
            title_cell = f"[{title}]({row['product_url']})"
        elif pd.notna(row.get("product_id")):
            title_cell = f"[{title}](https://www.noon.com/product/{row['product_id']})"
        else:
            title_cell = title

        price_val = f"{float(row['price']):.2f}" if pd.notna(row.get("price")) else "N/A"
        rev_val = str(int(row["review_count"])) if pd.notna(row.get("review_count")) else "0"
        sold_val = str(int(row["sold_recently"])) if pd.notna(row.get("sold_recently")) else "0"
        margin_val = f"{float(row['margin_war_pct']):.1f}%" if pd.notna(row.get("margin_war_pct")) else "N/A"
        express_val = "Yes" if row.get("is_express") else "No"

        # Amazon 比价
        pid = str(row.get("product_id", ""))
        amz_info = amz_dict.get(pid)
        if amz_info is not None and pd.notna(amz_info.get("amazon_price_sar")):
            amz_price = float(amz_info["amazon_price_sar"])
            amz_reviews = int(amz_info["amazon_review_count"]) if pd.notna(amz_info.get("amazon_review_count")) else 0
            amz_cell = f"{amz_price:.0f} SAR / {amz_reviews} 评论"
        else:
            amz_cell = "-"

        lines.append(f"| {i} | {title_cell} | {price_val} | {rev_val} | {sold_val} | {margin_val} | {express_val} | {amz_cell} |")

    lines.append("")

    # Amazon 交叉对比总结
    if df_amazon is not None and len(df_amazon) > 0:
        lines.append("### Amazon 交叉对比摘要\n")
        amz_prices = pd.to_numeric(df_amazon.get("amazon_price_sar", pd.Series(dtype=float)), errors="coerce").dropna()
        noon_prices = pd.to_numeric(df_amazon.get("noon_price_sar", pd.Series(dtype=float)), errors="coerce").dropna()
        if len(amz_prices) > 0 and len(noon_prices) > 0:
            lines.append(f"- 匹配到 Amazon 对标产品: {len(df_amazon)} 个")
            lines.append(f"- Noon 中位价格: {fmt_sar(float(noon_prices.median()))}")
            lines.append(f"- Amazon 中位价格: {fmt_sar(float(amz_prices.median()))}")
            price_diff = float(noon_prices.median() - amz_prices.median())
            if price_diff > 0:
                lines.append(f"- Noon 价格平均高于 Amazon {fmt_sar(abs(price_diff))}, 有降价空间或 Amazon 供应链优势")
            else:
                lines.append(f"- Noon 价格平均低于 Amazon {fmt_sar(abs(price_diff))}, Noon 定价有竞争力")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════

def generate_report(report_path: str, category: str, output_path: str | None) -> str:
    """生成完整分析报告"""
    df_cat, overview_row, df_amazon, matched_name = load_data(report_path, category)

    if output_path is None:
        output_path = f"analysis_{slugify(matched_name)}.md"

    print(f"[INFO] 品类 '{matched_name}' 共 {len(df_cat)} 个产品")

    # 构建报告
    sections = []

    # 标题
    sections.append(f"# {matched_name} — Noon KSA 市场深潜分析报告\n")
    sections.append(f"> 数据来源: `{Path(report_path).name}` | 产品数: {len(df_cat)} | 生成时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n")
    sections.append("---\n")

    # 各章节
    sections.append(section_overview(df_cat, overview_row, matched_name))
    sections.append(section_competition(df_cat))
    sections.append(section_profit_simulation(df_cat, matched_name))
    sections.append(section_strategy(df_cat, matched_name))
    sections.append(section_benchmark(df_cat, df_amazon))

    report_text = "\n".join(sections)

    # 写入文件
    out = Path(output_path)
    out.write_text(report_text, encoding="utf-8")
    print(f"[OK] 报告已生成: {out.resolve()}")
    return str(out.resolve())


def main():
    parser = argparse.ArgumentParser(
        description="Noon KSA 品类深潜分析器 — 基于监控报告生成 Markdown 分析报告"
    )
    parser.add_argument(
        "--report", required=True,
        help="监控报告 xlsx 文件路径"
    )
    parser.add_argument(
        "--category", required=True,
        help="目标子品类名称（支持模糊匹配）"
    )
    parser.add_argument(
        "--output", default=None,
        help="输出 Markdown 文件路径（默认 analysis_{slug}.md）"
    )
    args = parser.parse_args()
    generate_report(args.report, args.category, args.output)


if __name__ == "__main__":
    main()
