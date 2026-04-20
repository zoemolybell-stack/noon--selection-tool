"""
V6 KSA FBN 定价引擎 — 纯 Python 实现

数据源: config/v6_data.py (从 generate_pricing_v6.py 迁移的权威费率数据)
计算逻辑: 复刻 noon-pricing-v6.html 的 JS 计算函数

核心函数:
  determine_size_tier(l, w, h) → 尺寸分档
  calculate_commission(price, category_name, mode) → 佣金
  lookup_fbn_fee(tier_name, cw, price, country) → FBN出库费
  calculate_ksa_fbn_profit(...) → 完整利润计算（双费率输出）
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.v6_data import (
    CATEGORIES, CATEGORY_INDEX, FBN_SIZE_TIERS,
    FBN_KSA_FEES, FBN_UAE_FEES, KSA_VAT, KSA_RESTOCK_FEE,
)


def determine_size_tier(l_cm: float, w_cm: float, h_cm: float) -> tuple:
    """
    判定 FBN 尺寸分档。
    将三维排序为降序后，依次匹配7个档位，返回第一个符合的。
    返回: (name, max_l, max_w, max_h, pkg_wt, max_cw)
    """
    dims = sorted([l_cm, w_cm, h_cm], reverse=True)
    d1, d2, d3 = dims
    for tier in FBN_SIZE_TIERS:
        name, max_l, max_w, max_h, pkg_wt, max_cw = tier
        if d1 <= max_l and d2 <= max_w and d3 <= max_h:
            return tier
    return FBN_SIZE_TIERS[-1]  # Bulky


def calculate_commission(
    price: float,
    category_name: str,
    mode: str = "ksa_fbn",
) -> float:
    """
    计算佣金。三种类型: Fixed / Threshold / Sliding
    mode: 'uae_fbn', 'uae_fbp', 'ksa_fbn', 'ksa_fbp'
    """
    idx = CATEGORY_INDEX.get(category_name)
    if idx is None:
        idx = CATEGORY_INDEX["其他通用 General/Other"]

    cat = CATEGORIES[idx]
    offsets = {"uae_fbn": 1, "uae_fbp": 5, "ksa_fbn": 9, "ksa_fbp": 13}
    o = offsets[mode]
    comm_type = cat[o]
    base = cat[o + 1]
    limit = cat[o + 2]
    tier = cat[o + 3]

    if comm_type == "Fixed":
        return price * base
    elif comm_type == "Threshold":
        return price * base if price <= limit else price * tier
    else:  # Sliding
        return min(price, limit) * base + max(0, price - limit) * tier


def lookup_fbn_fee(
    tier_name: str,
    chargeable_weight: float,
    price: float,
    country: str = "KSA",
) -> float:
    """
    查 FBN 出库费。
    根据尺寸档名 + 计费重找行，再根据售价 ≤25 / >25 选低/高档。
    """
    table = FBN_KSA_FEES if country == "KSA" else FBN_UAE_FEES
    for row_tier, max_wt, low_fee, high_fee in table:
        if row_tier == tier_name and chargeable_weight <= max_wt:
            return low_fee if price <= 25 else high_fee
    return 0.0


def calculate_ksa_fbn_profit(
    purchase_price_cny: float,
    length_cm: float,
    width_cm: float,
    height_cm: float,
    weight_kg: float,
    target_price_sar: float,
    category_name: str,
    shipping_rate_cbm: float,
    shipping_rate_cbm_peacetime: float,
    exchange_rate: float = 1.90,
    ad_rate: float = 0.05,
    return_rate: float = 0.03,
) -> dict:
    """
    KSA FBN 模式完整利润计算，输出双费率结果（战时 + 和平时）。

    返回 dict 包含所有中间步骤和最终利润。
    """
    price = target_price_sar
    exch = exchange_rate

    # 1. 体积重
    volume_weight = length_cm * width_cm * height_cm / 5000

    # 2. 尺寸分档
    tier = determine_size_tier(length_cm, width_cm, height_cm)
    tier_name, _, _, _, pkg_wt, max_cw = tier

    # 3. FBN 计费重 = MIN(MAX(实重, 体积重) + 包装附加, 尺寸档上限)
    raw_cw = max(weight_kg, volume_weight) + pkg_wt
    fbn_cw = min(raw_cw, max_cw)

    # 4. VAT 倒算
    vat_amount = price / (1 + KSA_VAT) * KSA_VAT

    # 5. 佣金
    commission = calculate_commission(price, category_name, "ksa_fbn")

    # 6. 佣金 VAT
    commission_vat = commission * KSA_VAT

    # 7. FBN 出库费
    fbn_fee = lookup_fbn_fee(tier_name, fbn_cw, price, "KSA")

    # 8. 退货管理费（单件）
    return_fee = min(15, commission * 0.2)

    # 9. 广告费
    ads = price * ad_rate

    # 10. 退货管理费摊销
    return_admin = return_fee * return_rate

    # 11. 物流退货损耗
    logistics_loss = (fbn_fee + KSA_RESTOCK_FEE) * return_rate

    # 12. 平台端费用合计 (SAR)
    platform_costs_sar = (
        vat_amount + commission + commission_vat + fbn_fee
        + ads + return_admin + logistics_loss
    )

    # 13. 收入 (CNY)
    revenue_cny = price * exch

    # ═══ 战时费率 ═══
    head_cost_war = (length_cm * width_cm * height_cm / 1_000_000) * shipping_rate_cbm
    total_cost_war = purchase_price_cny + head_cost_war + platform_costs_sar * exch
    net_profit_war = revenue_cny - total_cost_war
    margin_war = net_profit_war / revenue_cny if revenue_cny > 0 else 0
    roi_war = net_profit_war / total_cost_war if total_cost_war > 0 else 0

    # ═══ 和平时费率 ═══
    head_cost_peace = (length_cm * width_cm * height_cm / 1_000_000) * shipping_rate_cbm_peacetime
    total_cost_peace = purchase_price_cny + head_cost_peace + platform_costs_sar * exch
    net_profit_peace = revenue_cny - total_cost_peace
    margin_peace = net_profit_peace / revenue_cny if revenue_cny > 0 else 0
    roi_peace = net_profit_peace / total_cost_peace if total_cost_peace > 0 else 0

    # 14. 月度仓储费估算（30天周转）
    cbf = length_cm * width_cm * height_cm / 28317
    monthly_storage_sar = cbf * 2.5  # KSA_MONTHLY_STORAGE

    return {
        # 基础信息
        "category": category_name,
        "target_price_sar": price,
        "size_tier": tier_name,
        "volume_weight_kg": round(volume_weight, 4),
        "fbn_chargeable_weight_kg": round(fbn_cw, 3),
        "pkg_weight_kg": pkg_wt,
        # 费用明细 (SAR)
        "vat_sar": round(vat_amount, 2),
        "commission_sar": round(commission, 2),
        "commission_rate_pct": round(commission / price * 100, 2) if price > 0 else 0,
        "commission_vat_sar": round(commission_vat, 2),
        "fbn_fee_sar": round(fbn_fee, 2),
        "return_fee_sar": round(return_fee, 2),
        "ads_sar": round(ads, 2),
        "return_admin_sar": round(return_admin, 4),
        "logistics_loss_sar": round(logistics_loss, 4),
        "platform_costs_sar": round(platform_costs_sar, 2),
        # ── 战时费率 ──
        "head_cost_war_cny": round(head_cost_war, 2),
        "total_cost_war_cny": round(total_cost_war, 2),
        "net_profit_war_cny": round(net_profit_war, 2),
        "margin_war_pct": round(margin_war * 100, 2),
        "roi_war_pct": round(roi_war * 100, 2),
        # ── 和平时费率 ──
        "head_cost_peace_cny": round(head_cost_peace, 2),
        "total_cost_peace_cny": round(total_cost_peace, 2),
        "net_profit_peace_cny": round(net_profit_peace, 2),
        "margin_peace_pct": round(margin_peace * 100, 2),
        "roi_peace_pct": round(roi_peace * 100, 2),
        # 通用
        "revenue_cny": round(revenue_cny, 2),
        "monthly_storage_sar": round(monthly_storage_sar, 2),
    }


# ═══════════════════════════════════════════════════
# 验证脚本 — 直接运行此文件输出测试结果
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("V6 KSA FBN 定价引擎验证")
    print("=" * 60)
    print()
    print("测试用例：厨具/床品/卫浴/装饰 Home")
    print("  采购价: 25 CNY")
    print("  尺寸: 15×10×8 cm, 重量: 0.8 kg")
    print("  售价: 89 SAR")
    print("  海运: 2000/950 CNY/CBM, 汇率: 1.90, 广告: 5%, 退货: 3%")
    print()

    result = calculate_ksa_fbn_profit(
        purchase_price_cny=25,
        length_cm=15,
        width_cm=10,
        height_cm=8,
        weight_kg=0.8,
        target_price_sar=89,
        category_name="厨具/床品/卫浴/装饰 Home",
        shipping_rate_cbm=2000,
        shipping_rate_cbm_peacetime=950,
        exchange_rate=1.90,
        ad_rate=0.05,
        return_rate=0.03,
    )

    print("─── 计算结果 ───")
    print(f"  尺寸层级:           {result['size_tier']}")
    print(f"  体积重:             {result['volume_weight_kg']} kg")
    print(f"  FBN计费重(含包装):  {result['fbn_chargeable_weight_kg']} kg")
    print(f"  FBN出库费:          {result['fbn_fee_sar']} SAR")
    print(f"  VAT:                {result['vat_sar']} SAR")
    print(f"  佣金:               {result['commission_sar']} SAR ({result['commission_rate_pct']}%)")
    print(f"  佣金VAT:            {result['commission_vat_sar']} SAR")
    print(f"  广告费:             {result['ads_sar']} SAR")
    print(f"  退货管理费(单件):   {result['return_fee_sar']} SAR")
    print(f"  退货摊销:           {result['return_admin_sar']} SAR")
    print(f"  物流损耗:           {result['logistics_loss_sar']} SAR")
    print(f"  平台费用合计:       {result['platform_costs_sar']} SAR")
    print(f"  收入:               {result['revenue_cny']} CNY")
    print()
    print("─── 战时费率 (2000 CNY/CBM) ───")
    print(f"  头程成本:           {result['head_cost_war_cny']} CNY")
    print(f"  总成本:             {result['total_cost_war_cny']} CNY")
    print(f"  净利润:             {result['net_profit_war_cny']} CNY")
    print(f"  净利率:             {result['margin_war_pct']}%")
    print(f"  ROI:                {result['roi_war_pct']}%")
    print()
    print("─── 和平时费率 (950 CNY/CBM) ───")
    print(f"  头程成本:           {result['head_cost_peace_cny']} CNY")
    print(f"  总成本:             {result['total_cost_peace_cny']} CNY")
    print(f"  净利润:             {result['net_profit_peace_cny']} CNY")
    print(f"  净利率:             {result['margin_peace_pct']}%")
    print(f"  ROI:                {result['roi_peace_pct']}%")
    print()

    # 验证基准对比
    print("─── 验证基准对比 ───")
    checks = [
        ("尺寸层级", result["size_tier"], "Standard Parcel"),
        ("FBN计费重", result["fbn_chargeable_weight_kg"], 0.9),
        ("FBN出库费", result["fbn_fee_sar"], 10.0),
        ("VAT", result["vat_sar"], 11.61),
        ("佣金", result["commission_sar"], 12.46),
        ("佣金VAT", result["commission_vat_sar"], 1.87),
        ("战时总成本", result["total_cost_war_cny"], 105.02),
        ("战时净利润", result["net_profit_war_cny"], 64.08),
        ("战时净利率%", result["margin_war_pct"], 37.89),
        ("和平总成本", result["total_cost_peace_cny"], 103.76),
        ("和平净利润", result["net_profit_peace_cny"], 65.34),
        ("和平净利率%", result["margin_peace_pct"], 38.64),
    ]

    all_pass = True
    for name, actual, expected in checks:
        if isinstance(expected, str):
            ok = actual == expected
            diff = ""
        else:
            diff_val = abs(actual - expected)
            pct_diff = diff_val / abs(expected) * 100 if expected != 0 else 0
            ok = pct_diff < 0.5
            diff = f"  (差异: {diff_val:.4f}, {pct_diff:.2f}%)"
        status = "✓" if ok else "✗"
        if not ok:
            all_pass = False
        print(f"  {status} {name}: 计算={actual}, 预期={expected}{diff}")

    print()
    if all_pass:
        print("所有验证通过！定价引擎计算正确。")
    else:
        print("有验证未通过！请检查计算逻辑。")
