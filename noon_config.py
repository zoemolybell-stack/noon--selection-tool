"""
noon_config.py — Noon 运营工具链统一配置中心

所有费率、汇率、佣金规则、配送费表集中管理于此文件。
修改任何参数只需改这一处，所有下游工具自动生效。

使用方式：
  from noon_config import CONFIG, get_commission, lookup_fbn_fee, lookup_fbp_fee
  from noon_config import get_fbp_reimbursement, calc_chargeable_weight, calc_full_profit
"""

# ============================================================
# 全局变量
# ============================================================
CONFIG = {
    'KSA': {
        'currency': 'SAR',
        'vat_rate': 0.15,
        'exchange_rate': 1.92,    # SAR → CNY
        'restock_fee': 0,
        'fbn_head_rate': 800,     # FBN头程 CNY/m³ (默认值)
        'fbp_head_rate': 35,      # FBP头程 CNY/kg (默认值)
        'fbp_ops_fee': 2,         # FBP操作费/件 (SAR)
        'default_ads_rate': 0.05,
        'default_return_rate': 0.05,
    },
    'UAE': {
        'currency': 'AED',
        'vat_rate': 0.05,
        'exchange_rate': 1.96,
        'restock_fee': 0,
        'fbn_head_rate': 800,
        'fbp_head_rate': 35,
        'fbp_ops_fee': 2,
        'default_ads_rate': 0.05,
        'default_return_rate': 0.05,
    },
}

# ============================================================
# 佣金表（完整版，覆盖所有品类）
# ============================================================
# 格式: category_key → {country: (type, base_rate, threshold, tier_rate)}
# type: Fixed / Threshold / Sliding
#
# Threshold（门槛跳变）: 售价<=threshold按base_rate，>threshold全额按tier_rate
# Sliding（超额累进）: threshold内按base_rate，超出部分按tier_rate

COMMISSION_TABLE = {
    # ─── 时尚服饰 ───
    '服装鞋履 Apparel': {
        'KSA': ('Fixed', 0.27, 0, 0),
        'UAE': ('Fixed', 0.27, 0, 0),
    },
    '旅行箱包 Luggage': {
        'KSA': ('Fixed', 0.20, 0, 0),
        'UAE': ('Fixed', 0.20, 0, 0),
    },
    '其他包袋 Bags': {
        'KSA': ('Fixed', 0.25, 0, 0),
        'UAE': ('Fixed', 0.25, 0, 0),
    },
    '手表 Watches': {
        'KSA': ('Sliding', 0.15, 5000, 0.05),
        'UAE': ('Sliding', 0.15, 5000, 0.05),
    },
    '眼镜 Eyewear': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '高级珠宝 Fine Jewelry': {
        'KSA': ('Sliding', 0.15, 1000, 0.05),
        'UAE': ('Sliding', 0.16, 1000, 0.05),
    },
    '时尚饰品 Fashion Jewelry': {
        'KSA': ('Fixed', 0.20, 0, 0),
        'UAE': ('Fixed', 0.20, 0, 0),
    },
    '金银条 Gold/Silver Bars': {
        'KSA': ('Fixed', 0.05, 0, 0),
        'UAE': ('Fixed', 0.05, 0, 0),
    },
    # ─── 电子产品 ───
    '手机平板 Mobiles/Tablets': {
        'KSA': ('Threshold', 0.06, 750, 0.055),
        'UAE': ('Threshold', 0.06, 500, 0.05),
    },
    '电脑 Laptops/Desktops': {
        'KSA': ('Fixed', 0.065, 0, 0),
        'UAE': ('Fixed', 0.06, 0, 0),
    },
    'SIM卡 SIM Cards': {
        'KSA': ('Fixed', 0.04, 0, 0),
        'UAE': ('Fixed', 0.04, 0, 0),
    },
    '触控笔 Stylus': {
        'KSA': ('Fixed', 0.10, 0, 0),
        'UAE': ('Fixed', 0.10, 0, 0),
    },
    '手机配件 Phone Accessories': {
        'KSA': ('Threshold', 0.20, 50, 0.15),
        'UAE': ('Threshold', 0.20, 50, 0.15),
    },
    'U盘 USB Flash': {
        'KSA': ('Threshold', 0.155, 250, 0.085),
        'UAE': ('Threshold', 0.15, 250, 0.08),
    },
    '存储卡 Memory Cards': {
        'KSA': ('Fixed', 0.075, 0, 0),
        'UAE': ('Fixed', 0.07, 0, 0),
    },
    '硬盘SSD Storage': {
        'KSA': ('Fixed', 0.065, 0, 0),
        'UAE': ('Fixed', 0.06, 0, 0),
    },
    '显示器显卡 Monitors/GPU': {
        'KSA': ('Fixed', 0.065, 0, 0),
        'UAE': ('Fixed', 0.06, 0, 0),
    },
    '网络配件 Network': {
        'KSA': ('Fixed', 0.065, 0, 0),
        'UAE': ('Fixed', 0.06, 0, 0),
    },
    '电脑硬件 PC Hardware': {
        'KSA': ('Fixed', 0.135, 0, 0),
        'UAE': ('Fixed', 0.13, 0, 0),
    },
    '软件 Software': {
        'KSA': ('Fixed', 0.155, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '电脑包 Laptop Bags': {
        'KSA': ('Fixed', 0.155, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '打印扫描 Printers': {
        'KSA': ('Fixed', 0.065, 0, 0),
        'UAE': ('Fixed', 0.06, 0, 0),
    },
    '办公电子 Office Electronics': {
        'KSA': ('Fixed', 0.135, 0, 0),
        'UAE': ('Fixed', 0.13, 0, 0),
    },
    '耳机 Headphones': {
        'KSA': ('Fixed', 0.20, 0, 0),
        'UAE': ('Threshold', 0.15, 250, 0.08),
    },
    'ARVR AR/VR': {
        'KSA': ('Fixed', 0.12, 0, 0),
        'UAE': ('Fixed', 0.12, 0, 0),
    },
    '智能穿戴 Wearables': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '相机镜头 Cameras': {
        'KSA': ('Threshold', 0.10, 250, 0.08),
        'UAE': ('Fixed', 0.06, 0, 0),
    },
    '电视 TVs': {
        'KSA': ('Fixed', 0.08, 0, 0),
        'UAE': ('Fixed', 0.05, 0, 0),
    },
    '投影流媒体 Projectors': {
        'KSA': ('Fixed', 0.10, 0, 0),
        'UAE': ('Fixed', 0.05, 0, 0),
    },
    '游戏主机 Consoles': {
        'KSA': ('Fixed', 0.05, 0, 0),
        'UAE': ('Fixed', 0.06, 0, 0),
    },
    '游戏软件 Video Games': {
        'KSA': ('Fixed', 0.14, 0, 0),
        'UAE': ('Fixed', 0.10, 0, 0),
    },
    '游戏配件 Gaming Accessories': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '礼品卡 Gift Cards': {
        'KSA': ('Fixed', 0.10, 0, 0),
        'UAE': ('Fixed', 0.05, 0, 0),
    },
    '电子配件 Electronics Accessories': {
        'KSA': ('Threshold', 0.20, 50, 0.15),
        'UAE': ('Fixed', 0.20, 0, 0),
    },
    '大家电 Large Appliances': {
        'KSA': ('Threshold', 0.12, 250, 0.06),
        'UAE': ('Threshold', 0.15, 50, 0.05),
    },
    '小家电 Small Appliances': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Fixed', 0.13, 0, 0),
    },
    # ─── 美妆个护 ───
    '香水 Fragrance': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Fixed', 0.14, 0, 0),
    },
    '彩妆通用 Makeup Generic': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Threshold', 0.08, 50, 0.15),
    },
    '彩妆品牌 Makeup Branded': {
        'KSA': ('Fixed', 0.10, 0, 0),
        'UAE': ('Threshold', 0.08, 50, 0.15),
    },
    '个护通用 Personal Care Generic': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Threshold', 0.08, 50, 0.15),
    },
    '个护品牌 Personal Care Branded': {
        'KSA': ('Threshold', 0.09, 50, 0.125),
        'UAE': ('Threshold', 0.08, 50, 0.15),
    },
    '健康营养 Health Nutrition': {
        'KSA': ('Threshold', 0.085, 50, 0.11),
        'UAE': ('Threshold', 0.08, 50, 0.14),
    },
    # ─── 家居 ───
    '清洁卫浴 Cleaning': {
        'KSA': ('Fixed', 0.09, 0, 0),
        'UAE': ('Fixed', 0.09, 0, 0),
    },
    '厨具餐饮 Kitchen': {
        'KSA': ('Fixed', 0.14, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '床品卫浴 Bedding/Bath': {
        'KSA': ('Fixed', 0.14, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '家具 Furniture': {
        'KSA': ('Sliding', 0.15, 750, 0.10),
        'UAE': ('Sliding', 0.15, 750, 0.10),
    },
    # ─── 其他 ───
    '运动户外 Sports': {
        'KSA': ('Threshold', 0.20, 30, 0.13),
        'UAE': ('Threshold', 0.20, 30, 0.13),
    },
    '玩具 Toys': {
        'KSA': ('Fixed', 0.14, 0, 0),
        'UAE': ('Fixed', 0.14, 0, 0),
    },
    '母婴 Baby': {
        'KSA': ('Threshold', 0.08, 50, 0.15),
        'UAE': ('Threshold', 0.08, 50, 0.15),
    },
    '图书 Books': {
        'KSA': ('Fixed', 0.13, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '汽配电子 Auto Electronics': {
        'KSA': ('Fixed', 0.07, 0, 0),
        'UAE': ('Fixed', 0.05, 0, 0),
    },
    '汽配零件 Auto Parts': {
        'KSA': ('Fixed', 0.10, 0, 0),
        'UAE': ('Fixed', 0.10, 0, 0),
    },
    '办公文具 Office Supplies': {
        'KSA': ('Fixed', 0.12, 0, 0),
        'UAE': ('Fixed', 0.12, 0, 0),
    },
    '杂货非食品 Grocery Non-Food': {
        'KSA': ('Fixed', 0.09, 0, 0),
        'UAE': ('Fixed', 0.09, 0, 0),
    },
    '宠物用品 Pet': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
    '其他通用 General/Other': {
        'KSA': ('Fixed', 0.15, 0, 0),
        'UAE': ('Fixed', 0.15, 0, 0),
    },
}

# v6_category 关键词 → COMMISSION_TABLE key 的模糊映射
CATEGORY_KEYWORD_MAP = {
    '服装': '服装鞋履 Apparel', '鞋': '服装鞋履 Apparel', 'Apparel': '服装鞋履 Apparel',
    '箱包': '旅行箱包 Luggage', 'Luggage': '旅行箱包 Luggage',
    '手表': '手表 Watches', 'Watches': '手表 Watches',
    '珠宝': '时尚饰品 Fashion Jewelry', 'Jewelry': '时尚饰品 Fashion Jewelry',
    '眼镜': '眼镜 Eyewear', 'Eyewear': '眼镜 Eyewear',
    '手机': '手机平板 Mobiles/Tablets', '平板': '手机平板 Mobiles/Tablets', 'Mobile': '手机平板 Mobiles/Tablets',
    '电脑': '电脑 Laptops/Desktops', '笔记本': '电脑 Laptops/Desktops', 'Laptop': '电脑 Laptops/Desktops',
    '耳机': '耳机 Headphones', 'Headphone': '耳机 Headphones',
    '相机': '相机镜头 Cameras', 'Camera': '相机镜头 Cameras',
    '电视': '电视 TVs', 'TV': '电视 TVs',
    '家电': '小家电 Small Appliances', 'Appliance': '小家电 Small Appliances',
    '香水': '香水 Fragrance', 'Fragrance': '香水 Fragrance',
    '彩妆': '彩妆通用 Makeup Generic', 'Makeup': '彩妆通用 Makeup Generic',
    '护肤': '个护通用 Personal Care Generic', 'Personal Care': '个护通用 Personal Care Generic',
    '清洁': '清洁卫浴 Cleaning', '卫浴': '清洁卫浴 Cleaning', 'Cleaning': '清洁卫浴 Cleaning',
    '厨具': '厨具餐饮 Kitchen', 'Kitchen': '厨具餐饮 Kitchen',
    '床品': '床品卫浴 Bedding/Bath', 'Bedding': '床品卫浴 Bedding/Bath',
    '家具': '家具 Furniture', 'Furniture': '家具 Furniture',
    '运动': '运动户外 Sports', '户外': '运动户外 Sports', 'Sports': '运动户外 Sports',
    '玩具': '玩具 Toys', 'Toys': '玩具 Toys',
    '母婴': '母婴 Baby', 'Baby': '母婴 Baby',
    '宠物': '宠物用品 Pet', 'Pet': '宠物用品 Pet',
    '汽配': '汽配零件 Auto Parts', '汽车': '汽配零件 Auto Parts', 'Auto': '汽配零件 Auto Parts',
    'Automotive': '汽配零件 Auto Parts',
}

# ============================================================
# FBN 配送费表  格式: [(max_weight_kg, fee_low_asp, fee_high_asp)]
# fee_low_asp: ASP<=25 SAR, fee_high_asp: ASP>25 SAR
# ============================================================
FBN_FEES = {
    'KSA': [
        (0.10, 6.0, 8.0), (0.25, 6.0, 8.0), (0.50, 6.5, 8.5),
        (1.00, 7.0, 9.0), (1.50, 8.5, 11.5), (2.00, 9.0, 12.0),
        (3.00, 10.0, 13.0), (5.00, 14.0, 18.0), (10.0, 19.0, 23.0),
        (15.0, 24.0, 28.0), (20.0, 29.0, 33.0), (30.0, 39.0, 43.0),
    ],
    'UAE': [
        (0.10, 5.5, 7.5), (0.25, 6.0, 8.0), (0.50, 6.0, 8.0),
        (1.00, 6.5, 8.5), (1.50, 8.5, 10.5), (2.00, 9.0, 11.0),
        (3.00, 10.0, 12.0), (5.00, 14.0, 15.5), (10.0, 19.0, 20.5),
        (15.0, 23.5, 25.0), (20.0, 28.0, 30.0), (30.0, 37.5, 39.5),
    ],
}

# ============================================================
# FBP 配送费表 (Drop-off)  格式: [(max_weight_kg, fee)]
# ============================================================
FBP_FEES = {
    'KSA': [(0.25, 17), (0.50, 17.5), (1.00, 18.5), (1.50, 19.5), (2.00, 20.5), (3.00, 27), (10.0, 34), (30.0, 60)],
    'UAE': [(0.25, 12), (0.50, 12.5), (1.00, 13), (1.50, 13.5), (2.00, 14), (3.00, 15.5), (10.0, 30), (30.0, 50)],
}

# ============================================================
# FBP 运费补贴 (Reimbursement)
# ============================================================
FBP_REIMBURSEMENT = {
    'KSA': [(100, 12), (500, 6), (float('inf'), 0)],
    'UAE': [(100, 10), (float('inf'), 0)],
}


# ============================================================
# 公共函数
# ============================================================

def get_commission(price, v6_category, country='KSA', commission_pct_override=None):
    """
    计算佣金。优先使用override值，再精确匹配，再模糊匹配，最后降级。
    返回 (amount, rate)
    """
    if commission_pct_override is not None and commission_pct_override > 0:
        rate = commission_pct_override / 100 if commission_pct_override > 1 else commission_pct_override
        return price * rate, rate

    entry = COMMISSION_TABLE.get(v6_category)
    if not entry and v6_category:
        for keyword, mapped_key in CATEGORY_KEYWORD_MAP.items():
            if keyword in v6_category:
                entry = COMMISSION_TABLE.get(mapped_key)
                break
    if not entry:
        entry = COMMISSION_TABLE['其他通用 General/Other']

    rule = entry.get(country, entry.get('KSA'))
    comm_type, base, threshold, tier = rule

    if comm_type == 'Fixed':
        return price * base, base
    elif comm_type == 'Threshold':
        if price <= threshold:
            return price * base, base
        else:
            return price * tier, tier
    else:  # Sliding
        amount = min(price, threshold) * base + max(0, price - threshold) * tier
        return amount, (amount / price if price > 0 else 0)


def lookup_fbn_fee(weight, price, country='KSA'):
    """FBN出库费查表。weight=计费重, price=售价(决定ASP高低档)"""
    table = FBN_FEES.get(country, FBN_FEES['KSA'])
    col = 1 if price <= 25 else 2
    for row in table:
        if weight <= row[0]:
            return row[col]
    return table[-1][col]


def lookup_fbp_fee(weight, country='KSA'):
    """FBP配送费查表 (Drop-off模式)"""
    table = FBP_FEES.get(country, FBP_FEES['KSA'])
    for max_w, fee in table:
        if weight <= max_w:
            return fee
    return table[-1][1]


def get_fbp_reimbursement(price, country='KSA'):
    """FBP运费补贴 (作为正向收入)"""
    for threshold, reimb in FBP_REIMBURSEMENT.get(country, FBP_REIMBURSEMENT['KSA']):
        if price < threshold:
            return reimb
    return 0


def calc_chargeable_weight(length_cm, width_cm, height_cm, actual_weight_kg):
    """计算计费重 = MAX(实重, 体积重)"""
    return max(actual_weight_kg, length_cm * width_cm * height_cm / 5000)


def calc_full_profit(price, purchase_cny, chargeable_weight,
                     v6_category=None, commission_pct=None,
                     fbn_fee_override=None, country='KSA',
                     ads_rate=None, return_rate=None,
                     head_cost_cny=0, mode='FBN'):
    """
    完整利润计算（FBN或FBP），返回dict含所有中间值和最终利润/ROI。

    参数:
        price: 售价 (SAR/AED)
        purchase_cny: 采购成本 (CNY)
        chargeable_weight: 计费重 (kg)
        v6_category: V6品类名 (用于佣金查表)
        commission_pct: 佣金率覆盖值 (0-100或0-1)
        fbn_fee_override: FBN出库费覆盖值
        country: 'KSA' 或 'UAE'
        ads_rate: 广告费率 (0-1)
        return_rate: 退货率 (0-1)
        head_cost_cny: 头程成本 (CNY)
        mode: 'FBN' 或 'FBP'
    """
    cfg = CONFIG[country]
    vat_rate = cfg['vat_rate']
    exch = cfg['exchange_rate']
    restock = cfg['restock_fee']
    ads = ads_rate if ads_rate is not None else cfg['default_ads_rate']
    ret = return_rate if return_rate is not None else cfg['default_return_rate']

    # VAT 倒算
    vat = price / (1 + vat_rate) * vat_rate

    # 佣金
    comm_amount, comm_rate = get_commission(price, v6_category, country, commission_pct)
    comm_vat = comm_amount * vat_rate

    # 退货管理费
    unit_ret_fee = min(15, comm_amount * 0.2)
    ret_admin = unit_ret_fee * ret

    # 广告费
    ads_fee = price * ads

    if mode == 'FBN':
        ff = fbn_fee_override if fbn_fee_override else lookup_fbn_fee(chargeable_weight, price, country)
        log_loss = (ff + restock) * ret
        total_cost = purchase_cny + head_cost_cny + (vat + comm_amount + comm_vat + ff + ads_fee + ret_admin + log_loss) * exch
        revenue = price * exch
        reimb = 0
    else:  # FBP
        ff = lookup_fbp_fee(chargeable_weight, country)
        reimb = get_fbp_reimbursement(price, country)
        log_loss = ((head_cost_cny / exch if exch > 0 else 0) + ff + restock) * ret
        total_cost = purchase_cny + head_cost_cny + (vat + comm_amount + comm_vat + ff + ads_fee + ret_admin + log_loss) * exch
        revenue = (price + reimb) * exch

    net_profit = revenue - total_cost
    roi = net_profit / total_cost if total_cost > 0 else 0
    margin = net_profit / (price * exch) if price > 0 else 0

    return {
        'price': price, 'purchase_cny': purchase_cny, 'mode': mode, 'country': country,
        'vat': round(vat, 2), 'commission': round(comm_amount, 2), 'commission_rate': round(comm_rate, 4),
        'commission_vat': round(comm_vat, 2), 'fulfillment_fee': round(ff, 2), 'reimbursement': round(reimb, 2),
        'ads_fee': round(ads_fee, 2), 'return_admin': round(ret_admin, 2), 'log_loss': round(log_loss, 2),
        'head_cost_cny': round(head_cost_cny, 2), 'total_cost_cny': round(total_cost, 2),
        'revenue_cny': round(revenue, 2), 'net_profit_cny': round(net_profit, 2),
        'roi': round(roi, 4), 'margin': round(margin, 4),
    }


# ============================================================
# 自测
# ============================================================
if __name__ == '__main__':
    print("=" * 50)
    print("  noon_config.py 自测")
    print("=" * 50)

    # 测试佣金
    amt, rate = get_commission(100, '汽配零件 Auto Parts', 'KSA')
    print(f"佣金(汽配100SAR): {amt:.2f} SAR ({rate*100:.1f}%)")
    assert abs(amt - 10.0) < 0.01, f"Expected 10.0, got {amt}"

    # 测试FBN费
    fee = lookup_fbn_fee(1.5, 50, 'KSA')
    print(f"FBN费(1.5kg, 50SAR, KSA): {fee} SAR")
    assert fee == 11.5, f"Expected 11.5, got {fee}"

    # 测试FBP费
    fee = lookup_fbp_fee(1.0, 'KSA')
    print(f"FBP费(1.0kg, KSA): {fee} SAR")
    assert fee == 18.5, f"Expected 18.5, got {fee}"

    # 测试补贴
    reimb = get_fbp_reimbursement(80, 'KSA')
    print(f"FBP补贴(80SAR, KSA): {reimb} SAR")
    assert reimb == 12, f"Expected 12, got {reimb}"

    # 测试完整利润
    result = calc_full_profit(
        price=100, purchase_cny=30, chargeable_weight=1.0,
        v6_category='汽配零件 Auto Parts', country='KSA',
        head_cost_cny=5, mode='FBN'
    )
    print(f"\nFBN利润(100SAR汽配): 净利{result['net_profit_cny']:.2f}CNY, 利率{result['margin']*100:.1f}%")

    result_fbp = calc_full_profit(
        price=100, purchase_cny=30, chargeable_weight=1.0,
        v6_category='汽配零件 Auto Parts', country='KSA',
        head_cost_cny=5, mode='FBP'
    )
    print(f"FBP利润(100SAR汽配): 净利{result_fbp['net_profit_cny']:.2f}CNY, 利率{result_fbp['margin']*100:.1f}%, 补贴{result_fbp['reimbursement']}SAR")

    # 测试模糊映射
    amt, rate = get_commission(100, '汽配电子类 Something', 'KSA')
    print(f"\n模糊映射(汽配电子类): 佣金率={rate*100:.1f}%")

    print("\n所有自测通过!")
