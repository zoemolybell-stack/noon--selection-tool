"""
V6 定价引擎数据 — 从 generate_pricing_v6.py 迁移（权威数据源）

数据结构:
- CATEGORIES: 51个品类的佣金规则，每个品类17个字段
  (name, uae_fbn_type/base/limit/tier, uae_fbp_type/base/limit/tier,
         ksa_fbn_type/base/limit/tier, ksa_fbp_type/base/limit/tier)
- FBN_SIZE_TIERS: 7个FBN尺寸分档
  (name, max_l, max_w, max_h, pkg_wt_kg, max_chg_wt)
- FBN_KSA_FEES: KSA FBN出库费查表（60行）
  (size_tier, max_weight, low_fee, high_fee)
- FBN_UAE_FEES: UAE FBN出库费查表（50行）
"""

# ── 常量 ──
KSA_VAT = 0.15
UAE_VAT = 0.05
KSA_RESTOCK_FEE = 3.0     # SAR
KSA_MONTHLY_STORAGE = 2.5  # SAR/CBF/月

# ── 佣金表（完整51品类 × 4模式） ──
CATEGORIES = [
    # Fashion
    ("服装/鞋履 Apparel/Footwear", "Fixed",0.27,0,0, "Fixed",0.27,0,0, "Fixed",0.27,0,0, "Fixed",0.27,0,0),
    ("旅行箱包 Bags:Luggage", "Fixed",0.20,0,0, "Fixed",0.20,0,0, "Fixed",0.20,0,0, "Fixed",0.20,0,0),
    ("其他包袋 Bags:Other", "Fixed",0.25,0,0, "Fixed",0.25,0,0, "Fixed",0.25,0,0, "Fixed",0.25,0,0),
    ("手表 Watches", "Sliding",0.15,5000,0.05, "Sliding",0.15,5000,0.05, "Sliding",0.15,5000,0.05, "Sliding",0.15,5000,0.05),
    ("眼镜 Eyewear", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("高级珠宝 Jewelry:Fine", "Sliding",0.16,1000,0.05, "Sliding",0.16,1000,0.05, "Sliding",0.15,1000,0.05, "Sliding",0.15,1000,0.05),
    ("时尚饰品 Jewelry:Fashion", "Fixed",0.25,0,0, "Fixed",0.25,0,0, "Fixed",0.25,0,0, "Fixed",0.25,0,0),
    ("金条/金币 Gold Bars", "Fixed",0.05,0,0, "Fixed",0.05,0,0, "Fixed",0.05,0,0, "Fixed",0.05,0,0),
    ("银条 Silver Bars", "Fixed",0.10,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0),
    # Electronics
    ("手机/平板 Mobiles/Tablets", "Threshold",0.06,500,0.05, "Threshold",0.06,500,0.05, "Threshold",0.05,750,0.045, "Threshold",0.06,750,0.055),
    ("苹果/三星国际版", "Fixed",0.06,0,0, "Fixed",0.06,0,0, "Fixed",0.06,0,0, "Fixed",0.06,0,0),
    ("SIM卡 SIM Cards", "Fixed",0.04,0,0, "Fixed",0.04,0,0, "Fixed",0.04,0,0, "Fixed",0.04,0,0),
    ("触控笔 Stylus Pens", "Fixed",0.10,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0),
    ("笔记本/台式机 Laptops", "Fixed",0.06,0,0, "Fixed",0.065,0,0, "Fixed",0.06,0,0, "Fixed",0.065,0,0),
    ("内存卡 Memory Cards", "Fixed",0.07,0,0, "Fixed",0.075,0,0, "Fixed",0.07,0,0, "Fixed",0.075,0,0),
    ("U盘 USB Flash", "Threshold",0.15,250,0.08, "Threshold",0.155,250,0.085, "Threshold",0.15,250,0.08, "Threshold",0.155,250,0.085),
    ("硬盘/SSD Storage", "Fixed",0.06,0,0, "Fixed",0.065,0,0, "Fixed",0.06,0,0, "Fixed",0.065,0,0),
    ("网络配件 Network", "Fixed",0.06,0,0, "Fixed",0.065,0,0, "Fixed",0.06,0,0, "Fixed",0.065,0,0),
    ("显示器/显卡 Monitors/GPU", "Fixed",0.06,0,0, "Fixed",0.065,0,0, "Fixed",0.06,0,0, "Fixed",0.065,0,0),
    ("其他电脑硬件 PC Hardware", "Fixed",0.13,0,0, "Fixed",0.135,0,0, "Fixed",0.13,0,0, "Fixed",0.135,0,0),
    ("软件 Software", "Fixed",0.15,0,0, "Fixed",0.155,0,0, "Fixed",0.15,0,0, "Fixed",0.155,0,0),
    ("电脑包 Laptop Bags", "Fixed",0.15,0,0, "Fixed",0.155,0,0, "Fixed",0.15,0,0, "Fixed",0.155,0,0),
    ("打印机/扫描仪 Printers", "Fixed",0.06,0,0, "Fixed",0.065,0,0, "Fixed",0.06,0,0, "Fixed",0.065,0,0),
    ("其他办公电子 Office Electronics", "Fixed",0.13,0,0, "Fixed",0.135,0,0, "Fixed",0.13,0,0, "Fixed",0.135,0,0),
    ("手机配件≤50 Phone Acc≤50", "Fixed",0.20,0,0, "Fixed",0.20,0,0, "Fixed",0.20,0,0, "Fixed",0.20,0,0),
    ("手机配件>50 Phone Acc>50", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("耳机 Headphones", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.20,0,0, "Fixed",0.20,0,0),
    ("AR/VR眼镜", "Fixed",0.12,0,0, "Fixed",0.12,0,0, "Fixed",0.12,0,0, "Fixed",0.12,0,0),
    ("智能手表/穿戴 Smartwatches", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("相机/镜头 Cameras", "Fixed",0.06,0,0, "Fixed",0.06,0,0, "Threshold",0.10,250,0.08, "Threshold",0.10,250,0.08),
    ("电视 TVs", "Fixed",0.05,0,0, "Fixed",0.05,0,0, "Fixed",0.08,0,0, "Fixed",0.08,0,0),
    ("投影/流媒体 Projectors", "Fixed",0.05,0,0, "Fixed",0.05,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0),
    ("游戏主机 Consoles", "Fixed",0.06,0,0, "Fixed",0.06,0,0, "Fixed",0.05,0,0, "Fixed",0.05,0,0),
    ("视频游戏 Video Games", "Fixed",0.10,0,0, "Fixed",0.10,0,0, "Fixed",0.14,0,0, "Fixed",0.14,0,0),
    ("游戏配件 Gaming Acc", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("大型家电 Large Appliances", "Threshold",0.15,50,0.05, "Threshold",0.15,50,0.05, "Threshold",0.12,250,0.06, "Threshold",0.12,250,0.06),
    ("小型家电 Small Appliances", "Fixed",0.13,0,0, "Fixed",0.13,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    # Beauty
    ("香水 Fragrance", "Fixed",0.14,0,0, "Fixed",0.14,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("彩妆-白牌 Makeup Generic", "Threshold",0.08,50,0.15, "Threshold",0.08,50,0.15, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("彩妆-品牌 Makeup Branded", "Threshold",0.08,50,0.15, "Threshold",0.08,50,0.15, "Fixed",0.10,0,0, "Fixed",0.10,0,0),
    ("个护-通用 Personal Care Generic", "Threshold",0.08,50,0.15, "Threshold",0.08,50,0.15, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("个护-品牌 Personal Care Branded", "Threshold",0.08,50,0.15, "Threshold",0.08,50,0.15, "Threshold",0.09,50,0.125, "Threshold",0.09,50,0.125),
    ("健康营养品≤50 Health≤50", "Fixed",0.08,0,0, "Fixed",0.08,0,0, "Fixed",0.085,0,0, "Fixed",0.085,0,0),
    ("健康营养品>50 Health>50", "Fixed",0.14,0,0, "Fixed",0.14,0,0, "Fixed",0.11,0,0, "Fixed",0.11,0,0),
    # Home
    ("清洁卫浴 Cleaning", "Fixed",0.09,0,0, "Fixed",0.09,0,0, "Fixed",0.09,0,0, "Fixed",0.09,0,0),
    ("厨具/床品/卫浴/装饰 Home", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.14,0,0, "Fixed",0.14,0,0),
    ("家具 Furniture", "Sliding",0.15,750,0.10, "Sliding",0.15,750,0.10, "Sliding",0.15,750,0.10, "Sliding",0.15,750,0.10),
    # Other
    ("运动户外 Sports", "Threshold",0.20,30,0.13, "Threshold",0.20,30,0.13, "Threshold",0.20,30,0.13, "Threshold",0.20,30,0.13),
    ("玩具 Toys", "Fixed",0.14,0,0, "Fixed",0.14,0,0, "Fixed",0.14,0,0, "Fixed",0.14,0,0),
    ("母婴 Baby Products", "Threshold",0.08,50,0.15, "Threshold",0.08,50,0.15, "Threshold",0.08,50,0.15, "Threshold",0.08,50,0.15),
    ("图书 Books", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.13,0,0, "Fixed",0.13,0,0),
    ("办公文具 Office Supplies", "Fixed",0.12,0,0, "Fixed",0.12,0,0, "Fixed",0.12,0,0, "Fixed",0.12,0,0),
    ("汽配电子 Auto Electronics", "Fixed",0.05,0,0, "Fixed",0.05,0,0, "Fixed",0.07,0,0, "Fixed",0.07,0,0),
    ("汽配零件 Auto Parts", "Fixed",0.10,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0),
    ("杂货非食品 Grocery Non-Food", "Fixed",0.09,0,0, "Fixed",0.09,0,0, "Fixed",0.09,0,0, "Fixed",0.09,0,0),
    ("礼品卡 Gift Cards", "Fixed",0.05,0,0, "Fixed",0.05,0,0, "Fixed",0.10,0,0, "Fixed",0.10,0,0),
    ("宠物用品 Pet Supplies", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
    ("其他通用 General/Other", "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0, "Fixed",0.15,0,0),
]

# ── 品类名到索引的快速查找 ──
CATEGORY_INDEX = {cat[0]: i for i, cat in enumerate(CATEGORIES)}

# ── FBN 尺寸分档（7档） ──
FBN_SIZE_TIERS = [
    # (name, max_l, max_w, max_h, pkg_wt_kg, max_chg_wt)
    ("Small Envelope", 20, 15, 1, 0.02, 0.1),
    ("Standard Envelope", 33, 23, 2.5, 0.04, 0.5),
    ("Large Envelope", 33, 23, 5, 0.04, 1.0),
    ("Standard Parcel", 45, 34, 26, 0.10, 12.0),
    ("Oversize", 130, 34, 26, 0.24, 30.0),
    ("Extra Oversize", 130, 130, 130, 0.24, 30.0),
    ("Bulky", 999, 999, 999, 0.40, 999),
]

# ── FBN KSA 出库费查表（60行） ──
# (size_tier, max_weight, low_fee_sar, high_fee_sar)
# low_fee: ASP ≤ 25 SAR, high_fee: ASP > 25 SAR
FBN_KSA_FEES = [
    ("Small Envelope", 0.1, 5.5, 7.5),
    ("Standard Envelope", 0.1, 6.0, 8.0),
    ("Standard Envelope", 0.25, 6.0, 8.0),
    ("Standard Envelope", 0.5, 6.5, 8.5),
    ("Large Envelope", 1.0, 7.0, 9.0),
    ("Standard Parcel", 0.25, 7.0, 9.0),
    ("Standard Parcel", 0.5, 7.5, 9.5),
    ("Standard Parcel", 1.0, 8.0, 10.0),
    ("Standard Parcel", 1.5, 8.5, 11.5),
    ("Standard Parcel", 2.0, 9.0, 12.0),
    ("Standard Parcel", 3.0, 10.0, 13.0),
    ("Standard Parcel", 4.0, 11.0, 14.0),
    ("Standard Parcel", 5.0, 12.0, 15.0),
    ("Standard Parcel", 6.0, 13.0, 16.0),
    ("Standard Parcel", 7.0, 14.0, 17.0),
    ("Standard Parcel", 8.0, 15.0, 18.0),
    ("Standard Parcel", 9.0, 16.0, 19.0),
    ("Standard Parcel", 10.0, 17.0, 20.0),
    ("Standard Parcel", 11.0, 18.0, 21.0),
    ("Standard Parcel", 12.0, 19.0, 22.0),
    ("Oversize", 1.0, 10.0, 14.0),
    ("Oversize", 2.0, 11.0, 15.0),
    ("Oversize", 3.0, 12.0, 16.0),
    ("Oversize", 4.0, 13.0, 17.0),
    ("Oversize", 5.0, 14.0, 18.0),
    ("Oversize", 6.0, 15.0, 19.0),
    ("Oversize", 7.0, 16.0, 20.0),
    ("Oversize", 8.0, 17.0, 21.0),
    ("Oversize", 9.0, 18.0, 22.0),
    ("Oversize", 10.0, 19.0, 23.0),
    ("Oversize", 15.0, 24.0, 28.0),
    ("Oversize", 20.0, 29.0, 33.0),
    ("Oversize", 25.0, 34.0, 38.0),
    ("Oversize", 30.0, 39.0, 43.0),
    ("Extra Oversize", 4.0, 15.5, 18.5),
    ("Extra Oversize", 5.0, 16.5, 19.5),
    ("Extra Oversize", 6.0, 17.5, 20.5),
    ("Extra Oversize", 7.0, 18.5, 21.5),
    ("Extra Oversize", 8.0, 19.5, 22.5),
    ("Extra Oversize", 9.0, 20.5, 23.5),
    ("Extra Oversize", 10.0, 21.5, 24.5),
    ("Extra Oversize", 15.0, 25.0, 29.0),
    ("Extra Oversize", 20.0, 30.0, 34.0),
    ("Extra Oversize", 25.0, 35.0, 39.0),
    ("Extra Oversize", 30.0, 39.0, 44.0),
    ("Bulky", 20.0, 33.0, 37.0),
    ("Bulky", 21.0, 35.0, 39.0),
    ("Bulky", 22.0, 37.0, 41.0),
    ("Bulky", 23.0, 39.0, 43.0),
    ("Bulky", 24.0, 41.0, 45.0),
    ("Bulky", 25.0, 43.0, 47.0),
    ("Bulky", 26.0, 45.0, 49.0),
    ("Bulky", 27.0, 47.0, 51.0),
    ("Bulky", 28.0, 49.0, 53.0),
    ("Bulky", 29.0, 51.0, 55.0),
    ("Bulky", 30.0, 53.0, 57.0),
    ("Bulky", 35.0, 61.0, 65.0),
    ("Bulky", 40.0, 70.0, 75.0),
]

# ── FBN UAE 出库费查表（50行，备用） ──
FBN_UAE_FEES = [
    ("Small Envelope", 0.1, 5.0, 7.0),
    ("Standard Envelope", 0.1, 5.5, 7.5),
    ("Standard Envelope", 0.25, 6.0, 8.0),
    ("Standard Envelope", 0.5, 6.0, 8.0),
    ("Large Envelope", 1.0, 6.5, 8.5),
    ("Standard Parcel", 0.25, 7.0, 8.5),
    ("Standard Parcel", 0.5, 7.0, 9.0),
    ("Standard Parcel", 1.0, 8.0, 10.0),
    ("Standard Parcel", 1.5, 8.5, 10.5),
    ("Standard Parcel", 2.0, 9.0, 11.0),
    ("Standard Parcel", 3.0, 10.0, 12.0),
    ("Standard Parcel", 4.0, 11.0, 13.0),
    ("Standard Parcel", 5.0, 12.0, 14.0),
    ("Standard Parcel", 6.0, 13.0, 15.0),
    ("Standard Parcel", 7.0, 14.0, 16.0),
    ("Standard Parcel", 8.0, 15.0, 17.0),
    ("Standard Parcel", 9.0, 16.0, 18.0),
    ("Standard Parcel", 10.0, 17.0, 19.0),
    ("Standard Parcel", 11.0, 18.0, 20.0),
    ("Standard Parcel", 12.0, 19.0, 21.0),
    ("Oversize", 1.0, 10.0, 12.0),
    ("Oversize", 2.0, 11.0, 13.0),
    ("Oversize", 3.0, 12.0, 14.0),
    ("Oversize", 4.0, 13.0, 14.5),
    ("Oversize", 5.0, 14.0, 15.5),
    ("Oversize", 6.0, 15.0, 16.5),
    ("Oversize", 7.0, 16.0, 17.5),
    ("Oversize", 8.0, 17.0, 18.5),
    ("Oversize", 9.0, 18.0, 19.5),
    ("Oversize", 10.0, 19.0, 20.5),
    ("Oversize", 15.0, 23.5, 25.0),
    ("Oversize", 20.0, 28.0, 30.0),
    ("Oversize", 25.0, 33.0, 34.5),
    ("Oversize", 30.0, 37.5, 39.5),
    ("Extra Oversize", 1.0, 11.0, 13.0),
    ("Extra Oversize", 2.0, 12.0, 14.0),
    ("Extra Oversize", 3.0, 13.0, 15.0),
    ("Extra Oversize", 4.0, 14.0, 16.0),
    ("Extra Oversize", 5.0, 15.0, 17.0),
    ("Extra Oversize", 6.0, 16.0, 18.0),
    ("Extra Oversize", 7.0, 17.0, 19.0),
    ("Extra Oversize", 8.0, 18.0, 20.0),
    ("Extra Oversize", 9.0, 19.0, 21.0),
    ("Extra Oversize", 10.0, 20.0, 22.0),
    ("Extra Oversize", 15.0, 25.0, 27.0),
    ("Extra Oversize", 20.0, 30.0, 32.0),
    ("Extra Oversize", 25.0, 35.0, 37.0),
    ("Extra Oversize", 30.0, 40.0, 42.0),
    ("Bulky", 20.0, 33.0, 35.0),
    ("Bulky", 21.0, 35.0, 37.0),
    ("Bulky", 22.0, 37.0, 39.0),
    ("Bulky", 23.0, 39.0, 41.0),
    ("Bulky", 24.0, 41.0, 43.0),
    ("Bulky", 25.0, 43.0, 45.0),
    ("Bulky", 26.0, 45.0, 47.0),
    ("Bulky", 27.0, 47.0, 49.0),
    ("Bulky", 28.0, 49.0, 51.0),
    ("Bulky", 29.0, 51.0, 53.0),
    ("Bulky", 30.0, 53.0, 55.0),
]
