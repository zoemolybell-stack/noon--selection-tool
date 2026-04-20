"""
品类默认尺寸/重量/采购比例（模式2 比例估算用）

当无法从 Alibaba.com 获取实际采购价或从详情页获取尺寸时，
使用这些默认值进行快速粗筛。

数据格式:
    V6品类名 → {
        "l": 长cm, "w": 宽cm, "h": 高cm, "wt": 重量kg,
        "cost_ratio": 采购成本占售价比例（默认0.30）
    }
"""

COST_DEFAULTS = {
    # ── Fashion ──
    "服装/鞋履 Apparel/Footwear": {"l": 30, "w": 25, "h": 5, "wt": 0.40, "cost_ratio": 0.25},
    "旅行箱包 Bags:Luggage": {"l": 55, "w": 38, "h": 25, "wt": 3.50, "cost_ratio": 0.30},
    "其他包袋 Bags:Other": {"l": 30, "w": 25, "h": 10, "wt": 0.50, "cost_ratio": 0.25},
    "手表 Watches": {"l": 12, "w": 10, "h": 8, "wt": 0.20, "cost_ratio": 0.20},
    "眼镜 Eyewear": {"l": 17, "w": 8, "h": 5, "wt": 0.10, "cost_ratio": 0.20},
    "高级珠宝 Jewelry:Fine": {"l": 10, "w": 8, "h": 4, "wt": 0.08, "cost_ratio": 0.15},
    "时尚饰品 Jewelry:Fashion": {"l": 12, "w": 8, "h": 3, "wt": 0.05, "cost_ratio": 0.15},
    "金条/金币 Gold Bars": {"l": 10, "w": 6, "h": 2, "wt": 0.10, "cost_ratio": 0.90},
    "银条 Silver Bars": {"l": 10, "w": 6, "h": 2, "wt": 0.15, "cost_ratio": 0.85},

    # ── Electronics ──
    "手机/平板 Mobiles/Tablets": {"l": 18, "w": 10, "h": 6, "wt": 0.30, "cost_ratio": 0.60},
    "苹果/三星国际版": {"l": 18, "w": 10, "h": 6, "wt": 0.30, "cost_ratio": 0.70},
    "SIM卡 SIM Cards": {"l": 10, "w": 6, "h": 1, "wt": 0.02, "cost_ratio": 0.50},
    "触控笔 Stylus Pens": {"l": 20, "w": 5, "h": 3, "wt": 0.05, "cost_ratio": 0.25},
    "笔记本/台式机 Laptops": {"l": 40, "w": 30, "h": 8, "wt": 2.50, "cost_ratio": 0.65},
    "内存卡 Memory Cards": {"l": 10, "w": 7, "h": 1, "wt": 0.02, "cost_ratio": 0.40},
    "U盘 USB Flash": {"l": 12, "w": 8, "h": 2, "wt": 0.03, "cost_ratio": 0.30},
    "硬盘/SSD Storage": {"l": 15, "w": 10, "h": 3, "wt": 0.20, "cost_ratio": 0.55},
    "网络配件 Network": {"l": 25, "w": 18, "h": 6, "wt": 0.40, "cost_ratio": 0.45},
    "显示器/显卡 Monitors/GPU": {"l": 60, "w": 40, "h": 15, "wt": 5.00, "cost_ratio": 0.60},
    "其他电脑硬件 PC Hardware": {"l": 25, "w": 18, "h": 8, "wt": 0.50, "cost_ratio": 0.45},
    "软件 Software": {"l": 20, "w": 14, "h": 2, "wt": 0.10, "cost_ratio": 0.10},
    "电脑包 Laptop Bags": {"l": 42, "w": 32, "h": 8, "wt": 0.80, "cost_ratio": 0.25},
    "打印机/扫描仪 Printers": {"l": 45, "w": 35, "h": 20, "wt": 6.00, "cost_ratio": 0.55},
    "其他办公电子 Office Electronics": {"l": 30, "w": 22, "h": 10, "wt": 1.00, "cost_ratio": 0.40},
    "手机配件≤50 Phone Acc≤50": {"l": 15, "w": 10, "h": 3, "wt": 0.08, "cost_ratio": 0.20},
    "手机配件>50 Phone Acc>50": {"l": 18, "w": 12, "h": 5, "wt": 0.15, "cost_ratio": 0.30},
    "耳机 Headphones": {"l": 20, "w": 18, "h": 8, "wt": 0.30, "cost_ratio": 0.25},
    "AR/VR眼镜": {"l": 30, "w": 20, "h": 12, "wt": 0.50, "cost_ratio": 0.45},
    "智能手表/穿戴 Smartwatches": {"l": 12, "w": 10, "h": 8, "wt": 0.15, "cost_ratio": 0.35},
    "相机/镜头 Cameras": {"l": 18, "w": 14, "h": 10, "wt": 0.60, "cost_ratio": 0.55},
    "电视 TVs": {"l": 120, "w": 75, "h": 15, "wt": 12.00, "cost_ratio": 0.65},
    "投影/流媒体 Projectors": {"l": 30, "w": 22, "h": 12, "wt": 1.50, "cost_ratio": 0.50},
    "游戏主机 Consoles": {"l": 35, "w": 25, "h": 10, "wt": 3.50, "cost_ratio": 0.70},
    "视频游戏 Video Games": {"l": 17, "w": 14, "h": 2, "wt": 0.10, "cost_ratio": 0.40},
    "游戏配件 Gaming Acc": {"l": 25, "w": 18, "h": 8, "wt": 0.35, "cost_ratio": 0.30},

    # ── Appliances ──
    "大型家电 Large Appliances": {"l": 80, "w": 60, "h": 60, "wt": 35.00, "cost_ratio": 0.55},
    "小型家电 Small Appliances": {"l": 35, "w": 25, "h": 25, "wt": 3.00, "cost_ratio": 0.40},

    # ── Beauty ──
    "香水 Fragrance": {"l": 12, "w": 8, "h": 8, "wt": 0.25, "cost_ratio": 0.15},
    "彩妆-白牌 Makeup Generic": {"l": 15, "w": 10, "h": 5, "wt": 0.15, "cost_ratio": 0.15},
    "彩妆-品牌 Makeup Branded": {"l": 15, "w": 10, "h": 5, "wt": 0.15, "cost_ratio": 0.25},
    "个护-通用 Personal Care Generic": {"l": 20, "w": 10, "h": 8, "wt": 0.30, "cost_ratio": 0.20},
    "个护-品牌 Personal Care Branded": {"l": 20, "w": 10, "h": 8, "wt": 0.30, "cost_ratio": 0.30},
    "健康营养品≤50 Health≤50": {"l": 12, "w": 8, "h": 8, "wt": 0.20, "cost_ratio": 0.25},
    "健康营养品>50 Health>50": {"l": 20, "w": 12, "h": 12, "wt": 0.50, "cost_ratio": 0.30},

    # ── Home ──
    "清洁卫浴 Cleaning": {"l": 30, "w": 15, "h": 10, "wt": 0.80, "cost_ratio": 0.25},
    "厨具/床品/卫浴/装饰 Home": {"l": 30, "w": 20, "h": 10, "wt": 0.80, "cost_ratio": 0.25},
    "家具 Furniture": {"l": 100, "w": 60, "h": 40, "wt": 15.00, "cost_ratio": 0.35},

    # ── Other ──
    "运动户外 Sports": {"l": 35, "w": 25, "h": 12, "wt": 0.80, "cost_ratio": 0.25},
    "玩具 Toys": {"l": 30, "w": 22, "h": 10, "wt": 0.50, "cost_ratio": 0.25},
    "母婴 Baby Products": {"l": 30, "w": 20, "h": 12, "wt": 0.60, "cost_ratio": 0.30},
    "图书 Books": {"l": 25, "w": 18, "h": 3, "wt": 0.40, "cost_ratio": 0.35},
    "办公文具 Office Supplies": {"l": 25, "w": 18, "h": 5, "wt": 0.30, "cost_ratio": 0.25},
    "汽配电子 Auto Electronics": {"l": 20, "w": 15, "h": 8, "wt": 0.40, "cost_ratio": 0.35},
    "汽配零件 Auto Parts": {"l": 30, "w": 20, "h": 10, "wt": 1.00, "cost_ratio": 0.30},
    "杂货非食品 Grocery Non-Food": {"l": 25, "w": 15, "h": 10, "wt": 0.50, "cost_ratio": 0.35},
    "礼品卡 Gift Cards": {"l": 15, "w": 10, "h": 1, "wt": 0.02, "cost_ratio": 0.80},
    "宠物用品 Pet Supplies": {"l": 30, "w": 20, "h": 12, "wt": 1.00, "cost_ratio": 0.30},
    "其他通用 General/Other": {"l": 25, "w": 18, "h": 8, "wt": 0.50, "cost_ratio": 0.30},
}


def get_defaults(category_name: str) -> dict:
    """获取品类默认值，找不到则返回通用默认值"""
    return COST_DEFAULTS.get(category_name, COST_DEFAULTS["其他通用 General/Other"])
