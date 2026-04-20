"""
Noon 品类 → V6 品类名映射

两层映射策略：
1. 精确映射：Noon Category 筛选器的一级品类名（如 "Home & Kitchen"）→ V6品类
2. 关键词匹配：品类路径中的文本关键词 → V6品类（作为 fallback）
"""

# ═══ 一级品类精确映射 ═══
# Noon Category 筛选器的顶层品类名（data-qa="Category" 下第一层）→ V6品类
# 这些是 Noon 搜索结果页左侧筛选器中显示的品类名，大小写精确匹配
NOON_PRIMARY_CATEGORY_MAP = {
    # Fashion
    "Fashion": "服装/鞋履 Apparel/Footwear",
    "Women's Fashion": "服装/鞋履 Apparel/Footwear",
    "Men's Fashion": "服装/鞋履 Apparel/Footwear",
    "Kids Fashion": "服装/鞋履 Apparel/Footwear",
    "Watches & Accessories": "手表 Watches",

    # Electronics
    "Electronics & Mobiles": "手机配件≤50 Phone Acc≤50",
    "Mobiles": "手机/平板 Mobiles/Tablets",
    "Tablets": "手机/平板 Mobiles/Tablets",
    "Laptops & Computers": "笔记本/台式机 Laptops",
    "TV & Audio": "电视 TVs",
    "Gaming": "游戏主机 Consoles",
    "Cameras": "相机/镜头 Cameras",

    # Home
    "Home & Kitchen": "厨具/床品/卫浴/装饰 Home",
    "Home": "厨具/床品/卫浴/装饰 Home",
    "Kitchen & Dining": "厨具/床品/卫浴/装饰 Home",
    "Furniture": "家具 Furniture",
    "Home Improvement": "厨具/床品/卫浴/装饰 Home",

    # Appliances
    "Appliances": "小型家电 Small Appliances",
    "Large Appliances": "大型家电 Large Appliances",
    "Small Appliances": "小型家电 Small Appliances",

    # Beauty
    "Beauty": "个护-通用 Personal Care Generic",
    "Beauty & Health": "个护-通用 Personal Care Generic",
    "Fragrance": "香水 Fragrance",
    "Makeup": "彩妆-白牌 Makeup Generic",
    "Skincare": "个护-通用 Personal Care Generic",
    "Personal Care": "个护-通用 Personal Care Generic",
    "Health & Nutrition": "健康营养品≤50 Health≤50",

    # Baby & Toys
    "Baby & Toys": "母婴 Baby Products",
    "Baby Products": "母婴 Baby Products",
    "Toys & Games": "玩具 Toys",

    # Sports
    "Sports, Fitness & Outdoors": "运动户外 Sports",
    "Sports & Outdoors": "运动户外 Sports",
    "Sports": "运动户外 Sports",

    # Auto
    "Automotive": "汽配零件 Auto Parts",

    # Grocery
    "Grocery": "杂货非食品 Grocery Non-Food",
    "Supermarket": "杂货非食品 Grocery Non-Food",

    # Other
    "Books & Media": "图书 Books",
    "Stationery & Office Supplies": "办公文具 Office Supplies",
    "Pet Supplies": "宠物用品 Pet Supplies",
    "Garden & Outdoor": "厨具/床品/卫浴/装饰 Home",
    "Tools & Hardware": "厨具/床品/卫浴/装饰 Home",
}


# ═══ 关键词匹配映射（fallback） ═══
# key = 品类路径中出现的英文关键词（小写），value = V6 CATEGORIES 中的品类名
CATEGORY_MAP = {
    # ── Fashion ──
    "women's clothing": "服装/鞋履 Apparel/Footwear",
    "men's clothing": "服装/鞋履 Apparel/Footwear",
    "clothing": "服装/鞋履 Apparel/Footwear",
    "apparel": "服装/鞋履 Apparel/Footwear",
    "dresses": "服装/鞋履 Apparel/Footwear",
    "t-shirts": "服装/鞋履 Apparel/Footwear",
    "shirts": "服装/鞋履 Apparel/Footwear",
    "pants": "服装/鞋履 Apparel/Footwear",
    "jeans": "服装/鞋履 Apparel/Footwear",
    "underwear": "服装/鞋履 Apparel/Footwear",
    "sleepwear": "服装/鞋履 Apparel/Footwear",
    "sportswear": "服装/鞋履 Apparel/Footwear",
    "activewear": "服装/鞋履 Apparel/Footwear",
    "shoes": "服装/鞋履 Apparel/Footwear",
    "footwear": "服装/鞋履 Apparel/Footwear",
    "sneakers": "服装/鞋履 Apparel/Footwear",
    "sandals": "服装/鞋履 Apparel/Footwear",
    "boots": "服装/鞋履 Apparel/Footwear",
    "slippers": "服装/鞋履 Apparel/Footwear",
    "luggage": "旅行箱包 Bags:Luggage",
    "suitcase": "旅行箱包 Bags:Luggage",
    "travel bag": "旅行箱包 Bags:Luggage",
    "handbag": "其他包袋 Bags:Other",
    "backpack": "其他包袋 Bags:Other",
    "shoulder bag": "其他包袋 Bags:Other",
    "wallet": "其他包袋 Bags:Other",
    "purse": "其他包袋 Bags:Other",
    "tote": "其他包袋 Bags:Other",
    "watches": "手表 Watches",
    "watch": "手表 Watches",
    "sunglasses": "眼镜 Eyewear",
    "eyeglasses": "眼镜 Eyewear",
    "eyewear": "眼镜 Eyewear",
    "fine jewelry": "高级珠宝 Jewelry:Fine",
    "gold jewelry": "高级珠宝 Jewelry:Fine",
    "diamond": "高级珠宝 Jewelry:Fine",
    "fashion jewelry": "时尚饰品 Jewelry:Fashion",
    "costume jewelry": "时尚饰品 Jewelry:Fashion",
    "bracelet": "时尚饰品 Jewelry:Fashion",
    "necklace": "时尚饰品 Jewelry:Fashion",
    "earring": "时尚饰品 Jewelry:Fashion",
    "ring": "时尚饰品 Jewelry:Fashion",

    # ── Electronics ──
    "mobile": "手机/平板 Mobiles/Tablets",
    "smartphone": "手机/平板 Mobiles/Tablets",
    "tablet": "手机/平板 Mobiles/Tablets",
    "ipad": "手机/平板 Mobiles/Tablets",
    "iphone": "苹果/三星国际版",
    "samsung galaxy": "苹果/三星国际版",
    "sim card": "SIM卡 SIM Cards",
    "stylus": "触控笔 Stylus Pens",
    "laptop": "笔记本/台式机 Laptops",
    "desktop": "笔记本/台式机 Laptops",
    "computer": "笔记本/台式机 Laptops",
    "macbook": "笔记本/台式机 Laptops",
    "memory card": "内存卡 Memory Cards",
    "sd card": "内存卡 Memory Cards",
    "micro sd": "内存卡 Memory Cards",
    "usb flash": "U盘 USB Flash",
    "flash drive": "U盘 USB Flash",
    "hard drive": "硬盘/SSD Storage",
    "ssd": "硬盘/SSD Storage",
    "external storage": "硬盘/SSD Storage",
    "router": "网络配件 Network",
    "networking": "网络配件 Network",
    "wifi": "网络配件 Network",
    "modem": "网络配件 Network",
    "monitor": "显示器/显卡 Monitors/GPU",
    "gpu": "显示器/显卡 Monitors/GPU",
    "graphics card": "显示器/显卡 Monitors/GPU",
    "keyboard": "其他电脑硬件 PC Hardware",
    "mouse": "其他电脑硬件 PC Hardware",
    "pc components": "其他电脑硬件 PC Hardware",
    "software": "软件 Software",
    "laptop bag": "电脑包 Laptop Bags",
    "laptop sleeve": "电脑包 Laptop Bags",
    "laptop stand": "其他电脑硬件 PC Hardware",
    "laptop accessories": "其他电脑硬件 PC Hardware",
    "monitor arms": "其他电脑硬件 PC Hardware",
    "monitor mount": "其他电脑硬件 PC Hardware",
    "tablet stand": "手机配件≤50 Phone Acc≤50",
    "tablet accessories": "手机配件≤50 Phone Acc≤50",
    "printer": "打印机/扫描仪 Printers",
    "scanner": "打印机/扫描仪 Printers",
    "ink cartridge": "打印机/扫描仪 Printers",
    "office electronics": "其他办公电子 Office Electronics",
    "phone case": "手机配件≤50 Phone Acc≤50",
    "phone cover": "手机配件≤50 Phone Acc≤50",
    "screen protector": "手机配件≤50 Phone Acc≤50",
    "phone charger": "手机配件≤50 Phone Acc≤50",
    "phone cable": "手机配件≤50 Phone Acc≤50",
    "phone holder": "手机配件≤50 Phone Acc≤50",
    "phone accessories": "手机配件≤50 Phone Acc≤50",
    "mobile accessories": "手机配件≤50 Phone Acc≤50",
    "mobiles & accessories": "手机配件≤50 Phone Acc≤50",
    "earphone": "耳机 Headphones",
    "headphone": "耳机 Headphones",
    "earbuds": "耳机 Headphones",
    "headset": "耳机 Headphones",
    "airpods": "耳机 Headphones",
    "speaker": "耳机 Headphones",
    "vr": "AR/VR眼镜",
    "virtual reality": "AR/VR眼镜",
    "smart watch": "智能手表/穿戴 Smartwatches",
    "smartwatch": "智能手表/穿戴 Smartwatches",
    "fitness tracker": "智能手表/穿戴 Smartwatches",
    "wearable": "智能手表/穿戴 Smartwatches",
    "camera": "相机/镜头 Cameras",
    "lens": "相机/镜头 Cameras",
    "dslr": "相机/镜头 Cameras",
    "gopro": "相机/镜头 Cameras",
    "television": "电视 TVs",
    "tv": "电视 TVs",
    "projector": "投影/流媒体 Projectors",
    "streaming": "投影/流媒体 Projectors",
    "playstation": "游戏主机 Consoles",
    "xbox": "游戏主机 Consoles",
    "nintendo": "游戏主机 Consoles",
    "console": "游戏主机 Consoles",
    "video game": "视频游戏 Video Games",
    "gaming accessories": "游戏配件 Gaming Acc",
    "gaming controller": "游戏配件 Gaming Acc",
    "gaming headset": "游戏配件 Gaming Acc",

    # ── Appliances ──
    "refrigerator": "大型家电 Large Appliances",
    "washing machine": "大型家电 Large Appliances",
    "air conditioner": "大型家电 Large Appliances",
    "dishwasher": "大型家电 Large Appliances",
    "dryer": "大型家电 Large Appliances",
    "oven": "大型家电 Large Appliances",
    "cooker": "大型家电 Large Appliances",
    "blender": "小型家电 Small Appliances",
    "mixer": "小型家电 Small Appliances",
    "toaster": "小型家电 Small Appliances",
    "coffee maker": "小型家电 Small Appliances",
    "iron": "小型家电 Small Appliances",
    "vacuum": "小型家电 Small Appliances",
    "air fryer": "小型家电 Small Appliances",
    "juicer": "小型家电 Small Appliances",
    "kettle": "小型家电 Small Appliances",
    "fan": "小型家电 Small Appliances",
    "heater": "小型家电 Small Appliances",
    "hair dryer": "小型家电 Small Appliances",
    "small appliance": "小型家电 Small Appliances",

    # ── Beauty ──
    "perfume": "香水 Fragrance",
    "fragrance": "香水 Fragrance",
    "cologne": "香水 Fragrance",
    "oud": "香水 Fragrance",
    "bakhoor": "香水 Fragrance",
    "makeup": "彩妆-白牌 Makeup Generic",
    "lipstick": "彩妆-白牌 Makeup Generic",
    "foundation": "彩妆-白牌 Makeup Generic",
    "mascara": "彩妆-白牌 Makeup Generic",
    "eyeshadow": "彩妆-白牌 Makeup Generic",
    "nail polish": "彩妆-白牌 Makeup Generic",
    "skincare": "个护-通用 Personal Care Generic",
    "moisturizer": "个护-通用 Personal Care Generic",
    "sunscreen": "个护-通用 Personal Care Generic",
    "face wash": "个护-通用 Personal Care Generic",
    "serum": "个护-通用 Personal Care Generic",
    "shampoo": "个护-通用 Personal Care Generic",
    "conditioner": "个护-通用 Personal Care Generic",
    "body wash": "个护-通用 Personal Care Generic",
    "deodorant": "个护-通用 Personal Care Generic",
    "hair care": "个护-通用 Personal Care Generic",
    "personal care": "个护-通用 Personal Care Generic",
    "supplement": "健康营养品≤50 Health≤50",
    "vitamin": "健康营养品≤50 Health≤50",
    "protein": "健康营养品>50 Health>50",
    "health": "健康营养品≤50 Health≤50",

    # ── Home ──
    "cleaning": "清洁卫浴 Cleaning",
    "detergent": "清洁卫浴 Cleaning",
    "mop": "清洁卫浴 Cleaning",
    "broom": "清洁卫浴 Cleaning",
    "kitchen": "厨具/床品/卫浴/装饰 Home",
    "cookware": "厨具/床品/卫浴/装饰 Home",
    "bedding": "厨具/床品/卫浴/装饰 Home",
    "pillow": "厨具/床品/卫浴/装饰 Home",
    "mattress": "厨具/床品/卫浴/装饰 Home",
    "curtain": "厨具/床品/卫浴/装饰 Home",
    "towel": "厨具/床品/卫浴/装饰 Home",
    "bathroom": "厨具/床品/卫浴/装饰 Home",
    "home decor": "厨具/床品/卫浴/装饰 Home",
    "decoration": "厨具/床品/卫浴/装饰 Home",
    "candle": "厨具/床品/卫浴/装饰 Home",
    "vase": "厨具/床品/卫浴/装饰 Home",
    "organizer": "厨具/床品/卫浴/装饰 Home",
    "storage": "厨具/床品/卫浴/装饰 Home",
    "lighting": "厨具/床品/卫浴/装饰 Home",
    "lamp": "厨具/床品/卫浴/装饰 Home",
    "rug": "厨具/床品/卫浴/装饰 Home",
    "carpet": "厨具/床品/卫浴/装饰 Home",
    "furniture": "家具 Furniture",
    "sofa": "家具 Furniture",
    "chair": "家具 Furniture",
    "table": "家具 Furniture",
    "desk": "家具 Furniture",
    "shelf": "家具 Furniture",
    "cabinet": "家具 Furniture",
    "wardrobe": "家具 Furniture",
    "bed frame": "家具 Furniture",

    # ── Other ──
    "sports": "运动户外 Sports",
    "fitness": "运动户外 Sports",
    "gym": "运动户外 Sports",
    "yoga": "运动户外 Sports",
    "outdoor": "运动户外 Sports",
    "camping": "运动户外 Sports",
    "cycling": "运动户外 Sports",
    "swimming": "运动户外 Sports",
    "football": "运动户外 Sports",
    "basketball": "运动户外 Sports",
    "toys": "玩具 Toys",
    "toy": "玩具 Toys",
    "puzzle": "玩具 Toys",
    "lego": "玩具 Toys",
    "doll": "玩具 Toys",
    "board game": "玩具 Toys",
    "baby": "母婴 Baby Products",
    "baby pillow": "母婴 Baby Products",
    "maternity pillow": "母婴 Baby Products",
    "nursing pillow": "母婴 Baby Products",
    "infant": "母婴 Baby Products",
    "diaper": "母婴 Baby Products",
    "stroller": "母婴 Baby Products",
    "feeding": "母婴 Baby Products",
    "book": "图书 Books",
    "books": "图书 Books",
    "novel": "图书 Books",
    "stationery": "办公文具 Office Supplies",
    "office supplies": "办公文具 Office Supplies",
    "pen": "办公文具 Office Supplies",
    "notebook": "办公文具 Office Supplies",
    "car electronics": "汽配电子 Auto Electronics",
    "dash cam": "汽配电子 Auto Electronics",
    "car charger": "汽配电子 Auto Electronics",
    "car mobile holder": "汽配电子 Auto Electronics",
    "car mount": "汽配电子 Auto Electronics",
    "car parts": "汽配零件 Auto Parts",
    "auto parts": "汽配零件 Auto Parts",
    "car accessories": "汽配零件 Auto Parts",
    "car seat organizer": "汽配零件 Auto Parts",
    "car consoles": "汽配零件 Auto Parts",
    "automotive consoles": "汽配零件 Auto Parts",
    "interior accessories": "汽配零件 Auto Parts",
    "wiper": "汽配零件 Auto Parts",
    "grocery": "杂货非食品 Grocery Non-Food",
    "snack": "杂货非食品 Grocery Non-Food",
    "beverages": "杂货非食品 Grocery Non-Food",
    "gift card": "礼品卡 Gift Cards",
    "pet": "宠物用品 Pet Supplies",
    "dog": "宠物用品 Pet Supplies",
    "cat food": "宠物用品 Pet Supplies",
    "pet supplies": "宠物用品 Pet Supplies",
    "cat": "宠物用品 Pet Supplies",
    "bird": "宠物用品 Pet Supplies",
    "fish": "宠物用品 Pet Supplies",
    "hamster": "宠物用品 Pet Supplies",
    "reptile": "宠物用品 Pet Supplies",

    # ── Garden & Outdoor ──
    "garden": "厨具/床品/卫浴/装饰 Home",
    "outdoor": "运动户外 Sports",
    "patio": "厨具/床品/卫浴/装饰 Home",
    "grill": "小型家电 Small Appliances",
    "bbq": "小型家电 Small Appliances",
    "plant": "厨具/床品/卫浴/装饰 Home",
    "seed": "厨具/床品/卫浴/装饰 Home",
    "flower": "厨具/床品/卫浴/装饰 Home",
    "lawn": "厨具/床品/卫浴/装饰 Home",

    # ── Tools & Hardware ──
    "tools": "厨具/床品/卫浴/装饰 Home",
    "hardware": "厨具/床品/卫浴/装饰 Home",
    "power tools": "厨具/床品/卫浴/装饰 Home",
    "hand tools": "厨具/床品/卫浴/装饰 Home",
    "drill": "厨具/床品/卫浴/装饰 Home",
    "saw": "厨具/床品/卫浴/装饰 Home",
    "hammer": "厨具/床品/卫浴/装饰 Home",
    "screwdriver": "厨具/床品/卫浴/装饰 Home",
    "wrench": "厨具/床品/卫浴/装饰 Home",
    "electrical": "厨具/床品/卫浴/装饰 Home",
    "plumbing": "厨具/床品/卫浴/装饰 Home",

    # ── Food & Grocery ──
    "food": "杂货非食品 Grocery Non-Food",
    "grocery": "杂货非食品 Grocery Non-Food",
    "beverage": "杂货非食品 Grocery Non-Food",
    "snack": "杂货非食品 Grocery Non-Food",
    "coffee": "杂货非食品 Grocery Non-Food",
    "tea": "杂货非食品 Grocery Non-Food",
    "rice": "杂货非食品 Grocery Non-Food",
    "pasta": "杂货非食品 Grocery Non-Food",
    "cereal": "杂货非食品 Grocery Non-Food",
    "chocolate": "杂货非食品 Grocery Non-Food",
    "candy": "杂货非食品 Grocery Non-Food",

    # ── Music & Media ──
    "music": "图书 Books",
    "movie": "图书 Books",
    "dvd": "图书 Books",
    "blu-ray": "图书 Books",
    "cd": "图书 Books",
    "vinyl": "图书 Books",

    # ── Industrial ──
    "industrial": "其他通用 General/Other",
    "scientific": "其他通用 General/Other",
    "medical": "健康营养品≤50 Health≤50",
}

# 默认品类
DEFAULT_CATEGORY = "其他通用 General/Other"


def map_category(primary_category: str, category_tree: list[str] | None = None) -> str:
    """
    将 Noon 品类信息映射到 V6 品类名。

    策略（优先级从高到低）：
    1. 用 category_tree（多级品类路径）做关键词匹配（最精确，能区分同一大类下的子品类）
    2. 精确匹配一级品类名
    3. 用 primary_category 文本做关键词匹配
    """
    if not primary_category and not category_tree:
        return DEFAULT_CATEGORY

    # 1. 用 category_tree 关键词匹配（从子品类到父品类，子品类更具体优先）
    if category_tree:
        # 拼接所有层级文本，从子到父
        tree_text = " ".join(reversed(category_tree)).lower()
        for keyword in sorted(CATEGORY_MAP, key=len, reverse=True):
            if keyword in tree_text:
                return CATEGORY_MAP[keyword]

    # 2. 精确匹配一级品类
    if primary_category and primary_category in NOON_PRIMARY_CATEGORY_MAP:
        return NOON_PRIMARY_CATEGORY_MAP[primary_category]

    # 3. 关键词匹配 primary_category 文本
    if primary_category:
        path_lower = primary_category.lower()
        for keyword in sorted(CATEGORY_MAP, key=len, reverse=True):
            if keyword in path_lower:
                return CATEGORY_MAP[keyword]

    return DEFAULT_CATEGORY
