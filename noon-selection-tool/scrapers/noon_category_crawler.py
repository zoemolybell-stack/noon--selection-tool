"""
Noon 类目爬虫 — 按类目导航逐级遍历，采集所有商品

功能:
- 从一级类目页面进入，提取 Category 筛选器中所有子类目链接
- 递归到最末级子类目
- 每个子类目最多取前500条商品
- 用 product_id 全局去重
- 断点续跑：已完成的子类目自动跳过
- 浏览器崩溃自动重启

数据存储:
  data/monitoring/categories/{category_name}/
  ├── category_tree.json
  ├── all_products.json
  ├── subcategory/{子类目名}.json
  └── crawl_summary.json
"""
import asyncio
import json
import logging
import os
import random
import re
import time
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from datetime import datetime
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from playwright.async_api import async_playwright, Browser, BrowserContext
from ops.crawler_control import load_runtime_category_map

from scrapers.noon_category_evidence import (
    build_category_failure_evidence,
    classify_category_path_match,
    extract_breadcrumb,
    extract_category_filter,
    scrape_public_product_details,
)
from scrapers.browser_runtime import page_looks_access_denied, resolve_browser_cdp_endpoint
from scrapers.noon_category_parsing import parse_products, parse_raw_product
from scrapers.noon_category_storage import (
    atomic_write_json,
    is_valid_subcategory_payload,
    iter_effective_subcategory_payloads,
    legacy_subcategory_file_path,
    load_existing_subcategory_seen_ids,
    load_seen_ids,
    load_subcategory_payload_from_path,
    merge_all_products,
    normalize_subcategory_name,
    read_json_file,
    resolve_subcategory_file_path,
    save_subcategory,
    subcategory_file_path,
)
from scrapers.noon_category_traversal import (
    build_runtime_subcategory_records,
    crawl_category_recursive as crawl_category_recursive_helper,
    extract_subcategories_from_page as extract_subcategories_from_page_helper,
)

logger = logging.getLogger(__name__)

NOON_BASE = "https://www.noon.com/saudi-en"
NOON_TOOL_DIR = Path(__file__).parent.parent  # noon-selection-tool
PLATFORM_HOME_NAV_FILE = NOON_TOOL_DIR / "config" / "noon_home_navigation_saudi_en.json"
RUNTIME_CATEGORY_MAP_FILE = NOON_TOOL_DIR / "config" / "runtime_category_map.json"

PLATFORM_NAV_ALIASES = {
    "electronics": ["electronics"],
    "beauty": ["beauty-and-fragrance", "beauty"],
    "home_kitchen": ["home-and-kitchen"],
    "grocery": ["grocery"],
    "fashion": ["mens-fashion", "womens-fashion", "kids-fashion"],
    "baby": ["baby", "toys"],
    "sports": ["sports-and-outdoors"],
    "office": ["stationery"],
    "automotive": ["automotive"],
}

_SIGNAL_NUMERIC_ONLY_RE = re.compile(r"^[\d\s.,%+-]+$")
_SIGNAL_STOCK_RE = re.compile(
    r"((?:only|last)\s+(\d+)\s+(?:left(?:\s+in\s+stock)?|remaining))",
    re.IGNORECASE,
)
_SIGNAL_SOLD_RE = re.compile(r"(([\d,]+)\+?\s*sold(?:\s+recently)?)", re.IGNORECASE)
_SIGNAL_RANK_RE = re.compile(r"(#\d+\s+in\s+.+)", re.IGNORECASE)
_SIGNAL_LOWEST_PRICE_RE = re.compile(
    r"(lowest price in (?:\d+\s+days?|a year|a month|a week))",
    re.IGNORECASE,
)
_SIGNAL_PROMOTION_RE = re.compile(
    r"(\b\d+%\s*off\b|cashback|deal|extra\s+\d+|lowest price)",
    re.IGNORECASE,
)
_SIGNAL_AD_RE = re.compile(r"(\bsponsored\b|\bpromoted\b|\u0625\u0639\u0644\u0627\u0646)", re.IGNORECASE)
_SIGNAL_BADGE_RE = re.compile(
    r"(best seller|selling out fast|sell out fast|top rated|free delivery|lowest price|cashback|deal|"
    r"sold|only\s+\d+|last\s+\d+|#\d+\s+in|express|global|supermall|marketplace|get it by|get in \d+)",
    re.IGNORECASE,
)


def _is_explicit_ad_signal(value: str) -> bool:
    normalized = re.sub(r"\s+", " ", value or "").strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    return lowered == "ad" or bool(_SIGNAL_AD_RE.search(normalized))


def _pid_is_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _build_proxy_from_env() -> dict | None:
    direct_proxy = str(os.getenv("PROXY_URL") or os.getenv("NOON_PROXY_URL") or "").strip()
    if direct_proxy:
        return {"server": direct_proxy}

    host = str(os.getenv("PROXY_HOST") or "").strip()
    if not host:
        return None

    port = str(os.getenv("PROXY_PORT") or "").strip()
    user = str(os.getenv("PROXY_USER") or "").strip()
    password = str(os.getenv("PROXY_PASS") or "").strip()
    auth = f"{user}:{password}@" if user else ""
    server = f"http://{auth}{host}:{port}" if port else f"http://{auth}{host}"
    return {"server": server}

# Noon 一级类目导航 URL（saudi-en）
CATEGORY_URLS = {
    "automotive": f"{NOON_BASE}/automotive-store/",
    "baby": f"{NOON_BASE}/baby/",
    "beauty": f"{NOON_BASE}/beauty/",
    "electronics": f"{NOON_BASE}/electronics/",
    "fashion": f"{NOON_BASE}/fashion/",
    "grocery": f"{NOON_BASE}/noon-supermarket/",
    "home": f"{NOON_BASE}/home-kitchen/",
    "home_kitchen": f"{NOON_BASE}/home-kitchen/",
    "kids": f"{NOON_BASE}/toys-and-games/",
    "office": f"{NOON_BASE}/stationery/",
    "pets": f"{NOON_BASE}/pet-supplies/",
    "sports": f"{NOON_BASE}/sports-outdoors/",
    "tools": f"{NOON_BASE}/tools-and-home-improvement/",
    "garden": f"{NOON_BASE}/garden/",
}

# 对 noon 前台中已确认存在的真实类目链接做人工兜底。
# 这些 override 优先级高于自动匹配，适合逐轮把 unresolved 压下来。
CATEGORY_URL_OVERRIDES = {
    "electronics": {
        "mobiles_tablets": {
            "name": "Mobiles & Accessories",
            "url": f"{NOON_BASE}/electronics-and-mobiles/mobiles-and-accessories/",
        },
        "computers_laptops": {
            "name": "Computers & Accessories",
            "url": f"{NOON_BASE}/electronics-and-mobiles/computers-and-accessories/",
        },
        "audio": {
            "name": "Portable Audio & Video",
            "url": f"{NOON_BASE}/electronics-and-mobiles/portable-audio-and-video/",
        },
        "cameras": {
            "name": "Camera, Photo & Video",
            "url": f"{NOON_BASE}/electronics-and-mobiles/camera-and-photo-16165/",
        },
        "gaming": {
            "name": "Video Games",
            "url": f"{NOON_BASE}/electronics-and-mobiles/video-games-10181/",
        },
        "wearables": {
            "name": "Wearable Technology",
            "url": f"{NOON_BASE}/electronics-and-mobiles/wearable-technology/",
        },
        "feature_phones": {
            "name": "Feature Phones",
            "url": f"{NOON_BASE}/electronics-and-mobiles/mobiles-and-accessories/mobiles-20905/feature_phones/",
        },
        "speakers": {
            "name": "Portable Bluetooth Speakers",
            "url": f"{NOON_BASE}/electronics-and-mobiles/portable-audio-and-video/bluetooth-speakers/",
        },
        "home_audio": {
            "name": "Home Audio",
            "url": f"{NOON_BASE}/electronics-and-mobiles/home-audio/",
        },
        "camera_accessories": {
            "name": "Camera & Photo Accessories",
            "url": f"{NOON_BASE}/electronics-and-mobiles/camera-and-photo-16165/accessories-16794/",
        },
        "camera_lenses": {
            "name": "Camera Lenses",
            "url": f"{NOON_BASE}/electronics-and-mobiles/camera-and-photo-16165/lenses-16166/",
        },
        "gaming_accessories": {
            "name": "Video Game Accessories",
            "url": f"{NOON_BASE}/electronics-and-mobiles/video-games-10181/gaming-accessories/",
        },
        "vr_headsets": {
            "name": "Virtual Reality Headsets",
            "url": f"{NOON_BASE}/electronics-and-mobiles/wearable-technology/virtual-reality-headsets/",
        },
    },
    "automotive": {
        "car_accessories": {
            "name": "Car Interior Accessories",
            "url": f"{NOON_BASE}/automotive/interior-accessories/",
        },
        "car_parts": {
            "name": "Automotive Replacement Parts",
            "url": f"{NOON_BASE}/automotive/replacement-parts-16014/",
        },
        "motorcycle": {
            "name": "Motorcycle & Powersports",
            "url": f"{NOON_BASE}/automotive/motorcycle-and-powersports/",
        },
        "tools_equipment": {
            "name": "Automotive Tools & Equipment",
            "url": f"{NOON_BASE}/automotive/tools-and-equipment/",
        },
        "car_care_products": {
            "name": "Car Care",
            "url": f"{NOON_BASE}/automotive/car-care/",
        },
        "car_safety": {
            "name": "Automotive Interior Safety Accessories",
            "url": f"{NOON_BASE}/automotive/interior-accessories/safety-17765/",
        },
        "motorcycle_parts": {
            "name": "Motorcycle & Powersports Parts",
            "url": f"{NOON_BASE}/automotive/motorcycle-and-powersports/parts-16434/",
        },
        "repair_tools": {
            "name": "Car Repair Tools",
            "url": f"{NOON_BASE}/automotive/car-care/car-care-tools-and-equipment/repair-tools-20635/",
        },
        "polish_wax": {
            "name": "Car Polishes & Waxes",
            "url": f"{NOON_BASE}/automotive/car-care/exterior-care/car-polishes-and-waxes/",
        },
    },
    "beauty": {
        "skincare": {
            "name": "Skin Care",
            "url": f"{NOON_BASE}/beauty/skin-care-16813/",
        },
        "makeup": {
            "name": "Makeup",
            "url": f"{NOON_BASE}/beauty/makeup-16142/",
        },
        "hair_care": {
            "name": "Hair Care",
            "url": f"{NOON_BASE}/beauty/hair-care/",
        },
        "fragrances": {
            "name": "Fragrance",
            "url": f"{NOON_BASE}/beauty/fragrance/",
        },
        "personal_care": {
            "name": "Personal Care",
            "url": f"{NOON_BASE}/beauty/personal-care-16343/",
        },
        "beauty_tools": {
            "name": "Makeup Tools & Accessories",
            "url": f"{NOON_BASE}/beauty/makeup-16142/makeup-brushes-and-tools/",
        },
        "face_care": {
            "name": "Face Care",
            "url": f"{NOON_BASE}/beauty/skin-care-16813/face-17406/",
        },
        "eye_makeup": {
            "name": "Eyes",
            "url": f"{NOON_BASE}/beauty/makeup-16142/eyes-17047/",
        },
        "lip_makeup": {
            "name": "Lips",
            "url": f"{NOON_BASE}/beauty/makeup-16142/lips/",
        },
        "nail_care": {
            "name": "Nail Makeup",
            "url": f"{NOON_BASE}/beauty/makeup-16142/nails-20024/",
        },
        "hair_styling": {
            "name": "Styling Products",
            "url": f"{NOON_BASE}/beauty/hair-care/styling-products-17991/",
        },
        "hair_tools": {
            "name": "Styling Tools",
            "url": f"{NOON_BASE}/beauty/hair-care/styling-tools/",
        },
        "oral_care": {
            "name": "Oral Hygiene",
            "url": f"{NOON_BASE}/beauty/personal-care-16343/oral-hygiene/",
        },
        "body_hygiene": {
            "name": "Bath & Body",
            "url": f"{NOON_BASE}/beauty/personal-care-16343/bath-and-body/",
        },
        "beauty_devices": {
            "name": "Facial Machines",
            "url": f"{NOON_BASE}/beauty/skin-care-16813/facial-machines/",
        },
    },
    "sports": {
        "sports_clothing": {
            "name": "Sportswear",
            "url": f"{NOON_BASE}/sportswear-all/",
        },
        "sports_accessories": {
            "name": "Exercise & Fitness Accessories",
            "url": f"{NOON_BASE}/sports-and-outdoors/exercise-and-fitness/accessories-18821/",
        },
        "yoga_pilates": {
            "name": "Yoga",
            "url": f"{NOON_BASE}/sports-and-outdoors/exercise-and-fitness/yoga-16328/",
        },
        "treadmills": {
            "name": "Treadmills",
            "url": f"{NOON_BASE}/sports-and-outdoors/exercise-and-fitness/cardio-training/treadmills/",
        },
        "exercise_bikes": {
            "name": "Exercise Bikes",
            "url": f"{NOON_BASE}/sports-and-outdoors/exercise-and-fitness/cardio-training/exercise-bike/",
        },
        "weights": {
            "name": "Dumbbells",
            "url": f"{NOON_BASE}/sports-and-outdoors/exercise-and-fitness/strength-training-equipment/weights-accessories/dumbbells/",
        },
        "camping": {
            "name": "Camping & Hiking",
            "url": f"{NOON_BASE}/sports-and-outdoors/outdoor-recreation/camping-and-hiking-16354/",
        },
        "cycling": {
            "name": "Cycling",
            "url": f"{NOON_BASE}/sports-and-outdoors/cycling-16009/",
        },
        "water_sports": {
            "name": "Boating & Water Sports",
            "url": f"{NOON_BASE}/sports-and-outdoors/sports/boating-and-water-sports/",
        },
        "winter_sports": {
            "name": "Winter Sports",
            "url": f"{NOON_BASE}/sports-and-outdoors/sports/snow-sports-16612/",
        },
        "mens_sportswear": {
            "name": "Men's Activewear",
            "url": f"{NOON_BASE}/fashion/men-31225/clothing-16204/active-16233/sportswear-all/",
        },
        "womens_sportswear": {
            "name": "Women's Activewear",
            "url": f"{NOON_BASE}/fashion/women-31229/clothing-16021/active-16202/sportswear-all/",
        },
        "sports_shoes": {
            "name": "Sports Shoes",
            "url": f"{NOON_BASE}/sports-shoes/",
        },
        "fitness_trackers": {
            "name": "Fitness Trackers",
            "url": f"{NOON_BASE}/electronics-and-mobiles/wearable-technology/fitness-trackers-and-accessories/fitness-trackers/",
        },
        "sports_bags": {
            "name": "Backpacks & Bags",
            "url": f"{NOON_BASE}/sports-and-outdoors/outdoor-recreation/camping-and-hiking-16354/backpacks-and-bags/",
        },
        "water_bottles": {
            "name": "Sports Water Bottles",
            "url": f"{NOON_BASE}/sports-and-outdoors/exercise-and-fitness/exercise-fitness-running/sports-water-bottles/",
        },
    },
    "baby": {
        "baby_gear": {
            "name": "Baby Transport",
            "url": f"{NOON_BASE}/baby-products/baby-transport/",
        },
        "baby_care": {
            "name": "Bathing & Skin Care",
            "url": f"{NOON_BASE}/baby-products/bathing-and-skin-care/",
        },
        "baby_feeding": {
            "name": "Feeding",
            "url": f"{NOON_BASE}/baby-products/feeding-16153/",
        },
        "baby_nursery": {
            "name": "Nursery",
            "url": f"{NOON_BASE}/baby-products/nursery/",
        },
        "toys": {
            "name": "Toys & Games",
            "url": f"{NOON_BASE}/toys-and-games/",
        },
        "strollers": {
            "name": "Strollers",
            "url": f"{NOON_BASE}/baby-products/baby-transport/standard/strollers/",
        },
        "car_seats": {
            "name": "Car Seats",
            "url": f"{NOON_BASE}/baby-products/baby-transport/car-seats/",
        },
        "baby_carriers": {
            "name": "Carrier and Slings",
            "url": f"{NOON_BASE}/baby-products/baby-transport/carrier-and-slings/",
        },
        "high_chairs": {
            "name": "Highchairs",
            "url": f"{NOON_BASE}/baby-products/feeding-16153/highchairs-and-booster-seats/highchairs/",
        },
        "diapers": {
            "name": "Diapers",
            "url": f"{NOON_BASE}/baby-products/diapering/diapers-noon/",
        },
        "baby_bath": {
            "name": "Bathing and Baby Care",
            "url": f"{NOON_BASE}/baby-products/bathing-and-skin-care/",
        },
        "baby_skincare": {
            "name": "Hair Body and Skin Care",
            "url": f"{NOON_BASE}/baby-products/bathing-and-skin-care/skin-care-24519/",
        },
        "baby_grooming": {
            "name": "Grooming and Healthcare Kits",
            "url": f"{NOON_BASE}/baby-products/bathing-and-skin-care/grooming-and-healthcare-kits/",
        },
        "bottles": {
            "name": "Bottles",
            "url": f"{NOON_BASE}/baby-products/feeding-16153/bottle-feeding/bottles-17092/",
        },
        "breast_pumps": {
            "name": "Breast Pumps",
            "url": f"{NOON_BASE}/baby-products/feeding-16153/breastfeeding/breast-pumps/",
        },
        "baby_food": {
            "name": "Baby Foods",
            "url": f"{NOON_BASE}/grocery-store/baby-care-food/baby-foods/noon-supermarket/",
        },
        "high_chairs_boosters": {
            "name": "Highchairs and Booster Seats",
            "url": f"{NOON_BASE}/baby-products/feeding-16153/highchairs-and-booster-seats/",
        },
        "cribs": {
            "name": "Cribs Beds Mattresses",
            "url": f"{NOON_BASE}/baby-products/nursery/bedding-17446/cribs-beds-mattresses/",
        },
        "educational_toys": {
            "name": "Learning and Education",
            "url": f"{NOON_BASE}/toys-and-games/learning-and-education/",
        },
        "dolls": {
            "name": "Dolls and Accessories",
            "url": f"{NOON_BASE}/toys-and-games/dolls-and-accessories/",
        },
        "outdoor_toys": {
            "name": "Sports and Outdoor Play",
            "url": f"{NOON_BASE}/toys-and-games/sports-and-outdoor-play/",
        },
    },
    "grocery": {
        "pantry_staples": {
            "name": "Dried Beans Grains and Rice",
            "url": f"{NOON_BASE}/grocery-store/dried-beans-grains-and-rice/",
        },
        "snacks_beverages": {
            "name": "Beverages",
            "url": f"{NOON_BASE}/grocery-store/beverages-16314/",
        },
        "health_wellness": {
            "name": "Health",
            "url": f"{NOON_BASE}/health/",
        },
        "household_supplies": {
            "name": "Cleaning Supplies",
            "url": f"{NOON_BASE}/home-and-kitchen/household-supplies/cleaning-supplies/",
        },
        "rice_grains": {
            "name": "Rice",
            "url": f"{NOON_BASE}/grocery-store/dried-beans-grains-and-rice/rice/",
        },
        "coffee_tea": {
            "name": "Tea Coffee",
            "url": f"{NOON_BASE}/grocery-store/beverages-16314/grocery-tea-coffee/",
        },
        "juices": {
            "name": "Juices",
            "url": f"{NOON_BASE}/grocery-store/beverages-16314/juices/",
        },
        "vitamins_supplements": {
            "name": "Vitamins and Dietary Supplements",
            "url": f"{NOON_BASE}/health/vitamins-and-dietary-supplements/",
        },
        "medical_supplies": {
            "name": "Medical Supplies and Equipment",
            "url": f"{NOON_BASE}/p-9401/health/medical-supplies-and-equipment/",
        },
        "fitness_nutrition": {
            "name": "Sports Nutrition",
            "url": f"{NOON_BASE}/health/sports-nutrition/",
        },
        "cleaning_supplies": {
            "name": "Cleaning Supplies",
            "url": f"{NOON_BASE}/home-and-kitchen/household-supplies/cleaning-supplies/",
        },
        "laundry_supplies": {
            "name": "Laundry Care",
            "url": f"{NOON_BASE}/grocery-store/home-care-and-cleaning/grocery-laundry-care/noon-supermarket/",
        },
        "paper_products": {
            "name": "Paper Plastic Wraps",
            "url": f"{NOON_BASE}/grocery-store/home-care-and-cleaning/paper-plastic-wraps/",
        },
    },
    "garden": {
        "gardening_tools": {
            "name": "Gardening Hand Tools",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/gardening-and-lawn-care/hand-tools-20039/",
        },
        "plants_seeds": {
            "name": "Horticulture Plants Seeds",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/horticulture-plants-seeds/",
        },
        "outdoor_furniture": {
            "name": "Patio Furniture and Accessories",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/patio-furniture-and-accessories/",
        },
        "bbq_grills": {
            "name": "Outdoor Cooking",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/outdoor-cooking/",
        },
        "pots_planters": {
            "name": "Pots Planters and Container Accessories",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/gardening-and-lawn-care/pots-planters-container-accessories/",
        },
        "indoor_plants": {
            "name": "Indoor Plants",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/horticulture-plants-seeds/indoor-plants/",
        },
        "patio_sets": {
            "name": "Patio Conversation Sets",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/patio-furniture-and-accessories/conversation-sets/",
        },
        "outdoor_tables": {
            "name": "Outdoor Tables",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/patio-furniture-and-accessories/tables-19134/",
        },
        "grills": {
            "name": "Barbeque Grills",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/outdoor-cooking/barbeque-grills/",
        },
        "bbq_tools": {
            "name": "Barbeque Tools Accessories",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/outdoor-cooking/barbeque-tools-accessories/",
        },
        "charcoal_accessories": {
            "name": "Charcoal Fuel and Firestarters",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/outdoor-cooking/barbeque-tools-accessories/fuel-firestarters/",
        },
        "watering_equipment": {
            "name": "Watering Equipment",
            "url": f"{NOON_BASE}/home-and-kitchen/patio-lawn-and-garden/watering-irrigation/watering-equipment-19447/",
        },
    },
    "pets": {
        "dog_supplies": {
            "name": "Dog Supplies",
            "url": f"{NOON_BASE}/pet-supplies/dogs-16275/",
        },
        "cat_supplies": {
            "name": "Cat Supplies",
            "url": f"{NOON_BASE}/pet-supplies/cats-16737/",
        },
        "pet_grooming": {
            "name": "Dog Grooming",
            "url": f"{NOON_BASE}/pet-supplies/dogs-16275/grooming-23314/",
        },
        "dog_beds": {
            "name": "Pet Orthopaedic Beds",
            "url": f"{NOON_BASE}/pet-supplies/housing-and-bedding/pet-orthopaedic-beds/",
        },
        "dog_collars": {
            "name": "Dog Collars Harnesses and Leashes",
            "url": f"{NOON_BASE}/pet-supplies/dogs-16275/dog-training-and-behavior-aids/collars-harnesses-and-leashes-17359/",
        },
        "cat_litter": {
            "name": "Cat Health Supplies",
            "url": f"{NOON_BASE}/pet-supplies/cats-16737/health-supplies-16738/noon-supermarket/",
        },
        "bird_food": {
            "name": "Pet Supplies",
            "url": f"{NOON_BASE}/pet-supplies/",
        },
        "bird_cages": {
            "name": "Pet Cages",
            "url": f"{NOON_BASE}/pet-supplies/housing-and-bedding/pet-cages/",
        },
        "pet_brushes": {
            "name": "Pet Brushes",
            "url": f"{NOON_BASE}/pet-supplies/dogs-16275/grooming-23314/brushes-23315/",
        },
        "pet_clippers": {
            "name": "Pet Grooming",
            "url": f"{NOON_BASE}/pet-supplies/dogs-16275/grooming-23314/noon-deals-sa/",
        },
    },
    "office": {
        "office_electronics": {
            "name": "Office Electronics",
            "url": f"{NOON_BASE}/office-supplies/office-electronics/",
        },
        "office_supplies": {
            "name": "Stationery",
            "url": f"{NOON_BASE}/office-supplies/stationery-16397/",
        },
        "desk_accessories": {
            "name": "Desk Accessories and Workspace Organizers",
            "url": f"{NOON_BASE}/office-supplies/desk-accessories-and-workspace-organizers/",
        },
        "art_crafts": {
            "name": "Arts and Crafts Supplies",
            "url": f"{NOON_BASE}/office-supplies/education-and-crafts/arts-and-crafts-supplies/",
        },
        "printers_scanners": {
            "name": "Printers and Scanners",
            "urls": [
                f"{NOON_BASE}/electronics-and-mobiles/computers-and-accessories/computer-accessories/printers/",
                f"{NOON_BASE}/electronics-and-mobiles/computers-and-accessories/computer-accessories/scanners/",
            ],
        },
        "pens_pencils": {
            "name": "Pens and Pencils",
            "urls": [
                f"{NOON_BASE}/office-supplies/writing-and-correction-supplies-16515/pens-and-refills-16672/ballpoint-pens/",
                f"{NOON_BASE}/office-supplies/writing-and-correction-supplies-16515/pencils-17928/",
            ],
        },
        "files_folders": {
            "name": "Folders",
            "url": f"{NOON_BASE}/office-supplies/stationery-16397/filing-products/folders/",
        },
        "desk_organizers": {
            "name": "Desk Accessories and Workspace Organizers",
            "url": f"{NOON_BASE}/office-supplies/desk-accessories-and-workspace-organizers/",
        },
        "tape dispensers": {
            "name": "Office Tape Dispensers",
            "url": f"{NOON_BASE}/office-supplies/stationery-16397/tape-adhesives-and-fasteners/tape-17351/office-tape-dispensers-18995/",
        },
        "painting_supplies": {
            "name": "Paints and Finishes",
            "url": f"{NOON_BASE}/office-supplies/education-and-crafts/arts-and-crafts-supplies/paints-and-finishes/",
        },
        "craft_supplies": {
            "name": "Arts and Crafts Supplies",
            "url": f"{NOON_BASE}/office-supplies/education-and-crafts/arts-and-crafts-supplies/",
        },
        "calligraphy": {
            "name": "Calligraphy Pens",
            "url": f"{NOON_BASE}/office-supplies/writing-and-correction-supplies-16515/pens-and-refills-16672/calligraphy-pens/",
        },
    },
    "home_kitchen": {
        "bedding_bath": {
            "name": "Bedding and Bath",
            "url": f"{NOON_BASE}/home-and-kitchen/bedding-16171/",
        },
        "kitchen_dining": {
            "name": "Kitchen & Dining Furniture",
            "url": f"{NOON_BASE}/home-and-kitchen/furniture-10180/kitchen-furniture/",
        },
        "office_furniture": {
            "name": "Home Office Furniture",
            "url": f"{NOON_BASE}/home-and-kitchen/furniture-10180/home-office-furniture/",
        },
        "bakeware": {
            "name": "Bakeware",
            "url": f"{NOON_BASE}/home-and-kitchen/kitchen-and-dining/bakeware/",
        },
        "kitchen_tools": {
            "name": "Kitchen Utensils and Gadgets",
            "url": f"{NOON_BASE}/home-and-kitchen/kitchen-and-dining/kitchen-utensils-and-gadgets/",
        },
        "vacuum_cleaners": {
            "name": "Vacuums and Floor Care",
            "url": f"{NOON_BASE}/home-and-kitchen/home-appliances-31235/large-appliances/vacuums-and-floor-care/",
        },
        "air_conditioners": {
            "name": "Air Conditioners",
            "url": f"{NOON_BASE}/home-and-kitchen/home-appliances-31235/large-appliances/heating-cooling-and-air-quality/air-conditioners/",
        },
        "rugs_carpets": {
            "name": "Area Rugs and Pads",
            "url": f"{NOON_BASE}/home-and-kitchen/home-decor/area-rugs-and-pads/",
        },
        "pillows": {
            "name": "Bed Pillows",
            "url": f"{NOON_BASE}/home-and-kitchen/bedding-16171/bed-pillows-positioners/bed-pillows/",
        },
        "towels": {
            "name": "Towels",
            "url": f"{NOON_BASE}/home-and-kitchen/bath-16182/towels-19524/",
        },
        "bathroom_accessories": {
            "name": "Bathroom Accessories",
            "url": f"{NOON_BASE}/home-and-kitchen/bath-16182/bathroom-accessories/",
        },
    },
    "fashion": {
        "mens_watches": {
            "name": "Men's Watches",
            "url": f"{NOON_BASE}/fashion/men-31225/mens-watches/",
        },
        "womens_bags": {
            "name": "Women's Handbags",
            "url": f"{NOON_BASE}/handbags-16699/",
        },
        "womens_jewelry": {
            "name": "Women's Jewellery",
            "url": f"{NOON_BASE}/fashion/women-31229/womens-jewellery/",
        },
        "womens_watches": {
            "name": "Women's Watches",
            "url": f"{NOON_BASE}/fashion/women-31229/womens-watches/",
        },
        "kids_shoes": {
            "name": "Kids Shoes",
            "urls": [
                f"{NOON_BASE}/fashion/boys-31221/shoes-16689/",
                f"{NOON_BASE}/shoes-17594/",
            ],
        },
        "baby_clothing": {
            "name": "Baby Clothing and Shoes",
            "url": f"{NOON_BASE}/baby-products/clothing-shoes-and-accessories/",
        },
    },
    "tools": {
        "hardware": {
            "name": "Hardware",
            "url": f"{NOON_BASE}/tools-and-home-improvement/hardware-16055/",
        },
        "electrical": {
            "name": "Electrical",
            "url": f"{NOON_BASE}/tools-and-home-improvement/electrical-16287/",
        },
        "plumbing": {
            "name": "Rough Plumbing",
            "url": f"{NOON_BASE}/tools-and-home-improvement/rough-plumbing/",
        },
        "screws_bolts": {
            "name": "Nails Screws and Fasteners",
            "url": f"{NOON_BASE}/tools-and-home-improvement/hardware-16055/nails-fasteners-screws/",
        },
        "nails": {
            "name": "Nails & Screws",
            "url": f"{NOON_BASE}/tools-and-home-improvement/hardware-16055/nails-fasteners-screws/nails-screws-and-fasteners/",
        },
        "hinges_locks": {
            "name": "Door Hardware and Locks",
            "url": f"{NOON_BASE}/tools-and-home-improvement/hardware-16055/door-hardware-and-locks/",
        },
        "wires_cables": {
            "name": "Electrical Wire",
            "url": f"{NOON_BASE}/tools-and-home-improvement/electrical-16287/electrical-wire/",
        },
        "switches_sockets": {
            "name": "Outlets and Accessories",
            "url": f"{NOON_BASE}/tools-and-home-improvement/electrical-16287/outlets-and-accessories/",
        },
        "pipes_fittings": {
            "name": "Pipes Pipe Fittings and Accessories",
            "url": f"{NOON_BASE}/tools-and-home-improvement/rough-plumbing/pipes-pipe-fittings-and-accessories/",
        },
        "plumbing_tools": {
            "name": "Plumbing Tools",
            "url": f"{NOON_BASE}/tools-and-home-improvement/rough-plumbing/plumbing-tools/",
        },
    },
}


class NoonCategoryCrawler:
    """Noon 类目遍历爬虫"""

    def __init__(
        self,
        category_name: str,
        data_dir: Path,
        max_products_per_sub: int = 500,
        max_depth: int = 3,
        target_subcategory: str = None,
        on_subcategory_saved: Callable[[str, Path, dict], None] | None = None,
    ):
        self.category_name = category_name.lower()
        self.max_products_per_sub = max_products_per_sub
        self.max_depth = max_depth
        self.target_subcategory = target_subcategory
        self.base_dir = data_dir / "monitoring" / "categories" / self.category_name
        self.sub_dir = self.base_dir / "subcategory"
        self.lock_path = self.base_dir / "category_crawl.lock"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.sub_dir.mkdir(parents=True, exist_ok=True)

        self._playwright = None
        self._browser: Browser | None = None
        self._seen_product_ids: set[str] = set()
        self._errors: list[dict] = []
        self._consecutive_failures = 0
        self._max_consecutive_failures = 5
        self._resolved_config_tree: list[dict] | None = None
        self._platform_nav_tree: dict | None = None
        self._runtime_category_map: dict | None = None
        self._run_lock_payload: dict | None = None
        self._subcategory_saved_callback = on_subcategory_saved

    def _browser_headless(self) -> bool:
        return _env_flag("NOON_BROWSER_HEADLESS", _env_flag("BROWSER_HEADLESS", False))

    def _browser_channel(self) -> str:
        return str(os.getenv("NOON_BROWSER_CHANNEL") or os.getenv("BROWSER_CHANNEL") or "").strip()

    def _browser_executable_path(self) -> str:
        return str(
            os.getenv("NOON_BROWSER_EXECUTABLE_PATH")
            or os.getenv("BROWSER_EXECUTABLE_PATH")
            or ""
        ).strip()

    def _browser_cdp_endpoint(self) -> str:
        endpoint = str(
            os.getenv("NOON_BROWSER_CDP_ENDPOINT")
            or os.getenv("BROWSER_CDP_ENDPOINT")
            or ""
        ).strip()
        return resolve_browser_cdp_endpoint(endpoint)

    def _browser_warmup_url(self) -> str:
        return str(
            os.getenv("NOON_CATEGORY_WARMUP_URL")
            or os.getenv("NOON_BROWSER_WARMUP_URL")
            or f"{NOON_BASE}/search?q=foam%20roller"
        ).strip()

    def _access_denied_max_attempts(self) -> int:
        raw = str(os.getenv("NOON_CATEGORY_ACCESS_DENIED_MAX_ATTEMPTS") or "3").strip()
        try:
            value = int(raw)
        except ValueError:
            value = 3
        return max(1, min(value, 5))

    def _access_denied_backoff_seconds(self, attempt_number: int) -> int:
        return max(5, min(90, 15 * max(1, attempt_number)))

    def _build_access_denied_evidence(
        self,
        *,
        category_context: dict[str, object],
        page_url: str,
        page_number: int | None,
        attempt_number: int,
        page_state: str = "blocked",
    ) -> dict[str, object]:
        return build_category_failure_evidence(
            failure_category="access_denied",
            short_evidence=f"akamai_access_denied_on_category_page_attempt_{attempt_number}",
            snapshot_id="",
            category_context=category_context,
            page_url=page_url,
            page_number=page_number,
            page_state=page_state,
        )

    def _browser_cache_dir(self) -> Path:
        profile_root_raw = str(os.getenv("NOON_BROWSER_PROFILE_ROOT") or os.getenv("BROWSER_PROFILE_ROOT") or "").strip()
        profile_root = Path(profile_root_raw) if profile_root_raw else (self.base_dir.parent.parent.parent / ".browser_profiles")
        cache_dir = profile_root / "category" / self.category_name / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def _normalize_subcategory_name(self, sub_name: str) -> str:
        return normalize_subcategory_name(sub_name)

    def _legacy_subcategory_file_path(self, sub_name: str) -> Path:
        return legacy_subcategory_file_path(self.sub_dir, sub_name)

    def _subcategory_file_path(self, sub_name: str) -> Path:
        return subcategory_file_path(self.sub_dir, sub_name)

    def _resolve_subcategory_file_path(self, sub_name: str) -> Path:
        return resolve_subcategory_file_path(self.sub_dir, sub_name)

    def _read_json_file(self, path: Path, *, label: str):
        return read_json_file(path, label=label, logger=logger)

    def _atomic_write_json(self, path: Path, payload):
        atomic_write_json(path, payload)

    def _coerce_pid(self, value) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _acquire_run_lock(self) -> dict:
        while True:
            if self.lock_path.exists():
                existing = self._read_json_file(self.lock_path, label="运行锁") or {}
                existing_pid = self._coerce_pid(
                    existing.get("pid") if isinstance(existing, dict) else 0
                )
                if _pid_is_alive(existing_pid):
                    raise SystemExit(
                        "category crawler is already running: "
                        f"category={self.category_name}, pid={existing_pid}, lock={self.lock_path}"
                    )
                logger.warning("[category-crawler] 检测到陈旧运行锁，已清理: %s", self.lock_path)
                self.lock_path.unlink(missing_ok=True)
                continue

            payload = {
                "pid": os.getpid(),
                "category": self.category_name,
                "started_at": datetime.now().isoformat(),
            }
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                continue

            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, ensure_ascii=False, indent=2)
                self._run_lock_payload = payload
                return payload
            except Exception:
                self.lock_path.unlink(missing_ok=True)
                raise

    def _release_run_lock(self):
        if not self.lock_path.exists():
            self._run_lock_payload = None
            return

        existing = self._read_json_file(self.lock_path, label="运行锁")
        if isinstance(existing, dict):
            existing_pid = self._coerce_pid(existing.get("pid"))
            if existing_pid not in (0, os.getpid()):
                logger.warning(
                    "[category-crawler] 运行锁持有者已变化，跳过释放: %s",
                    self.lock_path,
                )
                self._run_lock_payload = None
                return

        self.lock_path.unlink(missing_ok=True)
        self._run_lock_payload = None

    def _is_valid_category_tree(self, payload) -> bool:
        if not isinstance(payload, list) or not payload:
            return False
        for item in payload:
            if not isinstance(item, dict):
                return False
            if not str(item.get("name") or "").strip():
                return False
            if not str(item.get("url") or "").strip():
                return False
        return True

    def _is_valid_subcategory_payload(self, payload, expected_sub_name: str | None = None) -> bool:
        return is_valid_subcategory_payload(payload, expected_sub_name=expected_sub_name)

    def _load_subcategory_payload_from_path(
        self,
        path: Path,
        *,
        expected_sub_name: str | None = None,
    ) -> dict | None:
        return load_subcategory_payload_from_path(
            path,
            expected_sub_name=expected_sub_name,
            logger=logger,
        )

    # ── 浏览器管理 ──

    async def _start_browser(self):
        self._playwright = await async_playwright().start()
        cdp_endpoint = self._browser_cdp_endpoint()
        if cdp_endpoint:
            self._browser = await self._playwright.chromium.connect_over_cdp(cdp_endpoint)
            logger.info("[category-crawler] 浏览器已通过 CDP 接入: %s", cdp_endpoint)
            return
        headless = self._browser_headless()
        launch_args = {
            "headless": headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                f"--disk-cache-dir={self._browser_cache_dir()}",
            ],
        }
        if not headless:
            launch_args["args"].append("--window-position=-9999,-9999")
        proxy = _build_proxy_from_env()
        if proxy:
            launch_args["proxy"] = proxy
        executable_path = self._browser_executable_path()
        if executable_path:
            launch_args["executable_path"] = executable_path
        else:
            channel = self._browser_channel()
            if channel:
                launch_args["channel"] = channel
        self._browser = await self._playwright.chromium.launch(**launch_args)
        logger.info(f"[category-crawler] 浏览器已启动 (headless={headless})")

    async def _stop_browser(self):
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
        self._browser = None
        self._playwright = None
        logger.info(f"[category-crawler] 浏览器已关闭")

    async def _restart_browser(self):
        logger.warning("[category-crawler] 浏览器重启中...")
        await self._stop_browser()
        await asyncio.sleep(3)
        await self._start_browser()

    async def _new_context(self) -> BrowserContext:
        ctx = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        try:
            from playwright_stealth import Stealth
            stealth = Stealth()
            await stealth.apply_stealth_async(ctx)
        except Exception:
            logger.warning("[category-crawler] stealth 伪装应用失败，跳过")
        return ctx

    async def _warmup_context(self, page):
        warmup_url = self._browser_warmup_url()
        if not warmup_url:
            return
        try:
            await page.goto(warmup_url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(2500)
        except Exception as exc:
            logger.debug("[category-crawler] warmup navigation failed %s: %s", warmup_url, exc)

    async def _page_body_text(self, page) -> str:
        try:
            return await page.locator("body").inner_text(timeout=5000)
        except Exception:
            return ""

    async def _is_access_denied_page(self, page) -> bool:
        try:
            title = await page.title()
        except Exception:
            title = ""
        body_text = await self._page_body_text(page)
        return page_looks_access_denied(title=title, body_text=body_text)

    async def _handle_access_denied_retry(
        self,
        *,
        sub_name: str,
        attempt_number: int,
        max_attempts: int,
    ):
        if attempt_number >= max_attempts:
            return
        backoff_seconds = self._access_denied_backoff_seconds(attempt_number)
        logger.warning(
            "[category-crawler] %s 命中 access_denied，准备重试 (%s/%s)，退避 %s 秒并重启浏览器",
            sub_name,
            attempt_number,
            max_attempts,
            backoff_seconds,
        )
        await asyncio.sleep(backoff_seconds)
        await self._restart_browser()

    def _get_entry_url(self) -> str:
        """获取一级类目入口 URL，优先使用已知映射。"""
        return CATEGORY_URLS.get(self.category_name) or f"{NOON_BASE}/{self.category_name}/"

    def _canonicalize_url(self, url: str) -> str:
        """移除 query/fragment，统一尾部斜杠，方便做 URL 匹配与去重。"""
        parts = urlsplit(url)
        path = parts.path.rstrip("/") + "/"
        return urlunsplit((parts.scheme, parts.netloc, path, "", ""))

    def _normalize_label(self, value: str) -> str:
        """将文本归一化为便于比较的 token 字符串。"""
        if not value:
            return ""
        value = unicodedata.normalize("NFKD", value)
        value = value.encode("ascii", "ignore").decode("ascii")
        value = value.lower().replace("&", " and ")
        value = re.sub(r"\ball\b", " ", value)
        value = re.sub(r"[^a-z0-9]+", " ", value)
        return re.sub(r"\s+", " ", value).strip()

    def _tokenize(self, *values: str) -> set[str]:
        stopwords = {"all", "and", "for", "the", "with", "of", "saudi", "en", "www", "com", "store", "noon"}
        tokens: set[str] = set()

        def normalize_token(token: str) -> str:
            if token.endswith("ies") and len(token) > 4:
                return token[:-3] + "y"
            if token.endswith("s") and not token.endswith("ss") and len(token) > 3:
                return token[:-1]
            return token

        for value in values:
            normalized = self._normalize_label(value)
            tokens.update(
                normalize_token(token)
                for token in normalized.split()
                if len(token) >= 2 and token not in stopwords
            )
        return tokens

    def _has_strong_candidate_match(self, node: dict, candidate: dict) -> bool:
        candidate_text = self._normalize_label(candidate.get("text", ""))
        candidate_text_tokens = self._tokenize(candidate_text)

        for variant in self._node_variants(node):
            normalized = self._normalize_label(variant)
            if not normalized:
                continue
            variant_tokens = self._tokenize(normalized)
            if not variant_tokens:
                continue
            if candidate_text == normalized:
                return True
            if len(variant_tokens) == 1:
                token = next(iter(variant_tokens))
                if token in candidate_text_tokens:
                    return True
                continue

            if len(variant_tokens & candidate_text_tokens) == len(variant_tokens):
                extra_text_tokens = candidate_text_tokens - variant_tokens
                if not extra_text_tokens:
                    return True
        return False

    def _get_category_config(self) -> dict | None:
        """读取当前一级类目的本地配置节点。"""
        tree_file = NOON_TOOL_DIR / "config" / "category_tree.json"
        if not tree_file.exists():
            return None

        data = json.loads(tree_file.read_text(encoding="utf-8"))
        for cat in data.get("categories", []):
            if cat.get("id") == self.category_name:
                return cat
        return None

    def _node_variants(self, node: dict) -> list[str]:
        variants = []
        for value in (
            node.get("name_en"),
            node.get("id"),
            (node.get("id") or "").replace("_", " "),
        ):
            if value and value not in variants:
                variants.append(value)
        return variants

    def _get_override(self, config_id: str | None) -> dict | None:
        if not config_id:
            return None
        runtime_record = self._get_runtime_record(config_id)
        if runtime_record:
            source_urls = runtime_record.get("source_urls") or []
            resolved_url = runtime_record.get("resolved_url")
            if resolved_url and resolved_url not in source_urls:
                source_urls = [resolved_url, *source_urls]
            source_urls = [url for url in source_urls if isinstance(url, str) and url]
            if source_urls:
                return {
                    "name": runtime_record.get("display_name") or runtime_record.get("subcategory_name") or config_id,
                    "url": source_urls[0],
                    "urls": source_urls,
                    "expected_path": runtime_record.get("expected_path", []),
                    "platform_nav_path": runtime_record.get("platform_nav_path", []),
                    "_source_label": "runtime_taxonomy_map",
                    "_match_score": 1200,
                }
        return CATEGORY_URL_OVERRIDES.get(self.category_name, {}).get(config_id)

    def _override_urls(self, override: dict | None) -> list[str]:
        if not override:
            return []
        urls = override.get("urls")
        if isinstance(urls, list):
            return [url for url in urls if isinstance(url, str) and url]
        url = override.get("url")
        return [url] if isinstance(url, str) and url else []

    def _load_platform_nav_tree(self) -> dict | None:
        if self._platform_nav_tree is not None:
            return self._platform_nav_tree
        if not PLATFORM_HOME_NAV_FILE.exists():
            self._platform_nav_tree = {}
            return self._platform_nav_tree
        try:
            self._platform_nav_tree = json.loads(PLATFORM_HOME_NAV_FILE.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("[category-crawler] 读取首页导航树失败: %s", exc)
            self._platform_nav_tree = {}
        return self._platform_nav_tree

    def _load_runtime_category_map(self) -> dict:
        if self._runtime_category_map is not None:
            return self._runtime_category_map
        try:
            runtime_map = load_runtime_category_map()
            self._runtime_category_map = runtime_map if isinstance(runtime_map, dict) else {}
        except Exception as exc:
            logger.warning("[category-crawler] 读取 runtime category map 失败: %s", exc)
            if not RUNTIME_CATEGORY_MAP_FILE.exists():
                self._runtime_category_map = {}
                return self._runtime_category_map
            try:
                self._runtime_category_map = json.loads(RUNTIME_CATEGORY_MAP_FILE.read_text(encoding="utf-8"))
            except Exception as fallback_exc:
                logger.warning("[category-crawler] runtime category map fallback 澶辫触: %s", fallback_exc)
                self._runtime_category_map = {}
        return self._runtime_category_map

    def _get_runtime_record(self, config_id: str | None) -> dict | None:
        if not config_id:
            return None
        runtime_map = self._load_runtime_category_map()
        return runtime_map.get("categories", {}).get(self.category_name, {}).get(config_id)

    def _get_platform_nav_candidates(self) -> list[dict]:
        tree = self._load_platform_nav_tree() or {}
        aliases = PLATFORM_NAV_ALIASES.get(self.category_name, [])
        if not aliases:
            return []

        candidates: list[dict] = []
        for category in tree.get("categories", []):
            cat_id = category.get("id")
            if cat_id not in aliases:
                continue
            cat_name = category.get("name_en") or cat_id
            cat_url = category.get("url")
            if cat_url:
                candidates.append({
                    "text": cat_name,
                    "url": self._canonicalize_url(cat_url),
                    "relative_depth": 0,
                    "source": "platform_home_nav",
                    "platform_path": [cat_name],
                })
            for group in category.get("groups", []):
                group_name = group.get("name_en") or ""
                group_url = group.get("url") or ""
                if group_name and group_url:
                    candidates.append({
                        "text": group_name,
                        "url": self._canonicalize_url(group_url),
                        "relative_depth": 1,
                        "source": "platform_home_nav",
                        "platform_path": [cat_name, group_name],
                    })
                for child in group.get("children", []):
                    child_name = child.get("name_en") or ""
                    child_url = child.get("url") or ""
                    if not child_name or not child_url:
                        continue
                    candidates.append({
                        "text": child_name,
                        "url": self._canonicalize_url(child_url),
                        "relative_depth": 2,
                        "source": "platform_home_nav",
                        "platform_path": [cat_name, group_name, child_name],
                    })
        return candidates

    def _build_paged_url(self, url: str, page_number: int) -> str:
        if page_number <= 1:
            return url
        parts = urlsplit(url)
        query_items = dict(parse_qsl(parts.query, keep_blank_values=True))
        query_items["page"] = str(page_number)
        query = urlencode(sorted(query_items.items()))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))

    def _build_runtime_subcategories(self) -> list[dict]:
        return build_runtime_subcategory_records(
            category_name=self.category_name,
            runtime_map=self._load_runtime_category_map(),
            canonicalize_url=self._canonicalize_url,
        )

    async def discover_category_tree(self) -> list[dict]:
        runtime_subcategories = self._build_runtime_subcategories()
        if runtime_subcategories:
            logger.info(
                "[category-crawler] 使用 runtime category map: %s 个子类目",
                len(runtime_subcategories),
            )
            return runtime_subcategories

        entry_url = self._get_entry_url()
        discovered = await self.crawl_category_recursive(entry_url, depth=1, path=[])
        normalized = []
        for item in discovered:
            expected_path = list(item.get("path") or [])
            normalized.append(
                {
                    "name": item.get("name") or (expected_path[-1] if expected_path else ""),
                    "url": self._canonicalize_url(str(item.get("url") or entry_url)),
                    "source_urls": [self._canonicalize_url(str(item.get("url") or entry_url))],
                    "depth": item.get("depth", 0),
                    "path": expected_path,
                    "expected_path": expected_path,
                    "platform_nav_path": [],
                    "config_id": "",
                    "parent_config_id": "",
                    "source": "recursive_discovery",
                }
            )
        return normalized

    async def _get_target_subcategory(self, target_subcategory: str) -> list[dict]:
        target_key = str(target_subcategory or "").strip()
        if not target_key:
            return []

        runtime_subcategories = self._build_runtime_subcategories()
        for item in runtime_subcategories:
            if str(item.get("config_id") or "").strip() == target_key:
                return [item]
            if self._normalize_subcategory_name(item.get("name", "")) == self._normalize_subcategory_name(target_key):
                return [item]

        override = self._get_override(target_key)
        if override:
            source_urls = [self._canonicalize_url(url) for url in self._override_urls(override)]
            if not source_urls and override.get("url"):
                source_urls = [self._canonicalize_url(str(override["url"]))]
            expected_path = list(override.get("expected_path") or [])
            runtime_record = self._get_runtime_record(target_key) or {}
            return [
                {
                    "name": override.get("name") or target_key,
                    "url": source_urls[0] if source_urls else self._get_entry_url(),
                    "source_urls": source_urls or [self._get_entry_url()],
                    "depth": max(1, len(expected_path)),
                    "path": expected_path,
                    "expected_path": expected_path,
                    "platform_nav_path": list(override.get("platform_nav_path") or runtime_record.get("platform_nav_path") or []),
                    "config_id": target_key,
                    "parent_config_id": runtime_record.get("parent_config_id") or "",
                    "source": override.get("_source_label") or "category_override",
                }
            ]

        discovered = await self.discover_category_tree()
        return [
            item
            for item in discovered
            if self._normalize_subcategory_name(item.get("name", "")) == self._normalize_subcategory_name(target_key)
        ][:1]

    async def scrape_subcategory(
        self,
        url: str,
        sub_name: str,
        expected_path: list[str] | None = None,
    ) -> dict:
        requested_url = str(url or "").strip()

        category_context = {
            "category_name": self.category_name,
            "subcategory": sub_name,
            "expected_path": list(expected_path or []),
        }
        accumulated_page_evidence: list[dict[str, object]] = []
        last_result = {
            "requested_url": requested_url,
            "resolved_url": self._canonicalize_url(requested_url) if requested_url else "",
            "effective_category_path": " > ".join(expected_path or []) or sub_name,
            "breadcrumb": {"items": [], "links": [], "path": ""},
            "category_filter": {"links": [], "texts": []},
            "breadcrumb_match_status": "unknown",
            "products": [],
            "page_evidence": [],
        }
        max_attempts = self._access_denied_max_attempts()

        for attempt_number in range(1, max_attempts + 1):
            ctx = await self._new_context()
            page = await ctx.new_page()
            products: list[dict] = []
            page_evidence: list[dict[str, object]] = []
            resolved_url = self._canonicalize_url(requested_url) if requested_url else ""
            breadcrumb = {"items": [], "links": [], "path": ""}
            category_filter = {"links": [], "texts": []}
            breadcrumb_match_status = "unknown"
            effective_category_path = " > ".join(expected_path or []) or sub_name
            retry_after_attempt = False

            try:
                await self._warmup_context(page)
                if await self._is_access_denied_page(page):
                    page_evidence.append(
                        self._build_access_denied_evidence(
                            category_context=category_context,
                            page_url=page.url or self._browser_warmup_url(),
                            page_number=None,
                            attempt_number=attempt_number,
                            page_state="blocked_warmup",
                        )
                    )
                    retry_after_attempt = attempt_number < max_attempts
                else:
                    seen_ids: set[str] = set()
                    max_pages = max(1, min(20, ((self.max_products_per_sub or 1) + 59) // 60))
                    for page_number in range(1, max_pages + 1):
                        page_url = self._build_paged_url(requested_url, page_number)
                        try:
                            await page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
                            await page.wait_for_timeout(3500)
                        except Exception as exc:
                            if await self._is_access_denied_page(page):
                                page_evidence.append(
                                    self._build_access_denied_evidence(
                                        category_context=category_context,
                                        page_url=page_url,
                                        page_number=page_number,
                                        attempt_number=attempt_number,
                                    )
                                )
                                retry_after_attempt = attempt_number < max_attempts
                                break
                            page_evidence.append(
                                build_category_failure_evidence(
                                    failure_category="timeout",
                                    short_evidence=f"page_goto_timeout:{exc}",
                                    snapshot_id="",
                                    category_context=category_context,
                                    page_url=page_url,
                                    page_number=page_number,
                                    page_state="timeout",
                                )
                            )
                            break

                        if await self._is_access_denied_page(page):
                            page_evidence.append(
                                self._build_access_denied_evidence(
                                    category_context=category_context,
                                    page_url=page_url,
                                    page_number=page_number,
                                    attempt_number=attempt_number,
                                )
                            )
                            retry_after_attempt = attempt_number < max_attempts
                            break

                        if page_number == 1:
                            resolved_url = self._canonicalize_url(page.url or requested_url)
                            breadcrumb = await self._extract_breadcrumb(page)
                            category_filter = await extract_category_filter(page)
                            match_payload = classify_category_path_match(
                                expected_path=expected_path,
                                breadcrumb=breadcrumb,
                                category_filter=category_filter,
                                fallback_name=sub_name,
                            )
                            breadcrumb_match_status = str(match_payload.get("status") or "unknown")
                            effective_category_path = str(
                                match_payload.get("effective_category_path")
                                or effective_category_path
                                or sub_name
                            ).strip()

                        page_products = await self._parse_products(
                            page,
                            rank_offset=len(products),
                            category_path=effective_category_path,
                        )
                        if not page_products:
                            if page_number == 1:
                                page_evidence.append(
                                    build_category_failure_evidence(
                                        failure_category="page_recognition_failed",
                                        short_evidence="no_products_detected_on_category_page",
                                        snapshot_id="",
                                        category_context=category_context,
                                        page_url=page_url,
                                        page_number=page_number,
                                        page_state="empty",
                                    )
                                )
                            break

                        new_products = 0
                        for product in page_products:
                            product_id = (
                                product.get("product_id")
                                or product.get("sku")
                                or product.get("product_url")
                                or product.get("url")
                                or product.get("title")
                                or ""
                            )
                            if product_id and product_id in seen_ids:
                                continue
                            if product_id:
                                seen_ids.add(product_id)
                            products.append(product)
                            new_products += 1
                            if len(products) >= self.max_products_per_sub:
                                break

                        if len(products) >= self.max_products_per_sub:
                            break
                        if new_products == 0:
                            break
                        if len(page_products) < 20:
                            break
            finally:
                await page.close()
                await ctx.close()

            last_result = {
                "requested_url": requested_url,
                "resolved_url": resolved_url,
                "effective_category_path": effective_category_path,
                "breadcrumb": breadcrumb,
                "category_filter": category_filter,
                "breadcrumb_match_status": breadcrumb_match_status,
                "products": products,
                "page_evidence": [*accumulated_page_evidence, *page_evidence],
            }

            if retry_after_attempt:
                accumulated_page_evidence.extend(page_evidence)
                await self._handle_access_denied_retry(
                    sub_name=sub_name,
                    attempt_number=attempt_number,
                    max_attempts=max_attempts,
                )
                continue

            break

        return last_result

    def _terminal_source_run_failure_category(self, source_runs: list[dict[str, object]]) -> str:
        if any(int(run.get("product_count") or 0) > 0 for run in source_runs if isinstance(run, dict)):
            return ""
        ordered_codes = (
            "access_denied",
            "timeout",
            "page_parse_failure",
            "result_contract_mismatch",
        )
        seen_codes: set[str] = set()
        for run in source_runs:
            if not isinstance(run, dict):
                continue
            page_evidence = run.get("page_evidence")
            if not isinstance(page_evidence, list):
                continue
            for item in page_evidence:
                if not isinstance(item, dict):
                    continue
                code = str(item.get("failure_category") or "").strip().lower()
                if code:
                    seen_codes.add(code)
        for code in ordered_codes:
            if code in seen_codes:
                return code
        return ""

    async def _extract_breadcrumb(self, page) -> dict:
        return await extract_breadcrumb(page)

    async def _parse_products(
        self,
        page,
        *,
        rank_offset: int = 0,
        category_path: str = "",
    ) -> list[dict]:
        return await parse_products(
            page,
            rank_offset=rank_offset,
            category_path=category_path,
            detail_fetcher=self._scrape_public_product_details,
        )

    async def _scrape_public_product_details(self, product_url: str) -> dict | None:
        return await scrape_public_product_details(self._new_context, product_url, logger)

    def _parse_raw_product(
        self,
        data: dict,
        *,
        search_rank: int | None = None,
        category_path: str = "",
    ) -> dict:
        return parse_raw_product(
            data,
            search_rank=search_rank,
            category_path=category_path,
        )

    # ── 主流程 ──

    async def run(self) -> dict:
        """
        完整类目爬取流程:
        1. 发现类目树
        2. 逐个子类目爬取商品
        3. 全局 product_id 去重
        4. 保存结果
        """
        start_time = time.time()
        logger.info(f"[category-crawler] 开始爬取类目: {self.category_name}")
        self._acquire_run_lock()

        try:
            await self._start_browser()

            try:
                # 1. 发现类目树（如果指定了目标子类目，直接获取目标；否则发现所有子类目）
                if self.target_subcategory:
                    logger.info(f"[category-crawler] 目标子类目模式：{self.target_subcategory}")
                    subcategories = await self._get_target_subcategory(self.target_subcategory)
                    if not subcategories:
                        logger.error(f"[category-crawler] 未找到目标子类目：{self.target_subcategory}")
                        return {"error": f"target subcategory not found: {self.target_subcategory}"}
                    logger.info(f"[category-crawler] 目标子类目：{len(subcategories)} 个")
                else:
                    subcategories = await self.discover_category_tree()
                    if not subcategories:
                        logger.error(f"[category-crawler] 未发现任何子类目")
                        return {"error": "no subcategories found"}

                # 2. 逐个子类目爬取
                total_raw = 0
                total_deduped = 0
                completed = 0
                skipped = 0

                for i, sub in enumerate(subcategories, 1):
                    sub_name = sub["name"]
                    source_urls = sub.get("source_urls") or [sub["url"]]
                    sub_url = source_urls[0]

                    # 断点续跑
                    if self._is_subcategory_done(sub_name):
                        skipped += 1
                        logger.info(f"[{i}/{len(subcategories)}] 跳过已完成: {sub_name}")
                        continue

                    logger.info(f"[{i}/{len(subcategories)}] 爬取: {sub_name} → {sub_url}")

                    try:
                        products = []
                        source_runs = []
                        for source_url in source_urls:
                            scrape_result = await self.scrape_subcategory(
                                source_url,
                                sub_name,
                                expected_path=sub.get("expected_path"),
                            )
                            products.extend(scrape_result.get("products", []))
                            source_runs.append({
                                "requested_url": scrape_result.get("requested_url", source_url),
                                "resolved_url": scrape_result.get("resolved_url", source_url),
                                "effective_category_path": scrape_result.get("effective_category_path", sub_name),
                                "breadcrumb": scrape_result.get("breadcrumb", {}),
                                "category_filter": scrape_result.get("category_filter", {}),
                                "breadcrumb_match_status": scrape_result.get("breadcrumb_match_status", "unknown"),
                                "product_count": len(scrape_result.get("products", [])),
                                "page_evidence": list(scrape_result.get("page_evidence", [])),
                            })
                        raw_count = len(products)

                        # 仅在当前子类目内部去重，避免组合页/交叉页被前序子类目清空。
                        unique_products = []
                        seen_sub_ids: set[str] = set()
                        for p in products:
                            pid = (
                                p.get("product_id")
                                or p.get("sku")
                                or p.get("product_url")
                                or p.get("url")
                                or p.get("title")
                                or ""
                            )
                            if pid and pid in seen_sub_ids:
                                continue
                            if pid:
                                seen_sub_ids.add(pid)
                            unique_products.append(p)

                        deduped_full_count = len(unique_products)
                        truncated_count = max(0, deduped_full_count - self.max_products_per_sub)
                        if truncated_count:
                            unique_products = unique_products[:self.max_products_per_sub]

                        total_raw += raw_count
                        total_deduped += len(unique_products)

                        primary_run = next(
                            (run for run in source_runs if run.get("breadcrumb", {}).get("items")),
                            source_runs[0] if source_runs else {},
                        )
                        terminal_failure_category = self._terminal_source_run_failure_category(source_runs)
                        if terminal_failure_category:
                            raise RuntimeError(
                                f"{terminal_failure_category}: no products collected for subcategory '{sub_name}'"
                            )

                        self._save_subcategory(sub_name, unique_products, {
                            "url": sub_url,
                            "source_urls": source_urls,
                            "source_runs": source_runs,
                            "resolved_url": primary_run.get("resolved_url", sub_url),
                            "config_id": sub.get("config_id"),
                            "parent_config_id": sub.get("parent_config_id"),
                            "expected_path": sub.get("expected_path", []),
                            "platform_nav_path": sub.get("platform_nav_path", []),
                            "breadcrumb_items": primary_run.get("breadcrumb", {}).get("items", []),
                            "breadcrumb_path": primary_run.get("breadcrumb", {}).get("path", ""),
                            "category_filter_texts": primary_run.get("category_filter", {}).get("texts", []),
                            "category_filter_links": primary_run.get("category_filter", {}).get("links", []),
                            "breadcrumb_match_status": primary_run.get("breadcrumb_match_status", "unknown"),
                            "raw_count": raw_count,
                            "deduped_full_count": deduped_full_count,
                            "deduped_count": len(unique_products),
                            "duplicates_removed": raw_count - deduped_full_count,
                            "truncated_count": truncated_count,
                        })

                        completed += 1
                        self._consecutive_failures = 0
                        logger.info(
                            f"  → {raw_count} 条 → 去重后 {len(unique_products)} 条"
                        )

                    except Exception as e:
                        self._consecutive_failures += 1
                        logger.error(f"[category-crawler] 子类目 '{sub_name}' 失败: {e}")
                        self._errors.append({"subcategory": sub_name, "error": str(e)})

                        # 连续失败暂停
                        if self._consecutive_failures >= self._max_consecutive_failures:
                            logger.warning(f"[category-crawler] 连续 {self._consecutive_failures} 次失败，暂停10分钟")
                            await asyncio.sleep(600)
                            self._consecutive_failures = 0
                            # 重启浏览器
                            await self._restart_browser()

                    # 每个子类目间随机延迟
                    await asyncio.sleep(random.uniform(3, 5))

            finally:
                await self._stop_browser()

            # 3. 合并所有子类目数据
            all_products = self._merge_all_products()
            all_products_path = self.base_dir / "all_products.json"
            self._atomic_write_json(all_products_path, all_products)

            # 4. 保存汇总
            elapsed = time.time() - start_time
            summary = {
                "category": self.category_name,
                "finished_at": datetime.now().isoformat(),
                "duration_minutes": round(elapsed / 60, 1),
                "total_subcategories": len(subcategories),
                "completed": completed,
                "skipped_resume": skipped,
                "total_raw_products": total_raw,
                "total_unique_products": len(all_products),
                "errors": self._errors,
            }
            summary_path = self.base_dir / "crawl_summary.json"
            self._atomic_write_json(summary_path, summary)

            logger.info(
                f"[category-crawler] 完成: {len(subcategories)} 子类目, "
                f"{len(all_products)} 独立商品, "
                f"耗时 {elapsed/60:.1f} 分钟"
            )

            return summary
        finally:
            self._release_run_lock()

    def _is_subcategory_done(self, sub_name: str) -> bool:
        path = self._resolve_subcategory_file_path(sub_name)
        return self._load_subcategory_payload_from_path(path, expected_sub_name=sub_name) is not None

    def _save_subcategory(self, sub_name: str, products: list[dict], meta: dict):
        save_subcategory(
            sub_dir=self.sub_dir,
            sub_name=sub_name,
            products=products,
            meta=meta,
            on_saved=self._subcategory_saved_callback,
        )

    def _load_seen_ids(self):
        self._seen_product_ids.update(load_seen_ids(self.sub_dir, logger=logger))

    def _load_existing_sub_ids(self, sub_name: str):
        self._seen_product_ids.update(
            load_existing_subcategory_seen_ids(self.sub_dir, sub_name, logger=logger)
        )

    def _iter_effective_subcategory_payloads(self) -> list[tuple[Path, dict]]:
        return iter_effective_subcategory_payloads(self.sub_dir, logger=logger)

    def _merge_all_products(self) -> list[dict]:
        return merge_all_products(self.sub_dir, logger=logger)

    # ── 递归深度爬取 ──

    async def _extract_subcategories_from_page(self, page, url: str, depth: int) -> list[dict]:
        return await extract_subcategories_from_page_helper(page, url, depth, logger)

    async def crawl_category_recursive(self, url: str, depth: int = 1,
                                       path: list[str] = None) -> list[dict]:
        return await crawl_category_recursive_helper(
            new_context=self._new_context,
            max_depth=self.max_depth,
            url=url,
            logger=logger,
            depth=depth,
            path=path,
        )
