#!/usr/bin/env python3
"""
noon_zsku_bootstrap.py

给定 noon 商品 URL，自动提取公开商品信息，生成一份可复核的 ZSKU 草稿，
并可直接产出一份基于 NIS 模板的上传文件。

当前定位：
1. 只处理公开商品页 -> 我方 Seller SKU/ZSKU 草稿
2. 不调用 Noon 私有卖家写接口
3. 优先输出可复核数据，再生成 NIS
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

import noon_nis_generator as nis_generator


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "data" / "zsku_drafts"


@dataclass
class CategoryGuess:
    family: str
    product_type: str
    reason: str


CATEGORY_RULES: list[tuple[list[str], CategoryGuess]] = [
    (
        ["dumbbell", "dumbbells"],
        CategoryGuess(
            family="strength_training_equipment",
            product_type="Dumbbells",
            reason="标题/面包屑命中 Dumbbells",
        ),
    ),
    (
        ["kettlebell", "kettlebells"],
        CategoryGuess(
            family="strength_training_equipment",
            product_type="Kettlebells",
            reason="标题/面包屑命中 Kettlebells",
        ),
    ),
    (
        ["barbell"],
        CategoryGuess(
            family="strength_training_equipment",
            product_type="Weight Lifting Bar",
            reason="标题/面包屑命中 Barbell",
        ),
    ),
    (
        ["home gym", "home gyms"],
        CategoryGuess(
            family="strength_training_equipment",
            product_type="Home Gyms",
            reason="标题/面包屑命中 Home Gyms",
        ),
    ),
    (
        ["weight vest", "weight vests"],
        CategoryGuess(
            family="strength_training_equipment",
            product_type="Weight Vests",
            reason="标题/面包屑命中 Weight Vests",
        ),
    ),
    (
        ["exercise bike", "exercise bikes"],
        CategoryGuess(
            family="exercise_machines",
            product_type="Exercise Bikes",
            reason="标题/面包屑命中 Exercise Bikes",
        ),
    ),
    (
        ["exercise mat", "exercise mats", "yoga mat", "yoga mats"],
        CategoryGuess(
            family="accessories",
            product_type="Exercise Mats",
            reason="标题/面包屑命中 Exercise Mats",
        ),
    ),
    (
        ["exercise band", "exercise bands", "resistance band", "resistance bands"],
        CategoryGuess(
            family="accessories",
            product_type="Exercise Bands",
            reason="标题/面包屑命中 Exercise Bands",
        ),
    ),
    (
        ["strength", "fitness", "gym", "exercise"],
        CategoryGuess(
            family="fmsports_outdoor",
            product_type="Strength Training Equipment",
            reason="标题/面包屑命中泛健身词，使用保守兜底类目",
        ),
    ),
]


def sanitize_filename(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return value.strip("._-") or "draft"


def extract_json_ld_objects(raw_items: list[str]) -> list[dict[str, Any]]:
    return _extract_json_ld_objects(raw_items, "")


def _extract_json_ld_objects(raw_items: list[str], html_text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    candidates = list(raw_items)
    if html_text:
        candidates.extend(
            match.group(1)
            for match in re.finditer(
                r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
                html_text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )

    for raw in candidates:
        cleaned = html.unescape(raw).strip()
        if not cleaned:
            continue
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            items.append(payload)
        elif isinstance(payload, list):
            items.extend([item for item in payload if isinstance(item, dict)])
    return items


def find_schema_object(items: list[dict[str, Any]], schema_type: str) -> dict[str, Any] | None:
    wanted = schema_type.lower()
    for item in items:
        raw_type = item.get("@type")
        if isinstance(raw_type, str) and raw_type.lower() == wanted:
            return item
        if isinstance(raw_type, list) and any(str(t).lower() == wanted for t in raw_type):
            return item
    return None


def normalize_text_list(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        cleaned = re.sub(r"\s+", " ", value).strip(" -|,")
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(cleaned)
    return normalized


def pick_first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        cleaned = re.sub(r"\s+", " ", str(value)).strip()
        if cleaned:
            return cleaned
    return ""


def normalize_price_text(value: Any) -> str:
    text = pick_first_text(value)
    if not text:
        return ""
    match = re.search(r"(\d+(?:\.\d{1,2})?)", text.replace(",", ""))
    return match.group(1) if match else text


def normalize_availability_text(value: Any) -> str:
    text = pick_first_text(value)
    if not text:
        return ""
    lowered = text.lower()
    if "out of stock" in lowered:
        return "out_of_stock"
    if "left in stock" in lowered or "in stock" in lowered or "available" in lowered:
        return text
    return text


def build_feature_candidates(title: str, sections: list[dict[str, str]]) -> list[str]:
    candidates: list[str] = []

    for chunk in re.split(r"[|;,/]+", title):
        cleaned = re.sub(r"\s+", " ", chunk).strip()
        if len(cleaned) >= 4:
            candidates.append(cleaned)

    for section in sections:
        label = section.get("label", "").strip()
        text = section.get("text", "").strip()
        if label:
            candidates.append(label)
        if text:
            for chunk in re.split(r"[|;\n]+", text):
                cleaned = re.sub(r"\s+", " ", chunk).strip(" -")
                if 6 <= len(cleaned) <= 160:
                    candidates.append(cleaned)

    filtered: list[str] = []
    for value in normalize_text_list(candidates):
        lower = value.lower()
        if lower.startswith("online shopping for"):
            continue
        if lower in {"overview", "details", "specifications", "highlights", "product details"}:
            continue
        filtered.append(value)

    return filtered[:8]


def infer_category(title: str, breadcrumbs: list[str]) -> CategoryGuess:
    haystack = " | ".join([title, *breadcrumbs]).lower()
    for keywords, guess in CATEGORY_RULES:
        if any(keyword in haystack for keyword in keywords):
            return guess
    return CategoryGuess(
        family="fmsports_outdoor",
        product_type="Strength Training Equipment",
        reason="未命中特定规则，使用保守兜底类目",
    )


def build_partner_sku(source_sku: str, prefix: str) -> str:
    source = re.sub(r"[^A-Za-z0-9_-]+", "", source_sku.upper())
    return f"{prefix}-{source}" if source else f"{prefix}-DRAFT-001"


def validate_category(family: str, product_type: str, nis_template: Path | None) -> tuple[bool, Any]:
    if not nis_template:
        return True, None
    return nis_generator.validate_nis_entry(family, product_type, str(nis_template))


def find_chrome_path() -> str | None:
    candidates = [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    ]
    for path in candidates:
        if path.exists():
            return str(path)
    return None


def scrape_noon_product(
    url: str,
    html_path: Path,
    screenshot_path: Path,
    headless: bool,
) -> dict[str, Any]:
    with sync_playwright() as playwright:
        browser = None
        launch_errors: list[str] = []

        for kwargs in (
            {"channel": "chrome", "headless": headless, "args": ["--disable-blink-features=AutomationControlled"]},
            {"executable_path": find_chrome_path(), "headless": headless, "args": ["--disable-blink-features=AutomationControlled"]},
            {"headless": headless, "args": ["--disable-blink-features=AutomationControlled"]},
        ):
            launch_kwargs = {k: v for k, v in kwargs.items() if v}
            try:
                browser = playwright.chromium.launch(**launch_kwargs)
                break
            except Exception as exc:  # pragma: no cover - fallback path
                launch_errors.append(f"{launch_kwargs}: {exc}")

        if browser is None:
            raise RuntimeError("无法启动浏览器: " + " | ".join(launch_errors))

        context = browser.new_context(
            locale="en-SA",
            viewport={"width": 1440, "height": 2200},
            extra_http_headers={"Accept-Language": "en-SA,en;q=0.9"},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(6000)
        except PlaywrightTimeoutError:
            page.wait_for_timeout(3000)

        # noon 页面常见地区/语言/弹窗，先做温和关闭，不因失败中断。
        for selector in (
            "button[aria-label='Close']",
            "button:has-text('Got it')",
            "button:has-text('Close')",
            "button:has-text('No Thanks')",
        ):
            try:
                locator = page.locator(selector).first
                if locator.is_visible(timeout=1500):
                    locator.click(timeout=1500)
                    page.wait_for_timeout(500)
            except Exception:
                pass

        html_path.write_text(page.content(), encoding="utf-8")
        page.screenshot(path=str(screenshot_path), full_page=True)

        payload = page.evaluate(
            """
            () => {
              const pickText = (selectors) => {
                for (const selector of selectors) {
                  const node = document.querySelector(selector);
                  const text = (node?.textContent || '').replace(/\\s+/g, ' ').trim();
                  if (text) return text;
                }
                return '';
              };

              const ld = [...document.querySelectorAll('script[type="application/ld+json"]')]
                .map((node) => node.textContent || '')
                .filter(Boolean);

              const sectionLabels = /overview|highlights|details|specifications|product details/i;
              const sections = [];
              for (const heading of Array.from(document.querySelectorAll('h2, h3, h4'))) {
                const label = (heading.textContent || '').trim();
                if (!sectionLabels.test(label)) continue;
                const texts = [];
                let node = heading.nextElementSibling;
                let depth = 0;
                while (node && depth < 8 && !/^H[234]$/.test(node.tagName)) {
                  const text = (node.textContent || '').trim();
                  if (text) texts.push(text);
                  node = node.nextElementSibling;
                  depth += 1;
                }
                sections.push({ label, text: texts.join(' | ') });
              }

              const breadcrumbTexts = [...document.querySelectorAll('nav a, [aria-label="breadcrumb"] a')]
                .map((node) => (node.textContent || '').trim())
                .filter(Boolean);

              const nudgeTexts = [...document.querySelectorAll('[class*="nudgeText"], [class*="stock"], [data-qa*="stock"]')]
                .map((node) => (node.textContent || '').replace(/\\s+/g, ' ').trim())
                .filter(Boolean);

              const availabilityText = nudgeTexts.find((text) => /stock|available|left/i.test(text)) || '';

              return {
                url: location.href,
                title: document.querySelector('h1')?.textContent?.trim() || document.title || '',
                metaDescription: document.querySelector('meta[name="description"]')?.content || '',
                ld,
                sections,
                breadcrumbTexts,
                domBrand: pickText([
                  '[class*="BrandStoreCtaV2"] a',
                  'a[href*="/brand/"]',
                  'a[href*="/feierdun/"]',
                ]),
                domSeller: pickText([
                  'strong[class*="soldBy"]',
                  '[class*="soldBy"] strong',
                  '[class*="PartnerRatingsV2"] strong',
                ]),
                domPrice: pickText([
                  '[class*="priceNowText"]',
                  '[data-qa="div-price-now"] strong',
                  '[data-qa="div-price-now"] span:last-child',
                ]),
                domAvailability: availabilityText,
                domNudgeTexts: nudgeTexts,
              };
            }
            """
        )

        context.close()
        browser.close()
        return payload


def fallback_source_sku(source_url: str, html_text: str) -> str:
    for pattern in (
        r"/([A-Z0-9]{12,})/p/",
        r'"sku"\s*:\s*"([A-Z0-9]{12,})"',
        r"https://f\.nooncdn\.com/p/pzsku/([A-Z0-9]{12,})/",
    ):
        match = re.search(pattern, source_url if "/p/" in pattern else html_text)
        if match:
            return match.group(1).strip()
    return ""


def fallback_images(html_text: str) -> list[str]:
    matches = re.findall(r"https://f\.nooncdn\.com/p/pzsku/[^\"]+?\.jpg\?width=\d+", html_text)
    return normalize_text_list(matches)[:7]


def generic_long_description(
    product_name: str,
    brand: str,
    feature_candidates: list[str],
    category_path: str,
) -> str:
    intro = (
        f"<p><b>{brand} {product_name}</b> is a seller-draft listing generated from a public noon page. "
        f"It is positioned under {category_path or 'the selected category'} and should be reviewed before upload.</p>"
    )

    features = ["<p><b>Key Features:</b></p>", "<ul>"]
    for feature in feature_candidates[:6]:
        features.append(f"<li>{feature}</li>")
    features.append("</ul>")

    closing = (
        "<p>Please review brand authorization, exact specifications, package contents, "
        "barcode/GTIN, and compliance details before publishing this Seller SKU.</p>"
    )
    return "\n".join([intro, *features, closing])


def generic_bullets(feature_candidates: list[str]) -> list[str]:
    bullets: list[str] = []
    for feature in feature_candidates[:5]:
        cleaned = re.sub(r"\s+", " ", feature).strip()
        if len(cleaned) < 50:
            cleaned = f"{cleaned} - suitable for daily training and home gym use"
        if len(cleaned) > 150:
            cleaned = cleaned[:150].rsplit(" ", 1)[0]
        bullets.append(cleaned)

    while len(bullets) < 5:
        bullets.append("Seller draft generated from source listing; verify exact product specifications before upload")
    return bullets[:5]


def build_processed_product(draft: dict[str, Any], feature_candidates: list[str]) -> dict[str, Any]:
    title_en = nis_generator.generate_title(
        draft["product_name"],
        draft["brand"],
        feature_candidates[:3],
    )
    return {
        "family": draft["family"],
        "product_type": draft["product_type"],
        "product_subtype": draft.get("product_subtype", ""),
        "sku": draft["sku"],
        "brand": draft["brand"],
        "parent_group_key": "",
        "title_en": title_en,
        "long_description_en": generic_long_description(
            draft["product_name"],
            draft["brand"],
            feature_candidates,
            draft["source_breadcrumbs"],
        ),
        "bullets_en": generic_bullets(feature_candidates),
        "whats_in_box_en": draft["package_contents"] or draft["product_name"],
        "image_urls": [item for item in draft["image_urls"].split(";") if item],
    }


def build_draft_record(
    source_url: str,
    market: str,
    sku_prefix: str,
    scraped: dict[str, Any],
    html_text: str,
    nis_template: Path | None,
) -> dict[str, Any]:
    ld_objects = _extract_json_ld_objects(scraped.get("ld", []), html_text)
    product_schema = find_schema_object(ld_objects, "Product") or {}
    breadcrumb_schema = find_schema_object(ld_objects, "BreadcrumbList") or {}

    title = str(product_schema.get("name") or scraped.get("title") or "").strip()
    brand_raw = product_schema.get("brand") or {}
    if isinstance(brand_raw, dict):
        brand = str(brand_raw.get("name") or "Generic").strip()
    else:
        brand = str(brand_raw or "Generic").strip()

    source_sku = str(product_schema.get("sku") or "").strip()
    price = normalize_price_text(product_schema.get("price") or scraped.get("domPrice"))
    seller_name = pick_first_text(scraped.get("domSeller"))
    availability = normalize_availability_text(scraped.get("domAvailability"))

    offers = product_schema.get("offers") or []
    if isinstance(offers, dict):
        offers = [offers]
    if offers:
        offer = offers[0]
        price = normalize_price_text(offer.get("price") or price)
        availability = normalize_availability_text(offer.get("availability") or availability)
        seller = offer.get("seller") or {}
        if isinstance(seller, dict):
            seller_name = pick_first_text(seller.get("name"), seller_name)

    images = [str(url).strip() for url in product_schema.get("image", []) if str(url).strip()]
    description = str(product_schema.get("description") or scraped.get("metaDescription") or "").strip()

    breadcrumb_items = []
    item_list = breadcrumb_schema.get("itemListElement") or []
    for item in item_list:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if name:
            breadcrumb_items.append(name)
    breadcrumb_items = normalize_text_list([*breadcrumb_items, *scraped.get("breadcrumbTexts", [])])

    if not source_sku:
        source_sku = fallback_source_sku(source_url, html_text)
    if not images:
        images = fallback_images(html_text)
    if not brand or brand == "Generic":
        brand = pick_first_text(scraped.get("domBrand"), brand)
    if not brand or brand == "Generic":
        brand_match = re.search(r"Online shopping for\s+([^.]+)\.", description, flags=re.IGNORECASE)
        if brand_match:
            brand = brand_match.group(1).strip()
    if not seller_name:
        seller_match = re.search(r'"seller"\s*:\s*\{\s*"name"\s*:\s*"([^"]+)"', html_text)
        if seller_match:
            seller_name = seller_match.group(1).strip()
    if not price:
        price_match = re.search(r'"price"\s*:\s*"([^"]+)"', html_text)
        if price_match:
            price = normalize_price_text(price_match.group(1))
    if not availability:
        availability = normalize_availability_text(
            pick_first_text(
                *scraped.get("domNudgeTexts", []),
                re.search(r"(Only\s+\d+\s+left\s+in\s+stock)", html_text, flags=re.IGNORECASE).group(1)
                if re.search(r"(Only\s+\d+\s+left\s+in\s+stock)", html_text, flags=re.IGNORECASE)
                else "",
            )
        )

    category_guess = infer_category(title, breadcrumb_items)
    is_valid, suggestion = validate_category(category_guess.family, category_guess.product_type, nis_template)
    validation_status = "valid" if is_valid else "invalid"

    feature_candidates = build_feature_candidates(title, scraped.get("sections", []))
    key_features = "; ".join(feature_candidates[:6])
    partner_sku = build_partner_sku(source_sku, sku_prefix)

    raw_record = {
        "product_name": title,
        "family": category_guess.family,
        "product_type": category_guess.product_type,
        "product_subtype": "",
        "brand": brand or "Generic",
        "sku": partner_sku,
        "key_features": key_features,
        "material": "",
        "dimensions_cm": "",
        "weight_kg": "",
        "color": "",
        "package_contents": "",
        "image_urls": ";".join(images[:7]),
        "target_market": market,
        "source_url": source_url,
        "source_sku": source_sku,
        "source_brand": brand,
        "source_price": price,
        "source_seller": seller_name,
        "source_availability": availability,
        "source_description": description,
        "source_breadcrumbs": " > ".join(breadcrumb_items),
        "category_reason": category_guess.reason,
        "category_validation_status": validation_status,
        "category_validation_suggestion": json.dumps(suggestion, ensure_ascii=False) if suggestion else "",
    }
    return raw_record


def write_csv_row(record: dict[str, Any], path: Path) -> None:
    fieldnames = [
        "product_name",
        "family",
        "product_type",
        "product_subtype",
        "brand",
        "sku",
        "key_features",
        "material",
        "dimensions_cm",
        "weight_kg",
        "color",
        "package_contents",
        "image_urls",
        "target_market",
        "source_url",
        "source_sku",
        "source_brand",
        "source_price",
        "source_seller",
        "source_availability",
        "source_description",
        "source_breadcrumbs",
        "category_reason",
        "category_validation_status",
        "category_validation_suggestion",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({key: record.get(key, "") for key in fieldnames})


def build_output_paths(output_dir: Path, slug: str) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    return {
        "json": output_dir / f"{slug}.json",
        "csv": output_dir / f"{slug}.csv",
        "xlsx": output_dir / f"{slug}.xlsx",
        "html": output_dir / f"{slug}.html",
        "png": output_dir / f"{slug}.png",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="从 noon 商品 URL 生成 ZSKU/NIS 草稿")
    parser.add_argument("--url", required=True, help="公开 noon 商品 URL")
    parser.add_argument("--market", default="KSA", help="目标市场，默认 KSA")
    parser.add_argument("--sku-prefix", default="COPY", help="生成 Partner SKU 的前缀")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录")
    parser.add_argument("--slug", default="", help="输出文件名 slug，默认根据源 SKU 自动生成")
    parser.add_argument("--nis-template", default="", help="可选：NIS.xlsx 模板路径")
    parser.add_argument("--headless", action="store_true", help="以 headless 模式抓取页面")
    args = parser.parse_args()

    nis_template = Path(args.nis_template).resolve() if args.nis_template else None
    if nis_template and not nis_template.exists():
        print(f"ERROR: NIS template not found: {nis_template}")
        return 1

    market = str(args.market or "KSA").strip().upper()
    if market not in {"KSA", "UAE"}:
        print("ERROR: market must be KSA or UAE")
        return 1

    provisional_slug = sanitize_filename(args.slug) if args.slug else "noon_zsku_draft"
    provisional_paths = build_output_paths(Path(args.output_dir).resolve(), provisional_slug)

    print("[1/4] Scraping public noon product page...")
    scraped = scrape_noon_product(
        url=args.url,
        html_path=provisional_paths["html"],
        screenshot_path=provisional_paths["png"],
        headless=args.headless,
    )
    html_text = provisional_paths["html"].read_text(encoding="utf-8", errors="ignore")

    draft = build_draft_record(
        source_url=args.url,
        market=market,
        sku_prefix=args.sku_prefix,
        scraped=scraped,
        html_text=html_text,
        nis_template=nis_template,
    )

    final_slug = sanitize_filename(args.slug or draft.get("source_sku") or provisional_slug)
    if final_slug != provisional_slug:
        final_paths = build_output_paths(Path(args.output_dir).resolve(), final_slug)
        provisional_paths["html"].replace(final_paths["html"])
        provisional_paths["png"].replace(final_paths["png"])
    else:
        final_paths = provisional_paths

    print("[2/4] Writing extracted draft data...")
    feature_candidates = [item.strip() for item in draft["key_features"].split(";") if item.strip()]
    processed = build_processed_product(draft, feature_candidates)

    payload = {
        "source": {
            "url": draft["source_url"],
            "sku": draft["source_sku"],
            "brand": draft["source_brand"],
            "seller": draft["source_seller"],
            "price": draft["source_price"],
            "availability": draft["source_availability"],
            "breadcrumbs": draft["source_breadcrumbs"],
        },
        "draft": draft,
        "generated_nis_fields": processed,
    }
    final_paths["json"].write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv_row(draft, final_paths["csv"])

    print("[3/4] Generating NIS workbook...")
    if nis_template:
        nis_generator.generate_nis_file([processed], str(final_paths["xlsx"]), str(nis_template))
    else:
        print("       Skipped XLSX generation because --nis-template was not provided.")

    print("[4/4] Done")
    print(f"       JSON: {final_paths['json']}")
    print(f"       CSV : {final_paths['csv']}")
    if nis_template:
        print(f"       XLSX: {final_paths['xlsx']}")
    print(f"       HTML: {final_paths['html']}")
    print(f"       PNG : {final_paths['png']}")
    print(f"       Family/Product Type: {draft['family']} / {draft['product_type']}")
    print(f"       Category reason: {draft['category_reason']}")
    print(f"       Validation: {draft['category_validation_status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
