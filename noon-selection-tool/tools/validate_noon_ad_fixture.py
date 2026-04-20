import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.noon_category_crawler import NoonCategoryCrawler


ROOT = Path(__file__).resolve().parents[1]
PROBE_JSON = ROOT / "data" / "adhoc_runs" / "ad_overlay_probe_20260328" / "multi_query_overlay_scan.json"
OUTPUT_DIR = ROOT / "data" / "reports" / "ad_fixture_validation_20260328"


def _load_fixture_rows() -> tuple[dict, dict]:
    data = json.loads(PROBE_JSON.read_text(encoding="utf-8"))
    positive = None
    negative = None
    for block in data:
        for row in block.get("rows", []):
            overlay_html = row.get("overlayHtml", "") or ""
            if not positive and "STag-module" in overlay_html:
                positive = row
            if not negative and "BestSellerTag-module" in overlay_html:
                negative = row
            if positive and negative:
                return positive, negative
    raise RuntimeError("未找到足够的广告/非广告夹具样本")


def _build_fixture_html(positive: dict, negative: dict) -> str:
    def build_card(title: str, overlay_html: str, price: str) -> str:
        return f"""
        <div class="FixtureProductBox">
          <a href="/saudi-en/p/fake-product/">
            <div data-qa="productImagePLP_{title}" class="ProductImageSection-module-scss-module__8NuU7G__imageSection">
              {overlay_html}
            </div>
            <div data-qa="plp-product-brand">Fixture Brand</div>
            <div data-qa="plp-product-box-name">{title}</div>
            <div data-qa="plp-product-box-price">{price}</div>
          </a>
        </div>
        """

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Noon Ad Fixture Validation</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 24px;
      background: #f5f5f5;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(260px, 320px));
      gap: 20px;
    }}
    .FixtureProductBox {{
      background: white;
      padding: 16px;
      border-radius: 16px;
      min-height: 260px;
    }}
    [data-qa="plp-product-box-name"] {{
      display: block;
      margin-top: 12px;
      font-size: 14px;
      line-height: 1.4;
      color: #222;
    }}
    [data-qa="plp-product-box-price"] {{
      margin-top: 8px;
      font-size: 18px;
      font-weight: 700;
    }}
  </style>
</head>
<body>
  <div class="grid">
    {build_card(positive.get("title", "Positive Ad Fixture"), positive.get("overlayHtml", ""), "99")}
    {build_card(negative.get("title", "Negative Fixture"), negative.get("overlayHtml", ""), "88")}
  </div>
</body>
</html>
"""


async def main() -> None:
    positive, negative = _load_fixture_rows()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fixture_html = OUTPUT_DIR / "ad_fixture_validation.html"
    fixture_html.write_text(_build_fixture_html(positive, negative), encoding="utf-8")

    crawler = NoonCategoryCrawler("electronics", OUTPUT_DIR, max_products_per_sub=2)
    await crawler._start_browser()
    try:
        ctx = await crawler._new_context()
        page = await ctx.new_page()
        await page.goto(fixture_html.resolve().as_uri(), wait_until="load", timeout=30000)
        parsed = await crawler._parse_products(page, category_path="Fixture > Ad Validation")
        await page.close()
        await ctx.close()
    finally:
        await crawler._stop_browser()

    positive_result = next((item for item in parsed if item.get("title") == positive.get("title")), {})
    negative_result = next((item for item in parsed if item.get("title") == negative.get("title")), {})

    report = {
        "fixture_html": str(fixture_html),
        "positive_source_title": positive.get("title"),
        "negative_source_title": negative.get("title"),
        "positive_expected_ad": True,
        "positive_detected_is_ad": bool(positive_result.get("is_ad")),
        "positive_ad_marker_texts": positive_result.get("ad_marker_texts", []),
        "negative_expected_ad": False,
        "negative_detected_is_ad": bool(negative_result.get("is_ad")),
        "negative_ad_marker_texts": negative_result.get("ad_marker_texts", []),
        "status": "pass" if positive_result.get("is_ad") and not negative_result.get("is_ad") else "fail",
    }

    report_path = OUTPUT_DIR / "ad_fixture_validation_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
