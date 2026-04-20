"""
SHEIN 沙特站爬虫 — 付费 Scraper API 调用

支持 Bright Data / Oxylabs / ScraperAPI 三种提供商。
.env 中配置 SHEIN_API_KEY 和 SHEIN_API_PROVIDER。

API Key 未配置时跳过采集，相关字段留空，不影响整体流程。
"""
import json
import logging

import httpx

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class SheinScraper(BaseScraper):

    def __init__(self, settings):
        super().__init__("shein", settings)
        self.api_key = settings.shein_api_key
        self.provider = settings.shein_api_provider

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def scrape_keyword(self, keyword: str) -> dict:
        """通过付费 API 搜索 SHEIN 沙特站"""
        result = {
            "keyword": keyword,
            "total_results": 0,
            "products": [],
        }

        if not self.is_configured:
            result["error"] = "SHEIN API 未配置，跳过"
            return result

        try:
            if self.provider == "brightdata":
                result = await self._fetch_brightdata(keyword, result)
            elif self.provider == "oxylabs":
                result = await self._fetch_oxylabs(keyword, result)
            elif self.provider == "scraperapi":
                result = await self._fetch_scraperapi(keyword, result)
            else:
                result["error"] = f"未知 API 提供商: {self.provider}"
        except Exception as e:
            logger.warning(f"[shein] '{keyword}' API调用失败: {e}")
            result["error"] = str(e)

        return result

    async def _fetch_brightdata(self, keyword: str, result: dict) -> dict:
        """Bright Data SERP API"""
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.brightdata.com/serp/req",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "zone": "shein",
                    "url": f"https://www.shein.com/ar-sa/pdsearch/{keyword}/",
                    "country": "sa",
                    "format": "json",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            # 解析 Bright Data 返回格式
            if isinstance(data, dict):
                result["total_results"] = data.get("total_results", 0)
                raw_products = data.get("products", data.get("results", []))
                result["products"] = self._normalize_products(raw_products[:20])

        return result

    async def _fetch_oxylabs(self, keyword: str, result: dict) -> dict:
        """Oxylabs E-Commerce Scraper API"""
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://realtime.oxylabs.io/v1/queries",
                auth=(self.api_key, ""),
                json={
                    "source": "universal_ecommerce",
                    "url": f"https://www.shein.com/ar-sa/pdsearch/{keyword}/",
                    "geo_location": "Saudi Arabia",
                    "render": "html",
                    "parse": True,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            results_list = data.get("results", [])
            if results_list:
                content = results_list[0].get("content", {})
                result["total_results"] = content.get("total_results", 0)
                result["products"] = self._normalize_products(
                    content.get("results", {}).get("organic", [])[:20]
                )

        return result

    async def _fetch_scraperapi(self, keyword: str, result: dict) -> dict:
        """ScraperAPI"""
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(
                "https://api.scraperapi.com",
                params={
                    "api_key": self.api_key,
                    "url": f"https://www.shein.com/ar-sa/pdsearch/{keyword}/",
                    "country_code": "sa",
                    "render": "true",
                },
            )
            resp.raise_for_status()
            # ScraperAPI 返回原始 HTML，需要解析
            # 这里简化处理，实际应用中需要用 BeautifulSoup 解析
            result["error"] = "ScraperAPI 返回 HTML，需实现 HTML 解析"

        return result

    def _normalize_products(self, raw_products: list) -> list[dict]:
        """将不同 API 返回格式标准化"""
        products = []
        for p in raw_products:
            product = {
                "title": p.get("title", p.get("name", "")),
                "price": 0.0,
                "discount_price": None,
                "sold_count": 0,
                "review_count": 0,
                "rating": None,
            }

            # 价格（尝试多种字段名）
            for key in ["price", "sale_price", "current_price"]:
                if key in p:
                    try:
                        product["price"] = float(str(p[key]).replace("SAR", "").replace(",", "").strip())
                        break
                    except (ValueError, TypeError):
                        continue

            # 折扣价
            for key in ["original_price", "retail_price"]:
                if key in p:
                    try:
                        product["discount_price"] = float(str(p[key]).replace("SAR", "").replace(",", "").strip())
                        break
                    except (ValueError, TypeError):
                        continue

            # 销量
            for key in ["sold_count", "sales", "sold"]:
                if key in p:
                    try:
                        product["sold_count"] = int(str(p[key]).replace(",", "").replace("+", ""))
                        break
                    except (ValueError, TypeError):
                        continue

            # 评论数
            for key in ["review_count", "reviews", "comment_count"]:
                if key in p:
                    try:
                        product["review_count"] = int(str(p[key]).replace(",", ""))
                        break
                    except (ValueError, TypeError):
                        continue

            # 评分
            for key in ["rating", "star"]:
                if key in p:
                    try:
                        product["rating"] = float(p[key])
                        break
                    except (ValueError, TypeError):
                        continue

            if product["title"]:
                products.append(product)

        return products

    async def run(self, keywords: list[str]) -> list[dict]:
        """重写 run：SHEIN 不需要浏览器，直接用 API"""
        if not self.is_configured:
            logger.warning("[shein] API 未配置（SHEIN_API_KEY 为空），跳过全部 SHEIN 采集")
            return []

        # 过滤已完成
        pending = [kw for kw in keywords if not self.is_completed(kw)]
        skipped = len(keywords) - len(pending)
        if skipped > 0:
            logger.info(f"[shein] 跳过 {skipped} 个已完成关键词")

        results = []
        for i, kw in enumerate(pending, 1):
            result = await self.scrape_with_retry(kw)
            if result:
                results.append(result)
            if i % 10 == 0:
                logger.info(f"[shein] 进度: {i}/{len(pending)}")
            await self.random_delay()

        return results
