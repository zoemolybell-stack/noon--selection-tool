"""
Amazon.sa scraper.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

from scrapers.amazon_product_parser import parse_amazon_product_payload
from scrapers.base_scraper import BaseScraper

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - environment fallback
    BeautifulSoup = None

logger = logging.getLogger(__name__)

AMAZON_BASE = "https://www.amazon.sa"
AMAZON_SEARCH_LANGUAGE = "en_AE"
AMAZON_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
AMAZON_ERROR_MARKERS = (
    "\u0646\u0639\u062a\u0630\u0631",
    "\u062d\u062f\u062b \u062e\u0637\u0623",
    "something went wrong",
    "we're sorry",
    "sorry! something went wrong",
)
AMAZON_ZERO_RESULT_MARKERS = (
    "no results for",
    "try checking your spelling",
    "did you mean",
    "did not match any products",
    "\u0644\u0645 \u064a\u062a\u0645 \u0627\u0644\u0639\u062b\u0648\u0631",
)


class AmazonScraper(BaseScraper):
    def __init__(self, settings, max_products: int | None = None):
        super().__init__("amazon", settings)
        self._max_products = max_products or settings.products_per_keyword

    def _search_url(self, keyword: str, page_num: int = 1) -> str:
        return f"{AMAZON_BASE}/s?k={quote_plus(keyword)}&page={page_num}&language={AMAZON_SEARCH_LANGUAGE}"

    def _normalize_text(self, value: Any) -> str:
        return " ".join(str(value or "").split())

    def _build_error_evidence(self, title: str, body_text: str, *, url: str) -> list[str]:
        haystack = f"{title}\n{body_text}".lower()
        evidence: list[str] = []
        for marker in AMAZON_ERROR_MARKERS:
            if marker.lower() in haystack:
                evidence.append(f"external_site_error:{marker}")
        if evidence and url:
            evidence.append(f"url:{url}")
        if not evidence and title.strip():
            evidence.append("page_recognition_failed:amazon_search_markup")
            evidence.append(f"title:{title.strip()[:120]}")
        if not evidence and url:
            evidence.append("page_recognition_failed:amazon_search_markup")
            evidence.append(f"url:{url}")
        return evidence[:5]

    def _build_zero_result_evidence(self, title: str, body_text: str, *, url: str) -> list[str]:
        haystack = f"{title}\n{body_text}".lower()
        evidence: list[str] = []
        for marker in AMAZON_ZERO_RESULT_MARKERS:
            if marker.lower() in haystack:
                evidence.append(marker)
        if evidence and url:
            evidence.append(f"url:{url}")
        return evidence[:5]

    def _extract_total_results_text(self, text: str) -> int:
        numbers = re.findall(r"[\d,]+", text or "")
        if not numbers:
            return 0
        try:
            return max(int(value.replace(",", "")) for value in numbers)
        except ValueError:
            return 0

    def _soup_card_nodes(self, soup) -> list[Any]:
        cards = soup.select('[data-component-type="s-search-result"]')
        if cards:
            return cards
        cards = soup.select('div.s-main-slot div[data-asin]')
        return [card for card in cards if (card.get("data-asin") or "").strip()]

    def _extract_total_results_from_soup(self, soup) -> int:
        for selector in (
            'span[data-component-type="s-result-info-bar"]',
            'div.s-breadcrumb',
            'div.sg-col-inner',
        ):
            node = soup.select_one(selector)
            if not node:
                continue
            total = self._extract_total_results_text(node.get_text(" ", strip=True))
            if total:
                return total
        return 0

    def _is_upstream_error_text(self, value: Any) -> bool:
        lowered = self._normalize_text(value).lower()
        return any(
            token in lowered
            for token in (
                "timeout",
                "timed out",
                "net::err",
                "service unavailable",
                "temporarily unavailable",
                "something went wrong",
                "we're sorry",
                "robot",
                "captcha",
                "http error 503",
            )
        )

    def _parse_card_from_soup(self, card: Any, *, search_rank: int = 0) -> dict:
        payload: dict[str, Any] = {
            "title": "",
            "asin": (card.get("data-asin") or "").strip(),
            "href": "",
            "priceText": "",
            "originalPriceText": "",
            "ratingText": "",
            "reviewText": "",
            "sellerText": "",
            "brandText": "",
            "cardText": self._normalize_text(card.get_text(" ", strip=True)),
            "imageCount": len(card.select('img[src]')),
            "isPrime": False,
            "isChoice": False,
            "isBestSeller": False,
            "isAd": False,
            "productUrl": "",
        }
        card_html = str(card)

        for selector in ("h2 a span", "h2 span", '[data-cy="title-recipe"] span'):
            node = card.select_one(selector)
            if node and node.get_text(" ", strip=True):
                payload["title"] = self._normalize_text(node.get_text(" ", strip=True))
                break

        link_node = None
        for selector in ("h2 a[href]", 'a.a-link-normal[href*="/dp/"]', 'a[href*="/dp/"]'):
            node = card.select_one(selector)
            if node and node.get("href"):
                link_node = node
                break
        if link_node is not None:
            href = link_node.get("href") or ""
            if href:
                full_url = f"{AMAZON_BASE}{href}" if href.startswith("/") else href
                payload["href"] = full_url
                payload["productUrl"] = full_url
                if not payload["asin"]:
                    asin_match = re.search(r"/dp/([A-Z0-9]{10})", full_url)
                    if asin_match:
                        payload["asin"] = asin_match.group(1)

        if not payload["productUrl"]:
            href_match = re.search(r'href=["\']([^"\']*/dp/[A-Z0-9]{10}[^"\']*)["\']', card_html, re.IGNORECASE)
            if href_match:
                href = href_match.group(1)
                full_url = f"{AMAZON_BASE}{href}" if href.startswith("/") else href
                payload["href"] = full_url
                payload["productUrl"] = full_url

        if payload["asin"] and not payload["productUrl"]:
            payload["productUrl"] = f"{AMAZON_BASE}/dp/{payload['asin']}"

        price_node = card.select_one(".a-price .a-offscreen")
        if price_node:
            payload["priceText"] = self._normalize_text(price_node.get_text(" ", strip=True))

        original_price_node = card.select_one(".a-price.a-text-price .a-offscreen")
        if original_price_node:
            payload["originalPriceText"] = self._normalize_text(
                original_price_node.get_text(" ", strip=True)
            )

        rating_node = card.select_one(".a-icon-alt")
        if rating_node:
            payload["ratingText"] = self._normalize_text(rating_node.get_text(" ", strip=True))

        review_node = card.select_one('a[href*="customerReviews"] span, span.a-size-base.s-underline-text')
        if review_node:
            payload["reviewText"] = self._normalize_text(review_node.get_text(" ", strip=True))

        badge_text = payload["cardText"].lower()
        payload["isBestSeller"] = "best seller" in badge_text
        payload["isChoice"] = "amazon's choice" in badge_text
        payload["isPrime"] = " prime " in f" {badge_text} " or "free delivery" in badge_text
        payload["isAd"] = "sponsored" in badge_text

        brand_node = card.select_one('h5 span, div[data-cy="title-recipe"] + div span')
        if brand_node:
            candidate = self._normalize_text(brand_node.get_text(" ", strip=True))
            if candidate and not re.fullmatch(r"[\d.]+", candidate):
                payload["brandText"] = candidate

        seller_node = card.select_one("span.a-size-base-plus.a-color-base")
        if seller_node:
            payload["sellerText"] = self._normalize_text(seller_node.get_text(" ", strip=True))

        return parse_amazon_product_payload(payload, search_rank=search_rank)

    async def _fetch_search_html_via_browser(self, url: str) -> str:
        ctx = await self.new_context()
        page = await ctx.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(3500)
            return await page.content()
        finally:
            await page.close()
            await ctx.close()

    async def _fetch_search_html(self, keyword: str, page_num: int) -> tuple[str, str, str, list[str]]:
        url = self._search_url(keyword, page_num)
        request = Request(url, headers=AMAZON_HTTP_HEADERS)
        fetch_evidence: list[str] = []
        last_exc: Exception | None = None
        for attempt, timeout_seconds in enumerate((30, 45), start=1):
            try:
                with urlopen(request, timeout=timeout_seconds) as response:
                    html = response.read().decode("utf-8", "replace")
                if attempt > 1:
                    fetch_evidence.append(f"http_retry_success:{attempt}")
                return url, html, "http_html", fetch_evidence
            except Exception as exc:
                last_exc = exc
                fetch_evidence.append(f"http_attempt_{attempt}:{exc.__class__.__name__}")
                if not self._is_upstream_error_text(exc) or attempt >= 2:
                    break
                await asyncio.sleep(1.5 * attempt)
        if self._browser is not None:
            try:
                html = await self._fetch_search_html_via_browser(url)
                fetch_evidence.append("browser_fallback:http_html_failed")
                return url, html, "browser_html", fetch_evidence
            except Exception as exc:
                last_exc = exc
                fetch_evidence.append(f"browser_fallback_failed:{exc.__class__.__name__}")
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"amazon_search_fetch_failed:{url}")

    async def _scrape_keyword_via_http(self, keyword: str) -> dict:
        result = self._build_result_payload(
            keyword,
            page_state="unknown",
            extra={
                "page_title": "",
                "page_url": "",
                "data_source": "http_html",
                "result_card_count": 0,
            },
        )

        def dedupe_texts(values: list[str]) -> list[str]:
            output: list[str] = []
            seen: set[str] = set()
            for raw in values:
                text = self._normalize_text(raw)
                if not text:
                    continue
                lowered = text.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                output.append(text)
            return output

        if BeautifulSoup is None:
            result["page_state"] = "error"
            result["error_evidence"] = [
                "dependency_missing:beautifulsoup4",
                "beautifulsoup4_unavailable",
            ]
            result["failure_details"] = [
                self._build_failure_detail(
                    keyword,
                    failure_category="dependency_missing",
                    short_evidence="beautifulsoup4_unavailable",
                    page_state="error",
                )
            ]
            return self._normalize_result_payload(keyword, result)

        target = self._max_products
        page_num = 1
        all_products: list[dict] = []
        zero_result_evidence: list[str] = []
        last_error_evidence: list[str] = []
        fetch_evidence: list[str] = []
        failure_details: list[dict[str, Any]] = []
        last_title = ""
        last_url = ""
        last_data_source = str(result.get("data_source") or "http_html")

        while len(all_products) < target and page_num <= 5:
            try:
                url, html, data_source, page_fetch_evidence = await self._fetch_search_html(keyword, page_num)
                soup = BeautifulSoup(html, "html.parser")
                title = self._normalize_text(soup.title.get_text(" ", strip=True) if soup.title else "")
                body_text = self._normalize_text(soup.get_text(" ", strip=True))
                last_title = title
                last_url = url
                last_data_source = data_source
                fetch_evidence = dedupe_texts(fetch_evidence + page_fetch_evidence)

                cards = self._soup_card_nodes(soup)
                if page_num == 1:
                    result["result_card_count"] = len(cards)
                    result["page_title"] = title
                    result["page_url"] = url
                    result["data_source"] = data_source
                    result["total_results"] = self._extract_total_results_from_soup(soup)

                if cards:
                    for card in cards:
                        product = self._parse_card_from_soup(card, search_rank=len(all_products) + 1)
                        if product.get("product_id") and product.get("title"):
                            all_products.append(product)
                        if len(all_products) >= target:
                            break
                    if len(all_products) >= target:
                        break
                    page_num += 1
                    continue

                if data_source != "browser_html" and self._browser is not None:
                    try:
                        browser_html = await self._fetch_search_html_via_browser(url)
                        browser_soup = BeautifulSoup(browser_html, "html.parser")
                        browser_title = self._normalize_text(browser_soup.title.get_text(" ", strip=True) if browser_soup.title else "")
                        browser_body_text = self._normalize_text(browser_soup.get_text(" ", strip=True))
                        browser_cards = self._soup_card_nodes(browser_soup)
                        if browser_cards:
                            fetch_evidence = dedupe_texts(fetch_evidence + ["browser_fallback:empty_http_cards"])
                            last_title = browser_title or last_title
                            last_url = url
                            last_data_source = "browser_html"
                            if page_num == 1:
                                result["page_title"] = browser_title or title
                                result["page_url"] = url
                                result["data_source"] = "browser_html"
                                result["result_card_count"] = len(browser_cards)
                                result["total_results"] = self._extract_total_results_from_soup(browser_soup)
                            for card in browser_cards:
                                product = self._parse_card_from_soup(card, search_rank=len(all_products) + 1)
                                if product.get("product_id") and product.get("title"):
                                    all_products.append(product)
                                if len(all_products) >= target:
                                    break
                            if len(all_products) >= target:
                                break
                            page_num += 1
                            continue
                        title = browser_title or title
                        body_text = browser_body_text or body_text
                        fetch_evidence = dedupe_texts(fetch_evidence + ["browser_fallback:empty_http_cards_no_match"])
                    except Exception as browser_exc:
                        fetch_evidence = dedupe_texts(fetch_evidence + [f"browser_fallback_failed:{browser_exc.__class__.__name__}"])

                zero_result_evidence = self._build_zero_result_evidence(title, body_text, url=url)
                if zero_result_evidence:
                    result["page_state"] = "zero_results"
                    result["zero_result_evidence"] = zero_result_evidence
                    failure_details.append(
                        self._build_failure_detail(
                            keyword,
                            failure_category="zero_results",
                            short_evidence=zero_result_evidence[0],
                            page_url=url,
                            page_number=page_num,
                            page_state="zero_results",
                        )
                    )
                    break

                last_error_evidence = self._build_error_evidence(title, body_text, url=url)
                failure_category = (
                    "amazon_upstream_blocked"
                    if any(item.lower().startswith("external_site_error:") for item in last_error_evidence)
                    else "amazon_parse_failure"
                )
                result["page_state"] = "partial_results" if all_products else "error"
                result["error_evidence"] = dedupe_texts(last_error_evidence + fetch_evidence)
                failure_details.append(
                    self._build_failure_detail(
                        keyword,
                        failure_category=failure_category,
                        short_evidence=last_error_evidence[0] if last_error_evidence else "amazon_page_recognition_failed",
                        page_url=url,
                        page_number=page_num,
                        page_state=result["page_state"],
                    )
                )
                break
            except Exception as exc:
                error_text = self._normalize_text(str(exc))
                lowered = error_text.lower()
                failure_category = (
                    "amazon_upstream_blocked"
                    if any(token in lowered for token in ("timeout", "timed out", "net::err", "service unavailable"))
                    else "amazon_parse_failure"
                )
                result["page_state"] = "partial_results" if all_products else "error"
                result["page_url"] = last_url or result.get("page_url") or ""
                result["data_source"] = last_data_source
                result["error_evidence"] = dedupe_texts(
                    list(result.get("error_evidence") or [])
                    + fetch_evidence
                    + [f"{failure_category}:{exc.__class__.__name__}", error_text]
                )
                failure_details.append(
                    self._build_failure_detail(
                        keyword,
                        failure_category=failure_category,
                        short_evidence=error_text or exc.__class__.__name__,
                        page_url=last_url,
                        page_number=page_num,
                        page_state=result["page_state"],
                    )
                )
                break

        result["products"] = all_products[:target]
        if result["products"]:
            if result["page_state"] == "unknown":
                result["page_state"] = "results"
            result["result_card_count"] = max(result["result_card_count"], len(result["products"]))
        elif result["page_state"] == "unknown":
            if zero_result_evidence:
                result["page_state"] = "zero_results"
                result["zero_result_evidence"] = zero_result_evidence
            else:
                result["page_state"] = "error"
                result["error_evidence"] = last_error_evidence or self._build_error_evidence(
                    last_title,
                    "",
                    url=last_url,
                )
                result["error_evidence"] = dedupe_texts(list(result.get("error_evidence") or []) + fetch_evidence)
                failure_details.append(
                    self._build_failure_detail(
                        keyword,
                        failure_category="amazon_parse_failure",
                        short_evidence=(result["error_evidence"][0] if result["error_evidence"] else "amazon_search_unclassified"),
                        page_url=last_url,
                        page_number=page_num,
                        page_state="error",
                    )
                )

        result["error_evidence"] = dedupe_texts(list(result.get("error_evidence") or []))
        result["zero_result_evidence"] = dedupe_texts(list(result.get("zero_result_evidence") or []))
        result["failure_details"] = failure_details
        return self._normalize_result_payload(keyword, result)

    async def scrape_keyword(self, keyword: str) -> dict:
        """Scrape Amazon.sa search results for one keyword."""
        result = await self._scrape_keyword_via_http(keyword)
        if result.get("page_state") == "results":
            logger.info(
                "[amazon] '%s' scraped via %s: products=%s total_results=%s",
                keyword,
                result.get("data_source"),
                len(result.get("products") or []),
                result.get("total_results"),
            )
        elif result.get("page_state") == "zero_results":
            logger.info("[amazon] '%s' returned zero results", keyword)
        else:
            logger.warning(
                "[amazon] '%s' returned error page: %s",
                keyword,
                ", ".join(result.get("error_evidence") or []),
            )
        return result

    async def scrape_bestsellers(self) -> list[dict]:
        """
        Scrape Amazon.sa Best Sellers pages for reverse keyword discovery.
        """
        ctx = await self.new_context()
        page = await ctx.new_page()
        items = []

        try:
            await page.goto(f"{AMAZON_BASE}/gp/bestsellers/", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            cat_links = page.locator('a[href*="/bestsellers/"]')
            cat_count = await cat_links.count()
            cat_urls = []
            for index in range(min(cat_count, 20)):
                href = await cat_links.nth(index).get_attribute("href")
                if href and "/bestsellers/" in href:
                    full_url = f"{AMAZON_BASE}{href}" if href.startswith("/") else href
                    if full_url not in cat_urls:
                        cat_urls.append(full_url)

            for cat_url in cat_urls[:10]:
                try:
                    await page.goto(cat_url, wait_until="domcontentloaded", timeout=20000)
                    await page.wait_for_timeout(1500)

                    product_cards = page.locator('[class*="zg-item"], [class*="p13n-sc-uncoverable-faceout"]')
                    count = await product_cards.count()

                    for index in range(min(count, 20)):
                        try:
                            card = product_cards.nth(index)
                            title_el = card.locator('a span, [class*="title"]')
                            price_el = card.locator('[class*="price"]')

                            title = ""
                            if await title_el.count() > 0:
                                title = (await title_el.first.inner_text()).strip()

                            price = 0.0
                            if await price_el.count() > 0:
                                text = await price_el.first.inner_text()
                                nums = re.findall(r'[\d.]+', text.replace(",", ""))
                                if nums:
                                    price = float(nums[0])

                            if title:
                                items.append(
                                    {
                                        "title": title,
                                        "rank": index + 1,
                                        "price": price,
                                        "source_url": cat_url,
                                    }
                                )
                        except Exception:
                            continue

                    await self.random_delay()
                except Exception as exc:
                    logger.debug(f"[amazon] BSR category page failed: {exc}")
                    continue

        except Exception as exc:
            logger.error(f"[amazon] Best Sellers scrape failed: {exc}")
        finally:
            await page.close()
            await ctx.close()

        logger.info(f"[amazon] scraped {len(items)} bestseller items")
        return items

    async def scrape_product_details(self, product_url: str) -> dict:
        """Scrape dimensions and weight from an Amazon product detail page."""
        ctx = await self.new_context()
        page = await ctx.new_page()
        details = {"dimensions": None, "weight_kg": None}

        try:
            await page.goto(product_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            specs_text = ""
            for selector in ['#productDetails_techSpec_section_1', '#detailBullets_feature_div', '#prodDetails']:
                element = page.locator(selector)
                if await element.count() > 0:
                    specs_text = await element.first.inner_text()
                    break

            dim_match = re.search(
                r'(\d+\.?\d*)\s*[x脳]\s*(\d+\.?\d*)\s*[x脳]\s*(\d+\.?\d*)\s*cm',
                specs_text,
                re.IGNORECASE,
            )
            if dim_match:
                details["dimensions"] = {
                    "l": float(dim_match.group(1)),
                    "w": float(dim_match.group(2)),
                    "h": float(dim_match.group(3)),
                }

            wt_match = re.search(r'(\d+\.?\d*)\s*(kg|kilogram|g|gram)', specs_text, re.IGNORECASE)
            if wt_match:
                weight = float(wt_match.group(1))
                unit = wt_match.group(2).lower()
                if unit.startswith("g") and weight > 10:
                    weight /= 1000
                details["weight_kg"] = weight

        except Exception as exc:
            logger.debug(f"[amazon] detail page failed: {exc}")
        finally:
            await page.close()
            await ctx.close()

        return details
