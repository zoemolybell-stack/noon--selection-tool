"""
Noon.com 爬虫 — 搜索结果 + Express筛选 + 联想词 + 详情页尺寸

搜索模式: 每个关键词抓取 total_results, express_count, 商品列表, 联想词
详情页模式: 进入商品页抽取规格参数（尺寸/重量），仅模式1精确采集时使用
"""
import logging
import re
import time
from urllib.parse import parse_qs, urlparse

from scrapers.base_scraper import BaseScraper
from scrapers.noon_delivery_detection import DELIVERY_MARKER_JS_PATTERN
from scrapers.noon_product_parser import parse_noon_product_payload

logger = logging.getLogger(__name__)

NOON_BASE = "https://www.noon.com/saudi-en"


def _classify_empty_results_page(
    *,
    page_num: int,
    collected_count: int,
    total_results: int,
    target_count: int,
    visible_name_count: int,
) -> dict[str, object]:
    expected_total = max(int(total_results or 0), 0)
    effective_target = min(int(target_count or 0), expected_total) if expected_total > 0 else int(target_count or 0)

    if page_num > 1 and visible_name_count <= 0 and expected_total > 0 and effective_target > 0 and collected_count >= effective_target:
        return {
            "kind": "results_exhausted",
            "page_state": "results",
            "failure_category": "",
            "short_evidence": f"results_exhausted:no_more_cards_page_{page_num}",
            "error_evidence": [],
        }

    if visible_name_count > 0:
        page_state = "partial_results" if page_num > 1 or collected_count > 0 else "error"
        return {
            "kind": "page_parse_failure",
            "page_state": page_state,
            "failure_category": "page_recognition_failed",
            "short_evidence": f"page_parse_failure:visible_names_{visible_name_count}",
            "error_evidence": [
                f"page_parse_failure:visible_names_{visible_name_count}",
                f"visible_name_count:{visible_name_count}",
            ],
        }

    failure_category = "page_recognition_failed" if page_num == 1 else "partial_results"
    page_state = "partial_results" if page_num > 1 or collected_count > 0 else "error"
    short_evidence = (
        "page_recognition_failed:noon_search_results"
        if page_num == 1
        else f"partial_results:no_more_cards_page_{page_num}"
    )
    return {
        "kind": "empty_page",
        "page_state": page_state,
        "failure_category": failure_category,
        "short_evidence": short_evidence,
        "error_evidence": [short_evidence, "selector_miss:plp-product-box-name"],
    }


class NoonScraper(BaseScraper):

    def __init__(self, settings, skip_suggestions: bool = False, max_products: int | None = None):
        super().__init__("noon", settings)
        self._skip_suggestions = skip_suggestions
        self._max_products = max_products or settings.products_per_keyword

    def _normalize_text(self, value: object) -> str:
        return " ".join(str(value or "").split())

    def _is_timeout_error(self, value: object) -> bool:
        lowered = self._normalize_text(value).lower()
        return any(token in lowered for token in ("timeout", "timed out", "net::err"))

    async def _page_has_visible_search_results(self, page, url: str) -> bool:
        target_url = str(url or "").strip()
        if not target_url:
            return False
        try:
            current_url = self._normalize_text(getattr(page, "url", ""))
        except Exception:
            current_url = ""
        if not current_url:
            return False
        try:
            target_parts = urlparse(target_url)
            current_parts = urlparse(current_url)
            if current_parts.path != target_parts.path:
                return False
            target_query = parse_qs(target_parts.query)
            current_query = parse_qs(current_parts.query)
            if target_query.get("q") != current_query.get("q"):
                return False
            if target_query.get("page") != current_query.get("page"):
                return False
            visible_name_count = await page.locator('[data-qa="plp-product-box-name"]').count()
        except Exception:
            return False
        return int(visible_name_count or 0) > 0

    async def _goto_search_page_with_retry(self, page, url: str) -> None:
        last_exc: Exception | None = None
        for attempt, timeout_ms in enumerate((30000, 45000), start=1):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                await page.wait_for_timeout(5000 if attempt == 1 else 6500)
                return
            except Exception as exc:
                last_exc = exc
                if self._is_timeout_error(exc):
                    await page.wait_for_timeout(1200)
                    if await self._page_has_visible_search_results(page, url):
                        logger.warning("[noon] search page timeout but visible results found, continuing url=%s", url)
                        return
                if not self._is_timeout_error(exc) or attempt >= 2:
                    raise
                logger.warning("[noon] search page timeout, retrying attempt=%s url=%s", attempt + 1, url)
                await page.wait_for_timeout(1500 * attempt)
        if last_exc is not None:
            raise last_exc

    async def scrape_keyword(self, keyword: str) -> dict:
        """爬取 Noon 搜索结果页，自动翻页到目标商品数"""
        ctx = await self.new_context()
        page = await ctx.new_page()
        result = self._build_result_payload(
            keyword,
            page_state="unknown",
            extra={
                "express_results": 0,
                "primary_category": "",
                "category_path": [],
                "page_url": "",
            },
        )

        def normalize_text(value: object) -> str:
            return self._normalize_text(value)

        def dedupe_texts(values: list[str]) -> list[str]:
            output: list[str] = []
            seen: set[str] = set()
            for raw in values:
                text = normalize_text(raw)
                if not text:
                    continue
                lowered = text.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                output.append(text)
            return output

        try:
            target = self._max_products
            all_products = []
            page_num = 1
            max_pages = (target // 50) + 2  # Noon ~50条/页
            failure_details: list[dict] = []

            while len(all_products) < target and page_num <= max_pages:
                url = f"{NOON_BASE}/search/?q={keyword}&page={page_num}"
                result["page_url"] = url

                try:
                    await self._goto_search_page_with_retry(page, url)
                except Exception as exc:
                    error_text = normalize_text(str(exc))
                    failure_category = (
                        "timeout"
                        if self._is_timeout_error(error_text)
                        else "page_recognition_failed"
                    )
                    result["page_state"] = "partial_results" if all_products else "error"
                    result["error_evidence"] = dedupe_texts(
                        list(result.get("error_evidence") or [])
                        + [f"{failure_category}:{exc.__class__.__name__}", error_text, f"url:{url}", "retry_exhausted:page_goto"]
                    )
                    failure_details.append(
                        self._build_failure_detail(
                            keyword,
                            failure_category=failure_category,
                            short_evidence=error_text or exc.__class__.__name__,
                            page_url=url,
                            page_number=page_num,
                            page_state=result["page_state"],
                        )
                    )
                    break

                if page_num == 1:
                    result["total_results"] = await self._get_total_results(page)
                    cat_info = await self._get_category_filter(page)
                    result["primary_category"] = cat_info.get("primary", "")
                    result["category_path"] = cat_info.get("path", [])

                prev_count = 0
                stall = 0
                for _ in range(15):
                    await page.evaluate("window.scrollBy(0, window.innerHeight * 1.5)")
                    await page.wait_for_timeout(1200)
                    names = page.locator('[data-qa="plp-product-box-name"]')
                    count = await names.count()
                    if count == prev_count:
                        stall += 1
                        if stall >= 3:
                            break
                    else:
                        stall = 0
                    prev_count = count

                visible_name_count = int(prev_count or 0)
                page_products = await self._parse_products(
                    page,
                    rank_offset=len(all_products),
                    category_path=" > ".join(result["category_path"]) or result["primary_category"],
                )
                if not page_products:
                    if page_num == 1 and int(result.get("total_results") or 0) <= 0:
                        result["page_state"] = "zero_results"
                        result["zero_result_evidence"] = dedupe_texts(
                            list(result.get("zero_result_evidence") or [])
                            + ["zero_results:no_product_cards_page_1", f"url:{url}"]
                        )
                        failure_details.append(
                            self._build_failure_detail(
                                keyword,
                                failure_category="zero_results",
                                short_evidence="no_product_cards_page_1",
                                page_url=url,
                                page_number=page_num,
                                page_state="zero_results",
                            )
                        )
                    else:
                        empty_page = _classify_empty_results_page(
                            page_num=page_num,
                            collected_count=len(all_products),
                            total_results=int(result.get("total_results") or 0),
                            target_count=target,
                            visible_name_count=visible_name_count,
                        )
                        if str(empty_page.get("kind") or "") == "results_exhausted":
                            logger.info(
                                "[noon] '%s' results exhausted at page=%s collected=%s total_results=%s",
                                keyword,
                                page_num,
                                len(all_products),
                                int(result.get("total_results") or 0),
                            )
                            break
                        failure_category = str(empty_page.get("failure_category") or "page_recognition_failed")
                        result["page_state"] = str(empty_page.get("page_state") or "error")
                        evidence_token = str(empty_page.get("short_evidence") or "")
                        result["error_evidence"] = dedupe_texts(
                            list(result.get("error_evidence") or [])
                            + list(empty_page.get("error_evidence") or [])
                            + [f"url:{url}"]
                        )
                        failure_details.append(
                            self._build_failure_detail(
                                keyword,
                                failure_category=failure_category,
                                short_evidence=evidence_token,
                                page_url=url,
                                page_number=page_num,
                                page_state=result["page_state"],
                            )
                        )
                    break

                all_products.extend(page_products)
                expected_total = int(result.get("total_results") or 0)
                effective_target = min(target, expected_total) if expected_total > 0 else target
                if effective_target > 0 and len(all_products) >= effective_target:
                    break
                page_num += 1

            result["products"] = all_products[:target]

            if result["products"]:
                if result["page_state"] == "unknown":
                    result["page_state"] = "results"
                result["suggested_keywords"] = await self._get_suggestions(page, keyword)
                result["express_results"] = await self._get_express_count(page, keyword)
                result["express_in_page"] = sum(1 for p in result["products"] if p.get("is_express"))
            elif result["page_state"] == "unknown":
                result["page_state"] = "error"
                result["error_evidence"] = dedupe_texts(
                    list(result.get("error_evidence") or [])
                    + ["page_recognition_failed:noon_empty_results", f"url:{result.get('page_url') or ''}"]
                )
                failure_details.append(
                    self._build_failure_detail(
                        keyword,
                        failure_category="page_recognition_failed",
                        short_evidence="noon_empty_results",
                        page_url=str(result.get("page_url") or ""),
                        page_number=page_num,
                        page_state="error",
                    )
                )

            result["failure_details"] = failure_details

        except Exception as e:
            logger.error(f"[noon] '{keyword}' 爬取失败: {e}")
            result["error"] = str(e)
            result["page_state"] = "partial_results" if result["products"] else "error"
            result["error_evidence"] = dedupe_texts(
                list(result.get("error_evidence") or [])
                + [f"runtime_error:{e.__class__.__name__}", normalize_text(str(e))]
            )
            result["failure_details"] = list(result.get("failure_details") or []) + [
                self._build_failure_detail(
                    keyword,
                    failure_category="runtime_error",
                    short_evidence=str(e),
                    page_url=str(result.get("page_url") or ""),
                    page_number=page_num if "page_num" in locals() else None,
                    page_state=str(result.get("page_state") or "error"),
                )
            ]
        finally:
            await page.close()
            await ctx.close()

        result["error_evidence"] = dedupe_texts(list(result.get("error_evidence") or []))
        result["zero_result_evidence"] = dedupe_texts(list(result.get("zero_result_evidence") or []))
        return self._normalize_result_payload(keyword, result)

    async def _get_total_results(self, page) -> int:
        """从页面文本提取搜索结果总数"""
        try:
            content = await page.content()
            match = re.search(r'([\d,]+)\s*results', content, re.IGNORECASE)
            if match:
                return int(match.group(1).replace(",", ""))
        except Exception:
            pass
        return 0

    async def _get_category_filter(self, page) -> dict:
        """
        从搜索结果页的 Category 筛选器提取主品类。
        返回 {"primary": "Home & Kitchen", "path": ["Home & Kitchen", "Storage & Organisation", ...]}
        """
        try:
            data = await page.evaluate("""() => {
                const container = document.querySelector('[data-qa="Category"]');
                if (!container) return {primary: '', path: []};

                const content = container.querySelector('[class*="content"]') || container;

                // 找所有 ExpandableCategory 按钮（一级品类）
                const topButtons = content.querySelectorAll(
                    ':scope > ul > li > button [class*="name"], ' +
                    ':scope > ul > li > button'
                );

                // 第一级品类名（排在第一个的是结果最多的）
                let primary = '';
                for (const btn of topButtons) {
                    const name = btn.innerText?.trim();
                    if (name && name.length > 2 && !/^All /i.test(name)) {
                        primary = name;
                        break;
                    }
                }

                // 提取完整品类路径（所有层级的文本，去掉 "All xxx"）
                const allText = content.innerText || '';
                const lines = allText.split('\\n')
                    .map(l => l.trim())
                    .filter(l => l && l.length > 2 && !/^All /i.test(l));

                // 取前5个作为品类路径（从粗到细）
                const path = lines.slice(0, 5);

                return {primary, path};
            }""")
            return data or {"primary": "", "path": []}
        except Exception as e:
            logger.debug(f"[noon] Category 筛选器提取失败: {e}")
            return {"primary": "", "path": []}

    async def _get_express_count(self, page, keyword: str) -> int:
        """
        获取 Express 真实总数：访问带 fulfillment=express 筛选的搜索 URL，
        读取筛选后页面显示的结果数。
        """
        try:
            express_url = (
                f"{NOON_BASE}/search/?q={keyword}"
                "&f%5Bfulfillment%5D%5B%5D=express"
            )
            await page.goto(express_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)

            content = await page.content()
            match = re.search(r'([\d,]+)\s*results', content, re.IGNORECASE)
            if match:
                return int(match.group(1).replace(",", ""))
        except Exception as e:
            logger.debug(f"[noon] Express 数量获取失败: {e}")
        return 0

    async def _parse_products(
        self,
        page,
        *,
        rank_offset: int = 0,
        category_path: str = "",
    ) -> list[dict]:
        """解析搜索结果商品"""
        products = []
        try:
            name_els = page.locator('[data-qa="plp-product-box-name"]')
            count = await name_els.count()

            for i in range(count):
                try:
                    product = await self._parse_product_at(
                        page,
                        i,
                        rank_offset=rank_offset,
                        category_path=category_path,
                    )
                    if product.get("title"):
                        products.append(product)
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"[noon] 商品列表解析失败: {e}")

        return products

    async def _parse_product_at(
        self,
        page,
        index: int,
        *,
        rank_offset: int = 0,
        category_path: str = "",
    ) -> dict:
        """通过 JS 在页面中解析第 index 个商品"""
        data = await page.evaluate("""(idx) => {
            const collectSignalTexts = (card) => {
                if (!card) return [];
                const snippets = [];
                const seen = new Set();
                const nodes = card.querySelectorAll('div, span, p, small, strong, a, button, [data-qa], [aria-label], [title], [alt]');
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                for (const node of nodes) {
                    const values = [
                        node.innerText || node.textContent || '',
                        node.getAttribute && node.getAttribute('aria-label'),
                        node.getAttribute && node.getAttribute('title'),
                        node.getAttribute && node.getAttribute('alt'),
                    ];
                    for (const rawValue of values) {
                        const value = normalize(rawValue);
                        const lowered = value.toLowerCase();
                        if (!value || value.length > 120 || seen.has(lowered)) continue;
                        if (!/(sell out fast|selling out fast|best seller|top rated|limited time|sponsored|ad|express|global|marketplace|supermall|only |last |sold|#|lowest price|free delivery|get it by|get in |cashback|big deal|extra |\\b\\d+%\\s*off\\b)/i.test(value)) {
                            continue;
                        }
                        seen.add(lowered);
                        snippets.push(value);
                    }
                }
                return snippets;
            };
            const collectMarkerDetails = (card, pattern) => {
                const markers = [];
                const seen = new Set();
                const nodes = card.querySelectorAll('img, div, span, p, small, strong, a, button, [data-qa], [aria-label], [title], [alt]');
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                for (const node of nodes) {
                    const text = normalize(node.innerText || node.textContent || '');
                    const dataQa = normalize(node.getAttribute && node.getAttribute('data-qa'));
                    const alt = normalize(node.getAttribute && node.getAttribute('alt'));
                    const title = normalize(node.getAttribute && node.getAttribute('title'));
                    const ariaLabel = normalize(node.getAttribute && node.getAttribute('aria-label'));
                    const className = normalize(
                        typeof node.className === 'string'
                            ? node.className
                            : (node.className && node.className.baseVal) || ''
                    );
                    const attrBundle = normalize([dataQa, alt, title, ariaLabel, className].filter(Boolean).join(' | '));
                    const hasExplicitAttrMatch = !!attrBundle && pattern.test(attrBundle);
                    const hasShortTextMatch = !!text && text.length <= 80 && pattern.test(text);
                    if (!hasExplicitAttrMatch && !hasShortTextMatch) continue;
                    const merged = normalize([
                        hasExplicitAttrMatch ? attrBundle : '',
                        hasShortTextMatch ? text : '',
                    ].filter(Boolean).join(' | '));
                    if (!merged) continue;
                    const dedupeKey = merged.toLowerCase();
                    if (seen.has(dedupeKey)) continue;
                    seen.add(dedupeKey);
                    markers.push({
                        tag: node.tagName || '',
                        text: text,
                        dataQa: dataQa,
                        alt: alt,
                        title: title,
                        ariaLabel: ariaLabel,
                        className: className,
                        merged: merged,
                    });
                }
                return markers;
            };
            const names = document.querySelectorAll('[data-qa="plp-product-box-name"]');
            if (idx >= names.length) return null;
            const nameEl = names[idx];

            // 找到最近的 <a> 祖先（商品链接）
            const link = nameEl.closest('a');
            const href = link ? link.getAttribute('href') : '';

            // 找到 ProductBox wrapper
            const card = nameEl.closest('[class*="ProductBox"]') || nameEl.parentElement;
            const cardText = card ? card.innerText : '';

            // 价格
            const priceEl = card.querySelector('[data-qa="plp-product-box-price"]');
            const priceText = priceEl ? priceEl.innerText : '';

            // 原价（划线价）
            const wasEl = card.querySelector('[class*="was" i], del, [class*="oldPrice" i], [class*="Was"]');
            const wasText = wasEl ? wasEl.innerText : '';

            // 评分区域
            const ratingEl = card.querySelector('[class*="Rating"], [class*="rating"]');
            const ratingText = ratingEl
                ? (
                    ratingEl.getAttribute('aria-label')
                    || ratingEl.getAttribute('title')
                    || ratingEl.closest('[aria-label]')?.getAttribute('aria-label')
                    || ratingEl.closest('[title]')?.getAttribute('title')
                    || ratingEl.innerText
                )
                : '';

            // 卖家名称 (seller_name)
            const sellerEl = card.querySelector('[data-qa="plp-product-box-seller"], [class*="seller"], [class*="Seller"]');
            const sellerText = sellerEl ? sellerEl.innerText : '';

            // 品牌名 (brand) - 从标题或专门区域提取
            const brandEl = card.querySelector('[data-qa="plp-product-brand"], [class*="brand"] a, [class*="Brand"] a');
            const brandText = brandEl ? brandEl.innerText : '';

            // 图片
            const imgs = card.querySelectorAll('img[src*="nooncdn.com/p/"], img[srcset*="nooncdn.com/p/"], img[data-src*="nooncdn.com/p/"]');
            const primaryImg = card.querySelector('img[src*="nooncdn.com/p/"], img[srcset*="nooncdn.com/p/"], img[data-src*="nooncdn.com/p/"]');
            const primaryImageUrl = primaryImg
                ? (
                    primaryImg.currentSrc
                    || primaryImg.getAttribute('src')
                    || primaryImg.getAttribute('data-src')
                    || primaryImg.getAttribute('data-original')
                    || (((primaryImg.getAttribute('srcset') || '').split(',')[0] || '').trim().split(/\\s+/)[0])
                )
                : '';
            const signalTexts = collectSignalTexts(card);
            const deliveryMarkers = collectMarkerDetails(
                card,
                /__DELIVERY_MARKER_PATTERN__/i
            );

            // Express
            const expressImg = card.querySelector('img[alt*="express" i]');
            const hasExpress = !!expressImg
                || deliveryMarkers.some(marker => /(product-noon-express|noon[-\\s]?express|\\bfbn\\b|\\bexpress\\b)/i.test(marker.merged || ''))
                || /express/i.test(cardText)
                || signalTexts.some(text => /express/i.test(text));

            // Best Seller
            const hasBestSeller = /best\\s*seller/i.test(cardText) || signalTexts.some(text => /best\\s*seller/i.test(text));

            // Ad / Sponsored / إعلان
            const isAd = /\\b(Ad|Sponsored)\\b/i.test(cardText) || /إعلان/.test(cardText);

            return {
                title: nameEl.innerText.trim(),
                href: href,
                priceText: priceText,
                wasText: wasText,
                ratingText: ratingText,
                sellerText: sellerText,
                brandText: brandText,
                cardText: cardText,
                signalTexts: signalTexts,
                deliveryMarkers: deliveryMarkers,
                imgCount: imgs.length,
                imageUrl: primaryImageUrl,
                isExpress: hasExpress,
                isBestSeller: hasBestSeller,
                isAd: isAd,
            };
        }""".replace("__DELIVERY_MARKER_PATTERN__", DELIVERY_MARKER_JS_PATTERN), index)

        if not data:
            return {}

        product = parse_noon_product_payload(
            data,
            search_rank=rank_offset + index + 1,
            category_path=category_path,
        )
        if product.get("review_count_is_estimated") and product.get("product_url"):
            detail = await self.scrape_product_dimensions(product["product_url"])
            if detail:
                if detail.get("review_count") is not None:
                    product["review_count"] = detail["review_count"]
                    product["review_count_is_estimated"] = False
                if detail.get("rating") is not None:
                    product["rating"] = detail["rating"]
                if detail.get("image_url"):
                    product["image_url"] = detail["image_url"]
        return product

    async def _get_suggestions(self, page, keyword: str) -> list[str]:
        """
        通过搜索框获取联想词。
        注意：此方法较慢（~30s），全量扫描时建议跳过（设 skip_suggestions=True）。
        """
        if self._skip_suggestions:
            return []

        suggestions = []
        try:
            search = page.locator(
                'header input[name="q"], '
                'header input[placeholder*="search" i], '
                'input[name="q"], '
                'input[type="search"]:not([id*="filter"]):not([name*="filter"])'
            )
            if await search.count() > 0:
                await search.first.click()
                await search.first.fill("")
                await page.wait_for_timeout(300)
                prefix = keyword[:min(len(keyword), 8)]
                await search.first.type(prefix, delay=50)
                await page.wait_for_timeout(1500)

                suggestion_items = page.locator(
                    '[class*="suggestion"] a, [class*="Suggestion"] a, '
                    '[class*="autocomplete"] li, [class*="Autocomplete"] li, '
                    '[class*="Dropdown"] a[href*="search"]'
                )
                count = await suggestion_items.count()
                for i in range(min(count, 10)):
                    try:
                        text = (await suggestion_items.nth(i).inner_text(timeout=2000)).strip().lower()
                        if text and text != keyword.lower() and len(text) >= 3:
                            suggestions.append(text)
                    except Exception:
                        break
        except Exception as e:
            logger.debug(f"[noon] 联想词获取失败: {e}")

        return suggestions

    async def scrape_product_dimensions(self, product_url: str) -> dict | None:
        """
        模式1: 爬取商品详情页，提取尺寸/重量规格参数。
        返回 {"length_cm": x, "width_cm": y, "height_cm": z, "weight_kg": w} 或 None。
        """
        ctx = await self.new_context()
        page = await ctx.new_page()
        result = None

        try:
            await page.goto(product_url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            # 展开规格参数
            for sel in ['button:has-text("View All")', '[class*="specification"] button', '[class*="viewAll"]']:
                btn = page.locator(sel)
                if await btn.count() > 0:
                    await btn.first.click()
                    await page.wait_for_timeout(500)
                    break

            page_text = await page.inner_text("body")
            page_html = await page.content()

            dims = None
            for pattern in [
                r'(\d+\.?\d*)\s*[x×]\s*(\d+\.?\d*)\s*[x×]\s*(\d+\.?\d*)\s*cm',
                r'length[:\s]*(\d+\.?\d*)\s*cm.*?width[:\s]*(\d+\.?\d*)\s*cm.*?height[:\s]*(\d+\.?\d*)\s*cm',
            ]:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    dims = {
                        "length_cm": float(match.group(1)),
                        "width_cm": float(match.group(2)),
                        "height_cm": float(match.group(3)),
                    }
                    break

            weight = None
            wt_match = re.search(r'(\d+\.?\d*)\s*kg', page_text, re.IGNORECASE)
            if wt_match:
                weight = float(wt_match.group(1))
            else:
                g_match = re.search(r'(\d+\.?\d*)\s*g(?:ram)?s?\b', page_text, re.IGNORECASE)
                if g_match and float(g_match.group(1)) > 10:
                    weight = float(g_match.group(1)) / 1000

            result = dims or {}
            if weight:
                result["weight_kg"] = weight

            review_match = re.search(r'(\d[\d,]*)\s+Ratings\b', page_text, re.IGNORECASE)
            if review_match:
                try:
                    result["review_count"] = int(review_match.group(1).replace(",", ""))
                except ValueError:
                    pass

            rating_match = re.search(r'\b(\d+(?:\.\d+)?)\s+(\d[\d,]*)\s+Ratings\b', page_text, re.IGNORECASE)
            if rating_match:
                try:
                    result["rating"] = float(rating_match.group(1))
                except ValueError:
                    pass

            image_match = re.search(
                r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                page_html,
                re.IGNORECASE,
            )
            if image_match:
                result["image_url"] = image_match.group(1)

            if not result:
                result = None

        except Exception as e:
            logger.debug(f"[noon] 详情页解析失败 {product_url}: {e}")
        finally:
            await page.close()
            await ctx.close()

        return result
