from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from typing import Any


def build_category_failure_evidence(
    *,
    failure_category: str,
    short_evidence: str,
    snapshot_id: str = "",
    category_context: dict[str, object] | None = None,
    page_url: str = "",
    page_number: int | None = None,
    expected_result_file: str = "",
    page_state: str = "",
) -> dict[str, object]:
    return {
        "platform": "noon",
        "source_type": "category",
        "keyword": "",
        "category_context": dict(category_context or {}),
        "snapshot_id": str(snapshot_id or "").strip(),
        "failure_category": str(failure_category or "").strip(),
        "short_evidence": " ".join(str(short_evidence or "").split()).strip()[:300],
        "page_url": str(page_url or "").strip(),
        "page_number": page_number,
        "expected_result_file": str(expected_result_file or "").strip(),
        "page_state": str(page_state or "").strip(),
    }


async def extract_breadcrumb(page: Any) -> dict:
    try:
        data = await page.evaluate(
            r"""() => {
                const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                const seen = new Set();
                const items = [];
                const links = [];
                const containers = [
                    document.querySelector('nav[aria-label*="breadcrumb" i]'),
                    document.querySelector('[aria-label*="breadcrumb" i]'),
                    document.querySelector('[data-qa*="breadcrumb" i]'),
                    document.querySelector('[class*="breadcrumb" i]'),
                ].filter(Boolean);

                for (const container of containers) {
                    const nodes = container.querySelectorAll('a, span, li, div');
                    for (const node of nodes) {
                        const text = normalize(node.innerText || node.textContent || '');
                        if (!text || text.length > 120) continue;
                        if (/^(home|clear|all)$/i.test(text)) continue;
                        const key = text.toLowerCase();
                        if (seen.has(key)) continue;
                        seen.add(key);
                        items.push(text);

                        const href = node.getAttribute && node.getAttribute('href');
                        if (href) {
                            try {
                                const url = new URL(href, location.origin).toString();
                                if (url.includes('/saudi-en/')) {
                                    links.push({ text, url });
                                }
                            } catch {
                            }
                        }
                    }
                    if (items.length) {
                        break;
                    }
                }

                return { items, links };
            }"""
        )
    except Exception:
        return {"items": [], "links": [], "path": ""}

    items = data.get("items", []) or []
    links = data.get("links", []) or []
    return {
        "items": items,
        "links": links,
        "path": " > ".join(items),
    }


async def extract_category_filter(page: Any) -> dict:
    try:
        data = await page.evaluate(
            r"""() => {
                const normalize = (value) => (value || '').replace(/\s+/g, ' ').trim();
                const links = [];
                const texts = [];
                const seenLinks = new Set();
                const seenTexts = new Set();
                const container = document.querySelector('[data-qa="Category"]');
                if (!container) {
                    return { links, texts };
                }

                const nodes = container.querySelectorAll('a[href], button, label, span');
                for (const node of nodes) {
                    const text = normalize(node.innerText || node.textContent || '');
                    if (text && text.length <= 120 && !/^(all|clear)$/i.test(text)) {
                        const key = text.toLowerCase();
                        if (!seenTexts.has(key)) {
                            seenTexts.add(key);
                            texts.push(text);
                        }
                    }

                    const href = node.getAttribute && node.getAttribute('href');
                    if (href) {
                        try {
                            const url = new URL(href, location.origin).toString();
                            if (url.includes('/saudi-en/')) {
                                const key = `${text.toLowerCase()}|${url}`;
                                if (!seenLinks.has(key)) {
                                    seenLinks.add(key);
                                    links.push({ text, url });
                                }
                            }
                        } catch {
                        }
                    }
                }
                return { links, texts };
            }"""
        )
    except Exception:
        return {"links": [], "texts": []}
    return {
        "links": data.get("links", []) or [],
        "texts": data.get("texts", []) or [],
    }


def classify_category_path_match(
    *,
    expected_path: list[str] | None,
    breadcrumb: dict | None,
    category_filter: dict | None,
    fallback_name: str = "",
) -> dict[str, object]:
    expected_items = [str(item or "").strip() for item in list(expected_path or []) if str(item or "").strip()]
    breadcrumb_items = [str(item or "").strip() for item in list((breadcrumb or {}).get("items") or []) if str(item or "").strip()]
    breadcrumb_without_home = [item for item in breadcrumb_items if item.lower() != "home"]
    filter_texts = [str(item or "").strip() for item in list((category_filter or {}).get("texts") or []) if str(item or "").strip()]

    def _normalize(items: list[str]) -> list[str]:
        return [re.sub(r"\s+", " ", item).strip().casefold() for item in items if re.sub(r"\s+", " ", item).strip()]

    expected_norm = _normalize(expected_items)
    actual_norm = _normalize(breadcrumb_without_home)
    filter_norm = _normalize(filter_texts)

    status = "unknown"
    if expected_norm:
        if actual_norm and len(actual_norm) >= len(expected_norm) and actual_norm[-len(expected_norm):] == expected_norm:
            status = "matched"
        elif expected_norm and expected_norm[-1] in actual_norm:
            status = "partial"
        elif expected_norm and expected_norm[-1] in filter_norm:
            status = "partial"
        elif actual_norm or filter_norm:
            status = "mismatch"
        else:
            status = "missing"
    elif actual_norm:
        status = "matched"
    elif filter_norm:
        status = "partial"

    effective_items = breadcrumb_without_home or expected_items or ([fallback_name] if fallback_name else [])
    return {
        "status": status,
        "effective_category_path": " > ".join(effective_items),
        "effective_items": effective_items,
    }


async def scrape_public_product_details(
    new_context: Callable[[], Awaitable[Any]],
    product_url: str,
    logger,
) -> dict | None:
    ctx = await new_context()
    page = await ctx.new_page()
    result = None
    try:
        await page.goto(product_url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)
        page_text = await page.inner_text("body")
        page_html = await page.content()

        result = {}
        review_match = re.search(r"(\d[\d,]*)\s+Ratings\b", page_text, re.IGNORECASE)
        if review_match:
            try:
                result["review_count"] = int(review_match.group(1).replace(",", ""))
            except ValueError:
                pass

        rating_match = re.search(r"\b(\d+(?:\.\d+)?)\s+(\d[\d,]*)\s+Ratings\b", page_text, re.IGNORECASE)
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
    except Exception as exc:
        logger.debug("[category-crawler] public detail enrichment failed %s: %s", product_url, exc)
    finally:
        await page.close()
        await ctx.close()
    return result
