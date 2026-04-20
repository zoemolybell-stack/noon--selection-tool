from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


def build_runtime_subcategory_records(
    *,
    category_name: str,
    runtime_map: dict,
    canonicalize_url: Callable[[str], str],
) -> list[dict]:
    category_records = runtime_map.get("categories", {}).get(category_name, {})
    items: list[dict] = []
    for config_id, record in sorted(category_records.items(), key=lambda item: (item[1].get("display_name") or item[0]).lower()):
        source_urls: list[str] = []
        resolved_url = record.get("resolved_url")
        if isinstance(resolved_url, str) and resolved_url:
            source_urls.append(canonicalize_url(resolved_url))
        for raw_url in record.get("source_urls") or []:
            if isinstance(raw_url, str) and raw_url:
                normalized = canonicalize_url(raw_url)
                if normalized not in source_urls:
                    source_urls.append(normalized)
        if not source_urls:
            continue
        items.append(
            {
                "name": record.get("display_name") or record.get("subcategory_name") or config_id,
                "url": source_urls[0],
                "source_urls": source_urls,
                "depth": max(1, len(record.get("expected_path") or [])),
                "path": list(record.get("expected_path") or []),
                "expected_path": list(record.get("expected_path") or []),
                "platform_nav_path": list(record.get("platform_nav_path") or []),
                "config_id": config_id,
                "parent_config_id": record.get("parent_config_id"),
                "source": "runtime_category_map",
            }
        )
    return items


async def extract_subcategories_from_page(page, url: str, depth: int, logger) -> list[dict]:
    subcategories = []
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(4000)
        await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
        await page.wait_for_timeout(2000)

        expand_buttons = page.locator('[data-qa="Category"] button, [class*="expand"] button')
        expand_count = await expand_buttons.count()
        for index in range(min(expand_count, 20)):
            try:
                await expand_buttons.nth(index).click()
                await page.wait_for_timeout(300)
            except Exception:
                continue

        results = await page.evaluate(
            """() => {
                const results = [];
                const catContainer = document.querySelector('[data-qa="Category"]');
                if (catContainer) {
                    const links = catContainer.querySelectorAll('a[href]');
                    const seen = new Set();
                    for (const a of links) {
                        const name = (a.innerText || '').trim();
                        const href = a.getAttribute('href') || '';
                        if (!name || name.length < 2) continue;
                        if (/^All /i.test(name) || /^Clear$/i.test(name)) continue;
                        if (!href.includes('/saudi-en/')) continue;
                        if (seen.has(href)) continue;
                        seen.add(href);
                        results.push({
                            name: name.replace(/\\(\\d+\\)/, '').trim(),
                            url: href.startsWith('/') ? 'https://www.noon.com' + href : href,
                        });
                    }
                }
                return results;
            }"""
        )

        for result in results:
            subcategories.append(
                {
                    "name": result["name"],
                    "url": result["url"],
                    "depth": depth,
                }
            )
    except Exception as exc:
        logger.debug("提取子类目失败(depth=%s): %s", depth, exc)
    return subcategories


async def crawl_category_recursive(
    *,
    new_context: Callable[[], Awaitable[Any]],
    max_depth: int,
    url: str,
    logger,
    depth: int = 1,
    path: list[str] | None = None,
) -> list[dict]:
    if path is None:
        path = []
    if depth > max_depth:
        logger.info("达到最大深度 %s，停止递归", max_depth)
        return []

    ctx = await new_context()
    page = await ctx.new_page()
    result = []
    try:
        subcategories = await extract_subcategories_from_page(page, url, depth, logger)
        for sub in subcategories:
            sub_path = [*path, sub["name"]]
            sub["path"] = sub_path
            if depth < max_depth:
                child_subs = await crawl_category_recursive(
                    new_context=new_context,
                    max_depth=max_depth,
                    url=sub["url"],
                    logger=logger,
                    depth=depth + 1,
                    path=sub_path,
                )
                if not child_subs:
                    result.append(sub)
                else:
                    result.extend(child_subs)
            else:
                result.append(sub)
            await asyncio.sleep(0.5)
    except Exception as exc:
        logger.debug("递归抓取失败: %s", exc)
    finally:
        await page.close()
        await ctx.close()
    return result
