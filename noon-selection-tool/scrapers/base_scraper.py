"""
Async Playwright scraper base class.
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path

from playwright.async_api import Browser, BrowserContext, async_playwright
from scrapers.browser_runtime import resolve_browser_cdp_endpoint

logger = logging.getLogger(__name__)

WINDOWS_ILLEGAL_FILENAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')
FILENAME_SEPARATOR_RE = re.compile(r"[\s_]+")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def build_keyword_result_stem(keyword: str, *, max_base_length: int = 80) -> str:
    raw = str(keyword or "").strip().lower()
    cleaned = WINDOWS_ILLEGAL_FILENAME_RE.sub("_", raw)
    cleaned = FILENAME_SEPARATOR_RE.sub("_", cleaned).strip("._ ")
    if not cleaned:
        cleaned = "keyword"
    base = cleaned[:max_base_length].rstrip("._ ") or "keyword"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{base}__{digest}"


def _normalize_result_text(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_result_texts(values: list[object] | None) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in list(values or []):
        text = _normalize_result_text(raw)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        output.append(text)
    return output


def build_failure_detail(
    platform: str,
    keyword: str,
    *,
    failure_category: str,
    short_evidence: str,
    expected_result_file: str = "",
    page_url: str = "",
    page_number: int | None = None,
    page_state: str = "error",
) -> dict[str, object]:
    return {
        "platform": str(platform or "").strip(),
        "keyword": str(keyword or "").strip(),
        "failure_category": str(failure_category or "").strip(),
        "short_evidence": _normalize_result_text(short_evidence)[:300],
        "expected_result_file": str(expected_result_file or "").strip(),
        "page_url": str(page_url or "").strip(),
        "page_number": page_number,
        "page_state": str(page_state or "").strip(),
    }


def _normalize_failure_details(
    platform: str,
    keyword: str,
    failure_details: list[object] | None,
    *,
    expected_result_file: str = "",
    default_page_state: str = "",
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in list(failure_details or []):
        if not isinstance(raw, dict):
            continue
        detail = dict(raw)
        normalized_detail = build_failure_detail(
            str(detail.get("platform") or platform or "").strip(),
            str(detail.get("keyword") or keyword or "").strip(),
            failure_category=str(detail.get("failure_category") or "").strip(),
            short_evidence=str(detail.get("short_evidence") or "").strip(),
            expected_result_file=str(detail.get("expected_result_file") or expected_result_file or "").strip(),
            page_url=str(detail.get("page_url") or "").strip(),
            page_number=detail.get("page_number"),
            page_state=str(detail.get("page_state") or default_page_state or "").strip(),
        )
        dedupe_key = json.dumps(normalized_detail, ensure_ascii=False, sort_keys=True)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(normalized_detail)
    return normalized


def get_keyword_result_payload_error(
    platform: str,
    keyword: str,
    payload: object,
    *,
    require_meta: bool,
) -> str:
    if not isinstance(payload, dict):
        return "not_object"
    products = payload.get("products")
    if not isinstance(products, list):
        return "missing_products"
    meta = payload.get("_meta")
    if require_meta:
        if not isinstance(meta, dict):
            return "missing_meta"
    if isinstance(meta, dict):
        meta_platform = str(meta.get("platform") or "").strip().lower()
        if meta_platform and meta_platform != str(platform or "").strip().lower():
            return "platform_mismatch"
        meta_keyword = str(meta.get("keyword") or "").strip().lower()
        expected_keyword = str(keyword or "").strip().lower()
        if meta_keyword and meta_keyword != expected_keyword:
            return "keyword_mismatch"
    return ""


def normalize_keyword_result_payload(
    platform: str,
    keyword: str,
    payload: dict[str, object],
    *,
    expected_result_file: str = "",
) -> dict[str, object]:
    normalized = dict(payload or {})
    products = normalized.get("products")
    if not isinstance(products, list):
        products = []
    total_results_raw = normalized.get("total_results")
    try:
        total_results = int(total_results_raw)
    except Exception:
        total_results = len(products)

    page_state = _normalize_result_text(normalized.get("page_state"))
    if not page_state:
        page_state = "results" if products else "unknown"

    normalized["keyword"] = str(keyword or "").strip()
    normalized["products"] = products
    normalized["total_results"] = total_results
    normalized["suggested_keywords"] = _dedupe_result_texts(normalized.get("suggested_keywords"))
    normalized["page_state"] = page_state
    normalized["error"] = _normalize_result_text(normalized.get("error"))
    normalized["error_evidence"] = _dedupe_result_texts(normalized.get("error_evidence"))
    normalized["zero_result_evidence"] = _dedupe_result_texts(normalized.get("zero_result_evidence"))
    normalized["failure_details"] = _normalize_failure_details(
        platform,
        keyword,
        normalized.get("failure_details"),
        expected_result_file=expected_result_file,
        default_page_state=page_state,
    )
    return normalized


def build_keyword_result_payload(
    platform: str,
    keyword: str,
    *,
    products: list[object] | None = None,
    total_results: int = 0,
    suggested_keywords: list[object] | None = None,
    page_state: str = "unknown",
    error: str = "",
    error_evidence: list[object] | None = None,
    zero_result_evidence: list[object] | None = None,
    failure_details: list[object] | None = None,
    expected_result_file: str = "",
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = dict(extra or {})
    payload.update(
        {
            "keyword": keyword,
            "products": list(products or []),
            "total_results": total_results,
            "suggested_keywords": list(suggested_keywords or []),
            "page_state": page_state,
            "error": error,
            "error_evidence": list(error_evidence or []),
            "zero_result_evidence": list(zero_result_evidence or []),
            "failure_details": list(failure_details or []),
        }
    )
    return normalize_keyword_result_payload(
        platform,
        keyword,
        payload,
        expected_result_file=expected_result_file,
    )


class BaseScraper:
    """Shared base class for platform scrapers."""

    def __init__(self, platform_name: str, settings):
        self.platform = platform_name
        self.settings = settings
        self.data_dir = settings.snapshot_dir / platform_name
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._playwright = None
        self._browser: Browser | None = None
        self._semaphore = asyncio.Semaphore(settings.concurrent_browsers)

    def _browser_headless(self) -> bool:
        return _env_flag(
            "NOON_BROWSER_HEADLESS",
            _env_flag(
                "BROWSER_HEADLESS",
                bool(getattr(self.settings, "browser_headless", False)),
            ),
        )

    def _browser_channel(self) -> str:
        channel = str(getattr(self.settings, "browser_channel", "") or "").strip()
        return channel

    def _browser_executable_path(self) -> str:
        executable_path = str(
            getattr(self.settings, "browser_executable_path_value", "")
            or os.getenv("NOON_BROWSER_EXECUTABLE_PATH")
            or os.getenv("BROWSER_EXECUTABLE_PATH")
            or ""
        ).strip()
        return executable_path

    def _browser_cdp_endpoint(self) -> str:
        endpoint = str(
            getattr(self.settings, "browser_cdp_endpoint_value", "")
            or os.getenv("NOON_BROWSER_CDP_ENDPOINT")
            or os.getenv("BROWSER_CDP_ENDPOINT")
            or ""
        ).strip()
        return resolve_browser_cdp_endpoint(endpoint)

    def _browser_cache_dir(self) -> Path:
        profile_root = getattr(self.settings, "browser_profile_root_path", None)
        if profile_root is None:
            profile_root = Path(os.getenv("BROWSER_PROFILE_ROOT") or (self.data_dir.parent / ".browser_profiles"))
        cache_dir = Path(profile_root) / self.platform / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    async def start_browser(self):
        """Launch Playwright browser."""
        self._playwright = await async_playwright().start()
        cdp_endpoint = self._browser_cdp_endpoint()
        if cdp_endpoint:
            self._browser = await self._playwright.chromium.connect_over_cdp(cdp_endpoint)
            logger.info(f"[{self.platform}] browser connected over CDP ({cdp_endpoint})")
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
        if self.settings.proxy_url:
            launch_args["proxy"] = {"server": self.settings.proxy_url}
        executable_path = self._browser_executable_path()
        if executable_path:
            launch_args["executable_path"] = executable_path
        else:
            channel = self._browser_channel()
            if channel:
                launch_args["channel"] = channel

        self._browser = await self._playwright.chromium.launch(**launch_args)
        logger.info(f"[{self.platform}] browser started (headless={headless})")

    async def stop_browser(self):
        """Close Playwright browser."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info(f"[{self.platform}] browser stopped")

    async def new_context(self) -> BrowserContext:
        """Create a new browser context with stealth applied when available."""
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
            logger.warning("playwright-stealth unavailable; continuing without stealth")
        return ctx

    def is_completed(self, keyword: str) -> bool:
        """Whether a keyword result file already exists."""
        return self.load_result(keyword) is not None

    def _result_path(self, keyword: str) -> Path:
        return self.data_dir / f"{build_keyword_result_stem(keyword)}.json"

    def _is_valid_result_payload(self, keyword: str, payload: object) -> bool:
        return not get_keyword_result_payload_error(
            self.platform,
            keyword,
            payload,
            require_meta=True,
        )

    def _load_result_payload(self, keyword: str) -> dict | None:
        path = self._result_path(keyword)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("[%s] invalid result json ignored: %s (%s)", self.platform, path.name, exc)
            return None
        if not self._is_valid_result_payload(keyword, payload):
            logger.warning("[%s] invalid result payload ignored: %s", self.platform, path.name)
            return None
        return normalize_keyword_result_payload(
            self.platform,
            keyword,
            payload,
            expected_result_file=str(path),
        )

    def _atomic_write_json(self, path: Path, payload: dict):
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.parent / f".{path.name}.{os.getpid()}.tmp"
        try:
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temp_path, path)
        finally:
            temp_path.unlink(missing_ok=True)

    def save_result(self, keyword: str, data: dict):
        """Persist one keyword result JSON file."""
        path = self._result_path(keyword)
        payload = normalize_keyword_result_payload(
            self.platform,
            keyword,
            dict(data),
            expected_result_file=str(path),
        )
        payload["_meta"] = {
            "platform": self.platform,
            "keyword": keyword,
            "scraped_at": datetime.now().isoformat(),
        }
        self._atomic_write_json(path, payload)

    def load_result(self, keyword: str) -> dict | None:
        """Load a previously persisted keyword result."""
        return self._load_result_payload(keyword)

    async def random_delay(self):
        """Apply a random inter-request delay."""
        delay = random.uniform(self.settings.min_delay, self.settings.max_delay)
        await asyncio.sleep(delay)

    async def scrape_keyword(self, keyword: str) -> dict:
        """Subclass hook for scraping one keyword."""
        raise NotImplementedError

    async def scrape_with_retry(self, keyword: str) -> dict | None:
        """Scrape one keyword with retry and semaphore control."""
        async with self._semaphore:
            last_exc: Exception | None = None
            for attempt in range(1, self.settings.max_retries + 1):
                try:
                    result = await self.scrape_keyword(keyword)
                    self.save_result(keyword, result)
                    return result
                except Exception as exc:
                    last_exc = exc
                    wait = 2 ** attempt + random.random()
                    logger.warning(
                        f"[{self.platform}] '{keyword}' attempt {attempt} failed: {exc}. "
                        f"retrying in {wait:.1f}s"
                    )
                    if attempt < self.settings.max_retries:
                        await asyncio.sleep(wait)
            if last_exc is not None:
                self.save_result(keyword, self._build_failure_result(keyword, last_exc))
            logger.error(f"[{self.platform}] '{keyword}' failed after {self.settings.max_retries} attempts")
            return None

    def _build_failure_result(self, keyword: str, exc: Exception) -> dict:
        message = str(exc or "").strip()
        lowered = message.lower()
        if isinstance(exc, ModuleNotFoundError) or "modulenotfounderror" in lowered:
            category = "dependency_missing"
        elif "beautifulsoup4_unavailable" in lowered:
            category = "runtime_import_error" if importlib.util.find_spec("bs4") is not None else "dependency_missing"
        elif isinstance(exc, ImportError) or "importerror" in lowered:
            category = "runtime_import_error"
        elif "timeout" in lowered or "timed out" in lowered or "net::err" in lowered:
            category = "timeout"
        else:
            category = "runtime_error"

        result_path = self._result_path(keyword)
        return build_keyword_result_payload(
            self.platform,
            keyword,
            products=[],
            total_results=0,
            suggested_keywords=[],
            page_state="error",
            error=message,
            error_evidence=[
                f"{category}:{exc.__class__.__name__}",
                f"expected_result_file:{result_path.name}",
            ],
            failure_details=[
                build_failure_detail(
                    self.platform,
                    keyword,
                    failure_category=category,
                    short_evidence=message[:300] if message else exc.__class__.__name__,
                    expected_result_file=str(result_path),
                    page_state="error",
                )
            ],
            expected_result_file=str(result_path),
        )

    def _build_result_payload(self, keyword: str, **kwargs) -> dict[str, object]:
        return build_keyword_result_payload(
            self.platform,
            keyword,
            expected_result_file=str(self._result_path(keyword)),
            **kwargs,
        )

    def _normalize_result_payload(self, keyword: str, payload: dict[str, object]) -> dict[str, object]:
        return normalize_keyword_result_payload(
            self.platform,
            keyword,
            payload,
            expected_result_file=str(self._result_path(keyword)),
        )

    def _build_failure_detail(self, keyword: str, **kwargs) -> dict[str, object]:
        return build_failure_detail(
            self.platform,
            keyword,
            expected_result_file=str(self._result_path(keyword)),
            **kwargs,
        )

    async def _run_one_keyword(self, keyword: str) -> tuple[str, dict | None]:
        await self.random_delay()
        result = await self.scrape_with_retry(keyword)
        return keyword, result

    async def run(self, keywords: list[str]) -> list[dict]:
        """Run keyword scraping with resumability and controlled concurrency."""
        pending = [kw for kw in keywords if not self.is_completed(kw)]
        skipped = len(keywords) - len(pending)
        if skipped > 0:
            logger.info(f"[{self.platform}] skipped {skipped} completed keywords; pending={len(pending)}")

        if not pending:
            logger.info(f"[{self.platform}] no pending keywords")
            return [self.load_result(kw) for kw in keywords if self.load_result(kw)]

        await self.start_browser()
        results_by_keyword: dict[str, dict] = {}
        start_time = time.time()
        completed = 0

        try:
            tasks = [asyncio.create_task(self._run_one_keyword(kw)) for kw in pending]
            for task in asyncio.as_completed(tasks):
                keyword, result = await task
                completed += 1
                if result:
                    results_by_keyword[keyword] = result

                if completed % 10 == 0 or completed == len(pending):
                    elapsed = time.time() - start_time
                    avg = elapsed / completed if completed else 0
                    eta = avg * (len(pending) - completed)
                    logger.info(
                        f"[{self.platform}] progress: {completed}/{len(pending)} "
                        f"(success={len(results_by_keyword)}, elapsed={elapsed:.0f}s, ETA={eta:.0f}s)"
                    )
        finally:
            await self.stop_browser()

        results = []
        for keyword in keywords:
            payload = results_by_keyword.get(keyword)
            if payload is None and self.is_completed(keyword):
                payload = self.load_result(keyword)
            if payload:
                results.append(payload)
        return results
