"""
Product Parser - Core parsing logic with pagination support
"""

import asyncio
import random
import re
from typing import Any, Callable, Dict, List, Optional, Set

from playwright.async_api import async_playwright, Browser, Page

from crawler import UrlCrawler
from extractor import FieldExtractor


class ProductParser:
    """Main parser class with multi-page and pagination support"""

    def __init__(
        self,
        source_url: str,
        limit: int = 0,
        workers: int = 1,
        requests_per_sec: float = 3.0,
        max_sitemaps: int = 120,
        progress_callback: Optional[Callable] = None
    ):
        self.source_url = source_url
        self.limit = limit
        self.workers = max(1, min(4, workers))
        self.requests_per_sec = max(1.0, min(20.0, requests_per_sec))
        self.max_sitemaps = max_sitemaps
        self.progress_callback = progress_callback or (lambda *args: None)

        self.discovered_urls: List[str] = []
        self.parsed_products: List[Dict[str, Any]] = []
        self.rate_limit_retries = 0
        self.detected_fields: Set[str] = set()
        self._seen_source_urls: Set[str] = set()
        self._results_lock = asyncio.Lock()

        self.delay_between_requests = 1.0 / self.requests_per_sec
        self.browser: Optional[Browser] = None

    def parse(self) -> Dict[str, Any]:
        """Main entry point - synchronous wrapper for async parsing"""
        return asyncio.run(self._parse_async())

    def snapshot_result(
        self,
        completed: bool = True,
        error: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build a normalized result payload from currently parsed data."""
        records = list(self.parsed_products)
        if self.limit > 0 and len(records) > self.limit:
            records = records[:self.limit]

        detected_fields: Set[str] = set(self.detected_fields)
        for product in records:
            detected_fields.update(product.keys())

        return {
            "discovered_urls": len(self.discovered_urls),
            "parsed_products": len(records),
            "rate_limit_retries": self.rate_limit_retries,
            "detected_fields": sorted(detected_fields),
            "records": records,
            "completed": completed,
            "error": error,
        }

    async def _parse_async(self) -> Dict[str, Any]:
        """Async parsing implementation"""
        async with async_playwright() as p:
            # Launch browser in headless mode
            self.browser = await p.chromium.launch(headless=True)

            try:
                # Step 1: Discover URLs
                await self._discover_urls()

                # Step 2: Parse products from discovered URLs
                await self._parse_products()

                return self.snapshot_result(completed=True)

            finally:
                await self.browser.close()

    async def _discover_urls(self):
        """Discover product URLs via sitemap or crawling"""
        self.progress_callback("discovering_urls", {"source": self.source_url})

        crawler = UrlCrawler(
            source_url=self.source_url,
            max_sitemaps=self.max_sitemaps,
            target_limit=self.limit,
            progress_callback=self.progress_callback
        )

        urls = await crawler.discover_from_site_api()
        if urls:
            self.progress_callback("api_urls_discovered", {
                "count": len(urls),
                "source": self.source_url,
            })

        # Try sitemap if API discovery did not produce enough URLs
        if len(urls) < 10:
            sitemap_urls = await crawler.discover_from_sitemap(self.browser)
            urls.extend(sitemap_urls)

        # If not enough URLs, try crawling
        if len(urls) < 10:
            self.progress_callback("sitemap_insufficient", {
                "found": len(urls),
                "switching_to_crawl": True
            })
            crawled_urls = await crawler.discover_by_crawling(self.browser)
            urls.extend(crawled_urls)

        # Remove duplicates
        self.discovered_urls = list(dict.fromkeys(urls))
        if (
            self.source_url
            and self.source_url not in self.discovered_urls
            and self._looks_like_product_url(self.source_url.lower())
        ):
            self.discovered_urls.insert(0, self.source_url)
        self.discovered_urls = self._rank_discovered_urls(self.discovered_urls)

        # Keep a larger candidate pool for limited runs: some URLs can fail (429/blocked),
        # so parsing only exactly `limit` URLs often returns fewer products than requested.
        if self.limit > 0:
            pool_size = max(120, self.limit * 40)
            pool_size = min(pool_size, 5000)
            self.discovered_urls = self.discovered_urls[:pool_size]

        self.progress_callback("urls_discovered", {
            "count": len(self.discovered_urls),
            "target": self.limit if self.limit > 0 else len(self.discovered_urls),
        })

    async def _parse_products(self):
        """Parse products from discovered URLs with worker pool"""
        if not self.discovered_urls:
            return

        target_count = self.limit if self.limit > 0 else len(self.discovered_urls)
        self.progress_callback("parsing_started", {
            "total_urls": len(self.discovered_urls),
            "target": target_count,
            "workers": self.workers
        })

        # Create worker pool
        queue = asyncio.Queue()
        for url in self.discovered_urls:
            await queue.put(url)

        # Start workers
        workers = [
            asyncio.create_task(self._worker(queue, worker_id))
            for worker_id in range(self.workers)
        ]

        # Wait for all URLs to be processed
        await queue.join()

        # Cancel workers
        for worker in workers:
            worker.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        self.progress_callback("parsing_finished", {
            "total_parsed": len(self.parsed_products)
        })

    async def _worker(self, queue: asyncio.Queue, worker_id: int):
        """Worker coroutine that processes URLs from queue"""
        context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            while True:
                url = await queue.get()

                try:
                    if self.limit > 0:
                        async with self._results_lock:
                            if len(self.parsed_products) >= self.limit:
                                continue

                    # Rate limiting
                    await asyncio.sleep(self.delay_between_requests)

                    # Parse the URL
                    product_data = await self._parse_single_url(page, url)

                    if product_data:
                        source_url = str(product_data.get("source_url") or url).strip() or url
                        parsed_total = None
                        async with self._results_lock:
                            if source_url in self._seen_source_urls:
                                parsed_total = None
                            elif self.limit > 0 and len(self.parsed_products) >= self.limit:
                                parsed_total = None
                            else:
                                self._seen_source_urls.add(source_url)
                                self.parsed_products.append(product_data)
                                parsed_total = len(self.parsed_products)

                        if parsed_total is not None:
                            self.progress_callback("product_parsed", {
                                "worker": worker_id,
                                "url": url,
                                "total": parsed_total,
                                "target": self.limit if self.limit > 0 else len(self.discovered_urls)
                            })

                except Exception as e:
                    self.progress_callback("parse_error", {
                        "worker": worker_id,
                        "url": url,
                        "error": str(e)
                    })

                finally:
                    queue.task_done()

        except asyncio.CancelledError:
            pass
        finally:
            await context.close()

    async def _parse_single_url(
        self,
        page: Page,
        url: str,
        retry_count: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Parse a single product page"""
        try:
            # Navigate with timeout
            response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Handle rate limiting (429)
            if response and response.status == 429:
                self.rate_limit_retries += 1

                if retry_count < 5:
                    backoff = (2 ** retry_count) * 1.5 + random.uniform(0.25, 0.95)
                    self.progress_callback("rate_limited", {
                        "url": url,
                        "retry_after": backoff,
                        "retry_count": retry_count + 1
                    })

                    await asyncio.sleep(backoff)
                    return await self._parse_single_url(page, url, retry_count + 1)
                else:
                    return None

            # Check for successful response
            if not response or response.status >= 400:
                return None

            # Get page content
            content = await page.content()

            # Extract fields using BeautifulSoup
            extractor = FieldExtractor(content, url)
            product_data = extractor.extract()

            return product_data

        except Exception as e:
            if retry_count < 3:
                await asyncio.sleep(1.0)
                return await self._parse_single_url(page, url, retry_count + 1)
            return None

    def _rank_discovered_urls(self, urls: List[str]) -> List[str]:
        if not urls:
            return []
        return sorted(urls, key=lambda value: (-self._score_discovered_url(value), value))

    def _score_discovered_url(self, url: str) -> int:
        lower = url.strip().lower()
        score = 0

        if self._looks_like_product_url(lower):
            score += 18

        if re.search(r'-\d{4,}/?$', lower):
            score += 8

        if re.search(r'/catalog/(books|knigi)-\d+/?$', lower):
            score -= 14

        if "/books/details/" in lower:
            score += 14

        for token in (
            "/article/",
            "/articles/",
            "/blog/",
            "/news/",
            "/author/",
            "/authors/",
            "/search",
            "/cart",
            "/basket",
        ):
            if token in lower:
                score -= 10

        for token in ("/category/", "/categories/", "/catalog/", "/catalogue/", "/collection/"):
            if token in lower:
                score -= 4

        if "/product/" in lower:
            score += 10

        return score

    def _looks_like_product_url(self, url: str) -> bool:
        if re.search(r'/catalog/(books|knigi)-\d+/?$', url):
            return False
        product_patterns = (
            r'/product/',
            r'/products/',
            r'/item/',
            r'/book/',
            r'/books/details/[^/?#]+',
            r'/dp/',
            r'/p/',
            r'/isbn/',
            r'/goods/',
            r'/catalog/[^/]+/[^/]+-\d{4,}/?$',
        )
        for pattern in product_patterns:
            if re.search(pattern, url):
                return True
        if re.search(r'/[a-z0-9][a-z0-9-]{3,}-\d{4,}/?$', url):
            return True
        return False
