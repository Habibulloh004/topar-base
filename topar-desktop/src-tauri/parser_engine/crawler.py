"""
URL Crawler - Discovers product URLs from sitemaps or by crawling
"""

import asyncio
import re
from typing import Callable, List, Optional, Set
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from playwright.async_api import Browser


class UrlCrawler:
    """Discovers product URLs from sitemaps or by crawling pages"""

    def __init__(
        self,
        source_url: str,
        max_sitemaps: int = 120,
        target_limit: int = 0,
        progress_callback: Optional[Callable] = None
    ):
        self.source_url = source_url
        self.max_sitemaps = max_sitemaps
        self.target_limit = max(0, int(target_limit or 0))
        self.progress_callback = progress_callback or (lambda *args: None)
        self.base_domain = self._extract_domain(source_url)

    def _extract_domain(self, url: str) -> str:
        """Extract base domain from URL"""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    async def discover_from_site_api(self) -> List[str]:
        """Discover URLs from known site APIs when DOM/sitemap has no product links."""
        if self._is_bookuz_books_listing():
            return await self._discover_bookuz_books_api()
        return []

    def _is_bookuz_books_listing(self) -> bool:
        parsed = urlparse(self.source_url)
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]

        if host != "book.uz":
            return False

        path = (parsed.path or "").rstrip("/").lower()
        return path == "/books"

    async def _discover_bookuz_books_api(self) -> List[str]:
        """Fetch book.uz product detail URLs via backend API pagination."""
        self.progress_callback("api_discovery_started", {
            "source": "book.uz",
            "endpoint": "https://backend.book.uz/user-api/book",
        })

        urls: List[str] = []
        seen: Set[str] = set()
        page = 1
        per_page = 100
        max_pages = 1500
        soft_target = 0
        if self.target_limit > 0:
            soft_target = max(self.target_limit + 50, self.target_limit * 2)

        while page <= max_pages:
            api_url = f"https://backend.book.uz/user-api/book?page={page}&limit={per_page}"
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                self.progress_callback("api_discovery_error", {
                    "source": "book.uz",
                    "page": page,
                    "error": str(exc),
                })
                break

            data = payload.get("data") if isinstance(payload, dict) else None
            items = data.get("data") if isinstance(data, dict) else None
            total = data.get("total") if isinstance(data, dict) else None

            if not isinstance(items, list) or not items:
                break

            for item in items:
                if not isinstance(item, dict):
                    continue
                slug = str(item.get("link") or item.get("_id") or "").strip()
                if not slug:
                    continue

                product_url = urljoin(self.base_domain, f"/books/details/{slug}")
                if product_url in seen:
                    continue
                seen.add(product_url)
                urls.append(product_url)

                if soft_target > 0 and len(urls) >= soft_target:
                    break

            self.progress_callback("api_page_processed", {
                "source": "book.uz",
                "page": page,
                "items_found": len(items),
                "total_urls": len(urls),
                "reported_total": int(total) if isinstance(total, int) else None,
            })

            if soft_target > 0 and len(urls) >= soft_target:
                break
            if isinstance(total, int) and total > 0 and len(urls) >= total:
                break

            page += 1
            await asyncio.sleep(0.05)

        return urls

    async def discover_from_sitemap(self, browser: Browser) -> List[str]:
        """Try to discover URLs from XML sitemaps"""
        self.progress_callback("checking_sitemap", {"url": self.source_url})

        # Common sitemap locations
        sitemap_urls = [
            self.source_url if "sitemap" in self.source_url.lower() else None,
            f"{self.base_domain}/sitemap.xml",
            f"{self.base_domain}/sitemap_index.xml",
            f"{self.base_domain}/product-sitemap.xml",
            f"{self.base_domain}/products-sitemap.xml",
        ]

        # Check robots.txt for sitemap
        try:
            robots_url = f"{self.base_domain}/robots.txt"
            robots_resp = requests.get(robots_url, timeout=10)
            if robots_resp.status_code == 200:
                for line in robots_resp.text.split('\n'):
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        sitemap_urls.append(sitemap_url)
        except Exception:
            pass

        # Remove None values and duplicates
        sitemap_urls = list(dict.fromkeys(url for url in sitemap_urls if url))

        all_urls: Set[str] = set()
        processed_sitemaps = 0

        for sitemap_url in sitemap_urls:
            if processed_sitemaps >= self.max_sitemaps:
                break

            try:
                urls = await self._parse_sitemap(sitemap_url)
                all_urls.update(urls)
                processed_sitemaps += 1

                self.progress_callback("sitemap_processed", {
                    "url": sitemap_url,
                    "urls_found": len(urls),
                    "total_urls": len(all_urls)
                })

            except Exception as e:
                self.progress_callback("sitemap_error", {
                    "url": sitemap_url,
                    "error": str(e)
                })

        return list(all_urls)

    async def _parse_sitemap(self, sitemap_url: str) -> List[str]:
        """Parse a single sitemap XML file"""
        try:
            response = requests.get(sitemap_url, timeout=30)
            response.raise_for_status()

            # Parse XML
            root = ET.fromstring(response.content)

            # Check if this is a sitemap index
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            # Look for nested sitemaps
            sitemap_elements = root.findall('.//ns:sitemap/ns:loc', namespace)
            if sitemap_elements:
                # This is a sitemap index, recursively parse child sitemaps
                all_urls = []
                for elem in sitemap_elements[:self.max_sitemaps]:
                    child_url = elem.text
                    if child_url:
                        child_urls = await self._parse_sitemap(child_url)
                        all_urls.extend(child_urls)
                return all_urls

            # Look for URL entries
            url_elements = root.findall('.//ns:url/ns:loc', namespace)
            urls = [elem.text for elem in url_elements if elem.text]

            # If no namespace, try without namespace
            if not urls:
                url_elements = root.findall('.//url/loc')
                urls = [elem.text for elem in url_elements if elem.text]

            return urls

        except ET.ParseError:
            # Maybe it's not XML, try parsing as text
            try:
                response = requests.get(sitemap_url, timeout=30)
                urls = re.findall(r'https?://[^\s<>"]+', response.text)
                return urls
            except Exception:
                return []
        except Exception:
            return []

    async def discover_by_crawling(self, browser: Browser) -> List[str]:
        """Discover URLs by crawling pages (fallback method)"""
        self.progress_callback("crawling_started", {"source": self.source_url})

        context = await browser.new_context()
        page = await context.new_page()

        discovered_urls: Set[str] = set()
        to_visit = [self.source_url]
        visited = set()
        max_pages_to_crawl = 50  # Limit crawling to avoid infinite loops

        try:
            while to_visit and len(discovered_urls) < 2000 and len(visited) < max_pages_to_crawl:
                current_url = to_visit.pop(0)

                if current_url in visited:
                    continue

                visited.add(current_url)

                try:
                    await page.goto(current_url, wait_until="domcontentloaded", timeout=15000)
                    content = await page.content()

                    # Parse with BeautifulSoup
                    soup = BeautifulSoup(content, 'lxml')

                    # Find all links
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        absolute_url = urljoin(current_url, href)

                        # Only process URLs from same domain
                        if not absolute_url.startswith(self.base_domain):
                            continue

                        # Check if this looks like a product page
                        if self._is_product_url(absolute_url):
                            discovered_urls.add(absolute_url)

                        # Check if this looks like a category/listing page
                        elif self._is_listing_url(absolute_url) and absolute_url not in visited:
                            if len(to_visit) < 20:  # Limit queue size
                                to_visit.append(absolute_url)

                    # Check for pagination
                    pagination_links = self._find_pagination_links(soup, current_url)
                    for next_page_url in pagination_links:
                        if next_page_url not in visited and len(to_visit) < 20:
                            to_visit.append(next_page_url)

                    self.progress_callback("crawling_progress", {
                        "current_url": current_url,
                        "discovered": len(discovered_urls),
                        "visited": len(visited)
                    })

                    # Rate limiting
                    await asyncio.sleep(0.5)

                except Exception as e:
                    self.progress_callback("crawl_error", {
                        "url": current_url,
                        "error": str(e)
                    })

        finally:
            await context.close()

        return list(discovered_urls)

    def _is_product_url(self, url: str) -> bool:
        """Heuristic to determine if URL is a product page"""
        url_lower = url.lower()

        # Explicit listing/category endpoints should not be treated as products.
        if re.search(r'/catalog/(books|knigi)-\d+/?$', url_lower):
            return False
        if re.search(r'/catalogue/(books|knigi)-\d+/?$', url_lower):
            return False

        # Strong product URL markers.
        for pattern in (
            r'/product/',
            r'/products/',
            r'/item/',
            r'/book/',
            r'/books/details/[^/?#]+',
            r'/isbn/',
            r'/dp/',
            r'/p/',
            r'/goods/',
            r'/catalog/[^/]+/[^/]+-\d{4,}/?$',
            r'/product-\d+',
        ):
            if re.search(pattern, url_lower):
                return True

        # Slug-with-id product paths.
        if re.search(r'/[a-z0-9][a-z0-9-]{3,}-\d{4,}/?$', url_lower):
            return True

        return False

    def _is_listing_url(self, url: str) -> bool:
        """Heuristic to determine if URL is a category/listing page"""
        listing_patterns = [
            r'/category/',
            r'/catalog/',
            r'/collection/',
            r'/c/',
            r'/shop/',
            r'/products$',
            r'/books(?:/)?(?:\?|$)',
        ]

        url_lower = url.lower()
        for pattern in listing_patterns:
            if re.search(pattern, url_lower):
                return True

        return False

    def _find_pagination_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """Find pagination next page links"""
        next_links = []

        # Look for common pagination patterns
        pagination_selectors = [
            ('a', {'class': re.compile(r'next|pagination-next', re.I)}),
            ('a', {'rel': 'next'}),
            ('a', {'aria-label': re.compile(r'next', re.I)}),
        ]

        for tag, attrs in pagination_selectors:
            elements = soup.find_all(tag, attrs)
            for elem in elements:
                href = elem.get('href')
                if href:
                    absolute_url = urljoin(current_url, href)
                    if absolute_url.startswith(self.base_domain):
                        next_links.append(absolute_url)

        # Look for numbered pagination
        pagination_area = soup.find(['nav', 'div'], class_=re.compile(r'pagination', re.I))
        if pagination_area:
            for link in pagination_area.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(current_url, href)
                if absolute_url.startswith(self.base_domain) and absolute_url != current_url:
                    next_links.append(absolute_url)

        return next_links[:5]  # Limit to first 5 pagination links
