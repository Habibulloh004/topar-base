"""
Field Extractor - Extracts product fields from HTML pages
"""

import json
import re
from typing import Any, Dict, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup


class FieldExtractor:
    """Extracts structured product data from HTML"""

    def __init__(self, html_content: str, page_url: str):
        self.soup = BeautifulSoup(html_content, 'lxml')
        self.page_url = page_url
        self.data: Dict[str, Any] = {}
        self._sources: Set[str] = set()

    def extract(self) -> Dict[str, Any]:
        """Main extraction method - tries multiple strategies"""

        # Structured data first, then broader metadata and heuristic fallbacks.
        self._extract_from_jsonld()
        self._extract_meta_tags()

        # Compatibility/fallback extractors.
        self._extract_from_opengraph()
        self._extract_from_microdata()
        self._extract_from_schema()
        self._extract_from_html()

        # Add source URL
        self.data['source_url'] = self.page_url
        if 'url' not in self.data:
            self.data['url'] = self.page_url

        if self._sources:
            self.data['extraction'] = self._pick_primary_source()

        if not self._is_valid_product_record():
            return {}

        return self.data

    def _extract_from_jsonld(self) -> bool:
        """Extract from JSON-LD structured data"""
        scripts = self.soup.find_all('script', type='application/ld+json')
        found = False

        for script in scripts:
            try:
                raw = script.string or script.get_text()
                if not raw:
                    continue

                json_data = self._safe_json_loads(raw)
                if json_data is None:
                    continue

                # Handle @graph arrays
                items = json_data if isinstance(json_data, list) else [json_data]
                if isinstance(json_data, dict) and '@graph' in json_data:
                    graph = json_data.get('@graph')
                    if isinstance(graph, list):
                        items = graph
                    elif isinstance(graph, dict):
                        items = [graph]

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    if self._is_product_like_item(item):
                        # Keep full structured keys for product entities only.
                        flattened: Dict[str, Any] = {}
                        self._flatten_map(item, "", 3, flattened)
                        for key, value in flattened.items():
                            self._set_if_missing(key, self._normalize_value(key, value))
                        self._parse_jsonld_product(item)
                        self._sources.add("jsonld")
                        found = True

            except (json.JSONDecodeError, TypeError):
                continue

        return found

    def _parse_jsonld_product(self, item: Dict[str, Any]):
        """Parse JSON-LD product data"""
        # Name/Title
        if 'name' in item:
            self._set_if_missing('title', self._clean_text(item['name']))

        # Description
        if 'description' in item:
            self._set_if_missing('description', self._clean_text(item['description']))

        # ISBN
        if 'isbn' in item:
            self._set_if_missing('isbn', self._clean_text(item['isbn']))

        # GTIN/Barcode
        for field in ['gtin', 'gtin13', 'gtin14', 'gtin12', 'sku', 'productID']:
            if field in item:
                self._set_if_missing('gtin', self._clean_text(item[field]))
                break

        # Image
        if 'image' in item:
            image = item['image']
            if isinstance(image, str):
                self._set_if_missing('image', self._make_absolute_url(image))
            elif isinstance(image, list) and image:
                self._set_if_missing('image', self._make_absolute_url(str(image[0])))
            elif isinstance(image, dict) and 'url' in image:
                self._set_if_missing('image', self._make_absolute_url(str(image['url'])))

        # Price
        if 'offers' in item:
            offers = item['offers']
            if isinstance(offers, dict):
                if 'price' in offers:
                    self._set_if_missing('price', self._parse_price(offers['price']))
                if 'priceCurrency' in offers:
                    self._set_if_missing('currency', self._clean_text(offers['priceCurrency']))
                if 'availability' in offers:
                    self._set_if_missing('availability', self._clean_text(offers['availability']))
            elif isinstance(offers, list) and offers:
                first_offer = offers[0] if isinstance(offers[0], dict) else None
                if first_offer:
                    if 'price' in first_offer:
                        self._set_if_missing('price', self._parse_price(first_offer['price']))
                    if 'priceCurrency' in first_offer:
                        self._set_if_missing('currency', self._clean_text(first_offer['priceCurrency']))
                    if 'availability' in first_offer:
                        self._set_if_missing('availability', self._clean_text(first_offer['availability']))

        # Author (for books)
        if 'author' in item:
            author = item['author']
            if isinstance(author, str):
                self._set_if_missing('author', self._clean_text(author))
            elif isinstance(author, dict) and 'name' in author:
                self._set_if_missing('author', self._clean_text(author['name']))
            elif isinstance(author, list):
                authors = []
                for a in author:
                    if isinstance(a, str):
                        authors.append(self._clean_text(a))
                    elif isinstance(a, dict) and 'name' in a:
                        authors.append(self._clean_text(a['name']))
                if authors:
                    self._set_if_missing('author', ', '.join(authors))

        # Publisher
        if 'publisher' in item:
            publisher = item['publisher']
            if isinstance(publisher, str):
                self._set_if_missing('publisher', self._clean_text(publisher))
            elif isinstance(publisher, dict) and 'name' in publisher:
                self._set_if_missing('publisher', self._clean_text(publisher['name']))

        # Brand
        if 'brand' in item:
            brand = item['brand']
            if isinstance(brand, str):
                self._set_if_missing('brand', self._clean_text(brand))
            elif isinstance(brand, dict) and 'name' in brand:
                self._set_if_missing('brand', self._clean_text(brand['name']))

        # Additional properties
        for key in ['datePublished', 'numberOfPages', 'bookFormat', 'inLanguage']:
            if key in item:
                self._set_if_missing(key, self._normalize_value(key, item[key]))

        # Common rating aliases
        rating = item.get('aggregateRating')
        if isinstance(rating, dict):
            if 'ratingValue' in rating:
                self._set_if_missing('rating', self._normalize_value('rating', rating['ratingValue']))
            if 'reviewCount' in rating:
                self._set_if_missing('review_count', self._normalize_value('review_count', rating['reviewCount']))

    def _extract_meta_tags(self) -> bool:
        """Extract all meta tags into meta.* plus useful canonical fallbacks."""
        found = False
        for meta in self.soup.find_all('meta'):
            key = meta.get('property') or meta.get('name') or meta.get('itemprop')
            value = meta.get('content')
            if not key or not value:
                continue

            clean_key = self._normalize_key(key).lower()
            if not clean_key:
                continue

            found = True
            self._set_if_missing(f"meta.{clean_key}", self._normalize_value(clean_key, value))

            mapped_field = self._map_meta_key_to_field(clean_key)
            if mapped_field:
                normalized = self._normalize_value(mapped_field, value)
                self._set_if_missing(mapped_field, normalized)

            if clean_key.startswith("og:"):
                self._sources.add("opengraph")

        if found and "opengraph" not in self._sources:
            self._sources.add("meta")
        return found

    def _extract_from_opengraph(self):
        """Extract from Open Graph meta tags"""
        og_mapping = {
            'og:title': 'title',
            'og:description': 'description',
            'og:image': 'image',
            'og:price:amount': 'price',
            'og:price:currency': 'currency',
            'product:isbn': 'isbn',
        }

        for og_prop, field in og_mapping.items():
            if field in self.data:
                continue

            meta = self.soup.find('meta', property=og_prop)
            if meta and meta.get('content'):
                value = meta['content']
                self._set_if_missing(field, self._normalize_value(field, value))
                self._sources.add("opengraph")

    def _extract_from_microdata(self):
        """Extract from HTML5 microdata attributes"""
        found = False

        # Find elements with itemprop
        for elem in self.soup.find_all(attrs={'itemprop': True}):
            itemprop = self._normalize_key(elem.get('itemprop', ''))
            if not itemprop:
                continue
            found = True

            prop_mapping = {
                'name': 'title',
                'description': 'description',
                'image': 'image',
                'price': 'price',
                'priceCurrency': 'currency',
                'availability': 'availability',
                'isbn': 'isbn',
                'sku': 'sku',
                'gtin': 'gtin',
                'gtin13': 'gtin',
                'gtin14': 'gtin',
                'author': 'author',
                'publisher': 'publisher',
                'brand': 'brand',
            }
            normalized_lookup = re.sub(r'[^a-z0-9]', '', itemprop.lower())
            value = self._extract_element_value(elem)
            if value is None:
                continue

            self._set_if_missing(itemprop, self._normalize_value(itemprop, value))

            for source_key, target in prop_mapping.items():
                if re.sub(r'[^a-z0-9]', '', source_key.lower()) == normalized_lookup:
                    self._set_if_missing(target, self._normalize_value(target, value))
                    break

        if found:
            self._sources.add("microdata")

    def _extract_from_schema(self):
        """Extract from schema.org class names"""
        # Common schema.org patterns
        schema_selectors = {
            'title': ['.product-name', '.product-title', 'h1.title', 'h1[itemprop="name"]'],
            'description': ['.product-description', '.description', '[itemprop="description"]'],
            'price': ['.price', '.product-price', '[data-price]', '[itemprop="price"]'],
            'isbn': ['.isbn', '[data-isbn]'],
            'author': ['.author', '.book-author'],
        }

        found = False
        for field, selectors in schema_selectors.items():
            if field in self.data:
                continue

            for selector in selectors:
                elem = self.soup.select_one(selector)
                if elem:
                    if field == 'price':
                        value = (
                            elem.get('data-price')
                            or elem.get('content')
                            or elem.get_text(" ", strip=True)
                        )
                    else:
                        value = elem.get_text(" ", strip=True)
                    if value:
                        self._set_if_missing(field, self._normalize_value(field, value))
                        found = True
                        break
        if found:
            self._sources.add("schema")

    def _extract_from_html(self):
        """Fallback: heuristic extraction from HTML structure"""
        # Title: Usually in h1, or specific meta tags
        if 'title' not in self.data:
            h1 = self.soup.find('h1')
            if h1:
                self._set_if_missing('title', self._clean_text(h1.get_text()))
            else:
                title_meta = self.soup.find('meta', attrs={'name': 'title'})
                if title_meta:
                    self._set_if_missing('title', self._clean_text(title_meta.get('content', '')))
                elif self.soup.title:
                    self._set_if_missing('title', self._clean_text(self.soup.title.get_text()))

        # Description: meta description
        if 'description' not in self.data:
            desc_meta = self.soup.find('meta', attrs={'name': 'description'})
            if desc_meta:
                self._set_if_missing('description', self._clean_text(desc_meta.get('content', '')))

        # Image: first og:image or first large image
        if 'image' not in self.data:
            og_image = self.soup.find('meta', property='og:image')
            if og_image:
                self._set_if_missing('image', self._make_absolute_url(og_image.get('content', '')))
            else:
                # Find first non-inline image
                images = self.soup.find_all('img', src=True)
                for img in images:
                    src = img.get('src', '')
                    if src and not src.startswith('data:'):
                        self._set_if_missing('image', self._make_absolute_url(src))
                        break

        # Try to find data-attributes with useful info
        for elem in self.soup.find_all(attrs={'data-product-id': True}):
            self._set_if_missing('product_id', self._clean_text(elem.get('data-product-id')))

        for elem in self.soup.find_all(attrs={'data-sku': True}):
            self._set_if_missing('sku', self._clean_text(elem.get('data-sku')))

        for elem in self.soup.find_all(attrs={'data-price': True}):
            self._set_if_missing('price', self._parse_price(elem.get('data-price')))

        for elem in self.soup.find_all(attrs={'data-currency': True}):
            self._set_if_missing('currency', self._clean_text(elem.get('data-currency')))

        self._sources.add("html")

    def _set_if_missing(self, key: str, value: Any):
        """Set key only when missing and value is meaningful."""
        if not key or value is None:
            return
        if isinstance(value, str) and not value.strip():
            return
        if key in self.data:
            return
        self.data[key] = value

    def _extract_element_value(self, elem) -> Optional[str]:
        """Extract value from DOM element depending on tag type."""
        if elem.name == 'meta':
            return elem.get('content')
        if elem.name == 'img':
            return elem.get('src')
        if elem.name == 'a':
            return elem.get('href') or elem.get_text(strip=True)
        if elem.name in ('input', 'textarea'):
            return elem.get('value') or elem.get_text(strip=True)
        return elem.get_text(strip=True)

    def _map_meta_key_to_field(self, meta_key: str) -> Optional[str]:
        """Map known meta keys to canonical output fields."""
        mapping = {
            'title': 'title',
            'og:title': 'title',
            'twitter:title': 'title',
            'description': 'description',
            'og:description': 'description',
            'twitter:description': 'description',
            'og:image': 'image',
            'twitter:image': 'image',
            'og:url': 'url',
            'product:price:amount': 'price',
            'og:price:amount': 'price',
            'product:price:currency': 'currency',
            'og:price:currency': 'currency',
            'product:availability': 'availability',
            'og:availability': 'availability',
            'product:isbn': 'isbn',
            'book:isbn': 'isbn',
            'isbn': 'isbn',
        }
        return mapping.get(meta_key)

    def _is_product_like_item(self, item: Dict[str, Any]) -> bool:
        """Heuristic check for product-like JSON objects."""
        item_type = item.get('@type', '')
        type_tokens: Set[str] = set()
        if isinstance(item_type, str):
            lowered = item_type.lower()
            type_tokens.add(lowered)
            if 'product' in lowered or 'book' in lowered:
                return True
        elif isinstance(item_type, list):
            for t in item_type:
                if isinstance(t, str):
                    lowered = t.lower()
                    type_tokens.add(lowered)
                    if 'product' in lowered or 'book' in lowered:
                        return True

        if type_tokens and type_tokens.issubset({
            "website",
            "webpage",
            "organization",
            "localbusiness",
            "store",
            "breadcrumblist",
            "itemlist",
            "searchaction",
            "thing",
        }):
            return False

        keys = {re.sub(r'[^a-z0-9]', '', str(k).lower()) for k in item.keys()}
        has_name = 'name' in keys or 'title' in keys
        if not has_name:
            return False

        strong_markers = sum(
            marker in keys
            for marker in {
                'offers',
                'price',
                'sku',
                'isbn',
                'gtin',
                'gtin13',
                'gtin14',
                'productid',
                'availability',
            }
        )
        soft_markers = sum(
            marker in keys
            for marker in {
                'aggregaterating',
                'review',
                'author',
                'publisher',
                'brand',
                'numberofpages',
                'bookformat',
                'description',
                'image',
            }
        )

        return strong_markers >= 2 or (strong_markers >= 1 and soft_markers >= 1)

    def _flatten_map(self, value: Any, prefix: str, depth: int, out: Dict[str, Any]):
        """Flatten nested dict/list structure using dot notation."""
        if depth < 0:
            return

        if isinstance(value, dict):
            for raw_key, item in value.items():
                key = self._normalize_key(raw_key)
                if not key:
                    continue
                next_prefix = f"{prefix}.{key}" if prefix else key
                self._flatten_map(item, next_prefix, depth - 1, out)
            return

        if isinstance(value, list):
            if not value:
                return
            if self._all_primitives(value):
                values = []
                for item in value:
                    cleaned = self._clean_text(item)
                    if cleaned:
                        values.append(cleaned)
                if not values or not prefix:
                    return
                if self._looks_like_url_key(prefix):
                    out[prefix] = values[0]
                    if len(values) > 1:
                        out[f"{prefix}.all"] = ", ".join(values)
                else:
                    out[prefix] = ", ".join(values)
                return

            if depth <= 0:
                return

            for index, item in enumerate(value):
                next_prefix = f"{prefix}.{index}" if prefix else str(index)
                self._flatten_map(item, next_prefix, depth - 1, out)
            return

        if prefix:
            out[prefix] = value

    def _all_primitives(self, values: list) -> bool:
        for item in values:
            if isinstance(item, (dict, list)):
                return False
        return True

    def _normalize_key(self, key: Any) -> str:
        if key is None:
            return ""
        if not isinstance(key, str):
            key = str(key)
        key = key.strip()
        key = re.sub(r'\s+', '_', key)
        return key

    def _normalize_value(self, key: str, value: Any) -> Any:
        """Normalize value by key semantics."""
        if value is None:
            return None

        key_lower = key.lower()

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return value

        text = self._clean_text(value)
        if not text:
            return None

        if self._looks_like_url_key(key_lower):
            return self._make_absolute_url(text)

        if self._looks_like_count_key(key_lower):
            parsed_int = self._parse_int(text)
            if parsed_int is not None:
                return parsed_int

        if self._looks_like_numeric_key(key_lower):
            parsed_float = self._parse_price(text)
            if parsed_float is not None:
                return parsed_float
            # Skip huge noisy text blobs from heuristic selectors.
            if len(text) > 40:
                return None

        return text

    def _looks_like_url_key(self, key: str) -> bool:
        return any(
            token in key
            for token in ['url', 'image', 'logo', 'icon', 'thumbnail', 'cover']
        )

    def _looks_like_numeric_key(self, key: str) -> bool:
        return any(
            token in key
            for token in ['price', 'rating', 'score', 'amount', 'value', 'count', 'quantity', 'number']
        )

    def _looks_like_count_key(self, key: str) -> bool:
        return any(token in key for token in ['count', 'quantity', 'numberofpages', 'pages'])

    def _safe_json_loads(self, text: str) -> Any:
        """Robust JSON parsing for script contents."""
        text = text.strip()
        if not text:
            return None

        # Remove common JS wrappers.
        if text.startswith("<!--") and text.endswith("-->"):
            text = text[4:-3].strip()
        if text.endswith(";"):
            text = text[:-1].strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Some pages include multiple JSON objects in one script tag.
            decoder = json.JSONDecoder()
            index = 0
            values = []
            while index < len(text):
                while index < len(text) and text[index].isspace():
                    index += 1
                if index >= len(text):
                    break
                try:
                    value, next_index = decoder.raw_decode(text, index)
                except json.JSONDecodeError:
                    return None
                values.append(value)
                index = next_index
            if not values:
                return None
            return values if len(values) > 1 else values[0]

    def _clean_text(self, text: Any) -> str:
        """Clean and normalize text"""
        if text is None:
            return ""
        if not isinstance(text, str):
            text = str(text)

        # Remove extra whitespace
        text = ' '.join(text.split())

        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)

        return text.strip()

    def _parse_price(self, price: Any) -> Optional[float]:
        """Parse price string to float"""
        if isinstance(price, (int, float)):
            return float(price)

        if not isinstance(price, str):
            return None

        # Remove currency symbols and spaces
        price_str = re.sub(r'[^\d.,]', '', price)

        # Handle different decimal separators
        if ',' in price_str and '.' in price_str:
            # Assume last separator is decimal
            if price_str.rindex(',') > price_str.rindex('.'):
                price_str = price_str.replace('.', '').replace(',', '.')
            else:
                price_str = price_str.replace(',', '')
        else:
            price_str = price_str.replace(',', '.')

        try:
            return float(price_str)
        except ValueError:
            return None

    def _parse_int(self, value: Any) -> Optional[int]:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if not isinstance(value, str):
            return None
        value = value.strip()
        if not value:
            return None
        value = re.sub(r'[^\d-]', '', value)
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _make_absolute_url(self, url: str) -> str:
        """Convert relative URL to absolute"""
        if not url:
            return url

        if url.startswith('data:'):
            return url

        return urljoin(self.page_url, url)

    def _pick_primary_source(self) -> str:
        priority = [
            "jsonld",
            "opengraph",
            "microdata",
            "schema",
            "meta",
            "html",
        ]
        for source in priority:
            if source in self._sources:
                return source
        return "html"

    def _is_valid_product_record(self) -> bool:
        title = self._clean_text(self.data.get('title', ''))
        if len(title) < 2:
            return False

        has_price = False
        price_value = self.data.get('price')
        if isinstance(price_value, (int, float)):
            has_price = float(price_value) > 0
        elif isinstance(price_value, str):
            parsed_price = self._parse_price(price_value)
            has_price = parsed_price is not None and parsed_price > 0

        has_strong_id = any(
            self._clean_text(self.data.get(field, ""))
            for field in ('isbn', 'gtin', 'sku', 'product_id')
        )
        has_availability = bool(self._clean_text(self.data.get('availability', '')))
        has_image = bool(self._clean_text(self.data.get('image', '')))
        has_description = len(self._clean_text(self.data.get('description', ''))) >= 40
        url_product_like = self._looks_like_product_url(self.page_url.lower())

        meta_og_type = self._clean_text(self.data.get('meta.og:type', '')).lower()
        meta_product = 'product' in meta_og_type or 'book' in meta_og_type
        type_product = self._has_product_type(self.data.get('@type'))

        if type_product and (has_price or has_strong_id or has_availability or url_product_like):
            return True
        if meta_product and (has_price or has_strong_id or has_availability or url_product_like):
            return True
        if has_price and (has_strong_id or has_availability or url_product_like):
            return True
        if has_strong_id and (has_price or has_availability or url_product_like or has_image):
            return True
        if url_product_like and has_image and has_description:
            return True

        return False

    def _has_product_type(self, type_value: Any) -> bool:
        if isinstance(type_value, str):
            lowered = type_value.lower()
            return 'product' in lowered or 'book' in lowered
        if isinstance(type_value, list):
            for item in type_value:
                if isinstance(item, str):
                    lowered = item.lower()
                    if 'product' in lowered or 'book' in lowered:
                        return True
        return False

    def _looks_like_product_url(self, url_lower: str) -> bool:
        if re.search(r'/catalog/(books|knigi)-\d+/?$', url_lower):
            return False
        for token in ('/product/', '/products/', '/item/', '/book/', '/isbn/', '/dp/', '/p/', '/goods/'):
            if token in url_lower:
                return True
        if re.search(r'/[a-z0-9][a-z0-9-]{3,}-\d{4,}/?$', url_lower):
            return True
        return False
