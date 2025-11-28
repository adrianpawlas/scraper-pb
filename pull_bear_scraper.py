#!/usr/bin/env python3
"""
Pull & Bear Fashion Scraper
Scrapes all products from Pull & Bear, generates embeddings, and stores in Supabase.
"""

import asyncio
import io
import json
import logging
import time
import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin

import aiohttp
import requests
import torch
from PIL import Image
from supabase import create_client, Client
from transformers import AutoProcessor, AutoModel
from tqdm.asyncio import tqdm
import psycopg2
from psycopg2.extras import execute_values

from config import (
    SUPABASE_URL, SUPABASE_KEY, PULL_BEAR_BASE_URL, PULL_BEAR_APP_ID,
    PULL_BEAR_LANGUAGE_ID, PULL_BEAR_LOCALE, BATCH_SIZE, MAX_WORKERS,
    EMBEDDING_MODEL, CATEGORY_IDS, GENDER_MAPPING, CATEGORY_CLASSIFICATION,
    PRODUCT_LIMIT
)

# Import additional functions for loading product IDs
import requests
from typing import Dict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pull_bear_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
]

def get_random_user_agent() -> str:
    """Get a random user agent from the list."""
    return random.choice(USER_AGENTS)

def get_realistic_headers(referer: str = None) -> Dict[str, str]:
    """Get realistic HTTP headers for requests."""
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }

    if referer:
        headers['Referer'] = referer

    return headers


def load_category_urls(filename: str = "category_urls.txt") -> Dict[str, str]:
    """
    Load category URLs from a text file.
    Format: category_id=url
    Lines starting with # are ignored.
    """
    urls = {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        category_id, url = line.split('=', 1)
                        urls[category_id.strip()] = url.strip()
    except FileNotFoundError:
        logger.warning(f"{filename} not found, will use API fallback")
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")

    return urls


async def load_product_ids_from_url_async(category_id: str, urls: Dict[str, str]) -> List[int]:
    """
    Load product IDs from a URL using Playwright to bypass API blocking.
    This is the primary method for loading from category_urls.txt
    """
    if category_id not in urls:
        return []

    url = urls[category_id]

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not available, cannot load from URL")
        return []

    try:
        # Add delay to avoid rate limiting
        import asyncio
        await asyncio.sleep(1)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--single-process',
                    '--disable-gpu'
                ]
            )
            context = await browser.new_context(
                user_agent=get_random_user_agent(),
                viewport={'width': 1280, 'height': 720},
                locale='en-GB',
                timezone_id='Europe/London'
            )
            page = await context.new_page()

            try:
                # Visit main site first to set cookies and establish session
                await page.goto('https://www.pullandbear.com/', wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(2)

                # Navigate to men's section to establish browsing pattern
                await page.goto('https://www.pullandbear.com/en/man-c1030148976.html', wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(1)

                # Make the API request through Playwright with realistic headers
                headers = get_realistic_headers('https://www.pullandbear.com/en/man-c1030148976.html')
                response = await page.request.get(url, headers=headers, timeout=30000)

                if response.status == 200:
                    try:
                        data = await response.json()
                        product_ids = data.get("productIds", [])
                        if product_ids:
                            logger.info(f"Successfully loaded {len(product_ids)} product IDs from URL for category {category_id}")
                            return product_ids
                        else:
                            logger.warning(f"No productIds found in URL response for category {category_id}")
                            logger.debug(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'not dict'}")
                    except Exception as json_error:
                        logger.error(f"Failed to parse JSON response for category {category_id}: {json_error}")
                        # Try to get text response for debugging
                        text = await response.text()
                        logger.debug(f"Raw response (first 500 chars): {text[:500]}")
                else:
                    logger.warning(f"URL request failed with status {response.status} for category {category_id}")
                    logger.debug(f"URL: {url}")

            finally:
                await browser.close()

    except Exception as e:
        logger.error(f"Error loading from URL with Playwright for category {category_id}: {e}")

    return []




class PullBearScraper:
    """Main scraper class for Pull & Bear products."""

    def __init__(self):
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.session: Optional[aiohttp.ClientSession] = None
        self.processor = None
        self.model = None
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.category_urls = load_category_urls()

        # Initialize the embedding model
        self._init_embedding_model()

    def _init_embedding_model(self):
        """Initialize the SigLIP model for image embeddings."""
        try:
            logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
            self.processor = AutoProcessor.from_pretrained(EMBEDDING_MODEL)
            self.model = AutoModel.from_pretrained(EMBEDDING_MODEL)
            self.model.eval()
            logger.info("Embedding model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            raise

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            headers=get_realistic_headers('https://www.pullandbear.com/'),
            connector=aiohttp.TCPConnector(limit=10, ttl_dns_cache=300),
            timeout=aiohttp.ClientTimeout(total=30, connect=10)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=True)

    def build_api_url(self, category_id: int, product_ids: List[int] = None, page: int = None) -> str:
        """Build the Pull & Bear API URL for a category and optionally product IDs."""
        url = (
            f"{PULL_BEAR_BASE_URL}/productsArray?"
            f"categoryId={category_id}&"
            f"appId={PULL_BEAR_APP_ID}&"
            f"languageId={PULL_BEAR_LANGUAGE_ID}&"
            f"locale={PULL_BEAR_LOCALE}"
        )

        if product_ids:
            product_ids_str = '%2C'.join(map(str, product_ids))
            url += f"&productIds={product_ids_str}"

        if page is not None:
            url += f"&page={page}"

        return url

    async def fetch_products_batch(self, category_id: int, product_ids: List[int] = None, page: int = None) -> Dict[str, Any]:
        """Fetch a batch of products from the Pull & Bear API with retry logic."""
        url = self.build_api_url(category_id, product_ids, page)
        max_retries = 3

        for attempt in range(max_retries):
            try:
                # Update headers with fresh user agent for each attempt
                headers = get_realistic_headers('https://www.pullandbear.com/')
                headers.update({'Accept': 'application/json'})

                async with self.session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        products = data.get('products', [])
                        logger.info(f"API returned {len(products)} products for category {category_id} (page {page or 0})")
                        return data
                    elif response.status == 403:
                        logger.warning(f"403 Forbidden on attempt {attempt + 1}/{max_retries} for {url}")
                        if attempt < max_retries - 1:
                            delay = (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff with jitter
                            logger.info(f"Waiting {delay:.2f} seconds before retry...")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.error(f"403 Forbidden after {max_retries} attempts: {url}")
                            return {"products": []}
                    else:
                        logger.warning(f"API request failed with status {response.status}: {url}")
                        return {"products": []}
            except Exception as e:
                logger.error(f"Error fetching products (attempt {attempt + 1}/{max_retries}): {e} - {url}")
                if attempt < max_retries - 1:
                    delay = (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(delay)
                else:
                    return {"products": []}

        return {"products": []}

    def extract_product_info(self, product: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract product information from the API response."""
        products_data = []

        # Handle bundle products (products with multiple colors/variants)
        if product.get('bundleProductSummaries'):
            for variant in product['bundleProductSummaries']:
                if variant.get('detail'):
                    colors = variant['detail'].get('colors', [])
                    if not colors:
                        continue

                    for color in colors:
                        try:
                            product_data = self._extract_single_product(product, variant, color)
                            if product_data:
                                products_data.append(product_data)
                        except Exception as e:
                            logger.error(f"Error extracting product: {e}")
                            continue
        else:
            # Handle single products
            if product.get('detail', {}).get('colors'):
                for color in product['detail']['colors']:
                    product_data = self._extract_single_product(product, product, color)
                    if product_data:
                        products_data.append(product_data)

        logger.debug(f"Extracted {len(products_data)} products from {product.get('id')}")
        return products_data

    def _extract_single_product(self, bundle_product: Dict, variant: Dict, color: Dict) -> Optional[Dict[str, Any]]:
        """Extract information for a single product variant."""
        try:
            # Get the best image URL (prioritizes product-only "p1" shots)
            image_url = self._get_best_image_url(variant)

            if not image_url:
                return None

            # Extract basic information
            reference = color.get('reference', color.get('displayReference', f"no_ref_{color['id']}"))
            product_id = f"{reference}-{color['id']}"
            title = bundle_product.get('nameEn', bundle_product.get('name', ''))
            description = variant.get('detail', {}).get('longDescription', '')

            # Extract category information
            category = self._extract_category(bundle_product)

            # Extract gender
            gender = GENDER_MAPPING.get(variant.get('sectionNameEN', ''), 'unisex')

            # Extract pricing (use the first available size's price)
            price = None
            currency = 'EUR'  # Pull & Bear uses EUR
            if color.get('sizes') and len(color['sizes']) > 0:
                first_size = color['sizes'][0]
                price_str = first_size.get('price', '')
                if price_str:
                    try:
                        # Convert cents to decimal (e.g., 9990 -> 99.90)
                        price_cents = int(price_str)
                        price = float(price_cents) / 100
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse price: {price_str}")
                        price = None

            # Extract size information
            sizes = [size['name'] for size in color.get('sizes', []) if size.get('isBuyable')]

            # Build product URL
            product_url = f"https://www.pullandbear.com/en/{variant.get('productUrl', '')}.html"

            # Create comprehensive metadata
            metadata = {
                'reference': color.get('reference', ''),
                'display_reference': color.get('displayReference', ''),
                'color_name': color.get('name', ''),
                'color_id': color['id'],
                'sizes': sizes,
                'availability_date': variant.get('availabilityDate', ''),
                'family_info': variant.get('detail', {}).get('familyInfo', {}),
                'subfamily_info': variant.get('detail', {}).get('subfamilyInfo', {}),
                'certified_materials': color.get('certifiedMaterials', {}),
                'composition': color.get('composition', []),
                'composition_by_zone': color.get('compositionByZone', []),
                'care_instructions': color.get('care', []),
                'sustainability': color.get('sustainability', {}),
                'traceability': color.get('traceability', {}),
                'country_of_origin': color.get('country', ''),
                'weight': first_size.get('weight', '') if color.get('sizes') and len(color['sizes']) > 0 else '',
                'bundle_colors': bundle_product.get('bundleColors', []),
                'related_categories': bundle_product.get('relatedCategories', []),
                'tags': bundle_product.get('tags', []),
                'attributes': bundle_product.get('attributes', []),
            }

            return {
                'id': product_id,
                'source': 'scraper',
                'product_url': product_url,
                'affiliate_url': None,
                'image_url': image_url,
                'brand': 'Pull & Bear',
                'title': title,
                'description': description,
                'category': self._classify_category(bundle_product),
                'gender': gender,
                'price': price,
                'currency': currency,
                'metadata': json.dumps(metadata),
                'size': ', '.join(sizes) if sizes else None,
                'second_hand': False,
                'created_at': None,  # Will be set by database default
                'embedding': None,  # Will be added later
            }

        except Exception as e:
            logger.error(f"Error extracting product info: {e}")
            return None

    def _get_best_image_url(self, variant: Dict) -> Optional[str]:
        """Get the best quality image URL from variant data - prioritizes product-only shots."""
        try:
            variant_detail = variant.get('detail', {})
            if not variant_detail:
                return None

            xmedia = variant_detail.get('xmedia', [])
            if not xmedia:
                return None

            # First priority: Look for "s1" (product-only shot)
            for xmedia_item in xmedia:
                for item in xmedia_item.get('xmediaItems', []):
                    medias = item.get('medias', [])
                    for media in medias:
                        if media.get('extraInfo', {}).get('originalName') == 's1':
                            # Try deliveryUrl first (higher quality), then fallback to url
                            url = None
                            if media.get('extraInfo', {}).get('deliveryUrl'):
                                url = media['extraInfo']['deliveryUrl']
                            elif media.get('url'):
                                url = media['url']

                            if url:
                                if url.startswith('//'):
                                    url = 'https:' + url
                                elif url.startswith('/') and 'pullandbear' in url:
                                    url = 'https://static.pullandbear.net' + url
                                elif url.startswith('assets/'):
                                    url = 'https://static.pullandbear.net/' + url
                                return url

            # Second priority: If no "p1" found, get any product image
            for xmedia_item in xmedia:
                for item in xmedia_item.get('xmediaItems', []):
                    medias = item.get('medias', [])
                    if medias:
                        media = medias[0]  # Get first available media
                        url = None
                        if media.get('extraInfo', {}).get('deliveryUrl'):
                            url = media['extraInfo']['deliveryUrl']
                        elif media.get('url'):
                            url = media['url']

                        if url:
                            if url.startswith('//'):
                                url = 'https:' + url
                            elif url.startswith('/') and 'pullandbear' in url:
                                url = 'https://static.pullandbear.net' + url
                            elif url.startswith('assets/'):
                                url = 'https://static.pullandbear.net/' + url
                            return url

            return None

        except Exception as e:
            logger.error(f"Error extracting image URL: {e}")
            return None

    def _extract_category(self, product: Dict) -> str:
        """Extract category information from product data."""
        try:
            categories = []
            for cat in product.get('relatedCategories', []):
                if cat.get('name'):
                    categories.append(cat['name'])

            return ', '.join(categories) if categories else 'Unknown'

        except Exception as e:
            logger.error(f"Error extracting category: {e}")
            return 'Unknown'

    def _classify_category(self, product: Dict) -> Optional[str]:
        """Classify category as accessory, footwear, or None for clothing."""
        try:
            # Find the category key from our configuration
            for gender_key, categories in CATEGORY_IDS.items():
                for category_key, category_id in categories.items():
                    # Check if this product belongs to a classified category
                    if CATEGORY_CLASSIFICATION.get(category_key):
                        # Check if the product's related categories contain this category
                        for related_cat in product.get('relatedCategories', []):
                            if related_cat.get('id') == category_id:
                                return CATEGORY_CLASSIFICATION[category_key]

            # If not found in classified categories, return None (clothing)
            return None

        except Exception as e:
            logger.error(f"Error classifying category: {e}")
            return None

    async def generate_embedding(self, image_url: str) -> Optional[List[float]]:
        """Generate 768-dim embedding for an image URL with better error handling."""
        try:
            # Validate URL
            if not image_url or not image_url.startswith(('http://', 'https://')):
                logger.warning(f"Invalid image URL: {image_url}")
                return None

            # Download image with timeout and headers
            headers = get_realistic_headers('https://www.pullandbear.com/')
            headers.update({'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8'})

            async with self.session.get(image_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status != 200:
                    logger.warning(f"Failed to download image {image_url}: HTTP {response.status}")
                    return None

                # Check content type
                content_type = response.headers.get('content-type', '').lower()
                if not content_type.startswith('image/'):
                    logger.warning(f"Invalid content type for {image_url}: {content_type}")
                    return None

                image_data = await response.read()

                # Basic validation of image data
                if len(image_data) < 100:  # Too small to be a valid image
                    logger.warning(f"Image data too small for {image_url}: {len(image_data)} bytes")
                    return None

            # Process image in thread pool
            loop = asyncio.get_event_loop()
            embedding = await loop.run_in_executor(
                self.executor,
                self._process_image_embedding,
                image_data
            )

            return embedding

        except asyncio.TimeoutError:
            logger.warning(f"Timeout downloading image: {image_url}")
            return None
        except Exception as e:
            logger.error(f"Error generating embedding for {image_url}: {e}")
            return None

    def _process_image_embedding(self, image_data: bytes) -> Optional[List[float]]:
        """Process image and generate embedding (runs in thread pool) with better error handling."""
        try:
            # Open image
            image = Image.open(io.BytesIO(image_data))

            # Convert to RGB and ensure it's valid
            try:
                image = image.convert('RGB')
                # Verify image is not corrupted by trying to load it
                image.load()
            except Exception as convert_error:
                logger.warning(f"Failed to convert/validate image: {convert_error}")
                image.close()
                return None

            # Check minimum dimensions
            if image.size[0] < 10 or image.size[1] < 10:
                logger.warning(f"Image too small: {image.size}")
                image.close()
                return None

            # Process with SigLIP - need both image and text inputs
            inputs = self.processor(
                images=image,
                text=[""],  # Empty text input
                return_tensors="pt",
                padding=True
            )

            with torch.no_grad():
                outputs = self.model(**inputs)

            # Close image to free memory
            image.close()

            # Get the image embeddings (768-dim)
            if hasattr(outputs, 'image_embeds'):
                embedding = outputs.image_embeds.squeeze().tolist()
            else:
                # Fallback: try pooler_output or last_hidden_state
                if hasattr(outputs, 'pooler_output'):
                    embedding = outputs.pooler_output.squeeze().tolist()
                elif hasattr(outputs, 'last_hidden_state'):
                    embedding = outputs.last_hidden_state.mean(dim=1).squeeze().tolist()
                else:
                    logger.error("No suitable embedding output found")
                    return None

            # Ensure it's a list of floats
            if isinstance(embedding, float):
                return [embedding]
            elif isinstance(embedding, list):
                return embedding
            else:
                # Convert numpy array or tensor to list
                return embedding.tolist()

        except Exception as e:
            logger.error(f"Error processing image embedding: {e}")
            return None

    async def scrape_category(self, category_name: str, category_id: int, product_ids: List[int] = None, stats: Dict[str, int] = None) -> List[Dict[str, Any]]:
        """Scrape all products from a specific category using the provided product IDs."""
        logger.info(f"Starting to scrape category: {category_name} (ID: {category_id}) with {len(product_ids or [])} product IDs")

        all_products = []

        try:
            # Try to get ALL products from the category with pagination support
            logger.info(f"Fetching ALL products from category {category_name} with pagination...")

            # Try multiple pages to get all products
            max_pages = 50  # Try up to 50 pages to get all products
            products_found = False

            for page in range(max_pages):
                data = await self.fetch_products_batch(category_id, page=page)

                if data.get('products') and len(data['products']) > 0:
                    products_found = True
                    logger.info(f"Page {page}: got {len(data['products'])} products from category {category_name}")

                    for product in data['products']:
                        try:
                            product_data = self.extract_product_info(product)
                            all_products.extend(product_data)
                        except Exception as e:
                            logger.error(f"Error extracting product info for product {product.get('id', 'unknown')}: {e}")
                            stats['extraction_errors'] += 1
                            continue

                        # Respect product limit during extraction
                        if PRODUCT_LIMIT > 0 and len(all_products) >= PRODUCT_LIMIT:
                            break

                    # If we got products but very few, it might be the last page
                    if len(data['products']) < 10:  # Assume less than 10 means last page
                        logger.info(f"Reached end of pagination at page {page}")
                        break
                else:
                    # No more products on this page
                    logger.info(f"No more products found at page {page}, stopping pagination")
                    break

                # Check if we've hit the product limit
                if PRODUCT_LIMIT > 0 and len(all_products) >= PRODUCT_LIMIT:
                    logger.info(f"Reached product limit of {PRODUCT_LIMIT}, stopping pagination")
                    break

            if not products_found:
                # Fallback: if no products returned through pagination, try with the provided product IDs
                logger.warning(f"No products found through pagination for category {category_name}, trying with provided product IDs")
                if product_ids:
                    data = await self.fetch_products_batch(category_id, product_ids)
                    if data.get('products'):
                        for product in data['products']:
                            product_data = self.extract_product_info(product)
                            all_products.extend(product_data)

                            # Respect product limit during extraction
                            if PRODUCT_LIMIT > 0 and len(all_products) >= PRODUCT_LIMIT:
                                break

        except Exception as e:
            logger.error(f"Error scraping category {category_name}: {e}")

        logger.info(f"Found {len(all_products)} products in category {category_name}")
        return all_products

    async def process_products_batch(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of products: generate embeddings and prepare for database."""
        processed_products = []

        for product in products:
            # Generate embedding
            if product['image_url']:
                embedding = await self.generate_embedding(product['image_url'])
                if embedding:
                    product['embedding'] = embedding
                    processed_products.append(product)

        return processed_products

    async def save_to_supabase(self, products: List[Dict[str, Any]]) -> int:
        """Save products to Supabase database."""
        try:
            # Convert embeddings to proper format for Supabase vector
            for product in products:
                if product.get('embedding'):
                    product['embedding'] = f"[{', '.join(map(str, product['embedding']))}]"

            # Insert in batches
            batch_size = 100
            inserted_count = 0

            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]

                result = self.supabase.table('products').upsert(
                    batch,
                    on_conflict='source,product_url'
                ).execute()

                inserted_count += len(result.data) if result.data else 0

            logger.info(f"Inserted/updated {inserted_count} products in database")
            return inserted_count

        except Exception as e:
            logger.error(f"Error saving to Supabase: {e}")
            return 0

    async def run_full_scrape(self):
        """Run the complete scraping pipeline."""
        logger.info("Starting full Pull & Bear scrape")

        start_time = time.time()
        total_products = 0
        stats = {
            'categories_processed': 0,
            'categories_failed': 0,
            'categories_skipped': 0,
            'products_found': 0,
            'products_with_embeddings': 0,
            'embedding_errors': 0,
            'extraction_errors': 0
        }

        # Scrape all categories
        all_products = []
        seen_product_urls = set()  # Track unique product URLs to avoid duplicates

        # Scrape men's categories - get ALL products from each category
        for category_name, category_data in CATEGORY_IDS['men'].items():
            category_id = category_data['category_id']
            stats['categories_processed'] += 1

            try:
                logger.info(f"Processing category: {category_name} ({category_id})")

                # Load product IDs for this category - use Playwright as primary method for URLs
                product_ids = await load_product_ids_from_url_async(category_id, self.category_urls)

                # Fallback: Try direct API if Playwright URL loading failed
                if not product_ids:
                    logger.info(f"  URL loading failed for {category_name}, trying direct API...")
                    product_ids = await self._discover_product_ids_from_api_async(category_id)

                # Final fallback: Try Playwright API discovery
                if not product_ids:
                    logger.info(f"  Direct API blocked for {category_name}, trying Playwright API discovery...")
                    product_ids = await self._discover_product_ids_with_playwright_async(category_id)

                if not product_ids:
                    logger.warning(f"No product IDs found for category {category_name} ({category_id}) using any method, skipping")
                    stats['categories_failed'] += 1
                    continue

                products = await self.scrape_category(f"men_{category_name}", category_id, product_ids, stats)

                # Filter out duplicates
                category_products_added = 0
                for product in products:
                    product_url = product.get('product_url')
                    if product_url and product_url not in seen_product_urls:
                        all_products.append(product)
                        seen_product_urls.add(product_url)
                        category_products_added += 1

                stats['products_found'] += category_products_added
                logger.info(f"Added {category_products_added} unique products from category {category_name}")

                # Check product limit for testing
                if PRODUCT_LIMIT > 0 and len(all_products) >= PRODUCT_LIMIT:
                    all_products = all_products[:PRODUCT_LIMIT]
                    logger.info(f"Reached product limit of {PRODUCT_LIMIT}, stopping scraping")
                    break

            except Exception as e:
                logger.error(f"Error processing category {category_name} ({category_id}): {e}")
                stats['categories_failed'] += 1
                continue

        # Scrape women's categories (if we haven't reached the limit)
        if PRODUCT_LIMIT == 0 or len(all_products) < PRODUCT_LIMIT:
            for category_name, category_data in CATEGORY_IDS['women'].items():
                category_id = category_data['category_id']

                products = await self.scrape_category(f"women_{category_name}", category_id, stats=stats)

                # Filter out duplicates
                for product in products:
                    product_url = product.get('product_url')
                    if product_url and product_url not in seen_product_urls:
                        all_products.append(product)
                        seen_product_urls.add(product_url)

                # Check product limit for testing
                if PRODUCT_LIMIT > 0 and len(all_products) >= PRODUCT_LIMIT:
                    all_products = all_products[:PRODUCT_LIMIT]
                    logger.info(f"Reached product limit of {PRODUCT_LIMIT}, stopping scraping")
                    break

        logger.info(f"Total products collected: {len(all_products)}")

        # Process products in batches (generate embeddings)
        logger.info("Generating embeddings...")

        processed_products = []
        for i in range(0, len(all_products), BATCH_SIZE):
            batch = all_products[i:i + BATCH_SIZE]
            try:
                processed_batch = await self.process_products_batch(batch)
                processed_products.extend(processed_batch)
                stats['products_with_embeddings'] += len(processed_batch)

                # Count embedding errors (products without embeddings)
                embedding_errors_in_batch = len(batch) - len(processed_batch)
                stats['embedding_errors'] += embedding_errors_in_batch

            except Exception as e:
                logger.error(f"Error processing batch {i//BATCH_SIZE + 1}: {e}")
                stats['embedding_errors'] += len(batch)
                continue

            # Progress update
            logger.info(f"Processed {len(processed_products)}/{len(all_products)} products with embeddings")

        # Save to database
        logger.info("Saving to database...")
        saved_count = await self.save_to_supabase(processed_products)

        end_time = time.time()
        duration = end_time - start_time

        # Log comprehensive statistics
        logger.info("=" * 60)
        logger.info("SCRAPE COMPLETED - STATISTICS:")
        logger.info(f"Categories processed: {stats['categories_processed']}")
        logger.info(f"Categories failed: {stats['categories_failed']}")
        logger.info(f"Categories skipped: {stats['categories_skipped']}")
        logger.info(f"Total products found: {stats['products_found']}")
        logger.info(f"Products with embeddings: {stats['products_with_embeddings']}")
        logger.info(f"Embedding errors: {stats['embedding_errors']}")
        logger.info(f"Extraction errors: {stats['extraction_errors']}")
        logger.info(f"Products saved to DB: {saved_count}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 60)

        return {
            'total_collected': len(all_products),
            'processed': len(processed_products),
            'saved': saved_count,
            'duration': duration,
            'stats': stats
        }

    async def _discover_product_ids_from_api_async(self, category_id: str) -> List[int]:
        """
        Fetch ALL product IDs from the category endpoint using aiohttp.
        This is similar to the CLI approach but async.
        """
        # Use the same URL format that worked in the CLI
        url = f"https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/category/{category_id}/product?languageId=-15&showProducts=false&priceFilter=true&appId=1"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Referer': 'https://www.pullandbear.com/',
        }

        try:
            import aiohttp
            import asyncio
            await asyncio.sleep(1)  # Rate limiting

            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        product_ids = data.get("productIds", [])
                        if product_ids:
                            logger.info(f"Loaded {len(product_ids)} product IDs from API for category {category_id}")
                            return product_ids
                        else:
                            logger.warning(f"No productIds found in API response for category {category_id}")
                    else:
                        logger.warning(f"API request failed with status {response.status} for category {category_id}")

        except Exception as e:
            logger.error(f"Error fetching product IDs from API for category {category_id}: {e}")

        return []

    async def _discover_product_ids_with_playwright_async(self, category_id: str) -> List[int]:
        """
        Use Playwright to get product IDs by visiting the category page and extracting data.
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not available")
            return []

        url = f"https://www.pullandbear.com/itxrest/3/catalog/store/24009477/20309455/category/{category_id}/product?languageId=-15&showProducts=false&priceFilter=true&appId=1"

        try:
            import asyncio
            await asyncio.sleep(2)  # Rate limiting

            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-accelerated-2d-canvas',
                        '--no-first-run',
                        '--no-zygote',
                        '--single-process',
                        '--disable-gpu'
                    ]
                )
                context = await browser.new_context(
                    user_agent=get_random_user_agent(),
                    viewport={'width': 1280, 'height': 720},
                    locale='en-GB',
                    timezone_id='Europe/London'
                )
                page = await context.new_page()

                try:
                    # Visit main site first to set cookies
                    await page.goto('https://www.pullandbear.com/', wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(2)

                    # Navigate to men's section
                    await page.goto('https://www.pullandbear.com/en/man-c1030148976.html', wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(1)

                    # Make the API request through Playwright with realistic headers
                    headers = get_realistic_headers('https://www.pullandbear.com/en/man-c1030148976.html')
                    response = await page.request.get(url, headers=headers, timeout=30000)
                    if response.status == 200:
                        data = await response.json()
                        product_ids = data.get("productIds", [])
                        if product_ids:
                            logger.info(f"Loaded {len(product_ids)} product IDs with Playwright for category {category_id}")
                            return product_ids
                        else:
                            logger.warning(f"No productIds found in Playwright response for category {category_id}")
                    else:
                        logger.warning(f"Playwright request failed with status {response.status} for category {category_id}")

                finally:
                    await browser.close()

        except Exception as e:
            logger.error(f"Error with Playwright for category {category_id}: {e}")

        return []


async def main():
    """Main entry point."""
    try:
        async with PullBearScraper() as scraper:
            results = await scraper.run_full_scrape()
            print(f"Scrape completed successfully: {results}")

    except KeyboardInterrupt:
        logger.info("Scrape interrupted by user")
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        raise


if __name__ == "__main__":
    # Run the scraper
    asyncio.run(main())
