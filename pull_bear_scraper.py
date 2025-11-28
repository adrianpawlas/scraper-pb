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


def load_product_ids_from_url(category_id: str, urls: Dict[str, str]) -> List[int]:
    """
    Load product IDs from a URL if available in the urls dict.
    Uses Playwright to avoid API blocking.
    """
    if category_id not in urls:
        return []

    url = urls[category_id]

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not available, cannot load from URL")
        return []

    try:
        # Add delay to avoid rate limiting
        import time
        time.sleep(2)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
            )
            page = context.new_page()

            # Visit main site first to set cookies
            page.goto('https://www.pullandbear.com/', wait_until='domcontentloaded')
            time.sleep(1)

            # Make the API request
            response = page.request.get(url)
            if response.status == 200:
                data = response.json()
                product_ids = data.get("productIds", [])
                if product_ids:
                    logger.info(f"Loaded {len(product_ids)} product IDs from URL for category {category_id}")
                    browser.close()
                    return product_ids
                else:
                    logger.warning(f"No productIds found in response for category {category_id}")
            else:
                logger.warning(f"Playwright request failed with status {response.status} for category {category_id}")

            browser.close()

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
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-GB,en;q=0.9',
            }
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
        """Fetch a batch of products from the Pull & Bear API."""
        url = self.build_api_url(category_id, product_ids, page)

        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    products = data.get('products', [])
                    logger.info(f"API returned {len(products)} products for category {category_id} (page {page or 0})")
                    return data
                else:
                    logger.warning(f"API request failed: {response.status} - {url}")
                    return {"products": []}
        except Exception as e:
            logger.error(f"Error fetching products: {e} - {url}")
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
            product_id = f"{color['reference']}-{color['id']}"
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
        """Generate 768-dim embedding for an image URL."""
        try:
            # Download image
            async with self.session.get(image_url) as response:
                if response.status != 200:
                    return None

                image_data = await response.read()

            # Process image in thread pool
            loop = asyncio.get_event_loop()
            embedding = await loop.run_in_executor(
                self.executor,
                self._process_image_embedding,
                image_data
            )

            return embedding

        except Exception as e:
            logger.error(f"Error generating embedding for {image_url}: {e}")
            return None

    def _process_image_embedding(self, image_data: bytes) -> Optional[List[float]]:
        """Process image and generate embedding (runs in thread pool)."""
        try:
            # Open image
            image = Image.open(io.BytesIO(image_data)).convert('RGB')

            # Verify image is valid
            image.verify()  # This will raise an exception if the image is corrupted
            image.close()

            # Re-open the image after verification
            image = Image.open(io.BytesIO(image_data)).convert('RGB')

            # Process with SigLIP - need both image and text inputs
            inputs = self.processor(
                images=image,
                text=[""],  # Empty text input
                return_tensors="pt",
                padding=True
            )

            with torch.no_grad():
                outputs = self.model(**inputs)

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

    async def scrape_category(self, category_name: str, category_id: int, product_ids: List[int] = None) -> List[Dict[str, Any]]:
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
                        product_data = self.extract_product_info(product)
                        all_products.extend(product_data)

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

        # Scrape all categories
        all_products = []
        seen_product_urls = set()  # Track unique product URLs to avoid duplicates

        # Scrape men's categories - get ALL products from each category
        for category_name, category_data in CATEGORY_IDS['men'].items():
            category_id = category_data['category_id']

            # Load product IDs for this category from URLs only
            product_ids = load_product_ids_from_url(category_id, self.category_urls)

            if not product_ids:
                logger.warning(f"No product IDs found for category {category_name} ({category_id}) from URL, skipping")
                continue

            products = await self.scrape_category(f"men_{category_name}", category_id, product_ids)

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

        # Scrape women's categories (if we haven't reached the limit)
        if PRODUCT_LIMIT == 0 or len(all_products) < PRODUCT_LIMIT:
            for category_name, category_data in CATEGORY_IDS['women'].items():
                category_id = category_data['category_id']

                products = await self.scrape_category(f"women_{category_name}", category_id)

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
            processed_batch = await self.process_products_batch(batch)
            processed_products.extend(processed_batch)

            # Progress update
            logger.info(f"Processed {len(processed_products)}/{len(all_products)} products")

        # Save to database
        logger.info("Saving to database...")
        saved_count = await self.save_to_supabase(processed_products)

        end_time = time.time()
        duration = end_time - start_time

        logger.info(
            f"Scrape completed! "
            f"Total products: {len(all_products)}, "
            f"Processed with embeddings: {len(processed_products)}, "
            f"Saved to DB: {saved_count}, "
            f"Duration: {duration:.2f} seconds"
        )

        return {
            'total_collected': len(all_products),
            'processed': len(processed_products),
            'saved': saved_count,
            'duration': duration
        }


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
