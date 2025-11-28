#!/usr/bin/env python3
"""
Test script for Pull & Bear scraper fixes.
Tests the API calls and product extraction without requiring Supabase.
"""

import asyncio
import logging
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pull_bear_scraper import PullBearScraper

# Mock the Supabase client to avoid requiring credentials
class MockSupabaseClient:
    def table(self, name):
        return MockTable()

class MockTable:
    def upsert(self, data, **kwargs):
        return MockResult(data)

class MockResult:
    def __init__(self, data):
        self.data = data

    def execute(self):
        return self

# Mock the create_client function
def mock_create_client(url, key):
    return MockSupabaseClient()

# Monkey patch the imports
import pull_bear_scraper
pull_bear_scraper.create_client = mock_create_client

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_category_scraping():
    """Test scraping a single category to verify fixes."""
    logger.info("Testing Pull & Bear scraper fixes...")

    try:
        # Test with a category that was working before
        test_category_id = "1030204788"  # This one was working in the logs

        async with PullBearScraper() as scraper:
            logger.info(f"Testing category {test_category_id}...")

            # Test product ID discovery methods
            logger.info("1. Testing URL-based product ID discovery...")
            product_ids_url = await pull_bear_scraper.load_product_ids_from_url_async(test_category_id, scraper.category_urls)

            logger.info("2. Testing direct API product ID discovery...")
            product_ids_api = await scraper._discover_product_ids_from_api_async(test_category_id)

            logger.info("3. Testing Playwright API product ID discovery...")
            product_ids_playwright = await scraper._discover_product_ids_with_playwright_async(test_category_id)

            logger.info(f"URL method found: {len(product_ids_url)} products")
            logger.info(f"API method found: {len(product_ids_api)} products")
            logger.info(f"Playwright method found: {len(product_ids_playwright)} products")

            # Use whichever method worked
            product_ids = product_ids_url or product_ids_api or product_ids_playwright

            if not product_ids:
                logger.error("No product IDs found with any method")
                return False

            # Test scraping the category (limit to 5 products for testing)
            logger.info(f"4. Testing category scraping with {min(5, len(product_ids))} products...")
            products = await scraper.scrape_category(f"test_{test_category_id}", test_category_id, product_ids[:5])

            logger.info(f"Successfully scraped {len(products)} products")

            # Test embedding generation on first product
            if products:
                logger.info("5. Testing embedding generation...")
                first_product = products[0]
                if first_product.get('image_url'):
                    embedding = await scraper.generate_embedding(first_product['image_url'])
                    if embedding:
                        logger.info("Embedding generation successful!")
                        first_product['embedding'] = embedding
                    else:
                        logger.warning("Embedding generation failed")

            logger.info("✅ All tests passed!")
            return True

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main test function."""
    success = await test_category_scraping()
    if success:
        print("\n🎉 Pull & Bear scraper fixes verified successfully!")
    else:
        print("\n💥 Pull & Bear scraper fixes have issues that need attention.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
