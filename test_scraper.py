#!/usr/bin/env python3
"""
Test script for Bershka scraper components.
"""

import asyncio
import json
from bershka_scraper import BershkaScraper


async def test_api_connection():
    """Test basic API connection and data extraction."""
    print("Testing Bershka API connection...")

    async with BershkaScraper() as scraper:
        # Test with the men's category URL provided
        category_id = 1010834564  # Men's category
        product_ids = [199850199, 200346172, 200711323]  # Sample product IDs

        print(f"Fetching products from category {category_id}...")
        data = await scraper.fetch_products_batch(category_id, product_ids)

        if data.get('products'):
            print(f"Successfully fetched {len(data['products'])} products")

            # Debug: Print the structure of the first product
            if data['products']:
                first_product = data['products'][0]
                print(f"\nFirst product keys: {list(first_product.keys())}")
                print(f"Product type: {first_product.get('type')}")
                print(f"Has bundleProductSummaries: {'bundleProductSummaries' in first_product}")
                if 'bundleProductSummaries' in first_product:
                    print(f"Number of bundle summaries: {len(first_product['bundleProductSummaries'])}")

            # Test product extraction
            for product in data['products'][:1]:  # Test first product
                print(f"\nTesting product extraction for: {product.get('name', 'Unknown')}")
                print(f"Product has bundleProductSummaries: {bool(product.get('bundleProductSummaries'))}")

                if product.get('bundleProductSummaries'):
                    for i, variant in enumerate(product['bundleProductSummaries']):
                        print(f"Variant {i} keys: {list(variant.keys())}")
                        if 'detail' in variant:
                            print(f"Variant {i} detail keys: {list(variant['detail'].keys())}")
                            colors = variant['detail'].get('colors', [])
                            print(f"Variant {i} has {len(colors)} colors")
                            if colors:
                                print(f"First color keys: {list(colors[0].keys())}")

                extracted_products = scraper.extract_product_info(product)

                print(f"Extracted {len(extracted_products)} product variants")

                if extracted_products:
                    sample_product = extracted_products[0]
                    print("Sample product data:")
                    print(json.dumps({
                        'id': sample_product['id'],
                        'title': sample_product['title'],
                        'price': sample_product['price'],
                        'currency': sample_product['currency'],
                        'gender': sample_product['gender'],
                        'category': sample_product['category'],
                        'image_url': sample_product['image_url'][:100] + "..." if sample_product['image_url'] else None,
                        'sizes': sample_product['size']
                    }, indent=2))

                    # Test embedding generation
                    if sample_product['image_url']:
                        print("\nTesting embedding generation...")
                        embedding = await scraper.generate_embedding(sample_product['image_url'])
                        if embedding:
                            print(f"Generated embedding with {len(embedding)} dimensions")
                            print(f"First 5 values: {embedding[:5]}")
                        else:
                            print("Failed to generate embedding")
        else:
            print("No products found in API response")


async def test_category_scraping():
    """Test scraping a small category."""
    print("\nTesting category scraping...")

    async with BershkaScraper() as scraper:
        # Test with men's category using sample product IDs from the provided URL
        category_id = 1010834564  # Men's category
        sample_product_ids = [199850199, 200346172, 200711323, 203971838, 201304218, 204544074, 205222885, 204203891, 204203890, 203677704, 202812583, 202680979, 202411683, 202238171, 201967673, 201927866, 201129538, 201096327, 201096326, 201096315]

        products = await scraper.scrape_category("men_all", category_id, sample_product_ids)

        print(f"Found {len(products)} products in men's category")

        if products:
            # Show sample products
            for product in products[:3]:
                print(f"- {product['title']} ({product['gender']}) - {product['price']} {product['currency']}")
        else:
            print("No products extracted. Check the data structure or API response.")


async def main():
    """Run all tests."""
    print("Starting Bershka scraper tests...\n")

    try:
        await test_api_connection()
        await test_category_scraping()

        print("\n[SUCCESS] All tests completed successfully!")

    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
