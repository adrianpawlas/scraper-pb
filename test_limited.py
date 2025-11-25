#!/usr/bin/env python3
"""
Limited test of the Bershka scraper (10 products max).
"""

import os
import asyncio

# Set environment variables for limited testing
os.environ['PRODUCT_LIMIT'] = '10'

from bershka_scraper import BershkaScraper


async def main():
    """Run limited test of the Bershka scraper."""
    print("Running limited Bershka scraper test (10 products max)...")

    try:
        async with BershkaScraper() as scraper:
            results = await scraper.run_full_scrape()

            print("\n[SUCCESS] Limited test completed successfully!")
            print("Results:")
            print(f"   • Total products collected: {results['total_collected']}")
            print(f"   • Products with embeddings: {results['processed']}")
            print(f"   • Products saved to database: {results['saved']}")
            print(f"   • Duration: {results['duration']:.2f} seconds")
            return results

    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"\nTest failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
