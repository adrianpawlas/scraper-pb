#!/usr/bin/env python3
"""
Production runner for the Bershka scraper.
Run this to scrape all Bershka products and store them in Supabase.
"""

import asyncio
import sys
from bershka_scraper import BershkaScraper


async def main():
    """Run the complete Bershka scraping pipeline."""
    try:
        async with BershkaScraper() as scraper:
            print("🚀 Starting Bershka product scraper...")
            results = await scraper.run_full_scrape()

            print("\n✅ Scrape completed successfully!")
            print("📊 Results:")
            print(f"   • Total products collected: {results['total_collected']}")
            print(f"   • Products with embeddings: {results['processed']}")
            print(f"   • Products saved to database: {results['saved']}")
            print(f"   • Duration: {results['duration']:.2f} seconds")
            return results

    except KeyboardInterrupt:
        print("\n⏹️  Scraper interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Scraper failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
