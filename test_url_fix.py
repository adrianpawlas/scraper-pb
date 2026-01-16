#!/usr/bin/env python3
"""
Simple test to verify the Pull & Bear URL fix.
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pull_bear_scraper import PullBearScraper

def test_url_construction():
    """Test that URLs are constructed correctly."""

    # Create a scraper instance (we'll mock what we need)
    scraper = PullBearScraper()

    # Mock variant data similar to what the API returns
    mock_variant = {
        'productUrl': 'stranger-things-cap-l03004900',
        'detail': {
            'colors': [{
                'reference': 'L03004900',
                'displayReference': 'L03004900',
                'id': 12345,
                'sizes': [{'name': 'S', 'isBuyable': True, 'price': '2990'}],
                'name': 'Black'
            }]
        },
        'sectionNameEN': 'MAN',
        'nameEn': 'Stranger Things Cap',
        'detail': {
            'longDescription': 'Cool cap',
            'colors': [{
                'reference': 'L03004900',
                'displayReference': 'L03004900',
                'id': 12345,
                'sizes': [{'name': 'S', 'isBuyable': True, 'price': '2990'}],
                'name': 'Black'
            }]
        }
    }

    mock_bundle_product = {
        'id': 12345,
        'nameEn': 'Stranger Things Cap',
        'bundleColors': [],
        'relatedCategories': [{'name': 'Accessories', 'id': '123'}],
        'tags': [],
        'attributes': []
    }

    # Test the URL construction by calling the method
    try:
        products = scraper._extract_single_product(mock_bundle_product, mock_variant, mock_variant['detail']['colors'][0])

        if products:
            product_url = products[0]['product_url']
            print(f"Generated URL: {product_url}")

            # Check if the URL is correct
            expected_url = "https://www.pullandbear.com/mt/stranger-things-cap-l03004900"

            if product_url == expected_url:
                print("✅ URL construction is CORRECT!")
                print(f"   Expected: {expected_url}")
                print(f"   Got:      {product_url}")
                return True
            else:
                print("❌ URL construction is INCORRECT!")
                print(f"   Expected: {expected_url}")
                print(f"   Got:      {product_url}")
                return False
        else:
            print("❌ No product extracted")
            return False

    except Exception as e:
        print(f"❌ Error testing URL construction: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_url_construction()
    if success:
        print("\n🎉 URL fix verified successfully!")
    else:
        print("\n💥 URL fix has issues!")
        sys.exit(1)