from typing import List, Optional
try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    from .http_client import PoliteSession
except ImportError:
    from http_client import PoliteSession


def scrape_category_for_product_ids(session: PoliteSession, category_url: str, headers: Optional[dict] = None) -> List[str]:
    """Scrape a category page to find product IDs."""
    try:
        resp = session.get(category_url, headers=headers)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Look for product links that contain product IDs
        product_ids = []

        # Find links with product URLs
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Look for Bershka product URLs
            if '/p/' in href and ('bershka.com' in href or href.startswith('/')):
                # Extract product ID from URL like /p/123456789.html
                if '/p/' in href:
                    try:
                        # Extract the number after /p/
                        parts = href.split('/p/')
                        if len(parts) > 1:
                            product_part = parts[1].split('.')[0]
                            if product_part.isdigit():
                                product_ids.append(product_part)
                    except:
                        continue

        # Remove duplicates and return
        return list(set(product_ids))

    except Exception as e:
        print(f"Error scraping category {category_url}: {e}")
        return []


def discover_product_ids_for_categories(session: PoliteSession, category_urls: List[str], headers: Optional[dict] = None) -> dict:
    """Discover product IDs for multiple categories."""
    category_product_ids = {}

    for category_url in category_urls:
        print(f"Discovering product IDs for category: {category_url}")
        product_ids = scrape_category_for_product_ids(session, category_url, headers)

        # Use the category ID as key
        if 'categoryId=' in category_url:
            category_id = category_url.split('categoryId=')[1].split('&')[0]
            category_product_ids[category_id] = product_ids
            print(f"Found {len(product_ids)} product IDs for category {category_id}")

    return category_product_ids
