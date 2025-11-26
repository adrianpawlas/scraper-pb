import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
import re
import json
import os

try:
    from .config import load_env, get_supabase_env
    from .http_client import PoliteSession
    from .db import SupabaseREST
    from .api_ingestor import ingest_api
    from .transform import to_supabase_row
    from .embeddings import get_image_embedding
except ImportError:
    # Fallback for direct execution
    from config import load_env, get_supabase_env
    from http_client import PoliteSession
    from db import SupabaseREST
    from api_ingestor import ingest_api
    from transform import to_supabase_row
    from embeddings import get_image_embedding


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
        print(f"  {filename} not found, will use API fallback")
    except Exception as e:
        print(f"  Error loading {filename}: {e}")

    return urls


def load_product_ids_from_url(category_id: str, urls: Dict[str, str], headers: Dict[str, str]) -> List[int]:
    """
    Load product IDs from a URL if available in the urls dict.
    """
    if category_id not in urls:
        return []

    url = urls[category_id]
    try:
        import requests
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            product_ids = data.get("productIds", [])
            if product_ids:
                print(f"  Loaded {len(product_ids)} product IDs from URL for category {category_id}")
                return product_ids
        else:
            print(f"  URL request failed with status {response.status_code} for category {category_id}")
    except Exception as e:
        print(f"  Error loading from URL for category {category_id}: {e}")

    return []


def load_product_ids_from_file(category_id: str, data_dir: str = "category_data") -> List[int]:
    """
    Load product IDs from a local JSON file if available.
    Files should be named: {category_id}.json or category_{category_id}.json
    The JSON should have a 'productIds' array.
    """
    possible_files = [
        os.path.join(data_dir, f"{category_id}.json"),
        os.path.join(data_dir, f"category_{category_id}.json"),
        f"{category_id}.json",
        f"category_{category_id}.json",
    ]

    for filepath in possible_files:
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    product_ids = data.get("productIds", [])
                    if product_ids:
                        print(f"  Loaded {len(product_ids)} product IDs from {filepath}")
                        return product_ids
            except Exception as e:
                print(f"  Error loading {filepath}: {e}")

    return []


def discover_product_ids_with_playwright(category_id: str, category_ids_url_template: str, debug: bool = False) -> List[int]:
    """
    Use Playwright to get product IDs by:
    1. First visiting the main site to get cookies
    2. Then making the API request with those cookies
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        if debug:
            print("  Playwright not available")
        return []
    
    product_ids = []
    captured_ids = []
    
    def handle_response(response):
        """Capture product IDs from API responses."""
        nonlocal captured_ids
        if "category" in response.url and "product" in response.url:
            try:
                data = response.json()
                ids = data.get("productIds", [])
                if ids:
                    captured_ids.extend(ids)
                    print(f"  [Playwright] Captured {len(ids)} product IDs from API")
            except:
                pass
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-GB'
            )
            page = context.new_page()
            
            # Listen for API responses
            page.on("response", handle_response)
            
            # First visit the main site to get cookies
            print(f"  [Playwright] Visiting main site for cookies...")
            page.goto("https://www.bershka.com/us/", wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(2000)
            
            # Now try to make the API request directly using the browser context
            api_url = category_ids_url_template.format(category_id=category_id)
            print(f"  [Playwright] Fetching API: {api_url[:60]}...")
            
            try:
                response = page.goto(api_url, wait_until='domcontentloaded', timeout=30000)
                if response and response.status == 200:
                    try:
                        data = response.json()
                        product_ids = data.get("productIds", [])
                        if product_ids:
                            print(f"  [Playwright] Got {len(product_ids)} product IDs from direct API call")
                    except:
                        pass
            except Exception as e:
                if debug:
                    print(f"  [Playwright] Direct API failed: {e}")
            
            # If direct API didn't work, try loading the category page
            if not product_ids and not captured_ids:
                # Determine correct URL based on category
                if category_id == "1010834564":
                    cat_url = f"https://www.bershka.com/us/men/clothes/view-all-c{category_id}.html"
                else:
                    cat_url = f"https://www.bershka.com/us/women/clothes/view-all-c{category_id}.html"
                
                print(f"  [Playwright] Loading category page: {cat_url[:60]}...")
                page.goto(cat_url, wait_until='networkidle', timeout=60000)
                page.wait_for_timeout(5000)
                
                # Scroll to trigger lazy loading
                for _ in range(3):
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    page.wait_for_timeout(1000)
                
                # Check if we captured any IDs from network
                if captured_ids:
                    product_ids = list(set(captured_ids))
                else:
                    # Try to extract from page content
                    content = page.content()
                    matches = re.findall(r'"productIds"\s*:\s*\[([\d,\s]+)\]', content)
                    if matches:
                        for match in matches:
                            ids = [int(x.strip()) for x in match.split(',') if x.strip().isdigit()]
                            product_ids.extend(ids)
                        product_ids = list(set(product_ids))
                        print(f"  [Playwright] Found {len(product_ids)} product IDs from page content")
            
            browser.close()
            
    except Exception as e:
        print(f"  [Playwright] Error: {e}")
    
    return product_ids or captured_ids


def discover_product_ids_from_api(session: PoliteSession, category_id: str, category_ids_url_template: str, headers: Dict[str, str], debug: bool = False) -> List[int]:
    """
    Fetch ALL product IDs from the category endpoint.
    This is the key step - the category/product endpoint returns ALL product IDs for a category.
    For example, men's category returns 888+ product IDs.
    """
    
    # Build the URL using the template
    url = category_ids_url_template.format(category_id=category_id)
    
    if debug:
        print(f"  Fetching product IDs from: {url[:80]}...")
    
    try:
        resp = session.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            product_ids = data.get("productIds", [])
            if product_ids:
                print(f"  Found {len(product_ids)} product IDs from category API")
                return product_ids
            else:
                if debug:
                    print(f"  No productIds in response. Keys: {list(data.keys())}")
        else:
            print(f"  API returned status {resp.status_code}")
            if debug and resp.status_code == 403:
                print("  API is blocked (403 Forbidden)")
    except Exception as e:
        print(f"  Error fetching product IDs: {str(e)[:80]}")
    
    return []


def run_for_site(site: Dict, session: PoliteSession, db: SupabaseREST, supa_env: Dict[str, str], limit: int = 0) -> int:
    """
    Scrape products for a single site using the two-step approach:
    1. Fetch ALL product IDs from category/product endpoint
    2. Fetch product details in batches using productsArray endpoint
    """
    brand = site.get("brand", "Unknown")
    merchant = site.get("merchant", brand)
    source = site.get("source", "scraper")
    debug = bool(site.get("debug"))

    collected: List[Dict] = []
    seen_product_ids: Set[int] = set()

    if site.get("api"):
        api_conf = site["api"]
        headers = api_conf.get("headers", {})

        # Prewarm cookies/session
        for warm_url in api_conf.get("prewarm", []):
            try:
                session.get(warm_url, headers=headers)
            except Exception:
                pass

        # Get URL templates
        category_ids_url_template = api_conf.get("category_ids_url")
        products_url = api_conf.get("products_url")
        product_url_template = api_conf.get("product_url_template")
        batch_size = api_conf.get("batch_size", 50)
        
        if not category_ids_url_template:
            print("Error: Missing category_ids_url in config")
            return 0
        if not products_url:
            print("Error: Missing products_url in config")
            return 0

        category_endpoints = api_conf.get("category_endpoints", [])
        total_products_found = 0
        
        for cat_conf in category_endpoints:
            category_id = cat_conf.get("id")
            category_name = cat_conf.get("name", category_id)
            category_gender = cat_conf.get("gender")
            category_type = cat_conf.get("category")
            
            if not category_id:
                continue
            
            print(f"\nProcessing category: {category_name} ({category_id})")

            # Step 0: Load category URLs from file
            category_urls = load_category_urls()

            # Step 1: Check for URL first (only source now)
            product_ids = load_product_ids_from_url(category_id, category_urls, headers)

            # Step 2: If URL failed, try API
            if not product_ids:
                product_ids = discover_product_ids_from_api(
                    session, category_id, category_ids_url_template, headers, debug
                )

            # Step 3: If API failed, try Playwright
            if not product_ids:
                print("  API blocked, trying Playwright...")
                product_ids = discover_product_ids_with_playwright(category_id, category_ids_url_template, debug)
            
            if not product_ids:
                print(f"  No products found, skipping")
                continue
            
            total_products_found += len(product_ids)
            
            # Filter duplicates (products may appear in multiple categories)
            new_product_ids = [pid for pid in product_ids if pid not in seen_product_ids]
            seen_product_ids.update(product_ids)
            
            if len(new_product_ids) < len(product_ids):
                print(f"  {len(product_ids) - len(new_product_ids)} duplicates filtered, {len(new_product_ids)} new products")
            
            if not new_product_ids:
                continue
            
            # Step 3: Fetch products in batches
            for i in range(0, len(new_product_ids), batch_size):
                batch_ids = new_product_ids[i:i + batch_size]
                batch_ids_str = ",".join(str(pid) for pid in batch_ids)
                
                batch_url = products_url.format(
                    category_id=category_id,
                    product_ids=batch_ids_str
                )
                
                print(f"  Batch {i//batch_size + 1}/{(len(new_product_ids) + batch_size - 1)//batch_size}: fetching {len(batch_ids)} products...")
                
                try:
                    batch_products = ingest_api(
                        session,
                        batch_url,
                        api_conf["items_path"],
                        api_conf["field_map"],
                        {"headers": headers},
                        debug=False,
                    )
                    
                    print(f"    Got {len(batch_products)} products from API")
                    
                    for p in batch_products:
                        p["merchant"] = merchant
                        p["source"] = source
                        p["gender"] = category_gender
                        
                        if category_type:
                            p["category"] = category_type
                        
                        if site.get("country"):
                            p["country"] = site.get("country")

                        if not p.get("external_id"):
                            p["external_id"] = p.get("product_id") or p.get("id")

                        product_id = p.get("external_id") or p.get("product_id")
                        title = p.get("title", "product")
                        slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
                        p["product_url"] = product_url_template.format(
                            slug=slug,
                            product_id=product_id
                        )

                        row = to_supabase_row(p)

                        image_url = row.get("image_url")
                        if not image_url or not isinstance(image_url, str):
                            continue
                        
                        # Skip video files
                        if any(ext in image_url.lower() for ext in ['.mp4', '.m3u8', '.webm', 'video']):
                            if debug:
                                print(f"    [SKIP] Video file: {image_url[:50]}...")
                            continue
                        
                        # Skip incomplete Bershka URLs
                        if 'bershka' in image_url.lower() and 'assets/public' not in image_url:
                            continue

                        emb = get_image_embedding(image_url)
                        if emb is not None:
                            row["embedding"] = emb
                            collected.append(row)
                        
                        if limit and len(collected) >= limit:
                            print(f"\n  Reached limit of {limit} products")
                            break
                    
                    if limit and len(collected) >= limit:
                        break
                        
                except Exception as e:
                    print(f"  Error fetching batch: {e}")
                    continue
            
            if limit and len(collected) >= limit:
                break
            
            print(f"  Category done. Total collected so far: {len(collected)} products with embeddings")
        
        print(f"\n{'='*50}")
        print(f"Total products discovered across all categories: {total_products_found}")
        print(f"Unique products after deduplication: {len(seen_product_ids)}")

    if collected:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] {brand}: processed {len(collected)} products with embeddings")
        if supa_env["url"] and supa_env["key"]:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {brand}: upserting to database...")
            
            upsert_batch_size = 50
            success_count = 0
            for i in range(0, len(collected), upsert_batch_size):
                batch = collected[i:i + upsert_batch_size]
                try:
                    db.upsert_products(batch)
                    success_count += len(batch)
                    print(f"  Upserted batch {i//upsert_batch_size + 1}: {len(batch)} products (total: {success_count})")
                except Exception as e:
                    print(f"  Error upserting batch: {e}")
                    for row in batch:
                        try:
                            db.upsert_products([row])
                            success_count += 1
                        except Exception as e2:
                            print(f"    Failed to insert product {row.get('id')}: {str(e2)[:80]}")
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {brand}: database operations completed ({success_count} products)")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {brand}: skipping database upsert (credentials not set)")

    return len(collected)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bershka fashion scraper")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of products (for testing)")

    args = parser.parse_args()

    load_env()
    supa_env = get_supabase_env()
    db = SupabaseREST(url=supa_env["url"], key=supa_env["key"])

    from config import load_sites_config
    sites = load_sites_config("sites.yaml")

    if not sites:
        print("Error: No sites configured in sites.yaml")
        return

    session = PoliteSession(default_headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en;q=0.9',
    }, respect_robots=False)

    total = 0
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting Bershka scraper...")

    start_time = datetime.now()
    site_count = run_for_site(sites[0], session, db, supa_env, limit=args.limit)
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Bershka: imported {site_count} products ({duration:.1f}s)")
    total += site_count

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Total: imported {total} products")


if __name__ == "__main__":
    main()
