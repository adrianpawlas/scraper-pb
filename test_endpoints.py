import requests

# Try to find an endpoint that returns product IDs for a category
base_url = 'https://www.bershka.com/itxrest/3/catalog/store/45009578/40259549'

test_urls = [
    f'{base_url}/category/1010834564',
    f'{base_url}/category/1010834564/products',
    f'{base_url}/category/1010834564/productIds',
    f'{base_url}/products?categoryId=1010834564',
    f'{base_url}/productsArray?categoryId=1010834564&page=1',
    f'{base_url}/productsArray?categoryId=1010834564&showAll=true',
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-GB,en;q=0.9',
}

for url in test_urls:
    try:
        print(f'Testing: {url}')
        resp = requests.get(url, headers=headers, timeout=10)
        print(f'Status: {resp.status_code}')
        if resp.status_code == 200:
            content_type = resp.headers.get('content-type', 'unknown')
            print(f'Content-Type: {content_type}')
            if 'application/json' in content_type:
                try:
                    data = resp.json()
                    print(f'Response keys: {list(data.keys())}')
                    products = data.get('products', [])
                    if products:
                        print(f'Products: {len(products)}')
                        print(f'First product: {products[0].get("id") if products else "None"}')
                except Exception as e:
                    print(f'JSON parse error: {e}')
            else:
                print(f'Response length: {len(resp.text)} chars')
        print('---')
    except Exception as e:
        print(f'Error: {e}')
        print('---')
