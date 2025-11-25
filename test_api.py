import requests
import json

# Test different API variations
base_url = 'https://www.bershka.com/itxrest/3/catalog/store/45009578/40259549'

# Try different endpoints
test_urls = [
    f'{base_url}/category/1010834564/products?ajax=true',
    f'{base_url}/category/1010834564/products',
    f'{base_url}/productsArray?categoryId=1010834564&ajax=true',
    f'{base_url}/products?categoryId=1010834564&ajax=true'
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
            try:
                data = resp.json()
                products = data.get('products', [])
                print(f'Products found: {len(products)}')
                if products:
                    print(f'First product ID: {products[0].get("id")}')
                    break
            except:
                print('Not JSON response')
        print('---')
    except Exception as e:
        print(f'Error: {e}')
        print('---')
