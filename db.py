import json
from typing import Dict, List
import requests


class SupabaseREST:
    """Minimal Supabase PostgREST helper for upserting into 'products' table.

    This helper uses the primary key 'id' for idempotent upserts.
    """

    def __init__(self, url: str, key: str) -> None:
        self.base_url = url.rstrip("/")
        self.key = key
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        })

    def upsert_products(self, products: List[Dict]) -> None:
        """Upsert a list of product dicts into the 'products' table using primary key 'id'."""
        if not products:
            return

        # Deduplicate by 'id' within this batch to avoid conflicts
        seen: Dict[str, Dict] = {}
        for p in products:
            key = p.get('id')
            if key:
                seen[key] = p
        products = list(seen.values())

        # Normalize all products to have the same keys (Supabase requirement)
        all_keys = set()
        for p in products:
            all_keys.update(p.keys())

        normalized_products = []
        for p in products:
            normalized = {key: p.get(key) for key in all_keys}
            normalized_products.append(normalized)

        endpoint = f"{self.base_url}/rest/v1/products"
        headers = {
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }

        # Chunk inserts to keep requests reasonable (metadata can be large)
        chunk_size = 100
        for i in range(0, len(normalized_products), chunk_size):
            chunk = normalized_products[i:i + chunk_size]
            resp = self.session.post(endpoint, headers=headers, data=json.dumps(chunk), timeout=60)
            if resp.status_code not in (200, 201, 204):
                raise RuntimeError(f"Supabase upsert failed: {resp.status_code} {resp.text}")

    def delete_missing_for_source_merchant_country(self, source: str, merchant_name: str, country: str, current_ids: List[str]) -> None:
        """Delete products for a given source not present in current_ids."""
        if current_ids is None:
            current_ids = []

        # Fetch existing IDs scoped by source only
        url = f"{self.base_url}/rest/v1/products?source=eq.{source}&select=id"
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        all_ids = [r.get("id") for r in resp.json() if r.get("id") is not None]
        to_delete = [eid for eid in all_ids if eid not in current_ids]

        chunk_size = 300
        for j in range(0, len(to_delete), chunk_size):
            chunk_del = to_delete[j:j + chunk_size]
            for eid in chunk_del:
                del_url = f"{self.base_url}/rest/v1/products?source=eq.{source}&id=eq.{eid}"
                del_resp = self.session.delete(del_url, timeout=60)
                if del_resp.status_code not in (200, 204):
                    raise RuntimeError(f"Supabase delete failed: {del_resp.status_code} {del_resp.text}")
