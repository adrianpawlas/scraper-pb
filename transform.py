import json
import re
from typing import Any, Dict, List, Optional

# No external imports needed for this module


def _format_price(price_val: Any, currency: Any) -> Optional[str]:
    """Format price as text e.g. '29.99EUR' or '20.90USD,450CZK'. Accepts already-formatted string or value+currency."""
    if price_val is None:
        return None
    if isinstance(price_val, str) and price_val.strip():
        # Already multi-currency or "29.99EUR" style
        s = price_val.strip()
        if re.match(r"^[\d.,]+\s*[A-Z]{3}", s) or "," in s:
            return s
        # Might be just number
        cur = str(currency).strip().upper() if currency else "EUR"
        return f"{s}{cur}" if cur else s
    cur = str(currency).strip().upper() if currency else "EUR"
    try:
        if isinstance(price_val, (int, float)):
            if isinstance(price_val, int) and price_val >= 1000:
                price_val = price_val / 100.0
            return f"{float(price_val):.2f}{cur}"
    except Exception:
        pass
    return str(price_val) + (cur if cur else "")


def _normalize_availability(raw_availability: Any) -> str:
    """Normalize availability to one of: 'in_stock', 'out_of_stock', 'unknown'."""
    if isinstance(raw_availability, bool):
        return "in_stock" if raw_availability else "out_of_stock"
    if raw_availability is None:
        return "unknown"
    text = str(raw_availability).strip().lower()
    mapping = {
        "in_stock": "in_stock",
        "instock": "in_stock",
        "in stock": "in_stock",
        "available": "in_stock",
        "out_of_stock": "out_of_stock",
        "out-of-stock": "out_of_stock",
        "outofstock": "out_of_stock",
        "sold_out": "out_of_stock",
        "sold-out": "out_of_stock",
        "sold out": "out_of_stock",
        "unavailable": "out_of_stock",
        "coming_soon": "unknown",
        "coming-soon": "unknown",
        "preorder": "unknown",
        "pre-order": "unknown",
    }
    return mapping.get(text, "unknown")


def to_supabase_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a generic scraped product to your Supabase products schema.

    Expected minimal input keys (from API/HTML):
    - source: str (e.g., 'manual', 'api', 'awin')
    - external_id: str (stable per merchant) - will be used as the 'id' field
    - merchant_name: str
    - merchant_id: str|int (optional)
    - title: str
    - description: str (optional)
    - brand: str (optional)
    - price: float|str (optional)
    - currency: str (e.g., 'GBP')
    - image_url: str
    - product_url: str
    - affiliate_url: str (optional)

    All other columns are left null unless provided.
    """

    row: Dict[str, Any] = {}

    # Use external_id as the primary key 'id'
    # Use external_id as the primary key 'id'
    external_id = raw.get("external_id") or raw.get("product_id")
    row["id"] = str(external_id) if external_id else str(raw.get("product_url", "unknown"))
    row["source"] = raw.get("source") or "scraper"
    row["title"] = raw.get("title") or "Unknown title"
    row["description"] = raw.get("description")
    row["brand"] = raw.get("brand") or "Bershka"
    # Price: text format e.g. "20.90USD,450CZK" (multi-currency) or "29.99EUR"
    row["price"] = _format_price(raw.get("price"), raw.get("currency"))
    row["sale"] = _format_price(raw.get("sale"), raw.get("sale_currency")) or raw.get("sale")  # same format as price when on sale
    image_url = raw.get("image_url")
    if image_url:
        # Handle relative URLs
        if image_url.startswith('/'):
            image_url = f"https://static.bershka.net{image_url}"
        # Handle protocol-relative URLs
        elif image_url.startswith('//'):
            image_url = f"https:{image_url}"
    row["image_url"] = image_url
    
    # Product URL should be unique - use the one generated in cli.py or construct from ID
    product_url = raw.get("product_url")
    if not product_url and external_id:
        # Generate a unique product URL if not provided
        title = raw.get("title", "product")
        slug = re.sub(r'[^a-z0-9]+', title.lower(), '-').strip('-')
        product_url = f"https://www.bershka.com/us/{slug}-c0p{external_id}.html"
    row["product_url"] = product_url
    row["affiliate_url"] = raw.get("affiliate_url")
    row["country"] = raw.get("country")

    # Additional images: comma+space separated "url1 , url2"
    add_imgs = raw.get("additional_images")
    if isinstance(add_imgs, list):
        row["additional_images"] = " , ".join(str(u).strip() for u in add_imgs if u)
    elif isinstance(add_imgs, str) and add_imgs.strip():
        row["additional_images"] = add_imgs.strip()
    else:
        row["additional_images"] = None

    # Set second_hand to FALSE for all current brands (they are not second-hand marketplaces)
    row["second_hand"] = False

    # Gender: lowercase "man" / "woman" (per products table convention)
    raw_gender = raw.get("gender")
    if raw_gender:
        gender_str = str(raw_gender).strip().upper()
        if gender_str in ("MAN", "MEN", "MALE", "GUY", "BOY") or any(w in gender_str for w in ["MEN", "MAN", "MALE"]):
            row["gender"] = "man"
        elif gender_str in ("WOMAN", "WOMEN", "FEMALE", "LADY", "GIRL") or any(w in gender_str for w in ["WOMEN", "WOMAN", "FEMALE"]):
            row["gender"] = "woman"
        else:
            row["gender"] = gender_str.lower() if gender_str else None
    else:
        row["gender"] = None

    # Category: comma-separated e.g. "Sweaters" or "Sweaters, Hoodies"
    cat = raw.get("category")
    if isinstance(cat, list):
        row["category"] = ", ".join(str(c).strip() for c in cat if c) or None
    elif isinstance(cat, str) and cat.strip():
        # "Sweaters & Hoodies" -> "Sweaters, Hoodies"
        row["category"] = re.sub(r"\s*&\s*", ", ", cat.strip())
    else:
        row["category"] = None

    # Normalize sizes: accept str, list[str], or nested lists → text (comma-separated)
    size_val = raw.get("size") or raw.get("sizes")
    try:
        if isinstance(size_val, list):
            flat_sizes: List[str] = []
            for s in size_val:
                if isinstance(s, list):
                    for t in s:
                        if isinstance(t, str) and t.strip():
                            flat_sizes.append(t.strip())
                elif isinstance(s, str) and s.strip():
                    flat_sizes.append(s.strip())
            row["size"] = ", ".join(dict.fromkeys(flat_sizes)) if flat_sizes else None
        elif isinstance(size_val, str):
            row["size"] = size_val.strip() or None
    except Exception:
        pass

    # Price/sale already set as text by _format_price; no numeric normalization (table expects text)

    # Build metadata json: include base info, plus site/source-specific _meta and useful raw fields
    try:
        # Start with a minimal base so metadata is never empty
        meta: Dict[str, Any] = {}
        for k in ("source", "id"):
            v = row.get(k)
            if v not in (None, ""):
                meta[k] = v
        if isinstance(raw.get("_meta"), dict):
            meta.update(raw["_meta"])  # type: ignore[arg-type]
        # include helpful raw context when present
        for k in ("_raw_item", "_raw_html_len"):
            if raw.get(k) is not None:
                meta[k] = raw.get(k)
        # attach original price/currency fields pre-normalization when available
        if raw.get("price") is not None and "original_price" not in meta:
            meta["original_price"] = raw.get("price")
        if raw.get("currency") is not None and "original_currency" not in meta:
            meta["original_currency"] = raw.get("currency")
        if raw.get("sale") is not None and "sale" not in meta:
            meta["sale"] = raw.get("sale")
        row["metadata"] = json.dumps(meta) if meta else None
    except Exception:
        pass

    return row
