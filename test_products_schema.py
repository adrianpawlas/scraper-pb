#!/usr/bin/env python3
"""Quick test that product row shape and transform match the Supabase products table schema."""

import os
import sys

# Test transform produces correct keys and types (no network)
def test_transform():
    from transform import to_supabase_row

    raw = {
        "external_id": "test-123",
        "source": "scraper",
        "title": "Test Product",
        "description": "A test",
        "brand": "Bershka",
        "price": 29.99,
        "currency": "EUR",
        "sale": 19.99,
        "image_url": "https://example.com/img.jpg",
        "product_url": "https://example.com/product-123",
        "gender": "MAN",
        "category": "Sweaters & Hoodies",
        "additional_images": ["https://a.com/1.jpg", "https://a.com/2.jpg"],
    }
    row = to_supabase_row(raw)

    assert "id" in row and row["id"] == "test-123"
    assert row["source"] == "scraper"
    assert row["brand"] == "Bershka"
    assert row["gender"] == "man"
    assert row["category"] == "Sweaters, Hoodies"
    assert row["additional_images"] == "https://a.com/1.jpg , https://a.com/2.jpg"
    assert row["second_hand"] is False
    assert "price" in row and isinstance(row["price"], str) and "EUR" in row["price"]
    assert "sale" in row
    assert "currency" not in row, "products table has no currency column"
    assert "metadata" in row and (row["metadata"] is None or isinstance(row["metadata"], str))
    print("  transform.to_supabase_row: OK")


# Test text embedding (loads model, can be slow)
def test_text_embedding():
    from embeddings import get_text_embedding

    emb = get_text_embedding("Test product title and description.")
    assert emb is not None, "get_text_embedding should return a list"
    assert len(emb) == 768, f"expected 768-dim, got {len(emb)}"
    print("  get_text_embedding (768-dim): OK")


def main():
    print("Testing products schema alignment...")
    test_transform()
    if "--embedding" in sys.argv:
        try:
            test_text_embedding()
        except Exception as e:
            print(f"  get_text_embedding failed: {e}")
            raise
    else:
        print("  get_text_embedding: skipped (run with --embedding to test)")
    print("Done.")


if __name__ == "__main__":
    main()
    sys.exit(0)
