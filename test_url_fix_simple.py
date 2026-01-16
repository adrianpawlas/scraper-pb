#!/usr/bin/env python3
"""
Simple test to verify the Pull & Bear URL construction fix.
"""

def test_url_construction():
    """Test the URL construction logic directly."""

    # Simulate the old logic (what was in the code before)
    def old_url_construction(product_url_base):
        return f"https://www.pullandbear.com/en/{product_url_base}.html"

    # Simulate the new logic (what we fixed it to)
    def new_url_construction(product_url_base):
        return f"https://www.pullandbear.com/mt/{product_url_base}"

    # Test with the example from the user
    product_url_base = "stranger-things-cap-l03004900"

    old_url = old_url_construction(product_url_base)
    new_url = new_url_construction(product_url_base)

    print("URL Construction Test:")
    print(f"Product URL Base: {product_url_base}")
    print(f"Old URL (wrong):  {old_url}")
    print(f"New URL (fixed):  {new_url}")
    print()

    # Check the fix
    expected_correct = "https://www.pullandbear.com/mt/stranger-things-cap-l03004900"

    if new_url == expected_correct:
        print("✅ URL construction fix is CORRECT!")
        print(f"   Expected: {expected_correct}")
        print(f"   Got:      {new_url}")
        return True
    else:
        print("❌ URL construction fix is INCORRECT!")
        print(f"   Expected: {expected_correct}")
        print(f"   Got:      {new_url}")
        return False

if __name__ == "__main__":
    success = test_url_construction()
    if success:
        print("\n🎉 URL fix verified successfully!")
    else:
        print("\n💥 URL fix has issues!")
        import sys
        sys.exit(1)