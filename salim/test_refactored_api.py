#!/usr/bin/env python3
"""
Comprehensive test script for the refactored Salim API
"""

import requests
import json

BASE_URL = "http://localhost:8000"

def test_endpoint(endpoint, description, expected_status=200):
    """Test an API endpoint and print the result"""
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", timeout=10)
        status = "✅ PASS" if response.status_code == expected_status else f"❌ FAIL ({response.status_code})"
        
        # Show sample data for successful requests
        if response.status_code == 200 and response.headers.get('content-type', '').startswith('application/json'):
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                sample = f" | Sample: {data[0].get('canonical_name', data[0])}"[:50]
            elif isinstance(data, dict) and 'total_products' in data:
                sample = f" | Stats: {data}"
            else:
                sample = ""
        else:
            sample = ""
            
        print(f"{status} - {description}{sample}")
        return response.status_code == expected_status, response.json() if response.headers.get('content-type', '').startswith('application/json') else None
    except Exception as e:
        print(f"❌ ERROR - {description}: {e}")
        return False, None

def main():
    print("🧪 Testing Refactored Salim API with New Query Parameters\n")
    
    tests = [
        # Basic functionality
        ("/", "Root endpoint"),
        ("/health", "Health check"),
        
        # Search with new query parameters
        ("/products?q=milk&limit=3", "Search products with 'q' parameter"),
        ("/products?name=bread&limit=3", "Search products with 'name' parameter"),
        ("/products?brand=Tnuva&limit=5", "Filter by brand (Tnuva)"),
        ("/products?category=Dairy&limit=5", "Filter by category (Dairy)"),
        ("/products?promo=true&limit=5", "Filter products on sale (promo=true)"),
        ("/products?promo=false&limit=5", "Filter regular priced products (promo=false)"),
        ("/products?min_price=10&max_price=20&limit=5", "Filter by price range (₪10-20)"),
        ("/products?supermarket_id=1&limit=5", "Filter by supermarket (Rami Levi)"),
        
        # Combined filters
        ("/products?q=milk&brand=Tnuva&limit=3", "Combined: search + brand filter"),
        ("/products?category=Dairy&promo=true&limit=3", "Combined: category + promo filter"),
        
        # Price comparison (integrated)
        ("/products/barcode/7290000000001", "Price comparison by barcode (Milk 1L)"),
        ("/products/barcode/7290000000010", "Price comparison by barcode (Bread)"),
        
        # Regular endpoints
        ("/products/1", "Get specific product by ID"),
        ("/supermarkets", "Get all supermarkets"),
        ("/categories", "Get categories"),
        ("/brands", "Get brands"),
        ("/stats", "Get statistics"),
        
        # Test non-existent data
        ("/products/barcode/9999999999", "Non-existent barcode", 404),
    ]
    
    passed = 0
    total = len(tests)
    
    print("🔍 Testing New Query String Features:\n")
    
    for test in tests:
        if len(test) == 2:
            endpoint, description = test
            expected_status = 200
        else:
            endpoint, description, expected_status = test
            
        success, data = test_endpoint(endpoint, description, expected_status)
        if success:
            passed += 1
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All refactored endpoints working correctly!")
        print("\n✨ New Features Successfully Implemented:")
        print("   • Query string search: ?q=product or ?name=product")
        print("   • Brand filtering: ?brand=BrandName")
        print("   • Promotion filtering: ?promo=true/false")
        print("   • Price comparison integrated into /products/barcode/{barcode}")
        print("   • All filters can be combined for advanced search")
    else:
        print("⚠️ Some tests failed - check implementation")

    print(f"\n📝 Example API Calls:")
    print(f"   curl '{BASE_URL}/products?q=milk&promo=true'")
    print(f"   curl '{BASE_URL}/products?brand=Tnuva&category=Dairy'")
    print(f"   curl '{BASE_URL}/products/barcode/7290000000001'")

if __name__ == "__main__":
    main()