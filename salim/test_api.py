#!/usr/bin/env python3
"""
Simple script to test all API endpoints after route refactoring
"""

import requests
import json

BASE_URL = "http://localhost:8000"

def test_endpoint(endpoint, description):
    """Test an API endpoint and print the result"""
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", timeout=5)
        status = "✅ PASS" if response.status_code == 200 else f"❌ FAIL ({response.status_code})"
        print(f"{status} - {description}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ ERROR - {description}: {e}")
        return False

def main():
    print("🧪 Testing Salim API Endpoints After Route Refactoring\n")
    
    tests = [
        ("/", "Root endpoint"),
        ("/health", "Health check"),
        ("/supermarkets", "Get all supermarkets"),
        ("/supermarkets/1", "Get specific supermarket"),
        ("/supermarkets/1/products?limit=5", "Get supermarket products"),
        ("/products?search=milk&limit=5", "Search products"),
        ("/products/1", "Get specific product"),
        ("/products/barcode/7290000000001", "Get products by barcode"),
        ("/compare/barcode/7290000000001", "Compare prices by barcode"),
        ("/compare/product?search=milk", "Compare product prices"),
        ("/categories", "Get categories"),
        ("/brands", "Get brands"),
        ("/stats", "Get statistics"),
    ]
    
    passed = 0
    total = len(tests)
    
    for endpoint, description in tests:
        if test_endpoint(endpoint, description):
            passed += 1
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All endpoints working correctly!")
    else:
        print("⚠️ Some endpoints failed - check logs")

if __name__ == "__main__":
    main()