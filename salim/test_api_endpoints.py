"""
Test script for FastAPI endpoints.
Tests each endpoint with real data to ensure everything works.
"""

import requests
import json
from datetime import datetime
from typing import Dict, Any


BASE_URL = "http://localhost:8000/api/v1"


def print_response(response: requests.Response, endpoint: str):
    """Pretty print API response."""
    print(f"\n📍 Endpoint: {endpoint}")
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if isinstance(data, list):
            print(f"   Results: {len(data)} items")
            if data and len(data) > 0:
                print(f"   Sample:")
                # Pretty print first item
                print(json.dumps(data[0], indent=4, ensure_ascii=False, default=str))
        else:
            print(json.dumps(data, indent=4, ensure_ascii=False, default=str))
    else:
        print(f"   Error: {response.text}")


def test_health():
    """Test health check endpoint."""
    print("\n" + "="*60)
    print("🏥 TESTING HEALTH CHECK")
    print("="*60)
    
    response = requests.get(f"{BASE_URL}/health/")
    print_response(response, "GET /health/")
    return response.status_code == 200


def test_supermarkets():
    """Test supermarkets endpoints."""
    print("\n" + "="*60)
    print("🏪 TESTING SUPERMARKETS ENDPOINTS")
    print("="*60)
    
    # Test 1: Get all supermarkets
    print("\n1️⃣ Get all supermarkets:")
    response = requests.get(f"{BASE_URL}/supermarkets/")
    print_response(response, "GET /supermarkets/")
    
    if response.status_code != 200:
        return False
    
    supermarkets = response.json()
    if not supermarkets:
        print("   ⚠️ No supermarkets found")
        return False
    
    # Test 2: Get specific supermarket details
    first_provider = supermarkets[0]['provider']
    print(f"\n2️⃣ Get details for '{first_provider}':")
    response = requests.get(f"{BASE_URL}/supermarkets/{first_provider}")
    print_response(response, f"GET /supermarkets/{first_provider}")
    
    # Test 3: Get products from supermarket
    print(f"\n3️⃣ Get products from '{first_provider}' (limit 5):")
    response = requests.get(f"{BASE_URL}/supermarkets/{first_provider}/products?limit=5")
    print_response(response, f"GET /supermarkets/{first_provider}/products")
    
    # Test 4: Search for products with Hebrew
    print(f"\n4️⃣ Search for 'חלב' in '{first_provider}':")
    response = requests.get(f"{BASE_URL}/supermarkets/{first_provider}/products?search=חלב&limit=3")
    print_response(response, f"GET /supermarkets/{first_provider}/products?search=חלב")
    
    return True


def test_products():
    """Test products endpoints."""
    print("\n" + "="*60)
    print("📦 TESTING PRODUCTS ENDPOINTS")
    print("="*60)
    
    # Test 1: Get products with various filters
    print("\n1️⃣ Get all products (limit 5):")
    response = requests.get(f"{BASE_URL}/products?limit=5")
    print_response(response, "GET /products?limit=5")
    
    # Test 2: Search for products by name
    print("\n2️⃣ Search for products containing 'חלב':")
    response = requests.get(f"{BASE_URL}/products?name=חלב&limit=3")
    print_response(response, "GET /products?name=חלב")
    
    # Test 3: Filter by price range
    print("\n3️⃣ Get products between 10-20 ILS:")
    response = requests.get(f"{BASE_URL}/products?min_price=10&max_price=20&limit=3")
    print_response(response, "GET /products?min_price=10&max_price=20")
    
    # Test 4: Get products with discounts
    print("\n4️⃣ Get products with discounts:")
    response = requests.get(f"{BASE_URL}/products?has_discount=true&limit=3")
    print_response(response, "GET /products?has_discount=true")
    
    return response.status_code == 200


def test_barcode():
    """Test barcode price comparison endpoint."""
    print("\n" + "="*60)
    print("🔍 TESTING BARCODE PRICE COMPARISON")
    print("="*60)
    
    # First, get a product with a barcode
    response = requests.get(f"{BASE_URL}/products?limit=50")
    if response.status_code != 200:
        print("   ❌ Failed to get products")
        return False
    
    products = response.json()
    barcode_product = None
    for product in products:
        if product.get('barcode') and product['barcode'] != 'None':
            barcode_product = product
            break
    
    if not barcode_product:
        print("   ⚠️ No products with barcodes found")
        return False
    
    barcode = barcode_product['barcode']
    print(f"\n1️⃣ Compare prices for barcode '{barcode}':")
    print(f"   Product: {barcode_product.get('product_name', 'Unknown')}")
    
    response = requests.get(f"{BASE_URL}/products/barcode/{barcode}")
    print_response(response, f"GET /products/barcode/{barcode}")
    
    return response.status_code == 200


def main():
    """Run all API tests."""
    print("\n🚀 STARTING API ENDPOINT TESTS 🚀")
    print(f"Testing against: {BASE_URL}")
    
    # Check if API is running
    try:
        response = requests.get(f"{BASE_URL}/health/", timeout=5)
    except requests.exceptions.ConnectionError:
        print("\n❌ API is not running! Please start the FastAPI server first.")
        print("   Run: uvicorn app.main:app --reload")
        return False
    
    tests = [
        ("Health Check", test_health),
        ("Supermarkets", test_supermarkets),
        ("Products", test_products),
        ("Barcode Comparison", test_barcode),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"\n❌ Test '{test_name}' failed with error: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {test_name}: {status}")
    
    all_passed = all(success for _, success in results)
    if all_passed:
        print("\n🎉 All API tests passed!")
    else:
        print("\n⚠️ Some API tests failed.")
    
    return all_passed


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
