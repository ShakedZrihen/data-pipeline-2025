"""
Test script to verify database connection and SQLAlchemy models work correctly.
Run this before proceeding to API endpoints.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database import SessionLocal, test_connection, get_table_counts
from app.models import Product, Branch, Price
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload


def test_database_connection():
    """Test basic database connectivity."""
    print("\n" + "="*60)
    print("🔍 TESTING DATABASE CONNECTION")
    print("="*60)
    
    if test_connection():
        counts = get_table_counts()
        print(f"\n📊 Table row counts:")
        for table, count in counts.items():
            print(f"  - {table}: {count} rows")
        return True
    return False


def test_product_queries():
    """Test querying products table."""
    print("\n" + "="*60)
    print("🏷️ TESTING PRODUCT QUERIES")
    print("="*60)
    
    try:
        with SessionLocal() as session:
            # Get first 5 products
            products = session.query(Product).limit(5).all()
            print(f"\n✅ Found {len(products)} products")
            for p in products:
                print(f"  - {p.product_name} | Brand: {p.brand_name} | Barcode: {p.barcode}")
            
            # Test Hebrew search
            hebrew_search = "חלב"
            hebrew_products = session.query(Product).filter(
                Product.product_name.ilike(f"%{hebrew_search}%")
            ).limit(3).all()
            print(f"\n🔍 Hebrew search for '{hebrew_search}':")
            for p in hebrew_products:
                print(f"  - {p.product_name} ({p.brand_name})")
            
            return True
    except Exception as e:
        print(f"❌ Error querying products: {e}")
        return False


def test_branch_queries():
    """Test querying branches table."""
    print("\n" + "="*60)
    print("🏪 TESTING BRANCH QUERIES")
    print("="*60)
    
    try:
        with SessionLocal() as session:
            # Get unique providers
            providers = session.query(Branch.provider, func.count(Branch.branch_id)).group_by(Branch.provider).all()
            print(f"\n✅ Found {len(providers)} supermarket chains:")
            for provider, count in providers:
                print(f"  - {provider}: {count} branches")
            
            # Get sample branches
            branches = session.query(Branch).limit(5).all()
            print(f"\n📍 Sample branches:")
            for b in branches:
                print(f"  - {b.provider} - {b.name} | {b.city}")
            
            return True
    except Exception as e:
        print(f"❌ Error querying branches: {e}")
        return False


def test_price_queries():
    """Test querying prices with relationships."""
    print("\n" + "="*60)
    print("💰 TESTING PRICE QUERIES WITH RELATIONSHIPS")
    print("="*60)
    
    try:
        with SessionLocal() as session:
            # Get recent prices with product and branch info
            recent_prices = session.query(Price)\
                .options(selectinload(Price.product), selectinload(Price.branch))\
                .order_by(Price.ts.desc())\
                .limit(5)\
                .all()
            
            print(f"\n✅ Found {len(recent_prices)} recent prices:")
            for price in recent_prices:
                discount = f" (Discount: ₪{price.discount_price})" if price.discount_price else ""
                print(f"  - {price.product.product_name} @ {price.branch.provider}")
                print(f"    Price: ₪{price.price}{discount} | Final: ₪{price.final_price}")
            
            # Test finding prices for a specific barcode
            sample_product = session.query(Product).filter(Product.barcode.isnot(None)).first()
            if sample_product:
                prices_for_barcode = session.query(Price)\
                    .join(Product)\
                    .join(Branch)\
                    .filter(Product.barcode == sample_product.barcode)\
                    .all()
                
                print(f"\n🔍 Prices for barcode {sample_product.barcode} ({sample_product.product_name}):")
                for price in prices_for_barcode[:3]:
                    print(f"  - {price.branch.provider}: ₪{price.final_price}")
            
            return True
    except Exception as e:
        print(f"❌ Error querying prices: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_complex_query():
    """Test a complex query joining all tables."""
    print("\n" + "="*60)
    print("🔗 TESTING COMPLEX QUERIES")
    print("="*60)
    
    try:
        with SessionLocal() as session:
            # Find products with promotions (discount_price is not null)
            promo_products = session.query(
                Product.product_name,
                Product.brand_name,
                Branch.provider,
                Price.price,
                Price.discount_price,
                Price.final_price
            ).select_from(Price)\
             .join(Product)\
             .join(Branch)\
             .filter(Price.discount_price.isnot(None))\
             .limit(5)\
             .all()
            
            print(f"\n🎯 Products with promotions:")
            for item in promo_products:
                savings = float(item.price - item.discount_price) if item.discount_price else 0
                print(f"  - {item.product_name} @ {item.provider}")
                print(f"    Regular: ₪{item.price} | Promo: ₪{item.discount_price} | Savings: ₪{savings:.2f}")
            
            return True
    except Exception as e:
        print(f"❌ Error with complex query: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "🚀 STARTING DATABASE AND MODEL TESTS " + "🚀")
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Product Queries", test_product_queries),
        ("Branch Queries", test_branch_queries),
        ("Price Queries", test_price_queries),
        ("Complex Queries", test_complex_query)
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
        print("\n🎉 All tests passed! Ready to proceed with API endpoints.")
    else:
        print("\n⚠️ Some tests failed. Please fix issues before proceeding.")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
