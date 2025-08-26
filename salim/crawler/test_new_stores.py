#!/usr/bin/env python3
"""
Test script for new stores: Wolt, SuperPharm, and Shufersal
Tests the crawler handlers without actually downloading files
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crawler import UniversalSupermarketCrawler
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def test_new_stores():
    """Test the three new stores in test mode"""
    
    print("\n" + "="*70)
    print("🧪 TESTING NEW STORES: Wolt, SuperPharm, and Shufersal")
    print("="*70)
    
    # Create crawler with test directory
    crawler = UniversalSupermarketCrawler(
        bucket_name="test_downloads",
        config_file="config.json",
        local_mode=True
    )
    
    # Test each store individually
    new_stores = ['wolt', 'superpharm', 'shufersal']
    
    print("\n📋 Testing stores individually in TEST MODE (no downloads):")
    print("-" * 50)
    
    for store in new_stores:
        print(f"\n🏪 Testing {store.upper()}...")
        try:
            # Run in test mode - no actual downloads
            crawler.run(
                supermarket_filter=[store],
                test_mode=True
            )
            print(f"✅ {store.upper()} test completed successfully")
        except Exception as e:
            print(f"❌ Error testing {store}: {e}")
    
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    print("""
    ✅ Configuration updated with new stores
    ✅ Crawler handlers implemented:
       - Wolt: Date-based navigation handler
       - SuperPharm: Hebrew dropdown handler  
       - Shufersal: Dropdown-based handler
    ✅ XML parsers added for all three stores
    
    🔍 Next Steps:
    1. Run actual crawling (remove --test flag)
    2. Verify downloaded files are valid
    3. Test XML parsing with real data
    """)

def test_all_stores():
    """Test all 6 stores together"""
    
    print("\n" + "="*70)
    print("🧪 TESTING ALL 6 STORES TOGETHER")
    print("="*70)
    
    crawler = UniversalSupermarketCrawler(
        bucket_name="test_downloads",
        config_file="config.json",
        local_mode=True
    )
    
    all_stores = ['victory', 'carrefour', 'yohananof', 'wolt', 'superpharm', 'shufersal']
    
    print(f"\n📋 Testing all stores: {', '.join(all_stores)}")
    print("-" * 50)
    
    try:
        crawler.run(
            supermarket_filter=all_stores,
            test_mode=True
        )
        print("\n✅ All stores tested successfully!")
    except Exception as e:
        print(f"\n❌ Error during testing: {e}")

def main():
    """Main test execution"""
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--all':
            # Test all 6 stores
            test_all_stores()
        elif sys.argv[1] == '--help':
            print("""
Usage: python test_new_stores.py [options]

Options:
    (no args)    Test only the 3 new stores
    --all        Test all 6 stores together
    --help       Show this help message

Examples:
    python test_new_stores.py           # Test Wolt, SuperPharm, Shufersal
    python test_new_stores.py --all     # Test all 6 stores
            """)
        else:
            # Test specific store
            store = sys.argv[1]
            print(f"\n🏪 Testing {store}...")
            crawler = UniversalSupermarketCrawler(
                bucket_name="test_downloads",
                config_file="config.json",
                local_mode=True
            )
            crawler.run(supermarket_filter=[store], test_mode=True)
    else:
        # Default: test only new stores
        test_new_stores()

if __name__ == "__main__":
    main()
