#!/usr/bin/env python3
"""
Test script to run the crawler locally and save files for pipeline testing
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from crawler import UniversalSupermarketCrawler

def main():
    """
    Test the crawler locally
    Save files to local directory instead of S3
    """
    # Create local test directory
    test_dir = Path("test_downloads")
    test_dir.mkdir(exist_ok=True)
    
    print("=" * 60)
    print("CRAWLER LOCAL TEST MODE")
    print("=" * 60)
    print(f"Files will be saved to: {test_dir.absolute()}")
    print()
    
    # Initialize crawler in local mode
    crawler = UniversalSupermarketCrawler(
        bucket_name=str(test_dir),
        config_file="config.json",
        local_mode=True  # This enables local file saving
    )
    
    # Test specific supermarkets or all
    if len(sys.argv) > 1:
        # Test specific supermarkets
        supermarket_filter = sys.argv[1].split(',')
        print(f"Testing supermarkets: {supermarket_filter}")
    else:
        # Test all configured supermarkets in test mode first
        supermarket_filter = None
        print("Testing all configured supermarkets")
    
    # Run in test mode first to see what's available
    print("\n" + "=" * 60)
    print("PHASE 1: Test Mode (no downloads)")
    print("=" * 60)
    crawler.run(supermarket_filter=supermarket_filter, test_mode=True)
    
    # Ask user if they want to proceed with actual downloads
    print("\n" + "=" * 60)
    response = input("Do you want to proceed with actual downloads? (y/n): ")
    
    if response.lower() == 'y':
        print("\n" + "=" * 60)
        print("PHASE 2: Downloading Files")
        print("=" * 60)
        
        # Create new crawler instance for actual download
        crawler = UniversalSupermarketCrawler(
            bucket_name=str(test_dir),
            config_file="config.json",
            local_mode=True
        )
        
        # Download files
        crawler.run(supermarket_filter=supermarket_filter, test_mode=False)
        
        # Show downloaded files
        print("\n" + "=" * 60)
        print("Downloaded Files:")
        print("=" * 60)
        
        providers_dir = test_dir / "providers"
        if providers_dir.exists():
            for provider_dir in providers_dir.iterdir():
                if provider_dir.is_dir():
                    print(f"\n{provider_dir.name}:")
                    for branch_dir in provider_dir.iterdir():
                        if branch_dir.is_dir():
                            print(f"  {branch_dir.name}:")
                            for file in branch_dir.glob("*.gz"):
                                size_kb = file.stat().st_size / 1024
                                print(f"    - {file.name} ({size_kb:.1f} KB)")
    else:
        print("Download cancelled.")

if __name__ == "__main__":
    main()
