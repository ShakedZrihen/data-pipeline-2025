#!/usr/bin/env python3
"""
Runner for the supermarket crawler.
Works on Windows & Mac.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base import SupermarketCrawler

def main():
    if len(sys.argv) != 2:
        print("Usage: python run.py <config_name>")
        print("Available configs: yohananof, osherad, ramilevi, tivtaam")
        sys.exit(1)
    
    config_name = sys.argv[1]
    
    try:
        print(f"Starting crawler for {config_name}...")
        crawler = SupermarketCrawler(config_name)
        downloaded_files = crawler.crawl()
        
        if downloaded_files:
            print(f"\n✅ Successfully downloaded {len(downloaded_files)} files:")
            for file_path in downloaded_files:
                print(f"  - {file_path}")
        else:
            print("\n❌ No files were downloaded.")
            
    except FileNotFoundError as e:
        print(f"❌ Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()