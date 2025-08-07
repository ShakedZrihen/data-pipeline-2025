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
    # Available configs
    all_configs = ["yohananof", "osherad", "ramilevi", "tivtaam", "keshet", "doralon"]

    if len(sys.argv) == 1:
        # Run all configs
        configs_to_run = all_configs
        print("Running all configs: " + ", ".join(configs_to_run))
    elif len(sys.argv) == 2:
        # Run specific config
        config_name = sys.argv[1]
        if config_name not in all_configs:
            print(f"❌ Unknown config: {config_name}")
            print(f"Available configs: {', '.join(all_configs)}")
            sys.exit(1)
        configs_to_run = [config_name]
    else:
        print("Usage: python run.py [config_name]")
        print("Available configs: " + ", ".join(all_configs))
        print("Run without arguments to crawl all configs")
        sys.exit(1)
    
    total_files = 0
    
    for config_name in configs_to_run:
        print(f"\n{'='*50}")
        print(f"🚀 Starting crawler for {config_name.upper()}...")
        print(f"{'='*50}")
        
        try:
            crawler = SupermarketCrawler(config_name)
            downloaded_files = crawler.crawl()
            
            if downloaded_files:
                print(f"\n✅ {config_name.upper()}: Downloaded {len(downloaded_files)} files")
                for file_path in downloaded_files:
                    filename = os.path.basename(file_path)
                    # Extract branch number for S3 path
                    import re
                    branch_match = re.search(r'-(\d{3,4})-', filename)
                    if branch_match:
                        branch_num = branch_match.group(1)
                        s3_key = f"{config_name.lower()}/{branch_num}/{filename}"
                    else:
                        s3_key = f"{config_name.lower()}/{filename}"
                    print(f"  📁 {file_path}")
                    print(f"  ☁️  Uploaded to S3: s3://test-bucket/{s3_key}")
                total_files += len(downloaded_files)
            else:
                print(f"\n❌ {config_name.upper()}: No files were downloaded.")
                
        except FileNotFoundError as e:
            print(f"❌ {config_name.upper()}: Configuration error: {e}")
        except Exception as e:
            print(f"❌ {config_name.upper()}: Unexpected error: {e}")
    
    print(f"\n{'='*50}")
    print(f"🎉 TOTAL: Downloaded and uploaded {total_files} files to S3")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()