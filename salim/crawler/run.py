from base import SupermarketCrawler

def main():
    """
    Main function to initialize and run the crawler for a specific shop.
    """
    # --- CHOOSE WHICH CONFIG TO RUN ---
    # Change this value to 'osherad' to run the username-only config.
    config_to_run = 'yohananof' 
    
    try:
        print(f"--- 🚀 Starting Supermarket Crawler for: {config_to_run} ---")
        crawler = SupermarketCrawler(config_name=config_to_run)
        downloaded_files = crawler.crawl()
        
        if downloaded_files:
            print("\n✅ Successfully downloaded:")
            for f in downloaded_files:
                print(f"   - {f}")
        else:
            print("\n⚠️ No files were downloaded.")
            
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: Could not start crawler. {e}")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
        
    print("\n--- 🏁 Crawler finished ---")


if __name__ == "__main__":
    main()
