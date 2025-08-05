import os
from base import SupermarketCrawler

def main():
    """
    Main function to find all configs in the 'configs' folder
    and run the crawler for each one sequentially.
    """
    configs_dir = 'configs'

    # 1. Check if the configuration directory exists.
    if not os.path.isdir(configs_dir):
        print(f"❌ Error: The '{configs_dir}' directory was not found.")
        print("Please make sure you are running this script from the correct location.")
        return

    # 2. Find all JSON files in the directory.
    config_files = [f for f in os.listdir(configs_dir) if f.endswith('.json')]

    if not config_files:
        print(f"🤷 No configuration files (.json) found in the '{configs_dir}' directory.")
        return

    print(f"✅ Found {len(config_files)} configurations to run: {', '.join([os.path.splitext(f)[0] for f in config_files])}")
    
    # 3. Loop through each configuration file and run the crawler.
    for config_file in config_files:
        # Get the name of the config without the '.json' extension
        config_name = os.path.splitext(config_file)[0]
        
        # Use a try...except block for each crawler to make the script more resilient.
        # If one crawler fails, the script will log the error and move to the next one.
        try:
            print(f"\n{'='*60}")
            print(f"--- 🚀 Starting Supermarket Crawler for: {config_name} ---")
            
            # Initialize and run the crawler for the current configuration.
            crawler = SupermarketCrawler(config_name=config_name)
            crawler.crawl()
            
            print(f"--- ✅ Crawler for {config_name} finished. ---")
            
        except FileNotFoundError as e:
            print(f"\n❌ ERROR: Could not start crawler for {config_name}. {e}")
        except Exception as e:
            print(f"\n❌ An unexpected error occurred while running the crawler for {config_name}: {e}")
        
    print(f"\n{'='*60}")
    print("\n--- 🏁 All crawler jobs have been processed. ---")


if __name__ == "__main__":
    main()
