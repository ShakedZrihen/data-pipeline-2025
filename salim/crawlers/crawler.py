import time
from config import SUPERMARKETS
from browser_manager import BrowserManager
from s3_manager import S3Manager
from file_manager import FileManager
from web_scraper import WebScraper
from branch_mapper import BranchMapper


class SupermarketCrawler:
    def __init__(self):
        self.browser_manager = BrowserManager()
        self.driver = self.browser_manager.get_driver()
        self.download_dir = self.browser_manager.get_download_dir()
        
        self.file_manager = FileManager(self.download_dir)
        self.s3_manager = S3Manager()
        self.web_scraper = WebScraper(self.driver, self.file_manager)
        self.branch_mapper = BranchMapper()
        
        self.supermarkets = SUPERMARKETS
    
    def crawl_supermarket_fast(self, supermarket_name):
        """crawl workflow with search-based approach"""
        print(f"\n{'='*60}")
        print(f"CRAWLING: {supermarket_name}")
        print(f"{'='*60}")
        
        try:
            # Login
            if not self.web_scraper.login_to_supermarket(supermarket_name):
                return {'success': False, 'error': 'Login failed'}
            
            # Wait for initial data load
            self.web_scraper.wait_for_processing()
            
            # Search and download PromoFull files immediately
            print("\n--- STEP 1: Search and download PromoFull files ---")
            promo_results = self.web_scraper.search_and_download_files("PromoFull", supermarket_name, 2)
            
            # Search and download PriceFull files immediately
            print("\n--- STEP 2: Search and download PriceFull files ---")
            price_results = self.web_scraper.search_and_download_files("PriceFull", supermarket_name, 2)
            
            if not price_results and not promo_results:
                print(f"No PriceFull or PromoFull files found for {supermarket_name}")
                return {'success': True, 'downloads': {'price_files': [], 'promo_files': []}}
            
            # Organize downloaded files by branch
            print("\n--- STEP 3: Organizing files by branch ---")
            organized_results = self.organize_files_by_branch(price_results, promo_results, supermarket_name)
            
            return organized_results
        
        except Exception as e:
            print(f"ERROR: Crawl failed for {supermarket_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def organize_files_by_branch(self, price_results, promo_results, supermarket_name):
        """Organize downloaded files by branch based on middle number"""
        try:
            print("Organizing files by branch identifier...")
            
            # Collect all downloaded files
            all_files = []
            for file_info in price_results:
                if file_info['success']:
                    all_files.append(file_info['name'])
            for file_info in promo_results:
                if file_info['success']:
                    all_files.append(file_info['name'])
            
            # Group files by branch identifier
            branch_groups = {}
            for filename in all_files:
                branch_id, date_only, full_timestamp = self.file_manager.extract_file_info(filename)
                if branch_id and date_only and full_timestamp:
                    if branch_id not in branch_groups:
                        branch_groups[branch_id] = {'price': None, 'promo': None, 'date': date_only, 'timestamp': full_timestamp}
                    
                    if filename.lower().startswith('pricefull'):
                        branch_groups[branch_id]['price'] = filename
                    elif filename.lower().startswith('promofull'):
                        branch_groups[branch_id]['promo'] = filename
                    
                    print(f"File {filename} -> Branch {branch_id}, Date {date_only}, Timestamp {full_timestamp}")
            
            # Organize files into branch folders
            organized_results = {
                'success': True,
                'downloads': {
                    'branches': {}
                }
            }
            
            branch_keys = list(branch_groups.keys())
            print(f"Found {len(branch_keys)} branch groups: {branch_keys}")
            
            # Smart assignment: prioritize branches with both file types
            complete_branches = []  # Branches with both price and promo files
            incomplete_branches = []  # Branches with only one file type
            
            for branch_id in branch_keys:
                group = branch_groups[branch_id]
                if group['price'] and group['promo']:
                    complete_branches.append(branch_id)
                else:
                    incomplete_branches.append(branch_id)
            
            # Sort complete branches first, then incomplete
            sorted_branches = complete_branches + incomplete_branches
            print(f"Complete branches (both files): {complete_branches}")
            print(f"Incomplete branches (one file): {incomplete_branches}")
            
            # Get dynamic branch names from XML based on actual branch IDs found
            branch_names = []
            for branch_id in sorted_branches:
                xml_branch_name = self.branch_mapper.get_branch_name(supermarket_name, branch_id)
                if xml_branch_name:
                    branch_names.append(xml_branch_name)
                else:
                    # Fallback: use unmapped if no XML mapping found
                    branch_names.append(f"branch_{branch_id}")
            
            # Assign to branch folders
            for i in range(len(sorted_branches)):
                branch_id = sorted_branches[i]
                branch_name = branch_names[i]
                group = branch_groups[branch_id]
                date = group['date']
                timestamp = group['timestamp']
                
                organized_results['downloads']['branches'][branch_name] = {
                    'branch_id': branch_id,
                    'price_file': None,
                    'promo_file': None
                }
                
                # Move and rename PriceFull file
                if group['price']:
                    new_name = f"PriceFull{timestamp}.gz"
                    moved_path = self.file_manager.move_file_to_branch_folder(
                        group['price'], supermarket_name, branch_name, new_name
                    )
                    if moved_path:
                        organized_results['downloads']['branches'][branch_name]['price_file'] = new_name
                        
                        # Upload to S3
                        s3_success = self.s3_manager.upload_to_s3(moved_path, supermarket_name, branch_name, 'price', timestamp)
                        organized_results['downloads']['branches'][branch_name]['price_s3'] = s3_success
                
                # Move and rename PromoFull file
                if group['promo']:
                    new_name = f"PromoFull{timestamp}.gz"
                    moved_path = self.file_manager.move_file_to_branch_folder(
                        group['promo'], supermarket_name, branch_name, new_name
                    )
                    if moved_path:
                        organized_results['downloads']['branches'][branch_name]['promo_file'] = new_name
                        
                        # Upload to S3
                        s3_success = self.s3_manager.upload_to_s3(moved_path, supermarket_name, branch_name, 'promo', timestamp)
                        organized_results['downloads']['branches'][branch_name]['promo_s3'] = s3_success
                
                print(f"Branch {branch_name}: PriceFull{timestamp}.gz + PromoFull{timestamp}.gz")
            
            # Handle unmapped files (files that couldn't be assigned to any branch)
            unmapped_files = []
            assigned_branch_ids = set(sorted_branches)
            
            for branch_id in branch_keys:
                if branch_id not in assigned_branch_ids:
                    group = branch_groups[branch_id]
                    if group['price']:
                        unmapped_files.append(group['price'])
                    if group['promo']:
                        unmapped_files.append(group['promo'])
            
            if unmapped_files:
                print(f"Moving {len(unmapped_files)} unmapped files to 'unmapped' folder")
                organized_results['downloads']['unmapped'] = []
                
                for filename in unmapped_files:
                    # Extract info for proper naming
                    branch_id, date_only, full_timestamp = self.file_manager.extract_file_info(filename)
                    
                    if filename.lower().startswith('pricefull'):
                        new_name = f"PriceFull{full_timestamp}.gz"
                        file_type = 'price'
                    else:
                        new_name = f"PromoFull{full_timestamp}.gz"
                        file_type = 'promo'
                    
                    # Move to unmapped folder
                    moved_path = self.file_manager.move_file_to_branch_folder(
                        filename, supermarket_name, "unmapped", new_name
                    )
                    
                    if moved_path:
                        # Upload to S3 in unmapped folder
                        s3_success = self.s3_manager.upload_to_s3(moved_path, supermarket_name, "unmapped", file_type, full_timestamp)
                        
                        organized_results['downloads']['unmapped'].append({
                            'original_filename': filename,
                            'new_filename': new_name,
                            'branch_id': branch_id,
                            's3_success': s3_success
                        })
                        
                        print(f"Unmapped: {filename} -> unmapped/{new_name} (Branch ID: {branch_id})")

            return organized_results

        except Exception as e:
            print(f"ERROR: File organization failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def close(self):
        """Close browser and cleanup"""
        self.browser_manager.close()


def run_crawl_cycle():
    """Run a single crawl cycle for all supermarkets"""
    crawler = SupermarketCrawler()
    
    try:
        print("Starting supermarket crawler cycle...")

        all_results = {}
        
        for supermarket_name in crawler.supermarkets.keys():
            result = crawler.crawl_supermarket_fast(supermarket_name)
            all_results[supermarket_name] = result
            time.sleep(1)  # Minimal delay between supermarkets
        
        # Print summary
        print(f"\n{'='*60}")
        print("FINAL SUMMARY (Organized by Branch + S3 Upload)")
        print(f"{'='*60}")
        
        for supermarket, result in all_results.items():
            print(f"\n{supermarket}:")
            if result['success'] and 'branches' in result['downloads']:
                branches = result['downloads']['branches']
                for branch_name, branch_info in branches.items():
                    print(f"  {branch_name} (Branch ID: {branch_info['branch_id']}):")
                    if branch_info['price_file']:
                        s3_status = "[S3-OK]" if branch_info.get('price_s3', False) else "[S3-FAIL]"
                        print(f"    SUCCESS {s3_status}: {branch_info['price_file']}")
                    if branch_info['promo_file']:
                        s3_status = "[S3-OK]" if branch_info.get('promo_s3', False) else "[S3-FAIL]"
                        print(f"    SUCCESS {s3_status}: {branch_info['promo_file']}")
                
                # Show unmapped files if any
                if 'unmapped' in result['downloads'] and result['downloads']['unmapped']:
                    print(f"  unmapped folder:")
                    for unmapped_file in result['downloads']['unmapped']:
                        s3_status = "[S3-OK]" if unmapped_file.get('s3_success', False) else "[S3-FAIL]"
                        print(f"    SUCCESS {s3_status}: {unmapped_file['new_filename']} (Branch ID: {unmapped_file['branch_id']})")
            else:
                print(f"  ERROR: {result.get('error', 'Unknown error')}")
        
        return all_results
        
    except Exception as e:
        print(f"Crawl cycle error: {e}")
        return None
    finally:
        crawler.close()


def main():
    """Main function - runs crawler continuously every minute"""
    print("Starting continuous supermarket crawler...")
    print("Press Ctrl+C to stop")
    
    cycle_count = 1
    
    try:
        while True:
            print(f"\n{'='*80}")
            print(f"STARTING CRAWL CYCLE #{cycle_count}")
            print(f"{'='*80}")
            
            cycle_start_time = time.time()
            
            # Run the crawl cycle
            results = run_crawl_cycle()
            
            cycle_end_time = time.time()
            cycle_duration = cycle_end_time - cycle_start_time
            
            print(f"\n{'='*80}")
            print(f"CYCLE #{cycle_count} COMPLETED in {cycle_duration:.1f} seconds")
            print(f"{'='*80}")
            
            # Wait for 1 hour before next cycle
            print("Waiting 3600 seconds (1 hour) before next cycle...")
            time.sleep(3600)
            
            cycle_count += 1
            
    except KeyboardInterrupt:
        print(f"\n\nCrawler stopped by user after {cycle_count - 1} cycles")
        print("Goodbye!")
    except Exception as e:
        print(f"\nUnexpected error in main loop: {e}")
        print("Crawler stopped")


if __name__ == "__main__":
    main()