#!/usr/bin/env python3
import os
from dotenv import load_dotenv
load_dotenv()

import sys
import gzip
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
import requests
from urllib.parse import urljoin, urlparse
import boto3


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class UniversalSupermarketCrawler:
    def __init__(self, bucket_name, config_file=None):
        self.bucket = bucket_name
        if config_file and Path(config_file).exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        else:
            self.config = self.get_default_config()
        self.download_dir = Path.cwd() / "downloads"
        self.download_dir.mkdir(exist_ok=True)
        self.s3 = boto3.client("s3")
        self.date_str = datetime.now().strftime("%Y-%m-%d")

    def get_default_config(self):
        return {
            "supermarkets": [
                {
                    "name": "yohananof",
                    "display_name": "יוחננוף",
                    "url": "https://url.publishedprices.co.il/login",
                    "username": "yohananof",
                    "password": ""
                },
                {
                    "name": "victory",
                    "display_name": "ויקטורי",
                    "url": "https://laibcatalog.co.il/"
                },
                {
                    "name": "carrefour",
                    "display_name": "קרפור / מגה",
                    "url": "https://prices.carrefour.co.il/"
                },
                {
                    "name": "shufersal",
                    "display_name": "שופרסל",
                    "url": "http://prices.shufersal.co.il/"
                },
                {
                    "name": "wolt",
                    "display_name": "Wolt",
                    "url": "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"
                },
                {
                    "name": "super-pharm",
                    "display_name": "סופר פארם",
                    "url": "http://prices.super-pharm.co.il/"
                },
                {
                    "name": "hazi-hinam",
                    "display_name": "חצי חינם",
                    "url": "https://shop.hazi-hinam.co.il/Prices"
                },
                {
                    "name": "quik",
                    "display_name": "קוויק",
                    "url": "https://prices.quik.co.il/"
                },
                {
                    "name": "citymarket",
                    "display_name": "סיטי מרקט",
                    "url": "https://www.citymarket-shops.co.il/"
                }
            ],
            "max_branches": 2
        }

    # ──────────────────────────────────────────────────────────
    #  1) SPECIAL HANDLER: Carrefour (date + category dropdown)
    # ──────────────────────────────────────────────────────────
    def handle_carrefour_site(self, driver):
        """Special handling for Carrefour requiring date+category filtering"""
        logger.info("Special handling for Carrefour site (requires date selection)")
        # 1) set yesterday's date on the date input
        yesterday = datetime.now() - timedelta(days=1)
        iso_date = yesterday.strftime("%Y-%m-%d")
        try:
            date_input = driver.find_element(By.CSS_SELECTOR, "input[type='date']")
            driver.execute_script(
                "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('change'));",
                date_input, iso_date
            )
            logger.info(f"Set date to: {iso_date}")
            time.sleep(3)
        except Exception as e:
            logger.warning(f"Could not set date via input: {e}")

        #click חיפוש if it exists
        try:
            btn = driver.find_element(By.XPATH, "//button[contains(text(),'חיפוש')]")
            btn.click()
            logger.info("Clicked search button")
            time.sleep(3)
        except:
            pass

        # 3) for each category select + download first N files
        downloaded = []
        max_files = self.config.get("max_branches", 2)
        categories = {"pricefull": "price", "promofull": "promo"}

        for cat_key, file_type in categories.items():
            try:
                dropdown = None
                for sel in ['cat_filter', 'category', 'filter', 'type']:
                    try:
                        dropdown = driver.find_element(By.ID, sel)
                        break
                    except:
                        continue
                if not dropdown:
                    logger.error("Category dropdown not found; skipping Carrefour category logic")
                    break

                select = Select(dropdown)
                for opt in select.options:
                    val = (opt.get_attribute('value') or opt.text).lower()
                    if cat_key in val:
                        select.select_by_visible_text(opt.text)
                        logger.info(f"Selected category: {opt.text}")
                        time.sleep(2)
                        break

                links = self.find_all_download_links(driver)
                for idx, link in enumerate(links[:max_files]):
                    branch = idx + 1
                    prefix = f"{file_type.capitalize()}Full"
                    filename = f"{prefix}_{self.date_str}_{branch}.gz"
                    path = self.download_file(link['url'], filename)
                    if path:
                        downloaded.append({
                            'path': path,
                            'branch': f"branch_{branch}",
                            'type': file_type
                        })
            except Exception as e:
                logger.error(f"Error processing Carrefour category '{cat_key}': {e}")

        return downloaded

    # ──────────────────────────────────────────────────────────
    #  2) SPECIAL HANDLER: Victory (no :has() CSS, fallback to generic)
    # ──────────────────────────────────────────────────────────
    def handle_victory_site(self, driver):
        """Optimized handler for Victory site which has huge tables"""
        logger.info("Special handling for Victory site (optimized)")
        downloaded = []
        max_files = self.config.get("max_branches", 2)

        try:
            # pick up to first 10 .gz links on the page
            links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.gz']")[:10]
            price_count = promo_count = 0

            for link in links:
                if price_count >= max_files and promo_count >= max_files:
                    break
                href = link.get_attribute('href')
                try:
                    row = link.find_element(By.XPATH, "./ancestor::tr")
                    row_text = row.text.lower()
                except:
                    row_text = link.text.lower()

                # promo detection
                if any(k in row_text for k in ['מבצע', 'promo']) and promo_count < max_files:
                    promo_count += 1
                    filename = f"PromoFull_{self.date_str}_{promo_count}.gz"
                    path = self.download_file(href, filename)
                    if path:
                        downloaded.append({
                            'path': path,
                            'branch': f"branch_{promo_count}",
                            'type': 'promo'
                        })

                # price detection
                elif any(k in row_text for k in ['מחיר', 'price']) and price_count < max_files:
                    price_count += 1
                    filename = f"PriceFull_{self.date_str}_{price_count}.gz"
                    path = self.download_file(href, filename)
                    if path:
                        downloaded.append({
                            'path': path,
                            'branch': f"branch_{price_count}",
                            'type': 'price'
                        })

            # if we didn’t find enough fall back to generic
            if price_count < max_files or promo_count < max_files:
                logger.info("Victory: insufficient special-case files, falling back to generic handler")
                links = self.find_all_download_links(driver)
                return self.organize_and_download_files(links, "victory")

            logger.info(f"Victory: downloaded {len(downloaded)} files via optimized handler")
            return downloaded

        except Exception as e:
            logger.error(f"Error handling Victory site: {e}")
            links = self.find_all_download_links(driver)
            return self.organize_and_download_files(links, "victory")
        
    def setup_driver(self):
        """Setup Chrome driver"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        
        prefs = {
            "download.default_directory": str(self.download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        
        driver = webdriver.Chrome(options=options)
        
        # Enable download in headless mode
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": str(self.download_dir.absolute())
        })
        
        return driver

    def detect_site_type(self, driver, url):
        """Automatically detect what type of site we're dealing with"""
        logger.info(f"Detecting site type for {url}")
        
        driver.get(url)
        time.sleep(2)  # Allow time for page to load

        # Check for login form
        login_indicators = [
            "input[type='password']",
            "input[name*='user']",
            "#username",
            ".login-form",
            "form[action*='login']"
        ]
        
        for selector in login_indicators:
            if driver.find_elements(By.CSS_SELECTOR, selector):
                logger.info("Detected: Login required site")
                return "login"
        
        # Check for table structure
        tables = driver.find_elements(By.TAG_NAME, "table")
        if tables:
            for table in tables:
                links = table.find_elements(By.CSS_SELECTOR, "a[href*='.gz'], a[href*='download']")
                if links:
                    logger.info("Detected: Table-based site")
                    return "table"
        
        # Check for direct download links
        download_indicators = [
            ".downloadBtn",
            "a[href*='.gz']",
            "a[href*='Price']",
            "a[href*='Promo']",
            "button[onclick*='download']"
        ]
        
        for selector in download_indicators:
            if driver.find_elements(By.CSS_SELECTOR, selector):
                logger.info("Detected: Direct download site")
                return "direct"
        
        # Default fallback
        logger.info("Detected: Generic site (will try all methods)")
        return "generic"

    def handle_login(self, driver, supermarket):
        """Handle login if credentials are provided"""
        if 'username' not in supermarket:
            return False
        
        logger.info(f"Attempting login for {supermarket.get('display_name', supermarket['name'])}")
        
        try:
            # Find username field (try multiple selectors)
            username_selectors = [
                "input[type='text']",
                "input[name*='user']",
                "#username",
                "input[placeholder*='user']"
            ]
            
            username_field = None
            for selector in username_selectors:
                try:
                    username_field = driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if username_field:
                username_field.clear()
                username_field.send_keys(supermarket['username'])
                
                try:
                    password_field = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
                    password_field.clear()
                    if supermarket.get('password'):
                        password_field.send_keys(supermarket['password'])
                except:
                    logger.info("No password field found or not required")
                
                submit_selectors = [
                    "button[type='submit']",
                    "input[type='submit']",
                    ".login-button",
                    "button:not([type='button'])"
                ]
                
                for selector in submit_selectors:
                    try:
                        submit = driver.find_element(By.CSS_SELECTOR, selector)
                        submit.click()
                        time.sleep(2)  
                        return True
                    except:
                        continue
                        
            return False
            
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def find_all_download_links(self, driver):
        """Find ALL possible download links on the page - OPTIMIZED VERSION"""
        download_links = []
        seen_urls = set()
        
        MAX_LINKS = 100  
        
        # Start with the most specific selectors first
        priority_selectors = [
            "a[href*='.gz']",  
            ".downloadBtn",   
            "a[href*='Price']", 
            "a[href*='Promo']" 
        ]
        
        # Try priority selectors first
        for selector in priority_selectors:
            if len(download_links) >= MAX_LINKS:
                break
                
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)[:50]  
                for elem in elements:
                    if len(download_links) >= MAX_LINKS:
                        break
                        
                    try:
                        href = elem.get_attribute('href')
                        if href and href not in seen_urls:
                            seen_urls.add(href)
                            
                            # Quick file type detection
                            filename = href.split('/')[-1].lower()
                            file_type = "unknown"
                            
                            if 'price' in filename:
                                file_type = "price"
                            elif 'promo' in filename:
                                file_type = "promo"
                            
                            download_links.append({
                                'url': href,
                                'text': elem.text[:50] if elem.text else "",
                                'type': file_type,
                                'element': elem,
                                'filename': filename
                            })
                    except:
                        continue
            except:
                continue
        
        # If we need more linkscheck tables (but limit processing)
        if len(download_links) < 10:  
            try:
                tables = driver.find_elements(By.TAG_NAME, "table")[:2]  
                for table in tables:
                    if len(download_links) >= MAX_LINKS:
                        break
                        
                    rows = table.find_elements(By.TAG_NAME, "tr")[:20]  
                    for row in rows:
                        if len(download_links) >= MAX_LINKS:
                            break
                            
                        try:
                            links = row.find_elements(By.TAG_NAME, "a")[:2]  
                            for link in links:
                                href = link.get_attribute('href')
                                if href and href not in seen_urls and '.gz' in href:
                                    seen_urls.add(href)
                                    
                                    cells = row.find_elements(By.TAG_NAME, "td")
                                    row_text = " ".join([cell.text[:20] for cell in cells[:3]])
                                    
                                    file_type = "unknown"
                                    if 'price' in row_text.lower():
                                        file_type = "price"
                                    elif 'promo' in row_text.lower():
                                        file_type = "promo"
                                    
                                    download_links.append({
                                        'url': href,
                                        'text': row_text[:50],
                                        'type': file_type,
                                        'element': link
                                    })
                        except:
                            continue
            except:
                pass
        
        logger.info(f"Found {len(download_links)} download links (limited search)")
        return download_links

    def download_file(self, url, filename):
        """Download a file"""
        try:
            local_path = self.download_dir / filename
            
            logger.info(f"Downloading: {filename}")
            
            response = requests.get(url, stream=True, timeout=30, verify=False)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(8192):
                    if chunk:
                        f.write(chunk)
            
            return local_path
            
        except Exception as e:
            logger.error(f"Failed to download {filename}: {e}")
            return None

    def organize_and_download_files(self, download_links, supermarket_name):
        """Organize files by branch and download them"""
        downloaded = []
        
        # Debug
        logger.info(f"File type breakdown for {supermarket_name}:")
        price_files = [link for link in download_links if link['type'] == 'price']
        promo_files = [link for link in download_links if link['type'] == 'promo']
        unknown_files = [link for link in download_links if link['type'] == 'unknown']
        
        logger.info(f"  - Price files found: {len(price_files)}")
        logger.info(f"  - Promo files found: {len(promo_files)}")
        logger.info(f"  - Unknown files found: {len(unknown_files)}")
        
        if price_files:
            logger.info(f"  First price file: {price_files[0].get('filename', 'N/A')}")
        if promo_files:
            logger.info(f"  First promo file: {promo_files[0].get('filename', 'N/A')}")
        
        max_branches = self.config.get('max_branches', 2)
        
        # Download price files
        for i, link in enumerate(price_files[:max_branches]):
            branch_num = i + 1
            filename = f"PriceFull_{self.date_str}_{branch_num}.gz"
            
            file_path = self.download_file(link['url'], filename)
            if file_path:
                downloaded.append({
                    'path': file_path,
                    'branch': f"branch_{branch_num}",
                    'type': 'price'
                })
        
        # Download promo files if they exist
        if promo_files:
            for i, link in enumerate(promo_files[:max_branches]):
                branch_num = i + 1
                filename = f"PromoFull_{self.date_str}_{branch_num}.gz"
                
                file_path = self.download_file(link['url'], filename)
                if file_path:
                    downloaded.append({
                        'path': file_path,
                        'branch': f"branch_{branch_num}",
                        'type': 'promo'
                    })
        else:
            logger.info(f"No promo files available for {supermarket_name}")
        
        if not price_files and unknown_files:
            logger.info("No specifically typed files found, using unknown files...")
            for i, link in enumerate(unknown_files[:max_branches * 2]):
                branch_num = (i // 2) + 1
                # Try to determine type from filename
                filename = link.get('filename', '')
                if 'promo' in filename.lower():
                    file_type = "promo"
                    filename_new = f"PromoFull_{self.date_str}_{branch_num}.gz"
                else:
                    file_type = "price"
                    filename_new = f"PriceFull_{self.date_str}_{branch_num}.gz"
                
                file_path = self.download_file(link['url'], filename_new)
                if file_path:
                    downloaded.append({
                        'path': file_path,
                        'branch': f"branch_{branch_num}",
                        'type': file_type
                    })
        
        return downloaded


    def upload_to_s3(self, file_info, supermarket_name):
        """Upload a downloaded file to S3, under <supermarket>/<branch>/filename.gz"""
        try:
            file_path = file_info['path']
            branch    = file_info['branch']

            if not str(file_path).endswith('.gz'):
                gz_path = file_path.with_suffix(file_path.suffix + '.gz')
                with open(file_path, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
                    f_out.writelines(f_in)
                file_path.unlink()  # remove the original
                file_path = gz_path

            # Build the key and upload
            s3_key = f"{supermarket_name}/{branch}/{file_path.name}"
            self.s3.upload_file(str(file_path), self.bucket, s3_key)
            logger.info(f"Uploaded to s3://{self.bucket}/{s3_key}")

            file_path.unlink()
            return True

        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False
        
    def handle_dropdown_site(self, driver, supermarket_name):
        """Handle sites that use dropdowns to filter file types (Mega, Shufersal)"""
        logger.info(f"Handling dropdown-based site for {supermarket_name}")
        
        downloaded_files = []
        
        # Look for category/filter dropdowns
        category_selectors = ['cat_filter', 'category', 'filter', 'type']
        category_dropdown = None
        
        for selector_id in category_selectors:
            try:
                category_dropdown = driver.find_element(By.ID, selector_id)
                break
            except:
                continue
        
        if not category_dropdown:
            try:
                category_dropdown = driver.find_element(By.CSS_SELECTOR, "select[onchange*='filter'], select[onchange*='category']")
            except:
                logger.warning("No category dropdown found")
                return downloaded_files
        
        # Get available categories
        select = Select(category_dropdown)
        options = select.options
        logger.info(f"Found {len(options)} category options")

        categories_to_process = ['pricefull', 'promofull']
        
        for category in categories_to_process:
            try:
                option_found = False
                for option in options:
                    option_value = option.get_attribute('value').lower()
                    option_text = option.text.lower()
                    
                    if category in option_value or category in option_text:
                        logger.info(f"Selecting category: {option.text}")
                        select.select_by_visible_text(option.text)
                        option_found = True
                        time.sleep(3) 
                        break
                
                if not option_found:
                    logger.warning(f"Category {category} not found in dropdown")
                    continue
                
                links = self.find_all_download_links(driver)
                logger.info(f"Found {len(links)} links for {category}")
                
                # Download first N files for this category
                max_files = 2  
                for i, link in enumerate(links[:max_files]):
                    branch_num = i + 1
                    if 'price' in category:
                        filename = f"PriceFull_{self.date_str}_{branch_num}.gz"
                        file_type = 'price'
                    else:
                        filename = f"PromoFull_{self.date_str}_{branch_num}.gz"
                        file_type = 'promo'
                    
                    file_path = self.download_file(link['url'], filename)
                    if file_path:
                        downloaded_files.append({
                            'path': file_path,
                            'branch': f"branch_{branch_num}",
                            'type': file_type
                        })
                
            except Exception as e:
                logger.error(f"Error processing category {category}: {e}")
        
        return downloaded_files

    def crawl_supermarket(self, driver, supermarket):
        """Crawl a single supermarket site"""
        name = supermarket['name']
        display_name = supermarket.get('display_name', name)
        url = supermarket['url']

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {display_name}")
        logger.info(f"URL: {url}")

        try:
            site_type = self.detect_site_type(driver, url)

            if site_type == "login" or 'username' in supermarket:
                if not self.handle_login(driver, supermarket):
                    logger.warning("Login failed, trying to proceed anyway...")


            if name == 'victory':
                downloaded = self.handle_victory_site(driver)

            elif name == 'carrefour':
                downloaded = self.handle_carrefour_site(driver)

            elif name in ['shufersal'] or site_type == "generic":
                # dropdown-based (Shufersal, etc.)
                try:
                    driver.find_element(By.ID, "cat_filter")
                    logger.info("Detected dropdown-based file filtering")
                    downloaded = self.handle_dropdown_site(driver, name)
                except:
                    links = self.find_all_download_links(driver)
                    downloaded = self.organize_and_download_files(links, name)

            else:
                # fallback
                links = self.find_all_download_links(driver)
                downloaded = self.organize_and_download_files(links, name)

            uploaded_count = 0
            for info in downloaded:
                if self.upload_to_s3(info, name):
                    uploaded_count += 1

            logger.info(f"Successfully uploaded {uploaded_count} files for {display_name}")
            return uploaded_count

        except Exception as e:
            logger.error(f"Error crawling {display_name}: {e}")
            return 0
        
    def run(self, supermarket_filter=None, test_mode=False):
        """
        Main execution
        supermarket_filter: optional list of supermarket names to process
        test_mode: if True, only detect and list files without downloading
        """
        logger.info("Starting Universal Supermarket Crawler")
        if test_mode:
            logger.info("*** RUNNING IN TEST MODE - NO DOWNLOADS ***")
        logger.info(f"S3 bucket: {self.bucket}")
        
        supermarkets = self.config['supermarkets']
        logger.info(f"Available supermarkets in config: {[s['name'] for s in supermarkets]}")
        
        if supermarket_filter:
            logger.info(f"Filter requested for: {supermarket_filter}")
            supermarkets = [s for s in supermarkets if s['name'] in supermarket_filter]
            if not supermarkets:
                logger.warning(f"No supermarkets found matching filter: {supermarket_filter}")
                logger.info(f"Available options: {[s['name'] for s in self.config['supermarkets']]}")
        
        logger.info(f"Processing {len(supermarkets)} supermarkets")
        
        driver = self.setup_driver()
        results = {}
        
        try:
            for supermarket in supermarkets:
                if test_mode:
                    test_results = self.test_supermarket(driver, supermarket)
                    results[supermarket['name']] = test_results
                else:
                    uploaded = self.crawl_supermarket(driver, supermarket)
                    results[supermarket['name']] = uploaded
                time.sleep(1)  
                
        finally:
            driver.quit()
            
            # Cleanup
            for file in self.download_dir.glob("*"):
                try:
                    file.unlink()
                except:
                    pass
            
            # Summary
            logger.info(f"\n{'='*60}")
            if test_mode:
                logger.info("TEST MODE SUMMARY")
                logger.info(f"{'='*60}")
                for name, info in results.items():
                    logger.info(f"\n{name}:")
                    logger.info(f"  Site type: {info['site_type']}")
                    logger.info(f"  Total links: {info['total_links']}")
                    logger.info(f"  Price files: {info['price_files']}")
                    logger.info(f"  Promo files: {info['promo_files']}")
                    if info['sample_files']:
                        logger.info(f"  Sample files:")
                        for sample in info['sample_files'][:3]:
                            logger.info(f"    - {sample}")
            else:
                logger.info("CRAWLER SUMMARY")
                logger.info(f"{'='*60}")
                total = 0
                for name, count in results.items():
                    logger.info(f"{name}: {count} files")
                    total += count
                logger.info(f"\nTotal files uploaded: {total}")
                logger.info(f"S3 bucket: {self.bucket}")
    
    def test_supermarket(self, driver, supermarket):
        """Test a supermarket site without downloading"""
        name = supermarket['name']
        display_name = supermarket.get('display_name', name)
        url = supermarket['url']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"TESTING: {display_name}")
        logger.info(f"URL: {url}")
        
        try:
            site_type = self.detect_site_type(driver, url)
            if site_type == "login" or 'username' in supermarket:
                if not self.handle_login(driver, supermarket):
                    logger.warning("Login failed, trying anyway…")

            # Run special handlers for Victory/Carrefour in test mode
            items = None
            if name == 'victory':
                items = self.handle_victory_site(driver)
            elif name == 'carrefour':
                items = self.handle_carrefour_site(driver)

            # Fallback to generic link finder
            if items is None:
                download_links = self.find_all_download_links(driver)
                total         = len(download_links)
                price_count   = sum(1 for l in download_links if l['type']=='price')
                promo_count   = sum(1 for l in download_links if l['type']=='promo')
                unknown_count = sum(1 for l in download_links if l['type']=='unknown')
                sample_files  = [l.get('filename', l['url'][-50:]) for l in download_links[:5]]
            else:
                total         = len(items)
                price_count   = sum(1 for i in items if i['type']=='price')
                promo_count   = sum(1 for i in items if i['type']=='promo')
                unknown_count = 0
                sample_files  = [Path(i['path']).name for i in items[:5]]

            # 4) Return summary
            return {
                'site_type':     site_type,
                'total_links':   total,
                'price_files':   price_count,
                'promo_files':   promo_count,
                'unknown_files': unknown_count,
                'sample_files':  sample_files
            }
            
        except Exception as e:
            logger.error(f"Error testing {display_name}: {e}")
            return {
                'site_type':     'error',
                'total_links':   0,
                'price_files':   0,
                'promo_files':   0,
                'unknown_files': 0,
                'sample_files':  []
            }

def create_config_file():
    """Create a sample configuration file"""
    config = {
        "supermarkets": [
            {
                "name": "yohananof",
                "display_name": "יוחננוף",
                "url": "https://url.publishedprices.co.il/login",
                "username": "yohananof",
                "password": ""
            },
            {
                "name": "victory",
                "display_name": "ויקטורי",
                "url": "https://laibcatalog.co.il/"
            },
            {
                "name": "carrefour",
                "display_name": "קרפור / מגה",
                "url": "https://prices.carrefour.co.il/"
            },
            {
                "name": "shufersal",
                "display_name": "שופרסל",
                "url": "http://prices.shufersal.co.il/"
            },
            {
                "name": "wolt",
                "display_name": "Wolt",
                "url": "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"
            },
            {
                "name": "super-pharm",
                "display_name": "סופר פארם",
                "url": "http://prices.super-pharm.co.il/"
            },
            {
                "name": "hazi-hinam",
                "display_name": "חצי חינם",
                "url": "https://shop.hazi-hinam.co.il/Prices"
            },
            {
                "name": "quik",
                "display_name": "קוויק",
                "url": "https://prices.quik.co.il/"
            },
            {
                "name": "citymarket",
                "display_name": "סיטי מרקט",
                "url": "https://www.citymarket-shops.co.il/"
            }
        ],
        "max_branches": 2,
        "max_files_per_site": 10
    }
    
    with open('supermarkets_config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    
    print("Created supermarkets_config.json with supermarket configurations")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python universal_supermarket_crawler.py <s3-bucket-name> [options]")
        print("\nOptions:")
        print("  --test                     Run in test mode (no downloads)")
        print("  --config <file>            Use specific config file")
        print("  --only <name1,name2>       Only process specific supermarkets")
        print("\nExamples:")
        print("  python universal_supermarket_crawler.py my-bucket")
        print("  python universal_supermarket_crawler.py my-bucket --test")
        print("  python universal_supermarket_crawler.py my-bucket --config supermarkets_config.json")
        print("  python universal_supermarket_crawler.py my-bucket --only yohananof,victory")
        print("  python universal_supermarket_crawler.py my-bucket --config config.json --only yohananof --test")
        print("\nTo create a sample config file:")
        print("  python universal_supermarket_crawler.py --create-config")
        sys.exit(1)
    
    if sys.argv[1] == "--create-config":
        create_config_file()
        sys.exit(0)
    
    bucket_name = sys.argv[1]
    config_file = None
    supermarket_filter = None
    test_mode = False
    
    # parse args
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == '--test':
            test_mode = True
        elif sys.argv[i] == '--config' and i + 1 < len(sys.argv):
            config_file = sys.argv[i + 1]
            i += 1
        elif sys.argv[i] == '--only' and i + 1 < len(sys.argv):
            supermarket_filter = sys.argv[i + 1].split(',')
            i += 1
        elif sys.argv[i].endswith('.json'):
            config_file = sys.argv[i]
        elif ',' in sys.argv[i]:
            supermarket_filter = sys.argv[i].split(',')
        i += 1
    
    crawler = UniversalSupermarketCrawler(bucket_name, config_file)
    crawler.run(supermarket_filter, test_mode)
