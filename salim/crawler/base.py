import json
import os
import time
import requests
import re
from bs4 import BeautifulSoup
from typing import List, Optional
from urllib.parse import urljoin
import platform
from collections import defaultdict
import boto3
from botocore.exceptions import ClientError

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# Suppress only the single InsecureRequestWarning from urllib3 needed for this fix
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class SupermarketCrawler:
    """
    A robust crawler that uses Selenium for login/navigation and Requests for fast downloading.
    This version is refactored for better modularity.
    """
    def __init__(self, config_name: str):
        """Initializes the crawler with a specific configuration."""
        self.config_name = config_name
        self.config = self._load_config(config_name)
        self.download_dir = self.config.get("name", "default_downloads")
        os.makedirs(self.download_dir, exist_ok=True)
        print(f"Files will be saved to the '{self.download_dir}/' directory.")
        
        self.session = requests.Session()
        self.driver = None # Initialize driver as None
        
        # Initialize S3 client for LocalStack
        # Use environment variable for S3 endpoint (Docker network)
        s3_endpoint = os.environ.get('S3_ENDPOINT', 'http://localhost:4566')
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        self.s3_bucket = 'test-bucket'

    def _load_config(self, config_name: str) -> dict:
        """Loads the JSON configuration file for the specified supermarket."""
        config_path = os.path.join('configs', f'{config_name}.json')
        print(f"Loading configuration from: {config_path}")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}.")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _init_driver(self) -> webdriver.Chrome:
        """
        Initializes a headless Chrome WebDriver using webdriver-manager (works on Windows & Mac).
        """
        if not self.driver:
            print("Initializing Selenium WebDriver...")
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            try:
                # Use webdriver-manager like Lady Gaga crawler
                chromedriver_path = ChromeDriverManager().install()
                service = Service(chromedriver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("WebDriver initialized successfully.")
            except Exception as e:
                print(f"Failed to initialize WebDriver with service: {e}")
                print("Trying alternative approach...")
                # Alternative approach without service (like Lady Gaga crawler fallback)
                try:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    print("WebDriver initialized successfully with fallback method.")
                except Exception as e2:
                    print(f"Failed with fallback method too: {e2}")
                    raise
            
        return self.driver

    def _login_with_selenium(self) -> bool:
        """
        Handles the login process using Selenium.
        
        Returns:
            bool: True if login is successful, False otherwise.
        """
        credentials = self.config.get("credentials")
        base_url = self.config.get("base_url")

        print("Using Selenium to log in...")
        try:
            self.driver.get(base_url)
            wait = WebDriverWait(self.driver, 20)
            
            print("... Waiting for login page to load ...")
            username_field = wait.until(EC.presence_of_element_located((By.ID, "username")))
            password_field = self.driver.find_element(By.ID, "password")
            submit_button = self.driver.find_element(By.ID, "login-button")

            print("... Entering credentials ...")
            username_field.send_keys(credentials.get("username"))
            password_field.send_keys(credentials.get("password", ""))
            
            print("... Submitting login form ...")
            submit_button.click()

            # Wait for an element on the next page to confirm successful login
            print("... Waiting for page to load after login ...")
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.context tr a.f")))
            print("Login successful.")
            return True

        except TimeoutException:
            print("Timed out during login. Check credentials or website layout.")
            screenshot_path = 'selenium_login_failure.png'
            self.driver.save_screenshot(screenshot_path)
            print(f"Screenshot saved to '{screenshot_path}'")
            return False
        except Exception as e:
            print(f"An unexpected error occurred during login: {e}")
            return False

    def _find_and_filter_files(self) -> List[str]:
        """
        Finds all file links on the current page and filters them based on config patterns.
        Assumes the driver is already on the correct, logged-in page.
        
        Returns:
            List[str]: A list of filtered, full URLs.
        """
        print("... Scraping and filtering file links ...")
        try:
            page_content = self.driver.page_source
            current_url = self.driver.current_url
            
            soup = BeautifulSoup(page_content, 'lxml')
            file_links_selector = self.config["selectors"]["file_links"]
            links = soup.select(file_links_selector)
            
            full_urls = [urljoin(current_url, link.get('href')) for link in links]
            print(f"Found {len(full_urls)} total file links on the page.")

            # Filter the URLs based on the patterns in the config
            file_patterns = self.config.get("file_patterns", {})
            valid_prefixes = tuple([v.split('{')[0] for v in file_patterns.values() if v])

            if not valid_prefixes:
                print("No 'file_patterns' found in config. Returning all found links.")
                return full_urls

            filtered_urls = [url for url in full_urls if os.path.basename(url).startswith(valid_prefixes)]

            print(f"Found {len(filtered_urls)} matching files after filtering.")
            return filtered_urls
        except Exception as e:
            print(f"An error occurred while finding and filtering files: {e}")
            return []

    def get_latest_files(self, gz_urls: List[str]) -> List[str]:
        """
        Get the latest PriceFull and PromoFull files for each branch
        
        Args:
            gz_urls (List[str]): List of all .gz file URLs
            
        Returns:
            List[str]: List of latest file URLs to download
        """
        print("... Finding latest files for each branch ...")
        
        # Parse file information
        file_info = defaultdict(lambda: {'price': None, 'promo': None})
        
        for url in gz_urls:
            filename = url.split('/')[-1]  # Get filename from URL
            
            # Parse PriceFull files
            price_match = re.match(r'PriceFull(\d+)-(\d+)-(\d+)\.gz', filename)
            if price_match:
                store_id = price_match.group(1)
                branch = price_match.group(2)
                date = price_match.group(3)
                key = f"{store_id}-{branch}"
                
                # Keep the latest date
                if not file_info[key]['price'] or date > file_info[key]['price']['date']:
                    file_info[key]['price'] = {'url': url, 'date': date}
            
            # Parse PromoFull files
            promo_match = re.match(r'PromoFull(\d+)-(\d+)-(\d+)\.gz', filename)
            if promo_match:
                store_id = promo_match.group(1)
                branch = promo_match.group(2)
                date = promo_match.group(3)
                key = f"{store_id}-{branch}"
                
                # Keep the latest date
                if not file_info[key]['promo'] or date > file_info[key]['promo']['date']:
                    file_info[key]['promo'] = {'url': url, 'date': date}
        
        # Return URLs of latest files
        latest_files = []
        for key, files in file_info.items():
            if files['price']:
                latest_files.append(files['price']['url'])
                print(f"Latest PriceFull for {key}: {files['price']['date']}")
            if files['promo']:
                latest_files.append(files['promo']['url'])
                print(f"Latest PromoFull for {key}: {files['promo']['date']}")
        
        print(f"Found {len(latest_files)} latest files to download")
        return latest_files

    def download_file(self, url: str) -> Optional[str]:
        """
        Downloads a single file, but first checks if it already exists.
        """
        filename = os.path.basename(url)
        local_path = os.path.join(self.download_dir, filename)

        if os.path.exists(local_path):
            print(f"Skipping {filename} (already exists).")
            return local_path # Return the path so it's still counted as "handled"

        try:
            # Transfer cookies from the authenticated Selenium session to the requests session.
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
            
            print(f"Downloading {filename}...")
            response = self.session.get(url, stream=True, verify=False)
            response.raise_for_status()
            
            # Write the file to disk in chunks.
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Successfully saved to {local_path}")
            return local_path
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url}. Error: {e}")
            return None

    def crawl(self) -> List[str]:
        """Main crawling method that orchestrates the entire process."""
        self._init_driver()
        
        if not self._login_with_selenium():
            print("Crawl aborted due to login failure.")
            self.close()
            return []

        all_gz_urls = self._find_and_filter_files()
        
        if not all_gz_urls:
            print("No files found to download after filtering.")
            self.close()
            return []

        # Get only the latest files for each branch
        latest_gz_urls = self.get_latest_files(all_gz_urls)

        if not latest_gz_urls:
            print("No latest files found to download.")
            self.close()
            return []

        print(f"\nStarting download of {len(latest_gz_urls)} latest files...")
        downloaded_files = []
        for url in latest_gz_urls:
            file_path = self.download_file(url)
            if file_path:
                # Upload to S3
                self.upload_to_s3(file_path)
                downloaded_files.append(file_path)

        self.close()
        print(f"\nCrawl finished. Downloaded {len(downloaded_files)} latest files.")
        return downloaded_files

    def upload_to_s3(self, file_path: str) -> bool:
        """Upload a file to S3."""
        try:
            filename = os.path.basename(file_path)
            print(f"Uploading {filename} to S3...")
            
            with open(file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=filename,
                    Body=f
                )
            
            print(f"✅ Successfully uploaded {filename} to S3")
            return True
            
        except Exception as e:
            print(f"❌ Failed to upload {filename} to S3: {e}")
            return False

    def close(self):
        """Closes the WebDriver session to free up resources."""
        if self.driver:
            self.driver.quit()
            print("WebDriver closed.")
