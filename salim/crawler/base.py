import json
import os
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Optional
from urllib.parse import urljoin
import platform  # Import the platform module

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Automatically manages the browser driver
from webdriver_manager.chrome import ChromeDriverManager
# Import for specifying Chrome browser type, essential for ARM Macs
from webdriver_manager.core.os_manager import ChromeType 

# Suppress only the single InsecureRequestWarning from urllib3 needed for this fix
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class SupermarketCrawler:
    """
    A robust crawler that uses Selenium for login/navigation and Requests for fast downloading.
    This version fixes the login issue by handling the entire authenticated session within Selenium.
    """
    def __init__(self, config_name: str):
        """
        Initializes the crawler with a specific configuration.
        
        Args:
            config_name (str): The name of the configuration file (e.g., 'yohananof').
        """
        self.config_name = config_name
        self.config = self._load_config(config_name)
        self.download_dir = self.config.get("name", "default_downloads")
        os.makedirs(self.download_dir, exist_ok=True)
        print(f"✅ Files will be saved to the '{self.download_dir}/' directory.")
        
        self.session = requests.Session()
        self.driver = None # Initialize driver as None

    def _load_config(self, config_name: str) -> dict:
        """
        Loads the JSON configuration file for the specified supermarket.
        
        Args:
            config_name (str): The base name of the config file.
        
        Returns:
            dict: The loaded configuration.
        
        Raises:
            FileNotFoundError: If the configuration file cannot be found.
        """
        # Defines potential paths for the config file for flexibility
        config_path = os.path.join('configs', f'{config_name}.json')
        print(f"ℹ️  Loading configuration from: {config_path}")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"❌ Configuration file not found at: {config_path}.")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _init_driver(self) -> webdriver.Chrome:
        """
        Initializes a headless Chrome WebDriver.
        It specifically handles the driver for macOS on ARM (Apple Silicon) vs. other systems.
        """
        if not self.driver:
            print("🚀 Initializing Selenium WebDriver...")
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-dev-shm-usage")

            try:
                # FIX: Correctly detect ARM Macs and use the Google Chrome driver, not Chromium.
                if platform.system() == "Darwin" and platform.machine() == "arm64":
                    print("   - Detected macOS ARM64, ensuring correct driver for Google Chrome...")
                    driver_path = ChromeDriverManager(chrome_type=ChromeType.GOOGLE).install()
                    service = Service(driver_path)
                else:
                    # For all other systems, use the default installation.
                    print("   - Detected standard OS, using default driver...")
                    service = Service(ChromeDriverManager().install())
                
                self.driver = webdriver.Chrome(service=service, options=chrome_options)

            except Exception as e:
                print(f"   - Webdriver-manager failed: {e}. Falling back to system default chromedriver.")
                # Fallback if webdriver-manager fails for any reason.
                self.driver = webdriver.Chrome(options=chrome_options)
            
            print("✅ WebDriver initialized.")
        return self.driver

    def login_and_find_files_with_selenium(self) -> List[str]:
        """
        Uses Selenium to perform the entire login process and scrape file links.
        This is the core fix, as it keeps the session within a single context (the browser).
        
        Returns:
            List[str]: A list of full URLs to the '.gz' files found on the page.
        """
        self._init_driver()
        credentials = self.config.get("credentials")
        base_url = self.config.get("base_url")

        print(f"🤖 Using Selenium to log in and navigate to files...")
        
        try:
            # 1. Go directly to the login page.
            self.driver.get(base_url)
            
            # 2. Wait for the login form elements to be ready.
            wait = WebDriverWait(self.driver, 20) # Increased wait time for better reliability.
            print("... Waiting for login page to load ...")
            username_field = wait.until(EC.presence_of_element_located((By.ID, "username")))
            password_field = self.driver.find_element(By.ID, "password")
            submit_button = self.driver.find_element(By.ID, "login-button")

            # 3. Enter credentials and click the login button.
            print("... Entering credentials ...")
            username_field.send_keys(credentials.get("username"))
            # This handles cases where a password is not required by the config.
            password_field.send_keys(credentials.get("password", "")) 
            
            print("... Submitting login form ...")
            submit_button.click()

            # 4. CRITICAL STEP: After login, wait for an element that ONLY exists on the file listing page.
            # This confirms the login was successful and the page has loaded.
            print("... Waiting for file list to load after login ...")
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.context tr a.f")))
            
            # 5. If the wait succeeds, scrape the links from the page.
            print("... File page loaded successfully. Scraping links ...")
            page_content = self.driver.page_source
            current_url = self.driver.current_url
            
            soup = BeautifulSoup(page_content, 'lxml')
            file_links_selector = self.config["selectors"]["file_links"]
            links = soup.select(file_links_selector)
            
            # Construct full URLs from the relative links found.
            full_urls = [urljoin(current_url, link.get('href')) for link in links]
            print(f"📄 Found {len(full_urls)} total file links on the page.")

            # --- NEW: Filter the found URLs based on the patterns in the config ---
            print("... Filtering links based on 'file_patterns' from config ...")
            file_patterns = self.config.get("file_patterns", {})
            # Get the prefixes like "PriceFull", "PromoFull" from the config values
            valid_prefixes = tuple([v.split('{')[0] for v in file_patterns.values() if v])

            if not valid_prefixes:
                print("⚠️ No 'file_patterns' found in config. Returning all found links.")
                return full_urls

            filtered_urls = []
            for url in full_urls:
                filename = os.path.basename(url)
                if filename.startswith(valid_prefixes):
                    filtered_urls.append(url)

            print(f"✅ Found {len(filtered_urls)} matching files after filtering.")
            return filtered_urls
            
        except TimeoutException:
            print("❌ Timed out waiting for page to load. This could be due to wrong credentials, a website change, or a network issue.")
            screenshot_path = 'selenium_timeout_failure.png'
            self.driver.save_screenshot(screenshot_path)
            print(f"📸 Screenshot of the failure page saved to '{screenshot_path}' for debugging.")
            return []
        except Exception as e:
            print(f"❌ An unexpected error occurred during the Selenium process: {e}")
            return []

    def download_file(self, url: str) -> Optional[str]:
        """
        Downloads a single file using the `requests` library for speed.
        It transfers the login cookies from Selenium to the `requests` session first.
        
        Args:
            url (str): The full URL of the file to download.
        
        Returns:
            Optional[str]: The local path to the downloaded file, or None if it failed.
        """
        try:
            # Transfer cookies from the authenticated Selenium session to the requests session.
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

            filename = os.path.basename(url)
            local_path = os.path.join(self.download_dir, filename)
            
            print(f"⬇️  Downloading {filename}...")
            response = self.session.get(url, stream=True, verify=False)
            response.raise_for_status()
            
            # Write the file to disk in chunks.
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"👍 Successfully saved to {local_path}")
            return local_path
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to download {url}. Error: {e}")
            return None

    def crawl(self) -> List[str]:
        """
        Main crawling method that orchestrates the entire process.
        """
        # The entire login and file-finding logic is now in one robust method.
        gz_urls = self.login_and_find_files_with_selenium()

        if not gz_urls:
            print("🤷 No files found to download.")
            self.close()
            return []

        print(f"\n🚀 Starting download of {len(gz_urls)} files...")
        downloaded_files = []
        for url in gz_urls:
            file_path = self.download_file(url)
            if file_path:
                downloaded_files.append(file_path)

        self.close() # Close the browser session after we are done.
        print(f"\n🎉 Crawl finished. Downloaded {len(downloaded_files)} files.")
        return downloaded_files

    def close(self):
        """Closes the WebDriver session to free up resources."""
        if self.driver:
            self.driver.quit()
            print("✅ WebDriver closed.")
