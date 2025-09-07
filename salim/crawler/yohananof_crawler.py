import os
import re
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import defaultdict
import time
import sys

# Selenium imports based on the cheat sheet
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from uploader.upload_to_s3 import upload_file_to_s3

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class YohananofCrawler:
    def __init__(self, config):
        self.config = config
        self.download_folder = os.path.join("salim", "downloads", self.config["provider"])
        os.makedirs(self.download_folder, exist_ok=True)
        self.session = requests.Session()
        self.driver = None # Initialize driver as None

    def crawl(self):
        print(f"Logging in as {self.config['username']}")

        # Step 1: Set up Selenium (headless Chrome)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        # self.driver = webdriver.Chrome(
        #     service=Service(ChromeDriverManager().install()),
        #     options=chrome_options
        # )
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        try:
            # Step 2: Open login page
            self.driver.get(self.config["login_url"])

            # Step 3: Enter username and press Enter (no password required)
            self.driver.get("https://url.publishedprices.co.il/login")
            username_field = self.driver.find_element(By.NAME, "username")
            username_field.send_keys(self.config["username"])
            username_field.send_keys(Keys.ENTER)
            time.sleep(3)  # Wait for redirection

            # Step 4: Wait for the /file page to load
            WebDriverWait(self.driver, 10).until(EC.url_contains("/file"))
            self.driver.get(self.config["file_list_url"])

            # Step 5: Wait until files appear
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.f"))
            )
            print("File list loaded.")

            # Step 6: Parse page source using BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            file_links = soup.find_all("a", href=re.compile(r"\.gz$"))
            print(f"Found {len(file_links)} .gz files")

            if not file_links:
                print("No files found.")
                return

            # Step 7: Group files by branch ID
            branch_files = defaultdict(lambda: {"price": [], "promo": []})

            for link in file_links:
                href = link["href"]
                filename = os.path.basename(href)

                # Extract branch ID and timestamp from filename
                # Format: PriceFull7290803800003-001-202508060010.gz
                # or: PromoFull7290803800003-001-202508060010.gz
                match = re.search(r"-(\d{3})-(\d{12})\.gz$", filename)
                if not match:
                    continue
                    
                branch_id = match.group(1)
                timestamp = match.group(2)

                # Check if it's a price or promo file
                filename_lower = filename.lower()
                if filename_lower.startswith("pricefull"):
                    branch_files[branch_id]["price"].append({
                        "filename": filename,
                        "href": href,
                        "timestamp": timestamp
                    })
                elif filename_lower.startswith("promofull"):
                    branch_files[branch_id]["promo"].append({
                        "filename": filename, 
                        "href": href,
                        "timestamp": timestamp
                    })

            if not branch_files:
                print("No price or promo files matched.")
                return
            
            print(f"Found files for {len(branch_files)} branches")

            # Step 8: Download the latest file for each type per branch
            for branch_id, files in branch_files.items():
                print(f"\nProcessing branch {branch_id}:")
                
                # Create branch folder
                branch_folder = os.path.join(self.download_folder, branch_id)
                os.makedirs(branch_folder, exist_ok=True)
                
                # Get latest price file
                if files["price"]:
                    # Sort by timestamp and get the latest
                    latest_price = max(files["price"], key=lambda x: x["timestamp"])
                    
                    # Format: priceFull_001_20250806_0010.gz
                    formatted_filename = f"priceFull_{branch_id}_{latest_price['timestamp'][:8]}_{latest_price['timestamp'][8:]}.gz"
                    
                    print(f"  Latest price file: {latest_price['filename']} -> {formatted_filename}")
                    self.download_file(latest_price['href'], formatted_filename, branch_folder)
                else:
                    print(f"  No price file found for branch {branch_id}")

                # Get latest promo file
                if files["promo"]:
                    # Sort by timestamp and get the latest
                    latest_promo = max(files["promo"], key=lambda x: x["timestamp"])
                    
                    # Format: promoFull_001_20250806_0010.gz
                    formatted_filename = f"promoFull_{branch_id}_{latest_promo['timestamp'][:8]}_{latest_promo['timestamp'][8:]}.gz"
                    
                    print(f"  Latest promo file: {latest_promo['filename']} -> {formatted_filename}")
                    self.download_file(latest_promo['href'], formatted_filename, branch_folder)
                else:
                    print(f"  No promo file found for branch {branch_id}")

        finally:
            # Step 9: Always close the browser
            if self.driver:
                self.driver.quit()

    # def download_file(self, href, filename, destination_folder):
    #     """Download a file using requests to specific folder"""
    #     full_url = urljoin(self.config["base_url"], href)
    #     dest_path = os.path.join(destination_folder, filename)

    #     print(f"Downloading {filename}...")
    #     try:
    #         response = requests.get(full_url, stream=True, verify=False)
    #         response.raise_for_status()  # Raise an exception for bad status codes

    #         # Only remove older files of the same type (price or promo)
    #         file_type_prefix = "priceFull" if filename.lower().startswith("pricefull") else "promoFull"
    #         for existing_file in os.listdir(destination_folder):
    #             if (
    #                 existing_file.endswith(".gz")
    #                 and existing_file != filename
    #                 and existing_file.lower().startswith(file_type_prefix.lower())
    #             ):
    #                 os.remove(os.path.join(destination_folder, existing_file))
            

    #         with open(dest_path, "wb") as f:
    #             for chunk in response.iter_content(chunk_size=8192):
    #                 f.write(chunk)
    #         print(f"Saved to {dest_path}")

    #         # Upload to S3
    #         s3_key = f"providers/{os.path.basename(destination_folder)}/{filename}"
    #         upload_file_to_s3(dest_path, s3_key)
            
    #     except requests.RequestException as e:
    #         print(f"Error downloading {filename}: {str(e)}")

    def download_file(self, href, filename, destination_folder):
        full_url = urljoin(self.config["base_url"], href) # the download URL
        dest_path = os.path.join(destination_folder, filename) # the destination path where the files will be saved

        print(f"Downloading {filename}...")
        try:
            # Set cookies from Selenium to requests session for the files will be accessible on download
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

            response = self.session.get(full_url, stream=True, verify=False)
            response.raise_for_status()  # Raise an exception for bad status codes

            # Only remove older files of the same type (price or promo)
            file_type_prefix = "priceFull" if filename.lower().startswith("pricefull") else "promoFull"
            for existing_file in os.listdir(destination_folder):
                if (
                    existing_file.endswith(".gz")
                    and existing_file != filename
                    and existing_file.lower().startswith(file_type_prefix.lower())
                ):
                    os.remove(os.path.join(destination_folder, existing_file))
                
            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Saved to {dest_path}")

            # Upload to S3
            s3_key = f"providers/{os.path.basename(destination_folder)}/{filename}"
            upload_file_to_s3(dest_path, s3_key)

        except requests.RequestException as e:
            print(f"Error downloading {filename}: {str(e)}")



if __name__ == "__main__":
    config = {
        "provider": "yohananof",
        "username": "yohananof",
        "login_url": "https://url.publishedprices.co.il/login",
        "file_list_url": "https://url.publishedprices.co.il/file",
        "base_url": "https://url.publishedprices.co.il"
    }

    crawler = YohananofCrawler(config)
    crawler.crawl()
    
