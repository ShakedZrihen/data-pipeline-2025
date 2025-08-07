import os
import re
import time
import ssl
from datetime import datetime
from typing import List, Dict
from urllib.parse import urlsplit, unquote
from pathlib import Path
import boto3

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm

# Suppress SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"
PROVIDERS = {
    "יוחננוף": {"username": "yohananof", "password": "", "folder": "yohananof"},
    "חצי חינם": {"username": "", "password": "", "folder": "hatzi-hinam"},
    "ויקטורי": {"username": "", "password": "", "folder": "victory"},
}

# S3 Setup
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)
S3_BUCKET = "gov-price-files-hanif-2025"

buckets = s3.list_buckets()
print("Buckets:", [bucket['Name'] for bucket in buckets['Buckets']])

try:
    s3.create_bucket(Bucket=S3_BUCKET)
    print(f"Created bucket: {S3_BUCKET}")
except s3.exceptions.BucketAlreadyExists:
    print(f"Bucket already exists: {S3_BUCKET}")
except Exception as e:
    print(f"Error creating bucket: {e}")

# Selenium Utilities
def init_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--start-maximized")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scroll_page_to_end(driver: webdriver.Chrome) -> None:
    last_height = 0
    while True:
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.4)
        current_height = driver.execute_script("return document.body.scrollHeight")
        if current_height == last_height:
            break
        last_height = current_height

def navigate_to_provider(driver: webdriver.Chrome, provider_name: str) -> None:
    print(f"Accessing page for {provider_name}...")
    driver.get(GOV_URL)
    scroll_page_to_end(driver)
    xpath = f"//tr[td[contains(normalize-space(.), '{provider_name}')]]//a[contains(., 'לצפייה במחירים')]"
    link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))
    driver.execute_script("arguments[0].click()", link)
    print("Navigated to prices page.")

def perform_login(driver: webdriver.Chrome, username: str, password: str) -> None:
    def _submit_login_form():
        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys(password)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        print("Login form submitted.")

    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "username")))
        print("Found login form in main document.")
        _submit_login_form()
        return
    except TimeoutException:
        pass

    for frame in driver.find_elements(By.TAG_NAME, "iframe"):
        driver.switch_to.frame(frame)
        try:
            if driver.find_elements(By.NAME, "username"):
                print("Found login form in iframe.")
                _submit_login_form()
                driver.switch_to.default_content()
                return
        finally:
            driver.switch_to.default_content()

    print("ℹNo login form required.")

def wait_for_files(driver: webdriver.Chrome) -> None:
    print("Loading file list...")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
                       "or contains(@href,'.csv') or contains(@href,'.gz')]")
        )
    )
    time.sleep(1.5)

def extract_file_links(driver: webdriver.Chrome) -> List[str]:
    print("Searching for downloadable files...")
    try:
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
            "or contains(@href,'.csv') or contains(@href,'.gz')]"
        )
        return list({link.get_attribute("href") for link in links if link.get_attribute("href")})
    except Exception as e:
        print(f"Error extracting links: {e}")
        return []

# File Processing
def get_timestamp_from_url(url: str) -> datetime:
    match = re.search(r"(\d{12})$", urlsplit(url).path.split("/")[-1])
    return datetime.strptime(match.group(1), "%Y%m%d%H%M") if match else datetime.min

def classify_file(url: str) -> str:
    filename = urlsplit(url).path.lower().split("/")[-1]
    if "promofull" in filename:
        return "promo"
    if "pricefull" in filename:
        return "price"
    return "other"

def select_recent_files(urls: List[str]) -> List[str]:
    print(f"Selecting recent files... {len(urls)}")
    price_files = [url for url in urls if classify_file(url) == "price"]
    promo_files = [url for url in urls if classify_file(url) == "promo"]

    print(f"Found {len(price_files)} price files and {len(promo_files)} promo files.")
    price_files.sort(key=get_timestamp_from_url, reverse=True)
    promo_files.sort(key=get_timestamp_from_url, reverse=True)
    
    selected = []
    if price_files:
        selected.append(price_files[0])
    if promo_files:
        selected.append(promo_files[0])
    elif len(price_files) > 1:
        selected.append(price_files[1])
    return selected

# Download Handling
def get_safe_filename(url: str) -> str:
    return unquote(os.path.basename(urlsplit(url).path))

def download_and_save_file(url: str, provider_folder: str) -> None:
    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        
        filename = get_safe_filename(url)
        timestamp = re.search(r"(\d{12})", filename)
        timestamp = timestamp.group(1) if timestamp else "000000000000"
        branch = re.search(r"(\d{13}-\d{3})", filename)
        branch = branch.group(1) if branch else "unknown_branch"
        file_type = "pricesFull" if "price" in filename.lower() else "promoFull"
        new_filename = f"{file_type}_{timestamp}.gz"

        folder_path = Path("downloads") / provider_folder / branch
        folder_path.mkdir(parents=True, exist_ok=True)
        local_path = folder_path / new_filename
        
        
        with requests.get(url, stream=True, verify=False, timeout=40) as response:
            response.raise_for_status()
            with open(local_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        print(f"Saved file to {local_path}")
        
        try:
            # Upload to S3
            s3_key = f"providers/{branch}/{new_filename}"
            s3.upload_file(local_path, S3_BUCKET, s3_key)
            print(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload to S3: {e}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def process_downloads(urls: List[str], provider_folder: str) -> None:
    if not urls:
        print("No files to download.")
        return
    for url in tqdm(urls, desc="Downloading"):
        print(f"Downloading {url}...")
        download_and_save_file(url, provider_folder)

# Provider Processing
def handle_provider(provider_name: str, config: Dict[str, str]) -> None:
    driver = init_driver()
    try:
        navigate_to_provider(driver, provider_name)
        perform_login(driver, config["username"], config["password"])
        wait_for_files(driver)
        links = extract_file_links(driver)
        print(f"Discovered {len(links)} files.")
        selected_urls = select_recent_files(links)
        print(f"Selected {len(selected_urls)} files for download.")
        process_downloads(selected_urls, config["folder"])
    finally:
        print("Closing browser.")
        try:
            driver.quit()
        except Exception:
            pass

def main() -> None:
    for provider_name, config in PROVIDERS.items():
        print(f"\nStarting processing for {provider_name}")
        try:
            handle_provider(provider_name, config)
        except Exception as e:
            print(f"Failed to process {provider_name}: {e}")

if __name__ == "__main__":
    main()