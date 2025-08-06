import os
import time
import platform
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import boto3
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    convert_xml_to_json,
    download_file_from_link,
    extract_and_delete_gz,
)

# --- S3 CONFIG ---
s3 = boto3.client("s3", region_name="us-east-1")
S3_BUCKET = "moranbenyamin-gov-prices"  # my bucket name

def init_chrome_options():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return chrome_options

def get_chromedriver_path():
    try:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            from webdriver_manager.core.os_manager import ChromeType
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()
        return driver_path
    except Exception:
        return "chromedriver"


from urllib.parse import urljoin
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def get_download_links_from_page(driver, download_base_url):
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "a[href$='.gz'], a[href$='.pdf'], a[href$='.xlsx'], a[href$='.csv']")
        )
    )

    links_elements = driver.find_elements(
        By.CSS_SELECTOR,
        "a[href$='.gz'], a[href$='.pdf'], a[href$='.xlsx'], a[href$='.csv']"
    )
    download_links = []
    for elem in links_elements:
        href = elem.get_attribute("href")
        if href.startswith("http"):
            download_links.append(href)
        else:
            download_links.append(urljoin(download_base_url, href))
    return download_links



def get_next_page_button(driver, current_page):
    try:
        next_page_num = current_page + 1
        next_button = driver.find_element(
            By.CSS_SELECTOR, f"button.paginationBtn[data-page='{next_page_num}']"
        )
        if next_button and next_button.is_enabled():
            return next_button
        return None
    except NoSuchElementException:
        return None

def upload_to_s3(local_path, branch_name, category_name):
    filename = os.path.basename(local_path)

    match = re.search(r"(\d{13})-(\d+)-(\d{12})", filename)
    if match:
        chain_id = match.group(1)
        branch_id = match.group(2)
        timestamp = match.group(3)
    else:
        from datetime import datetime
        chain_id = "unknown_chain"
        branch_id = branch_name.replace(" ", "_")
        timestamp = datetime.now().strftime("%Y%m%d%H%M")

    s3_filename = f"{category_name}_{timestamp}.gz"
    s3_key = f"providers/{chain_id}-{branch_id}/{s3_filename}"

    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")


def filter_latest_price_and_promo(links, category_value):
    """מחזיר את הקובץ האחרון של הקטגוריה הנתונה בלבד"""
    def extract_timestamp(url):
        match = re.search(r"(\d{12})", url)
        return match.group(1) if match else "000000000000"

    filtered = [l for l in links if category_value.lower() in l.lower()]
    filtered.sort(key=extract_timestamp, reverse=True)
    return [filtered[0]] if filtered else []



def crawl_category(driver, category_value, category_name, download_base_url, max_pages, branch_name):
    if category_value:
        try:
            category_select = Select(driver.find_element("id", "cat_filter"))
            category_select.select_by_value(category_value)
            time.sleep(3)
        except Exception:
            pass

    output_dir = os.path.join("prices", branch_name or "default_branch")
    os.makedirs(output_dir, exist_ok=True)

    total_successful = 0
    total_failed = 0
    page_num = 1

    while page_num <= max_pages:
        all_links = get_download_links_from_page(driver, download_base_url)
        selected_links = filter_latest_price_and_promo(all_links, category_value)
        if not selected_links:
            break

        for link in selected_links:
            output_path = download_file_from_link(link, output_dir)
            if output_path:
                try:
                    upload_to_s3(output_path, branch_name, category_name)
                    total_successful += 1
                except Exception:
                    total_failed += 1
            else:
                total_failed += 1

        break

    return {
        "category": category_name,
        "pages_processed": page_num,
        "successful_downloads": total_successful,
        "failed_downloads": total_failed,
        "output_dir": output_dir,
    }

def crawl(start_url, download_base_url, login_function=None, categories=None, max_pages=2):
    chrome_options = init_chrome_options()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        driver.get(start_url)
        
        if "cpfta_prices_regulations" in start_url:
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href]"))
            )
            all_links = [a.get_attribute("href") for a in driver.find_elements(By.CSS_SELECTOR, "a[href]")]
            matching_links = [l for l in all_links if download_base_url in l]
            if matching_links:
                driver.get(matching_links[0])
            else:
                raise Exception(f"link does not match {download_base_url}")
            
            if login_function:
                login_function(driver)

        try:
            branch_select = Select(driver.find_element("id", "branch_filter"))
            branches = [
                option.get_attribute("value")
                for option in branch_select.options
                if option.get_attribute("value")
            ]
        except Exception:
            branches = [None]

        categories = categories or [
            {"value": "pricefull", "name": "pricesFull"},
            {"value": "promofull", "name": "promoFull"},
        ]

        all_results = []

        for branch_value in branches:
            branch_name = "default_branch"
            if branch_value:
                branch_select.select_by_value(branch_value)
                branch_name = branch_select.first_selected_option.text.strip()
                time.sleep(3)

            for category in categories:
                result = crawl_category(
                    driver=driver,
                    category_value=category["value"],
                    category_name=category["name"],
                    download_base_url=download_base_url,
                    max_pages=max_pages,
                    branch_name=branch_name,
                )
                all_results.append(result)

        return all_results

    finally:
        driver.quit()
