import os
import time
import re
import sys
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC

import boto3
from botocore.config import Config

# Allow "from utils import ..." when running from subpackages
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import (
    convert_xml_to_json,   # לא בשימוש כרגע, אפשר להסיר אם תרצי
    download_file_from_link,
    extract_and_delete_gz, # לא בשימוש כרגע, אפשר להסיר אם תרצי
)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "test-bucket")
S3_FORCE_PATH_STYLE = os.getenv("S3_FORCE_PATH_STYLE", "true").lower() == "true"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    config=Config(signature_version="s3v4",
                  s3={"addressing_style": "path" if S3_FORCE_PATH_STYLE else "auto"}),
)

def ensure_bucket():
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=S3_BUCKET)

SELENIUM_REMOTE_URL = os.getenv("SELENIUM_REMOTE_URL", "http://selenium:4444/wd/hub")

def init_chrome_options():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    return opts

def make_driver():
    return webdriver.Remote(command_executor=SELENIUM_REMOTE_URL,
                            options=init_chrome_options())

def get_download_links_from_page(driver, download_base_url):
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR,
            "a[href$='.gz'], a[href$='.pdf'], a[href$='.xlsx'], a[href$='.csv']"))
    )
    link_elems = driver.find_elements(By.CSS_SELECTOR,
        "a[href$='.gz'], a[href$='.pdf'], a[href$='.xlsx'], a[href$='.csv']")
    links = []
    for elem in link_elems:
        href = elem.get_attribute("href")
        if not href:
            continue
        links.append(href if href.startswith("http") else urljoin(download_base_url, href))
    return links

def get_next_page_button(driver, current_page):
    try:
        next_button = driver.find_element(
            By.CSS_SELECTOR, f"button.paginationBtn[data-page='{current_page + 1}']"
        )
        return next_button if next_button and next_button.is_enabled() else None
    except NoSuchElementException:
        return None

# ---------- File name parsing helpers ----------

FILE_RE = re.compile(
    r'(?P<cat>pricefull|promofull)\D*(?P<chain>\d{13})-(?P<store>\d+)-(?P<ts>\d{12})',
    re.IGNORECASE
)

def parse_link(u: str):
    """
    Returns (category, chain_id, store_id, timestamp) or None.
    """
    m = FILE_RE.search(u)
    if not m:
        return None
    return (
        m.group('cat').lower(),   # 'pricefull' | 'promofull'
        m.group('chain'),         # 13 digits
        m.group('store'),         # store id as string
        m.group('ts'),            # 12-digit timestamp
    )

def select_latest_per_branch(links, category_value: str):
    """
    For pages WITHOUT a branch dropdown:
    group by store id and select newest ts per store (per category).
    """
    wanted = category_value.lower()
    best_by_store = {}  # store_id -> (ts, url)
    for url in links:
        p = parse_link(url)
        if not p:
            continue
        cat, _chain, store, ts = p
        if cat != wanted:
            continue
        cur = best_by_store.get(store)
        if (cur is None) or (ts > cur[0]):
            best_by_store[store] = (ts, url)
    return [t[1] for t in best_by_store.values()]

def filter_by_category_and_store(links, category_value: str, expected_store: str | None):
    """
    For pages WITH a branch dropdown (or when you know the store id):
    pick newest file for the given store id and category.
    """
    wanted = category_value.lower()
    cand = []
    for url in links:
        p = parse_link(url)
        if not p:
            continue
        cat, _chain, store, ts = p
        if cat != wanted:
            continue
        if expected_store is None or str(store) == str(expected_store):
            cand.append((ts, url))
    cand.sort(key=lambda x: x[0], reverse=True)
    return [cand[0][1]] if cand else []

# ---------- S3 upload (filename-only parsing, no XML) ----------

def upload_to_s3(local_path, branch_name, category_name):
    """
    Save as: providers/<chain>/<store>/<category>_<timestamp>.gz
    where:
      - <chain>, <store>, <timestamp> are parsed from the filename.
      - <category> is 'pricesFull' / 'promoFull' (as passed in).
    """
    filename = os.path.basename(local_path)

    chain_id = None
    branch_id = None
    timestamp = None

    m = re.search(r"(\d{13})-(\d+)-(\d{12})", filename)
    if m:
        chain_id, branch_id, timestamp = m.group(1), m.group(2), m.group(3)

    # Fallbacks (למקרה נדיר שאין התאמה בשם הקובץ)
    if not chain_id:
        chain_id = "unknown_chain"
    if not branch_id:
        branch_id = (branch_name or "default_branch").replace(" ", "_")
    if not timestamp:
        m_ts = re.search(r"(\d{12})", filename)
        timestamp = m_ts.group(1) if m_ts else datetime.now().strftime("%Y%m%d%H%M")

    s3_filename = f"{category_name}_{timestamp}.gz"
    s3_key = f"providers/{chain_id}/{branch_id}/{s3_filename}"
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")

# ---------- Crawl helpers ----------

def has_branch_dropdown(driver):
    try:
        driver.find_element(By.ID, "branch_filter")
        return True
    except NoSuchElementException:
        return False

def crawl_category(
    driver,
    category_value,
    category_name,
    download_base_url,
    max_pages,
    branch_name,
    expected_store=None,
    use_latest_per_branch=False,
):
    """
    If use_latest_per_branch=True (no dropdown pages):
        - selects newest file per store for the given category (one per branch)
    Else:
        - selects newest file for the expected_store & category (single file)
    """
    if category_value:
        try:
            Select(driver.find_element("id", "cat_filter")).select_by_value(category_value)
            time.sleep(2)
        except Exception:
            pass

    output_dir = os.path.join("prices", (branch_name or "default_branch"))
    os.makedirs(output_dir, exist_ok=True)

    total_successful = 0
    total_failed = 0
    page_num = 1

    while page_num <= max_pages:
        all_links = get_download_links_from_page(driver, download_base_url)

        if use_latest_per_branch:
            selected_links = select_latest_per_branch(all_links, category_value)
        else:
            selected_links = filter_by_category_and_store(all_links, category_value, expected_store)

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
        break  # we select exactly what we need in one shot

    return {
        "category": category_name,
        "pages_processed": page_num,
        "successful_downloads": total_successful,
        "failed_downloads": total_failed,
        "output_dir": output_dir,
    }

def crawl(start_url, download_base_url, login_function=None, categories=None, max_pages=2):
    ensure_bucket()

    driver = make_driver()
    try:
        driver.get(start_url)

        if "cpfta_prices_regulations" in start_url:
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href]")))
            all_links = [a.get_attribute("href") for a in driver.find_elements(By.CSS_SELECTOR, "a[href]")]
            matching_links = [l for l in all_links if l and download_base_url in l]
            if matching_links:
                driver.get(matching_links[0])
            else:
                raise Exception(f"link does not match {download_base_url}")
            if login_function:
                login_function(driver)

        dropdown_present = has_branch_dropdown(driver)

        branches = [None]
        branch_select = None
        if dropdown_present:
            try:
                branch_select = Select(driver.find_element("id", "branch_filter"))
                branches = [opt.get_attribute("value") for opt in branch_select.options if opt.get_attribute("value")]
            except Exception:
                branches = [None]  # defensive

        categories = categories or [
            {"value": "pricefull", "name": "pricesFull"},
            {"value": "promofull", "name": "promoFull"},
        ]

        all_results = []

        if dropdown_present:
            # Branch-aware flow (e.g., Carrefour)
            for branch_value in branches:
                branch_name = "default_branch"
                if branch_value and branch_select:
                    branch_select.select_by_value(branch_value)
                    try:
                        branch_name = branch_select.first_selected_option.text.strip()
                    except Exception:
                        pass
                    time.sleep(2)

                for category in categories:
                    result = crawl_category(
                        driver=driver,
                        category_value=category["value"],
                        category_name=category["name"],
                        download_base_url=download_base_url,
                        max_pages=max_pages,
                        branch_name=branch_name,
                        expected_store=branch_value,
                        use_latest_per_branch=False,
                    )
                    all_results.append(result)
        else:
            # No dropdown (e.g., Yohananof / Tiv Taam): newest per branch for each category
            branch_name = "all_branches"
            for category in categories:
                result = crawl_category(
                    driver=driver,
                    category_value=category["value"],
                    category_name=category["name"],
                    download_base_url=download_base_url,
                    max_pages=max_pages,
                    branch_name=branch_name,
                    expected_store=None,
                    use_latest_per_branch=True,
                )
                all_results.append(result)

        return all_results
    finally:
        driver.quit()
