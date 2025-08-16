from base import CrawlerBase
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import re
from utils import (
    delete_file,
    download_file_from_link,
    extract_and_delete_gz,
)

# Regex to extract timestamp from filename like: PriceFull7290058140886-036-202508091200.gz
FILENAME_TS_RE = re.compile(r"PriceFull\d+-\d+-([0-9]{12})\.gz$", re.IGNORECASE)

def parse_dt_from_filename(href: str):
    """
    Extracts local datetime (Asia/Jerusalem) from filename like:
    .../PriceFull7290058140886-036-202508091200.gz
    Returns aware datetime or None.
    """
    m = FILENAME_TS_RE.search(href)
    if not m:
        return None
    ts = m.group(1)  # e.g., 202508091200
    try:
        dt_naive = datetime.strptime(ts, "%Y%m%d%H%M")
        return dt_naive.replace(tzinfo=ZoneInfo("Asia/Jerusalem"))
    except Exception:
        return None

class CerberusCrawler(CrawlerBase):
    def __init__(self, user_name):
        self.user_name = user_name

    def crawl(self, driver, name, uploaded_count=0):
        driver.get("https://url.publishedprices.co.il/login")

        # Login
        username_field = driver.find_element(By.ID, "username")
        username_field.send_keys(self.user_name)
        login_button = driver.find_element(By.ID, "login-button")
        login_button.click()
        time.sleep(5)

        # Filter
        try:
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            wait = WebDriverWait(driver, 10)
            search_input = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input.form-control.input-sm"))
            )
            search_input.clear()
            search_input.send_keys("pricefull")

            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table#fileList a[href^="/file/d/"]'))
            )
            time.sleep(1)
            
            print("Searched for 'pricefull' and results loaded.")
        except Exception as e:
            print(f"Search failed: {e}")

        # ---- Time filtering: only today ----
        now = datetime.now(ZoneInfo("Asia/Jerusalem"))
        today = now.date()
        
        # Start from beginning of today
        start_time = datetime.combine(today, datetime.min.time()).replace(tzinfo=ZoneInfo("Asia/Jerusalem"))

        print(f"Filtering files from today: {start_time} to {now}")

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        file_links = soup.select('table#fileList a[href^="/file/d/"]')
        files_paths = []

        for a_tag in file_links:
            file_link = a_tag["href"]
            if not file_link.startswith("http"):
                file_link = "https://url.publishedprices.co.il" + file_link
            
            # Parse datetime from filename and filter BEFORE downloading
            dt_local = parse_dt_from_filename(file_link)
            if not dt_local:
                print(f" Cannot parse datetime from filename: {file_link}")
                continue

            # Apply time filter - only today
            if dt_local.date() != today:
                print(f" File not from today: {file_link} (date: {dt_local.date()})")
                continue

            print(f" File from today: {file_link} (date: {dt_local.date()}, time: {dt_local.time()})")
            
            # Download the file only if it passes the filter
            try:
                file_path = download_file_from_link(file_link, driver=driver)
                if file_path:
                    file_path = extract_and_delete_gz(file_path)
                    success = CrawlerBase.upload_file_to_s3(self, file_path, s3_key=name)
                    delete_file(file_path)
                    if success:
                        uploaded_count += 1
                    files_paths.append(file_path)
            except Exception as e:
                print(f"Error downloading {file_link}: {e}")
                continue

        print(f"Total valid file links found (from today): {len(files_paths)}")
        return uploaded_count


    def get_driver(self):
        options = Options()
        options.add_argument("--headless=new")  # Use 'new' headless mode for Chrome 109+
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        return driver
