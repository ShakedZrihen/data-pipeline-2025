import os
import time
import re
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import boto3
from botocore.config import Config
import shutil

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/downloads")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }
    opts.add_experimental_option("prefs", prefs)
    return opts


def make_driver():
    opts = init_chrome_options()
    return webdriver.Remote(command_executor=SELENIUM_REMOTE_URL, options=opts)


def find_download_elements(driver):
    selectors = [
        "a.downloadBtn",
        "button.downloadBtn",
        "a[href$='.gz']",
        "a[href$='.xml']",
        "a[href*='download']",
        "a[href*='/file?']",
        "button[data-href*='.gz']",
        "button[data-href*='/file?']",
        "a:has(span.downloadBtn)",
    ]
    elems = []
    for sel in selectors:
        try:
            elems += driver.find_elements(By.CSS_SELECTOR, sel)
        except Exception as e:
            print(f"[WARN] Failed selector '{sel}': {e}")

    seen, uniq = set(), []
    for e in elems:
        try:
            key = (e.tag_name, e.get_attribute("href") or e.get_attribute("data-href") or e.text)
            if key not in seen:
                seen.add(key)
                uniq.append(e)
        except Exception as e:
            print(f"[WARN] Failed to extract key from element: {e}")

    print(f"[DEBUG] find_download_elements: found {len(uniq)} candidates")
    return uniq


def wait_for_new_download(download_dir: str, before_set: set[str], timeout=120):

    end = time.time() + timeout
    if not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)

    while time.time() < end:
        time.sleep(0.5)
        all_files = {os.path.join(download_dir, filename) for filename in os.listdir(download_dir)}
        new_files = [f for f in all_files if f not in before_set]
        for f in new_files:
            if not f.endswith(".crdownload") and os.path.getsize(f) > 0:
                cr = f + ".crdownload"
                if not os.path.exists(cr):
                    return f
    return None


def click_and_download(driver, el, download_dir: str, timeout=120):

    before = set()
    if not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)
    for filename in os.listdir(download_dir):
        full_path = os.path.join(download_dir, filename)
        before.add(full_path)

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    time.sleep(0.3)

    try:
        el.click()

    except Exception:
        driver.execute_script("arguments[0].click();", el)

    handles_before = driver.window_handles
    time.sleep(0.5)
    handles_after = driver.window_handles
    if len(handles_after) > len(handles_before):
        driver.switch_to.window(handles_after[-1])
        time.sleep(1.5)
        driver.close()
        driver.switch_to.window(handles_before[0])    

    downloaded = wait_for_new_download(download_dir, before, timeout=timeout)
    return downloaded


def crawl_category(driver, category_value, category_name, max_pages, branch_name, use_latest_per_branch=False):

    if category_value:
        try:
            Select(driver.find_element("id", "cat_filter")).select_by_value(category_value)
            time.sleep(3)
        except Exception:
            pass

    output_dir = os.path.join("prices", (branch_name or "default_branch"))
    os.makedirs(output_dir, exist_ok=True)

    total_successful = 0
    total_failed = 0
    page_num = 1

    while page_num <= max_pages:
        time.sleep(5)
        all_elems = find_download_elements(driver)
        if use_latest_per_branch:
            hrefs = []
            for e in all_elems:
                href = e.get_attribute("href") or e.get_attribute("data-href")
                if href:
                    hrefs.append(href)
            selected_hrefs = set(select_latest_per_branch(hrefs, category_value))
            elems = []
            for e in all_elems:
                href = e.get_attribute("href") or e.get_attribute("data-href")
                if href in selected_hrefs:
                    elems.append(e)
        else:
            elems = all_elems

        if not elems:
            break

        for el in elems:
            try:
                path = click_and_download(driver, el, DOWNLOAD_DIR, timeout=180)
                if not path:
                    total_failed += 1
                    print("Download did not complete in time.")
                    continue

                filename = os.path.basename(path)
                local_path = os.path.join(output_dir, filename)
                shutil.copy2(path, local_path)

                if is_gzip_file(local_path):
                    try:
                        upload_to_s3(local_path, branch_name, category_name)
                        total_successful += 1
                    except Exception as e:
                        print(f"Upload failed for {local_path}: {e}")
                        total_failed +=1
                else:
                    total_failed += 1

            except Exception as e:
                print(f"Download/Upload failed: {e}")
                total_failed += 1

        next_btn = get_next_page_button(driver, page_num)
        if next_btn and next_btn.is_enabled():
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(2)
            page_num += 1

        else:
            break

    print(f"[SUMMARY- CRAWLER] Category={category_name}, Pages={page_num}, Success={total_successful}, Failed={total_failed}")
    return {
        "category": category_name,
        "pages_processed": page_num,
        "successful_downloads": total_successful,
        "failed_downloads": total_failed,
        "output_dir": output_dir,
    }

def get_next_page_button(driver, current_page):
    try:
        next_button = driver.find_element(
            By.CSS_SELECTOR, f"button.paginationBtn[data-page='{current_page + 1}']"
        )
        return next_button if next_button and next_button.is_enabled() else None
    except NoSuchElementException:
        return None


FILE_RE = re.compile(
    r'(?P<cat>pricefull|promofull)\D*(?P<chain>\d{13})-(?P<store>\d+)-(?P<ts>\d{12})',
    re.IGNORECASE
)

def parse_link(u: str):
    m = FILE_RE.search(u)
    if not m:
        return None
    return (
        m.group('cat').lower(),
        m.group('chain'), 
        m.group('store'),
        m.group('ts'),
    )

def select_latest_per_branch(links, category_value: str):
    wanted = category_value.lower()
    best_by_store = {} 
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


def is_gzip_file(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(2) == b"\x1f\x8b"
    except Exception:
        return False



def upload_to_s3(local_path, branch_name, category_name):
    filename = os.path.basename(local_path)
    chain_id = None
    branch_id = None
    timestamp = None
    match = re.search(r"(\d{13})-(\d+)-(\d{12})", filename)
    if match:
        chain_id, branch_id, timestamp = match.group(1), match.group(2), match.group(3)
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


def has_branch_dropdown(driver):
    try:
        driver.find_element(By.ID, "branch_filter")
        return True
    except NoSuchElementException:
        return False


def crawl(start_url, login_function=None, categories=None, max_pages=2):
    ensure_bucket()
    driver = make_driver()
    try:
        if login_function:
            print("[DEBUG] calling login_function…")
            login_function(driver)
        else:
            print(f"[DEBUG] navigating to start_url: {start_url}")
            driver.get(start_url)

        print("[DEBUG] arrived:", driver.current_url)

        dropdown_present = has_branch_dropdown(driver)
        print("[DEBUG] has_branch_dropdown:", dropdown_present)

        branches = [None]
        branch_select = None
        if dropdown_present:
            try:
                branch_select = Select(driver.find_element("id", "branch_filter"))
                branches = [opt.get_attribute("value") for opt in branch_select.options if opt.get_attribute("value")]
                print(f"[DEBUG] found {len(branches)} branches:", branches[:5])
            except Exception as e:
                print(f"[DEBUG] branch dropdown error: {e}")
                branches = [None]

        categories = categories or [
            {"value": "pricefull", "name": "pricesFull"},
            {"value": "promofull", "name": "promoFull"},
        ]

        all_results = []

        if dropdown_present:
            # CARREFOUR
            for branch_value in branches:
                branch_name = "default_branch"
                if branch_value and branch_select:
                    branch_select.select_by_value(branch_value)
                    try:
                        branch_name = branch_select.first_selected_option.text.strip()
                    except Exception as e:
                        print(f"[WARN] Failed to read selected branch name: {e}")
                    time.sleep(2)

                for category in categories:
                    result = crawl_category(
                        driver=driver,
                        category_value=category["value"],
                        category_name=category["name"],
                        max_pages=max_pages,
                        branch_name=branch_name,
                        use_latest_per_branch=False,
                    )
                    all_results.append(result)
        else:
            # ALL OTHERS
            branch_name = "all_branches"
            for category in categories:
                result = crawl_category(
                    driver=driver,
                    category_value=category["value"],
                    category_name=category["name"],
                    max_pages=max_pages,
                    branch_name=branch_name,
                    use_latest_per_branch=True,
                )
                all_results.append(result)

        return all_results
    finally:
        driver.quit()
