import os
import re
import time
import ssl
import gzip
import shutil
from datetime import datetime
from typing import List
from urllib.parse import urlsplit, unquote

import requests
import urllib3
import boto3
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# Disable insecure request warnings (you were already doing this)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ------------ AWS S3 ------------ #
s3 = boto3.client("s3", region_name="us-east-1")
S3_BUCKET = "gov-price-files-hanif-2025"

# ------------ Target ------------ #
GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

PROVIDERS = {
    "יוחננוף": {"username": "yohananof", "password": "", "folder": "yohananof"},
    "שופרסל": {"username": "", "password": "", "folder": "shufersal"},
    "ויקטורי": {"username": "", "password": "", "folder": "victory"},
}

# ------------ Selenium ------------ #
def setup_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

def scroll_to_bottom(driver: webdriver.Chrome):
    last_height = 0
    while True:
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.4)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def go_to_provider_page(driver: webdriver.Chrome, provider_name: str):
    print(f"🔍 Navigating to gov.il page for {provider_name}…")
    driver.get(GOV_URL)
    scroll_to_bottom(driver)
    xpath = f"//tr[td[contains(normalize-space(.), '{provider_name}')]]//a[contains(., 'לצפייה במחירים')]"
    link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))
    driver.execute_script("arguments[0].click()", link)
    print("✅ Clicked on 'לצפייה במחירים'.")

def login_to_provider(driver: webdriver.Chrome, username: str, password: str):
    def _fill_form():
        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys(password)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        print("✅ Login submitted.")

    # main doc
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "username")))
        print("🔐 Login form detected (main doc).")
        _fill_form()
        return
    except TimeoutException:
        pass

    # in iframes
    for fr in driver.find_elements(By.TAG_NAME, "iframe"):
        driver.switch_to.frame(fr)
        try:
            if driver.find_elements(By.NAME, "username"):
                print("🔐 Login form detected (inside iframe).")
                _fill_form()
                driver.switch_to.default_content()
                return
        finally:
            driver.switch_to.default_content()

    print("ℹ️ No login required for this provider.")

def wait_for_file_page(driver: webdriver.Chrome):
    print("⏳ Waiting for file list to appear…")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') or contains(@href,'.csv') or contains(@href,'.gz')]")
        )
    )
    time.sleep(1.2)

def find_download_links(driver: webdriver.Chrome) -> List[str]:
    print("🔎 Scanning for downloadable files…")
    try:
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') or contains(@href,'.csv') or contains(@href,'.gz')]",
        )
        return list({l.get_attribute("href") for l in links if l.get_attribute("href")})
    except NoSuchWindowException:
        print("❌ Browser window was closed unexpectedly.")
        return []

# ------------ Requests session from Selenium cookies ------------ #
def make_session_from_driver(driver) -> requests.Session:
    s = requests.Session()
    for c in driver.get_cookies():
        # Some sites require domain; some don't — set domain if available.
        s.cookies.set(c["name"], c["value"], domain=c.get("domain"))
    s.verify = False  # match your previous verify=False behavior
    return s

# ------------ File helpers ------------ #
DATE_RE = re.compile(r"(\d{12})$")
BRANCH_RE = re.compile(r"(\d{13}-\d{3})")

def _safe_filename(url: str) -> str:
    path = urlsplit(url).path
    return unquote(os.path.basename(path))

def _classify(url: str) -> str:
    name = url.split("/")[-1].lower()
    if "promo" in name:
        return "promo"
    if "price" in name:
        return "price"
    return "other"

def _timestamp_from_name(name: str) -> str:
    m = re.search(r"(\d{12})", name)
    if m:
        return m.group(1)
    # fallback to "now" UTC in the compact format
    return datetime.utcnow().strftime("%Y%m%d%H%M")

def _branch_from_name(name: str) -> str:
    m = BRANCH_RE.search(name)
    return m.group(1) if m else "unknown_branch"

def filter_recent_price_and_promo(urls: List[str]) -> List[str]:
    def _ts(url: str):
        m = DATE_RE.search(url.split("/")[-1])
        try:
            return datetime.strptime(m.group(1), "%Y%m%d%H%M") if m else datetime.min
        except Exception:
            return datetime.min

    price_files = [u for u in urls if _classify(u) == "price"]
    promo_files = [u for u in urls if _classify(u) == "promo"]

    price_files.sort(key=_ts, reverse=True)
    promo_files.sort(key=_ts, reverse=True)

    selected = []
    if price_files:
        selected.append(price_files[0])
    if promo_files:
        selected.append(promo_files[0])
    elif len(price_files) > 1:
        selected.append(price_files[1])
    return selected

# ------------ Download + Upload (robust) ------------ #
def download_file(url: str, provider_folder: str, session: requests.Session | None = None):
    ssl._create_default_https_context = ssl._create_unverified_context
    sess = session or requests.Session()

    try:
        r = sess.get(url, stream=True, timeout=40)
        r.raise_for_status()
        raw = r.content
        ctype = (r.headers.get("Content-Type") or "").lower()

        # HTML guard — don’t save/upload login/error pages
        head = raw[:2048].lstrip().lower()
        if "text/html" in ctype or head.startswith(b"<!") or b"<html" in head:
            print(f"⚠️  HTML instead of data for {url} — skipping.")
            return

        source_name = _safe_filename(url)
        branch_name = _branch_from_name(source_name)
        timestamp = _timestamp_from_name(source_name)
        ftype = "pricesFull" if "price" in source_name.lower() else "promoFull"
        new_filename = f"{ftype}_{timestamp}.gz"

        # ensure gzip
        if len(raw) >= 2 and raw[:2] == b"\x1f\x8b":
            gz_bytes = raw
        else:
            gz_bytes = gzip.compress(raw)

        # local save (your existing tree)
        folder_path = os.path.join("downloads", provider_folder, branch_name)
        os.makedirs(folder_path, exist_ok=True)
        local_path = os.path.join(folder_path, new_filename)
        with open(local_path, "wb") as f:
            f.write(gz_bytes)
        print(f"✅ Downloaded to {local_path}")

        # S3 upload (explicit Body)
        s3_key = f"providers/{provider_folder}/{branch_name}/{new_filename}"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=gz_bytes)
        print(f"🟢 Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")

    except Exception as exc:
        print(f"❌ Failed to process {url}: {exc}")

def download_files(urls: List[str], provider_folder: str, session: requests.Session | None = None):
    if not urls:
        print("⚠️ No downloadable links found.")
        return
    for url in tqdm(urls, desc=f"⬇️ Downloading files for {provider_folder}"):
        download_file(url, provider_folder, session=session)

# ------------ Orchestration ------------ #
def process_provider(provider_name: str, info: dict):
    driver = setup_driver()
    try:
        go_to_provider_page(driver, provider_name)
        login_to_provider(driver, info["username"], info["password"])
        wait_for_file_page(driver)

        links = find_download_links(driver)
        print(f"🔗 Found {len(links)} downloadable files.")
        chosen = filter_recent_price_and_promo(links)
        print(f"🎯 Selected {len(chosen)} recent price/promo files.")

        session = make_session_from_driver(driver)
        download_files(chosen, info["folder"], session=session)
    finally:
        print("🧹 Closing browser.")
        try:
            driver.quit()
        except Exception:
            pass

def main(only: List[str] | None = None):
    for provider, cfg in PROVIDERS.items():
        if only and provider not in only:
            continue
        print(f"\n🚀 Processing provider: {provider}")
        try:
            process_provider(provider, cfg)
        except Exception as e:
            print(f"❌ Error processing {provider}: {e}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", help="Process only these provider display names (e.g. --only יוחננוף ויקטורי)")
    args = ap.parse_args()
    main(only=args.only)
