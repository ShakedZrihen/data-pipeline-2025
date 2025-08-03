# -*- coding: utf-8 -*-
"""
crawler.py – הורדת קבצי Price/Promo מהעמוד:
https://www.gov.il/he/pages/cpfta_prices_regulations
"""

import os
import re
import time
import ssl
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
from selenium.common.exceptions import (
    NoSuchWindowException,
    TimeoutException,
)
from webdriver_manager.chrome import ChromeDriverManager

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ------------ S3 Setup ------------ #
s3 = boto3.client("s3", region_name="us-east-1")
S3_BUCKET = "gov-price-files-hanif-2025"

# ------------ Providers ------------ #
GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"

PROVIDERS = {
    "יוחננוף": {"username": "yohananof", "password": "", "folder": "yohananof"},
    "שופרסל": {"username": "", "password": "", "folder": "shufersal"},
    "ויקטורי": {"username": "", "password": "", "folder": "victory"},
}

# ------------ Selenium Utils ------------ #

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

    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "username")))
        print(f"🔐 Login form detected (main doc).")
        _fill_form()
        return
    except TimeoutException:
        pass

    for fr in driver.find_elements(By.TAG_NAME, "iframe"):
        driver.switch_to.frame(fr)
        if driver.find_elements(By.NAME, "username"):
            print("🔐 Login form detected (inside iframe).")
            _fill_form()
            driver.switch_to.default_content()
            return
        driver.switch_to.default_content()

    print("ℹ️ No login required for this provider.")

def wait_for_file_page(driver: webdriver.Chrome):
    print("⏳ Waiting for file list to appear…")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
                       "or contains(@href,'.csv') or contains(@href,'.gz')]"))
    )
    time.sleep(1.5)

def find_download_links(driver: webdriver.Chrome) -> List[str]:
    print("🔎 Scanning for downloadable files…")
    try:
        links = driver.find_elements(By.XPATH,
            "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
            "or contains(@href,'.csv') or contains(@href,'.gz')]")
        return list({l.get_attribute("href") for l in links if l.get_attribute("href")})
    except NoSuchWindowException:
        print("❌ Browser window was closed unexpectedly.")
        return []

# ------------ File Filters ------------ #

DATE_RE = re.compile(r"(\d{12})$")

def _timestamp(url: str) -> datetime:
    m = DATE_RE.search(url.split("/")[-1])
    return datetime.strptime(m.group(1), "%Y%m%d%H%M") if m else datetime.min

def _classify(url: str) -> str:
    name = url.split("/")[-1].lower()
    if "promo" in name:
        return "promo"
    if "price" in name:
        return "price"
    return "other"

def filter_recent_price_and_promo(urls: List[str]) -> List[str]:
    price_files = [u for u in urls if _classify(u) == "price"]
    promo_files = [u for u in urls if _classify(u) == "promo"]

    price_files.sort(key=_timestamp, reverse=True)
    promo_files.sort(key=_timestamp, reverse=True)

    selected = []
    if price_files:
        selected.append(price_files[0])
    if promo_files:
        selected.append(promo_files[0])
    elif len(price_files) > 1:
        selected.append(price_files[1])
    return selected

# ------------ Download + Upload ------------ #

def _safe_filename(url: str) -> str:
    path = urlsplit(url).path
    return unquote(os.path.basename(path))

def download_file(url: str, provider_folder: str):
    ssl._create_default_https_context = ssl._create_unverified_context

    try:
        filename = _safe_filename(url)

        # Extract timestamp
        timestamp_match = re.search(r"(\d{12})", filename)
        timestamp = timestamp_match.group(1) if timestamp_match else "000000000000"

        # Extract branch name
        branch_match = re.search(r"(\d{13}-\d{3})", filename)
        branch_name = branch_match.group(1) if branch_match else "unknown_branch"

        # Determine file type
        ftype = "pricesFull" if "price" in filename.lower() else "promoFull"
        new_filename = f"{ftype}_{timestamp}.gz"

        # Local path
        folder_path = os.path.join("downloads", provider_folder, branch_name)
        os.makedirs(folder_path, exist_ok=True)
        local_path = os.path.join(folder_path, new_filename)

        # Download
        with requests.get(url, stream=True, verify=False, timeout=40) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        print(f"✅ Downloaded to {local_path}")

        # Upload to S3
        s3_key = f"providers/{branch_name}/{new_filename}"
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"🟢 Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")

    except Exception as exc:
        print(f"❌ Failed to process {url}: {exc}")

def download_files(urls: List[str], provider_folder: str):
    if not urls:
        print("⚠️ No downloadable links found.")
        return
    for url in tqdm(urls, desc="⬇️ Downloading files"):
        download_file(url, provider_folder)

# ------------ Main Provider Runner ------------ #

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
        download_files(chosen, info["folder"])
    finally:
        print("🧹 Closing browser.")
        try:
            driver.quit()
        except Exception:
            pass

# ------------ Main ------------ #

def main():
    for provider, cfg in PROVIDERS.items():
        print(f"\n🚀 Processing provider: {provider}")
        try:
            process_provider(provider, cfg)
        except Exception as e:
            print(f"❌ Error processing {provider}: {e}")

if __name__ == "__main__":
    main()
