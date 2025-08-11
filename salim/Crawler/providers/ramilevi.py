import sys, os
import requests
import json
import time
import urllib3
import re
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


LOGIN_URL = "https://url.publishedprices.co.il/login"
PAGE_URL = "https://url.publishedprices.co.il/file"
BASE_HOST = "https://url.publishedprices.co.il"

USERNAME = "RamiLevi"
USER_SEL = "input[name='username']"
SUBMIT_SEL = "#login-button, .row button"

PRICE_TOKEN = "pricefull"
PROMO_TOKEN = "promofull"

SLUG = "ramilevi"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)


def _login_and_get_links(driver):
    driver.get(LOGIN_URL)

    # 1. Wait for username field and login
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, USER_SEL))
    )
    driver.find_element(By.CSS_SELECTOR, USER_SEL).send_keys(USERNAME)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, SUBMIT_SEL))
    ).click()

    # 2. Navigate to file page and wait for the table to load
    time.sleep(2.0)
    driver.get(PAGE_URL)

    # 3. Wait for the table itself to be visible
    WebDriverWait(driver, 15).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "#fileList"))
    )

    # 4. CRITICAL FIX: Wait for the table's content (the rows) to be loaded
    try:
        WebDriverWait(driver, 10).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, "#fileList tbody tr")) > 0
        )
    except Exception as e:
        print(f"[WARN] No files found in the table body. {e}")
        return []

    # 5. Collect the links directly from the driver
    links = []
    link_elements = driver.find_elements(By.CSS_SELECTOR, "#fileList a[href$='.gz'], #fileList a[href$='.GZ']")
    
    for el in link_elements:
        href = el.get_attribute("href")
        if href:
            links.append(href)
    
    return links

def _split_price_promo(links):
    prices = [u for u in links if PRICE_TOKEN in u.lower()]
    promos = [u for u in links if PROMO_TOKEN in u.lower()]
    return prices, promos


def _session_from_driver(driver) -> requests.Session:
    """Creates a requests.Session with the browser's cookies and User-Agent."""
    s = requests.Session()
    for c in driver.get_cookies():
        s.cookies.set(
            name=c.get("name"),
            value=c.get("value"),
            domain=c.get("domain"),
            path=c.get("path", "/")
        )
    try:
        ua = driver.execute_script("return navigator.userAgent;")
        s.headers.update({"User-Agent": ua})
    except Exception:
        pass
    return s


def _download_files(session: requests.Session, urls, dest_folder):
    os.makedirs(dest_folder, exist_ok=True)
    for url in urls:
        filename = os.path.basename(urlparse(url).path)
        out_path = os.path.join(dest_folder, filename)
        print(f"Downloading {filename} ...")
        try:
            with session.get(url, stream=True, timeout=120, verify=False) as r:
                r.raise_for_status()
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(1024 * 64):
                        if chunk:
                            f.write(chunk)
        except Exception as e:
            print(f"[WARN] failed: {url} -> {e}")


def run(ts: str):
    os.makedirs("out", exist_ok=True)
    out_prices = os.path.join("out", f"{SLUG}_prices_{ts}.json")
    out_promos = os.path.join("out", f"{SLUG}_promos_{ts}.json")
    out_folder = os.path.join("out", f"{SLUG}_{ts}")

    driver = _driver()
    try:
        links = _login_and_get_links(driver)
        prices, promos = _split_price_promo(links)

        with open(out_prices, "w", encoding="utf-8") as f:
            json.dump(prices, f, ensure_ascii=False, indent=2)
        with open(out_promos, "w", encoding="utf-8") as f:
            json.dump(promos, f, ensure_ascii=False, indent=2)

        if not links:
            print("[ERROR] No download links found. The output files and folder will be empty.")
            return out_prices, out_promos

        session = _session_from_driver(driver)
        _download_files(session, prices, out_folder)
        _download_files(session, promos, out_folder)

        return out_prices, out_promos
    finally:
        driver.quit()


if __name__ == "__main__":
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    print("\n=== RamiLevi ===\n")
    p, q = run(ts)
    print("saved:", p)
    print("saved:", q)