#!/usr/bin/env python3
"""
Cleaned crawler: yohananof (login) + king-like providers (kingstore/maayan).
Downloads latest 2 price + latest 2 promo .gz files per provider.
"""

import os
import re
import time
import logging
import certifi
import requests
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, unquote

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

import boto3  # ADDED for AWS S3 upload

# ---------- CONFIG ----------
BASE_LISTING_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"
DOWNLOAD_DIR = os.path.join(os.getcwd(), "providers")
HEADLESS = True
VERIFY_SSL = True

TARGET_STORES = [
    ("מ. יוחננוף", "yohananof"),
    ("אלמשהדאוי קינג סטור", "kingstore"),
    ("ג.מ מעיין אלפיים", "maayan"),
]

YOHANANOF_USERNAME = "yohananof"
YOHANANOF_PASSWORD = ""

PAGE_WAIT = 0.6

# AWS S3 CONFIG 
BUCKET_NAME = "govil-price-lists" 

# Create the S3 client 
s3_client = boto3.client("s3")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger("crawler")

# regexes
ABS_DATE_RE = re.compile(r"(\d{1,2}):(\d{2})\s+(\d{1,2})/(\d{1,2})/(\d{4})")
REL_HE_RE = re.compile(r"לפני\s*(\d+)?\s*(שנייה|שניות|דקה|דקות|שעה|שעות|יום|ימים)", re.I)
GZ_ONCLICK_RE = re.compile(r"Download\(['\"]([^'\"]+\.gz)['\"]\)", re.I)

# ---------- helpers ----------
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def parse_absolute_he_date(text):
    if not text:
        return None
    m = ABS_DATE_RE.search(text)
    if not m:
        return None
    hh, mm, dd, mo, yy = m.groups()
    try:
        return datetime(int(yy), int(mo), int(dd), int(hh), int(mm))
    except Exception:
        return None

def parse_relative_he(text):
    if not text:
        return None
    m = REL_HE_RE.search(text)
    if not m:
        return None
    num = m.group(1)
    unit = m.group(2)
    n = int(num) if num and num.isdigit() else 1
    now = datetime.now()
    unit = unit.strip()
    if unit.startswith("שנייה"): return now - timedelta(seconds=n)
    if unit.startswith("דקה"): return now - timedelta(minutes=n)
    if unit.startswith("שעה"):  return now - timedelta(hours=n)
    if unit.startswith("יום"):   return now - timedelta(days=n)
    return now

def session_from_driver(driver):
    s = requests.Session()
    ua = driver.execute_script("return navigator.userAgent;")
    s.headers.update({"User-Agent": ua})
    for c in driver.get_cookies():
        s.cookies.set(c['name'], c['value'], domain=c.get('domain', None), path=c.get('path', '/'))
    return s

def download_stream(session, url, dest, verify=True):
    try:
        verify_target = certifi.where() if verify else False
        with session.get(url, stream=True, timeout=60, verify=verify_target) as r:
            r.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(8192):
                    if chunk:
                        fh.write(chunk)
        return True
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return False

# ---------- Selenium ----------
def make_driver(headless=HEADLESS):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(45)
    return driver

# ---------- Crawler ----------
class GovCrawler:
    def __init__(self, driver):
        self.driver = driver

    def load_listing_rows(self):
        logger.info("Loading gov.il listing page")
        self.driver.get(BASE_LISTING_URL)
        time.sleep(PAGE_WAIT)
        parsed = []
        try:
            rows = self.driver.find_elements(By.XPATH, "//table//tr")
        except Exception:
            rows = []
        for r in rows:
            try:
                tds = r.find_elements(By.XPATH, ".//td")
                if not tds:
                    continue
                name = tds[0].text.strip()
                anchor = None
                try:
                    anchor = r.find_element(By.XPATH, ".//a[contains(normalize-space(.),'לצפייה במחירים') or contains(.,'לצפייה במחירים')]")
                except Exception:
                    try:
                        anchor = r.find_element(By.XPATH, ".//a[@href]")
                    except Exception:
                        anchor = None
                parsed.append((name, anchor, r))
            except Exception:
                continue
        return parsed

    def find_store_anchor(self, fragment):
        for name, anchor, row in self.load_listing_rows():
            if fragment in name:
                return name, anchor, row
        return None, None, None

    def open_provider(self, anchor):
        href = anchor.get_attribute("href")
        if not href:
            return None
        self.driver.execute_script("window.open(arguments[0]);", href)
        time.sleep(0.6)
        self.driver.switch_to.window(self.driver.window_handles[-1])
        time.sleep(0.6)
        return True

    def process_store(self, fragment, folder):
        logger.info(f"Processing: {fragment} -> {folder}")
        name, anchor, row = self.find_store_anchor(fragment)
        if not name or not anchor:
            logger.warning(f"Store link not found for fragment '{fragment}'")
            return
        if not self.open_provider(anchor):
            logger.warning("Could not open provider page")
            return

        ensure_dir(os.path.join(DOWNLOAD_DIR, folder))
        try:
            if "יוחננוף" in name:
                self.handle_yohananof(folder)
            else:
                self.handle_kinglike(folder)
        except Exception as e:
            logger.exception(f"Error handling provider {folder}: {e}")
        finally:
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
            except Exception:
                pass

    # ----- Yohananof -----
    def handle_yohananof(self, folder):
        logger.info("Yohananof: attempting login")
        try:
            wait = WebDriverWait(self.driver, 12)
            u = wait.until(EC.presence_of_element_located((By.ID, "username")))
            p = self.driver.find_element(By.ID, "password")
            btn = self.driver.find_element(By.ID, "login-button")
            u.clear(); u.send_keys(YOHANANOF_USERNAME)
            p.clear(); p.send_keys(YOHANANOF_PASSWORD or "")
            btn.click()
            time.sleep(1.2)
        except TimeoutException:
            logger.debug("Login form not present (maybe already logged in)")

        clicked = False
        possible_texts = ["Price", "Prices", "מחירים", "לצפייה במחירים", "PriceFull"]
        for text in possible_texts:
            try:
                a = WebDriverWait(self.driver, 4).until(
                    EC.element_to_be_clickable((By.XPATH, f"//a[contains(normalize-space(.),'{text}')]"))
                )
                try:
                    a.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", a)
                time.sleep(1.0)
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            try:
                anchors = self.driver.find_elements(By.XPATH, "//a[contains(@href,'file') or contains(@href,'Price') or contains(@href,'Download')]")
                if anchors:
                    el = anchors[0]
                    try:
                        el.click()
                    except Exception:
                        self.driver.execute_script("arguments[0].click();", el)
                    time.sleep(1.0)
            except Exception:
                pass

        rows = self.extract_table_rows_with_gz(relative_time=True)
        if not rows:
            rows = self.extract_gz_from_html(self.driver.page_source or "", base=self.driver.current_url)

        if not rows:
            logger.warning("Yohananof: no .gz files found")
            return

        sess = session_from_driver(self.driver)
        # optional: CSRF from meta
        try:
            meta = self.driver.find_element(By.XPATH, "//meta[@name='csrftoken']")
            token = meta.get_attribute("content")
            if token:
                sess.headers.update({"X-CsrfToken": token})
        except Exception:
            pass

        self.select_and_download_from_rows(rows, folder, sess)

    # ----- King-like providers -----
    def handle_kinglike(self, folder):
        logger.info("Generic king-like provider: trying to trigger BuildHtml / Download buttons")
        try:
            buttons = self.driver.find_elements(By.XPATH, "//button[contains(normalize-space(.),'הורדה') or contains(normalize-space(.),'להורדה')]")
            for b in buttons[:6]:
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", b)
                    try:
                        b.click()
                    except Exception:
                        self.driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.12)
                except Exception:
                    pass
        except Exception:
            pass

        rows = self.extract_table_rows_with_gz(relative_time=False)
        if not rows:
            rows = self.extract_gz_from_html(self.driver.page_source or "", base=self.driver.current_url)

        if not rows:
            logger.warning("Generic provider: no .gz files found")
            return

        sess = session_from_driver(self.driver)
        self.select_and_download_from_rows(rows, folder, sess)

    # ----- extraction helpers -----
    def extract_table_rows_with_gz(self, relative_time=False):
        results = []
        try:
            rows = self.driver.find_elements(By.XPATH, "//table//tr")
        except Exception:
            rows = []
        for r in rows:
            try:
                tds = r.find_elements(By.TAG_NAME, "td")
                if not tds or len(tds) < 5:
                    continue
                fname = tds[0].text.strip()
                if not fname.lower().endswith(".gz"):
                    try:
                        a = r.find_element(By.XPATH, ".//a[contains(@href,'.gz')]")
                        href = a.get_attribute("href")
                        fname = unquote(os.path.basename(urlparse(href).path))
                    except Exception:
                        continue
                type_text = tds[2].text.strip() if len(tds) >= 3 else ""
                date_text = tds[4].text.strip() if len(tds) >= 5 else ""
                dt = None
                if relative_time and "לפני" in date_text:
                    dt = parse_relative_he(date_text)
                else:
                    dt = parse_absolute_he_date(date_text)
                if dt is None:
                    m = re.search(r"(20\d{2})(\d{2})(\d{2})(\d{2})?(\d{2})?", fname)
                    if m:
                        try:
                            yyyy = int(m.group(1)); mm = int(m.group(2)); dd = int(m.group(3))
                            hh = int(m.group(4)) if m.group(4) else 0
                            mi = int(m.group(5)) if m.group(5) else 0
                            dt = datetime(yyyy, mm, dd, hh, mi)
                        except Exception:
                            dt = datetime.now()
                dt = dt or datetime.now()
                download_hint = None
                try:
                    btn = r.find_element(By.XPATH, ".//button[@onclick]")
                    oc = btn.get_attribute("onclick") or ""
                    m = GZ_ONCLICK_RE.search(oc)
                    if m:
                        download_hint = m.group(1)
                except Exception:
                    pass
                try:
                    a = r.find_element(By.XPATH, ".//a[contains(@href, '.gz')]")
                    href = a.get_attribute("href")
                    if href:
                        download_hint = href
                except Exception:
                    pass
                results.append({"filename": fname, "type": type_text, "date": dt, "download_hint": download_hint})
            except Exception:
                continue
        return results

    def extract_gz_from_html(self, html, base):
        found = []
        for m in re.finditer(r'href=[\'"]([^\'"]+?\.gz)[\'"]', html, re.I):
            url = urljoin(base, m.group(1))
            fn = unquote(os.path.basename(urlparse(url).path))
            found.append({"filename": fn, "type": "", "date": datetime.now(), "download_hint": url})
        for m in GZ_ONCLICK_RE.finditer(html):
            fn = m.group(1)
            url = urljoin(base, fn)
            found.append({"filename": fn, "type": "", "date": datetime.now(), "download_hint": fn})
        return found

    # ----- select & download -----
    def select_and_download_from_rows(self, rows, folder, session):
        logger.info(f"Found {len(rows)} candidates for {folder}")
        price, promo = [], []
        for r in rows:
            key = (r.get("filename", "") + " " + r.get("type", "")).lower()
            if "promo" in key or "מבצע" in key or "מבצעים" in key or "promo" in r.get("filename", "").lower():
                promo.append(r)
            else:
                price.append(r)
        price.sort(key=lambda x: x.get("date") or datetime.min, reverse=True)
        promo.sort(key=lambda x: x.get("date") or datetime.min, reverse=True)
        price = price[:2]; promo = promo[:2]
        logger.info(f"Will download {len(price)} price + {len(promo)} promo for {folder}")

        for item in price + promo:
            final_url = None
            hint = item.get("download_hint")
            fn = item.get("filename")
            if hint and isinstance(hint, str) and hint.lower().startswith("http"):
                final_url = hint
            elif hint and isinstance(hint, str) and hint.lower().endswith(".gz"):
                base = self.driver.current_url
                parsed = urlparse(base)
                base_root = f"{parsed.scheme}://{parsed.netloc}"
                download_ajax = urljoin(base_root, "Download.aspx?FileNm=" + hint)
                logger.info(f"Requesting Download.aspx for {fn}: {download_ajax}")
                try:
                    r = session.post(download_ajax, timeout=20, verify=certifi.where() if VERIFY_SSL else False)
                    if r.status_code == 200:
                        try:
                            data = r.json()
                            spath = ""
                            if isinstance(data, list) and data:
                                spath = data[0].get("SPath", "") or data[-1].get("SPath", "")
                            elif isinstance(data, dict):
                                spath = data.get("SPath", "")
                            if spath:
                                final_url = spath
                                logger.info(f"Got SPath: {spath}")
                        except Exception:
                            txt = r.text or ""
                            m = re.search(r'"SPath"\s*:\s*"([^"]+)"', txt)
                            if m:
                                final_url = m.group(1)
                except Exception as e:
                    logger.warning(f"Download.aspx call failed for {hint}: {e}")
            else:
                try:
                    a = self.driver.find_element(By.XPATH, f"//a[contains(@href, '{fn}')]")
                    href = a.get_attribute("href")
                    if href:
                        final_url = urljoin(self.driver.current_url, href)
                except Exception:
                    final_url = None

            if not final_url:
                logger.warning(f"Could not resolve final URL for {fn}; skipping")
                continue

            ensure_dir(os.path.join(DOWNLOAD_DIR, folder))
            dest = os.path.join(DOWNLOAD_DIR, folder, fn)
            logger.info(f"Downloading {final_url} -> {dest} (date={item.get('date')})")
            ok = download_stream(session, final_url, dest, verify=VERIFY_SSL)
            if not ok and VERIFY_SSL:
                logger.warning("Retrying download with verify=False")
                ok = download_stream(session, final_url, dest, verify=False)

            # === Upload to S3 ===
            if ok:
                s3_key = f"{folder}/{fn}"
                try:
                    s3_client.upload_file(dest, BUCKET_NAME, s3_key)
                    logger.info(f"Uploaded to s3://{BUCKET_NAME}/{s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload {fn} to S3: {e}")
            # ====================

            time.sleep(0.25)

# ---------- main ----------
def main():
    ensure_dir(DOWNLOAD_DIR)
    driver = make_driver()
    crawler = GovCrawler(driver)
    try:
        for frag, folder in TARGET_STORES:
            try:
                crawler.process_store(frag, folder)
            except Exception as e:
                logger.exception(f"Store {frag} failed: {e}")
            time.sleep(0.9)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    logger.info("All done.")

if __name__ == "__main__":
    main()