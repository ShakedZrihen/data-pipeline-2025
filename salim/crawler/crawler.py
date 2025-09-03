#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import gzip
import json
import time
import logging
import requests
import urllib3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import boto3

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# Silence TLS warnings (we also do session.verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("crawler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class UniversalSupermarketCrawler:
    """
    Crawler that:
      - Logs in with Selenium (if needed) and syncs cookies to a requests.Session
      - Finds .gz links, makes them absolute, and tries to download with requests
      - Falls back to a real browser click if direct GET fails
      - Validates GZIP before upload
      - Uploads to S3 as: providers/<provider>/<branch>/<file>.gz
        where <file> is pricesFull_<UTC>_b{n}.gz or promoFull_<UTC>_b{n}.gz
    """

    def __init__(self, bucket_name: str, config_file: str | None = None, local_mode: bool = False):
        """
        bucket_name: when local_mode=True, treated as a local folder path to save files under ./providers/...
        local_mode: if True, do not upload to S3; save under <bucket_name>/providers/<provider>/<branch>/.
        """
        self.bucket = bucket_name
        self.local_mode = bool(local_mode)
        self.s3 = boto3.client("s3") if not self.local_mode else None
        self.config = self._load_config(config_file)

        self.download_dir = Path.cwd() / "downloads"
        self.download_dir.mkdir(exist_ok=True)

        # UTC timestamp that stays constant for this run
        self.ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # requests session that will be fed Selenium cookies after login
        self.session = requests.Session()
        self.session.verify = False  # many of these portals have odd cert chains
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Accept": "application/x-gzip, application/gzip, application/octet-stream;q=0.9, */*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        })

    # ──────────────────────────────────────────────────────────
    # Config
    # ──────────────────────────────────────────────────────────
    def _load_config(self, file_path: str | None):
        if file_path and Path(file_path).exists():
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        # default config (your list)
        return {
            "supermarkets": [
                {
                    "name": "yohananof",
                    "display_name": "יוחננוף",
                    "url": "https://url.publishedprices.co.il/login",
                    "username": "yohananof",
                    "password": ""
                },
                {"name": "victory", "display_name": "ויקטורי", "url": "https://laibcatalog.co.il/"},
                {"name": "carrefour", "display_name": "קרפור", "url": "https://prices.carrefour.co.il/"},
                {"name": "shufersal", "display_name": "שופרסל", "url": "http://prices.shufersal.co.il/"},
                {"name": "mega", "display_name": "מגה", "url": "https://prices.mega.co.il/"},
                {"name": "wolt", "display_name": "Wolt", "url": "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html"},
                {"name": "super-pharm", "display_name": "סופר פארם", "url": "http://prices.super-pharm.co.il/"},
                {"name": "hazi-hinam", "display_name": "חצי חינם", "url": "https://shop.hazi-hinam.co.il/Prices"}
            ],
            "max_branches": 2,
            "max_files_per_site": 10
        }

    # ──────────────────────────────────────────────────────────
    # Selenium
    # ──────────────────────────────────────────────────────────
    def setup_driver(self):
        options = Options()
        try:
            options.add_argument("--headless=new")
        except Exception:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        prefs = {
            "download.default_directory": str(self.download_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=options)
        # Allow downloads in headless Chrome
        try:
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": str(self.download_dir.absolute()),
            })
        except Exception:
            pass
        return driver

    def sync_cookies_to_session(self, driver):
        """Copy Selenium cookies into the requests session and set Referer."""
        jar = requests.cookies.RequestsCookieJar()
        for c in driver.get_cookies():
            name, value = c.get("name"), c.get("value")
            domain = c.get("domain") or ""
            path = c.get("path") or "/"
            if name and value:
                jar.set(name, value, domain=domain, path=path)
        self.session.cookies.update(jar)
        try:
            self.session.headers["Referer"] = driver.current_url
        except Exception:
            pass
        logger.info("Synchronized Selenium cookies into requests session")

    def detect_site_type(self, driver, url):
        logger.info(f"Detecting site type for {url}")
        driver.get(url)
        time.sleep(2)

        # login-ish?
        for selector in [
            "input[type='password']",
            "input[name*='user']",
            "#username",
            ".login-form",
            "form[action*='login']"
        ]:
            if driver.find_elements(By.CSS_SELECTOR, selector):
                logger.info("Detected: Login required site")
                return "login"

        # table with .gz links?
        tables = driver.find_elements(By.TAG_NAME, "table")
        if tables:
            for t in tables:
                if t.find_elements(By.CSS_SELECTOR, "a[href*='.gz'], a[href*='download']"):
                    logger.info("Detected: Table-based site")
                    return "table"

        # direct links?
        for selector in [".downloadBtn", "a[href*='.gz']", "a[href*='Price']", "a[href*='Promo']"]:
            if driver.find_elements(By.CSS_SELECTOR, selector):
                logger.info("Detected: Direct download site")
                return "direct"

        logger.info("Detected: Generic site")
        return "generic"

    def handle_login(self, driver, supermarket) -> bool:
        """Best-effort login; still sync cookies even if no password is required."""
        if "username" not in supermarket:
            return False
        logger.info(f"Attempting login for {supermarket.get('display_name', supermarket['name'])}")
        try:
            user = None
            for sel in ["#username", "input[name*='user']", "input[type='text']"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    user = els[0]; break
            if user:
                user.clear()
                user.send_keys(supermarket["username"])

            pwd = None
            for sel in ["input[type='password']", "input[name*='pass']"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    pwd = els[0]; break
            if pwd and supermarket.get("password"):
                pwd.clear()
                pwd.send_keys(supermarket["password"])

            for sel in ["button[type='submit']", "input[type='submit']", ".login-button", "button:not([type='button'])"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    els[0].click()
                    time.sleep(2)
                    break

            self.sync_cookies_to_session(driver)
            return True
        except Exception as e:
            logger.warning(f"Login flow issue: {e}")
            self.sync_cookies_to_session(driver)
            return False

    # ──────────────────────────────────────────────────────────
    # Post-login: make sure we actually reach the list with .gz links
    # ──────────────────────────────────────────────────────────
    def wait_for_gz_links(self, driver, timeout=20):
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href$='.gz'], a[href*='.gz?']"))
            )
            return True
        except Exception:
            return False

    def scroll_and_check(self, driver, steps=4, pause=0.7):
        for _ in range(steps):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/3);")
            time.sleep(pause)
            if driver.find_elements(By.CSS_SELECTOR, "a[href$='.gz'], a[href*='.gz?']"):
                return True
        return False

    def click_nav_candidates_to_find_links(self, driver):
        keywords = [
            "מחיר", "מבצע", "קבצים", "רשימות",   # Hebrew
            "Price", "Promo", "Download", "Files", "List"
        ]
        anchors = driver.find_elements(By.CSS_SELECTOR, "a, button")
        anchors = sorted(
            anchors,
            key=lambda el: (
                "menu" not in (el.get_attribute("role") or "").lower(),
                len((el.text or "").strip()) == 0
            )
        )
        tried = 0
        for el in anchors:
            if tried >= 8:
                break
            txt = (el.text or "").strip()
            if not txt:
                continue
            low = txt.lower()
            if any(k in low for k in [k.lower() for k in keywords]):
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    el.click()
                    time.sleep(1.5)
                    if self.wait_for_gz_links(driver, timeout=6) or self.scroll_and_check(driver):
                        return True
                    tried += 1
                except Exception:
                    continue
        return False

    def dump_debug_html(self, driver, tag="no_links"):
        try:
            dbg = (self.download_dir / f"debug_{tag}_{int(time.time())}.html")
            dbg.write_text(driver.page_source, encoding="utf-8")
            logger.info(f"Saved debug HTML: {dbg}")
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────
    # Link discovery & validated downloads
    # ──────────────────────────────────────────────────────────
    def find_all_download_links(self, driver):
        links = []
        seen = set()
        MAX = 100
        base = driver.current_url

        # priority selectors first
        for sel in ["a[href*='.gz']", ".downloadBtn", "a[href*='Price']", "a[href*='Promo']"]:
            if len(links) >= MAX:
                break
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, sel)[:50]:
                    href = el.get_attribute("href")
                    if not href:
                        continue
                    href = urljoin(base, href)
                    if href in seen:
                        continue
                    seen.add(href)
                    fname = href.split("/")[-1].lower()
                    ftype = "price" if "price" in fname else ("promo" if "promo" in fname else "unknown")
                    links.append({
                        "url": href,
                        "text": (el.text or "")[:50],
                        "type": ftype,
                        "filename": fname,
                        "element": el,  # keep for browser fallback
                    })
            except Exception:
                pass

        # fallback: scan first tables
        if len(links) < 10:
            try:
                for table in driver.find_elements(By.TAG_NAME, "table")[:2]:
                    for row in table.find_elements(By.TAG_NAME, "tr")[:20]:
                        for a in row.find_elements(By.TAG_NAME, "a")[:2]:
                            href = a.get_attribute("href")
                            if not href:
                                continue
                            href = urljoin(base, href)
                            if href in seen or ".gz" not in href:
                                continue
                            seen.add(href)
                            row_text = row.text.lower()
                            ftype = "price" if "price" in row_text else ("promo" if "promo" in row_text else "unknown")
                            links.append({
                                "url": href,
                                "text": row_text[:50],
                                "type": ftype,
                                "filename": href.split("/")[-1],
                                "element": a,
                            })
            except Exception:
                pass

        logger.info(f"Found {len(links)} download links (limited search)")
        return links

    def _is_gzip_magic(self, data: bytes) -> bool:
        return len(data) >= 2 and data[0] == 0x1F and data[1] == 0x8B

    def download_via_browser(self, driver, element, final_filename: str, timeout: int = 60) -> Path | None:
        """Click the link in Selenium and pick up the .gz from downloads."""
        before = {p for p in self.download_dir.glob("*")}
        try:
            element.click()
        except Exception as e:
            logger.error(f"Browser click failed: {e}")
            return None

        deadline = time.time() + timeout
        latest = None
        while time.time() < deadline:
            after = {p for p in self.download_dir.glob("*")}
            new_files = list(after - before)
            gz_done = [p for p in new_files if p.name.endswith(".gz") and not p.name.endswith(".crdownload")]
            if gz_done:
                latest = max(gz_done, key=lambda p: p.stat().st_mtime)
                break
            time.sleep(0.5)

        if not latest or not latest.exists():
            logger.error("Browser download timed out or not found")
            return None

        target = self.download_dir / final_filename
        try:
            latest.replace(target)
        except Exception:
            # fallback copy
            import shutil
            shutil.copy2(latest, target)
            try:
                latest.unlink(missing_ok=True)
            except Exception:
                pass

        # validate gzip
        try:
            with gzip.open(target, "rb") as gz:
                gz.read(64)
        except OSError as e:
            logger.error(f"GZIP check failed after browser download: {e}")
            try:
                target.unlink(missing_ok=True)
            except Exception:
                pass
            return None

        return target

    def download_file_with_validation(self, driver, link: dict, final_filename: str) -> Path | None:
        """
        Try authenticated requests first; if it fails (TLS/HTML/etc), fallback to browser click.
        """
        url = link["url"]
        local_path = self.download_dir / final_filename
        logger.info(f"Downloading (validated): {final_filename}")

        # 1) Try requests (fast path)
        try:
            resp = self.session.get(url, stream=True, timeout=45, allow_redirects=True)
            ct = resp.headers.get("Content-Type", "")
            logger.info(f"   HTTP {resp.status_code}, Content-Type={ct!r}")
            resp.raise_for_status()

            it = resp.iter_content(chunk_size=8192)
            first = next(it, b"")
            if not first:
                raise RuntimeError("Empty response body")

            if not self._is_gzip_magic(first) and "gzip" not in ct.lower():
                raise RuntimeError("Not a GZIP response")

            with open(local_path, "wb") as f:
                f.write(first)
                for chunk in it:
                    if chunk:
                        f.write(chunk)

            # final gzip check
            with gzip.open(local_path, "rb") as gz:
                gz.read(64)
            return local_path

        except Exception as e:
            logger.warning(f"Direct download failed ({e}); trying browser fallback...")

        # 2) Browser fallback
        el = link.get("element")
        if not el:
            logger.error("No element available for browser fallback")
            return None

        return self.download_via_browser(driver, el, final_filename)

    # ──────────────────────────────────────────────────────────
    # Site-specific helpers (Carrefour, Victory)
    # ──────────────────────────────────────────────────────────
    def handle_carrefour_filters(self, driver):
        """
        UPDATED: Carrefour already lists newest files for today's date.
        Do NOT force the date to yesterday. Optionally click 'חיפוש' (Search) if present,
        then wait for links.
        """
        try:
            buttons = driver.find_elements(By.XPATH, "//button[contains(., 'חיפוש') or contains(., 'Search')]")
            if buttons:
                buttons[0].click()
                logger.info("Carrefour: clicked search")
                time.sleep(2.0)
        except Exception:
            pass

        self.wait_for_gz_links(driver, timeout=10)
        self.scroll_and_check(driver)

    def handle_victory_optimized(self, driver):
        """
        Victory sometimes has large tables; try quick .gz anchors first then fallback to generic.
        FIXED: Properly identify file types as price or promo instead of marking all as unknown.
        """
        links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.gz']")
        if links:
            result = []
            for a in links[:50]:
                href = a.get_attribute("href") or ""
                text = a.text or ""
                filename = href.split("/")[-1].lower()
                
                # Try to get surrounding context for better type detection
                try:
                    # Get parent row if in a table
                    parent = a.find_element(By.XPATH, "./ancestor::tr[1]")
                    parent_text = parent.text.lower() if parent else ""
                except:
                    parent_text = ""
                
                # Combine all available text for type detection
                combined = (filename + " " + text + " " + parent_text).lower()
                
                # Determine file type based on keywords
                if any(kw in combined for kw in ["price", "pricefull", "מחיר"]):
                    ftype = "price"
                elif any(kw in combined for kw in ["promo", "promofull", "מבצע", "promotion"]):
                    ftype = "promo"
                else:
                    # Last resort: check filename patterns
                    # Many sites use patterns like PriceFull*, PromoFull*
                    if "pricefull" in filename or filename.startswith("price"):
                        ftype = "price"
                    elif "promofull" in filename or filename.startswith("promo"):
                        ftype = "promo"
                    else:
                        ftype = "unknown"
                
                result.append({
                    "url": urljoin(driver.current_url, href),
                    "text": text[:50],
                    "type": ftype,
                    "filename": filename,
                    "element": a
                })
            
            # Log type distribution for debugging
            price_count = sum(1 for l in result if l['type'] == 'price')
            promo_count = sum(1 for l in result if l['type'] == 'promo')
            unknown_count = sum(1 for l in result if l['type'] == 'unknown')
            logger.info(f"Victory link types: {price_count} price, {promo_count} promo, {unknown_count} unknown")
            
            return result
        return self.find_all_download_links(driver)

    # ──────────────────────────────────────────────────────────
    # Orchestration
    # ──────────────────────────────────────────────────────────

    # NEW: helper to read timestamp from Carrefour filenames and sort newest first
    def _extract_timestamp_from_filename(self, fname: str):
        """
        Files end with -YYYYMMDDHHMM(.gz) or sometimes -YYYYMMDDHHMMSS(.gz).
        Return a datetime for sorting; None if not found.
        """
        if not fname:
            return None
        fname = fname.lower()
        m = re.search(r'(\d{14}|\d{12})(?=\.gz$)', fname)
        if not m:
            return None
        s = m.group(1)
        try:
            if len(s) == 14:
                return datetime.strptime(s, "%Y%m%d%H%M%S")
            return datetime.strptime(s, "%Y%m%d%H%M")
        except Exception:
            return None

    def organize_and_download_files(self, driver, download_links, supermarket_name):
        """
        Download up to max_br price + promo files, validate GZIP, and return
        a list of {"path": Path, "branch": "branch_X", "type": "pricesFull|promoFull"}.
        Filenames are unique per-branch to avoid collisions during upload.
        """
        downloaded = []

        price_files   = [l for l in download_links if l['type'] == 'price']
        promo_files   = [l for l in download_links if l['type'] == 'promo']
        unknown_files = [l for l in download_links if l['type'] == 'unknown']

        # >>> NEW: sort by timestamp in filename so we take the newest 2 of each
        def _sort_newest_first(files):
            return sorted(
                files,
                key=lambda l: (self._extract_timestamp_from_filename(l.get('filename', '')) or datetime.min),
                reverse=True
            )

        price_files = _sort_newest_first(price_files)
        promo_files = _sort_newest_first(promo_files)
        # <<<

        logger.info(f"File type breakdown for {supermarket_name}:")
        logger.info(f"  - Price files found: {len(price_files)}")
        logger.info(f"  - Promo files found: {len(promo_files)}")
        logger.info(f"  - Unknown files found: {len(unknown_files)}")

        max_br = self.config.get('max_branches', 2)

        # --- Prices: newest first, unique filename per branch ---
        for i, link in enumerate(price_files[:max_br], start=1):
            branch_num = i
            branch     = f"branch_{branch_num}"
            filename   = f"pricesFull_{self.ts}_b{branch_num}.gz"
            path = self.download_file_with_validation(driver, link, filename)
            if path:
                downloaded.append({"path": path, "branch": branch, "type": "pricesFull"})

        # --- Promos: newest first, unique filename per branch ---
        for i, link in enumerate(promo_files[:max_br], start=1):
            branch_num = i
            branch     = f"branch_{branch_num}"
            filename   = f"promoFull_{self.ts}_b{branch_num}.gz"
            path = self.download_file_with_validation(driver, link, filename)
            if path:
                downloaded.append({"path": path, "branch": branch, "type": "promoFull"})

        # --- Fallback: unknowns, keep filenames unique even if multiple per branch/type ---
        # Modified to ensure we try to download both price and promo files
        if len(downloaded) < max_br * 2 and unknown_files:
            from collections import defaultdict
            per_branch_type_count = defaultdict(int)
            
            # Sort unknowns by newest first
            unknown_files = _sort_newest_first(unknown_files)
            
            # Track how many of each type we've downloaded
            price_downloaded = sum(1 for d in downloaded if d['type'] == 'pricesFull')
            promo_downloaded = sum(1 for d in downloaded if d['type'] == 'promoFull')
            
            for link in unknown_files[:max_br * 2]:
                fname = (link.get("filename") or "").lower()
                
                # Try to balance price and promo files
                if "promo" in fname and promo_downloaded < max_br:
                    inferred_type = "promoFull"
                    branch_num = promo_downloaded + 1
                    promo_downloaded += 1
                elif price_downloaded < max_br:
                    inferred_type = "pricesFull"
                    branch_num = price_downloaded + 1
                    price_downloaded += 1
                elif promo_downloaded < max_br:
                    inferred_type = "promoFull"
                    branch_num = promo_downloaded + 1
                    promo_downloaded += 1
                else:
                    continue  # We have enough files
                    
                branch = f"branch_{branch_num}"
                per_branch_type_count[(inferred_type, branch_num)] += 1
                seq = per_branch_type_count[(inferred_type, branch_num)]
                filename = f"{inferred_type}_{self.ts}_b{branch_num}_{seq}.gz"
                path = self.download_file_with_validation(driver, link, filename)
                if path:
                    downloaded.append({"path": path, "branch": branch, "type": inferred_type})

        return downloaded

    def upload_to_s3(self, file_info, supermarket_name):
        """
        Upload following the exact pattern expected by the extractor:
        providers/<provider>/<branch>/<file>.gz
        where <file> is pricesFull_<timestamp>_b{n}.gz or promoFull_<timestamp>_b{n}.gz
        """
        file_path: Path = file_info["path"]
        branch = file_info.get("branch", "branch_1")
        key = f"providers/{supermarket_name}/{branch}/{file_path.name}"
        if self.local_mode:
            # Save to local folder structure for testing
            dest = Path(self.bucket) / key.replace("/", os.sep)
            dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                data = file_path.read_bytes()
                dest.write_bytes(data)
                logger.info(f"Saved locally: {dest}")
                return True
            except Exception as e:
                logger.error(f"Failed to save locally: {e}")
                return False
            finally:
                try:
                    file_path.unlink(missing_ok=True)
                except Exception:
                    pass
        else:
            try:
                with open(file_path, "rb") as f:
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=f.read(),
                        ContentType="application/gzip",
                    )
                logger.info(f"Uploaded to s3://{self.bucket}/{key}")
                return True
            except Exception as e:
                logger.error(f"Failed to upload to S3: {e}")
                return False
            finally:
                # Clean up local file after upload attempt
                try:
                    file_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def crawl_supermarket(self, driver, supermarket):
        name = supermarket["name"]
        display = supermarket.get("display_name", name)
        url = supermarket["url"]

        logger.info("\n" + "=" * 60)
        logger.info(f"Processing: {display}")
        logger.info(f"URL: {url}")

        site_type = self.detect_site_type(driver, url)

        if site_type == "login" or "username" in supermarket:
            self.handle_login(driver, supermarket)

        # Post-login: try to ensure we see the list
        got_links = self.wait_for_gz_links(driver, timeout=20)
        if not got_links:
            got_links = self.scroll_and_check(driver)
        if not got_links:
            got_links = self.click_nav_candidates_to_find_links(driver)
        if not got_links:
            self.dump_debug_html(driver, tag=f"{name}_no_links")
            logger.warning(f"No .gz links detected for {name} at {url}. Dumped HTML for debugging.")

        # Site-specific nudges (optional but helpful)
        if name == "carrefour":
            self.handle_carrefour_filters(driver)

        # Find links
        if name == "victory":
            links = self.handle_victory_optimized(driver)
        else:
            links = self.find_all_download_links(driver)

        if not links:
            self.dump_debug_html(driver, tag=f"{name}_post_find_all")
            logger.info(f"Successfully uploaded 0 files for {display}")
            return 0

        downloaded = self.organize_and_download_files(driver, links, name)

        uploaded = 0
        for info in downloaded:
            if self.upload_to_s3(info, name):
                uploaded += 1

        logger.info(f"Successfully uploaded {uploaded} files for {display}")
        return uploaded

    def run(self, supermarket_filter=None):
        logger.info("Starting Universal Supermarket Crawler")
        logger.info(f"S3 bucket: {self.bucket}")

        supermarkets = self.config["supermarkets"]
        logger.info(f"Available supermarkets in config: {[s['name'] for s in supermarkets]}")

        if supermarket_filter:
            logger.info(f"Filter requested for: {supermarket_filter}")
            supermarkets = [s for s in supermarkets if s["name"] in supermarket_filter]
            if not supermarkets:
                logger.warning(f"No supermarkets match filter: {supermarket_filter}")
                return

        driver = self.setup_driver()
        results = {}
        try:
            for s in supermarkets:
                uploaded = self.crawl_supermarket(driver, s)
                results[s["name"]] = uploaded
                time.sleep(1)
        finally:
            try:
                driver.quit()
            except Exception:
                pass
            # clean tmp
            for f in self.download_dir.glob("*"):
                try:
                    f.unlink()
                except Exception:
                    pass

            logger.info("\n" + "=" * 60)
            logger.info("CRAWLER SUMMARY")
            logger.info("=" * 60)
            total = 0
            for name, count in results.items():
                logger.info(f"{name}: {count} files")
                total += count
            logger.info(f"\nTotal files uploaded: {total}")
            logger.info(f"S3 bucket: {self.bucket}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python crawler_final.py <s3-bucket> [--config config.json] [--only name1,name2]")
        sys.exit(1)
    bucket = sys.argv[1]
    config_file = None
    only = None
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--config" and i + 1 < len(sys.argv):
            config_file = sys.argv[i + 1]
            i += 1
        elif sys.argv[i] == "--only" and i + 1 < len(sys.argv):
            only = sys.argv[i + 1].split(",")
            i += 1
        i += 1

    crawler = UniversalSupermarketCrawler(bucket, config_file)
    crawler.run(supermarket_filter=only)


if __name__ == "__main__":
    main()
