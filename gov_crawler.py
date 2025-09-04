# gov_crawler.py
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from config import (
    BASE_LISTING_URL,
    PAGE_WAIT,
    TARGET_STORES,
    YOHANANOF_USERNAME,
    YOHANANOF_PASSWORD,
    VERIFY_SSL,
    logger,
    GZ_ONCLICK_RE,
    S3_CLIENT,
    BUCKET_NAME,
)
from utils import (
    ensure_dir,
    parse_absolute_he_date,
    parse_relative_he,
    session_from_driver,
    download_stream,
    filename_from_url,
)

class GovCrawler:
    def __init__(self, driver):
        self.driver = driver

    # listing page
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
        if not name:
            logger.warning(f"Store fragment {fragment} not found")
            return
        if not anchor:
            logger.warning(f"No listing link for {name}")
            return
        if not self.open_provider(anchor):
            logger.warning("Could not open provider page")
            return

        ensure_dir(f"{BUCKET_NAME}")  # no-op if already exists locally; kept for parity
        ensure_dir(__import__("os").path.join(__import__("os").getcwd(), "providers", folder))

        try:
            if "יוחננוף" in name:
                self.handle_yohananof(folder)
            else:
                self.handle_kinglike(folder)
        except Exception as e:
            logger.exception(f"Error handling provider {folder}: {e}")
        finally:
            # close provider tab and return
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])
            except Exception:
                pass

    # Yohananof flow
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

        # click a post-login "Price" link if present
        clicked = False
        possible_texts = ["Price", "Prices", "מחירים", "לצפייה במחירים", "PriceFull"]
        for t in possible_texts:
            try:
                a = WebDriverWait(self.driver, 4).until(
                    EC.element_to_be_clickable((By.XPATH, f"//a[contains(normalize-space(.),'{t}')]"))
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
        # optional CSRF token
        try:
            meta = self.driver.find_element(By.XPATH, "//meta[@name='csrftoken']")
            token = meta.get_attribute("content")
            if token:
                sess.headers.update({"X-CsrfToken": token})
        except Exception:
            pass

        self.select_and_download_from_rows(rows, folder, sess)

    # Generic king-like providers
    def handle_kinglike(self, folder):
        logger.info("Generic king-like provider: attempt to trigger BuildHtml / Download buttons")
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
            logger.warning("Generic: no files found on page")
            return

        sess = session_from_driver(self.driver)
        self.select_and_download_from_rows(rows, folder, sess)

    # extraction helpers
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
                        fname = unquote(urlparse(href).path.split("/")[-1])
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
                    # fallback parse in filename
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
                # download hint (onclick or anchor)
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
            fn = unquote(urlparse(url).path.split("/")[-1])
            found.append({"filename": fn, "type": "", "date": datetime.now(), "download_hint": url})
        for m in GZ_ONCLICK_RE.finditer(html):
            fn = m.group(1)
            url = urljoin(base, fn)
            found.append({"filename": fn, "type": "", "date": datetime.now(), "download_hint": fn})
        return found

    # select & download
    def select_and_download_from_rows(self, rows, folder, session):
        logger.info(f"Found {len(rows)} candidates")
        price = []
        promo = []
        for r in rows:
            key = (r.get("filename", "") + " " + r.get("type", "")).lower()
            if "promo" in key or "מבצע" in key or "מבצעים" in key or "promo" in r.get("filename", "").lower():
                promo.append(r)
            else:
                price.append(r)
        price.sort(key=lambda x: x.get("date") or datetime.min, reverse=True)
        promo.sort(key=lambda x: x.get("date") or datetime.min, reverse=True)
        price = price[:2]
        promo = promo[:2]
        logger.info(f"Will download {len(price)} price + {len(promo)} promo files for {folder}")

        for item in price + promo:
            final_url = None
            hint = item.get("download_hint")
            fn = item.get("filename")
            # if hint is full url
            if hint and isinstance(hint, str) and hint.lower().startswith("http"):
                final_url = hint
            elif hint and isinstance(hint, str) and hint.lower().endswith(".gz"):
                base = self.driver.current_url
                parsed = urlparse(base)
                base_root = f"{parsed.scheme}://{parsed.netloc}"
                download_ajax = urljoin(base_root, "Download.aspx?FileNm=" + hint)
                logger.info(f"Requesting provider Download.aspx for {fn}: {download_ajax}")
                try:
                    r = session.post(download_ajax, timeout=20, verify= (True if VERIFY_SSL else False))
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
                # fallback try to find anchor in DOM
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

            ensure_dir(__import__("os").path.join(__import__("os").getcwd(), "providers", folder))
            dest = __import__("os").path.join(__import__("os").getcwd(), "providers", folder, fn)
            logger.info(f"Downloading {final_url} -> {dest} (date={item.get('date')})")
            ok = download_stream(session, final_url, dest, verify=VERIFY_SSL)
            if not ok and VERIFY_SSL:
                logger.warning("Retrying with verify=False")
                ok = download_stream(session, final_url, dest, verify=False)

            # upload to S3 if successful
            if ok:
                s3_key = f"{folder}/{fn}"
                try:
                    S3_CLIENT.upload_file(dest, BUCKET_NAME, s3_key)
                    logger.info(f"Uploaded to s3://{BUCKET_NAME}/{s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload {fn} to S3: {e}")

            time.sleep(0.35)
