# crawler.py (top of file)
import json, os, re, time
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException

# local packages
from managers.driver_manager import DriverManager
from managers.file_manager import FileManager
from managers.s3_manager import S3Manager
from utils.date import parse_date
from utils.enums import ENUMS



class Crawler:
    def __init__(self):
        # managers
        self.driver_manager = DriverManager()
        self.driver = self.driver_manager.get_chromedriver()
        self.file_manager = FileManager()
        self.s3 = S3Manager()

        self._req_sess = None
        with open("config.json", "r", encoding="utf-8") as file:
            self.config = json.load(file)

    def run(self):
        """Fetch and process data from the target source."""
        for superMarket in self.config[ENUMS.CONFIG_KEY.value]:
            print(f"Crawling superMarket: {superMarket['name']}")
            try:
                soup = self.driver_manager.get_html_parser(superMarket["url"])

                # Optional login
                if superMarket.get("username") and superMarket["username"] != "none":
                    username_input = self.driver.find_element(By.NAME, "username")
                    username_input.send_keys(superMarket["username"])
                    if superMarket.get("password") and superMarket["password"] != "none":
                        password_input = self.driver.find_element(By.NAME, "password")
                        password_input.send_keys(superMarket["password"])

                    button = self.driver.find_element("id", "login-button")
                    button.click()
                    time.sleep(2)
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")

                data = self.extract_data(soup, superMarket)
                self.save_file(data, superMarket)
            except Exception as e:
                print(f"Error crawling {superMarket['name']}: {e}")

    def extract_data(self, soup, superMarket):
        """
        Extract the latest price row and the latest promo row for the SAME branch.
        """
        if not soup:
            return {"price": None, "promo": None, "branch": None}

        table_body = soup.select_one(superMarket["table-body-selector"])
        if not table_body:
            return {"price": None, "promo": None, "branch": None}

        price_tr = None
        promo_tr = None
        branch = None

        rows = table_body.select(superMarket["table-row"])

        # Find latest price row
        for row in rows:
            name_el = row.select_one(superMarket["name-selector"])
            if not name_el:
                continue
            col_text = name_el.get_text(strip=True)
            if col_text.startswith(superMarket["file-price-name"]):
                price_tr = self.return_latest_row(row, price_tr, superMarket)

        # infer branch from selected price row
        if price_tr:
            branch = self.get_branch(price_tr, superMarket)

        # Find latest promo row for same branch (if known)
        for row in rows:
            name_el = row.select_one(superMarket["name-selector"])
            if not name_el:
                continue
            col_text = name_el.get_text(strip=True)
            if col_text.startswith(superMarket["file-promo-name"]):
                if branch:
                    row_branch = self.get_branch(row, superMarket)
                    if row_branch != branch:
                        continue
                promo_tr = self.return_latest_row(row, promo_tr, superMarket)

        return {"price": price_tr, "promo": promo_tr, "branch": branch}

    def get_branch(self, row, superMarket):
        sel = superMarket.get("branch-selector")
        if sel and sel != "none":
            el = row.select_one(sel)
            if el:
                val = el.get_text(strip=True)
                if val:
                    return val

        # Fallback: parse from the name field (e.g., based on filename pattern)
        raw = ""
        name_sel = superMarket.get("name-selector")
        if name_sel:
            name_el = row.select_one(name_sel)
            if name_el:
                raw = name_el.get_text(strip=True)

        base = os.path.basename(raw)

        m = re.search(r'-(\d+)-\d{12}\.', base)
        if m:
            return m.group(1)

        parts = base.split('-')
        if len(parts) >= 3 and parts[1].isdigit():
            return parts[1]

        return None

    def return_latest_row(self, row, latest_row, superMarket):
        def get_date_el(el):
            return el.select_one(superMarket["date-selector"]) if el else None

        if not row:
            return latest_row
        row_el = get_date_el(row)
        if not row_el:
            return latest_row

        if not latest_row:
            return row
        latest_el = get_date_el(latest_row)
        if not latest_el:
            return row

        row_text = row_el.text.strip()
        latest_text = latest_el.text.strip()

        row_dt = parse_date(row_text)
        latest_dt = parse_date(latest_text)

        if not row_dt and not latest_dt:
            return latest_row
        if not row_dt:
            return latest_row
        if not latest_dt:
            return row

        return row if row_dt > latest_dt else latest_row

    def save_file(self, data, superMarket):
        if not data or not data.get("promo") or not data.get("price"):
            print(f"No data found for superMarket {superMarket['name']}")
            return None

        price_row_id = data["price"].get("id")
        promo_row_id = data["promo"].get("id")

        if not price_row_id or not promo_row_id:
            print("Missing row IDs for price/promo; skipping downloads.")
            return None

        sel_price_row = self.driver.find_element(By.ID, price_row_id)
        sel_promo_row = self.driver.find_element(By.ID, promo_row_id)

        more_info_selector = superMarket.get("more-info-selector")
        if more_info_selector and more_info_selector != "none":
            print("Opening More info")
            price_more_info = sel_price_row.find_element(By.CSS_SELECTOR, more_info_selector)
            promo_more_info = sel_promo_row.find_element(By.CSS_SELECTOR, more_info_selector)

            if promo_more_info:
                promo_more_info.click()
            if price_more_info:
                price_more_info.click()
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, superMarket["download-button"]))
            )

        price_dt = parse_date(data["price"].select_one(superMarket["date-selector"]).text.strip())
        promo_dt = parse_date(data["promo"].select_one(superMarket["date-selector"]).text.strip())
        price_ts = price_dt.strftime("%Y%m%d_%H%M%S")
        promo_ts = promo_dt.strftime("%Y%m%d_%H%M%S")

        provider_name = superMarket.get("name", "default")
        branch = data.get("branch", "default")
        download_dir = os.path.join("supermarkets", provider_name, branch)
        os.makedirs(download_dir, exist_ok=True)

        if self._req_sess is None:
            self._req_sess = self.driver_manager.session_from_driver()

        # Gather download buttons (in-row or in the details row just after)
        download_buttons = []
        for row_el in (sel_price_row, sel_promo_row):
            btns = row_el.find_elements(By.CSS_SELECTOR, superMarket["download-button"])
            if not btns:
                try:
                    details_row = row_el.find_element(By.XPATH, "following-sibling::tr[1]")
                    btns = details_row.find_elements(By.CSS_SELECTOR, superMarket["download-button"])
                except NoSuchElementException:
                    btns = []
            download_buttons.extend(btns)

        for btn in download_buttons:
            onclick = btn.get_attribute("onclick") or ""
            match = re.search(r"Download\('([^']+)'\)", onclick)
            raw = match.group(1) if match else (btn.get_attribute("href") or "")
            if not raw:
                continue

            # Build the absolute link
            if match:
                raw_link = urljoin(self.driver.current_url, f"/Download/{raw.lstrip('/')}")
            else:
                raw_link = urljoin(self.driver.current_url, raw)
            print(f"Download Link: {raw_link}")

            parsed = urlparse(raw_link)
            base_name = os.path.basename(parsed.path)
            ext = os.path.splitext(base_name)[1] or ""

            lower = base_name.lower()
            if lower.startswith("price"):
                prefix, ts = "price", price_ts
            elif lower.startswith("promo"):
                prefix, ts = "promo", promo_ts
            else:
                prefix, ts = "file", price_ts

            filename = f"{prefix}_{ts}{ext}"

            print(f"Downloading {filename}…")
            outPath = self.file_manager.download_file_from_link(
                raw_link,
                download_dir,
                filename=filename,
                session=self._req_sess,
                verify_cert=False
            )

            if not outPath and match and "/" not in raw:
                alt_link = urljoin(self.driver.current_url, f"/file/d/{raw}")
                print(f"Retrying via {alt_link} …")
                outPath = self.file_manager.download_file_from_link(
                    alt_link,
                    download_dir,
                    filename=filename,
                    session=self._req_sess,
                    verify_cert=False
                )

            if not outPath:
                print(f"{filename} not available, skipping.")
                continue
            if outPath:
                s3_key = f"{provider_name}/{branch or 'default'}/{filename}".replace("\\", "/")
                self.s3.upload_file_from_path(outPath, s3_key)


