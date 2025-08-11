import json
import os
import re
import time
from bs4 import BeautifulSoup
from utils.browser_utils import *
from utils.time_date_utils import *
from utils import download_file_from_link
from s3.upload_to_s3 import *
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from urllib.parse import urljoin, urlparse


class Crawler:
    def __init__(self):
        self.driver = get_chromedriver()
        self._req_sess = None  
        with open("config.json", "r", encoding="utf-8") as file:
            self.config = json.load(file)

    def crawl(self):
        """Fetch and process data from the target source."""
        for provider in self.config["providers"]:
            print(f"Crawling provider: {provider['name']}")
            try:
                soup = get_html_parser(self.driver, provider["url"])
                if provider["username"] != "none":
                    username_input = self.driver.find_element(By.NAME, "username")
                    username_input.send_keys(provider["username"])
                    if provider["password"] != "none":
                        password_input = self.driver.find_element(By.NAME, "password")
                        password_input.send_keys(provider["password"])

                    button = self.driver.find_element("id", "login-button")
                    button.click()
                    time.sleep(2)
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")

                data = self.extract_data(soup, provider)
                saved_files = self.save_file(data, provider)
                self.upload_file(saved_files, provider)
            except Exception as e:
                print(f"Error crawling {provider['name']}: {e}")
        pass

    def extract_data(self, soup, provider):
        """
        Extract the latest price row and the latest promo row for the SAME branch.
        """
        if not soup:
            return None

        table_body = soup.select_one(provider["table-body-selector"])
        if not table_body:
            return {"price": None, "promo": None, "branch": None}

        price_tr = None
        promo_tr = None
        branch = None

        rows = table_body.select(provider["table-row"])

        for row in rows:
            name_el = row.select_one(provider["name-selector"])
            if not name_el:
                continue
            col_text = name_el.get_text(strip=True)
            if col_text.startswith(provider["file-price-name"]):
                price_tr = self.return_latest_row(row, price_tr, provider)

        if price_tr:
            branch = self.get_branch(price_tr, provider)

        for row in rows:
            name_el = row.select_one(provider["name-selector"])
            if not name_el:
                continue
            col_text = name_el.get_text(strip=True)
            if col_text.startswith(provider["file-promo-name"]):
                if branch:
                    row_branch = self.get_branch(row, provider)
                    if row_branch != branch:
                        continue
                promo_tr = self.return_latest_row(row, promo_tr, provider)

        return {
            "price": price_tr,
            "promo": promo_tr,
            "branch": branch
        }

    def get_branch(self, row, provider):
        sel = provider.get("branch-selector")
        if sel and sel != "none":
            el = row.select_one(sel)
            if el:
                val = el.get_text(strip=True)
                if val:
                    return val 

        raw = ""
        name_sel = provider.get("name-selector")
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

    def return_latest_row(self, row, latest_row, provider):
        def get_date_el(el):
            return el.select_one(provider["date-selector"]) if el else None

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

    def save_file(self, data, provider):
        if not data["promo"] or not data["price"]:
            print(f"No data found for provider {provider['name']}")
            return

        price_row_id = data["price"]["id"]
        promo_row_id = data["promo"]["id"]

        sel_price_row = self.driver.find_element(By.ID, price_row_id)
        sel_promo_row = self.driver.find_element(By.ID, promo_row_id)

        more_info_selector = provider.get("more-info-selector")
        if more_info_selector != "none":
            print("Opening More info")
            price_more_info = sel_price_row.find_element(By.CSS_SELECTOR, more_info_selector)
            promo_more_info = sel_promo_row.find_element(By.CSS_SELECTOR, more_info_selector)

            if promo_more_info:
                promo_more_info.click()
            if price_more_info:
                price_more_info.click()
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, provider["download-button"]))
            )

        price_dt = parse_date(data["price"].select_one(provider["date-selector"]).text.strip())
        promo_dt = parse_date(data["promo"].select_one(provider["date-selector"]).text.strip())
        price_ts = price_dt.strftime("%Y%m%d_%H%M%S")
        promo_ts = promo_dt.strftime("%Y%m%d_%H%M%S")

        provider_name = provider.get("name", "default")
        branch = data.get("branch", "default")
        download_dir = os.path.join("providers", provider_name, branch)
        os.makedirs(download_dir, exist_ok=True)

        if self._req_sess is None:
            self._req_sess = session_from_driver(self.driver)

        download_buttons = []
        for row in (sel_price_row, sel_promo_row):
            btns = row.find_elements(By.CSS_SELECTOR, provider["download-button"])
            if not btns:
                try:
                    details_row = row.find_element(By.XPATH, "following-sibling::tr[1]")
                    btns = details_row.find_elements(By.CSS_SELECTOR, provider["download-button"])
                except NoSuchElementException:
                    btns = []
            download_buttons.extend(btns)

        saved_files = []
        for btn in download_buttons:
            onclick = btn.get_attribute("onclick") or ""
            match = re.search(r"Download\('([^']+)'\)", onclick)
            raw = match.group(1) if match else btn.get_attribute("href") or ""
            if not raw:
                continue

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
            out = download_file_from_link(
                raw_link,
                download_dir,
                filename,
                session=self._req_sess,  
                verify_cert=False         
            )

            if not out and match and "/" not in raw:
                alt_link = urljoin(self.driver.current_url, f"/file/d/{raw}")
                print(f"Retrying via {alt_link} …")
                out = download_file_from_link(
                    alt_link,
                    download_dir,
                    filename,
                    session=self._req_sess,
                    verify_cert=False
                )

            if not out:
                print(f"{filename} not available, skipping.")
                continue

            saved_files.append(filename)

        return saved_files

    def upload_file(self, saved_files, provider):
        """Upload the downloaded files to local s3 bucket."""
        for file in saved_files:
            upload_file_to_s3(provider["name"], file)