import json
import os
import re
from utils.browser_utils import *
from utils.time_date_utils import * 
from utils import download_file_from_link
from s3.upload_to_s3 import *
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urljoin, urlparse

class Crawler:
    driver=""
    config = ""

    def __init__(self):
        self.driver = get_chromedriver()
        with open("config.json", "r", encoding="utf-8") as file:
            self.config = json.load(file)

    def crawl(self):
        """
        Fetch and process data from the target source.
        """
        for provider in self.config["providers"]:
            print(f"Crawling provider: {provider['name']}")
            try:
                soup = get_html_parser(self.driver, provider["url"])
                if provider["username"]!="none":
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
        Extract data from the BeautifulSoup object.
        :param soup: BeautifulSoup object containing the page content
        :return: Extracted data
        """
        if not soup:
            return None
        table_body = soup.select_one(provider["table-body-selector"])
        price_tr = ""
        promo_tr = ""
        for row in table_body.select(provider["table-row"]):
            column_name = row.select_one(provider["name-selector"])

            if column_name:
                col_text = column_name.text.strip()
                if col_text.startswith("Price"):
                    price_tr = self.return_latest_row(row, price_tr, provider)
                elif col_text.startswith("Promo"):
                    promo_tr = self.return_latest_row(row, promo_tr, provider)

        return {
            "price": price_tr,
            "promo": promo_tr
        }
    
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
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, provider["download-button"])))
         
        price_dt = parse_date(data["price"].select_one(provider["date-selector"]).text.strip())
        promo_dt = parse_date(data["promo"].select_one(provider["date-selector"]).text.strip())
        price_ts = price_dt.strftime("%Y%m%d_%H%M%S")
        promo_ts = promo_dt.strftime("%Y%m%d_%H%M%S")

        branch = provider.get("name", "default")
        download_dir = os.path.join("providers", branch)
        os.makedirs(download_dir, exist_ok=True)

        download_buttons = self.driver.find_elements(By.CSS_SELECTOR, provider["download-button"])
        
        saved_files =[]
        for btn in download_buttons:
            onclick = btn.get_attribute("onclick") or ""
            match = re.search(r"Download\('([^']+)'\)", onclick)
            raw = match.group(1) if match else btn.get_attribute("href") or ""
            if not raw:
                continue
            
            raw_link = urljoin(self.driver.current_url, raw)
            parsed = urlparse(raw_link)
            base_name = os.path.basename(parsed.path)

            lower = base_name.lower()
            if lower.startswith("price"):
                prefix, ts = "price", price_ts
            elif lower.startswith("promo"):
                prefix, ts = "promo", promo_ts
            else:
                prefix, ts = "file", price_ts
            
            ext = os.path.splitext(base_name)[1] or ""
            filename = f"{prefix}_{ts}{ext}"
            
            print(f"Downloading {filename}...")
            download_file_from_link(raw_link, download_dir, filename)
            saved_files.append(filename)
        
        return saved_files

    def upload_file(self, saved_files, provider):
        """
        Upload the downloaded files to local s3 bucket.
        """
        for file in saved_files:
            upload_file_to_s3(provider["name"], file)
        
