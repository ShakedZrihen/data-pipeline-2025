import os
from browser_utils import *
from time_date_utils import *
from selenium.webdriver.common.by import By
import json
import pprint  # at top

class Crawler:
    driver=""
    config = ""

    def __init__(self):
        self.driver = get_chromedriver()
        with open("config.json", "r", encoding="utf-8") as file:
            self.config = json.load(file)
        # soup = get_html_parser(driver, providers_url)
        # finish the get all providers and input them into the config.json (username, password, url)

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

                self.save_file(data, provider, soup)
                # self.upload_file()
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
        for row in table_body.select("tr"):
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



    def save_file(self, data, provider, soup):
        if not os.path.exists("providers"):
            os.makedirs("providers")
        promo_row = data["promo"]
        price_row = data["price"]
        if not promo_row or not price_row:
            print(f"No data found for provider {provider['name']}")
            return
        
        more_info_selector = provider.get("more-info-selector")
        if more_info_selector:
            promo_more_info = promo_row.select_one(more_info_selector)
            price_more_info = price_row.select_one(more_info_selector)
            if promo_more_info:
                promo_more_info.click()
            if price_more_info:
                price_more_info.click()
        
        #print download button
        download_button = soup.select(provider["download-button-selector"])
        print(f"{download_button}")

    def upload_file(self, filepath):
        """
        Upload the file to a remote destination (e.g., cloud storage).
        :param filepath: Path to the file to be uploaded
        """
        pass
