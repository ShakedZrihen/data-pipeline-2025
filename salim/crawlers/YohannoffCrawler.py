from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from Base import *
from datetime import datetime
import time
import os
import shutil
from selenium.common.exceptions import WebDriverException

class YohananofCrawler(CrawlerBase):

    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": os.path.expanduser("~/Downloads"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)

    def login(self):
        self.driver.get("https://url.publishedprices.co.il/login")
        # Fill in username
        username_input = self.driver.find_element(By.ID, "username")
        username_input.send_keys("yohananof")

        # Click login
        login_button = self.driver.find_element(By.ID, "login-button")
        login_button.click()

        # Wait until the "Processing..." message disappears
        WebDriverWait(self.driver, 20).until(
            EC.invisibility_of_element_located((By.XPATH, "//div[text()='Processing...']"))
        )

        # Then wait for at least one row to be present
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//tr[starts-with(@id, 'Price')]"))
        )
        

    def extract_file_links(self):
        self.login()  # First perform login

        # rows = self.driver.find_elements(By.XPATH, "//tr[starts-with(@id, 'Price')]")
        rows = self.driver.find_elements(By.XPATH, "//tr[starts-with(@id, 'Price') or starts-with(@id, 'Promo')]")
        found = {"pricesFull": None, "promoFull": None}

        for row in rows:
            try:
                link = row.find_element(By.TAG_NAME, "a")
                href = link.get_attribute("href")  # e.g., "/file/d/PriceXXXX.gz"
                filename = link.get_attribute("title").strip()  # e.g., "PriceXXXX.gz"
            except Exception as e:
                print(f"Failed to extract download link: {e}")
                continue

            if not filename.lower().endswith(".gz"):
                continue

            # Derive branch from filename pattern: e.g., "Price7290803800003-001-202508060900.gz"
            try:
                branch = filename.split("-")[1]  # "001"
            except IndexError:
                branch = "unknown"

            full_url = href if href.startswith("http") else f"https://url.publishedprices.co.il{href}"
            file_type = "pricesFull" if filename.lower().startswith("price") else "promoFull"


            if file_type == "pricesFull" and found["pricesFull"] is None:
                found["pricesFull"] = {"url": full_url, "branch": branch, "type": "pricesFull"}
            elif file_type == "promoFull" and found["promoFull"] is None:
                found["promoFull"] = {"url": full_url, "branch": branch, "type": "promoFull"}

            if all(found.values()):
                break

        return [v for v in found.values() if v]


    def get_timestamp(self):
        return datetime.now().strftime("%Y%m%d%H%M%S")


    def download_file(self, file_entry):
    # Setup
        url = file_entry["url"]
        folder = os.path.join("providers", self.provider_name, file_entry["branch"])
        filename = f"{file_entry['type']}_{self.get_timestamp()}.gz"
        filepath = os.path.join(folder, filename)

        # Assume Chrome downloads go to ~/Downloads or a configured location
        download_dir = os.path.expanduser("~/Downloads")  # adjust if needed

        try:
            self.driver.get(url)
        except WebDriverException as e:
            print(f"Selenium failed to open URL: {e}")
            return

        # Wait for download to complete
        time.sleep(5)  # can be increased if needed

        # Find the newest .gz file in the download directory
        downloaded_file = None
        for f in sorted(os.listdir(download_dir), key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True):
            if f.endswith(".gz"):
                downloaded_file = os.path.join(download_dir, f)
                break

        if not downloaded_file or not os.path.exists(downloaded_file):
            print("No .gz file found in download directory.")
            return

        # Move to target folder with renamed filename
        os.makedirs(folder, exist_ok=True)
        shutil.move(downloaded_file, filepath)
        upload_file_to_s3(self.provider_name, file_entry["branch"], filepath)

