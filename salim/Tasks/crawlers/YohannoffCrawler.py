# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium import webdriver
# from Base import *
# from datetime import datetime
# import time
# import os
# import shutil
# from selenium.common.exceptions import WebDriverException

# class YohananofCrawler(CrawlerBase):

#     options = webdriver.ChromeOptions()
#     prefs = {
#         "download.default_directory": os.path.expanduser("~/Downloads"),
#         "download.prompt_for_download": False,
#         "download.directory_upgrade": True,
#         "safebrowsing.enabled": True
#     }
#     options.add_argument("--headless=new")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--no-sandbox")
#     options.add_experimental_option("prefs", prefs)
#     driver = webdriver.Chrome(options=options)

#     def login(self):
#         self.driver.get("https://url.publishedprices.co.il/login")
#         # Fill in username
#         username_input = self.driver.find_element(By.ID, "username")
#         username_input.send_keys("yohananof")

#         # Click login
#         login_button = self.driver.find_element(By.ID, "login-button")
#         login_button.click()

#         # Wait until the "Processing..." message disappears
#         WebDriverWait(self.driver, 20).until(
#             EC.invisibility_of_element_located((By.XPATH, "//div[text()='Processing...']"))
#         )

#         # Then wait for at least one row to be present
#         WebDriverWait(self.driver, 10).until(
#             EC.presence_of_element_located((By.XPATH, "//tr[starts-with(@id, 'Price')]"))
#         )
        

#     def extract_file_links(self):
#         self.login()  # First perform login

#         # rows = self.driver.find_elements(By.XPATH, "//tr[starts-with(@id, 'Price')]")
#         rows = self.driver.find_elements(By.XPATH, "//tr[starts-with(@id, 'Price') or starts-with(@id, 'Promo')]")
#         found = {"pricesFull": None, "promoFull": None}

#         for row in rows:
#             try:
#                 link = row.find_element(By.TAG_NAME, "a")
#                 href = link.get_attribute("href")  # e.g., "/file/d/PriceXXXX.gz"
#                 filename = link.get_attribute("title").strip()  # e.g., "PriceXXXX.gz"
#             except Exception as e:
#                 print(f"Failed to extract download link: {e}")
#                 continue

#             if not filename.lower().endswith(".gz"):
#                 continue

#             # Derive branch from filename pattern: e.g., "Price7290803800003-001-202508060900.gz"
#             try:
#                 branch = filename.split("-")[1]  # "001"
#             except IndexError:
#                 branch = "unknown"

#             full_url = href if href.startswith("http") else f"https://url.publishedprices.co.il{href}"
#             file_type = "pricesFull" if filename.lower().startswith("price") else "promoFull"


#             if file_type == "pricesFull" and found["pricesFull"] is None:
#                 found["pricesFull"] = {"url": full_url, "branch": branch, "type": "pricesFull"}
#             elif file_type == "promoFull" and found["promoFull"] is None:
#                 found["promoFull"] = {"url": full_url, "branch": branch, "type": "promoFull"}

#             if all(found.values()):
#                 break

#         return [v for v in found.values() if v]


#     def get_timestamp(self):
#         return datetime.now().strftime("%Y%m%d%H%M%S")


#     def download_file(self, file_entry):
#     # Setup
#         url = file_entry["url"]
#         folder = os.path.join("providers", self.provider_name, file_entry["branch"])
#         filename = f"{file_entry['type']}_{self.get_timestamp()}.gz"
#         filepath = os.path.join(folder, filename)

#         # Assume Chrome downloads go to ~/Downloads or a configured location
#         download_dir = os.path.expanduser("~/Downloads")  # adjust if needed

#         try:
#             self.driver.get(url)
#         except WebDriverException as e:
#             print(f"Selenium failed to open URL: {e}")
#             return

#         # Wait for download to complete
#         time.sleep(5)  # can be increased if needed

#         # Find the newest .gz file in the download directory
#         downloaded_file = None
#         for f in sorted(os.listdir(download_dir), key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True):
#             if f.endswith(".gz"):
#                 downloaded_file = os.path.join(download_dir, f)
#                 break

#         if not downloaded_file or not os.path.exists(downloaded_file):
#             print("No .gz file found in download directory.")
#             return

#         # Move to target folder with renamed filename
#         os.makedirs(folder, exist_ok=True)
#         shutil.move(downloaded_file, filepath)
#         upload_file_to_s3(self.provider_name, file_entry["branch"], filepath)


import os
import time
import glob
import gzip
import shutil
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from Base import *

# Portable base (relative to this file)
PROJECT_DIR = os.path.dirname(__file__)
PROVIDERS_ROOT = os.path.abspath(os.path.join(PROJECT_DIR, "providers", "Yohananof"))
os.makedirs(PROVIDERS_ROOT, exist_ok=True)

def gzip_xml(xml_path: str, gz_path: str) -> str:
    with open(xml_path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    return gz_path

class YohananofCrawler(CrawlerBase):

    def __init__(self, login_url, username):
        super().__init__(login_url, "Yohananof")  # provider name for S3
        self.login_url = login_url
        self.username = username

        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        self.driver = webdriver.Chrome(options=options)

    def _set_download_dir(self, folder: str):
        os.makedirs(folder, exist_ok=True)
        # Ensure headless downloads go to 'folder'
        self.driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": folder}
        )

    def login(self):
        self.driver.get(self.login_url)
        self.driver.find_element(By.ID, "username").send_keys(self.username)
        self.driver.find_element(By.ID, "login-button").click()
        WebDriverWait(self.driver, 20).until(
            EC.invisibility_of_element_located((By.XPATH, "//div[text()='Processing...']"))
        )
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//tr[starts-with(@id, 'Price')]"))
        )

    def extract_file_links(self):
        self.login()
        rows = self.driver.find_elements(
            By.XPATH, "//tr[starts-with(@id, 'Price') or starts-with(@id, 'Promo')]"
        )
        found = {"pricesFull": None, "promoFull": None}

        for row in rows:
            try:
                link = row.find_element(By.TAG_NAME, "a")
                href = link.get_attribute("href")
                filename = link.get_attribute("title").strip()  # may show .gz or .xml
            except Exception as e:
                print(f"Failed to extract download link: {e}")
                continue

            # Accept BOTH .gz and .xml
            if not (filename.lower().endswith(".gz") or filename.lower().endswith(".xml")):
                continue

            try:
                branch = filename.split("-")[1]  # e.g., "001"
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

    def _wait_for_new_file(self, folder: str, before_set, timeout=90):
        """Wait until a new .gz or .xml appears in folder (and no .crdownload remains)."""
        start = time.time()
        while time.time() - start < timeout:
            # ignore partials
            if any(name.endswith(".crdownload") for name in os.listdir(folder)):
                time.sleep(0.5)
                continue

            current = set(
                os.path.join(folder, f)
                for f in os.listdir(folder)
                if f.lower().endswith(".gz") or f.lower().endswith(".xml")
            )
            new_files = list(current - before_set)
            if new_files:
                new_files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                return new_files[0]
            time.sleep(0.5)
        return None

    def download_file(self, file_entry):
        url = file_entry["url"]
        branch = file_entry.get("branch", "unknown")

        # Branch-specific folder under providers/Yohananof
        folder = os.path.join(PROVIDERS_ROOT, branch)
        self._set_download_dir(folder)

        # Snapshot existing .gz/.xml to detect the new one
        before = set(
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(".gz") or f.lower().endswith(".xml") or f.endswith(".crdownload")
        )

        try:
            self.driver.get(url)
        except WebDriverException as e:
            print(f"Selenium failed to open URL: {e}")
            return

        # Find what actually arrived (gz or xml)
        downloaded_path = self._wait_for_new_file(folder, before, timeout=90)
        if not downloaded_path or not os.path.exists(downloaded_path):
            print(f"No .gz/.xml file appeared in {folder}.")
            return

        # Normalize names with timestamp base (gz only)
        base = f"{file_entry['type']}_{self.get_timestamp()}"
        final_gz  = os.path.join(folder, base + ".gz")

        try:
            if downloaded_path.lower().endswith(".xml"):
                # Server gave XML → gzip it, then remove XML (keep only gz)
                temp_xml = downloaded_path
                gzip_xml(temp_xml, final_gz)
                try:
                    os.remove(temp_xml)
                except OSError:
                    pass
                print(f"Downloaded XML and gzipped → {final_gz}")

            elif downloaded_path.lower().endswith(".gz"):
                # Server gave GZ → just rename to our final gz name
                os.replace(downloaded_path, final_gz)
                print(f"Downloaded GZ → {final_gz}")

            else:
                print(f"Unexpected extension: {downloaded_path}")
                return

        except Exception as e:
            print(f"Post-download processing failed: {e}")
            print(f"Folder listing: {os.listdir(folder)}")
            return

        # Verify and upload gz ONLY
        if not os.path.exists(final_gz):
            print(f"Expected gz not found: {final_gz}")
            print(f"Folder listing: {os.listdir(folder)}")
            return

        upload_file_to_s3(self.provider_name, branch, final_gz)

