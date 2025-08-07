from .base import CrawlerBase
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup
from .utils import (
    convert_xml_to_json,
    download_file_from_link,
    extract_and_delete_gz,
)

class CerberusCrawler(CrawlerBase):
    def __init__(self, user_name):
        self.user_name = user_name

    def crawl(self, driver):
        driver.get("https://url.publishedprices.co.il/login")

        # Login
        username_field = driver.find_element(By.ID, "username")
        username_field.send_keys(self.user_name)
        login_button = driver.find_element(By.ID, "login-button")
        login_button.click()
        time.sleep(5)

        # Filter
        try:
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            wait = WebDriverWait(driver, 10)
            search_input = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input.form-control.input-sm"))
            )
            search_input.clear()
            search_input.send_keys("pricefull")

            # ✅ Wait for at least one matching file link to appear
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table#fileList a[href^="/file/d/"]'))
            )
            time.sleep(1)
            print("Searched for 'pricefull' and results loaded.")
        except Exception as e:
            print(f"Search failed: {e}")

        # Extract links
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        file_links = soup.select('table#fileList a[href^="/file/d/"]')
        files_paths = []

        for a_tag in file_links:
            file_link = a_tag["href"]
            if not file_link.startswith("http"):
                file_link = "https://url.publishedprices.co.il" + file_link
            print(f"Found file link: {file_link}")
            file_path = download_file_from_link(file_link, driver=driver)
            if file_path:
                file_path = extract_and_delete_gz(file_path)
                files_paths.append(file_path)

        print(f"Total valid file links found: {len(files_paths)}")
        return files_paths


    def get_driver(self):
        options = Options()
        options.add_argument("--headless=new")  # Use 'new' headless mode for Chrome 109+
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        return driver
