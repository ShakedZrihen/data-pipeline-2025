import os
import json
import time
import sys
import requests
import urllib3
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium import webdriver

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from crawlers.base import CrawlerBase
from crawlers.utils.html_utils import extract_file_links
from crawlers.utils.file_utils import (create_provider_dir, extract_file_info, download_file_with_session, transfer_cookies)

LOGIN_URL = "https://url.publishedprices.co.il/login"
PROVIDER_URL = "https://url.publishedprices.co.il/file"
BASE_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "local_files")
PROVIDER_NAME = "yohananof"

class YohananofCrawler(CrawlerBase):
    def __init__(self, provider_url):
        super().__init__(provider_url)
        self.session = requests.Session()
    
    def login_and_get_driver(self):
        """Open browser, log in, and navigate to the files page."""
        chrome_options = self.init_chrome_options()
        chromedriver_path = self.get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get(LOGIN_URL)
        time.sleep(2)

        username_input = driver.find_element(By.ID, "username")
        username_input.send_keys(PROVIDER_NAME)

        login_button = driver.find_element(By.ID, "login-button")
        login_button.click()
        
        # Wait for the next page to load
        time.sleep(3)
        return driver
    

    def get_page_source_authenticated(self, provider_url):
        driver = self.login_and_get_driver()
        transfer_cookies(driver, self.session)
        driver.get(provider_url)
        time.sleep(2)
        html = driver.page_source
        driver.quit()
        return html


    def get_page_source(self, provider_url):
        return self.get_page_source_authenticated(provider_url)

    def download_files_from_html(self, page_html):
        file_links = extract_file_links(page_html, self.providers_base_url)
        
        filtered_links = [
            link for link in file_links
            if link["name"].lower().startswith("pricefull") or link["name"].lower().startswith("promofull")
        ]
        
        provider_dir = create_provider_dir(BASE_FOLDER, PROVIDER_NAME)
        files_info = []

        for link in filtered_links:
            file_url = link["url"]
            file_name = link["name"]
            file_local_path = os.path.join(provider_dir, file_name)
            
            success = download_file_with_session(self.session, file_url, file_local_path)
            if not success:
                print(f"Skipping {file_name}, download failed")
                continue

            file_info = extract_file_info(PROVIDER_NAME, file_name, file_local_path)
            files_info.append(file_info)
        
        with open(os.path.join(provider_dir, "file_info.json"), "w", encoding="utf-8") as f:
            json.dump(files_info, f, indent=4, ensure_ascii=False)

        return provider_dir


crawler = YohananofCrawler(PROVIDER_URL)
crawler.run(PROVIDER_URL)
