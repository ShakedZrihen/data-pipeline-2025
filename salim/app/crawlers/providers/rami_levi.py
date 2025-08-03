import sys, os
import requests
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from app.crawlers.base import CrawlerBase
from app.crawlers.utils.file_utils import extract_file_info, download_file_with_session

PROVIDER_URL = "https://url.publishedprices.co.il/login"
PROVIDER_NAME = "ramilevi"

class RamiLeviCrawler(CrawlerBase):
    
    def __init__(self):
        super().__init__(PROVIDER_URL)
        self.session = requests.Session()
        self.base_url = "https://url.publishedprices.co.il"
        
    def get_page_source(self, provider_url):
        """Override base method to use login functionality"""
        chrome_options = self.init_chrome_options()
        chromedriver_path = self.get_chromedriver_path()
        
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            driver.get(provider_url)
            time.sleep(3)
            
            username_field = driver.find_element(By.ID, "username")
            password_field = driver.find_element(By.ID, "password")
            
            username_field.send_keys("RamiLevi")
            
            login_button = driver.find_element(By.ID, "login-button")
            login_button.click()
            
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "fileList"))
            )
            
            time.sleep(5)
            
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "#fileList tbody tr")) > 0
                )
            except:
                pass
            
            page_html = driver.page_source
            
            # Save cookies for file downloads
            cookies = driver.get_cookies()
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
                
            return page_html
            
        finally:
            driver.quit()
    
    def download_files_from_html(self, page_html):
        # Create provider directory like in original code
        base_provider_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "local_files", "ramilevy")
        os.makedirs(base_provider_dir, exist_ok=True)
        
        soup = BeautifulSoup(page_html, 'html.parser')
        
        table = soup.find('table', {'id': 'fileList'})
        
        if not table:
            return base_provider_dir
            
        tbody = table.find('tbody')
        if not tbody:
            return base_provider_dir
            
        rows = tbody.find_all('tr')
        
        files_info = []
        seen_filenames = set()
        
        for row in rows:
            link_element = row.find('a', class_='f')
            if not link_element:
                continue
                
            filename = link_element.get('title', '')
            file_href = link_element.get('href', '')
            
            if not (filename.endswith('.gz') or filename.endswith('.xml')):
                continue
            
            # Only process files that contain "Full" in their name
            if "Full" not in filename:
                continue
            
            # Skip duplicates
            if filename in seen_filenames:
                continue

            seen_filenames.add(filename)

            file_url = urljoin(self.base_url, file_href)
            
            # Download using custom function with session
            file_local_path = os.path.join(base_provider_dir, filename)
            success = download_file_with_session(self.session, file_url, file_local_path)
            
            if success:
                # Extract file info using utility function
                file_info = extract_file_info(PROVIDER_NAME, filename, file_local_path)
                files_info.append(file_info)
        
        # Save file info
        with open(os.path.join(base_provider_dir, "file_info.json"), "w", encoding="utf-8") as f:
            json.dump(files_info, f, indent=4, ensure_ascii=False)
        
        return base_provider_dir
    

if __name__ == "__main__":
    crawler = RamiLeviCrawler()
    crawler.run(PROVIDER_URL)
