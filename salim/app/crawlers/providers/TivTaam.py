import sys, os
import requests
import json
import gzip
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
import re
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from app.crawlers.base import CrawlerBase

PROVIDER_URL = "https://url.publishedprices.co.il/login"

class TivTaamCrawler(CrawlerBase):
    
    def __init__(self):
        super().__init__(PROVIDER_URL)
        self.session = requests.Session()
        self.base_url = "https://url.publishedprices.co.il"
        
    def login_and_get_file_page(self):
        chrome_options = self.init_chrome_options()
        chromedriver_path = self.get_chromedriver_path()
        
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            driver.get(PROVIDER_URL)
            time.sleep(3)
            
            username_field = driver.find_element(By.ID, "username")
            password_field = driver.find_element(By.ID, "password")
            
            username_field.send_keys("TivTaam")
            
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
            
            cookies = driver.get_cookies()
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
                
            return page_html
            
        finally:
            driver.quit()
    
    def get_page_source(self, provider_url):
        """Override base method to use login functionality"""
        return self.login_and_get_file_page()
    
    def download_files_from_html(self, page_html):
        base_provider_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "local_files", "tivtaam")
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
        
        for row in rows:
            link_element = row.find('a', class_='f')
            if not link_element:
                continue
                
            filename = link_element.get('title', '')
            file_href = link_element.get('href', '')
            
            if not (filename.endswith('.gz') or filename.endswith('.xml')):
                continue
                
            file_url = urljoin(self.base_url, file_href)
            
            branch, file_type = self.parse_filename(filename)
            
            file_path = self.download_file(file_url, filename, base_provider_dir)
            
            if file_path:
                files_info.append({
                    "file_path": file_path,
                    "branch": branch,
                    "file_type": file_type,
                    "full_date_time": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                })
        
        file_info_path = os.path.join(base_provider_dir, "file_info.json")
        with open(file_info_path, 'w', encoding='utf-8') as f:
            json.dump(files_info, f, indent=2, ensure_ascii=False)
        
        return base_provider_dir
    
    def parse_filename(self, filename):
        name_without_ext = filename.replace('.gz', '').replace('.xml', '')
        
        pattern = r'(Price|PriceFull|Promo|PromoFull|Stores)(\d+)-(\d+)-'
        match = re.match(pattern, name_without_ext)
        
        if match:
            original_type = match.group(1)
            chain_id = match.group(2)
            branch = match.group(3)
            
            if original_type in ['Price', 'PriceFull']:
                file_type = "prices"
            elif original_type in ['Promo', 'PromoFull']:
                file_type = "promos"
            elif original_type == 'Stores':
                file_type = "stores"
            else:
                file_type = "unknown"
            
            return branch, file_type
        else:
            return "unknown", "unknown"
    
    def download_file(self, url, filename, download_dir):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://url.publishedprices.co.il/file',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = self.session.get(url, headers=headers, stream=True, verify=False)
            response.raise_for_status()
            
            file_path = os.path.join(download_dir, filename)
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return file_path
            
        except Exception as e:
            return None
    
if __name__ == "__main__":
    crawler = TivTaamCrawler()
    crawler.run()