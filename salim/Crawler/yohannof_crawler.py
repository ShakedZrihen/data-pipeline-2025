import base
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
from utils import (
    convert_xml_to_json,
    download_file_from_link,
    extract_and_delete_gz,
)
class YohannofCrawler(base.CrawlerBase):
    def __init__(self,user_name):
        self.user_name=user_name
    def crawl(self,driver):
        driver.get("https://url.publishedprices.co.il/login") 
        username_field = driver.find_element(By.ID, "username")
        username_field.send_keys(self.user_name)
        btn=driver.find_element(By.ID, "login-button")
        btn.click()
        time.sleep(5)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        links=soup.find_all("a",class_="f")
        files_paths=[]
        for link in links:
            file=self.save_file(link)
            files_paths.append(file)
        return files_paths
   
    def get_driver(self):
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-blink-features=AutomationControlled")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        return driver
    