from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

class CrawlerBase(ABC):
    def __init__(self, base_url, provider_name):
        self.base_url = base_url
        self.provider_name = provider_name
        self.driver = None

    def init_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def run(self):
        self.init_driver()
        try:
            if self.driver is None:
                print("Driver initialization failed, exiting.")
                return
            self.driver.get(self.base_url)
            time.sleep(2)
            file_entries = self.extract_file_links()
            if not file_entries:
                print("No file entries found, exiting.")
                return
            for entry in file_entries:
                self.download_file(entry)
        finally:
            if self.driver is not None:
                self.driver.quit()
            else:
                print("Driver was not initialized, skipping quit.")



    @abstractmethod
    def download_file(self, entry):
        pass


    @abstractmethod
    def extract_file_links(self):
        pass
