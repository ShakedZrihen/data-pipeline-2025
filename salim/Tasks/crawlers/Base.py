from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
import re
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

        # service = Service(ChromeDriverManager().install())
        driver_path = os.getenv("CHROMEDRIVER", "/usr/bin/chromedriver")
        service = Service(executable_path=driver_path)
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

    def last_token_ts12(name: str) -> str:
        # take the last '-' token before extension, strip quotes/newlines
        base = os.path.basename(name)
        stem, _ = os.path.splitext(base)
        token = stem.rsplit('-', 1)[-1].strip(' "\'')
        token = re.sub(r"\s+", " ", token)

        # already digits?
        if re.fullmatch(r"\d{12}", token):  # YYYYMMDDhhmm
            return token
        if re.fullmatch(r"\d{14}", token):  # YYYYMMDDhhmmss
            return token[:12]

        # "DD/MM/YYYY HH:MM"
        m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4}) (\d{1,2}):(\d{2})", token)
        if m:
            d, mo, y, h, mi = map(int, m.groups())
            return f"{y:04d}{mo:02d}{d:02d}{h:02d}{mi:02d}"

        # "HH:MM DD/MM/YYYY"
        m = re.fullmatch(r"(\d{1,2}):(\d{2}) (\d{1,2})/(\d{1,2})/(\d{4})", token)
        if m:
            h, mi, d, mo, y = map(int, m.groups())
            return f"{y:04d}{mo:02d}{d:02d}{h:02d}{mi:02d}"

        raise ValueError(f"Unrecognized last token: {token!r}")

    @abstractmethod
    def download_file(self, entry):
        pass


    @abstractmethod
    def extract_file_links(self):
        pass
