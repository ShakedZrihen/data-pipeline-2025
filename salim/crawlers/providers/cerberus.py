import os
import json
import time
import sys
import requests
import urllib3
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from crawlers.base import CrawlerBase

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from crawlers.base import CrawlerBase
from crawlers.utils.html_utils import extract_file_links
from crawlers.utils.file_utils import (
    create_provider_dir, extract_file_info, download_file_with_session, transfer_cookies
)

LOGIN_URL = "https://url.publishedprices.co.il/login"
PROVIDER_URL = "https://url.publishedprices.co.il/file"
BASE_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "local_files")

SUPERMARKETS = {
    "yohananof": "yohananof",
    "tiv_taam": "tivtaam",
    "rami_levy": "ramilevi",
    "osher_ad": "osherad",
    "keshet_tamim": "keshet",
}

BRANCH_WHITELIST = {
    "yohananof": ["032", "035", "028", "027", "029", "025", "005", "015", "021", "019"],
    "tiv_taam":  ["089","019","091","083","084","014","020","079","087","085"],
    "rami_levy": ["013","016","017","043","045","050","055","056","703","059"],
    "osher_ad": ["010","011","020","022","024","025","013", "014","017","018"],
    "keshet_tamim":  ["005","013","017","019","020","022","024","102","104","105"],
}

def extract_branch_code(file_name: str):
    m = re.search(r'[_-](\d{3})[_-]', file_name)
    if m:
        return m.group(1)
    m = re.search(r'[_-](\d{3})(?:\.|$)', file_name)
    return m.group(1) if m else None

def pick_first_per_branch(links, allowed_branches):
    seen = {}
    for link in links:
        code = CerberusCrawler.extract_branch_code(link["name"])
        if code in allowed_branches and code not in seen:
            seen[code] = link
    return list(seen.values())

class CerberusCrawler(CrawlerBase):
    def __init__(self, supermarket: str , username: str ):
        super().__init__()
        self.base_url = PROVIDER_URL
        self.session = requests.Session()
        self.supermarket = supermarket
        self.username = username

    def login_and_get_driver(self):
        chrome_options = self.init_chrome_options()
        chromedriver_path = self.get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get(LOGIN_URL)
        time.sleep(2)

        username_input = driver.find_element(By.ID, "username")
        username_input.clear()
        username_input.send_keys(self.username)

        login_button = driver.find_element(By.ID, "login-button")
        login_button.click()
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
        file_links = extract_file_links(page_html, self.base_url)
        filtered_links = [
            link for link in file_links
            if link["name"].lower().startswith("pricefull")
            or link["name"].lower().startswith("promofull")
        ]
            
        allowed_branches = set(BRANCH_WHITELIST.get(self.supermarket, []))
        if allowed_branches:
            filtered_links = [
                link for link in filtered_links
                if (extract_branch_code(link["name"]) in allowed_branches)
            ]

        selected = {}  # key: (branch, kind) -> link
        for link in filtered_links:
            n = link["name"].lower()
            if n.startswith("pricefull"):
                kind = "price"
            elif n.startswith("promofull"):
                kind = "promo"
            else:
                continue

            branch = extract_branch_code(link["name"])
            if not branch:
                continue

            key = (branch, kind)
            if key not in selected:  # לוקחים את הראשון בלבד
                selected[key] = link

        filtered_links = list(selected.values())
            
        provider_dir = create_provider_dir(BASE_FOLDER, self.supermarket)
        files_info = []

        for link in filtered_links:
            file_url = link["url"]
            file_name = link["name"]
            file_local_path = os.path.join(provider_dir, file_name)

            success = download_file_with_session(self.session, file_url, file_local_path)
            if not success:
                print(f"Skipping {file_name}, download failed")
                continue

            file_info = extract_file_info(self.supermarket, file_name, file_local_path)
            files_info.append(file_info)

        with open(os.path.join(provider_dir, "file_info.json"), "w", encoding="utf-8") as f:
            json.dump(files_info, f, indent=4, ensure_ascii=False)

        return provider_dir

    def run(self):
        return super().run(self.base_url)
    
if __name__ == "__main__":
    for market, uname in SUPERMARKETS.items():
        print(f"=== Running crawler for {market} ({uname}) ===")
        crawler = CerberusCrawler(supermarket=market, username=uname)
        crawler.run()
