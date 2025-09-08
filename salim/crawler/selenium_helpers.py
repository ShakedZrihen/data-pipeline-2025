import os
import time
from typing import List
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--window-size=1920,1080")

    chrome_bin = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
    if os.path.exists(chrome_bin):
        options.binary_location = chrome_bin

    chromedriver_path = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
    service = Service(executable_path=chromedriver_path, log_path="/tmp/chromedriver.log")

    driver = Chrome(service=service, options=options)
    return driver

def scroll_page_to_end(driver: webdriver.Chrome) -> None:
    last_height = 0
    while True:
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.4)
        current_height = driver.execute_script("return document.body.scrollHeight")
        if current_height == last_height:
            break
        last_height = current_height
        
def wait_for_files(driver: webdriver.Chrome) -> None:
    print("Loading file list...")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
                       "or contains(@href,'.csv') or contains(@href,'.gz')]")
        )
    )
    time.sleep(1.5)

def extract_file_links(driver: webdriver.Chrome) -> List[str]:
    print("Searching for downloadable files...")
    try:
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@href,'.pdf') or contains(@href,'.xlsx') "
            "or contains(@href,'.csv') or contains(@href,'.gz')]"
        )
        return list({link.get_attribute("href") for link in links if link.get_attribute("href")})
    except Exception as e:
        print(f"Error extracting links: {e}")
        return []
