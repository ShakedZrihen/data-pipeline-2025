import os
import time
import platform
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import dateparser
import requests
import json
import re
import html


def init_chrome_options():
    chrome_options = Options()

    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    return chrome_options


def get_chromedriver_path():
    """Get the correct chromedriver path for the current system"""
    try:
        # For macOS ARM64, we need to specify the architecture
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            print("Detected macOS ARM64, using specific chromedriver...")
            # Use a more specific approach for ARM64 Macs
            from webdriver_manager.core.os_manager import ChromeType

            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()

        print(f"Chrome driver path: {driver_path}")
        return driver_path
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        # Fallback to system chromedriver if available
        return "chromedriver"


def crawl():
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws"
    chrome_options = init_chrome_options()

    print("Setting up Chrome driver...")
    try:
        chromedriver_path = get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        print("Trying alternative approach...")
        # Alternative approach without service
        driver = webdriver.Chrome(options=chrome_options)
    
    print(f"Navigating to {url}")
    driver.get(url)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    articles = []

    for result in soup.select("div.SoAPf"):
        article = {}

        title_elem = result.select_one("div.n0jPhd.ynAwRc.MBeuO.nDgy9d span")
        desc_elem = result.select_one("div.GI74Re.nDgy9d span")
        date_elem = result.select_one("div.OSrXXb.rbYSKb.LfVVr span")
        image_elem = result.select_one("img")  # Try this — update selector if needed

        article["title"] = title_elem.get_text(strip=True) if title_elem else None
        article["description"] = desc_elem.get_text(strip=True) if desc_elem else None
        if date_elem:
            raw_date = date_elem.get_text(strip=True)
            parsed_date = dateparser.parse(raw_date, languages=['he'])  # תומך בעברית כמו "לפני שנה"
            article["date"] = parsed_date.strftime('%Y-%m-%d') if parsed_date else raw_date
        else:
            article["date"] = None

        article["image"] = image_elem["src"] if image_elem and image_elem.has_attr("src") else None
        articles.append(article)

    filename = "lady_gaga_news.json"
    with open(filename, "w", encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(articles)} articles to {filename}")
    driver.quit()
    


if __name__ == "__main__":
    crawl() 