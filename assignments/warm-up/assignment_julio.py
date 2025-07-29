
import os
import time
import platform
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import requests
import json

HEADLESS = True
TZ = ZoneInfo("Asia/Jerusalem")

def convert_relative_to_date(rel_str):
    """
    Turn strings from "3 minutes ago", "4 hours ago", "5 days ago" or "yesterday"
    into a DD/MM/YYYY %H:%M:%S date in the local TimeZone.
    """
    now = datetime.now(TZ)
    str = rel_str.lower().strip()

    if str == "yesterday":
        dt = now - timedelta(days=1)
    else:
        parts = str.split()
        # Expect formats: ["<n>", "<unit>", "ago"]
        if len(parts) >= 3 and parts[0].isdigit() and parts[-1] == "ago":
            n = int(parts[0])
            unit = parts[1]
            if unit.startswith("minute"):
                dt = now - timedelta(minutes=n)
            elif unit.startswith("hour"):
                dt = now - timedelta(hours=n)
            elif unit.startswith("day"):
                dt = now - timedelta(days=n)
            elif unit.startswith("week"):
                dt = now - timedelta(weeks=n)
            else:
                return "None"
        else:
            # fallback for unexpected formats
            return "None"

    return dt.strftime("%d/%m/%Y %H:%M:%S")

def init_chrome_options():
    chrome_options = Options()

    # Set up headless Chrome
    if HEADLESS:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Makes it that the website/browser is in english
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_experimental_option(
        "prefs",
        {"intl.accept_languages": "en,en_US"}
    )
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
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&hl=en&source=univ&tbo=u&sa=X"
    chrome_options = init_chrome_options()

    print("Setting up Chrome driver...")
    try:
        pass
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
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    articles   = soup.select("div.SoaBEf")
    results = []
    article_num = 1
    for article in  articles:
        title_tag = article.select_one("div.n0jPhd.ynAwRc.MBeuO.nDgy9d")
        desc_tag = article.select_one("div.GI74Re.nDgy9d")
        date_tag = article.select_one("div.OSrXXb.rbYSKb.LfVVr")
        img_tag = article.select_one("div.uhHOwf.BYbUcd img")

        title = title_tag.get_text(strip=True) if title_tag else "None"
        description = desc_tag.get_text(strip=True)  if desc_tag  else "None"
        date = convert_relative_to_date(date_tag.get_text(strip=True))  if date_tag  else "None"
        img_url = (img_tag.get("src") or img_tag.get("data-src")) if img_tag else "None"
        results.append({
            "id":          article_num,
            "title":       title,
            "description": description,
            "date":        date,
            "image":       img_url
        })
        article_num+=1

    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("Saved results.json")



if __name__ == "__main__":
    crawl() 