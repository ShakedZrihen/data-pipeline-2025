
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
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import requests
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Jerusalem")
HEADLESS = False

def init_chrome_options():
    chrome_options = Options()

    # Set up headless Chrome
    if HEADLESS:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_experimental_option(
        "prefs",
        {"intl.accept_languages": "en,en_US"}
    )

    return chrome_options


def get_chromedriver_path():
    """Get the correct chromedriver path for the current system"""
    try:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            print("Detected macOS ARM64, using specific chromedriver...")
            from webdriver_manager.core.os_manager import ChromeType

            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()

        print(f"Chrome driver path: {driver_path}")
        return driver_path
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        return "chromedriver"


def convert_relative_to_date(rel_str):
    now = datetime.now(TZ)
    s = rel_str.lower().strip()
    if s == "yesterday":
        dt = now - timedelta(days=1)
    else:
        parts = s.split()
        if len(parts) >= 3 and parts[0].isdigit() and parts[-1] == "ago":
            n = int(parts[0])
            unit = parts[1]
            sec_map = {
                "minute": 60,
                "hour":   3600,
                "day":    86400,
                "week":   604800
            }
            total_secs = 0
            for base, seconds in sec_map.items():
                if unit.startswith(base):
                    total_secs = n * seconds
                    break
            dt = now - timedelta(seconds=total_secs)
        else:
            return "No date found"

    return dt.strftime("%d/%m/%Y %H:%M")

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
        driver = webdriver.Chrome(options=chrome_options)
    
    print(f"Navigating to {url}")
    driver.get(url)
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    articles = soup.select("div.SoaBEf")
    articles_data = []
    
    for article_div in articles:
        title = article_div.select_one("div.n0jPhd.ynAwRc.MBeuO.nDgy9d")
        description = article_div.select_one("div.GI74Re.nDgy9d")
        date = article_div.select_one("div.OSrXXb.rbYSKb.LfVVr")
        image_url = article_div.select_one("div.lSfe4c.r5bEn.aI5QMe img")

        title_text = title.get_text(strip=True) if title else "No title found"
        description_text = description.get_text(strip=True) if description else "No description found" 
        date_text = date.get_text(strip=True) if date else "No date found"
        image_src = image_url['src'] or image_url.get('data-src') or image_url.get("data-iurl") if image_url else "No image found"

        if date_text != "No date found":
            date_text = convert_relative_to_date(date_text)
       
        articles_data.append({
            "title": title_text,
            "description": description_text,
            "date": date_text,
            "image_url": image_src
        })

    print(articles_data)
    with open("articles_data.json", "w", encoding="utf-8") as f:
        json.dump(articles_data, f, ensure_ascii=False, indent=2)



if __name__ == "__main__":
    crawl() 