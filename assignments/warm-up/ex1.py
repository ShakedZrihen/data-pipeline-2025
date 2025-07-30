import time
import platform
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json

TZ = ZoneInfo("Asia/Jerusalem")
HEADLESS = True

def init_chrome_options():
    chrome_options = Options()
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

def convert_relative_to_date(rel_str):
    rel_str = rel_str.lower().strip()
    current_time = datetime.now(TZ)

    if rel_str == "yesterday":
        result_time = current_time - timedelta(days=1)

    elif "ago" in rel_str:
        tokens = rel_str.split()
        if len(tokens) >= 3 and tokens[0].isdigit():
            amount = int(tokens[0])
            unit = tokens[1]

            unit_seconds = {
                "minute": 60,
                "hour":   3600,
                "day":    86400,
                "week":   604800,
            }

            matched = [unit_key for unit_key in unit_seconds if unit.startswith(unit_key)]
            if matched:
                seconds = unit_seconds[matched[0]] * amount
                result_time = current_time - timedelta(seconds=seconds)
            else:
                return "No date found"
        else:
            return "No date found"

    else:
        return "No date found"

    return result_time.strftime("%d/%m/%Y %H:%M")

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
    time.sleep(1)
  
    soup = BeautifulSoup(driver.page_source, "html.parser")
    articles = soup.select("div.SoaBEf")
    results = []
    for article in articles:
        title_tag = article.select_one("div.n0jPhd.ynAwRc.MBeuO.nDgy9d")
        description_tag = article.select_one("div.GI74Re.nDgy9d")
        date_tag = article.select_one("div.OSrXXb.rbYSKb.LfVVr span")
        img_tag = article.select_one("div.uhHOwf.BYbUcd img")
        
        title = title_tag.get_text(strip=True) if title_tag else "No title found"
        description = description_tag.get_text(strip=True) if description_tag else "No description found"
        date = date_tag.get_text(strip=True) if date_tag else "No date found"
        img_url = img_tag['src'] if img_tag and 'src' in img_tag.attrs else "No image found"
        if date != "No date found":
            date = convert_relative_to_date(date)
        results.append({
            "title": title,
            "description": description,
            "date": date,
            "img_url": img_url
        })
        
        print(results)
        with open("articles.json", "w") as f:
            f.write(json.dumps(results))
        
        
if __name__ == "__main__":
    crawl() 