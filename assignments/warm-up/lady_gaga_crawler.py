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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests
import json
from datetime import datetime


def init_chrome_options():
    chrome_options = Options()

    # Set up headless Chrome
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")

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
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"
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

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "SoaBEf"))
    ) # delay to avoid aggressive crawling

    soup = BeautifulSoup(driver.page_source, "html.parser")
    with open("page.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)

    articles = []
    results = soup.find_all("div", class_="SoaBEf")

    for result in results:
        article = {}
        
        title_tag = result.find("div", class_="n0jPhd ynAwRc MBeuO nDgy9d")
        article["title"] = title_tag.get_text(strip=True) if title_tag else None
        print(article["title"])

        description_tag = result.find("div", class_="GI74Re nDgy9d")
        article["description"] = description_tag.get_text(strip=True) if description_tag else None
        
        date_wrapper = result.find("div", class_="OSrXXb rbYSKb LfVVr")
        date_tag = date_wrapper.find("span", attrs={"data-ts": True}) if date_wrapper else None

        if date_tag and date_tag.has_attr("data-ts"):
            timestamp = int(date_tag["data-ts"])
            dt = datetime.fromtimestamp(timestamp)  # According to the computer's local time
            article["date"] = dt.isoformat()
        else:
            article["date"] = None

        img_tag = result.find("img")
        if img_tag and img_tag.has_attr("src") and not img_tag["src"].startswith("data:"):
            article["image"] = img_tag["src"]
        else:
            article["image"] = None

        articles.append(article)

    driver.quit()
    print(json.dumps(articles, indent=2, ensure_ascii=False))
    # Save to file
    with open("lady_gaga_news.json", "w") as f:
        json.dump(articles, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    crawl()
