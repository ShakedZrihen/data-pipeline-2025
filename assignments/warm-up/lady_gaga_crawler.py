#Importing necessary libraries
import os
import time
import json
import requests
import platform
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.parse import urljoin
from datetime import datetime, timezone
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager



def init_chrome_options():
    chrome_options = Options()

    # Set up headless Chrome
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")

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
    time.sleep(1)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    news = soup.find_all('div', attrs={'class': 'SoaBEf'})

    data = []

    for article in news:
        new_article = {}

        title = article.find("div", class_="n0jPhd ynAwRc MBeuO nDgy9d")
        new_article["title"] = title.text if title else None

        description = article.find("div", class_="GI74Re nDgy9d")
        new_article["description"] = description.text if description else None

        image = article.find("img")
        new_article["image"] = image["src"] if image else None

        date = article.find("span", attrs={"data-ts": True})
        if date:

            try:
                timestamp = int(date["data-ts"])
                new_article["date"] = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

            except Exception as e:
                new_article["date"] = None

        else:
            new_article["date"] = None

        data.append(new_article)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print("\nCrawling successfully finished!")


if __name__ == "__main__":
    crawl()