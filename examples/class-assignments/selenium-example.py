
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


# h1 <- title v
# h4 <- author
# video <-- .player__container video
# content <- .article-content

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
    url = "https://www.nbcbayarea.com/news/local/lady-gaga-ozzy-osbourne-san-francisco-concert/3920853/"
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
  
    soup = BeautifulSoup(driver.page_source, "html.parser")

    article = {}

    article["title"] = soup.find("h1").get_text(strip=True)
    author_details = soup.find("h4").get_text(strip=True).split('•')
    article["author"] = author_details[0]
    article["published_at"] = author_details[1]
    article["updated_at"] = author_details[2]
    article["content"] = soup.find_all(class_="article-content")[0].get_text(strip=True)

    print(article)
    with open(f"{article['title']}.json", "w") as f:
        f.write(json.dumps(article))




if __name__ == "__main__":
    crawl() 