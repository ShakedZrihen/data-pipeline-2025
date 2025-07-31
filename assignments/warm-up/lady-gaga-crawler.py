import os
import time
from datetime import datetime
import platform
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json

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

def crawl_taylor_swift_news():
    """
    Crawl Google News for Lady Gaga articles and extract structured data.

    Scrapes articles from Google News search results, extracting title, description,
    date, and image URL for each article. Saves results to JSON file.

    Returns:
        None: Results are saved to 'lady_gaga_news_crawl.json'

    Raises:
        Exception: If crawling fails due to network or parsing issues
    """
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

    try:
        print(f"Navigating to {url}")
        driver.get(url)

        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        articles = soup.find_all("div", class_="SoaBEf")
        article_list = []

        for i, article in enumerate(articles):
            try:
                article_data = {}
            # get title
                title = article.find('div',class_='n0jPhd ynAwRc MBeuO nDgy9d')
                if title:
                    article_data['title'] = title.get_text(strip=True)
                else:
                    article_data['title'] = "No title found"

                #get description
                description = article.find('div', class_='GI74Re nDgy9d')
                if description:
                    article_data['description'] = description.get_text(strip=True)
                else:
                    article_data['description'] = "No description found"

                #get date
                date = article.find('span', attrs={'data-ts': True})
                if date:
                    # Convert timestamp to date
                    timestamp = int(date['data-ts'])
                    date_obj = datetime.fromtimestamp(timestamp)
                    article_data['date'] = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    article_data['date'] = "Date not found"

                #get image url
                img_url = article.find('img')
                if img_url:
                    article_data['img_url'] = img_url.get('src')
                else:
                    article_data['img_url'] = "No image found"

                article_list.append(article_data)
                print(f"Processed article {i+1}/{len(articles)}: {article_data['title'][:50]}")
            #article specific error
            except Exception as e:
                print(f"Error processing article {i}: {e}")
                continue

        print(f"Total articles found {len(article_list)}")
        with open("lady_gaga_news_crawl.json", "w",encoding="utf-8") as f:
            json.dump(article_list, f, indent=4, ensure_ascii=False)

    #global error handling
    except Exception as e:
        print(f"Error during crawling articles: {e}")
    finally:
        driver.quit()
        print("Chrome driver closed.")



if __name__ == "__main__":
    crawl_taylor_swift_news()