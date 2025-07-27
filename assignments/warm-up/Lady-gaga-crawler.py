
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

    time.sleep(3)  # Wait for the page to load
    soup = BeautifulSoup(driver.page_source, "html.parser")
    article_containers = soup.find_all('div', class_='SoaBEf')
    print(f"Found {len(article_containers)} articles.")
    
    articles = []
    for i, container in enumerate(article_containers):
            print(f"{i+1}/{len(article_containers)}")
            
            try:
                article = {}

                # Extract title
                title_element = container.find('div', class_='n0jPhd ynAwRc MBeuO nDgy9d')
                if title_element:
                    article['title'] = title_element.get_text(strip=True)
                else:
                    article['title'] = "No title found"
                
                # Extract description
                description_element = container.find('div', class_='GI74Re nDgy9d')
                if description_element:
                    article['description'] = description_element.get_text(strip=True)
                else:
                    article['description'] = "Description not found"

                # Extract date
                date_element = container.find('span', attrs={'data-ts': True})
                if date_element:
                    # Convert timestamp to readable date
                    timestamp = int(date_element['data-ts'])
                    from datetime import datetime
                    date_obj = datetime.fromtimestamp(timestamp)
                    article['date'] = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    article['date'] = "Date not found"
                
                # Extract imgage URL
                img_element = container.find('img')
                if img_element and img_element.get('src'):
                    article['image_url'] = img_element['src']
                else:
                    article['image_url'] = None

                # add the article to the list
                articles.append(article)

            except Exception as e:
                print(f"Error extracting article {i+1}: {e}")
                continue
            
    driver.quit()
    # save articles to a JSON file
    with open('lady_gaga_articles.json', 'w') as f:
        json.dump(articles, f, indent=4)


if __name__ == "__main__":
    crawl() 