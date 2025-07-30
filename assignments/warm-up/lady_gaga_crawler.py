# scriped at 30.07.25 18:58

"""
lady_gaga_crawler.py

Script to extract Google News articles related to "Lady Gaga" and save them to a JSON file.
"""

import json
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import os
import sys


# Config
SEARCH_URL = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"


# Logging setup
# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
def init_chrome_options():
    """Initialize headless Chrome options."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return chrome_options

def get_chromedriver_path():
    """Get the path to the ChromeDriver executable."""
    return ChromeDriverManager().install()

def crawl():
    """Main crawling function that extracts articles and saves them to JSON."""
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"
    chrome_options = init_chrome_options()

    logging.info("Launching browser...")
    service = Service(get_chromedriver_path())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        logging.info(f"Navigating to {url}")
        driver.get(url)
        time.sleep(3)  # Let page load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        article_divs = soup.find_all("div", class_="SoaBEf")

        if not article_divs:
            logging.warning("No articles found. Check page structure or class name.")
            return

        articles = []
        for article in article_divs:
            title_tag = article.find("div", class_="n0jPhd ynAwRc MBeuO nDgy9d")
            description_tag = article.find("div", class_="GI74Re nDgy9d")
            date_tag = article.find("span", attrs={"data-ts": True})
            
            date_str = None
            if date_tag and date_tag.has_attr("data-ts"):
                timestamp = int(date_tag["data-ts"])

                # Convert to local time (e.g., Israel)
                dt_local = datetime.fromtimestamp(timestamp, tz=ZoneInfo("Asia/Jerusalem"))
                date_str = dt_local.strftime("%Y-%m-%d")

            image_tag = article.find("img")

            article_data = {
                "title": title_tag.get_text(strip=True) if title_tag else None,
                "description": description_tag.get_text(strip=True) if description_tag else None,
                "date": date_str,
                "image": image_tag["src"] if image_tag and image_tag.has_attr("src") else None,
            }
            articles.append(article_data)
        
        # Save to JSON file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(script_dir, "lady_gaga_articles.json")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        
        print(f"Extracted {len(articles)} articles.")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    crawl()
