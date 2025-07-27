
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
import re


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

def convert_relative_date_to_absolute(text):
    text = text.strip()
    now = datetime.now()

    if "לפני" in text:
        # days
        if "יומיים" in text:
            return (now - timedelta(days=2)).strftime("%Y-%m-%d")
        elif re.search(r"\d+ ימים", text):
            days = int(re.search(r"\d+", text).group())
            return (now - timedelta(days=days)).strftime("%Y-%m-%d")
        elif "יום" in text:
            return (now - timedelta(days=1)).strftime("%Y-%m-%d")

        # hours
        elif "שעתיים" in text:
            return (now - timedelta(hours=2)).strftime("%Y-%m-%d")
        elif re.search(r"\d+ שעות", text):
            hours = int(re.search(r"\d+", text).group())
            return (now - timedelta(hours=hours)).strftime("%Y-%m-%d")
        elif "שעה" in text:
            return (now - timedelta(hours=1)).strftime("%Y-%m-%d")

        # minutes
        elif "דקות" in text or "דקה" in text:
            return now.strftime("%Y-%m-%d")

    return text


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
        driver = webdriver.Chrome(options=chrome_options)
    
    print(f"Navigating to {url}")
    driver.get(url)
  
    soup = BeautifulSoup(driver.page_source, "html.parser")

    articles = []

    for a_tag in soup.find_all("a", class_="WlydOe"):
        article = {}

        title_div = a_tag.find("div", class_="n0jPhd ynAwRc MBeuO nDgy9d")
        article["title"] = title_div.get_text(strip=True) if title_div else ""

        
        desc_div = a_tag.find("div", class_="GI74Re nDgy9d")
        article["description"] = desc_div.get_text(strip=True) if desc_div else ""
        date_div = a_tag.find("div", class_="OSrXXb rbYSKb LfVVr")
        if date_div and date_div.find("span"):
            raw_date = date_div.find("span").get_text(strip=True)
            article["date"] = convert_relative_date_to_absolute(raw_date)
        else:
            article["date"] = ""

        article["link"] = a_tag.get("href", "")

        img_tag = a_tag.find("img")
        article["image"] = img_tag.get("src", "") if img_tag else ""

        articles.append(article)

    for i, art in enumerate(articles, 1):
        print(f"Article #{i}")
        print(f"Title: {art['title']}")
        print(f"Description: {art['description']}")

        date_div = a_tag.find("div", class_="OSrXXb rbYSKb LfVVr")
        if date_div and date_div.find("span"):
            raw_date = date_div.find("span").get_text(strip=True)
            article["date"] = convert_relative_date_to_absolute(raw_date)
        else:
            article["date"] = ""
            print(f"Link: {art['link']}")
    img_info = art['image']
    if img_info:
        print(f"Image: yes, length = {len(img_info)} chars")
    else:
        print("Image: no")
    print("-" * 40)


    with open("lady_gaga_news.json", "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    driver.quit()


if __name__ == "__main__":
    crawl()