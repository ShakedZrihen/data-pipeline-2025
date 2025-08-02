import os
import time
import platform
import json
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def init_chrome_options():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return chrome_options

def get_chromedriver_path():
    try:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            from webdriver_manager.core.os_manager import ChromeType
            return ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            return ChromeDriverManager().install()
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        return "chromedriver"

def crawl():
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"
    chrome_options = init_chrome_options()
    print("Setting up Chrome driver...")

    chromedriver_path = get_chromedriver_path()
    driver = webdriver.Chrome(service=Service(chromedriver_path), options=chrome_options)

    print(f"Navigating to {url}")
    driver.get(url)
    time.sleep(5)  

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    titles = soup.find_all("div", class_="n0jPhd ynAwRc MBeuO nDgy9d")
    descriptions = soup.find_all("div", class_="GI74Re nDgy9d")
    dates_divs = soup.find_all("div", class_="OSrXXb rbYSKb LfVVr")

    image_blocks = soup.find_all(class_="uhHOwf BYbUcd")
    
    image_urls = []
    for img_block in image_blocks:
        img = img_block.find('img')
        if img and img.has_attr('src'):
            image_urls.append(img['src'])
        else:
            image_urls.append(None)
    
    dates = []
    for date_div in dates_divs:
        ts = date_div.find("span").get("data-ts")
        date = datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
        dates.append(date)

    num_results = min(20, len(titles), len(descriptions), len(dates))
    results = []
    for i in range(num_results):
        
        image_url = image_urls[i] if i < len(image_urls) else None
        
        results.append({
            "title": titles[i].get_text(strip=True),
            "description": descriptions[i].get_text(strip=True),
            "date": dates[i],
            "image": image_url
        })

    with open("lady_gaga_news.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    print("Starting the crawler...")
    crawl()
