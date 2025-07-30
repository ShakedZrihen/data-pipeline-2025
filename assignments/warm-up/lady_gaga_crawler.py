import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import json
from utils import translate_and_parse_date  

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
    """Get the ChromeDriver path using webdriver-manager (Windows-compatible)"""
    try:
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
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    articles = []   

    for block in soup.find_all(class_="lSfe4c r5bEn aI5QMe"):
        article = {}

        title_tag = block.find(class_="n0jPhd ynAwRc MBeuO nDgy9d")
        if title_tag:
            article["title"] = title_tag.get_text(strip=True)
        else:
            article["title"] = "No title found"

        desc_tag = block.find(class_="GI74Re nDgy9d")
        if desc_tag:
            article["description"] = desc_tag.get_text(strip=True)
        else:
            article["description"] = "No description found"

        date_tag = block.find(class_="OSrXXb rbYSKb LfVVr")
        if date_tag:
            date = date_tag.get_text(strip=True)

            parsed_date = translate_and_parse_date(date)  
            article["date"] = parsed_date
        else:
            article["date"] = "No date found"

        # image- there are two img but the first one is the picture of the article
        img_tag = block.find("img")
        if img_tag and img_tag.get("src"):
            article["image"] = img_tag.get("src")
        else:
            article["image"] = "No image found"


        articles.append(article)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

            
if __name__ == "__main__":
    crawl() 