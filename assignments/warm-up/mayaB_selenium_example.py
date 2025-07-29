import platform, json, re, os, base64
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta

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

def setCromeDriver():
    chrome_options = init_chrome_options()
    try:
        chromedriver_path = get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        print("Trying alternative approach...")
        driver = webdriver.Chrome(options=chrome_options)
    return driver

def findElements(soup):
    articles = []
    numArticles = soup.find_all("div", attrs={"role": "heading", "aria-level": "3"})
    for i in range(len(numArticles)):
        title = soup.find_all("div", {"role": "heading", "aria-level": "3"})[i].get_text(strip=True)
        description = soup.find_all("div", style=lambda s: s and "-webkit-line-clamp" in s)[i].get_text(strip=True)
        ts = soup.find_all("span", attrs={"data-ts": True})[i].get("data-ts")
        date = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
        img_tag = soup.select('div.lSfe4c')[i].find_all('img')[0]
        img_url = img_tag.get("src") if img_tag else None
        img_src = img_url.split(",")[1] if img_url and img_url.startswith("data:image") else None

        article = {
        "title": title,
        "description": description,
        "date": date,
        "img_src": img_src
        }
        articles.append(article)
    return articles
    
def saveToJsonFile(articles):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "all_articles.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    
def crawl():
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"
    print(f"Navigating to {url}")
    driver = setCromeDriver()
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    articles = findElements(soup)
    saveToJsonFile(articles)   
        
if __name__ == "__main__":
    crawl()