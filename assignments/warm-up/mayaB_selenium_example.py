import platform
import json
import re
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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
    
    # Automatically download and manage Chrome driver
    print("Setting up Chrome driver...")
    try:
        # pass
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
    article["title"] = soup.find_all(class_="n0jPhd ynAwRc MBeuO nDgy9d")[2].get_text(strip=True)
    article["description"] = soup.find_all(class_="GI74Re nDgy9d")[2].get_text(strip=True)
    article["date"] = soup.find_all(class_="OSrXXb rbYSKb LfVVr")[2].get_text(strip=True)
    article["img_url"] = soup.find_all(class_="uhHOwf BYbUcd")[2].find("img").get("src")
        
    script_dir = os.path.dirname(os.path.abspath(__file__))
    safe_title = re.sub(r'[\\/*?:"<>|]', "_", article["title"])
    file_path = os.path.join(script_dir, f"{safe_title}.json")
    
    with open(file_path, "w") as f:
        f.write(json.dumps(article))
        
if __name__ == "__main__":
    crawl()