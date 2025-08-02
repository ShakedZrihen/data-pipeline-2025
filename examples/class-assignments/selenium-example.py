
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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import requests
import json
import re
import html


# h1 <- title v
# h4 <- author
# video <-- .player__container video
# content <- .article-content

def init_chrome_options():
    chrome_options = Options()

    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

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


def extract_video_urls(driver):
    """Extract video URLs from the page using multiple strategies"""
    video_urls = []
    wait = WebDriverWait(driver, 10)
    try:
        video_playlist_elements = driver.find_elements(By.CSS_SELECTOR, '[data-react-component="VideoPlaylist"]')
        for element in video_playlist_elements:
            data_props = element.get_attribute("data-props")
            if data_props:
                decoded_props = html.unescape(data_props)
                try:
                    props_data = json.loads(decoded_props)
                    if "videos" in props_data:
                        for video in props_data["videos"]:
                            print(f"Video data: {json.dumps(video, indent=2)}")
                            if "mp4Url" in video and video["mp4Url"]:
                                video_urls.append(video["mp4Url"])
                                print(f"Found MP4 URL: {video['mp4Url']}")
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON from data-props: {e}")
    except Exception as e:
        print(f"Error in NBC-specific extraction: {e}")    
    return list(set(video_urls))


def download_mp4_video(url, filename):
    try:
        response = requests.get(url, stream=True, allow_redirects=True)
        response.raise_for_status()

        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)        
    except Exception as e:
        print(f"Error downloading MP4 video: {e}")
        return False


def crawl():
    url = "https://www.nbcbayarea.com/news/local/lady-gaga-ozzy-osbourne-san-francisco-concert/3920853/"
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
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    time.sleep(3)

    print("Extracting video URLs...")
    video_urls = extract_video_urls(driver)
    soup = BeautifulSoup(driver.page_source, "html.parser")

    article = {}

    try:
        article["title"] = soup.find("h1").get_text(strip=True)
        author_details = soup.find("h4").get_text(strip=True).split('•')
        article["author"] = author_details[0]
        article["published_at"] = author_details[1]
        article["updated_at"] = author_details[2]
        article["content"] = soup.find_all(class_="article-content")[0].get_text(strip=True)
        article["video_urls"] = video_urls
    except Exception as e:
        print(f"Error extracting article data: {e}")
        article["title"] = "Error extracting title"
        article["video_urls"] = video_urls
    
    filename = f"{article['title'].replace('/', '_')}.json"

    with open(filename, "w") as f:
        f.write(json.dumps(article))
    
    if video_urls:
        for i, video_url in enumerate(video_urls):
            video_filename = f"{article['title'].replace('/', '_').replace(':', '_')}_video_{i+1}.mp4"
            print(f"\nDownloading video {i+1} of {len(video_urls)}:")
            download_mp4_video(video_url, video_filename)
    else:
        print("No MP4 videos found to download")
    
    driver.quit()




if __name__ == "__main__":
    crawl() 