import time
import platform
import json
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dateutil import parser



HEADLESS_MODE = True
SITE_URL = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X&hl=en"
TZ = ZoneInfo("Asia/Jerusalem")


def configure_chrome_options():
    """
    Configure Chrome browser options for Selenium.
    """
    options = Options()
    if HEADLESS_MODE:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=en-US")
    options.add_experimental_option("prefs", {"intl.accept_languages": "en,en_US"})
    return options


def get_compatible_chromedriver_path():
    """
    Detect platform and return the correct ChromeDriver path.
    """
    try:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            from webdriver_manager.core.os_manager import ChromeType
            return ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        return ChromeDriverManager().install()
    except Exception as error:
        print(f"WebDriver Manager error: {error}")
        return "chromedriver"


def launch_chrome_driver():
    """
    Initialize and return a Chrome WebDriver instance.
    """
    options = configure_chrome_options()
    try:
        service = Service(get_compatible_chromedriver_path())
        return webdriver.Chrome(service=service, options=options)
    except Exception as error:
        print(f"Failed to initialize WebDriver: {error}")
        return webdriver.Chrome(options=options)


def extract_article_data(article_element, article_index):
    """
    Extracts information from a single Google News article element.
    """

    title_element = article_element.select_one("div.n0jPhd.ynAwRc.MBeuO.nDgy9d")
    description_element = article_element.select_one("div.GI74Re.nDgy9d")
    date_element = article_element.select_one("div.OSrXXb, span.WG9SHc span")
    image_element = article_element.select_one("div.uhHOwf.BYbUcd img")

    return {
        "id": article_index,
        "title": title_element.get_text(strip=True) if title_element else "None",
        "description": description_element.get_text(strip=True) if description_element else "None",
        "date": convert_relative_to_date(date_element.get_text(strip=True)) if date_element else "None",
        "image_url": (image_element.get("src") or image_element.get("data-src")) if image_element else "None"
    }
   

def convert_relative_to_date(rel_str):
    """
    Convert relative time to date time
    """
    now = datetime.now(TZ)
    s = rel_str.lower().strip()

    if s == "yesterday":
        dt = now - timedelta(days=1)
    elif "ago" in s:
        parts = s.split()
        n = int(parts[0])
        if "minute" in s:
            dt = now - timedelta(minutes=n)
        elif "hour" in s:
            dt = now - timedelta(hours=n)
        elif "day" in s:
            dt = now - timedelta(days=n)
        elif "week" in s:
            dt = now - timedelta(weeks=n)
        else:
            print(f"DEBUG: Date string not recognized -> {rel_str}")
            return "None"
    else:
        try:
            dt = parser.parse(s)
        except:
            print(f"DEBUG: Date string not recognized -> {rel_str}")
            return "None"

    return dt.strftime("%d/%m/%Y %H:%M:%S")



def extract_all_articles(parsed_html):
    """
    Extracts all news articles from the Google News search results page.
    """
    article_elements = parsed_html.select("div.SoaBEf")
    extracted_articles = []

    for index, article in enumerate(article_elements, start=1):
        article_data = extract_article_data(article, index)
        extracted_articles.append(article_data)

    return extracted_articles


def save_articles_to_json_file(articles_data, filename="results.json"):
    """
    Saves extracted article data to a JSON file.
    """
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(articles_data, file, ensure_ascii=False, indent=2)
    print(f"Saved {len(articles_data)} articles to {filename}")


def crawl():
    """
    Main function to crawl Google News and save extracted data to JSON.
    """
    target_url = SITE_URL
    print("Initializing Chrome WebDriver")
    driver = launch_chrome_driver()

    print(f"Navigating to {target_url}")
    driver.get(target_url)
    time.sleep(2)

    parsed_html = BeautifulSoup(driver.page_source, "html.parser")
    articles_data = extract_all_articles(parsed_html)
    save_articles_to_json_file(articles_data)

    driver.quit()


if __name__ == "__main__":
    crawl()
