from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime


def init_chrome_options():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return chrome_options

def get_chromedriver_path():
    return ChromeDriverManager().install()

def crawl():
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws"
    chrome_options = init_chrome_options()

    print("Launching browser...")
    service = Service(get_chromedriver_path())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        print(f"Navigating to {url}")
        driver.get(url)
        time.sleep(2)  # Let page load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        first_article = soup.find("div", class_="SoaBEf")

        if first_article:
            title_tag = first_article.find("div", class_="n0jPhd ynAwRc MBeuO nDgy9d")
            description_tag = first_article.find("div", class_="GI74Re nDgy9d")
            date_tag = first_article.find("span", attrs={"data-ts": True})
            date_str = None
            if date_tag and date_tag.has_attr("data-ts"):
                timestamp = int(date_tag["data-ts"])
                date_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
            image_tag = first_article.find("img")

            article_data = {
                "title": title_tag.get_text(strip=True) if title_tag else None,
                "description": description_tag.get_text(strip=True) if description_tag else None,
                "date": date_str,
                "image": image_tag["src"] if image_tag and image_tag.has_attr("src") else None,
            }

            print("First article data:")
            print(article_data)
        else:
            print("No article found")

    finally:
        driver.quit()

if __name__ == "__main__":
    crawl()
