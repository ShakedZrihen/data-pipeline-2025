
import time
import json
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def init_driver(headless=True) -> webdriver.Chrome:
    """
    Initializes the Chrome WebDriver with headless option.
    :param headless: If True, runs Chrome in headless mode (no GUI).
    :return: Configured Chrome WebDriver instance.
    """
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def parse_article_block(article, index: int) -> dict:
    """
    Parses a single article block and extracts title, description, date, and image.
    :param article: A Selenium WebElement representing the article.
    :param index: Article index for logging.
    :return: Dictionary containing article info.
    """
    # --- Extract title ---
    title = ""
    try:
        title_elem = article.find_element(By.CSS_SELECTOR, 'div[role="heading"] span[dir="ltr"]')
        title = title_elem.text.strip()
    except:
        print(f"[{index}] Missing title element")

    # --- Extract description ---
    description = ""
    try:
        desc_elem = article.find_element(By.CSS_SELECTOR, 'div.GI74Re span[dir="ltr"]')
        description = desc_elem.text.strip()
    except:
        print(f"[{index}] Missing description element")

    # --- Extract date ---
    timestamp = ""
    try:
        date_elem = article.find_element(By.CSS_SELECTOR, 'span[data-ts]')
        ts_str = date_elem.get_attribute("data-ts")
        if ts_str:
            timestamp = datetime.datetime.fromtimestamp(int(ts_str)).strftime("%Y-%m-%d %H:%M:%S")
    except:
        print(f"[{index}] Missing or invalid date")

    # --- Extract image ---
    image = ""
    try:
        img_elem = article.find_element(By.CSS_SELECTOR, "img")
        image = img_elem.get_attribute("src") or ""
    except:
        print(f"[{index}] Missing image element")

    return {
        "title": title,
        "description": description,
        "date": timestamp,
        "image": image
    }


def scrape_google_news() -> list[dict]:
    """
    Scrapes Google News search results for 'Lady Gaga in the news'.
    Extracts title, description, date, and image from each article block.
    :return: List of dictionaries containing article data.
    """
    driver = init_driver(headless=True)
    driver.get("https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws")
    time.sleep(2)

    results = []
    article_blocks = driver.find_elements(By.CSS_SELECTOR, "div.SoaBEf")

    for i, article in enumerate(article_blocks, 1):
        try:
            article_data = parse_article_block(article, i)
            results.append(article_data)
        except Exception as e:
            print(f"[!] Skipping article {i} due to error:", e)
            continue

    driver.quit()
    return results


def save_to_json(data: list[dict], filename="lady_gaga_news.json") -> None:
    """
    Saves the scraped data to a JSON file.
    :param data: List of article dictionaries.
    :param filename: Output filename (default: lady_gaga_news.json).
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    news = scrape_google_news()
    save_to_json(news)
