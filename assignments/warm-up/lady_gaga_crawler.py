import json
from datetime import datetime
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def configure_browser():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,800")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def extract_news_items(html):
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("div.SoaBEf")
    news = []

    for block in blocks:
        item = {
            "title": None,
            "description": None,
            "date": None,
            "image": None
        }

        title = block.select_one("div.n0jPhd.ynAwRc.MBeuO.nDgy9d")
        if title:
            item["title"] = title.get_text(strip=True)

        desc = block.select_one("div.GI74Re.nDgy9d")
        if desc:
            item["description"] = desc.get_text(strip=True)

        date_tag = block.select_one("div.OSrXXb.rbYSKb.LfVVr span[data-ts]")
        if date_tag and date_tag.has_attr("data-ts"):
            ts = int(date_tag["data-ts"])
            item["date"] = datetime.fromtimestamp(ts).isoformat()

        img = block.find("img")
        if img and img.has_attr("src"):
            item["image"] = img["src"]

        news.append(item)

    return news

def run_scraper():
    target = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws"
    browser = configure_browser()
    browser.get(target)

    try:
        WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "SoaBEf")))
        sleep(1.5)
        content = browser.page_source
        results = extract_news_items(content)
    finally:
        browser.quit()

    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(json.dumps(results, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    run_scraper()

