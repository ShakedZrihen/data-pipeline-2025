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

def find_pagination_elements(driver):
    """Find pagination elements to determine total pages"""
    try:
        # Look for pagination buttons with the specific format
        pagination_buttons = driver.find_elements(
            By.CSS_SELECTOR, "button.paginationBtn"
        )

        if pagination_buttons:
            print(f"Found {len(pagination_buttons)} pagination buttons")
            return pagination_buttons

        # Fallback to other pagination selectors if the specific format isn't found
        pagination_selectors = [
            "nav[aria-label='pagination']",
            ".pagination",
            ".pager",
            "[class*='pagination']",
            "[class*='pager']",
        ]

        for selector in pagination_selectors:
            try:
                pagination = driver.find_element(By.CSS_SELECTOR, selector)
                page_links = pagination.find_elements(By.TAG_NAME, "a")
                if page_links:
                    return page_links
            except NoSuchElementException:
                continue

        # If no pagination found, return None
        return None
    except Exception as e:
        print(f"Error finding pagination: {e}")
        return None

def get_next_page_button(driver, current_page):
    """Find the next page button based on the specific format"""
    try:
        # Look for the next page button with data-page attribute
        next_page_num = current_page + 1
        next_button = driver.find_element(
            By.CSS_SELECTOR, f"button.paginationBtn[data-page='{next_page_num}']"
        )

        if next_button and next_button.is_enabled():
            return next_button

        # Alternative: look for button with onclick containing the next page number
        all_pagination_buttons = driver.find_elements(
            By.CSS_SELECTOR, "button.paginationBtn"
        )
        for button in all_pagination_buttons:
            onclick_attr = button.get_attribute("onclick")
            if onclick_attr and f"changePage({next_page_num})" in onclick_attr:
                if button.is_enabled():
                    return button

        return None
    except NoSuchElementException:
        return None
    except Exception as e:
        print(f"Error finding next page button: {e}")
        return None

def get_download_links_from_page(driver, download_base_url):
    """Extract download links from the current page"""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    price_tags = soup.find_all("a", class_="downloadBtn")

    download_links = []
    for a_tag in price_tags:
        if a_tag and a_tag.has_attr("href"):
            href = a_tag["href"]
            link = urljoin(download_base_url, href)
            download_links.append(link)

    return download_links

def crawl_lady_gaga(driver): #getting all the info from the articles
    articles = driver.find_elements(By.CSS_SELECTOR, "div.SoAPf")
    results = []

    for article in articles:
        try:
            title = article.find_element(By.CSS_SELECTOR, "div.n0jPhd.ynAwRc.MBeuO.nDgy9d").text
        except:
            title = None

        try:
            description = article.find_element(By.CSS_SELECTOR, "div.GI74Re.nDgy9d").text
        except:
            description = None

        try:
            date = article.find_element(By.CSS_SELECTOR, "div.OSrXXb.rbYSKb.LfVVr").text
        except:
            date = None

        try:
            image = article.find_element(By.CSS_SELECTOR, "img").get_attribute("src")
        except:
            image = None

        results.append({
            "title": title,
            "description": description,
            "date": date,
            "image": image,
        })

        time.sleep(2)

    return results


def crawl():
    url = "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X&hl=en"

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

    try:
        print(f"Opening {url}")
        driver.get(url)

        articles = crawl_lady_gaga(driver)

        print(f"\n✅ Found {len(articles)} articles")
        for i, a in enumerate(articles, 1):
            print(f"\nArticle {i}")
            for k, v in a.items():
                print(f"{k}: {v}")

    finally:
        driver.quit()
        print("Chrome driver closed.")


if __name__ == "__main__":
    crawl()
