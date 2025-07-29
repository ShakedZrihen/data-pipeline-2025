import json
import platform
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


# Initalizes default chrome options
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


class Crawler:
    """
    Lady Gaga Crawler class.
    """

    def __init__(self, url: str):
        self.url = url
        self.options = init_chrome_options()
        try:
            # Initialize drivers and headless browser
            chromedriver_path = get_chromedriver_path()
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=self.options)
            self.driver = driver
        except Exception as e:
            print(f"Failed to initialize Chrome driver: {e}")
            print("Trying alternative approach...")
            # Alternative approach without service
            driver = webdriver.Chrome(options=self.options)
            self.driver = driver

    def crawl(self):
        """
        crawls the Lady Gaga url news page.
        """
        try:
            print(f"Navigating to {self.url}")
            self.driver.get(self.url)

            elems = self.driver.find_elements(by=By.CLASS_NAME, value="SoaBEf")
            articles = []
            for i in range(len(elems)):
                data = {}
                print(f"scraping article #{i}: ")
                title = (
                    elems[i]
                    .find_element(by=By.CLASS_NAME, value="SoAPf")
                    .find_elements(by=By.TAG_NAME, value="div")[3]
                    .text
                )
                desc = (
                    elems[i]
                    .find_element(by=By.CLASS_NAME, value="SoAPf")
                    .find_elements(by=By.TAG_NAME, value="div")[4]
                    .text
                )
                date = (
                    elems[i]
                    .find_element(by=By.CLASS_NAME, value="SoAPf")
                    .find_element(by=By.XPATH, value=".//span[@data-ts]")
                    .get_attribute("data-ts")
                )
                date = int(date)
                img = (
                    elems[i]
                    .find_element(by=By.CLASS_NAME, value="gpjNTe")
                    .find_element(by=By.TAG_NAME, value="div")
                    .find_element(by=By.TAG_NAME, value="div")
                    .find_element(by=By.TAG_NAME, value="img")
                    .get_attribute("src")
                )
                data["title"] = title if title else ""
                data["desc"] = desc if desc else ""
                data["image"] = img if img else ""

                dt = datetime.fromtimestamp(date, tz=timezone.utc)
                dt_local = dt.astimezone(ZoneInfo("Asia/Jerusalem"))
                dt_local = dt_local.strftime("%d-%m-%Y, %H:%M:%S")
                data["date"] = dt_local if date else ""

                print(f"title: {title}")
                print(f"desc: {desc}")
                print(f"date: {dt_local}")
                articles.append(data)

            serialized_data = json.dumps(articles)
            print(f"Finished scraping {len(articles)} articles.")

            with open("assignments/warm-up/scraped.json", "w") as f:
                f.write(serialized_data)

            return articles

        except Exception as e:
            print(f"Error during crawl: {e}")
            return []
        finally:
            print("Closing driver...")
            self.driver.close()


if __name__ == "__main__":
    crwler = Crawler(
        "https://www.google.com/search?q=lady+gaga+in+the+news&tbm=nws&source=univ&tbo=u&sa=X"
    )
    crwler.crawl()
