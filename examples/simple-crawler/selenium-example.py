import os
import time
import platform
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

from utils import (
    convert_xml_to_json,
    download_file_from_link,
    extract_and_delete_gz,
)


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
    url = "https://prices.mega.co.il/"
    download_base_url = "https://prices.carrefour.co.il/" # this sometimes changes so if it failed take a look at the page and update the url

    chrome_options = init_chrome_options()

    # Automatically download and manage Chrome driver
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

    # Select branch with value "0084"
    print("Selecting branch...")
    select = Select(driver.find_element("id", "branch_filter"))
    select.select_by_value("0084")
    branch_name = select.first_selected_option.text.strip()
    print(f"Selected branch: {branch_name}")

    # Wait for page to update
    print("Waiting for page to update...")
    time.sleep(3)

    # Get the HTML after JS has rendered
    print("Getting page source...")
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    price_tags = soup.find_all("a", class_="downloadBtn")
    print(f"Found {len(price_tags)} download links")

    output_dir = os.path.join("prices", branch_name)
    os.makedirs(output_dir, exist_ok=True)

    for i, a_tag in enumerate(price_tags, 1):
        if a_tag and a_tag.has_attr("href"):
            href = a_tag["href"]
            link = urljoin(download_base_url, href)
            print(f"[{i}/{len(price_tags)}] Downloading {link}...")
            output_path = download_file_from_link(link, output_dir)
            print(f"Output path: {output_path}")
            if output_path:
                print(f"Extracting {output_path}...")
                output_path = extract_and_delete_gz(output_path)
                if output_path:
                    convert_xml_to_json(output_path)
        else:
            print("Download link not found.")


if __name__ == "__main__":
    crawl()
