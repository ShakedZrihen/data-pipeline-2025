import os
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select

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


def crawl():
    url = "https://prices.mega.co.il/"
    chrome_options = init_chrome_options()

    chromedriver_path = os.path.join(
        os.path.dirname(__file__), "chromedriver-mac-arm64", "chromedriver"
    )

    service = Service(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)

    # Select branch with value "0084"
    select = Select(driver.find_element("id", "branch_filter"))
    select.select_by_value("0084")
    branch_name = select.first_selected_option.text.strip()

    # Wait for page to update
    time.sleep(3)

    # Get the HTML after JS has rendered
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    price_tags = soup.find_all("a", class_="downloadBtn")

    output_dir = os.path.join("prices", branch_name)
    os.makedirs(output_dir, exist_ok=True)

    for a_tag in price_tags:
        if a_tag and a_tag.has_attr("href"):
            href = a_tag["href"]
            link = urljoin(url, href)
            print(f"Downloading {link}...")
            output_path = download_file_from_link(link, output_dir)
            if output_path:
                print(f"Extracting {output_path}...")
                output_path = extract_and_delete_gz(output_path)
                convert_xml_to_json(output_path)
        else:
            print("Download link not found.")


if __name__ == "__main__":
    crawl()
