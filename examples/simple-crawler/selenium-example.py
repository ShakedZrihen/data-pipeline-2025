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


def crawl_category(
    driver, category_value, category_name, download_base_url, max_pages, branch_name
):
    """Crawl a specific category and return statistics"""
    print(f"\n{'='*60}")
    print(f"STARTING CRAWL FOR CATEGORY: {category_name}")
    print(f"{'='*60}")

    # Select category filter
    print(f"Selecting category filter: {category_name}...")
    try:
        category_select = Select(driver.find_element("id", "cat_filter"))
        category_select.select_by_value(category_value)
        print(f"Selected category: {category_name}")

        # Wait for page to update after category selection
        print("Waiting for page to update after category selection...")
        time.sleep(3)
    except Exception as e:
        print(f"Error selecting category filter: {e}")
        print("Continuing without category filter...")

    # Create output directory using branch name (keeping existing structure)
    output_dir = os.path.join("prices", branch_name)
    os.makedirs(output_dir, exist_ok=True)

    total_successful = 0
    total_failed = 0
    page_num = 1

    while page_num <= max_pages:
        print(f"\n{'-'*40}")
        print(f"Processing Page {page_num} - {category_name}")
        print(f"{'-'*40}")

        # Get download links from current page
        download_links = get_download_links_from_page(driver, download_base_url)
        print(f"Found {len(download_links)} download links on page {page_num}")

        if not download_links:
            print(f"No download links found on page {page_num}. Stopping.")
            break

        # Download files from current page using existing utils functions
        for i, link in enumerate(download_links, 1):
            print(f"[{i}/{len(download_links)}] Downloading {link}...")
            output_path = download_file_from_link(link, output_dir)
            print(f"Output path: {output_path}")

            if output_path:
                try:
                    print(f"Extracting {output_path}...")
                    output_path = extract_and_delete_gz(output_path)
                    if output_path:
                        convert_xml_to_json(output_path)
                        total_successful += 1
                        print(f"✅ Successfully processed: {output_path}")
                except Exception as e:
                    print(f"❌ Error processing {output_path}: {e}")
                    total_failed += 1
            else:
                total_failed += 1
                print(f"❌ Failed to download: {link}")

        print(f"Page {page_num} summary: {len(download_links)} files processed")

        # Try to navigate to next page
        if page_num < max_pages:
            try:
                print(f"Looking for next page button (page {page_num + 1})...")
                next_button = get_next_page_button(driver, page_num)

                if next_button and next_button.is_enabled():
                    print(
                        f"Found next page button. Clicking to navigate to page {page_num + 1}..."
                    )
                    next_button.click()
                    time.sleep(3)  # Wait for page to load
                    page_num += 1
                else:
                    print("No next page button found or it's disabled. Stopping.")
                    break

            except Exception as e:
                print(f"Error navigating to next page: {e}")
                break
        else:
            print(f"Reached maximum page limit ({max_pages}). Stopping.")
            break

    print(f"\n{'-'*40}")
    print(f"CATEGORY {category_name} COMPLETE")
    print(f"{'-'*40}")
    print(f"Total pages processed: {page_num}")
    print(f"Total successful downloads: {total_successful}")
    print(f"Total failed downloads: {total_failed}")
    print(f"Output directory: {output_dir}")

    return {
        "category": category_name,
        "pages_processed": page_num,
        "successful_downloads": total_successful,
        "failed_downloads": total_failed,
        "output_dir": output_dir,
    }


def crawl():
    url = "https://prices.mega.co.il/"
    download_base_url = "https://prices.carrefour.co.il/"  # this sometimes changes so if it failed take a look at the page and update the url
    max_pages = 2

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

    try:
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

        # Define categories to crawl
        categories = [
            {"value": "pricefull", "name": "PriceFull"},
            {"value": "promofull", "name": "PromoFull"},
        ]

        all_results = []

        # Crawl each category
        for category in categories:
            result = crawl_category(
                driver=driver,
                category_value=category["value"],
                category_name=category["name"],
                download_base_url=download_base_url,
                max_pages=max_pages,
                branch_name=branch_name,
            )
            all_results.append(result)

        # Print final summary
        print(f"\n{'='*60}")
        print(f"FINAL CRAWLING SUMMARY")
        print(f"{'='*60}")

        total_successful = sum(r["successful_downloads"] for r in all_results)
        total_failed = sum(r["failed_downloads"] for r in all_results)
        total_pages = sum(r["pages_processed"] for r in all_results)

        for result in all_results:
            print(
                f"{result['category']}: {result['successful_downloads']} successful, {result['failed_downloads']} failed, {result['pages_processed']} pages"
            )

        print(
            f"\nTOTAL: {total_successful} successful, {total_failed} failed, {total_pages} pages processed"
        )
        print(f"Categories processed: {len(all_results)}")

    except Exception as e:
        print(f"Error during crawling: {e}")
    finally:
        driver.quit()
        print("Chrome driver closed.")


if __name__ == "__main__":
    crawl()
