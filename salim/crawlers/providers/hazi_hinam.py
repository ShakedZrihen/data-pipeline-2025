import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from crawlers.base import CrawlerBase
from crawlers.utils.html_utils import extract_file_links
from crawlers.utils.file_utils import (create_provider_dir, download_file, extract_file_info, build_page_url)
import requests
from bs4 import BeautifulSoup

PROVIDER_URL = "https://shop.hazi-hinam.co.il/Prices"
BASE_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "local_files")
PROVIDER_NAME = "hazi-hinam"

class HaziHinamCrawler(CrawlerBase):
    def get_total_pages_url_format(self, base_url: str) -> int:
        """Detect total number of pages for the site"""
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        page_links = [
            int(a.get_text(strip=True))
            for a in soup.select("ul.pagination li a")
            if a.get_text(strip=True).isdigit()
        ]
        if page_links:
            return max(page_links)

        return 1
    

    def get_all_pages_file_links(self, base_url: str):
        """Iterates through all pages and returns filtered links for PriceFull/PromoFull."""
        total_pages = self.get_total_pages_url_format(base_url)
        print(f"Found {total_pages} pages")

        all_links = []

        for page_num in range(1, total_pages + 1):
            url = build_page_url(base_url, page_num)
            print(f"Fetching {url}...")
            response = requests.get(url)
            response.raise_for_status()

            page_links = extract_file_links(response.text, base_url)

            # Only files with a valid name and a gz extension
            valid_links = [
                link for link in page_links
                if link["name"] and link["url"].endswith(".gz")
            ]
            all_links.extend(valid_links)

        # Filter by PriceFull/PromoFull
        return [
            link for link in all_links
            if link["name"].lower().startswith("pricefull")
            or link["name"].lower().startswith("promofull")
        ]


    def download_files_from_html(self, page_html=None):
        filtered_links = self.get_all_pages_file_links(self.providers_base_url)

        provider_dir = create_provider_dir(BASE_FOLDER, PROVIDER_NAME)
        files_info = []

        for link in filtered_links:
            file_url = link["url"]
            file_name = link["name"]
            file_local_path = os.path.join(provider_dir, file_name)
            
            download_file(file_url, file_local_path)

            file_info = extract_file_info(PROVIDER_NAME, file_name, file_local_path)
            files_info.append(file_info)
        
        with open(os.path.join(provider_dir, "file_info.json"), "w", encoding="utf-8") as f:
            json.dump(files_info, f, indent=4, ensure_ascii=False)

        return provider_dir


crawler = HaziHinamCrawler(PROVIDER_URL)
crawler.run(PROVIDER_URL)
