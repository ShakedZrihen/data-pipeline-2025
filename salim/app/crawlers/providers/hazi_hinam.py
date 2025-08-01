import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from app.crawlers.base import CrawlerBase
from app.crawlers.utils.html_utils import extract_file_links
from app.crawlers.utils.file_utils import (create_provider_dir, download_file, extract_file_info)

PROVIDER_URL = "https://shop.hazi-hinam.co.il/Prices"
BASE_FOLDER = "salim/app/crawlers/local_files"
PROVIDER_NAME = "hazi-hinam"

class HaziHinamCrawler(CrawlerBase):
    def download_files_from_html(self, page_html):
        file_links = extract_file_links(page_html, self.providers_base_url)
        
        provider_dir = create_provider_dir(BASE_FOLDER, PROVIDER_NAME)
        files_info = []

        for link in file_links:
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
