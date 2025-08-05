import json
import os
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
# from .selenium_driver import SeleniumDriver


class SupermarketCrawler:
    def __init__(self, config_name: str):
        """
        Initialize crawler with specific supermarket config

        Args:
            config (dict): Supermarket configuration
        """
        self.config_name = config_name
        self.config = self.load_config(config_name)
        self.session = requests.Session()
        self.setup_session()

    def load_config(self, config_name: str) -> dict:
        """
        Load configuration from JSON file

        Args:
            config_name (str): Name of the configuration file

        Returns:
            dict: Configuration dictionary
        """
        config_path = os.path.join(os.path.dirname(__file__), 'configs', f'{config_name}.json')
        with open(config_path, 'r') as f:
            return json.load(f)

    def setup_session(self):
        """
        Setup session with necessary headers and cookies
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session.headers.update(headers)

def crawl(self, url: str) -> List[str]:
    """
    Main crawling method - find and download .gz files
    
    Args:
        url (str): URL to crawl for .gz files (we'll use file_listing_url from config)
        
    Returns:
        List[str]: List of downloaded .gz file paths
    """
    # 1. Login first (since this site requires authentication)
    if not self.login():
        print("Failed to login")
        return []
    
    # 2. Get the file listing page
    file_listing_url = self.config.get('file_listing_url')
    response = self.session.get(file_listing_url)
    
    # 3. Check if successful
    if response.status_code != 200:
        print(f"Failed to fetch file listing. Status code: {response.status_code}")
        return []
    
    # 4. Find .gz files in the HTML
    gz_urls = self.find_gz_files(response.text)
    
    # 5. Get the latest files for each branch
    latest_files = self.get_latest_files(gz_urls)
    
    # 6. Download each .gz file
    downloaded_files = []
    for gz_url in latest_files:
        file_path = self.download_gz_file(gz_url)
        if file_path:
            downloaded_files.append(file_path)
    
    return downloaded_files