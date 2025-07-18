
import requests
from bs4 import BeautifulSoup
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LadyGagaCrawler:
    def __init__(self, url):
        self.url = url

    def fetch_page(self):
        try:
            logging.info(f'Fetching page: {self.url}')
            response = requests.get(self.url)
            response.raise_for_status()
            return response.text
        except requests.HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')
        except Exception as err:
            logging.error(f'Other error occurred: {err}')
        return None

    def parse_page(self, html_content):
        try:
            logging.info('Parsing page content')
            soup = BeautifulSoup(html_content, 'html.parser')
            headlines = [h2.get_text() for h2 in soup.find_all('h2')]
            return headlines
        except AttributeError as attr_err:
            logging.error(f'Attribute error: {attr_err}')
        except Exception as e:
            logging.error(f'Error parsing page: {e}')
        return []

    def run(self):
        html_content = self.fetch_page()
        if html_content:
            data = self.parse_page(html_content)
            logging.info(f'Extracted data: {data}')
            return data
        return []

if __name__ == '__main__':
    url = 'https://example.com/lady-gaga-news'  # Replace with actual URL
    crawler = LadyGagaCrawler(url)
    crawler.run()
