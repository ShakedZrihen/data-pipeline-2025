import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from utils import download_file_from_link, extract_and_delete_gz, convert_xml_to_json


def crawl():
    url = "https://prices.mega.co.il/"
    download_base_url = "https://prices.carrefour.co.il/" # this sometimes changes so if it failed take a look at the page and update the url
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch page. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    price_tags = soup.find_all("a", class_="downloadBtn")
    
    output_dir = "prices"
    os.makedirs(output_dir, exist_ok=True)

    for a_tag in price_tags:
        if a_tag and a_tag.has_attr("href"):
            href = a_tag["href"]
            link = urljoin(download_base_url, href)
            print(f"Downloading {link}...")
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
