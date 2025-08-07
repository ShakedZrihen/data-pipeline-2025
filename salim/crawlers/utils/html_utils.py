from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_file_links(page_html: str, provider_base_url: str):
    soup = BeautifulSoup(page_html, "html.parser")
    provider_files = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()

        if any(href.lower().endswith(ext) for ext in [".gz", ".xml", ".zip", ".xlsx", ".xls", ".pdf"]):
            file_url = urljoin(provider_base_url, href)
            file_name = href.split("/")[-1]

            provider_files.append({"url": file_url, "name": file_name})

    return provider_files