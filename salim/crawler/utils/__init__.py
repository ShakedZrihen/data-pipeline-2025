import os
import requests
import certifi
import urllib3
from requests.exceptions import SSLError

def download_file_from_link(link, output_dir, filename=None):
    if filename is None:
        filename = os.path.basename(link)
    output_path = os.path.join(output_dir, filename)
    try:
        response = requests.get(link, stream=True, verify=certifi.where())
    except SSLError:
        print(f"SSL verify failed for {link!r}, retrying without verification…")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.get(link, stream=True, verify=False)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded to {output_path}")
        return output_path
    else:
        print(f"Failed to download. Status code: {response.status_code}")
        return None