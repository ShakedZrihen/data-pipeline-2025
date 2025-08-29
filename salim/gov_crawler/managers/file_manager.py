import os
import requests
import certifi
import urllib3
from requests.exceptions import SSLError
from urllib.parse import urlparse

class FileManager:
    def __init__(self):
        pass

    def download_file_from_link(self, link, output_dir, filename=None, session=None, verify_cert=True):
        # Use provided session or create a new one
        if session is None:
            s = requests.Session()
        else:
            s = session

        # Figure out filename
        if filename is None:
            url_path = urlparse(link).path
            base = os.path.basename(url_path)
            if base:
                filename = base
            else:
                filename = "downloaded.file"

        # Make sure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_path = os.path.join(output_dir, filename)

        # Try to download with SSL verification
        try:
            if verify_cert:
                verify = certifi.where()
            else:
                verify = False
            response = s.get(link, stream=True, verify=verify, allow_redirects=True)
        except SSLError:
            print(f"SSL error for {link}, trying without verification")
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            response = s.get(link, stream=True, verify=False, allow_redirects=True)

        # Check response status
        if response.status_code != 200:
            print(f"Download failed. Status code: {response.status_code}")
            return None

        # Check if response is HTML
        content_type = response.headers.get("Content-Type", "").lower()
        if "text/html" in content_type:
            print("Response is HTML, not a file.")
            return None

        # Write file
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"File downloaded to {output_path}")
        return output_path