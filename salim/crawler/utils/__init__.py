import os
import requests
import certifi
import urllib3
from requests.exceptions import SSLError
from urllib.parse import urlparse


def download_file_from_link(link, output_dir, filename=None, session=None, verify_cert=True):
    """
    Stream-download a file using requests (optionally an authenticated session).
    - If `session` is None, a new Session is used (backwards-compatible).
    - If `filename` is None, derive from the URL path (query stripped).
    Returns the output path, or None on failure / HTML response.
    """
    s = session or requests.Session()

    # decide output filename
    if filename is None:
        base = os.path.basename(urlparse(link).path)  # strips ?query
        filename = base or "downloaded.file"

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    try:
        verify = certifi.where() if verify_cert else False
        response = s.get(link, stream=True, verify=verify, allow_redirects=True)
    except SSLError:
        print(f"SSL verify failed for {link!r}, retrying without verification…")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = s.get(link, stream=True, verify=False, allow_redirects=True)

    if response.status_code != 200:
        print(f"Failed to download. Status code: {response.status_code}")
        return None

    # avoid saving login HTML as a .gz
    ctype = (response.headers.get("Content-Type") or "").lower()
    if "text/html" in ctype:
        print("Got HTML instead of a file (likely not authenticated).")
        return None

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print(f"Downloaded to {output_path}")
    return output_path
