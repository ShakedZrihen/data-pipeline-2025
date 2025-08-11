import os
import requests
import certifi
import urllib3
from requests.exceptions import SSLError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_file_from_link(link: str, output_dir: str, filename: str | None = None, session: requests.Session | None = None, verify_cert: bool = True) -> str | None:
    """
    Download a file to output_dir. If `filename` is provided, use it; else use the URL basename.
    Supports passing an authenticated `session` (e.g., built from Selenium cookies).
    Returns the full output path on success, or None on failure / HTML response.
    """
    os.makedirs(output_dir, exist_ok=True)
    out_name = filename or (os.path.basename(link) or "download.bin")
    out_path = os.path.join(output_dir, out_name)

    sess = session or requests.Session()
    verify = certifi.where() if verify_cert else False

    try:
        with sess.get(link, stream=True, timeout=60, verify=verify, allow_redirects=True) as r:
            if r.status_code != 200:
                print(f"Failed to download. Status code: {r.status_code}")
                return None

            # Read first chunk to sniff for unexpected HTML (e.g., login page)
            it = r.iter_content(chunk_size=8192)
            try:
                first_chunk = next(it)
            except StopIteration:
                print("Empty response body.")
                return None

            head = (first_chunk or b"")[:256].lower()
            if b"<!doctype html" in head or b"<html" in head:
                print("Got HTML instead of a file (likely not authenticated).")
                return None

            with open(out_path, "wb") as f:
                if first_chunk:
                    f.write(first_chunk)
                for chunk in it:
                    if chunk:
                        f.write(chunk)

        print(f"Downloaded to {out_path}")
        return out_path

    except SSLError as e:
        print(f"SSL error while downloading {link}: {e}")
        return None
    except Exception as e:
        print(f"Error downloading {link}: {e}")
        return None
