import os
import requests
from selenium.webdriver.common.action_chains import ActionChains


def build_page_url(base_url: str, page_num: int) -> str:
    if "hazi-hinam" in base_url:
        return f"{base_url}?p={page_num}"
    else:
        return f"{base_url}?page={page_num}"
    

def create_provider_dir(base_folder: str, provider_name) -> str:
    """
    Creates the folder where the files will be saved
    """
    provider_dir = os.path.join(base_folder, provider_name)
    os.makedirs(provider_dir, exist_ok=True)
    return provider_dir


def download_file(url: str, dest_path: str):
    """
    Downloads a file from the URL and saves it Locally to the given path
    """
    response = requests.get(url)
    response.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(response.content)


def download_file_with_session(session, url, file_path):
    """
    Download a file using an authenticated requests.Session (after login).
    If verify=False is used, SSL certificate errors are ignored.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://url.publishedprices.co.il',
        }

        response = session.get(url, headers=headers, stream=True, verify=False)
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        return True

    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False
        

def transfer_cookies(driver, session):
    """Passes cookies from Selenium to requests.Session"""
    for cookie in driver.get_cookies():
        session.cookies.set(cookie["name"], cookie["value"])


def extract_file_info(branch: str, file_name: str, file_local_path: str):
    # Handle TivTaam specific file types
    if "pricefull" in file_name.lower() or "price" in file_name.lower():
        file_type = "prices"
    elif "promofull" in file_name.lower() or "promo" in file_name.lower():
        file_type = "promos"
    elif "stores" in file_name.lower():
        file_type = "stores"
    else:
        file_type = "prices"  # default

    return {
        "file_path": file_local_path,
        "branch": branch,
        "file_type": file_type,
    }


def extract_branch_and_timestamp(path_or_name: str):
    file_name = os.path.basename(path_or_name)
    name_without_ext = os.path.splitext(file_name)[0]
    parts = name_without_ext.split("-")

    # format of ...-ID-YYYYMMDDHHMM
    if len(parts[-1]) == 12:  
        branch = parts[-2]
        timestamp_raw = parts[-1]

    # format of ...-000-ID-YYYYMMDD-HHMMSS
    elif len(parts[-1]) == 6 and len(parts[-2]) == 8:
        branch = parts[-3]
        timestamp_raw = parts[-2] + parts[-1]  # Connectors date + time

    else:
        branch = "unknown"
        timestamp_raw = "000000000000"

    # Convert to format with _
    full_date_time = (
        f"{timestamp_raw[:4]}_{timestamp_raw[4:6]}_{timestamp_raw[6:8]}_"
        f"{timestamp_raw[8:10]}_{timestamp_raw[10:12]}"
    )

    return branch, full_date_time

