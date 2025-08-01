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


def extract_file_info(branch: str, file_name: str, file_local_path: str):
    file_type = "promos" if "promo" in file_name.lower() else "prices"

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

