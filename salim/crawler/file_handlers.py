import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, unquote
from typing import List
import requests
import ssl
from tqdm import tqdm
from settings import s3, S3_BUCKET

def get_timestamp_from_url(url: str) -> datetime:
    match = re.search(r"(\d{12})$", urlsplit(url).path.split("/")[-1])
    return datetime.strptime(match.group(1), "%Y%m%d%H%M") if match else datetime.min

def classify_file(url: str) -> str:
    filename = urlsplit(url).path.lower().split("/")[-1]
    if "promofull" in filename:
        return "promo_full"
    if "pricefull" in filename:
        return "price_full"
    if "promo" in filename and "full" not in filename:
        return "promo"
    if "price" in filename and "full" not in filename:
        return "price"
    return "other"

def select_recent_files(urls: List[str]) -> List[str]:
    print(f"Selecting recent files... {len(urls)}")
    price_files = [url for url in urls if classify_file(url) == "price"]
    promo_files = [url for url in urls if classify_file(url) == "promo"]
    price_full_files = [url for url in urls if classify_file(url) == "price_full"]
    promo_full_files = [url for url in urls if classify_file(url) == "promo_full"]

    print(f"Found {len(price_files)} price files, {len(promo_files)} promo files, {len(price_full_files)} price_full files, and {len(promo_full_files)} promo_full files.")
    price_files.sort(key=get_timestamp_from_url, reverse=True)
    promo_files.sort(key=get_timestamp_from_url, reverse=True)
    price_full_files.sort(key=get_timestamp_from_url, reverse=True)
    promo_full_files.sort(key=get_timestamp_from_url, reverse=True)

    selected = []
    if len(promo_full_files) > 0:
        selected.append(promo_full_files[0])
    if len(price_full_files) > 0:
        selected.append(price_full_files[0])
    if len(promo_files) > 0 and len(promo_full_files) == 0:
        selected.append(promo_files[0])
    if len(price_files) > 0 and len(price_full_files) == 0:
        selected.append(price_files[0])
    return selected
# Download Handling
def get_safe_filename(url: str) -> str:
    return unquote(os.path.basename(urlsplit(url).path))

def download_and_save_file(url: str, provider_folder: str) -> None:
    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        
        filename = get_safe_filename(url)
        timestamp = re.search(r"(\d{12})", filename)
        timestamp = timestamp.group(1) if timestamp else "000000000000"
        branch = re.search(r"(\d{13}-\d{3})", filename)
        branch = branch.group(1) if branch else "unknown_branch"
        file_type = "pricesFull" if "price" in filename.lower() else "promoFull"
        new_filename = f"{file_type}_{timestamp}.gz"

        folder_path = Path("downloads") / provider_folder / branch
        folder_path.mkdir(parents=True, exist_ok=True)
        local_path = folder_path / new_filename
        
        
        with requests.get(url, stream=True, verify=False, timeout=40) as response:
            response.raise_for_status()
            with open(local_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
        print(f"Saved file to {local_path}")
        
        try:
            # Upload to S3
            s3_key = f"providers/{branch}/{new_filename}"
            s3.upload_file(local_path, S3_BUCKET, s3_key)
            print(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload to S3: {e}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def process_downloads(urls: List[str], provider_folder: str) -> None:
    print(f"Processing downloads for {urls}...")
    if not urls:
        print("No files to download.")
        return
    for url in tqdm(urls, desc="Downloading"):
        print(f"Downloading {url}...")
        download_and_save_file(url, provider_folder)
