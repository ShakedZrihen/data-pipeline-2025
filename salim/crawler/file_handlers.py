import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, unquote
from typing import List
import requests
import ssl
import certifi
from tqdm import tqdm
from settings import s3, S3_BUCKET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_timestamp(url: str) -> datetime:
    
    filename = unquote(urlsplit(url).path.split("/")[-1])

    m = re.search(r"(\d{8}-\d{6})", filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d-%H%M%S")
        except Exception:
            pass

    m = re.search(r"(\d{14})", filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
        except Exception:
            pass

    m = re.search(r"(\d{12})", filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d%H%M")
        except Exception:
            pass

    m = re.search(r"(\d{8}-\d{4})", filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d-%H%M")
        except Exception:
            pass

    return datetime.min

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
    price_files = [url for url in urls if classify_file(url) == "price"]
    promo_files = [url for url in urls if classify_file(url) == "promo"]
    price_full_files = [url for url in urls if classify_file(url) == "price_full"]
    promo_full_files = [url for url in urls if classify_file(url) == "promo_full"]

    print(f"Found {len(price_files)} price files, {len(promo_files)} promo files, {len(price_full_files)} price_full files, and {len(promo_full_files)} promo_full files.")
    
    price_files.sort(key=get_timestamp, reverse=True)
    promo_files.sort(key=get_timestamp, reverse=True)
    price_full_files.sort(key=get_timestamp, reverse=True)
    promo_full_files.sort(key=get_timestamp, reverse=True)

    latest_price_full = max(price_full_files, key=get_timestamp) if price_full_files else None
    latest_price = max(price_files, key=get_timestamp) if price_files else None
    latest_promo_full = max(promo_full_files, key=get_timestamp) if promo_full_files else None
    latest_promo = max(promo_files, key=get_timestamp) if promo_files else None
    
    print(f"Latest file determined: {latest_price_full}, {latest_price}, {latest_promo_full}, {latest_promo}")
    selected = []
    if latest_promo_full:
        selected.append(latest_promo_full)
    if latest_price_full:
        selected.append(latest_price_full)
    if latest_promo and not latest_promo_full:
        selected.append(latest_promo)
    if latest_price and not latest_price_full:
        selected.append(latest_price)
    return selected

def get_safe_filename(url: str) -> str:
    return unquote(os.path.basename(urlsplit(url).path))



def index_of_branch(parts: List[str]) -> int:
    
    year = str(datetime.now().year)
    for i, p in enumerate(parts):
        if year in p:
            return i - 1
    return -1
def download_and_save_file(url: str, provider_folder: str) -> None:
    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        filename = get_safe_filename(url)
        
        parts = filename.split("-")
        branch_code = parts[index_of_branch(parts)] if index_of_branch(parts) != -1 else "unknown"

        dt = get_timestamp(filename)
        timestamp = int(dt.timestamp()) if dt != datetime.min else int(datetime.now().timestamp())
        
        file_type = None
        match filename.lower():
            case f if "price" in f and "full" in f:
                file_type = "pricesFull"
            case f if "promo" in f and "full" in f:
                file_type = "promoFull"
            case f if "price" in f and "full" not in f:
                file_type = "prices"
            case f if "promo" in f and "full" not in f:
                file_type = "promo"

        # safe local path
        new_filename = f"{provider_folder}_{branch_code}_{file_type}_{timestamp}.gz"
        project_root = Path(__file__).resolve().parents[1]
        folder_path = project_root / "downloads"
        folder_path.mkdir(parents=True, exist_ok=True)
        local_path = folder_path / new_filename

        # requests session with retries
        session = requests.Session()
        session.verify = certifi.where()   
        retries = Retry(total=5, backoff_factor=1, status_forcelist=(429,500,502,503,504), allowed_methods=frozenset(["GET","HEAD"]))
        session.mount("https://", HTTPAdapter(max_retries=retries))

        headers = {"User-Agent": "price-crawler/1.0"}
        with session.get(url, stream=True, timeout=30, headers=headers) as response:
            response.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"Saved file to {local_path}")
        # upload if configured
        if S3_BUCKET:
            s3.upload_file(str(local_path), S3_BUCKET, new_filename)
            print(f"Uploaded to S3: s3://{S3_BUCKET}/{new_filename}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def process_downloads(urls: List[str], provider_folder: str) -> None:
    if not urls:
        print("No files to download.")
        return
    for url in tqdm(urls, desc="Downloading"):
        print(f"Downloading {url}...")
        download_and_save_file(url, provider_folder)
