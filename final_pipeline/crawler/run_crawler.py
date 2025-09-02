import os
import re
import time
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import boto3
import os
from dotenv import load_dotenv
from pathlib import Path

# S3: step1
# def read_aws_credentials(filepath="aws/credentials.txt"):
#     creds = {}
#     with open(filepath) as f:
#         for line in f:
#             if "=" in line:
#                 key, val = line.strip().split("=", 1)
#                 creds[key.strip()] = val.strip()
#     return creds

# --- S3 uploader (ENV-based, no files) ---
def upload_all_providers_to_s3():
    import os, boto3, mimetypes
    from pathlib import Path
    from botocore.config import Config

    # compose/.env
    bucket = os.getenv("S3_BUCKET", "raw-prices")
    prefix = os.getenv("S3_PREFIX", "prices/").rstrip("/") + "/"

    # MinIO/AWS
    endpoint = os.getenv("S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL_S3")
    access_key = os.getenv("S3_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    # Check if we're using MinIO (has endpoint) and use MinIO client for better compatibility
    if endpoint and "minio" in endpoint.lower():
        print("[upload] Using MinIO client for direct file uploads...")
        return upload_with_minio_client(bucket, prefix, endpoint, access_key, secret_key)
    else:
        print("[upload] Using S3 client for AWS S3...")
        return upload_with_s3_client(bucket, prefix, endpoint, access_key, secret_key, region)

def upload_with_minio_client(bucket, prefix, endpoint, access_key, secret_key):
    """Upload files using MinIO client for better compatibility"""
    try:
        from minio import Minio
        from minio.error import S3Error
        
        # Parse endpoint to get host and port
        if endpoint.startswith('http://'):
            endpoint = endpoint[7:]
        elif endpoint.startswith('https://'):
            endpoint = endpoint[8:]
        
        # Create MinIO client
        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # Set to True if using HTTPS
        )
        
        providers_root = Path("/app/providers")
        if not providers_root.exists():
            print(f"[upload] providers dir not found: {providers_root}")
            return

        uploaded = 0
        for p in providers_root.rglob("*"):
            if p.is_file():
                rel = p.relative_to(providers_root).as_posix()  # e.g. "Goodpharm/Price....gz"
                key = f"{prefix}{rel}"                          # e.g. "prices/Goodpharm/Price....gz"
                
                try:
                    # Upload file directly using MinIO client
                    minio_client.fput_object(
                        bucket_name=bucket,
                        object_name=key,
                        file_path=str(p),
                        content_type="application/octet-stream"
                    )
                    
                    print(f"[upload] minio://{bucket}/{key} ({p.stat().st_size} bytes)")
                    uploaded += 1
                    
                except S3Error as e:
                    print(f"[upload] MinIO error uploading {p}: {e}")
                except Exception as e:
                    print(f"[upload] Error uploading {p}: {e}")

        print(f"[upload] done. {uploaded} files uploaded via MinIO client.")
        
    except ImportError:
        print("[upload] MinIO client not available, falling back to S3 client...")
        return upload_with_s3_client(bucket, prefix, endpoint, access_key, secret_key, "us-east-1")
    except Exception as e:
        print(f"[upload] MinIO client error: {e}, falling back to S3 client...")
        return upload_with_s3_client(bucket, prefix, endpoint, access_key, secret_key, "us-east-1")

def upload_with_s3_client(bucket, prefix, endpoint, access_key, secret_key, region):
    """Upload files using S3 client (AWS S3 or MinIO S3 API)"""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint if endpoint else None,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )

    providers_root = Path("/app/providers")
    if not providers_root.exists():
        print(f"[upload] providers dir not found: {providers_root}")
        return

    uploaded = 0
    for p in providers_root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(providers_root).as_posix()  # e.g. "Goodpharm/Price....gz"
            key = f"{prefix}{rel}"                          # e.g. "prices/Goodpharm/Price....gz"
            
            try:
                # Read file content and upload using put_object to avoid MinIO directory issues
                with open(p, 'rb') as f:
                    file_content = f.read()
                
                ctype, _ = mimetypes.guess_type(p.name)
                extra = {"ContentType": ctype or "application/octet-stream"}
                
                # Use put_object instead of upload_file
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=file_content,
                    **extra
                )
                
                print(f"[upload] s3://{bucket}/{key} ({len(file_content)} bytes)")
                uploaded += 1
                
            except Exception as e:
                print(f"[upload] Error uploading {p}: {e}")

    print(f"[upload] done. {uploaded} files uploaded via S3 client.")


def upload_file_to_s3(local_path, bucket_name, s3_key, aws_access_key, aws_secret_key):
    s3 = boto3.client('s3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    try:
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"Uploaded: {local_path} -> s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {local_path}: {e}")

def wait_for_downloads(directory, timeout=15):
    for _ in range(timeout):
        if not any(f.endswith(".crdownload") for f in os.listdir(directory)):
            return
        time.sleep(1)

# def setup_driver(download_dir):
#     chrome_options = Options()
#     chrome_options.add_argument("--headless=new")
#     chrome_options.add_experimental_option("prefs", {
#         "download.default_directory": download_dir,
#         "download.prompt_for_download": False,
#         "download.directory_upgrade": True,
#         "safebrowsing.enabled": True
#     })
#     return webdriver.Chrome(service=Service(), options=chrome_options)

def setup_driver(download_dir: str):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    import shutil, os

    opts = Options()
    # headless + flags 
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("prefs", {
        "download.default_directory": download_dir
    })
    binary = shutil.which("chromium") or shutil.which("chromium-browser")
    if not binary:
        raise RuntimeError("chromium binary not found in container")
    opts.binary_location = binary

    # chromedriver
    driver_path = shutil.which("chromedriver") or "/usr/lib/chromium/chromedriver"
    if not os.path.exists(driver_path):
        raise RuntimeError(f"chromedriver not found (tried: {driver_path})")
    svc = Service(executable_path=driver_path)

    return webdriver.Chrome(service=svc, options=opts)

def crawl_button_site(name, url):
    print(f"\nCrawling {name}...")
    download_dir = os.path.abspath(os.path.join("providers", name))
    os.makedirs(download_dir, exist_ok=True)
    driver = setup_driver(download_dir)

    try:
        driver.get(url)
        time.sleep(4)
        buttons = driver.find_elements(By.XPATH, "//button[contains(@onclick, 'Download')]")
        print(f"Found {len(buttons)} buttons")
        price = promo = False

        for btn in buttons:
            onclick = btn.get_attribute("onclick")
            if not price and "Price" in onclick:
                print(f"Clicking PRICE: {onclick}")
                driver.execute_script(onclick)
                price = True
                time.sleep(4)
                wait_for_downloads(download_dir)
            elif not promo and "Promo" in onclick:
                print(f"Clicking PROMO: {onclick}")
                driver.execute_script(onclick)
                promo = True
                time.sleep(4)
                wait_for_downloads(download_dir)
            if price and promo:
                break
    finally:
        driver.quit()

    print(f"Done. Files saved to: {download_dir}\n")


def crawl_carrefour():
    print("\nCrawling Carrefour...")
    supermarket = "Carrefour"
    base_url = "https://prices.carrefour.co.il/"
    download_dir = os.path.abspath(os.path.join("providers", supermarket))
    os.makedirs(download_dir, exist_ok=True)

    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")
    price_url = promo_url = None

    for a in soup.select("a.downloadBtn"):
        href = a.get("href", "")
        if "Price" in href and not price_url:
            price_url = urljoin(base_url, href)
        elif "Promo" in href and not promo_url:
            promo_url = urljoin(base_url, href)
        if price_url and promo_url:
            break

    for label, file_url in [("PRICE", price_url), ("PROMO", promo_url)]:
        if file_url:
            filename = os.path.basename(file_url)
            path = os.path.join(download_dir, filename)
            print(f"Downloading {label}: {filename}")
            r = requests.get(file_url)
            if r.status_code == 200:
                with open(path, "wb") as f:
                    f.write(r.content)
                print(f"Saved: {filename}")
            else:
                print(f"Failed: {file_url}")
    print(f"Done. Files saved to: {download_dir}\n")


def get_latest_files(files_info):
    def extract_date(filename):
        match = re.search(r'(Price|Promo)[^\d]*(\d+)-(\d+)-(\d+)', filename)
        return match.group(4) if match else ''

    latest = {'Price': ('', None), 'Promo': ('', None)}
    for info in files_info:
        date = extract_date(info['filename'])
        if not date:
            continue
        if 'Price' in info['filename'] and date > latest['Price'][0]:
            latest['Price'] = (date, info)
        elif 'Promo' in info['filename'] and date > latest['Promo'][0]:
            latest['Promo'] = (date, info)
    return [x[1] for x in latest.values() if x[1]]


def crawl_published_site(name, username):
    print(f"\nCrawling {name}...")
    download_dir = os.path.abspath(os.path.join("providers", name))
    os.makedirs(download_dir, exist_ok=True)
    driver = setup_driver(download_dir)

    try:
        driver.get("https://url.publishedprices.co.il/login")
        time.sleep(2)
        driver.find_element(By.ID, "username").send_keys(username)
        driver.find_element(By.ID, "login-button").click()
        time.sleep(2)
        driver.get("https://url.publishedprices.co.il/files")

        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tbody.context a.f")))
        links = driver.find_elements(By.CSS_SELECTOR, "tbody.context a.f")
        files_info = [{'filename': os.path.basename(link.get_attribute("href")), 'button': link} for link in links]

        latest_files = get_latest_files(files_info)
        for file_info in latest_files:
            print(f"Downloading {file_info['filename']}")
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", file_info['button'])
                file_info['button'].click()
                time.sleep(4)
                wait_for_downloads(download_dir)
            except Exception as e:
                print(f"Failed to download {file_info['filename']}: {e}")
    finally:
        driver.quit()

    print(f"Done. Files saved to: {download_dir}\n")


# def upload_all_providers_to_s3(providers_dir="providers", credentials_path="aws/credentials.txt"):
#     creds = read_aws_credentials(credentials_path)
#     bucket = creds["BUCKET_NAME"]
#     key = creds["AWS_ACCESS_KEY_ID"]
#     secret = creds["AWS_SECRET_ACCESS_KEY"]

#     for supermarket in os.listdir(providers_dir):
#         full_path = os.path.join(providers_dir, supermarket)
#         if os.path.isdir(full_path):
#             for file in os.listdir(full_path):
#                 local_file_path = os.path.join(full_path, file)
#                 s3_key = f"{supermarket}/{file}"
#                 upload_file_to_s3(local_file_path, bucket, s3_key, key, secret)



def main():
    crawl_button_site("Goodpharm", "https://goodpharm.binaprojects.com/Main.aspx")
    crawl_button_site("zolbegadol", "https://zolvebegadol.binaprojects.com/Main.aspx")
    crawl_carrefour()
    crawl_published_site("Yohananof", "yohananof")
    crawl_published_site("OsherAd", "osherad")
    crawl_published_site("TivTaam", "TivTaam")
    print("\n Uploading to S3...")
    upload_all_providers_to_s3()

if __name__ == "__main__":
    main()