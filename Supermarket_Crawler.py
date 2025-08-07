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

# S3: step1:
def read_aws_credentials(filepath="aws/credentials.txt"):
    creds = {}
    with open(filepath) as f:
        for line in f:
            if "=" in line:
                key, val = line.strip().split("=", 1)
                creds[key.strip()] = val.strip()
    return creds

def upload_file_to_s3(local_path, bucket_name, s3_key, aws_access_key, aws_secret_key):
    s3 = boto3.client('s3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    try:
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"Uploaded: {local_path} ➝ s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {local_path}: {e}")

def wait_for_downloads(directory, timeout=15):
    for _ in range(timeout):
        if not any(f.endswith(".crdownload") for f in os.listdir(directory)):
            return
        time.sleep(1)


def setup_driver(download_dir):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    return webdriver.Chrome(service=Service(), options=chrome_options)


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


def upload_all_providers_to_s3(providers_dir="providers", credentials_path="aws/credentials.txt"):
    creds = read_aws_credentials(credentials_path)
    bucket = creds["BUCKET_NAME"]
    key = creds["AWS_ACCESS_KEY_ID"]
    secret = creds["AWS_SECRET_ACCESS_KEY"]

    for supermarket in os.listdir(providers_dir):
        full_path = os.path.join(providers_dir, supermarket)
        if os.path.isdir(full_path):
            for file in os.listdir(full_path):
                local_file_path = os.path.join(full_path, file)
                s3_key = f"{supermarket}/{file}"
                upload_file_to_s3(local_file_path, bucket, s3_key, key, secret)



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
