import os, json, boto3
import time
import platform
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import shutil

import requests, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit, unquote
# === S3 simulator target ===
S3_SIMULATOR_ROOT = r"C:\Users\Daniella Elbaz\Desktop\שנה ג סמסטר קיץ\סדנת פייתון\data-pipeline-2025\examples\s3-simulator\providers"
# ===== gov.il crawler (PDF/XLS/XLSX) =====
GOVIL_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"
FILE_EXTS = (".pdf", ".xls", ".xlsx")

def _safe_name(u: str) -> str:
    name = os.path.basename(unquote(urlsplit(u).path))
    name = re.sub(r'[<>:"/\\|?*]+', "_", name).strip()
    return name or "downloaded_file"

def _download_stream(u: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, _safe_name(u))
    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.get(u, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(8192):
                if chunk: f.write(chunk)
    print(f"[gov.il] Saved: {out_path}")

def crawl_govil():
    try:
        html = requests.get(GOVIL_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).text
    except Exception as e:
        print(f"[gov.il] HTTP error: {e}")
        html = ""

    def collect_from_html(html_text: str, base: str):
        soup = BeautifulSoup(html_text, "lxml")
        urls = set()
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href: 
                continue
            full = urljoin(base, href)
            if full.lower().endswith(FILE_EXTS):
                urls.add(full)
        for m in re.finditer(r'(https?://[^\s"\'<>]+?\.(?:pdf|xls|xlsx))', html_text, re.IGNORECASE):
            urls.add(m.group(1))
        return sorted(urls)

    links = collect_from_html(html, GOVIL_URL) if html else []
    if not links:
        try:
            chromedriver_path = get_chromedriver_path()
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=Options())
            driver.get(GOVIL_URL)
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1.0)

            texts = ["הצג עוד","טען עוד","פתח","הרחב","רשימות מחירים","מסמכים",
                     "load more","show more","expand","documents","files","all"]
            for t in texts:
                for el in driver.find_elements(By.XPATH, f"//*[contains(normalize-space(.), '{t}')]"):
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        el.click(); time.sleep(0.3)
                    except Exception:
                        pass

            last_h = 0
            for _ in range(6):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);"); time.sleep(0.7)
                h = driver.execute_script("return document.body.scrollHeight;")
                if h == last_h: break
                last_h = h

            html2 = driver.page_source
            links = collect_from_html(html2, driver.current_url)
        finally:
            try: driver.quit()
            except: pass

    if not links:
        print("[gov.il] No downloadable files found.")
        return

    out_dir = os.path.join(os.path.dirname(__file__), "downloads", "gov_il", datetime.now().strftime("%Y-%m-%d"))
    for u in links:
        try:
            _download_stream(u, out_dir)
        except Exception as e:
            print(f"[gov.il] Download failed for {u}: {e}")

def init_chrome_options(supermarket: str) -> Options:
    chrome_options = Options()

    base_dir = r'C:\Users\Daniella Elbaz\Desktop\שנה ג סמסטר קיץ\סדנת פייתון\data-pipeline-2025\salim\app\finalCrawler'
    download_dir = os.path.join(base_dir, 'providers', supermarket, 'temp')
    os.makedirs(download_dir, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")

    return chrome_options

def get_chromedriver_path() -> str:
    try:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            print("Detected macOS ARM64, using specific chromedriver...")
            from webdriver_manager.core.os_manager import ChromeType
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()

        print(f"Chrome driver path: {driver_path}")
        return driver_path
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        return "chromedriver"

def crawler(username: str):
    base_dir = r'C:\Users\Daniella Elbaz\Desktop\שנה ג סמסטר קיץ\סדנת פייתון\data-pipeline-2025\salim\app\finalCrawler'
    chrome_options = init_chrome_options(username)
    download_dir = os.path.join(base_dir, 'providers', username, 'temp')

    url = "https://url.publishedprices.co.il/login"

    try:
        chromedriver_path = get_chromedriver_path()
        service = Service(chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        driver = webdriver.Chrome(options=chrome_options)

    try:
        print(f"Opening {url}")
        driver.get(url)
        time.sleep(2)

        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys("")
        driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)
        time.sleep(3)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.context.allow-dropdown-overflow tr"))
        )

        rows = driver.find_elements(By.CSS_SELECTOR, "tbody.context.allow-dropdown-overflow tr")

        file_map = []
        for row in rows:
            try:
                link_el = row.find_element(By.CSS_SELECTOR, "a")
                text = link_el.text
                href = link_el.get_attribute("href")
                if "PriceFull" in text and ".gz" in text:
                    timestamp_str = text.split("-")[-1].replace(".gz", "")[:10]
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H")
                    file_map.append((href, timestamp))
            except Exception:
                continue

        if not file_map:
            print("No files found.")
            driver.quit()
            return

        latest_hour = max(file_map, key=lambda x: x[1])[1]
        latest_files = [link for link, ts in file_map if ts == latest_hour]

        print(f"\nFound {len(latest_files)} files from {latest_hour.strftime('%Y-%m-%d %H:00')}:")
        for link in latest_files:
            print(link)
            filename = os.path.basename(link)
            try:
                el = driver.find_element(By.LINK_TEXT, filename)
                el.click()
                print(f"Downloading: {filename}")
                time.sleep(2)

                full_path = os.path.join(download_dir, filename)
                branch_id, ts = parse_pricefull_filename(filename)
                if branch_id and ts:
                    s3_key = upload_to_s3(final_path, provider=username, branch=branch_id, ts=ts)
                    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")
                    send_pointer_message(provider=username, branch=branch_id, ts=ts, s3_key=s3_key)
                    print("SQS pointer sent.")
                else:
                    print(f"Skip {filename}: cannot parse branch/timestamp")
                while not os.path.exists(full_path):
                    time.sleep(1)
                while any(f.endswith(".crdownload") for f in os.listdir(download_dir)):
                    time.sleep(1)

                parts = filename.split("-")
                branch_id = parts[1] if len(parts) >= 3 else "unknown"

                final_dir = os.path.join(base_dir, 'providers', username, branch_id)
                os.makedirs(final_dir, exist_ok=True)

                final_path = os.path.join(final_dir, filename)
                try:
                    os.replace(full_path, final_path)
                except Exception:
                    shutil.move(full_path, final_path)
                print(f"Saved to: {final_path}")
                # --- S3-simulator copy (exact same filename) ---
                try:
                    s3_dir = os.path.join(S3_SIMULATOR_ROOT, username, branch_id)
                    os.makedirs(s3_dir, exist_ok=True)
                    s3_target = os.path.join(s3_dir, filename)
                    shutil.copyfile(final_path, s3_target)
                    print(f"S3-simulator copy: {s3_target}")
                except Exception as e:
                    print(f"Could not copy to S3 simulator: {e}")
                # --- end S3-simulator copy ---
            except Exception as e:
                print(f"{filename}: {e}")

        temp_dir = os.path.join(base_dir, 'providers', username, 'temp')
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                print(f"Could not delete temp folder: {e}")

    finally:
        driver.quit()
        print("Chrome driver closed.")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
S3_BUCKET = os.getenv("S3_BUCKET", "prices-raw")
OUTBOX_SQS_URL = os.getenv("OUTBOX_SQS_URL")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
)
sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION,
    endpoint_url=AWS_ENDPOINT_URL,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
)

def parse_pricefull_filename(fname: str):
    m = re.match(r"^PriceFull-(?P<branch>\d+)-(?P<ts>\d{10})\.gz$", fname)
    if not m:
        return None, None
    branch = m.group("branch")
    ts_str = m.group("ts")
    ts = datetime.strptime(ts_str, "%Y%m%d%H")
    return branch, ts

def upload_to_s3(local_path: str, provider: str, branch: str, ts: datetime) -> str:
    key = f"raw/{provider}/{branch}/PriceFull-{ts.strftime('%Y%m%d%H')}.gz"
    s3.upload_file(local_path, S3_BUCKET, key)
    return key

def send_pointer_message(provider: str, branch: str, ts: datetime, s3_key: str):
    envelope = {
        "provider": provider,
        "branch": branch,
        "type": "pricesFull",
        "timestamp": ts.isoformat() + "Z",
        "items_total": 0,
        "items_sample": [],
        "s3_bucket": S3_BUCKET,
        "s3_key": s3_key,
    }
    sqs.send_message(
        QueueUrl=OUTBOX_SQS_URL,
        MessageBody=json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
    )

if __name__ == "__main__":
    crawl_govil()
    crawler("yohananof")
    time.sleep(1)
    crawler("Keshet")
    time.sleep(1)
    crawler("politzer")
