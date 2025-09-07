import os
import re
import time
import json
import urllib3
import requests
import platform
from bs4 import BeautifulSoup
from selenium import webdriver
from urllib.parse import urljoin
from datetime import datetime, timezone, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def make_driver():
    chrome_options = init_chrome_options()
    try:
        driver_bin = ChromeDriverManager().install()
        service = Service(executable_path=driver_bin)
        return webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        print(f"WebDriverManager failed ({e}), trying Selenium Manager...")
        return webdriver.Chrome(options=chrome_options)

def init_chrome_options():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--lang=en-US")

    return chrome_options


def get_chromedriver_path():
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


def parse_elements(elements):

    latest_reports = {}

    for element in elements:

        link = element.find('a', href=True)
        if not link:
            continue

        href = link["href"]

        match = re.search(r'-(\d{3})-', href)
        if not match:
            continue
        branch = match.group(1)

        time = element.find("time")["datetime"]

        report = {
            "branch": branch,
            "href": href,
            "time": time
        }

        if branch not in latest_reports or time > latest_reports[branch]["time"]:
            latest_reports[branch] = report

    return latest_reports


def clean_branch_folder(provider, branch):
    folder = os.path.join("crawler_data", provider, branch)
    if os.path.exists(folder):
        for f in os.listdir(folder):
            os.remove(os.path.join(folder, f))

def is_gzip_response(resp: requests.Response, first_bytes: bytes) -> bool:
    if b"\x1f\x8b" == first_bytes[:2]:
        return True
    ctype = (resp.headers.get("Content-Type") or "").lower()
    return "application/gzip" in ctype or "application/x-gzip" in ctype or "octet-stream" in ctype


def download_and_save_reports(reports, provider, type, clean=False, url = "https://url.publishedprices.co.il", session: requests.Session=None):

    if session is None:
        session = requests.Session()

    for branch, report in reports.items():

        if clean:
            clean_branch_folder(provider, branch)

        file_url = url + report['href']
        timestamp = report['time']

        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        formatted_time = timestamp.strftime("%Y-%m-%d_%H-%M")
        file_name = f"{type}Full_{formatted_time}.gz"
        save_dir = os.path.join(f"crawler_data/{provider}", branch)
        save_path = os.path.join(save_dir, file_name)
        os.makedirs(save_dir, exist_ok=True)

        print(f"Downloading files for {provider}, branch number {branch}")
        try:
            with session.get(file_url, stream=True, allow_redirects=True, timeout=60, verify=False) as resp:
                resp.raise_for_status()
                it = resp.iter_content(chunk_size=8192)
                first_chunk = next(it, b"")
                if not is_gzip_response(resp, first_chunk):
                    sample = first_chunk.decode("utf-8", errors="ignore")[:400]
                    raise RuntimeError(f"Not a GZIP (got {resp.headers.get('Content-Type')}), sample: {sample!r}")

                with open(save_path, "wb") as f:
                    if first_chunk:
                        f.write(first_chunk)
                    for chunk in it:
                        if chunk:
                            f.write(chunk)
        except Exception as e:
            print(f"Downloading error: {e}")

def requests_session_from_driver(driver) -> requests.Session:
    session = requests.Session()

    try:
        user_agent = driver.execute_script("return navigator.userAgent;")
        session.headers.update({"User-Agent": user_agent})
    except Exception:
        pass

    for c in driver.get_cookies():
        session.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path"))
    return session

def crawl():

    driver = make_driver()

    wait = WebDriverWait(driver, 20)

    login_url = "https://url.publishedprices.co.il/login"
    files_url = "https://url.publishedprices.co.il/file"
    providers = ["Yohananof", "TivTaam", "OsherAd"]
    usernames = ["yohananof", "TivTaam", "osherad"]

    os.makedirs("crawler_data", exist_ok=True)

    for i in range(3):

        print(f"Getting data for {providers[i]} stores")

        driver.get(login_url)
        time.sleep(1)

        print(f"Entering database for {providers[i]}")

        wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(usernames[i])
        wait.until(EC.element_to_be_clickable((By.ID, "login-button"))).click()
        time.sleep(1)

        wait.until(EC.any_of(EC.url_contains("/file"),EC.presence_of_element_located((By.TAG_NAME, "body"))))
        if "/file" not in driver.current_url:
            driver.get(files_url)

        session = requests_session_from_driver(driver)

        search_box = driver.find_element(By.CSS_SELECTOR,".form-control.input-sm")
        search_box.send_keys("PriceFull")
        time.sleep(1)


        soup = BeautifulSoup(driver.page_source, "html.parser")
        prices = soup.find_all('tr')

        search_box.clear()
        search_box.send_keys("PromoFull")
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        promos = soup.find_all('tr')

        latest_prices = parse_elements(prices)
        latest_promos = parse_elements(promos)

        download_and_save_reports(latest_prices, providers[i], "Price", True, session=session)
        download_and_save_reports(latest_promos, providers[i], "Promo", False, session=session)

        print(f"Scrawling for {providers[i]} finished successfully!")
        time.sleep(3)

    print(f"Scrawling for all providers finished successfully!")


if __name__ == "__main__":
    crawl()