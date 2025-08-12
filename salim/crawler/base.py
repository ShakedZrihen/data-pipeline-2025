import os
import platform
import re
import shutil
import sys
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from webdriver_manager.chrome import ChromeDriverManager


# Initalizes default chrome options
def init_chrome_options():
    chrome_options = Options()
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.prompt_for_download": False,
            "download.default_directory": "/tmp/salim",  # Set this to your preferred folder
            "directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    # Set up headless Chrome
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return chrome_options


def get_chromedriver_path():
    """Get the correct chromedriver path for the current system"""
    try:
        # For macOS ARM64, we need to specify the architecture
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            print("Detected macOS ARM64, using specific chromedriver...")
            # Use a more specific approach for ARM64 Macs
            from webdriver_manager.core.os_manager import ChromeType

            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        else:
            driver_path = ChromeDriverManager().install()

        print(f"Chrome driver path: {driver_path}")
        return driver_path
    except Exception as e:
        print(f"Error with webdriver-manager: {e}")
        print("Falling back to system chromedriver...")
        # Fallback to system chromedriver if available
        return "chromedriver"


class Crawler:
    def __init__(self):
        self.options = init_chrome_options()
        self.download_dir = "/tmp/salim"
        self.latest_branches = dict()

        self.s3_client = boto3.client(
            "s3",
            # we use localhost because we are using awslocal
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        try:
            # Initialize drivers and headless browser
            chromedriver_path = get_chromedriver_path()
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=self.options)
            self.driver = driver
        except Exception as e:
            print(f"Failed to initialize Chrome driver: {e}")
            print("Trying alternative approach...")
            # Alternative approach without service
            driver = webdriver.Chrome(options=self.options)
            self.driver = driver

    def crawl(self):
        raise NotImplementedError

    def upload_s3(self, s3_key, filepath, bucket_name="test-bucket"):
        try:
            if not os.path.exists(filepath):
                print(f"Error: File '{filepath}' not found!")
                sys.exit(1)

            self.s3_client.upload_file(filepath, bucket_name, s3_key)
            print(f"{filepath} uploaded to s3://{bucket_name}/{s3_key}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "NoSuchBucket":
                print(f"Error: Bucket '{bucket_name}' does not exist!")
                print(
                    "Make sure LocalStack services are running with: docker-compose up"
                )
            else:
                print(f"Error uploading file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)
        pass

    def move_file(self, old: str, new: str):
        os.makedirs(os.path.dirname(new), exist_ok=True)
        files = [f for f in os.listdir(old) if not f.endswith(".crdownload")]
        if not files:
            print(f"No completed downloads found in {old}, continuing.")
            return False
        files.sort(key=lambda f: os.path.getmtime(os.path.join(old, f)), reverse=True)
        src_file = os.path.join(old, files[0])
        shutil.move(src_file, new)
        print(f"moved {src_file} -> {new}")
        return True

    def get_all_branches(self, table_row: list[WebElement]):
        print("Getting branch data...")
        for row in table_row:
            branch, date = self.get_branch(row)
            self.upsert_branch(branch, date)
        return self.latest_branches

    def upsert_branch(self, branch: str, stamp: datetime) -> None:
        current = self.latest_branches.get(branch, None)
        if current is None or stamp > current:
            self.latest_branches[branch] = stamp

    def format_filename_to_folder(self, fname: str, price_type: str) -> str:
        # ex. PriceFull7290803800003-001-202508031000.gz
        prefix = re.match(r"^[^\d]+", fname)
        if not prefix:
            prefix = ""
        else:
            prefix = prefix.group()

        full_date = fname.split("-")[-1].split(".")[0]
        date = full_date[:8]
        date_time = full_date[8:]

        suffix = fname.split("-")[-1].split(".")[1]
        price_filename = "_".join([prefix, date, date_time])
        price_filename = price_filename + "." + suffix
        branch = fname.split("-")[-2]
        return os.path.join(self.store, price_type, branch, price_filename)

    @staticmethod
    def parse_time(time_str: str) -> datetime:
        try:
            return datetime.strptime(time_str, "%Y%m%d%H%M")
        except Exception as e:
            print(f"Got parse_time exception: {e}. continuing...")
            return datetime.min

    def get_branch(self, row: WebElement) -> tuple[str, datetime]:
        td_list = row.find_elements(by=By.TAG_NAME, value="td")
        name = td_list[0].text
        branch = name.split("-")[1]
        date = name.split("-")[2].split(".")[0]
        date = self.parse_time(date)
        return branch, date

    def wait_for_any_download_complete(
        self, download_dir: str, timeout: int = 10, poll_interval: float = 0.5
    ):
        """
        Wait until all .crdownload files in the directory are gone,
        indicating that all Chrome downloads have completed.

        Args:
            download_dir (str): Path to the download folder
            timeout (int): Max time to wait in seconds
            poll_interval (float): Time between checks in seconds

        Raises:
            TimeoutError: If no download completes within the timeout
        """

        start_time = time.time()
        time.sleep(0.1)
        while True:
            downloading = any(
                f.endswith(".crdownload") for f in os.listdir(download_dir)
            )
            if not downloading:
                return  # All downloads complete

            if time.time() - start_time > timeout:
                # remove all contents of the download dir
                for name in os.listdir(download_dir):
                    path = os.path.join(download_dir, name)
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        try:
                            os.remove(path)
                        except FileNotFoundError:
                            pass
                raise TimeoutError("Download did not complete within timeout.")

            time.sleep(poll_interval)
