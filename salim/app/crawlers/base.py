from abc import ABC, abstractmethod
import boto3
import os
import sys
import os
import time
import platform
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from botocore.exceptions import ClientError

do_not_use = "https://www.gov.il/he/pages/cpfta_prices_regulations"

class CrawlerBase(ABC):
    def __init__(self, provider_url):
        self.providers_base_url = provider_url


    def init_chrome_options(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        return chrome_options


    def get_chromedriver_path(self):
        try:
            if platform.system() == "Darwin" and platform.machine() == "arm64":
                from webdriver_manager.core.os_manager import ChromeType
                driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            else:
                driver_path = ChromeDriverManager().install()
            return driver_path
        except Exception as e:
            return "chromedriver"
    

    def get_page_source(self, provider_url):
        chrome_options = self.init_chrome_options()
        chromedriver_path = self.get_chromedriver_path()
        web_driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)

        web_driver.get(provider_url)
        time.sleep(10)

        page_html = web_driver.page_source
        web_driver.quit()
        return page_html
    

    def upload_file_to_s3(self, file_path, branch_name, file_type):        
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        
        bucket_name = 'test-bucket'
        
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' not found!")
            sys.exit(1)
        
        # Build timestamp for unique file naming
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")   
        
        # Build S3 key according to structure
        s3_key = f"providers/{branch_name}/{file_type}Full_{timestamp}.gz"
        
        try:
            s3_client.upload_file(file_path, bucket_name, s3_key)
            print(f"{file_path} uploaded to s3://{bucket_name}/{s3_key}")        
            print("\nFiles in bucket:")
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    filename = obj['Key']
                    size = obj['Size']
                    modified = obj['LastModified']
                    print(f"  - {filename} (Size: {size} bytes, Modified: {modified})")
            else:
                print("  No files found in bucket")
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"Error: Bucket '{bucket_name}' does not exist!")
                print("Make sure LocalStack services are running with: docker-compose up")
            else:
                print(f"Error uploading file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)
    

    def get_files_info(self, provider_dir):
        file_info_path = os.path.join(provider_dir, "file_info.json")

        if not os.path.exists(file_info_path):
            raise FileNotFoundError(f"file_info.json not found in {provider_dir}")

        with open(file_info_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        files_info = []
        for entry in data:
            files_info.append({
                "file_path": entry.get("file_path"),
                "branch": entry.get("branch"),
                "file_type": entry.get("file_type")
            })

        return files_info


    def run(self, provider_url):
        """
        1. Get page HTML.
        2. Download all files and get their local paths.
        3. Upload each file to S3 under the correct branch folder.
        """
        page_html = self.get_page_source(provider_url)
        provider_dir = self.download_files_from_html(page_html)
        files_info = self.get_files_info(provider_dir)
        for file in files_info:
            self.upload_file_to_s3(
                file_path=file["file_path"],
                branch_name=file["branch"],
                file_type=file["file_type"]
        )
        return
    

    @abstractmethod
    def download_files_from_html(self, page_html):
        """
        Abstract method that each subclass must implement.

        Responsibilities:
        - Parse the given HTML content (page_html)
        - Locate all links to downloadable files (PDF, Excel, etc.)
        - Download the files to a local directory
        - Return a json of files info
        """
        pass

