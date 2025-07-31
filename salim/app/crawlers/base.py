from abc import ABC, abstractmethod
import boto3
import os
import sys
import os
import time
import platform
from urllib.parse import urljoin
# from bs4 import BeautifulSoup
from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import Select
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
# import requests
# import json
# import re
# import html
from botocore.exceptions import ClientError

providers_base_url = "https://www.gov.il/he/pages/cpfta_prices_regulations"

class CrawlerBase(ABC):
    def __init__(self, providers_base_url):
        self.providers_base_url = providers_base_url

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
    
    @abstractmethod
    def crawl():
        pass
    
    def fetch(self, provider_path =""):
        provider_url = self.providers_base_url + provider_path
        options = self.init_chrome_options()
        driver_path = self.get_chromedriver_path()
        driver = webdriver.Chrome(executable_path=driver_path, options=options)
        driver.get(provider_url)
        time.sleep(10)
        page = driver.page_source
        driver.quit()
        return page      
        
    def save_file(self, provider_files):
        pass   
    
    def upload_file_to_s3(self, provider_path):        
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        
        bucket_name = 'test-bucket'
        file_path = './ShakedZrihen.txt'
        s3_key = provider_path
        
        try:
            if not os.path.exists(file_path):
                print(f"Error: File '{file_path}' not found!")
                sys.exit(1)
            
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
 
    def run(self, provider_path=""):
        provider_files = self.fetch(provider_path)
        self.save_file(provider_files)
        self.upload_file_to_s3(provider_path)
        return