import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Supermarket configurations
SUPERMARKETS = {
    "politzer": {"username": "politzer", "branches": ["herzilya", "tel_aviv"]},
    "Keshet": {"username": "Keshet", "branches": ["netanya", "petah_tikva"]}, 
    "yohananof": {"username": "yohananof", "branches": ["akko", "haifa"]}
}

# URLs
LOGIN_URL = "https://url.publishedprices.co.il/login"

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET = "supermarket-crawler"

# Timeouts and limits
PROCESSING_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 15
LOGIN_WAIT = 10
MAX_FILES_PER_TYPE = 2

# Chrome driver options
CHROME_OPTIONS = [
    "--headless",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--window-size=1920,1080",
    "--disable-gpu",
    "--ignore-certificate-errors",
    "--ignore-ssl-errors",
    "--allow-running-insecure-content",
    "--disable-web-security",
    "--disable-logging",
    "--log-level=3",
    "--silent"
]

# Download preferences
DOWNLOAD_PREFS = {
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}