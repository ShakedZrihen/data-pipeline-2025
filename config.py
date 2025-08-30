# config.py
import os
import re
import boto3
import logging

# ---------- CONFIG ----------
BASE_LISTING_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"
DOWNLOAD_DIR = os.path.join(os.getcwd(), "providers")
HEADLESS = True
VERIFY_SSL = True

TARGET_STORES = [
    ("מ. יוחננוף", "yohananof"),
    ("אלמשהדאוי קינג סטור", "kingstore"),
    ("ג.מ מעיין אלפיים", "maayan"),
]

YOHANANOF_USERNAME = "yohananof"
YOHANANOF_PASSWORD = ""

PAGE_WAIT = 0.6

# AWS S3 config
BUCKET_NAME = "govil-price-lists"   # <--- change to your bucket name if needed
S3_CLIENT = boto3.client("s3")

# logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger("crawler")

# regexes
ABS_DATE_RE = re.compile(r"(\d{1,2}):(\d{2})\s+(\d{1,2})/(\d{1,2})/(\d{4})")
REL_HE_RE = re.compile(r"לפני\s*(\d+)?\s*(שנייה|שניות|דקה|דקות|שעה|שעות|יום|ימים)", re.I)
GZ_ONCLICK_RE = re.compile(r"Download\(['\"]([^'\"]+\.gz)['\"]\)", re.I)
