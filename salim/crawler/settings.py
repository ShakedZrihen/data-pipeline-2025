import boto3

# URLs and Provider Configuration
GOV_URL = "https://www.gov.il/he/pages/cpfta_prices_regulations"
PROVIDERS = {
    "יוחננוף": {"username": "yohananof", "password": "", "folder": "yohananof"},
    "חצי חינם": {"username": "", "password": "", "folder": "hatzi-hinam"},
    "ויקטורי": {"username": "", "password": "", "folder": "victory"},
}

# S3 Configuration
S3_BUCKET = "gov-price-files-hanif-2025"
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)