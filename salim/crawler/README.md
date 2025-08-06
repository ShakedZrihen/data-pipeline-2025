# Supermarket Crawler

Downloads .gz files from supermarkets and uploads them to S3.

## How to run

1. Start S3 and create bucket:
   ```bash
   cd examples/s3-simulator
   docker-compose up -d
   python create_bucket.py //if a bucket wasnet created
   ```

2. Run the crawler:
   ```bash
   cd salim/crawler
   pip install -r requirements.txt
   python run.py
   ```

