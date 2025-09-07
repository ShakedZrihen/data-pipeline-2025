# Supermarket Crawler

Downloads .gz files from supermarkets and uploads them to S3.

## How to run

1. Start the entire pipeline:
   ```bash
   cd salim/Project
   docker-compose up -d
   ```

2. If you need to create the S3 bucket manually:
   ```bash
   cd salim/Project/crawler
   python create_bucket.py
   ```

3. Run the crawler manually (if not using Docker):
   ```bash
   cd salim/Project/crawler
   pip install -r requirements.txt
   python run.py
   ```

## Docker Setup

The crawler runs automatically when you start the pipeline with `docker-compose up -d`.

