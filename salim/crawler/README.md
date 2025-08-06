# Supermarket Crawler

Downloads .gz files from supermarkets and uploads them to S3.

## How to run

1. Start S3 simulator:
   ```bash
   cd examples/s3-simulator
   docker-compose up -d
   ```

2. Install Python dependencies:
   ```bash
   cd salim/crawler
   pip install -r requirements.txt
   ```

3. Run all crawlers:
   ```bash
   python run.py
   ```

Or run specific crawler:
   ```bash
   python run.py yohananof
   ```

## Available configs
- `yohananof` 
- `osherad`
- `ramilevi` 
- `tivtaam`

## Files

- `base.py` - the main crawler
- `run.py` - run the crawler(s)
- `configs/` - settings for each supermarket
- `downloads/` - where files get saved
