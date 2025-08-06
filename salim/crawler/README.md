# Supermarket Crawler

Downloads .gz files from supermarkets and uploads them to S3.

## How to run

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the crawler:
   ```bash
   python run.py yohananof
   ```

That's it! Works on Windows & Mac.

## Files

- `base.py` - the main crawler
- `run.py` - run the crawler
- `configs/yohananof.json` - settings for Yohananof
- `downloads/` - where files get saved
