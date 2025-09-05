# Supermarket Crawlers

Automated web crawlers that download supermarket price and promotion files from Israeli supermarket chains and upload them to S3.

## Quick Setup

### 1. Create S3 Bucket (Required)

**Important**: Create an S3 bucket first (crawlers don't create it automatically):
```bash
aws s3 mb s3://your-bucket-name
```

### 2. Local Environment

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Install Chrome browser** and ChromeDriver
3. **Create `.env` file** with your credentials

### 3. Environment Variables (.env file)

```
AWS_ACCESS_KEY_ID = your-aws-access-key
AWS_SECRET_ACCESS_KEY = your-aws-secret-key  
AWS_DEFAULT_REGION = us-east-1
S3_BUCKET_NAME = your-s3-bucket-name
```

## Files

- **crawler.py** - Main crawler orchestrator
- **config.py** - Supermarket configurations and URLs
- **browser_manager.py** - Chrome browser setup and management
- **web_scraper.py** - Web scraping logic and file downloads
- **file_manager.py** - Local file handling and cleanup
- **s3_manager.py** - S3 upload operations
- **branch_mapper.py** - Maps supermarket branches to locations
- **Branches/** - Branch-specific configuration files

## Usage

```bash
# Run specific supermarket
python crawler.py politzer

# Run all supermarkets
python crawler.py
```

## Features

- **Multi-supermarket support**: Politzer, Keshet, Yohananof
- **Automated login**: Handles supermarket website authentication
- **File download**: Downloads pricesFull and promoFull files
- **Branch mapping**: Processes multiple branches per supermarket
- **S3 upload**: Automatically uploads files to AWS S3
- **File cleanup**: Removes temporary files after upload
- **Error handling**: Comprehensive error logging and retry logic
- **Headless browsing**: Runs without GUI for automation

## Supported Supermarkets

| Supermarket   | 
|---------------|
| Politzer      | 
| Keshet        | 
| Yohananof     | 

## Data Flow

1. **Initialize browser** with Chrome WebDriver
2. **Login** to supermarket website
3. **Navigate** to file download section
4. **Download files** for each branch (prices + promotions)
5. **Upload files** to S3 bucket
6. **Clean up** temporary files
7. **Log results** and errors

## Local Testing

```bash
# Test browser setup
python -c "
from browser_manager import BrowserManager
browser = BrowserManager()
driver = browser.get_driver()
print('Browser initialized successfully!')
driver.quit()
"

# Test S3 connection
python -c "
from s3_manager import S3Manager
s3 = S3Manager()
print('S3 connection successful!')
"
```

## Configuration

### Adding New Supermarket

1. **Add to config.py**:
```python
SUPERMARKETS = {
    "new_chain": {
        "username": "new_chain", 
        "branches": ["branch1", "branch2"]
    }
}
```

2. **Update web scraping logic** in `web_scraper.py`
3. **Add branch mappings** in `branch_mapper.py`

### Customizing Downloads

- **Download directory**: Set in `browser_manager.py`
- **File patterns**: Configure in `web_scraper.py`
- **S3 bucket structure**: Modify in `s3_manager.py`

## Troubleshooting

- **Chrome driver issues**: Update ChromeDriver version
- **Login failed**: Check supermarket credentials
- **Download timeout**: Increase wait times in `web_scraper.py`
- **S3 upload failed**: Verify AWS credentials and permissions
- **Files not found**: Check download directory and file patterns
- **Browser crashes**: Restart with fresh browser instance

## Important Notes

- Crawlers run in **headless mode** by default
- Downloads are stored in temporary directory
- Files are automatically cleaned after S3 upload
- Supports Hebrew text and special characters
- Handles dynamic website loading with proper wait times
- Uses search-based approach for efficient branch processing

## AWS Permissions Required

- **S3 permissions**: PutObject on your bucket
- **IAM user**: With programmatic access enabled