# Extractor Lambda Module

## Overview
Processes compressed XML files from S3, extracts price/promotion data, sends to SQS, and tracks processing timestamps.

## 🏗️ Architecture
```
S3 (trigger) → Extractor Lambda → SQS → Consumer Lambda (prints JSON)
```

## 📁 Project Structure
```
extractor/
├── lambda_function.py      # Main Lambda handler (entry point)
├── consumer_lambda.py      # Consumer Lambda that prints JSON from SQS
├── src/                    # Modular components (NOT all logic in one file!)
│   ├── __init__.py
│   ├── s3_handler.py      # S3 operations (download, decompress)
│   ├── xml_processor.py   # XML parsing for different providers
│   ├── normalizer.py      # Data normalization to JSON
│   └── sqs_producer.py    # SQS message publishing
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## ✨ Features
- **Modular Design**: Logic separated into different modules as required
- **Multi-Provider Support**: Handles Victory, Carrefour, Yohananof schemas
- **Error Handling**: Validates files, handles corrupt data
- **Local JSON Output**: Saves results locally for testing/review
- **SQS Integration**: Sends normalized data to SQS queue

## 🚀 Deployment

### Lambda Configuration
```python
Runtime: Python 3.11
Handler: lambda_function.lambda_handler
Timeout: 60 seconds
Memory: 512 MB
```

### Environment Variables
```bash
SQS_QUEUE_URL=https://sqs.il-central-1.amazonaws.com/YOUR_ACCOUNT/price-updates
SAVE_LOCAL_JSON=true  # For testing, saves JSON to /tmp
AWS_REGION=il-central-1
AWS_DEFAULT_REGION=il-central-1
```

### S3 Trigger Configuration
```json
{
  "LambdaFunctionConfigurations": [
    {
      "LambdaFunctionArn": "arn:aws:lambda:REGION:ACCOUNT:function:salim-extractor",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {"Name": "prefix", "Value": "providers/"},
            {"Name": "suffix", "Value": ".gz"}
          ]
        }
      }
    }
  ]
}
```

## 📦 Input Format

### S3 Path Structure
```
providers/<provider>/<branch>/<type>Full_<timestamp>.gz
```

Example:
```
providers/victory/tel-aviv/pricesFull_20250808_120000.gz
providers/yohananof/jerusalem/promoFull_20250808_180000.gz
```

## 📤 Output Format

### SQS Message JSON
```json
{
  "provider": "victory",
  "branch": "tel-aviv",
  "type": "pricesFull",
  "timestamp": "2025-08-08T12:00:00Z",
  "items_count": 294,
  "source_file": "providers/victory/tel-aviv/pricesFull_20250808_120000.gz",
  "items": [
    {
      "product": "חלב תנובה 3%",
      "price": 5.9,
      "unit": "liter",
      "barcode": "7290000009358",
      "manufacturer": "תנובה"
    }
  ]
}
```

## 🧪 Local Testing (Works Without AWS!)

### 1. Test with Sample Data
```bash
cd extractor
python test_local.py
```

### 2. Test with Your Files
```bash
# With Victory price file
python test_local.py /path/to/PriceFull.gz victory tel-aviv pricesFull

# With Carrefour promo file  
python test_local.py /path/to/PromoFull.gz carrefour jerusalem promoFull
```

### 3. Test Lambda Functions (Simulated)
```bash
# Test extractor (needs boto3 but works without AWS credentials)
python lambda_function.py

# Test consumer
python consumer_lambda.py
```

### 4. Check Output
All test outputs are saved in `test_output/` directory as JSON files

## 📝 Module Descriptions

### `lambda_function.py`
- Main entry point for Lambda
- Orchestrates the entire processing flow
- Handles S3 events

### `src/s3_handler.py`
- Downloads files from S3
- Decompresses .gz files
- Parses S3 keys to extract metadata

### `src/xml_processor.py`
- Parses XML with provider-specific logic
- Handles different schemas (Victory, Carrefour, Yohananof)
- Extracts product/price/promotion data

### `src/normalizer.py`
- Converts parsed XML to standardized JSON
- Normalizes Hebrew units to English
- Ensures consistent output format

### `src/sqs_producer.py`
- Sends messages to SQS
- Handles batch sending
- Adds message attributes

### `consumer_lambda.py`
- Reads messages from SQS
- Prints JSON content (as per requirement)
- Returns processing status

## 🔍 Validation
The extractor includes validation to:
- Ensure files are valid gzip
- Verify XML content (not HTML error pages)
- Handle missing/malformed data gracefully

## 📊 Performance
- Processes files up to 100MB
- Handles 200+ items per file
- Timeout set to 60 seconds

## 👤 Author
Amir Khalifa - BigData Course Project 
