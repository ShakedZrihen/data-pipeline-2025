# Extractor Lambda Function

Processes uploaded S3 files (pricesFull/promoFull), extracts product data, and sends it to SQS for further processing.

## Quick Setup

### 1. Deploy to AWS Lambda

1. **Create deployment package** with all Python files and dependencies
2. **Function name**: `supermarket-extractor`
3. **Runtime**: Python 3.11
4. **Handler**: `extractor.lambda_handler`
5. **Timeout**: 15 minutes
6. **Memory**: 512 MB

### 2. Environment Variables

```
S3_BUCKET_NAME = your-s3-bucket-name
SQS_QUEUE_URL = your-sqs-queue-url
DYNAMODB_TABLE_NAME = your-dynamodb-table-name
LOG_LEVEL = INFO
```

### 3. IAM Execution Role

Create a Lambda execution role with these permissions:

- **CloudWatch Logs access** (attach `AWSLambdaBasicExecutionRole` policy)
- **S3 permissions**: GetObject, PutObject on your S3 bucket
- **SQS permissions**: SendMessage on your SQS queue
- **DynamoDB permissions**: GetItem, PutItem, UpdateItem on your tracking table
- **Trust relationship**: Allow Lambda service to use this role

### 4. Configure S3 Trigger

- **Source**: S3 bucket
- **Event type**: Object Created (All)
- **Prefix**: (optional, e.g., `uploads/`)
- **Suffix**: `.gz` or `.xml` (based on your file types)

## Local Testing

```bash
pip install -r requirements.txt
python -c "
from extractor import ExtractorLambda
extractor = ExtractorLambda()
print('Extractor initialized successfully!')
"
```

## Files

- **extractor.py** - Main Lambda handler
- **config.py** - Configuration management
- **s3_handler.py** - S3 file operations
- **file_processor.py** - XML file processing
- **data_normalizer.py** - Data cleaning and normalization
- **sqs_producer.py** - SQS message publishing
- **dynamodb_tracker.py** - Processing tracking
- **requirements.txt** - Python dependencies

## Features

- Processes both `pricesFull` and `promoFull` XML files
- Extracts product data with Hebrew text support
- Normalizes prices, units, and product names
- Tracks processing history in DynamoDB
- Sends processed data to SQS for consumption
- Comprehensive error handling and logging
- Duplicate file detection

## Data Flow

1. **File uploaded** to S3 bucket
2. **S3 event triggers** Lambda function
3. **Download file** from S3
4. **Extract XML data** (products, prices, metadata)
5. **Normalize data** (clean names, convert units)
6. **Track processing** in DynamoDB
7. **Send batches** to SQS queue
8. **Log results** to CloudWatch

## Troubleshooting

- **S3 access denied**: Check S3 permissions in IAM role
- **SQS send failed**: Verify SQS queue URL and permissions
- **DynamoDB errors**: Check table name and permissions
- **File processing failed**: Check file format (XML structure)
- **Memory issues**: Increase Lambda memory for large files
- **Timeout**: Increase Lambda timeout for large files
- **Logs**: Check CloudWatch `/aws/lambda/supermarket-extractor`

## Configuration

Environment variables can be set in Lambda console or config.py:

- **S3_BUCKET_NAME**: Source bucket for uploaded files
- **SQS_QUEUE_URL**: Target queue for processed data
- **DYNAMODB_TABLE_NAME**: Table for tracking processing history
- **LOG_LEVEL**: DEBUG, INFO, WARNING, ERROR

## Important Notes

- Handler must be `extractor.lambda_handler`
- Requires all Python files in deployment package
- Handles both compressed (.gz) and uncompressed files
- Supports Hebrew text encoding
- Automatically detects file type from filename