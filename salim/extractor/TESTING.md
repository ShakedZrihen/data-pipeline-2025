# Testing Instructions

## Quick Setup

1. **Install dependencies**
```bash
pip install -r requirements.txt
```

2. **Configure AWS credentials**
Create `.env` file:
```bash
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=il-central-1
```

## Test Options

### Option 1: Local Test (No AWS Required)
```bash
# Test with sample data
python test_local.py

# Test with your own .gz file
python test_local.py /path/to/file.gz victory tel-aviv pricesFull
```

### Option 2: Real AWS Test (Requires AWS Account)
```bash
# Create AWS resources first
aws s3 mb s3://test-bucket --region il-central-1
aws sqs create-queue --queue-name test-queue --region il-central-1

# Run test
python test_real_aws.py
```
This will:
1. Upload a file to S3
2. Process it with Lambda logic
3. Send message to SQS
4. Update timestamp database

## Verify Requirements

✅ **Modular code**: Check `src/` directory - logic is separated into modules
✅ **S3 trigger**: Lambda processes S3 events (see `lambda_handler` in `lambda_function.py`)
✅ **SQS output**: Messages sent to queue (see `src/sqs_producer.py`)
✅ **Database update**: Timestamps tracked (see `src/db_handler.py`)

## Expected Output

Successful test shows:
- File processed from S3
- Message sent to SQS with normalized data
- Timestamp recorded in database
- JSON saved locally in `test_output/` or `/tmp/extractor_output/`
