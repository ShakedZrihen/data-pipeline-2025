# SQS Consumer Lambda Function

Processes SQS messages containing supermarket product data, enriches them with OpenAI, and inserts them into Supabase database.

## Architecture

This Lambda function uses **Docker containerization** for building Linux-compatible deployment packages with all dependencies included. The OpenAI enrichment processes products in batches of 50 items per API call, with failed batches sent to a Dead Letter Queue for retry processing.

## Quick Setup

### 1. Deploy to AWS Lambda

1. **Upload** `sqs-consumer-final.zip` to AWS Lambda
2. **Function name**: `supermarket-sqs-consumer`
3. **Runtime**: Python 3.11
4. **Handler**: `consumer_simple.lambda_handler`
5. **Timeout**: 10 minutes
6. **Memory**: 1024 MB

### 2. Environment Variables

```
SUPABASE_URL = 
SUPABASE_SERVICE_ROLE_KEY =
```

### 3. IAM Execution Role

Create a Lambda execution role with these permissions:

- **CloudWatch Logs access** (attach `AWSLambdaBasicExecutionRole` policy)
- **SQS permissions**: ReceiveMessage, DeleteMessage, GetQueueAttributes on your SQS queue
- **Trust relationship**: Allow Lambda service to use this role

### 4. Configure SQS Trigger

- **Source**: SQS queue
- **Batch size**: 1
- **Enable trigger**: Yes

## Package Creation Process

Create the Lambda deployment package using Docker to ensure Linux compatibility:

```bash
# Install dependencies using Docker (from sqs-consumer directory)
docker run --rm -v "C:\Users\HaDzE7\Desktop\University SE\data-pipeline-2025\salim\sqs-consumer:/var/task" python:3.11 pip install -r /var/task/requirements.txt -t /var/task

# Create deployment zip (include all files and dependencies)
zip -r sqs-consumer-package.zip . -x "*.zip" "docker-build/*" "*.pyc" "__pycache__/*"
```

**Note**: This command installs Python dependencies directly into the project folder using Docker, ensuring Linux compatibility for AWS Lambda.

## Local Testing

```bash
pip install -r requirements.txt
python -c "
from consumer_simple import SimpleSQSConsumer
consumer = SimpleSQSConsumer()
print('Connection test passed!')
"
```

## Files

- **consumer_simple.py** - Main Lambda code
- **lambda_function.py** - AWS entry point
- **sqs-consumer-final.zip** - Deployment package

## Features

- **OpenAI Product Enrichment**: Batch processing (50 items/call) for manufacturer, category, and kosher status
- **Docker Containerization**: Linux-compatible builds using AWS Lambda Python base image
- **Dead Letter Queue Integration**: Failed enrichment batches sent to DLQ for retry processing
- **Improved JSON Parsing**: Handles OpenAI markdown-wrapped responses correctly
- Processes both `pricesFull` and `promoFull` files
- Handles duplicate entries automatically
- Normalizes Hebrew product names and units
- Comprehensive error logging

## Troubleshooting

- **Timeout**: Increase Lambda timeout to 10 minutes
- **Duplicates**: Function handles this automatically
- **Logs**: Check CloudWatch `/aws/lambda/supermarket-sqs-consumer`

## Important Notes

- Handler must be `consumer_simple.lambda_handler`
- Requires SQS permissions in Lambda execution role