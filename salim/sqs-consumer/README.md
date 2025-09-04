# SQS Consumer Lambda Function

Processes SQS messages containing supermarket product data and inserts them into Supabase database.

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