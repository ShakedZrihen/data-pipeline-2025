#!/bin/bash
set -e

# These lines ensure the script always has the right credentials
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"

echo "--- Starting Lambda Deployment ---"

# --- CONFIGURATION ---
LAMBDA_NAME="prices-extractor-lambda"
ZIP_FILE="fileb://extractor-lambda.zip"
S3_BUCKET="providers"
ROLE_ARN="arn:aws:iam::000000000000:role/lambda-role"
QUEUE_NAME="prices-queue"
TABLE_NAME="LastRunTimestamps"

# Get the SQS Queue URL
QUEUE_URL=$(aws --endpoint-url=http://localhost:4566 sqs get-queue-url --queue-name ${QUEUE_NAME} --query "QueueUrl" --output text)

# 1. DEPLOY LAMBDA (Create if not exists, otherwise update)
echo "--> Deploying Lambda function code..."
aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name "$LAMBDA_NAME" \
    --zip-file "$ZIP_FILE" > /dev/null || \
aws --endpoint-url=http://localhost:4566 lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --runtime python3.9 \
    --role "$ROLE_ARN" \
    --handler lambda_function.lambda_handler \
    --zip-file "$ZIP_FILE" \
    --timeout 30 \
    --memory-size 256 > /dev/null

# Update Lambda configuration with environment variables
aws --endpoint-url=http://localhost:4566 lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --environment "Variables={QUEUE_URL=$QUEUE_URL,DDB_TABLE=$TABLE_NAME}" > /dev/null

LAMBDA_ARN=$(aws --endpoint-url=http://localhost:4566 lambda get-function --function-name "$LAMBDA_NAME" --query 'Configuration.FunctionArn' --output text)

# 2. ADD PERMISSION FOR S3 TO INVOKE LAMBDA
echo "--> Adding S3 permission to Lambda..."
aws --endpoint-url=http://localhost:4566 lambda add-permission \
    --function-name "$LAMBDA_NAME" \
    --statement-id "s3-invoke-permission-$(date +%s)" \
    --action "lambda:InvokeFunction" \
    --principal s3.amazonaws.com \
    --source-arn "arn:aws:s3:::${S3_BUCKET}" > /dev/null

# 3. CREATE THE S3 TRIGGER
echo "--> Creating S3 trigger..."
aws --endpoint-url=http://localhost:4566 s3api put-bucket-notification-configuration \
    --bucket "$S3_BUCKET" \
    --notification-configuration '{
        "LambdaFunctionConfigurations": [{
            "LambdaFunctionArn": "'"$LAMBDA_ARN"'",
            "Events": ["s3:ObjectCreated:*"]
        }]
    }'

echo "✅ Deployment and trigger setup complete!"