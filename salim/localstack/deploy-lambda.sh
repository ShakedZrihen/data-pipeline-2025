#!/bin/bash
set -e

echo "--- Starting Lambda Deployment ---"

REGION="us-east-1"
LAMBDA_NAME="prices-extractor-lambda"
ZIP_FILE="/etc/localstack/init/ready.d/extractor-lambda.zip"
S3_BUCKET="providers"

# 0) IAM role + inline policy
if ! awslocal iam get-role --role-name lambda-role >/dev/null 2>&1; then
  awslocal iam create-role --role-name lambda-role \
    --assume-role-policy-document '{
      "Version":"2012-10-17",
      "Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]
    }' >/dev/null
  awslocal iam put-role-policy --role-name lambda-role --policy-name lambda-inline --policy-document '{
      "Version":"2012-10-17",
      "Statement":[{"Effect":"Allow","Action":[
        "logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents",
        "s3:GetObject","sqs:SendMessage",
        "dynamodb:PutItem","dynamodb:UpdateItem","dynamodb:GetItem"
      ],"Resource":"*"}]
    }' >/dev/null
fi
ROLE_ARN=$(awslocal iam get-role --role-name lambda-role --query 'Role.Arn' --output text)

# 1) תשתיות (אידמפוטנטי)
awslocal s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null || awslocal s3api create-bucket --bucket "$S3_BUCKET" >/dev/null
awslocal sqs get-queue-url --queue-name prices-queue >/dev/null 2>&1 || awslocal sqs create-queue --queue-name prices-queue >/dev/null
QUEUE_URL=$(awslocal sqs get-queue-url --queue-name prices-queue --query QueueUrl --output text)
awslocal dynamodb list-tables --query "TableNames[]" --output text | grep -q "LastRunTimestamps" || \
awslocal dynamodb create-table --table-name LastRunTimestamps \
  --attribute-definitions AttributeName=ProviderBranchType,AttributeType=S \
  --key-schema AttributeName=ProviderBranchType,KeyType=HASH \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 >/dev/null

# 2) Lambda create/update
if awslocal lambda get-function --function-name "$LAMBDA_NAME" >/dev/null 2>&1; then
  echo "--> Updating code for $LAMBDA_NAME"
  awslocal lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file "fileb://$ZIP_FILE" >/dev/null
else
  echo "--> Creating function $LAMBDA_NAME"
  awslocal lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --runtime python3.9 \
    --role "$ROLE_ARN" \
    --handler lambda_function.lambda_handler \
    --zip-file "fileb://$ZIP_FILE" \
    --region "$REGION" >/dev/null
fi

awslocal lambda wait function-active-v2 --function-name "$LAMBDA_NAME"

awslocal lambda update-function-configuration \
  --function-name "$LAMBDA_NAME" \
  --environment "Variables={QUEUE_URL=$QUEUE_URL,DDB_TABLE=LastRunTimestamps}" >/dev/null

awslocal lambda add-permission \
  --function-name "$LAMBDA_NAME" \
  --statement-id s3invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::$S3_BUCKET >/dev/null 2>&1 || true

LAMBDA_ARN=$(awslocal lambda get-function --function-name "$LAMBDA_NAME" --query 'Configuration.FunctionArn' --output text)
awslocal s3api put-bucket-notification-configuration --bucket "$S3_BUCKET" --notification-configuration "{
  \"LambdaFunctionConfigurations\": [{
    \"Id\": \"prices-trigger\",
    \"LambdaFunctionArn\": \"${LAMBDA_ARN}\",
    \"Events\": [\"s3:ObjectCreated:*\"]   # שלב ראשון: בלי suffix
  }]
}"

echo "--- ✅ Deployment complete! ---"
