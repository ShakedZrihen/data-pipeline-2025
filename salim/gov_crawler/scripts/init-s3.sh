#!/bin/bash

echo "Initializing S3 bucket with webhook notifications..."

# Wait for LocalStack to be ready
sleep 10

# Create bucket
awslocal s3 mb s3://supermarkets
echo "Created bucket: supermarkets"

# Wait for Lambda function to be ready
echo "Waiting for Lambda function to be ready..."
sleep 5

echo "S3 initialization completed!"
echo "Webhook events should be triggered for each file upload!"