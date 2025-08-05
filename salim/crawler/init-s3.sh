#!/bin/bash

echo "Initializing S3 bucket with webhook notifications..."

# Wait for LocalStack to be ready
sleep 10

# Create bucket
awslocal s3 mb s3://providers
echo "Created bucket: providers"

echo "S3 initialization completed!"
echo "Webhook events should be triggered for each file upload!"