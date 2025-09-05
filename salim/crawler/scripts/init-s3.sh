#!/bin/sh

echo "Initializing S3 bucket with webhook notifications..."

# Wait for LocalStack to be ready
sleep 10

# Create bucket
awslocal s3 mb s3://supermarkets
echo "Created bucket: supermarkets"

