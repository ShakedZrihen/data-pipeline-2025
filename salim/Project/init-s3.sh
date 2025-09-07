#!/bin/bash

echo "Initializing S3..."

# Wait for LocalStack to be ready
sleep 5

# Create the test-bucket
echo "Creating test-bucket..."
aws --endpoint-url=http://s3:4566 s3 mb s3://test-bucket

# Set bucket policy for public read access
echo "Setting bucket policy..."
aws --endpoint-url=http://s3:4566 s3api put-bucket-policy --bucket test-bucket --policy '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadGetObject",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::test-bucket/*"
    }
  ]
}'

echo "S3 initialization completed successfully!"
