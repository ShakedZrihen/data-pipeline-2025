#!/bin/bash

echo "Initializing S3 bucket and SQS queue..."

# Wait for LocalStack to be ready
sleep 10

# Check if bucket already exists
if awslocal s3 ls | grep -q "test-bucket"; then
  echo "Bucket 'test-bucket' already exists. Skipping creation."
else
  awslocal s3 mb s3://test-bucket
  echo "Created bucket: test-bucket"
fi

# Create SQS queue if it doesn't exist
if awslocal sqs list-queues | grep -q "my-queue"; then
  echo "Queue 'my-queue' already exists. Skipping creation."
else
  awslocal sqs create-queue --queue-name my-queue
  echo "Created SQS queue: my-queue"
fi

# Wait for Lambda function to be ready
echo "Waiting for Lambda function to be ready..."
sleep 5

echo "S3 and SQS initialization completed!"
echo "Webhook events should be triggered for each file upload."
