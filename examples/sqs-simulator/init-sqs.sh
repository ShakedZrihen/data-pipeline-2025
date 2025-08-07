#!/bin/bash

echo "Initializing SQS queue..."

# Wait for LocalStack to be ready
sleep 10

# Create SQS queue
awslocal sqs create-queue --queue-name test-queue
echo "Created queue: test-queue"

# Get queue URL for reference
QUEUE_URL=$(awslocal sqs get-queue-url --queue-name test-queue --output text)
echo "Queue URL: $QUEUE_URL"

# Wait for Lambda function to be ready
echo "Waiting for Lambda function to be ready..."
sleep 5

echo "SQS initialization completed!"
echo "Queue is ready to receive messages!"