#!/bin/sh
set -e

echo "Initializing SQS queue..."
sleep 5
awslocal sqs create-queue --queue-name sqs-queue 
QUEUE_URL=$(awslocal sqs get-queue-url --queue-name sqs-queue --output text)
echo "Queue URL: $QUEUE_URL"
echo "SQS initialization completed!"
