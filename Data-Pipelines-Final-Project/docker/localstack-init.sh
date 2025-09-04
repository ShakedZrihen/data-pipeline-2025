#!/usr/bin/env bash
set -euo pipefail

export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-eu-central-1}

echo "==> creating buckets"
awslocal s3 mb s3://price-data || true
awslocal s3 mb s3://govil-price-lists || true

echo "==> creating queues"
awslocal sqs create-queue --queue-name price-extractor-in            >/dev/null
awslocal sqs create-queue --queue-name price-extractor-events        >/dev/null
awslocal sqs create-queue --queue-name price-extractor-events-dlq    >/dev/null

ACCOUNT_ID=000000000000
REGION=eu-central-1
QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT_ID}:price-extractor-in"

cat >/tmp/notification.json <<'JSON'
{
  "QueueConfigurations": [
    {
      "Id": "s3-to-sqs-in",
      "QueueArn": "REPLACE_ME",
      "Events": ["s3:ObjectCreated:*"]
    }
  ]
}
JSON

sed -i "s#REPLACE_ME#${QUEUE_ARN}#g" /tmp/notification.json
awslocal s3api put-bucket-notification-configuration \
  --bucket price-data \
  --notification-configuration file:///tmp/notification.json

echo "==> init done"
