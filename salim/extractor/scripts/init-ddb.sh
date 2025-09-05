#!/bin/sh
set -e

TABLE="${DDB_TABLE:-ExtractorState}"

echo "Creating table '$TABLE'..."
awslocal dynamodb create-table \
  --table-name "$TABLE" \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST || true

echo "Waiting for table '$TABLE'..."
awslocal dynamodb wait table-exists --table-name "$TABLE"
echo "Table '$TABLE' is ready."
