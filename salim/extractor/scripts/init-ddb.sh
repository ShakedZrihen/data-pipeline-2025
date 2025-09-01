#!/usr/bin/env bash

echo "Creating table '$DDB_TABLE'..."
aws --endpoint-url "$LOCALSTACK_ENDPOINT" --region "$AWS_REGION" dynamodb create-table \
--table-name "$DDB_TABLE" \
--attribute-definitions AttributeName=pk,AttributeType=S \
--key-schema AttributeName=pk,KeyType=HASH \
--billing-mode PAY_PER_REQUEST

echo "Waiting for table to be ready..."
aws --endpoint-url "$LOCALSTACK_ENDPOINT" --region "$AWS_REGION" dynamodb wait table-exists --table-name "$DDB_TABLE"
echo "Table '$DDB_TABLE' is ready."
