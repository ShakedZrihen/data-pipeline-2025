#!/bin/sh
set -e
echo "Initializing S3 bucket..."
awslocal s3 mb s3://supermarkets || true
echo "S3 initialization completed!"
