#!/bin/sh
set -e

# Ensure required env vars are present or set defaults
AWS_REGION="${AWS_REGION:-il-central-1}"
S3_BUCKET="${S3_BUCKET:-salim-prices}"

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION S3_BUCKET

python /app/crawler.py "$S3_BUCKET" --config /app/config.json

