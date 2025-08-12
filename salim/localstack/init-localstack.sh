#!/bin/bash

# Run LocalStack (with only S3 service)
docker run -p 4566:4566 -p 4510-4559:4510-4559 \
  -e SERVICES=s3 \
  -e DEBUG=1 \
  localstack/localstack