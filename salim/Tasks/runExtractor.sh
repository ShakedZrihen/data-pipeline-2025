#!/bin/sh
set -euo pipefail

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}

AWS_ENDPOINT="http://localstack:4566"
AWS_REGION="${AWS_DEFAULT_REGION}"

BUCKET="providers-bucket"
QUEUE_NAME="providers-events"

LAMBDA_NAME="unzipLambda"
LAMBDA_HANDLER="lambda_unzip.handler"
LAMBDA_RUNTIME="python3.11"
LAMBDA_ROLE_ARN="arn:aws:iam::000000000000:role/lambda-role"
LAMBDA_ENDPOINT_IN_CONTAINER="http://localstack:4566"

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="${PROJECT_ROOT}/lambda_extractor"
PKG_ZIP="${PROJECT_ROOT}/unzip_lambda.zip"

awsls() { aws --endpoint-url="${AWS_ENDPOINT}" --region "${AWS_REGION}" "$@"; }
say()   { echo -e "\n==> $*"; }

say "Packaging unzip lambda → ${PKG_ZIP}"
rm -f "${PKG_ZIP}"
(
  cd "${SRC_DIR}"
  zip -r "${PKG_ZIP}" . -x "__pycache__/*" "*.pyc" ".DS_Store" "*/.DS_Store" > /dev/null
)

say "Ensuring S3 bucket s3://${BUCKET}"
awsls s3api head-bucket --bucket "${BUCKET}" >/dev/null 2>&1 || \
  awsls s3api create-bucket --bucket "${BUCKET}" >/dev/null

say "Ensuring SQS queue ${QUEUE_NAME}"
awsls sqs create-queue --queue-name "${QUEUE_NAME}" >/dev/null 2>&1 || true
QUEUE_URL="$(awsls sqs get-queue-url --queue-name "${QUEUE_NAME}" --query 'QueueUrl' --output text)"

if awsls lambda get-function --function-name "${LAMBDA_NAME}" >/dev/null 2>&1; then
  say "Updating ${LAMBDA_NAME} code"
  if ! awsls lambda update-function-code --function-name "${LAMBDA_NAME}" --zip-file "fileb://${PKG_ZIP}" >/dev/null; then
    say "Hot-swap failed, recreating ${LAMBDA_NAME} ..."
    awsls lambda delete-function --function-name "${LAMBDA_NAME}" >/dev/null 2>&1 || true
    awsls lambda create-function \
      --function-name "${LAMBDA_NAME}" \
      --runtime "${LAMBDA_RUNTIME}" \
      --role "${LAMBDA_ROLE_ARN}" \
      --handler "${LAMBDA_HANDLER}" \
      --timeout 60 \
      --memory-size 512 \
      --zip-file "fileb://${PKG_ZIP}" \
      --environment "Variables={ENDPOINT_URL=${LAMBDA_ENDPOINT_IN_CONTAINER},S3_BUCKET=${BUCKET},SQS_QUEUE_NAME=${QUEUE_NAME},AWS_REGION=${AWS_REGION}}" >/dev/null
  fi

  say "Updating ${LAMBDA_NAME} config (handler/env/limits)"
  awsls lambda update-function-configuration \
    --function-name "${LAMBDA_NAME}" \
    --handler "${LAMBDA_HANDLER}" \
    --runtime "${LAMBDA_RUNTIME}" \
    --timeout 60 \
    --memory-size 512 \
    --role "${LAMBDA_ROLE_ARN}" \
    --environment "Variables={ENDPOINT_URL=${LAMBDA_ENDPOINT_IN_CONTAINER},S3_BUCKET=${BUCKET},SQS_QUEUE_NAME=${QUEUE_NAME},AWS_REGION=${AWS_REGION}}" >/dev/null
else
  say "Creating lambda ${LAMBDA_NAME}"
  awsls lambda create-function \
    --function-name "${LAMBDA_NAME}" \
    --runtime "${LAMBDA_RUNTIME}" \
    --role "${LAMBDA_ROLE_ARN}" \
    --handler "${LAMBDA_HANDLER}" \
    --timeout 60 \
    --memory-size 512 \
    --zip-file "fileb://${PKG_ZIP}" \
    --environment "Variables={ENDPOINT_URL=${LAMBDA_ENDPOINT_IN_CONTAINER},S3_BUCKET=${BUCKET},SQS_QUEUE_NAME=${QUEUE_NAME},AWS_REGION=${AWS_REGION}}" >/dev/null
fi

say "Granting S3 invoke permission"
awsls lambda add-permission \
  --function-name "${LAMBDA_NAME}" \
  --statement-id s3invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn "arn:aws:s3:::${BUCKET}" \
  --source-account 000000000000 >/dev/null 2>&1 || true


awsls lambda wait function-active-v2 --function-name "${LAMBDA_NAME}"

say "Configuring S3 → Lambda notifications (only providers/*.gz)"
awsls s3api put-bucket-notification-configuration \
  --bucket "${BUCKET}" \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [
      {
        \"Id\": \"only-gz-under-providers\",
        \"LambdaFunctionArn\": \"arn:aws:lambda:${AWS_REGION}:000000000000:function:${LAMBDA_NAME}\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {
          \"Key\": {
            \"FilterRules\": [
              {\"Name\": \"prefix\", \"Value\": \"providers/\"},
              {\"Name\": \"suffix\", \"Value\": \".gz\"}
            ]
          }
        }
      }
    ]
  }" >/dev/null

echo "[INFO] Healthcheck ready flag written"
touch /tmp/ready.txt

say "Starting SQS consumer ..."
PYTHONUNBUFFERED=1 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION="${AWS_REGION}" \
AWS_ENDPOINT_URL="${AWS_ENDPOINT}" SQS_QUEUE_NAME="${QUEUE_NAME}" \
exec python3 -u "${PROJECT_ROOT}/sqs_consumer.py"

