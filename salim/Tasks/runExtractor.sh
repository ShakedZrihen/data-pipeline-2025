
#!/usr/bin/env bash
set -euo pipefail

# ---- LocalStack dummy creds for this script ----
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}

AWS_ENDPOINT="http://localhost:4566"                # CLI talks to localhost
AWS_REGION="${AWS_DEFAULT_REGION}"

BUCKET="providers-bucket"
QUEUE_NAME="providers-events"

LAMBDA_NAME="unzipLambda"
LAMBDA_HANDLER="lambda_unzip.handler"               # flat layout
LAMBDA_RUNTIME="python3.11"
LAMBDA_ROLE_ARN="arn:aws:iam::000000000000:role/lambda-role"
LAMBDA_ENDPOINT_IN_CONTAINER="http://localstack:4566"  # hostname visible to lambda container

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="${PROJECT_ROOT}/extractor/lambda/lambda_extractor"
PKG_ZIP="${PROJECT_ROOT}/unzip_lambda.zip"

awsls() { aws --endpoint-url="${AWS_ENDPOINT}" --region "${AWS_REGION}" "$@"; }
say()   { echo -e "\n==> $*"; }

# ---- Package lambda (flat, exclude junk) ----
say "Packaging unzip lambda → ${PKG_ZIP}"
rm -f "${PKG_ZIP}"
(
  cd "${SRC_DIR}"
  zip -r "${PKG_ZIP}" . \
    -x "__pycache__/*" "*.pyc" ".DS_Store" "*/.DS_Store" > /dev/null
)

# ---- Ensure S3 bucket + SQS queue ----
say "Ensuring S3 bucket s3://${BUCKET}"
awsls s3 mb "s3://${BUCKET}" >/dev/null 2>&1 || true

say "Ensuring SQS queue ${QUEUE_NAME}"
awsls sqs create-queue --queue-name "${QUEUE_NAME}" >/dev/null 2>&1 || true
QUEUE_URL="$(awsls sqs get-queue-url --queue-name "${QUEUE_NAME}" --query 'QueueUrl' --output text)"

# ---- Create or update Lambda ----
if awsls lambda get-function --function-name "${LAMBDA_NAME}" >/dev/null 2>&1; then
  say "Updating ${LAMBDA_NAME} code"
  awsls lambda update-function-code \
    --function-name "${LAMBDA_NAME}" \
    --zip-file "fileb://${PKG_ZIP}" >/dev/null

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

# ---- Allow S3 to invoke Lambda (idempotent) ----
say "Granting S3 invoke permission"
awsls lambda add-permission \
  --function-name "${LAMBDA_NAME}" \
  --statement-id s3invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn "arn:aws:s3:::${BUCKET}" \
  --source-account 000000000000 >/dev/null 2>&1 || true

# ---- Wire S3 → Lambda notifications (CHANGED: add prefix/suffix filter to only .gz under providers/) ----
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

# ---- Wire S3 → Lambda notifications (CHANGED: add prefix/suffix filter to only .gz under providers/) ----
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

# ---- Start consumer (blocking) ----
say "Starting SQS consumer ..."
QUEUE_URL="${QUEUE_URL}" ENDPOINT_URL="${AWS_ENDPOINT}" \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION="${AWS_REGION}" \
python3 "${PROJECT_ROOT}/extractor/lambda/lambda_consumer/sqs_consumer.py"
