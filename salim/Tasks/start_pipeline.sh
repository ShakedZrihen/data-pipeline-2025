
#!/usr/bin/env bash
set -euo pipefail

############################################
# Config (override by exporting before run)
############################################
EXTRACTOR_DIR="${EXTRACTOR_DIR:-extractor/lambda/lambda_extractor}"
CONSUMER_DIR="${CONSUMER_DIR:-extractor/lambda/lambda_consumer}"
BUCKET="${BUCKET:-providers-bucket}"
QUEUE="${QUEUE:-providers-events}"
REGION="${REGION:-us-east-1}"
ENDPOINT="${ENDPOINT:-http://localhost:4566}"
ZIP_DIR="${ZIP_DIR:-$PWD}"

# Lambdas run in their own Docker containers; they must call the LocalStack container by DNS name:
LAMBDA_ENDPOINT="${LAMBDA_ENDPOINT:-http://s3-simulator:4566}"



# Logging mode: container | cw | none
LOG_MODE="${LOG_MODE:-container}"

# Optional: re-trigger processing for already-present .gz files
REPLAY_EXISTING="${REPLAY_EXISTING:-0}"

# Mirror Lambda logs to S3 (if your code uses it)
LOG_TO_S3="${LOG_TO_S3:-0}"
LOG_BUCKET="${LOG_BUCKET:-$BUCKET}"

############################################
# Local credentials for LocalStack
############################################
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$REGION}"
export AWS_S3_FORCE_PATH_STYLE=true

awslocal() { aws --endpoint-url="$ENDPOINT" --region "$REGION" "$@"; }
log(){ printf "\n\033[1;36m[start]\033[0m %s\n" "$*"; }
need(){ command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1"; exit 1; }; }

zip_dir_into(){ # $1=dir $2=outzip
  local dir="$1" out="$2"
  rm -f "$out"
  if [ -d "$dir" ]; then
    (cd "$dir" && zip -qr "$out" .)
  else
    echo "ERROR: directory not found: $dir"; exit 1;
  fi
}

need aws; need zip; need docker; need docker-compose

############################################
# 1) Start LocalStack
############################################
log "Starting LocalStack..."
docker-compose up -d

############################################
# 2) Ensure S3 ready + create bucket
############################################
log "Ensuring S3 is ready (path-style on)..."
for i in $(seq 1 40); do
  if awslocal s3api list-buckets >/dev/null 2>&1; then break; fi
  sleep 1
  [ "$i" -eq 40 ] && { echo "ERROR: S3 not ready after 40s"; exit 1; }
done

log "Creating S3 bucket: s3://$BUCKET"
awslocal s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1 || true
awslocal s3api head-bucket --bucket "$BUCKET" >/dev/null

############################################
# 2.5) Ensure DynamoDB table for last runs
############################################
log "Ensuring DynamoDB table: LastRuns"
awslocal dynamodb create-table \
  --table-name LastRuns \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST >/dev/null 2>&1 || true

############################################
# 3) Package Lambdas
############################################
log "Packaging extractor..."
zip_dir_into "$EXTRACTOR_DIR" "$ZIP_DIR/extractor.zip"

log "Packaging consumer..."
zip_dir_into "$CONSUMER_DIR" "$ZIP_DIR/consumer.zip"

############################################
# 4) Deploy/Update Lambdas (env vars included)
############################################
LAMBDA_ENV="Variables={ENDPOINT_URL=$LAMBDA_ENDPOINT,AWS_REGION=$REGION,S3_BUCKET=$BUCKET,SQS_QUEUE_NAME=$QUEUE,DDB_TABLE=LastRuns,OUTPUT_JSON_PREFIX=Json/,LOG_TO_S3=$LOG_TO_S3,LOG_BUCKET=$LOG_BUCKET}"

log "Deploy extractor"
if awslocal lambda get-function --function-name extractor >/dev/null 2>&1; then
  awslocal lambda update-function-code --function-name extractor --zip-file "fileb://$ZIP_DIR/extractor.zip" >/dev/null
  awslocal lambda update-function-configuration --function-name extractor \
    --environment "$LAMBDA_ENV" \
    --handler extractor.handler \
    --timeout 60 \
    --memory-size 512 >/dev/null
else
  awslocal lambda create-function \
    --function-name extractor \
    --runtime python3.11 \
    --handler extractor.handler \
    --zip-file "fileb://$ZIP_DIR/extractor.zip" \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 60 \
    --memory-size 512 \
    --environment "$LAMBDA_ENV" >/dev/null
fi

log "Deploy consumer"
if awslocal lambda get-function --function-name consumer >/dev/null 2>&1; then
  awslocal lambda update-function-code --function-name consumer --zip-file "fileb://$ZIP_DIR/consumer.zip" >/dev/null
  awslocal lambda update-function-configuration --function-name consumer \
    --environment "$LAMBDA_ENV" \
    --handler consumer_handler.handler \
    --timeout 60 \
    --memory-size 256 >/dev/null
else
  awslocal lambda create-function \
    --function-name consumer \
    --runtime python3.11 \
    --handler consumer_handler.handler \
    --zip-file "fileb://$ZIP_DIR/consumer.zip" \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 60 \
    --memory-size 256 \
    --environment "$LAMBDA_ENV" >/dev/null
fi

############################################
# 5) Wire S3 -> extractor (no filter, for debug)
############################################
log "Allow S3 to invoke extractor"
awslocal lambda add-permission \
  --function-name extractor \
  --statement-id s3invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn "arn:aws:s3:::$BUCKET" >/dev/null 2>&1 || true

# Get the function ARN dynamically
EXTRACTOR_ARN=$(awslocal lambda get-function --function-name extractor \
  --query 'Configuration.FunctionArn' --output text)

log "Reset bucket notifications"
awslocal s3api put-bucket-notification-configuration \
  --bucket "$BUCKET" \
  --notification-configuration '{}'

log "Set bucket notification (ObjectCreated -> extractor, no filter)"
awslocal s3api put-bucket-notification-configuration --bucket "$BUCKET" \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [{
      \"Id\": \"InvokeExtractorOnCreate\",
      \"LambdaFunctionArn\": \"$EXTRACTOR_ARN\",
      \"Events\": [\"s3:ObjectCreated:*\"] 
    }]
  }"

# If you want to switch to filtered triggers later, replace the block above with:
# awslocal s3api put-bucket-notification-configuration --bucket "$BUCKET" \
#   --notification-configuration \"{
#     \\\"LambdaFunctionConfigurations\\\": [{
#       \\\"Id\\\": \\\"InvokeExtractorOnGz\\\",
#       \\\"LambdaFunctionArn\\\": \\\"$EXTRACTOR_ARN\\\",
#       \\\"Events\\\": [\\\"s3:ObjectCreated:*\\\"],
#       \\\"Filter\\\": { \\\"Key\\\": { \\\"FilterRules\\\": [
#         {\\\"Name\\\": \\\"prefix\\\", \\\"Value\\\": \\\"providers/\\\"},
#         {\\\"Name\\\": \\\"suffix\\\", \\\"Value\\\": \\\".gz\\\"}
#       ]}}
#     }]
#   }\"

############################################
# 6) Create SQS + map to consumer
############################################
log "Create SQS queue + map to consumer"
QUEUE_URL=$(awslocal sqs create-queue --queue-name "$QUEUE" --query QueueUrl --output text)
QUEUE_ARN=$(awslocal sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
if ! awslocal lambda list-event-source-mappings --function-name consumer --query 'EventSourceMappings[?EventSourceArn==`'"$QUEUE_ARN"'`]' --output text | grep -q .; then
  awslocal lambda create-event-source-mapping \
    --function-name consumer \
    --event-source-arn "$QUEUE_ARN" \
    --batch-size 5 >/dev/null
fi

############################################
# 7) Run crawlers (uploads .gz under providers/...)
############################################
log "Running crawlers..."
python3 crawlers/RunAllCrawlers.py || true

############################################
# 7.5) Optionally re-trigger existing .gz files (ObjectCreated)
############################################
if [ "$REPLAY_EXISTING" = "1" ]; then
  log "Replaying existing .gz to fire S3 events again"
  awslocal s3 ls "s3://$BUCKET/providers/" --recursive | awk '/\.gz$/ {print $4}' \
  | while read -r key; do
      new="${key%.gz}_replay_$(date +%s).gz"
      awslocal s3 cp "s3://$BUCKET/$key" "s3://$BUCKET/$new" >/dev/null
      echo "Replayed $key -> $new"
    done
fi

############################################
# 8) Quick post-check: wait a bit for JSON, or print SQS stats
############################################
log "Waiting up to 30s for Json/ output (consumer)..."
for i in $(seq 1 30); do
  if awslocal s3 ls "s3://$BUCKET/Json/" --recursive | grep -q .; then
    echo "✔ JSON found under s3://$BUCKET/Json/"
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    echo "No JSON yet. Printing SQS queue stats for debugging:"
    awslocal sqs get-queue-attributes \
      --queue-url "$QUEUE_URL" \
      --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateNumberOfMessagesDelayed \
      --output table || true
    echo "Tip: If messages are >0 and not decreasing, consumer code may not be writing JSON. If 0, extractor may not be sending."
  fi
done

############################################
# 9) Logging options (choose mode)
############################################
if [ "$LOG_MODE" = "cw" ]; then
  awslocal logs create-log-group --log-group-name /aws/lambda/extractor  2>/dev/null || true
  awslocal logs create-log-group --log-group-name /aws/lambda/consumer   2>/dev/null || true

  FIRST_KEY=$(awslocal s3 ls "s3://$BUCKET/providers/" --recursive | awk '/\.gz$/ {print $4; exit}')
  if [ -n "${FIRST_KEY:-}" ]; then
    cat > /tmp/s3_event.json <<EOF
{"Records":[{"s3":{"bucket":{"name":"$BUCKET"},"object":{"key":"$FIRST_KEY"}}}]}
EOF
    awslocal lambda invoke --function-name extractor --payload fileb:///tmp/s3_event.json /tmp/out.json >/dev/null 2>&1 || true
  fi

  log "Tailing CloudWatch logs (Ctrl+C to stop)..."
  awslocal logs tail /aws/lambda/extractor --follow &
  awslocal logs tail /aws/lambda/consumer  --follow

elif [ "$LOG_MODE" = "container" ]; then
  log "Showing LocalStack container logs (Ctrl+C to stop). This includes Lambda stdout/stderr."
  docker logs -f s3-simulator

else
  log "LOG_MODE=none — not tailing any logs. Pipeline is deployed and live."
  echo "Tip: set LOG_MODE=container (default) or LOG_MODE=cw to see runtime logs."
fi
