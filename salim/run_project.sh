set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

S3_ENDPOINT_HOST="http://localhost:4566"
SQS_ENDPOINT_HOST="http://localhost:4566"
DDB_ENDPOINT_HOST="http://localstack:4566"

BUCKET="${S3_BUCKET:-test-bucket}"
QUEUE="${OUT_QUEUE:-price-events}"
DLQ_QUEUE="${DLQ_QUEUE:-price-events-dlq}"
DDB_TABLE="${DDB_TABLE:-last_run}"
LAMBDA_NAME="${LAMBDA_NAME:-s3-extractor}"
LAMBDA_RUNTIME="python3.11"
LAMBDA_ROLE_ARN="arn:aws:iam::000000000000:role/lambda-role"
LAMBDA_HANDLER="handler_s3.lambda_handler"
LAMBDA_ZIP="${PROJECT_ROOT}/lambda/function.zip"

L_ENV_OUT_DIR="${EXTRACTOR_OUT_DIR:-/tmp/extractor_data}"
L_ENV_MANIFEST="${EXTRACTOR_MANIFEST:-/tmp/manifest/manifest.json}"
L_ENV_S3_ENDPOINT="http://localstack:4566"
L_ENV_SQS_ENDPOINT="http://localstack:4566"
L_ENV_QUEUE="${QUEUE}"
L_ENV_MAX_PART="${MAX_ITEMS_PER_MESSAGE:-250}"
L_ENV_REGION="${AWS_DEFAULT_REGION}"
L_ENV_SAVE_LOCAL_JSON="${SAVE_LOCAL_JSON:-0}"
L_ENV_UPLOAD_RESULTS_TO_S3="${UPLOAD_RESULTS_TO_S3:-0}"
L_ENV_DEBUG="${EXTRACTOR_DEBUG:-1}"

SEED=0
REBUILD=0
for arg in "$@"; do
  case "$arg" in
    --seed) SEED=1 ;;
    --rebuild) REBUILD=1 ;;
  esac
done

log() { printf "\033[1;36m[%s]\033[0m %s\n" "$(date +%H:%M:%S)" "$*"; }
err() { printf "\033[1;31m[ERR]\033[0m %s\n" "$*" >&2; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || { err "Missing command: $1"; exit 1; }; }
require_cmd docker
require_cmd python3
require_cmd curl

AWS() { "${PROJECT_ROOT}/.venv/bin/aws" --endpoint-url "${S3_ENDPOINT_HOST}" "$@"; }

wait_http_ok() {
  local url="$1" tries="${2:-90}" sleep_s="${3:-2}"
  for ((i=1;i<=tries;i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    sleep "$sleep_s"
  done
  return 1
}

wait_lambda_active() {
  local name="$1" tries="${2:-90}"
  for ((i=1;i<=tries;i++)); do
    state="$(AWS lambda get-function-configuration --function-name "$name" --query 'State' --output text 2>/dev/null || true)"
    [[ "$state" == "Active" ]] && return 0
    sleep 1
  done
  return 1
}

retry() {
  local n=0 max=5 delay=2
  while true; do
    "$@" && return 0 || {
      n=$((n+1))
      [[ $n -ge $max ]] && return 1
      sleep "$delay"
      delay=$((delay*2))
    }
  done
}

log "Starting docker compose..."
if [[ "${REBUILD}" -eq 1 ]]; then
  docker compose -f "${COMPOSE_FILE}" up -d --build --force-recreate
else
  docker compose -f "${COMPOSE_FILE}" up -d
fi

log "Waiting for LocalStack health..."
wait_http_ok "${S3_ENDPOINT_HOST}/_localstack/health" 90 2 || { err "LocalStack is not responding"; exit 1; }
log "LocalStack is healthy"

log "Preparing Python venv..."
cd "${PROJECT_ROOT}"
if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
  log "Created venv: .venv"
fi

source ".venv/bin/activate"
pip install --upgrade pip >/dev/null
pip install -r requirements.txt >/dev/null || true
pip install awscli boto3 >/dev/null
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION
log "AWS CLI: $(./.venv/bin/aws --version)"

log "Ensuring S3 bucket: ${BUCKET}"
AWS s3api head-bucket --bucket "${BUCKET}" >/dev/null 2>&1 || AWS s3 mb "s3://${BUCKET}"

log "Ensuring SQS queues..."
AWS sqs get-queue-url --queue-name "${QUEUE}" >/dev/null 2>&1 || AWS sqs create-queue --queue-name "${QUEUE}" >/dev/null
AWS sqs get-queue-url --queue-name "${DLQ_QUEUE}" >/dev/null 2>&1 || AWS sqs create-queue --queue-name "${DLQ_QUEUE}" >/dev/null
QURL="$(AWS sqs get-queue-url --queue-name "${QUEUE}" --query 'QueueUrl' --output text)"
DLQURL="$(AWS sqs get-queue-url --queue-name "${DLQ_QUEUE}" --query 'QueueUrl' --output text)"
log "SQS URL: ${QURL}"
log "DLQ URL: ${DLQURL}"

log "Ensuring DynamoDB table: ${DDB_TABLE}"
if ! AWS dynamodb describe-table --table-name "${DDB_TABLE}" >/dev/null 2>&1; then
  AWS dynamodb create-table \
    --table-name "${DDB_TABLE}" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
  for i in {1..30}; do
    STATUS="$(AWS dynamodb describe-table --table-name "${DDB_TABLE}" --query 'Table.TableStatus' --output text || true)"
    [[ "${STATUS}" == "ACTIVE" ]] && break
    sleep 1
  done
  log "DynamoDB status: ${STATUS}"
fi

log "Packaging Lambda zip..."
python3 - <<PY
import os, zipfile
root = r"${PROJECT_ROOT}"
zip_path = os.path.join(root, "lambda", "function.zip")
z = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)
z.write(os.path.join(root, "lambda", "handler_s3.py"), "handler_s3.py")
ext_root = os.path.join(root, "extractor")
for base, _, files in os.walk(ext_root):
    for f in files:
        if f.endswith((".py", ".json", ".xml", ".txt")):
            fp = os.path.join(base, f)
            arc = os.path.join("extractor", os.path.relpath(fp, ext_root))
            z.write(fp, arc)
z.close()
print("OK")
PY

log "Deploying Lambda: ${LAMBDA_NAME}"
if AWS lambda get-function --function-name "${LAMBDA_NAME}" >/dev/null 2>&1; then
  AWS lambda update-function-code --function-name "${LAMBDA_NAME}" --zip-file "fileb://${LAMBDA_ZIP}" >/dev/null
else
  AWS lambda create-function \
    --function-name "${LAMBDA_NAME}" \
    --runtime "${LAMBDA_RUNTIME}" \
    --role "${LAMBDA_ROLE_ARN}" \
    --handler "${LAMBDA_HANDLER}" \
    --zip-file "fileb://${LAMBDA_ZIP}" \
    --timeout 120 --memory-size 512 >/dev/null
fi

log "Waiting Lambda to be Active before config update..."
wait_lambda_active "${LAMBDA_NAME}" || { err "Lambda is not Active"; exit 1; }

log "Updating Lambda environment..."
retry AWS lambda update-function-configuration \
  --function-name "${LAMBDA_NAME}" \
  --environment "Variables={
    EXTRACTOR_OUT_DIR=${L_ENV_OUT_DIR},
    EXTRACTOR_MANIFEST=${L_ENV_MANIFEST},
    S3_ENDPOINT=${L_ENV_S3_ENDPOINT},
    SQS_ENDPOINT=${L_ENV_SQS_ENDPOINT},
    OUT_QUEUE=${L_ENV_QUEUE},
    MAX_ITEMS_PER_MESSAGE=${L_ENV_MAX_PART},
    AWS_DEFAULT_REGION=${L_ENV_REGION},
    SAVE_LOCAL_JSON=${L_ENV_SAVE_LOCAL_JSON},
    UPLOAD_RESULTS_TO_S3=${L_ENV_UPLOAD_RESULTS_TO_S3},
    EXTRACTOR_DEBUG=${L_ENV_DEBUG},
    DDB_ENDPOINT=${DDB_ENDPOINT_HOST},
    DDB_TABLE=${DDB_TABLE}
  }"

log "Waiting Lambda to be Active after config update..."
wait_lambda_active "${LAMBDA_NAME}" || { err "Lambda did not become Active"; exit 1; }

log "Waiting for Lambda to be Active"
for i in {1..60}; do
  STATE="$(AWS lambda get-function-configuration --function-name "${LAMBDA_NAME}" --query 'State' --output text || true)"
  [[ "${STATE}" == "Active" ]] && break
  sleep 1
done
log "Lambda state: ${STATE}"

log "Granting S3 invoke permission..."
AWS lambda add-permission \
  --function-name "${LAMBDA_NAME}" \
  --statement-id s3invoke \
  --action "lambda:InvokeFunction" \
  --principal "s3.amazonaws.com" \
  --source-arn "arn:aws:s3:::${BUCKET}" \
  --source-account "000000000000" >/dev/null 2>&1 || true

log "Configuring S3 notifications..."
AWS s3api put-bucket-notification-configuration \
  --bucket "${BUCKET}" \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [{
      \"Id\": \"gz-trigger\",
      \"LambdaFunctionArn\": \"arn:aws:lambda:${AWS_DEFAULT_REGION}:000000000000:function:${LAMBDA_NAME}\",
      \"Events\": [\"s3:ObjectCreated:*\"],
      \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\":\"suffix\",\"Value\":\".gz\"}]}}
    }]
  }" >/dev/null

log "Waiting for PostgreSQL..."
DB_CID="$(docker compose -f "${COMPOSE_FILE}" ps -q db || true)"
for i in {1..60}; do
  hs="$(docker inspect -f '{{.State.Health.Status}}' "${DB_CID}" 2>/dev/null || echo starting)"
  [[ "$hs" == "healthy" ]] && break
  sleep 1
done
log "PostgreSQL is healthy"

log "Applying DB schema..."
docker exec -i "${DB_CID}" psql -U postgres -d salim_db <<'SQL'
CREATE TABLE IF NOT EXISTS ingested_message (
  message_id TEXT PRIMARY KEY,
  provider   TEXT,
  branch_code TEXT,
  type       TEXT,
  ts_doc     timestamptz,
  body       jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS price_item (
  provider     TEXT NOT NULL,
  branch_code  TEXT NOT NULL,
  product_code TEXT NOT NULL,
  product_name TEXT NOT NULL,
  unit         TEXT,
  price        NUMERIC(10,2) NOT NULL,
  ts           timestamptz   NOT NULL,
  PRIMARY KEY (provider, branch_code, product_code, ts)
);

CREATE TABLE IF NOT EXISTS promo_item (
  provider     TEXT NOT NULL,
  branch_code  TEXT NOT NULL,
  product_code TEXT NOT NULL,
  description  TEXT,
  start_ts     timestamptz,
  end_ts       timestamptz,
  price        NUMERIC(10,2),
  rate         NUMERIC(10,4),
  quantity     INT,
  ts_ingested  timestamptz DEFAULT now(),
  PRIMARY KEY (provider, branch_code, product_code, description, start_ts, end_ts)
);

CREATE TABLE IF NOT EXISTS supermarket (
  supermarket_id SERIAL PRIMARY KEY,
  name         TEXT,
  branch_name  TEXT,
  city         TEXT,
  address      TEXT,
  website      TEXT,
  provider     TEXT NOT NULL,
  branch_code  TEXT NOT NULL,
  created_at   timestamptz DEFAULT now(),
  UNIQUE(provider, branch_code)
);

CREATE OR REPLACE VIEW v_latest_price AS
SELECT p.provider, p.branch_code, p.product_code, p.product_name, p.unit, p.price, p.ts
FROM (
  SELECT pi.*,
         row_number() OVER (PARTITION BY pi.provider, pi.branch_code, pi.product_code ORDER BY pi.ts DESC) AS rn
  FROM price_item pi
) p
WHERE p.rn = 1;

INSERT INTO supermarket(name, branch_name, website, provider, branch_code)
VALUES ('Yohananof','יוחננוף מפוח','https://www.yochananof.co.il/','Yohananof','001')
ON CONFLICT (provider, branch_code) DO NOTHING;
SQL

log "Starting consumer..."
docker compose -f "${COMPOSE_FILE}" up -d price-consumer

log "Waiting for consumer metrics..."
wait_http_ok "http://localhost:9108/metrics" 60 1 || true

if [[ "${SEED}" -eq 1 ]]; then
  log "Seeding a sample PriceFull to S3..."
  TS="$(date -u +"%Y-%m-%d_%H-%M")"
  TMP_XML="/tmp/PriceFull_${TS}.xml"
  cat > "${TMP_XML}" <<'XML'
<Root>
  <Items Count="1">
    <Item>
      <ItemCode>1234567890123</ItemCode>
      <ItemName>Test Item</ItemName>
      <ItemPrice>9.99</ItemPrice>
      <UnitOfMeasure>unit</UnitOfMeasure>
    </Item>
  </Items>
</Root>
XML
  gzip -c "${TMP_XML}" > "/tmp/PriceFull_${TS}.gz"
  AWS s3 cp "/tmp/PriceFull_${TS}.gz" "s3://${BUCKET}/Yohananof/001/PriceFull_${TS}.gz" >/dev/null
  log "Seed uploaded: Yohananof/001/PriceFull_${TS}.gz"
fi

log "[Success] Project is up"