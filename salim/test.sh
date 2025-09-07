set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="${1:-$ROOT/test}"
BUCKET="${S3_BUCKET:-test-bucket}"

AWS_BIN="$ROOT/.venv/bin/aws"
AWS="$AWS_BIN --endpoint-url http://localhost:4566 --region us-east-1"


export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "[INFO] Source: $SRC_DIR"
echo "[INFO] Bucket: $BUCKET"


$AWS s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 || {
  echo "[INFO] Creating bucket $BUCKET"
  $AWS s3 mb "s3://$BUCKET"
}

echo "[INFO] Uploading *.gz to s3://$BUCKET/"
$AWS s3 cp "$SRC_DIR" "s3://$BUCKET/" --recursive --exclude "*" --include "*.gz"

echo "[INFO] Done. Listing objects:"
$AWS s3 ls "s3://$BUCKET/" --recursive