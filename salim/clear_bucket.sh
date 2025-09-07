set -Eeuo pipefail

log(){ printf "\033[1;36m[INFO]\033[0m %s\n" "$*"; }
err(){ printf "\033[1;31m[ERR]\033[0m %s\n" "$*" >&2; }

BUCKET="${1:-${S3_BUCKET:-test-bucket}}"

if [[ -n "${AWS:-}" ]]; then
  AWS_BIN="$AWS"
elif [[ -x "./.venv/bin/aws" ]]; then
  AWS_BIN="./.venv/bin/aws --endpoint-url ${AWS_ENDPOINT:-http://localhost:4566} --region ${AWS_REGION:-us-east-1}"
else
  AWS_BIN="aws --endpoint-url ${AWS_ENDPOINT:-http://localhost:4566} --region ${AWS_REGION:-us-east-1}"
fi

log "Bucket: s3://${BUCKET}"
if ! $AWS_BIN s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
  err "Bucket does not exist or not accessible"
  exit 1
fi

log "Deleting current objects"
$AWS_BIN s3 rm "s3://${BUCKET}" --recursive >/dev/null 2>&1 || true

ver_status="$($AWS_BIN s3api get-bucket-versioning --bucket "$BUCKET" --query 'Status' --output text 2>/dev/null || echo "None")"
if [[ "$ver_status" == "Enabled" || "$ver_status" == "Suspended" ]]; then
  log "Versioning detected: $ver_status"
  while :; do
    COUNT="$($AWS_BIN s3api list-object-versions --bucket "$BUCKET" --query 'length(Versions) + length(DeleteMarkers)' --output text 2>/dev/null || echo 0)"
    [[ "$COUNT" == "None" ]] && COUNT=0
    if (( COUNT == 0 )); then
      break
    fi
    log "Deleting batch of versions: $COUNT remaining"
    TMP="$(mktemp)"
    $AWS_BIN s3api list-object-versions --bucket "$BUCKET" \
      --query '{Objects: ((Versions || `[]`)[].{Key:Key,VersionId:VersionId}) + ((DeleteMarkers || `[]`)[].{Key:Key,VersionId:VersionId}), Quiet: `true`}' \
      --output json > "$TMP"
    if grep -q '"Objects": \[\]' "$TMP"; then
      rm -f "$TMP"
      break
    fi
    $AWS_BIN s3api delete-objects --bucket "$BUCKET" --delete "file://$TMP" >/dev/null 2>&1 || true
    rm -f "$TMP"
  done
fi

log "Bucket is empty"