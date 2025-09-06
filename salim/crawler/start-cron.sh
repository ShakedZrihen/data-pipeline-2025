#!/bin/sh
set -e

# Write a dynamic crontab with current environment values
CRON_FILE=/etc/cron.d/crawler
cat > "$CRON_FILE" <<EOF
SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
AWS_REGION=${AWS_REGION:-il-central-1}
S3_BUCKET=${S3_BUCKET:-salim-prices}
# Run at minute 0 every hour
0 * * * * root /usr/local/bin/cron_run.sh >> /proc/1/fd/1 2>&1
EOF

chmod 0644 "$CRON_FILE"
crontab "$CRON_FILE"

echo "Starting cron in foreground..."
exec cron -f

