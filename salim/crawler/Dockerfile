FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    chromium chromium-driver fonts-liberation unzip curl \
  && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD bash -lc '\
  until curl -sf ${S3_ENDPOINT:-http://localstack:4566}/_localstack/health | grep -Eq "\"(running|available)\""; do \
    echo "[crawler] waiting for LocalStack..." ; sleep 2 ; \
  done ; \
  python init_s3.py || true ; \
  while true; do \
    echo "[crawler] start $(date -u)" ; \
    python crawler.py && python upload_to_s3.py ; \
    echo "[crawler] done  $(date -u)" ; \
    sleep ${CRAWLER_INTERVAL:-3600} ; \
  done'