Crawler
=======

Overview
--------

This crawler image now runs on‑demand (no cron inside the container). Trigger it when you want a single run and avoid surprise hourly costs.

Quick Start (Docker)
--------------------

1. Build the image:

   ```bash
   docker build -t salim-crawler ./crawler
   ```

2. Run one-off with required AWS creds and bucket (uses `crawler/config.json`):

   ```bash
   docker run --rm \
     -e AWS_ACCESS_KEY_ID=... \
     -e AWS_SECRET_ACCESS_KEY=... \
     -e AWS_REGION=il-central-1 \
     -e S3_BUCKET=salim-prices \
     -v $(pwd)/crawler/config.json:/app/config.json \
     -v crawler_downloads:/app/downloads \
     salim-crawler python crawler.py ${S3_BUCKET:-salim-prices} --config config.json
   ```

Quick Start (docker-compose)
----------------------------

- The `crawler` service in `docker-compose.yml` is configured to run on-demand and not restart automatically.

  ```bash
  docker compose build crawler
  docker compose run --rm crawler
  ```

Scheduling
----------

There is no cron inside the image. To run on a cadence, trigger the container externally (e.g., GitHub Actions, GitLab CI, AWS ECS Scheduled Task, or a host-level cron) using the same command shown above.

TLS handshake issues
--------------------

Some supermarket portals only support legacy TLS/ciphers. If direct downloads fail with an SSL handshake error, enable a compatibility mode for the internal `requests` session:

- Set environment variable `CRAWLER_TLS_COMPAT=1` when running the crawler (or in Docker ENV).

This mounts a custom HTTPS adapter that:
- Lowers the minimum TLS version if needed (allows TLS 1.0/1.1).
- Reduces OpenSSL security level to permit older ciphers.
- Keeps certificate verification disabled in line with the existing session configuration.

By default this mode is OFF to keep secure defaults. Only enable it for endpoints that require legacy TLS.

TLS verification control
------------------------

Control certificate verification via `CRAWLER_SSL_VERIFY`:
- `0` or `false` (default): disable verification.
- `1` or `true`: enable verification using system CAs.
- Absolute/relative path: use a custom CA bundle file.
