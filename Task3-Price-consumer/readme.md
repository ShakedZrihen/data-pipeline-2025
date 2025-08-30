# Price Consumer — Queue → Normalize/Validate → PostgreSQL (idempotent)

## What it does
- Reads messages from SQS (configurable provider),
- normalizes/enriches them,
- validates schema,
- upserts into Postgres idempotently (PK: provider, branch, doc_type, ts, product),
- invalid messages go to DLQ with context.
- Emits logs (CloudWatch) + metrics hooks.

## Deploy
- Package `consumer.py`, `db.py`, `normalizer.py`, `validator.py`, `metrics.py`, and `migrations/`.
- Set Lambda env vars:
  - `DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT=5432, DB_SSLMODE=require`
  - `DLQ_URL`
  - `AWS_REGION=eu-central-1`

## First-time DB setup
- Either run `migrations/001_create_price_items.sql` manually,
- **or** invoke Lambda test with:
  ```json
  {"action":"setup"}

Manual test message (SQS → Send)
{
  "provider": "yohananof",
  "branch": "main",
  "type": "promoFull",
  "timestamp": "2025-08-12T20:29:15Z",
  "items": [
    {"product": "Example A", "price": 12.0, "unit": "unit"},
    {"product": "Example B", "price": 9.9, "unit": "unit"}
  ]
}

## dev
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill values
python consumer.py migrate           # apply migrations locally (if DB reachable)
python consumer.py consume-file sample.json
