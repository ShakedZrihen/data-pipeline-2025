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