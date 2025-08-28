CREATE TABLE IF NOT EXISTS prices (
  provider text NOT NULL,
  branch   text NOT NULL,
  type     text NOT NULL,
  ts       timestamptz NOT NULL,
  product  text NOT NULL,
  price    numeric,
  unit     text,
  raw      jsonb,
  PRIMARY KEY (provider, branch, type, ts, product)
);

CREATE INDEX IF NOT EXISTS idx_prices_ts ON prices(ts);
