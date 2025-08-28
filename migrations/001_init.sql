CREATE TABLE IF NOT EXISTS prices (
  provider  text        NOT NULL,
  branch    text        NOT NULL,
  type      text        NOT NULL,
  ts        timestamptz NOT NULL,
  product   text        NOT NULL,
  price     numeric     NOT NULL CHECK (price > 0),
  unit      text        NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, branch, type, ts, product)
);

CREATE INDEX IF NOT EXISTS idx_prices_ts       ON prices (ts);
CREATE INDEX IF NOT EXISTS idx_prices_provider ON prices (provider);
