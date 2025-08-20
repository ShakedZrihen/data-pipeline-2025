CREATE TABLE IF NOT EXISTS prices (
  provider   text    NOT NULL,
  branch     text    NOT NULL,
  type       text    NOT NULL,
  ts         timestamptz NOT NULL,
  product    text    NOT NULL,
  price      numeric(12,2),
  unit       text,
  meta       jsonb DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, branch, type, ts, product)
);
CREATE INDEX IF NOT EXISTS idx_prices_ts ON prices (ts);
