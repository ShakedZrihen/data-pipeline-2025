BEGIN;

CREATE TABLE IF NOT EXISTS supermarkets (
  chain_id   TEXT PRIMARY KEY,
  chain_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stores (
  store_id   TEXT NOT NULL UNIQUE,
  chain_id   TEXT NOT NULL REFERENCES supermarkets(chain_id),
  store_name TEXT NOT NULL,
  address    TEXT,
  city       TEXT,


  PRIMARY KEY (chain_id, store_id)
);

CREATE TABLE IF NOT EXISTS items (
  chain_id      TEXT NOT NULL REFERENCES supermarkets(chain_id),
  store_id      TEXT NOT NULL REFERENCES stores(store_id),
  code          TEXT NOT NULL,

  name          TEXT,
  brand         TEXT,
  unit          TEXT,
  qty           NUMERIC(12,3),
  unit_price    NUMERIC(12,4),

  regular_price NUMERIC(12,2),

  promo_price   NUMERIC(12,2),
  promo_start   TIMESTAMPTZ,
  promo_end     TIMESTAMPTZ,

  PRIMARY KEY (chain_id, store_id, code)
);

COMMIT;
