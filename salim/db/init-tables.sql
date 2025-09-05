BEGIN;

-- 1) Supermarkets 
CREATE TABLE IF NOT EXISTS supermarkets (
  chain_id   TEXT PRIMARY KEY,
  chain_name TEXT NOT NULL
);

-- 2) Stores
CREATE TABLE IF NOT EXISTS stores (
  store_id   TEXT PRIMARY KEY,
  store_name TEXT NOT NULL,
  address    TEXT,
  city       TEXT
);

-- 3) Items 
CREATE TABLE IF NOT EXISTS items_current (
  chain_id      TEXT NOT NULL REFERENCES supermarkets(chain_id),
  store_id      TEXT NOT NULL REFERENCES stores(store_id),
  code          TEXT NOT NULL,

  -- Identity / enrichment
  name          TEXT,
  brand         TEXT,
  unit          TEXT,
  qty           NUMERIC(12,3),
  unit_price    NUMERIC(12,4),

  -- Latest regular price
  regular_price NUMERIC(12,2),

  -- Latest promo
  promo_price   NUMERIC(12,2),
  promo_start   TIMESTAMPTZ,
  promo_end     TIMESTAMPTZ,

  PRIMARY KEY (chain_id, store_id, code)
);

COMMIT;
