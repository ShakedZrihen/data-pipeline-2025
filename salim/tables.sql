-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE supermarkets (
  supermarket_id   SERIAL PRIMARY KEY,
  name             TEXT NOT NULL,
  branch_name      TEXT,
  city             TEXT,
  address          TEXT,
  website          TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
  product_id       BIGSERIAL PRIMARY KEY,
  supermarket_id   INT NOT NULL REFERENCES supermarkets(supermarket_id) ON DELETE CASCADE,

  barcode          TEXT NOT NULL,         

  canonical_name   TEXT NOT NULL,
  brand            TEXT,
  category         TEXT,
  size_value       NUMERIC(12,3),
  size_unit        TEXT,                   -- 'kg','l','unit'...


  price            NUMERIC(12,2) NOT NULL,
  currency         CHAR(3) NOT NULL DEFAULT 'ILS',
  list_price       NUMERIC(12,2),
  promo_price      NUMERIC(12,2),
  promo_text       TEXT,
  loyalty_only     BOOLEAN DEFAULT FALSE,
  in_stock         BOOLEAN,

  collected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

  source           TEXT,
  raw_hash         TEXT,

  UNIQUE (supermarket_id, barcode, collected_at)
);

-- indexes
CREATE UNIQUE INDEX supermarkets_name_branch_idx ON supermarkets (LOWER(name), COALESCE(LOWER(branch_name), ''));
CREATE INDEX products_supermarket_idx  ON products (supermarket_id);
CREATE INDEX products_barcode_idx      ON products (barcode);
CREATE INDEX products_time_idx         ON products (collected_at DESC);
CREATE INDEX products_name_trgm_idx    ON products USING GIN (canonical_name gin_trgm_ops);
