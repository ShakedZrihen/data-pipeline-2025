-- migrations/001_create_price_items.sql

CREATE TABLE IF NOT EXISTS public.price_items (
  provider   text        NOT NULL,
  branch     text        NOT NULL,
  doc_type   text        NOT NULL,
  ts         timestamptz NOT NULL,
  product    text        NOT NULL,
  unit       text        NOT NULL,
  price      numeric     NOT NULL,
  src_key    text        NULL,
  etag       text        NULL,
  updated_at timestamptz NOT NULL DEFAULT NOW(),
  CONSTRAINT price_items_pk PRIMARY KEY (provider, branch, doc_type, ts, product)
);

CREATE INDEX IF NOT EXISTS price_items_branch_type_ts_idx
  ON public.price_items (provider, branch, doc_type, ts DESC);
