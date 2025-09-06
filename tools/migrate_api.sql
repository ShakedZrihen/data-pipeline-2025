
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS supermarkets (
  supermarket_id   INTEGER PRIMARY KEY,
  provider         TEXT NOT NULL,
  branch           TEXT NOT NULL,
  name             TEXT,
  branch_name      TEXT,
  city             TEXT,
  address          TEXT,
  website          TEXT,
  created_at       TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_supermarkets_provider_branch
  ON supermarkets(provider, branch);

INSERT OR IGNORE INTO supermarkets (provider, branch, name)
SELECT DISTINCT m.provider, m.branch, m.provider
FROM messages m;

DROP VIEW IF EXISTS products_vw;

CREATE VIEW products_vw AS
SELECT
  s.supermarket_id,
  s.provider,
  s.branch,
  ei.rowid              AS product_id,
  COALESCE(ei.barcode, NULL) AS barcode,
  ei.product_name       AS canonical_name,
  COALESCE(ei.brand, NULL)   AS brand,
  COALESCE(ei.category, NULL) AS category,
  CAST(ei.qty_std AS REAL)    AS size_value,
  ei.unit_std                 AS size_unit,
  ei.price                    AS price,
  'ILS'                       AS currency,
  NULL                        AS promo_price,
  NULL                        AS promo_text,
  1                           AS in_stock,
  m.ts                        AS collected_at
FROM enriched_items ei
JOIN messages m         ON m.id = ei.message_id
JOIN supermarkets s     ON s.provider = m.provider AND s.branch = m.branch;
