-- =========================================
-- Extensions
-- =========================================
CREATE EXTENSION IF NOT EXISTS citext;

-- =========================================
-- Products
-- =========================================
CREATE TABLE IF NOT EXISTS public.products (
    product_id   SERIAL PRIMARY KEY,
    barcode      TEXT UNIQUE,
    product_name CITEXT NOT NULL,                 -- normalized name (no brand)
    brand_name   CITEXT NOT NULL DEFAULT 'Unknown',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Prevent duplicates for products with NO barcode (fallback identity)
CREATE UNIQUE INDEX IF NOT EXISTS ux_products_name_brand_when_no_barcode
  ON public.products (product_name, brand_name)
  WHERE barcode IS NULL;

CREATE INDEX IF NOT EXISTS idx_products_name  ON public.products (product_name);
CREATE INDEX IF NOT EXISTS idx_products_brand ON public.products (brand_name);

-- =========================================
-- Branches (Supermarkets)
-- =========================================
CREATE TABLE IF NOT EXISTS public.branches (
    branch_id  SERIAL PRIMARY KEY,
    name       CITEXT NOT NULL,
    address    CITEXT NOT NULL DEFAULT 'Unknown',
    city       CITEXT NOT NULL DEFAULT 'Unknown',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Full uniqueness to support ON CONFLICT (name, city, address)
    CONSTRAINT uq_branches_name_city_address UNIQUE (name, city, address)
);

CREATE INDEX IF NOT EXISTS idx_branches_city ON public.branches (city);

-- =========================================
-- Prices (fact)
-- =========================================
CREATE TABLE IF NOT EXISTS public.prices (
    price_id       SERIAL PRIMARY KEY,
    product_id     INT NOT NULL REFERENCES public.products(product_id) ON DELETE CASCADE,
    branch_id      INT NOT NULL REFERENCES public.branches(branch_id) ON DELETE CASCADE,
    price          NUMERIC(12,4) NOT NULL CHECK (price > 0),
    discount_price NUMERIC(12,4) CHECK (discount_price IS NULL OR discount_price >= 0),
    ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
    final_price    NUMERIC(12,4) GENERATED ALWAYS AS (COALESCE(discount_price, price)) STORED,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_price UNIQUE (product_id, branch_id, ts),
    CONSTRAINT chk_discount_le_price CHECK (discount_price IS NULL OR discount_price <= price)
);

-- Helpful composite and time indexes
CREATE INDEX IF NOT EXISTS idx_prices_product_branch_ts
  ON public.prices (product_id, branch_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_prices_ts ON public.prices (ts);

-- =========================================
-- updated_at trigger function
-- =========================================
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

-- =========================================
-- Triggers
-- =========================================
DROP TRIGGER IF EXISTS trg_products_updated_at ON public.products;
CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON public.products
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_branches_updated_at ON public.branches;
CREATE TRIGGER trg_branches_updated_at
BEFORE UPDATE ON public.branches
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
