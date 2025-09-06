
CREATE EXTENSION IF NOT EXISTS citext;


CREATE TABLE IF NOT EXISTS public.products (
    product_id   SERIAL PRIMARY KEY,
    barcode      TEXT UNIQUE,
    product_name CITEXT NOT NULL,                
    brand_name   CITEXT NOT NULL DEFAULT 'Unknown',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE UNIQUE INDEX IF NOT EXISTS ux_products_name_brand_when_no_barcode
  ON public.products (product_name, brand_name)
  WHERE barcode IS NULL;

CREATE INDEX IF NOT EXISTS idx_products_name  ON public.products (product_name);
CREATE INDEX IF NOT EXISTS idx_products_brand ON public.products (brand_name);


CREATE TABLE IF NOT EXISTS public.branches (
    branch_id  SERIAL PRIMARY KEY,
    provider   CITEXT NOT NULL DEFAULT 'Unknown',
    name       CITEXT NOT NULL,
    address    CITEXT NOT NULL DEFAULT 'Unknown',
    city       CITEXT NOT NULL DEFAULT 'Unknown',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_branches_provider_name_city_address UNIQUE (provider, name, city, address)
);


ALTER TABLE public.branches
  ADD COLUMN IF NOT EXISTS provider CITEXT;

ALTER TABLE public.branches
  ALTER COLUMN provider SET DEFAULT 'Unknown';

UPDATE public.branches
  SET provider = 'Unknown'
  WHERE provider IS NULL;

ALTER TABLE public.branches
  ALTER COLUMN provider SET NOT NULL;


DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_branches_name_city_address'
      AND conrelid = 'public.branches'::regclass
  ) THEN
    ALTER TABLE public.branches
      DROP CONSTRAINT uq_branches_name_city_address;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_branches_provider_name_city_address'
      AND conrelid = 'public.branches'::regclass
  ) THEN
    ALTER TABLE public.branches
      ADD CONSTRAINT uq_branches_provider_name_city_address
      UNIQUE (provider, name, city, address);
  END IF;
END $$;


CREATE INDEX IF NOT EXISTS idx_branches_city           ON public.branches (city);
CREATE INDEX IF NOT EXISTS idx_branches_provider       ON public.branches (provider);
CREATE INDEX IF NOT EXISTS idx_branches_provider_city  ON public.branches (provider, city);


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


CREATE INDEX IF NOT EXISTS idx_prices_product_branch_ts
  ON public.prices (product_id, branch_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_prices_ts ON public.prices (ts);

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_products_updated_at ON public.products;
CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON public.products
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_branches_updated_at ON public.branches;
CREATE TRIGGER trg_branches_updated_at
BEFORE UPDATE ON public.branches
FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
