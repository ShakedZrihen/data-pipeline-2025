ALTER TABLE public.price_items
  ADD COLUMN IF NOT EXISTS barcode      text,
  ADD COLUMN IF NOT EXISTS canonical_name text,
  ADD COLUMN IF NOT EXISTS brand         text,
  ADD COLUMN IF NOT EXISTS category      text,
  ADD COLUMN IF NOT EXISTS size_value    numeric,
  ADD COLUMN IF NOT EXISTS size_unit     text,
  ADD COLUMN IF NOT EXISTS promo_price   numeric,
  ADD COLUMN IF NOT EXISTS promo_text    text,
  ADD COLUMN IF NOT EXISTS in_stock      boolean;

CREATE INDEX IF NOT EXISTS price_items_barcode_idx
  ON public.price_items (barcode);
