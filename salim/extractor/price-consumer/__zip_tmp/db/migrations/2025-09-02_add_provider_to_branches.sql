BEGIN;
ALTER TABLE public.branches
  ADD COLUMN IF NOT EXISTS provider CITEXT NOT NULL DEFAULT 'Unknown';

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

CREATE INDEX IF NOT EXISTS idx_branches_provider       ON public.branches (provider);
CREATE INDEX IF NOT EXISTS idx_branches_provider_city  ON public.branches (provider, city);
CREATE INDEX IF NOT EXISTS idx_branches_city           ON public.branches (city);
COMMIT;