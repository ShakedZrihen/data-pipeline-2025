create schema if not exists prices;

create table if not exists prices.items (
  id bigserial primary key,
  item_uid text not null,
  provider text not null,
  branch text not null,
  type text not null,
  ts_utc timestamptz not null,
  product text not null,
  unit text,
  price numeric,
  source_batch_ts timestamptz not null default now(),
  unique (item_uid)
);

create index if not exists idx_prices_items_provider_ts on prices.items(provider, ts_utc);
create index if not exists idx_prices_items_branch on prices.items(branch);
create index if not exists idx_prices_items_product on prices.items using gin (product gin_trgm_ops);