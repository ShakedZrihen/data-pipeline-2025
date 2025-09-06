# consumer/config.py
from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    supabase_db_url: str
    db_pool_size: int
    items_bucket: str | None
    processed_prefix: str
    print_items_limit: int
    log_level: str
    db_schema: str
    table_products: str
    table_branches: str
    table_prices: str

def _req(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def _opt_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _ensure_sslmode(url: str) -> str:
    if "sslmode=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}sslmode=require"

def load_config() -> Config:
    supa = _ensure_sslmode(_req("SUPABASE_DB_URL"))
    return Config(
        supabase_db_url=supa,
        db_pool_size=_opt_int("DB_POOL_SIZE", 8),
        items_bucket=os.getenv("ITEMS_BUCKET"),
        processed_prefix=os.getenv("PROCESSED_PREFIX", "processed"),
        print_items_limit=_opt_int("PRINT_ITEMS_LIMIT", 5),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        db_schema=os.getenv("DB_SCHEMA", "public"),
        table_products=os.getenv("TABLE_PRODUCTS", "products"),
        table_branches=os.getenv("TABLE_BRANCHES", "branches"),
        table_prices=os.getenv("TABLE_PRICES", "prices"),
    )
