import os
import ssl
from typing import List, Dict, Any, Optional
import pg8000.dbapi as pg

def _env(name: str, default=None):
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def get_conn():
    host = _env("PGHOST", "postgres")
    db   = _env("PGDATABASE", "pricedb")
    user = _env("PGUSER", "postgres")
    pwd  = _env("PGPASSWORD", "postgres")
    port = int(_env("PGPORT", "5432"))
    sslmode = os.getenv("PGSSLMODE", "").lower()
    ctx: Optional[ssl.SSLContext] = None
    if sslmode in ("require", "verify-ca", "verify-full"):
        ctx = ssl.create_default_context()
    return pg.connect(user=user, password=pwd, host=host, database=db, port=port, ssl_context=ctx)

def run_migration_file(conn, path: str):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    finally:
        cur.close()

def _update_promos(conn, rows: List[Dict[str, Any]]) -> int:
    cur = conn.cursor()
    updated = 0
    for r in rows:
        provider = r["provider"]; branch = r["branch"]
        promo_price = r.get("promo_price") or r.get("price") or 0.0
        promo_text  = r.get("promo_text") or r.get("product") or None
        barcode     = r.get("barcode")
        product     = r.get("product")

        if barcode:
            cur.execute(
                """
                UPDATE public.price_items
                   SET promo_price = %s,
                       promo_text  = %s,
                       updated_at  = NOW()
                 WHERE provider = %s
                   AND branch   = %s
                   AND doc_type = 'pricesFull'
                   AND barcode  = %s
                   AND ts = (
                        SELECT MAX(ts) FROM public.price_items
                         WHERE provider=%s AND branch=%s AND doc_type='pricesFull' AND barcode=%s
                   )
                """,
                (promo_price, promo_text, provider, branch, barcode, provider, branch, barcode),
            )
            updated += cur.rowcount
        else:
            cur.execute(
                """
                UPDATE public.price_items
                   SET promo_price = %s,
                       promo_text  = %s,
                       updated_at  = NOW()
                 WHERE provider = %s
                   AND branch   = %s
                   AND doc_type = 'pricesFull'
                   AND product  = %s
                   AND ts = (
                        SELECT MAX(ts) FROM public.price_items
                         WHERE provider=%s AND branch=%s AND doc_type='pricesFull' AND product=%s
                   )
                """,
                (promo_price, promo_text, provider, branch, product, provider, branch, product),
            )
            updated += cur.rowcount
    cur.close()
    return updated

def upsert_rows(conn, rows: List[Dict[str, Any]]):
    if not rows:
        return 0

    if rows[0].get("doc_type") == "promoFull":
        n = _update_promos(conn, rows)
        conn.commit()
        return n

    cols = [
        "provider","branch","doc_type","ts","product",
        "unit","price","src_key","etag",
        "barcode","canonical_name","brand","category",
        "size_value","size_unit","currency",
        "promo_price","promo_text","in_stock",
    ]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"""
        INSERT INTO public.price_items ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (provider, branch, doc_type, ts, product)
        DO UPDATE SET
          unit           = EXCLUDED.unit,
          price          = EXCLUDED.price,
          src_key        = EXCLUDED.src_key,
          etag           = EXCLUDED.etag,
          barcode        = COALESCE(EXCLUDED.barcode, public.price_items.barcode),
          canonical_name = COALESCE(EXCLUDED.canonical_name, public.price_items.canonical_name),
          brand          = COALESCE(EXCLUDED.brand, public.price_items.brand),
          category       = COALESCE(EXCLUDED.category, public.price_items.category),
          size_value     = COALESCE(EXCLUDED.size_value, public.price_items.size_value),
          size_unit      = COALESCE(EXCLUDED.size_unit, public.price_items.size_unit),
          currency       = COALESCE(EXCLUDED.currency, public.price_items.currency),
          promo_price    = COALESCE(EXCLUDED.promo_price, public.price_items.promo_price),
          promo_text     = COALESCE(EXCLUDED.promo_text, public.price_items.promo_text),
          in_stock       = COALESCE(EXCLUDED.in_stock, public.price_items.in_stock),
          updated_at     = NOW();
    """
    params = [tuple(r.get(k) for k in cols) for r in rows]
    cur = conn.cursor()
    try:
        cur.executemany(sql, params)
    finally:
        cur.close()
    return len(params)
