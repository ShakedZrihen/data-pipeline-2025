import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv

print("[BOOT] starting json_loader.py", flush=True)

# ---------- env ----------
env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)
PG_DSN = (
    os.getenv("PG_DSN")
    or os.getenv("DATABASE_URL")
    or "postgresql://postgres:postgres@localhost:5432/postgres"
)

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _as_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() == "none":
            return None
        return int(s)
    except Exception:
        return None

def _as_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() == "none":
            return None
        return float(s)
    except Exception:
        return None

def _as_str(x) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def _first(*vals):
    for v in vals:
        v = _as_str(v)
        if v:
            return v
    return None

def _first_list(d: Dict[str, Any], *keys: str) -> Optional[List[Any]]:
    """Find first existing key in d whose value is a list."""
    for k in keys:
        v = d.get(k)
        if isinstance(v, list):
            return v
    return None
class Cache:
    """
    Lightweight cache held by enricher for this process.
    We cache:
      - supers:  (provider, branch_number) -> super_id (uuid)
      - file-scope dedupe: set of (branch_number, barcode)
    """
    def __init__(self):
        self.super_by_key: Dict[Tuple[str, int], str] = {}
        self._seen_products: set[Tuple[int, str]] = set()

    def reset_file_scope(self):
        self._seen_products.clear()

def _execute_values(cur, sql: str, rows: Iterable[tuple]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    extras.execute_values(cur, sql, rows, page_size=1000)
    return len(rows)

def _ensure_super(
    cur,
    cache: Cache,
    provider: Optional[str],
    branch_number: Optional[int],
    branch_name: Optional[str] = None,
    address: Optional[str] = None,
) -> Optional[str]:
    provider = _as_str(provider)
    br = _as_int(branch_number)
    if not provider or br is None or br == 0:
        return None

    key = (provider, br)
    if key in cache.super_by_key:
        return cache.super_by_key[key]

    sql = """
        INSERT INTO supers (provider, branch_number, branch_name, address)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (provider, branch_number)
        DO UPDATE SET
            branch_name = COALESCE(EXCLUDED.branch_name, supers.branch_name),
            address     = COALESCE(EXCLUDED.address, supers.address)
        RETURNING id
    """
    cur.execute(sql, (provider, br, branch_name, address))
    super_id = cur.fetchone()[0]
    cache.super_by_key[key] = super_id
    return super_id

def _extract_prices_payload(d: Dict[str, Any]) -> Tuple[str, Optional[int], Optional[str], Optional[str], List[Dict[str, Any]]]:
    """
    Normalized price JSON:
      { "provider": "...", "branch": <int>, "type": "price", "timestamp": "...", "items": [...] }
    Items: { "itemCode": "<barcode>", "product": "<name>", "price": <float>, "updatedAt": "<iso>" }
    """
    provider = _first(d.get("provider"), d.get("chain"), d.get("brand"))
    branch_number = _as_int(d.get("branch") or d.get("branch_number") or d.get("store_id"))
    branch_name = _first(d.get("branch_name"), d.get("store_name"))
    address = _first(d.get("address"), d.get("store_address"))
    items = _first_list(d, "items", "Items", "products", "data") or []
    return provider or "", branch_number, branch_name, address, items

def _parse_price_item(it: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    barcode = _first(it.get("itemCode"), it.get("barcode"), it.get("Barcode"), it.get("product_id"))
    name    = _first(it.get("product"), it.get("name"), it.get("product_name"))
    price   = _as_float(it.get("price") or it.get("Price") or it.get("storePrice"))
    return barcode, name, price

def _extract_promos_payload(d: Dict[str, Any]) -> Tuple[str, Optional[int], List[Dict[str, Any]]]:
    """
    Normalized promo JSON:
      { "provider": "...", "branch": <int>, "type": "promo", "timestamp": "...", "items": [...] }
    """
    provider = _first(d.get("provider"), d.get("chain"), d.get("brand"))
    branch_number = _as_int(d.get("branch") or d.get("branch_number") or d.get("store_id"))
    promos = _first_list(d, "items", "promotions", "data") or []
    return provider or "", branch_number, promos

def _parse_promo(it: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[float], Optional[float], Optional[float], List[str]]:
    """
    Returns: promotion_id, start_at, end_at, min_qty, discount_rate, discount_price, barcodes[]
    """
    promo_id = _first(it.get("promotionId"), it.get("promotion_id"), it.get("id"))
    start    = _first(it.get("start"), it.get("start_at"), it.get("startDate"))
    end      = _first(it.get("end"), it.get("end_at"), it.get("endDate"))
    min_qty  = _as_float(it.get("minQty") or it.get("min_qty"))
    rate     = _as_float(it.get("discountRate") or it.get("discount_rate"))
    price    = _as_float(it.get("discountedPrice") or it.get("discount_price"))

    prods = []
    v = it.get("products") or it.get("items") or []
    if isinstance(v, list):
        for p in v:
            if isinstance(p, dict):
                bc = _first(p.get("itemCode"), p.get("barcode"), p.get("product_id"))
                if bc:
                    prods.append(bc)
            elif isinstance(p, (str, int)):
                s = _as_str(p)
                if s:
                    prods.append(s)

    prods = list(dict.fromkeys(prods))
    return promo_id, start, end, min_qty, rate, price, prods

def load_prices_file(cur, cache: Cache, path: str) -> int:
    """
    Upsert products for a single JSON file.
    """
    cache.reset_file_scope()

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    provider, branch_number, branch_name, address, items = _extract_prices_payload(data)

    super_id = _ensure_super(cur, cache, provider, branch_number, branch_name, address)

    rows = []
    seen = cache._seen_products
    now = _now_utc()

    br = _as_int(branch_number)
    if br is None or br == 0:
        print(f"[INFO] Message file {os.path.basename(path)}: 0 products parsed", flush=True)
        return 0

    for it in items:
        if not isinstance(it, dict):
            continue
        barcode, name, price = _parse_price_item(it)
        if not barcode:
            continue
        key = (br, barcode)
        if key in seen:
            continue
        seen.add(key)

        rows.append((barcode, br, _as_str(name), _as_float(price), now, super_id))

    if not rows:
        print(f"[INFO] Message file {os.path.basename(path)}: 0 products parsed", flush=True)
        return 0

    sql = """
        INSERT INTO products (barcode, branch_number, name, price, updated_at, super_id)
        VALUES %s
        ON CONFLICT (branch_number, barcode)
        DO UPDATE SET
            name       = COALESCE(EXCLUDED.name, products.name),
            price      = EXCLUDED.price,
            updated_at = EXCLUDED.updated_at,
            super_id   = COALESCE(EXCLUDED.super_id, products.super_id)
    """
    cnt = _execute_values(cur, sql, rows)
    print(f"[RESULT] Upserted products from {os.path.basename(path)}: {cnt}", flush=True)
    return cnt

def load_promos_file(cur, cache: Cache, path: str) -> int:
    """
    Upsert promos + link them to products (by barcode) for a single JSON file.
    Returns the number of links inserted/ensured (not the number of promo rows).
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    provider, branch_number, promos = _extract_promos_payload(data)

    br = _as_int(branch_number)
    if br is None or br == 0:
        print(f"[INFO] Message file {os.path.basename(path)}: 0 promotions parsed", flush=True)
        return 0

    _ensure_super(cur, cache, provider, br)

    promo_rows: List[Tuple[str, Optional[str], Optional[str], Optional[float], Optional[float], Optional[float], int]] = []
    link_pairs: List[Tuple[str, str]] = []  # (promo_id, barcode)

    for it in promos:
        if not isinstance(it, dict):
            continue
        promo_id, start, end, min_qty, rate, price, barcodes = _parse_promo(it)
        if not promo_id:
            continue

        promo_rows.append((promo_id, start, end, min_qty, rate, price, br))
        for bc in barcodes:
            link_pairs.append((promo_id, bc))

    if promo_rows:
        try:
            sql_promo = """
                INSERT INTO promos (promotion_id, start_at, end_at, min_qty, discount_rate, discount_price, branch_id)
                VALUES %s
                ON CONFLICT (promotion_id, branch_id)
                DO UPDATE SET
                    start_at       = COALESCE(EXCLUDED.start_at, promos.start_at),
                    end_at         = COALESCE(EXCLUDED.end_at, promos.end_at),
                    min_qty        = COALESCE(EXCLUDED.min_qty, promos.min_qty),
                    discount_rate  = COALESCE(EXCLUDED.discount_rate, promos.discount_rate),
                    discount_price = COALESCE(EXCLUDED.discount_price, promos.discount_price)
            """
            _execute_values(cur, sql_promo, promo_rows)
        except psycopg2.Error as e:
            for r in promo_rows:
                (promotion_id, start_at, end_at, min_qty, discount_rate, discount_price, branch_id) = r
                cur.execute(
                    """
                    UPDATE promos
                       SET start_at = COALESCE(%s, start_at),
                           end_at = COALESCE(%s, end_at),
                           min_qty = COALESCE(%s, min_qty),
                           discount_rate = COALESCE(%s, discount_rate),
                           discount_price = COALESCE(%s, discount_price)
                     WHERE promotion_id = %s AND branch_id = %s
                    """,
                    (start_at, end_at, min_qty, discount_rate, discount_price, promotion_id, branch_id),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        INSERT INTO promos (promotion_id, start_at, end_at, min_qty, discount_rate, discount_price, branch_id)
                        SELECT %s,%s,%s,%s,%s,%s,%s
                        WHERE NOT EXISTS (
                          SELECT 1 FROM promos WHERE promotion_id = %s AND branch_id = %s
                        )
                        """,
                        (promotion_id, start_at, end_at, min_qty, discount_rate, discount_price, branch_id,
                         promotion_id, branch_id),
                    )

    # Link promos -> products (only for products that already exist in this branch)
    link_cnt = 0
    if link_pairs:
        # dedupe within file
        link_pairs = list({(p, bc) for (p, bc) in link_pairs})

        # keep only barcodes that exist for this branch to avoid FK errors
        bcodes = [bc for _, bc in link_pairs]
        # chunk the IN-list to keep queries reasonable
        existing: set[str] = set()
        CHUNK = 1000
        for i in range(0, len(bcodes), CHUNK):
            chunk = bcodes[i : i + CHUNK]
            cur.execute(
                "SELECT barcode FROM products WHERE branch_number = %s AND barcode = ANY(%s)",
                (br, chunk),
            )
            for (bc,) in cur.fetchall():
                existing.add(bc)

        rows = [(p, bc, br) for (p, bc) in link_pairs if bc in existing]
        if rows:
            sql_link = """
                INSERT INTO promo_to_product (promo_id, barcode, branch_id)
                VALUES %s
                ON CONFLICT (promo_id, barcode, branch_id) DO NOTHING
            """
            link_cnt = _execute_values(cur, sql_link, rows)

    print(f"[RESULT] Inserted promo->product links from {os.path.basename(path)}: {link_cnt}", flush=True)
    return link_cnt
