import os
import uuid
import json
import glob
from datetime import datetime
from decimal import Decimal
import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv

print("[BOOT] starting json_loader.py", flush=True)

# ---------- .env ----------
load_dotenv()

def _ensure_sslmode(dsn: str) -> str:
    if "sslmode=" in dsn:
        return dsn
    sep = "&" if "?" in dsn else "?"
    return f"{dsn}{sep}sslmode=require"

PG_DSN = os.getenv("PG_DSN") or os.getenv("SUPABASE_DB_URL")
if not PG_DSN:
    raise RuntimeError("Environment variable PG_DSN (or SUPABASE_DB_URL) is not set")
PG_DSN = _ensure_sslmode(PG_DSN)

# ---------- helpers ----------
def parse_iso(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def to_int_or_none(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def as_decimal(x):
    return None if x is None else Decimal(str(x))

# ---------- UPSERT statements ----------
UPSERT_SUPER = """
INSERT INTO supers (id, address, provider, branch_number, branch_name)
VALUES (%(id)s, %(address)s, %(provider)s, %(branch_number)s, %(branch_name)s)
ON CONFLICT (provider, branch_number) DO UPDATE
SET address = COALESCE(EXCLUDED.address, supers.address),
    branch_name = COALESCE(EXCLUDED.branch_name, supers.branch_name)
RETURNING id;
"""

UPSERT_PRODUCT = """
INSERT INTO products (id, brand, name, barcode, updatedat, super_id, price)
VALUES (%(id)s, %(brand)s, %(name)s, %(barcode)s, %(updatedat)s, %(super_id)s, %(price)s)
ON CONFLICT (super_id, barcode) DO UPDATE
SET brand = COALESCE(EXCLUDED.brand, products.brand),
    name  = COALESCE(EXCLUDED.name,  products.name),
    price = COALESCE(EXCLUDED.price, products.price),
    updatedat = GREATEST(COALESCE(products.updatedat, 'epoch'::timestamp),
                         COALESCE(EXCLUDED.updatedat, 'epoch'::timestamp))
RETURNING id;
"""

UPSERT_PROMO = """
INSERT INTO promos (start, "end", minQty, discountRate, discountPrice, promotion_id)
VALUES (%(start)s, %(end)s, %(minQty)s, %(discountRate)s, %(discountPrice)s, %(promotion_id)s)
ON CONFLICT ON CONSTRAINT promos_promotion_id_uniq DO UPDATE
SET start = LEAST(COALESCE(promos.start, EXCLUDED.start), EXCLUDED.start),
    "end" = GREATEST(COALESCE(promos."end", EXCLUDED."end"), EXCLUDED."end"),
    minQty = COALESCE(EXCLUDED.minQty, promos.minQty),
    discountRate = COALESCE(EXCLUDED.discountRate, promos.discountRate),
    discountPrice = COALESCE(EXCLUDED.discountPrice, promos.discountPrice)
RETURNING id;
"""

UPSERT_PROMO_LINK = """
INSERT INTO promo_to_product (product_id, promo_id)
VALUES (%(product_id)s, %(promo_id)s)
ON CONFLICT (product_id, promo_id) DO NOTHING;
"""

# ---------- ID cache ----------
class IdCache:
    def __init__(self):
        self.super_by_key = {}        # (provider, branch_number) -> uuid
        self.product_by_key = {}      # (super_id, barcode) -> uuid

    def get_super(self, provider, branch):
        return self.super_by_key.get((provider, branch))

    def put_super(self, provider, branch, sid):
        self.super_by_key[(provider, branch)] = sid

    def get_product(self, super_id, barcode):
        return self.product_by_key.get((super_id, barcode))

    def put_product(self, super_id, barcode, pid):
        self.product_by_key[(super_id, barcode)] = pid

# ---------- logical ops ----------
def upsert_super(cur, cache: IdCache, *, provider: str, branch_number: str | None,
                 address: str | None = None, branch_name: str | None = None):
    key = (provider, branch_number or "")
    cached = cache.get_super(*key)
    if cached:
        print(f"[DEBUG] super found in cache: {provider}, {branch_number} -> {cached}")
        return cached
    sid = str(uuid.uuid4())
    print(f"[DEBUG] inserting new super: provider={provider}, branch={branch_number}, id={sid}")
    cur.execute(UPSERT_SUPER, dict(
        id=sid, address=address, provider=provider,
        branch_number=branch_number, branch_name=branch_name
    ))
    dbid = cur.fetchone()[0]
    dbid = str(dbid)
    cache.put_super(*key, sid)
    print(f"[DEBUG] super upserted -> dbid={dbid}")
    return dbid

def upsert_product(cur, cache: IdCache, *, super_id, barcode: str,
                   name: str | None, brand: str | None, updated_at, price):
    if not barcode:
        print("[DEBUG] skipping product without barcode")
        return None
    cached = cache.get_product(super_id, barcode)
    if cached:
        pid = cached
        print(f"[DEBUG] product found in cache: {barcode} -> {pid}")
    else:
        pid = str(uuid.uuid4())
        print(f"[DEBUG] inserting new product: barcode={barcode}, name={name}, brand={brand}, id={pid}")
    cur.execute(UPSERT_PRODUCT, dict(
        id=pid, brand=brand, name=name, barcode=barcode,
        updatedat=updated_at, super_id=super_id, price=price
    ))
    dbid = cur.fetchone()[0]
    dbid = str(dbid)
    cache.put_product(super_id, barcode, dbid)
    print(f"[DEBUG] product upserted -> dbid={dbid}")
    return dbid

# ---------- loaders ----------
def load_prices_file(cur, cache: IdCache, path: str):
    print(f"\n[DEBUG] loading prices file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    provider = (doc.get("provider") or "UNKNOWN").strip()
    branch_number = str(doc.get("branch")) if doc.get("branch") is not None else None
    branch_name = (doc.get("branch_name") or "").strip() if "branch_name" in doc else None
    timestamp = parse_iso(doc["timestamp"]) if doc.get("timestamp") else None

    print(f"[DEBUG] provider={provider}, branch={branch_number}, branch_name={branch_name}, timestamp={timestamp}")

    super_id = upsert_super(cur, cache,
                            provider=provider,
                            branch_number=branch_number,
                            branch_name=branch_name)

    rows = 0
    for it in doc.get("items", []):
        barcode = str(it.get("itemCode") or "").strip()
        if not barcode:
            print("[DEBUG] skipping item without barcode")
            continue
        name = it.get("product")
        updated_at = parse_iso(it["updatedat"]) if it.get("updatedat") else timestamp
        price = float(it["price"]) if it.get("price") is not None else None  

        print(f"[DEBUG] processing product: barcode={barcode}, name={name}, price={price}")

        pid = upsert_product(cur, cache,
                             super_id=super_id,
                             barcode=barcode,
                             name=name,
                             brand=branch_number,   # כרגע את שמה branch_number כ-brand
                             updated_at=updated_at,
                             price=price)
        if pid:
            rows += 1
    print(f"[DEBUG] total products upserted from {path}: {rows}")
    return rows

def load_promos_file(cur, cache: IdCache, path: str):
    print(f"\n[DEBUG] loading promos file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    provider = (doc.get("provider") or "UNKNOWN").strip()
    branch_number = str(doc.get("branch")) if doc.get("branch") is not None else None
    branch_name = (doc.get("branch_name") or "").strip() if "branch_name" in doc else None

    print(f"[DEBUG] provider={provider}, branch={branch_number}, branch_name={branch_name}")

    super_id = upsert_super(cur, cache,
                            provider=provider,
                            branch_number=branch_number,
                            branch_name=branch_name)

    links = 0
    for promo in doc.get("items", []):
        promo_id_ext = str(promo.get("promotionId")) if promo.get("promotionId") is not None else None
        start = parse_iso(promo["start"]) if promo.get("start") else None
        end = parse_iso(promo["end"]).replace(tzinfo=None) if promo.get("end") else None
        min_qty = float(promo["minQty"]) if promo.get("minQty") is not None else None
        discount_price = float(promo["discountedPrice"]) if promo.get("discountedPrice") is not None else None
        discount_rate = float(promo["discountRate"]) if promo.get("discountRate") is not None else None

        print(f"[DEBUG] upserting promo: id={promo_id_ext}, start={start}, end={end}")

        cur.execute(UPSERT_PROMO, dict(
            start=start, end=end, minQty=min_qty,
            discountRate=discount_rate, discountPrice=discount_price,
            promotion_id=promo_id_ext
        ))
        promo_db_id = cur.fetchone()[0]
        print(f"[DEBUG] promo upserted -> dbid={promo_db_id}")

        for p in promo.get("products", []):
            barcode = str(p.get("itemCode") or "").strip()
            if not barcode:
                print("[DEBUG] skipping promo product without barcode")
                continue
            print(f"[DEBUG] linking promo {promo_db_id} to product barcode={barcode}")
            pid = upsert_product(cur, cache,
                                 super_id=super_id,
                                 barcode=barcode,
                                 name=p.get("name"),
                                 brand=None,
                                 updated_at=None,
                                 price=None)
            if not pid:
                continue
            cur.execute(UPSERT_PROMO_LINK, dict(product_id=str(pid), promo_id=str(promo_db_id)))
            links += 1
    print(f"[DEBUG] total promo links from {path}: {links}")
    return links

# ---------- main ----------
def load_folder(prices_glob: str, promos_glob: str):
    print("[BOOT] starting json_loader.py", flush=True)
    cache = IdCache()
    print(f"[DEBUG] connecting to database: {PG_DSN}")
    with psycopg2.connect(PG_DSN) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            n_prod = 0
            for path in sorted(glob.glob(prices_glob)):
                print(f"[DEBUG] processing file: {path}")
                n_prod += load_prices_file(cur, cache, path)
            print(f"[RESULT] Upserted products (with prices): {n_prod}")

            n_links = 0
            for path in sorted(glob.glob(promos_glob)):
                print(f"[DEBUG] processing file: {path}")
                n_links += load_promos_file(cur, cache, path)
            print(f"[RESULT] Inserted promo->product links: {n_links}")

        print("[DEBUG] committing transaction")
        conn.commit()
