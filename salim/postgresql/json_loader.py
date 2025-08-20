import os
import uuid
import json
import glob
from datetime import datetime

import psycopg2
import psycopg2.extras as pgx
from dotenv import load_dotenv

print("[BOOT] starting json_loader.py", flush=True)

# ---------- env ----------
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
def parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.replace(tzinfo=None)  # store tz-naive

# ---------- SQL ----------
UPSERT_SUPER = """
INSERT INTO supers (id, address, provider, branch_number, branch_name)
VALUES (%(id)s, %(address)s, %(provider)s, %(branch_number)s, %(branch_name)s)
ON CONFLICT (provider, branch_number) DO UPDATE
SET address = COALESCE(EXCLUDED.address, supers.address),
    branch_name = COALESCE(EXCLUDED.branch_name, supers.branch_name)
RETURNING id;
"""

PRODUCTS_UPSERT_SQL = """
INSERT INTO products (id, brand, name, barcode, updated_at, super_id, price)
VALUES %s
ON CONFLICT (barcode) DO UPDATE
SET brand      = COALESCE(EXCLUDED.brand, products.brand),
    name       = COALESCE(EXCLUDED.name,  products.name),
    price      = COALESCE(EXCLUDED.price, products.price),
    updated_at = GREATEST(COALESCE(products.updated_at, 'epoch'::timestamp),
                          COALESCE(EXCLUDED.updated_at, 'epoch'::timestamp))
RETURNING id, barcode;
"""

UPSERT_PROMO = """
INSERT INTO promos (start, "end", min_qty, discount_rate, discount_price, promotion_id)
VALUES (%(start)s, %(end)s, %(min_qty)s, %(discount_rate)s, %(discount_price)s, %(promotion_id)s)
ON CONFLICT (promotion_id) DO UPDATE
SET start          = LEAST(COALESCE(promos.start, EXCLUDED.start), EXCLUDED.start),
    "end"          = GREATEST(COALESCE(promos."end", EXCLUDED."end"), EXCLUDED."end"),
    min_qty        = COALESCE(EXCLUDED.min_qty, promos.min_qty),
    discount_rate  = COALESCE(EXCLUDED.discount_rate, promos.discount_rate),
    discount_price = COALESCE(EXCLUDED.discount_price, promos.discount_price)
RETURNING id;
"""

INSERT_PROMO_LINKS = """
INSERT INTO promo_to_product (product_id, promo_id)
VALUES %s
ON CONFLICT (product_id, promo_id) DO NOTHING;
"""

# ---------- caches ----------
class IdCache:
    def __init__(self):
        self.super_by_key = {}   # (provider, branch_number) -> supers.id (uuid)
        self.product_by_bc = {}  # barcode -> products.id (uuid)

    def get_super(self, provider, branch):
        return self.super_by_key.get((provider, branch))

    def put_super(self, provider, branch, sid):
        self.super_by_key[(provider, branch)] = sid

    def get_product_id(self, barcode):
        return self.product_by_bc.get(barcode)

    def put_product_id(self, barcode, pid):
        self.product_by_bc[barcode] = pid

# ---------- DB ops ----------
def upsert_super(cur, cache: IdCache, *, provider: str, branch_number: str | None,
                 address: str | None = None, branch_name: str | None = None) -> str:
    key = (provider, branch_number or "")
    sid = cache.get_super(*key)
    if sid:
        return sid

    # fast path read
    cur.execute("SELECT id FROM supers WHERE provider = %s AND branch_number = %s",
                (provider, branch_number))
    row = cur.fetchone()
    if row:
        sid = str(row[0])
        cache.put_super(*key, sid)
        return sid

    # insert if new
    provisional = str(uuid.uuid4())
    cur.execute(UPSERT_SUPER, dict(
        id=provisional, address=address, provider=provider,
        branch_number=branch_number, branch_name=branch_name
    ))
    sid = str(cur.fetchone()[0])
    cache.put_super(*key, sid)
    return sid

def upsert_products_batch(cur, rows):
    if not rows:
        return {}
    template = """(%(id)s, %(brand)s, %(name)s, %(barcode)s, %(updated_at)s, %(super_id)s, %(price)s)"""
    pgx.execute_values(cur, PRODUCTS_UPSERT_SQL, rows, template=template, page_size=1000)
    returned = cur.fetchall()  # [(id, barcode), ...]
    return {barcode: str(pid) for (pid, barcode) in returned}

def insert_promo_links_batch(cur, links):
    if not links:
        return 0
    pgx.execute_values(cur, INSERT_PROMO_LINKS, links, page_size=2000)
    return len(links)

# ---------- loaders ----------
def load_prices_file(cur, cache: IdCache, path: str):
    print(f"\n[INFO] loading prices file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    provider = (doc.get("provider") or "UNKNOWN").strip()
    branch_number = str(doc.get("branch")) if doc.get("branch") is not None else None
    branch_name = (doc.get("branch_name") or "").strip() if "branch_name" in doc else None
    file_ts = parse_iso(doc.get("timestamp"))

    super_id = upsert_super(cur, cache,
                            provider=provider,
                            branch_number=branch_number,
                            branch_name=branch_name)

    batch = []
    items = doc.get("items", []) or []
    for idx, it in enumerate(items, 1):
        barcode = str(it.get("itemCode") or "").strip()
        if not barcode:
            continue
        name = it.get("product") or ""
        updated_at = parse_iso(it.get("updatedAt")) or file_ts
        pid = cache.get_product_id(barcode) or str(uuid.uuid4())
        batch.append(dict(
            id=pid,
            brand=(branch_number or ""),   # NOT NULL in schema
            name=name,
            barcode=barcode,
            updated_at=updated_at,
            super_id=super_id,
            price=float(it["price"]) if it.get("price") is not None else None
        ))
        if idx % 1000 == 0:
            print(f"[DEBUG] queued {idx} products...")

    if batch:
        barcode_to_id = upsert_products_batch(cur, batch)
        for bc, pid in barcode_to_id.items():
            cache.put_product_id(bc, pid)

    print(f"[RESULT] Upserted products from {os.path.basename(path)}: {len(batch)}")
    return len(batch)

def load_promos_file(cur, cache: IdCache, path: str):
    """
    ENSURE-EXIST:
      - Upsert promo
      - Ensure every referenced barcode exists in products (create placeholder if missing)
      - Link (barcode, promotion_id)
    """
    print(f"\n[INFO] loading promos file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    provider = (doc.get("provider") or "UNKNOWN").strip()
    branch_number = str(doc.get("branch")) if doc.get("branch") is not None else None
    branch_name = (doc.get("branch_name") or "").strip() if "branch_name" in doc else None

    super_id = upsert_super(cur, cache,
                            provider=provider,
                            branch_number=branch_number,
                            branch_name=branch_name)

    total_links = 0
    promos_list = doc.get("items", []) or []
    for i, promo in enumerate(promos_list, 1):
        promo_id_ext = str(promo.get("promotionId")) if promo.get("promotionId") is not None else None
        if not promo_id_ext:
            continue  # skip promos without stable ID

        start = parse_iso(promo.get("start"))
        end = parse_iso(promo.get("end"))
        min_qty = float(promo["minQty"]) if promo.get("minQty") is not None else None
        discount_price = float(promo["discountedPrice"]) if promo.get("discountedPrice") is not None else None
        discount_rate = float(promo["discountRate"]) if promo.get("discountRate") is not None else None

        # upsert promo
        cur.execute(UPSERT_PROMO, dict(
            start=start,
            end=end,
            min_qty=min_qty,
            discount_rate=discount_rate,
            discount_price=discount_price,
            promotion_id=promo_id_ext
        ))
        _ = cur.fetchone()[0]  # not used for linking

        # gather barcodes (unique)
        prod_rows = []
        links = []
        seen_bc = set()

        for p in promo.get("products", []) or []:
            bc = str(p.get("itemCode") or "").strip()
            if not bc or bc in seen_bc:
                continue
            seen_bc.add(bc)

            # ensure-exist: create placeholder if not known yet
            pid = cache.get_product_id(bc)
            if not pid:
                pid = str(uuid.uuid4())
                prod_rows.append(dict(
                    id=pid,
                    brand=(branch_number or ""),   # NOT NULL
                    name=p.get("name") or "",
                    barcode=bc,
                    updated_at=None,
                    super_id=super_id,
                    price=None
                ))

            links.append((bc, promo_id_ext))

        # batch-insert any placeholders
        if prod_rows:
            bc_to_id = upsert_products_batch(cur, prod_rows)
            for bc, pid in bc_to_id.items():
                cache.put_product_id(bc, pid)

        # insert links
        total_links += insert_promo_links_batch(cur, links)

        if i % 200 == 0:
            print(f"[DEBUG] processed {i} promos...")

    print(f"[RESULT] Inserted promo->product links from {os.path.basename(path)}: {total_links}")
    return total_links

# ---------- main ----------
def load_folder(prices_glob: str, promos_glob: str):
    print("[BOOT] starting json_loader.py", flush=True)
    cache = IdCache()
    print(f"[INFO] connecting to database")
    with psycopg2.connect(PG_DSN) as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                n_prod = 0
                for path in sorted(glob.glob(prices_glob)):
                    print(f"[INFO] processing file: {path}")
                    n_prod += load_prices_file(cur, cache, path)
                print(f"[RESULT] Upserted products (with prices): {n_prod}")

                n_links = 0
                for path in sorted(glob.glob(promos_glob)):
                    print(f"[INFO] processing file: {path}")
                    n_links += load_promos_file(cur, cache, path)
                print(f"[RESULT] Inserted promo->product links: {n_links}")

            print("[INFO] committing transaction")
            conn.commit()
        except Exception as e:
            print(f"[ERROR] rolling back due to: {e!r}")
            conn.rollback()
            raise

if __name__ == "__main__":
    load_folder(
        prices_glob="./price*.json",
        promos_glob="./promo*.json",
    )
