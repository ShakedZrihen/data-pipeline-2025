import os
from typing import List, Optional
import os
from typing import List, Optional
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
import psycopg

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "pricedb")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

app = FastAPI(
    title="Price API",
    version="1.0.0",
    description="Supermarket prices — search, filter, compare (barcode).",
)

def get_conn():
    return psycopg.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

class Supermarket(BaseModel):
    supermarket_id: int
    name: str
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: Optional[str] = None

class Product(BaseModel):
    product: str
    provider: str
    branch: str
    price: float
    unit: Optional[str] = None
    ts: str
    barcode: Optional[str] = None
    canonical_name: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    size_value: Optional[float] = None
    size_unit: Optional[str] = None
    currency: Optional[str] = None
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    in_stock: Optional[bool] = None

class PriceComparison(BaseModel):
    product_id: Optional[int] = None 
    supermarket_id: int
    supermarket_name: str
    canonical_name: Optional[str]
    brand: Optional[str]
    category: Optional[str]
    barcode: Optional[str]
    price: float
    promo_price: Optional[float]
    promo_text: Optional[str]
    size_value: Optional[float]
    size_unit: Optional[str]
    in_stock: Optional[bool]
    savings: float

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    return {"ok": True, "docs": "/docs"}


@app.get("/supermarkets", response_model=List[Supermarket], tags=["supermarkets"])
def list_supermarkets():
    sql = """
    SELECT row_number() over(order by provider) as supermarket_id,
           provider as name,
           NULL::text as branch_name,
           NULL::text as city,
           NULL::text as address,
           NULL::text as website,
           NOW()::timestamptz as created_at
    FROM (SELECT DISTINCT provider FROM price_items) t
    ORDER BY name;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [
        Supermarket(
            supermarket_id=r[0], name=r[1], branch_name=r[2],
            city=r[3], address=r[4], website=r[5],
            created_at=r[6].isoformat()
        )
        for r in rows
    ]

@app.get("/supermarkets/{supermarket_id}", response_model=Supermarket, tags=["supermarkets"])
def get_supermarket(supermarket_id: int):
    sql = """
    WITH providers AS (
      SELECT DISTINCT provider
      FROM price_items
      ORDER BY provider
    )
    SELECT row_number() over() as supermarket_id, provider
    FROM providers
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    if supermarket_id < 1 or supermarket_id > len(rows):
        raise HTTPException(status_code=404, detail="Not found")
    r = rows[supermarket_id - 1]
    return Supermarket(
        supermarket_id=supermarket_id,
        name=r[1], branch_name=None, city=None, address=None, website=None,
        created_at=None
    )

@app.get("/supermarkets/{supermarket_id}/products", response_model=List[Product], tags=["supermarkets"])
def products_by_supermarket(supermarket_id: int, search: Optional[str] = Query(None)):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            WITH providers AS (
              SELECT DISTINCT provider
              FROM price_items
              ORDER BY provider
            )
            SELECT provider FROM providers LIMIT 1 OFFSET %s
        """, (supermarket_id-1,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Supermarket not found")
        provider = row[0]

        base_select = """
            SELECT product, provider, branch, price, unit, ts,
                   barcode, canonical_name, brand, category, size_value, size_unit,
                   currency, promo_price, promo_text, in_stock
            FROM price_items
            WHERE provider=%s
        """
        params = [provider]
        if search:
            base_select += " AND product ILIKE %s"
            params.append(f"%{search}%")
        base_select += " ORDER BY ts DESC, product LIMIT 200"

        cur.execute(base_select, params)
        rows = cur.fetchall()

    return [
        Product(
            product=r[0], provider=r[1], branch=r[2], price=float(r[3]),
            unit=r[4], ts=r[5].isoformat(),
            barcode=r[6], canonical_name=r[7], brand=r[8], category=r[9],
            size_value=float(r[10]) if r[10] is not None else None,
            size_unit=r[11], currency=r[12],
            promo_price=float(r[13]) if r[13] is not None else None,
            promo_text=r[14], in_stock=r[15],
        )
        for r in rows
    ]


@app.get("/products", response_model=List[Product], tags=["products"])
def search_products(
    name: Optional[str] = Query(None, alias="q"),
    promo: Optional[bool] = Query(None),
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    supermarket_id: Optional[int] = None,
):
    where = []
    params = []
    if name:
        where.append("(product ILIKE %s OR canonical_name ILIKE %s)")
        params.extend([f"%{name}%", f"%{name}%"])
    if promo is True:
        where.append("promo_price IS NOT NULL")
    if min_price is not None:
        where.append("price >= %s"); params.append(min_price)
    if max_price is not None:
        where.append("price <= %s"); params.append(max_price)

    provider = None
    if supermarket_id:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
              WITH providers AS (SELECT DISTINCT provider FROM price_items ORDER BY provider)
              SELECT provider FROM providers LIMIT 1 OFFSET %s
            """, (supermarket_id-1,))
            row = cur.fetchone()
            if row: provider = row[0]
    if provider:
        where.append("provider = %s"); params.append(provider)

    sql = """
      SELECT product, provider, branch, price, unit, ts,
             barcode, canonical_name, brand, category, size_value, size_unit,
             currency, promo_price, promo_text, in_stock
      FROM price_items
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts DESC, product LIMIT 200"

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        Product(
            product=r[0], provider=r[1], branch=r[2], price=float(r[3]),
            unit=r[4], ts=r[5].isoformat(),
            barcode=r[6], canonical_name=r[7], brand=r[8], category=r[9],
            size_value=float(r[10]) if r[10] is not None else None,
            size_unit=r[11], currency=r[12],
            promo_price=float(r[13]) if r[13] is not None else None,
            promo_text=r[14], in_stock=r[15],
        )
        for r in rows
    ]

@app.get("/products/barcode/{barcode}", response_model=List[PriceComparison], tags=["products"])
def by_barcode(barcode: str):
    sql = """
    SELECT provider, branch, price, promo_price, promo_text,
           canonical_name, brand, category, size_value, size_unit, in_stock
    FROM price_items
    WHERE barcode = %s
    ORDER BY price ASC, ts DESC
    LIMIT 500
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (barcode,))
        rows = cur.fetchall()

    if not rows:
        return []

    prices = [float(r[2]) for r in rows if r[2] is not None]
    if not prices:
        return []
    min_price = min(prices)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT provider FROM (SELECT DISTINCT provider FROM price_items) t ORDER BY provider")
        provs = [r[0] for r in cur.fetchall()]
    prov_to_id = {p: i+1 for i, p in enumerate(provs)}

    out: List[PriceComparison] = []
    for r in rows:
        provider = r[0]
        price = float(r[2]) if r[2] is not None else 0.0
        out.append(PriceComparison(
            product_id=None,
            supermarket_id=prov_to_id.get(provider, 0),
            supermarket_name=provider,
            canonical_name=r[5],
            brand=r[6],
            category=r[7],
            barcode=barcode,
            price=price,
            promo_price=float(r[3]) if r[3] is not None else None,
            promo_text=r[4],
            size_value=float(r[8]) if r[8] is not None else None,
            size_unit=r[9],
            in_stock=r[10],
            savings=round(price - min_price, 2),
        ))
    out.sort(key=lambda x: (x.price, x.savings))
    return out
